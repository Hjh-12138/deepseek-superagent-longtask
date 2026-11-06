/**
 * Token 账本 · 实施文档 §4。
 *
 * 核心不变式：
 * - 可用余额 = 上限 − 已结算 − 在途预留。两个并行 Worker 不得同时花掉同一份余额。
 * - 每次调用前在同一事务预留；用量返回后结算并释放余量。
 * - 真正的重试是新的 attemptId，单独记账（归并流式样本后按尝试记）。
 * - 崩溃时保留未结算预留（state=reserved）并对账，不能直接释放。
 * - 严格模式下，无法证明额度充足（无输入上界）时必须在调用前停下。
 *
 * 账本本身不发起模型调用：调用方（调度器）先 reserve，拿到批准再调用，
 * 用 tokenUsage 投影的每次尝试增量调 settle / recordAttempt。
 */

import type { Store } from "../store/store.js";
import { newId } from "../store/store.js";
import type { LedgerEntry, Reservation, TaskBudgetState, UsageSample } from "../types.js";
import {PriceTable, formatCny, priceUsage, priceBound, priceTokenCeiling, type MoneyReservation} from "./money.js";

export const COLLECTION_BUDGET = "budgets";
export const COLLECTION_ENTRIES = "ledger";
export const COLLECTION_RESERVATIONS = "reservations";

export type ReserveResult =
  | { ok: true; reservationId: string }
  | { ok: false; reason: "insufficient"; available: number; requested: number }
  | { ok: false; reason: "unknown_input_bound"; available: number };

export function normalizeUsage(raw: UsageSample): { inputTokens: number; outputTokens: number } {
  // 缓存分项按互斥字段归并：适配器对同一请求只报 cacheRead 或 cacheWrite 之一；
  // 两个字段各加一次即可，总量 = 输入合计 + 输出，不再与分项重复相加（实施文档 §4）。
  const inputTokens = Math.max(0, raw.uncachedInputTokens) + Math.max(0, raw.cacheReadTokens) + Math.max(0, raw.cacheWriteTokens);
  return { inputTokens, outputTokens: Math.max(0, raw.outputTokens) };
}

export class TokenLedger {
  readonly moneyAccountingProtocol = 1;
  readonly protectedReserveVersion = 1;
  readonly requestAccountingProtocol = 1;
  constructor(
    private readonly store: Store,
    private readonly now: () => Date = () => new Date(),
    readonly prices = new PriceTable(),
    readonly route = {provider: "deepseek-official", model: "deepseek-flash"},
  ) {}

  moneyView(taskId: string) {
    const m = this.getBudget(taskId)?.money;
    return m ? {currency: m.currency, limitAmount: formatCny(m.limitNano), calculatedAmount: formatCny(m.settledNano),
      reservedAmount: formatCny(m.reservedNano), availableAmount: formatCny(BigInt(m.limitNano) - BigInt(m.settledNano) - BigInt(m.reservedNano)),
      basis: m.basis, note: "按用量及价格快照核算；内置价格按高峰价保守核算，不是平台实扣；仅含托管模型请求"} : null;
  }
  moneyEntries(taskId: string) {
    return this.store.list<LedgerEntry>(COLLECTION_ENTRIES).filter(e => e.taskId === taskId && e.money).slice(-20).map(e => ({
      attemptId: e.attemptId, at: e.createdAt, usage: e.raw, sessionId: e.money!.sessionId, role: e.money!.role, expertRelPath: e.money!.expertRelPath,
      provider: e.money!.price.provider, model: e.money!.price.model, priceSnapshot: e.money!.price, calculatedAmount: formatCny(e.money!.chargedNano), basis: e.money!.basis,
    }));
  }
  moneyForecast(taskId: string, input: number, output: number, protectedTokens = 0, closingTokens = 0) {
    const m = this.getBudget(taskId)?.money;
    if (!m) return null;
    const p = this.prices.get(this.route.provider, this.route.model, this.now());
    return {available: BigInt(m.limitNano) - BigInt(m.settledNano) - BigInt(m.reservedNano),
      need: priceBound(p, input, output), protected: priceTokenCeiling(p, protectedTokens), closing: priceTokenCeiling(p, closingTokens),
      usedRatio: Number(BigInt(m.settledNano) + BigInt(m.reservedNano)) / Number(m.limitNano)};
  }
  /** Bind the ACTUAL request route before the request leaves the host. Same transaction reserves money. */
  async reserveMoney(reservationId: string, route: {provider: string; model: string}, input: number, output: number, protectedTokens: number,
    identity: Omit<MoneyReservation, "price" | "reservedNano">): Promise<void> {
    await this.store.transact(data => {
      const r = data.collections.reservations?.[reservationId] as Reservation | undefined;
      if (!r || r.state !== "reserved") throw Error("模型请求缺少有效预留");
      const b = data.collections.budgets?.[r.taskId] as TaskBudgetState | undefined, m = b?.money;
      if (!m) return;
      const task = data.collections.tasks?.[r.taskId] as {status: string} | undefined;
      if (task && !(identity.role === "acceptor" ? task.status === "awaiting_acceptance" : ["active", "rework"].includes(task.status))) throw Error("任务在金额预留前已停止");
      const sessions = data.collections.taskSessions?.[r.taskId] as Record<string, {sessionId: string; revokedAt?: string}> | undefined;
      if (Object.values(sessions ?? {}).some(s => s.sessionId === identity.sessionId && s.revokedAt)) throw Error("执行会话已撤权");
      if (r.money) throw Error("同一预留不能重复发起模型请求");
      if (data.collections.accountingIssues?.[r.taskId]) throw Error("费用待核对，拒绝请求");
      const price = this.prices.get(route.provider, route.model, this.now()), need = priceBound(price, input, output);
      const available = BigInt(m.limitNano) - BigInt(m.settledNano) - BigInt(m.reservedNano);
      if (available < need + priceTokenCeiling(price, protectedTokens)) throw Error(`金额不足：本次预留¥${formatCny(need)}，另留收尾验收费用，可用¥${formatCny(available)}`);
      m.reservedNano = (BigInt(m.reservedNano) + need).toString();
      r.money = {...identity, price, reservedNano: need.toString()};
    });
  }

  /** 初始化或读取任务预算状态（任务确认时在确认事务中调用） */
  async ensureBudget(taskId: string, limitTokens: number, strict: boolean): Promise<TaskBudgetState> {
    let state: TaskBudgetState | undefined;
    await this.store.transact((data) => {
      const coll = (data.collections[COLLECTION_BUDGET] ??= {});
      const existing = coll[taskId] as TaskBudgetState | undefined;
      if (existing) {
        existing.limitTokens = limitTokens;
        existing.strict = strict;
        state = existing;
      } else {
        state = {
          taskId,
          limitTokens,
          strict,
          settledTokens: 0,
          reservedTokens: 0,
          grants: [],
        };
        coll[taskId] = state;
      }
    });
    if (!state) throw new Error("预算状态初始化失败");
    return state;
  }

  getBudget(taskId: string): TaskBudgetState | undefined {
    return this.store.get<TaskBudgetState>(COLLECTION_BUDGET, taskId);
  }

  async releaseUnsent(reservationId: string, preparedButNotSent = false): Promise<void> {
    await this.store.transact(data => {
      const r = data.collections.reservations?.[reservationId] as Reservation | undefined;
      if (!r || r.state !== "reserved" || (r.money && !preparedButNotSent)) return;
      const b = data.collections.budgets?.[r.taskId] as TaskBudgetState;
      if (r.money && b.money) b.money.reservedNano = (BigInt(b.money.reservedNano) - BigInt(r.money.reservedNano)).toString();
      b.reservedTokens -= r.tokens; r.state = "released"; r.settledAt = this.now().toISOString();
    });
  }

  accountingIssue(taskId: string): {taskId: string; sessionId: string; reason: string} | undefined {
    return this.store.get("accountingIssues", taskId);
  }

  /** 未知费用不是0：持久阻止恢复，保留未结算预留供人工核对。 */
  async blockAccounting(taskId: string, sessionId: string, reason: string): Promise<void> {
    await this.store.transact(data => {
      (data.collections.accountingIssues ??= {})[taskId] = {taskId, sessionId, reason, createdAt: this.now().toISOString()};
    });
  }

  usageEventSeen(key: string): boolean { return Boolean(this.store.get("usageEvents", key)); }

  available(taskId: string): number {
    const b = this.getBudget(taskId);
    if (!b) return 0;
    return b.limitTokens - b.settledTokens - b.reservedTokens;
  }

  /**
   * 调用前预留。estimatedInput 无法给出时传 null：
   * 严格模式拒绝（返回 unknown_input_bound），宽松模式按最小预留通过并留下警示。
   * protectedTokens在同一事务检查但不计为在途用量，留给后续验收/其他执行者报告。
   */
  async reserve(taskId: string, estimatedInput: number | null, estimatedOutput: number, protectedTokens = 0): Promise<ReserveResult> {
    if (!Number.isSafeInteger(protectedTokens) || protectedTokens < 0) throw new Error("受保护额度必须为非负整数");
    let result: ReserveResult | undefined;
    await this.store.transact((data) => {
      const coll = (data.collections[COLLECTION_BUDGET] ??= {});
      const b = coll[taskId] as TaskBudgetState | undefined;
      if (!b) {
        result = { ok: false, reason: "insufficient", available: 0, requested: Number.NaN };
        return;
      }
      const avail = b.limitTokens - b.settledTokens - b.reservedTokens;
      if (data.collections.accountingIssues?.[taskId]) { result = {ok: false, reason: "unknown_input_bound", available: avail}; return; }
      if (estimatedInput === null) {
        if (b.strict) {
          result = { ok: false, reason: "unknown_input_bound", available: avail };
          return;
        }
        estimatedInput = 0; // 宽松模式：接受未知输入，结算时补记
      }
      const requested = estimatedInput + Math.max(0, estimatedOutput);
      if (avail < requested + protectedTokens) {
        result = { ok: false, reason: "insufficient", available: avail, requested: requested + protectedTokens };
        return;
      }
      b.reservedTokens += requested;
      const reservation: Reservation = {
        id: newId("rsv"),
        taskId,
        tokens: requested,
        state: "reserved",
        createdAt: this.now().toISOString(),
        settledAt: null,
      };
      ((data.collections[COLLECTION_RESERVATIONS] ??= {})[reservation.id]) = reservation;
      result = { ok: true, reservationId: reservation.id };
    });
    if (!result) throw new Error("预留事务未产生结果");
    return result;
  }

  /**
   * 用真实用量结算一次尝试：释放预留、按实际用量记账。
   * 重试是独立 attemptId，各记一条；实际用量可超过预留（结算不回滚余额下限为 0 的限制——
   * 超支部分使 available 变负，调度器下一次 reserve 会被拒，符合“事后发现超支即停”。
   */
  async settle(reservationId: string, usage: UsageSample | null, note: string | null, eventKey?: string): Promise<{ overLimit: boolean }> {
    let overLimit = false;
    await this.store.transact((data) => {
      if (eventKey && data.collections.usageEvents?.[eventKey]) return;
      const rsv = (data.collections[COLLECTION_RESERVATIONS] ??= {})[reservationId] as Reservation | undefined;
      if (!rsv) throw new Error(`预留不存在: ${reservationId}`);
      if (rsv.state !== "reserved") throw new Error(`预留状态不可结算: ${rsv.state}`);
      const budgets = (data.collections[COLLECTION_BUDGET] ??= {});
      const b = budgets[rsv.taskId] as TaskBudgetState | undefined;
      if (!b) throw new Error(`预算不存在: ${rsv.taskId}`);
      if (b.money && (!usage || !rsv.money)) throw Error("金额结算缺少请求价格快照或用量，必须核对");
      b.reservedTokens -= rsv.tokens;
      if (usage !== null) {
        const { inputTokens, outputTokens } = normalizeUsage(usage);
        b.settledTokens += inputTokens + outputTokens;
        const entry: LedgerEntry = {
          taskId: rsv.taskId,
          attemptId: reservationId,
          inputTokens,
          outputTokens,
          raw: usage,
          kind: "attempt",
          createdAt: this.now().toISOString(),
          note,
        };
        if (b.money && rsv.money) {
          const charged = priceUsage(rsv.money.price, usage);
          b.money.reservedNano = (BigInt(b.money.reservedNano) - BigInt(rsv.money.reservedNano)).toString();
          b.money.settledNano = (BigInt(b.money.settledNano) + charged).toString();
          entry.money = {...rsv.money, chargedNano: charged.toString(), basis: "usage-priced-estimate"};
          if (BigInt(b.money.settledNano) + BigInt(b.money.reservedNano) > BigInt(b.money.limitNano)) overLimit = true;
        }
        ((data.collections[COLLECTION_ENTRIES] ??= {})[`${reservationId}#attempt`]) = entry;
        if (b.settledTokens > b.limitTokens) overLimit = true;
      }
      rsv.state = "settled";
      rsv.settledAt = this.now().toISOString();
      if (eventKey) (data.collections.usageEvents ??= {})[eventKey] = {taskId: rsv.taskId, reservationId};
    });
    return { overLimit };
  }

  /**
   * 崩溃恢复对账：预留未经结算即被重启发现。不能直接释放——
   * 由恢复器核对会话历史/产物后调用：usage=null 且 confirmConsumed=false 才允许 released，
   * 否则把估算量转为 settled（保守记账，宁可高估）。
   */
  async reconcile(reservationId: string, usage: UsageSample | null, confirmConsumed: boolean): Promise<void> {
    await this.store.transact((data) => {
      const rsv = (data.collections[COLLECTION_RESERVATIONS] ??= {})[reservationId] as Reservation | undefined;
      if (!rsv || rsv.state !== "reserved") return;
      const budgets = (data.collections[COLLECTION_BUDGET] ??= {});
      const b = budgets[rsv.taskId] as TaskBudgetState | undefined;
      if (!b) return;
      if (b.money && !rsv.money && usage === null) {
        // Monetary requests must durably bind a price before the transport starts; an unbound pre-step never left.
        b.reservedTokens -= rsv.tokens; rsv.state = "released"; rsv.settledAt = this.now().toISOString(); return;
      }
      if (b.money && usage === null && confirmConsumed && rsv.money) {
        // A crash is not proof of cost or non-delivery. Keep both reservations and latch recovery.
        (data.collections.accountingIssues ??= {})[rsv.taskId] = {taskId: rsv.taskId, sessionId: rsv.money.sessionId,
          reason: "重启遗留模型请求费用未知，保留在途金额，须核对请求用量", createdAt: this.now().toISOString()};
        const t = data.collections.tasks?.[rsv.taskId] as {status: string; pauseReason: string | null; resumeStatus?: string} | undefined;
        if (t && !["cancelled", "delivered"].includes(t.status)) {if (["active", "rework", "awaiting_acceptance"].includes(t.status)) t.resumeStatus = t.status; t.status = "paused"; t.pauseReason = "budget";}
        return;
      }
      if (b.money && usage && !rsv.money) throw Error("金额恢复缺少价格快照，不可猜测费用");
      if (b.money && rsv.money) {
        b.money.reservedNano = (BigInt(b.money.reservedNano) - BigInt(rsv.money.reservedNano)).toString();
        if (usage) b.money.settledNano = (BigInt(b.money.settledNano) + priceUsage(rsv.money.price, usage)).toString();
      }
      if (usage !== null || confirmConsumed) {
        const inputTokens = usage ? normalizeUsage(usage).inputTokens : rsv.tokens;
        const outputTokens = usage ? normalizeUsage(usage).outputTokens : 0;
        b.reservedTokens -= rsv.tokens;
        b.settledTokens += inputTokens + outputTokens;
        const entry: LedgerEntry = {
          taskId: rsv.taskId,
          attemptId: reservationId,
          inputTokens,
          outputTokens,
          raw: usage ?? { uncachedInputTokens: rsv.tokens, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 },
          kind: "adjustment",
          createdAt: this.now().toISOString(),
          note: "恢复对账：未结算预留按估算或确认用量转为已结算",
        };
        if (rsv.money && usage) entry.money = {...rsv.money, chargedNano: priceUsage(rsv.money.price, usage).toString(), basis: "usage-priced-estimate"};
        ((data.collections[COLLECTION_ENTRIES] ??= {})[`${reservationId}#reconcile`]) = entry;
        rsv.state = "orphaned";
      } else {
        b.reservedTokens -= rsv.tokens;
        rsv.state = "released";
      }
      rsv.settledAt = this.now().toISOString();
    });
  }

  /** 用户追加额度 */
  async grant(taskId: string, tokens: number, note: string): Promise<void> {
    await this.store.transact((data) => {
      const b = (data.collections[COLLECTION_BUDGET] ??= {})[taskId] as TaskBudgetState | undefined;
      if (!b) throw new Error(`预算不存在: ${taskId}`);
      if (b.money) throw Error("金额任务不能通过追加Token改变额度");
      b.limitTokens += tokens;
      b.grants.push({ tokens, at: this.now().toISOString(), note });
    });
  }

  /** 任务总消耗（含重开任务继承：调用方把原任务 settledTokens 一并传入新预算的初始值） */
  totalSettled(taskId: string): number {
    return this.getBudget(taskId)?.settledTokens ?? 0;
  }

  /** 列出崩溃后仍处 reserved 状态的预留（恢复器入口） */
  listUnsettledReservations(): Reservation[] {
    return this.store
      .list<Reservation>(COLLECTION_RESERVATIONS)
      .filter((r) => r.state === "reserved");
  }
}
