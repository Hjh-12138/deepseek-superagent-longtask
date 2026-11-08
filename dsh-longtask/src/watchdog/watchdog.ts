/**
 * Watchdog 与恢复器 · 设计文档 §3/§5、实施文档 §2-§3。
 *
 * Watchdog 问“还有进展吗”：读产物变化、工具结果、验收差异及等待依赖；
 * 重复发消息本身不算进展；先尝试换路径（纠偏），重复失败才带着
 * 尝试记录和一个具体问题交给用户。
 *
 * 恢复器问“重启后从哪继续”：核对磁盘产物、会话与已投递消息，
 * 不盲目重复写文件、提交代码或对外发送（实施文档 §3-6）。
 */

import type { Store } from "../store/store.js";
import type { Task, TaskProgressState } from "../types.js";
import { COLLECTION_TASKS } from "../tasks/task-service.js";
import type { TokenLedger } from "../ledger/token-ledger.js";
import {DEFAULT_JOB_TIMEOUT_MS,jobWithinGrace,type ObservedJob} from "../tasks/job-state.js";

export const COLLECTION_PROGRESS = "progress";

export interface WatchdogConfig {
  /** 停滞阈值（毫秒）：无实质进展超过该值先纠偏。可配置的实现参数（设计文档 §5） */
  stallThresholdMs: number;
  /** 连续纠偏次数达到该值才求助用户 */
  maxCorrections: number;
  jobTimeoutMs?: number;
}

export const DEFAULT_WATCHDOG_CONFIG: WatchdogConfig = {
  stallThresholdMs: 15 * 60 * 1000,
  maxCorrections: 2,
};

export type WatchdogAction =
  | { action: "none"; note: string; state: "progress" | "waiting" }
  | { action: "correct"; mode: "slow" | "loop"; instruction: string; note: string }
  | {
      action: "escalate";
      question: string;
      attempts: string[];
      /** 求助必须带证据：在等什么、最后一次实质进展、已产出多少产物 */
      evidence: { waiting: string[]; lastSubstantiveAt: string; artifacts: number; corrections: number };
      note: string;
    };

/** Watchdog 看到的等待事实：由 taskWaits 计算，而不是猜 waitingOn 字符串 */
export interface WatchdogWaitView {
  /** 是否存在尚未满足的等待 */
  active: boolean;
  /** 等待描述（求助时作为证据） */
  descriptions: string[];
}

/** 计算指令哈希：用于识别“重复发同一消息”（重复发消息不算进展）；去除全部空白 */
export function instructionKey(text: string): string {
  return text.replace(/\s+/g, "").toLowerCase().slice(0, 200);
}

/**
 * 纯决策：给定进展状态与当前时间，返回 Watchdog 动作。
 * - 等待中的任务（登记了唤醒条件）不算停滞——正常等待与无效循环是两个判断
 * - 最近指令重复出现 → 判定无效循环，纠偏要求换方法
 * - 超过停滞阈值 → 先纠偏；纠偏次数用尽 → 带证据求助
 */
export function watchdogDecision(
  progress: TaskProgressState,
  wait: WatchdogWaitView,
  now: number,
  cfg: WatchdogConfig = DEFAULT_WATCHDOG_CONFIG,
): WatchdogAction {
  // 三态之一：正常等待。有未满足的等待就不算停滞，重复发消息不是进展，但"确实在等"也不该催
  if (wait.active) return { action: "none", state: "waiting", note: "任务有未满足的等待条件，属正常等待" };
  const hashes = progress.recentInstructionHashes;
  const last = hashes[hashes.length - 1];
  const prev = hashes[hashes.length - 2];
  const repeated = last !== undefined && prev === last;

  const elapsed = now - Math.max(Date.parse(progress.lastSubstantiveAt),
    Date.parse(progress.observationStartedAt ?? progress.lastSubstantiveAt));
  if (repeated && elapsed > cfg.stallThresholdMs) {
    if (progress.corrections < cfg.maxCorrections) {
      return {
        action: "correct",
        mode: "loop",
        instruction: "检测到重复执行同一做法且无实质进展：请换一种方法推进，先说明上次尝试及结果，再给出不同的下一步；如果确实无法推进，请明确说明缺什么。",
        note: `第 ${progress.corrections + 1} 次纠偏（无效循环）：指令重复 ${instructionKey(last!)}`,
      };
    }
    return {
      action: "escalate",
      question: "任务已多次纠偏仍无进展，请确认：当前卡点是什么？需要补充哪一项输入或决定？",
      attempts: hashes.slice(-4),
      evidence: { waiting: wait.descriptions, lastSubstantiveAt: progress.lastSubstantiveAt, artifacts: 0, corrections: progress.corrections },
      note: `纠偏 ${progress.corrections} 次后仍停滞（${Math.round(elapsed / 60000)} 分钟无实质进展），带证据求助`,
    };
  }
  if (elapsed > cfg.stallThresholdMs * 2) {
    if (progress.corrections < cfg.maxCorrections) {
      return {
        action: "correct",
        mode: "slow",
        instruction: "任务长时间无实质进展：请复核当前缺项是否仍可执行；若在等外部结果，请用 longtask_report 的 waiting 说明等待对象；若可执行，请给出具体下一步。",
        note: `超过 2 倍停滞阈值无实质进展，第 ${progress.corrections + 1} 次纠偏（缓慢）`,
      };
    }
    return {
      action: "escalate",
      question: "任务长时间无实质进展且纠偏未果，请确认：剩余工作是什么？是否缺少用户输入或额度？",
      attempts: hashes.slice(-4),
      evidence: { waiting: wait.descriptions, lastSubstantiveAt: progress.lastSubstantiveAt, artifacts: 0, corrections: progress.corrections },
      note: `长时间停滞（${Math.round(elapsed / 60000)} 分钟），带证据求助`,
    };
  }
  return { action: "none", state: "progress", note: "有近期实质进展，不打扰" };
}

export class Watchdog {
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly store: Store,
    private readonly cfg: WatchdogConfig = DEFAULT_WATCHDOG_CONFIG,
    private readonly now: () => number = () => Date.now(),
  ) {}

  /** 读或初始化任务的进展状态 */
  getProgress(taskId: string): TaskProgressState {
    return (
      this.store.get<TaskProgressState>(COLLECTION_PROGRESS, taskId) ?? {
        taskId,
        lastSubstantiveAt: new Date().toISOString(),
        recentInstructionHashes: [],
        corrections: 0,
        lastAttemptNote: null,
      }
    );
  }

  /** 调度器投递指令时登记：指令哈希进环形队列，供重复检测 */
  async recordInstruction(taskId: string, content: string): Promise<void> {
    await this.store.transact((data) => {
      const coll = (data.collections[COLLECTION_PROGRESS] ??= {});
      const p = (coll[taskId] as TaskProgressState | undefined) ?? {
        taskId,
        lastSubstantiveAt: new Date().toISOString(),
        recentInstructionHashes: [],
        corrections: 0,
        lastAttemptNote: null,
      };
      p.recentInstructionHashes = [...p.recentInstructionHashes, instructionKey(content)].slice(-8);
      coll[taskId] = p;
    });
  }

  /** 实质进展登记：产物变化、工具结果、验收差异（由监听器调用；发消息不算） */
  async recordSubstantiveProgress(taskId: string, note: string): Promise<void> {
    await this.store.transact((data) => {
      const coll = (data.collections[COLLECTION_PROGRESS] ??= {});
      const p = (coll[taskId] as TaskProgressState | undefined) ?? {
        taskId,
        lastSubstantiveAt: "",
        recentInstructionHashes: [],
        corrections: 0,
        lastAttemptNote: null,
      };
      p.lastSubstantiveAt = new Date().toISOString();
      p.lastAttemptNote = note;
      p.corrections = 0;
      coll[taskId] = p;
    });
  }

  /**
   * 纠偏落地登记（审查 P2-12）：只累加纠偏次数，不动进展时间——
   * 纠偏本身不是实质进展，次数用尽才会升级求助。
   */
  async applyCorrection(taskId: string, note: string): Promise<void> {
    await this.store.transact((data) => {
      const coll = (data.collections[COLLECTION_PROGRESS] ??= {});
      const p = (coll[taskId] as TaskProgressState | undefined) ?? {
        taskId,
        lastSubstantiveAt: new Date().toISOString(),
        recentInstructionHashes: [],
        corrections: 0,
        lastAttemptNote: null,
      };
      p.corrections += 1;
      p.lastAttemptNote = note;
      coll[taskId] = p;
    });
  }

  /** 对所有活跃任务跑一轮检查，返回应执行的动作 */
  checkAll(): Array<{ task: Task; action: WatchdogAction }> {
    const out: Array<{ task: Task; action: WatchdogAction }> = [];
    const waits = this.store.list<{ taskId: string; taskVersion?: number; status: string; description: string; wakeCondition: string | null; kind?: string; observed?: boolean; executorRelPath?: string }>("taskWaits");
    for (const task of this.store.list<Task>(COLLECTION_TASKS)) {
      if (task.status !== "active" && task.status !== "rework") continue;
      const active = waits.filter((w) => w.taskId === task.id && w.status === "waiting" &&
        (w.taskVersion ?? 1) === task.versions[task.currentVersion]?.version);
      const version = task.versions[task.currentVersion]!;
      const liveJobs = this.store.list<ObservedJob>("backgroundJobs").filter(j=>j.taskId===task.id&&j.taskVersion===version.version&&
        jobWithinGrace(j,this.now(),this.cfg.jobTimeoutMs ?? DEFAULT_JOB_TIMEOUT_MS,task.createdAt));
      // 只有"全部执行者都有可靠外部等待"才整体免催：一位下游等待不能屏蔽上游停滞（审查 C8）
      const executors = version.executors.filter(e=>{
        const a=version.executionPlanVersion&&version.assignments?.find(a=>a.relPath===e.relPath);
        const completed=this.store.get<{completed:string[]}>("batchProgress",`${task.id}:v${version.version}:${e.relPath}`)?.completed ?? [];
        return !a || !a.batches?.every(b=>completed.includes(b.id));
      }).map(e=>e.relPath);
      const waitingExecutors = new Set(
        [...liveJobs.map(j=>j.relPath),...active.filter(w => w.kind === "user_approval" && w.observed && w.executorRelPath).map(w => w.executorRelPath!), ...active
          .map((w) => w.wakeCondition)
          .filter((c): c is string => /^(dependency|executor-reply):/.test(c ?? "") || Boolean(c?.startsWith("job:")&&liveJobs.some(j=>c===`job:${j.relPath}:${j.jobId}`)))
          .map((c) => c.split(":")[1] ?? "")],
      );
      const allBlocked = executors.length > 0 && executors.every((rel) => waitingExecutors.has(rel));
      const action = watchdogDecision(
        this.getProgress(task.id),
        { active: allBlocked, descriptions: active.map((w) => w.description) },
        this.now(),
        this.cfg,
      );
      if (action.action !== "none") out.push({ task, action });
    }
    return out;
  }

  /** 宿主侧定期调用（实施文档 §2：Watchdog / 恢复器：计时） */
  start(intervalMs: number, handler: (entries: Array<{ task: Task; action: WatchdogAction }>) => void | Promise<void>): void {
    this.stop();
    this.timer = setInterval(() => {
      void handler(this.checkAll());
    }, intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

// ---------------------------------------------------------------------------
// 恢复器
// ---------------------------------------------------------------------------

export interface RecoveryReport {
  /** 恢复为可运行的任务 */
  reactivated: string[];
  /** 保持原状态的任务及原因（主动暂停、待答复、预算耗尽不误启） */
  kept: Array<{ taskId: string; reason: string }>;
  /** 投递结果未知的消息：恢复器先查会话历史，再决定是否重投 */
  unknownDeliveries: Array<{ messageId: string; taskId: string; action: "redeliver" | "mark_delivered" | "needs_check" }>;
  /** 崩溃时未结算的预留数（调用方交给账本对账） */
  unsettledReservations: number;
  /** 本次启动已对账（转保守结算）的预留数 */
  reconciledReservations: number;
  /** 会话已不存在的任务（Worker 会话丢失，需要重建） */
  brokenSessions: Array<{ taskId: string; relPath: string }>;
}

export interface RecoveryHost {
  sessionAlive(sessionKey: string): Promise<boolean>;
}

/**
 * 重启恢复。原则（实施文档 §3-6）：
 * - 恢复已确认且原本可运行的任务（active/rework/awaiting_acceptance）
 * - 主动暂停（user）、待答复（missing_input）、预算耗尽（budget）保持原状态
 * - 已投递消息和外部动作先核对，不盲目重复副作用
 */
export async function recoverOnStartup(store: Store, host: RecoveryHost, ledger: TokenLedger): Promise<RecoveryReport> {
  const report: RecoveryReport = { reactivated: [], kept: [], unknownDeliveries: [], unsettledReservations: 0, reconciledReservations: 0, brokenSessions: [] };

  // 崩溃遗留的在途预留必须对账，不能只统计不处理：
  // 预留状态会永久占住 available（既不结算也不释放），几次崩溃后任务就会"看起来预算跑完了"。
  // 无法确认该请求是否真的发出，按估算保守记账（宁可高估消耗，绝不低估上限）。
  const orphans = ledger.listUnsettledReservations();
  report.unsettledReservations = orphans.length;
  for (const rsv of orphans) {
    try {
      await ledger.reconcile(rsv.id, null, true);
      if (!ledger.listUnsettledReservations().some(r => r.id === rsv.id)) report.reconciledReservations++;
    } catch {
      // 单笔对账失败不影响其余恢复动作；遗留预留会继续占用额度并被下一次启动再次核对
    }
  }

  for (const task of store.list<Task>(COLLECTION_TASKS)) {
    switch (task.status) {
      case "active":
      case "rework":
      case "awaiting_acceptance": {
        // 核对任务会话是否存活；不存活则记录，需要重建（不自动重投任何消息）
        const sessions = store.get<Record<string, string>>("taskSessions", task.id) ?? {};
        let broken = false;
        for (const [relPath, sessionKey] of Object.entries(sessions)) {
          if (!(await host.sessionAlive(sessionKey))) {
            report.brokenSessions.push({ taskId: task.id, relPath });
            broken = true;
          }
        }
        if (broken) {
          report.kept.push({ taskId: task.id, reason: "Worker 会话丢失，需重建后由用户确认继续" });
        } else {
          report.reactivated.push(task.id);
        }
        break;
      }
      case "paused": {
        const labels: Record<string, string> = { user: "主动暂停", budget: "预算耗尽", missing_input: "待答复", watchdog: "Watchdog 暂停", host_restart: "宿主重启" };
        report.kept.push({ taskId: task.id, reason: `${labels[task.pauseReason ?? ""] ?? task.pauseReason ?? "已暂停"}，不误启` });
        break;
      }
      default:
        report.kept.push({ taskId: task.id, reason: `状态 ${task.status} 无需恢复动作` });
    }
  }

  for (const msg of store.list<{ id: string; taskId: string; delivery: string; kind: string }>("messages")) {
    if (msg.delivery === "unknown") {
      // 投递结果未知：先查会话历史或实际产物再决定，这里只登记
      report.unknownDeliveries.push({ messageId: msg.id, taskId: msg.taskId, action: "needs_check" });
    }
  }
  return report;
}
