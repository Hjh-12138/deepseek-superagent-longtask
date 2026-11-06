/**
 * DSH 通用长任务插件 · 入口。
 *
 * Cordis 插件约定（与本机 SDK 0.2.1-rc.5 的 dsh-goal-round-driver 一致）：
 * 导出 name / inject / apply；生成器体保持同步，异步初始化在 ready 内完成，
 * 卸载时等待 ready、停表、释放会话、落盘存储。
 *
 * 阶段 1 审查修正：
 * - P1-3：create 按 SDK 传必填 sessionId，持有 AgentHandle{agent, dispose}；
 *   followup 在 handle.agent 上调用；卸载时释放全部会话。
 * - ctx.provide("longtask") 暴露插件服务API；user-entry注册面向用户会话的正式操作工具。
 * - P1-6：taskSessions 持久化真实 sessionId；session/event 按真实 id 归属；
 *   inspectTurn 解析 <longtask_report> 结构化报告；验收会话裁决
 *   （<longtask_verdicts>）被消费并进入 recordVerdicts。
 * - P1-7：agent/pre-step 任务级预算闸门——任务会话余额不足时拒绝新模型步骤；
 *   assistant/message 的 usage 用于结算在途预留。
 * - P2-12：Watchdog 纠偏经 TaskService 投递，纠偏计数单独累加。
 */

import { appendFileSync, existsSync, mkdirSync, readFileSync, statSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { createUserMessage, lastAssistantStreamChunk, type AssistantStreamRecord, type UserMessage } from "@deepseek-ai/dsh-llm";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";
import { JsonStore } from "./store/store.js";
import { TokenLedger, normalizeUsage } from "./ledger/token-ledger.js";
import {PriceTable, DEFAULT_PRICES, formatCny, type PriceCard} from "./ledger/money.js";
import {
  TaskService,
  type HostBridge,
  type SessionLease,
  type TaskSessionRecord,
  type TurnContext,
} from "./tasks/task-service.js";
import { Watchdog, recoverOnStartup } from "./watchdog/watchdog.js";
import {
  normalizeAdmission,
  boundRequestInput,
  hasTrustedBound,
  requestCostFor,
  requestAdmissionCost,
  planAcceptance,
  type AdmissionPolicy,
} from "./budget/admission.js";
import { renderProbeReport, runDeferredProbe, type ProbeReport } from "./probe/probe.js";
import { scanCatalog, loadExpertDefinition } from "./catalog/scanner.js";
import type { AgentHandle, HostContext, SessionEvent } from "./host-types.js";
import type { ExpertRef, Task, Verdict, LedgerEntry } from "./types.js";
import { registerUserTaskTools, USER_TASK_TOOLS, USER_TASK_ENTRY_PROTOCOL } from "./user-entry.js";
import {RouterNotices, ROUTER_NOTICE_PROTOCOL} from "./router-notices.js";
import {ApprovalWaits} from "./approval-waits.js";

export const name = "longtask";

/** 依赖的宿主服务 */
export const inject = ["agents", "sessions", "tools"];
const ACCEPTOR_READ_TOOLS = ["read", "glob", "grep", "ls", "working_directory"];

// 加载标记：模块被宿主 import 时即写入（同步、幂等追加）。
// 写到模块自身相对的确定路径，不依赖宿主 cwd（GUI 应用 cwd 不可控）。
// 用于联调时区分"插件未被加载"与"插件已加载但 effect 失败"。
try {
  appendFileSync(
    join(fileURLToPath(new URL("../../.longtask/load-marker.log", import.meta.url))),
    `${new Date().toISOString()} import cwd=${process.cwd()}\n`,
    "utf8",
  );
} catch {
  // 只读环境忽略
}

export interface LongtaskConfig {
  /** Exact provider/model prices. Missing/expired prices reject monetary requests. */
  modelPrices: PriceCard[];
  dataDir: string;
  agencyAgentsRoot: string;
  probeOnStart: boolean;
  probeInitialDelayMs: number;
  probeRetryDelayMs: number;
  probeMaxAttempts: number;
  probeCreateTimeoutMs: number;
  probeIdleTimeoutMs: number;
  watchdogIntervalMs: number;
  stallThresholdMs: number;
  /** Worker/验收会话使用的供应商路由（空则用宿主默认） */
  workerProvider: string;
  /** Worker/验收会话模型 ID（空则用宿主默认；实测不填会导致轮次不启动） */
  workerModel: string;
  /** 会话工作目录（绝对路径，决定 workspace-write 沙箱的写入根） */
  workerCwd: string;
  /** 单次请求最大输出 Token */
  workerMaxTokens: number;
  /** Worker 会话的父 Agent 策略：none=独立根会话；auto=挂到首个用户会话下（用于继承发起方上下文） */
  workerParentAgent: "none" | "auto";
  /** 会话能力集（agent preset）。空字符串 = 不指定，只拿到宿主全局工具 */
  workerPreset: string;
  /**
   * 验收会话的能力集（P2-9）。空 = 与执行者相同。
   * 宿主 CreateAgentOptions 没有权限字段，只读只能靠这里指定一个只读能力集实现；
   * 未指定时验收提示词会如实说明"只读是纪律要求，不是技术拦截"。
   */
  acceptorPreset: string;
  /** 独立验收工具白名单：拒绝shell及所有非只读工具，与宿主preset无关。 */
  acceptorReadOnly: boolean;
  /** 首次请求的输入 Token 估算（尚无实测值时使用） */
  preStepInputEstimateTokens: number;
  /** 输入估算安全系数：按实测值上浮，覆盖工具定义/系统提示波动 */
  preStepInputSafetyFactor: number;
  /** 验收会话最多允许的模型步骤数，超过则暂停等待人工 */
  acceptorMaxSteps: number;
  /** 执行会话最多允许的模型步骤数，超过则暂停等待人工（缺省 24） */
  executorMaxSteps: number;
  /**
   * 预算收尾阈值（0~1，0 表示关闭）：已用额度达到该比例时，给执行者下一次"停止探索、
   * 落盘、给报告"的收尾指令（每任务版本一次）。给验收留出额度，避免"预算没超、活儿交不出去"。
   */
  wrapUpAtBudgetRatio: number;
  /**
   * 自适应收尾：扣除下一请求及验收保留后，预算还能覆盖的探索步数，
   * 或执行步数上限的剩余步数，不超过此值时提前收尾；0关闭这两个提前触发条件。
   */
  wrapUpMinStepsLeft: number;
  /**
   * 读/检索类工具调用上限：超过后一次性下达"停止读取、转入只写"的指令。
   * 每读一个文件，工具结果永久留在上下文里，后续每一步都为它付费。
   */
  toolReadBudget: number;
  /** 验收预留最低值；实际按验收正文、产物读取、结论请求规划。0显式关闭。 */
  acceptanceReserveTokens: number;
  /** 同时运行的任务上限（阶段3）：超出按优先级排队，0 表示不限制 */
  maxConcurrentTasks: number;
}

export const defaultConfig: LongtaskConfig = {
  modelPrices: DEFAULT_PRICES,
  dataDir: process.env.DSH_LONGTASK_DATA_DIR || join(homedir(), ".dsh", "longtask"),
  agencyAgentsRoot: process.env.DSH_LONGTASK_EXPERTS_ROOT || join(homedir(), ".dsh", "agency-agents"),
  probeOnStart: true,
  probeInitialDelayMs: 8000,
  probeRetryDelayMs: 5000,
  probeMaxAttempts: 4,
  probeCreateTimeoutMs: 20_000,
  probeIdleTimeoutMs: 45_000,
  watchdogIntervalMs: 60_000,
  stallThresholdMs: 15 * 60_000,
  workerProvider: "deepseek-official",
  workerModel: "deepseek-flash",
  workerCwd: process.env.DSH_LONGTASK_WORKSPACE || join(homedir(), "Documents", "deepseek-harness", "default-workspace"),
  workerMaxTokens: 8000,
  workerParentAgent: "none",
  workerPreset: "standard",
  acceptorPreset: "",
  acceptorReadOnly: false, // 兼容旧profile；正式bundle显式启用。
  preStepInputEstimateTokens: 12_000,
  preStepInputSafetyFactor: 1.25,
  acceptorMaxSteps: 8,
  executorMaxSteps: 24,
  wrapUpAtBudgetRatio: 0.65,
  wrapUpMinStepsLeft: 3,
  toolReadBudget: 6,
  acceptanceReserveTokens: 26_000,
  maxConcurrentTasks: 2,
};

// ---------------------------------------------------------------------------
// 结构化报告解析（P1-6：可校验的轮次结果提取）
// ---------------------------------------------------------------------------

export interface WorkerReport {
  done: boolean;
  next: string[];
  waiting: string | null;
}

/**
 * 取出正文里最后一个「载荷以 { 开头」且能解析的协议块。
 *
 * 不能用"第一个开标签配第一个闭标签"：交接文本里常常先提到一句 `<longtask_report>`
 * （不带闭标签），按首个开标签匹配会把后面几百字散文当成 JSON，解析失败 → 整轮被当成
 * "未声明完成" → 任务静默停摆（联调实测踩过）。改成：
 * - 载荷必须紧接 `{`：散文里的提及天然被排除；
 * - 取最后一个可解析块：模型若引用了模板示例（done:false），以最后给出的真实结论为准。
 */
function lastProtocolPayload(text: string, tag: string): string | null {
  const re = new RegExp(`<${tag}>\\s*(\\{[\\s\\S]*?\\})\\s*</${tag}>`, "g");
  let payload: string | null = null;
  for (const match of text.matchAll(re)) {
    const candidate = match[1];
    if (!candidate) continue;
    try {
      JSON.parse(candidate);
      payload = candidate; // 只保留最后一个能解析的
    } catch {
      // 不是合法 JSON：可能是正文引用的片段，跳过
    }
  }
  return payload;
}

export function parseWorkerReport(assistantText: string | null): WorkerReport | null {
  if (!assistantText) return null;
  const payload = lastProtocolPayload(assistantText, "longtask_report");
  if (!payload) return null;
  try {
    const raw = JSON.parse(payload) as Record<string, unknown>;
    return {
      done: raw["done"] === true,
      next: Array.isArray(raw["next"]) ? (raw["next"] as unknown[]).map(String) : [],
      waiting: typeof raw["waiting"] === "string" ? raw["waiting"] : null,
    };
  } catch {
    return null;
  }
}

export interface AcceptorReport {
  roundId: string;
  verdicts: Verdict[];
  artifactHashes: Record<string, string>;
}

/**
 * 执行专家之间的直连协议（阶段2）。
 * 模型在回复里写一个 `<longtask_message to="..." summary="...">正文</longtask_message>`，
 * 插件解析后落记录并把正文只发给收件人；summary 进 Router 摘要视图。
 * 这样 Router 不必转发全文，也不需要给会话额外装工具。
 */
export interface ExpertMessageBlock {
  to: string;
  body: string;
  summary: string | null;
  /** 显式声明的产物路径（逗号或竖线分隔）：先核验登记，再随消息交接 */
  artifacts: string[];
}

export function parseExpertMessages(text: string | null): ExpertMessageBlock[] {
  if (!text) return [];
  const blocks: ExpertMessageBlock[] = [];
  const re = /<longtask_message\b([^>]*)>([\s\S]*?)<\/longtask_message>/g;
  for (const match of text.matchAll(re)) {
    const attrs = match[1] ?? "";
    const body = (match[2] ?? "").trim();
    const to = /to\s*=\s*"([^"]+)"/.exec(attrs)?.[1]?.trim();
    if (!to || !body) continue;
    const summary = /summary\s*=\s*"([^"]*)"/.exec(attrs)?.[1]?.trim() ?? null;
    const rawArtifacts = /artifacts\s*=\s*"([^"]*)"/.exec(attrs)?.[1] ?? "";
    const artifacts = rawArtifacts
      .split(/[|,]/)
      .map((a) => a.trim())
      .filter((a) => a.length > 0);
    blocks.push({ to, body, summary: summary && summary.length > 0 ? summary : null, artifacts });
  }
  return blocks;
}

export function parseAcceptorVerdicts(assistantText: string | null): AcceptorReport | null {
  if (!assistantText) return null;
  const payload = lastProtocolPayload(assistantText, "longtask_verdicts");
  if (!payload) return null;
  try {
    const raw = JSON.parse(payload) as Record<string, unknown>;
    if (typeof raw["roundId"] !== "string" || !Array.isArray(raw["verdicts"])) return null;
    return {
      roundId: raw["roundId"],
      verdicts: raw["verdicts"] as Verdict[],
      artifactHashes: (raw["artifactHashes"] ?? {}) as Record<string, string>,
    };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// 生产 HostBridge（真实 SDK 协议）
// ---------------------------------------------------------------------------

interface LiveSession {
  lease: SessionLease;
  handle: AgentHandle;
}

/** SDK 的只读谱系元数据；只用于识别任务后代，不能把父用户会话误归入任务。 */
interface BudgetAgent {
  id?: string;
  session?: { id?: string; header?: { parentSession?: string } };
  cancel?(cause: Record<string, unknown>): void;
}

function createHostBridge(
  ctx: HostContext,
  opts: {
    cwd: string;
    provider: string;
    model: string;
    maxTokens: number;
    parentMode: "none" | "auto";
    /** 执行者能力集 */
    preset: string;
    /** 验收者能力集（空 = 与执行者相同）；只读是否生效由实际挂载结果判定（P2-9） */
    acceptorPreset?: string;
    acceptorReadOnly?: boolean;
    stepControls: Map<string, UserMessage[]>;
    endedTurns: Set<string>;
    routerSessionId(taskId: string): string | undefined;
  },
  diag: (line: string) => void,
): HostBridge & { disposeAll(): Promise<void> } {
  const agents = ctx.get<{ create(o: Record<string, unknown>): Promise<AgentHandle> }>("agents");
  if (!agents) throw new Error("宿主缺少 agents 服务，插件无法创建 Worker 会话");
  const live = new Map<string, LiveSession>();
  /** 各角色实际挂上的能力集：preset 挂载失败会降级，声称只读前必须看这里 */
  const appliedPreset = new Map<"executor" | "acceptor", string>();
  /** 用户会话识别：宿主铸造的 id 形如 session-<uuid> */
  const pickParentAgent = (taskId: string): unknown => {
    if (opts.parentMode !== "auto") return undefined;
    try {
      const registry = ctx.get<{get(id: string): {id: string} | undefined; list(): Array<{ id: string }> }>("agents");
      const bound = opts.routerSessionId(taskId);
      if (bound) return registry?.get?.(bound); // never parent a bound task to an unrelated live conversation
      const all = registry?.list?.() ?? [];
      const userAgent = all.find((a) => typeof a.id === "string" && a.id.startsWith("session-"));
      if (userAgent) diag(`parent:using ${userAgent.id}`);
      return userAgent;
    } catch {
      return undefined;
    }
  };

  return {
    async createSession(taskId: string, expert: ExpertRef, role: "executor" | "acceptor"): Promise<SessionLease> {
      const sessionId = randomUUID(); // 调用方提供的单一活动身份（CreateAgentOptions.sessionId）
      const parentAgent = pickParentAgent(taskId);
      // 预设必须在 setup 里挂载到 agent 作用域，meta.agentPreset 只是会话标签：
      // agentPresets.mount(agentCtx, id) 才会把 preset 的插件树（含 fs/bash 工具）装进该 agent。
      const createOnce = (preset: string) =>
        agents.create({
          sessionId,
          ...(parentAgent ? { parentAgent } : {}),
          meta: { cwd: opts.cwd, ...(preset ? { agentPreset: preset } : {}) },
          agentOptions: { provider: opts.provider, model: opts.model, maxTokens: opts.maxTokens },
          setup: async (agentCtx: unknown) => {
              if (preset) {
                  const registry = ctx.get<{ mount(c: unknown, id?: string): Promise<{ id: string }> }>("agentPresets");
                  if (!registry?.mount) {
                    diag(`session:preset-registry-missing preset=${preset}`);
                  } else {
                    const bound = await registry.mount(agentCtx, preset);
                    diag(`session:preset-mounted preset=${bound?.id ?? preset}`);
                  }
              }
              // 用户管理工具不进入Worker/验收的模型工具面，避免增加上下文或自行管理任务。
              // Cordis属性访问绑定调用方scope；get("tools")返回原服务，不能用它做scoped限制。
              const scoped = agentCtx as {tools?: { get?(name: string): unknown; restrict?(filter: {deny?: string[]; allow?: string[]}): () => void }};
              const workerTools = scoped.tools;
              const visible = USER_TASK_TOOLS.filter(name => workerTools?.get?.(name));
              if (visible.length && workerTools?.restrict) workerTools.restrict({deny: visible});
              if (role === "acceptor" && opts.acceptorReadOnly && workerTools?.restrict) workerTools.restrict({allow: ACCEPTOR_READ_TOOLS.filter(name => workerTools.get?.(name))});
          },
        });
      let handle: AgentHandle;
      const wantedPreset = role === "acceptor" && opts.acceptorPreset ? opts.acceptorPreset : opts.preset;
      try {
        handle = await createOnce(wantedPreset);
        appliedPreset.set(role, wantedPreset);
      } catch (error) {
        // preset id 不匹配会让创建直接失败；降级为不带 preset，并把事实写进诊断，绝不静默
        const message = String((error as Error)?.message ?? error);
        if (!wantedPreset || !/preset/i.test(message)) throw error;
        diag(`session:preset-fallback want=${wantedPreset} role=${role} reason=${message.slice(0, 200)}`);
        // 降级时退到不带 preset，而不是"换一个能力集"：宁可少能力，也不静默给出错误权限
        handle = await createOnce("");
        appliedPreset.set(role, "");
      }
      diag(
        `session:created key=${`${role}:${taskId}:${expert.relPath}`} id=${handle.agent.id} status=${handle.agent.status} cwd=${opts.cwd} model=${opts.model} preset=${wantedPreset || "(none)"}`,
      );
      const key = `${role}:${taskId}:${expert.relPath}:${sessionId.slice(0, 8)}`;
      live.set(key, { handle, lease: { sessionKey: key, sessionId, dispose: handle.dispose } });
      ctx.logger.info(`longtask: 已创建${role === "acceptor" ? "验收" : "执行"}会话 ${key} -> ${handle.agent.id}`);
      return { sessionKey: key, sessionId, dispose: handle.dispose };
    },
    appliedPreset(role: "executor" | "acceptor"): string | null {
      return appliedPreset.get(role) ?? null;
    },
    async deliver(sessionKey: string, content: string, dedupeKey: string, target: "next-turn" | "next-step" = "next-turn") {
      const session = live.get(sessionKey);
      if (!session) return "failed";
      try {
        const message = createUserMessage({
          content: [{ type: "text", text: content }],
          source: { kind: "longtask", dedupeKey },
        });
        if (target === "next-step") {
          if (opts.endedTurns.has(session.handle.agent.session.id)) {
            diag(`deliver:skipped key=${sessionKey} target=next-step reason=turn-ended`);
            return "failed";
          }
          if (typeof session.handle.agent.steer !== "function") {
            diag(`deliver:unsupported key=${sessionKey} target=${target}（宿主缺少 steer）`);
            return "failed";
          }
          const capturing = opts.stepControls.get(session.handle.agent.session.id);
          if (capturing) capturing.push(message); // inbox 已 claim：并入当前步骤提案，不再排到下一步
          else session.handle.agent.steer(message);
        } else {
          session.handle.agent.followup(message);
        }
        diag(`deliver:ok key=${sessionKey} target=${target === "next-step" && opts.stepControls.has(session.handle.agent.session.id) ? "current-step" : target} status=${session.handle.agent.status} len=${content.length}`);
        return "delivered";
      } catch (error) {
        ctx.logger.warn(`longtask: ${target} 投递失败: ${String((error as Error).message)}`);
        return "unknown"; // 结果未知：恢复器先查会话历史再决定是否重投
      }
    },
    async stop(sessionKey: string) {
      const session = live.get(sessionKey);
      if (!session) return false;
      try {
        session.handle.agent.cancel({ kind: "user" }); // 取消运行中活动并清空排队消息
        return true;
      } catch (error) {
        ctx.logger.warn(`longtask: 停止会话失败 ${sessionKey}: ${String((error as Error).message)}`);
        return false;
      }
    },
    async sessionAlive(sessionKey: string) {
      return live.has(sessionKey);
    },
    async releaseSession(sessionKey: string) {
      const session = live.get(sessionKey);
      if (!session) return;
      live.delete(sessionKey);
      try {
        await session.handle.dispose();
      } catch (error) {
        ctx.logger.warn(`longtask: 释放会话失败 ${sessionKey}: ${String((error as Error).message)}`);
      }
    },
    async disposeAll() {
      for (const [key] of live) await this.releaseSession(key);
    },
  };
}

// ---------------------------------------------------------------------------
// apply
// ---------------------------------------------------------------------------

export function apply(ctx: HostContext, options: Partial<LongtaskConfig> = {}): void {
  const cfg: LongtaskConfig = { ...defaultConfig, ...options };
  // 诊断日志：真实桌面宿主里 ctx.logger 不可观测，把初始化阶段与异常栈落盘到 plugin.log。
  // 落点跟随 dataDir（而不是模块相对路径），否则测试/多次运行会互相污染同一份日志，
  // 出事时"日志里到底是谁的"都说不清。
  const diagFile = (() => {
    try {
      const base = cfg.dataDir && cfg.dataDir.trim() ? resolve(cfg.dataDir) : fileURLToPath(new URL("../../.longtask", import.meta.url));
      return join(base, "plugin.log");
    } catch {
      return fileURLToPath(new URL("../../.longtask/plugin.log", import.meta.url));
    }
  })();
  const diag = (line: string) => {
    try {
      mkdirSync(dirname(diagFile), { recursive: true });
      appendFileSync(diagFile, `${new Date().toISOString()} [${process.pid}] ${line}\n`, "utf8");
    } catch {
      // 诊断不可用时静默，不影响主流程
    }
  };
  diag(`apply-entered probeOnStart=${cfg.probeOnStart} dataDir=${cfg.dataDir}`);
  // 凭证诊断：provider=deepseek-official 走 DEEPSEEK_API_KEY；缺失会让请求在传输层失败
  diag(
    `env:DEEPSEEK_API_KEY=${process.env["DEEPSEEK_API_KEY"] ? "set" : "missing"} ` +
      `DEEPSEEK_BASE_URL=${process.env["DEEPSEEK_BASE_URL"] ? "set" : "missing"} ` +
      `keys=${Object.keys(process.env).length}`,
  );
  // apply 入口标记：与模块加载标记配合，区分"未导入"与"apply 中途失败"。
  try {
    appendFileSync(
      join(fileURLToPath(new URL("../../.longtask/load-marker.log", import.meta.url))),
      `${new Date().toISOString()} apply-entered probeOnStart=${cfg.probeOnStart}\n`,
      "utf8",
    );
  } catch {
    // 只读环境忽略
  }


  const lifecycle = function* () {
    diag("stage:effect-body");
    const disposers: Array<() => void | Promise<void>> = [];
    /** 本次 apply 的实例标识：热重载时新旧实例共存，探针报告必须能分辨来源 */
    const instanceId = randomUUID().slice(0, 8);
    let runtimeReady = false;
    let userEntryReady = false;
    /** 卸载信号：旧实例的延迟探针必须随卸载取消，否则会用过期结论污染 probe-latest */
    const probeController = new AbortController();
    const ready = (async () => {
      diag(`stage:ready-start instance=${instanceId}`);
      const { writeFile, mkdir } = await import("node:fs/promises");
      await mkdir(cfg.dataDir, { recursive: true });
      diag("stage:dataDir-ready");
      const saveReport = async (report: ProbeReport, label: string) => {
        // 已卸载：不再写任何报告（否则旧实例会覆盖最新能力结论）
        if (probeController.signal.aborted) return;
        diag(`probe:save ${label} ${cfg.dataDir} instance=${report.instanceId}`);
        const stamp = report.probedAt.replace(/[:.]/g, "-");
        await writeFile(join(cfg.dataDir, `probe-${label}-${stamp}.md`), renderProbeReport(report), "utf8");
        await writeFile(join(cfg.dataDir, `probe-${label}-${stamp}.json`), JSON.stringify(report, null, 2), "utf8");
        diag(`probe:saved ${label} instance=${report.instanceId}`);
      };

      // ---- 阶段 0：组合就绪后延迟探针 ----
      // 注意：探针绝不能出现在关键路径上——它内部可能挂起（宿主调用无返回），
      // 一旦 await 在这里，store/tasks/ctx.provide 将全部不可用。故放到最后 fire-and-forget。
      const startProbe = () => {
        diag(`probe:scheduled enabled=${cfg.probeOnStart} delay=${cfg.probeInitialDelayMs} instance=${instanceId}`);
        if (!cfg.probeOnStart) return;
        void (async () => {
          try {
            const report = await runDeferredProbe(ctx, saveReport, {
              initialDelayMs: cfg.probeInitialDelayMs,
              retryDelayMs: cfg.probeRetryDelayMs,
              maxAttempts: cfg.probeMaxAttempts,
              instanceId,
              signal: probeController.signal,
              timeouts: {
                createTimeoutMs: cfg.probeCreateTimeoutMs,
                idleTimeoutMs: cfg.probeIdleTimeoutMs,
                onProgress: (stage) => diag(`probe:${stage} instance=${instanceId}`),
              },
            });
            if (!report) {
              diag(`probe:cancelled instance=${instanceId}（实例已卸载，停止探测且不写报告）`);
              return;
            }
            if (report.blockers.length > 0) {
              ctx.logger.warn(`longtask: 探针重试后仍有阻塞点（能力缺失，任务不会误标为已运行）：\n- ${report.blockers.join("\n- ")}`);
            } else {
              ctx.logger.info("longtask: 宿主探针通过，无阻塞点");
            }
          } catch (error) {
            diag(`PROBE-ERROR ${String((error as Error)?.stack ?? error)}`);
            ctx.logger.error(`longtask: 探针执行异常（不影响插件其余能力）：${String((error as Error).message)}`);
          }
        })();
      };

      // ---- 会话状态：每个任务会话的最新助手文本与最近预留 ----
      const lastAssistantText = new Map<string, string>();
      /**
       * 本轮累积的助手文本（按 turn 自动重置）。
       * 只保留"最后一条"会丢报告：模型可能在中间步骤给出结论，随后又补一句收尾说明。
       */
      const turnTexts = new Map<string, { turn: number; text: string }>();
      const currentTurnText = (sessionKey: string): string | undefined => turnTexts.get(sessionKey)?.text;
      const lastReservation = new Map<string, string>(); // sessionId -> reservationId
      /** 在途预留的落盘 Promise：用量回传可能早于预留落账，结算必须等它 */
      const reserveInFlight = new Map<string, Promise<void>>();
      const settlementInFlight = new Map<string, Promise<void>>();
      const accountingBlocked = new Set<string>();
      const seenUsageEvents = new Set<string>();
      const seenUsageObjects = new WeakSet<object>();
      const requestStarted = new Set<string>();
      const seriesInput = new Map<string, number>();
      const blockAccounting = (record: TaskSessionRecord, reason: string) => {
        accountingBlocked.add(record.taskId); // 先同步关闸，异步持久化期间也不能请求或写工具。
        const pending = (async () => {
          await ledger.blockAccounting(record.taskId, record.sessionId, reason);
          await tasks.pause(record.taskId, "budget", reason);
        })();
        settlementInFlight.set(record.sessionId, pending);
        void pending.catch(error => ctx.logger.error(`longtask: 用量异常落盘失败，内存闸门保持关闭：${String(error)}`));
      };
      const stepControls = new Map<string, UserMessage[]>();
      const endedTurns = new Set<string>();
      /** 会话已准入的模型步数（验收步数上限用） */
      const stepCounts = new Map<string, number>();
      /** 会话实测输入量（上一次请求的真实输入），作为下一次准入的实测链条基准 */
      const lastInputTokens = new Map<string, number>();
      /** 会话实测输出量：它会原样进入下一轮上下文，是输入上界的一部分（P1-1） */
      const lastOutputTokens = new Map<string, number>();
      /** 本次请求准入时预留的额度（上界+输出上限），用于事后核对上界是否被击穿 */
      const lastAdmissionNeed = new Map<string, number>();
      /** 每任务最近一步的实耗（含缓存分项）：自适应收尾用它估算"还能走几步" */
      const lastStepCost = new Map<string, number>();
      /** 每会话读/检索类工具调用次数（读预算） */
      const toolReadCounts = new Map<string, number>();
      // 尚未包含在上次实测输入中的 surface 增量。普通工具结果直接进入历史，
      // 不经 pre-step.messages；必须独立累计，直到下一次带 usage 的模型响应建立新基线。
      const surfaceInputBytes = new Map<string, number>();
      const surfaceEventKeys = new Map<string, Set<string>>();
      const inputSize = (value: unknown): number => {
        const json = JSON.stringify(value);
        // UTF-8 字节数比 JS 字符数更保守；连同 role/callId 等消息结构一起计入。
        return json === undefined ? 0 : Buffer.byteLength(json, "utf8");
      };

      // ---- inspectTurn：结构化报告 → TurnContext（P1-6） ----
      const inspectTurn = async (sessionKey: string): Promise<TurnContext> => {
        // 整轮文本一起解析：报告可能出现在中间步骤，只读最后一条会漏
        const report = parseWorkerReport(currentTurnText(sessionKey) ?? lastAssistantText.get(sessionKey) ?? null);
        const lease = tasks.listTasks().flatMap(t => tasks.getSessions(t.id)).find(s => s.sessionKey === sessionKey);
        const jobs = lease ? tasks.pendingBackgroundJobs(lease.taskId, lease.relPath) : [];
        const activeJobs = lease ? tasks.activeBackgroundJobs(lease.taskId, lease.relPath) : [];
        const waiting = report?.waiting ?? null;
        // 只有真实 job 生命周期可以证明后台命令仍在运行；未知说明不伪装成可靠专家等待。
        const expertWait = waiting !== null && /专家|交接|等待\s*[A-Z]|等\s*[A-Z]|expert|reply/i.test(waiting);
        return {
          status: "active",
          reportedNextSteps: report?.next ?? [],
          claimedDone: report?.done ?? false,
          activeCommand: activeJobs.length > 0,
          pendingBackgroundResults: jobs.some(id => !activeJobs.includes(id)),
          backgroundJobIds: jobs,
          pendingReply: expertWait,
          unhandledEvent: waiting !== null && !expertWait && jobs.length === 0,
          canContinueTruncated: Boolean(lease && requestStarted.has(lease.sessionId) && (lastOutputTokens.get(lease.sessionId) ?? 0) > 0
            && !lastReservation.has(lease.sessionId) && !ledger.accountingIssue(lease.taskId)),
        };
      };

      // ---- 核心服务组装 ----
      const store = await JsonStore.load(join(cfg.dataDir, "state.json"));
      diag("stage:store-loaded");
      const ledger = new TokenLedger(store, () => new Date(), new PriceTable(cfg.modelPrices), {provider: cfg.workerProvider, model: cfg.workerModel});
      const host = createHostBridge(
    ctx,
    {
      cwd: cfg.workerCwd,
      provider: cfg.workerProvider,
      model: cfg.workerModel,
      maxTokens: cfg.workerMaxTokens,
      parentMode: cfg.workerParentAgent,
      preset: cfg.workerPreset,
      acceptorPreset: cfg.acceptorPreset,
      acceptorReadOnly: cfg.acceptorReadOnly,
      stepControls,
      endedTurns,
      routerSessionId: taskId => store.get<Task>("tasks", taskId)?.routerSessionId,
    },
    diag,
  );
      diag("stage:host-bridge");
      const expertCache = new Map<string, string>();
      const loadExpert = async (ref: ExpertRef): Promise<string> => {
        const cached = expertCache.get(ref.relPath);
        if (cached !== undefined) return cached;
        const def = await loadExpertDefinition(cfg.agencyAgentsRoot, ref);
        if (!def.hashMatches) {
          ctx.logger.warn(`longtask: 专家 ${ref.relPath} 定义已变化（快照 ${ref.contentHash.slice(0, 8)} ≠ 当前 ${def.currentHash.slice(0, 8)}），使用当前版本并记录`);
        }
        expertCache.set(ref.relPath, def.text);
        return def.text;
      };
      // ---- 预算准入策略（唯一口径）：派工闸门 / 恢复 / 请求前准入共用 ----
      const admission = normalizeAdmission({
        baselineInputTokens: cfg.preStepInputEstimateTokens,
        inputSafetyFactor: cfg.preStepInputSafetyFactor,
        maxOutputTokens: cfg.workerMaxTokens,
      });
      const watchdog = new Watchdog(store, { stallThresholdMs: cfg.stallThresholdMs, maxCorrections: 2 });
      const tasks = new TaskService({
        store,
        ledger,
        host,
        loadExpert,
        inspectTurn,
        admission,
        maxConcurrentTasks: cfg.maxConcurrentTasks,
        onInstruction: (id, content) => watchdog.recordInstruction(id, content),
        onSubstantiveProgress: (id, note) => watchdog.recordSubstantiveProgress(id, note),
        acceptorMaxSteps: cfg.acceptorMaxSteps,
        executorMaxSteps: cfg.executorMaxSteps,
        // 只读是否真的生效由"实际挂上的能力集"判定：只有在显式配置了验收能力集、
        // 且它确实是另一个能力集且挂载成功时，才允许提示词声称"工具层拦截"（P2-9）
        readOnlyAcceptor: () => {
          if (cfg.acceptorReadOnly) return delegationGuardReady;
          const applied = host.appliedPreset?.("acceptor") ?? null;
          return Boolean(cfg.acceptorPreset) && applied === cfg.acceptorPreset && cfg.acceptorPreset !== cfg.workerPreset;
        },
        // 交付物按 dataDir 解析绝对路径后比对完整路径，而不是只看文件名（P1-6）
        resolveDeliverable: (p: string) => (isAbsolute(p) ? resolve(p) : resolve(cfg.dataDir, p)),
        fileExists: (abs: string) => existsSync(abs),
        fileSize: (abs: string) => { try { return statSync(abs).size; } catch { return null; } },
        // 验收结算前回算磁盘哈希，确认产物仍等于冻结版本（P1-5）
        verifyArtifact: async (location: string, expected: string) => {
          try {
            const { createHash } = await import("node:crypto");
            const { readFile } = await import("node:fs/promises");
            const actual = createHash("sha256").update(await readFile(location)).digest("hex");
            return actual === expected;
          } catch {
            return false; // 读不出 = 无法证明仍匹配，按不匹配处理
          }
        },
      });
      if (USER_TASK_ENTRY_PROTOCOL !== 3 || typeof store.subscribe !== "function" || typeof tasks.bindRouter !== "function" || tasks.moneyConfirmationProtocol !== 1 || ledger.moneyAccountingProtocol !== 1 || ledger.protectedReserveVersion !== 1 || ledger.requestAccountingProtocol !== 1 || tasks.userConfirmationProtocol !== 1 || tasks.userActionsProtocol !== 1 || typeof tasks.getExecutorDependency !== "function" || typeof tasks.noteArtifactWrite !== "function" ||
          typeof tasks.getBackgroundJobs !== "function" || typeof tasks.reconcileClarificationAnswer !== "function" || typeof tasks.getTaskEvidence !== "function" || typeof tasks.getRequiredTools !== "function" || typeof tasks.estimateAcceptanceInput !== "function") {
        throw new Error("插件模块版本不一致，请完整退出并重启 DSH 后加载新版本");
      }
      diag(`limits:configured executorMaxSteps=${cfg.executorMaxSteps} acceptorMaxSteps=${cfg.acceptorMaxSteps} wrapUpAtBudgetRatio=${cfg.wrapUpAtBudgetRatio} wrapUpMinStepsLeft=${cfg.wrapUpMinStepsLeft} acceptanceReserveTokens=${cfg.acceptanceReserveTokens}`);

      // 阶段一只允许插件登记的执行者/验收者发起模型请求，禁止绕过账本自行委派。
      // guard 在所有 pre-execute 策略之后、工具正文之前执行，allow 不能覆盖它。
      const agents = ctx.get<{ get(id: string): BudgetAgent | undefined }>("agents");
      const parentLinks = new Map<string, string>();
      const taskOwner = (agent: BudgetAgent | undefined): TaskSessionRecord | undefined => {
        let current = agent;
        let id = current?.session?.id ?? current?.id;
        const seen = new Set<string>();
        while (id && !seen.has(id)) {
          seen.add(id);
          const owner = tasks.findSessionBySessionId(id);
          if (owner) return owner;
          const parent = current?.session?.header?.parentSession ?? parentLinks.get(id);
          if (!parent) return undefined;
          parentLinks.set(id, parent);
          id = parent;
          current = agents?.get(id);
        }
        return undefined;
      };
      const tools = ctx.get<{ guard?(fn: (exec: { name: string; agent?: BudgetAgent }) => string | undefined): () => void }>("tools");
      let delegationGuardReady = false;
      if (typeof tools?.guard === "function") {
        const blocked = new Set(["subagent", "spawn_teammate", "workflow", "ralph", "send_message"]);
        const disposeGuard = tools.guard((exec) => {
          const owner = taskOwner(exec.agent);
          if (!owner) return undefined; // 用户会话及其普通子会话不受影响
          if (accountingBlocked.has(owner.taskId) || ledger.accountingIssue(owner.taskId)) return "任务用量尚未核对，拒绝继续执行工具";
          if (cfg.acceptorReadOnly && owner.role === "acceptor" && !ACCEPTOR_READ_TOOLS.includes(exec.name)) return "独立验收只允许读取和检索；写入、shell及其他工具均被拒绝";
          if ((USER_TASK_TOOLS as readonly string[]).includes(exec.name)) return "任务管理入口只供用户会话使用，执行和验收专家不能操作";
          const id = exec.agent?.session?.id ?? exec.agent?.id;
          // 撤权会话（被替换的执行者）：即使宿主仍在跑，也不得执行任何工具
          if (id) {
            const rec = tasks.findSessionBySessionId(id);
            if (rec?.revokedAt) {
              diag(`authority:tool-blocked task=${rec.taskId} rel=${rec.relPath} tool=${exec.name}`);
              return "该执行专家已被替换，不能继续执行任务工具；请结束本轮并交回任务负责人。";
            }
            if (rec?.role === "executor" && tasks.getExecutorDependency(rec.taskId, rec.relPath).state === "waiting") {
              return "前置交接尚未成功送达；当前执行者只能等待，不得读写文件或提交完成。";
            }
          }
          if (id !== owner.sessionId || blocked.has(exec.name)) {
            diag(`delegation:blocked task=${owner.taskId} session=${id} tool=${exec.name}`);
            return "longtask 阶段一禁止未托管委派：子 Agent/工作流未纳入任务预算，请在当前会话完成交付。";
          }
          return undefined;
        });
        disposers.push(disposeGuard);
        delegationGuardReady = true;
        diag("delegation:guard-ready");
      } else {
        diag("delegation:guard-unavailable（任务模型步骤将拒绝，防止未托管委派绕过预算）");
      }

      const approvalWaits = new ApprovalWaits(store, message => ctx.logger.warn(`longtask: ${message}`));
      disposers.push(() => approvalWaits.dispose());
      for (const id of await approvalWaits.restore()) await tasks.pause(id, "missing_input", "运行时重载后原工具审批已失效；请核对Worker会话，再由用户恢复并重新处理宿主审批。未重放工具操作。", true);

      // ---- 重启恢复：先核对再动作 ----
      const recovery = await recoverOnStartup(store, host, ledger);
      diag(`stage:recovery kept=${recovery.kept.length} reactivated=${recovery.reactivated.length}`);
      // 阶段3：恢复只做分类与对账；真正的恢复派工延后到准入/事件监听装好之后（见 ready 末尾）
      const restartTargets = new Set<string>([
        ...recovery.reactivated,
        ...recovery.brokenSessions.map((s) => s.taskId),
      ]);
      ctx.logger.info(
        `longtask: 恢复检查完成 —— 可续 ${recovery.reactivated.length}；保持 ${recovery.kept.length}；投递未知 ${recovery.unknownDeliveries.length}；未结算预留 ${recovery.unsettledReservations}（已对账 ${recovery.reconciledReservations}）`,
      );

      // ---- 专家目录检索（P1-4：Router 选人入口的数据面）----
      let catalogCache: Awaited<ReturnType<typeof scanCatalog>> | null = null;
      const searchExperts = async (query: string) => {
        if (!catalogCache) catalogCache = await scanCatalog(cfg.agencyAgentsRoot, join(cfg.agencyAgentsRoot, "divisions.json"));
        const q = query.toLowerCase();
        return catalogCache.experts
          .filter((e) => e.name.toLowerCase().includes(q) || e.intro.toLowerCase().includes(q) || e.division.includes(q))
          .slice(0, 20);
      };

      // ---- 用户任务入口（P1-4）：ctx.provide 暴露同一 TaskService ----
      const observedExecutorEnds = new Map<string, { turnId: string; session: { id: string }; event: SessionEvent }>();
      const api = {
        propose: (input: Parameters<TaskService["propose"]>[0]) => tasks.propose(input),
        confirm: (taskId: string, inheritedSettledTokens?: number) => tasks.confirm(taskId, inheritedSettledTokens),
        proposeNewVersion: (taskId: string, patch: Parameters<TaskService["proposeNewVersion"]>[1]) => tasks.proposeNewVersion(taskId, patch),
        pause: (taskId: string, note: string) => tasks.pause(taskId, "user", note),
        resume: (taskId: string) => tasks.resume(taskId),
        grantBudget: (taskId: string, tokens: number, note: string, grantId: string) => tasks.grantBudget(taskId, tokens, note, grantId),
        cancel: (taskId: string) => tasks.cancel(taskId),
        getTask: (taskId: string) => tasks.getTask(taskId),
        listTasks: () => tasks.listTasks(),
        getRuntimeInfo: () => ({ stage3Protocol: 2, budgetProtectionVersion: ledger.protectedReserveVersion, ready: runtimeReady, maxConcurrentTasks: cfg.maxConcurrentTasks,
          userEntryProtocol: USER_TASK_ENTRY_PROTOCOL, moneyAccountingProtocol: ledger.moneyAccountingProtocol, routerNoticeProtocol: ROUTER_NOTICE_PROTOCOL, approvalWaitProtocol: 1, userActionsProtocol: tasks.userActionsProtocol, requestAccountingProtocol: ledger.requestAccountingProtocol, acceptorReadOnly: cfg.acceptorReadOnly, userEntryReady,
          stallThresholdMs: cfg.stallThresholdMs, watchdogIntervalMs: cfg.watchdogIntervalMs,
          pid: process.pid, bootAt: Math.round((Date.now() - process.uptime() * 1000) / 1000), instanceId }),
        getTaskSessions: (taskId: string) => structuredClone(tasks.getSessions(taskId)),
        getTaskEvidence: (taskId: string) => tasks.getTaskEvidence(taskId),
        /** 预算快照（可用/已结算/已预留限量）：供调用方核对"固定预算内"是否真的守住 */
        getBudget: (taskId: string) => ledger.getBudget(taskId),
        getMoneyBudget: (taskId: string) => ledger.moneyView(taskId),
        getModelExpenses: (taskId: string) => ledger.moneyEntries(taskId),
        /** 待结算验收轮次的检查点（验收进度），无则 undefined */
        getAcceptanceProgress: (taskId: string) => tasks.latestPendingRound(taskId)?.checkpoint,
        searchExperts,
        registerArtifact: (taskId: string, location: string, contentHash: string | null, ownerRelPath: string) =>
          tasks.registerArtifact(taskId, location, contentHash, ownerRelPath),
        // ---- 阶段 2：专家协作 ----
        /** 执行专家之间的定向消息：先落记录再通知收件人，重复触发不重复投递 */
        sendExpertMessage: (
          taskId: string,
          fromRelPath: string,
          toRelPath: string,
          content: string,
          options?: Parameters<TaskService["sendExpertMessage"]>[4],
        ) => tasks.sendExpertMessage(taskId, fromRelPath, toRelPath, content, options),
        /** 交接摘要：关联原消息，只给结论与产物引用（Router 不必读全文） */
        recordHandoffSummary: (
          taskId: string,
          input: Parameters<TaskService["recordHandoffSummary"]>[1],
        ) => tasks.recordHandoffSummary(taskId, input),
        getExpertMessage: (taskId: string, messageId: string) => tasks.getExpertMessage(taskId, messageId),
        reconcileExpertMessage: (...args: Parameters<TaskService["reconcileExpertMessage"]>) => tasks.reconcileExpertMessage(...args),
        getExecutorDependency: (taskId: string, relPath: string) => tasks.getExecutorDependency(taskId, relPath),
        /** 只重放已观测的本任务事件到 longtask 消费入口；不广播给宿主持久化/投影。 */
        replayExecutorTurnEnd: (taskId: string, sessionId: string, sourceTurnId: string): boolean => {
          const record = tasks.findSessionBySessionId(sessionId);
          const task = tasks.getTask(taskId);
          const observed = observedExecutorEnds.get(sessionId);
          if (!record || record.taskId !== taskId || record.role !== "executor" || record.revokedAt
            || !task || !["active", "rework"].includes(task.status) || observed?.turnId !== sourceTurnId) {
            throw new Error("只能重放仍在执行的任务中已观测到的当前执行者结束事件");
          }
          collaboration(record, "event_replayed", { sourceTurnId });
          onSessionEvent(observed.session, structuredClone(observed.event));
          return true;
        },
        /** 变更执行人选：留下理由/未完成项/交接对象；验收专家变更需用户确认 */
        changeExecutor: (
          taskId: string,
          change: { add: ExpertRef; remove?: string; reason: string; pendingItems?: string[]; handoffTo?: string },
        ) => tasks.changeExecutor(taskId, change),
        /** Router 摘要视图：目标/状态/人选/摘要/产物引用，不含专家全文 */
        getRouterDigest: (taskId: string) => tasks.routerDigest(taskId),
        /** 阶段3：当前等待（含历史状态），供诊断与界面读取 */
        listWaits: (taskId: string) => tasks.listWaits(taskId),
        /** 阶段3：手动触发一次过期等待清理（恢复流程也会自动调用） */
        sweepWaits: (taskId?: string) => tasks.sweepStaleWaits(taskId),
        /** 阶段3：调整优先级（数值越大越先运行） */
        setTaskPriority: (taskId: string, priority: number) => tasks.setTaskPriority(taskId, priority),
        /** 阶段3：按并发上限重排（排队中的任务状态为 paused/queued） */
        enforceConcurrency: (limit?: number) => tasks.enforceConcurrency(limit ?? cfg.maxConcurrentTasks),
        /** 阶段3：名额让出后恢复排队任务 */
        releaseQueued: (limit?: number) => tasks.releaseQueued(limit ?? cfg.maxConcurrentTasks),
        /** 阶段3：用户反馈分类（补充信息 / 标准遗漏重开 / 新增关联任务 / 需要澄清） */
        applyUserFeedback: (...args: Parameters<TaskService["applyUserFeedback"]>) => tasks.applyUserFeedback(...args),
        /** 阶段3：纠错重开（新增/修订标准，预算沿用） */
        reopenWithCriteria: (...args: Parameters<TaskService["reopenWithCriteria"]>) => tasks.reopenWithCriteria(...args),
        /** 阶段3：重启后自动接续（单任务粒度，恢复流程会自动调用） */
        resumeAfterRestart: (taskId: string) => tasks.resumeAfterRestart(taskId),
        /** 阶段3：提交澄清答复并恢复（答案先送达负责人，再继续） */
        answerClarification: (taskId: string, answer: string) => tasks.answerClarification(taskId, answer),
        getClarificationAnswers: (taskId: string) => tasks.getClarificationAnswers(taskId),
        reconcileClarificationAnswer: (...args: Parameters<TaskService["reconcileClarificationAnswer"]>) => tasks.reconcileClarificationAnswer(...args),
        reconcileBackgroundJob: (...args: Parameters<TaskService["reconcileBackgroundJob"]>) => tasks.reconcileBackgroundJob(...args),
        getBackgroundJobs: (taskId: string) => tasks.getBackgroundJobs(taskId),
      };
      ctx.provide("longtask", api);
      const routerNotices = new RouterNotices(store, ctx);
      disposers.push(() => routerNotices.dispose());
      diag("stage:provided-longtask");
      const entry = registerUserTaskTools({ctx, tasks, ledger, workspace: cfg.workerCwd, ready: () => runtimeReady,
        isManaged: agent => Boolean(taskOwner(agent)),
        experts: async (fresh = false) => {
          if (fresh || !catalogCache) catalogCache = await scanCatalog(cfg.agencyAgentsRoot, join(cfg.agencyAgentsRoot, "divisions.json"));
          return catalogCache.experts;
        },
      });
      userEntryReady = entry.ready;
      disposers.push(() => entry.dispose());
      diag(`user-entry:ready=${entry.ready}`);

      /**
       * 产物冻结：执行者声明完成时按交付物路径登记内容哈希。
       * 必须在 onTurnEnd 之前完成——验收轮次在那一刻冻结产物哈希，晚注册就比不上。
       */
      /**
       * 登记本轮实际写入的产物（与"提交整任务验收"分开）。
       * 专家只完成一部分、报告 done=false 时，中间产物同样要有归属与哈希，
       * 否则交接消息与 Router 摘要里都没有产物引用（阶段2 审查 P1-2）。
       */
      const registerDeliverables = async (record: TaskSessionRecord, _assistantText: string | null, explicit: string[] = []) => {
        const registered = await tasks.registerTurnArtifacts(record.taskId, record.relPath, explicit);
        for (const abs of registered) diag(`artifact:registered ${abs} owner=${record.relPath}`);
        return registered;
      };

      /**
       * 预算收尾：结算后判断是否该让执行者停止探索。
       * 三个触发条件取先到者：
       * - 已用比例达到阈值（绝对压力）；
       * - 为下一请求及验收留足额度后，探索余量不足 wrapUpMinStepsLeft 步；
       * - 执行步数上限也只剩这些步。结算后及下一步准入前均检查。
       */
      const closingPlan = (taskId: string, sessionId: string, forecast: number) => {
        const acceptance = cfg.acceptanceReserveTokens > 0
          ? planAcceptance(admission, { ...tasks.estimateAcceptanceInput(taskId), minimumTokens: cfg.acceptanceReserveTokens })
          : { firstRequest: 0, conclusionRequest: 0, readGrowth: 0, tokens: 0, unknownArtifacts: 0 };
        const task = tasks.getTask(taskId)!; const own = tasks.findSessionBySessionId(sessionId);
        const entries = store.list<LedgerEntry>("ledger");
        const otherReports = tasks.currentVersion(task).executors.filter(e => e.relPath !== own?.relPath).map(e => {
          const lease = tasks.getSessions(taskId).filter(s => s.role === "executor" && !s.revokedAt && s.relPath === e.relPath).at(-1);
          const last = lease && entries.findLast(x => x.note?.includes(lease.sessionId));
          return Math.max(requestAdmissionCost(admission), last ? last.inputTokens + last.outputTokens + admission.maxOutputTokens : 0);
        });
        const otherReportTokens = otherReports.reduce((a, b) => a + b, 0);
        // 当前执行者至少还需一次落盘和一次最终报告；其他执行者也各保留一次报告。
        return { taskId, taskVersion: tasks.currentVersion(task).version, acceptance, otherReportTokens,
          closingTokens: forecast * 2 + admission.maxOutputTokens + otherReportTokens,
          protectedTokens: acceptance.tokens + otherReportTokens };
      };

      const maybeWrapUp = async (taskId: string, sessionId: string, upcoming = false, inboundBytes = 0): Promise<void> => {
        if (endedTurns.has(sessionId)) return;
        const role = tasks.findSessionBySessionId(sessionId)?.role;
        const task = tasks.getTask(taskId);
        if (role !== "executor" || !task || !["active", "rework"].includes(task.status)) return;
        const b = ledger.getBudget(taskId);
        if (!b || !(b.limitTokens > 0)) return;
        let available = b.limitTokens - b.settledTokens - b.reservedTokens;
        let usedRatio = (b.settledTokens + b.reservedTokens) / b.limitTokens;
        const forecast = requestCostFor(boundRequestInput(admission, {
          lastInputTokens: lastInputTokens.get(sessionId), lastOutputTokens: lastOutputTokens.get(sessionId),
          inboundChars: inboundBytes + (surfaceInputBytes.get(sessionId) ?? 0),
        }), admission);
        const plan = closingPlan(taskId, sessionId, forecast), acceptance = plan.acceptance.tokens;
        const perStep = Math.max(forecast, lastStepCost.get(taskId) ?? 0);
        let budgetStepsLeft = perStep > 0 ? Math.max(0, available - acceptance - plan.closingTokens) / perStep : Number.POSITIVE_INFINITY;
        try {
          const m = ledger.moneyForecast(taskId, forecast - admission.maxOutputTokens, admission.maxOutputTokens, plan.protectedTokens, plan.closingTokens);
          if (m) {usedRatio = m.usedRatio; budgetStepsLeft = Number(m.available - m.protected - m.closing) / Math.max(1, Number(m.need));}
        } catch {return;} // the admission gate presents the missing/expired price and pauses before requesting
        const executorStepsLeft = Math.max(0, cfg.executorMaxSteps - (stepCounts.get(sessionId) ?? 0) - Number(upcoming));
        const stepsLeft = Math.min(budgetStepsLeft, executorStepsLeft);
        const byRatio = cfg.wrapUpAtBudgetRatio > 0 && usedRatio >= cfg.wrapUpAtBudgetRatio;
        const bySteps = cfg.wrapUpMinStepsLeft > 0 && (
          (lastInputTokens.has(sessionId) && budgetStepsLeft <= cfg.wrapUpMinStepsLeft) || executorStepsLeft <= cfg.wrapUpMinStepsLeft
        );
        if (!byRatio && !bySteps) return;
        const sent = await tasks.requestWrapUp(taskId, usedRatio);
        if (sent) {
          diag(
            `budget:wrap-up task=${taskId} used=${Math.round(usedRatio * 100)}% stepsLeft=${stepsLeft.toFixed(1)} budgetStepsLeft=${budgetStepsLeft.toFixed(1)} executorStepsLeft=${executorStepsLeft} nextRequest=${forecast} acceptanceReserve=${acceptance}（${bySteps ? "按剩余预算/执行步数" : "按已用比例"}）`,
          );
        }
      };

      // ---- session/event：按真实 sessionId 归属（P1-6） ----
      let collaborationOrder = 0;
      const pendingArtifactReads = new Map<string, { path: string; turn: unknown }>();
      const pendingArtifactWrites = new Map<string, string>();
      const artifactWritesInFlight = new Map<string, Promise<void>>();
      const collaboration = (record: TaskSessionRecord, kind: string, extra: Record<string, unknown> = {}) => {
        diag(`collaboration:${JSON.stringify({ kind, taskId: record.taskId, sessionId: record.sessionId,
          instanceId, order: ++collaborationOrder, at: new Date().toISOString(), ...extra })}`);
      };
      const onSessionEvent = (session: { id: string }, event: SessionEvent) => {
        const record = tasks.findSessionBySessionId(session.id);
        if (!record) return; // 无关会话：不处理
        if (event.type === "approval/asked" || event.type === "approval/decided" || event.type === "turn/end") {
          void approvalWaits.observe(record, event).catch(error => ctx.logger.warn(`longtask: 审批等待登记失败：${String(error)}`));
          if (event.type.startsWith("approval/")) return;
        }
        if (event.type === "turn/end") endedTurns.add(record.sessionId);
        else if (event.type === "turn/start") endedTurns.delete(record.sessionId);
        let eventUsage: Partial<NonNullable<SessionEvent["data"]["usage"]>> | undefined = event.data.usage;
        let usageEventKey: string | undefined;
        if (event.type === "assistant/message" || event.type === "assistant/attempt") {
          const seq = (event as SessionEvent & {seq?: number}).seq;
          const messageId = event.data.message?.id;
          usageEventKey = typeof seq === "number" ? `${session.id}:seq:${seq}` : typeof messageId === "string" ? `${session.id}:message:${messageId}` : undefined;
          if (seenUsageObjects.has(event) || (usageEventKey && (seenUsageEvents.has(usageEventKey) || ledger.usageEventSeen(usageEventKey)))) return;
          seenUsageObjects.add(event); if (usageEventKey) seenUsageEvents.add(usageEventKey);
          if (!eventUsage && Array.isArray(event.data.stream)) {
            try { eventUsage = lastAssistantStreamChunk(event.data.stream as AssistantStreamRecord[], "usage")?.usage; }
            catch { /* malformed stream等同无法核对，不猜零费用 */ }
          }
          if (!eventUsage && (lastReservation.has(session.id) || requestStarted.has(session.id))) {
            blockAccounting(record, `模型尝试结束但缺少 Token 用量（${event.type}），保留预留并暂停核对`);
            return;
          }
        }
        if (event.type === "turn/end" && (accountingBlocked.has(record.taskId) || ledger.accountingIssue(record.taskId))) return;
        if (event.type === "turn/end" && settlementInFlight.has(session.id)) {
          void settlementInFlight.get(session.id)!.then(() => onSessionEvent(session, event)).catch(() => {});
          return;
        }
        // 诊断：关键事件带负载（截断），错误原因直接可见，不必再靠猜。
        // 工具调用与审批必须记名字：否则"预算烧在什么动作上"只能靠翻会话原始日志。
        const detail =
          event.type === "turn/end" || event.type.startsWith("llm/") || event.type === "step/end"
            ? ` data=${JSON.stringify(event.data ?? {}).slice(0, 800)}`
            : event.type === "tool/call" || event.type === "tool/result"
              ? ` tool=${String(event.data.name ?? event.data.tool ?? event.data.toolName ?? "?")}`
              : event.type === "approval/asked" || event.type === "approval/decided"
                ? ` approval=${JSON.stringify(event.data ?? {}).slice(0, 240)}`
                : "";
        // 带上 session：联调证据需要按会话核对"等待→交接→继续"的顺序
        diag(`event:${event.type} session=${session.id} role=${record.role} turn=${String(event.data.turn ?? "")} reason=${String(event.data.reason?.kind ?? "")}${detail}`);

        // 撤权检查先于所有 surface、工具计数和控制分支；只允许真实费用继续结算。
        if (record.revokedAt && !eventUsage) {
          diag(`authority:ignored task=${record.taskId} rel=${record.relPath} event=${event.type}`);
          return;
        }
        const executorWaiting = record.role === "executor" && tasks.getExecutorDependency(record.taskId, record.relPath).state === "waiting";
        if (executorWaiting && event.type !== "user/message" && !eventUsage) {
          diag(`dependency:event-ignored task=${record.taskId} rel=${record.relPath} event=${event.type}`);
          return;
        }

        // SDK 0.2.1-rc.5 dsh-jobs 的持久生命周期事件，按 jobId/owner 精确关联。
        if ((event.type === "job/registered" || event.type === "job/settled") && record.role === "executor") {
          const jobId = String(event.data.jobId ?? "");
          if (jobId && (event.data.owner === undefined || event.data.owner === session.id)) {
            void tasks.observeBackgroundJob(record.taskId, record.relPath, jobId, event.type === "job/settled", record.sessionKey)
              .catch(e => { diag(`job:observe-error task=${record.taskId} ${String(e)}`); void tasks.pause(record.taskId, "missing_input", "后台事件落盘失败，先核对命令状态"); });
          }
          return;
        }

        if (event.type === "subagent/catalog" && typeof event.data.childId === "string") {
          parentLinks.set(event.data.childId, session.id);
          return;
        }

        if (event.type === "tool/result" || event.type === "system/message" || event.type === "user/message" || event.type === "developer/message") {
          // SDK 的 user/message.data 本身就是消息；其余 surface 事件使用 data.message。
          const message = event.type === "user/message" ? event.data : event.data.message;
          if (message) {
            const toolCallId = typeof message.toolCallId === "string" ? message.toolCallId : message.callId;
            if (event.type === "tool/result" && typeof toolCallId === "string") {
              const key = `${session.id}:${toolCallId}`;
              const write = pendingArtifactWrites.get(key);
              pendingArtifactWrites.delete(key);
              if (write && !message.isError && record.role === "executor") {
                const saved = tasks.noteArtifactWrite(record.taskId, record.relPath, write, key);
                const pending = Promise.all([artifactWritesInFlight.get(record.taskId), saved]).then(() => undefined);
                artifactWritesInFlight.set(record.taskId, pending);
                void pending.then(() => {
                  if (artifactWritesInFlight.get(record.taskId) === pending) artifactWritesInFlight.delete(record.taskId);
                }).catch((error) => {
                  diag(`artifact:write-proof-error task=${record.taskId} ${String(error)}`);
                  void tasks.pause(record.taskId, "missing_input", "实际写入证据落盘失败，停止继续派工并核对");
                });
              }
              const read = pendingArtifactReads.get(key);
              pendingArtifactReads.delete(key);
              if (read && !message.isError) {
                try {
                  collaboration(record, "artifact_read", { path: read.path, turn: read.turn, callId: toolCallId,
                    hash: createHash("sha256").update(readFileSync(read.path)).digest("hex") });
                } catch { /* 无法证明读取版本，证据保持缺失，不冒充成功 */ }
              }
            }
            const source = message.source as { dedupeKey?: string } | undefined;
            if (event.type === "user/message" && source?.dedupeKey?.startsWith(`expert-msg:${record.taskId}:`)) {
              collaboration(record, "consumed", { dedupeKey: source.dedupeKey, messageId: message.id });
            }
            const control = /^(wrap-up|read-stop):/.exec(source?.dedupeKey ?? "");
            if (event.type === "user/message" && control) {
              // 实际进入模型可见 surface，区别于仅投递到队列。
              diag(`control:consumed task=${record.taskId} session=${record.sessionId} kind=${control[1]} dedupe=${source!.dedupeKey}`);
            }
            const seq = (event as SessionEvent & { seq?: number }).seq;
            const identity = typeof seq === "number" ? `seq:${seq}`
              : typeof message.id === "string" ? `${event.type}:${message.id}`
              : event.type === "tool/result" && typeof message.callId === "string"
                ? `tool:${event.data.turn ?? ""}:${message.callId}` : null;
            const seen = surfaceEventKeys.get(record.sessionId) ?? new Set<string>();
            if (identity === null || !seen.has(identity)) {
              if (identity !== null) seen.add(identity);
              surfaceEventKeys.set(record.sessionId, seen);
              surfaceInputBytes.set(record.sessionId, (surfaceInputBytes.get(record.sessionId) ?? 0) + inputSize(message));
            }
          }
          return;
        }

        // 读预算：读/检索类调用会把内容永久写进上下文，后续每一步都为它付费。
        // 超过上限就打断一次（每任务版本一次），把执行者从"读"推向"写"。
        if (event.type === "tool/call") {
          const toolName = String(event.data.name ?? event.data.tool ?? event.data.toolName ?? "");
          if (["read", "write", "edit"].includes(toolName) && typeof event.data.callId === "string") {
            try {
              const args = JSON.parse(String(event.data.arguments));
              const path = isAbsolute(args.file_path) ? resolve(args.file_path) : resolve(cfg.dataDir, args.file_path);
              const task = tasks.getTask(record.taskId);
              if (task && tasks.currentVersion(task).deliverables.some((d) => resolve(cfg.dataDir, d).toLowerCase() === path.toLowerCase())) {
                if (toolName === "read") pendingArtifactReads.set(`${session.id}:${event.data.callId}`, { path, turn: event.data.turn });
                else pendingArtifactWrites.set(`${session.id}:${event.data.callId}`, path);
              }
            } catch { /* 非标准读取参数不生成版本证据 */ }
          }
          if (/^(read|glob|grep|ls|list|find|rg|search|cat|head|tail)/i.test(toolName)) {
            const reads = (toolReadCounts.get(record.sessionId) ?? 0) + 1;
            toolReadCounts.set(record.sessionId, reads);
            if (reads > cfg.toolReadBudget) {
              void tasks
                .requestReadStop(record.taskId, reads)
                .then((sent) => {
                  if (sent) diag(`budget:read-stop task=${record.taskId} reads=${reads}`);
                })
                .catch((e) => ctx.logger.warn(`longtask: 读预算指令失败: ${String((e as Error).message)}`));
            }
          }
          return;
        }

        if (event.type === "assistant/message" || event.type === "assistant/attempt") {
          const text = (event.data.message?.content ?? [])
            .filter((b) => b.type === "text" && typeof b.text === "string")
            .map((b) => b.text!)
            .join("\n");
          if (text && !executorWaiting && !record.revokedAt) {
            lastAssistantText.set(record.sessionKey, text);
            // 同一 turn 内累积：换 turn 就重置，避免上一轮的报告被当成这一轮的结论
            const turn = typeof event.data.turn === "number" ? event.data.turn : 0;
            const prev = turnTexts.get(record.sessionKey);
            const merged = prev && prev.turn === turn ? `${prev.text}\n${text}` : text;
            turnTexts.set(record.sessionKey, { turn, text: merged });
          }
          // 用量结算（P1-7）：供应商返回用量 → 结算该会话最近的在途预留
          const usage = eventUsage;
          if (usage) {
            // SDK 的 inputTokens 就是未缓存输入（实测：inputTokens + cacheRead + output = totalTokens），
            // 但字段名与账本的 uncachedInputTokens 不同：两处读法必须一致，
            // 否则只报 inputTokens 的宿主会把实测输入算成 0，准入上界系统性低估。
            const sample = {
              uncachedInputTokens: usage.uncachedInputTokens ?? usage.inputTokens,
              outputTokens: usage.outputTokens,
              cacheReadTokens: usage.cacheReadTokens ?? 0,
              cacheWriteTokens: usage.cacheWriteTokens ?? 0,
            };
            if (Object.values(sample).some((n) => typeof n !== "number" || !Number.isFinite(n) || n < 0)) {
              // 无效用量不进实测链条：一旦把 0/NaN 记成"实测输入"，
              // 下一轮上界会退化甚至恒为 0，等于把严格预算的口径打穿。
              diag(`usage:invalid session=${record.sessionId}`);
              blockAccounting(record, "宿主返回的 Token 用量无效，保留预留并暂停核对");
              return;
            }
            // 实测输入与账本结算同一口径（normalizeUsage）：未缓存 + 缓存读 + 缓存写
            const normalized = normalizeUsage({
              uncachedInputTokens: sample.uncachedInputTokens!,
              outputTokens: sample.outputTokens!,
              cacheReadTokens: sample.cacheReadTokens!,
              cacheWriteTokens: sample.cacheWriteTokens!,
            });
            lastInputTokens.set(record.sessionId, normalized.inputTokens);
            // 本次实测输入已经包含此前 surface 增量；后续工具结果会重新累积。
            surfaceInputBytes.set(record.sessionId, 0);
            lastOutputTokens.set(record.sessionId, normalized.outputTokens);
            const settlement = (async () => {
              // 等预留落账：pre-step 的 reserve 是异步写盘，用量可能先到
              const pending = reserveInFlight.get(record.sessionId);
              if (pending) await pending;
              const rsv = lastReservation.get(record.sessionId);
              if (!rsv) {
                diag(`usage:no-reservation session=${record.sessionId}（用量 ${sample.uncachedInputTokens}+${sample.outputTokens} 无法结算）`);
                if (requestStarted.has(record.sessionId)) blockAccounting(record, "模型用量没有对应预留，暂停核对费用");
                return;
              }
              lastReservation.delete(record.sessionId);
              const admittedNeed = lastAdmissionNeed.get(record.sessionId);
              lastAdmissionNeed.delete(record.sessionId);
              // 击穿比对用"总输入（含缓存分项）+ 输出"：准入上界含缓存，实耗少算缓存会把漏账说成没超
              const actualTotal = normalized.inputTokens + normalized.outputTokens;
              lastStepCost.set(record.taskId, actualTotal);
              if (typeof admittedNeed === "number" && actualTotal > admittedNeed) {
                // 上界被击穿：本轮已花出去无法追回，但必须留痕并让严格上限的失效可见
                diag(
                  `budget:bound-exceeded session=${record.sessionId} 实耗 ${actualTotal} > 准入上界 ${admittedNeed}（差 ${actualTotal - admittedNeed}）`,
                );
                ctx.logger.warn(
                  `longtask: 会话 ${record.sessionId} 实际用量 ${actualTotal} 超过准入上界 ${admittedNeed}（差 ${actualTotal - admittedNeed}），已记录；下一轮上界改用实测链条`,
                );
              }
              try {
                const { overLimit } = await ledger.settle(
                  rsv,
                  {uncachedInputTokens: sample.uncachedInputTokens!, outputTokens: sample.outputTokens!, cacheReadTokens: sample.cacheReadTokens!, cacheWriteTokens: sample.cacheWriteTokens!},
                  `会话 ${record.sessionId} 实际用量`,
                  usageEventKey,
                );
                if (overLimit) await tasks.pause(record.taskId, "budget", "结算发现用量超出上限，任务暂停");
                else await maybeWrapUp(record.taskId, record.sessionId);
              } catch (error) {
                diag(`usage:settle-error session=${record.sessionId} ${String((error as Error).message)}`);
                blockAccounting(record, "模型用量结算失败，拒绝继续请求，等待核对账本");
              }
            })();
            settlementInFlight.set(record.sessionId, settlement);
            void settlement.finally(() => {
              if (settlementInFlight.get(record.sessionId) === settlement) settlementInFlight.delete(record.sessionId);
              diag(`usage:processed session=${record.sessionId} task=${record.taskId}`);
            }).catch((error) => ctx.logger.warn(`longtask: 结算同步失败: ${String(error)}`));
          }
          return;
        }
        if (event.type !== "turn/end") return;

        const kindRaw = event.data.reason?.kind ?? "completed";
        if (kindRaw === "blocked") {
          // A guard rejection is not a completed model turn. Let an earlier plugin pause commit first.
          void (async () => {
            await store.transact(() => {});
            const current = tasks.getTask(record.taskId);
            if (!current || !["active", "rework", "awaiting_acceptance"].includes(current.status)) return;
            if (!requestStarted.has(record.sessionId)) {
              const id = lastReservation.get(record.sessionId);
              if (id) {await ledger.releaseUnsent(id); lastReservation.delete(record.sessionId);}
            }
            await tasks.pause(record.taskId, "missing_input", "宿主拒绝了本轮模型准入（blocked），未把它当成完成或继续派工。请在主会话核对限制与账本；Worker里发送继续不能解除任务暂停。", true);
          })().catch(error => ctx.logger.warn(`longtask: 宿主阻塞状态登记失败：${String(error)}`));
          return;
        }
        const kind = (["completed", "aborted", "max-tokens", "cancelled", "error"] as const).includes(kindRaw as never)
          ? (kindRaw as "completed" | "aborted" | "max-tokens" | "cancelled" | "error")
          : kindRaw === "interrupted" || kindRaw === "forked" ? "aborted" : "error";
        const turnId = event.data.id ?? `${session.id}:turn:${event.data.turn ?? Date.now()}`;
        if (record.role === "executor") {
          observedExecutorEnds.set(session.id, { turnId, session: { id: session.id }, event: structuredClone(event) });
          const report = parseWorkerReport(currentTurnText(record.sessionKey) ?? lastAssistantText.get(record.sessionKey) ?? null);
          collaboration(record, "turn_end", { turn: event.data.turn, sourceTurnId: turnId, reason: kind,
            waiting: kind === "completed" && report?.done === false ? report.waiting : null });
        }

        // 验收会话：消费 <longtask_verdicts>（P1-6）
        if (record.role === "acceptor") {
          const text = lastAssistantText.get(record.sessionKey) ?? null;
          const verdictsReport = parseAcceptorVerdicts(text);
          if (verdictsReport) {
            void tasks
              .recordVerdicts(record.taskId, verdictsReport.verdicts, verdictsReport.artifactHashes, verdictsReport.roundId)
              .catch((e) => ctx.logger.warn(`longtask: 验收结论落盘失败: ${String((e as Error).message)}`));
            return;
          }
          // 未给结论 → 落检查点：已用步数 + 已有结论 + 本轮输出摘要，
          // 重建验收会话时据此接续，不再重复核对已经检查过的内容。
          const round = tasks.latestPendingRound(record.taskId);
          const usedSteps = stepCounts.get(record.sessionId) ?? 0;
          if (!round) return; // 没有待结算轮次：只记账收尾，不催办
          if (text) {
            const checked = [...text.matchAll(/\[([a-z0-9_-]+)\]\s*(?:✅|pass|通过|满足)/gi)].map((m) => m[1]!);
            void tasks
              .recordAcceptorProgress(record.taskId, round.roundId, {
                steps: usedSteps,
                lastSummary: text,
                checkedCriteria: checked,
              })
              .then(() => diag(`acceptance:checkpoint task=${record.taskId} round=${round.roundId} steps=${usedSteps} checked=${checked.join(",") || "-"}`))
              .catch((e) => ctx.logger.warn(`longtask: 验收检查点落盘失败: ${String((e as Error).message)}`));
          }
          // 收敛出口：只在"任务仍待验收 + 本轮正常结束"时动作。
          // 暂停/取消之后的 aborted/blocked 事件只收尾，不得自唤醒（否则会形成无效投递循环）。
          const live = tasks.getTask(record.taskId);
          if (live?.status !== "awaiting_acceptance") {
            diag(`acceptance:idle task=${record.taskId} status=${live?.status ?? "?"} kind=${kind}（只记账，不催办）`);
            return;
          }
          if (kind !== "completed") {
            diag(`acceptance:no-verdict-${kind} task=${record.taskId} round=${round.roundId}（非正常结束，不催办）`);
            return;
          }
          if (usedSteps >= cfg.acceptorMaxSteps) {
            void tasks.pause(
              record.taskId,
              "missing_input",
              `验收用满 ${cfg.acceptorMaxSteps} 步仍未给出结论（轮次 ${round.roundId}），交人工判定`,
            );
            diag(`acceptance:no-verdict-cap task=${record.taskId} steps=${usedSteps}`);
          } else {
            void tasks
              .nudgeAcceptor(record.taskId, round.roundId)
              .then((sent) => diag(`acceptance:${sent ? "nudged" : "nudge-skipped"} task=${record.taskId} round=${round.roundId} steps=${usedSteps}`))
              .catch((e) => ctx.logger.warn(`longtask: 验收催办失败: ${String((e as Error).message)}`));
          }
          return;
        }

        // 执行会话：先冻结产物（声明完成的那一轮），再走续做/等待/待验收决策
        void (async () => {
          await artifactWritesInFlight.get(record.taskId);
          const turnText = currentTurnText(record.sessionKey) ?? lastAssistantText.get(record.sessionKey) ?? null;
          if (kind === "completed") {
            try {
              await registerDeliverables(record, turnText);
            } catch (e) {
              ctx.logger.warn(`longtask: 产物登记失败: ${String((e as Error).message)}`);
            }
            // 阶段2：专家直连消息（正文只发收件人，summary 进 Router 摘要）
            // 动作身份 = 轮次 + 协议块序号：同一轮重放幂等，不同轮次相同句子不会被吞。
            const blocks = parseExpertMessages(turnText);
            for (let i = 0; i < blocks.length; i++) {
              const msg = blocks[i]!;
              try {
                // 显式声明的产物先核验登记，再随消息交接（中间产物也要有归属与哈希）
                const resolved = msg.artifacts.length > 0 ? await registerDeliverables(record, turnText, msg.artifacts) : [];
                const refs = [...new Set([...resolved, ...tasks.listOwnedArtifacts(record.taskId, record.relPath)])];
                const sent = await tasks.sendExpertMessage(record.taskId, record.relPath, msg.to, msg.body, {
                  artifactRefs: refs,
                  actionId: `${turnId}:msg:${i}`,
                  sourceTurnId: turnId,
                });
                if (msg.summary && sent.messageId) {
                  await tasks.recordHandoffSummary(record.taskId, {
                    from: record.relPath,
                    to: msg.to,
                    summary: msg.summary,
                    artifactRefs: refs,
                    causedByMessageId: sent.messageId,
                    actionId: `${turnId}:sum:${i}`,
                    sourceTurnId: turnId,
                  });
                }
                diag(
                  `expert:message from=${record.relPath} to=${msg.to} ok=${sent.ok} refs=${refs.length}` +
                    `${sent.reason ? ` reason=${sent.reason}` : ""}${msg.summary ? " summary=1" : ""}`,
                );
              } catch (error) {
                // 目标专家不在本任务等业务错误：不能打断本轮收尾
                ctx.logger.warn(`longtask: 专家直连消息未送达: ${String((error as Error).message)}`);
              }
            }
          }
          await tasks.onTurnEnd(record.taskId, record.relPath, kind, turnId);
        })().catch((e) => ctx.logger.warn(`longtask: turn/end 处理失败: ${String((e as Error).message)}`));
      };
      const disposeSessionListener = ctx.on("session/event", onSessionEvent);
      if (typeof disposeSessionListener === "function") disposers.push(disposeSessionListener);

      // ---- 请求前预算准入（硬）：放行前必须完成原子预留 ----
      // 旧实现按固定估算预留、且不等预留落账就 next()，于是"先调用、事后才发现超限"。
      const inboundCharsOf = (messages: unknown): number => {
        if (!Array.isArray(messages)) return 0;
        return messages.reduce((total, message) => total + inputSize(message), 0);
      };

      const admitStep = async (payload: { agent?: BudgetAgent; messages?: unknown }, next: () => unknown) => {
        const sessionId = payload.agent?.session?.id;
        if (!sessionId) return next();
        const record = tasks.findSessionBySessionId(sessionId);
        if (record && (accountingBlocked.has(record.taskId) || ledger.accountingIssue(record.taskId))) return {kind: "reject" as const};
        // 执行资格按"会话 + 任职代次"判定：路径重回名单不代表旧会话复活
        if (record?.revokedAt) {
          diag(`authority:revoked task=${record.taskId} rel=${record.relPath} session=${sessionId}`);
          return { kind: "reject" as const };
        }
        if (!record) {
          const owner = taskOwner(payload.agent);
          if (!owner) return next(); // 真正无关的会话放行
          diag(`delegation:rejected task=${owner.taskId} session=${sessionId}（未托管后代不得请求模型）`);
          try { payload.agent?.cancel?.({ kind: "user" }); } catch { /* 拒绝仍然生效 */ }
          void tasks.pause(owner.taskId, "budget", `发现未纳入预算的后代会话 ${sessionId}，已拒绝模型步骤，任务暂停核对`)
            .catch((error) => ctx.logger.warn(`longtask: 委派异常暂停失败: ${String(error)}`));
          return { kind: "reject" as const };
        }
        if (!delegationGuardReady) {
          void tasks.pause(record.taskId, "budget", "宿主缺少 tools.guard，不能在工具执行前阻止未托管委派，拒绝启动模型步骤")
            .catch((error) => ctx.logger.warn(`longtask: 能力不足暂停失败: ${String(error)}`));
          return { kind: "reject" as const };
        }
        const task = tasks.getTask(record.taskId);
        const roleCanRun = record.role === "acceptor"
          ? task?.status === "awaiting_acceptance"
          : task?.status === "active" || task?.status === "rework";
        if (!task || !roleCanRun) {
          return { kind: "reject" as const }; // 只允许与当前阶段匹配的角色进入模型步骤
        }
        if (record.role === "executor") {
          const required = tasks.getRequiredTools(record.taskId);
          if (required.length) {
            const deadline = Date.now() + 15_000;
            // 通用的执行能力前置条件：重启时给宿主插件装载留出时间，不借模型请求重试缺失工具。
            while (required.some(name => !ctx.get<{ get?(name: string, scope?: BudgetAgent): unknown }>("tools")?.get?.(name, payload.agent))) {
              const current = tasks.getTask(record.taskId);
              if (probeController.signal.aborted || !current || !["active", "rework"].includes(current.status)
                || tasks.currentVersion(current).version !== tasks.currentVersion(task).version || tasks.findSessionBySessionId(sessionId)?.revokedAt) return { kind: "reject" as const };
              if (Date.now() >= deadline) {
                await tasks.pause(record.taskId, "missing_input", `必需工具尚未装载：${required.join("、")}；未发起模型请求，请装载后恢复`);
                return { kind: "reject" as const };
              }
              await new Promise(resolve => setTimeout(resolve, 25));
            }
          }
          if (!await tasks.executorReady(record.taskId, record.relPath)) {
            diag(`dependency:waiting task=${record.taskId} rel=${record.relPath} session=${sessionId}`);
            return { kind: "reject" as const };
          }
          const fresh = tasks.getTask(record.taskId);
          if (tasks.findSessionBySessionId(sessionId)?.revokedAt || !fresh || !["active", "rework"].includes(fresh.status)) return { kind: "reject" as const };
        }
        // 执行资格：被换下的执行者不能继续消耗预算（历史记录保留审计）
        if (record.role === "executor") {
          const current = tasks.currentVersion(task);
          if (!current.executors.some((e) => e.relPath === record.relPath)) {
            diag(`authority:revoked task=${record.taskId} rel=${record.relPath} session=${sessionId}`);
            return { kind: "reject" as const };
          }
        }

        // 步数上限：验收不得无限探查，执行者也不得"读到没预算"。
        // 预算兜底是最后一道闸，不该成为唯一一道——被预算拦下时往往已经花掉了九成。
        if (!stepCounts.has(sessionId)) {
          // 验收重启后从检查点续计，否则新会话又白拿一整套步数
          stepCounts.set(
            sessionId,
            record.role === "acceptor" ? tasks.latestPendingRound(record.taskId)?.checkpoint?.steps ?? 0 : 0,
          );
        }
        const steps = (stepCounts.get(sessionId) ?? 0) + 1;
        const stepCap = record.role === "acceptor" ? cfg.acceptorMaxSteps : cfg.executorMaxSteps;
        if (steps > stepCap) {
          void tasks.pause(
            record.taskId,
            "missing_input",
            record.role === "acceptor"
              ? `验收会话已用 ${stepCap} 步仍未给出结论，任务暂停等待人工判定`
              : `执行会话已用 ${stepCap} 步仍未完成，任务暂停等待人工判断（避免把预算烧在探索上）`,
          );
          diag(`budget:${record.role === "acceptor" ? "acceptor" : "executor"}-step-cap session=${sessionId} steps=${steps}`);
          return { kind: "reject" as const };
        }
        stepCounts.set(sessionId, steps);

        // SDK 的 waterfall 末端还会把 runtime-context 追加到 enter.messages。
        // next() 在这里仅生成步骤提案，不发模型请求；拿到提案后预留，再交还宿主执行。
        const proposed = await next();
        const decision = proposed as { kind?: string; messages?: unknown } | null | undefined;
        if (decision?.kind === "reject") return proposed;
        const proposedMessages = Array.isArray(decision?.messages) ? decision.messages : payload.messages;

        // 硬准入（P1-1）：输入上界 + 输出上限必须被可用余额覆盖，且预留必须落账成功才放行。
        // 用实测链条估算本步输入：上轮输入 + 上轮输出 + 新入站消息与 surface 增量的完整 UTF-8 字节数。
        // 该口径覆盖消息内容和已观测结构；动态系统提示词/工具 schema 等宿主隐藏部分仍依赖声明基线。
        // 首轮没有实测链条时只能用声明基线；严格模式下基线被显式声明为 0 则拒绝调用。
        // 输入上界只用**本会话**的实测链条。不能借用同任务其它会话的值：
        // 新验收会话的 prompt 只有"系统 + 验收请求"，借用执行者的 5 万上下文
        // 会把 2.4 万的准入算成 6 万，验收在门口就被预算掐死（联调实测）。
        const bound = boundRequestInput(admission, {
          lastInputTokens: lastInputTokens.get(sessionId),
          lastOutputTokens: lastOutputTokens.get(sessionId),
          inboundChars: inboundCharsOf(proposedMessages) + (surfaceInputBytes.get(sessionId) ?? 0),
        });
        const budgetState = ledger.getBudget(record.taskId);
        const strict = budgetState?.strict ?? true;
        if (strict && !hasTrustedBound(admission, bound)) {
          const note =
            `严格模式拒绝调用：宿主 agent/pre-step 只提供本步新增消息，拿不到组装后的完整 prompt，` +
            `而本任务未声明输入基线（preStepInputEstimateTokens=0），无法给出可信输入上界。` +
            `本次可推上界：${bound.detail}。请声明基线或改用非严格预算后重试。`;
          diag(`budget:untrusted-bound task=${record.taskId} session=${sessionId}`);
          void tasks.pause(record.taskId, "budget", note);
          return { kind: "reject" as const };
        }
        const need = requestCostFor(bound, admission);
        // 同一计划用于收尾和硬准入；绝不按预算百分比削减必要验收费用。
        const plan = record.role === "executor" ? closingPlan(record.taskId, sessionId, need) : null;
        const reserve = plan?.protectedTokens ?? 0;
        try {
          const money = ledger.moneyForecast(record.taskId, bound.tokens, admission.maxOutputTokens, reserve);
          if (money && money.available < money.need + money.protected) {
            await tasks.pause(record.taskId, "budget", `请求前金额准入不足：本次需¥${formatCny(money.need)}，保护验收及其他专家报告¥${formatCny(money.protected)}，可用¥${formatCny(money.available)}；未追加额度`);
            return {kind: "reject" as const};
          }
        } catch (error) {await tasks.pause(record.taskId, "budget", String((error as Error).message)); return {kind: "reject" as const};}
        if (plan) await store.transact(data => {
          (data.collections["closingBudgetPlans"] ??= {})[record.taskId] = { ...plan, sessionId, requestTokens: need, availableTokens: ledger.available(record.taskId), updatedAt: new Date().toISOString() };
        });
        const available = ledger.available(record.taskId);
        if (available < need + reserve) {
          const note =
            `会话 ${sessionId} 请求前准入失败：需要 ${need}` +
            `（输入上界 ${bound.tokens} + 输出上限 ${admission.maxOutputTokens}；${bound.detail}）` +
            `${reserve > 0 ? `，另需为验收及其他专家报告保留 ${reserve}` : ""}，` +
            `可用 ${Math.max(0, available)}。保留进度停止，未请求追加额度。` +
            `标准：${tasks.currentVersion(task).criteria.map(c => c.id).join("、")}；` +
            `已登记产物：${store.list<{ taskId: string; location: string; contentHash: string | null }>("artifacts").filter(a => a.taskId === record.taskId).map(a => `${a.location}#${a.contentHash ?? "未知哈希"}`).join("；") || "无"}`;
          diag(`budget:reject ${note}`);
          void tasks.pause(record.taskId, "budget", note);
          return { kind: "reject" as const };
        }

        // 预留是放行的前提：await 到落账为止。落盘失败、额度竞争失败都不得继续调用，
        // 否则账本又退回"事后记账"，严格上限形同虚设。
        const pending = ledger
          .reserve(record.taskId, bound.tokens, admission.maxOutputTokens, reserve)
          .then((r) => {
            if (r.ok) {
              lastReservation.set(sessionId, r.reservationId);
              return { ok: true as const };
            }
            return { ok: false as const, reason: `可用额度在预留时被抢占（可用 ${r.available}，需要 ${need}）` };
          })
          .catch((error) => ({ ok: false as const, reason: `预留落盘失败：${String((error as Error).message)}` }));
        reserveInFlight.set(sessionId, pending.then(() => undefined));
        const reserved = await pending;
        if (reserveInFlight.get(sessionId)) reserveInFlight.delete(sessionId);
        if (!reserved.ok) {
          const note = `会话 ${sessionId} 请求前预留失败，拒绝调用：${reserved.reason}`;
          diag(`budget:reserve-failed ${note}`);
          void tasks.pause(record.taskId, "budget", note);
          return { kind: "reject" as const };
        }
        diag(`budget:admitted task=${record.taskId} session=${sessionId} need=${need} basis=${bound.basis} ${bound.detail}`);
        lastAdmissionNeed.set(sessionId, need);
        seriesInput.set(sessionId, bound.tokens);
        requestStarted.delete(sessionId);
        return proposed;
      };
      const onPreStep = async (payload: { agent?: BudgetAgent; messages?: unknown }, next: () => unknown) => {
        const sid = payload.agent?.session?.id;
        const record = sid ? tasks.findSessionBySessionId(sid) : undefined;
        if (!sid || !record) return admitStep(payload, next);
        const controls: UserMessage[] = [];
        stepControls.set(sid, controls);
        try {
          // SDK 在 hook 前已经 claim inbox；等待上一响应完成结算/收尾，晚到的控制直接并入本步。
          await settlementInFlight.get(sid);
          if (requestStarted.has(sid) && lastReservation.has(sid)) {
            blockAccounting(record, "上一请求没有完成用量结算，拒绝进入下一步");
            return {kind: "reject" as const};
          }
          await maybeWrapUp(record.taskId, sid, true, inboundCharsOf(payload.messages));
          return await admitStep(payload, async () => {
            const proposed = await next();
            const decision = proposed as { kind?: string; messages?: unknown[] } | null | undefined;
            if (decision?.kind !== "enter" || controls.length === 0) return proposed;
            return { ...decision, messages: [...(decision.messages ?? (Array.isArray(payload.messages) ? payload.messages : [])), ...controls] };
          });
        } finally { if (stepControls.get(sid) === controls) stepControls.delete(sid); }
      };
      const disposePreStep = ctx.on("agent/pre-step", onPreStep);
      if (typeof disposePreStep === "function") disposers.push(disposePreStep);
      // SDK step()内部while重试不会再经过pre-step；prepareRequest则每次都会请求此钩子。
      const disposeRequest = ctx.on("agent/request", async (payload: {agent?: BudgetAgent; signal?: AbortSignal}, next: () => Promise<Record<string, unknown>>) => {
        const sid = payload.agent?.session?.id;
        const record = sid ? tasks.findSessionBySessionId(sid) : undefined;
        if (!sid || !record) {
          if (taskOwner(payload.agent)) throw new Error("未托管任务后代不得请求模型");
          return next();
        }
        await settlementInFlight.get(sid);
        payload.signal?.throwIfAborted();
        const current = tasks.getTask(record.taskId);
        if (record.revokedAt || accountingBlocked.has(record.taskId) || ledger.accountingIssue(record.taskId)
          || !current || !(record.role === "acceptor" ? current.status === "awaiting_acceptance" : ["active", "rework"].includes(current.status))) throw new Error("任务已停止或模型用量待核对，拒绝请求");
        if (requestStarted.has(sid)) {
          if (lastReservation.has(sid)) {
            blockAccounting(record, "前一次模型尝试未完成用量核对，拒绝重试");
            throw new Error("前一次尝试用量未核对");
          }
          const input = Math.max(seriesInput.get(sid) ?? 0, lastInputTokens.get(sid) ?? 0) + (surfaceInputBytes.get(sid) ?? 0);
          const need = input + admission.maxOutputTokens;
          const protectedTokens = record.role === "executor" ? closingPlan(record.taskId, sid, need).protectedTokens : 0;
          const reserved = await ledger.reserve(record.taskId, input, admission.maxOutputTokens, protectedTokens);
          if (!reserved.ok) { await tasks.pause(record.taskId, "budget", "请求重试的剩余额度不足，保持原上限停止"); throw new Error("重试预算不足"); }
          lastReservation.set(sid, reserved.reservationId); lastAdmissionNeed.set(sid, need); seriesInput.set(sid, input);
          diag(`budget:retry-admitted task=${record.taskId} session=${sid} need=${need}`);
        } else if (!lastReservation.has(sid)) throw new Error("模型请求没有对应的准入预留");
        // 供应商真正使用的输出上限也要受约束，不能只限制账本中的估计值。
        const config = await next();
        const maxTokens = Math.min(typeof config.maxTokens === "number" ? config.maxTokens : admission.maxOutputTokens, admission.maxOutputTokens);
        const rsv = lastReservation.get(sid)!;
        try {
          payload.signal?.throwIfAborted();
          const live = tasks.getTask(record.taskId), lease = tasks.findSessionBySessionId(sid);
          if (lease?.revokedAt || !live || !(record.role === "acceptor" ? live.status === "awaiting_acceptance" : ["active", "rework"].includes(live.status))) throw Error("请求准备期间任务已停止，未发起模型请求");
          if (ledger.getBudget(record.taskId)?.money) {
            if (typeof config.provider !== "string" || typeof config.model !== "string") throw Error("实际模型路由缺失，拒绝金额请求");
            const input = seriesInput.get(sid);
            if (input === undefined) throw Error("请求输入预留缺失");
            const protectedTokens = record.role === "executor" ? closingPlan(record.taskId, sid, input + maxTokens).protectedTokens : 0;
            await ledger.reserveMoney(rsv, {provider: config.provider, model: config.model}, input, maxTokens, protectedTokens,
              {sessionId: sid, role: record.role, expertRelPath: record.relPath,
                turn: (payload as {turn?: number}).turn, step: (payload as {step?: number}).step});
          }
          payload.signal?.throwIfAborted();
          const readyTask = tasks.getTask(record.taskId);
          if (tasks.findSessionBySessionId(sid)?.revokedAt || !readyTask || !(record.role === "acceptor" ? readyTask.status === "awaiting_acceptance" : ["active", "rework"].includes(readyTask.status))) throw Error("金额预留期间任务已停止，未发起模型请求");
        } catch (error) {
          await ledger.releaseUnsent(rsv, true); lastReservation.delete(sid); lastAdmissionNeed.delete(sid);
          await tasks.pause(record.taskId, "budget", String((error as Error).message)); throw error;
        }
        requestStarted.add(sid);
        return {...config, maxTokens};
      });
      if (typeof disposeRequest === "function") disposers.push(disposeRequest);
      diag("stage:listeners-ready");

      // ---- Watchdog：纠偏经 TaskService 投递（P2-12） ----
      watchdog.start(cfg.watchdogIntervalMs, async () => {
        // 阶段3：每轮先清理过期等待（条件已满足/任务已不在运行态），避免 waitingOn 永久悬挂
        const swept = await tasks.sweepStaleWaits().catch((e) => {
          ctx.logger.warn(`longtask: 清理过期等待失败: ${String((e as Error).message)}`);
          return 0;
        });
        if (swept > 0) diag(`wait:swept count=${swept}`);
        // 阶段3：并发名额让出后恢复排队任务，再按上限重排（防止确认后超并发）
        const promoted = await tasks.releaseQueued(cfg.maxConcurrentTasks).catch(() => []);
        if (promoted.length > 0) diag(`schedule:released count=${promoted.length}`);
        await tasks.enforceConcurrency(cfg.maxConcurrentTasks).catch(() => ({ running: [], queued: [] }));
        for (const { task, action } of watchdog.checkAll()) {
          if (action.action === "correct") {
            ctx.logger.warn(`longtask: [${task.id}] 纠偏 —— ${action.note}`);
            await watchdog.applyCorrection(task.id, action.note);
            await tasks.sendWatchdogCorrection(task.id, action.instruction);
          } else if (action.action === "escalate") {
            ctx.logger.warn(`longtask: [${task.id}] 求助用户 —— ${action.question}（尝试记录: ${action.attempts.join(" | ")}）`);
            await tasks.escalateWatchdog(task.id, {
              question: action.question,
              attempts: action.attempts,
              waiting: action.evidence.waiting,
              lastSubstantiveAt: action.evidence.lastSubstantiveAt,
              mode: "slow",
            });
          }
        }
      });
      disposers.push(() => watchdog.stop());

      diag("stage:watchdog-started");

      // ---- 重启恢复派工：必须在 session/event、agent/pre-step、tools.guard 全部就绪之后 ----
      // 否则恢复 followup 会抢在预算准入之前启动模型，首步绕过准入、用量与结束事件也可能丢失。
      await tasks.sweepStaleWaits();
      await tasks.enforceConcurrency(cfg.maxConcurrentTasks);
      for (const taskId of restartTargets) {
        try {
          const outcome = await tasks.resumeAfterRestart(taskId);
          diag(`restart:resume task=${taskId} outcome=${outcome}`);
          if (outcome === "skipped") {
            ctx.logger.warn(`longtask: 任务 ${taskId} 未自动接续（状态或会话不满足），已保留原状态`);
          }
        } catch (error) {
          ctx.logger.warn(`longtask: 任务 ${taskId} 重启接续失败: ${String((error as Error).message)}`);
        }
      }

      // ---- 阶段 0 探针：此时组合与服务均已就绪，异步执行不阻塞任何入口 ----
      runtimeReady = true;
      routerNotices.wake();
      startProbe();
      // 探针是 fire-and-forget 的，但卸载必须能取消它：
      // 否则热重载后旧实例会继续延迟/重试并写 probe-latest，用过期结论污染最新能力判断。
      disposers.push(() => {
        probeController.abort();
        diag(`probe:abort instance=${instanceId}`);
      });
      diag("stage:ready-complete");

      disposers.push(() => store.close());
      disposers.push(() => host.disposeAll());
    })().catch((error) => {
      diag(`INIT-ERROR ${String((error as Error)?.stack ?? error)}`);
      ctx.logger.error(`longtask: 初始化失败: ${String((error as Error).message ?? error)}`);
    });

    yield async () => {
      await ready;
      runtimeReady = false;
      for (const d of disposers) await d();
      ctx.logger.info("longtask: 已卸载，存储已落盘，会话已释放");
    };
  };
  try {
    ctx.effect(lifecycle, "longtask lifecycle");
  } catch (error) {
    diag(`EFFECT-THROW ${String((error as Error)?.stack ?? error)}`);
    throw error;
  }
}
