/**
 * 阶段 0 · 宿主探针 · 实施文档 §1、阶段 0 出口。
 *
 * 只做能力核对，不做业务：逐项实测后输出可复现的探针记录（JSON），
 * 标明硬限额及自动恢复支持程度。缺失必要服务时报告缺失能力，
 * 避免任务被误标为已运行。
 *
 * 阶段 1 审查 P1-1/3 修正：
 * - pre-step 拒绝钩子只匹配探针 Agent，观测到拒绝路径完成后在 finally 移除；
 *   pass 基于实际观测（钩子被调用且拒绝了探针消息），不以"注册成功"替代。
 * - agents.create 按真实 SDK 传必填 sessionId，返回 AgentHandle{agent, dispose}；
 *   探针结束（含失败路径）释放探针会话。
 *
 * 探针项：1 服务存在性 2 独立会话创建 3 followup+turn/end 原因 4 pre-step 拒绝
 * 5 tokenUsage 投影 6 node:sqlite。
 */

import { randomUUID } from "node:crypto";
import { createUserMessage } from "@deepseek-ai/dsh-llm";
import type { AgentLike, HostContext, SessionEvent } from "../host-types.js";

export interface ProbeItem {
  id: string;
  description: string;
  status: "pass" | "fail" | "unsupported" | "skipped";
  detail: string;
}

export interface ProbeReport {
  /** 哪个插件实例产生的报告：热重载时新旧实例会同时存在，报告必须可追溯，否则会把旧实例的失败当成现状 */
  instanceId: string;
  probedAt: string;
  hostNodeVersion: string;
  nodeSqliteAvailable: boolean;
  items: ProbeItem[];
  /** 阻塞点：探针失败的必要能力 */
  blockers: string[];
}

/** 探针超时配置：宿主调用可能永不返回，必须可调（默认值面向真实宿主，测试可调小）。 */
export interface ProbeTimeouts {
  createTimeoutMs?: number;
  idleTimeoutMs?: number;
  disposeTimeoutMs?: number;
  onProgress?: (stage: string) => void;
}

/** 探针内所有"可能永不返回"的宿主调用都必须带超时：探针绝不能拖死插件初始化。 */
function withTimeout<T>(task: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} 超时（${ms}ms 未返回）`)), ms);
    task.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

export async function probeHost(ctx: HostContext, timeouts: ProbeTimeouts = {}, instanceId = "unknown"): Promise<ProbeReport> {
  const createTimeoutMs = timeouts.createTimeoutMs ?? 20_000;
  const idleTimeoutMs = timeouts.idleTimeoutMs ?? 45_000;
  const disposeTimeoutMs = timeouts.disposeTimeoutMs ?? 10_000;
  const items: ProbeItem[] = [];  const blockers: string[] = [];
  const add = (id: string, description: string, status: ProbeItem["status"], detail: string, blocker = false) => {
    items.push({ id, description, status, detail });
    if (blocker && status !== "pass") blockers.push(`${id}: ${detail}`);
    timeouts.onProgress?.(`${id}: ${status} ${detail}`);
  };

  // ---- 1. 服务存在性 ----
  const agents = ctx.get<{ create(o: Record<string, unknown>): Promise<{ agent: AgentLike; dispose(): Promise<void> }> }>("agents");
  add("svc.agents", "ctx.agents 服务（会话创建/续写）", agents ? "pass" : "fail", agents ? "存在" : "未注册", true);
  const sessions = ctx.get<unknown>("sessions");
  add("svc.sessions", "ctx.sessions 服务（flush 持久化检查点）", sessions ? "pass" : "fail", sessions ? "存在" : "未注册", true);
  const tokenMeter = ctx.get<unknown>("tokenMeter");
  add(
    "svc.tokenMeter",
    "ctx.tokenMeter / tokenUsage 投影（用量记账）",
    tokenMeter ? "pass" : "unsupported",
    tokenMeter ? "存在" : "未注册：Token 账本退化为仅按预留记账，无法用供应商返回值结算（宽松模式）",
    true,
  );
  const subagents = ctx.get<unknown>("subagents");
  add("svc.subagents", "subagents 服务（等待子会话收敛）", subagents ? "pass" : "unsupported", subagents ? "存在" : "未注册：并行专家收敛检查降级");

  // ---- 2. 独立会话创建（真实协议：必填 sessionId，返回 {agent, dispose}）----
  let probeAgent: AgentLike | undefined;
  let probeDispose: (() => Promise<void>) | undefined;
  const sessionEventListeners: Array<() => void> = [];
  const registerListener = (event: string, listener: (...args: any[]) => void) => {
    const dispose = ctx.on(event, listener);
    if (typeof dispose === "function") sessionEventListeners.push(dispose);
  };
  let preStepRemover: (() => void) | undefined;

  try {
    if (!agents) throw new Error("agents 服务不存在");
    timeouts.onProgress?.("agent.create:start");
    const handle = await withTimeout(agents.create({ sessionId: randomUUID(), agentOptions: {} }), createTimeoutMs, "agents.create");
    probeAgent = handle.agent; // 句柄解构：followup/status 在 handle.agent 上
    probeDispose = handle.dispose;
    add("agent.create", "独立会话创建（agents.create + 必填 sessionId）", "pass", `agent=${probeAgent.id}`, true);
  } catch (error) {
    add("agent.create", "独立会话创建（agents.create + 必填 sessionId）", "fail", String((error as Error).message), true);
  }

  if (probeAgent) {
    // ---- 3. followup 投递 + turn/end 原因观测 ----
    const turnEnds: SessionEvent[] = [];
    registerListener("session/event", (session: { id: string }, event: SessionEvent) => {
      if (session.id === probeAgent!.session.id && event.type === "turn/end") turnEnds.push(event);
    });
    try {
      probeAgent.followup(createUserMessage({
        content: [{ type: "text", text: "[longtask-probe] 这是宿主能力探针消息。不要执行任何工具，直接回复 OK 并结束。" }],
        source: { kind: "longtask-probe" },
      }));
      add("agent.followup", "followup 投递（续写原会话入口）", "pass", "已排队", true);
    } catch (error) {
      add("agent.followup", "followup 投递（续写原会话入口）", "fail", String((error as Error).message), true);
    }

    // ---- 4. pre-step 拒绝路径：仅对探针 Agent 生效的一次性钩子 ----
    let hookInvoked = false;
    let hookRejected = false;
    const preStepHandler = (payload: { agent?: AgentLike; messages?: unknown[] }, next: () => unknown) => {
      if (payload.agent !== probeAgent) return next(); // 无关会话：正常放行
      hookInvoked = true;
      try {
        return { kind: "reject" as const }; // 验证拒绝路径：探针消息被拒，不进入模型
      } finally {
        hookRejected = true;
      }
    };
    try {
      const dispose = ctx.on("agent/pre-step", preStepHandler);
      preStepRemover = typeof dispose === "function" ? dispose : () => {};
    } catch (error) {
      add("agent.pre-step", "agent/pre-step 拒绝钩子安装", "fail", String((error as Error).message), true);
    }

    // ---- 等探针轮次收敛（拒绝路径的观测窗口），随后清理 ----
    // 关键：whenIdle 可能永不返回（模型调用挂起），必须设超时，否则整个探针连同插件初始化一起卡死。
    let idleOk = false;
    let idleDetail = "";
    try {
      await withTimeout(probeAgent.whenIdle(), idleTimeoutMs, "whenIdle");
      idleOk = true;
      idleDetail = "探针轮次已收敛";
    } catch (error) {
      idleDetail = String((error as Error).message);
    }
    add("agent.whenIdle", "探针轮次收敛（whenIdle）", idleOk ? "pass" : "unsupported", idleDetail);
    if (preStepRemover) {
      preStepRemover(); // finally 语义：无论观测结果如何都移除
      preStepRemover = undefined;
      add(
        "agent.pre-step",
        "agent/pre-step 拒绝钩子（作用域限定 + 一次性）",
        hookInvoked && hookRejected ? "pass" : "unsupported",
        hookInvoked && hookRejected ? "已观测：探针消息被拒绝，钩子随后移除" : "钩子未观测到调用（可能消息未进入 pre-step）",
      );
    }
    add(
      "turn.end.observation",
      "turn/end 事件与原因观测",
      turnEnds.length > 0 ? "pass" : "unsupported",
      turnEnds.length > 0 ? `观测到 ${turnEnds.length} 次 turn/end，原因: ${turnEnds.map((e) => String(e.data.reason?.kind)).join(",")}` : "未观测到 turn/end",
    );
  }

  // ---- 5. tokenUsage 投影字段 ----
  const projections = ctx.get<Record<string, unknown>>("sessionProjections");
  const usageProjectionReady =
    projections !== undefined &&
    (projections["tokenUsage"] !== undefined || (typeof (projections as { stateOf?: unknown }).stateOf === "function"));
  add(
    "projection.tokenUsage",
    "tokenUsage 投影（uncachedInput/output/cacheRead/cacheWrite 分项）",
    usageProjectionReady ? "pass" : "unsupported",
    usageProjectionReady
      ? "投影注册表存在（sessionProjections 服务已激活；tokenUsage 单元由 token-meter 注册）"
      : "投影未注册：无法区分重试与重复上报，严格模式硬上限不可承诺",
  );

  // ---- 6. node:sqlite 可用性（存储选型）----
  let sqliteOk = false;
  try {
    timeouts.onProgress?.("storage.sqlite:import");
    const { DatabaseSync } = (await import("node:sqlite")) as { DatabaseSync: new (p: string) => { close(): void; exec(s: string): void } };
    const db = new DatabaseSync(":memory:");
    db.exec("CREATE TABLE probe (id TEXT)");
    db.close();
    sqliteOk = true;
  } catch {
    sqliteOk = false;
  }
  add("storage.sqlite", "node:sqlite（实施文档建议的事务存储）", sqliteOk ? "pass" : "unsupported", sqliteOk ? "可用" : "不可用：使用 JSON 事务存储（原子写），SQLite 待宿主运行时验证", true);

  // ---- 探针会话清理：无论成功失败都释放，不留孤儿 ----
  if (probeDispose) {
    try {
      timeouts.onProgress?.("agent.dispose:start");
      await withTimeout(probeDispose(), disposeTimeoutMs, "agent.dispose");
      items.push({ id: "agent.dispose", description: "探针会话清理", status: "pass", detail: "已释放探针 Agent" });
    } catch (error) {
      items.push({ id: "agent.dispose", description: "探针会话清理", status: "fail", detail: String((error as Error).message) });
    }
  }
  for (const remove of sessionEventListeners) remove();
  timeouts.onProgress?.("probe:complete");

  return {
    instanceId,
    probedAt: new Date().toISOString(),
    hostNodeVersion: process.version,
    nodeSqliteAvailable: sqliteOk,
    items,
    blockers,
  };
}

/**
 * 组合就绪后的探针调度。组合激活是 service-availability 驱动的，
 * apply 同步执行时 agent-loop/token-meter 可能尚未注册——真实探针已复现此误报。
 * 因此：延迟后首轮探测，若仍有阻塞点则按间隔重试，直到通过或次数用尽；
 * 每轮报告都落盘（attempt-N），最后一份同时写入 probe-latest，供人工核对。
 *
 * 生命周期：宿主热重载会先 apply 新实例再卸载旧实例。旧实例若还在等待/重试，
 * 必须能在卸载时被取消——否则它会继续探测并写入 probe-latest，
 * 用一份来自"已死实例"的失败报告污染最新能力结论（真实日志已复现）。
 */
export async function runDeferredProbe(
  ctx: HostContext,
  save: (report: ProbeReport, label: string) => Promise<void>,
  options: {
    initialDelayMs?: number;
    retryDelayMs?: number;
    maxAttempts?: number;
    timeouts?: ProbeTimeouts;
    /** 实例标识：写进报告，便于分辨同一进程里新旧实例 */
    instanceId?: string;
    /** 卸载信号：abort 后不再探测、不再落盘 */
    signal?: AbortSignal;
  } = {},
): Promise<ProbeReport | null> {
  const { initialDelayMs = 5000, retryDelayMs = 5000, maxAttempts = 4, timeouts, instanceId = "unknown", signal } = options;
  const sleep = (ms: number): Promise<void> =>
    new Promise<void>((resolve) => {
      if (signal?.aborted) return resolve();
      const timer = setTimeout(finish, ms);
      const onAbort = () => finish();
      function finish(): void {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  let report: ProbeReport | undefined;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    timeouts?.onProgress?.(`attempt-${attempt}:waiting`);
    await sleep(attempt === 1 ? initialDelayMs : retryDelayMs);
    if (signal?.aborted) {
      timeouts?.onProgress?.(`attempt-${attempt}:cancelled`);
      return null; // 已卸载：不探测、不落盘
    }
    timeouts?.onProgress?.(`attempt-${attempt}:start`);
    report = await probeHost(ctx, timeouts, instanceId);
    if (signal?.aborted) return null;
    await save(report, `attempt-${attempt}`);
    ctx.logger.info(`longtask: 探针第 ${attempt}/${maxAttempts} 轮完成，阻塞点 ${report.blockers.length} 个`);
    if (report.blockers.length === 0) break;
  }
  if (report && !signal?.aborted) await save(report, "latest");
  return report ?? null;
}

/** 把探针报告渲染为人类可读文本（保存到探针记录文件头部） */
export function renderProbeReport(report: ProbeReport): string {
  const lines = [
    `# DSH 宿主探针记录 · ${report.probedAt}`,
    `实例 ${report.instanceId} · Node ${report.hostNodeVersion} · node:sqlite ${report.nodeSqliteAvailable ? "可用" : "不可用"}`,
    "",
    "| 项 | 结果 | 说明 |",
    "|---|---|---|",
    ...report.items.map((i) => `| ${i.id} | ${i.status} | ${i.description}：${i.detail} |`),
    "",
    report.blockers.length > 0 ? `## 阻塞点\n${report.blockers.map((b) => `- ${b}`).join("\n- ")}` : "## 无阻塞点",
    "",
    "硬限额结论：仅在 svc.tokenMeter 与 projection.tokenUsage 均通过、且适配器能提供输入上界时，才可承诺严格模式硬上限；",
    "自动恢复：以任务持久记录为准，goal 驱动的 goal 在恢复后默认未激活（dsh-goal-round-driver README），恢复授权属宿主行为。",
  ];
  return lines.join("\n");
}
