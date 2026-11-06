import {createUserMessage, boundContextSummary, type UserMessage} from "@deepseek-ai/dsh-llm";
import type {HostContext} from "./host-types.js";
import type {Store} from "./store/store.js";
import type {Task, EventEnvelope, TaskBudgetState, MessageRecord} from "./types.js";
import {formatCny} from "./ledger/money.js";
import {pendingApprovals, type TaskApproval} from "./approval-waits.js";

declare module "@deepseek-ai/dsh-llm" {
  interface MessageSourceMap {"longtask-status": {kind: "longtask-status"; form: "notice"; summary: string; taskId: string; eventId: string}}
}
export const ROUTER_NOTICE_PROTOCOL = 2;
interface Notice {
  id: string; taskId: string; targetSessionId: string; eventId: string;
  taskStatus: string; pauseReason: string | null; message: UserMessage;
  delivery: "pending" | "delivered" | "superseded"; sessionSeq?: number;
  topic?: "task" | "approval";
  approvalKeys?: string[];
}
interface NoticeSession {
  id: string;
  snapshotEvents(): readonly {type: string; seq: number; data: {id?: string}}[];
  append(type: "user/message", message: UserMessage, options: {surfaceOp: "append"}): {seq: number};
}
const topic = (e: EventEnvelope) => e.type === "task.approval_changed" ? "approval" : "task";
const relevant = (e: EventEnvelope) => ["task.approval_changed", "task.paused", "task.resumed", "task.cancelled", "task.delivered", "task.router_bound"].includes(e.type)
  || (e.type === "task.continue" && e.payload.cause === "output_truncated");

/** Durable, summary-only status relay. Appends a visible plugin context without waking a model. */
export class RouterNotices {
  private closed = false;
  private dirty = false;
  private running: Promise<void> | undefined;
  private readonly stops: Array<() => void> = [];
  constructor(private readonly store: Store, private readonly ctx: HostContext) {
    const stop = store.subscribe?.(() => this.wake()); if (stop) this.stops.push(stop);
    const restored = ctx.on("agent/created", () => this.wake()); if (restored) this.stops.push(restored);
    const retry = setInterval(() => {if (store.list<Notice>("routerNotices").some(n => n.delivery === "pending")) this.wake()}, 30_000);
    retry.unref(); this.stops.push(() => clearInterval(retry));
  }
  wake(): void {
    if (this.closed) return;
    this.dirty = true;
    if (this.running) return;
    this.running = Promise.resolve().then(async () => {
      while (this.dirty && !this.closed) {this.dirty = false; await this.scan();}
    }).catch(error => this.ctx.logger.warn(`longtask: 主会话状态待投递：${String(error)}`))
      .finally(() => {this.running = undefined; if (this.dirty && !this.closed) this.wake()});
  }
  async idle(): Promise<void> {await this.running;}
  async dispose(): Promise<void> {this.closed = true; for (const stop of this.stops) stop(); await this.running;}

  private async scan(): Promise<void> {
    const snapshot = this.store.snapshot(), latest = new Map<string, EventEnvelope>();
    for (const id of snapshot.eventOrder) {const e = snapshot.events[id] as EventEnvelope; if (e && relevant(e)) latest.set(`${e.taskId}:${topic(e)}`, e);}
    for (const event of latest.values()) {
      const taskId = event.taskId;
      if (this.closed) return;
      const task = this.store.get<Task>("tasks", taskId);
      if (!task?.routerSessionId) continue;
      const id = `${taskId}:${task.routerSessionId}:${event.eventId}`;
      if (this.store.get("routerNotices", id)) continue;
      // The continuation event precedes dispatch; wait for its actual delivery result.
      if (event.type === "task.continue" && !this.store.list<MessageRecord>("messages").some(m => m.taskId === taskId && m.dedupeKey === event.dedupeKey)) continue;
      const text = this.content(task, event);
      await this.store.transact(data => {
        const notices = (data.collections.routerNotices ??= {});
        if (notices[id]) return;
        notices[id] = {id, taskId, eventId: event.eventId, targetSessionId: task.routerSessionId!, taskStatus: task.status, pauseReason: task.pauseReason,
          topic: topic(event), ...(topic(event) === "approval" ? {approvalKeys: pendingApprovals(data, taskId).map(a => a.key).sort()} : {}),
          message: createUserMessage({content: [{type: "text", text: text ?? "此状态已过期或无需通知"}],
            source: {kind: "longtask-status", form: "notice", summary: boundContextSummary(`${text?.split("\n")[0] ?? "长任务状态"} ${taskId} ${text?.split("\n")[3] ?? ""}`), taskId, eventId: event.eventId}}), delivery: text ? "pending" : "superseded"} satisfies Notice;
      });
    }
    for (const n of this.store.list<Notice>("routerNotices")) {
      if (this.closed) return;
      if (n.delivery !== "pending") continue;
      const task = this.store.get<Task>("tasks", n.taskId);
      const current = this.store.snapshot();
      const currentEvent = current.eventOrder.map(id => current.events[id] as EventEnvelope).findLast(e => e?.taskId === n.taskId && relevant(e) && topic(e) === (n.topic ?? "task"));
      const approvalSame = n.topic === "approval" && JSON.stringify(pendingApprovals(current, n.taskId).map(a => a.key).sort()) === JSON.stringify(n.approvalKeys ?? []);
      const sameStatus = task?.status === n.taskStatus || (approvalSame && ["active", "rework"].includes(task?.status ?? "") && ["active", "rework"].includes(n.taskStatus));
      if (!task || task.routerSessionId !== n.targetSessionId || !sameStatus || task.pauseReason !== n.pauseReason || currentEvent?.eventId !== n.eventId || (n.topic === "approval" && !approvalSame)) {
        await this.mark(n.id, "superseded"); continue;
      }
      const agent = this.ctx.get<{get(id: string): {session?: NoticeSession} | undefined}>("agents")?.get(n.targetSessionId);
      const session = agent?.session, sessions = this.ctx.get<{flush(session: NoticeSession): Promise<void>}>("sessions");
      if (!session || session.id !== n.targetSessionId || typeof session.append !== "function" || typeof session.snapshotEvents !== "function" || !sessions?.flush) continue;
      // A crash/flush failure after append must not produce another visible copy.
      const existing = session.snapshotEvents().find(e => e.type === "user/message" && e.data.id === n.message.id);
      const event = existing ?? session.append("user/message", n.message, {surfaceOp: "append"});
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([sessions.flush(session), new Promise<never>((_, reject) => {timer = setTimeout(() => reject(Error("主会话持久化超时，状态保持待投递")), 5000)})]);
      } finally {if (timer) clearTimeout(timer);}
      if (!this.closed) await this.mark(n.id, "delivered", event.seq);
    }
  }
  private async mark(id: string, delivery: Notice["delivery"], sessionSeq?: number) {
    await this.store.transact(data => {const n = data.collections.routerNotices?.[id] as Notice | undefined;
      if (n) {n.delivery = delivery; if (sessionSeq !== undefined) n.sessionSeq = sessionSeq;}});
  }
  private content(task: Task, e: EventEnvelope): string | null {
    const v = task.versions[task.currentVersion]!, b = this.store.get<TaskBudgetState>("budgets", task.id);
    let state: string, why: string, next: string;
    if (e.type === "task.approval_changed") {
      const saved = this.store.snapshot(), pending = pendingApprovals(saved, task.id);
      const last = saved.collections.taskApprovals?.[String(e.payload.approvalKey)] as TaskApproval | undefined;
      if (!last || last.taskVersion !== v.version) return null;
      const outcome = last.outcome === "allowed-once" ? "已批准一次" : last.outcome === "rejected" ? "已拒绝" : last.outcome === "cancelled" ? "已取消" : last.outcome === "unavailable" ? "审批通道不可用，未获授权" : "等待已失效";
      if (pending.length) {
        state = `等待用户审批（${pending.length}项）`;
        why = pending.map(a => `工具 ${a.toolName}；Worker会话 ${a.sessionId}；专家 ${a.relPath}；审批 ${a.requestId}；原因：${a.reason.slice(0, 160)}`).join("\n");
        if (last.status !== "pending") why += `\n最近处理：${last.toolName}，${outcome}。`;
        next = "请打开上述Worker会话中的DSH审批卡片，由用户批准或拒绝；在Router发送继续不等于批准。原工具等待宿主决定，不额外派工。";
      } else {
        state = last.status === "resolved" && last.outcome === "allowed-once" ? "审批已通过" : last.status === "resolved" ? "审批未通过" : "审批等待已失效";
        why = `工具 ${last.toolName}；Worker会话 ${last.sessionId}；审批 ${last.requestId}；${outcome}。`;
        next = last.outcome === "allowed-once" ? "等待状态已更新；宿主继续处理原工具调用，任务仍受当前状态与预算约束，插件不重复发送继续。" : "请在对应Worker核对宿主决定或失效原因；如需重试，由用户处理原任务和新的审批请求，插件不会自动批准或重放工具。";
      }
    } else if (e.type === "task.continue" && ["active", "rework"].includes(task.status)) {
      const delivery = this.store.list<MessageRecord>("messages").findLast(m => m.taskId === task.id && m.dedupeKey === e.dedupeKey)?.delivery;
      if (delivery === "delivered") {
        state = "单次输出截断，已自动接续"; why = "触发的是单次回复长度上限，原任务原预算内的接续指令已投递，后续准入成功才会继续执行；不代表任务总预算耗尽。";
        next = "无需在Worker里发送继续；如后续准入不足，会另行通知。";
      } else {
        state = "截断后的接续投递未确认"; why = `接续指令投递状态为${delivery ?? "未知"}，不能宣称已继续执行。`;
        next = "请在主会话核对任务消息与收件证据，不要在Worker反复发送继续，也不要自动重投结果未知的动作。";
      }
    } else if ((e.type === "task.paused" || e.type === "task.router_bound") && task.status === "paused" && !["user", "queued"].includes(task.pauseReason ?? "")) {
      state = "已暂停，需要处理";
      const saved = this.store.snapshot();
      const pause = saved.eventOrder.map(id => saved.events[id] as EventEnvelope).findLast(x => x?.taskId === task.id && x.type === "task.paused");
      why = String(pause?.payload.note ?? e.payload.note ?? task.pauseReason).slice(0, 1200);
      next = task.pauseReason === "budget" ? "请在本主会话查看此任务的账本及暂停原因；预算不足或费用待核对时，发送继续不能解除限制，也不会增加额度。" :
        `请在本主会话处理原因后说“恢复任务 ${task.id}，沿用原预算”；不要在Worker会话发送继续，Worker无权解除任务暂停。`;
    } else if (e.type === "task.delivered" && task.status === "delivered") {
      state = "已完成独立验收并交付"; why = "全部完成标准已通过独立验收。"; next = "可在本主会话查看任务报告与产物引用。";
    } else return null;
    const budget = b?.money ? `人民币总上限¥${formatCny(b.money.limitNano)}；核算¥${formatCny(b.money.settledNano)}；在途¥${formatCny(b.money.reservedNano)}；可用¥${formatCny(BigInt(b.money.limitNano)-BigInt(b.money.settledNano)-BigInt(b.money.reservedNano))}（核算非平台实扣）` :
      b ? `任务Token总上限${b.limitTokens}；已用${b.settledTokens}；在途${b.reservedTokens}；可用${b.limitTokens-b.settledTokens-b.reservedTokens}` : "尚未建立预算账本";
    return [`【长任务状态｜${state}】`, `任务：${task.id}；版本v${v.version}；状态：${task.status}${task.pauseReason ? "/"+task.pauseReason : ""}`,
      `目标：${v.goal.slice(0, 240)}`, `原因：${why}`, budget, next,
      "这是插件状态摘要，非用户恢复或追加预算的授权。Router应据此向用户说明状态；不得把本通知当作开工指令。"].join("\n");
  }
}
