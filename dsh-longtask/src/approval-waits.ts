import {randomUUID} from "node:crypto";
import type {Store, Tx, StoredData} from "./store/store.js";
import type {Task, TaskWait} from "./types.js";
import type {TaskSessionRecord} from "./tasks/task-service.js";
import type {SessionEvent} from "./session-events.js";

export interface TaskApproval extends TaskSessionRecord {
  key: string; requestId: string; taskVersion: number; toolName: string; reason: string; callId?: string;
  status: "pending" | "resolved" | "expired"; outcome?: string;
  askedAt: string; resolvedAt?: string; waitId: string;
}
const running = (t: Task) => ["active", "rework", "awaiting_acceptance"].includes(t.status);
function eligible(data: StoredData, a: TaskSessionRecord & {taskVersion?: number}): boolean {
  const t = data.collections.tasks?.[a.taskId] as Task | undefined;
  const lease = (data.collections.taskSessions?.[a.taskId] as Record<string, TaskSessionRecord> | undefined)?.[a.sessionKey];
  return Boolean(t && lease && !lease.revokedAt && lease.sessionId === a.sessionId && lease.role === a.role && lease.relPath === a.relPath && running(t)
    && (a.taskVersion === undefined || a.taskVersion === t.versions[t.currentVersion]?.version)
    && (a.role === "acceptor" ? t.status === "awaiting_acceptance" : ["active", "rework"].includes(t.status)));
}
export function pendingApprovals(data: StoredData, taskId: string): TaskApproval[] {
  return (Object.values(data.collections.taskApprovals ?? {}) as TaskApproval[])
    .filter(a => a.taskId === taskId && a.status === "pending" && eligible(data, a));
}

/** Observes host decisions; never answers approvals or starts a model turn. */
export class ApprovalWaits {
  private closed = false;
  private dirty = false;
  private running: Promise<void> | undefined;
  private readonly unsubscribe: () => void;
  constructor(private readonly store: Store, private readonly warn: (message: string) => void,
    private readonly now: () => Date = () => new Date()) {
    this.unsubscribe = store.subscribe?.(() => this.wake()) ?? (() => {});
  }
  private wake() {
    if (this.closed) return;
    this.dirty = true;
    if (this.running) return;
    this.running = Promise.resolve().then(async () => {
      while (this.dirty && !this.closed) {
        this.dirty = false;
        const pending = this.store.list<TaskApproval>("taskApprovals").filter(a => a.status === "pending");
        const snapshot = pending.length ? this.store.snapshot() : undefined;
        const stale = snapshot && pending.some(a => !eligible(snapshot, a));
        if (stale) await this.store.transact(data => {
          for (const a of Object.values(data.collections.taskApprovals ?? {}) as TaskApproval[]) {
            if (a.status === "pending" && !eligible(data, a)) this.finish(data, a, "expired", "task-or-session-changed");
          }
        });
      }
    }).catch(error => this.warn(`审批等待状态核对失败：${String(error)}`))
      .finally(() => {this.running = undefined; if (this.dirty && !this.closed) this.wake()});
  }
  async idle() {await this.running;}
  async dispose() {this.closed = true; this.unsubscribe(); await this.running;}
  /** Native approval promises do not survive a runtime reload; do not replay or grant them. */
  async restore(): Promise<string[]> {
    const affected = new Set<string>();
    if (this.store.list<TaskApproval>("taskApprovals").some(a => a.status === "pending")) await this.store.transact(data => {
      for (const a of Object.values(data.collections.taskApprovals ?? {}) as TaskApproval[]) if (a.status === "pending") {
        affected.add(a.taskId); this.finish(data, a, "expired", "runtime-reloaded");
      }
    });
    return [...affected];
  }
  async observe(lease: TaskSessionRecord, event: SessionEvent): Promise<void> {
    if (this.closed) return;
    if (event.type === "turn/end") {
      // Must enqueue behind an asked transaction that may not have committed yet.
      await this.store.transact(data => {for (const a of Object.values(data.collections.taskApprovals ?? {}) as TaskApproval[]) {
        if (a.sessionId === lease.sessionId && a.taskId === lease.taskId && a.status === "pending") this.finish(data, a, "expired", "turn-ended");
      }}); return;
    }
    if (!["approval/asked", "approval/decided"].includes(event.type)) return;
    const id = event.data.id;
    if (typeof id !== "string" || !id || id.length > 256) return;
    const key = `${lease.taskId}:${lease.sessionId}:${id}`;
    await this.store.transact(data => {
      const records = (data.collections.taskApprovals ??= {}), old = records[key] as TaskApproval | undefined;
      if (event.type === "approval/decided") {
        const outcome = ["allowed-once", "rejected", "cancelled", "unavailable"].includes(String(event.data.outcome)) ? String(event.data.outcome) : "unavailable";
        if (old?.status === "pending") this.finish(data, old, eligible(data, old) ? "resolved" : "expired", eligible(data, old) ? outcome : "task-or-session-changed");
        else if (!old) records[key] = {...lease, key, requestId: id, taskVersion: 0, toolName: "", reason: "", status: "resolved", outcome,
          askedAt: this.now().toISOString(), resolvedAt: this.now().toISOString(), waitId: ""} satisfies TaskApproval;
        return; // Tombstones prevent a late/replayed asked event reopening an already closed request.
      }
      if (old || !eligible(data, lease) || typeof event.data.toolName !== "string") return;
      const t = data.collections.tasks![lease.taskId] as Task, at = this.now().toISOString();
      const reason = (event.data as Record<string, unknown>).reason;
      const a: TaskApproval = {...lease, key, requestId: id, taskVersion: t.versions[t.currentVersion]!.version,
        toolName: event.data.toolName.slice(0, 160), reason: typeof reason === "string" ? reason.slice(0, 800) : "宿主请求用户审批",
        ...(typeof event.data.callId === "string" ? {callId: event.data.callId.slice(0, 256)} : {}), status: "pending", askedAt: at, waitId: `approval:${lease.sessionId}:${id}`};
      records[key] = a;
      const wait: TaskWait = {id: a.waitId, taskId: a.taskId, taskVersion: a.taskVersion, executorRelPath: a.relPath,
        kind: "user_approval", description: `等待用户审批 ${a.toolName}；Worker会话 ${a.sessionId}；审批 ${id}`,
        wakeCondition: a.waitId, status: "waiting", since: at, observed: true};
      (data.collections.taskWaits ??= {})[wait.id] = wait;
      t.waitingOn = {kind: wait.kind, description: wait.description, wakeCondition: wait.wakeCondition, since: at};
      this.changed(data, a);
    });
  }
  private finish(data: Tx, a: TaskApproval, status: "resolved" | "expired", outcome: string) {
    if (a.status !== "pending") return;
    a.status = status; a.outcome = outcome; a.resolvedAt = this.now().toISOString();
    const w = data.collections.taskWaits?.[a.waitId] as TaskWait | undefined;
    if (w?.status === "waiting") {w.status = status === "resolved" ? "woken" : "expired"; w.resolvedAt = a.resolvedAt; w.resolvedBy = `approval:${outcome}`;}
    const t = data.collections.tasks?.[a.taskId] as Task | undefined;
    if (t) {
      const next = (Object.values(data.collections.taskWaits ?? {}) as TaskWait[]).find(w => w.taskId === a.taskId && w.status === "waiting" && w.taskVersion === t.versions[t.currentVersion]?.version);
      t.waitingOn = running(t) && next ? {kind: next.kind, description: next.description, wakeCondition: next.wakeCondition, since: next.since} : null;
    }
    this.changed(data, a);
  }
  private changed(data: Tx, a: TaskApproval) {
    data.appendEvent({eventId: `evt_approval_${randomUUID()}`, taskId: a.taskId, taskVersion: a.taskVersion, source: "host", type: "task.approval_changed",
      payload: {approvalKey: a.key, status: a.status, outcome: a.outcome ?? null}, payloadRef: null, causedBy: null,
      dedupeKey: `approval:${a.key}:${a.status}`, createdAt: this.now().toISOString()});
  }
}
