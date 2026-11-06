import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtemp} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {Session} from "@deepseek-ai/dsh-session";
import {JsonStore} from "../src/store/store.js";
import {TokenLedger} from "../src/ledger/token-ledger.js";
import {TaskService} from "../src/tasks/task-service.js";
import {RouterNotices} from "../src/router-notices.js";
import type {HostContext} from "../src/host-types.js";
import type {Task, TaskBudgetState, EventEnvelope} from "../src/types.js";
const rootId = "session-owner", otherId = "session-unrelated";
const expert = (relPath: string) => ({relPath, division: "d", name: relPath, intro: "EXPERT_FULL_TEXT_MUST_NOT_LEAK", contentHash: "h", snapshottedAt: new Date().toISOString()});
async function fixture() {
  const path = join(await mkdtemp(join(tmpdir(), "longtask-notices-")), "state.json"), store = await JsonStore.load(path), ledger = new TokenLedger(store);
  const host: any = {stop: async () => true, createSession: async () => {throw Error("不得派工")}, releaseSession: async () => {}, sessionAlive: async () => false};
  const tasks = new TaskService({store, ledger, host});
  const task = await tasks.propose({goal: "形成交付物", criteria: [{id: "c1", text: "可读"}], deliverables: ["result.md"], executors: [expert("worker.md")], acceptor: expert("acceptor.md"), budget: {limitTokens: 500000, strict: true}}, null, "none", rootId);
  await store.transact(data => {const t = data.collections.tasks![task.id] as Task; t.status = "active"; t.versions[0]!.confirmedAt = new Date().toISOString();});
  await ledger.ensureBudget(task.id, 500000, true);
  const root = Session.create(rootId as any), other = Session.create(otherId as any), agents = new Map<string, any>();
  let wakes = 0, flushes = 0, failFlush = false;
  const live = (session: Session) => ({id: session.id, session, followup() {wakes++}, steer() {wakes++}, inject() {wakes++}});
  agents.set(otherId, live(other)); agents.set(rootId, live(root));
  const listeners = new Map<string, Array<() => void>>();
  const ctx: HostContext = {get: (name: string): any => name === "agents" ? {get: (id: string) => agents.get(id)} : name === "sessions" ? {flush: async () => {flushes++; if (failFlush) throw Error("injected flush failure")}} : undefined,
    on(name, fn) {const list = listeners.get(name) ?? []; list.push(fn); listeners.set(name, list); return () => {const i = list.indexOf(fn); if (i >= 0) list.splice(i, 1)}},
    logger: {info() {}, warn() {}, error() {}}, effect() {}, provide() {}};
  const notices = new RouterNotices(store, ctx);
  const texts = () => root.snapshotEvents().filter(e => e.type === "user/message").map(e => (e.data as any).content.map((c: any) => c.text ?? "").join(""));
  const settle = async () => {for (let n = 0; n < 5; n++) {await new Promise(r => setTimeout(r, 5)); await notices.idle()}};
  return {path, store, ledger, tasks, taskId: task.id, root, other, agents, ctx, notices, texts, settle,
    restore() {for (const fn of listeners.get("agent/created") ?? []) fn()}, setFailFlush(value: boolean) {failFlush = value},
    get wakes() {return wakes}, get flushes() {return flushes}, async close() {await notices.dispose(); await store.close()}};
}
test("预算暂停主动写入精确Router的真实SDK会话，无模型唤醒、无专家全文", async () => {
  const f = await fixture(); try {
    await f.tasks.pause(f.taskId, "budget", "下一请求及验收额度不足"); await f.settle();
    assert.equal(f.texts().length, 1); assert.equal(f.other.snapshotEvents().filter(e => e.type === "user/message").length, 0);
    const text = f.texts()[0]!; assert.match(text, /下一请求及验收额度不足/); assert.match(text, /主会话/); assert.match(text, /500000/);
    assert.match(text, /继续不能解除/); assert.doesNotMatch(text, /EXPERT_FULL_TEXT/); assert.equal(f.wakes, 0); assert.ok(f.flushes > 0);
    const event = f.root.snapshotEvents().find(e => e.type === "user/message")!; assert.equal((event.data as any).source.kind, "longtask-status");
    assert.equal((event.data as any).source.form, "notice"); assert.match((event.data as any).source.summary, /已暂停/); assert.ok((event.data as any).source.summary.length <= 120);
    assert.equal(event.surfaceOp, "append"); assert.equal(f.store.list<any>("routerNotices")[0].delivery, "delivered");
  } finally {await f.close()}
});
test("状态重放及其他事务不重复写主会话；人民币费用按原账本显示", async () => {
  const f = await fixture(); try {
    await f.store.transact(data => {(data.collections.budgets![f.taskId] as TaskBudgetState).money = {currency: "CNY", limitNano: "5000000000", settledNano: "12000000", reservedNano: "84000000", basis: "usage-priced-estimate"}});
    await f.tasks.pause(f.taskId, "budget", "金额预算不足"); await f.settle();
    f.notices.wake(); await f.store.transact(() => {}); await f.settle();
    assert.equal(f.texts().length, 1); assert.match(f.texts()[0]!, /核算¥0\.012/); assert.match(f.texts()[0]!, /在途¥0\.084/); assert.match(f.texts()[0]!, /可用¥4\.904/);
  } finally {await f.close()}
});
test("单次截断自动续做与总预算暂停使用不同主会话提示", async () => {
  const f = await fixture(); try {
    await f.store.transact(data => {data.appendEvent({eventId: "truncated", taskId: f.taskId, taskVersion: 1, source: "router", type: "task.continue", payload: {cause: "output_truncated"}, payloadRef: null, causedBy: null, dedupeKey: "truncated", createdAt: new Date().toISOString()})});
    await f.settle(); assert.equal(f.texts().length, 0, "记录续做意图不等于真实投递");
    await f.store.transact(data => {(data.collections.messages ??= {}).m = {id: "m", taskId: f.taskId, dedupeKey: "truncated", delivery: "delivered"}});
    await f.settle(); assert.match(f.texts()[0]!, /单次输出截断，已自动接续/); assert.match(f.texts()[0]!, /不代表任务总预算耗尽/);
    await f.tasks.pause(f.taskId, "budget", "本次需要额度大于可用余额"); await f.settle();
    assert.equal(f.texts().length, 2); assert.match(f.texts()[1]!, /已暂停，需要处理/); assert.equal(f.wakes, 0);
  } finally {await f.close()}
});
test("Router离线不借其他会话广播；恢复加载后投递一次", async () => {
  const f = await fixture(); try {
    const owner = f.agents.get(rootId); f.agents.delete(rootId);
    await f.tasks.pause(f.taskId, "watchdog", "连续输出截断已达上限"); await f.settle();
    assert.equal(f.texts().length, 0); assert.equal(f.other.snapshotEvents().length, 0); assert.equal(f.store.list<any>("routerNotices")[0].delivery, "pending");
    f.agents.set(rootId, owner); f.restore(); await f.settle();
    assert.equal(f.texts().length, 1); assert.match(f.texts()[0]!, new RegExp("恢复任务 " + f.taskId)); assert.match(f.texts()[0]!, /不要在Worker/);
  } finally {await f.close()}
});
test("主会话append成功而flush失败：重启按稳定消息id对账，不重复显示", async () => {
  const f = await fixture(); let restored: JsonStore | undefined, next: RouterNotices | undefined;
  try {
    f.setFailFlush(true); await f.tasks.pause(f.taskId, "budget", "费用待核对"); await f.settle(); assert.equal(f.texts().length, 1);
    assert.equal(f.store.list<any>("routerNotices")[0].delivery, "pending");
    await f.notices.dispose(); await f.store.close(); restored = await JsonStore.load(f.path); f.setFailFlush(false);
    next = new RouterNotices(restored, f.ctx); next.wake(); await next.idle();
    assert.equal(f.texts().length, 1); assert.equal(restored.list<any>("routerNotices")[0].delivery, "delivered");
  } finally {await next?.dispose(); await restored?.close(); await f.close()}
});
test("离线期间任务恢复后，过期暂停通知不再误投", async () => {
  const f = await fixture(); try {
    const owner = f.agents.get(rootId); f.agents.delete(rootId); await f.tasks.pause(f.taskId, "budget", "旧暂停原因"); await f.settle();
    await f.store.transact(data => {const t = data.collections.tasks![f.taskId] as Task; t.status = "active"; t.pauseReason = null;
      data.appendEvent({eventId: "resumed", taskId: f.taskId, taskVersion: 1, source: "user", type: "task.resumed", payload: {}, payloadRef: null, causedBy: null, dedupeKey: "resumed", createdAt: new Date().toISOString()});});
    f.agents.set(rootId, owner); f.restore(); await f.settle(); assert.equal(f.texts().length, 0);
    assert.equal(f.store.list<any>("routerNotices").filter(n => n.delivery === "pending").length, 0);
  } finally {await f.close()}
});
test("旧任务不猜Router，明确绑定后才回传；卸载后不再投递", async () => {
  const f = await fixture(); try {
    await f.store.transact(data => {delete (data.collections.tasks![f.taskId] as Task).routerSessionId});
    await f.tasks.pause(f.taskId, "watchdog", "单次输出截断"); await f.settle(); assert.equal(f.texts().length, 0);
    await f.tasks.bindRouter(f.taskId, rootId); await f.settle(); assert.equal(f.texts().length, 1);
    await f.notices.dispose(); await f.store.transact(data => {const t = data.collections.tasks![f.taskId] as Task; t.status = "active"; t.pauseReason = null;});
    await f.tasks.pause(f.taskId, "budget", "后续暂停"); await f.settle(); assert.equal(f.texts().length, 1);
  } finally {await f.close()}
});
