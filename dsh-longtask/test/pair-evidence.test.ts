import { test } from "node:test";
import {loadSdk} from "./sdk-tools.js";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const load = () => import(pathToFileURL(join(process.cwd(), "..", "dsh-longtask-e2e", "evidence.mjs")).href);
function raw() {
  const taskId = "task_pair";
  const createdAt = "2026-11-04T12:01:00.000Z";
  const contentHash = createHash("sha256").update("真实比较表").digest("hex");
  const body = "请接手比较表";
  const m = { id: "m", taskId, kind: "expert_direct", fromRelPath: "A", toRelPath: "B", createdAt,
    delivery: "delivered", deliveredAt: createdAt, actionId: "turn-A:msg:0", sourceTurnId: "turn-A", dedupeKey: "expert-msg:task_pair:turn-A:msg:0",
    artifactRefs: ["table.md"], artifactVersions: { "table.md": contentHash }, body, bodyHash: createHash("sha256").update(body).digest("hex"),
    deliveryAttempts: [{ id: "attempt", startedAt: createdAt, finishedAt: createdAt, outcome: "delivered" }] };
  const s = { id: "s", taskId, kind: "summary", replyToId: "m", fromRelPath: "A", toRelPath: "B", excerpt: "初稿交接", createdAt, artifactRefs: ["table.md"], artifactVersions: { "table.md": contentHash } };
  const version = { version: 1, goal: "任务中预置的协议示例：" + body, criteria: [{ id: "c1", text: "交付比较表" }], executors: [{ relPath: "A" }, { relPath: "B" }], acceptor: { relPath: "R" } };
  return { taskId, state: { collections: { messages: { m, s },
    tasks: { [taskId]: { status: "delivered", currentVersion: 0, versions: [version] } },
    artifacts: { a: { taskId, location: "table.md", contentHash, createdAt, ownerRelPath: "A" }, b: { taskId, location: "recommendation.md", contentHash, createdAt, ownerRelPath: "B" } },
    taskSessions: { [taskId]: { B: { sessionId: "session-B", relPath: "B", role: "executor" } } } } },
    digest: { taskId, status: "delivered", goal: version.goal, criteriaVersion: 1, criteria: version.criteria, acceptor: "R", budget: null,
      executors: ["A", "B"], latestSummaries: [{ messageId: "s", from: "A", to: "B", summary: "初稿交接", artifactRefs: ["table.md"], causedByMessageId: "m", at: createdAt }],
      artifacts: [{ location: "table.md", hash: contentHash.slice(0, 12), owner: "A" }, { location: "recommendation.md", hash: contentHash.slice(0, 12), owner: "B" }] },
    observations: [
      { kind: "turn_end", taskId, sessionId: "session-B", instanceId: "i", order: 1, at: "2026-11-04T12:00:00.000Z", reason: "completed", waiting: "等 A 交接", turn: 1 },
      { kind: "consumed", taskId, sessionId: "session-B", instanceId: "i", order: 2, at: createdAt, dedupeKey: `${m.dedupeKey}#deliver` },
      { kind: "artifact_read", taskId, sessionId: "session-B", instanceId: "i", order: 3, at: createdAt, path: "table.md", hash: contentHash, turn: 2 },
      { kind: "turn_end", taskId, sessionId: "session-B", instanceId: "i", order: 4, at: createdAt, reason: "completed", waiting: null, turn: 2 },
    ], replay: { taskId, sourceTurnId: "turn-A", actionId: m.actionId, eventCount: 2 } };
}
const envelope = (stage2: unknown) => ({ scenario: "pair", status: "delivered", budget: { limitTokens: 500000, settledTokens: 200000, reservedTokens: 0 },
  accounting: { verified: true, actualTokens: 200000, descendants: [] }, controls: { boundExceeded: 0 }, stage2 });

test("审查26 D4：原始状态与宿主观测完整时才通过 pair", async () => {
  const { extractPairEvidence, assessScenario } = await load();
  const result = assessScenario(envelope(extractPairEvidence(raw())));
  assert.equal(result.passed, true, result.reasons.join("；"));
});

test("真机28：目标预置相同示例句不算正文转发，篡改目标塞入全文会失败", async () => {
  const { extractPairEvidence } = await load();
  const f = raw();
  assert.ok(f.digest.goal.includes(f.state.collections.messages.m.body));
  assert.equal(extractPairEvidence(f).digestContainsBody, false);
  f.digest.goal += "\n额外转发正文：" + f.state.collections.messages.m.body;
  assert.equal(extractPairEvidence(f).digestContainsBody, true);
});

test("审查26 D4：原始证据负向对照拒绝错误结束、伪版本和同记录双投", async () => {
  const { extractPairEvidence, assessScenario } = await load();
  const cases: Array<[string, (f: ReturnType<typeof raw>) => void]> = [
    ["短正文泄漏", (f) => { (f.digest as Record<string, unknown>).rawExpertBody = f.state.collections.messages.m.body; }],
    ["正文哈希不符", (f) => { f.state.collections.messages.m.body += "被修改"; }],
    ["无真实等待", (f) => { f.observations[0]!.waiting = null; }],
    ["无实际消费", (f) => { f.observations.splice(1, 1); }],
    ["交接后只有错误结束", (f) => { f.observations[3]!.reason = "error"; }],
    ["未读取交接产物", (f) => { f.observations.splice(2, 1); }],
    ["读到了不同版本", (f) => { f.observations[2]!.hash = "b".repeat(64); }],
    ["无关摘要版本键", (f) => { f.state.collections.messages.s.artifactVersions = { "wrong.md": "not-a-hash" } as never; }],
    ["版本不在产物历史", (f) => { f.state.collections.artifacts.a.contentHash = "c".repeat(64); }],
    ["同一记录实际双投", (f) => { f.state.collections.messages.m.deliveryAttempts.push({ ...f.state.collections.messages.m.deliveryAttempts[0]!, id: "second-attempt" }); }],
    ["没有真实事件重放", (f) => { f.replay.eventCount = 1; }],
    ["重复消费", (f) => { f.observations.push({ ...f.observations[1]!, order: 5 }); }],
    ["重放对象不匹配", (f) => { f.replay.sourceTurnId = "other-turn"; }],
  ];
  for (const [name, change] of cases) {
    const f = raw(); change(f);
    assert.equal(assessScenario(envelope(extractPairEvidence(f))).passed, false, name);
  }
});

test("真机28 pair：真实 Cordis 总线只收到一个原事件，重放限定到产品入口", async () => {
  const { Context } = await import("@deepseek-ai/cordis");
  const { createPairFixture } = await import(pathToFileURL(join(process.cwd(), "..", "dsh-longtask-e2e", "pair-fixture.mjs")).href);
  const root = new Context();
  const bus = root as unknown as { on(name: string, fn: (session: { id: string }, event: { type: string }) => void): () => void; emit(name: string, ...args: unknown[]): void };
  let tools = 0, ends = 0, productReplays = 0;
  root.provide("tools", { register: () => { tools++; return () => { tools--; }; } });
  const fixture = createPairFixture({ runId: "r", sender: "A", recipient: "B", artifactPath: "unused",
    getDependency: () => ({ state: "waiting", fromRelPath: "A" }),
    replayTurnEnd: () => { productReplays++; return true; },
    readSessions: () => [{ sessionId: "sender", relPath: "A", role: "executor" }] });
  try {
    const stop = bus.on("session/event", (_session, event) => { if (event.type === "turn/end") ends++; });
    fixture.install(root); fixture.bind("task");
    bus.emit("session/event", { id: "sender" }, { type: "assistant/message", data: { turn: 1, message: { content: [{ type: "text", text: '<longtask_message summary="交接" to="B">继续</longtask_message>' }] } } });
    bus.emit("session/event", { id: "sender" }, { type: "turn/end", data: { turn: 1, reason: { kind: "completed" } } });
    assert.equal(ends, 1); assert.equal(productReplays, 1); assert.equal(fixture.snapshot().eventCount, 2);
    fixture.dispose(); assert.equal(tools, 0);
    bus.emit("session/event", { id: "sender" }, { type: "turn/end", data: { turn: 1, reason: { kind: "completed" } } });
    assert.equal(ends, 2, "卸载后只有外部原事件"); assert.equal(productReplays, 1);
    stop();
  } finally { fixture.dispose(); }
});

test("真机28：真实 SDK 持久化拒绝同序号重复事件，夹具不得广播原 seq", async () => {
  const { materializeAppendBatch, assertContiguous } = await loadSdk("dsh-session-persistence");
  const event = { type: "turn/end", seq: 1, time: Date.now(), data: { turn: 1, reason: { kind: "completed" } } };
  assert.equal(materializeAppendBatch([event]).length, 1);
  assert.doesNotThrow(() => assertContiguous("test", materializeAppendBatch([event]), 1));
  assert.throws(() => assertContiguous("test", materializeAppendBatch([event, structuredClone(event)]), 1));
});
