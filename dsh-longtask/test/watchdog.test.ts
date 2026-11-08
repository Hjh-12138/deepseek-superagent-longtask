import { test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { promises as fs } from "node:fs";
import { JsonStore } from "../src/store/store.js";
import { TokenLedger } from "../src/ledger/token-ledger.js";
import {
  Watchdog,
  watchdogDecision,
  instructionKey,
  recoverOnStartup,
  DEFAULT_WATCHDOG_CONFIG,
  COLLECTION_PROGRESS,
} from "../src/watchdog/watchdog.js";
import { TaskService, type TurnContext } from "../src/tasks/task-service.js";
import type { TaskProgressState, ExpertRef } from "../src/types.js";

function progress(patch: Partial<TaskProgressState>): TaskProgressState {
  return {
    taskId: "t1",
    lastSubstantiveAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
    recentInstructionHashes: [],
    corrections: 0,
    lastAttemptNote: null,
    ...patch,
  };
}

const WAITING = { active: true, descriptions: ["research/b.md 等待 research/a.md 交接比较表"] };
const NOT_WAITING = { active: false, descriptions: [] as string[] };

test("正常等待不算停滞：有未满足的等待就不催（三态之一）", () => {
  const d = watchdogDecision(progress({ recentInstructionHashes: ["a", "a"], lastSubstantiveAt: new Date(Date.now() - 6 * 3600_000).toISOString() }), WAITING, Date.now());
  assert.equal(d.action, "none");
  assert.equal(d.action === "none" && d.state, "waiting");
});

test("缓慢：超过 2 倍阈值、但没有重复指令 → 纠偏方向是'说明等待对象或给出下一步'", () => {
  const cfg = { ...DEFAULT_WATCHDOG_CONFIG, stallThresholdMs: 1000 };
  const slow = progress({ recentInstructionHashes: ["a", "b"], lastSubstantiveAt: new Date(Date.now() - 60_000).toISOString() });
  const d = watchdogDecision(slow, NOT_WAITING, Date.now(), cfg);
  assert.equal(d.action, "correct");
  assert.equal(d.action === "correct" && d.mode, "slow");
  assert.ok(d.action === "correct" && d.instruction.includes("等待对象"));
});

test("无效循环：同一指令重复且超阈值 → 纠偏方向是'换一种方法'", () => {
  const cfg = { ...DEFAULT_WATCHDOG_CONFIG, stallThresholdMs: 1000 };
  const loop = progress({ recentInstructionHashes: ["k", "k"], lastSubstantiveAt: new Date(Date.now() - 5_000).toISOString() });
  const d = watchdogDecision(loop, NOT_WAITING, Date.now(), cfg);
  assert.equal(d.action, "correct");
  assert.equal(d.action === "correct" && d.mode, "loop");
  assert.ok(d.action === "correct" && d.instruction.includes("换一种方法"));
});

test("有近期实质进展 → 不打扰", () => {
  const d = watchdogDecision(progress({ lastSubstantiveAt: new Date().toISOString() }), NOT_WAITING, Date.now());
  assert.equal(d.action, "none");
  assert.equal(d.action === "none" && d.state, "progress");
});

test("重复同一指令且无进展 → 先纠偏；纠偏用尽 → 带证据求助", () => {
  const cfg = { ...DEFAULT_WATCHDOG_CONFIG, stallThresholdMs: 1000 };
  const repeated = progress({ recentInstructionHashes: ["k", "k"], lastSubstantiveAt: new Date(Date.now() - 60_000).toISOString() });
  const d1 = watchdogDecision(repeated, NOT_WAITING, Date.now(), cfg);
  assert.equal(d1.action, "correct");

  const exhausted = progress({ ...repeated, corrections: 2 });
  const d2 = watchdogDecision(exhausted, NOT_WAITING, Date.now(), cfg);
  assert.equal(d2.action, "escalate");
  assert.ok(d2.action === "escalate" && d2.attempts.length > 0); // 带尝试记录
  assert.ok(d2.action === "escalate" && d2.question.includes("？")); // 带一个具体问题
  assert.ok(d2.action === "escalate" && d2.evidence.lastSubstantiveAt, "求助必须带最后实质进展时间");
});

test("instructionKey 归一化空白与大小写（重复发消息本身不算进展）", () => {
  assert.equal(instructionKey("请 继续  执行"), instructionKey("请继续执行"));
  assert.equal(instructionKey("Next Step: A"), instructionKey("next step: a"));
});

test("recordInstruction 只影响重复检测，不算实质进展", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-wd-"));
  const store = await JsonStore.load(join(dir, "state.json"));
  const wd = new Watchdog(store);
  await wd.recordInstruction("t1", "请继续执行");
  const p = store.get<TaskProgressState>(COLLECTION_PROGRESS, "t1")!;
  assert.equal(p.recentInstructionHashes.length, 1);
  assert.equal(p.lastAttemptNote, null);
  await store.close();
});

test("recordSubstantiveProgress 重置纠偏计数", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-wd-"));
  const store = await JsonStore.load(join(dir, "state.json"));
  const wd = new Watchdog(store);
  await wd.recordInstruction("t1", "a");
  await wd.recordSubstantiveProgress("t1", "产物已更新");
  const p = store.get<TaskProgressState>(COLLECTION_PROGRESS, "t1")!;
  assert.equal(p.corrections, 0);
  assert.equal(p.lastAttemptNote, "产物已更新");
  await store.close();
});

// ---------------------------------------------------------------------------
// 恢复器
// ---------------------------------------------------------------------------

function expert(relPath: string): ExpertRef {
  return { division: "d", relPath, name: relPath, intro: "", contentHash: "h", snapshottedAt: new Date().toISOString() };
}

async function makeWorld() {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-rec-"));
  const store = await JsonStore.load(join(dir, "state.json"));
  const ledger = new TokenLedger(store);
  const liveSessions = new Set<string>();
  const host = {
    async createSession() {
      const k = `s${liveSessions.size}`;
      liveSessions.add(k);
      return { sessionKey: k, sessionId: `sid-${liveSessions.size}`, dispose: async () => {} };
    },
    async deliver() {
      return "delivered" as const;
    },
    async stop() {
      return true;
    },
    async sessionAlive(k: string) {
      return liveSessions.has(k);
    },
    async releaseSession(k: string) {
      liveSessions.delete(k);
    },
  };
  const svc = new TaskService({ store, ledger, host });
  return { store, ledger, svc, liveSessions, host };
}

test("重启恢复：active 任务会话存活则续；暂停与预算不足不误启；投递未知登记为 needs_check", async () => {
  const { store, svc, ledger } = await makeWorld();
  const input = {
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [expert("e1.md")],
    acceptor: expert("r.md"),
    budget: { limitTokens: 500_000, strict: false },
  };
  const a = await svc.propose(input);
  await svc.confirm(a.id);
  const p = await svc.propose(input);
  await svc.confirm(p.id);
  await svc.pause(p.id, "user", "用户主动暂停");
  const b = await svc.propose(input);
  await svc.confirm(b.id);
  await svc.pause(b.id, "budget", "预算耗尽");

  // 制造一条投递结果未知的消息
  await store.transact((data) => {
    (data.collections["messages"] ??= {})["m-unknown"] = {
      id: "m-unknown",
      taskId: a.id,
      kind: "dispatch",
      delivery: "unknown",
    };
  });
  // 制造一条未结算预留（崩溃现场）
  await ledger.ensureBudget(a.id, 1000, false);
  await ledger.reserve(a.id, 100, 0);

  const report = await recoverOnStartup(store, { sessionAlive: async () => true }, ledger);
  // 只有 active 的 a 会被恢复；p（主动暂停）与 b（预算耗尽）不误启
  assert.deepEqual(report.reactivated, [a.id]);
  assert.ok(report.kept.some((k) => k.taskId === b.id && k.reason.includes("预算")));
  assert.ok(report.kept.some((k) => k.taskId === p.id && k.reason.includes("主动暂停")));
  assert.equal(report.unknownDeliveries[0]?.action, "needs_check");
  // 崩溃预留必须被对账，不能只统计不处理：留在 reserved 会永久占住 available
  assert.ok(report.unsettledReservations >= 1, "启动时发现未结算预留");
  assert.equal(report.reconciledReservations, report.unsettledReservations, "每一笔都被对账");
  assert.equal(ledger.listUnsettledReservations().length, 0, "对账后不再有悬挂的 reserved 预留");
  const after = ledger.getBudget(a.id)!;
  assert.equal(after.reservedTokens, 0, "在途占额归零");
  assert.ok(after.settledTokens >= 100, "按估算保守记账（宁可高估消耗）");
});

test("宿主已登记运行命令在turn结束前也有有限等待窗口，重放注册不延长",async()=>{
  const f=await makeWorld();try{
    const task=await f.svc.propose({goal:"g",criteria:[{id:"c",text:"c"}],deliverables:["d"],executors:[expert("a")],acceptor:expert("r"),budget:{limitTokens:2000000,strict:true}});
    await f.svc.confirm(task.id);await f.svc.observeBackgroundJob(task.id,"a","compile",false);
    const registered=f.store.list<{registeredAt:string}>("backgroundJobs")[0]!.registeredAt;
    await f.svc.observeBackgroundJob(task.id,"a","compile",false);
    assert.equal(f.store.list<{registeredAt:string}>("backgroundJobs")[0]!.registeredAt,registered);
    const now=Date.now();
    const wd=new Watchdog(f.store,{stallThresholdMs:1000,maxCorrections:2,jobTimeoutMs:7200000},()=>now+3600000);
    assert.deepEqual(wd.checkAll(),[],"不需要等turn/end建立taskWaits才识别真实命令");
    const expired=new Watchdog(f.store,{stallThresholdMs:1000,maxCorrections:2,jobTimeoutMs:7200000},()=>now+10800000);
    assert.equal(expired.checkAll()[0]!.action.action,"correct","真正超时仍可纠偏");
  }finally{await f.store.close();}
});
