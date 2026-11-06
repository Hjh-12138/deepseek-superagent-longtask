import { test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { promises as fs } from "node:fs";
import { JsonStore } from "../src/store/store.js";
import { TokenLedger } from "../src/ledger/token-ledger.js";
import {
  TaskService,
  decideAfterTurnEnd,
  type HostBridge,
  type SessionLease,
  type TurnContext,
  type TurnEndKind,
} from "../src/tasks/task-service.js";
import type { Criterion, ExpertRef, Verdict } from "../src/types.js";
import { DEFAULT_ADMISSION, requestAdmissionCost } from "../src/budget/admission.js";

const CRITERIA: Criterion[] = [
  { id: "c1", text: "交付比较表" },
  { id: "c2", text: "给出有依据的推荐" },
];

function expert(relPath: string, name = relPath): ExpertRef {
  return { division: "research", relPath, name, intro: "", contentHash: "hash", snapshottedAt: new Date().toISOString() };
}

interface FakeSession {
  key: string;
  sessionId: string;
  deliveries: string[];
  targets: Array<"next-turn" | "next-step">;
  dedupes: string[];
  stopped: boolean;
  alive: boolean;
  disposed: boolean;
}

function fakeHost(options: { dedupe?: boolean; failNext?: number; unknownNext?: number } = {}) {
  // 真实 HostBridge 不按 dedupeKey 去重；测试默认保留幂等是为了兼容旧断言，
  // 负向用例（重复事件/失败重试）必须显式关掉，否则会把宿主去重当成插件正确。
  const dedupe = options.dedupe ?? true;
  let failRemaining = options.failNext ?? 0;
  let unknownRemaining = options.unknownNext ?? 0;
  const sessions = new Map<string, FakeSession>();
  let counter = 0;
  const host: HostBridge = {
    async createSession(taskId, expertRef, role) {
      const sessionId = `sess-${++counter}`;
      const key = `${role}:${taskId}:${expertRef.relPath}:${counter}`;
      sessions.set(key, { key, sessionId, deliveries: [], targets: [], dedupes: [], stopped: false, alive: true, disposed: false });
      return { sessionKey: key, sessionId, dispose: async () => void (sessions.get(key)!.disposed = true) } satisfies SessionLease;
    },
    async deliver(sessionKey, content, dedupeKey, target = "next-turn") {
      const s = sessions.get(sessionKey);
      if (!s || !s.alive) return "failed";
      if (failRemaining > 0) {
        failRemaining--;
        return "failed"; // 明确投递失败：插件必须能重试，而不是当成已处理
      }
      if (unknownRemaining > 0) {
        // 已实际收件，但确认阶段抛错：宿主只能报 unknown
        unknownRemaining--;
        s.dedupes.push(dedupeKey);
        s.deliveries.push(content);
        s.targets.push(target);
        return "unknown";
      }
      if (dedupe && s.dedupes.includes(dedupeKey)) return "delivered"; // 宿主侧幂等
      s.dedupes.push(dedupeKey);
      s.deliveries.push(content);
      s.targets.push(target);
      return "delivered";
    },
    async stop(sessionKey) {
      const s = sessions.get(sessionKey);
      if (!s) return false;
      s.stopped = true; // 模拟 cancel({kind:"user"})：终止活动并清空队列
      return true;
    },
    async sessionAlive(key) {
      return sessions.get(key)?.alive ?? false;
    },
    async releaseSession(key) {
      sessions.delete(key);
    },
  };
  return {
    host,
    sessions,
    failNextDeliveries: (n: number) => void (failRemaining = n),
    unknownNextDeliveries: (n: number) => void (unknownRemaining = n),
  };
}

interface Extras {
  /** 并发上限（阶段3 审查 C3/C4 用） */
  maxConcurrentTasks?: number;
  /** 假宿主行为：关掉键去重、注入若干次投递失败（负向用例用） */
  host?: { dedupe?: boolean; failNext?: number; unknownNext?: number };
  /** 交付物是否真实存在于磁盘（P1-6：仅登记不算交付） */
  fileExists?: (abs: string) => boolean;
  fileSize?: (abs: string) => number | null;
  /** 磁盘回算哈希是否仍等于冻结值（P1-5） */
  verifyArtifact?: (location: string, expected: string) => Promise<boolean>;
}

async function makeService(
  inspectTurn?: (key: string) => Promise<TurnContext>,
  loadExpert?: (ref: ExpertRef) => Promise<string>,
  extras: Extras = {},
  shared?: { store: JsonStore; ledger: TokenLedger },
) {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-tasks-"));
  const store = shared?.store ?? (await JsonStore.load(join(dir, "state.json")));
  const ledger = shared?.ledger ?? new TokenLedger(store);
  const { host, sessions, failNextDeliveries, unknownNextDeliveries } = fakeHost(extras.host);
  // 交付物路径解析：测试里声明的是相对名（"比较表"），登记产物在 E:/e2e 下
  const svc = new TaskService({
    store,
    ledger,
    host,
    inspectTurn,
    loadExpert,
    resolveDeliverable: (p) => (p.includes("/") || p.includes("\\") ? p : `E:/e2e/${p}`),
    maxConcurrentTasks: extras.maxConcurrentTasks,
    fileExists: extras.fileExists,
    fileSize: extras.fileSize,
    verifyArtifact: extras.verifyArtifact,
  });
  return { store, ledger, svc, sessions, failNextDeliveries, unknownNextDeliveries };
}

function proposedInput(budget = { limitTokens: 200_000, strict: true }) {
  return {
    goal: "比较两个方案并交付推荐",
    criteria: CRITERIA,
    deliverables: ["比较表", "推荐"],
    executors: [expert("research/a.md"), expert("research/b.md")],
    acceptor: expert("research/reviewer.md"),
    budget,
  };
}

test("审查34：并发答复原子占位，排队答案完整保存并在出队时优先送达", async () => {
  const {svc,store,sessions}=await makeService(undefined,undefined,{maxConcurrentTasks:1,host:{dedupe:false}});
  const tasks=[];
  for(let i=0;i<2;i++){
    const t=await svc.propose({...proposedInput({limitTokens:1_000_000,strict:true}),deliverables:[`answer-${i}`]});
    await svc.confirm(t.id);await svc.pause(t.id,"missing_input","口径？");tasks.push(t);
  }
  const answers=tasks.map((_,i)=>`ANSWER-${i}-${"完整答案".repeat(60)}`);
  const results=await Promise.all(tasks.map((t,i)=>svc.answerClarification(t.id,answers[i]!)));
  assert.equal(svc.listTasks().filter(t=>t.status==="active").length,1);
  assert.equal(results.filter(x=>x.action==="queued").length,1);
  const waiting=tasks.find(t=>svc.getTask(t.id)!.pauseReason==="queued")!;
  const index=tasks.indexOf(waiting);const pending=svc.getClarificationAnswers(waiting.id);
  assert.equal(pending[0]!.answer,answers[index]);assert.equal(pending[0]!.delivery,"pending");
  await svc.answerClarification(waiting.id,answers[index]!);
  assert.equal(svc.getClarificationAnswers(waiting.id).length,1,"重复提交不会创建第二条答案");
  await svc.pause(tasks.find(t=>t.id!==waiting.id)!.id,"user","让出名额");await svc.releaseQueued(1);
  assert.equal(svc.getClarificationAnswers(waiting.id)[0]!.delivery,"delivered");
  assert.equal([...sessions.values()].flatMap(s=>s.deliveries).filter(m=>m.includes(answers[index]!)).length,1);
  await store.close();
});

test("审查34：答复预算不足保留正文，恢复后只补投答案；unknown 必须对账",async()=>{
  const f=await makeService(undefined,undefined,{host:{dedupe:false}});
  const t=await f.svc.propose(proposedInput());await f.svc.confirm(t.id);await f.svc.pause(t.id,"missing_input","问题");
  const r=await f.ledger.reserve(t.id,1,1);assert.equal(r.ok,true);
  if(r.ok)await f.ledger.settle(r.reservationId,{uncachedInputTokens:199999,outputTokens:0,cacheReadTokens:0,cacheWriteTokens:0},"用尽");
  assert.equal((await f.svc.answerClarification(t.id,"FULL-BUDGET-ANSWER")).action,"paused");
  assert.equal(f.svc.getClarificationAnswers(t.id)[0]!.delivery,"pending");
  await f.svc.grantBudget(t.id,200000,"测试恢复","answer-grant");await f.svc.resume(t.id);
  assert.equal(f.svc.getClarificationAnswers(t.id)[0]!.delivery,"delivered");
  await f.svc.pause(t.id,"missing_input","第二个问题");f.unknownNextDeliveries(1);
  await f.svc.answerClarification(t.id,"UNKNOWN-ANSWER");
  const answer=f.svc.getClarificationAnswers(t.id).find(a=>a.answer==="UNKNOWN-ANSWER")!;
  assert.equal(answer.delivery,"unknown");
  const count=()=>[...f.sessions.values()].flatMap(s=>s.deliveries).filter(m=>m.includes("UNKNOWN-ANSWER")).length;
  const before=count();await f.svc.resume(t.id);assert.equal(count(),before,"未知投递不盲投");
  await f.svc.reconcileClarificationAnswer(t.id,answer.id,"delivered","实际收件证据");
  await f.svc.resume(t.id);assert.equal(count(),before);await f.store.close();
});

test("审查34：恢复验收先认领，排队保留验收阶段和原轮次",async()=>{
  const old=await makeService();
  const a=await old.svc.propose({...proposedInput(),deliverables:["other"]});await old.svc.confirm(a.id);
  const b=await old.svc.propose({...proposedInput(),deliverables:["accept"]});await old.svc.confirm(b.id);
  await old.svc.registerArtifact(b.id,"E:/e2e/accept","a".repeat(64),"research/a.md");
  const round=await old.svc.runAcceptance(b.id,"test-round","research/a.md");
  await old.store.transact(d=>{(d.collections.tasks![b.id] as any).status="awaiting_acceptance";});
  const recovered=await makeService(undefined,undefined,{maxConcurrentTasks:1},{store:old.store,ledger:old.ledger});
  assert.equal(await recovered.svc.resumeAfterRestart(b.id),"queued");
  assert.equal(recovered.sessions.size,0,"未获得名额不得创建验收会话");
  assert.equal(recovered.svc.getTask(b.id)!.resumeStatus,"awaiting_acceptance");
  await recovered.svc.pause(a.id,"user","让出");await recovered.svc.releaseQueued(1);
  assert.equal(recovered.svc.getTask(b.id)!.status,"awaiting_acceptance");
  assert.ok([...recovered.sessions.values()].every(s=>s.key.startsWith("acceptor:")));
  assert.ok([...recovered.sessions.values()][0]!.deliveries[0]!.includes(round));await old.store.close();
});

test("审查34：交接 outbox 在名额认领后补投，返工任务实际排队并恢复返工阶段",async()=>{
  const f=await makeService();
  const other=await f.svc.propose({...proposedInput(),deliverables:["other"]});await f.svc.confirm(other.id);
  const t=await f.svc.propose(dependentInput());await f.svc.confirm(t.id);
  await f.svc.registerArtifact(t.id,"E:/e2e/比较表","a".repeat(64),"research/a.md");f.failNextDeliveries(1);
  const msg=await f.svc.sendExpertMessage(t.id,"research/a.md","research/b.md","OUTBOX-RETRY",{actionId:"pending",artifactRefs:["E:/e2e/比较表"]});
  const g=await makeService(undefined,undefined,{maxConcurrentTasks:1},{store:f.store,ledger:f.ledger});
  assert.equal(await g.svc.resumeAfterRestart(t.id),"queued");assert.equal(g.sessions.size,0);assert.equal(g.svc.getExpertMessage(t.id,msg.messageId!)!.delivery,"failed");
  await g.svc.pause(other.id,"user","让出");await g.svc.releaseQueued(1);assert.equal(g.svc.getExpertMessage(t.id,msg.messageId!)!.delivery,"delivered");
  await g.svc.registerArtifact(t.id,"E:/e2e/推荐","b".repeat(64),"research/b.md");
  await f.store.transact(d=>{(d.collections.tasks![t.id] as any).status="awaiting_acceptance";});
  const roundId=await g.svc.runAcceptance(t.id,"actual-gap","research/a.md");
  const round=g.svc.latestPendingRound(t.id)!;
  await g.svc.recordVerdicts(t.id,validVerdicts("research/reviewer.md",CRITERIA,1,{c1:{outcome:"gap",gap:"UNIQUE-REWORK-GAP",evidence:"表格第二项缺依据"}}),round.artifactHashes,roundId);
  await f.store.transact(d=>{(d.collections.tasks![t.id] as any).status="rework";(d.collections.tasks![other.id] as any).status="active";(d.collections.tasks![other.id] as any).priority=9;});
  const policy=await g.svc.enforceConcurrency(1);assert.ok(policy.queued.includes(t.id));assert.equal(g.svc.getTask(t.id)!.status,"paused");assert.equal(g.svc.getTask(t.id)!.resumeStatus,"rework");
  await g.svc.pause(other.id,"user","让出");await g.svc.releaseQueued(1);assert.equal(g.svc.getTask(t.id)!.status,"rework");
  assert.ok([...g.sessions.values()].find(s=>s.alive&&s.key.includes("research/a.md"))!.deliveries.at(-1)!.includes("UNIQUE-REWORK-GAP"));await f.store.close();
});

test("审查34：重建 Worker 收到多条完整交接、消息身份和完整哈希，Router 不收原文",async()=>{
  const {svc,sessions,store}=await makeService(undefined,undefined,{host:{dedupe:false}});
  const t=await svc.propose(proposedInput({limitTokens:1000000,strict:true}));await svc.confirm(t.id);
  await svc.registerArtifact(t.id,"E:/e2e/比较表","a".repeat(64),"research/a.md");
  const bodies=["前文".repeat(180)+"FIRST-TAIL", "第二决定".repeat(100)+"SECOND-TAIL"];
  const ids=[];for(let i=0;i<2;i++)ids.push((await svc.sendExpertMessage(t.id,"research/a.md","research/b.md",bodies[i]!,{actionId:`handoff-${i}`,artifactRefs:["E:/e2e/比较表"]})).messageId!);
  for(const session of sessions.values())session.alive=false;
  await svc.resumeAfterRestart(t.id);
  const current=[...sessions.values()].find(s=>s.alive&&s.key.includes("research/b.md"))!;
  for(const b of bodies)assert.ok(current.deliveries[0]!.includes(b));for(const id of ids)assert.ok(current.deliveries[0]!.includes(id));assert.ok(current.deliveries[0]!.includes("a".repeat(64)));
  const digest=JSON.stringify(svc.routerDigest(t.id));for(const b of bodies)assert.ok(!digest.includes(b));await store.close();
});

test("审查34：unknown 普通回复对账唤醒相同等待，保留未满足的另一条等待",async()=>{
  const {svc,store,unknownNextDeliveries}=await makeService(async()=>({status:"active",reportedNextSteps:[],claimedDone:false,activeCommand:false,pendingReply:true,unhandledEvent:false}));
  const t=await svc.propose(proposedInput());await svc.confirm(t.id);await svc.onTurnEnd(t.id,"research/a.md","completed","wait");
  const wait=svc.listWaits(t.id)[0]!;const other=await svc.registerWait(t.id,{kind:"user_reply",wakeCondition:"user_reply",description:"待决定"});
  unknownNextDeliveries(1);const m=await svc.sendExpertMessage(t.id,"research/b.md","research/a.md","答复",{actionId:"reconcile"});
  await svc.reconcileExpertMessage(t.id,m.messageId!,"delivered","已核对实际收件");
  assert.equal(svc.listWaits(t.id).find(w=>w.id===wait.id)!.status,"woken");assert.equal(svc.listWaits(t.id).find(w=>w.id===other)!.status,"waiting");assert.equal(svc.getTask(t.id)!.waitingOn!.wakeCondition,"user_reply");await store.close();
});

test("审查34：同名等待跨版本隔离，旧消息对账不能解除新版本等待",async()=>{
  const f=await makeService(async()=>({status:"active",reportedNextSteps:[],claimedDone:false,activeCommand:false,pendingReply:true,unhandledEvent:false}));
  const t=await f.svc.propose(proposedInput({limitTokens:1000000,strict:true}));await f.svc.confirm(t.id);await f.svc.onTurnEnd(t.id,"research/a.md","completed","v1-wait");
  f.unknownNextDeliveries(1);const old=await f.svc.sendExpertMessage(t.id,"research/b.md","research/a.md","旧回复",{actionId:"old"});
  await f.svc.reopenWithCriteria(t.id,{criteria:[{id:"c3",text:"新标准"}],reason:"明确新版本"});
  await f.svc.onTurnEnd(t.id,"research/a.md","completed","v2-wait");
  const wait=f.svc.listWaits(t.id).find(w=>w.taskVersion===2&&w.status==="waiting")!;assert.ok(wait,"同条件也必须创建新版本等待");
  await f.svc.reconcileExpertMessage(t.id,old.messageId!,"delivered","旧消息实际收件");assert.equal(f.svc.listWaits(t.id).find(w=>w.id===wait.id)!.status,"waiting");
  await f.svc.sendExpertMessage(t.id,"research/b.md","research/a.md","新版回复",{actionId:"new"});assert.equal(f.svc.listWaits(t.id).find(w=>w.id===wait.id)!.status,"woken");
  await f.svc.sweepStaleWaits(t.id);assert.ok(f.svc.listWaits(t.id).filter(w=>w.taskVersion===1).every(w=>w.status==="expired"));await f.store.close();
});

test("审查34：未知后台等待有时限，不会永久屏蔽监督；显式 0 不关闭产物互斥",async()=>{
  const f=await makeService(undefined,undefined,{maxConcurrentTasks:0});const t=await f.svc.propose(proposedInput());await f.svc.confirm(t.id);
  const id=await f.svc.registerWait(t.id,{kind:"background_command",wakeCondition:"background:research/a.md",description:"无法观察"});
  await f.store.transact(d=>{(d.collections.taskWaits![id] as any).since="2000-01-01T00:00:00Z";});await f.svc.sweepStaleWaits(t.id);assert.equal(f.svc.listWaits(t.id).find(w=>w.id===id)!.status,"expired");
  const b=await f.svc.propose(proposedInput());await f.svc.confirm(b.id);assert.equal(f.svc.getTask(b.id)!.pauseReason,"queued");
  await f.svc.pause(t.id,"user","让出");await f.svc.releaseQueued(0);assert.equal(f.svc.getTask(b.id)!.status,"active");await f.store.close();
});

test("审查34：原后台会话丢失先核对而不重跑，证据确认结束后可恢复",async()=>{
  const {svc,store,sessions}=await makeService();const t=await svc.propose(proposedInput());await svc.confirm(t.id);
  await svc.observeBackgroundJob(t.id,"research/a.md","orphan",false);
  for(const s of sessions.values())s.alive=false;
  const before=sessions.size;assert.equal(await svc.resumeAfterRestart(t.id),"needs-reconcile");assert.equal(sessions.size,before);assert.equal(svc.getTask(t.id)!.pauseReason,"missing_input");
  await assert.rejects(()=>svc.reconcileBackgroundJob(t.id,"research/a.md","orphan",""),/证据/);
  await svc.reconcileBackgroundJob(t.id,"research/a.md","orphan","核对持久日志及实际产物，命令已经结束");
  await svc.resume(t.id);assert.equal(svc.getTask(t.id)!.status,"active");assert.ok([...sessions.values()].some(s=>s.alive));await store.close();
});

function dependentInput() {
  const input = proposedInput();
  return { ...input, assignments: [
    { relPath: "research/a.md", deliverables: [input.deliverables[0]!] },
    { relPath: "research/b.md", deliverables: [input.deliverables[1]!], waitFor: { fromRelPath: "research/a.md", artifactRefs: [input.deliverables[0]!] } },
  ] };
}

test("审查29：确认分工先记录调度等待，B 不创建会话也不消耗预算", async () => {
  const { svc, store, sessions, ledger } = await makeService();
  try {
    const task = await svc.propose(dependentInput()); await svc.confirm(task.id);
    assert.equal(sessions.size, 1); assert.ok([...sessions.values()][0]!.key.includes("research/a.md"));
    assert.equal(svc.getExecutorDependency(task.id, "research/b.md").state, "waiting");
    assert.equal(await svc.executorReady(task.id, "research/b.md"), false);
    assert.equal(ledger.getBudget(task.id)!.settledTokens, 0);
    assert.equal(Object.values(store.snapshot().events).filter((e) => (e as { type: string }).type === "expert.waiting").length, 1);
    const decision = await svc.onTurnEnd(task.id, "research/b.md", "completed", "提前声称完成");
    assert.equal(decision?.action, "wait"); assert.equal(svc.getTask(task.id)!.status, "active");
    assert.equal(sessions.size, 1, "不催促等待者补报告，不创建验收会话");
  } finally { await store.close(); }
});

test("审查29：文件存在或无版本留言不能唤醒 B，真实交接只启动一次", async () => {
  const { svc, store, sessions } = await makeService(undefined, undefined, { host: { dedupe: false } });
  try {
    const task = await svc.propose(dependentInput()); await svc.confirm(task.id);
    await svc.registerArtifact(task.id, "E:/e2e/比较表", "a".repeat(64), "research/a.md");
    assert.equal(await svc.executorReady(task.id, "research/b.md"), false, "仅有文件登记不是交接事件");
    const unrelated = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "表在磁盘", { actionId: "no-version" });
    assert.equal(unrelated.delivery, "pending"); assert.equal(sessions.size, 1);
    const sent = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "请按版本接手", { actionId: "valid-handoff", artifactRefs: ["E:/e2e/比较表"] });
    assert.equal(sent.delivery, "delivered"); assert.equal(await svc.executorReady(task.id, "research/b.md"), true);
    assert.equal(sessions.size, 2); assert.equal(svc.getExecutorDependency(task.id, "research/b.md").messageId, sent.messageId);
    const replay = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "请按版本接手", { actionId: "valid-handoff", artifactRefs: ["E:/e2e/比较表"] });
    assert.equal(replay.duplicate, true);
    assert.equal([...sessions.values()].find((s) => s.key.includes("research/b.md"))!.deliveries.filter((d) => d.includes("请按版本接手")).length, 1);
    await svc.proposeNewVersion(task.id, { goal: "确认新版本后重新交接" }); await svc.confirm(task.id);
    assert.equal(await svc.executorReady(task.id, "research/b.md"), false, "旧版本消息不能解除新版本依赖");
  } finally { await store.close(); }
});

test("审查29：分工与前置交接在开工前校验，循环或错误负责人不创建任务", async () => {
  const { svc, store, sessions } = await makeService();
  try {
    const cycle = dependentInput();
    Object.assign(cycle.assignments[0]!, { waitFor: { fromRelPath: "research/b.md", artifactRefs: ["推荐"] } });
    await assert.rejects(svc.propose(cycle), /循环/);
    const wrong = dependentInput(); wrong.assignments[1]!.waitFor!.artifactRefs = ["推荐"];
    await assert.rejects(svc.propose(wrong), /前置交接/);
    const duplicate = dependentInput(); duplicate.assignments[1]!.deliverables = ["比较表"];
    await assert.rejects(svc.propose(duplicate), /只有一位/);
    assert.equal(svc.listTasks().length, 0); assert.equal(sessions.size, 0);
  } finally { await store.close(); }
});

test("审查29：A 未结束轮次而 B 先完成，归属仍由成功写入观测判定", async () => {
  const { svc, store } = await makeService();
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-owner-")); const table = join(dir, "table.md"), rec = join(dir, "rec.md");
  try {
    const task = await svc.propose({ ...proposedInput(), deliverables: [table, rec] }); await svc.confirm(task.id);
    await fs.writeFile(table, "A 写的比较表"); await svc.noteArtifactWrite(task.id, "research/a.md", table, "A:write:1");
    await fs.writeFile(rec, "B 写的推荐"); await svc.noteArtifactWrite(task.id, "research/b.md", rec, "B:write:1");
    await svc.registerTurnArtifacts(task.id, "research/b.md");
    const artifacts = (svc.routerDigest(task.id) as { artifacts: Array<{ location: string; owner: string }> }).artifacts;
    assert.equal(artifacts.find((a) => a.location === table)!.owner, "research/a.md");
    assert.equal(artifacts.find((a) => a.location === rec)!.owner, "research/b.md");
  } finally { await store.close(); }
});

test("审查29：替换上游同步交接负责人和依赖，旧消息不能越过新人选", async () => {
  const { svc, store } = await makeService();
  try {
    const task = await svc.propose(dependentInput()); await svc.confirm(task.id);
    await svc.registerArtifact(task.id, "E:/e2e/比较表", "a".repeat(64), "research/a.md");
    await svc.changeExecutor(task.id, { add: expert("research/c.md"), remove: "research/a.md", reason: "C 接手上游" });
    assert.equal(svc.getExecutorDependency(task.id, "research/b.md").fromRelPath, "research/c.md");
    assert.equal(svc.getExecutorDependency(task.id, "research/b.md").state, "waiting");
    assert.equal((svc.routerDigest(task.id) as { artifacts: Array<{ owner: string }> }).artifacts[0]!.owner, "research/c.md");
    await assert.rejects(svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "旧人选交接", { actionId: "old", artifactRefs: ["E:/e2e/比较表"] }), /不是本任务/);
    await svc.sendExpertMessage(task.id, "research/c.md", "research/b.md", "由新人选交接", { actionId: "new", artifactRefs: ["E:/e2e/比较表"] });
    assert.equal(await svc.executorReady(task.id, "research/b.md"), true);
  } finally { await store.close(); }
});

/** 组装合法验收结论（绑定真实验收人与标准版本） */
function validVerdicts(acceptorRelPath: string, criteria: Criterion[], version: number, overrides?: Partial<Record<string, Partial<Verdict>>>): Verdict[] {
  return criteria.map((c) => ({
    criterionId: c.id,
    outcome: "pass",
    evidence: `证据：产物已核对 [${c.id}]`,
    gap: null,
    criteriaVersion: version,
    artifactHashes: {},
    verdictBy: acceptorRelPath,
    createdAt: new Date().toISOString(),
    ...(overrides?.[c.id] ?? {}),
  }));
}

test("确认前不消耗执行预算、不分派；确认后同事务落盘并分派", async () => {
  const { svc, sessions } = await makeService();
  const task = await svc.propose(proposedInput());
  assert.equal(task.status, "proposed");
  assert.equal([...sessions.values()].length, 0); // 未确认无会话

  await svc.confirm(task.id);
  const confirmed = svc.getTask(task.id)!;
  assert.equal(confirmed.status, "active");
  assert.ok(confirmed.versions[0]!.confirmedAt);
  const all = [...sessions.values()];
  assert.equal(all.filter((s) => s.deliveries.length === 1).length, 2);
});

test("Worker 初始消息携带目标/标准/专家定义/报告协议（P1-5）", async () => {
  const { svc, sessions } = await makeService(undefined, async (ref) => `专家定义全文：${ref.relPath} 的工作方法`);
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const brief = [...sessions.values()].find((s) => s.key.includes("research/a.md"))!.deliveries[0]!;
  assert.ok(brief.includes("比较两个方案并交付推荐"), "包含目标");
  assert.ok(brief.includes("[c1] 交付比较表"), "包含标准");
  assert.ok(brief.includes("[c2] 给出有依据的推荐"), "包含第二条标准");
  assert.ok(brief.includes("专家定义全文：research/a.md"), "包含专家定义全文");
  assert.ok(brief.includes("<longtask_report>"), "包含报告协议");
  assert.ok(brief.includes("比较表"), "包含交付物");
});

test("turn/end 正常结束：有缺项则交回执行者，同一轮重复事件不重复投递", async () => {
  const turnCtx: TurnContext = { status: "active", reportedNextSteps: ["补齐推荐依据"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false };
  const { svc, sessions } = await makeService(async () => turnCtx);
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);

  const d1 = await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  assert.ok(d1 && d1.action === "dispatch_next");
  const executorSession = [...sessions.values()].find((s) => s.key.includes("research/a.md"))!;
  const afterFirst = executorSession.deliveries.length;

  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1"); // 重复送达
  assert.equal(executorSession.deliveries.length, afterFirst);
});

/** 登记两项交付物的产物（验收齐备性预检要求产物已落盘） */
async function registerDeliverables(svc: TaskService, taskId: string, owner = "research/a.md") {
  await svc.registerArtifact(taskId, "E:/e2e/比较表", "sha256-compare", owner);
  await svc.registerArtifact(taskId, "E:/e2e/推荐", "sha256-recommend", owner);
}

test("声明做完 → 验收轮次登记 → 缺项退回 → 补齐再验收 → 全部通过才交付", async () => {
  let claimed = false;
  const { svc } = await makeService(async () => ({
    status: "active",
    reportedNextSteps: [],
    claimedDone: claimed,
    activeCommand: false,
    pendingReply: false,
    unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);

  claimed = true;
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  assert.equal(svc.getTask(task.id)!.status, "awaiting_acceptance");
  const round1 = svc.latestPendingRound(task.id)!;
  assert.ok(round1, "验收轮次已登记");
  assert.equal(round1.criteriaVersion, 1);

  const reviewer = "research/reviewer.md";
  // 第一版验收：故意漏掉一项（阶段 1 出口场景）
  const gapVerdicts = validVerdicts(reviewer, CRITERIA, 1, { c2: { outcome: "gap", gap: "推荐项缺失", evidence: "无推荐内容" } });
  const outcome1 = await svc.recordVerdicts(task.id, gapVerdicts, round1.artifactHashes, round1.roundId);
  assert.equal(outcome1, "rework");
  assert.equal(svc.getTask(task.id)!.status, "rework");
  assert.equal(svc.latestPendingRound(task.id), undefined); // 轮次已结算

  // 重复提交同一轮结论：幂等，不重复退回
  await svc.recordVerdicts(task.id, gapVerdicts, round1.artifactHashes, round1.roundId);
  assert.equal(svc.getTask(task.id)!.status, "rework");

  // 补齐后重新验收（新轮次）
  claimed = true;
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-2");
  assert.equal(svc.getTask(task.id)!.status, "awaiting_acceptance");
  const round2 = svc.latestPendingRound(task.id)!;

  const passVerdicts = validVerdicts(reviewer, CRITERIA, 1);
  const outcome2 = await svc.recordVerdicts(task.id, passVerdicts, round2.artifactHashes, round2.roundId);
  assert.equal(outcome2, "delivered");
  assert.equal(svc.getTask(task.id)!.status, "delivered");
});

test("交付物未落盘就声明完成：直接退回执行者，不建验收轮次、不开验收会话", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: true, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  // 只登记一项，缺「推荐」
  await svc.registerArtifact(task.id, "E:/e2e/比较表", "sha256-compare", "research/a.md");

  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  assert.equal(svc.getTask(task.id)!.status, "rework", "缺交付物 → 退回而不是进入待验收");
  assert.equal(svc.latestPendingRound(task.id), undefined, "不得登记验收轮次");
  assert.equal([...sessions.values()].filter((s) => s.key.startsWith("acceptor:")).length, 0, "不得创建验收会话");
  const executor = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!;
  const last = executor.deliveries.at(-1)!;
  assert.ok(last.includes("推荐"), "退回消息点名缺失的交付物");
  assert.ok(last.includes("实际写入"), "退回消息要求落盘而非口头描述");
});

test("验收请求限定核对范围：只核对冻结产物、禁止写与检索、给出步数上限", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: true, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");

  const acceptor = [...sessions.values()].find((s) => s.key.startsWith("acceptor:"))!;
  const req = acceptor.deliveries[0]!;
  assert.ok(req.includes("核对范围"), "含核对范围章节");
  assert.ok(req.includes("E:/e2e/比较表"), "列出可读的产物文件");
  assert.ok(req.includes("不写、不删、不移动"), "限制写操作（避免沙箱报错消耗步数）");
  assert.ok(req.includes("不做耗时命令"), "禁止耗时命令");
  assert.ok(req.includes("沙箱/权限报错"), "明确沙箱报错不是缺项证据");
  assert.ok(req.includes("一次性给出全部结论"), "要求一次收敛");
  assert.ok(req.includes("最多 8 步（已用 0 步）"), "给出步数上限");
  // P2-9：没有声明只读能力集时，不得声称"已技术拦截"
  assert.ok(req.includes("没有工具级只读限制"), "如实降级表述");
  assert.ok(!req.includes("写入/删除/移动在工具层会被拒绝"), "不能空口承诺只读");
  // P2-8：不得用一句话把"事实是否正确"从判定范围里删掉
  assert.ok(!req.includes("不是「外部事实是否正确」"), "不再改写任务语义");
  assert.ok(req.includes("条文要求事实、数据、引用正确时"), "按标准条文决定是否核对事实");
  assert.ok(req.includes("不得写成 pass"), "无法核对时必须 gap，不能放行");
});

test("验收检查点：会话重建后接续已核对内容，不再从头核对", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: true, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  const round = svc.latestPendingRound(task.id)!;

  // 验收跑了 5 步只核完 c1（沙箱报错、翻资料等），随后会话失效
  await svc.recordAcceptorProgress(task.id, round.roundId, {
    steps: 5,
    checkedCriteria: ["c1"],
    lastSummary: "已核对比较表：12 行，覆盖 A/B 两方案",
  });
  await svc.recordAcceptorProgress(task.id, round.roundId, { steps: 5, lastSummary: "已核对比较表：12 行，覆盖 A/B 两方案" });

  const dead = [...sessions.values()].find((s) => s.key.startsWith("acceptor:"))!;
  dead.alive = false;
  await svc.pause(task.id, "budget", "余额不足");
  await svc.grantBudget(task.id, 100_000, "用户批准追加", "approval-1");
  await svc.resume(task.id);

  const resumed = [...sessions.values()].filter((s) => s.key.startsWith("acceptor:")).at(-1)!;
  assert.notEqual(resumed.key, dead.key, "失效会话被替换");
  const req = resumed.deliveries[0]!;
  assert.ok(req.includes(`轮次 ${round.roundId}`), "同一轮次续接，不是新轮次");
  assert.ok(req.includes("## 检查点"), "注入检查点");
  assert.ok(req.includes("已给出结论的标准：c1"), "已核对项不再重核");
  assert.ok(req.includes("已核对比较表：12 行"), "上一轮结论摘要保留");
  assert.ok(req.includes("已用 5 步"), "步数续计，不重新发一整套步数");
  assert.ok(req.includes("只补齐尚缺结论的标准"), "明确接续方式");
});

test("验收校验（P1-8）：漏项/重复标准/空证据/伪造验收人/未知轮次均被拒", async () => {
  let claimed = false;
  const { svc } = await makeService(async () => ({ status: "active", reportedNextSteps: [], claimedDone: claimed, activeCommand: false, pendingReply: false, unhandledEvent: false }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  claimed = true;
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  const round = svc.latestPendingRound(task.id)!;
  const reviewer = "research/reviewer.md";

  // 两条重复的 c1/pass、完全漏掉 c2 → 拒绝
  await assert.rejects(
    svc.recordVerdicts(
      task.id,
      [
        { criterionId: "c1", outcome: "pass", evidence: "e1", gap: null, criteriaVersion: 1, artifactHashes: {}, verdictBy: reviewer, createdAt: new Date().toISOString() },
        { criterionId: "c1", outcome: "pass", evidence: "e2", gap: null, criteriaVersion: 1, artifactHashes: {}, verdictBy: reviewer, createdAt: new Date().toISOString() },
      ],
      round.artifactHashes,
      round.roundId,
    ),
    /缺少验收结论|重复结论/,
  );

  // 空证据 → 拒绝
  await assert.rejects(
    svc.recordVerdicts(task.id, validVerdicts(reviewer, CRITERIA, 1, { c1: { evidence: "  " } }), round.artifactHashes, round.roundId),
    /证据为空/,
  );

  // 伪造验收人（执行者自验收）→ 拒绝
  await assert.rejects(
    svc.recordVerdicts(task.id, validVerdicts("research/a.md", CRITERIA, 1), round.artifactHashes, round.roundId),
    /不是已配置的独立验收人/,
  );

  // 未知轮次 → 拒绝
  await assert.rejects(svc.recordVerdicts(task.id, validVerdicts(reviewer, CRITERIA, 1), {}, "round_fabricated"), /未知验收轮次/);

  // 轮次仍为 pending，任务仍在待验收
  assert.equal(svc.latestPendingRound(task.id)!.roundId, round.roundId);
  assert.equal(svc.getTask(task.id)!.status, "awaiting_acceptance");
});

test("验收基于旧标准版本的轮次 → 结论失效被拒", async () => {
  let claimed = false;
  const { svc } = await makeService(async () => ({ status: "active", reportedNextSteps: [], claimedDone: claimed, activeCommand: false, pendingReply: false, unhandledEvent: false }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);

  claimed = true;
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-0");
  const staleRound = svc.latestPendingRound(task.id)!;

  await svc.proposeNewVersion(task.id, { criteria: [...CRITERIA, { id: "c3", text: "新标准" }] });
  await svc.confirm(task.id);

  claimed = true;
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");

  const reviewer = "research/reviewer.md";
  await assert.rejects(
    svc.recordVerdicts(task.id, validVerdicts(reviewer, CRITERIA, 1), staleRound.artifactHashes, staleRound.roundId),
    /旧标准版本/,
  );
});

test("零预算严格模式：确认后不分派，任务进入预算暂停（P1-7）", async () => {
  const { svc, sessions } = await makeService();
  const task = await svc.propose(proposedInput({ limitTokens: 0, strict: true }));
  await svc.confirm(task.id);
  const t = svc.getTask(task.id)!;
  assert.equal(t.status, "paused");
  assert.equal(t.pauseReason, "budget");
  assert.equal([...sessions.values()].length, 0, "零预算不创建会话、不投递");
});

test("预算不足时暂停；追加额度恢复后可继续（P1-7）", async () => {
  const turnCtx: TurnContext = { status: "active", reportedNextSteps: ["补齐推荐"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false };
  const { svc, ledger, sessions } = await makeService(async () => turnCtx);
  const task = await svc.propose(proposedInput({ limitTokens: 10_000, strict: true }));
  await svc.confirm(task.id);
  // 每次投递预留 12000 > 10000：初始分派即被拒，任务暂停
  assert.equal(svc.getTask(task.id)!.status, "paused");
  assert.equal(svc.getTask(task.id)!.pauseReason, "budget");

  await ledger.grant(task.id, 1_000_000, "追加");
  await svc.resume(task.id);
  assert.equal(svc.getTask(task.id)!.status, "active");
  assert.ok([...sessions.values()].every((s) => s.deliveries.length >= 1), "追加后完成初始分派");

  const decision = await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  assert.ok(decision && decision.action === "dispatch_next");
});

test("显式取消停止活动会话（P1-9）；取消后调度器不再处理", async () => {
  const { svc, sessions } = await makeService();
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const live = [...sessions.values()];
  assert.ok(live.length >= 2);

  await svc.cancel(task.id);
  assert.ok(live.every((s) => s.stopped), "所有任务会话被 stop（cancel 运行中活动并清空队列）");
  assert.equal(svc.getTask(task.id)!.status, "cancelled");

  const d = await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-9");
  assert.equal(d, null);
});

test("追加预算幂等保留用量，验收暂停后恢复原轮次并替换失效会话", async () => {
  const turn: TurnContext = { status: "active", reportedNextSteps: [], claimedDone: true, activeCommand: false, pendingReply: false, unhandledEvent: false };
  const { svc, ledger, sessions, store } = await makeService(async () => turn);
  try {
    const task = await svc.propose(proposedInput({ limitTokens: 600_000, strict: false }));
    await svc.confirm(task.id);
    await svc.registerArtifact(task.id, "产物.md", "sha256-original", "research/a.md");
    await svc.registerArtifact(task.id, "E:/e2e/比较表", "sha256-compare", "research/a.md");
    await svc.registerArtifact(task.id, "E:/e2e/推荐", "sha256-recommend", "research/a.md");
    await svc.onTurnEnd(task.id, "research/a.md", "completed", "done-1");
    const round = svc.latestPendingRound(task.id)!;
    assert.equal(svc.getTask(task.id)!.status, "awaiting_acceptance");
    const oldAcceptor = [...sessions.values()].find((s) => s.key.startsWith("acceptor:"))!;
    const executorDeliveries = [...sessions.values()].filter((s) => s.key.startsWith("executor:")).map((s) => s.deliveries.length);
    const reservation = await ledger.reserve(task.id, 1000, 0);
    assert.ok(reservation.ok);
    await ledger.settle(reservation.reservationId, { uncachedInputTokens: 592_905, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);
    await svc.pause(task.id, "budget", "余额不足");
    await assert.rejects(svc.resume(task.id), /预算不足/);
    assert.equal(svc.getTask(task.id)!.status, "paused");
    const reserved = ledger.getBudget(task.id)!.reservedTokens;
    await svc.grantBudget(task.id, 200_000, "用户批准追加", "approval-1");
    await svc.grantBudget(task.id, 200_000, "用户批准追加", "approval-1");
    await assert.rejects(svc.grantBudget(task.id, 1, "重用授权", "approval-1"), /不一致/);
    await assert.rejects(svc.grantBudget(task.id, -1, "无效", "invalid"), /正整数/);
    assert.equal(ledger.getBudget(task.id)!.limitTokens, 800_000);
    assert.equal(ledger.getBudget(task.id)!.settledTokens, 592_905);
    assert.equal(ledger.getBudget(task.id)!.reservedTokens, reserved);
    assert.equal(ledger.getBudget(task.id)!.grants.length, 1);
    assert.equal(svc.currentVersion(svc.getTask(task.id)!).budget.limitTokens, 800_000);
    oldAcceptor.alive = false;
    await svc.resume(task.id);
    assert.equal(svc.getTask(task.id)!.status, "awaiting_acceptance");
    assert.equal(svc.latestPendingRound(task.id)!.roundId, round.roundId);
    assert.deepEqual(svc.latestPendingRound(task.id)!.artifactHashes, round.artifactHashes);
    assert.deepEqual([...sessions.values()].filter((s) => s.key.startsWith("executor:")).map((s) => s.deliveries.length), executorDeliveries);
    const resumedAcceptor = [...sessions.values()].filter((s) => s.key.startsWith("acceptor:")).at(-1)!;
    assert.notEqual(resumedAcceptor.key, oldAcceptor.key);
    assert.ok(resumedAcceptor.deliveries[0]!.includes(round.roundId));
    assert.ok(resumedAcceptor.deliveries[0]!.includes("sha256-original"));
    const deliveryCount = resumedAcceptor.deliveries.length;
    await svc.resume(task.id);
    assert.equal(resumedAcceptor.deliveries.length, deliveryCount, "重复恢复不再次投递");
  } finally { await store.close(); }
});

test("用户暂停同样停止活动会话；恢复后状态正确", async () => {
  const { svc, sessions } = await makeService();
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const live = [...sessions.values()];

  await svc.pause(task.id, "user", "用户主动暂停");
  assert.ok(live.every((s) => s.stopped));
  assert.equal(svc.getTask(task.id)!.status, "paused");

  await svc.resume(task.id);
  assert.equal(svc.getTask(task.id)!.status, "active");
});

test("重开任务确认时继承原任务用量（设计文档 §2-5）", async () => {
  const { svc, ledger } = await makeService();
  const original = await svc.propose(proposedInput());
  await svc.confirm(original.id);
  const r = await ledger.reserve(original.id, 1000, 0);
  assert.ok(r.ok);
  await ledger.settle(r.reservationId, { uncachedInputTokens: 1200, outputTokens: 300, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);

  const reopened = await svc.propose(proposedInput(), original.id, "reopen");
  await svc.confirm(reopened.id, ledger.totalSettled(original.id));
  assert.equal(ledger.getBudget(reopened.id)!.settledTokens, 1500);
});

// ---- 预算硬准入（本轮联调暴露：旧实现每步只预留 6,000，实际请求 20,000～32,000）----

test("派工闸门与请求前准入同口径：额度够不了一步就当场暂停，不建空会话", async () => {
  const { svc, ledger, sessions } = await makeService();
  // 默认准入策略：基线输入 12,000 × 1.25 + 输出上限 8,000 = 23,000
  const need = requestAdmissionCost(DEFAULT_ADMISSION);
  assert.equal(need, 23_000);

  const task = await svc.propose(proposedInput({ limitTokens: need - 1, strict: true }));
  await svc.confirm(task.id);
  const t = svc.getTask(task.id)!;
  assert.equal(t.status, "paused");
  assert.equal(t.pauseReason, "budget");
  assert.equal([...sessions.values()].length, 0, "额度不足以支撑一次请求时不得创建会话");

  await ledger.grant(task.id, need, "追加");
  await svc.resume(task.id);
  assert.equal(svc.getTask(task.id)!.status, "active");
  assert.equal([...sessions.values()].filter((s) => s.deliveries.length === 1).length, 2);
});

test("恢复校验按一步成本判定：只够 1 Token 也不解锁", async () => {
  const { svc, ledger } = await makeService();
  const task = await svc.propose(proposedInput({ limitTokens: 200_000, strict: true }));
  await svc.confirm(task.id);
  const r = await ledger.reserve(task.id, 1000, 0);
  assert.ok(r.ok);
  await ledger.settle(r.reservationId, { uncachedInputTokens: 199_990, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);
  await svc.pause(task.id, "budget", "余额不足");
  // 剩余 10 Token：旧实现（> 0 即解锁）会放行，新实现必须拒绝
  await assert.rejects(svc.resume(task.id), /一份请求至少需要 23000 Token/);
  await svc.grantBudget(task.id, 100_000, "用户批准追加", "approval-1");
  await svc.resume(task.id);
  assert.equal(svc.getTask(task.id)!.status, "active");
});

test("预算耗尽暂停 → 追加额度恢复：必须唤醒执行者；会话已失效时新会话带完整简报", async () => {
  const turnCtx: TurnContext = { status: "active", reportedNextSteps: [], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false };
  const { svc, ledger, sessions } = await makeService(async () => turnCtx);
  const task = await svc.propose(proposedInput({ limitTokens: 200_000, strict: true }));
  await svc.confirm(task.id);

  // 掏空预算（模拟跑完）
  const r = await ledger.reserve(task.id, 1000, 0);
  assert.ok(r.ok);
  await ledger.settle(r.reservationId, { uncachedInputTokens: 199_990, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);
  await svc.pause(task.id, "budget", "预算跑完");
  assert.equal(svc.getTask(task.id)!.status, "paused");

  // 会话随暂停/重启失效
  const oldExecutor = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!;
  oldExecutor.alive = false;
  const oldDeliveries = oldExecutor.deliveries.length;

  await svc.grantBudget(task.id, 300_000, "用户批准追加", "approval-1");
  await svc.resume(task.id);
  assert.equal(svc.getTask(task.id)!.status, "active");

  const freshExecutor = [...sessions.values()].filter((s) => s.key.startsWith("executor:")).at(-1)!;
  assert.notEqual(freshExecutor.key, oldExecutor.key, "失效会话被替换");
  assert.equal(oldExecutor.deliveries.length, oldDeliveries, "旧会话不再被投递");
  assert.equal(freshExecutor.deliveries.length, 1, "恢复必须真的唤醒执行者（旧实现只重放事件，等于没人被叫醒）");
  const wake = freshExecutor.deliveries[0]!;
  assert.ok(wake.includes("# 任务简报"), "新会话带完整简报（否则它没有目标/标准/交付物）");
  assert.ok(wake.includes("比较两个方案并交付推荐"), "含目标");
  assert.ok(wake.includes("[c1] 交付比较表"), "含完成标准");
  assert.ok(wake.includes("任务恢复"), "含本次恢复指令；恢复不能无凭据宣称追加额度");
  assert.ok(wake.includes("<longtask_report>"), "含报告协议");
});

test("恢复时不做无谓扣费：额度不足一步时 resume 被拒且不投递", async () => {
  const { svc, ledger, sessions } = await makeService();
  const task = await svc.propose(proposedInput({ limitTokens: 100_000, strict: true }));
  await svc.confirm(task.id);
  const r = await ledger.reserve(task.id, 1000, 0);
  assert.ok(r.ok);
  await ledger.settle(r.reservationId, { uncachedInputTokens: 99_990, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);
  await svc.pause(task.id, "budget", "预算跑完");
  const before = [...sessions.values()].map((s) => s.deliveries.length);

  await assert.rejects(svc.resume(task.id), /可用预算不足/);
  assert.equal(svc.getTask(task.id)!.status, "paused", "拒绝恢复后仍保持暂停");
  assert.equal(ledger.getBudget(task.id)!.settledTokens, 99_990, "被拒的恢复不产生任何消耗");
  assert.deepEqual([...sessions.values()].map((s) => s.deliveries.length), before, "被拒的恢复不投递");
});

test("执行者简报带收敛纪律：先写后改、限制读文件次数、给出步数上限", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const brief = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!.deliveries[0]!;
  assert.ok(brief.includes("收敛纪律"), "有收敛纪律章节");
  assert.ok(brief.includes("先落盘第一版交付物"), "先写后改，避免通读资料");
  assert.ok(brief.includes("不超过 6 次"), "读预算量化到次数，而不是泛泛的'少读'");
  assert.ok(brief.includes("永久留在上下文里"), "说明读取的持续成本");
  assert.ok(brief.includes("不需要为核对内部实现去读源码"), "把'核对源码'这条歧义路径关掉");
  assert.ok(brief.includes("最多执行 24 步"), "给出执行步数上限");
  assert.ok(brief.includes("不要整篇重写"), "恢复时不重写已有产物");
});

test("decideAfterTurnEnd 决策表覆盖设计文档 §5", () => {
  const base: TurnContext = { status: "active", reportedNextSteps: [], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false };
  const run = (patch: Partial<TurnContext>, kind: TurnEndKind = "completed") => decideAfterTurnEnd({ ...base, ...patch }, kind);

  assert.equal(run({ reportedNextSteps: ["x"] }).action, "dispatch_next");
  assert.equal(run({ claimedDone: true }).action, "submit_acceptance");
  assert.equal(run({ activeCommand: true }).action, "wait");
  assert.equal(run({ pendingReply: true }).action, "wait");
  assert.equal(run({ unhandledEvent: true }).action, "wait");
  assert.equal(run({}).action, "stop");
  assert.equal(run({}, "max-tokens").action, "stop");
  assert.equal(run({ claimedDone: true }, "aborted").action, "stop");
  assert.equal(run({ claimedDone: true }, "error").action, "stop");
});

// ---------------------------------------------------------------------------
// 阶段 1 收敛：审查 07 的阻塞项（P1-2 / P1-3 / P1-4 / P1-5 / P1-6 / P1-7）
// ---------------------------------------------------------------------------

const DONE_TURN: TurnContext = { status: "active", reportedNextSteps: [], claimedDone: true, activeCommand: false, pendingReply: false, unhandledEvent: false };

test("P1-2 迟到验收不能复活已取消任务：结论被拒、轮次作废、状态保持 cancelled", async () => {
  const { svc } = await makeService(async () => DONE_TURN);
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  const round = svc.latestPendingRound(task.id)!;
  assert.equal(svc.getTask(task.id)!.status, "awaiting_acceptance");

  // 用户叫停，验收人此时才给出结论
  await svc.cancel(task.id);
  assert.equal(svc.getTask(task.id)!.status, "cancelled");

  await assert.rejects(
    svc.recordVerdicts(task.id, validVerdicts("research/reviewer.md", CRITERIA, 1), round.artifactHashes, round.roundId),
    /不接受验收结论/,
  );
  assert.equal(svc.getTask(task.id)!.status, "cancelled", "不得被迟到结论复活为 rework/delivered");
  assert.equal(svc.latestPendingRound(task.id), undefined, "取消时待结算轮次已作废");
});

test("P1-3 专家加载期间被取消：投递被拦下，迟到的新会话被释放", async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  // 简报渲染需要 await 专家定义加载：模拟一次很慢的加载
  const { svc, sessions } = await makeService(undefined, async () => {
    await gate;
    return "# 专家定义";
  });
  const task = await svc.propose(proposedInput());

  const confirming = svc.confirm(task.id); // 不 await：卡在简报渲染里
  await new Promise((r) => setTimeout(r, 20));
  await svc.cancel(task.id);
  release();
  await confirming;

  const executors = [...sessions.values()].filter((s) => s.key.startsWith("executor:"));
  assert.equal(executors.flatMap((s) => s.deliveries).length, 0, "取消后不得再有任何执行投递");
  assert.ok(executors.every((s) => s.key !== undefined && s.deliveries.length === 0));
  assert.equal(svc.getTask(task.id)!.status, "cancelled");
});

test("P1-4 暂停后的验收结束事件不再催办；同一轮次只催办一次", async () => {
  const { svc, sessions } = await makeService(async () => DONE_TURN);
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  const round = svc.latestPendingRound(task.id)!;
  const acceptor = [...sessions.values()].find((s) => s.key.startsWith("acceptor:"))!;
  const afterRequest = acceptor.deliveries.length;

  // 验收人本轮没给结论 → 催办一次
  assert.equal(await svc.nudgeAcceptor(task.id, round.roundId), true);
  assert.equal(acceptor.deliveries.length, afterRequest + 1);
  assert.ok(acceptor.deliveries.at(-1)!.includes("上一轮没有给出结论"));
  // 重复催办不再投递（幂等键只绑轮次，不再绑 turnId）
  assert.equal(await svc.nudgeAcceptor(task.id, round.roundId), false);
  assert.equal(acceptor.deliveries.length, afterRequest + 1);

  // 暂停后：结束事件不得再催办，也不得改状态
  await svc.pause(task.id, "user", "先停一下");
  const before = acceptor.deliveries.length;
  const decision = await svc.onTurnEnd(task.id, "research/reviewer.md", "completed", "turn-2");
  assert.equal(decision, null, "暂停任务结束事件不产生调度决策");
  assert.equal(acceptor.deliveries.length, before, "暂停后不再催办验收人");
  assert.equal(svc.getTask(task.id)!.status, "paused");
});

test("P1-5 验收必须回填冻结哈希：省略或多报都被拒；磁盘内容变了也算失败", async () => {
  let claimed = false;
  const { svc } = await makeService(
    async () => ({ status: "active", reportedNextSteps: [], claimedDone: claimed, activeCommand: false, pendingReply: false, unhandledEvent: false }),
    undefined,
    // 磁盘回算：E:/e2e/推荐 已被改写，与冻结哈希不一致
    { verifyArtifact: async (location, expected) => !location.endsWith("推荐") && expected.length > 0 },
  );
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  claimed = true;
  await registerDeliverables(svc, task.id);
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  const round = svc.latestPendingRound(task.id)!;
  const reviewer = "research/reviewer.md";

  // 省略冻结哈希（原实现的漏洞：{} 也能通过）
  await assert.rejects(
    svc.recordVerdicts(task.id, validVerdicts(reviewer, CRITERIA, 1), {}, round.roundId),
    /哈希集合与冻结版本不一致/,
  );
  // 多报一个不存在的产物
  await assert.rejects(
    svc.recordVerdicts(task.id, validVerdicts(reviewer, CRITERIA, 1), { ...round.artifactHashes, art_fake: "x" }, round.roundId),
    /哈希集合与冻结版本不一致/,
  );
  // 哈希齐了但磁盘内容已变
  await assert.rejects(
    svc.recordVerdicts(task.id, validVerdicts(reviewer, CRITERIA, 1), round.artifactHashes, round.roundId),
    /已不等于验收时冻结的版本/,
  );
  assert.equal(svc.getTask(task.id)!.status, "awaiting_acceptance", "失败结论不改变状态");
});

test("P1-6 同名不同目录的产物不算交付：按完整路径比对，且必须真实存在", async () => {
  const { svc, sessions } = await makeService(async () => DONE_TURN, undefined, {
    // 只有 under/ 下的那份存在
    fileExists: (abs) => abs.includes("/under/"),
  });
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  // 错误目录：basename 相同，完整路径不同
  await svc.registerArtifact(task.id, "E:/e2e/under/比较表", "sha256-compare", "research/a.md");
  await svc.registerArtifact(task.id, "E:/e2e/推荐", "sha256-recommend", "research/a.md");

  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  assert.equal(svc.getTask(task.id)!.status, "rework", "同名文件不能顶替指定交付物");
  assert.equal(svc.latestPendingRound(task.id), undefined, "不得进入待验收");
  assert.equal([...sessions.values()].filter((s) => s.key.startsWith("acceptor:")).length, 0);
  const executor = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!;
  assert.ok(executor.deliveries.at(-1)!.includes("比较表"), "退回消息点名真正缺失的交付物");
  assert.ok(executor.deliveries.at(-1)!.includes("E:/e2e/比较表"), "点名完整目标路径，避免再写错目录");
});

test("P1-7 确认新版本后账本改用新预算；降额到已消耗以下被拒", async () => {
  const { svc, ledger } = await makeService();
  const task = await svc.propose(proposedInput({ limitTokens: 500_000, strict: true }));
  await svc.confirm(task.id);
  assert.equal(ledger.getBudget(task.id)!.limitTokens, 500_000);

  // 确认新方案：预算抬到 900,000 并放宽 strict
  await svc.proposeNewVersion(task.id, { budget: { limitTokens: 900_000, strict: false } });
  await svc.confirm(task.id);
  const budget = ledger.getBudget(task.id)!;
  assert.equal(budget.limitTokens, 900_000, "账本上限跟随新版本");
  assert.equal(budget.strict, false, "strict 同样跟随新版本");
  assert.equal(svc.getTask(task.id)!.versions.at(-1)!.budget.limitTokens, 900_000, "版本预算与账本一致");
  // 上限 = 初始 + 全部 grants，保持可对账
  assert.equal(budget.grants.reduce((s, g) => s + g.tokens, 0), 400_000);

  // 已经消耗 300,000 后再降到 100,000：不得覆盖既有消耗
  const r = await ledger.reserve(task.id, 100, 0);
  assert.ok(r.ok, "降额前先制造一笔合法消耗");
  await ledger.settle(r.reservationId, { uncachedInputTokens: 300_000, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);
  await svc.proposeNewVersion(task.id, { budget: { limitTokens: 100_000, strict: true } });
  await assert.rejects(svc.confirm(task.id), /低于已消耗与在途合计/);
  assert.equal(ledger.getBudget(task.id)!.limitTokens, 900_000, "被拒的确认不改动账本");
});

test("本轮未给报告块：先催办一次，再缺则暂停；stop 必须留痕，不得静默停摆", async () => {
  const { svc, sessions, store } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const executor = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!;
  const before = executor.deliveries.length;

  const d1 = await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-1");
  assert.ok(d1 && d1.action === "stop" && d1.reason === "no_work");
  assert.equal(svc.getTask(task.id)!.status, "active", "先给一次补报告的机会，不立刻暂停");
  assert.equal(executor.deliveries.length, before + 1, "催办要真的投递给执行者");
  const nudge = executor.deliveries.at(-1)!;
  assert.ok(nudge.includes("<longtask_report>"), "催办只要求补报告块");
  assert.ok(nudge.includes("不要重做"), "明确不重做已完成的工作");

  // 留痕：stop 与催办都要有事件，否则任务停在 active 只能翻会话原文才发现（本轮联调踩过）
  const events = (store.snapshot().events ?? {}) as Record<string, { taskId: string; type: string }>;
  const types = Object.values(events).filter((e) => e.taskId === task.id).map((e) => e.type);
  assert.ok(types.includes("task.turn_stopped"), "stop 必须落 task.turn_stopped");
  assert.ok(types.includes("task.report_requested"), "催办必须落 task.report_requested");

  // 催办被忽略：下一轮（新 turnId）仍无报告 → 暂停交人工，不能无限催办
  const d2 = await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-2");
  assert.ok(d2 && d2.action === "stop");
  assert.equal(svc.getTask(task.id)!.status, "paused");
  assert.equal(svc.getTask(task.id)!.pauseReason, "missing_input");
  assert.equal(executor.deliveries.length, before + 1, "每个任务只催办一次");
});

test("收尾指令幂等：同一任务版本只下发一次，暂停后不再下发", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const executor = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!;
  const before = executor.deliveries.length;

  assert.equal(await svc.requestWrapUp(task.id, 0.7), true);
  for (const s of sessions.values()) if (s.key.startsWith('executor:')) assert.equal(s.deliveries.filter(m=>m.includes('【预算收尾】')).length,1,'所有在职专家都收到收尾');
  assert.equal(executor.deliveries.length, before + 1);
  const wrap = executor.deliveries.at(-1)!;
  assert.ok(wrap.includes("预算已使用约 70%"), "写明已用比例");
  assert.ok(wrap.includes("停止读取新文件"), "要求停止探索");
  assert.ok(wrap.includes("<longtask_report>"), "要求给出报告块");
  assert.equal(executor.targets.at(-1), "next-step", "收尾是当前轮控制消息");

  // 同一版本重复触发：不再投递
  assert.equal(await svc.requestWrapUp(task.id, 0.8), false);
  for (const s of sessions.values()) if (s.key.startsWith('executor:')) assert.equal(s.deliveries.filter(m=>m.includes('【预算收尾】')).length,1);
  assert.equal(executor.deliveries.length, before + 1, "每个任务版本只发一次");

  // 暂停后不得下发
  await svc.pause(task.id, "budget", "余额不足");
  assert.equal(await svc.requestWrapUp(task.id, 0.9), false);
});

test('验收成本预览复用真实请求正文，未知产物和文件增长会更新规划',async()=>{
  let size:number|null=null;const {svc,sessions,store}=await makeService(undefined,undefined,{fileSize:()=>size});
  try{
    const t=await svc.propose({...proposedInput(),deliverables:['result'],executors:[expert('research/a.md')]});await svc.confirm(t.id);
    assert.equal(svc.estimateAcceptanceInput(t.id).unknownArtifacts,1);
    size=128;await svc.registerArtifact(t.id,'E:/e2e/result','a'.repeat(64),'research/a.md');
    const small=svc.estimateAcceptanceInput(t.id);assert.equal(small.unknownArtifacts,0);assert.equal(small.artifactBytes,128);
    size=8192;assert.equal(svc.estimateAcceptanceInput(t.id).artifactBytes,8192);
    await svc.runAcceptance(t.id,'preview-round','research/a.md');
    const content=[...sessions.values()].find(s=>s.key.startsWith('acceptor:'))!.deliveries.at(-1)!;
    const preview=svc.estimateAcceptanceInput(t.id);
    assert.ok(preview.requestBytes>=Buffer.byteLength(JSON.stringify({role:'user',content:[{type:'text',text:content}]}),'utf8'));
    assert.ok(content.includes('E:/e2e/result'));assert.ok(content.includes('a'.repeat(64)));
  }finally{await store.close();}
});

test("读预算指令：超过读上限后打断一次，转入只写；每任务版本只发一次", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const executor = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!;
  const before = executor.deliveries.length;

  assert.equal(await svc.requestReadStop(task.id, 7), true);
  assert.equal(executor.deliveries.length, before + 1);
  const stop = executor.deliveries.at(-1)!;
  assert.ok(stop.includes("读预算用尽"), "点明读预算");
  assert.ok(stop.includes("不要再 read/grep/glob"), "给出禁止清单");
  assert.ok(stop.includes("永久留在上下文里"), "说明为什么会变贵");
  assert.ok(stop.includes("<longtask_report>"), "要求给出报告块");
  assert.equal(executor.targets.at(-1), "next-step", "读预算提醒进入当前轮下一步");

  assert.equal(await svc.requestReadStop(task.id, 12), false);
  assert.equal(executor.deliveries.length, before + 1, "同一版本只打断一次");
});

test("读预算并发去重：同一批 tool/call 同时触发时只投递一条（实测 6ms 内两次）", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const executor = [...sessions.values()].find((s) => s.key.startsWith("executor:"))!;
  const before = executor.deliveries.length;

  const results = await Promise.all([
    svc.requestReadStop(task.id, 7),
    svc.requestReadStop(task.id, 8),
  ]);
  assert.equal(results.filter(Boolean).length, 1, "并发触发只允许一条生效");
  assert.equal(executor.deliveries.length, before + 1, "不得重复投递同一条收敛指令");
  assert.equal(executor.deliveries.filter((d) => d.includes("读预算用尽")).length, 1);
});

// ---------------------------------------------------------------------------
// 阶段 2：专家协作（定向消息 / 交接摘要 / 变更人选 / 返工归属 / Router 摘要）
// ---------------------------------------------------------------------------

test("阶段2 定向消息：动作身份去重，不同动作即使同文/同文不同产物也各自投递", async () => {
  const { svc, sessions, store } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const receiver = [...sessions.values()].find((s) => s.key.includes("research/b.md"))!;
  const before = receiver.deliveries.length;

  // 同一动作（actionId 相同）重放：只投一次
  const a1 = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "采用方案甲", { actionId: "turn-1:msg:0" });
  assert.equal(a1.ok, true);
  const replay = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "采用方案甲", { actionId: "turn-1:msg:0" });
  assert.equal(replay.ok, true, "重放按幂等成功返回");
  assert.equal(replay.duplicate, true);
  assert.equal(receiver.deliveries.length, before + 1, "同一次动作不得重复投递");

  // 不同动作、前缀相同、结论相反：必须各自送达（旧实现按前 80 字去重会吞掉）
  await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "采用方案甲，因为成本更低，且上线时间可控，风险可接受。", { actionId: "turn-1:msg:1" });
  await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "采用方案甲，因为成本更低，且上线时间可控，但风险不可接受。", { actionId: "turn-1:msg:2" });
  assert.equal(receiver.deliveries.length, before + 3, "结论相反的合法消息不能被当成重复");

  // 同正文、不同产物引用与回复对象：也是不同动作
  const m1 = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "请复核产物", { actionId: "turn-1:msg:3", artifactRefs: ["v1.md"], replyToId: "m1" });
  const m2 = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "请复核产物", { actionId: "turn-1:msg:4", artifactRefs: ["v2.md"], replyToId: "m2" });
  assert.equal(m1.ok, true);
  assert.equal(m2.ok, true);
  assert.notEqual(m1.messageId, m2.messageId);
  assert.equal(receiver.deliveries.length, before + 5);

  // 全文持久化：长正文尾部标记必须能在快照里回查（旧实现只存 240 字符）
  const tail = "TAIL-MARKER-8f3c";
  const longBody = `${"正文".repeat(400)}${tail}`;
  await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", longBody, { actionId: "turn-1:msg:5" });
  const snapshot = JSON.stringify(store.snapshot());
  assert.ok(snapshot.includes(tail), "完整正文必须落盘，可按 messageId 回查");
  const rec = Object.values(store.snapshot().collections.messages ?? {}).find(
    (m) => (m as { body?: string }).body?.includes(tail),
  ) as { excerpt: string; bodyHash: string; contentRef: string | null } | undefined;
  assert.ok(rec);
  assert.equal(rec!.excerpt.length, 240, "摘要仍截断");
  assert.ok(rec!.bodyHash, "正文指纹单独记录");

  // 非本任务专家 / 自说自话：拒绝
  await assert.rejects(svc.sendExpertMessage(task.id, "research/a.md", "research/reviewer.md", "x"), /不是本任务的执行专家/);
  await assert.rejects(svc.sendExpertMessage(task.id, "research/a.md", "research/a.md", "x"), /不能是同一位专家/);
});

test("阶段2 outbox：投递失败保留记录，同一动作可重试送达；预算拒绝保留待投递", async () => {
  const { svc, sessions, ledger, store, failNextDeliveries } = await makeService(
    async () => ({ status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false }),
    undefined,
    { host: { dedupe: false } },
  );
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const receiver = [...sessions.values()].find((s) => s.key.includes("research/b.md"))!;
  const before = receiver.deliveries.length;

  failNextDeliveries(1);
  const first = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "口径已改，请更新分母", { actionId: "turn-2:msg:0" });
  assert.equal(first.ok, false, "首次投递失败必须如实返回");
  assert.equal(first.delivery, "failed");
  assert.equal(receiver.deliveries.length, before, "失败时收件人没收到");

  // 同一动作重试：沿用原记录补投，而不是被判成 duplicate 永久跳过
  const retry = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "口径已改，请更新分母", { actionId: "turn-2:msg:0" });
  assert.equal(retry.ok, true, "失败记录必须可重试");
  assert.equal(retry.messageId, first.messageId, "重试不生成新的业务动作");
  assert.equal(receiver.deliveries.length, before + 1, "重试后送达");

  // 预算拒绝：保留 pending，不标记为已处理
  const tiny = await svc.propose({ ...proposedInput(), budget: { limitTokens: 40_000, strict: true } });
  await svc.confirm(tiny.id);
  // 先真实结算掉大部分额度（派工闸门只看可用额度，不预先占用）
  const rsv = await ledger.reserve(tiny.id, 15_000, 8_000);
  assert.equal(rsv.ok, true);
  await ledger.settle(
    (rsv as { reservationId: string }).reservationId,
    { uncachedInputTokens: 30_000, outputTokens: 5_000, cacheReadTokens: 0, cacheWriteTokens: 0 },
    "压掉小额任务的可用额度",
  );
  const tinyReceiver = [...sessions.values()].filter((s) => s.key.includes("research/b.md")).at(-1)!;
  const beforeTiny = tinyReceiver.deliveries.length;
  const blocked = await svc.sendExpertMessage(tiny.id, "research/a.md", "research/b.md", "预算不足时这条要待投", { actionId: "turn-3:msg:0" });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.reason, "budget");
  assert.equal(blocked.delivery, "pending", "预算拒绝保留待投递，而不是失败终态");
  assert.equal(tinyReceiver.deliveries.length, beforeTiny);
  const pending = Object.values(store.snapshot().collections.messages ?? {}).find(
    (m) => (m as { body?: string }).body === "预算不足时这条要待投",
  ) as { delivery: string } | undefined;
  assert.ok(pending);
  assert.equal(pending!.delivery, "pending", "记录保留，额度恢复后可用同一动作重投");
});

test("阶段2 交接摘要：并发幂等（同一动作只落一条），Router 只看摘要与引用", async () => {
  const { svc, store } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  await svc.registerArtifact(task.id, "E:/e2e/比较表", "sha256-compare", "research/a.md");

  const sent = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "统计口径已改为全部受访者，请按此更新分母。", {
    actionId: "turn-4:msg:0",
    artifactRefs: ["E:/e2e/比较表"],
  });
  // 并发写同一个摘要动作：只能落一条
  const ids = await Promise.all([
    svc.recordHandoffSummary(task.id, { from: "research/a.md", to: "research/b.md", summary: "比例口径已修正，见表 3", artifactRefs: ["E:/e2e/比较表"], causedByMessageId: sent.messageId, actionId: "turn-4:sum:0" }),
    svc.recordHandoffSummary(task.id, { from: "research/a.md", to: "research/b.md", summary: "比例口径已修正，见表 3", artifactRefs: ["E:/e2e/比较表"], causedByMessageId: sent.messageId, actionId: "turn-4:sum:0" }),
  ]);
  assert.equal(ids[0], ids[1], "同一动作并发只落一条，返回同一 ID");
  const summaries = Object.values(store.snapshot().collections.messages ?? {}).filter((m) => (m as { kind?: string }).kind === "summary");
  assert.equal(summaries.length, 1);

  // 不同动作、同样一句结论：是新的摘要（不同轮次可以重复说同一句话）
  const other = await svc.recordHandoffSummary(task.id, { from: "research/a.md", to: "research/b.md", summary: "比例口径已修正，见表 3", actionId: "turn-5:sum:0" });
  assert.ok(other && other !== ids[0]);

  const digest = svc.routerDigest(task.id) as {
    latestSummaries: Array<{ messageId: string; summary: string; artifactRefs: string[]; causedByMessageId: string | null }>;
    artifacts: Array<{ location: string; owner: string | null }>;
  };
  assert.equal(digest.latestSummaries.length, 2);
  assert.equal(digest.latestSummaries.at(-1)!.summary, "比例口径已修正，见表 3");
  assert.deepEqual(digest.latestSummaries.at(-1)!.artifactRefs, ["E:/e2e/比较表"]);
  assert.equal(digest.latestSummaries.at(-1)!.causedByMessageId, sent.messageId, "摘要能回到原讨论");
  assert.equal(digest.artifacts[0]!.owner, "research/a.md");

  // Router 视图不得携带直连正文
  const serialized = JSON.stringify(digest);
  assert.ok(!serialized.includes("请按此更新分母"), "Router 摘要不得包含讨论全文");
});

test("阶段2 变更人选：并发替换同一人只有一个成功；单执行者也能替换；旧执行者立即失去资格", async () => {
  const { svc, sessions, store } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  await svc.registerArtifact(task.id, "E:/e2e/比较表", "sha256-compare", "research/a.md");

  // 并发替换同一个旧执行者：必须有一个明确失败，不能静默覆盖
  const results = await Promise.allSettled([
    svc.changeExecutor(task.id, { add: expert("research/c.md"), remove: "research/b.md", reason: "C 接手" }),
    svc.changeExecutor(task.id, { add: expert("research/d.md"), remove: "research/b.md", reason: "D 接手" }),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1, "并发替换同一人只能成功一次");
  const list = svc.getTask(task.id)!.versions.at(-1)!.executors.map((e) => e.relPath);
  assert.equal(list.length, 2, "最终名单长度正确");
  assert.ok(list.includes("research/a.md"));

  // 旧执行者会话被停止、关联被撤销
  const removed = list.includes("research/c.md") ? "research/d.md" : "research/c.md";
  const removedSessions = [...sessions.values()].filter((s) => s.key.includes(removed));
  for (const s of removedSessions) assert.equal(s.stopped, true, "被换下的执行者会话必须停止");

  // 单执行者直接替换（旧实现会误报"不能移除最后一位"）
  const solo = await svc.propose({ ...proposedInput(), executors: [expert("research/solo.md")] });
  await svc.confirm(solo.id);
  await svc.changeExecutor(solo.id, { add: expert("research/new.md"), remove: "research/solo.md", reason: "换人" });
  assert.deepEqual(svc.getTask(solo.id)!.versions.at(-1)!.executors.map((e) => e.relPath), ["research/new.md"]);

  // 验收人变更仍必须走用户确认
  await assert.rejects(
    svc.changeExecutor(solo.id, { add: expert("research/reviewer.md"), reason: "换验收人" }),
    /验收专家变更需用户单独确认/,
  );
});

// ---------------------------------------------------------------------------
// 审查 25：产物版本 / unknown 对账 / 恢复消费 / 撤销代次
// ---------------------------------------------------------------------------

test("审查25 C3：接手者修改他人产物必须登记新版本，归属保持不变", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-ver-"));
  const file = join(dir, "shared.md");
  await fs.writeFile(file, "A 的第一版\n", "utf8");
  const { svc, store } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose({ ...proposedInput(), deliverables: [file] });
  await svc.confirm(task.id);

  await svc.registerTurnArtifacts(task.id, "research/a.md", [file]);
  const first = svc.routerDigest(task.id) as { artifacts: Array<{ location: string; hash: string; owner: string | null }> };
  assert.equal(first.artifacts[0]!.owner, "research/a.md");

  // B 合法接手并修改同一文件（不显式声明归属）
  await fs.writeFile(file, "A 的第一版 + B 补充的推荐\n", "utf8");
  // 隐式登记（B 只是"报完成"，没有显式声明归属）：内容更新、归属不变
  await svc.registerTurnArtifacts(task.id, "research/b.md");
  const digest = svc.routerDigest(task.id) as { artifacts: Array<{ location: string; hash: string; owner: string | null }> };
  assert.notEqual(digest.artifacts[0]!.hash, first.artifacts[0]!.hash, "内容变了必须产生新版本（否则验收冻结旧哈希）");
  assert.equal(digest.artifacts[0]!.owner, "research/a.md", "归属仍记原作者（A 仍在职）");
  const versions = Object.values(store.snapshot().collections.artifacts ?? {}).filter(
    (a) => (a as { location?: string }).location === file,
  );
  assert.equal(versions.length, 2, "历史版本保留，可追溯");

  // 原负责人离任后，接手者接管归属
  await svc.changeExecutor(task.id, { add: expert("research/c.md"), remove: "research/a.md", reason: "A 退出" });
  await fs.writeFile(file, "C 版本\n", "utf8");
  await svc.registerTurnArtifacts(task.id, "research/c.md", [file]);
  const after = svc.routerDigest(task.id) as { artifacts: Array<{ owner: string | null }> };
  assert.equal(after.artifacts[0]!.owner, "research/c.md", "原作者离任后由接手者接管归属");
});

test("审查26 D1：resume 补投与同动作重放串行，实际仅送达一次", async () => {
  const { svc, store, sessions, failNextDeliveries } = await makeService(undefined, undefined, { host: { dedupe: false } });
  let release = () => {};
  const pending: Promise<unknown>[] = [];
  try {
    const task = await svc.propose(proposedInput()); await svc.confirm(task.id);
    const receiver = [...sessions.values()].find((s) => s.key.includes("research/b.md"))!;
    failNextDeliveries(1);
    const first = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "唯一交接", { actionId: "resume-race" });
    assert.equal(first.delivery, "failed");
    await svc.pause(task.id, "user", "本地恢复测试");
    const host = (svc as unknown as { o: { host: HostBridge } }).o.host;
    const deliver = host.deliver;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let entered!: () => void;
    const firstEntered = new Promise<void>((resolve) => { entered = resolve; });
    let calls = 0;
    host.deliver = async (...args) => {
      if (args[2].includes("resume-race")) { calls++; entered(); await gate; }
      return deliver(...args);
    };
    const resume = svc.resume(task.id); pending.push(resume);
    await firstEntered;
    const replay = svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "唯一交接", { actionId: "resume-race" }); pending.push(replay);
    await new Promise((resolve) => setTimeout(resolve, 50));
    release(); await Promise.all(pending);
    assert.equal(calls, 1, "补投和重放不能各调一次宿主");
    assert.equal(receiver.deliveries.filter((text) => text.includes("唯一交接")).length, 1);
    const stored = svc.getExpertMessage(task.id, first.messageId!)!;
    assert.deepEqual(stored.deliveryAttempts!.map((a) => a.outcome), ["failed", "delivered"]);
  } finally { release(); await Promise.allSettled(pending); await store.close(); }
});

test("审查26：交接摘要沿用原消息版本，不随接手后的文件变化漂移", async () => {
  const { svc, store } = await makeService();
  try {
    const task = await svc.propose(proposedInput()); await svc.confirm(task.id);
    await svc.registerArtifact(task.id, "E:/e2e/比较表", "a".repeat(64), "research/a.md");
    const sent = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "请接手", { actionId: "version", artifactRefs: ["E:/e2e/比较表"] });
    await svc.registerArtifact(task.id, "E:/e2e/比较表", "b".repeat(64), "research/a.md");
    await svc.recordHandoffSummary(task.id, { from: "research/a.md", to: "research/b.md", summary: "初稿交接", causedByMessageId: sent.messageId, artifactRefs: ["E:/e2e/比较表"] });
    const summary = store.list<import("../src/types.js").MessageRecord>("messages").find((m) => m.kind === "summary")!;
    assert.equal(summary.artifactVersions!["E:/e2e/比较表"], "a".repeat(64));
  } finally { await store.close(); }
});

test("审查25 C1：unknown 不盲投，对账后才允许继续", async () => {
  const { svc, sessions, store, unknownNextDeliveries } = await makeService(
    async () => ({ status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false }),
    undefined,
    { host: { dedupe: false } },
  );
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  unknownNextDeliveries(1); // 首次派工之后再注入：让"专家消息"这一次返回 unknown
  const receiver = [...sessions.values()].find((s) => s.key.includes("research/b.md"))!;
  const before = receiver.deliveries.length;

  const first = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "口径已改", { actionId: "turn-9:msg:0" });
  assert.equal(first.delivery, "unknown");
  assert.equal(receiver.deliveries.length, before + 1, "宿主实际已收件，只是确认失败");

  // 重放同一动作：不能盲目再投一次
  const replay = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "口径已改", { actionId: "turn-9:msg:0" });
  assert.equal(replay.ok, false);
  assert.equal(replay.reason, "unknown-needs-reconcile");
  assert.equal(receiver.deliveries.length, before + 1, "unknown 状态不得重复投递");

  // 对账为已送达：之后按幂等成功处理
  await svc.reconcileExpertMessage(task.id, first.messageId!, "delivered", "已按会话历史核对该 actionId 收件一次");
  const after = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "口径已改", { actionId: "turn-9:msg:0" });
  assert.equal(after.ok, true);
  assert.equal(after.duplicate, true);
  assert.equal(receiver.deliveries.length, before + 1);
  const rec = Object.values(store.snapshot().collections.messages ?? {}).find((m) => (m as { id?: string }).id === first.messageId) as { delivery: string };
  assert.equal(rec.delivery, "delivered");
});

test("审查25 C2：预算恢复后 resume 必须消费 pending 的交接消息", async () => {
  const { svc, sessions, ledger } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose({ ...proposedInput(), budget: { limitTokens: 60_000, strict: true } });
  await svc.confirm(task.id);
  const receiver = [...sessions.values()].find((s) => s.key.includes("research/b.md"))!;

  // 压掉额度，让交接消息进入 pending
  const rsv = await ledger.reserve(task.id, 15_000, 8_000);
  await ledger.settle((rsv as { reservationId: string }).reservationId, { uncachedInputTokens: 40_000, outputTokens: 2_000, cacheReadTokens: 0, cacheWriteTokens: 0 }, "压额度");
  const blocked = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "这条交接必须补投", { actionId: "turn-10:msg:0" });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.delivery, "pending");
  const before = receiver.deliveries.length;

  // 追加额度并恢复：outbox 必须被消费
  await svc.grantBudget(task.id, 200_000, "补投交接", "grant-c2");
  await svc.resume(task.id);
  assert.ok(receiver.deliveries.length > before, "恢复后必须补投 pending 交接");
  assert.ok(receiver.deliveries.at(-1)!.includes("这条交接必须补投"));
});

// ---------------------------------------------------------------------------
// 阶段 3 · 任务1：统一等待与唤醒
// ---------------------------------------------------------------------------

test("阶段3 等待登记：依赖阻塞会登记等待，交接送达即唤醒并清空 waitingOn", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const bRef = expert("research/b.md");
  const task = await svc.propose({
    ...proposedInput(),
    executors: [expert("research/a.md"), bRef],
    deliverables: ["比较表", "推荐"],
    assignments: [
      { relPath: "research/a.md", deliverables: ["比较表"] },
      { relPath: "research/b.md", deliverables: ["推荐"], waitFor: { fromRelPath: "research/a.md", artifactRefs: ["比较表"] } },
    ],
  });
  await svc.confirm(task.id);

  // 依赖未就绪：B 被登记为等待，任务级 waitingOn 可见
  const waits = svc.listWaits(task.id);
  const depWait = waits.find((w) => w.wakeCondition === "dependency:research/b.md");
  assert.ok(depWait, "依赖阻塞必须登记等待");
  assert.equal(depWait!.status, "waiting");
  assert.equal(svc.getTask(task.id)!.waitingOn?.kind, "expert_message", "任务级 waitingOn 同步镜像");

  // 交接送达：等待被唤醒，waitingOn 清空
  await svc.registerArtifact(task.id, "E:/e2e/比较表", "a".repeat(64), "research/a.md");
  const sent = await svc.sendExpertMessage(task.id, "research/a.md", "research/b.md", "比较表已落盘", {
    actionId: "stage3:msg:0",
    artifactRefs: ["E:/e2e/比较表"],
  });
  assert.equal(sent.ok, true);
  const after = svc.listWaits(task.id).find((w) => w.id === depWait!.id)!;
  assert.equal(after.status, "woken", "交接送达必须唤醒依赖等待");
  assert.equal(after.resolvedBy, "expert_message");
  assert.equal(svc.getTask(task.id)!.waitingOn ?? null, null, "唤醒后不得继续显示等待中");
});

test("阶段3 过期清理：条件已满足的等待会被扫掉，不再永久悬挂", async () => {
  const { svc } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose({ ...proposedInput(), executors: [expert("research/a.md")] });
  await svc.confirm(task.id);

  // 人为登记一条"依赖已就绪"的等待（模拟历史遗留 / 崩溃恢复后的悬挂）
  const id = await svc.registerWait(task.id, {
    kind: "expert_message",
    description: "等待已经不存在的交接",
    wakeCondition: "dependency:research/a.md",
  });
  assert.equal(svc.listWaits(task.id).find((w) => w.id === id)!.status, "waiting");
  assert.ok(svc.getTask(task.id)!.waitingOn, "登记后 waitingOn 有值");

  const swept = await svc.sweepStaleWaits(task.id);
  assert.ok(swept >= 1, "条件已满足的等待必须被清理");
  const after = svc.listWaits(task.id).find((w) => w.id === id)!;
  assert.equal(after.status, "expired");
  assert.equal(after.resolvedBy, "sweep");
  assert.equal(svc.getTask(task.id)!.waitingOn ?? null, null);
});

test("阶段3 验收等待：结论结算即唤醒；恢复会唤醒人工等待", async () => {
  let claimed = false;
  const { svc } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: claimed, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  await registerDeliverables(svc, task.id);

  // 等验收结果的等待：结论到达后应被唤醒
  const waitId = await svc.registerWait(task.id, { kind: "acceptance", description: "等独立验收结论", wakeCondition: "acceptance" });
  claimed = true;
  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-s3-1");
  const round = svc.latestPendingRound(task.id)!;
  await svc.recordVerdicts(task.id, validVerdicts("research/reviewer.md", CRITERIA, 1), round.artifactHashes, round.roundId);
  assert.equal(svc.listWaits(task.id).find((w) => w.id === waitId)!.status, "woken", "验收结论必须唤醒等待");

  // 人工等待：resume 唤醒
  const paused = await svc.propose(proposedInput());
  await svc.confirm(paused.id);
  await svc.pause(paused.id, "missing_input", "需要用户补充口径");
  const userWait = await svc.registerWait(paused.id, { kind: "user_reply", description: "等用户补充口径", wakeCondition: "user_reply" });
  await svc.grantBudget(paused.id, 100_000, "补充后恢复", "grant-s3");
  await svc.resume(paused.id);
  assert.equal(svc.listWaits(paused.id).find((w) => w.id === userWait)!.status, "woken", "resume 必须唤醒人工等待");
});

// ---------------------------------------------------------------------------
// 阶段 3 · 任务3/4/5
// ---------------------------------------------------------------------------

test("阶段3 重启接续：会话已死也自动重建并投递恢复指令；未知投递先核对不推进", async () => {
  const { svc, sessions, ledger, store, unknownNextDeliveries } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);

  // 模拟宿主重启：所有会话已死
  for (const s of sessions.values()) s.alive = false;
  const outcome = await svc.resumeAfterRestart(task.id);
  assert.equal(outcome, "resumed", "可运行任务必须自动接续");
  const revived = [...sessions.values()].filter((s) => s.alive && s.key.startsWith("executor:"));
  assert.ok(revived.length >= 1, "会话必须被重建");
  assert.ok(revived.some((s) => s.deliveries.some((d) => d.includes("重启恢复"))), "必须投递恢复上下文，而不是空转");

  // 另一任务：存在未知投递 → 只核对，不推进
  const other = await svc.propose(proposedInput());
  await svc.confirm(other.id);
  unknownNextDeliveries(1);
  await svc.sendExpertMessage(other.id, "research/a.md", "research/b.md", "这条会 unknown", { actionId: "restart:unknown:0" });
  const outcome2 = await svc.resumeAfterRestart(other.id);
  assert.equal(outcome2, "needs-reconcile");
  assert.equal(svc.getTask(other.id)!.status, "paused");
  assert.equal(svc.getTask(other.id)!.pauseReason, "missing_input");
  const pauseEvent = Object.values(store.snapshot().events as Record<string, { taskId: string; type: string; payload?: { note?: string } }>)
    .find((e) => e.taskId === other.id && e.type === "task.paused");
  assert.ok(pauseEvent, "暂停必须留事件");
  assert.ok(String(pauseEvent?.payload?.note ?? "").includes("投递结果未知"), "暂停原因要说明需核对未知投递");
  void ledger;
});

test("阶段3 多任务调度：并发上限按优先级排队，名额让出后自动恢复", async () => {
  let claimed = false;
  const { svc } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: claimed, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const low = await svc.propose({ ...proposedInput(), goal: "低优先级任务" });
  await svc.setTaskPriority(low.id, 1);
  await svc.confirm(low.id);
  const high = await svc.propose({ ...proposedInput(), goal: "高优先级任务" });
  await svc.setTaskPriority(high.id, 9);
  await svc.confirm(high.id);

  const res = await svc.enforceConcurrency(1);
  assert.deepEqual(res.running, [high.id], "高优先级先运行");
  assert.deepEqual(res.queued, [low.id]);
  assert.equal(svc.getTask(low.id)!.status, "paused");
  assert.equal(svc.getTask(low.id)!.pauseReason, "queued");

  // 高优先级任务交付后让出名额：排队的低优先级任务自动恢复
  await registerDeliverables(svc, high.id);
  claimed = true;
  await svc.onTurnEnd(high.id, "research/a.md", "completed", "turn-sched-1");
  const round = svc.latestPendingRound(high.id)!;
  await svc.recordVerdicts(high.id, validVerdicts("research/reviewer.md", CRITERIA, 1), round.artifactHashes, round.roundId);
  assert.equal(svc.getTask(high.id)!.status, "delivered");

  const promoted = await svc.releaseQueued(1);
  assert.deepEqual(promoted, [low.id], "名额让出后按优先级恢复排队任务");
  assert.equal(svc.getTask(low.id)!.status, "active");
});

test("阶段3 用户反馈：补充进原任务、标准遗漏重开、新增要求建关联任务、歧义先澄清", async () => {
  const { svc, sessions, ledger } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const executor = [...sessions.values()].find((s) => s.key.includes("research/a.md"))!;
  const before = executor.deliveries.length;
  const budgetBefore = svc.getTask(task.id)!.versions.at(-1)!.budget.limitTokens;

  // 1) 补充信息：进原任务，预算不变
  const supplement = await svc.applyUserFeedback(task.id, { kind: "supplement", text: "分母用全部受访者" });
  assert.equal(supplement.action, "delivered");
  assert.ok(executor.deliveries.at(-1)!.includes("分母用全部受访者"));
  assert.equal(svc.getTask(task.id)!.versions.at(-1)!.budget.limitTokens, budgetBefore, "补充信息不追加预算");
  assert.ok(executor.deliveries.length > before);

  // 2) 标准遗漏：重开并继承已用额度
  const rsv = await ledger.reserve(task.id, 15_000, 8_000);
  await ledger.settle((rsv as { reservationId: string }).reservationId, { uncachedInputTokens: 20_000, outputTokens: 1_000, cacheReadTokens: 0, cacheWriteTokens: 0 }, "先花掉一部分");
  const settledBefore = (svc as unknown as { o: { ledger: { getBudget(id: string): { settledTokens: number } } } });
  const reopened = await svc.applyUserFeedback(task.id, {
    kind: "missing_criteria",
    text: "原标准漏了推荐依据",
    criteria: [{ id: "c3", text: "推荐必须给出依据" }],
  });
  assert.equal(reopened.action, "reopened");
  const v = svc.getTask(task.id)!.versions.at(-1)!;
  assert.equal(v.criteria.length, 3, "新增标准进入同一任务的新版本");
  assert.equal(svc.getTask(task.id)!.status, "active");
  assert.equal(v.budget.limitTokens, budgetBefore, "重开不追加预算");
  assert.ok(svc.getTask(task.id)!.versions.at(-1)!.criteria.some((c) => c.id === "c3"));
  void settledBefore;

  // 3) 新增要求：建关联任务、独立预算、等待确认
  const child = await svc.applyUserFeedback(task.id, {
    kind: "new_requirement",
    text: "再加一份风险清单",
    goal: "在比较表基础上补一份风险清单",
    deliverables: ["风险清单"],
    budget: { limitTokens: 120_000, strict: true },
  });
  assert.equal(child.action, "child-proposed");
  const childTask = svc.getTask(child.relatedTaskId!)!;
  assert.equal(childTask.relatedTaskId, task.id);
  assert.equal(childTask.relationKind, "child");
  assert.equal(childTask.status, "proposed", "新增要求必须单独确认预算");
  assert.equal(childTask.versions[0]!.budget.limitTokens, 120_000);

  // 4) 歧义：暂停并提一个具体问题
  const clarified = await svc.applyUserFeedback(task.id, { kind: "clarify", text: "“更稳妥”指成本还是风险？" });
  assert.equal(clarified.action, "clarified");
  assert.equal(svc.getTask(task.id)!.status, "paused");
  assert.equal(svc.getTask(task.id)!.pauseReason, "missing_input");
});

// ---------------------------------------------------------------------------
// 阶段3 审查 33：调度/等待/反馈的关键反例
// ---------------------------------------------------------------------------

test("审查33 C3/C4：并发上限在派工前认领，超出任务不派工直接排队", async () => {
  const { svc, sessions } = await makeService(
    async () => ({ status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false }),
    undefined,
    { maxConcurrentTasks: 1 },
  );
  const mk = async (goal: string) => {
    const t = await svc.propose({ ...proposedInput(), goal, deliverables: [`out-${goal}.md`] });
    await svc.confirm(t.id);
    return t;
  };
  const one = await mk("一");
  const two = await mk("二");
  const three = await mk("三");

  assert.equal(svc.getTask(one.id)!.status, "active");
  for (const t of [two, three]) {
    assert.equal(svc.getTask(t.id)!.status, "paused", "超出名额必须直接排队");
    assert.equal(svc.getTask(t.id)!.pauseReason, "queued");
  }
  const dispatched = [...sessions.values()].filter((s) => s.deliveries.length > 0);
  assert.equal(dispatched.length, 2, "只应有第一位任务的两个执行者被派工，排队任务不得先跑一轮");
});

test("审查33 C5：出队按各执行者恢复，不只叫醒第一位", async () => {
  const { svc, sessions } = await makeService(
    async () => ({ status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false }),
    undefined,
    { maxConcurrentTasks: 1 },
  );
  const first = await svc.propose({ ...proposedInput(), deliverables: ["a.md"] });
  await svc.confirm(first.id);
  const queuedTask = await svc.propose({ ...proposedInput(), deliverables: ["b.md"] });
  await svc.confirm(queuedTask.id);
  assert.equal(svc.getTask(queuedTask.id)!.pauseReason, "queued");

  await svc.pause(first.id, "user", "让出名额");
  const promoted = await svc.releaseQueued(1);
  assert.deepEqual(promoted, [queuedTask.id]);
  assert.equal(svc.getTask(queuedTask.id)!.status, "active");
  const resumed = [...sessions.values()].filter((s) => s.key.includes(queuedTask.id) && s.deliveries.some((d) => d.includes("排队转运行")));
  assert.equal(resumed.length, 2, "两位执行者都要收到恢复上下文，而不是只叫醒第一位");
});

test("审查33 C7：普通等待（等某位专家）在真实消息送达时解除，无需人工 resume", async () => {
  let waiting = true;
  const { svc } = await makeService(async () => ({
    status: "active", reportedNextSteps: [], claimedDone: false, activeCommand: false, pendingReply: waiting, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);

  await svc.onTurnEnd(task.id, "research/a.md", "completed", "turn-c7");
  const waits = svc.listWaits(task.id);
  const wait = waits.find((w) => w.status === "waiting");
  assert.ok(wait, "需要等另一位专家时必须登记等待");
  assert.equal(wait!.wakeCondition, "executor-reply:research/a.md", "等待必须带可匹配条件，而不是 null");

  await svc.registerArtifact(task.id, "E:/e2e/比较表", "b".repeat(64), "research/b.md");
  await svc.sendExpertMessage(task.id, "research/b.md", "research/a.md", "口径已确认，可以继续", { actionId: "c7:msg:0" });
  assert.equal(svc.listWaits(task.id).find((w) => w.id === wait!.id)!.status, "woken", "真实答复送达必须解除等待");
  assert.equal(svc.getTask(task.id)!.waitingOn ?? null, null);
});

test("审查33 C8：一位下游等待不能屏蔽上游停滞监督", async () => {
  const { Watchdog } = await import("../src/watchdog/watchdog.js");
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-wd-c8-"));
  const store = await JsonStore.load(join(dir, "state.json"));
  const ledger = new TokenLedger(store);
  const { svc } = await makeService(
    async () => ({ status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false }),
    undefined,
    {},
    { store, ledger },
  );
  const bRef = expert("research/b.md");
  const task = await svc.propose({
    ...proposedInput(),
    executors: [expert("research/a.md"), bRef],
    deliverables: ["比较表", "推荐"],
    assignments: [
      { relPath: "research/a.md", deliverables: ["比较表"] },
      { relPath: "research/b.md", deliverables: ["推荐"], waitFor: { fromRelPath: "research/a.md", artifactRefs: ["比较表"] } },
    ],
  });
  await svc.confirm(task.id);
  // 只有一个执行者（B）在等待，A 没有任何等待 → 任务不得整体免催
  const watchdog = new Watchdog(store, { stallThresholdMs: 1, maxCorrections: 2 });
  await watchdog.recordInstruction(task.id, "同一句指令");
  await watchdog.recordInstruction(task.id, "同一句指令");
  await store.transact((data) => {
    const coll = (data.collections["progress"] ??= {});
    const p = coll[task.id] as Record<string, unknown>;
    p["lastSubstantiveAt"] = new Date(Date.now() - 600_000).toISOString();
  });
  const entries = watchdog.checkAll();
  assert.ok(entries.some((e) => e.task.id === task.id), "上游停滞必须被监督，不能因下游等待而整体免催");
});

test("审查33 C10/C11：澄清答复能接回；重开消息带完整新标准", async () => {
  const { svc, sessions } = await makeService(async () => ({
    status: "active", reportedNextSteps: ["x"], claimedDone: false, activeCommand: false, pendingReply: false, unhandledEvent: false,
  }));
  const task = await svc.propose(proposedInput());
  await svc.confirm(task.id);
  const executor = [...sessions.values()].find((s) => s.key.includes("research/a.md"))!;

  await svc.applyUserFeedback(task.id, { kind: "clarify", text: "成本还是风险优先？" });
  assert.equal(svc.getTask(task.id)!.status, "paused");
  const answered = await svc.answerClarification(task.id, "风险优先");
  assert.equal(answered.action, "resumed");
  assert.equal(svc.getTask(task.id)!.status, "active");
  assert.ok(executor.deliveries.at(-1)!.includes("用户答复"), "答案必须先送达负责人");
  assert.ok(executor.deliveries.at(-1)!.includes("风险优先"));

  const before = executor.deliveries.length;
  await svc.applyUserFeedback(task.id, {
    kind: "missing_criteria",
    text: "补一条唯一标记标准",
    criteria: [{ id: "c9", text: "UNIQUE_CRITERION_34C7 必须出现" }],
  });
  const reopenMsg = executor.deliveries.slice(before).find((d) => d.includes("标准已更新"));
  assert.ok(reopenMsg, "已有会话必须收到版本变更通知");
  assert.ok(reopenMsg!.includes("UNIQUE_CRITERION_34C7"), "通知里必须带完整新标准，而不是泛化一句");
  assert.ok(reopenMsg!.includes("v2"), "必须说明版本号");
});
