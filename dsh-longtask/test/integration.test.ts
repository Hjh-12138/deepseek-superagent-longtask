import {prepareSdkTools, loadSdk} from "./sdk-tools.js";
/**
 * 链路级集成测试 · 通过真实 apply(ctx) 入口跑通阶段 1 全链路
 * （下一轮审查入口：确认 → 执行 → 提交验收 → 退回续做 → 再验收 → 交付）。
 *
 * 模拟宿主实现真实协议：agents.create 必填 sessionId、返回 AgentHandle{agent, dispose}；
 * session/event 分发；agent/pre-step 拦截链。零预算与活动取消场景同样走 apply 入口。
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { promises as fs } from "node:fs";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { apply, parseWorkerReport, parseAcceptorVerdicts, type LongtaskConfig } from "../src/index.js";
import type { HostContext, AgentHandle } from "../src/host-types.js";
import type { ExpertRef } from "../src/types.js";
import {DEFAULT_PRICES} from "../src/ledger/money.js";
import type { TaskService } from "../src/tasks/task-service.js";

/** ctx.provide("longtask") 暴露的用户入口 */
interface LongtaskApi {
  getMoneyBudget(taskId: string): {limitAmount: string; calculatedAmount: string; reservedAmount: string; availableAmount: string} | null;
  getModelExpenses(taskId: string): Array<{provider: string; model: string; calculatedAmount: string}>;
  sendExpertMessage(...args: Parameters<TaskService["sendExpertMessage"]>): ReturnType<TaskService["sendExpertMessage"]>;
  getExpertMessage(...args: Parameters<TaskService["getExpertMessage"]>): ReturnType<TaskService["getExpertMessage"]>;
  reconcileExpertMessage(...args: Parameters<TaskService["reconcileExpertMessage"]>): ReturnType<TaskService["reconcileExpertMessage"]>;
  replayExecutorTurnEnd(taskId: string, sessionId: string, sourceTurnId: string): boolean;
  getExecutorDependency(...args: Parameters<TaskService["getExecutorDependency"]>): ReturnType<TaskService["getExecutorDependency"]>;
  propose(input: Record<string, unknown>): Promise<{ id: string }>;
  confirm(taskId: string, inheritedSettledTokens?: number): Promise<void>;
  pause(taskId: string, note: string): Promise<void>;
  resume(taskId: string): Promise<void>;
  cancel(taskId: string): Promise<void>;
  getTask(taskId: string): { id: string; status: string; pauseReason: string | null } | undefined;
  listTasks(): unknown[];
  getBudget(taskId: string): import("../src/types.js").TaskBudgetState | undefined;
  registerArtifact(...args: Parameters<TaskService["registerArtifact"]>): ReturnType<TaskService["registerArtifact"]>;
  listWaits(...args: Parameters<TaskService["listWaits"]>): ReturnType<TaskService["listWaits"]>;
  sweepWaits(taskId?: string): Promise<number>;
  applyUserFeedback(...args: Parameters<TaskService["applyUserFeedback"]>): ReturnType<TaskService["applyUserFeedback"]>;
  answerClarification(...args: Parameters<TaskService["answerClarification"]>): ReturnType<TaskService["answerClarification"]>;
  getClarificationAnswers(...args: Parameters<TaskService["getClarificationAnswers"]>): ReturnType<TaskService["getClarificationAnswers"]>;
  releaseQueued(limit?:number): Promise<string[]>;
  getBackgroundJobs(...args: Parameters<TaskService["getBackgroundJobs"]>): ReturnType<TaskService["getBackgroundJobs"]>;
  getRuntimeInfo(): { ready: boolean; stage3Protocol: number; budgetProtectionVersion: number; maxConcurrentTasks: number; pid: number; bootAt: number };
  getTaskEvidence(...args: Parameters<TaskService["getTaskEvidence"]>): ReturnType<TaskService["getTaskEvidence"]>;
  getRouterDigest(taskId: string): {
    artifacts: Array<{ location: string; hash: string | null; owner: string | null }>;
    executors: string[];
    latestSummaries: Array<{ summary: string; artifactRefs: string[]; messageId: string; causedByMessageId: string | null }>;
  } | null;
}

// ---------------------------------------------------------------------------
// 模拟宿主
// ---------------------------------------------------------------------------

function makeFakeHost(options: { failPresets?: string[]; noToolGuard?: boolean } = {}) {
  interface FakeAgent {
    id: string;
    sessionId: string;
    messages: string[];
    stopped: boolean;
    session: { id: string; header?: { parentSession?: string } };
    nextTurn: string[];
    nextStep: string[];
    consumeStep(turnStart?: boolean): string[];
    followup: AgentHandle["agent"]["followup"];
  }
  const agentsById = new Map<string, FakeAgent>();
  const handles: AgentHandle[] = [];
  const listeners = new Map<string, Array<(...args: any[]) => void>>();
  const provided = new Map<string, unknown>();
  const teardowns: Array<() => unknown> = [];
  const stepMessages = new Map<string, unknown[]>();
  const mountedPresets = new Map<string, string>();
  /** setup（含 preset 挂载）已完成的会话：投递必须发生在这之后 */
  const readySessions = new Set<string>();
  const failPresets = options.failPresets ?? [];
  let createCalls = 0;

  const agentsService = {
    async create(options: { sessionId?: string; meta?: Record<string, unknown>; setup?(agentCtx: unknown, agent: AgentHandle["agent"]): unknown | Promise<unknown> }) {
      assert.ok(options.sessionId, "CreateAgentOptions.sessionId 必填（真实 SDK 协议）");
      const wanted = (options.meta as { agentPreset?: string } | undefined)?.agentPreset;
      if (wanted && failPresets.includes(wanted)) throw new Error(`agent preset not found: ${wanted}`);
      createCalls++;
      const sessionId = options.sessionId!;
      const agentCtx = { sessionId };
      const agent = {
        id: sessionId,
        sessionId,
        messages: [] as string[],
        stopped: false,
        nextTurn: [] as string[],
        nextStep: [] as string[],
        consumeStep(turnStart = false) {
          return [...(turnStart ? agent.nextTurn.splice(0) : []), ...agent.nextStep.splice(0)];
        },
        followup(message: { id: string; role: string; content: ReadonlyArray<{ type: string; text?: string }> }) {
          assert.equal(message.role, "user", "followup 接收完整 UserMessage，缺少 role 会导致适配器序列化失败");
          assert.ok(message.id, "followup 消息必须有稳定 id");
          assert.ok(readySessions.has(sessionId), "首次投递前必须等待 agent 作用域的 preset 挂载完成");
          const text = message.content.map((b) => b.text ?? "").join("");
          agent.messages.push(text);
          agent.nextTurn.push(text);
        },
        steer(message: Parameters<AgentHandle["agent"]["followup"]>[0]) {
          assert.equal(message.role, "user");
          assert.ok(message.id);
          assert.ok(readySessions.has(sessionId));
          const text = message.content.map((b) => b.type === "text" ? b.text : "").join("");
          agent.messages.push(text);
          agent.nextStep.push(text);
        },
        cancel() {
          agent.stopped = true;
        },
        async whenIdle() {},
        status: "idle",
        session: { id: sessionId },
      };
      // 与真实 SDK 一致：先 await setup，再发布 Agent；meta 标签不能替代装配。
      await options.setup?.(agentCtx, agent);
      readySessions.add(sessionId);
      agentsById.set(sessionId, agent);
      const handle: AgentHandle = {
        agent,
        dispose: async () => {
          agentsById.delete(sessionId);
        },
      };
      handles.push(handle);
      return handle;
    },
    get(id: string) {
      const a = agentsById.get(id);
      return a ? { ...a } : undefined;
    },
    list() {
      return [...agentsById.values()];
    },
  };

  const logs: string[] = [];
  const toolGuards = new Set<(exec: { name: string; agent?: unknown }) => string | undefined>();
  const ctx: HostContext = {
    logger: {
      info: (m: string) => logs.push(`[info] ${m}`),
      warn: (m: string) => logs.push(`[warn] ${m}`),
      error: (m: string) => logs.push(`[error] ${m}`),
    },
    get: <T>(name: string): T | undefined => {
      if (name === "agents") return agentsService as unknown as T;
      if (name === "sessions") return { flush: async () => true } as T;
      if (name === "tools") return (options.noToolGuard ? {} : {
        guard(fn: (exec: { name: string; agent?: unknown }) => string | undefined) {
          toolGuards.add(fn);
          return () => { toolGuards.delete(fn); };
        },
      }) as T;
      if (name === "agentPresets") return {
        async mount(agentCtx: { sessionId: string }, preset: string) {
          assert.ok(agentCtx.sessionId, "必须挂载到 setup 提供的 Agent 上下文");
          await new Promise<void>((r) => setTimeout(r, 1));
          mountedPresets.set(agentCtx.sessionId, preset);
          return { id: preset };
        },
      } as unknown as T;
      return undefined;
    },
    on: (event: string, listener: (...args: any[]) => void) => {
      (listeners.get(event) ?? listeners.set(event, []).get(event)!).push(listener);
      return () => {
        const arr = listeners.get(event);
        const index = arr?.indexOf(listener) ?? -1;
        if (index >= 0) arr!.splice(index, 1);
      };
    },
    off: (event: string, listener: (...args: any[]) => void) => {
      const arr = listeners.get(event);
      if (arr) arr.splice(arr.indexOf(listener), 1);
    },
    effect(factory: () => Generator<unknown>) {
      const gen = factory();
      const step = gen.next();
      // 生成器体是同步的；yield 出 teardown 函数
      const value = step.value as unknown;
      if (!step.done && typeof value === "function") teardowns.push(value as () => unknown);
    },
    provide: (name: string, value: unknown) => provided.set(name, value),
  };

  const emitSessionEvent = (sessionId: string, event: Record<string, unknown>) => {
    for (const l of listeners.get("session/event") ?? []) l({ id: sessionId }, event);
  };
  // 真实 SDK 的 waterfall 允许返回 Promise：准入必须 await 到预留落账才放行
  const emitPreStep = async (sessionId: string, messages: unknown[] = [], runtimeMessages: unknown[] = []) => {
    for (const l of listeners.get("agent/pre-step") ?? []) {
      const result = await (l as (...a: unknown[]) => unknown)(
        { agent: agentsById.get(sessionId) ?? { session: { id: sessionId }, id: sessionId }, messages },
        () => ({ kind: "enter", messages: [...messages, ...runtimeMessages] }),
      );
      if (result && (result as { kind: string }).kind === "reject") return false;
      if (result && Array.isArray((result as { messages?: unknown[] }).messages)) stepMessages.set(sessionId, (result as { messages: unknown[] }).messages);
    }
    return true;
  };
  const flush = () => new Promise<void>((r) => setTimeout(r, 5));
  const emitRequest = async (sessionId: string, config: Record<string, unknown> = {provider: "offline", model: "offline", maxTokens: 8000}) => {
    let calls = 0;
    for (const listener of listeners.get("agent/request") ?? []) await listener(
      {agent: agentsById.get(sessionId), turn: 1, step: 1, signal: new AbortController().signal},
      async () => { calls++; return config; },
    );
    return calls;
  };
  const waitFor = async (cond: () => boolean, timeoutMs = 2000) => {
    const start = Date.now();
    while (!cond()) {
      if (Date.now() - start > timeoutMs) throw new Error(`waitFor 超时\n插件日志：\n${logs.join("\n")}`);
      await flush();
    }
  };

  const emitTool = async (sessionId: string, name: string, body: () => unknown) => {
    const exec = { name, agent: agentsById.get(sessionId) ?? { id: sessionId, session: { id: sessionId } } };
    for (const guard of toolGuards) {
      const reason = guard(exec);
      if (reason !== undefined) return { denied: true, reason };
    }
    await body();
    return { denied: false };
  };
  const addUnmanagedAgent = (sessionId: string, parentSession: string) => {
    const agent: FakeAgent = {
      id: sessionId, sessionId, session: { id: sessionId, header: { parentSession } },
      messages: [], stopped: false, nextStep: [], nextTurn: [], consumeStep: () => [], followup: () => {},
    };
    agentsById.set(sessionId, agent);
    return agent;
  };
  return { ctx, provided, agentsById, mountedPresets, handles, stepMessages, toolGuards, emitTool, emitRequest, addUnmanagedAgent, emitSessionEvent, emitPreStep, flush, waitFor, stats: { get createCalls() { return createCalls; } }, teardowns, logs };
}

// 助手：取某一会话的最新投递
test("发布预算：整个usage缺失时保留预留，阻止工具、续做、交付及手动恢复", async () => {
  const {fake,api}=await makeApp();
  try {
    const t=await api.propose({goal:"缺usage不能交付",criteria:[{id:"c1",text:"有产物"}],deliverables:["a.md"],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:400000,strict:true}});
    await api.confirm(t.id);const worker=[...fake.agentsById.values()][0]!;
    assert.equal(await fake.emitPreStep(worker.id),true);const reserved=api.getBudget(t.id)!.reservedTokens;assert.ok(reserved>0);
    fake.emitSessionEvent(worker.id,{seq:100,...assistantEvent('<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>',1)});
    fake.emitSessionEvent(worker.id,turnEndEvent(1));
    await fake.waitFor(()=>api.getTask(t.id)?.status==='paused');
    assert.equal(api.getTask(t.id)?.pauseReason,'budget');assert.equal(api.getBudget(t.id)!.reservedTokens,reserved);assert.equal(api.getBudget(t.id)!.settledTokens,0);
    assert.equal(await fake.emitPreStep(worker.id),false);assert.equal((await fake.emitTool(worker.id,'write',()=>assert.fail('不能写入'))).denied,true);
    await assert.rejects(()=>api.resume(t.id),/用量|核对|对账/);assert.equal(fake.agentsById.size,1);
  } finally {for(const stop of fake.teardowns)await stop()}
});

test("发布预算：同一步SDK重试走request闸门，失败attempt从stream结算，重复事件不误扣下一次预留",async()=>{
  const {fake,api}=await makeApp();
  try{
    const t=await api.propose({goal:"同step两次尝试",criteria:[{id:"c1",text:"完成"}],deliverables:["a.md"],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:400000,strict:true}});await api.confirm(t.id);const worker=[...fake.agentsById.values()][0]!;
    assert.equal(await fake.emitPreStep(worker.id),true);assert.equal(await fake.emitRequest(worker.id),1);
    const attempt={seq:101,type:'assistant/attempt',data:{turn:1,step:1,stream:[{type:'chunk',time:1,chunk:{type:'usage',usage:{inputTokens:7,outputTokens:4,cacheReadTokens:0,cacheWriteTokens:0,totalTokens:11}}},{type:'chunk',time:2,chunk:{type:'finish',reason:{kind:'error'},failure:{code:'TRANSPORT',message:'offline'}}}]}};
    fake.emitSessionEvent(worker.id,attempt);await fake.waitFor(()=>api.getBudget(t.id)!.settledTokens===11);
    assert.equal(await fake.emitRequest(worker.id),1,'同step第二次request必须重新预留');const reserved=api.getBudget(t.id)!.reservedTokens;assert.ok(reserved>0);
    fake.emitSessionEvent(worker.id,structuredClone(attempt));await fake.flush();assert.equal(api.getBudget(t.id)!.reservedTokens,reserved);
    fake.emitSessionEvent(worker.id,{seq:102,...assistantUsageEvent(1,{inputTokens:9,outputTokens:4,cacheReadTokens:0,cacheWriteTokens:0})});await fake.waitFor(()=>api.getBudget(t.id)!.settledTokens===24);
    assert.equal(api.getBudget(t.id)!.reservedTokens,0);const entries=Object.values(api.getTaskEvidence(t.id).collections.ledger??{}) as any[];assert.equal(entries.length,2);assert.notEqual(entries[0].attemptId,entries[1].attemptId);
  }finally{for(const stop of fake.teardowns)await stop()}
});

test("发布预算：失败attempt没有usage不按零费用重试，未知预留不得释放",async()=>{
  const {fake,api}=await makeApp();try{
    const t=await api.propose({goal:"失败用量未知",criteria:[{id:'c1',text:'完成'}],deliverables:['a.md'],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:400000,strict:true}});await api.confirm(t.id);const worker=[...fake.agentsById.values()][0]!;
    await fake.emitPreStep(worker.id);await fake.emitRequest(worker.id);const reserved=api.getBudget(t.id)!.reservedTokens;
    fake.emitSessionEvent(worker.id,{seq:103,type:'assistant/attempt',data:{turn:1,step:1,stream:[]}});
    await assert.rejects(()=>fake.emitRequest(worker.id));await fake.waitFor(()=>api.getTask(t.id)?.status==='paused');assert.equal(api.getBudget(t.id)!.reservedTokens,reserved);
  }finally{for(const stop of fake.teardowns)await stop()}
});

test("单次输出截断：原总预算足够时自动续原Worker，同一结束事件重放不重复派工",async()=>{
  const {fake,api}=await makeApp();try{
    const t=await api.propose({goal:'截断继续',criteria:[{id:'c1',text:'完成'}],deliverables:['a.md'],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:500000,strict:true}});await api.confirm(t.id);const w=[...fake.agentsById.values()][0]!;
    await fake.emitPreStep(w.id);await fake.emitRequest(w.id);fake.emitSessionEvent(w.id,{seq:300,...assistantUsageEvent(1,{inputTokens:22338,outputTokens:8000,cacheReadTokens:0,cacheWriteTokens:0},'输出在单次上限被截断')});
    const end={seq:301,type:'turn/end',data:{turn:1,reason:{kind:'max-tokens'}}};fake.emitSessionEvent(w.id,end);await fake.waitFor(()=>w.messages.length===2);
    fake.emitSessionEvent(w.id,structuredClone(end));await fake.flush();assert.equal(w.messages.length,2);assert.equal(fake.stats.createCalls,1);assert.equal(api.getTask(t.id)?.status,'active');assert.equal(api.getBudget(t.id)?.settledTokens,30338);assert.equal(api.getBudget(t.id)?.limitTokens,500000);assert.match(w.messages[1]!,/单次|截断/);
  }finally{for(const stop of fake.teardowns)await stop()}
});

test("发布预算：SDK重试剩余额度不足时在request下游前拒绝，无关用户请求放行",async()=>{
  const {fake,api}=await makeApp({acceptanceReserveTokens:0,wrapUpAtBudgetRatio:0,wrapUpMinStepsLeft:0});try{
    assert.equal(await fake.emitRequest('unrelated'),1);
    const t=await api.propose({goal:'最后额度不够重试',criteria:[{id:'c1',text:'完成'}],deliverables:['a.md'],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:50000,strict:true}});await api.confirm(t.id);const w=[...fake.agentsById.values()][0]!;
    assert.equal(await fake.emitPreStep(w.id),true);await fake.emitRequest(w.id);
    fake.emitSessionEvent(w.id,{seq:201,type:'assistant/attempt',data:{turn:1,step:1,stream:[{type:'chunk',time:1,chunk:{type:'usage',usage:{inputTokens:25000,outputTokens:1000,cacheReadTokens:0,cacheWriteTokens:0}}}]}});
    await assert.rejects(()=>fake.emitRequest(w.id),/重试预算不足/);assert.equal(api.getBudget(t.id)!.settledTokens,26000);assert.equal(api.getBudget(t.id)!.reservedTokens,0);assert.equal(api.getTask(t.id)?.pauseReason,'budget');
  }finally{for(const stop of fake.teardowns)await stop()}
});

test("发布预算：部分usage字段缺失后重启仍保守扣账并保持核对暂停，不能恢复清标记",async()=>{
  const {fake,api,dir}=await makeApp();let second:ReturnType<typeof makeFakeHost>|undefined;try{
    const t=await api.propose({goal:'异常用量跨重启',criteria:[{id:'c1',text:'完成'}],deliverables:['a.md'],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:400000,strict:true}});await api.confirm(t.id);const w=[...fake.agentsById.values()][0]!;
    await fake.emitPreStep(w.id);await fake.emitRequest(w.id);const reserved=api.getBudget(t.id)!.reservedTokens;
    fake.emitSessionEvent(w.id,{seq:202,type:'assistant/message',data:{turn:1,step:1,message:{role:'assistant',content:[]},usage:{inputTokens:2}}});await fake.waitFor(()=>api.getTask(t.id)?.status==='paused');
    for(const stop of fake.teardowns.splice(0))await stop();second=makeFakeHost();apply(second.ctx,{dataDir:dir,agencyAgentsRoot:join(dir,'experts'),probeOnStart:false,watchdogIntervalMs:3600000});await second.waitFor(()=>Boolean(second!.provided.get('longtask')));
    const recovered=second.provided.get('longtask') as LongtaskApi;await second.waitFor(()=>recovered.getRuntimeInfo().ready);assert.equal(recovered.getTask(t.id)?.status,'paused');assert.equal(recovered.getBudget(t.id)!.settledTokens,reserved);assert.equal(recovered.getBudget(t.id)!.reservedTokens,0);assert.equal(second.stats.createCalls,0);
    await assert.rejects(()=>recovered.resume(t.id),/用量|核对/);assert.ok(recovered.getTaskEvidence(t.id).collections.accountingIssues?.[t.id]);
  }finally{for(const stop of fake.teardowns)await stop();for(const stop of second?.teardowns??[])await stop()}
});

function lastDelivery(agent: { messages: string[] }): string {
  assert.ok(agent.messages.length > 0, "会话应已收到投递");
  return agent.messages[agent.messages.length - 1]!;
}

/**
 * 从验收请求里取回冻结哈希：请求内的结论协议模板已把 artifactHashes 原样给出，
 * 验收人照抄回填即可。测试用同一来源，避免把空哈希当成合法结论。
 */
function frozenHashes(acceptRequest: string): Record<string, string> {
  const block = /<longtask_verdicts>([\s\S]*?)<\/longtask_verdicts>/.exec(acceptRequest);
  assert.ok(block, "验收请求应包含结论协议模板");
  const proto = JSON.parse(block![1]!) as { artifactHashes?: Record<string, string> };
  return proto.artifactHashes ?? {};
}

function assistantEvent(text: string, turn: number) {
  return {
    type: "assistant/message",
    data: { turn, message: { role: "assistant", content: [{ type: "text", text }] } },
  };
}

/** 带真实用量的事件：账本结算与下一轮输入上界都依赖它（P1-1） */
function assistantUsageEvent(turn: number, usage: Record<string, number>, text = "本轮输出") {
  return {
    type: "assistant/message",
    data: { turn, message: { role: "assistant", content: [{ type: "text", text }] }, usage },
  };
}

/** 模拟 inbox 新入站内容；普通工具结果另经 session/event 写入 surface。 */
function inboundChars(n: number) {
  return [{ role: "tool", content: [{ type: "text", text: "x".repeat(n) }] }];
}

function turnEndEvent(turn: number) {
  return { type: "turn/end", data: { turn, reason: { kind: "completed" } } };
}

function expertRef(relPath: string, name: string): ExpertRef {
  return { division: "research", relPath, name, intro: "测试专家", contentHash: "hash123", snapshottedAt: new Date().toISOString() };
}

const EXECUTOR = expertRef("research/worker.md", "Worker");
const REVIEWER = expertRef("research/reviewer.md", "Reviewer");

async function makeApp(overrides: Partial<LongtaskConfig> = {}, hostOptions: { failPresets?: string[]; noToolGuard?: boolean } = {}) {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-int-"));
  // 临时专家库：走真实目录扫描器，专家定义与 fake 专家 relPath 对应
  const expertsRoot = join(dir, "experts");
  await fs.mkdir(join(expertsRoot, "research"), { recursive: true });
  await fs.writeFile(
    join(expertsRoot, "divisions.json"),
    JSON.stringify({ divisions: { research: { label: "Research" } } }),
    "utf8",
  );
  const expertMd = (name: string) => `---\nname: ${name}\ndescription: 测试专家 ${name}\n---\n\n# ${name} 工作方法\n`;
  await fs.writeFile(join(expertsRoot, "research", "worker.md"), expertMd("Worker"), "utf8");
  await fs.writeFile(join(expertsRoot, "research", "reviewer.md"), expertMd("Reviewer"), "utf8");

  const fake = makeFakeHost(hostOptions);
  const cfg: Partial<LongtaskConfig> = {
    dataDir: dir,
    agencyAgentsRoot: expertsRoot,
    probeOnStart: false,
    watchdogIntervalMs: 3600_000,
    ...overrides,
  };
  apply(fake.ctx, cfg);
  // 初始化是异步的（store 落盘、目录扫描）：等到服务真正暴露再返回，避免把调度时序当失败
  await fake.waitFor(() => Boolean(fake.provided.get("longtask")), 5000);
  const api = fake.provided.get("longtask") as LongtaskApi;
  assert.ok(api, "longtask 服务已注册（P1-4）");
  return { fake, api, dir };
}

function findAgentByMessage(fake: ReturnType<typeof makeFakeHost>, substring: string) {
  for (const agent of fake.agentsById.values()) {
    if (agent.messages.some((m) => m.includes(substring))) return agent;
  }
  return undefined;
}

test('阶段三消费者接口：就绪状态和只读审计快照不暴露其他任务',async()=>{
  const {fake,api}=await makeApp();
  try{
    await fake.waitFor(()=>api.getRuntimeInfo().ready);assert.equal(api.getRuntimeInfo().stage3Protocol,2);assert.equal(api.getRuntimeInfo().budgetProtectionVersion,1);assert.equal(api.getRuntimeInfo().pid,process.pid);
    const input={goal:'独立任务',criteria:[{id:'c1'}],deliverables:['one.md'],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:200000,strict:true}};
    const a=await api.propose(input),b=await api.propose({...input,goal:'UNRELATED-PRIVATE-MARKER',deliverables:['two.md']});await api.confirm(a.id);
    const evidence=api.getTaskEvidence(a.id);assert.equal(JSON.stringify(evidence).includes('UNRELATED-PRIVATE-MARKER'),false);assert.equal(evidence.collections.tasks![b.id],undefined);
    (evidence.collections.tasks![a.id] as any).status='cancelled';assert.equal(api.getTask(a.id)!.status,'active');
  }finally{for(const stop of fake.teardowns)await stop();}
  assert.equal(api.getRuntimeInfo().ready,false);
});

test('阶段三恢复能力前置：工具迟到时不进入模型，装载后放行，取消时结束等待',async()=>{
  const {fake,api}=await makeApp();const get=fake.ctx.get.bind(fake.ctx);let loaded=false;
  fake.ctx.get=((name:string)=>name==='tools'?{...get<any>('tools'),get:(_tool:string,scope:unknown)=>loaded&&[...fake.agentsById.values()].includes(scope as any)?{name:'checkpoint'}:undefined}:get(name)) as typeof fake.ctx.get;
  try{
    const input={goal:'依赖工具',criteria:[{id:'c1'}],deliverables:['capability.md'],executors:[EXECUTOR],acceptor:REVIEWER,requiredTools:['checkpoint'],budget:{limitTokens:500000,strict:true}};
    const t=await api.propose(input);await api.confirm(t.id);const worker=[...fake.agentsById.values()][0]!;
    let completed=false;const pending=fake.emitPreStep(worker.sessionId).then(result=>{completed=true;return result;});await fake.flush();await fake.flush();assert.equal(completed,false);
    loaded=true;assert.equal(await pending,true);assert.equal(api.getBudget(t.id)!.settledTokens,0);
    loaded=false;const second=await api.propose({...input,deliverables:['other-capability.md']});await api.confirm(second.id);
    const next=[...fake.agentsById.values()].find(a=>a.sessionId!==worker.sessionId)!;const waiting=fake.emitPreStep(next.sessionId);await fake.flush();await api.cancel(second.id);assert.equal(await waiting,false);assert.equal(api.getBudget(second.id)!.settledTokens,0);
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("审查34 真实apply：并发/同产物派工前限制与Watchdog进展钩子确实接线",async()=>{
  const {fake,api,dir}=await makeApp({maxConcurrentTasks:1});
  try{
    const tasks=[];for(let i=0;i<3;i++){const t=await api.propose({goal:`任务${i}`,criteria:[{id:"c1",text:"交付"}],deliverables:[`d${i}`],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:500000,strict:true}});await api.confirm(t.id);tasks.push(t);}
    assert.equal(fake.agentsById.size,1);assert.equal((api.listTasks() as any[]).filter(t=>t.status==="active").length,1);
    const load=async()=>JSON.parse(await fs.readFile(join(dir,"state.json"),"utf8"));
    const p=(await load()).collections.progress[tasks[0]!.id];assert.ok(p.recentInstructionHashes.length>0);
    await new Promise(r=>setTimeout(r,10));await api.registerArtifact(tasks[0]!.id,join(dir,"d0"),"a".repeat(64),EXECUTOR.relPath);
    const changed=(await load()).collections.progress[tasks[0]!.id];assert.ok(Date.parse(changed.lastSubstantiveAt)>Date.parse(p.lastSubstantiveAt));
    await new Promise(r=>setTimeout(r,10));await api.registerArtifact(tasks[0]!.id,join(dir,"d0"),"a".repeat(64),EXECUTOR.relPath);assert.equal((await load()).collections.progress[tasks[0]!.id].lastSubstantiveAt,changed.lastSubstantiveAt);
  }finally{for(const stop of fake.teardowns)await stop();}
  const app=await makeApp({maxConcurrentTasks:2});
  try{for(let i=0;i<2;i++){const t=await app.api.propose({goal:`同产物${i}`,criteria:[{id:"c1"}],deliverables:["shared.md"],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:500000,strict:true}});await app.api.confirm(t.id);}assert.equal(app.fake.agentsById.size,1,"同产物任务不得并行开工");}finally{for(const stop of app.fake.teardowns)await stop();}
});

test("审查34 真实job生命周期：精确唤醒、重复结算不重做、结果先到也能继续",async()=>{
  const {fake,api,dir}=await makeApp();
  try{
    const t=await api.propose({goal:"后台命令",criteria:[{id:"c1"}],deliverables:["job.md"],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:1000000,strict:true}});await api.confirm(t.id);
    const worker=[...fake.agentsById.values()][0]!;
    const emitJob=(id:string,settled:boolean,owner=worker.sessionId)=>fake.emitSessionEvent(worker.sessionId,{type:settled?"job/settled":"job/registered",data:{jobId:id,owner,status:settled?"completed":"running"}});
    const pollJob=async(id:string,predicate:(j:any)=>boolean)=>fake.waitFor(()=>predicate(api.getBackgroundJobs(t.id).find(j=>j.jobId===id)));
    emitJob("j1",false);await pollJob("j1",j=>!!j&&!j.settled);
    fake.emitSessionEvent(worker.sessionId,assistantEvent('<longtask_report>{"done":false,"next":[],"waiting":"后台命令j1"}</longtask_report>',1));fake.emitSessionEvent(worker.sessionId,turnEndEvent(1));
    await fake.waitFor(()=>api.listWaits(t.id).some(w=>w.wakeCondition===`job:${EXECUTOR.relPath}:j1`&&w.status==="waiting"));
    const before=worker.messages.length;emitJob("foreign",true,"unrelated-session");await fake.flush();assert.equal(worker.messages.length,before);
    emitJob("j1",true);await fake.waitFor(()=>worker.messages.length===before+1);assert.ok(api.listWaits(t.id).every(w=>w.status!=="waiting"));
    emitJob("j1",true);await fake.flush();await fake.flush();assert.equal(worker.messages.length,before+1);
    emitJob("early",false);emitJob("early",true);await pollJob("early",j=>j?.settled);
    const prior=worker.messages.length;
    fake.emitSessionEvent(worker.sessionId,assistantEvent('<longtask_report>{"done":false,"next":[],"waiting":"后台命令early"}</longtask_report>',2));fake.emitSessionEvent(worker.sessionId,turnEndEvent(2));
    await fake.waitFor(()=>worker.messages.length===prior+1);assert.ok(worker.messages.at(-1)!.includes("early"));assert.ok(api.listWaits(t.id).every(w=>w.status!=="waiting"));
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("审查34 答复持久化生命周期：先关闭旧实例，新apply出队时完整接回答案",async()=>{
  const first=await makeApp({maxConcurrentTasks:1});const {api,dir}=first;
  let second:ReturnType<typeof makeFakeHost>|undefined;
  try{
    const tasks=[];for(let i=0;i<2;i++){const t=await api.propose({goal:`重启答复${i}`,criteria:[{id:"c1"}],deliverables:[`answer-${i}.md`],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:500000,strict:true}});await api.confirm(t.id);tasks.push(t);}
    await api.applyUserFeedback(tasks[1]!.id,{kind:"clarify",text:"口径？"});
    const body="RESTART-ANSWER-"+"完整正文".repeat(80);assert.equal((await api.answerClarification(tasks[1]!.id,body)).action,"queued");
    for(const stop of first.fake.teardowns)await stop();
    second=makeFakeHost();apply(second.ctx,{dataDir:dir,agencyAgentsRoot:join(dir,"experts"),probeOnStart:false,maxConcurrentTasks:1,watchdogIntervalMs:3600000});
    await second.waitFor(()=>!!second!.provided.get("longtask"));
    await second.waitFor(()=>second!.logs.some(l=>l.includes("恢复")&&l.includes("会话"))||[...second!.agentsById.values()].some(a=>a.messages.length>0));
    const api2=second.provided.get("longtask") as LongtaskApi;
    assert.equal(api2.getClarificationAnswers(tasks[1]!.id)[0]!.answer,body);
    await api2.cancel(tasks[0]!.id);await api2.releaseQueued(1);
    assert.equal([...second.agentsById.values()].flatMap(a=>a.messages).filter(m=>m.includes(body)).length,1);
    assert.equal(api2.getClarificationAnswers(tasks[1]!.id)[0]!.delivery,"delivered");
  }finally{if(second){for(const stop of second.teardowns)await stop();}else{for(const stop of first.fake.teardowns)await stop();}}
});

// ---------------------------------------------------------------------------
// 报告解析
// ---------------------------------------------------------------------------

test("parseWorkerReport / parseAcceptorVerdicts 结构化提取", () => {
  const worker = parseWorkerReport("前言\n<longtask_report>\n{\"done\": false, \"next\": [\"a\", \"b\"], \"waiting\": null}\n</longtask_report>\n后记");
  assert.ok(worker);
  assert.equal(worker!.done, false);
  assert.deepEqual(worker!.next, ["a", "b"]);

  assert.equal(parseWorkerReport("无报告块"), null);
  assert.equal(parseWorkerReport("<longtask_report>not json</longtask_report>"), null);

  // 本轮联调的真实形态：正文先提到一句标签（不带闭标签），末尾才是真报告块。
  // 按"首个开标签配首个闭标签"会把几百字散文当 JSON，整轮被判成"未声明完成"。
  const proseThenReport = [
    "按 `<longtask_report>` 的 done/next/waiting 续做或收尾，投递带幂等键。",
    "",
    "核对结论：c1 与 c2 均已对照源码验证。",
    "",
    "<longtask_report>",
    '{"done": true, "next": [], "waiting": null}',
    "</longtask_report>",
  ].join("\n");
  const parsed = parseWorkerReport(proseThenReport);
  assert.ok(parsed, "散文里提到标签不得污染解析");
  assert.equal(parsed!.done, true);

  // 引用了模板示例（done:false）又给出真实结论：以最后一个可解析块为准
  const exampleThenReal = [
    "格式示例：",
    "<longtask_report>",
    '{"done": false, "next": ["示例"], "waiting": null}',
    "</longtask_report>",
    "",
    "我的实际结论：",
    "<longtask_report>",
    '{"done": true, "next": [], "waiting": null}',
    "</longtask_report>",
  ].join("\n");
  assert.equal(parseWorkerReport(exampleThenReal)!.done, true, "取最后一个可解析块");

  const acceptor = parseAcceptorVerdicts(
    '<longtask_verdicts>{"roundId":"round_1","verdicts":[{"criterionId":"c1","outcome":"pass","evidence":"e"}],"artifactHashes":{}}</longtask_verdicts>',
  );
  assert.ok(acceptor);
  assert.equal(acceptor!.roundId, "round_1");
  assert.equal(acceptor!.verdicts[0]!.criterionId, "c1");

  // 验收结论同样会被正文提及污染
  const verdictsWithProse = [
    "我会按 `<longtask_verdicts>` 的格式给结论。",
    '<longtask_verdicts>{"roundId":"round_2","verdicts":[{"criterionId":"c1","outcome":"gap","evidence":"缺"}],"artifactHashes":{}}</longtask_verdicts>',
  ].join("\n");
  assert.equal(parseAcceptorVerdicts(verdictsWithProse)!.roundId, "round_2");
});

// ---------------------------------------------------------------------------
// 全链路：确认 → 执行 → 提交验收 → 退回续做 → 再验收 → 交付
// ---------------------------------------------------------------------------

test("apply 全链路：双标准任务经真实入口跑通提交/退回/续做/再验收/交付", async () => {
  const { fake, api, dir } = await makeApp();

  const task = (await api.propose({
    goal: "比较两个方案并交付推荐",
    criteria: [
      { id: "c1", text: "交付比较表" },
      { id: "c2", text: "给出有依据的推荐" },
    ],
    deliverables: ["比较表", "推荐"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);

  // Worker 收到含目标/标准/专家定义的简报
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  assert.ok(worker, "Worker 会话已创建");
  assert.equal(fake.mountedPresets.get(worker.sessionId), "standard", "执行会话已挂载预设");
  const brief = lastDelivery(worker);
  assert.ok(brief.includes("比较两个方案并交付推荐"));
  assert.ok(brief.includes("[c1] 交付比较表"));
  assert.ok(brief.includes("<longtask_report>"));

  // 第一轮：报告有缺项 → 续做投递
  fake.emitSessionEvent(worker.sessionId, assistantEvent('进度\n<longtask_report>{"done": false, "next": ["补齐推荐依据"], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
  await fake.waitFor(() => worker.messages.length >= 2);
  assert.ok(lastDelivery(worker).includes("补齐推荐依据"), "缺项交回负责 Worker");

  // 第二轮：声明做完 → 待验收 → 验收请求发出
  // Worker 落盘交付物（验收齐备性预检要求产物真实存在，否则直接退回）
  await fs.writeFile(join(dir, "比较表"), "# 比较表\nA vs B\n", "utf8");
  await fs.writeFile(join(dir, "推荐"), "# 推荐\n选 A\n", "utf8");
  fake.emitSessionEvent(worker.sessionId, assistantEvent('完成\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 2));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(2));
  await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
  assert.equal((api.getTask(task.id) as { status: string }).status, "awaiting_acceptance");

  const acceptor = findAgentByMessage(fake, "独立验收请求")!;
  assert.equal(fake.mountedPresets.get(acceptor.sessionId), "standard", "独立验收会话同样需要完整工具能力");
  assert.equal(await fake.emitPreStep(acceptor.sessionId), true, "awaiting_acceptance 必须放行验收人");
  assert.equal(await fake.emitPreStep(worker.sessionId), false, "验收期间不允许执行者继续修改产物");
  const acceptRequest = lastDelivery(acceptor);
  const roundMatch = /轮次 (round_[a-z0-9]+)/.exec(acceptRequest);
  assert.ok(roundMatch, "验收请求含轮次 ID");
  const roundId = roundMatch![1]!;
  assert.ok(acceptRequest.includes("[c2] 给出有依据的推荐"));
  const frozen1 = frozenHashes(acceptRequest);
  assert.ok(Object.keys(frozen1).length >= 2, "两个交付物都应在验收时冻结哈希");
  // 该验收请求已经准入：响应必须同时带模型用量，不能用无usage事件模拟正常成功。
  fake.emitSessionEvent(acceptor.sessionId, assistantUsageEvent(1, {inputTokens: 100, outputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0}));
  await fake.waitFor(() => api.getBudget(task.id)!.reservedTokens === 0);

  // 第一版验收：漏掉 c2（只有 c1）→ 被拒，不进入交付
  fake.emitSessionEvent(
    acceptor.sessionId,
    assistantEvent(
      `<longtask_verdicts>${JSON.stringify({ roundId, verdicts: [{ criterionId: "c1", outcome: "pass", evidence: "比较表已核对", gap: null, verdictBy: REVIEWER.relPath, criteriaVersion: 1 }], artifactHashes: frozen1 })}</longtask_verdicts>`,
      1,
    ),
  );
  fake.emitSessionEvent(acceptor.sessionId, turnEndEvent(1));
  await fake.flush();
  assert.equal((api.getTask(task.id) as { status: string }).status, "awaiting_acceptance", "漏项结论被拒，状态不变");

  // 完整结论：c1 pass、c2 gap → rework，缺项交回 Worker
  fake.emitSessionEvent(
    acceptor.sessionId,
    assistantEvent(
      `<longtask_verdicts>${JSON.stringify({
        roundId,
        verdicts: [
          { criterionId: "c1", outcome: "pass", evidence: "比较表已核对", gap: null, verdictBy: REVIEWER.relPath, criteriaVersion: 1 },
          { criterionId: "c2", outcome: "gap", evidence: "无推荐内容", gap: "推荐项缺失", verdictBy: REVIEWER.relPath, criteriaVersion: 1 },
        ],
        artifactHashes: frozen1,
      })}</longtask_verdicts>`,
      2,
    ),
  );
  fake.emitSessionEvent(acceptor.sessionId, turnEndEvent(2));
  await fake.waitFor(() => worker.messages.some((m) => m.includes("验收退回")));
  assert.equal((api.getTask(task.id) as { status: string }).status, "rework");
  assert.ok(lastDelivery(worker).includes("[c2]"), "缺项交回负责人");

  // 第三轮：补齐后再次声明做完 → 新验收轮 → 全部通过 → 交付
  fake.emitSessionEvent(worker.sessionId, assistantEvent('已补齐\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 3));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(3));
  const acceptor2 = findAgentByMessage(fake, "独立验收请求")!;
  await fake.waitFor(() => acceptor2.messages.filter((m) => m.includes("独立验收请求")).length >= 2, 2000);
  const round2Request = acceptor2.messages.filter((m) => m.includes("独立验收请求")).pop()!;
  const round2 = /轮次 (round_[a-z0-9]+)/.exec(round2Request)![1]!;
  assert.notEqual(round2, roundId, "退回后新轮次");
  const frozen2 = frozenHashes(round2Request);

  fake.emitSessionEvent(
    acceptor2.sessionId,
    assistantEvent(
      `<longtask_verdicts>${JSON.stringify({
        roundId: round2,
        verdicts: [
          { criterionId: "c1", outcome: "pass", evidence: "比较表已核对", gap: null, verdictBy: REVIEWER.relPath, criteriaVersion: 1 },
          { criterionId: "c2", outcome: "pass", evidence: "推荐及依据已核对", gap: null, verdictBy: REVIEWER.relPath, criteriaVersion: 1 },
        ],
        artifactHashes: frozen2,
      })}</longtask_verdicts>`,
      3,
    ),
  );
  fake.emitSessionEvent(acceptor2.sessionId, turnEndEvent(3));
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "delivered");
  assert.equal((api.getTask(task.id) as { status: string }).status, "delivered");
});

test("apply 零预算：确认不产生执行投递，任务进入预算暂停（P1-7）", async () => {
  const { fake, api } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 0, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const t = api.getTask(task.id) as { status: string; pauseReason: string | null };
  assert.equal(t.status, "paused");
  assert.equal(t.pauseReason, "budget");
  assert.equal(fake.agentsById.size, 0, "零预算不创建会话");
});

test("apply 活动任务取消：运行中会话被 stop（P1-9）", async () => {
  const { fake, api } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  assert.ok(worker && !worker.stopped);

  await api.cancel(task.id);
  await fake.flush();
  assert.ok(worker.stopped, "cancel 停止运行中的 Agent");
  assert.equal((api.getTask(task.id) as { status: string }).status, "cancelled");
});

test("apply pre-step 预算闸门：余额耗尽时拒绝任务会话的新步骤，无关会话放行（P1-7）", async () => {
  const { fake, api } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  assert.ok(await fake.emitPreStep(worker.sessionId), "有余额时放行");
  assert.ok(await fake.emitPreStep("unrelated-session"), "无关会话放行");

  // 掏空预算（模拟并发耗尽）：暂停后验证闸门
  await api.pause(task.id, "用户暂停");
  await fake.flush();
  assert.equal(await fake.emitPreStep(worker.sessionId), false, "暂停后任务会话新步骤被拒");
  assert.equal(await fake.emitPreStep("unrelated-session"), true, "暂停不影响无关会话");
});

test("apply 验收步数上限：验收迟迟不给结论 → 拒绝新步骤并暂停交人工", async () => {
  const { fake, api, dir } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  // 交付物落盘后声明做完 → 进入验收
  await fs.writeFile(join(dir, "d"), "产物内容\n", "utf8");
  fake.emitSessionEvent(worker.sessionId, assistantEvent('完成\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
  await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
  const acceptor = findAgentByMessage(fake, "独立验收请求")!;

  // 默认上限 8 步：8 步放行，第 9 步拒绝并把任务交给人工
  for (let i = 1; i <= 8; i++) assert.equal(await fake.emitPreStep(acceptor.sessionId), true, `第 ${i} 步放行`);
  assert.equal(await fake.emitPreStep(acceptor.sessionId), false, "第 9 步被拒（验收不得无限探查）");
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  const t = api.getTask(task.id) as { status: string; pauseReason: string | null };
  assert.equal(t.status, "paused");
  assert.equal(t.pauseReason, "missing_input", "交还人工而不是反复烧预算");
});

test("apply 验收未给结论：不得静默挂着，必须催办一次并落检查点", async () => {
  const { fake, api, dir } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  await fs.writeFile(join(dir, "d"), "产物内容\n", "utf8");
  fake.emitSessionEvent(worker.sessionId, assistantEvent('完成\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
  await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
  const acceptor = findAgentByMessage(fake, "独立验收请求")!;
  await fake.emitPreStep(acceptor.sessionId);
  const before = acceptor.messages.length;

  // 验收跑了一轮，什么都没裁决就结束
  fake.emitSessionEvent(acceptor.sessionId, assistantUsageEvent(1, {inputTokens: 100, outputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0}, "我先看看产物……然后我们再讨论怎么核对。"));
  fake.emitSessionEvent(acceptor.sessionId, turnEndEvent(1));

  await fake.waitFor(() => acceptor.messages.length > before);
  const nudge = acceptor.messages.at(-1)!;
  assert.ok(nudge.includes("上一轮没有给出结论"), "催办而不是静默等待");
  assert.ok(nudge.includes("<longtask_verdicts>"), "催办里重申结论协议");
  assert.ok(nudge.includes("不要再调用任何工具"), "明确要求直接收敛");
  assert.equal((api.getTask(task.id) as { status: string }).status, "awaiting_acceptance", "催办不改状态");
});

test("apply 执行步数上限：执行者把预算烧在探索上之前就被拦住并交人工", async () => {
  const { fake, api, dir } = await makeApp();
  // 预算给足，确保先撞到步数上限而不是预算上限
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 3_000_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  // 默认上限 24 步：24 步放行，第 25 步拒绝
  for (let i = 1; i <= 24; i++) assert.equal(await fake.emitPreStep(worker.sessionId), true, `第 ${i} 步放行`);
  assert.equal(await fake.emitPreStep(worker.sessionId), false, "第 25 步被拒（执行者探索有界）");
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  const t = api.getTask(task.id) as { status: string; pauseReason: string | null };
  assert.equal(t.pauseReason, "missing_input", "交人工判断，不是靠预算兜底");
  await fs.writeFile(join(dir, "unused"), "", "utf8");
});

test("apply 真实 SDK usage.inputTokens 正确结算且不产生 NaN", async () => {
  const { fake, api, dir } = await makeApp();
  try {
    const task = await api.propose({
      goal: "g",
      criteria: [{ id: "c1", text: "t" }],
      deliverables: ["d"],
      executors: [EXECUTOR],
      acceptor: REVIEWER,
      budget: { limitTokens: 500_000, strict: true },
    });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    await fake.flush();
    fake.emitSessionEvent(worker.sessionId, {
      ...assistantEvent("执行中", 1),
      data: {
        ...assistantEvent("执行中", 1).data,
        usage: { inputTokens: 12480, outputTokens: 423, cacheReadTokens: 8576, cacheWriteTokens: 0, totalTokens: 21479 },
      },
    });
    await fake.flush();
    // 预留落账与结算都是写盘，等结算真正落盘再断言（避免把调度时序当成账本正确性）
    // Windows读句柄会阻止原子rename；先从已提交API等结算，不在写盘期间反复打开state.json。
    await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 21479 && api.getBudget(task.id)?.reservedTokens === 0);
    const state = JSON.parse(await fs.readFile(join(dir, "state.json"), "utf8"));
    const budget = state.collections.budgets[task.id] as { settledTokens: number; reservedTokens: number };
    assert.equal(budget!.settledTokens, 21479);
    assert.ok(Number.isFinite(budget!.reservedTokens));
    assert.equal(budget!.reservedTokens, 0, "结算后不留残余预留（否则下一次准入会被虚高占用）");
  } finally {
    for (const teardown of fake.teardowns) await teardown();
  }
});

// ---------------------------------------------------------------------------
// P1-1：严格预算的准入必须"放行前原子预留"，口径用可推输入上界
// ---------------------------------------------------------------------------

test("P1-1 放行前必须完成预留：准入后账本立刻有在途额度，不是事后记账", async () => {
  const { fake, api } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  // 没有实测链条时：声明基线 12,000 × 1.25 + 输出上限 8,000 = 23,000
  const budget = api.getBudget(task.id) as { reservedTokens: number; settledTokens: number };
  assert.equal(budget.reservedTokens, 23_000, "next() 返回时预留必须已经落账");
  assert.equal(budget.settledTokens, 0, "准入不产生消耗，只占用在途额度");
});

test("P1-1 首次大输入：一进来就要 10 万 Token 的会话被当场拒绝，而不是调用后才发现超限", async () => {
  const { fake, api } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1", text: "t" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 60_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  // 首轮就带进 100,000 字符上下文：固定估算（23,000）会放过去，上界口径（123,000）不会
  assert.equal(await fake.emitPreStep(worker.sessionId, inboundChars(100_000)), false);
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  const t = api.getTask(task.id) as { status: string; pauseReason: string | null };
  assert.equal(t.status, "paused");
  assert.equal(t.pauseReason, "budget");
  const budget = api.getBudget(task.id) as { reservedTokens: number };
  assert.equal(budget.reservedTokens, 0, "被拒的请求不占额度、不产生消耗");
});

test("P1-1 工具结果突增：上界跟着涨，旧口径会放过的请求被拦下", async () => {
  // 本用例隔离输入上界机制；完整验收保护由预算闭环回归覆盖。
  const { fake, api } = await makeApp({ acceptanceReserveTokens: 0 });
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 60_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  // 第一步正常放行 → 实测输入 21,000、输出 1,200 落账
  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { uncachedInputTokens: 21_000, outputTokens: 1_200, cacheReadTokens: 0, cacheWriteTokens: 0 }));
  await fake.waitFor(() => {
    const b = api.getBudget(task.id) as { settledTokens: number };
    return b.settledTokens === 22_200;
  });

  // 某步工具结果暴增 40,000 字符：上界 21,000 + 1,200 + 40,000 = 62,200，加输出上限 70,200
  // 旧口径只有 ceil(21,000 × 1.25) + 8,000 = 34,250，会被放过去然后超限。
  const before = api.getBudget(task.id) as { reservedTokens: number };
  assert.equal(before.reservedTokens, 0, "上一步已结算，无在途占用");
  assert.equal(await fake.emitPreStep(worker.sessionId, inboundChars(40_000)), false);
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  assert.equal((api.getTask(task.id) as { pauseReason: string | null }).pauseReason, "budget");
});

test("SDK surface 工具结果不经过 inbox：空 pre-step.messages 仍须拦下超预算请求", async () => {
  const { fake, api } = await makeApp({ wrapUpAtBudgetRatio: 0, wrapUpMinStepsLeft: 0, acceptanceReserveTokens: 0 });
  try {
    const task = await api.propose({
      goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"],
      executors: [EXECUTOR], acceptor: REVIEWER,
      budget: { limitTokens: 60_000, strict: true },
    });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, {
      inputTokens: 0, cacheReadTokens: 21_000, cacheWriteTokens: 0, outputTokens: 1_200,
    }));
    await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 22_200);

    // SDK appendToolResult 直接写 surface，不进入下一步 inbox；不能靠 messages 捕获。
    fake.emitSessionEvent(worker.sessionId, {
      type: "tool/result", seq: 12,
      data: { turn: 1, message: { role: "tool", id: "result-1", callId: "read-1", content: [{ type: "text", text: "字".repeat(40_000) }] } },
    });
    assert.equal(await fake.emitPreStep(worker.sessionId, []), false);
    await fake.waitFor(() => api.getTask(task.id)?.status === "paused");
    assert.equal(api.getTask(task.id)?.pauseReason, "budget");
    assert.equal(api.getBudget(task.id)?.reservedTokens, 0, "拒绝发生在请求前，不新增在途预留");
    assert.equal(api.getBudget(task.id)?.settledTokens, 22_200, "被拒的步骤不消耗 Token");
  } finally {
    for (const teardown of fake.teardowns) await teardown();
  }
});

test("surface 增量按事件去重、计入完整 UTF-8 消息，在下一次实测输入后归零", async () => {
  const { fake, api } = await makeApp({ wrapUpAtBudgetRatio: 0, wrapUpMinStepsLeft: 0, acceptanceReserveTokens: 0 });
  try {
    const task = await api.propose({
      goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"],
      executors: [EXECUTOR], acceptor: REVIEWER,
      budget: { limitTokens: 500_000, strict: true },
    });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, {
      inputTokens: 21_000, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 1_200,
    }));
    await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 22_200);

    const result = { role: "tool", id: "result-2", callId: "read-2", content: [{ type: "text", text: "中文结果".repeat(100) }] };
    const context = { role: "user", id: "context-1", source: { kind: "runtime-context" }, content: [{ type: "text", text: "运行时上下文" }] };
    const system = { role: "system", id: "system-1", content: [{ type: "text", text: "补充系统说明" }] };
    const developer = { role: "developer", id: "developer-1", content: [{ type: "text", text: "补充开发者说明" }] };
    const toolEvent = { type: "tool/result", seq: 22, data: { turn: 1, message: result } };
    fake.emitSessionEvent(worker.sessionId, toolEvent);
    fake.emitSessionEvent(worker.sessionId, toolEvent); // 事件重放不能重复加算
    fake.emitSessionEvent(worker.sessionId, { type: "user/message", seq: 23, data: context });
    fake.emitSessionEvent(worker.sessionId, { type: "system/message", seq: 24, data: { turn: 1, message: system } });
    fake.emitSessionEvent(worker.sessionId, { type: "developer/message", seq: 25, data: { turn: 1, message: developer } });
    assert.equal(await fake.emitPreStep(worker.sessionId, []), true);
    const addedBytes = [result, context, system, developer].reduce((total, message) => total + Buffer.byteLength(JSON.stringify(message), "utf8"), 0);
    assert.equal(api.getBudget(task.id)?.reservedTokens, 21_000 + 1_200 + addedBytes + 8_000);

    fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, {
      inputTokens: 23_000, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 1_000,
    }));
    await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 46_200 && api.getBudget(task.id)?.reservedTokens === 0);
    fake.emitSessionEvent(worker.sessionId, toolEvent); // 已纳入实测基线的事件重放也不得重算
    assert.equal(await fake.emitPreStep(worker.sessionId, []), true);
    assert.equal(api.getBudget(task.id)?.reservedTokens, 23_000 + 1_000 + 8_000, "实测输入已包含旧结果，不再叠加");
  } finally {
    for (const teardown of fake.teardowns) await teardown();
  }
});

test("waterfall 末端追加的 runtime-context 在本步放行前计入预算", async () => {
  const { fake, api } = await makeApp({ wrapUpAtBudgetRatio: 0, wrapUpMinStepsLeft: 0, acceptanceReserveTokens: 0 });
  try {
    const task = await api.propose({
      goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"],
      executors: [EXECUTOR], acceptor: REVIEWER,
      budget: { limitTokens: 60_000, strict: true },
    });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    const context = { role: "user", id: "runtime-1", source: { kind: "runtime-context" }, content: [{ type: "text", text: "运行时资料".repeat(10_000) }] };
    assert.equal(await fake.emitPreStep(worker.sessionId, [], [context]), false, "原始 inbox 为空，下游拼入的大上下文也不能漏算");
    await fake.waitFor(() => worker.stopped);
    assert.equal(api.getTask(task.id)?.pauseReason, "budget");
    assert.equal(api.getBudget(task.id)?.reservedTokens, 0);
  } finally {
    for (const teardown of fake.teardowns) await teardown();
  }
});

test("P1-1 未声明基线时严格模式拒绝调用并公布宿主边界", async () => {
  const { fake, api, dir } = await makeApp({ preStepInputEstimateTokens: 0 });
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  assert.equal(await fake.emitPreStep(worker.sessionId), false, "拿不到可信上界就不调用");
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  const t = api.getTask(task.id) as { status: string; pauseReason: string | null };
  assert.equal(t.pauseReason, "budget");
  // pause 在状态落盘后还会更新待投递记录；等停止完成再读文件，避免 Windows 读句柄与 rename 竞争。
  await fake.waitFor(() => worker.stopped);
  const persisted = await fs.readFile(join(dir, "state.json"), "utf8");
  assert.ok(persisted.includes("agent/pre-step"), "暂停原因里说明宿主边界（只给本步新增消息）");
  assert.ok(persisted.includes("严格模式拒绝调用"), "明确是严格模式的拒绝，而不是笼统的余额不足");
});

test("P1-1 并发/重试抢占：预检都过、预留只有一个成功，失败方被拒并暂停", async () => {
  const { fake, api } = await makeApp({ acceptanceReserveTokens: 0 });
  // 额度够一步（23,000）但不够两步
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 30_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  // 两次准入同时做预检（都会看到 30,000 可用），但原子预留只能成功一次
  const results = await Promise.all([
    fake.emitPreStep(worker.sessionId),
    fake.emitPreStep(worker.sessionId),
  ]);
  assert.equal(results.filter(Boolean).length, 1, "并发准入只允许一个通过");
  const budget = api.getBudget(task.id) as { reservedTokens: number; limitTokens: number };
  assert.equal(budget.reservedTokens, 23_000, "只落下一笔预留，不重复占用");
  assert.ok(budget.reservedTokens <= budget.limitTokens);
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  assert.equal((api.getTask(task.id) as { pauseReason: string | null }).pauseReason, "budget");
});

// ---------------------------------------------------------------------------
// P2-9：验收只读要么真的落到工具层，要么如实降级表述
// ---------------------------------------------------------------------------

test("P2-9 配置了验收能力集：验收会话按只读能力集创建，并可声称工具层拦截", async () => {
  const { fake, api, dir } = await makeApp({ acceptorPreset: "read-only" });
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  assert.equal(fake.mountedPresets.get(worker.sessionId), "standard", "执行者仍是完整能力集");

  await fs.writeFile(join(dir, "d"), "产物内容\n", "utf8");
  fake.emitSessionEvent(worker.sessionId, assistantEvent('完成\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
  await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
  const acceptor = findAgentByMessage(fake, "独立验收请求")!;
  assert.equal(fake.mountedPresets.get(acceptor.sessionId), "read-only", "验收会话挂的是只读能力集");
  assert.ok(lastDelivery(acceptor).includes("已挂载只读能力集"), "确实生效后才敢这么写");
});

test("P2-9 只读能力集挂不上：降级为无预设并如实说明没有技术拦截", async () => {
  const { fake, api, dir } = await makeApp({ acceptorPreset: "read-only" }, { failPresets: ["read-only"] });
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  await fs.writeFile(join(dir, "d"), "产物内容\n", "utf8");
  fake.emitSessionEvent(worker.sessionId, assistantEvent('完成\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
  await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
  const acceptor = findAgentByMessage(fake, "独立验收请求")!;

  const req = lastDelivery(acceptor);
  assert.ok(req.includes("没有工具级只读限制"), "挂不上就不许声称技术拦截");
  assert.ok(!req.includes("已挂载只读能力集"));
});

test("P1-1 实测输入必须吃下 SDK 的 inputTokens 与缓存分项：下一步准入上界不能因此低估", async () => {
  const { fake, api } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  const budgetOf = () => api.getBudget(task.id) as { reservedTokens: number; settledTokens: number };
  const waitSettled = async (total: number) => {
    await fake.waitFor(() => budgetOf().settledTokens === total && budgetOf().reservedTokens === 0);
  };

  // 第一步：声明基线准入（23,000）
  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  assert.equal(budgetOf().reservedTokens, 23_000);

  // 只报 inputTokens 的真实 SDK 用量 → 实测输入应为 21,000（旧实现会算成 0）
  fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 21_000, outputTokens: 1_200, cacheReadTokens: 0, cacheWriteTokens: 0 }));
  await waitSettled(22_200);

  // 下一步准入上界 = 21,000 + 1,200 + 8,000 = 30,200（旧实现只会预留 1,200 + 8,000）
  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  assert.equal(budgetOf().reservedTokens, 30_200, "inputTokens 计入实测链条");

  // 带缓存读的用量：总输入 = 12,480 + 8,576 = 21,056
  fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(2, { inputTokens: 12_480, outputTokens: 423, cacheReadTokens: 8_576, cacheWriteTokens: 0 }));
  await waitSettled(22_200 + 21_479);

  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  assert.equal(budgetOf().reservedTokens, 21_056 + 423 + 8_000, "缓存分项计入实测输入，上界不因口径漏算而低估");
});

test("轮次内累积文本：报告在中间步骤给出、后面再补收尾说明也不丢（本轮联调形态）", async () => {
  const { fake, api, dir } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  await fs.writeFile(join(dir, "d"), "产物内容\n", "utf8");

  // 同一 turn 的两条助手消息：第一条给出结论，第二条只是收尾说明（会覆盖"最后一条"）
  fake.emitSessionEvent(worker.sessionId, assistantEvent('已完成\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(worker.sessionId, assistantEvent("以上为交付说明，正文见文件。", 1));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));

  await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
  assert.equal((api.getTask(task.id) as { status: string }).status, "awaiting_acceptance", "整轮文本里的报告仍然生效");
});

test("P1-1 新验收会话不得继承执行者的实测输入：否则 2.4 万的准入会被算成 6 万", async () => {
  const { fake, api, dir } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;

  // 执行者先跑出 5 万的实测输入
  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 50_000, outputTokens: 1_000, cacheReadTokens: 0, cacheWriteTokens: 0 }));
  await fake.waitFor(() => (api.getBudget(task.id) as { settledTokens: number }).settledTokens === 51_000);

  // 提交验收 → 全新的验收会话
  await fs.writeFile(join(dir, "d"), "产物内容\n", "utf8");
  fake.emitSessionEvent(worker.sessionId, assistantEvent('完成\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
  await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
  const acceptor = findAgentByMessage(fake, "独立验收请求")!;
  const acceptReq = lastDelivery(acceptor);

  const acceptMessage = { role: "user", content: [{ type: "text", text: acceptReq }] };
  assert.equal(await fake.emitPreStep(acceptor.sessionId, [acceptMessage]), true);
  // 新会话按声明基线与完整消息字节数准入；绝不能把执行者的 5 万算进来。
  const expected = 15_000 + Buffer.byteLength(JSON.stringify(acceptMessage), "utf8") + 8_000;
  const budget = api.getBudget(task.id) as { reservedTokens: number };
  assert.equal(budget.reservedTokens, expected, "验收会话只按自己的链条/声明基线准入");
  assert.ok(budget.reservedTokens < expected + 40_000, "不得继承执行者的上下文体量");
});

test("预算收尾使用 steer：当前 turn 的下一步即可消费，初始简报仍走 followup", async () => {
  const { fake, api } = await makeApp();
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 100_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  assert.equal(worker.nextTurn.length, 1, "初始简报进入 next-turn");
  assert.equal(worker.consumeStep(true).length, 1, "模拟当前 turn 开始并消费初始简报");

  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  // 一笔结算到 70%（阈值 65%）
  fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 68_000, outputTokens: 2_000, cacheReadTokens: 0, cacheWriteTokens: 0 }));
  await fake.waitFor(() => worker.messages.some((m) => m.includes("预算收尾")));
  const wrap = worker.messages.at(-1)!;
  assert.ok(wrap.includes("停止读取新文件"), "明确要求停止探索");
  assert.ok(wrap.includes("就地修改"), "已有产物不整篇重写");
  assert.ok(wrap.includes("<longtask_report>"), "要求给出报告块");
  assert.equal(worker.nextTurn.length, 0, "收尾不能再排到下个 turn");
  assert.deepEqual(worker.consumeStep(), [wrap], "未发出 turn/end 前，下一步就能读到收尾提醒");

  // 收尾指令本身也走准入：它有在途预留，下一步是否够用由预算决定（这里是 7,000，不够一步）
  assert.equal(await fake.emitPreStep(worker.sessionId), false, "收尾之后额度已不足以再走一步");
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  assert.equal((api.getTask(task.id) as { pauseReason: string | null }).pauseReason, "budget");
});

test("阶段一委派闸门在工具正文前阻止五路并发 subagent，不影响普通工具或无关会话", async () => {
  const { fake, api, dir } = await makeApp();
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 350_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    let starts = 0;
    const attempts = await Promise.all(Array.from({ length: 5 }, () => fake.emitTool(worker.sessionId, "subagent", () => { starts++; })));
    assert.equal(starts, 0, "子会话创建正文不得执行，不能靠事后补账防超支");
    assert.ok(attempts.every((r) => r.denied));
    for (const name of ["spawn_teammate", "workflow", "ralph", "send_message"]) {
      assert.equal((await fake.emitTool(worker.sessionId, name, () => { starts++; })).denied, true, name);
    }
    assert.equal((await fake.emitTool(worker.sessionId, "write", () => { starts++; })).denied, false);
    assert.equal((await fake.emitTool("ordinary-user-session", "subagent", () => { starts++; })).denied, false);
    assert.equal(starts, 2);
    assert.equal(api.getBudget(task.id)?.settledTokens, 0);
    assert.equal(api.getBudget(task.id)?.reservedTokens, 0);
    await fs.writeFile(join(dir, "d"), "交付物");
    fake.emitSessionEvent(worker.sessionId, assistantEvent('<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>', 1));
    fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
    await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
    const acceptor = findAgentByMessage(fake, "独立验收请求")!;
    assert.equal((await fake.emitTool(acceptor.sessionId, "subagent", () => { starts++; })).denied, true, "验收人也不能把费用转移到子会话");
    await api.cancel(task.id);
  } finally {
    for (const teardown of fake.teardowns) await teardown();
    assert.equal(fake.toolGuards.size, 0, "卸载撤销全局 guard");
  }
});

test("步数上限联动：预算充裕时也应在最后两步前发收尾，而非撞到上限直接暂停", async () => {
  const { fake, api } = await makeApp({ executorMaxSteps: 5, wrapUpAtBudgetRatio: 0, wrapUpMinStepsLeft: 2 });
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 1_000_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    worker.consumeStep(true);
    for (let i = 1; i <= 3; i++) {
      assert.equal(await fake.emitPreStep(worker.sessionId), true);
      fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 10_000, outputTokens: 1_000, cacheReadTokens: 0, cacheWriteTokens: 0 }));
      await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 11_000 * i);
    }
    await fake.waitFor(() => worker.nextStep.some((m) => m.includes("预算收尾")) || JSON.stringify(fake.stepMessages.get(worker.sessionId)).includes("预算收尾"));
    assert.equal(api.getTask(task.id)?.status, "active");
    assert.equal(worker.nextTurn.length, 0);
  } finally { for (const teardown of fake.teardowns) await teardown(); }
});

test("结算与下一步同步：usage 后立即申请下一步，收尾须进入本次提案而非迟到下一步", async () => {
  const { fake, api } = await makeApp({ wrapUpAtBudgetRatio: 0.01, wrapUpMinStepsLeft: 0 });
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 1_000_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 10_000, outputTokens: 1_000, cacheReadTokens: 0, cacheWriteTokens: 0 }));
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    assert.ok(JSON.stringify(fake.stepMessages.get(worker.sessionId)).includes("预算收尾"), "不能让异步结算/投递落后于本次 inbox.claim");
    assert.equal(worker.nextStep.length, 0, "当前提案已经包含收尾，不得重复排队");
  } finally { for (const teardown of fake.teardowns) await teardown(); }
});

test("turn/end 关闭控制窗口：异步结算不得唤醒已结束轮次，正常续轮再接收收尾", async () => {
  const { fake, api, dir } = await makeApp({ preStepInputEstimateTokens: 50_000, wrapUpAtBudgetRatio: 0.05, wrapUpMinStepsLeft: 0 });
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 1_000_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    worker.consumeStep(true);
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 50_000, outputTokens: 1000, cacheReadTokens: 0, cacheWriteTokens: 0 }, '<longtask_report>{"done":false,"next":["补齐产物"],"waiting":null}</longtask_report>'));
    fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
    await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 51_000);
    await fake.waitFor(() => worker.nextTurn.length > 0);
    let processed = false;
    for (let i = 0; i < 100; i++) {
      if ((await fs.readFile(join(dir, "plugin.log"), "utf8")).includes(`usage:processed session=${worker.sessionId}`)) { processed = true; break; }
      await fake.flush();
    }
    assert.ok(processed, "等待结算及收尾投递全部处理完成，再检查是否迟到唤醒");
    assert.equal(worker.messages.some((m) => m.includes("预算收尾")), false, "结算到达时turn已结束，不能用steer额外唤醒");
    assert.equal(worker.nextStep.length, 0);
    fake.emitSessionEvent(worker.sessionId, { type: "turn/start", data: { turn: 2 } });
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    assert.ok(JSON.stringify(fake.stepMessages.get(worker.sessionId)).includes("预算收尾"), "正常续轮打开窗口后，仍能按预算收尾");
  } finally { for (const teardown of fake.teardowns) await teardown(); }
});

test("收尾按下一步上界和验收保留规划：26万预算不必等到65%才开始收尾", async () => {
  const { fake, api } = await makeApp({ preStepInputEstimateTokens: 22_000, wrapUpAtBudgetRatio: 0, wrapUpMinStepsLeft: 3 });
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 260_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    let currentStepControl = false;
    for (let i = 1; i <= 4; i++) {
      assert.equal(await fake.emitPreStep(worker.sessionId), true);
      currentStepControl ||= JSON.stringify(fake.stepMessages.get(worker.sessionId)).includes("预算收尾");
      fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 26_000, outputTokens: 500, cacheReadTokens: 0, cacheWriteTokens: 0 }));
      await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 26_500 * i);
    }
    await fake.waitFor(() => currentStepControl || worker.messages.some((m) => m.includes("预算收尾")));
    assert.equal(api.getBudget(task.id)?.settledTokens, 106_000);
    assert.equal(api.getTask(task.id)?.status, "active");
  } finally { for (const teardown of fake.teardowns) await teardown(); }
});

test("真实 SDK ToolRuntime 执行管线：allow 策略无法覆盖 longtask guard，五路委派正文均不执行", async () => {
  const { fake, api } = await makeApp();
  const { Context } = await import("@deepseek-ai/cordis");
  const { ToolRuntime, defineTool } = await loadSdk("dsh-tools");
  const root = new Context();
  root.provide("systemPrompt", { tools() {} });
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 350_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    const runtime = await prepareSdkTools(root,new ToolRuntime(root));
    let bodies = 0;
    runtime.register(defineTool({
      name: "subagent", parameters: {}, output: { schema: { type: "json" }, render: () => [{ type: "text", text: "started" }] },
      execute: async () => { bodies++; return {}; },
    }));
    assert.equal(fake.toolGuards.size, 1, "实际 apply 必须注册单调 guard");
    runtime.guard([...fake.toolGuards][0]);
    (root as unknown as HostContext).on("tools/pre-execute", async () => ({ kind: "allow" as const }));
    const results = await Promise.all(Array.from({ length: 5 }, (_, i) => runtime.execute({
      name: "subagent", callId: `spawn-${i}`, arguments: {}, agent: worker, signal: new AbortController().signal,
    })));
    assert.equal(bodies, 0);
    assert.ok(results.every((r: { isError?: boolean }) => r.isError));
    const unrelated = await runtime.execute({ name: "subagent", callId: "normal-user", arguments: {}, agent: { id: "user", session: { id: "user" } }, signal: new AbortController().signal });
    assert.equal(unrelated.isError, false);
    assert.equal(bodies, 1, "普通用户会话仍可正常委派");
  } finally {
    await root.fiber.dispose();
    for (const teardown of fake.teardowns) await teardown();
  }
});

test("未托管后代的首步直接拒绝，不能以无关会话放行；暂停及取消后仍不可启动", async () => {
  const { fake, api } = await makeApp();
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 350_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    fake.addUnmanagedAgent("unmanaged-child", worker.sessionId);
    fake.addUnmanagedAgent("unmanaged-grandchild", "unmanaged-child");
    assert.equal(await fake.emitPreStep("unmanaged-grandchild"), false);
    await fake.waitFor(() => api.getTask(task.id)?.status === "paused");
    assert.equal(api.getBudget(task.id)?.reservedTokens, 0);
    assert.equal(await fake.emitPreStep("unmanaged-child"), false);
    assert.equal((await fake.emitTool("unmanaged-grandchild", "read", () => { throw Error("不得执行"); })).denied, true);
    await api.cancel(task.id);
    assert.equal(await fake.emitPreStep("unmanaged-grandchild"), false);
    assert.equal(api.getTask(task.id)?.status, "cancelled");
    fake.addUnmanagedAgent("unrelated-child", "ordinary-user-session");
    assert.equal(await fake.emitPreStep("unrelated-child"), true, "不拦截用户正常的子会话");
  } finally { for (const teardown of fake.teardowns) await teardown(); }
});

test("宿主没有单调工具 guard 时拒绝模型步骤，避免提示词级禁令冒充技术拦截", async () => {
  const { fake, api } = await makeApp({}, { noToolGuard: true });
  try {
    const task = await api.propose({ goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 350_000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    assert.equal(await fake.emitPreStep(worker.sessionId), false);
    await fake.waitFor(() => worker.stopped);
    assert.equal(api.getTask(task.id)?.pauseReason, "budget");
    assert.equal(api.getBudget(task.id)?.reservedTokens, 0);
  } finally { for (const teardown of fake.teardowns) await teardown(); }
});

test("读预算超限使用 steer：工具仍在当前 turn 时，下一步能消费停止探索指令", async () => {
  const { fake, api } = await makeApp({ toolReadBudget: 1, wrapUpAtBudgetRatio: 0, wrapUpMinStepsLeft: 0 });
  try {
    const task = await api.propose({
      goal: "g", criteria: [{ id: "c1" }], deliverables: ["d"],
      executors: [EXECUTOR], acceptor: REVIEWER,
      budget: { limitTokens: 500_000, strict: true },
    });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    assert.equal(worker.consumeStep(true).length, 1);
    fake.emitSessionEvent(worker.sessionId, { type: "tool/call", data: { turn: 1, name: "read_file" } });
    assert.deepEqual(worker.consumeStep(), [], "限额内不干预");
    fake.emitSessionEvent(worker.sessionId, { type: "tool/call", data: { turn: 1, name: "read_file" } });
    await fake.waitFor(() => worker.messages.length === 2);
    const [control] = worker.consumeStep();
    assert.ok(control?.includes("读预算"));
    assert.ok(control?.includes("<longtask_report>"));
    assert.equal(worker.nextTurn.length, 0, "不等待当前 turn 结束");
  } finally {
    for (const teardown of fake.teardowns) await teardown();
  }
});

test("执行者准入为验收保留额度：只够本步、不够验收时提前停下，而不是把最后一分钱花光", async () => {
  // 关掉收尾指令，单独验证"验收额度保留"这道闸
  const { fake, api } = await makeApp({ wrapUpAtBudgetRatio: 0, wrapUpMinStepsLeft: 0 });
  const task = (await api.propose({
    goal: "g",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 620_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  const settled = () => (api.getBudget(task.id) as { settledTokens: number }).settledTokens;

  // 两步各实耗 20 万：上下文 20 万，下一步就要 20.8 万
  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 200_000, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }));
  await fake.waitFor(() => settled() === 200_000);
  assert.equal(await fake.emitPreStep(worker.sessionId), true);
  fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(2, { inputTokens: 200_000, outputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 }));
  await fake.waitFor(() => settled() === 400_000);

  // 可用 220,000：够本步（208,000），但不够"本步 + 验收预留 26,000" → 必须提前停
  assert.equal(await fake.emitPreStep(worker.sessionId), false);
  await fake.waitFor(() => (api.getTask(task.id) as { status: string }).status === "paused");
  assert.equal((api.getTask(task.id) as { pauseReason: string | null }).pauseReason, "budget");
});

test("联调覆盖判定：wrapup/gap-inject 缺少目标证据不得通过，漏记子会话不得算预算内成功", async () => {
  const { assessScenario } = await import(new URL("../../../dsh-longtask-e2e/evidence.mjs", import.meta.url).href);
  const base = {
    taskId: "test-task", runId: "test-run", scenario: "basic", status: "delivered",
    budget: { limitTokens: 1000, settledTokens: 800, reservedTokens: 0 },
    accounting: { verified: true, actualTokens: 800, descendants: [] },
    controls: { wrapUp: 0, wrapUpConsumed: 0, readStop: 0, readStopConsumed: 0, boundExceeded: 0 },
    rounds: [{ roundId: "round-two", outcome: "delivered", verdicts: ["c1:pass", "c2:pass"] }],
  };
  assert.equal(assessScenario(base).passed, true);
  assert.equal(assessScenario({ ...base, scenario: "wrapup" }).passed, false);
  const fixture = { version: "checkpoint-v2", pendingAtControl: true, receiptIssued: true, controlOrder: 2, receiptOrder: 3, artifactMatches: true, receiptFileMatches: true };
  const wrap = { ...base, scenario: "wrapup", fixture, controls: { ...base.controls, wrapUp: 1, wrapUpConsumed: 1 } };
  assert.equal(assessScenario(wrap).passed, true);
  assert.equal(assessScenario({ ...wrap, controls: { ...wrap.controls, wrapUpConsumed: 0 } }).passed, false, "只入队不算消费");
  assert.equal(assessScenario({ ...wrap, fixture: undefined }).passed, false, "旧场景即使消费过也不能替代检查点闭环");
  for (const change of [{ pendingAtControl: false }, { receiptOrder: 1 }, { artifactMatches: false }, { receiptFileMatches: false }]) {
    assert.equal(assessScenario({ ...wrap, fixture: { ...fixture, ...change } }).passed, false);
  }
  const fault = { version: "gap-before-freeze-v2", taskId: base.taskId, runId: base.runId, injected: true, repaired: true, artifactMatches: true, criteriaUnchanged: true,
    phases: { injected: 1, firstFrozen: 2, gap: 3, repaired: 4, secondFrozen: 5 }, firstRoundId: "round-one", secondRoundId: "round-two", beforeHash: "a".repeat(64), injectedHash: "b".repeat(64), repairedHash: "c".repeat(64), failure: null };
  const gap = { ...base, scenario: "gap-inject", fault };
  assert.equal(assessScenario(gap).passed, false, "首次全pass不是退回证据");
  const rework = { roundId: "round-one", outcome: "rework", verdicts: ["c1:pass", "c2:gap"] };
  assert.equal(assessScenario({ ...gap, rounds: [rework, ...base.rounds] }).passed, true);
  assert.equal(assessScenario({ ...gap, rounds: [...base.rounds, rework] }).passed, false, "不能颠倒退回与交付顺序");
  assert.equal(assessScenario({ ...gap, rounds: [rework, ...base.rounds], fault: { ...gap.fault, repaired: false } }).passed, false);
  for (const change of [{ version: "old-missing-input" }, { taskId: "other" }, { criteriaUnchanged: false }, { injectedHash: fault.beforeHash }, { secondRoundId: "round-one" }, { phases: { ...fault.phases, firstFrozen: 0 } }]) {
    assert.equal(assessScenario({ ...gap, rounds: [rework, ...base.rounds], fault: { ...fault, ...change } }).passed, false);
  }
  assert.equal(assessScenario({ ...base, accounting: { verified: false, actualTokens: 800, descendants: ["child"] } }).passed, false);
  assert.equal(assessScenario({ ...base, accounting: { verified: true, actualTokens: 900, descendants: [] } }).passed, false);
  assert.equal(assessScenario({ ...base, budget: { ...base.budget, settledTokens: 1001 } }).passed, false);
});

test("联调真实 apply 入口：wrapup 未触发仍结束运行，但只写 finished、不能写 done", async () => {
  const { apply: applyE2e } = await import(new URL("../../../dsh-longtask-e2e/index.js", import.meta.url).href);
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-e2e-coverage-"));
  const transcriptDir = join(dir, "e2e");
  await fs.mkdir(transcriptDir);
  const id = "coverage-fixture";
  await fs.writeFile(join(dir, "plugin.log"), `budget:admitted task=${id}\n`);
  await fs.writeFile(join(dir, "state.json"), JSON.stringify({ collections: { taskSessions: { [id]: { worker: { sessionId: "worker" }, acceptor: { sessionId: "acceptor" } } } }, events: {}, eventOrder: [] }));
  let status = "proposed";
  const task = () => ({ id, status, versions: [{ goal: "[E2E] coverage" }], currentVersion: 0 });
  const api = {
    searchExperts: async (division: string) => [division === "engineering" ? EXECUTOR : REVIEWER],
    listTasks: () => [], propose: async () => task(), confirm: async () => { status = "delivered"; },
    getTask: () => task(), getBudget: () => ({ limitTokens: 200, settledTokens: 100, reservedTokens: 0 }),
    registerArtifact: async () => {}, pause: async () => {},
  };
  const teardowns: Array<() => unknown> = [];
  const services: Record<string, unknown> = {
    sessions: { flush: async () => true },
    longtask: api,
    tools: { register: () => () => {}, guard: () => () => {} },
    subagents: { listDescendants: async () => [] },
    agents: { get: (sid: string) => ({ session: { id: sid } }) },
    sessionProjections: { stateOf: () => ({ totals: { uncachedInputTokens: 30, cacheReadTokens: 10, cacheWriteTokens: 0, outputTokens: 10 } }) },
  };
  applyE2e({
    get: (name: string) => services[name], logger: { info() {} },
    on: () => () => {},
    effect(factory: () => Generator) { const value = factory().next().value; if (typeof value === "function") teardowns.push(value as () => unknown); },
  }, { transcriptDir, artifactDir: join(dir, "artifacts"), scenario: "wrapup", force: true, pollIntervalMs: 1, timeoutMs: 1000 });
  try {
    const deadline = Date.now() + 4000;
    while (!(await fs.readdir(transcriptDir)).some((name) => name.startsWith("summary-"))) {
      if (Date.now() > deadline) throw Error("联调脚手架未在预期时间结束");
      await new Promise((r) => setTimeout(r, 10));
    }
    const names = await fs.readdir(transcriptDir);
    assert.ok(names.some((name) => name.startsWith("finished-")), "记录结束，避免下次重载自动重复消耗");
    assert.equal(names.some((name) => name.startsWith("done-")), false, "delivered 不等于收尾验证通过");
    const run = await fs.readFile(join(transcriptDir, names.find((name) => name.endsWith(".jsonl"))!), "utf8");
    assert.ok(run.includes("scenario_not_covered"));
    assert.ok(run.includes('"verified":true'), "本用例宿主对账通过，失败点确实是收尾未触发");
  } finally { for (const teardown of teardowns) await teardown(); }
});

async function harnessFixture(options: { manual?: boolean; timeoutMs?: number } = {}) {
  const { apply: applyE2e } = await import(new URL("../../../dsh-longtask-e2e/index.js", import.meta.url).href);
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-e2e-restart-"));
  const transcriptDir = join(dir, "e2e");
  await fs.mkdir(transcriptDir);
  await fs.writeFile(join(transcriptDir, "finished-old.json"), "{}");
  await fs.writeFile(join(dir, "plugin.log"), "");
  const tasks = new Map<string, { id: string; status: string; currentVersion: number; versions: Array<{ goal: string }> }>();
  const leases: Record<string, unknown> = {};
  const limits = new Map<string, number>();
  let calls = 0;
  const proposals: Array<{ goal: string; deliverables: string[] }> = [];
  const registered: Array<{ id: string; path: string; hash: string; owner: string }> = [];
  const tools = new Map<string, any>();
  const eventListeners = new Set<(...args: any[]) => void>();
  const api = {
    searchExperts: async (division: string) => [division === "engineering" ? EXECUTOR : REVIEWER],
    listTasks: () => [...tasks.values()],
    propose: async (input: { goal: string; deliverables: string[]; budget: { limitTokens: number } }) => {
      proposals.push(input);
      const id = `explicit-${++calls}`;
      const t = { id, status: "proposed", currentVersion: 0, versions: [{ goal: input.goal }] };
      tasks.set(id, t); limits.set(id, input.budget.limitTokens);
      leases[id] = { worker: { sessionId: `${id}-worker`, role: "executor" }, acceptor: { sessionId: `${id}-acceptor`, role: "acceptor" } };
      await fs.writeFile(join(dir, "state.json"), JSON.stringify({ collections: { taskSessions: leases }, events: {}, eventOrder: [] }));
      return t;
    },
    confirm: async (id: string) => { tasks.get(id)!.status = options.manual ? "active" : "delivered"; },
    getTask: (id: string) => tasks.get(id),
    getBudget: (id: string) => ({ limitTokens: limits.get(id), settledTokens: 100, reservedTokens: 0 }),
    registerArtifact: async (id: string, path: string, hash: string, owner: string) => { registered.push({ id, path, hash, owner }); },
    pause: async (id: string) => { tasks.get(id)!.status = "paused"; },
  };
  const teardowns: Array<() => unknown> = [];
  const services: Record<string, unknown> = {
    sessions: { flush: async () => true },
    longtask: api, subagents: { listDescendants: async () => [] },
    tools: { register: (tool: any) => { tools.set(tool.name, tool); return () => tools.delete(tool.name); }, guard: () => () => {} },
    agents: { get: (id: string) => ({ session: { id } }) },
    sessionProjections: { stateOf: () => ({ totals: { uncachedInputTokens: 30, cacheReadTokens: 10, cacheWriteTokens: 0, outputTokens: 10 } }) },
  };
  applyE2e({ get: (name: string) => services[name], on: (_name: string, fn: (...args: any[]) => void) => { eventListeners.add(fn); return () => eventListeners.delete(fn); }, logger: { info() {} }, effect(factory: () => Generator) {
    const value = factory().next().value; if (typeof value === "function") teardowns.push(value as () => unknown);
  } }, { transcriptDir, artifactDir: join(dir, "artifacts"), pollIntervalMs: 1, timeoutMs: options.timeoutMs ?? (options.manual ? 5000 : 1000) });
  const wait = async (predicate: () => Promise<boolean>) => {
    const deadline = Date.now() + 4000;
    while (!await predicate()) { if (Date.now() > deadline) throw Error("显式运行请求未被接收"); await new Promise((r) => setTimeout(r, 10)); }
  };
  return { dir, transcriptDir, proposals, registered, tools, eventListeners, tasks,
    emit: (sid: string, event: unknown) => { for (const fn of eventListeners) fn({ id: sid }, event); },
    calls: () => calls, wait, close: async () => { for (const teardown of teardowns) await teardown(); } };
}

test("收尾夹具消费者 apply 完整握手：真任务归属、先登记回执、校对产物、结束后清理", async () => {
  const h = await harnessFixture({ manual: true });
  const { publishRunRequest } = await import(new URL("../../../dsh-longtask-e2e/request-run.mjs", import.meta.url).href);
  try {
    publishRunRequest({ scenario: "wrapup", budgetTokens: 260000 }, h.transcriptDir);
    await h.wait(async () => h.tasks.get("explicit-1")?.status === "active");
    const runId = /runId=([a-f0-9-]+)/.exec(h.proposals[0]!.goal)![1];
    const tool = h.tools.get("e2e_wrapup_checkpoint");
    assert.ok(tool, "真实消费者入口装配检查点工具");
    const exec = { agent: { session: { id: "explicit-1-worker" } }, signal: new AbortController().signal };
    h.emit("explicit-1-worker", { type: "step/start", data: { turn: 1, step: 1 } });
    assert.equal(JSON.parse(await tool.execute({ runId, action: "advance", cursor: 0 }, exec)).checked, 1);
    h.emit("explicit-1-worker", { type: "step/start", data: { turn: 1, step: 2 } });
    h.emit("explicit-1-worker", { type: "user/message", data: { source: { kind: "longtask", dedupeKey: "wrap-up:explicit-1:v1" } } });
    const receipt = JSON.parse(await tool.execute({ runId, action: "close", cursor: 1 }, exec)).receipt;
    assert.equal(h.registered.length, 1, "返回回执前已经通过公开 API 登记");
    assert.equal(h.registered[0]!.owner, "harness");
    const rawReceipt = await fs.readFile(h.registered[0]!.path);
    assert.equal(createHash("sha256").update(rawReceipt).digest("hex"), h.registered[0]!.hash);
    await fs.writeFile(h.proposals[0]!.deliverables[0]!, `本地模拟产物\n${receipt}\n`);
    await fs.writeFile(join(h.dir, "plugin.log"), "budget:wrap-up task=explicit-1\ncontrol:consumed task=explicit-1 kind=wrap-up\n");
    h.tasks.get("explicit-1")!.status = "delivered";
    await h.wait(async () => (await fs.readdir(h.transcriptDir)).some((n) => n.startsWith("done-")));
    const summary = (await fs.readdir(h.transcriptDir)).find((n) => n.startsWith("summary-"))!;
    assert.match(await fs.readFile(join(h.transcriptDir, summary), "utf8"), /场景验证：通过/);
    assert.equal(h.tools.size, 0, "任务结束立即注销测试工具");
    assert.equal(h.eventListeners.size, 0, "不留下其他任务的事件监听器");
  } finally { await h.close(); }
});

test("收尾夹具消费者超时：暂停执行、不发回执、不自动重试", async () => {
  const h = await harnessFixture({ manual: true, timeoutMs: 10 });
  const { publishRunRequest } = await import(new URL("../../../dsh-longtask-e2e/request-run.mjs", import.meta.url).href);
  try {
    publishRunRequest({ scenario: "wrapup", budgetTokens: 260000 }, h.transcriptDir);
    await h.wait(async () => (await fs.readdir(h.transcriptDir)).some((n) => n.startsWith("finished-") && n !== "finished-old.json"));
    assert.equal(h.tasks.get("explicit-1")!.status, "paused");
    assert.equal(h.calls(), 1);
    assert.equal(h.registered.length, 0);
    assert.equal(h.tools.size, 0);
    assert.equal((await fs.readdir(h.transcriptDir)).some((n) => n.startsWith("done-")), false);
  } finally { await h.close(); }
});

test("收尾夹具真实 apply：两轮独立路径且不改旧产物；目标声明检查点和固定预算", async () => {
  const h = await harnessFixture();
  const { publishRunRequest } = await import(new URL("../../../dsh-longtask-e2e/request-run.mjs", import.meta.url).href);
  const old = join(h.transcriptDir, "..", "artifacts", "wrapup.md");
  await fs.mkdir(join(old, ".."), { recursive: true });
  await fs.writeFile(old, "历史冻结产物");
  try {
    const summaries = async () => (await fs.readdir(h.transcriptDir)).filter((n) => n.startsWith("summary-"));
    for (let i = 1; i <= 2; i++) {
      publishRunRequest({ scenario: "wrapup", budgetTokens: 260000 }, h.transcriptDir);
      await h.wait(async () => (await summaries()).length === i);
    }
    assert.notEqual(h.proposals[0]!.deliverables[0], h.proposals[1]!.deliverables[0]);
    assert.equal(await fs.readFile(old, "utf8"), "历史冻结产物");
    for (const p of h.proposals) {
      assert.match(p.goal, /checkpoint-v2/);
      assert.match(p.goal, /e2e_wrapup_checkpoint/);
      assert.match(p.goal, /action=close/);
      assert.ok(Buffer.byteLength(p.goal, "utf8") < 6000, "移除重复背景，避免每个请求和验收继承几万字节的人工压力");
      assert.match(p.goal, /模型步骤不是turn/);
      assert.match(p.goal, /nextCursor/);
      await assert.rejects(fs.stat(p.deliverables[0]!), { code: "ENOENT" }, "夹具不替执行者生成交付物");
    }
  } finally { await h.close(); }
});

test("收尾夹具：一次模型步骤只推进一次，必须活动中真实消费后才发本轮回执", async () => {
  const { createWrapupFixture } = await import(new URL("../../../dsh-longtask-e2e/wrapup-fixture.mjs", import.meta.url).href);
  const directory = await fs.mkdtemp(join(tmpdir(), "lt-wrap-fixture-"));
  const artifact = join(directory, "wrapup.md");
  let registered = 0;
  const fixture = createWrapupFixture({ runId: "run-one", directory, ownsSession: (task: string, sid: string) => task === "task-one" && sid === "worker", onReceipt: async (_task: string, path: string) => {
    assert.match(await fs.readFile(path, "utf8"), /WRAPUP-/);
    registered++;
  } });
  fixture.bind("task-one");
  const agent = { session: { id: "worker" } };
  const exec = { agent, signal: new AbortController().signal };
  let cursor = 0;
  const call = (action: string, runId = "run-one", caller = exec) => fixture.tool.execute({ runId, action, cursor }, caller).then((raw: string) => {
    const result = JSON.parse(raw); if (Number.isSafeInteger(result.nextCursor)) cursor = result.nextCursor; return result;
  });
  const observe = (type: string, data: unknown, id = "worker") => fixture.observe({ id }, { type, data });
  observe("step/start", { turn: 1, step: 1 });
  await fs.writeFile(artifact, "WRAPUP-旧回执");
  assert.equal((await call("close")).status, "pending");
  const repeated = await Promise.all([call("advance"), call("advance"), call("advance")]);
  assert.equal(repeated[0].checked, 1);
  assert.deepEqual(repeated[0], repeated[2]);
  assert.equal(fixture.snapshot(artifact).explored, 1);
  for (const [kind, key, sid] of [["longtask", "wrap-up:other:v1", "worker"], ["other", "wrap-up:task-one:v1", "worker"], ["longtask", "wrap-up:task-one:v1", "acceptor"]]) {
    observe("user/message", { source: { kind, dedupeKey: key } }, sid);
    assert.equal((await call("close")).status, "pending");
  }
  await assert.rejects(call("advance", "old-run"), /不属于/);
  await assert.rejects(call("advance", "run-one", { ...exec, agent: { session: { id: "acceptor" } } }), /不属于/);
  observe("step/start", { turn: 1, step: 2 });
  await assert.rejects(fixture.tool.execute({ runId: "run-one", action: "advance", cursor: 0 }, exec), /过期检查点/);
  assert.equal((await call("advance")).checked, 2);
  observe("step/start", { turn: 1, step: 3 });
  observe("user/message", { source: { kind: "longtask", dedupeKey: "wrap-up:task-one:v1" } });
  assert.equal((await call("advance")).status, "closing");
  const result = await call("close");
  assert.match(result.receipt, /^WRAPUP-/);
  assert.equal(result.receiptRecordWritten, true);
  assert.equal(result.artifactRegistered, true);
  assert.match(result.instruction, /write成功后直接输出done=true/);
  assert.equal((await call("close")).receipt, result.receipt, "幂等领取");
  assert.equal(registered, 1, "回执在返回模型前登记，重复领取不重复登记");
  const before = fixture.snapshot(artifact);
  assert.equal(before.artifactMatches, false, "旧回执不能通过");
  assert.equal(before.pendingAtControl, true);
  assert.ok(before.receiptOrder > before.controlOrder);
  await fs.writeFile(artifact, `八条测试要点\n${result.receipt}\n`);
  assert.equal(fixture.snapshot(artifact).artifactMatches, true);
  assert.equal(fixture.snapshot(artifact).receiptFileMatches, true);
  await fs.writeFile(fixture.receiptPath, "篡改记录");
  assert.equal(fixture.snapshot(artifact).receiptFileMatches, false);
  fixture.dispose();
  await assert.rejects(call("close"), /不属于/);
});

test("收尾夹具：迟到控制无效、缺能力不安装、检查到上限后停止且不追加预算", async () => {
  const { createWrapupFixture, MAX_CHECKS } = await import(new URL("../../../dsh-longtask-e2e/wrapup-fixture.mjs", import.meta.url).href);
  const directory = await fs.mkdtemp(join(tmpdir(), "lt-wrap-negative-"));
  let pauses = 0;
  const make = () => createWrapupFixture({ runId: "run", directory, ownsSession: (_task: string, sid: string) => sid === "worker", onExhausted: async () => { pauses++; } });
  const late = make(); late.bind("task");
  assert.throws(() => late.install({ get: () => undefined }), /缺失时不创建付费任务/);
  const exec = { agent: { session: { id: "worker" } }, signal: new AbortController().signal };
  const cursors = new Map<any, number>();
  const call = (f: any, action: string) => f.tool.execute({ runId: "run", action, cursor: cursors.get(f) ?? 0 }, exec).then((raw: string) => {
    const r = JSON.parse(raw); if (Number.isSafeInteger(r.nextCursor)) cursors.set(f, r.nextCursor); return r;
  });
  late.observe({ id: "worker" }, { type: "step/start", data: { turn: 1, step: 1 } });
  await call(late, "advance");
  late.observe({ id: "worker" }, { type: "turn/end", data: {} });
  late.observe({ id: "worker" }, { type: "user/message", data: { source: { kind: "longtask", dedupeKey: "wrap-up:task:v1" } } });
  assert.equal(late.snapshot().pendingAtControl, false);
  await assert.rejects(call(late, "close"), /活动模型步骤/);
  const capped = make(); capped.bind("task");
  for (let step = 1; step <= MAX_CHECKS; step++) {
    capped.observe({ id: "worker" }, { type: "step/start", data: { turn: 1, step } });
    const reply = await call(capped, "advance");
    assert.equal(reply.status, step === MAX_CHECKS ? "exhausted" : "checking");
  }
  assert.equal(pauses, 1);
  assert.equal((await call(capped, "advance")).status, "exhausted");
  assert.equal(pauses, 1, "重复或并行调用不得重复暂停");
  assert.equal(capped.snapshot().receiptIssued, false);
  assert.equal(capped.snapshot().explored, MAX_CHECKS);
  capped.dispose(); late.dispose();
});

test("收尾夹具与产品 apply 联动：默认策略及26万固定预算下，真实控制进入当前提案后才能完成握手", async () => {
  const { createWrapupFixture } = await import(new URL("../../../dsh-longtask-e2e/wrapup-fixture.mjs", import.meta.url).href);
  const { fake, api, dir } = await makeApp(); // 不调低收尾比例或步数上限。
  const task = await api.propose({ goal: "[E2E-WRAPUP] 活动检查", criteria: [{ id: "c1", text: "收尾" }], deliverables: [join(dir, "out.md")], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 260000, strict: true } });
  await api.confirm(task.id);
  const worker = findAgentByMessage(fake, "research/worker.md")!;
  const fixture = createWrapupFixture({ runId: "run", directory: dir, ownsSession: (id: string, sid: string) => id === task.id && sid === worker.sessionId,
    onReceipt: async (id: string, path: string) => {
      const hash = createHash("sha256").update(await fs.readFile(path)).digest("hex");
      await (api as unknown as { registerArtifact(id: string, path: string, hash: string, owner: string): Promise<void> }).registerArtifact(id, path, hash, "harness");
    },
  });
  fixture.bind(task.id);
  fake.ctx.on("session/event", fixture.observe);
  const exec = { agent: worker, signal: new AbortController().signal };
  let cursor = 0;
  const call = (action: string) => fixture.tool.execute({ runId: "run", action, cursor }, exec).then((raw: string) => {
    const r = JSON.parse(raw); if (Number.isSafeInteger(r.nextCursor)) cursor = r.nextCursor; return r;
  });
  try {
    let consumed = false;
    assert.equal(await fake.emitPreStep(worker.sessionId), true);
    for (let step = 1; step <= 7; step++) {
      fake.emitSessionEvent(worker.sessionId, { type: "step/start", data: { turn: 1, step } });
      // 消费的是产品真实返回的 UserMessage（保留 source），没有自行构造收尾消息。
      for (const message of fake.stepMessages.get(worker.sessionId) ?? []) {
        if ((message as { source?: { dedupeKey?: string } }).source?.dedupeKey?.startsWith("wrap-up:")) {
          fake.emitSessionEvent(worker.sessionId, { type: "user/message", data: message });
          consumed = true;
        }
      }
      if (consumed) {
        assert.equal((await call("close")).status, "close");
        break;
      }
      assert.equal((await call("advance")).status, "checking");
      fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 26000, outputTokens: 1000, cacheReadTokens: 0, cacheWriteTokens: 0 }));
      assert.equal(await fake.emitPreStep(worker.sessionId), true);
    }
    assert.equal(consumed, true);
    const evidence = fixture.snapshot();
    assert.equal(evidence.pendingAtControl, true);
    assert.equal(evidence.receiptIssued, true);
    assert.ok(evidence.explored > 0 && evidence.explored < 8);
    assert.equal(api.getTask(task.id)?.status, "active", "收尾消费发生时尚有落盘与报告工作");
    const b = api.getBudget(task.id)!;
    assert.equal(b.limitTokens, 260000);
    assert.ok(b.settledTokens + b.reservedTokens < b.limitTokens);
    await fs.writeFile(join(dir, "out.md"), "收尾产物");
    fake.emitSessionEvent(worker.sessionId, assistantUsageEvent(1, { inputTokens: 26000, outputTokens: 1000, cacheReadTokens: 0, cacheWriteTokens: 0 }, '<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>'));
    fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
    await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
    const acceptance = lastDelivery(findAgentByMessage(fake, "独立验收请求")!);
    assert.ok(acceptance.includes(fixture.receiptPath), "验收的允许读取清单必须含回执");
    const receiptHash = createHash("sha256").update(await fs.readFile(fixture.receiptPath)).digest("hex");
    assert.ok(Object.values(frozenHashes(acceptance)).includes(receiptHash), "回执与真实产物一起冻结，不能在验收后补写");
  } finally { fixture.dispose(); for (const teardown of fake.teardowns) await teardown(); }
});

test("收尾夹具真实 SDK ToolRuntime：schema可装配、正文按身份拦截、卸载注销工具", async () => {
  const { createWrapupFixture, WRAPUP_TOOL } = await import(new URL("../../../dsh-longtask-e2e/wrapup-fixture.mjs", import.meta.url).href);
  const { Context } = await import("@deepseek-ai/cordis");
  const { ToolRuntime } = await loadSdk("dsh-tools");
  const { apply: applyRepeatReminder } = await loadSdk("dsh-repeat-tool-reminder");
  const root = new Context(); root.provide("systemPrompt", { tools() {} });
  const runtime = await prepareSdkTools(root,new ToolRuntime(root));
  applyRepeatReminder(root, { thresholds: [3, 5, 8], include: [], exclude: [], argumentsPreviewChars: 500 });
  const directory = await fs.mkdtemp(join(tmpdir(), "lt-wrap-sdk-"));
  const fixture = createWrapupFixture({ runId: "run", directory, ownsSession: (_task: string, sid: string) => sid === "worker", ownsAcceptorSession: (_task: string, sid: string) => sid === "acceptor" });
  fixture.bind("task");
  fixture.install({ get: () => runtime, on: (name: string, fn: (...args: any[]) => void) => root.on(name as any, fn as any) });
  try {
    const agent = { id: "worker", session: { id: "worker" } };
    assert.ok(runtime.schemas(agent).some((s: { name: string }) => s.name === WRAPUP_TOOL));
    (root as any).emit("session/event", agent.session, { type: "step/start", data: { turn: 1, step: 1 } });
    const request = { name: WRAPUP_TOOL, callId: "check-1", arguments: { runId: "run", action: "advance", cursor: 0 }, agent, signal: new AbortController().signal };
    const result = await runtime.execute(request);
    assert.equal(result.isError, false);
    assert.equal(JSON.parse(result.value).checked, 1);
    for (let step = 2; step <= 4; step++) {
      (root as any).emit("session/event", agent.session, { type: "step/start", data: { turn: 1, step } });
      const progressed = await runtime.execute({ ...request, callId: `check-${step}`, arguments: { ...request.arguments, cursor: step - 1 } });
      assert.equal(JSON.parse(progressed.value).checked, step);
      assert.equal((progressed.additionalContexts ?? []).some((m: any) => m.source?.kind === "repeat-tool-reminder"), false, "真实宿主重复调用检测识别不同的检查点");
    }
    let repeated;
    for (let n = 0; n < 2; n++) repeated = await runtime.execute({ ...request, callId: `duplicate-${n}`, arguments: { ...request.arguments, cursor: 3 } });
    assert.ok((repeated.additionalContexts ?? []).some((m: any) => m.source?.kind === "repeat-tool-reminder"), "阳性对照：原样重放仍会被真实宿主提醒，未关闭防循环机制");
    let shellBodies = 0;
    runtime.register({ name: "pwsh", parameters: { type: "object", properties: {} }, output: { schema: { type: "string" }, render: (_args: unknown, value: string) => [{ type: "text", text: value }] }, execute: async () => { shellBodies++; return "executed"; } });
    for (const id of ["worker", "acceptor"]) {
      const blocked = await runtime.execute({ ...request, name: "pwsh", callId: `shell-${id}`, arguments: {}, agent: { id, session: { id } } });
      assert.equal(blocked.isError, true);
    }
    assert.equal(shellBodies, 0, "本任务的命令在沙箱初始化之前被拦下");
    assert.equal((await runtime.execute({ ...request, name: "pwsh", callId: "shell-user", arguments: {}, agent: { id: "user", session: { id: "user" } } })).isError, false);
    assert.equal(shellBodies, 1, "不影响用户正常会话");
    const foreign = await runtime.execute({ ...request, callId: "foreign", agent: { id: "user", session: { id: "user" } } });
    assert.equal(foreign.isError, true);
    fixture.dispose();
    assert.equal(runtime.schemas(agent).some((s: { name: string }) => s.name === WRAPUP_TOOL), false);
    assert.equal((await runtime.execute({ ...request, name: "pwsh", callId: "after-dispose", arguments: {} })).isError, false, "结束后移除临时命令限制");
  } finally { fixture.dispose(); await root.fiber.dispose(); }
});

test('阶段三A费用回放：16万在额外探索前暂停留证，20万响应收尾后完成独立验收且无追加',async()=>{
  for(const limitTokens of [160000,200000]){
    const {fake,api,dir}=await makeApp();const artifact=join(dir,'stage3-A.md');
    try{
      const task=await api.propose({goal:'生命周期A费用回放',criteria:[{id:'c1',text:'产物存在且内容正确'}],deliverables:[artifact],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens,strict:true}});await api.confirm(task.id);
      const worker=findAgentByMessage(fake,'research/worker.md')!;
      const costs=limitTokens===160000?[21926,22286,22468,22063,22280,22696]:[21926,22286,22468,22063,22696];
      await fs.writeFile(artifact,'真实受控产物');
      let blocked=false;
      for(let step=0;step<costs.length;step++){
        const entered=await fake.emitPreStep(worker.sessionId);
        if(!entered){assert.equal(limitTokens,160000);assert.ok(step<costs.length-1,'应在耗尽报告和验收预算前停下');blocked=true;break;}
        fake.emitSessionEvent(worker.sessionId,assistantUsageEvent(1,{inputTokens:costs[step]!-100,outputTokens:100,cacheReadTokens:0,cacheWriteTokens:0},step===costs.length-1?'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>':'执行检查点'));
      }
      if(blocked){
        await fake.waitFor(()=>worker.stopped);const budget=api.getBudget(task.id)!;
        assert.ok(budget.settledTokens<133719);assert.equal(budget.reservedTokens,0);
        const evidence=api.getTaskEvidence(task.id);const plan=evidence.collections.closingBudgetPlans![task.id] as any;
        assert.ok(plan.acceptance.tokens>28945);assert.ok(plan.acceptance.tokens>limitTokens*.1);
        const last=Object.values(evidence.events).at(-1) as any;assert.match(last.payload.note,/未请求追加额度/);assert.match(last.payload.note,/标准：c1/);
        assert.equal(findAgentByMessage(fake,'独立验收请求'),undefined);continue;
      }
      fake.emitSessionEvent(worker.sessionId,turnEndEvent(1));await fake.waitFor(()=>!!findAgentByMessage(fake,'独立验收请求'));
      const acceptor=findAgentByMessage(fake,'独立验收请求')!;assert.equal(api.getBudget(task.id)!.settledTokens,111439);
      const admitted=await fake.emitPreStep(acceptor.sessionId,inboundChars(5945));assert.equal(admitted,limitTokens===200000);
      const actual=await fs.readFile(artifact,'utf8');assert.equal(actual,'真实受控产物');
      fake.emitSessionEvent(acceptor.sessionId,assistantUsageEvent(1,{inputTokens:15500,outputTokens:200,cacheReadTokens:0,cacheWriteTokens:0},'已读取实际产物'));
      assert.equal(await fake.emitPreStep(acceptor.sessionId),true);
      const request=lastDelivery(acceptor),roundId=/轮次 (round_[a-z0-9]+)/.exec(request)![1];
      fake.emitSessionEvent(acceptor.sessionId,assistantUsageEvent(1,{inputTokens:16000,outputTokens:400,cacheReadTokens:0,cacheWriteTokens:0},`<longtask_verdicts>${JSON.stringify({roundId,verdicts:[{criterionId:'c1',outcome:'pass',evidence:actual,gap:null,verdictBy:REVIEWER.relPath,criteriaVersion:1}],artifactHashes:frozenHashes(request)})}</longtask_verdicts>`));fake.emitSessionEvent(acceptor.sessionId,turnEndEvent(1));
      await fake.waitFor(()=>api.getTask(task.id)?.status==='delivered'&&api.getBudget(task.id)?.reservedTokens===0);
      const budget=api.getBudget(task.id)!;assert.equal(budget.settledTokens,143539);assert.equal(budget.limitTokens,200000);assert.deepEqual((budget as any).grants,[]);
    }finally{for(const teardown of fake.teardowns)await teardown();}
  }
});

test('阶段三实际简报费用矩阵：A五步/B三步/C三步及各两步验收全部在50万内',async()=>{
  const {stage3TaskInput}=await import(new URL('../../../dsh-longtask-e2e/stage3-driver.mjs',import.meta.url).href);
  const {STAGE3_TOOL,splitBudget}=await import(new URL('../../../dsh-longtask-e2e/stage3-protocol.mjs',import.meta.url).href);
  const {fake,api,dir}=await makeApp();const get=fake.ctx.get.bind(fake.ctx);
  fake.ctx.get=((name:string)=>name==='tools'?{...get<any>('tools'),get:(name:string)=>name===STAGE3_TOOL?{name}:undefined}:get(name)) as typeof fake.ctx.get;
  const run='11111111-1111-4111-8111-111111111111',shares=splitBudget(500000),costs:any={A:[21926,22286,22468,22063,22696],B:[22177,22472,22300],C:[22082,22226,22300]};let total=0;
  try{
    const out=join(dir,`stage3-${run}`);await fs.mkdir(out);
    for(const role of ['A','B','C'] as const){
      const artifact=join(out,`${role}.md`),spec=stage3TaskInput(run,role,artifact);
      const t=await api.propose({...spec,executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:shares[role],strict:true}});await api.confirm(t.id);
      const worker=[...fake.agentsById.values()].find(a=>a.messages.some(m=>m.includes(spec.goal)))!;
      for(let step=0;step<costs[role].length;step++){
        assert.equal(await fake.emitPreStep(worker.sessionId,step===0?inboundChars(17000):[]),true,`${role}执行step${step+1}`);
        if(step===1)await fs.writeFile(artifact,`STAGE3 ${run} ${role}\n${role==='A'?'后台写入完成':role==='B'?'口径码：ST3-confirmed':'优先任务完成'}`);
        fake.emitSessionEvent(worker.sessionId,assistantUsageEvent(1,{inputTokens:costs[role][step]-100,outputTokens:100,cacheReadTokens:0,cacheWriteTokens:0},step===costs[role].length-1?'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>':'成本回放'));
      }
      fake.emitSessionEvent(worker.sessionId,turnEndEvent(1));
      await fake.waitFor(()=>[...fake.agentsById.values()].some(a=>a.messages.some(m=>m.includes('独立验收请求')&&m.includes(t.id))));
      const acceptor=[...fake.agentsById.values()].find(a=>a.messages.some(m=>m.includes('独立验收请求')&&m.includes(t.id)))!;
      const request=lastDelivery(acceptor),roundId=/轮次 (round_[a-z0-9]+)/.exec(request)![1];
      assert.equal(await fake.emitPreStep(acceptor.sessionId,inboundChars(5945)),true,`${role}验收首步`);
      const actual=await fs.readFile(artifact,'utf8');fake.emitSessionEvent(acceptor.sessionId,assistantUsageEvent(1,{inputTokens:15500,outputTokens:200,cacheReadTokens:0,cacheWriteTokens:0},actual));
      assert.equal(await fake.emitPreStep(acceptor.sessionId),true,`${role}验收结论`);
      fake.emitSessionEvent(acceptor.sessionId,assistantUsageEvent(1,{inputTokens:16000,outputTokens:400,cacheReadTokens:0,cacheWriteTokens:0},`<longtask_verdicts>${JSON.stringify({roundId,verdicts:spec.criteria.map((c:any)=>({criterionId:c.id,outcome:'pass',evidence:actual,gap:null,verdictBy:REVIEWER.relPath,criteriaVersion:1})),artifactHashes:frozenHashes(request)})}</longtask_verdicts>`));fake.emitSessionEvent(acceptor.sessionId,turnEndEvent(1));
      await fake.waitFor(()=>api.getTask(t.id)?.status==='delivered'&&api.getBudget(t.id)?.reservedTokens===0);
      const budget=api.getBudget(t.id)!;assert.equal(budget.limitTokens,shares[role]);assert.deepEqual((budget as any).grants,[]);total+=budget.settledTokens;
    }
    assert.equal(total,341296);assert.ok(total<500000);
  }finally{for(const stop of fake.teardowns)await stop();}
});

test('预算计划随产物增长重算；暂停恢复和同目录重新装配不重置费用、不误启预算暂停',async()=>{
  const {fake,api,dir}=await makeApp();let second:ReturnType<typeof makeAppLike>|undefined;
  try{
    const artifact=join(dir,'growth.md');await fs.writeFile(artifact,'小产物');
    const t=await api.propose({goal:'固定预算恢复',criteria:[{id:'c1'}],deliverables:[artifact],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:200000,strict:true}});await api.confirm(t.id);
    const worker=findAgentByMessage(fake,'research/worker.md')!;assert.equal(await fake.emitPreStep(worker.sessionId),true);
    fake.emitSessionEvent(worker.sessionId,assistantUsageEvent(1,{inputTokens:20000,outputTokens:100,cacheReadTokens:0,cacheWriteTokens:0}));await fake.waitFor(()=>api.getBudget(t.id)?.settledTokens===20100&&api.getBudget(t.id)?.reservedTokens===0);
    const initial=(api.getTaskEvidence(t.id).collections.closingBudgetPlans![t.id] as any).acceptance.tokens;
    await api.pause(t.id,'用户暂停后产物变化');await fs.writeFile(artifact,'x'.repeat(40000));await api.resume(t.id);
    const current=[...fake.agentsById.values()].find(a=>a.messages.some(m=>m.includes('任务恢复')))!;
    assert.equal(await fake.emitPreStep(current.sessionId),false);await fake.waitFor(()=>api.getTask(t.id)?.pauseReason==='budget');
    const grown=(api.getTaskEvidence(t.id).collections.closingBudgetPlans![t.id] as any).acceptance.tokens;assert.ok(grown>initial);assert.equal(api.getBudget(t.id)!.settledTokens,20100);assert.equal(api.getBudget(t.id)!.limitTokens,200000);
    for(const stop of fake.teardowns)await stop();second=makeAppLike(dir);await second.waitFor(()=>!!second!.provided.get('longtask'));
    const restored=second.provided.get('longtask') as LongtaskApi;assert.equal(restored.getTask(t.id)!.pauseReason,'budget');assert.equal(restored.getBudget(t.id)!.settledTokens,20100);assert.equal(second.agentsById.size,0);
  }finally{if(second){for(const stop of second.teardowns)await stop();}else{for(const stop of fake.teardowns)await stop();}}
});

test('固定预算退回再验收：返工沿用费用并重新保护验收，最终结算不追加',async()=>{
  const {fake,api,dir}=await makeApp();
  try{
    const artifact=join(dir,'rework-budget.md');await fs.writeFile(artifact,'初稿');
    const t=await api.propose({goal:'固定预算返工',criteria:[{id:'c1'}],deliverables:[artifact],executors:[EXECUTOR],acceptor:REVIEWER,budget:{limitTokens:300000,strict:true}});await api.confirm(t.id);
    const worker=findAgentByMessage(fake,'research/worker.md')!;
    for(let round=1;round<=2;round++){
      assert.equal(await fake.emitPreStep(worker.sessionId),true);if(round===2)await fs.writeFile(artifact,'修订完成');
      const plan=api.getTaskEvidence(t.id).collections.closingBudgetPlans![t.id] as any;assert.ok(plan.acceptance.tokens>plan.acceptance.firstRequest);assert.equal(plan.taskVersion,1);
      fake.emitSessionEvent(worker.sessionId,assistantUsageEvent(round,{inputTokens:20000,outputTokens:200,cacheReadTokens:0,cacheWriteTokens:0},'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>'));fake.emitSessionEvent(worker.sessionId,turnEndEvent(round));
      await fake.waitFor(()=>api.getTask(t.id)?.status==='awaiting_acceptance'&&[...fake.agentsById.values()].reduce((n,a)=>n+a.messages.filter(m=>m.includes('独立验收请求')).length,0)>=round);
      const acceptor=[...fake.agentsById.values()].find(a=>a.messages.some(m=>m.includes('独立验收请求')))!;
      const request=acceptor.messages.findLast(m=>m.includes('独立验收请求'))!,roundId=/轮次 (round_[a-z0-9]+)/.exec(request)![1];
      assert.equal(await fake.emitPreStep(acceptor.sessionId),true);
      fake.emitSessionEvent(acceptor.sessionId,assistantUsageEvent(round,{inputTokens:10000,outputTokens:200,cacheReadTokens:0,cacheWriteTokens:0},`<longtask_verdicts>${JSON.stringify({roundId,verdicts:[{criterionId:'c1',outcome:round===1?'gap':'pass',gap:round===1?'补齐要求':null,evidence:'已读取产物版本',verdictBy:REVIEWER.relPath,criteriaVersion:1}],artifactHashes:frozenHashes(request)})}</longtask_verdicts>`));fake.emitSessionEvent(acceptor.sessionId,turnEndEvent(round));
      await fake.waitFor(()=>api.getTask(t.id)?.status===(round===1?'rework':'delivered')&&api.getBudget(t.id)?.reservedTokens===0);
      assert.equal(api.getBudget(t.id)!.settledTokens,round*30400);
    }
    assert.equal(api.getBudget(t.id)!.limitTokens,300000);assert.deepEqual((api.getBudget(t.id) as any).grants,[]);
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("收尾固定预算覆盖最终报告与三步验收：不同请求成本均完整交付，不追加额度", async () => {
  const { createWrapupFixture, MAX_CHECKS } = await import(new URL("../../../dsh-longtask-e2e/wrapup-fixture.mjs", import.meta.url).href);
  for (const baseInput of [16000, 22000, 28000]) {
    const { fake, api, dir } = await makeApp();
    const artifact = join(dir, "matrix.md");
    const task = await api.propose({ goal: "[E2E-WRAPUP] 收尾完整链路", criteria: [{ id: "c1", text: "文件含回执" }], deliverables: [artifact], executors: [EXECUTOR], acceptor: REVIEWER, budget: { limitTokens: 260000, strict: true } });
    await api.confirm(task.id);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    const fixture = createWrapupFixture({ runId: "matrix", directory: dir, ownsSession: (_id: string, sid: string) => sid === worker.sessionId });
    fixture.bind(task.id); fake.ctx.on("session/event", fixture.observe);
    const usage = (input: number, text = "继续") => assistantUsageEvent(1, { inputTokens: input, outputTokens: 300, cacheReadTokens: 0, cacheWriteTokens: 0 }, text);
    const exec = { agent: worker, signal: new AbortController().signal };
    let cursor = 0;
    let steps = 0;
    try {
      assert.equal(await fake.emitPreStep(worker.sessionId, inboundChars(12000)), true);
      let token;
      for (let step = 1; step <= MAX_CHECKS; step++) {
        steps = step;
        fake.emitSessionEvent(worker.sessionId, { type: "step/start", data: { turn: 1, step } });
        let closing = false;
        for (const message of fake.stepMessages.get(worker.sessionId) ?? []) {
          if ((message as any).source?.dedupeKey?.startsWith("wrap-up:")) {
            fake.emitSessionEvent(worker.sessionId, { type: "user/message", data: message }); closing = true;
          }
        }
        const result = JSON.parse(await fixture.tool.execute({ runId: "matrix", action: closing ? "close" : "advance", cursor }, exec));
        if (closing) { token = result.receipt; fake.emitSessionEvent(worker.sessionId, usage(baseInput + step * 160)); break; }
        cursor = result.nextCursor;
        fake.emitSessionEvent(worker.sessionId, usage(baseInput + step * 160));
        assert.equal(await fake.emitPreStep(worker.sessionId), true, `input=${baseInput} step=${step}`);
      }
      assert.ok(token, "未靠降低阈值即可消费收尾");
      assert.equal(await fake.emitPreStep(worker.sessionId), true, "留有写入步骤");
      await fs.writeFile(artifact, `本地模拟产物\n${token}`);
      fake.emitSessionEvent(worker.sessionId, usage(baseInput + (++steps) * 160));
      assert.equal(await fake.emitPreStep(worker.sessionId), true, "留有最终报告步骤");
      fake.emitSessionEvent(worker.sessionId, usage(baseInput + (++steps) * 160, '<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>'));
      fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
      await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
      const acceptor = findAgentByMessage(fake, "独立验收请求")!;
      const request = lastDelivery(acceptor);
      const roundId = /轮次 (round_[a-z0-9]+)/.exec(request)![1];
      for (let step = 1; step <= 3; step++) {
        assert.equal(await fake.emitPreStep(acceptor.sessionId), true, `input=${baseInput} 验收step=${step}`);
        const text = step < 3 ? "读取并核对产物" : `<longtask_verdicts>${JSON.stringify({ roundId, verdicts: [{ criterionId: "c1", outcome: "pass", evidence: "已核对回执与产物", gap: null, verdictBy: REVIEWER.relPath, criteriaVersion: 1 }], artifactHashes: frozenHashes(request) })}</longtask_verdicts>`;
        fake.emitSessionEvent(acceptor.sessionId, usage(16000 + step * 160, text));
      }
      fake.emitSessionEvent(acceptor.sessionId, turnEndEvent(1));
      await fake.waitFor(() => api.getTask(task.id)?.status === "delivered");
      await fake.waitFor(() => api.getBudget(task.id)?.reservedTokens === 0);
      const budget = api.getBudget(task.id)!;
      assert.equal(budget.limitTokens, 260000);
      assert.ok(budget.settledTokens <= 260000);
      assert.equal(fake.logs.some((l) => l.includes("bound-exceeded")), false, "模拟请求均受准入上界覆盖");
    } finally { fixture.dispose(); for (const teardown of fake.teardowns) await teardown(); }
  }
});

test("联调同一实例接收连续两次显式请求；已结束记录不阻止重跑，也不自动再跑", async () => {
  const h = await harnessFixture();
  const { publishRunRequest } = await import(new URL("../../../dsh-longtask-e2e/request-run.mjs", import.meta.url).href);
  try {
    const request = { scenario: "basic", budgetTokens: 200, strict: true };
    publishRunRequest(request, h.transcriptDir);
    assert.throws(() => publishRunRequest({ ...request, budgetTokens: 300 }, h.transcriptDir), { code: "EEXIST" }, "不覆盖待消费请求");
    const summaries = async () => (await fs.readdir(h.transcriptDir)).filter((name) => name.startsWith("summary-"));
    await h.wait(async () => (await summaries()).length === 1);
    publishRunRequest({ ...request, budgetTokens: 300 }, h.transcriptDir);
    await h.wait(async () => (await summaries()).length === 2);
    await new Promise((r) => setTimeout(r, 1200));
    assert.equal(h.calls(), 2, "每个显式请求只创建一次任务，空闲时不自动花预算");
  } finally { await h.close(); }
});

test("联调错误请求不得回退默认预算开跑；拒绝后仍能接收修正请求", async () => {
  const h = await harnessFixture();
  const { publishRunRequest } = await import(new URL("../../../dsh-longtask-e2e/request-run.mjs", import.meta.url).href);
  try {
    await fs.writeFile(join(h.transcriptDir, "run-request.json"), "{bad json");
    await h.wait(async () => (await fs.readdir(h.transcriptDir)).some((name) => name.startsWith("run-request-rejected-")));
    assert.equal(h.calls(), 0);
    assert.throws(() => publishRunRequest({ scenario: "wrapup", budgetTokens: 200, wrapUpAtBudgetRatio: 0.1 }, h.transcriptDir), /不支持的字段/);
    publishRunRequest({ scenario: "basic", budgetTokens: 200, strict: true }, h.transcriptDir);
    await h.wait(async () => (await fs.readdir(h.transcriptDir)).some((name) => name.startsWith("summary-")));
    assert.equal(h.calls(), 1);
  } finally { await h.close(); }
});

test("故障夹具：最终无工具完成报告同步注入；冻结后不改、由原Worker真实修复", async () => {
  const { createGapFixture, GAP_BODY } = await import(new URL("../../../dsh-longtask-e2e/gap-fixture.mjs", import.meta.url).href);
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-gap-unit-"));
  const artifactPath = join(dir, "out.md");
  const marker = "SYNC-TEST-RUN";
  const original = `${GAP_BODY}\n${marker}\n`;
  await fs.writeFile(artifactPath, original);
  const view: any = { task: { status: "proposed" }, version: { version: 1, goal: "g", criteria: ["c1", "c2"], confirmedAt: null },
    sessions: [{ sessionId: "worker", role: "executor" }, { sessionId: "acceptor", role: "acceptor" }], rounds: [], artifacts: [], events: [] };
  const entries: any[] = [];
  const fixture = createGapFixture({ runId: "run", artifactPath, marker, readView: () => view, log: (kind: string, data: unknown) => entries.push({ kind, data }) });
  fixture.bind("task"); view.task.status = "active"; view.version.confirmedAt = "confirmed";
  const report = assistantEvent('<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>', 1);
  fixture.observe({ id: "unrelated" }, report);
  fixture.observe({ id: "worker" }, { ...report, data: { ...report.data, message: { ...report.data.message, content: [...report.data.message.content, { type: "tool-call", name: "write" }] } } });
  assert.equal(await fs.readFile(artifactPath, "utf8"), original, "带工具调用的中间报告不能提前注入");
  fixture.observe({ id: "worker" }, report);
  const corrupted = await fs.readFile(artifactPath, "utf8");
  assert.equal(corrupted, original.slice(0, original.lastIndexOf(marker)));
  assert.equal(view.rounds.length, 0, "注入发生时没有冻结轮次");
  fixture.observe({ id: "worker" }, report);
  assert.equal(entries.filter((e) => e.kind === "fault_injected").length, 1, "同一报告重复送达幂等");
  const firstHash = createHash("sha256").update(corrupted).digest("hex");
  view.artifacts.push({ id: "a1", location: artifactPath, contentHash: firstHash });
  view.rounds.push({ roundId: "r1", state: "pending", artifactHashes: { a1: firstHash } });
  view.task.status = "awaiting_acceptance";
  fixture.observe({ id: "acceptor" }, { type: "step/start", data: { turn: 1, step: 1 } });
  assert.equal(await fs.readFile(artifactPath, "utf8"), corrupted, "验收开始后不再改变缺项版本");
  assert.equal(fixture.snapshot().phases.firstFrozen, 2);
  view.rounds[0].state = "settled";
  view.events.push({ type: "task.verdict", payload: { roundId: "r1", verdicts: [{ criterionId: "c2", outcome: "gap" }] } });
  view.task.status = "rework";
  await fs.writeFile(artifactPath, original); // 模拟原Worker实际写入修复，夹具只观察。
  fixture.observe({ id: "worker" }, assistantEvent('<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>', 2));
  const repairedHash = createHash("sha256").update(original).digest("hex");
  view.artifacts.push({ id: "a2", location: artifactPath, contentHash: repairedHash });
  view.rounds.push({ roundId: "r2", state: "pending", artifactHashes: { a2: repairedHash } });
  view.task.status = "awaiting_acceptance";
  fixture.observe({ id: "acceptor" }, { type: "step/start", data: { turn: 2, step: 1 } });
  assert.deepEqual(fixture.snapshot().phases, { injected: 1, firstFrozen: 2, gap: 3, repaired: 4, secondFrozen: 5 });
  assert.equal(fixture.snapshot().artifactMatches, true);
  assert.equal(fixture.snapshot().failure, null);
  assert.equal(fixture.guard({ name: "write", agent: { session: { id: "acceptor" } } })?.includes("验收只允许"), true);
  assert.equal(fixture.guard({ name: "pwsh", agent: { session: { id: "worker" } } })?.includes("不运行终端"), true);
  assert.equal(fixture.guard({ name: "pwsh", agent: { session: { id: "user" } } }), undefined);
  fixture.dispose();
  assert.equal(fixture.guard({ name: "write", agent: { session: { id: "acceptor" } } }), undefined);
});

test("故障夹具负例：未写齐、已有冻结或验收版本错误均停止，不能伪装注入成功", async () => {
  const { createGapFixture, GAP_BODY } = await import(new URL("../../../dsh-longtask-e2e/gap-fixture.mjs", import.meta.url).href);
  for (const mode of ["missing-marker", "extra-domain-lines", "already-frozen", "wrong-frozen-hash", "criteria-changed"]) {
    const dir = await fs.mkdtemp(join(tmpdir(), "lt-gap-reject-"));
    const path = join(dir, "out.md");
    const content = `${GAP_BODY}\n${mode === "extra-domain-lines" ? "未经要求的领域扩写。\n" : ""}${mode === "missing-marker" ? "" : "SYNC-X\n"}`;
    await fs.writeFile(path, content);
    const view: any = { task: { status: "proposed" }, version: { version: 1, goal: "g" }, sessions: [{ sessionId: "w", role: "executor" }, { sessionId: "a", role: "acceptor" }], rounds: [], artifacts: [], events: [] };
    let pauses = 0;
    const fixture = createGapFixture({ runId: "run", artifactPath: path, marker: "SYNC-X", readView: () => view, onFailure: async () => { pauses++; } });
    fixture.bind("t"); view.task.status = "active";
    if (mode === "already-frozen") view.rounds.push({ roundId: "r", state: "pending" });
    if (mode === "criteria-changed") view.version.goal = "changed";
    fixture.observe({ id: "w" }, assistantEvent('<longtask_report>{"done":true}</longtask_report>', 1));
    if (mode === "wrong-frozen-hash") {
      view.task.status = "awaiting_acceptance";
      view.rounds.push({ roundId: "r", state: "pending", artifactHashes: { a: "wrong" } });
      view.artifacts.push({ id: "a", location: path });
      fixture.observe({ id: "a" }, { type: "step/start", data: {} });
    } else assert.equal(await fs.readFile(path, "utf8"), content, "前提无效时不能修改文件");
    await Promise.resolve();
    assert.ok(fixture.snapshot().failure, mode);
    assert.equal(pauses, 1, mode);
    fixture.dispose();
  }
});

test("故障注入完整双apply链路：按真机成本回放，读后编辑、再验收最坏输出仍在26万内交付", async () => {
  const { apply: applyE2e } = await import(new URL("../../../dsh-longtask-e2e/index.js", import.meta.url).href);
  const { GAP_BODY } = await import(new URL("../../../dsh-longtask-e2e/gap-fixture.mjs", import.meta.url).href);
  const { fake, api, dir } = await makeApp();
  const transcriptDir = join(dir, "e2e");
  const usageTotals = new Map<string, { input: number; output: number }>();
  const harnessTeardowns: Array<() => unknown> = [];
  const publicApi = { ...api, searchExperts: async (q: string) => [q === "engineering" ? EXECUTOR : REVIEWER] };
  applyE2e({
    get: (name: string) => name === "longtask" ? publicApi : name === "subagents" ? { listDescendants: async () => [] }
      : name === "sessionProjections" ? { stateOf: (session: { id: string }) => { const u = usageTotals.get(session.id)!; return { totals: { uncachedInputTokens: u.input, outputTokens: u.output, cacheReadTokens: 0, cacheWriteTokens: 0 } }; } }
      : fake.ctx.get(name), on: fake.ctx.on, logger: { info() {} },
    effect(factory: () => Generator) { const fn = factory().next().value; if (typeof fn === "function") harnessTeardowns.push(fn as () => unknown); },
  }, { transcriptDir, artifactDir: join(transcriptDir, "artifacts"), scenario: "gap-inject", taskBudgetTokens: 260000, strict: true, force: true, pollIntervalMs: 1, timeoutMs: 15000 });
  const load = async () => JSON.parse(await fs.readFile(join(dir, "state.json"), "utf8"));
  let total = 0;
  // 来自task_mw1yp115ntinc2实机usage，剔除明确无效的盲edit和重复read；
  // 最后一个尚未发生的验收结论，使用实机read后的输入上界27748和完整8000输出限额。
  const samples = [[21613, 1577], [23289, 302], [19337, 728], [20734, 3337], [24355, 116], [25140, 230], [26042, 288], [25267, 168], [27748, 8000]];
  let sampleIndex = 0;
  const steps = new Map<string, number>();
  const tick = async (sid: string, turn: number, text: string) => {
    assert.equal(await fake.emitPreStep(sid, inboundChars(2000)), true, `请求${sampleIndex + 1}必须在固定余额内准入`);
    const key = `${sid}:${turn}`, step = (steps.get(key) ?? 0) + 1; steps.set(key, step);
    fake.emitSessionEvent(sid, { type: "step/start", data: { turn, step } });
    const [input, output] = samples[sampleIndex++]!;
    const u = usageTotals.get(sid) ?? { input: 0, output: 0 }; u.input += input!; u.output += output!; usageTotals.set(sid, u);
    total += input! + output!;
    fake.emitSessionEvent(sid, assistantUsageEvent(turn, { inputTokens: input!, outputTokens: output!, cacheReadTokens: 0, cacheWriteTokens: 0 }, text));
    await fake.waitFor(() => api.getBudget(taskId)?.settledTokens === total);
  };
  let taskId = "";
  try {
    await fake.waitFor(() => (api.listTasks() as any[]).some((t) => t.versions[t.currentVersion].goal.startsWith("[E2E-GAP-INJECT]")), 4000);
    const task = (api.listTasks() as any[])[0]; taskId = task.id;
    await fake.waitFor(() => Boolean(findAgentByMessage(fake, "research/worker.md")));
    const version = task.versions[task.currentVersion];
    const path = version.deliverables[0];
    const marker = /SYNC-[A-F0-9-]{36}/.exec(version.goal)![0];
    assert.match(version.goal, /gap-before-freeze-v2/);
    assert.doesNotMatch(version.goal, /nonce\.txt|验收阶段才提供/);
    const worker = findAgentByMessage(fake, "research/worker.md")!;
    const content = `${GAP_BODY}\n${marker}\n`;
    await tick(worker.sessionId, 1, "准备写入完整文档");
    await fs.writeFile(path, content);
    await tick(worker.sessionId, 1, '<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>');
    const badContent = await fs.readFile(path, "utf8");
    assert.notEqual(badContent.trimEnd().split(/\r?\n/).at(-1), marker, "完成报告到达时同步移除末行");
    assert.equal(Object.values((await load()).collections.acceptanceRounds ?? {}).length, 0, "尚未turn/end，不能已有冻结轮次");
    fake.emitSessionEvent(worker.sessionId, turnEndEvent(1));
    await fake.waitFor(() => Boolean(findAgentByMessage(fake, "独立验收请求")));
    const acceptor = findAgentByMessage(fake, "独立验收请求")!;
    let forbiddenWrites = 0;
    assert.equal((await fake.emitTool(acceptor.sessionId, "write", () => { forbiddenWrites++; })).denied, true, "故障测试的验收人不能自行补文件");
    assert.equal(forbiddenWrites, 0);
    const verdict = async (turn: number) => {
      const request = lastDelivery(acceptor);
      assert.match(request, /唯一角色是独立验收者/);
      assert.match(request, /> \[E2E-GAP-INJECT\]/, "执行目标作为引用资料，不能再次给验收人派工");
      const roundId = /轮次 (round_[a-z0-9]+)/.exec(request)![1];
      await tick(acceptor.sessionId, turn, "读取产物");
      const actual = await fs.readFile(path, "utf8");
      assert.equal(actual.trimEnd().split(/\r?\n/).slice(0, 4).join("\n"), GAP_BODY);
      if (turn === 2) fake.emitSessionEvent(acceptor.sessionId, { type: "tool/result", data: { turn, message: { role: "tool", toolCallId: "replay-read", content: [{ type: "text", text: "x".repeat(2313) }] } } });
      const outcome = actual.trimEnd().split(/\r?\n/).at(-1) === marker ? "pass" : "gap";
      await tick(acceptor.sessionId, turn, `<longtask_verdicts>${JSON.stringify({ roundId, artifactHashes: frozenHashes(request), verdicts: [
        { criterionId: "c1", outcome: "pass", evidence: "前四行逐行符合正文模板，校验末行按c2判定", gap: null, verdictBy: REVIEWER.relPath, criteriaVersion: 1 },
        { criterionId: "c2", outcome, evidence: `实际末行=${actual.trimEnd().split(/\r?\n/).at(-1)}`, gap: outcome === "gap" ? "缺少校验末行" : null, verdictBy: REVIEWER.relPath, criteriaVersion: 1 },
      ] })}</longtask_verdicts>`);
      fake.emitSessionEvent(acceptor.sessionId, turnEndEvent(turn));
      return { roundId, outcome, hashes: frozenHashes(request) };
    };
    const first = await verdict(1);
    assert.equal(first.outcome, "gap", "根据真实读取的缺项文件产生结论");
    assert.ok(Object.values(first.hashes).includes(createHash("sha256").update(badContent).digest("hex")));
    await fake.waitFor(() => api.getTask(taskId)?.status === "rework");
    await fake.waitFor(() => /修改前先用read读取当前文件/.test(lastDelivery(worker)));
    assert.match(lastDelivery(worker), /修改前先用read读取当前文件/);
    assert.match(lastDelivery(worker), /不要依赖上轮缓存直接edit/);
    fake.emitSessionEvent(worker.sessionId, { type: "turn/start", data: { turn: 2 } });
    await tick(worker.sessionId, 2, "先read取得当前文件和版本");
    assert.equal(await fs.readFile(path, "utf8"), badContent);
    await tick(worker.sessionId, 2, "依据最新版本edit补齐末行");
    await fs.writeFile(path, content);
    await tick(worker.sessionId, 2, '<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>');
    fake.emitSessionEvent(worker.sessionId, turnEndEvent(2));
    await fake.waitFor(() => api.getTask(taskId)?.status === "awaiting_acceptance");
    await fake.waitFor(() => /轮次 (round_[a-z0-9]+)/.exec(lastDelivery(acceptor))?.[1] !== first.roundId);
    fake.emitSessionEvent(acceptor.sessionId, { type: "turn/start", data: { turn: 2 } });
    const second = await verdict(2);
    assert.equal(second.outcome, "pass");
    assert.notEqual(first.roundId, second.roundId);
    assert.equal(Object.values(second.hashes).includes(createHash("sha256").update(badContent).digest("hex")), false, "再次冻结不能混入历史缺项版本");
    await fake.waitFor(() => api.getTask(taskId)?.status === "delivered");
    const deadline = Date.now() + 4000;
    while (!(await fs.readdir(transcriptDir)).some((n) => n.startsWith("done-"))) { if (Date.now() > deadline) throw Error("消费者未通过故障证据自检"); await fake.flush(); }
    const state = await load();
    assert.equal(Object.values(state.collections.artifacts).filter((a: any) => a.taskId === taskId).length, 2, "保留两版历史登记");
    assert.equal(api.getBudget(taskId)?.settledTokens, total);
    assert.equal(api.getBudget(taskId)?.reservedTokens, 0);
    assert.ok(total <= 260000);
    assert.equal(total, 228271, "使用实机已发生的成本及最终最坏输出，不以每步1.5万的理想成本证明通过");
    assert.equal(sampleIndex, samples.length);
    assert.equal(fake.logs.some((l) => l.includes("bound-exceeded")), false);
    const runs = (await fs.readdir(transcriptDir)).filter((n) => n.endsWith(".jsonl"));
    const rows = (await fs.readFile(join(transcriptDir, runs.at(-1)!), "utf8")).trim().split("\n").map((s) => JSON.parse(s));
    const end = rows.find((e) => e.kind === "scenario_end");
    assert.equal(end.coverage.passed, true);
    assert.deepEqual(end.fault.phases, { injected: 1, firstFrozen: 2, gap: 3, repaired: 4, secondFrozen: 5 });
    assert.equal(end.fault.worker, worker.sessionId);
    assert.equal(fake.toolGuards.size, 1, "结束移除测试guard，只保留产品委派guard");
  } finally {
    for (const stop of harnessTeardowns) await stop();
    for (const stop of fake.teardowns) await stop();
  }
});

test("阶段2 专家直连：回复里的 longtask_message 只发给对方，summary 进 Router 摘要", async () => {
  const { fake, api, dir } = await makeApp();
  const second = expertRef("research/worker2.md", "Worker2");
  await fs.writeFile(join(dir, "experts", "research", "worker2.md"), "---\nname: Worker2\ndescription: 二号执行专家\n---\n\n# Worker2\n", "utf8");
  const task = (await api.propose({
    goal: "两位专家协作完成比较与推荐",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR, second],
    acceptor: REVIEWER,
    budget: { limitTokens: 500_000, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();

  // 不能按"消息里是否出现路径"找会话：简报的协作专家列表会让两位专家的简报互相包含对方路径
  const executors = [...fake.agentsById.values()];
  const a = executors[0]!;
  const b = executors[1]!;
  assert.ok(b, "两位执行专家都收到初始派工");
  const bBefore = b.messages.length;

  await fs.writeFile(join(dir, "d"), "产物内容\n", "utf8");
  fake.emitSessionEvent(
    a.sessionId,
    assistantEvent(
      [
        "统计口径已核对。",
        '<longtask_message to="research/worker2.md" summary="比例口径已修正，见表 3">',
        "分母应取全部受访者，不是完成答题的人数；请按此更新比较表。",
        "</longtask_message>",
        '<longtask_report>{"done": false, "next": ["继续"], "waiting": null}</longtask_report>',
      ].join("\n"),
      1,
    ),
  );
  fake.emitSessionEvent(a.sessionId, turnEndEvent(1));
  await fake.waitFor(() => b.messages.length > bBefore);

  const direct = b.messages.at(-1)!;
  assert.ok(direct.includes("专家直连"), "对方收到的是一条定向消息");
  assert.ok(direct.includes("分母应取全部受访者"), "正文完整送达");
  assert.ok(!direct.includes("<longtask_message"), "协议块不原样转发");

  // 摘要事务在投递之后落盘：等它出现再断言（避免把时序当失败）
  await fake.waitFor(() => ((api as unknown as { getRouterDigest(id: string): { latestSummaries: unknown[] } }).getRouterDigest(task.id)?.latestSummaries.length ?? 0) > 0);
  const digest = (api as unknown as { getRouterDigest(id: string): { latestSummaries: Array<{ summary: string }> } }).getRouterDigest(task.id);
  assert.ok(digest);
  assert.equal(digest.latestSummaries[0]!.summary, "比例口径已修正，见表 3", "Router 只看到摘要");
});

// ---------------------------------------------------------------------------
// 阶段2 负向回归（审查 24：唤醒 / 中间产物 / 换人资格 / 重复结束事件）
// ---------------------------------------------------------------------------

/** 双执行专家任务：返回两位专家的会话（按创建顺序，不用消息内容匹配） */
async function makePairTask(budget = 500_000, dependent = false) {
  const app = await makeApp();
  const { fake, api, dir } = app;
  const second = expertRef("research/worker2.md", "Worker2");
  await fs.writeFile(join(dir, "experts", "research", "worker2.md"), "---\nname: Worker2\ndescription: 二号执行专家\n---\n\n# Worker2\n", "utf8");
  const task = (await api.propose({
    goal: "两位专家协作完成比较与推荐",
    criteria: dependent ? [{ id: "c1", text: "比较表" }, { id: "c2", text: "推荐" }] : [{ id: "c1" }],
    deliverables: dependent ? ["d", "recommendation"] : ["d"],
    ...(dependent ? { assignments: [
      { relPath: EXECUTOR.relPath, deliverables: ["d"] },
      { relPath: second.relPath, deliverables: ["recommendation"], waitFor: { fromRelPath: EXECUTOR.relPath, artifactRefs: ["d"] } },
    ] } : {}),
    executors: [EXECUTOR, second],
    acceptor: REVIEWER,
    budget: { limitTokens: budget, strict: true },
  })) as { id: string };
  await api.confirm(task.id);
  await fake.flush();
  const [a, b] = [...fake.agentsById.values()];
  return { ...app, task, a: a!, b: b! };
}

test("阶段2 唤醒：收件人已结束一轮在等待交接，定向消息仍能开启新一轮", async () => {
  const { fake, task, a, b } = await makePairTask();
  // B 先结束一轮，进入等待交接（waiting 非空 → 决策为 wait，不会触发报告催办）
  fake.emitSessionEvent(b.sessionId, assistantEvent('我在等 A 的口径。\n<longtask_report>{"done": false, "next": [], "waiting": "等 A 的统计口径"}</longtask_report>', 1));
  fake.emitSessionEvent(b.sessionId, turnEndEvent(1));
  await fake.flush();
  const before = b.messages.length;

  fake.emitSessionEvent(
    a.sessionId,
    assistantEvent(
      [
        '<longtask_message to="research/worker2.md" summary="口径已定">',
        "分母取全部受访者，请按此更新。",
        "</longtask_message>",
        '<longtask_report>{"done": false, "next": ["等待 B 补充"], "waiting": null}</longtask_report>',
      ].join("\n"),
      1,
    ),
  );
  fake.emitSessionEvent(a.sessionId, turnEndEvent(1));

  await fake.waitFor(() => b.messages.length > before);
  const delivered = b.messages.slice(before).join("\n");
  assert.ok(delivered.includes("专家直连"), "已结束一轮的收件人必须被唤醒");
  assert.ok(delivered.includes("分母取全部受访者"), "正文送达");
  void task;
});

test("阶段2 中间产物：done=false 也要登记并随交接带上引用", async () => {
  const { fake, api, dir, task, a } = await makePairTask();
  await fs.writeFile(join(dir, "d"), "A 写的中间产物\n", "utf8");

  fake.emitSessionEvent(
    a.sessionId,
    assistantEvent(
      [
        '<longtask_message to="research/worker2.md" summary="初稿已出" artifacts="d">',
        "比较表初稿已落盘，请补充推荐结论。",
        "</longtask_message>",
        '<longtask_report>{"done": false, "next": ["等待 B 补充"], "waiting": null}</longtask_report>',
      ].join("\n"),
      1,
    ),
  );
  fake.emitSessionEvent(a.sessionId, turnEndEvent(1));

  const digestOf = () =>
    (api as unknown as {
      getRouterDigest(id: string): { artifacts: Array<{ location: string; owner: string | null }>; latestSummaries: Array<{ artifactRefs: string[] }> } | null;
    }).getRouterDigest(task.id);
  await fake.waitFor(() => (digestOf()?.latestSummaries.length ?? 0) > 0);
  const digest = digestOf()!;
  assert.equal(digest.artifacts.length, 1, "done=false 的中间产物也必须登记");
  assert.equal(digest.artifacts[0]!.owner, EXECUTOR.relPath, "产物归属写作者");
  assert.ok(digest.latestSummaries[0]!.artifactRefs.some((p) => p.endsWith("d")), "交接消息带产物引用");
});

test("阶段2 换人资格：被换下的执行者会话停止，且请求准入被拒", async () => {
  const { fake, api, dir, a, b, task } = await makePairTask();
  const third = expertRef("research/worker3.md", "Worker3");
  await fs.writeFile(join(dir, "experts", "research", "worker3.md"), "---\nname: Worker3\ndescription: 三号执行专家\n---\n\n# Worker3\n", "utf8");

  await (api as unknown as {
    changeExecutor(id: string, change: { add: unknown; remove?: string; reason: string }): Promise<void>;
  }).changeExecutor(task.id, { add: third, remove: "research/worker2.md", reason: "B 缺少统计背景" });
  await fake.flush();

  assert.equal(b.stopped, true, "被换下的执行者会话必须停止");
  assert.equal(await fake.emitPreStep(b.sessionId), false, "被撤权的会话不得进入模型步骤");
  assert.equal(await fake.emitPreStep(a.sessionId), true, "仍在职的执行者不受影响");
});

test("阶段2 重复结束事件并发到达：只产生一次续做投递", async () => {
  const { fake, a } = await makePairTask();
  fake.emitSessionEvent(
    a.sessionId,
    assistantEvent('继续中。\n<longtask_report>{"done": false, "next": ["补充推荐"], "waiting": null}</longtask_report>', 1),
  );
  // 同一结束事件同步重放两次：两条异步处理链并发
  fake.emitSessionEvent(a.sessionId, turnEndEvent(1));
  fake.emitSessionEvent(a.sessionId, turnEndEvent(1));

  await fake.waitFor(() => a.messages.some((m) => m.includes("继续执行，缺项")));
  await fake.flush();
  const continues = a.messages.filter((m) => m.includes("继续执行，缺项"));
  assert.equal(continues.length, 1, "同一结束事件只能产生一次续做");
});

test("审查25 C4：被换下的会话迟到工具与迟到报告都不能生效", async () => {
  const { fake, api, dir, task, a, b } = await makePairTask();
  const third = expertRef("research/worker3.md", "Worker3");
  await fs.writeFile(join(dir, "experts", "research", "worker3.md"), "---\nname: Worker3\ndescription: 三号执行专家\n---\n\n# Worker3\n", "utf8");
  await (api as unknown as {
    changeExecutor(id: string, change: { add: unknown; remove?: string; reason: string }): Promise<void>;
  }).changeExecutor(task.id, { add: third, remove: "research/worker2.md", reason: "B 缺少统计背景" });
  await fake.flush();
  assert.equal(b.stopped, true);

  // 迟到工具：guard 必须拒绝
  const denied = await fake.emitTool(b.sessionId, "write", async () => {
    await fs.writeFile(join(dir, "late.md"), "B 迟到的写入\n", "utf8");
  });
  assert.equal(denied.denied, true, "撤权会话不得执行工具");
  assert.ok(String(denied.reason).includes("已被替换"), "拒绝原因要说明执行资格已撤销");

  // 迟到报告：即使宿主仍送来 turn/end，也不能登记产物或驱动任务
  const before = { artifacts: api.getRouterDigest(task.id)?.artifacts ?? [] };
  await fs.writeFile(join(dir, "d2.md"), "旧 B 迟到的产物\n", "utf8");
  fake.emitSessionEvent(b.sessionId, assistantEvent('迟到报告\n<longtask_report>{"done": true, "next": [], "waiting": null}</longtask_report>', 1));
  fake.emitSessionEvent(b.sessionId, turnEndEvent(1));
  await fake.flush();
  const after = { artifacts: api.getRouterDigest(task.id)?.artifacts ?? [] };
  assert.equal(after.artifacts.length, before.artifacts.length, "迟到的产物登记必须被拒绝");
  const status = (api.getTask(task.id) as { status: string }).status;
  assert.equal(status, "active", "迟到报告不得把任务推进到验收");
  void a;
});

test("审查25 C5：B→C→B 后，旧 B 会话仍然没有执行资格", async () => {
  const { fake, api, dir, task, b } = await makePairTask();
  const third = expertRef("research/worker3.md", "Worker3");
  await fs.writeFile(join(dir, "experts", "research", "worker3.md"), "---\nname: Worker3\ndescription: 三号执行专家\n---\n\n# Worker3\n", "utf8");
  const api2 = api as unknown as {
    changeExecutor(id: string, change: { add: unknown; remove?: string; reason: string }): Promise<void>;
  };
  const bRef = expertRef("research/worker2.md", "Worker2");
  await api2.changeExecutor(task.id, { add: third, remove: "research/worker2.md", reason: "先换成 C" });
  await api2.changeExecutor(task.id, { add: bRef, remove: "research/worker3.md", reason: "再换回 B" });
  await fake.flush();

  assert.equal(await fake.emitPreStep(b.sessionId), false, "旧 B 会话必须一直被拒绝，不能因路径回到名单而复活");
  const executors = [...fake.agentsById.keys()].filter((id) => id !== b.sessionId);
  assert.ok(executors.length >= 2, "新 B 与其余专家会话存在");
});

test("审查26 D2：真实服务可查原文、按依据对账并完成幂等或合法补投", async () => {
  const { fake, api, dir, task, b } = await makePairTask();
  try {
    const original = b.followup;
    b.followup = (message) => { original(message); throw Error("已入队但确认失败"); };
    const first = await api.sendExpertMessage(task.id, EXECUTOR.relPath, "research/worker2.md", "核对这次交接", { actionId: "ack-lost", sourceTurnId: "source-1" });
    assert.equal(first.delivery, "unknown");
    const stored = api.getExpertMessage(task.id, first.messageId!)!;
    assert.equal(stored.body, "核对这次交接");
    await assert.rejects(api.reconcileExpertMessage(task.id, first.messageId!, "delivered", ""), /依据/);
    await api.reconcileExpertMessage(task.id, first.messageId!, "delivered", "已核对宿主历史，ack-lost 入队一次");
    const replay = await api.sendExpertMessage(task.id, EXECUTOR.relPath, "research/worker2.md", "核对这次交接", { actionId: "ack-lost" });
    assert.equal(replay.duplicate, true);
    assert.equal(b.messages.filter((m) => m.includes("核对这次交接")).length, 1);
    b.followup = () => { throw Error("确认未入队"); };
    const second = await api.sendExpertMessage(task.id, EXECUTOR.relPath, "research/worker2.md", "第二次交接", { actionId: "not-enqueued" });
    await api.reconcileExpertMessage(task.id, second.messageId!, "undelivered", "按宿主历史确认 not-enqueued 未入队");
    b.followup = original;
    assert.equal((await api.sendExpertMessage(task.id, EXECUTOR.relPath, "research/worker2.md", "第二次交接", { actionId: "not-enqueued" })).delivery, "delivered");
    assert.equal(b.messages.filter((m) => m.includes("第二次交接")).length, 1);
    const state = JSON.parse(await fs.readFile(join(dir, "state.json"), "utf8"));
    const audits = (Object.values(state.events) as Array<{ type: string; payload: { evidence: string } }>).filter((e) => e.type === "expert.message_reconciled");
    assert.equal(audits.length, 2); assert.ok(audits.every((e) => e.payload.evidence.length > 0));
  } finally { for (const stop of fake.teardowns) await stop(); }
});

test("审查26 D3：撤权后迟到读取不误发纠偏，真实usage仍结算", async () => {
  const { fake, api, dir, task, a, b } = await makePairTask();
  try {
    assert.equal(await fake.emitPreStep(b.sessionId), true);
    const third = expertRef("research/worker3.md", "Worker3");
    await fs.writeFile(join(dir, "experts", "research", "worker3.md"), "---\nname: Worker3\ndescription: worker3\n---\n# Worker3");
    await (api as unknown as { changeExecutor: TaskService["changeExecutor"] }).changeExecutor(task.id, { add: third, remove: "research/worker2.md", reason: "换下 B" });
    for (let i = 0; i < 7; i++) fake.emitSessionEvent(b.sessionId, { type: "tool/call", data: { name: "read", turn: 1 } });
    fake.emitSessionEvent(b.sessionId, assistantUsageEvent(1, { inputTokens: 1000, outputTokens: 100, cacheReadTokens: 0, cacheWriteTokens: 0 }));
    await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 1100);
    assert.equal(a.messages.some((m) => m.includes("【读预算用尽】")), false);
    const log = await fs.readFile(join(dir, "plugin.log"), "utf8");
    assert.equal(log.includes("budget:read-stop"), false);
    assert.equal((await fake.emitTool(b.sessionId, "write", () => { throw Error("不得执行"); })).denied, true);
  } finally { for (const stop of fake.teardowns) await stop(); }
});

test("审查29 pair：调度等待零模型调用，交接后单次启动并独立验收", async () => {
  const { createPairFixture, PAIR_TOOL } = await import(pathToFileURL(join(process.cwd(), "..", "dsh-longtask-e2e", "pair-fixture.mjs")).href);
  const { extractPairEvidence, assessScenario } = await import(pathToFileURL(join(process.cwd(), "..", "dsh-longtask-e2e", "evidence.mjs")).href);
  const { fake, api, dir, task, a } = await makePairTask(500_000, true);
  let b: ReturnType<typeof findAgentByMessage>;
  let fixture: { dispose(): void } | undefined;
  try {
    assert.throws(() => api.replayExecutorTurnEnd(task.id, a.sessionId, "未观察到的事件"), /已观测/);
    let globalSenderEnds = 0;
    fake.ctx.on("session/event", (session: { id: string }, event: { type: string }) => {
      if (session.id === a.sessionId && event.type === "turn/end") globalSenderEnds++;
    });
    const registered = new Map<string, { execute(args: unknown, exec: unknown): Promise<string> }>();
    const fx = createPairFixture({ runId: "fixture-run", sender: EXECUTOR.relPath, recipient: "research/worker2.md", artifactPath: join(dir, "d"),
      getDependency: (...args: Parameters<TaskService["getExecutorDependency"]>) => api.getExecutorDependency(...args),
      replayTurnEnd: (...args: Parameters<LongtaskApi["replayExecutorTurnEnd"]>) => api.replayExecutorTurnEnd(...args),
      readSessions: () => Object.values(JSON.parse(requireState()).collections.taskSessions[task.id]) });
    function requireState() { return requireFs.readFileSync(join(dir, "state.json"), "utf8"); }
    const requireFs = await import("node:fs");
    fx.install({ ...fake.ctx, get: () => ({ register(tool: { name: string; execute(args: unknown, exec: unknown): Promise<string> }) { registered.set(tool.name, tool); return () => { registered.delete(tool.name); }; } }),
      emit: (_name: string, session: { id: string }, event: Record<string, unknown>) => fake.emitSessionEvent(session.id, event) });
    fixture = fx; fx.bind(task.id);
    assert.equal(fake.agentsById.size, 1, "B 不启动模型来报告等待");
    assert.equal(api.getExecutorDependency(task.id, "research/worker2.md").state, "waiting");
    await fs.writeFile(join(dir, "d"), "# 比较表\n方案A优于B\n");
    fake.emitSessionEvent(a.sessionId, { type: "tool/call", data: { turn: 1, name: "write", callId: "A-write", arguments: JSON.stringify({ file_path: join(dir, "d") }) } });
    fake.emitSessionEvent(a.sessionId, { type: "tool/result", data: { message: { toolCallId: "A-write", isError: false, content: [] } } });
    assert.equal(JSON.parse(await registered.get(PAIR_TOOL)!.execute({ runId: "fixture-run" }, { agent: { session: { id: a.sessionId } }, signal: new AbortController().signal })).ready, true);
    let incoming: Parameters<AgentHandle["agent"]["followup"]>[0] | undefined;
    const agents = fake.ctx.get<{ create(options: unknown): Promise<AgentHandle> }>("agents")!;
    const create = agents.create.bind(agents);
    agents.create = async (options) => {
      const handle = await create(options);
      if (!b) {
        b = fake.agentsById.get(handle.agent.session.id)!;
        const followup = handle.agent.followup.bind(handle.agent);
        handle.agent.followup = (message) => { incoming = message; followup(message); };
      }
      return handle;
    };
    fake.emitSessionEvent(a.sessionId, assistantEvent('<longtask_message to="research/worker2.md" summary="比较表初稿已交接" artifacts="d">请读取比较表，再补充推荐</longtask_message>\n<longtask_report>{"done":false,"next":[],"waiting":"等B推荐"}</longtask_report>', 1));
    fake.emitSessionEvent(a.sessionId, turnEndEvent(1));
    await fake.waitFor(() => !!incoming && (api.getRouterDigest(task.id)?.latestSummaries.length ?? 0) > 0);
    assert.equal(fx.snapshot().eventCount, 2);
    assert.equal(globalSenderEnds, 1, "longtask 重放不能广播给宿主持久化监听器");
    assert.ok(b);
    assert.equal(b.messages.filter((m) => m.includes("【专家直连】")).length, 1);
    fake.emitSessionEvent(b.sessionId, { type: "user/message", data: incoming! });
    fake.emitSessionEvent(b.sessionId, { type: "tool/call", data: { turn: 2, step: 1, callId: "read-table", name: "read", arguments: JSON.stringify({ file_path: join(dir, "d") }) } });
    const table = await fs.readFile(join(dir, "d"), "utf8");
    fake.emitSessionEvent(b.sessionId, { type: "tool/result", data: { turn: 2, message: { role: "tool", toolCallId: "read-table", source: { kind: "tool", callId: "read-table" }, isError: false, content: [{ type: "text", text: table }] } } });
    await fs.writeFile(join(dir, "recommendation"), "推荐方案A：" + table);
    fake.emitSessionEvent(b.sessionId, { type: "tool/call", data: { turn: 2, name: "write", callId: "B-write", arguments: JSON.stringify({ file_path: join(dir, "recommendation") }) } });
    fake.emitSessionEvent(b.sessionId, { type: "tool/result", data: { message: { toolCallId: "B-write", isError: false, content: [] } } });
    fake.emitSessionEvent(b.sessionId, assistantEvent('<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>', 2));
    fake.emitSessionEvent(b.sessionId, turnEndEvent(2));
    await fake.waitFor(() => !!findAgentByMessage(fake, "独立验收请求"));
    const acceptor = findAgentByMessage(fake, "独立验收请求")!;
    const request = lastDelivery(acceptor);
    const artifactHashes = frozenHashes(request);
    const roundId = /轮次 (round_[a-z0-9]+)/.exec(request)![1]!;
    const verdicts = ["c1", "c2"].map((criterionId) => ({ criterionId, outcome: "pass", evidence: "已核对比较表和推荐", gap: null, criteriaVersion: 1,
      verdictBy: REVIEWER.relPath, artifactHashes, createdAt: new Date().toISOString() }));
    fake.emitSessionEvent(acceptor.sessionId, assistantEvent(`<longtask_verdicts>${JSON.stringify({ roundId, verdicts, artifactHashes })}</longtask_verdicts>`, 1));
    fake.emitSessionEvent(acceptor.sessionId, turnEndEvent(1));
    await fake.waitFor(() => api.getTask(task.id)?.status === "delivered");
    assert.throws(() => api.replayExecutorTurnEnd(task.id, a.sessionId, fx.snapshot().sourceTurnId), /已观测/);
    const state = JSON.parse(requireState());
    const observations = (await fs.readFile(join(dir, "plugin.log"), "utf8")).split("\n").filter((l) => l.includes("collaboration:")).map((l) => JSON.parse(l.slice(l.indexOf("collaboration:") + 14)));
    const stage2 = extractPairEvidence({ taskId: task.id, state, digest: api.getRouterDigest(task.id), observations, replay: fx.snapshot() });
    const result = assessScenario({ scenario: "pair", status: api.getTask(task.id)?.status, taskId: task.id, budget: api.getBudget(task.id), accounting: { verified: true, actualTokens: 0, descendants: [] }, controls: { boundExceeded: 0 }, stage2 });
    assert.equal(result.passed, true, result.reasons.join("；"));
    for (const missingType of ["expert.waiting", "expert.dependency_ready"]) {
      const broken = structuredClone(state);
      for (const [key, value] of Object.entries(broken.events) as Array<[string, { type: string }]>) if (value.type === missingType) delete broken.events[key];
      const evidence = extractPairEvidence({ taskId: task.id, state: broken, digest: api.getRouterDigest(task.id), observations, replay: fx.snapshot() });
      assert.equal(evidence.waitedBeforeHandoff, false, `缺少 ${missingType} 时不能伪装调度等待闭环`);
    }
  } finally { fixture?.dispose(); for (const stop of fake.teardowns) await stop(); }
});

test("审查29：交接未知时 B 的模型、工具和提前完成均被拒，补投后恢复", async () => {
  const { fake, api, dir, task } = await makePairTask(500_000, true);
  try {
    const table = join(dir, "d"); await fs.writeFile(table, "A 的表");
    await (api as unknown as { registerArtifact: TaskService["registerArtifact"] }).registerArtifact(task.id, table, createHash("sha256").update("A 的表").digest("hex"), EXECUTOR.relPath);
    const agents = fake.ctx.get<{ create(options: unknown): Promise<AgentHandle> }>("agents")!;
    const create = agents.create.bind(agents);
    let followup!: AgentHandle["agent"]["followup"];
    agents.create = async (options) => { const h = await create(options); followup = h.agent.followup.bind(h.agent); h.agent.followup = () => { throw Error("收件确认未知"); }; return h; };
    const opts = { actionId: "unknown-dependency", artifactRefs: [table] };
    const sent = await api.sendExpertMessage(task.id, EXECUTOR.relPath, "research/worker2.md", "按指定版本接手", opts);
    assert.equal(sent.delivery, "unknown");
    const b = [...fake.agentsById.values()][1]!;
    assert.equal(await fake.emitPreStep(b.sessionId), false);
    assert.equal((await fake.emitTool(b.sessionId, "write", () => { throw Error("不应执行"); })).denied, true);
    assert.equal(api.getBudget(task.id)!.reservedTokens, 0);
    fake.emitSessionEvent(b.sessionId, assistantEvent('<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>', 1));
    fake.emitSessionEvent(b.sessionId, turnEndEvent(1));
    await api.reconcileExpertMessage(task.id, sent.messageId!, "undelivered", "已核对没有消息进入收件会话");
    b.followup = followup; agents.create = create;
    await api.sendExpertMessage(task.id, EXECUTOR.relPath, "research/worker2.md", "按指定版本接手", opts);
    assert.equal(api.getExecutorDependency(task.id, "research/worker2.md").state, "ready");
    assert.equal(await fake.emitPreStep(b.sessionId), true);
    fake.emitSessionEvent(b.sessionId, assistantUsageEvent(2, { inputTokens: 1000, outputTokens: 100, cacheReadTokens: 0, cacheWriteTokens: 0 }));
    await fake.waitFor(() => api.getBudget(task.id)?.settledTokens === 1100);
    assert.equal(api.getTask(task.id)!.status, "active", "交接前的旧完成报告不能在解锁后迟到生效");
    assert.equal(fake.agentsById.size, 2, "没有创建验收人");
  } finally { for (const stop of fake.teardowns) await stop(); }
});

test("审查29：真实工具写入观测归属，不因另一位专家先提交而抢认文件", async () => {
  const { fake, api, dir } = await makeApp();
  try {
    const second = expertRef("research/worker2.md", "Worker2");
    await fs.writeFile(join(dir, "experts", "research", "worker2.md"), "---\nname: Worker2\ndescription: worker2\n---\n# Worker2");
    const task = await api.propose({ goal: "两份交付", criteria: [{ id: "c1", text: "交付" }], deliverables: ["d", "recommendation"], executors: [EXECUTOR, second], acceptor: REVIEWER, budget: { limitTokens: 500000, strict: true } });
    await api.confirm(task.id);
    const [a, b] = [...fake.agentsById.values()];
    for (const [agent, path, callId] of [[a!, "d", "A-write"], [b!, "recommendation", "B-write"]] as const) {
      await fs.writeFile(join(dir, path), callId);
      fake.emitSessionEvent(agent.sessionId, { type: "tool/call", data: { name: "write", callId, arguments: JSON.stringify({ file_path: join(dir, path) }) } });
      fake.emitSessionEvent(agent.sessionId, { type: "tool/result", data: { message: { toolCallId: callId, isError: false, content: [] } } });
    }
    fake.emitSessionEvent(b!.sessionId, assistantEvent('<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>', 1));
    fake.emitSessionEvent(b!.sessionId, turnEndEvent(1));
    await fake.waitFor(() => !!findAgentByMessage(fake, "独立验收请求"));
    const rows = api.getRouterDigest(task.id)!.artifacts;
    assert.equal(rows.find((a) => a.location === join(dir, "d"))!.owner, EXECUTOR.relPath);
    assert.equal(rows.find((a) => a.location === join(dir, "recommendation"))!.owner, second.relPath);
  } finally { for (const stop of fake.teardowns) await stop(); }
});

// ---------------------------------------------------------------------------
// 阶段3 · 任务6：同数据目录二次 apply（模拟进程重启后的自动接续）
// ---------------------------------------------------------------------------

/** 用相同持久目录做同进程重新装配；不能替代真实杀进程验收。 */
function makeAppLike(dir: string) {
  const fake = makeFakeHost({});
  apply(fake.ctx, {
    dataDir: dir,
    agencyAgentsRoot: join(dir, "experts"),
    probeOnStart: false,
    watchdogIntervalMs: 3600_000,
  });
  return fake;
}

test("阶段3 生命周期：重启后自动接续运行中任务，不拉起已取消任务", async () => {
  const { fake, api, dir } = await makeApp();

  const running = (await api.propose({
    goal: "重启后要接着跑的任务",
    criteria: [{ id: "c1" }],
    deliverables: ["d"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 400_000, strict: true },
  })) as { id: string };
  await api.confirm(running.id);
  await fake.flush();

  const cancelled = (await api.propose({
    goal: "会被取消的任务",
    criteria: [{ id: "c1" }],
    deliverables: ["d2"],
    executors: [EXECUTOR],
    acceptor: REVIEWER,
    budget: { limitTokens: 200_000, strict: true },
  })) as { id: string };
  await api.confirm(cancelled.id);
  await fake.flush();
  await api.cancel(cancelled.id);
  await fake.flush();

  // 先排空并关闭旧实例，再从同一持久目录重新装配；此处没有杀进程。
  for (const stop of fake.teardowns) await stop();
  const second = makeAppLike(dir);
  try {
  await second.waitFor(() => Boolean(second.provided.get("longtask")));
  const api2 = second.provided.get("longtask") as LongtaskApi;

  await second.waitFor(() => [...second.agentsById.values()].some((a) => a.messages.some((m) => m.includes("重启恢复"))));
  const recovered = [...second.agentsById.values()].filter((a) => a.messages.some((m) => m.includes("重启恢复")));
  assert.equal(recovered.length, 1, "每位在职执行者只收到一条恢复指令，不重复派工");
  assert.equal((api2.getTask(running.id) as { status: string }).status, "active", "运行中任务保持可继续");

  const cancelledTouched = [...second.agentsById.values()].filter((a) => a.messages.some((m) => m.includes("会被取消的任务")));
  assert.equal(cancelledTouched.length, 0, "已取消任务不得被重启拉起");

  // 恢复指令必须带"先核对产物、别重复外部动作"的约束，而不是空转一句"继续"
  const resumeMsg = recovered[0]!.messages.find((m) => m.includes("重启恢复"))!;
  assert.ok(resumeMsg.includes("不要重复已经完成的写入"), "恢复必须要求先核对再动作");
  const persisted = api2.getTask(running.id) as unknown as { waitingOn?: unknown };
  assert.equal(persisted.waitingOn ?? null, null, "恢复后不应残留等待镜像");
  } finally { for (const stop of second.teardowns) await stop(); }
});


const moneyRoute = {provider: "deepseek-official", model: "deepseek-flash", maxTokens: 8000};
async function makeMoneyApp(limit = "5", config: Partial<LongtaskConfig> = {}) {
  const app = await makeApp({modelPrices:DEFAULT_PRICES.map(p=>({...p,verifiedAt:"2020-01-01",expiresAt:"2099-01-01"})),...config});
  const t = await app.api.propose({goal:"金额任务", criteria:[{id:"c1",text:"交付"}], deliverables:[join(app.dir,"money.md")], executors:[EXECUTOR], acceptor:REVIEWER,
    budget:{limitTokens:Number.MAX_SAFE_INTEGER,strict:true,money:{currency:"CNY",limitAmount:limit}}});
  await app.api.confirm(t.id); return {...app, taskId:t.id, worker:[...app.fake.agentsById.values()][0]!};
}
test("金额真实入口：实际request路由计费、缓存分项、失败重试和事件重放不漏不重",async()=>{
  const {fake,api,taskId,worker}=await makeMoneyApp();try{
    assert.equal(await fake.emitPreStep(worker.id),true);await fake.emitRequest(worker.id,moneyRoute);
    assert.ok(Number(api.getMoneyBudget(taskId)!.reservedAmount)>0);
    const event={seq:8300,...assistantUsageEvent(1,{inputTokens:1000,outputTokens:500,cacheReadTokens:2000,cacheWriteTokens:300})};
    fake.emitSessionEvent(worker.id,event);await fake.waitFor(()=>api.getMoneyBudget(taskId)?.calculatedAmount==='0.00668');
    fake.emitSessionEvent(worker.id,structuredClone(event));await fake.flush();assert.equal(api.getMoneyBudget(taskId)!.calculatedAmount,'0.00668');
    await fake.emitRequest(worker.id,moneyRoute);
    fake.emitSessionEvent(worker.id,{seq:8301,...assistantUsageEvent(1,{inputTokens:1000,outputTokens:500,cacheReadTokens:2000,cacheWriteTokens:300})});
    await fake.waitFor(()=>api.getMoneyBudget(taskId)?.calculatedAmount==='0.01336');
    assert.equal(api.getModelExpenses(taskId).length,2);assert.equal(api.getMoneyBudget(taskId)!.reservedAmount,'0');
    assert.equal(api.getMoneyBudget(taskId)!.limitAmount,'5');assert.deepEqual(api.getBudget(taskId)!.grants,[]);
  }finally{for(const stop of fake.teardowns)await stop();}
});
test("金额不足在pre-step挡住；缺价的实际模型在传输前挡住且释放未发出的占位",async()=>{
  const low=await makeMoneyApp('0.1');try{
    assert.equal(await low.fake.emitPreStep(low.worker.id),false);await low.fake.waitFor(()=>low.api.getTask(low.taskId)?.pauseReason==='budget');
    assert.equal(low.api.getMoneyBudget(low.taskId)!.calculatedAmount,'0');assert.equal(low.api.getBudget(low.taskId)!.reservedTokens,0);
  }finally{for(const stop of low.fake.teardowns)await stop();}
  const app=await makeMoneyApp();try{
    assert.equal(await app.fake.emitPreStep(app.worker.id),true);
    await assert.rejects(()=>app.fake.emitRequest(app.worker.id,{...moneyRoute,model:'unknown'}),/缺少模型价格/);
    assert.equal(app.api.getMoneyBudget(app.taskId)!.reservedAmount,'0');assert.equal(app.api.getBudget(app.taskId)!.reservedTokens,0);
    assert.equal(app.api.getTask(app.taskId)!.pauseReason,'budget');
  }finally{for(const stop of app.fake.teardowns)await stop();}
});
test("金额收尾按人民币比例触发，Token内部兼容计数不影响收尾，也不自动追加",async()=>{
  const {fake,api,taskId,worker}=await makeMoneyApp('5',{wrapUpAtBudgetRatio:0.001,wrapUpMinStepsLeft:0});try{
    await fake.emitPreStep(worker.id);await fake.emitRequest(worker.id,moneyRoute);
    fake.emitSessionEvent(worker.id,{seq:8400,...assistantUsageEvent(1,{inputTokens:10000,outputTokens:1000,cacheReadTokens:0,cacheWriteTokens:0})});
    await fake.waitFor(()=>api.getTaskEvidence(taskId).eventOrder.some(id=>(api.getTaskEvidence(taskId).events[id] as {type?:string})?.type==='task.wrap_up_requested'));
    assert.equal(api.getMoneyBudget(taskId)!.calculatedAmount,'0.028');assert.equal(api.getMoneyBudget(taskId)!.limitAmount,'5');
    assert.deepEqual(api.getBudget(taskId)!.grants,[]);
  }finally{for(const stop of fake.teardowns)await stop();}
});
test("金额未知用量不会免费重试；取消后迟到用量仍核算且不唤醒",async()=>{
  const first=await makeMoneyApp();try{
    await first.fake.emitPreStep(first.worker.id);await first.fake.emitRequest(first.worker.id,moneyRoute);const reserved=first.api.getMoneyBudget(first.taskId)!.reservedAmount;
    first.fake.emitSessionEvent(first.worker.id,{seq:8500,type:'assistant/attempt',data:{turn:1,stream:[]}});
    await first.fake.waitFor(()=>first.api.getTask(first.taskId)?.pauseReason==='budget');
    assert.equal(first.api.getMoneyBudget(first.taskId)!.reservedAmount,reserved);await assert.rejects(()=>first.api.resume(first.taskId),/未核对/);
    await assert.rejects(()=>first.fake.emitRequest(first.worker.id,moneyRoute));
  }finally{for(const stop of first.fake.teardowns)await stop();}
  const second=await makeMoneyApp();try{
    await second.fake.emitPreStep(second.worker.id);await second.fake.emitRequest(second.worker.id,moneyRoute);await second.api.cancel(second.taskId);
    second.fake.emitSessionEvent(second.worker.id,{seq:8501,...assistantUsageEvent(1,{inputTokens:1000,outputTokens:500,cacheReadTokens:0,cacheWriteTokens:0})});
    await second.fake.waitFor(()=>second.api.getMoneyBudget(second.taskId)?.calculatedAmount==='0.006');assert.equal(second.api.getTask(second.taskId)!.status,'cancelled');
  }finally{for(const stop of second.fake.teardowns)await stop();}
});


test("金额重启恢复：未知在途费用保持原金额和预留，不自动建Worker",async()=>{
  const app=await makeMoneyApp();let second:ReturnType<typeof makeAppLike>|undefined;try{
    await app.fake.emitPreStep(app.worker.id);await app.fake.emitRequest(app.worker.id,moneyRoute);
    const before=app.api.getMoneyBudget(app.taskId)!;assert.ok(Number(before.reservedAmount)>0);
    for(const stop of app.fake.teardowns)await stop();
    second=makeAppLike(app.dir);await second.waitFor(()=>Boolean(second!.provided.get('longtask')));
    const restored=second.provided.get('longtask') as LongtaskApi;
    assert.equal(restored.getMoneyBudget(app.taskId)!.limitAmount,'5');assert.equal(restored.getMoneyBudget(app.taskId)!.reservedAmount,before.reservedAmount);
    assert.equal(restored.getTask(app.taskId)!.pauseReason,'budget');assert.equal(second.agentsById.size,0);
    await assert.rejects(()=>restored.resume(app.taskId),/未核对/);
  }finally{if(second)for(const stop of second.teardowns)await stop();else for(const stop of app.fake.teardowns)await stop();}
});
test("截断接续有界：连续第三次仍截断则暂停，原金额和已用量始终保留",async()=>{
  const {fake,api,taskId,worker}=await makeMoneyApp();try{
    for(let turn=1;turn<=3;turn++){
      assert.equal(await fake.emitPreStep(worker.id),true);await fake.emitRequest(worker.id,moneyRoute);
      fake.emitSessionEvent(worker.id,{seq:8600+turn*2,...assistantUsageEvent(turn,{inputTokens:1000,outputTokens:8000,cacheReadTokens:0,cacheWriteTokens:0},'单次回复截断')});
      fake.emitSessionEvent(worker.id,{seq:8601+turn*2,type:'turn/end',data:{turn,reason:{kind:'max-tokens'}}});
      if(turn<3)await fake.waitFor(()=>worker.messages.length===turn+1);else await fake.waitFor(()=>api.getTask(taskId)?.status==='paused');
    }
    assert.equal(worker.messages.length,3);assert.equal(api.getTask(taskId)!.pauseReason,'watchdog');assert.equal(api.getMoneyBudget(taskId)!.limitAmount,'5');
    assert.equal(api.getMoneyBudget(taskId)!.calculatedAmount,'0.198');assert.equal(api.getMoneyBudget(taskId)!.reservedAmount,'0');
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("客资截断回归：已结束命令不拦接续，并行Worker与原金额预留保持不变",async()=>{
  const {fake,api,dir}=await makeApp({modelPrices:DEFAULT_PRICES.map(p=>({...p,verifiedAt:"2020-01-01",expiresAt:"2099-01-01"}))});
  try{
    const peer=expertRef("research/peer.md","Peer");
    await fs.writeFile(join(dir,"experts/research/peer.md"),"---\nname: Peer\ndescription: 并行前端\n---\n实现前端");
    const t=await api.propose({goal:"并行实现客资系统",criteria:[{id:"c1",text:"交付"}],deliverables:["backend.md","frontend.md"],executors:[EXECUTOR,peer],acceptor:REVIEWER,
      assignments:[{relPath:EXECUTOR.relPath,deliverables:["backend.md"]},{relPath:peer.relPath,deliverables:["frontend.md"]}],
      budget:{limitTokens:Number.MAX_SAFE_INTEGER,strict:true,money:{currency:"CNY",limitAmount:"60"}}});
    await api.confirm(t.id);
    const [backend,frontend]=[...fake.agentsById.values()];assert.ok(backend&&frontend);
    for(const worker of [backend,frontend]){assert.equal(await fake.emitPreStep(worker.id),true);await fake.emitRequest(worker.id,moneyRoute);}
    for(const jobId of ["install-python","check-env","install-deps"]){
      fake.emitSessionEvent(backend.id,{type:"job/registered",data:{jobId,owner:backend.id,status:"running"}});
      fake.emitSessionEvent(backend.id,{type:"job/settled",data:{jobId,owner:backend.id,status:"completed"}});
    }
    await fake.waitFor(()=>api.getBackgroundJobs(t.id).filter(j=>j.settled&&!j.notified).length===3);
    fake.emitSessionEvent(backend.id,{seq:9100,...assistantUsageEvent(1,{inputTokens:166,outputTokens:8000,cacheReadTokens:31360,cacheWriteTokens:0},"截断处")});
    await fake.waitFor(()=>api.getModelExpenses(t.id).length===1);
    const remaining=api.getBudget(t.id)!.reservedTokens,moneyReserved=api.getMoneyBudget(t.id)!.reservedAmount;
    assert.ok(remaining>0,"前端请求仍在途");
    const end={seq:9101,type:"turn/end",data:{turn:1,reason:{kind:"max-tokens"}}};fake.emitSessionEvent(backend.id,end);
    await fake.waitFor(()=>backend.messages.length===2||api.getTask(t.id)?.status==="paused");
    assert.equal(api.getTask(t.id)!.status,"active","一次已结算的截断不应暂停整个并行任务");
    assert.equal(backend.messages.length,2);assert.match(backend.messages[1]!,/截断/);
    assert.equal(frontend.messages.length,1);assert.equal(frontend.stopped,false);assert.equal(backend.stopped,false);
    assert.equal(api.getBudget(t.id)!.reservedTokens,remaining);assert.equal(api.getMoneyBudget(t.id)!.reservedAmount,moneyReserved);
    assert.equal(api.getMoneyBudget(t.id)!.limitAmount,"60");assert.equal(api.getMoneyBudget(t.id)!.calculatedAmount,"0.0655864");assert.deepEqual(api.getBudget(t.id)!.grants,[]);
    await fake.waitFor(()=>api.getBackgroundJobs(t.id).every(j=>j.notified));
    fake.emitSessionEvent(backend.id,structuredClone(end));
    for(const jobId of ["install-python","check-env","install-deps"])fake.emitSessionEvent(backend.id,{type:"job/settled",data:{jobId,owner:backend.id,status:"completed"}});
    await fake.flush();await fake.flush();assert.equal(backend.messages.length,2);assert.equal(frontend.stopped,false);
    fake.emitSessionEvent(frontend.id,{seq:9102,...assistantUsageEvent(1,{inputTokens:1000,outputTokens:1000,cacheReadTokens:0,cacheWriteTokens:0})});
    await fake.waitFor(()=>api.getMoneyBudget(t.id)!.reservedAmount==="0");
    assert.equal(api.getMoneyBudget(t.id)!.calculatedAmount,"0.0755864");assert.equal(api.getTask(t.id)!.status,"active");
    assert.equal(api.getTaskEvidence(t.id).collections.accountingIssues?.[t.id],undefined);
    const continuation=Object.values(api.getTaskEvidence(t.id).events).filter((e:any)=>e.type==="task.continue");
    assert.equal(continuation.length,1);assert.equal((continuation[0] as any).payload.cause,"output_truncated");
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("截断等待后台命令：只在真实命令全部结束后唤醒，重复事件不重复投递",async()=>{
  const {fake,api,taskId,worker}=await makeMoneyApp("60");try{
    assert.equal(await fake.emitPreStep(worker.id),true);await fake.emitRequest(worker.id,moneyRoute);
    for(const jobId of ["ready","running-a","running-b"])fake.emitSessionEvent(worker.id,{type:"job/registered",data:{jobId,owner:worker.id}});
    fake.emitSessionEvent(worker.id,{type:"job/settled",data:{jobId:"ready",owner:worker.id}});
    await fake.waitFor(()=>api.getBackgroundJobs(taskId).length===3&&api.getBackgroundJobs(taskId).find(j=>j.jobId==="ready")?.settled===true);
    fake.emitSessionEvent(worker.id,{seq:9200,...assistantUsageEvent(1,{inputTokens:1000,outputTokens:8000,cacheReadTokens:0,cacheWriteTokens:0},"截断")});
    const end={seq:9201,type:"turn/end",data:{turn:1,reason:{kind:"max-tokens"}}};fake.emitSessionEvent(worker.id,end);
    await fake.waitFor(()=>api.listWaits(taskId).some(w=>w.status==="waiting")||api.getTask(taskId)?.status==="paused");
    assert.equal(api.getTask(taskId)!.status,"active");assert.equal(worker.messages.length,1);assert.equal(worker.stopped,false);
    assert.equal(api.listWaits(taskId).filter(w=>w.status==="waiting").length,2);
    fake.emitSessionEvent(worker.id,structuredClone(end));
    fake.emitSessionEvent(worker.id,{type:"job/settled",data:{jobId:"running-a",owner:worker.id}});
    await fake.waitFor(()=>api.getBackgroundJobs(taskId).find(j=>j.jobId==="running-a")?.settled===true);assert.equal(worker.messages.length,1);
    fake.emitSessionEvent(worker.id,{type:"job/settled",data:{jobId:"running-b",owner:worker.id}});
    await fake.waitFor(()=>worker.messages.length===2);assert.equal(api.getTask(taskId)!.status,"active");
    fake.emitSessionEvent(worker.id,{type:"job/settled",data:{jobId:"running-b",owner:worker.id}});await fake.flush();await fake.flush();
    assert.equal(worker.messages.length,2);assert.ok(api.listWaits(taskId).every(w=>w.status!=="waiting"));assert.equal(api.getMoneyBudget(taskId)!.limitAmount,"60");
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("截断等待专家回复：保持定向等待，不暂停或催跑Worker",async()=>{
  const {fake,api,taskId,worker}=await makeMoneyApp("60");try{
    assert.equal(await fake.emitPreStep(worker.id),true);await fake.emitRequest(worker.id,moneyRoute);
    fake.emitSessionEvent(worker.id,{seq:9300,...assistantUsageEvent(1,{inputTokens:1000,outputTokens:8000,cacheReadTokens:0,cacheWriteTokens:0},'<longtask_report>{"done":false,"next":[],"waiting":"等待专家交接"}</longtask_report>')});
    fake.emitSessionEvent(worker.id,{seq:9301,type:"turn/end",data:{turn:1,reason:{kind:"max-tokens"}}});
    await fake.waitFor(()=>api.listWaits(taskId).some(w=>w.kind==="expert_message"&&w.status==="waiting")||api.getTask(taskId)?.status==="paused");
    assert.equal(api.getTask(taskId)!.status,"active");assert.equal(worker.messages.length,1);assert.equal(worker.stopped,false);
    assert.equal(api.getMoneyBudget(taskId)!.reservedAmount,"0");assert.equal(api.getMoneyBudget(taskId)!.limitAmount,"60");
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("Worker审批生命周期：轮次未结束即登记等待，决定后不代批、不重复续做",async()=>{
  const {fake,api,taskId,worker}=await makeMoneyApp("60");try{
    const before=structuredClone(api.getBudget(taskId));
    for(const outcome of ["allowed-once","rejected","cancelled","unavailable"]){
      const id="approval-"+outcome;
      const asked={type:"approval/asked",data:{id,toolName:"pwsh",callId:"call-"+id,reason:"需要用户确认此工具操作"}};
      fake.emitSessionEvent(worker.id,asked);fake.emitSessionEvent(worker.id,structuredClone(asked));
      for(let n=0;n<8;n++)await fake.flush();
      assert.equal(api.listWaits(taskId).filter(w=>w.status==="waiting").length,1,"approval/asked 应在 turn/end 前登记可靠等待");
      const evidence=api.getTaskEvidence(taskId),pending=Object.values(evidence.collections.taskApprovals??{}) as any[];
      assert.equal(pending.filter(a=>a.status==="pending").length,1);
      assert.equal((api.getTask(taskId) as any).waitingOn.kind,"user_approval");
      fake.emitSessionEvent(worker.id,{type:"approval/decided",data:{id,outcome}});
      await fake.waitFor(()=>api.listWaits(taskId).every(w=>w.status!=="waiting"));
      fake.emitSessionEvent(worker.id,{type:"approval/decided",data:{id,outcome}});await fake.flush();
      assert.equal(api.getTask(taskId)!.status,"active");assert.equal(worker.messages.length,1);assert.equal(worker.stopped,false);
      assert.deepEqual(api.getBudget(taskId),before,"审批状态不修改账本");
    }
    fake.emitSessionEvent(worker.id,{type:"approval/asked",data:{id:"unfinished",toolName:"pwsh"}});
    await fake.waitFor(()=>api.listWaits(taskId).some(w=>w.status==="waiting"));
    fake.emitSessionEvent(worker.id,{type:"turn/end",data:{turn:1,reason:{kind:"aborted"}}});
    await fake.waitFor(()=>api.listWaits(taskId).every(w=>w.status!=="waiting"));
    assert.equal((Object.values(api.getTaskEvidence(taskId).collections.taskApprovals??{}) as any[]).find(a=>a.requestId==="unfinished")?.status,"expired");
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("审批等待重载：原审批失效并暂停核对，不自动重建Worker或追加预算",async()=>{
  const first=await makeMoneyApp("60");let second:ReturnType<typeof makeFakeHost>|undefined;
  try{
    first.fake.emitSessionEvent(first.worker.id,{type:"approval/asked",data:{id:"before-reload",toolName:"pwsh",reason:"需要用户批准"}});
    await first.fake.waitFor(()=>first.api.listWaits(first.taskId).some(w=>w.status==="waiting"));
    const before=structuredClone(first.api.getBudget(first.taskId));
    for(const stop of first.fake.teardowns.splice(0))await stop();
    second=makeFakeHost();apply(second.ctx,{dataDir:first.dir,agencyAgentsRoot:join(first.dir,"experts"),probeOnStart:false,watchdogIntervalMs:3600000});
    await second.waitFor(()=>Boolean(second!.provided.get("longtask")));
    const api=second.provided.get("longtask") as LongtaskApi;
    assert.equal(api.getTask(first.taskId)!.status,"paused");assert.equal(api.getTask(first.taskId)!.pauseReason,"missing_input");
    assert.deepEqual(api.getBudget(first.taskId),before);assert.equal(second.agentsById.size,0);
    assert.ok(api.listWaits(first.taskId).every(w=>w.status!=="waiting"));
    assert.equal((Object.values(api.getTaskEvidence(first.taskId).collections.taskApprovals??{}) as any[])[0]!.outcome,"runtime-reloaded");
  }finally{for(const stop of first.fake.teardowns)await stop();if(second)for(const stop of second.teardowns)await stop();}
});

test("请求配置等待期间取消：金额不预留、不收费，也不恢复已取消任务",async()=>{
  const {fake,api,taskId,worker}=await makeMoneyApp();try{
    await fake.emitPreStep(worker.id);let release!:(v:Record<string,unknown>)=>void;
    const config=new Promise<Record<string,unknown>>(r=>{release=r});
    const pending=fake.emitRequest(worker.id,config as unknown as Record<string,unknown>);
    await fake.flush();await api.cancel(taskId);release(moneyRoute);
    await assert.rejects(()=>pending,/任务已停止/);
    assert.equal(api.getTask(taskId)!.status,'cancelled');assert.equal(api.getMoneyBudget(taskId)!.reservedAmount,'0');
    assert.equal(api.getMoneyBudget(taskId)!.calculatedAmount,'0');assert.equal(api.getBudget(taskId)!.reservedTokens,0);
  }finally{for(const stop of fake.teardowns)await stop();}
});

test("金额任务完整交付：执行→验收退回→返工→再验收均按请求收费，共用原金额不追加",async()=>{
  const {fake,api,taskId,worker,dir}=await makeMoneyApp('5',{acceptorReadOnly:true});try{
    for(let round=1;round<=2;round++){
      await fs.writeFile(join(dir,'money.md'),round===1?'初稿':'修订完成');
      assert.equal(await fake.emitPreStep(worker.id),true);await fake.emitRequest(worker.id,moneyRoute);
      fake.emitSessionEvent(worker.id,{seq:8700+round,...assistantUsageEvent(round,{inputTokens:1000,outputTokens:500,cacheReadTokens:0,cacheWriteTokens:0},'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>')});
      fake.emitSessionEvent(worker.id,turnEndEvent(round));
      await fake.waitFor(()=>api.getTask(taskId)?.status==='awaiting_acceptance'&&[...fake.agentsById.values()].reduce((n,a)=>n+a.messages.filter(m=>m.includes('独立验收请求')).length,0)>=round);
      const acceptor=findAgentByMessage(fake,'独立验收请求')!,request=acceptor.messages.findLast(m=>m.includes('独立验收请求'))!,roundId=/轮次 (round_[a-z0-9]+)/.exec(request)![1];
      assert.equal(await fake.emitPreStep(acceptor.id),true);await fake.emitRequest(acceptor.id,moneyRoute);
      const report={roundId,verdicts:[{criterionId:'c1',outcome:round===1?'gap':'pass',gap:round===1?'补齐要求':null,evidence:'已读取产物版本',verdictBy:REVIEWER.relPath,criteriaVersion:1}],artifactHashes:frozenHashes(request)};
      fake.emitSessionEvent(acceptor.id,{seq:8800+round,...assistantUsageEvent(round,{inputTokens:1000,outputTokens:500,cacheReadTokens:0,cacheWriteTokens:0},`<longtask_verdicts>${JSON.stringify(report)}</longtask_verdicts>`)});
      fake.emitSessionEvent(acceptor.id,turnEndEvent(round));
      await fake.waitFor(()=>api.getTask(taskId)?.status===(round===1?'rework':'delivered')&&api.getBudget(taskId)?.reservedTokens===0);
      assert.equal(api.getMoneyBudget(taskId)!.calculatedAmount,round===1?'0.012':'0.024');
    }
    assert.equal(api.getMoneyBudget(taskId)!.limitAmount,'5');assert.equal(api.getModelExpenses(taskId).length,4);assert.deepEqual(api.getBudget(taskId)!.grants,[]);
  }finally{for(const stop of fake.teardowns)await stop();}
});
