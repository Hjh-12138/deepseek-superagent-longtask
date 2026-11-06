/**
 * 探针健壮性测试（阶段 0 回归）。
 *
 * 背景：真实宿主中探针曾卡在 agent.whenIdle() 永不返回，导致 apply 的 ready
 * 永不完成、ctx.provide("longtask") 与全部领域服务不可用，表现为"插件没加载"。
 * 本文件锁定两条不变量：
 *   1. probeHost 对所有可能悬挂的宿主调用设超时，超时后仍产出报告；
 *   2. 探针在关键路径之外执行——即使 whenIdle 永不返回，用户入口也照常注册。
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Context } from "@deepseek-ai/cordis";

import { apply, type LongtaskConfig } from "../src/index.js";
import { probeHost } from "../src/probe/probe.js";
import type { AgentHandle, AgentLike, HostContext } from "../src/host-types.js";

test("真实 Cordis 作用域使用 on 返回的 disposer，探针清理后仍返回报告", async () => {
  const root = new Context();
  const disposed = { value: false };
  root.provide("agents", { create: async () => quickAgent(disposed) });
  root.provide("sessions", {});
  try {
    const report = await new Promise<Awaited<ReturnType<typeof probeHost>>>((resolve, reject) => {
      root.plugin({
        name: "longtask-probe-native-regression",
        inject: ["agents", "sessions"],
        apply(ctx: Context) {
          void probeHost(ctx as unknown as HostContext).then(resolve, reject);
        },
      });
    });
    assert.ok(report.items.some((item) => item.id === "agent.pre-step"));
    assert.equal(disposed.value, true);
  } finally {
    await root.fiber.dispose();
  }
});

/** 挂起的 whenIdle：模拟宿主轮次永不收敛 */
function hangingAgent(disposed: { value: boolean }): AgentHandle {
  const agent: AgentLike = {
    id: "probe-agent",
    session: { id: "probe-session" },
    status: "running",
    followup() {
      // 消息进入队列但永不产生 turn
    },
    cancel() {},
    whenIdle: () => new Promise<void>(() => {}),
  };
  return {
    agent,
    dispose: async () => {
      disposed.value = true;
    },
  };
}

function makeCtx(options: { hangWhenIdle: boolean; disposed?: { value: boolean } }) {
  const provided = new Map<string, unknown>();
  const listeners = new Map<string, Array<(...args: any[]) => void>>();
  const logs: string[] = [];
  const disposed = options.disposed ?? { value: false };
  const ctx: HostContext = {
    logger: {
      info: (m: string) => logs.push(`[info] ${m}`),
      warn: (m: string) => logs.push(`[warn] ${m}`),
      error: (m: string) => logs.push(`[error] ${m}`),
    },
    get: <T>(name: string): T | undefined =>
      name === "agents"
        ? ({
            create: async () => (options.hangWhenIdle ? hangingAgent(disposed) : quickAgent(disposed)),
            get: () => undefined,
            list: () => [],
          } as unknown as T)
        : undefined,
    on: (event: string, listener: (...args: any[]) => void) => {
      (listeners.get(event) ?? listeners.set(event, []).get(event)!).push(listener);
    },
    off: (event: string, listener: (...args: any[]) => void) => {
      const arr = listeners.get(event);
      if (arr) arr.splice(arr.indexOf(listener), 1);
    },
    effect(factory: () => Generator) {
      const gen = factory();
      const step = gen.next();
      void step; // 生成器体同步；teardown 由用户手动调用（本测试不卸载）
    },
    provide: (name: string, value: unknown) => provided.set(name, value),
  };
  return { ctx, provided, logs, disposed };
}

function quickAgent(disposed: { value: boolean }): AgentHandle {
  const agent: AgentLike = {
    id: "probe-agent",
    session: { id: "probe-session" },
    status: "idle",
    followup() {},
    cancel() {},
    whenIdle: async () => {},
  };
  return { agent, dispose: async () => void (disposed.value = true) };
}

const flush = () => new Promise<void>((r) => setTimeout(r, 10));
async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 3000) {
  const start = Date.now();
  while (!(await cond())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor 超时");
    await flush();
  }
}

test("probeHost：whenIdle 永不返回时超时收敛，仍产出报告并释放探针会话", async () => {
  const { ctx, disposed } = makeCtx({ hangWhenIdle: true });
  const started = Date.now();
  const report = await probeHost(ctx, { createTimeoutMs: 1000, idleTimeoutMs: 80, disposeTimeoutMs: 500 });
  const elapsed = Date.now() - started;

  assert.ok(elapsed < 3000, `探针应在超时后收敛（实际 ${elapsed}ms）`);
  assert.equal(report.items.find((i) => i.id === "agent.create")?.status, "pass");

  const idleItem = report.items.find((i) => i.id === "agent.whenIdle");
  assert.ok(idleItem, "应记录 whenIdle 项");
  assert.equal(idleItem!.status, "unsupported");
  assert.match(idleItem!.detail, /超时/);

  assert.equal(report.items.find((i) => i.id === "agent.dispose")?.status, "pass");
  assert.equal(disposed.value, true, "探针会话必须被释放，不留孤儿");
});

test("apply：探针悬挂不阻塞用户入口（ctx.provide 及时注册）且探针报告落盘", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "lt-probe-"));
  const { ctx, provided } = makeCtx({ hangWhenIdle: true });
  const cfg: Partial<LongtaskConfig> = {
    dataDir,
    agencyAgentsRoot: dataDir,
    probeOnStart: true,
    probeInitialDelayMs: 10,
    probeRetryDelayMs: 10,
    probeMaxAttempts: 1,
    probeIdleTimeoutMs: 80,
    probeCreateTimeoutMs: 1000,
    watchdogIntervalMs: 3600_000,
  };

  const t0 = Date.now();
  apply(ctx, cfg);
  await waitFor(() => provided.has("longtask"), 1500);
  const provideDelay = Date.now() - t0;
  assert.ok(provideDelay < 1200, `用户入口应在探针完成前注册（实际 ${provideDelay}ms）`);

  await waitFor(async () => {
    const files = await readdir(dataDir);
    return files.some((f) => f.startsWith("probe-attempt-1-"));
  }, 3000);
});

test("卸载取消：等待中的延迟探针收到 abort 后不再探测、不再写报告（热重载污染回归）", async () => {
  const { runDeferredProbe } = await import("../src/probe/probe.js");
  const controller = new AbortController();
  const saved: string[] = [];
  let probed = 0;
  const ctx = {
    logger: { info() {}, warn() {}, error() {} },
    get: () => undefined,
    on: () => () => {},
    effect: () => {},
    provide: () => {},
  } as unknown as HostContext;

  const run = runDeferredProbe(
    ctx,
    async (_report, label) => {
      saved.push(label);
    },
    {
      initialDelayMs: 10_000, // 远大于下面的 abort 时间
      retryDelayMs: 10_000,
      maxAttempts: 3,
      instanceId: "inst-old",
      signal: controller.signal,
      timeouts: { onProgress: (stage) => { if (stage.endsWith(":start")) probed++; } },
    },
  );

  // 旧实例还在等首次延迟时就卸载
  await new Promise((r) => setTimeout(r, 30));
  controller.abort();

  const report = await run;
  assert.equal(report, null, "被取消的探针不返回报告");
  assert.equal(saved.length, 0, "被取消的探针不落盘（否则会覆盖 probe-latest）");
  assert.equal(probed, 0, "取消后不得真的发起探测");
});

test("探针报告带实例标识：同一进程里新旧实例的报告可区分", async () => {
  const { runDeferredProbe } = await import("../src/probe/probe.js");
  const root = new Context();
  root.provide("agents", { create: async () => quickAgent({ value: false }) });
  root.provide("sessions", {});
  try {
    const labels: string[] = [];
    const report = await new Promise<Awaited<ReturnType<typeof runDeferredProbe>>>((resolve, reject) => {
      root.plugin({
        name: "longtask-probe-instance-id",
        inject: ["agents", "sessions"],
        apply(ctx: Context) {
          void runDeferredProbe(
            ctx as unknown as HostContext,
            async (_r, label) => {
              labels.push(label);
            },
            { initialDelayMs: 0, maxAttempts: 1, instanceId: "inst-new" },
          ).then(resolve, reject);
        },
      });
    });
    assert.ok(report, "未取消时返回报告");
    assert.equal(report!.instanceId, "inst-new");
    assert.ok(labels.includes("latest"), "正常路径仍写 latest");
  } finally {
    await root.fiber.dispose();
  }
});
