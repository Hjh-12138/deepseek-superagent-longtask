/**
 * 探针 CLI。两种用法：
 * 1. 宿主内：插件 apply({ probeOnStart: true }) 会自动运行并落盘报告（推荐）。
 * 2. 独立冒烟：node dist/src/probe/run-probe.js --mock
 *    用假造上下文跑通探针代码路径，验证报告生成；不证明宿主能力。
 */

import { probeHost, renderProbeReport, type ProbeReport } from "./probe.js";
import type { HostContext } from "../host-types.js";
import { writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";

function mockContext(): HostContext {
  const services: Record<string, unknown> = {
    agents: {
      create: async () => ({
        id: "probe-agent",
        session: { id: "probe-session" },
        status: "idle",
        followup: () => {},
        cancel: () => {},
        whenIdle: async () => {},
      }),
    },
    sessions: { flush: async () => {} },
    sessionProjections: { tokenUsage: {} },
    tokenMeter: {},
  };
  return {
    logger: { info: console.log, warn: console.warn, error: console.error },
    get: <T>(name: string) => services[name] as T | undefined,
    on: () => {},
    off: () => {},
    effect: () => {},
    provide: () => {},
  };
}

async function main(): Promise<void> {
  const useMock = process.argv.includes("--mock");
  const report: ProbeReport = await probeHost(useMock ? mockContext() : (throwNoHost() as never));
  const outDir = process.cwd();
  mkdirSync(outDir, { recursive: true });
  const stamp = report.probedAt.replace(/[:.]/g, "-");
  writeFileSync(join(outDir, `probe-${stamp}.md`), renderProbeReport(report), "utf8");
  writeFileSync(join(outDir, `probe-${stamp}.json`), JSON.stringify(report, null, 2), "utf8");
  console.log(renderProbeReport(report));
}

function throwNoHost(): never {
  throw new Error("独立模式必须带 --mock；真实探针请在 DSH 宿主中启用 probeOnStart");
}

if (process.argv[1] && process.argv[1].endsWith("run-probe.js")) {
  main().catch((e) => {
    console.error(String((e as Error).message ?? e));
    process.exit(1);
  });
}
