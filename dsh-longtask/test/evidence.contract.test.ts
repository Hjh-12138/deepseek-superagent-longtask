/**
 * 联调证据判定器的契约测试（审查 25 · C7）。
 *
 * 判定器是"阶段二出口是否通过"的唯一闸门，必须能被错误输入说服为"不通过"。
 * 这些用例直接 import 联调包里的纯函数，不启动宿主、不产生任何模型调用。
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
import { join } from "node:path";

const evidencePath = pathToFileURL(join(process.cwd(), "..", "dsh-longtask-e2e", "evidence.mjs")).href;

async function loadEvidence(): Promise<{
  SCENARIOS: string[];
  parseRunRequest(raw: unknown): { scenario: string };
  assessScenario(input: Record<string, unknown>): { passed: boolean; reasons: string[] };
}> {
  return (await import(evidencePath)) as never;
}

/** 一份"本应通过"的 pair 证据基线，用例只改动其中一个字段 */
function goodPairInput(overrides: Record<string, unknown> = {}) {
  return {
    taskId: "task_x",
    runId: "run_x",
    scenario: "pair",
    status: "delivered",
    budget: { limitTokens: 500_000, settledTokens: 200_000, reservedTokens: 0 },
    accounting: { verified: true, actualTokens: 200_000, descendants: [] },
    controls: { wrapUp: 0, readStop: 0, wrapUpConsumed: 0, readStopConsumed: 0, rejects: 0, boundExceeded: 0 },
    rounds: [],
    fault: undefined,
    fixture: undefined,
    stage2: {
      directTotal: 1,
      directDelivered: 1,
      distinctParticipants: true,
      bodyPersisted: true,
      digestContainsBody: false,
      summaries: 1,
      summaryRefs: 1,
      summaryVersions: 1,
      digestArtifacts: 2,
      waitedBeforeHandoff: true,
      consumedAfterHandoff: true,
      duplicateActions: 0,
      deliveryVerified: true,
      replayVerified: true,
    },
    ...overrides,
  };
}

test("运行请求解析器接受 pair，拒绝未登记场景", async () => {
  const { SCENARIOS, parseRunRequest } = await loadEvidence();
  assert.ok(SCENARIOS.includes("pair"), "pair 必须在统一场景枚举里");
  assert.equal(parseRunRequest({ scenario: "pair", budgetTokens: 260_000, strict: true }).scenario, "pair");
  assert.throws(() => parseRunRequest({ scenario: "nope", budgetTokens: 1 }), /有效scenario/);
});

test("证据判定器：合法 pair 证据可通过", async () => {
  const { assessScenario } = await loadEvidence();
  const result = assessScenario(goodPairInput());
  assert.equal(result.passed, true, result.reasons.join("；"));
});

test("证据判定器：任一错误都必须判失败（C7 负向对照）", async () => {
  const { assessScenario } = await loadEvidence();
  const cases: Array<[string, Record<string, unknown>, RegExp]> = [
    ["短正文泄漏进 Router", { stage2: { ...goodPairInput().stage2 as object, digestContainsBody: true } }, /携带了专家正文/],
    ["正文哈希对不上", { stage2: { ...goodPairInput().stage2 as object, bodyPersisted: false } }, /正文未完整持久化/],
    ["同一专家自说自话", { stage2: { ...goodPairInput().stage2 as object, distinctParticipants: false } }, /不是两位不同的执行专家/],
    ["交接前没有等待", { stage2: { ...goodPairInput().stage2 as object, waitedBeforeHandoff: false } }, /没有在交接前结束过一轮等待/],
    ["收到后没有消费", { stage2: { ...goodPairInput().stage2 as object, consumedAfterHandoff: false } }, /没有继续推进/],
    ["同一动作多条消息", { stage2: { ...goodPairInput().stage2 as object, duplicateActions: 1 } }, /幂等失效/],
    ["摘要没记产物版本", { stage2: { ...goodPairInput().stage2 as object, summaryVersions: 0 } }, /没有记录交接时的产物版本/],
    ["登记产物不足两份", { stage2: { ...goodPairInput().stage2 as object, digestArtifacts: 1 } }, /登记产物不足两份/],
  ];
  for (const [name, patch, pattern] of cases) {
    const result = assessScenario(goodPairInput(patch));
    assert.equal(result.passed, false, `${name} 应判失败`);
    assert.ok(
      result.reasons.some((r) => pattern.test(r)),
      `${name} 的失败原因应点名问题，实际：${result.reasons.join("；")}`,
    );
  }
});
