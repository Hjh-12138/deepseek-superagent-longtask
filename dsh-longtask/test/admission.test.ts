/**
 * 请求输入上界（P1-1）。
 *
 * 宿主 agent/pre-step 只给出本步新增消息，拿不到组装后的 prompt，
 * 所以"精确输入"不可得；但上界可推：
 *   prompt(N+1) = prompt(N) + 第 N 轮输出 + 本步新入站
 * 本文件把这套推法钉成可重复断言，防止退回"拍一个固定估算值"。
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_ADMISSION,
  boundRequestInput,
  hasTrustedBound,
  normalizeAdmission,
  requestCostFor,
  requestAdmissionCost,
  planAcceptance,
  type AdmissionPolicy,
} from "../src/budget/admission.js";

const POLICY: AdmissionPolicy = { baselineInputTokens: 12_000, inputSafetyFactor: 1.25, maxOutputTokens: 8000 };

test('验收计划包含真实请求、读取增长和结论；小预算不得把费用截成10%',()=>{
  const input={requestBytes:5945,artifactBytes:100,unknownArtifacts:0,artifactCount:1,minimumTokens:26000};
  const plan=planAcceptance(POLICY,input);assert.equal(plan.firstRequest,28945);
  assert.ok(plan.conclusionRequest>plan.firstRequest);assert.equal(plan.tokens,plan.firstRequest+plan.conclusionRequest);assert.ok(plan.tokens>160000*.1);
  assert.ok(planAcceptance(POLICY,{...input,artifactBytes:1000}).tokens>plan.tokens);
  assert.ok(planAcceptance(POLICY,{...input,unknownArtifacts:1}).tokens>plan.tokens);
  assert.equal(planAcceptance(POLICY,{...input,minimumTokens:200000}).tokens,200000);
});

test("首轮没有实测链条：用声明基线 + 本步入站字符（首次大输入不再被忽略）", () => {
  const warm = boundRequestInput(POLICY, { inboundChars: 0 });
  assert.equal(warm.basis, "declared");
  assert.equal(warm.tokens, 15_000, "12,000 × 1.25");

  // 首轮就带进来一大段上下文（粘贴长文、恢复的长简报）
  const firstBig = boundRequestInput(POLICY, { inboundChars: 60_000 });
  assert.equal(firstBig.basis, "declared");
  assert.equal(firstBig.tokens, 75_000, "15,000 + 60,000");
  assert.ok(firstBig.tokens > requestAdmissionCost(POLICY), "固定估算会低估这种请求");
});

test("有实测链条：上界 = 上轮输入 + 上轮输出 + 本步入站（工具结果突增也算得进去）", () => {
  const calm = boundRequestInput(POLICY, { lastInputTokens: 21_000, lastOutputTokens: 1_200, inboundChars: 800 });
  assert.equal(calm.basis, "measured");
  assert.equal(calm.tokens, 23_000);

  // 某一步工具结果暴增 40,000 字符：旧口径 1.25 × 21,000 ≈ 26,250 完全看不到这部分
  const burst = boundRequestInput(POLICY, { lastInputTokens: 21_000, lastOutputTokens: 1_200, inboundChars: 40_000 });
  assert.equal(burst.basis, "measured");
  assert.equal(burst.tokens, 62_200);
  const oldStyle = Math.ceil(21_000 * POLICY.inputSafetyFactor);
  assert.ok(burst.tokens > oldStyle * 2, `工具结果突增必须体现在上界里（旧口径 ${oldStyle}）`);
});

test("实测链条不乘安全系数：多轮之后不会指数膨胀", () => {
  // 上下文稳定不变的会话，每轮上界应当只按真实增量线性增长
  let input = 20_000;
  let output = 1_000;
  const bounds: number[] = [];
  for (let i = 0; i < 5; i++) {
    const b = boundRequestInput(POLICY, { lastInputTokens: input, lastOutputTokens: output, inboundChars: 500 });
    bounds.push(b.tokens);
    input = b.tokens - 500; // 下一轮实测输入就是本轮 prompt + 输出
    output = 1_000;
  }
  for (const b of bounds) assert.ok(b < 40_000, `上界不应膨胀到 ${b}`);
  assert.deepEqual(bounds, [...bounds].sort((a, b) => a - b), "只随真实增量单调增长");
});

test("严格模式的可信上界判据：未声明基线时首轮不可信", () => {
  const declared = boundRequestInput(POLICY, { inboundChars: 100 });
  assert.equal(hasTrustedBound(POLICY, declared), true);

  const unset: AdmissionPolicy = { ...POLICY, baselineInputTokens: 0 };
  const first = boundRequestInput(unset, { inboundChars: 100 });
  assert.equal(first.basis, "declared");
  assert.equal(hasTrustedBound(unset, first), false, "既无实测链条又未声明基线 → 必须拒绝调用");

  // 一旦有了实测链条，基线是否声明都不再影响
  const measured = boundRequestInput(unset, { lastInputTokens: 20_000, lastOutputTokens: 900 });
  assert.equal(measured.basis, "measured");
  assert.equal(hasTrustedBound(unset, measured), true);
});

test("准入成本 = 输入上界 + 输出上限；归一化允许显式声明 0 基线", () => {
  const bound = boundRequestInput(POLICY, { lastInputTokens: 10_000, lastOutputTokens: 500, inboundChars: 0 });
  assert.equal(requestCostFor(bound, POLICY), 10_500 + POLICY.maxOutputTokens);
  assert.equal(requestAdmissionCost(DEFAULT_ADMISSION, 10_000), Math.ceil(10_000 * 1.25) + 8000);

  assert.equal(normalizeAdmission({ baselineInputTokens: 0 }).baselineInputTokens, 0, "0 表示不声明基线，不能被静默替换成默认值");
  assert.equal(normalizeAdmission({ baselineInputTokens: Number.NaN }).baselineInputTokens, DEFAULT_ADMISSION.baselineInputTokens);
  assert.equal(normalizeAdmission({ inputSafetyFactor: 0.5 }).inputSafetyFactor, DEFAULT_ADMISSION.inputSafetyFactor, "系数不得小于 1");
});
