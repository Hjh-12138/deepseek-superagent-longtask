/**
 * 预算准入策略（P1-7 硬准入的唯一口径）。
 *
 * 背景：旧实现每步只预留 6,000 Token，而真实请求输入 20,000～32,000，
 * 结果是"返回后才发现超限"——账本永远是事后记账，而不是事前准入。
 *
 * 本模块把"一次模型请求至少要占用多少额度"收敛为一个纯函数，供三处共用：
 * - 派工闸门（TaskService.gateDispatch）：没额度就不建会话、不投递
 * - 恢复（TaskService.resume）：额度不足以支撑一步就不解锁
 * - 显式限额在agent/pre-step预留；宿主模式在最终llm/stream边界按解析后的上限预留
 *
 * 公式：cost = ceil(baseline 或 已观测输入 × 安全系数) + 输出上限
 * 已观测输入来自上一轮 assistant/message 的真实 usage，能自动跟随上下文增长。
 */

export interface AdmissionPolicy {
  /** 尚未观测到真实用量时的基线输入估算（含系统提示、简报、工具定义） */
  baselineInputTokens: number;
  /** 对已观测输入的上浮系数：吸收下一轮新增的工具结果与历史增长 */
  inputSafetyFactor: number;
  /** 显式上限；0仅表示宿主模式下尚未解析的前置规划，最终流边界必须取得正整数。 */
  maxOutputTokens: number;
}

export const DEFAULT_ADMISSION: AdmissionPolicy = {
  baselineInputTokens: 12_000,
  inputSafetyFactor: 1.25,
  maxOutputTokens: 16000,
};

/** 归一化策略：拒绝非有限值，避免 NaN 一路传播成"永远够用"或"永远不够" */
export function normalizeAdmission(partial?: Partial<AdmissionPolicy>): AdmissionPolicy {
  const pick = (value: number | undefined, fallback: number, min: number): number =>
    typeof value === "number" && Number.isFinite(value) && value >= min ? value : fallback;
  return {
    // baselineInputTokens 允许显式为 0：表示"不声明宿主基线"，
    // 此时严格模式在首轮（没有实测链条）会拒绝调用而不是猜一个数（P1-1）。
    baselineInputTokens: pick(partial?.baselineInputTokens, DEFAULT_ADMISSION.baselineInputTokens, 0),
    inputSafetyFactor: pick(partial?.inputSafetyFactor, DEFAULT_ADMISSION.inputSafetyFactor, 1),
    maxOutputTokens: pick(partial?.maxOutputTokens, DEFAULT_ADMISSION.maxOutputTokens, 0),
  };
}

/** 估算下一次请求的输入 Token（有实测值就用实测值上浮） */
export function estimateRequestInput(policy: AdmissionPolicy, observedInputTokens?: number): number {
  const base =
    typeof observedInputTokens === "number" && Number.isFinite(observedInputTokens) && observedInputTokens > 0
      ? observedInputTokens
      : policy.baselineInputTokens;
  return Math.ceil(base * policy.inputSafetyFactor);
}

/** 准入一次模型请求所需的最小可用额度 */
export function requestAdmissionCost(policy: AdmissionPolicy, observedInputTokens?: number): number {
  return estimateRequestInput(policy, observedInputTokens) + policy.maxOutputTokens;
}

/** 人类可读的准入说明（写进暂停原因与诊断日志，便于对账） */
export function describeAdmission(policy: AdmissionPolicy, observedInputTokens?: number): string {
  const input = estimateRequestInput(policy, observedInputTokens);
  const source = typeof observedInputTokens === "number" && observedInputTokens > 0 ? `实测输入 ${observedInputTokens}` : `基线输入 ${policy.baselineInputTokens}`;
  return policy.maxOutputTokens===0?`${source} × ${policy.inputSafetyFactor} = ${input}，输出上限待DSH解析，最终请求前另行足额预留`:`${source} × ${policy.inputSafetyFactor} = ${input} + 输出上限 ${policy.maxOutputTokens} = ${input + policy.maxOutputTokens}`;
}

// ---------------------------------------------------------------------------
// 请求输入上界（P1-1）
//
// 宿主的 agent/pre-step 只给出"本步新入站的消息"，拿不到组装后的完整 prompt，
// 所以"精确输入"不可得。但上界是可推的：
//   组装后的 prompt(N+1) = prompt(N) + 第 N 轮 assistant 输出 + 本步新入站内容
// 三项分别是：上一轮实测输入、上一轮实测输出、本步入站字符数（按 1 字符 = 1 Token 折算，
// 对 CJK 恰好、对 ASCII/JSON 是数倍高估，方向安全）。
// 只有会话第一次请求没有这条实测链条，此时只能用"声明基线"。声明基线的可靠性由调用方决定：
// 严格模式下若基线被显式声明为 0，则视为"无法取得可信上界"，必须拒绝调用。
// ---------------------------------------------------------------------------

export interface RequestObservation {
  /** 同一会话上一轮请求的实测输入（含缓存读写分项） */
  lastInputTokens?: number;
  /** 同一会话上一轮请求的实测输出：它会原样进入下一轮上下文 */
  lastOutputTokens?: number;
  /** 本步新入站内容的字符数（工具结果、用户消息、审批回填） */
  inboundChars?: number;
}

export interface InputBound {
  tokens: number;
  /** measured：由实测链条推出；declared：首轮只能靠声明基线 */
  basis: "measured" | "declared";
  /** 写进诊断日志与暂停原因，便于事后对账 */
  detail: string;
}

/** 1 字符按 1 Token 计：对中文准确，对代码/JSON 高估数倍——作为上界方向安全 */
const CHARS_PER_TOKEN_UPPER_BOUND = 1;

export function boundRequestInput(policy: AdmissionPolicy, observed?: RequestObservation): InputBound {
  const inboundChars =
    typeof observed?.inboundChars === "number" && Number.isFinite(observed.inboundChars) && observed.inboundChars > 0
      ? Math.ceil(observed.inboundChars / CHARS_PER_TOKEN_UPPER_BOUND)
      : 0;
  const lastIn =
    typeof observed?.lastInputTokens === "number" && Number.isFinite(observed.lastInputTokens) && observed.lastInputTokens > 0
      ? observed.lastInputTokens
      : null;
  const lastOut =
    typeof observed?.lastOutputTokens === "number" && Number.isFinite(observed.lastOutputTokens) && observed.lastOutputTokens > 0
      ? observed.lastOutputTokens
      : null;

  if (lastIn === null && lastOut === null) {
    const base = Math.ceil(policy.baselineInputTokens * policy.inputSafetyFactor);
    const tokens = base + inboundChars;
    return {
      tokens,
      basis: "declared",
      detail: `声明基线 ${policy.baselineInputTokens} × ${policy.inputSafetyFactor} = ${base} + 入站 ${inboundChars} = ${tokens}（本会话尚无实测链条）`,
    };
  }
  // 实测链条不再乘安全系数：上一轮输入/输出是实测值，本步入站按上界折算，
  // 若再乘系数会随轮次指数膨胀，反而把可用额度算没。
  const measured = (lastIn ?? 0) + (lastOut ?? 0);
  const tokens = measured + inboundChars;
  return {
    tokens,
    basis: "measured",
    detail: `上轮输入 ${lastIn ?? 0} + 上轮输出 ${lastOut ?? 0} + 入站 ${inboundChars} = ${tokens}`,
  };
}

/** 上界是否可声明为"可信"：没有实测链条时必须显式声明基线（>0）才算 */
export function hasTrustedBound(policy: AdmissionPolicy, bound: InputBound): boolean {
  return bound.basis === "measured" || policy.baselineInputTokens > 0;
}

/** 本次请求要占用的额度：输入上界 + 输出上限 */
export function requestCostFor(bound: InputBound, policy: AdmissionPolicy): number {
  return bound.tokens + policy.maxOutputTokens;
}

/** 两步验收计划：读取产物，再给结论。使用与正式请求相同的输入基线和输出上限。 */
export function planAcceptance(policy: AdmissionPolicy, input: {
  requestBytes: number; artifactBytes: number; unknownArtifacts: number; artifactCount: number; minimumTokens: number; readBudgetBytes?: number;
}) {
  const firstInput = boundRequestInput(policy, { inboundChars: input.requestBytes }).tokens;
  // 未来验收按有限读取计划预留，不假定所有产物全文进入上下文。
  // 这是可调整的收尾规划；每次实际请求仍按最终完整输入与宿主输出上限硬准入。
  const readBudgetBytes = input.readBudgetBytes ?? 65_536;
  if (!Number.isSafeInteger(readBudgetBytes) || readBudgetBytes < 0) throw Error("验收读取规划必须为非负安全整数");
  const unknownReadBytes = policy.maxOutputTokens > 0 ? Math.min(4096,Math.floor(policy.maxOutputTokens/6)) : 1024;
  const plannedReadBytes = Math.min(readBudgetBytes, input.artifactBytes + input.unknownArtifacts * unknownReadBytes);
  const readGrowth = plannedReadBytes * 6 + Math.min(input.artifactCount, 16) * 1024;
  const firstRequest = firstInput + policy.maxOutputTokens;
  const conclusionRequest = firstInput + policy.maxOutputTokens + readGrowth + policy.maxOutputTokens;
  const inputTokens = firstInput * 2 + policy.maxOutputTokens + readGrowth;
  const outputTokens = policy.maxOutputTokens * 2;
  const untypedTokens = Math.max(0,input.minimumTokens-inputTokens-outputTokens);
  return { firstRequest, conclusionRequest, readGrowth, inputTokens, outputTokens, untypedTokens,
    plannedReadBytes, readBudgetBytes, tokens: inputTokens+outputTokens+untypedTokens, unknownArtifacts: input.unknownArtifacts };
}
