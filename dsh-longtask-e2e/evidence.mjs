import { createHash } from "node:crypto";
import {normalizeCrash,STAGE4_VERSION} from './stage4-protocol.mjs';

/** 场景枚举：解析器、CLI 与驱动共用同一份，新增场景必须同时满足三处 */
export const SCENARIOS = ["basic", "pressure", "wrapup", "gap", "gap-inject", "pair", "doc", "dev"];

/** 纯证据判定：任务结束与场景验收通过是不同的结果；缺失证据按未覆盖处理。 */
export function parseRunRequest(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw Error("运行请求必须是JSON对象");
  const allowed = new Set(["at", "note", "requestId", "scenario", "budgetTokens", "strict", "crash", "stage4Version"]);
  if (Object.keys(raw).some((key) => !allowed.has(key))) throw Error("运行请求含不支持的字段；运行策略配置不能写在触发文件里");
  if (!SCENARIOS.includes(raw.scenario)) throw Error(`必须指定有效scenario（${SCENARIOS.join(" / ")}）`);
  if (!Number.isSafeInteger(raw.budgetTokens) || raw.budgetTokens <= 0) throw Error("必须显式指定正整数budgetTokens，不使用默认预算启动");
  if (raw.strict !== undefined && typeof raw.strict !== "boolean") throw Error("strict必须是布尔值");
  const crash=normalizeCrash(raw.crash);
  if(crash&&raw.scenario!=='doc')throw Error('故障注入只支持doc');
  if(['doc','dev'].includes(raw.scenario)&&(raw.stage4Version!==STAGE4_VERSION||raw.strict===false))throw Error('需要最新阶段四协议及严格预算');
  if(!['doc','dev'].includes(raw.scenario)&&raw.stage4Version!==undefined)throw Error('stage4Version仅用于doc/dev');
  return { ...raw, ...(crash?{crash}:{}), strict: raw.strict ?? true };
}

/** 原始持久状态 + 实际 surface/工具观测 → pair 证据。缺失观测不推断为成功。 */
export function extractPairEvidence({ state, digest, taskId, observations = [], replay }) {
  const messages = Object.values(state.collections?.messages ?? {}).filter((m) => m.taskId === taskId);
  const direct = messages.filter((m) => m.kind === "expert_direct");
  const summaries = messages.filter((m) => m.kind === "summary");
  const artifacts = Object.values(state.collections?.artifacts ?? {}).filter((a) => a.taskId === taskId);
  const sessions = Object.values(state.collections?.taskSessions?.[taskId] ?? {});
  const trace = observations.filter((o) => o.taskId === taskId);
  const hash = (s) => createHash("sha256").update(s).digest("hex");
  const bodyPersisted = direct.length > 0 && direct.every((m) => typeof m.body === "string" && m.body.length > 0 && hash(m.body) === m.bodyHash);
  // 比较字段来源，不能全文子串搜索：目标中的预置示例可能与实际留言相同。
  const task = state.collections?.tasks?.[taskId];
  const version = task?.versions?.[task.currentVersion];
  const latest = new Map();
  for (const a of artifacts) latest.set(a.location.replace(/\\/g, "/").replace(/\/+/g, "/").toLowerCase(), a);
  const budget = state.collections?.budgets?.[taskId];
  const expectedDigest = version ? {
    taskId, status: task.status, goal: version.goal, criteriaVersion: version.version,
    criteria: version.criteria.map((c) => ({ id: c.id, text: c.text })),
    executors: version.executors.map((e) => e.relPath), acceptor: version.acceptor.relPath,
    ...(version.assignments ? { assignments: version.assignments } : {}),
    artifacts: [...latest.values()].filter((a) => a.contentHash).map((a) => ({ location: a.location, hash: a.contentHash.slice(0, 12), owner: a.ownerRelPath ?? null })),
    latestSummaries: [...summaries].sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, 5).map((m) => ({
      messageId: m.id, from: m.fromRelPath, to: m.toRelPath, summary: m.excerpt,
      artifactRefs: m.artifactRefs ?? [], causedByMessageId: m.replyToId, at: m.createdAt,
    })),
    budget: budget ? { limit: budget.limitTokens, settled: budget.settledTokens, reserved: budget.reservedTokens } : null,
  } : null;
  const canonical = (value) => Array.isArray(value) ? value.map(canonical)
    : value && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map((k) => [k, canonical(value[k])])) : value;
  const routerViewMatches = !!expectedDigest && JSON.stringify(canonical(digest)) === JSON.stringify(canonical(expectedDigest));
  const executors = digest?.executors ?? [];
  const distinctParticipants = direct.length > 0 && direct.every((m) => m.fromRelPath !== m.toRelPath
    && executors.includes(m.fromRelPath) && executors.includes(m.toRelPath));
  const validSummaries = summaries.filter((s) => {
    const original = direct.find((m) => m.id === s.replyToId && m.fromRelPath === s.fromRelPath && m.toRelPath === s.toRelPath);
    return original && s.artifactRefs?.length > 0 && s.artifactRefs.every((ref) =>
      original.artifactRefs?.includes(ref) && /^[a-f0-9]{64}$/.test(s.artifactVersions?.[ref] ?? "")
      && s.artifactVersions[ref] === original.artifactVersions?.[ref]
      && artifacts.some((a) => a.location === ref && a.contentHash === s.artifactVersions[ref] && a.createdAt <= original.createdAt));
  });
  const first = direct.find((m) => m.delivery === "delivered");
  const recipient = sessions.find((s) => s.relPath === first?.toRelPath && s.role === "executor" && !s.revokedAt);
  const after = (b, a) => b.instanceId === a.instanceId && b.order > a.order;
  const consumed = first && recipient ? trace.filter((o) => o.kind === "consumed" && o.sessionId === recipient.sessionId
    && o.dedupeKey === `${first.dedupeKey}#deliver`) : [];
  const wait = first && recipient ? trace.find((o) => o.kind === "turn_end" && o.sessionId === recipient.sessionId
    && o.reason === "completed" && typeof o.waiting === "string" && o.waiting.trim() && o.at <= first.createdAt) : undefined;
  const dependency = version?.assignments?.find((a) => a.relPath === first?.toRelPath)?.waitFor;
  const storedEvents = Object.values(state.events ?? {}).filter((e) => e.taskId === taskId && e.taskVersion === version?.version);
  const pathKey = (path) => String(path).replace(/\\/g, "/").toLowerCase();
  const sameRefs = (refs, expected) => Array.isArray(refs) && Array.isArray(expected) && refs.length === expected.length
    && expected.every((p) => refs.some((r) => pathKey(r) === pathKey(p)));
  const scheduledWait = first && first.taskVersion === version?.version && dependency?.fromRelPath === first.fromRelPath
    && dependency.artifactRefs.every((p) => first.artifactRefs?.some((r) => pathKey(r) === pathKey(p)))
    && storedEvents.some((e) => e.type === "expert.waiting" && sameRefs(e.payload?.waitFor?.artifactRefs, dependency.artifactRefs)
    && e.payload?.relPath === first.toRelPath && e.payload?.waitFor?.fromRelPath === first.fromRelPath && e.createdAt <= first.createdAt)
    && storedEvents.some((e) => e.type === "expert.dependency_ready" && e.payload?.relPath === first.toRelPath && e.payload?.messageId === first.id);
  const read = consumed.length === 1 ? trace.find((o) => o.kind === "artifact_read" && o.sessionId === recipient.sessionId
    && after(o, consumed[0]) && first.artifactRefs?.includes(o.path) && o.hash === first.artifactVersions?.[o.path]) : undefined;
  const completed = read ? trace.find((o) => o.kind === "turn_end" && o.sessionId === recipient.sessionId
    && o.reason === "completed" && o.turn === read.turn && after(o, read)) : undefined;
  const actionCounts = new Map();
  for (const m of direct) actionCounts.set(m.actionId, (actionCounts.get(m.actionId) ?? 0) + 1);
  const accepted = (a) => a.outcome === "delivered" || a.reconciledAs === "delivered";
  const duplicateActions = [...actionCounts.values()].filter((n) => n > 1).length
    + direct.filter((m) => (m.deliveryAttempts ?? []).filter(accepted).length > 1).length;
  const deliveryVerified = direct.length > 0 && direct.every((m) => m.actionId && m.deliveryAttempts?.length > 0
    && m.deliveryAttempts.every((a) => a.finishedAt && (a.outcome !== "unknown" || a.reconciledAs))
    && m.deliveryAttempts.filter(accepted).length === (m.delivery === "delivered" ? 1 : 0));
  const replayVerified = !!first && replay?.taskId === taskId && replay.sourceTurnId === first.sourceTurnId
    && replay.eventCount === 2 && replay.actionId === first.actionId && consumed.length === 1 && deliveryVerified;
  return {
    directTotal: direct.length, directDelivered: direct.filter((m) => m.delivery === "delivered").length,
    distinctParticipants, bodyPersisted,
    digestContainsBody: !routerViewMatches,
    summaries: digest?.latestSummaries?.length ?? 0,
    summaryRefs: (digest?.latestSummaries ?? []).filter((s) => s.artifactRefs?.length > 0).length,
    summaryVersions: validSummaries.length, digestArtifacts: digest?.artifacts?.length ?? 0,
    waitedBeforeHandoff: consumed.length === 1 && (!!scheduledWait || (!!wait && after(consumed[0], wait))),
    consumedAfterHandoff: !!completed, duplicateActions, deliveryVerified, replayVerified,
    owners: (digest?.artifacts ?? []).map((a) => a.owner),
  };
}

export function assessScenario({ taskId, runId, scenario, status, budget, accounting, controls, rounds, fault, fixture, stage2 }) {
  const reasons = [];
  if(['doc','dev'].includes(scenario))reasons.push('阶段四必须使用独立原始会话与功能证据判定，不能仅用通用delivered');
  const number = (v) => Number.isSafeInteger(v) && v >= 0;
  if (status !== "delivered") reasons.push(`任务未交付：${status ?? "unknown"}`);
  if (!budget || ![budget.limitTokens, budget.settledTokens, budget.reservedTokens].every(number)) {
    reasons.push("预算快照缺失或无效");
  } else {
    if (budget.settledTokens + budget.reservedTokens > budget.limitTokens) reasons.push("实际用量或在途预留超过固定上限");
    if (budget.reservedTokens !== 0) reasons.push("任务结束仍有未结算预留");
  }
  if (!accounting?.verified) reasons.push(`会话树用量未核对：${accounting?.reason ?? "缺少宿主证据"}`);
  if ((accounting?.descendants?.length ?? 0) > 0) reasons.push("存在未托管后代，不能以主会话账本代表任务总消耗");
  if (!number(accounting?.actualTokens) || accounting.actualTokens !== budget?.settledTokens) reasons.push("宿主用量与任务账本不一致");
  if (!controls || controls.boundExceeded !== 0) reasons.push("控制证据缺失或发生请求上界击穿");
  const wrapConsumed = (controls?.wrapUp ?? 0) > 0 && (controls?.wrapUpConsumed ?? 0) > 0;
  const readConsumed = (controls?.readStop ?? 0) > 0 && (controls?.readStopConsumed ?? 0) > 0;
  if (scenario === "wrapup" && !wrapConsumed) reasons.push("收尾场景没有收尾触发及实际消费证据");
  if (scenario === "wrapup" && !(fixture?.version === "checkpoint-v2" && fixture.pendingAtControl
    && fixture.receiptIssued && Number.isSafeInteger(fixture.controlOrder) && Number.isSafeInteger(fixture.receiptOrder)
    && fixture.controlOrder > 0 && fixture.receiptOrder > fixture.controlOrder && fixture.artifactMatches && fixture.receiptFileMatches)) {
    reasons.push("收尾夹具缺少活动待办→真实消费→领取本轮回执→落盘的完整证据");
  }
  if (scenario === "pressure" && !wrapConsumed && !readConsumed) reasons.push("压力场景没有控制触发及实际消费证据");
  if (scenario === "gap" || scenario === "gap-inject") {
    const gapIndex = rounds?.findIndex((r) => r.outcome === "rework" && r.verdicts.some((v) => v.endsWith(":gap"))) ?? -1;
    const accepted = gapIndex >= 0 && rounds.slice(gapIndex + 1).some((r) => r.outcome === "delivered" && r.verdicts.length > 0 && r.verdicts.every((v) => v.endsWith(":pass")));
    if (!accepted) reasons.push("没有真实 gap→返工→再次验收全部通过的有序轮次");
    if (scenario === "gap-inject") {
      const p = fault?.phases;
      const phases = p && [p.injected, p.firstFrozen, p.gap, p.repaired, p.secondFrozen];
      const ordered = phases?.every((n, i) => Number.isSafeInteger(n) && n > (i === 0 ? 0 : phases[i - 1]));
      const first = gapIndex >= 0 && rounds[gapIndex];
      const second = gapIndex >= 0 && rounds.slice(gapIndex + 1).find((r) => r.outcome === "delivered" && r.verdicts.every((v) => v.endsWith(":pass")));
      if (!(fault?.version === "gap-before-freeze-v2" && fault.taskId === taskId && fault.runId === runId && taskId && runId
        && !fault.failure && fault.injected && fault.repaired && fault.artifactMatches && fault.criteriaUnchanged && ordered
        && /^[a-f0-9]{64}$/.test(fault.beforeHash ?? "") && /^[a-f0-9]{64}$/.test(fault.injectedHash ?? "")
        && /^[a-f0-9]{64}$/.test(fault.repairedHash ?? "") && fault.beforeHash !== fault.injectedHash && fault.repairedHash !== fault.injectedHash
        && fault.firstRoundId && fault.secondRoundId && fault.firstRoundId !== fault.secondRoundId
        && first?.roundId === fault.firstRoundId && first.verdicts.includes("c2:gap") && second?.roundId === fault.secondRoundId)) {
        reasons.push("故障注入、首次冻结、真实退回、Worker修复、再次冻结或最终文件的证据不完整");
      }
    }
  }
  if (scenario === "pair") {
    // 阶段2 出口：两位专家直接交接产物、Router 只看摘要、正文可回查、等待顺序与幂等可证
    if (!stage2) reasons.push("缺少阶段2协作文据");
    else {
      if (!(stage2.directTotal >= 1)) reasons.push("没有专家直连消息");
      if (!(stage2.directDelivered >= 1)) reasons.push("专家直连消息未成功投递（收件人未被唤醒）");
      if (!stage2.distinctParticipants) reasons.push("交接双方不是两位不同的执行专家");
      if (!stage2.bodyPersisted) reasons.push("直连正文未完整持久化或与 bodyHash 不符，无法按消息回查");
      if (!(stage2.summaries >= 1)) reasons.push("缺少交接摘要");
      if (!(stage2.summaryRefs >= 1)) reasons.push("交接摘要没有产物引用");
      if (!(stage2.summaryVersions >= 1)) reasons.push("交接摘要没有记录交接时的产物版本");
      if (!(stage2.digestArtifacts >= 2)) reasons.push("Router 摘要中登记产物不足两份");
      if (stage2.digestContainsBody) reasons.push("Router 摘要携带了专家正文或不匹配已批准的来源字段");
      if (!stage2.waitedBeforeHandoff) reasons.push("收件方没有在交接前结束过一轮等待");
      if (!stage2.consumedAfterHandoff) reasons.push("收件方在收到交接后没有继续推进（未消费）");
      if (!(stage2.duplicateActions === 0)) reasons.push("同一动作产生了多条消息（幂等失效）");
      if (!stage2.deliveryVerified) reasons.push("缺少按实际投递尝试核对的单次送达证据");
      if (!stage2.replayVerified) reasons.push("缺少真实结束事件重放及只消费一次的证据");
    }
  }
  if (!SCENARIOS.includes(scenario)) reasons.push(`未知测试场景：${scenario}`);
  return { passed: reasons.length === 0, reasons };
}
