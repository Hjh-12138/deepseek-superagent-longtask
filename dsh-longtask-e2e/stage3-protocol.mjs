import { openSync, writeFileSync, fsyncSync, closeSync, renameSync, unlinkSync, mkdirSync, readFileSync, linkSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';

export const STAGE3_VERSION = 'stage3-lifecycle-v2';
export const STAGE3_TOOL = 'longtask_e2e_stage3_checkpoint';
export const TERMINAL = new Set(['passed', 'failed', 'cancelled']);
export const sha = value => createHash('sha256').update(value).digest('hex');
export const readJson = path => JSON.parse(readFileSync(path, 'utf8'));
export const maybeJson = path => existsSync(path) ? readJson(path) : undefined;

/** 文件落盘后发布；重试仅处理 Windows 短暂文件占用，不重试业务动作。 */
export function atomicJson(path, value, exclusive = false) {
  mkdirSync(dirname(path), { recursive: true });
  const temp = `${path}.${randomUUID()}.tmp`; const fd = openSync(temp, 'wx');
  try { writeFileSync(fd, JSON.stringify(value, null, 2)); fsyncSync(fd); } finally { closeSync(fd); }
  try {
    if (exclusive) linkSync(temp, path);
    else {
      for (let n = 0; ; n++) {
        try { renameSync(temp, path); break; }
        catch (error) {
          if (!['EPERM','EACCES','EBUSY'].includes(error.code) || n >= 5) throw error;
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 15);
        }
      }
    }
  } finally { if (existsSync(temp)) unlinkSync(temp); }
}

export function splitBudget(total) {
  if (!Number.isSafeInteger(total) || total < 3) throw Error('必须明确指定至少 3 Token 的总上限');
  const A = Math.max(1, Math.floor(total * .40)); const B = Math.max(1, Math.floor(total * .30));
  return { A, B, C: total - A - B };
}
export function validateStart(request) {
  if (request?.version !== STAGE3_VERSION || typeof request.runId !== 'string' || !/^[a-f0-9-]{36}$/.test(request.runId)) throw Error('无效阶段三运行请求');
  splitBudget(request.totalTokens);
  if (request.window?.visible !== true || !Number.isSafeInteger(request.window.pid) || request.window.pid <= 0 || typeof request.window.processName !== 'string' || !request.window.processName) throw Error('开始前需要可见 DSH 窗口的观测基线');
  return request;
}
export function expectedBody(state, role) {
  const line = role === 'A' ? '后台写入完成' : role === 'C' ? '优先任务完成' : `口径码：${state.answerCode}`;
  return `STAGE3 ${state.runId} ${role}\n${line}`;
}
export function artifactMatches(state, role, text) {
  return typeof text === 'string' && text.replace(/\r\n/g,'\n').trimEnd() === expectedBody(state, role);
}
export function isStallReport(text) {
  let report;
  for (const match of String(text).matchAll(/<longtask_report>\s*(\{[\s\S]*?\})\s*<\/longtask_report>/g)) {
    try { report=JSON.parse(match[1]); } catch { /* 只认有效协议 */ }
  }
  return report?.done===false&&report.waiting==='STAGE3_WATCHDOG_EXTERNAL'&&Array.isArray(report.next)&&report.next.length===0;
}
export function realRestart(before, after) {
  return Number.isSafeInteger(before?.pid) && Number.isSafeInteger(after?.pid) && before.pid !== after.pid
    && Number.isSafeInteger(before.bootAt) && Number.isSafeInteger(after.bootAt) && after.bootAt >= before.bootAt;
}
export function validateHiddenProof(state, proof) {
  const samples = proof?.samples;
  const release = state.hiddenReleasedAt; const write = state.writes?.find(w => w.role === 'A');
  return Array.isArray(samples) && samples.length >= 2 && !!release && !!write
    && samples.every(s => s.hidden === true && s.hostAlive === true && s.hostPid === state.initialHost.pid
      && s.processName === state.window.processName && Number.isFinite(s.at))
    && samples.every((s,i) => !i || s.at > samples[i-1].at)
    && samples[0].at <= release && release <= write.at && write.at <= samples.at(-1).at
    && proof.artifactHash === write.hash && proof.runId === state.runId;
}

/** 只接受实际证据；缺一个条件就不能产出 done/passed。 */
export function assessStage3(state, { budgets, artifacts, rawAudit, answers, taskStates }) {
  const reasons = [];
  if(state.fixtureVersion===2&&rawAudit?.gateWaitsVerified!==true)reasons.push('缺少宿主模型闸门及等待期间零调用的原始证据');
  if (!validateHiddenProof(state, state.hiddenProof)) reasons.push('缺少窗口隐藏区间内的真实写入证据');
  if (!state.initialParallel || !state.initialQueued || !state.priorityPromoted) reasons.push('并行/排队/暂停优先证据未闭合');
  if (!realRestart(state.beforeRestart?.host, state.restartedHost)) reasons.push('未观测真实进程重启（插件重载不算）');
  if (!state.pausePreserved || !state.recoveredB || !state.recoveredC || !state.balancePreserved) reasons.push('恢复会话/主动暂停/累计用量证据不足');
  if (!state.answerQueued || !state.answerConsumed || !answers?.some(a => a.answer === state.answer && a.delivery === 'delivered')) reasons.push('完整答复的排队、持久化和实际消费证据不足');
  if (!state.cStalledAt || !state.watchdogReceipt || !Number.isFinite(state.cBaselineProgressAt)
    || !(state.watchdogConfig?.stallThresholdMs > 0) || state.watchdogReceipt.at < state.cBaselineProgressAt + state.watchdogConfig.stallThresholdMs
    || state.watchdogReceipt.at < state.cStalledAt || rawAudit?.watchdogConsumed !== 1) reasons.push('没有自然计时的真实Watchdog纠偏及消费证据');
  const ids = new Set(); let total = 0;
  for (const role of ['A','B','C']) {
    const task = state.tasks?.[role]; const budget = budgets?.[role];
    if (!task?.id || ids.has(task.id)) reasons.push(`${role}任务身份缺失或重复`); ids.add(task?.id);
    if (taskStates?.[role] !== 'delivered') reasons.push(`${role}尚未独立验收交付`);
    if (!budget || budget.limitTokens !== state.shares[role] || budget.reservedTokens !== 0 || budget.grants?.length
      || !Number.isSafeInteger(budget.settledTokens) || budget.settledTokens < 0 || budget.settledTokens > budget.limitTokens) reasons.push(`${role}预算不符、未结算或出现追加`);
    total += budget?.settledTokens ?? 0;
    const before = state.beforeRestart?.budgets?.[role];
    if (!before || budget?.limitTokens !== before.limitTokens || budget?.settledTokens < before.settledTokens) reasons.push(`${role}重启后累计用量回退`);
    if (!artifactMatches(state, role, artifacts?.[role]?.text) || !artifacts?.[role]?.frozenMatches) reasons.push(`${role}真实产物/冻结哈希不符`);
    const writes = state.writes?.filter(w => w.role === role) ?? [];
    if (writes.length !== 1 || !writes[0]?.callId || !writes[0]?.sessionId || writes[0]?.hash !== artifacts?.[role]?.hash) reasons.push(`${role}写入缺失、重复或版本不符`);
    if (role === 'C' && !(writes[0]?.at >= state.watchdogReceipt?.at)) reasons.push('C未在实际Watchdog纠偏后写入');
  }
  if (Object.values(state.shares ?? {}).reduce((a,b)=>a+b,0) !== state.totalTokens || total > state.totalTokens) reasons.push('总预算不守恒或超额');
  const rawWrite = rawAudit?.writes?.find(w=>w.role==='A');
  if (!rawWrite || !(rawWrite.at >= state.hiddenProof?.samples?.[0]?.at && rawWrite.at <= state.hiddenProof?.samples?.at(-1)?.at)) reasons.push('原始写入时间不在隐藏采样区间内');
  if (!rawAudit?.verified || rawAudit.actualTokens !== total || rawAudit.descendants !== 0 || rawAudit.answerConsumed !== 1 || !rawAudit.writesMatch || !rawAudit.watchdogVerified
    || !rawAudit.sessions?.length || !rawAudit.sessions.every(s=>s.matches&&s.actualTokens===s.ledgerTokens&&/^[a-f0-9]{64}$/.test(s.rawSha256))
    || new Set(rawAudit.sessions.map(s=>s.sessionId)).size !== rawAudit.sessions.length
    || !['A','B','C'].every(role=>rawAudit.sessions.some(s=>s.role===role&&s.agentRole==='executor')&&rawAudit.sessions.some(s=>s.role===role&&s.agentRole==='acceptor'))) reasons.push('持久原始会话与账本未对齐');
  return { passed: reasons.length === 0, reasons, settledTokens: total, limitTokens: state.totalTokens };
}

export function publishControl(directory, state, action, extra = {}) {
  if (!state || TERMINAL.has(state.phase) && !(state.phase === 'failed' && action === 'recover-tools')) throw Error('没有可控制的阶段三运行');
  const command = { runId: state.runId, action, id: randomUUID(), at: Date.now(), ...extra };
  atomicJson(join(directory, 'controls', `${command.id}.json`), command, true); return command;
}
