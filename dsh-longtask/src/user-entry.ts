import {createHash} from "node:crypto";
import {isAbsolute} from "node:path";
import {statSync} from "node:fs";
import {pendingApprovals} from "./approval-waits.js";
import {searchExperts, recommendExperts, reviewStaffing} from "./catalog/recommend.js";
import type {HostContext} from "./host-types.js";
import type {TaskService, ProposeInput} from "./tasks/task-service.js";
import type {TokenLedger} from "./ledger/token-ledger.js";
import type {ExpertRef, Task, ExecutorAssignment} from "./types.js";
import {parseCny, formatCny} from "./ledger/money.js";

export const USER_TASK_TOOLS = ["longtask_experts", "longtask_propose", "longtask_confirm", "longtask_control", "longtask_tasks"] as const;
export const USER_TASK_ENTRY_PROTOCOL = 3;
export interface EntryAgent { id?: string; session?: {id?: string; header?: {parentSession?: string}} }
interface Execution {agent?: EntryAgent; callId: string; signal: AbortSignal}
interface Definition {name: string; description: string; parameters: Record<string, unknown>; output: {schema: {type: "string"}; render(args: unknown, value: string): Array<{type: "text"; text: string}>}; execute(args: unknown, exec: Execution): Promise<string>}
interface Tools {register(def: Definition): () => void}
interface Approval {request(input: {agent: EntryAgent; toolName: string; callId: string; reason: string; signal: AbortSignal}): Promise<"allowed-once" | "rejected" | "cancelled" | "unavailable">}
interface Questions {ask(input: {agent: EntryAgent; signal: AbortSignal; wait: {callId: string}; questions: Array<{id: string; question: string; detail: string}>}): Promise<{answers: Array<{id: string; selected: string[]; custom?: string}>}>}
interface Options {ctx: HostContext; tasks: TaskService; ledger: TokenLedger; experts(fresh?: boolean): Promise<ExpertRef[]>; isManaged(agent: EntryAgent): boolean; ready(): boolean; workspace: string}
const string = {type: "string", minLength: 1, maxLength: 2000};
const object = (properties: Record<string, unknown>, required: string[] = []) => ({type: "object", additionalProperties: false, properties, required});
const strings = (maxItems: number) => ({type: "array", minItems: 1, maxItems, uniqueItems: true, items: string});
const integer = {type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER};
const taskIdSchema = {...string, maxLength: 128};
const versionSchema = {type: "integer", minimum: 1};
const criterionSchema = object({id: {type: "string", pattern: "^[A-Za-z][A-Za-z0-9_-]{0,63}$"}, text: string}, ["id", "text"]);
const assignmentSchema = object({relPath: string, deliverables: strings(32), waitFor: object({fromRelPath: string, artifactRefs: strings(32)}, ["fromRelPath", "artifactRefs"])}, ["relPath", "deliverables"]);
function args(value: unknown): Record<string, unknown> {if (!value || typeof value !== "object" || Array.isArray(value)) throw Error("参数必须是对象"); return value as Record<string, unknown>}
function text(value: unknown, label: string): string {if (typeof value !== "string" || !value.trim() || value.length > 2000) throw Error(`${label}必须是1..2000字的文本`); return value.trim()}
function number(value: unknown, label: string, min = 0): number {if (!Number.isSafeInteger(value) || (value as number) < min) throw Error(`${label}必须是大于等于${min}的安全整数`); return value as number}
function texts(value: unknown, label: string, max = 32): string[] {if (!Array.isArray(value) || !value.length || value.length > max) throw Error(`${label}数量无效`); const items = value.map(v => text(v, label)); if (new Set(items).size !== items.length) throw Error(`${label}不能重复`); return items}
function criteriaOf(value: unknown) {
  if (!Array.isArray(value) || !value.length || value.length > 32) throw Error("完成标准数量无效");
  const criteria = value.map(c => {const r = args(c), id = text(r.id, "标准编号"); if (!/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(id)) throw Error("标准编号无效"); return {id, text: text(r.text, "完成标准")}});
  if (new Set(criteria.map(c => c.id)).size !== criteria.length) throw Error("完成标准编号不能重复"); return criteria;
}

/** 用户会话工具适配层。所有任务变更复用TaskService；不产生任何独立模型调用。 */
export function registerUserTaskTools(o: Options): {ready: boolean; dispose(): Promise<void>} {
  const tools = o.ctx.get<Tools>("tools");
  if (typeof tools?.register !== "function") return {ready: false, async dispose() {}};
  const stops: Array<() => void> = [], closing = new AbortController(), calls = new Set<Promise<string>>(), confirming = new Set<string>();
  const locks = new Map<string, Promise<unknown>>();
  const epochs = new Map<string, number>(), approvalControllers = new Map<string, Set<AbortController>>();
  let closed = false;
  const assertCaller = (exec: Execution) => {
    exec.signal.throwIfAborted();
    if (closed || !o.ready()) throw Error("长任务服务尚未就绪或已经卸载");
    if (!exec.agent?.session?.id || o.isManaged(exec.agent)) throw Error("这些入口只供用户会话管理任务，执行专家和验收专家不能自行操作");
  };
  const locked = async <T>(id: string, fn: () => Promise<T>): Promise<T> => {const previous = locks.get(id) ?? Promise.resolve(); const p = previous.then(fn, fn); locks.set(id, p); try {return await p} finally {if (locks.get(id) === p) locks.delete(id)}};
  const epoch = (id: string) => epochs.get(id) ?? 0;
  const checkEpoch = (id: string, ticket: number) => {if (epoch(id) !== ticket) throw Error("用户已暂停、取消或调整方案，本次确认失效")};
  const invalidate = (id: string) => {epochs.set(id, epoch(id) + 1); for (const controller of approvalControllers.get(id) ?? []) controller.abort()};
  const task = (id: string): Task => {const t = o.tasks.getTask(id); if (!t) throw Error("任务不存在：" + id); return t};
  const plan = (t: Task) => {
    const v = o.tasks.currentVersion(t);
    return {taskId: t.id, version: v.version, goal: v.goal, criteria: v.criteria, deliverables: v.deliverables,
      executors: v.executors.map(e => ({relPath: e.relPath, name: e.name, contentHash: e.contentHash})), acceptor: {relPath: v.acceptor.relPath, name: v.acceptor.name, contentHash: v.acceptor.contentHash},
      assignments: v.assignments ?? [], requiredTools: v.requiredTools ?? [], budget: v.budget.money ?? v.budget,
      staffingReview: reviewStaffing(v.goal, v.executors, v.acceptor)};
  };
  const fingerprint = (t: Task) => createHash("sha256").update(JSON.stringify(plan(t))).digest("hex");
  const budgetView = (t: Task) => {const b = o.ledger.getBudget(t.id), p = o.tasks.currentVersion(t).budget; if (p.money) return o.ledger.moneyView(t.id) ?? {currency: "CNY", limitAmount: null, suggestedAmount: p.money.suggestedAmount ?? null, requiresUserAmount: true}; return {limitTokens: b?.limitTokens ?? p.limitTokens, settledTokens: b?.settledTokens ?? 0, reservedTokens: b?.reservedTokens ?? 0, availableTokens: b ? b.limitTokens - b.settledTokens - b.reservedTokens : p.limitTokens, strict: b?.strict ?? p.strict}};
  const view = (t: Task) => {
    const b = o.ledger.getBudget(t.id), p = plan(t);
    const evidence = o.tasks.getTaskEvidence(t.id), pauseEvent = evidence.eventOrder.map(id => evidence.events[id] as {type?: string; payload?: {note?: string}}).filter(e => e.type === "task.paused").at(-1);
    return {...p, routerSessionId: t.routerSessionId ?? null, relatedTaskId: t.relatedTaskId, relationKind: t.relationKind, status: t.status, pauseReason: t.pauseReason, pauseNote: t.status === "paused" ? pauseEvent?.payload?.note ?? null : null, waiting: t.waitingOn,
      pendingApprovals: pendingApprovals(evidence, t.id)
        .map(a => ({sessionId: a.sessionId, expert: a.relPath, role: a.role, requestId: a.requestId, toolName: a.toolName, reason: a.reason})),
      accountingIssue: o.ledger.accountingIssue(t.id) ?? null, unknownAnswerDelivery: o.tasks.getClarificationAnswers(t.id).some(a => a.delivery === "unknown"),
      ...(o.tasks.currentVersion(t).budget.money ? {recentModelExpenses: o.ledger.moneyEntries(t.id)} : {}),
      budget: budgetView(t),
      progress: o.tasks.routerDigest(t.id), acceptance: o.tasks.latestPendingRound(t.id)?.checkpoint ?? null};
  };
  const approve = async (name: string, t: Task, exec: Execution, ticket: number, operation?: string) => {
    checkEpoch(t.id, ticket);
    const approval = o.ctx.get<Approval>("approval");
    const p = plan(t), b = o.ledger.getBudget(t.id);
    const money = o.tasks.currentVersion(t).budget.money;
    const reason = [operation ?? (name === "longtask_confirm" ? "确认按以下方案开始长任务" : "确认在原预算内恢复长任务"), `任务：${t.id}；方案版本：${p.version}`,
      `目标：${p.goal}`, "完成标准：", ...p.criteria.map(c => `- ${c.id}：${c.text}`), "交付物：", ...p.deliverables.map(d => `- ${d}`),
      `执行专家：${p.executors.map(e => `${e.name}（${e.relPath}）`).join("、")}`, `独立验收：${p.acceptor.name}（${p.acceptor.relPath}）`,
      ...(p.staffingReview.needsDiscussion.length ? [`职责覆盖提醒：${p.staffingReview.needsDiscussion.map(r=>r.label).join("、")}未选专职专家，可由现有人选兼任；请核对分工和完成标准中由谁负责，不会自动增加专家或预算。`] : []),
      `分工：${JSON.stringify(p.assignments)}`, `任务工作目录：${o.workspace}`, money ?
        `人民币总上限：${money.limitAmount === null ? "待您填写" : "¥" + money.limitAmount}；核算账本：${JSON.stringify(o.ledger.moneyView(t.id))}。${money.suggestedAmount ? `Agent建议¥${money.suggestedAmount}，仅供参考。` : ""}\n按模型价格与用量核算；内置价格按高峰价保守核算，不是平台实扣；预算仅含本任务托管模型请求，第三方付费工具不在其中。` :
        `历史任务固定Token上限：${o.tasks.currentVersion(t).budget.limitTokens}；已用：${b?.settledTokens ?? 0}；在途：${b?.reservedTokens ?? 0}`,
      "不自动追加额度；只有独立验收通过才交付。"].join("\n");
    const controller = new AbortController(), pending = approvalControllers.get(t.id) ?? new Set<AbortController>(); pending.add(controller); approvalControllers.set(t.id, pending);
    const signal = AbortSignal.any([exec.signal, closing.signal, controller.signal]); signal.throwIfAborted();
    try {
      if (name === "longtask_confirm" && money?.limitAmount === null) {
        const questions = o.ctx.get<Questions>("userQuestions");
        if (typeof questions?.ask !== "function") throw Error("宿主没有金额输入通道；任务未启动，不能让Agent代填预算");
        const answer = await questions.ask({agent: exec.agent!, signal, wait: {callId: exec.callId}, questions: [{id: "task_budget_cny",
          question: "请填写本任务人民币总上限（例如5或2.50）；提交金额即确认按下面方案开始。关闭或不填写则不开工。", detail: reason}]});
        signal.throwIfAborted(); assertCaller(exec); checkEpoch(t.id, ticket);
        if (answer.answers?.length !== 1 || answer.answers[0]?.id !== "task_budget_cny" || !answer.answers[0].custom?.trim()) throw Error("未收到用户填写的金额，任务未启动");
        const amount = parseCny(answer.answers[0].custom.trim());
        if (amount <= 0n) throw Error("人民币总上限必须大于0，任务未启动");
        return {limitAmount: formatCny(amount), sessionId: exec.agent!.session!.id!, callId: exec.callId};
      }
      if (typeof approval?.request !== "function") throw Error("宿主没有用户确认通道；任务保持原状，未启动");
      const result = await approval.request({agent: exec.agent!, toolName: name, callId: exec.callId, reason, signal});
      signal.throwIfAborted(); assertCaller(exec); checkEpoch(t.id, ticket);
      if (result !== "allowed-once") throw Error(result === "rejected" ? "用户未确认，任务保持原状" : "用户确认未完成，任务保持原状");
    } finally {pending.delete(controller); if (!pending.size) approvalControllers.delete(t.id)}
  };
  const checkExperts = async (t: Task, exec: Execution) => {
    const current = new Map((await o.experts(true)).map(e => [e.relPath, e.contentHash])); assertCaller(exec);
    const v = o.tasks.currentVersion(t);
    if ([...v.executors, v.acceptor].some(e => current.get(e.relPath) !== e.contentHash)) throw Error("专家定义已变更，请更新方案并重新核对后确认");
  };
  const define = (name: string, description: string, parameters: Record<string, unknown>, run: (a: Record<string, unknown>, exec: Execution) => Promise<unknown>) => {
    stops.push(tools.register({name, description, parameters, output: {schema: {type: "string"}, render: (_args, value) => [{type: "text", text: value}]},
      execute(value, exec) {
        const call = (async () => {
          assertCaller(exec); const a = args(value), properties = parameters.properties as Record<string, unknown>, required = parameters.required as string[];
          if (Object.keys(a).some(k => !Object.hasOwn(properties, k))) throw Error("参数含不支持的字段");
          if (required.some(k => a[k] === undefined)) throw Error("缺少必要参数：" + required.filter(k => a[k] === undefined).join("、"));
          return JSON.stringify(await run(a, exec));
        })(); calls.add(call); void call.finally(() => calls.delete(call)).catch(() => {}); return call;
      }}));
  };
  try {
    define("longtask_experts", "中文或英文检索本地专家。提交方案前用task传完整目标与技术栈，查看职责及候选推荐，再按复杂度与预算确定分工；小任务可兼任，不固定前后端两人。query可额外过滤目录页，不裁掉task职责推荐。仅返回元数据，专家全文派工时注入，不启动Worker。", object({query: {type: "string", maxLength: 200}, task: string, offset: integer, limit: {type: "integer", minimum: 1, maximum: 50}}), async (a, exec) => {
      const query = typeof a.query === "string" ? a.query.toLowerCase().trim() : "", offset = a.offset === undefined ? 0 : number(a.offset, "offset"), limit = a.limit === undefined ? 20 : number(a.limit, "limit", 1);
      if (a.query !== undefined && (typeof a.query !== "string" || a.query.length > 200)) throw Error("query必须是不超过200字的文本");
      if (limit > 50) throw Error("limit最大为50"); const taskText = a.task === undefined ? query : text(a.task,"任务目标"),catalog = await o.experts();
      const items = searchExperts(catalog,query || (a.task === undefined ? "" : taskText)); assertCaller(exec);
      const counts = new Map<string,number>(); for (const e of catalog) counts.set(e.division,(counts.get(e.division)??0)+1);
      return {total: items.length, offset, workspace: o.workspace, catalogTotal:catalog.length, divisions:[...counts].map(([division,count])=>({division,count})),
        recommendation:recommendExperts(catalog,taskText),
        experts: items.slice(offset, offset + limit).map(({expert:{relPath,name,division,intro},score,matchedResponsibilities})=>({relPath,name,division,intro,score,matchedResponsibilities}))};
    });
    define("longtask_propose", "生成待确认长任务方案，支持修改尚未启动的方案。必须明确目标、标准、绝对交付路径、执行和独立验收专家；可提供suggestedBudgetCny作为人民币预算建议。实际金额必须由用户在longtask_confirm的宿主输入框填写，Agent不能设定总上限；有歧义先澄清。不启动Worker。", object({
      goal: string, criteria: {type: "array", minItems: 1, maxItems: 32, items: criterionSchema}, deliverables: strings(32), executorRelPaths: strings(8), acceptorRelPath: string, suggestedBudgetCny: {type: "string", pattern: "^(0|[1-9][0-9]{0,6})(\\.[0-9]{1,9})?$"},
      assignments: {type: "array", minItems: 1, maxItems: 8, items: assignmentSchema}, taskId: taskIdSchema, expectedVersion: versionSchema, parentTaskId: taskIdSchema,
    }, ["goal", "criteria", "deliverables", "executorRelPaths", "acceptorRelPath"]), async (a, exec) => {
      if (!isAbsolute(o.workspace) || !statSync(o.workspace, {throwIfNoEntry: false})?.isDirectory()) throw Error("任务工作目录尚未配置为存在的绝对目录；请先设置workerCwd");
      const goal = text(a.goal, "目标"), deliverables = texts(a.deliverables, "交付路径"), paths = texts(a.executorRelPaths, "执行专家", 8), acceptorPath = text(a.acceptorRelPath, "验收专家");
      if (!deliverables.every(isAbsolute)) throw Error("交付物必须使用绝对路径；请先确认任务工作目录");
      const criteria = criteriaOf(a.criteria);
      if (paths.includes(acceptorPath)) throw Error("验收专家必须独立于执行专家");
      const catalog = new Map((await o.experts(true)).map(e => [e.relPath, e])); assertCaller(exec);
      const expert = (path: string) => {const found = catalog.get(path); if (!found) throw Error("专家不在本地目录：" + path); return found};
      let assignments: ExecutorAssignment[];
      if (a.assignments !== undefined) {
        if (!Array.isArray(a.assignments) || !a.assignments.length || a.assignments.length > 8) throw Error("分工数量无效");
        assignments = a.assignments.map(item => {const r = args(item); return {relPath: text(r.relPath, "分工专家"), deliverables: texts(r.deliverables, "分工交付物"), ...(r.waitFor ? {waitFor: {fromRelPath: text(args(r.waitFor).fromRelPath, "前置专家"), artifactRefs: texts(args(r.waitFor).artifactRefs, "交接产物")}} : {})}});
      } else {if (paths.length !== 1) throw Error("多位执行专家必须明确各自assignments分工"); assignments = [{relPath: paths[0]!, deliverables}]}
      const suggestedAmount = a.suggestedBudgetCny === undefined ? undefined : formatCny(parseCny(a.suggestedBudgetCny as string));
      const input: ProposeInput = {goal, criteria, deliverables, executors: paths.map(expert), acceptor: expert(acceptorPath), assignments,
        budget: {limitTokens: Number.MAX_SAFE_INTEGER, strict: true, money: {currency: "CNY", limitAmount: null, ...(suggestedAmount ? {suggestedAmount} : {})}}};
      if (JSON.stringify(input).length > 24000) throw Error("方案过长，请精简为可核对的目标和标准");
      if ((a.taskId === undefined) !== (a.expectedVersion === undefined)) throw Error("调整方案需同时给出taskId和expectedVersion");
      if (a.parentTaskId !== undefined && a.taskId !== undefined) throw Error("关联新要求与修改原方案不能混用");
      if (a.taskId !== undefined) return locked(text(a.taskId, "任务ID"), async () => {
        assertCaller(exec); const t = task(text(a.taskId, "任务ID"));
        if (t.status !== "proposed" || t.versions.some(v => v.confirmedAt) || o.tasks.getSessions(t.id).length) throw Error("只能修改从未启动的待确认方案");
        if (o.tasks.currentVersion(t).version !== number(a.expectedVersion, "方案版本", 1)) throw Error("方案版本已变化，请先查看最新方案");
        await o.tasks.proposeNewVersion(t.id, input); invalidate(t.id); return {task: view(task(t.id)), recommendation: recommendExperts([...catalog.values()],goal), next: "请先核对staffingReview和职责候选，明确兼任安排，再由用户确认当前版本；使用longtask_confirm完成宿主确认后才开始"};
      });
      const parent = a.parentTaskId === undefined ? undefined : text(a.parentTaskId, "父任务ID"); if (parent) task(parent);
      assertCaller(exec); const t = await o.tasks.propose(input, parent, parent ? "child" : "none", exec.agent!.session!.id!); return {task: view(t), recommendation: recommendExperts([...catalog.values()],goal), next: "方案尚未执行；先核对staffingReview与职责候选，明确兼任安排。用户可调整，使用longtask_confirm展示方案并完成宿主确认"};
    });
    define("longtask_confirm", "将当前方案交给宿主展示给用户确认；仅用户同意后启动。必须传入longtask_propose/longtask_tasks返回的taskId和version；不接受模型代填已确认标记，不追加预算。", object({taskId: taskIdSchema, version: versionSchema}, ["taskId", "version"]), async (a, exec) => {
      const id = text(a.taskId, "任务ID"), version = number(a.version, "方案版本", 1), original = task(id);
      if (o.tasks.currentVersion(original).version !== version) throw Error("方案版本已变化，请查看最新方案后再确认");
      if (o.tasks.currentVersion(original).confirmedAt) return {task: view(original), alreadyConfirmed: true};
      if (original.status !== "proposed") throw Error("当前任务不在待确认状态");
      if (confirming.has(id)) return {task: view(original), confirmationPending: true};
      confirming.add(id); const stamp = fingerprint(original), ticket = epoch(id);
      try {
        await checkExperts(original, exec);
        const moneyApproval = await approve("longtask_confirm", original, exec, ticket);
        await checkExperts(task(id), exec);
        return await locked(id, async () => {assertCaller(exec); checkEpoch(id, ticket); const fresh = task(id); if (fresh.status !== "proposed" || fingerprint(fresh) !== stamp) throw Error("确认期间方案或状态已变化，未启动；请核对最新方案"); await o.tasks.confirm(id, 0, version, moneyApproval, exec.agent!.session!.id!); return {task: view(task(id)), started: true}});
      } finally {confirming.delete(id)}
    });
    define("longtask_control", "按用户意图管理任务：pause暂停、resume恢复、cancel取消；answer以text回答任务澄清；supplement补充信息；clarify暂停并提具体问题；reopen按criteria纠错重开已交付任务；replace_executor以addRelPath替换removeRelPath。reopen/replace_executor必须带expectedVersion。答复恢复、重开、换人需宿主确认且不增加预算。新增要求用longtask_propose的parentTaskId单独建方案。有歧义先询问，结果未知不重投。", object({taskId: taskIdSchema, action: {type: "string", enum: ["pause", "resume", "cancel", "answer", "supplement", "clarify", "reopen", "replace_executor"]}, reason: string, text: string, expectedVersion: versionSchema,
      criteria: {type: "array", minItems: 1, maxItems: 32, items: criterionSchema}, addRelPath: string, removeRelPath: string, pendingItems: strings(32)}, ["taskId", "action"]), async (a, exec) => {
      const id = text(a.taskId, "任务ID"), action = a.action, reason = a.reason === undefined ? "用户从正式入口操作" : text(a.reason, "原因"); task(id);
      const fields: Record<string, string[]> = {pause: [], cancel: [], resume: [], answer: ["text"], supplement: ["text"], clarify: ["text"], reopen: ["criteria", "expectedVersion"], replace_executor: ["addRelPath", "removeRelPath", "pendingItems", "expectedVersion"]};
      if (typeof action !== "string" || !fields[action]) throw Error("不支持的任务操作");
      if (Object.keys(a).some(k => !["taskId", "action", "reason", ...fields[action]!].includes(k))) throw Error("参数与当前操作不匹配");
      if (action === "pause" || action === "cancel") {invalidate(id); return locked(id, async () => {assertCaller(exec); if (action === "pause" && task(id).status === "proposed") return {task: view(task(id)), notStarted: true}; if (action === "pause") await o.tasks.pause(id, "user", reason); else await o.tasks.cancel(id); return {task: view(task(id))}})}
      if (action === "clarify" || action === "supplement") {
        const body = text(a.text, action === "clarify" ? "具体问题" : "补充信息");
        if (!["active", "rework"].includes(task(id).status)) throw Error("只有执行或返工任务接受补充/澄清；不解除其他暂停");
        if (action === "clarify") invalidate(id);
        return locked(id, async () => {assertCaller(exec); if (!["active", "rework"].includes(task(id).status)) throw Error("任务状态已变化"); const result = await o.tasks.applyUserFeedback(id, {kind: action, text: body}); return {result, task: view(task(id))}});
      }
      if (["answer", "reopen", "replace_executor"].includes(action)) {
        const original = task(id), stamp = fingerprint(original), status = original.status, ticket = epoch(id), version = o.tasks.currentVersion(original).version;
        if (o.ledger.accountingIssue(id)) throw Error("模型用量尚未核对，不能继续执行");
        if (o.tasks.getClarificationAnswers(id).some(a => a.delivery === "unknown")) throw Error("答复投递结果未知，先按实际收件证据对账，不自动重投");
        let operation: string, run: () => Promise<unknown>, preview = original;
        if (action === "answer") {
          const answer = text(a.text, "澄清答复");
          if (status !== "paused" || original.pauseReason !== "missing_input") throw Error("当前不是等待用户输入的暂停任务；不能用答复解除用户/预算暂停");
          operation = `确认将以下答复交回原任务并在原预算内继续：\n${answer}`;
          run = () => o.tasks.answerClarification(id, answer);
        } else {
          if (number(a.expectedVersion, "方案版本", 1) !== version) throw Error("方案版本已变化，请先查看任务");
          preview = structuredClone(original);
          const v = preview.versions[preview.currentVersion]!;
          if (action === "reopen") {
            if (status !== "delivered") throw Error("纠错重开仅用于已交付任务；运行中修改标准需先停下核对");
            const criteria = criteriaOf(a.criteria);
            for (const c of criteria) {const at = v.criteria.findIndex(x => x.id === c.id); if (at >= 0) v.criteria[at] = c; else v.criteria.push(c)}
            if (JSON.stringify(v.criteria) === JSON.stringify(o.tasks.currentVersion(original).criteria)) throw Error("完成标准没有变化，不重复重开");
            v.version++; operation = `确认纠错重开原任务，新版本沿用全部已用费用与固定上限。原因：${reason}`;
            run = () => o.tasks.reopenWithCriteria(id, {criteria, reason, expectedVersion: version});
          } else {
            if (!["active", "rework"].includes(status)) throw Error("仅执行或返工阶段可以更换执行专家");
            const addPath = text(a.addRelPath, "接手专家"), remove = text(a.removeRelPath, "原执行专家");
            if (!v.executors.some(e => e.relPath === remove) || v.executors.some(e => e.relPath === addPath) || v.acceptor.relPath === addPath) throw Error("只能替换在职执行专家，接手者不能是已有执行者或验收人");
            const add = (await o.experts(true)).find(e => e.relPath === addPath); assertCaller(exec); if (!add) throw Error("接手专家不在本地目录");
            const pendingItems = a.pendingItems === undefined ? [] : texts(a.pendingItems, "未完成项");
            v.executors = v.executors.map(e => e.relPath === remove ? add : e);
            v.assignments = v.assignments?.map(s => ({...s, relPath: s.relPath === remove ? addPath : s.relPath, ...(s.waitFor ? {waitFor: {...s.waitFor, fromRelPath: s.waitFor.fromRelPath === remove ? addPath : s.waitFor.fromRelPath}} : {})}));
            operation = `确认更换执行专家：${remove} → ${addPath}。原因：${reason}\n未完成项：${pendingItems.join("；") || "按现有进度接手"}`;
            run = () => o.tasks.changeExecutor(id, {add, remove, reason, pendingItems, expectedVersion: version});
          }
        }
        await checkExperts(preview, exec); await approve("longtask_control", preview, exec, ticket, operation); await checkExperts(preview, exec);
        return locked(id, async () => {assertCaller(exec); checkEpoch(id, ticket); const fresh = task(id); if (fresh.status !== status || fresh.pauseReason !== original.pauseReason || fingerprint(fresh) !== stamp) throw Error("审批期间任务已变化，本次操作失效"); await o.tasks.bindRouter(id, exec.agent!.session!.id!); const result = await run(); invalidate(id); return {result, task: view(task(id))}});
      }
      if (action !== "resume") throw Error("操作必须为pause、resume或cancel");
      const original = task(id); if (original.status !== "paused") throw Error("只有暂停任务可以恢复"); const stamp = fingerprint(original), ticket = epoch(id);
      if (o.ledger.accountingIssue(id)) throw Error("模型用量尚未核对，不能恢复");
      if (o.tasks.getClarificationAnswers(id).some(a => a.delivery === "unknown")) throw Error("答复投递结果未知，须先按实际收件证据对账；此入口不会自动重投");
      await approve("longtask_control", original, exec, ticket);
      return locked(id, async () => {assertCaller(exec); checkEpoch(id, ticket); const fresh = task(id); if (fresh.status !== "paused" || fingerprint(fresh) !== stamp) throw Error("恢复期间方案或状态已变化，未启动"); await o.tasks.bindRouter(id, exec.agent!.session!.id!); await o.tasks.resume(id); return {task: view(task(id))}});
    });
    define("longtask_tasks", "查看长任务列表或taskId指定的详情。返回真实状态、方案、人选、产物引用、验收进度、人民币总上限/核算费用/在途/剩余金额；历史任务保留Token口径。不唤醒任务，不转发专家全文。", object({taskId: taskIdSchema, offset: integer, limit: {type: "integer", minimum: 1, maximum: 50}}), async a => {
      if (a.taskId !== undefined) return {task: view(task(text(a.taskId, "任务ID"))), workspace: o.workspace};
      const all = o.tasks.listTasks().sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)), offset = a.offset === undefined ? 0 : number(a.offset, "offset"), limit = a.limit === undefined ? 20 : number(a.limit, "limit", 1);
      if (limit > 50) throw Error("limit最大为50");
      return {total: all.length, offset, workspace: o.workspace, tasks: all.slice(offset, offset + limit).map(t => ({taskId: t.id, goal: o.tasks.currentVersion(t).goal.slice(0, 240), goalTruncated: o.tasks.currentVersion(t).goal.length > 240, version: o.tasks.currentVersion(t).version, status: t.status, pauseReason: t.pauseReason, accountingBlocked: Boolean(o.ledger.accountingIssue(t.id)), budget: budgetView(t)}))};
    });
  } catch (error) {closed = true; closing.abort(); for (const stop of stops.reverse()) stop(); throw error}
  return {ready: true, async dispose() {if (closed) return; closed = true; closing.abort(); for (const stop of stops.reverse()) stop(); await Promise.allSettled([...calls])}};
}
