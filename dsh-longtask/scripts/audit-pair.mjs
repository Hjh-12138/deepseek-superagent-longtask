// Read-only audit of an already finished pair. Never starts or resumes agents.
import { readFileSync, writeFileSync, readdirSync, existsSync, mkdirSync, copyFileSync } from "node:fs";
import { resolve, join, basename } from "node:path";
import { homedir } from "node:os";
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { JsonStore } from "../dist/src/store/store.js";
import { TokenLedger } from "../dist/src/ledger/token-ledger.js";
import { TaskService } from "../dist/src/tasks/task-service.js";
import { parseWorkerReport } from "../dist/src/index.js";
import { extractPairEvidence, assessScenario } from "../../dsh-longtask-e2e/evidence.mjs";

const root = resolve(import.meta.dirname, "..");
const taskId = process.argv[2];
if (!taskId) throw Error("用法：node scripts/audit-pair.mjs <已结束任务ID> [证据目录]");
const output = resolve(process.argv[3] ?? join(root, ".longtask/verification", `pair-${taskId}`));
mkdirSync(output, { recursive: true });
const state = JSON.parse(readFileSync(join(root, ".longtask/state.json"), "utf8"));
const task = state.collections.tasks?.[taskId];
if (!task || !["delivered", "cancelled"].includes(task.status)) throw Error("只审计已结束任务，不操作运行中任务");
const values = (name) => Object.values(state.collections[name] ?? {}).filter((v) => v.taskId === taskId);
const sessions = Object.values(state.collections.taskSessions?.[taskId] ?? {});
const direct = values("messages").filter((m) => m.kind === "expert_direct");
if (direct.length !== 1) throw Error("本审计要求一条可明确追溯的 A→B 交接");
const receiver = sessions.find((s) => s.relPath === direct[0].toRelPath && s.role === "executor");
const sha = (data) => createHash("sha256").update(data).digest("hex");
const sessionRoot = join(homedir(), ".dsh/sessions");
const parents = readdirSync(sessionRoot, { withFileTypes: true }).filter((d) => d.isDirectory());
const decoded = new Map();
const sessionAudit = [];
for (const session of sessions) {
  const parent = parents.find((d) => existsSync(join(sessionRoot, d.name, session.sessionId, "session.v4.jsonl.zstd")));
  if (!parent) throw Error(`缺少宿主原始会话：${session.sessionId}`);
  const path = join(sessionRoot, parent.name, session.sessionId, "session.v4.jsonl.zstd");
  const bytes = readFileSync(path); let offset = 0, text = "";
  while (offset < bytes.length) {
    const frame = zstdDecompressSync(bytes.subarray(offset), { info: true });
    if (!(frame.engine.bytesWritten > 0)) throw Error("无效 zstd 串接帧");
    offset += frame.engine.bytesWritten; text += frame.buffer.toString("utf8");
  }
  const events = text.trim().split("\n").map(JSON.parse);
  decoded.set(session.sessionId, events);
  copyFileSync(path, join(output, `${session.sessionId}.jsonl.zstd`));
  const used = events.filter((e) => e.type === "assistant/message" && e.data.usage).reduce((sum, e) => {
    const u = e.data.usage;
    return sum + (u.inputTokens ?? u.uncachedInputTokens) + u.cacheReadTokens + u.cacheWriteTokens + u.outputTokens;
  }, 0);
  const ledgerUsed = values("ledger").filter((e) => e.note?.includes(session.sessionId)).reduce((n, e) => n + e.inputTokens + e.outputTokens, 0);
  sessionAudit.push({ sessionId: session.sessionId, role: session.role, relPath: session.relPath,
    rawSha256: sha(bytes), actualTokens: used, ledgerTokens: ledgerUsed, matches: used === ledgerUsed,
    steps: events.filter((e) => e.type === "assistant/message" && e.data.usage).length,
    toolErrors: events.filter((e) => e.type === "tool/result" && e.data.message?.isError).length,
    descendants: events.filter((e) => e.type === "subagent/catalog").length });
}

const observations = [], calls = new Map(); let turnText = "", currentTurn;
for (const e of decoded.get(receiver.sessionId)) {
  if (!e.type || e.time === undefined) continue; // session 文件首行是头，不是事件
  const d = e.data ?? {};
  const base = { taskId, sessionId: receiver.sessionId, instanceId: `host-raw:${receiver.sessionId}`,
    order: e.seq, at: new Date(e.time).toISOString(), origin: "host-session-raw", seq: e.seq };
  if (e.type === "assistant/message") {
    if (currentTurn !== d.turn) { currentTurn = d.turn; turnText = ""; }
    turnText += "\n" + (d.message?.content ?? []).filter((b) => b.type === "text").map((b) => b.text).join("\n");
  }
  if (e.type === "turn/end") observations.push({ ...base, kind: "turn_end", turn: d.turn, reason: d.reason?.kind,
    waiting: d.reason?.kind === "completed" && currentTurn === d.turn ? parseWorkerReport(turnText)?.waiting ?? null : null });
  if (e.type === "user/message" && d.source?.dedupeKey === `${direct[0].dedupeKey}#deliver`) observations.push({ ...base, kind: "consumed", dedupeKey: d.source.dedupeKey, messageId: d.id });
  if (e.type === "tool/call" && d.name === "read") calls.set(d.callId, { path: JSON.parse(d.arguments).file_path, turn: d.turn });
  if (e.type === "tool/result" && !d.message?.isError) {
    const call = calls.get(d.message?.toolCallId);
    if (!call || !direct[0].artifactRefs?.includes(call.path)) continue;
    const bytes = readFileSync(call.path), meta = d.meta;
    const lines = bytes.toString("utf8").split(/\r?\n/); if (lines.at(-1) === "") lines.pop();
    // 不凭当前文件推断旧读取：SDK meta 必须含全部实际返回行，并逐行匹配封存文件。
    const fullReadMatches = meta?.path === call.path && meta.offset === 1 && meta.totalLines === lines.length
      && meta.lines?.length === lines.length && meta.lines.every((l, i) => l.number === i + 1 && l.text === lines[i]);
    if (!fullReadMatches || sha(bytes) !== direct[0].artifactVersions?.[call.path]) throw Error("原始 read 快照不能证明交接版本");
    observations.push({ ...base, kind: "artifact_read", turn: call.turn, path: call.path, hash: sha(bytes), toolCallId: d.message.toolCallId, fullReadMatches });
  }
}

const runFile = readdirSync(join(root, ".longtask/e2e")).filter((f) => /^run-.*\.jsonl$/.test(f)).find((f) =>
  readFileSync(join(root, ".longtask/e2e", f), "utf8").includes(`"taskId":"${taskId}"`));
if (!runFile) throw Error("缺少本轮真实联调日志");
const run = readFileSync(join(root, ".longtask/e2e", runFile), "utf8").trim().split("\n").map(JSON.parse);
const ended = run.find((e) => e.kind === "scenario_end" && e.taskId === taskId);
const replay = run.find((e) => e.kind === "pair_event_replayed" && e.taskId === taskId);
if (!ended || !replay) throw Error("缺少结束或真实重放证据");
copyFileSync(join(root, ".longtask/e2e", runFile), join(output, runFile));
const store = new JsonStore(join(output, "never-written.json"), state);
const service = new TaskService({ store, ledger: new TokenLedger(store), host: {} });
const digest = service.routerDigest(taskId);
const stage2 = extractPairEvidence({ state, digest, taskId, observations, replay });
const coverage = assessScenario({ ...ended, taskId, scenario: "pair", stage2 });
const collaborationCoverage = structuredClone(coverage);
const budget = state.collections.budgets[taskId];
const actualTokens = sessionAudit.reduce((n, s) => n + s.actualTokens, 0);
const accountingMatches = sessionAudit.every((s) => s.matches) && actualTokens === budget.settledTokens && !sessionAudit.some((s) => s.descendants);
if (!accountingMatches) coverage.reasons.push("原始会话文件用量未齐，不能完成独立逐项对账");
coverage.passed = coverage.passed && accountingMatches;
const latestArtifacts = new Map();
for (const a of values("artifacts")) latestArtifacts.set(a.location, a);
const artifacts = [...latestArtifacts.values()].map((a) => {
  const bytes = readFileSync(a.location); copyFileSync(a.location, join(output, basename(a.location)));
  return { ...a, actualSha256: sha(bytes), matches: sha(bytes) === a.contentHash };
});
if (!artifacts.every((a) => a.matches)) { coverage.passed = false; coverage.reasons.push("最终产物哈希不符"); }
const filtered = { collections: Object.fromEntries(Object.keys(state.collections).map((key) => [key,
  key === "taskSessions" ? { [taskId]: state.collections[key][taskId] } : Object.fromEntries(Object.entries(state.collections[key]).filter(([id, v]) => id === taskId || v.taskId === taskId))])),
  events: Object.fromEntries(Object.entries(state.events).filter(([, e]) => e.taskId === taskId)) };
writeFileSync(join(output, "state-task.json"), JSON.stringify(filtered, null, 2));
writeFileSync(join(output, "observations-from-host-raw.json"), JSON.stringify(observations, null, 2));
writeFileSync(join(output, "router-digest.json"), JSON.stringify(digest, null, 2));
const report = { taskId, mode: "offline re-evaluation of the same real run; no new model calls",
  originalCoverage: ended.coverage, collaborationCoverage, coverage, stage2, budget,
  durableRawTokens: actualTokens, hostProjectionTokens: ended.accounting?.actualTokens, accountingMatches,
  sessions: sessionAudit, artifacts, rounds: values("acceptanceRounds"), replay };
writeFileSync(join(output, "audit.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify({ taskId, originalCoverage: ended.coverage, collaborationCoverage, coverage,
  durableRawTokens: actualTokens, hostProjectionTokens: ended.accounting?.actualTokens, accountingMatches, stage2, output }, null, 2));
process.exitCode = coverage.passed ? 0 : 2;
