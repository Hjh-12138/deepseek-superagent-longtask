#!/usr/bin/env node
/**
 * 长任务诊断器：回答"预算跑完了为什么还没到 delivered"。
 *
 * 用法：
 *   node scripts/diagnose.mjs                          # 用默认 dataDir，诊断最近一条任务
 *   node scripts/diagnose.mjs <dataDir>                # 指定 dataDir
 *   node scripts/diagnose.mjs <dataDir> <taskId>       # 指定任务
 *
 * 只读：不修改 state.json，不改任务状态，不发任何投递。
 *
 * 输出四块：账本（钱够不够、花在谁身上）、事件时间线、验收轮次与检查点、
 * 判定结论（属于哪一类失败 + 下一步动作），最后附 plugin.log 里的关键诊断行。
 */

import { readFileSync, existsSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { homedir } from "node:os";
import {formatCny} from "../dist/src/ledger/money.js";

const DEFAULT_DATA_DIR = process.env.DSH_LONGTASK_DATA_DIR || join(homedir(), ".dsh", "longtask");
const dataDir = resolve(process.argv[2] ?? DEFAULT_DATA_DIR);
const wantedTaskId = process.argv[3];

const die = (msg) => {
  console.error(`✗ ${msg}`);
  process.exit(1);
};

const statePath = join(dataDir, "state.json");
if (!existsSync(statePath)) die(`找不到 ${statePath}`);

const state = JSON.parse(readFileSync(statePath, "utf8"));
const C = state.collections ?? {};
const list = (name) => Object.values(C[name] ?? {});
const eventsOf = (taskId) =>
  (state.eventOrder ?? [])
    .map((id) => state.events?.[id])
    .filter((e) => e && e.taskId === taskId)
    .sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));

const sessionsOf = (taskId) =>
  list("taskSessions")
    .flatMap((group) => Object.values(group ?? {}))
    .filter((s) => s?.taskId === taskId);

// ---- 选任务 ----
const tasks = list("tasks");
if (tasks.length === 0) die("state.json 里没有任何任务");
const pick = (t) => String(t.updatedAt ?? t.createdAt ?? "");
const task = wantedTaskId
  ? tasks.find((t) => t.id === wantedTaskId) ?? die(`任务不存在: ${wantedTaskId}`)
  : [...tasks].sort((a, b) => pick(b).localeCompare(pick(a)))[0];

const budget = list("budgets").find((b) => b.taskId === task.id);
const version = task.versions?.[task.currentVersion];
const sessions = sessionsOf(task.id);
const artifacts = list("artifacts").filter((a) => a.taskId === task.id);
const rounds = list("acceptanceRounds").filter((r) => r.taskId === task.id);
const entries = list("ledger").filter((e) => e.taskId === task.id);
const events = eventsOf(task.id);

const n = (v) => (typeof v === "number" && Number.isFinite(v) ? v : null);
const fmt = (v) => (n(v) === null ? "损坏(null/NaN)" : n(v).toLocaleString("en-US"));
const pad = (s, w) => String(s).padEnd(w);
const line = (ch = "-") => console.log(ch.repeat(72));

// ---- 1. 概览 ----
console.log("");
console.log(`长任务诊断 · ${dataDir}`);
line("=");
console.log(`任务      ${task.id}`);
console.log(`目标      ${String(version?.goal ?? "").slice(0, 80)}`);
console.log(`状态      ${task.status}${task.pauseReason ? `（暂停原因: ${task.pauseReason}）` : ""}`);
console.log(`版本      v${version?.version ?? "?"}  标准 ${version?.criteria?.length ?? 0} 条  交付物 ${version?.deliverables?.length ?? 0} 个`);

// ---- 2. 账本 ----
console.log("");
console.log("## 账本");
const limit = n(budget?.limitTokens);
const settled = n(budget?.settledTokens);
const reserved = n(budget?.reservedTokens);
const money = budget?.money;
if (money) {
  console.log(`人民币限额 ¥${formatCny(money.limitNano)} ｜ 核算 ¥${formatCny(money.settledNano)} ｜ 在途 ¥${formatCny(money.reservedNano)}`);
  console.log(`可用 ¥${formatCny(BigInt(money.limitNano)-BigInt(money.settledNano)-BigInt(money.reservedNano))}；按用量与价格快照核算，不是平台实扣。`);
} else console.log(`Token限额 ${fmt(limit)} ｜ 已结算 ${fmt(settled)} ｜ 在途预留 ${fmt(reserved)}`);
if (!money && limit !== null && settled !== null && reserved !== null) {
  const available = limit - settled - reserved;
  console.log(`可用 ${available.toLocaleString("en-US")}${available < 0 ? "  ← 已超限（旧账允许透支，硬准入后不会再发生）" : ""}`);
}
if (budget?.grants?.length) {
  for (const g of budget.grants) console.log(`  追加 +${g.tokens}（${g.at}）${g.note ?? ""}`);
}

// ---- 3. 消耗归集：钱花在谁身上 ----
console.log("");
console.log("## 消耗归集（按会话/角色）");
const bySession = new Map(sessions.map((s) => [s.sessionId, s]));
const agg = new Map();
for (const e of entries) {
  const sid = e.money?.sessionId ?? /会话 ([0-9a-f-]+)/.exec(e.note ?? "")?.[1] ?? "other";
  const a = agg.get(sid) ?? { role: e.money?.role ?? bySession.get(sid)?.role ?? "?", rel: e.money?.expertRelPath ?? bySession.get(sid)?.relPath ?? "?", requests: 0, input: 0, output: 0, nano: 0n, last: "" };
  a.requests += 1;
  a.input += n(e.inputTokens) ?? 0;
  a.output += n(e.outputTokens) ?? 0;
  if (e.money) a.nano += BigInt(e.money.chargedNano);
  a.last = String(e.createdAt ?? "").slice(11, 19);
  agg.set(sid, a);
}
const totals = [...agg.values()].reduce((s, a) => s + a.input + a.output, 0);
if (agg.size === 0) console.log("（没有已结算的用量记录）");
for (const [sid, a] of [...agg.entries()].sort((x, y) => y[1].input + y[1].output - (x[1].input + x[1].output))) {
  const sum = a.input + a.output;
  const pct = totals > 0 ? Math.round((sum / totals) * 100) : 0;
  console.log(
    `${pad(a.role, 9)} ${sid.slice(0, 8)}  请求 ${String(a.requests).padStart(2)} 次  ` +
      `输入 ${fmt(a.input).padStart(9)} 输出 ${fmt(a.output).padStart(6)}  合计 ${fmt(sum).padStart(9)}  占比 ${String(pct).padStart(3)}%`,
  );
  if (a.rel !== "?") console.log(`          ${a.rel}`);
  if (money) console.log(`          核算费用 ¥${formatCny(a.nano)}`);
}
console.log(`总计 ${totals.toLocaleString("en-US")}`);

// ---- 4. 事件时间线 ----
console.log("");
console.log("## 事件时间线");
if (events.length === 0) console.log("（无事件）");
for (const e of events) {
  const payload = JSON.stringify(e.payload ?? {});
  console.log(`${String(e.createdAt).slice(11, 19)}  ${pad(e.type, 22)} ${payload.slice(0, 96)}`);
}

// ---- 5. 验收轮次 ----
console.log("");
console.log("## 验收轮次");
if (rounds.length === 0) console.log("（未登记过验收轮次）");
for (const r of rounds) {
  const cp = r.checkpoint;
  console.log(
    `${r.roundId}  状态 ${pad(r.state, 8)} 标准版本 v${r.criteriaVersion}  冻结产物 ${Object.keys(r.artifactHashes ?? {}).length} 个` +
      (cp ? `  检查点: 已用 ${cp.steps} 步 / 已核 ${cp.checkedCriteria?.length ?? 0} 条` : ""),
  );
  if (cp?.lastSummary) console.log(`   上一轮摘要: ${String(cp.lastSummary).replace(/\s+/g, " ").slice(0, 160)}`);
}

// ---- 6. 产物 ----
console.log("");
console.log("## 产物与交付物");
const base = (p) => String(p).replace(/\\/g, "/").split("/").pop().toLowerCase();
const registered = new Set(artifacts.filter((a) => a.contentHash).map((a) => base(a.location)));
for (const d of version?.deliverables ?? []) {
  const ok = registered.has(base(d));
  console.log(`${ok ? "✓" : "✗"} ${d}${ok ? "" : "   ← 未登记产物：验收前预检会直接退回，不会开验收会话"}`);
}

// ---- 7. 判定 ----
console.log("");
line("=");
console.log("## 判定");
const verdicts = [];
const roleShare = (role) => {
  const rows = [...agg.values()].filter((a) => a.role === role);
  const sum = rows.reduce((s, a) => s + a.input + a.output, 0);
  return totals > 0 ? sum / totals : 0;
};
const pendingRound = rounds.find((r) => r.state === "pending");

const accountingIssue = C.accountingIssues?.[task.id];
if (accountingIssue) {
  verdicts.push(["模型用量待核对", `${accountingIssue.reason}；保留预留或重启保守结算，不允许通过resume或加额绕过。`]);
} else if (task.status === "delivered") {
  verdicts.push(["已完成", "任务已交付，无需处置。"]);
} else if (!budget) {
  verdicts.push(["未确认", "没有预算账本：任务可能还没 confirm。"]);
} else if (settled === null || reserved === null) {
  verdicts.push(["账本损坏", "settled/reserved 不是有限数（历史 NaN 污染）：先修账本再谈追加，否则任何准入判断都不可信。"]);
} else if (task.status === "paused" && task.pauseReason === "budget") {
  if (money) {
    verdicts.push(["金额预算或价格准入暂停", `核算 ¥${formatCny(money.settledNano)} / 总上限 ¥${formatCny(money.limitNano)}；查看暂停事件的具体原因，可能是验收保留、剩余额度或价格失效。没有自动追加。`]);
  } else {
  const acceptorShare = roleShare("acceptor");
  verdicts.push(["预算耗尽", `已结算 ${settled.toLocaleString("en-US")} / 限额 ${limit?.toLocaleString("en-US")}。`]);
  if (acceptorShare > 0.4) {
    verdicts.push([
      "主因是验收，不是干活",
      `验收占 ${Math.round(acceptorShare * 100)}% 的消耗。若追加额度只是让验收继续同样地跑，钱会再烧一遍——先检查验收请求是否带范围约束与检查点，再决定是否追加。`,
    ]);
  }
  if (pendingRound) verdicts.push(["有待验收轮次", `追加额度 + resume 后会用检查点续接 ${pendingRound.roundId}，不会从头重核。`]);
  else verdicts.push(["无待验收轮次", "追加额度 + resume 后应唤醒执行者续做；若它仍无产出，问题在工具能力或任务设计，加钱无用。"]);
  }
} else if (task.status === "paused" && task.pauseReason === "missing_input") {
  verdicts.push(["等人工裁决", "通常是验收步数用尽或 Watchdog 求助：看上面的检查点摘要，人工判 pass/gap，或改任务后重开。"]);
} else if (task.status === "awaiting_acceptance") {
  verdicts.push(["卡在待验收", "验收请求已投递但没有有效结论：检查验收会话是否还活着、是否被催办过（日志 acceptance:nudged / no-verdict-cap）。"]);
} else if (task.status === "rework") {
  const missing = (version?.deliverables ?? []).filter((d) => !registered.has(base(d)));
  verdicts.push(["验收退回执行中", missing.length > 0 ? `还缺产物：${missing.join("、")}` : "产物齐备，问题在标准是否可满足（看最近一次 gap 结论）。"]);
} else if (task.status === "cancelled") {
  verdicts.push(["已取消", "人为中止，会话已停止。需要继续请重开任务（继承已用量）。"]);
} else {
  verdicts.push([task.status, "见上方时间线。"]);
}
for (const [k, v] of verdicts) console.log(`- ${k}：${v}`);

// ---- 8. 日志关键行 ----
const logPath = join(dataDir, "plugin.log");
if (existsSync(logPath)) {
  console.log("");
  console.log("## 日志关键行（优先本任务的，最近 12 条）");
  const keys = /budget:reject|budget:acceptor-step-cap|budget:reserve|acceptance:|usage:no-reservation|usage:settle-error|usage:invalid|artifact:registered|session:preset|INIT-ERROR/;
  const matched = readFileSync(logPath, "utf8").split(/\r?\n/).filter((l) => keys.test(l));
  const mine = matched.filter((l) => l.includes(task.id));
  const rows = (mine.length > 0 ? mine : matched).slice(-12);
  if (rows.length === 0) console.log("（无匹配）");
  for (const r of rows) console.log(`  ${r.slice(0, 180)}`);
}

// ---- 9. 其它任务 ----
if (tasks.length > 1) {
  console.log("");
  console.log("## 其它任务");
  for (const t of tasks.filter((t) => t.id !== task.id).slice(-8)) {
    const b = list("budgets").find((x) => x.taskId === t.id);
    console.log(`  ${t.id}  ${pad(t.status, 20)} ${pad(t.pauseReason ?? "-", 14)} ${b?.money ? `核算=¥${formatCny(b.money.settledNano)}` : `Token settled=${fmt(b?.settledTokens)}`}`);
  }
}
console.log("");
