import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";

export const GAP_FIXTURE_VERSION = "gap-before-freeze-v2";
export const GAP_BODY = "# 变更内容\n修复配置并保留处理记录。\n# 回滚与降级\n恢复原配置并核对结果。";
const hash = (value) => createHash("sha256").update(value).digest("hex");
const specHash = (view) => {
  const { confirmedAt: _confirmationMetadata, ...spec } = view.version ?? {};
  return hash(JSON.stringify(spec));
};
const doneReport = (message) => {
  if (message?.content?.some((b) => b.type === "tool-call")) return false;
  const text = (message?.content ?? []).filter((b) => b.type === "text").map((b) => b.text).join("\n");
  for (const match of text.matchAll(/<longtask_report>\s*([\s\S]*?)\s*<\/longtask_report>/g)) {
    try { if (JSON.parse(match[1]).done === true) return true; } catch { /* 非报告正文 */ }
  }
  return false;
};
const validBody = (text) => {
  const lines = text.trimEnd().split(/\r?\n/);
  return (lines.length === 4 || lines.length === 5) && lines.slice(0, 4).join("\n") === GAP_BODY;
};

/** 明确标记的测试故障：最终完成报告同步注入，先于产品turn/end的异步哈希登记。 */
export function createGapFixture({ runId, artifactPath, marker, readView, log = () => {}, onFailure = async () => {} }) {
  let taskId, expectedSpec, worker, failure;
  let beforeHash, injectedHash, repairedHash, firstRoundId, secondRoundId;
  let closed = false;
  let order = 0;
  const phases = {};
  const seenReports = new Set();
  const disposers = [];
  const fail = (error) => {
    if (failure || closed) return;
    failure = String(error?.message ?? error);
    log("fault_failed", { taskId, version: GAP_FIXTURE_VERSION, message: failure });
    void Promise.resolve().then(() => onFailure(taskId, failure)).catch((e) => log("fault_pause_failed", { taskId, message: String(e) }));
  };
  const check = (view) => {
    if (!view?.task || !view.version) throw Error("无法读取本轮任务与标准版本");
    if (specHash(view) !== expectedSpec) throw Error("测试期间任务标准或目标发生变化");
  };
  const roleOf = (view, sid) => view.sessions?.find((s) => s.sessionId === sid)?.role;
  const artifactOf = (view) => view.artifacts?.filter((a) => resolve(a.location) === resolve(artifactPath)).at(-1);
  const recordPhase = (name, kind, extra = {}) => {
    phases[name] = ++order;
    log(kind, { version: GAP_FIXTURE_VERSION, runId, taskId, order, ...extra });
  };
  const refresh = (view) => {
    if (!phases.injected) return;
    const gap = view.events?.find((e) => e.type === "task.verdict" && e.payload?.roundId === firstRoundId
      && e.payload.verdicts?.some((v) => v.criterionId === "c2" && v.outcome === "gap"));
    if (gap && !phases.gap) recordPhase("gap", "fault_gap_observed", { roundId: firstRoundId });
  };
  const observe = (session, event) => {
    if (closed || failure || !taskId || !["step/start", "assistant/message"].includes(event.type)) return;
    try {
      const view = readView(taskId);
      const role = roleOf(view, session.id);
      if (!role) return;
      check(view); refresh(view);
      if (role === "acceptor" && event.type === "step/start") {
        const round = view.rounds?.find((r) => r.state === "pending");
        if (!round) throw Error("验收步骤缺少待核对轮次");
        const artifact = artifactOf(view);
        const frozen = artifact && round.artifactHashes?.[artifact.id];
        if (!firstRoundId) {
          if (!phases.injected || frozen !== injectedHash || hash(readFileSync(artifactPath)) !== injectedHash) throw Error("首轮冻结的不是注入缺项版本");
          firstRoundId = round.roundId;
          recordPhase("firstFrozen", "fault_frozen", { roundId: firstRoundId, contentHash: frozen });
        } else if (round.roundId !== firstRoundId && !secondRoundId) {
          if (!phases.gap || !phases.repaired || frozen !== repairedHash || hash(readFileSync(artifactPath)) !== repairedHash) throw Error("再次验收缺少真实退回修复或修复版本冻结证据");
          secondRoundId = round.roundId;
          recordPhase("secondFrozen", "fault_repair_frozen", { roundId: secondRoundId, contentHash: frozen });
        }
        return;
      }
      if (role !== "executor" || event.type !== "assistant/message" || !doneReport(event.data?.message)) return;
      const reportKey = `${session.id}:${event.seq ?? event.data.message.id ?? `${event.data.turn}:${event.data.step ?? ""}:${hash(JSON.stringify(event.data.message))}`}`;
      if (seenReports.has(reportKey)) return;
      seenReports.add(reportKey);
      if (!phases.injected) {
        if (view.task.status !== "active" || view.rounds?.length) throw Error("首轮已经进入验收，禁止冻结后再修改文件");
        const original = readFileSync(artifactPath, "utf8");
        if (!validBody(original) || original.trimEnd().split(/\r?\n/).at(-1) !== marker) throw Error("执行者首轮未写齐标准，不能冒充测试注入");
        const corrupted = original.slice(0, original.lastIndexOf(marker));
        beforeHash = hash(original); injectedHash = hash(corrupted);
        writeFileSync(artifactPath, corrupted, { encoding: "utf8", flag: "w" });
        if (hash(readFileSync(artifactPath)) !== injectedHash) throw Error("缺项注入未完整落盘");
        worker = session.id;
        recordPhase("injected", "fault_injected", { sessionId: worker, seq: event.seq ?? null, turn: event.data.turn, step: event.data.step,
          beforeHash, injectedHash, note: "完成报告已到，验收尚未冻结；只移除最后一行校验标记" });
      } else if (!phases.repaired) {
        if (view.task.status !== "rework" || !phases.gap || session.id !== worker) throw Error("原执行者尚未收到真实gap返工，禁止把重复声明算作修复");
        const content = readFileSync(artifactPath, "utf8");
        if (!validBody(content) || content.trimEnd().split(/\r?\n/).at(-1) !== marker) throw Error("返工声明完成时缺项仍未补齐");
        repairedHash = hash(content);
        recordPhase("repaired", "fault_repaired", { sessionId: worker, contentHash: repairedHash, note: "仅观测原Worker实际修复，夹具未恢复文件" });
      }
    } catch (error) { fail(error); }
  };
  const snapshot = () => {
    let artifactMatches = false, criteriaUnchanged = false;
    try {
      const view = readView(taskId);
      criteriaUnchanged = specHash(view) === expectedSpec;
      const text = readFileSync(artifactPath, "utf8");
      artifactMatches = Boolean(repairedHash && hash(text) === repairedHash && validBody(text) && text.trimEnd().split(/\r?\n/).at(-1) === marker);
    } catch { /* 证据不足 */ }
    return { version: GAP_FIXTURE_VERSION, taskId, runId, injected: Boolean(phases.injected), repaired: Boolean(phases.repaired),
      worker, beforeHash, injectedHash, repairedHash, firstRoundId, secondRoundId, phases: { ...phases }, artifactMatches, criteriaUnchanged, failure: failure ?? null };
  };
  const guard = (exec) => {
    if (closed || !taskId) return;
    try {
      const role = roleOf(readView(taskId), exec.agent?.session?.id);
      if (!role) return;
      if (["pwsh", "powershell", "bash", "shell", "exec", "exec_command"].includes(exec.name)) return "本轮测试使用write/edit与read文件工具，不运行终端命令；完成后直接报告，验收只读产物。";
      if (role === "acceptor" && !["read", "glob", "working_directory"].includes(exec.name)) return "本轮测试的验收只允许read/glob读取产物，不得修改文件或调用其他能力。";
    } catch (error) { fail(error); return "测试状态不可读，本轮暂停核对。"; }
  };
  return {
    version: GAP_FIXTURE_VERSION, observe, guard, snapshot,
    bind(id) {
      if (taskId) throw Error("故障夹具不可复用任务");
      taskId = id;
      const view = readView(id);
      if (!view?.version || view.task?.status !== "proposed" || view.rounds?.length) throw Error("必须绑定尚未确认且没有验收轮次的新任务");
      expectedSpec = specHash(view);
    },
    install(ctx) {
      const tools = ctx.get("tools");
      if (typeof tools?.guard !== "function" || typeof ctx.on !== "function") throw Error("gap-inject缺少tools.guard或session/event，不创建模型任务");
      const stopGuard = tools.guard(guard);
      try {
        const stopEvents = ctx.on("session/event", observe);
        if (typeof stopEvents !== "function") throw Error("session/event未返回清理函数");
        disposers.push(stopEvents, stopGuard);
      } catch (error) { stopGuard(); throw error; }
    },
    dispose() { if (closed) return; closed = true; for (const stop of disposers) stop(); },
  };
}
