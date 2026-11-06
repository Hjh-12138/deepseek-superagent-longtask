import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

export const PAIR_TOOL = "longtask_e2e_pair_ready";
export const PAIR_VERSION = "pair-handoff-v4";

/** 只做测试握手与原事件重放；不写产物、不伪造专家报告或验收结论。 */
export function createPairFixture({ runId, sender, recipient, artifactPath, readSessions, replayTurnEnd, getDependency, log = () => {} }) {
  let taskId, closed = false, waiting = false, replay;
  const texts = new Map();
  const pending = new Set();
  const disposers = [];
  const report = (text) => {
    let last;
    for (const m of text.matchAll(/<longtask_report>\s*([\s\S]*?)\s*<\/longtask_report>/g)) {
      try { last = JSON.parse(m[1]); } catch { /* 只认有效 JSON */ }
    }
    return last;
  };
  const observe = (session, event) => {
    if (closed || !taskId) return;
    const role = readSessions(taskId).find((s) => s.sessionId === session.id && !s.revokedAt);
    if (!role || role.role !== "executor") return;
    if (event.type === "assistant/message") {
      const text = (event.data.message?.content ?? []).filter((b) => b.type === "text").map((b) => b.text).join("\n");
      const prior = texts.get(session.id);
      texts.set(session.id, { turn: event.data.turn, text: prior?.turn === event.data.turn ? `${prior.text}\n${text}` : text });
    }
    if (event.type !== "turn/end" || event.data.reason?.kind !== "completed") return;
    const current = texts.get(session.id);
    if (current?.turn !== event.data.turn) return;
    if (role.relPath === recipient) {
      const result = report(current.text);
      if (result?.done === false && typeof result.waiting === "string" && result.waiting.trim()) {
        waiting = true;
        for (const finish of [...pending]) finish();
      }
    }
    const targets = [...current.text.matchAll(/<longtask_message\b([^>]*)>/g)].map((m) => /\bto\s*=\s*"([^"]+)"/.exec(m[1])?.[1]);
    if (role.relPath !== sender || targets.length !== 1 || targets[0] !== recipient) return;
    const sourceTurnId = event.data.id ?? `${session.id}:turn:${event.data.turn}`;
    if (replay) {
      if (replay.sourceTurnId === sourceTurnId) replay.eventCount++;
      return;
    }
    // 同步重放到产品消费入口，覆盖并发处理；绝不向全局总线重复发布 seq。
    replay = { taskId, sourceTurnId, actionId: `${sourceTurnId}:msg:0`, eventCount: 1 };
    if (replayTurnEnd(taskId, session.id, sourceTurnId) !== true) throw Error("longtask 未确认事件重放");
    replay.eventCount = 2;
    log("pair_event_replayed", { ...replay });
  };
  return {
    version: PAIR_VERSION,
    bind(id) { taskId = id; },
    snapshot() { return replay ? { ...replay } : null; },
    install(ctx) {
      const tools = ctx.get?.("tools");
      if (typeof tools?.register !== "function" || typeof ctx.on !== "function" || typeof replayTurnEnd !== "function" || typeof getDependency !== "function") {
        throw Error("pair 缺少工具注册/宿主事件重放能力，不创建模型任务");
      }
      const disposeEvents = ctx.on("session/event", observe);
      if (typeof disposeEvents !== "function") throw Error("pair 事件订阅不可清理");
      disposers.push(disposeEvents);
      try {
        disposers.push(tools.register({
          name: PAIR_TOOL,
          description: "仅供 E2E-PAIR：A 写完比较表后调用，核对调度器已登记 B 的前置等待，再交接；无需启动 B 的模型报告等待。",
          parameters: { type: "object", additionalProperties: false, required: ["runId"], properties: { runId: { type: "string" } } },
          output: { schema: { type: "string" }, render: (_args, value) => [{ type: "text", text: value }] },
          async execute(args, exec) {
            const role = taskId && readSessions(taskId).find((s) => s.sessionId === exec.agent?.session?.id && !s.revokedAt);
            if (closed || args.runId !== runId || role?.relPath !== sender) throw Error("pair 握手不属于当前发送者");
            exec.signal.throwIfAborted();
            const dependency = getDependency(taskId, recipient);
            if (dependency?.state === "waiting" && dependency.fromRelPath === sender) waiting = true;
            if (!waiting) await new Promise((resolve, reject) => {
              const finish = (error) => {
                clearTimeout(timer); pending.delete(finish); exec.signal.removeEventListener("abort", abort);
                error ? reject(error) : resolve();
              };
              const abort = () => finish(Error("pair 握手已取消"));
              const timer = setTimeout(() => finish(Error("等待接收专家超时；本轮停止，不追加预算")), 60000);
              pending.add(finish); exec.signal.addEventListener("abort", abort, { once: true });
            });
            if (closed) throw Error("pair 已卸载");
            return JSON.stringify({ ready: true, artifactPath, sha256: createHash("sha256").update(readFileSync(artifactPath)).digest("hex") });
          },
        }));
      } catch (error) { for (const stop of disposers.splice(0)) stop(); throw error; }
    },
    dispose() {
      if (closed) return;
      closed = true;
      for (const finish of [...pending]) finish(Error("pair 已卸载"));
      for (const stop of disposers.splice(0)) stop();
    },
  };
}
