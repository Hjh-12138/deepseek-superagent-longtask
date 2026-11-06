import { randomUUID } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export const WRAPUP_TOOL = "e2e_wrapup_checkpoint";
export const WRAPUP_FIXTURE_VERSION = "checkpoint-v2";
// 对齐默认执行上限24步减去收尾窗口3步；实际预算闸门可更早停止。
export const MAX_CHECKS = 21;

/** 测试夹具，不改变生产策略、不发送控制消息、不生成验收结论。 */
export function createWrapupFixture({ runId, directory, ownsSession, ownsAcceptorSession = () => false, log = () => {}, onExhausted = async () => {}, onReceipt = async () => {} }) {
  const receiptPath = join(directory, "wrapup-receipt.json");
  let taskId;
  let worker;
  let ordinal = 0;
  let explored = 0;
  let control;
  let receipt;
  let registration;
  let receiptIssued = false;
  let closed = false;
  let exhausted = false;
  let stop;
  const sessions = new Map();
  const replies = new Map();
  const owns = (sid) => taskId && ownsSession(taskId, sid);
  const snapshot = (artifactPath) => {
    let artifactMatches = false;
    let receiptFileMatches = false;
    if (receipt) {
      try { artifactMatches = readFileSync(artifactPath, "utf8").trimEnd().split(/\r?\n/).at(-1) === receipt.token; } catch { /* 尚未落盘 */ }
      try { receiptFileMatches = readFileSync(receiptPath, "utf8") === JSON.stringify(receipt, null, 2); } catch { /* 缺失即失败 */ }
    }
    return {
      version: WRAPUP_FIXTURE_VERSION, runId, taskId, worker, explored,
      pendingAtControl: control?.pending === true,
      controlOrder: control?.order ?? null, receiptOrder: receipt?.order ?? null,
      receiptIssued, receiptPath, artifactMatches, receiptFileMatches,
    };
  };
  const observe = (session, event) => {
    if (closed || !owns(session.id)) return;
    let s = sessions.get(session.id);
    if (!s) { s = { open: false, step: null }; sessions.set(session.id, s); }
    if (event.type === "step/start") { s.open = true; s.step = `${event.data.turn}:${event.data.step}`; }
    if (event.type === "turn/end") s.open = false;
    if (event.type === "user/message" && event.data?.source?.kind === "longtask"
      && event.data.source.dedupeKey === `wrap-up:${taskId}:v1` && !control) {
      // 只认真实 surface 消费，不认入队、提示词里的“收尾”或其他任务的消息。
      control = { order: ++ordinal, pending: s.open && explored > 0 && !receipt, sessionId: session.id };
      log("wrapup_control_observed", { ...snapshot(), sessionId: session.id });
    }
  };
  const tool = {
    name: WRAPUP_TOOL,
    description: "仅供 E2E-WRAPUP 测试：advance 一次领取一项检查；收到 longtask 真实预算收尾后用 close 领取结束回执。其他任务不能使用。",
    parameters: {
      type: "object", additionalProperties: false, required: ["runId", "action", "cursor"],
      properties: { runId: { type: "string" }, action: { type: "string", enum: ["advance", "close"] }, cursor: { type: "integer", minimum: 0 } },
    },
    output: { schema: { type: "string" }, render: (_args, value) => [{ type: "text", text: value }] },
    async execute(args, exec) {
      exec.signal.throwIfAborted();
      const sid = exec.agent?.session?.id;
      if (closed || args?.runId !== runId || !owns(sid) || (worker && worker !== sid)) throw Error("检查点不属于当前运行的执行会话");
      if (args.action !== "advance" && args.action !== "close") throw Error("无效检查点动作");
      if (!Number.isSafeInteger(args.cursor) || args.cursor < 0) throw Error("必须使用上次返回的整数nextCursor，首次cursor=0");
      const s = sessions.get(sid);
      if (!s?.open || !s.step) throw Error("未观察到活动模型步骤，不能领取检查点");
      worker = sid;
      if (exhausted) return JSON.stringify({ status: "exhausted", instruction: "本轮已停止，不再领取回执或重试。" });
      if (args.action === "close") {
        if (args.cursor !== explored) throw Error(`过期检查点，当前cursor=${explored}`);
        if (!control || control.sessionId !== sid || !control.pending) return JSON.stringify({ status: "pending", instruction: "尚未消费真实预算收尾。每个模型请求调用一次 advance 继续检查，不要等待外部输入或猜测回执。" });
        if (!receipt) {
          receipt = { runId, taskId, sessionId: sid, order: ++ordinal, controlOrder: control.order, token: `WRAPUP-${randomUUID()}` };
          writeFileSync(receiptPath, JSON.stringify(receipt, null, 2), { encoding: "utf8", flag: "wx" });
        }
        // 登记后验收才允许读取；必须发生在执行者提交完成、冻结验收产物之前。
        registration ??= Promise.resolve().then(() => onReceipt(taskId, receiptPath));
        await registration;
        exec.signal.throwIfAborted();
        if (closed || !s.open) throw Error("结束窗口已关闭，不再发回执");
        if (!receiptIssued) { receiptIssued = true; log("wrapup_receipt_issued", snapshot()); }
        return JSON.stringify({ status: "close", receipt: receipt.token, receiptPath, receiptRecordWritten: true, artifactRegistered: true,
          instruction: "夹具已经写入并登记回执记录，以上token就是记录原值，无需再确认回执文件。立即调用write写入八条要点与末行回执；write成功后直接输出done=true的longtask_report。不要运行pwsh或终端命令，不要glob或读取回执，后续核对交给独立验收。" });
      }
      if (control) return JSON.stringify({ status: "closing", instruction: "已消费预算收尾；停止检查，调用 close 领取结束回执。" });
      // 同一模型请求的并行调用和 run_code 内循环不能提前走完检查链。
      const key = `${sid}:${s.step}`;
      if (replies.has(key)) return replies.get(key);
      if (args.cursor !== explored) throw Error(`过期检查点，当前cursor=${explored}，禁止重放旧检查`);
      explored++;
      if (explored === MAX_CHECKS) {
        exhausted = true;
        log("wrapup_fixture_exhausted", snapshot());
        await onExhausted(taskId);
        return JSON.stringify({ status: "exhausted", instruction: "检查次数上限已到但未消费收尾，场景未覆盖，本轮已暂停；不要继续或申请追加预算。" });
      }
      const reply = JSON.stringify({ status: "checking", checked: explored, nextCursor: explored, maxChecks: MAX_CHECKS,
        instruction: "本项检查已完成。使用nextCursor作为下一次cursor，在当前turn的下一模型步骤继续advance；模型步骤不是新turn，不要结束turn或输出进度报告。收到真实预算收尾后使用当前nextCursor调close。不要在run_code循环或重放旧cursor。" });
      replies.set(key, reply);
      log("wrapup_checkpoint", { ...snapshot(), step: s.step });
      return reply;
    },
  };
  return {
    tool, observe, receiptPath, snapshot,
    guard(exec) {
      const sid = exec.agent?.session?.id;
      if (!taskId || !(owns(sid) || ownsAcceptorSession(taskId, sid))) return;
      if (["pwsh", "powershell", "bash", "shell", "exec", "exec_command"].includes(exec.name)) {
        return "本轮收尾测试禁止终端命令；DSH的Windows命令沙箱ACL初始化失败。执行者使用write后直接提交报告，验收者使用read核对已登记文件。不要换终端或重复尝试。";
      }
    },
    bind(id) { if (taskId) throw Error("检查点不可跨任务复用"); taskId = id; },
    install(ctx) {
      const tools = ctx.get("tools");
      if (typeof tools?.register !== "function" || typeof tools?.guard !== "function" || typeof ctx.on !== "function") throw Error("wrapup 需要宿主 tools.register、tools.guard 与 session/event，缺失时不创建付费任务");
      const disposeTool = tools.register(tool);
      let disposeGuard;
      let disposeEvents;
      try {
        disposeGuard = tools.guard(this.guard);
        disposeEvents = ctx.on("session/event", observe);
        if (typeof disposeEvents !== "function") throw Error("session/event 未返回清理函数，拒绝安装检查点");
      }
      catch (error) { disposeGuard?.(); disposeTool(); throw error; }
      stop = () => { disposeEvents?.(); disposeGuard?.(); disposeTool(); };
    },
    dispose() { if (closed) return; closed = true; stop?.(); },
  };
}
