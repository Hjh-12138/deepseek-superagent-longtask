/**
 * 阶段 1 联调驱动（真实 DSH 宿主内运行）。
 *
 * 定位：本插件是 longtask 的**第三方消费者**，只通过 ctx.get("longtask")
 * 使用公开 API，不触碰内部实现——这样联调验证的就是真实插件入口，
 * 而不是把测试代码塞进产品代码里。
 *
 * 行为：
 * 1. 轮询等待 longtask 服务可用（服务由 longtask 插件 ctx.provide 暴露）；
 * 2. 从真实专家目录选两位不同专家（执行 + 独立验收）；
 * 3. propose → confirm → 观察状态机直到 delivered / paused / cancelled / 超时；
 * 4. 全过程写入 transcriptDir/run-<时间戳>.jsonl，并生成一份 summary.md。
 *
 * 安全：任何异常都被捕获并记入记录，不向宿主抛错，不影响 DSH 正常使用。
 */

import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { join, resolve } from "node:path";
// 宿主热重载入口时，Node仍可能缓存其子模块；协议升级须同步更换子模块缓存键。
import { assessScenario, parseRunRequest, extractPairEvidence } from "./evidence.mjs?protocol=8";
import { createPairFixture, PAIR_TOOL } from "./pair-fixture.mjs?protocol=3";
import { createWrapupFixture, WRAPUP_TOOL, WRAPUP_FIXTURE_VERSION, MAX_CHECKS } from "./wrapup-fixture.mjs?protocol=2";
import { createGapFixture, GAP_FIXTURE_VERSION, GAP_BODY } from "./gap-fixture.mjs?protocol=2";
import { createStage3Driver } from "./stage3-driver.mjs?protocol=4";
import {createStage4Driver} from './stage4-driver.mjs?protocol=2';

export const name = "dsh-longtask-e2e";
export const inject = ["tools"];

const TERMINAL = new Set(["delivered", "cancelled"]);

function nowIso() {
  return new Date().toISOString();
}

export function apply(ctx, config = {}) {
  const cfg = {
    transcriptDir: resolve(import.meta.dirname, "../dsh-longtask/.longtask/e2e"),
    artifactDir: resolve(import.meta.dirname, "../dsh-longtask/.longtask/e2e/artifacts"),
    taskBudgetTokens: 600000,
    timeoutMs: 1_500_000,
    pollIntervalMs: 3000,
    autoConfirm: true,
    force: false,
    autoRun: false,
    /** 场景：basic=常规短文；pressure=压力触发（读预算/收尾）；gap=故意缺项（退回再验收） */
    scenario: "basic",
    /** 预算严格模式：真机验证"无法取得可信上界就拒绝"必须走 strict:true */
    strict: false,
    ...config,
  };
  const defaults = { ...cfg };

  try {
    mkdirSync(cfg.artifactDir, { recursive: true });
  } catch {
    /* 记录目录不可用时仍然继续，记录会落到下方的 fallback */
  }

  let stamp = nowIso().replace(/[:.]/g, "-");
  let transcriptPath = join(cfg.transcriptDir, `run-${stamp}.jsonl`);
  let summaryPath = join(cfg.transcriptDir, `summary-${stamp}.md`);
  const events = [];

  const log = (kind, data = {}) => {
    const entry = { ts: nowIso(), kind, ...data };
    events.push(entry);
    try {
      appendFileSync(transcriptPath, `${JSON.stringify(entry)}\n`, "utf8");
    } catch {
      /* 写盘失败不阻塞场景 */
    }
    try {
      ctx.logger?.info?.(`longtask-e2e: ${kind} ${JSON.stringify(data).slice(0, 200)}`);
    } catch {
      /* logger 不可用忽略 */
    }
  };

  let started = false;
  let hasRun = false;
  let disposed = false;
  let activeFixture;
  let poller;
  const stage3 = createStage3Driver(ctx, { directory: join(cfg.transcriptDir, "stage3"), artifactDir: cfg.artifactDir, timeoutMs: Math.max(cfg.timeoutMs, 1_800_000) });
  const stage4=createStage4Driver(ctx,{directory:join(cfg.transcriptDir,'stage4'),artifactDir:cfg.artifactDir,timeoutMs:cfg.timeoutMs});
  const beginRun = (runStamp = nowIso().replace(/[:.]/g, "-")) => {
    stamp = runStamp;
    transcriptPath = join(cfg.transcriptDir, `run-${stamp}.jsonl`);
    summaryPath = join(cfg.transcriptDir, `summary-${stamp}.md`);
    events.length = 0;
    started = true;
    hasRun = true;
  };

  /**
   * 控制证据：直接从插件日志里数本任务的控制指令。
   * 只靠最终状态无法区分"机制没触发"和"触发但没生效"，这一项必须自报。
   */
  const controlEvidence = (taskId) => {
    try {
      const text = readFileSync(join(cfg.transcriptDir, "..", "plugin.log"), "utf8");
      const lines = text.split("\n").filter((l) => l.includes(taskId));
      return {
        admitted: lines.filter((l) => l.includes("budget:admitted")).length,
        rejects: lines.filter((l) => l.includes("budget:reject")).length,
        wrapUp: lines.filter((l) => l.includes("budget:wrap-up")).length,
        readStop: lines.filter((l) => l.includes("budget:read-stop")).length,
        wrapUpConsumed: lines.filter((l) => l.includes("control:consumed") && l.includes("kind=wrap-up")).length,
        readStopConsumed: lines.filter((l) => l.includes("control:consumed") && l.includes("kind=read-stop")).length,
        boundExceeded: lines.filter((l) => l.includes("budget:bound-exceeded")).length,
      };
    } catch {
      return null;
    }
  };

  /** 验收证据：直接读状态文件里的事件，回报每一轮的结论（gap/rework 必须可见） */
  const roundEvidence = (taskId) => {
    try {
      const state = JSON.parse(readFileSync(join(cfg.transcriptDir, "..", "state.json"), "utf8"));
      const events = state.eventOrder
        .map((id) => (state.events ?? {})[id])
        .filter((e) => e && e.taskId === taskId);
      return events
        .filter((e) => e.type === "task.delivered" || e.type === "task.verdict")
        .map((e) => ({
          roundId: e.payload?.roundId ?? null,
          outcome: e.type === "task.delivered" ? "delivered" : "rework",
          verdicts: (e.payload?.verdicts ?? []).map((v) => `${v.criterionId}:${v.outcome}`),
        }));
    } catch {
      return null;
    }
  };

  /** 只读宿主当前投影与后代目录，不创建/恢复会话。缺少能力就不能声称整棵树已对账。 */
  const accountingEvidence = async (taskId) => {
    try {
      const state = JSON.parse(readFileSync(join(cfg.transcriptDir, "..", "state.json"), "utf8"));
      const leases = Object.values(state.collections.taskSessions?.[taskId] ?? {});
      const subagents = ctx.get("subagents");
      const projections = ctx.get("sessionProjections");
      const agents = ctx.get("agents");
      const sessionService = ctx.get("sessions");
      if (!leases.length || typeof subagents?.listDescendants !== "function" || typeof projections?.stateOf !== "function") {
        throw new Error("缺少任务会话、后代目录或用量投影接口");
      }
      const descendants = [];
      let actualTokens = 0;
      for (const lease of leases) {
        const children = await subagents.listDescendants(lease.sessionId, AbortSignal.timeout(3000));
        descendants.push(...children);
        const session = agents?.get?.(lease.sessionId)?.session;
        if (!session) throw new Error(`会话已不可读：${lease.sessionId}`);
        if (typeof sessionService?.flush !== "function" || await sessionService.flush(session) !== true) {
          throw new Error(`会话未通过持久化屏障：${lease.sessionId}`);
        }
        const usage = projections.stateOf(session, "tokenUsage")?.totals;
        const buckets = usage && [usage.uncachedInputTokens, usage.cacheReadTokens, usage.cacheWriteTokens, usage.outputTokens];
        if (!buckets || !buckets.every((n) => Number.isSafeInteger(n) && n >= 0)) throw new Error(`用量不可读：${lease.sessionId}`);
        actualTokens += buckets.reduce((a, b) => a + b, 0);
      }
      return { verified: descendants.length === 0, actualTokens, descendants };
    } catch (error) {
      return { verified: false, actualTokens: null, descendants: [], reason: String(error?.message ?? error) };
    }
  };

  const scenario = async (api) => {
    if(['doc','dev'].includes(cfg.scenario)||cfg.crash)throw Error('阶段四只接受显式版本化运行请求，不使用旧场景/启动配置');
    const t0 = Date.now();
    log("scenario_start", { transcriptPath, budgetTokens: cfg.taskBudgetTokens });

    const executors = await api.searchExperts("engineering");
    const acceptors = await api.searchExperts("research");
    if (disposed) return;
    log("experts_scanned", {
      executors: executors.slice(0, 3).map((e) => e.relPath),
      acceptors: acceptors.slice(0, 3).map((e) => e.relPath),
    });

    const executor = executors[0];
    // pair 场景需要两位不同的执行专家（第二位由简报按相对路径识别）
    const peer = cfg.scenario === "pair" ? executors.find((e) => e.relPath !== executor?.relPath) : undefined;
    if (cfg.scenario === "pair" && !peer) throw new Error("pair 需要两位不同的执行专家，当前目录不足");
    // 检索按简介文本匹配，结果可能跨部门；优先取 division 精确命中的独立验收人
    const acceptor =
      acceptors.find((e) => e.division === "research" && e.relPath !== executor?.relPath) ??
      acceptors.find((e) => e.relPath !== executor?.relPath) ??
      acceptors[0];
    if (!executor || !acceptor) throw new Error("专家目录为空，无法组成执行/验收对");

    // 本轮产物永不复用旧路径；历史验收冻结的文件保持原样。
    const runId = randomUUID();
    const runArtifactDir = join(cfg.artifactDir, `${stamp}-${runId}`);
    mkdirSync(runArtifactDir, { recursive: true });
    if (cfg.scenario === "wrapup") {
      if (typeof api.registerArtifact !== "function" || typeof api.pause !== "function") throw Error("wrapup 缺少产物登记或暂停入口，不创建模型任务");
      activeFixture = createWrapupFixture({
        runId, directory: runArtifactDir, log,
        ownsSession: (taskId, sid) => {
          try {
            const state = JSON.parse(readFileSync(join(cfg.transcriptDir, "..", "state.json"), "utf8"));
            return Object.values(state.collections.taskSessions?.[taskId] ?? {}).some((s) => s.sessionId === sid && s.role === "executor");
          } catch { return false; }
        },
        ownsAcceptorSession: (taskId, sid) => {
          try {
            const state = JSON.parse(readFileSync(join(cfg.transcriptDir, "..", "state.json"), "utf8"));
            return Object.values(state.collections.taskSessions?.[taskId] ?? {}).some((s) => s.sessionId === sid && s.role === "acceptor");
          } catch { return false; }
        },
        onExhausted: async (id) => { await api.pause(id, "E2E检查点用尽但未消费收尾；本轮停止，不追加预算"); },
        onReceipt: async (id, path) => {
          await api.registerArtifact(id, path, createHash("sha256").update(readFileSync(path)).digest("hex"), "harness");
        },
      });
      activeFixture.install(ctx); // 能力不足先失败，不创建模型任务。
    }

    // 场景规格：同一套驱动覆盖"常规 / 压力 / 缺项"三类用例，预算与 strict 由 run-request 决定。
    // 压力场景必须能必然触发读预算：列 8 个材料文件（读预算默认 6 次）。
    const spec = (() => {
      if (cfg.scenario === "pair") {
        // 阶段2 出口：两位专家直接交接产物，Router 只看摘要。
        // 第一位写表并用协议块交接；第二位先结束一轮等待，收到交接后再写推荐。
        const tablePath = join(runArtifactDir, "pair-table.md");
        const recPath = join(runArtifactDir, "pair-recommendation.md");
        const marker = `TBL-${runId.slice(0, 8).toUpperCase()}`;
        activeFixture = createPairFixture({ runId, sender: executor.relPath, recipient: peer.relPath, artifactPath: tablePath, log,
          getDependency: typeof api.getExecutorDependency === "function" ? (...args) => api.getExecutorDependency(...args) : undefined,
          replayTurnEnd: typeof api.replayExecutorTurnEnd === "function" ? (...args) => api.replayExecutorTurnEnd(...args) : undefined,
          readSessions: (id) => Object.values(JSON.parse(readFileSync(join(cfg.transcriptDir, "..", "state.json"), "utf8")).collections.taskSessions?.[id] ?? {}) });
        activeFixture.install(ctx);
        return {
          artifactPath: tablePath,
          deliverables: [tablePath, recPath],
          assignments: [
            { relPath: executor.relPath, deliverables: [tablePath] },
            { relPath: peer.relPath, deliverables: [recPath], waitFor: { fromRelPath: executor.relPath, artifactRefs: [tablePath] } },
          ],
          goal: [
            `[E2E-PAIR][${marker}] 两位执行专家协作完成交付，本轮 runId=${runId}。`,
            `第一位执行专家（${executor?.relPath}）：先写比较表 ${tablePath}，首行必须是「# 比较表 ${marker}」，至少 4 行方案对比；写完后用协议块交接：`,
            `写完表后必须先调用 ${PAIR_TOOL}，参数 runId="${runId}"；它核对调度器已将 B 置为等待。返回 ready=true 后才发送下面唯一一条协议块，不要再发其他交接块。`,
            `<longtask_message to="${peer?.relPath}" summary="比较表已完成，见表" artifacts="${tablePath}">比较表已落盘，请据此写推荐。</longtask_message>`,
            `本轮报告 done=false、next=[]、waiting="等待 B 完成推荐"（必须进入等待，不要要求插件立刻续做）。`,
            `第二位执行专家（${peer?.relPath}）：前置消息送达前由插件调度等待，不启动你的模型。收到交接才做推荐，不把文件存在当作消息到达，不自己制作比较表；`,
            `收到交接后读 ${tablePath}，写推荐 ${recPath}（至少 2 条理由，且必须引用表中的 ${marker} 与至少一个方案名），两份文件都落盘后报告 done=true。`,
          ].join("\n"),
          criteria: [
            { id: "c1", text: `比较表 ${tablePath} 存在，首行含 ${marker}，至少 4 行方案对比` },
            { id: "c2", text: `推荐 ${recPath} 存在，至少 2 条理由，且引用 ${marker} 与比较表中的一个方案名` },
          ],
        };
      }
      if (cfg.scenario === "pressure") {
        const dir = join(runArtifactDir, "materials");
        mkdirSync(dir, { recursive: true });
        const files = [];
        for (let i = 1; i <= 8; i++) {
          const file = join(dir, `f${i}.txt`);
          writeFileSync(file, `第 ${i} 份材料：仅用于压力场景，关键词 pressure-${i}。\n`, "utf8");
          files.push(file);
        }
        const artifactPath = join(runArtifactDir, "pressure.md");
        return {
          artifactPath,
          goal: `[E2E-PRESSURE] 读取下列 8 个材料文件，各用一句话概括，写入 ${artifactPath}：\n${files.map((f) => `- ${f}`).join("\n")}`,
          criteria: [
            { id: "c1", text: `产物 ${artifactPath} 存在且为中文 Markdown，恰好 8 行（一行一个材料）` },
            { id: "c2", text: "每行格式为 `fN.txt — 一句话摘要`，N 覆盖 1..8 且不重复（依据产物自身即可判定）" },
          ],
        };
      }
      if (cfg.scenario === "wrapup") {
        // 有未完成检查的握手足以覆盖真实消费，无需重复背景垫高每一步和验收输入。
        const artifactPath = join(runArtifactDir, "wrapup.md");
        const facts = [
          "提案阶段记录任务目标、完成标准和需要交付的产物。",
          "确认阶段固定任务范围与预算，再将执行工作交给专家。",
          "执行专家按简报完成工作，通过结构化报告说明当前进度。",
          "预算控制逐步预留与结算用量，余额不足时停止继续探索。",
          "收尾指令要求保存当前成果，并逐条核对任务完成标准。",
          "独立验收专家依据冻结产物和完成标准给出逐项结论。",
          "验收发现缺项后把任务退回执行专家，修复后重新验收。",
          "全部完成标准通过后交付成果，并保留报告与用量记录。",
        ];
        return {
          artifactPath,
          goal: `[E2E-WRAPUP][${WRAPUP_FIXTURE_VERSION}] 这是明确标注的收尾机制测试，不是自然完成速度测试。本轮 runId=${runId}。\n` +
            `使用 ${WRAPUP_TOOL} 工具，runId固定为上述值，首次cursor=0。每次action=advance完成一项模拟检查，下一次cursor必须用上次返回的nextCursor。每个模型步骤调用一次，模型步骤不是turn：在同一turn连续推进，不要提前结束turn或输出done=false进度报告；不能在run_code中循环、并行调用或重放旧cursor。\n` +
            `检查尚未结束时，插件应自然发出【预算收尾】。收到这条真实指令后立即停止advance，使用最新cursor调用action=close领取结束回执。夹具将自行写入并登记回执记录；无需再读取或确认它。未收到指令时应继续检查，不等待用户。最多${MAX_CHECKS}次检查；未触发时停止，不追加额度。\n` +
            `最终文件 ${artifactPath} 必须恰好九行：前八行以「- 」开头、每条至少十五字且互不重复，最后一行是close返回的WRAPUP-回执。直接使用write工具写文件；write成功后立即输出done=true的longtask_report，不再调用pwsh、终端、glob或read核对，核对交给独立验收。验收者使用read读取已登记文件，不执行终端命令。\n` +
            facts.map((f) => `- ${f}`).join("\n"),
          criteria: [
            { id: "c1", text: `产物 ${artifactPath} 存在且恰好九行；前八行是互不重复的中文要点，每行以「- 」开头且不少于十五字` },
            { id: "c2", text: `末行回执必须与本轮只写一次的测试记录 ${activeFixture.receiptPath} 中 token 完全一致，且记录 runId=${runId}；文件不存在、回执缺失或使用旧回执均不满足` },
          ],
        };
      }
      if (cfg.scenario === "gap-inject") {
        const artifactPath = join(runArtifactDir, "gap-inject.md");
        const marker = `SYNC-${runId.toUpperCase()}`;
        if (typeof api.pause !== "function") throw Error("gap-inject缺少暂停入口，不创建模型任务");
        activeFixture = createGapFixture({
          runId, artifactPath, marker, log,
          readView: (id) => {
            const state = JSON.parse(readFileSync(join(cfg.transcriptDir, "..", "state.json"), "utf8"));
            const c = state.collections;
            const task = c.tasks?.[id];
            return { task, version: task?.versions[task.currentVersion], sessions: Object.values(c.taskSessions?.[id] ?? {}),
              artifacts: Object.values(c.artifacts ?? {}).filter((a) => a.taskId === id),
              rounds: Object.values(c.acceptanceRounds ?? {}).filter((r) => r.taskId === id),
              events: (state.eventOrder ?? []).map((key) => state.events[key]).filter((e) => e?.taskId === id) };
          },
          onFailure: (id, reason) => api.pause(id, `E2E故障注入证据失效，本轮暂停：${reason}`),
        });
        activeFixture.install(ctx);
        return {
          artifactPath,
          goal: `[E2E-GAP-INJECT][${GAP_FIXTURE_VERSION}] 本轮runId=${runId}。目标产物 ${artifactPath}：前四行必须原样为以下正文，第五行必须恰好是 ${marker}。文件不包含代码围栏，不增加标题、空行、技术细节或其他内容。\n\`\`\`text\n${GAP_BODY}\n\`\`\`\n` +
            "测试说明：全部输入已给齐；驱动仅在首次完成报告后、验收冻结前移除校验末行，用于验证真实退回闭环，不修改标准、不伪造结论、不替执行者修复。\n" +
            "角色约束：执行者负责首次write写齐并提交longtask_report；收到真实gap后，必须先read当前文件（注入属于外部修改，旧缓存已失效），再edit补齐末行，编辑成功后直接提交完成报告，不再额外read复核。独立验收者只read已登记文件并输出longtask_verdicts，不能执行上述写入或longtask_report要求。两种角色均不运行终端或读取仓库资料。",
          criteria: [
            { id: "c1", text: `产物 ${artifactPath} 存在，按去掉文件末尾空白后计4或5行，前四行必须逐行原样匹配下列代码块；校验末行单独按c2判定：\n\`\`\`text\n${GAP_BODY}\n\`\`\`` },
            {
              id: "c2",
              text: `文档最后一行必须恰好是 ${marker}；缺失、被改写或使用其他轮次标记均不满足。依据实际读取的文件核对，不接受只引用执行者口头报告。`,
            },
          ],
        };
      }
      if (cfg.scenario === "gap") {
        const artifactPath = join(runArtifactDir, "gap-sample.md");
        return {
          artifactPath,
          // 目标里只说"写两节"，表格要求只出现在标准里：首轮最常见的形态是列表，从而产生真实 gap
          goal: `[E2E-GAP] 写一份简短的变更说明 ${artifactPath}，包含「变更内容」与「回滚与降级」两节，全文不超过 60 行。`,
          criteria: [
            { id: "c1", text: `产物 ${artifactPath} 存在且为中文 Markdown，正文不超过 60 行，含「变更内容」与「回滚与降级」两节` },
            {
              id: "c2",
              text: "「回滚与降级」一节必须是一张 Markdown 表格：表头恰为 `| 步骤 | 触发条件 | 执行人 | 验证方式 |`，至少 3 行数据行；且『验证方式』列每一行都必须是可直接执行的命令或可判定的断言（例如 `curl -sf http://host/health`、`SELECT count(*) FROM t WHERE ...`）。仅写「确认…」「检查…」这类描述性动作、或缺少任一列，均不满足",
            },
          ],
        };
      }
      const artifactPath = join(runArtifactDir, "e2e-sample.md");
      return {
        artifactPath,
        goal: "[E2E] 写一份简短的中文 Markdown 说明文档，讲清 DSH 长任务插件的处理链路：提案→确认→派工→验收→交付。只写五节、每节 2-4 句，不需要核对源码或外部资料，不要读仓库文件。",
        criteria: [
          { id: "c1", text: `产物文件存在且为中文 Markdown，路径 ${artifactPath}，正文不超过 80 行` },
          { id: "c2", text: "正文按 提案/确认/派工/验收/交付 分成五节，每节说明该环节的触发条件与产出（依据文档自身即可判定）" },
        ],
      };
    })();
    const { artifactPath, goal, criteria } = spec;
    log("scenario_spec", { scenario: cfg.scenario, runId, artifactPath, budgetTokens: cfg.taskBudgetTokens, strict: cfg.strict,
      fixtureVersion: activeFixture?.version ?? (activeFixture ? WRAPUP_FIXTURE_VERSION : null) });

    // 上一进程遗留的任务：
    // - paused 的任务是用户明确要求"保持暂停、不追加额度"的，既不取消也不恢复，原样留着；
    // - 其余未终结且会话随进程退出的，取消掉，避免"看着在跑实际不动"。
    const stale = (api.listTasks() ?? []).filter((t) => {
      const goalText = t.versions[t.currentVersion]?.goal ?? "";
      // 三类场景的目标都以 [E2E 开头（[E2E] / [E2E-PRESSURE] / [E2E-GAP]）
      return goalText.startsWith("[E2E") && !TERMINAL.has(t.status);
    });
    for (const t of stale) {
      if (t.status === "paused") {
        log("stale_kept_paused", { taskId: t.id, pauseReason: t.pauseReason ?? null, note: "按用户要求保持暂停，不追加额度" });
        continue;
      }
      try {
        await api.cancel(t.id);
        log("stale_cancelled", { taskId: t.id, prevStatus: t.status });
      } catch (error) {
        log("stale_cancel_failed", { taskId: t.id, message: String(error?.message ?? error) });
      }
    }

    let task;
    {
      if (disposed) return;
      task = await api.propose({
        goal,
        criteria,
        deliverables: spec.deliverables ?? [artifactPath],
        executors: peer ? [executor, peer] : [executor],
        ...(spec.assignments ? { assignments: spec.assignments } : {}),
        acceptor,
        budget: { limitTokens: cfg.taskBudgetTokens, strict: cfg.strict },
      });
      log("proposed", { taskId: task.id, executor: executor.relPath, peer: peer?.relPath ?? null, acceptor: acceptor.relPath });
      activeFixture?.bind(task.id);
    }

    if (task.status === "proposed") {
      if (disposed) return;
      if (!cfg.autoConfirm) {
        log("await_user_confirm", { taskId: task.id });
        return;
      }
      await api.confirm(task.id);
      log("confirmed", { taskId: task.id });
    }

    // 观察状态机
    const deadline = Date.now() + cfg.timeoutMs;
    let lastStatus = "";
    let lastWaiting = "";
    let finalTask = task;
    while (!disposed && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, cfg.pollIntervalMs));
      if (disposed) return;
      const t = api.getTask(task.id);
      finalTask = t;
      const waiting = String(t.waitingOn ?? "");
      if (t.status !== lastStatus || waiting !== lastWaiting) {
        log("status", {
          taskId: t.id,
          status: t.status,
          waitingOn: t.waitingOn ?? null,
          pauseReason: t.pauseReason ?? null,
          budget: typeof api.getBudget === "function" ? api.getBudget(t.id) ?? null : null,
          acceptanceProgress: typeof api.getAcceptanceProgress === "function" ? api.getAcceptanceProgress(t.id) ?? null : null,
          elapsedMs: Date.now() - t0,
        });
        lastStatus = t.status;
        lastWaiting = waiting;
      }
      if (TERMINAL.has(t.status) || t.status === "paused") break;
    }

    if (!TERMINAL.has(finalTask?.status) && finalTask?.status !== "paused") {
      await api.pause(task.id, "E2E测试超时，本轮停止，不重试或追加预算");
      finalTask = api.getTask(task.id);
      log("fixture_timeout", { taskId: task.id, scenario: cfg.scenario });
    }
    const elapsedMs = Date.now() - t0;
    const budget = typeof api.getBudget === "function" ? api.getBudget(task.id) ?? null : null;
    const controls = controlEvidence(task.id);
    const rounds = roundEvidence(task.id);
    const stage2 = (() => {
      if (cfg.scenario !== "pair") return undefined;
      try {
        const state = JSON.parse(readFileSync(join(cfg.transcriptDir, "..", "state.json"), "utf8"));
        const digest = typeof api.getRouterDigest === "function" ? api.getRouterDigest(task.id) : null;
        const observations = readFileSync(join(cfg.transcriptDir, "..", "plugin.log"), "utf8").split("\n").flatMap((line) => {
          const start = line.indexOf("collaboration:");
          if (start < 0) return [];
          try { return [JSON.parse(line.slice(start + "collaboration:".length))]; } catch { return []; }
        });
        return extractPairEvidence({ state, digest, taskId: task.id, observations, replay: activeFixture?.snapshot() });
      } catch {
        return undefined;
      }
    })();
    const accounting = await accountingEvidence(task.id);
    const fault = cfg.scenario === "gap-inject" ? activeFixture?.snapshot() : undefined;
    const fixture = cfg.scenario === "wrapup" ? activeFixture?.snapshot(artifactPath) : undefined;
    const coverage = assessScenario({ taskId: task.id, runId, scenario: cfg.scenario, status: finalTask?.status, budget, accounting, controls, rounds, fault, fixture, stage2 });
    log("scenario_end", {
      taskId: task.id,
      scenario: cfg.scenario,
      status: finalTask?.status ?? "unknown",
      elapsedMs,
      budget,
      controls,
      rounds,
      accounting,
      coverage,
      fixture,
      fault,
      stage2,
    });
    // 用例自检：目标机制没被触发就不计入证据，避免"跑成功"被当成"机制已验证"
    log(coverage.passed ? "scenario_verified" : "scenario_not_covered", coverage);

    // 汇总
    const lines = [
      `# ${cfg.scenario === "pair" ? "阶段 2 专家协作" : "阶段 1"}联调记录 · ${stamp}`,
      "",
      `- 任务：${task.id}`,
      `- 最终状态：**${finalTask?.status ?? "unknown"}**（耗时 ${Math.round(elapsedMs / 1000)}s）`,
      `- 暂停原因：${finalTask?.pauseReason ?? "—"}`,
      `- 执行专家：${executor.relPath}`,
      `- 验收专家：${acceptor.relPath}`,
      `- 交付物：${artifactPath}`,
      `- 预算：限额 ${budget?.limitTokens ?? "?"} / 已结算 ${budget?.settledTokens ?? "?"} / 在途 ${budget?.reservedTokens ?? "?"}（strict=${cfg.strict}）`,
      `- 场景：${cfg.scenario}`,
      ...(fixture ? [`- 测试夹具：${fixture.version}；收尾时有待办 ${fixture.pendingAtControl}；回执与产物匹配 ${fixture.artifactMatches}`] : []),
      ...(stage2 ? [`- 阶段2协作：直连 ${stage2.directDelivered}/${stage2.directTotal} 已投递；摘要 ${stage2.summaries} 条（带产物引用 ${stage2.summaryRefs}）；登记产物 ${stage2.digestArtifacts}；正文已持久化 ${stage2.bodyPersisted}；Router 含正文 ${stage2.digestContainsBody}`] : []),
      ...(fault ? [`- 故障夹具：${fault.version}；注入 ${fault.injected}；Worker修复 ${fault.repaired}；异常 ${fault.failure ?? "无"}`] : []),
      `- 场景验证：${coverage.passed ? "通过" : `未通过：${coverage.reasons.join("；")}`}`,
      `- 宿主对账：${accounting.verified ? "已核对" : "未通过"}；主会话用量 ${accounting.actualTokens ?? "未知"}；后代目录项 ${accounting.descendants.length}`,
      `- 控制触发：收尾 ${controls?.wrapUp ?? "?"} 次 / 读预算 ${controls?.readStop ?? "?"} 次 / 准入拒绝 ${controls?.rejects ?? "?"} 次 / 上界击穿 ${controls?.boundExceeded ?? "?"} 次`,
      `- 验收轮次：${rounds && rounds.length > 0 ? rounds.map((r) => `${r.outcome}[${r.verdicts.join(",")}]`).join(" → ") : "无"}`,
      `- 事件数：${events.length}；逐条记录：${transcriptPath}`,
      "",
      "## 状态变化",
      "",
      ...events.filter((e) => e.kind === "status").map((e) => {
        const b = e.budget ? `（已结算 ${e.budget.settledTokens}）` : "";
        return `- ${e.ts} → ${e.status}${b}（${e.elapsedMs}ms）`;
      }),
    ];
    try {
      writeFileSync(summaryPath, lines.join("\n"), "utf8");
    } catch {
      /* 忽略 */
    }
    log("summary_written", { summaryPath });
    // finished 只表示一次运行已经结束；done 只代表目标场景及用量核对均通过。
    writeFileSync(join(cfg.transcriptDir, `finished-${stamp}.json`), JSON.stringify({ taskId: task.id, at: nowIso(), elapsedMs, coverage }, null, 2), "utf8");
    if (coverage.passed) {
      try {
        writeFileSync(join(cfg.transcriptDir, `done-${stamp}.json`), JSON.stringify({ taskId: task.id, at: nowIso(), elapsedMs, scenario: cfg.scenario, coverage }, null, 2), "utf8");
      } catch {
        /* 忽略 */
      }
    }
  };

  const scenarioStartedAt = Date.now();
  let warnedInvisible = false;
  let skipLogged = false;
  /** 一次性重跑触发文件：存在即强制开一轮新场景（消费后改名归档），与 resume-request.json 同模式 */
  const runRequestPath = join(cfg.transcriptDir, "run-request.json");

  const alreadyDone = () => {
    if (hasRun) return true;
    if (cfg.force) return false;
    try {
      return readdirSync(cfg.transcriptDir).some((f) => f.startsWith("done-") || f.startsWith("finished-"));
    } catch {
      return false;
    }
  };

  const start = () => {
    if (started || disposed) return;
    const api = ctx.get("longtask");
    if (!api) {
      if (!warnedInvisible && Date.now() - scenarioStartedAt > 60_000) {
        warnedInvisible = true;
        log("service_not_visible", { type: typeof api, note: "60s 内 ctx.get('longtask') 仍不可见：服务暴露或注入方式需要排查" });
      }
      return;
    }
    if(stage4.claimed()){
      if(!api.getRuntimeInfo?.().ready)return;
      void stage4.tick().catch(error=>log('stage4_controller_error',{message:String(error.message)}));return;
    }
    if (stage3.claimed()) {
      if (!api.getRuntimeInfo?.().ready) return;
      void stage3.tick().catch(error => log("stage3_controller_error", { message: String(error.message) }));
      return;
    }
    // 显式续验控制文件：只给既有任务追加已批准额度，不创建/取消任务。
    const resumePath = join(cfg.transcriptDir, "resume-request.json");
    if (existsSync(resumePath)) {
      if (typeof api.grantBudget !== "function") return; // 等待新版服务就绪
      beginRun();
      void (async () => {
        const request = JSON.parse(readFileSync(resumePath, "utf8"));
        if (typeof request.taskId !== "string" || typeof request.grantId !== "string" ||
            !Number.isSafeInteger(request.tokens) || request.tokens <= 0) throw new Error("无效的续验授权记录");
        const before = api.getTask(request.taskId);
        if (!before) throw new Error("待续验任务不存在");
        log("resume_existing", { taskId: request.taskId, status: before.status, grantId: request.grantId });
        await api.grantBudget(request.taskId, request.tokens, request.note ?? "用户批准追加", request.grantId);
        log("budget_granted", { taskId: request.taskId, tokens: request.tokens, grantId: request.grantId });
        await api.resume(request.taskId);
        renameSync(resumePath, join(cfg.transcriptDir, `resume-request-applied-${stamp}.json`));
        const deadline = Date.now() + cfg.timeoutMs;
        let lastStatus;
        let task;
        do {
          task = api.getTask(request.taskId);
          if (task.status !== lastStatus) {
            log("status", { taskId: task.id, status: task.status, pauseReason: task.pauseReason });
            lastStatus = task.status;
          }
          if (TERMINAL.has(task.status) || task.status === "paused") break;
          await new Promise((r) => setTimeout(r, cfg.pollIntervalMs));
        } while (!disposed && Date.now() < deadline);
        if (disposed) return;
        log("scenario_end", { taskId: request.taskId, status: task.status, resumed: true });
        writeFileSync(summaryPath, `# 原任务续验\n\n- 任务：${request.taskId}\n- 最终状态：${task.status}\n- 用户批准追加：${request.tokens} Token\n- 授权幂等键：${request.grantId}\n- 记录：${transcriptPath}\n`, "utf8");
        writeFileSync(join(cfg.transcriptDir, `finished-${stamp}.json`), JSON.stringify({ taskId: task.id, at: nowIso(), resumed: true, status: task.status, verified: false }), "utf8");
      })().catch((error) => log("scenario_error", { stack: String(error?.stack ?? error) })).finally(() => { started = false; });
      return;
    }
    if ((alreadyDone() || (!cfg.autoRun && !cfg.force)) && !existsSync(runRequestPath)) {
      // 已交付过：不再永久退出，保持轮询等 run-request.json 出现——
      // 否则每验证一轮都要改 profile 配置或手动挪 done 标记。
      if (!skipLogged) {
        skipLogged = true;
        log("skip_already_done", { note: `已有运行结束记录；等待显式请求 ${runRequestPath}，仅重载不会重新消耗预算。可用 request-run.mjs 提交一轮。` });
      }
      return;
    }
    Object.assign(cfg, defaults);
    // 消费一次性触发文件，避免下一轮又跑；文件里可指定场景/预算/严格模式
    if (existsSync(runRequestPath)) {
      try {
        const request = parseRunRequest(JSON.parse(readFileSync(runRequestPath, "utf8")));
        stamp = nowIso().replace(/[:.]/g, "-");
        renameSync(runRequestPath, join(cfg.transcriptDir, `run-request-applied-${stamp}.json`));
        if(['doc','dev'].includes(request.scenario)){
          stage4.enqueue(request);log('stage4_requested',{scenario:request.scenario,requestId:request.requestId,crash:request.crash});return;
        }
        cfg.scenario = request.scenario;
        cfg.taskBudgetTokens = request.budgetTokens;
        cfg.strict = request.strict;
        beginRun(stamp);
        log("run_requested", {
          scenario: cfg.scenario,
          budgetTokens: cfg.taskBudgetTokens,
          strict: cfg.strict,
          note: "检测到 run-request.json，本轮强制启动新场景",
        });
      } catch (error) {
        log("run_request_invalid", { message: String(error?.message ?? error) });
        try { renameSync(runRequestPath, join(cfg.transcriptDir, `run-request-rejected-${Date.now()}.json`)); } catch { /* 无有效请求就不运行 */ }
        started = false;
        return;
      }
    } else beginRun();
    log("service_detected", {});
    void scenario(api).catch((error) => {
      log("scenario_error", { stack: String(error?.stack ?? error) });
    }).finally(() => { activeFixture?.dispose(); activeFixture = undefined; started = false; skipLogged = false; });
  };

  poller = setInterval(start, 1000);

  try {
    ctx.effect(function* () {
      yield async () => {
        disposed = true;
        if (poller) clearInterval(poller);
        await stage4.dispose();
        stage3.dispose();
        activeFixture?.dispose();
      };
    });
  } catch {
    /* 卸载钩子不可用不影响联调 */
  }
}
