/**
 * 领域类型 · 对应 01-设计文档 §2-§5 与 02-实施文档 §2-§4。
 *
 * 所有跨重启持久的数据结构集中在这里定义。
 * 宿主服务（agents/sessions）的结构类型见 host-types.ts。
 */

// ---------------------------------------------------------------------------
// 专家目录（设计文档 §4）
// ---------------------------------------------------------------------------

/** 一位专家的目录条目。relPath 是相对 agency-agents 根目录的路径，可区分同名专家。 */
export interface ExpertRef {
  /** 分类目录名，来自 divisions.json 的 divisions 键，例如 "research" */
  division: string;
  /** 相对专家库根目录的路径，例如 "research/research-synthesist.md" */
  relPath: string;
  /** front-matter 的 name */
  name: string;
  /** front-matter 的 description（简介，用于检索） */
  intro: string;
  /** 正文快照的内容哈希（sha256），用于记录选用版本，保证事后可还原 */
  contentHash: string;
  /** 快照采集时间（ISO 8601） */
  snapshottedAt: string;
}

// ---------------------------------------------------------------------------
// 任务与版本（设计文档 §2、实施文档 §2）
// ---------------------------------------------------------------------------

export type TaskStatus =
  | "proposed" // Router 已给方案，等待用户一次确认（确认前不消耗执行预算）
  | "active" // 已确认，可分派/续做
  | "awaiting_acceptance" // 执行者声明做完，等验收
  | "rework" // 验收退回，缺项交回负责人
  | "paused" // 用户暂停 / 预算不足 / 缺少决定
  | "delivered" // 独立验收全部通过
  | "cancelled"; // 显式取消，不得自启

export type PauseReason = "user" | "budget" | "missing_input" | "watchdog" | "host_restart" | "queued";

/** 一条可核对的完成标准 */
export interface Criterion {
  id: string;
  text: string;
  /** 与条文中的实测要求合并；不能用review覆盖明确的运行/浏览器要求。 */
  verification?: Array<"review" | "command" | "browser">;
}

export interface VerificationPlan {
  /** 已确认的项目目录；实测只在其临时副本内运行。 */
  workspace: string;
  checks: Array<{id: string; kind: "command" | "browser"; criterionIds: string[]; purpose: string;
    program: string; args: string[]; timeoutMs: number}>;
}

export type TaskWorkKind = "development" | "verification" | "document";
export interface VerificationScope { included: string[]; excluded: string[] }

export interface BudgetSpec {
  /** New formal tasks use CNY; old token tasks are kept without conversion. */
  money?: {currency: "CNY"; limitAmount: string | null; suggestedAmount?: string};
  /** 任务 Token 上限（输入+输出合计，缓存分项归并后计入输入） */
  limitTokens: number;
  /** 严格模式：无法证明额度充足时在调用前暂停，而不是事后超支 */
  strict: boolean;
}

/** 已确认的分工与单一前置交接；等待期间不启动接手者的模型。 */
export interface ExecutorAssignment {
  relPath: string;
  deliverables: string[];
  module?: string;
  responsibilities?: string[];
  criterionIds?: string[];
  coverageRationale?: string;
  /** 顺序执行；每批最多四个确切文件，不把整个系统塞进一次输出。 */
  batches?: Array<{id: string; title: string; deliverables: string[]}>;
  waitFor?: { fromRelPath: string; artifactRefs: string[] };
}

/** 任务的某一版目标/标准/预算。用户确认后生效；变更需确认新版本。 */
export interface WorkModule {
  id: string;
  title: string;
  scope: string;
  leadRelPath: string;
  contributors?: string[];
  criterionIds: string[];
  deliverables: string[];
  dependsOn?: string[];
}

export interface TaskVersion {
  workKind?: TaskWorkKind;
  verificationScope?: VerificationScope;
  verificationPolicyVersion?: 1;
  verificationPlan?: VerificationPlan;
  executionPlanVersion?: 1 | 2;
  workModules?: WorkModule[];
  staffingRationale?: string;
  version: number;
  goal: string;
  criteria: Criterion[];
  /** 交付物清单（描述性路径或说明） */
  deliverables: string[];
  /** 执行专家（relPath 列表），Router 可在已批准范围内增补替换并留痕 */
  executors: ExpertRef[];
  assignments?: ExecutorAssignment[];
  /** 执行者开步前必须已经装载的工具；不改变验收者能力集。 */
  requiredTools?: string[];
  acceptorRequiredTools?: string[];
  /** 独立验收专家，变更需用户单独确认 */
  acceptor: ExpertRef;
  budget: BudgetSpec;
  /** 用户确认记录；null 表示未确认（proposed） */
  confirmedAt: string | null;
}

export interface Task {
  /** Exact user session owning status delivery; never inferred from the first live Agent. */
  routerSessionId?: string;
  id: string;
  /** 交付后纠错重开时指向原任务；新增交付物时指向父任务 */
  relatedTaskId: string | null;
  /** relatedTaskId 的语义 */
  relationKind: "none" | "reopen" | "child";
  status: TaskStatus;
  versions: TaskVersion[];
  /** 当前生效版本下标 */
  currentVersion: number;
  pauseReason: PauseReason | null;
  /** 等待对象与唤醒条件（实施文档 §3-2） */
  waitingOn: WaitingState | null;
  createdAt: string;
  updatedAt: string;
  /** 优先级：并行决策时用于呈现（设计文档 §2-4） */
  priority: number;
  /** 暂停前的执行阶段；出队不能把返工或验收重置为普通执行。 */
  resumeStatus?: "active" | "rework" | "awaiting_acceptance";
}

export interface WaitingState {
  /** 在等什么：后台命令 / 某位专家 / 用户答复 / 验收结果 */
  kind: "background_command" | "expert_message" | "user_reply" | "user_approval" | "acceptance" | "external";
  description: string;
  /** 唤醒条件的事件类型或去重键 */
  wakeCondition: string | null;
  since: string;
}

/**
 * 等待登记（阶段3）：任务在等什么、满足什么条件算唤醒。
 * 一条任务可有多个并行等待（等交接 + 等后台命令），但只有一个对外可见的 waitingOn。
 */
export interface TaskWait {
  id: string;
  taskId: string;
  kind: WaitingState["kind"];
  description: string;
  /** 唤醒条件：dependency:<relPath> / acceptance / user_reply / turn:<turnId>；null 表示只能人工解除 */
  wakeCondition: string | null;
  status: "waiting" | "woken" | "expired";
  since: string;
  /** 唤醒/过期时间与原因（排障用） */
  taskVersion?: number;
  executorRelPath?: string;
  observed?: boolean;
  resolvedAt?: string;
  resolvedBy?: string;
}

/** 产物登记：位置与版本 + 验收结论引用（实施文档 §2 产物及裁决） */
export interface Artifact {
  id: string;
  taskId: string;
  /** 磁盘或会话内引用 */
  location: string;
  /** 产物内容哈希，用于验收时核对版本是否变化（实施文档 §3-3） */
  contentHash: string | null;
  ownerRelPath: string;
  createdAt: string;
}

// ---------------------------------------------------------------------------
// 专家消息（设计文档 §4：执行专家直连定向消息）
// ---------------------------------------------------------------------------

export type MessageKind = "dispatch" | "expert_direct" | "summary" | "acceptance_request" | "acceptance_verdict" | "watchdog";

export interface ExpertDeliveryAttempt {
  id: string;
  startedAt: string;
  finishedAt: string | null;
  outcome: "pending" | "delivered" | "unknown" | "failed";
  reconciledAs?: "delivered" | "undelivered";
}

export interface MessageRecord {
  id: string;
  taskId: string;
  kind: MessageKind;
  fromRelPath: string | null;
  toRelPath: string | null;
  replyToId: string | null;
  /** 大内容按引用：指向产物/日志文件，信封只存必要内容 */
  contentRef: string | null;
  excerpt: string;
  /** 投递状态：unknown 表示投递结果未知，恢复时先核对会话历史再决定是否重投 */
  delivery: "pending" | "delivered" | "unknown" | "failed";
  /** 幂等键：同一次动作的重放只触发一次投递（阶段2） */
  dedupeKey: string | null;
  /** 本条消息引用的产物（阶段2：专家间交接不必转发全文，只给引用 + 摘要） */
  artifactRefs?: string[];
  /** 交接时刻的产物版本（路径 → sha256）：后续被接手者修改也能追溯当时版本 */
  artifactVersions?: Record<string, string>;
  /** 完整正文：持久化后可按 messageId 回查原讨论（阶段2 审查 P1-6） */
  body?: string;
  /** 正文指纹（完整正文规范化后的 sha256），用于争议核查，不用于去重 */
  bodyHash?: string;
  /** 消息来源轮次（宿主 turn 标识），配合协议块序号构成动作身份 */
  sourceTurnId?: string;
  /** 动作身份：同一次动作的重放共用一个 actionId（重试用），不同动作即使内容相同也不合并 */
  actionId?: string;
  taskVersion?: number;
  /** 每次实际补投单独留痕；同一条消息记录不等于只投递过一次。 */
  deliveryAttempts?: ExpertDeliveryAttempt[];
  createdAt: string;
  deliveredAt: string | null;
}

// ---------------------------------------------------------------------------
// 验收（实施文档 §2 产物及裁决、§3-3）
// ---------------------------------------------------------------------------

export type VerdictOutcome = "pass" | "gap";

export interface Verdict {
  /** 只认可宿主专用验证工具实际生成的运行记录，不接受文字自报。 */
  verificationRunIds?: string[];
  criterionId: string;
  outcome: VerdictOutcome;
  /** 证据引用（产物版本哈希、文件位置、会话消息） */
  evidence: string;
  /** 缺项说明（outcome=gap 时必填） */
  gap: string | null;
  /** 核对能力不足应暂停补齐能力，不能让执行者反复修改正确产物。 */
  gapKind?: "deliverable" | "capability";
  /** 验收时核对的标准版本与产物哈希；变化则旧结论失效 */
  criteriaVersion: number;
  artifactHashes: Record<string, string>;
  verdictBy: string; // 验收专家 relPath
  createdAt: string;
}

// ---------------------------------------------------------------------------
// Token 账本（实施文档 §4）
// ---------------------------------------------------------------------------

/** 一次模型尝试的用量。字段口径与 tokenUsage 投影一致。 */
export interface UsageSample {
  uncachedInputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
}

export interface LedgerEntry {
  money?: import("./ledger/money.js").MoneyReservation & {chargedNano: string; basis: "usage-priced-estimate"};
  taskId: string;
  /** 请求尝试 ID；真正的重试是新的 attemptId */
  attemptId: string;
  /** 归并后的输入合计（uncached + cacheRead + cacheWrite，按适配器互斥字段归并） */
  inputTokens: number;
  outputTokens: number;
  raw: UsageSample;
  kind: "attempt" | "reservation" | "adjustment";
  createdAt: string;
  note: string | null;
}

export interface Reservation {
  money?: import("./ledger/money.js").MoneyReservation;
  id: string;
  taskId: string;
  tokens: number;
  /** 崩溃后保留未结算预留并对账，不能直接释放（实施文档 §4） */
  state: "reserved" | "settled" | "orphaned" | "released";
  createdAt: string;
  settledAt: string | null;
}

export interface TaskBudgetState {
  money?: import("./ledger/money.js").MoneyState;
  taskId: string;
  limitTokens: number;
  strict: boolean;
  settledTokens: number;
  reservedTokens: number;
  /** 追加额度记录 */
  grants: Array<{ tokens: number; at: string; note: string }>;
}

// ---------------------------------------------------------------------------
// 事件信封（实施文档 §2：eventId、taskId、任务版本、来源、类型、正文引用、因果事件 ID、创建时间）
// ---------------------------------------------------------------------------

export type EventType =
  | "task.batch_completed"
  | "task.proposed"
  | "task.router_bound"
  | "task.approval_changed"
  | "task.confirmed"
  | "task.dispatch"
  | "task.continue"
  | "task.wait"
  | "task.wake"
  | "task.submit_acceptance"
  | "task.verdict"
  | "task.delivered"
  | "task.paused"
  | "task.resumed"
  | "task.cancelled"
  /** 执行专家之间的定向消息（阶段2：先落记录再通知收件人） */
  | "expert.message"
  /** 定向消息影响交付时另记一条摘要，Router 只看摘要（阶段2） */
  | "expert.summary"
  | "expert.message_reconciled"
  | "expert.message_delivery"
  | "expert.waiting"
  | "expert.dependency_ready"
  | "artifact.written"
  /** 增补或替换执行专家（阶段2：保留理由、未完成项、交接对象与产物引用） */
  | "task.executor_changed"
  /** 轮次结束但没有可继续的动作（含"未给出报告块"）：必须留痕，不能静默停摆 */
  | "task.turn_stopped"
  /** 本轮结束但报告块缺失/不可解析 → 已向执行者催办 */
  | "task.report_requested"
  /** 预算已到收尾阈值 → 已向执行者下达"停止探索、落盘、给报告"的收尾指令 */
  | "task.wrap_up_requested"
  /** 读/检索类工具调用超预算 → 已要求执行者转入"只写不读" */
  | "task.read_stop_requested"
  | "expert.message"
  | "watchdog.correction"
  | "watchdog.escalate"
  | "ledger.reserved"
  | "ledger.settled"
  | "ledger.granted"
  | "ledger.exhausted";

export interface EventEnvelope {
  eventId: string;
  taskId: string;
  /** 事件发生时依据的任务版本号 */
  taskVersion: number;
  source: "router" | "worker" | "acceptor" | "watchdog" | "user" | "host";
  type: EventType;
  /** 载荷引用：大日志/原文按引用读取，信封只存必要内容 */
  payloadRef: string | null;
  payload: Record<string, unknown>;
  /** 因果事件 ID */
  causedBy: string | null;
  /** 幂等去重键 */
  dedupeKey: string | null;
  createdAt: string;
}

// ---------------------------------------------------------------------------
// Watchdog（设计文档 §3、实施文档 §2）
// ---------------------------------------------------------------------------

export interface TaskProgressState {
  taskId: string;
  /** 最近实质进展时间；重复发消息本身不算进展（实施文档 §3-5） */
  lastSubstantiveAt: string;
  /** 当前执行阶段的观察起点；恢复/重启不把暂停时间算成停滞，也不伪造产出。 */
  observationStartedAt?: string;
  /** 最近 N 次投递指令的内容哈希，用于识别无效重复 */
  recentInstructionHashes: string[];
  /** 纠偏次数 */
  corrections: number;
  /** 上次尝试及结果（设计文档 §5） */
  lastAttemptNote: string | null;
}
