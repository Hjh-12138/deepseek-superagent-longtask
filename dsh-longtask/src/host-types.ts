/**
 * 宿主服务结构类型 · 按 DSH 0.2.1-rc.5 真实 SDK 声明（dsh-agent/lib/types）。
 *
 * 阶段 1 审查 P1-3 修正：agents.create 必填 sessionId，返回 AgentHandle =
 * { agent, dispose }——句柄不是 Agent 本体，followup 等运行时方法在
 * handle.agent 上。所有接口在探针实测前不作为已验证能力。
 */

import type { SessionEvent, SessionLike } from "./session-events.js";
import type { UserMessage } from "@deepseek-ai/dsh-llm";
declare module "@deepseek-ai/dsh-llm" {
  interface MessageSourceMap {
    longtask: { kind: "longtask"; dedupeKey: string };
    "longtask-probe": { kind: "longtask-probe" };
  }
}
export type { SessionEvent, SessionLike } from "./session-events.js";

/** dsh-agent 运行时 Agent（子集，与 Agent 接口的 followup/cancel/whenIdle/status 对齐） */
export interface AgentLike {
  id: string;
  session: SessionLike;
  status: "idle" | "running" | string;
  followup(message: UserMessage): void;
  /** 在当前 turn 的下一步输入边界投递控制消息（SDK 0.2.1-rc.5）。 */
  steer?(message: UserMessage): void;
  cancel(cause: Record<string, unknown>, options?: { keepInbox?: boolean }): void;
  whenIdle(): Promise<void>;
}

/**
 * AgentHandle（dsh-agent/lib/types/index.d.ts）：
 * create/resume 返回的所属句柄，dispose 是唯一 teardown 能力。
 */
export interface AgentHandle {
  agent: AgentLike;
  dispose(): Promise<void>;
}

/**
 * CreateAgentOptions（对齐真实 SDK）：sessionId 由调用方提供的
 * 单一活动身份（agent registry 与 session log 共用）。
 */
export interface CreateAgentOptions {
  sessionId: string;
  /** 会话元数据：cwd 决定工作区（沙箱 workspace-write 的写入根） */
  meta?: { cwd?: string };
  agentOptions?: { provider?: string; model?: string; maxTokens?: number; reasoningEffort?: string };
  setup?(agentCtx: unknown, agent: AgentLike): unknown | Promise<unknown>;
}

/** dsh-agent 的 ctx.agents 服务（子集） */
export interface AgentsService {
  get(id: string): AgentLike | undefined;
  list(): AgentLike[];
  create(options: CreateAgentOptions): Promise<AgentHandle>;
}

/** dsh-session 的 ctx.sessions 服务（子集） */
export interface SessionsService {
  flush(session: SessionLike): Promise<void>;
}

/** tokenUsage 投影/供应商用量（dsh-token-meter 口径） */
export interface TokenUsage {
  uncachedInputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
}

/** Cordis 上下文（插件用到的面） */
export interface HostContext {
  logger: { info(msg: string): void; warn(msg: string): void; error(msg: string): void };
  get<T = unknown>(service: string): T | undefined;
  on(event: string, listener: (...args: any[]) => void): (() => void) | void;
  off?(event: string, listener: (...args: any[]) => void): void;
  effect(factory: () => Generator, label?: string): void;
  provide(name: string, value: unknown): void;
  [key: string]: unknown;
}
