/**
 * dsh-session 会话事件类型（子集，按 0.2.1-rc.5 dsh-session/lib/types/types.d.ts）。
 */

export interface SessionLike {
  id: string;
  /** 持久化落盘（goal-round-driver 在排队前 await sessions.flush） */
  flush?: () => Promise<void>;
}

/** assistant/message 事件载荷：message.content 为内容块数组 */
export interface AssistantMessagePayload {
  turn: number;
  step: number;
  message: {
    role: "assistant";
    content: Array<{ type: string; text?: string }>;
    [key: string]: unknown;
  };
  usage?: {
    /** SDK 0.2.1-rc.5：未缓存输入，与 cacheRead/cacheWrite 互斥分项。 */
    inputTokens?: number;
    /** 兼容已归一化的账本采样。 */
    uncachedInputTokens?: number;
    outputTokens: number;
    cacheReadTokens: number;
    cacheWriteTokens: number;
  };
  interrupted?: true;
}

export interface SessionEvent {
  type: string;
  data: {
    id?: string;
    /** turn/end 的结束原因（dsh-goal-round-driver 监听 turn/end 时读取） */
    reason?: { kind: string; [key: string]: unknown };
    /** assistant/message 载荷 */
    message?: AssistantMessagePayload["message"];
    usage?: AssistantMessagePayload["usage"];
    turn?: number;
    interrupted?: true;
    [key: string]: unknown;
  };
}
