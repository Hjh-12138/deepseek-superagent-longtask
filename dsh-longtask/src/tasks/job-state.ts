export const DEFAULT_JOB_TIMEOUT_MS = 2 * 60 * 60_000;

export interface ObservedJob {
  taskId: string; taskVersion: number; relPath: string; jobId: string;
  settled: boolean; registeredAt?: string;
}

/** 宿主已登记的运行命令获得有限等待窗口；重放注册不能延长窗口。 */
export function jobWithinGrace(job: ObservedJob, now: number, timeoutMs: number, fallbackAt: string): boolean {
  const since = Date.parse(job.registeredAt ?? fallbackAt);
  return !job.settled && Number.isFinite(since) && now - since < timeoutMs;
}
