/**
 * 事务化存储层 · 实施文档 §2。
 *
 * 首版默认 JSON 事务存储（内存 + 原子落盘：临时文件 + rename + fsync）。
 * SQLite 适配位预留，运行时由 probe 探测 node:sqlite 可用性。
 *
 * 设计要点（阶段 1 审查 P2-10/11 修正后）：
 * - 事务在**隔离副本**上执行：mutator 修改的是 this.data 的深拷贝，
 *   未提交的写入对外（get/list/findByDedupe）不可见；mutator 抛错直接丢弃副本。
 * - 成功路径：副本 → 原子落盘 → 一次性替换 this.data（提交点）。
 * - close() 区分"停止接收新事务"与"排空已接受事务"：已排队的事务
 *   全部执行完毕后 close 才返回，不丢已接受的写入。
 * - 事务串行执行，避免并发 rename 竞争与读-改-写丢失。
 */

import { promises as fs } from "node:fs";
import { dirname, join } from "node:path";

export interface StoredData {
  /** 集合名 -> (id -> 记录) */
  collections: Record<string, Record<string, unknown>>;
  /** 按插入顺序保留的事件日志（eventId -> envelope） */
  events: Record<string, unknown>;
  eventOrder: string[];
  /** 幂等键索引：dedupeKey -> eventId */
  dedupeIndex: Record<string, string>;
  meta: { schemaVersion: number; savedAt: string };
}

export interface EventEnvelopeInput {
  eventId: string;
  dedupeKey: string | null;
  [key: string]: unknown;
}

/** 事务对象：集合读写 + 事件追加（dedupe 幂等）；操作的是事务私有副本 */
export type Tx = StoredData & {
  appendEvent(envelope: EventEnvelopeInput): { deduplicated: boolean };
  findByDedupe(dedupeKey: string): unknown | undefined;
};

type Mutator = (data: Tx) => void | Promise<void>;

export interface Store {
  subscribe?(listener: () => void): () => void;
  /** 在同一事务中读取并修改多个集合；异常或落盘失败都不留半成品；未提交写入对外不可见 */
  transact(mutator: Mutator): Promise<void>;
  get<T>(collection: string, id: string): T | undefined;
  put<T>(collection: string, id: string, value: T): void;
  remove(collection: string, id: string): void;
  list<T>(collection: string): T[];
  /** 事务外的事件追加（立即提交） */
  appendEvent(envelope: EventEnvelopeInput): { deduplicated: boolean };
  findByDedupe(dedupeKey: string): unknown | undefined;
  /** 只读快照（测试与恢复核对用） */
  snapshot(): StoredData;
  /** 停止接收新事务，排空已接受事务后返回 */
  close(): Promise<void>;
}

const SCHEMA_VERSION = 1;

export function emptyData(): StoredData {
  return { collections: {}, events: {}, eventOrder: [], dedupeIndex: {}, meta: { schemaVersion: SCHEMA_VERSION, savedAt: "" } };
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

/** 在指定数据集上追加事件（dedupe 幂等）。供事务副本与已提交数据共用。 */
function appendEventTo(data: StoredData, envelope: EventEnvelopeInput): { deduplicated: boolean } {
  if (envelope.dedupeKey !== null && data.dedupeIndex[envelope.dedupeKey] !== undefined) {
    return { deduplicated: true };
  }
  data.events[envelope.eventId] = clone(envelope);
  data.eventOrder.push(envelope.eventId);
  if (envelope.dedupeKey !== null) data.dedupeIndex[envelope.dedupeKey] = envelope.eventId;
  return { deduplicated: false };
}

function findByDedupeIn(data: StoredData, dedupeKey: string): unknown | undefined {
  const eventId = data.dedupeIndex[dedupeKey];
  if (eventId === undefined) return undefined;
  return data.events[eventId];
}

export class JsonStore implements Store {
  private readonly committedListeners = new Set<() => void>();
  subscribe(listener: () => void): () => void {this.committedListeners.add(listener); return () => {this.committedListeners.delete(listener)};}
  private data: StoredData;
  private readonly filePath: string;
  /** 串行化事务队列 */
  private queue: Promise<unknown> = Promise.resolve();
  /** true 后拒绝新事务；已接受的事务照常执行 */
  private closed = false;

  constructor(filePath: string, initial?: StoredData) {
    this.filePath = filePath;
    this.data = initial ? clone(initial) : emptyData();
  }

  /** 从磁盘加载；文件不存在返回空实例，损坏时备份并抛错（由恢复器决定后续） */
  static async load(filePath: string): Promise<JsonStore> {
    let raw: string;
    try {
      raw = await fs.readFile(filePath, "utf8");
    } catch {
      return new JsonStore(filePath);
    }
    try {
      const parsed = JSON.parse(raw) as StoredData;
      if (parsed.meta?.schemaVersion !== SCHEMA_VERSION) {
        throw new Error(`不支持的存储 schema 版本: ${String((parsed.meta as { schemaVersion?: number } | undefined)?.schemaVersion)}`);
      }
      return new JsonStore(filePath, parsed);
    } catch (error) {
      if ((error as Error).message.includes("schema 版本")) throw error;
      const backup = `${filePath}.corrupt-${Date.now()}`;
      await fs.copyFile(filePath, backup).catch(() => {});
      throw new Error(`存储文件损坏，已备份到 ${backup}: ${(error as Error).message}`);
    }
  }

  // ---- 已提交数据的只读面：永远看不到未提交副本 ----

  get<T>(collection: string, id: string): T | undefined {
    return this.data.collections[collection]?.[id] as T | undefined;
  }

  put<T>(collection: string, id: string, value: T): void {
    (this.data.collections[collection] ??= {})[id] = clone(value);
  }

  remove(collection: string, id: string): void {
    delete this.data.collections[collection]?.[id];
  }

  list<T>(collection: string): T[] {
    return Object.values(this.data.collections[collection] ?? {}) as T[];
  }

  appendEvent(envelope: EventEnvelopeInput): { deduplicated: boolean } {
    return appendEventTo(this.data, envelope);
  }

  findByDedupe(dedupeKey: string): unknown | undefined {
    return findByDedupeIn(this.data, dedupeKey);
  }

  snapshot(): StoredData {
    return clone(this.data);
  }

  // ---- 事务 ----

  /** 接受新事务；closed 后拒绝。已接受的在队列上照常执行。 */
  transact(mutator: Mutator): Promise<void> {
    if (this.closed) return Promise.reject(new Error("store 已关闭"));
    const run = this.queue.then(() => this.runTransact(mutator));
    this.queue = run.catch(() => {}); // 失败不阻塞后续事务
    return run;
  }

  /**
   * 隔离执行：深拷贝当前提交态作为事务工作副本，mutator 只见副本；
   * 成功后落盘副本，再一次性替换 this.data（提交点）。
   * 抛错或落盘失败 → 副本直接丢弃，已提交态不受影响。
   */
  private async runTransact(mutator: Mutator): Promise<void> {
    const work = clone(this.data);
    const tx: Tx = {
      ...work,
      appendEvent: (envelope) => appendEventTo(work, envelope),
      findByDedupe: (key) => findByDedupeIn(work, key),
    };
    await mutator(tx); // 抛错则 work 被丢弃
    await this.persist(work); // 落盘失败同样丢弃
    this.data = work; // 提交点
    for (const listener of this.committedListeners) {try {listener()} catch {/* notification observers cannot roll back a committed transaction */}}
  }

  private async persist(data: StoredData): Promise<void> {
    data.meta.savedAt = new Date().toISOString();
    const serialized = JSON.stringify(data);
    const dir = dirname(this.filePath);
    await fs.mkdir(dir, { recursive: true });
    const tmp = join(dir, `.tmp-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    const handle = await fs.open(tmp, "w");
    try {
      await handle.writeFile(serialized, "utf8");
      await handle.sync();
    } finally {
      await handle.close();
    }
    await fs.rename(tmp, this.filePath);
  }

  /** 停止接收新事务，等待所有已接受事务结束（含失败）后返回 */
  async close(): Promise<void> {
    this.closed = true;
    await this.queue.catch(() => {});
  }
}

/** 生成内容哈希（专家快照、产物版本核对共用） */
export async function contentHash(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/** 生成短随机 ID */
export function newId(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
}
