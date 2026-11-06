import { test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { promises as fs } from "node:fs";
import { JsonStore, contentHash, newId, emptyData } from "../src/store/store.js";

test("事务原子性：mutator 抛错则内存与磁盘都不变", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-store-"));
  const file = join(dir, "state.json");
  const store = new JsonStore(file);
  await store.transact((data) => {
    (data.collections["tasks"] ??= {})["t1"] = { id: "t1" };
  });

  await assert.rejects(
    store.transact((data) => {
      data.collections["tasks"]!["t1"] = { id: "t1", mutated: true };
      throw new Error("boom");
    }),
    /boom/,
  );
  assert.equal(store.get<{ id: string; mutated?: boolean }>("tasks", "t1")?.mutated, undefined);

  await store.close();
  const reloaded = await JsonStore.load(file);
  assert.equal(reloaded.get<{ id: string }>("tasks", "t1")?.id, "t1");
});

test("事务隔离：未提交写入对外不可见（P2-11）", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-store-"));
  const store = new JsonStore(join(dir, "state.json"));
  let release: () => void = () => {};
  const gate = new Promise<void>((r) => (release = r));

  const tx = store.transact(async (data) => {
    (data.collections["counters"] ??= {})["n"] = { value: 1 };
    await gate; // 挂在事务中段，模拟异步 mutator
  });

  await Promise.resolve(); // 让事务开始执行并写入副本
  // 事务中段：外部读取必须仍见提交前状态
  assert.equal(store.get<{ value: number }>("counters", "n"), undefined);
  assert.equal(store.findByDedupe("k-x"), undefined);

  release();
  await tx;
  assert.equal(store.get<{ value: number }>("counters", "n")?.value, 1); // 提交后可见

  // 失败事务不外泄：写入副本后抛错，外部从未见过
  let release2: () => void = () => {};
  const gate2 = new Promise<void>((r) => (release2 = r));
  const tx2 = store.transact(async (data) => {
    (data.collections["counters"] ??= {})["n"] = { value: 2 };
    await gate2;
    throw new Error("late failure");
  });
  await Promise.resolve();
  release2();
  await assert.rejects(tx2, /late failure/);
  assert.equal(store.get<{ value: number }>("counters", "n")?.value, 1); // 仍是提交态
  await store.close();
});

test("事务内读取使用隔离副本", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-store-"));
  const store = new JsonStore(join(dir, "state.json"));
  await store.transact((data) => {
    (data.collections["c"] ??= {})["a"] = { v: 1 };
    // 同一事务内读自己刚写的
    assert.equal(data.collections["c"]?.["a"] && (data.collections["c"]!["a"] as { v: number }).v, 1);
    // dedupe 在事务内立即可查（写入副本）
    data.appendEvent({ eventId: "e1", dedupeKey: "dk1", type: "t" });
    assert.ok(data.findByDedupe("dk1"));
  });
  await store.close();
});

test("close 排空已接受事务：close 前排队的事务不丢（P2-10）", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-store-"));
  const file = join(dir, "state.json");
  const store = new JsonStore(file);
  await store.transact((data) => {
    (data.collections["c"] ??= {})["k"] = { v: 0 };
  });

  const queued = store.transact((data) => {
    (data.collections["c"]!["k"] as { v: number }).v = 42;
  });
  // close 不接受新事务，但必须等 queued 完成
  await store.close();
  await queued; // 不应抛"store 已关闭"
  assert.equal(store.get<{ v: number }>("c", "k")?.v, 42);

  await assert.rejects(store.transact(() => {}), /已关闭/);
  const reloaded = await JsonStore.load(file);
  assert.equal(reloaded.get<{ v: number }>("c", "k")?.v, 42);
});

test("并发事务串行执行，读-改-写不丢失", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-store-"));
  const store = new JsonStore(join(dir, "state.json"));
  await Promise.all(
    Array.from({ length: 20 }, (_, i) =>
      store.transact((data) => {
        const coll = (data.collections["counters"] ??= {});
        const c = (coll["n"] as { value: number } | undefined) ?? { value: 0 };
        c.value += 1;
        coll["n"] = c;
        void i;
      }),
    ),
  );
  assert.equal(store.get<{ value: number }>("counters", "n")?.value, 20);
  await store.close();
});

test("dedupeKey 幂等：同键事件只落一次", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-store-"));
  const store = new JsonStore(join(dir, "state.json"));
  const e1 = { eventId: newId("evt"), dedupeKey: "k1", type: "task.dispatch" };
  const e2 = { eventId: newId("evt"), dedupeKey: "k1", type: "task.dispatch" };
  await store.transact((data) => {
    assert.equal(data.appendEvent(e1).deduplicated, false);
  });
  await store.transact((data) => {
    assert.equal(data.appendEvent(e2).deduplicated, true);
  });
  assert.equal(store.snapshot().eventOrder.length, 1);
  await store.close();
});

test("损坏文件：加载报错并备份原文件", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-store-"));
  const file = join(dir, "state.json");
  await fs.writeFile(file, "{ not json", "utf8");
  await assert.rejects(JsonStore.load(file), /损坏/);
  const entries = await fs.readdir(dir);
  assert.ok(entries.some((e) => e.includes(".corrupt-")));
});

test("contentHash 稳定且区分内容", async () => {
  const a1 = await contentHash("hello");
  const a2 = await contentHash("hello");
  const b = await contentHash("hello!");
  assert.equal(a1, a2);
  assert.notEqual(a1, b);
  assert.equal(a1.length, 64);
});

test("emptyData 结构完整", () => {
  const d = emptyData();
  assert.deepEqual(Object.keys(d).sort(), ["collections", "dedupeIndex", "eventOrder", "events", "meta"]);
});
