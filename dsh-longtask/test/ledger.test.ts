import { test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { promises as fs } from "node:fs";
import { JsonStore } from "../src/store/store.js";
import { TokenLedger } from "../src/ledger/token-ledger.js";

async function makeLedger() {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-ledger-"));
  const store = await JsonStore.load(join(dir, "state.json"));
  return { store, ledger: new TokenLedger(store) };
}

test("预留-结算：可用余额 = 上限 − 已结算 − 在途预留", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 1000, false);

  const r1 = await ledger.reserve("t1", 400, 100); // 预留 500
  assert.ok(r1.ok);
  assert.equal(ledger.available("t1"), 500);

  const r2 = await ledger.reserve("t1", 400, 200); // 需要 600 > 500
  assert.ok(!r2.ok && r2.reason === "insufficient");
  assert.equal(r2.available, 500);

  const usage = { uncachedInputTokens: 350, outputTokens: 80, cacheReadTokens: 0, cacheWriteTokens: 0 };
  await ledger.settle(r1.ok ? r1.reservationId : "", usage, null);
  assert.equal(ledger.available("t1"), 1000 - 430);
  await store.close();
});

test("并行 Worker 不共享同一份余额：两个预留同时占用", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 1000, false);
  const r1 = await ledger.reserve("t1", 600, 0);
  const r2 = await ledger.reserve("t1", 600, 0);
  assert.ok(r1.ok);
  assert.ok(!r2.ok && r2.reason === "insufficient"); // 第二个 Worker 被拒
  await store.close();
});

test('验收保留在原子预留事务中保护；并发执行者不能抢走，也不算已消耗',async()=>{
  const {store,ledger}=await makeLedger();try{
    await ledger.ensureBudget('protected',1000,true);
    const results=await Promise.all([ledger.reserve('protected',200,0,700),ledger.reserve('protected',200,0,700)]);
    assert.equal(results.filter(r=>r.ok).length,1);assert.equal(ledger.getBudget('protected')!.reservedTokens,200);assert.equal(ledger.totalSettled('protected'),0);
    assert.equal((await ledger.reserve('protected',700,0)).ok,true,'验收自身可使用保护额度，不是双重预留');
    await assert.rejects(()=>ledger.reserve('protected',1,0,-1));
  }finally{await store.close();}
});

test("缓存分项归并：inputTokens = uncached + cacheRead + cacheWrite，总量不重复相加", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 10_000, false);
  const r = await ledger.reserve("t1", 1000, 500);
  assert.ok(r.ok);
  const usage = { uncachedInputTokens: 1200, outputTokens: 300, cacheReadTokens: 5000, cacheWriteTokens: 0 };
  const { overLimit } = await ledger.settle(r.ok ? r.reservationId : "", usage, null);
  assert.equal(ledger.totalSettled("t1"), 1200 + 5000 + 300); // 6500，输出只计一次
  assert.ok(!overLimit);
  await store.close();
});

test("重试单独计费：两个 attemptId 各记一条", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 10_000, false);
  const r1 = await ledger.reserve("t1", 100, 50);
  const r2 = await ledger.reserve("t1", 100, 50);
  assert.ok(r1.ok && r2.ok);
  const u1 = { uncachedInputTokens: 120, outputTokens: 60, cacheReadTokens: 0, cacheWriteTokens: 0 };
  const u2 = { uncachedInputTokens: 130, outputTokens: 40, cacheReadTokens: 0, cacheWriteTokens: 0 };
  await ledger.settle(r1.reservationId, u1, "首次尝试");
  await ledger.settle(r2.reservationId, u2, "重试"); // 真正的重试是新 attemptId
  assert.equal(ledger.totalSettled("t1"), (120 + 60) + (130 + 40));
  await store.close();
});

test("严格模式：无输入上界时在调用前拒绝", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 10_000, true);
  const r = await ledger.reserve("t1", null, 100);
  assert.ok(!r.ok && r.reason === "unknown_input_bound");
  assert.equal(ledger.getBudget("t1")!.reservedTokens, 0);
  await store.close();
});

test("宽松模式：无输入上界放行，结算时补记", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 10_000, false);
  const r = await ledger.reserve("t1", null, 100);
  assert.ok(r.ok);
  await ledger.settle(r.reservationId, { uncachedInputTokens: 900, outputTokens: 100, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);
  assert.equal(ledger.totalSettled("t1"), 1000);
  await store.close();
});

test("崩溃对账：未结算保留为 reserved；确认消耗才转 settled（不能直接释放）", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 1000, false);
  const r = await ledger.reserve("t1", 300, 100);
  assert.ok(r.ok);
  assert.equal(ledger.listUnsettledReservations().length, 1);

  // 直接释放被拒绝（confirmConsumed=false 且无用量）→ released
  await ledger.reconcile(r.reservationId, null, false);
  assert.equal(ledger.listUnsettledReservations().length, 0);

  // 第二种路径：确认消耗但无用量 → 按预留额估算转结算
  const r2 = await ledger.reserve("t1", 300, 100);
  assert.ok(r2.ok);
  await ledger.reconcile(r2.reservationId, null, true);
  assert.equal(ledger.totalSettled("t1"), 400); // 保守记账
  assert.equal(ledger.getBudget("t1")!.reservedTokens, 0);
  await store.close();
});

test("超支检测：结算后超上限置 overLimit", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 100, false);
  const r = await ledger.reserve("t1", 10, 10);
  assert.ok(r.ok);
  const { overLimit } = await ledger.settle(r.reservationId, { uncachedInputTokens: 500, outputTokens: 500, cacheReadTokens: 0, cacheWriteTokens: 0 }, null);
  assert.ok(overLimit);
  const next = await ledger.reserve("t1", 10, 10);
  assert.ok(!next.ok); // 余额为负，下一次预留被拒
  await store.close();
});

test("追加额度：上限增加可再次预留", async () => {
  const { store, ledger } = await makeLedger();
  await ledger.ensureBudget("t1", 100, false);
  const r1 = await ledger.reserve("t1", 100, 0);
  assert.ok(r1.ok);
  const blocked = await ledger.reserve("t1", 10, 10);
  assert.ok(!blocked.ok);
  await ledger.grant("t1", 200, "用户追加");
  const r2 = await ledger.reserve("t1", 10, 10);
  assert.ok(r2.ok);
  await store.close();
});
