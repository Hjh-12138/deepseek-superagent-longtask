import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtemp} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {JsonStore} from "../src/store/store.js";
import {TokenLedger} from "../src/ledger/token-ledger.js";
import {PriceTable, DEFAULT_PRICES, parseCny, formatCny, priceUsage, priceBound} from "../src/ledger/money.js";
import type {TaskBudgetState, Reservation} from "../src/types.js";
const route = {provider: "deepseek-official", model: "deepseek-flash"};
const clock = () => new Date("2026-11-06T02:00:00+08:00");
const identity = {sessionId: "s", role: "executor", expertRelPath: "worker.md"};
const usage = {uncachedInputTokens: 1000, cacheReadTokens: 2000, cacheWriteTokens: 300, outputTokens: 500};
async function fixture(limit = "5") {
  const path = join(await mkdtemp(join(tmpdir(), "longtask-money-")), "state.json"), store = await JsonStore.load(path);
  const ledger = new TokenLedger(store, clock);
  await ledger.ensureBudget("t", Number.MAX_SAFE_INTEGER, true);
  await store.transact(data => {const b = data.collections.budgets!.t as TaskBudgetState;
    b.money = {currency: "CNY", limitNano: parseCny(limit).toString(), settledNano: "0", reservedNano: "0", basis: "usage-priced-estimate"};});
  const reserve = async (input = 10000, output = 8000) => {const r = await ledger.reserve("t", input, output); assert.ok(r.ok); return r.reservationId;};
  return {path, store, ledger, reserve};
}
test("金额精度：纳元累计不舍入到分，拒绝指数/负数/浮点数输入", () => {
  assert.equal(formatCny(parseCny("0.000000001") * 1000n), "0.000001");
  for (const invalid of ["-1", "1e3", "NaN", "1.0000000001", "01", "￥5", "", " 5 "]) assert.throws(() => parseCny(invalid));
  const p = new PriceTable().get(route.provider, route.model, clock());
  assert.equal(formatCny(priceUsage(p, usage)), "0.00668");
  assert.equal(formatCny(priceBound(p, 10000, 8000)), "0.084");
});
test("价格路由：缺价/过期/未来/重复路由/零价格均不当作免费", () => {
  const table = new PriceTable(); assert.throws(() => table.get("other", route.model, clock()), /缺少/);
  assert.throws(() => table.get(route.provider, route.model, new Date("2027-01-01")), /更新/);
  assert.throws(() => table.get(route.provider, route.model, new Date("2020-01-01")), /更新/);
  assert.throws(() => new PriceTable([...DEFAULT_PRICES, ...DEFAULT_PRICES]), /重复/);
  assert.throws(() => new PriceTable([{...DEFAULT_PRICES[0]!, input: "0"}]), /正数/);
});
test("金额结算与事件幂等在同一事务，价格快照与专家归属可追溯", async () => {
  const f = await fixture(); try {
    const id = await f.reserve(); await f.ledger.reserveMoney(id, route, 10000, 8000, 0, identity);
    assert.equal(f.ledger.moneyView("t")?.reservedAmount, "0.084");
    await f.ledger.settle(id, usage, "paid", "event1"); await f.ledger.settle(id, usage, "duplicate", "event1");
    const m = f.ledger.moneyView("t")!; assert.equal(m.calculatedAmount, "0.00668"); assert.equal(m.reservedAmount, "0");
    const entries = f.ledger.moneyEntries("t"); assert.equal(entries.length, 1); assert.equal(entries[0]!.expertRelPath, "worker.md");
    assert.equal(entries[0]!.priceSnapshot.input, "2"); assert.equal(entries[0]!.priceSnapshot.id.length, 64);
    await assert.rejects(() => f.ledger.grant("t", 1_000_000, "不能绕过金额预算"), /不能/);
  } finally {await f.store.close();}
});
test("并行专家原子预留金额：两份请求不能同时花掉同一份钱", async () => {
  const f = await fixture("0.1"); try {
    const a = await f.reserve(), b = await f.reserve();
    const outcomes = await Promise.allSettled([f.ledger.reserveMoney(a, route, 10000, 8000, 0, identity), f.ledger.reserveMoney(b, route, 10000, 8000, 0, {...identity, sessionId: "b"})]);
    assert.equal(outcomes.filter(x => x.status === "fulfilled").length, 1); assert.equal(f.ledger.moneyView("t")?.reservedAmount, "0.084");
    assert.equal(f.ledger.moneyView("t")?.availableAmount, "0.016");
  } finally {await f.store.close();}
});
test("验收费用受保护；缺价失败无金额预留，未发出请求可释放Token占位", async () => {
  const f = await fixture("0.1"); try {
    const id = await f.reserve();
    await assert.rejects(() => f.ledger.reserveMoney(id, route, 10000, 8000, 5000, identity), /金额不足/);
    await assert.rejects(() => f.ledger.reserveMoney(id, {...route, model: "unknown"}, 10000, 8000, 0, identity), /缺少/);
    assert.equal(f.ledger.moneyView("t")?.reservedAmount, "0"); await f.ledger.releaseUnsent(id); assert.equal(f.ledger.getBudget("t")?.reservedTokens, 0);
  } finally {await f.store.close();}
});
test("真正重试单独收费；重建价格表不改变既有请求的价格快照", async () => {
  const f = await fixture(); try {
    const id = await f.reserve(); await f.ledger.reserveMoney(id, route, 10000, 8000, 0, identity);
    const changed = new TokenLedger(f.store, clock, new PriceTable([{...DEFAULT_PRICES[0]!, input: "20", output: "80"}]));
    await changed.settle(id, usage, null, "e1"); assert.equal(changed.moneyView("t")?.calculatedAmount, "0.00668");
    const second = await f.reserve(); await f.ledger.reserveMoney(second, route, 10000, 8000, 0, identity); await f.ledger.settle(second, usage, null, "e2");
    assert.equal(f.ledger.moneyView("t")?.calculatedAmount, "0.01336"); assert.equal(f.ledger.moneyEntries("t").length, 2);
  } finally {await f.store.close();}
});
test("崩溃恢复：未知费用保留预留并关闸，重启不清账不当作零费用", async () => {
  const f = await fixture(); const id = await f.reserve(); await f.ledger.reserveMoney(id, route, 10000, 8000, 0, identity); await f.store.close();
  const store = await JsonStore.load(f.path), restored = new TokenLedger(store, clock); try {
    await restored.reconcile(id, null, true); assert.ok(restored.accountingIssue("t"));
    assert.equal(restored.moneyView("t")?.reservedAmount, "0.084"); assert.equal(restored.moneyView("t")?.calculatedAmount, "0");
    assert.equal(store.get<Reservation>("reservations", id)?.state, "reserved");
    await assert.rejects(() => restored.reserveMoney(id, route, 10000, 8000, 0, identity));
  } finally {await store.close();}
});
test("恢复可证明未发出的占位不收费；核对到真实用量按原价结算", async () => {
  const f = await fixture(); try {
    const unsent = await f.reserve(); await f.ledger.reconcile(unsent, null, true); assert.equal(f.ledger.getBudget("t")?.settledTokens, 0);
    const sent = await f.reserve(); await f.ledger.reserveMoney(sent, route, 10000, 8000, 0, identity);
    await f.ledger.reconcile(sent, usage, true); assert.equal(f.ledger.moneyView("t")?.calculatedAmount, "0.00668"); assert.equal(f.ledger.moneyView("t")?.reservedAmount, "0");
  } finally {await f.store.close();}
});

test("未来验收输入按输入价保留，实际请求上限与原金额保护不变",async()=>{
  const f=await fixture("8");try{
    const plan={inputTokens:555024,outputTokens:512000};
    const forecast=f.ledger.moneyForecast("t",21000,256000,plan)!;
    assert.equal(formatCny(forecast.protected),"5.206048");
    const id=await f.reserve(21000,256000);
    await f.ledger.reserveMoney(id,route,21000,256000,plan,identity);
    assert.equal(f.ledger.moneyView("t")!.reservedAmount,"2.09");
    assert.equal(f.ledger.moneyView("t")!.limitAmount,"8");
  }finally{await f.store.close();}
});
