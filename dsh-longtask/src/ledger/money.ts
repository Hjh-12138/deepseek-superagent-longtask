import {createHash} from "node:crypto";
import type {UsageSample} from "../types.js";

/** All persisted money is a decimal string of nano-CNY; never binary floats or rounded cents. */
const SCALE = 1_000_000_000n;
export function parseCny(value: string): bigint {
  if (typeof value !== "string" || !/^(0|[1-9]\d{0,6})(\.\d{1,9})?$/.test(value)) throw Error("金额须为0至9999999之间的人民币数字，最多9位小数");
  const [whole, fraction = ""] = value.split(".");
  return BigInt(whole!) * SCALE + BigInt(fraction.padEnd(9, "0"));
}
export function formatCny(nano: bigint | string): string {
  const n = BigInt(nano), abs = n < 0n ? -n : n;
  const fraction = (abs % SCALE).toString().padStart(9, "0").replace(/0+$/, "");
  return `${n < 0n ? "-" : ""}${abs / SCALE}${fraction ? "." + fraction : ""}`;
}
export interface PriceCard {
  provider: string; model: string; currency: "CNY";
  /** CNY per million tokens, mutually exclusive SDK buckets. */
  input: string; cacheRead: string; cacheWrite: string; output: string;
  source: string; verifiedAt: string; expiresAt: string;
  basis: "conservative-ceiling" | "configured-estimate";
}
export interface PriceSnapshot extends PriceCard {id: string}
export interface MoneyState {currency: "CNY"; limitNano: string; settledNano: string; reservedNano: string; basis: "usage-priced-estimate"}
export interface MoneyReservation {
  reservedNano: string; price: PriceSnapshot; sessionId: string; role: string;
  expertRelPath: string; turn?: number; step?: number;
}
export const DEFAULT_PRICES: PriceCard[] = [
  {provider: "deepseek-official", model: "deepseek-flash", currency: "CNY", input: "2", cacheRead: "0.04", cacheWrite: "2", output: "8",
    source: "https://api-docs.deepseek.com/zh-cn/quick_start/pricing/", verifiedAt: "2026-11-05T17:20:00Z", expiresAt: "2026-12-06T00:00:00+08:00", basis: "conservative-ceiling"},
];
export class PriceTable {
  private readonly cards: PriceCard[];
  constructor(cards: PriceCard[] = DEFAULT_PRICES) {
    this.cards = structuredClone(cards);
    const keys = new Set<string>();
    for (const c of this.cards) {
      const key = `${c.provider}\0${c.model}`;
      if (!c.provider || !c.model || keys.has(key) || c.currency !== "CNY" || !["conservative-ceiling", "configured-estimate"].includes(c.basis)
        || !c.source || !Number.isFinite(Date.parse(c.verifiedAt)) || !Number.isFinite(Date.parse(c.expiresAt))) throw Error("模型价格表无效或模型路由重复");
      keys.add(key);
      if ([c.input, c.cacheRead, c.cacheWrite, c.output].some(v => parseCny(v) <= 0n)) throw Error("模型价格必须为正数，不允许未知价格按零元处理");
    }
  }
  get(provider: string, model: string, now = new Date()): PriceSnapshot {
    const c = this.cards.find(x => x.provider === provider && x.model === model);
    if (!c) throw Error(`缺少模型价格：${provider}/${model}，未发起请求`);
    if (now.getTime() >= Date.parse(c.expiresAt) || now.getTime() < Date.parse(c.verifiedAt)) throw Error(`模型价格待更新：${provider}/${model}，未发起请求`);
    return {...structuredClone(c), id: createHash("sha256").update(JSON.stringify(c)).digest("hex")};
  }
}
function count(n: number): bigint {if (!Number.isSafeInteger(n) || n < 0) throw Error("计费用量必须为非负安全整数"); return BigInt(n)}
export function priceUsage(p: PriceCard, u: UsageSample): bigint {
  const numerator = count(u.uncachedInputTokens) * parseCny(p.input) + count(u.cacheReadTokens) * parseCny(p.cacheRead)
    + count(u.cacheWriteTokens) * parseCny(p.cacheWrite) + count(u.outputTokens) * parseCny(p.output);
  return (numerator + 999_999n) / 1_000_000n;
}
/** Cache hits cannot be promised in advance. Reserve all input at the most expensive input bucket. */
export function priceBound(p: PriceCard, input: number, output: number): bigint {
  const maxInput = [p.input, p.cacheRead, p.cacheWrite].map(parseCny).reduce((a,b) => a > b ? a : b);
  return (count(input) * maxInput + count(output) * parseCny(p.output) + 999_999n) / 1_000_000n;
}
export function priceTokenCeiling(p: PriceCard, tokens: number): bigint {
  const rate = [p.input, p.cacheRead, p.cacheWrite, p.output].map(parseCny).reduce((a,b) => a > b ? a : b);
  return (count(tokens) * rate + 999_999n) / 1_000_000n;
}

export type TokenProjection = number | {inputTokens: number; outputTokens: number; untypedTokens?: number};
export function priceProjection(p: PriceCard, projection: TokenProjection): bigint {
  return typeof projection === "number" ? priceTokenCeiling(p,projection)
    : priceBound(p,projection.inputTokens,projection.outputTokens)+priceTokenCeiling(p,projection.untypedTokens ?? 0);
}
