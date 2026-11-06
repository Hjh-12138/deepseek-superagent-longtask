import { linkSync, mkdirSync, unlinkSync, writeFileSync,existsSync,readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseRunRequest, SCENARIOS } from "./evidence.mjs";
import {STAGE4_VERSION,STAGE4_TERMINAL} from './stage4-protocol.mjs';

/** 原子发布一次显式运行请求；不覆盖排队中的请求，不重启DSH、不追加旧任务预算。 */
export function publishRunRequest(raw, directory = resolve(import.meta.dirname, "../dsh-longtask/.longtask/e2e")) {
  const request = parseRunRequest({ at: new Date().toISOString(), requestId: randomUUID(),
    ...(['doc','dev'].includes(raw?.scenario)?{stage4Version:STAGE4_VERSION}:{}),...raw });
  const dir = resolve(directory);
  for(const [sub,terminal] of [['stage3',new Set(['passed','failed','cancelled'])],['stage4',STAGE4_TERMINAL]]){
    const path=join(dir,sub,'state.json');if(existsSync(path)&&!terminal.has(JSON.parse(readFileSync(path,'utf8')).phase))throw Error(`已有${sub}运行，不能混跑`);
  }
  if(existsSync(join(dir,'crash-pending.json')))throw Error('有未核对的崩溃标记，不提交新请求');
  if(existsSync(join(dir,'stage4/request.json')))throw Error('已有阶段四待执行请求，不提交新请求');
  mkdirSync(dir, { recursive: true });
  const temp = join(dir, `.request-${randomUUID()}.tmp`);
  const target = join(dir, "run-request.json");
  writeFileSync(temp, JSON.stringify(request, null, 2), { encoding: "utf8", flag: "wx" });
  try {
    // link是一次不可覆盖的发布：消费者看不到半写入的JSON，已有请求也不会被替换。
    linkSync(temp, target);
  } finally { unlinkSync(temp); }
  return { path: target, request };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [scenario, amount, directory] = process.argv.slice(2);
    if (!scenario || !/^\d+$/.test(amount ?? "")) throw Error(`用法：node request-run.mjs <${SCENARIOS.join("|")}> <Token上限> [记录目录]`);
    const result = publishRunRequest({ scenario, budgetTokens: Number(amount), strict: true }, directory);
    console.log(`已提交一次${scenario}联调，上限${result.request.budgetTokens} Token：${result.path}`);
  } catch (error) {
    console.error(error.code === "EEXIST" ? "已有待执行请求，未覆盖；等待该请求被消费后再提交。" : String(error.message ?? error));
    process.exitCode = 1;
  }
}
