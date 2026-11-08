import {promises as fs} from "node:fs";
import {join} from "node:path";
import {randomUUID} from "node:crypto";
import type {Store,Tx} from "./store/store.js";
import type {Task,Verdict,TaskVersion} from "./types.js";
import {missingVerification,verificationReview,VERIFICATION_RUNS,type VerificationRun} from "./verification.js";

export const DELIVERY_EVIDENCE="deliveryEvidence";
export interface DeliveryEvidence {
  id:string;taskId:string;version:number;roundId:string;createdAt:string;
  exportState:"pending"|"exported"|"error";paths?:{report:string;json:string;plan:string;reproduce:string};lastError?:string;
  snapshot:{taskId:string;version:number;roundId:string;completedAt:string;goal:string;signer:string;relatedTaskId:string|null;
    criteria:TaskVersion["criteria"];verdicts:Verdict[];scope:{included:string[];excluded:string[]};
    artifacts:Array<{id:string;location:string;sha256:string}>;runs:VerificationRun[];plan:TaskVersion["verificationPlan"]|null;
    missingRuntimeEvidence:string[];money:unknown;historical:boolean};
}
export function queueDeliveryEvidence(data:Tx,task:Task,round:{roundId:string;checkpoint?:{stepEpoch?:number}},verdicts:Verdict[],hashes:Record<string,string>,historical=false):void {
  const v=task.versions[task.currentVersion]!,id=`${task.id}:v${v.version}:${round.roundId}`,coll=(data.collections[DELIVERY_EVIDENCE]??={});if(coll[id])return;
  const ids=new Set(verdicts.flatMap(v=>v.verificationRunIds??[]));
  const runs=(Object.values(data.collections[VERIFICATION_RUNS]??{}) as VerificationRun[]).filter(r=>r.taskId===task.id&&r.version===v.version&&r.roundId===round.roundId&&r.stepEpoch===(round.checkpoint?.stepEpoch??0)&&ids.has(r.id));
  const artifacts=Object.entries(hashes).map(([id,sha256])=>({id,sha256,location:(data.collections.artifacts?.[id] as {location?:string}|undefined)?.location??"未登记路径"}));
  const excluded=[...v.verificationScope?.excluded??[]];
  if(!runs.some(r=>r.kind==="browser"&&r.status==="passed"))excluded.push("本轮没有成功的browser运行记录；不证明浏览器渲染、页面交互、刷新会话或控制台已通过。");
  excluded.push("未列入本轮标准及检查记录的安装链、环境和行为，不作通过结论。");
  coll[id]={id,taskId:task.id,version:v.version,roundId:round.roundId,createdAt:task.updatedAt,exportState:"pending",snapshot:{
    taskId:task.id,version:v.version,roundId:round.roundId,completedAt:task.updatedAt,goal:v.goal,signer:v.acceptor.relPath,relatedTaskId:task.relatedTaskId,
    criteria:structuredClone(v.criteria),verdicts:structuredClone(verdicts),scope:{included:structuredClone(v.verificationScope?.included??verificationReview(v).map(r=>`[${r.criterionId}] ${v.criteria.find(c=>c.id===r.criterionId)!.text}（${r.methods.join("/")}）`)),excluded:[...new Set(excluded)]},
    artifacts,runs:structuredClone(runs),plan:structuredClone(v.verificationPlan??null),money:structuredClone((data.collections.budgets?.[task.id] as {money?:unknown}|undefined)?.money??null),
    missingRuntimeEvidence:missingVerification(v,verdicts,runs,round.roundId,round.checkpoint?.stepEpoch??0,hashes),historical,
  }} satisfies DeliveryEvidence;
}

/** 历史交付仅导出当时证据；不修改标准、不重新结算，也不把旧静态pass升级为实测。 */
export async function queueHistoricalEvidence(store:Store):Promise<void>{
  await store.transact(data=>{
    for(const t of Object.values(data.collections.tasks??{}) as Task[]){
      if(t.status!=="delivered")continue;
      const v=t.versions[t.currentVersion]!,event=data.eventOrder.map(id=>data.events[id] as {type:string;taskId:string;taskVersion:number;payload:{roundId:string;verdicts:Verdict[];artifactHashes:Record<string,string>}}).findLast(e=>e.type==="task.delivered"&&e.taskId===t.id&&e.taskVersion===v.version);
      const r=event&&data.collections.acceptanceRounds?.[event.payload.roundId] as {roundId:string;checkpoint?:{stepEpoch?:number}}|undefined;
      if(event&&r)queueDeliveryEvidence(data,t,r,event.payload.verdicts,event.payload.artifactHashes,true);
    }
  });
}

export function renderDeliveryEvidence(record:DeliveryEvidence):string {
  const s=record.snapshot;
  return ["# 最终独立验收证据索引",`任务：${s.taskId}；标准版本：v${s.version}；轮次：${s.roundId}`,`记录的交付时间：${s.completedAt}；独立验收人：${s.signer}`,
    s.historical?"本附件从历史交付记录导出，不代表新增实测或重新验证当前文件。":"本附件由宿主根据本轮已结算验收记录生成，与执行者报告分开保存。",
    s.missingRuntimeEvidence.length?`实测证据仍有缺口（历史delivered不作实测已完成解释）：\n${s.missingRuntimeEvidence.map(x=>`- ${x}`).join("\n")}`:"本轮结论已关联所需运行记录；实际覆盖面以下列标准、检查及排除范围为准。",
    "## 验收范围",...s.scope.included.map(x=>`- 包含：${x}`),...s.scope.excluded.map(x=>`- 未覆盖/排除：${x}`),
    "## 逐条签署记录",...s.verdicts.map(v=>`- [${v.criterionId}] ${v.outcome}；运行记录：${v.verificationRunIds?.join("、")||"静态核对"}\n  证据：${v.evidence}`),
    "## 冻结产物",...s.artifacts.map(a=>`- ${a.location}\n  SHA-256：${a.sha256}`),
    "## 实测记录",...s.runs.map(r=>`- ${r.checkId}：${r.status} / exitCode=${r.exitCode}；ID=${r.id}\n  日志：${r.logPath}\n  输出摘要哈希：${r.outputHash}；输入摘要：${r.inputHash}`),
    "## 复现与边界","同目录 verification-plan.json 保存已确认的固定程序、参数及超时。可运行：node reproduce.mjs --workspace <项目目录>。",
    "复现脚本在新副本运行并检查冻结产物；它不等同于宿主沙箱中的正式独立验收。正式重验请使用原任务verify入口。",
    "原执行者报告及哈希保持不变。计划中未声明的浏览器检查、干净安装或其他环境，不由HTTP资源200或代码阅读替代。",""] .join("\n\n");
}

const REPRODUCE=String.raw`import fs from 'node:fs';import os from 'node:os';import path from 'node:path';import crypto from 'node:crypto';import {spawnSync} from 'node:child_process';
const doc=JSON.parse(fs.readFileSync(new URL('./verification-plan.json',import.meta.url),'utf8'));
if(!doc.plan){console.log('本轮仅有静态核对，没有可运行检查。');process.exit(0)}
const at=process.argv.indexOf('--workspace'),root=fs.realpathSync(at>=0?process.argv[at+1]:doc.plan.workspace);
const within=(r,p)=>{const q=path.relative(r,p);return !q||(!path.isAbsolute(q)&&q!=='..'&&!q.startsWith('..'+path.sep))};
const quote=s=>"'"+s.replaceAll("'","''")+"'";
let failed=false;
for(const check of doc.plan.checks){
 const temp=fs.mkdtempSync(path.join(os.tmpdir(),'longtask-replay-')),copy=path.join(temp,'project');
 try{
  fs.cpSync(root,copy,{recursive:true,dereference:true,filter:p=>{const rel=path.relative(root,p);if(rel.split(path.sep).some(n=>n==='.git'||n==='.longtask'||/^\.env(?:\.|$)/.test(n)))return false;if(!within(root,fs.realpathSync(p)))throw Error('拒绝项目外链接：'+rel);return true}});
  for(const a of doc.artifacts){const rel=path.relative(doc.plan.workspace,a.location);if(!within(doc.plan.workspace,a.location))throw Error('产物不在验证项目内');const bytes=fs.readFileSync(path.join(copy,rel));if(crypto.createHash('sha256').update(bytes).digest('hex')!==a.sha256)throw Error('冻结产物已变化：'+rel)}
  const command="$ErrorActionPreference='Stop'\n$exe=(Get-Command -Name "+quote(check.program)+" -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source\n& $exe "+check.args.map(quote).join(' ')+"\nexit $LASTEXITCODE";
  const program=process.platform==='win32'?'pwsh':check.program,args=process.platform==='win32'?['-NoLogo','-NoProfile','-NonInteractive','-Command',command]:check.args;
  const r=spawnSync(program,args,{cwd:copy,timeout:check.timeoutMs,encoding:'utf8',windowsHide:true,maxBuffer:16*1024*1024});process.stdout.write(r.stdout||'');process.stderr.write(r.stderr||'');console.log(check.id+' exitCode='+r.status);if(r.error||r.status!==0)failed=true;
 }catch(e){console.error(check.id+': '+String(e));failed=true}
 finally{if(fs.existsSync(temp)&&fs.realpathSync(temp)===path.resolve(temp)&&within(fs.realpathSync(os.tmpdir()),path.resolve(temp)))fs.rmSync(temp,{recursive:true,force:true})}
}
process.exitCode=failed?1:0;
`;

export async function exportDeliveryEvidence(record:DeliveryEvidence,root:string):Promise<NonNullable<DeliveryEvidence["paths"]>>{
  for(const id of [record.taskId,record.roundId])if(!/^[A-Za-z0-9_-]+$/.test(id))throw Error("验收索引路径标识无效");
  if(!Number.isSafeInteger(record.version)||record.version<1)throw Error("验收索引版本无效");
  const dir=join(root,record.taskId,`v${record.version}`,record.roundId);await fs.mkdir(dir,{recursive:true});
  const paths={report:join(dir,"FINAL-ACCEPTANCE.md"),json:join(dir,"evidence.json"),plan:join(dir,"verification-plan.json"),reproduce:join(dir,"reproduce.mjs")};
  const write=async(file:string,content:string)=>{
    try{const old=await fs.readFile(file,"utf8");if(old!==content)throw Error(`验收附件已存在且内容不同，保留原文件：${file}`);return;}catch(e){if((e as NodeJS.ErrnoException).code!=="ENOENT")throw e;}
    const temp=`${file}.${randomUUID()}.tmp`;try{await fs.writeFile(temp,content,{flag:"wx"});await fs.rename(temp,file);}finally{await fs.rm(temp,{force:true});}
  };
  await write(paths.json,JSON.stringify(record.snapshot,null,2)+"\n");
  await write(paths.plan,JSON.stringify({plan:record.snapshot.plan,artifacts:record.snapshot.artifacts},null,2)+"\n");
  await write(paths.reproduce,REPRODUCE);
  await write(paths.report,renderDeliveryEvidence(record));return paths;
}

/** 事务内outbox + 可重试导出，文件失败不丢签署快照，也不伪造运行结果。 */
export class DeliveryEvidenceExporter {
  private closing=false;private action:Promise<void>=Promise.resolve();private stop:(()=>void)|undefined;
  constructor(private store:Store,private root:string,private warn:(message:string)=>void){this.stop=store.subscribe?.(()=>{void this.flush()});}
  flush():Promise<void>{
    this.action=this.action.then(async()=>{
      if(this.closing)return;
      for(const record of this.store.list<DeliveryEvidence>(DELIVERY_EVIDENCE).filter(r=>r.exportState==="pending")){
        try{const paths=await exportDeliveryEvidence(record,this.root);await this.store.transact(data=>{const r=data.collections[DELIVERY_EVIDENCE]?.[record.id] as DeliveryEvidence|undefined;if(r){r.paths=paths;r.exportState="exported";delete r.lastError;}});}
        catch(error){this.warn(`最终验收附件导出失败：${String(error)}`);await this.store.transact(data=>{const r=data.collections[DELIVERY_EVIDENCE]?.[record.id] as DeliveryEvidence|undefined;if(r){r.exportState="error";r.lastError=String(error);}});}
      }
    }).catch(error=>this.warn(String(error)));return this.action;
  }
  async retry(taskId?:string):Promise<void>{await this.store.transact(data=>{for(const r of Object.values(data.collections[DELIVERY_EVIDENCE]??{}) as DeliveryEvidence[])if(r.exportState==="error"&&(!taskId||r.taskId===taskId))r.exportState="pending";});await this.flush();}
  async dispose():Promise<void>{this.stop?.();await this.flush();this.closing=true;await this.action;}
}
