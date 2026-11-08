import {createHash, randomUUID} from "node:crypto";
import {promises as fs} from "node:fs";
import {dirname, isAbsolute, join, relative, resolve, sep} from "node:path";
import {tmpdir} from "node:os";
import type {Criterion, TaskVersion, VerificationPlan, Verdict} from "./types.js";
import type {Store} from "./store/store.js";
import type {HostContext} from "./host-types.js";
import type {TaskService} from "./tasks/task-service.js";

export const VERIFICATION_TOOL = "longtask_verify";
export const VERIFICATION_RUNS = "verificationRuns";
export type VerificationKind = "review" | "command" | "browser";
export const digest = (value: unknown): string => createHash("sha256").update(JSON.stringify(value)).digest("hex");

// 识别执行行为，而不是仅凭技术名词要求实测；文档/引用语境先由behaviorClauses剔除。
const commandRunner = String.raw`(?:pytest\b|python(?:\d(?:\.\d+)?)?\b|uv\s+run\b|poetry\s+run\b|cargo\s+(?:test|run|build)\b|go\s+(?:test|run|build)\b|dotnet\s+test\b|mvn\s+test\b|gradle(?:w)?\s+test\b|(?:npm|pnpm|yarn)\s+(?:test|start|run)\b|(?:npx\s+)?(?:vitest|jest|mocha|playwright|cypress)\b)`;
const runCommand = new RegExp(String.raw`(?:执行|运行|跑通|启动)\s*${commandRunner}|\b(?:run|execute|invoke)\s+(?:the\s+)?${commandRunner}`,"i");
const separateCommandAction = new RegExp(String.raw`(?:^|并|且|同时|另需|还需|还要|再)(?:需|须|要|应|必须)?(?:实际|亲自)?(?:执行|运行)\s*(?=${commandRunner}|__verification_quote_)`,"i");
const imperativeCommand = /^(?:(?:请|需|须|应|必须|需要|并|且|同时)\s*)*(?:执行|运行|启动|安装|编译|构建|跑通)|^(?:(?:please|then|also|and|must|should)\s+)*(?:run|execute|invoke|start|launch|install|build|compile)\b/i;
const omittedTests = /^(?:no\s+(?:(?:browser|command|runtime|integration|unit|e2e)\s+)?tests?\s+(?:(?:are|is)\s+)?(?:required|needed|necessary)|(?:(?:browser|command|runtime|integration|unit|e2e)\s+)?tests?\s+(?:are|is)\s+not\s+(?:required|needed|necessary)|(?:this|the)\s+(?:task|round|check)\s+(?:does\s+not|doesn't)\s+require\s+(?:(?:browser|command|runtime|integration|unit|e2e)\s+)?tests?)\b/i;

/** 内容说明与行为验收按分句区分；文档词不能豁免后续独立的实际执行要求。 */
function behaviorClauses(text:string):string[] {
  const quotations:string[]=[];
  const visible=text.replace(/```[\s\S]*?```/g,"")
    .replace(/“[^”]*”|‘[^’]*’|《[^》]*》|"[^"\n]*"|`[^`\n]*`|(?<!\w)'[^'\n]*'(?!\w)/g,value=>`__verification_quote_${quotations.push(value)-1}__`)
    .replace(/[A-Za-z]:[\\/][^\s，；。]+/g,"").replace(/https?:\/\/\S+/g,"");
  const clauses:string[]=[];let documenting=false;
  const document="(?:文档|README|报告|手册|教程|指南|说明书|文章|规范|章节|documentation|manual|guide|report)";
  const explains="(?:包含|覆盖|说明|介绍|描述|记录|列出|给出|提供|解释|阐述|讲解|明确|写明|写清|展示|注明|总结|梳理|引用|收录|标题为|documents?|describes?|explains?|includes?|covers?)";
  const subject=new RegExp(document+".{0,24}"+explains,"i");
  const writing=new RegExp("(?:编写|撰写|补充|整理|输出|生成|交付|write|draft).{0,24}"+document,"i");
  for(let clause of visible.split(/[。；;\n，,]|[.!?]\s+(?=[A-Za-z])/).map(s=>s.trim()).filter(Boolean)){
    clause=clause.replace(/^(?:[-*]\s+|\d+[.)]\s+)/,"");
    clause=clause.replace(omittedTests,"").replace(/^\s*(?:and|but|then)\s+/i,"").trim();
    if(/^(?:请)?(?:本任务|本轮|验收)?(?:不要求|不需要|不必|不用|无需|无须|不要|禁止)(?:实际)?(?:执行|运行|安装|启动|打开|测试|实测)/.test(clause))continue;
    if(/^(?:please\s+)?(?:(?:this|the)\s+(?:task|round|check)\s+)?(?:do\s+not|don't|must\s+not|never|no\s+need\s+to|does\s+not\s+(?:need|require)(?:\s+to)?)\s+(?:actually\s+)?(?:run|execut|open|launch|start|test|install)/i.test(clause))continue;
    // “说明如何实际运行”仍属内容；“并实际运行”是另一个必须执行的动作。
    const action=/(?:^|并|且|同时|另需|还需|还要|再)(?:需|须|要|应|必须)?(?:实际|亲自)(?:执行|运行|安装|启动|打开|操作|测试|验证)|(?:^|\band\b|\bthen\b|\balso\b)\s*(?:actually|physically)\s+(?:run|execute|start|launch|open|test)/i.exec(clause) ?? separateCommandAction.exec(clause);
    const byGuide=/(?:按(?:照)?|依照|根据).{0,24}(?:文档|README|手册|指南).*(?:执行|运行|安装|启动|跑通|浏览器|登录|操作|点击)/i.test(clause);
    if(action){clause=clause.slice(action.index);documenting=false;}
    else if(!byGuide&&(subject.test(clause)||writing.test(clause)||/^(?:并|且|同时)?(?:说明|介绍|描述|记录|列出|解释|写明|写清|注明|引用|讲解|document\b)/i.test(clause)
      ||documenting&&/^(?:(?:并|且|同时)?(?:以及|及|包含|覆盖|给出|提供))|^(?:安装步骤|测试命令|操作步骤|浏览器兼容性|示例)[:：]/.test(clause))){documenting=true;continue;}
    else documenting=false;
    clauses.push(clause.replace(/__verification_quote_(\d+)__/g,(_all,index:string)=>quotations[Number(index)]??""));
  }
  return clauses;
}
export function verificationMethods(c: Criterion): VerificationKind[] {
  const methods = new Set<VerificationKind>(c.verification ?? []);
  let command=false,browser=false;
  for(const text of behaviorClauses(c.text??"")){
    const browserAction=/(?:打开|使用|通过|实测|验证|测试|检查).*浏览器|(?:在|浏览器)(?:中|内|上)?.*(?:登录|操作|点击|测试|验证)|控制台.*(?:无|没有|不.*报错)|页面.*(?:可用|交互|点击)|刷新.*(?:会话|登录)|端到端|e2e\s*(?:测试|验证|检查)|playwright\s*(?:测试|验证|检查)|\b(?:use|open|launch|test|verify|check)(?:\s+\w+){0,3}\s+(?:web\s+)?browser\b|\b(?:in|with|through)\s+(?:a\s+|the\s+)?browser\b.*\b(?:log\s*in|login|click|refresh|session|interact)|\b(?:browser|playwright|cypress|e2e|end.to.end)\s+tests?\b/i.test(text);
    browser ||= browserAction;
    command ||= /(?:npm|pnpm|yarn)\s+(?:test|start|run)|运行(?:测试|结果|成功)|测试(?:全部|全数|均|用例|通过)|接口(?:集成)?测试|启动后|启动.*(?:访问|返回|成功)|HTTP\s*\d{3}|(?:实际|亲自)(?:执行|运行|安装|启动)|(?:按(?:照)?|依照|根据).{0,24}(?:文档|README|手册|指南).*(?:跑通|安装|运行|执行|启动)/i.test(text)
      ||runCommand.test(text)||/(?:所有|全部|全数).*测试.*(?:必须|应|需|均|全部)?通过|\b(?:all\s+)?tests?\s+(?:(?:must|should|shall)\s+)?pass\b/i.test(text)
      ||!browserAction&&(/实测/.test(text)||imperativeCommand.test(text));
  }
  if(command)methods.add("command");
  if(browser)methods.add("browser");
  if (!methods.size) methods.add("review");
  return [...methods];
}
export function verificationReview(v: Pick<TaskVersion, "criteria" | "verificationPlan">) {
  return v.criteria.map(c => ({criterionId:c.id, methods:verificationMethods(c),
    checks:(v.verificationPlan?.checks ?? []).filter(k=>k.criterionIds.includes(c.id)).map(k=>({id:k.id,kind:k.kind,purpose:k.purpose}))}));
}
export function validateVerificationPlan(v: Pick<TaskVersion, "criteria" | "verificationPlan"> & {verificationPolicyVersion?:1}): string[] {
  const errors: string[] = [], plan=v.verificationPlan, ids=new Set<string>(), criteria=new Set(v.criteria.map(c=>c.id));
  for(const c of v.criteria) if(c.verification && (!Array.isArray(c.verification)||!c.verification.length||c.verification.some(m=>!["review","command","browser"].includes(m)))) errors.push(`[${c.id}] verification无效`);
  if(v.verificationPolicyVersion)for(const c of v.criteria)if(!c.verification?.length)errors.push(`[${c.id}] 新方案须明确verification验证方式：review/command/browser，不能省略后按静态核对交付`);
  if(plan){
    if(!isAbsolute(plan.workspace)||!plan.workspace.trim()) errors.push("verificationPlan.workspace须为项目绝对目录");
    if(!Array.isArray(plan.checks)||!plan.checks.length||plan.checks.length>64) errors.push("verificationPlan.checks须含1..64项");
    for(const c of plan.checks ?? []){
      if(!/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(c.id)||ids.has(c.id))errors.push("验证检查id非法或重复");ids.add(c.id);
      if(!["command","browser"].includes(c.kind))errors.push(`${c.id}：检查类型无效`);
      if(!c.criterionIds?.length||c.criterionIds.some(id=>!criteria.has(id)))errors.push(`${c.id}：须关联有效criterionIds`);
      if(typeof c.purpose!=="string"||!c.purpose.trim())errors.push(`${c.id}：须说明验证行为purpose`);
      if(typeof c.program!=="string"||!c.program.trim()||c.program.length>2048||/[\r\n\0]/.test(c.program)||c.program.startsWith("-"))errors.push(`${c.id}：program无效`);
      if(!Array.isArray(c.args)||c.args.length>128||c.args.some(a=>typeof a!=="string"||a.length>8192||a.includes("\0")))errors.push(`${c.id}：args无效`);
      if(!Number.isSafeInteger(c.timeoutMs)||c.timeoutMs<1000||c.timeoutMs>300000)errors.push(`${c.id}：timeoutMs须为1000..300000`);
    }
  }
  for(const row of verificationReview(v))for(const kind of row.methods.filter(k=>k!=="review")) if(!row.checks.some(c=>c.kind===kind))errors.push(`[${row.criterionId}] 需要${kind}实测，请提供对应verificationPlan检查；不能只读代码后通过`);
  return errors;
}

export interface VerificationRun {
  id:string; taskId:string; version:number; roundId:string; stepEpoch:number; sessionId:string;
  checkId:string; kind:"command"|"browser"; criterionIds:string[]; planHash:string; artifactHashes:Record<string,string>;
  inputHash:string; status:"passed"|"failed"|"capability"|"cancelled"; exitCode:number|null;
  startedAt:string; finishedAt:string; logPath:string; outputHash:string; summary:string;
}

/** 文字证据、执行者自报、其他轮次或旧产物的实测均不能替代本轮独立记录。 */
export function missingVerification(v:TaskVersion, verdicts:Verdict[], runs:VerificationRun[], roundId:string, epoch:number, hashes:Record<string,string>):string[] {
  const missing:string[]=[];
  for(const verdict of verdicts.filter(v=>v.outcome==="pass")){
    const c=v.criteria.find(c=>c.id===verdict.criterionId);if(!c)continue;
    for(const kind of verificationMethods(c).filter(k=>k!=="review")){
      const checks=v.verificationPlan?.checks.filter(k=>k.kind===kind&&k.criterionIds.includes(c.id)) ?? [];
      if(!checks.length){missing.push(`[${c.id}] 缺少${kind}实测计划及真实运行证据`);continue;}
      for(const check of checks){
        const ok=runs.some(r=>verdict.verificationRunIds?.includes(r.id)&&r.checkId===check.id&&r.kind===kind&&r.status==="passed"&&r.exitCode===0&&r.version===v.version&&r.roundId===roundId&&r.stepEpoch===epoch&&r.planHash===digest(v.verificationPlan)&&digest(r.artifactHashes)===digest(hashes)&&r.criterionIds.includes(c.id));
        if(!ok)missing.push(`[${c.id}] 检查${check.id}缺少本轮当前产物版本的成功运行记录`);
      }
    }
  }
  return missing;
}

/** 除冻结交付物外，运行时实际读到的测试脚本、依赖和配置也必须仍是同一份输入。 */
export async function verifyRunInputs(run:VerificationRun,plan:VerificationPlan):Promise<boolean>{
  try{
    const output=await fs.readFile(run.logPath,"utf8");if(digest(output)!==run.outputHash)return false;
    const log=JSON.parse(output) as {workspace:string;inputManifest:Array<[string,string]>};
    const root=await fs.realpath(plan.workspace);if(log.workspace!==root||digest(log.inputManifest)!==run.inputHash)return false;
    const current:Array<[string,string]>=[];
    const walk=async(path:string):Promise<void>=>{
      const rel=relative(root,path);if(rel.split(sep).some(p=>p===".git"||p===".longtask"||/^\.env(?:\.|$)/.test(p)))return;
      if(!within(root,await fs.realpath(path)))throw Error("项目外链接");
      const stat=await fs.stat(path);if(stat.isDirectory()){for(const name of await fs.readdir(path))await walk(join(path,name));}
      else if(stat.isFile())current.push([rel,createHash("sha256").update(await fs.readFile(path)).digest("hex")]);
    };
    await walk(root);return digest(current.sort(([a],[b])=>a.localeCompare(b)))===run.inputHash;
  }catch{return false;}
}

interface ShellResult {exitCode:number|null; timedOut?:boolean; aborted?:boolean; signal?:string|null; stdout:{text:string}; stderr:{text:string}; sandbox?:{mode:string;enforcement?:string;denied?:boolean;runnerFailed?:boolean}}
interface Shell {sandboxMode?:string; resolve(request:Record<string,unknown>):Record<string,unknown>; execute(spec:Record<string,unknown>):Promise<{result():Promise<ShellResult>}>}
export function verificationCapabilityIssue(ctx:HostContext):string|undefined {
  const shell=ctx.get<Shell>("shell");
  return !shell?.sandboxMode||typeof shell.resolve!=="function"||typeof shell.execute!=="function" ? "受控实测需要宿主具备沙箱shell服务；不会降级为无沙箱执行或静态通过" : undefined;
}
const within=(root:string,path:string)=>{const rel=relative(root,path);return rel===""||(!rel.startsWith(`..${sep}`)&&rel!==".."&&!isAbsolute(rel));};
async function physicalPath(path:string):Promise<string>{
  const target=resolve(path);let parent=target;
  for(;;){try{return resolve(await fs.realpath(parent),relative(parent,target));}catch(error){if((error as NodeJS.ErrnoException).code!=="ENOENT")throw error;const next=dirname(parent);if(next===parent)throw error;parent=next;}}
}
const quote=(s:string)=>process.platform==="win32"?`'${s.replaceAll("'","''")}'`:`'${s.replaceAll("'",`'"'"'`)}'`;
export function verificationCommand(check:VerificationPlan["checks"][number]):string {
  const argv=[check.program,...check.args].map(quote).join(" ");
  return process.platform==="win32"?`$ErrorActionPreference = 'Stop'\n$verificationExe = (Get-Command -Name ${quote(check.program)} -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source\n& $verificationExe ${check.args.map(quote).join(" ")}\nexit $LASTEXITCODE`:`exec ${argv}`;
}

export function registerVerificationTool(o:{ctx:HostContext;store:Store;tasks:TaskService;workspace:string;dataDir:string;blocked(taskId:string):boolean}):{dispose():Promise<void>} {
  const tools=o.ctx.get<{register(def:unknown):()=>void}>("tools"),closing=new AbortController(),pending=new Map<string,Promise<string>>();
  let disposed=false;
  const stop=tools?.register?.({name:VERIFICATION_TOOL,description:"独立验收专用实测：只传已确认verificationPlan的checkId。在项目临时副本及宿主写入沙箱内执行固定program/args；返回真实运行记录ID与输出。源码只读，不能临时改命令、安装到原目录或自报测试通过。browser检查须由计划中的浏览器自动化脚本实际执行。",
    parameters:{type:"object",additionalProperties:false,properties:{checkId:{type:"string",pattern:"^[A-Za-z][A-Za-z0-9_-]{0,63}$"}},required:["checkId"]},
    output:{schema:{type:"string"},render:(_a:unknown,value:string)=>[{type:"text",text:value}]},
    execute:async(args:unknown,exec:{agent?:{session?:{id?:string}};signal:AbortSignal})=>{
      const a=args as Record<string,unknown>;if(!a||Object.keys(a).some(k=>k!=="checkId")||typeof a.checkId!=="string")throw Error("只接受已确认检查的checkId");
      const sid=exec.agent?.session?.id,lease=sid&&o.tasks.findSessionBySessionId(sid);if(!lease||lease.role!=="acceptor"||lease.revokedAt)throw Error("仅当前独立验收会话可实测");
      const task=o.tasks.getTask(lease.taskId),round=o.tasks.latestPendingRound(lease.taskId);if(!task||task.status!=="awaiting_acceptance"||!round||o.blocked(task.id)||disposed)throw Error("当前任务不允许实测");
      const v=o.tasks.currentVersion(task),plan=v.verificationPlan,check=plan?.checks.find(c=>c.id===a.checkId);if(!plan||!check)throw Error("检查不在已确认verificationPlan中");
      const issue=verificationCapabilityIssue(o.ctx);if(issue)throw Error(issue);
      const epoch=round.checkpoint?.stepEpoch??0,key=`${round.roundId}:${epoch}:${check.id}`,signal=AbortSignal.any([exec.signal,closing.signal]);
      const live=()=>{signal.throwIfAborted();const t=o.tasks.getTask(task.id),r=o.tasks.latestPendingRound(task.id);if(disposed||t?.status!=="awaiting_acceptance"||o.blocked(task.id)||o.tasks.findSessionBySessionId(sid!)?.revokedAt||r?.roundId!==round.roundId||(r.checkpoint?.stepEpoch??0)!==epoch||digest(o.tasks.currentVersion(t).verificationPlan)!==digest(plan))throw Error("验收权限、轮次或计划已变化，实测记录不能用于交付");};
      const prior=o.store.list<VerificationRun>(VERIFICATION_RUNS).find(r=>r.taskId===task.id&&r.roundId===round.roundId&&r.stepEpoch===epoch&&r.checkId===check.id&&r.planHash===digest(plan)&&r.sessionId===sid);
      if(prior){live();return JSON.stringify(prior);}
      if(pending.has(key))return pending.get(key)!;
      const run=(async()=>{
        live();const errors=validateVerificationPlan(v);if(errors.length)throw Error(errors.join("；"));
        const workspace=await fs.realpath(o.workspace),source=await fs.realpath(plan.workspace);if(!within(workspace,source))throw Error("验证项目目录须位于任务工作目录内");
        const dataDir=await physicalPath(o.dataDir);if(within(source,dataDir)||within(dataDir,source))throw Error("插件数据目录与验证项目目录重叠；请先将数据目录迁移到项目之外，未执行检查");
        // 宿主workspace-write还授予平台临时目录写入权，原项目位于该目录时不能声称源码受保护。
        if(process.platform!=="win32"&&(within(await fs.realpath(tmpdir()),source)||within("/tmp",source)))throw Error("原项目位于宿主沙箱默认可写临时目录，无法保证源码只读；请使用临时目录之外的项目路径");
        const scratch=await fs.mkdtemp(join(tmpdir(),"dsh-verify-"));let snapshot=join(scratch,"project");
        // 不跟随指向项目外部的链接；副本不能让沙箱写入回到原文件或依赖目录。
        try {
        const manifest:Array<[string,string]>=[];
        await fs.cp(source,snapshot,{recursive:true,dereference:true,filter:async(path)=>{
          signal.throwIfAborted();const rel=relative(source,path);if(rel.split(sep).some(p=>p===".git"||p===".longtask"||/^\.env(?:\.|$)/.test(p)))return false;
          const physical=await fs.realpath(path);if(!within(source,physical))throw Error(`实测副本不允许项目外链接：${rel}`);
          if((await fs.stat(path)).isFile())manifest.push([rel,createHash("sha256").update(await fs.readFile(path)).digest("hex")]);return true;
        }});
        snapshot=await fs.realpath(snapshot);live();
        for(const [id,hash] of Object.entries(round.artifactHashes)){
          const art=o.store.get<{location:string}>("artifacts",id);if(!art||!within(source,resolve(art.location)))throw Error("冻结交付物必须全部位于验证项目目录内");
          const bytes=await fs.readFile(join(snapshot,relative(source,resolve(art.location))));if(createHash("sha256").update(bytes).digest("hex")!==hash)throw Error("实测副本与本轮冻结产物不一致");
        }
        const id=`verify_${randomUUID()}`,startedAt=new Date().toISOString(),shell=o.ctx.get<Shell>("shell")!;
        const spec=shell.resolve({command:verificationCommand(check),workdir:snapshot,timeoutMs:check.timeoutMs,onExpiry:"kill",signal,
          sandboxPolicy:{mode:"workspace-write",workspaceRoot:snapshot,additionalWritableRoots:[]},dshEnv:{}});
        const policy=spec.sandboxPolicy as {mode?:string;workspaceRoot?:string};if(policy?.mode!=="workspace-write"||policy.workspaceRoot!==snapshot)throw Error("宿主未保留验证副本沙箱策略，未执行检查");
        let result:ShellResult;
        try{result=await(await shell.execute(spec)).result();}catch(e){signal.throwIfAborted();result={exitCode:null,stdout:{text:""},stderr:{text:String(e)}};}
        live();const confined=result.sandbox?.mode==="workspace-write"&&Boolean(result.sandbox.enforcement)&&!result.sandbox.denied&&!result.sandbox.runnerFailed;
        let frozenUnchanged=true;
        for(const [artifactId,hash] of Object.entries(round.artifactHashes)){
          const art=o.store.get<{location:string}>("artifacts",artifactId);
          try{if(!art||createHash("sha256").update(await fs.readFile(join(snapshot,relative(source,resolve(art.location))))).digest("hex")!==hash)frozenUnchanged=false;}catch{frozenUnchanged=false;}
        }
        manifest.sort(([a],[b])=>a.localeCompare(b));
        const output=JSON.stringify({workspace:source,inputManifest:manifest,check,stdout:result.stdout.text,stderr:result.stderr.text,exitCode:result.exitCode,timedOut:result.timedOut??false,sandbox:result.sandbox??null});
        const logDir=join(o.dataDir,"verification-reports");await fs.mkdir(logDir,{recursive:true});const logPath=join(logDir,`${id}.json`);await fs.writeFile(logPath,output,{flag:"wx"});
        const receipt:VerificationRun={id,taskId:task.id,version:v.version,roundId:round.roundId,stepEpoch:epoch,sessionId:sid!,checkId:check.id,kind:check.kind,criterionIds:[...check.criterionIds],planHash:digest(plan),artifactHashes:{...round.artifactHashes},
          inputHash:digest(manifest.sort(([a],[b])=>a.localeCompare(b))),status:!confined?"capability":result.aborted?"cancelled":result.exitCode===0&&!result.timedOut&&!result.signal&&frozenUnchanged?"passed":"failed",exitCode:result.exitCode,
          startedAt,finishedAt:new Date().toISOString(),logPath,outputHash:digest(output),summary:((frozenUnchanged?"":"验证脚本修改了冻结产物，不能作为通过证据。\n")+result.stdout.text+"\n"+result.stderr.text).slice(-16000)};
        await o.store.transact(data=>{live();(data.collections[VERIFICATION_RUNS]??={})[id]=receipt;});return JSON.stringify(receipt);
        } finally {
          // 只清理本工具mkdtemp创建且仍为真实目录的临时根，绝不追随替换的链接。
          try{if((await fs.realpath(scratch))===resolve(scratch)&&within(await fs.realpath(tmpdir()),resolve(scratch)))await fs.rm(scratch,{recursive:true,force:true});}catch(e){o.ctx.logger.warn(`验证副本清理失败：${String(e)}`);}
        }
      })().finally(()=>pending.delete(key));pending.set(key,run);return run;
    }});
  return {async dispose(){disposed=true;closing.abort();stop?.();await Promise.allSettled([...pending.values()]);}};
}
