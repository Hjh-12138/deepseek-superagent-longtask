import {prepareSdkTools, loadSdk} from "./sdk-tools.js";
import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync} from "node:fs";
import {tmpdir} from "node:os";import {join} from "node:path";import {pathToFileURL} from "node:url";
import {Context} from "@deepseek-ai/cordis";
import {Session} from "@deepseek-ai/dsh-session";
import {apply, type LongtaskConfig} from "../src/index.js";
import {USER_TASK_TOOLS} from "../src/user-entry.js";
import {JsonStore} from "../src/store/store.js";
import {TokenLedger} from "../src/ledger/token-ledger.js";
import {TaskService} from "../src/tasks/task-service.js";
import {DEFAULT_PRICES} from "../src/ledger/money.js";
const {ToolRuntime}=await loadSdk("dsh-tools");
// 与ToolRuntime使用同一宿主scope实例；两份SDK的WeakMap作用域不可混用。
const {createScope}=await loadSdk("dsh-scope");
const sleep=(ms:number)=>new Promise(r=>setTimeout(r,ms));
async function until(fn:()=>boolean){for(let n=0;n<300;n++){if(fn())return;await sleep(10)}throw Error("入口验证超时")}

async function fixture(overrides: Partial<LongtaskConfig> = {}){
  const directory=mkdtempSync(join(tmpdir(),"longtask-user-entry-")),experts=join(directory,"experts"),root=new Context();
  mkdirSync(join(experts,"research"),{recursive:true});writeFileSync(join(experts,"divisions.json"),JSON.stringify({divisions:{research:{label:"Research"}}}));
  for(const name of ["worker","reviewer","second"])writeFileSync(join(experts,"research",name+".md"),`---\nname: ${name}\ndescription: 专家 ${name}\n---\n# 正文不得进入目录结果\n`);
  root.provide("systemPrompt",{tools(){}});const tools=await prepareSdkTools(root,new ToolRuntime(root)),agents=new Map<string,any>(),scopes:any[]=[],created:any[]=[],briefs:any[]=[],approvals:any[]=[],teardowns:any[]=[],logs:string[]=[];
  let stops=0,approvalResult="allowed-once",approvalOverride:any,userAmount="5";
  function makeAgent(id:string,parent?:any){const log=Session.create(id as any);const a:any={id,session:{id,header:parent?{parentSession:parent.session.id}:{},append:log.append.bind(log),snapshotEvents:log.snapshotEvents.bind(log)},status:"idle",followup:(message:any)=>briefs.push(message),steer:(message:any)=>briefs.push(message),cancel:()=>{stops++},whenIdle:async()=>{}};
    const scope=createScope(root,a,parent?{parent}:undefined);scopes.push(scope);a.ctx=scope.ctx;agents.set(id,a);return a;}
  const user=makeAgent("session-user");
  const services=new Map<string,any>([["tools",tools],["sessions",{flush:async()=>true}],["agents",{get:(id:string)=>agents.get(id),list:()=>[...agents.values()],async create(options:any){const a=makeAgent(options.sessionId);a.parentAgent=options.parentAgent;await options.setup?.(a.ctx,a);created.push(a);return{agent:a,dispose:async()=>{agents.delete(a.id)}}}}],
    ["approval",{async request(input:any){approvals.push(input);return approvalOverride?approvalOverride(input):approvalResult}}],
    ["userQuestions",{async ask(input:any){const a={...input,reason:input.questions[0].detail};approvals.push(a);const outcome=approvalOverride?await approvalOverride(a):approvalResult;
      return {answers:[{id:"task_budget_cny",selected:[],...(outcome==="allowed-once"?{custom:userAmount}:{})}]};}}]]);
  const ctx:any={get:(name:string)=>services.get(name),provide:(name:string,value:any)=>{services.set(name,value);root.provide(name,value)},on:(name:string,fn:any)=>(root as any).on(name,fn),
    logger:{info:(m:string)=>logs.push(m),warn:(m:string)=>logs.push(m),error:(m:string)=>logs.push(m)},effect:(factory:any)=>{const it=factory(),first=it.next();if(typeof first.value==='function')teardowns.push(first.value)}};
  apply(ctx,{dataDir:directory,agencyAgentsRoot:experts,workerCwd:directory,workerParentAgent:"none",workerPreset:"",acceptorPreset:"",workerProvider:"deepseek-official",workerModel:"deepseek-flash",modelPrices:DEFAULT_PRICES.map(p=>({...p,verifiedAt:"2020-01-01",expiresAt:"2099-01-01"})),probeOnStart:false,watchdogIntervalMs:3600000,...overrides});
  await until(()=>services.get("longtask")?.getRuntimeInfo().ready===true);const api=services.get("longtask");let seq=0;
  async function call(name:string,args:any={},agent=user,signal=new AbortController().signal){return tools.execute({name,arguments:args,agent,signal,callId:`user-${++seq}`})}
  async function ok(name:string,args:any={},agent=user){const result=await call(name,args,agent);assert.equal(result.isError,false,JSON.stringify(result));return JSON.parse(result.value)}
  const proposal=(patch:any={})=>({goal:"比较存储方案",criteria:[{id:"c1",text:"产物可读"}],deliverables:[join(directory,"result.md")],executorRelPaths:["research/worker.md"],acceptorRelPath:"research/reviewer.md",suggestedBudgetCny:"5",...patch});
  return {directory,experts,root,tools,services,api,user,created,briefs,approvals,logs,makeAgent,call,ok,proposal,setAmount:(v:string)=>{userAmount=v},setApproval:(v:string)=>{approvalResult=v},setApprovalHandler:(v:any)=>{approvalOverride=v},get stopCalls(){return stops},
    async close(){for(const stop of teardowns.splice(0))await stop();for(const scope of scopes)await scope.dispose();await root.fiber.dispose()}};
}

test("金额由用户填写：模型建议不生效，工具参数不能代填总额，完整方案只确认一次",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal({suggestedBudgetCny:'999'})),id=p.task.taskId;
    assert.equal(p.task.budget.limitAmount,null);assert.equal(f.api.getBudget(id),undefined);
    assert.equal((await f.call('longtask_confirm',{taskId:id,version:1,budgetCny:'999'})).isError,true);
    f.setAmount('2.50');const started=await f.ok('longtask_confirm',{taskId:id,version:1});
    assert.equal(started.task.budget.limitAmount,'2.5');assert.equal(f.approvals.length,1);assert.equal(f.approvals[0].questions[0].options,undefined);
    assert.match(f.approvals[0].questions[0].detail,/产物可读/);assert.match(f.approvals[0].questions[0].detail,/高峰价/);
    assert.equal(f.api.getTask(id).versions[0].budget.money.limitAmount,'2.5');
    assert.equal(f.api.getTaskEvidence(id).eventOrder.map((eid:string)=>f.api.getTaskEvidence(id).events[eid]).find((e:any)=>e.type==='task.confirmed').payload.moneyApproval.sessionId,f.user.session.id);
    await f.ok('longtask_control',{taskId:id,action:'pause'});await f.ok('longtask_control',{taskId:id,action:'resume'});
    assert.equal(f.api.getBudget(id).money.limitNano,'2500000000');assert.equal(f.approvals.length,2);
  }finally{await f.close()}
});

test("金额输入缺失/非法/取消均不开工，不用建议兜底；普通审批自动同意不能代填金额",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;
    for(const value of ['', '0','-1','1e3','5元','1.0000000001']){f.setAmount(value);assert.equal((await f.call('longtask_confirm',{taskId:id,version:1})).isError,true);assert.equal(f.created.length,0);assert.equal(f.api.getBudget(id),undefined);}
    f.services.delete('userQuestions');assert.equal((await f.call('longtask_confirm',{taskId:id,version:1})).isError,true);assert.equal(f.created.length,0);
  }finally{await f.close()}
});

test("真实正式入口绑定调用方Router，多个用户会话不串通知；Worker继续仍不能绕过暂停",async()=>{
  const f=await fixture({workerParentAgent:'auto'});try{
    const owner=f.makeAgent('session-second-user');
    const p=await f.ok('longtask_propose',f.proposal(),owner),id=p.task.taskId;
    assert.equal(p.task.routerSessionId,owner.session.id);
    assert.equal((await f.call('longtask_propose',f.proposal({routerSessionId:f.user.session.id}),owner)).isError,true);
    await f.ok('longtask_confirm',{taskId:id,version:1},owner);const worker=f.created[0];assert.equal(worker.parentAgent,owner);
    await f.api.pause(id,'故障模拟：输出截断');
    const state=f.api.getTaskEvidence(id);assert.equal(state.collections.tasks[id].routerSessionId,owner.session.id);
    await f.ok('longtask_control',{taskId:id,action:'resume'},owner);
    (f.root as any).emit('session/event',worker.session,{type:'turn/end',data:{turn:1,reason:{kind:'blocked'}}});
    await until(()=>owner.session.snapshotEvents().some((e:any)=>e.type==='user/message'&&e.data.source.kind==='longtask-status'));
    assert.equal(f.user.session.snapshotEvents().filter((e:any)=>e.type==='user/message').length,0);
    const notice=owner.session.snapshotEvents().findLast((e:any)=>e.type==='user/message').data;
    assert.match(notice.content[0].text,/主会话/);assert.match(notice.content[0].text,/不要在Worker/);
    assert.equal((await f.call('longtask_control',{taskId:id,action:'resume'},worker)).isError,true);
    const before=f.api.getBudget(id).settledTokens;const decision=await (f.root as any).waterfall('agent/pre-step',{agent:worker,messages:[{role:'user',content:[{type:'text',text:'继续'}]}],signal:new AbortController().signal},async()=>({kind:'enter',messages:[]}));
    assert.equal(decision.kind,'reject');assert.equal(f.api.getBudget(id).settledTokens,before);
  }finally{await f.close()}
});

test("真实截断自动接续回传Router；金额准入暂停也回传，并保持原金额",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;await f.ok('longtask_confirm',{taskId:id,version:1});const worker=f.created[0];
    await (f.root as any).waterfall('agent/pre-step',{agent:worker,messages:[],signal:new AbortController().signal},async()=>({kind:'enter',messages:[]}));
    await (f.root as any).waterfall('agent/request',{agent:worker,turn:1,step:1,signal:new AbortController().signal},async()=>({provider:'deepseek-official',model:'deepseek-flash',maxTokens:8000}));
    (f.root as any).emit('session/event',worker.session,{seq:9100,type:'assistant/message',data:{turn:1,step:1,message:{role:'assistant',content:[{type:'text',text:'输出截断'}]},usage:{inputTokens:1000,outputTokens:8000,cacheReadTokens:0,cacheWriteTokens:0}}});
    (f.root as any).emit('session/event',worker.session,{type:'turn/end',data:{turn:1,reason:{kind:'max-tokens'}}});
    await until(()=>f.user.session.snapshotEvents().some((e:any)=>e.data.source?.summary?.includes('已自动接续')));
    assert.equal(f.api.getTask(id).status,'active');assert.equal(f.api.getMoneyBudget(id).limitAmount,'5');assert.equal(f.api.getMoneyBudget(id).calculatedAmount,'0.066');
    assert.equal(f.created.length,1);assert.ok(f.briefs.some(m=>JSON.stringify(m).includes('截断处继续')));
  }finally{await f.close()}
  const low=await fixture();try{
    low.setAmount('0.1');const p=await low.ok('longtask_propose',low.proposal()),id=p.task.taskId;await low.ok('longtask_confirm',{taskId:id,version:1});
    const worker=low.created[0];assert.ok(worker);const result=await (low.root as any).waterfall('agent/pre-step',{agent:worker,messages:[],signal:new AbortController().signal},async()=>({kind:'enter',messages:[]}));
    assert.equal(result.kind,'reject');await until(()=>low.user.session.snapshotEvents().some((e:any)=>e.data.source?.kind==='longtask-status'));
    const notice=low.user.session.snapshotEvents().findLast((e:any)=>e.data.source?.kind==='longtask-status').data;
    assert.match(notice.content[0].text,/金额准入不足/);assert.match(notice.content[0].text,/发送继续不能解除/);
    assert.equal(low.api.getMoneyBudget(id).limitAmount,'0.1');assert.equal(low.api.getMoneyBudget(id).calculatedAmount,'0');
  }finally{await low.close()}
});

test("正式入口：只加载核心apply可发现五个用户工具；提案/确认/暂停/查看走真实SDK管线",async()=>{
  const f=await fixture();try{
    assert.equal(f.api.getRuntimeInfo().userEntryReady,true);assert.equal(f.api.getRuntimeInfo().userEntryProtocol,3);
    assert.deepEqual(f.tools.schemas(f.user).map((s:any)=>s.name).sort(),[...USER_TASK_TOOLS].sort());
    const experts=await f.ok("longtask_experts",{query:"worker"});assert.equal(experts.total,1);assert.doesNotMatch(JSON.stringify(experts),/正文不得/);
    const proposal=await f.ok("longtask_propose",f.proposal()),id=proposal.task.taskId;assert.equal(proposal.task.status,"proposed");assert.equal(f.created.length,0);assert.equal(f.api.getBudget(id),undefined);
    const started=await f.ok("longtask_confirm",{taskId:id,version:1});assert.equal(started.task.status,"active");assert.equal(f.created.length,1);assert.equal(f.approvals.length,1);
    assert.match(f.approvals[0].reason,/比较存储方案/);assert.match(f.approvals[0].reason,/Agent建议¥5/);assert.match(f.approvals[0].reason,/产物可读/);assert.match(f.approvals[0].reason,/research\/reviewer.md/);
    assert.equal((await f.ok("longtask_confirm",{taskId:id,version:1})).alreadyConfirmed,true);assert.equal(f.approvals.length,1);assert.equal(f.created.length,1);
    const paused=await f.ok("longtask_control",{taskId:id,action:"pause",reason:"用户暂停"});assert.equal(paused.task.pauseReason,"user");assert.ok(f.stopCalls>=1);
    const detail=await f.ok("longtask_tasks",{taskId:id});assert.equal(detail.task.budget.calculatedAmount,"0");assert.equal(detail.task.budget.limitAmount,"5");assert.equal((await f.ok("longtask_tasks")).total,1);
    assert.equal((await f.ok("longtask_control",{taskId:id,action:"resume"})).task.status,"active");assert.equal(f.approvals.length,2);assert.deepEqual(f.api.getBudget(id).grants,[]);
    assert.equal((await f.ok("longtask_control",{taskId:id,action:"cancel"})).task.status,"cancelled");assert.equal((await f.call("longtask_control",{taskId:id,action:"resume"})).isError,true);
  }finally{await f.close();}
});

test("正式确认：宿主拒绝/取消/缺确认通道均不创建Worker，不接受模型confirmed标记",async()=>{
  const f=await fixture();try{
    const p=await f.ok("longtask_propose",f.proposal()),id=p.task.taskId;
    for(const outcome of ["rejected","cancelled","unavailable"]){f.setApproval(outcome);assert.equal((await f.call("longtask_confirm",{taskId:id,version:1})).isError,true);assert.equal(f.api.getTask(id).status,"proposed");assert.equal(f.created.length,0);assert.equal(f.api.getBudget(id),undefined);}
    f.services.delete("userQuestions");assert.equal((await f.call("longtask_confirm",{taskId:id,version:1})).isError,true);
    assert.equal((await f.call("longtask_confirm",{taskId:id,version:1,confirmed:true})).isError,true);assert.equal(f.created.length,0);
  }finally{await f.close();}
});

test("正式提案：本地专家、独立验收、标准唯一、绝对路径和显式预算均前置校验",async()=>{
  const f=await fixture();try{
    for(const patch of [{executorRelPaths:["../outside.md"]},{acceptorRelPath:"research/worker.md"},{criteria:[{id:"c1",text:"a"},{id:"c1",text:"b"}]},{deliverables:["relative.md"]},{budgetTokens:200000},{suggestedBudgetCny:"-1"},{strict:false},{executorRelPaths:["research/worker.md","research/second.md"]}])assert.equal((await f.call("longtask_propose",f.proposal(patch))).isError,true,JSON.stringify(patch));
    assert.equal(f.api.listTasks().length,0);assert.equal(f.created.length,0);
    const zero=await f.ok("longtask_propose",f.proposal());f.setAmount("0");assert.equal((await f.call("longtask_confirm",{taskId:zero.task.taskId,version:1})).isError,true);assert.equal(f.created.length,0);
  }finally{await f.close();}
});

test("方案调整：未确认可换人改分工；旧版本和确认期间变化均不启动",async()=>{
  const f=await fixture();try{
    const first=await f.ok("longtask_propose",f.proposal()),id=first.task.taskId;
    const updated=await f.ok("longtask_propose",f.proposal({taskId:id,expectedVersion:1,executorRelPaths:["research/second.md"]}));assert.equal(updated.task.version,2);assert.equal(updated.task.executors[0].relPath,"research/second.md");
    assert.equal((await f.call("longtask_confirm",{taskId:id,version:1})).isError,true);assert.equal(f.approvals.length,0);
    let release:any;f.setApprovalHandler(()=>new Promise(r=>{release=r}));const pending=f.call("longtask_confirm",{taskId:id,version:2});await until(()=>!!release);
    assert.equal((await f.ok("longtask_confirm",{taskId:id,version:2})).confirmationPending,true);assert.equal(f.approvals.length,1);
    await f.api.proposeNewVersion(id,{goal:"审批期间改了目标"});release("allowed-once");assert.equal((await pending).isError,true);assert.equal(f.created.length,0);assert.equal(f.api.getTask(id).status,"proposed");
  }finally{await f.close();}
});

test("专家定义变更不沿用旧方案开工；已经启动的方案不能通过propose静默重写",async()=>{
  const f=await fixture();try{
    const p=await f.ok("longtask_propose",f.proposal()),id=p.task.taskId;
    const file=join(f.experts,"research/worker.md"),original=readFileSync(file,'utf8');writeFileSync(file,original+"changed");
    assert.equal((await f.call("longtask_confirm",{taskId:id,version:1})).isError,true);assert.equal(f.approvals.length,0);writeFileSync(file,original);
    await f.ok("longtask_confirm",{taskId:id,version:1});assert.equal((await f.call("longtask_propose",f.proposal({taskId:id,expectedVersion:1}))).isError,true);
  }finally{await f.close();}
});

test("任务管理工具不进入真实Worker工具面；Worker、验收与后代均不能调用管理正文",async()=>{
  const f=await fixture();try{
    const p=await f.ok("longtask_propose",f.proposal());await f.ok("longtask_confirm",{taskId:p.task.taskId,version:1});const worker=f.created[0];
    assert.equal(f.tools.schemas(worker).some((s:any)=>(USER_TASK_TOOLS as readonly string[]).includes(s.name)),false);
    assert.equal((await f.call("longtask_tasks",{},worker)).isError,true);
    const definition=f.tools.get("longtask_propose",f.user);assert.ok(definition);
    await assert.rejects(()=>definition.execute(f.proposal(),{agent:worker,callId:"direct",signal:new AbortController().signal}),/只供用户会话/);
    const child=f.makeAgent("child",worker);assert.equal((await f.call("longtask_propose",f.proposal(),child)).isError,true);
    assert.equal(f.api.listTasks().length,1);assert.equal(f.approvals.length,1);
  }finally{await f.close();}
});

test("卸载取消待确认请求并注销工具，不留下迟到确认启动",async()=>{
  const f=await fixture();let pending:Promise<any>|undefined;
  try{
    const p=await f.ok("longtask_propose",f.proposal());f.setApprovalHandler((input:any)=>new Promise((_r,reject)=>input.signal.addEventListener('abort',()=>reject(Error('unloaded')),{once:true})));
    pending=f.call("longtask_confirm",{taskId:p.task.taskId,version:1});await until(()=>f.approvals.length===1);await f.close();assert.equal((await pending).isError,true);
    assert.equal(f.tools.schemas(f.user).some((s:any)=>(USER_TASK_TOOLS as readonly string[]).includes(s.name)),false);assert.equal(f.created.length,0);
  }finally{await f.close();if(pending)await pending;}
});

test("原子确认绑定预期版本，排队事务中的旧审批不能确认新方案",async()=>{
  const directory=mkdtempSync(join(tmpdir(),"longtask-confirm-version-")),store=await JsonStore.load(join(directory,'state.json')),ledger=new TokenLedger(store);
  const host:any={createSession:async()=>{throw Error('不得派工')},deliver:async()=>{throw Error('不得投递')},sessionAlive:async()=>false,releaseSession:async()=>{},stop:async()=>true};
  const tasks=new TaskService({store,ledger,host});const expert=(relPath:string)=>({relPath,division:'d',name:relPath,intro:'',contentHash:'h',snapshottedAt:new Date().toISOString()});
  try{
    const task=await tasks.propose({goal:'旧方案',criteria:[{id:'c1',text:'标准'}],deliverables:[join(directory,'a.md')],executors:[expert('a.md')],acceptor:expert('b.md'),budget:{limitTokens:200000,strict:true}});
    await tasks.proposeNewVersion(task.id,{goal:'新方案'});await assert.rejects(()=>tasks.confirm(task.id,0,1),/方案版本已变化/);assert.equal(tasks.getTask(task.id)?.status,'proposed');assert.equal(ledger.getBudget(task.id),undefined);
  }finally{await store.close();}
});

test("暂停/取消优先于迟到的开工或恢复确认；旧窗口同意不能重新启动",async()=>{
  for(const action of ["pause","cancel"]){
    const f=await fixture();try{
      const p=await f.ok("longtask_propose",f.proposal());let release:any;f.setApprovalHandler(()=>new Promise(r=>{release=r}));
      const pending=f.call("longtask_confirm",{taskId:p.task.taskId,version:1});await until(()=>!!release);
      await f.ok("longtask_control",{taskId:p.task.taskId,action});release("allowed-once");assert.equal((await pending).isError,true);assert.equal(f.created.length,0);
      assert.equal(f.api.getTask(p.task.taskId).status,action==='pause'?'proposed':'cancelled');
    }finally{await f.close();}
  }
  const f=await fixture();try{
    const p=await f.ok("longtask_propose",f.proposal());await f.ok("longtask_confirm",{taskId:p.task.taskId,version:1});await f.ok("longtask_control",{taskId:p.task.taskId,action:'pause'});const before=f.briefs.length;
    let release:any;f.setApprovalHandler(()=>new Promise(r=>{release=r}));const pending=f.call("longtask_control",{taskId:p.task.taskId,action:'resume'});await until(()=>!!release);
    await f.ok("longtask_control",{taskId:p.task.taskId,action:'pause'});release('allowed-once');assert.equal((await pending).isError,true);assert.equal(f.briefs.length,before);assert.equal(f.api.getTask(p.task.taskId).pauseReason,'user');
  }finally{await f.close();}
});

test("正式反馈：具体澄清问题可查看，答复经审批先投递后恢复，用户暂停不能被答案解除",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;await f.ok('longtask_confirm',{taskId:id,version:1});
    const waiting=await f.ok('longtask_control',{taskId:id,action:'clarify',text:'请明确交付面向谁？'});assert.match(waiting.task.pauseNote,/交付面向谁/);const count=f.briefs.length;
    f.setApproval('rejected');assert.equal((await f.call('longtask_control',{taskId:id,action:'answer',text:'面向三人团队'})).isError,true);assert.equal(f.briefs.length,count);
    f.setApproval('allowed-once');const resumed=await f.ok('longtask_control',{taskId:id,action:'answer',text:'面向三人团队'});assert.equal(resumed.task.status,'active');assert.match(f.approvals.at(-1).reason,/面向三人团队/);
    assert.equal(f.briefs.filter(m=>JSON.stringify(m).includes('【用户答复】')).length,1);assert.equal(resumed.task.budget.limitAmount,"5");assert.deepEqual(f.api.getBudget(id).grants,[]);
    await f.ok('longtask_control',{taskId:id,action:'supplement',text:'输出用中文'});const n=f.briefs.length;await f.ok('longtask_control',{taskId:id,action:'supplement',text:'输出用中文'});assert.equal(f.briefs.length,n);
    await f.ok('longtask_control',{taskId:id,action:'pause'});assert.equal((await f.call('longtask_control',{taskId:id,action:'answer',text:'不能隐式恢复'})).isError,true);
    assert.equal((await f.call('longtask_control',{taskId:id,action:'resume',text:'多余参数'})).isError,true);
  }finally{await f.close()}
});

test("正式换人：只替换本地在职执行者，经审批转交分工并撤权，不改变验收者及预算",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;await f.ok('longtask_confirm',{taskId:id,version:1});
    const change={taskId:id,action:'replace_executor',addRelPath:'research/second.md',removeRelPath:'research/worker.md',expectedVersion:1,reason:'改由分析专家接手',pendingItems:['补推荐依据']};
    f.setApproval('rejected');assert.equal((await f.call('longtask_control',change)).isError,true);assert.equal(f.api.getTask(id).versions[0].executors[0].relPath,'research/worker.md');
    f.setApproval('allowed-once');const changed=await f.ok('longtask_control',change);assert.equal(changed.task.executors[0].relPath,'research/second.md');assert.equal(changed.task.assignments[0].relPath,'research/second.md');
    assert.equal(changed.task.acceptor.relPath,'research/reviewer.md');assert.equal(changed.task.budget.limitAmount,"5");assert.ok(f.api.getTaskSessions(id).find((s:any)=>s.relPath==='research/worker.md').revokedAt);
    assert.match(f.approvals.at(-1).reason,/补推荐依据/);assert.ok(f.briefs.some(m=>JSON.stringify(m).includes('【接手任务】')));
    assert.equal((await f.call('longtask_control',{...change,addRelPath:'research/reviewer.md',removeRelPath:'research/second.md'})).isError,true);
    assert.equal((await f.call('longtask_tasks',{},f.created[0])).isError,true);
  }finally{await f.close()}
});

test("正式只读验收：真实SDK工具面隐藏write和shell，guard拒绝全局工具绕过，执行者权限保留",async()=>{
  const f=await fixture({acceptorReadOnly:true});const stops:any[]=[];try{
    let writes=0;for(const name of ['read','write','bash'])stops.push(f.tools.register({name,description:name,parameters:{type:'object',properties:{}},output:{schema:{type:'string'},render:(_a:any,v:any)=>[{type:'text',text:v}]},execute:async()=>{if(name!=='read')writes++;return name}}));
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;await f.ok('longtask_confirm',{taskId:id,version:1});const worker=f.created[0];assert.equal((await f.call('write',{},worker)).isError,false);
    writeFileSync(join(f.directory,'result.md'),'验收只读核对');(f.root as any).emit('session/event',worker.session,{type:'assistant/message',data:{turn:1,message:{role:'assistant',content:[{type:'text',text:'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>'}]}}});
    (f.root as any).emit('session/event',worker.session,{type:'turn/end',data:{turn:1,reason:{kind:'completed'}}});await until(()=>f.created.length===2).catch(e=>{throw Error(`${e}\n${f.logs.join('\n')}\n${JSON.stringify(f.api.getTask(id))}`)});const reviewer=f.created[1];
    const names=f.tools.schemas(reviewer).map((s:any)=>s.name);assert.ok(names.includes('read'));assert.equal(names.includes('write'),false);assert.equal(names.includes('bash'),false);
    assert.equal((await f.call('read',{},reviewer)).isError,false);assert.equal((await f.call('write',{},reviewer)).isError,true);assert.equal((await f.call('bash',{},reviewer)).isError,true);assert.equal(writes,1);
    assert.equal(f.api.getRuntimeInfo().acceptorReadOnly,true);
  }finally{for(const stop of stops)stop();await f.close()}
});

test("正式关联新要求与纠错：子任务独立待确认；已交付任务经审批同ID升版本且继承费用",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;await f.ok('longtask_confirm',{taskId:id,version:1});
    const child=await f.ok('longtask_propose',f.proposal({parentTaskId:id,goal:'额外需求',deliverables:[join(f.directory,'child.md')],suggestedBudgetCny:"1"}));assert.equal(child.task.relatedTaskId,id);assert.equal(child.task.relationKind,'child');assert.equal(child.task.status,'proposed');assert.equal(f.api.getBudget(child.task.taskId),undefined);assert.equal(f.created.length,1);
    const worker=f.created[0];
    await (f.root as any).waterfall('agent/pre-step',{agent:worker,messages:[],signal:new AbortController().signal},async()=>({kind:'enter',messages:[]}));
    await (f.root as any).waterfall('agent/request',{agent:worker,turn:1,step:1,signal:new AbortController().signal},async()=>({provider:'deepseek-official',model:'deepseek-flash',maxTokens:8000}));
    writeFileSync(join(f.directory,'result.md'),'原交付物');
    (f.root as any).emit('session/event',worker.session,{seq:100,type:'assistant/message',data:{turn:1,step:1,message:{role:'assistant',content:[{type:'text',text:'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>'}]},usage:{inputTokens:7,outputTokens:4,cacheReadTokens:0,cacheWriteTokens:0}}});
    (f.root as any).emit('session/event',worker.session,{type:'turn/end',data:{turn:1,reason:{kind:'completed'}}});
    await until(()=>f.created.length===2 && f.briefs.some(m=>JSON.stringify(m).includes('<longtask_verdicts>')));const reviewer=f.created[1],brief=f.briefs.map(m=>m.content.map((c:any)=>c.text??'').join('')).findLast(s=>s.includes('独立验收请求'))!;
    const template=JSON.parse(/<longtask_verdicts>([\s\S]*?)<\/longtask_verdicts>/.exec(brief)![1]!);
    template.verdicts=[{criterionId:'c1',outcome:'pass',evidence:'读取产物通过',gap:null,verdictBy:'research/reviewer.md',criteriaVersion:1}];
    (f.root as any).emit('session/event',reviewer.session,{type:'assistant/message',data:{turn:1,step:1,message:{role:'assistant',content:[{type:'text',text:`<longtask_verdicts>${JSON.stringify(template)}</longtask_verdicts>`}]}}});
    (f.root as any).emit('session/event',reviewer.session,{type:'turn/end',data:{turn:1,reason:{kind:'completed'}}});await until(()=>f.api.getTask(id).status==='delivered');
    const reopen={taskId:id,action:'reopen',expectedVersion:1,reason:'遗漏结论依据',criteria:[{id:'c2',text:'补充结论依据'}]};const before=f.api.getBudget(id).settledTokens;assert.equal(before,11);
    f.setApproval('rejected');assert.equal((await f.call('longtask_control',reopen)).isError,true);assert.equal(f.api.getTask(id).status,'delivered');
    f.setApproval('allowed-once');const next=await f.ok('longtask_control',reopen);assert.equal(next.task.taskId,id);assert.equal(next.task.version,2);assert.equal(f.api.getBudget(id).settledTokens,before);assert.equal(next.task.budget.limitAmount,"5");assert.deepEqual(f.api.getBudget(id).grants,[]);assert.equal(next.task.criteria.length,2);
    assert.match(f.approvals.at(-1).reason,/补充结论依据/);assert.equal((await f.call('longtask_control',reopen)).isError,true);
  }finally{await f.close()}
});
