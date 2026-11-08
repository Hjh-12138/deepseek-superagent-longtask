import {prepareSdkTools, loadSdk} from "./sdk-tools.js";
import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from "node:fs";
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
  const services=new Map<string,any>([["tools",tools],["sessions",{flush:async()=>true}],["agents",{get:(id:string)=>agents.get(id),list:()=>[...agents.values()],async create(options:any){const a=makeAgent(options.sessionId,options.parentAgent);a.parentAgent=options.parentAgent;await options.setup?.(a.ctx,a);created.push(a);return{agent:a,dispose:async()=>{agents.delete(a.id)}}}}],
    ["approval",{async request(input:any){approvals.push(input);return approvalOverride?approvalOverride(input):approvalResult}}],
    ["userQuestions",{async ask(input:any){const a={...input,reason:input.questions[0].detail};approvals.push(a);const outcome=approvalOverride?await approvalOverride(a):approvalResult;
      return {answers:[{id:"task_budget_cny",selected:[],...(outcome==="allowed-once"?{custom:userAmount}:{})}]};}}]]);
  const ctx:any={get:(name:string)=>services.get(name),provide:(name:string,value:any)=>{services.set(name,value);root.provide(name,value)},on:(name:string,fn:any)=>(root as any).on(name,fn),
    logger:{info:(m:string)=>logs.push(m),warn:(m:string)=>logs.push(m),error:(m:string)=>logs.push(m)},effect:(factory:any)=>{const it=factory(),first=it.next();if(typeof first.value==='function')teardowns.push(first.value)}};
  apply(ctx,{dataDir:directory,agencyAgentsRoot:experts,workerCwd:directory,workerMaxTokens:16000,workerParentAgent:"none",workerPreset:"",acceptorPreset:"",workerProvider:"deepseek-official",workerModel:"deepseek-flash",modelPrices:DEFAULT_PRICES.map(p=>({...p,verifiedAt:"2020-01-01",expiresAt:"2099-01-01"})),probeOnStart:false,watchdogIntervalMs:3600000,...overrides});
  await until(()=>services.get("longtask")?.getRuntimeInfo().ready===true);const api=services.get("longtask");let seq=0;
  async function call(name:string,args:any={},agent=user,signal=new AbortController().signal){return tools.execute({name,arguments:args,agent,signal,callId:`user-${++seq}`})}
  async function ok(name:string,args:any={},agent=user){const result=await call(name,args,agent);assert.equal(result.isError,false,JSON.stringify(result));return JSON.parse(result.value)}
  const proposal=(patch:any={})=>({goal:"比较存储方案",criteria:[{id:"c1",text:"产物可读"}],deliverables:[join(directory,"result.md")],executorRelPaths:["research/worker.md"],acceptorRelPath:"research/reviewer.md",suggestedBudgetCny:"5",...patch});
  return {directory,experts,root,tools,services,api,user,created,briefs,approvals,logs,makeAgent,call,ok,proposal,setAmount:(v:string)=>{userAmount=v},setApproval:(v:string)=>{approvalResult=v},setApprovalHandler:(v:any)=>{approvalOverride=v},get stopCalls(){return stops},
    async close(){for(const stop of teardowns.splice(0))await stop();for(const scope of scopes)await scope.dispose();await root.fiber.dispose()}};
}

function addEngineeringCatalog(f:Awaited<ReturnType<typeof fixture>>){
  writeFileSync(join(f.experts,"divisions.json"),JSON.stringify({divisions:{research:{label:"Research"},engineering:{label:"Engineering"},testing:{label:"Testing"}}}));
  const definitions=[
    ["engineering/frontend.md","Frontend Developer","Build accessible React and Vue user interfaces"],
    ["engineering/backend.md","Backend Architect","Design backend services and server architecture"],
    ["engineering/database.md","Database Optimizer","Optimize SQL databases and query performance"],
    ["engineering/architect.md","Software Architect","Design software system architecture"],
    ["engineering/reviewer.md","Code Reviewer","Independent code review and correctness"],
    ["testing/automation.md","Test Automation Engineer","Automated tests and regression suites"],
  ];
  for(const [rel,name,intro] of definitions){mkdirSync(join(f.experts,rel!.split('/')[0]!),{recursive:true});writeFileSync(join(f.experts,rel!),`---\nname: ${name}\ndescription: ${intro}\n---\nEXPERT_BODY_SHOULD_STAY_LAZY`);}
}

test("专家入口支持中文数据库检索，不加载专家正文或启动Worker",async()=>{
  const f=await fixture();try{
    addEngineeringCatalog(f);const result=await f.ok("longtask_experts",{query:"数据库"});
    assert.equal(result.experts[0]?.relPath,"engineering/database.md");assert.doesNotMatch(JSON.stringify(result),/EXPERT_BODY_SHOULD_STAY_LAZY/);assert.equal(f.created.length,0);
  }finally{await f.close();}
});

test("软件方案缺职责和模块计划时禁止确认，不能仅凭提醒开工",async()=>{
  const f=await fixture();try{
    addEngineeringCatalog(f);
    const p=await f.ok('longtask_propose',f.proposal({goal:'开发React前端和Node后端客资系统，SQLite数据库与测试',executorRelPaths:['engineering/backend.md'],acceptorRelPath:'engineering/reviewer.md'}));
    const result=await f.call('longtask_confirm',{taskId:p.task.taskId,version:1});
    assert.equal(result.isError,true,'未分配职责、源码、批次的方案不得开工');
    assert.equal(f.created.length,0);assert.equal(f.approvals.length,0);assert.equal(f.api.getBudget(p.task.taskId),undefined);
  }finally{await f.close()}
});

test("实测标准缺计划时确认前拦截，不发审批、不建预算；静态提案明确展示缺口",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal({criteria:[{id:'c1',text:'npm test 全部通过'}]}));
    assert.ok(p.task.verificationPlanErrors.length);const result=await f.call('longtask_confirm',{taskId:p.task.taskId,version:1});
    assert.equal(result.isError,true);assert.match(JSON.stringify(result),/实测计划/);assert.equal(f.created.length,0);assert.equal(f.approvals.length,0);assert.equal(f.api.getBudget(p.task.taskId),undefined);
  }finally{await f.close()}
});

test("纯文档正式提案：技术说明不触发开发模块校验，也不依赖实测shell",async()=>{
  const f=await fixture();try{
    f.services.delete('shell');
    const p=await f.ok('longtask_propose',f.proposal({goal:'编写API接口使用文档',criteria:[{id:'doc',text:'文档包含接口集成测试用例、npm test命令说明、浏览器兼容性及HTTP 401示例',verification:['review']}]}));
    assert.equal(p.task.staffingReview.domain,'general');assert.deepEqual(p.task.verificationPlanErrors,[]);
    const result=await f.ok('longtask_confirm',{taskId:p.task.taskId,version:1});assert.equal(result.task.status,'active');assert.equal(f.created.length,1);
  }finally{await f.close()}
});

test("真实SDK正式工具链：声明实测、只读验收调用专用工具、引用运行记录交付与原预算补测",async()=>{
  mkdirSync(join(process.cwd(),'.longtask','verification'),{recursive:true});
  const source=mkdtempSync(join(process.cwd(),'.longtask','verification','entry-verify-')),file=join(source,'artifact.txt');writeFileSync(file,'frozen');
  const f=await fixture({workerCwd:source,acceptorReadOnly:true});try{
    f.tools.register({name:'read',description:'read',parameters:{type:'object',properties:{}},output:{schema:{type:'string'},render:()=>[]},execute:async()=>"frozen"});
    let executions=0;
    f.services.set('shell',{sandboxMode:'workspace-write',resolve:(r:any)=>r,execute:async(spec:any)=>{executions++;assert.notEqual(spec.workdir,source);assert.equal(spec.sandboxPolicy.workspaceRoot,spec.workdir);return {result:async()=>({exitCode:0,stdout:{text:'HOST_CHECK_OK'},stderr:{text:''},sandbox:{mode:'workspace-write',enforcement:'fixture'}})}}});
    const verificationPlan={workspace:source,checks:[{id:'check',kind:'command',criterionIds:['c1'],purpose:'检查冻结内容',program:'node',args:['--version'],timeoutMs:10000}]};
    const p=await f.ok('longtask_propose',f.proposal({deliverables:[file],criteria:[{id:'c1',text:'运行测试结果通过',verification:['command']}],verificationPlan})),id=p.task.taskId;
    await f.ok('longtask_confirm',{taskId:id,version:1});assert.match(f.approvals[0].reason,/实测计划/);
    const worker=f.created[0];let seq=9700;
    const turn=async(agent:any,text:string)=>{
      (f.root as any).emit('session/event',agent.session,{seq:seq++,type:'turn/start',data:{turn:1}});
      const decision=await (f.root as any).waterfall('agent/pre-step',{agent,turn:1,step:1,messages:[],signal:new AbortController().signal},async()=>({kind:'enter',messages:[]}));assert.notEqual(decision.kind,'reject');
      await (f.root as any).waterfall('agent/request',{agent,turn:1,step:1,signal:new AbortController().signal},async()=>({provider:'deepseek-official',model:'deepseek-flash',maxTokens:16000}));
      (f.root as any).emit('session/event',agent.session,{seq:seq++,type:'assistant/message',data:{turn:1,step:1,message:{role:'assistant',content:[{type:'text',text}]},usage:{inputTokens:1000,outputTokens:100,cacheReadTokens:0,cacheWriteTokens:0}}});
      (f.root as any).emit('session/event',agent.session,{seq:seq++,type:'turn/end',data:{turn:1,reason:{kind:'completed'}}});
    };
    await turn(worker,'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>');
    await until(()=>f.created.length===2&&f.api.getTask(id).status==='awaiting_acceptance');const acceptor=f.created[1];
    await until(()=>f.api.getTaskSessions(id).some((s:any)=>s.role==='acceptor'&&s.sessionId===acceptor.session.id));
    assert.equal((await f.call('longtask_verify',{checkId:'check'},worker)).isError,true);
    assert.ok(f.tools.schemas(acceptor).some((s:any)=>s.name==='longtask_verify'));
    assert.ok(!f.tools.schemas(acceptor).some((s:any)=>s.name==='pwsh'));
    const run=await f.ok('longtask_verify',{checkId:'check'},acceptor);assert.equal(run.status,'passed');assert.equal(executions,1);
    await turn(acceptor,`<longtask_verdicts>${JSON.stringify({roundId:run.roundId,artifactHashes:run.artifactHashes,verdicts:[{criterionId:'c1',outcome:'pass',evidence:run.id,verificationRunIds:[run.id],gap:null,verdictBy:'research/reviewer.md',criteriaVersion:1}]})}</longtask_verdicts>`);
    await until(()=>f.api.getTask(id).status==='delivered');
    assert.equal((await f.ok('longtask_tasks',{taskId:id})).task.runtimeVerification.verified,true);
    const money=f.api.getMoneyBudget(id);await f.ok('longtask_control',{taskId:id,action:'verify',expectedVersion:1,verificationPlan,criteria:[{id:'c1',text:'按README实际运行测试并通过',verification:['command','review']}],verificationScope:{included:['服务和接口实测'],excluded:['浏览器交互']},reason:'用户选择服务级口径'});
    assert.equal(f.api.getTask(id).status,'awaiting_acceptance');assert.equal(f.api.getTask(id).versions.at(-1).version,2);
    assert.equal(f.created.length,2,'补测不重新创建开发Worker');assert.deepEqual(f.api.getMoneyBudget(id),money);
    assert.match(f.approvals.at(-1).reason,/标准变化/);assert.match(f.approvals.at(-1).reason,/浏览器交互/);
    assert.equal(f.api.getTask(id).versions[1].criteria[0].text,'按README实际运行测试并通过');
  }finally{await f.close();assert.ok(source.startsWith(join(process.cwd(),'.longtask','verification')));rmSync(source,{recursive:true,force:true})}
});

test("正式复验方案只交报告可确认，不必把既有测试代码伪装为新交付",async()=>{
  const f=await fixture();try{
    f.services.set('shell',{sandboxMode:'workspace-write',resolve:(r:any)=>r,execute:async()=>{throw Error('确认不得运行命令')}});
    const p=await f.ok('longtask_propose',f.proposal({goal:'对已交付的API系统运行级复验，不改业务代码，只输出报告',criteria:[{id:'c1',text:'npm test全部通过',verification:['command']}],verificationPlan:{workspace:f.directory,checks:[{id:'api',kind:'command',criterionIds:['c1'],purpose:'执行既有测试',program:'node',args:['tests/run-tests.js'],timeoutMs:10000}]}}));
    assert.equal(p.task.workKind,'verification');assert.equal(p.task.executionPlanValidation,null);assert.equal(p.task.deliverables.length,1);assert.match(p.task.deliverables[0],/result.md$/);
    const started=await f.ok('longtask_confirm',{taskId:p.task.taskId,version:1});assert.equal(started.task.status,'active');assert.equal(f.created.length,1);
  }finally{await f.close()}
});

test("显式Worker限额16000时请求上限与准入预留一致",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal());await f.ok('longtask_confirm',{taskId:p.task.taskId,version:1});
    const worker=f.created[0];
    const decision=await (f.root as any).waterfall('agent/pre-step',{agent:worker,turn:1,step:1,messages:[]},async()=>({kind:'continue'}));
    assert.notEqual(decision.kind,'reject');
    const config=await (f.root as any).waterfall('agent/request',{agent:worker,turn:1,step:1,signal:new AbortController().signal},async()=>({provider:'deepseek-official',model:'deepseek-flash',maxTokens:50000}));
    assert.equal(config.maxTokens,16000);assert.equal(f.api.getBudget(p.task.taskId).reservedTokens,31000);
  }finally{await f.close()}
});

test("完整软件模块计划可确认；职责、标准、批次进入用户确认和角色简报",async()=>{
  const f=await fixture();try{
    addEngineeringCatalog(f);
    const entries=[['engineering/backend.md','后端',['architecture','backend','api'],'api','server.ts'],['engineering/frontend.md','前端',['frontend'],'ui','App.tsx'],['engineering/database.md','数据库',['database'],'db','schema.prisma'],['testing/automation.md','回归',['testing'],'regression','server.test.ts']] as const;
    const assignments=entries.map(([relPath,module,responsibilities,id,file])=>({relPath,module,responsibilities:[...responsibilities],criterionIds:[id],coverageRationale:'先明确边界并按小批次实现',deliverables:[join(f.directory,file)],batches:[{id,title:'完成'+module+'并落盘',deliverables:[join(f.directory,file)]}]}));
    const p=await f.ok('longtask_propose',f.proposal({goal:'开发React前端和Node后端客资系统，SQLite数据库与测试',criteria:entries.map(x=>({id:x[3],text:x[3]+'_ONLY',verification:['review']})),executorRelPaths:entries.map(x=>x[0]),acceptorRelPath:'engineering/reviewer.md',assignments,deliverables:assignments.flatMap(a=>a.deliverables),workModules:entries.map((x,i)=>({id:x[3],title:['客户服务','客户操作界面','客户资料持久化','流程回归'][i],scope:'按模块边界实现并核对'+x[3],leadRelPath:x[0],criterionIds:[x[3]],deliverables:assignments[i]!.deliverables}))}));
    assert.equal(p.task.executionPlanValidation.valid,true,JSON.stringify(p.task.executionPlanValidation));
    await f.ok('longtask_confirm',{taskId:p.task.taskId,version:1});assert.equal(f.created.length,4);assert.match(f.approvals[0].reason,/coverageRationale|先明确边界/);
    const backend=JSON.stringify(f.briefs.find(b=>JSON.stringify(b).includes('模块：后端')));assert.match(backend,/api_ONLY/);assert.doesNotMatch(backend,/ui_ONLY|regression_ONLY/);assert.match(backend,/batchId/);
  }finally{await f.close()}
});

test("真实会话事件驱动批次：写盘与用量结算后推进，最后一批才启动验收",async()=>{
  const f=await fixture();try{
    addEngineeringCatalog(f);const files=[join(f.directory,'server.ts'),join(f.directory,'server.test.ts')];
    const p=await f.ok('longtask_propose',f.proposal({goal:'开发后端系统',criteria:[{id:'core',text:'业务正确',verification:['review']}],executorRelPaths:['engineering/backend.md'],acceptorRelPath:'engineering/reviewer.md',deliverables:files,workModules:[{id:'core',title:'业务服务与回归',scope:'实现服务行为并验证',leadRelPath:'engineering/backend.md',criterionIds:['core'],deliverables:files}],
      assignments:[{relPath:'engineering/backend.md',module:'小型后端',responsibilities:['architecture','backend','testing'],coverageRationale:'范围较小，由后端实现并提供测试证据，验收保持独立',criterionIds:['core'],deliverables:files,batches:files.map((file,i)=>({id:'batch'+i,title:'完成文件'+i,deliverables:[file]}))}]}));
    const id=p.task.taskId;await f.ok('longtask_confirm',{taskId:id,version:1});const worker=f.created[0];
    for(let i=0;i<2;i++){
      (f.root as any).emit('session/event',worker.session,{seq:9600+i*10,type:'turn/start',data:{turn:i+1}});
      await (f.root as any).waterfall('agent/pre-step',{agent:worker,turn:i+1,step:1,messages:[],signal:new AbortController().signal},async()=>({kind:'enter',messages:[]}));
      await (f.root as any).waterfall('agent/request',{agent:worker,turn:i+1,step:1,signal:new AbortController().signal},async()=>({provider:'deepseek-official',model:'deepseek-flash',maxTokens:16000}));
      writeFileSync(files[i]!,`export const result${i}=true;`);
      (f.root as any).emit('session/event',worker.session,{seq:9601+i*10,type:'assistant/message',data:{turn:i+1,step:1,message:{role:'assistant',content:[{type:'text',text:`<longtask_report>{"done":true,"batchId":"batch${i}","next":[],"waiting":null}</longtask_report>`}]},usage:{inputTokens:1000,outputTokens:100,cacheReadTokens:0,cacheWriteTokens:0}}});
      (f.root as any).emit('session/event',worker.session,{seq:9602+i*10,type:'turn/end',data:{turn:i+1,reason:{kind:'completed'}}});
      if(i===0){await until(()=>JSON.stringify(f.briefs.at(-1)).includes('只执行当前批次 batch1'));assert.equal(f.created.length,1);assert.equal(f.api.getTask(id).status,'active');}
      else {await until(()=>f.api.getTask(id).status==='awaiting_acceptance'&&f.created.length===2);assert.equal(f.created.length,2);}
    }
    assert.equal(f.api.getBudget(id).settledTokens,2200);assert.equal(f.api.getBudget(id).reservedTokens,0);
  }finally{await f.close()}
});

test("完整开发任务返回职责推荐；前后端方案提示测试等专职空缺，不自动加人",async()=>{
  const f=await fixture();try{
    addEngineeringCatalog(f);const goal="开发Vue前端和FastAPI后端的客资系统，用SQLite数据库持久化，并用pytest编写自动化测试";
    const result=await f.ok("longtask_experts",{query:"前端",task:goal,limit:1});
    assert.equal(result.experts[0].relPath,"engineering/frontend.md");assert.ok(result.catalogTotal>result.experts.length);
    assert.equal(result.recommendation.domain,"software");
    assert.ok(result.recommendation.responsibilities.some((r:any)=>r.id==="database"&&r.candidates.some((e:any)=>e.relPath==="engineering/database.md")));
    assert.ok(result.recommendation.responsibilities.some((r:any)=>r.id==="testing"&&r.candidates.some((e:any)=>e.relPath==="testing/automation.md")));
    const p=await f.ok("longtask_propose",f.proposal({goal,executorRelPaths:["engineering/frontend.md","engineering/backend.md"],acceptorRelPath:"engineering/reviewer.md",assignments:[{relPath:"engineering/frontend.md",deliverables:[join(f.directory,"result.md")]},{relPath:"engineering/backend.md",deliverables:[join(f.directory,"backend.md")]}],deliverables:[join(f.directory,"result.md"),join(f.directory,"backend.md")]}));
    assert.equal(p.task.executors.length,2);assert.equal(f.created.length,0);
    assert.ok(p.task.staffingReview.needsDiscussion.some((r:any)=>r.id==="testing"));
    assert.ok(p.recommendation.responsibilities.some((r:any)=>r.id==="testing"&&r.candidates.some((e:any)=>e.relPath==="testing/automation.md")),"仅按前后端提交方案时也应自动返回职责候选");
    assert.equal((await f.call("longtask_confirm",{taskId:p.task.taskId,version:1})).isError,true);assert.equal(f.created.length,0);
  }finally{await f.close();}
});

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
  const low=await fixture({workerMaxTokens:8000});try{
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
    assert.equal(f.api.getRuntimeInfo().userEntryReady,true);assert.equal(f.api.getRuntimeInfo().userEntryProtocol,7);
    assert.deepEqual(f.tools.schemas(f.user).map((s:any)=>s.name).filter((name:string)=>name!=="longtask_verify").sort(),[...USER_TASK_TOOLS].sort());
    assert.equal((await f.call("longtask_verify",{checkId:"unknown"})).isError,true,"用户会话不能执行独立验收工具");
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

test("恢复前识别宿主never策略，不发必拒审批、不误报用户拒绝，切回ask后可恢复",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;
    await f.ok('longtask_confirm',{taskId:id,version:1});
    await f.ok('longtask_control',{taskId:id,action:'pause'});
    const approval=f.services.get('approval'),count=f.approvals.length,briefs=f.briefs.length;
    const budget=JSON.stringify(f.api.getBudget(id));let policy='never',observed:any;
    approval.effectivePolicy=(session:any)=>{observed=session;return policy};
    for(let i=0;i<2;i++){
      const result=await f.call('longtask_control',{taskId:id,action:'resume'});
      assert.equal(result.isError,true);assert.match(JSON.stringify(result),/宿主.*never/);
      assert.match(JSON.stringify(result),/ask/);assert.doesNotMatch(JSON.stringify(result),/用户未确认/);
    }
    assert.equal(observed,f.user.session);assert.equal(f.approvals.length,count);
    assert.equal(f.briefs.length,briefs);assert.equal(f.api.getTask(id).status,'paused');
    assert.equal(JSON.stringify(f.api.getBudget(id)),budget);
    policy='ask';await f.ok('longtask_control',{taskId:id,action:'resume'});
    assert.equal(f.approvals.length,count+1);assert.equal(f.api.getTask(id).status,'active');
  }finally{await f.close()}
});

test("宿主审批非授权结果分别报告，不把宿主拒绝归因于用户，也不重试或改走提问",async()=>{
  const f=await fixture();try{
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;
    await f.ok('longtask_confirm',{taskId:id,version:1});await f.ok('longtask_control',{taskId:id,action:'pause'});
    const expected={rejected:/宿主拒绝.*不代表用户/,cancelled:/确认.*取消/,unavailable:/确认通道.*不可用/};
    for(const [outcome,pattern] of Object.entries(expected)){
      f.setApproval(outcome);const count=f.approvals.length;
      const result=await f.call('longtask_control',{taskId:id,action:'resume'});
      assert.equal(result.isError,true);assert.match(JSON.stringify(result),pattern);
      assert.equal(f.approvals.length,count+1);assert.equal(f.api.getTask(id).status,'paused');
    }
  }finally{await f.close()}
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
  const f=await fixture({acceptorReadOnly:true,acceptorReadTools:["reference_lookup"]});const stops:any[]=[];try{
    let writes=0;for(const name of ['read','write','bash','reference_lookup'])stops.push(f.tools.register({name,description:name,parameters:{type:'object',properties:{}},output:{schema:{type:'string'},render:(_a:any,v:any)=>[{type:'text',text:v}]},execute:async()=>{if(name==='write'||name==='bash')writes++;return name}}));
    const p=await f.ok('longtask_propose',f.proposal()),id=p.task.taskId;await f.ok('longtask_confirm',{taskId:id,version:1});const worker=f.created[0];assert.equal((await f.call('write',{},worker)).isError,false);
    writeFileSync(join(f.directory,'result.md'),'验收只读核对');(f.root as any).emit('session/event',worker.session,{type:'assistant/message',data:{turn:1,message:{role:'assistant',content:[{type:'text',text:'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>'}]}}});
    (f.root as any).emit('session/event',worker.session,{type:'turn/end',data:{turn:1,reason:{kind:'completed'}}});await until(()=>f.created.length===2).catch(e=>{throw Error(`${e}\n${f.logs.join('\n')}\n${JSON.stringify(f.api.getTask(id))}`)});const reviewer=f.created[1];
    const names=f.tools.schemas(reviewer).map((s:any)=>s.name);assert.ok(names.includes('read'));assert.ok(names.includes('reference_lookup'));assert.equal((await f.call('reference_lookup',{},reviewer)).isError,false);assert.equal(names.includes('write'),false);assert.equal(names.includes('bash'),false);
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
    f.services.set('shell',{sandboxMode:'workspace-write',resolve:(r:any)=>r,execute:async()=>{throw Error('重开确认不得代跑验收')}});
    const verificationPlan={workspace:f.directory,checks:[{id:'supplement-check',kind:'command',criterionIds:['c2'],purpose:'补充结论依据检查',program:'node',args:['--version'],timeoutMs:10000}]};
    const reopen={taskId:id,action:'reopen',expectedVersion:1,reason:'遗漏结论依据',criteria:[{id:'c2',text:'补充结论依据并实际运行检查',verification:['command']}],verificationPlan,verificationScope:{included:['补充检查'],excluded:['浏览器交互']}};const before=f.api.getBudget(id).settledTokens;assert.equal(before,11);
    f.setApproval('rejected');assert.equal((await f.call('longtask_control',reopen)).isError,true);assert.equal(f.api.getTask(id).status,'delivered');
    f.setApproval('allowed-once');const next=await f.ok('longtask_control',reopen);assert.equal(next.task.taskId,id);assert.equal(next.task.version,2);assert.equal(f.api.getBudget(id).settledTokens,before);assert.equal(next.task.budget.limitAmount,"5");assert.deepEqual(f.api.getBudget(id).grants,[]);assert.equal(next.task.criteria.length,2);
    assert.match(f.approvals.at(-1).reason,/补充结论依据/);assert.equal((await f.call('longtask_control',reopen)).isError,true);
    assert.deepEqual(f.api.getTask(id).versions[1].verificationPlan,verificationPlan);assert.deepEqual(next.task.verificationScope.excluded,['浏览器交互']);
  }finally{await f.close()}
});

test("方案声明的验收能力不可用时不开Worker，补齐前不消耗执行预算",async()=>{
  const f=await fixture({acceptorReadOnly:true});try{
    const p=await f.ok("longtask_propose",f.proposal({acceptorRequiredTools:["reference_lookup"]}));
    assert.deepEqual(p.task.acceptorRequiredTools,["reference_lookup"]);
    const result=await f.call("longtask_confirm",{taskId:p.task.taskId,version:1});
    assert.equal(result.isError,true);assert.equal(f.created.length,0);
    assert.equal(f.api.getTask(p.task.taskId).status,"proposed");
  }finally{await f.close()}
});

for(const late of [false,true])test(`验收继承预设作用域读取工具${late?"（迟到装载）":""}，白名单不能用全局视图过滤`,async()=>{
  const f=await fixture({acceptorReadOnly:true,workerParentAgent:"auto"}),stops:Array<()=>void>=[];let writes=0;
  try{
    const definition=(name:string,execute:()=>Promise<string>)=>({name,description:name,parameters:{type:"object",properties:{}},output:{schema:{type:"string"},render:(_a:any,v:string)=>[{type:"text",text:v}]},execute});
    stops.push(f.tools.register(definition("working_directory",async()=>f.directory)));
    const addReaders=()=>{
      stops.push(f.user.ctx.tools.register(definition("read",async()=>readFileSync(join(f.directory,"result.md"),"utf8"))));
      stops.push(f.user.ctx.tools.register(definition("glob",async()=>"result.md")));
    };
    if(!late)addReaders();
    stops.push(f.user.ctx.tools.register(definition("write",async()=>{writes++;return "written"})));
    assert.equal(f.tools.get("read"),undefined,"read不在全局注册，来自继承的预设作用域");
    const p=await f.ok("longtask_propose",f.proposal()),id=p.task.taskId;await f.ok("longtask_confirm",{taskId:id,version:1});
    const worker=f.created[0];assert.ok(f.tools.schemas(worker).some((s:any)=>s.name==="write"));
    writeFileSync(join(f.directory,"result.md"),"READABLE_ARTIFACT");
    (f.root as any).emit("session/event",worker.session,{type:"assistant/message",data:{turn:1,message:{role:"assistant",content:[{type:"text",text:'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>'}]}}});
    (f.root as any).emit("session/event",worker.session,{type:"turn/end",data:{turn:1,reason:{kind:"completed"}}});
    await until(()=>f.created.length===2&&f.api.getTaskSessions(id).some((s:any)=>s.role==="acceptor"));let acceptor=f.created[1];
    if(late){
      assert.deepEqual(f.tools.schemas(acceptor).map((s:any)=>s.name).filter((name:string)=>name!=="longtask_verify"),["working_directory"]);
      addReaders();
      const step=await (f.root as any).waterfall("agent/pre-step",{agent:acceptor,messages:[]},async()=>({kind:"enter",messages:[]}));
      assert.equal(step.kind,"reject","迟到工具要求重组旧快照，未发模型请求");
      (f.root as any).emit("session/event",acceptor.session,{type:"turn/end",data:{turn:1,reason:{kind:"blocked"}}});
      await until(()=>JSON.stringify(f.briefs.at(-1)).includes("读取工具现已就绪"));
      assert.equal(f.api.getTask(id).status,"awaiting_acceptance");
      acceptor=f.created.at(-1);
    }
    const names=f.tools.schemas(acceptor).map((s:any)=>s.name);
    assert.ok(names.includes("read"),JSON.stringify(names));assert.ok(names.includes("glob"));assert.equal(names.includes("write"),false);
    const result=await f.call("read",{},acceptor);assert.equal(result.isError,false);assert.match(JSON.stringify(result),/READABLE_ARTIFACT/);
    assert.equal((await f.call("write",{},acceptor)).isError,true);assert.equal(writes,0);
  }finally{for(const stop of stops)stop();await f.close();}
});
