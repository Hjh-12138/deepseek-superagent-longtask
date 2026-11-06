import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,existsSync,readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import * as zlib from 'node:zlib';

const P=await import(new URL('../../../dsh-longtask-e2e/stage3-protocol.mjs',import.meta.url).href);
const D=await import(new URL('../../../dsh-longtask-e2e/stage3-driver.mjs',import.meta.url).href);
const A=await import(new URL('../../../dsh-longtask-e2e/stage3-audit.mjs',import.meta.url).href);
const C=await import(new URL('../../../dsh-longtask-e2e/request-stage3.mjs',import.meta.url).href);
const runId='11111111-1111-4111-8111-111111111111';
const temp=()=>mkdtempSync(join(tmpdir(),'lt-stage3-'));
function good(){
  const state:any={version:P.STAGE3_VERSION,runId,totalTokens:500000,shares:P.splitBudget(500000),window:{visible:true,pid:8,processName:'DeepSeek Harness'},initialHost:{pid:1,bootAt:10},
    beforeRestart:{host:{pid:1,bootAt:10},budgets:{}},restartedHost:{pid:2,bootAt:20},hiddenReleasedAt:150,tasks:{},writes:[],answerCode:'ST3-confirmed',answer:'完整用户答复 ST3-confirmed',
    initialParallel:true,initialQueued:true,priorityPromoted:true,pausePreserved:true,recoveredB:true,recoveredC:true,balancePreserved:true,answerQueued:true,answerConsumed:true,
    cStalledAt:150,cBaselineProgressAt:0,watchdogReceipt:{at:180},watchdogConfig:{stallThresholdMs:100,watchdogIntervalMs:10}};
  const budgets:any={},artifacts:any={},taskStates:any={};const sessions:any[]=[];
  for(const role of ['A','B','C']){
    state.tasks[role]={id:`t${role}`,artifact:`/${role}.md`};
    artifacts[role]={text:P.expectedBody(state,role),hash:P.sha(P.expectedBody(state,role)),frozenMatches:true};
    state.writes.push({role,at:200,callId:`write-${role}`,sessionId:`${role}-executor`,hash:artifacts[role].hash});
    budgets[role]={taskId:`t${role}`,limitTokens:state.shares[role],settledTokens:1000,reservedTokens:0,grants:[]};state.beforeRestart.budgets[role]={...budgets[role],settledTokens:500};taskStates[role]='delivered';
    for(const agentRole of ['executor','acceptor'])sessions.push({role,agentRole,sessionId:`${role}-${agentRole}`,matches:true,actualTokens:500,ledgerTokens:500,rawSha256:'a'.repeat(64)});
  }
  state.hiddenProof={runId,artifactHash:artifacts.A.hash,samples:[100,300].map(at=>({at,hidden:true,hostAlive:true,hostPid:1,processName:state.window.processName}))};
  return {state,input:{budgets,artifacts,taskStates,answers:[{id:'answer1',answer:state.answer,delivery:'delivered'}],rawAudit:{verified:true,actualTokens:3000,descendants:0,answerConsumed:1,watchdogConsumed:1,watchdogVerified:true,writesMatch:true,writes:state.writes,sessions}}};
}

test('阶段三契约：总预算守恒，必须显式授权，已有请求不被覆盖',()=>{
  for(const total of [3,499999,500000])assert.equal(Object.values(P.splitBudget(total)).reduce((a:any,b:any)=>a+b,0),total);
  assert.deepEqual(P.splitBudget(500000),{A:200000,B:150000,C:150000});
  for(const bad of [0,-1,NaN,Infinity,2,500.5])assert.throws(()=>P.splitBudget(bad));
  const dir=temp();const request=C.publishStart(dir,500000,{visible:true,pid:123,processName:'DeepSeek Harness'});
  assert.equal(request.totalTokens,500000);assert.throws(()=>C.publishStart(dir,500000,request.window),/EEXIST/);
  assert.equal(P.readJson(join(dir,'request.json')).runId,request.runId);
});

test('阶段三契约：插件重载和单个可见/迟到样本不能证明生命周期通过',()=>{
  const {state}=good();assert.equal(P.realRestart({pid:1,bootAt:10},{pid:1,bootAt:10}),false);
  assert.equal(P.realRestart({pid:1,bootAt:10},{pid:1,bootAt:11}),false);
  assert.equal(P.realRestart(state.beforeRestart.host,state.restartedHost),true);
  assert.equal(P.validateHiddenProof(state,state.hiddenProof),true);
  for(const mutate of [
    (p:any)=>p.samples[1].hidden=false,(p:any)=>p.samples[0].hostAlive=false,(p:any)=>p.samples[0].at=210,
    (p:any)=>p.samples[1].at=180,(p:any)=>p.artifactHash='bad',(p:any)=>p.samples[0].hostPid=999,
  ]){const proof=structuredClone(state.hiddenProof);mutate(proof);assert.equal(P.validateHiddenProof(state,proof),false);}
});

test('阶段三判定器负向对照：缺窗口、假重启、预算重置、重复写、答复缺失、原始证据缺项均失败',()=>{
  const {state,input}=good();assert.equal(P.assessStage3(state,input).passed,true);
  const negatives:[string,(s:any,i:any)=>void][]=[
    ['无窗口',s=>s.hiddenProof=undefined],['只有重载',s=>s.restartedHost=s.beforeRestart.host],['未排队',s=>s.initialQueued=false],
    ['暂停误启',s=>s.pausePreserved=false],['未恢复C',s=>s.recoveredC=false],['答案没消费',s=>s.answerConsumed=false],
    ['答案不同',(_s,i)=>i.answers[0].answer='truncated'],['重复副作用',s=>s.writes.push({...s.writes[0],callId:'duplicate'})],
    ['实际文件错',(_s,i)=>i.artifacts.C.text='wrong'],['旧冻结哈希',(_s,i)=>i.artifacts.A.frozenMatches=false],
    ['预算追加',(_s,i)=>i.budgets.A.grants=[{tokens:1}]],['余额重置',(s,i)=>i.budgets.A.settledTokens=s.beforeRestart.budgets.A.settledTokens-1],
    ['在途未结算',(_s,i)=>i.budgets.A.reservedTokens=1],['raw账本不一致',(_s,i)=>i.rawAudit.sessions[0].matches=false],
    ['raw缺验收',(_s,i)=>i.rawAudit.sessions=i.rawAudit.sessions.filter((s:any)=>s.agentRole!=='acceptor')],
    ['raw答复重复',(_s,i)=>i.rawAudit.answerConsumed=2],['raw后代',(_s,i)=>i.rawAudit.descendants=1],['未交付',(_s,i)=>i.taskStates.B='active'],
    ['未真实纠偏',s=>s.watchdogReceipt=undefined],['跳过真实计时',s=>s.watchdogReceipt.at=10],['无纠偏消费',(_s,i)=>i.rawAudit.watchdogConsumed=0],
    ['复用会话',(_s,i)=>i.rawAudit.sessions[1].sessionId=i.rawAudit.sessions[0].sessionId],
  ];
  for(const [name,mutate]of negatives){const s=structuredClone(state),i=structuredClone(input);mutate(s,i);assert.equal(P.assessStage3(s,i).passed,false,name);}
  state.fixtureVersion=2;assert.equal(P.assessStage3(state,input).passed,false,'宿主闸门版本必须附原始零调用证明');
});

function fakeContext(state:any,runtime:any={ready:true,stage3Protocol:2,maxConcurrentTasks:2}){
  const registered=new Map<string,any>(),guards=new Set<any>(),listeners=new Map<string,any>();let proposed=0;
  const statuses:any=Object.fromEntries(['A','B','C'].map(role=>[`t${role}`,{id:`t${role}`,status:role==='A'?'paused':'active',pauseReason:role==='A'?'user':null}]));
  const budgets:any=good().input.budgets;const paused:string[]=[];
  const api:any={getRuntimeInfo:()=>({...runtime,budgetProtectionVersion:1,pid:process.pid,stallThresholdMs:1000,watchdogIntervalMs:100}),getTask:(id:string)=>statuses[id],getTaskSessions:(id:string)=>[{taskId:id,sessionId:`${id}-sid`,relPath:'worker',role:'executor'}],
    getBudget:(id:string)=>budgets[id.slice(1)],listTasks:()=>[],searchExperts:async(q:string)=>[{relPath:q==='engineering'?'worker':'reviewer'}],
    getTaskEvidence:()=>({}),getClarificationAnswers:()=>[],answerClarification:async()=>({action:'queued'}),releaseQueued:async()=>[],setTaskPriority:async()=>{},applyUserFeedback:async()=>{},
    propose:async()=>{proposed++;return{id:'never'};},pause:async(id:string)=>{paused.push(id);statuses[id].status='paused';statuses[id].pauseReason='user';},cancel:async()=>{}};
  const ctx:any={get:(name:string)=>name==='longtask'?api:name==='tools'?{register:(tool:any)=>{registered.set(tool.name,tool);return()=>registered.delete(tool.name);},guard:(f:any)=>{guards.add(f);return()=>guards.delete(f);}}:undefined,
    on:(event:string,f:any)=>{listeners.set(event,f);return()=>listeners.delete(event);}};
  return {ctx,api,registered,guards,statuses,budgets,paused,get proposed(){return proposed;}};
}

test('阶段三驱动：无请求不运行，预检失败不创建模型任务',async()=>{
  const dir=temp();const f=fakeContext(undefined,{ready:true,stage3Protocol:2,maxConcurrentTasks:1});
  const d=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:join(dir,'artifacts')});
  try{await d.tick();assert.equal(f.proposed,0);assert.equal(f.guards.size,0);
    P.atomicJson(join(dir,'request.json'),{version:P.STAGE3_VERSION,runId,totalTokens:500000,window:{visible:true,pid:1,processName:'DeepSeek Harness'}});
    await d.tick();assert.equal(f.proposed,0);assert.ok(readdirSync(dir).some(n=>n.startsWith('request-rejected-')));
    P.atomicJson(join(dir,'request.json'),{version:'stage3-lifecycle-v1',runId:'22222222-2222-4222-8222-222222222222',totalTokens:500000,window:{visible:true,pid:1,processName:'DeepSeek Harness'}});
    await d.tick();assert.equal(f.proposed,0);assert.ok(readdirSync(dir).filter(n=>n.startsWith('request-rejected-')).map(n=>P.readJson(join(dir,n))).some(r=>r.request.version==='stage3-lifecycle-v1'&&r.reason.includes('无效阶段三运行请求')));
  }finally{d.dispose();}
});

function seed(){
  const {state}=good();state.phase='await-host-restart';state.deadline=Date.now()+60000;state.controller='old';state.events=[];state.eventKeys={};state.modelSteps=[];state.checkpoints=[];state.gates={};
  state.initialHost={pid:process.pid,bootAt:Math.round((Date.now()-process.uptime()*1000)/1000)};state.beforeRestart.host=state.initialHost;
  delete state.restartedHost;state.recoveredB=false;state.recoveredC=false;state.pausePreserved=false;
  delete state.answer;delete state.answerCode;state.answerQueued=false;state.answerConsumed=false;
  state.beforeRestart.sessions={A:['tA-sid'],B:['oldB'],C:['oldC']};return state;
}
test('阶段三驱动：同进程新实例仍留在待重启，工具护栏不掩盖重复写入',async()=>{
  const dir=temp(),s=seed();P.atomicJson(join(dir,'state.json'),s);const f=fakeContext(s);const d=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:dir});
  try{await d.tick();assert.equal(d.inspect().phase,'await-host-restart');assert.equal(d.inspect().restartedHost,undefined);
    const guard=[...f.guards][0];assert.equal(guard({name:'pwsh',agent:{session:{id:'unrelated'}}}),undefined);
    assert.ok(guard({name:'write',agent:{session:{id:'tA-sid'}},arguments:{file_path:join(dir,'state.json')}}));
    const exec={name:'write',agent:{session:{id:'tA-sid'}},arguments:{file_path:s.tasks.A.artifact}};assert.equal(guard(exec),undefined);assert.equal(guard(exec),undefined,'第二次写不能靠guard遮掉');
    await assert.rejects(()=>f.registered.get(P.STAGE3_TOOL).execute({runId,role:'A',stage:'ready'},{agent:{session:{id:'unrelated'}},signal:new AbortController().signal}),/不属于/);
  }finally{d.dispose();}
});

test('阶段三启动回归：显式依赖tools，服务晚于控制器创建时仍注册握手工具',async()=>{
  const entry=await import(new URL('../../../dsh-longtask-e2e/index.js',import.meta.url).href);assert.ok(entry.inject.includes('tools'));
  const dir=temp(),s=seed();P.atomicJson(join(dir,'state.json'),s);const f=fakeContext(s);const get=f.ctx.get;let available=false;
  f.ctx.get=(name:string)=>name==='tools'&&!available?undefined:get(name);
  const d=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:dir});
  try{assert.equal(f.registered.size,0);available=true;await d.tick();assert.ok(f.registered.has(P.STAGE3_TOOL));assert.equal(f.guards.size,1);assert.equal(d.inspect().phase,'await-host-restart');}
  finally{d.dispose();assert.equal(f.registered.size,0);assert.equal(f.guards.size,0);}
});

test('宿主模型闸门：C等调度/重启不调用模型，B恢复等待可取消，无关会话放行',async()=>{
  const dir=temp(),s=seed();s.fixtureVersion=2;P.atomicJson(join(dir,'state.json'),s);
  const f=fakeContext(s);let entered=0;const listeners=new Map<string,any>();
  f.ctx.on=(name:string,fn:any)=>{listeners.set(name,fn);return()=>listeners.delete(name);};
  const active=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:dir});const pending:Promise<any>[]=[];
  const until=async(p:()=>boolean)=>{for(let n=0;n<200;n++){if(p())return;await new Promise(r=>setTimeout(r,5));}throw Error('gate not observed');};
  try{
    const before=listeners.get('agent/pre-step');
    await before({agent:{session:{id:'unrelated'}}},()=>{entered++;return{kind:'enter'};});assert.equal(entered,1);
    const c=before({agent:{session:{id:'tC-sid'}}},()=>{entered++;return{kind:'enter'};});pending.push(c);
    await until(()=>active.inspect().checkpoints.some((x:any)=>x.role==='C'&&x.stage==='pre-model'));
    assert.equal(entered,1);assert.equal(f.budgets.C.settledTokens,1000);assert.equal(f.budgets.C.reservedTokens,0);
    active.inspect().gates['C.model']=true;assert.equal((await c).kind,'enter');assert.equal(entered,2);
    active.inspect().beforeRestart.host={pid:process.pid+1,bootAt:0};
    const b=before({agent:{session:{id:'tB-sid'}}},()=>{entered++;return{kind:'enter'};});pending.push(b);
    await until(()=>active.inspect().checkpoints.some((x:any)=>x.role==='B'&&x.stage==='pre-model'));
    f.statuses.tB.status='paused';assert.equal((await b).kind,'reject');assert.equal(entered,2);assert.equal(f.budgets.B.settledTokens,1000);
  }finally{active.dispose();await Promise.all(pending);}
});

function toolFailure(){
  const dir=temp(),s=seed();const at=Date.now();s.beforeRestart.host={pid:process.pid+1000,bootAt:s.initialHost.bootAt-10};s.initialHost=s.beforeRestart.host;
  s.beforeRestart.at=at-100;s.phase='failed';s.failure='B需要人工核对，联调不盲目继续';s.events=[{kind:'failed',at}];
  s.writes=s.writes.filter((w:any)=>w.role==='A');for(const role of ['A','B','C'])s.tasks[role].artifact=join(dir,`${role}.md`);
  writeFileSync(s.tasks.A.artifact,P.expectedBody(s,'A'));P.atomicJson(join(dir,'state.json'),s);
  const f=fakeContext(s);const get=f.ctx.get;f.ctx.get=(name:string)=>name==='tools'?{...get(name),get:(name:string)=>f.registered.get(name)}:get(name);
  const views:any={};const resumed:string[]=[];
  for(const role of ['A','B','C']){
    f.statuses[`t${role}`]={id:`t${role}`,status:'paused',pauseReason:'user'};s.beforeRestart.budgets[role].settledTokens=f.budgets[role].settledTokens;
    views[`t${role}`]={events:{root:{type:'task.paused',createdAt:new Date(at-20).toISOString(),payload:{reason:'missing_input',note:`必需工具尚未装载：${P.STAGE3_TOOL}；未发起模型请求，请装载后恢复`}},
      failed:{type:'task.paused',createdAt:new Date(at+10).toISOString(),payload:{reason:'user',note:`阶段三联调停止：${s.failure}`}}}};
  }
  f.api.getTaskEvidence=(id:string)=>views[id];f.api.resume=async(id:string)=>{resumed.push(id);f.statuses[id]={id,status:'active',pauseReason:null};};
  P.atomicJson(join(dir,'state.json'),s);return{dir,s,f,views,resumed};
}

test('阶段三工具故障恢复：原失败归档，仅恢复B/C，原任务原用量且只允许一次',async()=>{
  const {dir,s,f,resumed}=toolFailure();const d=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:dir});
  try{
    const control=P.publishControl(dir,s,'recover-tools');assert.equal(d.claimed(),true);await d.tick();
    const ack=P.readJson(join(dir,'acks',`${control.id}.json`));assert.equal(ack.ok,true,ack.reason);assert.deepEqual(resumed,['tB','tC']);assert.equal(f.statuses.tA.status,'paused');assert.equal(f.proposed,0);
    const failed=P.readJson(join(dir,`failure-before-tool-recovery-${runId}.json`));assert.equal(failed.phase,'failed');assert.equal(failed.failure,s.failure);
    assert.equal(d.inspect().runId,s.runId);assert.deepEqual(d.inspect().shares,s.shares);assert.equal(d.inspect().phase,'await-host-restart');assert.ok(d.inspect().toolRecoveryAttemptedAt);
    assert.equal(existsSync(join(dir,`done-${runId}.json`)),false);
    d.inspect().phase='failed';d.inspect().failure=s.failure;const duplicate=P.publishControl(dir,d.inspect(),'recover-tools');await d.tick();assert.equal(P.readJson(join(dir,'acks',`${duplicate.id}.json`)).ok,false);assert.equal(resumed.length,2);
  }finally{d.dispose();}
});

test('阶段三工具故障恢复负向对照：费用变化、原因不符、产物变化、假重启均不得复活',async()=>{
  for(const kind of ['budget','reason','artifact','failure','same-pid']){
    const {dir,s,f,views,resumed}=toolFailure();
    if(kind==='budget')f.budgets.B.settledTokens++;
    if(kind==='reason')views.tC.events.root.payload.note='未知投递，需要对账';
    if(kind==='artifact')writeFileSync(s.tasks.B.artifact,'unauthorized');
    if(kind==='failure')s.failure='其他失败';
    if(kind==='same-pid')s.beforeRestart.host.pid=process.pid;
    P.atomicJson(join(dir,'state.json'),s);const d=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:dir});
    try{const command=P.publishControl(dir,s,'recover-tools');await d.tick();assert.equal(P.readJson(join(dir,'acks',`${command.id}.json`)).ok,false,kind);assert.deepEqual(resumed,[]);assert.equal(d.inspect().phase,'failed');assert.equal(existsSync(join(dir,`failure-before-tool-recovery-${runId}.json`)),false);}
    finally{d.dispose();}
  }
});

test('阶段三驱动离线编排：并行→排队→受控写入→暂停优先→持久重启检查点，重载不能越关',async()=>{
  const dir=temp(),statuses:any={},budgets:any={},leases:any={},registered=new Map<string,any>(),listeners=new Map<string,any>(),guards=new Set<any>();
  let count=0;const controllers:AbortController[]=[];const waits:Promise<any>[]=[];
  const api:any={getRuntimeInfo:()=>({ready:true,stage3Protocol:2,budgetProtectionVersion:1,maxConcurrentTasks:2,pid:process.pid,stallThresholdMs:1000,watchdogIntervalMs:100}),listTasks:()=>Object.values(statuses),searchExperts:async(q:string)=>[{relPath:q==='engineering'?'worker':'reviewer'}],
    propose:async(input:any)=>{const role=['A','B','C'][count++]!,id=`t${role}`;statuses[id]={id,status:'proposed',pauseReason:null};budgets[id]={limitTokens:input.budget.limitTokens,settledTokens:1000,reservedTokens:0,grants:[]};leases[id]=[];return statuses[id];},
    confirm:async(id:string)=>{if(Object.values(statuses).filter((t:any)=>t.status==='active').length>=2){statuses[id].status='paused';statuses[id].pauseReason='queued';}else{statuses[id].status='active';leases[id]=[{taskId:id,sessionId:`${id}-sid`,relPath:'worker',role:'executor'}];}},
    getTask:(id:string)=>statuses[id],getBudget:(id:string)=>budgets[id],getTaskSessions:(id:string)=>leases[id],getTaskEvidence:(id:string)=>({collections:{progress:{[id]:{lastSubstantiveAt:new Date().toISOString()}}}}),getClarificationAnswers:()=>[],answerClarification:async()=>({action:'queued'}),applyUserFeedback:async()=>{},
    pause:async(id:string)=>{statuses[id].status='paused';statuses[id].pauseReason='user';},resume:async()=>{},cancel:async()=>{},setTaskPriority:async()=>{},
    releaseQueued:async()=>{statuses.tC.status='active';statuses.tC.pauseReason=null;leases.tC=[{taskId:'tC',sessionId:'tC-sid',relPath:'worker',role:'executor'}];return['tC'];}};
  const ctx:any={get:(name:string)=>name==='longtask'?api:name==='tools'?{register:(t:any)=>{registered.set(t.name,t);return()=>registered.delete(t.name);},guard:(f:any)=>{guards.add(f);return()=>guards.delete(f);}}
    :name==='sessions'?{flush:async()=>true}:name==='agents'?{get:(id:string)=>({session:{id}})}:undefined,
    on:(name:string,f:any)=>{listeners.set(name,f);return()=>listeners.delete(name);}};
  let driver=D.createStage3Driver(ctx,{directory:dir,artifactDir:join(dir,'artifacts')});
  const until=async(predicate:()=>boolean)=>{for(let i=0;i<200;i++){if(predicate())return;await new Promise(r=>setTimeout(r,5));}throw Error('offline checkpoint timeout');};
  const enter=(role:string,stage:string)=>{const c=new AbortController();controllers.push(c);const p=registered.get(P.STAGE3_TOOL).execute({runId,role,stage},{agent:{session:{id:`t${role}-sid`}},callId:`checkpoint-${role}-${stage}`,signal:c.signal});waits.push(p.catch(()=>{}));return p;};
  try{
    P.atomicJson(join(dir,'request.json'),{version:P.STAGE3_VERSION,runId,totalTokens:500000,window:{visible:true,pid:123,processName:'DeepSeek Harness'}});await driver.tick();
    assert.equal(count,3);assert.equal(statuses.tC.pauseReason,'queued');
    const ready=enter('A','ready');enter('B','ready');await until(()=>driver.inspect().checkpoints.length===2);await driver.tick();assert.equal(driver.inspect().phase,'await-hidden');
    const first={at:Date.now(),hidden:true,hostAlive:true,hostPid:process.pid,processName:'DeepSeek Harness'};
    P.publishControl(dir,driver.inspect(),'hidden-release',{sample:first});await driver.tick();await ready;
    const path=driver.inspect().tasks.A.artifact;writeFileSync(path,P.expectedBody(driver.inspect(),'A'));
    const onEvent=listeners.get('session/event');onEvent({id:'tA-sid'},{type:'tool/call',data:{name:'write',callId:'real-write-A',arguments:JSON.stringify({file_path:path})}});
    onEvent({id:'tA-sid'},{type:'tool/result',data:{message:{toolCallId:'real-write-A',isError:false}}});enter('A','written');
    await until(()=>driver.inspect().writes.length===1&&driver.inspect().checkpoints.length===3);await driver.tick();assert.equal(driver.inspect().phase,'await-hidden-proof');
    const last={...first,at:Math.max(Date.now(),driver.inspect().writes[0].at+1)};
    P.publishControl(dir,driver.inspect(),'hidden-proof',{proof:{runId,samples:[first,last],artifactHash:driver.inspect().writes[0].hash}});await driver.tick();
    P.publishControl(dir,driver.inspect(),'prepare-restart');await driver.tick();assert.equal(statuses.tA.pauseReason,'user');assert.equal(statuses.tC.status,'active');
    const c= listeners.get('agent/pre-step')({agent:{session:{id:'tC-sid'}}},()=>({kind:'enter'}));waits.push(c.catch(()=>{}));
    await until(()=>driver.inspect().checkpoints.length===4);await driver.tick();assert.equal(driver.inspect().phase,'await-host-restart');
    driver.dispose();for(const c of controllers)c.abort();await Promise.all(waits);
    driver=D.createStage3Driver(ctx,{directory:dir,artifactDir:join(dir,'artifacts')});await driver.tick();assert.equal(driver.inspect().phase,'await-host-restart');assert.equal(existsSync(join(dir,`done-${runId}.json`)),false);
  }finally{driver.dispose();for(const c of controllers)c.abort();await Promise.all(waits);}
});

test('阶段三离线模拟恢复输入：先验证主动暂停，再排队保存完整答复，工具不泄露口径码',async()=>{
  const dir=temp(),s=seed();s.beforeRestart.host={pid:process.pid+1000,bootAt:s.initialHost.bootAt-10};s.initialHost=s.beforeRestart.host;
  s.tasks.A.artifact=join(dir,'A.md');writeFileSync(s.tasks.A.artifact,P.expectedBody(s,'A'));P.atomicJson(join(dir,'state.json'),s);
  const f=fakeContext(s);const answers:any[]=[];
  f.api.resume=async(id:string)=>{f.statuses[id].status='active';f.statuses[id].pauseReason=null;};
  f.api.applyUserFeedback=async(id:string)=>{f.statuses[id].status='paused';f.statuses[id].pauseReason='missing_input';};
  f.api.getClarificationAnswers=()=>answers;
  f.api.answerClarification=async(id:string,answer:string)=>{
    assert.equal(Object.values(f.statuses).filter((t:any)=>t.status==='active').length,2);
    answers.push({id:'full-answer',answer,delivery:'pending'});f.statuses[id].status='paused';f.statuses[id].pauseReason='queued';return{action:'queued'};
  };
  const d=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:dir});const pending:Promise<any>[]=[];const aborts:AbortController[]=[];
  const enter=(role:string,stage:string)=>{const c=new AbortController();aborts.push(c);const p=f.registered.get(P.STAGE3_TOOL).execute({runId,role,stage},{agent:{session:{id:`t${role}-sid`}},callId:`${role}-${stage}`,signal:c.signal});pending.push(p.catch(()=>{}));return p;};
  const until=async(p:()=>boolean)=>{for(let n=0;n<200;n++){if(p())return;await new Promise(r=>setTimeout(r,5));}throw Error('offline flow timeout');};
  try{
    const b=enter('B','ready');enter('C','ready');await until(()=>d.inspect().checkpoints.length===2);await d.tick();assert.equal(d.inspect().phase,'recovered-ready');assert.equal(d.inspect().pausePreserved,true);
    P.publishControl(dir,d.inspect(),'continue');await d.tick();assert.equal(d.inspect().phase,'feedback-wait-A');
    const written=enter('A','written');await until(()=>d.inspect().checkpoints.length===3);await d.tick();assert.equal(d.inspect().phase,'finishing');assert.equal(f.statuses.tB.pauseReason,'queued');
    const checked=JSON.parse(await written);assert.equal(checked.verifiedArtifact.content,P.expectedBody(s,'A'));assert.equal(checked.verifiedArtifact.sha256,P.sha(P.expectedBody(s,'A')));assert.match(checked.action,/不再read或write/);
    assert.equal(answers[0].answer,d.inspect().answer);assert.ok(answers[0].answer.length>120);
    const response=await b;assert.ok(!response.includes(d.inspect().answerCode),'口径码只能来自真实用户答复，不能从握手工具拿到');
    assert.equal(existsSync(join(dir,`done-${runId}.json`)),false,'本离线构造输入不生成真机通过标记');
  }finally{d.dispose();for(const c of aborts)c.abort();await Promise.all(pending);}
});

test('阶段三驱动：额度被改变时只暂停本轮任务，不追加、不操作其他任务',async()=>{
  const dir=temp(),s=seed();s.phase='finishing';P.atomicJson(join(dir,'state.json'),s);const f=fakeContext(s);f.budgets.B.grants=[{tokens:1}];
  const d=D.createStage3Driver(f.ctx,{directory:dir,artifactDir:dir});
  try{await d.tick();assert.equal(d.inspect().phase,'failed');assert.deepEqual(f.paused.sort(),['tA','tB','tC']);assert.equal(f.proposed,0);assert.equal(existsSync(join(dir,`done-${runId}.json`)),false);}finally{d.dispose();}
});

test('阶段三原始审计：跨会话逐帧解码、真实答复和写入次数与账本一致',()=>{
  const {state,input}=good();const root=temp(),out=join(root,'audit'),evidence:any={};
  for(const role of ['A','B','C']){
    const taskId=state.tasks[role].id;state.tasks[role].artifact=join(root,`${role}.md`);const leases:any={},ledger:any={};
    for(const kind of ['executor','acceptor']){
      const sid=`${role}-${kind}`;leases[sid]={taskId,role:kind,sessionId:sid};ledger[sid]={taskId,note:sid,inputTokens:500,outputTokens:0};
      const dir=join(root,'sessions','workspace',sid);mkdirSync(dir,{recursive:true});
      const events:any[]=[{seq:1,type:'assistant/message',data:{usage:{inputTokens:500,cacheReadTokens:0,cacheWriteTokens:0,outputTokens:0}}}];
      if(kind==='executor'){
        if(role==='B')events.push({seq:2,type:'user/message',data:{id:'user-answer',source:{dedupeKey:'clarify-answer:answer1'},content:[{type:'text',text:state.answer}]}});
        events.push({seq:3,type:'tool/call',data:{name:'write',callId:`write-${role}`,arguments:JSON.stringify({file_path:state.tasks[role].artifact})}},
          {seq:4,time:200,type:'tool/result',data:{message:{toolCallId:`write-${role}`,isError:false}}});
        if(role==='C'){
          events.length=0;
          events.push({seq:1,type:'assistant/message',data:{turn:1,usage:{inputTokens:250,cacheReadTokens:0,cacheWriteTokens:0,outputTokens:0},message:{content:[{type:'text',text:'<longtask_report>{"done":false,"next":[],"waiting":"STAGE3_WATCHDOG_EXTERNAL"}</longtask_report>'}]}}},
            {seq:2,time:160,type:'turn/end',data:{turn:1,reason:{kind:'completed'}}},
            {seq:3,time:180,type:'user/message',data:{source:{dedupeKey:`watchdog:${taskId}:correct`},content:[{type:'text',text:'【Watchdog 纠偏】'}]}},
            {seq:4,type:'assistant/message',data:{turn:2,usage:{inputTokens:250,cacheReadTokens:0,cacheWriteTokens:0,outputTokens:0}}},
            {seq:5,type:'tool/call',data:{name:'write',callId:'write-C',arguments:JSON.stringify({file_path:state.tasks.C.artifact})}},
            {seq:6,time:200,type:'tool/result',data:{message:{toolCallId:'write-C',isError:false}}});
        }
      }
      const header=zlib.zstdCompressSync(Buffer.from(JSON.stringify({header:true})+'\n'));
      const body=zlib.zstdCompressSync(Buffer.from(events.map(e=>JSON.stringify(e)).join('\n')+'\n'));
      writeFileSync(join(dir,'session.v4.jsonl.zstd'),Buffer.concat([header,body]));
    }
    evidence[role]={collections:{taskSessions:{[taskId]:leases},ledger,budgets:{[taskId]:input.budgets[role]},clarificationAnswers:role==='B'?{answer1:input.answers[0]}:{}}};
  }
  const audit=A.auditStage3(state,evidence,out,join(root,'sessions'));assert.equal(audit.verified,true);assert.equal(audit.actualTokens,3000);assert.equal(audit.sessions.length,6);assert.equal(audit.answerConsumed,1);
  state.fixtureVersion=2;state.modelGateReleasedAt=150;state.beforeRestart.sessions={A:['A-executor'],B:['B-old'],C:['C-old']};
  state.checkpoints=[{role:'C',sessionId:'C-old',stage:'pre-model',kind:'host-pre-step',host:{pid:1},at:120},
    ...['B','C'].map(role=>({role,sessionId:`${role}-executor`,stage:'pre-model',kind:'host-pre-step',host:{pid:2},at:140}))];
  for(const role of ['B','C']){
    const taskId=state.tasks[role].id,old=`${role}-old`,dir=join(root,'sessions','workspace',old);mkdirSync(dir,{recursive:true});
    evidence[role].collections.taskSessions[taskId][old]={taskId,sessionId:old,role:'executor'};
    writeFileSync(join(dir,'session.v4.jsonl.zstd'),zlib.zstdCompressSync(Buffer.from(JSON.stringify({header:true})+'\n')));
    const path=join(root,'sessions','workspace',`${role}-executor`,'session.v4.jsonl.zstd');
    const events=A.decodeSession(readFileSync(path));for(const e of events)if(e.type==='assistant/message')e.time=e.data.turn===1?155:200;
    writeFileSync(path,zlib.zstdCompressSync(Buffer.from(events.map((e:any)=>JSON.stringify(e)).join('\n')+'\n')));
  }
  assert.equal(A.auditStage3(state,evidence,out,join(root,'sessions')).gateWaitsVerified,true);
  const bPath=join(root,'sessions','workspace','B-executor','session.v4.jsonl.zstd'),bEvents=A.decodeSession(readFileSync(bPath));
  bEvents.find((e:any)=>e.type==='assistant/message').time=149;
  writeFileSync(bPath,zlib.zstdCompressSync(Buffer.from(bEvents.map((e:any)=>JSON.stringify(e)).join('\n')+'\n')));
  const early=A.auditStage3(state,evidence,out,join(root,'sessions'));assert.equal(early.gateWaitsVerified,false);assert.equal(early.verified,false,'提前调用模型不得用闸门元数据蒙混通过');
  evidence.A.collections.ledger['A-executor'].inputTokens=499;
  assert.equal(A.auditStage3(state,evidence,out,join(root,'sessions')).verified,false);
});
