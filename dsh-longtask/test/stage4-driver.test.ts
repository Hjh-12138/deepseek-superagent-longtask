import {prepareSdkTools, loadSdk} from "./sdk-tools.js";
import {test} from 'node:test';import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,existsSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';import * as zlib from 'node:zlib';
import {pathToFileURL} from 'node:url';
const D=await import(new URL('../../../dsh-longtask-e2e/stage4-driver.mjs',import.meta.url).href);
const P=await import(new URL('../../../dsh-longtask-e2e/stage4-protocol.mjs',import.meta.url).href);
const V=await import(new URL('../../../dsh-longtask-e2e/stage4-validation.mjs',import.meta.url).href);
const A=await import(new URL('../../../dsh-longtask-e2e/stage4-audit.mjs',import.meta.url).href);
const R=await import(new URL('../../../dsh-longtask-e2e/request-run.mjs',import.meta.url).href);
const E=await import(new URL('../../../dsh-longtask-e2e/evidence.mjs',import.meta.url).href);
const S=await import(new URL('../../../dsh-longtask-e2e/stage3-protocol.mjs',import.meta.url).href);
const entry=await import(new URL('../../../dsh-longtask-e2e/index.js',import.meta.url).href);
const CLI=await import(new URL('../../../dsh-longtask-e2e/request-stage4.mjs',import.meta.url).href);
const sleep=(n:number)=>new Promise(r=>setTimeout(r,n));const temp=()=>mkdtempSync(join(tmpdir(),'lt-stage4-'));
async function until(fn:()=>boolean){for(let n=0;n<300;n++){if(fn())return;await sleep(10)}throw Error('stage4 timeout');}
const doc='# 问题\n三人单机需要任务状态持久化。\n# 三种方案\nJSON简单，SQLite支持事务，外部数据库支持共享。\n# 取舍\n维护成本和事务都需要考虑。\n# 结论\n选择SQLite。\n1. 本地事务。\n2. 不增加服务维护。';
const validParser=`export function parseRange(text){if(typeof text!=='string'||!text||!/^\\d+(?:-\\d+)?(?:,\\d+(?:-\\d+)?)*$/.test(text))throw Error('bad');return text.split(',').flatMap(s=>{const [a,b=a]=s.split('-').map(Number);if(a>b)throw Error('descending');return Array.from({length:b-a+1},(_,i)=>a+i)})}`;
const validTests=`import assert from 'node:assert/strict';import {parseRange} from './parse-range.mjs';assert.deepStrictEqual(parseRange('1-3,7,9-10'),[1,2,3,7,9,10]);assert.deepStrictEqual(parseRange('7'),[7]);assert.throws(()=>parseRange('bad'));`;
test('阶段四解析：固定预算、版本与故障检查点先验证，超长定时器和多余字段不落请求',()=>{
  for(const crash of [{afterMs:2147483648},{at:'bad'},{timeoutMs:0},{at:'after-write',other:1}])assert.throws(()=>P.normalizeCrash(crash));
  assert.deepEqual(P.normalizeCrash({at:'after-write',afterMs:20000}),{at:'after-write',timeoutMs:20000});
  const dir=join(temp(),'not-created');assert.throws(()=>R.publishRunRequest({scenario:'doc',budgetTokens:200000,crash:{afterMs:2147483648}},dir));assert.equal(existsSync(dir),false);
  const result=R.publishRunRequest({scenario:'doc',budgetTokens:200000,crash:{at:'model-inflight',timeoutMs:10000}},temp());assert.equal(result.request.stage4Version,P.STAGE4_VERSION);assert.equal(result.request.crash.at,'model-inflight');
  assert.throws(()=>E.parseRunRequest({scenario:'dev',budgetTokens:200000}));assert.throws(()=>R.publishRunRequest({scenario:'pair',budgetTokens:200000,crash:{afterMs:20}},temp()));
});
test('阶段四功能测试：正确模块实际运行，自写断言不足/错误实现/非法导入均拒绝',async()=>{
  const dir=temp(),parser=join(dir,'parse-range.mjs'),tests=join(dir,'parse-range.test.mjs');writeFileSync(parser,validParser);writeFileSync(tests,validTests);
  const good=await V.validateDevelopment(parser,tests);assert.equal(good.passed,true,good.error);assert.equal(good.fixedCases,9);assert.equal(good.workerAssertions,3);
  writeFileSync(parser,`export function parseRange(text){if(typeof text!=='string')throw Error();text.split(',');text.split('-');return []}`);
  writeFileSync(tests,`import assert from 'node:assert/strict';import{parseRange}from'./parse-range.mjs';assert.ok(Array.isArray(parseRange('7')));assert.ok(true);assert.ok(true);`);
  assert.equal((await V.validateDevelopment(parser,tests)).passed,false,'固定输入必须抓住永远返回[]');
  writeFileSync(parser,validParser);writeFileSync(tests,`import fs from 'node:fs';fs.writeFileSync('escape','x');`);assert.equal((await V.validateDevelopment(parser,tests)).passed,false);assert.equal(existsSync(join(dir,'escape')),false);
  writeFileSync(tests,`import assert from 'node:assert/strict';assert.ok(true);`);assert.equal((await V.validateDevelopment(parser,tests)).passed,false);
  writeFileSync(parser,`JSON.stringify=()=> 'same';export function parseRange(x){if(typeof x!=='string'||!x||x==='bad'||x==='3-1'||x==='1,,2')throw Error();return []}`);writeFileSync(tests,`import assert from 'node:assert/strict';assert.ok(true);assert.ok(true);assert.ok(true);`);assert.equal((await V.validateDevelopment(parser,tests)).passed,false,'篡改VM比较器不能改固定预期值');
});
test('阶段四文稿：明确主题和结构，漏节与缺理由拒绝',()=>{const p=join(temp(),'note.md');writeFileSync(p,doc);assert.equal(V.validateDocument(p).passed,true);writeFileSync(p,doc.replace('# 取舍','取舍'));assert.equal(V.validateDocument(p).passed,false);});

function harness(){
  const directory=join(temp(),'stage4'),artifactDir=join(temp(),'artifacts'),rawRoot=join(temp(),'sessions');mkdirSync(rawRoot);
  const data:any={collections:{tasks:{},budgets:{},taskSessions:{},ledger:{},reservations:{},artifacts:{},clarificationAnswers:{},messages:{}},events:{}};
  const agents=new Map<string,any>(),listeners=new Map<string,any>(),tools=new Map<string,any>(),guards=new Set<any>();let count=0,proposals=0,exits=0,pid=100;
  const api:any={getRuntimeInfo:()=>({ready:true,budgetProtectionVersion:1}),listTasks:()=>Object.values(data.collections.tasks),searchExperts:async(q:string)=>[{relPath:q==='engineering'?'worker':'reviewer'}],
    propose:async(input:any)=>{const id='t'+(++proposals);data.collections.tasks[id]={id,status:'proposed',pauseReason:null,versions:[input],currentVersion:0};data.collections.budgets[id]={taskId:id,limitTokens:input.budget.limitTokens,settledTokens:0,reservedTokens:0,strict:true,grants:[]};data.collections.taskSessions[id]={};return data.collections.tasks[id];},
    confirm:async(id:string)=>{data.collections.tasks[id].status='active';addSession(id);},getTask:(id:string)=>data.collections.tasks[id],getBudget:(id:string)=>data.collections.budgets[id],getTaskSessions:(id:string)=>Object.values(data.collections.taskSessions[id]??{}),
    getTaskEvidence:()=>structuredClone(data),pause:async(id:string)=>{data.collections.tasks[id].status='paused';data.collections.tasks[id].pauseReason='user';},
    registerArtifact:async(id:string,path:string,hash:string,owner:string)=>{data.collections.artifacts[path]={id:path,taskId:id,location:path,contentHash:hash,ownerRelPath:owner};},
    applyUserFeedback:async(id:string)=>{data.collections.tasks[id].status='paused';data.collections.tasks[id].pauseReason='missing_input';},
    getClarificationAnswers:(id:string)=>Object.values(data.collections.clarificationAnswers).filter((x:any)=>x.taskId===id),
    answerClarification:async(id:string,answer:string)=>{const a:any={id:'answer',taskId:id,answer,delivery:'unknown'};data.collections.clarificationAnswers.answer=a;data.collections.tasks[id].status='active';const agent=[...agents.values()].at(-1)!;
      try{agent.followup({id:'msg',role:'user',source:{dedupeKey:'clarify-answer:answer'},content:[{type:'text',text:answer}]});a.delivery='delivered';}catch{data.collections.tasks[id].status='paused';data.collections.tasks[id].pauseReason='missing_input';}return{action:'pending'};},
  };
  function addSession(id:string,role='executor'){const sid='s'+(++count),lease={taskId:id,sessionId:sid,sessionKey:sid,relPath:role==='executor'?'worker':'reviewer',role};data.collections.taskSessions[id][sid]=lease;
    const agent={session:{id:sid},followup:(_m:any)=>{},cancel:()=>{}};agents.set(sid,agent);raw(sid,[]);return agent;}
  function raw(sid:string,events:any[]){const dir=join(rawRoot,'workspace',sid);mkdirSync(dir,{recursive:true});writeFileSync(join(dir,'session.v4.jsonl.zstd'),zlib.zstdCompressSync(Buffer.from([{header:true},...events].map(x=>JSON.stringify(x)).join('\n')+'\n')));}
  const ctx:any={get:(name:string)=>name==='longtask'?api:name==='agents'?{get:(sid:string)=>agents.get(sid)}:name==='sessions'?{flush:async()=>true}:name==='tools'?{register:(t:any)=>{tools.set(t.name,t);return()=>tools.delete(t.name)},guard:(g:any)=>{guards.add(g);return()=>guards.delete(g)}}:undefined,
    on:(name:string,fn:any)=>{listeners.set(name,fn);return()=>listeners.delete(name)},logger:{info:()=>{},warn:()=>{}},effect:(_fn:any)=>{}};
  const create=()=>D.createStage4Driver(ctx,{directory,artifactDir,sessionRoot:rawRoot,settleMs:0,exitProcess:()=>{exits++},hostIdentity:()=>({pid,bootAt:pid})});
  const request=(crash?:any)=>({stage4Version:P.STAGE4_VERSION,requestId:crypto.randomUUID(),scenario:'doc',budgetTokens:200000,strict:true,...(crash?{crash}: {})});
  function usage(id:string,sid:string,tokens:number,key=sid){data.collections.ledger[key]={taskId:id,attemptId:key,kind:'attempt',inputTokens:tokens,outputTokens:0,note:`会话 ${sid} 实际用量`};data.collections.budgets[id].settledTokens+=tokens;}
  function delivered(s:any){const id=s.taskId;const acceptor=addSession(id,'acceptor');usage(id,acceptor.session.id,100);raw(acceptor.session.id,[sample(100)]);const hashes:any={};
    for(const [n,path] of s.paths.entries()){const hash=S.sha(readFileSync(path)),key='art'+n;data.collections.artifacts[key]={id:key,taskId:id,location:path,contentHash:hash};hashes[key]=hash;}data.collections.tasks[id].status='delivered';
    data.events.done={taskId:id,type:'task.delivered',payload:{artifactHashes:hashes,verdicts:['c1','c2'].map(criterionId=>({criterionId,outcome:'pass',verdictBy:'reviewer',evidence:'实际核对'}))}};}
  return{directory,artifactDir,rawRoot,data,api,ctx,agents,listeners,tools,guards,create,request,addSession,raw,usage,delivered,setPid:(n:number)=>{pid=n},get proposals(){return proposals},get exits(){return exits}};
}
const sample=(tokens:number,step=1)=>({seq:1,type:'assistant/message',time:200,data:{turn:1,step,usage:{inputTokens:tokens,cacheReadTokens:0,cacheWriteTokens:0,outputTokens:0}}});
const writeEvents=(path:string,callId='w1')=>[{seq:2,type:'tool/call',time:201,data:{turn:1,step:1,name:'write',callId,arguments:JSON.stringify({file_path:path})}},{seq:3,type:'tool/result',time:202,data:{message:{toolCallId:callId,isError:false}}}];

test('阶段四显式入口：未确认不安排退出；未命中检查点只失败，卸载后不迟到退出',async()=>{
  const f=harness(),d=f.create();try{d.enqueue(f.request({at:'model-inflight',timeoutMs:10}));await d.tick();assert.equal(d.inspect().phase,'running');await sleep(35);assert.equal(f.exits,0);assert.equal(d.inspect().phase,'failed');assert.equal(f.api.getTask(d.inspect().taskId).pauseReason,'user');}finally{await d.dispose();}
  const other=harness(),driver=other.create();driver.enqueue(other.request({at:'after-write',timeoutMs:40}));await driver.tick();await driver.dispose();await sleep(70);assert.equal(other.exits,0);assert.equal(existsSync(join(other.directory,'../crash-pending.json')),false);
});

test('阶段四在途故障：真实流首块前后绑定预留，换PID后逐笔保守对账并接回原任务',async()=>{
  const f=harness();let d=f.create();
  try{
    d.enqueue(f.request({at:'model-inflight',timeoutMs:10000}));await d.tick();const s=d.inspect(),sid=[...f.agents.keys()][0]!;
    f.data.collections.reservations.r1={id:'r1',taskId:s.taskId,tokens:40000,state:'reserved'};f.data.collections.budgets[s.taskId].reservedTokens=40000;
    f.listeners.get('session/event')({id:sid},{seq:1,type:'step/start',data:{turn:1,step:1}});
    const stream=f.listeners.get('llm/stream')({sessionId:sid},async function*(){yield{type:'text-delta',text:'真实源首块'}});
    await assert.rejects(()=>stream[Symbol.asyncIterator]().next(),/退出函数已返回/);assert.equal(f.exits,1);assert.equal(s.phase,'awaiting-restart');assert.equal(s.crashEvidence.before.reservations[0].id,'r1');
    await d.tick();assert.equal(d.inspect().phase,'awaiting-restart','同PID重载不算恢复');await d.dispose();
    f.data.collections.reservations.r1.state='orphaned';f.data.collections.budgets[s.taskId].reservedTokens=0;f.data.collections.budgets[s.taskId].settledTokens=40000;
    f.data.collections.ledger.adjust={taskId:s.taskId,attemptId:'r1',kind:'adjustment',inputTokens:40000,outputTokens:0,note:'恢复保守记账'};
    f.agents.clear();f.setPid(200);const newer=f.addSession(s.taskId);d=f.create();await d.tick();assert.equal(d.inspect().phase,'running');assert.equal(d.inspect().recovery.reservationsReconciled,true);assert.equal(f.proposals,1);
    mkdirSync(f.artifactDir,{recursive:true});writeFileSync(s.paths[0],doc);f.usage(s.taskId,newer.session.id,1000);f.raw(newer.session.id,[sample(1000),...writeEvents(s.paths[0])]);f.delivered(s);
    await d.tick();assert.equal(d.inspect().phase,'passed',d.inspect().failure);const result=S.readJson(join(f.directory,`result-${s.runId}.json`));assert.equal(result.accounting.knownTokens,1100);assert.equal(result.accounting.conservativeTokens,40000);assert.equal(result.coverage.chargedTokens,41100);assert.equal(f.exits,1);
  }finally{await d.dispose();}
});

test('阶段四写后崩溃：旧文件保留，恢复重写同文件即失败，未重写则通过',async()=>{
  for(const duplicate of [false,true]){
    const f=harness();let d=f.create();
    try{
      d.enqueue(f.request({at:'after-write',timeoutMs:10000}));await d.tick();const s=d.inspect(),sid=[...f.agents.keys()][0]!;writeFileSync(s.paths[0],doc);f.usage(s.taskId,sid,1000);f.raw(sid,[sample(1000),...writeEvents(s.paths[0])]);
      for(const e of writeEvents(s.paths[0]))f.listeners.get('session/event')({id:sid},e);await until(()=>d.inspect().phase==='awaiting-restart');assert.equal(f.exits,1);await d.dispose();
      f.agents.clear();f.setPid(200);const next=f.addSession(s.taskId);d=f.create();await d.tick();assert.equal(d.inspect().recovery.artifactPreserved,true);
      f.usage(s.taskId,next.session.id,1000);f.raw(next.session.id,[sample(1000),...(duplicate?writeEvents(s.paths[0],'duplicate'):[])]);f.delivered(s);await d.tick();
      assert.equal(d.inspect().phase,duplicate?'failed':'passed');assert.equal(f.proposals,1);
    }finally{await d.dispose();}
  }
});

test('阶段四未知投递：实际调用入队后丢确认，重启保持missing_input且不重投',async()=>{
  const f=harness();let d=f.create();const waits:Promise<any>[]=[];
  try{
    d.enqueue(f.request({at:'delivery-unknown',timeoutMs:10000}));await d.tick();const s=d.inspect(),agent=[...f.agents.values()][0]!;let actualAccepted=0;agent.followup=()=>{actualAccepted++};
    const wait=f.listeners.get('agent/pre-step')({agent},()=>{throw Error('未知投递不得调用模型')});waits.push(wait);await until(()=>d.inspect().holdReady);
    await d.tick();await wait;assert.equal(actualAccepted,1);assert.equal(f.exits,1);assert.equal(d.inspect().crashEvidence.unknown.acceptedBeforeAckLoss,true);await d.dispose();
    f.agents.clear();f.setPid(200);d=f.create();await d.tick();assert.equal(d.inspect().phase,'recovery-quiet');await d.tick();assert.equal(d.inspect().phase,'passed');
    assert.equal(d.inspect().coverage.outcome,'safely-paused');assert.equal(f.api.getTask(s.taskId).pauseReason,'missing_input');assert.equal(f.proposals,1);assert.equal(actualAccepted,1);
  }finally{await d.dispose();await Promise.all(waits);}
});

test('阶段四原始对账负例：费用复位/重复调整/缺日志不能通过',async()=>{
  const f=harness(),d=f.create();try{
    d.enqueue(f.request());await d.tick();const s=d.inspect(),sid=[...f.agents.keys()][0]!;writeFileSync(s.paths[0],doc);f.usage(s.taskId,sid,1000);f.raw(sid,[sample(1000),...writeEvents(s.paths[0])]);f.delivered(s);
    const good=f.api.getTaskEvidence();assert.equal(A.auditStage4(s,good,join(temp(),'audit'),f.rawRoot).verified,true);
    good.collections.budgets[s.taskId].settledTokens=0;assert.equal(A.auditStage4(s,good,join(temp(),'audit'),f.rawRoot).verified,false);
    const bad=f.api.getTaskEvidence();bad.collections.ledger.extra={taskId:s.taskId,attemptId:'not-captured',kind:'adjustment',inputTokens:1000,outputTokens:0};assert.equal(A.auditStage4(s,bad,join(temp(),'audit'),f.rawRoot).verified,false);
    assert.throws(()=>A.auditStage4(s,bad,join(temp(),'audit'),temp()),/缺少原始会话/);
  }finally{await d.dispose();}
});

test('阶段四真正apply请求入口传递crash，非法请求不创建任务；超时关闭本轮任务',async()=>{
  const f=harness(),base=join(temp(),'e2e'),stops:any[]=[];mkdirSync(base);const originalEffect=f.ctx.effect;f.ctx.effect=(fn:any)=>{const it=fn();const first=it.next();if(typeof first.value==='function')stops.push(first.value)};
  try{
    R.publishRunRequest({scenario:'doc',budgetTokens:200000,crash:{at:'model-inflight',timeoutMs:30000}},base);
    entry.apply(f.ctx,{transcriptDir:base,artifactDir:f.artifactDir,pollIntervalMs:5,timeoutMs:100});
    await until(()=>f.proposals===1);const statePath=join(base,'stage4/state.json');assert.equal(S.readJson(statePath).crash.at,'model-inflight');
    await until(()=>S.readJson(statePath).phase==='failed');assert.equal(f.api.getTask('t1').status,'paused');assert.equal(f.exits,0);
  }finally{for(const stop of stops)await stop();f.ctx.effect=originalEffect;}
});

test('阶段四开发工具全链：写源码、实际测试回执、独立验收冻结与最终重跑',async()=>{
  for(const tamper of [false,true]){
    const f=harness(),d=f.create();try{
      d.enqueue({...f.request(),scenario:'dev'});await d.tick();const s=d.inspect(),agent=[...f.agents.values()][0]!;
      writeFileSync(s.paths[0],validParser);writeFileSync(s.paths[1],validTests);
      const tool=f.tools.get(P.STAGE4_TOOL);const validation=JSON.parse(await tool.execute({runId:s.runId},{agent}));assert.equal(validation.passed,true);
      assert.equal(S.readJson(s.receipt).fixedCases,9);const guard=[...f.guards][0];
      assert.ok(guard({agent,name:'write',arguments:{file_path:s.receipt.toUpperCase()}}),'回执大小写绕过仍拒绝');
      assert.ok(guard({agent,name:'read',arguments:{file_path:join(f.directory,'state.json')}}));
      assert.equal(guard({agent,name:'write',arguments:{file_path:s.paths[0]}}),undefined);
      if(tamper){const receipt=S.readJson(s.receipt);receipt.runId='other-run';S.atomicJson(s.receipt,receipt);}
      f.usage(s.taskId,agent.session.id,1000);f.raw(agent.session.id,[sample(1000)]);f.delivered(s);await d.tick();
      assert.equal(d.inspect().phase,tamper?'failed':'passed');
      const result=S.readJson(join(f.directory,`result-${s.runId}.json`));assert.equal(result.validation.receiptVerified,!tamper);assert.equal(result.artifacts.length,3);
    }finally{await d.dispose();}
  }
});

test('阶段四判定负例：只delivered、漏裁决、伪验收、无原始验收及旧哈希均不通过',async()=>{
  const f=harness(),d=f.create();try{
    d.enqueue(f.request());await d.tick();const s=d.inspect(),sid=[...f.agents.keys()][0]!;writeFileSync(s.paths[0],doc);f.usage(s.taskId,sid,1000);f.raw(sid,[sample(1000)]);f.delivered(s);
    const evidence=f.api.getTaskEvidence(),accounting=A.auditStage4(s,evidence,join(temp(),'audit'),f.rawRoot);
    const base={state:s,evidence,artifacts:[{frozenMatches:true}],accounting,validation:{passed:true}};
    assert.equal(P.assessStage4(base).passed,true);
    for(const mutate of [(v:any)=>v.artifacts=[],(v:any)=>v.artifacts[0].frozenMatches=false,(v:any)=>v.evidence.events.done.payload.verdicts.pop(),
      (v:any)=>v.evidence.events.done.payload.verdicts[0].verdictBy='worker',(v:any)=>v.accounting.sessions=v.accounting.sessions.filter((x:any)=>x.role!=='acceptor'),(v:any)=>v.validation.passed=false]){
      const bad=structuredClone(base);mutate(bad);assert.equal(P.assessStage4(bad).passed,false);
    }
    f.raw(sid,[{seq:1,type:'assistant/message',data:{turn:1,step:1}}]);assert.throws(()=>A.auditStage4(s,evidence,join(temp(),'audit'),f.rawRoot),/缺少usage/);
  }finally{await d.dispose();}
});

test('阶段四未确认与其他任务隔离：确认挂起不armed，不关联其他会话流',async()=>{
  const f=harness(),d=f.create();let release:any;f.api.confirm=async()=>new Promise(r=>{release=r});
  try{
    d.enqueue(f.request({at:'model-inflight',timeoutMs:10}));const pending=d.tick();await until(()=>d.inspect()?.phase==='proposed'&&!!release);
    await sleep(35);assert.equal(f.exits,0);assert.equal(d.inspect().phase,'proposed');
    let calls=0;const stream=f.listeners.get('llm/stream')({sessionId:'unrelated'},async function*(){calls++;yield{type:'text-delta'}});
    await stream[Symbol.asyncIterator]().next();assert.equal(calls,1);assert.equal(f.exits,0);release();await pending;
  }finally{await d.dispose();}
});

test('阶段四恢复负例：已用回退、预留重复结算、unknown自动恢复或答复变更均保留现场失败',async()=>{
  for(const change of ['rollback','extra-adjustment','auto-resume','answer-change']){
    const f=harness();let d=f.create();
    try{
      if(change==='rollback'||change==='extra-adjustment'){
        d.enqueue(f.request({at:'model-inflight',timeoutMs:10000}));await d.tick();const s=d.inspect(),sid=[...f.agents.keys()][0]!;f.usage(s.taskId,sid,500);f.raw(sid,[sample(500,0)]);
        f.data.collections.reservations.r={id:'r',taskId:s.taskId,tokens:1000,state:'reserved'};f.data.collections.budgets[s.taskId].reservedTokens=1000;
        f.listeners.get('session/event')({id:sid},{type:'step/start',data:{turn:1,step:1}});
        await assert.rejects(()=>f.listeners.get('llm/stream')({sessionId:sid},async function*(){yield{type:'text-delta'}})[Symbol.asyncIterator]().next());await d.dispose();
        const b=f.data.collections.budgets[s.taskId];b.reservedTokens=0;b.settledTokens=change==='rollback'?0:1500;f.data.collections.reservations.r.state='orphaned';
        f.data.collections.ledger.a={taskId:s.taskId,attemptId:'r',kind:'adjustment',inputTokens:1000,outputTokens:0};
        if(change==='extra-adjustment')f.data.collections.ledger.b={...f.data.collections.ledger.a};
      }else{
        d.enqueue(f.request({at:'delivery-unknown',timeoutMs:10000}));await d.tick();const agent=[...f.agents.values()][0]!;
        const pending=f.listeners.get('agent/pre-step')({agent},()=>{throw Error('不得调用模型')});await until(()=>d.inspect().holdReady);await d.tick();await pending;await d.dispose();
        if(change==='auto-resume')f.data.collections.tasks[d.inspect().taskId].status='active';else f.data.collections.clarificationAnswers.answer.answer='changed';
      }
      f.agents.clear();f.setPid(200);d=f.create();await d.tick();assert.equal(d.inspect().phase,'failed',change);assert.equal(existsSync(join(f.directory,'../crash-pending.json')),true);assert.equal(f.proposals,1);
    }finally{await d.dispose();}
  }
});

test('阶段四unknown重投检测读取真实嵌套message.source，不按空日志冒充无重复',async()=>{
  const f=harness(),d=f.create();try{
    d.enqueue(f.request());await d.tick();const s=d.inspect(),sid=[...f.agents.keys()][0]!;s.crashEvidence={unknown:{dedupeKey:'clarify-answer:a'},before:{reservations:[]}};
    f.raw(sid,[1,2].map(seq=>({seq,type:'user/message',data:{message:{source:{dedupeKey:'clarify-answer:a'}}}})));
    assert.equal(A.auditStage4(s,f.api.getTaskEvidence(),join(temp(),'audit'),f.rawRoot).unknownConsumptions,2);
  }finally{await d.dispose();}
});

test('阶段四CLI只发布显式请求；停止撤销检查点、pending阻止混跑',async()=>{
  const dir=temp();const result=CLI.stage4Command(['crash','after-write','200000'],dir);const req=S.readJson(result.path);assert.equal(req.crash.at,'after-write');assert.equal(req.crash.timeoutMs,300000);
  assert.equal(CLI.stage4Command(['status'],dir).pending.requestId,req.requestId);assert.throws(()=>CLI.stage4Command(['doc','200000'],dir));
  const f=harness(),d=f.create();try{
    d.enqueue(f.request({at:'after-write',timeoutMs:10000}));await d.tick();CLI.stage4Command(['cancel'],join(f.directory,'..'));await d.tick();assert.equal(d.inspect().phase,'failed');assert.equal(f.exits,0);assert.equal(f.api.getTask(d.inspect().taskId).status,'paused');
    S.atomicJson(join(f.directory,'request.json'),{});assert.throws(()=>R.publishRunRequest({scenario:'basic',budgetTokens:200000},join(f.directory,'..')),/待执行请求/);
  }finally{await d.dispose();}
});

test('阶段四真实SDK ToolRuntime：工具可装配执行、回执和验收写入被拦、卸载移除',async()=>{
  const {Context}=await import('@deepseek-ai/cordis');
  const {ToolRuntime}=await loadSdk('dsh-tools');
  const root=new Context();root.provide('systemPrompt',{tools(){}});const runtime=await prepareSdkTools(root,new ToolRuntime(root)),f=harness();
  const get=f.ctx.get;f.ctx.get=(name:string)=>name==='tools'?runtime:get(name);f.ctx.on=(name:string,fn:any)=>(root as any).on(name,fn);
  const d=f.create();try{
    d.enqueue({...f.request(),scenario:'dev'});await d.tick();const s=d.inspect(),agent=[...f.agents.values()][0]!;writeFileSync(s.paths[0],validParser);writeFileSync(s.paths[1],validTests);
    assert.ok(runtime.schemas(agent).some((x:any)=>x.name===P.STAGE4_TOOL));
    const request={name:P.STAGE4_TOOL,callId:'validate',arguments:{runId:s.runId},agent,signal:new AbortController().signal};
    const result=await runtime.execute(request);assert.equal(result.isError,false,JSON.stringify(result));assert.equal(JSON.parse(result.value).fixedCases,9);
    let writes=0;runtime.register({name:'write',parameters:{type:'object',properties:{file_path:{type:'string'}}},output:{schema:{type:'string'},render:(_a:any,v:string)=>[{type:'text',text:v}]},execute:async()=>{writes++;return'ok'}});
    const acceptor=f.addSession(s.taskId,'acceptor');
    assert.equal((await runtime.execute({...request,name:'write',arguments:{file_path:s.receipt}})).isError,true);
    assert.equal((await runtime.execute({...request,name:'write',arguments:{file_path:s.paths[0]},agent:acceptor})).isError,true);assert.equal(writes,0);
    assert.equal((await runtime.execute({...request,name:'write',arguments:{file_path:'user-file'},agent:{session:{id:'user'},id:'user'}})).isError,false);assert.equal(writes,1);
    await d.dispose();assert.equal(runtime.schemas(agent).some((x:any)=>x.name===P.STAGE4_TOOL),false);
  }finally{await d.dispose();await root.fiber.dispose();}
});

test('阶段四真实apply拒绝非法故障请求，不创建任务或安排退出',async()=>{
  const f=harness(),base=temp(),stops:any[]=[];f.ctx.effect=(fn:any)=>{const it=fn(),x=it.next();if(typeof x.value==='function')stops.push(x.value)};
  try{
    writeFileSync(join(base,'run-request.json'),JSON.stringify({...f.request({at:'model-inflight',timeoutMs:2147483648})}));
    entry.apply(f.ctx,{transcriptDir:base,artifactDir:f.artifactDir});await until(()=>!existsSync(join(base,'run-request.json')));assert.equal(f.proposals,0);assert.equal(f.exits,0);
  }finally{for(const stop of stops)await stop();}
});

test('文稿首步拿到实际不存在事实与工具说明，恢复拿到存在事实，拒绝重复预置产物',async()=>{
  const f=harness();let d=f.create();try{
    d.enqueue(f.request());await d.tick();const s=d.inspect(),agent=[...f.agents.values()][0]!;
    const goal=f.api.getTask(s.taskId).versions[0].goal;assert.match(goal,/消费者已核对.*不存在/);assert.match(goal,/pwsh.*不可用/);
    const first=await f.listeners.get('agent/pre-step')({agent,messages:[]},()=>({kind:'enter',messages:[]}));
    const text=first.messages.map((m:any)=>m.content.map((b:any)=>b.text).join('')).join('');assert.match(text,/不存在.*直接.*write/);
    assert.doesNotMatch(text,/先read/);assert.equal(f.api.getBudget(s.taskId).settledTokens,0);
    writeFileSync(s.paths[0],doc);await d.dispose();f.api.getTask(s.taskId).status='active';const next=f.addSession(s.taskId);d=f.create();
    const resumed=await f.listeners.get('agent/pre-step')({agent:next,messages:[]},()=>({kind:'enter',messages:[]}));
    const resumeText=resumed.messages.map((m:any)=>m.content.map((b:any)=>b.text).join('')).join('');assert.match(resumeText,/已存在/);assert.match(resumeText,/read.*一次/);assert.match(resumeText,/不.*write/);
  }finally{await d.dispose();}
  const other=harness(),driver=other.create(),request=other.request(),path=join(other.artifactDir,`stage4-${request.requestId}`,'research-note.md');
  mkdirSync(join(other.artifactDir,`stage4-${request.requestId}`),{recursive:true});writeFileSync(path,'必须保留');
  try{driver.enqueue(request);await driver.tick();assert.equal(other.proposals,0);assert.equal(readFileSync(path,'utf8'),'必须保留');}finally{await driver.dispose();}
});

test('文稿真实SDK写入回执：自动登记真实哈希，下一步直接报告；错误/阻断/他人调用不冒充完成',async()=>{
  const {Context}=await import('@deepseek-ai/cordis');
  const {ToolRuntime}=await loadSdk('dsh-tools');
  const root=new Context();root.provide('systemPrompt',{tools(){}});const runtime=await prepareSdkTools(root,new ToolRuntime(root)),f=harness(),get=f.ctx.get;
  f.ctx.get=(name:string)=>name==='tools'?runtime:get(name);f.ctx.on=(name:string,fn:any)=>(root as any).on(name,fn);
  const d=f.create();let blocked=false;const stop=(root as any).on('tools/post-execute',async(_e:any,_r:any,next:any)=>blocked?{kind:'block',feedback:[{type:'text',text:'真实下游阻断'}]}:next());
  try{
    d.enqueue(f.request());await d.tick();const s=d.inspect(),agent=[...f.agents.values()][0]!;let bodyCalls=0;
    runtime.register({name:'write',parameters:{type:'object',properties:{file_path:{type:'string'},content:{type:'string'}}},output:{schema:{type:'string'},render:(_a:any,v:string)=>[{type:'text',text:v}]},execute:async(a:any)=>{bodyCalls++;if(a.content==='fail')throw Error('failed write');writeFileSync(a.file_path,a.content);return'Created file'}});
    const request={name:'write',callId:'write-once',arguments:{file_path:s.paths[0],content:doc},agent,signal:new AbortController().signal};
    const result=await runtime.execute(request);assert.equal(result.isError,false);const notices=result.additionalContexts??[];assert.equal(notices.length,1);
    const text=notices[0].content.map((b:any)=>b.text).join('');assert.match(text,/结构检查通过/);assert.match(text,/直接.*longtask_report/);assert.match(text,/不.*read/);
    assert.ok(Object.values(f.data.collections.artifacts).some((a:any)=>a.contentHash===S.sha(readFileSync(s.paths[0]))));assert.equal(f.api.getTask(s.taskId).status,'active','回执不能代替独立验收');
    assert.equal(bodyCalls,1);assert.equal(f.api.getBudget(s.taskId).limitTokens,200000);assert.equal(f.api.getBudget(s.taskId).settledTokens,0);
    const failed=await runtime.execute({...request,callId:'failed',arguments:{...request.arguments,content:'fail'}});assert.equal(failed.isError,true);assert.equal(failed.additionalContexts?.length??0,0);
    const invalid=await runtime.execute({...request,callId:'invalid',arguments:{...request.arguments,content:'bad'}});assert.equal(invalid.isError,false);assert.match(invalid.additionalContexts[0].content[0].text,/未通过/);assert.doesNotMatch(invalid.additionalContexts[0].content[0].text,/<longtask_report>/);
    blocked=true;const denied=await runtime.execute({...request,callId:'blocked'});assert.equal(denied.isError,true);assert.equal(denied.additionalContexts?.length??0,0);blocked=false;
    const foreign=await runtime.execute({...request,callId:'foreign',agent:{id:'user',session:{id:'user'}}});assert.equal(foreign.additionalContexts?.length??0,0);
  }finally{stop();await d.dispose();await root.fiber.dispose();}
});

test('文稿存在性提示经过真实Cordis waterfall上游预算计量，已核对文件不重复探测',async()=>{
  const {Context}=await import('@deepseek-ai/cordis');const root=new Context(),f=harness();let counted='';
  const stop=(root as any).on('agent/pre-step',async(_payload:any,next:any)=>{const proposed=await next();counted=JSON.stringify(proposed.messages);return proposed});
  f.ctx.on=(name:string,fn:any)=>(root as any).on(name,fn);const d=f.create();
  try{
    d.enqueue(f.request());await d.tick();const s=d.inspect(),agent=[...f.agents.values()][0]!;
    const first=await (root as any).waterfall(agent,'agent/pre-step',{agent,messages:[]},async()=>({kind:'enter',messages:[]}));
    assert.match(counted,/不存在/);assert.equal(first.messages.length,1);
    writeFileSync(s.paths[0],doc);await(root as any).waterfall(agent,'tools/post-execute',{agent,name:'write',callId:'checked',arguments:{file_path:s.paths[0]}},{isError:false},async()=>({kind:'accept'}));
    await d.dispose();f.api.getTask(s.taskId).status='active';const reopened=f.create();try{
      const enter=await(root as any).waterfall(agent,'agent/pre-step',{agent,messages:[]},async()=>({kind:'enter',messages:[]}));
      assert.match(JSON.stringify(enter.messages),/已核对本会话/);assert.match(counted,/不再read或write/);
    }finally{await reopened.dispose();}
  }finally{stop();await d.dispose();await root.fiber.dispose();}
});
