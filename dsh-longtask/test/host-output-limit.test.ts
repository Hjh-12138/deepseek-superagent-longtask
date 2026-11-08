import {prepareSdkTools,loadSdk} from "./sdk-tools.js";
import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,mkdirSync,writeFileSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {apply} from "../src/index.js";
import {DEFAULT_PRICES} from "../src/ledger/money.js";
import {priceBound} from "../src/ledger/money.js";
const [{Context,Service},{AgentRegistry},{SessionStore},{SessionProjectionRegistry},{SystemPrompt},{ToolRuntime},{AgentLoop},{LlmRuntime,LlmAdapter},retry]=await Promise.all(
  ['cordis','dsh-agent','dsh-session','dsh-session-projection','dsh-system-prompt','dsh-tools','dsh-agent-loop','dsh-llm','dsh-llm-retry'].map(loadSdk));
const wait=async(fn:()=>boolean)=>{for(let i=0;i<500;i++){if(fn())return;await new Promise(r=>setTimeout(r,10))}throw Error('真实宿主上限测试超时')};

async function fixture(limits:Array<number|undefined>,budget:number|string=500000,requestedLimit?:number,boundary?:"foreign-sdk"|"linked-signal",acceptanceReserveTokens=0){
  const root=new Context(),dir=mkdtempSync(join(tmpdir(),'host-output-')),events:any[]=[],calls:any[]=[],errors:any[]=[];
  mkdirSync(join(dir,'experts/research'),{recursive:true});writeFileSync(join(dir,'experts/divisions.json'),JSON.stringify({divisions:{research:{label:'Research'}}}));
  for(const n of ['worker','reviewer'])writeFileSync(join(dir,'experts/research',n+'.md'),`---\nname: ${n}\ndescription: test\n---\nfixture`);
  new SessionStore(root);new SessionProjectionRegistry(root);const agents=new AgentRegistry(root);if(typeof agents[Service.init]==='function')await agents[Service.init]();
  new SystemPrompt(root,{includeHarnessIdentity:false,includeRuntimeContext:false});await prepareSdkTools(root,new ToolRuntime(root));
  const llm=new LlmRuntime(root);if(typeof llm[Service.init]==='function')await llm[Service.init]();
  if(boundary){
    const original=llm.prepareCall.bind(llm);
    const foreign=boundary==='foreign-sdk'?await import(import.meta.resolve('@deepseek-ai/dsh-llm')+'?isolated-peer-sdk'):undefined;
    llm.prepareCall=async(...args:any[])=>{const prepared=await original(...args);return {...prepared,stream:(request:any)=>{
      const copy=Object.freeze({...request,...(boundary==='linked-signal'?{signal:AbortSignal.any([request.signal])}:{})});
      return prepared.stream(foreign?foreign.markAgentLoopRequest(copy):copy);
    }}};
  }
  let api:any;
  class Adapter extends LlmAdapter {
    providerInfo(provider:string){return {id:provider,name:'Offline'}}
    providerRetryPolicy(){return {mode:'normal',maxRetries:1,retryableCodes:['TRANSPORT'],initialDelayMs:0,maxDelayMs:0,jitterRatio:0}}
    async resolveModel(provider:string,id:string){const limit=limits[Math.min(calls.length,limits.length-1)];return {provider,id,name:id,...(limit===undefined?{}:{defaultMaxTokens:limit})}}
    async *stream(request:any){
      const evidence=api.getTaskEvidence(task.id),b=api.getBudget(task.id),r=Object.values(evidence.collections.reservations??{}).find((r:any)=>r.state==='reserved') as any;
      calls.push({request,reservedTokens:b.reservedTokens,reservedAmount:b.money?.reservedNano,reservation:r});
      yield {type:'usage',usage:{inputTokens:1,outputTokens:4,cacheReadTokens:0,cacheWriteTokens:0}};
      yield {type:'block-start',index:0,blockType:'text'};
      yield {type:'block-end',index:0,block:{type:'text',text:'<longtask_report>{"done":false,"next":[],"waiting":"等待外部结果"}</longtask_report>'}};
      yield {type:'finish',reason:limits.length>1&&calls.length===1?{kind:'error',failure:{code:'TRANSPORT',message:'offline retry'}}:{kind:'stop'}};
    }
  }
  llm.registerAdapter(['offline'],new Adapter());root.on('session/event',(_s:any,e:any)=>events.push(e));root.on('agent/error',(e:any)=>errors.push(e));
  if(requestedLimit!==undefined)root.on('agent/request',async(_payload:any,next:any)=>({...await next(),maxTokens:requestedLimit}));
  const loop=new AgentLoop(root,{agents:[],maxParallelToolCalls:1});if(typeof loop[Service.init]==='function')await loop[Service.init]();retry.apply(root,{});
  apply(root,{dataDir:dir,agencyAgentsRoot:join(dir,'experts'),workerCwd:dir,workerPreset:'',workerParentAgent:'none',workerProvider:'offline',workerModel:'offline',probeOnStart:false,watchdogIntervalMs:3600000,
    modelPrices:DEFAULT_PRICES.map(p=>({...p,provider:'offline',model:'offline',verifiedAt:'2020-01-01',expiresAt:'2099-01-01'})),acceptanceReserveTokens,wrapUpAtBudgetRatio:0,wrapUpMinStepsLeft:0});
  await wait(()=>Boolean(root.get('longtask')?.getRuntimeInfo().ready));api=root.get('longtask');
  const expert=(relPath:string)=>({relPath,division:'research',name:relPath,intro:'test',contentHash:'fixture',snapshottedAt:new Date().toISOString()});
  const task=await api.propose({goal:'核对请求上限',criteria:[{id:'c1',text:'完成'}],deliverables:[join(dir,'unused.md')],executors:[expert('research/worker.md')],acceptor:expert('research/reviewer.md'),
    budget:{limitTokens:typeof budget==='number'?budget:Number.MAX_SAFE_INTEGER,strict:true,...(typeof budget==='string'?{money:{currency:'CNY',limitAmount:budget}}:{})}});
  await api.confirm(task.id);
  return {root,llm,api,task,calls,events,errors,async finish(){await wait(()=>events.some(e=>e.type==='turn/end')||api.getTask(task.id).status==='paused');await wait(()=>api.getTask(task.id).status==='paused'||api.getBudget(task.id).reservedTokens===0)},async close(){await root.fiber.dispose()}};
}

for(const limit of [1024,32000])test(`DSH默认输出${limit}原样传递，供应商收到请求前Token已预留`,async()=>{
  const f=await fixture([limit]);try{await f.finish();assert.equal(f.calls.length,1);assert.equal(f.calls[0].request.maxTokens,limit);assert.ok(f.calls[0].reservedTokens>=limit);assert.equal(f.api.getBudget(f.task.id).settledTokens,5);}finally{await f.close()}
});
test('DSH大上限无法准入时零供应商请求，不按较小默认值放行',async()=>{
  const f=await fixture([64000],45000);try{await f.finish();assert.equal(f.calls.length,0);assert.equal(f.api.getTask(f.task.id).pauseReason,'budget');assert.equal(f.api.getBudget(f.task.id).reservedTokens,0);assert.equal(f.api.getBudget(f.task.id).settledTokens,0);}finally{await f.close()}
});
for(const invalid of [undefined,0,-1,NaN,Infinity,1.5])test(`DSH上限${invalid}无效时暂停，不伪造默认输出额度`,async()=>{
  const f=await fixture([invalid]);try{await f.finish();assert.equal(f.calls.length,0);assert.equal(f.api.getTask(f.task.id).status,'paused');assert.equal(f.api.getBudget(f.task.id).reservedTokens,0);}finally{await f.close()}
});
for(const limit of [4096,64000])test(`DSH请求钩子指定${limit}时保留实际值，不以模型默认覆盖`,async()=>{
  const f=await fixture([32000],500000,limit);try{await f.finish();assert.equal(f.calls.length,1);assert.equal(f.calls[0].request.maxTokens,limit);assert.ok(f.calls[0].reservedTokens>=limit);}finally{await f.close()}
});
test('DSH重试上限变化时每次按新上限预留金额和Token',async()=>{
  const f=await fixture([2048,32768],'10');try{await f.finish();assert.equal(f.calls.length,2);assert.deepEqual(f.calls.map(c=>c.request.maxTokens),[2048,32768]);
    for(const c of f.calls){const input=c.reservedTokens-c.request.maxTokens;assert.ok(input>=0);assert.equal(c.reservedAmount,priceBound(c.reservation.money.price,input,c.request.maxTokens).toString());}
    assert.ok(BigInt(f.calls[1].reservedAmount)>BigInt(f.calls[0].reservedAmount));assert.equal(f.api.getBudget(f.task.id).reservedTokens,0);assert.equal(f.api.getBudget(f.task.id).money.reservedNano,'0');assert.equal(f.api.getModelExpenses(f.task.id).length,2);}finally{await f.close()}
});
test('实际上限对应金额不足时释放未发出预留，零供应商请求',async()=>{
  const f=await fixture([32000],'0.1');try{await f.finish();assert.equal(f.calls.length,0);assert.equal(f.api.getTask(f.task.id).pauseReason,'budget');assert.equal(f.api.getBudget(f.task.id).reservedTokens,0);assert.equal(f.api.getBudget(f.task.id).money.reservedNano,'0');assert.equal(f.api.getBudget(f.task.id).settledTokens,0);}finally{await f.close()}
});
test('重试上限增大而金额不足时拒发第二次，不把未发出的尝试记成费用未知',async()=>{
  const f=await fixture([2048,32768],'0.1');try{await f.finish();assert.equal(f.calls.length,1);assert.equal(f.api.getTask(f.task.id).pauseReason,'budget');assert.equal(f.api.getBudget(f.task.id).reservedTokens,0);assert.equal(f.api.getBudget(f.task.id).money.reservedNano,'0');assert.equal(f.api.getTaskEvidence(f.task.id).collections.accountingIssues?.[f.task.id],undefined);assert.equal(f.api.getBudget(f.task.id).settledTokens,5);}finally{await f.close()}
});
test('普通DSH会话仍走原流，不写入Worker任务预算',async()=>{
  const f=await fixture([32000]);try{await f.finish();const before=structuredClone(f.api.getBudget(f.task.id));const prepared=await f.llm.prepareCall({provider:'offline',model:'offline'});for await(const _ of prepared.stream({...prepared.config,messages:[],sessionId:'unmanaged',signal:new AbortController().signal})){}assert.equal(f.calls.at(-1).request.maxTokens,32000);assert.deepEqual(f.api.getBudget(f.task.id),before);}finally{await f.close()}
});
for(const boundary of ['foreign-sdk','linked-signal'] as const)test(`合法请求跨${boundary}边界不应被对象身份误拦，仍须足额预留`,async()=>{
  const f=await fixture([256000],'1000',undefined,boundary);try{await f.finish();assert.equal(f.calls.length,1);assert.equal(f.calls[0].request.maxTokens,256000);assert.ok(f.calls[0].reservedTokens>=256000);assert.equal(f.api.getBudget(f.task.id).settledTokens,5);assert.equal(f.api.getBudget(f.task.id).money.reservedNano,'0');}finally{await f.close()}
});
test('移除私有身份判断后，缺少请求准备的Worker调用仍被拒发',async()=>{
  const f=await fixture([32000]);try{await f.finish();const sessionId=f.calls[0].request.sessionId;
    const prepared=await f.llm.prepareCall({provider:'offline',model:'offline'});
    await assert.rejects(async()=>{for await(const _ of prepared.stream({...prepared.config,messages:[],sessionId,signal:new AbortController().signal})){}},/准入准备/);
    assert.equal(f.calls.length,1);assert.equal(f.api.getBudget(f.task.id).settledTokens,5);assert.equal(f.api.getBudget(f.task.id).reservedTokens,0);assert.equal(f.api.getTask(f.task.id).pauseReason,'missing_input');
  }finally{await f.close()}
});

test("256k宿主上限与默认验收保护在8元内正常准入，仍足额预留当前真实请求",async()=>{
  const f=await fixture([256000],"8",undefined,undefined,26000);try{
    await f.finish();assert.equal(f.calls.length,1);
    assert.equal(f.calls[0].request.maxTokens,256000);
    assert.ok(f.calls[0].reservedTokens>=256000);
    assert.equal(f.api.getMoneyBudget(f.task.id).limitAmount,"8");
  }finally{await f.close()}
});
