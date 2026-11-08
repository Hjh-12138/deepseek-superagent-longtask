import {prepareSdkTools} from "./sdk-tools.js";
import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,mkdirSync,writeFileSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {pathToFileURL} from "node:url";
import {apply} from "../src/index.js";

// 使用目标宿主原版loop/retry/session/scope；只把供应商stream换成确定性离线响应。
const mod=async(name:string)=>import("@deepseek-ai/"+name);
const [{Context},{AgentRegistry},{SessionStore},{SessionProjectionRegistry},{SystemPrompt},{ToolRuntime},{AgentLoop},retry]=await Promise.all([
  mod("cordis"),mod("dsh-agent"),mod("dsh-session"),mod("dsh-session-projection"),mod("dsh-system-prompt"),mod("dsh-tools"),mod("dsh-agent-loop"),mod("dsh-llm-retry"),
]);
const delay=(ms:number)=>new Promise(r=>setTimeout(r,ms));

for (const unknownUsage of [false,true]) test(unknownUsage ? "原版SDK loop+retry：失败响应缺usage时只发一次请求并持久暂停（离线stream）" : "原版SDK loop+retry：同step两次供应商尝试分别准入和计费（离线stream，无网络）",async()=>{
  const root=new Context(),dir=mkdtempSync(join(tmpdir(),"longtask-real-sdk-")),events:any[]=[],errors:any[]=[],requests:any[]=[];
  mkdirSync(join(dir,'experts/research'),{recursive:true});writeFileSync(join(dir,'experts/divisions.json'),JSON.stringify({divisions:{research:{label:'Research'}}}));
  for(const n of ['worker','reviewer'])writeFileSync(join(dir,'experts/research',n+'.md'),`---\nname: ${n}\ndescription: test\n---\nfixture`);
  new SessionStore(root);new SessionProjectionRegistry(root);const registry=new AgentRegistry(root);const {Service}=await mod("cordis");if(typeof registry[Service.init]==="function")await registry[Service.init]();new SystemPrompt(root,{includeHarnessIdentity:false,includeRuntimeContext:false});await prepareSdkTools(root,new ToolRuntime(root));
  root.on('agent/error',(e:any)=>errors.push(e));root.on('session/event',(_s:any,e:any)=>events.push(e));
  let calls=0;
  root.provide('llm',{async prepareCall(config:any){return {config,adapterDefaults:{},retryPolicy:{mode:'normal',maxRetries:1,retryableCodes:['TRANSPORT'],initialDelayMs:0,maxDelayMs:0,jitterRatio:0},
    async *stream(request:any){calls++;requests.push(request);if(calls>2)throw Error('unexpected extra request');
      if(!unknownUsage)yield {type:'usage',usage:{inputTokens:calls===1?7:9,outputTokens:4,cacheReadTokens:0,cacheWriteTokens:0}};
      yield {type:'finish',reason:calls===1?{kind:'error',failure:{code:'TRANSPORT',message:'offline deterministic failure'}}:{kind:'stop'}};
    }}}});
  const loop=new AgentLoop(root,{agents:[],maxParallelToolCalls:1});if(typeof loop[Service.init]==="function")await loop[Service.init]();retry.apply(root,{});
  apply(root,{workerMaxTokens:16000,dataDir:dir,agencyAgentsRoot:join(dir,'experts'),workerCwd:dir,workerPreset:'',workerParentAgent:'none',workerProvider:'offline',workerModel:'offline',probeOnStart:false,watchdogIntervalMs:3600000,acceptanceReserveTokens:0,wrapUpAtBudgetRatio:0,wrapUpMinStepsLeft:0});
  try{
    for(let i=0;i<500&&!root.get('longtask')?.getRuntimeInfo().ready;i++)await delay(10);const api=root.get('longtask');assert.ok(api?.getRuntimeInfo().ready);
    const e=(relPath:string)=>({relPath,division:'research',name:relPath,intro:'test',contentHash:'fixture',snapshottedAt:new Date().toISOString()});
    const task=await api.propose({goal:'只核对SDK重试记账',criteria:[{id:'c1',text:'完成'}],deliverables:[join(dir,'unused.md')],executors:[e('research/worker.md')],acceptor:e('research/reviewer.md'),budget:{limitTokens:200000,strict:true}});await api.confirm(task.id);
    for(let i=0;i<500&&!events.some(e=>e.type==='turn/end');i++)await delay(10);
    if(unknownUsage){
      for(let i=0;i<500&&api.getTask(task.id).status!=='paused';i++)await delay(10);
      assert.equal(calls,1);assert.equal(api.getTask(task.id).pauseReason,'budget');assert.ok(api.getBudget(task.id).reservedTokens>0);assert.equal(api.getBudget(task.id).settledTokens,0);
      assert.ok(api.getTaskEvidence(task.id).collections.accountingIssues[task.id]);await assert.rejects(()=>api.resume(task.id),/用量|核对/);return;
    }
    for(let i=0;i<500&&api.getBudget(task.id).reservedTokens;i++)await delay(10);
    assert.equal(calls,2,JSON.stringify(errors));assert.equal(events.filter(e=>e.type==='step/start').length,1);assert.equal(events.filter(e=>e.type==='llm/retry-started').length,1);
    assert.equal(events.filter(e=>e.type==='assistant/attempt').length,1);assert.equal(events.filter(e=>e.type==='assistant/message').length,1);
    assert.equal(api.getBudget(task.id).settledTokens,24);assert.equal(api.getBudget(task.id).reservedTokens,0);
    const ledger=Object.values(api.getTaskEvidence(task.id).collections.ledger) as any[];assert.equal(ledger.length,2);assert.notEqual(ledger[0].attemptId,ledger[1].attemptId);
    assert.ok(requests.every(r=>r.maxTokens<=16000));
  }finally{await root.fiber.dispose()}
});
