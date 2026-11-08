import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {prepareSdkTools,loadSdk} from "./sdk-tools.js";
import {apply} from "../src/index.js";
import {DEFAULT_PRICES} from "../src/ledger/money.js";
const [{Context,Service},{AgentRegistry},{SessionStore},{SessionProjectionRegistry},{SystemPrompt},{ToolRuntime},{AgentLoop},{LlmRuntime,LlmAdapter}]=await Promise.all(
  ["cordis","dsh-agent","dsh-session","dsh-session-projection","dsh-system-prompt","dsh-tools","dsh-agent-loop","dsh-llm"].map(loadSdk));
const wait=async(fn:()=>boolean)=>{for(let i=0;i<500;i++){if(fn())return;await new Promise(r=>setTimeout(r,10));}throw Error("验收真实循环超时");};

for(const readonly of [true,false])test(`真实AgentLoop迟到read必须重组快照（只读=${readonly}），首个付费验收请求即可读取产物`,async()=>{
  const root=new Context(),dir=mkdtempSync(join(tmpdir(),"acceptor-loop-")),calls:any[]=[],events:any[]=[],errors:any[]=[];
  mkdirSync(join(dir,"experts/research"),{recursive:true});writeFileSync(join(dir,"experts/divisions.json"),JSON.stringify({divisions:{research:{label:"Research"}}}));
  for(const n of ["worker","reviewer"])writeFileSync(join(dir,`experts/research/${n}.md`),`---\nname: ${n}\ndescription: test\n---\nfixture`);
  const file=join(dir,"result.md");writeFileSync(file,"ACTUAL_READ_EVIDENCE");
  new SessionStore(root);new SessionProjectionRegistry(root);const agents=new AgentRegistry(root);if(typeof agents[Service.init]==="function")await agents[Service.init]();
  new SystemPrompt(root,{includeHarnessIdentity:false,includeRuntimeContext:false});const tools=await prepareSdkTools(root,new ToolRuntime(root));
  const definition=(name:string,execute:()=>Promise<string>)=>({name,description:name,parameters:{type:"object",properties:{}},output:{schema:{type:"string"},render:(_a:any,v:string)=>[{type:"text",text:v}]},execute});
  tools.register(definition("working_directory",async()=>dir));tools.register(definition("write",async()=>{throw Error("验收不能写入");}));
  const llm=new LlmRuntime(root);if(typeof llm[Service.init]==="function")await llm[Service.init]();
  let api:any,task:any,late=true,readCalls=0;
  root.on("system-prompt/assemble",async(_assembly:any,context:any,next:any)=>{
    const sid=context.scope?.session?.id??context.scope?.id;
    if(late&&api?.getTaskSessions(task?.id).some((s:any)=>s.role==="acceptor"&&s.sessionId===sid)){
      late=false;tools.register(definition("read",async()=>{readCalls++;return readFileSync(file,"utf8");}));
    }
    return next();
  });
  class Adapter extends LlmAdapter{
    providerInfo(id:string){return{id,name:id};}
    async resolveModel(provider:string,id:string){return{provider,id,name:id,defaultMaxTokens:16000};}
    async *stream(request:any){
      const lease=api.getTaskSessions(task.id).find((s:any)=>s.sessionId===request.sessionId);calls.push({role:lease.role,tools:request.tools?.map((t:any)=>t.name)});
      yield{type:"usage",usage:{inputTokens:100,outputTokens:100,cacheReadTokens:0,cacheWriteTokens:0}};
      if(lease.role==="acceptor"&&readCalls===0){
        yield{type:"block-start",index:0,blockType:"tool-call"};yield{type:"block-end",index:0,block:{type:"tool-call",id:"read-proof",name:"read",arguments:"{}"}};yield{type:"finish",reason:{kind:"tool-calls"}};return;
      }
      const round=lease.role==="acceptor"?Object.values(api.getTaskEvidence(task.id).collections.acceptanceRounds).find((r:any)=>r.state==="pending") as any:undefined;
      const text=lease.role==="executor"?'<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>':`<longtask_verdicts>${JSON.stringify({roundId:round.roundId,artifactHashes:round.artifactHashes,verdicts:[{criterionId:"c1",outcome:"pass",gap:null,evidence:"ACTUAL_READ_EVIDENCE",criteriaVersion:1,verdictBy:"research/reviewer.md"}]})}</longtask_verdicts>`;
      yield{type:"block-start",index:0,blockType:"text"};yield{type:"block-end",index:0,block:{type:"text",text}};yield{type:"finish",reason:{kind:"stop"}};
    }
  }
  llm.registerAdapter(["offline"],new Adapter());root.on("session/event",(_s:any,e:any)=>events.push(e));root.on("agent/error",(e:any)=>errors.push(e));
  const loop=new AgentLoop(root,{agents:[],maxParallelToolCalls:{get:()=>1}});if(typeof loop[Service.init]==="function")await loop[Service.init]();
  apply(root,{dataDir:dir,agencyAgentsRoot:join(dir,"experts"),workerCwd:dir,workerPreset:"",workerParentAgent:"none",workerProvider:"offline",workerModel:"offline",acceptorReadOnly:readonly,probeOnStart:false,watchdogIntervalMs:3600000,acceptanceReserveTokens:0,wrapUpAtBudgetRatio:0,wrapUpMinStepsLeft:0,
    modelPrices:DEFAULT_PRICES.map(p=>({...p,provider:"offline",model:"offline",verifiedAt:"2020-01-01",expiresAt:"2099-01-01"}))});
  try{
    await wait(()=>Boolean(root.get("longtask")?.getRuntimeInfo().ready));api=root.get("longtask");
    const expert=(relPath:string)=>({relPath,division:"research",name:relPath,intro:"fixture",contentHash:"fixture",snapshottedAt:new Date().toISOString()});
    task=await api.propose({goal:"验收文件",criteria:[{id:"c1",text:"产物内容可核对"}],deliverables:[file],executors:[expert("research/worker.md")],acceptor:expert("research/reviewer.md"),budget:{limitTokens:Number.MAX_SAFE_INTEGER,strict:true,money:{currency:"CNY",limitAmount:"5"}}});
    await api.confirm(task.id);
    await wait(()=>api.getTask(task.id).status==="delivered"||api.getTask(task.id).status==="paused");
    assert.equal(api.getTask(task.id).status,"delivered",JSON.stringify({errors:errors.map(e=>String(e?.error??e)),status:api.getTask(task.id),log:readFileSync(join(dir,"plugin.log"),"utf8").slice(-3000)}));
    const acceptance=calls.filter(c=>c.role==="acceptor");assert.equal(acceptance.length,2);assert.equal(readCalls,1);
    assert.equal(api.getModelExpenses(task.id).length,3,"只有执行者1次与成功重组后的验收2次计费，旧快照零调用");
    assert.ok(acceptance.every(c=>c.tools.includes("read")&&(!readonly||!c.tools.includes("write"))),JSON.stringify(acceptance));
    assert.ok(events.some(e=>e.type==="turn/end"&&e.data.reason.kind==="blocked"));
    assert.equal(api.getMoneyBudget(task.id).limitAmount,"5");
  }finally{await root.fiber.dispose();}
});
