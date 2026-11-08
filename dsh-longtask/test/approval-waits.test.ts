import {test} from "node:test";
import assert from "node:assert/strict";
import {mkdtemp} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {Session} from "@deepseek-ai/dsh-session";
import {JsonStore} from "../src/store/store.js";
import {TokenLedger} from "../src/ledger/token-ledger.js";
import {TaskService, type TaskSessionRecord} from "../src/tasks/task-service.js";
import {ApprovalWaits, type TaskApproval} from "../src/approval-waits.js";
import {RouterNotices} from "../src/router-notices.js";
import {Watchdog} from "../src/watchdog/watchdog.js";
import type {HostContext} from "../src/host-types.js";
import type {Task} from "../src/types.js";

async function fixture() {
  const store=await JsonStore.load(join(await mkdtemp(join(tmpdir(),"lt-approval-")),"state.json")),ledger=new TokenLedger(store);
  const sent:Array<{key:string;text:string}>=[];
  const host:any={stop:async()=>true,sessionAlive:async()=>true,releaseSession:async()=>{},
    createSession:async()=>{throw Error("不得创建额外Worker")},deliver:async(key:string,text:string)=>{sent.push({key,text});return "delivered"}};
  const tasks=new TaskService({store,ledger,host});
  const expert=(relPath:string)=>({relPath,division:"d",name:relPath,intro:"FULL_EXPERT_BODY_MUST_NOT_LEAK",contentHash:"h",snapshottedAt:new Date().toISOString()});
  const t=await tasks.propose({goal:"通用审批任务",criteria:[{id:"c1",text:"完成"}],deliverables:["result.md"],executors:[expert("a.md"),expert("b.md")],acceptor:expert("r.md"),budget:{limitTokens:500000,strict:true}},null,"none","router");
  const a:TaskSessionRecord={taskId:t.id,sessionKey:"worker-a",sessionId:"session-a",relPath:"a.md",role:"executor",createdAt:new Date().toISOString()},b={...a,sessionKey:"worker-b",sessionId:"session-b",relPath:"b.md"};
  await store.transact(data=>{const task=data.collections.tasks![t.id] as Task;task.status="active";task.versions[0]!.confirmedAt=new Date().toISOString();(data.collections.taskSessions??={})[t.id]={[a.sessionKey]:a,[b.sessionKey]:b};});
  await ledger.ensureBudget(t.id,500000,true);
  const router=Session.create("router" as any),other=Session.create("other" as any),agents=new Map<string,any>();let wakes=0;
  const live=(session:Session)=>({session,followup(){wakes++},steer(){wakes++},inject(){wakes++}});agents.set("router",live(router));agents.set("other",live(other));
  const ctx:HostContext={get:(name:string):any=>name==="agents"?{get:(id:string)=>agents.get(id)}:name==="sessions"?{flush:async()=>{}}:undefined,
    on:()=>()=>{},logger:{info(){},warn(){},error(){}},effect(){},provide(){}};
  const approvals=new ApprovalWaits(store,m=>{throw Error(m)}),notices=new RouterNotices(store,ctx);
  const texts=()=>router.snapshotEvents().filter(e=>e.type==="user/message").map(e=>(e.data as any).content.map((b:any)=>b.text??"").join(""));
  const settle=async()=>{for(let n=0;n<5;n++){await new Promise(r=>setTimeout(r,5));await approvals.idle();await notices.idle();}};
  const ask=(lease:TaskSessionRecord,id="request",reason="只读检查需要用户确认")=>approvals.observe(lease,{type:"approval/asked",data:{id,toolName:"pwsh",reason:reason as any,arguments:"RAW_COMMAND_MUST_NOT_LEAK"}});
  const decide=(lease:TaskSessionRecord,id="request",outcome="allowed-once")=>approvals.observe(lease,{type:"approval/decided",data:{id,outcome}});
  return{store,ledger,tasks,taskId:t.id,a,b,approvals,notices,router,other,agents,texts,settle,ask,decide,sent,get wakes(){return wakes},
    async close(){await approvals.dispose();await notices.dispose();await store.close()}};
}

test("审批即时回传真实Router会话：多个Worker同编号独立跟踪，摘要不含工具正文",async()=>{
  const f=await fixture();try{
    const before=structuredClone(f.ledger.getBudget(f.taskId));await f.ask(f.a);await f.ask(f.b);await f.settle();
    const text=f.texts().at(-1)!;assert.match(text,/等待用户审批（2项）/);assert.match(text,/session-a/);assert.match(text,/session-b/);assert.match(text,/pwsh/);
    assert.match(text,/审批卡片/);assert.doesNotMatch(text,/FULL_EXPERT_BODY|RAW_COMMAND/);assert.equal(f.wakes,0);assert.equal(f.other.snapshotEvents().length,0);
    await f.decide(f.a);await f.settle();assert.match(f.texts().at(-1)!,/等待用户审批（1项）/);assert.match(f.texts().at(-1)!,/已批准一次/);
    assert.equal(f.store.list<TaskApproval>("taskApprovals").filter(a=>a.status==="pending")[0]!.sessionId,f.b.sessionId);
    await f.decide(f.b,"request","rejected");await f.settle();assert.match(f.texts().at(-1)!,/审批未通过/);assert.match(f.texts().at(-1)!,/已拒绝/);
    assert.ok(f.tasks.listWaits(f.taskId).every(w=>w.status!=="waiting"));assert.deepEqual(f.ledger.getBudget(f.taskId),before);assert.equal(f.sent.length,0);
  }finally{await f.close();}
});

test("审批通知提供结构化Worker入口，多行目标不挤掉摘要中的待审批原因",async()=>{
  const f=await fixture();try{
    await f.store.transact(data=>{(data.collections.tasks![f.taskId] as Task).versions[0]!.goal='开发系统\n技术栈：不能挤占审批原因'});
    await f.ask(f.a,'native-request','安装原生依赖需要用户决定');await f.settle();
    const message=f.router.snapshotEvents().findLast(e=>e.type==='user/message')!.data as any;
    assert.deepEqual(message.source.workers,[{sessionId:f.a.sessionId,relPath:f.a.relPath,toolName:'pwsh',requestId:'native-request'}]);
    assert.match(message.source.summary,/等待用户审批/);assert.doesNotMatch(message.source.summary,/技术栈/);
    assert.equal(message.source.kind,'longtask-status');assert.equal(f.wakes,0);
  }finally{await f.close()}
});

test("审批事件重放、决定先到及无关会话：不复活等待、不重复通知",async()=>{
  const f=await fixture();try{
    await f.ask(f.a);await f.settle();const count=f.texts().length;
    await f.ask(f.a);await f.settle();assert.equal(f.texts().length,count);
    await f.decide(f.a);await f.settle();const resolved=f.texts().length;
    await f.decide(f.a);await f.ask(f.a);await f.decide(f.a,"late","cancelled");await f.ask(f.a,"late");
    await f.ask({...f.a,sessionId:"unrelated"},"unrelated");await f.settle();
    assert.equal(f.texts().length,resolved);assert.ok(f.tasks.listWaits(f.taskId).every(w=>w.status!=="waiting"));assert.equal(f.wakes,0);
  }finally{await f.close();}
});

test("Router离线期间审批已结束：仅显示决定，其他任务通知不能吞掉待审批",async()=>{
  const f=await fixture();try{
    const root=f.agents.get("router");f.agents.delete("router");await f.ask(f.a);await f.settle();await f.decide(f.a);await f.settle();
    assert.equal(f.texts().length,0);f.agents.set("router",root);f.notices.wake();await f.settle();assert.equal(f.texts().length,1);assert.match(f.texts()[0]!,/审批已通过/);
    f.agents.delete("router");await f.ask(f.a,"next");await f.settle();
    await f.store.transact(data=>{data.appendEvent({eventId:"other-topic",taskId:f.taskId,taskVersion:1,source:"host",type:"task.router_bound",payload:{},payloadRef:null,causedBy:null,dedupeKey:"other-topic",createdAt:new Date().toISOString()});});
    f.agents.set("router",root);f.notices.wake();await f.settle();assert.match(f.texts().at(-1)!,/等待用户审批/);assert.match(f.texts().at(-1)!,/next/);
  }finally{await f.close();}
});

test("审批等待受撤权、任务取消、轮次结束约束，迟到批准不恢复任务",async()=>{
  for(const change of ["revoked","cancelled","turn-end"]){
    const f=await fixture();try{
      await f.ask(f.a);await f.settle();
      if(change==="revoked")await f.store.transact(data=>{((data.collections.taskSessions![f.taskId] as any)[f.a.sessionKey]).revokedAt=new Date().toISOString();});
      else if(change==="cancelled")await f.tasks.cancel(f.taskId);
      else await f.approvals.observe(f.a,{type:"turn/end",data:{turn:1,reason:{kind:"aborted"}}});
      await f.settle();assert.equal(f.store.list<TaskApproval>("taskApprovals")[0]!.status,"expired");
      await f.decide(f.a);await f.ask(f.a);await f.settle();assert.ok(f.tasks.listWaits(f.taskId).every(w=>w.status!=="waiting"));assert.equal(f.sent.length,0);
      if(change==="cancelled")assert.equal(f.tasks.getTask(f.taskId)!.status,"cancelled");
    }finally{await f.close();}
  }
});

test("可靠审批等待不超龄；Watchdog只纠偏未等审批的Worker",async()=>{
  const f=await fixture();try{
    await f.ask(f.a);await f.settle();await f.store.transact(data=>{for(const w of Object.values(data.collections.taskWaits??{}) as any[])w.since="2000-01-01T00:00:00Z";});
    await f.tasks.sweepStaleWaits();assert.equal(f.tasks.listWaits(f.taskId).filter(w=>w.status==="waiting").length,1);
    const wd=new Watchdog(f.store,undefined,()=>Date.now()+86400000);assert.equal(wd.checkAll().length,1,"另一位未等待的执行者仍受停滞检查");
    await f.tasks.sendWatchdogCorrection(f.taskId,"推进未阻塞部分");assert.equal(f.sent.length,1);assert.equal(f.sent[0]!.key,f.b.sessionKey);
    await f.ask(f.b);await f.settle();assert.equal(wd.checkAll().length,0);assert.equal(await f.tasks.sendWatchdogCorrection(f.taskId,"不得催审批"),false);assert.equal(f.sent.length,1);
  }finally{await f.close();}
});

test("运行时重载失效旧审批并保留账本；原型决定不会被重放",async()=>{
  const f=await fixture();try{
    await f.ask(f.a);await f.settle();const before=structuredClone(f.ledger.getBudget(f.taskId));
    await f.approvals.dispose();const restored=new ApprovalWaits(f.store,()=>{});
    try{assert.deepEqual(await restored.restore(),[f.taskId]);assert.deepEqual(f.ledger.getBudget(f.taskId),before);assert.ok(f.tasks.listWaits(f.taskId).every(w=>w.status!=="waiting"));
      await restored.observe(f.a,{type:"approval/decided",data:{id:"request",outcome:"allowed-once"}});assert.equal(f.store.list<TaskApproval>("taskApprovals")[0]!.status,"expired");
    }finally{await restored.dispose();}
  }finally{await f.close();}
});

test("审批刚登记就结束轮次：排队落盘不能留下孤儿审批等待",async()=>{
  const f=await fixture();try{
    const asked=f.ask(f.a);const ended=f.approvals.observe(f.a,{type:"turn/end",data:{turn:1,reason:{kind:"aborted"}}});
    await Promise.all([asked,ended]);await f.settle();
    assert.equal(f.store.list<TaskApproval>("taskApprovals")[0]!.status,"expired");assert.ok(f.tasks.listWaits(f.taskId).every(w=>w.status!=="waiting"));
  }finally{await f.close();}
});

test("验收Worker的审批同样回传，决定不等于通过任务验收",async()=>{
  const f=await fixture();try{
    const reviewer:TaskSessionRecord={...f.a,role:"acceptor",relPath:"r.md",sessionKey:"reviewer",sessionId:"reviewer-session"};
    await f.store.transact(data=>{(data.collections.tasks![f.taskId] as Task).status="awaiting_acceptance";(data.collections.taskSessions![f.taskId] as any)[reviewer.sessionKey]=reviewer;});
    await f.ask(reviewer);await f.settle();assert.match(f.texts().at(-1)!,/reviewer-session/);
    await f.decide(reviewer);await f.settle();assert.equal(f.tasks.getTask(f.taskId)!.status,"awaiting_acceptance");assert.equal(f.sent.length,0);
  }finally{await f.close();}
});
