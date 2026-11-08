import {test} from "node:test";
import assert from "node:assert/strict";
import {promises as fs} from "node:fs";
import {join,resolve} from "node:path";
import {createHash} from "node:crypto";
import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {JsonStore} from "../src/store/store.js";
import {TokenLedger} from "../src/ledger/token-ledger.js";
import {TaskService,type HostBridge} from "../src/tasks/task-service.js";
import type {Task,TaskVersion,Verdict,VerificationPlan} from "../src/types.js";
import {registerVerificationTool,missingVerification,verificationMethods,validateVerificationPlan,digest,VERIFICATION_RUNS,type VerificationRun} from "../src/verification.js";
import {DELIVERY_EVIDENCE,DeliveryEvidenceExporter,exportDeliveryEvidence,queueDeliveryEvidence,queueHistoricalEvidence,type DeliveryEvidence} from "../src/delivery-evidence.js";

const execute=promisify(execFile),sha=(s:string)=>createHash("sha256").update(s).digest("hex");
const expert=(relPath:string)=>({relPath,division:"d",name:relPath,intro:"",contentHash:"h",snapshottedAt:new Date().toISOString()});
async function fixture(){
  const parent=resolve(".longtask/verification");await fs.mkdir(parent,{recursive:true});const dir=await fs.mkdtemp(join(parent,"verify-test-")),project=join(dir,"project");await fs.mkdir(project);
  await fs.writeFile(join(project,"app.txt"),"frozen source");
  await fs.writeFile(join(project,"check.cjs"),"const assert=require('node:assert/strict'),fs=require('node:fs');assert.equal(fs.readFileSync('app.txt','utf8'),'frozen source');fs.writeFileSync('runtime-output.txt','actual side effect in copy');console.log('REAL_CHECK_PASSED');");
  const store=await JsonStore.load(join(dir,"state.json")),ledger=new TokenLedger(store),deliveries:string[]=[];let sequence=0,calls=0,definition:any,handler:any;
  const host:HostBridge={async createSession(){const sid=`session-${++sequence}`;return {sessionKey:sid,sessionId:sid,dispose:async()=>{}}},async deliver(_key,body){deliveries.push(body);return "delivered"},async stop(){return true},async sessionAlive(){return true},async releaseSession(){}};
  const svc=new TaskService({store,ledger,host,verifyArtifact:async(path,hash)=>sha(await fs.readFile(path,"utf8"))===hash});
  const plan:VerificationPlan={workspace:project,checks:[{id:"api",kind:"command",criterionIds:["c1"],purpose:"运行检查并断言源码与临时输出",program:process.execPath,args:["check.cjs"],timeoutMs:10000}]};
  const t=await svc.propose({goal:"验证结果",criteria:[{id:"c1",text:"npm test 全部通过",verification:["command"]}],deliverables:[join(project,"app.txt")],executors:[expert("worker.md")],acceptor:expert("review.md"),budget:{limitTokens:2000000,strict:true},verificationPlan:plan});
  await svc.confirm(t.id);await svc.registerArtifact(t.id,join(project,"app.txt"),sha("frozen source"),"worker.md");
  await store.transact(data=>{(data.collections.tasks![t.id] as Task).status="awaiting_acceptance"});const roundId=await svc.runAcceptance(t.id,"initial");
  const shell:any={sandboxMode:"workspace-write",resolve:(r:any)=>r,execute:async(spec:any)=>{calls++;return {result:async()=>{
    if(handler)return handler(spec);
    // 实际子进程验证命令/副本/返回码；本夹具仅模拟宿主沙箱元数据，非OS沙箱验证。
    let stdout="",stderr="",exitCode=0;
    try{const result=await execute(process.platform==="win32"?"pwsh":"bash",process.platform==="win32"?["-NoLogo","-NoProfile","-NonInteractive","-Command",spec.command]:["-c",spec.command],{cwd:spec.workdir,timeout:spec.timeoutMs,signal:spec.signal});stdout=result.stdout;stderr=result.stderr;}catch(e:any){stdout=e.stdout??"";stderr=e.stderr??String(e);exitCode=typeof e.code==="number"?e.code:1;}
    return {exitCode,stdout:{text:stdout},stderr:{text:stderr},sandbox:{mode:"workspace-write",enforcement:"fixture-host-contract"}};
  }}}};
  const services=new Map<string,any>([["shell",shell],["tools",{register:(d:any)=>{definition=d;return()=>{}}}]]);
  const ctx:any={get:(name:string)=>services.get(name),logger:{warn(){},info(){},error(){}}};
  const tool=registerVerificationTool({ctx,store,tasks:svc,workspace:dir,dataDir:join(dir,"reports"),blocked:()=>false});
  const lease=svc.getSessions(t.id).find(s=>s.role==="acceptor")!,agent={session:{id:lease.sessionId}};
  const call=(args:any={checkId:"api"},caller=agent)=>definition.execute(args,{agent:caller,signal:new AbortController().signal});
  const verdict=(ids?:string[]):Verdict=>({criterionId:"c1",outcome:"pass",evidence:"已读源码和测试文件",gap:null,criteriaVersion:svc.currentVersion(svc.getTask(t.id)!).version,artifactHashes:svc.latestPendingRound(t.id)!.artifactHashes,verdictBy:"review.md",createdAt:new Date().toISOString(),...(ids?{verificationRunIds:ids}:{})});
  return {dir,project,store,ledger,svc,plan,t,roundId,call,verdict,deliveries,services,shell,get calls(){return calls},setHandler:(h:any)=>{handler=h},async close(){await tool.dispose();await store.close();assert.ok(resolve(dir).startsWith(parent));await fs.rm(dir,{recursive:true,force:true});}};
}

test("实测要求不能被review降级；命令与浏览器检查分别覆盖",()=>{
  assert.deepEqual(verificationMethods({id:"c",text:"npm test 全部通过，浏览器控制台无报错",verification:["review"]}),["review","command","browser"]);
  const criteria=[{id:"c",text:"npm test 全部通过，浏览器控制台无报错"}];assert.equal(validateVerificationPlan({criteria}).length,2);
  assert.deepEqual(verificationMethods({id:"doc",text:"README包含接口概览"}),["review"]);
  assert.deepEqual(verificationMethods({id:"doc",text:"产物 C:\\workspace\\e2e\\result.md 与模板一致"}),["review"]);
  assert.ok(validateVerificationPlan({criteria:[{id:"c",text:"功能符合要求"}],verificationPolicyVersion:1}).some(e=>e.includes("明确verification")));
});

const documentCriteria=[
  "No browser tests are required; only review the existing report.",
  "Browser tests are not required.",
  "This task does not require browser tests.",
  "文档说明如何执行 pytest，并给出所有测试通过时的示例输出。",
  "The README describes how to run pytest and use a browser to log in.",
  "Document the steps to execute cargo test and verify session persistence in a browser.",
  "Do not run pytest or open a browser; only review the existing report.",
  "Please do not run pytest; only review the report.",
  "请不要运行 pytest，只审阅已有报告。",
  "Document how to run the integration test suite and execute ./scripts/verify.sh.",
  "文档包含安装步骤、npm test 命令说明和浏览器兼容性说明。",
  "README 说明如何运行 npm test、如何启动服务，以及浏览器支持范围。",
  "在用户手册中描述浏览器登录、刷新后保持会话的操作步骤。",
  "文档列出接口集成测试用例和预期 HTTP 401 响应。",
  "教程介绍 Playwright 测试及端到端测试的写法。",
  "撰写浏览器兼容性报告，引用控制台无报错的示例。",
  "README documents npm test and browser compatibility.",
  "Document the steps to launch the server and run browser tests.",
  "规范中引用“实际运行 npm test 全部通过”的示例，不要求执行命令。",
  "README 包含以下操作示例：\n```bash\nnpm test\n```\n并说明浏览器测试范围。",
  "测试报告记录已有运行日志、退出码及浏览器截图引用。",
  "README 包含测试运行说明，并写明 npm test 通过时应出现的输出。",
  "说明运行结果和浏览器控制台输出。",
  "文档提供安装说明，并注明‘浏览器控制台无报错’的验收口径。",
  "编写部署文档，解释 npm start 后如何访问页面。",
  "文档说明如何运行测试，不需要运行 npm test。",
  "只编写操作手册，禁止打开浏览器。",
  "文档引用“实际运行 npm test，浏览器控制台无报错”作为示例。",
  "交付 README\n- 安装步骤：npm test\n- 浏览器兼容性：Chrome和Firefox",
  "文档引用`并实际运行 npm test`作为命令示例。",
  "文档引用'并实际打开浏览器'作为操作示例。",
];
for(const text of documentCriteria)test(`文档内容要求不触发实测：${text.slice(0,30)}`,()=>{
  const criteria=[{id:"doc",text,verification:["review"] as const}].map(c=>({...c,verification:[...c.verification]}));
  assert.deepEqual(verificationMethods(criteria[0]!),["review"]);
  assert.deepEqual(validateVerificationPlan({criteria}),[]);
});
const runtimeCriteria:Array<[string,string[]]>=[
  ["No browser tests are required; run pytest.",["command"]],
  ["执行 pytest，所有测试必须通过",["command"]],
  ["运行 python -m pytest 并确认通过。",["command"]],
  ["执行 cargo test 和 go test ./...。",["command"]],
  ["Run pytest and ensure all tests pass.",["command"]],
  ["All tests must pass.",["command"]],
  ["Execute ./scripts/verify.sh and check the exit code.",["command"]],
  ["Run the integration test suite.",["command"]],
  ["运行自定义回归脚本。",["command"]],
  ["Use a browser to log in and verify session persistence.",["browser"]],
  ["Open the browser, click Save and refresh to verify the session.",["browser"]],
  ["The README describes the setup. Run pytest and ensure all tests pass.",["command"]],
  ["文档说明测试步骤，并执行 pytest 验证结果。",["command"]],
  ["Do not run pytest; use a browser to verify login.",["browser"]],
  ["按文档实际执行 npm install 和 npm test，测试全部通过。",["command"]],
  ["根据 README 安装依赖并启动服务，访问页面成功。",["command"]],
  ["文档包含安装说明；另需实际执行 npm test 并验证全部通过。",["command"]],
  ["README 写清启动步骤，并实际启动服务验证接口返回 HTTP 200。",["command"]],
  ["README 写清登录说明，同时在浏览器中实际登录并检查控制台无报错。",["browser"]],
  ["按文档从零跑通项目。",["command"]],
  ["依照 README 在浏览器登录、点击保存并刷新，会话保持且控制台无报错。",["browser"]],
  ["执行 `npm test`，所有测试通过。",["command"]],
  ["`npm test` 全部通过。",["command"]],
  ["启动后访问 http://localhost:3000 返回登录页。",["command"]],
  ["实际打开浏览器完成线索增删改查。",["browser"]],
  ["文档标题为《浏览器控制台无报错》，并实际运行 npm test。",["command"]],
  ["“npm test”全部通过。",["command"]],
  ["README describes the commands and actually run npm test successfully.",["command"]],
];
for(const [text,kinds] of runtimeCriteria)test(`明确实测不被文档语境豁免：${text.slice(0,30)}`,()=>{
  const methods=verificationMethods({id:"c",text,verification:["review"]});
  assert.deepEqual(methods,["review",...kinds]);
  assert.equal(validateVerificationPlan({criteria:[{id:"c",text}]}).length,kinds.length);
});

test("文档内容仍保留显式指定的实测方式",()=>{
  assert.deepEqual(verificationMethods({id:"c",text:"No browser tests are required.",verification:["browser"]}),["browser"]);
  assert.deepEqual(verificationMethods({id:"c",text:"README说明npm test命令和浏览器支持范围",verification:["command","browser"]}),["command","browser"]);
});

test("历史签署金额和范围快照不随后续预算结算与标准范围变化",async()=>{
  const f=await fixture();try{
    await f.store.transact(data=>{
      const task=data.collections.tasks![f.t.id] as Task;
      task.versions[task.currentVersion]!.verificationScope={included:["原签署范围"],excluded:[]};
      (data.collections.budgets![f.t.id] as any).money={currency:"CNY",settledNano:"100",reservedNano:"0",limitNano:"1000"};
      queueDeliveryEvidence(data,task,{roundId:"snapshot-round"},[],{});
    });
    await f.store.transact(data=>{
      (data.collections.budgets![f.t.id] as any).money.settledNano="200";
      const task=data.collections.tasks![f.t.id] as Task;
      task.versions[task.currentVersion]!.verificationScope!.included.push("新增范围");
    });
    const row=f.store.list<DeliveryEvidence>(DELIVERY_EVIDENCE).find(r=>r.roundId==="snapshot-round")!;
    assert.equal((row.snapshot.money as any).settledNano,"100");assert.deepEqual(row.snapshot.scope.included,["原签署范围"]);
  }finally{await f.close()}
});

test("正式确认拒绝将中英文执行要求声明为review后无计划开工",async()=>{
  const f=await fixture();try{
    for(const text of ["执行 pytest，所有测试必须通过","Run pytest and ensure all tests pass.","Use a browser to log in and verify session persistence."]){
      const t=await f.svc.propose({goal:"验证现有系统",criteria:[{id:"c1",text,verification:["review"]}],deliverables:[join(f.project,"app.txt")],executors:[expert("writer.md")],acceptor:expert("editor.md"),budget:{limitTokens:2000000,strict:true}});
      await assert.rejects(()=>f.svc.confirm(t.id),/需要(?:command|browser)实测/);
      assert.equal(f.svc.getTask(t.id)!.status,"proposed");assert.equal(f.svc.getSessions(t.id).length,0);assert.equal(f.ledger.getBudget(t.id),undefined);
    }
  }finally{await f.close()}
});

test("仅读文件及自报22/22不能交付，保留轮次与预算等待实测",async()=>{
  const f=await fixture();try{const before=JSON.stringify(f.ledger.getBudget(f.t.id));
    assert.equal(await f.svc.recordVerdicts(f.t.id,[f.verdict()],f.svc.latestPendingRound(f.t.id)!.artifactHashes,f.roundId),"paused");
    assert.equal(f.svc.getTask(f.t.id)!.status,"paused");assert.equal(f.svc.latestPendingRound(f.t.id)!.roundId,f.roundId);assert.equal(JSON.stringify(f.ledger.getBudget(f.t.id)),before);
    assert.ok(JSON.stringify(f.store.snapshot().events).includes("待实测"));assert.ok(!Object.values(f.store.snapshot().events).some((e:any)=>e.type==="task.delivered"));
  }finally{await f.close()}
});

test("纯文档可无实测计划、无shell完成静态验收交付",async()=>{
  const f=await fixture();try{
    f.services.delete('shell');
    const t=await f.svc.propose({goal:'编写API接口文档',criteria:[{id:'doc',text:'文档说明npm test命令、接口测试用例及浏览器兼容性',verification:['review']}],deliverables:[join(f.project,'app.txt')],executors:[expert('writer.md')],acceptor:expert('editor.md'),budget:{limitTokens:2000000,strict:true}});
    await f.svc.confirm(t.id);await f.svc.registerArtifact(t.id,join(f.project,'app.txt'),sha('frozen source'),'writer.md');
    await f.store.transact(d=>{(d.collections.tasks![t.id] as Task).status='awaiting_acceptance'});const id=await f.svc.runAcceptance(t.id,'document');
    const hashes=f.svc.latestPendingRound(t.id)!.artifactHashes;
    assert.equal(await f.svc.recordVerdicts(t.id,[{criterionId:'doc',outcome:'pass',evidence:'已读取文档并核对结构、说明及引用',gap:null,criteriaVersion:1,artifactHashes:hashes,verdictBy:'editor.md',createdAt:new Date().toISOString()}],hashes,id),'delivered');
    assert.equal(f.calls,0);assert.equal(f.store.list(VERIFICATION_RUNS).length,0);
  }finally{await f.close()}
});

test("专用工具实际执行固定命令，写入仅发生在副本，持久化记录后才可交付",async()=>{
  const f=await fixture();try{
    const [one,two]=await Promise.all([f.call(),f.call()]),run=JSON.parse(one) as VerificationRun;
    assert.equal(one,two);assert.equal(f.calls,1);assert.equal(run.status,"passed");assert.match(run.summary,/REAL_CHECK_PASSED/);assert.ok(run.inputHash);
    assert.equal(await fs.readFile(join(f.project,"app.txt"),"utf8"),"frozen source");await assert.rejects(fs.access(join(f.project,"runtime-output.txt")));
    assert.match(await fs.readFile(run.logPath,"utf8"),/REAL_CHECK_PASSED/);
    assert.equal(await f.call(),one);assert.equal(f.calls,1);
    const disk=JSON.parse(await fs.readFile(join(f.dir,"state.json"),"utf8"));assert.equal(disk.collections[VERIFICATION_RUNS][run.id].id,run.id);
    assert.equal(await f.svc.recordVerdicts(f.t.id,[f.verdict([run.id])],run.artifactHashes,f.roundId),"delivered");
  }finally{await f.close()}
});

test("拒绝伪造/旧轮次/旧epoch/旧产物/旧计划/错误版本及缺少任一检查的记录",async()=>{
  const f=await fixture();try{
    const run=JSON.parse(await f.call()) as VerificationRun,v=f.svc.currentVersion(f.svc.getTask(f.t.id)!),verdict=f.verdict([run.id]);
    for(const patch of [{id:"fake"},{roundId:"old"},{stepEpoch:99},{artifactHashes:{}},{planHash:"old"},{version:0},{status:"failed"},{kind:"browser"},{criterionIds:[]}]){
      assert.ok(missingVerification(v,[verdict],[{...run,...patch} as VerificationRun],f.roundId,0,run.artifactHashes).length,JSON.stringify(patch));
    }
    const extra=structuredClone(v);extra.verificationPlan!.checks.push({...extra.verificationPlan!.checks[0]!,id:"second"});
    const adjusted={...run,planHash:digest(extra.verificationPlan)};assert.ok(missingVerification(extra,[verdict],[adjusted],f.roundId,0,run.artifactHashes).some(s=>s.includes("second")));
    await f.store.transact(data=>{(data.collections[VERIFICATION_RUNS]![run.id] as VerificationRun).taskId="other-task"});
    assert.equal(await f.svc.recordVerdicts(f.t.id,[verdict],run.artifactHashes,f.roundId),"paused");
  }finally{await f.close()}
});

for(const mode of ["failure","timeout","unconfined","denied","runnerFailed","mutated"]){
  test(`检查${mode}不能作为pass运行证据`,async()=>{
    const f=await fixture();try{
      f.setHandler(async(spec:any)=>{if(mode==="mutated")await fs.writeFile(join(spec.workdir,"app.txt"),"modified by test");return {exitCode:mode==="failure"?7:0,timedOut:mode==="timeout",stdout:{text:"claimed success"},stderr:{text:""},...(mode==="unconfined"?{}:{sandbox:{mode:"workspace-write",enforcement:"fixture",denied:mode==="denied",runnerFailed:mode==="runnerFailed"}})}});
      const run=JSON.parse(await f.call()) as VerificationRun;assert.notEqual(run.status,"passed");assert.equal(await fs.readFile(join(f.project,"app.txt"),"utf8"),"frozen source");
      assert.equal(await f.svc.recordVerdicts(f.t.id,[f.verdict([run.id])],run.artifactHashes,f.roundId),"paused");
    }finally{await f.close()}
  });
}

test("验收不能传新命令或由Router/Worker代跑；无沙箱能力时零执行",async()=>{
  const f=await fixture();try{
    await assert.rejects(f.call({checkId:"api",command:"changed"}),/只接受/);await assert.rejects(f.call({checkId:"unknown"}),/不在/);
    await assert.rejects(f.call({checkId:"api"},{session:{id:"router"}}),/独立验收/);
    f.services.delete("shell");await assert.rejects(f.call(),/沙箱shell/);assert.equal(f.calls,0);assert.equal(f.store.list(VERIFICATION_RUNS).length,0);
  }finally{await f.close()}
});

for(const kind of ["script","log"]){
  test(`运行后${kind}发生变化不能沿用旧成功证据`,async()=>{
    const f=await fixture();try{
      const run=JSON.parse(await f.call()) as VerificationRun;
      await fs.writeFile(kind==="script"?join(f.project,"check.cjs"):run.logPath,"replaced");
      assert.equal(await f.svc.recordVerdicts(f.t.id,[f.verdict([run.id])],run.artifactHashes,f.roundId),"paused");
    }finally{await f.close()}
  });
}

test("运行期间取消后，迟到成功不能登记或交付",async()=>{
  const f=await fixture();try{
    let release:any,started:any;const ready=new Promise<void>(r=>started=r);
    f.setHandler(()=>{started();return new Promise(r=>release=r)});const running=f.call();await ready;await f.svc.cancel(f.t.id);
    release({exitCode:0,stdout:{text:"late success"},stderr:{text:""},sandbox:{mode:"workspace-write",enforcement:"fixture"}});
    await assert.rejects(running,/轮次|权限/);assert.equal(f.store.list(VERIFICATION_RUNS).length,0);assert.equal(f.svc.getTask(f.t.id)!.status,"cancelled");
  }finally{await f.close()}
});

test("已交付旧任务可补实测计划：旧记录失效，不重新派开发、不清零费用",async()=>{
  const f=await fixture();try{
    const run=JSON.parse(await f.call()) as VerificationRun;await f.svc.recordVerdicts(f.t.id,[f.verdict([run.id])],run.artifactHashes,f.roundId);
    const before=JSON.stringify(f.ledger.getBudget(f.t.id)),workers=f.svc.getSessions(f.t.id).filter(s=>s.role==="executor").length;
    await f.svc.configureVerification(f.t.id,f.plan,1);
    assert.equal(f.svc.currentVersion(f.svc.getTask(f.t.id)!).version,2);assert.equal(f.svc.getTask(f.t.id)!.status,"awaiting_acceptance");
    assert.notEqual(f.svc.latestPendingRound(f.t.id)!.roundId,f.roundId);assert.equal(f.svc.getSessions(f.t.id).filter(s=>s.role==="executor").length,workers);
    assert.equal(JSON.stringify(f.ledger.getBudget(f.t.id)),before);await assert.rejects(f.svc.configureVerification(f.t.id,f.plan,1),/审批/);
  }finally{await f.close()}
});

test("原任务可一次修订标准和实测范围后仅重验，保留身份、费用与修订前后记录",async()=>{
  const f=await fixture();try{
    await f.store.transact(d=>{const t=d.collections.tasks![f.t.id] as Task;t.status='delivered';t.versions[0]!.criteria=[{id:'c1',text:'浏览器交互正常',verification:['browser']}];delete t.versions[0]!.verificationPlan;});
    const budget=JSON.stringify(f.ledger.getBudget(f.t.id)),workers=f.svc.getSessions(f.t.id).filter(s=>s.role==='executor').length;
    await f.svc.configureVerification(f.t.id,f.plan,1,{criteria:[{id:'c1',text:'npm test全部通过',verification:['command']}],scope:{included:['接口运行测试'],excluded:['浏览器交互']},reason:'用户确认服务级范围'});
    const t=f.svc.getTask(f.t.id)!;assert.equal(t.status,'awaiting_acceptance');assert.equal(t.versions[0]!.criteria[0]!.text,'浏览器交互正常');assert.equal(t.versions[1]!.criteria[0]!.verification![0],'command');
    assert.equal(f.svc.getSessions(f.t.id).filter(s=>s.role==='executor').length,workers);assert.equal(JSON.stringify(f.ledger.getBudget(f.t.id)),budget);
    const event=Object.values(f.store.snapshot().events).find((e:any)=>e.type==='task.reopened') as any;assert.equal(event.payload.criteriaChanges[0].before.text,'浏览器交互正常');assert.equal(event.payload.scope.excluded[0],'浏览器交互');
  }finally{await f.close()}
});

test("补测修订失败保持旧标准和费用，新增标准或旧版本不能悄悄生效",async()=>{
  const f=await fixture();try{
    await f.svc.pause(f.t.id,'missing_input','待能力');const before=JSON.stringify(f.svc.getTask(f.t.id));
    await assert.rejects(f.svc.configureVerification(f.t.id,f.plan,1,{criteria:[{id:'unknown',text:'新增',verification:['review']}]}),/原标准/);
    await assert.rejects(f.svc.configureVerification(f.t.id,f.plan,1,{criteria:[{id:'c1',text:'浏览器控制台无报错',verification:['browser']}]}),/browser/);
    assert.equal(JSON.stringify(f.svc.getTask(f.t.id)),before);
  }finally{await f.close()}
});

test("交付事务保存最终签署快照，导出附件与可运行复现脚本，不改冻结文件",async()=>{
  const f=await fixture();try{
    const run=JSON.parse(await f.call()) as VerificationRun;await f.svc.recordVerdicts(f.t.id,[f.verdict([run.id])],run.artifactHashes,f.roundId);
    const row=f.store.list<DeliveryEvidence>(DELIVERY_EVIDENCE)[0]!;assert.equal(row.snapshot.roundId,f.roundId);assert.equal(row.snapshot.runs[0]!.id,run.id);assert.ok(row.snapshot.scope.excluded.some(x=>x.includes('browser')));
    const paths=await exportDeliveryEvidence(row,join(f.dir,'final'));const report=await fs.readFile(paths.report,'utf8');assert.match(report,/最终独立验收/);assert.ok(report.includes(run.id));assert.ok(report.includes(sha('frozen source')));
    const replay=await execute(process.execPath,[paths.reproduce,'--workspace',f.project],{windowsHide:true});assert.match(replay.stdout,/REAL_CHECK_PASSED/);
    assert.equal(await fs.readFile(join(f.project,'app.txt'),'utf8'),'frozen source');assert.equal(await fs.readFile(paths.report,'utf8'),report);
    assert.deepEqual(await exportDeliveryEvidence(row,join(f.dir,'final')),paths,'重复导出幂等');
  }finally{await f.close()}
});

test("附件写入失败保存错误状态，修复路径后可重试，不重开任务和重新扣费",async()=>{
  const f=await fixture();let exporter:DeliveryEvidenceExporter|undefined;try{
    const run=JSON.parse(await f.call()) as VerificationRun;await f.svc.recordVerdicts(f.t.id,[f.verdict([run.id])],run.artifactHashes,f.roundId);
    const root=join(f.dir,'blocked');await fs.writeFile(root,'file');exporter=new DeliveryEvidenceExporter(f.store,root,()=>{});await exporter.flush();
    assert.equal(f.store.list<DeliveryEvidence>(DELIVERY_EVIDENCE)[0]!.exportState,'error');const budget=JSON.stringify(f.ledger.getBudget(f.t.id));
    await fs.unlink(root);await exporter.retry();assert.equal(f.store.list<DeliveryEvidence>(DELIVERY_EVIDENCE)[0]!.exportState,'exported');assert.equal(f.svc.getTask(f.t.id)!.status,'delivered');assert.equal(JSON.stringify(f.ledger.getBudget(f.t.id)),budget);
  }finally{await exporter?.dispose();await f.close()}
});

test("历史交付只导出既有签署，静态通过不伪造实测、不改冻结文件与费用",async()=>{
  const f=await fixture();try{
    const run=JSON.parse(await f.call()) as VerificationRun;await f.svc.recordVerdicts(f.t.id,[f.verdict([run.id])],run.artifactHashes,f.roundId);
    await f.store.transact(d=>{delete d.collections[DELIVERY_EVIDENCE];delete d.collections[VERIFICATION_RUNS];});
    const budget=JSON.stringify(f.ledger.getBudget(f.t.id));await queueHistoricalEvidence(f.store);
    const row=f.store.list<DeliveryEvidence>(DELIVERY_EVIDENCE)[0]!;assert.equal(row.snapshot.historical,true);assert.equal(row.snapshot.runs.length,0);assert.ok(row.snapshot.missingRuntimeEvidence.length>0);
    const paths=await exportDeliveryEvidence(row,join(f.dir,'history'));assert.match(await fs.readFile(paths.report,'utf8'),/不代表新增实测|缺口/);
    assert.equal(await fs.readFile(join(f.project,'app.txt'),'utf8'),'frozen source');assert.equal(f.svc.getTask(f.t.id)!.status,'delivered');assert.equal(JSON.stringify(f.ledger.getBudget(f.t.id)),budget);
    await queueHistoricalEvidence(f.store);assert.equal(f.store.list(DELIVERY_EVIDENCE).length,1);
  }finally{await f.close()}
});
