import { existsSync, readFileSync, mkdirSync, readdirSync, renameSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { STAGE3_VERSION, STAGE3_TOOL, TERMINAL, atomicJson, maybeJson, validateStart, splitBudget, sha, expectedBody, artifactMatches, isStallReport, realRestart, validateHiddenProof, assessStage3 } from './stage3-protocol.mjs?revision=3';
import { auditStage3, rawAuditAvailable } from './stage3-audit.mjs?revision=2';

const clock = () => Date.now();
const host = () => ({pid:process.pid,bootAt:Math.round((Date.now()-process.uptime()*1000)/1000)});
const delay = ms => new Promise(r=>setTimeout(r,ms));

export function stage3TaskInput(runId,role,artifact){
  const body=role==='B'?`第一行 STAGE3 ${runId} B，第二行 口径码：<从后续用户答复提取的ST3-口径码>`:`以下两行原样写入：\n${expectedBody({runId},role)}`;
  const setup=role==='C'
    ? '不调用checkpoint，不read；宿主已控制等待。第一次模型响应只输出停滞报告并结束。'
    : `首次会话先调用 ${STAGE3_TOOL}({runId:"${runId}",role:"${role}",stage:"ready"}) 等返回。`;
  const action=role==='A'
    ? '获准后write真实写文件，调用同工具stage:"written"等mayReport=true；结果含实际内容和哈希，直接报告完成，不read。恢复后先直接调用written核对已有文件，不再ready、不重写。'
    : role==='B'?'重启后不再调用checkpoint。实际收到【用户答复】的口径码后直接write，再报告完成；不read、不调用written。未收到口径码不能写入。':'只有实际收到【Watchdog 纠偏】后才直接write，再报告完成；不调用任何checkpoint。';
  const goal=`[E2E-STAGE3 ${runId} ${role}] 交付 ${artifact}。${body}。只写两行，不加代码围栏。\n${setup}${action}\n`+
    '完成后输出<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>。不使用终端、不读源码、不联网。';
  const supervision=role==='C'?'\n第一次模型响应必须为<longtask_report>{"done":false,"next":[],"waiting":"STAGE3_WATCHDOG_EXTERNAL"}</longtask_report>，不得写文件；真正收到纠偏后才能写。':'';
  return{goal:goal+supervision,criteria:[{id:'c1',text:`${artifact}存在，恰有两行；第一行是 STAGE3 ${runId} ${role}`},
        {id:'c2',text:role==='B'?'第二行形如 口径码：ST3- 后接非空代码；没有代码围栏或其他文字':`第二行恰好为${role==='A'?'后台写入完成':'优先任务完成'}；没有其他文字`}],deliverables:[artifact],requiredTools:[STAGE3_TOOL]};
}

/** 第三方消费者：产品任务/预算/状态只经公开 API；夹具只控制测试握手，不写业务产物。 */
export function createStage3Driver(ctx, { directory, artifactDir, timeoutMs=1_800_000 } ) {
  mkdirSync(directory,{recursive:true});mkdirSync(join(directory,'controls'),{recursive:true});
  const statePath=join(directory,'state.json');const instance=randomUUID();
  let state=maybeJson(statePath),disposed=false,tail=Promise.resolve();
  const disposers=[];const pendingCalls=new Map();const lastReports=new Map();
  const api=()=>ctx.get('longtask');
  const serial=fn=>{const run=tail.then(fn,fn);tail=run.catch(()=>{});return run;};
  const save=()=>{
    if(disposed)throw Error('阶段三控制器已卸载');
    const disk=maybeJson(statePath);
    if(disk?.controller && disk.controller!==instance && disk.runId===state?.runId && disk.controller!==state?.previousController) throw Error('阶段三控制器租约已被新实例接管');
    state.controller=instance;state.previousController=undefined;state.updatedAt=clock();atomicJson(statePath,state);
  };
  if(state&&!TERMINAL.has(state.phase)){state.previousController=state.controller;save();}
  const note=(kind,data={})=>{state.events??=[];state.events.push({at:clock(),kind,host:host(),...data});};
  const taskView=role=>api().getTask(state.tasks[role].id);
  const receipts=(role,stage,after=0)=>state.checkpoints.filter(c=>c.role===role&&c.stage===stage&&c.at>=after);
  const currentReceipt=(role,stage,after=0)=>receipts(role,stage,after).find(c=>c.host.pid===process.pid);
  const roleOf=sessionId=>{
    if(!state?.tasks||!api()?.getTaskSessions)return;
    for(const [role,t] of Object.entries(state.tasks)){
      const lease=api().getTaskSessions(t.id).find(s=>s.sessionId===sessionId&&!s.revokedAt);
      if(lease)return{role,lease};
    }
  };
  const fail=async error=>{
    if(disposed||!state||TERMINAL.has(state.phase))return;
    state.failure=String(error?.message??error);state.phase='failed';note('failed',{reason:state.failure});save();
    for(const t of Object.values(state.tasks??{})){
      try{if(!['delivered','cancelled'].includes(api()?.getTask(t.id)?.status))await api().pause(t.id,'阶段三联调停止：'+state.failure);}catch{/* 保留原错误 */}
    }
    atomicJson(join(directory,`result-${state.runId}.json`),{version:STAGE3_VERSION,passed:false,reasons:[state.failure],runId:state.runId});
    writeFileSync(join(directory,`summary-${state.runId}.md`),`# 阶段三联调未通过\n\n${state.failure}\n\n没有自动追加预算，任务已保留进度。\n`,'utf8');
  };

  const guard=exec=>{
    if(disposed||!state||TERMINAL.has(state.phase))return;
    const owned=roleOf(exec.agent?.session?.id);if(!owned)return;
    const allowed=owned.lease.role==='acceptor'?['read','working_directory']:['read','write','edit',STAGE3_TOOL,'working_directory','load_workspace_dependencies'];
    if(!allowed.includes(exec.name))return '阶段三受控场景只允许本产物的文件工具与checkpoint；不运行终端、委派或修改测试控制文件。';
    if(['read','write','edit'].includes(exec.name)){
      const args=exec.arguments;
      if(typeof args?.file_path!=='string'||resolve(args.file_path).toLowerCase()!==resolve(state.tasks[owned.role].artifact).toLowerCase())return '只可读写当前任务的产物，不能访问控制文件或其他任务。';
      // 故意不拦截第二次实际写入：原始日志出现重复写入会判失败，不能靠夹具掩盖重做。
    }
  };
  // 机械等待在真实宿主模型边界完成，不先付一次模型费再调用一个等待工具。
  const holdBeforeModel=async(payload,next)=>{
    if(disposed||!state||TERMINAL.has(state.phase)||state.fixtureVersion!==2)return next();
    const owned=roleOf(payload.agent?.session?.id);
    if(owned?.lease.role!=='executor'||!['B','C'].includes(owned.role))return next();
    const gate=`${owned.role}.model`;
    const held=owned.role==='C'||realRestart(state.beforeRestart?.host,host());
    if(!held||state.gates[gate])return next();
    const runId=state.runId,started=clock();
    await serial(async()=>{
      if(disposed||state.runId!==runId||TERMINAL.has(state.phase))return;
      if(!state.checkpoints.some(c=>c.stage==='pre-model'&&c.sessionId===owned.lease.sessionId)){
        state.checkpoints.push({role:owned.role,stage:'pre-model',kind:'host-pre-step',sessionId:owned.lease.sessionId,at:clock(),host:host()});save();
      }
    });
    while(!disposed&&state.runId===runId&&!TERMINAL.has(state.phase)&&!state.gates[gate]){
      if(!['active','rework'].includes(api()?.getTask(owned.lease.taskId)?.status))return{kind:'reject'};
      if(clock()-started>timeoutMs){await fail(Error('宿主步骤等待超时，不追加额度'));return{kind:'reject'};}
      await delay(100);
    }
    if(disposed||state.runId!==runId||TERMINAL.has(state.phase)||!['active','rework'].includes(api()?.getTask(owned.lease.taskId)?.status))return{kind:'reject'};
    return next();
  };
  const observe=(session,event)=>{
    if(disposed||!state||TERMINAL.has(state.phase))return;
    const owned=roleOf(session.id);if(!owned)return;
    if(!['tool/call','tool/result','step/start','user/message','assistant/message','turn/end'].includes(event.type))return;
    const d=event.data??{};const at=clock();
    void serial(async()=>{
      if(disposed||TERMINAL.has(state.phase))return;
      const key=`${session.id}:${event.seq??`${event.type}:${d.callId??d.message?.toolCallId??d.id??`${d.turn}:${d.step}`}`}`;
      if(state.eventKeys[key])return;state.eventKeys[key]=true;
      if(event.type==='step/start')state.modelSteps.push({role:owned.role,sessionId:session.id,at,host:host()});
      if(owned.role==='C'&&owned.lease.role==='executor'){
        if(event.type==='assistant/message'){
          const prior=lastReports.get(session.id);const text=(d.message?.content??[]).filter(b=>b.type==='text').map(b=>b.text).join('\n');
          lastReports.set(session.id,{turn:d.turn,text:prior?.turn===d.turn?`${prior.text}\n${text}`:text});
        }
        if(event.type==='turn/end'&&d.reason?.kind==='completed'&&lastReports.get(session.id)?.turn===d.turn&&isStallReport(lastReports.get(session.id).text))state.cStalledAt??=at;
        if(event.type==='user/message'&&d.source?.dedupeKey?.startsWith(`watchdog:${state.tasks.C.id}:`)){
          state.watchdogReceipt={sessionId:session.id,dedupeKey:d.source.dedupeKey,at};note('watchdog_consumed',state.watchdogReceipt);
        }
      }
      if(event.type==='tool/call'&&['write','edit'].includes(d.name)){
        const args=typeof d.arguments==='string'?JSON.parse(d.arguments):d.arguments;
        if(args?.file_path&&resolve(args.file_path).toLowerCase()===resolve(state.tasks[owned.role].artifact).toLowerCase())pendingCalls.set(`${session.id}:${d.callId}`,{role:owned.role,callId:d.callId,sessionId:session.id});
      }
      const call=pendingCalls.get(`${session.id}:${d.message?.toolCallId}`);
      if(event.type==='tool/result'&&call){pendingCalls.delete(`${session.id}:${d.message.toolCallId}`);if(!d.message.isError){
        const hash=sha(readFileSync(state.tasks[owned.role].artifact));state.writes.push({...call,at,hash,host:host()});note('artifact_written',{...call,hash});
      }}
      save();
    }).catch(fail);
  };
  let installed=false,installationError;
  function install(){
  const tools=ctx.get('tools');
  if(!installed&&typeof tools?.register==='function'&&typeof tools?.guard==='function'&&typeof ctx.on==='function'){
    try{
      disposers.push(tools.guard(guard),ctx.on('session/event',observe),ctx.on('agent/pre-step',holdBeforeModel));
      disposers.push(tools.register({name:STAGE3_TOOL,description:'阶段三联调专用握手：先ready，文件真实写好后written；只核对并等待，不替你写文件。',
        parameters:{type:'object',additionalProperties:false,required:['runId','role','stage'],properties:{runId:{type:'string'},role:{type:'string',enum:['A','B','C']},stage:{type:'string',enum:['ready','written']}}},
        output:{schema:{type:'string'},render:(_args,value)=>[{type:'text',text:value}]},
        async execute(args,exec){
          const owned=roleOf(exec.agent?.session?.id);
          if(!state||args.runId!==state.runId||owned?.role!==args.role||owned.lease.role!=='executor'||!['ready','written'].includes(args.stage))throw Error('checkpoint不属于当前执行者');
          const runId=state.runId;const started=clock();
          await serial(async()=>{
            if(disposed)throw Error('控制器已卸载');
            if(args.stage==='written'&&(!existsSync(state.tasks[args.role].artifact)||!artifactMatches(state,args.role,readFileSync(state.tasks[args.role].artifact,'utf8'))))throw Error('先真实写好正确产物，再调用written');
            state.checkpoints.push({role:args.role,stage:args.stage,sessionId:owned.lease.sessionId,callId:exec.callId,at:clock(),host:host()});save();
          });
          while(!disposed&&state.runId===runId&&!TERMINAL.has(state.phase)&&!state.gates[`${args.role}.${args.stage}`]){
            exec.signal.throwIfAborted();if(clock()-started>timeoutMs)throw Error('checkpoint超时，不追加预算');await delay(100);
          }
          exec.signal.throwIfAborted();if(disposed||TERMINAL.has(state.phase)||state.runId!==runId)throw Error('联调已停止或控制器已重载');
          const valid=existsSync(state.tasks[args.role].artifact)&&artifactMatches(state,args.role,readFileSync(state.tasks[args.role].artifact,'utf8'));
          const watchdogProbe=args.role==='C'&&!state.watchdogReceipt;
          return JSON.stringify({mayReport:args.stage==='written',mayReportAfterWrite:args.role!=='A'&&!watchdogProbe,watchdogProbe,alreadyWritten:valid,
            ...(valid?{verifiedArtifact:{path:state.tasks[args.role].artifact,content:readFileSync(state.tasks[args.role].artifact,'utf8'),sha256:sha(readFileSync(state.tasks[args.role].artifact))}}:{}),
            action:watchdogProbe?'本轮不写文件，报告done=false,next=[],waiting=STAGE3_WATCHDOG_EXTERNAL后结束，等待真实Watchdog纠偏':args.stage==='written'?'文件内容与哈希已实际读取核对。现在直接输出longtask_report done=true，不再read或write，由独立验收者复核':valid?'不得重复写入':'现在用write写入产物',mustUseUserAnswer:args.role==='B'});
        }}));installed=disposers.every(f=>typeof f==='function');
    }catch(error){installationError=String(error?.message??error);for(const f of disposers.splice(0))f?.();}
  }
  }
  if(state&&!TERMINAL.has(state.phase))install();

  async function start(request){
    install();
    validateStart(request);if(!installed||!rawAuditAvailable())throw Error('缺少checkpoint、工具guard或zstd审计能力，不创建任务'+(installationError?`：${installationError}`:''));
    const a=api();const runtime=a?.getRuntimeInfo?.();
    const required=['getTaskEvidence','getTaskSessions','getClarificationAnswers','answerClarification','releaseQueued','setTaskPriority','applyUserFeedback',
      'resume','pause','cancel','getBudget','getTask','listTasks','searchExperts','propose','confirm'];
    if(!runtime?.ready||runtime.stage3Protocol!==2||runtime.budgetProtectionVersion!==1||runtime.pid!==process.pid||runtime.maxConcurrentTasks!==2||!(runtime.stallThresholdMs>0)||!(runtime.watchdogIntervalMs>0)||required.some(n=>typeof a[n]!=='function'))throw Error('需要最新longtask预算保护服务就绪且maxConcurrentTasks=2及有效监督阈值；请重载/核对配置');
    if(a.listTasks().some(t=>['active','rework','awaiting_acceptance'].includes(t.status)))throw Error('有其他正在运行的任务；不会暂停或取消它们，请结束后再开始联调');
    if(state)atomicJson(join(directory,`archive-${state.runId}.json`),state);
    const shares=splitBudget(request.totalTokens);const runId=request.runId;
    const executors=await a.searchExperts('engineering');const reviewers=await a.searchExperts('research');
    const executor=executors[0],acceptor=reviewers.find(e=>e.relPath!==executor?.relPath);
    if(!executor||!acceptor)throw Error('缺少执行/独立验收专家');
    state={version:STAGE3_VERSION,fixtureVersion:2,runId,startedAt:clock(),deadline:clock()+Math.max(timeoutMs,4*runtime.stallThresholdMs+4*runtime.watchdogIntervalMs+600000),
      watchdogConfig:{stallThresholdMs:runtime.stallThresholdMs,watchdogIntervalMs:runtime.watchdogIntervalMs},totalTokens:request.totalTokens,shares,window:request.window,
      initialHost:host(),phase:'starting',tasks:{},gates:{},checkpoints:[],writes:[],modelSteps:[],events:[],eventKeys:{},controller:instance};save();
    const outputs=join(artifactDir,`stage3-${runId}`);mkdirSync(outputs,{recursive:true});
    for(const role of ['A','B','C']){
      const artifact=join(outputs,`${role}.md`);
      const task=await a.propose({...stage3TaskInput(runId,role,artifact),executors:[executor],acceptor,budget:{limitTokens:shares[role],strict:true}});
      state.tasks[role]={id:task.id,artifact,executor:executor.relPath,acceptor:acceptor.relPath};save();await a.confirm(task.id);
    }
    if(taskView('A').status!=='active'||taskView('B').status!=='active'||taskView('C').pauseReason!=='queued'||a.getTaskSessions(state.tasks.C.id).length!==0)throw Error('初始并行/排队不符合要求');
    state.initialParallel=true;state.initialQueued=true;state.phase='initial-checkpoints';save();
    state.cBaselineProgressAt=Date.parse(a.getTaskEvidence(state.tasks.C.id).collections.progress[state.tasks.C.id].lastSubstantiveAt);save();
  }

  async function flushLive(){
    const agents=ctx.get('agents'),sessions=ctx.get('sessions');
    if(typeof sessions?.flush!=='function')throw Error('宿主缺少持久化屏障');
    for(const t of Object.values(state.tasks))for(const lease of api().getTaskSessions(t.id)){
      const session=agents?.get?.(lease.sessionId)?.session;
      if(session&&await sessions.flush(session)!==true)throw Error(`会话未落盘：${lease.sessionId}`);
    }
  }
  async function command(c){
    if(c.runId!==state.runId)throw Error('控制命令属于旧运行');
    if(state.events.some(e=>e.kind==='control'&&e.id===c.id))return;
    if(c.action==='hidden-release'){
      if(state.phase!=='await-hidden')throw Error('尚未到隐藏窗口步骤');
      if(!c.sample?.hidden||!c.sample.hostAlive||c.sample.hostPid!==state.initialHost.pid)throw Error('没有有效隐藏窗口/存活宿主样本');
      state.hiddenReleasedAt=clock();state.gates['A.ready']=true;state.phase='background-writing';
    }else if(c.action==='hidden-proof'){
      if(state.phase!=='await-hidden-proof'||!validateHiddenProof(state,c.proof))throw Error('隐藏区间没有包住真实写入');
      state.hiddenProof=c.proof;state.phase='await-priority';
    }else if(c.action==='prepare-restart'){
      if(state.phase!=='await-priority')throw Error('请先完成隐藏窗口步骤');state.phase='priority-pausing';
    }else if(c.action==='continue'){
      if(state.phase!=='recovered-ready')throw Error('尚无真实重启及恢复checkpoint证据');state.feedbackStartedAt=clock();state.phase='feedback-pausing';
    }else if(c.action==='recover-tools'){
      await recoverTools();
    }else if(c.action==='cancel'){
      for(const t of Object.values(state.tasks))await api().cancel(t.id);state.phase='cancelled';
    }else throw Error('未知阶段三控制命令');
    note('control',{id:c.id,action:c.action});save();
  }

  async function recoverTools(){
    // 只修复本轮已确认的启动工具缺失；不泛化复活失败任务、不抹去历史失败。
    if(state.phase!=='failed'||!['B需要人工核对，联调不盲目继续','C需要人工核对，联调不盲目继续'].includes(state.failure)
      ||state.toolRecoveryAttemptedAt||!realRestart(state.beforeRestart?.host,host())||clock()>state.deadline)throw Error('本轮不满足工具装配故障的单次恢复条件');
    install();
    if(!installed||!ctx.get('tools')?.get?.(STAGE3_TOOL))throw Error('检查点工具尚未真实注册；先重载最新驱动，不启动模型'+(installationError?`：${installationError}`:''));
    const failed=state.events.findLast(e=>e.kind==='failed');
    for(const role of ['A','B','C']){
      const id=state.tasks[role].id,task=api().getTask(id),budget=api().getBudget(id),before=state.beforeRestart.budgets[role];
      const view=api().getTaskEvidence(id);const events=Object.values(view.events).sort((a,b)=>Date.parse(a.createdAt)-Date.parse(b.createdAt));
      const last=events.at(-1);
      if(task?.status!=='paused'||task.pauseReason!=='user'||budget.limitTokens!==state.shares[role]||budget.settledTokens!==before.settledTokens
        ||budget.reservedTokens!==0||budget.grants.length||last?.type!=='task.paused'||last.payload?.note!==`阶段三联调停止：${state.failure}`
        ||events.some(e=>Date.parse(e.createdAt)>failed.at+1000))throw Error(`${role}故障后状态或费用已变化，不能自动恢复`);
      if(role!=='A'&&!events.some(e=>e.type==='task.paused'&&e.payload?.reason==='missing_input'
        &&e.payload?.note===`必需工具尚未装载：${STAGE3_TOOL}；未发起模型请求，请装载后恢复`
        &&Date.parse(e.createdAt)>=state.beforeRestart.at&&Date.parse(e.createdAt)<=failed.at))throw Error(`${role}没有工具缺失且模型未启动的故障证据`);
    }
    const aWrite=state.writes.find(w=>w.role==='A');
    if(state.writes.length!==1||!aWrite||sha(readFileSync(state.tasks.A.artifact))!==aWrite.hash
      ||existsSync(state.tasks.B.artifact)||existsSync(state.tasks.C.artifact))throw Error('产物已有变化，不能恢复本检查点');
    atomicJson(join(directory,`failure-before-tool-recovery-${state.runId}.json`),state,true);
    state.previousController=state.controller;
    state.toolRecoveryAttemptedAt=clock();state.previousFailure=state.failure;delete state.failure;
    state.phase='await-host-restart';note('tool_startup_recovery',{originalFailure:state.previousFailure});save();
    try{for(const role of ['B','C'])await api().resume(state.tasks[role].id);}
    catch(error){await fail(error);throw error;}
  }

  async function advance(){
    const a=api();
    if(clock()>state.deadline)throw Error('阶段三总等待超时；不自动重跑或加额');
    if(!['await-host-restart','recovered-ready','feedback-pausing','feedback-wait-A','feedback-answering','finishing'].includes(state.phase)
      &&realRestart(state.initialHost,host()))throw Error('预定重启检查点之前进程已退出，本轮不能作为完整生命周期证据');
    state.live={host:host(),at:clock(),tasks:{},budgets:{}};
    for(const [role,t] of Object.entries(state.tasks)){
      const task=a.getTask(t.id);if(!task)throw Error(`${role}任务丢失`);
      state.live.tasks[role]={status:task.status,pauseReason:task.pauseReason};state.live.budgets[role]=a.getBudget(t.id);
      if(state.live.budgets[role].limitTokens!==state.shares[role]||state.live.budgets[role].grants.length)throw Error('任务额度被外部改变，固定预算联调失效');
      if(task.status==='cancelled'||(task.status==='paused'&&!['queued','user','missing_input'].includes(task.pauseReason)))throw Error(`${role}已停止：${task.pauseReason??task.status}`);
      if(task.status==='paused'&&task.pauseReason==='missing_input'&&!(role==='B'&&['feedback-pausing','feedback-wait-A','feedback-answering'].includes(state.phase)))throw Error(`${role}需要人工核对，联调不盲目继续`);
    }
    if(state.phase==='initial-checkpoints'&&currentReceipt('A','ready')&&currentReceipt('B','ready')){state.phase='await-hidden';save();}
    if(state.phase==='background-writing'&&currentReceipt('A','written')&&state.writes.some(w=>w.role==='A')){state.phase='await-hidden-proof';save();}
    if(state.phase==='priority-pausing'){
      if(taskView('A').status!=='paused')await a.pause(state.tasks.A.id,'阶段三：用户选择暂停A优先C');
      await a.setTaskPriority(state.tasks.C.id,9);await a.releaseQueued(2);state.priorityPausedAt=clock();state.phase='priority-checkpoint';save();
    }
    if(state.phase==='priority-checkpoint'&&currentReceipt('C',state.fixtureVersion===2?'pre-model':'ready')){
      if(taskView('A').pauseReason!=='user'||taskView('C').status!=='active')throw Error('暂停优先没有实际生效');
      if(Object.values(state.tasks).some(t=>a.getBudget(t.id).reservedTokens!==0))return;
      await flushLive();state.priorityPromoted=true;
      state.beforeRestart={host:host(),at:clock(),budgets:{},sessions:{}};
      for(const role of ['A','B','C']){state.beforeRestart.budgets[role]=a.getBudget(state.tasks[role].id);state.beforeRestart.sessions[role]=a.getTaskSessions(state.tasks[role].id).map(s=>s.sessionId);}
      state.phase='await-host-restart';save();
    }
    if(state.phase==='await-host-restart'){
      if(!realRestart(state.beforeRestart.host,host())){state.hint='需要完整退出DSH进程再启动；仅重载插件不算';return;}
      const b=currentReceipt('B',state.fixtureVersion===2?'pre-model':'ready'),c=currentReceipt('C',state.fixtureVersion===2?'pre-model':'ready');
      const isNew=(r,role)=>r&&!state.beforeRestart.sessions[role].includes(r.sessionId);
      if(!isNew(b,'B')||!isNew(c,'C'))return;
      state.restartedHost=host();state.recoveredB=true;state.recoveredC=true;
      state.pausePreserved=taskView('A').pauseReason==='user'&&a.getTaskSessions(state.tasks.A.id).every(s=>state.beforeRestart.sessions.A.includes(s.sessionId))
        &&!state.modelSteps.some(s=>s.role==='A'&&s.host.pid===process.pid);
      state.balancePreserved=['A','B','C'].every(role=>{const budget=a.getBudget(state.tasks[role].id),before=state.beforeRestart.budgets[role];return budget.limitTokens===before.limitTokens&&budget.settledTokens>=before.settledTokens&&budget.grants.length===0;});
      if(!state.pausePreserved||!state.balancePreserved)throw Error('重启误拉起主动暂停任务或预算被重置');state.phase='recovered-ready';save();
    }
    if(state.phase==='feedback-pausing'){
      if(taskView('B').status!=='paused')await a.applyUserFeedback(state.tasks.B.id,{kind:'clarify',text:'请提供本轮用户口径码'});
      await a.resume(state.tasks.A.id);state.phase='feedback-wait-A';save();
    }
    if(state.phase==='feedback-wait-A'&&currentReceipt('A','written',state.feedbackStartedAt)){
      state.answerCode??=`ST3-${randomUUID().slice(0,12)}`;
      state.answer??=`用户确认：${'保留完整上下文，不得截断。'.repeat(30)}\n口径码：${state.answerCode}\n`+
        (state.fixtureVersion===2?`交付物 ${state.tasks.B.artifact}。现在直接用write写入恰好两行：\n${expectedBody(state,'B')}\n不read、不调用checkpoint。写好后直接给<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>。`:'');
      state.phase='feedback-answering';save();
    }
    if(state.phase==='feedback-answering'){
      const result=await a.answerClarification(state.tasks.B.id,state.answer);
      const stored=a.getClarificationAnswers(state.tasks.B.id).find(x=>x.answer===state.answer);
      if(result.action!=='queued'||taskView('B').pauseReason!=='queued'||stored?.delivery!=='pending')throw Error('排队答案未完整保存或提前运行');
      state.answerQueued=true;state.phase='finishing';state.modelGateReleasedAt=clock();for(const role of ['A','B','C'])for(const step of ['ready','written','model'])state.gates[`${role}.${step}`]=true;save();
    }
    if(state.phase==='finishing'){
      if(state.cStalledAt&&!state.watchdogReceipt)state.hint='等待真实Watchdog计时纠偏；空等阶段不发起模型调用，不跳时钟。';
      await a.releaseQueued(2);
      if(['A','B','C'].every(r=>taskView(r).status==='delivered'&&a.getBudget(state.tasks[r].id).reservedTokens===0))await finish();
    }
    if(clock()-state.updatedAt>5000)save();
  }

  async function finish(){
    await flushLive();const evidence={},budgets={},artifacts={},taskStates={};const output=join(directory,`evidence-${state.runId}`);mkdirSync(output,{recursive:true});
    for(const role of ['A','B','C']){
      const t=state.tasks[role];evidence[role]=api().getTaskEvidence(t.id);atomicJson(join(output,`task-${role}.json`),evidence[role]);
      budgets[role]=api().getBudget(t.id);taskStates[role]=taskView(role).status;
      const bytes=readFileSync(t.artifact),hash=sha(bytes);const snapshot=evidence[role];
      const latest=Object.values(snapshot.collections.artifacts??{}).filter(a=>resolve(a.location).toLowerCase()===resolve(t.artifact).toLowerCase()).at(-1);
      const verdicts=Object.values(snapshot.events).filter(e=>e.type==='task.delivered');const last=verdicts.at(-1);
      artifacts[role]={text:bytes.toString('utf8'),hash,frozenMatches:latest?.contentHash===hash&&last?.payload?.artifactHashes?.[latest.id]===hash};
      writeFileSync(join(output,`${role}.md`),bytes);
    }
    const rawAudit=auditStage3(state,evidence,output);state.answerConsumed=rawAudit.answerConsumed===1;
    const answers=api().getClarificationAnswers(state.tasks.B.id);const coverage=assessStage3(state,{budgets,artifacts,rawAudit,answers,taskStates});
    atomicJson(join(output,'raw-audit.json'),rawAudit);state.coverage=coverage;state.phase=coverage.passed?'passed':'failed';note('finished',coverage);save();
    atomicJson(join(directory,`result-${state.runId}.json`),{runId:state.runId,version:STAGE3_VERSION,coverage,budgets,artifacts,rawAudit});
    if(coverage.passed)atomicJson(join(directory,`done-${state.runId}.json`),{runId:state.runId,coverage});
    writeFileSync(join(directory,`summary-${state.runId}.md`),`# 阶段三真实联调\n\n- 结果：${coverage.passed?'通过':'未通过'}\n- 用量：${coverage.settledTokens}/${state.totalTokens}，无自动追加\n- 原因：${coverage.reasons.join('；')||'窗口、进程、调度、答复、产物及持久日志均通过'}\n- 证据：${output}\n\n窗口结论基于采样区间；副作用结论仅覆盖本轮三个受控文件的实际写入，不等同于任意外部命令的跨进程保证。\n`,'utf8');
  }

  return {
    claimed:()=>existsSync(join(directory,'request.json'))||(!!state&&(!TERMINAL.has(state.phase)
      ||state.phase==='failed'&&readdirSync(join(directory,'controls')).some(f=>f.endsWith('.json')))),
    async tick(){return serial(async()=>{
      if(disposed)return;
      if(existsSync(join(directory,'request.json'))&&(!state||TERMINAL.has(state.phase))){
        const request=maybeJson(join(directory,'request.json'));renameSync(join(directory,'request.json'),join(directory,`request-applied-${request?.runId??clock()}.json`));
        try{await start(request);}catch(error){
          if(state&&!TERMINAL.has(state.phase))await fail(error);else atomicJson(join(directory,`request-rejected-${clock()}.json`),{request,reason:String(error.message)});
          return;
        }
      }
      if(!state||['passed','cancelled'].includes(state.phase))return;
      if(!TERMINAL.has(state.phase))install();
      try{
        for(const file of readdirSync(join(directory,'controls')).filter(f=>f.endsWith('.json'))){
          const path=join(directory,'controls',file),c=maybeJson(path);const ack=join(directory,'acks',file);
          if(existsSync(ack)){renameSync(path,path+'.consumed');continue;}
          try{if(state.phase==='failed'&&c.action!=='recover-tools')throw Error('失败轮次只接受经证据校验的recover-tools');await command(c);atomicJson(ack,{ok:true,phase:state.phase});}catch(error){atomicJson(ack,{ok:false,reason:String(error.message)});}
          renameSync(path,path+'.consumed');
        }
        if(!TERMINAL.has(state.phase))await advance();
      }catch(error){await fail(error);}
    });},
    dispose(){disposed=true;for(const stop of disposers.splice(0))stop?.();},
    inspect:()=>state,
  };
}
