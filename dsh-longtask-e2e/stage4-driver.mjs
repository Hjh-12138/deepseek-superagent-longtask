import {existsSync,mkdirSync,readFileSync,writeFileSync,renameSync} from 'node:fs';
import {join,resolve} from 'node:path';import {randomUUID} from 'node:crypto';
import {atomicJson,maybeJson,sha} from './stage3-protocol.mjs';
import {STAGE4_VERSION,STAGE4_TOOL,STAGE4_TERMINAL,normalizeCrash,stage4Spec,assessStage4,documentInstructions,DOC_REPORT} from './stage4-protocol.mjs?revision=2';
import {validateDevelopment,validateDocument} from './stage4-validation.mjs';
import {auditStage4} from './stage4-audit.mjs';
import {rawAuditAvailable} from './stage3-audit.mjs';
const delay=ms=>new Promise(r=>setTimeout(r,ms));
const identity=()=>({pid:process.pid,bootAt:Math.round((Date.now()-process.uptime()*1000)/1000)});

/** 仅第三方验收消费者。exitProcess/hostIdentity只供离线测试注入，不来自运行请求/插件配置。 */
export function createStage4Driver(ctx,{directory,artifactDir,markerPath=join(directory,'../crash-pending.json'),timeoutMs=1500000,
  exitProcess=code=>process.exit(code),hostIdentity=identity,sessionRoot,settleMs=2000}={}){
  mkdirSync(directory,{recursive:true});const statePath=join(directory,'state.json'),requestPath=join(directory,'request.json');
  let state=maybeJson(statePath),disposed=false,installed=false,timer,freezing,heldAgent,restoreFollowup;const stops=[];
  let tail=Promise.resolve();const serial=fn=>{const p=tail.then(fn,fn);tail=p.catch(()=>{});return p;};const docPrimed=new Set();
  const notice=(text,key)=>({id:randomUUID(),role:'user',source:{kind:'longtask',dedupeKey:key},content:[{type:'text',text}]});
  const api=()=>ctx.get('longtask'),active=()=>state&&!STAGE4_TERMINAL.has(state.phase),host=()=>hostIdentity();
  const save=()=>{if(disposed)throw Error('阶段四已卸载');state.updatedAt=Date.now();atomicJson(statePath,state);};
  const note=(kind,data={})=>{state.events??=[];state.events.push({kind,at:Date.now(),host:host(),...data});};
  const clearFault=()=>{if(timer)clearTimeout(timer);timer=undefined;restoreFollowup?.();restoreFollowup=undefined;};
  const owned=sid=>state?.taskId&&api()?.getTaskSessions?.(state.taskId).find(s=>s.sessionId===sid&&!s.revokedAt);
  const view=()=>api().getTaskEvidence(state.taskId);
  const fail=async error=>{
    if(disposed||!active())return;clearFault();state.failure=String(error?.message??error);state.phase='failed';note('failed',{reason:state.failure});save();
    try{const task=api()?.getTask(state.taskId);if(task&&['active','rework','awaiting_acceptance'].includes(task.status))await api().pause(state.taskId,'阶段四停止：'+state.failure);}catch{}
    atomicJson(join(directory,`result-${state.runId}.json`),{runId:state.runId,passed:false,reasons:[state.failure]});
    writeFileSync(join(directory,`summary-${state.runId}.md`),`# 阶段四未通过\n\n${state.failure}\n\n不自动重跑或追加额度。\n`);
  };
  const flush=async()=>{
    const service=ctx.get('sessions');if(typeof service?.flush!=='function')throw Error('宿主没有持久化屏障');
    for(const lease of api().getTaskSessions(state.taskId)){const agent=ctx.get('agents')?.get?.(lease.sessionId);if(agent?.session&&await service.flush(agent.session)!==true)throw Error('会话未落盘 '+lease.sessionId);}
  };
  const images=()=>Object.fromEntries(state.paths.filter(existsSync).map(p=>[p,sha(readFileSync(p))]));
  const snapshot=()=>{
    const evidence=view(),b=evidence.collections.budgets[state.taskId];
    return{evidence,budget:b,reservations:Object.values(evidence.collections.reservations??{}).filter(r=>r.state==='reserved'),
      sessionIds:api().getTaskSessions(state.taskId).map(s=>s.sessionId),artifactHashes:images()};
  };
  const freezeFault=async trigger=>{
    if(freezing)return freezing;
    if(!active()||state.phase!=='running'||!state.crash||state.crashEvidence)throw Error('故障点未授权或已消费');
    state.phase='crash-freezing';clearFault();save();
    freezing=(async()=>{
      if(state.crash.at==='after-write'){
        const until=Date.now()+2000;while(api().getBudget(state.taskId).reservedTokens>0&&Date.now()<until&&!disposed)await delay(10);
        if(api().getBudget(state.taskId).reservedTokens>0)throw Error('写后响应结算尚未稳定，未执行故障退出');
      }
      await flush();
      if(disposed||state.phase!=='crash-freezing')return;
      const before=snapshot();
      if(state.crash.at==='model-inflight'&&(before.reservations.length!==1||before.budget.reservedTokens<=0
        ||before.reservations[0].tokens!==before.budget.reservedTokens||trigger.step?.sessionId!==trigger.sessionId
        ||!Number.isSafeInteger(trigger.step?.turn)||!Number.isSafeInteger(trigger.step?.step)))throw Error('未命中真实在途预留或步骤身份');
      if(state.crash.at==='after-write'&&(!before.artifactHashes[state.paths[0]]||!trigger.callId))throw Error('未命中实际文件写入');
      if(state.crash.at==='delivery-unknown'&&!Object.values(before.evidence.collections.clarificationAnswers??{}).some(a=>a.delivery==='unknown'&&a.id===trigger.unknown?.answerId))throw Error('未知投递尚未落盘');
      await flush();
      if(disposed||state.phase!=='crash-freezing')return;
      const task=api().getTask(state.taskId);
      if(!task||!(state.crash.at==='delivery-unknown'?task.status==='paused'&&task.pauseReason==='missing_input':['active','rework'].includes(task.status)))throw Error('任务已结束或取消，撤销退出');
      const b=api().getBudget(state.taskId);if(b.limitTokens!==state.budgetTokens||b.grants?.length)throw Error('固定预算已变更，撤销退出');
      if(existsSync(join(directory,'cancel.json')))throw Error('用户已请求停止，撤销退出');
      if(api().listTasks().some(t=>t.id!==state.taskId&&['active','rework','awaiting_acceptance'].includes(t.status)))throw Error('发现其他运行中长任务，撤销故障退出');
      const evidence={version:STAGE4_VERSION,runId:state.runId,taskId:state.taskId,pid:host().pid,host:host(),at:Date.now(),fired:true,kind:state.crash.at,...trigger,before};
      atomicJson(join(directory,`before-crash-${state.runId}.json`),evidence,true);
      state.crashEvidence=evidence;state.phase='awaiting-restart';note('crash_fired',{kind:state.crash.at});save();atomicJson(markerPath,evidence,true);
      exitProcess(9);
    })();
    return freezing;
  };
  const beforeModel=async(payload,next)=>{
    const lease=owned(payload.agent?.session?.id);if(!lease||lease.role!=='executor'||!active())return next();
    while(!disposed&&active()&&['starting','proposed'].includes(state.phase))await delay(10);
    if(state.crash?.at==='delivery-unknown'&&!state.crashEvidence){heldAgent=payload.agent;state.holdReady=true;save();
      while(!disposed&&active()&&!state.crashEvidence&&['active','rework'].includes(api()?.getTask(state.taskId)?.status))await delay(25);
      return{kind:'reject'};
    }
    while(!disposed&&active()&&['crash-freezing','awaiting-restart','recovering'].includes(state.phase))await delay(25);
    if(disposed||!active())return{kind:'reject'};
    const proposed=await next();
    if(disposed||!active()||state.scenario!=='doc'||proposed?.kind!=='enter'||docPrimed.has(lease.sessionId))return proposed;
    const exists=existsSync(state.paths[0]),check=state.documentChecks?.[lease.sessionId],verified=exists&&check?.passed&&check.hash===sha(readFileSync(state.paths[0]));
    const facts=notice(documentInstructions(state.paths[0],{exists,verified}),`stage4-doc-facts:${state.runId}:${lease.sessionId}`);
    docPrimed.add(lease.sessionId);
    return {...proposed,messages:[...(proposed.messages??payload.messages??[]),facts]};
  };
  function install(){
    if(installed)return;const tools=ctx.get('tools');if(typeof tools?.register!=='function'||typeof tools?.guard!=='function'||typeof ctx.on!=='function')throw Error('缺少验收工具、guard或宿主钩子');
    const guard=exec=>{
      const lease=owned(exec.agent?.session?.id);if(!lease||!active())return;
      const allowed=lease.role==='acceptor'?['read','working_directory']:['read','write','edit','working_directory',STAGE4_TOOL];
      if(!allowed.includes(exec.name))return'阶段四只允许本轮文件工具和固定测试，不调用终端或委派';
      if(['read','write','edit'].includes(exec.name)){
        const p=exec.arguments?.file_path;if(typeof p!=='string'||!state.paths.some(x=>resolve(x).toLowerCase()===resolve(p).toLowerCase()))return'只允许本轮产物路径';
        if(['write','edit'].includes(exec.name)&&(lease.role==='acceptor'||state.receipt&&resolve(p).toLowerCase()===resolve(state.receipt).toLowerCase()))return'验收只读，固定测试回执不可由模型修改';
      }
    };
    stops.push(tools.guard(guard),ctx.on('agent/pre-step',beforeModel));
    // 在真实工具管线附回执，不伪造Worker报告、不绕过独立验收，也不额外调用模型。
    stops.push(ctx.on('tools/post-execute',async(exec,result,next)=>{
      const downstream=await next(),lease=owned(exec.agent?.session?.id);
      if(disposed||!active()||state.scenario!=='doc'||lease?.role!=='executor'||result.isError||downstream.kind==='block'
        ||!['write','edit','read'].includes(exec.name)||typeof exec.arguments?.file_path!=='string'
        ||resolve(exec.arguments.file_path).toLowerCase()!==resolve(state.paths[0]).toLowerCase())return downstream;
      const runId=state.runId,taskId=state.taskId,validation=validateDocument(state.paths[0]),hash=validation.hashes?.[state.paths[0]];
      if(validation.passed)await api().registerArtifact(taskId,state.paths[0],hash,lease.relPath);
      if(disposed||!active()||state.runId!==runId)return downstream;
      (state.documentChecks??={})[lease.sessionId]={passed:validation.passed,hash,callId:exec.callId,path:state.paths[0]};save();
      const text=validation.passed?
        `【文稿回执】磁盘结构检查通过，产物已登记，SHA-256=${hash}。结构检查不代替语义验收；按已写/已读正文逐项确认标准，满足后下一步直接输出${DOC_REPORT}。不再read、探测存在性或重复write；有真实缺项则如实报告done=false。`:
        `【文稿回执】结构检查未通过：${validation.error}。根据具体缺项修正，不能声明完成。`;
      return {...downstream,additionalContexts:[...(downstream.additionalContexts??[]),notice(text,`stage4-doc-check:${runId}:${lease.sessionId}:${exec.callId}`)]};
    }));
    stops.push(ctx.on('llm/stream',function(options,next){
      const lease=owned(options.sessionId);
      if(!lease||lease.role!=='executor'||state?.crash?.at!=='model-inflight'||state.crashEvidence||state.phase!=='running')return next();
      return(async function*(){const stream=next()[Symbol.asyncIterator]();try{
        try{
          const first=await stream.next();if(first.done||first.value?.type==='finish')throw Error('没有收到有效模型流首块，不注入故障');
          await freezeFault({sessionId:lease.sessionId,step:state.lastStep,firstChunkType:first.value?.type??'unknown'});
        }catch(error){await fail(error);throw error;}
        throw Error('故障退出函数已返回，停止本轮流');
      }finally{await stream.return?.();}})();
    }));
    stops.push(ctx.on('session/event',(session,event)=>{
      const lease=owned(session.id);if(!lease||!active())return;
      if(event.type==='step/start'&&lease.role==='executor'){state.lastStep={sessionId:session.id,turn:event.data.turn,step:event.data.step,seq:event.seq};save();}
      if(event.type==='tool/call'&&lease.role==='executor'&&['write','edit'].includes(event.data.name)){
        const a=typeof event.data.arguments==='string'?JSON.parse(event.data.arguments):event.data.arguments;
        (state.calls??={})[event.data.callId]={path:a?.file_path,sessionId:session.id};save();
      }
      if(event.type==='tool/result'&&lease.role==='executor'&&!event.data.message?.isError){
        const call=state.calls?.[event.data.message?.toolCallId];
        if(call&&resolve(call.path??'')===resolve(state.paths[0])&&state.crash?.at==='after-write'&&state.phase==='running'&&!state.crashEvidence){
          void freezeFault({sessionId:session.id,callId:event.data.message.toolCallId,step:state.lastStep}).catch(fail);
        }
      }
    }));
    stops.push(tools.register({name:STAGE4_TOOL,description:'实际执行本轮parseRange模块、自写测试和独立固定输入，生成不可由模型写入的哈希回执。',
      parameters:{type:'object',required:['runId'],additionalProperties:false,properties:{runId:{type:'string'}}},output:{schema:{type:'string'},render:(_a,v)=>[{type:'text',text:v}]},
      async execute(args,exec){const lease=owned(exec.agent?.session?.id);if(!active()||state.scenario!=='dev'||args.runId!==state.runId||lease?.role!=='executor')throw Error('不属于本轮开发执行者');
        const runId=state.runId;
        const validation=await validateDevelopment(state.paths[0],state.paths[1]);
        if(disposed||!active()||state.runId!==runId||!['active','rework'].includes(api().getTask(state.taskId)?.status))throw Error('测试期间任务已停止，回执未提交');
        if(validation.passed){const receipt={runId:state.runId,...validation};atomicJson(state.receipt,receipt);await api().registerArtifact(state.taskId,state.receipt,sha(readFileSync(state.receipt)),'harness');}
        state.validation=validation;save();return JSON.stringify(validation);
      }}));installed=stops.every(s=>typeof s==='function');if(!installed)throw Error('宿主清理协议不完整');
  }
  if(active())install();
  async function start(request){
    const runtime=api()?.getRuntimeInfo?.();if(!runtime?.ready||runtime.budgetProtectionVersion!==1||!rawAuditAvailable())throw Error('需要已就绪的预算保护服务和Node zstd能力');
    for(const name of ['getTaskEvidence','getTaskSessions','propose','confirm','pause','registerArtifact','applyUserFeedback','answerClarification','getClarificationAnswers'])if(typeof api()[name]!=='function')throw Error('缺少公开服务 '+name);
    if(api().listTasks().some(t=>['active','rework','awaiting_acceptance'].includes(t.status)))throw Error('存在其他执行中任务，不创建/退出宿主');
    if(state)atomicJson(join(directory,`archive-${state.runId}.json`),state);
    const runId=request.requestId??randomUUID(),outputs=join(artifactDir,`stage4-${runId}`),spec=stage4Spec(request.scenario,runId,outputs);mkdirSync(outputs,{recursive:true});
    if(request.scenario==='doc'&&existsSync(spec.paths[0]))throw Error('新文稿运行的目标文件已存在，拒绝覆盖；恢复必须沿原运行状态');
    state={version:STAGE4_VERSION,runId,scenario:request.scenario,budgetTokens:request.budgetTokens,crash:normalizeCrash(request.crash),phase:'starting',
      initialHost:host(),startedAt:Date.now(),deadline:Date.now()+timeoutMs,paths:spec.paths,receipt:spec.receipt,events:[]};save();install();
    const executors=await api().searchExperts('engineering'),reviewers=await api().searchExperts('research'),executor=executors[0],acceptor=reviewers.find(x=>x.relPath!==executor?.relPath);
    if(!executor||!acceptor)throw Error('缺少执行与独立验收专家');
    state.executor=executor.relPath;state.acceptor=acceptor.relPath;state.phase='proposed';save();
    const task=await api().propose({...spec,deliverables:spec.paths,requiredTools:[STAGE4_TOOL],executors:[executor],acceptor,budget:{limitTokens:state.budgetTokens,strict:true}});
    state.taskId=task.id;save();await api().confirm(task.id);state.phase='running';state.confirmedAt=Date.now();save();
    if(state.crash){timer=setTimeout(()=>{if(active()&&state.phase==='running'&&!state.crashEvidence)void fail(Error('未在期限内命中故障检查点，未执行退出，场景未覆盖'));},state.crash.timeoutMs);timer.unref?.();}
  }
  async function injectUnknown(){
    if(!heldAgent||!state.holdReady||state.unknownInjecting)return;state.unknownInjecting=true;save();
    await api().applyUserFeedback(state.taskId,{kind:'clarify',text:'请提供故障确认口令'});
    const agent=heldAgent,original=agent.followup;if(typeof original!=='function')throw Error('无法观察真实消息入队方法');
    let accepted;
    const wrapper=function(message){const key=message.source?.dedupeKey;
      if(!key?.startsWith('clarify-answer:'))return original.call(this,message);
      const answer=api().getClarificationAnswers(state.taskId).find(a=>key===`clarify-answer:${a.id}`);
      if(!answer)throw Error('无持久用户答复身份');
      original.call(this,message);accepted={dedupeKey:key,answerId:answer.id,messageId:message.id,acceptedBeforeAckLoss:true,bodyHash:sha(JSON.stringify(message.content))};
      throw Error('阶段四故障注入：真实入队已返回，确认丢失');
    };
    agent.followup=wrapper;restoreFollowup=()=>{if(agent.followup===wrapper)agent.followup=original;};
    try{await api().answerClarification(state.taskId,`STAGE4_ACK_${state.runId}`);}finally{restoreFollowup?.();restoreFollowup=undefined;}
    if(!accepted)throw Error('没有命中真实答复投递，未注入');
    await freezeFault({sessionId:agent.session.id,unknown:accepted});
  }
  async function recover(){
    const fault=state.crashEvidence,marker=maybeJson(markerPath);
    if(host().pid===fault.pid){state.hint='等待真实宿主进程重启；重载插件不算';return;}
    if(!marker||marker.version!==STAGE4_VERSION||marker.runId!==state.runId||marker.taskId!==state.taskId||!marker.fired)throw Error('重启标记缺失或不属于本轮');
    const current=view(),b=current.collections.budgets[state.taskId],old=fault.before.budget,task=api().getTask(state.taskId);
    if(!task||b.limitTokens!==old.limitTokens||b.settledTokens<old.settledTokens||b.grants.length!==old.grants.length||b.settledTokens+b.reservedTokens>b.limitTokens)throw Error('重启任务/费用回退或上限变化');
    const recovery={verified:true,pid:host().pid,budget:b,reservationsReconciled:false,artifactPreserved:false,unknownUnchanged:false};
    if(state.crash.at==='model-inflight'){
      const entries=Object.values(current.collections.ledger??{});recovery.reservationsReconciled=fault.before.reservations.length>0&&fault.before.reservations.every(r=>{
        const rows=entries.filter(e=>e.attemptId===r.id);return current.collections.reservations?.[r.id]?.state==='orphaned'&&rows.length===1&&rows[0].kind==='adjustment'&&rows[0].inputTokens+rows[0].outputTokens===r.tokens;
      });if(!recovery.reservationsReconciled)throw Error('崩溃在途预留没有逐笔保守结算');
    }
    if(state.crash.at==='after-write'){
      recovery.artifactPreserved=Object.entries(fault.before.artifactHashes).every(([p,h])=>existsSync(p)&&sha(readFileSync(p))===h);if(!recovery.artifactPreserved)throw Error('崩溃前产物丢失或改变');
    }
    if(state.crash.at==='delivery-unknown'){
      const answer=current.collections.clarificationAnswers?.[fault.unknown.answerId];
      recovery.unknownUnchanged=task.status==='paused'&&task.pauseReason==='missing_input'&&answer?.delivery==='unknown'
        &&sha(JSON.stringify(answer))===sha(JSON.stringify(fault.before.evidence.collections.clarificationAnswers?.[fault.unknown.answerId]))
        &&api().getTaskSessions(state.taskId).every(s=>fault.before.sessionIds.includes(s.sessionId));
      if(!recovery.unknownUnchanged)throw Error('unknown投递被自动恢复或丢失');
    }
    state.recovery=recovery;state.phase=state.crash.at==='delivery-unknown'?'recovery-quiet':'running';state.recoveredAt=Date.now();note('crash_recovered',{taskId:state.taskId});save();
    renameSync(markerPath,join(directory,`crash-applied-${state.runId}.json`));
  }
  async function finish(){
    clearFault();await flush();const evidence=view(),out=join(directory,`evidence-${state.runId}`);mkdirSync(out,{recursive:true});atomicJson(join(out,'task.json'),evidence);
    const unknown=state.crash?.at==='delivery-unknown',validation=unknown?{passed:true}:state.scenario==='dev'?await validateDevelopment(state.paths[0],state.paths[1]):validateDocument(state.paths[0]);
    if(state.scenario==='dev'){
      const receipt=maybeJson(state.receipt);
      validation.receiptVerified=receipt?.runId===state.runId&&receipt.passed===true&&receipt.fixedCases===9&&receipt.workerAssertions>=3
        &&state.paths.slice(0,2).every(p=>receipt.hashes?.[p]===validation.hashes?.[p]);
      validation.passed=validation.passed&&validation.receiptVerified;
    }
    const artifacts=[];
    if(!unknown)for(const path of state.paths){const bytes=readFileSync(path),hash=sha(bytes),latest=Object.values(evidence.collections.artifacts??{}).filter(a=>resolve(a.location)===resolve(path)).at(-1),last=Object.values(evidence.events).filter(e=>e.type==='task.delivered').at(-1);
      artifacts.push({path,hash,frozenMatches:latest?.contentHash===hash&&last?.payload?.artifactHashes?.[latest.id]===hash});writeFileSync(join(out,path.split(/[\\/]/).at(-1)),bytes);
    }
    const accounting=auditStage4(state,evidence,join(out,'raw'),sessionRoot);
    const coverage=assessStage4({state,evidence,artifacts,accounting,validation,recovery:state.recovery});
    state.phase=coverage.passed?'passed':'failed';state.coverage=coverage;save();
    const result={version:STAGE4_VERSION,runId:state.runId,taskId:state.taskId,coverage,accounting,validation,artifacts,recovery:state.recovery};atomicJson(join(directory,`result-${state.runId}.json`),result);
    if(coverage.passed)atomicJson(join(directory,`done-${state.runId}.json`),{runId:state.runId,coverage});
    writeFileSync(join(directory,`summary-${state.runId}.md`),`# 阶段四 ${state.scenario}${state.crash?' / '+state.crash.at:''}\n\n- 用例：${coverage.passed?'通过':'未通过'}\n- 产品状态：${api().getTask(state.taskId).status}\n- 用量：${coverage.chargedTokens}/${state.budgetTokens}\n- 原始可观测用量：${accounting.knownTokens}；保守调整：${accounting.conservativeTokens}（不能冒称精确账单）\n- 原因：${coverage.reasons.join('；')||'证据闭合'}\n- 证据：${out}\n\n不承诺任意外部动作exactly-once；进程退出不代表断电测试。\n`);
  }
  return{
    claimed:()=>active()||existsSync(requestPath)||existsSync(markerPath),inspect:()=>state,
    enqueue(request){
      if(request.stage4Version!==STAGE4_VERSION||!['doc','dev'].includes(request.scenario)||!Number.isSafeInteger(request.budgetTokens)||request.budgetTokens<=0||request.strict!==true)throw Error('无效阶段四请求或非严格预算');
      const crash=normalizeCrash(request.crash);if(crash&&request.scenario!=='doc')throw Error('故障注入只支持受控doc任务');
      if(active()||existsSync(markerPath))throw Error('已有阶段四任务或未处理崩溃标记，不创建新任务');atomicJson(requestPath,{...request,crash},true);
    },
    async tick(){return serial(async()=>{
      if(disposed)return;
      try{
        if(existsSync(requestPath)&&!active()){const request=maybeJson(requestPath);renameSync(requestPath,join(directory,`request-applied-${request.requestId??Date.now()}.json`));
          try{await start(request);}catch(error){if(active())await fail(error);else atomicJson(join(directory,`request-rejected-${request.requestId??Date.now()}.json`),{request,error:String(error.message)});}return;}
        if(!active()){if(existsSync(markerPath))throw Error('有崩溃标记但缺少运行状态；保留现场，禁止新任务');return;}
        const cancelPath=join(directory,'cancel.json');
        if(existsSync(cancelPath)){
          const cancel=maybeJson(cancelPath);renameSync(cancelPath,join(directory,`cancel-applied-${cancel.runId??Date.now()}.json`));
          if(cancel.runId===state.runId){await fail(Error('用户停止本轮阶段四验收'));return;}
        }
        const task=state.taskId&&api().getTask(state.taskId);
        if(Date.now()>state.deadline)throw Error('阶段四观察超时，保留进度停止');
        if(state.phase==='awaiting-restart'){await recover();return;}
        if(!task||task.status==='cancelled')throw Error('任务已取消或丢失');
        const b=api().getBudget(state.taskId);if(b.limitTokens!==state.budgetTokens||b.grants.length)throw Error('固定预算被外部修改');
        if(state.phase==='recovery-quiet'){
          const f=state.crashEvidence,a=api().getClarificationAnswers(state.taskId).find(x=>x.id===f.unknown.answerId);
          if(task.status!=='paused'||task.pauseReason!=='missing_input'||a?.delivery!=='unknown'||sha(JSON.stringify(a))!==sha(JSON.stringify(f.before.evidence.collections.clarificationAnswers?.[f.unknown.answerId]))||api().getTaskSessions(state.taskId).some(s=>!f.before.sessionIds.includes(s.sessionId)))throw Error('unknown等待期间发生自动重投或答复变化');
          if(Date.now()-state.recoveredAt>=settleMs)await finish();return;
        }
        if(state.phase==='crash-freezing')return;
        if(task.status==='paused')throw Error('任务暂停：'+task.pauseReason);
        if(state.crash?.at==='delivery-unknown'&&!state.crashEvidence){await injectUnknown();return;}
        if(task.status==='delivered'){
          if(state.crash&&!state.crashEvidence)throw Error('任务已完成却未命中故障，场景未覆盖');
          if(b.reservedTokens===0)await finish();
        }
      }catch(error){if(active())await fail(error);else if(!state)atomicJson(join(directory,'startup-error.json'),{error:String(error.message)});}
    });},
    async dispose(){if(disposed)return;clearFault();disposed=true;
      if(active()&&state.taskId&&state.phase!=='awaiting-restart'){
        try{const t=api()?.getTask(state.taskId);if(t&&['active','rework','awaiting_acceptance'].includes(t.status))await api().pause(state.taskId,'阶段四驱动卸载，停止本轮');}catch{}
      }
      for(const stop of stops.splice(0))stop?.();
    },
  };
}
