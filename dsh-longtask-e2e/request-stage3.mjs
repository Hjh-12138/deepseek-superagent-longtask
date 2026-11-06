import { existsSync, readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { STAGE3_VERSION, TERMINAL, atomicJson, maybeJson, splitBudget, publishControl } from './stage3-protocol.mjs?revision=3';

export const DEFAULT_DIRECTORY=resolve(import.meta.dirname,'../dsh-longtask/.longtask/e2e/stage3');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
export function windowProbe({uiPid,processName='DeepSeek Harness',hostPid=0}={}){
  if(process.platform!=='win32')throw Error('窗口证据采集需要目标Windows桌面');
  const args=['-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',join(import.meta.dirname,'stage3-window.ps1'),'-ProcessName',processName,'-HostProcessId',String(hostPid)];
  if(uiPid)args.push('-UiProcessId',String(uiPid));
  return JSON.parse(execFileSync('powershell.exe',args,{windowsHide:true,encoding:'utf8',timeout:10000,maxBuffer:200000}).trim().replace(/^\uFEFF/,''));
}
export function publishStart(directory,totalTokens,window){
  splitBudget(totalTokens);const old=maybeJson(join(directory,'state.json'));
  if(old&&!TERMINAL.has(old.phase))throw Error(`已有运行 ${old.runId}，阶段 ${old.phase}；用status检查，不重复开工`);
  if(existsSync(join(directory,'..','run-request.json'))||existsSync(join(directory,'..','resume-request.json')))throw Error('存在其他联调请求，请先处理，不能混跑');
  const request={version:STAGE3_VERSION,runId:randomUUID(),totalTokens,window,at:Date.now()};
  atomicJson(join(directory,'request.json'),request,true);return request;
}
async function waitState(directory,predicate,timeoutMs=180000,runId){
  const deadline=Date.now()+timeoutMs;
  do{
    const state=maybeJson(join(directory,'state.json'));
    if(state&&(!runId||state.runId===runId)){
      if(state.phase==='failed')throw Error(state.failure??state.coverage?.reasons?.join('；')??'联调失败，查看报告');
      if(state.phase==='cancelled')throw Error('本轮已取消');
      if(predicate(state))return state;
    }
    if(runId){const rejected=readdirSync(directory).filter(f=>f.startsWith('request-rejected-')).map(f=>maybeJson(join(directory,f))).find(x=>x?.request?.runId===runId);if(rejected)throw Error(rejected.reason);}
    await sleep(500);
  }while(Date.now()<deadline);
  throw Error('等待超时；请求不会自动重提。用status检查，并确认已加载最新插件和驱动');
}
async function control(directory,state,action,extra){
  const cmd=publishControl(directory,state,action,extra);const ackPath=join(directory,'acks',`${cmd.id}.json`);const end=Date.now()+30000;
  while(!existsSync(ackPath)){if(Date.now()>end)throw Error('控制命令已保存但宿主尚未确认；用status检查，不要反复提交');await sleep(300);}
  const ack=maybeJson(ackPath);if(!ack.ok)throw Error(ack.reason);return ack;
}
export async function main(argv=process.argv.slice(2)){
  const dirIndex=argv.indexOf('--dir');const directory=dirIndex<0?DEFAULT_DIRECTORY:resolve(argv[dirIndex+1]??'');
  const pidIndex=argv.indexOf('--ui-pid');const uiPid=pidIndex<0?undefined:Number(argv[pidIndex+1]);
  const action=argv[0];const get=()=>maybeJson(join(directory,'state.json'));
  if(!action||['help','--help','-h'].includes(action)){
    console.log('用法：node request-stage3.mjs start <总Token上限> [--ui-pid 界面PID]\n      node request-stage3.mjs status|hidden|restart|continue|cancel|windows|recover-tools [--dir 记录目录]\n顺序：start → 隐藏DSH窗口后hidden → restart准备 → 完整退出DSH进程再启动 → continue。recover-tools仅单次修复有证据的启动工具缺失，沿用原任务及预算。不会自动杀进程或追加预算。');return;
  }
  if(action==='windows'){console.log(JSON.stringify(windowProbe({uiPid}),null,2));return;}
  if(action==='status'){
    const state=get();console.log(state?JSON.stringify({runId:state.runId,version:state.version,fixtureVersion:state.fixtureVersion,phase:state.phase,hint:TERMINAL.has(state.phase)?undefined:state.hint,totalTokens:state.totalTokens,shares:state.shares,live:state.live,failure:state.failure,coverage:state.coverage,summary:join(directory,`summary-${state.runId}.md`)},null,2):'尚无阶段三运行');return;
  }
  if(action==='start'){
    if(!/^\d+$/.test(argv[1]??''))throw Error('start后必须明确填写总Token上限');
    const window=windowProbe({uiPid});const request=publishStart(directory,Number(argv[1]),window);
    console.log(`已提交阶段三一次运行，总上限 ${request.totalTokens} Token，分配 ${JSON.stringify(splitBudget(request.totalTokens))}；不自动加额。`);
    const state=await waitState(directory,s=>s.phase!=='starting',30000,request.runId);
    console.log(`当前阶段 ${state.phase}。等status显示await-hidden后，关闭DSH交互窗口使其隐藏后台，再运行hidden命令。`);return;
  }
  if(action==='hidden'){
    let state=await waitState(directory,s=>s.phase==='await-hidden'||s.phase==='await-priority');
    if(state.phase==='await-priority'){console.log('隐藏窗口证据已经完成，可运行restart');return;}
    const sample=()=>windowProbe({processName:state.window.processName,hostPid:state.initialHost.pid});
    const samples=[sample()];if(!samples[0].hidden||!samples[0].hostAlive)throw Error('请关闭DSH交互窗口使其隐藏后台，保持宿主进程存活；最小化不算隐藏');
    await control(directory,state,'hidden-release',{sample:samples[0]});
    const deadline=Date.now()+180000;
    do{
      const observation=sample();samples.push(observation);
      if(!observation.hidden||!observation.hostAlive)throw Error('采样期间窗口重新可见或宿主退出，不能宣布后台验证通过');
      state=get();if(state.phase==='failed')throw Error(state.failure);
      if(state.phase==='await-hidden-proof'){
        const write=state.writes.find(w=>w.role==='A');const proof={runId:state.runId,samples,artifactHash:write?.hash};
        await control(directory,state,'hidden-proof',{proof});
        console.log('已观测隐藏区间内的真实写入；现在可以重新打开DSH窗口，再运行restart准备命令。');return;
      }
      await sleep(500);
    }while(Date.now()<deadline);
    throw Error('未在隐藏窗口区间内观测到真实写入；不自动重试或加额');
  }
  if(action==='restart'){
    let state=get();if(!state)throw Error('请先start');
    if(state.phase!=='await-host-restart')await control(directory,state,'prepare-restart');
    state=await waitState(directory,s=>s.phase==='await-host-restart');
    console.log(`重启检查点已落盘，旧宿主PID=${state.beforeRestart.host.pid}。现在完整退出DSH进程，再启动DSH并保持插件开启；仅重载插件不算。启动后运行continue。`);return;
  }
  if(action==='recover-tools'){
    const state=get();if(!state)throw Error('没有待恢复的运行');
    await control(directory,state,'recover-tools');
    console.log('工具缺失故障已核对并恢复；保留原任务、已用量、总上限和后台写入证据。原失败记录已归档。等待恢复检查点后执行continue，不要重新start。');return;
  }
  if(action==='continue'){
    let state=get();if(!state)throw Error('请先start');
    if(state.phase==='passed'){console.log('已通过：'+join(directory,`summary-${state.runId}.md`));return;}
    if(state.phase==='await-host-restart')state=await waitState(directory,s=>s.phase==='recovered-ready',60000);
    if(state.phase==='recovered-ready')await control(directory,state,'continue');
    else if(!['feedback-pausing','feedback-wait-A','feedback-answering','finishing'].includes(state.phase))throw Error('当前阶段不接受continue：'+state.phase);
    const minutes=Math.ceil((2*state.watchdogConfig.stallThresholdMs+state.watchdogConfig.watchdogIntervalMs)/60000);
    console.log(`正在完成答复排队和真实Watchdog检查；按当前阈值最多约${minutes}分钟空等后纠偏。空等不调用模型，不追加额度，保持DSH运行；可另开终端status。`);
    state=await waitState(directory,s=>s.phase==='passed',Math.max(900000,state.deadline-Date.now()+10000));
    console.log(`阶段三通过，用量 ${state.coverage.settledTokens}/${state.totalTokens}。报告：${join(directory,`summary-${state.runId}.md`)}`);return;
  }
  if(action==='cancel'){await control(directory,get(),'cancel');console.log('已请求取消本轮三项测试任务，不操作其他任务');return;}
  throw Error('未知命令；使用--help');
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url)main().catch(error=>{console.error(String(error.message??error));process.exitCode=1;});
