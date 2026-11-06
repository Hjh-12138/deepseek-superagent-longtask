import {resolve,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {existsSync} from 'node:fs';
import {publishRunRequest} from './request-run.mjs';
import {atomicJson,maybeJson} from './stage3-protocol.mjs';
import {FAULTS,STAGE4_TERMINAL} from './stage4-protocol.mjs';

export function stage4Command(args,directory=resolve(import.meta.dirname,'../dsh-longtask/.longtask/e2e')){
  const [action,...rest]=args,statePath=join(directory,'stage4/state.json'),state=maybeJson(statePath);
  if(action==='status'){
    if(rest.length)throw Error('status不接受参数');
    const pending=maybeJson(join(directory,'run-request.json'))??maybeJson(join(directory,'stage4/request.json'));
    return {phase:state?.phase??'idle',runId:state?.runId,taskId:state?.taskId,scenario:state?.scenario,
      crash:state?.crash,failure:state?.failure,coverage:state?.coverage,pending,
      waitingRestart:existsSync(join(directory,'crash-pending.json')),report:state?join(directory,`stage4/summary-${state.runId}.md`):undefined};
  }
  if(action==='cancel'){
    if(rest.length||!state||STAGE4_TERMINAL.has(state.phase))throw Error('没有可停止的阶段四运行');
    const path=join(directory,'stage4/cancel.json');atomicJson(path,{runId:state.runId,at:new Date().toISOString()},true);
    return{message:'已请求停止本轮；保持DSH运行等待消费，不清零用量或追加预算。',path};
  }
  const crash=action==='crash';
  if(!['doc','dev','crash'].includes(action)||rest.length!==(crash?2:1))throw Error('用法：doc|dev <Token上限>；crash <model-inflight|after-write|delivery-unknown> <Token上限>；status；cancel');
  const checkpoint=crash?rest[0]:undefined,amount=crash?rest[1]:rest[0];
  if(crash&&!FAULTS.includes(checkpoint)||!/^\d+$/.test(amount??''))throw Error('必须指定有效故障检查点和正整数Token上限');
  const result=publishRunRequest({scenario:crash?'doc':action,budgetTokens:Number(amount),strict:true,
    ...(crash?{crash:{at:checkpoint,timeoutMs:300000}}:{})},directory);
  return{message:`已提交一次阶段四${crash?checkpoint:action}，固定上限${result.request.budgetTokens} Token；${crash?'命中检查点后会退出DSH，重新启动后接续原任务。':'由运行中的DSH消费。'}`,path:result.path};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{console.log(JSON.stringify(stage4Command(process.argv.slice(2)),null,2));}
  catch(error){console.error(String(error.message));process.exitCode=1;}
}
