import { existsSync, readdirSync, readFileSync, copyFileSync, mkdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { homedir } from 'node:os';
import * as zlib from 'node:zlib';
import { sha, isStallReport } from './stage3-protocol.mjs';

export const rawAuditAvailable = () => typeof zlib.zstdDecompressSync === 'function';
export function decodeSession(bytes) {
  if (!rawAuditAvailable()) throw Error('原始会话审计需要支持 zstd 的 Node 24+');
  let offset=0, content='';
  while(offset<bytes.length){
    const frame=zlib.zstdDecompressSync(bytes.subarray(offset),{info:true});
    if(!(frame.engine.bytesWritten>0))throw Error('无效zstd帧');
    offset+=frame.engine.bytesWritten;content+=frame.buffer.toString('utf8');
  }
  const events=content.trim().split('\n').filter(Boolean).map(JSON.parse);
  const seen=new Set();
  for(const e of events){if(typeof e.seq==='number'){if(seen.has(e.seq))throw Error('原始日志有重复seq');seen.add(e.seq);}}
  return events;
}

/** 只读持久日志：老进程会话也必须能核对，不靠重建后的内存投影代替。 */
export function auditStage3(state, evidence, output, sessionRoot=join(homedir(),'.dsh','sessions')) {
  mkdirSync(output,{recursive:true});
  const parents=readdirSync(sessionRoot,{withFileTypes:true}).filter(d=>d.isDirectory());
  const sessions=[];const allIds=new Set();let actualTokens=0,descendants=0,answerConsumed=0;
  const writes=[],stalls=[],watchdogs=[];let earlyGateStep=false;
  const answer=evidence.B.collections.clarificationAnswers && Object.values(evidence.B.collections.clarificationAnswers)
    .find(a=>a.answer===state.answer&&a.delivery==='delivered');
  for(const role of ['A','B','C']){
    const taskId=state.tasks[role].id;const view=evidence[role];const ids=new Set();
    for(const lease of Object.values(view.collections.taskSessions?.[taskId]??{})){
      if(ids.has(lease.sessionId))continue;ids.add(lease.sessionId);
      if(allIds.has(lease.sessionId))throw Error('不同任务复用了同一个会话，不能重复归账');allIds.add(lease.sessionId);
      const parent=parents.find(p=>existsSync(join(sessionRoot,p.name,lease.sessionId,'session.v4.jsonl.zstd')));
      if(!parent)throw Error(`缺少原始会话：${lease.sessionId}`);
      const path=join(sessionRoot,parent.name,lease.sessionId,'session.v4.jsonl.zstd');const bytes=readFileSync(path);
      const events=decodeSession(bytes);copyFileSync(path,join(output,`${lease.sessionId}.jsonl.zstd`));
      let used=0;const calls=new Map();let turn,turnText='';
      for(const e of events){
        const d=e.data??{};
        if(state.fixtureVersion===2&&state.checkpoints.some(c=>c.stage==='pre-model'&&c.sessionId===lease.sessionId)
          &&(e.type==='step/start'||e.type==='assistant/message'&&d.usage)){
          const at=new Date(e.time).getTime();if(!Number.isFinite(at)||at<state.modelGateReleasedAt)earlyGateStep=true;
        }
        if(e.type==='assistant/message'&&d.usage){const u=d.usage;const buckets=[u.inputTokens??u.uncachedInputTokens,u.cacheReadTokens,u.cacheWriteTokens,u.outputTokens];
          if(!buckets.every(x=>Number.isSafeInteger(x)&&x>=0))throw Error('原始usage字段缺失');used+=buckets.reduce((a,b)=>a+b,0);}
        if(e.type==='subagent/catalog')descendants++;
        if(role==='C'&&lease.role==='executor'){
          if(e.type==='assistant/message'){if(turn!==d.turn){turn=d.turn;turnText='';}turnText+='\n'+(d.message?.content??[]).filter(b=>b.type==='text').map(b=>b.text).join('\n');}
          if(e.type==='turn/end'&&d.reason?.kind==='completed'&&turn===d.turn&&isStallReport(turnText))stalls.push({sessionId:lease.sessionId,seq:e.seq,at:new Date(e.time).getTime()});
          if(e.type==='user/message'&&d.source?.dedupeKey?.startsWith(`watchdog:${taskId}:`))watchdogs.push({sessionId:lease.sessionId,seq:e.seq,at:new Date(e.time).getTime()});
        }
        if(role==='B'&&lease.role==='executor'&&e.type==='user/message'&&answer&&d.source?.dedupeKey===`clarify-answer:${answer.id}`){
          const text=(d.content??[]).filter(b=>b.type==='text').map(b=>b.text).join('\n');if(!text.includes(state.answer))throw Error('实际答复正文不完整');answerConsumed++;
        }
        if(e.type==='tool/call'&&['write','edit'].includes(d.name)){
          const args=typeof d.arguments==='string'?JSON.parse(d.arguments):d.arguments;
          if(args?.file_path&&resolve(args.file_path).toLowerCase()===resolve(state.tasks[role].artifact).toLowerCase())calls.set(d.callId,{name:d.name,seq:e.seq});
        }
        const call=calls.get(d.message?.toolCallId);
        if(e.type==='tool/result'&&call&&!d.message.isError){writes.push({role,sessionId:lease.sessionId,callId:d.message.toolCallId,seq:e.seq,at:new Date(e.time).getTime()});calls.delete(d.message.toolCallId);}
      }
      const ledger=Object.values(view.collections.ledger??{}).filter(e=>e.note?.includes(lease.sessionId)).reduce((n,e)=>n+e.inputTokens+e.outputTokens,0);
      sessions.push({role,sessionId:lease.sessionId,agentRole:lease.role,rawSha256:sha(bytes),actualTokens:used,ledgerTokens:ledger,matches:used===ledger});actualTokens+=used;
    }
  }
  const ledgerTotal=Object.values(evidence).reduce((n,v)=>n+Object.values(v.collections.budgets??{})[0].settledTokens,0);
  const writesMatch=['A','B','C'].every(role=>writes.filter(w=>w.role===role).length===1&&state.writes.filter(w=>w.role===role).length===1
    &&writes.some(w=>w.role===role&&w.callId===state.writes.find(x=>x.role===role).callId&&w.sessionId===state.writes.find(x=>x.role===role).sessionId));
  const watchdogVerified=watchdogs.length===1&&Number.isFinite(watchdogs[0].at)&&watchdogs[0].at>=state.cBaselineProgressAt+state.watchdogConfig.stallThresholdMs
    &&stalls.some(s=>s.sessionId===watchdogs[0].sessionId&&s.seq<watchdogs[0].seq&&s.at<=watchdogs[0].at)
    &&writes.some(w=>w.role==='C'&&w.sessionId===watchdogs[0].sessionId&&w.seq>watchdogs[0].seq&&w.at>=watchdogs[0].at);
  const gates=state.checkpoints?.filter(c=>c.stage==='pre-model')??[];
  const gateWaitsVerified=state.fixtureVersion!==2||Number.isFinite(state.modelGateReleasedAt)&&!earlyGateStep
    &&gates.every(c=>c.kind==='host-pre-step'&&allIds.has(c.sessionId)&&c.at<=state.modelGateReleasedAt)
    &&gates.some(c=>c.role==='C'&&c.host.pid===state.initialHost.pid&&state.beforeRestart.sessions.C.includes(c.sessionId))
    &&['B','C'].every(role=>gates.some(c=>c.role===role&&c.host.pid===state.restartedHost?.pid&&!state.beforeRestart.sessions[role].includes(c.sessionId)));
  return {verified:sessions.length>0&&sessions.every(s=>s.matches)&&actualTokens===ledgerTotal&&descendants===0&&answerConsumed===1&&writesMatch&&watchdogVerified&&gateWaitsVerified,
    actualTokens,ledgerTotal,descendants,answerConsumed,watchdogConsumed:watchdogs.length,watchdogVerified,gateWaitsVerified,writesMatch,writes,sessions};
}
