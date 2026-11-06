import {readFileSync,readdirSync,existsSync,mkdirSync,copyFileSync} from 'node:fs';
import {join,resolve} from 'node:path';import {homedir} from 'node:os';
import {decodeSession} from './stage3-audit.mjs';import {sha} from './stage3-protocol.mjs';
export function auditStage4(state,evidence,output,sessionRoot=join(homedir(),'.dsh/sessions')){
  mkdirSync(output,{recursive:true});const parents=readdirSync(sessionRoot,{withFileTypes:true}).filter(x=>x.isDirectory());
  const leases=Object.values(evidence.collections.taskSessions?.[state.taskId]??{}),seen=new Set(),sessions=[],writes=[];let knownTokens=0,descendants=0,unknownConsumptions=0;
  const before=state.crashEvidence,orphans=before?.before?.reservations??[],entries=Object.values(evidence.collections.ledger??{});
  let coveredKnown=0;
  for(const lease of leases){
    if(seen.has(lease.sessionId))throw Error('重复会话身份');seen.add(lease.sessionId);
    const parent=parents.find(p=>existsSync(join(sessionRoot,p.name,lease.sessionId,'session.v4.jsonl.zstd')));if(!parent)throw Error('缺少原始会话 '+lease.sessionId);
    const path=join(sessionRoot,parent.name,lease.sessionId,'session.v4.jsonl.zstd'),bytes=readFileSync(path),events=decodeSession(bytes);copyFileSync(path,join(output,`${lease.sessionId}.jsonl.zstd`));
    let tokens=0,covered=0;const calls=new Map();
    for(const event of events){const d=event.data??{};
      if(event.type==='assistant/message'&&!d.usage)throw Error('原始assistant/message缺少usage');
      if(event.type==='assistant/message'&&d.usage){const u=d.usage,parts=[u.inputTokens??u.uncachedInputTokens,u.cacheReadTokens,u.cacheWriteTokens,u.outputTokens];if(!parts.every(n=>Number.isSafeInteger(n)&&n>=0))throw Error('usage不完整');const used=parts.reduce((a,b)=>a+b,0);tokens+=used;
        if(orphans.length&&lease.sessionId===before.sessionId&&d.turn===before.step?.turn&&d.step===before.step?.step){coveredKnown+=used;covered+=used;}
      }
      if(event.type==='subagent/catalog')descendants++;
      if(before?.unknown&&event.type==='user/message'&&(d.message?.source??d.source)?.dedupeKey===before.unknown.dedupeKey)unknownConsumptions++;
      if(event.type==='tool/call'&&['write','edit'].includes(d.name)){const args=typeof d.arguments==='string'?JSON.parse(d.arguments):d.arguments;if(args?.file_path&&state.paths.some(p=>resolve(p).toLowerCase()===resolve(args.file_path).toLowerCase()))calls.set(d.callId,args.file_path);}
      if(event.type==='tool/result'&&calls.has(d.message?.toolCallId)&&!d.message.isError){writes.push({sessionId:lease.sessionId,callId:d.message.toolCallId,path:state.paths.find(p=>resolve(p).toLowerCase()===resolve(calls.get(d.message.toolCallId)).toLowerCase()),at:new Date(event.time).getTime()});calls.delete(d.message.toolCallId);}
    }
    const charged=entries.filter(e=>e.kind==='attempt'&&e.note?.includes(lease.sessionId)).reduce((n,e)=>n+e.inputTokens+e.outputTokens,0);
    sessions.push({sessionId:lease.sessionId,role:lease.role,knownTokens:tokens,attemptTokens:charged,matches:tokens-covered===charged,rawSha256:sha(bytes)});knownTokens+=tokens;
  }
  const adjustments=entries.filter(e=>e.kind==='adjustment');let conservativeTokens=0,validAdjustments=adjustments.length===orphans.length;
  for(const r of orphans){const matches=adjustments.filter(e=>e.attemptId===r.id),reservation=evidence.collections.reservations?.[r.id];
    if(matches.length!==1||matches[0].inputTokens+matches[0].outputTokens!==r.tokens||reservation?.state!=='orphaned')validAdjustments=false;
    else conservativeTokens+=r.tokens;
  }
  const attempts=entries.filter(e=>e.kind==='attempt').reduce((n,e)=>n+e.inputTokens+e.outputTokens,0),chargedTokens=attempts+conservativeTokens;
  const budget=evidence.collections.budgets[state.taskId];
  return{verified:leases.length>0&&sessions.every(s=>s.matches)&&validAdjustments&&coveredKnown<=conservativeTokens&&knownTokens-coveredKnown===attempts&&chargedTokens===budget.settledTokens&&descendants===0,
    knownTokens,coveredKnown,conservativeTokens,chargedTokens,descendants,unknownConsumptions,sessions,writes,validAdjustments};
}
