/** Offline, read-only historical export. Never opens the production store for writing. */
import {readFileSync,mkdirSync,writeFileSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
import {queueHistoricalEvidence,exportDeliveryEvidence,DELIVERY_EVIDENCE} from '../dist/src/delivery-evidence.js';
const args=process.argv.slice(2),value=k=>args[args.indexOf(k)+1];
for(const flag of ['--state','--out','--task'])if(!args.includes(flag)||!value(flag))throw Error(`需要${flag}`);
const state=resolve(value('--state')),out=resolve(value('--out')),taskId=value('--task');
const bytes=readFileSync(state),sourceSha256=createHash('sha256').update(bytes).digest('hex');
const data=JSON.parse(bytes.toString('utf8').replace(/^\uFEFF/,''));
// Work on this private in-memory snapshot. No JsonStore, subscriptions or model calls.
delete data.collections[DELIVERY_EVIDENCE];
await queueHistoricalEvidence({transact:async mutate=>{await mutate(data)}});
const records=Object.values(data.collections[DELIVERY_EVIDENCE]??{}).filter(r=>r.taskId===taskId);
if(!records.length)throw Error('没有找到已交付任务的最终签署轮次');
mkdirSync(out,{recursive:true});
for(const record of records){const paths=await exportDeliveryEvidence(record,out);console.log(JSON.stringify({taskId,version:record.version,roundId:record.roundId,paths}))}
const provenance=join(out,`source-${taskId}-${sourceSha256}.json`);
writeFileSync(provenance,JSON.stringify({source:state,sourceSha256,exportedAt:new Date().toISOString(),historical:true,modelCalls:0,sourceWritten:false},null,2)+'\n',{flag:'wx'});
if(createHash('sha256').update(readFileSync(state)).digest('hex')!==sourceSha256)throw Error('导出期间源状态变化，请重新核对快照');
