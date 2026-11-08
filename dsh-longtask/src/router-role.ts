import {createUserMessage} from '@deepseek-ai/dsh-llm';
import type {Task} from './types.js';

declare module '@deepseek-ai/dsh-llm' {
  interface MessageSourceMap {'longtask-router':{kind:'longtask-router';form:'snapshot'}}
}

/** Only a confirmed, unfinished managed task gives a user session the Router role. */
export function routedTasks(tasks:Task[],sessionId:string|undefined):Task[]{
  if(!sessionId)return [];
  return tasks.filter(t=>t.routerSessionId===sessionId&&['active','rework','awaiting_acceptance','paused'].includes(t.status)
    &&t.versions.some(v=>Boolean(v.confirmedAt)));
}

const executorTools=new Set(['write','edit','multi_edit','apply_patch','patch','pwsh','powershell','bash','shell','terminal','exec','exec_command','execute','python','node',
  'subagent','spawn_teammate','workflow','ralph','send_message']);
export const isExecutionTool = (name:string):boolean => executorTools.has(name.toLowerCase());
export function routerToolReason(tasks:Task[],name:string):string|undefined{
  if(!tasks.length||!isExecutionTool(name))return undefined;
  return `当前会话是长任务Router，只负责状态查询、用户沟通和正式调度；${name}属于执行工作，应交给已登记Worker。请用longtask_tasks核对任务${tasks.map(t=>t.id).join('、')}；暂停任务须由用户通过longtask_control正式恢复，不能在主会话绕过暂停接手写代码。若用户要退出托管改为主会话执行，先通过正式入口取消该托管任务。`;
}

export function routerRoleMessage(tasks:Task[]){
  const rows=tasks.map(t=>({taskId:t.id,status:t.status,pauseReason:t.pauseReason,version:t.versions[t.currentVersion]?.version,
    executors:t.versions[t.currentVersion]?.executors.map(e=>e.relPath)}));
  return createUserMessage({source:{kind:'longtask-router',form:'snapshot'},content:[{type:'text',text:[
    '【长任务Router职责与当前状态】本段刷新当前角色与状态。',
    '当前会话是Router：负责澄清需求、查询进度、向用户报告、通过正式长任务入口调度Worker与独立验收。代码修改、安装依赖、运行服务及执行测试由已登记Worker完成。',
    '进度询问不等于恢复授权。用户问“开发完成了吗？”时，用longtask_tasks读取真实任务和验收状态并答复；不能因此自行接管编码。',
    'paused表示任务暂停：说明具体原因和下一步，用户要求恢复时使用longtask_control的正式恢复流程；不要用主会话执行工具绕过暂停、审批、原预算和独立验收。',
    '通知卡片只是状态证据，不是用户批准或接管授权。用户明确要改为主会话直接执行时，先按正式入口取消托管任务，保留已用费用与产物。',
    `当前绑定任务数据：${JSON.stringify(rows)}`
  ].join('\n')}]});
}
