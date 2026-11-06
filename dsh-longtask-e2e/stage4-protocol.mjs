import {join} from 'node:path';
export const STAGE4_VERSION='stage4-acceptance-v1';
export const STAGE4_TOOL='longtask_e2e_stage4_validate';
export const STAGE4_TERMINAL=new Set(['passed','failed','cancelled']);
export const FAULTS=['model-inflight','after-write','delivery-unknown'];
export const DOC_REPORT='<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>';
/** 消费者按实际磁盘状态提供事实，不让模型付费探测存在性。 */
export function documentInstructions(path,{exists,verified=false}){
  return [
    `【本轮文稿文件事实】${path}`,
    '工具说明：pwsh、终端、glob、委派、开发测试工具不可用。只用本轮路径的write/read/edit；验收人只读。',
    !exists?'消费者已核对文件不存在，直接用write一次写入完整文稿；无需再探测存在性。':
      verified?'文件已存在，消费者已核对本会话的结构检查与哈希；按已写正文核对标准后直接报告，不再read或write。':
      '文件已存在；这是恢复上下文，read本文件一次核对实际正文。完整则直接报告，不重复write或润色；确有缺项才edit。',
    `成功write后消费者自动读取磁盘、核对结构并登记哈希，回执随工具结果返回。结构回执通过且你逐项确认标准满足后，下一步直接输出${DOC_REPORT}，不再read。独立验收仍由另一会话完成；有真实缺项则报告done=false及next。`,
  ].join('\n');
}
export function normalizeCrash(raw){
  if(raw===undefined)return undefined;
  if(!raw||typeof raw!=='object'||Array.isArray(raw)||Object.keys(raw).some(k=>!['at','timeoutMs','afterMs'].includes(k)))throw Error('crash含无效字段');
  if(raw.timeoutMs!==undefined&&raw.afterMs!==undefined)throw Error('不能同时指定timeoutMs和旧afterMs');
  const timeoutMs=raw.timeoutMs??raw.afterMs??30000,at=raw.at??'model-inflight';
  if(!FAULTS.includes(at)||!Number.isSafeInteger(timeoutMs)||timeoutMs<=0||timeoutMs>2147483647)throw Error('crash需要有效检查点，等待期限为1..2147483647ms');
  return{at,timeoutMs};
}
export function stage4Spec(scenario,runId,directory){
  if(scenario==='dev'){
    const parser=join(directory,'parse-range.mjs'),tests=join(directory,'parse-range.test.mjs'),receipt=join(directory,'validation.json');
    return{paths:[parser,tests,receipt],receipt,goal:`[E2E-DEV ${runId}] 实现 ${parser}，导出 parseRange(text)：把 "1-3,7,9-10" 展开为 [1,2,3,7,9,10]；"7" 得到 [7]；"0,2-3" 得到 [0,2,3]。输入仅允许非空、逗号分隔的非负整数或闭区间；倒序区间、空段、非数字和非字符串必须抛错。\n`+
      `再写 ${tests}，仅用 import assert from 'node:assert/strict'、可选 import test from 'node:test' 及 './parse-range.mjs'，至少执行3条同步断言；断言限用assert()、ok、strictEqual、deepStrictEqual、throws。只用write/edit写这两个文件，不用终端、不读仓库、不导入其他模块。\n`+
      `写完调用 ${STAGE4_TOOL}({runId:"${runId}"})，由独立固定测试实际执行模块和测试文件，并生成 ${receipt}（不得自己写或改回执）。失败时按工具错误修正，未通过不能报告完成。通过后直接给<longtask_report>{"done":true,"next":[],"waiting":null}</longtask_report>。`,
      criteria:[{id:'c1',text:`${parser} 与 ${tests} 存在；模块满足目标中的正常、单值及非法输入语义，测试文件实际执行至少3条断言。`},
        {id:'c2',text:`${receipt} 为测试工具生成的通过回执，passed=true且fixedCases=9，两个文件的哈希与回执一致；结合源码与回执逐项核对，不自行执行终端命令。`}]};
  }
  const doc=join(directory,'research-note.md');
  return{paths:[doc],goal:`[E2E-DOC ${runId}] 为3人单机桌面工具选择任务状态存储，交付 ${doc}，中文Markdown不超过80行。背景事实：JSON部署简单，但并发更新需要自行串行化；SQLite有本地事务、不需额外服务；外部数据库便于跨机器共享，但增加服务维护。\n`+
    '按 # 问题、# 三种方案、# 取舍、# 结论 四节顺序写，每节3-5句。三种方案必须分别包含JSON、SQLite、外部数据库。结论明确选一个方案，再用独立的1.和2.两行说明两条理由。只依据上述背景，不联网、不读仓库。\n'+
    documentInstructions(doc,{exists:false}),
    criteria:[{id:'c1',text:`${doc} 为不超过80行的中文Markdown，问题/三种方案/取舍/结论四节有序齐全，正文分别讨论JSON、SQLite、外部数据库。`},
      {id:'c2',text:'结论明确选择一个方案，有1.和2.两条编号理由，理由与给定背景及单机三人团队条件一致；不需要检索外部资料。'}]};
}
export function assessStage4({state,evidence,artifacts,accounting,validation,recovery}){
  const reasons=[],task=evidence?.collections?.tasks?.[state.taskId],budget=evidence?.collections?.budgets?.[state.taskId];
  if(!budget||budget.limitTokens!==state.budgetTokens||budget.reservedTokens!==0||budget.grants?.length||budget.settledTokens>budget.limitTokens)reasons.push('预算不守恒、追加或仍有预留');
  if(!accounting?.verified||accounting.chargedTokens!==budget?.settledTokens||accounting.descendants!==0)reasons.push('原始会话、保守调整与账本不符');
  if(state.crash){
    if(!state.crashEvidence?.fired||!recovery?.verified||recovery.pid===state.crashEvidence.pid)reasons.push('缺少真实进程退出和重启对账');
    if(state.crash.at==='delivery-unknown'){
      if(task?.status!=='paused'||task.pauseReason!=='missing_input'||!recovery?.unknownUnchanged||accounting.unknownConsumptions>1)reasons.push('未知投递未安全暂停或发生重投');
      return{passed:!reasons.length,reasons,outcome:'safely-paused',chargedTokens:budget?.settledTokens};
    }
    if(state.crash.at==='model-inflight'&&!recovery?.reservationsReconciled)reasons.push('没有逐笔在途预留对账');
    if(state.crash.at==='after-write'&&(!recovery?.artifactPreserved||accounting.writes.filter(w=>w.path===state.paths[0]).length!==1))reasons.push('已有产物被重复写入或版本改变');
  }
  if(task?.status!=='delivered')reasons.push('任务未独立验收交付');
  const leases=Object.values(evidence?.collections?.taskSessions?.[state.taskId]??{});
  if(!leases.some(s=>s.role==='acceptor'&&s.relPath===state.acceptor&&accounting?.sessions?.some(a=>a.sessionId===s.sessionId&&a.knownTokens>0)))reasons.push('缺少独立验收会话原始用量');
  if(!artifacts?.length||artifacts.length!==state.paths.length||artifacts.some(a=>!a.frozenMatches))reasons.push('产物缺失或冻结哈希不符');
  const delivered=Object.values(evidence?.events??{}).filter(e=>e.type==='task.delivered').at(-1);
  const verdicts=delivered?.payload?.verdicts;
  if(!Array.isArray(verdicts)||verdicts.length!==2||!['c1','c2'].every(id=>verdicts.filter(v=>v.criterionId===id&&v.outcome==='pass'&&v.verdictBy===state.acceptor&&v.evidence?.trim()).length===1))reasons.push('缺少完整独立c1/c2结论');
  if(validation?.passed!==true)reasons.push('文稿结构或开发实际功能测试失败');
  return{passed:!reasons.length,reasons,outcome:'delivered',chargedTokens:budget?.settledTokens};
}
