import {isAbsolute, normalize} from "node:path";
import {reviewStaffing} from "./catalog/recommend.js";
import {taskWorkKind} from "./work-kind.js";
import type {TaskWorkKind} from "./types.js";
import type {Criterion, ExecutorAssignment, ExpertRef, WorkModule} from "./types.js";

interface Plan {
  workKind?:TaskWorkKind;
  executionPlanVersion?: 1 | 2; workModules?: WorkModule[]; staffingRationale?: string;
  goal: string; criteria: Criterion[]; deliverables: string[];
  executors: ExpertRef[]; acceptor: ExpertRef; assignments?: ExecutorAssignment[];
}
const pathKey = (p: string) => {const s=normalize(p).replace(/\\/g,"/");return process.platform==="win32"?s.toLowerCase():s};

/** 模块依赖映射到实际文件所属批次；同一批是原子执行单元，不建立自身等待。 */
export function moduleBatchPrerequisites(plan:Pick<Plan,"assignments"|"workModules">,relPath:string,batchId:string) {
  const batch=plan.assignments?.find(a=>a.relPath===relPath)?.batches?.find(b=>b.id===batchId);
  const result=new Map<string,{relPath:string;batchId:string;deliverables:string[];moduleIds:string[]}>();
  if(!batch)return [];
  const files=new Set(batch.deliverables.map(pathKey));
  for(const module of plan.workModules??[]){
    if(!module.deliverables.some(f=>files.has(pathKey(f))))continue;
    for(const id of module.dependsOn??[]){
      const upstream=plan.workModules?.find(m=>m.id===id);if(!upstream)continue;
      const inputs=new Set(upstream.deliverables.map(pathKey));
      for(const a of plan.assignments??[])for(const b of a.batches??[]){
        const deliverables=b.deliverables.filter(f=>inputs.has(pathKey(f)));
        if(!deliverables.length||a.relPath===relPath&&b.id===batchId)continue;
        const key=JSON.stringify([a.relPath,b.id]),entry=result.get(key)??{relPath:a.relPath,batchId:b.id,deliverables:[],moduleIds:[]};
        entry.deliverables=[...new Set([...entry.deliverables,...deliverables])];
        entry.moduleIds=[...new Set([...entry.moduleIds,id])];result.set(key,entry);
      }
    }
  }
  return [...result.values()];
}

/** 开工前可核对的责任矩阵；只检查分工完整性，不把规则匹配当作能力证明。 */
export function validateExecutionPlan(plan: Plan): {valid: boolean; errors: string[]; duties: string[]; warnings: string[]} {
  if(taskWorkKind(plan.goal,plan.workKind)==="verification"){
    const errors:string[]=[],assigned=new Set<string>(),covered=new Set<string>(),files=new Set(plan.deliverables.map(pathKey));
    if(plan.executors.some(e=>e.relPath===plan.acceptor.relPath))errors.push("验收人必须独立于执行者");
    for(const a of plan.assignments??[]){
      if(!plan.executors.some(e=>e.relPath===a.relPath))errors.push("复验负责人不在执行专家中");
      for(const file of a.deliverables){const key=pathKey(file);if(!isAbsolute(file)||!files.has(key)||assigned.has(key))errors.push("复验交付文件须属于任务且唯一分工");assigned.add(key);}
      for(const id of a.criterionIds??(plan.executors.length===1?plan.criteria.map(c=>c.id):[])){if(!plan.criteria.some(c=>c.id===id))errors.push("复验分工引用未知标准");covered.add(id);}
    }
    if((plan.assignments??[]).length!==plan.executors.length||new Set((plan.assignments??[]).map(a=>a.relPath)).size!==plan.executors.length)errors.push("每位复验执行者须恰有一个分工");
    if([...files].some(f=>!assigned.has(f)))errors.push("复验报告或交付文件未分工");
    if(plan.criteria.some(c=>!covered.has(c.id)))errors.push("复验标准未落实到负责人");
    return {valid:errors.length===0,errors,duties:["testing","documentation"],warnings:[]};
  }
  const review=reviewStaffing([plan.goal,...plan.criteria.map(c=>c.text)].join("\n"),plan.executors,plan.acceptor);
  const duties=review.domain==="software"?review.responsibilities.filter(r=>r.phase==="executor").map(r=>r.id):[...new Set((plan.assignments??[]).flatMap(a=>a.responsibilities??[]))],errors:string[]=[];
  const add=(s:string)=>errors.push(s),assignments=plan.assignments??[];
  const criteria=new Set(plan.criteria.map(c=>c.id)),files=new Set(plan.deliverables.map(pathKey)),owners=new Set<string>(),covered=new Set<string>();
  if(plan.executors.some(e=>e.relPath===plan.acceptor.relPath))add("验收人必须独立于执行者");
  if(assignments.length!==plan.executors.length||new Set(assignments.map(a=>a.relPath)).size!==assignments.length)add("每位执行者必须恰有一个模块分工");
  const fileOwners=new Set<string>();
  for(const a of assignments){
    const label=a.module?.trim()||a.relPath;
    if(!plan.executors.some(e=>e.relPath===a.relPath))add(`${label}：负责人不在执行专家中`);
    if(!a.module?.trim())add(`${label}：缺少module模块名称`);
    if(!a.responsibilities?.length)add(`${label}：缺少responsibilities职责`);
    for(const duty of a.responsibilities??[]){if(!duties.includes(duty))add(`${label}：未知或非本任务职责 ${duty}`);owners.add(duty);}
    if((a.responsibilities?.length??0)>1&&!a.coverageRationale?.trim())add(`${label}：兼任多项职责须填写coverageRationale及落实办法`);
    if(!a.criterionIds?.length)add(`${label}：缺少criterionIds模块完成标准`);
    for(const id of a.criterionIds??[]){if(!criteria.has(id))add(`${label}：未知完成标准 ${id}`);covered.add(id);}
    if(!a.deliverables.length)add(`${label}：缺少模块交付文件`);
    for(const file of a.deliverables){if(!isAbsolute(file)||!files.has(pathKey(file)))add(`${label}：交付文件必须是总清单中的绝对路径 ${file}`);if(fileOwners.has(pathKey(file)))add(`交付文件重复归属 ${file}`);fileOwners.add(pathKey(file));}
    const implementation=(a.responsibilities??[]).some(d=>["frontend","backend","database","testing","mobile","desktop","unity","unreal","godot"].includes(d));
    if(implementation&&a.deliverables.every(p=>/\.(?:md|mdx|txt|rst|pdf|docx?|pptx?|xlsx?|csv|png|jpe?g|gif|svg|webp)$/i.test(p)))add(`${label}：实现职责必须列出源码/测试/配置文件，不能仅交付说明文档`);
    const batches=a.batches??[],batchFiles=new Set<string>(),ids=new Set<string>();
    if(!batches.length||batches.length>32)add(`${label}：须提供1..32个顺序执行批次batches`);
    for(const b of batches){
      if(!/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(b.id)||ids.has(b.id))add(`${label}：批次编号非法或重复`);ids.add(b.id);
      if(!b.title?.trim())add(`${label}：批次须有具体目标title`);
      if(!b.deliverables.length||b.deliverables.length>4)add(`${label}/${b.id}：每批须含1..4个文件`);
      for(const file of b.deliverables){const key=pathKey(file);if(!a.deliverables.some(p=>pathKey(p)===key)||batchFiles.has(key))add(`${label}/${b.id}：批次文件须属于本模块且只编排一次 ${file}`);batchFiles.add(key);}
    }
    if(a.deliverables.some(file=>!batchFiles.has(pathKey(file))))add(`${label}：批次未覆盖全部模块文件`);
  }
  for(const duty of duties)if(!owners.has(duty))add(`职责 ${duty} 缺少负责人，请增选专家或明确兼任`);
  for(const id of criteria)if(!covered.has(id))add(`完成标准 ${id} 未落实到模块`);
  for(const file of files)if(!fileOwners.has(file))add(`交付文件未分工 ${file}`);
  const warnings:string[]=[];
  if (plan.executionPlanVersion===2) {
    const modules=plan.workModules ?? [];
    if (!modules.length || modules.length>32) add("请先提供业务模块workModules，再确定各模块负责人和协作者");
    const ids=new Set<string>(),moduleCriteria=new Set<string>(),moduleFiles=new Set<string>();
    for (const m of modules) {
      if (!/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(m.id)||ids.has(m.id)) add("业务模块id非法或重复");
      ids.add(m.id);
      if (!m.title?.trim() || !m.scope?.trim()) add(`${m.id}：业务模块须明确title和scope工作边界`);
      const participants=new Set([m.leadRelPath,...m.contributors ?? []]);
      if ([...participants].some(p=>!plan.executors.some(e=>e.relPath===p))) add(`${m.id}：模块负责人或协作者不在执行专家中`);
      if (!m.criterionIds.length || m.criterionIds.some(id=>!criteria.has(id))) add(`${m.id}：模块须引用有效完成标准`);
      if (m.criterionIds.some(id=>!assignments.some(a=>participants.has(a.relPath)&&a.criterionIds?.includes(id)))) add(`${m.id}：完成标准须落实到本模块参与者的职责分工`);
      m.criterionIds.forEach(id=>moduleCriteria.add(id));
      if (!m.deliverables.length) add(`${m.id}：模块须有具体交付物`);
      for (const file of m.deliverables) {
        const key=pathKey(file);moduleFiles.add(key);
        const owner=assignments.find(a=>a.deliverables.some(p=>pathKey(p)===key));
        if (!files.has(key)||!owner||!participants.has(owner.relPath)) add(`${m.id}：交付物须属于模块参与者的实际分工 ${file}`);
      }
    }
    for (const id of criteria) if (!moduleCriteria.has(id)) add(`完成标准 ${id} 未覆盖到业务模块`);
    for (const file of files) if (!moduleFiles.has(file)) add(`交付物未覆盖到业务模块 ${file}`);
    const visiting=new Set<string>(),visited=new Set<string>();
    const visit=(id:string):void=>{
      if (visiting.has(id)) {add("业务模块依赖存在循环");return;}
      if (visited.has(id)) return;
      visiting.add(id);
      for (const dep of modules.find(m=>m.id===id)?.dependsOn ?? []) {
        if (!ids.has(dep)) add(`${id}：依赖未知业务模块 ${dep}`); else visit(dep);
      }
      visiting.delete(id);visited.add(id);
    };
    modules.forEach(m=>visit(m.id));
    // 模块图无环仍可能与专家的固定批次顺序或显式交接互锁，确认前拒绝无法调度的计划。
    const batchGraph=new Map<string,string[]>(),node=(rel:string,id:string)=>JSON.stringify([rel,id]);
    for(const a of assignments)for(const [i,b] of (a.batches??[]).entries()){
      const deps=moduleBatchPrerequisites(plan,a.relPath,b.id).map(d=>node(d.relPath,d.batchId));
      if(i>0)deps.push(node(a.relPath,a.batches![i-1]!.id));
      if(i===0&&a.waitFor){const source=assignments.find(s=>s.relPath===a.waitFor!.fromRelPath),last=source?.batches?.at(-1);if(last)deps.push(node(source!.relPath,last.id));}
      batchGraph.set(node(a.relPath,b.id),deps);
    }
    const pending=new Set<string>(),done=new Set<string>();
    const visitBatch=(id:string):void=>{if(pending.has(id)){add("业务模块依赖与执行批次顺序或交接等待冲突，会造成调度循环；请调整批次顺序或模块边界");return;}if(done.has(id))return;pending.add(id);for(const dep of batchGraph.get(id)??[])visitBatch(dep);pending.delete(id);done.add(id);};
    for(const id of batchGraph.keys())visitBatch(id);
    if (modules.length && modules.every(m=>/^(?:前端|后端|前端实现|后端实现|前端模块|后端模块|frontend|backend)$/i.test((m.title ?? "").trim()))) {
      add("业务模块不能只写前端/后端；请按用户可验收的业务能力拆分，技术职责仍在assignments声明");
    }
    if (modules.length>=3 && plan.executors.length<=2) {
      warnings.push("多业务模块由少量专家承担：请核对兼任负担、批次顺序及独立测试安排，必要时增选专家");
      if (!plan.staffingRationale?.trim()) add("三个及以上业务模块仅选一至两位专家，须提供staffingRationale说明合并理由、分批安排和测试职责，或增选合适专家");
    }
  }
  return {valid:errors.length===0,errors:[...new Set(errors)],duties,warnings};
}
