import {test} from "node:test";
import assert from "node:assert/strict";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {validateExecutionPlan} from "../src/execution-plan.js";
import type {ExpertRef,ExecutorAssignment,WorkModule} from "../src/types.js";
const e=(relPath:string):ExpertRef=>({relPath,name:relPath,division:"engineering",intro:"",contentHash:"hash",snapshottedAt:"now"});

test("复验现有系统只交报告：不强制开发职责、源码交付或业务模块",()=>{
  const report=join(tmpdir(),"VERIFICATION.md");
  const p={workKind:"verification" as const,executionPlanVersion:2 as const,goal:"对已交付系统执行运行级复验，不改业务代码，只输出报告",criteria:[{id:"v1",text:"测试全部通过"}],deliverables:[report],executors:[e("qa")],acceptor:e("reviewer"),assignments:[{relPath:"qa",deliverables:[report],criterionIds:["v1"]}]};
  assert.equal(validateExecutionPlan(p).valid,true);
  assert.equal(validateExecutionPlan({...p,workKind:"development",goal:"开发系统"}).valid,false,"开发任务不能沿用复验豁免");
  assert.equal(validateExecutionPlan({...p,acceptor:e("qa")}).valid,false);
});
function plan(){
  const files=[join(tmpdir(),"server.ts"),join(tmpdir(),"server.test.ts")];
  const assignments:ExecutorAssignment[]=[{relPath:"backend",module:"后端实现",responsibilities:["architecture","backend"],coverageRationale:"先设计接口边界，再逐文件实现",criterionIds:["api"],deliverables:[files[0]!],batches:[{id:"server",title:"实现服务入口",deliverables:[files[0]!]}]},
    {relPath:"tests",module:"测试回归",responsibilities:["testing"],criterionIds:["test"],deliverables:[files[1]!],batches:[{id:"tests",title:"运行测试并写证据",deliverables:[files[1]!]}]}];
  return {goal:"开发后端系统",criteria:[{id:"api",text:"业务可用"},{id:"test",text:"验证正确"}],deliverables:files,executors:[e("backend"),e("tests")],acceptor:e("reviewer"),assignments};
}
test("职责矩阵：允许说明清楚的兼任，拒绝遗漏和虚构职责",()=>{
  const p=plan();assert.equal(validateExecutionPlan(p).valid,true);
  p.assignments[0]!.responsibilities=["backend","magic"];p.assignments[1]!.responsibilities=["backend"];
  const r=validateExecutionPlan(p);assert.equal(r.valid,false);assert.match(r.errors.join(";"),/architecture.*缺少负责人/);assert.match(r.errors.join(";"),/未知.*magic/);
});
test("模块矩阵：标准归属、源码路径与批次文件必须完整一致",()=>{
  const p=plan();p.assignments[0]!.criterionIds=["test","unknown"];p.assignments[0]!.deliverables=[join(tmpdir(),"README.md")];p.assignments[0]!.batches=[{id:"all",title:"所有文件",deliverables:p.deliverables}];
  const r=validateExecutionPlan(p);assert.equal(r.valid,false);assert.match(r.errors.join(";"),/api.*未落实/);assert.match(r.errors.join(";"),/源码/);assert.match(r.errors.join(";"),/批次文件须属于本模块/);
});
test("分批边界：禁止超过四文件、重复编号、空目标及非独立验收",()=>{
  const p=plan();p.assignments[0]!.batches=[{id:"same",title:"",deliverables:Array(5).fill(p.deliverables[0])},{id:"same",title:"重复",deliverables:[p.deliverables[0]!]}];p.acceptor=p.executors[0]!;
  const r=validateExecutionPlan(p);assert.equal(r.valid,false);assert.match(r.errors.join(";"),/1\.\.4/);assert.match(r.errors.join(";"),/编号非法或重复/);assert.match(r.errors.join(";"),/验收人必须独立/);
});

test("按业务模块重复后端职责和共享标准，PHP等技术栈均可实施",()=>{
  for(const ext of ["php","c","h","hpp","rb","dart","m","mm","R","ipynb","customlang"]){
    const p=plan(),file=join(tmpdir(),`payments.${ext}`);
    const third:ExecutorAssignment={relPath:"payments",module:"支付业务",responsibilities:["backend"],criterionIds:["api"],deliverables:[file],batches:[{id:"payment",title:"支付流程",deliverables:[file]}]};
    p.deliverables.push(file);p.executors.push(e("payments"));p.assignments.push(third);
    assert.deepEqual(validateExecutionPlan(p).errors,[],ext);
  }
});

function businessPlan(){
  const p=plan();
  const workModules:WorkModule[]=[
    {id:"customers",title:"客户管理",scope:"客户业务操作与规则",leadRelPath:"backend",criterionIds:["api"],deliverables:[p.deliverables[0]!]},
    {id:"dedupe",title:"线索去重",scope:"线索归并与去重规则",leadRelPath:"backend",criterionIds:["api"],deliverables:[p.deliverables[0]!],dependsOn:["customers"]},
    {id:"regression",title:"流程回归",scope:"验证客户和线索行为",leadRelPath:"tests",criterionIds:["test"],deliverables:[p.deliverables[1]!],dependsOn:["dedupe"]},
  ];
  return {...p,executionPlanVersion:2 as const,workModules,staffingRationale:""};
}
test("业务模块先于配员：不能只填前后端，少量专家承担多模块须有合并及验证依据",()=>{
  const p=businessPlan();
  assert.match(validateExecutionPlan(p).errors.join(";"),/staffingRationale/);
  p.staffingRationale="两名专家串行小批次：后端逐模块实现，测试专职回归，独立验收不参与实现";
  assert.equal(validateExecutionPlan(p).valid,true);
  assert.equal(validateExecutionPlan(p).warnings.length,1,"允许合理兼任，但把工作量风险展示给用户");
  p.workModules.forEach((m,i)=>m.title=i%2?"前端":"后端");
  assert.match(validateExecutionPlan(p).errors.join(";"),/不能只写前端\/后端/);
});
test("业务模块检查覆盖、参与者与依赖，不以专家人数代替质量",()=>{
  const p=businessPlan();p.staffingRationale="按模块串行实现和独立测试";
  p.workModules[0]!.leadRelPath="outside";
  p.workModules[0]!.dependsOn=["regression"];
  p.workModules.pop();
  const r=validateExecutionPlan(p);assert.equal(r.valid,false);assert.match(r.errors.join(";"),/参与者|协作者/);assert.match(r.errors.join(";"),/未覆盖到业务模块/);assert.match(r.errors.join(";"),/未知业务模块/);
  const valid=businessPlan();valid.staffingRationale="串行";valid.workModules[0]!.dependsOn=["regression"];
  assert.match(validateExecutionPlan(valid).errors.join(";"),/依赖存在循环/);
});
