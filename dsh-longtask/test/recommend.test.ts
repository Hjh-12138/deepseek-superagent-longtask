import {test} from "node:test";
import assert from "node:assert/strict";
import type {ExpertRef} from "../src/types.js";
import {searchExperts,recommendExperts,reviewStaffing} from "../src/catalog/recommend.js";
const e=(relPath:string,name:string,intro="Metadata only"):ExpertRef=>({relPath,name,intro,division:relPath.split("/")[0]!,contentHash:"h",snapshottedAt:"2026-01-01"});
const front=e("engineering/frontend.md","Frontend Developer"),back=e("engineering/backend.md","Backend Architect"),db=e("engineering/database.md","Database Optimizer"),
  dbre=e("engineering/dbre.md","Database Reliability Engineer"),api=e("engineering/api.md","API Platform Engineer"),qa=e("testing/automation.md","Test Automation Engineer"),
  apiTester=e("testing/api.md","API Tester"),reviewer=e("engineering/reviewer.md","Code Reviewer"),market=e("marketing/writer.md","Content Writer"),
  godot=e("game-development/godot/gameplay.md","Godot Gameplay Scripter"),multi=e("game-development/godot/multiplayer.md","Godot Multiplayer Engineer"),
  unity=e("game-development/unity/developer.md","Unity Developer"),shader=e("game-development/unity/shader.md","Unity Shader Engineer");
const catalog=[front,back,db,dbre,api,qa,apiTester,reviewer,market,godot,multi,unity,shader];

test("中文多关键词按专长组合排序，数据库性能与API测试不退化成泛后端",()=>{
  assert.equal(searchExperts(catalog,"数据库性能优化")[0]!.expert.relPath,db.relPath);
  assert.equal(searchExperts(catalog,"API测试")[0]!.expert.relPath,apiTester.relPath);
  assert.equal(searchExperts(catalog,"前端")[0]!.expert.relPath,front.relPath);
});
test("中文游戏专长命中嵌套路径，任务上下文也影响职责组内候选顺序",()=>{
  assert.equal(searchExperts(catalog,"Godot多人联机")[0]!.expert.relPath,multi.relPath);
  assert.equal(searchExperts(catalog,"Unity着色器")[0]!.expert.relPath,shader.relPath);
  const r=recommendExperts(catalog,"开发Godot多人联机游戏");
  assert.equal(r.responsibilities.find(r=>r.id==="godot")!.candidates[0]!.relPath,multi.relPath);
});
test("精确英文、全角版本词与空查询保持可预测结果及稳定顺序",()=>{
  assert.equal(searchExperts(catalog,"Frontend Developer")[0]!.expert.relPath,front.relPath);
  assert.equal(searchExperts(catalog,"ＶＵＥ３前端")[0]!.expert.relPath,front.relPath);
  assert.deepEqual(searchExperts(catalog,"").map(m=>m.expert.relPath),searchExperts([...catalog].reverse(),"").map(m=>m.expert.relPath));
  assert.equal(searchExperts(catalog,"不存在的专长词").length,0);
});
test("简单前端改动不自动推荐数据库后端；职责允许兼任且不修改入参",()=>{
  const original=JSON.stringify(catalog),r=recommendExperts(catalog,"修复Vue3页面按钮样式");
  assert.equal(r.domain,"software");assert.ok(r.responsibilities.some(r=>r.id==="frontend"));
  assert.ok(!r.responsibilities.some(r=>["database","backend","architecture","deployment"].includes(r.id)));
  assert.match(r.guidance,/兼任/);assert.equal(JSON.stringify(catalog),original);
});
test("非开发任务不强加软件角色；主题提及数据库也不等于要写代码",()=>{
  for(const task of ["制定客户开发营销计划","用AI撰写营销文案","调研数据库选型对比并写研究报告","整理数据库技术文档"]){
    const r=recommendExperts(catalog,task);assert.equal(r.domain,"general",task);assert.equal(r.responsibilities.length,0);
  }
  assert.ok(searchExperts(catalog,"营销文案").some(m=>m.expert.relPath===market.relPath));
});
test("推荐只引用现有定义，缺人显示空候选，不用不相关角色凑数",()=>{
  const r=recommendExperts([front,back],"开发Vue前端FastAPI后端，SQLite数据库，自动化测试");
  assert.equal(r.responsibilities.find(r=>r.id==="database")!.candidates.length,0);
  assert.equal(r.responsibilities.find(r=>r.id==="testing")!.candidates.length,0);
  assert.ok(r.responsibilities.every(r=>r.candidates.length<=2));
});
test("独立验收人不能被算作执行测试专职人员，职责提示不自动改分工",()=>{
  const selected=[front,back],r=reviewStaffing("开发Vue前端FastAPI后端，SQLite数据库，自动化测试",selected,qa);
  assert.ok(r.needsDiscussion.some(r=>r.id==="database"));assert.ok(r.needsDiscussion.some(r=>r.id==="testing"));
  assert.deepEqual(selected,[front,back]);
});

test("本地中文命名专家也参与职责推荐，不依赖固定英文文件名",()=>{
  const custom=e("engineering/custom.md","数据库性能专家","本地SQL优化定义");
  assert.equal(searchExperts([front,custom],"数据库")[0]!.expert.relPath,custom.relPath);
  const r=recommendExperts([custom],"实现SQLite数据库查询优化");
  assert.equal(r.responsibilities.find(r=>r.id==="database")!.candidates[0]!.relPath,custom.relPath);
});

test("职责组优先本方向专职专家，不被介绍中提及技术栈的旁支专家挤占",()=>{
  const architect=e("engineering/software.md","Software Architect"),multiAgent=e("engineering/multi-agent.md","Multi-Agent Systems Architect","FastAPI SQLite pytest");
  const r=recommendExperts([...catalog,architect,multiAgent],"开发Vue3前端FastAPI后端系统，SQLite，pytest测试和API接口");
  assert.equal(r.responsibilities.find(r=>r.id==="architecture")!.candidates[0]!.relPath,architect.relPath);
  assert.equal(r.responsibilities.find(r=>r.id==="api")!.candidates[0]!.relPath,api.relPath);
  assert.equal(r.responsibilities.find(r=>r.id==="testing")!.candidates[0]!.relPath,qa.relPath);
});

test("任务明确排除的后端和数据库不进入前端职责建议",()=>{
  const r=recommendExperts(catalog,"实现Vue3静态页面，不需要后端和数据库。只交付前端界面。");
  assert.equal(r.domain,"software");assert.ok(r.responsibilities.some(r=>r.id==="frontend"));
  assert.ok(!r.responsibilities.some(r=>["backend","database"].includes(r.id)));
});

test("未选WordPress等特定平台时不混入其专属开发候选，显式指定后可发现",()=>{
  const wordpress=e("engineering/wordpress-performance.md","WordPress Performance Engineer","FastAPI SQLite API"),uswds=e("engineering/uswds.md","USWDS Developer");
  const r=recommendExperts([...catalog,wordpress,uswds],"开发Vue3前端FastAPI后端系统并验证性能");
  assert.ok(r.responsibilities.every(r=>r.candidates.every(e=>![wordpress.relPath,uswds.relPath].includes(e.relPath))));
  assert.ok(recommendExperts([wordpress],"优化WordPress网站性能").responsibilities.some(r=>r.candidates.some(e=>e.relPath===wordpress.relPath)));
});
