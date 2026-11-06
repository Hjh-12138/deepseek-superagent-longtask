import type {ExpertRef} from "../types.js";

interface Duty {
  id: string; label: string; phase: "executor" | "acceptor";
  aliases: string[]; specialists: RegExp; primary?: RegExp; related?: RegExp;
}
// Metadata-only, deterministic discovery. No model request or expert-body loading.
const DUTIES: Duty[] = [
  {id:"architecture",label:"系统架构与边界",phase:"executor",aliases:["架构","系统设计","architecture","system design"],primary:/software.architect/i,specialists:/software.architect|backend.architect/i},
  {id:"frontend",label:"前端实现",phase:"executor",aliases:["前端","页面","网页","用户界面","frontend","front-end","vue","vue3","react","html","css","vite"],specialists:/frontend|front.end|uswds.developer/i},
  {id:"backend",label:"后端与业务逻辑",phase:"executor",aliases:["后端","服务端","微服务","backend","back-end","fastapi","django","spring","nestjs","express"],specialists:/backend|back.end|senior.developer/i},
  {id:"api",label:"接口与集成",phase:"executor",aliases:["接口","集成","api","rest","graphql","webhook"],primary:/api.platform/i,specialists:/api.platform|integration.developer|integration.engineer/i,related:/backend|api.tester/i},
  {id:"database",label:"数据库与数据一致性",phase:"executor",aliases:["数据库","数据存储","持久化","sql","sqlite","mysql","postgresql","postgres","gaussdb","mongodb"],primary:/database.optimizer|database.reliability|gaussdb/i,specialists:/database|gaussdb|data.engineer/i,related:/backend/i},
  {id:"testing",label:"测试实现与回归",phase:"executor",aliases:["测试","回归","单测","联调","pytest","playwright","jest","vitest","testing","test automation","e2e"],primary:/test.automation/i,specialists:/test.automation|api.tester|workflow.optimizer|test.results.analyzer/i},
  {id:"security",label:"安全与权限",phase:"executor",aliases:["安全","鉴权","权限","认证","登录","隐私","security","oauth","sso","authentication","authorization","privacy"],specialists:/security|identity.access|privacy.engineer|penetration|threat/i},
  {id:"performance",label:"性能与容量",phase:"executor",aliases:["性能","压测","延迟","并发","performance","latency","benchmark","p95","p99"],primary:/performance.benchmarker/i,specialists:/performance|database.optimizer|sre/i},
  {id:"deployment",label:"部署与运行",phase:"executor",aliases:["部署","上线","容器","持续集成","devops","deploy","docker","kubernetes","ci/cd","sre"],specialists:/devops|platform.engineer|sre|release.engineer/i},
  {id:"design",label:"界面与交互设计",phase:"executor",aliases:["交互","视觉","界面设计","可用性","ui设计","ux","ui design","figma"],specialists:/ui.designer|ux.architect|ux.researcher|ui.finish/i},
  {id:"data",label:"数据处理与可视化",phase:"executor",aliases:["数据分析","数据处理","可视化","数据看板","报表","echarts","data visualization","analytics"],specialists:/data.engineer|data.visualization/i},
  {id:"mobile",label:"移动端与小程序",phase:"executor",aliases:["移动端","安卓","小程序","ios","android","flutter","mobile","微信"],specialists:/mobile.app|mobile.release|wechat.mini.program/i},
  {id:"desktop",label:"桌面应用",phase:"executor",aliases:["桌面","electron","tauri","desktop"],specialists:/desktop.app/i},
  {id:"ai",label:"AI与检索增强",phase:"executor",aliases:["人工智能","大模型","检索增强","智能体","ai","llm","rag","agent"],specialists:/ai.engineer|rag.pipeline|llm.post.training|multi.agent.systems|prompt.engineer/i},
  {id:"documentation",label:"开发与使用文档",phase:"executor",aliases:["文档","说明书","readme","documentation","technical writing"],specialists:/technical.writer|document.compiler/i},
  {id:"unity",label:"Unity开发",phase:"executor",aliases:["unity"],specialists:/unity/i},
  {id:"unreal",label:"Unreal开发",phase:"executor",aliases:["unreal","虚幻","ue5"],specialists:/unreal/i},
  {id:"godot",label:"Godot开发",phase:"executor",aliases:["godot","戈多"],specialists:/godot/i},
  {id:"roblox",label:"Roblox开发",phase:"executor",aliases:["roblox"],specialists:/roblox/i},
  {id:"blender",label:"Blender工具开发",phase:"executor",aliases:["blender"],specialists:/blender/i},
  {id:"shaders",label:"着色器与图形渲染",phase:"executor",aliases:["着色器","渲染","shader","rendering"],specialists:/shader|rendering|technical.artist/i},
  {id:"realtime",label:"实时协作与多人联机",phase:"executor",aliases:["联机","多人","实时协作","websocket","multiplayer","realtime","real-time"],specialists:/multiplayer|realtime.collaboration|networking/i},
  {id:"acceptance",label:"独立评审与验收",phase:"acceptor",aliases:["代码审查","代码评审","验收","code review","reviewer","acceptance"],primary:/code.reviewer|reality.checker/i,specialists:/code.reviewer|reality.checker|evidence.collector/i},
];
const DIVISIONS: Record<string,string[]> = {
  engineering:["工程","软件开发","编程","研发"],testing:["测试"],security:["安全"],design:["设计"],product:["产品"],
  "project-management":["项目管理","排期"],research:["研究","调研"],marketing:["营销","市场","文案","社媒"],finance:["财务","财务管理","会计"],sales:["销售"],
  support:["客服","支持"],"game-development":["游戏","游戏开发"],gis:["地理","地图","地理信息"],academic:["学术"],healthcare:["医疗"],
  "paid-media":["广告投放","付费投放"],"spatial-computing":["空间计算","增强现实","虚拟现实"],specialized:["专项","专门领域"],
};
const normalize = (s:string) => s.normalize("NFKC").toLowerCase().replace(/\s+/g," ").trim();
function contains(text:string, phrase:string): boolean {
  const p=normalize(phrase);
  if (/^[a-z0-9 /+-]+$/.test(p)) return new RegExp(`(^|[^a-z0-9])${p.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")}($|[^a-z0-9])`,"i").test(text);
  return text.includes(p);
}
const hits = (text:string) => DUTIES.filter(d=>d.aliases.some(a=>contains(text,a)));
const identity = (e:ExpertRef) => normalize(`${e.name} ${e.relPath}`);
const PLATFORM_HINTS: Array<[RegExp,string[]]> = [
  [/wordpress/i,["wordpress"]],[/drupal/i,["drupal"]],[/filament/i,["filament"]],[/uswds/i,["uswds","美国政府"]],
  [/gaussdb/i,["gaussdb","高斯"]],[/solidity/i,["solidity","智能合约","区块链"]],[/rust.refactoring/i,["rust"]],
  [/wechat.mini.program/i,["微信","小程序","wechat"]],
];
const applicable = (e:ExpertRef,task:string) => PLATFORM_HINTS.every(([pattern,aliases])=>!pattern.test(identity(e))||aliases.some(a=>contains(normalize(task),a)));
const specialistScore = (e:ExpertRef,d:Duty) => d.primary?.test(identity(e)) ? 160 : d.specialists.test(identity(e)) || d.aliases.some(a=>contains(normalize(e.name),a)) ? 90 : d.related?.test(identity(e)) ? 20 : 0;
export interface ExpertMatch {expert:ExpertRef; score:number; matchedResponsibilities:string[]}
export function searchExperts(experts:readonly ExpertRef[], query:string): ExpertMatch[] {
  const q=normalize(query),duties=hits(q),divisions=Object.entries(DIVISIONS).filter(([,aliases])=>aliases.some(a=>contains(q,a))).map(([d])=>d);
  const terms=[...new Set(q.match(/[a-z0-9][a-z0-9+#.-]*/g)??[])];
  return experts.map(expert=>{
    const id=identity(expert),intro=normalize(expert.intro),matchedResponsibilities:string[]=[];let score=0;
    if(q){if(normalize(expert.name)===q||normalize(expert.relPath)===q)score+=200;
      else if(id.includes(q))score+=100;else if(intro.includes(q))score+=30;
      if(normalize(expert.division)===q)score+=50;
      for(const t of terms){if(contains(id,t))score+=20;else if(contains(intro,t))score+=3;}
      for(const d of duties){const n=specialistScore(expert,d);if(n){score+=n;matchedResponsibilities.push(d.label);}}
      if(divisions.includes(expert.division))score+=10;
      if(!applicable(expert,q))score-=60;
    }
    return {expert,score,matchedResponsibilities};
  }).filter(m=>!q||m.score>0).sort((a,b)=>b.score-a.score||a.expert.relPath.localeCompare(b.expert.relPath));
}
function taskDuties(task:string): {domain:"software"|"general";duties:Duty[]} {
  const text=normalize(task).replace(/(?:不需要|无需|不做|不用|不涉及|不包含|排除)[^，。；;.!?\n]*/g," "),detected=hits(text);
  const researchOnly=/调研|研究|对比|比较|报告|文稿|文案|文档|说明书/.test(text)
    && !/开发|实现|编程|修复|重构|调试|测试|部署|构建|优化/.test(text)
    && !["build","implement","fix","debug","refactor","test","deploy"].some(a=>contains(text,a));
  if(researchOnly)return {domain:"general",duties:[]};
  const software=/软件|编程|程序|前端|后端|网页|网站|数据库|小程序|桌面应用|游戏/.test(text)
    || /(?:开发|实现).{0,30}(?:系统|平台|应用)/.test(text)
    || ["frontend","backend","api","database","mobile","desktop","unity","unreal","godot","roblox","blender","shaders"].some(id=>detected.some(d=>d.id===id))
    || ["python","typescript","javascript"].some(a=>contains(text,a));
  if(!software)return {domain:"general",duties:[]};
  const ids=new Set(detected.map(d=>d.id));
  if(/系统|平台|架构|full.stack/.test(text))ids.add("architecture");
  if(/(?:开发|实现).{0,30}(?:系统|平台)/.test(text)&&!ids.has("backend")&&!ids.has("unity")&&!ids.has("godot")&&!ids.has("unreal"))ids.add("backend");
  ids.add("testing");ids.add("acceptance");
  return {domain:"software",duties:DUTIES.filter(d=>ids.has(d.id))};
}
const summary = (e:ExpertRef) => ({relPath:e.relPath,name:e.name,division:e.division,intro:e.intro.slice(0,120)});
export function recommendExperts(experts:readonly ExpertRef[],task:string) {
  const {domain,duties}=taskDuties(task);
  const context = new Map(searchExperts(experts,task).map(m=>[m.expert.relPath,m.score]));
  return {domain,responsibilities:duties.map(d=>({id:d.id,label:d.label,phase:d.phase,
    reason:d.id==="testing"?"开发任务应说明测试与回归如何交付":d.id==="acceptance"?"完成标准需要独立验收":"任务中出现该方向的能力需求",
    candidates:experts.filter(expert=>specialistScore(expert,d)>=90&&applicable(expert,task))
      .sort((a,b)=>specialistScore(b,d)-specialistScore(a,d)||(context.get(b.relPath)??0)-(context.get(a.relPath)??0)||a.relPath.localeCompare(b.relPath)).slice(0,2).map(summary)})),
    guidance:domain==="software"?"职责不等于必须单独开Worker。按任务复杂度与预算合并分工；没有专职专家的方向应说明由谁兼任，验收人仍须独立。只推荐目录中实际存在的人选，不自动选人或开工。":"按任务主题检索适当领域，明确交付分工；不强加软件开发角色。"};
}
export function reviewStaffing(task:string,executors:readonly ExpertRef[],acceptor:ExpertRef) {
  const {domain,duties}=taskDuties(task);
  const responsibilities=duties.map(d=>({id:d.id,label:d.label,phase:d.phase,
    specialistPaths:(d.phase==="acceptor"?[acceptor]:executors).filter(e=>specialistScore(e,d)>=90).map(e=>e.relPath)}));
  return {domain,responsibilities,needsDiscussion:responsibilities.filter(r=>r.phase==="executor"&&!r.specialistPaths.length),
    note:"未选专职专家不等于无法完成；小任务可兼任，应在分工与完成标准中明确。推荐不会修改现有人选、预算或任务状态。"};
}
