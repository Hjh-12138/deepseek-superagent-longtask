import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,existsSync,openSync,readSync,closeSync} from 'node:fs';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {runInNewContext,compileFunction} from 'node:vm';

const root=fileURLToPath(new URL('../../',import.meta.url));
const sample={type:'user/message',seq:197,time:1793989944911,surfaceOp:'append',data:{id:'notice-one',role:'user',
  source:{kind:'longtask-status',form:'notice',summary:'等待用户审批 · pwsh',taskId:'task-one',eventId:'approval-one',workers:[{sessionId:'worker-one',toolName:'pwsh',requestId:'request-one',relPath:'backend.md'}]},
  content:[{type:'text',text:'【长任务状态｜等待用户审批（1项）】\n原因：安装原生依赖需要用户决定\n请打开对应Worker审批卡片'}]}};
function loadClient(){
  let factory:any;const registered:any[]=[],renderers:any[]=[],opened:string[]=[];
  const source=readFileSync(resolve(root,'client/client.js'),'utf8');
  runInNewContext(source,{window:{__ModuleLoader__:{load(value:any){assert.equal(value.id,'dsh-plugin-longtask');factory=value.factory}}}});
  const entry=factory((id:string)=>{assert.equal(id,'react');return {createElement:(tag:any,props:any,...children:any[])=>({tag,props:props??{},children})}});
  entry.apply({uiConversation:{events:{register(d:any){registered.push(d)}}},slots:{inject(_name:string,fn:()=>void){fn()},register(spec:any,component:any){renderers.push({spec,component})}},uiWorkspace:{openSession(id:string){opened.push(id)}}});
  return {entry,definition:registered[0],renderers,opened};
}
function asarText(pkg:string,rel:string){
  const fd=openSync('D:/DSH/resources/app.asar','r');try{const head=Buffer.alloc(16);readSync(fd,head,0,16,0);const bytes=Buffer.alloc(head.readUInt32LE(12));readSync(fd,bytes,0,bytes.length,16);let entry=JSON.parse(bytes.toString()).files.dsh.files.node_modules.files['@deepseek-ai'].files[pkg];for(const part of rel.split('/'))entry=entry.files[part];const content=Buffer.alloc(Number(entry.size));readSync(fd,content,0,content.length,8+head.readUInt32LE(4)+Number(entry.offset));return content.toString('utf8');}finally{closeSync(fd)}
}

test('Router通知有可加载客户端入口，按服务依赖装配，不改为人工消息或唤醒模型',()=>{
  const manifest=JSON.parse(readFileSync(resolve(root,'package.json'),'utf8'));
  assert.equal(manifest.exports['./client'],'./client/client.js');assert.equal(manifest.dsh.client.platform,'web');
  assert.ok(manifest.files.includes('client'));assert.ok(manifest.dsh.client.inject.includes('@deepseek-ai/dsh-client-ui-chat'));
  const {entry}=loadClient();assert.deepEqual(Array.from(entry.inject),['uiConversation','slots','uiWorkspace']);
});

test('Router卡片只匹配本插件追加通知；正文按文本呈现，按钮仅打开Worker会话',()=>{
  const f=loadClient(),d=f.definition;
  assert.equal(d.target,'chat');assert.equal(d.match(sample).exclusive,true);
  for(const event of [{...sample,type:'developer/message'},{...sample,surfaceOp:{replace:1}},{...sample,data:{...sample.data,source:{kind:'user'}}}])assert.equal(d.match(event),null);
  const match={event:sample,location:{kind:'session'}};
  const context={key:'stable-key',id:'notice-one',start:match,matches:[match],state:d.start({},match)};
  const node=d.buildViewNode(context);assert.equal(node.key,'stable-key');assert.equal(node.kind,'longtask-status');assert.equal(node.visibility,'visible');assert.equal(node.groupTree,'input');
  const renderer=f.renderers.find(x=>x.spec.key===node.kind);assert.ok(renderer);
  const view=renderer.component({node});const flatten=(x:any):any[]=>!x||typeof x!=='object'?[]:[x,...(x.children??[]).flatMap((c:any)=>Array.isArray(c)?c.flatMap(flatten):flatten(c))];
  const elements=flatten(view),button=elements.find(x=>x.tag==='button');assert.ok(button);button.props.onClick();assert.deepEqual(f.opened,['worker-one']);
  assert.match(JSON.stringify(view),/安装原生依赖需要用户决定/);assert.doesNotMatch(JSON.stringify(view),/dangerouslySetInnerHTML/);
  const legacy={...context,state:d.start({},{event:{...sample,data:{...sample.data,source:{kind:'longtask-status'}}}})};
  assert.ok(renderer.component({node:d.buildViewNode(legacy)}),'已经落盘但被隐藏的旧通知也应显示');
  const oldSession='acdbe03b-b0e7-457d-bca0-d5fdba1ed343',oldRequest='f4c2fa7c-12a7-486c-8ac7-f97fb41bb8e5';
  const old={...legacy,state:{...legacy.state,text:`【等待用户审批】\n原因：工具 pwsh；Worker会话 ${oldSession}；专家 backend.md；审批 ${oldRequest}；原因：安装需要用户决定`}};
  const oldView=renderer.component({node:d.buildViewNode(old)});
  flatten(oldView).find(x=>x.tag==='button').props.onClick();assert.deepEqual(f.opened,['worker-one',oldSession]);
});

test('实际桌面Chat过滤器及ConversationNodeAssembler：旧context被隐藏，新卡片在轮次结束后可见且重放不重复',t=>{
  if(!existsSync('D:/DSH/resources/app.asar'))return t.skip('可选实际桌面客户端合同验证');
  const chat=asarText('dsh-client-ui-chat','lib/client.js'),start=chat.indexOf('function isVisibleChatNode(node)'),end=chat.indexOf('\n\t\t}',start)+4;
  assert.ok(start>0&&end>start);const visible=compileFunction(`return (${chat.slice(start,end)});`)();
  assert.equal(visible({visibility:'visible',kind:'context',data:{content:sample.data.content}}),false,'原form=notice上下文确实被宿主过滤');
  const source=asarText('dsh-client-ui-conversation','lib/client.js'),body=source.slice(source.indexOf('var module ='),source.indexOf('/** Shared lifecycle and stable-entry storage'));
  const {ConversationNodeAssembler}=compileFunction(body+'\nreturn {ConversationNodeAssembler};',['require'])(()=>({notifySubscribers(){}}));
  const {definition}=loadClient(),definitions={entries:()=>[definition],fallbackEntry:()=>undefined};
  let nodes:any[]=[];const view={target:'chat',create:()=>({replace:(input:any)=>{nodes=[...input.nodes];return {nodes}},apply:(input:any)=>{for(const n of input.upserts){const at=nodes.findIndex(x=>x.key===n.key);if(at<0)nodes.push(n);else nodes[at]=n}return {nodes}},publish(){}})};
  const assembler=new ConversationNodeAssembler(definitions,{entries:()=>[view]});
  const rows=[{type:'turn/start',seq:1,time:1,data:{turn:1}},{type:'turn/end',seq:2,time:2,data:{turn:1,reason:{kind:'completed'}}},sample].map(event=>({type:'persistent',event}));
  assembler.replaceWindow(rows,false);assembler.activateTarget('chat');assembler.flush();
  assert.equal(nodes.length,1);assert.equal(visible(nodes[0]),true);const key=nodes[0].key;
  assembler.append(rows[2]);assembler.flush();assert.equal(nodes.length,1);
  assembler.replaceWindow(rows,false);assembler.flush();assert.equal(nodes.length,1);assert.equal(nodes[0].key,key);
});
