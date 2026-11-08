/** CRM service verification example. Runs in an isolated copy; does not certify browser UI or clean install. */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {once} from 'node:events';
import assert from 'node:assert/strict';

const argv=process.argv.slice(2),value=k=>argv[argv.indexOf(k)+1];
const within=(root,p)=>{const r=path.relative(root,p);return !r||(!path.isAbsolute(r)&&r!=='..'&&!r.startsWith('..'+path.sep))};
const modes=new Set(['suite','boot','readme','all']);

export function readAccounts(readme){
  const lines=readme.split(/\r?\n/),header=lines.findIndex(l=>/^\|\s*用户名\s*\|\s*密码\s*\|\s*姓名\s*\|\s*角色\s*\|/.test(l));
  assert.ok(header>=0,'README缺少完整账号表');
  const rows=[];
  for(const line of lines.slice(header+2)){
    if(!line.trim().startsWith('|'))break;
    const cells=line.split('|').slice(1,-1).map(c=>c.trim().replace(/^`(.*)`$/,'$1'));
    assert.equal(cells.length,4,'账号表格式错误');assert.ok(cells.every(Boolean),'账号表字段不能为空');
    rows.push({username:cells[0],password:cells[1],name:cells[2],roleLabel:cells[3]});
  }
  assert.ok(rows.length>0,'账号表为空');assert.equal(new Set(rows.map(r=>r.username)).size,rows.length,'账号表有重复项');return rows;
}

async function probe(root,mode){
  const readme=fs.readFileSync(path.join(root,'README.md'),'utf8'),accounts=readAccounts(readme);
  for(const command of ['npm install','npm run setup','npm start'])assert.ok(readme.includes(command),`README缺少${command}`);
  process.env.CRM_DB_PATH=path.join(root,'isolated-verification.db');
  const {startServer}=await import(pathToFileURL(path.join(root,'server/index.js')).href);
  const {closeDb}=await import(pathToFileURL(path.join(root,'server/db.js')).href);
  // The app startup logs demo passwords. Suppress those logs rather than copying credentials into evidence.
  const log=console.log;console.log=()=>{};
  const server=startServer({port:0,host:'127.0.0.1'});
  try{
    if(!server.listening)await once(server,'listening');console.log=log;
    const base=`http://127.0.0.1:${server.address().port}`;
    const request=(route,options={})=>fetch(base+route,{...options,signal:AbortSignal.timeout(10000)});
    let token;
    for(const account of accounts){
      const login=await request('/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({username:account.username,password:account.password})});
      assert.equal(login.status,200,`README账号登录失败：${account.username}`);
      const body=await login.json();assert.equal(body.code,0);assert.ok(body.data.token,'缺token');
      const me=await request('/api/auth/me',{headers:{authorization:`Bearer ${body.data.token}`}});assert.equal(me.status,200);
      const user=(await me.json()).data;
      assert.ok(user.username===account.username&&user.name===account.name&&user.roleLabel===account.roleLabel,`README身份或角色不符：${account.username}`);
      token??=body.data.token;log(`README_ACCOUNT_OK ${account.username}`);
    }
    if(mode!=='readme'){
      const home=await request('/');assert.equal(home.status,200);const html=await home.text();assert.ok(/客资/.test(html),'首页标题缺失');
      const assets=[...html.matchAll(/(?:src|href)\s*=\s*["']([^"']+)["']/g)].map(m=>m[1]).filter(u=>!/^([a-z]+:|\/\/|#|data:)/i.test(u));
      assert.ok(assets.length>0,'没有本地页面资源');
      for(const asset of assets){const response=await fetch(new URL(asset,base),{signal:AbortSignal.timeout(10000)});assert.equal(response.status,200,`资源失败：${asset}`);await response.arrayBuffer();log(`ASSET_OK ${asset}`)}
      assert.equal((await request('/api/leads')).status,401,'未登录接口必须401');
      const headers={authorization:`Bearer ${token}`};
      const leads=await request('/api/leads',{headers}),stats=await request('/api/stats/overview',{headers});assert.equal(leads.status,200);assert.equal(stats.status,200);
      const list=(await leads.json()).data,overview=(await stats.json()).data;assert.equal(list.total,60);assert.equal(list.total,overview.total);
      const template=await request('/api/leads/template',{headers});assert.equal(template.status,200);assert.match(template.headers.get('content-type')||'',/spreadsheetml/);assert.ok((await template.arrayBuffer()).byteLength>0);
      log(`SERVICE_OK total=${list.total} assets=${assets.length} unauthorized=401 template=xlsx`);
    }
    log('SCOPE_EXCLUDED browser-rendering browser-interactions clean-install');
  }finally{console.log=log;server.closeAllConnections();await new Promise(r=>server.close(r));closeDb()}
}

async function main(){
  const mode=argv.includes('--mode')?value('--mode'):'all';assert.ok(modes.has(mode),'--mode suite|boot|readme|all');
  assert.ok(argv.includes('--workspace')&&value('--workspace'),'需要--workspace <项目绝对目录>');
  const root=fs.realpathSync(value('--workspace'));assert.ok(fs.statSync(root).isDirectory());
  if(argv.includes('--internal-probe')){assert.ok(within(fs.realpathSync(os.tmpdir()),root)&&/^crm-service-check-/.test(path.basename(path.dirname(root))),'内部检查只能在本脚本的临时副本运行');await probe(root,mode);return}
  const temp=fs.mkdtempSync(path.join(os.tmpdir(),'crm-service-check-')),copy=path.join(temp,'project');
  try{
    fs.cpSync(root,copy,{recursive:true,dereference:true,filter:p=>{
      const parts=path.relative(root,p).split(path.sep);
      if(parts.some(n=>['.git','.longtask','data'].includes(n)||/^\.env(?:\.|$)/.test(n)))return false;
      if(!within(root,fs.realpathSync(p)))throw Error('拒绝项目外链接');return true;
    }});
    const run=args=>{const result=spawnSync(process.execPath,args,{cwd:copy,encoding:'utf8',windowsHide:true,timeout:120000,maxBuffer:16*1024*1024});process.stdout.write(result.stdout||'');process.stderr.write(result.stderr||'');if(result.error)throw result.error;assert.equal(result.status,0,'检查进程失败')};
    if(mode==='suite'||mode==='all')run(['tests/run-tests.js']);
    if(mode!=='suite')run([fileURLToPath(import.meta.url),'--internal-probe','--workspace',copy,'--mode',mode]);
    console.log(`VERIFICATION_PASSED mode=${mode}`);
  }finally{if(fs.realpathSync(temp)===path.resolve(temp)&&within(fs.realpathSync(os.tmpdir()),path.resolve(temp)))fs.rmSync(temp,{recursive:true,force:true})}
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))main().catch(error=>{console.error(error.message);process.exitCode=1});
