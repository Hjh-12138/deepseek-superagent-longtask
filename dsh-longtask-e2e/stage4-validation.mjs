import {readFileSync,statSync,existsSync} from 'node:fs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {join} from 'node:path';
import {sha} from './stage3-protocol.mjs';
const execute=promisify(execFile);
export async function validateDevelopment(parserPath,testPath){
  const before={};for(const path of [parserPath,testPath]){
    if(!existsSync(path)||statSync(path).size>65536)return{passed:false,error:'两个源码文件必须存在且各不超过64KiB'};
    before[path]=sha(readFileSync(path));
  }
  const runner=join(import.meta.dirname,'stage4-vm-runner.mjs');
  let result;
  try{
    const {stdout}=await execute(process.execPath,['--permission',... [runner,parserPath,testPath].map(p=>`--allow-fs-read=${p}`),
      '--experimental-vm-modules','--disallow-code-generation-from-strings','--max-old-space-size=48',runner,parserPath,testPath],
      {timeout:5000,maxBuffer:65536,windowsHide:true,env:{...process.env,NODE_OPTIONS:''}});
    result=JSON.parse(stdout);
  }catch(error){try{result=JSON.parse(error.stdout)}catch{result={passed:false,error:'固定功能测试失败或超时：'+String(error.message).slice(0,300)}}}
  const unchanged=Object.entries(before).every(([p,h])=>existsSync(p)&&sha(readFileSync(p))===h);
  return{...result,passed:result.passed===true&&unchanged,hashes:before,unchanged};
}
export function validateDocument(path){
  if(!existsSync(path)||statSync(path).size>65536)return{passed:false,error:'文稿不存在或过大'};
  const bytes=readFileSync(path),text=bytes.toString('utf8'),sections=['问题','三种方案','取舍','结论'];
  const ordered=sections.map(s=>text.search(new RegExp(`^#{1,3}\\s*${s}\\s*$`,'m')));
  const conclusion=text.slice(ordered[3]);
  const passed=text.split(/\r?\n/).length<=80&&ordered.every((n,i)=>n>=0&&(!i||n>ordered[i-1]))
    &&['JSON','SQLite','外部数据库'].every(s=>text.includes(s))&&/JSON|SQLite|外部数据库/.test(conclusion)&&/^1[.、]/m.test(conclusion)&&/^2[.、]/m.test(conclusion);
  return{passed,hashes:{[path]:sha(bytes)},sections:ordered,error:passed?undefined:'四节、三方案或两条编号理由不完整'};
}
