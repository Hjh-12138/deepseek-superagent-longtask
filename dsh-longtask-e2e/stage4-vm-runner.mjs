// 仅由父驱动以受限子进程启动；模型代码不获得 Node 全局对象或任意模块加载器。
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';
const [parserPath,testPath]=process.argv.slice(2);
const context=vm.createContext(Object.create(null),{codeGeneration:{strings:false,wasm:false}});
const make=(source,id)=>new vm.SourceTextModule(source,{context,identifier:id});
const parser=make(readFileSync(parserPath,'utf8'),'parser');
const assertions=make(`
let count=0;const fail=m=>{throw Error(m||'assertion failed')};
export const ok=(v,m)=>{count++;if(!v)fail(m)};
export const strictEqual=(a,b,m)=>{count++;if(!Object.is(a,b))fail(m)};
export const equal=strictEqual;
export const deepStrictEqual=(a,b,m)=>{count++;if(JSON.stringify(a)!==JSON.stringify(b))fail(m)};
export const deepEqual=deepStrictEqual;
export const throws=(fn,expected)=>{count++;let caught;try{fn()}catch(e){caught=e}if(!caught)fail('did not throw');if(expected instanceof RegExp&&!expected.test(String(caught)))fail('wrong error')};
export const getCount=()=>count;
export default Object.assign(ok,{ok,equal,strictEqual,deepEqual,deepStrictEqual,throws});`,'assert');
const tests=make(`export let count=0;export function test(name,fn){const r=fn();if(r&&typeof r.then==='function')throw Error('use synchronous tests');count++}export default test;`,'test');
const workerTests=make(readFileSync(testPath,'utf8'),'worker-tests');
const linker=async(spec,ref)=>{
  if(spec==='node:assert/strict')return assertions;
  if(spec==='node:test')return tests;
  if(ref===workerTests&&spec==='./parse-range.mjs')return parser;
  throw Error('模块测试不允许导入 '+spec);
};
try{
  await parser.link(linker);await assertions.link(linker);await tests.link(linker);await workerTests.link(linker);
  await workerTests.evaluate({timeout:1000});
  const workerAssertions=assertions.namespace.getCount(),parseRange=parser.namespace.parseRange;
  assert.ok(workerAssertions>=3,'测试文件实际执行的断言少于3条');assert.equal(typeof parseRange,'function');
  // 固定预期值和比较器留在可信宿主域，模型代码改写VM的JSON/Array也不能改变判据。
  for(const [input,expected] of [['1-3,7,9-10',[1,2,3,7,9,10]],['7',[7]],['0,2-3',[0,2,3]]]){
    const actual=parseRange(input);assert.ok(Array.isArray(actual),'结果必须是数组');assert.equal(actual.length,expected.length,'固定输入长度错误');
    for(let i=0;i<expected.length;i++)assert.equal(actual[i],expected[i],'固定输入值错误');
  }
  for(const bad of ['', 'bad','3-1','1,,2',null,42])assert.throws(()=>parseRange(bad),'非法输入未抛错');
  process.stdout.write(JSON.stringify({passed:true,fixedCases:9,workerAssertions}));
}catch(error){process.stdout.write(JSON.stringify({passed:false,error:String(error.message).slice(0,1000)}));process.exitCode=1;}
