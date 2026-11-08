/** Offline candidate check. Writes only an isolated verification directory; does not install into a live profile. */
import {readFileSync, writeFileSync, mkdirSync, mkdtempSync} from 'node:fs';
import {join, resolve, isAbsolute} from 'node:path';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
import {execFileSync} from 'node:child_process';
import assert from 'node:assert/strict';

const root=resolve(import.meta.dirname,'..'),pkg=JSON.parse(readFileSync(join(root,'package.json'),'utf8'));
const evidence=process.argv[2]?resolve(process.argv[2]):join(root,'.longtask','verification',`release-${pkg.version}`);
mkdirSync(evidence,{recursive:true});
const manifest=JSON.parse(readFileSync(join(evidence,'pack.json'),'utf8').replace(/^\uFEFF/,''))[0];
assert.equal(manifest.version,pkg.version);
const archive=join(root,'release','artifacts',manifest.filename),sandbox=mkdtempSync(join(evidence,'package-check-'));
const installed=join(sandbox,'node_modules',pkg.name);mkdirSync(installed,{recursive:true});
execFileSync('tar',['-xf',archive,'--strip-components=1','-C',installed]);
const hash=file=>createHash('sha256').update(readFileSync(file)).digest('hex');
for(const f of manifest.files){
  assert.ok(!isAbsolute(f.path)&&!f.path.split(/[\\/]/).includes('..'));
  assert.doesNotMatch(f.path,/(^|\/)(test|node_modules|\.longtask|dsh-longtask-e2e)(\/|$)|state\.json|\.credentials/);
  assert.equal(hash(join(installed,f.path)),hash(join(root,f.path)),f.path);
}
const fromPackage=createRequire(join(sandbox,'entry.cjs')),entry=fromPackage(pkg.name);
assert.equal(typeof entry.apply,'function');assert.equal(entry.name,'longtask');assert.equal(entry.defaultConfig.modelPrices[0].currency,'CNY');
assert.equal(entry.defaultConfig.workerMaxTokens,'host');
assert.equal(entry.defaultConfig.executorMaxSteps,0);
assert.equal(entry.defaultConfig.acceptorMaxSteps,0);
assert.equal(entry.defaultConfig.acceptorProgressWindowSteps,8);
assert.equal(entry.defaultConfig.acceptorMaxIdleSteps,3);
assert.equal(entry.defaultConfig.toolReadBudget,0);
assert.equal(entry.defaultConfig.acceptanceReadBudgetBytes,65536);
assert.equal(entry.defaultConfig.backgroundJobTimeoutMs,7200000);
assert.deepEqual(entry.defaultConfig.acceptorReadTools,[]);
assert.equal(fromPackage(pkg.name+'/package.json').version,pkg.version);
const dataDir=join(sandbox,'synthetic-data');mkdirSync(dataDir);
const state={collections:{tasks:{t:{id:'t',status:'paused',pauseReason:'budget',currentVersion:0,versions:[{version:1,goal:'金额诊断验证',criteria:[],deliverables:[]}]}},
  budgets:{t:{taskId:'t',limitTokens:Number.MAX_SAFE_INTEGER,settledTokens:3000,reservedTokens:0,grants:[],money:{currency:'CNY',limitNano:'5000000000',settledNano:'12000000',reservedNano:'0',basis:'usage-priced-estimate'}}}},events:{},eventOrder:[]};
const statePath=join(dataDir,'state.json');writeFileSync(statePath,JSON.stringify(state));const before=hash(statePath);
const diagnostic=execFileSync(process.execPath,[join(installed,'scripts','diagnose.mjs'),dataDir,'t'],{encoding:'utf8'});
assert.match(diagnostic,/人民币限额 ¥5/);assert.match(diagnostic,/核算 ¥0\.012/);assert.doesNotMatch(diagnostic,/9,007,199,254,740,991/);
assert.equal(hash(statePath),before);
writeFileSync(join(evidence,'package-diagnose.log'),diagnostic);
const result={verifiedAt:new Date().toISOString(),version:pkg.version,archive,sha256:hash(archive),size:manifest.size,files:manifest.files.length,
  packageNameRequire:true,allPackagedFilesMatch:true,syntheticMoneyDiagnosis:true,diagnosisReadOnly:true,liveProfileTouched:false,modelCalls:0};
writeFileSync(join(evidence,'package-check.json'),JSON.stringify(result,null,2)+'\n');
writeFileSync(archive+'.sha256',result.sha256+'  '+manifest.filename+'\n');
console.log(JSON.stringify(result,null,2));
