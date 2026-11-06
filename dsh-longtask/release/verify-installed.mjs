/** 本机安装结果的只读复核：不发送请求，不启动/恢复任务。 */
import {readFileSync,writeFileSync,existsSync,lstatSync,realpathSync,readdirSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const base=resolve(import.meta.dirname,'..'),artifact=join(base,'release/artifacts'),record=JSON.parse(readFileSync(join(artifact,'latest-installation.json'),'utf8'));
const hash=p=>createHash('sha256').update(readFileSync(p)).digest('hex'),profile=record.profile,pkgRoot=join(profile,'node_modules/dsh-plugin-longtask');
const manifest=JSON.parse(readFileSync(join(profile,'package.json'),'utf8')),pkg=JSON.parse(readFileSync(join(pkgRoot,'package.json'),'utf8'));
assert.equal(pkg.version,'0.1.1-rc.1');assert.equal(lstatSync(pkgRoot).isSymbolicLink(),false);assert.equal(realpathSync(pkgRoot).toLowerCase(),pkgRoot.toLowerCase());
assert.equal(manifest.dependencies['dsh-longtask-e2e'],undefined);assert.ok(!manifest.dsh.profile.bundles.includes('dsh-longtask-e2e'));assert.ok(manifest.dsh.profile.bundles.includes('dsh-plugin-longtask'));
assert.equal(hash(record.archive),record.archiveSha256);assert.equal(hash(record.historicalStatePath),record.historicalStateSha256);
const files=JSON.parse(readFileSync(join(base,'.longtask/verification/release-0.1.1-rc.1/pack.json'),'utf8').replace(/^\uFEFF/,''))[0].files;
for(const f of files)assert.equal(hash(join(pkgRoot,f.path)),hash(join(base,'.longtask/verification/release-0.1.1-rc.1/snapshot',f.path)),f.path);
const config=readFileSync(join(profile,'cordis.patch.yml'),'utf8');assert.match(config,/acceptorReadOnly:\s*true/);assert.match(config,/longtask-production/);
const dataDir=join(homedir(),'.dsh/longtask-production'),log=readFileSync(join(dataDir,'plugin.log'),'utf8');assert.match(log,/user-entry:ready=true/);assert.match(log,/stage:ready-complete/);assert.doesNotMatch(log,/INIT-ERROR/);
const probes=readdirSync(dataDir).filter(f=>f.startsWith('probe-latest-')&&f.endsWith('.json')).sort();assert.ok(probes.length);
const probePath=join(dataDir,probes.at(-1)),probe=JSON.parse(readFileSync(probePath,'utf8'));assert.deepEqual(probe.blockers,[]);
const statePath=join(dataDir,'state.json'),state=existsSync(statePath)?JSON.parse(readFileSync(statePath,'utf8')):{collections:{}};
const tasks=Object.values(state.collections.tasks??{}),budgets=Object.values(state.collections.budgets??{});
assert.equal(tasks.length,0);assert.equal(budgets.length,0);
const result={verifiedAt:new Date().toISOString(),installed:true,version:pkg.version,archiveSha256:record.archiveSha256,installedPath:pkgRoot,installedAsDeveloperLink:false,installedFilesVerified:files.length,
  profile,profileHashes:['package.json','cordis.patch.yml','pnpm-lock.yaml'].map(name=>({name,sha256:hash(join(profile,name))})),activeBundles:manifest.dsh.profile.bundles,
  dataDir,productionStateExists:existsSync(statePath),taskCount:tasks.length,budgetCount:budgets.length,entryReady:true,probePath,probeBlockers:probe.blockers,
  historicalStateUnchanged:true,oldE2ESourcePreserved:existsSync(resolve(base,'../dsh-longtask-e2e')),e2eLoaded:false,e2eLeftoverLinkRetained:existsSync(join(profile,'node_modules/dsh-longtask-e2e')),
  cleanupPolicyRejection:'Removing the leftover E2E junction was blocked by automatic approval; only generic blocked by policy was returned. No alternate deletion was attempted.',
  scope:'installed and startup checked; no paid acceptance task submitted; no task created or resumed',backup:record.backup};
writeFileSync(join(record.backup,'verification.json'),JSON.stringify(result,null,2));
console.log(JSON.stringify(result));
