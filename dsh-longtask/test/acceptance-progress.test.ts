import {test} from "node:test";
import assert from "node:assert/strict";
import {acceptanceActivityDecision,noteAcceptanceProof,type AcceptanceActivity} from "../src/acceptance-progress.js";
const activity=():AcceptanceActivity=>({taskId:"t",roundId:"r",stepEpoch:0,proofs:[],lastProgressStep:0,checkpointStep:0});
test("验收按证据续窗，重复证据不刷新进度",()=>{
  const a=activity();noteAcceptanceProof(a,"file:range1",2);noteAcceptanceProof(a,"file:range1",5);
  assert.equal(a.lastProgressStep,2);assert.equal(acceptanceActivityDecision(a,5,8,3),"correct");
  a.correctedAtStep=5;assert.equal(acceptanceActivityDecision(a,8,8,3),"pause");
  noteAcceptanceProof(a,"file:range2",8);assert.equal(acceptanceActivityDecision(a,8,8,3),"checkpoint");
  assert.equal(a.correctedAtStep,undefined);
});
