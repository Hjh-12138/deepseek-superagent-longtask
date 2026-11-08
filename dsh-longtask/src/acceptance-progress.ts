export interface AcceptanceActivity {
  taskId:string;
  roundId:string;
  stepEpoch:number;
  proofs:string[];
  lastProgressStep:number;
  checkpointStep:number;
  correctedAtStep?:number;
}

export function acceptanceActivityKey(roundId:string,epoch:number):string {return `${roundId}:${epoch}`;}

/** 只对成功且未重复的读取/核对证据续窗，切目录、报错和重复读取不算进展。 */
export function noteAcceptanceProof(activity:AcceptanceActivity,proof:string,step:number):void {
  if(activity.proofs.includes(proof))return;
  activity.proofs.push(proof);
  activity.lastProgressStep=step;
  delete activity.correctedAtStep;
}

export function acceptanceActivityDecision(activity:AcceptanceActivity,usedSteps:number,windowSteps:number,idleSteps:number):"continue"|"checkpoint"|"correct"|"pause" {
  const idle=usedSteps-activity.lastProgressStep;
  if(idle>=idleSteps){
    if(activity.correctedAtStep===undefined)return "correct";
    if(usedSteps-activity.correctedAtStep>=idleSteps)return "pause";
  }
  if(usedSteps-activity.checkpointStep>=windowSteps)return "checkpoint";
  return "continue";
}
