import type {TaskWorkKind} from "./types.js";

/** 识别工作目标，已有代码只作为核对输入时不强加开发职责。显式种类须在确认中展示。 */
export function taskWorkKind(goal:string,declared?:TaskWorkKind):TaskWorkKind {
  if(declared)return declared;
  const purpose=goal.split(/\r?\n/,1)[0]??goal;
  if(/^(?:请)?\s*(?:开发|实现|编程|修复|重构)/.test(purpose))return "development";
  if(/复验|复测|对.*(?:已交付|现有|已有).*(?:运行|核验|测试|验证)|^(?:verify|retest|validate)\b/i.test(purpose))return "verification";
  if(/^(?:请|帮我|仅|只需)?\s*(?:编写|撰写|整理|输出|生成|写).*?(?:文档|手册|报告|指南|README)/i.test(purpose))return "document";
  return "development";
}
