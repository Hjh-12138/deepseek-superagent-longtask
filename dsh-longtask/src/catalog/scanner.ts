/**
 * 专家目录扫描器 · 设计文档 §4、实施文档 §2。
 *
 * 按 divisions.json 指定的分类扫描原始 Markdown；从 front-matter 读取名称、简介；
 * 用相对路径区分同名专家；记录内容哈希用于版本还原。
 * Router 先检索目录，被选中后才加载完整定义（懒加载）。
 */

import { promises as fs } from "node:fs";
import { join, relative, sep, resolve, isAbsolute } from "node:path";
import { contentHash } from "../store/store.js";
import type { ExpertRef } from "../types.js";

export interface DivisionInfo {
  label: string;
  icon?: string;
  color?: string;
}

export interface CatalogScanResult {
  experts: ExpertRef[];
  divisions: Record<string, DivisionInfo>;
  /** 扫描时跳过的文件及原因（坏 front-matter 等），启动报告用 */
  skipped: Array<{ path: string; reason: string }>;
}

/** 解析 Markdown front-matter（--- ... ---），返回键值对与正文 */
export function parseFrontMatter(text: string): { fields: Record<string, string>; body: string } | null {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/.exec(text);
  if (!match || !match[1]) return null;
  const fields: Record<string, string> = {};
  for (const line of match[1].split(/\r?\n/)) {
    const kv = /^([A-Za-z_-]+)\s*:\s*(.*)$/.exec(line);
    if (!kv || !kv[1] || !kv[2]) continue;
    let value = kv[2].trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    fields[kv[1].toLowerCase()] = value;
  }
  return { fields, body: text.slice(match[0].length).replace(/^\r?\n+/, "") };
}

/**
 * 扫描专家库根目录。divisionsPath 指向 divisions.json；rootPath 是专家库根目录。
 * 递归扫描 divisions.json 列出的分类；不跟随符号链接或目录联接。
 */
export async function scanCatalog(rootPath: string, divisionsPath: string): Promise<CatalogScanResult> {
  const raw = await fs.readFile(divisionsPath, "utf8");
  const parsed = JSON.parse(raw) as { divisions?: Record<string, DivisionInfo> };
  const divisions = parsed.divisions ?? {};
  const experts: ExpertRef[] = [];
  const skipped: CatalogScanResult["skipped"] = [];

  for (const division of Object.keys(divisions)) {
    if (!division || division !== division.trim() || division.endsWith(".") || /[\\/:]/.test(division) || isAbsolute(division)) {
      skipped.push({path: division, reason: "无效分类目录名"}); continue;
    }
    const dir = join(rootPath, division);
    const files: string[] = [];
    const walk = async (directory: string): Promise<void> => {
      let entries;
      try { entries = await fs.readdir(directory, {withFileTypes: true}); }
      catch { skipped.push({path: relative(rootPath, directory), reason: "分类目录读取失败"}); return; }
      for (const entry of entries.sort((a,b)=>a.name.localeCompare(b.name))) {
        const path = join(directory, entry.name);
        if (entry.isSymbolicLink()) {skipped.push({path: relative(rootPath,path), reason: "跳过符号链接或目录联接"}); continue;}
        if (entry.isDirectory()) await walk(path);
        else if (entry.isFile() && entry.name.endsWith(".md")) files.push(path);
      }
    };
    try {
      const stat = await fs.lstat(dir);
      if (stat.isSymbolicLink() || !stat.isDirectory()) {skipped.push({path:division,reason:"分类必须是普通目录"});continue;}
      await walk(dir);
    } catch {
      skipped.push({ path: division, reason: "分类目录不存在" });
      continue;
    }
    for (const abs of files) {
      let text: string;
      try {
        text = await fs.readFile(abs, "utf8");
      } catch (error) {
        skipped.push({ path: abs, reason: `读取失败: ${(error as Error).message}` });
        continue;
      }
      const fm = parseFrontMatter(text);
      if (!fm || !fm.fields["name"] || !fm.fields["description"]) {
        skipped.push({ path: abs, reason: "缺少 front-matter 或 name/description 字段" });
        continue;
      }
      const rel = relative(rootPath, abs).split(sep).join("/");
      experts.push({
        division,
        relPath: rel,
        name: fm.fields["name"],
        intro: fm.fields["description"],
        contentHash: await contentHash(text),
        snapshottedAt: new Date().toISOString(),
      });
    }
  }

  experts.sort((a, b) => a.relPath.localeCompare(b.relPath));
  return { experts, divisions, skipped };
}

/**
 * 懒加载专家完整定义，并核对哈希是否与目录快照一致。
 * 哈希不一致说明专家文件在选中后被修改过——调用方应记录新版本而不是静默替换。
 */
export async function loadExpertDefinition(
  rootPath: string,
  ref: Pick<ExpertRef, "relPath" | "contentHash">,
): Promise<{ text: string; hashMatches: boolean; currentHash: string }> {
  const abs = resolve(rootPath, ref.relPath), rel = relative(resolve(rootPath), abs);
  if (!rel || isAbsolute(ref.relPath) || isAbsolute(rel) || rel === ".." || rel.startsWith(`..${sep}`)) throw Error("专家路径必须在专家库内");
  let current = resolve(rootPath);
  for (const part of rel.split(sep)) {
    current = join(current, part);
    if ((await fs.lstat(current)).isSymbolicLink()) throw Error("专家定义不能通过符号链接加载");
  }
  const text = await fs.readFile(abs, "utf8");
  const currentHash = await contentHash(text);
  return { text, hashMatches: currentHash === ref.contentHash, currentHash };
}
