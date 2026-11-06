import { test } from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { parseFrontMatter, scanCatalog, loadExpertDefinition } from "../src/catalog/scanner.js";

test("front-matter 解析：name/description 提取与引号剥离", () => {
  const text = `---\nname: Research Synthesist\ndescription: "Expert in review, synthesis"\ncolor: "#9333EA"\n---\n\n# Body\n`;
  const fm = parseFrontMatter(text);
  assert.ok(fm);
  assert.equal(fm!.fields["name"], "Research Synthesist");
  assert.equal(fm!.fields["description"], "Expert in review, synthesis");
  assert.equal(fm!.fields["color"], "#9333EA");
  assert.ok(fm!.body.startsWith("# Body"));
});

test("无 front-matter 返回 null", () => {
  assert.equal(parseFrontMatter("no front matter here"), null);
});

test("扫描临时目录：坏文件被跳过并留痕", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-cat-"));
  await fs.mkdir(join(dir, "research"));
  await fs.writeFile(join(dir, "divisions.json"), JSON.stringify({ divisions: { research: { label: "Research" } } }), "utf8");
  await fs.writeFile(join(dir, "research", "good.md"), "---\nname: A\ndescription: B\n---\nbody", "utf8");
  await fs.writeFile(join(dir, "research", "bad.md"), "没有 front matter", "utf8");
  const result = await scanCatalog(dir, join(dir, "divisions.json"));
  assert.equal(result.experts.length, 1);
  assert.equal(result.experts[0]!.name, "A");
  assert.equal(result.skipped.length, 1);
  assert.match(result.skipped[0]!.reason, /front-matter/);
});

test("同名专家由相对路径区分", async () => {
  const dir = await fs.mkdtemp(join(tmpdir(), "lt-cat-"));
  await fs.mkdir(join(dir, "research"), { recursive: true });
  await fs.mkdir(join(dir, "engineering"), { recursive: true });
  await fs.writeFile(join(dir, "divisions.json"), JSON.stringify({ divisions: { research: { label: "R" }, engineering: { label: "E" } } }), "utf8");
  const fm = "---\nname: Same Name\ndescription: dup\n---\nbody";
  await fs.writeFile(join(dir, "research", "a.md"), fm, "utf8");
  await fs.writeFile(join(dir, "engineering", "b.md"), fm, "utf8");
  const result = await scanCatalog(dir, join(dir, "divisions.json"));
  assert.equal(result.experts.length, 2);
  assert.equal(result.experts[0]!.name, result.experts[1]!.name); // 同名
  assert.notEqual(result.experts[0]!.relPath, result.experts[1]!.relPath); // 路径区分
});

// 可选真实专家库检查；普通测试使用上面的临时夹具，不依赖开发者本机目录。
const ROOT = process.env.DSH_LONGTASK_TEST_EXPERTS_ROOT ?? "";
const hasRoot = ROOT !== "" && await fs
  .stat(ROOT)
  .then(() => true)
  .catch(() => false);

test(
  "扫描真实专家库：divisions 分类、唯一相对路径、稳定哈希、懒加载核对",
  { skip: !hasRoot },
  async () => {
    const result = await scanCatalog(ROOT, join(ROOT, "divisions.json"));
    assert.ok(result.experts.length > 0, "专家库应有条目");
    const divisions = new Set(result.experts.map((e) => e.division));
    assert.ok(divisions.size > 1);

    const paths = new Set(result.experts.map((e) => e.relPath));
    assert.equal(paths.size, result.experts.length);

    for (const e of result.experts) assert.match(e.contentHash, /^[0-9a-f]{64}$/);

    const first = result.experts[0]!;
    const def = await loadExpertDefinition(ROOT, first);
    assert.ok(def.hashMatches);

    console.log(`[catalog] 扫描 ${result.experts.length} 位专家，${divisions.size} 个分类，跳过 ${result.skipped.length} 个文件`);
  },
);
