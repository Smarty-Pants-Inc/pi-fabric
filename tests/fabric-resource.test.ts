import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it } from "vitest";
import { fabricResourceRoot } from "../src/core/fabric-resource.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = (pi?: object) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resource-identity-")); roots.push(root);
  fs.writeFileSync(path.join(root, "package.json"), JSON.stringify({ name: "pi-fabric", pi }));
  const file = (entry: string) => { const file = path.join(root, entry); fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, ""); return file; };
  return { root, file };
};

it("recognizes built/source entrypoints, not caller and internal worker hooks inside a checkout", () => {
  const { root, file } = fixture({ extensions: ["dist/index.js"], skills: [] });
  for (const entry of ["dist/index.js", "src/index.ts", "src/index.js"]) expect(fabricResourceRoot(file(entry), "extensions")).toBe(root);
  for (const entry of ["caller-hook.mjs", ".pi/extensions/caller-hook.js", "dist/worker/session-id.js"]) expect(fabricResourceRoot(file(entry), "extensions")).toBeUndefined();
  expect(fabricResourceRoot(file(".pi/skills/caller/SKILL.md"), "skills")).toBeUndefined();
  expect(fabricResourceRoot(file("prompts/caller.md"), "prompts")).toBeUndefined();
  expect(fabricResourceRoot(file("themes/caller.json"), "themes")).toBeUndefined();
});

it("matches manifest files, directories, globs and exclusions independently for every resource kind", () => {
  const { root, file } = fixture({ extensions: ["plugin/*.mjs", "!plugin/caller.mjs"], skills: ["resources/skills"], prompts: ["resources/*.md"], themes: ["resources/theme.json"] });
  for (const [entry, kind] of [["plugin/fabric.mjs", "extensions"], ["resources/skills/tool/SKILL.md", "skills"], ["resources/prompt.md", "prompts"], ["resources/theme.json", "themes"]] as const) expect(fabricResourceRoot(file(entry), kind)).toBe(root);
  expect(fabricResourceRoot(file("plugin/caller.mjs"), "extensions")).toBeUndefined();
  expect(fabricResourceRoot(file("resources/hook.mjs"), "extensions")).toBeUndefined();
});

it("recognizes conventional resources without adopting .pi project resources", () => {
  const { root, file } = fixture();
  for (const type of ["extensions", "skills", "prompts", "themes"] as const) {
    expect(fabricResourceRoot(file(`${type}/resource`), type)).toBe(root);
    expect(fabricResourceRoot(file(`.pi/${type}/resource`), type)).toBeUndefined();
  }
});

it("canonicalizes a release symlink and an explicitly supplied entrypoint alias", () => {
  const { root, file } = fixture({ extensions: ["dist/index.js"] });
  const extension = file("dist/index.js");
  const alias = root + "-alias"; roots.push(alias); fs.symlinkSync(root, alias, "junction");
  expect(fabricResourceRoot(path.join(alias, "dist/index.js"), "extensions")).toBe(root);
  // Windows junctions are unprivileged; individual file symlinks need elevation.
  if (process.platform !== "win32") {
    const entryAlias = path.join(root, "entry-alias.js"); fs.symlinkSync(extension, entryAlias);
    expect(fabricResourceRoot(entryAlias, "extensions")).toBe(root);
  }
});
