import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, expect, it } from "vitest";
import { DefaultPackageManager, DefaultResourceLoader, SettingsManager } from "@earendil-works/pi-coding-agent";
import { installFabricResourcePin } from "../src/worker/resource-pin.js";

const cleanups: Array<() => void> = [];
afterEach(() => { for (const cleanup of cleanups.splice(0).reverse()) cleanup(); });

it.each([false, true])("filters before native trust bootstrap and every reload, retaining other authorized resources (trust: %s)", async trusted => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resource-pin-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const profile = path.join(root, "profile"); fs.mkdirSync(profile);
  const marks = path.join(root, "executed.jsonl");
  const factory = (label: string) => `import fs from 'node:fs'; export default function() { fs.appendFileSync(${JSON.stringify(marks)}, JSON.stringify(${JSON.stringify(label)})+'\\n'); }`;
  const release = (name: string) => {
    const dir = path.join(root, name); fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: "pi-fabric", type: "module", pi: { extensions: ["index.mjs"] } }));
    fs.writeFileSync(path.join(dir, "index.mjs"), factory(name)); return dir;
  };
  const a = release("A"); const b = release("B"); const c = release("C");
  const other = path.join(root, "other"); fs.mkdirSync(other);
  fs.writeFileSync(path.join(other, "package.json"), JSON.stringify({ name: "other-package", type: "module", pi: { extensions: ["index.mjs"], skills: ["skill"], prompts: ["prompt.md"] } }));
  fs.writeFileSync(path.join(other, "index.mjs"), factory("other-package"));
  fs.mkdirSync(path.join(other, "skill")); fs.writeFileSync(path.join(other, "skill/SKILL.md"), "---\nname: other-skill\ndescription: Other authorized skill\n---\nOther skill\n");
  fs.writeFileSync(path.join(other, "prompt.md"), "---\ndescription: Other authorized prompt\n---\nOther prompt\n");
  fs.mkdirSync(path.join(profile, "extensions")); fs.writeFileSync(path.join(profile, "extensions/profile.ts"), factory("profile-extension"));
  fs.mkdirSync(path.join(root, ".pi/extensions"), { recursive: true }); fs.writeFileSync(path.join(root, ".pi/extensions/project.ts"), factory("project-extension"));
  const settingsFile = path.join(profile, "settings.json");
  const select = (release: string) => fs.writeFileSync(settingsFile, JSON.stringify({ packages: [release, other] }));
  select(b);
  const settings = SettingsManager.create(root, profile, { projectTrusted: false });
  cleanups.push(installFabricResourcePin(DefaultPackageManager, path.join(a, "index.mjs")));
  const loader = new DefaultResourceLoader({ cwd: root, agentDir: profile, settingsManager: settings, additionalExtensionPaths: [path.join(a, "index.mjs")] });
  const before = fs.readFileSync(settingsFile, "utf8");
  await loader.reload({ resolveProjectTrust: async ({ extensionsResult }) => {
    expect(extensionsResult.errors).toEqual([]);
    expect(extensionsResult.extensions.map(extension => extension.path)).not.toContain(path.join(b, "index.mjs"));
    // Selector mutates after the bootstrap pass, before final discovery.
    select(c); return trusted;
  } });
  const assertLoaded = () => {
    expect(loader.getExtensions().errors).toEqual([]);
    const executed = new Set(fs.readFileSync(marks, "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(executed).toEqual(new Set(["A", "other-package", "profile-extension", ...(trusted ? ["project-extension"] : [])]));
    expect(loader.getExtensions().extensions.some(extension => extension.resolvedPath === path.join(a, "index.mjs") || extension.path === path.join(a, "index.mjs"))).toBe(true);
    expect(loader.getSkills().skills.map(skill => skill.name)).toContain("other-skill");
    expect(loader.getPrompts().prompts.map(prompt => prompt.name)).toContain("prompt");
  };
  assertLoaded();
  select(b); const saved = fs.readFileSync(settingsFile, "utf8");
  await loader.reload(); assertLoaded();
  expect(fs.readFileSync(settingsFile, "utf8")).toBe(saved); expect(saved).toBe(before);
  expect(settings.isProjectTrusted()).toBe(trusted);
  // Explicit -e resources from another generation must be filtered too.
  const paths = await new DefaultPackageManager({ cwd: root, agentDir: profile, settingsManager: settings }).resolveExtensionSources([b, a], { temporary: true });
  expect(paths.extensions.map(resource => pathToFileURL(resource.path).href)).toEqual([pathToFileURL(path.join(a, "index.mjs")).href]);
});
