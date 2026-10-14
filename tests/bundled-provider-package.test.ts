import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

// The published artifact: pack the checkout (dist is built by CI before tests),
// unpack it, and load it in the real Pi 1.1.0 host offline, as Pi installs it.
const repo = path.resolve(import.meta.dirname, "..");
const manifest = JSON.parse(fs.readFileSync(path.join(repo, "package.json"), "utf8")) as {
  dependencies: Record<string, string>;
  peerDependencies: Record<string, string>;
  bundledDependencies: string[];
  pi: { extensions: string[] };
};
const PROVIDER = "pi-coralbricks-provider";
const PROVIDER_ENTRY = `./node_modules/${PROVIDER}/index.ts`;
const hostPackages = ["@earendil-works/pi-ai", "@earendil-works/pi-agent-core", "@earendil-works/pi-coding-agent", "@earendil-works/pi-tui", "typebox"];
const npm = process.platform === "win32" ? "npm.cmd" : "npm";

let temp: string;
let packed: string;
let files: string[];
beforeAll(() => {
  temp = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bundled-provider-"));
  const json = execFileSync(npm, ["pack", "--ignore-scripts", "--json", "--pack-destination", temp], {
    cwd: repo, encoding: "utf8", shell: process.platform === "win32", maxBuffer: 64 * 1024 * 1024,
  });
  const [result] = JSON.parse(json) as Array<{ filename: string; files: Array<{ path: string }> }>;
  files = result!.files.map((file) => file.path);
  execFileSync("tar", ["-xzf", path.join(temp, path.basename(result!.filename)), "-C", temp]);
  packed = path.join(temp, "package");
  // Pi installs production dependencies next to the package; host peers stay absent.
  for (const name of Object.keys(manifest.dependencies)) {
    if (manifest.bundledDependencies.includes(name)) continue;
    const target = path.join(packed, "node_modules", name);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.symlinkSync(fs.realpathSync(path.join(repo, "node_modules", name)), target, process.platform === "win32" ? "junction" : "dir");
  }
}, 120_000);
afterAll(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  if (temp) fs.rmSync(temp, { recursive: true, force: true });
});

describe("bundled Coral Bricks provider package", () => {
  it("pins the provider exactly, bundles it, and loads its extension entry", () => {
    expect(manifest.dependencies[PROVIDER]).toMatch(/^\d+\.\d+\.\d+$/);
    expect(manifest.bundledDependencies).toEqual([PROVIDER]);
    // Fleet revert #864 keeps the native child entry, avoiding duplicate fabric_exec registration.
    expect(manifest.pi.extensions).toEqual(["./dist/index.js", PROVIDER_ENTRY]);
    const provider = JSON.parse(fs.readFileSync(path.join(repo, "node_modules", PROVIDER, "package.json"), "utf8"));
    expect(provider.version).toBe(manifest.dependencies[PROVIDER]);
    expect(provider.pi.extensions.map((entry: string) => `./node_modules/${PROVIDER}/${entry.replace(/^\.\//, "")}`)).toEqual([PROVIDER_ENTRY]);
    for (const host of hostPackages) expect(provider.dependencies?.[host]).toBeUndefined();
  });

  it("ships the provider in the tarball without physical Pi host packages", () => {
    for (const file of ["package.json", "index.ts", "models.json", "patch.json"]) {
      expect(files).toContain(`node_modules/${PROVIDER}/${file}`);
    }
    const bundled = JSON.parse(fs.readFileSync(path.join(packed, "node_modules", PROVIDER, "package.json"), "utf8"));
    expect(bundled.version).toBe(manifest.dependencies[PROVIDER]);
    const outside = files.filter((file) => file.startsWith("node_modules/") && !file.startsWith(`node_modules/${PROVIDER}/`));
    expect(outside).toEqual([]);
    expect(files.filter((file) => hostPackages.some((host) => file.includes(`node_modules/${host}/`)))).toEqual([]);
    for (const entry of manifest.pi.extensions) expect(fs.existsSync(path.join(packed, entry)), entry).toBe(true);
  });

  async function loadPacked(packages: unknown[]) {
    const agentDir = path.join(temp, `agent-${Math.random().toString(36).slice(2)}`);
    fs.mkdirSync(agentDir, { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("CORALBRICKS_API_KEY", "");
    const sdk = await import("@earendil-works/pi-coding-agent");
    expect(sdk.VERSION).toBe("1.1.0");
    const settingsManager = sdk.SettingsManager.inMemory({ packages, compaction: { enabled: false }, retry: { enabled: false } } as never);
    const loader = new sdk.DefaultResourceLoader({
      cwd: agentDir, agentDir, settingsManager,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    });
    await loader.reload();
    return { sdk, agentDir, loaded: loader.getExtensions() };
  }

  it("loads the packed provider offline in a real Pi 1.1.0 host, beside Fabric", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", async (url: unknown) => { requests.push(String(url)); return new Response("", { status: 503 }); });
    const { sdk, agentDir, loaded } = await loadPacked([packed]);
    expect(loaded.errors).toEqual([]);
    const paths = loaded.extensions.map((extension) => path.relative(packed, extension.path).split(path.sep).join("/"));
    expect(paths).toEqual(["dist/index.js", `node_modules/${PROVIDER}/index.ts`]);
    const natives = loaded.runtime.pendingNativeProviderRegistrations.filter(({ provider }) => provider.id === "coralbricks");
    expect(natives).toHaveLength(1);
    const models = sdk.ModelRuntime.create({
      authPath: path.join(agentDir, "auth.json"), modelsPath: null, modelsStorePath: path.join(agentDir, "models-cache"), allowModelNetwork: false,
    });
    const runtime = await models;
    runtime.registerNativeProvider(natives[0]!.provider);
    for (const id of ["glm-5.3-fast", "deepseek-v4.1-flash-fast"]) expect(runtime.getModel("coralbricks", id), id).toBeDefined();
    expect(requests).toEqual([]);
  }, 60_000);

  it("leaves the provider out with the package filter `!node_modules/pi-coralbricks-provider/**`", async () => {
    const { loaded } = await loadPacked([{ source: packed, extensions: [`!node_modules/${PROVIDER}/**`] }]);
    expect(loaded.errors).toEqual([]);
    const paths = loaded.extensions.map((extension) => path.relative(packed, extension.path).split(path.sep).join("/"));
    expect(paths).toEqual(["dist/index.js"]);
    expect(loaded.runtime.pendingNativeProviderRegistrations.some(({ provider }) => provider.id === "coralbricks")).toBe(false);
  }, 60_000);
});

describe("bundle-pi-packages prepack step", () => {
  it("materializes a symlinked bundled package and prunes nested Pi host packages", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bundle-step-"));
    try {
      const store = path.join(root, "store", PROVIDER);
      fs.mkdirSync(path.join(store, "node_modules", "@earendil-works", "pi-ai"), { recursive: true });
      fs.mkdirSync(path.join(store, "node_modules", "left-pad"), { recursive: true });
      fs.writeFileSync(path.join(store, "package.json"), JSON.stringify({ name: PROVIDER, version: "1.0.0" }));
      fs.writeFileSync(path.join(store, "index.ts"), "export default () => {};\n");
      fs.writeFileSync(path.join(root, "package.json"), JSON.stringify({ bundledDependencies: [PROVIDER] }));
      fs.mkdirSync(path.join(root, "node_modules"));
      fs.symlinkSync(store, path.join(root, "node_modules", PROVIDER), process.platform === "win32" ? "junction" : "dir");
      const result = spawnSync(process.execPath, [path.join(repo, "scripts/bundle-pi-packages.mjs"), root], { encoding: "utf8" });
      expect(result.status, result.stderr).toBe(0);
      const target = path.join(root, "node_modules", PROVIDER);
      expect(fs.lstatSync(target).isSymbolicLink()).toBe(false);
      expect(fs.readFileSync(path.join(target, "index.ts"), "utf8")).toContain("export default");
      expect(fs.existsSync(path.join(target, "node_modules"))).toBe(false);
      expect(fs.existsSync(path.join(store, "node_modules", "@earendil-works", "pi-ai"))).toBe(true);
      // Idempotent on a real directory; a nested host copy there is pruned.
      fs.mkdirSync(path.join(target, "node_modules", "typebox"), { recursive: true });
      fs.mkdirSync(path.join(target, "node_modules", "left-pad"), { recursive: true });
      const again = spawnSync(process.execPath, [path.join(repo, "scripts/bundle-pi-packages.mjs"), root], { encoding: "utf8" });
      expect(again.status, again.stderr).toBe(0);
      expect(fs.existsSync(path.join(target, "node_modules", "typebox"))).toBe(false);
      expect(fs.existsSync(path.join(target, "node_modules", "left-pad"))).toBe(true);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
