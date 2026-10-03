import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { WORKER_PROTOCOL_VERSION } from "../src/agents/worker-protocol.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const worker = `import fs from "node:fs";
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const now = Date.now();
fs.writeFileSync(args.get("status-file"), JSON.stringify({
  id: args.get("id"), name: args.get("name"), task: "probe", status: "completed", runner: "pi", transport: "process",
  cwd: args.get("cwd"), startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0,
  text: JSON.stringify({ worker: import.meta.url, extension: args.get("fabric-extension"), release: args.get("fabric-release") }),
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, exitCode: 0
}));`;

const fixture = (fullCodeMode = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-spawn-release-"));
  roots.push(root);
  const profile = path.join(root, "profile");
  fs.mkdirSync(profile);
  vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  const release = (name: string, version: number | undefined = WORKER_PROTOCOL_VERSION) => {
    const directory = path.join(root, name);
    fs.mkdirSync(path.join(directory, "dist"), { recursive: true });
    fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({ name: "pi-fabric", type: "module" }));
    fs.writeFileSync(path.join(directory, "dist/worker.js"), worker);
    fs.writeFileSync(path.join(directory, "dist/index.js"), "export {};\n");
    if (version !== undefined) fs.writeFileSync(path.join(directory, "dist/worker-protocol.json"), JSON.stringify({ version }));
    return directory;
  };
  const parent = release("parent");
  const select = (directory: string) => fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ packages: [{ source: directory }] }));
  select(parent);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], maxConcurrent: 1, timeoutMs: 4_000 }, {
    workerPath: path.join(parent, "dist/worker.js"), fabricExtensionPath: path.join(parent, "dist/index.js"),
    fullCodeMode, runRoot: path.join(root, "runs"),
  });
  managers.push(manager);
  return { root, profile, parent, manager, release, select };
};

const probe = async (f: ReturnType<typeof fixture>, expected: string) => {
  const handle = await f.manager.spawn({ task: "probe", transport: "process" });
  expect(handle.fabricRelease).toBe(expected);
  const result = await f.manager.wait(handle.id);
  expect(result.status, result.error).toBe("completed");
  expect(result.fabricRelease).toBe(expected);
  expect(JSON.parse(result.text)).toMatchObject({
    worker: pathToFileURL(path.join(expected, "dist/worker.js")).href,
    extension: path.join(expected, "dist/index.js"), release: expected,
  });
  return result;
};

describe("process transport spawn-time Fabric release selection", () => {
  it("runs the newly active compatible release, resolving settings anew for each spawn", async () => {
    const f = fixture();
    const current = f.release("current-newer");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    f.select(current); // Activation after manager construction must affect the next child.
    await probe(f, current);
    f.select(f.parent);
    await probe(f, f.parent);
    expect(warn).not.toHaveBeenCalled();
  });

  it("follows canonical installed roots when the parent's worker is addressed through a symlink", async () => {
    const f = fixture();
    const current = f.release("canonical-current");
    const alias = path.join(f.root, "parent-alias");
    fs.symlinkSync(f.parent, alias, "junction");
    const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 4_000 }, {
      workerPath: path.join(alias, "dist/worker.js"), fabricExtensionPath: path.join(alias, "dist/index.js"),
      fullCodeMode: true, runRoot: path.join(f.root, "alias-runs"),
    });
    managers.push(manager);
    f.select(current);
    await probe({ ...f, manager }, current);
  });

  it("preserves an explicit custom worker instead of replacing it with the installed release", async () => {
    const f = fixture();
    const custom = path.join(f.parent, "custom-worker.mjs");
    fs.writeFileSync(custom, worker);
    const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 4_000 }, {
      workerPath: custom, fabricExtensionPath: path.join(f.parent, "dist/index.js"),
      fullCodeMode: true, runRoot: path.join(f.root, "custom-runs"),
    });
    managers.push(manager);
    f.select(f.release("installed-but-not-custom"));
    const result = await manager.run({ task: "custom probe", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.fabricRelease).toBe(f.parent);
    expect(JSON.parse(result.text).worker).toBe(pathToFileURL(custom).href);
  });

  it("preserves an explicit non-Fabric hook while selecting the installed worker", async () => {
    const f = fixture();
    const hook = path.join(f.root, "custom-hook.mjs");
    fs.writeFileSync(hook, "export default function () {}\n");
    const current = f.release("custom-hook-current");
    const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 4_000 }, {
      workerPath: path.join(f.parent, "dist/worker.js"), fabricExtensionPath: hook,
      fullCodeMode: true, runRoot: path.join(f.root, "custom-hook-runs"),
    });
    managers.push(manager); f.select(current);
    const result = await manager.run({ task: "custom hook", transport: "process" });
    expect(result.status).toBe("completed");
    expect(result.fabricRelease).toBe(current);
    expect(JSON.parse(result.text)).toMatchObject({ worker: pathToFileURL(path.join(current, "dist/worker.js")).href, extension: hook });
  });

  it("selects the current worker without enabling Fabric when extensions are disabled", async () => {
    const f = fixture(false);
    const current = f.release("no-extensions-current");
    f.select(current);
    const result = await f.manager.run({ task: "native only", transport: "process", extensions: false });
    expect(result.status).toBe("completed");
    expect(result.fabricRelease).toBe(current);
    expect(JSON.parse(result.text)).toMatchObject({ worker: pathToFileURL(path.join(current, "dist/worker.js")).href, release: current });
    expect(JSON.parse(result.text).extension).toBeUndefined();
  });

  it("pins the selected extension even outside full-code/recursive mode", async () => {
    const f = fixture(false);
    const current = f.release("native-tools-current");
    f.select(current);
    await probe(f, current);
  });

  it("falls back to the parent for an incompatible protocol and emits one warning naming both", async () => {
    const f = fixture();
    const current = f.release("incompatible", WORKER_PROTOCOL_VERSION + 1);
    f.select(current);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await probe(f, f.parent);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0]?.[0]).toContain(current);
    expect(warn.mock.calls[0]?.[0]).toContain(f.parent);
    expect(warn.mock.calls[0]?.[0]).toMatch(/protocol/i);
  });

  it("pins the parent's extension on incompatible fallback outside full-code mode", async () => {
    const f = fixture(false);
    f.select(f.release("native-incompatible", WORKER_PROTOCOL_VERSION + 1));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await probe(f, f.parent);
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("uses the parent without a warning when the profile has no active pointer", async () => {
    const f = fixture();
    fs.unlinkSync(path.join(f.profile, "settings.json"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await probe(f, f.parent);
    expect(warn).not.toHaveBeenCalled();
  });

  it.each(["unversioned", "malformed", "missing-worker", "missing-extension"])("safely falls back for a %s installed release", async kind => {
    const f = fixture();
    const current = f.release(kind);
    const manifest = path.join(current, "dist/worker-protocol.json");
    if (kind === "unversioned") fs.unlinkSync(manifest);
    if (kind === "malformed") fs.writeFileSync(manifest, "not-json");
    if (kind === "missing-worker") fs.unlinkSync(path.join(current, "dist/worker.js"));
    if (kind === "missing-extension") fs.unlinkSync(path.join(current, "dist/index.js"));
    f.select(current);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await probe(f, f.parent);
    expect(warn).toHaveBeenCalledTimes(1);
  });
});
