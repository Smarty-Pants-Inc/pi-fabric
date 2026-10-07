import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { WORKER_PROTOCOL_VERSION } from "../src/agents/worker-protocol.js";
import type { AgentHandleInfo } from "../src/agents/types.js";

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
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-spawn-release-")));
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
  let launched: AgentHandleInfo | undefined;
  const handle = await f.manager.spawn({ task: "probe", transport: "process" }, undefined, undefined, undefined, undefined,
    value => { launched = value; });
  // A terminal result can precede native close on Windows. The next spawn may
  // return a queued receipt, whose release has not been selected yet. Assert
  // the launch callback as well as the result, never guess it at admission.
  const result = await f.manager.wait(handle.id);
  expect(result.status, result.error).toBe("completed");
  expect(launched).toMatchObject({ id: handle.id, status: "running", fabricRelease: expected });
  if (handle.status !== "queued") expect(handle.fabricRelease).toBe(expected);
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

  it("selects release metadata at native launch, not when returning a queued receipt", async () => {
    const f = fixture();
    const gate = path.join(f.root, "finish-first");
    // Hold the first worker before it writes its terminal record, independent
    // of scheduling speed and the platform's native-close implementation.
    fs.writeFileSync(path.join(f.parent, "dist/worker.js"), worker.replace("const args = new Map();", `
await new Promise(resolve => {
  const timer = setInterval(() => { if (fs.existsSync(${JSON.stringify(gate)})) { clearInterval(timer); resolve(); } }, 10);
});
const args = new Map();`));
    const first = await f.manager.spawn({ task: "first", transport: "process" });
    let launched: AgentHandleInfo | undefined;
    const queued = await f.manager.spawn({ task: "queued", transport: "process" }, undefined, undefined, undefined, undefined,
      value => { launched = value; });
    expect(queued.status).toBe("queued");
    expect(queued.fabricRelease).toBeUndefined();
    expect(launched).toBeUndefined();
    const current = f.release("activated-while-queued");
    f.select(current);
    fs.writeFileSync(gate, "release");
    expect((await f.manager.wait(first.id)).status).toBe("completed");
    const result = await f.manager.wait(queued.id);
    expect(result.status, result.error).toBe("completed");
    expect(launched).toMatchObject({ id: queued.id, status: "running", fabricRelease: current });
    expect(result.fabricRelease).toBe(current);
    expect(JSON.parse(result.text)).toMatchObject({
      worker: pathToFileURL(path.join(current, "dist/worker.js")).href,
      extension: path.join(current, "dist/index.js"), release: current,
    });
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

  it("preserves an explicit non-Fabric hook inside a pi-fabric checkout while selecting the installed worker", async () => {
    const f = fixture();
    fs.copyFileSync(path.resolve("package.json"), path.join(f.root, "package.json"));
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

// Keep processSlice fixtures isolated from release-selection fixtures.
{
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = (script = 'while [ "$1" != "--" ]; do shift; done; shift; exec "$@"') => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-slice-")); roots.push(root);
  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, 'import fs from "node:fs"; fs.appendFileSync("started", String(process.pid)+"\\n"); setInterval(() => {}, 1000);');
  fs.writeFileSync(path.join(root, "systemd-run"), `#!/bin/sh\nprintf '%s\\n' "$@" > "${root}/scope-args"\n${script}\n`, { mode: 0o700 });
  vi.stubEnv("PATH", root); // runtime is process.execPath, never PATH
  return { root, worker, request: { id: "test", name: "test", cwd: root, workerPath: worker, workerArguments: [] } };
};
const workerStarted = async (root: string) => { await vi.waitFor(() => expect(fs.existsSync(path.join(root, "started"))).toBe(true)); return Number(fs.readFileSync(path.join(root, "started"), "utf8").trim()); };

describe.skipIf(process.platform !== "linux")("ProcessTransport processSlice (#4383)", () => {
  it("propagates the host slice from AgentManager through a real process worker", async () => {
    const f = fixture();
    const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, processSlice: "batch.slice", budgetUsd: 0, sessionExport: false, timeoutMs: 5_000 }, {
      runRoot: path.join(f.root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    try {
      const result = await manager.run({ task: "slice propagation", transport: "process" });
      expect(result.status).toBe("completed");
      expect(fs.readFileSync(path.join(f.root, "scope-args"), "utf8")).toContain("--slice=batch.slice\n");
    } finally { await manager.close(); }
  });
  it("keeps direct launches on non-Linux platforms even when configured", async () => {
    const f = fixture();
    // Portable custody needs ps, while the scope fixture intentionally hides PATH.
    vi.stubEnv("PATH", `${f.root}:/usr/bin:/bin`);
    vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const handle = await new ProcessTransport("batch.slice").launch(f.request);
    try { expect(await workerStarted(f.root)).toBe(Number(handle.sessionId)); expect(fs.existsSync(path.join(f.root, "scope-args"))).toBe(false); expect(warn).not.toHaveBeenCalled(); }
    finally { await handle.stop(); await handle.waitForClose?.(); }
  });
  it("rechecks launch authority before a failed scope can fall back", async () => {
    const f = fixture("/bin/sleep 0.1; exit 1"); let allowed = true;
    const launching = new ProcessTransport("batch.slice").launch({ ...f.request, authorize: () => allowed });
    const outcome = launching.then(() => undefined, error => error);
    await vi.waitFor(() => expect(fs.existsSync(path.join(f.root, "scope-args"))).toBe(true));
    allowed = false;
    expect(await outcome).toBeInstanceOf(Error);
    expect(fs.existsSync(path.join(f.root, "started"))).toBe(false);
  });
  it("unconfirmed scope teardown vetoes fallback and retains custody debt", async () => {
    const f = fixture("trap '' TERM; while :; do :; done"); const debt = vi.fn();
    await expect(new ProcessTransport("batch.slice").launch({ ...f.request, onUnconfirmedExit: debt }))
      .rejects.toThrow("termination is unconfirmed");
    expect(debt).toHaveBeenCalledOnce(); expect(fs.existsSync(path.join(f.root, "started"))).toBe(false);
  }, 20_000);

  it("is off by default", async () => {
    const f = fixture(); const handle = await new ProcessTransport().launch(f.request);
    try { expect(await workerStarted(f.root)).toBe(Number(handle.sessionId)); expect(fs.existsSync(path.join(f.root, "scope-args"))).toBe(false); }
    finally { await handle.stop(); await handle.waitForClose?.(); }
  });
  it("execs the scoped worker in place, retaining PID, PGID, native close and exit fence", async () => {
    const f = fixture(); const warn = vi.spyOn(console, "warn");
    const handle = await new ProcessTransport("batch.slice").launch(f.request);
    const pid = Number(handle.sessionId);
    try {
      expect(await workerStarted(f.root)).toBe(pid);
      const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8").slice(fs.readFileSync(`/proc/${pid}/stat`, "utf8").lastIndexOf(")") + 2).split(" ");
      expect(Number(stat[2])).toBe(pid); // field 5 is PGID
      expect(fs.readFileSync(path.join(f.root, "scope-args"), "utf8").split("\n").slice(0, 6)).toEqual(["--user", "--scope", "--slice=batch.slice", "--quiet", "--collect", "--"]);
      expect(await handle.isAlive()).toBe(true); expect(warn).not.toHaveBeenCalled();
    } finally { await handle.stop(); await handle.waitForClose?.(); }
    expect(await handle.isAlive()).toBe(false); expect(handle.lostContact?.()).toBeUndefined();
    const kill = vi.spyOn(process, "kill"); await handle.stop(); expect(kill).not.toHaveBeenCalled();
  });
  it.each(["unavailable", "failed"])("warns once and launches directly when systemd-run is %s", async mode => {
    const f = fixture("exit 1"); if (mode === "unavailable") fs.unlinkSync(path.join(f.root, "systemd-run"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {}); const transport = new ProcessTransport("batch.slice");
    for (let i = 0; i < 2; i++) {
      if (fs.existsSync(path.join(f.root, "started"))) fs.unlinkSync(path.join(f.root, "started"));
      const handle = await transport.launch(f.request);
      try { expect(await workerStarted(f.root)).toBe(Number(handle.sessionId)); }
      finally { await handle.stop(); await handle.waitForClose?.(); }
    }
    expect(warn).toHaveBeenCalledTimes(1); expect(warn.mock.calls[0]![0]).toContain("launching worker normally");
  });
  it("handles an executable disappearing after lookup with a close-fenced fallback", async () => {
    const f = fixture(); const warn = vi.fn();
    const handle = await spawnDetached(f.worker, [], f.root, undefined, undefined, { executable: path.join(f.root, "missing"), slice: "batch.slice", warn });
    try { expect(await workerStarted(f.root)).toBe(handle.pid); expect(warn).toHaveBeenCalledOnce(); }
    finally { await handle.stop(); await handle.waitForClose(); }
  });
  it("does not replay a worker that fails after successful scope admission", async () => {
    const f = fixture(); fs.writeFileSync(f.worker, 'import fs from "node:fs"; fs.appendFileSync("started", String(process.pid)+"\\n"); process.exit(1);');
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const handle = await new ProcessTransport("batch.slice").launch(f.request); await handle.waitForClose?.();
    expect(fs.readFileSync(path.join(f.root, "started"), "utf8").trim().split("\n")).toEqual([handle.sessionId]);
    expect(warn).not.toHaveBeenCalled(); expect(await handle.isAlive()).toBe(false);
  });
});
}
