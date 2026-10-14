import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { TaskCgroupStopUnconfirmedError, TaskScopeAdmissionError } from "../src/agents/transports/task-cgroup.js";
import fs from "node:fs";
import net from "node:net";
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

  it("never auto-selects a v1 worker without the receipt/attributed-refusal boundary", async () => {
    expect(WORKER_PROTOCOL_VERSION).toBe(2);
    const f = fixture();
    const legacy = f.release("legacy-terminal-protocol", 1);
    f.select(legacy);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await probe(f, f.parent);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]?.[0]).toContain("worker protocol 1");
    expect(warn.mock.calls[0]?.[0]).toContain("worker protocol 2");
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
  // smarty-dev#8257: keep these uncapped processSlice fixtures ineligible for the default task cap on
  // any host (a developer's live user bus must not turn them into capped launches).
  vi.stubEnv("XDG_RUNTIME_DIR", root); vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
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
      const scopeArgs = fs.readFileSync(path.join(f.root, "scope-args"), "utf8").split("\n");
      expect(scopeArgs.slice(0, 5)).toEqual(["--user", "--scope", "--slice=batch.slice", "--quiet", "--collect"]);
      expect(scopeArgs[5]).toMatch(/^--unit=fabric-scope-.+\.scope$/);
      expect(scopeArgs[6]).toBe("--");
      // A fake systemd-run admits into the caller cgroup, not our unique scope.
      expect(handle.liveness).toBeUndefined();
      expect(handle.treeClosed).toBeUndefined();
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
  it("smarty-dev#8257: a nested launch skips the parent task's systemd-run PATH shim and admits via the next real backend", async () => {
    const f = fixture();
    const shimDir = path.join(f.root, "task-shim"); fs.mkdirSync(shimDir);
    const shim = path.join(shimDir, "systemd-run");
    fs.writeFileSync(shim, `#!/bin/sh\nprintf ran >> "${f.root}/shim-ran"\nexit 1\n`, { mode: 0o700 });
    vi.stubEnv("PATH", `${shimDir}${path.delimiter}${f.root}`);
    vi.stubEnv("FABRIC_TASK_SYSTEMD_RUN_WRAPPER", shim);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const handle = await new ProcessTransport("batch.slice").launch(f.request);
    try {
      expect(await workerStarted(f.root)).toBe(Number(handle.sessionId));
      expect(fs.existsSync(path.join(f.root, "shim-ran"))).toBe(false);
      expect(fs.readFileSync(path.join(f.root, "scope-args"), "utf8")).toContain("--slice=batch.slice\n");
      expect(warn).not.toHaveBeenCalled();
    } finally { await handle.stop(); await handle.waitForClose?.(); }
    expect(fs.readFileSync(path.join(f.root, "started"), "utf8").trim().split("\n")).toEqual([handle.sessionId]); // exec'd once
  });
  it("smarty-dev#8257: without the wrapper variable, ordinary first-on-PATH lookup is unchanged", async () => {
    const f = fixture();
    const shimDir = path.join(f.root, "first"); fs.mkdirSync(shimDir);
    fs.writeFileSync(path.join(shimDir, "systemd-run"), `#!/bin/sh\nprintf ran >> "${f.root}/shim-ran"\nexit 1\n`, { mode: 0o700 });
    vi.stubEnv("PATH", `${shimDir}${path.delimiter}${f.root}`);
    vi.stubEnv("FABRIC_TASK_SYSTEMD_RUN_WRAPPER", undefined);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const handle = await new ProcessTransport("batch.slice").launch(f.request);
    try {
      expect(await workerStarted(f.root)).toBe(Number(handle.sessionId));
      expect(fs.readFileSync(path.join(f.root, "shim-ran"), "utf8")).toBe("ran"); // first entry used, failed
      expect(fs.existsSync(path.join(f.root, "scope-args"))).toBe(false);
      expect(warn).toHaveBeenCalledOnce(); // existing failed-scope fallback
    } finally { await handle.stop(); await handle.waitForClose?.(); }
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

// smarty-dev#8257 MECHANISM tests (fake systemd-run, no real scope): capped-scope argv, eligibility
// and fallbacks. The real cgroup/memory/stop path is proven only by tests/task-cgroup.test.ts.
{
const roots: string[] = [];
const servers: net.Server[] = [];
afterEach(async () => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const server of servers.splice(0)) await new Promise(resolve => server.close(resolve));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const GIB = 1024 ** 3;
const isV2 = (() => { try { return fs.statfsSync("/sys/fs/cgroup").type === 0x63677270; } catch { return false; } })();
/** Eligibility inputs: a fixture-owned listening bus socket (or none), and the memory controller's
 * delegation to this UID's user manager, simulated by answering ONLY its subtree_control read. */
const fixture = async (bus: "live" | "none", delegated = true) => {
  const uid = process.getuid!();
  const readFileSync = fs.readFileSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) =>
    String(file).endsWith(`/user@${uid}.service/cgroup.subtree_control`)
      ? (delegated ? "cpu io memory pids\n" : "cpu io pids\n")
      : (readFileSync as (...a: unknown[]) => string | Buffer)(file, ...rest)) as typeof fs.readFileSync);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-cap-")); roots.push(root);
  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, 'import fs from "node:fs"; fs.appendFileSync("started", String(process.pid)+"\\n"); setInterval(() => {}, 1000);');
  fs.writeFileSync(path.join(root, "systemd-run"), `#!/bin/sh\nprintf '%s\\n' "$@" > "${root}/scope-args"\nwhile [ "$1" != "--" ]; do shift; done; shift; exec "$@"\n`, { mode: 0o700 });
  vi.stubEnv("PATH", root);
  const runtime = path.join(root, "run"); fs.mkdirSync(runtime, { mode: 0o700 });
  vi.stubEnv("XDG_RUNTIME_DIR", runtime);
  if (bus === "live") {
    const server = net.createServer(); servers.push(server);
    await new Promise<void>(resolve => server.listen(path.join(runtime, "bus"), resolve));
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", `unix:path=${path.join(runtime, "bus")}`);
  } else vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
  return { root, worker, request: { id: "cap", name: "cap", cwd: root, workerPath: worker, workerArguments: [] as string[] } };
};
const started = async (root: string) => { await vi.waitFor(() => expect(fs.existsSync(path.join(root, "started"))).toBe(true)); return Number(fs.readFileSync(path.join(root, "started"), "utf8").trim()); };
const scopeArgs = (root: string): string[] | undefined => fs.existsSync(path.join(root, "scope-args"))
  ? fs.readFileSync(path.join(root, "scope-args"), "utf8").split("\n").filter(Boolean) : undefined;
/** Accept `-p X=Y` or `--property=X=Y`. */
const properties = (args: string[]): string[] => args.flatMap((arg, index) =>
  arg.startsWith("--property=") ? [arg.slice(11)] : args[index - 1] === "-p" ? [arg] : []);
const launch = async (transport: ProcessTransport, request: Awaited<ReturnType<typeof fixture>>["request"], root: string) => {
  vi.spyOn(console, "warn").mockImplementation(() => {});
  const handle = await transport.launch(request);
  try { expect(await started(root)).toBe(Number(handle.sessionId)); return handle; }
  finally { await handle.stop(); await handle.waitForClose?.(); }
};

/** A fake runner admits into the caller's cgroup: the capped receipt can never verify. The launch
 * must fail typed, BEFORE the gate releases the worker, with no unconfined replay. */
const refused = async (transport: ProcessTransport, request: Awaited<ReturnType<typeof fixture>>["request"], root: string) => {
  vi.spyOn(console, "warn").mockImplementation(() => {});
  const outcome = await transport.launch(request).then(async handle => { await handle.stop(); return handle; }, (error: unknown) => error);
  expect(outcome).toBeInstanceOf(TaskScopeAdmissionError);
  expect((outcome as TaskScopeAdmissionError).code).toBe("TASK_SCOPE_ADMISSION_FAILED");
  expect((outcome as TaskScopeAdmissionError).unit).toMatch(/^fabric-task-.+\.scope$/);
  await new Promise(resolve => setTimeout(resolve, 200));
  expect(fs.existsSync(path.join(root, "started")), "worker ran outside a verified capped scope").toBe(false);
};

describe.skipIf(process.platform !== "linux" || !isV2)("ProcessTransport task memory cap mechanism (smarty-dev#8257)", () => {
  it("an eligible task requests a capped fabric-task scope in app.slice with swap 0; an unverifiable receipt fails typed with no worker", async () => {
    const f = await fixture("live");
    await refused(new ProcessTransport(undefined, undefined, undefined, 24 * GIB), f.request, f.root);
    const args = scopeArgs(f.root)!;
    expect(args).toBeDefined();
    expect(args.slice(0, 2)).toEqual(["--user", "--scope"]);
    expect(args).toContain("--slice=app.slice");
    expect(args.find(arg => arg.startsWith("--unit="))).toMatch(/^--unit=fabric-task-.+\.scope$/);
    expect(properties(args)).toEqual(expect.arrayContaining([expect.stringMatching(/^MemoryMax=(25769803776|24G)$/), "MemorySwapMax=0"]));
  });

  it("passes a custom cap in bytes and keeps a host processSlice as the parent slice", async () => {
    const f = await fixture("live");
    await refused(new ProcessTransport("batch.slice", undefined, undefined, GIB), f.request, f.root);
    const args = scopeArgs(f.root)!;
    expect(args).toContain("--slice=batch.slice");
    expect(args.find(arg => arg.startsWith("--unit="))).toMatch(/^--unit=fabric-task-.+\.scope$/);
    expect(properties(args)).toEqual(expect.arrayContaining([`MemoryMax=${GIB}`, "MemorySwapMax=0"]));
  });

  it("does not cap actors: an actor launch keeps the uncapped processSlice scope, or none", async () => {
    const f = await fixture("live");
    vi.stubEnv("FABRIC_TASK_MEMORY_MAX", "25769803776");
    vi.stubEnv("FABRIC_TASK_CGROUP", "/a/fabric-task-parent.scope");
    vi.stubEnv("FABRIC_TASK_SYSTEMD_RUN_WRAPPER", "/parent/bin/systemd-run");
    vi.stubEnv("PI_FABRIC_ROLE", "actor");
    fs.appendFileSync(f.worker, '\nfs.writeFileSync("actor-env.json", JSON.stringify(process.env));');
    const actor = { ...f.request, workerArguments: ["--actor-id", "actor:a"] };
    await launch(new ProcessTransport(undefined, undefined, undefined, 24 * GIB), actor, f.root);
    expect(scopeArgs(f.root)).toBeUndefined();
    fs.unlinkSync(path.join(f.root, "started"));
    await launch(new ProcessTransport("batch.slice", undefined, undefined, 24 * GIB), actor, f.root);
    const args = scopeArgs(f.root)!;
    expect(args.find(arg => arg.startsWith("--unit="))).toMatch(/^--unit=fabric-scope-.+\.scope$/);
    expect(properties(args).filter(p => /^Memory(Swap)?Max=/.test(p))).toEqual([]);
    const environment = JSON.parse(fs.readFileSync(path.join(f.root, "actor-env.json"), "utf8"));
    for (const key of ["FABRIC_TASK_MEMORY_MAX", "FABRIC_TASK_CGROUP", "FABRIC_TASK_SYSTEMD_RUN_WRAPPER"]) {
      expect(environment[key], `uncapped actor inherited ${key}`).toBeUndefined();
    }
    expect(environment.PI_FABRIC_ROLE).toBe("actor");
    const { cappedTaskBashBlockReason } = await import("../src/guards/actor-bash-hook.js");
    expect(cappedTaskBashBlockReason(environment, "/usr/bin/systemd-run --user --scope -- true")).toBeUndefined();
  });

  it("without a user bus the cap is ineligible: direct launch, no systemd-run, no cgroup claim", async () => {
    const f = await fixture("none");
    const handle = await launch(new ProcessTransport(undefined, undefined, undefined, 24 * GIB), f.request, f.root);
    expect(scopeArgs(f.root)).toBeUndefined();
    expect(handle.cgroup).toBeUndefined();
  });

  it("without the memory controller delegated to the user manager the cap is ineligible", async () => {
    const f = await fixture("live", false);
    const handle = await launch(new ProcessTransport(undefined, undefined, undefined, 24 * GIB), f.request, f.root);
    expect(scopeArgs(f.root)).toBeUndefined();
    expect(handle.cgroup).toBeUndefined();
  });

  it("without unified cgroup v2 (no root cgroup.controllers) the cap is ineligible", async () => {
    const f = await fixture("live");
    const statSync = fs.statSync;
    vi.spyOn(fs, "statSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (String(file) === "/sys/fs/cgroup/cgroup.controllers") throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
      return (statSync as (...a: unknown[]) => fs.Stats)(file, ...rest);
    }) as typeof fs.statSync);
    const handle = await launch(new ProcessTransport(undefined, undefined, undefined, 24 * GIB), f.request, f.root);
    expect(scopeArgs(f.root)).toBeUndefined();
    expect(handle.cgroup).toBeUndefined();
  });

  it("keeps direct non-Linux launches unchanged when a cap is configured", async () => {
    const f = await fixture("live");
    vi.stubEnv("PATH", `${f.root}:/usr/bin:/bin`); // portable custody needs ps
    vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    const handle = await launch(new ProcessTransport(undefined, undefined, undefined, 24 * GIB), f.request, f.root);
    expect(scopeArgs(f.root)).toBeUndefined();
    expect(handle.cgroup).toBeUndefined();
    expect(console.warn).not.toHaveBeenCalled();
  });

  it("an omitted cap defaults to 24 GiB on an eligible host; an invalid cap is refused at construction", async () => {
    const f = await fixture("live");
    await refused(new ProcessTransport(), f.request, f.root);
    expect(properties(scopeArgs(f.root)!)).toEqual(expect.arrayContaining([`MemoryMax=${24 * GIB}`, "MemorySwapMax=0"]));
    for (const bad of [0, -1, 1.5, Number.NaN]) expect(() => new ProcessTransport(undefined, undefined, undefined, bad)).toThrow(/memoryMaxBytes/);
  });
});
}

const directory = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-transport-"));
  roots.push(root);
  return root;
};

describe("process transport startup diagnostics", () => {
  it("rejects a failed spawn without an unhandled child error", async () => {
    const root = directory();
    await expect(spawnDetached(path.join(root, "missing.mjs"), [], path.join(root, "missing-cwd"))).rejects.toThrow(/ENOENT/);
    await new Promise(resolve => setImmediate(resolve));
  });

  // Upstream bounded stderr capture is not ported yet (smarty-dev#5776 follow-up): a piped stderr
  // inherited by execution descendants would hold back the child "close" event our process-tree
  // custody relies on. Unskip with the port.
  it.skip("surfaces bounded stderr even when the worker cannot execute its bootstrap", async () => {
    const root = directory();
    const worker = path.join(root, "crash.mjs");
    fs.writeFileSync(worker, 'process.stderr.write("x".repeat(60000) + "\\nworker-bootstrap-sentinel 界面\\n"); process.exitCode = 1;');
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 20000, sessionExport: false }, {
      workerPath: worker, runRoot: path.join(root, "runs"), fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "probe", transport: "process", extensions: false });
    expect(result.status).toBe("failed");
    expect(result.error).toContain("worker stderr:");
    expect(result.error).toContain("worker-bootstrap-sentinel 界面");
    expect(result.error).not.toContain("�");
    expect(result.error!.length).toBeLessThan(21000);
  });
});

// smarty-dev#8257 MECHANISM (env only): a NEW task never inherits the parent's capped-boundary evidence,
// while the worker's own same-run native child keeps it (opt-in preserveTaskBoundary).
describe("taskAgentEnvironment capped-boundary evidence (smarty-dev#8257)", () => {
  const parent = { PATH: "/task/bin:/usr/bin", FABRIC_TASK_MEMORY_MAX: "25769803776", FABRIC_TASK_CGROUP: "/a/fabric-task-x.scope",
    FABRIC_TASK_SYSTEMD_RUN_WRAPPER: "/task/bin/systemd-run", OTHER: "kept" };
  it("scrubs memory/cgroup/wrapper by default (new task launch), leaving PATH and others untouched", async () => {
    const { taskAgentEnvironment } = await import("../src/agents/task-environment.js");
    const child = taskAgentEnvironment({ ...parent });
    expect(child.FABRIC_TASK_MEMORY_MAX).toBeUndefined();
    expect(child.FABRIC_TASK_CGROUP).toBeUndefined();
    expect(child.FABRIC_TASK_SYSTEMD_RUN_WRAPPER).toBeUndefined();
    expect(child).toMatchObject({ PATH: parent.PATH, OTHER: "kept" });
    expect(taskAgentEnvironment({ ...parent }, false).FABRIC_TASK_CGROUP).toBeUndefined();
  });
  it("new actor scrub keeps roles while dropping only the caller's boundary", async () => {
    const { withoutTaskBoundary } = await import("../src/agents/task-environment.js");
    const environment = { ...parent, SMARTY_ROLE: "actor@abcdef123456", PI_FABRIC_ROLE: "actor", PI_FABRIC_ACTOR_ID: "actor:a" };
    const child = withoutTaskBoundary(environment);
    expect(child).toMatchObject({ SMARTY_ROLE: environment.SMARTY_ROLE, PI_FABRIC_ROLE: "actor", PI_FABRIC_ACTOR_ID: "actor:a", PATH: parent.PATH });
    for (const key of ["FABRIC_TASK_MEMORY_MAX", "FABRIC_TASK_CGROUP", "FABRIC_TASK_SYSTEMD_RUN_WRAPPER"]) expect(child[key]).toBeUndefined();
    expect(environment.FABRIC_TASK_CGROUP).toBe(parent.FABRIC_TASK_CGROUP); // original task stays truthful
  });
  it("preserves all three for the same run when preserveTaskBoundary is true", async () => {
    const { taskAgentEnvironment } = await import("../src/agents/task-environment.js");
    expect(taskAgentEnvironment({ ...parent }, true)).toMatchObject({ FABRIC_TASK_MEMORY_MAX: parent.FABRIC_TASK_MEMORY_MAX,
      FABRIC_TASK_CGROUP: parent.FABRIC_TASK_CGROUP, FABRIC_TASK_SYSTEMD_RUN_WRAPPER: parent.FABRIC_TASK_SYSTEMD_RUN_WRAPPER });
  });
});

// smarty-dev#8257 MECHANISM (mock transport handle via ProcessTransport.launch, no OS process): the
// public agents.stop path after a latched watcher failure. Excludes the real kernel/EPERM branch
// (tests/task-cgroup.test.ts) and native Pi (named proof .local/out/prove-task-stop.mjs).
describe("AgentManager stop after latched lostContact (smarty-dev#8257)", () => {
  const debtRoots: string[] = [];
  const debtManagers: AgentManager[] = [];
  afterEach(async () => {
    vi.restoreAllMocks();
    for (const manager of debtManagers.splice(0)) await manager.close().catch(() => undefined);
    for (const root of debtRoots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const scenario = async (cgroup: string | undefined) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stop-debt-")); debtRoots.push(root);
    const debt = "Owned scope watcher failed: Error: EIO";
    const stop = vi.fn(async () => {
      if (cgroup) throw new TaskCgroupStopUnconfirmedError(cgroup, [424242], "SIGTERM/SIGKILL did not empty the subtree");
    });
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async () => ({
      kind: "process" as const, sessionId: "424241", ...(cgroup ? { cgroup } : {}),
      isAlive: async () => true, lostContact: () => debt, stopDebt: () => debt,
      waitForClose: async () => undefined, closed: new Promise<void>(() => undefined), stop,
    }));
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false, timeoutMs: 60_000 },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    debtManagers.push(manager);
    const handle = await manager.spawn({ task: "HANG", transport: "process" });
    const outcome = await manager.stop(handle.id).then(result => result, (error: unknown) => error);
    return { stop, outcome };
  };
  it("a cgroup-bearing handle still runs its owned stop and surfaces TASK_CGROUP_STOP_UNCONFIRMED with exact pids", async () => {
    const cgroup = "/user.slice/user-1.slice/user@1.service/app.slice/fabric-task-x.scope";
    const { stop, outcome } = await scenario(cgroup);
    expect(stop).toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(TaskCgroupStopUnconfirmedError);
    expect((outcome as TaskCgroupStopUnconfirmedError).code).toBe("TASK_CGROUP_STOP_UNCONFIRMED");
    expect([...(outcome as TaskCgroupStopUnconfirmedError).pids]).toEqual([424242]);
    expect((outcome as TaskCgroupStopUnconfirmedError).cgroup).toBe(cgroup);
  });
  it("counterexample: an uncapped handle with the same native debt keeps logical stop (no typed throw)", async () => {
    const { outcome } = await scenario(undefined);
    expect(outcome).not.toBeInstanceOf(Error);
    expect((outcome as { status?: string }).status).toBeDefined();
  });
});
