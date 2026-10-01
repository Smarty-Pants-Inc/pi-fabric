import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { processStartTime, residentProcessAlive } from "../src/residency/process-identity.js";
import {
  residentLaunchSpec, handoverPath, mainGenerationPath, handoverCustodyPath, handoverOutcomePath,
  writeLaunchSnapshot, writeHandoverState, writeHandoverImmutable, readHandoverJson,
  type ResidentHandoverPlan, type ResidentHandoverState, type ResidentMainGeneration,
} from "../src/residency/handover.js";
import type { ResidentHostConfig, ResidentHostOwner } from "../src/residency/protocol.js";
import { launchLog, stopAllOwned } from "./helpers/owned-processes.js";

const cleanups = new Set<() => Promise<void>>();
afterEach(async () => { await Promise.all([...cleanups].map((cleanup) => cleanup())); });
const available = process.platform === "linux" && fs.existsSync("dist/residency/launcher.js");
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function until(predicate: () => boolean, timeout = 15_000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error("Timed out at supervised launcher boundary"); await delay(20); }
}
const logs = (root: string): Array<Record<string, unknown>> => {
  try { return fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)); }
  catch { return []; }
};
async function fixture(mode: "ready" | "exit" | "hang" | "terminal-block" | "fast-exit" | "fallback-uncertain", killableMain = false) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-launcher-handover-"));
  const ownership = launchLog(root);
  const a = path.join(root, "A"), b = path.join(root, "B");
  for (const release of [a, b]) {
    fs.mkdirSync(release);
    fs.cpSync("dist", path.join(release, "dist"), { recursive: true });
    fs.writeFileSync(path.join(release, "package.json"), JSON.stringify({ name: "pi-fabric", type: "module" }));
    fs.symlinkSync(path.resolve("node_modules"), path.join(release, "node_modules"), "dir");
  }
  const residencyRoot = path.join(root, "resident"); fs.mkdirSync(residencyRoot);
  const config: ResidentHostConfig = { format: 1, rootId: "session:supervised", sessionId: "supervised", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot, fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(a, "dist/worker.js"), fabricExtensionPath: path.join(a, "dist/index.js"),
    piBinary: path.resolve("tests/fixtures/handover-pi.mjs"), claudeBinary: "fixture-claude", vedaBinary: "fixture-veda", kernel: "typescript", pythonRuntime: "monty",
    piModels: { available: [{ provider: "fixture", id: "A" }], aliases: {}, defaultModel: "fixture/A" } };
  const previous = residentLaunchSpec(config, path.join(a, "dist/residency/pi-entry.js"));
  const target = residentLaunchSpec({ ...config, workerPath: path.join(b, "dist/worker.js"), fabricExtensionPath: path.join(b, "dist/index.js"),
    kernel: "python", pythonRuntime: "cpython", piModels: { available: [{ provider: "fixture", id: "B" }], aliases: {}, defaultModel: "fixture/B" } }, path.join(b, "dist/residency/pi-entry.js"));
  const configPath = path.join(residencyRoot, "config.json"); fs.writeFileSync(configPath, JSON.stringify(previous.config));
  const fault = path.join(root, "fallback-fsync-fault.mjs");
  fs.writeFileSync(fault, `import fs from 'node:fs';
const sync = fs.fsyncSync;
let injected = false;
fs.fsyncSync = function(fd) {
 const config = process.env.PI_FABRIC_TEST_FAULT_ROOT || ${JSON.stringify(residencyRoot)};
 let state; try { state = JSON.parse(fs.readFileSync(config + '/handover.json', 'utf8')); } catch {}
 if (!injected && !process.env.PI_FABRIC_RESIDENT_LAUNCHER && fs.fstatSync(fd).isDirectory() && state?.phase === 'fallback') {
   injected = true;
   const deadline = Date.now() + 5000;
   while (!fs.existsSync(config + '/fixture-business.json') && Date.now() < deadline) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
   if (!fs.existsSync(config + '/fixture-business.json')) throw Error('business did not start before injected fault');
   throw Error('injected fallback directory fsync after visible rename and admitted business');
 }
 return sync.call(fs, fd);
};`);
  const env = { ...process.env, ...ownership.env, ...(mode === "fallback-uncertain" ? { NODE_OPTIONS: `${ownership.env.NODE_OPTIONS} --import=${pathToFileURL(fault).href}` } : {}), PI_FABRIC_TEST_TARGET_MODE: mode,
    ...(killableMain ? { PI_FABRIC_TEST_HANDOVER_AFTER_RELEASE_MS: "1500" } : {}) };
  const children: ChildProcess[] = [];
  const startLauncher = (release: string) => {
    const child = spawn(process.execPath, [path.join(release, "dist/residency/launcher.js"), "--config", configPath], { env, stdio: "ignore" });
    children.push(child); return child;
  };
  let closed = false;
  const cleanup = async () => {
    if (closed) return;
    // A killed orphan may be a zombie owned by init; it is exited and cannot
    // be signalled or reaped by this fixture. Only live launch-time identities.
    const live = ownership.owned().filter((owned) => {
      try { const stat = fs.readFileSync(`/proc/${owned.pid}/stat`, "utf8"); return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0] !== "Z"; } catch { return false; }
    });
    await stopAllOwned(live, 2_000, 5_000);
    await Promise.all(children.map((child) => child.exitCode !== null || child.signalCode !== null ? Promise.resolve() : new Promise<void>((resolve) => child.once("close", () => resolve()))));
    fs.rmSync(root, { recursive: true, force: true }); closed = true; cleanups.delete(cleanup);
  };
  cleanups.add(cleanup);
  const launcher = startLauncher(a);
  const ownerFile = path.join(residencyRoot, "owner.json");
  await until(() => !!readHandoverJson<ResidentHostOwner>(ownerFile)?.handover);
  const owner = readHandoverJson<ResidentHostOwner>(ownerFile)!;
  const inode = fs.statSync(path.join(residencyRoot, "host.lock")).ino;
  let mainChild: ChildProcess | undefined;
  if (killableMain) {
    mainChild = spawn(process.execPath, ["-e", "setInterval(()=>{},1000)"], { env, stdio: "ignore" });
    children.push(mainChild);
    await new Promise<void>((resolve, reject) => { mainChild!.once("spawn", resolve); mainChild!.once("error", reject); });
  }
  const main: ResidentMainGeneration = { pid: mainChild?.pid ?? process.pid, processStartTime: processStartTime(mainChild?.pid ?? process.pid)!,
    nonce: randomUUID(), rootId: config.rootId, sessionId: config.sessionId, releaseRoot: b };
  const plan: ResidentHandoverPlan = { id: randomUUID(), rootId: config.rootId, old: { pid: owner.pid, processStartTime: owner.processStartTime!,
    token: owner.token, hostId: owner.hostId }, launcher: owner.handover!.launcher, previous, target, main,
    caller: { identity: { id: config.rootId, name: "Main", kind: "main", sessionId: config.sessionId }, hostId: config.rootId }, createdAt: Date.now() };
  writeHandoverImmutable(mainGenerationPath(residencyRoot), main);
  writeLaunchSnapshot(residencyRoot, previous); writeLaunchSnapshot(residencyRoot, target);
  const state = () => readHandoverJson<ResidentHandoverState>(handoverPath(residencyRoot));
  const service = () => readHandoverJson<{ pid: number; config: ResidentHostConfig; attempt: { id: string; kind: string } }>(path.join(residencyRoot, "fixture-service.json"));
  return { root, residencyRoot, owner, inode, previous, target, plan, mainChild, state, service,
    commit: () => writeHandoverState(residencyRoot, plan, "custody"),
    competingLauncher: () => startLauncher(b),
    close: cleanup, launcher,
  };
}

describe.skipIf(!available)("native launcher release custody and bounded recovery", () => {
  it("blocks fallback when a fast-exiting B leaves an unobserved detached helper", async () => {
    const f = await fixture("fast-exit");
    try {
      f.commit();
      await until(() => ["blocked", "fallback"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase).toBe("blocked");
      expect(f.state()?.error).toMatch(/membership|unproven/i);
      expect(logs(f.residencyRoot).filter(e => e.event === "child-spawned" && e.kind === "fallback")).toHaveLength(0);
      const escaped = readHandoverJson<{ pid: number; birth: string }>(path.join(f.residencyRoot, "fixture-escaped.json"))!;
      expect(residentProcessAlive(escaped.pid, escaped.birth)).toBe(true);
    } finally { await f.close(); }
  }, 30_000);

  it("retains A and admitted business after fallback rename succeeds but directory fsync fails", async () => {
    const f = await fixture("fallback-uncertain", true);
    try {
      f.commit();
      await until(() => f.state()?.phase === "released" && !residentProcessAlive(f.owner.pid, f.owner.processStartTime));
      const mainExited = new Promise<void>(resolve => f.mainChild!.once("close", () => resolve()));
      f.mainChild!.kill("SIGKILL"); await mainExited;
      await until(() => logs(f.residencyRoot).some(e => e.event === "handover-terminal-uncertain") || f.state()?.phase === "blocked");
      expect(f.state()?.phase).toBe("fallback");
      const a = readHandoverJson<ResidentHostOwner>(path.join(f.residencyRoot, "owner.json"))!;
      const business = readHandoverJson<{ pid: number; birth: string; ticks: number }>(path.join(f.residencyRoot, "fixture-business.json"))!;
      expect(residentProcessAlive(a.pid, a.processStartTime)).toBe(true);
      expect(residentProcessAlive(business.pid, business.birth)).toBe(true);
      await until(() => (readHandoverJson<{ ticks: number }>(path.join(f.residencyRoot, "fixture-business.json"))?.ticks ?? 0) > business.ticks);
      expect(f.launcher.exitCode).toBeNull();
      expect(logs(f.residencyRoot).filter(e => e.event === "child-spawned" && e.kind === "fallback")).toHaveLength(1);
      expect(logs(f.residencyRoot).filter(e => e.event === "child-spawned" && e.kind === "target")).toHaveLength(0);
    } finally { await f.close(); }
  }, 30_000);


  it("keeps owned B alive when terminal publication is indeterminate instead of cutting possible work", async () => {
    const f = await fixture("terminal-block");
    try {
      f.commit();
      await until(() => logs(f.residencyRoot).some(e => e.event === "handover-terminal-uncertain"));
      const b = readHandoverJson<ResidentHostOwner>(path.join(f.residencyRoot, "owner.json"))!;
      expect(b.releaseRoot).toBe(f.target.releaseRoot);
      expect(residentProcessAlive(b.pid, b.processStartTime)).toBe(true);
      expect(f.launcher.exitCode).toBeNull();
      expect(logs(f.residencyRoot).filter(e => e.event === "child-spawned" && e.kind === "target")).toHaveLength(1);
      expect(logs(f.residencyRoot).filter(e => e.event === "child-spawned" && e.kind === "fallback")).toHaveLength(0);
    } finally { await f.close(); }
  }, 30_000);

  it("takes exact A/B custody before A exits, and commits one B attempt without a competing launcher", async () => {
    const f = await fixture("ready");
    try {
      f.commit();
      await until(() => !!readHandoverJson(handoverCustodyPath(f.residencyRoot, f.plan.id)));
      const rival = f.competingLauncher();
      await new Promise<void>((resolve) => rival.once("close", () => resolve()));
      await until(() => f.state()?.phase === "complete" && !!f.service());
      const seen = readHandoverJson<{ owner: ResidentHostOwner; receipt: { id: string } }>(path.join(f.residencyRoot, "fixture-custody-seen.json"))!;
      expect(seen.owner.token).toBe(f.owner.token);
      expect(seen.receipt.id).toBe(f.plan.id);
      expect(residentProcessAlive(f.owner.pid, f.owner.processStartTime)).toBe(false);
      expect(f.service()!.config).toEqual(f.target.config);
      expect(logs(f.residencyRoot).filter((e) => e.event === "child-spawned" && e.kind === "target")).toHaveLength(1);
      expect(logs(f.residencyRoot).filter((e) => e.event === "child-spawned" && e.kind === "fallback")).toHaveLength(0);
      expect(fs.statSync(path.join(f.residencyRoot, "host.lock")).ino).toBe(f.inode);
    } finally { await f.close(); }
  }, 30_000);

  it("blocks failed B without fallback even when desired config is overwritten", async () => {
    const f = await fixture("exit");
    try {
      fs.writeFileSync(path.join(f.residencyRoot, "config.json"), JSON.stringify({ ...f.target.config, kernel: "python", workerPath: "/unrelated/C/worker.js", piModels: { defaultModel: "fixture/C" } }));
      f.commit();
      await until(() => f.state()?.phase === "blocked");
      expect(f.service()).toBeUndefined();
      expect(f.state()?.error).toMatch(/membership.*unproven/);
      const traces = logs(f.residencyRoot);
      expect(traces.filter((e) => e.event === "child-spawned" && e.kind === "target")).toHaveLength(1);
      expect(traces.filter((e) => e.event === "child-spawned" && e.kind === "fallback")).toHaveLength(0);
      const targetPid = traces.find((e) => e.event === "child-spawned" && e.kind === "target")!.pid as number;
      expect(residentProcessAlive(targetPid)).toBe(false);
      expect(readHandoverJson(handoverOutcomePath(f.residencyRoot, f.target))).toMatchObject({ id: f.plan.id });
      await delay(200);
      expect(logs(f.residencyRoot).filter((e) => e.event === "child-spawned" && e.kind === "target")).toHaveLength(1);
      expect(fs.statSync(path.join(f.residencyRoot, "host.lock")).ino).toBe(f.inode);
    } finally { await f.close(); }
  }, 30_000);

  it("stops observed hung B processes but blocks fallback without complete membership proof", async () => {
    const f = await fixture("hang");
    try {
      f.commit();
      await until(() => f.state()?.phase === "blocked", 45_000);
      const traces = logs(f.residencyRoot);
      expect(traces.filter((e) => e.event === "child-spawned" && e.kind === "target")).toHaveLength(1);
      expect(traces.filter((e) => e.event === "child-spawned" && e.kind === "fallback")).toHaveLength(0);
      const targetExit = traces.findIndex((e) => e.event === "child-exit" && e.kind === "target");
      expect(targetExit).toBeGreaterThanOrEqual(0);
      expect(f.service()).toBeUndefined();
      expect(f.state()?.error).toMatch(/membership.*unproven/);
      expect(fs.statSync(path.join(f.residencyRoot, "host.lock")).ino).toBe(f.inode);
    } finally { await f.close(); }
  }, 60_000);

  it("launcher restores A if Main dies after A released its fence and before B spawn", async () => {
    const f = await fixture("ready", true);
    try {
      f.commit();
      await until(() => f.state()?.phase === "released" && !residentProcessAlive(f.owner.pid, f.owner.processStartTime));
      const mainExited = new Promise<void>((resolve) => f.mainChild!.once("close", () => resolve()));
      f.mainChild!.kill("SIGKILL");
      await mainExited;
      await until(() => f.state()?.phase === "fallback" && !!f.service());
      expect(f.service()!.config).toEqual(f.previous.config);
      expect(logs(f.residencyRoot).filter((e) => e.event === "child-spawned" && e.kind === "target")).toHaveLength(0);
      expect(logs(f.residencyRoot).filter((e) => e.event === "child-spawned" && e.kind === "fallback")).toHaveLength(1);
    } finally { await f.close(); }
  }, 30_000);
});
