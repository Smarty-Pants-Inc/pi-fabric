import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
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
async function fixture(mode: "ready" | "exit" | "hang" | "fast-exit", killableMain = false) {
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
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, nice: 19 }, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(a, "dist/worker.js"), fabricExtensionPath: path.join(a, "dist/index.js"),
    piBinary: path.resolve("tests/fixtures/handover-pi.mjs"), claudeBinary: "fixture-claude", vedaBinary: "fixture-veda", kernel: "typescript", pythonRuntime: "monty",
    piModels: { available: [{ provider: "fixture", id: "A" }], aliases: {}, defaultModel: "fixture/A" } };
  const previous = residentLaunchSpec(config, path.join(a, "dist/residency/pi-entry.js"));
  const target = residentLaunchSpec({ ...config, workerPath: path.join(b, "dist/worker.js"), fabricExtensionPath: path.join(b, "dist/index.js"),
    kernel: "python", pythonRuntime: "cpython", piModels: { available: [{ provider: "fixture", id: "B" }], aliases: {}, defaultModel: "fixture/B" } }, path.join(b, "dist/residency/pi-entry.js"));
  const configPath = path.join(residencyRoot, "config.json"); fs.writeFileSync(configPath, JSON.stringify(previous.config));
  const env = { ...process.env, ...ownership.env, PI_FABRIC_TEST_TARGET_MODE: mode };
  const children: ChildProcess[] = [];
  const startLauncher = (release: string) => {
    const child = spawn(process.execPath, [path.join(release, "dist/residency/launcher.js"), "--config", configPath], { env, stdio: "ignore" });
    children.push(child); return child;
  };
  let closed = false;
  const cleanup = async () => {
    if (closed) return;
    // Stop/reap every direct controller before snapshotting the launch log.
    // Otherwise a failing assertion at `starting` can race a just-spawned B's
    // preload/helper registration, leaving it outside the cleanup snapshot.
    await Promise.all(children.map(async child => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const exited = new Promise<void>(resolve => child.once("close", () => resolve()));
      child.kill("SIGTERM"); await exited;
    }));
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
  return { root, residencyRoot, owner, inode, previous, target, plan, mainChild, state,
    commit: () => writeHandoverState(residencyRoot, plan, "custody"),
    close: cleanup, launcher,
  };
}

// Automatic release is currently unavailable: no attempt-owned containment
// exists. Do not retain the old tests that required unsafe B attempts/outages.
describe.skipIf(!available)("native launcher pre-exit release deferral", () => {
  it.each(["ready", "exit", "hang", "fast-exit"] as const)("refuses %s B before custody and keeps the exact A generation alive", async mode => {
    const f = await fixture(mode);
    try {
      f.commit();
      await until(() => ["cancelled", "starting", "complete", "blocked"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase).toBe("cancelled");
      expect(f.state()?.error).toMatch(/attempt.*exit|recovery.*unavailable/i);
      expect(residentProcessAlive(f.owner.pid, f.owner.processStartTime)).toBe(true);
      expect(readHandoverJson<ResidentHostOwner>(path.join(f.residencyRoot, "owner.json"))?.token).toBe(f.owner.token);
      expect(fs.existsSync(handoverCustodyPath(f.residencyRoot, f.plan.id))).toBe(false);
      expect(fs.existsSync(handoverOutcomePath(f.residencyRoot, f.target))).toBe(false);
      expect(fs.existsSync(path.join(f.residencyRoot, "fixture-escaped.json"))).toBe(false);
      expect(logs(f.residencyRoot).filter(e => e.event === "child-spawned" && (e.kind === "target" || e.kind === "fallback"))).toHaveLength(0);
      expect(fs.statSync(path.join(f.residencyRoot, "host.lock")).ino).toBe(f.inode);
      expect(f.launcher.exitCode).toBeNull();
    } finally { await f.close(); }
  }, 25_000);

  it("keeps A supervised when Main dies during a deferred transaction", async () => {
    const f = await fixture("ready", true);
    try {
      f.commit();
      await until(() => ["cancelled", "released"].includes(f.state()?.phase ?? ""));
      expect(f.state()?.phase).toBe("cancelled");
      const exited = new Promise<void>(resolve => f.mainChild!.once("close", () => resolve()));
      f.mainChild!.kill("SIGKILL"); await exited;
      await delay(150);
      expect(residentProcessAlive(f.owner.pid, f.owner.processStartTime)).toBe(true);
      expect(f.launcher.exitCode).toBeNull();
      expect(logs(f.residencyRoot).filter(e => e.event === "child-spawned" && (e.kind === "target" || e.kind === "fallback"))).toHaveLength(0);
      expect(fs.existsSync(handoverCustodyPath(f.residencyRoot, f.plan.id))).toBe(false);
    } finally { await f.close(); }
  }, 25_000);
});
