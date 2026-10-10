import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import { readWakeJson } from "../src/residency/wake.js";
import { processStartTime, residentProcessAlive } from "../src/residency/process-identity.js";

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const until = async (done: () => boolean, ms = 45_000) => {
  const start = performance.now();
  while (!done() && performance.now() - start < ms) await sleep(50);
  expect(done()).toBe(true);
};
const sample = (pid: number) => {
  const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  const fields = stat.slice(stat.lastIndexOf(")") + 2).split(/\s+/);
  const status = fs.readFileSync(`/proc/${pid}/status`, "utf8");
  return { ticks: Number(fields[11]) + Number(fields[12]), rssKiB: Number(status.match(/^VmRSS:\s+(\d+)/m)?.[1] ?? 0) };
};

// Exercises real native exit + compiled launcher + production ResidentHost; the Pi/model
// transport is an explicitly fake executable so the test never spends credentials/inference.
it.skipIf(process.platform !== "linux")("native dormant host exits, is woken by committed deliveries, drains FIFO once, and releases CPU/RSS", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dormant-native-"));
  const bun = execFileSync("bun", ["-e", "console.log(process.execPath)"], { encoding: "utf8" }).trim();
  const pi = path.join(root, "fake-pi");
  const quote = (text: string) => "'" + text.replaceAll("'", "'\\''") + "'";
  fs.writeFileSync(pi, `#!/bin/sh\nexec ${quote(bun)} ${quote(path.resolve("tests/helpers/dormant-resident.ts"))} "$@"\n`, { mode: 0o700 });
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:dormant-native", sessionId: "dormant-native", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:dormant-native"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention }, workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: pi, claudeBinary: pi, vedaBinary: pi, watchdog: { enabled: false },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const configPath = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  const owned = new Map<number, string | undefined>();
  let first: ReturnType<typeof spawn> | undefined;
  let firstExit: Promise<number | null> | undefined;
  let mesh: MeshStore | undefined;
  const ownerPath = path.join(config.residencyRoot, "owner.json");
  const owner = () => readWakeJson<ResidentHostOwner>(ownerPath);
  const launcherPids = () => {
    try { return fs.readFileSync(path.join(config.residencyRoot, "launcher.log"), "utf8").split("\n").flatMap(line => {
      try { const row = JSON.parse(line); return row.event === "launcher-started" ? [row] : []; } catch { return []; }
    }) as Array<{ pid: number; processStartTime?: string }>; } catch { return []; }
  };
  try {
    first = spawn(process.execPath, [path.resolve("dist/residency/launcher.js"), "--config", configPath], { stdio: "ignore" });
    owned.set(first.pid!, processStartTime(first.pid!));
    firstExit = new Promise(resolve => first!.once("exit", code => resolve(code)));
    await until(() => !!owner() || first!.exitCode !== null);
    if (!owner()) {
      const diagnostics = ["launcher.log", "child-stderr.log", "error.json"].map(file => {
        try { return `${file}: ${fs.readFileSync(path.join(config.residencyRoot, file), "utf8")}`; } catch { return file + ": absent"; }
      }).join("\n");
      throw new Error(`Native host failed to start (${first.exitCode})\n${diagnostics}`);
    }
    const warm = owner()!;
    owned.set(warm.pid, warm.processStartTime);
    await sleep(3_000); // settle startup/registry publication before sampling warm idle CPU
    const before = sample(warm.pid);
    const launcherBefore = sample(first.pid!);
    const t0 = performance.now();
    await sleep(3_000);
    const after = sample(warm.pid);
    const launcherAfter = sample(first.pid!);
    const clockTicks = Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8" }).trim());
    const seconds = (performance.now() - t0) / 1_000;
    await until(() => !residentProcessAlive(warm.pid, warm.processStartTime));
    expect(await firstExit).toBe(0);
    expect(owner()).toBeUndefined();
    const report = { transport: "fake Pi/model; real ResidentHost and compiled launcher", sampleSeconds: seconds,
      before: { hostRssKiB: after.rssKiB, launcherRssKiB: launcherAfter.rssKiB,
        hostCpuPercent: (after.ticks - before.ticks) / clockTicks / seconds * 100,
        launcherCpuPercent: (launcherAfter.ticks - launcherBefore.ticks) / clockTicks / seconds * 100 },
      after: { hostAlive: false, launcherAlive: false, hostRssKiB: 0, launcherRssKiB: 0, cpuPercent: 0 } };
    console.log("IDLE_MEASURE", JSON.stringify(report));
    if (process.env.TASK_OUT) fs.writeFileSync(path.join(process.env.TASK_OUT, "native-idle-measure.json"), JSON.stringify(report, null, 2) + "\n");

    mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    await mesh.publish({ topic: "native.wake", from: { id: "publisher", kind: "main", name: "publisher" }, data: { n: 1 } });
    let pending = [2, 3].map(n => ({ topic: "native.wake", from: { id: "publisher", kind: "main" as const, name: "publisher" }, data: { n } }));
    // publishBatch commits a bounded prefix, not necessarily its entire input
    // (50 ms/fsync budget). Retry only its uncommitted suffix before measuring FIFO.
    while (pending.length) {
      const committed = await mesh.publishBatch(pending);
      expect(committed.length).toBeGreaterThan(0);
      expect(committed.length).toBeLessThanOrEqual(pending.length);
      pending = pending.slice(committed.length);
    }
    await until(() => !!owner() && owner()!.pid !== warm.pid);
    const woken = owner()!;
    owned.set(woken.pid, woken.processStartTime);
    for (const row of launcherPids()) owned.set(row.pid, row.processStartTime);
    const processed = () => {
      try { return fs.readFileSync(path.join(root, "processed.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line).task as string); }
      catch { return []; }
    };
    try {
      await until(() => processed().length >= 3 || owner()?.pid !== woken.pid);
      expect(processed()).toHaveLength(3);
    }
    catch (error) {
      const registry = readWakeJson<unknown>(path.join(config.actorRoot, "actors.json"));
      throw new Error(`${String(error)}; processed=${JSON.stringify(processed())}; registry=${JSON.stringify(registry)}`);
    }
    expect(processed().map(task => Number(task.match(/"n"\s*:\s*(\d+)/)?.[1]))).toEqual([1, 2, 3]);
    await until(() => !residentProcessAlive(woken.pid, woken.processStartTime));
    await until(() => launcherPids().every(row => !residentProcessAlive(row.pid, row.processStartTime)));
    expect(processed()).toHaveLength(3);
    expect(owner()).toBeUndefined();
    expect(fs.existsSync(configPath)).toBe(true);
    expect(new (await import("../src/actors/registry-store.js")).ActorRegistryStore(config.actorRoot).records().find(row => row.name === "native-listener")?.status).toBe("dormant");
    console.log("NATIVE_WAKE", JSON.stringify({ processed: [1, 2, 3], exactlyOnce: true, hostExited: true, allLaunchersExited: true }));
  } finally {
    for (const row of launcherPids()) owned.set(row.pid, row.processStartTime);
    const current = owner(); if (current) owned.set(current.pid, current.processStartTime);
    for (const [pid, birth] of owned) if (residentProcessAlive(pid, birth)) { try { process.kill(pid, "SIGTERM"); } catch { /* exited */ } }
    await until(() => [...owned].every(([pid, birth]) => !residentProcessAlive(pid, birth)), 20_000);
    if (firstExit) await firstExit;
    mesh?.closeState();
    if (process.env.TASK_OUT) {
      const output = path.join(process.env.TASK_OUT, "native-process-evidence");
      fs.mkdirSync(output, { recursive: true });
      for (const [source, name] of [[path.join(root, "processed.jsonl"), "processed.jsonl"],
        [path.join(config.actorRoot, "actors.json"), "actors.json"],
        [path.join(config.residencyRoot, "actor-mesh-cursor.json.project"), "cursor.project.json"],
        [path.join(config.residencyRoot, "actor-mesh-cursor.json.session"), "cursor.session.json"]]) {
        try { fs.copyFileSync(source!, path.join(output, name!)); } catch { fs.rmSync(path.join(output, name!), { force: true }); }
      }
      for (const file of ["launcher.log", "child-stderr.log", "error.json", "sleeping.json", "wake-request.json"]) {
        try { fs.copyFileSync(path.join(config.residencyRoot, file), path.join(output, file)); }
        catch { fs.rmSync(path.join(output, file), { force: true }); } // no stale failed-run evidence
      }
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 120_000);
