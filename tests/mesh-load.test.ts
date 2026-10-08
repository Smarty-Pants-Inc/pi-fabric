import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

const script = fileURLToPath(new URL("../scripts/mesh-load.ts", import.meta.url));
const roots: string[] = [];
const running: ChildProcessWithoutNullStreams[] = [];
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-load-"));
  roots.push(root);
  return root;
};
afterEach(async () => {
  // Stop leftovers through stdin, never a signal, so this also holds on Windows.
  await Promise.all(running.splice(0).map(child => child.exitCode !== null ? undefined : new Promise<void>(resolve => {
    child.once("exit", () => resolve());
    child.stdin.end("stop\n");
  })));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const start = (args: string[], env: NodeJS.ProcessEnv = {}) => {
  const child = spawn("bun", [script, ...args], { stdio: ["pipe", "pipe", "pipe"], env: { ...process.env, ...env } });
  running.push(child);
  const out = { stdout: "", stderr: "" };
  child.stdout.setEncoding("utf8").on("data", chunk => { out.stdout += chunk; });
  child.stderr.setEncoding("utf8").on("data", chunk => { out.stderr += chunk; });
  const exit = new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", code => resolve(code));
  });
  return { child, out, exit };
};
const runId = async (out: { stderr: string }) => {
  for (let i = 0; i < 300 && !/run ([0-9a-f]{8}) /.test(out.stderr); i++) await new Promise(resolve => setTimeout(resolve, 50));
  const id = /run ([0-9a-f]{8}) /.exec(out.stderr)?.[1];
  expect(id, out.stderr).toMatch(/^[0-9a-f]{8}$/);
  return id!;
};
const keysOf = (root: string, run: string) => new MeshStore(root, 64 * 1024, 100).listAll()
  .filter(entry => JSON.stringify(entry).includes(`mesh-load-${run}-`)).map(entry => entry.key).sort();
const synthetic = (root: string) => new MeshStore(root, 64 * 1024, 100).listAll()
  .filter(entry => JSON.stringify(entry).includes("mesh-load-")).map(entry => entry.key);
const waitFor = async (check: () => boolean, ms = 20_000) => {
  const deadline = Date.now() + ms;
  while (!check() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 100));
  return check();
};

describe("mesh-load", () => {
  it("drives real mesh writes and cleans synthetic presence on termination", async () => {
    const root = tempRoot();
    const { out, exit } = start(["--root", root, "--target-writes-per-min", "1200", "--target-processes", "2", "--duration", "10", "--profile", "fleet"]);
    const exitCode = await exit;
    expect(exitCode, out.stderr).toBe(0);
    const reports = out.stdout.split("\n").filter(Boolean).map(line => JSON.parse(line) as { workers: number; writesPerMin: number; busyPct: number; timeouts: number });
    expect(reports.length).toBeGreaterThan(0);
    expect(reports.some(report => report.workers >= 2)).toBe(true);
    expect(reports.at(-1)!.writesPerMin).toBeGreaterThanOrEqual(960);
    expect(synthetic(root)).toEqual([]);
  }, 30_000);

  it("isolates concurrent runs on one root: stopping one leaves the other's keys", async () => {
    const root = tempRoot();
    const a = start(["--root", root, "--target-writes-per-min", "60", "--target-processes", "1", "--duration", "0"]);
    const b = start(["--root", root, "--target-writes-per-min", "60", "--target-processes", "1", "--duration", "0"]);
    const [runA, runB] = [await runId(a.out), await runId(b.out)];
    expect(runA).not.toBe(runB);
    expect(await waitFor(() => keysOf(root, runA).length === 2 && keysOf(root, runB).length === 2)).toBe(true);
    const keysB = keysOf(root, runB);
    expect(keysB.some(key => key.startsWith(`actors/mesh-load-${runB}-1/`))).toBe(true);
    a.child.stdin.write("stop\n");
    expect(await a.exit, a.out.stderr).toBe(0);
    expect(keysOf(root, runA)).toEqual([]);
    expect(keysOf(root, runB)).toEqual(keysB);
    b.child.stdin.write("stop\n");
    expect(await b.exit, b.out.stderr).toBe(0);
    expect(synthetic(root)).toEqual([]);
  }, 60_000);

  it("seeds hub-shaped state, mixes keyed puts and custody ops, caps in-flight writes and keeps the seed", async () => {
    const root = tempRoot();
    // 60000/min is a 1 ms pace; two workers contend for the lock, so writes wait and the cap must skip.
    const { out, exit } = start(["--root", root, "--target-writes-per-min", "60000", "--target-processes", "2", "--max-workers", "2",
      "--seed-state-mb", "0.3", "--put-share", "0.4", "--custody-share", "0.4", "--duration", "6", "--control-interval", "1"], { PI_FABRIC_LOCK_STATS: "1" });
    expect(await exit, out.stderr).toBe(0);
    expect(out.stderr).toMatch(/seeded \d+ records; state\.json is \d+ bytes/);
    const final = JSON.parse(out.stdout.trim().split("\n").at(-1)!) as { final: boolean; workers: number; skipped: number; maxInFlight: number; writesPerMin: number };
    expect(final).toMatchObject({ final: true, workers: 2, maxInFlight: 1 });
    expect(final.skipped).toBeGreaterThan(0);
    expect(final.writesPerMin).toBeGreaterThan(0);
    expect(fs.statSync(path.join(root, "state.json")).size).toBeGreaterThanOrEqual(300_000);
    // L8 records all three paced kinds plus heartbeats: keyed puts and heartbeats are writeBatch, custody ops custody.
    const stats = fs.readdirSync(path.join(root, "lock-stats")).map(file => fs.readFileSync(path.join(root, "lock-stats", file), "utf8")).join("");
    for (const lockClass of ["writeBatch", "custody", "publish"]) expect(stats).toContain(`"${lockClass}"`);
    const store = new MeshStore(root, 64 * 1024, 100);
    const seeds = store.listAll().filter(entry => entry.key.startsWith("fleet/actors/load-seed-a")).length;
    expect(seeds).toBeGreaterThan(0);
    // Run keys (presence, actor, keyed puts) are gone; the seed stays, and a second seed is a no-op.
    expect(synthetic(root)).toEqual([]);
    const again = start(["--root", root, "--target-writes-per-min", "60", "--target-processes", "1", "--seed-state-mb", "0.3", "--duration", "1"]);
    expect(await again.exit, again.out.stderr).toBe(0);
    expect(again.out.stderr).toContain("seeded 0 records");
    expect(new MeshStore(root, 64 * 1024, 100).listAll().filter(entry => entry.key.startsWith("fleet/actors/load-seed-a")).length).toBe(seeds);
  }, 60_000);

  it("does not add workers once the target rate is met", async () => {
    const root = tempRoot();
    // Several 2 s control windows at an easy target: the old controller added a worker on each check.
    const { out, exit } = start(["--root", root, "--target-writes-per-min", "600", "--target-processes", "1", "--duration", "9", "--control-interval", "2"]);
    expect(await exit, out.stderr).toBe(0);
    const final = JSON.parse(out.stdout.trim().split("\n").at(-1)!) as { workers: number; maxInFlight: number };
    expect(final.workers).toBe(1);
    expect(final.maxInFlight).toBe(1);
    expect(synthetic(root)).toEqual([]);
  }, 30_000);

  it("rejects an invalid --duration and an unreachable --target-writes-per-min", () => {
    const root = tempRoot();
    const run = (...extra: string[]) => spawnSync("bun", [script, "--root", root, "--target-processes", "1", "--dry-run", ...extra], { encoding: "utf8" });
    for (const duration of ["-1", "abc"]) {
      const result = run("--target-writes-per-min", "60", "--duration", duration);
      expect(result.status, result.stderr).toBe(2);
      expect(result.stderr).toContain("--duration must be a number of seconds >= 0");
    }
    for (const target of ["5", "0"]) {
      const result = run("--target-writes-per-min", target);
      expect(result.status, result.stderr).toBe(2);
      expect(result.stderr).toContain("--target-writes-per-min must be at least 12");
    }
    const unbounded = run("--target-writes-per-min", "12", "--duration", "0");
    expect(unbounded.status, unbounded.stderr).toBe(0);
    expect(JSON.parse(unbounded.stdout)).toMatchObject({ dryRun: true, durationSec: 0, workerCap: 1, seedStateMb: 0, putShare: 0, custodyShare: 0, maxInFlight: 1, controlIntervalSec: 30 });
    for (const [flag, value, message] of [["--seed-state-mb", "31", "--seed-state-mb must be"], ["--seed-state-mb", "-1", "--seed-state-mb must be"],
      ["--put-share", "1.5", "--put-share must be"], ["--custody-share", "-0.1", "--custody-share must be"], ["--max-in-flight", "0", "--max-in-flight must be"], ["--control-interval", "0", "--control-interval must be"]] as const) {
      const result = run("--target-writes-per-min", "60", flag, value);
      expect(result.status, result.stderr).toBe(2);
      expect(result.stderr).toContain(message);
    }
    const help = spawnSync("bun", [script, "--help"], { encoding: "utf8" });
    expect(help.status).toBe(0);
    expect(help.stdout).toContain("0 (default) is explicit unbounded mode");
    expect(synthetic(root)).toEqual([]);
  }, 30_000);
});
