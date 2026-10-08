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

const start = (args: string[]) => {
  const child = spawn("bun", [script, ...args], { stdio: ["pipe", "pipe", "pipe"] });
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
    expect(JSON.parse(unbounded.stdout)).toMatchObject({ dryRun: true, durationSec: 0, workerCap: 1 });
    const help = spawnSync("bun", [script, "--help"], { encoding: "utf8" });
    expect(help.status).toBe(0);
    expect(help.stdout).toContain("0 (default) is explicit unbounded mode");
    expect(synthetic(root)).toEqual([]);
  }, 30_000);
});
