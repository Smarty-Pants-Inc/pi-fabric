import { type ChildProcess, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { lockFile } from "../src/records/server.js";
import { same, startTime } from "./helpers/owned-processes.js";

const linux = process.platform === "linux";
const dirs: string[] = [];
const fds: number[] = [];
const children: ChildProcess[] = [];
afterEach(() => {
  // Only processes this file spawned, by their own handles (a no-op once reaped).
  for (const child of children.splice(0)) if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  for (const fd of fds.splice(0)) fs.closeSync(fd);
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const tempLock = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "records-lock-"));
  dirs.push(dir);
  return path.join(dir, "out.lock");
};

// Diagnostic only (#160 S3): flock processes whose fd 3 is this lock file. Never authorizes a signal:
// any process may open the same file, so a match is not ownership.
const flockWaiters = (file: string): number[] => fs.readdirSync("/proc").filter((pid) => /^\d+$/.test(pid)).flatMap((pid) => {
  try {
    const argv = fs.readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0");
    if (!argv.some((arg) => path.basename(arg) === "flock")) return [];
    return fs.readlinkSync(`/proc/${pid}/fd/3`) === file ? [Number(pid)] : [];
  } catch { return []; }
});

const waitFor = async (check: () => boolean, ms: number) => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (check()) return true; await new Promise((r) => setTimeout(r, 50)); }
  return check();
};

type Launched = { child: ChildProcess; pid: number; started: string };
// Identity retained at launch from this test's own handle: pid plus /proc start time.
const launch = (argv: string[], stdio: Parameters<typeof spawn>[2]["stdio"] = "ignore"): Launched => {
  const child = spawn(argv[0]!, argv.slice(1), { stdio });
  children.push(child);
  const pid = child.pid!;
  return { child, pid, started: startTime(pid) };
};
const exited = (child: ChildProcess) => new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
  if (child.exitCode !== null || child.signalCode !== null) resolve({ code: child.exitCode, signal: child.signalCode });
  else child.once("exit", (code, signal) => resolve({ code, signal }));
});

/**
 * Fixture cleanup: SIGKILL the issuer only while its handle is live and its pid still has the
 * start time recorded at launch; then release the lock (close the holder fd), so any flock waiter
 * acquires it and exits on its own within the bound. Signals nothing else. Returns the waiters
 * still present after the bound (diagnostic; the caller fails on them).
 */
const releaseFixture = async (issuer: Launched, holder: number, file: string, ms = 2_000): Promise<number[]> => {
  if (issuer.child.exitCode === null && issuer.child.signalCode === null && same(issuer)) issuer.child.kill("SIGKILL");
  const index = fds.indexOf(holder);
  if (index >= 0) { fds.splice(index, 1); fs.closeSync(holder); }
  await waitFor(() => flockWaiters(file).length === 0, ms);
  return flockWaiters(file);
};

describe.skipIf(!linux)("records lockFile (#1720)", () => {
  it("a SIGKILLed issuer leaves no flock waiter behind", async () => {
    const file = tempLock();
    const holder = await lockFile(file);
    fds.push(holder);
    const server = path.resolve("src/records/server.ts");
    // A short wait deadline also bounds the helper if the parent-death binding ever fails.
    const issuer = launch(["bun", "-e", `const { lockFile } = await import(${JSON.stringify(server)}); await lockFile(${JSON.stringify(file)}, 30);`]);
    try {
      expect(await waitFor(() => flockWaiters(file).length > 0, 10_000)).toBe(true);
      issuer.child.kill("SIGKILL");
      // Still holding the lock: only the parent-death binding can end the helper here.
      expect(await waitFor(() => flockWaiters(file).length === 0, 2_000)).toBe(true);
    } finally {
      expect(await releaseFixture(issuer, holder, file)).toEqual([]);
    }
  }, 20_000);

  it("cleanup never signals an unrelated matching flock or a stale identity", async () => {
    const file = tempLock();
    const holder = await lockFile(file);
    fds.push(holder);
    // Unrelated: matches the scan (flock, fd 3 on this file) but is not the fixture's issuer.
    const own = fs.openSync(file, "r+");
    fds.push(own);
    const unrelated = launch(["flock", "-x", "-w", "60", "3"], ["ignore", "ignore", "ignore", own]);
    const unrelatedExit = exited(unrelated.child);
    expect(await waitFor(() => flockWaiters(file).includes(unrelated.pid), 10_000)).toBe(true);
    // Stale: a live pid whose start time differs from the recorded one.
    const sleeper = launch(["sleep", "30"]);
    const stale: Launched = { ...sleeper, started: `${sleeper.started}0` };
    expect(same(stale)).toBe(false);

    expect(await releaseFixture(stale, holder, file, 10_000)).toEqual([]);
    // The unrelated waiter got the released lock and exited by itself, not by a signal.
    expect(await unrelatedExit).toEqual({ code: 0, signal: null });
    expect(sleeper.child.exitCode).toBeNull();
    expect(sleeper.child.signalCode).toBeNull();
    expect(same(sleeper)).toBe(true);
  }, 20_000);

  it("times out a wait with a clear error", async () => {
    const file = tempLock();
    fds.push(await lockFile(file));
    await expect(lockFile(file, 0.3)).rejects.toThrow(/timed out after 0.3s waiting for lock/);
  });
});
