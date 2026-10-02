import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { FabricShellJobStore, raceShellHang, trackShellOperations } from "../src/core/shell-jobs.js";
import { SCRATCH_OWNER_FILE, sweepScratch } from "../src/storage/scratch.js";

const roots: string[] = [];
const stores: FabricShellJobStore[] = [];
const store = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "shell-cleanup-security-"));
  roots.push(root);
  const jobs = new FabricShellJobStore(root);
  stores.push(jobs);
  return { root, jobs };
};
afterEach(async () => {
  await Promise.all(stores.splice(0).map(jobs => jobs.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("review F1: shell cleanup requires confirmed exit", () => {
  it.each([false, true])("retains an uncached live on-disk PID at shutdown (spilled=%s)", async spilled => {
    const { jobs } = store();
    const job = jobs.begin("bash", "uncached live child");
    fs.writeFileSync(job.pidPath, String(process.pid));
    expect(job.pid).toBeUndefined();
    if (spilled) { job.spill(); await job.persistLog(); }
    await jobs.close();
    expect(fs.readFileSync(job.pidPath, "utf8")).toBe(String(process.pid));
    if (spilled) expect(fs.existsSync(job.logPath!)).toBe(true);
  });

  it.each([false, true])("retains a delayed launch after cancellation until the real operation exits (spilled=%s)", async spilled => {
    const { root, jobs } = store();
    const job = jobs.begin("bash", "delayed launch");
    const directory = path.dirname(job.pidPath);
    let resolve!: (value: { exitCode: number | null }) => void;
    const operation = new Promise<{ exitCode: number | null }>(done => { resolve = done; });
    const tracked = trackShellOperations({ exec: () => operation }, job, "bash");
    const execution = raceShellHang({
      job, hangMs: 0, parentSignal: undefined,
      execute: signal => tracked.exec("delayed launch", root, { signal, onData: () => {} }),
    });
    // Exercise the timeout window too: PID acquisition has already given up.
    await expect(job.readPid()).resolves.toBeUndefined();
    if (spilled) { job.spill(); await job.persistLog(); }
    const close = jobs.close();
    try {
      await close; // Retention, rather than an unbounded shutdown drain, is allowed.
      expect(job.abort.signal.aborted).toBe(true);
      expect(fs.existsSync(directory)).toBe(true);
      expect(JSON.parse(fs.readFileSync(path.join(directory, SCRATCH_OWNER_FILE), "utf8")).closedAt).toBeUndefined();
      fs.writeFileSync(job.pidPath, String(process.pid)); // launch after cancellation
      expect((await sweepScratch({ tempRoot: root, maxAgeMs: 0, recentMs: 0, maxItems: 0 })).removed).toEqual([]);
      expect(fs.existsSync(directory)).toBe(true);
      if (spilled) expect(fs.existsSync(job.logPath!)).toBe(true);
    } finally {
      // Confirm the real operation exited, even on the old-head assertion failure.
      if (fs.existsSync(directory)) fs.writeFileSync(job.pidPath, "2147483647");
      resolve({ exitCode: 0 });
      await execution;
      await close;
    }
    await sweepScratch({ tempRoot: root, released: new Set([directory]) });
    expect(fs.existsSync(directory)).toBe(false);
  });

  it("does not treat an aborted operation rejection without a PID as confirmed exit", async () => {
    const { root, jobs } = store();
    const job = jobs.begin("bash", "abort before PID");
    const tracked = trackShellOperations({ exec: async () => { throw new Error("abort acknowledged, exit unknown"); } }, job, "bash");
    await expect(raceShellHang({ job, hangMs: 0, parentSignal: undefined,
      execute: signal => tracked.exec("abort", root, { signal, onData: () => {} }),
    })).resolves.toMatchObject({ status: "error" });
    job.spill();
    await job.persistLog();
    await jobs.close();
    expect(fs.existsSync(path.join(path.dirname(job.pidPath), "operation.pending"))).toBe(true);
    expect((await sweepScratch({ tempRoot: root, maxAgeMs: 0, recentMs: 0, maxItems: 0 })).removed).toEqual([]);
    // Durable uncertainty also vetoes the abandoned-owner sweep.
    const ownerPath = path.join(path.dirname(job.pidPath), SCRATCH_OWNER_FILE);
    const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
    fs.writeFileSync(ownerPath, JSON.stringify({ ...owner, pid: 2147483647, orphanedAt: 1 }));
    expect((await sweepScratch({ tempRoot: root, now: Date.now(), orphanGraceMs: 0, maxAgeMs: 0 })).removed).toEqual([]);
    expect(fs.existsSync(job.logPath!)).toBe(true);
  });
});
