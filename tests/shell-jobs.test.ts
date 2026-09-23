import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  appendShellHangNotice,
  FabricShellJobStore,
  SHELL_TAIL_BYTES, SHELL_LOG_BYTES, SHELL_COMPLETED_HANDLES,
  formatShellHangNotice,
  parseShellPid,
  raceShellHang,
  wrapShellCommandForPid,
} from "../src/core/shell-jobs.js";

const stores: FabricShellJobStore[] = [];
const store = (): FabricShellJobStore => {
  const jobs = new FabricShellJobStore();
  stores.push(jobs);
  return jobs;
};

afterEach(async () => {
  await Promise.all(stores.splice(0).map((jobs) => jobs.close()));
});

describe("shell hang helpers", () => {
  it("wraps bash so the child writes its pid without extra stdout", () => {
    const wrapped = wrapShellCommandForPid("echo hi", "/tmp/job.pid", "bash");
    expect(wrapped).toContain("/tmp/job.pid");
    expect(wrapped).toContain("/proc/$$/winpid");
    expect(wrapped.endsWith("echo hi")).toBe(true);
    expect(wrapped.startsWith("printf ")).toBe(true);
  });

  it("formats a still-running notice with pid and live path", () => {
    expect(formatShellHangNotice({
      elapsedMs: 47_400,
      pid: 41291,
      logPath: "/tmp/pi-fabric-shell-x/output.log",
    })).toBe("[Still running after 47s (pid 41291). Bounded live output (may be truncated): /tmp/pi-fabric-shell-x/output.log]");
  });

  it("appends the notice like an output-budget spill", () => {
    expect(appendShellHangNotice("hello", "[note]")).toBe("hello\n\n[note]");
    expect(appendShellHangNotice("", "[note]")).toBe("[note]");
  });

  it("parses pid files", () => {
    expect(parseShellPid("41291\n")).toBe(41291);
    expect(parseShellPid("nope")).toBeUndefined();
  });
});

describe("bounded shell lifecycle", () => {
  it("keeps only a copied tail, discloses truncation, and releases it at finish", async () => {
    const job = store().begin("bash", "loud");
    job.append(Buffer.alloc(SHELL_TAIL_BYTES * 3, 97));
    job.append(Buffer.from("END"));
    const snapshot = job.snapshotText(SHELL_TAIL_BYTES * 10);
    expect(snapshot).toContain("Output truncated");
    expect(snapshot.endsWith("END")).toBe(true);
    expect(snapshot.length).toBeLessThan(SHELL_TAIL_BYTES + 100);
    await job.finish(0);
    expect(job.snapshotText()).toBe("");
    job.append(Buffer.from("ignored"));
    expect(job.snapshotText()).toBe("");
    expect(fs.existsSync(path.dirname(job.pidPath))).toBe(false);
  });

  it("bounds disk logs and retires spilled PID files while retaining readable logs", async () => {
    const job = store().begin("bash", "loud");
    fs.writeFileSync(job.pidPath, String(process.pid));
    job.append(Buffer.alloc(SHELL_TAIL_BYTES * 2, 97));
    job.append(Buffer.from("start"));
    const log = await job.persistLog();
    job.spill();
    job.append(Buffer.alloc(SHELL_LOG_BYTES * 2, 98));
    await job.finish(0);
    expect(fs.existsSync(job.pidPath)).toBe(false);
    expect(fs.statSync(log).size).toBeLessThanOrEqual(SHELL_LOG_BYTES);
    const text = fs.readFileSync(log, "utf8");
    expect(text).toContain("Not a full-output archive");
    expect(text).toContain("Pre-spill output truncated");
    expect(text).toContain("Shell log truncated");
    expect(text).toContain("start");
    expect(job.snapshotText()).toBe("");
  });

  it("persists the tail even when a spilled command exits before the provider asks", async () => {
    const job = store().begin("bash", "fast exit");
    job.append(Buffer.from("last output"));
    job.spill();
    await job.finish(0);
    const log = await job.persistLog();
    expect(fs.readFileSync(log, "utf8")).toContain("last output");
    expect(job.snapshotText()).toBe("");
  });

  it("cleans failed log writes and tolerates disk errors in the data callback", async () => {
    const job = store().begin("bash", "errors");
    const write = vi.spyOn(fs, "writeSync").mockImplementation(() => { throw new Error("disk full"); });
    await expect(job.persistLog()).rejects.toThrow("disk full");
    expect(fs.existsSync(path.join(path.dirname(job.pidPath), "output.log"))).toBe(false);
    write.mockRestore();
    const log = await job.persistLog();
    vi.spyOn(fs, "writeSync").mockImplementation(() => { throw new Error("disk full"); });
    expect(() => job.append(Buffer.from("x"))).not.toThrow();
    write.mockRestore();
    await job.finish(1);
    expect(fs.existsSync(job.pidPath)).toBe(false);
    expect(fs.readFileSync(log, "utf8")).toContain("Not a full-output archive");
  });

  it("bounds completed handles without evicting running jobs", async () => {
    const jobs = store();
    const active = jobs.begin("bash", "active");
    const first = jobs.begin("bash", "first");
    await first.finish(0);
    for (let i = 0; i < SHELL_COMPLETED_HANDLES + 2; i++) await jobs.begin("bash", String(i)).finish(0);
    expect(jobs.list()).toHaveLength(SHELL_COMPLETED_HANDLES + 1);
    expect(jobs.get(first.id)).toBeUndefined();
    expect(jobs.get(active.id)).toBe(active);
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 25 * 60 * 60 * 1000);
    expect(jobs.list()).toHaveLength(1);
  });
});

describe("raceShellHang", () => {
  it("returns the execute result when the command finishes first", async () => {
    const jobs = store();
    const job = jobs.begin("bash", "echo");
    const result = await raceShellHang({
      hangMs: 200,
      parentSignal: undefined,
      job,
      execute: async () => "ok",
    });
    expect(result).toEqual({ status: "done", value: "ok" });
    await job.finish(0);
  });

  it("spills when the hang budget elapses first", async () => {
    const jobs = store();
    const job = jobs.begin("bash", "sleep");
    let continued = false;
    const result = await raceShellHang({
      hangMs: 40,
      parentSignal: undefined,
      job,
      execute: async (signal) => {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => {
            continued = true;
            resolve();
          }, 400);
          signal.addEventListener("abort", () => {
            clearTimeout(timer);
            reject(new Error("aborted"));
          }, { once: true });
        });
        return "late";
      },
    });
    expect(result).toEqual({ status: "spilled" });
    expect(job.spilled).toBe(true);
    expect(continued).toBe(false);
    job.abort.abort();
    await new Promise((resolve) => setTimeout(resolve, 20));
  });

  it("kills the nested wait when the parent aborts before spill", async () => {
    const jobs = store();
    const job = jobs.begin("bash", "sleep");
    const parent = new AbortController();
    const pending = raceShellHang({
      hangMs: 5_000,
      parentSignal: parent.signal,
      job,
      execute: async (signal) => {
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
        });
        return "nope";
      },
    });
    parent.abort(new Error("cancel turn"));
    const result = await pending;
    expect(result).toMatchObject({ status: "error" });
    expect(job.spilled).toBe(false);
  });

  it("spills immediately when requested, without waiting for hangMs", async () => {
    const jobs = store();
    const job = jobs.begin("bash", "sleep");
    const pending = raceShellHang({
      hangMs: 5_000,
      immediate: true,
      parentSignal: undefined,
      job,
      execute: async (signal) => {
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
        });
        return "nope";
      },
    });
    await expect(pending).resolves.toEqual({ status: "spilled" });
    job.abort.abort();
  });

  it("spills on demand so ctrl+b does not need background:true", async () => {
    const jobs = store();
    const job = jobs.begin("bash", "sleep");
    const pending = raceShellHang({
      hangMs: 0,
      parentSignal: undefined,
      job,
      execute: async (signal) => {
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
        });
        return "nope";
      },
    });
    expect(jobs.spillWaiting()).toBe(1);
    await expect(pending).resolves.toEqual({ status: "spilled" });
    job.abort.abort();
  });
});
