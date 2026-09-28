import fs from "node:fs";
import { afterEach, describe, expect, it, vi } from "vitest";
import { type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { normalizeFabricConfig } from "../src/config.js";

const stores: FabricShellJobStore[] = [];
const registries: ActionRegistry[] = [];

afterEach(async () => {
  await Promise.all(registries.splice(0).map((registry) => registry.close()));
  await Promise.all(stores.splice(0).map((jobs) => jobs.close()));
});

const invokeBash = async (
  command: string,
  hangMs: number,
  signal?: AbortSignal,
  extra: Record<string, unknown> = {},
  jobs = new FabricShellJobStore(),
) => {
  stores.push(jobs);
  const provider = new PiToolsProvider(process.cwd(), undefined, undefined, {
    powerShellToolDefinitionFactory: undefined,
    getShellHangMs: () => hangMs,
    shellJobs: jobs,
  });
  const registry = new ActionRegistry();
  registry.register(provider);
  registries.push(registry);
  const result = await registry.invoke(
    "pi.bash",
    { command, ...extra },
    {
      cwd: process.cwd(),
      signal: signal ?? new AbortController().signal,
      parentToolCallId: "parent",
      nestedToolCallId: "fabric_test-hang",
      extensionContext: {
        cwd: process.cwd(),
        sessionManager: {
          getSessionId: () => "hang-test",
          getSessionFile: () => undefined,
        },
      } as unknown as ExtensionContext,
      update: () => undefined,
      approve: async () => {},
      audits: [],
      maxResultChars: 100_000,
    },
  ) as {
    ok: boolean;
    output: string;
    details: {
      running?: boolean;
      taskId?: string;
      pid?: number;
      logPath?: string;
      elapsedMs?: number;
    } | null;
  };
  return { result, jobs };
};

// A detached Windows shell can be reaped before a probe observes it. The spill
// contract itself is asserted on every platform; only these probes are scoped.
const windowsShell = process.platform === "win32";
// The pass-through test proves that a command which ends before the threshold is
// returned unchanged. A tight threshold made it a race against a starved runner
// (smarty-dev#883), so give it room far above any shell start stall.
const SHORT_COMMAND_HANG_MS = 30_000;
const PID_PROBE_EXACT = !windowsShell;

describe("pi.bash auto-spill", () => {
  it("tracks a real detached nonzero exit, task ID, cwd, and terminal event", async () => {
    const { result, jobs } = await invokeBash("printf start; sleep 0.3; printf failed; exit 7", 0, undefined, { background: true, description: "Failing build" });
    expect(result.details?.taskId).toEqual(expect.any(String));
    const job = jobs.get(result.details!.taskId!)!;
    await vi.waitFor(() => expect(job.info().finishedAt).toBeDefined(), { timeout: 5000 });
    expect(job.info()).toMatchObject({ status: "failed", exitCode: 7, description: "Failing build", cwd: process.cwd() });
    expect(await job.outputText()).toContain("failed");
  });

  it("starts an opt-in monitor on the protected shell path and stops it at its deadline", async () => {
    const { result, jobs } = await invokeBash("printf 'CI: waiting\\n'; sleep 8", 0, undefined, { monitor: { delivery: "ui", timeoutMs: 1000, intervalMs: 1000 } });
    const job = jobs.get(result.details!.taskId!)!;
    expect(job.info().monitor?.delivery).toBe("ui");
    await vi.waitFor(() => expect(job.info().finishedAt).toBeDefined(), { timeout: 5000 });
    expect(job.info().status).toBe("timed_out");
    expect(job.abort.signal.aborted).toBe(true);
  });

  it("requires explicit valid monitor delivery before creating a shell job", async () => {
    await expect(invokeBash("echo forbidden", 0, undefined, { monitor: {} })).rejects.toThrow();
    await expect(invokeBash("echo forbidden", 0, undefined, { background: false, monitor: { delivery: "wake" } })).rejects.toThrow("background:false");
  });
  it("lets a short command pass through unchanged", async () => {
    const { result } = await invokeBash('printf "hi\\n"', SHORT_COMMAND_HANG_MS);
    expect(result.ok).toBe(true);
    expect(result.output).toBe("hi\n");
    expect(result.details).not.toMatchObject({ running: true });
  });

  it("spills a hung command as ok:true with a live log and pid", async () => {
    const { result, jobs } = await invokeBash("printf start; sleep 8; printf done", 120);
    expect(result.ok).toBe(true);
    expect(result.output).toContain("[Still running after ");
    expect(result.output).toContain("Bounded live output (may be truncated):");
    expect(result.details?.running).toBe(true);
    expect(result.details?.logPath).toBeTruthy();
    const logPath = result.details!.logPath!;
    expect(fs.existsSync(logPath)).toBe(true);
    const pid = result.details?.pid;
    expect(pid).toEqual(expect.any(Number));
    if (typeof pid === "number" && PID_PROBE_EXACT) {
      expect(() => process.kill(pid, 0)).not.toThrow();
      try { process.kill(-pid, "SIGKILL"); } catch { process.kill(pid, "SIGKILL"); }
    }
    expect(jobs.list().some((job) => job.status === "spilled")).toBe(true);
  });

  it("does not auto-spill when hangMs is 0", async () => {
    const { result } = await invokeBash('printf "done\\n"', 0);
    expect(result.ok).toBe(true);
    expect(result.output).toBe("done\n");
    expect(result.output).not.toContain("Still running");
  });

  it("normalizes shellHangMs including off", () => {
    expect(normalizeFabricConfig({}).executor.shellHangMs).toBe(120_000);
    expect(normalizeFabricConfig({ executor: { shellHangMs: 0 } }).executor.shellHangMs).toBe(0);
    expect(normalizeFabricConfig({ executor: { shellHangMs: -5 } }).executor.shellHangMs).toBe(0);
    expect(normalizeFabricConfig({ executor: { shellHangMs: 20 * 60_000 } }).executor.shellHangMs).toBe(600_000);
  });

  it("spills immediately when background:true", async () => {
    const { result, jobs } = await invokeBash("printf start; sleep 8; printf done", 120_000, undefined, { background: true });
    expect(result.ok).toBe(true);
    expect(result.details?.running).toBe(true);
    expect(result.details?.logPath).toBeTruthy();
    const pid = result.details?.pid;
    expect(pid).toEqual(expect.any(Number));
    if (typeof pid === "number" && PID_PROBE_EXACT) {
      expect(() => process.kill(pid, 0)).not.toThrow();
      try { process.kill(-pid, "SIGKILL"); } catch { process.kill(pid, "SIGKILL"); }
    }
    expect(jobs.list().some((job) => job.status === "spilled")).toBe(true);
    expect(result.details?.elapsedMs ?? 1_000).toBeLessThan(2_000);
  });

  // An explicit handoff must stay fast: only the auto-spill waits for a late pid (smarty-dev#883).
  // Every job's late-pid wait is made to take 1.5 s; the handoffs must not sit through it, and the
  // auto-spill counterexample must.
  describe("late pid wait", () => {
    const LATE_PID_MS = 1_500;
    const waits: unknown[] = [];
    const slowPidWait = () => {
      const begin = FabricShellJobStore.prototype.begin;
      vi.spyOn(FabricShellJobStore.prototype, "begin").mockImplementation(function (this: FabricShellJobStore, ...args) {
        const job = begin.apply(this, args);
        vi.spyOn(job, "waitForPid").mockImplementation(() => {
          waits.push(job.id);
          return new Promise((resolve) => setTimeout(() => resolve(undefined), LATE_PID_MS));
        });
        return job;
      });
      waits.length = 0;
    };

    it.each([
      ["background:true", { background: true }],
      ["a monitor", { monitor: { delivery: "ui", timeoutMs: 10_000 } }],
    ])("hands off with %s without waiting for a late pid", async (_label, extra) => {
      slowPidWait();
      const started = Date.now();
      const { result } = await invokeBash("printf start; sleep 8", 120_000, undefined, extra);
      expect(Date.now() - started).toBeLessThan(1_000);
      expect(result.details?.running).toBe(true);
      expect(waits).toEqual([]);
    });

    // Ctrl+B twice calls spillWaiting(); the tool call must return at once, not sit in the wait.
    it("hands off a manual Ctrl+B spill without waiting for a late pid", async () => {
      slowPidWait();
      const jobs = new FabricShellJobStore();
      const pending = invokeBash("printf start; sleep 8", 120_000, undefined, {}, jobs);
      await vi.waitFor(() => expect(jobs.waiting()).toHaveLength(1), { timeout: 5_000 });
      const spilledAt = Date.now();
      expect(jobs.spillWaiting()).toBe(1);
      const { result } = await pending;
      expect(Date.now() - spilledAt).toBeLessThan(1_000);
      expect(result.details?.running).toBe(true);
      expect(waits).toEqual([]);
    });

    it("auto-spill waits for a late pid (counterexample)", async () => {
      slowPidWait();
      const started = Date.now();
      const { result } = await invokeBash("printf start; sleep 8", 120);
      expect(result.details?.running).toBe(true);
      expect(waits).toHaveLength(1);
      expect(Date.now() - started).toBeGreaterThanOrEqual(LATE_PID_MS);
    });
  });

  // A real late pid write, through the PI_FABRIC_TEST_PID_DELAY_MS seam, past the 250 ms bounded read.
  describe("real delayed pid write", () => {
    afterEach(() => { delete process.env.PI_FABRIC_TEST_PID_DELAY_MS; });

    it("auto-spill result carries the late pid", async () => {
      process.env.PI_FABRIC_TEST_PID_DELAY_MS = "600";
      const { result } = await invokeBash("printf start; sleep 8", 120);
      expect(result.details?.running).toBe(true);
      expect(result.details?.pid).toEqual(expect.any(Number));
    });

    it("manual spill returns at once and the late pid reaches the job record", async () => {
      process.env.PI_FABRIC_TEST_PID_DELAY_MS = "600";
      const jobs = new FabricShellJobStore();
      const pending = invokeBash("printf start; sleep 8", 120_000, undefined, {}, jobs);
      await vi.waitFor(() => expect(jobs.waiting()).toHaveLength(1), { timeout: 5_000 });
      const spilledAt = Date.now();
      jobs.spillWaiting();
      const { result } = await pending;
      expect(Date.now() - spilledAt).toBeLessThan(1_000);
      const job = jobs.get(result.details!.taskId!)!;
      await vi.waitFor(() => expect(job.info().pid).toEqual(expect.any(Number)), { timeout: 5_000 });
    });
  });
});
