import fs from "node:fs";
import { afterEach, describe, expect, it } from "vitest";
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
) => {
  const jobs = new FabricShellJobStore();
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
      pid?: number;
      logPath?: string;
      elapsedMs?: number;
    } | null;
  };
  return { result, jobs };
};

// A Windows shell chain costs more to start than a tight hang threshold allows,
// and a detached shell there can be reaped before a probe observes it. The spill
// contract itself is asserted on every platform; only these probes are scoped.
const windowsShell = process.platform === "win32";
const SHORT_COMMAND_HANG_MS = windowsShell ? 2_000 : 80;
const PID_PROBE_EXACT = !windowsShell;

describe("pi.bash auto-spill", () => {
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
});
