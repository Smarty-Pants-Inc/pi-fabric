import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { installFabricShellHangKeys } from "../src/ui/shell-hang-keys.js";

const CTRL_B = "\x02";
const CTRL_K = "\x0b";

function harness() {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
  const inputs = new Set<(data: string) => unknown>();
  const jobs = new FabricShellJobStore();
  const job = jobs.begin("bash", "sleep 30");
  const notify = vi.fn();
  const context = {
    mode: "tui",
    ui: {
      notify,
      onTerminalInput: (handler: (data: string) => unknown) => {
        inputs.add(handler);
        return () => { inputs.delete(handler); };
      },
    },
  } as unknown as ExtensionContext;
  const dispose = installFabricShellHangKeys(context, {
    enabled: () => true,
    ownsInput: () => false,
    jobs: () => jobs,
  });
  return {
    dispose,
    notify,
    jobs,
    job,
    input: (data: string) => [...inputs].map(input => input(data)).find(result => result !== undefined),
  };
}

afterEach(async () => {
  vi.useRealTimers();
});

describe("shell hang keys", () => {
  it("cancels monitors on lone Escape but not arrow sequences or ordinary detached shells", async () => {
    const h = harness();
    h.job.spill();
    const watch = h.jobs.begin("bash", "watch", { monitor: { delivery: "ui", timeoutMs: 300000, intervalMs: 1000 } });
    watch.spill();
    h.input("\u001b[A"); vi.advanceTimersByTime(100);
    expect(watch.abort.signal.aborted).toBe(false);
    expect(h.input("\u001b")).toBeUndefined(); vi.advanceTimersByTime(100);
    expect(watch.abort.signal.aborted).toBe(true);
    expect(h.job.abort.signal.aborted).toBe(false);
    h.dispose(); await h.jobs.close();
  });
  it("requires ctrl+b twice within 1s to spill", async () => {
    const h = harness();
    expect(h.input(CTRL_B)).toEqual({ consume: true });
    expect(h.job.spilled).toBe(false);
    expect(h.notify).not.toHaveBeenCalled();
    expect(h.input(CTRL_B)).toEqual({ consume: true });
    expect(h.job.spilled).toBe(true);
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("still running"), "info");
    h.job.abort.abort();
    await h.jobs.close();
    h.dispose();
  });

  it("does not spill when the second ctrl+b is too late", async () => {
    const h = harness();
    expect(h.input(CTRL_B)).toEqual({ consume: true });
    vi.advanceTimersByTime(1_001);
    expect(h.input(CTRL_B)).toEqual({ consume: true });
    expect(h.job.spilled).toBe(false);
    h.job.abort.abort();
    await h.jobs.close();
    h.dispose();
  });

  it("kills on a single ctrl+k", async () => {
    const h = harness();
    expect(h.input(CTRL_K)).toEqual({ consume: true });
    expect(h.job.abort.signal.aborted).toBe(true);
    expect(h.notify).toHaveBeenCalledWith(expect.stringContaining("killed waiting shell"), "warning");
    await h.jobs.close();
    h.dispose();
  });
});
