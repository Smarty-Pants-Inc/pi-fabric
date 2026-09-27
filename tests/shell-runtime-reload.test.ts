import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";

describe("shell store runtime reinitialization", () => {
  it("retires old job handles and creates a writable store on each initialization attempt", async () => {
    const runtime = new FabricRuntimeState({} as ExtensionAPI, new CapturedToolCatalog());
    // Stop at the first contextual operation, after the real teardown/reset path.
    const context = { ui: { setStatus: () => { throw new Error("context checkpoint"); } } } as unknown as ExtensionContext;
    try {
      const old = runtime.shellJobs;
      const job = old.begin("bash", "old command"); job.spill();
      const events = vi.fn(); old.subscribe(events);
      await expect(runtime.initialize(context)).rejects.toThrow("context checkpoint");
      expect(job.abort.signal.aborted).toBe(true);
      expect(events).not.toHaveBeenCalled();
      expect(runtime.shellJobs).not.toBe(old);
      expect(() => old.begin("bash", "stale caller")).toThrow("closed");
      const next = runtime.shellJobs;
      expect(next.list()).toEqual([]);
      expect(() => next.begin("bash", "new command")).not.toThrow();
      await expect(runtime.initialize(context)).rejects.toThrow("context checkpoint");
      expect(runtime.shellJobs).not.toBe(next);
      expect(() => runtime.shellJobs.begin("bash", "newer command")).not.toThrow();
    } finally { await runtime.shutdown(); }
  });
});
