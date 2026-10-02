import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { SHELL_COMPLETED_HANDLES } from "../src/core/shell-jobs.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { FABRIC_SHELL_TIMING_EVENT } from "../src/protocol.js";

describe("self-reload busy gate (smarty-dev#2160)", () => {
  it("counts a live background shell job until it finishes", async () => {
    const runtime = new FabricRuntimeState({} as ExtensionAPI, new CapturedToolCatalog());
    try {
      expect(runtime.backgroundWorkCount()).toBe(0);
      const job = runtime.shellJobs.begin("bash", "npm test");
      job.spill(); // detached from its tool call: the turn can settle while it runs
      expect(runtime.backgroundWorkCount()).toBe(1);
      await job.finish(0);
      expect(runtime.backgroundWorkCount()).toBe(0);
    } finally { await runtime.shutdown(); }
  });

  it("does not prune finishing shell jobs out of the reload hold before their notices exist (smarty-dev#2216)", async () => {
    const runtime = new FabricRuntimeState({} as ExtensionAPI, new CapturedToolCatalog());
    let finishing: Promise<void>[] = [];
    try {
      const jobs = Array.from({ length: SHELL_COMPLETED_HANDLES + 1 }, () => runtime.shellJobs.begin("bash", "background build"));
      for (const job of jobs) job.spill();
      await Promise.all(jobs.map(job => job.persistLog()));
      finishing = jobs.map(job => job.finish(0));
      await Promise.resolve(); // finishedAt is set, but unlink still holds the finished events
      expect(jobs.every(job => job.finishedAt !== undefined && !job.announced)).toBe(true);
      runtime.shellJobs.list(); // retention pressure must not remove the reload hold
      expect(runtime.backgroundWorkCount()).toBe(jobs.length);
      await Promise.all(finishing);
      expect(runtime.backgroundWorkCount()).toBe(0);
      expect(runtime.shellJobs.list()).toHaveLength(SHELL_COMPLETED_HANDLES);
    } finally { await Promise.all(finishing); await runtime.shutdown(); }
  });

  it("counts a finished job until its completion event is sent (smarty-dev#2216)", async () => {
    const runtime = new FabricRuntimeState({} as ExtensionAPI, new CapturedToolCatalog());
    try {
      const job = runtime.shellJobs.begin("bash", "npm test");
      job.spill();
      const events: string[] = [];
      runtime.shellJobs.subscribe(event => events.push(event.type));
      const finishing = job.finish(0); // sets finished, then awaits before it sends the event
      expect(job.finished).toBe(true);
      expect(events).not.toContain("finished");
      expect(runtime.backgroundWorkCount()).toBe(1);
      await finishing;
      expect(events).toContain("finished");
      expect(runtime.backgroundWorkCount()).toBe(0);
    } finally { await runtime.shutdown(); }
  });
});

describe("shell store runtime reinitialization", () => {
  it("bridges each owning session and closes spans on reload and shutdown", async () => {
    const emit = vi.fn();
    const runtime = new FabricRuntimeState({ events: { emit } } as unknown as ExtensionAPI, new CapturedToolCatalog());
    const context = {
      sessionManager: { getSessionId: () => "owner" },
      ui: { setStatus: () => { throw new Error("context checkpoint"); } },
    } as unknown as ExtensionContext;
    try {
      await expect(runtime.initialize(context)).rejects.toThrow("context checkpoint");
      const old = runtime.shellJobs;
      const a = old.begin("bash", "a"); a.spill();
      await expect(runtime.initialize(context)).rejects.toThrow("context checkpoint");
      const b = runtime.shellJobs.begin("bash", "b"); b.spill();
      await runtime.shutdown();
      expect(emit.mock.calls).toEqual([
        [FABRIC_SHELL_TIMING_EVENT, expect.objectContaining({ sessionId: "owner", taskId: a.id, phase: "started" })],
        [FABRIC_SHELL_TIMING_EVENT, expect.objectContaining({ taskId: a.id, phase: "finished" })],
        [FABRIC_SHELL_TIMING_EVENT, expect.objectContaining({ sessionId: "owner", taskId: b.id, phase: "started" })],
        [FABRIC_SHELL_TIMING_EVENT, expect.objectContaining({ taskId: b.id, phase: "finished" })],
      ]);
    } finally { await runtime.shutdown(); }
  });
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
