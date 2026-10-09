import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { createHook, executionAsyncId } from "node:async_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal, completionConsumed, consumeCompletion, pendingCompletions, saveCompletion, setCompletionJournalSliceObserver, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import type { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-idle-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const recipient: CompletionRecipient = { rootId: "session:main", sessionId: "main", projectRoot: root, cwd: root, name: "main", startedAt: 1 };
  const result = (index: number): AgentRunResult => ({ id: index.toString(16).padStart(32, "0"), name: "child", task: "task", status: "completed", runner: "pi", transport: "process", cwd: root, text: "x".repeat(100_000), startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
  const file = (id: string) => path.join(meshRoot, "agent-completions", `${createHash("sha256").update(id).digest("hex")}.json`);
  const archive = (id: string) => path.join(path.dirname(file(id)), "archive", path.basename(file(id)));
  const receipt = (id: string) => path.join(path.dirname(file(id)), "receipts", path.basename(file(id)));
  const entries = new Map<string, any>();
  const mesh = { listAll: () => [...entries.values()], get: (key: string) => entries.get(key), put: async (args: any) => { entries.set(args.key, { key: args.key, value: args.value, updatedBy: args.identity, version: 1 }); }, delete: async (args: any) => { entries.delete(args.key); } } as unknown as MeshStore;
  const participants = { list: () => [] } as unknown as FabricParticipantSource;
  const journal = (enqueue = vi.fn()) => new CompletionJournal(meshRoot, recipient, participants, mesh, enqueue);
  const seed = (index: number, address = recipient) => {
    const value = result(index); fs.mkdirSync(path.dirname(file(value.id)), { recursive: true });
    fs.writeFileSync(file(value.id), JSON.stringify({ format: 1, recipient: address, result: value }));
    return value;
  };
  return { root, meshRoot, recipient, result, file, archive, receipt, journal, seed, mesh, resetClaims: () => entries.clear() };
};

// Test-only async_hooks seam: before/after delimit each uninterrupted synchronous
// execution section descended from the scan, including async-generator consumers.
// Unlike the journal's wall-clock slice observer, this excludes awaited file I/O,
// timer delays and scheduling gaps BETWEEN callbacks. Accumulate those sections
// until the existing journal slice observer checkpoints them: a chain of cheap
// microtasks must not turn into an unbounded synchronous slice. The probe timer
// is created before this hook, so its callbacks are not scan descendants.
const measureSynchronousSections = async (operation: (finishSlice: () => void) => Promise<void>) => {
  const descendants = new Set<number>();
  let synchronousStart = true;
  let activeSince: number | undefined;
  let activeCpuSince: NodeJS.CpuUsage | undefined;
  let depth = 0;
  let sliceMs = 0, syncMaxMs = 0, sliceCpuMs = 0, syncCpuMaxMs = 0, syncSamples = 0;
  const recordActive = () => {
    if (activeSince === undefined) return;
    const now = performance.now();
    sliceMs += now - activeSince;
    if (activeCpuSince !== undefined) {
      const cpu = process.cpuUsage(activeCpuSince);
      sliceCpuMs += (cpu.user + cpu.system) / 1000;
    }
    syncMaxMs = Math.max(syncMaxMs, sliceMs);
    syncCpuMaxMs = Math.max(syncCpuMaxMs, sliceCpuMs);
    syncSamples++;
    activeSince = now;
    activeCpuSince = process.platform === "linux" ? process.cpuUsage() : undefined;
  };
  const finishSlice = () => { recordActive(); sliceMs = 0; sliceCpuMs = 0; };
  const hook = createHook({
    init(id, _type, trigger) {
      if (synchronousStart || descendants.has(trigger) || descendants.has(executionAsyncId())) descendants.add(id);
    },
    before(id) {
      if (descendants.has(id) && depth++ === 0) {
        activeSince = performance.now();
        activeCpuSince = process.platform === "linux" ? process.cpuUsage() : undefined;
      }
    },
    after(id) {
      if (descendants.has(id) && --depth === 0) {
        recordActive();
        activeSince = undefined;
        activeCpuSince = undefined;
      }
    },
    destroy(id) { descendants.delete(id); },
  });
  hook.enable();
  try {
    // Also time the synchronous prefix, before operation() returns its promise.
    activeSince = performance.now();
    activeCpuSince = process.platform === "linux" ? process.cpuUsage() : undefined;
    depth = 1;
    let pending: Promise<void>;
    try { pending = operation(finishSlice); }
    finally {
      recordActive();
      depth = 0;
      activeSince = undefined;
      activeCpuSince = undefined;
      synchronousStart = false;
    }
    await pending;
  } finally { hook.disable(); }
  // Windows CPU accounting is tick-quantized; keep CPU time as a Linux-only diagnostic.
  return { syncMaxMs, syncCpuMaxMs: process.platform === "linux" ? syncCpuMaxMs : undefined, syncSamples };
};
const assertSynchronousWork = (measured: Awaited<ReturnType<typeof measureSynchronousSections>>, platform: NodeJS.Platform = process.platform): void => {
  expect(measured.syncSamples).toBeGreaterThan(0);
  // ponytail: smarty-dev#4640 tracks a Windows latency SLO on real hosts.
  // Parse/validate use the same main-thread JavaScript on every platform; awaited
  // fs.promises I/O is excluded. Shared Windows runners preempt for 17–690 ms,
  // and CPU accounting is quantized to 15.6 ms, so Windows timing is diagnostic
  // only. Linux enforces every slice in one pass, never a best-of-N minimum.
  if (platform === "linux") {
    expect(measured.syncMaxMs,
      "scan synchronous wall time must stay below 16 ms under the linux policy").toBeLessThan(16);
  }
};
// The concurrent timer is independent of generator bookkeeping. Measure the full
// gap between 1 ms ticks (without subtracting timer resolution), so a synchronous
// consumer block cannot disappear behind an async-generator/microtask yield.
const measureIdleSlices = async (label: string, operation: () => Promise<void>) => {
  const gaps: number[] = [], slices: number[] = [];
  const delayedTicks: { gapMs: number; sliceSamples: number; sinceSliceMs: number }[] = [];
  let lastSliceAt = performance.now();
  let lastTick = performance.now();
  let onTick: (() => void) | undefined;
  const nextTick = () => new Promise<void>(resolve => { onTick = resolve; });
  const timer = setInterval(() => {
    const now = performance.now();
    const gapMs = now - lastTick;
    gaps.push(gapMs);
    if (gapMs >= 16) delayedTicks.push({ gapMs, sliceSamples: slices.length, sinceSliceMs: now - lastSliceAt });
    lastTick = now;
    const resolve = onTick; onTick = undefined; resolve?.();
  }, 1);
  try {
    await nextTick(); // Arm the probe before any scan work starts.
    const start = performance.now();
    const synchronous = await measureSynchronousSections(async finishSlice => {
      setCompletionJournalSliceObserver(durationMs => {
        finishSlice(); // Sum ONLY directly timed synchronous sections within this journal slice.
        slices.push(durationMs); lastSliceAt = performance.now();
      });
      await operation();
    });
    const passMs = performance.now() - start;
    await nextTick(); // Include a block in the final consumer/final small slice.
    const ordered = [...gaps].sort((a, b) => a - b);
    const measured = { maxMs: Math.max(...gaps), p99Ms: ordered[Math.ceil(ordered.length * 0.99) - 1]!, samples: gaps.length, passMs,
      ...synchronous, sliceMaxMs: Math.max(0, ...slices), sliceSamples: slices.length, delayedTicks };
    console.info("[completion-journal latency]", JSON.stringify({ label, ...measured }));
    return measured;
  } finally { clearInterval(timer); setCompletionJournalSliceObserver(undefined); }
};
// Prepare outside the timed sections and measure exactly one complete scan.
const measureIdlePass = async (label: string, prepare: () => () => Promise<void>, verify?: (measured: Awaited<ReturnType<typeof measureIdleSlices>>) => void) => {
  const operation = prepare();
  const measured = await measureIdleSlices(label, operation);
  verify?.(measured);
  return measured;
};
const assertIdleLatency = (measured: Awaited<ReturnType<typeof measureIdleSlices>>, platform: NodeJS.Platform = process.platform): void => {
  assertSynchronousWork(measured, platform);
  expect(measured.samples).toBeGreaterThan(0);
};

describe("completion journal idle scans", () => {
  it("does not scan or canonicalize an absent journal during empty idle polls", async () => {
    const h = setup(); const journal = h.journal();
    const scan = vi.spyOn(fs.promises, "readdir");
    const canonical = vi.spyOn(fs.promises, "realpath");
    vi.useFakeTimers();
    try {
      await journal.drain(); await journal.drain();
      expect(scan).not.toHaveBeenCalled(); expect(canonical).not.toHaveBeenCalled();
    } finally { vi.useRealTimers(); }
  });

  it("strictly rejects a single Linux wall stall while Windows timing remains diagnostic", () => {
    const measured = { maxMs: 50, p99Ms: 2, samples: 200, passMs: 100,
      syncMaxMs: 1, syncCpuMaxMs: 48, syncSamples: 100, sliceMaxMs: 50, sliceSamples: 100, delayedTicks: [] };
    expect(() => assertIdleLatency({ ...measured, syncMaxMs: 15.9 }, "linux")).not.toThrow();
    for (const syncMaxMs of [16, 25, 50, 690]) {
      const stalled = { ...measured, syncMaxMs, syncCpuMaxMs: 1 };
      expect(() => assertIdleLatency(stalled, "linux")).toThrow("scan synchronous wall time");
      expect(() => assertIdleLatency(stalled, "win32")).not.toThrow();
    }
    for (const platform of ["linux", "win32"] as const) {
      expect(() => assertIdleLatency({ ...measured, syncCpuMaxMs: undefined }, platform)).not.toThrow();
      expect(() => assertIdleLatency({ ...measured, syncSamples: 0 }, platform)).toThrow();
      expect(() => assertIdleLatency({ ...measured, samples: 0 }, platform)).toThrow();
    }
  });

  it("times the synchronous prefix and resumed work, but not an awaited 30 ms timer", async () => {
    const measured = await measureSynchronousSections(async () => {
      await new Promise<void>(resolve => setTimeout(resolve, 30));
    });
    expect(measured.syncSamples).toBeGreaterThan(1);
    assertSynchronousWork(measured);
    for (const resumed of [false, true]) {
      const blocked = await measureSynchronousSections(async () => {
        if (resumed) await new Promise<void>(resolve => setImmediate(resolve));
        const start = performance.now();
        while (performance.now() - start < 25) { /* Verify both instrumentation paths. */ }
      });
      expect(blocked.syncMaxMs).toBeGreaterThanOrEqual(25);
      expect(() => assertSynchronousWork(blocked, "linux")).toThrow("scan synchronous wall time");
      expect(() => assertSynchronousWork(blocked, "win32")).not.toThrow();
    }
  });

  it("accumulates synchronous microtasks within a slice instead of budgeting each callback separately", async () => {
    const measured = await measureSynchronousSections(async finishSlice => {
      for (let index = 0; index < 6; index++) {
        await Promise.resolve(); // Not an event-loop turn or a new journal slice.
        const start = performance.now();
        while (performance.now() - start < 4) { /* Individually cheap, collectively over budget. */ }
      }
      finishSlice();
    });
    expect(measured.syncMaxMs).toBeGreaterThanOrEqual(24);
    expect(() => assertSynchronousWork(measured, "linux")).toThrow("scan synchronous wall time");
    expect(() => assertSynchronousWork(measured, "win32")).not.toThrow();
  });
  it.each([false, true])("forget hides the result immediately while claim retirement is held (refused: %s)", async refused => {
    const h = setup(); const result = h.seed(1); const journal = h.journal();
    await journal.drain(false);
    const remove = h.mesh.delete.bind(h.mesh);
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const deletion = vi.spyOn(h.mesh, "delete").mockImplementation(async args => {
      await held; if (refused) throw new Error("CAS refused"); return remove(args);
    });
    try {
      journal.forget(result.id);
      expect(journal.result(result.id)).toBeUndefined();
      expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
      expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.archive(result.id))).toBe(true);
      await vi.waitFor(() => expect(deletion).toHaveBeenCalledTimes(1));
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
    } finally { release(); }
    await deletion.mock.results[0]!.value.catch(() => undefined);
    deletion.mockRestore(); await journal.drain();
    expect(fs.existsSync(h.archive(result.id))).toBe(true); expect(fs.existsSync(h.receipt(result.id))).toBe(true);
    expect(journal.result(result.id)).toBeUndefined();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
  });

  it("forget remains logically absent when claim retirement fails, retaining archived recovery", async () => {
    const h = setup(); const result = h.seed(1); const journal = h.journal(); await journal.drain(false);
    const deletion = vi.spyOn(h.mesh, "delete").mockRejectedValueOnce(new Error("CAS failed"));
    journal.forget(result.id); expect(journal.result(result.id)).toBeUndefined();
    await vi.waitFor(() => expect(deletion).toHaveBeenCalled());
    await deletion.mock.results[0]!.value.catch(() => undefined);
    expect(fs.existsSync(h.archive(result.id))).toBe(true); expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
    expect(h.journal().result(result.id)).toMatchObject({ id: result.id });
    deletion.mockRestore(); await journal.drain();
    expect(fs.existsSync(h.archive(result.id))).toBe(true); expect(fs.existsSync(h.receipt(result.id))).toBe(true);
    expect(journal.result(result.id)).toBeUndefined();
  });

  it.each([2, 3])("yields between two-entry slices but not after a small final slice (%s entries)", async count => {
    const h = setup();
    for (let index = 1; index <= count; index++) h.seed(index);
    const turn = vi.spyOn(globalThis, "setImmediate");
    await h.journal().drain(false);
    if (count === 3) expect(turn).toHaveBeenCalled(); // Async file I/O may yield even before the CPU slice cap.
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(count);
  });

  it("includes consumer work in the budget and runs a real event-loop turn before the next entry", async () => {
    const h = setup(); const a = h.seed(1), b = h.seed(2);
    const bodies = new Set([h.file(a.id), h.file(b.id)]);
    const events: string[] = [];
    let turn: ReturnType<typeof setImmediate> | undefined;
    const read = fs.promises.readFile.bind(fs.promises);
    vi.spyOn(fs.promises, "readFile").mockImplementation((async (target: any, ...args: any[]) => {
      if (bodies.has(String(target))) {
        if (events.length === 0) {
          events.push("first body");
          await new Promise<void>(resolve => { turn = setImmediate(() => { events.push("event-loop turn"); resolve(); }); });
        } else events.push("next body");
      }
      return read(target, ...args);
    }) as typeof fs.promises.readFile);
    const enqueue = vi.fn();
    try {
      await h.journal(enqueue).drain();
      expect(events).toEqual(["first body", "event-loop turn", "next body"]);
      expect(enqueue).toHaveBeenCalledTimes(2); // The final single entry still reaches admission.
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(2);
    } finally { clearImmediate(turn); }
  });

  it.each(["linux", "win32"] as const)("observes a synchronous 25 ms final consumer block under the %s policy", async platform => {
    const h = setup();
    // Block the final enqueue after the scan's awaited I/O: neither a final
    // small slice nor async-generator yields may hide this from the guard.
    const enqueue = vi.fn(() => {
      const started = performance.now();
      while (performance.now() - started < 25) { /* Robust wall-clock stall, independent of CPU tick resolution. */ }
    });
    const measured = await measureIdlePass(`negative control: 25 ms consumer (${platform})`, () => {
      h.seed(1);
      const journal = h.journal(enqueue);
      return () => journal.drain();
    });
    // Sanity on both platforms: the scan completed and the stall is visible in
    // the logged synchronous maximum and independent timer maximum.
    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(measured.syncMaxMs).toBeGreaterThanOrEqual(25);
    expect(measured.maxMs).toBeGreaterThanOrEqual(25);
    if (platform === "linux") {
      expect(() => assertSynchronousWork(measured, platform)).toThrow("scan synchronous wall time");
      expect(() => assertIdleLatency(measured, platform)).toThrow("scan synchronous wall time");
    } else {
      expect(() => assertIdleLatency(measured, platform)).not.toThrow();
    }
  });

  it("a queued delivery callback cannot restore a forgotten result", async () => {
    const h = setup(); const result = h.seed(1);
    let delivered!: () => void;
    const journal = h.journal(vi.fn((_result: AgentRunResult, callback: () => void) => { delivered = callback; }));
    await journal.drain();
    journal.forget(result.id);
    delivered();
    await journal.drain();
    expect(journal.result(result.id)).toBeUndefined();
    expect(fs.existsSync(h.receipt(result.id))).toBe(true);
  });
  it("drains 100 consumed large envelopes and two pending ones without synchronous fsync or a 16ms blocking scan", async () => {
    const h = setup();
    for (let index = 1; index <= 100; index++) {
      const result = h.seed(index); consumeCompletion(h.meshRoot, result.id, h.recipient.sessionId);
    }
    const a = h.seed(101), b = h.seed(102);
    const callbacks = new Map<string, () => void>();
    const enqueue = vi.fn((result: AgentRunResult, delivered: () => void) => { callbacks.set(result.id, delivered); });
    let journal = h.journal(enqueue);
    const sync = vi.spyOn(fs, "fsyncSync");
    const asyncOpen = vi.spyOn(fs.promises, "open");
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(2);
    const measured = await measureIdlePass("consumed and pending", () => {
      for (let index = 1; index <= 102; index++) h.seed(index); // Restore pruned envelopes, keeping the same receipts.
      h.resetClaims(); enqueue.mockClear(); callbacks.clear();
      journal = h.journal(enqueue);
      return async () => { await journal.drain(); await journal.drain(); };
    }, measured => {
      expect(measured.sliceSamples).toBeGreaterThan(0);
      expect(enqueue.mock.calls.map(([result]) => result.id)).toEqual(expect.arrayContaining([a.id, b.id]));
      expect(enqueue).toHaveBeenCalledTimes(2);
      for (let index = 1; index <= 100; index++) expect(fs.existsSync(h.file(h.result(index).id))).toBe(false);
    });
    assertIdleLatency(measured);
    expect(sync).not.toHaveBeenCalled(); expect(asyncOpen).toHaveBeenCalled();
    expect(enqueue.mock.calls.map(([result]) => result.id)).toEqual(expect.arrayContaining([a.id, b.id]));
    expect(enqueue).toHaveBeenCalledTimes(2);
    for (let index = 1; index <= 100; index++) {
      const id = h.result(index).id; expect(fs.existsSync(h.file(id))).toBe(false); expect(completionConsumed(h.meshRoot, id)).toBe(true);
    }
    sync.mockRestore();
    callbacks.get(a.id)!(); callbacks.get(b.id)!();
    await journal.drain(); expect(enqueue).toHaveBeenCalledTimes(2);
    expect(fs.existsSync(h.file(a.id))).toBe(false); expect(fs.existsSync(h.file(b.id))).toBe(false);
  }, 15_000); // Allow a full async pass; Linux synchronous work stays below 16 ms.

  it.each(["consumed leftovers", "pending", "attempts"] as const)("bounds every idle slice with slow plain reads: %s", async state => {
    const h = setup();
    const count = state === "consumed leftovers" ? 260 : 100;
    for (let index = 1; index <= count; index++) {
      const result = h.seed(index);
      if (state === "consumed leftovers") consumeCompletion(h.meshRoot, result.id, h.recipient.sessionId);
      if (state === "attempts") {
        const target = path.join(path.dirname(h.file(result.id)), "attempts", path.basename(h.file(result.id)));
        fs.mkdirSync(path.dirname(target), { recursive: true });
        fs.renameSync(h.file(result.id), target); // Addressed but uncommitted: no supervisor can authorize promotion.
      }
    }
    const expected = pendingCompletions(h.meshRoot, h.root).map(envelope => envelope.result.id);
    const enqueue = vi.fn();
    const read = fs.promises.readFile.bind(fs.promises);
    let reads = 0;
    // Do not retain every large body in a spy's result history: the latency probe
    // should measure the journal's real reads/parsing, not instrumentation GC.
    const asyncRead = vi.spyOn(fs.promises, "readFile").mockImplementation((async (target: any, ...args: any[]) => {
      reads++;
      await new Promise(resolve => setTimeout(resolve, 2.3)); // Windows-like per-read latency.
      return read(target, ...args);
    }) as typeof fs.promises.readFile);
    const sync = vi.spyOn(fs, "fsyncSync");
    try {
      const measured = await measureIdlePass(state, () => {
        if (state === "consumed leftovers") {
          for (let index = 1; index <= count; index++) h.seed(index); // Restore the complete fixture before measurement.
        }
        h.resetClaims(); enqueue.mockClear(); reads = 0;
        const journal = h.journal(enqueue);
        return () => journal.drain();
      }, measured => {
        expect(reads).toBeGreaterThanOrEqual(100); // Exercise the full original fixture in the single pass.
        expect(measured.passMs).toBeGreaterThan(30); // A whole-pass timer would incorrectly reject this scan.
        expect(measured.sliceSamples).toBeGreaterThan(0);
        expect(sync).not.toHaveBeenCalled();
        expect(enqueue.mock.calls.map(([result]) => result.id)).toEqual(expected); // Preserve directory order.
        if (state === "consumed leftovers") {
          expect(fs.readdirSync(path.dirname(h.file(h.result(1).id))).filter(file => file.endsWith(".json"))).toHaveLength(132);
        }
      });
      assertIdleLatency(measured);
    } finally { asyncRead.mockRestore(); }
    if (state === "consumed leftovers") {
      const envelopes = fs.readdirSync(path.dirname(h.file(h.result(1).id))).filter(file => file.endsWith(".json"));
      expect(envelopes).toHaveLength(132); // Still cap destructive pruning at 128 per pass.
      for (let index = 1; index <= count; index++) expect(completionConsumed(h.meshRoot, h.result(index).id)).toBe(true);
    }
  }, 15_000);

  it("recovers a crash after the receipt barrier but before envelope archive without redelivery", async () => {
    const h = setup(); const result = h.seed(1); const journal = h.journal(); const rename = fs.renameSync;
    const crash = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(source) === h.file(result.id)) throw new Error("crash before archive"); rename(source, target);
    });
    expect(() => journal.acknowledge(result.id)).toThrow("crash before archive");
    expect(completionConsumed(h.meshRoot, result.id)).toBe(true); expect(fs.existsSync(h.file(result.id))).toBe(true);
    crash.mockRestore();
    const enqueue = vi.fn(); await h.journal(enqueue).drain();
    expect(enqueue).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.existsSync(h.archive(result.id))).toBe(true); expect(fs.existsSync(h.receipt(result.id))).toBe(true);
  });

  it("a fresh process prunes crash-left bodies, keeps receipts, and never republishes or redelivers consumed outcomes", () => {
    const h = setup(); const result = h.seed(1); consumeCompletion(h.meshRoot, result.id, h.recipient.sessionId);
    const source = path.resolve("src/agents/completion-journal.ts");
    const script = `import fs from 'node:fs'; import {CompletionJournal, saveCompletion} from ${JSON.stringify(source)}; const mesh={listAll:()=>[],get:()=>undefined,delete:async()=>{}}; const journal=new CompletionJournal(${JSON.stringify(h.meshRoot)},${JSON.stringify(h.recipient)},{list:()=>[]},mesh,()=>{throw new Error('redelivered')}); await journal.drain(); saveCompletion(${JSON.stringify(h.meshRoot)},${JSON.stringify(h.recipient)},${JSON.stringify({ ...result, text: "late save" })}); await journal.drain(); console.log('no redelivery');`;
    expect(execFileSync("bun", ["--eval", script], { encoding: "utf8", timeout: 15_000 })).toContain("no redelivery");
    expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.receipt(result.id))).toBe(true);
  });

  it("bounds old-envelope pruning to 128 per pass", async () => {
    const h = setup();
    for (let index = 1; index <= 130; index++) {
      const result = h.seed(index); consumeCompletion(h.meshRoot, result.id, h.recipient.sessionId);
      fs.renameSync(h.archive(result.id), h.file(result.id)); // Receipt-before-archive crash fixture.
    }
    const enqueue = vi.fn(); const journal = h.journal(enqueue); await journal.drain();
    expect(fs.readdirSync(path.dirname(h.file(h.result(1).id))).filter(file => file.endsWith(".json"))).toHaveLength(2);
    await journal.drain(); expect(enqueue).not.toHaveBeenCalled();
    expect(fs.readdirSync(path.dirname(h.file(h.result(1).id))).filter(file => file.endsWith(".json"))).toHaveLength(0);
  });

  it("checks canonical project and recipient before any fence or full-body read", async () => {
    const h = setup(); const foreignProject = h.seed(1, { ...h.recipient, projectRoot: path.dirname(h.root) });
    const foreignLane = h.seed(2, { ...h.recipient, rootId: "other", sessionId: "other", name: "other" });
    fs.mkdirSync(path.dirname(h.receipt(foreignProject.id)), { recursive: true });
    fs.writeFileSync(h.receipt(foreignProject.id), "invalid foreign receipt");
    fs.writeFileSync(h.receipt(foreignLane.id), "invalid foreign receipt");
    const read = vi.spyOn(fs, "readFileSync");
    await h.journal().drain();
    expect(read.mock.calls.some(([file]) => [h.file(foreignProject.id), h.receipt(foreignProject.id), h.file(foreignLane.id), h.receipt(foreignLane.id)].includes(String(file)))).toBe(false);
    expect(fs.existsSync(h.file(foreignProject.id))).toBe(true); expect(fs.existsSync(h.file(foreignLane.id))).toBe(true);
  });

  it("archives crash-left bodies only after async receipt and namespace confirmation", async () => {
    const h = setup(); const a = h.seed(1), b = h.seed(2);
    const receipts = path.dirname(h.receipt(a.id)); fs.mkdirSync(receipts, { recursive: true });
    const receipt = (id: string) => JSON.stringify({ id, sessionId: h.recipient.sessionId, consumedAt: 1 });
    fs.writeFileSync(h.receipt(a.id), receipt(a.id)); fs.writeFileSync(h.receipt(b.id), receipt(b.id));
    const counts = new Map<string, number>(); const open = fs.promises.open;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      const syncHandle = handle.sync.bind(handle);
      vi.spyOn(handle, "sync").mockImplementation(async () => { const file = String(args[0]); counts.set(file, (counts.get(file) ?? 0) + 1); await syncHandle(); });
      return handle;
    });
    const sync = vi.spyOn(fs, "fsyncSync"); const journal = h.journal(); await journal.drain();
    h.seed(1); await journal.drain();
    h.seed(1); const replacement = `${h.receipt(a.id)}.replacement`; fs.writeFileSync(replacement, receipt(a.id)); fs.renameSync(replacement, h.receipt(a.id));
    await journal.drain();
    expect(counts.get(h.receipt(a.id))).toBe(3); expect(counts.get(h.receipt(b.id))).toBe(1);
    if (process.platform !== "win32") expect(counts.get(receipts)).toBe(4);
    expect(sync).not.toHaveBeenCalled();
    expect(fs.existsSync(h.file(a.id))).toBe(false); expect(fs.existsSync(h.archive(a.id))).toBe(true);
    expect(fs.existsSync(h.archive(b.id))).toBe(true);
  });
  it.each(["torn", "invalid", "dangling"])("keeps unknown %s receipts fail-closed even on plain scans", async fault => {
    const h = setup(); const result = h.seed(1); fs.mkdirSync(path.dirname(h.receipt(result.id)), { recursive: true });
    if (fault === "dangling") fs.symlinkSync(path.join(h.root, "missing"), h.receipt(result.id));
    else fs.writeFileSync(h.receipt(result.id), fault === "torn" ? "{" : JSON.stringify({ id: result.id, sessionId: "main", consumedAt: 0 }));
    const sync = vi.spyOn(fs, "fsyncSync");
    expect(() => pendingCompletions(h.meshRoot, h.root)).toThrow(/replay fence/);
    await expect(h.journal().drain()).rejects.toThrow(/replay fence/);
    expect(sync).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(true);
  });
});
