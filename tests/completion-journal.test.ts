import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { performance } from "node:perf_hooks";
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
  return { root, meshRoot, recipient, result, file, receipt, journal, seed, mesh };
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
  setCompletionJournalSliceObserver(durationMs => { slices.push(durationMs); lastSliceAt = performance.now(); });
  try {
    await nextTick(); // Arm the probe before any scan work starts.
    const start = performance.now();
    await operation();
    const passMs = performance.now() - start;
    await nextTick(); // Include a block in the final consumer/final small slice.
    const measured = { maxMs: Math.max(...gaps), samples: gaps.length, passMs,
      sliceMaxMs: Math.max(0, ...slices), sliceSamples: slices.length, delayedTicks };
    console.info("[completion-journal latency]", JSON.stringify({ label, ...measured }));
    return measured;
  } finally { clearInterval(timer); setCompletionJournalSliceObserver(undefined); }
};
const assertIdleLatency = (measured: Awaited<ReturnType<typeof measureIdleSlices>>): void => {
  expect(measured.samples).toBeGreaterThan(0);
  expect(measured.maxMs).toBeLessThan(16); // Independent timer gaps, NOT sliceObserver.
};

describe("completion journal idle scans", () => {
  it.each([false, true])("forget hides the result immediately while claim retirement is held (refused: %s)", async refused => {
    const h = setup(); const result = h.seed(1); const journal = h.journal();
    await journal.drain(false); // Establish an authenticated claim before cleanup.
    const remove = h.mesh.delete.bind(h.mesh);
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const deletion = vi.spyOn(h.mesh, "delete").mockImplementation(async args => {
      await held;
      if (refused) throw new Error("CAS refused");
      return remove(args);
    });
    try {
      journal.forget(result.id);
      expect(journal.result(result.id)).toBeUndefined();
      expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
      expect(fs.existsSync(h.file(result.id))).toBe(true);
      await vi.waitFor(() => expect(deletion).toHaveBeenCalledTimes(1));
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
      expect(fs.existsSync(h.file(result.id))).toBe(true); // Never unlink before CAS.
    } finally { release(); }
    if (!refused) await vi.waitFor(() => expect(fs.existsSync(h.file(result.id))).toBe(false));
    else {
      // Wait for the held retirement to reject, then retry via the normal drain path.
      await deletion.mock.results[0]!.value.catch(() => undefined);
      expect(fs.existsSync(h.file(result.id))).toBe(true);
      expect(journal.result(result.id)).toBeUndefined();
    }
    deletion.mockRestore();
    await journal.drain();
    expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.existsSync(h.receipt(result.id))).toBe(true);
    expect(journal.result(result.id)).toBeUndefined();
  });

  it("forget remains logically absent when receipt confirmation fails, retaining durable recovery", async () => {
    const h = setup(); const result = h.seed(1); const journal = h.journal();
    await journal.drain(false);
    const open = vi.spyOn(fs.promises, "open").mockRejectedValueOnce(new Error("confirmation failed"));
    journal.forget(result.id);
    expect(journal.result(result.id)).toBeUndefined();
    await open.mock.results[0]!.value.catch(() => undefined);
    expect(fs.existsSync(h.file(result.id))).toBe(true);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
    expect(h.journal().result(result.id)).toMatchObject({ id: result.id });
    open.mockRestore();
    await journal.drain();
    expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.existsSync(h.receipt(result.id))).toBe(true);
    expect(journal.result(result.id)).toBeUndefined();
  });

  it.each([2, 3])("yields between two-entry slices but not after a small final slice (%s entries)", async count => {
    const h = setup();
    for (let index = 1; index <= count; index++) h.seed(index);
    const turn = vi.spyOn(globalThis, "setImmediate");
    await h.journal().drain(false);
    if (count === 2) expect(turn).not.toHaveBeenCalled();
    else expect(turn).toHaveBeenCalled();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(count);
  });

  it("includes consumer work in the budget and runs a real event-loop turn before the next entry", async () => {
    const h = setup(); const a = h.seed(1), b = h.seed(2);
    const bodies = new Set([h.file(a.id), h.file(b.id)]);
    const events: string[] = [];
    let turn: ReturnType<typeof setImmediate> | undefined;
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: any[]) => {
      if (bodies.has(String(target))) {
        if (events.length === 0) {
          events.push("first body");
          turn = setImmediate(() => events.push("event-loop turn"));
          const start = performance.now();
          while (performance.now() - start < 6) { /* Exceed the 4 ms budget on the first entry. */ }
        } else events.push("next body");
      }
      return (read as any)(target, ...args);
    }) as typeof fs.readFileSync);
    const enqueue = vi.fn();
    try {
      await h.journal(enqueue).drain();
      expect(events).toEqual(["first body", "event-loop turn", "next body"]);
      expect(enqueue).toHaveBeenCalledTimes(2); // The final single entry still reaches admission.
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(2);
    } finally { clearImmediate(turn); }
  });

  it("rejects a synchronous 20 ms consumer block with the independent latency detector", async () => {
    const h = setup(); const result = h.seed(1);
    const read = fs.readFileSync;
    let blocked = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: any[]) => {
      if (!blocked && String(target) === h.file(result.id)) {
        blocked = true;
        const start = performance.now();
        while (performance.now() - start < 20) { /* Negative control in pendingCompletion's body read. */ }
      }
      return (read as any)(target, ...args);
    }) as typeof fs.readFileSync);
    const enqueue = vi.fn();
    const measured = await measureIdleSlices("negative control: 20 ms consumer", () => h.journal(enqueue).drain());
    expect(blocked).toBe(true);
    expect(measured.maxMs).toBeGreaterThanOrEqual(20);
    expect(() => assertIdleLatency(measured)).toThrow(); // The very same <16 ms regression rejects it.
    expect(measured.sliceMaxMs).toBeGreaterThanOrEqual(20); // Also covers the final slice's observer.
    expect(enqueue).toHaveBeenCalledTimes(1);
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
    const journal = h.journal(enqueue);
    const sync = vi.spyOn(fs, "fsyncSync");
    const asyncOpen = vi.spyOn(fs.promises, "open");
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(2);
    const slices = await measureIdleSlices("consumed and pending", async () => { await journal.drain(); await journal.drain(); });
    assertIdleLatency(slices);
    expect(slices.sliceSamples).toBeGreaterThan(0);
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
  });

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
    const enqueue = vi.fn(); const journal = h.journal(enqueue);
    const read = fs.readFileSync;
    let reads = 0;
    // Do not retain every large body in a spy's result history: the latency probe
    // should measure the journal's real reads/parsing, not instrumentation GC.
    fs.readFileSync = ((target: fs.PathOrFileDescriptor, ...args: any[]) => {
      reads++;
      const start = performance.now();
      while (performance.now() - start < 0.3) { /* Emulate a slower local filesystem. */ }
      return (read as any)(target, ...args);
    }) as typeof fs.readFileSync;
    const sync = vi.spyOn(fs, "fsyncSync");
    try {
      const slices = await measureIdleSlices(state, () => journal.drain());
      expect(reads).toBeGreaterThanOrEqual(100);
      expect(slices.passMs).toBeGreaterThan(30); // A whole-pass timer would incorrectly reject this scan.
      assertIdleLatency(slices);
      expect(slices.sliceSamples).toBeGreaterThan(0);
      expect(sync).not.toHaveBeenCalled();
      expect(enqueue.mock.calls.map(([result]) => result.id)).toEqual(expected); // Preserve directory order.
    } finally { fs.readFileSync = read; }
    if (state === "consumed leftovers") {
      const envelopes = fs.readdirSync(path.dirname(h.file(h.result(1).id))).filter(file => file.endsWith(".json"));
      expect(envelopes).toHaveLength(132); // Still cap destructive pruning at 128 per pass.
      for (let index = 1; index <= count; index++) expect(completionConsumed(h.meshRoot, h.result(index).id)).toBe(true);
    }
  });

  it("recovers a crash after the receipt barrier but before envelope unlink without redelivery", async () => {
    const h = setup(); const result = h.seed(1); const journal = h.journal();
    const rm = fs.rmSync;
    const unlink = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) === h.file(result.id)) throw new Error("crash before unlink");
      rm(target, options);
    });
    expect(journal.acknowledge(result.id)).toBe(true); // Consumption fences synchronously; cleanup is async.
    await vi.waitFor(() => expect(unlink.mock.calls.some(([target]) => String(target) === h.file(result.id))).toBe(true));
    expect(completionConsumed(h.meshRoot, result.id)).toBe(true); expect(fs.existsSync(h.file(result.id))).toBe(true);
    unlink.mockRestore();
    const enqueue = vi.fn(); await h.journal(enqueue).drain();
    expect(enqueue).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.existsSync(h.receipt(result.id))).toBe(true);
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

  it.skipIf(process.platform === "win32")("reconfirms the entire namespace asynchronously before every destructive prune", async () => {
    const h = setup(); const a = h.seed(1), b = h.seed(2);
    const receipts = path.dirname(h.receipt(a.id)); fs.mkdirSync(receipts, { recursive: true });
    const receipt = (id: string) => JSON.stringify({ id, sessionId: h.recipient.sessionId, consumedAt: 1 });
    fs.writeFileSync(h.receipt(a.id), receipt(a.id)); fs.writeFileSync(h.receipt(b.id), receipt(b.id));
    const counts = new Map<string, number>(); const open = fs.promises.open;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args); const sync = handle.sync.bind(handle);
      vi.spyOn(handle, "sync").mockImplementation(async () => {
        const file = String(args[0]); counts.set(file, (counts.get(file) ?? 0) + 1); await sync();
      });
      return handle;
    });
    const sync = vi.spyOn(fs, "fsyncSync"); const journal = h.journal(); await journal.drain();
    expect(counts.get(h.receipt(a.id))).toBe(1); expect(counts.get(h.receipt(b.id))).toBe(1);
    expect(counts.get(receipts)).toBe(2);
    h.seed(1); await journal.drain(); expect(counts.get(h.receipt(a.id))).toBe(2);
    h.seed(1); const replacement = `${h.receipt(a.id)}.replacement`; fs.writeFileSync(replacement, receipt(a.id));
    fs.renameSync(replacement, h.receipt(a.id));
    await journal.drain();
    expect(counts.get(h.receipt(a.id))).toBe(3); expect(counts.get(receipts)).toBe(4);
    expect(counts.get(path.dirname(receipts))).toBe(4); expect(sync).not.toHaveBeenCalled();
    expect(fs.existsSync(h.file(a.id))).toBe(false);
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
