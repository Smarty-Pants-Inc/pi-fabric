import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal, completionConsumed, consumeCompletion, pendingCompletions, saveCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
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
  return { root, meshRoot, recipient, result, file, receipt, journal, seed };
};

describe("completion journal idle scans", () => {
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
    const beforeScan = performance.now();
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(2);
    const scanMs = performance.now() - beforeScan;
    const beforeDrain = performance.now(); const drain = journal.drain();
    const syncMs = performance.now() - beforeDrain;
    await drain; await journal.drain();
    expect(scanMs).toBeLessThan(16); expect(syncMs).toBeLessThan(16);
    expect(sync).not.toHaveBeenCalled(); expect(asyncOpen).not.toHaveBeenCalled();
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

  it("recovers a crash after the receipt barrier but before envelope unlink without redelivery", async () => {
    const h = setup(); const result = h.seed(1); const journal = h.journal();
    const rm = fs.rmSync;
    const unlink = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) === h.file(result.id)) throw new Error("crash before unlink");
      rm(target, options);
    });
    expect(() => journal.acknowledge(result.id)).toThrow("crash before unlink");
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

  it.skipIf(process.platform === "win32")("confirms imported receipt inodes once asynchronously and resyncs changed receipt entries, not stable ancestors", async () => {
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
    expect(counts.get(receipts)).toBe(1);
    h.seed(1); await journal.drain(); expect(counts.get(h.receipt(a.id))).toBe(1);
    h.seed(1); const replacement = `${h.receipt(a.id)}.replacement`; fs.writeFileSync(replacement, receipt(a.id));
    fs.renameSync(replacement, h.receipt(a.id));
    await journal.drain();
    expect(counts.get(h.receipt(a.id))).toBe(2); expect(counts.get(receipts)).toBe(2);
    expect(counts.get(path.dirname(receipts))).toBe(1); expect(sync).not.toHaveBeenCalled();
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
