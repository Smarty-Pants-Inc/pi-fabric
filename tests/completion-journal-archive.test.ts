import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
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
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-archive-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const recipient: CompletionRecipient = { rootId: "session:main", sessionId: "main", projectRoot: root, cwd: root, name: "main", startedAt: 1 };
  const directory = path.join(meshRoot, "agent-completions");
  const name = (id: string) => `${createHash("sha256").update(id).digest("hex")}.json`;
  const file = (id: string) => path.join(directory, name(id));
  const archive = (id: string) => path.join(directory, "archive", name(id));
  const receipt = (id: string) => path.join(directory, "receipts", name(id));
  const result = (index: number): AgentRunResult => ({ id: index.toString(16).padStart(32, "0"), name: "task", task: "synthetic", status: "completed", runner: "pi", transport: "process", cwd: root, text: "retained result", startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
  const seed = (index: number) => { const value = result(index); saveCompletion(meshRoot, recipient, value); return value; };
  const entries = new Map<string, any>();
  const mesh = { listAll: () => [...entries.values()], get: (key: string) => entries.get(key), put: async (args: any) => { entries.set(args.key, { key: args.key, value: args.value, updatedBy: args.identity, version: 1 }); }, delete: async (args: any) => { entries.delete(args.key); } } as unknown as MeshStore;
  const journal = (enqueue = vi.fn(), address = recipient) => new CompletionJournal(meshRoot, address, { list: () => [] } as unknown as FabricParticipantSource, mesh, enqueue);
  return { root, meshRoot, recipient, directory, file, archive, receipt, result, seed, mesh, journal };
};
const crashBeforeArchive = (source: string) => {
  const rename = fs.renameSync;
  return vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (String(from) === source) throw Object.assign(new Error("crash before archive"), { code: "EIO" });
    rename(from, to);
  });
};
const noDrainSync = () => {
  const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("drain must not fsync"); });
  const open = fs.promises.open;
  const handles: ReturnType<typeof vi.spyOn>[] = [];
  vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
    const handle = await open(...args);
    handles.push(vi.spyOn(handle, "sync").mockRejectedValue(new Error("drain must not sync")));
    return handle;
  });
  return { sync, handles };
};

describe("completion receipt-time archive", () => {
  it("archives on receipt, preserving body and inode, only after receipt barriers", () => {
    const h = setup(); const result = h.seed(1); const body = fs.readFileSync(h.file(result.id), "utf8"); const before = fs.statSync(h.file(result.id));
    const rename = fs.renameSync; const sync = fs.fsyncSync; let receiptRenamed = false; let receiptSynced = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { sync(fd); if (receiptRenamed && fs.fstatSync(fd).isDirectory()) receiptSynced = true; });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === h.receipt(result.id)) receiptRenamed = true;
      if (String(from) === h.file(result.id)) { expect(receiptSynced || process.platform === "win32").toBe(true); expect(completionConsumed(h.meshRoot, result.id)).toBe(true); }
      rename(from, to);
    });
    consumeCompletion(h.meshRoot, result.id, "main");
    expect(fs.existsSync(h.file(result.id))).toBe(false);
    expect(fs.readFileSync(h.archive(result.id), "utf8")).toBe(body);
    expect(fs.statSync(h.archive(result.id))).toMatchObject({ dev: before.dev, ino: before.ino });
    expect(JSON.parse(fs.readFileSync(h.receipt(result.id), "utf8"))).toMatchObject({ id: result.id, sessionId: "main" });
    expect(pendingCompletions(h.meshRoot, h.root)).toEqual([]);
  });

  it("does not archive when receipt durability fails", () => {
    const h = setup(); const result = h.seed(1);
    vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("receipt barrier failed"); });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("receipt barrier failed");
    expect(fs.existsSync(h.file(result.id))).toBe(true); expect(fs.existsSync(h.archive(result.id))).toBe(false);
  });

  it.each([false, true])("recovers receipt-before-rename crash with plain reads, claim=%s", async claim => {
    const h = setup(); const result = h.seed(1); const journal = h.journal();
    if (claim) await journal.drain(false);
    const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow("crash before archive"); crash.mockRestore();
    const before = fs.readFileSync(h.receipt(result.id), "utf8"); const mtime = fs.statSync(h.receipt(result.id)).mtimeMs;
    const enqueue = vi.fn(); const probes = noDrainSync();
    await h.journal(enqueue).drain(); await h.journal(enqueue).drain();
    expect(probes.sync).not.toHaveBeenCalled(); expect(probes.handles.every(spy => spy.mock.calls.length === 0)).toBe(true);
    expect(enqueue).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.archive(result.id))).toBe(true);
    expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(before); expect(fs.statSync(h.receipt(result.id)).mtimeMs).toBe(mtime);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
  });

  it("synchronous pending discovery skips crash-left receipts without replay or fsync", () => {
    const h = setup(); const result = h.seed(1); const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow(); crash.mockRestore();
    const sync = vi.spyOn(fs, "fsyncSync");
    expect(pendingCompletions(h.meshRoot, h.root)).toEqual([]); expect(sync).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(true);
  });

  it("concurrent recovery has exactly one winning rename and ignores the loser's ENOENT", async () => {
    const h = setup(); const result = h.seed(1); const crash = crashBeforeArchive(h.file(result.id));
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).toThrow(); crash.mockRestore();
    const rename = fs.promises.rename; let arrivals = 0; let wins = 0; let missing = 0; let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => {
      if (String(from) !== h.file(result.id)) return rename(from, to);
      if (++arrivals === 2) release(); await barrier;
      try { await rename(from, to); wins++; }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") missing++; throw error; }
    });
    const enqueue = vi.fn(); await Promise.all([h.journal(enqueue).drain(), h.journal(enqueue).drain()]);
    expect({ wins, missing }).toEqual({ wins: 1, missing: 1 }); expect(enqueue).not.toHaveBeenCalled(); expect(fs.existsSync(h.archive(result.id))).toBe(true);
  });

  it("tolerates a consumer racing the synchronous rename and keeps the first receipt", () => {
    const h = setup(); const result = h.seed(1); const rename = fs.renameSync; let wins = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === h.file(result.id)) {
        rename(from, to); wins++;
        // A sibling won after our path check but before our rename syscall.
        throw Object.assign(new Error("already archived"), { code: "ENOENT" });
      }
      rename(from, to);
    });
    expect(() => consumeCompletion(h.meshRoot, result.id, "main")).not.toThrow();
    const receipt = fs.readFileSync(h.receipt(result.id), "utf8");
    consumeCompletion(h.meshRoot, result.id, "other");
    expect(wins).toBe(1); expect(fs.readFileSync(h.receipt(result.id), "utf8")).toBe(receipt); expect(fs.existsSync(h.archive(result.id))).toBe(true);
  });

  it("does not touch any of 10,000 archived envelopes or receipts during repeated drains", async () => {
    const h = setup(); const archive = path.join(h.directory, "archive"); const receipts = path.join(h.directory, "receipts");
    fs.mkdirSync(archive, { recursive: true }); fs.mkdirSync(receipts);
    for (let index = 1; index <= 10_000; index++) {
      const result = h.result(index); fs.writeFileSync(h.archive(result.id), JSON.stringify({ format: 1, recipient: h.recipient, result }));
      fs.writeFileSync(h.receipt(result.id), JSON.stringify({ id: result.id, sessionId: "main", consumedAt: 1 }));
    }
    const historical = (value: unknown) => String(value) === archive || String(value).startsWith(archive + path.sep) || String(value) === receipts || String(value).startsWith(receipts + path.sep);
    const spies = [vi.spyOn(fs.promises, "readdir"), vi.spyOn(fs.promises, "open"), vi.spyOn(fs.promises, "readFile"), vi.spyOn(fs.promises, "stat"), vi.spyOn(fs, "readdirSync"), vi.spyOn(fs, "openSync"), vi.spyOn(fs, "readFileSync"), vi.spyOn(fs, "statSync")];
    const probes = noDrainSync(); const enqueue = vi.fn(); const journal = h.journal(enqueue);
    for (let index = 0; index < 5; index++) await journal.drain();
    for (const spy of spies) expect(spy.mock.calls.filter(args => historical(args[0]))).toHaveLength(0);
    expect(probes.sync).not.toHaveBeenCalled(); expect(enqueue).not.toHaveBeenCalled();
  }, 15_000);

  it("leaves unconsumed envelopes unchanged even alongside archived history", async () => {
    const h = setup(); const consumed = h.seed(1); consumeCompletion(h.meshRoot, consumed.id, "main"); const pending = h.seed(2);
    const before = fs.statSync(h.file(pending.id)); const body = fs.readFileSync(h.file(pending.id), "utf8"); const enqueue = vi.fn();
    await h.journal(enqueue).drain(false);
    expect(fs.readFileSync(h.file(pending.id), "utf8")).toBe(body); expect(fs.statSync(h.file(pending.id))).toMatchObject({ ino: before.ino, mtimeMs: before.mtimeMs });
    expect(fs.existsSync(h.archive(pending.id))).toBe(false); expect(completionConsumed(h.meshRoot, pending.id)).toBe(false); expect(enqueue).not.toHaveBeenCalled();
  });

  it("keeps targeted results after restart without widening exact-owner access", () => {
    const h = setup(); const result = h.seed(1); consumeCompletion(h.meshRoot, result.id, "main");
    expect(h.journal().result(result.id)).toMatchObject({ id: result.id, text: result.text });
    const foreign = h.journal(vi.fn(), { ...h.recipient, rootId: "session:other", sessionId: "other" });
    expect(foreign.result(result.id)).not.toHaveProperty("text"); expect(foreign.acknowledge(result.id)).toBe(false);
    saveCompletion(h.meshRoot, h.recipient, result); expect(fs.existsSync(h.file(result.id))).toBe(false);
  });

  it("retains archive and receipt while an exact-owner claim deletion is refused", async () => {
    const h = setup(); const result = h.seed(1); const journal = h.journal(); await journal.drain(false);
    consumeCompletion(h.meshRoot, result.id, "main"); vi.spyOn(h.mesh, "delete").mockRejectedValue(new Error("CAS refused"));
    const probes = noDrainSync(); await journal.drain(false);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1); expect(probes.sync).not.toHaveBeenCalled();
    expect(fs.existsSync(h.archive(result.id))).toBe(true); expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
  });
});
