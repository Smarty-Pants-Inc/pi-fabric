import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal, consumeCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import type { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-cache-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const recipient: CompletionRecipient = { rootId: "session:main", sessionId: "main", projectRoot: root, cwd: root, name: "main", startedAt: 1 };
  const foreign = { ...recipient, rootId: "session:away", sessionId: "away", name: "away" };
  const file = (id: string) => path.join(meshRoot, "agent-completions", hash(id) + ".json");
  const receipt = (id: string) => path.join(path.dirname(file(id)), "receipts", path.basename(file(id)));
  const result = (index: number): AgentRunResult => ({ id: index.toString(16).padStart(32, "0"), name: "task", task: "synthetic", status: "completed", runner: "pi", transport: "process", cwd: root, text: "x".repeat(20_000), startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
  const seed = (index: number, address = foreign) => {
    const value = result(index); fs.mkdirSync(path.dirname(file(value.id)), { recursive: true });
    fs.writeFileSync(file(value.id), JSON.stringify({ format: 1, recipient: address, result: value })); return value;
  };
  const entries = new Map<string, any>();
  const mesh = { listAll: () => [...entries.values()], get: (key: string) => entries.get(key), put: async (args: any) => { entries.set(args.key, { key: args.key, value: args.value, updatedBy: args.identity, version: 1 }); }, delete: async (args: any) => { entries.delete(args.key); } } as unknown as MeshStore;
  const enqueue = vi.fn();
  const journal = new CompletionJournal(meshRoot, () => recipient, { list: () => [] } as unknown as FabricParticipantSource, mesh, enqueue);
  return { root, meshRoot, recipient, foreign, result, seed, file, receipt, journal, enqueue };
};

describe("completion journal unchanged routing metadata", () => {
  it("does not reopen hundreds of unchanged foreign consumed envelopes on repeated idle polls", async () => {
    const h = setup(); const files = new Set<string>();
    for (let i = 1; i <= 300; i++) { const result = h.seed(i); files.add(h.file(result.id)); consumeCompletion(h.meshRoot, result.id, "away"); }
    const open = vi.spyOn(fs.promises, "open"); const read = vi.spyOn(fs.promises, "readFile");
    await h.journal.drain();
    expect(open.mock.calls.filter(([file]) => files.has(String(file)))).toHaveLength(300); // Not twice in one pass.
    open.mockClear(); read.mockClear();
    for (let i = 0; i < 3; i++) await h.journal.drain();
    expect(open.mock.calls.filter(([file]) => files.has(String(file)))).toHaveLength(0);
    expect(read).not.toHaveBeenCalled(); // No foreign bodies OR replay fences.
    expect(h.enqueue).not.toHaveBeenCalled();
  });

  it.each(["atomic replacement", "in-place write"] as const)("sees a same-size restored-mtime %s without a directory change", async change => {
    const h = setup(); const result = h.seed(1); const target = h.file(result.id);
    await h.journal.drain(); expect(h.enqueue).not.toHaveBeenCalled();
    const before = fs.statSync(target); const dirBefore = fs.statSync(path.dirname(target));
    const replacement = JSON.stringify({ format: 1, recipient: h.recipient, result });
    expect(Buffer.byteLength(replacement)).toBe(before.size);
    await new Promise(resolve => setTimeout(resolve, 10)); // Coarse timestamp filesystems still get a distinct ctime.
    if (change === "atomic replacement") { fs.writeFileSync(target + ".new", replacement); fs.utimesSync(target + ".new", before.atime, before.mtime); fs.renameSync(target + ".new", target); }
    else { fs.writeFileSync(target, replacement); fs.utimesSync(target, before.atime, before.mtime); expect(fs.statSync(path.dirname(target)).mtimeMs).toBe(dirBefore.mtimeMs); }
    await h.journal.drain(); expect(h.enqueue).toHaveBeenCalledTimes(1); expect(h.enqueue.mock.calls[0]![0].id).toBe(result.id);
  });

  it("rechecks a live Main's name/lane without reopening unchanged routing metadata", async () => {
    const h = setup(); const result = h.seed(1);
    await h.journal.drain(); expect(h.enqueue).not.toHaveBeenCalled();
    Object.assign(h.recipient, h.foreign);
    await h.journal.drain(); expect(h.enqueue).toHaveBeenCalledTimes(1); expect(h.enqueue.mock.calls[0]![0].id).toBe(result.id);
  });

  it("finds newly created entries while every old foreign entry is unchanged", async () => {
    const h = setup(); h.seed(1); await h.journal.drain();
    const result = h.seed(2, h.recipient);
    await h.journal.drain(); expect(h.enqueue).toHaveBeenCalledTimes(1); expect(h.enqueue.mock.calls[0]![0].id).toBe(result.id);
  });

  it("invalidates unknown stat results and retries instead of using a stale address", async () => {
    const h = setup(); const result = h.seed(1); await h.journal.drain();
    Object.assign(h.recipient, h.foreign);
    const stat = fs.promises.stat.bind(fs.promises);
    const fault = vi.spyOn(fs.promises, "stat").mockImplementation(((target: any, ...args: any[]) => {
      if (String(target) === h.file(result.id)) return Promise.reject(Object.assign(new Error("unreadable"), { code: "EACCES" }));
      return (stat as any)(target, ...args);
    }) as typeof fs.promises.stat);
    await h.journal.drain(); expect(h.enqueue).not.toHaveBeenCalled();
    fault.mockRestore(); const open = vi.spyOn(fs.promises, "open");
    await h.journal.drain(); expect(h.enqueue).toHaveBeenCalledTimes(1);
    expect(open.mock.calls.some(([target]) => String(target) === h.file(result.id))).toBe(true);
  });

  it("does not cache torn metadata or hide a repaired file with unchanged directory entries", async () => {
    const h = setup(); const result = h.seed(1); fs.writeFileSync(h.file(result.id), "{");
    await h.journal.drain(); expect(h.enqueue).not.toHaveBeenCalled();
    h.seed(1, h.recipient); await h.journal.drain(); expect(h.enqueue).toHaveBeenCalledTimes(1);
  });

  it("rechecks receipts after warming metadata and keeps invalid fences fail-closed", async () => {
    const h = setup(); const result = h.seed(1); await h.journal.drain(); Object.assign(h.recipient, h.foreign);
    fs.mkdirSync(path.dirname(h.receipt(result.id)), { recursive: true }); fs.writeFileSync(h.receipt(result.id), "{");
    await expect(h.journal.drain()).rejects.toThrow(/replay fence/);
    expect(h.enqueue).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(result.id))).toBe(true);
  });

  it("reopens an eligible cleanup address and confirms its receipt even after cache warmup", async () => {
    const h = setup(); const result = h.seed(1); await h.journal.drain(); Object.assign(h.recipient, h.foreign);
    consumeCompletion(h.meshRoot, result.id, "away");
    const open = vi.spyOn(fs.promises, "open"); await h.journal.drain();
    expect(open.mock.calls.some(([target]) => String(target) === h.file(result.id))).toBe(true);
    expect(open.mock.calls.some(([target]) => String(target) === h.receipt(result.id))).toBe(true);
    expect(fs.existsSync(h.file(result.id))).toBe(false); expect(fs.existsSync(h.receipt(result.id))).toBe(true);
    expect(h.enqueue).not.toHaveBeenCalled();
  });

  it("does not cache a replaced inode under the pre-open path stamp", async () => {
    const h = setup(); const result = h.seed(1); const target = h.file(result.id);
    const before = fs.statSync(target, { bigint: true });
    const stat = fs.promises.stat.bind(fs.promises);
    // Hold the old path stamp so a wrongly labelled cache entry would hit on the next pass.
    vi.spyOn(fs.promises, "stat").mockImplementation(((file: any, ...args: any[]) =>
      String(file) === target ? Promise.resolve(before) : (stat as any)(file, ...args)) as typeof fs.promises.stat);
    const open = fs.promises.open.bind(fs.promises); let replaced = false;
    const opened = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      if (String(args[0]) === target && !replaced) {
        replaced = true;
        fs.writeFileSync(target + ".new", JSON.stringify({ format: 1, recipient: { ...h.foreign, name: "else" }, result }));
        fs.renameSync(target + ".new", target);
      }
      return open(...args);
    });
    await h.journal.drain(); await h.journal.drain();
    expect(opened.mock.calls.filter(([file]) => String(file) === target)).toHaveLength(4);
    expect(h.enqueue).not.toHaveBeenCalled();
  });

  it.each(["name", "role"] as const)("rejects an immutable %s mismatch before canonical-path filesystem fan-out", async field => {
    const h = setup();
    const foreign = { ...h.foreign, cwd: path.join(h.root, "foreign-cwd"), projectRoot: path.join(h.root, "foreign-project"), ...(field === "role" ? { name: h.recipient.name, role: "other" } : {}) };
    h.seed(1, foreign);
    const realpath = vi.spyOn(fs.promises, "realpath");
    await h.journal.drain(); await h.journal.drain();
    expect(realpath.mock.calls.some(([file]) => [foreign.cwd, foreign.projectRoot].includes(String(file)))).toBe(false);
    expect(h.enqueue).not.toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32")("re-resolves canonical projects when a symlink changes but an envelope does not", async () => {
    const h = setup(); const elsewhere = path.join(h.root, "elsewhere"); fs.mkdirSync(elsewhere);
    const alias = path.join(h.root, "alias"); fs.symlinkSync(elsewhere, alias, "dir");
    h.seed(1, { ...h.recipient, projectRoot: alias });
    await h.journal.drain(); expect(h.enqueue).not.toHaveBeenCalled();
    fs.unlinkSync(alias); fs.symlinkSync(h.root, alias, "dir");
    await h.journal.drain(); expect(h.enqueue).toHaveBeenCalledTimes(1);
  });

  it("bounds routing metadata retention rather than accumulating every historical envelope", async () => {
    const h = setup(); for (let i = 1; i <= 1025; i++) h.seed(i);
    await h.journal.drain(); const open = vi.spyOn(fs.promises, "open"); await h.journal.drain();
    expect(open.mock.calls.length).toBeGreaterThan(0); // The finite 1024-slot cache must evict.
    expect(h.enqueue).not.toHaveBeenCalled();
  });
});
