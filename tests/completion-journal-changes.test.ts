import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal, consumeCompletion, saveWorkerCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const hash = (id: string) => createHash("sha256").update(id).digest("hex") + ".json";
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-changes-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const recipient: CompletionRecipient = { rootId: "session:main", sessionId: "main", projectRoot: root, cwd: root, name: "main", startedAt: 1 };
  const foreign = { ...recipient, rootId: "session:away", sessionId: "away", name: "away" };
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const enqueue = vi.fn();
  const journal = new CompletionJournal(meshRoot, recipient, {} as FabricParticipantSource, mesh, enqueue);
  const result = (index: number): AgentRunResult => ({ id: index.toString(16).padStart(32, "0"), name: "task", task: "test", status: "completed", runner: "pi", transport: "process", cwd: root, text: "x".repeat(20_000), startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
  const file = (id: string) => path.join(meshRoot, "agent-completions", hash(id));
  const seed = (index: number, address = foreign) => {
    const value = result(index); fs.mkdirSync(path.dirname(file(value.id)), { recursive: true });
    fs.writeFileSync(file(value.id), JSON.stringify({ format: 1, recipient: address, result: value })); return value;
  };
  return { root, meshRoot, recipient, foreign, mesh, journal, enqueue, file, seed, result };
};

describe("completion journal changed-path drains", () => {
  it("does zero idle I/O beyond initial discovery with 2000 envelopes and no larger routing cache", async () => {
    const h = fixture(); for (let i = 1; i <= 2000; i++) h.seed(i);
    await h.journal.drainChanged();
    const stat = vi.spyOn(fs.promises, "stat"), open = vi.spyOn(fs.promises, "open"), read = vi.spyOn(fs.promises, "readFile"), readdir = vi.spyOn(fs.promises, "readdir");
    const state = vi.spyOn(h.mesh, "listAll");
    for (let i = 0; i < 10; i++) await h.journal.drainChanged();
    expect(stat).not.toHaveBeenCalled(); expect(open).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled();
    expect(readdir).not.toHaveBeenCalled(); expect(state).not.toHaveBeenCalled(); expect(h.enqueue).not.toHaveBeenCalled();
    await h.journal.drainChanged(true, { safety: true });
    expect(readdir).not.toHaveBeenCalled(); expect(open).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled();
  });

  it.each(["foreign", "consumed"])("retires a deleted %s path from safety observation and reconciles hinted recreation", async kind => {
    const h = fixture(), value = h.seed(1, kind === "consumed" ? h.recipient : h.foreign), target = h.file(value.id);
    await h.journal.drainChanged();
    if (kind === "consumed") consumeCompletion(h.meshRoot, value.id, h.recipient.sessionId);
    h.enqueue.mockClear();
    fs.rmSync(target);
    await h.journal.drainChanged(true, { safety: true });
    const stat = vi.spyOn(fs.promises, "stat"), open = vi.spyOn(fs.promises, "open"), read = vi.spyOn(fs.promises, "readFile"), readdir = vi.spyOn(fs.promises, "readdir");
    for (let i = 0; i < 3; i++) await h.journal.drainChanged(true, { safety: true });
    expect(stat.mock.calls.some(([file]) => String(file) === target)).toBe(false);
    expect(open).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled(); expect(readdir).not.toHaveBeenCalled();
    fs.writeFileSync(target, JSON.stringify({ format: 1, recipient: h.recipient, result: value }));
    h.journal.changed("envelopes", path.basename(target));
    await h.journal.drainChanged();
    expect(readdir).not.toHaveBeenCalled();
    expect(open.mock.calls.some(([file]) => String(file) === target)).toBe(true);
    if (kind === "consumed") {
      expect(h.enqueue).not.toHaveBeenCalled();
      expect(fs.existsSync(target)).toBe(false);
      expect(fs.existsSync(path.join(path.dirname(target), "receipts", hash(value.id)))).toBe(true);
    } else {
      expect(h.enqueue).toHaveBeenCalledOnce();
      expect(h.enqueue.mock.calls[0]![0].id).toBe(value.id);
      expect(h.mesh.listAll("residency/completion-claims/")[0]?.value).toMatchObject({ rootId: h.recipient.rootId, sessionId: h.recipient.sessionId });
    }
  });

  it.each(["in-place", "atomic"])("routes just a filename repair after a same-size restored-mtime %s write", async kind => {
    const h = fixture(); for (let i = 1; i <= 30; i++) h.seed(i);
    await h.journal.drainChanged();
    const value = h.result(15), target = h.file(value.id), before = fs.statSync(target);
    const bytes = JSON.stringify({ format: 1, recipient: h.recipient, result: value }); expect(Buffer.byteLength(bytes)).toBe(before.size);
    if (kind === "atomic") { fs.writeFileSync(target + ".new", bytes); fs.renameSync(target + ".new", target); }
    else fs.writeFileSync(target, bytes);
    fs.utimesSync(target, before.atime, before.mtime);
    const open = vi.spyOn(fs.promises, "open"), readdir = vi.spyOn(fs.promises, "readdir");
    h.journal.changed("envelopes", path.basename(target)); await h.journal.drainChanged();
    expect(h.enqueue).toHaveBeenCalledOnce(); expect(h.enqueue.mock.calls[0]![0].id).toBe(value.id);
    expect(readdir).not.toHaveBeenCalled();
    expect(open.mock.calls.filter(([file]) => String(file).endsWith(".json") && path.dirname(String(file)) === path.dirname(target)).every(([file]) => String(file) === target)).toBe(true);
  });

  it("treats a filename event as invalidation even when the physical stamp repeats", async () => {
    const h = fixture(), value = h.seed(1), target = h.file(value.id);
    await h.journal.drainChanged(); const before = fs.statSync(target, { bigint: true });
    fs.writeFileSync(target, JSON.stringify({ format: 1, recipient: h.recipient, result: value }));
    const stat = fs.promises.stat.bind(fs.promises);
    vi.spyOn(fs.promises, "stat").mockImplementation(((file: any, ...args: any[]) =>
      String(file) === target ? Promise.resolve(before) : (stat as any)(file, ...args)) as typeof fs.promises.stat);
    h.journal.changed("envelopes", path.basename(target)); await h.journal.drainChanged();
    expect(h.enqueue).toHaveBeenCalledOnce();
  });

  it("discovers only changed/new files on a null filename and replaced directory", async () => {
    const h = fixture(); h.seed(1); await h.journal.drainChanged();
    const open = vi.spyOn(fs.promises, "open");
    h.seed(2, h.recipient); h.journal.changed("envelopes", null); await h.journal.drainChanged();
    expect(h.enqueue).toHaveBeenCalledOnce(); expect(open.mock.calls.some(([file]) => String(file) === h.file(h.result(1).id))).toBe(false);
    fs.renameSync(path.dirname(h.file(h.result(1).id)), path.join(h.meshRoot, "old-journal"));
    h.seed(3, h.recipient); h.journal.changed("envelopes", null); await h.journal.drainChanged();
    expect(h.enqueue).toHaveBeenCalledTimes(2);
  });

  it("keeps invalid receipts fail-closed and retries the exact repaired receipt without a directory scan", async () => {
    const h = fixture(), value = h.seed(1, h.recipient);
    const receipt = path.join(path.dirname(h.file(value.id)), "receipts", hash(value.id));
    fs.mkdirSync(path.dirname(receipt)); fs.writeFileSync(receipt, "{");
    await expect(h.journal.drainChanged()).rejects.toThrow(/replay fence/); expect(h.enqueue).not.toHaveBeenCalled();
    fs.rmSync(receipt); consumeCompletion(h.meshRoot, value.id, h.recipient.sessionId);
    const readdir = vi.spyOn(fs.promises, "readdir");
    h.journal.changed("receipts", path.basename(receipt)); await h.journal.drainChanged();
    expect(readdir).not.toHaveBeenCalled(); expect(fs.existsSync(h.file(value.id))).toBe(false); expect(fs.existsSync(receipt)).toBe(true);
    expect(h.enqueue).not.toHaveBeenCalled();
  });

  it("claims notification-disabled work without enqueue, and keeps foreign receipt/body metadata private", async () => {
    const h = fixture(), value = h.seed(1, h.recipient), foreign = h.seed(2);
    consumeCompletion(h.meshRoot, foreign.id, "away");
    const read = vi.spyOn(fs.promises, "readFile");
    await h.journal.drainChanged(false);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1); expect(h.enqueue).not.toHaveBeenCalled();
    expect(read.mock.calls.some(([file]) => String(file).includes(hash(foreign.id)))).toBe(false);
    expect(h.journal.result(value.id)).toMatchObject({ text: value.text });
  });

  it("retires a bodyless exact-owner claim after receipt repair and retains a refused CAS delete", async () => {
    const h = fixture(), value = h.result(1), ck = "residency/completion-claims/" + hash(value.id).slice(0, -5);
    await h.mesh.put({ key: ck, identity: { id: h.recipient.rootId, name: "main", kind: "main" }, value: { rootId: h.recipient.rootId, sessionId: h.recipient.sessionId } });
    const receipt = path.join(h.meshRoot, "agent-completions", "receipts", hash(value.id)); fs.mkdirSync(path.dirname(receipt), { recursive: true }); fs.writeFileSync(receipt, "{");
    await expect(h.journal.drainChanged(true, { claims: h.mesh.listAll("residency/completion-claims/") })).rejects.toThrow(/replay fence/);
    fs.rmSync(receipt); consumeCompletion(h.meshRoot, value.id, h.recipient.sessionId);
    const remove = vi.spyOn(h.mesh, "delete").mockRejectedValueOnce(new Error("CAS refused"));
    h.journal.changed("receipts", path.basename(receipt)); await h.journal.drainChanged();
    expect(h.mesh.get(ck)).toBeDefined(); expect(fs.existsSync(receipt)).toBe(true); expect(h.journal.hasPendingChanges).toBe(true);
    remove.mockRestore(); await h.journal.drainChanged(); expect(h.mesh.get(ck)).toBeDefined();
    await h.journal.drainChanged(true, { retryPending: true }); expect(h.mesh.get(ck)).toBeUndefined(); expect(fs.existsSync(receipt)).toBe(true);
  });

  it("revisits only known own quiet work when notification policy turns on", async () => {
    const h = fixture(); h.seed(1, h.recipient); for (let i = 2; i <= 40; i++) h.seed(i);
    await h.journal.drainChanged(false);
    const read = vi.spyOn(fs.promises, "readFile"), open = vi.spyOn(fs.promises, "open"), stat = vi.spyOn(fs.promises, "stat");
    await h.journal.drainChanged(false); await h.journal.drainChanged(false);
    expect(open).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled(); expect(stat).not.toHaveBeenCalled();
    await h.journal.drainChanged(true);
    expect(h.enqueue).toHaveBeenCalledOnce();
    expect(open.mock.calls.filter(([file]) => path.dirname(String(file)) === path.dirname(h.file(h.result(1).id))).every(([file]) => String(file) === h.file(h.result(1).id))).toBe(true);
    expect(read.mock.calls.some(([file]) => String(file).includes(hash(h.result(2).id)))).toBe(false);
  });

  it("retains a changed exact-owner path when the enqueue callback throws", async () => {
    const h = fixture(); h.seed(1, h.recipient); h.seed(2);
    h.enqueue.mockImplementationOnce(() => { throw new Error("inbox admission refused"); });
    await h.journal.drainChanged(); expect(h.journal.hasPendingChanges).toBe(true);
    const readdir = vi.spyOn(fs.promises, "readdir"), open = vi.spyOn(fs.promises, "open");
    await h.journal.drainChanged(); expect(h.enqueue).toHaveBeenCalledOnce();
    h.seed(3); h.journal.changed("envelopes", hash(h.result(3).id)); await h.journal.drainChanged();
    expect(h.enqueue).toHaveBeenCalledOnce(); // An unrelated completion is not retry admission.
    h.journal.changed("envelopes", hash(h.result(1).id)); await h.journal.drainChanged();
    expect(h.enqueue).toHaveBeenCalledTimes(2); expect(h.journal.hasPendingChanges).toBe(false); expect(readdir).not.toHaveBeenCalled();
    expect(open.mock.calls.some(([file]) => String(file) === h.file(h.result(2).id))).toBe(false);
  });

  it.skipIf(process.platform === "win32")("rechecks only known own canonical project aliases at the safety boundary", async () => {
    const h = fixture(), elsewhere = path.join(h.root, "elsewhere"), alias = path.join(h.root, "alias"); fs.mkdirSync(elsewhere); fs.symlinkSync(elsewhere, alias, "dir");
    h.seed(1, { ...h.recipient, projectRoot: alias }); h.seed(2);
    await h.journal.drainChanged(); expect(h.enqueue).not.toHaveBeenCalled();
    const open = vi.spyOn(fs.promises, "open"), read = vi.spyOn(fs.promises, "readFile"), readdir = vi.spyOn(fs.promises, "readdir");
    await h.journal.drainChanged(true, { safety: true });
    expect(open).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled(); expect(readdir).not.toHaveBeenCalled();
    fs.unlinkSync(alias); fs.symlinkSync(h.root, alias, "dir");
    await h.journal.drainChanged(true, { safety: true }); expect(h.enqueue).toHaveBeenCalledOnce();
    expect(open.mock.calls.some(([file]) => String(file) === h.file(h.result(2).id))).toBe(false);
  });

  it("observes producer death only for a known exact-addressed pending attempt", async () => {
    const h = fixture(), value = h.result(1), run = path.join(h.root, "run"); fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: process.pid } }));
    saveWorkerCompletion(path.join(run, "status.json"), value);
    await h.journal.drainChanged(); expect(h.journal.hasPendingAttempts).toBe(true); expect(h.enqueue).not.toHaveBeenCalled();
    const target = path.join(h.meshRoot, "agent-completions", "attempts", hash(value.id));
    const candidate = JSON.parse(fs.readFileSync(target, "utf8")); candidate.supervisor.pid = 2147483647;
    fs.writeFileSync(target, JSON.stringify(candidate));
    const readdir = vi.spyOn(fs.promises, "readdir");
    await h.journal.drainChanged(true, { retryPending: true });
    expect(readdir).not.toHaveBeenCalled(); expect(h.journal.hasPendingAttempts).toBe(false); expect(h.enqueue).toHaveBeenCalledOnce();
  });
});
