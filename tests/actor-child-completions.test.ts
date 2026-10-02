import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import type { AgentRunResult } from "../src/agents/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-child-completions-"));
  roots.push(root);
  const sessionFile = path.join(root, "session.jsonl");
  fs.writeFileSync(sessionFile, "");
  const store = new ActorChildCompletionStore(sessionFile);
  const spawner = { id: "actor:review", kind: "actor" as const, runId: "a".repeat(32) };
  const result = (id = "b".repeat(32)) => ({
    id, name: "review", status: "completed", text: "full result", startedAt: Date.now(), finishedAt: Date.now(),
    value: { private: true },
  } as AgentRunResult);
  return { root, store, spawner, result, sessionFile };
};

describe("actor child completion handoff storage", () => {
  it.each([true, false])("removes the archive but retains durable foreground consumption evidence (notify=%s)", (notify) => {
    const h = setup();
    const result = h.result();
    h.store.enqueue(result, h.spawner, notify);
    expect(fs.existsSync(h.store.resultFile(result.id))).toBe(true);
    h.store.discard(result.id);
    expect(fs.readdirSync(h.store.directory)).toEqual([`${result.id}.receipt`]);
  });

  it("does not archive a terminal status consumed before the settle event arrives", () => {
    const h = setup();
    const result = h.result();
    h.store.discard(result.id);
    h.store.enqueue(result, h.spawner);
    expect(fs.readdirSync(h.store.directory)).toEqual([`${result.id}.receipt`]);
  });

  it("retains an unread full outcome through handoff, then deletes it on consumption", () => {
    const h = setup();
    const result = h.result();
    h.store.enqueue(result, h.spawner);
    expect(h.store.pending()).toHaveLength(1);
    h.store.acknowledge(result.id, { handoff: true });
    expect(h.store.pending()).toEqual([]);
    expect(JSON.parse(fs.readFileSync(h.store.resultFile(result.id), "utf8"))).toMatchObject(result);
    h.store.releaseResult(result.id);
    expect(fs.existsSync(h.store.resultFile(result.id))).toBe(false);
    expect(h.store.received(result.id)).toBe(true);
  });

  it("keeps only a receipt, not a full archive, after live consumption", () => {
    const h = setup();
    const result = h.result();
    h.store.enqueue(result, h.spawner);
    h.store.consume(result.id);
    expect(fs.existsSync(h.store.resultFile(result.id))).toBe(false);
    expect(h.store.pending()).toEqual([]);
    expect(fs.readdirSync(h.store.directory)).toEqual([`${result.id}.receipt`]);
  });

  it("claims a live batch atomically without deleting its full outcomes before delivery", () => {
    const h = setup();
    const a = h.result();
    const b = h.result("c".repeat(32));
    for (const result of [a, b]) { h.store.enqueue(result, h.spawner); h.store.prepareLive(result.id); }
    h.store.consumeLiveBatch([a.id, b.id]);
    const restarted = new ActorChildCompletionStore(h.sessionFile);
    for (const result of [a, b]) {
      expect(restarted.received(result.id)).toBe(true);
      expect(JSON.parse(fs.readFileSync(restarted.resultFile(result.id), "utf8"))).toMatchObject(result);
      restarted.acknowledge(result.id);
      expect(fs.existsSync(restarted.resultFile(result.id))).toBe(false);
    }
    restarted.prune(1, Date.now() + 10);
    expect(fs.readdirSync(restarted.directory)).toEqual([]);
  });

  it("a second staged receipt failure leaves the entire unsent live batch recoverable after restart", () => {
    const h = setup();
    const a = h.result();
    const b = h.result("c".repeat(32));
    for (const result of [a, b]) h.store.enqueue(result, h.spawner);
    const rename = fs.renameSync;
    const failed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === path.join(h.store.directory, `${b.id}.receipt`)) throw new Error("second staged receipt failed");
      rename(from, to);
    });
    expect(() => h.store.consumeLiveBatch([a.id, b.id])).toThrow("second staged receipt failed");
    failed.mockRestore();
    const restarted = new ActorChildCompletionStore(h.sessionFile);
    expect(restarted.pending()).toHaveLength(2);
    for (const result of [a, b]) {
      expect(restarted.received(result.id)).toBe(false);
      expect(JSON.parse(fs.readFileSync(restarted.resultFile(result.id), "utf8"))).toMatchObject(result);
    }
    restarted.consumeLiveBatch([a.id, b.id]);
    expect(restarted.pending()).toEqual([]);
    for (const result of [a, b]) restarted.acknowledge(result.id);
  });

  it("does not read the session for in-flight envelopes across 100 polls", () => {
    const h = setup();
    const result = h.result();
    h.store.enqueue(result, h.spawner);
    const read = vi.spyOn(fs, "readFileSync");
    for (let i = 0; i < 100; i++) expect(h.store.pending({ actorId: h.spawner.id, inFlightRunId: h.spawner.runId })).toEqual([]);
    expect(read.mock.calls.filter(([file]) => file === h.sessionFile)).toHaveLength(0);
    fs.appendFileSync(h.sessionFile, JSON.stringify({ type: "custom_message", customType: "pi-fabric-agent-complete", details: { ids: [result.id] } }) + "\n");
    expect(h.store.pending({ actorId: h.spawner.id })).toEqual([]);
    expect(read.mock.calls.filter(([file]) => file === h.sessionFile)).toHaveLength(1);
    expect(fs.existsSync(h.store.resultFile(result.id))).toBe(false);
  });

  it("reads the committed-id snapshot at most once per eligible handoff across 100 retries", () => {
    const h = setup();
    h.store.enqueue(h.result(), h.spawner);
    const read = vi.spyOn(fs, "readFileSync");
    for (let i = 0; i < 100; i++) expect(h.store.pending({ actorId: h.spawner.id })).toHaveLength(1);
    expect(read.mock.calls.filter(([file]) => file === h.sessionFile)).toHaveLength(1);
    h.store.enqueue(h.result("c".repeat(32)), { ...h.spawner, runId: "d".repeat(32) });
    expect(h.store.pending({ actorId: h.spawner.id })).toHaveLength(2);
    expect(read.mock.calls.filter(([file]) => file === h.sessionFile)).toHaveLength(2);
  });

  it("does not rescan a committed handoff when cleanup fails", () => {
    const h = setup();
    const result = h.result();
    h.store.enqueue(result, h.spawner);
    fs.appendFileSync(h.sessionFile, JSON.stringify({ type: "message", message: { customType: "pi-fabric-agent-complete", details: { ids: [result.id] } } }) + "\n");
    const read = vi.spyOn(fs, "readFileSync");
    const acknowledge = vi.spyOn(h.store, "acknowledge").mockImplementation(() => { throw new Error("I/O failure"); });
    for (let i = 0; i < 100; i++) expect(h.store.pending()).toEqual([]);
    expect(acknowledge).toHaveBeenCalledTimes(100);
    expect(read.mock.calls.filter(([file]) => file === h.sessionFile)).toHaveLength(1);
    acknowledge.mockRestore();
    expect(h.store.pending()).toEqual([]);
    expect(fs.existsSync(h.store.resultFile(result.id))).toBe(false);
  });

  it("applies the configured retention TTL to archives, envelopes and receipts", () => {
    const h = setup();
    const unread = h.result();
    const muted = h.result("c".repeat(32));
    const live = h.result("d".repeat(32));
    const handed = h.result("e".repeat(32));
    const recent = h.result("f".repeat(32));
    h.store.enqueue(unread, h.spawner);
    h.store.enqueue(muted, h.spawner, false);
    h.store.enqueue(live, h.spawner);
    h.store.acknowledge(live.id);
    h.store.enqueue(handed, h.spawner);
    h.store.acknowledge(handed.id, { handoff: true });
    const now = Date.now();
    for (const file of fs.readdirSync(h.store.directory)) fs.utimesSync(path.join(h.store.directory, file), new Date(now - 2000), new Date(now - 2000));
    h.store.enqueue(recent, h.spawner, false);
    fs.writeFileSync(path.join(h.store.directory, "unrelated.txt"), "do not delete");
    h.store.prune(1000, now, new Set([handed.id]));
    expect(fs.readdirSync(h.store.directory).sort()).toEqual([`${handed.id}.receipt`, `${handed.id}.result.json`, `${recent.id}.result.json`, "unrelated.txt"].sort());
    h.store.releaseResult(handed.id);
    h.store.prune(1000, now + 2000);
    expect(fs.readdirSync(h.store.directory)).toEqual(["unrelated.txt"]);
  });
});
