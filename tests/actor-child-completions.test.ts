import { execFile } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import * as atomicWrites from "../src/core/atomic-write.js";
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

// Windows omits unsupported directory fsync. Exercise the same uncertain writer
// rejection there, while Unix probes keep their physical post-rename fsync fault.
const failPostRenameOnWindows = (target: (file: string) => boolean, fail: () => void) => {
  if (process.platform !== "win32") return;
  const write = atomicWrites.writeJsonAtomic;
  return vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, value, options) => {
    write(file, value, options);
    if (target(file)) fail();
  });
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

  it.each([false, true])("retries a persisted abandonment after restart without making a finalized observation unread (finalized=%s)", finalized => {
    const h = setup(); const result = h.result();
    h.store.enqueue(result, h.spawner);
    h.store.consume(result.id, { handoff: true, publication: true });
    const write = atomicWrites.writeJsonAtomic;
    const blocked = vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file.endsWith(".receipt") && (value as { unread?: boolean }).unread) throw new Error("rollback lock/storage unavailable");
      return write(file, value, options);
    });
    expect(() => h.store.abandonForeground(result.id)).toThrow("rollback lock/storage unavailable");
    blocked.mockRestore();
    if (finalized) h.store.discard(result.id);
    const next = new ActorChildCompletionStore(h.sessionFile);
    expect(next.pending()).toHaveLength(finalized ? 0 : 1);
    expect(next.received(result.id)).toBe(finalized);
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

  it("claims a completion exclusively across separate live and mailbox owner processes", async () => {
    const h = setup();
    const result = h.result();
    h.store.enqueue(result, h.spawner);
    const gate = path.join(h.root, "start-claims");
    const script = (role: "live" | "mailbox") => `
      import fs from "node:fs";
      import { ActorChildCompletionStore, ChildCompletionClaimLostError } from ${JSON.stringify(path.resolve("src/actors/child-completions.ts"))};
      const store = new ActorChildCompletionStore(${JSON.stringify(h.sessionFile)});
      fs.writeFileSync(${JSON.stringify(path.join(h.root, `${role}-ready`))}, "ready");
      const wait = new Int32Array(new SharedArrayBuffer(4));
      while (!fs.existsSync(${JSON.stringify(gate)})) Atomics.wait(wait, 0, 0, 5);
      try {
        ${role === "live" ? `store.consumeLiveBatch([${JSON.stringify(result.id)}]);` : `store.acknowledge(${JSON.stringify(result.id)}, { handoff: true });`}
        console.log("claimed");
      } catch (error) {
        if (!(error instanceof ChildCompletionClaimLostError)) throw error;
        console.log("lost");
      }
    `;
    const claims = (["live", "mailbox"] as const).map((role) => new Promise<string>((resolve, reject) => {
      execFile("bun", ["--eval", script(role)], { timeout: 10000, maxBuffer: 4096 }, (error, stdout) => {
        if (error) reject(error); else resolve(stdout.trim());
      });
    }));
    try {
      await vi.waitFor(() => {
        for (const role of ["live", "mailbox"]) expect(fs.existsSync(path.join(h.root, `${role}-ready`))).toBe(true);
      }, { timeout: 5000 });
      fs.writeFileSync(gate, "go");
      expect((await Promise.all(claims)).sort()).toEqual(["claimed", "lost"]);
      expect(h.store.received(result.id)).toBe(true);
      expect(JSON.parse(fs.readFileSync(h.store.resultFile(result.id), "utf8"))).toMatchObject(result);
    } finally {
      fs.writeFileSync(gate, "go");
      await Promise.allSettled(claims); // Never leave a test-owned worker behind.
    }
  }, 15000);

  it("Q6 retries every foreground durability barrier after a post-rename failure", () => {
    const h = setup();
    const result = h.result();
    h.store.enqueue(result, h.spawner);
    const rename = fs.renameSync;
    const sync = fs.fsyncSync;
    let renamed = false;
    let blocked = true;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (to === path.join(h.store.directory, `${result.id}.receipt`)) renamed = true;
    });
    const barriers = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      if (renamed && blocked && fs.fstatSync(fd).isDirectory()) throw new Error("foreground barrier failed after rename");
      sync(fd);
    });
    failPostRenameOnWindows((file) => file === path.join(h.store.directory, `${result.id}.receipt`), () => {
      if (blocked) throw new Error("foreground barrier failed after rename");
    });
    expect(() => h.store.consume(result.id, { handoff: true })).toThrow("foreground barrier failed after rename");
    expect(fs.existsSync(path.join(h.store.directory, `${result.id}.receipt`))).toBe(true);
    expect(() => h.store.consume(result.id, { handoff: true })).toThrow("foreground barrier failed after rename");
    blocked = false;
    const before = barriers.mock.calls.length;
    h.store.consume(result.id, { handoff: true });
    expect(barriers.mock.calls.length).toBeGreaterThan(before);
    const restarted = new ActorChildCompletionStore(h.sessionFile);
    expect(restarted.received(result.id)).toBe(true);
    expect(restarted.pending()).toEqual([]);
  });

  it("Q4 withdraws an unsent live batch after its post-rename barrier fails", () => {
    const h = setup();
    const a = h.result();
    const b = h.result("c".repeat(32));
    for (const result of [a, b]) h.store.enqueue(result, h.spawner);
    const rename = fs.renameSync;
    const sync = fs.fsyncSync;
    let renamed = false;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to).endsWith(".live-receipt")) renamed = true;
    });
    const failure = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      if (renamed && fs.fstatSync(fd).isDirectory()) throw new Error("live batch barrier failed after rename");
      sync(fd);
    });
    const windowsFailure = failPostRenameOnWindows((file) => file.endsWith(".live-receipt"), () => { throw new Error("live batch barrier failed after rename"); });
    expect(() => h.store.consumeLiveBatch([a.id, b.id])).toThrow("live batch barrier failed after rename");
    failure.mockRestore();
    windowsFailure?.mockRestore();
    const restarted = new ActorChildCompletionStore(h.sessionFile);
    expect(restarted.pending().map(({ result }) => result.id).sort()).toEqual([a.id, b.id].sort());
    restarted.consumeLiveBatch([a.id, b.id]);
    expect(restarted.pending()).toEqual([]);
    for (const result of [a, b]) restarted.acknowledge(result.id);
    expect(new ActorChildCompletionStore(h.sessionFile).pending()).toEqual([]);
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
