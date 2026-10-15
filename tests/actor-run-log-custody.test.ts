import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import * as privateDir from "../src/storage/private-dir.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import * as logTail from "../src/log-tail.js";
import { pruneActorRunArchives, pruneActorSessionBackups } from "../src/storage/retention.js";

// Decision C: archive custody/data-safety failure probes. Product pruning is recorded,
// never executed; fixtures remain under the canonical test TMPDIR for harness cleanup.
vi.mock("../src/storage/retention.js", async (original) => ({
  ...await original<typeof import("../src/storage/retention.js")>(),
  pruneActorRunArchives: vi.fn(),
  pruneActorSessionBackups: vi.fn(),
}));

const id = "a".repeat(32);
let root: string;
const event = '{"type":"fixture","text":"sole source"}\n';
beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-custody-v2-")); });
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks();
});
const fixture = (compressed = false) => {
  const source = path.join(root, "source");
  fs.mkdirSync(source, { mode: 0o700 });
  const actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: id };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 100 });
  const name = compressed ? "events.jsonl.zst" : "events.jsonl";
  // Copy tests intentionally treat the event artifact as opaque bytes; codec validity
  // belongs to the finalizer/reader integration tests, not a replacement codec here.
  fs.writeFileSync(path.join(source, name), event, { mode: 0o600 });
  fs.writeFileSync(path.join(source, "status.json"), JSON.stringify({ id, status: "completed", finishedAt: 1 }), { mode: 0o600 });
  const dest = path.join(root, "actor", "runs", id);
  return { source, actor, store, name, dest };
};

describe("actor mandatory run-log custody", () => {
  it.each([false, true])("durably copies and verifies mandatory events and status (compressed=%s)", async (compressed) => {
    const f = fixture(compressed);
    const write = vi.spyOn(privateDir, "writeArchiveFile");
    await f.store.retainRun(f.actor, id, f.source);
    expect(fs.readFileSync(path.join(f.dest, f.name), "utf8")).toBe(event);
    expect(fs.readFileSync(path.join(f.dest, "status.json"))).toEqual(fs.readFileSync(path.join(f.source, "status.json")));
    expect(write.mock.calls.filter(([, name]) => [f.name, "status.json"].includes(name)).map(([, name, , durable]) => [name, durable]))
      .toEqual([[f.name, true], ["status.json", true]]);
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
  });

  it("records pruning with both the actual latest-run and just-archived custody fences", async () => {
    const f = fixture();
    const latest = "b".repeat(32);
    await f.store.retainRun({ ...f.actor, lastRunId: latest }, id, f.source);
    const options = vi.mocked(pruneActorRunArchives).mock.calls.at(-1)![0];
    expect(options.latestRunId).toBe(latest);
    expect(options.isRetained?.(id)).toBe(true);
    expect(pruneActorSessionBackups).toHaveBeenCalledWith(f.actor.sessionFile);
    expect(fs.readFileSync(path.join(f.dest, f.name), "utf8")).toBe(event);
  });

  it("refuses an unsafe destination instead of resolving false archive success", async () => {
    const f = fixture(true);
    fs.mkdirSync(path.dirname(f.dest), { recursive: true });
    const foreign = path.join(root, "foreign"); fs.mkdirSync(foreign);
    fs.symlinkSync(foreign, f.dest, "dir");
    await expect(f.store.retainRun(f.actor, id, f.source)).rejects.toThrow();
    expect(fs.readdirSync(foreign)).toEqual([]);
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
  });

  it.each(["events.jsonl.zst", "status.json"])("rejects false mandatory copy of %s", async (target) => {
    const f = fixture(true);
    const write = privateDir.writeArchiveFile;
    vi.spyOn(privateDir, "writeArchiveFile").mockImplementation((held, name, content, durable) =>
      name === target ? false : write(held, name, content, durable));
    await expect(f.store.retainRun(f.actor, id, f.source)).rejects.toThrow();
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
  });

  it.each(["symlink", "hardlink", "foreign"])("refuses an unsafe %s mandatory destination without changing its victim", async (kind) => {
    const f = fixture(true);
    fs.mkdirSync(f.dest, { recursive: true });
    const victim = path.join(root, "victim"); fs.writeFileSync(victim, "victim");
    const entry = path.join(f.dest, f.name);
    if (kind === "symlink") fs.symlinkSync(victim, entry);
    else if (kind === "hardlink") fs.linkSync(victim, entry);
    else {
      fs.writeFileSync(entry, "foreign");
      const inode = fs.statSync(entry).ino, fstat = fs.fstatSync;
      vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) => {
        const stat = fstat(fd, options as never);
        return stat.ino === inode ? Object.assign(stat, { uid: (process.getuid?.() ?? 0) + 1 }) : stat;
      }) as typeof fs.fstatSync);
    }
    await expect(f.store.retainRun(f.actor, id, f.source)).rejects.toThrow();
    expect(fs.readFileSync(victim, "utf8")).toBe("victim");
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
  });

  it("rejects a failed durability barrier instead of certifying the archive", async () => {
    const f = fixture(true);
    vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("ENOSPC fsync"); });
    await expect(f.store.retainRun(f.actor, id, f.source)).rejects.toThrow(/fsync/);
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
  });

  it.each(["EACCES", "ENOSPC"])("keeps the source and allows a later successful retry after %s", async (code) => {
    const f = fixture(true);
    const write = privateDir.writeArchiveFile;
    const denied = vi.spyOn(privateDir, "writeArchiveFile").mockImplementation(() => { throw Object.assign(new Error(code), { code }); });
    await expect(f.store.retainRun(f.actor, id, f.source)).rejects.toThrow(code);
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
    denied.mockImplementation(write);
    await expect(f.store.retainRun(f.actor, id, f.source)).resolves.toBeUndefined();
    expect(fs.readFileSync(path.join(f.dest, f.name), "utf8")).toBe(event);
  });

  it("securely rejoins a complete prior archive after source disappearance, but rejects a fsync successor", async () => {
    const f = fixture();
    await f.store.retainRun(f.actor, id, f.source);
    await expect(f.store.retainRun(f.actor, id, undefined)).resolves.toBeUndefined();
    await expect(f.store.retainRun(f.actor, id, path.join(root, "already-cleaned"))).resolves.toBeUndefined();
    expect(fs.readFileSync(path.join(f.dest, f.name), "utf8")).toBe(event);

    // Recorded reader seam only: parent changes this spy to readRunLogPage with the
    // integration patch. No codec is implemented/mocked as a second parser here.
    const priorId = "b".repeat(32), prior = path.join(path.dirname(f.dest), priorId);
    fs.mkdirSync(prior);
    fs.writeFileSync(path.join(prior, "events.jsonl.zst"), "opaque complete reader fixture");
    fs.writeFileSync(path.join(prior, "status.json"), JSON.stringify({ id: priorId, status: "completed" }));
    const read = vi.spyOn(logTail, "readRunLogPage").mockImplementation(logical => {
      expect(logical).toMatch(/events\.jsonl$/);
      const stat = fs.statSync(path.join(prior, "events.jsonl.zst"), { bigint: true });
      return { lines: [], hasMore: false, generation: `${stat.dev}:${stat.ino}` };
    });
    await expect(f.store.retainRun(f.actor, priorId, undefined)).resolves.toBeUndefined();
    expect(read).toHaveBeenCalledTimes(1);
    read.mockRestore();

    const failedSync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("archive rejoin fsync failed"); });
    await expect(f.store.retainRun(f.actor, id, undefined)).rejects.toThrow(/rejoin fsync failed/);
    failedSync.mockRestore();
    const named = path.join(f.dest, f.name), inode = fs.statSync(named).ino, sync = fs.fsyncSync;
    let swapped = false;
    const swappingSync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      sync(fd);
      if (!swapped && fs.fstatSync(fd).ino === inode) {
        swapped = true;
        fs.renameSync(named, path.join(f.dest, "held-original"));
        fs.writeFileSync(named, event);
      }
    });
    await expect(f.store.retainRun(f.actor, id, undefined)).rejects.toThrow(/changed/);
    expect(swapped).toBe(true);
    expect(fs.readFileSync(path.join(f.dest, "held-original"), "utf8")).toBe(event);
    swappingSync.mockRestore();
    const originalRead = logTail.readRunLogPage;
    vi.spyOn(logTail, "readRunLogPage").mockImplementation((...args) => {
      const page = originalRead(...args);
      fs.appendFileSync(named, "changed during readback");
      return page;
    });
    await expect(f.store.retainRun(f.actor, id, undefined)).rejects.toThrow(/changed/);
  });

  it("never treats a source-less partial compressed/status pair as a verified prior archive", async () => {
    const f = fixture(true);
    fs.mkdirSync(f.dest, { recursive: true });
    fs.writeFileSync(path.join(f.dest, f.name), "partial zstd frame");
    fs.copyFileSync(path.join(f.source, "status.json"), path.join(f.dest, "status.json"));
    await expect(f.store.retainRun(f.actor, id, undefined)).rejects.toThrow();
    // The run-specific reader owns validation and typed errors. Exercise propagation
    // through its recorded seam; a plain-only empty page above is NOT codec proof.
    const codecError = Object.assign(new Error("partial zstd frame"), { name: "RunLogReadError" });
    const read = vi.spyOn(logTail, "readRunLogPage").mockImplementation(() => { throw codecError; });
    await expect(f.store.retainRun(f.actor, id, undefined)).rejects.toBe(codecError);
    expect(read).toHaveBeenCalledWith(expect.stringMatching(/events\.jsonl$/), 1);
    expect(fs.readFileSync(path.join(f.dest, f.name), "utf8")).toBe("partial zstd frame");
  });

  it("does not certify missing source/events/status", async () => {
    const f = fixture();
    await expect(f.store.retainRun(f.actor, id, undefined)).rejects.toThrow();
    await expect(f.store.retainRun(f.actor, id, path.join(root, "absent"))).rejects.toThrow();
    const noEvents = path.join(root, "no-events"); fs.mkdirSync(noEvents);
    fs.copyFileSync(path.join(f.source, "status.json"), path.join(noEvents, "status.json"));
    await expect(f.store.retainRun(f.actor, id, noEvents)).rejects.toThrow();
    const noStatus = path.join(root, "no-status"); fs.mkdirSync(noStatus);
    fs.writeFileSync(path.join(noStatus, "events.jsonl"), event);
    await expect(f.store.retainRun(f.actor, id, noStatus)).rejects.toThrow();
  });

  it.each(["symlink", "hardlink", "foreign"])("rejects %s source before copying authoritative bytes", async (kind) => {
    const f = fixture(true);
    const other = path.join(root, "other"); fs.mkdirSync(other);
    fs.copyFileSync(path.join(f.source, "status.json"), path.join(other, "status.json"));
    const file = path.join(other, f.name);
    if (kind === "symlink") fs.symlinkSync(path.join(f.source, f.name), file);
    else if (kind === "hardlink") fs.linkSync(path.join(f.source, f.name), file);
    else {
      fs.writeFileSync(file, event);
      const stat = fs.fstatSync;
      vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) => {
        const value = stat(fd, options as never);
        return value.isFile() ? Object.assign(value, { uid: (process.getuid?.() ?? 0) + 1 }) : value;
      }) as typeof fs.fstatSync);
    }
    await expect(f.store.retainRun(f.actor, id, other)).rejects.toThrow();
    expect(fs.existsSync(path.join(f.dest, f.name))).toBe(false);
  });

  it("rejects readback corruption and a source changed during the copy", async () => {
    const f = fixture(true);
    const write = privateDir.writeArchiveFile;
    const copying = vi.spyOn(privateDir, "writeArchiveFile").mockImplementation((held, name, content, durable) => {
      const result = write(held, name, content, durable);
      if (name === f.name) fs.writeFileSync(path.join(held.path, name), "corrupt");
      return result;
    });
    await expect(f.store.retainRun(f.actor, id, f.source)).rejects.toThrow();
    copying.mockImplementation((held, name, content, durable) => {
      if (name === f.name) fs.appendFileSync(path.join(f.source, name), "changed");
      return write(held, name, content, durable);
    });
    // A separate fresh destination avoids any replacement/deletion in this local probe.
    const other = { ...f.actor, sessionFile: path.join(root, "other-actor", "session.jsonl") };
    await expect(f.store.retainRun(other, id, f.source)).rejects.toThrow();
    expect(fs.existsSync(path.join(f.source, f.name))).toBe(true);
  });

  it.each(["source", "destination"])("rejects %s directory replacement during an otherwise successful copy", async (target) => {
    const f = fixture(true), write = privateDir.writeArchiveFile;
    const moved = path.join(root, "moved");
    vi.spyOn(privateDir, "writeArchiveFile").mockImplementation((held, name, content, durable) => {
      const result = write(held, name, content, durable);
      if (name === "status.json") {
        const directory = target === "source" ? f.source : f.dest;
        fs.renameSync(directory, moved);
        fs.mkdirSync(directory);
      }
      return result;
    });
    await expect(f.store.retainRun(f.actor, id, f.source)).rejects.toThrow(/directory changed/);
    const preserved = target === "source" ? moved : f.source;
    expect(fs.readFileSync(path.join(preserved, f.name), "utf8")).toBe(event);
  });

  it("preserves both representations of an interrupted compression without promoting zst over plain", async () => {
    const f = fixture();
    fs.writeFileSync(path.join(f.source, "events.jsonl.zst"), "partial compressed target");
    await f.store.retainRun(f.actor, id, f.source);
    expect(fs.readFileSync(path.join(f.dest, "events.jsonl"), "utf8")).toBe(event);
    expect(fs.readFileSync(path.join(f.dest, "events.jsonl.zst"), "utf8")).toBe("partial compressed target");
    expect(fs.readFileSync(path.join(f.source, "events.jsonl.zst"), "utf8")).toBe("partial compressed target");
  });
});

const managerFixture = async () => {
  const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
  // No worker is launched: run/finalize/commit/cleanup are fixture boundaries.
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0 }, { runRoot: path.join(root, "agent-runs") });
  closers.push(() => agents.close());
  const actors = new ActorManager("custody", { id: "session:custody", sessionId: "custody", name: "main", kind: "main" },
    mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, { actorRoot: path.join(root, "actors"), persistent: true });
  closers.push(() => actors.close());
  const actor = await actors.create({ name: "custody", instructions: "Fixture only.", responseMode: "text", model: "test/model" });
  const cleanup = vi.spyOn(agents, "cleanup").mockResolvedValue({ cleaned: true });
  const commit = vi.spyOn(agents, "commitActorArchive").mockResolvedValue();
  vi.spyOn(agents, "actorArchiveSources").mockReturnValue(new Map());
  return { actors, agents, actor, cleanup, commit };
};

describe("actor owner finalizer and reader boundary", () => {
  it("awaits finalization after locating the source but before archive writes, commit and recorded cleanup", async () => {
    const f = fixture(true), m = await managerFixture();
    let finalized = false;
    const trace: string[] = [];
    const finalize = vi.fn(async () => { await Promise.resolve(); finalized = true; trace.push("finalized"); });
    Object.assign(m.agents, { finalizeRunLog: finalize });
    vi.spyOn(m.agents, "runDirectory").mockImplementation(() => {
      trace.push("select");
      return f.source;
    });
    const write = privateDir.writeArchiveFile;
    vi.spyOn(privateDir, "writeArchiveFile").mockImplementation((...args) => {
      expect(finalized).toBe(true);
      trace.push(`copy:${args[1]}`);
      return write(...args);
    });
    m.commit.mockImplementation(async () => {
      trace.push("commit");
      expect(fs.readFileSync(path.join(m.actor.logDir!, id, f.name), "utf8")).toBe(event);
      expect(fs.existsSync(path.join(m.actor.logDir!, id, "status.json"))).toBe(true);
    });
    m.cleanup.mockImplementation(async () => { trace.push("cleanup"); return { cleaned: true }; });
    vi.spyOn(m.agents, "run").mockResolvedValue({ id, status: "completed", text: "fixture", toolCalls: 0, usage: {} } as never);
    await m.actors.ask(m.actor.id, "fixture");
    await m.actors.close();
    expect(finalize).toHaveBeenCalledWith(id);
    expect(trace).toEqual(["select", "finalized", "copy:events.jsonl.zst", "copy:status.json", "commit", "cleanup"]);
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
  });

  it.each(["live", "recovered"])("a rejected %s source finalizer keeps pending custody and prevents copy, commit and cleanup", async (kind) => {
    const f = fixture(true), m = await managerFixture();
    const finalize = vi.fn(async () => { throw new Error("writer exit unconfirmed"); });
    Object.assign(m.agents, { finalizeRunLog: finalize });
    const select = vi.spyOn(m.agents, "runDirectory").mockReturnValue(kind === "live" ? f.source : undefined);
    if (kind === "recovered") vi.mocked(m.agents.actorArchiveSources).mockReturnValue(new Map([[id, f.source]]));
    const write = vi.spyOn(privateDir, "writeArchiveFile");
    vi.spyOn(m.agents, "run").mockResolvedValue({ id, status: "completed", text: "fixture", toolCalls: 0, usage: {} } as never);
    await m.actors.ask(m.actor.id, "fixture");
    await m.actors.close();
    expect(finalize).toHaveBeenCalledTimes(2); // activation + pending-custody retry at close
    expect(select).toHaveBeenCalledTimes(2);
    expect(write).not.toHaveBeenCalled();
    expect(m.commit).not.toHaveBeenCalled();
    expect(m.cleanup).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
  });

  it.each(["undefined", "known-missing"])("rejoins a verified already-cleaned previous run with %s source without calling its finalizer", async (kind) => {
    const f = fixture(), m = await managerFixture(), nextId = "b".repeat(32);
    const next = path.join(root, "next-source"); fs.mkdirSync(next);
    fs.writeFileSync(path.join(next, "events.jsonl"), "next log\n");
    fs.writeFileSync(path.join(next, "status.json"), JSON.stringify({ id: nextId, status: "completed" }));
    let cleaned = false;
    const finalize = vi.fn(async (runId: string) => {
      if (runId === id && cleaned) throw new Error("already-cleaned ID has no terminal writer receipt");
    });
    Object.assign(m.agents, { finalizeRunLog: finalize });
    vi.spyOn(m.agents, "runDirectory").mockImplementation(runId => runId === nextId ? next : cleaned ? undefined : f.source);
    vi.mocked(m.agents.actorArchiveSources).mockImplementation(() => kind === "known-missing" && cleaned
      ? new Map([[id, path.join(root, "already-cleaned")]]) : new Map());
    m.cleanup.mockImplementation(async runId => { if (runId === id) cleaned = true; return { cleaned: true }; });
    vi.spyOn(m.agents, "run")
      .mockResolvedValueOnce({ id, status: "completed", text: "first", toolCalls: 0, usage: {} } as never)
      .mockResolvedValueOnce({ id: nextId, status: "completed", text: "next", toolCalls: 0, usage: {} } as never);
    const read = vi.spyOn(logTail, "readRunLogPage");
    await m.actors.ask(m.actor.id, "first");
    await m.actors.ask(m.actor.id, "next");
    await m.actors.close();
    expect(finalize.mock.calls.map(([runId]) => runId)).toEqual([id, nextId]);
    expect(m.commit.mock.calls.map(([runId]) => runId)).toEqual([id, nextId]);
    expect(m.cleanup.mock.calls.map(([runId]) => runId)).toEqual([id, id, nextId]);
    expect(read.mock.calls.some(([file, lines]) => lines === 1 && String(file).endsWith("events.jsonl"))).toBe(true);
    expect(fs.readFileSync(path.join(m.actor.logDir!, id, "events.jsonl"), "utf8")).toBe(event);
  });

  it.each(["undefined", "known-missing"])("never discharges %s source custody for a missing or broken prior archive", async (kind) => {
    const m = await managerFixture();
    const finalize = vi.fn(async () => { throw new Error("already-cleaned ID has no terminal writer receipt"); });
    Object.assign(m.agents, { finalizeRunLog: finalize });
    vi.spyOn(m.agents, "runDirectory").mockReturnValue(undefined);
    if (kind === "known-missing") vi.mocked(m.agents.actorArchiveSources).mockReturnValue(new Map([[id, path.join(root, "already-cleaned")]]));
    const retain = vi.spyOn(ActorLogStore.prototype, "retainRun");
    vi.spyOn(m.agents, "run").mockResolvedValue({ id, status: "completed", text: "fixture", toolCalls: 0, usage: {} } as never);
    await m.actors.ask(m.actor.id, "missing archive");
    // A later retry gets an independently readable event but invalid terminal status.
    const archived = path.join(m.actor.logDir!, id); fs.mkdirSync(archived, { recursive: true });
    fs.writeFileSync(path.join(archived, "events.jsonl"), event);
    fs.writeFileSync(path.join(archived, "status.json"), JSON.stringify({ id, actorId: m.actor.id, status: "running" }));
    await m.actors.close();
    expect(finalize).not.toHaveBeenCalled();
    expect(retain).toHaveBeenCalledTimes(2);
    await expect(retain.mock.results[0]!.value).rejects.toThrow(/no verified archive/);
    await expect(retain.mock.results[1]!.value).rejects.toThrow(/ownership\/terminal/);
    expect(m.commit).not.toHaveBeenCalled();
    expect(m.cleanup).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(archived, "events.jsonl"), "utf8")).toBe(event);
  });

  it.each(["symlink", "denied"])("never substitutes a verified prior archive for a %s source", async (kind) => {
    const f = fixture(), m = await managerFixture();
    const archived = path.join(m.actor.logDir!, id); fs.mkdirSync(archived, { recursive: true });
    fs.writeFileSync(path.join(archived, "events.jsonl"), event);
    fs.writeFileSync(path.join(archived, "status.json"), JSON.stringify({ id, actorId: m.actor.id, status: "completed" }));
    let source = f.source;
    if (kind === "symlink") {
      source = path.join(root, "source-alias"); fs.symlinkSync(f.source, source, "dir");
    } else {
      const lstat = fs.lstatSync;
      vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
        if (String(file) === source) throw Object.assign(new Error("EACCES source"), { code: "EACCES" });
        return lstat(file, options as never);
      }) as typeof fs.lstatSync);
    }
    Object.assign(m.agents, { finalizeRunLog: vi.fn(async () => {}) });
    vi.spyOn(m.agents, "runDirectory").mockReturnValue(source);
    vi.spyOn(m.agents, "run").mockResolvedValue({ id, status: "completed", text: "fixture", toolCalls: 0, usage: {} } as never);
    const write = vi.spyOn(privateDir, "writeArchiveFile"), read = vi.spyOn(logTail, "readRunLogPage");
    await m.actors.ask(m.actor.id, "fixture");
    await m.actors.close();
    expect(write).not.toHaveBeenCalled();
    expect(read).not.toHaveBeenCalled();
    expect(m.commit).not.toHaveBeenCalled();
    expect(m.cleanup).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.source, f.name), "utf8")).toBe(event);
    expect(fs.readFileSync(path.join(archived, "events.jsonl"), "utf8")).toBe(event);
  });

  it("a failed current archive stays pending while a valid prior archive can still be cleaned", async () => {
    const current = fixture(true), m = await managerFixture();
    const priorId = "b".repeat(32), prior = path.join(root, "prior-source");
    fs.mkdirSync(prior);
    fs.writeFileSync(path.join(prior, "events.jsonl"), "prior log\n");
    fs.writeFileSync(path.join(prior, "status.json"), JSON.stringify({ id: priorId, status: "completed" }));
    Object.assign(m.agents, { finalizeRunLog: vi.fn(async () => {}) });
    vi.spyOn(m.agents, "runDirectory").mockImplementation(runId => runId === id ? current.source : prior);
    const pendingSources = new Map([[priorId, prior]]);
    vi.mocked(m.agents.actorArchiveSources).mockReturnValue(pendingSources);
    m.commit.mockImplementation(async runId => { pendingSources.delete(runId); });
    vi.spyOn(m.agents, "run").mockResolvedValue({ id, status: "completed", text: "fixture", toolCalls: 0, usage: {} } as never);
    const write = privateDir.writeArchiveFile;
    vi.spyOn(privateDir, "writeArchiveFile").mockImplementation((held, name, content, durable) => {
      if (name === current.name) throw new Error("ENOSPC current events");
      return write(held, name, content, durable);
    });
    await m.actors.ask(m.actor.id, "fixture");
    await m.actors.close(); // retries the still-pending current run without a real cleanup
    expect(m.commit.mock.calls.map(([runId]) => runId)).toEqual([priorId]);
    expect(m.cleanup.mock.calls.map(([runId]) => runId)).toEqual([priorId]);
    expect(fs.readFileSync(path.join(current.source, current.name), "utf8")).toBe(event);
    expect(fs.readFileSync(path.join(m.actor.logDir!, priorId, "events.jsonl"), "utf8")).toBe("prior log\n");
  });

  it("checks compressed physical containment but passes the common reader the logical plain path", async () => {
    const m = await managerFixture();
    const run = path.join(m.actor.logDir!, id); fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, actorId: m.actor.id, status: "completed" }));
    fs.writeFileSync(path.join(run, "events.jsonl.zst"), "opaque reader fixture");
    const read = vi.spyOn(logTail, "readRunLogPage").mockReturnValue({ lines: [{ offset: 0, raw: event.trim() }], hasMore: false, generation: "physical:zst" });
    expect(m.actors.readLog(m.actor.id, { type: "run", runId: id }).run?.events).toEqual([{ offset: 0, raw: event.trim() }]);
    expect(read).toHaveBeenCalledWith(path.join(run, "events.jsonl"), 200, undefined, undefined, undefined);
    // Containment is independent of codec behavior, and is checked before any read.
    const outside = path.join(root, "outside.zst"); fs.writeFileSync(outside, "foreign");
    const other = "c".repeat(32), otherRun = path.join(m.actor.logDir!, other); fs.mkdirSync(otherRun);
    fs.writeFileSync(path.join(otherRun, "status.json"), JSON.stringify({ id: other, actorId: m.actor.id }));
    fs.symlinkSync(outside, path.join(otherRun, "events.jsonl.zst"));
    read.mockClear();
    expect(() => m.actors.readLog(m.actor.id, { type: "run", runId: other })).toThrow(/outside|unsafe|symbolic/i);
    expect(read).not.toHaveBeenCalled();

    // A status successor after the lexical/physical check must not be followed.
    const status = path.join(run, "status.json"), outsideStatus = path.join(root, "outside-status.json");
    fs.copyFileSync(status, outsideStatus);
    const open = fs.openSync;
    let swapped = false;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: string | number, ...rest: unknown[]) => {
      if (String(file) === status && !swapped) {
        swapped = true;
        fs.renameSync(status, path.join(run, "held-status.json"));
        fs.symlinkSync(outsideStatus, status);
      }
      return (open as (...args: unknown[]) => number)(file, flags, ...rest);
    }) as typeof fs.openSync);
    expect(() => m.actors.readLog(m.actor.id, { type: "run", runId: id })).toThrow();
    expect(swapped).toBe(true);
    expect(read).not.toHaveBeenCalled();
    expect(JSON.parse(fs.readFileSync(outsideStatus, "utf8")).actorId).toBe(m.actor.id);
  });

  it("uses plain in a dual representation and never falls back after a non-ENOENT plain error", async () => {
    const m = await managerFixture();
    const run = path.join(m.actor.logDir!, id); fs.mkdirSync(run, { recursive: true });
    const plain = path.join(run, "events.jsonl");
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, actorId: m.actor.id }));
    fs.writeFileSync(plain, event);
    const outside = path.join(root, "outside"); fs.writeFileSync(outside, "partial");
    fs.symlinkSync(outside, path.join(run, "events.jsonl.zst"));
    expect(m.actors.readLog(m.actor.id, { type: "run", runId: id }).run?.events[0]?.raw).toBe(event.trim());
    const lstat = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      if (String(file) === plain) throw Object.assign(new Error("EACCES plain"), { code: "EACCES" });
      return lstat(file, options as never);
    }) as typeof fs.lstatSync);
    expect(() => m.actors.readLog(m.actor.id, { type: "run", runId: id })).toThrow(/EACCES plain/);
  });

  it("rejects wrong status attribution on an otherwise valid compressed archive path", async () => {
    const m = await managerFixture();
    const run = path.join(m.actor.logDir!, id); fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, actorId: "different-actor", status: "completed" }));
    fs.writeFileSync(path.join(run, "events.jsonl.zst"), "owned, single-link fixture");
    expect(fs.realpathSync(run)).toBe(run);
    expect(() => m.actors.readLog(m.actor.id, { type: "run", runId: id })).toThrow(/does not belong/);
    expect(() => m.actors.readLog(m.actor.id, { type: "run", runId: "../outside" })).toThrow(/Invalid retained run ID/);
    expect(() => m.actors.readLog(m.actor.id, { type: "run", runId: "d".repeat(32) })).toThrow(/not retained/);

    // The source-less archive join has the same attribution fence, independent of
    // readLog's receiver checks. Missing target identity cannot waive explicit attribution.
    const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 100 });
    const target = { sessionFile: path.join(m.actor.logDir!, "..", "session.jsonl") };
    // Make event readability independently valid: rejection must be attribution,
    // not the still-absent compressed decoder in this isolated worktree.
    fs.writeFileSync(path.join(run, "events.jsonl"), event);
    await expect(store.retainRun({ ...target, id: m.actor.id }, id, undefined)).rejects.toThrow(/ownership\/terminal/);
    await expect(store.retainRun(target, id, undefined)).rejects.toThrow(/ownership\/terminal/);
    for (const record of [{ id: "e".repeat(32), actorId: m.actor.id, status: "completed" },
      { id, actorId: m.actor.id, status: "running" }, { id, actorId: m.actor.id, transport: "hosted" }]) {
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record));
      await expect(store.retainRun({ ...target, id: m.actor.id }, id, undefined)).rejects.toThrow(/ownership\/terminal/);
    }
  });
});
