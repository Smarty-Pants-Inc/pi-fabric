import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { discoverActorArchiveSources } from "../src/agents/actor-archive-discovery.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { MeshStore } from "../src/mesh/store.js";
import type { AgentRunResult } from "../src/agents/types.js";

// Keep: recorded 36,427-entry incident + raw-trace/archive custody data safety.
// Fixtures and raw sentinels are deliberately retained. Commit's metadata removal is recorded, never performed.
afterEach(() => vi.restoreAllMocks());
const actorId = "archive-owner";
const id = (value: number) => value.toString(16).padStart(32, "0");
function fixture() {
  const root = fs.mkdtempSync(path.join(process.env.PI_FABRIC_ARCHIVE_TEST_ROOT ?? os.tmpdir(), "actor-archive-discovery-test-"));
  const runRoot = path.join(root, "runs"), sessionFile = path.join(root, "session.jsonl");
  fs.mkdirSync(runRoot, { mode: 0o700 });
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0 }, { runRoot });
  const source = (runId: string, overrides: Record<string, unknown> = {}) => {
    const directory = path.join(runRoot, runId);
    fs.mkdirSync(directory, { mode: 0o700 });
    fs.writeFileSync(path.join(directory, "actor-run-archive-pending.json"), JSON.stringify({ format: 1, runId, actorId, sessionFile, ...overrides }), { mode: 0o600 });
    fs.writeFileSync(path.join(directory, "events.jsonl"), `raw trace ${runId}\n`, { mode: 0o600 });
    return directory;
  };
  return { root, runRoot, sessionFile, agents, source };
}

describe("actor archive source discovery", () => {
  it("uses the real manager entry point without synchronous filesystem discovery", async () => {
    const f = fixture(), directory = f.source(id(1));
    for (const method of ["readdirSync", "opendirSync", "lstatSync", "statSync", "readFileSync"] as const) {
      vi.spyOn(fs, method).mockImplementation(() => { throw new Error(`synchronous discovery: ${method}`); });
    }
    const pending = f.agents.actorArchiveSources(actorId, f.sessionFile);
    expect(pending).toBeInstanceOf(Promise);
    expect(await pending).toEqual(new Map([[id(1), directory]]));
  });

  it("performs no synchronous filesystem discovery in the asynchronous helper", async () => {
    const f = fixture(), directory = f.source(id(1));
    for (const method of ["readdirSync", "opendirSync", "lstatSync", "statSync", "readFileSync"] as const) {
      vi.spyOn(fs, method).mockImplementation(() => { throw new Error(`synchronous discovery: ${method}`); });
    }
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
  });

  it("validates exact fenced sources through the asynchronous helper without altering raw traces", async () => {
    const f = fixture(), directory = f.source(id(1));
    const trace = fs.readFileSync(path.join(directory, "events.jsonl"), "utf8");
    f.source(id(2), { actorId: "other" }); f.source(id(3), { sessionFile: `${f.sessionFile}.other` });
    f.source(id(4), { runId: id(5) }); f.source(id(5), { format: 2 });
    const malformed = f.source(id(6)), large = f.source(id(7));
    fs.writeFileSync(path.join(malformed, "actor-run-archive-pending.json"), "not JSON");
    fs.writeFileSync(path.join(large, "actor-run-archive-pending.json"), " ".repeat(4097));
    f.source("not-a-run-id");
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
    expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe(trace);
    expect(fs.existsSync(path.join(directory, "actor-run-archive-pending.json"))).toBe(true);
    expect(fs.existsSync(path.join(malformed, "actor-run-archive-pending.json"))).toBe(true);
  });

  it.skipIf(process.platform === "win32")("rejects symlink roots, run directories, marker symlinks and hard-linked markers", async () => {
    const f = fixture(), directory = f.source(id(1));
    const linkRoot = path.join(f.root, "linked-root"); fs.symlinkSync(f.runRoot, linkRoot, "dir");
    expect(await discoverActorArchiveSources(linkRoot, actorId, f.sessionFile)).toEqual(new Map());
    fs.symlinkSync(directory, path.join(f.runRoot, id(2)), "dir");
    const linked = path.join(f.runRoot, id(3)); fs.mkdirSync(linked);
    fs.symlinkSync(path.join(directory, "actor-run-archive-pending.json"), path.join(linked, "actor-run-archive-pending.json"));
    const hard = f.source(id(4));
    fs.linkSync(path.join(hard, "actor-run-archive-pending.json"), path.join(f.root, "extra-marker-link"));
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
  });

  it("uses exact helper lookup without root enumeration and observes later markers without a cache", async () => {
    const f = fixture(), directory = f.source(id(1));
    const enumeration = vi.spyOn(fs.promises, "opendir").mockRejectedValue(new Error("known runs must not enumerate"));
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(1))).toEqual(new Map([[id(1), directory]]));
    expect(await discoverActorArchiveSources(f.runRoot, "other", f.sessionFile, id(1))).toEqual(new Map());
    expect(await discoverActorArchiveSources(f.runRoot, actorId, `${f.sessionFile}.other`, id(1))).toEqual(new Map());
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, "../escape")).toEqual(new Map());
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(2))).toEqual(new Map());
    const later = f.source(id(2));
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(2))).toEqual(new Map([[id(2), later]]));
    expect(enumeration).not.toHaveBeenCalled();
  });

  it("inspects only the exact run for retries/commit and preserves collection custody on mismatch", async () => {
    const f = fixture(), directory = f.source(id(1));
    const marker = path.join(directory, "actor-run-archive-pending.json");
    const trace = fs.readFileSync(path.join(directory, "events.jsonl"), "utf8");
    const enumeration = vi.spyOn(fs.promises, "opendir").mockRejectedValue(new Error("must not enumerate for a known run"));
    const syncEnumeration = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("must not enumerate for commit"); });
    const remove = vi.spyOn(fs, "rmSync").mockImplementation(() => {});
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, id(1))).toEqual(new Map([[id(1), directory]]));
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile, "../escape")).toEqual(new Map());
    await f.agents.commitActorArchive(id(1), "other", f.sessionFile);
    await f.agents.commitActorArchive(id(1), actorId, `${f.sessionFile}.other`);
    expect(remove).not.toHaveBeenCalled();
    await f.agents.commitActorArchive(id(1), actorId, f.sessionFile);
    expect(remove.mock.calls).toEqual([[marker]]);
    expect(enumeration).not.toHaveBeenCalled(); expect(syncEnumeration).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe(trace);
    expect(fs.existsSync(marker)).toBe(true); // Recorded-call probe, never a real custody discharge/deletion.
    expect(f.agents.retentionCustodyVeto(id(1))).toBe(true);
  });

  it("awaits completion/close discovery and archives restarted-owner sources before recorded cleanup", async () => {
    const f = fixture();
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
    const mesh = new MeshStore(path.join(f.root, "mesh"), 64 * 1024, 100);
    const identity = { id: "session:archive-discovery", name: "Main", kind: "main" as const, sessionId: "archive-discovery" };
    const actors = new ActorManager(identity.sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, f.agents, () => {},
      { actorRoot: path.join(mesh.root, "actors"), persistent: true });
    const cleanup = vi.spyOn(f.agents, "cleanup").mockResolvedValue({ cleaned: false });
    const rm = fs.rmSync.bind(fs), commits: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (path.basename(String(file)) === "actor-run-archive-pending.json") { commits.push(String(file)); return; }
      rm(file, options); // Metadata lock release only; raw trees are never removed.
    });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let scanning = false;
    const discover = f.agents.actorArchiveSources.bind(f.agents);
    const calls: (string | undefined)[] = [];
    vi.spyOn(f.agents, "actorArchiveSources").mockImplementation(async (owner, session, runId?: string) => {
      calls.push(runId);
      if (runId === undefined) { scanning = true; await gate; }
      return discover(owner, session, runId);
    });
    try {
      const actor = await actors.create({ name: "discovery", instructions: "Answer briefly.", protected: false });
      const current = f.source(id(1), { actorId: actor.id, sessionFile: actor.sessionFile });
      const prior = f.source(id(2), { actorId: actor.id, sessionFile: actor.sessionFile });
      vi.spyOn(f.agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
        onSpawned?.({ id: id(1) } as Parameters<NonNullable<typeof onSpawned>>[0]);
        return { id: id(1), status: "completed", text: "answer", usage: {}, inferenceStarted: true } as AgentRunResult;
      });
      await actors.ask(actor.id, "Complete one run.");
      await vi.waitFor(() => expect(scanning).toBe(true));
      expect(fs.existsSync(path.join(current, "events.jsonl"))).toBe(true);
      expect(cleanup).not.toHaveBeenCalled(); // The full completion discovery is still pending.
      release();
      await vi.waitFor(() => expect(actors.inFlightCount()).toBe(0));
      for (const [runId, source] of [[id(1), current], [id(2), prior]]) {
        expect(fs.readFileSync(path.join(actor.logDir!, runId!, "events.jsonl"), "utf8")).toBe(fs.readFileSync(path.join(source!, "events.jsonl"), "utf8"));
        expect(commits).toContain(path.join(source!, "actor-run-archive-pending.json"));
      }
      expect(calls).toContain(id(1)); expect(calls).toContain(id(2)); expect(calls).toContain(undefined);
      const atClose = f.source(id(3), { actorId: actor.id, sessionFile: actor.sessionFile });
      await actors.close();
      expect(fs.readFileSync(path.join(actor.logDir!, id(3), "events.jsonl"), "utf8")).toBe(fs.readFileSync(path.join(atClose, "events.jsonl"), "utf8"));
      expect(fs.existsSync(path.join(atClose, "actor-run-archive-pending.json"))).toBe(true);
    } finally { release(); await actors.close(); }
  });

  it("closes the real directory on I/O failure and retries rather than caching an incomplete view", async () => {
    const f = fixture(), directory = f.source(id(1));
    const opendir = fs.promises.opendir.bind(fs.promises), open = fs.promises.open.bind(fs.promises);
    const held = await opendir(f.runRoot);
    vi.spyOn(fs.promises, "opendir").mockResolvedValueOnce(held).mockImplementation(opendir);
    vi.spyOn(fs.promises, "open").mockImplementationOnce(async (file, flags, mode) => {
      const handle = await open(file, flags, mode), close = handle.close.bind(handle);
      vi.spyOn(handle, "close").mockImplementationOnce(async () => { await close(); throw new Error("marker close failed"); });
      return handle;
    }).mockImplementation(open);
    await expect(discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).rejects.toThrow("marker close failed");
    await expect(held.read()).rejects.toMatchObject({ code: "ERR_DIR_CLOSED" });
    expect(await discoverActorArchiveSources(f.runRoot, actorId, f.sessionFile)).toEqual(new Map([[id(1), directory]]));
  });
});
