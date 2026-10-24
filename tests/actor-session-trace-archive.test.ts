import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// smarty-dev#16444 (R-keep-all-traces): retention, rotation, removal and reset never delete an actor session trace.
// Every trace a path used to delete is MOVED (one same-filesystem rename) into a retained archive with its sha256.
//
// THE GUARD: for this whole file (tests 1-4 plus an actor stop and remove), these deleters are spies that record any
// call whose existing target is a session trace (a path matching /session\.jsonl/ or below a session-archive/) or a
// directory holding one: fs.unlinkSync, fs.rmSync, fs.rmdirSync, fs.promises.unlink, fs.promises.rm, and the
// verifiedRemove deleter (module mock, also reached through the manager's lazy import). verifiedRemove's own
// directory-relative unlinks (/proc/self/fd/<fd>/<name>) go through the fs.unlinkSync spy too. Not covered: named
// imports of node:fs functions bound before the spies, fs.unlink/fs.rm callbacks, child processes and native helpers.
const violations: string[] = [];
let guarding = false;
const TRACE = /session\.jsonl|session-archive[\\/]/;
const holdsTrace = (target: string, depth = 0): boolean => {
  let stat: fs.Stats;
  try { stat = fs.lstatSync(target); } catch { return false; } // absent: the call deletes nothing
  if (!stat.isDirectory()) return TRACE.test(target);
  if (TRACE.test(`${target}/`) || depth > 32) return true;
  let names: string[];
  try { names = fs.readdirSync(target); } catch { return false; }
  return names.some(name => holdsTrace(path.join(target, name), depth + 1));
};
const record = (deleter: string, target: unknown): void => {
  if (!guarding) return;
  const file = target instanceof URL ? fileURLToPath(target) : Buffer.isBuffer(target) ? target.toString() : String(target);
  if (holdsTrace(file)) violations.push(`${deleter} ${file}`);
};

vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return {
    ...actual,
    verifiedRemove: (...args: Parameters<typeof actual.verifiedRemove>) => {
      if (args[2]?.dryRun !== true) record(`verifiedRemove(${args[2]?.deleter})`, args[0]);
      return actual.verifiedRemove(...args);
    },
  };
});

const { ActorManager } = await import("../src/actors/manager.js");
const { AgentManager } = await import("../src/agents/manager.js");
const { MeshStore } = await import("../src/mesh/store.js");
const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
const { deliverActorToMain } = await import("../src/actors/main-delivery.js");
const { pruneActorSessionBackups } = await import("../src/storage/retention.js");
const { fleetSessionArchive } = await import("../src/storage/session-archive.js");
type MeshIdentity = import("../src/mesh/store.js").MeshIdentity;

// vitest.config.cts sets restoreMocks: true (spies are restored before every test), so the spies go in per test.
beforeEach(() => {
  const wrap = <K extends "unlinkSync" | "rmSync" | "rmdirSync">(name: K) => {
    const original = fs[name].bind(fs) as (...args: unknown[]) => unknown;
    vi.spyOn(fs as unknown as Record<string, (...args: unknown[]) => unknown>, name)
      .mockImplementation((...args: unknown[]) => { record(`fs.${name}`, args[0]); return original(...args); });
  };
  wrap("unlinkSync"); wrap("rmSync"); wrap("rmdirSync");
  for (const name of ["unlink", "rm"] as const) {
    const original = fs.promises[name].bind(fs.promises) as (...args: unknown[]) => Promise<unknown>;
    vi.spyOn(fs.promises as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>, name)
      .mockImplementation(async (...args: unknown[]) => { record(`fs.promises.${name}`, args[0]); return original(...args); });
  }
  guarding = true;
});

const roots: string[] = [];
const managers: Array<{ close(): Promise<void> }> = [];
afterEach(async () => {
  // Manager close (actor stop, a non-persistent root-tree reset) runs under the guard; only the test's own temp
  // roots are removed without it.
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  guarding = false;
  try { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); } finally { guarding = true; }
});
afterAll(() => { guarding = false; vi.restoreAllMocks(); });

const sha = (file: string): string => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
type Usage = { tokens: number; contextWindow: number };

const setup = (options: { maxSessionBytes?: number; persistent?: boolean; usage?: () => Usage | undefined } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-trace-archive-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/session-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  const pi = { sendMessage: vi.fn() } as unknown as ExtensionAPI;
  const persistent = options.persistent ?? true;
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
    request => deliverActorToMain(pi, identity, request), {
      ...(persistent ? { actorRoot: path.join(root, "actors"), persistent: true } : {}),
      maxSessionBytes: options.maxSessionBytes ?? 0,
    });
  managers.push(actors, agents);
  if (options.usage) {
    const run = agents.run.bind(agents);
    vi.spyOn(agents, "run").mockImplementation(async (request, signal) => {
      const result = await run(request, signal);
      const usage = options.usage!();
      return usage ? { ...result, contextUsage: usage } : result;
    });
  }
  return { root, mesh, actors, identity };
};

const backups = (sessionFile: string): string[] => fs.readdirSync(path.dirname(sessionFile))
  .filter(name => name.startsWith("session.jsonl.") && name.endsWith(".bak")).sort();
const archiveOf = (sessionFile: string): string => path.join(path.dirname(sessionFile), "session-archive");
const archived = (directory: string): string[] => fs.existsSync(directory)
  ? fs.readdirSync(directory).filter(name => /^\d{8}T\d{9}Z-[0-9a-f]{12}(\.\d+)?\.jsonl$/.test(name)).sort() : [];
const manifest = (directory: string): Array<Record<string, any>> =>
  fs.readFileSync(path.join(directory, "MANIFEST.jsonl"), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line));
const resets = (actors: InstanceType<typeof ActorManager>, id: string) => actors.messages(id, 100)
  .filter(message => (message.data as { sessionReset?: unknown } | undefined)?.sessionReset);
/** Every trace the actor still holds, kept .bak plus its session-archive, by sha256. */
const heldHashes = (sessionFile: string): string[] => [
  ...backups(sessionFile).map(name => sha(path.join(path.dirname(sessionFile), name))),
  ...archived(archiveOf(sessionFile)).map(name => sha(path.join(archiveOf(sessionFile), name))),
].sort();

describe("actor session traces are archived, never deleted (smarty-dev#16444)", () => {
  it("1. three resets in a row keep all three traces byte-identical: the kept .bak plus the archive hold them", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "keeper", instructions: "Remember." });
    const before: string[] = [];
    for (const word of ["alpha", "bravo", "charlie"]) {
      await actors.ask(actor.id, word);
      before.push(sha(actor.sessionFile!));
      await actors.resetSession(actor.id);
    }
    expect(new Set(before).size).toBe(3);
    // The newest backup stays in place (resume semantics); the two older ones are in session-archive/.
    expect(backups(actor.sessionFile!)).toHaveLength(1);
    expect(sha(path.join(path.dirname(actor.sessionFile!), backups(actor.sessionFile!)[0]!))).toBe(before[2]);
    expect(archived(archiveOf(actor.sessionFile!))).toHaveLength(2);
    expect(heldHashes(actor.sessionFile!)).toEqual([...before].sort());
    // Each archive name carries the first 12 hex of its sha256; the manifest has the full sha256 and the MOVED-TO line.
    for (const name of archived(archiveOf(actor.sessionFile!))) {
      expect(sha(path.join(archiveOf(actor.sessionFile!), name)).slice(0, 12)).toBe(name.split("-")[1]!.slice(0, 12));
    }
    const lines = manifest(archiveOf(actor.sessionFile!));
    expect(lines.map(line => line.sha256)).toEqual([before[0], before[1]]);
    for (const line of lines) expect(line.line).toBe(`MOVED-TO ${line.to}`);
    // The message log: every reset has the displaced trace's sha256; the 2nd and 3rd name their move.
    const logged = resets(actors, actor.id).map(message => ({ reason: message.reason!, ...(message.data as any).sessionReset }));
    expect(logged.map(entry => entry.sha256)).toEqual(before);
    expect(logged[0]!.archivedTo).toEqual([]);
    expect(logged[1]!.archivedTo).toEqual([expect.objectContaining({ sha256: before[0], to: lines[0]!.to })]);
    expect(logged[2]!.archivedTo).toEqual([expect.objectContaining({ sha256: before[1], to: lines[1]!.to })]);
    expect(logged[2]!.reason).toContain(`\nMOVED-TO ${lines[1]!.to} (sha256 ${before[1]})`);
  });

  it("2a. a size rotation archives the older backup and unlinks nothing", async () => {
    const { actors } = setup({ maxSessionBytes: 64 });
    const actor = await actors.create({ name: "growing", instructions: "Grow." });
    await actors.ask(actor.id, "one");
    const first = sha(actor.sessionFile!);
    await actors.ask(actor.id, "two"); // size reset: "one" becomes the kept .bak
    const second = sha(actor.sessionFile!);
    await actors.ask(actor.id, "three"); // size reset: "one" moves into session-archive/
    expect(resets(actors, actor.id).map(message => (message.data as any).sessionReset.trigger)).toEqual(["size", "size"]);
    expect(heldHashes(actor.sessionFile!)).toEqual([first, second].sort());
    expect(manifest(archiveOf(actor.sessionFile!)).map(line => line.sha256)).toEqual([first]);
  });

  it("2b. a context rotation archives the older backup and unlinks nothing", async () => {
    let usage: Usage | undefined = { tokens: 250_000, contextWindow: 272_000 };
    const { actors } = setup({ usage: () => usage });
    const actor = await actors.create({ name: "filling", instructions: "Fill." });
    await actors.ask(actor.id, "one");
    const first = sha(actor.sessionFile!);
    await actors.ask(actor.id, "two"); // context reset: "one" becomes the kept .bak
    const second = sha(actor.sessionFile!);
    usage = undefined;
    await actors.ask(actor.id, "three"); // context reset: "one" moves into session-archive/
    expect(resets(actors, actor.id).map(message => (message.data as any).sessionReset.trigger)).toEqual(["context", "context"]);
    expect(heldHashes(actor.sessionFile!)).toEqual([first, second].sort());
    expect(manifest(archiveOf(actor.sessionFile!)).map(line => line.sha256)).toEqual([first]);
  });

  it("3. pruneActorSessionBackups on 5 backups unlinks nothing, archives 4 and keeps the newest in place", () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-trace-prune-"));
    roots.push(directory);
    const session = path.join(directory, "session.jsonl");
    fs.writeFileSync(session, "live\n");
    const names = ["20260920", "20260921", "20260922", "20260923", "20260924"].map(day => `${session}.${day}T150000000Z.bak`);
    const hashes = names.map((name, index) => {
      fs.writeFileSync(name, `trace ${index}\n`);
      fs.utimesSync(name, 1_000 + index, 1_000 + index);
      return sha(name);
    });
    const dry = pruneActorSessionBackups(session, { dryRun: true });
    expect(dry.map(move => move.from)).toEqual(names.slice(0, 4));
    expect(names.every(name => fs.existsSync(name))).toBe(true); // a dry run moves nothing
    const moved = pruneActorSessionBackups(session);
    expect(moved.map(move => move.from)).toEqual(names.slice(0, 4));
    expect(moved.map(move => move.sha256)).toEqual(hashes.slice(0, 4));
    expect(fs.existsSync(names[4]!)).toBe(true);
    expect(names.slice(0, 4).some(name => fs.existsSync(name))).toBe(false);
    const archive = archiveOf(session);
    expect(archived(archive)).toHaveLength(4);
    expect(moved.map(move => sha(move.to))).toEqual(hashes.slice(0, 4));
    expect(manifest(archive).map(line => [line.from, line.to, line.sha256])).toEqual(moved.map(move => [move.from, move.to, move.sha256]));
    expect(fs.readFileSync(session, "utf8")).toBe("live\n");
    // Idempotent: only the newest is left, so a second prune moves nothing.
    expect(pruneActorSessionBackups(session)).toEqual([]);
  });

  it("4a. actor removal leaves its session files in the fleet archive, with a manifest", async () => {
    const { actors, mesh, identity } = setup();
    const actor = await actors.create({ name: "leaving", instructions: "Leave." });
    const hashes: string[] = [];
    for (const word of ["one", "two", "three"]) {
      await actors.ask(actor.id, word);
      hashes.push(sha(actor.sessionFile!));
      await actors.resetSession(actor.id);
    }
    await actors.ask(actor.id, "four");
    hashes.push(sha(actor.sessionFile!));
    await actors.stop(actor.id);
    const removed = await actors.remove(actor.id);
    expect(removed).toMatchObject({ removed: true });
    expect(removed.pending).toBeUndefined();
    expect(fs.existsSync(path.dirname(actor.sessionFile!))).toBe(false);
    const fleet = fleetSessionArchive(mesh.root, identity.id, actor.id);
    const lines = manifest(fleet);
    // The live session and the kept .bak moved one by one; the actor's own session-archive/ moved whole.
    const files = lines.filter(line => line.kind === "file");
    const directory = lines.find(line => line.kind === "directory")!;
    expect(lines.every(line => line.actorId === actor.id && line.rootId === identity.id && line.line === `MOVED-TO ${line.to}`)).toBe(true);
    const names = files.map(line => path.basename(line.from)).sort();
    expect(names).toEqual(["session.jsonl", expect.stringMatching(/^session\.jsonl\.\d{8}T\d{9}Z\.bak$/)]);
    for (const line of files) expect(sha(line.to)).toBe(line.sha256);
    const held = [...files.map(line => sha(line.to)),
      ...archived(directory.to).map(name => sha(path.join(directory.to, name)))].sort();
    // Every trace this actor ever had: the three reset sessions (two archived, one kept .bak) and the live one.
    expect(held).toEqual([...hashes].sort());
    expect(fs.existsSync(path.join(directory.to, "MANIFEST.jsonl"))).toBe(true);
  });

  it("4b. a non-persistent manager's root-tree reset (close) moves its session traces into the fleet archive first", async () => {
    const { actors, mesh, identity } = setup({ persistent: false });
    const actor = await actors.create({ name: "ephemeral", instructions: "Pass." });
    await actors.ask(actor.id, "one");
    const first = sha(actor.sessionFile!);
    await actors.resetSession(actor.id);
    await actors.ask(actor.id, "two");
    const live = sha(actor.sessionFile!);
    const actorRoot = path.dirname(path.dirname(actor.sessionFile!));
    await managers.splice(0, 1)[0]!.close();
    expect(fs.existsSync(actorRoot)).toBe(false);
    const fleet = path.join(fleetSessionArchive(mesh.root, identity.id), actor.id);
    const lines = manifest(fleet);
    expect(lines.map(line => line.sha256).sort()).toEqual([first, live].sort());
    for (const line of lines) expect(sha(line.to)).toBe(line.sha256);
  });

  it("5. GUARD: no code path in tests 1-4 or the actor stop/remove unlinked a session trace", () => {
    const seen = [...violations];
    // Canary: the spies are live and do catch a real session-file unlink and a recursive rm of a tree holding one.
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-trace-canary-"));
    roots.push(directory);
    const canary = path.join(directory, "session.jsonl.canary");
    fs.writeFileSync(canary, "x");
    fs.unlinkSync(canary);
    fs.mkdirSync(path.join(directory, "tree"));
    fs.writeFileSync(path.join(directory, "tree", "session.jsonl"), "x");
    fs.rmSync(path.join(directory, "tree"), { recursive: true, force: true });
    expect(violations.slice(seen.length)).toEqual([`fs.unlinkSync ${canary}`, `fs.rmSync ${path.join(directory, "tree")}`]);
    expect(seen).toEqual([]);
  });
});
