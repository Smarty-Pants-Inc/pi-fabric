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
// call whose existing target is a session trace (a path matching /session\.jsonl/, an activation session
// `.activation-*.jsonl`, a message log `messages*.jsonl`, or below a session-archive/) or a directory holding one:
// fs.unlinkSync, fs.rmSync, fs.rmdirSync, fs.truncateSync, fs.promises.unlink, fs.promises.rm, fs.renameSync ONTO an
// existing trace (a rename-over replaces its bytes), and the verifiedRemove deleter (module mock, also reached through
// the manager's lazy import). verifiedRemove's own directory-relative unlinks (/proc/self/fd/<fd>/<name>) go through
// the fs.unlinkSync spy too. Not covered (smarty-dev#16469): named imports of node:fs functions bound before the
// spies, fs.unlink/fs.rm/fs.rename/fs.truncate callbacks and their fs.promises forms other than unlink/rm,
// fs.ftruncateSync and fs.writeFileSync/openSync(O_TRUNC) overwrites, child processes (workers) and native helpers.
const violations: string[] = [];
let guarding = false;
const TRACE = /session\.jsonl|session-archive[\\/]|\.activation-[^\\/]*\.jsonl|messages[^\\/]*\.jsonl/;
/** Test hooks around the real rename (fault injection and the at-rename snapshot of a moved tree). */
let beforeRename: ((source: string, destination: string) => void) | undefined;
let afterRename: ((source: string, destination: string) => void) | undefined;
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
  const wrap = <K extends "unlinkSync" | "rmSync" | "rmdirSync" | "truncateSync">(name: K) => {
    const original = fs[name].bind(fs) as (...args: unknown[]) => unknown;
    vi.spyOn(fs as unknown as Record<string, (...args: unknown[]) => unknown>, name)
      .mockImplementation((...args: unknown[]) => { record(`fs.${name}`, args[0]); return original(...args); });
  };
  wrap("unlinkSync"); wrap("rmSync"); wrap("rmdirSync"); wrap("truncateSync");
  const rename = fs.renameSync.bind(fs);
  vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
    record("fs.renameSync(over)", destination); // only an EXISTING trace destination is a violation
    beforeRename?.(String(source), String(destination));
    rename(source, destination);
    afterRename?.(String(source), String(destination));
  });
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
afterEach(() => { beforeRename = undefined; afterRename = undefined; });
afterAll(() => { guarding = false; vi.restoreAllMocks(); });

// smarty-dev#1568 P3: every ask spawns a fresh worker process (tests/fixtures/session-worker.mjs loads the public
// barrel), 3-6 s each on a loaded 8-CPU host; tests 1, 2a, 2b, 4a and 4b run 2-3 asks each. They share no state (each
// asserts its own actor's whole history), so they keep their own fixtures with an explicit timeout instead of the
// default 15 s.
const SLOW = 90_000;
/** Every entry below `root`, by relative path: a regular file's sha256, a link's target. */
const snapshot = (root: string): Record<string, string> => {
  const out: Record<string, string> = {};
  const walk = (directory: string, relative: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name), name = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isDirectory()) walk(file, name);
      else if (entry.isSymbolicLink()) out[name] = `link:${fs.readlinkSync(file)}`;
      else out[name] = sha(file);
    }
  };
  walk(root, "");
  return out;
};
/** An activation session the journal merge refused (worker/activation-session.ts: "isolated session retained"). */
const retainedActivation = (actorDir: string, marker: string): string => {
  const file = path.join(actorDir, `.activation-${marker}.jsonl`);
  fs.writeFileSync(file, `${JSON.stringify({ type: "session", version: 3, id: marker, timestamp: new Date().toISOString(), cwd: "/" })}\n${JSON.stringify({ type: "message", id: `${marker}-1`, parentId: null, message: { role: "user", content: marker } })}\n`);
  return file;
};

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
/** The archive's manifest lines; `phase` selects MOVING (the intent, before the rename) or MOVED-TO (after it). */
const manifest = (directory: string, phase?: "MOVING" | "MOVED-TO"): Array<Record<string, any>> =>
  fs.readFileSync(path.join(directory, "MANIFEST.jsonl"), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line))
    .filter(line => phase === undefined || line.phase === phase);
/** Each MOVED-TO line follows its own MOVING intent (same from, to and sha256). */
const intentsPrecede = (lines: Array<Record<string, any>>): void => {
  lines.forEach((line, index) => {
    if (line.phase !== "MOVED-TO") return;
    expect(lines.slice(0, index).some(before => before.phase === "MOVING" && before.from === line.from && before.to === line.to &&
      before.sha256 === line.sha256)).toBe(true);
  });
  expect(lines.filter(line => line.phase === "MOVING").length).toBe(lines.filter(line => line.phase === "MOVED-TO").length);
};
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
    // Each archive name carries the first 12 hex of its sha256; the manifest has the full sha256, a MOVING intent
    // before each rename and the MOVED-TO line after it.
    for (const name of archived(archiveOf(actor.sessionFile!))) {
      expect(sha(path.join(archiveOf(actor.sessionFile!), name)).slice(0, 12)).toBe(name.split("-")[1]!.slice(0, 12));
    }
    intentsPrecede(manifest(archiveOf(actor.sessionFile!)));
    const lines = manifest(archiveOf(actor.sessionFile!), "MOVED-TO");
    expect(lines.map(line => line.sha256)).toEqual([before[0], before[1]]);
    for (const line of lines) expect(line.line).toBe(`MOVED-TO ${line.to}`);
    // The message log: every reset has the displaced trace's sha256; the 2nd and 3rd name their move in archivedTo.
    // smarty-dev#1568 P3-4: the reason text is unchanged (a consumer may match it exactly).
    const logged = resets(actors, actor.id).map(message => ({ reason: message.reason!, ...(message.data as any).sessionReset }));
    expect(logged.map(entry => entry.sha256)).toEqual(before);
    expect(logged[0]!.archivedTo).toEqual([]);
    expect(logged[1]!.archivedTo).toEqual([expect.objectContaining({ sha256: before[0], to: lines[0]!.to })]);
    expect(logged[2]!.archivedTo).toEqual([expect.objectContaining({ sha256: before[1], to: lines[1]!.to })]);
    expect(logged.map(entry => entry.reason)).toEqual(Array(3).fill("session reset (requested)"));
  }, SLOW);

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
    expect(manifest(archiveOf(actor.sessionFile!), "MOVED-TO").map(line => line.sha256)).toEqual([first]);
  }, SLOW);

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
    expect(manifest(archiveOf(actor.sessionFile!), "MOVED-TO").map(line => line.sha256)).toEqual([first]);
  }, SLOW);

  const fiveBackups = () => {
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
    return { session, names, hashes };
  };

  it("3. pruneActorSessionBackups on 5 backups unlinks nothing, archives 4 and keeps the newest in place", () => {
    const { session, names, hashes } = fiveBackups();
    const dry = pruneActorSessionBackups(session, { dryRun: true });
    expect(dry.map(move => move.from)).toEqual(names.slice(0, 4));
    expect(names.every(name => fs.existsSync(name))).toBe(true); // a dry run moves nothing
    const moved = pruneActorSessionBackups(session);
    expect(moved.map(move => move.from)).toEqual(names.slice(0, 4));
    expect(moved.map(move => move.sha256)).toEqual(hashes.slice(0, 4));
    expect(moved.every(move => move.manifest === undefined)).toBe(true);
    expect(fs.existsSync(names[4]!)).toBe(true);
    expect(names.slice(0, 4).some(name => fs.existsSync(name))).toBe(false);
    const archive = archiveOf(session);
    expect(archived(archive)).toHaveLength(4);
    expect(moved.map(move => sha(move.to))).toEqual(hashes.slice(0, 4));
    // smarty-dev#1568 P2-1: a fsynced MOVING intent (from, to, sha256) before each rename, MOVED-TO after it.
    expect(manifest(archive).map(line => [line.phase, line.from, line.to, line.sha256]))
      .toEqual(moved.flatMap(move => [["MOVING", move.from, move.to, move.sha256], ["MOVED-TO", move.from, move.to, move.sha256]]));
    expect(fs.readFileSync(session, "utf8")).toBe("live\n");
    // Idempotent: only the newest is left, so a second prune moves nothing.
    expect(pruneActorSessionBackups(session)).toEqual([]);
  });

  it("3b. a failure AFTER the rename is reported as moved with manifest:false, never as kept in place (smarty-dev#1568 P2-1)", () => {
    const { session, names, hashes } = fiveBackups();
    const archive = archiveOf(session);
    // The MOVED-TO append fails (MANIFEST.jsonl made read-only right after the first rename); the MOVING intent is on disk.
    afterRename = (_source, destination) => {
      if (path.dirname(destination) === archive) { fs.chmodSync(path.join(archive, "MANIFEST.jsonl"), 0o400); afterRename = undefined; }
    };
    const moved = pruneActorSessionBackups(session);
    expect(moved[0]).toMatchObject({ from: names[0], sha256: hashes[0], manifest: false });
    // Linux and macOS: EACCES; Windows: EPERM (the read-only manifest refuses the append there with EPERM).
    expect(moved[0]!.problem).toMatch(/EACCES|EPERM|permission/i);
    expect(sha(moved[0]!.to)).toBe(hashes[0]);
    expect(fs.existsSync(names[0]!)).toBe(false);
    // The read-only manifest refuses the next MOVING intent: those backups stay in place (nothing moved, nothing lost).
    expect(moved).toHaveLength(1);
    expect(names.slice(1).every(name => fs.existsSync(name))).toBe(true);
    expect(manifest(archive).map(line => [line.phase, line.from, line.to, line.sha256]))
      .toEqual([["MOVING", names[0], moved[0]!.to, hashes[0]]]);
  });

  it("4a. actor removal moves the WHOLE actor dir into the fleet archive: every file byte-identical, with a manifest", async () => {
    const { actors, mesh, identity } = setup();
    const actor = await actors.create({ name: "leaving", instructions: "Leave." });
    const actorDir = path.dirname(actor.sessionFile!);
    for (const word of ["one", "two"]) {
      await actors.ask(actor.id, word);
      await actors.resetSession(actor.id);
    }
    await actors.ask(actor.id, "three");
    await actors.stop(actor.id);
    retainedActivation(actorDir, "refused-merge");
    const before = snapshot(actorDir);
    // Every kind of trace the tree holds: the live session, the kept .bak, the per-actor archive, the retained
    // activation session and the message log (where the reset records live).
    expect(Object.keys(before)).toEqual(expect.arrayContaining(["session.jsonl", ".activation-refused-merge.jsonl",
      "registry/messages.jsonl", "session-archive/MANIFEST.jsonl"]));
    expect(Object.keys(before).filter(name => /^session\.jsonl\.\d{8}T\d{9}Z\.bak$/.test(name))).toHaveLength(1);
    let atRename: Record<string, string> | undefined;
    const pinned = fs.lstatSync(actorDir);
    beforeRename = source => {
      const stat = fs.lstatSync(source, { throwIfNoEntry: false });
      if (stat?.dev === pinned.dev && stat.ino === pinned.ino) atRename = snapshot(source);
    };
    const removed = await actors.remove(actor.id);
    expect(removed).toMatchObject({ removed: true });
    expect(removed.pending).toBeUndefined();
    expect(fs.existsSync(actorDir)).toBe(false);
    const fleet = fleetSessionArchive(mesh.root, identity.id, actor.id);
    // One rename: the archived tree is exactly the tree at the rename, and holds every file the actor had before.
    expect(atRename).toBeDefined();
    expect(snapshot(fleet)).toEqual(atRename);
    for (const [name, digest] of Object.entries(before)) expect([name, snapshot(fleet)[name]]).toEqual([name, digest]);
    const lines = manifest(path.dirname(fleet));
    expect(lines.map(line => [line.phase, line.kind, line.from, line.to, line.actorId, line.rootId])).toEqual([
      ["MOVING", "tree", actorDir, fleet, actor.id, identity.id], ["MOVED-TO", "tree", actorDir, fleet, actor.id, identity.id]]);
    expect(lines[1]!.line).toBe(`MOVED-TO ${fleet}`);
    // The intent recorded each trace's sha256 before the rename.
    const traces = Object.fromEntries((lines[0]!.files as Array<{ path: string; sha256?: string }>).filter(file => file.sha256)
      .map(file => [file.path, file.sha256]));
    expect(traces).toMatchObject({ "session.jsonl": before["session.jsonl"], ".activation-refused-merge.jsonl": before[".activation-refused-merge.jsonl"] });
  }, SLOW);

  it("4b. a non-persistent manager's root-tree reset (close) moves the WHOLE actor root into the fleet archive", async () => {
    const { actors, mesh, identity } = setup({ persistent: false });
    const actor = await actors.create({ name: "ephemeral", instructions: "Pass." });
    await actors.ask(actor.id, "one");
    await actors.resetSession(actor.id);
    await actors.ask(actor.id, "two");
    const actorRoot = path.dirname(path.dirname(actor.sessionFile!));
    retainedActivation(path.dirname(actor.sessionFile!), "kept-at-close");
    const before = snapshot(actorRoot);
    const relative = path.basename(path.dirname(actor.sessionFile!));
    // A non-persistent manager keeps its message log in memory (no registry/messages.jsonl); every file it has moves.
    expect(Object.keys(before)).toEqual(expect.arrayContaining([`${relative}/session.jsonl`,
      `${relative}/.activation-kept-at-close.jsonl`, expect.stringMatching(new RegExp(`^${relative}/session\\.jsonl\\.\\d{8}T\\d{9}Z\\.bak$`))]));
    let atRename: Record<string, string> | undefined;
    beforeRename = source => { if (path.resolve(source) === path.resolve(actorRoot)) atRename = snapshot(source); };
    await managers.splice(0, 1)[0]!.close();
    expect(fs.existsSync(actorRoot)).toBe(false);
    const fleet = path.join(fleetSessionArchive(mesh.root, identity.id), "actor-root");
    expect(atRename).toBeDefined();
    expect(snapshot(fleet)).toEqual(atRename);
    for (const [name, digest] of Object.entries(before)) expect([name, snapshot(fleet)[name]]).toEqual([name, digest]);
    expect(manifest(path.dirname(fleet)).map(line => [line.phase, line.kind, line.from, line.to]))
      .toEqual([["MOVING", "tree", actorRoot, fleet], ["MOVED-TO", "tree", actorRoot, fleet]]);
  }, SLOW);

  // fabric-store's review (pi-fabric#1568): `create()` of a name whose namesake is STOPPED removes the namesake first
  // (src/actors/manager.ts create(): `if (sameName?.status === "stopped") await this.remove(sameName.id, ...)`). That
  // path must keep every trace of the old actor too.
  it("4c. stop, then create with the SAME name: every trace of the stopped namesake is byte-identical in its fleet archive", async () => {
    const { actors, mesh, identity } = setup();
    const old = await actors.create({ name: "namesake", instructions: "First." });
    const oldDir = path.dirname(old.sessionFile!);
    await actors.ask(old.id, "one");
    await actors.resetSession(old.id);
    await actors.ask(old.id, "two");
    await actors.stop(old.id);
    retainedActivation(oldDir, "namesake-refused-merge");
    const before = snapshot(oldDir);
    // Every kind of trace: the session, its kept .bak, the activation session and the message log.
    expect(Object.keys(before)).toEqual(expect.arrayContaining(["session.jsonl", ".activation-namesake-refused-merge.jsonl",
      "registry/messages.jsonl"]));
    expect(Object.keys(before).filter(name => /^session\.jsonl\.\d{8}T\d{9}Z\.bak$/.test(name))).toHaveLength(1);
    let atRename: Record<string, string> | undefined;
    const pinned = fs.lstatSync(oldDir);
    beforeRename = source => {
      const stat = fs.lstatSync(source, { throwIfNoEntry: false });
      if (stat?.dev === pinned.dev && stat.ino === pinned.ino) atRename = snapshot(source);
    };
    const next = await actors.create({ name: "namesake", instructions: "Second." });
    await actors.removalSettled(old.id);
    expect(next.id).not.toBe(old.id);
    expect(actors.list().map(actor => actor.id)).not.toContain(old.id);
    expect(actors.cleanupObligation(old.id)).toBeUndefined();
    expect(fs.existsSync(oldDir)).toBe(false);
    // The old actor's whole tree, by one rename: exactly the tree at the rename, and every file it had before.
    const fleet = fleetSessionArchive(mesh.root, identity.id, old.id);
    expect(atRename).toBeDefined();
    expect(snapshot(fleet)).toEqual(atRename);
    for (const [name, digest] of Object.entries(before)) expect([name, snapshot(fleet)[name]]).toEqual([name, digest]);
    expect(manifest(path.dirname(fleet)).map(line => [line.phase, line.from, line.to, line.actorId]))
      .toEqual([["MOVING", oldDir, fleet, old.id], ["MOVED-TO", oldDir, fleet, old.id]]);
    // The successor starts fresh in its own directory; the archive is untouched by it.
    expect(path.dirname(next.sessionFile!)).not.toBe(oldDir);
  }, SLOW);

  it("5. GUARD: no code path in tests 1-4 or the actor stop/remove deleted, truncated or renamed over a session trace", () => {
    const seen = [...violations];
    // Canary: the spies are live and catch a real session-file unlink, a recursive rm of a tree holding one, an
    // activation-session unlink, a truncate of a message log and a rename onto a session file.
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-trace-canary-"));
    roots.push(directory);
    const canary = path.join(directory, "session.jsonl.canary");
    fs.writeFileSync(canary, "x");
    fs.unlinkSync(canary);
    fs.mkdirSync(path.join(directory, "tree"));
    fs.writeFileSync(path.join(directory, "tree", "session.jsonl"), "x");
    fs.rmSync(path.join(directory, "tree"), { recursive: true, force: true });
    const activation = path.join(directory, ".activation-run.jsonl");
    fs.writeFileSync(activation, "x");
    fs.unlinkSync(activation);
    const messages = path.join(directory, "messages.jsonl");
    fs.writeFileSync(messages, "x");
    fs.truncateSync(messages, 0);
    const live = path.join(directory, "session.jsonl"), other = path.join(directory, "other.json");
    fs.writeFileSync(live, "x"); fs.writeFileSync(other, "y");
    fs.renameSync(other, live);
    expect(violations.slice(seen.length)).toEqual([`fs.unlinkSync ${canary}`, `fs.rmSync ${path.join(directory, "tree")}`,
      `fs.unlinkSync ${activation}`, `fs.truncateSync ${messages}`, `fs.renameSync(over) ${live}`]);
    expect(seen).toEqual([]);
  });
});
