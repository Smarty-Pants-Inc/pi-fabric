import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MESH_RETENTION_APPROVAL, MESH_RETENTION_HOLD, sweepMeshRetention, writeReport } from "../src/storage/retention-cli.js";
import { pruneActorSessionBackups } from "../src/storage/retention.js";
import { lockFile } from "../src/residency/file-lock.js";
import { spawnSync } from "node:child_process";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const snapshot = (root: string): unknown[] => fs.readdirSync(root).sort().map(name => {
  const file = path.join(root, name); const stat = fs.lstatSync(file);
  return [name, stat.ino, stat.mtimeMs, stat.ctimeMs, stat.isDirectory() ? snapshot(file) : stat.isSymbolicLink() ? fs.readlinkSync(file) : fs.readFileSync(file, "utf8")];
});
const make = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-sweep-")); roots.push(root);
  const host = path.join(root, "residency", "host");
  write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
  write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:proof", residencyRoot: host, meshRoot: root }));
  const actor = path.join(root, "actors", "project", "actor");
  write(path.join(root, "actors", "project", "actors.json"), JSON.stringify({ actors: [{ id: "actor", rootId: "session:proof", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest" }] }));
  const log = Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence }) + "\n").join("");
  for (const run of [path.join(host, "runs", "terminal"), path.join(actor, "runs", "archive"), path.join(actor, "runs", "latest")]) {
    write(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", finishedAt: 1 }));
    write(path.join(run, "events.jsonl"), log);
    write(path.join(run, "reply.json"), '{"text":"keep"}');
  }
  write(path.join(actor, "session.jsonl.20260927T150000000Z.bak"), "old");
  write(path.join(actor, "session.jsonl.20260928T150000000Z.bak"), "new");
  write(path.join(actor, "session.jsonl"), "live");
  return { root, host, actor };
};

describe.skipIf(process.platform !== "linux")("offline retained mesh sweep", () => {
  it("lists exact bytes with a whole-tree no-op dry run, then applies under existing native fences", async () => {
    const { root, actor, host } = make(); const before = snapshot(root);
    const plan = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: true });
    expect(snapshot(root)).toEqual(before);
    expect(plan.skipped).toEqual([]);
    expect(plan.changes).toHaveLength(3);
    expect(plan.bytesBefore).toBeGreaterThan(plan.bytesAfter);
    const applied = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: false });
    expect(applied.changes).toEqual(plan.changes);
    for (const run of [path.join(host, "runs", "terminal"), path.join(actor, "runs", "archive")]) {
      expect(fs.readFileSync(path.join(run, "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(201);
      expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"keep"}');
    }
    expect(fs.readFileSync(path.join(actor, "runs", "latest", "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(401);
    expect(fs.readdirSync(actor).filter(name => name.endsWith(".bak"))).toEqual(["session.jsonl.20260928T150000000Z.bak"]);
  });

  it("enumerates an owned actor registry larger than 1 MiB in dry-run and apply without losing latest-run vetoes", async () => {
    const { root, actor, host } = make();
    const file = path.join(root, "actors", "project", "actors.json");
    const registry = JSON.parse(fs.readFileSync(file, "utf8"));
    const messages = Array.from({ length: 100 }, (_, index) => ({ id: String(index), text: "x".repeat(1400) }));
    const first = registry.actors[0];
    registry.actors = Array.from({ length: 10 }, (_, index) => {
      const id = index ? `other-${index}` : "actor";
      const sessionFile = path.join(path.dirname(file), id, "session.jsonl");
      if (index) write(sessionFile, "live session");
      return { ...first, id, sessionFile, ...(index ? { lastRunId: undefined } : {}), messages };
    });
    write(file, JSON.stringify(registry));
    expect(fs.statSync(file).size).toBeGreaterThan(1024 * 1024);
    const before = snapshot(root);
    const plan = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: true });
    expect(snapshot(root)).toEqual(before);
    expect(plan.skipped).toEqual([]);
    expect(plan.changes.map(change => change.path).sort()).toEqual([
      path.join(host, "runs", "terminal", "events.jsonl"),
      path.join(actor, "runs", "archive", "events.jsonl"),
      path.join(actor, "session.jsonl.20260927T150000000Z.bak"),
    ].sort());
    const latest = snapshot(path.join(actor, "runs", "latest"));
    const applied = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: false });
    expect(applied.skipped).toEqual([]);
    expect(applied.changes).toEqual(plan.changes);
    expect(snapshot(path.join(actor, "runs", "latest"))).toEqual(latest);
    expect(fs.readFileSync(path.join(actor, "runs", "archive", "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(201);
    expect(fs.readdirSync(actor).filter(name => name.endsWith(".bak"))).toEqual(["session.jsonl.20260928T150000000Z.bak"]);
    fs.mkdirSync(`${file}.lock`);
    const locked = snapshot(root);
    expect((await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: false })).skipped).toEqual([
      expect.objectContaining({ path: path.dirname(file), reason: expect.stringMatching(/lock/) }),
    ]);
    expect(snapshot(root)).toEqual(locked);
  });

  it.each([undefined, "unknown", "herdr", "localterm", "tmux", "screen"])("never reports or changes a root with %s transport and no persisted native exit receipt", async transport => {
    const { root, host, actor } = make();
    const unsafe = [path.join(host, "runs", "terminal"), path.join(actor, "runs", "archive")];
    for (const run of unsafe) write(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport, finishedAt: 1, sessionId: "2147483647", exitCode: 0 }));
    const before = unsafe.map(run => ({ directoryMtime: fs.statSync(run).mtimeMs, tree: snapshot(run) }));
    for (const dryRun of [true, false]) {
      const result = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun });
      expect(result.changes.filter(change => unsafe.some(run => change.path.startsWith(run + path.sep)))).toEqual([]);
      expect(unsafe.map(run => ({ directoryMtime: fs.statSync(run).mtimeMs, tree: snapshot(run) }))).toEqual(before);
    }
  });

  it("does not displace a kernel-fenced holder even with stale dead-PID diagnostics", async () => {
    const { root, host } = make(); const fd = await lockFile(path.join(host, "host.lock"), 0, true);
    try {
      const before = snapshot(root);
      expect((await sweepMeshRetention(root, { dryRun: false })).changes).toEqual([]);
      expect(snapshot(root)).toEqual(before);
    } finally { fs.closeSync(fd); }
  });

  it("holds a dead root's host.lock only around that root's own sweep, so a waking host can claim it during the mesh-wide pass (smarty-dev#7766)", async () => {
    const { root, host } = make();
    // A waking host's claim: flock(1) non-blocking on the same host.lock, from another process.
    const claimable = () => spawnSync("flock", ["-x", "-n", path.join(host, "host.lock"), "true"]).status === 0;
    const claims: boolean[] = [];
    // The sweep reads runRetentionMs inside the root's fenced sweep and again before the mesh-wide pass.
    const options = { now: 30 * 86400000, dryRun: false, get runRetentionMs() { claims.push(claimable()); return 7 * 86400000; } };
    const result = await sweepMeshRetention(root, options);
    expect(result.skipped).toEqual([]);
    expect(claims[0]).toBe(false);
    expect(claims.at(-1)).toBe(true);
    expect(claimable()).toBe(true);
  });

  it("stops deleting the moment a hold appears mid-sweep, and a host waiting for a root's lock wins that root (smarty-dev#7766)", async () => {
    for (const event of ["hold", "wake"] as const) {
      const { root, host } = make();
      const runs = snapshot(path.join(host, "runs"));
      // residentRunRetentionMs is read inside the dead root's fenced sweep, before its first deletion.
      const options = { now: 30 * 86400000, dryRun: false, runRetentionMs: 7 * 86400000, get residentRunRetentionMs() {
        if (event === "hold") write(path.join(root, MESH_RETENTION_HOLD), "{}");
        // A waking host: flock(1) blocks on host.lock in the background, so /proc/locks lists a waiter.
        else spawnSync("sh", ["-c", `flock -x -w 20 '${path.join(host, "host.lock")}' true </dev/null >/dev/null 2>&1 & sleep 0.3`]);
        return 86400000;
      } };
      const result = await sweepMeshRetention(root, options);
      // A hold stops every deletion; a waking host stops its own root's work. The mesh-wide actor pass is
      // independent of host liveness by design (pi-fabric#645): it keeps its own registry fences.
      expect(result.changes.filter(change => event === "hold" || change.path.startsWith(host)), event).toEqual([]);
      expect(result.skipped.map(item => item.reason).join(" "), event).toMatch(event === "hold" ? /held/ : /host is waking/);
      if (event === "hold") fs.unlinkSync(path.join(root, MESH_RETENTION_HOLD));
      expect(snapshot(path.join(host, "runs")), event).toEqual(runs);
    }
  });

  it("reads the epoch approval only from our own regular file, never through a link; writes the report atomically without following one", async () => {
    const { root } = make();
    write(path.join(root, "state.json"), JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3, at: "2026-10-09T15:29:19.869Z" }));
    write(path.join(root, "state.db"), "");
    const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-approval-")); roots.push(elsewhere);
    write(path.join(elsewhere, "ok.json"), JSON.stringify({ epoch: 3 }));
    fs.symlinkSync(path.join(elsewhere, "ok.json"), path.join(root, MESH_RETENTION_APPROVAL));
    const before = snapshot(root);
    const options = { now: 30 * 86400000, dryRun: false, runRetentionMs: 7 * 86400000 };
    expect((await sweepMeshRetention(root, options)).skipped).toEqual([{ path: root, reason: expect.stringMatching(/ruling/) }]);
    expect(snapshot(root)).toEqual(before);
    fs.unlinkSync(path.join(root, MESH_RETENTION_APPROVAL));
    write(path.join(root, MESH_RETENTION_APPROVAL), JSON.stringify({ epoch: 3 }));
    fs.chmodSync(path.join(root, MESH_RETENTION_APPROVAL), 0o666);
    expect((await sweepMeshRetention(root, options)).skipped).toEqual([{ path: root, reason: expect.stringMatching(/ruling/) }]);
    // The report: a link at its path is replaced, its target never written.
    const report = path.join(root, ".mesh-retention-report.json");
    write(path.join(elsewhere, "target"), "keep");
    fs.symlinkSync(path.join(elsewhere, "target"), report);
    writeReport(report, "{}\n");
    expect(fs.readFileSync(path.join(elsewhere, "target"), "utf8")).toBe("keep");
    expect(fs.lstatSync(report).isFile()).toBe(true);
    expect(fs.readFileSync(report, "utf8")).toBe("{}\n");
    expect(fs.statSync(report).mode & 0o777).toBe(0o600);
  });

  it("re-checks the gate before EACH session-backup unlink, and never accepts an approval whose owner cannot be proven (smarty-dev#7766)", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-backups-")); roots.push(dir);
    const session = path.join(dir, "session.jsonl");
    write(session, "");
    for (const stamp of ["20260927T150000000Z", "20260928T150000000Z", "20260929T150000000Z", "20260930T150000000Z"]) write(`${session}.${stamp}.bak`, "x");
    let asked = 0;
    // A hold lands after the first unlink: the remaining older backups stay.
    expect(pruneActorSessionBackups(session, { stop: () => asked++ > 0 })).toHaveLength(1);
    expect(fs.readdirSync(dir).filter(name => name.endsWith(".bak"))).toHaveLength(3);
    // No getuid (Windows): even our own private approval naming the right epoch is refused.
    const { root } = make();
    write(path.join(root, "state.json"), JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3, at: "2026-10-09T15:29:19.869Z" }));
    write(path.join(root, "state.db"), "");
    write(path.join(root, MESH_RETENTION_APPROVAL), JSON.stringify({ epoch: 3 }));
    fs.chmodSync(path.join(root, MESH_RETENTION_APPROVAL), 0o600);
    // An unswitched mesh too (no marker): --apply refuses, a dry run still previews.
    const { root: plain } = make();
    const before = snapshot(plain);
    const getuid = process.getuid;
    Object.defineProperty(process, "getuid", { value: undefined, configurable: true, writable: true });
    try {
      for (const where of [root, plain]) {
        const result = await sweepMeshRetention(where, { now: 30 * 86400000, dryRun: false, runRetentionMs: 7 * 86400000 });
        expect(result.changes, where).toEqual([]);
        expect(result.skipped, where).toEqual([{ path: where, reason: expect.stringMatching(/ownership cannot be proven/) }]);
      }
      expect(snapshot(plain)).toEqual(before);
    } finally { Object.defineProperty(process, "getuid", { value: getuid, configurable: true, writable: true }); }
  });

  it("deletes nothing under an operator hold or after a backend switch until a ruling names the new epoch (smarty-dev#7766)", async () => {
    const { root } = make();
    const options = { now: 30 * 86400000, dryRun: false, runRetentionMs: 7 * 86400000 };
    write(path.join(root, MESH_RETENTION_HOLD), "{}");
    let before = snapshot(root);
    expect((await sweepMeshRetention(root, options)).skipped).toEqual([{ path: root, reason: expect.stringMatching(/held/) }]);
    expect(snapshot(root)).toEqual(before);
    fs.unlinkSync(path.join(root, MESH_RETENTION_HOLD));
    write(path.join(root, "state.json"), JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3, at: "2026-10-09T15:29:19.869Z" }));
    write(path.join(root, "state.db"), "");
    write(path.join(root, MESH_RETENTION_APPROVAL), JSON.stringify({ epoch: 2 }));
    before = snapshot(root);
    expect((await sweepMeshRetention(root, options)).skipped).toEqual([{ path: root, reason: expect.stringMatching(/epoch 3.*ruling/) }]);
    expect(snapshot(root)).toEqual(before);
    expect((await sweepMeshRetention(root, { ...options, dryRun: true })).changes.length).toBeGreaterThan(0);
    write(path.join(root, MESH_RETENTION_APPROVAL), JSON.stringify({ epoch: 3 }));
    expect((await sweepMeshRetention(root, options)).changes.length).toBeGreaterThan(0);
  });

  it("reads state.json itself: a link, malformed JSON, an unknown shape or a writable file refuses --apply; a legacy file state is unswitched (smarty-dev#7766)", async () => {
    const options = { now: 30 * 86400000, dryRun: false, runRetentionMs: 7 * 86400000 };
    const marker = JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3, at: "2026-10-09T15:29:19.869Z" });
    const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-state-")); roots.push(elsewhere);
    write(path.join(elsewhere, "marker.json"), marker);
    const cases: Array<[string, (file: string) => void, RegExp]> = [
      ["symlink to a valid marker", file => fs.symlinkSync(path.join(elsewhere, "marker.json"), file), /symbolic link/],
      ["malformed JSON", file => write(file, "{\"format\":1,"), /malformed JSON/],
      ["foreign-shaped object", file => write(file, JSON.stringify({ format: "other", entries: [] })), /neither a file-backend state nor a moved marker/],
      ["mode 0666 file", file => { write(file, JSON.stringify({ format: 1, entries: {} })); fs.chmodSync(file, 0o666); }, /group- or world-writable/],
    ];
    for (const [name, place, reason] of cases) {
      const { root } = make();
      place(path.join(root, "state.json"));
      const before = snapshot(root);
      const result = await sweepMeshRetention(root, options);
      expect(result.changes, name).toEqual([]);
      expect(result.skipped, name).toEqual([{ path: root, reason: expect.stringMatching(reason) }]);
      expect(snapshot(root), name).toEqual(before);
    }
    // A valid legacy file-backend state is an unswitched mesh: --apply proceeds as before.
    const { root: legacy } = make();
    write(path.join(legacy, "state.json"), JSON.stringify({ readGeneration: "00000000-0000-4000-8000-000000000000", backendEpoch: 1, format: 2, entries: {} }));
    fs.chmodSync(path.join(legacy, "state.json"), 0o600);
    const applied = await sweepMeshRetention(legacy, options);
    expect(applied.skipped).toEqual([]);
    expect(applied.changes.length).toBeGreaterThan(0);
  });

  it("refuses ambiguous identities and linked actor-reference trees without changing them", async () => {
    const { root, host } = make(); const second = path.join(root, "residency", "second");
    write(path.join(second, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(second, "config.json"), JSON.stringify({ format: 1, rootId: "session:proof", residencyRoot: second, meshRoot: root }));
    let before = snapshot(root);
    expect((await sweepMeshRetention(root, { dryRun: false })).changes).toEqual([]);
    expect(snapshot(root)).toEqual(before);
    fs.symlinkSync(path.join(root, "actors", "project"), path.join(root, "actors", "linked"), "dir"); before = snapshot(root);
    await expect(sweepMeshRetention(root, { dryRun: false })).rejects.toThrow(/uncertain/);
    expect(snapshot(root)).toEqual(before);
    expect(fs.existsSync(path.join(host, "runs", "terminal"))).toBe(true);
  });

  it.each(["live", "missing lock", "unknown lock", "unknown owner", "unknown registry", "unknown config"])("never changes a mesh with %s custody", async kind => {
    const { root, host } = make();
    if (kind === "live") write(path.join(host, "host.lock"), JSON.stringify({ pid: process.pid }));
    if (kind === "missing lock") fs.unlinkSync(path.join(host, "host.lock"));
    if (kind === "unknown lock") write(path.join(host, "host.lock"), "{");
    if (kind === "unknown owner") write(path.join(host, "owner.json"), "{}");
    if (kind === "unknown config") write(path.join(host, "config.json"), "{}");
    if (kind === "unknown registry") write(path.join(root, "actors", "project", "actors.json"), "{");
    const before = snapshot(root);
    const result = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: false });
    expect(result.changes).toEqual([]);
    expect(snapshot(root)).toEqual(before);
    expect(result.skipped.length).toBeGreaterThan(0);
  });
});
