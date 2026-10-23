import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MESH_RETENTION_APPLY_DISABLED_CODE, MESH_RETENTION_APPROVAL, MESH_RETENTION_HOLD, MeshRetentionApplyDisabledError, sweepMeshRetention, writeReport } from "../src/storage/retention-cli.js";
import { pruneActorSessionBackups } from "../src/storage/retention.js";
import { lockFile } from "../src/residency/file-lock.js";
import { spawnSync } from "node:child_process";
import { meshRetentionSweepArgs } from "../src/actors/manager.js";

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
  // Rotation order is trusted data (smarty-dev#7916): mtimes follow the rotations, never one shared millisecond.
  fs.utimesSync(path.join(actor, "session.jsonl.20260927T150000000Z.bak"), 1_000, 1_000);
  fs.utimesSync(path.join(actor, "session.jsonl.20260928T150000000Z.bak"), 1_001, 1_001);
  write(path.join(actor, "session.jsonl"), "live");
  // A valid, empty mesh state: --apply refuses on any doubt about state.json (smarty-dev#7916).
  write(path.join(root, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
  return { root, host, actor };
};
/** smarty-dev#10678 (pi-fabric#1014 SEC P2-1): the public apply rejects at its boundary, typed, and changes nothing. */
const applyRejects = async (root: string, options: { now?: number; runRetentionMs?: number } = {}) => {
  const before = snapshot(root);
  await expect(sweepMeshRetention(root, { ...options, dryRun: false })).rejects.toMatchObject({
    name: "MeshRetentionApplyDisabledError", code: MESH_RETENTION_APPLY_DISABLED_CODE, message: expect.stringMatching(/smarty-dev#10678/),
  });
  await expect(sweepMeshRetention(root, { ...options, dryRun: false })).rejects.toBeInstanceOf(MeshRetentionApplyDisabledError);
  expect(snapshot(root)).toEqual(before);
};

describe.skipIf(process.platform !== "linux")("offline retained mesh sweep", () => {
  it("lists exact bytes with a whole-tree no-op dry run under existing native fences", async () => {
    const { root, actor, host } = make(); const before = snapshot(root);
    const plan = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: true });
    expect(snapshot(root)).toEqual(before);
    expect(plan.skipped).toEqual([]);
    expect(plan.changes).toHaveLength(2);
    expect(plan.bytesBefore).toBeGreaterThan(plan.bytesAfter);
    expect(plan.changes.map(change => change.path).sort()).toEqual([
      path.join(host, "runs", "terminal", "events.jsonl"),
      path.join(actor, "runs", "archive", "events.jsonl"),
    ].sort());
    expect(plan.summary.applyRefusal).toBeUndefined();
    await applyRejects(root, { now: 7 * 3600000 });
    expect(fs.existsSync(path.join(actor, "session.jsonl.20260927T150000000Z.bak"))).toBe(true);
  });

  it("enumerates an owned actor registry larger than 1 MiB in a dry run without losing latest-run vetoes", async () => {
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
    ].sort());
    fs.mkdirSync(`${file}.lock`);
    const locked = snapshot(root);
    expect((await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: true })).skipped).toEqual([
      expect.objectContaining({ path: path.dirname(file), reason: expect.stringMatching(/lock/) }),
    ]);
    expect(snapshot(root)).toEqual(locked);
  });

  it.each([undefined, "unknown", "herdr", "localterm", "tmux", "screen"])("never reports or changes a root with %s transport and no persisted native exit receipt", async transport => {
    const { root, host, actor } = make();
    const unsafe = [path.join(host, "runs", "terminal"), path.join(actor, "runs", "archive")];
    for (const run of unsafe) write(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport, finishedAt: 1, sessionId: "2147483647", exitCode: 0 }));
    const before = unsafe.map(run => ({ directoryMtime: fs.statSync(run).mtimeMs, tree: snapshot(run) }));
    const result = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: true });
    expect(result.changes.filter(change => unsafe.some(run => change.path.startsWith(run + path.sep)))).toEqual([]);
    expect(unsafe.map(run => ({ directoryMtime: fs.statSync(run).mtimeMs, tree: snapshot(run) }))).toEqual(before);
    await applyRejects(root, { now: 7 * 3600000 });
    expect(unsafe.map(run => ({ directoryMtime: fs.statSync(run).mtimeMs, tree: snapshot(run) }))).toEqual(before);
  });

  it("does not displace a kernel-fenced holder even with stale dead-PID diagnostics", async () => {
    const { root, host } = make(); const fd = await lockFile(path.join(host, "host.lock"), 0, true);
    try {
      const before = snapshot(root);
      expect((await sweepMeshRetention(root, { dryRun: true })).changes).toEqual([]);
      expect(snapshot(root)).toEqual(before);
    } finally { fs.closeSync(fd); }
  });

  it("holds a dead root's host.lock only around that root's own sweep, so a waking host can claim it during the mesh-wide pass (smarty-dev#7766)", async () => {
    const { root, host } = make();
    // A waking host's claim: flock(1) non-blocking on the same host.lock, from another process.
    const claimable = () => spawnSync("flock", ["-x", "-n", path.join(host, "host.lock"), "true"]).status === 0;
    const claims: boolean[] = [];
    // The sweep reads runRetentionMs inside the root's fenced sweep and again before the mesh-wide pass.
    const options = { now: 30 * 86400000, dryRun: true, get runRetentionMs() { claims.push(claimable()); return 7 * 86400000; } };
    const result = await sweepMeshRetention(root, options);
    expect(result.skipped).toEqual([]);
    expect(claims[0]).toBe(false);
    expect(claims.at(-1)).toBe(true);
    expect(claimable()).toBe(true);
  });

  it("a host waiting for a root's lock wins that root mid-sweep (smarty-dev#7766)", async () => {
    const { root, host } = make();
    const runs = snapshot(path.join(host, "runs"));
    // residentRunRetentionMs is read inside the dead root's fenced sweep, before its first deletion.
    const options = { now: 30 * 86400000, dryRun: true, runRetentionMs: 7 * 86400000, get residentRunRetentionMs() {
      // A waking host: flock(1) blocks on host.lock in the background, so /proc/locks lists a waiter.
      spawnSync("sh", ["-c", `flock -x -w 20 '${path.join(host, "host.lock")}' true </dev/null >/dev/null 2>&1 & sleep 0.3`]);
      return 86400000;
    } };
    const result = await sweepMeshRetention(root, options);
    // A waking host stops its own root's work. The mesh-wide actor pass is independent of host liveness by design
    // (pi-fabric#645): it keeps its own registry fences.
    expect(result.changes.filter(change => change.path.startsWith(host))).toEqual([]);
    expect(result.skipped.map(item => item.reason).join(" ")).toMatch(/host is waking/);
    expect(snapshot(path.join(host, "runs"))).toEqual(runs);
  });

  it("a hold landing mid-sweep cannot be reached by apply: it rejects before arming the seam (smarty-dev#10678)", async () => {
    const { root } = make();
    let armed = false;
    // The former mid-sweep hold seam: never read, because apply rejects before the sweep starts.
    const options = { now: 30 * 86400000, dryRun: false, runRetentionMs: 7 * 86400000, get residentRunRetentionMs() {
      armed = true; write(path.join(root, MESH_RETENTION_HOLD), "{}"); return 86400000;
    } };
    const before = snapshot(root);
    await expect(sweepMeshRetention(root, options))
      .rejects.toBeInstanceOf(MeshRetentionApplyDisabledError);
    expect(armed).toBe(false);
    expect(snapshot(root)).toEqual(before);
  });

  it("reads the epoch approval only from our own regular file, never through a link", async () => {
    const { root } = make();
    write(path.join(root, "state.json"), JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3, at: "2026-10-09T15:29:19.869Z" }));
    write(path.join(root, "state.db"), "");
    const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-approval-")); roots.push(elsewhere);
    write(path.join(elsewhere, "ok.json"), JSON.stringify({ epoch: 3 }));
    fs.symlinkSync(path.join(elsewhere, "ok.json"), path.join(root, MESH_RETENTION_APPROVAL));
    const before = snapshot(root);
    // The gate is reported by the dry run's summary; the public apply rejects before it (smarty-dev#10678).
    const options = { now: 30 * 86400000, dryRun: true, runRetentionMs: 7 * 86400000 };
    expect((await sweepMeshRetention(root, options)).summary.applyRefusal).toMatch(/ruling/);
    expect(snapshot(root)).toEqual(before);
    await applyRejects(root, options);
    fs.unlinkSync(path.join(root, MESH_RETENTION_APPROVAL));
    write(path.join(root, MESH_RETENTION_APPROVAL), JSON.stringify({ epoch: 3 }));
    fs.chmodSync(path.join(root, MESH_RETENTION_APPROVAL), 0o666);
    expect((await sweepMeshRetention(root, options)).summary.applyRefusal).toMatch(/ruling/);
    await applyRejects(root, options);
  });

  it("writes the report atomically without following a link", () => {
    const { root } = make();
    const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-report-")); roots.push(elsewhere);
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

  it("retains raw session backups even when the sweep stop gate is requested", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-backups-")); roots.push(dir);
    const session = path.join(dir, "session.jsonl");
    write(session, "");
    for (const stamp of ["20260927T150000000Z", "20260928T150000000Z", "20260929T150000000Z", "20260930T150000000Z"]) write(`${session}.${stamp}.bak`, "x");
    let asked = 0;
    // A hold lands after the first unlink: the remaining older backups stay.
    expect(pruneActorSessionBackups(session, { stop: () => asked++ > 0 })).toEqual([]);
    expect(asked).toBe(2);
    expect(fs.readdirSync(dir).filter(name => name.endsWith(".bak"))).toHaveLength(4);
  });

  it("never accepts an approval whose owner cannot be proven (smarty-dev#7766)", async () => {
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
        // The gate is reported by the dry run's summary; the public apply rejects before it (smarty-dev#10678).
        const result = await sweepMeshRetention(where, { now: 30 * 86400000, dryRun: true, runRetentionMs: 7 * 86400000 });
        expect(result.summary.applyRefusal, where).toMatch(/ownership cannot be proven/);
        await applyRejects(where, { now: 30 * 86400000, runRetentionMs: 7 * 86400000 });
      }
      expect(snapshot(plain)).toEqual(before);
    } finally { Object.defineProperty(process, "getuid", { value: getuid, configurable: true, writable: true }); }
  });

  it("deletes nothing under an operator hold or after a backend switch until a ruling names the new epoch (smarty-dev#7766)", async () => {
    const { root } = make();
    // Each gate is reported by the dry run's summary; the public apply rejects before any of them (smarty-dev#10678).
    const options = { now: 30 * 86400000, dryRun: true, runRetentionMs: 7 * 86400000 };
    write(path.join(root, MESH_RETENTION_HOLD), "{}");
    let before = snapshot(root);
    expect((await sweepMeshRetention(root, options)).summary.applyRefusal).toMatch(/held/);
    expect(snapshot(root)).toEqual(before);
    await applyRejects(root, options);
    fs.unlinkSync(path.join(root, MESH_RETENTION_HOLD));
    write(path.join(root, "state.json"), JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3, at: "2026-10-09T15:29:19.869Z" }));
    write(path.join(root, "state.db"), "");
    write(path.join(root, MESH_RETENTION_APPROVAL), JSON.stringify({ epoch: 2 }));
    before = snapshot(root);
    const switched = await sweepMeshRetention(root, options);
    expect(switched.summary.applyRefusal).toMatch(/epoch 3.*ruling/);
    expect(switched.changes.length).toBeGreaterThan(0);
    expect(snapshot(root)).toEqual(before);
    await applyRejects(root, options);
    write(path.join(root, MESH_RETENTION_APPROVAL), JSON.stringify({ epoch: 3 }));
    const approved = await sweepMeshRetention(root, options);
    expect(approved.summary.applyRefusal).toBeUndefined();
    expect(approved.changes.length).toBeGreaterThan(0);
    // Even with every gate open, the public apply stays refused until smarty-dev#10678.
    await applyRejects(root, options);
  });

  it("--apply refuses until #10678 and changes nothing; the dry run previews and the daily sweep only reports (smarty-dev#7916)", () => {
    const { root, actor } = make();
    const cli = path.resolve("src/storage/retention-cli.ts");
    const runner = `const { createJiti } = require("jiti"); const { pathToFileURL } = require("node:url");
process.argv = [process.argv[0], ${JSON.stringify(cli)}, ...process.argv.slice(1)];
createJiti(pathToFileURL(process.cwd() + "/index.js").href).import(${JSON.stringify(cli)}).catch(error => { console.error(error); process.exitCode = 1; });`;
    const old = path.join(actor, "session.jsonl.20260927T150000000Z.bak");
    const report = path.join(root, "report.json");
    const dry = spawnSync(process.execPath, ["-e", runner, root, "--dry-run", "--report", report], { encoding: "utf8", timeout: 60_000 });
    expect(dry.status, dry.stderr).toBe(0);
    expect(dry.stderr).toMatch(/^pruneActorSessionBackups: would delete 0 \(0 bytes\), refused 1$/m);
    expect(dry.stderr).toMatch(/^  refused 1 \(0\.000 GiB\): raw session transcript retained$/m);
    expect(dry.stderr).toMatch(/^WOULD_FREE_GIB=\d+\.\d{3}$/m);
    expect(fs.existsSync(old)).toBe(true);
    const prior = fs.readFileSync(report, "utf8");
    const result = spawnSync(process.execPath, ["-e", runner, root, "--apply", "--report", report], { encoding: "utf8", timeout: 60_000 });
    expect(result.status, result.stderr).toBe(2);
    expect(result.stderr).toMatch(/--apply is disabled until smarty-dev#10678/);
    expect(fs.readFileSync(report, "utf8")).toBe(prior);
    expect(fs.existsSync(old)).toBe(true);
    const argv = meshRetentionSweepArgs(root, 7 * 86400000);
    expect(argv).toEqual([root, "--dry-run", "--runs-older-than", String(7 * 86400000), "--report", path.join(root, ".mesh-retention-report.json")]);
  });

  it("refuses ambiguous identities and linked actor-reference trees without changing them", async () => {
    const { root, host } = make(); const second = path.join(root, "residency", "second");
    write(path.join(second, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(second, "config.json"), JSON.stringify({ format: 1, rootId: "session:proof", residencyRoot: second, meshRoot: root }));
    let before = snapshot(root);
    expect((await sweepMeshRetention(root, { dryRun: true })).changes).toEqual([]);
    expect(snapshot(root)).toEqual(before);
    fs.symlinkSync(path.join(root, "actors", "project"), path.join(root, "actors", "linked"), "dir"); before = snapshot(root);
    await expect(sweepMeshRetention(root, { dryRun: true })).rejects.toThrow(/uncertain/);
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
    const result = await sweepMeshRetention(root, { now: 7 * 3600000, dryRun: true });
    expect(result.changes).toEqual([]);
    expect(snapshot(root)).toEqual(before);
    expect(result.skipped.length).toBeGreaterThan(0);
  });
});
