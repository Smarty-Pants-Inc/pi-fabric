import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { summaryLines, sweepMeshRetention } from "../src/storage/retention-cli.js";
import { MeshStore } from "../src/mesh/store.js";
import { claimMeshRetentionSweep, MESH_RETENTION_SWEEP_PREFIX, recordMeshRetentionSweeper, removeUnreferencedRun } from "../src/storage/retention.js";
import { MESH_RETENTION_SWEEP_INTERVAL_MS } from "../src/actors/manager.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { appendResidentLog } from "../src/residency/launcher.js";

// smarty-dev#3252: age-based removal of terminal runs mesh-wide, independent of the owner.
const DAY = 24 * 60 * 60 * 1_000;
const NOW = Date.now();
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const tmp = (prefix: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root); return root; };
const run = (directory: string, status: Record<string, unknown>) => {
  write(path.join(directory, "status.json"), JSON.stringify({ transport: "process", sessionId: "2147483647", ...status }));
  write(path.join(directory, "events.jsonl"), '{"sequence":1}\n');
  write(path.join(directory, "reply.json"), '{"text":"done"}');
  return directory;
};

/** A dead Main's actors: no live owner, no resident root, so no owner sweep ever visits them (#5652). */
const deadRootMesh = (actorStatus = "idle") => {
  const root = tmp("fabric-mesh-age-");
  const registry = path.join(root, "actors", "01a0cd9c-dead-session");
  const actor = path.join(registry, "actor");
  write(path.join(registry, "actors.json"), JSON.stringify({ actors: [{
    id: "actor", rootId: "session:01a0cd9c-dead-session", status: actorStatus, sessionFile: path.join(actor, "session.jsonl"),
    lastRunId: "latest", ...(actorStatus === "running" ? { inFlightRun: { id: "inflight", startedAt: NOW - 9 * DAY, ageS: 1 } } : {}),
  }] }));
  const runs = path.join(actor, "runs");
  const old = run(path.join(runs, "old"), { status: "completed", finishedAt: NOW - 8 * DAY });
  const latest = run(path.join(runs, "latest"), { status: "completed", finishedAt: NOW - 9 * DAY });
  const young = run(path.join(runs, "young"), { status: "completed", finishedAt: NOW - 2 * DAY });
  const inflight = run(path.join(runs, "inflight"), { status: "running", updatedAt: NOW - 9 * DAY });
  // A removed actor's directory: no registry row, no latest run to keep.
  const orphan = run(path.join(registry, "removed-actor", "runs", "orphan"), { status: "failed", finishedAt: NOW - 10 * DAY });
  for (const [order, stamp] of ["20260920T150000000Z", "20260921T150000000Z", "20260922T150000000Z"].entries()) {
    write(path.join(actor, `session.jsonl.${stamp}.bak`), stamp);
    // Rotation order is trusted data (smarty-dev#7916): mtimes follow the rotations, never one shared millisecond.
    fs.utimesSync(path.join(actor, `session.jsonl.${stamp}.bak`), 1_000 + order, 1_000 + order);
  }
  write(path.join(actor, "session.jsonl"), "live");
  return { root, registry, actor, runs, old, latest, young, inflight, orphan };
};

// Every sweep here is a dry run: apply is disabled until smarty-dev#7916 (sweepMeshRetention throws on dryRun:false).
// Each fence still decides what the plan lists; `applied` names the plan.
describe("mesh-wide age-based run retention", () => {
  // Dry run (smarty-dev#7916): the plan lists what an apply would remove; the disk is unchanged.
  it("plans a dead root's old terminal runs, keeps its latest, in-flight and young runs, and caps .bak files", async () => {
    const mesh = deadRootMesh();
    const plan = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(plan.skipped).toEqual([]);
    expect(plan.removedRuns.sort()).toEqual([mesh.old, mesh.orphan].sort());
    expect(plan.changes.map(change => change.path).filter(file => file.endsWith(".bak")).sort()).toEqual(
      ["20260920T150000000Z", "20260921T150000000Z"].map(stamp => path.join(mesh.actor, `session.jsonl.${stamp}.bak`)));
    for (const kept of [mesh.old, mesh.orphan, mesh.latest, mesh.young, mesh.inflight]) expect(fs.existsSync(path.join(kept, "reply.json"))).toBe(true);
    expect(fs.readdirSync(mesh.actor).filter(name => name.endsWith(".bak"))).toHaveLength(3);
  });

  // pi-fabric#850: the live dry run reported 7627 runs as "not terminal or run tree not proven safe". The report now
  // names the check that refused each run, and the bytes each refusal class holds.
  it("reports which check refused each expired run, with the bytes it holds", async () => {
    const mesh = deadRootMesh();
    write(path.join(mesh.old, "status.json"), JSON.stringify({ status: "completed", finishedAt: NOW - 8 * DAY }));
    write(path.join(mesh.orphan, "surprise.bin"), "x".repeat(1000));
    const plan = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    const item = plan.summary.deleters.pruneActorRunArchives!;
    expect(item.refused).toMatchObject({ "worker exit is unconfirmed: unknown root identity": 1, "unknown entry surprise.bin": 1, "status is running": 1 });
    expect(item.refusedBytes["unknown entry surprise.bin"]).toBeGreaterThan(1000);
    expect(summaryLines(true, plan.summary).join("\n")).toMatch(/refused 1 \(0\.000 GiB\): unknown entry surprise\.bin/);
  });

  it("without runRetentionMs removes nothing (compaction-only contract unchanged)", async () => {
    const mesh = deadRootMesh();
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true });
    expect(applied.removedRuns).toEqual([]);
    expect(fs.existsSync(mesh.old)).toBe(true);
  });

  it("never removes an in-flight run or another registry's latest run, and keeps .bak history of a running actor", async () => {
    const mesh = deadRootMesh("running");
    // The in-flight run is recorded terminal on disk but still referenced by its registry row.
    run(mesh.inflight, { status: "completed", finishedAt: NOW - 9 * DAY });
    // Another registry (a live owner that adopted the run) references "old" as its latest run.
    const other = path.join(mesh.root, "actors", "01a118ab-live-session");
    write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle", lastRunId: "old" }] }));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toEqual([mesh.orphan]);
    for (const kept of [mesh.old, mesh.latest, mesh.young, mesh.inflight]) expect(fs.existsSync(kept)).toBe(true);
    expect(fs.readdirSync(mesh.actor).filter(name => name.endsWith(".bak"))).toHaveLength(3);
  });

  // pi-fabric#645 review round 1: the final pre-delete check re-reads every registry's in-flight runs.
  const afterFirstSnapshot = (candidate: string, change: () => void) => {
    const original = fs.readFileSync;
    let done = false;
    // The candidate's status.json is read after the sweep's first reference snapshot and its
    // early checks, and before the final pre-delete check.
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (!done && file === path.join(candidate, "status.json")) { done = true; change(); }
      return (original as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.readFileSync);
    return () => done;
  };

  it("keeps a run another registry marks in flight after the sweep's first snapshot", async () => {
    const mesh = deadRootMesh();
    const other = path.join(mesh.root, "actors", "01a118ab-live-session");
    write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle" }] }));
    const injected = afterFirstSnapshot(mesh.old, () => write(path.join(other, "actors.json"), JSON.stringify({ actors: [{
      id: "adopter", status: "running", inFlightRun: { id: "old", startedAt: NOW, ageS: 0 },
    }] })));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).toEqual([mesh.orphan]);
    expect(fs.existsSync(path.join(mesh.old, "reply.json"))).toBe(true);
  });

  it("keeps runs other registries hold as preparing or pending-removal runs", async () => {
    const mesh = deadRootMesh();
    const young = run(path.join(mesh.runs, "removing"), { status: "completed", finishedAt: NOW - 8 * DAY });
    write(path.join(mesh.root, "actors", "01a118ab-live-session", "actors.json"), JSON.stringify({ actors: [
      { id: "preparer", status: "preparing", preparing: { phase: "admission", startedAt: NOW, attempts: 1, runId: "old" } },
      { id: "remover", status: "running", removal: { requestedAt: NOW, runId: "removing" } },
    ] }));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toEqual([mesh.orphan]);
    for (const kept of [mesh.old, young]) expect(fs.existsSync(kept)).toBe(true);
  });

  it("fails closed when a registry becomes unreadable before the final pre-delete check", async () => {
    const mesh = deadRootMesh();
    const other = path.join(mesh.root, "actors", "01a118ab-live-session");
    write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle" }] }));
    const injected = afterFirstSnapshot(mesh.old, () => fs.rmSync(path.join(other, "actors.json")));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(mesh.old);
    expect(fs.existsSync(mesh.old)).toBe(true);
  });

  it("an unreadable registry is a wildcard veto: nothing is removed", async () => {
    const mesh = deadRootMesh();
    write(path.join(mesh.root, "actors", "broken", "actors.json"), "{not json");
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toEqual([]);
    expect(fs.existsSync(mesh.old)).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("a reused PID no longer vetoes forever; the saved live worker still does", async () => {
    const mesh = deadRootMesh();
    // A live PID without a saved identity is unknown, not proven reuse: still vetoed.
    const unknown = run(path.join(mesh.runs, "unknown"), { status: "completed", finishedAt: NOW - 8 * DAY, sessionId: String(process.pid) });
    // Same live PID with its saved start identity: the run's own worker may still be running.
    const own = run(path.join(mesh.runs, "own"), { status: "completed", finishedAt: NOW - 8 * DAY, sessionId: String(process.pid),
      processStartTime: processStartTime(process.pid) });
    // A saved identity that differs from the live process: reuse.
    const differs = run(path.join(mesh.runs, "differs"), { status: "completed", finishedAt: NOW - 8 * DAY, sessionId: String(process.pid),
      processStartTime: "1" });
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toContain(differs);
    expect(applied.removedRuns).not.toContain(own);
    expect(applied.removedRuns).not.toContain(unknown);
    for (const kept of [differs, own]) expect(fs.existsSync(kept)).toBe(true);
    expect(fs.existsSync(unknown)).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("plans a proven-dead resident root's old runs under its flock, not a young one", async () => {
    const mesh = deadRootMesh();
    const host = path.join(mesh.root, "residency", "host");
    write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:dead-host", residencyRoot: host, meshRoot: mesh.root }));
    const old = run(path.join(host, "runs", "old-resident"), { status: "completed", finishedAt: NOW - 2 * DAY });
    const young = run(path.join(host, "runs", "young-resident"), { status: "completed", finishedAt: NOW - 1_000 });
    fs.utimesSync(old, (NOW - 2 * DAY) / 1000, (NOW - 2 * DAY) / 1000);
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toContain(old);
    expect(applied.removedRuns).not.toContain(young);
    for (const kept of [old, young]) expect(fs.existsSync(kept)).toBe(true);
  });
});

// pi-fabric#645 review round 2: the dead-resident sweep runs the same final pre-delete check.
describe.skipIf(process.platform !== "linux")("dead-resident sweep final pre-delete check", () => {
  const deadResident = () => {
    const mesh = deadRootMesh();
    const host = path.join(mesh.root, "residency", "host");
    write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:dead-host", residencyRoot: host, meshRoot: mesh.root }));
    const resident = run(path.join(host, "runs", "old-resident"), { status: "completed", finishedAt: NOW - 2 * DAY });
    fs.utimesSync(resident, (NOW - 2 * DAY) / 1000, (NOW - 2 * DAY) / 1000);
    const other = path.join(mesh.root, "actors", "01a118ab-live-session");
    write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle" }] }));
    return { mesh, resident, other };
  };
  const afterSnapshot = (candidate: string, change: () => void) => {
    const original = fs.readFileSync;
    let done = false;
    // The resident run's status.json is first read by the exit/tree checks, after the sweep's
    // reference snapshot and before its final pre-delete check.
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (!done && file === path.join(candidate, "status.json")) { done = true; change(); }
      return (original as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.readFileSync);
    return () => done;
  };

  it("plans the dead resident's old run when nothing references it (control)", async () => {
    const { mesh, resident } = deadResident();
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toContain(resident);
    expect(fs.existsSync(resident)).toBe(true);
  });

  // pi-fabric#645 review round 3: the default CLI mode (--runs-older-than, dry run) previews the
  // dead-resident runs --apply deletes, through the same selection and fences, without mutating.
  const snapshot = (root: string): unknown[] => fs.readdirSync(root).sort().map(name => {
    const file = path.join(root, name); const stat = fs.lstatSync(file);
    return [name, stat.ino, stat.mtimeMs, stat.ctimeMs, stat.isDirectory() ? snapshot(file) : fs.readFileSync(file, "utf8")];
  });

  it("dry run lists the dead-resident runs --apply then deletes, and leaves the disk unchanged", async () => {
    const { mesh, resident } = deadResident();
    const host = path.join(mesh.root, "residency", "host");
    const young = run(path.join(host, "runs", "young-resident"), { status: "completed", finishedAt: NOW - 1_000 });
    const before = snapshot(mesh.root);
    const plan = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(snapshot(mesh.root)).toEqual(before);
    expect(plan.skipped).toEqual([]);
    expect(plan.removedRuns).toContain(resident);
    expect(plan.removedRuns).not.toContain(young);
    expect(plan.changes.find(change => change.path === resident)).toMatchObject({ afterBytes: 0 });
    expect(plan.changes.find(change => change.path === resident)!.beforeBytes).toBeGreaterThan(0);
    // ponytail: the apply half (it deletes exactly the plan) is disabled until smarty-dev#7916; re-enable there.
  });

  it("dry run fails closed like --apply when a registry becomes unreadable after the snapshot", async () => {
    const { mesh, resident, other } = deadResident();
    const injected = afterSnapshot(resident, () => write(path.join(other, "actors.json"), "{not json"));
    const plan = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(plan.removedRuns).not.toContain(resident);
    expect(fs.existsSync(path.join(resident, "reply.json"))).toBe(true);
  });

  for (const [name, row] of [
    ["latest", { status: "idle", lastRunId: "old-resident" }],
    ["in-flight", { status: "running", inFlightRun: { id: "old-resident", startedAt: NOW, ageS: 0 } }],
    ["preparing", { status: "preparing", preparing: { phase: "admission", startedAt: NOW, attempts: 1, runId: "old-resident" } }],
    ["pending-removal", { status: "running", removal: { requestedAt: NOW, runId: "old-resident" } }],
  ] as const) {
    it(`keeps a dead resident's run another registry references as its ${name} run after the snapshot`, async () => {
      const { mesh, resident, other } = deadResident();
      const injected = afterSnapshot(resident, () =>
        write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", ...row }] })));
      const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
      expect(injected()).toBe(true);
      expect(applied.removedRuns).not.toContain(resident);
      expect(fs.existsSync(path.join(resident, "reply.json"))).toBe(true);
    });
  }

  for (const [name, change] of [
    ["unreadable", (other: string) => write(path.join(other, "actors.json"), "{not json")],
    ["vanished", (other: string) => fs.rmSync(path.join(other, "actors.json"))],
  ] as const) {
    it(`deletes nothing of the dead resident when a registry becomes ${name} after the snapshot`, async () => {
      const { mesh, resident, other } = deadResident();
      const injected = afterSnapshot(resident, () => change(other));
      const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
      expect(injected()).toBe(true);
      expect(applied.removedRuns.filter(item => item.startsWith(path.join(mesh.root, "residency")))).toEqual([]);
      expect(fs.existsSync(path.join(resident, "reply.json"))).toBe(true);
    });
  }
});

// pi-fabric#645 review round 4: the final pre-delete check is fenced against registry roots created
// after discovery and against references added during the slow size scan.
describe("final pre-delete check fences", () => {
  const onStatusRead = (candidate: string, change: () => void) => {
    const original = fs.readFileSync;
    let done = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (!done && file === path.join(candidate, "status.json")) { done = true; change(); }
      return (original as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.readFileSync);
    return () => done;
  };
  // Fires inside the CLI's recursive size scan (treeBytes) of the candidate, and nowhere else.
  const duringSizeScan = (candidate: string, change: () => void) => {
    const original = fs.lstatSync;
    let done = false;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (!done && String(file).startsWith(candidate + path.sep) && new Error().stack?.includes("treeBytes")) { done = true; change(); }
      return (original as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.lstatSync);
    return () => done;
  };
  const deadResident = () => {
    const mesh = deadRootMesh();
    const host = path.join(mesh.root, "residency", "host");
    write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:dead-host", residencyRoot: host, meshRoot: mesh.root }));
    const resident = run(path.join(host, "runs", "old-resident"), { status: "completed", finishedAt: NOW - 2 * DAY });
    fs.utimesSync(resident, (NOW - 2 * DAY) / 1000, (NOW - 2 * DAY) / 1000);
    return { mesh, resident };
  };
  const newRoot = (root: string, rows: unknown[]) =>
    write(path.join(root, "actors", "01a118ab-new-session", "actors.json"), JSON.stringify({ actors: rows }));
  const otherRegistry = (root: string) => {
    const other = path.join(root, "actors", "01a118ab-live-session");
    write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle" }] }));
    return other;
  };

  // pi-fabric#850 round 4: strict. Any registry-root change, a new actors.json in a watched directory, or an uncertain
  // walk stops every later delete of the sweep (fail closed). Continuing on a live mesh is smarty-dev#9058.
  const stopped = (root: string) => ({ path: path.join(root, "actors"), reason: expect.stringMatching(/registry roots changed during the sweep/) });
  it("keeps a run when a registry root with a cross-root reference is created after discovery", async () => {
    const mesh = deadRootMesh();
    const injected = onStatusRead(mesh.old, () => newRoot(mesh.root, [{ id: "adopter", status: "running", inFlightRun: { id: "old", startedAt: NOW, ageS: 0 } }]));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(mesh.old);
    expect(fs.existsSync(path.join(mesh.old, "reply.json"))).toBe(true);
    expect(applied.skipped).toContainEqual(stopped(mesh.root));
  });

  it("stops every later delete when an unrelated registry root is created after discovery", async () => {
    const mesh = deadRootMesh();
    const injected = onStatusRead(mesh.old, () => newRoot(mesh.root, []));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(mesh.old);
    expect(applied.skipped).toContainEqual(stopped(mesh.root));
  });

  // Race shape of round 2: a re-walk meets an entry that vanished between the listing and its lstat, or a hard-linked
  // file. Strict: the walk is uncertain and every later delete stops.
  it.each(["an entry that vanishes during the re-walk", "a hard-linked file"])("stops every later delete when a re-walk meets %s", async kind => {
    const mesh = deadRootMesh();
    const session = path.join(mesh.root, "actors", "01a118ab-new-session");
    const injected = onStatusRead(mesh.old, () => {
      write(path.join(session, "actors.json"), JSON.stringify({ actors: [] }));
      if (kind === "a hard-linked file") { write(path.join(session, "note.json"), "{}"); fs.linkSync(path.join(session, "note.json"), path.join(session, "note-copy.json")); }
    });
    if (kind !== "a hard-linked file") {
      const list = fs.readdirSync;
      vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
        const names = (list as (...args: unknown[]) => unknown)(target, ...rest);
        return target === session && Array.isArray(names) && names.every(name => typeof name === "string") ? [...names, "actors.json.4242.tmp"] : names;
      }) as typeof fs.readdirSync);
    }
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(mesh.old);
    expect(applied.skipped).toContainEqual(stopped(mesh.root));
  });

  // Race shape of round 3 (CODE c6106235901, SEC c6106236419): a session directory vanishes during a re-walk, then
  // reappears under the same name with an actors.json that references the run. Strict: the vanished entry makes the
  // walk uncertain, so every later delete stops before the reappearance can matter.
  it.skipIf(process.platform !== "linux")("--apply keeps a run a session directory references after it vanished during a re-walk and reappeared", async () => {
    const mesh = deadRootMesh();
    write(path.join(mesh.root, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
    const flicker = path.join(mesh.root, "actors", "01a118ab-flicker-session");
    write(path.join(flicker, "actors.json"), JSON.stringify({ actors: [] }));
    const away = path.join(tmp("fabric-flicker-away-"), "session");
    let armed = false, moved = false, restored = false;
    // A new session forces a re-walk at mesh.old's final check.
    const injected = onStatusRead(mesh.old, () => { write(path.join(mesh.root, "actors", "01a118ab-new-session", "actors.json"), JSON.stringify({ actors: [] })); armed = true; });
    const lstat = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      // The re-walk's lstat of the flicker directory: it vanishes right then.
      if (armed && !moved && file === flicker) { moved = true; fs.renameSync(flicker, away); }
      return (lstat as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.lstatSync);
    const { verifiedRemoveTestHooks } = await import("../src/storage/verified-remove.js");
    verifiedRemoveTestHooks.beforeOpen = target => {
      // After the re-walk and the watch rebuild, before the delete: it reappears with a reference to the run.
      if (target !== mesh.old || !moved || restored) return;
      restored = true;
      fs.renameSync(away, flicker);
      write(path.join(flicker, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle", lastRunId: "old" }] }));
    };
    try {
      const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
      expect([injected(), moved]).toEqual([true, true]);
      expect(applied.removedRuns).not.toContain(mesh.old);
      expect(fs.readFileSync(path.join(mesh.old, "reply.json"), "utf8")).toBe('{"text":"done"}');
      expect(applied.skipped).toContainEqual({ path: path.join(mesh.root, "actors"), reason: expect.stringMatching(/uncertain/) });
    } finally { delete verifiedRemoveTestHooks.beforeOpen; }
  });

  it("stops every later delete when the registry tree becomes uncertain after discovery", async () => {
    const mesh = deadRootMesh();
    const elsewhere = tmp("fabric-linked-registry-");
    const injected = onStatusRead(mesh.old, () => fs.symlinkSync(elsewhere, path.join(mesh.root, "actors", "01a118ab-linked"), "dir"));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(mesh.old);
    expect(applied.skipped).toContainEqual({ path: path.join(mesh.root, "actors"), reason: expect.stringMatching(/uncertain/) });
  });

  // smarty-dev#7916 (ASK 2 condition a): the per-delete check stats only actors/ and its non-registry children. A new
  // registry at actors/<session>/actors.json that references the run stops the --apply delete, whether its session
  // directory is new, already existed without a registry, or appears after the move into the graveyard. The last is
  // the race shape of round 4 (CODE c6106402825): a listed non-registry directory gains actors.json after the check's
  // probe; the helper's second check (proceed) probes again, stops the sweep and moves the run back.
  for (const when of ["new session directory", "existing session directory", "after the graveyard move", "existing session directory, after the graveyard move"] as const) {
    it.skipIf(process.platform !== "linux")(`--apply keeps a run a new actors/<session>/actors.json references (${when})`, async () => {
      const mesh = deadRootMesh();
      write(path.join(mesh.root, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
      const session = path.join(mesh.root, "actors", "01a118ab-new-session");
      if (when.startsWith("existing session directory")) fs.mkdirSync(session, { recursive: true });
      const rows = [{ id: "adopter", status: "idle", lastRunId: "old" }];
      const create = () => write(path.join(session, "actors.json"), JSON.stringify({ actors: rows }));
      let injected: () => boolean;
      if (when.endsWith("after the graveyard move")) {
        const { verifiedRemoveTestHooks } = await import("../src/storage/verified-remove.js");
        let done = false;
        verifiedRemoveTestHooks.beforeOpen = target => { if (target === mesh.old && !done) { done = true; create(); } };
        injected = () => done;
      } else injected = onStatusRead(mesh.old, create);
      try {
        const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
        expect(injected()).toBe(true);
        expect(applied.removedRuns).not.toContain(mesh.old);
        expect(fs.readFileSync(path.join(mesh.old, "reply.json"), "utf8")).toBe('{"text":"done"}');
        expect(applied.skipped).toContainEqual(stopped(mesh.root));
      } finally {
        const { verifiedRemoveTestHooks } = await import("../src/storage/verified-remove.js");
        delete verifiedRemoveTestHooks.beforeOpen;
      }
    });
  }

  it.skipIf(process.platform !== "linux")("keeps a dead resident's run when a registry root is created after discovery", async () => {
    const { mesh, resident } = deadResident();
    const injected = onStatusRead(resident, () => newRoot(mesh.root, [{ id: "adopter", status: "idle", lastRunId: "old-resident" }]));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(resident);
    expect(fs.existsSync(path.join(resident, "reply.json"))).toBe(true);
    expect(applied.skipped).toContainEqual(stopped(mesh.root));
  });

  it("keeps a run another registry references during the size scan", async () => {
    const mesh = deadRootMesh();
    const other = otherRegistry(mesh.root);
    const injected = duringSizeScan(mesh.old, () => write(path.join(other, "actors.json"), JSON.stringify({ actors: [{
      id: "adopter", status: "running", inFlightRun: { id: "old", startedAt: NOW, ageS: 0 },
    }] })));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(mesh.old);
    expect(applied.changes.map(change => change.path)).not.toContain(mesh.old);
    expect(fs.existsSync(path.join(mesh.old, "reply.json"))).toBe(true);
    expect(applied.removedRuns).toContain(mesh.orphan);
  });

  it.skipIf(process.platform !== "linux")("keeps a dead resident's run another registry references during the size scan", async () => {
    const { mesh, resident } = deadResident();
    const other = otherRegistry(mesh.root);
    const injected = duringSizeScan(resident, () =>
      write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle", lastRunId: "old-resident" }] })));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(resident);
    expect(fs.existsSync(path.join(resident, "reply.json"))).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("re-checks after the graveyard move and restores a run referenced in between", () => {
    const runs = tmp("fabric-tombstone-");
    const target = run(path.join(runs, "target"), { status: "completed", finishedAt: 1 });
    const judged = () => fs.lstatSync(target);
    let calls = 0;
    expect(removeUnreferencedRun(target, judged(), () => ++calls > 1)).toBe(false);
    expect(calls).toBe(2);
    expect(fs.readdirSync(runs)).toEqual(["target"]);
    expect(fs.readFileSync(path.join(target, "reply.json"), "utf8")).toBe('{"text":"done"}');
    expect(removeUnreferencedRun(target, judged(), () => { throw new Error("unreadable"); })).toBe(false);
    expect(removeUnreferencedRun(target, judged(), () => false, true)).toBe(true);
    expect(fs.readdirSync(runs)).toEqual(["target"]);
    expect(removeUnreferencedRun(target, judged(), () => false)).toBe(true);
    expect(fs.readdirSync(runs)).toEqual([]);
  });
});

describe("mesh retention sweep claim", () => {
  it("lets exactly one owner claim each interval, and drops stale slots", () => {
    const root = tmp("fabric-mesh-claim-");
    const hour = 60 * 60 * 1_000;
    expect(claimMeshRetentionSweep(root, hour, 10 * hour)).toBe(true);
    expect(claimMeshRetentionSweep(root, hour, 10 * hour + 5)).toBe(false);
    expect(claimMeshRetentionSweep(root, hour, 10 * hour + hour - 1)).toBe(false);
    expect(claimMeshRetentionSweep(root, hour, 11 * hour)).toBe(true);
    expect(claimMeshRetentionSweep(root, hour, 13 * hour)).toBe(true);
    expect(fs.readdirSync(root).filter(name => name.startsWith(MESH_RETENTION_SWEEP_PREFIX)).sort())
      .toEqual([`${MESH_RETENTION_SWEEP_PREFIX}13.json`]);
    expect(claimMeshRetentionSweep(path.join(root, "absent"), hour, 13 * hour)).toBe(false);
  });

  // smarty-dev#7916 (ops-lead, Ryzen 1 2026-10-11): each sweep took 30-35 min at ~0.7 core, hourly. Daily now, and
  // single flight: no slot is claimed while the previous slot's recorded sweeper provably still runs.
  const DAY = 24 * 60 * 60 * 1_000;
  const slotFile = (root: string, day: number) => path.join(root, `${MESH_RETENTION_SWEEP_PREFIX}${day}.json`);
  it("sweeps once a day: one claim per 24 h slot", () => {
    expect(MESH_RETENTION_SWEEP_INTERVAL_MS).toBe(DAY);
    const root = tmp("fabric-mesh-daily-");
    expect(claimMeshRetentionSweep(root, MESH_RETENTION_SWEEP_INTERVAL_MS, 10 * DAY)).toBe(true);
    for (const hours of [1, 6, 12, 23]) expect(claimMeshRetentionSweep(root, MESH_RETENTION_SWEEP_INTERVAL_MS, 10 * DAY + hours * 3_600_000)).toBe(false);
    expect(claimMeshRetentionSweep(root, MESH_RETENTION_SWEEP_INTERVAL_MS, 11 * DAY)).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("does not claim while the previous slot's sweeper provably still runs", () => {
    const root = tmp("fabric-mesh-single-flight-");
    expect(claimMeshRetentionSweep(root, DAY, 10 * DAY)).toBe(true);
    // This test process stands in for the still-running sweep: alive, with its own /proc start time.
    recordMeshRetentionSweeper(root, DAY, process.pid, 10 * DAY);
    expect(JSON.parse(fs.readFileSync(slotFile(root, 10), "utf8")).sweeper).toEqual({ pid: process.pid, startTime: expect.stringMatching(/^\d+$/) });
    expect(claimMeshRetentionSweep(root, DAY, 11 * DAY)).toBe(false);
    expect(claimMeshRetentionSweep(root, DAY, 12 * DAY)).toBe(false);
    expect(fs.existsSync(slotFile(root, 11))).toBe(false);
    expect(fs.existsSync(slotFile(root, 10))).toBe(true);
  });

  it.each([
    ["a dead pid", { pid: 2147483647, startTime: "1" }],
    ["a reused pid (live, another start time)", { pid: process.pid, startTime: "1" }],
    ["no recorded sweeper", undefined],
  ])("claims the next slot when the previous slot names %s", (_name, sweeper) => {
    const root = tmp("fabric-mesh-single-flight-");
    expect(claimMeshRetentionSweep(root, DAY, 10 * DAY)).toBe(true);
    if (sweeper) fs.writeFileSync(slotFile(root, 10), JSON.stringify({ pid: process.pid, claimedAt: 10 * DAY, sweeper }), { mode: 0o600 });
    expect(claimMeshRetentionSweep(root, DAY, 11 * DAY)).toBe(true);
  });

  it("claims the next slot when the previous claim is torn, and drops the former hourly slot files", () => {
    const root = tmp("fabric-mesh-single-flight-");
    fs.writeFileSync(slotFile(root, 10), '{"pid":1,"sweeper":{"pid":', { mode: 0o600 });
    fs.writeFileSync(slotFile(root, 492_000), JSON.stringify({ pid: 1, claimedAt: 1 }), { mode: 0o600 }); // an hourly-era slot
    expect(claimMeshRetentionSweep(root, DAY, 11 * DAY)).toBe(true);
    expect(fs.readdirSync(root).filter(name => name.startsWith(MESH_RETENTION_SWEEP_PREFIX)).sort())
      .toEqual([`${MESH_RETENTION_SWEEP_PREFIX}10.json`, `${MESH_RETENTION_SWEEP_PREFIX}11.json`]);
  });
});

describe("resident launcher log cap", () => {
  it("rotates a log past its cap to one .1 generation", () => {
    const root = tmp("fabric-launcher-log-");
    const file = path.join(root, "launcher.log");
    for (let index = 0; index < 50; index++) appendResidentLog(file, `${"x".repeat(99)}\n`, 1_000);
    expect(fs.statSync(file).size).toBeLessThanOrEqual(1_000);
    expect(fs.statSync(`${file}.1`).size).toBeLessThanOrEqual(1_000);
    expect(fs.readdirSync(root).sort()).toEqual(["launcher.log", "launcher.log.1"]);
  });
});

// smarty-dev#7916: --apply is back. It deletes exactly the dry run's plan (only expired, unreferenced data, through the
// same judgment and verification), and refuses on any doubt about state.json.
describe.skipIf(process.platform !== "linux")("apply through verified removal (smarty-dev#7916)", () => {
  const tree = (root: string): unknown[] => fs.readdirSync(root).sort().map(name => {
    const file = path.join(root, name); const stat = fs.lstatSync(file);
    return [name, stat.ino, stat.mtimeMs, stat.ctimeMs, stat.isDirectory() ? tree(file) : fs.readFileSync(file, "utf8")];
  });
  const withState = async () => {
    const mesh = deadRootMesh();
    const host = path.join(mesh.root, "residency", "host");
    write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:dead-host", residencyRoot: host, meshRoot: mesh.root }));
    const resident = run(path.join(host, "runs", "old-resident"), { status: "completed", finishedAt: NOW - 9 * DAY });
    fs.utimesSync(resident, (NOW - 9 * DAY) / 1000, (NOW - 9 * DAY) / 1000);
    // A state.json written by the real store.
    const store = new MeshStore(mesh.root, 64 * 1024, 100);
    await store.put({ key: "k", value: { ok: true }, identity: { id: "main", name: "main", kind: "main" } });
    return { mesh, resident };
  };

  it("deletes exactly the dry run's plan: expired, unreferenced runs and old backups; keeps latest, in-flight and young", async () => {
    const { mesh, resident } = await withState();
    const before = tree(mesh.root);
    const plan = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(tree(mesh.root)).toEqual(before);
    expect(plan.summary.applyRefusal).toBeUndefined();
    expect(plan.removedRuns.sort()).toEqual([mesh.old, mesh.orphan, resident].sort());
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.skipped).toEqual([]);
    expect(applied.removedRuns.sort()).toEqual(plan.removedRuns.sort());
    expect(applied.changes.map(change => change.path).sort()).toEqual(plan.changes.map(change => change.path).sort());
    for (const gone of plan.removedRuns) expect(fs.existsSync(gone)).toBe(false);
    for (const kept of [mesh.latest, mesh.young, mesh.inflight]) expect(fs.existsSync(path.join(kept, "reply.json"))).toBe(true);
    expect(fs.readdirSync(mesh.actor).filter(name => name.endsWith(".bak"))).toEqual(["session.jsonl.20260922T150000000Z.bak"]);
    // The summary's bytes are the same in both modes; no graveyard is left behind.
    const total = (result: typeof plan) => Object.values(result.summary.deleters).reduce((sum, item) => sum + item.bytes, 0);
    expect(total(applied)).toBe(total(plan));
    expect(total(plan)).toBeGreaterThan(0);
    expect(summaryLines(true, plan.summary).at(-1)).toMatch(/^WOULD_FREE_GIB=\d+\.\d{3}$/);
    const leftovers = (dir: string): string[] => fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry =>
      entry.name.startsWith(".retention-graveyard-") ? [entry.name] : entry.isDirectory() ? leftovers(path.join(dir, entry.name)) : []);
    expect(leftovers(mesh.root)).toEqual([]);
  });

  it.each([
    ["absent", undefined],
    ["an entry that is null", '{"format":1,"entries":{"k":null}}'],
    ["an entry under another key", '{"format":1,"entries":{"k":{"key":"x","version":1,"updatedAt":1,"updatedBy":{"id":"a","name":"a","kind":"main"}}}}'],
    ["an unknown envelope field", '{"format":1,"entries":{},"surprise":1}'],
    ["not JSON", "{"],
  ])("refuses --apply when state.json is %s, and changes nothing", async (_name, text) => {
    const { mesh } = await withState();
    if (text === undefined) fs.unlinkSync(path.join(mesh.root, "state.json"));
    else fs.writeFileSync(path.join(mesh.root, "state.json"), text, { mode: 0o600 });
    const before = tree(mesh.root);
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toEqual([]);
    expect(applied.changes).toEqual([]);
    expect(applied.skipped).toEqual([{ path: mesh.root, reason: expect.stringMatching(/mesh state is unknown/) }]);
    expect(tree(mesh.root)).toEqual(before);
  });
});
