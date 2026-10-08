import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";
import { claimMeshRetentionSweep, MESH_RETENTION_SWEEP_PREFIX } from "../src/storage/retention.js";
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
  for (const stamp of ["20260920T150000000Z", "20260921T150000000Z", "20260922T150000000Z"]) {
    write(path.join(actor, `session.jsonl.${stamp}.bak`), stamp);
  }
  write(path.join(actor, "session.jsonl"), "live");
  return { root, registry, actor, runs, old, latest, young, inflight, orphan };
};

describe("mesh-wide age-based run retention", () => {
  it("removes a dead root's old terminal runs, keeps its latest, in-flight and young runs, and caps .bak files", async () => {
    const mesh = deadRootMesh();
    const plan = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
    expect(plan.removedRuns.sort()).toEqual([mesh.old, mesh.orphan].sort());
    expect(fs.existsSync(mesh.old)).toBe(true);
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.skipped).toEqual([]);
    expect(applied.removedRuns.sort()).toEqual([mesh.old, mesh.orphan].sort());
    expect(fs.existsSync(mesh.old)).toBe(false);
    expect(fs.existsSync(mesh.orphan)).toBe(false);
    for (const kept of [mesh.latest, mesh.young, mesh.inflight]) expect(fs.existsSync(path.join(kept, "reply.json"))).toBe(true);
    expect(fs.readdirSync(mesh.actor).filter(name => name.endsWith(".bak"))).toEqual(["session.jsonl.20260922T150000000Z.bak"]);
    expect(fs.readFileSync(path.join(mesh.actor, "session.jsonl"), "utf8")).toBe("live");
  });

  it("without runRetentionMs removes nothing (compaction-only contract unchanged)", async () => {
    const mesh = deadRootMesh();
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false });
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
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
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
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
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
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toEqual([mesh.orphan]);
    for (const kept of [mesh.old, young]) expect(fs.existsSync(kept)).toBe(true);
  });

  it("fails closed when a registry becomes unreadable before the final pre-delete check", async () => {
    const mesh = deadRootMesh();
    const other = path.join(mesh.root, "actors", "01a118ab-live-session");
    write(path.join(other, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle" }] }));
    const injected = afterFirstSnapshot(mesh.old, () => fs.rmSync(path.join(other, "actors.json")));
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(injected()).toBe(true);
    expect(applied.removedRuns).not.toContain(mesh.old);
    expect(fs.existsSync(mesh.old)).toBe(true);
  });

  it("an unreadable registry is a wildcard veto: nothing is removed", async () => {
    const mesh = deadRootMesh();
    write(path.join(mesh.root, "actors", "broken", "actors.json"), "{not json");
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
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
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toContain(differs);
    expect(applied.removedRuns).not.toContain(own);
    expect(applied.removedRuns).not.toContain(unknown);
    expect(fs.existsSync(differs)).toBe(false);
    expect(fs.existsSync(own)).toBe(true);
    expect(fs.existsSync(unknown)).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("removes a proven-dead resident root's old runs under its flock, not a young one", async () => {
    const mesh = deadRootMesh();
    const host = path.join(mesh.root, "residency", "host");
    write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:dead-host", residencyRoot: host, meshRoot: mesh.root }));
    const old = run(path.join(host, "runs", "old-resident"), { status: "completed", finishedAt: NOW - 2 * DAY });
    const young = run(path.join(host, "runs", "young-resident"), { status: "completed", finishedAt: NOW - 1_000 });
    fs.utimesSync(old, (NOW - 2 * DAY) / 1000, (NOW - 2 * DAY) / 1000);
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toContain(old);
    expect(fs.existsSync(old)).toBe(false);
    expect(fs.existsSync(young)).toBe(true);
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

  it("removes the dead resident's old run when nothing references it (control)", async () => {
    const { mesh, resident } = deadResident();
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns).toContain(resident);
    expect(fs.existsSync(resident)).toBe(false);
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
    const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
    expect(applied.removedRuns.sort()).toEqual(plan.removedRuns.sort());
    expect(fs.existsSync(resident)).toBe(false);
    expect(fs.existsSync(young)).toBe(true);
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
      const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
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
      const applied = await sweepMeshRetention(mesh.root, { now: NOW, dryRun: false, runRetentionMs: 7 * DAY });
      expect(injected()).toBe(true);
      expect(applied.removedRuns.filter(item => item.startsWith(path.join(mesh.root, "residency")))).toEqual([]);
      expect(fs.existsSync(path.join(resident, "reply.json"))).toBe(true);
    });
  }
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
