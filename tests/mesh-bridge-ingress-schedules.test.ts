import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// KEEP: INGRESS-1 data safety. Real schedule release + swallowed filesystem failure
// must not make a held ingress helper replay after an append. SQLite and event bytes
// are genuine; hooks arrange only the failure/interleaving. Scratch belongs to test-env.
const BUDGET = 1_000;
const cleanups: Array<() => void | Promise<unknown>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const liveEvents = (root: string): MeshEvent[] => {
  const file = path.join(root, "events.jsonl");
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
};
const tickets = (root: string): string[] => {
  try { return fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort(); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
const outcome = <T>(work: Promise<T>) => work.then(value => ({ ok: true as const, value }),
  error => ({ ok: false as const, error: error as Error & { code?: string } }));
const fixture = async () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-ingress-schedules-"));
  const root = path.join(base, "hub");
  const hub = new MeshStore(root, 65_536, 100, { stateBackend: "sqlite", lockTimeoutMs: BUDGET });
  const far = new MeshStore(path.join(base, "far"), 65_536, 100, { stateBackend: "file" });
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const now = Date.now(), id = "session:schedules-far";
  const from: MeshIdentity = { id, name: "main", kind: "main", sessionId: id };
  await far.put({ key: `topology/hosts/${hash(id)}`, identity: from, value: {
    format: 1, id, rootId: id, identity: from, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(far.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(far.root, { key: `topology/participants/${hash(id)}`, version: 1, updatedAt: now, updatedBy: from,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: id, label: id,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  const side = new StoreBridgeSide(hub, "far"), peer = new StoreBridgeSide(far, "hub");
  await side.mirror(await peer.presence());
  expect(side.holds(id, from)).toBe(true);
  const event: BridgePublish = { topic: "fabric.control.ack", kind: "ack", from, to: "session:schedules-hub",
    text: "held target", data: { commandId: "schedules-target", bridge: { from: "far", id: "schedules-target" } } };
  const publish = (batch: boolean): Promise<{ sequence: number } | Array<{ sequence: number }>> =>
    batch ? side.publishBatch([{ event, held: [id] }]) : side.publish(event, [id]);
  const schedule = (key: string, dueAt: number) => hub.schedule({ topic: "schedule.delivery", from, text: key, key, dueAt }, now - 10_000);
  return { root, hub, from, publish, schedule, now, schedulesPath: path.join(root, "schedules.json") };
};
// A separate real connection owns state.db. Refusal is observed at the native BEGIN,
// then the writer releases so an unsafe helper replay is able to report false success.
const fenceBlocker = (root: string, releaseAfter = 1) => {
  const raw = new DatabaseSync(path.join(root, "state.db"));
  const exec = DatabaseSync.prototype.exec;
  raw.exec("PRAGMA busy_timeout = 0");
  let held = false, refusals = 0;
  const snapshots: Array<{ tickets: string[]; owner: string }> = [];
  const refused = deferred();
  const acquire = () => { exec.call(raw, "BEGIN IMMEDIATE"); held = true; };
  const release = () => { if (held) { exec.call(raw, "ROLLBACK"); held = false; } };
  cleanups.push(() => { release(); raw.close(); });
  vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, sql) {
    if (this !== raw && sql === "BEGIN IMMEDIATE") snapshots.push({ tickets: tickets(root),
      owner: fs.readFileSync(path.join(root, ".lock", "owner"), "utf8") });
    try { return exec.call(this, sql); }
    catch (error) {
      if (this !== raw && sql === "BEGIN IMMEDIATE" && held) {
        refusals++;
        if (refusals === releaseAfter) release();
        refused.resolve();
      }
      throw error;
    }
  });
  return { acquire, release, held: () => held, refused: refused.promise, snapshots, refusals: () => refusals };
};

describe("held ingress: store-level schedule effects consume helper replay", () => {
  for (const batch of [false, true]) {
    it.each(["rename-failure", "pruned", "append-then-unknown-error"] as const)(`${batch ? "batch" : "single"}: %s before a real target fence refusal`, async failure => {
      const f = await fixture();
      const future = await f.schedule("future", f.now + 60_000);
      const due = await f.schedule("due", f.now - 5_000);
      const original = fs.readFileSync(f.schedulesPath, "utf8");
      expect(liveEvents(f.root)).toEqual([]);
      const blocker = fenceBlocker(f.root);
      let appends = 0, renameFailures = 0, unknownFailures = 0;
      const rename = fs.renameSync.bind(fs);
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        if (failure === "rename-failure" && destination === f.schedulesPath && renameFailures === 0) {
          renameFailures++;
          expect(fs.readFileSync(f.schedulesPath, "utf8")).toBe(original);
          throw Object.assign(new Error("injected schedule replacement I/O failure"), { code: "EIO" });
        }
        return rename(source, destination);
      });
      const append = fs.appendFileSync.bind(fs);
      vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
        append(file, data, options); // Real complete append, even when its caller sees an unknown outcome.
        if (file === path.join(f.root, "events.jsonl") && String(data).includes(due.id)) {
          appends++;
          if (appends === 1) {
            blocker.acquire();
            if (failure === "append-then-unknown-error") {
              unknownFailures++;
              throw new Error("unknown outcome after actual scheduled append");
            }
          }
        }
      });
      const publish = vi.spyOn(f.hub, batch ? "publishBatch" : "publish");
      const result = await outcome(f.publish(batch));
      const events = liveEvents(f.root);
      const scheduled = events.filter(event => event.id === due.id);
      console.log(JSON.stringify({ gate: "INGRESS-1", batch, failure, ok: result.ok, storeCalls: publish.mock.calls.length,
        scheduleAppends: appends, scheduledSequences: scheduled.map(event => event.sequence),
        targetEvents: events.filter(event => event.text === "held target").length, refusals: blocker.refusals() }));
      expect(blocker.refusals(), "genuine target SQLite refusal followed the scheduled append").toBe(1);
      expect(renameFailures).toBe(failure === "rename-failure" ? 1 : 0);
      expect(unknownFailures).toBe(failure === "append-then-unknown-error" ? 1 : 0);
      expect(scheduled, "the helper must not re-release the same scheduled event ID").toHaveLength(1);
      expect(result.ok, "a store operation with an append effect cannot be helper-retried").toBe(false);
      if (!result.ok) expect(result.error.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
      expect(publish).toHaveBeenCalledTimes(1);
      expect(events).toHaveLength(1);
      expect(scheduled[0]).toMatchObject({ id: due.id, scheduled: { dueAt: due.dueAt, key: "due" } });
      expect(f.hub.read({ after: 0 })).toEqual(events);
      expect(f.hub.latestSequence()).toBe(scheduled[0]!.sequence);
      if (failure === "pruned") expect(f.hub.scheduled()).toEqual([future]);
      else expect(fs.readFileSync(f.schedulesPath, "utf8"), "failed replacement/unknown append left the due schedule on disk").toBe(original);
      expect(tickets(f.root)).toEqual([]);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      // A NEW caller invocation is still allowed. At-least-once crash/release replay
      // of an unpruned schedule is existing policy, not an automatic helper replay.
      await f.publish(batch);
      expect(liveEvents(f.root).filter(event => event.text === "held target")).toHaveLength(1);
      expect(f.hub.scheduled()).toEqual([future]);
    });
  }
});

describe("held ingress: effect-free schedule preflight retains FIFO retry", () => {
  for (const batch of [false, true]) {
    it.each(["absent", "future", "raced-no-due"] as const)(`${batch ? "batch" : "single"}: %s`, async preflight => {
      const f = await fixture();
      const future = preflight === "absent" ? undefined : await f.schedule("future", f.now + 60_000);
      if (preflight === "raced-no-due") {
        // The optimistic due check races a releaser; locked/no-due preflight has no
        // EventLog effect. Do not consume merely for entering releaseDueSchedules.
        vi.spyOn(f.hub, "nextScheduleDueAt").mockReturnValueOnce(f.now - 1);
      }
      const release = vi.spyOn(f.hub, "releaseDueSchedules");
      const blocker = fenceBlocker(f.root);
      blocker.acquire();
      const order: string[] = [], laterLock = new MeshLock(f.root, { lockTimeoutMs: BUDGET }, () => undefined);
      let queued: string[] = [], offLock = false;
      // Register OUTSIDE the ingress async-local scope: a genuine later ordinary
      // contender, not another acquisition borrowing the designated admission.
      const later = blocker.refused.then(async () => {
        offLock = !fs.existsSync(path.join(f.root, ".lock"));
        const work = laterLock.withLock(() => { order.push("later"); });
        queued = tickets(f.root);
        await work;
      });
      cleanups.push(() => later);
      const append = fs.appendFileSync.bind(fs);
      vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
        append(file, data, options);
        if (file === path.join(f.root, "events.jsonl")) order.push("target");
      });
      const publish = vi.spyOn(f.hub, batch ? "publishBatch" : "publish");
      const result = await outcome(f.publish(batch));
      await later;
      expect(result.ok).toBe(true);
      expect(blocker.refusals()).toBe(1);
      expect(publish).toHaveBeenCalledTimes(2);
      expect(release).toHaveBeenCalledTimes(preflight === "raced-no-due" ? 1 : 0);
      expect(blocker.snapshots).toHaveLength(2);
      const [first, second] = blocker.snapshots;
      expect(first!.tickets).toHaveLength(1);
      expect(queued, "later ordinary writer queues behind retained ingress").toHaveLength(2);
      expect(queued[0]).toBe(first!.tickets[0]);
      expect(second!.tickets).toEqual(queued);
      expect(second!.owner, "canonical lock was released and reacquired across refusal").not.toBe(first!.owner);
      expect(offLock, "retry retains admission, never canonical custody").toBe(true);
      expect(order).toEqual(["target", "later"]);
      expect(liveEvents(f.root)).toHaveLength(1);
      expect(liveEvents(f.root)[0]).toMatchObject({ text: "held target", verification: "bridge" });
      expect(f.hub.scheduled()).toEqual(future ? [future] : []);
      expect(tickets(f.root)).toEqual([]);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
    });
  }
});

// KEEP: known bounded self-queue gap, not a data-safety repair.
// This regression does not implement the separately tracked repair.
// Fix the acceptance bound BEFORE measurement: B=1,000 ms + 500 ms slack.
// A future schedule becomes due after a real retained refusal, self-queues,
// then appends once after bounded head reaping and irrevocably consumes replay.
it("bounds the known future-schedule self-wait and never helper-replays after its append", async () => {
  const selfWaitBoundMs = BUDGET + 500;
  const f = await fixture(), dueAt = Date.now() + 250;
  const scheduled = await f.schedule("becomes-due", dueAt);
  const blocker = fenceBlocker(f.root, 6);
  const originalPublish = f.hub.publish.bind(f.hub);
  let retained = "", firstFenceAt = 0;
  const fence = f.hub.withStateWriteFence.bind(f.hub);
  vi.spyOn(f.hub, "withStateWriteFence").mockImplementation(<T>(operation: () => T): T => {
    firstFenceAt ||= Date.now();
    return fence(operation);
  });
  // Normal retry drives the second call. Its one-shot gate waits for the real
  // due time; SQLite, canonical acquisitions, receipt reaping and append are real.
  const publish = vi.spyOn(f.hub, "publish").mockImplementation(async (...args) => {
    if (publish.mock.calls.length === 2) {
      retained = blocker.snapshots[0]!.tickets[0]!;
      expect(tickets(f.root)).toEqual([retained]);
      expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
      await new Promise<void>(resolve => setTimeout(resolve, Math.max(0, dueAt - Date.now()) + 1));
    }
    return originalPublish(...args);
  });
  const release = f.hub.releaseDueSchedules.bind(f.hub);
  let releasing = false, beforeRelease: string[] = [], selfWaitMs = -1;
  const releaser = vi.spyOn(f.hub, "releaseDueSchedules").mockImplementation(async (...args) => {
    beforeRelease = tickets(f.root);
    releasing = true;
    const started = performance.now();
    try { return await release(...args); }
    finally { selfWaitMs = performance.now() - started; releasing = false; }
  });
  const queue = meshLockQueueDirectory(f.root);
  const enqueues: Array<{ before: string[]; after: string[]; canonicalHeld: boolean }> = [];
  const write = fs.writeFileSync.bind(fs);
  vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
    if (releasing && typeof file === "string" && path.dirname(file) === queue && /^\d{24}-\d+-[a-f0-9-]+$/.test(path.basename(file))) {
      const before = tickets(f.root), canonicalHeld = fs.existsSync(path.join(f.root, ".lock"));
      write(file, data, options);
      enqueues.push({ before, after: tickets(f.root), canonicalHeld });
    } else write(file, data, options);
  });
  const append = fs.appendFileSync.bind(fs), scheduleQueues: string[][] = [];
  vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
    append(file, data, options);
    if (file === path.join(f.root, "events.jsonl") && String(data).includes(scheduled.id)) scheduleQueues.push(tickets(f.root));
  });
  blocker.acquire();
  const result = await outcome(f.publish(false));
  console.log(JSON.stringify({ gate: "future-schedule-self-queue", budgetMs: BUDGET, selfWaitBoundMs, selfWaitMs,
    refusals: blocker.refusals(), releaseAfter: 6, ok: result.ok, beforeRelease, enqueues, scheduleQueues,
    targetEvents: liveEvents(f.root).filter(event => event.text === "held target").length }));
  expect(firstFenceAt, "schedule was future at the first genuine target fence").toBeLessThan(dueAt);
  expect(releaser).toHaveBeenCalledTimes(1);
  expect(beforeRelease).toEqual([retained]);
  expect(enqueues, "actual ordinary schedule follower, not borrowed initial admission").toHaveLength(1);
  expect(enqueues[0]!.before).toEqual([retained]);
  expect(enqueues[0]!.after, "known gap: two physical receipts self-queue before reaping").toHaveLength(2);
  expect(enqueues[0]!.after[0]).toBe(retained);
  expect(enqueues[0]!.canonicalHeld, "retained head is off canonical lock").toBe(false);
  expect(scheduleQueues, "direct follower reaped old head before its actual scheduled append").toEqual([[enqueues[0]!.after[1]]]);
  expect(selfWaitMs).toBeGreaterThanOrEqual(0);
  expect(selfWaitMs, "fixed B+500 bound; never tune this to the observed run").toBeLessThanOrEqual(selfWaitBoundMs);
  expect(blocker.refusals()).toBe(2);
  expect(blocker.held(), "effect consumed helper before the configured sixth refusal could release SQLite").toBe(true);
  expect(blocker.snapshots).toHaveLength(2);
  expect(blocker.snapshots[1]!.tickets).toHaveLength(1);
  expect(blocker.snapshots[1]!.tickets).not.toContain(retained);
  expect(publish, "no automatic third attempt after the schedule effect").toHaveBeenCalledTimes(2);
  expect(result.ok).toBe(false);
  if (!result.ok) expect(result.error.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
  const events = liveEvents(f.root);
  expect(events).toHaveLength(1);
  expect(events[0]).toMatchObject({ id: scheduled.id, text: "becomes-due", scheduled: { dueAt, key: "becomes-due" } });
  expect(events.filter(event => event.text === "held target")).toEqual([]);
  expect(f.hub.read({ after: 0 })).toEqual(events);
  expect(f.hub.latestSequence()).toBe(events[0]!.sequence);
  expect(f.hub.scheduled()).toEqual([]);
  expect(tickets(f.root)).toEqual([]);
  expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
});

it("retains one physical ticket through more than three real fence refusals, then commits before an ordinary follower", async () => {
  const f = await fixture(), blocker = fenceBlocker(f.root, 6);
  blocker.acquire();
  const busyErrors: unknown[] = [];
  const physicalReceipts: Array<{ name: string; dev: number; ino: number }> = [];
  const fence = f.hub.withStateWriteFence.bind(f.hub);
  vi.spyOn(f.hub, "withStateWriteFence").mockImplementation(<T>(operation: () => T): T => {
    const name = tickets(f.root)[0]!;
    const { dev, ino } = fs.statSync(path.join(meshLockQueueDirectory(f.root), name));
    physicalReceipts.push({ name, dev, ino });
    try { return fence(operation); }
    catch (error) { busyErrors.push(error); throw error; }
  });
  let queued: string[] = [], offLock = false;
  // Registered before ingress starts: the ordinary publisher gets its own
  // physical receipt and cannot borrow the held-publication designation.
  const later = outcome(blocker.refused.then(async () => {
    offLock = !fs.existsSync(path.join(f.root, ".lock"));
    const work = f.hub.publish({ topic: "ordinary.queue", from: f.from, text: "ordinary after refusal" });
    queued = tickets(f.root);
    return work;
  }));
  cleanups.push(() => later);
  const publish = vi.spyOn(f.hub, "publish");
  const result = await outcome(f.publish(false));
  const ordinary = await later;
  console.log(JSON.stringify({ gate: "retained-fence-streak", refusals: blocker.refusals(), ok: result.ok,
    ordinaryOk: ordinary.ok, targetEvents: liveEvents(f.root).filter(event => event.text === "held target").length }));
  expect(blocker.refusals(), "six native BEGIN refusals, with no refusal-count retirement policy").toBe(6);
  expect(busyErrors).toHaveLength(6);
  expect(result.ok).toBe(true);
  expect(ordinary.ok).toBe(true);
  expect(publish).toHaveBeenCalledTimes(8); // Seven target attempts plus one ordinary publication.
  expect(blocker.snapshots).toHaveLength(7);
  expect(physicalReceipts).toHaveLength(7);
  for (const receipt of physicalReceipts) expect(receipt).toEqual(physicalReceipts[0]);
  const [first, ...retries] = blocker.snapshots;
  expect(first!.tickets).toHaveLength(1);
  expect(queued).toHaveLength(2);
  expect(queued[0]).toBe(first!.tickets[0]);
  for (const snapshot of retries) expect(snapshot.tickets).toEqual(queued);
  expect(new Set(blocker.snapshots.map(snapshot => snapshot.owner)).size).toBe(7);
  expect(offLock, "retention never sleeps with canonical custody").toBe(true);
  expect(blocker.held()).toBe(false);
  expect(liveEvents(f.root).map(event => event.text)).toEqual(["held target", "ordinary after refusal"]);
  if (result.ok && ordinary.ok && !Array.isArray(result.value)) expect(result.value.sequence).toBeLessThan(ordinary.value.sequence);
  expect(tickets(f.root)).toEqual([]);
  expect(fs.existsSync(path.join(f.root, ".lock"))).toBe(false);
});
