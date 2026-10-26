// Keep: data-safety/trust-boundary regressions for PR #1635. Real presence CAS,
// watches, receipts and routing run over the broker-semantic KV fixture; broker
// uncertainty and failed reads are not cheaply reachable in a user-flow test.
// Watch put/READY is the normal recovery driver. A 1–5 s jittered timer is only
// the safety net when a READY watch has no more events after a failed read.
import { performance } from "node:perf_hooks";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUnavailableError, TransportUncertainError } from "../src/transport/types.js";
import { hKey, kvWorld, natsLease, pKey } from "./helpers/bridge-nats-kv.js";

const MACHINE = "test-machine";
const ROOT = "session:withdrawal-root";
const ID = "agent:withdrawn";
const open: KvParticipantDirectory[] = [];

beforeEach(() => {
  vi.useFakeTimers({ now: 1_000_000 });
  vi.spyOn(performance, "now").mockImplementation(() => Date.now());
});
afterEach(async () => {
  await Promise.all(open.splice(0).map(view => view.close()));
  vi.restoreAllMocks();
  vi.useRealTimers();
});

function record(id = ID): FabricParticipantRecord {
  return { format: 1, id, kind: "agent", rootId: ROOT, ownerHostId: ROOT, ownerIdentityId: ROOT,
    ownerIncarnation: "inc-1", name: id, status: "running", runner: "pi", transport: "process",
    capabilities: ["steer"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
}

async function fixture(random: () => number = () => 1) {
  const world = kvWorld(), abort = new AbortController(), errors: unknown[] = [];
  const transport = world.transport(MACHINE, ROOT);
  const presence = transport.kv("presence"), kv = transport.kv.bind(transport);
  transport.kv = bucket => bucket === "presence" ? presence : kv(bucket);
  const read = presence.get.bind(presence);
  const reads: { key: string; at: number; failed: boolean }[] = [];
  let unavailable = false;
  let readGate: Promise<void> | undefined;
  vi.spyOn(presence, "get").mockImplementation(async <V>(key: string) => {
    reads.push({ key, at: Date.now(), failed: unavailable });
    await readGate;
    if (unavailable) throw new TransportUnavailableError("presence read unavailable after uncertain CAS");
    return read<V>(key);
  });
  const events: string[] = [];
  const watch = presence.watch.bind(presence);
  presence.watch = async function* <V>(prefix: string, signal: AbortSignal) {
    for await (const event of watch<V>(prefix, signal)) {
      events.push(event.type);
      yield event;
    }
  };
  const view = new KvParticipantDirectory({ transport,
    lease: { live: () => !abort.signal.aborted, signal: abort.signal }, identityId: ROOT,
    ownerIncarnation: "inc-1", rootId: ROOT, random, onError: error => { errors.push(error); } });
  open.push(view);
  world.leases.put(hKey(MACHINE, ROOT), `${ROOT}#inc-1`, natsLease(ROOT, "inc-1", Date.now()), 0);
  view.start();
  while (!view.ready) await view.whenChanged("ready", new AbortController().signal);
  const initial = record(), first = await view.publishBeforeLaunch(initial);
  await vi.advanceTimersByTimeAsync(0);
  expect(view.resolve(ID)?.live).toBe(true); // A genuinely routable participant, not an already-stale row.
  const key = pKey(MACHINE, ID);
  return { world, view, abort, errors, reads, events, initial, first, key,
    row: (id = ID) => world.presence.values.get(pKey(MACHINE, id)),
    readable: (value: boolean) => { unavailable = !value; },
    holdReads: () => {
      let release!: () => void;
      readGate = new Promise<void>(resolve => { release = resolve; });
      return () => { readGate = undefined; release(); };
    } };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;

async function interrupt(f: Fixture, committed: boolean, attempted = { ...f.initial, status: "idle" }) {
  const before = f.reads.length;
  // Use the fixture's actual CAS uncertainty: in the stored case it really writes
  // and emits put, and in the lost case the confirmed predecessor really remains.
  f.world.presence.failNextPut = key => {
    if (key !== pKey(MACHINE, attempted.id)) return undefined;
    f.readable(false);
    return committed ? "uncertain-stored" : "uncertain-lost";
  };
  await expect(f.view.publish(attempted)).rejects.toBeInstanceOf(TransportUncertainError);
  await vi.advanceTimersByTimeAsync(0);
  expect(f.reads.slice(before).filter(read => read.failed).length).toBeGreaterThanOrEqual(2);
  expect(f.view.ready).toBe(true); // No watch reset/disconnect is used to manufacture pending cleanup.
  return attempted;
}

async function recover(f: Fixture, driver: "manual" | "watch") {
  f.readable(true);
  if (driver === "manual") await f.view.reconcilePendingCleanup();
  else { f.world.presence.gap(); f.world.presence.resnapshot(); }
  await vi.advanceTimersByTimeAsync(0);
  expect(f.view.ready).toBe(true);
}

async function absentAndReadmittable(f: Fixture) {
  expect.soft(f.row(), "withdrawn predecessor/attempt must be CAS-deleted").toBeUndefined();
  expect.soft(f.view.resolve(ID)?.live, "withdrawn X must not remain routable").not.toBe(true);
  await vi.advanceTimersByTimeAsync(216_001); // Beyond even the latest successful-refresh deadline.
  expect.soft(f.row(), "withdrawal must not be restored or refreshed later").toBeUndefined();
  expect.soft(f.view.resolve(ID)?.live, "withdrawn X must still not be live after refresh deadline").not.toBe(true);
  await expect.soft(f.view.publishBeforeLaunch({ ...f.initial, startedAt: Date.now(), updatedAt: Date.now() }),
    "cleanup must release admission of this same id").resolves.toHaveProperty("revision");
}

describe("P1: withdrawal during uncertain presence update", () => {
  it.each([
    [false, "manual"], [false, "watch"], [true, "manual"], [true, "watch"],
  ] as const)("remove withdraws X before recovery (CAS stored=%s, recovery=%s)", async (committed, driver) => {
    const f = await fixture();
    await interrupt(f, committed);
    const uncertainRow = f.row();
    expect(uncertainRow?.revision).toBe(committed ? f.first.revision + 1 : f.first.revision);
    // participant-directory terminal/snapshot disappearance/withdrawBeforeLaunch
    // all call this public path. false is harmless; losing withdrawal intent is not.
    await f.view.remove(ID);
    await recover(f, driver);
    await absentAndReadmittable(f);
  });

  it.each([false, true])("receipt rollback withdraws its pending generation (CAS stored=%s)", async committed => {
    const f = await fixture();
    await interrupt(f, committed);
    await f.first.rollback();
    await recover(f, "manual");
    await absentAndReadmittable(f);
  });

  it("receipt rollback must not restore pending X when its first delete fails", async () => {
    const f = await fixture();
    await interrupt(f, false);
    const failure = new TransportUnavailableError("withdrawal delete unavailable");
    vi.spyOn(f.world.presence, "delete").mockImplementationOnce(() => { throw failure; });
    await expect(f.first.rollback()).rejects.toBe(failure);
    await recover(f, "manual");
    await absentAndReadmittable(f);
  });

  it("CONTROL: nonwithdrawn exact confirmed predecessor is restored and still refreshes", async () => {
    const f = await fixture();
    const attempted = await interrupt(f, false);
    await recover(f, "manual");
    expect(f.row()).toMatchObject({ revision: f.first.revision, value: f.initial });
    expect(f.view.resolve(ID)?.live).toBe(true);
    await vi.advanceTimersByTimeAsync(216_000);
    expect(f.row()!.revision).toBeGreaterThan(f.first.revision);
    const refreshed = f.row()!.revision;
    const put = vi.spyOn(f.world.presence, "put");
    await expect(f.view.publish(attempted)).resolves.toHaveProperty("revision");
    expect(put.mock.calls.at(-1)?.[3]).toBe(refreshed);
  });

  it("CONTROL: an old rollback cannot withdraw a newer pending publication generation", async () => {
    const f = await fixture();
    const next = { ...f.initial, startedAt: Date.now() + 1, sessionId: "generation-2" };
    const newer = await f.view.publish(next);
    await interrupt(f, false, { ...next, status: "idle" });
    expect(await f.first.rollback()).toBe(false);
    await recover(f, "manual");
    expect(f.row()).toMatchObject({ revision: newer.revision, value: next });
    await vi.advanceTimersByTimeAsync(216_000);
    expect(f.row()!.revision).toBeGreaterThan(newer.revision);
    expect(f.view.resolve(ID)?.live).toBe(true);
  });

  it.each([
    [false, "unknown revision"], [true, "unknown revision"],
    [false, "new incarnation"], [true, "new incarnation"],
    [false, "foreign owner"], [true, "foreign owner"],
  ] as const)("CONTROL: withdrawal cannot acquire successor (stored=%s, successor=%s)", async (committed, kind) => {
    const f = await fixture();
    await interrupt(f, committed);
    await f.view.remove(ID);
    const winner = kind === "unknown revision" ? f.initial : kind === "new incarnation"
      ? { ...f.initial, ownerIncarnation: "inc-2" }
      : { ...f.initial, ownerHostId: "session:foreign", ownerIdentityId: "session:foreign", ownerIncarnation: "foreign-inc" };
    const revision = f.world.presence.put(f.key, `${winner.ownerHostId}#${winner.ownerIncarnation}`, winner, f.row()!.revision);
    const remove = vi.spyOn(f.world.presence, "delete");
    await recover(f, "manual");
    await vi.advanceTimersByTimeAsync(216_001);
    expect(remove).not.toHaveBeenCalled();
    expect(f.row()).toMatchObject({ revision, value: winner });
    expect(await f.first.rollback()).toBe(false);
    await expect(f.view.publishBeforeLaunch(f.initial)).rejects.toThrow();
    expect(f.row()).toMatchObject({ revision, value: winner });
  });

  it("CONTROL: a foreign winner of the withdrawal delete CAS survives", async () => {
    const f = await fixture();
    await interrupt(f, true);
    await f.view.remove(ID);
    const winner = { ...f.initial, ownerIncarnation: "inc-2" };
    const remove = f.world.presence.delete.bind(f.world.presence);
    let revision = 0;
    vi.spyOn(f.world.presence, "delete").mockImplementationOnce((key, expected) => {
      revision = f.world.presence.put(key, `${ROOT}#inc-2`, winner, expected);
      return remove(key, expected);
    });
    await recover(f, "manual");
    expect(revision).toBeGreaterThan(0);
    expect(f.row()).toMatchObject({ revision, value: winner });
    await vi.advanceTimersByTimeAsync(216_001);
    expect(f.row()).toMatchObject({ revision, value: winner });
  });

  it("CONTROL: removing an unowned absent id remains a harmless false", async () => {
    const f = await fixture();
    const before = [...f.world.writes];
    expect(await f.view.remove("agent:never-owned")).toBe(false);
    expect(f.world.writes).toEqual(before);
    expect(f.view.resolve(ID)?.live).toBe(true);
  });
});

describe("P2: READY watch, failed cleanup read and autonomous bounded recovery", () => {
  it.each([
    [false, 0, 1_000], [true, 0, 1_000], [false, 1, 5_000], [true, 1, 5_000],
  ] as const)("recovers without a watch wake/publish nudge (stored=%s, sample=%s, delay=%s)", async (committed, sample, delay) => {
    const f = await fixture(() => sample);
    const attempted = await interrupt(f, committed);
    const readCount = f.reads.length, eventCount = f.events.length;
    f.readable(true); // No reconcile call, reset, READY, put or new publication here.
    await vi.advanceTimersByTimeAsync(delay - 1);
    expect(f.reads).toHaveLength(readCount);
    expect(f.events).toHaveLength(eventCount);
    await vi.advanceTimersByTimeAsync(1);
    expect.soft(f.reads.length, "a failed read must have an autonomous retry at the sampled endpoint").toBeGreaterThan(readCount);
    expect(f.events.slice(eventCount).filter(type => type !== "gone")).toEqual([]);
    expect(f.view.ready).toBe(true);
    if (committed) expect.soft(f.row(), "stored uncertain attempt must be cleaned without a watch wake").toBeUndefined();
    else expect(f.row()).toMatchObject({ revision: f.first.revision, value: f.initial });
    await expect.soft(f.view.publish(attempted), "autonomous reconciliation must unblock publication").resolves.toHaveProperty("revision");
  });

  it("fresh jitter draws bound repeated failed reads, with one coalesced safety net for multiple intents", async () => {
    let sample = 0;
    const f = await fixture(() => sample);
    const other = record("agent:second-pending");
    await f.view.publishBeforeLaunch(other);
    const existingTimers = vi.getTimerCount(); // Existing long presence-refresh timer is allowed.
    await interrupt(f, false);
    await interrupt(f, false, { ...other, status: "idle" });
    // Duplicate event requests may cause an immediate pass, but must not multiply
    // the persistent fallback. Observe only after all those events are consumed.
    for (let n = 0; n < 8; n++) f.world.presence.emit({ type: "ready" });
    await vi.advanceTimersByTimeAsync(0);
    const before = f.reads.length;
    const start = Date.now();
    expect(vi.getTimerCount(), "at most one short retry timer beyond existing refresh scheduling").toBeLessThanOrEqual(existingTimers + 1);
    sample = 1; // The retry due at +1 s must draw a fresh +5 s delay on failure.
    await vi.advanceTimersByTimeAsync(999);
    expect(f.reads).toHaveLength(before);
    await vi.advanceTimersByTimeAsync(1);
    expect.soft(f.reads.slice(before).map(read => [read.key, read.at - start]).sort()).toEqual(
      [[f.key, 1_000], [pKey(MACHINE, other.id), 1_000]].sort());
    const firstRetry = f.reads.length;
    sample = 0.25; // The next failed pass must independently draw +2 s.
    await vi.advanceTimersByTimeAsync(4_999);
    expect(f.reads).toHaveLength(firstRetry);
    await vi.advanceTimersByTimeAsync(1);
    expect.soft(f.reads.slice(firstRetry).map(read => read.at - start)).toEqual([6_000, 6_000]);
    const secondRetry = f.reads.length;
    await vi.advanceTimersByTimeAsync(1_999);
    expect(f.reads).toHaveLength(secondRetry);
    await vi.advanceTimersByTimeAsync(1);
    expect.soft(f.reads.slice(secondRetry).map(read => read.at - start)).toEqual([8_000, 8_000]);
    expect(f.row()).toMatchObject({ revision: f.first.revision, value: f.initial });
    expect(vi.getTimerCount()).toBeLessThanOrEqual(existingTimers + 1);
    f.readable(true);
    await vi.advanceTimersByTimeAsync(2_000);
    await expect.soft(f.view.publish({ ...f.initial, status: "idle" })).resolves.toHaveProperty("revision");
    await expect.soft(f.view.publish({ ...other, status: "idle" })).resolves.toHaveProperty("revision");
  });

  it("does not overlap retry reads while an earlier safety-net pass is unresolved", async () => {
    const f = await fixture(() => 0);
    await interrupt(f, false);
    const before = f.reads.length, writes = [...f.world.writes];
    const release = f.holdReads();
    try {
      await vi.advanceTimersByTimeAsync(1_000);
      expect.soft(f.reads.length, "first safety-net read must begin").toBe(before + 1);
      await vi.advanceTimersByTimeAsync(3_000); // Still inside the 5 s operation budget.
      expect.soft(f.reads.length, "no overlapping/hot-loop reads while the pass is pending").toBe(before + 1);
      expect(f.world.writes).toEqual(writes);
      f.readable(true);
      release();
      await vi.advanceTimersByTimeAsync(0);
      await expect.soft(f.view.publish({ ...f.initial, status: "idle" })).resolves.toHaveProperty("revision");
    } finally { release(); }
  });

  it.each(["READY", "put"] as const)("CONTROL: %s drives immediate recovery before the safety-net deadline", async driver => {
    const f = await fixture(() => 1);
    const attempted = await interrupt(f, false);
    f.readable(true);
    if (driver === "READY") f.world.presence.emit({ type: "ready" });
    else {
      // A delayed uncertain CAS really arrives at the broker with its original
      // predecessor revision, then emits put. No read revision is adopted to write.
      f.world.presence.put(f.key, `${ROOT}#inc-1`, attempted, f.first.revision);
    }
    await vi.advanceTimersByTimeAsync(0);
    if (driver === "READY") expect(f.row()).toMatchObject({ revision: f.first.revision });
    else expect(f.row()).toBeUndefined();
    await expect(f.view.publish(attempted)).resolves.toHaveProperty("revision");
  });

  it.each([
    ["close", false], ["close", true], ["lease abort", false], ["lease abort", true],
  ] as const)("CONTROL: %s cancels unresolved cleanup with no late timer reads/writes (stored=%s)", async (stop, committed) => {
    const f = await fixture(() => 0);
    await interrupt(f, committed);
    // Keep reads failed through shutdown, so close cannot accidentally make this
    // a successful-cleanup test instead of a pending-timer cancellation test.
    if (stop === "close") await f.view.close();
    else f.abort.abort(new Error("owner lease fenced"));
    await vi.advanceTimersByTimeAsync(0);
    const reads = f.reads.length, writes = [...f.world.writes];
    const remove = vi.spyOn(f.world.presence, "delete");
    f.readable(true);
    await vi.advanceTimersByTimeAsync(600_000);
    expect(f.reads).toHaveLength(reads);
    expect(f.world.writes).toEqual(writes);
    expect(remove).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    expect(f.view.ready).toBe(false);
  });
});

// R4-P1-01: the original uncertain CAS is still in flight after withdrawal.
// Keep the original 29 cases above unchanged; exercise the GET/delete boundary
// on the same stable KV bucket captured by the real participant view.
describe("R4-P1-01: delayed original CAS wins the withdrawal delete", () => {
  it.each(["manual", "watch"] as const)("retains withdrawal after predecessor GET/delete CAS loss (%s)", async driver => {
    const f = await fixture();
    const put = vi.spyOn(f.world.presence, "put");
    const attempted = await interrupt(f, false);
    const original = put.mock.calls.at(-1)!;
    expect(original).toEqual([f.key, `${ROOT}#inc-1`, attempted, f.first.revision]);
    expect(await f.view.remove(ID)).toBe(false); // The public terminal/disappearance path.
    expect(f.row()).toMatchObject({ revision: f.first.revision, value: f.initial });
    const remove = f.world.presence.delete.bind(f.world.presence);
    const deletes = vi.spyOn(f.world.presence, "delete");
    let delayedRevision = 0;
    deletes.mockImplementationOnce((key, expected) => {
      expect([key, expected]).toEqual([f.key, f.first.revision]); // GET observed R.
      delayedRevision = f.world.presence.put(...original); // Exact original CAS R -> R+1.
      return remove(key, expected); // Cleanup delete(R) must lose, not delete R+1.
    });
    await recover(f, driver);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(delayedRevision).toBeGreaterThan(f.first.revision);
    expect(deletes.mock.results[0]?.value).toBe(false);
    await absentAndReadmittable(f); // Actual absence, nonlive, no refresh, same-id admission.
  });

  it.each(["unknown revision", "new generation", "new incarnation", "foreign owner"] as const)(
    "CONTROL: a %s winner between GET and delete is never acquired", async kind => {
      const f = await fixture();
      await interrupt(f, false);
      expect(await f.view.remove(ID)).toBe(false);
      const winner = kind === "unknown revision" ? f.initial : kind === "new generation"
        ? { ...f.initial, startedAt: Date.now() + 1, sessionId: "generation-2" }
        : kind === "new incarnation" ? { ...f.initial, ownerIncarnation: "inc-2" }
        : { ...f.initial, ownerHostId: "session:foreign", ownerIdentityId: "session:foreign", ownerIncarnation: "foreign-inc" };
      const remove = f.world.presence.delete.bind(f.world.presence);
      const deletes = vi.spyOn(f.world.presence, "delete");
      let revision = 0;
      deletes.mockImplementationOnce((key, expected) => {
        expect(expected).toBe(f.first.revision);
        revision = f.world.presence.put(key, `${winner.ownerHostId}#${winner.ownerIncarnation}`, winner, expected);
        return remove(key, expected);
      });
      await recover(f, "manual");
      await vi.advanceTimersByTimeAsync(216_001);
      expect(deletes.mock.results[0]?.value).toBe(false);
      expect(deletes.mock.calls.every(([, expected]) => expected === f.first.revision)).toBe(true);
      expect(f.row()).toMatchObject({ revision, value: winner });
      expect(await f.first.rollback()).toBe(false);
      await expect(f.view.publishBeforeLaunch(f.initial)).rejects.toThrow();
      expect(f.row()).toMatchObject({ revision, value: winner });
    });

  it("retains intent through CAS loss and repeated failed reads without overlapping or hot-looping", async () => {
    const f = await fixture(() => 0);
    const put = vi.spyOn(f.world.presence, "put");
    await interrupt(f, false);
    const original = put.mock.calls.at(-1)!;
    await f.view.remove(ID);
    const remove = f.world.presence.delete.bind(f.world.presence);
    vi.spyOn(f.world.presence, "delete").mockImplementationOnce((key, expected) => {
      f.world.presence.put(...original);
      f.readable(false); // The put wake cannot complete its recovery read.
      return remove(key, expected);
    });
    await recover(f, "manual");
    const before = f.reads.length;
    await vi.advanceTimersByTimeAsync(999);
    expect(f.reads).toHaveLength(before);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.reads).toHaveLength(before + 1);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.reads).toHaveLength(before + 2);
    const release = f.holdReads();
    try {
      await vi.advanceTimersByTimeAsync(1_000);
      const held = f.reads.length;
      expect(held).toBe(before + 3);
      await vi.advanceTimersByTimeAsync(3_000); // Still within the existing operation budget.
      expect(f.reads).toHaveLength(held);
      f.readable(true);
      release();
      await vi.advanceTimersByTimeAsync(0);
      await absentAndReadmittable(f);
    } finally { release(); }
  });

  it.each(["close", "lease abort"] as const)("CONTROL: %s cancels a retained losing-CAS retry", async stop => {
    const f = await fixture(() => 0);
    const put = vi.spyOn(f.world.presence, "put");
    await interrupt(f, false);
    const original = put.mock.calls.at(-1)!;
    await f.view.remove(ID);
    const remove = f.world.presence.delete.bind(f.world.presence);
    vi.spyOn(f.world.presence, "delete").mockImplementationOnce((key, expected) => {
      f.world.presence.put(...original);
      f.readable(false);
      return remove(key, expected);
    });
    await recover(f, "manual");
    if (stop === "close") await f.view.close();
    else f.abort.abort(new Error("owner lease fenced after cleanup CAS loss"));
    await vi.advanceTimersByTimeAsync(0);
    const before = f.reads.length, writes = [...f.world.writes];
    f.readable(true);
    await vi.advanceTimersByTimeAsync(600_000);
    expect(f.reads).toHaveLength(before);
    expect(f.world.writes).toEqual(writes);
    expect(vi.getTimerCount()).toBe(0);
    expect(f.view.ready).toBe(false);
  });
});
