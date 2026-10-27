// Keep: timing/failure paths (independent refresh deadlines, broker failure and shutdown)
// are not cheaply reachable in a user flow. Exercise the real directory over fake NATS KV/CAS/watches.
import { performance } from "node:perf_hooks";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUnavailableError, TransportUncertainError } from "../src/transport/types.js";
import { hKey, kvWorld, natsLease, pKey } from "./helpers/bridge-nats-kv.js";

const MACHINE = "test-machine";
const ROOT = "session:jitter-root";
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

function record(id: string): FabricParticipantRecord {
  return { format: 1, id, kind: "agent", rootId: ROOT, ownerHostId: ROOT, ownerIdentityId: ROOT,
    ownerIncarnation: "inc-1", name: id, status: "running", runner: "pi", transport: "process",
    capabilities: ["steer"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
}

async function fixture(random?: () => number) {
  const world = kvWorld(), abort = new AbortController(), errors: unknown[] = [];
  const view = new KvParticipantDirectory({ transport: world.transport(MACHINE, ROOT),
    lease: { live: () => !abort.signal.aborted, signal: abort.signal }, identityId: ROOT,
    ownerIncarnation: "inc-1", rootId: ROOT, ...(random ? { random } : {}), onError: error => { errors.push(error); } });
  open.push(view);
  view.start();
  while (!view.ready) await view.whenChanged("ready", new AbortController().signal);
  const revision = (id: string) => world.presence.values.get(pKey(MACHINE, id))?.revision;
  return { world, view, abort, errors, revision };
}

describe("KV participant presence refresh jitter", () => {
  it("spreads simultaneous publications across the exact 144–216 s bounds, independently sampling every success", async () => {
    const random = vi.fn<() => number>().mockReturnValueOnce(0).mockReturnValueOnce(1)
      .mockReturnValueOnce(0.5).mockReturnValue(0.25);
    const f = await fixture(random);
    await f.view.publishBeforeLaunch(record("agent:a"));
    await f.view.publishBeforeLaunch(record("agent:b"));
    const a = f.revision("agent:a"), b = f.revision("agent:b");
    await vi.advanceTimersByTimeAsync(143_999);
    expect(f.revision("agent:a")).toBe(a);
    expect(f.revision("agent:b")).toBe(b);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.revision("agent:a")).toBeGreaterThan(a!);
    expect(f.revision("agent:b")).toBe(b);
    const renewedA = f.revision("agent:a");
    await vi.advanceTimersByTimeAsync(71_999);
    expect(f.revision("agent:b")).toBe(b);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.revision("agent:b")).toBeGreaterThan(b!);
    // A's second sample is 0.5, not its initial 0: next successful refresh at 144 + 180 s.
    await vi.advanceTimersByTimeAsync(107_999);
    expect(f.revision("agent:a")).toBe(renewedA);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.revision("agent:a")).toBeGreaterThan(renewedA!);
    expect(random).toHaveBeenCalledTimes(5);
    expect(f.errors).toEqual([]);
  });

  it("rearms an existing timer when a later publication samples an earlier deadline", async () => {
    const random = vi.fn<() => number>().mockReturnValueOnce(1).mockReturnValueOnce(0).mockReturnValue(1);
    const f = await fixture(random);
    await f.view.publishBeforeLaunch(record("agent:late"));
    await f.view.publishBeforeLaunch(record("agent:early"));
    const late = f.revision("agent:late"), early = f.revision("agent:early");
    await vi.advanceTimersByTimeAsync(143_999);
    expect(f.revision("agent:early")).toBe(early);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.revision("agent:early")).toBeGreaterThan(early!);
    expect(f.revision("agent:late")).toBe(late);
    await vi.advanceTimersByTimeAsync(72_000);
    expect(f.revision("agent:late")).toBeGreaterThan(late!);
  });

  it.each([[0, 144_000], [0.25, 162_000], [1, 180_000]])(
    "independently samples failure retry %s without exceeding 180 s or spinning", async (sample, delay) => {
      const random = vi.fn<() => number>().mockReturnValueOnce(0).mockReturnValueOnce(sample!).mockReturnValue(1);
      const f = await fixture(random);
      const rec = record("agent:failure");
      await f.view.publishBeforeLaunch(rec);
      const revision = f.revision(rec.id);
      const failure = new TransportUnavailableError("broker unavailable");
      const put = vi.spyOn(f.world.presence, "put").mockImplementationOnce(() => { throw failure; });
      await vi.advanceTimersByTimeAsync(144_000);
      expect(f.revision(rec.id)).toBe(revision);
      expect(f.errors).toEqual([failure]);
      expect(random).toHaveBeenCalledTimes(2);
      await f.view.refreshOwned(); // A not-yet-due failed row must not hot-loop.
      await vi.advanceTimersByTimeAsync(delay! - 1);
      expect(put).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1);
      const recovered = f.revision(rec.id);
      expect(recovered).toBeGreaterThan(revision!);
      expect(put.mock.calls.map(call => call[3])).toEqual([revision, revision]);
      expect(random).toHaveBeenCalledTimes(3);
      // Recovery resamples the full success spread, not the capped failure deadline.
      await vi.advanceTimersByTimeAsync(215_999);
      expect(f.revision(rec.id)).toBe(recovered);
      await vi.advanceTimersByTimeAsync(1);
      expect(f.revision(rec.id)).toBeGreaterThan(recovered!);
      const refreshed = f.revision(rec.id);
      const changed = { ...rec, status: "idle" };
      await f.view.publish(changed);
      expect(f.revision(rec.id)).toBeGreaterThan(refreshed!);
      expect(f.world.presence.values.get(pKey(MACHINE, rec.id))?.value).toEqual(changed);
      expect(random).toHaveBeenCalledTimes(5);
    });

  it("R5: after an initial 216 s delay and two transient failures, the third attempt is inside the 600 s TTL", async () => {
    const random = vi.fn<() => number>().mockReturnValueOnce(1).mockReturnValueOnce(1)
      .mockReturnValueOnce(1).mockReturnValue(0);
    const f = await fixture(random);
    const rec = record("agent:two-failures");
    const admitted = await f.view.publishBeforeLaunch(rec);
    const written = Date.now(), attempts: number[] = [], revisions: number[] = [];
    const put = f.world.presence.put.bind(f.world.presence);
    let failures = 2;
    vi.spyOn(f.world.presence, "put").mockImplementation((...args) => {
      attempts.push(Date.now() - written);
      revisions.push(args[3]);
      if (failures-- > 0) throw new TransportUnavailableError("transient presence failure");
      return put(...args);
    });
    await vi.advanceTimersByTimeAsync(700_000);
    expect(attempts).toEqual([216_000, 396_000, 576_000]);
    expect(attempts[2]).toBeLessThan(600_000);
    expect(revisions).toEqual([admitted.revision, admitted.revision, admitted.revision]);
    const recovered = f.revision(rec.id);
    expect(recovered).toBeGreaterThan(admitted.revision);
    expect(f.errors).toHaveLength(2);
    expect(random).toHaveBeenCalledTimes(4);
    await vi.advanceTimersByTimeAsync(19_999);
    expect(f.revision(rec.id)).toBe(recovered);
    await vi.advanceTimersByTimeAsync(1); // Recovery's independent low success sample: 576 + 144 s.
    expect(attempts).toEqual([216_000, 396_000, 576_000, 720_000]);
    expect(f.revision(rec.id)).toBeGreaterThan(recovered!);
  });

  it("does not advance successful-write evidence on failure or let a stale retry override a later publication", async () => {
    const random = vi.fn<() => number>().mockReturnValueOnce(0).mockReturnValue(1);
    const f = await fixture(random);
    const rec = record("agent:write-evidence");
    const admitted = await f.view.publishBeforeLaunch(rec);
    const failure = new TransportUnavailableError("broker unavailable");
    const put = vi.spyOn(f.world.presence, "put").mockImplementationOnce(() => { throw failure; });
    await vi.advanceTimersByTimeAsync(144_000);
    expect(f.revision(rec.id)).toBe(admitted.revision);
    expect(f.errors).toEqual([failure]);
    await vi.advanceTimersByTimeAsync(36_000);
    // Same content is now 180 s old from the last SUCCESS, not 36 s old from the failure.
    const published = await f.view.publish(rec);
    expect(published.revision).toBeGreaterThan(admitted.revision);
    expect(random).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(144_000); // The failed attempt's old retry at 324 s is cancelled.
    expect(f.revision(rec.id)).toBe(published.revision);
    expect(put).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(71_999);
    expect(f.revision(rec.id)).toBe(published.revision);
    await vi.advanceTimersByTimeAsync(1); // New success deadline: 180 + 216 s.
    expect(f.revision(rec.id)).toBeGreaterThan(published.revision);
    expect(random).toHaveBeenCalledTimes(4);
  });

  it("defaults to Math.random without a public configuration knob", async () => {
    const random = vi.spyOn(Math, "random").mockReturnValue(0.5);
    const f = await fixture();
    await f.view.publishBeforeLaunch(record("agent:default"));
    const revision = f.revision("agent:default");
    expect(random).toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(179_999);
    expect(f.revision("agent:default")).toBe(revision);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.revision("agent:default")).toBeGreaterThan(revision!);
  });

  it.each([
    ["close", false], ["lease abort", false], ["close", true], ["lease abort", true],
  ] as const)("cancels pending refreshes on %s (after failure: %s)", async (stop, fail) => {
    const random = vi.fn(() => 1);
    const f = await fixture(random);
    await f.view.publishBeforeLaunch(record("agent:stop"));
    const put = vi.spyOn(f.world.presence, "put");
    if (fail) {
      put.mockImplementationOnce(() => { throw new TransportUnavailableError("broker unavailable"); });
      await vi.advanceTimersByTimeAsync(216_000);
      expect(f.errors).toHaveLength(1);
    }
    if (stop === "close") await f.view.close();
    else f.abort.abort(new Error("owner fenced"));
    const writes = [...f.world.writes];
    await vi.advanceTimersByTimeAsync(600_000);
    expect(f.world.writes).toEqual(writes);
    expect(put).toHaveBeenCalledTimes(fail ? 1 : 0);
    expect(random).toHaveBeenCalledTimes(fail ? 2 : 1);
    expect(f.view.ready).toBe(false);
  });
});

// Keep: PR #1624's real restart exposed a lost confirmed receipt after an uncertain
// heartbeat update. Recovery must not turn a read revision into takeover authority.
async function restartPresence() {
  const f = await fixture(() => 1);
  f.world.leases.put(hKey(MACHINE, ROOT), `${ROOT}#inc-1`, natsLease(ROOT, "inc-1", Date.now()), 0);
  await vi.advanceTimersByTimeAsync(0);
  const initial: FabricParticipantRecord = { ...record(ROOT), kind: "root", transport: "host" };
  const first = await f.view.publish(initial);
  const attempted = { ...initial, status: "idle" };
  return { ...f, initial, first, attempted, key: pKey(MACHINE, ROOT) };
}

async function interruptPresence(f: Awaited<ReturnType<typeof restartPresence>>, committed: boolean) {
  let readable = true;
  const get = f.world.presence.values.get.bind(f.world.presence.values);
  vi.spyOn(f.world.presence.values, "get").mockImplementation(key => {
    if (!readable) throw new TransportUnavailableError("broker disconnected during presence reconciliation");
    return get(key);
  });
  const put = f.world.presence.put.bind(f.world.presence);
  vi.spyOn(f.world.presence, "put").mockImplementationOnce((...args) => {
    if (committed) put(...args);
    readable = false;
    throw new TransportUncertainError(args[0], { owner: args[1], value: args[2] });
  });
  await expect(f.view.publish(f.attempted)).rejects.toBeInstanceOf(TransportUncertainError);
  await vi.advanceTimersByTimeAsync(0); // The event-driven cleanup read is unavailable too.
  readable = true;
  return put;
}

describe("restart presence receipt recovery #1624", () => {
  it("restores only the exact previously confirmed receipt when the uncertain update did not commit", async () => {
    const f = await restartPresence();
    await interruptPresence(f, false);
    await f.view.reconcilePendingCleanup();
    expect(f.revision(ROOT)).toBe(f.first.revision);
    const put = vi.spyOn(f.world.presence, "put");
    await expect(f.view.publish(f.attempted)).resolves.toHaveProperty("revision");
    expect(put.mock.calls.at(-1)?.[3]).toBe(f.first.revision);
    expect(f.world.presence.values.get(f.key)?.value).toEqual(f.attempted);
  });

  it("cleans an uncertain committed attempt, then creates safely instead of adopting its unknown revision", async () => {
    const f = await restartPresence();
    await interruptPresence(f, true);
    const uncertainRevision = f.revision(ROOT)!;
    expect(uncertainRevision).toBeGreaterThan(f.first.revision);
    const remove = vi.spyOn(f.world.presence, "delete");
    await f.view.reconcilePendingCleanup();
    expect(remove.mock.calls).toEqual([[f.key, uncertainRevision]]);
    expect(f.revision(ROOT)).toBeUndefined();
    const put = vi.spyOn(f.world.presence, "put");
    await expect(f.view.publish(f.attempted)).resolves.toHaveProperty("revision");
    expect(put.mock.calls.at(-1)?.[3]).toBe(0);
  });

  it.each(["foreign owner", "same-incarnation unknown revision", "new generation"] as const)(
    "does not restore, overwrite or delete a %s observed during reconciliation", async winnerKind => {
      const f = await restartPresence();
      const put = await interruptPresence(f, false);
      const winner = winnerKind === "foreign owner"
        ? { ...f.initial, ownerHostId: "other-host", ownerIdentityId: "other-host", ownerIncarnation: "other-inc" }
        : winnerKind === "new generation" ? { ...f.initial, ownerIncarnation: "inc-2" } : f.initial;
      const revision = put(f.key, `${winner.ownerHostId}#${winner.ownerIncarnation}`, winner, f.first.revision);
      if (winnerKind === "new generation") {
        const leaseKey = hKey(MACHINE, ROOT);
        f.world.leases.put(leaseKey, `${ROOT}#inc-2`, natsLease(ROOT, "inc-2", Date.now()),
          f.world.leases.values.get(leaseKey)!.revision);
      }
      const remove = vi.spyOn(f.world.presence, "delete");
      await f.view.reconcilePendingCleanup();
      await expect(f.view.publish(f.attempted)).rejects.toThrow();
      await expect(f.view.publish(f.attempted, { force: true })).rejects.toThrow();
      expect(remove).not.toHaveBeenCalled();
      expect(f.world.presence.values.get(f.key)).toMatchObject({ revision, value: winner });
    });

  it("does not delete a new generation that wins the cleanup CAS", async () => {
    const f = await restartPresence();
    const put = await interruptPresence(f, true);
    const remove = f.world.presence.delete.bind(f.world.presence);
    const winner = { ...f.initial, ownerIncarnation: "inc-2" };
    let winnerRevision = 0;
    vi.spyOn(f.world.presence, "delete").mockImplementationOnce((key, revision) => {
      winnerRevision = put(key, `${ROOT}#inc-2`, winner, revision);
      const leaseKey = hKey(MACHINE, ROOT);
      f.world.leases.put(leaseKey, `${ROOT}#inc-2`, natsLease(ROOT, "inc-2", Date.now()),
        f.world.leases.values.get(leaseKey)!.revision);
      return remove(key, revision);
    });
    await f.view.reconcilePendingCleanup();
    await expect(f.view.publish(f.attempted)).rejects.toThrow();
    expect(winnerRevision).toBeGreaterThan(0);
    expect(f.world.presence.values.get(f.key)).toMatchObject({ revision: winnerRevision, value: winner });
  });

  it("withdraws the confirmed predecessor on close instead of restoring a closed owner", async () => {
    const f = await restartPresence();
    await interruptPresence(f, false);
    await f.view.close();
    expect(f.revision(ROOT)).toBeUndefined();
    const writes = [...f.world.writes];
    await vi.advanceTimersByTimeAsync(600_000);
    expect(f.world.writes).toEqual(writes);
  });

  it("retires the stale receipt when a confirmed refresh settles after its lease gate closes", async () => {
    const f = await restartPresence();
    let live = true;
    f.view.options.lease.live = () => live;
    const put = f.world.presence.put.bind(f.world.presence);
    vi.spyOn(f.world.presence, "put").mockImplementationOnce((...args) => {
      const revision = put(...args);
      live = false;
      return revision;
    });
    await expect(f.view.publish(f.attempted)).rejects.toThrow("Owner lease lost before launch admission settled");
    await f.view.reconcilePendingCleanup();
    live = true;
    await expect(f.view.publish(f.attempted)).resolves.toHaveProperty("revision");
  });
});
