import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { KV } from "@nats-io/kv";
import type { FabricHostLease } from "../src/topology/host-leases.js";
import { LeaseLostError, NatsKvLeaseStore, newLeaseIncarnation, type LeaseSnapshot } from "../src/topology/nats-kv-leases.js";
import { artifactDirectory, deferred, hasNatsLeaseServer, NatsLeaseCluster, sleep } from "./helpers/nats-lease-cluster.js";

interface Interval { owner: string; start: number; end: number; revisions: number[] }
interface Event { atMs: number; kind: string; owner?: string; revision?: number; detail?: string }
const makeLease = (id: string, identityId: string, ttl: number, startedAt: number): FabricHostLease => {
  const now = Date.now(); return { id, rootId: "fault-root", identityId, startedAt,
    updatedAt: now, expiresAt: now + ttl };
};

describe.skipIf(!hasNatsLeaseServer())("NatsKvLeaseStore leader-kill fault (R3, always sync; requires local binary)", () => {
  const cluster = new NatsLeaseCluster("fault", 5_000);
  beforeAll(async () => { await cluster.start(); }, 40_000);
  afterAll(async () => { await cluster.close(); }, 15_000);

  it("kills actual KV leader during competing renewals: max overlapping owners = 0", async () => {
    const ncA = await cluster.connection(), ncB = await cluster.connection();
    const a = await NatsKvLeaseStore.open(ncA, { bucket: cluster.bucket, maxLeaseMs: 5_000, timeoutMs: 400 });
    const b = await NatsKvLeaseStore.open(ncB, { bucket: cluster.bucket, maxLeaseMs: 5_000, timeoutMs: 400 });
    const id = "fault-lease", ttl = 1_000, start = performance.now();
    const events: Event[] = [], intervals: Interval[] = [];
    const at = (): number => performance.now() - start;
    const log = (event: Omit<Event, "atMs">): void => { events.push({ atMs: at(), ...event }); };
    const incarnationA = newLeaseIncarnation(), incarnationB = newLeaseIncarnation();
    const startedA = Date.now(), startedB = startedA; // Deliberately equal start milliseconds.
    let handleA = (await a.acquire(makeLease(id, "A", ttl, startedA), incarnationA))!;
    expect(handleA).toBeDefined();
    // Conservative intervals are client authority, NOT a sampled KV value. This catches
    // a successor acknowledged while a predecessor still believes its own lease is valid.
    const endFor = (handle: LeaseSnapshot): number => at() + Math.max(0, handle.lease.expiresAt - Date.now());
    const intervalA = { owner: "A", start: at(), end: endFor(handleA), revisions: [handleA.revision] };
    intervals.push(intervalA); log({ kind: "acquired", owner: "A", revision: handleA.revision });
    const renewalInFlight = deferred(), resumeRenewal = deferred();
    let activeA = true, attemptsA = 0, successesA = 0, errorsA = 0, renewalPausedAtKill = false;
    let attemptsB = 0, errorsB = 0, successesB = 0;
    const kv = (a as unknown as { kv: KV }).kv, update = kv.update.bind(kv);
    kv.update = async (...args) => {
      if (attemptsA === 3) {
        // Pin loss between the third renewal's real leader read and its CAS publish.
        // No synthetic publish failure: resume calls the real client against the failed cluster.
        renewalPausedAtKill = true;
        log({ kind: "renew-in-flight-before-leader-kill", owner: "A", revision: handleA.revision });
        renewalInFlight.resolve(); await resumeRenewal.promise;
      }
      return update(...args);
    };
    const renewA = (async () => {
      for (let i = 0; i < 40 && activeA; i++) {
        await sleep(40);
        attemptsA++;
        try {
          handleA = await a.renew(handleA, makeLease(id, "A", ttl, startedA));
          successesA++; intervalA.end = endFor(handleA); intervalA.revisions.push(handleA.revision);
          log({ kind: "renewed", owner: "A", revision: handleA.revision });
        } catch (error) {
          errorsA++; activeA = false;
          // Retain the entire OLD acknowledged deadline in the overlap sweep, even
          // though A stops now. This also tests a client retaining its old authority.
          log({ kind: "renew-failed-closed", owner: "A", detail: String(error) });
        }
      }
      if (activeA) {
        intervalA.end = Math.min(intervalA.end, at()); activeA = false;
        await a.release(handleA).catch(error => log({ kind: "release-unknown", owner: "A", detail: String(error) }));
      }
    })();
    const contenderB = (async () => {
      const deadline = performance.now() + 12_000;
      let handleB: LeaseSnapshot | undefined;
      while (!handleB && performance.now() < deadline) {
        attemptsB++;
        try {
          // Every timeout retry installs a new watcher. There is no periodic read/poll loop.
          handleB = await b.acquireWaiting(() => makeLease(id, "B", ttl, startedB), incarnationB, { waitMs: 2_000 });
        } catch (error) {
          errorsB++; log({ kind: "watch-retry", owner: "B", detail: String(error) });
          // A failed bounded request/watch is the retry trigger. No read-poll/backoff timer.
          // The next acquireWaiting installs its watcher BEFORE its one acquisition probe.
        }
      }
      if (!handleB) throw new Error("Successor did not acquire after leader failover");
      const intervalB = { owner: "B", start: at(), end: endFor(handleB), revisions: [handleB.revision] };
      intervals.push(intervalB); log({ kind: "acquired", owner: "B", revision: handleB.revision });
      expect(handleB.revision).toBeGreaterThan(intervalA.revisions.at(-1)!);
      await expect(a.renew(handleA, makeLease(id, "A", ttl, startedA))).rejects.toBeInstanceOf(LeaseLostError);
      expect(await a.release(handleA)).toBe(false);
      log({ kind: "stale-owner-renew-and-close-fenced", owner: "A", revision: handleA.revision });
      for (let i = 0; i < 10; i++) {
        await sleep(40);
        handleB = await b.renew(handleB, makeLease(id, "B", ttl, startedB));
        successesB++; intervalB.end = endFor(handleB); intervalB.revisions.push(handleB.revision);
        log({ kind: "renewed", owner: "B", revision: handleB.revision });
      }
      intervalB.end = Math.min(intervalB.end, at());
      expect(await b.release(handleB)).toBe(true);
      log({ kind: "released", owner: "B" });
    })();
    // Observe rejection immediately, but await/reap all work before test teardown.
    const outcomeB = contenderB.then(() => undefined, error => error);
    let killedLeader = "";
    try {
      await Promise.race([renewalInFlight.promise, renewA.then(() => {
        throw new Error("Owner A stopped before the pinned third renewal");
      })]);
      killedLeader = await cluster.killLeader();
      log({ kind: "leader-killed", detail: killedLeader });
    } finally {
      resumeRenewal.resolve();
      await Promise.allSettled([renewA, outcomeB]);
      kv.update = update;
    }
    await renewA;
    const errorB = await outcomeB;
    if (errorB) throw errorB;
    // Sweep every client authority endpoint. No sampling gap can hide a short overlap.
    const endpoints = intervals.flatMap(interval => [
      { time: interval.start, delta: 1 }, { time: interval.end, delta: -1 },
    ]).sort((x, y) => x.time - y.time || x.delta - y.delta);
    let active = 0, maxConcurrentOwners = 0;
    for (const endpoint of endpoints) { active += endpoint.delta; maxConcurrentOwners = Math.max(maxConcurrentOwners, active); }
    const maxOverlappingOwners = Math.max(0, maxConcurrentOwners - 1);
    const info = await cluster.jsm.streams.info(`KV_${cluster.bucket}`);
    const result = { scenario: "SIGKILL KV stream leader during renewal and competing watch acquisition",
      killedLeader, successorLeader: info.cluster?.leader, serverVersion: cluster.nc.info?.version,
      replicas: info.config.num_replicas, physicalHosts: 1, syncInterval: "always",
      renewalPausedAtKill, attemptsA, successesA, errorsA, attemptsB, errorsB, successesB,
      maxConcurrentOwners, maxOverlappingOwners,
      measurement: "Sweep of every acknowledged client authority endpoint; predecessor retains old deadline after failure; release ends authority; not sampled KV reads",
      intervals, events };
    writeFileSync(join(artifactDirectory(), "fault.json"), JSON.stringify(result, null, 2) + "\n");
    console.log(`FAULT ${JSON.stringify(result)}`);
    expect(killedLeader).not.toBe(""); expect(info.cluster?.leader).not.toBe(killedLeader);
    expect(renewalPausedAtKill).toBe(true);
    expect(successesB).toBe(10); expect(successesA).toBeGreaterThanOrEqual(2); expect(attemptsA).toBeGreaterThanOrEqual(3);
    expect(maxOverlappingOwners).toBe(0); expect(intervals).toHaveLength(2);
    expect(await b.read(id)).toBeUndefined();
  }, 30_000);
});
