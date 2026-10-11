import { readFileSync, writeFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { JetStreamClient } from "@nats-io/jetstream";
import type { FabricHostLease } from "../src/topology/host-leases.js";
import { LeaseLostError, NatsKvLeaseStore, newLeaseIncarnation, type LeaseSnapshot } from "../src/topology/nats-kv-leases.js";
import { artifactDirectory, deferred, hasNatsLeaseServer, NatsLeaseCluster, sleep } from "./helpers/nats-lease-cluster.js";

import { FencedLeaseResource } from "./helpers/nats-lease-resource.js";

interface Interval { owner: string; start: number; end: number; revisions: number[] }
interface Event { atMs: number; kind: string; owner?: string; revision?: number; detail?: string }
const makeLease = (id: string, identityId: string, ttl: number, startedAt: number): FabricHostLease => {
  const now = Date.now(); return { id, rootId: "fault-root", identityId, startedAt,
    updatedAt: now, expiresAt: now + ttl };
};

describe.skipIf(!hasNatsLeaseServer())("NatsKvLeaseStore leader-kill fault (R3, always sync; requires local binary)", () => {
  const cluster = new NatsLeaseCluster("fault", 10_000);
  // The protected resource has independent transport/leadership. Lease-leader
  // loss must not turn the observer into the same failed lease client.
  const protectedCluster = new NatsLeaseCluster("protected", 10_000);
  const resources: FencedLeaseResource[] = [];
  let killResource: FencedLeaseResource, pausedResource: FencedLeaseResource;
  beforeAll(async () => {
    await Promise.all([cluster.start(), protectedCluster.start()]);
    killResource = await FencedLeaseResource.open(protectedCluster, "KILL"); resources.push(killResource);
    pausedResource = await FencedLeaseResource.open(protectedCluster, "PAUSED"); resources.push(pausedResource);
  }, 40_000);
  afterAll(async () => {
    try { await Promise.all(resources.map(resource => resource.close())); }
    finally { await Promise.all([cluster.close(), protectedCluster.close()]); }
  }, 15_000);

  it("SIGSTOP across expiry then SIGCONT: stale owner cannot land a write after successor", async () => {
    // Run this before leader-kill, so only the OWNER is paused, not admission metadata.
    const resource = pausedResource;
    const child = spawn("bun", [resolve("tests/helpers/nats-lease-paused.ts"), cluster.servers.join(","),
      cluster.bucket, "paused-lease", resource.subject, protectedCluster.servers.join(",")], { stdio: ["ignore", "pipe", "pipe"] });
    const exited = once(child, "exit");
    let stdout = "", stderr = "", stopped = false;
    child.stderr!.on("data", chunk => { stderr += chunk.toString(); });
    try {
      const old = await new Promise<LeaseSnapshot>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Paused child not ready: ${stderr}`)), 10_000);
        child.stdout!.on("data", chunk => {
          stdout += chunk.toString();
          const ready = stdout.match(/PAUSE_READY (.+)\n/);
          if (ready) { clearTimeout(timer); resolve(JSON.parse(ready[1]!)); }
        });
        child.once("error", error => { clearTimeout(timer); reject(error); });
        child.once("exit", () => { clearTimeout(timer); reject(new Error(`Paused child exited early: ${stderr}`)); });
      });
      expect(child.kill("SIGSTOP")).toBe(true); stopped = true;
      await sleep(20); // OS signal delivery only, NOT expiry/acquisition polling.
      expect(readFileSync(`/proc/${child.pid}/status`, "utf8")).toMatch(/State:\s+T/);
      await resource.waitObserved("paused-A-first");
      // The lease watcher, not a client expiry timer, drives the successor.
      const successor = await cluster.store.acquireWaiting(() => makeLease("paused-lease", "paused-B", 5_000,
        old.lease.startedAt!), newLeaseIncarnation(), { waitMs: 12_000 });
      expect(successor.revision).toBeGreaterThan(old.revision);
      expect((await resource.write(successor, "paused-B", "paused-B-first")).accepted).toBe(true);
      expect(child.kill("SIGCONT")).toBe(true); stopped = false;
      const [code, signal] = await exited;
      expect({ code, signal, stderr }).toEqual({ code: 0, signal: null, stderr: "" });
      expect(stdout).toContain('STALE_WRITE {"accepted":false');
      expect((await resource.write(successor, "paused-B", "paused-B-after-stale-attempt")).accepted).toBe(true);
      expect(resource.violations).toEqual([]);
      expect(resource.observed.map(w => w.owner)).toEqual(["paused-A", "paused-B", "paused-B"]);
      expect(resource.rejected.map(w => w.writeId)).toEqual(["paused-A-stale-after-successor"]);
      const result = { scenario: "SIGSTOP owner across server TTL expiry; successor accepted write; SIGCONT stale write rejected",
        physicalHosts: 1, syncInterval: "always", pausedPid: child.pid,
        predecessorFence: old.revision, successorFence: successor.revision,
        protectedWrites: resource.observed, rejectedWrites: resource.rejected, fenceViolations: resource.violations,
        stdout, stderr, exitCode: code };
      writeFileSync(join(artifactDirectory(), "paused-owner.json"), JSON.stringify(result, null, 2) + "\n");
      console.log(`PAUSED_OWNER ${JSON.stringify(result)}`);
      expect(await cluster.store.release(successor)).toBe(true);
    } finally {
      if (stopped) child.kill("SIGCONT");
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exited;
      writeFileSync(join(artifactDirectory(), "paused-owner-process.log"), stdout + stderr);
    }
  }, 25_000);

  it("kills actual KV leader during renewals; independent protected writes remain fenced", async () => {
    const resource = killResource;
    const ncA = await cluster.connection(), ncB = await cluster.connection();
    const a = await NatsKvLeaseStore.open(ncA, { bucket: cluster.bucket, maxLeaseMs: 10_000, timeoutMs: 400, monitoringUrls: cluster.monitoringUrls });
    const b = await NatsKvLeaseStore.open(ncB, { bucket: cluster.bucket, maxLeaseMs: 10_000, timeoutMs: 400, monitoringUrls: cluster.monitoringUrls });
    const id = "fault-lease", ttl = 5_000, start = performance.now();
    const events: Event[] = [], intervals: Interval[] = [];
    const at = (): number => performance.now() - start;
    const log = (event: Omit<Event, "atMs">): void => { events.push({ atMs: at(), ...event }); };
    const incarnationA = newLeaseIncarnation(), incarnationB = newLeaseIncarnation();
    const startedA = Date.now(), startedB = startedA; // Deliberately equal start milliseconds.
    let handleA = (await a.acquire(makeLease(id, "A", ttl, startedA), incarnationA))!;
    expect(handleA).toBeDefined();
    expect((await resource.write(handleA, "A", "A-first")).accepted).toBe(true);
    // Conservative intervals are client authority, NOT a sampled KV value. This catches
    // a successor acknowledged while a predecessor still believes its own lease is valid.
    const endFor = (handle: LeaseSnapshot): number => at() + Math.max(0, handle.lease.expiresAt - Date.now());
    const intervalA = { owner: "A", start: at(), end: endFor(handleA), revisions: [handleA.revision] };
    intervals.push(intervalA); log({ kind: "acquired", owner: "A", revision: handleA.revision });
    const renewalInFlight = deferred(), resumeRenewal = deferred();
    let activeA = true, attemptsA = 0, successesA = 0, errorsA = 0, renewalPausedAtKill = false;
    let attemptsB = 0, errorsB = 0, successesB = 0;
    const js = (a as unknown as { js: JetStreamClient }).js, publish = js.publish.bind(js);
    js.publish = async (...args) => {
      if (attemptsA === 3) {
        // Pin loss between the third renewal's real leader read and its CAS publish.
        // No synthetic publish failure: resume calls the real client against the failed cluster.
        renewalPausedAtKill = true;
        log({ kind: "renew-in-flight-before-leader-kill", owner: "A", revision: handleA.revision });
        renewalInFlight.resolve(); await resumeRenewal.promise;
      }
      return publish(...args);
    };
    const renewA = (async () => {
      for (let i = 0; i < 40 && activeA; i++) {
        await sleep(40);
        attemptsA++;
        try {
          handleA = await a.renew(handleA, makeLease(id, "A", ttl, startedA));
          successesA++; intervalA.end = endFor(handleA); intervalA.revisions.push(handleA.revision);
          expect((await resource.write(handleA, "A", `A-renew-${successesA}`)).accepted).toBe(true);
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
      expect((await resource.write(handleB, "B", "B-first")).accepted).toBe(true);
      expect((await resource.write(handleA, "A", "A-stale-after-B")).accepted).toBe(false);
      await expect(a.renew(handleA, makeLease(id, "A", ttl, startedA))).rejects.toBeInstanceOf(LeaseLostError);
      expect(await a.release(handleA)).toBe(false);
      log({ kind: "stale-owner-renew-and-close-fenced", owner: "A", revision: handleA.revision });
      for (let i = 0; i < 10; i++) {
        await sleep(40);
        handleB = await b.renew(handleB, makeLease(id, "B", ttl, startedB));
        successesB++; intervalB.end = endFor(handleB); intervalB.revisions.push(handleB.revision);
        expect((await resource.write(handleB, "B", `B-renew-${successesB}`)).accepted).toBe(true);
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
      js.publish = publish;
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
      measurement: "Independent protected-resource KV watch verifies every accepted fence in stream order; authority interval sweep is supplemental only",
      protectedWrites: resource.observed, rejectedWrites: resource.rejected, fenceViolations: resource.violations,
      intervals, events };
    writeFileSync(join(artifactDirectory(), "fault.json"), JSON.stringify(result, null, 2) + "\n");
    console.log(`FAULT ${JSON.stringify(result)}`);
    expect(killedLeader).not.toBe(""); expect(info.cluster?.leader).not.toBe(killedLeader);
    expect(renewalPausedAtKill).toBe(true);
    expect(successesB).toBe(10); expect(successesA).toBeGreaterThanOrEqual(2); expect(attemptsA).toBeGreaterThanOrEqual(3);
    expect(maxOverlappingOwners).toBe(0); expect(intervals).toHaveLength(2);
    expect(resource.violations).toEqual([]);
    expect(resource.observed.filter(w => w.owner === "B")).toHaveLength(11);
    expect(resource.rejected.some(w => w.writeId === "A-stale-after-B")).toBe(true);
    expect(await b.read(id)).toBeUndefined();
  }, 30_000);
});
