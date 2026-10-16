import { createHash } from "node:crypto";
import { chmodSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { describe, expect, it, vi } from "vitest";
import {
  readLocalHostLeaseProcessProof, transportHostLeasePredecessorExited, TransportHostLease,
  TRANSPORT_HOST_LEASE_SAFETY_MS, TRANSPORT_HOST_LEASE_TTL_MS,
  type TransportHostLeaseLostError, type TransportHostLeaseReceipt,
} from "../../src/topology/host-leases.js";
import { KvParticipantDirectory } from "../../src/topology/participant-view.js";
import type { FabricParticipantRecord } from "../../src/topology/types.js";
import {
  DirectoryNotReadyError, TransportUnavailableError, TransportUncertainError,
  type FabricTransport, type TransportBucket, type TransportKv, type TransportKvEvent,
} from "../../src/transport/types.js";
import {
  boundedReceipt, createStubKv, faultGate, openRealAdapterPair, owner,
  type RealAdapterPair,
} from "../helpers/transport-harness.js";

/**
 * CONTRACT-v2 §§1,4,11,13; E6/E8/E11/E12/E19/E20.
 * Keep reasons: data safety, authenticated attribution, and native predecessor refusal.
 *
 * BROKER cases use the existing real factory pair: a fresh Path B server, bootstrap-only
 * provisioning (closed before runtime open), then distinct machine-A/B runtime JWTs.
 * The actual connection performs E12 USER.INFO; no admin carrier/verifyMachine bypass.
 * Both KvParticipantDirectory and TransportHostLease below are the actual A2 classes.
 * This is manually-composed public-class proof, not application-root wiring evidence.
 * An idle receiver lease in watch-only cases grants NO local consumption authority.
 *
 * The tap ONLY delays forwarding an ALREADY OBSERVED public ready event. It never makes
 * events, edits entries, reorders events, or derives readiness from get()/keys(). This is
 * A2 composition timing AFTER B's broker snapshot marker, NOT a hold inside that snapshot.
 *
 * Expected coverage exclusions (not PASS evidence):
 * - default test:affected has no broker: missing Path B binary skips unless
 *   NATS_AUTH_REQUIRED=1, which fails; invalid explicit NATS_BIN always errors;
 *   all source/protocol failures after prerequisites remain failures, never skips;
 * - acquired leases require Linux /proc; empty-watch composition does not acquire a lease;
 * - real consumer DELETE/reset is AUTHOR-ONLY HOSTED: opt in with
 *   FABRIC_TRANSPORT_HOSTED_PRESENCE_RESET=1 ONLY on an authorized hosted runner.
 *   The ordinary local run must NOT set it. No real KV delete/release is invoked here.
 * - true marker-after-data broker snapshot races, SIGSTOP/network partition, exited-child
 *   takeover, and root wiring to control.pause(reason) remain parent/native obligations.
 *   No fake PID, process-exit oracle, directory, admission engine or successor validator.
 * PROTOCOL/UNIT cases use a stub KV solely as the carrier; the actual lease class owns
 * cutoff, uncertainty, receipt custody, successor policy, abort, and loss notification.
 * Test receipts use the runner's private TMPDIR; the parent owns runner/cache placement
 * and executes with --cache=false. Nothing has been run by authoring.
 */

// Pass the CI broker gate explicitly for compatibility with the currently pinned helper.
// resolveNatsBinary() in that helper throws on invalid explicit NATS_BIN before any skip.
const required = process.env.NATS_AUTH_REQUIRED === "1";
const linux = process.platform === "linux";
const cutoffMs = TRANSPORT_HOST_LEASE_TTL_MS - TRANSPORT_HOST_LEASE_SAFETY_MS;
const token = (id: string): string => createHash("sha256").update(id).digest("hex").slice(0, 32);
function localDirectory(): string {
  const dir = mkdtempSync(join(tmpdir(), "fabric-transport-lease-"));
  chmodSync(dir, 0o700);
  return dir;
}
function receiptFile(dir: string, hostId: string): string {
  return join(dir, `transport-host-lease-${token(hostId)}.json`);
}
function receipt(dir: string, hostId: string): TransportHostLeaseReceipt {
  return JSON.parse(readFileSync(receiptFile(dir, hostId), "utf8")) as TransportHostLeaseReceipt;
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

interface Observation {
  readonly event: TransportKvEvent;
  /** Resolves when the directory requests its NEXT event, after applying this event. */
  readonly consumed: Promise<void>;
  release(): void;
}
/** Observation/backpressure only, never a second KV/watch or readiness implementation. */
function tapWatch(kv: TransportKv) {
  const observations: Observation[] = [];
  const listeners = new Set<() => void>();
  const store: TransportKv = {
    ttlMs: kv.ttlMs,
    get: kv.get.bind(kv),
    put: (key, writer, value, revision, signal) => kv.put(key, writer, value, revision, signal),
    delete: (key, revision, signal) => kv.delete(key, revision, signal),
    watch: async function* <V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
      for await (const event of kv.watch<V>(prefix, signal)) {
        const gate = faultGate(), applied = deferred<void>();
        const observation: Observation = { event, consumed: applied.promise, release: gate.release };
        observations.push(observation);
        for (const listener of [...listeners]) listener();
        // Gate only the real public marker; data and resets pass through unchanged.
        if (event.type === "ready") await gate.wait(signal);
        yield event;
        applied.resolve();
      }
    },
  };
  const wait = (predicate: (row: Observation) => boolean, label: string, timeoutMs = 5_000): Promise<Observation> => {
    let listener: (() => void) | undefined;
    const pending = new Promise<Observation>(resolve => {
      listener = () => {
        const found = observations.find(predicate);
        if (found) { listeners.delete(listener!); resolve(found); }
      };
      listeners.add(listener);
      listener();
    });
    return boundedReceipt(pending, label, timeoutMs).finally(() => { if (listener) listeners.delete(listener); });
  };
  return {
    store,
    marker: (index = 0) => wait(row => row.event.type === "ready" &&
      observations.filter(item => item.event.type === "ready").indexOf(row) === index, `real ready marker ${index}`),
    reset: (index = 0) => wait(row => row.event.type === "reset" &&
      observations.filter(item => item.event.type === "reset").indexOf(row) === index, `real reset ${index}`, 75_000),
    put: (key: string, revision: number) => wait(row => row.event.type === "put" &&
      row.event.entry.key === key && row.event.entry.revision === revision, "actual put reaches directory"),
    diagnostics: () => observations.map(row => row.event.type === "put"
      ? { type: row.event.type, key: row.event.entry.key, revision: row.event.entry.revision }
      : row.event),
    releaseAll: () => { for (const row of observations) row.release(); },
  };
}
function watchedTransport(transport: FabricTransport, taps: Record<TransportBucket, ReturnType<typeof tapWatch>>): FabricTransport {
  return {
    kind: transport.kind, self: transport.self,
    send: transport.send.bind(transport),
    inbox: signal => transport.inbox(signal),
    kv: bucket => taps[bucket].store,
    drainState: signal => transport.drainState(signal),
    close: () => transport.close(),
  };
}
function notReady(view: KvParticipantDirectory, id: string): void {
  expect(view.ready).toBe(false);
  expect(() => view.resolve(id)).toThrow(DirectoryNotReadyError);
  try { view.resolve(id); } catch (error) { expect(error).toHaveProperty("code", "FABRIC_DIRECTORY_NOT_READY"); }
}
async function consumed(row: Observation): Promise<void> {
  await boundedReceipt(row.consumed, `A2 applied actual ${row.event.type}`);
}

/** Read-only directory: presence is seeded through the authenticated sender, so close() owns
 * no publication receipts and cannot invoke presence.delete(). Held broker leases are cut
 * off locally, not released/deleted; the owned server is then stopped by pair.close(). */
async function compose(pair: RealAdapterPair, full: boolean) {
  const dir = localDirectory(), clock = { value: 0 }, errors: unknown[] = [];
  const senderLease = new TransportHostLease({ kv: pair.sender.kv("leases"), self: pair.sender.self,
    rootId: "root:sender", identityId: "identity:sender", incarnation: "sender-birth-1",
    localStateDir: dir, monotonic: () => clock.value });
  const receiverLease = new TransportHostLease({ kv: pair.receiver.kv("leases"), self: pair.receiver.self,
    rootId: "root:receiver", identityId: "identity:receiver", incarnation: "receiver-birth-1",
    localStateDir: dir }); // intentionally idle: empty snapshots, no local admission claim
  const taps = { presence: tapWatch(pair.receiver.kv("presence")), leases: tapWatch(pair.receiver.kv("leases")) };
  const view = new KvParticipantDirectory({ transport: watchedTransport(pair.receiver, taps), lease: receiverLease,
    rootId: "root:receiver", identityId: "identity:receiver", ownerIncarnation: "receiver-birth-1",
    onError: error => errors.push(error) });
  const record: FabricParticipantRecord = { format: 1, id: "task:presence-contract", kind: "agent",
    rootId: "root:sender", ownerHostId: pair.sender.self.hostId, ownerIdentityId: "identity:sender",
    ownerIncarnation: "sender-birth-1", name: "real KV participant", status: "running", runner: "pi",
    transport: "process", capabilities: ["followUp", "steer", "stop"], controlProtocol: "v1",
    startedAt: Date.now(), updatedAt: Date.now() };
  const key = `${pair.sender.self.machine}.p.${token(record.id)}`;
  let revision = 0;
  try {
    if (full) {
      const value = await senderLease.acquire(AbortSignal.timeout(5_000));
      expect(value).toMatchObject(readLocalHostLeaseProcessProof());
      // A hostile display/JSON machine label cannot replace the authenticated key prefix.
      revision = await pair.sender.kv("presence").put(key, senderLease.owner,
        { ...record, machine: pair.receiver.self.machine }, 0, AbortSignal.timeout(5_000));
    }
    return { view, taps, record, key, revision, senderLease, errors,
      close: async () => {
        clock.value = cutoffMs; senderLease.live(); // stop only the actual local cutoff alarm
        taps.presence.releaseAll(); taps.leases.releaseAll();
        try { await boundedReceipt(view.close(), "close read-only A2 directory"); }
        finally { rmSync(dir, { recursive: true, force: true }); }
      } };
  } catch (error) {
    clock.value = cutoffMs; senderLease.live();
    try { await view.close(); } finally { rmSync(dir, { recursive: true, force: true }); }
    throw error;
  }
}

describe("A2 real authenticated watch → manually-composed ParticipantView", () => {
  for (const full of [false, true]) {
    it.skipIf(full && !linux)(`${full ? "full" : "empty"} snapshot: resolve throws until BOTH actual ready markers; authenticated routing`, async context => {
      const opened = await openRealAdapterPair("nats", required);
      if (opened.status === "skip") { context.skip(opened.reason); return; }
      const pair = opened.pair;
      let composed: Awaited<ReturnType<typeof compose>> | undefined;
      try {
        composed = await compose(pair, full);
        const { view, taps, record } = composed;
        expect(view).toBeInstanceOf(KvParticipantDirectory);
        expect(composed.senderLease).toBeInstanceOf(TransportHostLease);
        notReady(view, record.id); view.start(); notReady(view, record.id);
        const [presence, leases] = await Promise.all([taps.presence.marker(), taps.leases.marker()]);
        if (full) await consumed(await taps.presence.put(composed.key, composed.revision));
        // Actual broker markers already exist, but neither has reached the actual A2 view.
        notReady(view, record.id);
        let changed = false;
        const ready = view.whenChanged("known-absent", AbortSignal.timeout(5_000)).then(() => { changed = true; });
        // Exercise both asymmetric orderings: neither a presence-only nor lease-only marker suffices.
        const [first, second] = full ? [leases, presence] : [presence, leases];
        first!.release(); await consumed(first!);
        notReady(view, record.id); expect(changed).toBe(false);
        second!.release(); await consumed(second!); await boundedReceipt(ready, "actual A2 ready waiter");
        expect(view.ready).toBe(true); expect(changed).toBe(true);
        expect(view.resolve("known-absent")).toBeUndefined();
        expect(view.leaseLive(), "idle receiver lease must not grant local consumption").toBe(false);
        if (full) {
          expect(view.resolve(record.id)).toEqual({ machine: pair.sender.self.machine,
            ownerHostId: pair.sender.self.hostId, ownerIdentityId: record.ownerIdentityId,
            ownerIncarnation: record.ownerIncarnation, capabilities: record.capabilities, live: true });
          expect(view.list()).toMatchObject([{ id: record.id, machine: pair.sender.self.machine, local: false, stale: false }]);
        } else expect(view.resolve(record.id)).toBeUndefined();
        expect(composed.errors).toEqual([]);
      } finally {
        try { await composed?.close(); } finally { await pair.close(); }
      }
    }, 60_000);
  }

  it.skipIf(!linux || process.env.FABRIC_TRANSPORT_HOSTED_PRESENCE_RESET !== "1")(
    "AUTHOR-ONLY HOSTED consumer DELETE: actual reset invalidates A2, re-ready cannot resurrect discarded presence", async context => {
      const opened = await openRealAdapterPair("nats", required);
      if (opened.status === "skip") { context.skip(opened.reason); return; }
      const pair = opened.pair;
      let composed: Awaited<ReturnType<typeof compose>> | undefined;
      try {
        if (!pair.kvReadiness) throw new Error("actual ordered-consumer reset fixture is required");
        composed = await compose(pair, true);
        const { view, taps, key, record, senderLease } = composed;
        view.start();
        for (const row of await Promise.all([taps.presence.marker(), taps.leases.marker()])) { row.release(); await consumed(row); }
        expect(view.resolve(record.id)).toHaveProperty("live", true);
        // Retire through a real newer broker PUT that A2 rejects, not a fabricated gone or delete.
        const revision = await pair.sender.kv("presence").put(key, "foreign-owner#invalid", record,
          composed.revision, AbortSignal.timeout(5_000));
        await consumed(await taps.presence.put(key, revision));
        expect(view.resolve(record.id)).toBeUndefined();
        for (let epoch = 0; epoch < 2; epoch++) {
          // HOSTED ONLY: this existing helper invokes the real administrative consumer DELETE.
          // It is never called by the ordinary local run and never by a runtime credential.
          await pair.kvReadiness.reset("presence", ">", AbortSignal.timeout(5_000));
          await consumed(await taps.presence.reset(epoch));
          notReady(view, record.id);
          const marker = await taps.presence.marker(epoch + 1);
          notReady(view, record.id); // replacement snapshot data alone cannot restore readiness
          const changed = view.whenChanged("known-absent", AbortSignal.timeout(5_000));
          marker.release(); await consumed(marker); await boundedReceipt(changed, "A2 replacement ready");
          expect(view.ready).toBe(true);
          expect(view.resolve(record.id), "old valid snapshot must not resurrect after reset").toBeUndefined();
        }
        const replacement = { ...record, capabilities: ["stop"] as FabricParticipantRecord["capabilities"], updatedAt: Date.now() };
        const restored = await pair.sender.kv("presence").put(key, senderLease.owner, replacement, revision, AbortSignal.timeout(5_000));
        await consumed(await taps.presence.put(key, restored));
        // The remote lease may legitimately expire during two 30s heartbeat reset detections.
        // Route identity/capabilities still prove only this new publication restored the row.
        expect(view.resolve(record.id)).toMatchObject({ machine: pair.sender.self.machine,
          ownerIncarnation: record.ownerIncarnation, capabilities: ["stop"] });
        expect(composed.errors).toEqual([]);
      } catch (error) {
        // Failure-only observations; no extra watch, retry, readiness or consumer mutation.
        try {
          console.error("NATS_PRESENCE_RESET_FAILURE", JSON.stringify({ pid: process.pid,
            broker: await pair.kvReadiness?.diagnostics?.(), ready: composed?.view.ready,
            presence: composed?.taps.presence.diagnostics(), leases: composed?.taps.leases.diagnostics(),
            errors: composed?.errors.map(error => String(error)) }));
        } catch (diagnosticError) { console.error("NATS_PRESENCE_RESET_DIAGNOSTIC_ERROR", String(diagnosticError)); }
        throw error;
      } finally {
        try { await composed?.close(); } finally { await pair.close(); }
      }
    }, 230_000);
});

/** Native receipt and /proc are real. Only KV traffic/time are deterministic carrier inputs. */
function protocolLease(kv: TransportKv, options: { onLost?: (error: TransportHostLeaseLostError) => void;
  monotonic?: () => number } = {}) {
  const dir = localDirectory(), clock = { value: 0 }, self = owner("test-a", "session:lease-contract");
  const lease = new TransportHostLease({ kv, self, rootId: "root:lease", identityId: "identity:lease",
    incarnation: "birth-one", localStateDir: dir, monotonic: options.monotonic ?? (() => clock.value),
    ...(options.onLost ? { onLost: options.onLost } : {}) });
  return { dir, clock, self, lease,
    close: () => { clock.value = cutoffMs + 100_000; lease.live(); rmSync(dir, { recursive: true, force: true }); } };
}

describe.skipIf(!linux)("A2 lease PROTOCOL/UNIT + real local /proc and durable birth receipt (not broker partition proof)", () => {
  it("monotonic cutoff ignores wall-clock jumps, aborts view/waiter and calls onLost exactly once", async () => {
    const kv = createStubKv(TRANSPORT_HOST_LEASE_TTL_MS), lost = vi.fn((_error: TransportHostLeaseLostError) => {
      throw new Error("diagnostic callback failure");
    });
    const f = protocolLease(kv, { onLost: lost });
    const presence = createStubKv(600_000);
    const transport: FabricTransport = { kind: "nats", self: f.self, kv: bucket => bucket === "leases" ? kv : presence,
      send: async () => { throw new Error("not used"); }, inbox: async function* () {},
      drainState: async () => ({ pending: 0, ackPending: 0 }), close: async () => {} };
    const view = new KvParticipantDirectory({ transport, lease: f.lease, rootId: "root:lease",
      identityId: "identity:lease", ownerIncarnation: "birth-one" });
    let restoreWall = (): void => {};
    try {
      await f.lease.acquire();
      view.start();
      // whenChanged is registered synchronously before the stub's snapshot markers are applied.
      await boundedReceipt(view.whenChanged("absent", AbortSignal.timeout(5_000)), "actual view initial readiness");
      expect(view.ready).toBe(true);
      const wall = vi.spyOn(Date, "now").mockReturnValue(1);
      restoreWall = () => { wall.mockRestore(); };
      f.clock.value = cutoffMs - 1; expect(f.lease.live()).toBe(true);
      wall.mockReturnValue(9_000_000_000_000); expect(f.lease.live()).toBe(true);
      const waiting = view.whenChanged("absent", AbortSignal.timeout(5_000));
      const rejected = expect(waiting).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_LOST", reason: "timeout" });
      f.clock.value = cutoffMs;
      expect(f.lease.live()).toBe(false); await rejected;
      expect(f.lease.signal.aborted).toBe(true);
      expect(f.lease.signal.reason).toMatchObject({ code: "FABRIC_HOST_LEASE_LOST", reason: "timeout" });
      expect(lost).toHaveBeenCalledTimes(1); expect(lost.mock.calls[0]?.[0]).toBe(f.lease.signal.reason);
      notReady(view, "absent"); expect(view.leaseLive()).toBe(false);
      expect(() => f.lease.renew()).toThrow(f.lease.signal.reason);
      expect(f.lease.live()).toBe(false); expect(lost).toHaveBeenCalledTimes(1);
    } finally {
      restoreWall();
      try { await view.close(); } finally { f.close(); }
    }
  });

  it("local alarm aborts/lost-callback without anyone calling live or renew", async () => {
    const loss = deferred<TransportHostLeaseLostError>();
    let forceCutoff = false;
    const monotonic = () => performance.now() + (forceCutoff ? cutoffMs + 1 : 0);
    const f = protocolLease(createStubKv(TRANSPORT_HOST_LEASE_TTL_MS), { monotonic, onLost: error => loss.resolve(error) });
    try {
      await f.lease.acquire();
      // No live()/renew() calls between acquisition and this actual setTimeout-driven abort.
      const error = await boundedReceipt(loss.promise, "actual unreferenced monotonic cutoff alarm", cutoffMs + 5_000);
      expect(error).toMatchObject({ code: "FABRIC_HOST_LEASE_LOST", reason: "timeout" });
      expect(f.lease.signal.reason).toBe(error); expect(f.lease.signal.aborted).toBe(true);
      expect(f.lease.live()).toBe(false);
    } finally { forceCutoff = true; f.lease.live(); f.close(); }
  }, 25_000);

  it("a PubAck arriving at the OLD cutoff cannot revive the actual lease", async () => {
    const kv = createStubKv(TRANSPORT_HOST_LEASE_TTL_MS), persisted = deferred<void>(), ack = faultGate();
    const lost = vi.fn(), f = protocolLease({ ...kv, put: async (key, writer, value, revision, signal) => {
      const result = await kv.put(key, writer, value, revision, signal);
      if (revision > 0) { persisted.resolve(); await ack.wait(signal!); }
      return result;
    } }, { onLost: lost });
    try {
      await f.lease.acquire(); f.clock.value = 5_000;
      const renewal = f.lease.renew(AbortSignal.timeout(5_000));
      const rejected = expect(renewal).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_LOST", reason: "timeout" });
      await boundedReceipt(persisted.promise, "renew CAS persisted before delayed acknowledgement");
      f.clock.value = cutoffMs; ack.release(); await rejected;
      expect(f.lease.live()).toBe(false); expect(lost).toHaveBeenCalledTimes(1);
      expect(() => f.lease.renew()).toThrow(f.lease.signal.reason);
    } finally { ack.release(); f.close(); }
  });

  it("an uncertain renewal does not extend the last confirmed monotonic window", async () => {
    const kv = createStubKv(TRANSPORT_HOST_LEASE_TTL_MS);
    const f = protocolLease({ ...kv, put: async (key, writer, value, revision, signal) => {
      if (revision > 0) throw new TransportUncertainError(key, value);
      return kv.put(key, writer, value, revision, signal);
    } });
    try {
      await f.lease.acquire(); f.clock.value = 5_000;
      await expect(f.lease.renew()).rejects.toHaveProperty("code", "FABRIC_TRANSPORT_UNCERTAIN");
      f.clock.value = cutoffMs - 1; expect(f.lease.live()).toBe(true);
      f.clock.value = cutoffMs; expect(f.lease.live()).toBe(false);
      expect(f.lease.signal.reason).toHaveProperty("reason", "timeout");
    } finally { f.close(); }
  });

  it("expired KV + disconnected carrier still refuses an authentically live local predecessor BEFORE any KV call", async () => {
    let brokerTime = Date.now();
    const kv = createStubKv(TRANSPORT_HOST_LEASE_TTL_MS, () => brokerTime), f = protocolLease(kv);
    try {
      const value = await f.lease.acquire(), proof = readLocalHostLeaseProcessProof();
      expect(value).toMatchObject(proof);
      expect(transportHostLeasePredecessorExited(proof), "current actual /proc birth must be alive").toBe(false);
      const before = readFileSync(receiptFile(f.dir, f.self.hostId), "utf8");
      expect(receipt(f.dir, f.self.hostId)).toEqual({ ownerHostId: f.self.hostId, incarnation: "birth-one",
        pid: proof.pid, processStartTime: proof.pidStartTime, bootId: proof.bootId,
        acquiredAt: expect.any(Number), releasedCleanly: false });
      brokerTime += TRANSPORT_HOST_LEASE_TTL_MS + 1; kv.expire();
      expect(await kv.get(f.lease.key), "carrier expiry is NOT successor authority").toBeUndefined();
      f.clock.value = cutoffMs; expect(f.lease.live()).toBe(false);
      const offline = vi.fn(async (): Promise<never> => { throw new TransportUnavailableError("carrier disconnected"); });
      const successor = new TransportHostLease({ kv: { ttlMs: kv.ttlMs,
        get: offline, put: offline, delete: offline, watch: kv.watch }, self: f.self,
        rootId: "root:lease", identityId: "identity:lease", incarnation: "birth-two", localStateDir: f.dir });
      await expect(successor.acquire(AbortSignal.timeout(5_000))).rejects.toMatchObject({
        code: "FABRIC_HOST_LEASE_PROOF_REQUIRED", reason: "alive" });
      expect(offline).not.toHaveBeenCalled();
      expect(successor.live()).toBe(false); expect(successor.signal.aborted).toBe(true);
      expect(readFileSync(receiptFile(f.dir, f.self.hostId), "utf8"), "refused successor cannot overwrite birth evidence").toBe(before);
    } finally { f.close(); }
  });

  it("release RECORDED-CALL PROBE: abort/clean durable receipt precede exact owned CAS; NO real delete primitive", async () => {
    const kv = createStubKv(TRANSPORT_HOST_LEASE_TTL_MS), calls: Array<{ key: string; revision: number }> = [];
    let f!: ReturnType<typeof protocolLease>;
    const probe: TransportKv = { ...kv, delete: async (key, revision) => {
      // Deliberately do NOT forward to kv.delete(), NatsKvStore.delete() or any product-delete primitive.
      calls.push({ key, revision });
      expect(f.lease.live()).toBe(false); expect(f.lease.signal.aborted).toBe(true);
      expect(receipt(f.dir, f.self.hostId)).toMatchObject({ incarnation: "birth-one", releasedCleanly: true,
        releasedAt: expect.any(Number) });
      return false; // moved/unknown removal; this probe claims no broker deletion or takeover
    } };
    f = protocolLease(probe);
    try {
      await f.lease.acquire();
      const current = await kv.get(f.lease.key); expect(current).toBeDefined();
      const first = f.lease.release(); expect(f.lease.release()).toBe(first);
      expect(await first).toBe(false);
      expect(calls).toEqual([{ key: f.lease.key, revision: current!.revision }]);
      expect(await kv.get(f.lease.key), "probe never removed the carrier row").toEqual(current);
      expect(() => f.lease.renew()).toThrow();
    } finally { f.close(); }
  });
});
