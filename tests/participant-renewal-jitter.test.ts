import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { KvParticipantDirectory } from "../src/topology/participant-view.js";
import { LIVENESS_POLICY_KEY, readHostLeaseCurrent, TransportHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUnavailableError, type FabricTransport } from "../src/transport/types.js";
import { hKey, kvWorld, pKey } from "./helpers/bridge-nats-kv.js";

// Keep: #6477's synchronized renewals and the admission failure path. Real directory,
// lease, carrier and projection; only the broker and elapsed time are fixtures.
const directories: ParticipantDirectory[] = [], stores: MeshStore[] = [], roots: string[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const store of stores.splice(0)) store.closeState();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function clock() {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const epoch = Date.now();
  let offset = 0;
  vi.spyOn(performance, "now").mockImplementation(() => Date.now() - epoch + offset);
  return {
    // Simulate event-loop starvation without delivering already-armed timers.
    age(ms: number) { offset += ms; },
    // Host sleep can age the broker TTL without advancing the monotonic clock.
    jumpWall(ms: number) { vi.setSystemTime(Date.now() + ms); offset -= ms; },
  };
}
function scratch() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "participant-renewal-jitter-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000, { stateBackend: "file" });
  stores.push(mesh);
  return { root, mesh };
}
function record(id: string): FabricParticipantRecord {
  return { format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
    name: id, status: "idle", runner: "pi", transport: "host", capabilities: ["followUp"],
    controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
}
async function nats(id: string, random: () => number, live?: () => boolean, reconnectOnSubscribe = false) {
  const { root, mesh } = scratch(), world = kvWorld();
  const listeners = new Set<() => void>();
  const base = world.transport("machine-a", id), leases = base.kv("leases"), presenceKv = base.kv("presence");
  const renewals: number[] = [];
  let renewalDelay = 0;
  const renewalPlan: Array<{ delay: number; fail?: boolean }> = [];
  const transport: FabricTransport = { ...base,
    onVerifiedReconnect(listener) {
      listeners.add(listener);
      if (reconnectOnSubscribe) listener();
      return () => { listeners.delete(listener); };
    },
    kv(bucket) {
      if (bucket === "presence") return presenceKv;
      if (bucket !== "leases") return base.kv(bucket);
      return { ttlMs: leases.ttlMs, get: leases.get.bind(leases), delete: leases.delete.bind(leases), watch: leases.watch.bind(leases),
        async put(key, owner, value, revision, signal) {
          if (revision > 0) {
            renewals.push(Date.now());
            const step = renewalPlan.shift();
            const delay = step?.delay ?? renewalDelay;
            if (delay) await new Promise<void>(resolve => setTimeout(resolve, delay));
            if (step?.fail) throw new TransportUnavailableError("injected renewal failure");
          }
          return leases.put(key, owner, value, revision, signal);
        } };
    },
  };
  const transportStateDir = path.join(root, "local");
  fs.mkdirSync(transportStateDir, { mode: 0o700 });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id,
    identity: { id, name: id, kind: "main" }, transport, transportStateDir, random,
    reapDeadHosts: false, ...(live ? { live } : {}) });
  directories.push(directory);
  const rootRecord = record(id);
  directory.registerSource(() => [rootRecord]);
  await directory.start();
  return { directory, world, renewals, rootRecord, presenceKv,
    reconnect() { for (const listener of listeners) listener(); },
    delayRenewal(ms: number) { renewalDelay = ms; },
    planRenewals(...steps: Array<{ delay: number; fail?: boolean }>) { renewalPlan.push(...steps); },
    lease() { return world.leases.values.get(hKey("machine-a", id))!; },
    presence() { return world.presence.values.get(pKey("machine-a", id))!; },
  };
}

async function expectContinuedAdmission(f: Awaited<ReturnType<typeof nats>>) {
  const before = f.renewals.length;
  // Cross several heartbeat turns and the last reconnect renewal's 13s cutoff.
  for (let elapsed = 0; elapsed < 20_000; elapsed += 1_000) {
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(f.directory.participantView?.resolve(f.rootRecord.id)?.live).toBe(true);
    expect(f.directory.transportLeaseSignal?.aborted).toBe(false);
    expect(f.directory.heartbeatArmed).toBe(true);
  }
  expect(f.renewals.length).toBeGreaterThanOrEqual(before + 3);
  // Immediate fixture PubAcks: these request-start gaps also bound every lease age,
  // not just the sampled admission checks above. No cutoff/recovery gap is allowed.
  for (let i = 1; i < f.renewals.length; i++) {
    expect(f.renewals[i]! - f.renewals[i - 1]!).toBeLessThanOrEqual(6_100);
  }
}

// Native local successor proof requires /proc. FILE coverage runs on every platform.
describe.skipIf(process.platform !== "linux")("NATS directory renewal jitter", () => {
  it("samples the low and high heartbeat bounds anew on every arm", async () => {
    clock();
    let sample = 0;
    const f = await nats("jitter-bounds", () => sample);
    const at = Date.now();
    sample = 1;
    await vi.advanceTimersByTimeAsync(3_999);
    expect(f.renewals).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.renewals).toEqual([at + 4_000]);
    sample = 0;
    await vi.advanceTimersByTimeAsync(5_999);
    expect(f.renewals).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.renewals).toEqual([at + 4_000, at + 10_000]);
    await vi.advanceTimersByTimeAsync(4_000);
    expect(f.renewals).toEqual([at + 4_000, at + 10_000, at + 14_000]);
    expect(f.directory.canConsumeMesh()).toBe(true);
  });

  it("max jitter plus a full 5s PubAck stays inside the 13s admission cutoff", async () => {
    clock();
    const f = await nats("jitter-slow-heartbeat", () => 1);
    const at = Date.now();
    f.planRenewals({ delay: 5_000 }, { delay: 5_000 });
    await vi.advanceTimersByTimeAsync(10_999);
    expect(f.renewals).toEqual([at + 6_000]);
    expect(f.directory.canConsumeMesh()).toBe(true);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.lease().value).toMatchObject({ updatedAt: at + 6_000, expiresAt: at + 21_000 });
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.renewals).toEqual([at + 6_000, at + 11_000, at + 16_000]);
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(f.directory.transportLeaseSignal?.aborted).toBe(false);
  });

  it("keeps a coalesced low-bound tick when the next arm samples high and PubAck takes 5s", async () => {
    clock();
    let sample = 0;
    const f = await nats("jitter-coalesced-heartbeat", () => sample);
    const at = Date.now();
    f.delayRenewal(5_000);
    await vi.advanceTimersByTimeAsync(4_000);
    expect(f.renewals).toEqual([at + 4_000]);
    sample = 1;
    await vi.advanceTimersByTimeAsync(4_000); // Joins the first renewal; next tick is not until 14s.
    expect(f.renewals).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.renewals).toEqual([at + 4_000, at + 9_000]);
    expect(f.directory.canConsumeMesh()).toBe(true);
    f.delayRenewal(0);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(f.directory.transportLeaseSignal?.aborted).toBe(false);
  });

  it("spreads reconnect renewal and forced presence inside the full-retry reserve, coalescing repeats", async () => {
    clock();
    let sample = 0;
    const fixtures = [];
    for (let i = 0; i < 3; i++) fixtures.push(await nats(`jitter-spread-${i}`, () => sample));
    const at = Date.now(), before = fixtures.map(f => f.presence().revision);
    await vi.advanceTimersByTimeAsync(1_000);
    for (let i = 0; i < fixtures.length; i++) { sample = (i + 1) / 3; fixtures[i]!.reconnect(); }
    for (const f of fixtures) { f.reconnect(); expect(f.renewals).toEqual([]); }
    for (const [index, f] of fixtures.entries()) expect(f.presence().revision).toBe(before[index]);
    await vi.advanceTimersByTimeAsync(1_999);
    const starts = fixtures.map(f => f.renewals[0]! - at);
    // At age 1s, two full 5s operations and strict slack leave a 1,999ms window.
    expect(starts).toEqual([1_666, 2_332, 2_999]);
    for (const [index, f] of fixtures.entries()) {
      expect(f.renewals).toHaveLength(1);
      expect(f.presence().revision).toBeGreaterThan(before[index]!);
      expect(f.directory.canConsumeMesh()).toBe(true);
    }
  });

  it("keeps the original reconnect presence deadline across suspend/resume (F1)", async () => {
    clock();
    let draws = 0;
    const f = await nats("jitter-resume-deadline", () => { draws++; return 1; });
    const at = Date.now(), before = f.presence().revision;
    f.reconnect(); // Samples the maximum 2,999ms deadline, reserving one full retry.
    await vi.advanceTimersByTimeAsync(1_000);
    f.directory.suspend();
    await vi.advanceTimersByTimeAsync(1_000);
    const pausedDraws = draws;
    f.directory.resume();
    f.directory.resume();
    expect(draws).toBe(pausedDraws + 1); // Heartbeat only, never resample reconnect.
    f.directory.scheduleRefresh();
    await vi.advanceTimersByTimeAsync(500); // Ordinary refresh must not force presence before the original due time.
    expect(f.presence().revision).toBe(before);
    expect(f.renewals).toEqual([]);
    await vi.advanceTimersByTimeAsync(498);
    expect(f.presence().revision).toBe(before);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.renewals).toEqual([at + 2_999]);
    expect(f.presence().revision).toBeGreaterThan(before);
  });

  it("renews after suspension immediately following a successful reconnect commit (F2)", async () => {
    clock();
    const f = await nats("jitter-postcommit-suspend", () => 1);
    const at = Date.now(), before = f.presence().revision;
    let suspendAfterCommit = true;
    f.directory.options.preparePublicationFence = () => () => true;
    f.directory.options.withPublicationFence = async publish => {
      const result = await publish();
      if (suspendAfterCommit) {
        suspendAfterCommit = false;
        f.directory.suspend(); // Confirmed, but the reconnect runner has not reached finally.
      }
      return result;
    };
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_999);
    expect(f.directory.heartbeatArmed).toBe(false);
    expect(f.renewals).toEqual([at + 2_999]);
    expect(f.presence().revision).toBeGreaterThan(before);
    const confirmed = f.presence().revision;
    await vi.advanceTimersByTimeAsync(100);
    expect(f.directory.participantView?.resolve(f.rootRecord.id)?.live).toBe(true);
    f.directory.resume();
    expect(f.directory.canConsumeMesh()).toBe(true);
    await vi.advanceTimersByTimeAsync(6_000);
    expect(f.renewals).toEqual([at + 2_999, at + 9_099]);
    expect(f.presence().revision).toBe(confirmed); // No outstanding forced refresh.
    await expectContinuedAdmission(f);
  });

  it.each([
    "before preparation", "before forced commit", "during forced commit",
    "after commit before finally", "after runner cleanup",
  ] as const)("keeps the lease admitted across short suspend/resume %s", async phase => {
    clock();
    const f = await nats(`jitter-phase-${phase.replaceAll(" ", "-")}`, () => 1);
    const before = f.presence().revision;
    let entered = false, release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const pause = async () => {
      if (entered) return;
      entered = true;
      f.directory.suspend();
      await gate;
    };
    f.directory.options.preparePublicationFence = () => () => true;
    if (phase === "before preparation") f.directory.options.beforePublication = pause;
    f.directory.options.withPublicationFence = async publish => {
      if (phase === "before forced commit") await pause();
      const result = await publish();
      if (phase === "after commit before finally") await pause();
      return result;
    };
    if (phase === "during forced commit") {
      const put = f.presenceKv.put.bind(f.presenceKv);
      vi.spyOn(f.presenceKv, "put").mockImplementationOnce(async (...args) => {
        await pause(); // CAS is in flight; the old token must not confirm the replacement.
        return put(...args);
      });
    }
    try {
      f.reconnect();
      await vi.advanceTimersByTimeAsync(2_999);
      if (phase === "after runner cleanup") {
        entered = true; // Timer advancement has drained the reconnect runner's microtasks.
        f.directory.suspend();
      }
      expect(entered).toBe(true);
      expect(f.directory.heartbeatArmed).toBe(false);
      expect(f.directory.canConsumeMesh()).toBe(false); // Lifecycle hold, not a lease lapse.
      await vi.advanceTimersByTimeAsync(100);
      expect(f.directory.participantView?.resolve(f.rootRecord.id)?.live).toBe(true);
      expect(f.directory.transportLeaseSignal?.aborted).toBe(false);
      f.directory.resume();
      f.directory.resume();
      expect(f.directory.canConsumeMesh()).toBe(true);
      release();
      await vi.advanceTimersByTimeAsync(0);
      expect(f.presence().revision).toBeGreaterThan(before);
      const confirmed = f.presence().revision;
      f.directory.scheduleRefresh();
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.presence().revision).toBe(confirmed); // Cancelled continuations left no false generation.
      await expectContinuedAdmission(f);
    } finally {
      release();
      await vi.advanceTimersByTimeAsync(0);
    }
  });

  it("retains a new reconnect received after the old commit but before suspension", async () => {
    clock();
    const f = await nats("jitter-postcommit-new-generation", () => 1);
    const at = Date.now(), before = f.presence().revision;
    let reconnectAfterCommit = true;
    f.directory.options.preparePublicationFence = () => () => true;
    f.directory.options.withPublicationFence = async publish => {
      const result = await publish();
      if (reconnectAfterCommit) {
        reconnectAfterCommit = false;
        f.reconnect(); // New evidence is NOT covered by the commit that just succeeded.
        f.directory.suspend();
      }
      return result;
    };
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_999);
    expect(f.presence().revision).toBe(before + 1);
    expect(f.renewals).toEqual([at + 2_999]);
    await vi.advanceTimersByTimeAsync(100);
    f.directory.resume();
    await vi.advanceTimersByTimeAsync(0); // Retained elapsed deadline: do not sample a new wait.
    expect(f.renewals).toEqual([at + 2_999, at + 3_099]);
    expect(f.presence().revision).toBe(before + 2); // The new generation got its own forced refresh.
    f.directory.scheduleRefresh();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.presence().revision).toBe(before + 2);
    await expectContinuedAdmission(f);
  });

  it("retains a reconnect received during suspension without publishing before its sampled due time", async () => {
    clock();
    const f = await nats("jitter-suspended-receiver", () => 1);
    const at = Date.now(), before = f.presence().revision;
    f.directory.suspend();
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_000);
    f.directory.resume();
    await vi.advanceTimersByTimeAsync(998);
    expect(f.renewals).toEqual([]);
    expect(f.presence().revision).toBe(before);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.renewals).toEqual([at + 2_999]);
    expect(f.presence().revision).toBeGreaterThan(before);
  });

  it("retains bootstrap reconnect evidence until the lease and view are ready", async () => {
    clock();
    const f = await nats("jitter-bootstrap-receiver", () => 1, undefined, true);
    const at = Date.now(), before = f.presence().revision;
    await vi.advanceTimersByTimeAsync(2_998);
    expect(f.renewals).toEqual([]);
    expect(f.presence().revision).toBe(before);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.renewals).toEqual([at + 2_999]);
    expect(f.presence().revision).toBeGreaterThan(before);
  });

  it("publishes genuine changes during a reconnect wait without confirming forced refresh early", async () => {
    clock();
    const f = await nats("jitter-changed-during-wait", () => 1);
    const before = f.presence().revision;
    f.reconnect();
    f.rootRecord.status = "working";
    f.directory.scheduleRefresh();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.presence().revision).toBeGreaterThan(before);
    expect(f.presence().value).toMatchObject({ status: "working" });
    const changed = f.presence().revision;
    await vi.advanceTimersByTimeAsync(2_998);
    expect(f.presence().revision).toBe(changed);
    expect(f.renewals).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.presence().revision).toBeGreaterThan(changed);
    expect(f.renewals).toHaveLength(1);
  });

  it("does not attribute a reconnect received during forced publication to the older generation", async () => {
    clock();
    const f = await nats("jitter-publication-generation", () => 1);
    const before = f.presence().revision;
    const put = f.world.presence.put.bind(f.world.presence);
    const writes = vi.spyOn(f.world.presence, "put").mockImplementationOnce((...args) => {
      f.reconnect(); // Commit already captured the first generation, not this second one.
      return put(...args);
    });
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_999);
    expect(writes).toHaveBeenCalledTimes(2);
    expect(f.presence().revision).toBe(before + 2);
    expect(f.renewals).toHaveLength(1); // Shared reconnect work, not another sampled renewal.
    f.directory.scheduleRefresh();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(writes).toHaveBeenCalledTimes(2);
  });

  it("arms retained reconnect evidence when an unsuspended owner becomes live again", async () => {
    clock();
    let live = true;
    const f = await nats("jitter-receiver-retired", () => 1, () => live);
    const before = f.presence().revision;
    live = false;
    f.reconnect();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.presence().revision).toBe(before);
    live = true;
    await vi.advanceTimersByTimeAsync(1_000); // Existing heartbeat observes the rebound owner.
    expect(f.renewals).toHaveLength(1);
    expect(f.presence().revision).toBeGreaterThan(before);
  });

  it("fences a cancelled async reconnect preparation from replacement work", async () => {
    clock();
    const f = await nats("jitter-cancelled-continuation", () => 1);
    const at = Date.now(), before = f.presence().revision;
    let release!: () => void, entered = false;
    const gate = new Promise<void>(resolve => { release = resolve; });
    f.directory.options.beforePublication = async () => {
      if (!entered) { entered = true; await gate; }
    };
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_999);
    expect(entered).toBe(true);
    expect(f.presence().revision).toBe(before);
    f.directory.suspend();
    f.delayRenewal(1_000);
    f.directory.resume(); // Same elapsed deadline, fresh token; renewal still in flight.
    release();
    await vi.advanceTimersByTimeAsync(0);
    f.directory.scheduleRefresh();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.presence().revision).toBe(before);
    expect(f.renewals).toEqual([at + 2_999, at + 2_999]);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.presence().revision).toBeGreaterThan(before);
    const confirmed = f.presence().revision;
    f.directory.scheduleRefresh();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.presence().revision).toBe(confirmed); // Old continuation did not leave a generation outstanding.
    expect(f.directory.canConsumeMesh()).toBe(true);
    f.delayRenewal(0);
  });

  it("resumes ordinary changed publication immediately without a reconnect", async () => {
    clock();
    const f = await nats("jitter-resume-normal", () => 1);
    const before = f.presence().revision;
    f.directory.suspend();
    f.rootRecord.status = "working";
    f.directory.resume();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.presence().revision).toBeGreaterThan(before);
    expect(f.presence().value).toMatchObject({ status: "working" });
    expect(f.renewals).toEqual([]);
  });

  it("does not resample a reconnect whose original due time elapsed while suspended", async () => {
    clock();
    const f = await nats("jitter-resume-overdue", () => 1);
    const at = Date.now(), before = f.presence().revision;
    f.reconnect();
    f.directory.suspend();
    await vi.advanceTimersByTimeAsync(5_500);
    f.directory.resume();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.renewals).toEqual([at + 5_500]);
    expect(f.presence().revision).toBeGreaterThan(before);
  });

  it("shortens a retained reconnect deadline by the aged full-retry budget on resume", async () => {
    const time = clock();
    const f = await nats("jitter-resume-cap", () => 1);
    const at = Date.now(), before = f.presence().revision;
    time.age(1_000); // Original deadline: now + 1,999ms, preserving two full 5s operations.
    f.reconnect();
    f.directory.suspend();
    time.age(1_000); // Resume has at most 999ms before the full-retry reserve.
    time.jumpWall(-86_400_000); // A backwards wall adjustment cannot extend the monotonic cap.
    f.planRenewals({ delay: 5_000 });
    f.directory.resume();
    await vi.advanceTimersByTimeAsync(998);
    expect(f.renewals).toEqual([]);
    expect(f.presence().revision).toBe(before);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.renewals).toEqual([at - 86_400_000 + 999]);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(f.presence().revision).toBeGreaterThan(before);
  });

  it.each(["monotonic", "wall"] as const)("caps an aged reconnect by the %s clock with full-retry slack", async kind => {
    const time = clock();
    const f = await nats(`jitter-aged-reconnect-${kind}`, () => 1);
    const at = Date.now(), before = f.presence().revision;
    if (kind === "monotonic") { time.age(2_000); time.jumpWall(-86_400_000); }
    else time.jumpWall(2_000); // Sleep: monotonic did not advance, but the broker's TTL did.
    const wallOffset = kind === "monotonic" ? -86_400_000 : 2_000;
    f.planRenewals({ delay: 5_000 });
    f.reconnect();
    await vi.advanceTimersByTimeAsync(998);
    f.reconnect();
    expect(f.renewals).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.renewals).toEqual([at + wallOffset + 999]);
    expect(f.presence().revision).toBe(before);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(f.presence().revision).toBeGreaterThan(before);
    expect(f.renewals).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.renewals).toHaveLength(2);
    expect(f.directory.canConsumeMesh()).toBe(true);
  });

  it.each(["close", "suspend"] as const)("cancels a pending reconnect and heartbeat on %s", async action => {
    clock();
    const f = await nats(`jitter-cancel-${action}`, () => 1);
    const before = f.presence().revision;
    f.reconnect();
    if (action === "close") {
      await f.directory.close();
      f.directory.suspend();
      f.directory.resume(); // A terminal close cannot be rebound into another heartbeat.
    } else f.directory.suspend();
    expect(f.directory.heartbeatArmed).toBe(false);
    f.reconnect();
    await vi.advanceTimersByTimeAsync(6_000);
    expect(f.renewals).toEqual([]);
    if (action === "suspend") {
      expect(f.presence().revision).toBe(before);
      f.directory.resume();
      f.directory.resume(); // Exactly one rearmed heartbeat; elapsed reconnect is immediately eligible.
      await vi.advanceTimersByTimeAsync(0);
      expect(f.renewals).toHaveLength(1);
      expect(f.presence().revision).toBeGreaterThan(before);
      await vi.advanceTimersByTimeAsync(5_999);
      expect(f.renewals).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(f.renewals).toHaveLength(2);
      expect(f.directory.canConsumeMesh()).toBe(true);
    } else expect(f.world.presence.values.size).toBe(0);
  });

  it.each([8_000, 13_000])("does not delay a near-cutoff/expired lease at age %ims", async age => {
    const time = clock();
    const f = await nats("jitter-expired-reconnect", () => 1);
    const at = Date.now();
    time.age(age);
    expect(f.directory.canConsumeMesh()).toBe(age < 13_000);
    f.reconnect();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.renewals).toEqual([at]);
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(f.directory.participantView?.resolve("jitter-expired-reconnect")?.live).toBe(true);
  });

  it("cancels a pending reconnect when the lease cutoff suspends admission", async () => {
    const time = clock();
    const f = await nats("jitter-cutoff-cancels", () => 1);
    f.reconnect();
    time.age(13_000);
    expect(f.directory.canConsumeMesh()).toBe(false); // Triggers onSuspended and cancels the timer.
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.renewals).toEqual([]);
    await vi.advanceTimersByTimeAsync(1_000); // Independent normal heartbeat recovers the lease.
    expect(f.renewals).toHaveLength(1);
    expect(f.directory.canConsumeMesh()).toBe(true);
  });

  it("cancels pending reconnect and heartbeat timers on terminal lease loss", async () => {
    const time = clock();
    const f = await nats("jitter-terminal-loss", () => 1);
    f.reconnect();
    time.age(-1); // A backwards monotonic clock is a terminal ownership loss, not recovery.
    expect(f.directory.canConsumeMesh()).toBe(false);
    expect(f.directory.transportLeaseSignal?.aborted).toBe(true);
    expect(f.directory.heartbeatArmed).toBe(false);
    f.directory.suspend();
    f.directory.resume();
    f.reconnect();
    await vi.advanceTimersByTimeAsync(6_000);
    expect(f.directory.heartbeatArmed).toBe(false);
    expect(f.renewals).toEqual([]);
  });

  it("keeps admission after a fast heartbeat failure followed by a full 5s retry", async () => {
    clock();
    const f = await nats("jitter-failed-heartbeat", () => 1);
    const at = Date.now();
    await vi.advanceTimersByTimeAsync(6_500);
    f.planRenewals({ delay: 0, fail: true }, { delay: 5_000 });
    for (let elapsed = 0; elapsed < 20_000; elapsed += 100) {
      await vi.advanceTimersByTimeAsync(100);
      expect(f.directory.canConsumeMesh()).toBe(true);
      expect(f.directory.participantView?.resolve(f.rootRecord.id)?.live).toBe(true);
      expect(f.directory.transportLeaseSignal?.aborted).toBe(false);
      expect(f.directory.heartbeatArmed).toBe(true);
    }
    expect(f.renewals.slice(0, 3)).toEqual([at + 6_000, at + 12_000, at + 13_000]);
    expect(f.lease().value).toMatchObject({ id: f.rootRecord.id });
  });

  it("retries a full 5s failed reconnect with a full 5s success strictly before 13s, with heartbeats pending", async () => {
    clock();
    const f = await nats("jitter-full-reconnect-retry", () => 1);
    const at = Date.now(), before = f.presence().revision;
    f.planRenewals({ delay: 5_000, fail: true }, { delay: 5_000 });
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_999);
    expect(f.renewals).toEqual([at + 2_999]);
    for (let elapsed = 0; elapsed < 10_000; elapsed += 100) {
      await vi.advanceTimersByTimeAsync(100);
      expect(f.directory.canConsumeMesh()).toBe(true);
      expect(f.directory.participantView?.resolve(f.rootRecord.id)?.live).toBe(true);
      expect(f.directory.transportLeaseSignal?.aborted).toBe(false);
      expect(f.directory.heartbeatArmed).toBe(true);
      if (elapsed < 4_900) expect(f.presence().revision).toBe(before);
    }
    expect(Date.now() - at).toBe(12_999); // A late reacquisition after a lapse is NOT success.
    expect(f.renewals.slice(0, 2)).toEqual([at + 2_999, at + 7_999]);
    expect(f.lease().value).toMatchObject({ updatedAt: expect.any(Number) });
    expect((f.lease().value as { updatedAt: number }).updatedAt).toBeGreaterThanOrEqual(at + 7_999);
    expect(f.presence().revision).toBeGreaterThan(before);
    await expectContinuedAdmission(f);
  });

  it("lets heartbeats and genuine changes proceed during reconnect renewal without forcing presence early or racing its owner", async () => {
    clock();
    const f = await nats("jitter-pending-reconnect-publication", () => 1);
    const view = f.directory.participantView as KvParticipantDirectory;
    const publish = view.publish.bind(view), receipts: Promise<unknown>[] = [], forcedAt: number[] = [];
    vi.spyOn(view, "publish").mockImplementation((rec, options) => {
      if (options?.force) forcedAt.push(Date.now());
      const receipt = publish(rec, options);
      receipts.push(receipt);
      return receipt;
    });
    const at = Date.now(), before = f.presence().revision;
    f.planRenewals({ delay: 5_000 });
    f.reconnect();
    await vi.advanceTimersByTimeAsync(3_500);
    f.rootRecord.status = "working";
    f.directory.scheduleRefresh();
    await vi.advanceTimersByTimeAsync(0);
    expect(f.presence().revision).toBeGreaterThan(before);
    expect(f.presence().value).toMatchObject({ status: "working" });
    const changed = f.presence().revision;
    await vi.advanceTimersByTimeAsync(2_500); // The ordinary heartbeat at 6s joins the pending renewal.
    expect(f.directory.heartbeatArmed).toBe(true);
    expect(f.renewals).toEqual([at + 2_999]);
    expect(forcedAt).toEqual([]);
    expect(f.presence().revision).toBe(changed);
    await vi.advanceTimersByTimeAsync(1_999);
    await Promise.all(receipts); // Surface every actual publication error, including the foreign-value refusal.
    expect(f.renewals).toEqual([at + 2_999, at + 7_999]);
    expect(forcedAt).toEqual([at + 7_999]);
    expect(f.presence().revision).toBeGreaterThan(changed);
    expect(f.presence().value).toMatchObject({ status: "working", ownerIncarnation: f.directory.ownerIncarnation });
    await expectContinuedAdmission(f);
  });

  it("bounds retries after repeated fast failures even once the lease budget is exhausted, and close never rearms", async () => {
    clock();
    const f = await nats("jitter-exhausted-retry", () => 1);
    f.planRenewals(...Array.from({ length: 100 }, () => ({ delay: 0, fail: true })));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(f.renewals.length).toBeGreaterThan(10);
    expect(f.renewals.length).toBeLessThanOrEqual(30); // One bounded recovery turn/second, never a 1ms hot loop.
    expect(f.directory.canConsumeMesh()).toBe(false);
    await f.directory.close();
    const attempts = f.renewals.length;
    f.reconnect(); f.directory.suspend(); f.directory.resume();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.renewals).toHaveLength(attempts);
    expect(f.directory.heartbeatArmed).toBe(false);
  });

  it.each(["close", "suspend"] as const)("does not rearm when an in-flight reconnect renewal fails after %s", async action => {
    clock();
    const f = await nats(`jitter-late-failure-${action}`, () => 1);
    const at = Date.now(), before = f.presence().revision;
    f.planRenewals({ delay: 5_000, fail: true });
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_999);
    const closing = action === "close" ? f.directory.close() : undefined;
    if (action === "suspend") f.directory.suspend();
    await vi.advanceTimersByTimeAsync(5_000);
    await closing;
    expect(f.directory.heartbeatArmed).toBe(false);
    expect(f.renewals).toEqual([at + 2_999]);
    if (action === "suspend") {
      expect(f.presence().revision).toBe(before);
      f.directory.resume();
      await vi.advanceTimersByTimeAsync(0);
      expect(f.renewals).toEqual([at + 2_999, at + 7_999]);
      expect(f.presence().revision).toBeGreaterThan(before);
      await expectContinuedAdmission(f);
    } else {
      f.directory.resume(); f.reconnect();
      await vi.advanceTimersByTimeAsync(10_000);
      expect(f.directory.heartbeatArmed).toBe(false);
      expect(f.renewals).toEqual([at + 2_999]);
    }
  });

  it("exposes only the nonnegative existing monotonic admission budget", async () => {
    const { root } = scratch(), world = kvWorld();
    fs.mkdirSync(path.join(root, "receipt"), { mode: 0o700 });
    let mono = 100;
    const lease = new TransportHostLease({ kv: world.transport("machine-a", "getter").kv("leases"),
      self: { machine: "machine-a", hostId: "getter" }, rootId: "getter", identityId: "getter",
      incarnation: "getter-incarnation", localStateDir: path.join(root, "receipt"), monotonic: () => mono });
    try {
      expect(lease.remainingMs()).toBe(0);
      await lease.acquire();
      expect(lease.remainingMs()).toBe(13_000);
      mono += 12_999;
      expect(lease.remainingMs()).toBe(1);
      mono++;
      expect(lease.remainingMs()).toBe(0);
      expect(lease.live()).toBe(false);
      expect(lease.recovering()).toBe(true);
    } finally { await lease.release(); }
    expect(lease.remainingMs()).toBe(0);
  });
});

it.each(["native", "legacy", "files-policy"] as const)("FILE timer preserves half-lease compatibility and idle semantics (%s)", async mode => {
  clock();
  const { mesh } = scratch(), id = `file-half-${mode}`;
  const identity = { id, name: id, kind: "main" as const }, at = Date.now();
  const hostKey = (hostId: string) => "topology/hosts/" + createHash("sha256").update(hostId).digest("hex");
  if (mode !== "native") {
    const peer = { id: "state-only", name: "state-only", kind: "main" as const };
    await mesh.put({ key: hostKey(peer.id), identity: peer, value: { format: 1, id: peer.id, rootId: peer.id,
      identity: peer, startedAt: at, updatedAt: at, expiresAt: at + 60_000 } });
  }
  if (mode === "files-policy") await mesh.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, hostLeases: "files" } });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id, identity,
    heartbeatMs: 1_000, leaseMs: 15_000, random: () => 0.5, reapDeadHosts: false });
  directories.push(directory);
  const root = record(id);
  directory.registerSource(() => [root]);
  await directory.start();
  const before = mesh.get(hostKey(id), { fresh: true })!;
  const heartbeat = vi.spyOn(directory, "refresh");
  for (let tick = 1; tick <= 8; tick++) {
    await vi.advanceTimersByTimeAsync(1_000);
    expect(heartbeat).toHaveBeenCalledTimes(tick);
    await heartbeat.mock.results[tick - 1]!.value; // Actual timer's receipt, never an explicit refresh.
    expect(readHostLeaseCurrent(mesh.root, id)).toMatchObject({ updatedAt: at + tick * 1_000, expiresAt: at + tick * 1_000 + 15_000 });
    const shared = mesh.get(hostKey(id), { fresh: true })!;
    if (mode === "legacy" && tick === 8) {
      expect(shared.version).toBeGreaterThan(before.version); // First tick after the unchanged 7.5s half-life.
      expect(shared.value).toMatchObject({ updatedAt: at + 8_000, expiresAt: at + 23_000 });
    } else expect(shared).toEqual(before); // No early fallback write, nor native/policy idle churn.
  }
});

it("FILE heartbeats resample both bounds, renew before lease expiry, and cancel on suspend/close", async () => {
  clock();
  const { mesh } = scratch(), id = "file-jitter";
  let sample = 0;
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: id, rootId: id,
    identity: { id, name: id, kind: "main" }, heartbeatMs: 1_000, leaseMs: 2_000,
    random: () => sample, reapDeadHosts: false });
  directories.push(directory);
  const root = record(id);
  directory.registerSource(() => [root]);
  await directory.start();
  const at = Date.now();
  const read = () => readHostLeaseCurrent(mesh.root, id)!;
  // Observe and join ONLY refreshes actually invoked by the heartbeat timer. Calling
  // refresh() here would manufacture the very lease write this regression must prove.
  const heartbeat = vi.spyOn(directory, "refresh");
  sample = 1;
  await vi.advanceTimersByTimeAsync(799);
  expect(read().updatedAt).toBe(at);
  await vi.advanceTimersByTimeAsync(1);
  expect(heartbeat).toHaveBeenCalledTimes(1);
  await heartbeat.mock.results[0]!.value;
  expect(read().updatedAt).toBe(at + 800);
  sample = 0;
  await vi.advanceTimersByTimeAsync(1_199);
  expect(read().updatedAt).toBe(at + 800);
  expect(read().expiresAt).toBeGreaterThan(Date.now());
  await vi.advanceTimersByTimeAsync(1);
  expect(heartbeat).toHaveBeenCalledTimes(2);
  await heartbeat.mock.results[1]!.value;
  expect(read().updatedAt).toBe(at + 2_000);
  directory.suspend();
  await vi.advanceTimersByTimeAsync(800);
  expect(read().updatedAt).toBe(at + 2_000);
  expect(directory.heartbeatArmed).toBe(false);
  directory.resume(); directory.resume();
  await vi.advanceTimersByTimeAsync(800);
  expect(heartbeat).toHaveBeenCalledTimes(3);
  const resumedHeartbeat = heartbeat.mock.results.at(-1)!;
  await resumedHeartbeat.value;
  expect(read().updatedAt).toBe(at + 3_600);
  await directory.close();
  await vi.advanceTimersByTimeAsync(2_000);
  expect(directory.heartbeatArmed).toBe(false);
  expect(readHostLeaseCurrent(mesh.root, id)).toBeUndefined();
});
