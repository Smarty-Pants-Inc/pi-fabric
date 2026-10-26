import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeaseCurrent, TransportHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricTransport } from "../src/transport/types.js";
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
    // A wall adjustment alone must not change the admission budget.
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
async function nats(id: string, random: () => number, live?: () => boolean) {
  const { root, mesh } = scratch(), world = kvWorld();
  const listeners = new Set<() => void>();
  const base = world.transport("machine-a", id), leases = base.kv("leases");
  const renewals: number[] = [];
  let renewalDelay = 0;
  const transport: FabricTransport = { ...base,
    onVerifiedReconnect(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    kv(bucket) {
      if (bucket !== "leases") return base.kv(bucket);
      return { ttlMs: leases.ttlMs, get: leases.get.bind(leases), delete: leases.delete.bind(leases), watch: leases.watch.bind(leases),
        async put(key, owner, value, revision, signal) {
          if (revision > 0) {
            renewals.push(Date.now());
            if (renewalDelay) await new Promise<void>(resolve => setTimeout(resolve, renewalDelay));
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
  return { directory, world, renewals,
    reconnect() { for (const listener of listeners) listener(); },
    delayRenewal(ms: number) { renewalDelay = ms; },
    lease() { return world.leases.values.get(hKey("machine-a", id))!; },
    presence() { return world.presence.values.get(pKey("machine-a", id))!; },
  };
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
    f.delayRenewal(5_000);
    await vi.advanceTimersByTimeAsync(10_999);
    expect(f.renewals).toEqual([at + 6_000]);
    expect(f.directory.canConsumeMesh()).toBe(true);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.lease().value).toMatchObject({ updatedAt: at + 6_000, expiresAt: at + 21_000 });
    await vi.advanceTimersByTimeAsync(6_000);
    expect(f.renewals).toEqual([at + 6_000, at + 12_000]);
    expect(f.directory.canConsumeMesh()).toBe(true);
    expect(f.directory.transportLeaseSignal?.aborted).toBe(false);
    f.delayRenewal(0);
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

  it("spreads reconnect renewal and forced presence, coalescing repeats without a heartbeat bypass", async () => {
    clock();
    let sample = 0;
    const fixtures = [];
    for (let i = 0; i < 3; i++) fixtures.push(await nats(`jitter-spread-${i}`, () => sample));
    const at = Date.now(), before = fixtures.map(f => f.presence().revision);
    await vi.advanceTimersByTimeAsync(3_000);
    for (let i = 0; i < fixtures.length; i++) { sample = (i + 1) / 3; fixtures[i]!.reconnect(); }
    await vi.advanceTimersByTimeAsync(1_000); // Original low-bound heartbeat must skip.
    for (const f of fixtures) { f.reconnect(); expect(f.renewals).toEqual([]); }
    for (const [index, f] of fixtures.entries()) expect(f.presence().revision).toBe(before[index]);
    await vi.advanceTimersByTimeAsync(4_000);
    const starts = fixtures.map(f => f.renewals[0]! - at);
    // At age 3s the 5s operation budget and strict slack cap the window at 4,999ms.
    expect(starts).toEqual([4_666, 6_332, 7_999]);
    for (const [index, f] of fixtures.entries()) {
      expect(f.renewals).toHaveLength(1);
      expect(f.presence().revision).toBeGreaterThan(before[index]!);
      expect(f.directory.canConsumeMesh()).toBe(true);
    }
  });

  it("caps an aged reconnect with strict operation slack using monotonic, not wall time", async () => {
    const time = clock();
    const f = await nats("jitter-aged-reconnect", () => 1);
    const at = Date.now(), before = f.presence().revision;
    time.age(5_000); // Lease's old cutoff is now only 8s away; heartbeat has not run.
    time.jumpWall(86_400_000);
    f.delayRenewal(5_000);
    f.reconnect();
    await vi.advanceTimersByTimeAsync(2_000);
    f.reconnect();
    await vi.advanceTimersByTimeAsync(999);
    expect(f.renewals).toEqual([at + 86_400_000 + 2_999]);
    expect(f.presence().revision).toBe(before);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(f.directory.canConsumeMesh()).toBe(true); // Old cutoff is one ms away, not reached.
    expect(f.presence().revision).toBeGreaterThan(before);
    expect(f.renewals).toHaveLength(1);
    // Cadence reset at reconnect start: next renewal is not delayed by the skipped old tick.
    f.delayRenewal(0);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.renewals).toHaveLength(2);
    expect(f.directory.canConsumeMesh()).toBe(true);
  });

  it.each(["close", "suspend"] as const)("cancels a pending reconnect and heartbeat on %s", async action => {
    clock();
    const f = await nats(`jitter-cancel-${action}`, () => 1);
    const before = f.presence().revision;
    f.reconnect();
    if (action === "close") await f.directory.close(); else f.directory.suspend();
    expect(f.directory.heartbeatArmed).toBe(false);
    f.reconnect();
    await vi.advanceTimersByTimeAsync(6_000);
    expect(f.renewals).toEqual([]);
    if (action === "suspend") {
      expect(f.presence().revision).toBe(before);
      f.directory.resume();
      f.directory.resume(); // Exactly one rearmed heartbeat.
      await vi.advanceTimersByTimeAsync(6_000);
      expect(f.renewals).toHaveLength(1);
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
    await vi.advanceTimersByTimeAsync(6_000);
    expect(f.renewals).toEqual([]);
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
  sample = 1;
  await vi.advanceTimersByTimeAsync(799);
  expect(read().updatedAt).toBe(at);
  await vi.advanceTimersByTimeAsync(1);
  await directory.refresh(); // Join the heartbeat's asynchronous native file custody.
  expect(read().updatedAt).toBe(at + 800);
  sample = 0;
  await vi.advanceTimersByTimeAsync(1_199);
  expect(read().updatedAt).toBe(at + 800);
  expect(read().expiresAt).toBeGreaterThan(Date.now());
  await vi.advanceTimersByTimeAsync(1);
  await directory.refresh();
  expect(read().updatedAt).toBe(at + 2_000);
  directory.suspend();
  await vi.advanceTimersByTimeAsync(800);
  expect(read().updatedAt).toBe(at + 2_000);
  expect(directory.heartbeatArmed).toBe(false);
  directory.resume(); directory.resume();
  await vi.advanceTimersByTimeAsync(800);
  await directory.refresh();
  expect(read().updatedAt).toBe(at + 3_600);
  await directory.close();
  await vi.advanceTimersByTimeAsync(2_000);
  expect(directory.heartbeatArmed).toBe(false);
  expect(readHostLeaseCurrent(mesh.root, id)).toBeUndefined();
});
