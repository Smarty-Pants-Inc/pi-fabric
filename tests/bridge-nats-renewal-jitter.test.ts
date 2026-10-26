// Keep: lease-freshness budgets and fence/in-flight timing cannot be reached cheaply in a user flow.
// Mirror jitter is intentionally deferred: at the default 5 s per-operation timeout, #put's
// put/get/put recovery budget already exceeds the 15 s TTL minus the existing 2 s safety margin.
// These tests use the real mirror/reader over fake NATS KV, not a replacement renewal algorithm.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { NatsKvMirror, NatsPresenceReader } from "../src/mesh/bridge-nats-presence.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUncertainError } from "../src/transport/types.js";
import { hKey, kvWorld } from "./helpers/bridge-nats-kv.js";

const MACHINE = "bridge-machine";
const open: Array<{ mirror: NatsKvMirror; reader: NatsPresenceReader }> = [];
const delay = (ms: number) => new Promise<void>(resolve => { setTimeout(resolve, ms); });

beforeEach(() => { vi.useFakeTimers({ now: 1_000_000 }); });
afterEach(async () => {
  for (const { mirror, reader } of open.splice(0)) { await mirror.fence(); await reader.close(); }
  vi.restoreAllMocks();
  vi.useRealTimers();
});

function record(id: string): FabricParticipantRecord {
  return { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    ownerIncarnation: "inc-1", name: id, status: "running", runner: "pi", transport: "host",
    capabilities: ["steer"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
}

async function fixture() {
  const world = kvWorld(), base = world.transport(MACHINE, "bridge-host");
  const leases = base.kv("leases"), presence = base.kv("presence");
  const transport = { ...base, kv: (bucket: "presence" | "leases") => bucket === "leases" ? leases : presence };
  const reader = new NatsPresenceReader({ transport, now: Date.now, monotonic: Date.now });
  const mirror = new NatsKvMirror({ transport, reader, now: Date.now, monotonic: Date.now,
    processProof: { pid: 42, pidStartTime: "420", bootId: "test-boot" } });
  open.push({ mirror, reader });
  reader.start();
  await reader.whenReady();
  const result = await mirror.replace("roots", ["session:root-a", "session:root-b"].map(id => ({ record: record(id), until: Infinity })), true);
  expect(result.refused).toEqual([]);
  return { world, mirror, reader, leases };
}

describe("NATS mirror renewal jitter safety exception", () => {
  it("retains the existing renewal cadence and fence cancels the pending pass timer", async () => {
    const f = await fixture();
    const initial = [...f.world.writes];
    await vi.advanceTimersByTimeAsync(4_999);
    expect(f.world.writes).toEqual(initial);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.world.leases.values.get(hKey(MACHINE, "session:root-a"))?.value).toMatchObject({ updatedAt: Date.now(), expiresAt: Date.now() + 15_000 });
    expect(f.mirror.holds("session:root-a")).toBe(true);
    expect(f.mirror.holds("session:root-b")).toBe(true);
    await f.mirror.fence();
    const writes = [...f.world.writes];
    await vi.advanceTimersByTimeAsync(30_000);
    expect(f.world.writes).toEqual(writes);
    // No artificial extension of authority after fencing.
    expect(f.reader.resolve("session:root-a")?.live).toBe(false);
  });

  it("coalesces passes and fence joins already-started writes without scheduling another pass", async () => {
    const f = await fixture();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const put = f.leases.put.bind(f.leases);
    let started = 0;
    vi.spyOn(f.leases, "put").mockImplementation(async (...args) => { started++; await gate; return put(...args); });
    const first = f.mirror.renew();
    expect(f.mirror.renew()).toBe(first);
    // Drain the per-key serial queues, but do not advance any timer.
    for (let turn = 0; turn < 10 && started !== 2; turn++) await Promise.resolve();
    expect(started).toBe(2);
    let fenced = false;
    const fencing = f.mirror.fence().then(() => { fenced = true; });
    await Promise.resolve();
    expect(fenced).toBe(false);
    release();
    await first;
    await fencing;
    const writes = [...f.world.writes];
    await vi.advanceTimersByTimeAsync(30_000);
    expect(f.world.writes).toEqual(writes);
    expect(started).toBe(2);
  });

  it("demonstrates why the existing default recovery budget cannot safely accept any additional jitter", async () => {
    const f = await fixture();
    const target = hKey(MACHINE, "session:root-a");
    const put = f.leases.put.bind(f.leases), get = f.leases.get.bind(f.leases);
    const started: number[] = [];
    let first = true;
    vi.spyOn(f.leases, "put").mockImplementation(async (...args) => {
      if (args[0] !== target) return put(...args);
      started.push(Date.now());
      await delay(4_999); // Each operation is individually inside the existing 5 s timeout.
      if (first) { first = false; throw new TransportUncertainError(target, {}); }
      return put(...args);
    });
    vi.spyOn(f.leases, "get").mockImplementation(async key => {
      if (key === target) await delay(4_999);
      return get(key);
    });
    // Use the real periodic path. A new lease value forces the uncertain write's get/CAS repair.
    const originalWrite = Date.now();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(started).toEqual([originalWrite + 5_000]);
    await vi.advanceTimersByTimeAsync(8_000);
    expect(f.mirror.holdState("session:root-a")).toBe("renewing");
    // The safety window is already exhausted with zero added jitter. No deadline is weakened here.
    await vi.advanceTimersByTimeAsync(6_997);
    expect(started).toEqual([originalWrite + 5_000, originalWrite + 14_998]);
    expect(f.world.leases.values.get(target)?.revision).toBeGreaterThan(1);
    // Coalescing retains one pass; another default timer starts only after that pass completes.
    const writes = [...f.world.writes];
    await vi.advanceTimersByTimeAsync(4_999);
    expect(f.world.writes).toEqual(writes);
  });
});
