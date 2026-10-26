// Keep: lease-freshness budgets and fence/in-flight timing cannot be reached cheaply in a user flow.
// The default 5 s timeout bounds the whole uncertain recovery, while renewals stay on a fixed-rate
// schedule. A pending recovery never extends routing authority beyond the 13 s freshness fence.
// These tests use the real mirror/reader over fake NATS KV, not a replacement renewal algorithm.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { NatsKvMirror, NatsPresenceReader } from "../src/mesh/bridge-nats-presence.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUncertainError, type TransportKv } from "../src/transport/types.js";
import { hKey, kvWorld } from "./helpers/bridge-nats-kv.js";

const MACHINE = "bridge-machine";
const open: Array<{ mirror: NatsKvMirror; reader: NatsPresenceReader }> = [];
const delay = (ms: number, signal?: AbortSignal) => new Promise<void>((resolve, reject) => {
  const timer = setTimeout(() => {
    signal?.removeEventListener("abort", abort);
    resolve();
  }, ms);
  const abort = (): void => {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
    reject(signal?.reason ?? new Error("aborted"));
  };
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
});

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

  it("bounds the whole recovery and preserves fixed-rate renewal without extending stale authority", async () => {
    const f = await fixture();
    const target = hKey(MACHINE, "session:root-a");
    const ackAt = Date.now();
    const put = f.leases.put.bind(f.leases), get = f.leases.get.bind(f.leases);
    const deadlines: number[] = [];
    vi.spyOn(AbortSignal, "timeout").mockImplementation(ms => {
      deadlines.push(ms);
      const controller = new AbortController();
      setTimeout(() => controller.abort(new DOMException("budget expired", "TimeoutError")), ms);
      return controller.signal;
    });

    const operations: Array<{ kind: "put" | "get"; at: number; signal: AbortSignal }> = [];
    const putStarts: number[] = [];
    const passes: Array<{ startedAt: number; settledAt?: number }> = [];
    const renew = f.mirror.renew.bind(f.mirror);
    vi.spyOn(f.mirror, "renew").mockImplementation(() => {
      const pass = renew();
      const timing: { startedAt: number; settledAt?: number } = { startedAt: Date.now() };
      passes.push(timing);
      void pass.then(() => { timing.settledAt = Date.now(); }, () => { timing.settledAt = Date.now(); });
      return pass;
    });

    let targetPuts = 0;
    vi.spyOn(f.leases, "put").mockImplementation(async (...args) => {
      if (args[0] !== target) return put(...args);
      const signal = args[4];
      if (!signal) throw new Error("renewal put did not receive its deadline signal");
      targetPuts++;
      putStarts.push(Date.now());
      operations.push({ kind: "put", at: Date.now(), signal });
      if (targetPuts === 1) {
        await delay(4_999, signal);
        throw new TransportUncertainError(target, {});
      }
      if (targetPuts === 2) await delay(10_000, signal);
      return put(...args);
    });
    vi.spyOn(f.leases, "get").mockImplementation((async (key: string, signal?: AbortSignal) => {
      if (key === target) {
        if (!signal) throw new Error("recovery read did not share the write deadline");
        operations.push({ kind: "get", at: Date.now(), signal });
        await delay(4_999, signal);
      }
      return get(key, signal);
    }) as TransportKv["get"]);

    // The first scheduled renewal starts at +5 s; a near-timeout uncertain put leaves only the
    // remainder of that same 5 s deadline for its recovery read.
    await vi.advanceTimersByTimeAsync(5_000);
    expect(putStarts).toEqual([ackAt + 5_000]);
    expect(f.mirror.holdState("session:root-a")).toBe("live");

    // One deadline aborts the put/get recovery by +10 s, before the +13 s authority fence.
    // The missed fixed-rate slot is then taken on the next timer turn; it is not shifted by
    // another full renewal interval. Fake timers defer a nested zero-delay timeout by 1 ms.
    await vi.advanceTimersByTimeAsync(5_000);
    expect(passes[0]?.settledAt).toBe(ackAt + 10_000);
    expect(putStarts).toEqual([ackAt + 5_000]);
    await vi.advanceTimersByTimeAsync(1);
    expect(operations.map(({ kind, at }) => ({ kind, at }))).toEqual([
      { kind: "put", at: ackAt + 5_000 },
      { kind: "get", at: ackAt + 9_999 },
      { kind: "put", at: ackAt + 10_001 },
    ]);
    expect(operations[0]!.signal).toBe(operations[1]!.signal);
    expect(operations[2]!.signal).not.toBe(operations[0]!.signal);
    expect(deadlines.every(ms => ms === 5_000)).toBe(true);
    expect(passes.map(pass => pass.startedAt)).toEqual([ackAt + 5_000, ackAt + 10_001]);
    expect(passes[0]!.settledAt!).toBeLessThan(ackAt + 13_000);

    // A later recovery is still in flight at the old lease's safety boundary. Neither the mirror
    // nor an actual reader may treat that unconfirmed lease as routing authority.
    await vi.advanceTimersByTimeAsync(2_999);
    expect(Date.now()).toBe(ackAt + 13_000);
    expect(f.mirror.holdState("session:root-a")).toBe("renewing");
    expect(f.mirror.holds("session:root-a")).toBe(false);
    expect(f.reader.resolve("session:root-a")?.live).toBe(false);
    expect(f.reader.resolve("session:root-a")?.liveUntil).toBeLessThanOrEqual(ackAt + 13_000);

    const fencing = f.mirror.fence();
    await vi.advanceTimersByTimeAsync(2_001);
    await fencing;
  });
});
