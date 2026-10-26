// Keep: timing/failure paths (independent refresh deadlines, broker failure and shutdown)
// are not cheaply reachable in a user flow. Exercise the real directory over fake NATS KV/CAS/watches.
import { performance } from "node:perf_hooks";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { TransportUnavailableError } from "../src/transport/types.js";
import { kvWorld, pKey } from "./helpers/bridge-nats-kv.js";

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

  it("samples a fresh bounded failure reschedule rather than spinning or pretending the failed write succeeded", async () => {
    const random = vi.fn<() => number>().mockReturnValueOnce(0).mockReturnValueOnce(1).mockReturnValue(0);
    const f = await fixture(random);
    await f.view.publishBeforeLaunch(record("agent:failure"));
    const revision = f.revision("agent:failure");
    const failure = new TransportUnavailableError("broker unavailable");
    vi.spyOn(f.world.presence, "put").mockImplementationOnce(() => { throw failure; });
    await vi.advanceTimersByTimeAsync(144_000);
    expect(f.revision("agent:failure")).toBe(revision);
    expect(f.errors).toEqual([failure]);
    expect(random).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(215_999);
    expect(f.revision("agent:failure")).toBe(revision);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.revision("agent:failure")).toBeGreaterThan(revision!);
    expect(random).toHaveBeenCalledTimes(3);
    // The failed refresh was not a successful-write receipt; a normal on-change publication still works.
    await f.view.publish({ ...record("agent:failure"), status: "idle" });
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

  it.each(["close", "lease abort"] as const)("cancels pending refreshes on %s", async stop => {
    const random = vi.fn(() => 1);
    const f = await fixture(random);
    await f.view.publishBeforeLaunch(record("agent:stop"));
    if (stop === "close") await f.view.close();
    else f.abort.abort(new Error("owner fenced"));
    const writes = [...f.world.writes];
    await vi.advanceTimersByTimeAsync(600_000);
    expect(f.world.writes).toEqual(writes);
    expect(random).toHaveBeenCalledTimes(1);
    expect(f.view.ready).toBe(false);
  });
});
