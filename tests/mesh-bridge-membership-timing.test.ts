import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as monotonicClock } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeMirrorDeferredError, MeshBridge, type BridgePresence, type BridgeSide } from "../src/mesh/bridge.js";
import type { MeshEvent } from "../src/mesh/store.js";

// Keep reason: the measured A' membership stall cannot be reproduced cheaply in a flow.
// Drive the real syncPresence()/step() entry points with independently settled children to
// check operational diagnostics without changing concurrency, freshness or failure policy.
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
};
const empty = (): BridgePresence => ({ hosts: [], participants: [], reserved: [] });
const side = (): BridgeSide => ({
  latestSequence: async () => 0,
  read: async after => ({ events: [], through: after }),
  presence: async () => empty(),
  relayPresence: async () => ({ hosts: [], participants: [] }),
  owned: async () => ({ hosts: [], participants: [] }),
  mirror: async () => undefined,
  publish: async () => ({ sequence: 1 }),
  bridgedIds: async after => ({ ids: [], through: after, done: true }),
});
const fixture = (onLog?: (line: string) => void) => {
  let mono = 0, wall = 1_800_000_000_000;
  vi.spyOn(monotonicClock, "now").mockImplementation(() => mono);
  vi.spyOn(Date, "now").mockImplementation(() => wall);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-membership-timing-"));
  roots.push(root);
  const local = side(), remote = side(), logs: string[] = [];
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(root, "cursor.json"), presenceMs: 60_000,
    log: line => { logs.push(line); onLog?.(line); } });
  return { bridge, local, remote, logs, mono: (value: number) => { mono = value; },
    wall: (value: number) => { wall = value; }, now: () => wall };
};
const timing = (step: string, elapsedMs: number, status = "fulfilled") =>
  `membership timing: step=${step} elapsedMs=${elapsedMs} status=${status}`;

describe("operational bridge membership timing", () => {
  it("times each child and join, including synchronous scanning, without serializing either pair", async () => {
    const f = fixture();
    const local = deferred<BridgePresence>(), remote = deferred<BridgePresence>();
    const relays = deferred<{ hosts: []; participants: [] }>();
    const outbound = deferred<void>(), inbound = deferred<void>();
    const relayEntered = deferred<void>(), mirrorsEntered = deferred<void>();
    const order: string[] = [];
    const observedAt = f.now() + 20_000;
    vi.spyOn(f.local, "presence").mockImplementation(() => {
      order.push("local.presence");
      f.mono(1_250); // Synchronous scan happens BEFORE presence() returns its promise.
      f.wall(observedAt);
      return local.promise;
    });
    vi.spyOn(f.remote, "presence").mockImplementation(() => {
      order.push("remote.presence");
      expect(f.now()).toBe(observedAt);
      return remote.promise;
    });
    vi.spyOn(f.local, "relayPresence").mockImplementation(() => {
      order.push("local.relayPresence"); relayEntered.resolve(); return relays.promise;
    });
    vi.spyOn(f.remote, "mirror").mockImplementation(() => {
      order.push("remote.mirror"); return outbound.promise;
    });
    const mirror = vi.spyOn(f.local, "mirror").mockImplementation(() => {
      order.push("local.mirror"); mirrorsEntered.resolve(); return inbound.promise;
    });

    let settled = false;
    const sync = f.bridge.syncPresence().then(() => { settled = true; });
    expect(f.bridge.options.debug).toBeUndefined(); // The CLI's normal log hook is sufficient.
    expect(order).toEqual(["local.presence", "remote.presence"]);
    expect(f.logs).toEqual([]);
    f.mono(1_350);
    f.wall(observedAt + 30_000);
    local.resolve(empty());
    await local.promise;
    expect(f.logs).toEqual([timing("syncPresence.local.presence", 1_350)]);
    expect(order).toHaveLength(2); // Remote read remains in flight, not serialized behind local.
    f.mono(2_550); remote.resolve(empty());
    await relayEntered.promise;
    f.mono(3_750); relays.resolve({ hosts: [], participants: [] });
    await mirrorsEntered.promise;
    expect(order).toEqual(["local.presence", "remote.presence", "local.relayPresence", "remote.mirror", "local.mirror"]);
    expect(mirror.mock.calls[0]?.[1]).toBe(observedAt); // Not scan start, reply time or commit time.
    f.mono(4_850); outbound.resolve();
    await outbound.promise;
    expect(settled).toBe(false);
    expect(f.logs).not.toContain(timing("syncPresence.mirror.allSettled", 1_100));
    f.mono(6_050); inbound.resolve();
    await sync;
    expect(f.logs).toEqual([
      timing("syncPresence.local.presence", 1_350),
      timing("syncPresence.remote.presence", 1_300),
      timing("syncPresence.presence.all", 1_300),
      timing("syncPresence.local.relayPresence", 1_200),
      timing("syncPresence.remote.mirror", 1_100),
      timing("syncPresence.local.mirror", 2_300),
      timing("syncPresence.mirror.allSettled", 2_300),
    ]);
  });

  it.each([0, 999, 1_000])("does not log fast children or joins at %i ms", async elapsed => {
    const f = fixture();
    vi.spyOn(f.local, "presence").mockImplementation(() => {
      f.mono(elapsed);
      return Promise.resolve(empty());
    });
    await f.bridge.syncPresence();
    expect(f.logs).toEqual([]);
  });

  it.each(["fulfilled", "rejected"] as const)("logs age on EVERY reuse and resets after a %s pass", async status => {
    const f = fixture();
    const first = deferred<BridgePresence>();
    const presence = vi.spyOn(f.local, "presence").mockReturnValueOnce(first.promise);
    const pass = f.bridge.syncPresence();
    const outcome = pass.catch(error => error);
    expect(f.bridge.syncPresence()).toBe(pass);
    f.mono(750); f.wall(f.now() + 3_600_000);
    expect(f.bridge.syncPresence()).toBe(pass);
    f.mono(2_500); f.wall(f.now() - 7_200_000);
    expect(f.bridge.syncPresence()).toBe(pass);
    expect(f.logs.filter(line => line.startsWith("presence sync:"))).toEqual([
      "presence sync: joined in-flight pass ageMs=0",
      "presence sync: joined in-flight pass ageMs=750",
      "presence sync: joined in-flight pass ageMs=2500",
    ]);
    const failure = new Error("private fixture payload must never enter timing logs");
    if (status === "fulfilled") first.resolve(empty());
    else first.reject(failure);
    expect(await outcome).toBe(status === "fulfilled" ? undefined : failure);
    const next = deferred<BridgePresence>();
    presence.mockReturnValueOnce(next.promise);
    f.mono(20_000);
    const second = f.bridge.syncPresence();
    expect(second).not.toBe(pass);
    f.mono(20_004);
    expect(f.bridge.syncPresence()).toBe(second);
    expect(f.logs.at(-1)).toBe("presence sync: joined in-flight pass ageMs=4");
    next.resolve(empty()); await second;
    expect(f.logs.join("\n")).not.toContain(failure.message);
    expect(presence).toHaveBeenCalledTimes(2);
  });

  it.each(["local.presence", "remote.presence", "local.relayPresence"] as const)(
    "reports a slow rejected %s without masking the error or starting mirrors", async step => {
      const f = fixture();
      const pending = deferred<BridgePresence>();
      const entered = deferred<void>();
      const read = () => { entered.resolve(); return pending.promise; };
      if (step === "local.presence") vi.spyOn(f.local, "presence").mockImplementation(read);
      else if (step === "remote.presence") vi.spyOn(f.remote, "presence").mockImplementation(read);
      else vi.spyOn(f.local, "relayPresence").mockImplementation(read);
      const localMirror = vi.spyOn(f.local, "mirror"), remoteMirror = vi.spyOn(f.remote, "mirror");
      const outcome = f.bridge.syncPresence().catch(error => error);
      await entered.promise;
      f.mono(1_501);
      const failure = new Error("secret fixture failure details");
      pending.reject(failure);
      expect(await outcome).toBe(failure);
      expect(f.logs).toContain(timing(`syncPresence.${step}`, 1_501, "rejected"));
      expect(localMirror).not.toHaveBeenCalled();
      expect(remoteMirror).not.toHaveBeenCalled();
      expect(f.logs.join("\n")).not.toContain(failure.message);
    });

  it("times a synchronous throw without invoking the next child, then permits a new pass", async () => {
    const f = fixture();
    const failure = new Error("private synchronous scan failure");
    vi.spyOn(f.local, "presence").mockImplementationOnce(() => { f.mono(1_001); throw failure; });
    const remote = vi.spyOn(f.remote, "presence");
    await expect(f.bridge.syncPresence()).rejects.toBe(failure);
    expect(remote).not.toHaveBeenCalled();
    expect(f.logs).toEqual([timing("syncPresence.local.presence", 1_001, "rejected")]);
    await f.bridge.syncPresence();
    expect(remote).toHaveBeenCalledTimes(1);
  });

  it.each(["local", "remote"] as const)("waits for both mirrors after the %s mirror rejects and preserves failure priority", async rejectingSide => {
    const f = fixture();
    const local = deferred<void>(), remote = deferred<void>(), entered = deferred<void>();
    const order: string[] = [];
    vi.spyOn(f.remote, "mirror").mockImplementation(() => { order.push("remote"); return remote.promise; });
    vi.spyOn(f.local, "mirror").mockImplementation(() => { order.push("local"); entered.resolve(); return local.promise; });
    let settled = false;
    const outcome = f.bridge.syncPresence().then(() => { settled = true; }, error => { settled = true; return error; });
    await entered.promise;
    expect(order).toEqual(["remote", "local"]);
    const transient = new BridgeMirrorDeferredError(true, 1_000);
    f.mono(1_101);
    const first = rejectingSide === "local" ? local : remote;
    const second = rejectingSide === "local" ? remote : local;
    first.reject(transient);
    await first.promise.catch(() => undefined);
    expect(f.logs).toContain(timing(`syncPresence.${rejectingSide}.mirror`, 1_101, "rejected"));
    expect(settled).toBe(false);
    expect(f.logs.some(line => line.includes("step=syncPresence.mirror.allSettled"))).toBe(false);
    const permanent = new Error("private permanent failure");
    f.mono(2_202); second.reject(permanent);
    expect(await outcome).toBe(permanent);
    expect(f.logs).toContain(timing("syncPresence.mirror.allSettled", 2_202));
    expect(f.logs.join("\n")).not.toContain(permanent.message);
  });

  it("does not let a throwing operational logger change success, failure or coalescing", async () => {
    const f = fixture(() => { throw new Error("logger unavailable"); });
    const pending = deferred<BridgePresence>();
    vi.spyOn(f.local, "presence").mockReturnValueOnce(pending.promise);
    const pass = f.bridge.syncPresence();
    f.mono(1_100);
    expect(f.bridge.syncPresence()).toBe(pass);
    pending.resolve(empty());
    await expect(pass).resolves.toBeUndefined();
    const failure = new Error("original failure");
    vi.spyOn(f.remote, "presence").mockImplementationOnce(() => { f.mono(2_200); throw failure; });
    await expect(f.bridge.syncPresence()).rejects.toBe(failure);
  });
});

// Exercise the private refresh through a real source page and its durable cursor, not by
// exposing or invoking private methods. An attributed but absent recipient requires refresh.
const refreshFixture = async () => {
  const joined = deferred<void>();
  const f = fixture(line => { if (line.startsWith("presence sync: joined")) joined.resolve(); });
  await f.bridge.start();
  f.logs.length = 0;
  const event: MeshEvent = { id: "timing-source-event", sequence: 1, createdAt: f.now(), verification: "mesh",
    topic: "fleet.work.timing", kind: "ask", from: { id: "session:sender-00000000", name: "sender", kind: "main" },
    to: "session:missing-00000000", text: "private source payload" };
  vi.spyOn(f.local, "read").mockImplementation(async after => ({ events: after < 1 ? [event] : [], through: 1 }));
  f.local.mirrored = id => id === event.to;
  const publish = vi.spyOn(f.remote, "publish");
  const first = deferred<BridgePresence>(), second = deferred<BridgePresence>(), freshEntered = deferred<void>();
  const samples = vi.spyOn(f.remote, "presence").mockReturnValueOnce(first.promise).mockImplementationOnce(() => {
    freshEntered.resolve(); return second.promise;
  });
  const pass = f.bridge.syncPresence();
  const passOutcome = pass.catch(error => error);
  const step = f.bridge.step();
  const stepOutcome = step.catch(error => error);
  await joined.promise;
  return { ...f, first, second, freshEntered, samples, passOutcome, stepOutcome, publish };
};

describe("membership refresh timing through step()", () => {
  it("times the joined pass and one trailing fresh pass without changing coalescing or routing", async () => {
    const f = await refreshFixture();
    f.mono(1_500); f.first.resolve(empty());
    await f.freshEntered.promise;
    expect(await f.passOutcome).toBeUndefined();
    expect(f.logs).toContain(timing("refreshMembership.syncPresence", 1_500));
    f.mono(3_100); f.second.resolve(empty());
    expect(await f.stepOutcome).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
    expect(f.logs).toContain(timing("refreshMembership.freshSyncPresence", 1_600));
    expect(f.samples).toHaveBeenCalledTimes(2);
    expect(f.publish).not.toHaveBeenCalled();
    const cursor = JSON.parse(fs.readFileSync(f.bridge.options.cursorPath, "utf8"));
    expect(cursor.toRemote.after).toBe(1);
    expect(f.logs.join("\n")).not.toContain("private source payload");
  });

  it.each(["joined", "fresh"] as const)("propagates the %s pass failure without retrying or advancing the cursor", async phase => {
    const f = await refreshFixture();
    const failure = new Error("private refresh failure");
    f.mono(1_500);
    if (phase === "joined") f.first.reject(failure);
    else {
      f.first.resolve(empty());
      await f.freshEntered.promise;
      f.mono(3_100); f.second.reject(failure);
    }
    expect(await f.stepOutcome).toBe(failure);
    expect(await f.passOutcome).toBe(phase === "joined" ? failure : undefined);
    expect(f.logs).toContain(timing(phase === "joined" ? "refreshMembership.syncPresence" : "refreshMembership.freshSyncPresence",
      phase === "joined" ? 1_500 : 1_600, "rejected"));
    expect(f.samples).toHaveBeenCalledTimes(phase === "joined" ? 1 : 2);
    expect(f.publish).not.toHaveBeenCalled();
    const cursor = JSON.parse(fs.readFileSync(f.bridge.options.cursorPath, "utf8"));
    expect(cursor.toRemote.after).toBe(0);
    expect(f.logs.join("\n")).not.toContain(failure.message);
  });
});
