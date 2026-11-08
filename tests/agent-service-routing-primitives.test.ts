import { describe, expect, it } from "vitest";
import { AgentAdmission } from "../src/agents/lifecycle.js";
import { Semaphore } from "../src/agents/semaphore.js";
import { selectStickyRoute } from "../src/agents/service-routing.js";
import { stableJsonHash } from "../src/core/stable-hash.js";

// Keep reason: failure paths (reserved-slot starvation, cancellation, double
// release and tuple aliasing) cannot be reached cheaply in the hosted flow.
// The parent owns product-path verification; these are primitive regressions.
// No polling, sleeps, child processes or external state are needed.

describe("reserved Semaphore capacity", () => {
  it("admits P0 into the reserve while normal requests are saturated", async () => {
    const semaphore = new Semaphore(3, 1);
    const first = await semaphore.acquire();
    const second = await semaphore.acquire();
    expect(semaphore.tryAcquire()).toBeUndefined();
    const order: string[] = [];
    const normal = semaphore.acquire().then((release) => { order.push("normal"); return release; });
    const p0 = semaphore.acquire(undefined, true).then((release) => { order.push("p0"); return release; });
    const releaseP0 = await p0;
    expect(order).toEqual(["p0"]);
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    first();
    // Queue handoff reserves synchronously, before the promise continuation.
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    const releaseNormal = await normal;
    expect(order).toEqual(["p0", "normal"]);
    second();
    releaseNormal();
    releaseP0();
  });

  it("lets P0 fill the whole pool and normal waits for a free slot", async () => {
    const semaphore = new Semaphore(3, 1);
    const p0 = await Promise.all([
      semaphore.acquire(undefined, true),
      semaphore.acquire(undefined, true),
      semaphore.acquire(undefined, true),
    ] as const);
    let started = false;
    const normal = semaphore.acquire().then((release) => { started = true; return release; });
    await Promise.resolve();
    expect(started).toBe(false);
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    p0[0]();
    const releaseNormal = await normal;
    // Normal quota counts normal holders, not all holders including P0.
    expect(started).toBe(true);
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    releaseNormal();
    p0[1]();
    p0[2]();
  });

  it("preserves FIFO among eligible waiters instead of globally preferring P0", async () => {
    const semaphore = new Semaphore(2, 1);
    const normalHolder = await semaphore.acquire();
    const p0Holder = await semaphore.acquire(undefined, true);
    const order: string[] = [];
    const queued = (name: string, priority = false) => semaphore.acquire(undefined, priority)
      .then((release) => { order.push(name); return release; });
    const n1 = queued("n1");
    const p1 = queued("p1", true);
    const n2 = queued("n2");
    const p2 = queued("p2", true);
    normalHolder();
    const releaseN1 = await n1;
    expect(order).toEqual(["n1"]);
    p0Holder();
    const releaseP1 = await p1;
    expect(order).toEqual(["n1", "p1"]);
    // A duplicate release cannot create another permit or wake p2.
    p0Holder();
    await Promise.resolve();
    expect(order).toEqual(["n1", "p1"]);
    releaseN1();
    const releaseN2 = await n2;
    expect(order).toEqual(["n1", "p1", "n2"]);
    releaseP1();
    const releaseP2 = await p2;
    expect(order).toEqual(["n1", "p1", "n2", "p2"]);
    releaseN2();
    releaseP2();
  });

  it("skips an ineligible normal head when a P0 holder releases", async () => {
    const semaphore = new Semaphore(2, 1);
    const normalHolder = await semaphore.acquire();
    const p0Holder = await semaphore.acquire(undefined, true);
    const order: string[] = [];
    const normal = semaphore.acquire().then((release) => { order.push("normal"); return release; });
    const p0 = semaphore.acquire(undefined, true).then((release) => { order.push("p0"); return release; });
    p0Holder();
    const releaseP0 = await p0;
    expect(order).toEqual(["p0"]);
    normalHolder();
    const releaseNormal = await normal;
    expect(order).toEqual(["p0", "normal"]);
    releaseNormal();
    releaseP0();
  });

  it("removes cancelled normal and P0 waiters without spending capacity", async () => {
    const semaphore = new Semaphore(2, 1);
    const normalHolder = await semaphore.acquire();
    const p0Holder = await semaphore.acquire(undefined, true);
    const normalAbort = new AbortController();
    const p0Abort = new AbortController();
    const cancelledNormal = expect(semaphore.acquire(normalAbort.signal)).rejects.toThrow("Operation aborted");
    const cancelledP0 = expect(semaphore.acquire(p0Abort.signal, true)).rejects.toThrow("Operation aborted");
    const nextNormal = semaphore.acquire();
    const nextP0 = semaphore.acquire(undefined, true);
    normalAbort.abort();
    p0Abort.abort();
    await Promise.all([cancelledNormal, cancelledP0]);
    p0Holder();
    const releaseP0 = await nextP0;
    normalHolder();
    const releaseNormal = await nextNormal;
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    releaseNormal();
    releaseP0();
    const permits = [semaphore.tryAcquire(), semaphore.tryAcquire(undefined, true)];
    expect(permits.every(Boolean)).toBe(true);
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    permits.forEach((release) => release?.());
  });

  it("does not revoke a granted permit when its old queue signal aborts", async () => {
    const semaphore = new Semaphore(1, 1);
    const holder = await semaphore.acquire(undefined, true);
    const controller = new AbortController();
    const queued = semaphore.acquire(controller.signal, true);
    holder();
    controller.abort();
    const release = await queued;
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    release();
    release();
    const next = semaphore.tryAcquire(undefined, true);
    expect(next).toBeTypeOf("function");
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    next?.();
  });

  it("rejects already-aborted calls without consuming a slot", async () => {
    const semaphore = new Semaphore(2, 1);
    const controller = new AbortController();
    controller.abort();
    for (const priority of [false, true]) {
      expect(() => semaphore.tryAcquire(controller.signal, priority)).toThrow("Operation aborted");
      await expect(semaphore.acquire(controller.signal, priority)).rejects.toThrow("Operation aborted");
    }
    const normal = semaphore.tryAcquire();
    const p0 = semaphore.tryAcquire(undefined, true);
    expect(normal).toBeTypeOf("function");
    expect(p0).toBeTypeOf("function");
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    normal?.();
    p0?.();
  });

  it.each([undefined, 0])("preserves default FIFO and full normal capacity with reserved=%s", async (reserved) => {
    const semaphore = new Semaphore(2, reserved);
    const first = await semaphore.acquire();
    const second = semaphore.tryAcquire();
    expect(second).toBeTypeOf("function");
    const order: string[] = [];
    const normal = semaphore.acquire().then((release) => { order.push("normal"); return release; });
    const p0 = semaphore.acquire(undefined, true).then((release) => { order.push("p0"); return release; });
    first();
    expect(semaphore.tryAcquire(undefined, true)).toBeUndefined();
    const releaseNormal = await normal;
    expect(order).toEqual(["normal"]);
    first();
    await Promise.resolve();
    expect(order).toEqual(["normal"]);
    second?.();
    const releaseP0 = await p0;
    expect(order).toEqual(["normal", "p0"]);
    releaseNormal();
    releaseP0();
  });

  it("allows all slots to be reserved and cancels a permanently blocked normal", async () => {
    const semaphore = new Semaphore(1, 1);
    expect(semaphore.tryAcquire()).toBeUndefined();
    const controller = new AbortController();
    const cancelled = expect(semaphore.acquire(controller.signal)).rejects.toThrow("Operation aborted");
    const p0 = semaphore.tryAcquire(undefined, true);
    expect(p0).toBeTypeOf("function");
    p0?.();
    expect(semaphore.tryAcquire()).toBeUndefined();
    controller.abort();
    await cancelled;
  });

  it.each([-1, 0.5, 4, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])("rejects invalid reservation %s", (reserved) => {
    expect(() => new Semaphore(3, reserved)).toThrow("reserved capacity");
    expect(() => new AgentAdmission(3, Infinity, Infinity, 0, reserved)).toThrow("reservedP0");
  });
});

describe("per-direct-parent AgentAdmission", () => {
  it("isolates recursive parent pools and their P0 reservations", async () => {
    const admission = new AgentAdmission(2, 10, 3, 4, 1);
    const rootNormal = await admission.acquire("root");
    const rootP0 = admission.tryAcquire("root", undefined, true);
    expect(rootP0).toBeTypeOf("function");
    expect(admission.tryAcquire("root")).toBeUndefined();
    expect(admission.tryAcquire("root", undefined, true)).toBeUndefined();
    // A running child is a new direct parent, not another consumer of root.
    const childNormal = await admission.acquire("child");
    const childP0 = admission.tryAcquire("child", undefined, true);
    expect(childP0).toBeTypeOf("function");
    expect(admission.tryAcquire("child")).toBeUndefined();
    const controller = new AbortController();
    const rootWait = expect(admission.acquire("root", controller.signal, true)).rejects.toThrow("Operation aborted");
    childNormal();
    childP0?.();
    // Releasing the child's pool cannot free a root permit.
    expect(admission.tryAcquire("root", undefined, true)).toBeUndefined();
    controller.abort();
    await rootWait;
    rootNormal();
    rootP0?.();
    expect(admission.starts).toBe(4);
    admission.admit(3);
    expect(admission.starts).toBe(5);
    expect(() => admission.admit(4)).toThrow("depth limit");
  });

  it("keeps the existing four constructor arguments and default normal capacity", async () => {
    const admission = new AgentAdmission(2, 2, 1, 1);
    const first = await admission.acquire("root");
    const second = admission.tryAcquire("root");
    expect(second).toBeTypeOf("function");
    expect(admission.tryAcquire("root")).toBeUndefined();
    admission.admit(1);
    expect(admission.starts).toBe(2);
    expect(() => admission.admit(1)).toThrow("start limit");
    first();
    second?.();
  });

  it("forwards the priority argument on asynchronous queued acquisition", async () => {
    const admission = new AgentAdmission(1, Infinity, Infinity, 0, 1);
    const holder = await admission.acquire("root", undefined, true);
    const controller = new AbortController();
    const normal = expect(admission.acquire("root", controller.signal)).rejects.toThrow("Operation aborted");
    const next = admission.acquire("root", undefined, true);
    holder();
    const release = await next;
    expect(admission.tryAcquire("root", undefined, true)).toBeUndefined();
    controller.abort();
    await normal;
    release();
  });
});

describe("sticky rendezvous route selection", () => {
  const routes = ["route-a", "route-b", "route-c"] as const;
  const keys = Array.from({ length: 128 }, (_, index) => `conversation:${index}`);

  it("chooses the greatest full SHA-256 tuple score independent of candidate order", () => {
    const winners = new Set<string>();
    for (const key of keys) {
      const expected = [...routes].sort((left, right) => {
        const a = stableJsonHash([key, left]);
        const b = stableJsonHash([key, right]);
        return a < b ? 1 : a > b ? -1 : left < right ? 1 : left > right ? -1 : 0;
      })[0]!;
      expect(selectStickyRoute(key, routes)).toBe(expected);
      expect(selectStickyRoute(key, [routes[2], routes[0], routes[1]])).toBe(expected);
      expect(selectStickyRoute(key, [...routes].reverse())).toBe(expected);
      winners.add(expected);
    }
    expect([...winners].sort()).toEqual([...routes]);
  });

  it("only moves keys to an added winner", () => {
    let moved = 0;
    for (const key of keys) {
      const before = selectStickyRoute(key, routes);
      const after = selectStickyRoute(key, [...routes, "route-d"]);
      if (after !== before) {
        expect(after).toBe("route-d");
        moved++;
      }
    }
    expect(moved).toBeGreaterThan(0);
    expect(moved).toBeLessThan(keys.length);
  });

  it("only remaps keys whose winner was removed", () => {
    for (const removed of routes) {
      const remaining = routes.filter((route) => route !== removed);
      let moved = 0;
      for (const key of keys) {
        const before = selectStickyRoute(key, routes);
        const after = selectStickyRoute(key, remaining);
        if (before === removed) {
          expect(remaining).toContain(after);
          moved++;
        } else {
          expect(after).toBe(before);
        }
      }
      expect(moved).toBeGreaterThan(0);
    }
  });

  it("keeps tuple boundaries disjoint, including delimiters, nulls and Unicode", () => {
    const tuples = [["ab", "c"], ["a", "bc"], ["a:b", "c"], ["a", "b:c"], ["a\u0000b", "c"], ["a", "b\u0000c"]];
    expect(new Set(tuples.map((tuple) => stableJsonHash(tuple))).size).toBe(tuples.length);
    const candidates = ["c", "bc", "b:c", "b\u0000c", "\"],[]", "路由/é"] as const;
    for (const key of ["ab", "a", "a:b", "a\u0000b", "\"],[\"", "用户/é"]) {
      const expected = candidates.reduce((winner, route) =>
        stableJsonHash([key, route]) > stableJsonHash([key, winner]) ? route : winner);
      expect(selectStickyRoute(key, candidates)).toBe(expected);
    }
  });

  it("supports one route and duplicate candidates without mutating frozen input", () => {
    expect(selectStickyRoute("key", ["only"])).toBe("only");
    const frozen = Object.freeze(["route-c", "route-a", "route-b", "route-a"]);
    expect(selectStickyRoute("key", frozen)).toBe(selectStickyRoute("key", routes));
    expect(frozen).toEqual(["route-c", "route-a", "route-b", "route-a"]);
  });

  it("rejects an empty key or empty candidate set", () => {
    expect(() => selectStickyRoute("", routes)).toThrow("key must not be empty");
    expect(() => selectStickyRoute("key", [])).toThrow("at least one route");
  });
});
