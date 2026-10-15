import { describe, expect, it } from "vitest";
import { ActorSlotRecovery, ActorSlotRecoveryParkedError } from "../src/actors/slot-recovery.js";

// Keep: data safety/trust boundary (#7991). A failed scope must never open its
// sibling's protected actors or leave an unjoined materializer behind a retry.
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};

describe("owned protected actor recovery", () => {
  it("shares one awaited stage across both scopes and opens them together", async () => {
    const recovery = new ActorSlotRecovery();
    const project = deferred(), session = deferred();
    const enabled: string[] = [];
    let calls = 0;
    recovery.register(() => { calls++; return project.promise; }, () => enabled.push("project"));
    recovery.register(() => { calls++; return session.promise; }, () => enabled.push("session"));
    const first = recovery.recover(), second = recovery.recover();
    expect(second).toBe(first);
    await Promise.resolve();
    expect(calls).toBe(2);
    project.resolve();
    await project.promise;
    expect(recovery.ready).toBe(false);
    expect(enabled).toEqual([]);
    session.resolve();
    await first;
    expect(recovery.ready).toBe(true);
    expect(enabled).toEqual(["project", "session"]);
  });

  it("joins a delayed sibling on failure, then permits an explicit unchanged retry", async () => {
    const recovery = new ActorSlotRecovery();
    const sibling = deferred();
    let fail = true, rejected = false;
    recovery.register(async () => { if (fail) throw new Error("authority unavailable"); });
    recovery.register(() => sibling.promise);
    const pending = recovery.recover().catch(error => { rejected = true; throw error; });
    const assertion = expect(pending).rejects.toThrow("authority unavailable");
    await Promise.resolve();
    await Promise.resolve();
    expect(rejected).toBe(false);
    expect(recovery.ready).toBe(false);
    expect(recovery.recover()).not.toBe(pending); // pending is a caller's catch wrapper
    sibling.resolve();
    await assertion;
    expect(recovery.ready).toBe(false);
    expect(recovery.status()).toMatchObject({ ready: false, error: { code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED", message: expect.stringContaining("authority unavailable") } });
    fail = false;
    await recovery.recover();
    expect(recovery.ready).toBe(true);
  });

  it("does not hide multiple failed scopes", async () => {
    const recovery = new ActorSlotRecovery();
    recovery.register(async () => { throw new Error("project"); });
    recovery.register(async () => { throw new Error("session"); });
    const error = await recovery.recover().catch(error => error);
    expect(error).toBeInstanceOf(ActorSlotRecoveryParkedError);
    expect(error.code).toBe("FABRIC_ACTOR_SLOT_RECOVERY_PARKED");
    expect(error.errors.map((error: Error) => error.message)).toEqual(["project", "session"]);
    expect(recovery.ready).toBe(false);
  });

  it("closes readiness during explicit reconciliation and forbids late scopes", async () => {
    const recovery = new ActorSlotRecovery();
    let stage = Promise.resolve();
    recovery.register(() => stage);
    await recovery.recover();
    expect(() => recovery.register(async () => {})).toThrow("after startup");
    const retry = deferred();
    stage = retry.promise;
    const pending = recovery.recover();
    expect(recovery.ready).toBe(false);
    retry.resolve();
    await pending;
    expect(recovery.ready).toBe(true);
    expect(recovery.status()).toEqual({ ready: true });
  });

  it("does not misclassify an unexpected enable callback failure as a protected-stage refusal", async () => {
    const recovery = new ActorSlotRecovery();
    recovery.register(async () => {}, () => { throw new Error("unexpected native enable failure"); });
    const error = await recovery.recover().catch(error => error);
    expect(error).not.toBeInstanceOf(ActorSlotRecoveryParkedError);
    expect(error.message).toBe("unexpected native enable failure");
    expect(recovery.status()).toEqual({ ready: false });
  });
});
