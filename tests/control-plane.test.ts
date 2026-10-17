import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MeshStore,
  type MeshIdentity,
  type MeshEvent,
  type MeshStoreOptions,
} from "../src/mesh/store.js";
import { CONTROL_CLAIMS_POLICY_KEY, FabricControlPlane, type FabricControlCommand, type FabricControlPlaneOptions, type MirroredControlOwner } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { createMainExecutionCeilingError } from "../src/async-settlement.js";
import { removeHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { FileControlReader, FileTransport } from "../src/transport/file.js";

const roots: string[] = [];
const planes: FabricControlPlane[] = [];
// Synthetic owner snapshot for tests that inject wire ACKs instead of running an owner.
const fixtureOwnerIncarnation = "fixture:owner";

const identity = (id: string): MeshIdentity => ({
  id,
  name: id,
  kind: "main",
  sessionId: id,
});

const plane = (
  meshRoot: string,
  id: string,
  storeOptions: MeshStoreOptions = {},
  controlOptions: Pick<FabricControlPlaneOptions, "pollMs" | "acknowledgementTimeoutMs" | "bridgeTimeoutMs" | "readMirroredOwner" | "mirrorLeaseWaitMs"> = {},
): FabricControlPlane => {
  const value = new FabricControlPlane(
    new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions),
    identity(id),
    {
      enabled: true,
      hostId: id,
      pollMs: 20,
      acknowledgementTimeoutMs: 1_000,
      ...controlOptions,
    },
  );
  planes.push(value);
  return value;
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map((value) => value.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Keep reason: #9751 is a recorded receipt-HOL failure and an outcome/ACK custody boundary.
// Exercise the real reader, local outcome store and ACK verifier, not an implementation queue.
describe("saved steer/followUp ACK obligations (#9751)", () => {
  const ownerId = "host:9751-owner";
  const senderId = "host:9751-sender";
  const seenKey = (commandId: string) => "topology/control-seen/" +
    createHash("sha256").update(`${ownerId}\0${commandId}`).digest("hex");
  const gate = () => {
    let open!: () => void;
    const promise = new Promise<void>(resolve => { open = resolve; });
    return { promise, open };
  };
  const until = (assertion: () => void) => vi.waitFor(assertion, { timeout: 3_000, interval: 20 });
  const settled = async <T,>(promise: Promise<T>): Promise<T> => {
    let done = false;
    void promise.then(() => { done = true; }, () => { done = true; });
    await until(() => expect(done).toBe(true));
    return promise;
  };
  const setup = (options: Partial<FabricControlPlaneOptions> = {}, storeOptions: MeshStoreOptions = {}) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-9751-")); roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions);
    const owner = new FabricControlPlane(mesh, identity(ownerId), {
      enabled: true, hostId: ownerId, pollMs: 20, acknowledgementTimeoutMs: 60_000, ...options,
    });
    planes.push(owner);
    const own = new MeshStore(path.join(meshRoot, "control-seen",
      createHash("sha256").update(ownerId).digest("hex").slice(0, 32)), 64 * 1024, 1_000);
    const command = (commandId: string, targetId = "agent:same", operation: "steer" | "followUp" = "steer") => ({
      topic: "fabric.control.command", kind: operation, from: identity(senderId), to: ownerId,
      data: { version: 1, commandId, targetId, operation, replyTo: senderId, ownerIncarnation: owner.incarnation,
        message: commandId, requestCreatedAt: Date.now(), requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
    });
    const saved = (commandId: string) => own.get(seenKey(commandId), { fresh: true })?.value;
    const acks = (commandId?: string) => mesh.read({ topic: "fabric.control.ack", limit: 1_000 })
      .filter(event => event.from.id === ownerId && (!commandId || (event.data as { commandId?: string }).commandId === commandId));
    // A request issued by this owner can only settle when its own reader consumes an inbound ACK.
    // This is an observable reader barrier, not a private cursor/queue diagnostic.
    let barrierSequence = 0;
    const inboundBarrier = async () => {
      const message = `reader barrier ${++barrierSequence}`;
      const request = owner.request("host:9751-peer", "agent:peer", "steer", {
        ownerIncarnation: fixtureOwnerIncarnation, message,
      }, "host:9751-peer");
      void request.catch(() => undefined);
      await until(() => expect(mesh.read({ topic: "fabric.control.command", limit: 1_000 })
        .some(event => event.from.id === ownerId && event.to === "host:9751-peer" &&
          (event.data as FabricControlCommand).message === message)).toBe(true));
      const event = mesh.read({ topic: "fabric.control.command", limit: 1_000 })
        .find(event => event.from.id === ownerId && event.to === "host:9751-peer" &&
          (event.data as FabricControlCommand).message === message)!;
      const data = event.data as FabricControlCommand;
      await mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("host:9751-peer"), to: ownerId,
        data: { version: 1, commandId: data.commandId, targetId: data.targetId, ownerIncarnation: fixtureOwnerIncarnation,
          accepted: true, messageId: "reader-advanced" } });
      return settled(request);
    };
    return { root, meshRoot, mesh, own, owner, command, saved, acks, inboundBarrier };
  };
  const holdFirstAck = (f: ReturnType<typeof setup>) => {
    const release = gate();
    const publish = f.mesh.publish.bind(f.mesh);
    let calls = 0, active = 0, highWater = 0;
    const spy = vi.spyOn(f.mesh, "publish").mockImplementation(async input => {
      if (input.topic !== "fabric.control.ack" || input.from.id !== ownerId) return publish(input);
      calls++; active++; highWater = Math.max(highWater, active);
      try {
        if (calls === 1) await release.promise;
        return await publish(input);
      } finally { active--; }
    });
    return { release, spy, calls: () => calls, highWater: () => highWater };
  };

  // Keep reason: the recorded #9751 custody regression needs native completion-error
  // transparency, while E1 deliberately reconciles the generic send path differently.
  it("9751: FILE native publish and generic send preserve definite pre-append errors and permit a real retry", async () => {
    const f = setup(), transport = new FileTransport(f.mesh, { machine: "mesh", hostId: ownerId });
    const publish = f.mesh.publish.bind(f.mesh);
    const exactError = Object.assign(new Error("definite pre-append refusal"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    const native = { ...f.command("native-definite"), id: "native:definite" };
    const generic = f.command("generic-definite");
    const envelope = { id: "logical:definite", kind: "command" as const,
      to: { machine: "mesh", hostId: ownerId }, targetId: "agent:same", operation: "steer",
      from: generic.from, body: generic.data };
    let refuse = true;
    const spy = vi.spyOn(f.mesh, "publish").mockImplementation(async input => {
      if (refuse) throw exactError; // No call-through: the native append has not begun.
      return publish(input);
    });
    try {
      await expect(transport.publish(native)).rejects.toBe(exactError);
      await expect(transport.send(envelope)).rejects.toBe(exactError);
      expect(spy).toHaveBeenCalledTimes(2);
      expect(spy.mock.calls[0]![0].id).toBe(native.id);
      const failedGenericId = spy.mock.calls[1]![0].id;
      expect(failedGenericId).toEqual(expect.any(String));
      expect(failedGenericId).not.toBe(envelope.id);
      expect(f.mesh.read({ limit: 1_000 })).toEqual([]);

      // Neither rejected operation may be reported as successful custody. Once the
      // definite refusal clears, explicit retries must reach the real append once each.
      refuse = false;
      const nativeReceipt = await transport.publish(native);
      const genericReceipt = await transport.send(envelope);
      const events = f.mesh.read({ limit: 1_000 });
      expect(spy).toHaveBeenCalledTimes(4);
      expect(events).toHaveLength(2);
      expect(events[0]).toEqual(nativeReceipt);
      expect(nativeReceipt).toMatchObject({ id: native.id, data: native.data });
      expect(events.filter(event => event.id === native.id)).toHaveLength(1);
      const genericEvent = events[1]!;
      expect(genericEvent.id).toBe(spy.mock.calls[3]![0].id);
      expect(genericEvent.id).not.toBe(failedGenericId);
      expect(genericEvent.id).not.toBe(envelope.id);
      expect(genericEvent.data).toEqual(envelope.body);
      expect(genericReceipt).toMatchObject({ id: envelope.id, sequence: genericEvent.sequence, duplicate: false,
        envelope: { id: envelope.id, body: envelope.body } });
    } finally { await transport.close(); }
  });

  it("9751: FILE native publish rejects the exact post-append error while generic send resolves the positive E1 sequence", async () => {
    const f = setup(), transport = new FileTransport(f.mesh, { machine: "mesh", hostId: ownerId });
    const publish = f.mesh.publish.bind(f.mesh), exactError = new Error("append completed, publisher completion failed");
    const command = f.command("native-postappend"), stamp = vi.fn(() => command.data);
    const native = { ...command, id: "native:postappend", data: stamp };
    const generic = f.command("generic-postappend");
    const envelope = { id: "logical:postappend", kind: "command" as const,
      to: { machine: "mesh", hostId: ownerId }, targetId: "agent:same", operation: "steer",
      from: generic.from, body: generic.data };
    const appended: MeshEvent[] = [];
    const spy = vi.spyOn(f.mesh, "publish").mockImplementation(async input => {
      const event = await publish(input); // Real native append and positive receipt, not a fabricated event.
      appended.push(event);
      throw exactError;
    });
    try {
      let nativeError: unknown;
      await transport.publish(native).catch(error => { nativeError = error; });
      // The identical call-through fault is NOT a native error for the generic E1 port.
      const receipt = await transport.send(envelope);
      const events = f.mesh.read({ limit: 1_000 });
      expect(spy).toHaveBeenCalledTimes(2);
      expect(events).toHaveLength(2);
      expect(events).toEqual(appended);
      expect(spy.mock.calls[0]![0].id).toBe(native.id);
      expect(events[0]).toMatchObject({ id: native.id, data: command.data });
      expect(events.filter(event => event.id === native.id)).toHaveLength(1);
      expect(stamp).toHaveBeenCalledOnce();
      const genericEvent = events[1]!;
      expect(genericEvent.id).toBe(spy.mock.calls[1]![0].id);
      expect(genericEvent.id).not.toBe(envelope.id);
      expect(genericEvent.id).not.toBe(native.id);
      expect(genericEvent.data).toEqual(envelope.body);
      expect(genericEvent.sequence).toBeGreaterThan(0);
      expect(receipt).toMatchObject({ id: envelope.id, sequence: genericEvent.sequence, duplicate: false,
        envelope: { id: envelope.id, body: envelope.body } });
      // A positive on-disk native event must not erase the publisher's completion error.
      expect(nativeError).toBe(exactError);
    } finally { await transport.close(); }
  });

  it("9751: saved ACK backlog does not block ordered steer/followUp admission", async () => {
    const f = setup(), blocked = holdFirstAck(f), order: string[] = [];
    const sender = plane(f.meshRoot, senderId, {}, { acknowledgementTimeoutMs: 60_000 });
    f.owner.start(command => { order.push(command.message!); return { accepted: true, messageId: command.message! }; });
    sender.start(() => ({ accepted: false }));
    let senderSettled = false;
    const first = sender.request(ownerId, "agent:same", "steer", { message: "A", ownerIncarnation: f.owner.incarnation }, ownerId);
    void first.then(() => { senderSettled = true; }, () => { senderSettled = true; });
    try {
      await until(() => expect(blocked.calls()).toBe(1));
      const a = (f.mesh.read({ topic: "fabric.control.command" })[0]!.data as FabricControlCommand).commandId;
      expect(f.saved(a)).toMatchObject({ acceptance: { accepted: true, messageId: "A" } });
      await f.mesh.publish(f.command("B", "agent:same", "followUp"));
      await f.mesh.publish(f.command("C", "agent:other"));
      // These are before gate-open requirements: baseline must go RED here, not merely on elapsed time.
      await until(() => {
        expect(order).toEqual(["A", "B", "C"]);
        expect(f.saved("B")).toMatchObject({ acceptance: { accepted: true, messageId: "B" } });
        expect(f.saved("C")).toMatchObject({ acceptance: { accepted: true, messageId: "C" } });
      });
      await expect(f.inboundBarrier()).resolves.toMatchObject({ acknowledged: true, messageId: "reader-advanced" });
      expect(senderSettled).toBe(false);
      expect(f.acks()).toEqual([]);
      expect(blocked.calls()).toBe(1); // one worker, not a detached swarm awaiting the same log lock
      blocked.release.open();
      await expect(settled(first)).resolves.toMatchObject({ acknowledged: true, messageId: "A" });
      await until(() => expect(f.acks()).toHaveLength(3));
      expect(f.acks().map(event => (event.data as { messageId: string }).messageId)).toEqual(["A", "B", "C"]);
      expect(order).toEqual(["A", "B", "C"]);
      expect(blocked.highWater()).toBe(1);
    } finally { blocked.release.open(); await settled(first).catch(() => undefined); blocked.spy.mockRestore(); }
  });

  it("9751: awaited handler admission preserves same-target order", async () => {
    const f = setup(), binding = gate(), entered = gate(), order: string[] = [];
    // The explicit scheduler barrier drains ready promise work, not a wall-clock performance budget.
    // A whole-handler detach runs B during this barrier while A awaits its activation binding.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      await f.mesh.publish(f.command("A"));
      await f.mesh.publish(f.command("B", "agent:same", "followUp"));
      f.owner.start(async command => {
        if (command.commandId === "A") { entered.open(); await binding.promise; }
        order.push(command.commandId);
        return { accepted: true, messageId: command.commandId };
      });
      await vi.advanceTimersByTimeAsync(0);
      await settled(entered.promise);
      await vi.advanceTimersByTimeAsync(0);
      expect(order).toEqual([]);
      expect(f.mesh.get(seenKey("B"), { fresh: true })).toBeUndefined();
      binding.open();
      await vi.advanceTimersByTimeAsync(0);
      expect(order).toEqual(["A", "B"]);
      expect(f.acks().map(event => (event.data as { messageId: string }).messageId)).toEqual(["A", "B"]);
    } finally { binding.open(); vi.useRealTimers(); }
  });

  it("9751: ACK custody caps at one active plus 99 pending without reading or claiming the 101st", async () => {
    const f = setup(), blocked = holdFirstAck(f), order: string[] = [];
    // Fill before start so the first real tail page is exactly the 100 admitted obligations.
    for (let index = 1; index <= 101; index++) await f.mesh.publish(f.command(`cap:${index}`));
    const tail = vi.spyOn(f.mesh, "tail");
    // Suppress incidental filesystem wakes: only start and ACK settlement may wake this reader.
    // Same fake-watch seam as control-plane-idle-wakes; no deleting child or alternate backend.
    vi.spyOn(fs, "watch").mockImplementation(() =>
      Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    f.owner.start(command => { order.push(command.commandId); return { accepted: true, messageId: command.commandId }; });
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(f.saved("cap:100")).toMatchObject({ acceptance: { accepted: true } });
      expect(order).toEqual(Array.from({ length: 100 }, (_, index) => `cap:${index + 1}`));
      expect(blocked.calls()).toBe(1);
      expect(f.mesh.get(seenKey("cap:101"), { fresh: true })).toBeUndefined();
      expect(f.saved("cap:101")).toBeUndefined();
      expect(tail.mock.results.some(result => result.type === "return" && result.value.events.some(
        (event: MeshEvent) => event.topic === "fabric.control.command" && (event.data as FabricControlCommand).commandId === "cap:101"))).toBe(false);
      const readsAtCapacity = tail.mock.calls.length, timersAtCapacity = timeouts.mock.calls.length;
      const armedAtCapacity = vi.getTimerCount();
      // Under the existing 60 s command deadlines, cross the entire 30 s retry ceiling.
      // Capacity alone must not arm/re-arm hot backoff, even when no watcher can rescue it.
      await vi.advanceTimersByTimeAsync(30_000);
      expect(tail).toHaveBeenCalledTimes(readsAtCapacity);
      expect(timeouts).toHaveBeenCalledTimes(timersAtCapacity);
      expect(vi.getTimerCount()).toBe(armedAtCapacity);
      blocked.release.open();
      await vi.advanceTimersByTimeAsync(0);
      expect(f.acks()).toHaveLength(101); // settlement itself wakes the 101st; no filesystem hint/timer
      expect(order).toEqual(Array.from({ length: 101 }, (_, index) => `cap:${index + 1}`));
      expect(blocked.calls()).toBe(101);
      expect(blocked.highWater()).toBe(1);
    } finally {
      blocked.release.open();
      // Individual mockRestore() leaves Vitest's restore callback registered. Clear it
      // while the fake clock is installed, or restoreMocks re-installs fake setTimeout
      // in the next test after useRealTimers(), stranding every real retry timer.
      vi.restoreAllMocks(); vi.useRealTimers();
    }
  }, 15_000);

  it("9751: duplicate commands coalesce while their ACK is active or queued", async () => {
    const f = setup(), blocked = holdFirstAck(f);
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    const a = f.command("A"), b = f.command("B", "agent:same", "followUp");
    f.owner.start(receive);
    try {
      await f.mesh.publish(a);
      await until(() => expect(blocked.calls()).toBe(1));
      await f.mesh.publish(b);
      await until(() => expect(f.saved("B")).toMatchObject({ acceptance: { accepted: true } }));
      await f.mesh.publish(a); await f.mesh.publish(b);
      await expect(f.inboundBarrier()).resolves.toMatchObject({ acknowledged: true });
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["A", "B"]);
      expect(blocked.calls()).toBe(1);
      blocked.release.open();
      await until(() => expect(f.acks()).toHaveLength(2));
      await expect(f.inboundBarrier()).resolves.toMatchObject({ acknowledged: true });
      expect(blocked.calls()).toBe(2); // no extra owned publication job for either duplicate
      expect(f.acks("A")).toHaveLength(1); expect(f.acks("B")).toHaveLength(1);
      expect(blocked.highWater()).toBe(1); expect(receive).toHaveBeenCalledTimes(2);
    } finally { blocked.release.open(); blocked.spy.mockRestore(); }
  });

  it("9751: failed outcome save cannot enqueue or publish an ACK and retries without handler replay", async () => {
    const f = setup(), release = gate(), put = MeshStore.prototype.put;
    let failed = false;
    const acceptance = { accepted: true, messageId: "saved-once", result: { revision: 1 } };
    const saves: { ifVersion: number | undefined; acceptance: unknown }[] = [];
    const receive = vi.fn(() => acceptance);
    const save = vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
      if (this.root === f.own.root && input.key === seenKey("save") && (input.value as { acceptance?: unknown }).acceptance) {
        saves.push({ ifVersion: input.ifVersion, acceptance: (input.value as { acceptance: unknown }).acceptance });
        if (!failed) {
          failed = true; await release.promise;
          // Reject before the real put: this attempt cannot persist an outcome.
          throw Object.assign(new Error("Timed out waiting for the Fabric mesh lock"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
        }
      }
      return put.call(this, input);
    });
    const publish = vi.spyOn(f.mesh, "publish");
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("save"));
      await until(() => expect(failed).toBe(true));
      const claim = f.mesh.get(seenKey("save"), { fresh: true })!;
      expect(claim.value).not.toHaveProperty("acceptance");
      // Outcome retries use the private record's version, or create-only if none exists.
      const outcomeVersion = f.own.get(seenKey("save"), { fresh: true })?.version ?? 0;
      expect(publish.mock.calls.filter(([input]) => input.topic === "fabric.control.ack")).toHaveLength(0);
      release.open();
      await until(() => expect(f.acks("save").length).toBeGreaterThan(0));
      await f.inboundBarrier();
      expect(saves).toEqual([
        { ifVersion: outcomeVersion, acceptance }, { ifVersion: outcomeVersion, acceptance },
      ]);
      expect(f.saved("save")).toMatchObject({ acceptance });
      // The legacy reader can re-answer its retained input after the retry. Receipt
      // duplication is harmless; changing the saved outcome or re-running the handler is not.
      for (const ack of f.acks("save")) expect(ack.data).toMatchObject({ ...acceptance, commandId: "save", ownerIncarnation: f.owner.incarnation });
      expect(receive).toHaveBeenCalledOnce();
    } finally { release.open(); save.mockRestore(); publish.mockRestore(); }
  });

  it("9751: known no-write ACK failure retries the exact saved outcome without handler replay", async () => {
    const f = setup(), retry = gate(), publish = f.mesh.publish.bind(f.mesh);
    const acceptance = { accepted: true, messageId: "original", result: { revision: 1 } };
    const receive = vi.fn(() => acceptance);
    let attempts = 0, beforeFailure: unknown;
    const spy = vi.spyOn(f.mesh, "publish").mockImplementation(async input => {
      if (input.topic === "fabric.control.ack" && input.from.id === ownerId) {
        attempts++;
        if (attempts === 1) {
          beforeFailure = f.saved("retry");
          // Reject before the real publish, unlike the uncertain post-append control below.
          throw Object.assign(new Error("Timed out waiting for the Fabric mesh lock"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
        }
        if (attempts === 2) await retry.promise;
      }
      return publish(input);
    });
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("retry"));
      await until(() => expect(attempts).toBe(2)); // the existing real backoff must actually fire
      expect(f.acks("retry")).toEqual([]); // the failed attempt appended nothing
      expect(beforeFailure).toMatchObject({ acceptance });
      expect(f.saved("retry")).toEqual(beforeFailure);
      expect(receive).toHaveBeenCalledOnce();
      retry.open();
      await until(() => expect(f.acks("retry").length).toBeGreaterThan(0));
      await f.inboundBarrier();
      expect(attempts).toBe(f.acks("retry").length + 1); // exactly one no-write refusal, including legacy replay ACKs
      expect(receive).toHaveBeenCalledOnce();
      expect(f.saved("retry")).toEqual(beforeFailure);
      for (const ack of f.acks("retry")) expect(ack.data).toMatchObject({ ...acceptance, commandId: "retry", ownerIncarnation: f.owner.incarnation });
      f.owner.pause(); await f.owner.checkpointForRelease();
      expect(f.owner.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    } finally { retry.open(); spy.mockRestore(); }
  });

  it("9751: uncertain ACK completion retains the saved result and permanently refuses release", async () => {
    const f = setup(), publish = f.mesh.publish.bind(f.mesh), receive = vi.fn(() => ({ accepted: true, messageId: "uncertain", result: { private: "PRIVATE-UNCERTAIN-RESULT" } }));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    let attempts = 0;
    const spy = vi.spyOn(f.mesh, "publish").mockImplementation(async input => {
      const event = await publish(input);
      if (input.topic === "fabric.control.ack" && input.from.id === ownerId && ++attempts === 1) {
        throw new Error("ACK append completed but publication completion is unknown");
      }
      return event;
    });
    f.owner.start(receive);
    try {
      const original = f.command("uncertain"); await f.mesh.publish(original);
      await until(() => expect(attempts).toBeGreaterThanOrEqual(1));
      await expect(f.inboundBarrier()).resolves.toMatchObject({ acknowledged: true });
      expect(f.saved("uncertain")).toMatchObject({ acceptance: { accepted: true, messageId: "uncertain" } });
      const settledAttempts = attempts;
      const receiptsBeforeReplay = f.acks("uncertain").length;
      await expect(f.inboundBarrier()).resolves.toMatchObject({ acknowledged: true });
      expect(receive).toHaveBeenCalledOnce(); expect(attempts).toBe(settledAttempts); // no hot-loop
      await f.mesh.publish(original); // a later replay may explicitly retry the persisted answer
      await until(() => expect(f.acks("uncertain")).toHaveLength(receiptsBeforeReplay + 1));
      expect(receive).toHaveBeenCalledOnce();
      f.owner.pause();
      await expect(f.owner.checkpointForRelease()).rejects.toThrow(/unsettled publication obligations/);
      expect(() => f.owner.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
      const saved = f.saved("uncertain");
      await expect(settled(f.owner.close())).resolves.toBeUndefined();
      await expect(settled(f.owner.close())).resolves.toBeUndefined();
      const notices = warn.mock.calls.map(call => call.map(String).join(" ")).filter(line => line.includes("Control close has unsettled acknowledgement obligations"));
      expect(notices).toHaveLength(1);
      expect(notices[0]).toContain("ACK"); expect(notices[0]).toContain("uncertain");
      expect(notices[0]).toContain(seenKey("uncertain"));
      expect(notices[0]).not.toContain("PRIVATE-UNCERTAIN-RESULT");
      expect(f.saved("uncertain")).toEqual(saved);
      await expect(f.owner.checkpointForRelease()).rejects.toThrow(/unsettled publication obligations/);
      expect(() => f.owner.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
    } finally { spy.mockRestore(); warn.mockRestore(); }
  });

  it("9751: transient second serialization failure retains completed-result custody without handler replay", async () => {
    const f = setup();
    let serializations = 0;
    const receive = vi.fn(() => ({ accepted: true, messageId: "serialized-once", result: {
      toJSON() {
        if (++serializations === 2) throw new Error("one transient second-serialization failure");
        return { revision: 1 };
      },
    } }));
    f.owner.start(receive);
    await f.mesh.publish(f.command("second-serialization"));
    await until(() => expect(f.acks("second-serialization").length).toBeGreaterThan(0));
    await f.inboundBarrier();
    expect(serializations).toBeGreaterThan(2);
    expect(receive).toHaveBeenCalledOnce();
    expect(f.saved("second-serialization")).toMatchObject({ acceptance: {
      accepted: true, messageId: "serialized-once", result: { revision: 1 },
    } });
    for (const ack of f.acks("second-serialization")) expect(ack.data).toMatchObject({
      accepted: true, messageId: "serialized-once", result: { revision: 1 }, ownerIncarnation: f.owner.incarnation,
    });
    f.owner.pause(); await f.owner.checkpointForRelease();
    expect(f.owner.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });

  it("9751: close resolves once with a body-free pending ACK warning while checkpoint still refuses", async () => {
    const f = setup(), publish = f.mesh.publish.bind(f.mesh);
    const acceptance = { accepted: true, messageId: "PRIVATE-PENDING-MESSAGE-ID", result: { private: "PRIVATE-PENDING-RESULT" } };
    const receive = vi.fn(() => acceptance);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    let attempts = 0;
    const spy = vi.spyOn(f.mesh, "publish").mockImplementation(async input => {
      if (input.topic === "fabric.control.ack" && input.from.id === ownerId) {
        attempts++;
        throw Object.assign(new Error("Timed out waiting for the Fabric mesh lock"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
      }
      return publish(input);
    });
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("pending-close"));
      await until(() => expect(attempts).toBeGreaterThan(0));
      f.owner.pause();
      await expect(settled(f.owner.checkpointForRelease())).rejects.toThrow(/unsettled publication obligations/);
      const saved = f.saved("pending-close");
      expect(saved).toMatchObject({ acceptance, ownerIncarnation: f.owner.incarnation });
      await expect(settled(f.owner.close())).resolves.toBeUndefined();
      await expect(settled(f.owner.close())).resolves.toBeUndefined();
      const notices = warn.mock.calls.map(call => call.map(String).join(" ")).filter(line => line.includes("Control close has unsettled acknowledgement obligations"));
      expect(notices).toHaveLength(1);
      expect(notices[0]).toContain("pending ACK"); expect(notices[0]).toContain("pending-close");
      expect(notices[0]).toContain(seenKey("pending-close"));
      expect(notices[0]).not.toContain(acceptance.messageId);
      expect(notices[0]).not.toContain(acceptance.result.private);
      expect(f.saved("pending-close")).toEqual(saved); expect(f.acks("pending-close")).toEqual([]);
      expect(receive).toHaveBeenCalledOnce();
      await expect(f.owner.checkpointForRelease()).rejects.toThrow(/unsettled publication obligations/);
      expect(() => f.owner.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
    } finally { spy.mockRestore(); warn.mockRestore(); }
  });

  it("9751: queued ACK publishes the exact saved acceptance despite mutation of the handler result", async () => {
    const f = setup(), blocked = holdFirstAck(f);
    const acceptance = { accepted: true, messageId: "stable", result: { revision: 1 } };
    const receive = vi.fn(() => acceptance);
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("mutable"));
      await until(() => expect(blocked.calls()).toBe(1));
      expect(f.saved("mutable")).toMatchObject({ acceptance: { accepted: true, messageId: "stable", result: { revision: 1 } } });
      acceptance.accepted = false; acceptance.messageId = "mutated"; acceptance.result.revision = 2;
      expect(f.saved("mutable")).toMatchObject({ acceptance: { accepted: true, messageId: "stable", result: { revision: 1 } } });
      blocked.release.open();
      await until(() => expect(f.acks("mutable")).toHaveLength(1));
      expect(f.acks("mutable")[0]!.data).toMatchObject({ accepted: true, messageId: "stable", result: { revision: 1 } });
      expect(receive).toHaveBeenCalledOnce();
    } finally { blocked.release.open(); blocked.spy.mockRestore(); }
  });

  it.each([
    ["steer", "9751:predecessor"], ["followUp", "9751:predecessor"],
    ["steer", undefined], ["followUp", undefined],
  ] as const)("9751: saved restart %s replay with epoch %s cannot block fresh admission or the inbound reader", async (operation, oldEpoch) => {
    const f = setup(), blocked = holdFirstAck(f);
    const acceptance = { accepted: true, messageId: "predecessor-result", result: { revision: 7 } };
    const original = f.command("replay-A", "agent:same", operation);
    const a = { ...original, data: { ...original.data, ownerIncarnation: oldEpoch } };
    await f.own.put({ key: seenKey("replay-A"), identity: identity(ownerId), ifVersion: 0, value: {
      format: 1, hostId: ownerId, commandId: "replay-A", targetId: "agent:same",
      ...(oldEpoch === undefined ? {} : { ownerIncarnation: oldEpoch }), expiresAt: Date.now() + 60_000, acceptance,
    } });
    const persisted = f.own.get(seenKey("replay-A"), { fresh: true });
    const put = vi.spyOn(MeshStore.prototype, "put");
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    await f.mesh.publish(a);
    await f.mesh.publish(f.command("fresh-B", "agent:other", "followUp"));
    f.owner.start(receive);
    try {
      await until(() => expect(blocked.calls()).toBe(1));
      // Must pass before opening A: a saved predecessor is ACK-only work, not reader work.
      await until(() => expect(f.saved("fresh-B")).toMatchObject({ acceptance: { accepted: true, messageId: "fresh-B" } }));
      await f.mesh.publish(a); await f.mesh.publish(f.command("fresh-B", "agent:other", "followUp"));
      await expect(f.inboundBarrier()).resolves.toMatchObject({ acknowledged: true, messageId: "reader-advanced" });
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["fresh-B"]);
      expect(f.acks()).toEqual([]); expect(blocked.calls()).toBe(1);
      expect(f.mesh.get(seenKey("replay-A"), { fresh: true })).toBeUndefined(); // no shared re-claim
      expect(put.mock.calls.some(([input]) => input.key === seenKey("replay-A"))).toBe(false); // no local re-save
      expect(f.own.get(seenKey("replay-A"), { fresh: true })).toEqual(persisted);
      blocked.release.open();
      await until(() => expect(f.acks()).toHaveLength(2));
      await f.inboundBarrier();
      expect(f.acks().map(event => (event.data as { commandId: string }).commandId)).toEqual(["replay-A", "fresh-B"]);
      expect(f.acks("replay-A")[0]!.data).toEqual({ version: 1, commandId: "replay-A", targetId: "agent:same",
        ...acceptance, ...(oldEpoch === undefined ? {} : { ownerIncarnation: oldEpoch }) });
      expect(f.acks("fresh-B")[0]!.data).toMatchObject({ ownerIncarnation: f.owner.incarnation });
      expect(f.own.get(seenKey("replay-A"), { fresh: true })).toEqual(persisted);
      expect(blocked.calls()).toBe(2); expect(blocked.highWater()).toBe(1); expect(receive).toHaveBeenCalledOnce();
    } finally { blocked.release.open(); put.mockRestore(); blocked.spy.mockRestore(); }
  });

  it("9751: saved restart replay shares the 100 ACK cap and leaves the next fresh command unread and unclaimed", async () => {
    const f = setup(), blocked = holdFirstAck(f), oldEpoch = "9751:predecessor";
    for (let index = 1; index <= 99; index++) {
      const id = `replay-cap:${index}`, command = f.command(id, "agent:same", index % 2 ? "steer" : "followUp");
      await f.own.put({ key: seenKey(id), identity: identity(ownerId), ifVersion: 0, value: {
        format: 1, hostId: ownerId, commandId: id, targetId: "agent:same", ownerIncarnation: oldEpoch,
        expiresAt: Date.now() + 60_000, acceptance: { accepted: true, messageId: id },
      } });
      await f.mesh.publish({ ...command, data: { ...command.data, ownerIncarnation: oldEpoch } });
    }
    const hundredth = await f.mesh.publish(f.command("replay-cap:100", "agent:other"));
    const next = await f.mesh.publish(f.command("replay-cap:101", "agent:other"));
    const tail = vi.spyOn(f.mesh, "tail"), drain = vi.spyOn(FileControlReader.prototype, "drain");
    vi.spyOn(fs, "watch").mockImplementation(() =>
      Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    f.owner.start(receive);
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(blocked.calls()).toBe(1);
      // 99 saved replays plus this fresh save prove the whole first page reached the SAME cap.
      expect(f.saved("replay-cap:100")).toMatchObject({ acceptance: { accepted: true } });
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["replay-cap:100"]);
      // The real call-through reader must commit the command that fills slot 100,
      // not strand it behind a post-handler capacity check or commit the next input.
      const reader = drain.mock.contexts[0] as FileControlReader;
      expect(reader.mesh).toBe(f.mesh);
      expect(reader.lastSequence).toBe(hundredth.sequence);
      expect(f.mesh.get(seenKey("replay-cap:101"), { fresh: true })).toBeUndefined();
      expect(f.saved("replay-cap:101")).toBeUndefined();
      // Strict pre-tail-page check: neither reading page two nor claiming its command is allowed.
      expect(tail.mock.results.some(result => result.type === "return" && result.value.events.some(
        (event: MeshEvent) => event.topic === "fabric.control.command" && (event.data as FabricControlCommand).commandId === "replay-cap:101"))).toBe(false);
      const readsAtCapacity = tail.mock.calls.length, timersAtCapacity = timeouts.mock.calls.length;
      const armedAtCapacity = vi.getTimerCount();
      await vi.advanceTimersByTimeAsync(30_000);
      expect(tail).toHaveBeenCalledTimes(readsAtCapacity);
      expect(timeouts).toHaveBeenCalledTimes(timersAtCapacity); expect(vi.getTimerCount()).toBe(armedAtCapacity);
      expect(blocked.calls()).toBe(1);
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["replay-cap:100"]);
      expect(reader.lastSequence).toBe(hundredth.sequence);
      blocked.release.open();
      await vi.advanceTimersByTimeAsync(0);
      expect(f.acks()).toHaveLength(101);
      expect(f.acks().map(event => (event.data as { commandId: string }).commandId)).toEqual(
        Array.from({ length: 101 }, (_, index) => `replay-cap:${index + 1}`));
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["replay-cap:100", "replay-cap:101"]);
      expect(reader.lastSequence).toBeGreaterThanOrEqual(next.sequence);
      expect(f.saved("replay-cap:101")).toMatchObject({ acceptance: { accepted: true }, ownerIncarnation: f.owner.incarnation });
      expect(blocked.calls()).toBe(101); expect(blocked.highWater()).toBe(1);
    } finally {
      blocked.release.open();
      // Restore the timeout spy under the fake clock, before returning to real timers.
      vi.restoreAllMocks(); vi.useRealTimers();
    }
  }, 15_000);

  it("9751: full FILE ACK custody consumes same-page ACK, cancel and owned replay but retains the next new command", async () => {
    const f = setup(), blocked = holdFirstAck(f), asking = gate();
    let askSignal: AbortSignal | undefined, acknowledged = false;
    const request = f.owner.request("host:9751-peer", "agent:peer", "steer", {
      ownerIncarnation: fixtureOwnerIncarnation, message: "capacity ACK barrier",
    }, "host:9751-peer");
    void request.then(() => { acknowledged = true; }, () => undefined);
    await until(() => expect(f.mesh.read({ topic: "fabric.control.command", limit: 1_000 })
      .some(event => event.from.id === ownerId && (event.data as FabricControlCommand).message === "capacity ACK barrier")).toBe(true));
    const outbound = f.mesh.read({ topic: "fabric.control.command", limit: 1_000 })
      .find(event => event.from.id === ownerId && (event.data as FabricControlCommand).message === "capacity ACK barrier")!.data as FabricControlCommand;
    const ask = f.command("same-page:ask");
    await f.mesh.publish({ ...ask, kind: "ask", data: { ...ask.data, operation: "ask" } });
    // Outbound barrier + detached ASK + 98 ACK obligations occupy page one.
    // Page two enters below capacity, then fills it before the exempt events.
    for (let index = 1; index <= 100; index++) await f.mesh.publish(f.command(`same-page:${index}`));
    const ack = await f.mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("host:9751-peer"), to: ownerId,
      data: { version: 1, commandId: outbound.commandId, targetId: outbound.targetId, ownerIncarnation: fixtureOwnerIncarnation,
        accepted: true, messageId: "same-page-ack" } });
    const cancel = f.command("same-page:cancel");
    const cancellation = await f.mesh.publish({ ...cancel, kind: "cancel", data: {
      ...cancel.data, operation: "cancel", cancelCommandId: "same-page:ask",
    } });
    const owned = await f.mesh.publish(f.command("same-page:100"));
    const next = await f.mesh.publish(f.command("same-page:101"));
    const tail = vi.spyOn(f.mesh, "tail"), drain = vi.spyOn(FileControlReader.prototype, "drain");
    vi.spyOn(fs, "watch").mockImplementation(() =>
      Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const receive = vi.fn((command: FabricControlCommand, _from: MeshIdentity, signal: AbortSignal) => {
      if (command.operation === "ask") {
        askSignal = signal;
        return asking.promise.then(() => ({ accepted: true, messageId: command.commandId }));
      }
      return { accepted: true, messageId: command.commandId };
    });
    f.owner.start(receive);
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(f.saved("same-page:100")).toMatchObject({ acceptance: { accepted: true } });
      expect(blocked.calls()).toBe(1);
      expect(f.acks()).toEqual([]);
      // Positive public effects: the actual ACK verifier settled a pending request,
      // and the actual cancellation path aborted the still-running ASK handler.
      expect(acknowledged).toBe(true);
      await expect(request).resolves.toMatchObject({ acknowledged: true, messageId: "same-page-ack" });
      expect(askSignal).toBeDefined(); expect(askSignal!.aborted).toBe(true);
      const reader = drain.mock.contexts[0] as FileControlReader;
      expect(reader.mesh).toBe(f.mesh);
      expect(reader.lastSequence).toBe(owned.sequence);
      expect(tail.mock.results.some(result => result.type === "return" &&
        [ack.id, cancellation.id, owned.id, next.id].every(id => result.value.events.some((event: MeshEvent) => event.id === id)))).toBe(true);
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual([
        "same-page:ask", ...Array.from({ length: 100 }, (_, index) => `same-page:${index + 1}`),
      ]);
      expect(f.mesh.get(seenKey("same-page:101"), { fresh: true })).toBeUndefined();
      expect(f.saved("same-page:101")).toBeUndefined();
      blocked.release.open();
      await vi.advanceTimersByTimeAsync(0);
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual([
        "same-page:ask", ...Array.from({ length: 101 }, (_, index) => `same-page:${index + 1}`),
      ]);
      expect(f.saved("same-page:101")).toMatchObject({ acceptance: { accepted: true } });
      expect(f.acks()).toHaveLength(101);
      expect(f.acks("same-page:100")).toHaveLength(1);
      expect(reader.lastSequence).toBeGreaterThanOrEqual(next.sequence);
      expect(blocked.calls()).toBe(101); expect(blocked.highWater()).toBe(1);
    } finally {
      asking.open(); blocked.release.open();
      vi.restoreAllMocks(); vi.useRealTimers();
    }
  }, 15_000);

  it("9751: queued saved restart replay owns no retry timer while the existing ACK worker is blocked", async () => {
    const f = setup(), blocked = holdFirstAck(f), oldEpoch = "9751:predecessor";
    for (const id of ["timer-A", "timer-C"]) {
      const command = f.command(id);
      await f.own.put({ key: seenKey(id), identity: identity(ownerId), ifVersion: 0, value: {
        format: 1, hostId: ownerId, commandId: id, targetId: "agent:same", ownerIncarnation: oldEpoch,
        expiresAt: Date.now() + 60_000, acceptance: { accepted: true, messageId: id },
      } });
      await f.mesh.publish({ ...command, data: { ...command.data, ownerIncarnation: oldEpoch } });
    }
    await f.mesh.publish(f.command("timer-B", "agent:other"));
    vi.spyOn(fs, "watch").mockImplementation(() =>
      Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() }) as unknown as fs.FSWatcher);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const timeouts = vi.spyOn(globalThis, "setTimeout"), tail = vi.spyOn(f.mesh, "tail");
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    f.owner.start(receive);
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(f.saved("timer-B")).toMatchObject({ acceptance: { accepted: true } });
      expect(blocked.calls()).toBe(1);
      const reads = tail.mock.calls.length, timers = timeouts.mock.calls.length, armed = vi.getTimerCount();
      await vi.advanceTimersByTimeAsync(30_000);
      expect(tail).toHaveBeenCalledTimes(reads); expect(timeouts).toHaveBeenCalledTimes(timers);
      expect(vi.getTimerCount()).toBe(armed); expect(blocked.calls()).toBe(1);
      blocked.release.open(); await vi.advanceTimersByTimeAsync(0);
      expect(f.acks().map(event => (event.data as { commandId: string }).commandId)).toEqual(["timer-A", "timer-C", "timer-B"]);
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["timer-B"]);
      expect(blocked.highWater()).toBe(1);
    } finally { blocked.release.open(); vi.restoreAllMocks(); vi.useRealTimers(); }
  });

  it.each([
    ["steer", false], ["followUp", false], ["ask", true], ["stop", true],
  ] as const)("9751: restart %s with saved=%s stays inline and blocks later admission", async (operation, saved) => {
    const f = setup(), blocked = holdFirstAck(f), oldEpoch = "9751:predecessor";
    const original = f.command("inline-A");
    const a = { ...original, kind: operation, data: { ...original.data, operation, ownerIncarnation: oldEpoch } };
    const acceptance = { accepted: true, messageId: "inline-predecessor-result" };
    await f.own.put({ key: seenKey("inline-A"), identity: identity(ownerId), ifVersion: 0, value: {
      format: 1, hostId: ownerId, commandId: "inline-A", targetId: "agent:same", ownerIncarnation: oldEpoch,
      expiresAt: Date.now() + 60_000, ...(saved ? { acceptance } : {}),
    } });
    const persisted = f.own.get(seenKey("inline-A"), { fresh: true });
    await f.mesh.publish(a); await f.mesh.publish(f.command("inline-B", "agent:other"));
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    f.owner.start(receive);
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(blocked.calls()).toBe(1); expect(receive).not.toHaveBeenCalled();
      expect(f.mesh.get(seenKey("inline-B"), { fresh: true })).toBeUndefined();
      expect(f.saved("inline-B")).toBeUndefined(); expect(f.acks()).toEqual([]);
      blocked.release.open(); await vi.advanceTimersByTimeAsync(0);
      expect(f.acks()).toHaveLength(2);
      const ack = f.acks("inline-A")[0]!.data;
      expect(ack).toEqual({ version: 1, commandId: "inline-A", targetId: "agent:same", ownerIncarnation: oldEpoch,
        ...(saved ? acceptance : { accepted: false, error: "Fabric control outcome is indeterminate after owner restart" }) });
      expect(ack).not.toHaveProperty("notRun");
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["inline-B"]);
      expect(f.own.get(seenKey("inline-A"), { fresh: true })).toEqual(persisted);
    } finally { blocked.release.open(); vi.restoreAllMocks(); vi.useRealTimers(); }
  });

  it.each(["local", "legacy-shared"] as const)("9751: cleanup after compaction protects an already-saved %s restart replay while its ACK is owned", async source => {
    const f = setup({}, { maxEventLogBytes: 80_000, retainedEventLogBytes: 40_000 }), blocked = holdFirstAck(f);
    const oldEpoch = "9751:predecessor", original = f.command("replay-protected"), now = Date.now;
    const store = source === "local" ? f.own : f.mesh;
    const acceptance = { accepted: true, messageId: "replay-protected-result" };
    await store.put({ key: seenKey("replay-protected"), identity: identity(ownerId), ifVersion: 0, value: {
      format: 1, hostId: ownerId, commandId: "replay-protected", targetId: "agent:same", ownerIncarnation: oldEpoch,
      sequence: 1, expiresAt: Date.now() + 60_000, acceptance,
    } });
    const persisted = store.get(seenKey("replay-protected"), { fresh: true });
    await f.mesh.publish({ ...original, data: { ...original.data, ownerIncarnation: oldEpoch } });
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    f.owner.start(receive);
    try {
      await until(() => expect(blocked.calls()).toBe(1));
      await store.put({ key: seenKey("replay-sweep-witness"), identity: identity(ownerId), ifVersion: 0, value: {
        format: 1, hostId: ownerId, commandId: "replay-sweep-witness", targetId: "agent:other", sequence: 1,
        expiresAt: 0, acceptance: { accepted: true },
      } });
      vi.spyOn(Date, "now").mockImplementation(() => now() + 16 * 60_000);
      for (let index = 0; index < 100; index++) await f.mesh.publish({ topic: "9751.compact", from: identity(senderId), text: "x".repeat(900) });
      expect(f.mesh.oldestSequence()).toBeGreaterThan(1);
      await f.mesh.publish(f.command("replay-sweep-trigger", "agent:other", "followUp"));
      await until(() => {
        expect(f.saved("replay-sweep-trigger")).toMatchObject({ acceptance: { accepted: true } });
        expect(store.get(seenKey("replay-sweep-witness"), { fresh: true })).toBeUndefined();
      });
      expect(store.get(seenKey("replay-protected"), { fresh: true })).toEqual(persisted);
      expect(blocked.calls()).toBe(1);
      blocked.release.open(); await until(() => expect(f.acks("replay-protected")).toHaveLength(1));
      expect(f.acks("replay-protected")[0]!.data).toMatchObject({ ...acceptance, ownerIncarnation: oldEpoch });
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["replay-sweep-trigger"]);
    } finally { blocked.release.open(); vi.restoreAllMocks(); }
  }, 15_000);

  it("9751: restart replays a saved predecessor outcome without adopting the new owner epoch", async () => {
    const f = setup(), oldEpoch = "9751:predecessor", command = f.command("restart-saved");
    command.data.ownerIncarnation = oldEpoch;
    await f.own.put({ key: seenKey(command.data.commandId), identity: identity(ownerId), ifVersion: 0,
      value: { format: 1, hostId: ownerId, commandId: command.data.commandId, targetId: "agent:same", ownerIncarnation: oldEpoch,
        expiresAt: Date.now() + 60_000, acceptance: { accepted: true, messageId: "predecessor-result" } } });
    await f.mesh.publish(command);
    const receive = vi.fn(() => ({ accepted: true })); f.owner.start(receive);
    await until(() => expect(f.acks(command.data.commandId)).toHaveLength(1));
    expect(f.acks(command.data.commandId)[0]!.data).toMatchObject({ accepted: true, messageId: "predecessor-result", ownerIncarnation: oldEpoch });
    expect(receive).not.toHaveBeenCalled();
  });

  it("9751: restart treats a predecessor claim without an outcome as indeterminate, never notRun", async () => {
    const f = setup(), oldEpoch = "9751:predecessor", command = f.command("restart-claim");
    command.data.ownerIncarnation = oldEpoch;
    await f.own.put({ key: seenKey(command.data.commandId), identity: identity(ownerId), ifVersion: 0,
      value: { format: 1, hostId: ownerId, commandId: command.data.commandId, targetId: "agent:same", ownerIncarnation: oldEpoch,
        expiresAt: Date.now() + 60_000 } });
    await f.mesh.publish(command);
    const receive = vi.fn(() => ({ accepted: true })); f.owner.start(receive);
    await until(() => expect(f.acks(command.data.commandId)).toHaveLength(1));
    const ack = f.acks(command.data.commandId)[0]!.data;
    expect(ack).toMatchObject({ accepted: false, ownerIncarnation: oldEpoch, error: "Fabric control outcome is indeterminate after owner restart" });
    expect(ack).not.toHaveProperty("notRun"); expect(receive).not.toHaveBeenCalled();
  });

  it("9751: cleanup after compaction protects outcomes whose ACK is still owned", async () => {
    const f = setup({}, { maxEventLogBytes: 80_000, retainedEventLogBytes: 40_000 }), blocked = holdFirstAck(f);
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    const now = Date.now;
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("protected"));
      await until(() => expect(blocked.calls()).toBe(1));
      expect(f.saved("protected")).toMatchObject({ acceptance: { accepted: true } });
      // This unowned expired row is a positive sweep witness; the owned row must survive the same sweep.
      await f.own.put({ key: seenKey("sweep-witness"), identity: identity(ownerId), ifVersion: 0, value: {
        format: 1, hostId: ownerId, commandId: "sweep-witness", targetId: "agent:other", sequence: 1,
        expiresAt: 0, acceptance: { accepted: true },
      } });
      vi.spyOn(Date, "now").mockImplementation(() => now() + 16 * 60_000);
      for (let index = 0; index < 100; index++) await f.mesh.publish({ topic: "9751.compact", from: identity(senderId), text: "x".repeat(900) });
      expect(f.mesh.oldestSequence()).toBeGreaterThan(1);
      await f.mesh.publish(f.command("sweep-trigger", "agent:other", "followUp"));
      await until(() => {
        expect(f.saved("sweep-trigger")).toMatchObject({ acceptance: { accepted: true } });
        expect(f.saved("sweep-witness")).toBeUndefined();
      });
      expect(f.saved("protected")).toMatchObject({ acceptance: { accepted: true, messageId: "protected" } });
      expect(blocked.calls()).toBe(1);
      blocked.release.open();
      await until(() => expect(f.acks("protected")).toHaveLength(1));
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["protected", "sweep-trigger"]);
    } finally { blocked.release.open(); vi.restoreAllMocks(); }
  }, 15_000);

  it("9751: lease loss at the actual ACK commit fence preserves custody until consumption resumes", async () => {
    let canConsume = true;
    const f = setup({ canConsumeMesh: () => canConsume }), publish = f.mesh.publish.bind(f.mesh);
    const receive = vi.fn(() => ({ accepted: true, messageId: "lease-saved" }));
    let fenced = false;
    const spy = vi.spyOn(f.mesh, "publish").mockImplementation(input => publish({ ...input,
      data: typeof input.data !== "function" ? input.data : (at: number) => {
        if (input.topic === "fabric.control.ack" && input.from.id === ownerId && !fenced) {
          fenced = true; canConsume = false;
        }
        return (input.data as (at: number) => unknown)(at); // real callback under the event writer lock
      },
    }));
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("lease"));
      await until(() => expect(fenced).toBe(true));
      expect(f.acks("lease")).toEqual([]);
      expect(f.saved("lease")).toMatchObject({ acceptance: { accepted: true, messageId: "lease-saved" } });
      f.owner.pause();
      await expect(f.owner.checkpointForRelease()).rejects.toThrow(/unsettled publication obligations/);
      canConsume = true; f.owner.resume();
      await until(() => expect(f.acks("lease")).toHaveLength(1));
      expect(receive).toHaveBeenCalledOnce();
      f.owner.pause(); await f.owner.checkpointForRelease();
      expect(f.owner.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    } finally { canConsume = true; spy.mockRestore(); }
  });

  it("9751: pause checkpoint joins a saved ACK instead of certifying a quiet handler", async () => {
    const f = setup(), blocked = holdFirstAck(f), receive = vi.fn(() => ({ accepted: true, messageId: "pause-saved" }));
    let checkpointSettled = false;
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("pause"));
      await until(() => expect(blocked.calls()).toBe(1));
      expect(f.saved("pause")).toMatchObject({ acceptance: { accepted: true } });
      f.owner.pause();
      const checkpoint = f.owner.checkpointForRelease();
      void checkpoint.then(() => { checkpointSettled = true; }, () => { checkpointSettled = true; });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(checkpointSettled).toBe(false);
      expect(() => f.owner.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
      blocked.release.open(); await settled(checkpoint);
      expect(f.acks("pause")).toHaveLength(1);
      expect(f.owner.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
      expect(receive).toHaveBeenCalledOnce();
    } finally { blocked.release.open(); blocked.spy.mockRestore(); }
  });

  it("9751: CLOSE JOIN drains queued ACKs across an awaited-save worker-generation handoff", async () => {
    for (const handoff of ["after-worker-settlement", "at-worker-completion"] as const) {
    const f = setup(), firstAck = gate(), newcomerAck = gate(), saveReturn = gate();
    const publish = f.mesh.publish.bind(f.mesh), put = MeshStore.prototype.put;
    const receive = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    let attempts = 0, saveHeld = false, closeSettled = false;
    const publication = vi.spyOn(f.mesh, "publish").mockImplementation(async input => {
      let attempt = 0;
      if (input.topic === "fabric.control.ack" && input.from.id === ownerId) {
        attempt = ++attempts;
        if (attempt === 1) await firstAck.promise;
        if (attempt === 4) await newcomerAck.promise;
      }
      const event = await publish(input);
      // Second schedule: D's awaited save returns exactly as C's ACK publication returns.
      // Reader handoff and old-worker completion now compete in adjacent promise continuations.
      if (attempt === 3 && handoff === "at-worker-completion") saveReturn.open();
      return event;
    });
    const saving = vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
      const result = await put.call(this, input); // real saved acceptance exists before this return gate
      if (this.root === f.own.root && input.key === seenKey("D") &&
          (input.value as { acceptance?: unknown }).acceptance && !saveHeld) {
        saveHeld = true; await saveReturn.promise;
      }
      return result;
    });
    f.owner.start(receive);
    try {
      await f.mesh.publish(f.command("A"));
      await until(() => expect(attempts).toBe(1));
      for (const id of ["B", "C"]) {
        await f.mesh.publish(f.command(id, "agent:same", "followUp"));
        await until(() => expect(f.saved(id)).toMatchObject({ acceptance: { accepted: true, messageId: id } }));
      }
      expect(attempts).toBe(1); // A active plus two queued ACKs, not merely a single blocked handler
      await f.mesh.publish(f.command("D"));
      await until(() => expect(saveHeld).toBe(true));
      expect(f.saved("D")).toMatchObject({ acceptance: { accepted: true, messageId: "D" } });
      const close = f.owner.close();
      void close.then(() => { closeSettled = true; }, () => { closeSettled = true; });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(closeSettled).toBe(false);
      firstAck.open();
      await until(() => expect(f.acks().map(event => (event.data as { messageId: string }).messageId)).toEqual(["A", "B", "C"]));
      if (handoff === "after-worker-settlement") {
        // First schedule: finish the old worker while D still awaits its real save return.
        // D then requires a newcomer generation, not another job on the original worker.
        await new Promise<void>(resolve => setImmediate(resolve));
        expect(attempts).toBe(3); expect(closeSettled).toBe(false);
        saveReturn.open();
      }
      await until(() => expect(attempts).toBe(4));
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(closeSettled).toBe(false); // rejecting for leftover custody is not a successful CLOSE JOIN
      expect(f.acks("D")).toEqual([]);
      newcomerAck.open();
      await expect(settled(close)).resolves.toBeUndefined();
      expect(f.acks().map(event => (event.data as { messageId: string }).messageId)).toEqual(["A", "B", "C", "D"]);
      expect(receive.mock.calls.map(([command]) => command.commandId)).toEqual(["A", "B", "C", "D"]);
      expect(attempts).toBe(4);
    } finally { firstAck.open(); newcomerAck.open(); saveReturn.open(); saving.mockRestore(); publication.mockRestore(); }
    }
  }, 15_000);
});

// Keep reason: raw control admission is a pre-publication trust boundary, including
// a lease lost while waiting for the actual event lock. Real store/watch; short real timer seam.
describe("raw mirrored control freshness admission", () => {
  const setup = (state: "live" | "legacy" | "stale" | "absent" | "translated-expired" | "source-lapsed") => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-control-freshness-")); roots.push(root);
    const at = Date.now();
    const live = (): MirroredControlOwner => ({ remoteHost: "forge", expiresAt: Date.now() + 60_000,
      ownerIncarnation: fixtureOwnerIncarnation, mirrorLease: 1, mirror: { remoteHost: "forge", observedAt: Date.now(),
        sourceObservedAt: at - 60_000, sourceUpdatedAt: at - 62_000, sourceExpiresAt: at - 30_000 } });
    let current = live();
    if (state === "legacy") current = { remoteHost: "forge", expiresAt: at + 60_000 };
    if (state === "stale") current.mirror = { ...current.mirror!, observedAt: at - 16_000,
      sourceExpiresAt: at - 60_001 };
    if (state === "absent") delete current.mirror;
    if (state === "translated-expired") current.expiresAt = at - 1;
    if (state === "source-lapsed") current.mirror = { ...current.mirror!, sourceExpiresAt: at - 60_001 };
    const read = vi.fn(() => current);
    const sender = plane(root, "sender", {}, { readMirroredOwner: read, mirrorLeaseWaitMs: 50 });
    sender.start(() => ({ accepted: false }));
    const send = (signal?: AbortSignal, budget?: { waited?: boolean }, key?: string) => sender.request("owner", "target", "followUp",
      { ownerIncarnation: fixtureOwnerIncarnation, message: "one attempt" }, "owner",
      { routedRemoteHost: "forge", ...(signal ? { signal } : {}),
        ...(budget ? { mirrorLeaseWaitBudget: budget } : {}), ...(key ? { idempotencyKey: key } : {}) });
    const commands = () => sender.mesh.read({ topic: "fabric.control.command" });
    const renew = (patch: Partial<MirroredControlOwner> = {}) => {
      current = { ...live(), ...patch };
      writeHostLease(root, { id: "owner", rootId: "target", identityId: "owner", updatedAt: Date.now(),
        expiresAt: current.expiresAt, ...(current.mirror ? { mirror: current.mirror } : {}) });
    };
    const ack = async () => {
      await vi.waitFor(() => expect(commands()).toHaveLength(1));
      const command = commands()[0]!.data as FabricControlCommand;
      await sender.mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("owner"), to: "sender",
        data: { version: 1, commandId: command.commandId, targetId: "target", accepted: true,
          ownerIncarnation: fixtureOwnerIncarnation, messageId: "once", bridge: { from: "forge" } } });
    };
    return { sender, send, commands, renew, ack, read,
      doubt: () => { const { mirror: _mirror, ...rest } = live(); current = rest; } };
  };

  it.each(["live", "legacy"] as const)("%s unexpired authority delivers without a renewal wait", async state => {
    const f = setup(state);
    const request = f.send();
    void request.catch(() => undefined);
    await f.ack();
    await expect(request).resolves.toMatchObject({ acknowledged: true, messageId: "once" });
    expect(f.commands()).toHaveLength(1);
  });

  it.each(["stale", "absent", "translated-expired"] as const)("%s authority waits once then refuses peer-stale before publication", async state => {
    const f = setup(state), start = Date.now();
    await expect(f.send()).rejects.toMatchObject({ code: "FABRIC_PEER_LEASE_STALE", retryable: true, remoteHost: "forge" });
    expect(Date.now() - start).toBeGreaterThanOrEqual(40);
    expect(Date.now() - start).toBeLessThan(2_000);
    expect(f.commands()).toEqual([]);
  });

  it("fresh source-lapsed evidence is immediately Unknown, even with future projection", async () => {
    const f = setup("source-lapsed"), start = Date.now();
    await expect(f.send()).rejects.toThrow("Unknown Fabric participant: target (its lease mirrored from remote host forge lapsed");
    expect(Date.now() - start).toBeLessThan(1_000);
    expect(f.read).toHaveBeenCalledOnce();
    expect(f.commands()).toEqual([]);
  });

  it("same-owner renewal publishes once, but a shared spent budget never grants a second wait", async () => {
    const f = setup("absent");
    const request = f.send();
    void request.catch(() => undefined);
    expect(f.commands()).toEqual([]);
    f.renew();
    await f.ack();
    await expect(request).resolves.toMatchObject({ acknowledged: true });
    expect(f.commands()).toHaveLength(1);

    const raced = setup("absent"), start = Date.now();
    await expect(raced.send(undefined, { waited: true })).rejects.toMatchObject({ code: "FABRIC_PEER_LEASE_STALE" });
    expect(Date.now() - start).toBeLessThan(1_000);
    expect(raced.commands()).toEqual([]);
  });

  it.each([false, true])("a commit-time lapse shares the original wait budget (already waited=%s), never appending stale authority", async waited => {
    const f = setup("live");
    const publish = f.sender.mesh.publish.bind(f.sender.mesh);
    let first = true, armed!: () => void;
    const atCommit = new Promise<void>(resolve => { armed = resolve; });
    const spy = vi.spyOn(f.sender.mesh, "publish").mockImplementation(input => publish({ ...input,
      data: typeof input.data !== "function" ? input.data : (at: number) => {
        if (input.topic === "fabric.control.command" && first) {
          first = false; f.doubt(); armed(); // Actual commit callback, not a preflight-only seam.
        }
        return (input.data as (at: number) => unknown)(at);
      } }));
    try {
      const request = f.send(undefined, { waited });
      const result = request.catch((error: unknown) => error);
      await atCommit;
      expect(f.commands()).toEqual([]);
      if (waited) {
        expect(await result).toMatchObject({ code: "FABRIC_PEER_LEASE_STALE", retryable: true });
        expect(f.commands()).toEqual([]);
        expect(spy).toHaveBeenCalledOnce(); // No second wait or publication attempt.
      } else {
        f.renew();
        await f.ack();
        await expect(request).resolves.toMatchObject({ acknowledged: true });
        expect(f.commands()).toHaveLength(1);
        expect(spy.mock.calls.filter(([input]) => input.topic === "fabric.control.command")).toHaveLength(2);
      }
    } finally { spy.mockRestore(); }
  });

  it("abort during raw admission preserves the exact reason and leaves nothing published", async () => {
    const f = setup("absent"), controller = new AbortController();
    const request = f.send(controller.signal);
    const reason = { cancelled: "raw admission" };
    const refusal = expect(request).rejects.toBe(reason);
    controller.abort(reason);
    await refusal;
    expect(f.commands()).toEqual([]);
  });

  it.each(["close", "epoch", "duplicate"] as const)("rechecks %s after a renewal await", async variant => {
    const f = setup("absent");
    const request = f.send(undefined, undefined, "same-key");
    const result = request.catch((error: unknown) => error);
    let duplicate: Promise<unknown> | undefined;
    if (variant === "close") await f.sender.close();
    if (variant === "duplicate") duplicate = f.send(undefined, undefined, "same-key").catch((error: unknown) => error);
    f.renew(variant === "epoch" ? { ownerIncarnation: "replacement-activation" } : {});
    if (variant === "duplicate") {
      await f.ack();
      const errors = await Promise.all([result, duplicate!]);
      expect(errors.filter(error => error instanceof Error && /already awaiting acknowledgement/.test(error.message))).toHaveLength(1);
      expect(f.commands()).toHaveLength(1);
    } else {
      const error = await result;
      expect((error as Error).message).toMatch(variant === "close" ? /closed/ : /incarnation/i);
      expect(f.commands()).toEqual([]);
    }
  });
});

describe("sender final delivery outcomes", () => {
  const outcomes = (root: string) => {
    const directory = path.join(root, "delivery-outcomes");
    return fs.existsSync(directory) ? fs.readdirSync(directory).flatMap(file =>
      fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").map(line => JSON.parse(line))) : [];
  };
  it.each(["Unknown Fabric participant: missing", "Main's followUp queue is full"])("records an owner rejection without notRun (%s) as unknown: its handler may have run", async reason => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-send-rejected-")); roots.push(root);
    const owner = plane(root, "host:owner"), sender = plane(root, "host:sender");
    owner.start(() => ({ accepted: false, error: reason })); sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:owner", "session:target", "followUp", { message: "later" })).rejects.toThrow(reason);
    const event = sender.mesh.read({ topic: "fabric.control.command" })[0]!;
    expect(outcomes(root)).toEqual([{ eventId: event.id, to: "session:target", from: "host:sender", mode: "followUp", outcome: "unknown", reason, at: expect.any(Number) }]);
  });
  it.each(["Unknown Fabric participant: missing", "Main's followUp queue is full"])("records an owner rejection with notRun: true (%s) as failed", async reason => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-send-failed-")); roots.push(root);
    const owner = plane(root, "host:owner"), sender = plane(root, "host:sender");
    owner.start(() => ({ accepted: false, error: reason, notRun: true })); sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:owner", "session:target", "followUp", { message: "later" })).rejects.toThrow(reason);
    // A proven-notRun message is resent once; each published attempt failed.
    const events = sender.mesh.read({ topic: "fabric.control.command" });
    expect(events.length).toBeGreaterThan(0);
    expect(outcomes(root)).toEqual(events.map(event => ({ eventId: event.id, to: "session:target", from: "host:sender", mode: "followUp", outcome: "failed", reason, at: expect.any(Number) })));
  });
  it("records an unclaimed command withdrawn at its deadline as failed (smarty-dev#6729)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-send-withdrawn-")); roots.push(root);
    const sender = plane(root, "host:sender", {}, { acknowledgementTimeoutMs: 30, pollMs: 5 });
    sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:missing", "session:target", "steer", { message: "hello" })).rejects.toThrow("now withdrawn");
    const event = sender.mesh.read({ topic: "fabric.control.command" })[0]!;
    expect(outcomes(root)).toEqual([{ eventId: event.id, to: "session:target", from: "host:sender", mode: "steer", outcome: "failed", reason: expect.stringContaining("not delivered"), at: expect.any(Number) }]);
  });
  it("records timeout unknown without falsely treating admission as delivered", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-send-unknown-")); roots.push(root);
    const owner = plane(root, "host:owner", {}, { pollMs: 5 }), sender = plane(root, "host:sender", {}, { acknowledgementTimeoutMs: 30, pollMs: 5 });
    // Claimed, never acknowledged before the sender gives up; released after the assertion.
    let release: (() => void) | undefined;
    owner.start(() => new Promise(resolve => { release = () => resolve({ accepted: false }); }));
    sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:owner", "session:target", "steer", { message: "hello" })).rejects.toThrow("outcome is unknown");
    release?.();
    const event = sender.mesh.read({ topic: "fabric.control.command" })[0]!;
    expect(outcomes(root)).toEqual([{ eventId: event.id, to: "session:target", from: "host:sender", mode: "steer", outcome: "unknown", reason: expect.stringContaining("outcome is unknown"), at: expect.any(Number) }]);
  });
});

describe("FabricControlPlane", () => {
  it("delivers ordinary control to older owners unfenced with one counted warning per request", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sender-incarnation-")); roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender");
    sender.start(() => ({ accepted: false }));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      for (const [index, operation] of (["steer", "followUp", "ask", "stop"] as const).entries()) {
        const input = { message: "legacy delivery", data: { ownerIncarnation: "payload-forgery" } };
        const pending = operation === "ask"
          ? sender.requestResult("host:older", "actor:target", operation, input)
          : sender.request("host:older", "actor:target", operation, input);
        await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(index + 1));
        const command = sender.mesh.read({ topic: "fabric.control.command" }).at(-1)!.data as FabricControlCommand;
        expect(command).not.toHaveProperty("ownerIncarnation");
        expect(command.data).toEqual(input.data);
        // The older release's handler and ACK know nothing about incarnation fields.
        await sender.mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("host:older"), to: "host:sender",
          data: { version: 1, commandId: command.commandId, targetId: command.targetId, accepted: true, messageId: "legacy", result: "answer" } });
        expect(await pending).toEqual(operation === "ask" ? "answer" : { queued: true, routed: "mesh", acknowledged: true, messageId: "legacy" });
        expect(warn).toHaveBeenCalledTimes(index + 1);
        expect(warn.mock.calls[index]![0]).toBe(`[pi-fabric] Unfenced legacy control delivery count=${index + 1} operation=${operation} target="actor:target"`);
      }
    } finally { warn.mockRestore(); }
  });

  it("counts one legacy warning even when a proven-notRun receipt causes a bounded resend", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-legacy-resend-")); roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender");
    sender.start(() => ({ accepted: false }));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const pending = sender.request("host:older", "actor:target", "followUp", { message: "legacy" }).catch(error => error);
    try {
      for (let attempt = 0; attempt < 2; attempt++) {
        await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(attempt + 1));
        const command = sender.mesh.read({ topic: "fabric.control.command" }).at(-1)!.data as FabricControlCommand;
        expect(command).not.toHaveProperty("ownerIncarnation");
        if (attempt === 0) await sender.mesh.put({
          key: "topology/control-seen/" + createHash("sha256").update(`host:older\0${command.commandId}`).digest("hex"),
          identity: identity("host:older"), ifVersion: 0,
          value: { format: 1, hostId: "host:older", commandId: command.commandId, targetId: command.targetId,
            expiresAt: Date.now() + 10_000, acceptance: { accepted: false, error: "Fabric control command expired", notRun: true } },
        });
        await sender.mesh.publish({ topic: "fabric.control.ack", kind: attempt === 0 ? "rejected" : "accepted",
          from: identity("host:older"), to: "host:sender", data: { version: 1, commandId: command.commandId,
            targetId: command.targetId, accepted: attempt === 1,
            ...(attempt === 0 ? { error: "Fabric control command expired", notRun: true } : { messageId: "legacy-retry" }) } });
      }
      expect(await pending).toMatchObject({ acknowledged: true, messageId: "legacy-retry" });
      expect(warn).toHaveBeenCalledOnce();
      expect(warn.mock.calls[0]![0]).toContain("Unfenced legacy control delivery count=1");
    } finally { await sender.close(); await pending; warn.mockRestore(); }
  });

  it("derives wake cause from the real event envelope, never a forged sender diagnostic", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-control-")); roots.push(root);
    const owner = plane(path.join(root, "mesh"), "host:owner");
    let admit!: (value: { command: FabricControlCommand; from: MeshIdentity }) => void;
    const admitted = new Promise<{ command: FabricControlCommand; from: MeshIdentity }>(resolve => { admit = resolve; });
    owner.start((command, from) => { admit({ command, from }); return { accepted: true }; });
    const from = identity("host:real-sender");
    const event = await owner.mesh.publish({ topic: "fabric.control.command", kind: "steer", from, to: "host:owner",
      data: { version: 1, commandId: "forged-cause", targetId: "main", operation: "steer", replyTo: from.id,
        ownerIncarnation: owner.incarnation, requestedAt: Date.now(), deadlineAt: Date.now() + 5000, message: "hello",
        wakeCause: { cause: "host-event", from: { id: "host:forged", name: "Forged", kind: "main" }, topic: "forged", key: "forged" },
      } });
    const result = await admitted;
    expect(result.from).toMatchObject(from);
    expect(result.command.wakeCause).toEqual({ cause: "steer", from: { id: from.id, name: from.name, kind: from.kind },
      topic: event.topic, key: event.id });
  });
  it.each([false, true])("only sheds an optional warning, not delivery fields or an oversized core (oversized=%s)", async oversized => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-advisory-budget-")); roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const make = (id: string) => {
      const value = new FabricControlPlane(new MeshStore(meshRoot, 2300, 1000), identity(id), {
        enabled: true, hostId: id, pollMs: 20, acknowledgementTimeoutMs: 1000,
      });
      planes.push(value); return value;
    };
    const owner = make("host:owner"), sender = make("host:sender");
    const delivery = { accepted: true, messageId: "accepted", triggered: false, reason: "held",
      pendingFollowUps: 1, oldestAgeS: 2, coalesced: true as const, replacedMessageId: "previous",
      ...(oversized ? { result: "x".repeat(300) } : {}),
    };
    owner.start(() => ({ ...delivery, warning: {
      code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId: "agent:target", kind: "agent", status: "running",
      message: "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion.",
    } }));
    sender.start(() => ({ accepted: false }));
    const outcome = sender.request("host:owner", "agent:target", "followUp", { message: "later", ownerIncarnation: owner.incarnation });
    if (oversized) await expect(outcome).rejects.toThrow("Fabric control result exceeds 2300 mesh event bytes");
    else await expect(outcome).resolves.toEqual({ queued: true, messageId: delivery.messageId, routed: "mesh", acknowledged: true,
      triggered: false, reason: "held", pendingFollowUps: 1, oldestAgeS: 2, coalesced: true, replacedMessageId: "previous" });
    const ack = sender.mesh.read({ topic: "fabric.control.ack", limit: 10 })[0]!.data;
    expect(ack).toMatchObject(oversized ? { accepted: false, error: "Fabric control result exceeds 2300 mesh event bytes" } : delivery);
    expect(ack).not.toHaveProperty("warning");
  });

  it("followUp advisory A6 validates incoming ACKs without changing success or retrying", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-followup-ack-")); roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender");
    sender.start(() => ({ accepted: false }));
    const valid = {
      code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId: "agent:target", kind: "agent", status: "running",
      message: "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion.",
    };
    // Publish directly: exercise the sender's parser independently of owner-side serialization.
    const variants = [undefined, null, "warning", [], {},
      { ...valid, code: "other" }, { ...valid, kind: "main" }, { ...valid, status: "completed" },
      { ...valid, message: "sender-supplied text" }, { ...valid, message: "x".repeat(257) },
      { ...valid, targetId: "agent:other" }, { ...valid, targetId: "x".repeat(201) },
      { ...valid, targetId: 42 }, valid, { ...valid, privateData: "must not leak" }];
    for (const [index, warning] of variants.entries()) {
      const outcome = sender.request("host:owner", "agent:target", "followUp", { ownerIncarnation: fixtureOwnerIncarnation, message: "later" });
      await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command", limit: 100 })).toHaveLength(index + 1));
      const command = sender.mesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1)!.data as { commandId: string };
      await sender.mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("host:owner"), to: "host:sender",
        data: { version: 1, commandId: command.commandId, targetId: "agent:target", ownerIncarnation: fixtureOwnerIncarnation, accepted: true, messageId: "accepted", ...(warning === undefined ? {} : { warning }) } });
      expect(await outcome).toEqual({ queued: true, messageId: "accepted", routed: "mesh", acknowledged: true,
        ...(index >= variants.length - 2 ? { warning: valid } : {}) });
    }
    expect(sender.mesh.read({ topic: "fabric.control.command", limit: 100 })).toHaveLength(variants.length);
    // A matching target still has to satisfy the bound; mismatch alone must not cover this check.
    for (const length of [200, 201]) {
      const targetId = "x".repeat(length);
      const outcome = sender.request("host:owner", targetId, "followUp", { ownerIncarnation: fixtureOwnerIncarnation, message: "later" });
      await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command", limit: 100 })).toHaveLength(variants.length + length - 199));
      const command = sender.mesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1)!.data as { commandId: string };
      await sender.mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("host:owner"), to: "host:sender",
        data: { version: 1, commandId: command.commandId, targetId, ownerIncarnation: fixtureOwnerIncarnation, accepted: true, messageId: "bounded", warning: { ...valid, targetId } } });
      expect(await outcome).toEqual({ queued: true, messageId: "bounded", routed: "mesh", acknowledged: true,
        ...(length === 200 ? { warning: { ...valid, targetId } } : {}) });
    }
    expect(sender.mesh.read({ topic: "fabric.control.command", limit: 100 })).toHaveLength(variants.length + 2);
  });

  it("retries cancellation beyond the production 10-second lock timeout with commit-time ACK timing", { timeout: 25_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "control-default-lock-")); roots.push(root);
    // No store timeout override and no ACK override: production defaults are 10s and 5s.
    const sender = new FabricControlPlane(new MeshStore(path.join(root, "mesh"), 65536, 1000), identity("session:sender"), { enabled: true, hostId: "session:sender" });
    planes.push(sender);
    const controller = new AbortController();
    const observed = sender.requestResult("session:owner", "actor:target", "ask", { ownerIncarnation: fixtureOwnerIncarnation }, "session:owner", { signal: controller.signal }).catch(error => error);
    await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(1));
    const lock = path.join(sender.mesh.root, ".lock");
    fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), `cancel-test\n${process.pid}\n${Date.now()}\n`);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const abortedAt = Date.now();
    try {
      controller.abort();
      expect(await observed).toMatchObject({ message: expect.stringContaining("cancelled") });
      await sender.close(); // final retry obligation must survive control-plane closure too
      await vi.waitFor(() => expect(warn.mock.calls.some(call => String(call[0]).includes("control cancellation: mesh lock timeout"))).toBe(true), { timeout: 12_000, interval: 25 });
      expect(Date.now() - abortedAt).toBeGreaterThanOrEqual(10_000);
      const releasedAt = Date.now(); fs.rmSync(lock, { recursive: true });
      await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command" }).filter(event => event.kind === "cancel")).toHaveLength(1), { timeout: 3_000 });
      const event = sender.mesh.read({ topic: "fabric.control.command" }).find(event => event.kind === "cancel")!;
      const command = event.data as FabricControlCommand;
      expect(command.requestCreatedAt).toBeLessThanOrEqual(abortedAt);
      expect(command.requestedAt).toBeGreaterThanOrEqual(releasedAt);
      expect(command.requestedAt).toBe(event.createdAt);
      expect(command.deadlineAt! - command.requestedAt).toBe(5_000);
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(sender.mesh.read({ topic: "fabric.control.command" }).filter(event => event.kind === "cancel")).toHaveLength(1);
    } finally { fs.rmSync(lock, { recursive: true, force: true }); warn.mockRestore(); }
  });
  it.each(["Escape", "ordinary timeout", "forged ceiling", "cloned ceiling", "ceiling without policy"])("still cancels the owner for %s, never from guest ceiling text", async cause => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-ceiling-")); roots.push(root);
    const owner = plane(path.join(root, "mesh"), "session:owner0000");
    const sender = plane(path.join(root, "mesh"), "session:sender000");
    const aborted = vi.fn();
    let entered = false;
    owner.start((_command, _from, signal) => new Promise(resolve => {
      entered = true;
      signal!.addEventListener("abort", () => { aborted(); resolve({ accepted: false, error: "owner cancelled" }); }, { once: true });
    }));
    sender.start(() => ({ accepted: false }));
    const controller = new AbortController();
    const observation = sender.requestResult("session:owner0000", "actor:target", "ask", { message: "accepted", ownerIncarnation: owner.incarnation }, "session:owner0000", { signal: controller.signal, timeoutMs: 5_000, detachOnMainCeiling: cause !== "ceiling without policy" }).catch(error => error);
    await vi.waitFor(() => expect(entered).toBe(true));
    const genuine = createMainExecutionCeilingError(700);
    const reason = cause === "ceiling without policy" ? genuine : cause === "cloned ceiling" ? structuredClone(genuine) : cause === "forged ceiling" ? Object.assign(new Error(genuine.message), { name: "MainExecutionCeilingError" }) : new Error(cause);
    controller.abort(reason);
    expect(await observation).toMatchObject({ message: expect.stringContaining("Remote Fabric request cancelled") });
    await vi.waitFor(() => expect(aborted).toHaveBeenCalledOnce());
    expect(sender.mesh.read({ topic: "fabric.control.command", limit: 20 }).filter(event => event.kind === "cancel")).toHaveLength(1);
  });

  it("retains the remote owner's independent ASK deadline after Main stops observing", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-owner-deadline-")); roots.push(root);
    const owner = plane(path.join(root, "mesh"), "session:owner0000");
    const sender = plane(path.join(root, "mesh"), "session:sender000");
    const aborted = vi.fn(); let entered = false;
    owner.start((_command, _from, signal) => new Promise(resolve => {
      entered = true;
      signal!.addEventListener("abort", () => { aborted(); resolve({ accepted: false, error: "owner budget" }); }, { once: true });
    }));
    sender.start(() => ({ accepted: false }));
    const controller = new AbortController();
    const observation = sender.requestResult("session:owner0000", "actor:target", "ask", { message: "accepted", ownerIncarnation: owner.incarnation }, "session:owner0000", { signal: controller.signal, timeoutMs: 500, detachOnMainCeiling: true }).catch(error => error);
    await vi.waitFor(() => expect(entered).toBe(true));
    const ceiling = createMainExecutionCeilingError(100); controller.abort(ceiling);
    expect(await observation).toBe(ceiling);
    expect(aborted).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(aborted).toHaveBeenCalledOnce(), { timeout: 2_000 });
    expect(sender.mesh.read({ topic: "fabric.control.command", limit: 20 }).filter(event => event.kind === "cancel")).toHaveLength(0);
  });
  it("keeps the production local deadline at 5 s and does not run a command arriving just after it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-local-deadline-"));
    roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender", {}, { acknowledgementTimeoutMs: 5_000 });
    const receiver = plane(path.join(root, "mesh"), "host:receiver", {}, { acknowledgementTimeoutMs: 5_000 });
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    const receive = vi.fn(() => ({ accepted: true }));
    const outcome = sender.request("host:receiver", "agent:target", "stop", { ownerIncarnation: receiver.incarnation }).catch((error: Error) => error);
    try {
      sender.start(() => ({ accepted: false }));
      await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(1));
      const command = sender.mesh.read({ topic: "fabric.control.command" })[0]!.data as FabricControlCommand;
      expect(command.deadlineAt).toBe(command.requestedAt + 5_000);
      now = command.requestedAt + 5_001;
      receiver.start(receive);
      expect(await outcome).toMatchObject({ message: "Fabric control command expired; not delivered, safe to resend." });
      expect(receive).not.toHaveBeenCalled();
      expect(sender.mesh.read({ topic: "fabric.control.ack" })[0]!.data)
        .toMatchObject({ accepted: false, error: "Fabric control command expired", notRun: true });
      expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(1);
    } finally {
      await sender.close(); await receiver.close();
      await outcome;
      clock.mockRestore();
    }
  });

  describe("pending mirrored owners", () => {
    const setup = async (timeoutMs = 1_000) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-mirror-"));
      roots.push(root);
      let lease: { remoteHost: string; expiresAt: number } | undefined = {
        remoteHost: "forge", expiresAt: Date.now() + 60_000,
      };
      const readMirroredOwner = vi.fn(() => lease);
      const sender = plane(path.join(root, "mesh"), "host:sender", {}, {
        acknowledgementTimeoutMs: timeoutMs, readMirroredOwner,
      });
      await sender.mesh.put({
        key: "topology/hosts/" + createHash("sha256").update("host:owner").digest("hex"),
        identity: identity("host:owner"), value: { id: "host:owner", remoteHost: "forge" },
      });
      sender.start(() => ({ accepted: false }));
      const events = () => sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      const command = async () => {
        await vi.waitFor(() => expect(events().some((event) => event.kind !== "cancel")).toBe(true));
        return events().find((event) => event.kind !== "cancel")!.data as { commandId: string };
      };
      const ack = async (commandId: string, remoteHost = "forge", from = "identity:owner", targetId = "agent:target") => {
        await sender.mesh.publish({
          topic: "fabric.control.ack", kind: "accepted", from: identity(from), to: "host:sender",
          data: { version: 1, commandId, targetId, ownerIncarnation: fixtureOwnerIncarnation, accepted: true, messageId: "delivered", result: "done", bridge: { from: remoteHost } },
        });
      };
      const once = async (cancels: number) => {
        await vi.waitFor(() => expect(events().filter((event) => event.kind === "cancel")).toHaveLength(cancels));
        expect(events().filter((event) => event.kind !== "cancel")).toHaveLength(1);
        expect(events().map((event) => (event.data as FabricControlCommand).destinationRemoteHost))
          .toEqual(Array(events().length).fill("forge"));
      };
      return { sender, readMirroredOwner, command, ack, once, setLease: (value: typeof lease) => { lease = value; } };
    };
    const settle = <T>(promise: Promise<T>) => promise.then(
      (value) => ({ value, error: undefined }),
      (error: Error) => ({ value: undefined, error }),
    );
    const unknown = "the outcome is unknown and it may still be delivered, so a retry can deliver it twice";

    it("reports an expired legacy projection as bridge-stale, publishes once and cancels once", async () => {
      const f = await setup();
      const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"));
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
      const { error } = await outcome;
      expect(error?.message).toContain("bridge-stale for remote host forge, target agent:target");
      expect(error?.message).not.toContain("lapsed");
      expect(error).toHaveProperty("code", "FABRIC_BRIDGE_STALE");
      expect(error?.message).toContain(unknown);
      expect(error).not.toHaveProperty("notRun");
      expect(f.readMirroredOwner).toHaveBeenCalledWith("host:owner", "identity:owner", "agent:target");
      await f.ack(commandId);
      await f.sender.close();
      await f.once(1);
      const reads = f.readMirroredOwner.mock.calls.length;
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(f.readMirroredOwner).toHaveBeenCalledTimes(reads); // no idle watchdog
    });

    it("keeps waiting beyond the captured expiry when the lease is renewed, then accepts the valid bridge ACK", async () => {
      const f = await setup();
      const oldExpiry = Date.now() + 200;
      f.setLease({ remoteHost: "forge", expiresAt: oldExpiry });
      const outcome = f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner");
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() + 60_000 });
      const { commandId } = await f.command();
      await new Promise((resolve) => setTimeout(resolve, Math.max(1, oldExpiry - Date.now() + 60)));
      expect(f.readMirroredOwner.mock.calls.length).toBeGreaterThan(1);
      await f.ack(commandId);
      await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "delivered" });
      await f.once(0);
    });

    it("names a healthy bridge with a lost ACK without claiming link death or retry safety", async () => {
      const f = await setup(100);
      vi.useFakeTimers();
      const outcome = settle(f.sender.request("host:owner", "agent:target", "followUp", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"));
      await vi.advanceTimersByTimeAsync(45_000);
      const { error } = await outcome;
      await f.sender.close();
      vi.useRealTimers();
      expect(error?.message).toContain("Fabric mesh bridge to remote host forge is not responding for agent:target");
      expect(error?.message).toContain(unknown);
      expect(error?.message).not.toMatch(/lapsed|is down|not run/i);
      expect(error).not.toHaveProperty("notRun");
      await f.once(1);
    }, 15_000);

    it("does not trust wrong bridge, identity or target ACKs while watching a lease", async () => {
      const f = await setup();
      let settled = false;
      const outcome = f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner")
        .finally(() => { settled = true; });
      const { commandId } = await f.command();
      await f.ack(commandId, "other");
      await f.ack(commandId, "forge", "identity:forged");
      await f.ack(commandId, "forge", "identity:owner", "agent:other");
      await new Promise((resolve) => setTimeout(resolve, 80));
      expect(settled).toBe(false);
      await f.ack(commandId);
      await expect(outcome).resolves.toMatchObject({ acknowledged: true });
      await f.once(0);
    });

    it.each(["lapse", "abort", "close"] as const)("settles %s during publish completion without waiting, then cancels the committed command once", async (winner) => {
      const f = await setup();
      const controller = new AbortController();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        const event = await publish(input);
        if (input.topic === "fabric.control.command" && input.kind === "ask") await gate;
        return event;
      });
      const outcome = settle(f.sender.requestResult("host:owner", "agent:target", "ask", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner", { signal: controller.signal }));
      await vi.waitFor(() => expect(f.sender.mesh.read({ topic: "fabric.control.command" }).some(event => event.kind === "ask")).toBe(true));
      if (winner === "lapse") f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
      if (winner === "abort") controller.abort();
      if (winner === "close") await f.sender.close();
      try {
        // This must settle before the original publish gate opens.
        const { error } = await outcome;
        expect(error?.message).toContain(winner === "lapse" ? "bridge-stale" : winner === "abort" ? "cancelled" : "closed");
        controller.abort();
        await f.sender.close();
      } finally {
        release();
      }
      await f.once(1);
    });

    it("lets a valid ACK win during publish and does not cancel on later lapse, abort or close", async () => {
      const f = await setup();
      const controller = new AbortController();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        const event = await publish(input);
        if (input.topic === "fabric.control.command" && input.kind === "ask") await gate;
        return event;
      });
      const outcome = f.sender.requestResult("host:owner", "agent:target", "ask", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner", { signal: controller.signal });
      try {
        const { commandId } = await f.command();
        await f.ack(commandId);
        await expect(outcome).resolves.toBe("done");
        f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
        controller.abort();
        await f.sender.close();
      } finally {
        release();
      }
      await f.once(0);
    });

    it("returns the lapse error without waiting for an in-flight cancellation", async () => {
      const f = await setup();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      const spy = vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        if (input.topic === "fabric.control.command" && input.kind === "cancel") await gate;
        return publish(input);
      });
      const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"));
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
      try {
        const { error } = await outcome;
        expect(error?.message).toContain("bridge-stale");
        expect(error?.message).not.toContain("lapsed");
        await f.ack(commandId);
        await f.sender.close();
        expect(spy.mock.calls.filter(([input]) => input.kind === "cancel")).toHaveLength(1);
      } finally {
        release();
      }
      await f.once(1);
    });

    it.each(["forge", null] as const)("freezes cancellation destination %s while publication is blocked and ownership changes", async (routedRemoteHost) => {
      const f = await setup();
      if (routedRemoteHost === null) f.setLease(undefined);
      const controller = new AbortController();
      const publish = f.sender.mesh.publish.bind(f.sender.mesh);
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      vi.spyOn(f.sender.mesh, "publish").mockImplementation(async (input) => {
        const event = await publish(input);
        if (input.topic === "fabric.control.command" && input.kind === "ask") await gate;
        return event;
      });
      const outcome = settle(f.sender.requestResult("host:owner", "agent:target", "ask",
        { ownerIncarnation: fixtureOwnerIncarnation, message: "private", data: { destinationRemoteHost: "ryzen2" } }, "identity:owner",
        { signal: controller.signal, routedRemoteHost }));
      await vi.waitFor(() => expect(f.sender.mesh.read({ topic: "fabric.control.command" }).some(event => event.kind === "ask")).toBe(true));
      try {
        f.setLease({ remoteHost: "ryzen2", expiresAt: Date.now() + 60_000 });
        controller.abort();
        expect((await outcome).error?.message).toContain("cancelled");
      } finally {
        release();
      }
      const commands = () => f.sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      await vi.waitFor(() => expect(commands()).toHaveLength(2));
      expect(commands().map((event) => event.kind)).toEqual(["ask", "cancel"]);
      expect(commands().map((event) => (event.data as FabricControlCommand).destinationRemoteHost))
        .toEqual([routedRemoteHost, routedRemoteHost]);
      expect((commands()[1]!.data as FabricControlCommand).cancelCommandId)
        .toBe((commands()[0]!.data as FabricControlCommand).commandId);
    });

    it("refuses a known native routing snapshot when fresh admission finds a mirror", async () => {
      const f = await setup();
      const publish = vi.spyOn(f.sender.mesh, "publish");
      await expect(f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner",
        { routedRemoteHost: null })).rejects.toThrow("Fabric native routing is unavailable for agent:target; the routed owner changed; this attempt was not published.");
      expect(publish).not.toHaveBeenCalled();
    });

    it("never retries a known native send into a newly mirrored owner", async () => {
      const f = await setup();
      f.setLease(undefined);
      const outcome = settle(f.sender.request("host:owner", "agent:target", "followUp", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner",
        { routedRemoteHost: null }));
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "ryzen2", expiresAt: Date.now() + 60_000 });
      await f.sender.mesh.publish({ topic: "fabric.control.ack", kind: "rejected",
        from: identity("identity:owner"), to: "host:sender", data: {
          version: 1, commandId, targetId: "agent:target", ownerIncarnation: fixtureOwnerIncarnation, accepted: false,
          error: "native notRun", notRun: true,
        } });
      expect((await outcome).error?.message).toContain("Fabric native routing is unavailable");
      const commands = f.sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      expect(commands).toHaveLength(1);
      expect(commands[0]!.data).toMatchObject({ destinationRemoteHost: null });
    });

    it("binds known native commands and ignores foreign stamped notRun after mirror takeover", async () => {
      const f = await setup();
      f.setLease(undefined);
      let settled = false;
      const outcome = f.sender.request("host:owner", "agent:target", "followUp", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner",
        { routedRemoteHost: null }).finally(() => { settled = true; });
      const { commandId } = await f.command();
      f.setLease({ remoteHost: "ryzen2", expiresAt: Date.now() + 60_000 });
      await f.sender.mesh.put({
        key: "topology/hosts/" + createHash("sha256").update("host:owner").digest("hex"),
        identity: identity("host:owner"), value: { id: "host:owner", remoteHost: "ryzen2" },
      });
      const tail = vi.spyOn(f.sender.mesh, "tail");
      await f.sender.mesh.publish({ topic: "fabric.control.ack", kind: "rejected",
        from: identity("identity:owner"), to: "host:sender", data: {
          version: 1, commandId, targetId: "agent:target", ownerIncarnation: fixtureOwnerIncarnation, accepted: false,
          error: "foreign notRun", notRun: true, bridge: { from: "ryzen2" },
        } });
      await vi.waitFor(() => expect(tail.mock.results.some((result) => result.type === "return" &&
        result.value.events.some((event) => event.topic === "fabric.control.ack"))).toBe(true));
      tail.mockRestore();
      expect(settled).toBe(false);
      await f.sender.mesh.publish({ topic: "fabric.control.ack", kind: "accepted",
        from: identity("identity:owner"), to: "host:sender", data: {
          version: 1, commandId, targetId: "agent:target", ownerIncarnation: fixtureOwnerIncarnation, accepted: true, messageId: "native",
        } });
      await expect(outcome).resolves.toMatchObject({ messageId: "native" });
      const events = f.sender.mesh.read({ topic: "fabric.control.command", limit: 100 });
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toMatchObject({ destinationRemoteHost: null });
    });

    it("does not start a watchdog or alter native ACKs when the read port returns no mirror", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-native-"));
      roots.push(root);
      const readMirroredOwner = vi.fn(() => undefined);
      const sender = plane(path.join(root, "mesh"), "host:sender", {}, { readMirroredOwner });
      const owner = plane(path.join(root, "mesh"), "host:owner");
      sender.start(() => ({ accepted: false }));
      owner.start(() => ({ accepted: true, messageId: "native" }));
      await expect(sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: owner.incarnation }))
        .resolves.toMatchObject({ acknowledged: true, messageId: "native" });
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(readMirroredOwner).toHaveBeenCalledTimes(1);
      expect(sender.mesh.read({ topic: "fabric.control.command", limit: 10 })[0]!.data)
        .toMatchObject({ destinationRemoteHost: null });
    });

    it("keeps no-port destinations unbound but refuses an unrevalidated explicit mirror", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-legacy-route-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");
      const owner = plane(meshRoot, "host:owner");
      sender.start(() => ({ accepted: false }));
      owner.start(() => ({ accepted: true }));
      await expect(sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: owner.incarnation }))
        .resolves.toMatchObject({ acknowledged: true });
      expect(sender.mesh.read({ topic: "fabric.control.command", limit: 100 })[0]!.data)
        .not.toHaveProperty("destinationRemoteHost");
      const publish = vi.spyOn(sender.mesh, "publish");
      await expect(sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: owner.incarnation }, "host:owner",
        { routedRemoteHost: "forge" })).rejects.toThrow("Fabric mesh bridge routing to remote host forge is unavailable");
      expect(publish).not.toHaveBeenCalled();
    });

    it("never adopts an unvalidated replacement link label", async () => {
      const f = await setup();
      f.setLease({ remoteHost: "forge", expiresAt: Date.now() + 200 });
      const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"));
      await f.command();
      f.setLease({ remoteHost: "forged-label", expiresAt: Date.now() + 60_000 });
      const { error } = await outcome;
      expect(error?.message).toContain("remote host forge");
      expect(error?.message).not.toContain("forged-label");
      await f.once(1);
    });
  });
  describe("mirrored message admission budget (production path, fake clock)", () => {
    // The transport gate models the mesh lock; request admission, timers, ACK authority,
    // cancellation and log consumption all run through the production control plane.
    const setup = async (blocked = false, mirror = true, port = true, bridgeTimeoutMs = 30_000) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-budget-"));
      roots.push(root);
      let lease = mirror ? { remoteHost: "forge", expiresAt: Date.now() + 15_000 } : undefined;
      let readFails = false;
      const readMirroredOwner = vi.fn(() => {
        if (readFails) throw new Error("directory read failed");
        return lease;
      });
      const sender = plane(path.join(root, "mesh"), "host:sender", {}, {
        acknowledgementTimeoutMs: 5_000, bridgeTimeoutMs,
        ...(port ? { readMirroredOwner } : {}),
      });
      // Obtain the transport envelope without depending on its private shape.
      const envelope = await sender.mesh.publish({ topic: "fixture", from: identity("host:sender") });
      vi.useFakeTimers();
      const admittedAt = Date.now();
      if (mirror) lease = { remoteHost: "forge", expiresAt: admittedAt + 15_000 };
      const events: MeshEvent[] = [];
      let release!: () => void;
      const gate = new Promise<void>((resolve) => { release = resolve; });
      const publish = vi.spyOn(sender.mesh, "publish").mockImplementation(async (input) => {
        if (blocked && input.topic === "fabric.control.command" && input.kind !== "cancel") await gate;
        const committedAt = Date.now();
        const event: MeshEvent = {
          ...envelope, ...input,
          data: typeof input.data === "function" ? input.data(committedAt) : input.data,
          sequence: events.length + 1, createdAt: committedAt,
        };
        events.push(event);
        // The modeled log lives in memory, so model its append notification: the real mesh
        // watcher (change-notifier) receives "events.jsonl" exactly as from an fs append.
        for (const watcher of rootWatchers) watcher.emit("change", "change", "events.jsonl");
        return event;
      });
      const tail = vi.spyOn(sender.mesh, "tail").mockImplementation((offset) => ({
        events: events.filter((event) => event.sequence > offset), nextOffset: events.length,
      }));
      // Idle has no poll (fv2-7978): log consumption wakes on the mesh root's real FSWatcher.
      const realWatch = fs.watch;
      const rootWatchers: fs.FSWatcher[] = [];
      const watch = vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
        const watcher = (realWatch as (...values: unknown[]) => fs.FSWatcher)(...args);
        if (args[0] === sender.mesh.root) rootWatchers.push(watcher);
        return watcher;
      }) as typeof fs.watch);
      sender.start(() => ({ accepted: false }));
      const commands = () => events.filter((event) => event.topic === "fabric.control.command");
      const ack = async (remoteHost: string | undefined = mirror ? "forge" : undefined) => {
        const command = commands().find((event) => event.kind !== "cancel")!.data as FabricControlCommand;
        await sender.mesh.publish({
          topic: "fabric.control.ack", kind: "accepted", from: identity("identity:owner"), to: "host:sender",
          data: { version: 1, commandId: command.commandId, targetId: "agent:target", accepted: true,
            messageId: "original", result: "done", ownerIncarnation: fixtureOwnerIncarnation, ...(remoteHost ? { bridge: { from: remoteHost } } : {}) },
        });
        await vi.advanceTimersByTimeAsync(20);
      };
      const dispose = async () => {
        release();
        await Promise.resolve();
        await sender.close();
        publish.mockRestore();
        tail.mockRestore();
        watch.mockRestore();
        vi.useRealTimers();
      };
      return { sender, admittedAt, publish, commands, ack, release, dispose, readMirroredOwner,
        failReads: () => { readFails = true; },
        setLease: (value: typeof lease) => { lease = value; } };
    };
    const settle = <T>(promise: Promise<T>) => promise.then(
      (value) => ({ value, error: undefined }), (error: Error) => ({ value: undefined, error }),
    );

    it.each(["steer", "followUp", "stop"] as const)("reports a proven expired mirrored %s as not delivered and safe to resend", async (operation) => {
      const f = await setup();
      try {
        f.setLease({ remoteHost: "forge", expiresAt: Date.now() + 120_000 });
        const outcome = settle(f.sender.request("host:owner", "agent:target", operation, { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"));
        // Messages retain their existing single notRun retry; stop never retries.
        const attempts = operation === "stop" ? 1 : 2;
        vi.spyOn(Math, "random").mockReturnValue(0.5);
        for (let attempt = 0; attempt < attempts; attempt++) {
          // The sole proven-notRun resend draws 50ms, not an immediate lockstep retry.
          await vi.advanceTimersByTimeAsync(attempt === 0 ? 0 : 50);
          const command = f.commands().filter((event) => event.kind !== "cancel")[attempt]!.data as FabricControlCommand;
          // The retry draw is charged after the bridge floor, so jitter never
          // adds time to the original second-attempt admission/ACK budget.
          expect(command.deadlineAt).toBe(command.requestedAt + 30_000 - (attempt === 0 ? 0 : 50));
          await vi.advanceTimersByTimeAsync(30_001);
          await f.sender.mesh.publish({
            topic: "fabric.control.ack", kind: "rejected", from: identity("identity:owner"), to: "host:sender",
            data: { version: 1, commandId: command.commandId, targetId: "agent:target", ownerIncarnation: fixtureOwnerIncarnation, accepted: false,
              error: "Fabric control command expired", notRun: true, bridge: { from: "forge" } },
          });
          await vi.advanceTimersByTimeAsync(20);
        }
        const { error } = await outcome;
        expect(error?.message).toBe("Fabric control command expired; not delivered, safe to resend.");
        expect(error?.message).not.toContain("outcome is unknown");
        expect(error).toHaveProperty("notRun", true);
        expect(f.commands().map((event) => event.kind)).toEqual(Array(attempts).fill(operation));
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });

    it.each(["steer", "followUp"] as const)("close owns a pending jittered %s resend", async (operation) => {
      const f = await setup();
      const random = vi.spyOn(Math, "random").mockReturnValue(0.5);
      try {
        const outcome = settle(f.sender.request("host:owner", "agent:target", operation, { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"));
        await vi.advanceTimersByTimeAsync(0);
        const command = f.commands()[0]!.data as FabricControlCommand;
        await f.sender.mesh.publish({
          topic: "fabric.control.ack", kind: "rejected", from: identity("identity:owner"), to: "host:sender",
          data: { version: 1, commandId: command.commandId, targetId: "agent:target", ownerIncarnation: fixtureOwnerIncarnation, accepted: false,
            error: "Fabric control command expired", notRun: true, bridge: { from: "forge" } },
        });
        await vi.advanceTimersByTimeAsync(20);
        expect(random).toHaveBeenCalled(); // Forced 50ms draw, still pending at close.
        expect(f.commands()).toHaveLength(1);
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0);
        const { error } = await outcome;
        expect(error?.message).toBe("Fabric control plane closed");
        await vi.advanceTimersByTimeAsync(100_000);
        expect(f.commands()).toHaveLength(1);
        await expect(f.sender.request("host:owner", "agent:target", operation, { ownerIncarnation: fixtureOwnerIncarnation })).rejects.toThrow("Fabric control plane closed");
      } finally {
        random.mockRestore();
        await f.dispose();
      }
    });

    it.each([[1, 30_000], [60_000, 60_000]])("uses configured bridge window %i, floored to %i ms", async (configured, expected) => {
      const f = await setup(false, true, true, configured);
      try {
        const outcome = f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner");
        await vi.advanceTimersByTimeAsync(0);
        const cmd = f.commands()[0]!.data as FabricControlCommand;
        expect(cmd.deadlineAt! - cmd.requestedAt).toBe(expected);
        await f.ack();
        await expect(outcome).resolves.toMatchObject({ acknowledged: true });
      } finally { await f.dispose(); }
    });

    it.each(["steer", "followUp"] as const)("settles %s at admission + 45 s during a 46 s lock, even with failed reads", async (operation) => {
      const f = await setup(true);
      try {
        let settled = false;
        const outcome = settle(f.sender.request("host:owner", "agent:target", operation, { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"))
          .then((value) => { settled = true; return value; });
        f.failReads(); // Still-live 15 s captured lease; no lapse evidence and no readable directory.
        await vi.advanceTimersByTimeAsync(44_999);
        expect(settled).toBe(false);
        expect(f.commands()).toHaveLength(0);
        await vi.advanceTimersByTimeAsync(1);
        expect(settled).toBe(true);
        const { error } = await outcome;
        expect(Date.now() - f.admittedAt).toBe(45_000);
        expect(error?.constructor).toBe(Error);
        expect(error?.message).toBe("Fabric mesh bridge to remote host forge is not responding for agent:target; the outcome is unknown and it may still be delivered, so a retry can deliver it twice.");
        expect(error).not.toHaveProperty("notRun");
        expect(f.publish).toHaveBeenCalledTimes(1); // No replay, no cancellation before commit.
        await vi.advanceTimersByTimeAsync(1_000);
        f.setLease({ remoteHost: "replacement", expiresAt: Date.now() + 60_000 });
        f.release();
        await vi.advanceTimersByTimeAsync(0);
        expect(f.commands().map((event) => event.kind)).toEqual([operation, "cancel"]);
        const [command, cancellation] = f.commands().map((event) => event.data as FabricControlCommand);
        expect(command).toMatchObject({ requestedAt: f.admittedAt + 46_000, deadlineAt: f.admittedAt + 76_000, destinationRemoteHost: "forge" });
        expect(cancellation).toMatchObject({ cancelCommandId: command!.commandId, destinationRemoteHost: "forge" });
        expect(cancellation!.deadlineAt! - cancellation!.requestedAt).toBe(30_000);
        await f.ack(); // Original ACK is too late and cannot revive the settled request.
        expect(await outcome).toEqual({ value: undefined, error });
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0); // Late commit must not arm another ACK timer.
        expect(f.commands()).toHaveLength(2);
      } finally { await f.dispose(); }
    });

    it("allows the original ACK inside the bridge budget despite replacement, and clears the prearmed timer", async () => {
      const f = await setup();
      try {
        let settled = false;
        const outcome = f.sender.request("host:owner", "agent:target", "followUp", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner")
          .finally(() => { settled = true; });
        await vi.advanceTimersByTimeAsync(8_000);
        f.setLease({ remoteHost: "replacement", expiresAt: Date.now() + 60_000 });
        await f.ack("replacement");
        expect(settled).toBe(false);
        await f.ack("forge");
        await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "original" });
        await vi.advanceTimersByTimeAsync(12_000);
        await f.sender.close();
        expect(f.commands()).toHaveLength(1);
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });

    it("does not let same-origin renewal extend the 45 s bridge budget", async () => {
      const f = await setup();
      try {
        f.setLease({ remoteHost: "forge", expiresAt: Date.now() + 120_000 });
        const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner"));
        await vi.advanceTimersByTimeAsync(44_000);
        f.setLease({ remoteHost: "forge", expiresAt: Date.now() + 60_000 });
        await vi.advanceTimersByTimeAsync(1_000);
        expect((await outcome).error?.message).toContain("is not responding");
        expect(f.commands().map((event) => event.kind)).toEqual(["steer", "cancel"]);
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });

    it.each(["abort", "close", "lapse"] as const)("clears admission timer on %s while blocked, then cancels once after commit", async (winner) => {
      const f = await setup(true);
      const controller = new AbortController();
      try {
        const outcome = settle(f.sender.request("host:owner", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner", { signal: controller.signal }));
        await vi.advanceTimersByTimeAsync(100);
        if (winner === "abort") controller.abort();
        if (winner === "close") await f.sender.close();
        if (winner === "lapse") {
          f.setLease({ remoteHost: "forge", expiresAt: Date.now() - 1 });
          await vi.advanceTimersByTimeAsync(20);
        }
        expect((await outcome).error?.message).toContain(winner === "abort" ? "cancelled" : winner === "close" ? "closed" : "bridge-stale");
        controller.abort();
        await f.sender.close();
        expect(vi.getTimerCount()).toBe(0);
        await vi.advanceTimersByTimeAsync(10_000);
        f.release();
        await vi.advanceTimersByTimeAsync(0);
        expect(f.commands().map((event) => event.kind)).toEqual(["steer", "cancel"]);
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });

    it.each(["native", "legacy", "ask", "result-message", "stop"] as const)("preserves postcommit waits beyond 9 s for %s", async (mode) => {
      const mirrored = mode !== "native" && mode !== "legacy";
      const f = await setup(true, mirrored, mode !== "legacy");
      try {
        let settled = false;
        const request = mode === "ask" || mode === "result-message"
          ? f.sender.requestResult("host:owner", "agent:target", mode === "ask" ? "ask" : "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner", { timeoutMs: 60_000 })
          : f.sender.request("host:owner", "agent:target", mode === "stop" ? "stop" : "steer", { ownerIncarnation: fixtureOwnerIncarnation }, "identity:owner");
        const outcome = request.finally(() => { settled = true; });
        // Renew same-origin lease for long result operations; no global lease cutoff.
        f.setLease(mirrored ? { remoteHost: "forge", expiresAt: Date.now() + 120_000 } : undefined);
        await vi.advanceTimersByTimeAsync(10_000);
        expect(settled).toBe(false);
        f.release();
        await vi.advanceTimersByTimeAsync(0);
        const command = f.commands()[0]!.data as FabricControlCommand;
        expect(command.requestedAt).toBe(f.admittedAt + 10_000);
        expect(command.deadlineAt).toBe(command.requestedAt + (mode === "ask" || mode === "result-message" ? 60_000 : mirrored ? 30_000 : 5_000));
        if (mode === "legacy") expect(command).not.toHaveProperty("destinationRemoteHost");
        if (mode === "native") expect(command.destinationRemoteHost).toBeNull();
        // Native/stop ACKs after their wire deadline still get the original grace.
        await vi.advanceTimersByTimeAsync(mode === "ask" || mode === "result-message" ? 20_000 : 6_000);
        expect(settled).toBe(false);
        await f.ack();
        if (mode === "ask" || mode === "result-message") await expect(outcome).resolves.toBe("done");
        else await expect(outcome).resolves.toMatchObject({ acknowledged: true });
        await f.sender.close();
        expect(f.commands()).toHaveLength(1);
        expect(vi.getTimerCount()).toBe(0);
      } finally { await f.dispose(); }
    });
  });

  describe("captured directory-backed ACK authority", () => {
    const setup = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-authority-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const writer = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const ownerId = "X"; // Canonical bridge root: host, root and identity are all X.
      const owner = identity(ownerId);
      const expiresAt = Date.now() + 60_000;
      const keyFor = (prefix: string) => prefix + createHash("sha256").update(ownerId).digest("hex");
      const hostKey = keyFor("topology/hosts/");
      const participantKey = keyFor("topology/participants/");
      const participant: FabricParticipantRecord = {
        format: 1, id: ownerId, kind: "root", rootId: ownerId,
        ownerHostId: ownerId, ownerIdentityId: ownerId, ownerIncarnation: fixtureOwnerIncarnation, name: "main", status: "idle",
        runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
        cwd: "/tmp/project", sessionId: ownerId, startedAt: 1, updatedAt: 2,
        pendingMessages: false, controlProtocol: "v1",
      };
      const advertise = async (remoteHost: string, expiry = expiresAt) => {
        await writer.writeBatch({ identity: owner, ops: [
          { kind: "put", key: hostKey, value: {
            format: 1, id: ownerId, rootId: ownerId, identity: owner,
            startedAt: 1, updatedAt: 2, expiresAt: expiry, remoteHost,
          } },
          { kind: "put", key: participantKey, value: { ...participant, remoteHost } },
        ] });
        writeHostLease(meshRoot, {
          id: ownerId, rootId: ownerId, identityId: ownerId,
          updatedAt: Date.now(), expiresAt: expiry,
        });
      };
      const withdraw = async () => {
        await writer.writeBatch({ identity: owner, ops: [
          { kind: "delete", key: hostKey }, { kind: "delete", key: participantKey },
        ] });
        removeHostLease(meshRoot, ownerId);
      };
      const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, { readCacheMs: 2_000 }), {
        enabled: true, hostId: "host:sender", rootId: "host:sender", identity: identity("host:sender"),
      });
      const readMirroredOwner = vi.fn((host: string, ownerIdentity: string | undefined, target: string) =>
        directory.mirroredControlOwner(host, ownerIdentity, target));
      await advertise("forge");
      expect(readMirroredOwner(ownerId, ownerId, ownerId)).toEqual({ remoteHost: "forge", expiresAt,
        ownerIncarnation: fixtureOwnerIncarnation });
      readMirroredOwner.mockClear();
      const sender = plane(meshRoot, "host:sender", { readCacheMs: 2_000 }, { readMirroredOwner });
      sender.start(() => ({ accepted: false }));
      const commands = () => writer.read({ topic: "fabric.control.command", limit: 100 });
      const command = async () => {
        await vi.waitFor(() => expect(commands().filter((event) => event.kind !== "cancel")).toHaveLength(1));
        expect(readMirroredOwner).toHaveBeenCalledWith(ownerId, ownerId, ownerId);
        return commands().find((event) => event.kind !== "cancel")!.data as { commandId: string };
      };
      const ack = (commandId: string, remoteHost: string | undefined, accepted = true) => writer.publish({
        topic: "fabric.control.ack", kind: accepted ? "accepted" : "rejected", from: owner, to: "host:sender",
        data: {
          version: 1, commandId, targetId: ownerId, ownerIncarnation: fixtureOwnerIncarnation, accepted,
          ...(accepted ? { messageId: "original-forge" } : { error: "replacement says not run", notRun: true }),
          ...(remoteHost === undefined ? {} : { bridge: { from: remoteHost } }),
        },
      });
      return { sender, ownerId, expiresAt, readMirroredOwner, advertise, withdraw, commands, command, ack };
    };

    it.each([
      ["replacement", "ryzen2"], ["replacement", undefined],
      ["withdrawal", "ryzen2"], ["withdrawal", undefined],
    ] as const)("rejects %s ACK origin %s with notRun without retry or native downgrade", async (metadata, stamp) => {
      const f = await setup();
      let settled = false;
      const outcome = f.sender.request(f.ownerId, f.ownerId, "followUp", { ownerIncarnation: fixtureOwnerIncarnation })
        .finally(() => { settled = true; });
      const { commandId } = await f.command();
      await f.withdraw();
      if (metadata === "replacement") await f.advertise("ryzen2");
      expect(f.readMirroredOwner(f.ownerId, f.ownerId, f.ownerId)?.remoteHost)
        .toBe(metadata === "replacement" ? "ryzen2" : undefined);
      // This is the exact pending UUID, target and identity: only the origin is wrong.
      const tail = vi.spyOn(f.sender.mesh, "tail");
      await f.ack(commandId, stamp, false);
      await vi.waitFor(() => expect(tail.mock.results.some((result) =>
        result.type === "return" && result.value.events.some((event) => event.topic === "fabric.control.ack"),
      )).toBe(true));
      tail.mockRestore();
      expect(settled).toBe(false);
      expect(f.commands().filter((event) => event.kind !== "cancel")).toHaveLength(1);
      expect(f.commands().filter((event) => event.kind === "cancel")).toHaveLength(0);
      await f.ack(commandId, "forge");
      await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "original-forge" });
      expect(f.commands().filter((event) => event.kind !== "cancel")).toHaveLength(1);
    });

    it.each([undefined, "forge"] as const)("does not adopt a replacement on bounded retry with routing snapshot %s", async (routedRemoteHost) => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "followUp", { ownerIncarnation: fixtureOwnerIncarnation }, f.ownerId,
        routedRemoteHost === undefined ? {} : { routedRemoteHost }).then(() => undefined, (error: Error) => error);
      const { commandId } = await f.command();
      await f.withdraw();
      await f.advertise("ryzen2");
      // Even a valid ORIGINAL rejection only authorizes retry to the original link.
      await f.ack(commandId, "forge", false);
      const error = await outcome;
      expect(error?.message).toBe("Fabric mesh bridge routing to remote host forge is unavailable for X; the routed owner could not be revalidated; this attempt was not published.");
      expect(error).not.toHaveProperty("notRun");
      expect(f.commands()).toHaveLength(1);
      expect(f.commands()[0]!.data).toMatchObject({ destinationRemoteHost: "forge" });
    });

    it("keeps the original destination on a healthy mirrored notRun retry", async () => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "followUp", { ownerIncarnation: fixtureOwnerIncarnation });
      const { commandId } = await f.command();
      await f.ack(commandId, "forge", false);
      await vi.waitFor(() => expect(f.commands()).toHaveLength(2));
      const retry = f.commands().find((event) => (event.data as FabricControlCommand).commandId !== commandId)!;
      expect(f.commands().map((event) => (event.data as FabricControlCommand).destinationRemoteHost))
        .toEqual(["forge", "forge"]);
      await f.ack((retry.data as FabricControlCommand).commandId, "forge");
      await expect(outcome).resolves.toMatchObject({ acknowledged: true });
    });

    it("accepts the original Forge ACK committed before withdrawal but consumed after it", async () => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "steer", { ownerIncarnation: fixtureOwnerIncarnation });
      const { commandId } = await f.command();
      // Hold the sender's actual log drain, not ACK publication or directory reads.
      const tail = vi.spyOn(f.sender.mesh, "tail").mockImplementation((offset) => ({ events: [], nextOffset: offset }));
      try {
        await f.ack(commandId, "forge");
        await f.withdraw();
        expect(f.readMirroredOwner(f.ownerId, f.ownerId, f.ownerId)).toBeUndefined();
        expect(Date.now()).toBeLessThan(f.expiresAt);
      } finally {
        tail.mockRestore();
      }
      await expect(outcome).resolves.toMatchObject({ acknowledged: true, messageId: "original-forge" });
      await f.sender.close();
      expect(f.commands().filter((event) => event.kind === "cancel")).toHaveLength(0);
    });

    it("does not revive a settled mirrored request when its original ACK arrives", async () => {
      const f = await setup();
      const outcome = f.sender.request(f.ownerId, f.ownerId, "steer", { ownerIncarnation: fixtureOwnerIncarnation }).then(
        () => undefined, (error: Error) => error,
      );
      const { commandId } = await f.command();
      await f.advertise("forge", Date.now() - 1);
      const error = await outcome;
      expect(error?.message).toContain("bridge-stale for remote host forge, target X");
      expect(error?.message).not.toContain("lapsed");
      expect(error).not.toHaveProperty("notRun");
      await f.withdraw();
      await f.advertise("forge");
      await f.ack(commandId, "forge");
      await f.sender.close();
      expect(f.commands().filter((event) => event.kind !== "cancel")).toHaveLength(1);
      await vi.waitFor(() => expect(f.commands().filter((event) => event.kind === "cancel")).toHaveLength(1));
      expect(await outcome).toBe(error);
    });
  });
  // smarty-dev#816: the deadline was stamped before the sender waited for the mesh lock (2-3 s
  // under load), so the owner got only what was left of the 5 s, or nothing. review/astra F1 on
  // #70: the sender's own wait must start at commit too.
  describe("with a publish that waits for the mesh lock", () => {
    const run = async (lockWaitMs: number, handlerMs: number) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");                     // a 1 s timeout
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, handlerMs));
        return { accepted: true, messageId: "delivered" };
      });
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      const original = MeshStore.prototype.publish;
      vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
        if (input.topic === "fabric.control.command" && input.kind === "steer") {
          await new Promise((resolve) => setTimeout(resolve, lockWaitMs));
        }
        return original.call(this, input);
      });
      const result = await sender.request("host:receiver", "agent:target", "steer", { message: "late", ownerIncarnation: receiver.incarnation });
      const event = new MeshStore(meshRoot, 64 * 1024, 1_000).read({ topic: "fabric.control.command", limit: 10 })
        .find((candidate) => candidate.kind === "steer")!;
      return { result, receive, event };
    };

    it("stamps the command's deadline at commit, and waits for a slow owner from there", async () => {
      // 2.5 s lock wait + 750 ms handler: the old sender timer (1 s + 2 s grace from the start) fired first.
      const { result, receive, event } = await run(2_500, 750);
      expect(result.messageId).toBe("delivered");
      expect(receive).toHaveBeenCalledTimes(1);
      expect((event.data as { requestedAt: number }).requestedAt).toBe(event.createdAt);
    }, 15_000);

    it("does not time out before its publish commits, however long the lock wait", async () => {
      const { result, receive } = await run(3_500, 0);                    // longer than the old 3 s timer
      expect(result.messageId).toBe("delivered");
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);
  });

  // smarty-dev#424: a lock timeout while the owner claimed, recorded or acknowledged a command
  // dropped it without an acknowledgement or a retry (dev-lead: 30 of 61 commands in 6 h).
  describe("after a lock timeout", () => {
    const lockTimeout = () => Object.assign(new Error("Timed out waiting for the Fabric mesh lock"), {
      code: "FABRIC_MESH_LOCK_TIMEOUT",
    });
    const failOnce = <K extends "put" | "publish">(method: K, when: (store: MeshStore, input: never) => boolean, afterMs = 0) => {
      const original = MeshStore.prototype[method] as (...args: unknown[]) => unknown;
      let failed = false;
      return vi.spyOn(MeshStore.prototype, method).mockImplementation(function (this: MeshStore, input: never) {
        if (!failed && when(this, input)) {
          failed = true;
          return new Promise((_resolve, reject) => setTimeout(() => reject(lockTimeout()), afterMs));
        }
        return original.call(this, input);
      } as never);
    };
    const run = async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn((received: { commandId: string }) => ({ accepted: true, messageId: "local:" + received.commandId }));
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 100));
      return { meshRoot, receive, ownerIncarnation: receiver.incarnation, request: () => sender.request("host:receiver", "agent:target", "steer", { message: "once", ownerIncarnation: receiver.incarnation }) };
    };
    const shared = (store: MeshStore) => !store.root.includes(`${path.sep}control-seen${path.sep}`);
    const isClaim = (input: { key?: string }) => input.key?.startsWith("topology/control-seen/") === true;

    it("on the shared claim, the command is claimed on the next poll and runs once", async () => {
      const { receive, request } = await run();
      const spy = failOnce("put", (store, input: { key?: string }) => shared(store) && isClaim(input));
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
      expect(spy.mock.results.some((entry) => entry.type === "return")).toBe(true);
    });

    it("while recording the outcome, the sender still gets it", async () => {
      const { receive, request } = await run();
      failOnce("put", (store, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(store) && isClaim(input) && input.value?.acceptance !== undefined);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
    });

    // Detached asynchronous commands pass the cursor: the owned queue retries only the ACK,
    // not the handler. A later replay still consults the durable claim/outcome.
    it.each(["ask"] as const)("of a detached %s, the bounded notification queue retries its outcome without re-execution", async operation => {
      const { meshRoot, receive, ownerIncarnation } = await run();
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const ask = {
        topic: "fabric.control.command", kind: operation, from: identity("host:sender"), to: "host:receiver",
        data: { version: 1, commandId: "command:ask", targetId: "agent:target", operation, replyTo: "host:sender",
          message: "inspect", ownerIncarnation, requestCreatedAt: Date.now(), requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 },
      };
      const acks = () => store.read({ topic: "fabric.control.ack", limit: 100 })
        .filter((event) => (event.data as { commandId?: string }).commandId === "command:ask");
      failOnce("put", (owner, input: { key?: string; value?: { acceptance?: unknown } }) =>
        !shared(owner) && isClaim(input) && input.value?.acceptance !== undefined);
      failOnce("publish", (_owner, input: { topic?: string }) => input.topic === "fabric.control.ack");
      await store.publish(ask);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      await vi.waitFor(() => expect(acks()).toHaveLength(1), { timeout: 3_000, interval: 20 });
      expect(acks()[0]!.data).toMatchObject({ accepted: true });
      await store.publish(ask); // the retried durable outcome answers a replay too
      await vi.waitFor(() => expect(acks()).toHaveLength(2), { timeout: 3_000, interval: 20 });
      expect(acks()[1]!.data).toMatchObject({ accepted: true });
      expect(receive).toHaveBeenCalledTimes(1);
    });

    it("on the acknowledgement, the next poll publishes the real outcome, even past the deadline", async () => {
      const { receive, request } = await run();
      // A real lock wait (10 s) outlasts the deadline (1 s here): the retry must not answer "expired".
      failOnce("publish", (_store, input: { topic?: string }) => input.topic === "fabric.control.ack", 1_200);
      const result = await request();
      expect(result.messageId).toMatch(/^local:/);
      expect(receive).toHaveBeenCalledTimes(1);
    });
  });

  // smarty-dev#643: dedupe records lived in the shared state (26% of it on the fleet), and each
  // received command rewrote that whole file twice.
  describe("dedupe records", () => {
    const seenKey = (hostId: string, commandId: string) =>
      "topology/control-seen/" + createHash("sha256").update(`${hostId}\0${commandId}`).digest("hex");
    const ownStore = (meshRoot: string, hostId: string) =>
      new MeshStore(path.join(meshRoot, "control-seen", createHash("sha256").update(hostId).digest("hex").slice(0, 32)), 64 * 1024, 1_000);
    const command = (commandId: string, to: string, ownerIncarnation?: string) => ({
      topic: "fabric.control.command", kind: "steer", from: identity("host:sender"), to,
      data: { version: 1, commandId, targetId: "agent:target", operation: "steer", replyTo: "host:sender", message: "m", requestedAt: Date.now(), ...(ownerIncarnation ? { ownerIncarnation } : {}) },
    });
    const ackFor = (store: MeshStore, commandId: string) =>
      store.read({ topic: "fabric.control.ack", limit: 100 }).find((event) => (event.data as { commandId?: string }).commandId === commandId);

    it("keep the outcome in the owner's own store; the shared state holds only the claim", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const sender = plane(meshRoot, "host:sender");
      const receiver = plane(meshRoot, "host:receiver");
      sender.start(() => ({ accepted: false }));
      receiver.start((received) => ({ accepted: true, messageId: "local:" + received.commandId }));
      await new Promise((resolve) => setTimeout(resolve, 100));             // past its one-time legacy move
      const result = await sender.request("host:receiver", "agent:target", "steer", { message: "once", ownerIncarnation: receiver.incarnation });
      const commandEvent = store.read({ topic: "fabric.control.command", limit: 10 }).at(-1)!;
      const commandId = (commandEvent.data as { commandId: string }).commandId;
      expect(result.messageId).toBe("local:" + commandId);
      const shared = store.listAll("topology/control-seen/");
      expect(shared.map((entry) => entry.key)).toEqual([seenKey("host:receiver", commandId)]);
      expect(shared[0]!.value).not.toHaveProperty("acceptance");            // one shared write, no outcome
      expect(shared[0]!.version).toBe(store.get(shared[0]!.key)!.version);
      expect(ownStore(meshRoot, "host:receiver").get(seenKey("host:receiver", commandId))?.value).toMatchObject({
        hostId: "host:receiver", commandId, sequence: commandEvent.sequence, acceptance: { accepted: true },
      });
    });

    it("left in the shared state by older runtimes still answer a replay, and stay there", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:old", "host:receiver"));          // replayed at startup
      await store.put({
        key: seenKey("host:receiver", "command:old"), identity: identity("host:receiver"), ifVersion: 0, value: {
          format: 1, hostId: "host:receiver", commandId: "command:old", targetId: "agent:target", expiresAt: Date.now() + 60_000,
          acceptance: { accepted: true, messageId: "earlier:command:old" },
        },
      });
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:old")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(ackFor(store, "command:old")?.data).toMatchObject({ accepted: true, messageId: "earlier:command:old" });
      await store.publish(command("command:old", "host:receiver"));          // republished
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(receive).not.toHaveBeenCalled();
      expect(store.get(seenKey("host:receiver", "command:old"))).toBeDefined(); // older runtimes still rely on it
    });

    // review/astra on #58, F2: a runtime before this change claims only the shared key and never
    // reads the owner's store. Its admission is get(key) then put(key, ifVersion 0).
    const olderRuntimeAdmits = (store: MeshStore, commandId: string) => ({
      checked: store.get(seenKey("host:receiver", commandId), { fresh: true }) === undefined,
      claim: () => store.put({
        key: seenKey("host:receiver", commandId), identity: identity("host:receiver"), ifVersion: 0,
        value: { format: 1, hostId: "host:receiver", commandId, targetId: "agent:target", expiresAt: Date.now() + 60_000 },
      }).then(() => true, () => false),
    });

    it("run a command once when an older runtime of the same host checked it first and claims it later", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const receiver = plane(meshRoot, "host:receiver");
      await store.publish(command("command:race", "host:receiver", receiver.incarnation));
      const older = olderRuntimeAdmits(store, "command:race");               // paused after its check
      expect(older.checked).toBe(true);
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      const olderRuns = (await older.claim()) ? 1 : 0;                       // it resumes and claims
      expect(receive.mock.calls.length + olderRuns).toBe(1);
    });

    it("run a command once when an older runtime of the same host claimed it first", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.publish(command("command:taken", "host:receiver"));
      const older = olderRuntimeAdmits(store, "command:taken");
      expect(older.checked && await older.claim()).toBe(true);               // it runs the command
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:taken")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:taken")?.data).toMatchObject({ accepted: false });
    });

    // review/astra on #58, F1: the deadline can pass while the claim waits for a lock, or after it
    // commits and before its promise resumes; the handler must not run then.
    const expiringCommand = async (meshRoot: string, commandId: string, deadlineInMs: number, ownerIncarnation: string) => {
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const requestedAt = Date.now();
      await store.publish({ ...command(commandId, "host:receiver"),
        data: { ...command(commandId, "host:receiver", ownerIncarnation).data, requestedAt, deadlineAt: requestedAt + deadlineInMs } });
      return store;
    };

    it("do not run a command whose claim returned after its deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const receiver = plane(meshRoot, "host:receiver");
      const store = await expiringCommand(meshRoot, "command:late", 400, receiver.incarnation);
      const put = MeshStore.prototype.put;
      vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
        const result = await put.call(this, input);                           // the claim commits ...
        if (this.root === meshRoot && input.ifVersion === 0) await new Promise((resolve) => setTimeout(resolve, 600));
        return result;                                                        // ... and returns late
      });
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(ackFor(store, "command:late")).toBeDefined(), { timeout: 3_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:late")?.data).toMatchObject({ accepted: false, error: "Fabric control command expired" });
      expect(ownStore(meshRoot, "host:receiver").get(seenKey("host:receiver", "command:late"))?.value)
        .toMatchObject({ acceptance: { accepted: false, error: "Fabric control command expired" } });
    });

    it("do not run a command whose shared claim lock waited past its deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const receiver = plane(meshRoot, "host:receiver");
      const store = await expiringCommand(meshRoot, "command:locked", 400, receiver.incarnation);
      await store.put({ key: "warm", value: 1, identity: identity("host:receiver") });
      const lock = path.join(store.root, ".lock");
      fs.mkdirSync(lock);                                                     // another writer holds it
      fs.writeFileSync(path.join(lock, "owner"), `held\n${process.pid}\n${Date.now()}\n`);   // a live owner
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 700));              // past the deadline
      fs.rmSync(lock, { recursive: true, force: true });                     // the lock is released
      await vi.waitFor(() => expect(ackFor(store, "command:locked")).toBeDefined(), { timeout: 5_000, interval: 20 });
      expect(receive).not.toHaveBeenCalled();
      expect(ackFor(store, "command:locked")?.data).toMatchObject({ accepted: false, error: "Fabric control command expired" });
    });

    // smarty-dev#816: shared claims were kept until their command left the event log (compacted
    // only at 64 MiB); 2,858 expired claims made up 46% of the shared state and saturated the lock.
    it("in the shared state go once expired plus a grace when the fleet owner allows it and the command had its own deadline", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const start = Date.now();
      const MINUTE = 60_000;
      const later = start + 16 * MINUTE;                                   // past the 15-min sweep interval
      const claim = (commandId: string, value: Record<string, unknown>) => store.put({
        key: seenKey("host:other", commandId), identity: identity("host:other"), ifVersion: 0,
        value: { format: 1, hostId: "host:other", commandId, targetId: "agent:target", ...value },
      });
      for (const id of ["command:flagged", "command:grace", "command:legacy"]) {
        await store.publish(command(id, "host:other"));                    // all still in the log
      }
      await claim("command:flagged", { expiresAt: start, explicitDeadline: true });
      await claim("command:grace", { expiresAt: later - 5 * MINUTE, explicitDeadline: true });   // inside the grace
      await claim("command:legacy", { expiresAt: start });                // an older writer: no flag
      await store.put({ key: CONTROL_CLAIMS_POLICY_KEY, value: { version: 1, sharedClaims: "expiry" }, identity: identity("host:owner") });
      const receiver = plane(meshRoot, "host:receiver");
      receiver.start(() => ({ accepted: true }));
      const sender = plane(meshRoot, "host:sender");
      sender.start(() => ({ accepted: false }));
      await sender.request("host:receiver", "agent:target", "steer", { message: "once", ownerIncarnation: receiver.incarnation });
      const fresh = store.listAll("topology/control-seen/").find((entry) =>
        (entry.value as { hostId?: string }).hostId === "host:receiver");
      expect(fresh?.value).toMatchObject({ explicitDeadline: true });    // new claims carry the flag
      const bulk = Array.from({ length: 137 }, (_, n) => `command:bulk-${n}`);
      const retiredValue = (hostId: string, commandId: string) => ({ format: 1, hostId, commandId,
        targetId: "agent:target", expiresAt: start, explicitDeadline: true, sequence: 0 });
      await store.writeBatch({ identity: identity("host:other"), ops: bulk.map(commandId => ({ kind: "put" as const,
        key: seenKey("host:other", commandId), value: retiredValue("host:other", commandId) })) });
      const own = ownStore(meshRoot, "host:receiver");
      await own.writeBatch({ identity: identity("host:receiver"), ops: bulk.map(commandId => ({ kind: "put" as const,
        key: seenKey("host:receiver", commandId), value: retiredValue("host:receiver", commandId) })) });
      const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
      const now = Date.now;
      vi.spyOn(Date, "now").mockImplementation(() => now() - start + later);
      // A wall-clock jump does not fire already-armed real timers. Trigger the
      // event-driven maintenance drain; the product also keeps its real sweep deadline.
      await store.publish({ topic: "test.maintenance", from: identity("host:owner") });
      await vi.waitFor(() => expect(store.get(seenKey("host:other", "command:flagged"))).toBeUndefined(),
        { timeout: 3_000, interval: 20 });
      expect(store.get(seenKey("host:other", "command:grace"))).toBeDefined();
      expect(store.get(seenKey("host:other", "command:legacy"))).toBeDefined();   // still in the log
      const sweeps = batches.mock.calls.filter(([input]) => input.ops.every((op) => op.kind === "delete")
        && input.ops.some((op) => op.key.startsWith("topology/control-seen/")));
      expect(sweeps.length).toBeGreaterThanOrEqual(1);
      await vi.waitFor(() => {
        expect(bulk.every(id => !store.get(seenKey("host:other", id), { fresh: true }))).toBe(true);
        expect(bulk.every(id => !own.get(seenKey("host:receiver", id), { fresh: true }))).toBe(true);
      }, { timeout: 3_000, interval: 20 });
      expect(sweeps.every(([input]) => input.ops.length <= 64 && input.ops.every(op => op.ifVersion !== undefined))).toBe(true);
    });

    // review/astra on #65: a runtime before phase 1 checks the deadline only at admission and knows
    // only the shared claim. Without the fleet owner's policy, the shared claim must outlive expiry.
    it("keep the shared claim for a paused older runtime through the sweep and tombstone eviction", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const requestedAt = Date.now();
      const receiver = plane(meshRoot, "host:receiver");
      await store.publish({ ...command("command:paused", "host:receiver"),
        data: { ...command("command:paused", "host:receiver", receiver.incarnation).data, requestedAt, deadlineAt: requestedAt + 5_000 } });
      const older = olderRuntimeAdmits(store, "command:paused");            // passed admission, then pauses
      expect(older.checked).toBe(true);
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      expect(store.get(seenKey("host:receiver", "command:paused"))?.value).toMatchObject({ explicitDeadline: true });
      const now = Date.now;
      vi.spyOn(Date, "now").mockImplementation(() => now() + 16 * 60_000);   // past expiry, grace and sweep
      await new Promise((resolve) => setTimeout(resolve, 300));             // sweeps run
      for (let index = 0; index < 1_010; index++) {                         // evict every older tombstone
        await store.writeBatch({ identity: identity("host:noise"), ops: [
          { kind: "put", key: `noise/${index}`, value: index }, { kind: "delete", key: `noise/${index}` },
        ] });
      }
      const olderRuns = (await older.claim()) ? 1 : 0;                      // the older runtime resumes
      expect(receive.mock.calls.length + olderRuns).toBe(1);
      expect(store.get(seenKey("host:receiver", "command:paused"))).toBeDefined();
    }, 60_000);

    it("are deleted only once expired and their command has left the log", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const storeOptions: MeshStoreOptions = { maxEventLogBytes: 80_000, retainedEventLogBytes: 40_000 };
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions);
      // smarty-dev#883: a 200 ms deadline expired before a slow Windows runner claimed the command.
      const sender = plane(meshRoot, "host:sender", storeOptions, { acknowledgementTimeoutMs: 1_000 });
      const receiver = plane(meshRoot, "host:receiver", storeOptions, { acknowledgementTimeoutMs: 1_000 });
      const receive = vi.fn(() => ({ accepted: true }));
      sender.start(() => ({ accepted: false }));
      receiver.start(receive);
      await sender.request("host:receiver", "agent:target", "steer", { message: "once", ownerIncarnation: receiver.incarnation });
      const commandId = (store.read({ topic: "fabric.control.command", limit: 10 }).at(-1)!.data as { commandId: string }).commandId;
      const own = ownStore(meshRoot, "host:receiver");
      // Expired (at deadline + 1 s) and past one more 1 s cleanup pass, but still in the log.
      await new Promise((resolve) => setTimeout(resolve, 3_200));
      expect(own.get(seenKey("host:receiver", commandId))).toBeDefined();
      for (let index = 0; index < 100; index++) {                          // rotate it out of the log
        await store.publish({ topic: "compact", from: identity("host:publisher"), text: "x".repeat(900) });
      }
      expect(store.oldestSequence()).toBeGreaterThan(1);
      await vi.waitFor(() => expect(own.get(seenKey("host:receiver", commandId))).toBeUndefined(), { timeout: 5_000, interval: 20 });
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);
  });

  it("routes to one execution owner and returns its acknowledgement", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const bystander = plane(meshRoot, "host:bystander");
    const receive = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: "local:" + command.commandId,
    }));
    const observe = vi.fn(() => ({ accepted: true }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);
    bystander.start(observe);

    await expect(
      sender.request("host:receiver", "agent:target", "steer", {
        message: "focus",
        triggerTurn: false, ownerIncarnation: receiver.incarnation }),
    ).resolves.toMatchObject({
      queued: true,
      routed: "mesh",
      acknowledged: true,
      messageId: expect.stringMatching(/^local:/),
    });
    expect(receive).toHaveBeenCalledWith(
      expect.objectContaining({
        targetId: "agent:target",
        operation: "steer",
        message: "focus",
        triggerTurn: false,
        replyTo: "host:sender",
      }),
      expect.objectContaining({ id: "host:sender" }),
      expect.any(AbortSignal),
      "mesh",
    );
    expect(observe).not.toHaveBeenCalled();
  });

  it("round-trips boolean trigger receipts without inventing a legacy or malformed report", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-trigger-"));
    roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender");
    const receiver = plane(path.join(root, "mesh"), "host:receiver");
    const reports = [true, false, undefined, "true", 1];
    sender.start(() => ({ accepted: false }));
    receiver.start(() => ({ accepted: true, triggered: reports.shift() } as never));
    for (const report of [true, false, undefined, undefined, undefined]) {
      const receipt = await sender.request("host:receiver", "session:main", "followUp", { message: "hello", ownerIncarnation: receiver.incarnation });
      if (report === undefined) expect(receipt).not.toHaveProperty("triggered");
      else expect(receipt.triggered).toBe(report);
    }
  });

  it("round-trips held wake reasons through control ACKs and validates malformed reports", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-backoff-")); roots.push(root);
    const sender = plane(path.join(root, "mesh"), "host:sender");
    const receiver = plane(path.join(root, "mesh"), "host:receiver");
    const reason = "provider-backoff until 2026-10-01T08:35:16.663Z";
    const reports = [
      { triggered: false, reason }, { triggered: true, reason },
      { reason }, { triggered: false, reason: 123 }, { triggered: false, reason: "x".repeat(201) },
    ];
    sender.start(() => ({ accepted: false }));
    receiver.start(() => ({ accepted: true, ...reports.shift() } as never));
    for (const expectedReason of [reason, undefined, undefined, undefined, undefined]) {
      const receipt = await sender.request("host:receiver", "session:main", "followUp", { message: "held", ownerIncarnation: receiver.incarnation });
      if (expectedReason) expect(receipt).toMatchObject({ triggered: false, reason: expectedReason });
      else expect(receipt).not.toHaveProperty("reason");
    }
  });

  // smarty-dev#1495: a followUp through the owner reports the target Main's queue, so the
  // sender can switch to steer; a malformed count from the owner is dropped, not passed on.
  it("passes the owner's followUp queue depth to the sender, and drops a malformed one", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const replies = [
      { accepted: true, messageId: "m1", pendingFollowUps: 3, oldestAgeS: 140 },
      { accepted: true, messageId: "m2", pendingFollowUps: -1, oldestAgeS: 2 },
      { accepted: true, messageId: "m3", pendingFollowUps: 1.5, oldestAgeS: "9" },
      { accepted: true, messageId: "m4" },
      { accepted: true, messageId: "m5", pendingFollowUps: 1, oldestAgeS: 9, coalesced: true, replacedMessageId: "m4" },
      { accepted: true, messageId: "m6", coalesced: "yes", replacedMessageId: 4 },
      { accepted: true, messageId: "m7", pendingFollowUps: 4, oldestAgeS: 700, stalled: true },   // smarty-dev#1826
      { accepted: true, messageId: "m8", pendingFollowUps: 4, oldestAgeS: 700, stalled: "yes" },
    ];
    sender.start(() => ({ accepted: false }));
    receiver.start(() => replies.shift() as never);
    const send = () => sender.request("host:receiver", "session:main", "followUp", { message: "later", ownerIncarnation: receiver.incarnation });
    await expect(send()).resolves.toEqual({ queued: true, messageId: "m1", routed: "mesh", acknowledged: true, pendingFollowUps: 3, oldestAgeS: 140 });
    for (const messageId of ["m2", "m3", "m4"]) {
      await expect(send()).resolves.toEqual({ queued: true, messageId, routed: "mesh", acknowledged: true });
    }
    // A coalesced followUp (smarty-dev#1495) says which held one it replaced; a malformed report is dropped.
    await expect(send()).resolves.toEqual({
      queued: true, messageId: "m5", routed: "mesh", acknowledged: true, pendingFollowUps: 1, oldestAgeS: 9, coalesced: true, replacedMessageId: "m4",
    });
    await expect(send()).resolves.toEqual({ queued: true, messageId: "m6", routed: "mesh", acknowledged: true });
    await expect(send()).resolves.toEqual({
      queued: true, messageId: "m7", routed: "mesh", acknowledged: true, pendingFollowUps: 4, oldestAgeS: 700, stalled: true,
    });
    await expect(send()).resolves.toEqual({ queued: true, messageId: "m8", routed: "mesh", acknowledged: true, pendingFollowUps: 4, oldestAgeS: 700 });
  });

  it("returns an authenticated result with the caller's actor binding", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const response = { id: "message:1", text: "done" };
    const receive = vi.fn((command: { binding?: unknown }) => ({
      accepted: true,
      messageId: "message:1",
      result: response,
    }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);

    await expect(
      sender.requestResult(
        "host:receiver",
        "actor:target",
        "ask",
        {
          message: "inspect",
          binding: { model: "provider/session-b", thinking: "high" }, ownerIncarnation: receiver.incarnation },
      ),
    ).resolves.toEqual(response);
    expect(receive).toHaveBeenCalledWith(
      expect.objectContaining({
        targetId: "actor:target",
        operation: "ask",
        binding: { model: "provider/session-b", thinking: "high" },
        deadlineAt: expect.any(Number),
      }),
      expect.objectContaining({ id: "host:sender" }),
      expect.any(AbortSignal),
      "mesh",
    );
  });
  it("ignores an acknowledgement forged by a different mesh identity", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    // Identity validation is the subject, not filesystem throughput on shared CI runners.
    const sender = plane(meshRoot, "host:sender", {}, { acknowledgementTimeoutMs: 5_000 });
    const receiver = plane(meshRoot, "host:receiver");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    sender.start(() => ({ accepted: false }));
    receiver.start(async (command) => {
      // Publish the forgery before the owner can emit its authentic acknowledgement.
      await store.publish({
        topic: "fabric.control.ack",
        kind: "accepted",
        from: identity("host:bystander"),
        to: "host:sender",
        data: {
          version: 1,
          commandId: command.commandId,
          targetId: command.targetId,
          accepted: true,
          messageId: "forged",
        },
      });
      return { accepted: true, messageId: "real:" + command.commandId };
    });

    await expect(
      sender.request("host:receiver", "agent:target", "steer", { message: "focus", ownerIncarnation: receiver.incarnation }),
    ).resolves.toMatchObject({
      acknowledged: true,
      messageId: expect.stringMatching(/^real:/),
    });
    expect(store.read({ topic: "fabric.control.ack", after: 0 })).toMatchObject([
      { from: { id: "host:bystander" }, data: { messageId: "forged" } },
      { from: { id: "host:receiver" }, data: { messageId: expect.stringMatching(/^real:/) } },
    ]);
  });

  it("recovers an unexpired current-incarnation command queued before polling starts", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const receiver = plane(meshRoot, "host:receiver");
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        ownerIncarnation: receiver.incarnation,
        commandId: "command:before-start",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "recover",
        requestedAt: Date.now(),
      },
    });
    const receive = vi.fn(() => ({ accepted: true, messageId: "recovered" }));
    receiver.start(receive);
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).toHaveBeenCalledTimes(1);
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:before-start",
          accepted: true,
        }),
      }),
    );
  });

  it("rejects an interrupted durable claim as indeterminate after restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const commandId = "command:interrupted";
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId,
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "unknown outcome",
        requestedAt: Date.now(),
      },
    });
    const seenKey =
      "topology/control-seen/" +
      createHash("sha256").update(`host:receiver\0${commandId}`).digest("hex");
    await store.put({
      key: seenKey,
      value: {
        format: 1,
        hostId: "host:receiver",
        commandId,
        targetId: "agent:target",
        expiresAt: Date.now() + 1_000,
      },
      identity: identity("host:receiver"),
      ifVersion: 0,
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId,
          accepted: false,
          error: "Fabric control outcome is indeterminate after owner restart",
        }),
      }),
    );
  });

  it("does not re-execute a command republished after owner restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const firstOwner = plane(meshRoot, "host:receiver");
    const firstHandler = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: command.commandId,
    }));
    sender.start(() => ({ accepted: false }));
    firstOwner.start(firstHandler);
    await sender.request("host:receiver", "agent:target", "steer", { message: "once", ownerIncarnation: firstOwner.incarnation });
    expect(firstHandler).toHaveBeenCalledTimes(1);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const original = store.read({ topic: "fabric.control.command", limit: 10 })[0];
    expect(original).toBeDefined();
    await firstOwner.close();

    const restartedOwner = plane(meshRoot, "host:receiver");
    const restartedHandler = vi.fn(() => ({ accepted: true }));
    restartedOwner.start(restartedHandler);
    await store.publish({
      topic: "fabric.control.command",
      kind: original!.kind,
      from: original!.from,
      ...(original!.to ? { to: original!.to } : {}),
      ...(original!.data === undefined ? {} : { data: original!.data }),
    });
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(restartedHandler).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 }).length).toBeGreaterThan(1);
  });

  it("rejects a replay outside the acknowledgement lifetime", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:replayed",
        ownerIncarnation: receiver.incarnation,
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "stale",
        requestedAt: Date.now() - 5_000,
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          commandId: "command:replayed",
          accepted: false,
          error: "Fabric control command expired",
        }),
      }),
    );
  });

  // smarty-dev#367: every restarting owner replays the retained log from its start; it
  // answered each long-expired command again, 21,479 acks nobody waited for.
  it("does not answer a command whose sender stopped waiting long ago", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:history",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "old",
        requestedAt: Date.now() - 10 * 60_000,
      },
    });
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);                                     // a restart replays the log
    await new Promise((resolve) => setTimeout(resolve, 150));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toEqual([]);
  });

  // smarty-dev#9751: a command a bridge delivered long after its deadline (its bridged copy is new on this mesh)
  // used to be skipped like history, with no ACK: its sender never learnt. It now gets the expired ACK.
  it("answers a command a bridge delivered past its deadline with an expired ACK, never a silent skip", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true }));
    receiver.start(receive);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:late",
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "late",
        requestedAt: Date.now() - 11 * 60_000,                 // a 30 s command, carried 11 min late
        deadlineAt: Date.now() - 11 * 60_000 + 30_000,
        bridge: { from: "qs32729", id: "evt-remote" },
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 150));

    expect(receive).not.toHaveBeenCalled();
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(expect.objectContaining({
      to: "host:sender",
      data: expect.objectContaining({ commandId: "command:late", accepted: false, error: "Fabric control command expired" }),
    }));
  });

  it("final-drains a command published immediately before close", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const receiver = plane(meshRoot, "host:receiver");
    const receive = vi.fn(() => ({ accepted: true, messageId: "drained" }));
    receiver.start(receive);
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await store.publish({
      topic: "fabric.control.command",
      kind: "steer",
      from: identity("host:sender"),
      to: "host:receiver",
      data: {
        version: 1,
        commandId: "command:before-close",
        ownerIncarnation: receiver.incarnation,
        targetId: "agent:target",
        operation: "steer",
        replyTo: "host:sender",
        message: "finish",
        requestedAt: Date.now(),
      },
    });

    await receiver.close();

    expect(receive).toHaveBeenCalledTimes(1);
    expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toContainEqual(
      expect.objectContaining({
        to: "host:sender",
        data: expect.objectContaining({
          commandId: "command:before-close",
          accepted: true,
        }),
      }),
    );
  });

  it("does not re-execute a retained command after event-log compaction", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const storeOptions: MeshStoreOptions = {
      maxEventLogBytes: 80_000,
      retainedEventLogBytes: 75_000,
    };
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000, storeOptions);
    for (let index = 0; index < 8; index++) {
      await store.publish({
        topic: "prefill",
        from: identity("host:prefill"),
        text: "p".repeat(900),
      });
    }
    const sender = plane(meshRoot, "host:sender", storeOptions);
    const receiver = plane(meshRoot, "host:receiver", storeOptions);
    const receive = vi.fn((command: { commandId: string }) => ({
      accepted: true,
      messageId: command.commandId,
    }));
    sender.start(() => ({ accepted: false }));
    receiver.start(receive);

    await sender.request("host:receiver", "agent:target", "steer", { message: "once", ownerIncarnation: receiver.incarnation });
    expect(receive).toHaveBeenCalledTimes(1);
    for (let index = 0; index < 62; index++) {
      await store.publish({
        topic: "compact",
        from: identity("host:publisher"),
        text: "x".repeat(900),
      });
    }
    await new Promise((resolve) => setTimeout(resolve, 160));

    expect(receive).toHaveBeenCalledTimes(1);
  });

  it("keeps control traffic responsive while an ask is running and cancels the owner", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const controller = new AbortController();
    let askStarted = false;
    let askAborted = false;
    receiver.start((command, _from, signal) => {
      if (command.operation !== "ask") {
        return { accepted: true, messageId: `accepted:${command.operation}` };
      }
      askStarted = true;
      return new Promise((resolve) => {
        signal.addEventListener("abort", () => {
          askAborted = true;
          resolve({ accepted: false, error: "actor request cancelled" });
        }, { once: true });
      });
    });
    sender.start(() => ({ accepted: false }));

    const ask = sender.requestResult(
      "host:receiver",
      "actor:target",
      "ask",
      { message: "inspect", ownerIncarnation: receiver.incarnation },
      "host:receiver",
      { timeoutMs: 2_000, signal: controller.signal },
    );
    await vi.waitFor(() => expect(askStarted).toBe(true));
    await expect(
      sender.request("host:receiver", "actor:target", "stop", { ownerIncarnation: receiver.incarnation }),
    ).resolves.toMatchObject({ acknowledged: true });

    controller.abort();
    await expect(ask).rejects.toThrow("Remote Fabric request cancelled");
    await vi.waitFor(() => expect(askAborted).toBe(true));
  });

  it("fences an immediately cancelled command before mesh admission", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    const publish = sender.mesh.publish.bind(sender.mesh);
    let release!: () => void, finish!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const finished = new Promise<void>(resolve => { finish = resolve; });
    vi.spyOn(sender.mesh, "publish").mockImplementation(async (input) => {
      try {
        if (input.topic === "fabric.control.command" && input.kind === "ask") await gate;
        return await publish(input);
      } finally { finish(); }
    });
    let ownerAborted = false;
    let ownerEntered = false;
    receiver.start((command, _from, signal) => {
      if (command.operation !== "ask") return { accepted: true };
      ownerEntered = true;
      return new Promise((resolve) => {
        signal.addEventListener("abort", () => {
          ownerAborted = true;
          resolve({ accepted: false, error: "cancelled" });
        }, { once: true });
      });
    });
    sender.start(() => ({ accepted: false }));
    const controller = new AbortController();

    const request = sender.requestResult(
      "host:receiver",
      "actor:target",
      "ask",
      { message: "inspect", ownerIncarnation: receiver.incarnation },
      "host:receiver",
      { timeoutMs: 1_000, signal: controller.signal },
    );
    controller.abort();

    await expect(request).rejects.toThrow("Remote Fabric request cancelled");
    release(); await finished;
    const commands = new MeshStore(meshRoot, 64 * 1024, 1_000).read({ topic: "fabric.control.command" });
    expect(commands).toHaveLength(0);
    expect(ownerAborted).toBe(false);
    expect(ownerEntered).toBe(false);
  });

  it("retains a completed ask outcome through its request deadline", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(
      meshRoot,
      "host:receiver",
      {},
      { pollMs: 20, acknowledgementTimeoutMs: 80 },
    );
    const receive = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 120));
      return { accepted: true, result: { id: "message:1", text: "done" } };
    });
    receiver.start(receive);
    sender.start(() => ({ accepted: false }));

    await expect(
      sender.requestResult(
        "host:receiver",
        "actor:target",
        "ask",
        { message: "inspect", ownerIncarnation: receiver.incarnation },
        "host:receiver",
        { timeoutMs: 500 },
      ),
    ).resolves.toMatchObject({ text: "done" });
    const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
    const original = store.read({ topic: "fabric.control.command", limit: 10 })
      .find((event) => event.kind === "ask");
    expect(original).toBeDefined();
    await store.publish({
      topic: "fabric.control.command",
      kind: original!.kind,
      from: original!.from,
      ...(original!.to ? { to: original!.to } : {}),
      ...(original!.data === undefined ? {} : { data: original!.data }),
    });
    await new Promise((resolve) => setTimeout(resolve, 180));

    expect(receive).toHaveBeenCalledTimes(1);
  });

  // smarty-dev#367: a retry after an acknowledgement timeout delivered the message twice.
  it("waits past the deadline for the acknowledgement of a command the owner admitted in time", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    // A 400 ms deadline leaves a slow runner (windows-latest) time to admit the command;
    // the handler then acknowledges after the deadline but inside the 2 x 400 ms grace.
    const sender = plane(meshRoot, "host:sender", {}, { pollMs: 20, acknowledgementTimeoutMs: 400 });
    const receiver = plane(meshRoot, "host:receiver", {}, { pollMs: 20, acknowledgementTimeoutMs: 400 });
    // Admitted within the deadline, acknowledged after it (a slow handler or a contended
    // mesh lock): the sender must report the delivery, not a timeout.
    receiver.start(async () => {
      await new Promise((resolve) => setTimeout(resolve, 500));
      return { accepted: true, messageId: "late-but-delivered" };
    });
    sender.start(() => ({ accepted: false }));
    await expect(sender.request("host:receiver", "agent:target", "steer", { message: "slow ack", ownerIncarnation: receiver.incarnation }))
      .resolves.toMatchObject({ acknowledged: true, messageId: "late-but-delivered" });
  });

  it("still times out, after the deadline plus a bounded grace, when no owner answers, and withdraws the unclaimed message", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender", {}, { pollMs: 20, acknowledgementTimeoutMs: 100 });
    sender.start(() => ({ accepted: false }));
    const started = Date.now();
    // No receiver runs, so nobody claimed it: smarty-dev#6729 makes that a definite non-delivery.
    const error = await sender.request("host:receiver", "agent:target", "steer", { ownerIncarnation: fixtureOwnerIncarnation, message: "nobody home" })
      .then(() => undefined, (failure: Error) => failure);
    expect(error?.message).toContain("Timed out waiting for the remote Fabric owner to acknowledge agent:target");
    expect(error?.message).toContain("not delivered, nothing was queued");
    expect(error?.message).not.toContain("outcome is unknown");
    expect(error).toMatchObject({ code: "FABRIC_CONTROL_NOT_DELIVERED", notDelivered: true });
    const waited = Date.now() - started;
    expect(waited).toBeGreaterThanOrEqual(100 + 200 - 20);   // the deadline plus 2 x 100 ms of grace
    expect(waited).toBeLessThan(2_000);

    // The owner resumes later: it answers the withdrawn command as expired and never runs it.
    const handler = vi.fn(() => ({ accepted: true, messageId: "late" }));
    const receiver = plane(meshRoot, "host:receiver", {}, { pollMs: 20, acknowledgementTimeoutMs: 100 });
    receiver.start(handler);
    const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.ack" })).toHaveLength(1));
    expect(mesh.read({ topic: "fabric.control.ack" })[0]!.data).toMatchObject({ accepted: false, error: "Fabric control command expired", notRun: true });
    expect(handler).not.toHaveBeenCalled();
  });

  it("keeps the outcome unknown when the owner claimed the message before the sender timed out", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender", {}, { pollMs: 20, acknowledgementTimeoutMs: 100 });
    const receiver = plane(meshRoot, "host:receiver", {}, { pollMs: 20, acknowledgementTimeoutMs: 100 });
    sender.start(() => ({ accepted: false }));
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    const handler = vi.fn(async () => { await held; return { accepted: true, messageId: "slow" }; });
    receiver.start(handler);
    const error = await sender.request("host:receiver", "agent:target", "steer", { message: "claimed", ownerIncarnation: receiver.incarnation })
      .then(() => undefined, (failure: Error) => failure);
    release();
    expect(handler).toHaveBeenCalledTimes(1);
    expect(error?.message).toContain("the outcome is unknown and it may still be delivered");
    expect(error).not.toHaveProperty("notDelivered");
  });

  it("surfaces owner rejection instead of reporting an unverified queue", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const sender = plane(meshRoot, "host:sender");
    const receiver = plane(meshRoot, "host:receiver");
    sender.start(() => ({ accepted: false }));
    receiver.start(() => ({ accepted: false, error: "target already settled" }));

    await expect(
      sender.request("host:receiver", "agent:missing", "stop", { ownerIncarnation: receiver.incarnation }),
    ).rejects.toThrow("target already settled");
  });

  // smarty-dev#816: an owner that picked a command up after its deadline rejected it as expired
  // (4 of 2,802 commands, mostly in a fleet relaunch); the sender's manual retry worked.
  describe("when the owner rejects a command as expired", () => {
    const run = async (operation: "followUp" | "stop", rejection?: string) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");                     // a 1 s deadline
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn((command: { commandId: string }) => rejection
        ? { accepted: false, error: rejection }
        : { accepted: true, messageId: "delivered:" + command.commandId });
      sender.start(() => ({ accepted: false }));
      const outcome = sender.request("host:receiver", "agent:target", operation, { message: "late", ownerIncarnation: receiver.incarnation })
        .then(
          (value) => ({ value, error: undefined as Error | undefined }),
          (error: Error) => ({ value: undefined, error: error as Error | undefined }),
        );
      // The owner polls only after the first command's deadline; later commands are on time.
      if (!rejection) await new Promise((resolve) => setTimeout(resolve, 1_300));
      receiver.start(receive);
      const settled = await outcome;
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const commands = store
        .read({ topic: "fabric.control.command", limit: 100 })
        .filter((event) => event.kind === operation);
      const acks = store.read({ topic: "fabric.control.ack", limit: 100 })
        .map((event) => event.data as { commandId: string });
      return { ...settled, receive, commands, acks, store };
    };

    const settle = <T>(promise: Promise<T>) => promise.then(
      (value) => ({ value, error: undefined as Error | undefined }),
      (error: Error) => ({ value: undefined as T | undefined, error: error as Error | undefined }),
    );

    // review/astra F1 on #121: the owner delivers, then dies before its acknowledgement; a new
    // runtime of the same host replays the command past its deadline while the sender still waits.
    const restartPastDeadline = async (crash: "acknowledgement" | "outcome") => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");                     // a 1 s deadline, 2 s grace
      const firstOwner = plane(meshRoot, "host:receiver");
      const receive = vi.fn((command: { commandId: string }) =>
        ({ accepted: true, messageId: "delivered:" + command.commandId }));
      let crashed = true;
      const publish = MeshStore.prototype.publish;
      vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
        if (crashed && input.topic === "fabric.control.ack") throw new Error("owner crashed");
        return publish.call(this, input);
      });
      const put = MeshStore.prototype.put;
      vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
        // "outcome": the claim commits, the handler runs, the outcome is never recorded.
        if (crashed && crash === "outcome" && this.root.includes("control-seen") && (input.value as { acceptance?: unknown }).acceptance) {
          throw new Error("owner crashed");
        }
        return put.call(this, input);
      });
      sender.start(() => ({ accepted: false }));
      firstOwner.start(receive);
      const outcome = settle(sender.request("host:receiver", "agent:target", "followUp", { message: "once", ownerIncarnation: firstOwner.incarnation }));
      await vi.waitFor(() => expect(receive).toHaveBeenCalledTimes(1), { timeout: 900, interval: 10 });
      await firstOwner.close();
      crashed = false;
      await new Promise((resolve) => setTimeout(resolve, 1_300));         // past the deadline
      const restarted = plane(meshRoot, "host:receiver");                 // fresh process state
      restarted.start(receive);
      const settled = await outcome;
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const commands = store.read({ topic: "fabric.control.command", limit: 100 })
        .filter((event) => event.kind === "followUp");
      const acks = store.read({ topic: "fabric.control.ack", limit: 100 }).map((event) => event.data);
      return { ...settled, receive, commands, acks };
    };

    it("answers a delivered message's recorded outcome after an owner restart past the deadline, without a retry", async () => {
      const { value, error, receive, commands, acks } = await restartPastDeadline("acknowledgement");
      expect(error).toBeUndefined();
      expect(commands).toHaveLength(1);
      const first = commands[0]!.data as { commandId: string };
      expect(value).toMatchObject({ acknowledged: true, messageId: "delivered:" + first.commandId });
      expect(acks).toEqual([expect.objectContaining({ commandId: first.commandId, accepted: true })]);
      expect(acks[0]).not.toHaveProperty("notRun");
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);

    it("reports a claimed message without an outcome as indeterminate after a restart past the deadline, without a retry", async () => {
      const { error, receive, commands, acks } = await restartPastDeadline("outcome");
      expect(error?.message).toBe("Fabric control outcome is indeterminate after owner restart");
      expect(commands).toHaveLength(1);
      expect(acks).toHaveLength(1);
      expect(acks[0]).not.toHaveProperty("notRun");
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);

    // review/astra F1 round 2 on #121: a runtime before smarty-dev#643 claims only the shared key.
    const sharedKey = (commandId: string) =>
      "topology/control-seen/" + createHash("sha256").update(`host:receiver\0${commandId}`).digest("hex");
    const olderRuntimeClaims = (store: MeshStore, commandId: string, acceptance?: object) => store.put({
      key: sharedKey(commandId), identity: identity("host:receiver"), ifVersion: 0, value: {
        format: 1, hostId: "host:receiver", commandId, targetId: "agent:target", expiresAt: Date.now() + 60_000,
        ...(acceptance ? { acceptance } : {}),
      },
    });

    it.each([
      ["its outcome", { accepted: true, messageId: "older:delivered" }],
      ["no outcome yet", undefined],
    ])("(e) does not report notRun or retry a message an older runtime claimed and delivered, with %s", async (_, recorded) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const sender = plane(meshRoot, "host:sender");                     // a 1 s deadline
      sender.start(() => ({ accepted: false }));
      const outcome = settle(sender.request("host:receiver", "agent:target", "followUp", { message: "once" }));
      let commandId = "";
      await vi.waitFor(() => {
        commandId = (store.read({ topic: "fabric.control.command", limit: 10 })[0]?.data as { commandId: string }).commandId;
        expect(commandId).toBeTruthy();
      }, { timeout: 900, interval: 10 });
      await olderRuntimeClaims(store, commandId, recorded);               // it claims and delivers
      const olderDeliveries = 1;
      await new Promise((resolve) => setTimeout(resolve, 1_300));         // past the deadline
      const receiver = plane(meshRoot, "host:receiver");
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      const { value, error } = await outcome;
      if (recorded) expect(value).toMatchObject({ acknowledged: true, messageId: "older:delivered" });
      else expect(error?.message).toBe("Fabric control outcome is indeterminate after owner restart");
      const acks = store.read({ topic: "fabric.control.ack", limit: 100 }).map((event) => event.data);
      expect(acks).toHaveLength(1);
      expect(acks[0]).not.toHaveProperty("notRun");
      expect(store.read({ topic: "fabric.control.command", limit: 10 }).filter((event) => event.kind === "followUp"))
        .toHaveLength(1);
      expect(receive.mock.calls.length + olderDeliveries).toBe(1);
    }, 15_000);

    it("(f) loses the shared claim and does not report notRun when its cached read missed a later claim", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await store.put({ key: "unrelated", identity: identity("host:other"), value: 1 }); // a state file to cache
      const receiver = plane(meshRoot, "host:receiver", { readCacheMs: 60_000 });
      const receive = vi.fn(() => ({ accepted: true }));
      receiver.start(receive);
      await new Promise((resolve) => setTimeout(resolve, 150));           // past its one-time legacy move
      expect(receiver.mesh.get(sharedKey("command:warm"))).toBeUndefined(); // warm: no claim
      await olderRuntimeClaims(store, "command:warm");                     // another runtime claims, delivers
      const olderDeliveries = 1;
      expect(receiver.mesh.get(sharedKey("command:warm"))).toBeUndefined(); // the cache still misses it
      const requestedAt = Date.now() - 2_000;                             // past its deadline, still answerable
      await store.publish({
        topic: "fabric.control.command", kind: "followUp", from: identity("host:sender"), to: "host:receiver",
        data: { version: 1, commandId: "command:warm", targetId: "agent:target", operation: "followUp", replyTo: "host:sender",
          message: "m", ownerIncarnation: receiver.incarnation, requestedAt, deadlineAt: requestedAt + 1_000 },
      });
      await vi.waitFor(() => expect(store.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(1),
        { timeout: 3_000, interval: 20 });
      const ack = store.read({ topic: "fabric.control.ack", limit: 10 })[0]!.data;
      expect(ack).toMatchObject({ accepted: false, error: "Fabric control outcome is indeterminate after owner restart" });
      expect(ack).not.toHaveProperty("notRun");
      expect(receive.mock.calls.length + olderDeliveries).toBe(1);
    }, 15_000);

    // An owner before #121 answers the same text without reading its claim: never retried.
    it("does not retry an expiry acknowledgement without notRun, as an older owner sends", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-control-"));
      roots.push(root);
      const meshRoot = path.join(root, "mesh");
      const sender = plane(meshRoot, "host:sender");
      sender.start(() => ({ accepted: false }));
      const store = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const outcome = settle(sender.request("host:receiver", "agent:target", "followUp", { message: "old" }));
      let command: { commandId: string } | undefined;
      await vi.waitFor(() => {
        command = store.read({ topic: "fabric.control.command", limit: 10 })[0]?.data as { commandId: string };
        expect(command).toBeDefined();
      }, { timeout: 900, interval: 10 });
      await store.publish({
        topic: "fabric.control.ack",
        kind: "rejected",
        from: identity("host:receiver"),
        to: "host:sender",
        data: { version: 1, commandId: command!.commandId, targetId: "agent:target", accepted: false, error: "Fabric control command expired" },
      });
      const { error } = await outcome;
      expect(error?.message).toBe("Fabric control command expired");
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(store.read({ topic: "fabric.control.command", limit: 10 }).filter((event) => event.kind === "followUp"))
        .toHaveLength(1);
    }, 15_000);

    it("retries a message once with a new command, and the owner delivers it once", async () => {
      const { value, error, receive, commands, acks, store } = await run("followUp");
      expect(error).toBeUndefined();
      expect(commands).toHaveLength(2);
      const first = commands[0]!.data as { commandId: string };
      const retry = commands[1]!.data as { commandId: string };
      expect(first.commandId).not.toBe(retry.commandId);
      // The owner won the shared claim for the first command, so nothing ran it: it proves notRun,
      // and records the expiry in that claim, where every runtime, new or old, finds it.
      expect(acks.find((ack) => ack.commandId === first.commandId))
        .toMatchObject({ accepted: false, error: "Fabric control command expired", notRun: true });
      expect(store.get(sharedKey(first.commandId))?.value).toMatchObject({
        hostId: "host:receiver", commandId: first.commandId,
        acceptance: { accepted: false, error: "Fabric control command expired", notRun: true },
      });
      expect(acks.find((ack) => ack.commandId === retry.commandId)).toMatchObject({ accepted: true });
      expect(value).toMatchObject({ acknowledged: true, messageId: "delivered:" + retry.commandId });
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);

    it("does not retry a command that is not a message", async () => {
      const { error, receive, commands } = await run("stop");
      expect(error?.message).toBe("Fabric control command expired; not delivered, safe to resend.");
      expect(commands).toHaveLength(1);
      expect(receive).not.toHaveBeenCalled();
    }, 15_000);

    it("does not retry a message the owner rejected for another reason", async () => {
      const { error, receive, commands } = await run("followUp", "target already settled");
      expect(error?.message).toBe("target already settled");
      expect(commands).toHaveLength(1);
      expect(receive).toHaveBeenCalledTimes(1);
    }, 15_000);
  });
});
