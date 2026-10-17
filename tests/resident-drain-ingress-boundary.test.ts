// Keep: data safety / trust-boundary races not cheaply reachable through a UI flow (#11962).
// No native worker, signal, Git write, or real removal is needed by these probes.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX, type FabricLifecycleSubscription } from "../src/lifecycle/types.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import { hasUnresolvedWorker } from "../src/storage/retention.js";

const deferred = <T = void>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const sourceIdentity = identity("source"), targetIdentity = identity("target");
const source = { id: "source", name: "source", kind: "root" as const, rootId: "source", runner: "pi" as const,
  ownerHostId: "source", ownerIdentityId: "source" };
const participant = (id: string): FabricParticipantInfo => ({ format: 1, id, kind: "root", rootId: id, ownerHostId: id,
  ownerIdentityId: id, name: id, status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
  startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: id === "target", stale: false });
const directory = (): FabricParticipantSource => {
  const records = [participant("source"), participant("target")];
  return { list: () => records, get: id => records.find(record => record.id === id), self: () => records[1]!,
    peers: () => [], refresh: async () => {}, scheduleRefresh() {} };
};
const cleanups: Array<() => Promise<unknown>> = [];
const root = () => fs.mkdtempSync(path.join(os.tmpdir(), "resident-drain-ingress-"));
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  // Fixtures are retained. The authorized test-env recorded-call wrapper retains TMPDIR too.
});
const meshFixture = () => new MeshStore(path.join(root(), "mesh"), 64 * 1024, 100);
const control = (mesh: MeshStore, fenced = true) => {
  const plane = new FabricControlPlane(mesh, identity("owner"), { enabled: true, hostId: "owner", pollMs: 20,
    acknowledgementTimeoutMs: 5000, ...(fenced ? { canConsumeMesh: () => true } : {}) });
  cleanups.push(() => { plane.pause(true); return plane.close(); });
  return plane;
};
const command = async (plane: FabricControlPlane, id = "original") => plane.mesh.publish({
  topic: "fabric.control.command", kind: "steer", from: identity("sender"), to: "owner", data: {
    version: 1, commandId: id, targetId: "target", operation: "steer", replyTo: "sender", ownerIncarnation: plane.incarnation,
    requestedAt: Date.now(), deadlineAt: Date.now() + 30_000, message: id,
  },
});
const kick = (plane: FabricControlPlane) => { plane.pause(); plane.resume(); };
const lifecycle = (mesh: MeshStore, deliver: ConstructorParameters<typeof LifecycleBroker>[4], participants = directory()) => {
  const broker = new LifecycleBroker(mesh, targetIdentity, participants,
    { enabled: true, pollMs: 20, maxReadEvents: 100, canConsumeMesh: () => true }, deliver);
  cleanups.push(() => broker.close());
  return broker;
};
const subscribe = (broker: LifecycleBroker, once = false) => broker.subscribe({ from: "source", to: "target",
  events: ["pi.agent_settled"], delivery: "followUp", triggerTurn: false, once });
const event = (mesh: MeshStore, kind = "pi.agent_settled") => mesh.publish({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC,
  kind, from: sourceIdentity, data: { version: 1, event: kind, source, occurredAt: 42 } });

describe("resident control admission versus accepted receipt custody", () => {
  it.each(["shared-lock", "after-shared"] as const)("holds %s without handler/false ACK/claim loss, then resumes the same command once", async stage => {
    const mesh = meshFixture(), plane = control(mesh);
    const original = await command(plane);
    const reached = deferred(), release = deferred(), acknowledged = deferred();
    const write = MeshStore.prototype.writeBatch;
    let suspended = false;
    vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function(this: MeshStore, input) {
      const shared = this.root === mesh.root;
      const match = input.beforeCommit && !suspended && shared;
      if (!match) return write.call(this, input);
      suspended = true;
      if (stage === "shared-lock") {
        reached.resolve(); await release.promise;
        return write.call(this, input); // pause is checked under the actual commit lock
      }
      const committed = await write.call(this, input);
      reached.resolve(); await release.promise;
      return committed;
    });
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      const result = await publish(input);
      if (input.topic === "fabric.control.ack") acknowledged.resolve();
      return result;
    });
    const handled: string[] = [];
    plane.start(value => { handled.push(value.commandId); return { accepted: true, messageId: "same-result" }; });
    kick(plane);
    await reached.promise;
    plane.pause(true); release.resolve();
    const checkpoint = await plane.checkpointForRelease().then(() => "saved", () => "held");
    expect(handled).toEqual([]);
    expect(mesh.read({ topic: "fabric.control.ack" })).toEqual([]);
    const sharedKey = "topology/control-seen/" + createHash("sha256").update("owner\0original").digest("hex");
    if (stage === "shared-lock") { expect(mesh.get(sharedKey, { fresh: true })).toBeUndefined(); expect(checkpoint).toBe("saved"); }
    else { expect(mesh.get(sharedKey, { fresh: true })?.value).toMatchObject({ commandId: "original" }); expect(checkpoint).toBe("held"); }
    expect(mesh.read({ topic: "fabric.control.command" })[0]?.id).toBe(original.id);
    plane.resume(); plane.resume();
    await acknowledged.promise;
    plane.pause(true); await plane.checkpointForRelease();
    expect(handled).toEqual(["original"]);
    expect(mesh.read({ topic: "fabric.control.ack" })).toHaveLength(1);
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });

  it.each(["shared-lock", "after-shared"] as const)("expires an unpaused %s claim delayed past its deadline without losing its outcome/ACK", async stage => {
    vi.useFakeTimers();
    const mesh = meshFixture(), plane = control(mesh);
    const original = await command(plane);
    const reached = deferred(), release = deferred(), acknowledged = deferred();
    const write = MeshStore.prototype.writeBatch;
    let suspended = false;
    vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function(this: MeshStore, input) {
      const shared = this.root === mesh.root;
      if (!input.beforeCommit || suspended || !shared) return write.call(this, input);
      suspended = true;
      if (stage === "shared-lock") {
        reached.resolve(); await release.promise;
        return write.call(this, input);
      }
      const committed = await write.call(this, input);
      reached.resolve(); await release.promise;
      return committed;
    });
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      const result = await publish(input); if (input.topic === "fabric.control.ack") acknowledged.resolve(); return result;
    });
    const handler = vi.fn(() => ({ accepted: true }));
    plane.start(handler); kick(plane); await reached.promise;
    // No release cut occurs while this claim is in flight. Custody is not admission.
    vi.setSystemTime(Date.now() + 60_000);
    expect(handler).not.toHaveBeenCalled(); expect(mesh.read({ topic: "fabric.control.ack" })).toEqual([]);
    release.resolve(); await acknowledged.promise;
    plane.pause(true); await plane.checkpointForRelease();
    expect(handler).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.command" })[0]?.id).toBe(original.id);
    expect(mesh.read({ topic: "fabric.control.ack" }).map(value => value.data)).toEqual([
      expect.objectContaining({ commandId: "original", ownerIncarnation: plane.incarnation,
        accepted: false, error: "Fabric control command expired", notRun: true }),
    ]);
    const local = new MeshStore(path.join(mesh.root, "control-seen", createHash("sha256").update("owner").digest("hex").slice(0, 32)), 64 * 1024, 100, { stateBackend: "file" });
    const key = "topology/control-seen/" + createHash("sha256").update("owner\0original").digest("hex");
    expect(local.get(key, { fresh: true })?.value).toMatchObject({ commandId: "original", sequence: original.sequence,
      ownerIncarnation: plane.incarnation, acceptance: { accepted: false, error: "Fabric control command expired", notRun: true } });
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });

  it("finishes the accepted handler/outcome/ACK but preserves the next input across an await", async () => {
    const mesh = meshFixture(), plane = control(mesh);
    const first = await command(plane, "first"), second = await command(plane, "second");
    const entered = deferred(), finish = deferred(), secondFinished = deferred();
    const calls: string[] = [];
    plane.start(async value => {
      calls.push(value.commandId);
      if (value.commandId === "first") { entered.resolve(); await finish.promise; }
      else secondFinished.resolve();
      return { accepted: true, messageId: value.commandId };
    });
    kick(plane); await entered.promise;
    plane.pause(true); finish.resolve(); await plane.checkpointForRelease();
    expect(calls).toEqual(["first"]);
    expect(mesh.read({ topic: "fabric.control.ack" }).map(value => value.data)).toEqual([expect.objectContaining({ commandId: "first", accepted: true })]);
    expect(mesh.read({ topic: "fabric.control.command" }).map(value => value.id)).toEqual([first.id, second.id]);
    plane.resume(); await secondFinished.promise;
    plane.pause(true); await plane.checkpointForRelease();
    expect(calls).toEqual(["first", "second"]);
  });

  it.each([
    ["known no-write", () => Object.assign(new Error("ACK lock timeout"), { code: "FABRIC_MESH_LOCK_TIMEOUT" })],
    ["indeterminate", () => new Error("ACK failed, publication outcome unknown")],
  ] as const)("vetoes unconfirmed %s ACK publication, retaining the exact outcome for repair without a second handler", async (kind, makeError) => {
    const mesh = meshFixture(), plane = control(mesh);
    await command(plane);
    const publication = deferred();
    const publish = mesh.publish.bind(mesh);
    let fault = true;
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      if (input.topic === "fabric.control.ack" && fault) { publication.resolve(); throw makeError(); }
      return publish(input);
    });
    const handler = vi.fn(() => ({ accepted: true, messageId: "accepted-original" }));
    plane.start(handler); kick(plane); await publication.promise; plane.pause(true);
    await expect(plane.checkpointForRelease()).rejects.toThrow();
    expect(() => plane.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
    expect(handler).toHaveBeenCalledOnce();
    fault = false;
    // The exact durable outcome already exists: resuming repairs publication only.
    const acknowledged = deferred();
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      const result = await publish(input); if (input.topic === "fabric.control.ack") acknowledged.resolve(); return result;
    });
    // A known no-write outcome is retained and retried; an indeterminate one is not, so a later
    // duplicate command repairs its ACK. Neither re-runs the completed handler.
    plane.resume();
    if (kind === "indeterminate") await command(plane);
    await acknowledged.promise; plane.pause(true);
    if (kind === "known no-write") {
      await plane.checkpointForRelease();
      expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    } else {
      // One later success cannot reconcile an earlier unknown publication; release stays vetoed.
      await expect(plane.checkpointForRelease()).rejects.toThrow(/unsettled publication/);
    }
    expect(handler).toHaveBeenCalledOnce();
    expect(mesh.read({ topic: "fabric.control.ack" })[0]?.data).toMatchObject({ accepted: true, messageId: "accepted-original" });
  });

  it("preserves a partial shared claim after the original command deadline is spent", async () => {
    vi.useFakeTimers();
    const mesh = meshFixture(), plane = control(mesh); await command(plane);
    const reached = deferred(), release = deferred(), acknowledged = deferred();
    const write = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async input => {
      const result = await write(input); reached.resolve(); await release.promise; return result;
    });
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      const result = await publish(input); if (input.topic === "fabric.control.ack") acknowledged.resolve(); return result;
    });
    const handler = vi.fn(() => ({ accepted: true })); plane.start(handler); kick(plane); await reached.promise;
    plane.pause(true); release.resolve(); await expect(plane.checkpointForRelease()).rejects.toThrow(/unsettled/);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(handler).not.toHaveBeenCalled(); expect(mesh.read({ topic: "fabric.control.ack" })).toEqual([]);
    plane.resume(); await acknowledged.promise; plane.pause(true); await plane.checkpointForRelease();
    expect(handler).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.ack" })[0]?.data).toMatchObject({ commandId: "original", accepted: false, notRun: true });
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });

  it.each([true, false])("preserves sole claim custody across the release cut and spent deadline, then settles once on resume (fenced=%s)", async fenced => {
    vi.useFakeTimers();
    const mesh = meshFixture(), plane = control(mesh, fenced), original = await command(plane);
    const reached = deferred(), release = deferred(), acknowledged = deferred();
    const write = MeshStore.prototype.writeBatch;
    let suspended = false;
    vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function(this: MeshStore, input) {
      const committed = await write.call(this, input);
      if (!suspended && input.beforeCommit && this.root === mesh.root) {
        suspended = true; reached.resolve(); await release.promise;
      }
      return committed;
    });
    const put = MeshStore.prototype.put;
    vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function(this: MeshStore, input) {
      const committed = await put.call(this, input);
      if (!fenced && !suspended && input.ifVersion === 0 && this.root === mesh.root) {
        suspended = true; reached.resolve(); await release.promise;
      }
      return committed;
    });
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      const result = await publish(input); if (input.topic === "fabric.control.ack") acknowledged.resolve(); return result;
    });
    const handler = vi.fn(() => ({ accepted: true })); plane.start(handler); kick(plane); await reached.promise;
    plane.pause(true); release.resolve(); await expect(plane.checkpointForRelease()).rejects.toThrow(/unsettled/);
    await vi.advanceTimersByTimeAsync(60_000);
    const local = new MeshStore(path.join(mesh.root, "control-seen", createHash("sha256").update("owner").digest("hex").slice(0, 32)), 64 * 1024, 100, { stateBackend: "file" });
    const key = "topology/control-seen/" + createHash("sha256").update("owner\0original").digest("hex");
    expect(mesh.get(key, { fresh: true })?.value).toMatchObject({ commandId: "original", ownerIncarnation: plane.incarnation });
    expect(mesh.get(key, { fresh: true })?.value).not.toHaveProperty("acceptance");
    expect(local.get(key, { fresh: true })).toBeUndefined();
    expect(handler).not.toHaveBeenCalled(); expect(mesh.read({ topic: "fabric.control.ack" })).toEqual([]);
    plane.resume(); plane.resume(); await acknowledged.promise;
    plane.pause(true); await plane.checkpointForRelease();
    expect(handler).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.command" })[0]?.id).toBe(original.id);
    expect(mesh.read({ topic: "fabric.control.ack" }).map(value => value.data)).toEqual([
      expect.objectContaining({ commandId: "original", ownerIncarnation: plane.incarnation,
        accepted: false, error: "Fabric control command expired", notRun: true }),
    ]);
    expect(local.get(key, { fresh: true })?.value).toMatchObject({ sequence: original.sequence,
      acceptance: { accepted: false, error: "Fabric control command expired", notRun: true } });
    expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
  });

  it("rejects a control outcome disk readback that changed after checkpoint", async () => {
    const mesh = meshFixture(), plane = control(mesh), ack = deferred(); await command(plane);
    const publish = mesh.publish.bind(mesh);
    vi.spyOn(mesh, "publish").mockImplementation(async input => {
      const result = await publish(input); if (input.topic === "fabric.control.ack") ack.resolve(); return result;
    });
    plane.start(() => ({ accepted: true, messageId: "original-outcome" })); kick(plane); await ack.promise;
    plane.pause(true); await plane.checkpointForRelease(); expect(plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    const local = new MeshStore(path.join(mesh.root, "control-seen", createHash("sha256").update("owner").digest("hex").slice(0, 32)), 64 * 1024, 100, { stateBackend: "file" });
    const key = "topology/control-seen/" + createHash("sha256").update("owner\0original").digest("hex");
    const saved = local.get(key, { fresh: true })!;
    await local.put({ key, identity: identity("owner"), ifVersion: saved.version, value: { ...(saved.value as object), acceptance: { accepted: true, messageId: "tampered" } } });
    expect(() => plane.releaseCheckpointReceipt()).toThrow(/readback/);
  });

  it("fences a stale checkpoint continuation after the accepted handler join", async () => {
    const mesh = meshFixture(), plane = control(mesh); await command(plane);
    const entered = deferred(), finish = deferred();
    plane.start(async () => { entered.resolve(); await finish.promise; return { accepted: true }; });
    kick(plane); await entered.promise; plane.pause(true);
    let valid = true;
    const checkpoint = plane.checkpointForRelease(() => valid).catch(error => error);
    valid = false; plane.resume(); finish.resolve();
    expect(await checkpoint).toHaveProperty("message", expect.stringMatching(/stale/));
    expect(() => plane.releaseCheckpointReceipt()).toThrow(/unavailable/);
  });
});

describe("resident lifecycle exact delivery receipt boundary", () => {
  it.each([false, true])("finishes accepted receipt (once=%s) without consuming the next event/subscription", async once => {
    const mesh = meshFixture(), entered = deferred(), finish = deferred(), nextEntered = deferred();
    const seen: string[] = [];
    const broker = lifecycle(mesh, async (subscription, value) => {
      seen.push(`${subscription.id}:${value.id}`);
      if (seen.length === 1) { entered.resolve(); await finish.promise; }
      else { broker.pause(true); nextEntered.resolve(); }
    });
    const firstSub = await subscribe(broker, once);
    const secondSub = once ? { ...firstSub, id: "zz-held-subscription", once: false } : firstSub;
    if (once) await mesh.put({ key: FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + secondSub.id, value: secondSub, identity: targetIdentity, ifVersion: 0 });
    const first = await event(mesh), second = await event(mesh);
    broker.start(); await entered.promise;
    broker.pause(true); finish.resolve(); await broker.checkpointForRelease();
    expect(seen).toEqual([`${firstSub.id}:${first.id}`]);
    const saved = broker.list();
    if (once) {
      expect(saved.find(sub => sub.id === firstSub.id)).toBeUndefined();
      expect(saved.find(sub => sub.id === secondSub.id)?.afterSequence).toBe(secondSub.afterSequence);
    } else expect(saved[0]?.afterSequence).toBe(first.sequence);
    expect(mesh.read({ topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC }).map(value => value.id)).toEqual([first.id, second.id]);
    expect(broker.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    broker.resume(); broker.resume(); await nextEntered.promise; await broker.checkpointForRelease();
    expect(seen[1]).toBe(`${secondSub.id}:${once ? first.id : second.id}`);
    if (!once) expect(broker.list()[0]?.afterSequence).toBe(second.sequence);
  });

  it("does not advance a held unmatched cursor after the commit-lock await", async () => {
    const mesh = meshFixture(), broker = lifecycle(mesh, () => { throw new Error("must not deliver"); });
    const sub = await subscribe(broker);
    // At least a page of unmatched input forces a durable cursor write.
    for (let n = 0; n < 100; n++) await mesh.publish({ topic: "unrelated", from: sourceIdentity, text: String(n) });
    const reached = deferred(), release = deferred(), write = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async input => { reached.resolve(); await release.promise; return write(input); });
    broker.start(); await reached.promise; broker.pause(true); release.resolve();
    await broker.checkpointForRelease();
    expect(broker.list()[0]?.afterSequence).toBe(sub.afterSequence);
  });

  it("holds new subscription admission after participant refresh and under the actual commit lock", async () => {
    const mesh = meshFixture(), reached = deferred(), release = deferred(), participants = directory();
    participants.refresh = async () => { reached.resolve(); await release.promise; };
    const broker = lifecycle(mesh, () => {}, participants);
    const pending = subscribe(broker).catch(error => error);
    await reached.promise; broker.pause(true); release.resolve();
    expect(await pending).toHaveProperty("message", expect.stringMatching(/admission is held/));
    expect(broker.list()).toEqual([]);
    broker.resume(); participants.refresh = async () => {};
    const locked = deferred(), unlock = deferred(), write = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async input => { locked.resolve(); await unlock.promise; return write(input); });
    const underLock = subscribe(broker).catch(error => error);
    await locked.promise; broker.pause(true); unlock.resolve();
    expect(await underLock).toHaveProperty("message", expect.stringMatching(/admission is held/));
    expect(broker.list()).toEqual([]);
  });

  it("preserves once-delete uncertainty and never replays accepted delivery", async () => {
    const mesh = meshFixture(), delivered = deferred();
    const deliver = vi.fn(() => { delivered.resolve(); });
    const broker = lifecycle(mesh, deliver);
    const sub = await subscribe(broker, true);
    const write = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementation(async input => {
      const result = await write(input);
      if (input.ops.some(operation => operation.kind === "delete")) throw new Error("visible once-delete without confirmed storage receipt");
      return result;
    });
    await event(mesh); broker.start(); await delivered.promise; broker.pause(true);
    await expect(broker.checkpointForRelease()).rejects.toThrow(/unconfirmed/);
    expect(mesh.get(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + sub.id, { fresh: true })).toBeUndefined();
    expect(() => broker.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
    broker.resume(); broker.pause(true);
    await expect(broker.checkpointForRelease()).rejects.toThrow(/unconfirmed/);
    expect(deliver).toHaveBeenCalledOnce();
  });

  it("checks stale checkpoint validity under the once-delete receipt lock before any mutation", async () => {
    const mesh = meshFixture(), delivered = deferred();
    const broker = lifecycle(mesh, () => delivered.resolve());
    const sub = await subscribe(broker, true);
    const write = mesh.writeBatch.bind(mesh);
    let refuse = true;
    vi.spyOn(mesh, "writeBatch").mockImplementation(async input => {
      if (refuse) throw new Error("receipt withheld");
      return write(input);
    });
    await event(mesh); broker.start(); await delivered.promise; broker.pause(true);
    await expect(broker.checkpointForRelease()).rejects.toThrow();
    refuse = false;
    const entered = deferred(), unlock = deferred();
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async input => { entered.resolve(); await unlock.promise; return write(input); });
    let valid = true;
    const stale = broker.checkpointForRelease(() => valid).catch(error => error);
    await entered.promise; valid = false; broker.resume(); broker.pause(true); unlock.resolve();
    expect(await stale).toHaveProperty("message", expect.stringMatching(/stale/));
    expect(mesh.get(FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + sub.id, { fresh: true })?.value).toMatchObject({ afterSequence: sub.afterSequence });
  });

  it("readbacks persisted cursor/once state and rejects external tampering", async () => {
    const mesh = meshFixture(), broker = lifecycle(mesh, () => {});
    const sub = await subscribe(broker); broker.pause(true); await broker.checkpointForRelease();
    const receipt = broker.releaseCheckpointReceipt() as { state: { subscriptions: Array<{ value: FabricLifecycleSubscription }> } };
    expect(receipt.state.subscriptions[0]?.value).toMatchObject({ id: sub.id, afterSequence: sub.afterSequence, once: false });
    const key = FABRIC_LIFECYCLE_SUBSCRIPTION_PREFIX + sub.id;
    const saved = mesh.get(key, { fresh: true })!;
    await mesh.put({ key, value: { ...sub, afterSequence: 999 }, identity: sourceIdentity, ifVersion: saved.version });
    expect(() => broker.releaseCheckpointReceipt()).toThrow(/readback changed/);
  });
});

type Launch = Parameters<ProcessTransport["launch"]>[0];
const argument = (request: Launch, name: string) => request.workerArguments[request.workerArguments.indexOf("--" + name) + 1]!;
const writeStatus = (request: Launch, status: "running" | "completed" | "stopped" | "failed", progress = false) => {
  const now = Date.now();
  fs.writeFileSync(argument(request, "status-file"), JSON.stringify({ id: request.id, name: request.name, task: fs.readFileSync(argument(request, "task-file"), "utf8"),
    status, runner: "pi", transport: "process", sessionId: "fixture-no-native-worker", cwd: request.cwd, startedAt: now, updatedAt: now,
    ...(status !== "running" ? { finishedAt: now } : {}), ...(status === "stopped" ? { error: "Agent stopped" } : {}),
    ...(status === "failed" ? { error: "Agent transport exited without a result" } : {}),
    turns: progress ? 2 : 0, toolCalls: progress ? 1 : 0, text: status === "completed" ? "accepted result" : "",
    usage: { input: progress ? 10 : 0, output: progress ? 5 : 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
};
const agent = (options: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const directory = root();
  const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, retainRuns: true,
    notifyOnComplete: false, nice: 19 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directory, "runs"), piBinary: process.execPath, ...options });
  cleanups.push(() => { manager.resumeAfterRelease(); return manager.close(); });
  return manager;
};
const transportFixture = () => {
  const launches: Launch[] = [];
  const closures: Array<ReturnType<typeof deferred<void>>> = [];
  const living: boolean[] = [];
  const stop = vi.fn(async () => {});
  let onLaunch: (() => void) | undefined;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const index = launches.push(request) - 1;
    living[index] = true; closures[index] = deferred();
    writeStatus(request, "running");
    onLaunch?.();
    return { kind: "process", sessionId: "fixture-no-native-worker", closed: closures[index]!.promise, isAlive: async () => living[index]!,
      stop: async () => { stop(); living[index] = false; closures[index]!.resolve(); } };
  });
  return { launches, stop, observeLaunch(callback: () => void) { onLaunch = callback; }, finish(index = 0, status: "completed" | "stopped" | "failed" = "completed", progress = false) {
    writeStatus(launches[index]!, status, progress); living[index] = false; closures[index]!.resolve();
  } };
};

describe("resident agent preserving launch/recovery admission", () => {
  it("holds a new spawn without failure and resumes it once", async () => {
    const manager = agent(), fixture = transportFixture(); manager.pauseForRelease(true);
    let finished = false;
    const spawn = manager.spawn({ task: "held-original", transport: "process" }).then(value => { finished = true; return value; });
    await expect(manager.checkpointForRelease(Date.now() + 500, () => true)).rejects.toThrow(/pending launch/);
    expect(finished).toBe(false); expect(fixture.launches).toEqual([]); expect(fixture.stop).not.toHaveBeenCalled();
    manager.resumeAfterRelease(); manager.resumeAfterRelease();
    const info = await spawn;
    expect(fixture.launches).toHaveLength(1); fixture.finish();
    await expect(manager.wait(info.id)).resolves.toMatchObject({ status: "completed", text: "accepted result" });
  });

  it("holds an already prepared spawn after model await, before files/transport commit", async () => {
    const preparation = deferred<string>(), entered = deferred();
    const manager = agent({ preparePiModel: async () => { entered.resolve(); return preparation.promise; } }), fixture = transportFixture();
    const spawn = manager.spawn({ task: "prepared-original", transport: "process" });
    await entered.promise; manager.pauseForRelease(true); preparation.resolve("provider/model");
    await expect(manager.checkpointForRelease()).rejects.toThrow(/pending launch/);
    expect(fixture.launches).toHaveLength(0);
    manager.resumeAfterRelease(); const info = await spawn;
    fixture.finish(); expect((await manager.wait(info.id)).status).toBe("completed");
  });

  it("rechecks the actual transport creation authority after adapter awaits without settling a held launch as failure", async () => {
    const manager = agent(), entered = deferred(), release = deferred();
    let preparations = 0, workers = 0;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      if (++preparations === 1) { entered.resolve(); await release.promise; }
      request.authorize?.(); // built-in adapter's synchronous worker creation boundary
      workers++; writeStatus(request, "completed");
      return { kind: "process", sessionId: "fixture-no-native-worker", isAlive: async () => false, stop: async () => {} };
    });
    const pending = manager.spawn({ task: "same transport input", transport: "process" });
    await entered.promise; manager.pauseForRelease(true); release.resolve();
    await expect(manager.checkpointForRelease()).rejects.toThrow(/pending launch/);
    expect(workers).toBe(0);
    manager.resumeAfterRelease(); manager.resumeAfterRelease();
    const info = await pending; expect(workers).toBe(1); expect(preparations).toBe(2);
    expect((await manager.wait(info.id)).status).toBe("completed");
  });

  it("keeps queued permit starts held while the already accepted worker naturally completes", async () => {
    const manager = agent(), fixture = transportFixture();
    const first = await manager.spawn({ task: "accepted-active", transport: "process" });
    const second = await manager.spawn({ task: "queued-original", transport: "process" });
    expect(second.status).toBe("queued"); manager.pauseForRelease(true);
    fixture.finish(); await expect(manager.wait(first.id)).resolves.toMatchObject({ status: "completed" });
    await expect(manager.checkpointForRelease()).rejects.toThrow(/pending launch/);
    expect(fixture.launches).toHaveLength(1);
    expect(manager.status(second.id)).toMatchObject({ status: "queued" });
    const launched = deferred();
    fixture.observeLaunch(() => launched.resolve());
    manager.resumeAfterRelease(); manager.resumeAfterRelease(); await launched.promise;
    fixture.finish(1); await expect(manager.wait(second.id)).resolves.toMatchObject({ status: "completed" });
    expect(fixture.launches).toHaveLength(2);
  });

  it.each(["stopped", "failed"] as const)("holds %s recovery during its original backoff without spending attempts or rewriting journals", async status => {
    vi.useFakeTimers();
    const manager = agent(), fixture = transportFixture();
    const info = await manager.spawn({ task: "recover-original", transport: "process" });
    fixture.finish(0, status, status === "stopped");
    // Native-close event enters recovery; advancing its event-driven backoff is bounded, not a polling probe.
    await vi.advanceTimersByTimeAsync(1);
    manager.pauseForRelease(true);
    const directory = manager.runDirectory(info.id)!;
    const originalStatus = fs.readFileSync(path.join(directory, "status.json"), "utf8");
    const originalTask = fs.readFileSync(path.join(directory, "task.txt"), "utf8");
    await vi.advanceTimersByTimeAsync(5000);
    expect(fixture.launches).toHaveLength(1); expect(fixture.stop).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(directory, "status.json"), "utf8")).toBe(originalStatus);
    expect(fs.readFileSync(path.join(directory, "task.txt"), "utf8")).toBe(originalTask);
    expect(fs.existsSync(path.join(directory, "relaunches.jsonl"))).toBe(false);
    await expect(manager.checkpointForRelease()).rejects.toThrow(/pending launch|unresolved worker/);
    manager.resumeAfterRelease(); manager.resumeAfterRelease(); await vi.advanceTimersByTimeAsync(5000);
    expect(fixture.launches).toHaveLength(2);
    const journal = fs.readFileSync(path.join(directory, "relaunches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(journal).toHaveLength(1);
    expect(journal[0]).toMatchObject(status === "stopped" ? { resumeAttempts: 1, startupAttempts: 1 } : { resumeAttempts: 0, startupAttempts: 2 });
    fixture.finish(1); await vi.advanceTimersByTimeAsync(1000); expect((await manager.wait(info.id)).status).toBe("completed");
  });

  it("holds recovery after model preparation without stop/task/journal/status mutation and resumes once", async () => {
    vi.useFakeTimers();
    const entered = deferred(), prepared = deferred<string>(); let calls = 0;
    const manager = agent({ preparePiModel: async () => {
      if (++calls === 1) return "provider/model";
      entered.resolve(); return prepared.promise;
    } });
    const fixture = transportFixture();
    const info = await manager.spawn({ task: "same recovery input", transport: "process" });
    fixture.finish(0, "stopped", true);
    await vi.advanceTimersByTimeAsync(5000); await entered.promise;
    manager.pauseForRelease(true);
    const directory = manager.runDirectory(info.id)!;
    const status = fs.readFileSync(path.join(directory, "status.json"), "utf8");
    prepared.resolve("provider/model"); await vi.advanceTimersByTimeAsync(5000);
    expect(fixture.launches).toHaveLength(1); expect(fixture.stop).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(directory, "task.txt"), "utf8")).toBe("same recovery input");
    expect(fs.readFileSync(path.join(directory, "status.json"), "utf8")).toBe(status);
    expect(fs.existsSync(path.join(directory, "relaunches.jsonl"))).toBe(false);
    manager.resumeAfterRelease(); manager.resumeAfterRelease(); await vi.advanceTimersByTimeAsync(1000);
    expect(fixture.launches).toHaveLength(2);
    const rows = fs.readFileSync(path.join(directory, "relaunches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(1); expect(rows[0]).toMatchObject({ resumeAttempts: 1, startupAttempts: 1 });
    fixture.finish(1); await vi.advanceTimersByTimeAsync(1000); expect((await manager.wait(info.id)).status).toBe("completed");
  });

  it("preserves a spent recovery deadline rather than resetting it on resume", async () => {
    vi.useFakeTimers();
    const manager = agent(), fixture = transportFixture();
    const info = await manager.spawn({ task: "deadline-original", transport: "process", timeoutMs: 1000 });
    fixture.finish(0, "stopped", true); await vi.advanceTimersByTimeAsync(1);
    manager.pauseForRelease(true);
    await vi.advanceTimersByTimeAsync(Number(argument(fixture.launches[0]!, "timeout-ms")) + 2000);
    expect(fixture.launches).toHaveLength(1); expect(fixture.stop).not.toHaveBeenCalled();
    manager.resumeAfterRelease(); await vi.advanceTimersByTimeAsync(1000);
    expect(fixture.launches).toHaveLength(1);
    expect((await manager.wait(info.id)).status).toBe("stopped");
    expect(fs.existsSync(path.join(manager.runDirectory(info.id)!, "relaunches.jsonl"))).toBe(false);
  });

  it("allows only the trusted process/Pi no-inference staged probe through the launch hold", async () => {
    const manager = agent(), fixture = transportFixture(); manager.pauseForRelease(true);
    const info = await manager.spawn({ task: "startup-probe", runner: "pi", transport: "process", residentStartupProbe: true });
    expect(fixture.launches[0]?.workerArguments).toContain("--resident-startup-probe"); fixture.finish();
    expect((await manager.wait(info.id)).status).toBe("completed");
    await expect(manager.spawn({ task: "not-a-probe", runner: "claude", transport: "process", residentStartupProbe: true })).rejects.toThrow(/Invalid resident startup probe/);
  });

  it("does not let a stale hung worker observation mark a resumed worker lost", async () => {
    const manager = agent(), fixture = transportFixture();
    const info = await manager.spawn({ task: "accepted-terminal", transport: "process" }); fixture.finish(); await manager.wait(info.id);
    // Public status cannot supply execution proof. Suspend the actual handle query.
    const entered = deferred(), query = deferred<boolean>();
    const launch = fixture.launches[0]!;
    // Capture the retained handle via the adapter launch result, not private-field access.
    const spy = vi.mocked(ProcessTransport.prototype.launch);
    const handle = await spy.mock.results[0]!.value;
    handle.isAlive = () => { entered.resolve(); return query.promise; };
    manager.pauseForRelease(true); let valid = true;
    const pending = manager.checkpointForRelease(Date.now() + 2000, () => valid).catch(error => error);
    await entered.promise; valid = false; manager.resumeAfterRelease(); query.resolve(false);
    expect(await pending).toHaveProperty("message", expect.stringMatching(/stale/));
    expect(hasUnresolvedWorker(path.dirname(argument(launch, "status-file")))).toBe(false);
    handle.isAlive = async () => false;
    expect(() => manager.releaseCheckpointReceipt()).toThrow(/unavailable/);
  });

  it("readbacks an empty held run root and rejects changed bytes instead of claiming UI quiet", async () => {
    const manager = agent(); manager.pauseForRelease(true); await manager.checkpointForRelease(Date.now() + 1000, () => true);
    const receipt = manager.releaseCheckpointReceipt() as { disk: { runRoot: string; files: unknown[] } };
    expect(receipt.disk.files).toEqual([]);
    fs.mkdirSync(receipt.disk.runRoot, { recursive: true }); fs.writeFileSync(path.join(receipt.disk.runRoot, "unexpected-fence.json"), "{}");
    expect(() => manager.releaseCheckpointReceipt()).toThrow(/readback changed/);
  });
});
