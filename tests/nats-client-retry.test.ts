// Keep reason: B's verified reopen exposes retryable unavailability, not native reconnect.
// Only the FabricTransport seam is injected. No broker/ACK verifier/manager internals are mocked.
// No hosted runners, child execution, real KV deletes, or recursive cleanup in this selection.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { AgentManager } from "../src/agents/manager.js";
import { FabricTargetTerminalError } from "../src/agents/terminal-target.js";
import { MeshStore } from "../src/mesh/store.js";
import { FabricControlPlane, type FabricControlCommand } from "../src/topology/control-plane.js";
import { TransportHostLease } from "../src/topology/host-leases.js";
import type { ParticipantView } from "../src/topology/participant-view.js";
import { TransportUnavailableError, type FabricTransport, type TransportDelivery, type TransportEnvelope, type TransportKv, type TransportKvEntry } from "../src/transport/types.js";

const owner = { machine: "machine-a", hostId: "retry-owner" };
const identity = { id: "retry-owner", name: "owner", kind: "main" as const };
const sender = { id: "retry-sender", name: "producer", kind: "main" as const };
const epoch = "retry-owner-activation";
const temporary = () => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-client-retry-"));
const temporaryMesh = () => new MeshStore(temporary(), DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents);
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
async function bounded(promise: Promise<void>, label: string): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`retry sequence stalled: ${label}`)), 3_000);
    })]);
  } finally { clearTimeout(timer); }
}
function untilAborted(signal: AbortSignal): Promise<void> {
  return signal.aborted ? Promise.resolve() : new Promise(resolve => signal.addEventListener("abort", () => resolve(), { once: true }));
}
const participants = (live: () => boolean): ParticipantView => ({
  leaseLive: live,
  resolve: id => ({ machine: id === sender.id ? "machine-b" : owner.machine,
    ownerHostId: id === sender.id ? "sender-host" : owner.hostId,
    ownerIdentityId: id === sender.id ? sender.id : identity.id,
    ownerIncarnation: epoch, capabilities: ["steer", "followUp"], live: live() }),
  whenChanged: (_id, signal) => untilAborted(signal),
});
const unusedKv: TransportKv = {
  ttlMs: 15_000,
  async get() { throw new Error("unexpected KV read"); },
  async put() { throw new Error("unexpected KV write"); },
  async delete() { throw new Error("real deletion excluded"); },
  async *watch() { throw new Error("unexpected KV watch"); },
};

// This single public receive sequence covers entry failure, ACK publication failure,
// broker redelivery, authenticated producer retention, and durable outcome dedupe.
describe("A2 callers across verified NATS reopen", () => {
  it("rebinds a retryable inbox entry and replays a saved outcome without repeating its handler", async () => {
    const redeliver = deferred(), completed = deferred();
    const order: string[] = [];
    const sent: TransportEnvelope[] = [];
    let inboxEntries = 0, handlerCalls = 0, ackAttempts = 0;
    const now = Date.now();
    const body: FabricControlCommand = { version: 1, commandId: "same-logical-command", targetId: "task-target",
      operation: "steer", replyTo: "sender-host", ownerIncarnation: epoch,
      message: "retained producer input", requestedAt: now, deadlineAt: now + 30_000 };
    const payload: TransportEnvelope = { id: "same-broker-event", kind: "command", to: owner,
      from: sender, targetId: body.targetId, operation: body.operation, body };
    const delivery = (attempt: number): TransportDelivery<TransportEnvelope> => ({
      payload, sourceMachine: "machine-b", verification: "bridge", attempt, sequence: 1,
      working() {},
      async ack() { order.push("broker-ack"); completed.resolve(); },
      nak(delay) { expect(delay).toBe(1_000); order.push("nak"); redeliver.resolve(); },
      async term(reason) { throw new Error(`unexpected terminal disposition: ${reason}`); },
    });
    const transport: FabricTransport = {
      kind: "nats", self: owner,
      async send<T>(envelope: TransportEnvelope<T>) {
        const createdAt = Date.now();
        const resolved = { ...envelope, body: typeof envelope.body === "function"
          ? (envelope.body as (at: number) => T)(createdAt) : envelope.body, createdAt };
        sent.push(resolved);
        if (++ackAttempts === 1) { order.push("ack-unavailable"); throw new TransportUnavailableError("verified reopen in progress"); }
        order.push("ack-persisted");
        return { id: envelope.id, sequence: 2, duplicate: false, envelope: resolved };
      },
      async *inbox(signal) {
        if (++inboxEntries === 1) { order.push("inbox-unavailable"); throw new TransportUnavailableError("connection lost before iterator entry"); }
        order.push("inbox-rebound");
        yield delivery(1);
        await redeliver.promise;
        if (!signal.aborted) yield delivery(2);
        await untilAborted(signal);
      },
      kv: () => unusedKv,
      async drainState() { return { pending: 0, ackPending: 0 }; },
      async close() {},
    };
    const plane = new FabricControlPlane(temporaryMesh(), identity, {
      enabled: true, hostId: owner.hostId, ownerIncarnation: epoch, transport,
      participants: participants(() => true), canConsumeMesh: () => true, relayOperatorMachine: "fabric-ops",
    });
    plane.start((command, from, _signal, verification) => {
      handlerCalls++;
      order.push("handler");
      expect(command.commandId).toBe(body.commandId);
      expect(command.message).toBe(body.message);
      expect(from).toEqual(sender);
      expect(verification).toBe("bridge");
      return { accepted: true, messageId: "application-receipt" };
    });
    try {
      await bounded(completed.promise, "inbox rebind and saved ACK replay");
      expect(plane.transportKind).toBe("nats");
      expect(handlerCalls).toBe(1);
      expect(inboxEntries).toBe(2);
      expect(sent).toHaveLength(2);
      expect(sent[1]?.body, JSON.stringify(sent)).toMatchObject({ accepted: true, messageId: "application-receipt", commandId: body.commandId });
      expect(sent[1]?.body).toMatchObject(sent[0]!.body as object);
      expect(order).toEqual(["inbox-unavailable", "inbox-rebound", "handler", "ack-unavailable", "nak", "ack-persisted", "broker-ack"]);
    } finally { await plane.close(); }
  });

  it.skipIf(process.platform !== "linux")("never rebinds after a foreign owner takes an expired lease", async () => {
    let monotonic = 0, inboxEntries = 0, handlerCalls = 0, revision = 0;
    let plane: FabricControlPlane | undefined;
    let current: TransportKvEntry<unknown> | undefined;
    const kv: TransportKv = { ...unusedKv,
      async get<V>() { return current as TransportKvEntry<V> | undefined; },
      async put<V>(key: string, ownerId: string, value: V, ifRevision: number) {
        if (ifRevision === 0) {
          current = { key, machine: owner.machine, owner: ownerId, value, revision: ++revision };
          return revision;
        }
        current = { key, machine: owner.machine, owner: "successor#foreign", value: {}, revision: ++revision };
        throw Object.assign(new Error("KV revision conflict"), { code: "FABRIC_KV_CONFLICT" });
      },
      async delete(_key, ifRevision) {
        if (current?.revision !== ifRevision) return false;
        current = undefined; return true;
      },
    };
    const lease = new TransportHostLease({ kv, self: owner, rootId: identity.id, identityId: identity.id,
      incarnation: epoch, localStateDir: temporary(), monotonic: () => monotonic,
      onLost: error => plane?.pause(error) }); // The public composition-owned E11 path.
    await lease.acquire();
    let unavailable = true;
    const transport: FabricTransport = {
      kind: "nats", self: owner,
      async send() { throw new Error("lease-loss path must not publish an ACK"); },
      async *inbox(signal) {
        inboxEntries++;
        if (unavailable) throw new TransportUnavailableError("verified reopen pending");
        await untilAborted(signal);
      },
      kv: () => unusedKv, async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
    };
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      plane = new FabricControlPlane(temporaryMesh(), identity, {
        enabled: true, hostId: owner.hostId, ownerIncarnation: epoch, transport,
        participants: participants(() => lease.live()), canConsumeMesh: () => lease.live(), relayOperatorMachine: "fabric-ops",
      });
      plane.start(() => { handlerCalls++; return { accepted: true }; });
      await vi.advanceTimersByTimeAsync(0); // First failed entry has armed its one-shot.
      expect(inboxEntries).toBe(1);
      monotonic = 13_000;
      expect(lease.live()).toBe(false); // A local cutoff holds admission until ownership is checked.
      expect(lease.recovering()).toBe(true);
      expect(lease.signal.aborted).toBe(false);
      await expect(lease.renew()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_LOST", reason: "conflict" });
      expect(lease.signal.aborted).toBe(true); // The observed foreign owner is terminal.
      unavailable = false;
      await vi.advanceTimersByTimeAsync(31_000);
      expect(inboxEntries).toBe(1);
      expect(handlerCalls).toBe(0);
      expect(() => lease.acquire()).toThrow();
    } finally {
      try { await plane?.close(); await lease.release(); } finally { vi.useRealTimers(); }
    }
    expect(current?.owner).toBe("successor#foreign"); // Old-revision release cannot delete its successor.
  });

  it("does not permanently pause when a live watch reset holds the ACK fence", async () => {
    let ready = true, handlerCalls = 0, ackAttempts = 0, brokerAcks = 0, naks = 0, inboxEntries = 0;
    const held = deferred(), replay = deferred(), completed = deferred();
    const now = Date.now();
    const body: FabricControlCommand = { version: 1, commandId: "watch-reset-command", targetId: "reset-target",
      operation: "steer", replyTo: "sender-host", ownerIncarnation: epoch,
      message: "retain this exact input", requestedAt: now, deadlineAt: now + 30_000 };
    const payload: TransportEnvelope = { id: "watch-reset-event", kind: "command", to: owner,
      from: sender, targetId: body.targetId, operation: body.operation, body };
    const sent: TransportEnvelope[] = [];
    const delivery = (attempt: number): TransportDelivery<TransportEnvelope> => ({
      payload, sourceMachine: "machine-b", verification: "bridge", attempt, sequence: 1,
      working() {}, async ack() { brokerAcks++; completed.resolve(); },
      nak() { naks++; }, async term(reason) { throw new Error(`unexpected TERM: ${reason}`); },
    });
    const transport: FabricTransport = {
      kind: "nats", self: owner,
      async send<T>(envelope: TransportEnvelope<T>) {
        ackAttempts++;
        const createdAt = Date.now();
        try {
          const resolved = { ...envelope, body: typeof envelope.body === "function"
            ? (envelope.body as (at: number) => T)(createdAt) : envelope.body, createdAt };
          sent.push(resolved);
          return { id: envelope.id, sequence: 2, duplicate: false, envelope: resolved };
        } catch (error) { held.resolve(); throw error; }
      },
      async *inbox(signal) {
        const attempt = ++inboxEntries;
        if (attempt > 1) await Promise.race([replay.promise, untilAborted(signal)]);
        if (!signal.aborted) yield delivery(attempt); // Same event after aborted iterator joins.
        await untilAborted(signal);
      },
      kv: () => unusedKv, async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
    };
    const view = participants(() => true);
    const plane = new FabricControlPlane(temporaryMesh(), identity, {
      enabled: true, hostId: owner.hostId, ownerIncarnation: epoch, transport,
      participants: view, canConsumeMesh: () => ready, relayOperatorMachine: "fabric-ops",
    });
    plane.start((command, from, _signal, verification) => {
      handlerCalls++;
      expect(command.commandId).toBe(body.commandId);
      expect(command.message).toBe(body.message);
      expect(from).toEqual(sender);
      expect(verification).toBe("bridge");
      // Model the public directory gate during B's reset: lease is still live.
      // The real claim, handler result, outcome store and ACK body fence all run.
      ready = false;
      return { accepted: true, messageId: "saved-reset-outcome" };
    });
    try {
      await bounded(held.promise, "ACK held during watch reset");
      expect(view.leaseLive()).toBe(true);
      expect(sent).toHaveLength(0);
      expect(brokerAcks).toBe(0);
      ready = true;
      replay.resolve();
      await bounded(completed.promise, "live watch readiness recovery without public resume");
      expect(handlerCalls).toBe(1);
      expect(ackAttempts).toBe(2);
      expect(sent).toHaveLength(1);
      expect(sent[0]?.body).toMatchObject({ accepted: true, messageId: "saved-reset-outcome", commandId: body.commandId });
      expect(brokerAcks).toBe(1);
      expect(naks).toBe(0);
      expect(inboxEntries).toBe(2);
    } finally { replay.resolve(); await plane.close(); }
  });

  it.each([new Error("unknown inbox failure"), Object.assign(new Error("authorization refused"), {
    name: "NatsAuthorizationError", code: "FABRIC_NATS_AUTHORIZATION",
  })])("does not retry an unknown or authorization inbox error: %s", async failure => {
    let inboxEntries = 0, handlerCalls = 0;
    const transport: FabricTransport = {
      kind: "nats", self: owner,
      async send() { throw new Error("failed intake must not publish an ACK"); },
      async *inbox() { inboxEntries++; throw failure; },
      kv: () => unusedKv, async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
    };
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const plane = new FabricControlPlane(temporaryMesh(), identity, {
      enabled: true, hostId: owner.hostId, ownerIncarnation: epoch, transport,
      participants: participants(() => true), canConsumeMesh: () => true, relayOperatorMachine: "fabric-ops",
    });
    try {
      plane.start(() => { handlerCalls++; return { accepted: true }; });
      await vi.advanceTimersByTimeAsync(31_000);
      expect(inboxEntries).toBe(1);
      expect(handlerCalls).toBe(0);
    } finally { try { await plane.close(); } finally { vi.useRealTimers(); } }
  });

  it.each(["pause", "close"] as const)("cancels a pending retry on explicit %s without auto-resume", async action => {
    let inboxEntries = 0;
    const transport: FabricTransport = {
      kind: "nats", self: owner,
      async send() { throw new Error("paused intake must not publish"); },
      async *inbox() { inboxEntries++; throw new TransportUnavailableError("reopen pending"); },
      kv: () => unusedKv, async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
    };
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const plane = new FabricControlPlane(temporaryMesh(), identity, {
      enabled: true, hostId: owner.hostId, ownerIncarnation: epoch, transport,
      participants: participants(() => true), canConsumeMesh: () => true, relayOperatorMachine: "fabric-ops",
    });
    try {
      plane.start(() => { throw new Error("no native admission while held"); });
      await vi.advanceTimersByTimeAsync(0);
      expect(inboxEntries).toBe(1);
      if (action === "pause") plane.pause(); else await plane.close();
      await vi.advanceTimersByTimeAsync(31_000);
      expect(inboxEntries).toBe(1);
    } finally { try { await plane.close(); } finally { vi.useRealTimers(); } }
  });

  it("joins a failed iteration and backs off one-shot retries from 1s to the 30s cap", async () => {
    let inboxEntries = 0, active = 0, maxActive = 0, joined = 0, brokerAcks = 0;
    const payload: TransportEnvelope = { id: "ack:orphan-event", kind: "ack", to: owner, from: sender,
      targetId: "orphan-target", operation: "accepted", body: { version: 1, commandId: "orphan-command",
        targetId: "orphan-target", accepted: true, bodyDigest: "0".repeat(64), ownerIncarnation: epoch } };
    const transport: FabricTransport = {
      kind: "nats", self: owner,
      async send() { throw new Error("orphan ACK intake must not publish"); },
      async *inbox() {
        const entry = ++inboxEntries;
        active++; maxActive = Math.max(maxActive, active);
        try {
          if (entry === 1) yield { payload, sourceMachine: "machine-b", verification: "bridge" as const,
            attempt: 1, sequence: 1, working() {}, async ack() { brokerAcks++; },
            nak() { throw new Error("unexpected NAK"); }, async term() { throw new Error("unexpected TERM"); } };
          throw new TransportUnavailableError("iteration unavailable");
        } finally { active--; joined++; }
      },
      kv: () => unusedKv, async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
    };
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const plane = new FabricControlPlane(temporaryMesh(), identity, {
      enabled: true, hostId: owner.hostId, ownerIncarnation: epoch, transport,
      participants: participants(() => true), canConsumeMesh: () => true, relayOperatorMachine: "fabric-ops",
    });
    try {
      plane.start(() => { throw new Error("ACK cannot enter a command handler"); });
      await vi.advanceTimersByTimeAsync(0);
      expect(brokerAcks).toBe(1);
      let expected = 1;
      for (const delay of [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]) {
        await vi.advanceTimersByTimeAsync(delay - 1);
        expect(inboxEntries).toBe(expected);
        await vi.advanceTimersByTimeAsync(1);
        expect(inboxEntries).toBe(++expected);
        expect(joined).toBe(inboxEntries);
      }
      expect(active).toBe(0);
      expect(maxActive).toBe(1);
    } finally { try { await plane.close(); } finally { vi.useRealTimers(); } }
  });

  it.skipIf(process.platform !== "linux")("suspends at the original 13s cutoff and recovers by same-owner CAS", async () => {
    let monotonic = 0, unavailable = false, revision = 0;
    let current: TransportKvEntry<unknown> | undefined;
    const deletes: Array<{ key: string; revision: number }> = [];
    const kv: TransportKv = { ...unusedKv,
      async get<V>() { return current as TransportKvEntry<V> | undefined; },
      async put<V>(key: string, ownerId: string, value: V, ifRevision: number) {
        if (unavailable) throw new TransportUnavailableError("reverification pending");
        if (ifRevision === 0 ? current !== undefined : current?.revision !== ifRevision) {
          throw Object.assign(new Error("KV revision conflict"), { code: "FABRIC_KV_CONFLICT" });
        }
        current = { key, machine: owner.machine, owner: ownerId, value, revision: ++revision };
        return revision;
      },
      async delete(key, ifRevision) { deletes.push({ key, revision: ifRevision }); return true; },
    };
    const lease = new TransportHostLease({ kv, self: owner, rootId: identity.id, identityId: identity.id,
      incarnation: epoch, localStateDir: temporary(), monotonic: () => monotonic });
    const warning = vi.spyOn(diagnostics, "warn").mockImplementation(() => undefined);
    try {
      await lease.acquire();
      monotonic = 5_000;
      unavailable = true;
      await expect(lease.renew()).rejects.toBeInstanceOf(TransportUnavailableError);
      expect(lease.live()).toBe(true);
      expect(lease.signal.aborted).toBe(false);
      monotonic = 12_999;
      expect(lease.live()).toBe(true); // Failure has not moved the acquisition's deadline.
      monotonic = 13_000;
      expect(lease.live()).toBe(false);
      expect(lease.recovering()).toBe(true);
      expect(lease.signal.aborted).toBe(false);
      expect(lease.live()).toBe(false); // Repeated reads do not repeat the suspend warning.
      expect(warning.mock.calls).toEqual([["src/topology/host-leases.ts",
        "[pi-fabric] NATS host lease suspended: reason=timeout elapsedMs=13000"]]);
      // The raw "reverification pending" renewal error above must not enter the warning.
      unavailable = false;
      await lease.renew(); // Only a receipt-verified, current-revision CAS restores admission.
      expect(lease.live()).toBe(true);
      expect(lease.recovering()).toBe(false);
      expect(lease.signal.aborted).toBe(false);
      expect(() => lease.acquire()).toThrow();
      expect(revision).toBe(2);
    } finally { await lease.release(); warning.mockRestore(); }
    expect(deletes).toEqual([{ key: lease.key, revision: 2 }]); // Recorded adapter only; no KV deletion.
  });

  it("keeps a real manager's final answer terminal rather than replaying input into a finished task", async () => {
    const root = temporary();
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true },
      { runRoot: path.join(root, "runs") });
    const finishedAt = Date.now();
    manager.restorePreviousRuns([{ id: "finished-task", name: "finished", task: "already done", runner: "pi",
      status: "completed", transport: "process", cwd: root, startedAt: finishedAt - 1, updatedAt: finishedAt,
      finishedAt, turns: 1, toolCalls: 0, text: "saved answer",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      finalAnswerReceipt: { id: "retained-final-answer", recordedAt: finishedAt } }]);
    try {
      for (let attempt = 0; attempt < 2; attempt++) {
        expect(() => manager.steer("finished-task", "retry", undefined, undefined,
          { idempotencyKey: "same-command" })).toThrow(FabricTargetTerminalError);
      }
      expect((await manager.wait("finished-task")).text).toBe("saved answer");
      expect(manager.runningCount()).toBe(0);
    } finally { await manager.close(); }
  });
});
