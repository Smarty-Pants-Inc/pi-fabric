// Keep reason: recorded public-control cancellation failure (smarty-dev#6477, pi-fabric#1624).
// Real request/inbox/handler and durable local claims; deterministic fake NATS carrier, not broker proof.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundQueue } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane, type FabricControlHandler, type FabricControlResult } from "../src/topology/control-plane.js";
import type { ParticipantRoute, ParticipantView } from "../src/topology/participant-view.js";
import {
  TransportDeniedError, TransportInvalidError, TransportUncertainError,
  TransportUnavailableError, TransportUnsupportedError,
  type FabricTransport, type TransportDelivery, type TransportDrainState,
  type TransportEnvelope, type TransportKv, type TransportOwner, type TransportSent,
} from "../src/transport/types.js";

const OWNER_HOST = "runtime:cancel-owner", OWNER_ID = "session:cancel-owner";
const CALLER_HOST = "runtime:cancel-caller", CALLER_ID = "session:cancel-caller";
const TARGET = "session:cancel-target", EPOCH = "cancel:owner", MESSAGE_ID = "executed-once";
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const address = (owner: TransportOwner): string => `${owner.machine}/${owner.hostId}`;
const nextTurn = (): Promise<void> => new Promise(resolve => setImmediate(resolve));
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

// Event gates, not polling. Abort closes the actual inbox iterator before fixture cleanup.
class Queue<T> {
  readonly values: T[] = [];
  #waiter: ((value: T | undefined) => void) | undefined;
  push(value: T): void {
    if (this.#waiter) { const done = this.#waiter; this.#waiter = undefined; done(value); }
    else this.values.push(value);
  }
  async take(signal?: AbortSignal): Promise<T | undefined> {
    if (signal?.aborted) return undefined;
    if (this.values.length) return this.values.shift()!;
    return new Promise(resolve => {
      const finish = (value: T | undefined) => {
        signal?.removeEventListener("abort", abort);
        if (this.#waiter === finish) this.#waiter = undefined;
        resolve(value);
      };
      const abort = () => finish(undefined);
      if (this.#waiter) throw new Error("Fixture queue has two consumers");
      this.#waiter = finish;
      signal?.addEventListener("abort", abort, { once: true });
    });
  }
  async required(): Promise<T> {
    const value = await this.take();
    if (value === undefined) throw new Error("Fixture event gate ended");
    return value;
  }
}
type WireEnvelope = TransportEnvelope<Record<string, unknown>> & {
  readonly body: Record<string, unknown>; readonly createdAt: number;
};
interface Publication {
  envelope: WireEnvelope; sourceMachine: string; sequence: number; destination: Endpoint;
}
type Disposition = "ack" | "nak" | "term";
class Carrier {
  readonly destinations = new Map<string, Endpoint>();
  sequence = 0;
  endpoint(self: TransportOwner): Endpoint {
    const endpoint = new Endpoint(this, self);
    this.destinations.set(address(self), endpoint);
    return endpoint;
  }
  deliver(publication: Publication, destination = publication.destination): Promise<Disposition> {
    const finished = deferred<Disposition>();
    destination.deliveries.push({
      payload: publication.envelope, sourceMachine: publication.sourceMachine,
      verification: "bridge", attempt: 1, sequence: publication.sequence,
      working() {}, async ack() { finished.resolve("ack"); },
      nak() { finished.resolve("nak"); }, async term() { finished.resolve("term"); },
    });
    return finished.promise;
  }
}
class Endpoint implements FabricTransport {
  readonly kind = "nats" as const;
  readonly deliveries = new Queue<TransportDelivery<TransportEnvelope>>();
  readonly commands = new Queue<Publication>();
  readonly acknowledgements = new Queue<Publication>();
  readonly reconnectListeners = new Set<() => void>();
  #closed = new AbortController();
  onVerifiedReconnect(listener: () => void): () => void {
    this.reconnectListeners.add(listener);
    return () => { this.reconnectListeners.delete(listener); };
  }
  reconnect(): void { for (const listener of [...this.reconnectListeners]) listener(); }
  constructor(readonly carrier: Carrier, readonly self: TransportOwner) {}
  async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    signal?.throwIfAborted(); this.#closed.signal.throwIfAborted();
    const destination = this.carrier.destinations.get(address(envelope.to));
    if (!destination) throw new TransportUnavailableError("Fixture destination absent");
    const createdAt = Date.now();
    const body = typeof envelope.body === "function"
      ? (envelope.body as (at: number) => T)(createdAt) : envelope.body;
    const resolved = JSON.parse(JSON.stringify({ ...envelope, body, createdAt })) as
      TransportEnvelope<T> & { body: T; createdAt: number };
    const publication: Publication = { envelope: resolved as unknown as WireEnvelope,
      sourceMachine: this.self.machine, sequence: ++this.carrier.sequence, destination };
    if (envelope.kind === "command") {
      this.commands.push(publication);
      void this.carrier.deliver(publication);
    } else this.acknowledgements.push(publication); // PubAck succeeds; ACK delivery is held.
    return { id: envelope.id, sequence: publication.sequence, duplicate: false, envelope: resolved };
  }
  async *inbox(signal: AbortSignal): AsyncIterable<TransportDelivery<TransportEnvelope>> {
    const local = new AbortController(), abort = () => local.abort();
    signal.addEventListener("abort", abort, { once: true });
    this.#closed.signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted || this.#closed.signal.aborted) local.abort();
    try {
      while (!local.signal.aborted) {
        const delivery = await this.deliveries.take(local.signal);
        if (!delivery) return;
        yield delivery;
      }
    } finally {
      signal.removeEventListener("abort", abort);
      this.#closed.signal.removeEventListener("abort", abort);
    }
  }
  kv(): TransportKv { throw new TransportUnsupportedError("Control-only fixture has no KV"); }
  async drainState(): Promise<TransportDrainState> { return { pending: 0, ackPending: 0 }; }
  async close(): Promise<void> { this.#closed.abort(); }
}
function observe<T>(promise: Promise<T>) {
  let result: { state: "pending" } | { state: "fulfilled"; value: T } | { state: "rejected"; error: unknown } = { state: "pending" };
  const settled = promise.then(value => { result = { state: "fulfilled", value }; },
    error => { result = { state: "rejected", error }; });
  return { get result() { return result; }, settled };
}
const fixtures: Fixture[] = [];
class Fixture {
  readonly root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cancel-recovery-"));
  readonly carrier = new Carrier();
  readonly routes = new Map<string, ParticipantRoute>();
  readonly planes: FabricControlPlane[] = [];
  readonly stores: MeshStore[] = [];
  readonly endpoints: Endpoint[] = [];
  executions = 0;
  leaseLive = true;
  readonly participants: ParticipantView = {
    resolve: id => this.routes.get(id), leaseLive: () => this.leaseLive,
    whenChanged: async () => { throw new Error("No directory watch expected"); },
  };
  constructor() { fixtures.push(this); }
  runtime(hostId: string, id: string, machine: string, incarnation: string) {
    const transport = this.carrier.endpoint({ hostId, machine });
    this.endpoints.push(transport);
    const mesh = new MeshStore(this.root, 64 * 1024, 1_000, { stateBackend: "file" });
    this.stores.push(mesh);
    const plane = new FabricControlPlane(mesh, identity(id), { enabled: true, hostId,
      ownerIncarnation: incarnation, participants: this.participants, transport,
      relayOperatorMachine: "fabric-ops", pollMs: 20, acknowledgementTimeoutMs: 1_000 });
    this.planes.push(plane);
    return { plane, transport };
  }
  owner(handler?: FabricControlHandler) {
    const owner = this.runtime(OWNER_HOST, OWNER_ID, "cancel-owner", EPOCH);
    const route: ParticipantRoute = { machine: "cancel-owner", ownerHostId: OWNER_HOST,
      ownerIdentityId: OWNER_ID, ownerIncarnation: EPOCH, capabilities: ["followUp"], live: true };
    this.routes.set(TARGET, route); this.routes.set(OWNER_ID, route);
    owner.plane.start((...args) => {
      this.executions++;
      return handler ? handler(...args) : { accepted: true, messageId: MESSAGE_ID };
    });
    return owner;
  }
  caller() {
    const caller = this.runtime(CALLER_HOST, CALLER_ID, "cancel-caller", "cancel:caller");
    this.routes.set(CALLER_ID, { machine: "cancel-caller", ownerHostId: CALLER_HOST,
      ownerIdentityId: CALLER_ID, ownerIncarnation: "cancel:caller", capabilities: [], live: true });
    caller.plane.start(() => { throw new Error("Caller must never execute this command"); });
    return caller;
  }
  retained(publication: Publication) {
    const seen = new MeshStore(path.join(this.root, "control-seen",
      createHash("sha256").update(OWNER_HOST).digest("hex").slice(0, 32)), 64 * 1024, 1_000, { stateBackend: "file" });
    this.stores.push(seen);
    const key = "topology/control-seen/" + createHash("sha256")
      .update(`${OWNER_HOST}\0${String(publication.envelope.body.commandId)}`).digest("hex");
    return seen.get(key, { fresh: true });
  }
  async dispose() {
    await Promise.all(this.planes.map(plane => plane.close()));
    await Promise.all(this.endpoints.map(endpoint => endpoint.close()));
    for (const store of this.stores) store.closeState();
    fs.rmSync(this.root, { recursive: true, force: true }); // Only this newly allocated fixture root.
  }
}
afterEach(async () => {
  try { for (const fixture of fixtures.splice(0)) await fixture.dispose(); }
  finally { vi.restoreAllMocks(); }
});
async function delivered(carrier: Carrier, ack: Publication, endpoint?: Endpoint) {
  expect(await carrier.deliver(ack, endpoint)).toBe("ack");
  await nextTurn();
}
function expectAccepted(observed: ReturnType<typeof observe<FabricControlResult>>) {
  expect(observed.result).toMatchObject({ state: "fulfilled", value: {
    acknowledged: true, queued: true, messageId: MESSAGE_ID, routed: "mesh",
  } });
}

type Caller = ReturnType<Fixture["caller"]>;
function cancellationFailure(caller: Caller, initial: Error) {
  const attempts = new Queue<TransportEnvelope>();
  const send = caller.transport.send.bind(caller.transport);
  let failure: Error | undefined = initial;
  caller.transport.send = async (envelope, signal) => {
    if (envelope.operation === "cancel") {
      attempts.push(envelope);
      if (failure) throw failure;
    }
    return send(envelope, signal);
  };
  return { attempts, restore(notify = true) {
    failure = undefined;
    if (notify) caller.transport.reconnect();
  } };
}
async function cancelled(f: Fixture, owner: ReturnType<Fixture["owner"]>, caller: Caller) {
  const abort = new AbortController();
  const request = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "original" },
    OWNER_ID, { idempotencyKey: "cancelled-original", signal: abort.signal }));
  const original = await caller.transport.commands.required();
  const originalAck = await owner.transport.acknowledgements.required();
  await nextTurn();
  abort.abort(); await request.settled;
  expect(request.result).toMatchObject({ state: "rejected", error: { message: expect.stringContaining("cancelled") } });
  expect(f.executions).toBe(1);
  return { original, originalAck };
}

describe("public NATS control cancellation recovery", () => {
  it.each([false, true])("closes with a usable carrier (injected cancellation failure: %s)", async fail => {
    const f = new Fixture(), owner = f.owner(), caller = f.caller();
    const attempted = deferred<void>(), send = caller.transport.send.bind(caller.transport);
    let cancellations = 0;
    caller.transport.send = async (envelope, signal) => {
      if (envelope.operation === "cancel") {
        cancellations++; attempted.resolve();
        if (fail) throw new TransportUnavailableError("NATS transport is temporarily unavailable");
      }
      return send(envelope, signal);
    };
    const abort = new AbortController();
    const request = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "original" },
      OWNER_ID, { idempotencyKey: "cancel-cause", signal: abort.signal }));
    await caller.transport.commands.required();
    const originalAck = await owner.transport.acknowledgements.required();
    await nextTurn(); // Original PubAck settled, but ACK delivery remains held.
    if (!fail) {
      await delivered(f.carrier, originalAck); await request.settled; expectAccepted(request);
      await expect(caller.plane.close()).resolves.toBeUndefined();
      expect(cancellations).toBe(0);
      return;
    }
    abort.abort(new Error("caller cancelled while broker was down"));
    await request.settled; await attempted.promise; await nextTurn();
    expect(request.result.state).toBe("rejected"); expect(cancellations).toBe(1);
    caller.transport.send = send; // Carrier restored: new public requests work again.
    const recovery = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "after recovery" },
      OWNER_ID, { idempotencyKey: "after-cancel-cause" }));
    const recoveredAck = await owner.transport.acknowledgements.required();
    await delivered(f.carrier, recoveredAck); await recovery.settled; expectAccepted(recovery);
    expect(originalAck.envelope.body.commandId).not.toBe(recoveredAck.envelope.body.commandId);
    await expect(caller.plane.close()).resolves.toBeUndefined();
  });

  it.each([true, false])("settles the identical cancel after recovery (reconnect wake: %s), without repeated receiver effects", async notify => {
    const f = new Fixture(), owner = f.owner(), caller = f.caller();
    const failure = cancellationFailure(caller, new TransportUnavailableError("network down"));
    const { original } = await cancelled(f, owner, caller);
    const first = await failure.attempts.required();
    await nextTurn();
    failure.restore(notify);
    const retry = await failure.attempts.required();
    expect(retry).toBe(first); // Stable envelope, body, deadline, destination AND command identity.
    const cancel = await caller.transport.commands.required();
    expect(cancel.envelope.id).toBe(first.id);
    expect(cancel.envelope.body).toMatchObject({ cancelCommandId: original.envelope.body.commandId,
      ownerIncarnation: EPOCH, targetId: TARGET });
    const ack = await owner.transport.acknowledgements.required();
    expect(ack.envelope.body).toMatchObject({ commandId: cancel.envelope.body.commandId, accepted: true });
    const retained = f.retained(cancel);
    expect(retained?.value).toMatchObject({ carrier: "nats", ownerIncarnation: EPOCH, acceptance: { accepted: true } });
    // Broker redelivery also follows the real durable dedupe path, not a carrier mock dedupe.
    expect(await f.carrier.deliver(cancel)).toBe("ack");
    await owner.transport.acknowledgements.required();
    expect(f.retained(cancel)).toEqual(retained);
    expect(f.executions).toBe(1); // Neither cancellation nor redelivery invokes the target handler.
    expect(failure.attempts.values).toHaveLength(0);
    expect(caller.transport.reconnectListeners.size).toBe(1); // Only the plane's inbox listener remains.
    caller.plane.pause();
    await expect(caller.plane.checkpointForRelease()).resolves.toBeUndefined();
    expect(caller.plane.releaseCheckpointReceipt()).toMatchObject({ admissionHeld: true });
    await expect(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "paused" }, OWNER_ID))
      .rejects.toMatchObject({ code: "FABRIC_TRANSPORT_DRAINING", notRun: true });
    expect(caller.transport.commands.values).toHaveLength(0);
    await expect(caller.plane.close()).resolves.toBeUndefined();
    expect(caller.transport.reconnectListeners.size).toBe(0);
  });

  it("keeps a persistent unavailable obligation owned through checkpoint/close and fails loudly AT its original deadline", async () => {
    const f = new Fixture(), owner = f.owner(), caller = f.caller();
    const failure = cancellationFailure(caller, new TransportUnavailableError("still unavailable"));
    await cancelled(f, owner, caller);
    const first = await failure.attempts.required();
    await nextTurn();
    caller.plane.pause();
    const checkpoint = observe(caller.plane.checkpointForRelease());
    const closing = observe(caller.plane.close());
    await nextTurn();
    expect(checkpoint.result.state).toBe("pending");
    expect(closing.result.state).toBe("pending");
    // Clock only: real I/O and event-loop scheduling stay live. No production constants shortened.
    vi.spyOn(Date, "now").mockReturnValue((first.body as { deadlineAt: number }).deadlineAt);
    caller.transport.reconnect();
    await checkpoint.settled; await closing.settled;
    for (const observed of [checkpoint, closing]) expect(observed.result).toMatchObject({ state: "rejected",
      error: { message: "Background mesh release has unconfirmed publication obligations" } });
    expect(failure.attempts.values).toHaveLength(0); // No publication at/after expiry.
    failure.restore(); // A late healthy carrier cannot relabel the unmet cancellation as success.
    await expect(caller.plane.checkpointForRelease()).rejects.toThrow("unconfirmed publication obligations");
    expect(() => caller.plane.releaseCheckpointReceipt()).toThrow();
    expect(f.executions).toBe(1);
    expect(caller.transport.reconnectListeners.size).toBe(0);
  });

  it.each([
    ["programming", new Error("programming bug")],
    ["invalid", new TransportInvalidError("invalid envelope")],
    ["denied", new TransportDeniedError("permission denied")],
    ["uncertain", new TransportUncertainError("uncertain-cancel", {})],
  ] as const)("does not retry or forgive a permanent %s failure after unrelated public success", async (_kind, error) => {
    const f = new Fixture(), owner = f.owner(), caller = f.caller();
    const failure = cancellationFailure(caller, error);
    await cancelled(f, owner, caller); await failure.attempts.required(); await nextTurn();
    failure.restore();
    const recovery = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "healthy" },
      OWNER_ID, { idempotencyKey: "healthy-after-permanent-failure" }));
    await caller.transport.commands.required();
    await delivered(f.carrier, await owner.transport.acknowledgements.required());
    await recovery.settled; expectAccepted(recovery);
    caller.plane.pause();
    await expect(caller.plane.checkpointForRelease()).rejects.toThrow("unconfirmed publication obligations");
    await expect(caller.plane.close()).rejects.toThrow("unconfirmed publication obligations");
    expect(failure.attempts.values).toHaveLength(0);
    expect(f.executions).toBe(2); // Original and unrelated recovery only.
  });

  it("aborts an admitted ASK only once and does not admit new target work after pause or lease loss", async () => {
    const f = new Fixture(), admitted = deferred<void>(), aborted = deferred<void>();
    let aborts = 0;
    const owner = f.owner(async (_command, _from, signal) => {
      admitted.resolve();
      await new Promise<void>(resolve => signal.addEventListener("abort", () => {
        aborts++; aborted.resolve(); resolve();
      }, { once: true }));
      return { accepted: true, messageId: MESSAGE_ID };
    });
    const caller = f.caller(), abort = new AbortController();
    const failure = cancellationFailure(caller, new TransportUnavailableError("ASK cancellation carrier unavailable"));
    const request = observe(caller.plane.request(OWNER_HOST, TARGET, "ask", { message: "active ASK" },
      OWNER_ID, { signal: abort.signal, idempotencyKey: "active-ask" }));
    await caller.transport.commands.required(); await admitted.promise; await nextTurn();
    abort.abort(); await request.settled; await failure.attempts.required(); await nextTurn();
    expect(aborts).toBe(0);
    failure.restore(); await failure.attempts.required();
    const cancel = await caller.transport.commands.required();
    await aborted.promise;
    // ASK and cancel outcomes save concurrently; consume both real publication gates.
    const firstAck = await owner.transport.acknowledgements.required();
    const secondAck = await owner.transport.acknowledgements.required();
    expect([firstAck, secondAck].some(ack => ack.envelope.body.commandId === cancel.envelope.body.commandId)).toBe(true);
    expect(await f.carrier.deliver(cancel)).toBe("ack");
    await owner.transport.acknowledgements.required();
    expect(aborts).toBe(1); expect(f.executions).toBe(1);
    owner.plane.pause();
    const blockedAbort = new AbortController();
    const blocked = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "must not run" },
      OWNER_ID, { signal: blockedAbort.signal, idempotencyKey: "paused-owner" }));
    await caller.transport.commands.required(); await nextTurn();
    expect(f.executions).toBe(1);
    f.leaseLive = false;
    owner.plane.resume(); await nextTurn(); // An explicit resume cannot override the live-lease fence.
    expect(f.executions).toBe(1);
    blockedAbort.abort(); await blocked.settled;
    await caller.transport.commands.required(); // Cancellation may publish, but never bypass receiver admission.
    await expect(caller.plane.close()).resolves.toBeUndefined();
    expect(aborts).toBe(1); expect(f.executions).toBe(1);
  });

  it.each([false, true])("retains a cancellation that becomes publishable only AFTER close (original publication uncertain: %s)", async uncertain => {
    const f = new Fixture(), owner = f.owner(), caller = f.caller();
    const published = deferred<void>(), release = deferred<void>(), abort = new AbortController();
    const failure = cancellationFailure(caller, new TransportUnavailableError("late cancel unavailable"));
    const send = caller.transport.send.bind(caller.transport);
    caller.transport.send = async (envelope, signal) => {
      const sent = await send(envelope, signal);
      if (envelope.operation !== "cancel") {
        published.resolve(); await release.promise;
        if (uncertain) throw new TransportUncertainError(envelope.id, sent.envelope.body);
      }
      return sent;
    };
    const request = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "in-flight PubAck" },
      OWNER_ID, { signal: abort.signal, idempotencyKey: "late-publication" }));
    await published.promise; await caller.transport.commands.required();
    await owner.transport.acknowledgements.required();
    abort.abort(); await request.settled;
    expect(failure.attempts.values).toHaveLength(0);
    const closing = observe(caller.plane.close());
    await nextTurn(); expect(closing.result.state).toBe("pending");
    release.resolve();
    const first = await failure.attempts.required(); await nextTurn();
    expect(closing.result.state).toBe("pending");
    failure.restore();
    expect(await failure.attempts.required()).toBe(first);
    await closing.settled;
    expect(closing.result).toMatchObject({ state: "fulfilled" });
    const cancel = await caller.transport.commands.required();
    expect((await owner.transport.acknowledgements.required()).envelope.body)
      .toMatchObject({ commandId: cancel.envelope.body.commandId, accepted: true });
    expect(f.executions).toBe(1);
  });

  it("leaves the generic queue fail-closed even when a later notification succeeds", async () => {
    const queue = new MeshBackgroundQueue("permanent countercontrol");
    try {
      await queue.enqueue(() => { throw new TransportInvalidError("not a mesh-lock retry"); });
      await expect(queue.checkpointForRelease()).rejects.toThrow("unconfirmed publication obligations");
      await queue.enqueue(() => undefined);
      await expect(queue.checkpointForRelease()).rejects.toThrow("unconfirmed publication obligations");
    } finally { await queue.close(); }
  });
});
