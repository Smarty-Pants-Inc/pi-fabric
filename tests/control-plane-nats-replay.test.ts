import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane, type FabricControlResult } from "../src/topology/control-plane.js";
import type { ParticipantRoute, ParticipantView } from "../src/topology/participant-view.js";
import {
  TransportUnavailableError,
  TransportUnsupportedError,
  type FabricTransport,
  type TransportDelivery,
  type TransportDrainState,
  type TransportEnvelope,
  type TransportKv,
  type TransportOwner,
  type TransportSent,
} from "../src/transport/types.js";

// Decision C: unit/control-protocol proof using the REAL request/inbox/handler and on-disk
// control-seen store. Not a live NATS broker, host-lease acquisition, or real Main canary.
// Acceptance ledger: successor replay settles once; both pending-epoch ACK fences survive;
// a pre-death durable OLD ACK still settles the ORIGINAL OLD pending request (lane C B2c).
const OWNER_HOST = "runtime:e24-owner";
const OWNER_ID = "session:e24-owner";
const CALLER_HOST = "runtime:e24-caller";
const CALLER_ID = "session:e24-caller";
const TARGET = "session:e24-target";
const OLD = "e24:owner-old";
const CURRENT = "e24:owner-current";
const KEY = "e24-same-logical-request";
const MESSAGE_ID = "e24-executed-once";
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const address = (owner: TransportOwner): string => `${owner.machine}/${owner.hostId}`;
const nextTurn = (): Promise<void> => new Promise(resolve => setImmediate(resolve));

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

// Event gates, not timer polling. Each endpoint has its own captured inbox even when a new
// endpoint takes over the same stable host address. Controlled ACK copies can target the old one.
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
  readonly body: Record<string, unknown>;
  readonly createdAt: number;
};
interface Publication {
  envelope: WireEnvelope;
  sourceMachine: string;
  sequence: number;
  destination: Endpoint;
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
      working() {},
      async ack() { finished.resolve("ack"); },
      nak() { finished.resolve("nak"); },
      async term() { finished.resolve("term"); },
    });
    return finished.promise;
  }
}

class Endpoint implements FabricTransport {
  readonly kind = "nats" as const;
  readonly deliveries = new Queue<TransportDelivery<TransportEnvelope>>();
  readonly commands = new Queue<Publication>();
  readonly acknowledgements = new Queue<Publication>();
  readonly refusedAcknowledgement = deferred<void>();
  refuseAcknowledgements = false;
  holdCommands = false;
  publishedAckCount = 0;
  #closed = new AbortController();
  constructor(readonly carrier: Carrier, readonly self: TransportOwner) {}
  async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    signal?.throwIfAborted();
    this.#closed.signal.throwIfAborted();
    // A pre-persistence failure: no ACK bytes exist in the carrier for case 2.
    if (envelope.kind === "ack" && this.refuseAcknowledgements) {
      this.refusedAcknowledgement.resolve();
      throw new TransportUnavailableError("Fixture: owner died without persisting an ACK");
    }
    const destination = this.carrier.destinations.get(address(envelope.to));
    if (!destination) throw new TransportUnavailableError("Fixture destination absent");
    const createdAt = Date.now();
    // Frozen port: evaluate the body stamp once, synchronously before persistence.
    const body = typeof envelope.body === "function"
      ? (envelope.body as (at: number) => T)(createdAt) : envelope.body;
    const resolved = JSON.parse(JSON.stringify({ ...envelope, body, createdAt })) as
      TransportEnvelope<T> & { body: T; createdAt: number };
    const sequence = ++this.carrier.sequence;
    const publication: Publication = { envelope: resolved as unknown as WireEnvelope,
      sourceMachine: this.self.machine, sequence, destination };
    if (envelope.kind === "command") {
      this.commands.push(publication);
      if (!this.holdCommands) void this.carrier.deliver(publication);
    } else {
      this.publishedAckCount++;
      this.acknowledgements.push(publication); // PubAck succeeds; delivery is held separately.
    }
    return { id: envelope.id, sequence, duplicate: false, envelope: resolved };
  }
  async *inbox(signal: AbortSignal): AsyncIterable<TransportDelivery<TransportEnvelope>> {
    const local = new AbortController();
    const abort = () => local.abort();
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
  async drainState(): Promise<TransportDrainState> {
    throw new TransportUnsupportedError("This fixture proves no broker drain receipt");
  }
  async close(): Promise<void> { this.#closed.abort(); }
}

type Observation<T> = { state: "pending" } | { state: "fulfilled"; value: T } | { state: "rejected"; error: unknown };
function observe<T>(promise: Promise<T>) {
  let result: Observation<T> = { state: "pending" };
  const settled = promise.then(value => { result = { state: "fulfilled", value }; },
    error => { result = { state: "rejected", error }; });
  return { get result() { return result; }, settled };
}

const fixtures: Fixture[] = [];
class Fixture {
  readonly root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-e24-"));
  readonly carrier = new Carrier();
  readonly routes = new Map<string, ParticipantRoute>();
  readonly planes: FabricControlPlane[] = [];
  readonly stores: MeshStore[] = [];
  readonly endpoints: Endpoint[] = [];
  executions = 0;
  readonly participants: ParticipantView = {
    resolve: id => this.routes.get(id),
    leaseLive: () => true,
    whenChanged: async () => { throw new Error("No directory watch expected on ACK ingress"); },
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
  owner(incarnation: string, machine = "e24-owner") {
    const owner = this.runtime(OWNER_HOST, OWNER_ID, machine, incarnation);
    const route: ParticipantRoute = { machine, ownerHostId: OWNER_HOST,
      ownerIdentityId: OWNER_ID, ownerIncarnation: incarnation, capabilities: ["followUp"], live: true };
    // Explicit successor routing; no lease or directory implementation is mocked as evidence.
    this.routes.set(TARGET, route);
    this.routes.set(OWNER_ID, route);
    owner.plane.start(() => { this.executions++; return { accepted: true, messageId: MESSAGE_ID }; });
    return owner;
  }
  caller() {
    // Recreated callers retain ALL stable source/command-id bindings, not just a new caller label.
    const caller = this.runtime(CALLER_HOST, CALLER_ID, "e24-caller", "e24:caller");
    this.routes.set(CALLER_ID, { machine: "e24-caller", ownerHostId: CALLER_HOST,
      ownerIdentityId: CALLER_ID, ownerIncarnation: "e24:caller", capabilities: [], live: true });
    caller.plane.start(() => { throw new Error("Caller must never execute this command"); });
    return caller;
  }
  send(caller: ReturnType<Fixture["caller"]>) {
    return observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "execute exactly once" },
      OWNER_ID, { idempotencyKey: KEY })); // No explicit epoch: REAL request re-resolves current route.
  }
  retained(command: Publication): Record<string, unknown> {
    const directory = path.join(this.root, "control-seen", createHash("sha256").update(OWNER_HOST).digest("hex").slice(0, 32));
    const seen = new MeshStore(directory, 64 * 1024, 1_000, { stateBackend: "file" });
    this.stores.push(seen);
    const key = "topology/control-seen/" + createHash("sha256")
      .update(`${OWNER_HOST}\0${String(command.envelope.body.commandId)}`).digest("hex");
    const record = seen.get(key, { fresh: true })?.value as Record<string, unknown> | undefined;
    expect(record).toMatchObject({ carrier: "nats", ownerIncarnation: OLD,
      sourceMachine: "e24-caller", requesterId: CALLER_ID,
      acceptance: { accepted: true, messageId: MESSAGE_ID } });
    return record!;
  }
  async dispose() {
    await Promise.all(this.planes.map(plane => plane.close()));
    await Promise.all(this.endpoints.map(endpoint => endpoint.close()));
    for (const store of this.stores) store.closeState();
    fs.rmSync(this.root, { recursive: true, force: true });
  }
}

afterEach(async () => {
  for (const fixture of fixtures.splice(0)) await fixture.dispose();
  vi.restoreAllMocks(); vi.useRealTimers();
});

async function delivered(carrier: Carrier, ack: Publication, endpoint?: Endpoint) {
  expect(await carrier.deliver(ack, endpoint)).toBe("ack");
  // ACK ingress is synchronous before broker ack(). A real event-loop turn drains the public
  // request's promise continuations without polling or advancing filesystem timers.
  await nextTurn();
}
function expectAccepted(observed: ReturnType<typeof observe<FabricControlResult>>) {
  // Primary regression assertion is PUBLIC settlement, before checking informational metadata.
  expect(observed.result).toMatchObject({ state: "fulfilled", value: {
    acknowledged: true, queued: true, messageId: MESSAGE_ID, routed: "mesh",
  } });
}
function expectSameLogicalRequest(first: Publication, retry: Publication) {
  expect(retry.envelope.id).not.toBe(first.envelope.id);
  expect(retry.envelope.body.commandId).toBe(first.envelope.body.commandId);
  expect(first.envelope.body.ownerIncarnation).toBe(OLD);
  expect(retry.envelope.body.ownerIncarnation).toBe(CURRENT);
  expect(retry.envelope.from).toEqual(first.envelope.from);
}

// Keep reason: cross-machine late confirmations must preserve proof-only resend and ACK authenticity.
describe("late cross-machine NATS message confirmation", () => {
  it.each([false, true])("late notRun permits only one resend (second expires=%s)", async secondExpires => {
    const f = new Fixture(), owner = f.owner(OLD), caller = f.caller();
    caller.transport.holdCommands = true;
    const resume = deferred<void>(), claimed = deferred<void>();
    const ownRoot = path.join(f.root, "control-seen", createHash("sha256").update(OWNER_HOST).digest("hex").slice(0, 32));
    const writeBatch = MeshStore.prototype.writeBatch;
    let held = false;
    vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function (this: MeshStore, input) {
      const result = await writeBatch.call(this, input);
      if (!held && this.root === ownRoot && input.ops.some(op => op.kind === "put" && op.ifVersion === 0)) {
        held = true; claimed.resolve(); await resume.promise;
      }
      return result;
    });
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const start = Date.now(), request = f.send(caller);
    try {
      const first = await caller.transport.commands.required();
      expect(Number(first.envelope.body.deadlineAt) - Number(first.envelope.body.requestedAt)).toBe(30_000);
      await vi.advanceTimersByTimeAsync(29_700);
      const firstDelivery = f.carrier.deliver(first);
      await claimed.promise;
      await vi.advanceTimersByTimeAsync(301);
      resume.resolve();
      expect(await firstDelivery).toBe("ack");
      const expired = await owner.transport.acknowledgements.required();
      expect(expired.envelope.body).toMatchObject({ accepted: false, notRun: true, error: "Fabric control command expired" });
      expect(f.executions).toBe(0);
      await vi.advanceTimersByTimeAsync(start + 45_000 - Date.now());
      expect(request.result.state).toBe("pending");
      await vi.advanceTimersByTimeAsync(1_000);
      const envelope = expired.envelope;
      for (const forged of [
        { ...expired, sourceMachine: "unrelated" },
        { ...expired, envelope: { ...envelope, from: identity("session:unrelated") } },
        ...[{ ownerIncarnation: "unrelated" }, { targetId: "session:unrelated" }, { bodyDigest: "unrelated" }].map(body =>
          ({ ...expired, envelope: { ...envelope, body: { ...envelope.body, ...body } } })),
      ]) {
        await delivered(f.carrier, forged);
        expect(request.result.state).toBe("pending");
      }
      await delivered(f.carrier, expired);
      await vi.advanceTimersByTimeAsync(50);
      const second = await caller.transport.commands.required();
      expect(second.envelope.body).toMatchObject({ requestCreatedAt: first.envelope.body.requestCreatedAt,
        ownerIncarnation: OLD, message: first.envelope.body.message });
      expect(second.envelope.to).toEqual(first.envelope.to);
      expect(second.envelope.body.commandId).not.toBe(first.envelope.body.commandId);
      expect(Number(second.envelope.body.deadlineAt) - Number(second.envelope.body.requestedAt)).toBe(29_950);
      if (secondExpires) await vi.advanceTimersByTimeAsync(30_001);
      expect(await f.carrier.deliver(second)).toBe("ack");
      const result = await owner.transport.acknowledgements.required();
      if (secondExpires) await vi.advanceTimersByTimeAsync(Number(second.envelope.body.requestedAt) + 46_000 - Date.now());
      await delivered(f.carrier, result); await request.settled;
      if (secondExpires) expect(request.result).toMatchObject({ state: "rejected", error: { notRun: true } });
      else expectAccepted(request);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(caller.transport.commands.values).toHaveLength(0);
      expect(f.executions).toBe(secondExpires ? 0 : 1);
    } finally { resume.resolve(); await nextTurn(); }
  });

  it.each([true, false])("late executed outcome (accepted=%s) stays unknown without retry", async accepted => {
    const f = new Fixture(), owner = f.owner(OLD), caller = f.caller();
    owner.plane.start(() => {
      f.executions++;
      return accepted ? { accepted: true, messageId: MESSAGE_ID } : { accepted: false, error: "executed rejection" };
    });
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    const request = f.send(caller), first = await caller.transport.commands.required();
    const ack = await owner.transport.acknowledgements.required();
    expect(ack.envelope.body).not.toHaveProperty("notRun");
    await vi.advanceTimersByTimeAsync(45_000); expect(request.result.state).toBe("pending");
    await vi.advanceTimersByTimeAsync(1_000); await delivered(f.carrier, ack); await request.settled;
    expect(request.result).toMatchObject({ state: "rejected", error: { message: expect.stringContaining("the outcome is unknown") } });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(caller.transport.commands.values.filter(item => item.envelope.operation === first.envelope.operation)).toHaveLength(0);
    expect(f.executions).toBe(1);
  });

  it.each(["same-machine", "stop", "ask", "result-message"] as const)("does not extend the ordinary NATS bound for %s", async mode => {
    const f = new Fixture(); f.owner(OLD, mode === "same-machine" ? "e24-caller" : "e24-owner");
    const caller = f.caller();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    const request = mode === "ask" || mode === "result-message"
      ? observe(caller.plane.requestResult(OWNER_HOST, TARGET, mode === "ask" ? "ask" : "followUp", {}, OWNER_ID, { timeoutMs: 30_000 }))
      : mode === "stop" ? observe(caller.plane.request(OWNER_HOST, TARGET, "stop", {}, OWNER_ID)) : f.send(caller);
    await caller.transport.commands.required();
    await vi.advanceTimersByTimeAsync(44_999); expect(request.result.state).toBe("pending");
    await vi.advanceTimersByTimeAsync(1); await request.settled;
    expect(request.result).toMatchObject({ state: "rejected", error: { message: expect.stringContaining("the outcome is unknown") } });
  });

  it("no ACK is bounded unknown without resend", async () => {
    const f = new Fixture(); f.owner(OLD); const caller = f.caller();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    const request = f.send(caller), first = await caller.transport.commands.required();
    await vi.advanceTimersByTimeAsync(45_000); expect(request.result.state).toBe("pending");
    await vi.advanceTimersByTimeAsync(4_999); expect(request.result.state).toBe("pending");
    await vi.advanceTimersByTimeAsync(1); await request.settled;
    expect(request.result).toMatchObject({ state: "rejected", error: { message: expect.stringContaining("the outcome is unknown") } });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(caller.transport.commands.values.filter(item => item.envelope.operation === first.envelope.operation)).toHaveLength(0);
    expect(f.executions).toBe(1);
  });
});

describe("E24 NATS retained outcome replay across owner restart (Decision C)", () => {
  it("binds a caller key to semantic input while retaining identical-retry dedupe", async () => {
    const f = new Fixture(), owner = f.owner(OLD), caller = f.caller();
    const exchange = async (message: string, destination: ReturnType<Fixture["owner"]>): Promise<Publication> => {
      const request = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message }, OWNER_ID, { idempotencyKey: KEY }));
      const command = await caller.transport.commands.required();
      const ack = await destination.transport.acknowledgements.required();
      await delivered(f.carrier, ack, caller.transport);
      await request.settled;
      expectAccepted(request);
      return command;
    };

    const first = await exchange("original message", owner);
    const successor = f.owner(CURRENT, "e24-owner-v2");
    const retry = observe(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "original message" }, OWNER_ID,
      { idempotencyKey: KEY }));
    const retried = await Promise.race([
      caller.transport.commands.required().then(command => ({ kind: "published" as const, command })),
      retry.settled.then(() => ({ kind: "settled" as const, result: retry.result })),
    ]);
    expect(retried.kind, retried.kind === "settled" ? JSON.stringify(retried.result) : undefined).toBe("published");
    if (retried.kind !== "published") return;
    const currentEpoch = retried.command;
    expectSameLogicalRequest(first, currentEpoch); // A new owner epoch must not change caller-input identity.
    const replay = await successor.transport.acknowledgements.required();
    await delivered(f.carrier, replay, caller.transport);
    await retry.settled;
    expectAccepted(retry); // The changed route returns the same original receipt.
    await expect(caller.plane.request(OWNER_HOST, TARGET, "followUp", { message: "edited message" }, OWNER_ID,
      { idempotencyKey: KEY })).rejects.toMatchObject({
        name: "FabricIdempotencyKeyReusedError", code: "FABRIC_IDEMPOTENCY_KEY_REUSED", retryable: false,
      });
    expect(caller.transport.commands.values).toHaveLength(0);
    expect(f.executions).toBe(1);
  });
  it("settles a recreated stable caller's current-epoch retry from disk, without executing twice", async () => {
    const f = new Fixture(), owner = f.owner(OLD), caller = f.caller();
    const original = f.send(caller);
    const first = await caller.transport.commands.required();
    const withheld = await owner.transport.acknowledgements.required();
    f.retained(first); // ACK persistence necessarily follows the REAL durable outcome write.
    expect(withheld.envelope.body.ownerIncarnation).toBe(OLD);
    expect(original.result.state).toBe("pending");
    await owner.plane.close();
    await caller.plane.close();
    await original.settled;
    const successor = f.owner(CURRENT), recreated = f.caller();
    const retry = f.send(recreated);
    const second = await recreated.transport.commands.required();
    const replay = await successor.transport.acknowledgements.required();
    expectSameLogicalRequest(first, second);
    // The captured pre-death OLD ACK must NOT settle this CURRENT-epoch pending retry.
    await delivered(f.carrier, withheld, recreated.transport);
    expect(retry.result.state).toBe("pending");
    await delivered(f.carrier, replay);
    expectAccepted(retry); // RED before E24: real ingress drops the incorrectly OLD-stamped replay.
    expect(replay.envelope.body).toMatchObject({ ownerIncarnation: CURRENT, originalOwnerIncarnation: OLD });
    expect(f.executions).toBe(1);
    f.retained(second); // Informational original epoch remains durable; do not rewrite its history.
  }, 15_000);

  it("does not let a current replay settle an original old-epoch pending request; timeout is UNKNOWN", async () => {
    const f = new Fixture(), owner = f.owner(OLD), originalCaller = f.caller();
    owner.transport.refuseAcknowledgements = true;
    const original = f.send(originalCaller);
    const first = await originalCaller.transport.commands.required();
    await owner.transport.refusedAcknowledgement.promise;
    f.retained(first);
    expect(owner.transport.publishedAckCount).toBe(0); // No pre-death durable ACK exists in this case.
    await owner.plane.close();
    const successor = f.owner(CURRENT), recreated = f.caller();
    const retry = f.send(recreated);
    const second = await recreated.transport.commands.required();
    const replay = await successor.transport.acknowledgements.required();
    expectSameLogicalRequest(first, second);
    // ACK-ingress protocol counterexample ONLY: two same-host caller objects alive is NOT a
    // production admission path or a two-live-runtime proof. We copy real wire bytes to the
    // captured old endpoint solely to test the epoch captured BEFORE owner death.
    await delivered(f.carrier, replay, originalCaller.transport);
    expect(original.result.state).toBe("pending");
    await delivered(f.carrier, replay);
    expectAccepted(retry);
    expect(replay.envelope.body).toMatchObject({ ownerIncarnation: CURRENT, originalOwnerIncarnation: OLD });
    expect(f.executions).toBe(1);
    // Keep REAL timers for mesh filesystem I/O. Product minimum is 30s plus 15s ACK grace;
    // no shortened constants, forged refusal, cancellation, or private timeout invocation.
    await original.settled;
    expect(original.result).toMatchObject({ state: "rejected", error: {
      message: expect.stringContaining("outcome is unknown"), idempotencyKey: KEY,
    } });
    if (original.result.state === "rejected") {
      expect(original.result.error).not.toHaveProperty("notRun", true);
      expect(original.result.error).not.toHaveProperty("notDelivered", true);
    }
    expect(f.executions).toBe(1);
  }, 90_000);

  it("allows a pre-death persisted OLD ACK, merely delayed, to settle the ORIGINAL OLD pending request (B2c)", async () => {
    const f = new Fixture(), owner = f.owner(OLD), caller = f.caller();
    const original = f.send(caller);
    const first = await caller.transport.commands.required();
    const durableBeforeDeath = await owner.transport.acknowledgements.required();
    f.retained(first);
    expect(owner.transport.publishedAckCount).toBe(1);
    expect(durableBeforeDeath.envelope.body.ownerIncarnation).toBe(OLD);
    await owner.plane.close();
    f.owner(CURRENT); // Mutable view now points at successor, but pending authority stays OLD.
    expect(f.routes.get(TARGET)?.ownerIncarnation).toBe(CURRENT);
    await delivered(f.carrier, durableBeforeDeath);
    expectAccepted(original);
    expect(f.executions).toBe(1);
    expect(caller.transport.commands.values).toHaveLength(0); // No automatic retry was published.
  }, 15_000);
});
