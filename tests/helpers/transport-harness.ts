// Frozen CONTRACT-v2 §§1,3–5,7,11 + erratum E1. Test scaffolding, not a carrier implementation.
// Keep reason: data safety and cross-machine trust-boundary fault tests.
// Only lightweight port errors and E16a's existing fingerprint load eagerly; no carrier,
// NATS, vitest, config, environment, filesystem or server setup imports.
import { AsyncLocalStorage } from "node:async_hooks";
import { controlBodyFingerprint } from "../../src/mesh/control-idempotency.js";
import type { FabricControlCommand } from "../../src/topology/control-plane.js";
import type { MeshIdentity } from "../../src/mesh/event-log.js";
import type { FabricPrincipal } from "../../src/fabric-provenance.js";
import type { FabricMessageSender } from "../../src/protocol.js";
import type {
  TransportOwner, TransportEnvelope, TransportSent, TransportDelivery, TransportKvEntry,
  TransportKvEvent, TransportKv, TransportBucket, TransportDrainState, FabricTransport,
  FabricTransportOptions, FabricTransportFactory,
} from "../../src/transport/types.js";
export type {
  TransportOwner, TransportEnvelope, TransportSent, TransportDelivery, TransportKvEntry,
  TransportKvEvent, TransportKv, TransportBucket, TransportDrainState, FabricTransport,
  FabricTransportOptions, FabricTransportFactory,
} from "../../src/transport/types.js";
import { TransportDeniedError, TransportUnavailableError, TransportUncertainError } from "../../src/transport/types.js";
export type ResolvedEnvelope<T = unknown> = TransportEnvelope<T> & { readonly body: T; readonly createdAt: number };
export const contractTiming = Object.freeze({
  retryHorizonMs: 24 * 60 * 60 * 1000, duplicateWindowMs: 10 * 60 * 1000,
  ackWaitMs: 30_000, workingEveryMs: 10_000, leaseTtlMs: 15_000,
  leaseRenewMs: 5_000, leaseSafetyMs: 2_000, presenceTtlMs: 10 * 60 * 1000,
});
// §10.3 describes a real-recipient canary, NOT a CI/stub performance claim.
export const risingNSteps = [1, 4, 16, 64] as const;
export const FIXTURE_TIME = 1_800_000_000_000;

export function owner(machine = "test-a", hostId = "session:transport-test"): TransportOwner {
  if (!/^[a-z0-9][a-z0-9-]{0,31}$/.test(machine)) throw new TypeError("Invalid fixture machine literal");
  if (!hostId) throw new TypeError("Fixture hostId must not be empty");
  return { machine, hostId };
}
export function identity(id = "session:transport-sender"): MeshIdentity {
  return { id, name: "transport-test", kind: "main" };
}

type CommandSemantics = Pick<FabricControlCommand, "wakeCause" | "triggerTurn" | "priority" | "binding" | "bindingProvenance">;
export interface CommandBody extends CommandSemantics {
  readonly version: 1;
  readonly commandId: string;
  readonly targetId: string;
  readonly operation: string;
  readonly ownerIncarnation: string;
  readonly replyTo: string;
  readonly requestedAt: number;
  readonly deadlineAt: number;
  readonly message?: string;
  readonly data?: unknown;
}
export interface CommandOptions extends CommandSemantics {
  readonly id?: string;
  readonly to?: TransportOwner;
  readonly from?: MeshIdentity;
  readonly targetId?: string;
  readonly operation?: string;
  readonly ownerIncarnation?: string;
  readonly replyTo?: string;
  readonly message?: string;
  readonly data?: unknown;
  readonly principal?: FabricPrincipal;
  readonly sender?: FabricMessageSender;
  readonly timeoutMs?: number;
  readonly body?: TransportEnvelope<CommandBody>["body"];
}
/** Default body is a commit-time stamp, not a clock read during fixture construction. */
export function commandEnvelope(options: CommandOptions = {}): TransportEnvelope<CommandBody> {
  const id = options.id ?? "command-1", to = options.to ?? owner();
  const from = options.from ?? identity(), targetId = options.targetId ?? to.hostId;
  const operation = options.operation ?? "followUp", timeoutMs = options.timeoutMs ?? 60_000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > contractTiming.retryHorizonMs) {
    throw new RangeError("Fixture timeout must be > 0 and <= 24 h");
  }
  return {
    id, kind: "command", to, from, targetId, operation,
    ...(options.principal === undefined ? {} : { principal: options.principal }),
    ...(options.sender === undefined ? {} : { sender: options.sender }),
    body: options.body ?? ((committedAt: number) => ({
      version: 1, commandId: id, targetId, operation,
      ownerIncarnation: options.ownerIncarnation ?? "incarnation-1",
      replyTo: options.replyTo ?? from.id, requestedAt: committedAt, deadlineAt: committedAt + timeoutMs,
      ...(options.message === undefined ? {} : { message: options.message }),
      ...(options.data === undefined ? {} : { data: options.data }),
      ...(options.wakeCause === undefined ? {} : { wakeCause: options.wakeCause }),
      ...(options.triggerTurn === undefined ? {} : { triggerTurn: options.triggerTurn }),
      ...(options.priority === undefined ? {} : { priority: options.priority }),
      ...(options.binding === undefined ? {} : { binding: options.binding }),
      ...(options.bindingProvenance === undefined ? {} : { bindingProvenance: options.bindingProvenance }),
    })),
  };
}
/** ACK bodies remain caller-owned: this helper does not invent an admission/outcome schema. */
export function ackEnvelope<T>(commandId: string, body: T, options: {
  readonly to?: TransportOwner; readonly from?: MeshIdentity; readonly targetId?: string;
  readonly operation?: "accepted" | "rejected"; readonly relay?: boolean;
} = {}): TransportEnvelope<T> {
  const to = options.to ?? owner("test-b", "session:transport-sender");
  return { id: `${options.relay ? "relay" : "ack"}:${commandId}`, kind: "ack", to,
    from: options.from ?? identity("session:transport-test"), targetId: options.targetId ?? to.hostId,
    operation: options.operation ?? "accepted", body };
}

/** Explicit fixture operation. Never call this before a real adapter send: it owns the stamp. */
export function resolveEnvelope<T>(envelope: TransportEnvelope<T>, committedAt = FIXTURE_TIME): ResolvedEnvelope<T> {
  if (!Number.isFinite(committedAt)) throw new TypeError("Invalid fixture commit time");
  const body = typeof envelope.body === "function"
    ? (envelope.body as (time: number) => T)(committedAt) : envelope.body;
  if (body !== null && typeof body === "object" && "then" in body && typeof body.then === "function") {
    throw new TypeError("A transport body stamp must be synchronous");
  }
  return { ...envelope, body, createdAt: committedAt };
}
/** Retry an uncertain attempt's resolved bytes; do not invoke its original stamp again. */
export function resolvedRetry<T>(envelope: TransportEnvelope<T>, resolvedBody: T): TransportEnvelope<T> {
  return { ...envelope, body: resolvedBody };
}

/** JSON semantics first (toJSON, omission, array holes), then lexical keys, including numeric keys.
 * Encoding directly rather than rebuilding objects preserves literal __proto__ and numeric-key order.
 */
export function canonicalJson(value: unknown): string {
  const wire = JSON.stringify(value);
  if (wire === undefined) throw new TypeError("Fixture value is not JSON serializable");
  const encode = (item: unknown): string => {
    if (Array.isArray(item)) return `[${item.map(encode).join(",")}]`;
    if (item !== null && typeof item === "object") {
      const record = item as Record<string, unknown>;
      return `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${encode(record[key])}`).join(",")}}`;
    }
    return JSON.stringify(item)!;
  };
  return encode(JSON.parse(wire));
}
/** E16a supersedes §3.2's provisional field list. Use the SAME production canonical input
 * and semantic projection, including its principal handling; no independent digest policy.
 * Epoch/route authentication is separate. This oracle never authorizes or admits a command.
 */
export function bodyDigest(body: unknown, principal?: FabricPrincipal): string {
  return controlBodyFingerprint(body, principal);
}

const abortReason = (signal: AbortSignal): unknown => signal.reason ?? new Error("Transport harness aborted");
const positiveDuration = (ms: number): void => {
  if (!Number.isFinite(ms) || ms <= 0 || ms > 2_147_483_647) throw new RangeError("A finite positive timeout is required");
};
/** A receipt deadline, never a polling interval. The underlying operation is still owned by
 * its caller: timeout does not cancel it, return an iterator, or turn cleanup into success.
 */
export async function boundedReceipt<T>(receipt: Promise<T>, label: string, timeoutMs = 5_000): Promise<T> {
  positiveDuration(timeoutMs);
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([receipt, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`bounded transport receipt deadline: ${label}`)), timeoutMs);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}
async function signalReceipt<T>(receipt: Promise<T>, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  let cancel: (() => void) | undefined;
  try {
    return await Promise.race([receipt, new Promise<never>((_resolve, reject) => {
      cancel = () => reject(abortReason(signal));
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) cancel();
    })]);
  } finally { if (cancel) signal.removeEventListener("abort", cancel); }
}
/** Event-driven, bounded observation; cancellation does NOT ack/nak in-hand deliveries. */
export async function collectInbox(transport: FabricTransport, options: {
  readonly count: number; readonly timeoutMs?: number; readonly signal?: AbortSignal;
}): Promise<TransportDelivery<TransportEnvelope>[]> {
  if (!Number.isInteger(options.count) || options.count < 0) throw new RangeError("Invalid inbox count");
  if (options.count === 0) return [];
  const timeoutMs = options.timeoutMs ?? 2_000;
  positiveDuration(timeoutMs);
  const controller = new AbortController(), deliveries: TransportDelivery<TransportEnvelope>[] = [];
  const abort = () => controller.abort(options.signal?.reason);
  options.signal?.addEventListener("abort", abort, { once: true });
  const timer = setTimeout(() => controller.abort(new Error(`Inbox timed out: ${deliveries.length}/${options.count} deliveries`)), timeoutMs);
  let iterator: AsyncIterator<TransportDelivery<TransportEnvelope>> | undefined;
  let onAbort: (() => void) | undefined;
  try {
    if (options.signal?.aborted) abort();
    controller.signal.throwIfAborted();
    iterator = transport.inbox(controller.signal)[Symbol.asyncIterator]();
    const cancelled = new Promise<never>((_resolve, reject) => {
      onAbort = () => reject(abortReason(controller.signal));
      controller.signal.addEventListener("abort", onAbort, { once: true });
      if (controller.signal.aborted) onAbort();
    });
    while (deliveries.length < options.count) {
      const next = await Promise.race([iterator.next(), cancelled]);
      if (next.done) throw new Error(`Inbox ended early: ${deliveries.length}/${options.count} deliveries`);
      deliveries.push(next.value);
    }
    return deliveries;
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener("abort", abort);
    if (onAbort) controller.signal.removeEventListener("abort", onAbort);
    controller.abort();
    // A faulty iterator must not make a timeout hang. Observe return rejection without awaiting it;
    // adapter abort/close conformance is tested separately. No background reader is started here.
    if (iterator?.return) void Promise.resolve().then(() => iterator!.return!()).catch(() => undefined);
  }
}
export async function ackAll<T>(deliveries: readonly TransportDelivery<T>[]): Promise<void> {
  // Explicit order, not a cursor/high-water mark: callers can pass [third, first, second].
  for (const delivery of deliveries) await delivery.ack();
}
export function nakAll<T>(deliveries: readonly TransportDelivery<T>[], delayMs?: number): void {
  if (delayMs !== undefined && (!Number.isFinite(delayMs) || delayMs < 0)) throw new RangeError("Invalid nak delay");
  for (const delivery of deliveries) delivery.nak(delayMs);
}
export function workingAll<T>(deliveries: readonly TransportDelivery<T>[]): void {
  for (const delivery of deliveries) delivery.working();
}

export type FaultPoint = "publish:before" | "publish:after" | "ack:before" | "ack:after"
  | "claim:before" | "claim:after" | "custody:before" | "custody:after"
  | "lease:before" | "lease:after" | "lock:acquired";
export interface FaultContext {
  readonly id?: string;
  readonly sequence?: number;
  readonly key?: string;
  readonly scope?: "global" | "owner-local";
}
export type FaultHook = (context: FaultContext) => void | Promise<void>;
/** Register before starting the operation. Hooks may throw, or await an explicit test gate.
 * Claim/custody/lease/lock points MUST be wired to real boundaries by the calling test; wrapping
 * a transport cannot observe those boundaries and proves nothing about their durability.
 */
export class FaultHooks {
  readonly events: Array<{ point: FaultPoint; context: FaultContext }> = [];
  private readonly hooks = new Map<FaultPoint, FaultHook>();
  private readonly spans = new AsyncLocalStorage<{ id: string; globalLocks: number }>();
  set(point: FaultPoint, hook: FaultHook): () => void {
    this.hooks.set(point, hook);
    return () => { if (this.hooks.get(point) === hook) this.hooks.delete(point); };
  }
  private record(point: FaultPoint, context: FaultContext): void {
    this.events.push({ point, context: { ...context } });
    if (point === "lock:acquired" && context.scope === "global") {
      const span = this.spans.getStore();
      if (span) span.globalLocks++;
    }
  }
  /** publish:before/lock hooks must be synchronous: no new await before the carrier's stamp. */
  hitSync(point: FaultPoint, context: FaultContext = {}): void {
    this.record(point, context);
    const result = this.hooks.get(point)?.(context);
    if (result && typeof result.then === "function") {
      void result.catch(() => undefined);
      throw new TypeError(`${point} requires a synchronous fault hook`);
    }
  }
  async hit(point: FaultPoint, context: FaultContext = {}): Promise<void> {
    this.record(point, context);
    await this.hooks.get(point)?.(context);
  }
  async around<T>(boundary: "claim" | "custody" | "lease", context: FaultContext, operation: () => Promise<T>): Promise<T> {
    await this.hit(`${boundary}:before`, context);
    const result = await operation();
    await this.hit(`${boundary}:after`, context);
    return result;
  }
  /** Span must include handler admission AND final broker ack. Instrument the actual MeshLock,
   * scoped to the mesh root; unrelated owner-local claim locks must be labelled owner-local.
   */
  async deliverySpan<T>(id: string, operation: () => Promise<T>): Promise<{ result: T; globalLocks: number }> {
    const span = { id, globalLocks: 0 };
    const result = await this.spans.run(span, operation);
    return { result, globalLocks: span.globalLocks };
  }
}
/** A reusable event gate, not a sleep/poll loop. The test must release it or abort its waiter. */
export function faultGate(): { wait(signal: AbortSignal): Promise<void>; release(): void } {
  let release!: () => void;
  const released = new Promise<void>(resolve => { release = resolve; });
  return { release, wait: async signal => {
    signal.throwIfAborted();
    let onAbort: (() => void) | undefined;
    const cancelled = new Promise<never>((_resolve, reject) => {
      onAbort = () => reject(abortReason(signal));
      signal.addEventListener("abort", onAbort, { once: true });
    });
    try { await Promise.race([released, cancelled]); }
    finally { if (onAbort) signal.removeEventListener("abort", onAbort); }
  } };
}
/** Lost PubAck fault. No resolved-body contents are included in the message. */
export class HarnessUncertainError extends TransportUncertainError {}
export function loseNextPublishAck(hooks: FaultHooks): void {
  const remove = hooks.set("publish:after", () => {
    remove();
    // withFaults captures the adapter's resolved body without adding it to the event log.
    throw new Error("Injected lost publish acknowledgement");
  });
}
/** Does not resolve/restamp bodies, retry, map product errors, fall back, or advance file cursors.
 * An after-publish hook failure is uncertain, never definitely-not-persisted (E1). Native errors
 * from transport.send pass through unchanged. Ack failures likewise leave settlement unknown.
 */
export function withFaults(transport: FabricTransport, hooks: FaultHooks): FabricTransport {
  return {
    kind: transport.kind, self: transport.self,
    send: async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
      hooks.hitSync("publish:before", { id: envelope.id });
      const sent = await transport.send(envelope, signal);
      try { await hooks.hit("publish:after", { id: sent.id, sequence: sent.sequence }); }
      catch { throw new HarnessUncertainError(sent.id, sent.envelope.body); }
      return sent;
    },
    inbox: async function* (signal) {
      for await (const delivery of transport.inbox(signal)) {
        yield {
          payload: delivery.payload, sourceMachine: delivery.sourceMachine, verification: delivery.verification,
          attempt: delivery.attempt, sequence: delivery.sequence,
          working: () => delivery.working(), nak: (delayMs?: number) => delivery.nak(delayMs),
          term: (reason: string) => delivery.term(reason),
          ack: async () => {
            const context = { id: delivery.payload.id, sequence: delivery.sequence };
            await hooks.hit("ack:before", context);
            await delivery.ack();
            await hooks.hit("ack:after", context);
          },
        };
      }
    },
    kv: bucket => transport.kv(bucket), drainState: signal => transport.drainState(signal), close: () => transport.close(),
  };
}

export interface StubKv extends TransportKv {
  /** Drive TTL from the injected clock; no timer or lease/takeover engine is installed. */
  expire(): void;
  /** Inject a watch gap: reset, fresh snapshot, exactly one ready. */
  resetWatches(): void;
}
// Carrier copies preserve JSON wire order; only controlBodyFingerprint canonicalizes policy input.
const jsonCopy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const matchesKvPrefix = (key: string, prefix: string): boolean => {
  const pattern = prefix.split("."), tokens = key.split(".");
  return pattern.every((token, index) => token === ">" ? index < tokens.length
    : token === "*" ? index < tokens.length : token === tokens[index])
    && (pattern.at(-1) === ">" || pattern.length === tokens.length);
};
/** §1 KV only. CAS revisions are bucket-wide; expiry is measured from the last successful put.
 * No directory, lease acquisition, process-exit proof, presence liveness, or authority is inferred.
 * Clock-driven expiry must be explicitly flushed when testing an otherwise idle watch.
 */
export function createStubKv(ttlMs: number, now: () => number = () => FIXTURE_TIME): StubKv {
  positiveDuration(ttlMs);
  let revision = 0;
  const entries = new Map<string, { entry: TransportKvEntry; writtenAt: number }>();
  interface Watcher { prefix: string; events: TransportKvEvent[]; wake?: () => void }
  const watchers = new Set<Watcher>();
  const push = (watcher: Watcher, event: TransportKvEvent) => {
    watcher.events.push(jsonCopy(event)); watcher.wake?.();
  };
  const emit = (event: TransportKvEvent) => {
    const key = event.type === "put" ? event.entry.key : event.type === "gone" ? event.key : undefined;
    for (const watcher of watchers) if (key === undefined || matchesKvPrefix(key, watcher.prefix)) push(watcher, event);
  };
  const expire = () => {
    for (const [key, stored] of entries) if (now() - stored.writtenAt >= ttlMs) {
      entries.delete(key);
      emit({ type: "gone", key, machine: stored.entry.machine, revision: ++revision, reason: "expired" });
    }
  };
  const snapshot = (watcher: Watcher) => {
    for (const [key, stored] of entries) if (matchesKvPrefix(key, watcher.prefix)) push(watcher, { type: "put", entry: stored.entry });
    push(watcher, { type: "ready" });
  };
  const keyMachine = (key: string): string => {
    const tokens = key.split(".");
    if (tokens.length !== 3 || tokens.some(token => !token) || !/^[a-z0-9][a-z0-9-]{0,31}$/.test(tokens[0]!)) {
      throw new TypeError("Fixture KV key must be <machine>.<token>.<token>");
    }
    return tokens[0]!;
  };
  return {
    ttlMs, expire,
    resetWatches: () => { expire(); for (const watcher of watchers) { push(watcher, { type: "reset" }); snapshot(watcher); } },
    get: async <V>(key: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> => {
      signal?.throwIfAborted(); expire();
      const stored = entries.get(key);
      return stored ? jsonCopy(stored.entry) as TransportKvEntry<V> : undefined;
    },
    put: async <V>(key: string, writer: string, value: V, ifRevision: number, signal?: AbortSignal): Promise<number> => {
      signal?.throwIfAborted(); expire();
      const machine = keyMachine(key), previous = entries.get(key);
      if ((previous?.entry.revision ?? 0) !== ifRevision) {
        throw Object.assign(new Error(`KV revision conflict on ${key}`), { code: "FABRIC_KV_CONFLICT", key, expected: ifRevision });
      }
      const copied = jsonCopy(value); // Validate before assigning a revision or changing storage.
      const entry: TransportKvEntry = { key, machine, owner: writer, value: copied, revision: ++revision };
      entries.set(key, { entry, writtenAt: now() }); emit({ type: "put", entry });
      return revision;
    },
    delete: async (key: string, ifRevision: number, signal?: AbortSignal): Promise<boolean> => {
      signal?.throwIfAborted(); expire();
      const previous = entries.get(key);
      if (!previous || previous.entry.revision !== ifRevision) return false;
      entries.delete(key);
      emit({ type: "gone", key, machine: previous.entry.machine, revision: ++revision, reason: "deleted" });
      return true;
    },
    watch: async function* <V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
      if (signal.aborted) return;
      expire();
      const watcher: Watcher = { prefix, events: [] };
      const wake = () => watcher.wake?.();
      watchers.add(watcher); snapshot(watcher);
      signal.addEventListener("abort", wake, { once: true });
      try {
        while (!signal.aborted) {
          const event = watcher.events.shift();
          if (event) { yield event as TransportKvEvent<V>; continue; }
          await new Promise<void>(resolve => {
            watcher.wake = resolve;
            if (signal.aborted || watcher.events.length) resolve();
          });
          delete watcher.wake;
        }
      } finally { watchers.delete(watcher); signal.removeEventListener("abort", wake); }
    },
  };
}

export interface StubDeliveryRecord {
  readonly delivery: TransportDelivery<TransportEnvelope>;
  working: number;
  acks: number;
  readonly naks: Array<number | undefined>;
  readonly terms: string[];
  inHand: boolean;
  settled: boolean;
}
export interface StubTransport extends FabricTransport {
  readonly kind: "nats";
  readonly records: readonly StubDeliveryRecord[];
  /** Explicit scripted redelivery/fabricated provenance for negative tests, NOT authentication. */
  enqueue(payload: TransportEnvelope, metadata?: {
    sourceMachine?: string; verification?: "mesh" | "bridge"; attempt?: number; sequence?: number;
  }): StubDeliveryRecord;
}
/** Model-free §1 carrier + KV ONLY, never a control plane/admission/claim engine.
 * Broker-window id dedupe is carrier behavior, not application idempotency. No automatic
 * redelivery, lease takeover, fsync, ACL or B2–B5 receipt semantics are claimed. working/nak
 * are recorded only; replay explicitly with enqueue(). Real NATS gates require a real server.
 * File-mode conformance MUST use the actual file adapter, not this NATS-shaped stub.
 */
export function createStubBus(now: () => number = () => FIXTURE_TIME): {
  open(self: TransportOwner): StubTransport; expireKv(): void; resetKvWatches(): void;
} {
  interface InboxState {
    records: StubDeliveryRecord[]; queue: StubDeliveryRecord[]; wake: Set<() => void>; sequence: number;
    ids: Map<string, { sequence: number; at: number }>;
  }
  const states = new Map<string, InboxState>();
  const buckets = { presence: createStubKv(contractTiming.presenceTtlMs, now), leases: createStubKv(contractTiming.leaseTtlMs, now) };
  const key = (self: TransportOwner) => JSON.stringify([self.machine, self.hostId]);
  const stateFor = (self: TransportOwner): InboxState => {
    const id = key(self);
    let state = states.get(id);
    if (!state) { state = { records: [], queue: [], wake: new Set(), sequence: 0, ids: new Map() }; states.set(id, state); }
    return state;
  };
  const enqueue = (state: InboxState, payload: TransportEnvelope, sourceMachine: string,
    verification: "mesh" | "bridge", attempt: number, sequence?: number): StubDeliveryRecord => {
    const seq = sequence ?? state.sequence + 1;
    state.sequence = Math.max(state.sequence, seq);
    const record: StubDeliveryRecord = {
      delivery: { payload, sourceMachine, verification, attempt, sequence: seq,
        working: () => { record.working++; }, ack: async () => { record.acks++; record.settled = true; },
        nak: delay => { record.naks.push(delay); }, term: async reason => { record.terms.push(reason); record.settled = true; } },
      working: 0, acks: 0, naks: [], terms: [], inHand: false, settled: false,
    };
    state.records.push(record); state.queue.push(record);
    for (const wake of state.wake) wake();
    return record;
  };
  return {
    expireKv: () => { buckets.presence.expire(); buckets.leases.expire(); },
    resetKvWatches: () => { buckets.presence.resetWatches(); buckets.leases.resetWatches(); },
    open: self => {
    const state = stateFor(self);
    let closed = false, iterated = false;
    const closing = new AbortController();
    const ownKey = (key: string) => {
      if (key.split(".")[0] !== self.machine) throw new TransportDeniedError("KV write outside credential-bound machine");
    };
    return {
      kind: "nats", self, records: state.records,
      enqueue: (payload, metadata = {}) => enqueue(state, payload, metadata.sourceMachine ?? self.machine,
        metadata.verification ?? "mesh", metadata.attempt ?? 1, metadata.sequence),
      send: async <T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> => {
        signal?.throwIfAborted();
        if (closed) throw new TransportUnavailableError("Stub transport is closed");
        // No await between stamp and storage; duplicate sends still evaluate THIS attempt's stamp.
        const committedAt = now(), resolved = resolveEnvelope(envelope, committedAt);
        const payload = jsonCopy(resolved), destination = stateFor(envelope.to);
        const previous = destination.ids.get(envelope.id);
        if (previous && committedAt - previous.at < contractTiming.duplicateWindowMs) {
          return { id: envelope.id, sequence: previous.sequence, duplicate: true, envelope: resolved };
        }
        const record = enqueue(destination, payload, self.machine, self.machine === envelope.to.machine ? "mesh" : "bridge", 1);
        destination.ids.set(envelope.id, { sequence: record.delivery.sequence, at: committedAt });
        return { id: envelope.id, sequence: record.delivery.sequence, duplicate: false, envelope: resolved };
      },
      inbox: async function* (signal) {
        if (iterated) throw new Error("Stub inbox permits only one iterator per process");
        iterated = true;
        const wake = () => { for (const listener of state.wake) listener(); };
        signal.addEventListener("abort", wake, { once: true });
        try {
          while (!closed && !signal.aborted) {
            const record = state.queue.shift();
            if (record) { record.inHand = true; yield record.delivery; continue; }
            await new Promise<void>(resolve => {
              const done = () => { state.wake.delete(done); resolve(); };
              state.wake.add(done);
              if (closed || signal.aborted || state.queue.length) done();
            });
          }
        } finally { signal.removeEventListener("abort", wake); }
      },
      kv: bucket => {
        const kv = buckets[bucket];
        return {
          ttlMs: kv.ttlMs, get: kv.get,
          put: async (key, writer, value, revision, signal) => { ownKey(key); return kv.put(key, writer, value, revision, signal); },
          delete: async (key, revision, signal) => { ownKey(key); return kv.delete(key, revision, signal); },
          watch: (prefix, signal) => kv.watch(prefix, AbortSignal.any([signal, closing.signal])),
        };
      },
      drainState: async signal => {
        signal?.throwIfAborted();
        return { pending: state.queue.filter(record => !record.settled).length,
          ackPending: state.records.filter(record => record.inHand && !record.settled).length };
      },
      close: async () => { closed = true; closing.abort(); for (const wake of state.wake) wake(); },
    };
  } };
}

export type AdapterKind = "stub" | "file" | "nats";
export interface AdapterSpec {
  readonly kind: AdapterKind;
  /** Caller supplies the real §1 options; no implicit config/root/env mutations. */
  readonly options?: FabricTransportOptions;
  readonly factory?: FabricTransportFactory;
  readonly stub?: StubTransport;
  readonly self?: TransportOwner;
  /** Export names are not frozen by §1. Override these when A1/B choose their entry points. */
  readonly moduleUrl?: string | URL;
  readonly exportName?: string;
  /** Explicit opt-in: real NATS with per-machine credentials, never the state-server/admin helper. */
  readonly natsReady?: boolean;
  readonly required?: boolean;
  readonly skipReason?: string;
}
export type AdapterResult =
  | { readonly status: "ready"; readonly kind: AdapterKind; readonly transport: FabricTransport }
  | { readonly status: "skip"; readonly kind: AdapterKind; readonly reason: string };
export const adapterKinds = ["stub", "file", "nats"] as const;

/** Source runners resolve .js imports to .ts; native built runners use the .js file.
 * Check only the requested entry, not error-message substrings that could hide broken dependencies.
 * Filesystem access is read-only, deferred until explicit fixture loading, and has no setup effects.
 */
async function resolveEntry(entry: URL): Promise<URL | undefined> {
  if (entry.protocol !== "file:") throw new TypeError("Fixture moduleUrl must resolve to a file URL");
  const { existsSync } = await import("node:fs");
  if (existsSync(entry)) return entry;
  const source = new URL(entry.href.replace(/\.js$/, ".ts"));
  return entry.pathname.endsWith(".js") && existsSync(source) ? source : undefined;
}
export async function openAdapter(spec: AdapterSpec): Promise<AdapterResult> {
  const skip = (reason: string): AdapterResult => {
    if (spec.required) throw new Error(`${spec.kind} transport required: ${reason}`);
    return { status: "skip", kind: spec.kind, reason };
  };
  if (spec.skipReason) return skip(spec.skipReason);
  if (spec.kind === "stub") {
    return { status: "ready", kind: "stub", transport: spec.stub ?? createStubBus().open(spec.self ?? owner()) };
  }
  // Only import on actual test use. Do not reuse nats-state-server.ts: that helper eagerly changes
  // a private config and provisions a DIFFERENT state-backend server, not this transport cohort.
  const entry = new URL(spec.moduleUrl ?? "../../src/transport/index.js", import.meta.url);
  let factory = spec.factory;
  if (!factory) {
    const resolved = await resolveEntry(entry);
    if (!resolved) return skip(`optional A1/B transport entry is not present: ${entry.href}`);
    // Once an entry exists, ALL import failures are failures, including missing transitive deps.
    const module = await import(resolved.href) as Record<string, unknown>;
    const name = spec.exportName ?? "openFabricTransport";
    if (typeof module[name] !== "function") throw new TypeError(`${entry} must export ${name}; set exportName/factory explicitly`);
    factory = module[name] as FabricTransportFactory;
  }
  if (spec.kind === "nats" && !spec.natsReady) {
    return skip("real NATS transport fixture not enabled: supply per-machine runtime credentials and natsReady: true");
  }
  if (!spec.options) throw new TypeError(`${spec.kind} transport requires caller-owned FabricTransportOptions`);
  const transport = await factory(spec.options);
  if (transport.kind !== spec.kind) {
    await transport.close();
    throw new Error(`Factory selected ${transport.kind}, expected ${spec.kind}; configure the requested carrier explicitly`);
  }
  return { status: "ready", kind: spec.kind, transport };
}
/** Runner-neutral parameterization. Callers register an actual skipped test with result.reason;
 * do not return early from a passing test on status=skip. Always close ready adapters in finally.
 * File cases have no NATS prerequisite and use the unchanged real factory/options path.
 */
export async function loadAdapterCases(specs: readonly AdapterSpec[]): Promise<AdapterResult[]> {
  const results: AdapterResult[] = [];
  try {
    for (const spec of specs) results.push(await openAdapter(spec));
    return results;
  } catch (error) {
    await Promise.allSettled(results.flatMap(result => result.status === "ready" ? [result.transport.close()] : []));
    throw error;
  }
}

/** Scheduling/administrative probes only. All watch events come from B's NatsKvStore and
 * the installed official ordered consumer, authenticated as the SAME machine as receiver.
 * This is not an adapter override, a second watch engine, or a marker-after-data hold hook.
 */
export interface RealKvConsumptionProbe {
  readonly store: TransportKv;
  readonly created: Promise<void>;
  release(): Promise<void>;
  expiry(key: string, revision: number, signal: AbortSignal): Promise<number>;
  close(): Promise<void>;
}
export interface RealKvReadiness {
  reset(bucket: TransportBucket, prefix: string, signal: AbortSignal): Promise<void>;
  holdConsumption(bucket: TransportBucket, prefix: string, signal: AbortSignal): Promise<RealKvConsumptionProbe>;
}
/** Actual factory carriers, retaining concrete FILE identity. Each NATS pair owns a fresh
 * Path B server/keys/store. close() is always the actual port operation, without a
 * pre-close iterator-return workaround for the superseded raw-consumer yield deadlock.
 */
export interface RealAdapterPair {
  readonly sender: FabricTransport;
  readonly receiver: FabricTransport;
  readonly kvReadiness?: RealKvReadiness;
  reopenReceiver(): Promise<FabricTransport>;
  close(): Promise<void>;
}
export type RealAdapterPairResult =
  | { readonly status: "ready"; readonly pair: RealAdapterPair }
  | { readonly status: "skip"; readonly reason: string };

export async function openRealAdapterPair(kind: "file" | "nats", required = false): Promise<RealAdapterPairResult> {
  const unavailable = (reason: string): RealAdapterPairResult => {
    if (required) throw new Error(`${kind} transport required: ${reason}`);
    return { status: "skip", reason };
  };
  // Import B's fixture only on actual NATS use; no state-backend/admin carrier is involved.
  const natsModule = kind === "nats" ? await import("./nats-cluster.js") : undefined;
  const binary = natsModule?.resolveNatsBinary();
  if (kind === "nats" && !binary) {
    if (process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED: Path B real-server prerequisite missing: set absolute NATS_BIN");
    return unavailable("Path B real-server prerequisite missing: set absolute NATS_BIN");
  }
  const [{ mkdtempSync, rmSync }, { tmpdir }, { join }, { DEFAULT_FABRIC_CONFIG }, { MeshStore }] = await Promise.all([
    import("node:fs"), import("node:os"), import("node:path"), import("../../src/config.js"), import("../../src/mesh/store.js"),
  ]);
  const root = mkdtempSync(join(tmpdir(), "fabric-transport-conformance-"));
  const mesh = new MeshStore(root, DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents,
    { stateBackend: "file", canWakeResidents: () => false });
  let server: import("./nats-cluster.js").PathBHarness | undefined;
  const opened = new Set<FabricTransport>();
  const probes = new Set<RealKvConsumptionProbe>();
  let cleaning: Promise<void> | undefined;
  const cleanup = (): Promise<void> => cleaning ??= (async () => {
    const errors: unknown[] = [];
    const settle = async (operations: Promise<unknown>[]) => {
      for (const result of await Promise.allSettled(operations)) if (result.status === "rejected") errors.push(result.reason);
    };
    try {
      // B's callback consumer must close independently of public yields. Do not return
      // inboxes here to make close succeed; T-CLOSE-SUSPENDED checks that contract directly.
      await settle([...opened].map(port => boundedReceipt(port.close(), "close actual fixture transport")));
      await settle([...probes].map(probe => boundedReceipt(probe.close(), "close KV scheduling probe")));
    } finally {
      // Even a broken return()/close() cannot strand this fixture's real child at Vitest's deadline.
      try { await server?.stop(); }
      finally { mesh.closeState(); rmSync(root, { recursive: true, force: true }); }
    }
    if (errors.length) throw new AggregateError(errors, "real transport fixture cleanup failed");
  })();
  try {
    if (kind === "nats") {
      server = await natsModule!.createPathBHarness({ binary: binary! });
      const [{ jetstream, jetstreamManager }, { ensureTransportStream }, { openNatsKvForMachine }, subjects] = await Promise.all([
        import("@nats-io/jetstream"), import("../../src/transport/nats/provision.js"),
        import("../../src/transport/nats/kv.js"), import("../../src/transport/nats/subjects.js"),
      ]);
      // This bootstrap connection only provisions; close it before opening either runtime
      // carrier. The reset probe below uses a SEPARATE admin connection solely for its fault.
      const bootstrap = await server.connect(server.users.bootstrap);
      try {
        const manager = await jetstreamManager(bootstrap), js = jetstream(bootstrap);
        const policy = { maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576,
          duplicateWindowMs: contractTiming.duplicateWindowMs, replicas: 1 as const };
        await ensureTransportStream(manager, { ...policy, name: subjects.ADVISORY_CAPTURE_STREAM,
          subjects: [subjects.transportAdvisoryStreamSubject], maxAgeMs: 30 * 86_400_000, maxMessageBytes: 65_536 });
        await openNatsKvForMachine(js, "leases", "machine-a", true);
        await openNatsKvForMachine(js, "presence", "machine-a", true);
        for (const machine of ["machine-a", "machine-b"]) {
          await ensureTransportStream(manager, { ...policy, name: subjects.transportInboxStream(machine),
            subjects: [subjects.transportInboxStreamSubject(machine)] });
        }
      } finally { await bootstrap.close(); }
    }
    const open = async (side: "sender" | "receiver"): Promise<FabricTransport> => {
      const user = side === "sender" ? server?.users.machineA : server?.users.machineB;
      const config: FabricTransportOptions["config"] = { ...DEFAULT_FABRIC_CONFIG.mesh, transport: kind,
        ...(server && user ? { nats: { ...DEFAULT_FABRIC_CONFIG.mesh.nats, servers: [server.serverUrl],
          machine: user.machine, credentials: user.credsPath, operatorMachine: "fabric-ops" } } : {})
      };
      const result = await openAdapter({ kind, options: { config, mesh, hostId: `session:${side}` },
        exportName: "openFabricTransport", natsReady: kind === "nats", required });
      if (result.status === "skip") throw new Error(result.reason);
      opened.add(result.transport);
      return result.transport;
    };
    const sender = await open("sender");
    let receiver = await open("receiver");
    const kvReadiness: RealKvReadiness | undefined = server ? {
      reset: async (bucket, prefix, signal) => {
        signal.throwIfAborted();
        const admin = await server!.connect(server!.users.bootstrap);
        try {
          const { jetstreamManager } = await import("@nats-io/jetstream");
          const manager = await jetstreamManager(admin), stream = `KV_fabric_${bucket}`;
          const subject = `$KV.fabric_${bucket}.${prefix}`;
          const active = [];
          // One bounded LIST, not consumer-info polling. Delete ONLY the exact live watch.
          for await (const info of manager.consumers.list(stream)) {
            const filters = info.config.filter_subjects ?? [info.config.filter_subject];
            if (info.push_bound && info.config.deliver_policy === "last_per_subject" && filters.includes(subject)) active.push(info);
          }
          if (active.length !== 1) throw new Error(`expected one active ordered ${bucket} watch, found ${active.length}`);
          signal.throwIfAborted();
          const deleted = await signalReceipt(manager.consumers.delete(stream, active[0]!.name), signal);
          if (!deleted) throw new Error("real ordered watch consumer was not deleted");
        } finally { await admin.close(); }
      },
      holdConsumption: async (bucket, prefix, signal) => {
        signal.throwIfAborted();
        const [{ openNatsConnection }, { openNatsKvForMachine }, { DeliverPolicy }] = await Promise.all([
          import("../../src/transport/nats/connection.js"), import("../../src/transport/nats/kv.js"), import("@nats-io/jetstream"),
        ]);
        const user = server!.users.machineB;
        const connection = await openNatsConnection({ servers: server!.serverUrl, credentials: user.credsPath, machine: user.machine });
        const gate = faultGate();
        let created!: () => void;
        const creation = new Promise<void>(resolve => { created = resolve; });
        const observers = new Set<import("@nats-io/jetstream").ConsumerMessages>();
        const consumers = connection.js.consumers;
        const getPushConsumer: typeof consumers.getPushConsumer = async (...args) => {
          if (args[0] !== `KV_fabric_${bucket}` || !args[1] || typeof args[1] !== "object" ||
            !("filter_subjects" in args[1]) || args[1].filter_subjects !== `$KV.fabric_${bucket}.${prefix}`) {
            throw new Error("consume hold must address only its requested real ordered watch");
          }
          const consumer = await consumers.getPushConsumer(...args);
          // Same seam as B's real KV suite: hold ONLY consume(), not metadata, callbacks,
          // statuses, pending counts or event order. No client-side event buffer is installed.
          return {
            info: cached => consumer.info(cached), delete: () => consumer.delete(),
            isPullConsumer: () => consumer.isPullConsumer(), isPushConsumer: () => consumer.isPushConsumer(),
            consume: async options => {
              created(); await gate.wait(signal); signal.throwIfAborted();
              return consumer.consume(options);
            },
          };
        };
        const client = Object.create(connection.js) as typeof connection.js;
        Object.defineProperty(client, "consumers", { value: { getPushConsumer } });
        let closingProbe: Promise<void> | undefined;
        try {
          const store = await openNatsKvForMachine(client, bucket, user.machine, false, connection.onStatus);
          const probe: RealKvConsumptionProbe = {
            store, created: creation, release: async () => { gate.release(); },
            expiry: async (key, revision, expirySignal) => {
              if (!key.startsWith(`${user.machine}.`)) throw new Error("expiry probe requires own-machine key");
              expirySignal.throwIfAborted();
              let expired!: (revision: number) => void;
              const marker = new Promise<number>(resolve => { expired = resolve; });
              const consumer = await consumers.getPushConsumer(`KV_fabric_${bucket}`, {
                filter_subjects: `$KV.fabric_${bucket}.${key}`, deliver_policy: DeliverPolicy.LastPerSubject,
              });
              const messages = await consumer.consume({ callback: msg => {
                if (msg.headers?.get("Nats-Marker-Reason") === "MaxAge" && msg.info.streamSequence > revision) expired(msg.info.streamSequence);
              } });
              observers.add(messages);
              try { return await signalReceipt(marker, expirySignal); }
              finally { await messages.close(); observers.delete(messages); }
            },
            close: () => closingProbe ??= (async () => {
              gate.release();
              try { await Promise.all([...observers].map(observer => observer.close())); }
              finally {
                try { await boundedReceipt(connection.close(), "drain real KV probe connection"); }
                finally { await connection.nc.close(); }
              }
            })(),
          };
          probes.add(probe);
          return probe;
        } catch (error) { gate.release(); await connection.close(); throw error; }
      },
    } : undefined;
    return { status: "ready", pair: { sender, get receiver() { return receiver; },
      ...(kvReadiness ? { kvReadiness } : {}),
      reopenReceiver: async () => { await boundedReceipt(receiver.close(), "close receiver before durable reopen"); receiver = await open("receiver"); return receiver; },
      close: cleanup } };
  } catch (error) { await cleanup(); throw error; }
}

export type ControlPlaneDependency =
  | { readonly status: "available"; readonly module: Pick<typeof import("../../src/topology/control-plane.js"), "FabricControlPlane"> }
  | { readonly status: "unavailable"; readonly dependency: "A1-control-plane"; readonly reason: string };
/** T4/T5/T17/T18 instantiate the REAL class. The getter is only a structural check:
 * callers supplying a carrier must also probe the actual constructor/start boundary. This runs
 * on the integrated A1 dispatcher without an environment opt-in; an older pinned A1 may refuse
 * the carrier. Only its typed unsupported refusal becomes missing coverage; all other failures stay failures.
 */
export async function loadControlPlane(options: {
  readonly transportIntegrated?: boolean; readonly required?: boolean;
  readonly probe?: (Class: typeof import("../../src/topology/control-plane.js").FabricControlPlane) => void | Promise<void>;
} = {}): Promise<ControlPlaneDependency> {
  const module = await import("../../src/topology/control-plane.js");
  if (typeof module.FabricControlPlane !== "function") throw new TypeError("Actual FabricControlPlane export is missing");
  const integrated = typeof Object.getOwnPropertyDescriptor(module.FabricControlPlane.prototype, "transportKind")?.get === "function";
  if (!integrated) {
    const reason = "A1 FabricControlPlane transport integration unavailable: public transportKind getter is absent. "
      + "T4/T5/T17/T18 and fault scenarios require the actual FabricControlPlane, not stub admission logic.";
    if (options.required || options.transportIntegrated) throw new Error(reason);
    return { status: "unavailable", dependency: "A1-control-plane", reason };
  }
  try { await options.probe?.(module.FabricControlPlane); }
  catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "FABRIC_TRANSPORT_UNSUPPORTED") throw error;
    const reason = `A1 real carrier dispatch unavailable: ${error.message}`;
    if (options.required || options.transportIntegrated) throw new Error(reason, { cause: error });
    return { status: "unavailable", dependency: "A1-control-plane", reason };
  }
  return { status: "available", module: { FabricControlPlane: module.FabricControlPlane } };
}
