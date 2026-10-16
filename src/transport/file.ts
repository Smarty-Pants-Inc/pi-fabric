import { randomUUID } from "node:crypto";
import type { MeshEvent, MeshPublishInput } from "../mesh/event-log.js";
import type { MeshStore } from "../mesh/store.js";
import { watchBridgeStore } from "../mesh/change-notifier.js";
import {
  TransportUncertainError, TransportUnavailableError, TransportUnsupportedError,
  type FabricTransport, type FabricTransportOptions, type TransportBucket,
  type TransportDelivery, type TransportDrainState, type TransportEnvelope,
  type TransportKv, type TransportOwner, type TransportSent,
} from "./types.js";

const CONTROL_TOPIC = "fabric.control.command";
const ACK_TOPIC = "fabric.control.ack";
const isControl = (event: MeshEvent, hostId: string): boolean =>
  event.to === hostId && (event.topic === CONTROL_TOPIC || event.topic === ACK_TOPIC);

/** The legacy page reader, including its deliberately different pre/post-handler gates.
 * A failed handler never commits its sequence or the page offset. Detached ASK admission
 * may commit its sequence: its outcome remains owned by the control plane, as before.
 * Watches, retained-outcome retries and release checkpoints still belong to that plane. */
export class FileControlReader {
  #offset = 0;
  #lastSequence = 0;
  #inHand = new Set<number>();
  constructor(readonly mesh: MeshStore, readonly hostId: string) {}
  get lastSequence(): number { return this.#lastSequence; }

  async drain(
    accept: (event: MeshEvent) => Promise<void> | void,
    paused: () => boolean,
    canConsume?: () => boolean,
    onConsumed?: (event: MeshEvent) => void,
    beforePage?: () => boolean,
    beforeEvent?: (event: MeshEvent) => boolean,
  ): Promise<boolean> {
    while (canConsume?.() !== false) {
      if (beforePage?.() === false) return false;
      const tail = this.mesh.tail(this.#offset, 100);
      for (const event of tail.events) {
        if (paused() || canConsume?.() === false) return false;
        if (event.sequence <= this.#lastSequence) continue;
        if (isControl(event, this.hostId)) {
          if (beforeEvent?.(event) === false) return false;
          this.#inHand.add(event.sequence);
          try {
            const handling = accept(event);
            // Legacy ACK handling is synchronous. Do not introduce an await between
            // ACK settlement and its cursor commit; commands still return promises.
            if (handling !== undefined) await handling;
          } finally { this.#inHand.delete(event.sequence); }
        }
        if (canConsume?.() === false) return false;
        this.#lastSequence = event.sequence;
        onConsumed?.(event);
      }
      this.#offset = tail.nextOffset;
      if (tail.events.length < 100) break;
    }
    return !paused() && canConsume?.() !== false;
  }

  drainState(): TransportDrainState {
    let offset = this.#offset, pending = 0;
    for (;;) {
      const tail = this.mesh.tail(offset, 100);
      for (const event of tail.events) {
        if (event.sequence > this.#lastSequence && isControl(event, this.hostId) && !this.#inHand.has(event.sequence)) pending++;
      }
      if (tail.events.length < 100 || tail.nextOffset === offset) break;
      offset = tail.nextOffset;
    }
    return { pending, ackPending: this.#inHand.size };
  }
}

/** File carrier. No KV emulation, no broker-style event dedupe, no new durable cursor.
 * The control plane uses reader.drain directly so its existing watch/retry/pause/release
 * lifecycle (including detached outcomes behind the cursor) is identical when injected. */
export class FileTransport implements FabricTransport {
  readonly kind = "file" as const;
  readonly reader: FileControlReader;
  readonly #publishes = new Set<Promise<unknown>>();
  readonly #closeController = new AbortController();
  #iterating = false;
  #readerTask: Promise<void> | undefined;
  #closed = false;
  constructor(readonly mesh: MeshStore, readonly self: TransportOwner) {
    this.reader = new FileControlReader(mesh, self.hostId);
  }

  /** Legacy-envelope entry: preserve native ids, stamps, fences and completion errors.
   * The control plane must observe publication uncertainty even after a positive append. */
  publish(input: MeshPublishInput): Promise<MeshEvent> {
    if (this.#closed) return Promise.reject(new TransportUnavailableError("File transport closed"));
    return this.#trackPublish(this.mesh.publish(input));
  }

  #trackPublish(operation: Promise<MeshEvent>): Promise<MeshEvent> {
    this.#publishes.add(operation);
    void operation.finally(() => this.#publishes.delete(operation)).catch(() => undefined);
    return operation;
  }

  /** Generic E1 send reconciliation: keep positive append evidence despite completion
   * failures; only an unchanged complete log boundary proves a pre-append failure. */
  async #publish(input: MeshPublishInput): Promise<MeshEvent> {
    if (this.#closed) throw new TransportUnavailableError("File transport closed");
    const id = input.id ?? randomUUID();
    let commitEntered = false, preAppendFailure = false, resolvedBody: unknown = input.data;
    let appended: MeshEvent | undefined;
    let before: ReturnType<MeshStore["latestCursor"]> | undefined;
    let after = 0;
    const original = input.data;
    // Do not turn fixed data into a stamp: native fixed data is snapshotted/encoded
    // BEFORE the lock, whereas native stamps must run UNDER it.
    const data = typeof original !== "function" ? original : (at: number) => {
      try {
        const value: unknown = (original as (at: number) => unknown)(at);
        resolvedBody = value === undefined ? undefined : JSON.parse(JSON.stringify(value)) as unknown;
        return resolvedBody;
      } catch (error) { preAppendFailure = true; throw error; }
    };
    try {
      return await this.mesh.publish({ ...input, id, data,
        ...(input.admit ? { admit: () => {
          try { input.admit!(); } catch (error) { preAppendFailure = true; throw error; }
        } } : {}),
        fence: commit => {
          const run = () => {
            // Same native lock, before append. Never use latestSequence as a cursor:
            // it includes reservations; here it only bounds positive id readback.
            before = this.mesh.latestCursor();
            after = this.mesh.latestSequence();
            commitEntered = true;
            const result = commit();
            // EventLog's fenced append returns {event, line, bytes}. A positive
            // receipt survives later compaction/wake/barrier throws. If a different
            // store cannot expose it, the fallback below stays conservative.
            if (typeof result === "object" && result !== null && "event" in result) {
              const event = result.event as MeshEvent | undefined;
              if (event?.id === id && Number.isSafeInteger(event.sequence)) appended = event;
            }
            return result;
          };
          return input.fence ? input.fence(run) : run();
        },
      });
    } catch (error) {
      if (appended) return appended;
      // Lock/admission/abort/stamp failures precede append. Preserve their exact typed
      // error (and abort reason), rather than disguising a commit-predicate refusal.
      if (!commitEntered || preAppendFailure) throw error;
      let definitelyNotAppended = false;
      try {
        let sequence = after;
        // Snapshot the readback horizon so concurrent writers cannot make this an
        // unbounded scan. A changed/compacted log cannot supply negative evidence.
        const horizon = this.mesh.latestCursor();
        const through = horizon.last?.sequence ?? sequence;
        for (;;) {
          const page = this.mesh.read({ after: sequence, limit: this.mesh.maxReadEvents });
          const committed = page.find(event => event.id === id);
          if (committed) return committed;
          const last = page.at(-1)?.sequence;
          if (page.length < this.mesh.maxReadEvents || last === undefined || last <= sequence || last >= through) break;
          sequence = last;
        }
        // A complete unchanged boundary from the same log generation proves that
        // size validation/serialization/index failures appended no complete event.
        // Do not use a counter reservation or a negative compacted-log lookup.
        const confirmed = this.mesh.latestCursor();
        const sameLast = (left: typeof before, right: typeof before): boolean =>
          left?.last?.sequence === right?.last?.sequence && left?.last?.id === right?.last?.id;
        definitelyNotAppended = before !== undefined && before.cursor === horizon.cursor && horizon.cursor === confirmed.cursor &&
          sameLast(before, horizon) && sameLast(horizon, confirmed);
      } catch { /* A failed readback cannot prove that nothing appended. */ }
      if (definitelyNotAppended) throw error;
      throw new TransportUncertainError(id, resolvedBody, { cause: error });
    }
  }

  async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    signal?.throwIfAborted();
    const body = typeof envelope.body === "function" || envelope.body === undefined
      ? envelope.body : JSON.parse(JSON.stringify(envelope.body)) as T;
    const captured = { ...envelope, to: { ...envelope.to }, from: { ...envelope.from }, body };
    // The port id is logical. File retries append a fresh native event; application
    // control-seen dedupes the stable commandId, not MeshEvent.id (E1).
    let event: MeshEvent;
    try {
      event = await this.#trackPublish(this.#publish({
        id: randomUUID(), topic: captured.kind === "command" ? CONTROL_TOPIC : ACK_TOPIC,
        kind: captured.operation, to: captured.to.hostId, from: captured.from,
        principal: captured.principal, ...(captured.sender ? { sender: captured.sender } : {}),
        data: captured.body, signal,
      }));
    } catch (error) {
      // A port caller retries its logical id, never the private native append id.
      if (error instanceof TransportUncertainError) {
        throw new TransportUncertainError(captured.id, error.resolvedBody, { cause: error });
      }
      throw error;
    }
    return { id: captured.id, sequence: event.sequence, duplicate: false,
      envelope: { ...captured, from: event.from, principal: event.principal,
        body: event.data as T, createdAt: event.createdAt,
        ...(event.sender ? { sender: event.sender } : {}) } };
  }

  async *inbox(signal: AbortSignal): AsyncIterable<TransportDelivery<TransportEnvelope>> {
    if (this.#iterating) throw new TransportUnavailableError("File inbox already has a reader");
    if (this.#closed || signal.aborted) return;
    this.#iterating = true;
    let localStop = false, dirty = true, done = false;
    let wake: (() => void) | undefined;
    let delayWake: (() => void) | undefined;
    let offer: TransportDelivery<TransportEnvelope> | undefined;
    let available: (() => void) | undefined;
    let receipt: (() => void) | undefined;
    let confirmation: { sequence: number; resolve: () => void; reject: (error: Error) => void } | undefined;
    let retry = false, retryMs = 100;
    let failure: unknown;
    const stopped = () => localStop || signal.aborted || this.#closeController.signal.aborted;
    const notify = () => { dirty = true; wake?.(); wake = undefined; };
    const abort = () => {
      notify(); delayWake?.(); available?.(); receipt?.();
      confirmation?.reject(new TransportUnavailableError("File inbox stopped before cursor confirmation"));
      confirmation = undefined;
    };
    signal.addEventListener("abort", abort, { once: true });
    this.#closeController.signal.addEventListener("abort", abort, { once: true });
    let unwatch: (() => void) | undefined;
    const armWatch = () => {
      if (unwatch || stopped()) return;
      try {
        unwatch = watchBridgeStore(this.mesh.root, notify, () => { unwatch?.(); unwatch = undefined; notify(); });
      } catch { /* Failed watches alone get a one-shot degraded retry below. */ }
    };
    const RETRY = Symbol("file inbox retry");
    const pumping = (async () => {
      try {
        while (!stopped()) {
          armWatch();
          if (!dirty) await new Promise<void>(resolve => {
            let timer: NodeJS.Timeout | undefined;
            const finish = () => { clearTimeout(timer); wake = undefined; resolve(); };
            wake = finish;
            if (!unwatch) { timer = setTimeout(finish, 1_000); timer.unref(); }
            if (dirty || stopped()) finish();
          });
          if (stopped()) break;
          dirty = false;
          try {
            await this.reader.drain(async event => {
              retry = false; retryMs = 100;
              const body = event.data;
              const targetId = body && typeof body === "object" && "targetId" in body && typeof body.targetId === "string" ? body.targetId : "";
              let settle!: () => void;
              let settled = false;
              const accepted = new Promise<void>(resolve => { settle = resolve; });
              // A delivery owns its own settlement, never the next delivery's closure.
              const finish = () => { if (!settled) { settled = true; settle(); } };
              receipt = finish;
              const confirmed = new Promise<void>((resolve, reject) => { confirmation = { sequence: event.sequence, resolve, reject }; });
              void confirmed.catch(() => undefined);
              const acknowledge = async () => {
                if (stopped()) throw new TransportUnavailableError("File inbox stopped");
                finish();
                await confirmed;
              };
              const commandId = body && typeof body === "object" && "commandId" in body && typeof body.commandId === "string" ? body.commandId : undefined;
              const logicalId = commandId === undefined ? event.id : event.topic === ACK_TOPIC ? `ack:${commandId}` : commandId;
              offer = {
                payload: { id: logicalId, kind: event.topic === ACK_TOPIC ? "ack" : "command",
                  to: this.self, targetId, operation: event.kind, from: event.from,
                  // The native callback reader retains absent legacy verification.
                  // Never lend principal authority to such a generic-port envelope.
                  ...(event.verification && event.principal ? { principal: event.principal } : {}),
                  ...(event.sender ? { sender: event.sender } : {}), body },
                sourceMachine: this.self.machine, verification: event.verification ?? "mesh",
                attempt: 1, sequence: event.sequence, working() {},
                ack: acknowledge,
                nak: delayMs => {
                  if (!stopped() && !settled) {
                    retry = true;
                    retryMs = Number.isFinite(delayMs) ? Math.max(0, Math.min(2_147_483_647, delayMs!)) : 100;
                    finish();
                  }
                },
                term: async reason => {
                  if (!stopped() && !settled) console.warn(`[pi-fabric] file control delivery terminated: ${reason}`);
                  await acknowledge();
                },
              };
              available?.(); available = undefined;
              await accepted;
              receipt = undefined;
              if (retry || stopped()) {
                confirmation?.reject(new TransportUnavailableError("File delivery retained before cursor commit"));
                confirmation = undefined;
                throw RETRY;
              }
            }, stopped, () => !stopped(), event => {
              if (confirmation?.sequence === event.sequence) { confirmation.resolve(); confirmation = undefined; }
            });
          } catch (error) {
            if (error !== RETRY) throw error;
            if (!stopped()) {
              await new Promise<void>(resolve => {
                const finish = () => { clearTimeout(timer); delayWake = undefined; resolve(); };
                const timer = setTimeout(finish, retryMs); timer.unref(); delayWake = finish;
                if (stopped()) finish();
              });
              dirty = true;
            }
          }
        }
      } finally { available?.(); }
    })();
    this.#readerTask = pumping;
    void pumping.catch(error => { failure = error; }).finally(() => { done = true; available?.(); });
    try {
      while (!stopped() && !done) {
        if (!offer) await new Promise<void>(resolve => { available = resolve; if (offer || done || stopped()) resolve(); });
        if (offer && !stopped()) { const delivery = offer; offer = undefined; yield delivery; }
      }
      if (failure !== undefined) throw failure;
    } finally {
      // Returning an iterator stops pulling; in-hand input stays before the cursor.
      localStop = true;
      abort();
      unwatch?.();
      signal.removeEventListener("abort", abort);
      this.#closeController.signal.removeEventListener("abort", abort);
      await pumping.catch(() => undefined);
      if (this.#readerTask === pumping) this.#readerTask = undefined;
      this.#iterating = false;
    }
  }

  kv(_bucket: TransportBucket): TransportKv { throw new TransportUnsupportedError("File transport has no KV"); }
  async drainState(signal?: AbortSignal): Promise<TransportDrainState> { signal?.throwIfAborted(); return this.reader.drainState(); }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeController.abort();
    await Promise.allSettled([...this.#publishes, ...(this.#readerTask ? [this.#readerTask] : [])]);
  }
}

export const openFileTransport = async (options: FabricTransportOptions): Promise<FileTransport> => {
  options.signal?.throwIfAborted();
  return new FileTransport(options.mesh, { machine: "mesh", hostId: options.hostId });
};
