import { createHash } from "node:crypto";
import { diagnostics } from "../core/diagnostics.js";
import { readNativeRecoveryCustody, nativeRecoveryCarrierCustody } from "../core/native-recovery.js";
import type { NativeRecoveryBinding, NativeRecoveryCustodyReader, NativeRecoveryCustodyResult,
  NativeRecoveryParkIntent, NativeRecoverySessionRef } from "../core/session-receipts.js";
import { appendDeliveryOutcome } from "../mesh/delivery-outcomes.js";
import { MeshBackgroundQueue } from "../core/atomic-write.js";
import type { MeshEvent, MeshIdentity, MeshStore } from "../mesh/store.js";
import {
  INBOX_HORIZON_MS, ROOT_ID_PREFIX, ROOT_INBOX_ACTION_KINDS, compareRootInboxEvents, crossIdentityReceipts, deliveryIdentity,
  eventReceipt, eventReceipts, rootInboxBudget, topicWorkReceipt, workKey,
  type NativeRecoveryInboxHandoff, type RootInboxBatch, type RootInboxEvent, type RootInboxReconcileOptions, type RootInboxSession,
} from "./root-inbox-shared.js";

export {
  INBOX_INJECT_EVENT_TEXT_BYTES, INBOX_INJECT_MAX_EVENTS, INBOX_INJECT_MAX_TEXT_BYTES, ROOT_ID_PREFIX, ROOT_INBOX_CUSTOM_TYPE,
  confirmedMainInboxIds, confirmedRootInboxSession, projectRootInboxReceipt, rootInboxBudget, rootInboxMessage, rootInboxSession,
  rootInboxSummary, sessionHoldsInboxBatch,
  type NativeRecoveryInboxHandoff, type RootInboxBatch, type RootInboxEvent, type RootInboxReconcileOptions, type RootInboxSession,
} from "./root-inbox-shared.js";

/**
 * A root session's durable inbox (smarty-dev#754 §3.2 step 3): work events addressed to this root
 * on `fleet.*` topics that no steer or follow-up delivered. Senders publish a shadow record there
 * before they steer (the #1255 rule); a steer can time out or reach a root that is shutting down,
 * but the event stays in the mesh and in its archive. The root reconciles at run start, every
 * turn boundary and every settle, from a processing cursor in mesh state.
 *
 * A batch is saved as pending before it is delivered, and the cursor moves past it only once the
 * session's own entries hold its message. Until then every reconcile delivers it again, so work
 * survives a stop between the save and the session's write: delivery is at least once. A work
 * event is skipped when the recipient has a confirmed native/inbox receipt for its sender and
 * work identity, persisted across reloads. Queued messages are not receipts. Addressed shadows
 * older than the configurable horizon are expired on first drain without buying model turns,
 * except answers, action kinds and completions: those remain actionable work.
 *
 * Ordinary durable admission stays bounded. Priority lookahead beyond that prefix is also offered,
 * without moving its cursor or adding pending ids: the log and the pinned prefix recover it.
 * Only shown rows are acknowledged; overflow is still work for the following turn.
 *
 * An event whose message entered a turn is acknowledged by id (smarty-dev#8962): it is never
 * injected again, whatever the session file confirms. A fully acknowledged batch moves the cursor
 * at once. A per-root high-water mark (never lowered, merged from the stored state on every save)
 * plus a bounded set of out-of-order acked ids keep a re-scan from an older cursor, a reload or a
 * stale writer from injecting anything at or below the mark again.
 */
export const ROOT_INBOX_PREFIX = "topology/inbox/";
export const WORK_TOPIC_PREFIX = "fleet.";
/** A shadow record newer than this waits a reconcile, so its steer can arrive first and win. */
const STEER_GRACE_MS = 60_000;
/**
 * Idle wake priority (smarty-dev#4313): an event addressed to this exact session id wakes an idle
 * Main at once, past the steer grace and the wake cooldown. It admits the ordered prefix before
 * it, so the cursor never passes earlier work it has not admitted (at least once: a native steer
 * that lands after the shadow was injected can duplicate it; confirmed receipts still suppress).
 * Name/alias-addressed events keep the grace; unaddressed events never hold the cursor.
 */
const NEVER_EXPIRE_KINDS = new Set(["answer", "blocked", "blocker", "ask", "handoff", "done", "completion"]);
const LATE_DELIVERY_HOUR_MS = 60 * 60_000;
const inboxHorizonMs = (): number => {
  const text = process.env.PI_FABRIC_INBOX_HORIZON_MS;
  const value = Number(text);
  return text?.trim() && Number.isFinite(value) && value > 0 ? value : INBOX_HORIZON_MS;
};
/** The cursor is saved when it moves past a delivered batch, and otherwise at most this often. */
const SAVE_INTERVAL_MS = 10 * 60_000;
/** One batch holds at most this many events and this much text; a longer text is cut. */
const MAX_BATCH_EVENTS = 20;
/** Bound persisted dedup independently of session lifetime; leave 25% for mesh/headroom. */
const MAX_DELIVERED_RECEIPTS = 1024;
/** Acked ids kept above the high-water mark; a pending batch holds at most MAX_BATCH_EVENTS. */
const MAX_ACKED_IDS = 256;
const RECEIPT_VALUE_FRACTION = 0.75;
const MAX_BATCH_TEXT_BYTES = 32 * 1024;
const MAX_EVENT_TEXT_BYTES = 8 * 1024;
/**
 * Every idle wake is a model turn (smarty-dev#1579), so an idle root wakes at most this often,
 * unless the batch holds an urgent event. PI_FABRIC_INBOX_WAKE_COOLDOWN_MS overrides it, capped at
 * MAX_WAKE_COOLDOWN_MS so a typo cannot silence idle wakes for days.
 */
const WAKE_COOLDOWN_MS = 5 * 60_000;
const URGENT_KINDS = new Set(["p0", "steer"]);
/** Each idle wake publishes one event here, so the fleet's wakes per hour can be counted. */
export const ROOT_INBOX_WAKE_TOPIC = "fabric.inbox.wake";
/** Observational, once per pending range, only when a turn proves the root is active. */
export const ROOT_INBOX_ALARM_TOPIC = "fleet.alarm.root-inbox";
const PENDING_ALARM_MS = 15 * 60_000;
const MAX_WAKE_COOLDOWN_MS = 60 * 60_000;
let warnedWakeCooldownCap = false;
const wakeCooldownMs = (): number => {
  const value = Number(process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS);
  if (!process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS?.trim() || !Number.isFinite(value) || value < 0) return WAKE_COOLDOWN_MS;
  if (value <= MAX_WAKE_COOLDOWN_MS) return value;
  if (!warnedWakeCooldownCap) {
    warnedWakeCooldownCap = true;
    diagnostics.warn("src/topology/root-inbox.ts", `[pi-fabric] PI_FABRIC_INBOX_WAKE_COOLDOWN_MS=${value} exceeds ${MAX_WAKE_COOLDOWN_MS}; using ${MAX_WAKE_COOLDOWN_MS}`);
  }
  return MAX_WAKE_COOLDOWN_MS;
};

interface RootInboxState {
  after: number;
  /** Reversible pre-snapshot scope locator only; neither a seal nor successor authority. */
  nativeRecoveryPark?: NativeRecoveryParkIntent;
  pending?: { through: number; ids: string[]; since: number; alarmed?: boolean };
  /** Hashed delivery/work identities, scoped by this recipient's state key. */
  delivered?: string[];
  /** Receipt times parallel to delivered. Legacy string-only states migrate on first save. */
  deliveredAt?: number[];
  /** Topic-scoped legacy key/ref claims already spent suppressing one shadow each. */
  fallbacks?: string[];
  /** Consumption times parallel to fallbacks; bounded with the delivered receipt ledger. */
  fallbackAt?: number[];
  /** Old native claims cannot regain suppression after their consumed marker is evicted. */
  fallbackThrough?: number;
  /** High-water mark above `after`: no event at or below it is injected again. Never lowered. */
  ackedThrough?: number;
  /** Ids acked out of order above the mark, each with an upper bound of its mesh sequence. */
  acked?: Array<[string, number]>;
}

/** Only cursor-only progress is gated; receipt/pending changes stay dirty until saved. */
const immediateSnapshot = (state: Partial<RootInboxState> | undefined): string => JSON.stringify([
  state?.pending, state?.delivered, state?.deliveredAt, state?.fallbacks, state?.fallbackAt, state?.fallbackThrough,
]);

const sequenceOf = (value: unknown): number =>
  typeof value === "number" && Number.isSafeInteger(value) ? value : Number.NEGATIVE_INFINITY;
const parseAcks = (value: unknown): Array<[string, number]> => Array.isArray(value)
  ? value.filter((item): item is [string, number] => Array.isArray(item) && typeof item[0] === "string" &&
    typeof item[1] === "number" && Number.isFinite(item[1])).map(([id, bound]) => [id, bound])
  : [];

export class RootInbox {
  #state: RootInboxState | undefined;
  #saved: string | undefined;
  #savedImmediate: string | undefined;
  #savedAt = 0;
  #wokeAt = Number.NEGATIVE_INFINITY;
  #dueAt = Number.POSITIVE_INFINITY;
  #delivered = new Map<string, number>();
  #acks = new Map<string, number>();
  #fallbacks = new Map<string, number>();
  #initialAfter: number | undefined;
  #native: { scope: NativeRecoverySessionRef; read: NativeRecoveryCustodyReader; chain: string } | undefined;
  #nativeResult: NativeRecoveryCustodyResult | undefined;
  #nativeBlocked: string | undefined;
  readonly #predecessors = new Map<string, RootInbox>();
  readonly #offered = new Map<string, { inbox: RootInbox; event: MeshEvent }>();
  readonly #reads = new Set<Promise<unknown>>();
  #writes = Promise.resolve();
  readonly #notifications = new MeshBackgroundQueue("root inbox wake notification");
  /** An ack is one-shot: its own queue retries a mesh lock timeout instead of dropping it. */
  readonly #ackWrites = new MeshBackgroundQueue("root inbox ack");

  async close(): Promise<void> {
    await Promise.all([this.#ackWrites.close(), this.#notifications.close(), ...[...this.#predecessors.values()].map(inbox => inbox.close())]);
  }

  constructor(
    readonly mesh: MeshStore,
    readonly identity: MeshIdentity,
    /** The ids and names a sender may address this root by (its id first). */
    readonly names: () => readonly string[],
    readonly options: { now?: () => number; steerGraceMs?: number; pageSize?: number; wakeCooldownMs?: number; horizonMs?: number } = {},
  ) {}

  get key(): string {
    return ROOT_INBOX_PREFIX + createHash("sha256").update(this.identity.id).digest("hex").slice(0, 32);
  }

  /** Reversible pre-snapshot flush. R/N suspend delivery; only native PREPARED seals S. */
  async parkForNativeRecovery(intent: NativeRecoveryParkIntent): Promise<NativeRecoveryInboxHandoff> {
    if (intent.version !== 1 || !intent.operationId || `${ROOT_ID_PREFIX}${intent.source.sessionId}` !== this.identity.id ||
        !intent.source.sessionFile || !intent.target.sessionFile || intent.source.sessionId === intent.target.sessionId) {
      throw new Error("Root inbox native recovery source mismatch");
    }
    const state = this.#load(), after = state.after;
    // Deliberately copy only intent fields. The handoff itself participates in PREPARED's
    // digest, so no digest/entry/phase can be required here. An aborted intent may be replaced.
    state.nativeRecoveryPark = JSON.parse(JSON.stringify({ version: 1, operationId: intent.operationId,
      source: intent.source, target: intent.target })) as NativeRecoveryParkIntent;
    await Promise.all(this.#reads);
    await Promise.all(this.#readers().map(async inbox => {
      await inbox.#ackWrites.checkpointForRelease();
      await inbox.#save(true); // A background first-attempt receipt is not a confirmed flush.
    }));
    return { version: 1, rootId: this.identity.id, key: this.key, after };
  }

  /** Install every committed predecessor reader before start/wake/context, keeping its own key/marks. */
  async adoptNativeRecoveryInbox(handoff: NativeRecoveryInboxHandoff, readNativeCustody: NativeRecoveryCustodyReader): Promise<void> {
    this.#nativeBlocked = "Root inbox native recovery adoption incomplete";
    if (handoff.version !== 1 || !handoff.rootId.startsWith(ROOT_ID_PREFIX) || handoff.rootId === this.identity.id ||
        !Number.isSafeInteger(handoff.after) || handoff.after < 0 || handoff.key !== this.#keyFor(handoff.rootId)) {
      throw new Error("Invalid native recovery inbox handoff");
    }
    // The handoff has no sessionFile. Parked cursor rows supply scope locators only;
    // native must validate the exact target. A row alone never grants adoption.
    let rootId = handoff.rootId;
    const visited = new Set<string>();
    let scope: NativeRecoverySessionRef;
    let result: NativeRecoveryCustodyResult;
    for (;;) {
      if (visited.has(rootId)) throw new Error("Cyclic root inbox recovery locator");
      visited.add(rootId);
      const locator = (this.mesh.get(this.#keyFor(rootId))?.value as Partial<RootInboxState> | undefined)?.nativeRecoveryPark;
      if (!locator || `${ROOT_ID_PREFIX}${locator.source.sessionId}` !== rootId) throw new Error("Missing parked root inbox recovery locator");
      scope = locator.target;
      result = readNativeRecoveryCustody(scope, readNativeCustody);
      if (result.status !== "validated") throw new Error(result.status === "hold" ? result.reason : "Missing native recovery custody");
      const edge = result.transfers.find(transfer => this.#intentKey(transfer) === this.#intentKey(locator));
      if (!edge || edge.phase !== "received") throw new Error("Root inbox recovery requires matching RECEIVED");
      rootId = `${ROOT_ID_PREFIX}${scope.sessionId}`;
      if (rootId === this.identity.id) break;
    }
    const chain = this.#committedChain(result);
    if (this.#native && this.#native.chain !== chain) throw new Error("Conflicting root inbox native recovery adoption");
    if (!result.transfers.some(edge => `${ROOT_ID_PREFIX}${edge.source.sessionId}` === handoff.rootId)) {
      throw new Error("Native recovery inbox predecessor absent");
    }
    const readers = new Map<string, RootInbox>();
    for (const edge of result.transfers) {
      const id = `${ROOT_ID_PREFIX}${edge.source.sessionId}`;
      const stored = this.mesh.get(this.#keyFor(id))?.value as Partial<RootInboxState> | undefined;
      const after = id === handoff.rootId ? handoff.after : stored?.after;
      if (!Number.isSafeInteger(after) || after! < 0) throw new Error("Missing native predecessor inbox boundary");
      const inbox = this.#predecessors.get(id) ?? new RootInbox(this.mesh, { id, name: id, kind: "main", sessionId: edge.source.sessionId }, () => [id], this.options);
      inbox.#initialAfter = after;
      inbox.#load();
      readers.set(id, inbox);
    }
    this.#predecessors.clear();
    for (const [id, inbox] of readers) this.#predecessors.set(id, inbox);
    this.#native = { scope: { ...scope }, read: readNativeCustody, chain };
    this.#nativeBlocked = undefined;
    this.#installNativeResult(result);
  }

  #keyFor(rootId: string): string {
    return ROOT_INBOX_PREFIX + createHash("sha256").update(rootId).digest("hex").slice(0, 32);
  }

  #intentKey(intent: NativeRecoveryParkIntent): string {
    return JSON.stringify([intent.operationId, intent.source.sessionId, intent.source.sessionFile,
      intent.target.sessionId, intent.target.sessionFile]);
  }

  #bindingKey(binding: NativeRecoveryBinding): string {
    return JSON.stringify([binding.operationId, binding.preparedDigest, binding.preparedEntryId,
      binding.source.sessionId, binding.source.sessionFile, binding.target.sessionId, binding.target.sessionFile]);
  }

  #committedChain(result: NativeRecoveryCustodyResult): string {
    if (result.status === "hold") throw new Error(result.reason);
    if (result.status !== "validated" || !result.transfers.length ||
        result.transfers.some(edge => edge.phase !== "received" || !edge.receivedEntryId ||
          edge.occurrences.some(occurrence => occurrence.state === "ambiguous")) ||
        `${ROOT_ID_PREFIX}${result.transfers.at(-1)!.target.sessionId}` !== this.identity.id) {
      throw new Error("Root inbox native recovery is not a committed unambiguous target chain");
    }
    return JSON.stringify(result.transfers.map(edge => [this.#bindingKey(edge), edge.receivedEntryId]));
  }

  #installNativeResult(result: NativeRecoveryCustodyResult): void {
    this.#nativeResult = result;
    for (const inbox of this.#predecessors.values()) inbox.#nativeResult = result;
  }

  #refreshNative(): void {
    if (this.#nativeBlocked) throw new Error(this.#nativeBlocked);
    if (!this.#native) return;
    const result = readNativeRecoveryCustody(this.#native.scope, this.#native.read);
    if (this.#committedChain(result) !== this.#native.chain) throw new Error("Root inbox native recovery chain changed");
    this.#installNativeResult(result);
  }

  #nativeStatus(id: string, receipts: readonly string[] = []): "none" | "hold" | "reserved" | "received" | "consumed" {
    if (!this.#nativeResult) return "none";
    let found: ReturnType<typeof nativeRecoveryCarrierCustody> = { status: "none" };
    for (const inboxId of new Set([id, eventReceipt(id), ...receipts])) {
      const custody = nativeRecoveryCarrierCustody(this.#nativeResult, { inboxId });
      if (custody.status === "hold") throw new Error(custody.reason ?? "Root inbox native custody hold");
      if (custody.status === "none") continue;
      if (found.status !== "none" && (found.transfer?.operationId !== custody.transfer?.operationId ||
          found.occurrence?.ordinal !== custody.occurrence?.ordinal)) throw new Error("Conflicting native root inbox occurrence identities");
      found = custody;
    }
    return found.status;
  }

  #nativePending(id: string, receipts: readonly string[] = []): boolean {
    const status = this.#nativeStatus(id, receipts);
    return status === "reserved" || status === "received";
  }

  #readers(): RootInbox[] { return [...this.#predecessors.values(), this]; }

  #receiptLedger(id: string, sequence?: number): RootInbox | undefined {
    const offered = this.#offered.get(id);
    if (offered) return offered.inbox;
    const pending = this.#readers().find(inbox => inbox.#state?.pending?.ids.includes(id));
    if (pending) return pending;
    if (!this.#predecessors.size) return this;
    // A restored native carrier may enter context before RootInbox offered it at T.
    // Resolve its ORIGINAL event address, never T's unrelated high-water mark.
    const event = this.#reread(sequence === undefined ? 0 : sequence - 1,
      { ids: [id], through: sequence ?? this.mesh.latestSequence(), since: this.#now() })[0];
    if (!event) return undefined;
    const inbox = event.to === this.identity.id ? this : event.to ? this.#predecessors.get(event.to) : undefined;
    if (inbox) {
      this.#offered.set(id, { inbox, event });
      while (this.#offered.size > MAX_DELIVERED_RECEIPTS) this.#offered.delete(this.#offered.keys().next().value!);
    }
    return inbox;
  }

  async #reading<T>(read: () => Promise<T>): Promise<T> {
    const work = read();
    this.#reads.add(work);
    try { return await work; }
    finally { this.#reads.delete(work); }
  }

  async #collect(session: RootInboxSession, options: RootInboxReconcileOptions, priority: boolean): Promise<RootInboxBatch> {
    const events: RootInboxEvent[] = [];
    let through = this.#load().after, skippedStale = 0;
    for (const inbox of this.#readers()) {
      const batch = await inbox.#next(session, options, priority);
      if (inbox === this) through = batch.through;
      skippedStale += batch.skippedStale ?? 0;
      for (const event of batch.events) {
        this.#offered.set(event.id, { inbox, event });
        while (this.#offered.size > MAX_DELIVERED_RECEIPTS) this.#offered.delete(this.#offered.keys().next().value!);
        events.push(event);
      }
      this.#dueAt = Math.min(this.#dueAt, inbox.#dueAt);
    }
    return { events, through,
      ...(skippedStale ? { skippedStale, horizonMs: this.#horizon() } : {}) };
  }

  /**
   * The batch to deliver now. A pending batch the session holds is committed first; one it does
   * not hold is delivered again. Ordinary admission stops at the steer grace (priority reads
   * admit through an exact-session event first) and batch bounds, so the cursor never passes
   * work it has not admitted. Ranked lookahead can offer newer work past that pending prefix.
   */
  async next(session: RootInboxSession, options: RootInboxReconcileOptions = {}): Promise<RootInboxBatch> {
    this.#dueAt = Number.POSITIVE_INFINITY;
    this.#refreshNative();
    return this.#reading(() => this.#collect(session, options, options.priority === true));
  }

  /**
   * The exact time (epoch ms) at which addressed work the last read held back (steer grace or
   * idle cooldown) becomes due; undefined when nothing is held. A hint for one one-shot idle
   * wake timer only; it never admits or commits anything.
   */
  get wakeDueAt(): number | undefined {
    return Number.isFinite(this.#dueAt) ? this.#dueAt : undefined;
  }

  async #next(session: RootInboxSession, options: RootInboxReconcileOptions, priority: boolean): Promise<RootInboxBatch> {
    const state = this.#load();
    let saveImmediately = this.#trimReceipts();
    saveImmediately = immediateSnapshot(state) !== this.#savedImmediate || saveImmediately;
    // Canonical history without a finite timestamp is exact-replay evidence only; do not
    // renew its ambiguous key/ref claim at read time.
    // T's history can contain several recipients. Do not copy that whole receipt ledger into
    // each predecessor: matched addressed events below retain only their own exact receipts.
    if (!this.#nativeResult) saveImmediately = this.#remember(session.delivered ?? [], session.deliveredAt, true) || saveImmediately;
    let skippedStale = 0;
    const reservations = new Set<string>();
    if (state.pending && options.turnEnd) await this.#alarmPending(state).catch(error => {
      // An observational alarm must never keep confirmed work wedged behind publication.
      diagnostics.warn("src/topology/root-inbox.ts", `[pi-fabric] root inbox alarm: ${error instanceof Error ? error.message : String(error)}`);
    });
    // An ack can land while this read awaits the mesh: it may commit this very batch.
    const held = state.pending;
    if (held) {
      // Acked ids are never delivered again (smarty-dev#8962); a fully acked batch commits.
      const unacked = held.through <= this.#mark() ? [] : held.ids.filter((id) => !this.#acks.has(id));
      const holdsBatch = unacked.length === 0 || session.holdsBatch(held.ids);
      if (options.commitOnly && !holdsBatch) {
        await this.#save(saveImmediately);
        return { events: [], through: state.after };
      }
      const reread = this.#reread(state.after, held);
      const pending = reread.filter((event) => !this.#acked(event));
      if (holdsBatch) {
        // smarty-dev#5518: only the session's own inbox carrier is native consumption; an ack-only commit is not.
        if (session.holdsBatch(held.ids)) for (const event of pending) appendDeliveryOutcome(this.mesh.root, {
          eventId: event.id, to: this.identity.id, from: event.from.id, mode: "publish",
        }, "delivered", "consumed by Main inbox as itself", this.#now());
        this.#remember(pending.flatMap(eventReceipts));
      } else {
        // A missing retained event proves neither delivery nor expiry. Keep its id pending, until
        // the batch itself is older than the stale horizon: its events are older still.
        const found = new Set(reread.map((event) => event.id));
        const withheld = new Set(unacked.filter(id => !found.has(id) && this.#nativePending(id) &&
          !this.#delivered.has(eventReceipt(id)) && !session.delivered?.has(eventReceipt(id))));
        let missing = unacked.filter((id) => !found.has(id) && !withheld.has(id) && this.#nativeStatus(id) !== "consumed" &&
          !this.#delivered.has(eventReceipt(id)) && !session.delivered?.has(eventReceipt(id)));
        if (missing.length && this.#now() - held.since > this.#horizon()) {
          skippedStale += missing.length;
          missing = [];
        }
        // A native delivery can arrive after the pending save. Recheck it on recovery too.
        const events = pending.filter((event) => {
          const status = this.#nativeStatus(event.id, eventReceipts(event));
          const reserved = status === "reserved" || status === "received";
          // Real canonical receipts win even while native still reports the saved holder pending.
          const confirmed = reserved ? this.#steered(event, session, reservations) : undefined;
          if (reserved && !confirmed) { withheld.add(event.id); return false; }
          if (status !== "consumed" && this.#stale(event)) { skippedStale++; return false; }
          if (!confirmed && !this.#steered(event, session, reservations)) return true;
          this.#consumeFallback(event, reservations);
          this.#remember(eventReceipts(event));
          return false;
        }).map((event) => this.#late(event));
        if (events.length || missing.length || withheld.size) {
          held.ids = [...events.map((event) => event.id), ...missing, ...withheld];
          // Retry a failed pending-cursor save before delivery: in-memory pending alone
          // is not durable admission, including after a mesh acquisition timeout.
          await this.#save(true);
          return this.#offer({ events, through: held.through, ...(skippedStale ? { skippedStale, horizonMs: this.#horizon() } : {}) },
            session, priority, reservations);
        }
      }
      state.after = Math.max(state.after, held.through);
      if (state.pending === held) delete state.pending;
      await this.#save(true);
    }
    if (options.commitOnly) {
      await this.#save(saveImmediately);
      return { events: [], through: state.after };
    }
    const batch = this.#scan(state.after, session, true, (event, reservations) => {
      saveImmediately = this.#consumeFallback(event, reservations) || saveImmediately;
      saveImmediately = this.#remember(eventReceipts(event)) || saveImmediately;
    }, priority, reservations);
    skippedStale += batch.skippedStale ?? 0;
    if (skippedStale) { batch.skippedStale = skippedStale; batch.horizonMs = this.#horizon(); }
    if (batch.events.length === 0) {
      state.after = batch.through;
      // Expiry and new receipts are durable immediately: a restart must not repeat the summary.
      await this.#save(saveImmediately || skippedStale > 0);
      return batch;
    }
    state.pending = { through: batch.through, ids: batch.events.map((event) => event.id), since: this.#now() };
    await this.#save(true);
    return this.#offer(batch, session, priority, reservations);
  }

  /**
   * The batch an idle root wakes for now (smarty-dev#1595), or undefined. It is `next`, at most
   * once per wake cooldown; an urgent event (kind p0 or steer) wakes at once. Inside the cooldown
   * nothing is saved, so a batch held back does not block an urgent event behind it as pending.
   * The urgency check looks past the batch bounds (review F1): an urgent event behind a full batch
   * still wakes the root at once. Turn/settle reconciliation brings the batches in order
   * without this idle cooldown, so the cursor never skips an event.
   * `idle` is checked again after the read: a turn that started meanwhile takes the batch itself.
   */
  async wake(session: RootInboxSession, idle: () => boolean): Promise<RootInboxBatch | undefined> {
    return this.#reading(() => this.#wake(session, idle));
  }

  async #wake(session: RootInboxSession, idle: () => boolean): Promise<RootInboxBatch | undefined> {
    this.#dueAt = Number.POSITIVE_INFINITY;
    this.#refreshNative();
    const cooldownEnd = this.#wokeAt + (this.options.wakeCooldownMs ?? wakeCooldownMs());
    const cooling = this.#now() < cooldownEnd;
    const peeked = this.#readers().flatMap(inbox => inbox.#peek(session));
    const urgent = peeked.some((event) => URGENT_KINDS.has(event.kind));
    // Work addressed to this exact session wakes at once too (smarty-dev#4313), unless delivered late.
    const exact = peeked.some((event) => this.#readers().some(inbox => inbox.#exact(event)) && !event.deliveredLate);
    if (cooling && !urgent && !exact) {
      if (peeked.length) this.#dueAt = Math.min(this.#dueAt, cooldownEnd);
      return undefined;
    }
    const batch = await this.#collect(session, {}, true);
    if (!idle()) return undefined;
    // A stale-only drain reports once but must not buy a model turn; nor does late actionable work.
    if (batch.events.length === 0) return batch.skippedStale ? batch : undefined;
    const waking = rootInboxBudget(batch.events).shown.filter((event) => !(event as RootInboxEvent).deliveredLate);
    if (waking.length === 0) return batch;
    const reason = urgent ? "p0" : exact ? "addressed" : "idle";
    this.#wokeAt = this.#now();
    void this.#notifications.enqueue(() => this.mesh.publish({
      topic: ROOT_INBOX_WAKE_TOPIC, kind: "idle-wake", from: this.identity,
      data: { count: waking.length, reason, ids: waking.map((event) => event.id) },
    }));
    return batch;
  }

  /**
   * Record injected events as consumed (R-token-waste): once a batch message entered a turn, its
   * events are never injected again, even if the canonical session file does not (yet) confirm the
   * message. Their receipts join the durable delivered set, so the pending batch commits at the
   * next reconcile and a later shadow of the same work is suppressed too.
   */
  async consume(ids: readonly string[], receipts: readonly string[] = []): Promise<void> {
    this.ack(ids, receipts);
    await Promise.all(this.#readers().map(inbox => inbox.#ackWrites.enqueue(() => inbox.#save(true))));
  }

  /**
   * Acknowledge events by id (smarty-dev#8962), in memory at once: a read in flight cannot
   * redeliver them, and `consume` persists the ack. An id of the pending batch is kept until the
   * whole batch is acked; then the cursor and the high-water mark move past the batch.
   */
  ack(ids: readonly string[], receipts: readonly string[] = []): void {
    this.#refreshNative();
    const groups = new Map<RootInbox, { ids: string[]; receipts: Set<string> }>();
    for (const id of ids) {
      const inbox = this.#receiptLedger(id);
      if (!inbox) throw new Error("Root inbox receipt has no original addressed ledger");
      const offered = this.#offered.get(id);
      const group = groups.get(inbox) ?? { ids: [], receipts: new Set<string>() };
      group.ids.push(id);
      if (offered) for (const receipt of eventReceipts(offered.event)) if (receipts.includes(receipt)) group.receipts.add(receipt);
      groups.set(inbox, group);
    }
    if (!ids.length) groups.set(this, { ids: [], receipts: new Set(receipts) });
    for (const [inbox, group] of groups) inbox.#ack(group.ids, groups.size === 1 ? receipts : [...group.receipts]);
  }

  #ack(ids: readonly string[], receipts: readonly string[]): void {
    const state = this.#load();
    this.#trimReceipts();
    this.#remember([...ids.map(eventReceipt), ...receipts]);
    const pending = state.pending;
    // smarty-dev#5518: an ack is the inbox carrier entering a turn, so it is native consumption.
    // Resolve the batch's newly acked events before the cursor moves past them; next() then never
    // sees them, and the journal dedups an id it already holds (exactly once per event id).
    // Lookahead can be above the pending prefix. Its ack must retain its actual sequence:
    // committing the prefix must not erase an out-of-order consumption receipt.
    const consumed = ids.filter((id) => !this.#acks.has(id));
    const through = Math.max(pending?.through ?? state.after, this.mesh.latestSequence());
    const delivered = consumed.length ? this.#reread(state.after, { ids: consumed, through, since: this.#now() }) : [];
    const sequences = new Map(delivered.map((event) => [event.id, event.sequence]));
    for (const id of ids) {
      if (!this.#acks.has(id)) this.#acks.set(id, sequences.get(id) ?? through);
    }
    if (pending && pending.ids.every((id) => this.#acks.has(id))) {
      state.after = Math.max(state.after, pending.through);
      delete state.pending;
    }
    this.#syncAcks();
    for (const event of delivered) {
      try {
        appendDeliveryOutcome(this.mesh.root, { eventId: event.id, to: this.identity.id, from: event.from.id, mode: "publish" },
          "delivered", "consumed by Main inbox as itself", this.#now());
      } catch (error) {
        // Observational evidence must never undo the ack that keeps the event from re-injection.
        diagnostics.warn("src/topology/root-inbox.ts", `[pi-fabric] root inbox delivery outcome: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }

  /**
   * Whether an event was acknowledged (smarty-dev#8962): its id is in the acked set, or its
   * sequence is at or below its own high-water mark. Ordinary sessions use cached state only;
   * native-restored carriers can resolve their original addressed ledger before their first offer.
   */
  acked(id: string, sequence?: number): boolean {
    this.#refreshNative();
    const status = this.#nativeStatus(id);
    if (status === "consumed") return true;
    const inbox = this.#receiptLedger(id, sequence);
    if (!inbox || !inbox.#state) return false;
    if (status === "received" || status === "reserved") return inbox.#acks.has(id) || inbox.#delivered.has(eventReceipt(id));
    return inbox.#acks.has(id) || (sequence !== undefined && sequence <= inbox.#mark());
  }

  /**
   * Set the start boundary now (review F3): a root with no saved cursor starts at the present
   * when it becomes available, not at its first read, so an event sent between the two counts.
   */
  start(): void {
    this.#load();
  }

  /** Every event `next` would bring now or in later batches, without the batch bounds or a save. */
  #peek(session: RootInboxSession): RootInboxEvent[] {
    const state = this.#load();
    const pending = state.pending && !session.holdsBatch(state.pending.ids) ? state.pending : undefined;
    const after = state.pending ? Math.max(state.after, state.pending.through) : state.after;
    const reservations = new Set<string>();
    return [...(pending ? this.#reread(state.after, pending).filter((event) =>
      !this.#acked(event) && !this.#nativePending(event.id, eventReceipts(event)) && !this.#stale(event) && !this.#steered(event, session, reservations)).map((event) => this.#late(event)) : []),
    ...this.#scan(after, session, false, undefined, true, reservations).events];
  }

  /** Addressed to this session's own id on a work topic (not a name or alias). */
  #exact(event: MeshEvent): boolean {
    return event.topic.startsWith(WORK_TOPIC_PREFIX) && event.to === this.identity.id;
  }

  /** Everything at or below this sequence was delivered and acked, or decided: never again. */
  #mark(): number {
    const state = this.#state!;
    return Math.max(state.after, state.ackedThrough ?? Number.NEGATIVE_INFINITY);
  }

  #acked(event: MeshEvent): boolean { return event.sequence <= this.#mark() || this.#acks.has(event.id); }

  /**
   * Keep the ordinary 20-event/32-KiB durable prefix, plus a ranked tail window that fits the
   * remaining persisted ack slots. Reserve every unacked pending id too, including missing ids,
   * so a full lookahead window leaves enough headroom for the pinned prefix to make progress.
   * Only the prefix is pending state; no cursor crosses intervening undisplayed work.
   */
  #offer(
    batch: RootInboxBatch, session: RootInboxSession, priority: boolean, reservations: Set<string>,
  ): RootInboxBatch {
    const seen = new Set<string>();
    const actions: RootInboxEvent[] = [], candidates: RootInboxEvent[] = [];
    const consider = (event: RootInboxEvent): void => {
      const receipts = eventReceipts(event);
      if (receipts.some((id) => seen.has(id))) return;
      for (const id of receipts) seen.add(id);
      if (ROOT_INBOX_ACTION_KINDS.has(event.kind)) {
        actions.push(event);
        actions.sort(compareRootInboxEvents);
        if (actions.length > MAX_ACKED_IDS) actions.pop();
      } else {
        candidates.push(event);
        candidates.sort(compareRootInboxEvents);
        if (candidates.length > MAX_BATCH_EVENTS) candidates.pop();
      }
    };
    batch.events.forEach(consider);
    // Reuse MAIN's scan protocol, including exact-session priority and one-use fallback claims.
    // This is observational lookahead: suppressed tail events are consumed only on admission.
    this.#scan(batch.through, session, false, undefined, priority, reservations).events.forEach(consider);
    const window: RootInboxEvent[] = [];
    let bytes = 0;
    for (const event of candidates) {
      const size = Math.min(Buffer.byteLength(event.text ?? ""), MAX_EVENT_TEXT_BYTES);
      if (window.length && bytes + size > MAX_BATCH_TEXT_BYTES) break;
      window.push(event);
      bytes += size;
    }
    const ids = new Set(batch.events.map((event) => event.id));
    const pendingSlots = this.#state!.pending?.ids.filter((id) => !this.#acks.has(id)).length ?? 0;
    // Nonaction lookahead uses the same cumulative ack capacity as actions.
    let remaining = Math.max(0, MAX_ACKED_IDS - this.#acks.size - pendingSlots);
    const lookahead = [...actions, ...window].filter((event) => {
      if (ids.has(event.id) || remaining === 0) return false;
      ids.add(event.id);
      remaining--;
      return true;
    });
    // Keep the pinned prefix offered even at capacity; without lookahead it gets the next turn.
    return this.#unacked({ ...batch, events: [...batch.events, ...lookahead] });
  }

  /** Drop what an ack recorded while this read awaited the mesh. */
  #unacked(batch: RootInboxBatch): RootInboxBatch {
    const events = batch.events.filter((event) => !this.#acked(event));
    return events.length === batch.events.length ? batch : { ...batch, events };
  }

  /** Prune acks the mark covers and bound the rest; keep the stored mark only above the cursor. */
  #syncAcks(): void {
    const state = this.#state!;
    if (state.ackedThrough !== undefined && state.ackedThrough <= state.after) delete state.ackedThrough;
    const mark = this.#mark();
    let entries = [...this.#acks].filter(([, bound]) => bound > mark);
    if (entries.length > MAX_ACKED_IDS) entries = entries.sort((a, b) => a[1] - b[1]).slice(-MAX_ACKED_IDS);
    this.#acks = new Map(entries);
    if (entries.length) state.acked = entries;
    else delete state.acked;
  }

  /**
   * Another writer of this cursor (a reloaded predecessor finishing a read) can hold an older
   * cached view. The stored mark and acks are merged in before a save, so they are never lowered.
   */
  #absorb(): void {
    const value = this.mesh.get(this.key)?.value as Partial<RootInboxState> | undefined;
    if (!value || typeof value !== "object") return;
    const state = this.#state!;
    if (value.nativeRecoveryPark !== undefined) state.nativeRecoveryPark ??= value.nativeRecoveryPark;
    const theirs = Math.max(sequenceOf(value.after), sequenceOf(value.ackedThrough));
    if (theirs > this.#mark()) state.ackedThrough = theirs;
    for (const [id, bound] of parseAcks(value.acked)) if (!this.#acks.has(id)) this.#acks.set(id, bound);
    if (typeof value.fallbackThrough === "number" && Number.isFinite(value.fallbackThrough)) {
      state.fallbackThrough = Math.max(state.fallbackThrough ?? Number.NEGATIVE_INFINITY, value.fallbackThrough);
    }
    if (Array.isArray(value.fallbacks)) value.fallbacks.forEach((id, index) => {
      if (typeof id !== "string") return;
      const time = value.fallbackAt?.[index];
      const at = typeof time === "number" && Number.isFinite(time) ? time : this.#fallbacks.get(id);
      // A legacy marker without an observation time cannot prove that its one-use claim is current.
      if (at === undefined) return;
      this.#fallbacks.set(id, Math.max(this.#fallbacks.get(id) ?? Number.NEGATIVE_INFINITY, at));
    });
    this.#syncAcks();
  }

  #scan(
    after: number, session: RootInboxSession, bounded = true,
    onDelivered?: (event: MeshEvent, reservations: ReadonlySet<string>) => void, priority = false,
    reservations = new Set<string>(),
  ): RootInboxBatch {
    const now = this.#now();
    const names = new Set(this.names().filter((name) =>
      name.trim() && (name === this.identity.id || !name.startsWith(ROOT_ID_PREFIX))));
    const grace = this.options.steerGraceMs ?? STEER_GRACE_MS;
    const cutoff = now - grace;
    const pageSize = this.options.pageSize ?? 500;
    const addressed = (event: MeshEvent): boolean =>
      event.topic.startsWith(WORK_TOPIC_PREFIX) && event.to !== undefined && names.has(event.to);
    // Priority reads admit the ordered prefix through the last eligible exact-session event now.
    // Active-turn reconciles keep the grace, so a steer into a running turn still wins.
    let bypassThrough = Number.NEGATIVE_INFINITY;
    const priorityReservations = new Set(reservations);
    if (priority) for (let cursor = after; ;) {
      const page = this.mesh.read({ after: cursor, limit: pageSize });
      for (const event of page) {
        cursor = Math.max(cursor, event.sequence);
        if (event.createdAt > cutoff && this.#exact(event) && !this.#acked(event) && !this.#steered(event, session, priorityReservations)) {
          bypassThrough = event.sequence;
        }
      }
      if (page.length < pageSize) break;
    }
    const events: RootInboxEvent[] = [];
    let through = after;
    let bytes = 0;
    let skippedStale = 0;
    const seen = new Set<string>();
    const result = (): RootInboxBatch => ({ events, through, ...(skippedStale ? { skippedStale, horizonMs: this.#horizon() } : {}) });
    for (;;) {
      const page = this.mesh.read({ after: through, limit: pageSize });
      for (const event of page) {
        // Native reservation is neither delivery nor expiry, and never advances this ledger.
        if (addressed(event) && !this.#acked(event) && this.#nativePending(event.id, eventReceipts(event)) &&
            !this.#steered(event, session, reservations)) return result();
        // Only unadmitted addressed work inside the grace holds the cursor; it is due once mature.
        if (event.createdAt > cutoff && event.sequence > bypassThrough && addressed(event) && !this.#acked(event)) {
          this.#dueAt = Math.min(this.#dueAt, event.createdAt + grace);
          return result();
        }
        if (addressed(event)) {
          if (this.#acked(event)) { /* Delivered and acked, at or below the mark: never again. */ }
          else if (this.#nativeStatus(event.id, eventReceipts(event)) === "consumed") onDelivered?.(event, reservations);
          else if (this.#stale(event)) skippedStale++;
          else if (this.#steered(event, session, reservations)) onDelivered?.(event, reservations);
          else if (!eventReceipts(event).some((id) => seen.has(id))) {
            const size = Math.min(Buffer.byteLength(event.text ?? ""), MAX_EVENT_TEXT_BYTES);
            if (bounded && (events.length >= MAX_BATCH_EVENTS || (events.length > 0 && bytes + size > MAX_BATCH_TEXT_BYTES))) {
              return result();
            }
            events.push(this.#late(event));
            bytes += size;
            for (const id of eventReceipts(event)) seen.add(id);
          }
        }
        through = Math.max(through, event.sequence);
      }
      if (page.length < pageSize) return result();
    }
  }

  // Positive delivery evidence only, qualified by sender and this recipient's state key.
  // Before Pi records a native message, its shadow remains recoverable (at least once).
  #steered(event: MeshEvent, session: RootInboxSession, reservations?: Set<string>): boolean {
    const has = (id: string): boolean => this.#delivered.has(id) || session.delivered?.has(id) === true;
    if (eventReceipts(event).some(has) || crossIdentityReceipts(event.from.id, event.data).some(has) || this.#nativeStatus(event.id, eventReceipts(event)) === "consumed") return true;
    const identity = deliveryIdentity(event.data);
    // An explicit deliveryId/messageId/commandId is a stable event identity. Once the exact receipts above
    // miss, this is distinct work: never let its shared key/ref fall back to a native carrier.
    // #1226 also records originating event receipts on Main carriers, so exact matching stays the
    // preferred path as carriers gain that metadata.
    if (identity) return false;
    const key = workKey(event.data);
    // smarty-dev#12140: a key/ref receipt is a native carrier's claim ("native-topic-*"), never a
    // consumed mesh event's: a distinct legacy event with the same sender, topic and key still
    // needs the key fallback only when it has no stable delivery identity of its own.
    const data = event.data && typeof event.data === "object" ? event.data as { ref?: unknown } : undefined;
    const kind: "key" | "ref" = key === undefined ? "ref" : "key";
    const value = key ?? (typeof data?.ref === "string" && data.ref.trim() ? data.ref.trim() : undefined);
    if (value === undefined) return false;
    const claim = topicWorkReceipt(event.from.id, kind, event.topic, value);
    const through = this.#state!.fallbackThrough;
    // A key/ref claim needs a positive integer observation time with at most 60s of future skew.
    // Invalid/missing/future timestamps are no proof; exact identity receipts were checked above.
    const at = session.delivered?.has(claim) ? session.deliveredAt?.get(claim) : this.#delivered.get(claim);
    if (typeof at !== "number" || !Number.isFinite(at) || !Number.isInteger(at) || at <= 0 || at > this.#now() + 60_000) return false;
    const claimed = has(claim) || (kind === "key" && session.holdsSteer(event.from.id, value, event.topic));
    // Canonical history can outlive its cursor receipt, even for an unused claim. Expiry
    // and eviction must fail open rather than resurrect that old carrier's suppression.
    if (at < this.#now() - this.#horizon()) return false;
    if (through !== undefined && at <= through) return false;
    if (!claimed || this.#fallbacks.has(claim) || reservations?.has(claim)) return false;
    reservations?.add(claim);
    return true;
  }

  #consumeFallback(event: MeshEvent, reservations: ReadonlySet<string>): boolean {
    if (deliveryIdentity(event.data)) return false;
    const data = event.data && typeof event.data === "object" ? event.data as { ref?: unknown } : undefined;
    const key = workKey(event.data);
    const kind: "key" | "ref" = key === undefined ? "ref" : "key";
    const value = key ?? (typeof data?.ref === "string" && data.ref.trim() ? data.ref.trim() : undefined);
    if (value === undefined) return false;
    const claim = topicWorkReceipt(event.from.id, kind, event.topic, value);
    if (!reservations.has(claim) || this.#fallbacks.has(claim)) return false;
    this.#fallbacks.set(claim, this.#now());
    return true;
  }

  #remember(ids: Iterable<string>, times?: ReadonlyMap<string, number>, canonicalHistory = false): boolean {
    let changed = false;
    const cutoff = this.#now() - this.#horizon();
    for (const id of ids) {
      // Timestamp-less canonical history remains available through session.delivered for exact
      // identity replay checks, but must not be silently renewed into a key/ref claim.
      const recorded = times?.get(id);
      if (canonicalHistory && !Number.isFinite(recorded)) continue;
      const at = Number.isFinite(recorded) ? recorded! : this.#delivered.get(id) ?? this.#now();
      if (at < cutoff) continue;
      const previous = this.#delivered.get(id);
      if (previous === undefined || at > previous) {
        this.#delivered.set(id, at);
        changed = true;
      }
    }
    return changed;
  }

  /** Oldest first, by canonical receipt time, not the most recent history scan. */
  #trimReceipts(): boolean {
    const before = this.#delivered.size + this.#fallbacks.size;
    const cutoff = this.#now() - this.#horizon();
    const state = this.#state!;
    const retire = (id: string, at: number): void => {
      // A restored canonical claim can be newer than its spent marker (clock skew or
      // an undated marker). Retiring only the marker's age would revive that claim.
      state.fallbackThrough = Math.max(state.fallbackThrough ?? Number.NEGATIVE_INFINITY, at,
        this.#delivered.get(id) ?? Number.NEGATIVE_INFINITY);
    };
    for (const [id, at] of this.#fallbacks) if (at < cutoff) {
      retire(id, at);
      this.#fallbacks.delete(id);
    }
    const all: Array<["delivered" | "fallback", string, number]> = [
      ...[...this.#delivered].filter(([, at]) => at >= cutoff).map(([id, at]): ["delivered", string, number] => ["delivered", id, at]),
      ...[...this.#fallbacks].map(([id, at]): ["fallback", string, number] => ["fallback", id, at]),
    ].sort((a, b) => a[2] - b[2]);
    const entries = all.slice(-MAX_DELIVERED_RECEIPTS);
    // Hashes do not expose receipt kind. Retire every evicted timestamp conservatively;
    // the watermark affects only legacy fallback, never the exact-ID checks above it.
    for (const [, id, at] of all.slice(0, -MAX_DELIVERED_RECEIPTS)) retire(id, at);
    const baseThrough = state.fallbackThrough;
    const assign = (offset: number): void => {
      if (baseThrough === undefined) delete state.fallbackThrough;
      else state.fallbackThrough = baseThrough;
      const kept = entries.slice(offset);
      const delivered = kept.filter(([kind]) => kind === "delivered");
      const fallbacks = kept.filter(([kind]) => kind === "fallback");
      state.delivered = delivered.map(([, id]) => id);
      state.deliveredAt = delivered.map(([, , at]) => at);
      state.fallbacks = fallbacks.map(([, id]) => id);
      state.fallbackAt = fallbacks.map(([, , at]) => at);
      // Include the final watermark in every byte-budget candidate, not after sizing it.
      for (const [, id, at] of entries.slice(0, offset)) retire(id, at);
    };
    // Account for the actual cursor and pending IDs too. Byte-size (not count) protects
    // configured small mesh values; count also caps memory at the default/larger limit.
    const budget = Math.floor(this.mesh.maxEventBytes * RECEIPT_VALUE_FRACTION);
    let low = 0, high = entries.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      assign(middle);
      if (Buffer.byteLength(JSON.stringify(state), "utf8") >= budget) low = middle + 1;
      else high = middle;
    }
    assign(low);
    const kept = entries.slice(low);
    this.#delivered = new Map(kept.filter(([kind]) => kind === "delivered").map(([, id, at]) => [id, at]));
    this.#fallbacks = new Map(kept.filter(([kind]) => kind === "fallback").map(([, id, at]) => [id, at]));
    return before !== kept.length;
  }

  #horizon(): number { return this.options.horizonMs ?? inboxHorizonMs(); }

  #neverExpire(event: MeshEvent): boolean { return event.to !== undefined && NEVER_EXPIRE_KINDS.has(event.kind); }

  #stale(event: MeshEvent): boolean {
    return event.createdAt < this.#now() - this.#horizon() && !this.#neverExpire(event);
  }

  #late(event: MeshEvent): RootInboxEvent {
    const age = this.#now() - event.createdAt;
    if (age <= this.#horizon() || !this.#neverExpire(event)) return event;
    const hours = Math.floor(age / LATE_DELIVERY_HOUR_MS);
    return { ...event, deliveredLate: true, text: `(delivered late: created ${new Date(event.createdAt).toISOString()}, ${hours} h ago) ${event.text ?? ""}` };
  }

  #reread(after: number, pending: NonNullable<RootInboxState["pending"]>): MeshEvent[] {
    const ids = new Set(pending.ids);
    const found: MeshEvent[] = [];
    const pageSize = this.options.pageSize ?? 500;
    for (let cursor = after; cursor < pending.through;) {
      const page = this.mesh.read({ after: cursor, limit: pageSize });
      if (page.length === 0) break;
      for (const event of page) {
        if (event.sequence > pending.through) return found;
        if (ids.delete(event.id)) found.push(event);
        cursor = Math.max(cursor, event.sequence);
      }
      if (page.length < pageSize) break;
    }
    return found;
  }

  async #alarmPending(state: RootInboxState): Promise<void> {
    const pending = state.pending!;
    const heldMs = this.#now() - pending.since;
    if (pending.alarmed || heldMs <= PENDING_ALARM_MS) return;
    // A publication receipt also deduplicates a reload after publish but before cursor save.
    await this.mesh.publish({
      topic: ROOT_INBOX_ALARM_TOPIC, kind: "alarm", from: this.identity,
      dedupeKey: `root-inbox-alarm:${this.identity.id}:${state.after}:${pending.through}`,
      data: { rootId: this.identity.id, pending: { after: state.after, through: pending.through, ids: pending.ids }, heldMs },
    });
    pending.alarmed = true;
  }

  #load(): RootInboxState {
    if (this.#state) return this.#state;
    const stored = this.mesh.get(this.key);
    const value = stored?.value as Partial<RootInboxState> | undefined;
    const saved = typeof value?.after === "number" && Number.isSafeInteger(value.after);
    const pending = value?.pending;
    // A root with no cursor starts at the present: the inbox is for what it misses from now on.
    this.#state = {
      after: saved ? value!.after! : this.#initialAfter ?? this.mesh.latestSequence(),
      ...(value?.nativeRecoveryPark ? { nativeRecoveryPark: value.nativeRecoveryPark } : {}),
      ...(Array.isArray(value?.delivered) ? { delivered: value.delivered.filter((id): id is string => typeof id === "string") } : {}),
      ...(Array.isArray(value?.fallbacks) ? { fallbacks: value.fallbacks.filter((id): id is string => typeof id === "string") } : {}),
      ...(pending && Array.isArray(pending.ids) && typeof pending.through === "number"
        ? { pending: { through: pending.through, ids: pending.ids.filter((id): id is string => typeof id === "string"),
          since: typeof pending.since === "number" && Number.isFinite(pending.since) ? pending.since : stored?.updatedAt ?? this.#now(),
          ...(pending.alarmed === true ? { alarmed: true } : {}),
        } }
        : {}),
      ...(saved && sequenceOf(value!.ackedThrough) > value!.after! ? { ackedThrough: value!.ackedThrough! } : {}),
    };
    this.#acks = new Map(parseAcks(value?.acked));
    this.#syncAcks();
    // Missing legacy timestamps are intentionally not upgraded to read/load time: a key/ref
    // claim without finite observation time must fail open. Exact canonical identity checks still
    // use the session receipt set directly when its entry is available.
    this.#delivered = new Map((this.#state.delivered ?? []).flatMap((id, index) => {
      const at = value?.deliveredAt?.[index];
      return typeof at === "number" && Number.isFinite(at) && Number.isInteger(at) && at > 0 && at <= this.#now() + 60_000
        ? [[id, at] as [string, number]] : [];
    }));
    // Fallbacks are spent markers, not delivery proof. Keep every string marker, even
    // with a missing/malformed age: dropping or immediately expiring it could resurrect
    // a fresh key/ref claim. Future ages are intentionally retained. Invalid ages use
    // the claim's valid observation time (or now) only for retention/eviction bookkeeping.
    this.#fallbacks = new Map((Array.isArray(value?.fallbacks) ? value.fallbacks : []).flatMap((id, index) => {
      if (typeof id !== "string") return [];
      const time = value?.fallbackAt?.[index];
      const at = typeof time === "number" && Number.isFinite(time) && Number.isInteger(time) && time > 0
        ? time : this.#delivered.get(id) ?? this.#now();
      return [[id, at] as [string, number]];
    }));
    if (typeof value?.fallbackThrough === "number" && Number.isFinite(value.fallbackThrough)) {
      this.#state.fallbackThrough = value.fallbackThrough;
    }
    this.#saved = saved ? JSON.stringify(value) : undefined;
    this.#savedImmediate = saved ? immediateSnapshot(value) : undefined;
    this.#trimReceipts();
    this.#savedAt = saved ? this.#now() : 0;
    return this.#state;
  }

  async #save(now: boolean): Promise<void> {
    const work = this.#writes.then(() => this.#saveState(now));
    this.#writes = work.catch(() => {}); // A failed save stays dirty, but a later explicit retry may succeed.
    await work;
  }

  async #saveState(now: boolean): Promise<void> {
    this.#absorb();
    this.#trimReceipts();
    const text = JSON.stringify(this.#state);
    if (text === this.#saved) return;
    if (!now && this.#now() - this.#savedAt < SAVE_INTERVAL_MS) return;
    const immediate = immediateSnapshot(this.#state);
    await this.mesh.put({ key: this.key, value: JSON.parse(text) as RootInboxState, identity: this.identity });
    this.#saved = text;
    this.#savedImmediate = immediate;
    this.#savedAt = this.#now();
  }

  #now(): number {
    return this.options.now?.() ?? Date.now();
  }
}
