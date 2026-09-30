import { randomUUID } from "node:crypto";
import fs from "node:fs";
import { readFileRetrying, writeFileAtomic } from "./core/atomic-write.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "./mesh/store.js";

const MAIN_AGENT_ALIAS = "main";
export type FabricAgentMessageDelivery = "steer" | "followUp";
/** How a direct agent message goes to Pi: a nextTurn one waits for the next user prompt. */
export type FabricMainAgentDelivery = FabricAgentMessageDelivery | "nextTurn";
const DIRECT_DELIVERIES: ReadonlySet<unknown> = new Set(["steer", "followUp", "nextTurn"]);

export interface FabricMainAgentInfo {
  id: string;
  name: "Main";
  kind: "main";
  status: "idle" | "running" | "remote";
  runner: "pi";
  transport: "host";
  cwd?: string;
  sessionId?: string;
  model?: string;
  thinking?: string;
  startedAt?: number;
  updatedAt: number;
  pendingMessages: boolean;
  local: boolean;
}

export interface FabricMainAgentDeliveryRequest {
  from: MeshIdentity;
  message: string;
  delivery: FabricMainAgentDelivery;
  triggerTurn?: boolean;
  data?: unknown;
  /**
   * A stable id of the sender's durable record (a resident actor's delivery record). Main journals
   * the message under it before it returns and admits each id once, across restarts and reloads,
   * so the sender may delete its record after a return (review round 2 on pi-fabric#160).
   */
  deliveryId?: string;
}

/** How far behind a Main is on followUps, so a sender can switch to steer (smarty-dev#1495). */
export interface FabricFollowUpQueueDepth {
  pendingFollowUps: number;
  oldestAgeS: number;
}

export interface FabricAgentMessageResult extends Partial<FabricFollowUpQueueDepth> {
  /** Advisory identifier provenance notice; appended to delivered text when admission permits. */
  notice?: string;
  queued: true;
  messageId: string;
  routed: "local" | "main" | "mesh";
  acknowledged?: boolean;
  /** Main had already admitted this deliveryId; nothing was sent again. */
  duplicate?: true;
  /** This followUp replaced a held one with the same sender and data.coalesceKey. */
  coalesced?: true;
  /** The id of the held followUp it replaced; that one is never delivered. */
  replacedMessageId?: string;
  /** Main is idle but its held followUps are past mesh.followUpStallSeconds (smarty-dev#1826). */
  stalled?: true;
}

export interface FabricMainModelSwitchResult {
  ok: boolean;
  error?: string;
}

export interface FabricMainAgentTarget {
  readonly id: string;
  readonly local: boolean;
  matches(id: string): boolean;
  info(context?: ExtensionContext): FabricMainAgentInfo;
  deliverAgent(request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult;
  // Switch Main's live session model in place. Only local hosts hold the pi
  // extension session required for the mutation, so remote targets omit it.
  switchModel?(
    target: { provider: string; id: string },
    context: ExtensionContext,
  ): Promise<FabricMainModelSwitchResult>;
  // smarty-dev#2119: a capped Main wait returned; flush every held followUp at the next tool
  // boundary, whatever its age. Local Mains with a followUp drain only.
  flushHeldAtNextBoundary?(): void;
}

// Keep identity resolution available to existing callers without making startup import the drain.
export { resolveFabricIdentity, type FabricIdentityResolution } from "./main-agent-identity.js";

const escapeXmlText = (value: string): string =>
  value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

// Attribute values are quoted and escaped, so an identity field cannot close the header or
// forge a second envelope (dev-lead review of pi-fabric#102).
const escapeXmlAttribute = (value: string): string =>
  escapeXmlText(value).replaceAll('"', "&quot;").replaceAll("'", "&apos;");

/**
 * The sender as Main shows it: a missing or empty name becomes the id. A sender without an id
 * or kind is refused (undefined). A nameless sender held for a busy Main made every hand-over
 * throw and blocked the queue for 7 h (smarty-dev#1826).
 */
const senderIdentity = (from: unknown): MeshIdentity | undefined => {
  const value = from as Partial<MeshIdentity> | null | undefined;
  if (typeof value?.id !== "string" || !value.id.trim() || typeof value.kind !== "string" || !value.kind.trim()) return undefined;
  const name = typeof value.name === "string" && value.name.trim() ? value.name : value.id;
  return { ...structuredClone(value), name } as MeshIdentity;
};

/** Admission and batch bounds for followUps Fabric holds for a busy Main. */
export const FOLLOW_UP_LIMITS = {
  senderItems: 50,
  senderBytes: 256 * 1024,
  totalItems: 200,
  totalBytes: 1024 * 1024,
  /** Undeleted source ids travel with their carrier; never truncate them to admit a replacement. */
  ancestryIds: 1024,
  /**
   * ponytail: a soft target for the raw text of one delivered message. A single item larger
   * than this still goes alone (the per-sender byte quota bounds it); headers and escaping add
   * to the rendered size.
   */
  batchBytes: 64 * 1024,
} as const;

const serializableData = (value: unknown): unknown => {
  try {
    const serialized = JSON.stringify(value);
    return serialized === undefined ? undefined : JSON.parse(serialized) as unknown;
  } catch {
    return { fabricUnserializable: true };
  }
};

/** A followUp's `data.coalesceKey`: a non-empty string of at most 200 characters, or none. */
export const followUpCoalesceKey = (data: unknown): string | undefined => {
  const key = (data as { coalesceKey?: unknown } | null | undefined)?.coalesceKey;
  return typeof key === "string" && key.length > 0 && key.length <= 200 ? key : undefined;
};

interface HeldAgentMessage {
  id: string;
  from: MeshIdentity;
  message: string;
  /** The first send of a coalesced chain: it keeps the queue position and the flush wait. */
  sentAt: number;
  /** When the newest replacement arrived (smarty-dev#1495). */
  replacedAt?: number;
  /**
   * A replacement's chain: the first id, and its place in the chain. Only a held item is
   * replaced, so a handed or delivered member is its chain's last; the others never go.
   */
  chain?: string;
  generation?: number;
  /** The held id this one replaced. */
  replaces?: string;
  data?: unknown;
  /** The sender's stable delivery id (FabricMainAgentDeliveryRequest.deliveryId). */
  deliveryId?: string;
  /** Replaced delivery ids: retained with the live carrier even after consumed-id eviction. */
  supersedes?: string[];
  /** Handed to Pi's queue; it may still be there after a reload. Unset while Fabric holds it. */
  handed?: true;
  /**
   * A direct (never held) delivery's own policy, as the request gave it. A replay sends it alone
   * with this policy: recovery never turns a passive message into a triggering one (security
   * round 3 S2 on pi-fabric#160). Unset for held followUps, which Fabric releases.
   */
  deliverAs?: FabricMainAgentDelivery;
  triggerTurn?: boolean;
}

/** The ids of the agent messages in a list of messages (Pi's pending queue at a boundary). */
const agentMessageIds = (messages: readonly unknown[] | undefined): Set<string> => {
  const ids = new Set<string>();
  for (const message of messages ?? []) {
    const value = message as { customType?: string; details?: { id?: unknown; items?: unknown } };
    if (value?.customType !== "pi-fabric-agent-message") continue;
    if (typeof value.details?.id === "string") ids.add(value.details.id);
    if (Array.isArray(value.details?.items)) {
      for (const item of value.details.items as Array<{ id?: unknown }>) if (typeof item?.id === "string") ids.add(item.id);
    }
  }
  return ids;
};

type BoundaryEvent = { context?: { pendingMessages?: readonly unknown[] } };

/** One item's envelope as Main reads it. Throws on an item it cannot render. */
const agentMessageBlock = (item: HeldAgentMessage, delivery: FabricMainAgentDelivery): string => [
  `<fabric-agent-message from_name="${escapeXmlAttribute(item.from.name)}" from_id="${escapeXmlAttribute(item.from.id)}" from_kind="${escapeXmlAttribute(item.from.kind)}" delivery="${escapeXmlAttribute(delivery)}" sent_at="${escapeXmlAttribute(new Date(item.sentAt).toISOString())}"${item.replacedAt === undefined ? "" : ` replaced_at="${escapeXmlAttribute(new Date(item.replacedAt).toISOString())}"`}>`,
  escapeXmlText(item.message),
  item.data === undefined ? undefined : `<data>${escapeXmlText(JSON.stringify(item.data))}</data>`,
  "</fabric-agent-message>",
].filter((line): line is string => Boolean(line)).join("\n");

const itemBytes = (item: HeldAgentMessage): number =>
  Buffer.byteLength(item.message) + (item.data === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.data))) +
  (item.deliveryId === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.deliveryId))) +
  (item.supersedes === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.supersedes)));

type SessionEntryLike = { id?: unknown; parentId?: unknown; type?: string; customType?: string; details?: { id?: unknown; items?: unknown } };

/** How many consumed delivery ids Main remembers beside its journal, newest last. */
const CONSUMED_DELIVERIES_MAX = 2000;
const deliveryKey = (deliveryId: string): string => `delivery:${deliveryId}`;

/** Add the followUp ids a persisted agent-message entry carries. */
const addDelivered = (ids: Set<string>, entry: SessionEntryLike | undefined): void => {
  if (entry?.type !== "custom_message" || entry.customType !== "pi-fabric-agent-message") return;
  // A delivered replacement's chain counts as delivered too: its earlier members never go.
  for (const item of [entry.details, ...(Array.isArray(entry.details?.items) ? entry.details.items : [])] as Array<{ id?: unknown; chain?: unknown; deliveryId?: unknown; supersedes?: unknown } | undefined>) {
    if (typeof item?.id === "string") ids.add(item.id);
    if (typeof item?.chain === "string") ids.add(item.chain);
    if (typeof item?.deliveryId === "string") ids.add(deliveryKey(item.deliveryId));
    if (Array.isArray(item?.supersedes)) {
      for (const id of item.supersedes) if (typeof id === "string") ids.add(deliveryKey(id));
    }
  }
};

export class MainAgentController implements FabricMainAgentTarget {
  readonly startedAt = Date.now();
  readonly #held: HeldAgentMessage[] = [];
  // Handed to Pi, not yet in the session's entries. Kept in the journal until they are.
  readonly #sent: HeldAgentMessage[] = [];
  // Replayed handoffs of an earlier controller: in Pi's queue after a live reload, lost after a
  // restart. Only a boundary shows Pi's queue, so they wait for one (#reconcile).
  readonly #unverified: HeldAgentMessage[] = [];
  // Every followUp id the session holds, on every branch, flushed or not: indexed from Pi's
  // append-only entry list, never from the active branch (dev-lead on pi-fabric#102).
  // ponytail: memory is O(unique ids + one id per entry scanned once), about 74 MiB at 1M ids;
  // the list is Pi's own, already in memory, so nothing is re-read from disk.
  #delivered = new Set<string>();
  // What #delivered indexes: the in-memory entry list ("") or a session file path. #scanned counts
  // entries of the former, bytes of the latter.
  #source: string | undefined;
  #scanned: number | undefined;
  #journal: string | undefined;
  // Delivery ids whose message the session holds, or that will never go (replaced, dropped):
  // persisted beside the journal, bounded, oldest first. This index also retains the owner halt
  // after the message journal is empty. With the journal's own items they make
  // deliverAgent idempotent by deliveryId across restarts and release reloads.
  #consumed = new Set<string>();
  #consumedDirty = false;
  readonly #unsubscribe: Array<() => void> = [];
  #context: ExtensionContext | undefined;
  #flushMs = 0;
  #flushAll = false;
  #stallS = 600;
  #suspended = false;
  // Owner stop, unlike a run's signal: survives reload and lifts only on user input.
  #halted = false;
  // Provider failures suppress wakes, but are not owner stops: native recovery lifts this gate.
  #providerFailed = false;
  // Preserve an unreadable owner index until explicit user input authorizes replacing it.
  #haltIndexUnknown = false;
  #closed = false;
  #reloading = false;
  #wake: ReturnType<typeof setInterval> | undefined;
  #operation: AbortSignal | undefined;

  constructor(
    readonly pi: ExtensionAPI,
    readonly id: string,
    readonly local: boolean,
    readonly cwd: string,
    readonly sessionId?: string,
  ) {}

  matches(id: string): boolean {
    const target = id.trim();
    return target === MAIN_AGENT_ALIAS || target === this.id;
  }

  info(context?: ExtensionContext): FabricMainAgentInfo {
    const model =
      this.local && context?.model
        ? `${context.model.provider}/${context.model.id}`
        : undefined;
    const thinking = this.local ? this.pi.getThinkingLevel() : undefined;
    return {
      id: this.id,
      name: "Main",
      kind: "main",
      status: this.local ? (context?.isIdle() === false ? "running" : "idle") : "remote",
      runner: "pi",
      transport: "host",
      ...(this.local ? { cwd: this.cwd, startedAt: this.startedAt } : {}),
      ...(this.sessionId ? { sessionId: this.sessionId } : {}),
      ...(model ? { model } : {}),
      ...(thinking ? { thinking } : {}),
      updatedAt: Date.now(),
      pendingMessages: this.local ? (context?.hasPendingMessages() ?? false) : false,
      local: this.local,
    };
  }

  async switchModel(
    target: { provider: string; id: string },
    context: ExtensionContext,
  ): Promise<FabricMainModelSwitchResult> {
    if (!this.local) {
      return { ok: false, error: `Main agent ${this.id} is owned by another Fabric process` };
    }
    const key = `${target.provider}/${target.id}`;
    const model = context.modelRegistry.find(target.provider, target.id);
    if (!model) return { ok: false, error: `Model is not available: ${key}` };
    const switched = await this.pi.setModel(model);
    if (!switched) return { ok: false, error: `No authentication configured for model: ${key}` };
    return { ok: true };
  }

  deliverUser(message: string, delivery: FabricAgentMessageDelivery): FabricAgentMessageResult {
    if (!this.local) throw new Error(`Main agent ${this.id} is owned by another Fabric process`);
    const text = message.trim();
    if (!text) throw new Error("Main agent message must not be empty");
    const messageId = randomUUID();
    this.pi.sendUserMessage(text, { deliverAs: delivery });
    return { queued: true, messageId, routed: "main" };
  }

  /** No more Pi handoffs once reload starts; an already-admitted control command journals only. */
  prepareReload(): void {
    this.#reloading = true;
    this.#stopWake();
  }

  /** Escape can halt an idle Main without producing an aborted run event. */
  halt(): void {
    this.#halted = true;
    this.#consumedDirty = true;
    this.#stopWake();
    this.#trySave();
  }

  deliverAgent(request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult {
    if (!this.local) throw new Error(`Main agent ${this.id} is owned by another Fabric process`);
    const message = request.message.trim();
    if (!message) throw new Error("Main agent message must not be empty");
    const sender = senderIdentity(request.from);
    if (!sender) throw new Error("Main agent message needs a sender with a string id and kind");
    const deliveryId = typeof request.deliveryId === "string" && request.deliveryId ? request.deliveryId : undefined;
    if (deliveryId !== undefined) {
      // A durable id needs the journal: without one (closed at shutdown or reload, or never
      // opened) the sender keeps its record for a later drain (Astra round 3 finding 2, pi-fabric#160).
      if (!this.#journal) throw new Error("Main has no follow-up journal open; retry the durable delivery later");
      const admitted = this.#admitted(deliveryId);
      if (admitted) return { queued: true, messageId: admitted, routed: "main", duplicate: true };
    }
    const item: HeldAgentMessage = {
      id: randomUUID(),
      from: sender,
      message,
      sentAt: Date.now(),
      ...(request.data === undefined ? {} : { data: serializableData(request.data) }),
      ...(deliveryId === undefined ? {} : { deliveryId }),
    };
    const triggerTurn = (request.triggerTurn ?? true) && !this.#halted && !this.#providerFailed;
    if (this.#reloading) {
      if (!this.#journal) throw new Error("Main has no follow-up journal open; retry after reload");
      item.deliverAs = request.delivery;
      item.triggerTurn = triggerTurn;
      this.#admit(item);
      this.#held.push(item);
      try { this.#save(); } catch (error) { this.#held.pop(); throw error; }
      return { queued: true, messageId: item.id, routed: "main", ...this.queueDepth(item.from.id) };
    }
    let replaced: HeldAgentMessage | undefined;
    // Pi releases its own followUp queue only when Main has no more work, so a Main that
    // chains turns reads it an hour late (smarty-dev#1495). Fabric holds a triggering
    // followUp for a busy Main instead: turn_end flushes the due ones as one steer, and
    // agent_settled releases the rest as a followUp. A non-triggering one never waited.
    const held = request.delivery === "followUp" && triggerTurn && this.#drainActive();
    if (held) {
      // smarty-dev#1495: replace a held same-sender/key followUp in place with the newest.
      // Reload can also hold direct deliveries: never consume one with a different mode or
      // trigger policy (pi-fabric#184 R1). Ordinary held followUps mean followUp/true.
      const key = followUpCoalesceKey(item.data);
      const index = key === undefined ? -1 : this.#held.findIndex((held) =>
        held.from.id === item.from.id && held.from.kind === item.from.kind && followUpCoalesceKey(held.data) === key &&
        (held.deliverAs ?? "followUp") === request.delivery && (held.triggerTurn ?? true) === triggerTurn);
      replaced = index < 0 ? undefined : this.#held[index];
      if (replaced) {
        // A compatible journalled replay still goes at its original boundary and policy.
        if (replaced.deliverAs !== undefined) {
          item.deliverAs = replaced.deliverAs;
          item.triggerTurn = replaced.triggerTurn ?? true;
        }
        item.sentAt = replaced.sentAt;
        item.replacedAt = Date.now();
        item.chain = replaced.chain ?? replaced.id;
        item.generation = (replaced.generation ?? 0) + 1;
        item.replaces = replaced.id;
        // smarty-dev#2339 F2: a failed mesh delete can outlive the last 2000 consumed ids.
        // The replacement journals every superseded delivery id until the carrier is consumed.
        item.supersedes = [...(replaced.supersedes ?? []), ...(replaced.deliveryId === undefined ? [] : [replaced.deliveryId])];
      }
      // Charge the proposed ancestry before mutating or acknowledging either carrier.
      this.#admit(item, replaced);
      if (replaced) this.#held[index] = item;
      else this.#held.push(item);
      // The replaced id is recorded as consumed in the same save, before the return: its sender's
      // record, if kept, is then refused after a restart (Astra round 3 finding 4, pi-fabric#160).
      const consumed = replaced ? new Set(this.#consumed) : this.#consumed;
      const consumedDirty = this.#consumedDirty;
      if (replaced) this.#consume(replaced);
      try {
        this.#save();                                   // journalled before it is acknowledged
      } catch (error) {
        if (replaced) this.#held[index] = replaced;
        else this.#held.pop();
        this.#consumed = consumed;
        this.#consumedDirty = consumedDirty;
        // The consumed file may already hold the replaced chain, which the journal still lists.
        if (replaced) { this.#consumedDirty = true; this.#trySave(); }
        throw new Error(`Main could not record the followUp: ${error instanceof Error ? error.message : String(error)}`);
      }
      if (this.#context!.isIdle()) this.#release(true);
    } else if (deliveryId !== undefined) {
      // A sent message may wait in Pi's volatile queue (prompt preflight, a settle): it stays in
      // the journal until the session holds it, and a restart replays it (#confirm, #replay).
      item.handed = true;
      item.deliverAs = request.delivery;
      item.triggerTurn = triggerTurn;
      this.#sent.push(item);
      try {
        this.#save();
      } catch (error) {
        this.#sent.pop();
        throw new Error(`Main could not record the message: ${error instanceof Error ? error.message : String(error)}`);
      }
      try {
        this.#send([item], request.delivery, triggerTurn, false);
      } catch (error) {
        this.#sent.splice(this.#sent.indexOf(item), 1);
        this.#trySave();
        throw error;
      }
    } else {
      this.#send([item], request.delivery, triggerTurn, false);
    }
    return {
      queued: true, messageId: item.id, routed: "main",
      ...(replaced ? { coalesced: true as const, replacedMessageId: replaced.id } : {}),
      // Only a followUp that waits in the held queue can be stalled; a non-triggering one went
      // straight to Pi (#123 review F1).
      ...(held ? this.#depthReport(item.from.id) : this.queueDepth(item.from.id)),
    };
  }

  /**
   * The followUps Fabric still holds for this Main, and the age of the oldest: the given
   * sender's own, or all of them.
   */
  queueDepth(fromId?: string): FabricFollowUpQueueDepth {
    // ponytail: counts only what Fabric holds. With the drain off, followUps sit in Pi's own
    // queue, which exposes no depth to extensions, and this reports 0.
    const mine = fromId === undefined ? this.#held : this.#held.filter((item) => item.from.id === fromId);
    const oldest = mine[0];
    return {
      pendingFollowUps: mine.length,
      oldestAgeS: oldest ? Math.max(0, Math.floor((Date.now() - oldest.sentAt) / 1000)) : 0,
    };
  }

  /**
   * The sender's queue depth, and `stalled` when Main is idle and its oldest held followUp, from
   * any sender, is past the stall threshold: no boundary will release the queue, which goes oldest
   * first, so every sender's items wait behind that one (smarty-dev#1826: one malformed item held
   * 4 to 8 followUps for 5.5 h, oldest 20520 s, and every sender saw an ack).
   */
  #depthReport(fromId: string): FabricFollowUpQueueDepth & { stalled?: true } {
    const depth = this.queueDepth(fromId);
    // ponytail: only an idle Main counts. A long busy turn legitimately holds items until its next
    // boundary, so their age alone says nothing about a stuck queue.
    const stalled = this.#stallS > 0 && this.#context?.isIdle() === true && this.queueDepth().oldestAgeS >= this.#stallS &&
      this.#held.length > 0;
    return stalled ? { ...depth, stalled: true } : depth;
  }

  /** Quotas for a new held item; one that replaces a held item frees that item's share first. */
  #admit(item: HeldAgentMessage, replacing?: HeldAgentMessage): void {
    const limits = FOLLOW_UP_LIMITS;
    const bytes = itemBytes(item);
    const held = replacing ? this.#held.filter((other) => other !== replacing) : this.#held;
    const mine = held.filter((other) => other.from.id === item.from.id);
    const mineBytes = mine.reduce((sum, other) => sum + itemBytes(other), 0);
    const totalBytes = held.reduce((sum, other) => sum + itemBytes(other), 0);
    const full = (what: string) => new Error(
      `Main's followUp queue is full (${what}); Main is busy and reads followUps only at its next tool boundary. ` +
        "Wait, or send a short steer.",
    );
    if ((item.supersedes?.length ?? 0) > limits.ancestryIds) throw full("replacement ancestry limit");
    if (mine.length + 1 > limits.senderItems || mineBytes + bytes > limits.senderBytes) {
      throw full(`${mine.length} of yours, ${mineBytes} bytes; limit ${limits.senderItems} or ${limits.senderBytes} bytes per sender`);
    }
    if (held.length + 1 > limits.totalItems || totalBytes + bytes > limits.totalBytes) {
      throw full(`${held.length} held, ${totalBytes} bytes; limit ${limits.totalItems} or ${limits.totalBytes} bytes in total`);
    }
  }

  /** The message id under which Main already admitted this delivery id, if it did. */
  #admitted(deliveryId: string): string | undefined {
    const item = [...this.#unverified, ...this.#sent, ...this.#held].find((item) =>
      item.deliveryId === deliveryId || item.supersedes?.includes(deliveryId));
    if (item) return item.id;
    if (this.#consumed.has(deliveryId) || this.#refreshDelivered().has(deliveryKey(deliveryId))) return deliveryId;
    return undefined;
  }

  /** Remember that this item's delivery id and its superseded ids are done. */
  #consume(item: HeldAgentMessage): void {
    const ids = [...(item.supersedes ?? []), ...(item.deliveryId === undefined ? [] : [item.deliveryId])];
    if (!ids.length) return;
    for (const id of ids) {
      this.#consumed.delete(id);
      this.#consumed.add(id);
    }
    while (this.#consumed.size > CONSUMED_DELIVERIES_MAX) this.#consumed.delete(this.#consumed.values().next().value!);
    this.#consumedDirty = true;
  }

  #consumedPath(): string | undefined {
    return this.#journal ? `${this.#journal}.delivered` : undefined;
  }

  /** Write the held and unconfirmed followUps (0600), or remove the journal when none are left. */
  #save(): void {
    if (!this.#journal) return;
    if (this.#consumedDirty && !this.#haltIndexUnknown) {
      // Before the journal: an item leaves the journal only once its id is recorded as consumed.
      const file = this.#consumedPath()!;
      writeFileAtomic(file, JSON.stringify({ version: 1, ids: [...this.#consumed], ...(this.#halted ? { halted: true } : {}) }), { durable: true });
      this.#consumedDirty = false;
    }
    const items = [...this.#unverified, ...this.#sent, ...this.#held];
    if (!items.length) { fs.rmSync(this.#journal, { force: true }); return; }
    writeFileAtomic(this.#journal, JSON.stringify({ version: 1, items }), { durable: true });
  }

  #trySave(): void {
    try { this.#save(); } catch {
      // The previous journal still lists every item; the next change writes it again.
    }
  }

  /**
   * Index the session entries persisted since the last call, whatever branch they are on.
   *
   * With a session file, only what the file holds counts: Pi inserts an entry in memory before it
   * writes it, and does not roll it back when the write fails, so getEntries() is no receipt
   * (security round 3 S1 on pi-fabric#160). Pi also defers a new session's first write until its
   * first assistant message; until then nothing is confirmed and every item stays journalled.
   * The file only grows within a session; a shorter file or another path starts over.
   *
   * An in-memory session has no durable store at all: its entry list is the only record, so it
   * counts as it did before.
   */
  #refreshDelivered(): Set<string> {
    try {
      const manager = this.#context?.sessionManager as
        { getEntries?: () => readonly unknown[]; getSessionFile?: () => string | undefined; isPersisted?: () => boolean } | undefined;
      const file = manager?.isPersisted?.() === false ? undefined : manager?.getSessionFile?.();
      if (file) { this.#indexSessionFile(file); return this.#delivered; }
      const entries = manager?.getEntries?.() ?? [];
      if (this.#source !== "" || this.#scanned === undefined || entries.length < this.#scanned) this.#restartIndex("");
      for (let index = this.#scanned!; index < entries.length; index++) addDelivered(this.#delivered, entries[index] as SessionEntryLike);
      this.#scanned = entries.length;
    } catch {
      // A failed session barrier supplies no receipt: keep every journalled payload.
      return new Set();
    }
    return this.#delivered;
  }

  #restartIndex(source: string): void {
    this.#delivered = new Set();
    this.#source = source;
    this.#scanned = 0;
  }

  /** Read the complete lines appended to the session file since the last call, in 1 MiB chunks. */
  #indexSessionFile(file: string): void {
    let fd: number;
    try {
      // ponytail: Windows' FlushFileBuffers (fsyncSync) needs a writable handle; this code never writes through it.
      fd = fs.openSync(file, process.platform === "win32" ? "r+" : "r");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      if (this.#source !== file) this.#restartIndex(file);    // not written yet: nothing persisted
      return;
    }
    try {
      const size = fs.fstatSync(fd).size;
      // Reading a complete line is not a stable-storage receipt. Sync before indexing new
      // bytes (including on restart); a failure leaves the index and journal untouched.
      if (this.#source !== file || this.#scanned === undefined || size !== this.#scanned) fs.fsyncSync(fd);
      if (this.#source !== file || this.#scanned === undefined || size < this.#scanned) this.#restartIndex(file);
      const buffer = Buffer.allocUnsafe(1 << 20);
      let position = this.#scanned!;
      let carry: Buffer[] = [];
      while (position < size) {
        const read = fs.readSync(fd, buffer, 0, Math.min(buffer.length, size - position), position);
        if (read <= 0) break;
        const view = buffer.subarray(0, read);
        let start = 0;
        for (let newline = view.indexOf(10); newline !== -1; newline = view.indexOf(10, start)) {
          const line = Buffer.concat([...carry, view.subarray(start, newline)]).toString("utf8");
          carry = [];
          start = newline + 1;
          // ponytail: only lines naming the custom type are parsed.
          if (line.includes("pi-fabric-agent-message")) {
            try { addDelivered(this.#delivered, JSON.parse(line) as SessionEntryLike); } catch { /* a torn line */ }
          }
        }
        position += read;
        if (start < read) carry.push(Buffer.from(view.subarray(start)));
        // A partial last line is read again next time, once it is complete.
        this.#scanned = position - carry.reduce((sum, part) => sum + part.length, 0);
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  /** The followUp ids in Pi's in-memory entry list, persisted or not. */
  #inMemory(): Set<string> {
    const ids = new Set<string>();
    try {
      for (const entry of this.#context?.sessionManager?.getEntries?.() ?? []) addDelivered(ids, entry as SessionEntryLike);
    } catch {
      // none known
    }
    return ids;
  }

  /**
   * At a boundary, where Pi shows its pending queue: a replayed handoff the session holds is
   * done, one still in Pi's queue awaits the session, and any other was lost and is held again.
   */
  #reconcile(event: BoundaryEvent): void {
    const pending = event.context?.pendingMessages;
    if (!this.#unverified.length || !Array.isArray(pending)) return;
    const queued = agentMessageIds(pending);
    const delivered = this.#refreshDelivered();
    // In Pi's memory but not on disk (a failed write, a live reload): Main has read it, so it is
    // not sent again in this process; it stays journalled and a restart replays it.
    const seen = this.#source !== "" && this.#unverified.some((item) => !delivered.has(item.id)) ? this.#inMemory() : new Set<string>();
    for (const item of this.#unverified.splice(0)) {
      if (delivered.has(item.id)) { this.#consume(item); continue; }
      if (queued.has(item.id) || seen.has(item.id)) this.#sent.push(item);
      else if (item.deliverAs !== undefined) {
        // A direct delivery goes again alone, with its own mode and triggerTurn; a failed send
        // waits for the next boundary.
        try {
          this.#send([item], item.deliverAs, item.triggerTurn ?? true, false);
          this.#sent.push(item);
        } catch {
          this.#unverified.push(item);
        }
      } else {
        const { handed: _handed, ...held } = item;
        this.#held.push(held);
      }
    }
    this.#held.sort((a, b) => a.sentAt - b.sentAt);
    this.#trySave();
  }

  /** Drop the handed-over followUps the session now holds (the only way one leaves the journal). */
  #confirm(): void {
    if (!this.#sent.length) return;
    const delivered = this.#refreshDelivered();
    const before = this.#sent.length;
    const kept = this.#sent.filter((item) => !delivered.has(item.id));
    for (const item of this.#sent) if (delivered.has(item.id)) this.#consume(item);
    this.#sent.splice(0, this.#sent.length, ...kept);
    if (this.#sent.length !== before) this.#trySave();
  }

  /** After a restart: every journalled followUp the session does not hold goes back in the queue. */
  #replay(): void {
    if (!this.#journal) return;
    try {
      const parsed = JSON.parse(readFileRetrying(this.#consumedPath()!)) as { version?: unknown; ids?: unknown; halted?: unknown } | null;
      if (!parsed || parsed.version !== 1 || !Array.isArray(parsed.ids) ||
        parsed.ids.some((id) => typeof id !== "string") ||
        (parsed.halted !== undefined && typeof parsed.halted !== "boolean")) throw new Error("Malformed halt index");
      this.#halted ||= parsed.halted === true;
      for (const id of parsed.ids.slice(-CONSUMED_DELIVERIES_MAX)) this.#consumed.add(id);
    } catch (error) {
      // Only absence proves there is no persisted owner stop. Unknown authority fails closed,
      // including when the payload journal is absent; incoming control messages stay passive.
      if ((error as { code?: unknown } | null)?.code !== "ENOENT") {
        this.#halted = true;
        this.#haltIndexUnknown = true;
        console.warn(`[pi-fabric] cannot read halt index ${this.#consumedPath()}; keeping Main halted until user input`, error);
      }
    }
    let items: HeldAgentMessage[] = [];
    try {
      const parsed = JSON.parse(fs.readFileSync(this.#journal, "utf8")) as { items?: unknown };
      if (Array.isArray(parsed.items)) {
        // An older runtime journalled nameless senders (smarty-dev#1826): they go under their id.
        for (const item of parsed.items as HeldAgentMessage[]) {
          const sender = senderIdentity(item?.from);
          if (typeof item?.id !== "string" || typeof item.message !== "string" || typeof item.sentAt !== "number" || !sender) {
            console.warn(`[pi-fabric] dropped a malformed followUp from the journal: ${String((item as { id?: unknown } | null)?.id)}`);
            continue;
          }
          // A policy this runtime cannot read is dropped: the item is then released as a held
          // followUp, as before policies were journalled.
          const { deliverAs, triggerTurn, supersedes, ...rest } = item;
          items.push({
            ...rest, from: sender,
            ...(Array.isArray(supersedes) ? { supersedes: supersedes.filter((id) => typeof id === "string") } : {}),
            ...(DIRECT_DELIVERIES.has(deliverAs) && typeof triggerTurn === "boolean" ? { deliverAs, triggerTurn } : {}),
          });
        }
      }
    } catch {
      return;                                            // no journal
    }
    // The owner rule (dev-lead on pi-fabric#102): skip every id the session holds; a handoff
    // may still sit in Pi's queue, so it waits for a boundary to check; the rest is held again.
    const delivered = this.#refreshDelivered();
    // One member per replacement chain survives, the latest (a handed one is its chain's last),
    // and none once the session holds one. So no replaced id comes back, however long the burst
    // (review/astra F1 on pi-fabric#114).
    const chainOf = (item: HeldAgentMessage) => typeof item.chain === "string" ? item.chain : item.id;
    const rank = (item: HeldAgentMessage) => typeof item.generation === "number" ? item.generation : 0;
    const last = new Map<string, HeldAgentMessage>();
    for (const item of items) {
      const other = last.get(chainOf(item));
      if (!other || rank(item) > rank(other)) last.set(chainOf(item), item);
    }
    for (const item of items.sort((a, b) => a.sentAt - b.sentAt)) {
      if (delivered.has(item.id) || delivered.has(chainOf(item)) || last.get(chainOf(item)) !== item ||
        (item.deliveryId !== undefined && this.#consumed.has(item.deliveryId))) {
        this.#consume(item);
        continue;
      }
      last.delete(chainOf(item));                          // a duplicate id goes once
      (item.handed ? this.#unverified : this.#held).push(item);
    }
    this.#trySave();
  }

  /**
   * Hold followUps for this local Main while it is busy, and flush those that have waited
   * flushMs at the next boundary between tool calls (turn_end, the hook the shell and
   * completion inboxes use). flushMs 0 keeps Pi's followUp queue, as before.
   */
  attachFollowUpDrain(context: ExtensionContext, flushMs: number, journal?: string, stallSeconds = 600): void {
    this.closeFollowUpDrain();
    if (!this.local) return;
    this.#stallS = stallSeconds;
    this.#context = context;
    this.#closed = false;
    this.#reloading = false;
    this.#halted = false;
    this.#providerFailed = false;
    this.#haltIndexUnknown = false;
    this.#journal = journal;
    const on = (name: string, fn: (event: any, ctx: ExtensionContext) => unknown): void => {
      if (typeof this.pi.on !== "function") return;
      const off = (this.pi.on as (name: string, fn: (event: any, ctx: ExtensionContext) => unknown) => unknown)(name, fn);
      if (typeof off === "function") this.#unsubscribe.push(off as () => void);
    };
    on("input", (event: { source?: string }, ctx) => {
      this.#context = ctx;
      if (event.source === "extension") return;
      this.#halted = false;
      this.#providerFailed = false;
      this.#haltIndexUnknown = false;
      this.#consumedDirty = true;
      this.#suspended = false;
      this.#trySave();
    });
    // Set gates before either drain branch can reconcile a lost Pi handoff. A turn error
    // precedes Pi's retry/overflow recovery decision, so never persist it as an owner stop.
    on("turn_end", (event: { message?: { stopReason?: string } }, ctx) => {
      const reason = event.message?.stopReason;
      if (ctx.signal?.aborted || reason === "aborted") this.halt();
      else if (reason === "error") { this.#providerFailed = true; this.#stopWake(); }
      else if (reason !== undefined) this.#providerFailed = false;
    });
    const settleGate = (event: { outcome?: string }, ctx: ExtensionContext): void => {
      if (ctx.signal?.aborted || event.outcome === "aborted") this.halt();
      else if (event.outcome === "error") { this.#providerFailed = true; this.#stopWake(); }
      else if (event.outcome === "completed") this.#providerFailed = false;
      // Older hosts omit outcome: neither grant recovery nor invent an owner stop.
    };
    on("agent_before_settle", settleGate);
    on("agent_settled", settleGate);
    // Observe the operation in BOTH drain modes. Pi also reports an extension's benign
    // decline as aborted, but only the operation signal proves an owner cancellation.
    on("session_before_compact", (event: { reason?: string; signal?: AbortSignal }) => {
      this.#operation = event.reason === "manual" ? event.signal : undefined;
    });
    on("session_compact_failed", (event: { reason?: string; aborted?: boolean; willRetry?: boolean; errorMessage?: string }, ctx) => {
      this.#context = ctx;
      // The aborted bit alone is ambiguous: an earlier handler may decline and stop
      // dispatch before we see the signal. Escape/halt is explicit; a seen signal
      // proves cancellation. Never manufacture durable owner intent from a decline.
      const ownerCancelled = ctx.signal?.aborted || this.#operation?.aborted;
      // Exact Pi no-op outcomes, with or without the host's error envelope. A provider
      // error merely quoting these phrases is still a terminal failure, not a no-op.
      const message = event.errorMessage?.replace(/^Compaction failed: /, "");
      const benign = message === "Already compacted" || message === "Nothing to compact (session too small)" ||
        message === "Compaction cancelled";
      if (ownerCancelled || (!event.aborted && !event.willRetry && !benign)) this.halt();
      // Unsuccessful boundaries never wake their queued replay, even for a benign rejection.
      // Later peer deliveries retain their own permission unless the owner really stopped.
      if (event.reason === "manual" && ctx.isIdle()) this.#release(false);
      this.#operation = undefined;
    });
    if (!(flushMs > 0) || typeof this.pi.on !== "function") {
      // Drain off: what an earlier drain journalled goes to Pi's own queue, under the same rule.
      // Each stays in the journal until the session holds it (review/astra F4 on pi-fabric#102).
      this.#replay();
      this.#closed = true;                                 // holds nothing new
      if (typeof this.pi.on !== "function") return;        // the next start confirms from the session
      if (this.#held.length && context.isIdle()) this.#release(true);
      on("turn_end", (event: BoundaryEvent, ctx) => { this.#context = ctx; this.#reconcile(event); this.#confirm(); });
      on("agent_before_settle", (event: BoundaryEvent & { outcome?: string }, ctx) => {
        this.#context = ctx;
        this.#reconcile(event);
        if (event.outcome === undefined || event.outcome === "completed") this.#release(true);
      });
      on("agent_settled", (_event, ctx) => { this.#context = ctx; this.#confirm(); });
      return;
    }
    this.#flushMs = flushMs;
    on("turn_end", (event: BoundaryEvent & { message?: { stopReason?: string } }, ctx) => {
      this.#context = ctx;
      if (ctx.signal?.aborted || ["aborted", "error"].includes(event.message?.stopReason ?? "")) {
        this.#suspended = true;
        this.#flushAll = false;
        return;
      }
      this.#reconcile(event);
      this.#confirm();
      this.#flushDue();
      this.#flushAll = false;
    });
    // Main is about to go idle. Hand the held followUps to Pi's followUp queue here, the last
    // boundary where Pi still continues the run for a queued message and where Pi itself drops
    // that continuation when the user cancels (review/astra F1 on pi-fabric#102).
    on("agent_before_settle", (event: BoundaryEvent & { outcome?: string }, ctx) => {
      this.#context = ctx;
      this.#reconcile(event);
      if (this.#suspended || (event.outcome !== undefined && event.outcome !== "completed")) return;
      this.#release(true);
    });
    // Only followUps that arrived after that boundary are left. Start a run for them only on a
    // settle Pi reports as completed: a cancel after the last turn sets no aborted turn_end, and
    // a Pi that reports no outcome cannot rule one out, so they are appended for the next run.
    on("agent_settled", (event: { outcome?: string }, ctx) => {
      this.#context = ctx;
      this.#confirm();
      const completed = !this.#suspended && event.outcome === "completed";
      this.#suspended = false;
      this.#release(completed);
    });
    // Manual /compact makes Main busy without a run, so no settle follows it. After a compaction
    // that completed, wake Main for what it held; after one that was cancelled or failed, append
    // them for the next run instead (review/astra on pi-fabric#102). A compaction inside a run
    // leaves them to that run's boundaries.
    // The operation's own abort signal says whether the user cancelled it: Pi can report that
    // cancel after Main is idle again, or not at all (review/astra on pi-fabric#102).
    on("session_before_tree", (event: { signal?: AbortSignal }) => { this.#operation = event.signal; });
    on("session_compact", (event: { reason?: string }, ctx) => {
      this.#context = ctx;
      if (event.reason === "manual") this.#wakeWhenIdle();
    });
    // A branch summary on /tree navigation is the same: busy without a run. A cancelled one
    // emits nothing, so its followUps wait for the next run.
    on("session_tree", (_event, ctx) => {
      this.#context = ctx;
      this.#wakeWhenIdle();
    });
    on("agent_start", (_event, ctx) => { this.#context = ctx; this.#suspended = false; this.#stopWake(); });
    this.#replay();
    if (this.#held.length && context.isIdle()) this.#release(true);
  }

  /** Stop holding; any held followUps go to Pi's own followUp queue, as before the drain. */
  closeFollowUpDrain(): void {
    this.#stopWake();
    for (const off of this.#unsubscribe.splice(0)) off();
    this.#closed = true;
    // With a journal, a reload or the next start replays them; handing them to Pi here as
    // well would deliver them twice. Without one, Pi's own queue takes them, as before.
    if (this.#journal) this.#trySave();
    else this.#release(true);
    this.#held.splice(0);
    this.#sent.splice(0);
    this.#unverified.splice(0);
    this.#consumed = new Set();
    this.#consumedDirty = false;
    this.#delivered = new Set();
    this.#source = undefined;
    this.#scanned = undefined;
    this.#journal = undefined;
    this.#context = undefined;
    this.#operation = undefined;
  }

  #drainActive(): boolean {
    return !this.#closed && this.#context !== undefined &&
      (this.#held.length > 0 || this.#context.isIdle() === false);
  }

  /**
   * smarty-dev#2119: an agents.wait in this Main hit its 60 s cap. The wait ended so Main can see
   * news, so the next turn_end flushes every held followUp, not only those past flushMs.
   */
  flushHeldAtNextBoundary(): void {
    if (this.#flushMs > 0 && !this.#closed) this.#flushAll = true;
  }

  #flushDue(): void {
    if (!this.#held.length || this.#suspended || this.#reloading) return;
    if (this.#halted || this.#providerFailed || this.#context?.signal?.aborted) { this.#release(false); return; }
    // A replayed reload-time steer/nextTurn keeps its own mode, even when Main is busy.
    while (this.#held[0]?.deliverAs !== undefined) {
      const first = this.#held[0]!;
      if (!this.#handOver(1, first.deliverAs!, first.triggerTurn ?? true, false)) return;
    }
    const now = Date.now();
    const age = this.#flushAll ? 0 : this.#flushMs;
    let due = 0;
    let bytes = 0;
    while (due < this.#held.length && this.#held[due]!.deliverAs === undefined && now - this.#held[due]!.sentAt >= age) {
      bytes += itemBytes(this.#held[due]!);
      // A byte-bounded FIFO prefix per boundary; the rest waits for the next one.
      if (due > 0 && bytes > FOLLOW_UP_LIMITS.batchBytes) break;
      due++;
    }
    if (!due) return;
    // One message, queued behind any steer already in Pi's queue: it never overtakes one.
    this.#handOver(due, "steer", true, true);
  }

  /** Send the first count held items as one message; they leave the queue only once it is sent. */
  #handOver(count: number, deliverAs: FabricMainAgentDelivery, triggerTurn: boolean, flushed: boolean): boolean {
    // smarty-dev#1826: an item that cannot be rendered fails every retry and, first in the queue,
    // holds every later followUp. It leaves the queue, with a report; the rest go. A failure of
    // Pi's queue itself keeps them all for the next boundary.
    const delivery: FabricMainAgentDelivery = flushed ? "followUp" : deliverAs;
    for (let index = 0; index < Math.min(count, this.#held.length);) {
      const item = this.#held[index]!;
      try {
        agentMessageBlock(item, delivery);
        index++;
      } catch (error) {
        this.#consume(this.#held.splice(index, 1)[0]!);
        count--;
        console.warn(
          `[pi-fabric] dropped undeliverable followUp ${item.id} from ${String(item.from?.id)}: ` +
            (error instanceof Error ? error.message : String(error)),
        );
      }
    }
    count = Math.min(count, this.#held.length);
    if (count <= 0) { this.#trySave(); return true; }
    const batch = this.#held.slice(0, count);
    try {
      this.#send(batch, deliverAs, triggerTurn, flushed);
    } catch {
      return false;                                      // kept; the next boundary or release retries
    }
    this.#held.splice(0, count);
    for (const item of batch) item.handed = true;
    this.#sent.push(...batch);
    this.#trySave();
    return true;
  }

  #wakeWhenIdle(): void {
    // Pi leaves its busy state only after every handler of the event has finished, which can
    // take any time (review/astra F2 on pi-fabric#102): wait for idle, not one tick. A release
    // or a new run ends the wait; a cancelled or failed operation releases without a run.
    // Armed with nothing held too: a followUp can arrive while a later handler still runs.
    // Only an operation whose abort signal Fabric saw, and that was not aborted, wakes Main: a
    // cancel stops the wait at once, before Main is idle, and its followUps wait (appended by
    // session_compact_failed, or held for the next run). Without the signal, success is unknown.
    if (this.#wake) return;
    const operation = this.#operation;
    // Retain cancellation evidence while later completion handlers still run.
    this.#wake = setInterval(() => {
      if (this.#closed || !operation || operation.aborted) this.#stopWake();
      else if (this.#context?.isIdle()) {
        if (this.#operation === operation) this.#operation = undefined;
        this.#held.length ? this.#release(true) : this.#stopWake();
      }
    }, 25);
    this.#wake.unref?.();
  }

  #stopWake(): void {
    if (this.#wake) clearInterval(this.#wake);
    this.#wake = undefined;
  }

  #release(triggerTurn: boolean): void {
    if (this.#reloading) return;
    this.#stopWake();
    // In byte-bounded messages, oldest first; a failed send keeps the rest for a retry.
    while (this.#held.length) {
      // Reload-time in-flight controls were never handed to Pi. Preserve their mode and policy.
      const first = this.#held[0]!;
      if (first.deliverAs !== undefined) {
        // A sender's saved permission cannot override a later cancelled/failed boundary.
        if (!this.#handOver(1, first.deliverAs, triggerTurn && (first.triggerTurn ?? true), false)) return;
        continue;
      }
      let count = 0;
      let bytes = 0;
      while (count < this.#held.length && this.#held[count]!.deliverAs === undefined) {
        bytes += itemBytes(this.#held[count]!);
        if (count > 0 && bytes > FOLLOW_UP_LIMITS.batchBytes) break;
        count++;
      }
      if (!this.#handOver(count, "followUp", triggerTurn, false)) return;
    }
  }

  #send(
    items: HeldAgentMessage[],
    deliverAs: FabricMainAgentDelivery,
    triggerTurn: boolean,
    flushed: boolean,
  ): void {
    triggerTurn &&= !this.#halted && !this.#providerFailed && !this.#context?.signal?.aborted;
    // Persist a downgraded explicit replay policy, including handoffs retried after a later reload.
    if (!triggerTurn) for (const item of items) if (item.deliverAs !== undefined) item.triggerTurn = false;
    // Each item keeps its own delivery mark: a flushed batch goes in as a steer, but it holds followUps.
    const delivery: FabricMainAgentDelivery = flushed ? "followUp" : deliverAs;
    const blocks = items.map((item) => agentMessageBlock(item, delivery));
    const itemDetails = (item: HeldAgentMessage) => ({
      id: item.id,
      from: item.from,
      delivery,
      sentAt: new Date(item.sentAt).toISOString(),
      ...(item.replacedAt === undefined ? {} : { replacedAt: new Date(item.replacedAt).toISOString() }),
      ...(item.chain === undefined ? {} : { chain: item.chain, generation: item.generation, replaces: item.replaces }),
      ...(item.data === undefined ? {} : { data: item.data }),
      ...(item.deliveryId === undefined ? {} : { deliveryId: item.deliveryId }),
      ...(item.supersedes?.length ? { supersedes: item.supersedes } : {}),
    });
    const first = items[0]!;
    this.pi.sendMessage(
      {
        customType: "pi-fabric-agent-message",
        content: [
          ...(flushed
            ? [this.#flushAll
              ? `${items.length} follow-up message(s) sent while you were busy, delivered at the tool boundary after a capped agents.wait. Oldest first; each is a followUp, not a steer.`
              : `${items.length} follow-up message(s) sent while you were busy, delivered at a tool boundary after waiting ${Math.round(this.#flushMs / 1000)} s or more. Oldest first; each is a followUp, not a steer.`]
            : []),
          ...blocks,
        ].join("\n\n"),
        display: true,
        details: {
          ...itemDetails(first),
          ...(items.length > 1 ? { id: randomUUID(), items: items.map(itemDetails) } : {}),
          triggerTurn,
          ...(flushed ? { flushed: true } : {}),
        },
      },
      { deliverAs, triggerTurn },
    );
  }
}
