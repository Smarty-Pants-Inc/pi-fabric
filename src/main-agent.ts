import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "./mesh/store.js";

const MAIN_AGENT_ALIAS = "main";
export type FabricAgentMessageDelivery = "steer" | "followUp";

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
  delivery: FabricAgentMessageDelivery;
  triggerTurn?: boolean;
  data?: unknown;
}

/** How far behind a Main is on followUps, so a sender can switch to steer (smarty-dev#1495). */
export interface FabricFollowUpQueueDepth {
  pendingFollowUps: number;
  oldestAgeS: number;
}

export interface FabricAgentMessageResult extends Partial<FabricFollowUpQueueDepth> {
  queued: true;
  messageId: string;
  routed: "local" | "main" | "mesh";
  acknowledged?: boolean;
  /** This followUp replaced a held one with the same sender and data.coalesceKey. */
  coalesced?: true;
  /** The id of the held followUp it replaced; that one is never delivered. */
  replacedMessageId?: string;
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
}

export interface FabricIdentityResolution {
  identity: MeshIdentity;
  mainAgentId: string;
}

export const resolveFabricIdentity = (
  sessionId: string,
  environment: NodeJS.ProcessEnv = process.env,
): FabricIdentityResolution => {
  const actorId = environment.PI_FABRIC_ACTOR_ID?.trim();
  const parentAgentId = environment.PI_FABRIC_PARENT_RUN?.trim();
  const identity: MeshIdentity = actorId
    ? {
        id: actorId,
        name: environment.PI_FABRIC_ACTOR_NAME?.trim() || actorId.slice(0, 8),
        kind: "actor",
        sessionId,
      }
    : parentAgentId
      ? {
          id: parentAgentId,
          name: environment.PI_FABRIC_AGENT_NAME?.trim() || parentAgentId.slice(0, 8),
          kind: "agent",
          sessionId,
        }
      : { id: `session:${sessionId}`, name: "main", kind: "main", sessionId };
  const inheritedMainAgentId = environment.PI_FABRIC_MAIN_AGENT_ID?.trim();
  return {
    identity,
    mainAgentId:
      inheritedMainAgentId || (identity.kind === "main" ? identity.id : `session:${sessionId}`),
  };
};

const escapeXmlText = (value: string): string =>
  value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

// Attribute values are quoted and escaped, so an identity field cannot close the header or
// forge a second envelope (dev-lead review of pi-fabric#102).
const escapeXmlAttribute = (value: string): string =>
  escapeXmlText(value).replaceAll('"', "&quot;").replaceAll("'", "&apos;");

/** Admission and batch bounds for followUps Fabric holds for a busy Main. */
export const FOLLOW_UP_LIMITS = {
  senderItems: 50,
  senderBytes: 256 * 1024,
  totalItems: 200,
  totalBytes: 1024 * 1024,
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

/** Superseded ids kept per held item: enough to audit a burst, bounded for the journal. */
const SUPERSEDED_KEPT = 32;

interface HeldAgentMessage {
  id: string;
  from: MeshIdentity;
  message: string;
  /** The first send of a coalesced chain: it keeps the queue position and the flush wait. */
  sentAt: number;
  /** When the newest replacement arrived (smarty-dev#1495). */
  replacedAt?: number;
  /** The held ids this one replaced, newest last: never delivered, even after a restart. */
  supersedes?: string[];
  data?: unknown;
  /** Handed to Pi's queue; it may still be there after a reload. Unset while Fabric holds it. */
  handed?: true;
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

const itemBytes = (item: HeldAgentMessage): number =>
  Buffer.byteLength(item.message) + (item.data === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.data)));

type SessionEntryLike = { id?: unknown; parentId?: unknown; type?: string; customType?: string; details?: { id?: unknown; items?: unknown } };

/** Add the followUp ids a persisted agent-message entry carries. */
const addDelivered = (ids: Set<string>, entry: SessionEntryLike | undefined): void => {
  if (entry?.type !== "custom_message" || entry.customType !== "pi-fabric-agent-message") return;
  if (typeof entry.details?.id === "string") ids.add(entry.details.id);
  if (Array.isArray(entry.details?.items)) {
    for (const item of entry.details.items as Array<{ id?: unknown }>) if (typeof item?.id === "string") ids.add(item.id);
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
  #scanned: number | undefined;
  #journal: string | undefined;
  readonly #unsubscribe: Array<() => void> = [];
  #context: ExtensionContext | undefined;
  #flushMs = 0;
  #suspended = false;
  #closed = false;
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

  deliverAgent(request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult {
    if (!this.local) throw new Error(`Main agent ${this.id} is owned by another Fabric process`);
    const message = request.message.trim();
    if (!message) throw new Error("Main agent message must not be empty");
    const item: HeldAgentMessage = {
      id: randomUUID(),
      from: structuredClone(request.from),
      message,
      sentAt: Date.now(),
      ...(request.data === undefined ? {} : { data: serializableData(request.data) }),
    };
    const triggerTurn = request.triggerTurn ?? true;
    let replaced: HeldAgentMessage | undefined;
    // Pi releases its own followUp queue only when Main has no more work, so a Main that
    // chains turns reads it an hour late (smarty-dev#1495). Fabric holds a triggering
    // followUp for a busy Main instead: turn_end flushes the due ones as one steer, and
    // agent_settled releases the rest as a followUp. A non-triggering one never waited.
    if (request.delivery === "followUp" && triggerTurn && this.#drainActive()) {
      // smarty-dev#1495: a held followUp with the same sender and data.coalesceKey is replaced
      // in place, so a sender that notifies on each state change leaves one message, the newest.
      const key = followUpCoalesceKey(item.data);
      const index = key === undefined ? -1 : this.#held.findIndex((held) =>
        held.from.id === item.from.id && followUpCoalesceKey(held.data) === key);
      replaced = index < 0 ? undefined : this.#held[index];
      this.#admit(item, replaced);
      if (replaced) {
        item.sentAt = replaced.sentAt;
        item.replacedAt = Date.now();
        item.supersedes = [...(replaced.supersedes ?? []), replaced.id].slice(-SUPERSEDED_KEPT);
        this.#held[index] = item;
      } else this.#held.push(item);
      try {
        this.#save();                                   // journalled before it is acknowledged
      } catch (error) {
        if (replaced) this.#held[index] = replaced;
        else this.#held.pop();
        throw new Error(`Main could not record the followUp: ${error instanceof Error ? error.message : String(error)}`);
      }
      if (this.#context!.isIdle()) this.#release(true);
    } else {
      this.#send([item], request.delivery, triggerTurn, false);
    }
    return {
      queued: true, messageId: item.id, routed: "main",
      ...(replaced ? { coalesced: true as const, replacedMessageId: replaced.id } : {}),
      ...this.queueDepth(item.from.id),
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
    if (mine.length + 1 > limits.senderItems || mineBytes + bytes > limits.senderBytes) {
      throw full(`${mine.length} of yours, ${mineBytes} bytes; limit ${limits.senderItems} or ${limits.senderBytes} bytes per sender`);
    }
    if (held.length + 1 > limits.totalItems || totalBytes + bytes > limits.totalBytes) {
      throw full(`${held.length} held, ${totalBytes} bytes; limit ${limits.totalItems} or ${limits.totalBytes} bytes in total`);
    }
  }

  /** Write the held and unconfirmed followUps (0600), or remove the journal when none are left. */
  #save(): void {
    if (!this.#journal) return;
    const items = [...this.#unverified, ...this.#sent, ...this.#held];
    if (!items.length) { fs.rmSync(this.#journal, { force: true }); return; }
    fs.mkdirSync(path.dirname(this.#journal), { recursive: true, mode: 0o700 });
    const temporary = `${this.#journal}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify({ version: 1, items }), { mode: 0o600 });
    fs.renameSync(temporary, this.#journal);
  }

  #trySave(): void {
    try { this.#save(); } catch {
      // The previous journal still lists every item; the next change writes it again.
    }
  }

  /**
   * Index the session entries appended since the last call, whatever branch they are on. Pi's
   * entry list only grows within a session; a shorter list (a new session) starts over.
   */
  #refreshDelivered(): Set<string> {
    try {
      const entries = this.#context?.sessionManager?.getEntries?.() ?? [];
      if (this.#scanned === undefined || entries.length < this.#scanned) {
        this.#delivered = new Set();
        this.#scanned = 0;
      }
      for (let index = this.#scanned; index < entries.length; index++) addDelivered(this.#delivered, entries[index] as SessionEntryLike);
      this.#scanned = entries.length;
    } catch {
      // The set stays as it was; the next boundary scans again.
    }
    return this.#delivered;
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
    for (const item of this.#unverified.splice(0)) {
      if (delivered.has(item.id)) continue;
      if (queued.has(item.id)) this.#sent.push(item);
      else {
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
    for (let index = this.#sent.length - 1; index >= 0; index--) {
      if (delivered.has(this.#sent[index]!.id)) this.#sent.splice(index, 1);
    }
    if (this.#sent.length !== before) this.#trySave();
  }

  /** After a restart: every journalled followUp the session does not hold goes back in the queue. */
  #replay(): void {
    if (!this.#journal) return;
    let items: HeldAgentMessage[] = [];
    try {
      const parsed = JSON.parse(fs.readFileSync(this.#journal, "utf8")) as { items?: unknown };
      if (Array.isArray(parsed.items)) {
        items = (parsed.items as HeldAgentMessage[]).filter((item) =>
          typeof item?.id === "string" && typeof item.message === "string" && typeof item.sentAt === "number" &&
          typeof item.from?.id === "string" && typeof item.from?.name === "string" && typeof item.from?.kind === "string");
      }
    } catch {
      return;                                            // no journal
    }
    // The owner rule (dev-lead on pi-fabric#102): skip every id the session holds; a handoff
    // may still sit in Pi's queue, so it waits for a boundary to check; the rest is held again.
    const delivered = this.#refreshDelivered();
    const seen = new Set<string>(items.flatMap((item) => Array.isArray(item.supersedes) ? item.supersedes : []));
    // A held followUp keeps only the newest of its (sender, coalesceKey), whatever the journal
    // lists: the same rule as a live replacement, so no superseded id comes back, however long
    // the burst was (review/astra F1 on pi-fabric#114; supersedes is a bounded audit trail).
    const newest = new Map<string, HeldAgentMessage>();
    const chain = (item: HeldAgentMessage) => {
      const key = item.handed ? undefined : followUpCoalesceKey(item.data);
      return key === undefined ? undefined : JSON.stringify([item.from.id, key]);
    };
    const updatedAt = (item: HeldAgentMessage) => typeof item.replacedAt === "number" ? item.replacedAt : item.sentAt;
    for (const item of items) {
      const key = chain(item);
      if (key === undefined || delivered.has(item.id) || seen.has(item.id)) continue;
      const other = newest.get(key);
      if (!other || updatedAt(item) >= updatedAt(other)) newest.set(key, item);
    }
    for (const item of items.sort((a, b) => a.sentAt - b.sentAt)) {
      if (delivered.has(item.id) || seen.has(item.id)) continue;
      const key = chain(item);
      if (key !== undefined && newest.get(key) !== item) continue;
      seen.add(item.id);
      (item.handed ? this.#unverified : this.#held).push(item);
    }
    this.#trySave();
  }

  /**
   * Hold followUps for this local Main while it is busy, and flush those that have waited
   * flushMs at the next boundary between tool calls (turn_end, the hook the shell and
   * completion inboxes use). flushMs 0 keeps Pi's followUp queue, as before.
   */
  attachFollowUpDrain(context: ExtensionContext, flushMs: number, journal?: string): void {
    this.closeFollowUpDrain();
    if (!this.local) return;
    this.#context = context;
    this.#closed = false;
    this.#journal = journal;
    const on = (name: string, fn: (event: any, ctx: ExtensionContext) => unknown): void => {
      const off = (this.pi.on as (name: string, fn: (event: any, ctx: ExtensionContext) => unknown) => unknown)(name, fn);
      if (typeof off === "function") this.#unsubscribe.push(off as () => void);
    };
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
      this.#reconcile(event);
      if (ctx.signal?.aborted || ["aborted", "error"].includes(event.message?.stopReason ?? "")) {
        this.#suspended = true;
        return;
      }
      this.#confirm();
      this.#flushDue();
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
    on("session_before_compact", (event: { reason?: string; signal?: AbortSignal }) => {
      this.#operation = event.reason === "manual" ? event.signal : undefined;
    });
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
    on("session_compact_failed", (event: { reason?: string }, ctx) => {
      this.#context = ctx;
      // Pi clears the compaction state before this event, so Main is idle here.
      if (event.reason === "manual" && ctx.isIdle()) this.#release(false);
    });
    on("agent_start", (_event, ctx) => { this.#context = ctx; this.#suspended = false; this.#stopWake(); });
    on("input", (_event, ctx) => { this.#context = ctx; this.#suspended = false; });
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
    this.#delivered = new Set();
    this.#scanned = undefined;
    this.#journal = undefined;
    this.#context = undefined;
  }

  #drainActive(): boolean {
    return !this.#closed && this.#context !== undefined &&
      (this.#held.length > 0 || this.#context.isIdle() === false);
  }

  #flushDue(): void {
    if (!this.#held.length || this.#suspended) return;
    const now = Date.now();
    let due = 0;
    let bytes = 0;
    while (due < this.#held.length && now - this.#held[due]!.sentAt >= this.#flushMs) {
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
  #handOver(count: number, deliverAs: FabricAgentMessageDelivery, triggerTurn: boolean, flushed: boolean): boolean {
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
    this.#operation = undefined;
    this.#wake = setInterval(() => {
      if (this.#closed || !operation || operation.aborted) this.#stopWake();
      else if (this.#context?.isIdle()) this.#held.length ? this.#release(true) : this.#stopWake();
    }, 25);
    this.#wake.unref?.();
  }

  #stopWake(): void {
    if (this.#wake) clearInterval(this.#wake);
    this.#wake = undefined;
  }

  #release(triggerTurn: boolean): void {
    this.#stopWake();
    // In byte-bounded messages, oldest first; a failed send keeps the rest for a retry.
    while (this.#held.length) {
      let count = 0;
      let bytes = 0;
      while (count < this.#held.length) {
        bytes += itemBytes(this.#held[count]!);
        if (count > 0 && bytes > FOLLOW_UP_LIMITS.batchBytes) break;
        count++;
      }
      if (!this.#handOver(count, "followUp", triggerTurn, false)) return;
    }
  }

  #send(
    items: HeldAgentMessage[],
    deliverAs: FabricAgentMessageDelivery,
    triggerTurn: boolean,
    flushed: boolean,
  ): void {
    // Each item keeps its own delivery mark: a flushed batch goes in as a steer, but it holds followUps.
    const delivery: FabricAgentMessageDelivery = flushed ? "followUp" : deliverAs;
    const blocks = items.map((item) => [
      `<fabric-agent-message from_name="${escapeXmlAttribute(item.from.name)}" from_id="${escapeXmlAttribute(item.from.id)}" from_kind="${escapeXmlAttribute(item.from.kind)}" delivery="${escapeXmlAttribute(delivery)}" sent_at="${escapeXmlAttribute(new Date(item.sentAt).toISOString())}"${item.replacedAt === undefined ? "" : ` replaced_at="${escapeXmlAttribute(new Date(item.replacedAt).toISOString())}"`}>`,
      escapeXmlText(item.message),
      item.data === undefined ? undefined : `<data>${escapeXmlText(JSON.stringify(item.data))}</data>`,
      "</fabric-agent-message>",
    ].filter((line): line is string => Boolean(line)).join("\n"));
    const itemDetails = (item: HeldAgentMessage) => ({
      id: item.id,
      from: item.from,
      delivery,
      sentAt: new Date(item.sentAt).toISOString(),
      ...(item.replacedAt === undefined ? {} : { replacedAt: new Date(item.replacedAt).toISOString() }),
      ...(item.supersedes?.length ? { supersedes: item.supersedes } : {}),
      ...(item.data === undefined ? {} : { data: item.data }),
    });
    const first = items[0]!;
    this.pi.sendMessage(
      {
        customType: "pi-fabric-agent-message",
        content: [
          ...(flushed
            ? [`${items.length} follow-up message(s) sent while you were busy, delivered at a tool boundary after waiting ${Math.round(this.#flushMs / 1000)} s or more. Oldest first; each is a followUp, not a steer.`]
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
