import { createHash, randomUUID } from "node:crypto";
import { diagnostics, notifyDiagnostic, notifyPrincipalDiagnosticOnce, resetPrincipalDiagnosticTurn } from "./core/diagnostics.js";
import { appendDeliveryOutcome, deliverySend, type DeliverySend, type DeliveryOutcome } from "./mesh/delivery-outcomes.js";
import type { AgentFollowUpRunningWarning } from "./agents/types.js";
import fs from "node:fs";
import path from "node:path";
import { readFileRetrying, writeFileAtomic } from "./core/atomic-write.js";
import { withConfirmedSessionFile } from "./core/session-receipts.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "./mesh/store.js";
import { takeCompactionDecline } from "./compaction/cancellation.js";
import { fabricProvenanceOptions, fabricProvenanceSupported, fabricTurnProvenance, fabricWakeCause, copyFabricProvenance, fabricWakeMessage, admittedFabricWakeCauses, sendFabricMessage, sendFabricUserMessage, type FabricWakeCause, type FabricTurnProvenance, type FabricPrincipal } from "./fabric-provenance.js";
import { classifyProviderError, type ProviderErrorClass } from "./provider-error-class.js";
import { extensionContextLive } from "./session-liveness.js";

import { FabricInterruptNotAuthorizedError, FabricInterruptRateLimitedError, type FabricInterruptAuthority } from "./interrupt-authority.js";

const MAIN_AGENT_ALIAS = "main";
// Pi can report idle while a prompt's preflight still runs. Sending a followUp then puts it
// in Pi's native queue, behind later followUps flushed as steers by this drain (#754).
// Older hosts do not expose this optional capability.
const promptPending = (ctx: ExtensionContext): boolean =>
  "isPromptPending" in ctx && typeof ctx.isPromptPending === "function" && ctx.isPromptPending() === true;

/** Mirror Pi's operation cancellation test: its native deadline is a recoverable failure. */
const isCompactionCancelled = (signal: AbortSignal | undefined): boolean =>
  signal?.aborted === true && !(signal.reason instanceof DOMException && signal.reason.name === "TimeoutError");
export type FabricAgentMessageDelivery = "steer" | "followUp";
export type FabricSteerPriority = "interrupt";

/** Public and owner-side validation: priority never changes a followUp into a steer. */
export function assertSteerPriority(priority: unknown, delivery: string): asserts priority is FabricSteerPriority | undefined {
  if (priority !== undefined && (priority !== "interrupt" || delivery !== "steer")) {
    throw new Error('priority must be "interrupt" and is supported only for a steer to Main');
  }
}
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
  /** Recorded admission only; absence (including old bridges) makes no sender claim. */
  verification?: "mesh" | "bridge";
  /** Producer-owned resident classification, carried through durable admission; never request.data. */
  source?: "actor-output" | "fabric-host" | undefined;
  /** Host-owned envelope metadata, never request.data. */
  principal?: FabricPrincipal | undefined;
  message: string;
  delivery: FabricMainAgentDelivery;
  priority?: FabricSteerPriority;
  triggerTurn?: boolean;
  /** Legacy sender diagnostics are ignored at admission. */
  wakeCause?: FabricWakeCause | undefined;
  /** Receiving route metadata, never hydrated from command/message data. */
  admissionTopic?: string | undefined;
  /** Host-hydrated origin metadata for a root-inbox event shadow. */
  inboxEventId?: string | undefined;
  inboxTopic?: string | undefined;
  inboxKey?: string | undefined;
  inboxReceipts?: string[] | undefined;
  data?: unknown;
  /**
   * A stable id of the sender's durable record (a resident actor's delivery record). Main journals
   * the message under it before it returns and admits each id once, across restarts and reloads,
   * so the sender may delete its record after a return (review round 2 on pi-fabric#160).
   */
  deliveryId?: string;
  /** Host-owned outcome identity; retained until native consumption. */
  outcomeSend?: DeliverySend;
}

/** How far behind a Main is on followUps, so a sender can switch to steer (smarty-dev#1495). */
export interface FabricFollowUpQueueDepth {
  pendingFollowUps: number;
  oldestAgeS: number;
}

export interface FabricAgentMessageResult extends Partial<FabricFollowUpQueueDepth> {
  deadlineAt?: number;
  /** Sender-only observation at task admission; delivery is unchanged. */
  warning?: AgentFollowUpRunningWarning;
  /** Advisory identifier provenance notice; appended to delivered text when admission permits. */
  notice?: string;
  queued: true;
  messageId: string;
  routed: "local" | "main" | "mesh";
  acknowledged?: boolean;
  /** Main requested a new turn at admission, not merely a triggering queue policy.
   * False for busy, passive, halted, reload-held, and duplicate deliveries; absent for
   * older owners or targets that cannot report Main's state. */
  triggered?: boolean;
  /** Why a requested wake was held, e.g. provider-backoff until an ISO timestamp. */
  reason?: string;
  /** Main is held by an operator halt; peer followUps remain passive. */
  halted?: true;
  /** Main had already admitted this deliveryId; nothing was sent again. */
  duplicate?: true;
  /** This followUp replaced a held one with the same sender and data.coalesceKey. */
  coalesced?: true;
  /** The id of the held followUp it replaced; that one is never delivered. */
  replacedMessageId?: string;
  /** Main is idle but its held followUps are past mesh.followUpStallSeconds (smarty-dev#1826). */
  stalled?: true;
}

/** smarty-dev#8742: one provider-dead incident on this Main. */
type ProviderDeadState = {
  /** non-retryable: a classified 4xx/gateway rejection; unclassified: repeated failures with no status or body. */
  kind: "non-retryable" | "unclassified";
  /** Allowlisted tokens only (status, known error type, fixed code); never provider text. */
  error: string;
  status?: number;
  type?: string;
  code?: string;
  /** Consecutive unclassified failures when the incident was raised. */
  failures?: number;
  since: number;
  /** Absolute deadline for the current recovery episode, when one is pending. */
  deadlineAt?: number;
  /** The one automatic post-compaction attempt has already been admitted. */
  attemptStarted?: boolean;
  phase: "recovering" | "compacting" | "probation" | "alarmed";
  alarmed: boolean;
};

/** Unclassified failures in a row (no successful turn between them) before Main alarms (smarty-dev#8742). */
export const UNCLASSIFIED_PROVIDER_DEAD_AFTER = 3;
/** Last-resort, non-rearming alarm deadline for a pending provider recovery step. */
export const MAIN_PROVIDER_RECOVERY_TIMEOUT_MS = 5 * 60_000;
/** Keep an unclassified streak bounded to one recovery window across reloads. */
const UNCLASSIFIED_PROVIDER_FAILURE_WINDOW_MS = MAIN_PROVIDER_RECOVERY_TIMEOUT_MS;
const MAX_PERSISTED_UNCLASSIFIED_FAILURES = UNCLASSIFIED_PROVIDER_DEAD_AFTER;

/** `provider-dead (non-retryable: <allowlisted short>)` or `provider-dead (unclassified)`. */
const providerDeadLabel = (dead: { kind: "non-retryable" | "unclassified"; error: string }): string =>
  dead.kind === "unclassified" ? "provider-dead (unclassified)" : `provider-dead (non-retryable: ${dead.error})`;

/**
 * smarty-dev#8742: a provider-dead Main. Every field is allowlisted: the HTTP status, a known error
 * type, a fixed code and Fabric's own cause. Provider message text never appears here, because this
 * event is published to ops.owner, to held messages' senders and across the mesh bridge; it stays
 * in the failed assistant message in the local session log.
 */
export interface FabricMainProviderDeadEvent {
  kind: "non-retryable" | "unclassified";
  /** One short line from allowlisted tokens, e.g. `409 invalid_request_error: compaction_affinity_missing`. */
  error: string;
  status?: number;
  type?: string;
  code?: string;
  /** Consecutive unclassified failures (kind unclassified). */
  failures?: number;
  /** When the incident began (ISO). */
  since: string;
  /**
   * compaction-failed: the recovery compaction failed. failed-after-compaction: the one attempt
   * after it failed non-retryably again. unclassified-repeated: UNCLASSIFIED_PROVIDER_DEAD_AFTER
   * failures in a row without an HTTP status, error body or known token. recovery-timeout: the
   * pending recovery step did not become runnable within its five-minute last-resort deadline.
   */
  cause: "compaction-failed" | "failed-after-compaction" | "unclassified-repeated" | "recovery-timeout";
  /** Peer messages Fabric holds unread for this Main, oldest first. */
  held: Array<{ id: string; from: string; sentAt: number }>;
}

export interface FabricMainAgentBindingResult extends FabricMainAgentInfo {
  caller: string;
  previous: { model?: string; thinking?: string };
}

export type FabricMainAgentBindingChange =
  | { operation: "setModel"; model: { provider: string; id: string } }
  | { operation: "setThinking"; thinking: Parameters<ExtensionAPI["setThinkingLevel"]>[0] };

export interface FabricMainModelSwitchResult {
  ok: boolean;
  error?: string;
}

export interface FabricMainAgentTarget {
  readonly id: string;
  readonly local: boolean;
  /** Local session mode, available even when its first mesh publication fails. */
  readonly interactive?: boolean;
  matches(id: string): boolean;
  /** Native process-owned halt/abort; remote targets omit this capability. */
  stop?(): { id: string; status: "stopped" };
  info(context?: ExtensionContext): FabricMainAgentInfo;
  deliverAgent(request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult;
  // Switch Main's live session model in place. Only local hosts hold the pi
  // extension session required for the mutation, so remote targets omit it.
  switchModel?(
    target: { provider: string; id: string },
    context: ExtensionContext,
  ): Promise<FabricMainModelSwitchResult>;
  bindingContext?(): ExtensionContext | undefined;
  setBinding?(
    change: FabricMainAgentBindingChange,
    caller: string,
    context: ExtensionContext,
    beforeCommit: () => void,
  ): Promise<FabricMainAgentBindingResult>;
  // smarty-dev#2119: a capped Main wait returned; flush every held followUp at the next tool
  // boundary, whatever its age. Local Mains with a followUp drain only.
  flushHeldAtNextBoundary?(): void;
  /** Local host capability, used to keep sender-specific lifecycle batches attributable. */
  supportsProvenance?(): boolean;
}

// Keep identity resolution available to existing callers without making startup import the drain.
export { resolveFabricIdentity, type FabricIdentityResolution } from "./fabric-provenance.js";

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

const mainWakeCause = (
  sender: MeshIdentity, source: FabricMainAgentDeliveryRequest["source"], delivery: FabricMainAgentDelivery,
  topic?: string, key?: string,
): FabricWakeCause => fabricWakeCause(sender,
  source === "fabric-host" ? "host-event" : source === "actor-output" ? "actor" :
    delivery === "steer" ? "steer" : "followUp", topic, key);

/** Resident alarms historically used actor labels. Their durable receipt alone proves no authorship. */
const mainSenderClaimAllowed = (sender: MeshIdentity, deliveryId: unknown, source: unknown): boolean =>
  source !== "fabric-host" && !(sender.kind === "actor" && typeof deliveryId === "string" &&
    deliveryId.startsWith("resident:") && source !== "actor-output");

export interface HeldAgentMessage {
  outcomeSend?: DeliverySend;
  supersededSends?: DeliverySend[];
  id: string;
  from: MeshIdentity;
  /** Resident producer evidence, retained even on hosts without Pi provenance support. */
  source?: FabricMainAgentDeliveryRequest["source"];
  /** Original verified admission, journalled before acknowledgement; never a Pi receipt stamp. */
  provenance?: FabricTurnProvenance | undefined;
  wakeCause?: FabricWakeCause;
  wakeCauses?: FabricWakeCause[];
  /** Receiving-envelope route snapshots, never rehydrated from serialized wake causes. */
  admissionTopic?: string;
  admissionKey?: string;
  /** Host-hydrated origin event identity and receipts; journaled with the native carrier for replay. */
  inboxEventId?: string;
  inboxTopic?: string;
  inboxKey?: string;
  inboxReceipts?: string[];
  /** A multi-source or differently classified producer has no reconstructable single envelope. */
  unattributedOnReplay?: true;
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
  /** Native retirement is complete; retain this item only for outcome bookkeeping. */
  outcomePending?: true;
  /** Host-observed own native receipt, not inferred from chain retirement or consumed ids. */
  outcomeNativeDelivered?: true;
  /** Persisted attempt reservations, including the first post-confirmation append. */
  outcomeRetryAttempts?: number;
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

/** Preserve explicit data; otherwise carry the admitted event's work claim at native handoff. */
const nativeItemData = (item: HeldAgentMessage): unknown =>
  item.data === undefined && item.inboxKey !== undefined
    ? { key: item.inboxKey, topic: item.inboxTopic } : item.data;

const itemBytes = (item: HeldAgentMessage): number => {
  const data = nativeItemData(item);
  return Buffer.byteLength(item.message) + (data === undefined ? 0 : Buffer.byteLength(JSON.stringify(data))) +
    // The retained key and native data are separate serialized representations: charge both.
    (item.inboxKey === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.inboxKey))) +
    (item.deliveryId === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.deliveryId))) +
    (item.supersedes === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.supersedes))) +
    (item.inboxReceipts === undefined ? 0 : Buffer.byteLength(JSON.stringify(item.inboxReceipts)));
};

type SessionEntryLike = { id?: unknown; parentId?: unknown; type?: string; customType?: string; details?: { id?: unknown; items?: unknown } };

/** How many consumed delivery ids Main remembers beside its journal, newest last. */
const CONSUMED_DELIVERIES_MAX = 2000;
/** Outcome bookkeeping is bounded separately from the initial native admission. */
export const MAIN_DELIVERY_OUTCOME_RETRY_LIMIT = 8;
const OUTCOME_DEAD_LETTER_REASON =
  `delivery outcome bookkeeping unknown; retry budget of ${MAIN_DELIVERY_OUTCOME_RETRY_LIMIT} exhausted`;
const deliveryKey = (deliveryId: string): string => `delivery:${deliveryId}`;

type PersistedProviderState = {
  unclassifiedFailures: number;
  unclassifiedSince?: number;
  unclassifiedAlarmed?: boolean;
  runUnclassified?: boolean;
  dead?: ProviderDeadState;
};

const PROVIDER_TYPES = new Set([
  "invalid_request_error", "authentication_error", "permission_error", "not_found_error",
  "request_too_large", "invalid_api_key", "model_not_found", "rate_limit_error", "overloaded_error",
  "api_error", "timeout_error", "rate_limit_exceeded",
]);
const providerToken = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= 200 && /^[A-Za-z0-9_.: -]+$/.test(value);

/** Read only the allowlisted provider episode fields written by this controller. */
const persistedProviderDead = (value: unknown): ProviderDeadState | undefined => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const dead = value as Partial<ProviderDeadState>;
  if (dead.kind !== "unclassified" && dead.kind !== "non-retryable") return undefined;
  if (!providerToken(dead.error) || typeof dead.since !== "number" || !Number.isSafeInteger(dead.since) || dead.since < 0 ||
      !["recovering", "compacting", "probation", "alarmed"].includes(dead.phase ?? "") ||
      typeof dead.alarmed !== "boolean") return undefined;
  if (dead.kind === "unclassified" && dead.error !== "unclassified") return undefined;
  if (dead.kind === "non-retryable" && dead.type !== undefined && !PROVIDER_TYPES.has(dead.type)) return undefined;
  if (dead.code !== undefined && dead.code !== "compaction_affinity_missing") return undefined;
  if (dead.status !== undefined && (!Number.isSafeInteger(dead.status) || dead.status < 400 || dead.status > 599)) return undefined;
  if (dead.failures !== undefined && (!Number.isSafeInteger(dead.failures) || dead.failures < 1 || dead.failures > MAX_PERSISTED_UNCLASSIFIED_FAILURES)) return undefined;
  if (dead.deadlineAt !== undefined && (!Number.isSafeInteger(dead.deadlineAt) || dead.deadlineAt < 0)) return undefined;
  if (dead.attemptStarted !== undefined && typeof dead.attemptStarted !== "boolean") return undefined;
  const head = [dead.status === undefined ? undefined : String(dead.status), dead.type].filter(Boolean).join(" ");
  const expected = head && dead.code ? `${head}: ${dead.code}` : head || dead.code || "unclassified";
  if (dead.error !== expected) return undefined;
  return {
    kind: dead.kind, error: dead.error, ...(dead.status === undefined ? {} : { status: dead.status }),
    ...(dead.type === undefined ? {} : { type: dead.type }), ...(dead.code === undefined ? {} : { code: dead.code }),
    ...(dead.failures === undefined ? {} : { failures: dead.failures }), since: dead.since,
    ...(dead.deadlineAt === undefined ? {} : { deadlineAt: dead.deadlineAt }),
    ...(dead.attemptStarted === undefined ? {} : { attemptStarted: dead.attemptStarted }), phase: dead.phase!, alarmed: dead.alarmed,
  };
};

const persistedProviderState = (value: unknown): PersistedProviderState | undefined => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const state = value as Partial<PersistedProviderState>;
  if (typeof state.unclassifiedFailures !== "number" || !Number.isSafeInteger(state.unclassifiedFailures) || state.unclassifiedFailures < 0 ||
      state.unclassifiedFailures > MAX_PERSISTED_UNCLASSIFIED_FAILURES ||
      (state.unclassifiedSince !== undefined && (!Number.isSafeInteger(state.unclassifiedSince) || state.unclassifiedSince < 0)) ||
      (state.unclassifiedAlarmed !== undefined && typeof state.unclassifiedAlarmed !== "boolean") ||
      (state.runUnclassified !== undefined && typeof state.runUnclassified !== "boolean")) return undefined;
  const dead = state.dead === undefined ? undefined : persistedProviderDead(state.dead);
  if (state.dead !== undefined && !dead) return undefined;
  return { unclassifiedFailures: state.unclassifiedFailures,
    ...(state.unclassifiedSince === undefined ? {} : { unclassifiedSince: state.unclassifiedSince }),
    ...(state.unclassifiedAlarmed === undefined ? {} : { unclassifiedAlarmed: state.unclassifiedAlarmed }),
    ...(state.runUnclassified === undefined ? {} : { runUnclassified: state.runUnclassified }),
    ...(dead === undefined ? {} : { dead }) };
};

// Owner authority must outlive a controller AND a reloaded module generation when storage
// rejects its write. Keep only uncommitted stops, scoped to the absolute index path; a
// confirmed durable write removes them. This is a live-reload handoff, not a disk receipt.
const PENDING_HALTS = Symbol.for("pi-fabric.main.pending-halts.v1");
const pendingHalts = (): Map<string, { warned: boolean }> =>
  ((globalThis as Record<symbol, unknown>)[PENDING_HALTS] ??= new Map<string, { warned: boolean }>()) as Map<string, { warned: boolean }>;

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
  readonly #replayed = new Set<string>();
  // Handed to Pi, not yet in the session's entries. Kept in the journal until they are.
  readonly #sent: HeldAgentMessage[] = [];
  // Native admission is confirmed, but one or more outcome records still need a best-effort retry.
  // These items never re-enter Pi's queue; the journal is their outcome-only retry obligation.
  readonly #outcomePending: HeldAgentMessage[] = [];
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
  #sessionFileIdentity: string | undefined;
  #journal: string | undefined;
  #inboxFence: { owns(id: string): boolean; active(): boolean } | undefined;
  // Delivery ids whose message the session holds, or that will never go (replaced, dropped):
  // persisted beside the journal, bounded, oldest first. This index also retains the owner halt
  // after the message journal is empty. With the journal's own items they make
  // deliverAgent idempotent by deliveryId across restarts and release reloads.
  #consumed = new Set<string>();
  #consumedDirty = false;
  readonly #unsubscribe: Array<() => void> = [];
  #context: ExtensionContext | undefined;
  readonly #tools = new Set<string>();
  // Only live admission may abort. Journal replay is an ordinary steer, never another abort.
  readonly #interrupts = new Set<string>();
  #interruptUsedThisTurn = false;
  #interruptResume = false;
  readonly #lastInterrupt = new Map<string, number>();
  #flushMs = 0;
  #flushAll = false;
  #stallS = 600;
  #suspended = false;
  // Owner stop, unlike a run's signal: survives reload and lifts only on user input.
  #halted = false;
  #peerSteerInFlight = false;
  // Provider failures hold peer wakes temporarily; only owner stops suppress them indefinitely.
  #providerFailed = false;
  #providerFailures = 0;
  #providerRetryAt = 0;
  // turn_end and both settle notifications describe one failure, not three retries.
  #providerFailureRecorded = false;
  #providerWake: ReturnType<typeof setTimeout> | undefined;
  #providerReleaseUntil: number | undefined;
  // Never place a second native continuation beyond a retry whose outcome is still unknown.
  #providerAttemptInFlight = false;
  // smarty-dev#8742: a non-retryable provider error is never backoff. recovering waits for idle,
  // compacting runs ctx.compact once, probation allows one attempt, alarmed holds until owner
  // input or a successful turn. One alarm per incident.
  #providerDead: ProviderDeadState | undefined;
  #providerDeadTimer: ReturnType<typeof setTimeout> | undefined;
  #providerDeadDeadline: ReturnType<typeof setTimeout> | undefined;
  #providerStateDirty = false;
  // smarty-dev#8742: an unclassified failure (no status, body or known token: a network blip, or a
  // dead provider Fabric cannot recognise) is backoff until the third settle in a row; then alarm.
  #unclassifiedFailures = 0;
  #unclassifiedSince: number | undefined;
  #unclassifiedAlarmed = false;
  #runUnclassified = false;
  // Preserve an unreadable owner index until explicit user input authorizes replacing it.
  #haltIndexUnknown = false;
  #closed = false;
  #reloading = false;
  #switching = false;
  #bindingsLive = false;
  #bindingMutation: Promise<unknown> = Promise.resolve();
  #wake: ReturnType<typeof setInterval> | undefined;
  #preflightWake: ReturnType<typeof setInterval> | undefined;
  #operation: AbortSignal | undefined;
  #offOperationAbort: (() => void) | undefined;
  // Keep the veto signal through both settlement notifications, including late owner aborts.
  #compactionDecline: AbortSignal | undefined;

  constructor(
    readonly pi: ExtensionAPI,
    readonly id: string,
    readonly local: boolean,
    readonly cwd: string,
    readonly sessionId?: string,
    readonly interactive = true,
    readonly onProviderWakeReleased?: (event: { until: string; messageIds: string[] }) => void,
    readonly interruptAuthority: FabricInterruptAuthority = {},
    readonly onProviderDead?: (event: FabricMainProviderDeadEvent) => void,
    readonly meshRoot?: string,
    readonly peerSteerResume = false,
  ) {}

  /** This Main's live drain owns non-retryable provider recovery (smarty-dev#8742). */
  ownsProviderRecovery(): boolean {
    return this.local && this.#bindingsLive && this.#context !== undefined && !this.#reloading;
  }

  matches(id: string): boolean {
    const target = id.trim();
    return target === MAIN_AGENT_ALIAS || target === this.id;
  }

  stop(): { id: string; status: "stopped" } {
    if (!this.local || !this.#bindingsLive || !this.#context) {
      throw new Error(`Main agent ${this.id} has no local stop controller`);
    }
    this.halt();
    this.#context.abort();
    return { id: this.id, status: "stopped" };
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
      // Minimal host contexts (bootstrap probes, fixtures) may omit isIdle.
      status: this.local ? (typeof context?.isIdle === "function" && context.isIdle() === false ? "running" : "idle") : "remote",
      runner: "pi",
      transport: "host",
      ...(this.local ? { cwd: this.cwd, startedAt: this.startedAt } : {}),
      ...(this.sessionId ? { sessionId: this.sessionId } : {}),
      ...(model ? { model } : {}),
      ...(thinking ? { thinking } : {}),
      updatedAt: Date.now(),
      pendingMessages: this.local && typeof context?.hasPendingMessages === "function" ? context.hasPendingMessages() : false,
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

  bindingContext(): ExtensionContext | undefined {
    return this.local && this.#bindingsLive ? this.#context : undefined;
  }

  setBinding(
    change: FabricMainAgentBindingChange,
    caller: string,
    context: ExtensionContext,
    beforeCommit: () => void,
  ): Promise<FabricMainAgentBindingResult> {
    // Defense in depth for direct/local callers: native model auth has no
    // cancellation-aware commit boundary. Keep the legacy switchModel separate.
    if (change.operation === "setModel") {
      return Promise.reject(new Error("Main setModel is not supported yet (own or remote); see smarty-dev#4153"));
    }
    const commit = (): FabricMainAgentBindingResult => {
      beforeCommit();
      if (!this.local || !this.#bindingsLive || context.sessionManager.getSessionId() !== this.sessionId) {
        throw new Error(`Main ${this.id} is not live; no binding change was queued`);
      }
      const snapshot = (): { model?: string; thinking?: string } => {
        const { model, thinking } = this.info(context);
        return { ...(model ? { model } : {}), ...(thinking ? { thinking } : {}) };
      };
      const previous = snapshot();
      // Pi's thinking setter synchronously clamps, mutates and journals: no await
      // separates the invocation/liveness fence above from the native commit.
      // Its async event notification happens only after the state is committed.
      // The in-flight inference is unchanged; the next turn uses read-back state.
      this.pi.setThinkingLevel(change.thinking);
      const after = snapshot();
      this.pi.appendEntry("pi-fabric.main-binding-change", {
        action: `agents.${change.operation}`, target: this.id, caller, before: previous, after,
      });
      return { ...this.info(context), caller, previous };
    };
    const mutation = this.#bindingMutation.then(commit);
    this.#bindingMutation = mutation.catch(() => {});
    return mutation;
  }

  supportsProvenance(): boolean { return fabricProvenanceSupported(this.pi); }

  deliverUser(
    message: string, delivery: FabricAgentMessageDelivery,
    from?: MeshIdentity, verification?: "mesh" | "bridge",
  ): FabricAgentMessageResult {
    if (!this.local) throw new Error(`Main agent ${this.id} is owned by another Fabric process`);
    const text = message.trim();
    if (!text) throw new Error("Main agent message must not be empty");
    const messageId = randomUUID();
    const options = { deliverAs: delivery };
    // An unknown caller cannot claim this Main's identity. Pi records the unclaimed turn as terminal.
    // The dashboard composer is human input (no Fabric sender). Keep it user.
    if (from) sendFabricUserMessage(this.pi, text, from, delivery, options, verification);
    else this.pi.sendUserMessage(text, options);
    return { queued: true, messageId, routed: "main" };
  }

  /** No more Pi handoffs once reload starts; an already-admitted control command journals only. */
  prepareReload(): void {
    this.#bindingsLive = false;
    this.#reloading = true;
    this.#stopWake();
    this.#stopProviderDeadDeadline();
  }

  /** Escape can halt an idle Main without producing an aborted run event. */
  halt(): void {
    this.#interrupts.clear();
    if (this.#runUnclassified) { this.#runUnclassified = false; this.#providerStateDirty = true; }
    this.#halted = true;
    this.#peerSteerInFlight = false;
    this.#providerReleaseUntil = undefined;
    this.#stopProviderDeadDeadline();
    this.#consumedDirty = true;
    const index = this.#consumedPath();
    if (index && !pendingHalts().has(index)) pendingHalts().set(index, { warned: false });
    this.#stopWake();
    this.#trySave();
  }

  deliverAgent(request: FabricMainAgentDeliveryRequest): FabricAgentMessageResult {
    assertSteerPriority(request.priority, request.delivery);
    if (request.priority && request.triggerTurn === false) throw new Error("interrupt priority requires a triggering steer");
    if (!this.local) throw new Error(`Main agent ${this.id} is owned by another Fabric process`);
    if (this.#inboxFence && !this.#inboxFence.active()) throw new Error("Main root rotated; address its successor");
    const message = request.message.trim();
    if (!message) throw new Error("Main agent message must not be empty");
    const sender = senderIdentity(request.from);
    if (!sender) throw new Error("Main agent message needs a sender with a string id and kind");
    if (request.priority === "interrupt") {
      const verified = request.verification === "mesh" || request.verification === "bridge";
      const session = (id: string): string => id.startsWith("session:") ? id.slice(8) : id;
      const ownRoot = sender.kind === "main" && session(sender.id) === session(this.id);
      if (!verified || !(ownRoot || this.interruptAuthority.isSupervisor?.(sender, this.id) === true)) {
        throw new FabricInterruptNotAuthorizedError(sender.id, this.id);
      }
    }
    const deliveryId = typeof request.deliveryId === "string" && request.deliveryId ? request.deliveryId : undefined;
    if (deliveryId !== undefined) {
      // A durable id needs the journal: without one (closed at shutdown or reload, or never
      // opened) the sender keeps its record for a later drain (Astra round 3 finding 2, pi-fabric#160).
      if (!this.#journal) throw new Error("Main has no follow-up journal open; retry the durable delivery later");
      const admitted = this.#admitted(deliveryId);
      if (admitted) return { queued: true, messageId: admitted, routed: "main", duplicate: true, triggered: false };
    }
    if (request.priority === "interrupt") {
      const now = performance.now();
      for (const [id, at] of this.#lastInterrupt) if (now - at >= 60_000) this.#lastInterrupt.delete(id);
      if (this.#lastInterrupt.has(sender.id)) throw new FabricInterruptRateLimitedError(sender.id, this.id);
    }
    const admittedCauses = admittedFabricWakeCauses(request);
    const admissionTopic = request.admissionTopic ?? (admittedCauses?.length === 1 ? admittedCauses[0]!.topic : undefined);
    const admissionKey = admittedCauses?.length === 1 ? admittedCauses[0]!.key : deliveryId;
    const derivedWake = mainWakeCause(sender, request.source, request.delivery, admissionTopic, admissionKey);
    // Some live producers admit several envelopes, or a writer distinct from the
    // routed sender. Their authenticated fields are not retained by this journal;
    // never reconstruct those origins from the serialized diagnostic on restart.
    const unattributedOnReplay = admittedCauses && (admittedCauses.length > 1 || admittedCauses.some(cause =>
      cause.cause !== derivedWake.cause || cause.from.id !== derivedWake.from.id ||
      cause.from.name !== derivedWake.from.name || cause.from.kind !== derivedWake.from.kind));
    const item: HeldAgentMessage = {
      id: randomUUID(),
      ...(request.outcomeSend ? { outcomeSend: request.outcomeSend } : {}),
      from: sender,
      ...(admittedCauses?.length ? { wakeCause: admittedCauses[0]!,
        ...(admittedCauses.length > 1 ? { wakeCauses: admittedCauses } : {}),
      } : { wakeCause: derivedWake }),
      ...(unattributedOnReplay ? { unattributedOnReplay: true } : {}),
      ...(admissionTopic ? { admissionTopic } : {}),
      ...(admissionKey ? { admissionKey } : {}),
      ...(typeof request.inboxEventId === "string" ? { inboxEventId: request.inboxEventId } : {}),
      ...(typeof request.inboxTopic === "string" ? { inboxTopic: request.inboxTopic } : {}),
      ...(typeof request.inboxKey === "string" ? { inboxKey: request.inboxKey } : {}),
      ...(Array.isArray(request.inboxReceipts) && request.inboxReceipts.every((id) => typeof id === "string")
        ? { inboxReceipts: [...request.inboxReceipts] } : {}),
      ...((request.verification === "mesh" || request.verification === "bridge") &&
        mainSenderClaimAllowed(sender, deliveryId, request.source) ? {
        provenance: fabricTurnProvenance(sender, request.delivery === "nextTurn" ? "actor" : request.delivery, request.verification, request.principal),
      } : {}),
      ...(request.source === "actor-output" || request.source === "fabric-host" ? { source: request.source } : {}),
      message,
      sentAt: Date.now(),
      ...(request.data === undefined ? {} : { data: serializableData(request.data) }),
      ...(deliveryId === undefined ? {} : { deliveryId }),
    };
    // Retry required owner authority at the next delivery, even without a durable delivery id.
    this.#retryPendingHalt();
    // Only a project-agent's new peer escalation control envelope gets one run; halt stays latched.
    // Replays return above; producer/backlog delivery and sender diagnostics grant no wake.
    const peerSteerWake = this.#halted && this.peerSteerResume && !this.#peerSteerInFlight && request.delivery === "steer" && request.triggerTurn !== false &&
      (request.verification === "mesh" || request.verification === "bridge") && sender.kind === "main" && sender.id !== this.id &&
      request.source === undefined && request.admissionTopic === undefined && !request.priority &&
      admittedCauses?.length === 1 && admittedCauses[0]!.cause === "steer" &&
      admittedCauses[0]!.topic === "fabric.control.command" &&
      admittedCauses[0]!.from.id === sender.id && admittedCauses[0]!.from.kind === "main" &&
      !this.#reloading && this.#bindingsLive && this.#context !== undefined && extensionContextLive(this.#context) &&
      this.#context.isIdle() && (this.#context as { isSettling?: () => boolean }).isSettling?.() === false &&
      !this.#context.signal?.aborted && !promptPending(this.#context) && !this.#haltIndexUnknown &&
      !this.#providerHeld() && !this.#providerAttemptInFlight;
    if (peerSteerWake) this.#peerSteerInFlight = true;
    const restorePeerWakeOnFailure = (): void => { if (peerSteerWake) this.halt(); };
    const triggerTurn = (request.triggerTurn ?? true) && (peerSteerWake || !this.#halted);
    const operatorHalted = this.#halted && !peerSteerWake;
    const providerHeld = triggerTurn && request.delivery !== "nextTurn" &&
      !this.#context?.signal?.aborted && (this.#providerHeld() || this.#providerAttemptInFlight);
    if (providerHeld && request.delivery === "steer") {
      item.deliverAs = request.delivery;
      item.triggerTurn = true;
    }
    if (this.#reloading) {
      if (!this.#journal) throw new Error("Main has no follow-up journal open; retry after reload");
      item.deliverAs = request.delivery;
      item.triggerTurn = triggerTurn;
      this.#admit(item);
      this.#held.push(item);
      try { this.#save(); } catch (error) { this.#held.pop(); throw error; }
      return { queued: true, messageId: item.id, routed: "main", triggered: false, ...this.queueDepth(item.from.id) };
    }
    if (request.priority === "interrupt" && triggerTurn && this.#context &&
      (this.#interrupts.size > 0 || (!this.#interruptUsedThisTurn && !this.#context.signal?.aborted && !this.#context.isIdle() && this.#tools.size > 0))) {
      if (typeof this.#context.abort !== "function") throw new Error("Main host has no tool abort hook");
      item.deliverAs = "steer";
      item.triggerTurn = true;
      this.#admit(item);
      const index = this.#interrupts.size;
      this.#held.splice(index, 0, item);
      try { this.#save(); } catch (error) { this.#held.splice(index, 1); throw error; }
      const abort = !this.#interruptUsedThisTurn;
      this.#interrupts.add(item.id);
      // Same native cancellation path as Escape. Do not queue into Pi's aborted run:
      // agent_settled hands this message over only after all tool results are persisted.
      if (abort) {
        this.#interruptUsedThisTurn = true;
        this.#interruptResume = true;
        this.#lastInterrupt.set(sender.id, performance.now());
        this.#context.abort();
      }
      return { queued: true, messageId: item.id, routed: "main", triggered: false,
        reason: abort ? "interrupt pending settlement" : "interrupt already used this turn; ordinary steer" };
    }
    let triggered: boolean | undefined = false;
    let replaced: HeldAgentMessage | undefined;
    // Pi releases its own followUp queue only when Main has no more work, so a Main that
    // chains turns reads it an hour late (smarty-dev#1495). Fabric holds a triggering
    // followUp for a busy Main instead: turn_end flushes the due ones as one steer, and
    // agent_settled releases the rest as a followUp. A non-triggering one never waited.
    const held = providerHeld || (request.delivery === "followUp" && triggerTurn && this.#drainActive());
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
        item.supersededSends = [...(replaced.supersededSends ?? []), ...(replaced.outcomeSend ? [replaced.outcomeSend] : [])];
        // Preserve every superseded source receipt. Admission charges the merged metadata
        // before replacing or consuming the held item; refuse over-budget merges, never truncate.
        if (replaced.inboxReceipts?.length) {
          item.inboxReceipts = [...new Set([...replaced.inboxReceipts, ...(item.inboxReceipts ?? [])])];
        }
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
      this.#recordSuperseded(item);
      this.#trySave();
      if (providerHeld) {
        this.#scheduleProviderWake();
        this.#armProviderDeadTimer();                   // a new delivery is an edge too; no-op unless pending
      } else if (this.#context && promptPending(this.#context)) this.#wakeAfterPreflight();
      else if (this.#context?.isIdle()) {
        const canTrigger = !this.#halted && !this.#providerHeld() && !this.#context?.signal?.aborted;
        this.#release(true);
        triggered = canTrigger && this.#sent.includes(item);
      }

    } else if (deliveryId !== undefined || this.#journal) {
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
        restorePeerWakeOnFailure();
        throw new Error(`Main could not record the message: ${error instanceof Error ? error.message : String(error)}`);
      }
      try {
        triggered = this.#send([item], request.delivery, triggerTurn, false, peerSteerWake);
      } catch (error) {
        this.#sent.splice(this.#sent.indexOf(item), 1);
        this.#trySave();
        restorePeerWakeOnFailure();
        throw error;
      }
    } else {
      try { triggered = this.#send([item], request.delivery, triggerTurn, false, peerSteerWake); }
      catch (error) { restorePeerWakeOnFailure(); throw error; }
    }
    return {
      queued: true, messageId: item.id, routed: "main",
      ...(triggered === undefined ? {} : { triggered }),
      ...(operatorHalted ? { halted: true as const, reason: "operator-halt" } :
        providerHeld ? { reason: this.#providerHoldReason() } : {}),
      ...(replaced ? { coalesced: true as const, replacedMessageId: replaced.id } : {}),
      // Only a followUp that waits in the held queue can be stalled; a non-triggering one went
      // straight to Pi (#123 review F1).
      ...(held ? this.#depthReport(item.from.id) : this.queueDepth(item.from.id)),
    };
  }

  /** Presence maintenance confirms only the existing durable native receipt path. */
  confirmInbox(): void { this.#confirm(); }

  /** Admit a claimed rotation carrier with its ORIGINAL native message id. The existing
   * delivery-id index and journal-before-Pi barrier make a crash/retry idempotent. */
  receiveInboxItem(original: HeldAgentMessage): void {
    if (!this.#journal || !this.local) throw new Error("Main inbox is not open");
    if (this.#inboxFence && (!this.#inboxFence.active() || !this.#inboxFence.owns(original.id))) throw new Error("Main inbox claim belongs elsewhere");
    const deliveryId = original.deliveryId ?? `inbox:${original.id}`;
    if (this.#admitted(deliveryId) || this.#refreshDelivered(true).has(original.id)) return;
    const { handed: _handed, ...item } = original;
    const retained = { ...item, deliveryId };
    this.#admit(retained);
    this.#held.push(retained);
    try { this.#save(); } catch (error) { this.#held.pop(); throw error; }
    if (this.#context?.isIdle()) this.#release(true);
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
    const stalled = !this.#providerHeld() && this.#stallS > 0 && this.#context?.isIdle() === true && this.queueDepth().oldestAgeS >= this.#stallS &&
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
    const item = [...this.#unverified, ...this.#sent, ...this.#held, ...this.#outcomePending].find((item) =>
      item.deliveryId === deliveryId || item.supersedes?.includes(deliveryId));
    if (item) {
      // A replayed rename can be visible even though its post-rename barriers failed.
      // Reading it (or a failed best-effort replay save) is not a durability receipt.
      // Re-establish the complete journal barrier chain before duplicate acceptance
      // lets the resident client delete its source; propagate failure for a later drain.
      this.#save();
      return item.id;
    }
    if (this.#consumed.has(deliveryId)) {
      // The same uncertainty applies to a recovered consumed-ID replacement, even
      // with no payload journal left. Force its own barriers, not just the journal's.
      this.#consumedDirty = true;
      this.#save();
      return deliveryId;
    }
    if (this.#refreshDelivered(true).has(deliveryKey(deliveryId))) return deliveryId;
    return undefined;
  }

  /** Remember that this item's delivery id and its superseded ids are done. */
  #consume(item: HeldAgentMessage): void {
    this.#replayed.delete(item.id);
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

  #providerSnapshot(): PersistedProviderState | undefined {
    if (!this.#unclassifiedFailures && this.#unclassifiedSince === undefined && !this.#runUnclassified && !this.#providerDead) return undefined;
    return {
      unclassifiedFailures: Math.min(this.#unclassifiedFailures, MAX_PERSISTED_UNCLASSIFIED_FAILURES),
      ...(this.#unclassifiedSince === undefined ? {} : { unclassifiedSince: this.#unclassifiedSince }),
      ...(this.#unclassifiedAlarmed ? { unclassifiedAlarmed: true } : {}),
      ...(this.#runUnclassified ? { runUnclassified: true } : {}),
      ...(this.#providerDead === undefined ? {} : { dead: { ...this.#providerDead } }),
    };
  }

  #saveIndex(): void {
    const file = this.#consumedPath();
    if (!file || (!this.#consumedDirty && !this.#providerStateDirty) || this.#haltIndexUnknown) return;
    const provider = this.#providerSnapshot();
    writeFileAtomic(file, JSON.stringify({ version: 1, ids: [...this.#consumed], ...(this.#halted ? { halted: true } : {}),
      ...(provider === undefined ? {} : { provider }) }), { durable: true });
    this.#consumedDirty = false;
    this.#providerStateDirty = false;
    pendingHalts().delete(file); // Only the completed durability barriers commit authority.
  }

  /** Write the held and unconfirmed followUps (0600), or remove the journal when none are left. */
  #save(): void {
    if (!this.#journal) return;
    // Before the journal: an item leaves the journal only once its id is recorded as consumed.
    this.#saveIndex();
    const items = [...this.#unverified, ...this.#sent, ...this.#held, ...this.#outcomePending];
    if (!items.length) { fs.rmSync(this.#journal, { force: true }); return; }
    writeFileAtomic(this.#journal, JSON.stringify({ version: 1, items }), { durable: true });
  }

  #trySave(indexOnly = false): void {
    try { indexOnly ? this.#saveIndex() : this.#save(); } catch (error) {
      // Delivery bookkeeping can retry on its next change, but an owner stop is required
      // authority. Its dirty handoff stays live until a save succeeds, with one notice.
      const pending = pendingHalts().get(this.#consumedPath()!);
      if (pending && !pending.warned) {
        pending.warned = true;
        const message = `Cannot persist Main halt index ${this.#consumedPath()}; keeping Main halted across reload and retrying at the next opportunity`;
        diagnostics.warn("src/main-agent.ts", `[pi-fabric] ${message}`, error);
        try { if (this.#context?.hasUI) notifyDiagnostic(this.#context, "src/main-agent.ts", message, "warning"); } catch { /* diagnostics must not lift the stop */ }
      }
    }
  }

  #retryPendingHalt(): void {
    if (this.#consumedDirty && pendingHalts().has(this.#consumedPath()!)) this.#trySave(true);
  }

  /**
   * Index the session entries persisted since the last call, whatever branch they are on.
   *
   * With a session file, only what the file holds counts: Pi inserts an entry in memory before it
   * writes it, and does not roll it back when the write fails, so getEntries() is no receipt
   * (security round 3 S1 on pi-fabric#160). Pi also defers a new session's first write until its
   * first assistant message; until then nothing is confirmed and every item stays journalled.
   * Appends are indexed incrementally; a shorter file, replacement inode or another path starts over.
   *
   * An in-memory session has no durable store at all: its entry list is the only record, so it
   * counts as it did before.
   */
  #refreshDelivered(failClosed = false): Set<string> {
    try {
      const manager = this.#context?.sessionManager as
        { getEntries?: () => readonly unknown[]; getSessionFile?: () => string | undefined; isPersisted?: () => boolean } | undefined;
      const file = manager?.isPersisted?.() === false ? undefined : manager?.getSessionFile?.();
      if (file) { this.#indexSessionFile(file); return this.#delivered; }
      const entries = manager?.getEntries?.() ?? [];
      if (this.#source !== "" || this.#scanned === undefined || entries.length < this.#scanned) this.#restartIndex("");
      for (let index = this.#scanned!; index < entries.length; index++) addDelivered(this.#delivered, entries[index] as SessionEntryLike);
      this.#scanned = entries.length;
    } catch (error) {
      // A failed session barrier supplies no receipt: keep every journalled payload.
      // A duplicate lookup must also retain the resident source, not fall through to
      // admitting/sending a second copy because its persisted receipt is uncertain.
      if (failClosed) throw new Error(`Main could not confirm the session receipt: ${error instanceof Error ? error.message : String(error)}`);
      return new Set();
    }
    return this.#delivered;
  }

  #restartIndex(source: string): void {
    this.#delivered = new Set();
    this.#source = source;
    this.#scanned = 0;
    this.#sessionFileIdentity = undefined;
  }

  /** Read the complete lines appended to the session file since the last call, in 1 MiB chunks. */
  #indexSessionFile(file: string): void {
    if (!withConfirmedSessionFile(file, (fd, stat) => {
      const size = stat.size;
      const identity = `${stat.dev}:${stat.ino}`;
      if (this.#source !== file || this.#scanned === undefined || size < this.#scanned || this.#sessionFileIdentity !== identity) {
        this.#restartIndex(file);
      }
      this.#sessionFileIdentity = identity;
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
    })) this.#restartIndex(file); // Not written (or removed): no cached persisted receipt.
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
      if (delivered.has(item.id)) {
        this.#consume(item);
        this.#queueOutcomeRetry(item, true);
        continue;
      }
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
    // All confirmed carriers have left the native arrays before the reservation save.
    this.#retryPendingOutcomes();
    this.#trySave();
  }

  #recordOutcome(send: DeliverySend | undefined, outcome: DeliveryOutcome, reason: string): boolean {
    const root = this.meshRoot ?? (this.#journal ? path.dirname(path.dirname(this.#journal)) : undefined);
    if (!send || !root) return true;
    try { appendDeliveryOutcome(root, send, outcome, reason); return true; }
    catch (error) { diagnostics.warn("src/main-agent.ts", "[pi-fabric] cannot append Main delivery outcome; retaining journal", error); return false; }
  }

  #recordSuperseded(item: HeldAgentMessage): boolean {
    const pending = item.supersededSends ?? [];
    const before = pending.length;
    while (pending.length && this.#recordOutcome(pending[0], "superseded", "replaced by a newer message for the same key before delivery")) pending.shift();
    if (!pending.length) delete item.supersededSends;
    return pending.length !== before;
  }

  #queueOutcomeRetry(item: HeldAgentMessage, nativeDelivered: boolean): void {
    if (!item.outcomeSend && !item.supersededSends?.length) return;
    item.outcomePending = true;
    if (nativeDelivered) item.outcomeNativeDelivered = true;
    else delete item.outcomeNativeDelivered;
    // Legacy journals start at zero; every post-retirement append needs a durable reservation.
    item.outcomeRetryAttempts ??= 0;
    if (!this.#outcomePending.includes(item)) this.#outcomePending.push(item);
  }

  #outcomeDeadLetterPath(item: HeldAgentMessage): string {
    // Never place an arbitrary legacy item.id in a filename. Include the journal path so two
    // roots sharing a directory cannot alias one another's private record.
    const key = createHash("sha256").update(JSON.stringify([this.#journal, item.id, item.deliveryId ?? null])).digest("hex").slice(0, 32);
    return path.join(path.dirname(this.#journal!), `main-delivery-outcome-dead-letter-${key}.json`);
  }

  /** Persist a private, allowlisted record after outcome bookkeeping is exhausted. */
  #writeOutcomeDeadLetter(item: HeldAgentMessage): boolean {
    if (!this.#journal) return false;
    const sends = [item.outcomeSend, ...(item.supersededSends ?? [])]
      .map((send) => deliverySend(send))
      .filter((send): send is DeliverySend => send !== undefined);
    const letter = {
      version: 1,
      kind: "main-delivery-outcome-dead-letter",
      native: item.outcomeNativeDelivered === true ? "delivered" : "unknown",
      bookkeeping: "unknown",
      reason: `${item.outcomeNativeDelivered === true ? "native carrier delivered" : "native carrier receipt unknown"}; ${OUTCOME_DEAD_LETTER_REASON}`,
      carrier: { id: item.id, ...(item.deliveryId === undefined ? {} : { deliveryId: item.deliveryId }) },
      sends,
      attempts: Math.min(item.outcomeRetryAttempts ?? MAIN_DELIVERY_OUTCOME_RETRY_LIMIT, MAIN_DELIVERY_OUTCOME_RETRY_LIMIT),
      at: Date.now(),
    };
    try {
      writeFileAtomic(this.#outcomeDeadLetterPath(item), JSON.stringify(letter), { durable: true, mode: 0o600, dirMode: 0o700 });
      return true;
    } catch (error) {
      diagnostics.warn("src/main-agent.ts", "[pi-fabric] cannot persist Main delivery outcome dead-letter; retaining journal", error);
      return false;
    }
  }

  /** Retry outcome-only journal items; they never block native delivery or re-enter Pi. */
  #retryPendingOutcomes(): boolean {
    if (!this.#journal) return false; // No durable reservation barrier, so no optional append.
    let changed = false;
    const kept: HeldAgentMessage[] = [];
    const retryable: HeldAgentMessage[] = [];
    const exhausted: HeldAgentMessage[] = [];
    for (const item of this.#outcomePending) {
      if ((item.outcomeRetryAttempts ?? 0) >= MAIN_DELIVERY_OUTCOME_RETRY_LIMIT) exhausted.push(item);
      else retryable.push(item);
    }
    // Once exhausted, this is the only operation permitted: never append another outcome.
    for (const item of exhausted) {
      if (this.#writeOutcomeDeadLetter(item)) {
        delete item.outcomePending;
        this.#consume(item);
        changed = true;
      } else kept.push(item);
    }
    // A reservation save must not serialize a completed exhausted item as a native carrier
    // after its outcomePending flag was cleared. Retain failed letters and all retryable items.
    this.#outcomePending.splice(0, this.#outcomePending.length, ...kept, ...retryable);
    // Reserve every retry in the durable journal before attempting any optional append. If the
    // reservation barrier fails, do not append: the next boundary can retry the same reservation.
    const reserved: Array<{ item: HeldAgentMessage; previous: number }> = [];
    for (const item of retryable) {
      const previous = item.outcomeRetryAttempts ?? 0;
      item.outcomeRetryAttempts = previous + 1;
      reserved.push({ item, previous });
    }
    if (reserved.length) {
      try {
        this.#save();
        changed = true;
      } catch {
        for (const { item, previous } of reserved) {
          if (item.outcomeRetryAttempts !== previous + 1) continue;
          if (previous === 0) delete item.outcomeRetryAttempts;
          else item.outcomeRetryAttempts = previous;
          kept.push(item);
        }
        const before = this.#outcomePending.length;
        this.#outcomePending.splice(0, this.#outcomePending.length, ...kept);
        return changed || kept.length !== before;
      }
    }
    const exhaustedAfterRetry: HeldAgentMessage[] = [];
    for (const item of retryable) {
      const supersededChanged = this.#recordSuperseded(item);
      const ownOutcomeHandled = item.outcomeSend === undefined ||
        (item.outcomeNativeDelivered === true &&
          this.#recordOutcome(item.outcomeSend, "delivered", "consumed by Main as itself"));
      changed ||= supersededChanged;
      if (item.supersededSends?.length || !ownOutcomeHandled) {
        if ((item.outcomeRetryAttempts ?? 0) >= MAIN_DELIVERY_OUTCOME_RETRY_LIMIT) exhaustedAfterRetry.push(item);
        else kept.push(item);
        continue;
      }
      delete item.outcomePending;
      this.#consume(item);
      changed = true;
    }
    for (const item of exhaustedAfterRetry) {
      if (this.#writeOutcomeDeadLetter(item)) {
        delete item.outcomePending;
        this.#consume(item);
        changed = true;
      } else kept.push(item);
    }
    if (kept.length !== this.#outcomePending.length) changed = true;
    this.#outcomePending.splice(0, this.#outcomePending.length, ...kept);
    return changed;
  }

  /** Drop the handed-over followUps the session now holds (the only way one leaves the journal). */
  #confirm(): void {
    this.#retryPendingHalt();
    let outcomesChanged = false;
    for (const item of this.#held) outcomesChanged = this.#recordSuperseded(item) || outcomesChanged;
    const delivered = this.#refreshDelivered();
    const before = this.#sent.length;
    const kept: HeldAgentMessage[] = [];
    for (const item of this.#sent) {
      if (!delivered.has(item.id)) {
        kept.push(item);
        continue;
      }
      // Retire native delivery immediately, but persist the outcome obligation and its first
      // reservation together before any optional append. A rejected save cannot bypass the cap.
      this.#consume(item);
      this.#queueOutcomeRetry(item, true);
    }
    this.#sent.splice(0, this.#sent.length, ...kept);
    outcomesChanged = this.#retryPendingOutcomes() || outcomesChanged;
    if (this.#sent.length !== before || outcomesChanged) this.#trySave();
  }

  /** After a restart: every journalled followUp the session does not hold goes back in the queue. */
  #replay(): void {
    if (!this.#journal) return;
    // A readable running index can be stale after a rejected stop write. The live handoff
    // overrides it before any replay/control can trigger, and remains dirty for a retry.
    if (pendingHalts().has(this.#consumedPath()!)) {
      this.#halted = true;
      this.#consumedDirty = true;
    }
    try {
      const parsed = JSON.parse(readFileRetrying(this.#consumedPath()!)) as
        { version?: unknown; ids?: unknown; halted?: unknown; provider?: unknown } | null;
      if (!parsed || parsed.version !== 1 || !Array.isArray(parsed.ids) ||
        parsed.ids.some((id) => typeof id !== "string") ||
        (parsed.halted !== undefined && typeof parsed.halted !== "boolean")) throw new Error("Malformed halt index");
      this.#halted ||= parsed.halted === true;
      for (const id of parsed.ids.slice(-CONSUMED_DELIVERIES_MAX)) this.#consumed.add(id);
      if (parsed.provider !== undefined) {
        const provider = persistedProviderState(parsed.provider);
        if (provider) {
          const now = Date.now();
          const stale = provider.unclassifiedSince !== undefined && now - provider.unclassifiedSince > UNCLASSIFIED_PROVIDER_FAILURE_WINDOW_MS;
          this.#unclassifiedFailures = stale ? 0 : provider.unclassifiedFailures;
          this.#unclassifiedSince = stale ? undefined : provider.unclassifiedSince;
          this.#unclassifiedAlarmed = provider.dead?.kind === "unclassified" && provider.dead.alarmed ||
            (!stale && provider.unclassifiedAlarmed === true);
          this.#runUnclassified = stale ? false : provider.runUnclassified === true;
          this.#providerDead = provider.dead === undefined ? undefined : { ...provider.dead };
          if (stale) this.#providerStateDirty = true;
        }
      }
    } catch (error) {
      // Only absence proves there is no persisted owner stop. Unknown authority fails closed,
      // including when the payload journal is absent; incoming control messages stay passive.
      if ((error as { code?: unknown } | null)?.code !== "ENOENT") {
        this.#halted = true;
        this.#haltIndexUnknown = true;
        diagnostics.warn("src/main-agent.ts", `[pi-fabric] cannot read halt index ${this.#consumedPath()}; keeping Main halted until user input`, error);
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
            diagnostics.warn("src/main-agent.ts", `[pi-fabric] dropped a malformed followUp from the journal: ${String((item as { id?: unknown } | null)?.id)}`);
            continue;
          }
          // A policy this runtime cannot read is dropped: the item is then released as a held
          // followUp, as before policies were journalled.
          const { deliverAs, triggerTurn, supersedes, provenance, source, wakeCause, wakeCauses, outcomeRetryAttempts, outcomeNativeDelivered, ...rest } = item;
          const retryAttempts = Number.isSafeInteger(outcomeRetryAttempts)
            ? Math.max(0, Math.min(MAIN_DELIVERY_OUTCOME_RETRY_LIMIT, outcomeRetryAttempts as number))
            : undefined;
          // Serialized diagnostics are not admission evidence. Reconstruct only when
          // the recorded envelope sender agrees with the displayed/routed sender.
          const admission = copyFabricProvenance(provenance);
          const via = admission?.via;
          const verified = admission?.sender.verified;
          const senderMatches = admission?.sender.id === sender.id &&
            (admission.sender.name || admission.sender.id) === sender.name &&
            admission.sender.kind === (verified === "bridge" ? "remote" : sender.kind);
          items.push({
            ...rest, from: sender,
            ...(senderMatches && mainSenderClaimAllowed(sender, item.deliveryId, source) ? {
              wakeCause: mainWakeCause(sender, source, via === "steer" ? "steer" : "followUp",
                typeof item.admissionTopic === "string" ? item.admissionTopic : undefined,
                typeof item.admissionKey === "string" ? item.admissionKey : item.deliveryId),
            } : {}),
            // Only a recorded admission method permits a claim. Old journals (native or
            // bridged) are UNKNOWN; payload fields and a missing bridge marker prove nothing.
            ...(senderMatches && mainSenderClaimAllowed(sender, item.deliveryId, source) ? {
              provenance: fabricTurnProvenance(sender, via === "steer" || via === "followUp" || via === "actor" || via === "replay"
                ? via : deliverAs === "steer" ? "steer" : "followUp", verified!, admission?.principal),
            } : {}),
            ...(source === "actor-output" || source === "fabric-host" ? { source } : {}),
            ...(retryAttempts === undefined ? {} : { outcomeRetryAttempts: retryAttempts }),
            ...(item.outcomePending === true && outcomeNativeDelivered === true ? { outcomeNativeDelivered: true as const } : {}),
            ...(Array.isArray(supersedes) ? { supersedes: supersedes.filter((id) => typeof id === "string") } : {}),
            ...(DIRECT_DELIVERIES.has(deliverAs) && typeof triggerTurn === "boolean" ? { deliverAs, triggerTurn } : {}),
          });
        }
      }
    } catch {
      this.#retryPendingHalt();                          // owner state survives an empty journal
      return;                                            // no journal
    }
    // The owner rule (dev-lead on pi-fabric#102): skip every id the session holds; a handoff
    // may still sit in Pi's queue, so it waits for a boundary to check; the rest is held again.
    const delivered = this.#refreshDelivered();
    // Outcome-only entries never re-enter Pi. Old journals without own-receipt metadata are
    // conservative unless the actual native receipt is still observable; consumed ids do not count.
    for (const item of items) {
      if (item.outcomePending === true) this.#queueOutcomeRetry(item,
        item.outcomeNativeDelivered === true || delivered.has(item.id));
    }
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
      if (item.outcomePending === true) continue;
      if (this.#inboxFence && !this.#inboxFence.owns(item.id)) continue;
      if (delivered.has(item.id) || delivered.has(chainOf(item)) || last.get(chainOf(item)) !== item ||
        (item.deliveryId !== undefined && this.#consumed.has(item.deliveryId))) {
        const nativeDelivered = delivered.has(item.id);
        this.#consume(item);
        if (nativeDelivered || item.supersededSends?.length) this.#queueOutcomeRetry(item, nativeDelivered);
        continue;
      }
      this.#recordSuperseded(item);
      last.delete(chainOf(item));                          // a duplicate id goes once
      this.#replayed.add(item.id);
      (item.handed ? this.#unverified : this.#held).push(item);
    }
    this.#retryPendingOutcomes();
    this.#trySave();
  }

  /**
   * Hold followUps for this local Main while it is busy, and flush those that have waited
   * flushMs at the next boundary between tool calls (turn_end, the hook the shell and
   * completion inboxes use). flushMs 0 keeps Pi's followUp queue, as before.
   */
  attachFollowUpDrain(context: ExtensionContext, flushMs: number, journal?: string, stallSeconds = 600,
    inboxFence?: { owns(id: string): boolean; active(): boolean }): void {
    this.closeFollowUpDrain();
    if (!this.local) return;
    this.#stallS = stallSeconds;
    this.#context = context;
    this.#bindingsLive = true;
    this.#closed = false;
    this.#reloading = false;
    this.#switching = false;
    this.#halted = false;
    this.#haltIndexUnknown = false;
    this.#journal = journal === undefined ? undefined : path.resolve(journal);
    this.#inboxFence = inboxFence;
    const on = (name: string, fn: (event: any, ctx: ExtensionContext) => unknown): void => {
      if (typeof this.pi.on !== "function") return;
      const off = (this.pi.on as (name: string, fn: (event: any, ctx: ExtensionContext) => unknown) => unknown)(name, fn);
      if (typeof off === "function") this.#unsubscribe.push(off as () => void);
    };
    // Native replacement aborts/settles the old run BEFORE session_shutdown. Fence
    // that boundary now so undelivered followUps cannot be appended to the old root.
    // A later extension may cancel the switch; explicit owner input reopens that inbox.
    on("session_before_switch", (event: { reason?: string }) => {
      if (event.reason === "new" || event.reason === "resume") { this.#switching = true; this.prepareReload(); }
    });
    on("input", (event: { source?: string }, ctx) => {
      this.#context = ctx;
      if (event.source === "extension") return;
      resetPrincipalDiagnosticTurn(this.pi, ctx);
      this.#peerSteerInFlight = false;
      this.#interruptUsedThisTurn = false;
      this.#interruptResume = false;
      if (this.#switching) { this.#switching = false; this.#reloading = false; this.#bindingsLive = true; }
      this.#halted = false;
      this.#recoverProvider(false);
      this.#haltIndexUnknown = false;
      this.#consumedDirty = true;
      this.#suspended = false;
      this.#trySave();
    });
    on("tool_execution_start", (event: { toolCallId: string }, ctx) => {
      this.#context = ctx;
      this.#tools.add(event.toolCallId);
    });
    on("tool_execution_end", (event: { toolCallId: string }) => { this.#tools.delete(event.toolCallId); });
    // Set gates before either drain branch can reconcile a lost Pi handoff. A turn error
    // precedes Pi's retry/overflow recovery decision, so never persist it as an owner stop.
    on("turn_end", (event: { message?: { stopReason?: string; errorMessage?: string; diagnostics?: unknown } }, ctx) => {
      const reason = event.message?.stopReason;
      this.#compactionDecline = undefined;
      if (ctx.signal?.aborted || reason === "aborted") { if (!this.#interrupts.size) this.halt(); }
      else if (reason === "error") {
        // smarty-dev#8742: a request the provider will reject again is not backoff.
        const failure = classifyProviderError(event.message);
        // Counted at settle, after Pi's own retries: one failed run is one failure.
        this.#runUnclassified = failure?.basis === "default";
        this.#providerStateDirty = true;
        this.#trySave(true);
        const dead = this.#providerDead;
        if (failure && !failure.retryable) this.#recordNonRetryable(failure, ctx);
        // An unclassified incident's one post-compaction attempt failed the same way: hold again, no second alarm.
        else if (this.#runUnclassified && dead?.kind === "unclassified" && dead.phase === "probation") this.#raiseProviderDead("unclassified-repeated");
        else this.#recordProviderFailure(true);
      } else if (reason !== undefined) {
        const dead = this.#providerDead;
        if (dead?.phase === "probation") this.#notify(`Main provider recovered after compaction (was ${providerDeadLabel(dead).slice("provider-dead (".length, -1)})`, "info");
        this.#recoverProvider();
        this.#suspended = false;
      }
    });
    const settleGate = (event: { outcome?: string; aborted?: boolean }, ctx: ExtensionContext, finalSettle = false): void => {
      if (ctx.signal?.aborted || event.aborted === true || isCompactionCancelled(this.#compactionDecline) ||
        (event.outcome === "aborted" && !this.#compactionDecline)) { if (!this.#interrupts.size) this.halt(); }
      // turn_end already classified a non-retryable failure: recover at idle, never back off.
      else if (event.outcome === "error" && this.#providerDead) { this.#runUnclassified = false; this.#armProviderDeadTimer(); }
      // The third unclassified failure in a row raises provider-dead instead of more silent backoff.
      else if (event.outcome === "error" && this.#countUnclassified()) { /* alarmed and held */ }
      else if (event.outcome === "error") this.#recordProviderFailure();
      else if (event.outcome === "completed") this.#recoverProvider();
      else if (finalSettle && event.outcome === undefined) this.#countUnclassified();
      // Older hosts omit outcome: only the final boundary counts an unclassified turn_end.
      // Every settle is an idle edge for a pending recovery step (smarty-dev#8742: event-driven, no polling).
      if (this.#providerDead && !this.#halted) this.#armProviderDeadTimer();
    };
    on("agent_before_settle", (event, ctx) => settleGate(event, ctx));
    on("agent_settled", (event, ctx) => settleGate(event, ctx, true));
    let interruptedSettle = false;
    on("agent_settled", (_event, ctx) => {
      this.#context = ctx;
      this.#peerSteerInFlight = false;
      this.#tools.clear();
      interruptedSettle = this.#interrupts.size > 0;
      while (this.#held[0] && this.#interrupts.has(this.#held[0].id)) {
        const id = this.#held[0].id;
        if (!this.#handOver(1, "steer", true, false)) break;
        this.#interrupts.delete(id);
      }
    });
    // Observe the operation in BOTH drain modes. Pi also reports an extension's benign
    // decline as aborted, but only the operation signal proves an owner cancellation.
    on("session_before_compact", (event: { reason?: string; signal?: AbortSignal }) => {
      this.#stopOperation();
      this.#operation = event.signal;
      this.#compactionDecline = undefined;
      // Native manual cancellation is owner intent even when Pi has already emitted
      // session_compact and is awaiting later handlers (some hosts emit no late failure).
      // Pi uses the same controller for its deadline, which is NOT owner intent.
      if (event.reason === "manual" && event.signal) {
        const signal = event.signal;
        const cancelled = () => { if (isCompactionCancelled(signal)) this.halt(); };
        signal.addEventListener("abort", cancelled, { once: true });
        this.#offOperationAbort = () => signal.removeEventListener("abort", cancelled);
        if (signal.aborted) cancelled();
      }
    });
    on("session_compact_failed", (event: { reason?: string; aborted?: boolean; willRetry?: boolean; errorMessage?: string }, ctx) => {
      this.#context = ctx;
      // The aborted bit alone is ambiguous: an earlier handler may decline and stop
      // dispatch before we see the signal. Escape/halt is explicit; a seen signal
      // proves cancellation unless the native deadline expired. Never manufacture
      // durable owner intent from a decline or recoverable operation timeout.
      const decline = takeCompactionDecline(this.pi);
      const operation = decline && decline.reason === event.reason ? decline.signal : this.#operation;
      const ownerCancelled = ctx.signal?.aborted || (event.aborted && isCompactionCancelled(operation));
      // Exact Pi no-op outcomes, with or without the host's error envelope. A provider
      // error merely quoting these phrases is still a recoverable failure, not a no-op.
      const message = event.errorMessage?.replace(/^Compaction failed: /, "");
      const benign = message === "Already compacted" || message === "Nothing to compact (session too small)" ||
        message === "Compaction cancelled";
      const dead = this.#providerDead;
      if (ownerCancelled) this.halt();
      else if (dead?.phase === "compacting" && event.reason === "manual") {
        // Fabric's own recovery compaction (ctx.compact is always "manual") ended without an owner
        // cancel: Pi's native deadline (aborted or not), an error, a veto or a no-op. Recovery failed:
        // one alarm and hold, never a silent provider-dead (smarty-dev#8742 item 4). Its error text
        // may echo provider content: it stays out of the alarm.
        this.#raiseProviderDead("compaction-failed");
      } else if (event.aborted && event.reason !== "manual" && operation && !operation.aborted) {
        // Pi settles an automatic extension veto as aborted. Retain its provenance
        // rather than converting the outcome into a durable owner stop.
        this.#compactionDecline = operation;
      } else if (!event.aborted && !event.willRetry && dead &&
        (dead.phase === "recovering" || dead.phase === "compacting")) {
        // Another compaction failed during the incident, including a benign no-op: alarm, never back off.
        this.#raiseProviderDead("compaction-failed");
      } else if (!event.aborted && !event.willRetry && !benign) {
        this.#recordProviderFailure(true);
      }
      // A finished (failed) compaction is an idle edge for a recovery step that found Main compacting.
      this.#armProviderDeadTimer();
      // Unsuccessful boundaries never wake their queued replay, even for a benign rejection.
      // Later peer deliveries retain permission after a veto; errors await recovery,
      // but only a real owner stop survives reload.
      if (event.reason === "manual" && ctx.isIdle()) this.#release(false);
      this.#stopOperation();
    });
    on("session_compact", (event: { reason?: string }, ctx) => {
      this.#context = ctx;
      // Compaction can unblock one retry, but its LLM-free success is not provider recovery.
      // Retain the failure/in-flight guard so later batches wait for that retry's outcome.
      this.#providerRetryAt = 0;
      this.#stopProviderWake();
      this.#compactionDecline = undefined;
      // smarty-dev#8742: any completed compaction (Fabric's recovery, a human /compact, Pi's
      // overflow recovery) ends a non-retryable incident's compaction step.
      this.#providerCompacted();
      // Retain the signal through EVERY completion handler, in both drain modes.
      if (event.reason === "manual") this.#wakeWhenIdle();
      else this.#stopOperation();
    });
    on("agent_start", (_event, ctx) => {
      resetPrincipalDiagnosticTurn(this.pi, ctx);
      // The HOLD-induced restart belongs to the interrupted turn, not a fresh abort budget.
      if (!this.#interruptResume) this.#interruptUsedThisTurn = false;
      this.#interruptResume = false;
      this.#compactionDecline = undefined;
      this.#stopOperation();
      this.#providerFailureRecorded = false;
      this.#stopProviderWake();
      // A run is the attempt (or its settle re-arms recovery); stop waiting for idle.
      this.#stopProviderDeadTimer();
      this.#stopProviderDeadDeadline();
    });
    // The end of a run, and a finished /tree summary, are idle edges for a pending recovery step.
    on("agent_end", () => { this.#armProviderDeadTimer(); });
    on("session_tree", () => { this.#armProviderDeadTimer(); });
    if (!(flushMs > 0) || typeof this.pi.on !== "function") {
      // Drain off: what an earlier drain journalled goes to Pi's own queue, under the same rule.
      // Each stays in the journal until the session holds it (review/astra F4 on pi-fabric#102).
      this.#replay();
      this.#resumeProviderAfterAttach();
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
      // Pi defers the interrupt prompt until every settled handler returns. Keep older
      // followUps held for that new run, rather than appending them ahead of the HOLD.
      if (!interruptedSettle) this.#release(completed);
    });
    // Manual /compact makes Main busy without a run, so no settle follows it. After a compaction
    // that completed, wake Main for what it held; after one that was cancelled or failed, append
    // them for the next run instead (review/astra on pi-fabric#102). A compaction inside a run
    // leaves them to that run's boundaries.
    // The operation's own abort signal says whether the user cancelled it: Pi can report that
    // cancel after Main is idle again, or not at all (review/astra on pi-fabric#102).
    on("session_before_tree", (event: { signal?: AbortSignal }) => { this.#stopOperation(); this.#operation = event.signal; });
    // A branch summary on /tree navigation is the same: busy without a run. A cancelled one
    // emits nothing, so its followUps wait for the next run.
    on("session_tree", (_event, ctx) => {
      this.#context = ctx;
      this.#wakeWhenIdle();
    });
    on("agent_start", (_event, ctx) => { this.#context = ctx; this.#suspended = false; this.#stopWake(); });
    this.#replay();
    this.#resumeProviderAfterAttach();
    if (this.#held.length && context.isIdle()) this.#release(true);
  }

  /** Stop holding; any held followUps go to Pi's own followUp queue, as before the drain. */
  closeFollowUpDrain(): void {
    this.#tools.clear();
    this.#interrupts.clear();
    this.#bindingsLive = false;
    this.#stopWake();
    for (const off of this.#unsubscribe.splice(0)) off();
    this.#closed = true;
    // With a journal, a reload or the next start replays them; handing them to Pi here as
    // well would deliver them twice. Without one, Pi's own queue takes them, as before.
    if (this.#journal) this.#trySave();
    else this.#release(true, true);
    this.#held.splice(0);
    this.#sent.splice(0);
    this.#unverified.splice(0);
    this.#consumed = new Set();
    this.#consumedDirty = false;
    this.#delivered = new Set();
    this.#source = undefined;
    this.#scanned = undefined;
    this.#sessionFileIdentity = undefined;
    this.#outcomePending.splice(0);
    this.#journal = undefined;
    this.#context = undefined;
    this.#providerReleaseUntil = undefined;
    this.#stopProviderDeadTimer();
    this.#stopProviderDeadDeadline();
    this.#stopOperation();
    this.#compactionDecline = undefined;
    takeCompactionDecline(this.pi);
  }

  #drainActive(): boolean {
    return this.#context !== undefined &&
      ((this.#providerReleaseUntil !== undefined && this.#held.length > 0) ||
        (!this.#closed && (this.#held.length > 0 || this.#context.isIdle() === false || promptPending(this.#context))));

  }

  /**
   * smarty-dev#2119: an agents.wait in this Main hit its 60 s cap. The wait ended so Main can see
   * news, so the next turn_end flushes every held followUp, not only those past flushMs.
   */
  flushHeldAtNextBoundary(): void {
    if (this.#flushMs > 0 && !this.#closed) this.#flushAll = true;
  }

  #flushDue(): void {
    if (!this.#held.length || this.#suspended || this.#reloading || this.#providerAttemptInFlight) return;
    if (this.#halted || this.#context?.signal?.aborted) { this.#release(false); return; }
    if (this.#providerHeld()) { this.#scheduleProviderWake(); return; }
    // A successful automatic retry can recover within the same run, without agent_start.
    if (this.#providerReleaseUntil !== undefined) { this.#release(true); return; }
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
    // Each sender-homogeneous prefix is queued behind any steer already in Pi's queue.
    // Keep the whole eligible FIFO batch at this boundary, even when capable hosts split it.
    while (due > 0) {
      const before = this.#held.length;
      if (!this.#handOver(due, "steer", true, true)) return;
      const removed = before - this.#held.length;
      if (removed <= 0) return;
      due -= removed;
    }
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
        diagnostics.warn(
          "src/main-agent.ts",
          `[pi-fabric] dropped undeliverable followUp ${item.id} from ${String(item.from?.id)}: ` +
            (error instanceof Error ? error.message : String(error)),
        );
      }
    }
    count = Math.min(count, this.#held.length);
    if (count <= 0) { this.#trySave(); return true; }
    // A turn has one sender. Keep legacy batching unchanged, and hand over only a homogeneous
    // FIFO prefix on capable hosts. Each successful prefix is journalled before the next send.
    if (this.supportsProvenance()) {
      const key = JSON.stringify(this.#provenance(this.#held[0]!));
      const different = this.#held.slice(0, count).findIndex(item => JSON.stringify(this.#provenance(item)) !== key);
      if (different > 0) count = different;
    }
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
      if (this.#contextRetired()) return;
      // Drain-off uses #closed to stop holding, not to end a manual operation. Close
      // itself cancels this timer; only the real idle boundary retires its signal.
      if (!this.#context || !operation || operation.aborted) {
        if (this.#operation === operation) this.#stopOperation();
        this.#stopWake();
      } else if (this.#context?.isIdle()) {
        if (this.#operation === operation) this.#stopOperation();
        this.#held.length ? this.#release(true) : this.#stopWake();
        this.#armProviderDeadTimer();                      // the operation's idle edge; no-op unless pending
      }
    }, 25);
    this.#wake.unref?.();
  }

  #wakeAfterPreflight(): void {
    // Handled input and failed validation clear isPromptPending without any run/settle
    // event. Observe that completion only after admission is journalled. If a run starts,
    // leave FIFO delivery to its boundaries; halt/reload/close cancel this waiter too.
    if (this.#preflightWake) return;
    this.#preflightWake = setInterval(() => {
      if (this.#contextRetired()) return;
      const ctx = this.#context;
      // A no-run preflight may also leave an async compaction handler finishing. Busy
      // alone is not proof of agent_start; that event explicitly cancels this waiter.
      if (ctx && (promptPending(ctx) || !ctx.isIdle())) return;
      if (ctx) this.#release(true); // #send retains owner/provider vetoes.
      else this.#stopWake();
      this.#armProviderDeadTimer();                        // the preflight's idle edge; no-op unless pending
    }, 25);
    this.#preflightWake.unref?.();
  }

  /**
   * smarty-dev#8881: AgentSession.dispose() and session replacement invalidate the bound ctx without
   * session_shutdown, and every getter then throws. A timer that finds it stale suspends the drain as
   * a native session switch does (prepareReload: admission only journals) and stops every loop; a
   * later live attachFollowUpDrain resumes from the journal.
   */
  #contextRetired(): boolean {
    const ctx = this.#context;
    if (!ctx || extensionContextLive(ctx)) return false;
    this.prepareReload();
    this.#stopOperation();
    this.#stopProviderDeadTimer();
    return true;
  }

  #stopOperation(): void {
    // Retiring a compaction signal must not cancel a scheduled provider retry.
    if (this.#wake) clearInterval(this.#wake);
    this.#wake = undefined;
    this.#offOperationAbort?.();
    this.#offOperationAbort = undefined;
    this.#operation = undefined;
  }

  #providerBackoffActive(): boolean {
    return this.#providerFailed && Date.now() < this.#providerRetryAt;
  }

  #recordProviderFailure(newFailure = false): void {
    // Late failures cannot lift a terminal alarmed hold. A human compaction can still earn probation.
    if (this.#providerDead?.phase === "alarmed") return;
    // A retryable failure (even after a non-retryable incident's compaction) is backoff again.
    this.#clearProviderDead();
    this.#providerAttemptInFlight = false;
    this.#stopProviderWake();
    if (newFailure || !this.#providerFailureRecorded) {
      // Saturate the exponent too: consecutive failures can continue indefinitely.
      this.#providerFailures = Math.min(this.#providerFailures + 1, 6);
      this.#providerRetryAt = Date.now() + Math.min(60_000 * 2 ** (this.#providerFailures - 1), 30 * 60_000);
      this.#providerFailureRecorded = true;
    }
    this.#providerFailed = true;
    this.#stopWake();
    this.#scheduleProviderWake();
    if (this.#providerStateDirty) this.#trySave(true);
  }

  #recoverProvider(resetFailures = true): void {
    const changed = this.#providerSnapshot() !== undefined;
    this.#clearProviderDead();
    this.#providerAttemptInFlight = false;
    this.#providerFailed = false;
    // Only a successful turn (not owner input) resets the unclassified-failure count.
    if (resetFailures) {
      this.#providerFailures = 0;
      this.#unclassifiedFailures = 0;
      this.#unclassifiedSince = undefined;
      this.#unclassifiedAlarmed = false;
      this.#runUnclassified = false;
    }
    this.#providerRetryAt = 0;
    this.#providerFailureRecorded = false;
    this.#stopProviderWake();
    if (changed) { this.#providerStateDirty = true; this.#trySave(true); }
  }

  /** Rebind the live controller without treating reload or owner input as provider success. */
  #resumeProviderAfterAttach(): void {
    if (this.#providerDead) {
      // A stale compaction callback must not advance the successor binding.
      this.#providerDead = { ...this.#providerDead };
      if (this.#providerDead.phase === "compacting") {
        // The old binding owns that compaction. Never grant another one on reload.
        this.#raiseProviderDead("compaction-failed");
      }
    }
    this.#providerAttemptInFlight = this.#providerDead?.attemptStarted === true;
    this.#providerFailed = this.#providerDead !== undefined;
    this.#providerRetryAt = 0;
    this.#providerFailureRecorded = this.#providerDead !== undefined;
    this.#providerReleaseUntil = undefined;
    this.#stopProviderWake();
    const dead = this.#providerDead;
    if (dead && !this.#halted && !this.#context?.signal?.aborted) {
      // Attach releases replay synchronously: an expired pending step must hold before that release.
      // An already-admitted attempt owns its outcome, not the pending recovery deadline.
      if ((dead.phase === "recovering" || dead.phase === "probation") && !this.#providerAttemptInFlight &&
        dead.deadlineAt !== undefined && dead.deadlineAt <= Date.now()) {
        this.#raiseProviderDead("recovery-timeout");
      } else this.#armProviderDeadTimer();
    }
    if (this.#providerStateDirty) this.#trySave(true);
  }

  #scheduleProviderWake(): void {
    // #closed also means the ordinary busy drain is disabled. Backoff holds work in both modes.
    // A non-retryable incident never schedules a timed retry (smarty-dev#8742).
    if (this.#providerDead || this.#providerWake || this.#providerAttemptInFlight || !this.#context || this.#reloading || this.#halted ||
      this.#context.signal?.aborted || !this.#held.some(item =>
        item.deliverAs !== "nextTurn" && (item.triggerTurn ?? true))) return;
    const until = this.#providerRetryAt;
    this.#providerReleaseUntil = until;
    this.#providerWake = setTimeout(() => {
      this.#providerWake = undefined;
      if (this.#contextRetired()) return;
      if (!this.#context || this.#reloading || this.#halted || this.#context.signal?.aborted) return;
      this.#release(true);
    }, Math.max(0, until - Date.now()));
    this.#providerWake.unref?.();
  }

  #stopProviderWake(): void {
    if (this.#providerWake) clearTimeout(this.#providerWake);
    this.#providerWake = undefined;

  }

  /** Backoff, or a non-retryable incident that is not in its one post-compaction attempt. */
  #providerHeld(): boolean {
    const phase = this.#providerDead?.phase;
    return this.#providerBackoffActive() || (phase !== undefined && phase !== "probation");
  }

  #providerHoldReason(): string {
    const dead = this.#providerDead;
    if (!dead) {
      return this.#providerAttemptInFlight ? "provider-retry in flight" : `provider-backoff until ${new Date(this.#providerRetryAt).toISOString()}`;
    }
    const alarmed = dead.kind === "unclassified" ? `; ${dead.failures ?? UNCLASSIFIED_PROVIDER_DEAD_AFTER} failures in a row, ops alarmed, held until owner input`
      : "; ops alarmed, held until owner input";
    const state = dead.phase === "alarmed" ? alarmed
      : dead.phase === "probation" ? "; retry after compaction in flight" : "; compacting once to recover";
    // Control acknowledgements carry a reason of at most 200 characters; a longer one is dropped,
    // which would turn this receipt back into a silent triggered:false. The label is built from
    // allowlisted tokens, so this bound is defence in depth.
    const label = providerDeadLabel(dead);
    const room = 200 - state.length;
    return `${label.length > room ? `${label.slice(0, room - 1)}…` : label}${state}`;
  }

  #notify(message: string, level: "info" | "warning", context = this.#context, onceKey?: string): void {
    if (level === "warning") diagnostics.warn("src/main-agent.ts", `[pi-fabric] ${message}`);
    try {
      if (!context?.hasUI) return;
      if (this.local && onceKey) notifyPrincipalDiagnosticOnce(this.pi, context, onceKey, message, level);
      else notifyDiagnostic(context, "src/main-agent.ts", message, level);
    } catch { /* diagnostics only */ }
  }

  /**
   * smarty-dev#8742: the provider rejected the request itself (an HTTP 4xx, or the gateway's
   * compaction_affinity_missing). Retrying the same context fails the same way, so this never
   * enters backoff: compact once at idle, allow one attempt, then alarm.
   */
  #recordNonRetryable(failure: ProviderErrorClass, context = this.#context): void {
    this.#providerAttemptInFlight = false;
    this.#stopProviderWake();
    const dead = this.#providerDead;
    const fields = {
      error: failure.short, ...(failure.status === undefined ? {} : { status: failure.status }),
      ...(failure.type === undefined ? {} : { type: failure.type }), ...(failure.code === undefined ? {} : { code: failure.code }),
    };
    if (dead?.phase === "probation") {
      // The incident becomes this classified rejection, whatever began it; still one alarm.
      delete dead.status; delete dead.type; delete dead.code; delete dead.failures;
      Object.assign(dead, { kind: "non-retryable" as const }, fields);
      this.#providerStateDirty = true;
      this.#raiseProviderDead("failed-after-compaction");
      return;
    }
    if (dead) return;                                    // one incident, one recovery
    const error = failure.short;
    this.#providerDead = { kind: "non-retryable", ...fields, since: Date.now(), phase: "recovering", alarmed: false };
    // Hold peer wakes without a retry deadline: #providerHeld, never #providerBackoffActive.
    this.#providerFailed = true;
    this.#providerRetryAt = 0;
    this.#providerFailureRecorded = true;
    this.#providerReleaseUntil = undefined;
    this.#stopWake();
    this.#providerStateDirty = true;
    this.#notify(`Provider rejected Main's request as non-retryable (${error}); compacting once to recover`, "warning", context, "provider-non-retryable");
    this.#armProviderDeadTimer();
  }

  /**
   * At idle: run the one recovery compaction, or the one post-compaction attempt.
   *
   * Event-driven, never polled: a zero-delay tick leaves the emitting hook, while one non-rearming
   * five-minute last-resort alarm covers a pending recovery step. If Main is busy at the tick, the
   * next lifecycle edge retries it: agent_end, settle, compaction completion, session_tree, idle or
   * preflight waiters, or a newly held peer message. agent_start stops the tick; an admitted probation
   * attempt is already in flight and needs neither timer; owner input ends the incident.
   */
  #armProviderDeadTimer(): void {
    const phase = this.#providerDead?.phase;
    if (phase !== "recovering" && phase !== "probation") return;
    if (phase === "probation" && this.#providerAttemptInFlight) return;
    if (!this.#providerDeadDeadline) {
      if (this.#providerDead!.deadlineAt === undefined) {
        this.#providerDead!.deadlineAt = Date.now() + MAIN_PROVIDER_RECOVERY_TIMEOUT_MS;
        this.#providerStateDirty = true;
        this.#trySave(true);
      }
      this.#providerDeadDeadline = setTimeout(() => {
        this.#providerDeadDeadline = undefined;
        if (this.#contextRetired()) return;
        const dead = this.#providerDead;
        const ctx = this.#context;
        try {
          if (!dead || (dead.phase !== "recovering" && dead.phase !== "probation") || !ctx ||
            !this.#bindingsLive || this.#reloading || this.#halted || ctx.signal?.aborted) return;
        } catch {
          this.#contextRetired();
          return;
        }
        this.#raiseProviderDead("recovery-timeout");
      }, Math.max(0, this.#providerDead!.deadlineAt! - Date.now()));
      this.#providerDeadDeadline.unref?.();
    }
    if (this.#providerDeadTimer) return;
    this.#providerDeadTimer = setTimeout(() => {
      this.#providerDeadTimer = undefined;
      if (this.#contextRetired()) return;
      const dead = this.#providerDead;
      const ctx = this.#context;
      try {
        if (!dead || !ctx || !this.#bindingsLive || this.#reloading || this.#halted || ctx.signal?.aborted) return;
        // Busy: no re-arm here. The edge that ends this busy state calls back in.
        if (!ctx.isIdle() || promptPending(ctx) || (ctx as { isSettling?: () => boolean }).isSettling?.() === true) return;
      } catch {
        this.#contextRetired();
        return;
      }
      if (dead.phase === "recovering") this.#compactForRecovery(dead, ctx);
      else if (dead.phase === "probation") this.#attemptAfterCompaction();
    }, 0);
    this.#providerDeadTimer.unref?.();
  }

  #stopProviderDeadTimer(): void {
    if (this.#providerDeadTimer) clearTimeout(this.#providerDeadTimer);
    this.#providerDeadTimer = undefined;
  }

  #stopProviderDeadDeadline(): void {
    if (this.#providerDeadDeadline) clearTimeout(this.#providerDeadDeadline);
    this.#providerDeadDeadline = undefined;
  }

  #clearProviderDead(): void {
    if (this.#providerDead) this.#providerStateDirty = true;
    this.#providerDead = undefined;
    this.#stopProviderDeadTimer();
    this.#stopProviderDeadDeadline();
  }

  /** The same path a human /compact takes; compaction extensions may drop provider-bound blocks. */
  #compactForRecovery(dead: ProviderDeadState, ctx: ExtensionContext): void {
    this.#stopProviderDeadDeadline();
    dead.phase = "compacting";
    this.#providerStateDirty = true;
    this.#trySave(true);
    // smarty-dev#8881: Pi may deliver these callbacks after dispose() or session replacement. Only the
    // incident they were started for, through the still-live ctx they were started on, may advance it;
    // a stale ctx retires only a stale current binding (#contextRetired), never a live successor.
    const fresh = (): boolean => {
      if (!extensionContextLive(ctx)) { this.#contextRetired(); return false; }
      return this.#providerDead === dead && this.#bindingsLive && !this.#reloading;
    };
    const failed = (): void => {
      if (!fresh() || dead.phase !== "compacting") return;
      this.#raiseProviderDead("compaction-failed");
    };
    try {
      // Pi calls onComplete after its compaction state is cleared: the idle edge for the one attempt.
      ctx.compact({ onComplete: () => { if (fresh()) { this.#providerCompacted(); this.#armProviderDeadTimer(); } }, onError: failed });
    } catch {
      failed();
    }
  }

  /** A completed compaction ends the incident's compaction step; the work goes once. */
  #providerCompacted(): void {
    const dead = this.#providerDead;
    if (!dead || dead.phase === "probation") return;
    // A compaction after the alarm (a human /compact) earns one more attempt; no second alarm.
    dead.phase = "probation";
    delete dead.attemptStarted;
    dead.deadlineAt = Date.now() + MAIN_PROVIDER_RECOVERY_TIMEOUT_MS;
    this.#providerStateDirty = true;
    this.#trySave(true);
    this.#providerFailed = true;                         // #releaseQueue makes it ONE attempt
    this.#providerRetryAt = 0;
    this.#stopProviderDeadTimer();
    this.#armProviderDeadTimer();
  }

  #attemptAfterCompaction(): void {
    if (this.#providerAttemptInFlight) return;
    if (this.#held.some(item => item.deliverAs !== "nextTurn" && (item.triggerTurn ?? true))) {
      this.#release(true);                               // held work goes first, as one attempt
      if (this.#providerAttemptInFlight) this.#stopProviderDeadDeadline();
      return;
    }
    // Nothing held: the failed turn itself is the work. Continue it once, as #4012's retry does.
    this.#providerAttemptInFlight = true;
    if (this.#providerDead) this.#providerDead.attemptStarted = true;
    this.#providerStateDirty = true;
    this.#trySave(true);
    try {
      sendFabricMessage(this.pi, {
        customType: "pi-fabric-provider-retry", display: true,
        content: "Continue the interrupted turn: the provider rejected the previous request as non-retryable, and Fabric compacted the session once to recover. This is Fabric's one automatic retry.",
      }, { deliverAs: "followUp", triggerTurn: true });
      this.#stopProviderDeadDeadline();
    } catch {
      this.#providerAttemptInFlight = false;
      this.#raiseProviderDead("failed-after-compaction");
    }
  }

  /**
   * smarty-dev#8742: a failed run whose error carried no status, body or known token. A network blip
   * stays on backoff; the UNCLASSIFIED_PROVIDER_DEAD_AFTER-th such settle in a row, with no
   * successful turn between, raises the provider-dead alarm instead of more silent backoff.
   */
  #countUnclassified(): boolean {
    if (!this.#runUnclassified) return false;
    this.#runUnclassified = false;
    const now = Date.now();
    if (this.#unclassifiedSince === undefined || now - this.#unclassifiedSince > UNCLASSIFIED_PROVIDER_FAILURE_WINDOW_MS) {
      this.#unclassifiedFailures = 0;
      this.#unclassifiedSince = now;
    }
    this.#unclassifiedFailures = Math.min(this.#unclassifiedFailures + 1, MAX_PERSISTED_UNCLASSIFIED_FAILURES);
    this.#providerStateDirty = true;
    this.#trySave(true);
    if (this.#unclassifiedFailures < UNCLASSIFIED_PROVIDER_DEAD_AFTER) return false;
    this.#providerAttemptInFlight = false;
    this.#stopProviderWake();
    this.#providerDead = { kind: "unclassified", error: "unclassified", failures: this.#unclassifiedFailures,
      since: this.#unclassifiedSince, phase: "alarmed", alarmed: this.#unclassifiedAlarmed };
    this.#providerFailed = true;
    this.#providerRetryAt = 0;
    this.#providerFailureRecorded = true;
    this.#providerReleaseUntil = undefined;
    this.#stopWake();
    this.#raiseProviderDead("unclassified-repeated");
    return true;
  }

  /** Terminal for the incident: hold work, no timers, one loud alarm. */
  #raiseProviderDead(cause: FabricMainProviderDeadEvent["cause"]): void {
    const dead = this.#providerDead;
    if (!dead) return;
    this.#stopProviderDeadTimer();
    this.#stopProviderDeadDeadline();
    dead.phase = "alarmed";
    this.#providerAttemptInFlight = false;
    this.#providerFailed = true;
    this.#providerRetryAt = 0;
    this.#stopProviderWake();
    this.#providerStateDirty = true;
    if (dead.alarmed) { this.#trySave(true); return; }
    dead.alarmed = true;
    if (dead.kind === "unclassified") this.#unclassifiedAlarmed = true;
    this.#trySave(true);
    const why = cause === "compaction-failed" ? "the recovery compaction failed"
      : cause === "unclassified-repeated" ? `${dead.failures ?? UNCLASSIFIED_PROVIDER_DEAD_AFTER} provider failures in a row without an HTTP status or error body`
      : cause === "recovery-timeout" ? "the recovery step did not become runnable before its deadline"
      : "the one retry after compaction failed again";
    // Fixed text and allowlisted tokens only; the provider's own error text stays in the session log.
    const message = `${providerDeadLabel(dead)}: ${why}. The provider's error text is in the session log. Held peer messages stay journalled and ops.owner is alarmed; fix the provider, then send any input to resume.`;
    this.#notify(message, "warning", this.#context, "provider-dead-terminal");
    try {
      this.onProviderDead?.({
        kind: dead.kind, error: dead.error, ...(dead.status === undefined ? {} : { status: dead.status }),
        ...(dead.type === undefined ? {} : { type: dead.type }), ...(dead.code === undefined ? {} : { code: dead.code }),
        ...(dead.failures === undefined ? {} : { failures: dead.failures }),
        since: new Date(dead.since).toISOString(), cause,
        held: this.#held.map(item => ({ id: item.id, from: item.from.id, sentAt: item.sentAt })),
      });
    } catch (error) {
      diagnostics.warn("src/main-agent.ts", "[pi-fabric] provider-dead alarm failed", error);
    }
  }

  /** Test/diagnostic view of the non-retryable incident (smarty-dev#8742). */
  get providerDeadState(): Readonly<ProviderDeadState> | undefined {
    return this.#providerDead === undefined ? undefined : { ...this.#providerDead };
  }

  #stopWake(): void {
    if (this.#wake) clearInterval(this.#wake);
    if (this.#preflightWake) clearInterval(this.#preflightWake);
    this.#wake = undefined;
    this.#preflightWake = undefined;
    this.#stopProviderWake();

  }

  #release(triggerTurn: boolean, closing = false): void {
    if (this.#reloading) return;
    if (!closing && this.#context && promptPending(this.#context)) {
      this.#wakeAfterPreflight();
      return;
    }
    if (!closing && this.#providerAttemptInFlight && !this.#halted && !this.#context?.signal?.aborted) return;
    // An error settle must not downgrade a held wake into passive Pi context forever.
    if (!closing && this.#providerFailed && !this.#halted && !this.#context?.signal?.aborted &&
      (this.#providerHeld() || !triggerTurn)) {
      // A failed boundary must still reconcile reload replay as passive context. Holding
      // it until recovery would revive its saved wake permission at an unrelated settle.
      // Fresh peer wakes are different: leave them journalled for the provider retry.
      while (this.#held[0] && this.#replayed.has(this.#held[0].id)) {
        const first = this.#held[0];
        first.deliverAs ??= "followUp";
        first.triggerTurn = false;
        if (!this.#handOver(1, first.deliverAs, false, false)) break;
      }
      this.#stopProviderWake();
      if (!this.#held.length) this.#providerReleaseUntil = undefined;
      this.#scheduleProviderWake();
      return;
    }
    this.#stopWake();
    const until = !closing && triggerTurn && !this.#halted && !this.#context?.signal?.aborted
      ? this.#providerReleaseUntil : undefined;
    const before = until === undefined ? [] : this.#held.map(item => item.id);
    this.#releaseQueue(triggerTurn, !closing && this.#providerFailed && triggerTurn && !this.#halted && !this.#context?.signal?.aborted);
    if (until !== undefined) {
      const remaining = new Set(this.#held.map(item => item.id));
      const messageIds = before.filter(id => !remaining.has(id));
      if (!this.#held.length) this.#providerReleaseUntil = undefined;
      if (messageIds.length) this.onProviderWakeReleased?.({ until: new Date(until).toISOString(), messageIds });
    }
  }

  #releaseQueue(triggerTurn: boolean, singleWake = false): void {
    // In byte/provenance-bounded messages, oldest first; a failed send keeps the rest.
    while (this.#held.length) {
      const first = this.#held[0]!;
      const deliverAs = first.deliverAs ?? "followUp";
      const wake = triggerTurn && (first.triggerTurn ?? true);
      let count = 1;
      if (first.deliverAs === undefined) {
        count = 0;
        let bytes = 0;
        while (count < this.#held.length && this.#held[count]!.deliverAs === undefined) {
          bytes += itemBytes(this.#held[count]!);
          if (count > 0 && bytes > FOLLOW_UP_LIMITS.batchBytes) break;
          count++;
        }
      }
      // Pi immediately consumes queued followUps even after an error. Keep every later
      // batch in Fabric until this attempt succeeds or records its next failure deadline.
      const attempt = singleWake && wake && deliverAs !== "nextTurn";
      if (attempt) this.#providerAttemptInFlight = true;
      if (!this.#handOver(count, deliverAs, wake, false)) {
        if (attempt) this.#providerAttemptInFlight = false;
        return;
      }
      if (attempt) return;
    }
  }

  #provenance(item: HeldAgentMessage): FabricTurnProvenance | undefined {
    // A replay not held by Pi is its first receipt, not a fresh sender admission.
    const admitted = item.provenance;
    return admitted ? { ...admitted, sender: { ...admitted.sender }, via: this.#replayed.has(item.id) ? "replay" : admitted.via } : undefined;
  }

  #send(
    items: HeldAgentMessage[],
    deliverAs: FabricMainAgentDelivery,
    triggerTurn: boolean,
    flushed: boolean,
    peerSteer = false,
  ): boolean | undefined {
    items = items.filter(item => !this.#inboxFence || this.#inboxFence.owns(item.id));
    if (this.#reloading || !items.length || (this.#inboxFence && !this.#inboxFence.active())) return false;
    const interrupt = this.#context?.isIdle() === true && items.every(item => this.#interrupts.has(item.id));
    triggerTurn &&= (peerSteer || !this.#halted) && !this.#providerHeld() && (!this.#context?.signal?.aborted || interrupt);
    // Persist a downgraded explicit replay policy, including handoffs retried after a later reload.
    if (!triggerTurn) for (const item of items) if (item.deliverAs !== undefined) item.triggerTurn = false;
    // Each item keeps its own delivery mark: a flushed batch goes in as a steer, but it holds followUps.
    const delivery: FabricMainAgentDelivery = flushed ? "followUp" : deliverAs;
    const blocks = items.map((item) => agentMessageBlock(item, delivery));
    const itemDetails = (item: HeldAgentMessage) => {
      // Use the same native shape admission charged, including synthesized work claims.
      const data = nativeItemData(item);
      return {
        id: item.id,
        from: item.from,
        delivery,
        sentAt: new Date(item.sentAt).toISOString(),
        ...(item.replacedAt === undefined ? {} : { replacedAt: new Date(item.replacedAt).toISOString() }),
        ...(item.chain === undefined ? {} : { chain: item.chain, generation: item.generation, replaces: item.replaces }),
        ...(data === undefined ? {} : { data }),
        ...(item.deliveryId === undefined ? {} : { deliveryId: item.deliveryId }),
        ...(item.inboxEventId === undefined ? {} : { inboxEventId: item.inboxEventId }),
        ...(item.inboxTopic === undefined ? {} : { inboxTopic: item.inboxTopic }),
        ...(item.inboxKey === undefined ? {} : { inboxKey: item.inboxKey }),
        ...(item.supersedes?.length ? { supersedes: item.supersedes } : {}),
      };
    };
    const first = items[0]!;
    const provenance = this.#provenance(first);
    const options = { deliverAs, triggerTurn };
    const triggered = !triggerTurn || deliverAs === "nextTurn" ? false : this.#context?.isIdle();
    this.pi.sendMessage(
      fabricWakeMessage(this.pi, {
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
          ...(items.some((item) => item.inboxReceipts?.length)
            ? { receipts: items.flatMap((item) => item.inboxReceipts ?? []) } : {}),
          triggerTurn,
          ...(flushed ? { flushed: true } : {}),
        },
      }, options, items.flatMap<FabricWakeCause | { cause: "unattributed" }>(item => this.#replayed.has(item.id) &&
        (!item.provenance || item.unattributedOnReplay === true)
        ? [{ cause: "unattributed" as const }]
        : item.wakeCauses ?? [item.wakeCause ?? mainWakeCause(item.from, item.source, delivery,
          item.admissionTopic, item.admissionKey ?? item.deliveryId)])),
      provenance ? fabricProvenanceOptions(this.pi, options, provenance) : options,
    );
    // A fresh delivery can win the deadline without going through #releaseQueue. It is
    // still the one retry attempt; later peer wakes must wait for its actual outcome.
    if (triggerTurn && deliverAs !== "nextTurn" && this.#providerFailed) {
      this.#providerAttemptInFlight = true;
      if (this.#providerDead) {
        this.#providerDead.attemptStarted = true;
        this.#stopProviderDeadDeadline();
        this.#providerStateDirty = true;
        this.#trySave(true);
      }
    }
    return triggered;
  }
}
