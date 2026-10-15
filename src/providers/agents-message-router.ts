import { appendDeliveryOutcome } from "../mesh/delivery-outcomes.js";
import { interruptErrorCode } from "../interrupt-authority.js";
import { boundAgentSpawner } from "../agents/spawner.js";
import { FabricTargetTerminalError, terminalRejectionFields } from "../agents/terminal-target.js";
import { terminalAgentStatuses } from "../agents/lifecycle.js";
import { invocationFabricPrincipal, snapshotFabricInvocation, fabricTurnProvenance, copyFabricWakeCause, copyFabricWakeAdmission, type FabricWakeCause, type FabricPrincipal } from "../fabric-provenance.js";
import type { AgentManager } from "../agents/manager.js";
import { DEFAULT_FOLLOW_UP_DEADLINE_MS } from "../agents/follow-up-delivery.js";
import type { ActorManager } from "../actors/manager.js";
import type { FabricActorInfo, FabricActorRunBinding } from "../actors/types.js";
import { assertSteerPriority, type FabricSteerPriority, type FabricAgentMessageResult, type FabricMainAgentTarget } from "../main-agent.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { FabricInvocationContext } from "../protocol.js";
import { controlOwnerIncarnation, warnUnfencedControlDelivery, controlActorBindingOptions, FabricControlStaleIncarnationError, type FabricControlPlane, type FabricControlCommand, type FabricControlAcceptance } from "../topology/control-plane.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../topology/types.js";
import type { FabricAgentRunner } from "../config.js";
import type { ResidencyClient } from "../residency/client.js";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { FabricParticipantStaleError, type MirrorLeaseWaitBudget } from "../topology/host-leases.js";
import { FabricPeerLeaseStaleError } from "../agents/peer-lease-stale.js";
import path from "node:path";
import { kernelFenceAvailable } from "../residency/file-lock.js";
import { processAlive } from "../storage/scratch.js";
import { assertTaskMainTarget, readTaskReturnAddress } from "../agents/task-return-address.js";

// MeshStore's default stale window; recovery waits, never weakens mesh locking.
export const RESIDENT_MESH_STALE_WINDOW_MS = 30_000;

export class FabricParticipantNotYetMirroredError extends Error {
  override readonly name = "FabricParticipantNotYetMirroredError";
  readonly code = "FABRIC_PARTICIPANT_NOT_YET_MIRRORED";
  readonly retryable = true;
  constructor(id: string, host?: string) {
    super(`Fabric participant ${id} is listed by peers but not yet mirrored for control${host ? ` from ${host}` : ""}. Retry after the next mesh bridge presence refresh.`);
  }
}

export class FabricDirectoryUnavailableError extends Error {
  override readonly name = "FabricDirectoryUnavailableError";
  readonly code = "FABRIC_DIRECTORY_UNAVAILABLE";
  readonly retryable = true;
  constructor(reason: string, cause?: unknown) {
    super(`Fabric directory unavailable (retry): ${reason}`, { cause });
  }
}

export class FabricParticipantNonInteractiveError extends Error {
  override readonly name = "FabricParticipantNonInteractiveError";
  readonly code = "FABRIC_PARTICIPANT_NON_INTERACTIVE";
  constructor(id: string) {
    super(`Fabric participant ${id} is non-interactive (print/JSON); it cannot receive followUp or steer messages.`);
  }
}

// A quiesced root keeps heartbeating with no capabilities while it shuts down; "does not support"
// read as a broken session (smarty-dev#1113).
const unsupported = (participant: { id: string; status?: string; interactive?: boolean }, kind: string): Error =>
  participant.interactive === false
    ? new FabricParticipantNonInteractiveError(participant.id)
    : participant.status === "stopping"
    ? new Error(`Fabric participant ${participant.id} is shutting down; its session will relaunch or end. Retry after it restarts.`)
    : new Error(`Fabric participant ${participant.id} does not support ${kind}`);

// A Pi session id (8-4-4-4-12). Actor and agent ids are 32 hex with no dashes, so they never match.
const SESSION_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// Route messages using only the ownership, delivery, and binding ports needed here.
// Unknown ids retain the old prefix; discovery-only targets get a named retryable error.
export const unknownParticipant = (
  participants: Pick<FabricParticipantSource, "lastKnown"> & Partial<Pick<FabricParticipantSource, "peers">>,
  id: string,
  label = "Fabric participant",
): Error => {
  const known = participants.lastKnown?.(id);
  // fv2-11703: an existing remote mirror whose validated owner lease lapsed is not discovery-only;
  // it keeps the "Unknown " lapse classification so lease resolution can wait for its renewal.
  const lapsedMirror = known?.participant.remoteHost !== undefined && Number.isFinite(known.lapsedMs);
  const peer = lapsedMirror ? undefined : participants.peers?.().find((candidate) => candidate.id === id);
  if (peer) return new FabricParticipantNotYetMirroredError(id, peer.host);
  if (!known) {
    const hint = SESSION_UUID.test(id.trim()) ? `; use 'session:${id.trim()}' for a Main session` : "";
    return new Error(
      `Unknown ${label}: ${id} (no record on this mesh root: the session has ended, ` +
        `has not joined yet, or uses another mesh root${hint})`,
    );
  }
  const remote = known.participant.remoteHost;
  if (remote) {
    const when = Number.isFinite(known.lapsedMs) ? ` ${Math.round(known.lapsedMs / 1000)} s ago` : "";
    return new Error(
      `Unknown ${label}: ${id} (its lease mirrored from remote host ${remote} lapsed${when}: ` +
        "the mesh bridge to that host is down, or the session has ended)",
    );
  }
  const when = Number.isFinite(known.lapsedMs)
    ? `its lease lapsed ${Math.round(known.lapsedMs / 1000)} s ago`
    : "its host is gone or was replaced";
  return new Error(`Unknown ${label}: ${id} (${when}, so the session has probably ended)`);
};

/** The selected root authority disappeared or changed before delivery; nothing was published. */
export class FabricRouteAuthorityError extends Error {
  readonly code = "FABRIC_ROUTE_AUTHORITY_CHANGED";
  constructor(id: string) {
    super(`Fabric native routing is unavailable for ${id}; the routed owner could not be revalidated; this attempt was not published. Retry after its native presence returns.`);
    this.name = "FabricRouteAuthorityError";
  }
}

/** Known pre-publication routing refusals: nothing was published, so the attempt failed. */
const PRE_PUBLICATION_REFUSALS = new Set([
  "FABRIC_ROUTE_AUTHORITY_CHANGED", "FABRIC_CONTROL_INCARNATION_REQUIRED",
  "FABRIC_PARTICIPANT_NOT_YET_MIRRORED", "FABRIC_PARTICIPANT_NON_INTERACTIVE",
  "FABRIC_PEER_LEASE_STALE",
]);
const prePublicationRefusal = (error: unknown, reason: string): string | undefined => {
  const code = error instanceof Error && "code" in error && typeof error.code === "string" ? error.code : undefined;
  if (code !== undefined && PRE_PUBLICATION_REFUSALS.has(code)) return `${code}: ${reason}`;
  return reason.includes("this attempt was not published") ? reason : undefined;
};

/** Distinguishes a pre-publication lease wait from an uncertain published ACK. */
class LeaseResolutionStale extends FabricParticipantStaleError {}

/** Only lookup can request lease recovery; owner ACKs and handlers must never replay. */
class LeaseResolutionRequired extends Error {
  /** Immutable pre-wait remote root authority; the retry may only refresh its epoch, never replace it. */
  constructor(readonly targetId: string, readonly original: Error, readonly authority?: FabricParticipantInfo) { super(original.message, { cause: original }); }
}

export class AgentMessageRouter {
  readonly #taskReturnAddress = readTaskReturnAddress();
  #legacyDeliveryCount = 0;
  /** Retained dormant wake routes this router resolved; only they may take the one wake resend. */
  readonly #dormantRoutes = new WeakSet<FabricParticipantInfo>();
  constructor(
    readonly manager: Pick<AgentManager, "status" | "steer" | "followUp" | "stop">,
    readonly actorManager: Pick<ActorManager, "identity" | "status" | "validateDirectMessage" | "tell" | "ask" | "stop" | "steerRemote" | "resolveBinding" | "resolveActivationBinding"> & { owns?: (id: string) => boolean; mesh?: { root: string } },
    readonly mainAgent: Pick<FabricMainAgentTarget, "matches" | "local" | "id" | "deliverAgent" | "interactive">,
    readonly participants: Pick<FabricParticipantSource, "get" | "scheduleRefresh" | "writeStalled" | "lastKnown"> & Partial<Pick<FabricParticipantSource, "peers" | "list" | "lineageAlive" | "routingUnavailable" | "refreshRoutingView" | "retainedRouteAllowed">> & {
      resolveRoutingLease?: (id: string, signal?: AbortSignal, budget?: MirrorLeaseWaitBudget) => Promise<boolean>;
    },
    readonly control: Pick<FabricControlPlane, "request"> | undefined,
    readonly resolvePiRunBinding: (binding: FabricActorRunBinding, runner: FabricAgentRunner, context: FabricInvocationContext, requiredPin?: boolean) => FabricActorRunBinding | Promise<FabricActorRunBinding>,
    readonly residency?: Pick<ResidencyClient, "ensureActor" | "hostId"> & { options: { config: { rootId: string; meshRoot: string } } },
    readonly spawner = boundAgentSpawner(),
  ) {}
  #get(id: string, fresh = false): FabricParticipantInfo | undefined {
    // Discovery can use its cached view, but sender admission must refresh positive records
    // too: a cached epoch cannot authorize control after a target reloads or downgrades.
    const participant = this.#directoryRead(() => fresh
      ? this.participants.get(id, undefined, { fresh: true })
      : this.participants.get(id) ?? this.participants.get(id, undefined, { fresh: true }));
    const reason = !participant ? this.#directoryUnavailable() : undefined;
    if (reason) throw new FabricDirectoryUnavailableError(reason);
    return participant;
  }

  #unknownParticipant(id: string, label?: string, authority?: FabricParticipantInfo): Error {
    // One lastKnown snapshot classifies the error and pins the pre-wait remote root authority.
    const known = this.#directoryRead(() => this.participants.lastKnown?.(id));
    const peers = this.participants.peers?.bind(this.participants);
    const error = this.#directoryRead(() => unknownParticipant({ lastKnown: () => known, ...(peers ? { peers } : {}) }, id, label));
    const lapsed = known?.participant;
    const pin = authority ?? (lapsed?.kind === "root" && lapsed.remoteHost !== undefined ? lapsed : undefined);
    return this.participants.resolveRoutingLease && error.message.startsWith("Unknown ")
      ? new LeaseResolutionRequired(id, error, pin) : error;
  }

  async #resolveRoutingLease(id: string, signal?: AbortSignal, budget?: MirrorLeaseWaitBudget): Promise<boolean | undefined> {
    signal?.throwIfAborted();
    try {
      const resolved = await this.participants.resolveRoutingLease?.(id, signal, budget);
      signal?.throwIfAborted();
      return resolved;
    } catch (error) {
      signal?.throwIfAborted();
      if (error instanceof FabricParticipantStaleError) {
        throw new LeaseResolutionStale(error.targetId, error.lapsedMs, error.idempotencyKey);
      }
      throw error;
    }
  }

  #directoryRead<T>(read: () => T): T {
    try {
      const value = read();
      const reason = this.#directoryUnavailable();
      if (reason) throw new FabricDirectoryUnavailableError(reason);
      return value;
    } catch (error) {
      if (error instanceof FabricDirectoryUnavailableError) throw error;
      throw new FabricDirectoryUnavailableError(error instanceof Error ? error.message : String(error), error);
    }
  }

  #directoryUnavailable(): string | undefined {
    return this.participants.routingUnavailable
      ? this.participants.routingUnavailable()
      : this.participants.writeStalled?.()?.message;
  }

  async #withDirectory<T>(operation: (authority?: FabricParticipantInfo, budget?: MirrorLeaseWaitBudget) => Promise<T>, id?: string, recoverLease = true, signal?: AbortSignal): Promise<T> {
    signal?.throwIfAborted();
    const budget: MirrorLeaseWaitBudget = {};
    let authority: FabricParticipantInfo | undefined;
    const retry = (pin = authority): Promise<T> => operation(pin, budget).catch(error => {
      if (error instanceof LeaseResolutionRequired) throw error.original;
      throw error;
    });
    const resolveLease = async (target: string): Promise<boolean | undefined> => {
      if (!this.participants.resolveRoutingLease) return undefined;
      // Bootstrap/late-read recovery must pin before waiting, even while the view is unavailable.
      // If this raw refusal-fence read fails, skip the wait, but never skip the canonical probe.
      try {
        const known = this.participants.lastKnown?.(target)?.participant ??
          this.participants.get(target, undefined, { fresh: true });
        if (known?.kind === "root" && known.remoteHost !== undefined) authority ??= known;
      } catch {
        // No wait took place, so no pre-wait authority was selected. The canonical refresh
        // below may recover; normal routing still revalidates its cached/fresh owner fence.
        return undefined;
      }
      return this.#resolveRoutingLease(target, signal, budget);
    };
    const reason = this.#directoryUnavailable();
    if (reason) {
      // Capture pre-wait authority before the canonical probe can replace its view. Classify
      // a proven lapse only after that probe; an unconfirmed/failed directory still wins.
      const resolved = recoverLease && id ? await resolveLease(id) : undefined;
      await this.#refreshDirectory(reason);
      if (resolved === false) throw this.#unknownParticipant(id!);
    }
    try {
      return await operation(authority, budget);
    } catch (error) {
      signal?.throwIfAborted();
      // Lease recovery is pre-publication only. A failed ACK is never replayed here.
      if (recoverLease && error instanceof LeaseResolutionRequired && this.participants.resolveRoutingLease) {
        // The canonical unknown classification already captured the pre-wait authority.
        if (await this.#resolveRoutingLease(error.targetId, signal, budget)) return retry(error.authority ?? authority);
        if (error.original instanceof FabricPeerLeaseStaleError) {
          // A fresh source-lapsed sample may have arrived during an indeterminate wait.
          // Reclassify that proof as root-gone, not the pre-wait freshness diagnostic.
          const refusal = this.#unknownParticipant(error.targetId);
          throw refusal instanceof LeaseResolutionRequired ? refusal.original : refusal;
        }
        throw error.original;
      }
      if (error instanceof LeaseResolutionRequired) throw error.original;
      // A read can fail after the preflight. Retry resolution once, never delivery after
      // an ACK/control error: only pre-publication directory failures have this class.
      if (!(error instanceof FabricDirectoryUnavailableError) || reason || !this.participants.refreshRoutingView) throw error;
      const resolved = recoverLease && id ? await resolveLease(id) : undefined;
      await this.#refreshDirectory(error.message);
      if (resolved === false) throw this.#unknownParticipant(id!);
      return retry();
    }
  }

  async #refreshDirectory(reason: string): Promise<void> {
    if (!this.participants.refreshRoutingView) throw new FabricDirectoryUnavailableError(reason);
    try {
      await this.participants.refreshRoutingView();
      const unavailable = this.#directoryUnavailable();
      if (unavailable) throw new Error(unavailable);
    } catch (error) {
      throw new FabricDirectoryUnavailableError(error instanceof Error ? error.message : String(error), error);
    }
  }

  #lapsedRoot(id: string): FabricParticipantInfo | undefined {
    // A write-stalled mesh explains the lapse, and delivery needs the mesh: report the stall.
    const reason = this.#directoryUnavailable();
    if (reason) throw new FabricDirectoryUnavailableError(reason);
    // A busy Main can miss its heartbeat without losing its durable control mailbox (smarty-dev#3686).
    // Read presence without filtering leases: get() and lastKnown() can both omit a root
    // when its lease renews between those reads. Unknown lineage is not proof of death.
    const root = this.participants.list
      ? this.#directoryRead(() => this.participants.list!({ scope: "project", kinds: ["root"], includeStale: true, fresh: true }))
        .find((participant) => participant.id === id)
      : this.#directoryRead(() => this.participants.lastKnown?.(id)?.participant);
    return root && this.#eligibleRetainedRoot(root) ? root : undefined;
  }

  #eligibleRetainedRoot(root: FabricParticipantInfo): boolean {
    // Names and exact ids share the same lease-independent native eligibility. A stalled
    // writer, reload/exit, dead lineage or lapsed bridge must never gain a new route by name.
    if (this.#directoryUnavailable() || root.kind !== "root") return false;
    // Reload leases are a hard bound, not an ordinary heartbeat flap; an exit is never routable.
    if (["reloading", "stopping"].includes(root.status)) return false;
    // A mirrored lease lapses when the mesh bridge stops: nothing would carry the reply, so the
    // sender gets the lapse error at once instead of an acknowledgement timeout (smarty-dev#2004).
    if (this.participants.retainedRouteAllowed?.(root.id) === false) return false;
    return !root.remoteHost && this.#directoryRead(() => this.participants.lineageAlive?.(root.rootId)) !== false;
  }

  #rootRouteSnapshot(id: string, authority?: FabricParticipantInfo): FabricParticipantInfo | undefined {
    // A pinned pre-wait authority replaces the cached view: a post-wait recache must not re-select a replacement.
    const cached = authority?.id === id && authority.kind === "root" ? authority : this.#directoryRead(() => this.participants.get(id));
    const fresh = this.#directoryRead(() => this.participants.get(id, undefined, { fresh: true })) ?? this.#lapsedRoot(id);
    if (cached?.kind === "root") {
      // fv2-11703: a cached remote root whose mirror lapsed is a lapse, not an authority change:
      // classify it so pre-publication lease resolution waits once and re-runs this fence.
      // Only the same validated link and owner; a removed or replaced mirror stays an authority change.
      if (!fresh && cached.remoteHost !== undefined) {
        const known = this.#directoryRead(() => this.participants.lastKnown?.(id));
        const lapsed = known?.participant;
        if (known && lapsed && Number.isFinite(known.lapsedMs) && lapsed.kind === "root" && lapsed.id === cached.id &&
          lapsed.remoteHost === cached.remoteHost && lapsed.rootId === cached.rootId &&
          lapsed.ownerHostId === cached.ownerHostId && lapsed.ownerIdentityId === cached.ownerIdentityId) {
          throw this.#unknownParticipant(id, undefined, cached);
        }
      }
      // Refresh the epoch only under the same authority and link. A replacement mirror
      // must never turn a native send into bridge publication, or redirect an existing link.
      if (!fresh || fresh.kind !== "root" || fresh.remoteHost !== cached.remoteHost || fresh.id !== cached.id ||
        fresh.rootId !== cached.rootId || fresh.ownerHostId !== cached.ownerHostId ||
        fresh.ownerIdentityId !== cached.ownerIdentityId) throw new FabricRouteAuthorityError(id);
      return fresh;
    }
    return fresh;
  }

  // A bare session UUID addresses its Main `session:<uuid>` when no participant has exactly
  // that id (smarty-dev#1729). Only the same UUID is tried: never a guess across ids.
  #sessionTarget(id: string): string {
    const bare = id.trim();
    if (!SESSION_UUID.test(bare) || this.#get(bare)) return id;
    const session = `session:${bare}`;
    return this.mainAgent.matches(session) || this.#get(session) || this.#lapsedRoot(session)
      ? session
      : id;
  }

  // Published root names are selectors, never owner authority. Resolve them on the shared
  // project directory (not just this lineage), then let the existing id-based route revalidate
  // ownership and capabilities. Exact ids, UUID aliases and local `main` keep precedence.
  #messageTarget(id: string): string {
    const target = this.#sessionTarget(id);
    if (this.mainAgent.matches(target) || this.#get(target) || this.#lapsedRoot(target) || target.trim().startsWith("session:")) return target;
    // A live publication supersedes retained names from predecessor sessions (#10720).
    // Without a live match, preserve the bounded native lease-grace route and ambiguity checks.
    const named = this.participants.list
      ? this.#directoryRead(() => this.participants.list!({ scope: "project", kinds: ["root"], includeStale: true, fresh: true }))
      .filter((participant) => participant.name === target)
      : [];
    const live = named.filter((participant) => !participant.stale);
    const matches = live.length ? live : named;
    if (matches.length > 1) {
      throw new Error(`Ambiguous Fabric participant: ${id} (${matches.map((participant) => participant.id).sort().join(", ")}); use an exact id`);
    }
    const root = matches[0];
    // Retained-only names still require native owner authority; a name is not an alias
    // for an exact predecessor id and never grants delivery capability.
    if (!root || (root.stale && !this.#eligibleRetainedRoot(root))) return target;
    // Do not let a published root name shadow an existing actor name or unique id prefix.
    // Reuse the actor resolver (including its ambiguity checks), without changing its route.
    let actorId: string | undefined;
    try {
      const { actor, participant } = this.resolveActorTarget(target);
      actorId = actor?.id ?? participant?.id;
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) throw error;
    }
    if (actorId) {
      throw new Error(`Ambiguous Fabric participant: ${id} (actor ${actorId}, root ${root.id}); use an exact id`);
    }
    return root.id;
  }

  /** Exact process-owned targets need no shared-directory authority or freshness. */
  isProcessOwnedTarget(id: string): boolean {
    if (this.mainAgent.local && this.mainAgent.matches(id)) return true;
    // UUID aliases and published names still require directory selector precedence.
    if (id.trim().startsWith("session:") || SESSION_UUID.test(id.trim())) return false;
    try { return this.manager.status(id).id === id; }
    // A name/prefix ambiguity is not local proof: preserve directory selector
    // precedence, then let ordinary task resolution report its original error.
    catch { return false; }
  }

  /** Stop shares delivery's exact local proof; actors and selectors retain directory admission. */
  withStopDirectory<T>(id: string, operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    return this.isProcessOwnedTarget(id) ? operation() : this.#withDirectory(operation, id, true, signal);
  }

  /** Normalize late stop-route reads just like message-route reads. */
  resolveStopParticipant(id: string): FabricParticipantInfo | undefined {
    return this.#get(id, true);
  }

  #localMainNonInteractive(): boolean {
    if (this.mainAgent.interactive === false) return true;
    // Preserve legacy directory-only non-interactive flags when readable, but never
    // probe shared writability to deliver to the process's own Main controller.
    try {
      return (this.participants.get(this.mainAgent.id) ??
        this.participants.get(this.mainAgent.id, undefined, { fresh: true }))?.interactive === false;
    } catch { return false; }
  }

  /** Use the same target resolution as delivery when grouping lifecycle sources. */
  isLocalMainTarget(id: string): boolean {
    return this.mainAgent.local && (this.mainAgent.matches(id) || this.mainAgent.matches(this.#messageTarget(id)));
  }

  async routeMessage(
    id: string,
    message: string,
    data: unknown,
    kind: "steer" | "followUp",
    context?: FabricInvocationContext,
    options: {
      principal?: FabricPrincipal | undefined;
      from?: MeshIdentity;
      wakeCause?: FabricWakeCause | undefined;
      triggerTurn?: boolean;
      priority?: FabricSteerPriority;
      binding?: FabricActorRunBinding;
      deadlineMs?: number;
      idempotencyKey?: string;
    } = {},
  ): Promise<FabricAgentMessageResult> {
    assertSteerPriority(options.priority, kind);
    // Resolve the child's bound target before recovery so retries retain the same
    // actor ownership/fence checks rather than trying to recover the alias itself.
    if (id === "spawner") {
      if (!this.spawner) throw new Error("This worker has no bound Fabric spawner; specify an explicit reply target");
      id = this.spawner.id;
    }
    // Capture before routing yields; a queued incoming turn cannot change this send.
    if (context) context = snapshotFabricInvocation(context);
    options = { ...options, wakeCause: copyFabricWakeCause(options.wakeCause), idempotencyKey: options.idempotencyKey ?? randomUUID(), principal: context ? invocationFabricPrincipal(context) : undefined };
    // Task-local `main` remains its immutable immediate return address.
    if (id.trim() === "main" && this.#taskReturnAddress?.spawnerId) id = this.#taskReturnAddress.spawnerId;
    const provenLocal = this.isProcessOwnedTarget(id);
    let result: FabricAgentMessageResult;
    try {
      result = provenLocal
        ? await this.#route(id, message, data, kind, context, options, true)
        : await this.#withDurableRecovery(id, recovering =>
          this.#withDirectory((authority, budget) => this.#route(id, message, data, kind, context, options, false, !recovering, authority, budget), id, !recovering, context?.signal));
    } catch (error) {
      context?.signal?.throwIfAborted();
      const reason = error instanceof Error ? error.message : String(error);
      const root = this.actorManager.mesh?.root ?? this.residency?.options.config.meshRoot;
      // Only pre-publication/local definite refusals. Remote ACK outcomes belong to control.
      const refusal = prePublicationRefusal(error, reason) ??
        (/^(Unknown |Main's followUp queue is full|Fabric actor queue limit reached|Fabric participant .* is non-interactive)/.test(reason) ? reason : undefined);
      if (root && !(error instanceof Error && "rejection" in error && error.rejection === "FABRIC_CONTROL_REJECTED") && refusal !== undefined) {
        appendDeliveryOutcome(root, { eventId: options.idempotencyKey!, to: id, from: (options.from ?? this.actorManager.identity).id, mode: kind }, "failed", refusal);
      }
      if (error instanceof FabricParticipantStaleError && !error.idempotencyKey) {
        throw new FabricParticipantStaleError(error.targetId, error.lapsedMs, options.idempotencyKey);
      }
      throw error;
    }
    // smarty-dev#1826: an ack alone hid a Main whose held followUps no boundary would release.
    // Older owners never report `stalled`, so their results pass unchanged.
    if (kind === "followUp" && result?.stalled) {
      throw new Error(
        `Fabric followUp to ${id} was accepted but is not being delivered: target idle and its held queue stalled ` +
          `(yours: ${result.pendingFollowUps ?? 0} held, oldest ${result.oldestAgeS ?? 0} s). The message is still held, not withdrawn. ` +
          "Use agents.steer meanwhile (smarty-dev#1826).",
      );
    }
    return result;
  }

  /** Only this root's non-owned durable actors can wait out a dead or ownerless local lock.
   * Live/corrupt holders and unrelated routing errors retain the ordinary failure path. */
  async #withDurableRecovery<T>(id: string, operation: (recovering?: boolean) => Promise<T>): Promise<T> {
    // Ownerless recovery needs the first integer millisecond strictly after the
    // stale boundary, plus the full final mesh write-timeout budget.
    let deadline = Date.now() + RESIDENT_MESH_STALE_WINDOW_MS + 1 + 10_000;
    let recovering = false;
    for (;;) {
      try { return await operation(recovering); }
      catch (error) {
        const lockError = error instanceof FabricDirectoryUnavailableError ? error.cause : error;
        const recoveryEvidence = (lockError instanceof Error && "code" in lockError && lockError.code === "FABRIC_MESH_LOCK_TIMEOUT") ||
          error instanceof LeaseResolutionStale;
        if (!recoveryEvidence || !this.residency || !kernelFenceAvailable()) throw error;
        // Retained ownership proves only eligibility to wait for lock recovery,
        // never permission to publish. The retried operation revalidates routing.
        let actor: FabricActorInfo | undefined;
        try { actor = this.actorManager.status(id); }
        catch (lookupError) {
          if (!(lookupError instanceof Error && /Unknown Fabric actor/.test(lookupError.message))) throw error;
        }
        const participant = this.participants.get(actor?.id ?? id);
        if ((actor?.residency ?? participant?.residency) !== "durable" ||
            (actor && (this.actorManager.owns?.(actor.id) ?? participant?.local)) ||
            !(actor?.rootId === this.residency.options.config.rootId || participant?.ownerHostId === this.residency.hostId)) throw error;
        const lockPath = path.join(this.residency.options.config.meshRoot, ".lock");
        const ownerPath = path.join(lockPath, "owner");
        let owner: string | undefined;
        let createdAt = Number.NaN;
        let staleBoundaryMs = 0;
        try { owner = fs.readFileSync(ownerPath, "utf8"); }
        catch (readError) {
          if (!(readError instanceof Error && "code" in readError && readError.code === "ENOENT")) throw error;
          // SIGKILL can interrupt legacy mkdir/publication or release, leaving no PID.
          // Wait for the same directory-mtime stale window MeshStore already enforces;
          // only MeshStore may reclaim it, with its existing identity checks and fence.
          try {
            const stat = fs.lstatSync(lockPath);
            if (!stat.isDirectory() || fs.existsSync(ownerPath)) throw error;
            // Date.now/timers use integer milliseconds, but MeshStore protects
            // the full-precision mtime while age <= the stale window. Flooring is
            // safe for future-time validation only: retry at floor(mtime) + 1 so
            // neither fractional timestamps nor an exact boundary retry early.
            createdAt = Math.floor(stat.mtimeMs);
            staleBoundaryMs = 1;
          } catch { throw error; }
        }
        if (owner !== undefined) {
          const [, pidText, createdText] = owner.trim().split("\n");
          const pid = Number(pidText);
          createdAt = Number(createdText);
          if (!Number.isSafeInteger(pid) || pid <= 0 || processAlive(pid)) throw error;
        }
        if (!Number.isFinite(createdAt) || createdAt < 0 || createdAt > Date.now()) throw error;
        const retryAt = createdAt + RESIDENT_MESH_STALE_WINDOW_MS + staleBoundaryMs;
        deadline = Math.min(deadline, retryAt + 10_000);
        const now = Date.now();
        const waitMs = Math.max(100, retryAt - now);
        // Leave a full mesh write-timeout budget for the final attempt. Never extend for a new holder.
        if (now + waitMs + 10_000 > deadline) throw error;
        // Only this independently validated dead/ownerless lock permits bypassing a
        // second lease wait. Canonical lock admission and residency recovery still run.
        recovering = true;
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
    }
  }

  async #route(
    id: string,
    message: string,
    data: unknown,
    kind: "steer" | "followUp",
    context?: FabricInvocationContext,
    options: {
      principal?: FabricPrincipal | undefined;
      from?: MeshIdentity;
      wakeCause?: FabricWakeCause | undefined;
      triggerTurn?: boolean;
      priority?: FabricSteerPriority;
      binding?: FabricActorRunBinding;
      deadlineMs?: number;
      idempotencyKey?: string;
    } = {},
    provenLocal = false,
    recoverLease = true,
    authority?: FabricParticipantInfo,
    budget?: MirrorLeaseWaitBudget,
  ): Promise<FabricAgentMessageResult> {
    context?.signal?.throwIfAborted();
    const provenance = fabricTurnProvenance(options.from ?? this.actorManager.identity, kind, "mesh", options.principal);
    // In a task child, main is the immutable immediate return address, not a role lookup.
    if (id.trim() === "main" && this.#taskReturnAddress?.spawnerId) id = this.#taskReturnAddress.spawnerId;
    if (!provenLocal) id = this.#messageTarget(id);
    if (options.deadlineMs !== undefined) {
      // Do not silently lose the requested guarantee on a Main/actor/remote route.
      let task: ReturnType<typeof this.manager.status> | undefined;
      try { task = this.manager.status(id); } catch (error) {
        if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
      }
      if (!task || task.runner !== "pi") throw new Error("Delivery deadlines require a local Pi task agent");
    }
    const isMain = this.mainAgent.matches(id);
    const remoteRoot = isMain || provenLocal ? undefined : this.#rootRouteSnapshot(id, authority);
    // Project members include peer roots, not just this host's Main and actors.
    // Resolve their current owner through the same capability/control path.
    if (isMain || remoteRoot?.kind === "root") {
      assertTaskMainTarget(this.#taskReturnAddress, isMain ? this.mainAgent.id : remoteRoot!.id);
      if (remoteRoot?.interactive === false) throw new FabricParticipantNonInteractiveError(remoteRoot.id);
      if (isMain && this.mainAgent.local) {
        // Local delivery needs no remote authority snapshot, but print/JSON Main is never interactive.
        if (this.#localMainNonInteractive()) {
          throw new FabricParticipantNonInteractiveError(this.mainAgent.id);
        }
        context?.activity?.({
          type: "entity",
          id: this.mainAgent.id,
          kind: "agent",
          name: "Main",
        });
        return this.mainAgent.deliverAgent(copyFabricWakeAdmission(options, {
          from: options.from ?? this.actorManager.identity,
          verification: "mesh", // In-process registered producer, not a received command.
          principal: options.principal,
          ...(options.wakeCause ? { wakeCause: options.wakeCause } : {}),
          message,
          delivery: kind,
          outcomeSend: { eventId: options.idempotencyKey!, to: this.mainAgent.id, from: (options.from ?? this.actorManager.identity).id, mode: kind },
          ...(options.priority ? { priority: options.priority } : {}),
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
          ...(data === undefined ? {} : { data }),
        }));
      }
      let participant = remoteRoot ?? this.#rootRouteSnapshot(this.mainAgent.id, authority);
      if (!participant) {
        throw this.participants.writeStalled?.() ?? this.#unknownParticipant(this.mainAgent.id, "Fabric Main participant");
      }
      if (participant.stale && this.participants.resolveRoutingLease) {
        if (!await this.#resolveRoutingLease(participant.id, context?.signal, budget)) throw this.#unknownParticipant(participant.id);
        const refreshed = this.#rootRouteSnapshot(participant.id, participant);
        if (!refreshed || refreshed.ownerHostId !== participant.ownerHostId || refreshed.ownerIdentityId !== participant.ownerIdentityId) {
          throw new FabricRouteAuthorityError(participant.id);
        }
        participant = refreshed;
      }
      if (participant.remoteHost && participant.bridgeStale) throw new LeaseResolutionRequired(participant.id,
        new FabricPeerLeaseStaleError(participant.id, participant.remoteHost, 0), participant);
      if (participant.interactive === false) throw new FabricParticipantNonInteractiveError(participant.id);
      if (!participant.capabilities.includes(kind)) throw unsupported(participant, kind);
      const ownerIncarnation = controlOwnerIncarnation(participant);
      if (ownerIncarnation === undefined && (!this.control || participant.controlProtocol === "legacy")) {
        if (options.priority) throw new Error("interrupt priority requires the Fabric Main control protocol");
        warnUnfencedControlDelivery(kind, participant.id, ++this.#legacyDeliveryCount);
        return context?.signal || options.principal
          ? this.actorManager.steerRemote(participant.id, message, kind, data, options.principal, context?.signal)
          : this.actorManager.steerRemote(participant.id, message, kind, data);
      }
      if (!this.control) throw new Error("Fabric control plane is unavailable");
      return this.control.request(
        participant.ownerHostId,
        participant.id,
        kind,
        {
          principal: options.principal,
          ownerIncarnation,
          ...(options.wakeCause ? { wakeCause: options.wakeCause } : {}),
          message,
          data,
          ...(options.priority ? { priority: options.priority } : {}),
          // Carry the local Main default across runtime generations (#3015).
          ...(kind === "followUp"
            ? { triggerTurn: options.triggerTurn ?? true }
            : typeof options.triggerTurn === "boolean" ? { triggerTurn: options.triggerTurn } : {}),
        },
        participant.ownerIdentityId,
        {
          ...(context?.signal ? { signal: context.signal } : {}),
          routedRemoteHost: participant.remoteHost ?? null,
          ...(budget?.waited ? { mirrorLeaseWaitBudget: budget } : {}),
          ...(options.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {}),
          ...(participant.status === "reloading" && typeof participant.reloadUntil === "number"
            ? { timeoutMs: Math.max(1, participant.reloadUntil - Date.now()) } : {}),
        },
      );
    }

    if (options.priority) throw new Error("interrupt priority is supported only for a Main target");

    // Explicit Main addresses never reach task/actor resolution, even when absent.
    if (id.trim().startsWith("session:")) {
      throw this.#unknownParticipant(id);
    }

    // Local one-shot agent: forward between its turns via the worker's
    // steer.jsonl channel, preserving the child's accumulated context.
    try {
      const status = this.manager.status(id);
      context?.activity?.({ type: "entity", id, kind: "agent", name: status.name });
      const result =
        kind === "steer"
          ? this.manager.steer(id, message, data, provenance)
          : status.runner === "pi"
            ? this.manager.followUp(id, message, data, provenance, { deadlineMs: options.deadlineMs ?? DEFAULT_FOLLOW_UP_DEADLINE_MS })
            : this.manager.followUp(id, message, data, provenance);
      return { queued: true, messageId: result.messageId, routed: "local",
        ...(result.undelivered ? { undelivered: result.undelivered } : {}),
        ...(result.warning ? { warning: result.warning } : {}),
        ...(result.deadlineAt !== undefined ? { deadlineAt: result.deadlineAt } : {}) };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
    }

    // A process-owned task that disappeared during delivery must not fall through
    // into directory-backed publication outside the availability wrapper.
    if (provenLocal) throw new Error(`Unknown Fabric agent: ${id}`);

    // An agent another host owns (a durable child in its spawner's resident host, or a peer's
    // task agent) takes steer and follow-up through its owner (smarty-dev#1323).
    let remoteAgent = this.#get(id, true);
    if (remoteAgent?.kind === "agent" && !remoteAgent.local) {
      if (terminalAgentStatuses.has(remoteAgent.status) && remoteAgent.stale) throw new FabricRouteAuthorityError(remoteAgent.id);
      const nativeTerminalTask = remoteAgent.runner === "pi" && remoteAgent.transport === "process" &&
        terminalAgentStatuses.has(remoteAgent.status) && Boolean(remoteAgent.finalAnswerReceiptId);
      if (nativeTerminalTask) {
        // Terminal tasks advertise no ingress capability. Their still-live exact
        // owner must nevertheless answer with the typed final receipt refusal.
        // Revalidate authority; stale/withdrawn/replaced owners never gain a route.
        const fresh = this.#directoryRead(() => this.participants.get(remoteAgent!.id, undefined, { fresh: true }));
        if (!fresh || fresh.stale || fresh.kind !== "agent" || fresh.runner !== "pi" || fresh.transport !== "process" ||
            !terminalAgentStatuses.has(fresh.status) || fresh.finalAnswerReceiptId !== remoteAgent.finalAnswerReceiptId ||
            fresh.startedAt !== remoteAgent.startedAt || fresh.ownerHostId !== remoteAgent.ownerHostId ||
            fresh.ownerIdentityId !== remoteAgent.ownerIdentityId || fresh.rootId !== remoteAgent.rootId ||
            fresh.remoteHost !== remoteAgent.remoteHost) throw new FabricRouteAuthorityError(remoteAgent.id);
        remoteAgent = fresh;
      }
      if (!nativeTerminalTask && !remoteAgent.capabilities.includes(kind)) throw new Error(`Fabric participant ${remoteAgent.id} does not support ${kind}`);
      // Capture the epoch from the final (possibly revalidated) record, never an earlier read.
      const ownerIncarnation = controlOwnerIncarnation(remoteAgent);
      if (!this.control) throw new Error("Fabric control plane is unavailable");
      context?.activity?.({ type: "entity", id: remoteAgent.id, kind: "agent", name: remoteAgent.name });
      return this.control.request(
        remoteAgent.ownerHostId,
        remoteAgent.id,
        kind,
        { message, data, principal: options.principal, ownerIncarnation },
        remoteAgent.ownerIdentityId,
        { idempotencyKey: options.idempotencyKey, routedRemoteHost: remoteAgent.remoteHost ?? null, ...(context?.signal ? { signal: context.signal } : {}) },
      );
    }
    // Persistent actors consume both delivery modes through their serial mailbox.
    this.actorManager.validateDirectMessage(message, data);
    let target: { actor?: FabricActorInfo; participant?: FabricParticipantInfo };
    try {
      target = await this.#resolveActorMessageTarget(id, recoverLease);
    } catch (error) {
      if (error instanceof Error && /Unknown Fabric actor/.test(error.message)) {
        throw this.participants.writeStalled?.() ?? this.#unknownParticipant(id);
      }
      throw error;
    }
    const { actor, participant } = target;
    const localActor = Boolean(actor && (this.actorManager.owns?.(actor.id) ?? (!participant || participant.local)));
    const binding = options.binding && context && localActor
      ? await this.resolvePiRunBinding(options.binding, actor!.runner, context, actor!.routeClass !== undefined)
      : options.binding;
    context?.signal?.throwIfAborted();
    if (actor && localActor) {
      context?.activity?.({ type: "entity", id: actor.id, kind: "actor", name: actor.name });
      const result = this.actorManager.tell(actor.id, message, data, {
        provenance,
        outcomeSend: { eventId: options.idempotencyKey!, to: actor.id, from: (options.from ?? this.actorManager.identity).id, mode: kind },
        ...(binding ? { overrides: binding } : {}),
      });
      return { queued: true, messageId: result.messageId, routed: "local" };
    }
    if (!participant) throw new Error(`Fabric actor ${actor!.id} has no live execution owner`);
    if (!participant.capabilities.includes(kind)) throw unsupported(participant, kind);
    const ownerIncarnation = controlOwnerIncarnation(participant);
    const sessionBinding = actor?.binding;
    const ownRoot = participant.rootId === this.mainAgent.id;
    const resolvedBinding = ownRoot ? binding : actor
      ? this.actorManager.resolveBinding(actor.id, binding)
      : binding;
    const needsBinding = Boolean(
      resolvedBinding?.model ||
        resolvedBinding?.thinking ||
        (!ownRoot && (sessionBinding?.model || sessionBinding?.thinking)),
    );
    if (needsBinding && !participant.capabilities.includes("actor-bindings")) {
      throw new Error(`Fabric actor owner ${participant.ownerHostId} does not support session bindings`);
    }
    if (ownerIncarnation === undefined && (!this.control || participant.controlProtocol === "legacy")) {
      if (needsBinding) throw new Error(`Fabric actor owner ${participant.ownerHostId} has no binding control channel`);
      warnUnfencedControlDelivery(kind, participant.id, ++this.#legacyDeliveryCount);
      return context?.signal || options.principal
        ? this.actorManager.steerRemote(participant.id, message, kind, data, options.principal, context?.signal)
        : this.actorManager.steerRemote(participant.id, message, kind, data);
    }
    if (!this.control) throw new Error("Fabric control plane is unavailable");
    const control = this.control;
    // The same idempotency key (one commandId) covers the one dormant-wake resend.
    return this.sendAfterDormantWake(participant, owner => {
      if (!owner.capabilities.includes(kind)) throw unsupported(owner, kind);
      if (needsBinding && !owner.capabilities.includes("actor-bindings")) {
        throw new Error(`Fabric actor owner ${owner.ownerHostId} does not support session bindings`);
      }
      return control.request(
        owner.ownerHostId,
        owner.id,
        kind,
        {
          principal: options.principal,
          ownerIncarnation: controlOwnerIncarnation(owner),
          message,
          data,
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
          ...(needsBinding && resolvedBinding ? { binding: resolvedBinding } : {}),
          ...(ownRoot ? { bindingProvenance: { kind: "owner-defaults" as const, rootId: this.mainAgent.id } } : {}),
        },
        owner.ownerIdentityId,
        {
          idempotencyKey: options.idempotencyKey, routedRemoteHost: owner.remoteHost ?? null,
          // Retained dormant authority has no live-owner ACK window yet. Its committed
          // command must include bounded cold startup, formerly waited out in preflight.
          ...(owner.stale ? { timeoutMs: 90_000 } : {}),
          ...(context?.signal ? { signal: context.signal } : {}),
        },
      );
    }, context?.signal);
  }

  /**
   * Dormant wake re-resolve (#774 with the #709 fence). A message to a dormant actor addresses its
   * retained wake route, which names the epoch of the activation that went dormant. Its committed
   * command wakes the actor; the woken successor runs under its own fresh epoch, publishes it
   * (participant record, then its rewritten wake route) and refuses the predecessor epoch with
   * FABRIC_CONTROL_STALE_INCARNATION before any claim or handler. The refusal attests no run.
   *
   * ponytail: one re-read and one resend, never a loop. Only a target this router resolved from a
   * retained dormant route qualifies. Re-read its participant once; only when the same owner and
   * actor lineage now advertise a different epoch, resend once under the same idempotency key. The
   * key keeps the commandId, so the owner's claim answers any duplicate with the one outcome. A
   * second refusal, a refusal from a live (non-dormant) target and every other error reach the
   * caller unchanged: re-resolving and deciding to send again stays the caller's decision (#709).
   */
  async sendAfterDormantWake<T>(
    addressed: FabricParticipantInfo,
    send: (owner: FabricParticipantInfo) => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    try {
      return await send(addressed);
    } catch (error) {
      if (!(error instanceof FabricControlStaleIncarnationError) || !this.#dormantRoutes.has(addressed)) throw error;
      let current: FabricParticipantInfo | undefined;
      let fresh: string | undefined;
      try {
        current = (await this.resolveActorMessageTarget(addressed.id, signal)).participant;
        fresh = current ? controlOwnerIncarnation(current) : undefined;
      } catch { signal?.throwIfAborted(); throw error; }
      signal?.throwIfAborted();
      if (!current || fresh === undefined || fresh === controlOwnerIncarnation(addressed) ||
          current.id !== addressed.id || current.kind !== "actor" || current.rootId !== addressed.rootId ||
          current.ownerHostId !== addressed.ownerHostId || current.ownerIdentityId !== addressed.ownerIdentityId ||
          current.actorOwnershipToken !== addressed.actorOwnershipToken ||
          (current.remoteHost ?? null) !== (addressed.remoteHost ?? null)) throw error;
      return send(current);
    }
  }

  async acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
    verification?: "mesh" | "bridge",
  ): Promise<FabricControlAcceptance> {
    try {
      assertSteerPriority(command.priority, command.operation);
      if (command.priority && !(this.mainAgent.local && this.mainAgent.matches(command.targetId))) {
        throw new Error("interrupt priority is supported only for a Main target");
      }
    } catch (error) { return { accepted: false, error: error instanceof Error ? error.message : String(error) }; }
    if (command.operation === "setModel" || command.operation === "setThinking") {
      return { accepted: false, error: "remote Main model changes are not supported yet; see smarty-dev#4153" };
    }
    if (command.operation === "cancel") {
      return { accepted: false, error: "Cancel commands are handled by the control plane" };
    }
    if (command.operation === "stop") {
      try {
        await this.manager.stop(command.targetId);
        this.participants.scheduleRefresh();
        return { accepted: true, messageId: command.commandId };
      } catch (error) {
        if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
          return { accepted: false, error: error instanceof Error ? error.message : String(error) };
        }
      }
      try {
        const actor = this.actorManager.status(command.targetId);
        const ownership = this.#get(actor.id);
        if (ownership && !ownership.local) {
          return { accepted: false, error: `Participant ${actor.id} is owned by ${ownership.ownerHostId}` };
        }
        await this.actorManager.stop(actor.id);
        this.participants.scheduleRefresh();
        return { accepted: true, messageId: command.commandId };
      } catch (error) {
        if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) {
          return { accepted: false, error: error instanceof Error ? error.message : String(error) };
        }
      }
      return { accepted: false, error: `Owner does not control Fabric participant ${command.targetId}` };
    }

    const provenance = verification === "mesh" || verification === "bridge"
      ? fabricTurnProvenance(from, command.operation === "steer" ? "steer" : "followUp", verification, command.principal) : undefined;
    const message = command.message?.trim();
    if (!message) return { accepted: false, error: "Fabric control message must not be empty" };
    if (command.operation === "ask") {
      try {
        const actor = this.actorManager.status(command.targetId);
        const ownership = this.#get(actor.id);
        if (ownership && !ownership.local) {
          return {
            accepted: false,
            error: `Participant ${actor.id} is owned by ${ownership.ownerHostId}`,
          };
        }
        const result = await this.actorManager.ask(
          actor.id,
          message,
          command.data,
          signal,
          { provenance, ...controlActorBindingOptions(command, from, actor.rootId, this.participants.get(from.id)?.rootId), sender: command.sender ?? null },
        );
        return { accepted: true, messageId: result.id, result };
      } catch (error) {
        return {
          accepted: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }
    if (command.operation !== "steer" && command.operation !== "followUp") {
      return { accepted: false, error: "Remote task operations require the task proxy/owner receiver", errorCode: "FABRIC_REMOTE_TASK_POLICY_REFUSED" };
    }
    if (this.mainAgent.local && this.mainAgent.matches(command.targetId)) {
      if (this.#localMainNonInteractive()) {
        return { accepted: false, error: new FabricParticipantNonInteractiveError(this.mainAgent.id).message };
      }
      let result: FabricAgentMessageResult;
      try {
        result = this.mainAgent.deliverAgent(copyFabricWakeAdmission(command, {
        from,
        ...(verification === undefined ? {} : { verification }),
        principal: provenance?.principal,
        message,
        delivery: command.operation,
        ...(command.priority ? { priority: command.priority } : {}),
        deliveryId: command.commandId,
        outcomeSend: { eventId: command.eventId ?? command.commandId, to: this.mainAgent.id, from: from.id, mode: command.operation },
        ...(typeof command.triggerTurn === "boolean"
          ? { triggerTurn: command.triggerTurn }
          : {}),
        ...(command.data === undefined ? {} : { data: command.data }),
        }));
      } catch (error) {
        // Admission refusals retain their typed code across verified mesh/bridge ACKs.
        const errorCode = interruptErrorCode(error);
        return { accepted: false, error: error instanceof Error ? error.message : String(error),
          ...(errorCode ? { errorCode, notRun: true } : {}) };
      }
      return {
        accepted: true,
        messageId: result.messageId,
        ...(typeof result.triggered === "boolean" ? { triggered: result.triggered } : {}),
        ...(typeof result.reason === "string" ? { reason: result.reason } : {}),
        ...(result.halted ? { halted: true as const } : {}),
        ...(result.pendingFollowUps === undefined ? {} : { pendingFollowUps: result.pendingFollowUps }),
        ...(result.oldestAgeS === undefined ? {} : { oldestAgeS: result.oldestAgeS }),
        ...(result.stalled ? { stalled: true as const } : {}),
        ...(result.coalesced ? { coalesced: true as const, replacedMessageId: result.replacedMessageId! } : {}),
      };
    }
    try {
      this.manager.status(command.targetId);
      const result =
        command.operation === "steer"
          ? this.manager.steer(command.targetId, message, command.data, provenance)
          : this.manager.followUp(command.targetId, message, command.data, provenance);
      if (result.undelivered) throw new FabricTargetTerminalError(command.targetId, result.undelivered.finalAnswerReceiptId, result.undelivered.runState);
      return { accepted: true, messageId: result.messageId,
        ...(result.warning ? { warning: result.warning } : {}) };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
        return { accepted: false, error: error instanceof Error ? error.message : String(error),
          ...terminalRejectionFields(error) };
      }
    }
    try {
      const actor = this.actorManager.status(command.targetId);
      const ownership = this.#get(actor.id);
      if (ownership && !ownership.local) {
        return { accepted: false, error: `Participant ${actor.id} is owned by ${ownership.ownerHostId}` };
      }
      const options = controlActorBindingOptions(command, from, actor.rootId, this.participants.get(from.id)?.rootId);
      // Refuse inexact owner-side model selections before acknowledging a synchronous tell.
      // Validate without promoting the owner's current defaults into fixed per-call overrides.
      await this.actorManager.resolveActivationBinding(actor.id, options);
      const result = this.actorManager.tell(actor.id, message, command.data, { provenance, ...options, sender: command.sender ?? null });
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) {
        return { accepted: false, error: error instanceof Error ? error.message : String(error) };
      }
    }
    return { accepted: false, error: `Owner does not control Fabric participant ${command.targetId}` };
  }

  /** A lease can still look fresh after SIGKILL. For this root's non-owned durable
   * actors, check the real owner before delivery, not just participant freshness. */
  async resolveActorMessageTarget(id: string, signal?: AbortSignal): Promise<ReturnType<AgentMessageRouter["resolveActorTarget"]>> {
    return this.#withDirectory(() => this.#resolveActorMessageTarget(id), id, true, signal);
  }

  async #resolveActorMessageTarget(id: string, recoverLease = true): Promise<ReturnType<AgentMessageRouter["resolveActorTarget"]>> {
    if (id.trim().startsWith("session:")) throw this.#unknownParticipant(id, "Fabric Main participant");
    const meshRoot = this.residency?.options.config.meshRoot ?? this.actorManager.mesh?.root;
    const definition = this.resolveActorTarget(id, false);
    if (meshRoot && definition.actor?.residency === "durable" &&
        ["dormant", "failed", "failing-preparation"].includes(definition.actor.status) &&
        !this.actorManager.owns?.(definition.actor.id) && !definition.participant) {
      const { dormantActorRoute } = await import("../residency/wake.js");
      const retained = dormantActorRoute(meshRoot, definition.actor);
      // No wake effect here. The ordinary caller checks capability/binding/control;
      // control then admits and commits a command. ONLY its post-commit hook wakes.
      if (retained) {
        if (!this.control) throw new Error(`Fabric actor owner ${retained.ownerHostId} has no acknowledged control channel`);
        this.#dormantRoutes.add(retained); // see sendAfterDormantWake
        return { actor: definition.actor, participant: retained };
      }
    }
    const target = this.resolveActorTarget(id, recoverLease);
    const { actor, participant } = target;
    if (this.residency && (actor?.residency ?? participant?.residency) === "durable" &&
        !(actor && (this.actorManager.owns?.(actor.id) ?? participant?.local)) &&
        (actor?.rootId === this.residency.options.config.rootId || participant?.ownerHostId === this.residency.hostId)) {
      if (!kernelFenceAvailable()) {
        if (!participant) throw new Error(`Fabric actor ${actor!.id} is owned by another host`);
        return target;
      }
      await this.#withDurableRecovery(id, () => this.residency!.ensureActor(actor?.id ?? participant!.id));
      return this.resolveActorTarget(actor?.id ?? participant!.id, recoverLease);
    }
    return target;
  }

  /** Status uses the same bounded lease recovery as followUp/steer. */
  async resolveParticipantFresh(id: string, signal?: AbortSignal): Promise<FabricParticipantInfo | undefined> {
    return this.#withDirectory(async (authority, budget) => {
      const known = this.#get(id);
      if (known?.kind === "root" && known.remoteHost || authority?.kind === "root" && authority.remoteHost) {
        const fresh = this.#rootRouteSnapshot(id, authority ?? known);
        if (fresh?.bridgeStale) throw new LeaseResolutionRequired(id,
          new FabricPeerLeaseStaleError(id, fresh.remoteHost!, 0), fresh);
        return fresh;
      }
      if (known || !this.participants.resolveRoutingLease) return known;
      const pin = this.#directoryRead(() => this.participants.lastKnown?.(id)?.participant);
      if (!await this.#resolveRoutingLease(id, signal, budget)) throw this.#unknownParticipant(id);
      const fresh = pin?.kind === "root" && pin.remoteHost ? this.#rootRouteSnapshot(id, pin) : this.#get(id);
      if (fresh?.remoteHost && fresh.bridgeStale) throw new LeaseResolutionRequired(id,
        new FabricPeerLeaseStaleError(id, fresh.remoteHost, 0), fresh);
      return fresh;
    }, id, true, signal);
  }

  /** Async callers revalidate an overdue/uninitialized view before resolving ownership. */
  async resolveActorTargetFresh(id: string, signal?: AbortSignal): Promise<ReturnType<AgentMessageRouter["resolveActorTarget"]>> {
    return this.#withDirectory(async () => this.resolveActorTarget(id), id, true, signal);
  }

  resolveActorTarget(id: string, recoverLease = true): {
    actor?: FabricActorInfo;
    participant?: FabricParticipantInfo;
  } {
    if (id.trim().startsWith("session:")) throw new Error(`Fabric Main participant ${id} is not an actor`);
    let actor: FabricActorInfo | undefined;
    try {
      actor = this.actorManager.status(id);
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) throw error;
    }
    const participant = this.#get(actor?.id ?? id, true);
    if (!actor && (!participant || participant.kind !== "actor")) {
      throw this.#unknownParticipant(id, "Fabric actor");
    }
    // A passive/project definition is not execution ownership. A retained exact
    // owner hidden by lease filtering must take the same pre-publication grace
    // path as an unknown target, before resident activation/replacement is tried.
    // Truly ownerless definitions keep their existing activation/status behavior.
    if (recoverLease && actor && !participant && !(this.actorManager.owns?.(actor.id) ?? false) &&
      this.participants.resolveRoutingLease &&
      this.#directoryRead(() => this.participants.lastKnown?.(actor.id))?.participant.kind === "actor") {
      // Preserve the exact resolved ID through #route's unknown-actor fallback.
      throw this.#unknownParticipant(actor.id);
    }
    return {
      ...(actor ? { actor } : {}),
      ...(participant?.kind === "actor" ? { participant } : {}),
    };
  }

}
