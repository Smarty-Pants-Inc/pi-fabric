import { boundAgentSpawner } from "../agents/spawner.js";
import { terminalRejectionFields } from "../agents/terminal-target.js";
import { terminalAgentStatuses } from "../agents/lifecycle.js";
import { invocationFabricPrincipal, snapshotFabricInvocation, fabricTurnProvenance, type FabricPrincipal } from "../fabric-provenance.js";
import type { AgentManager } from "../agents/manager.js";
import { DEFAULT_FOLLOW_UP_DEADLINE_MS } from "../agents/follow-up-delivery.js";
import type { ActorManager } from "../actors/manager.js";
import type { FabricActorInfo, FabricActorRunBinding } from "../actors/types.js";
import type { FabricAgentMessageResult, FabricMainAgentTarget } from "../main-agent.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { FabricInvocationContext } from "../protocol.js";
import { controlActorBindingOptions, type FabricControlPlane, type FabricControlCommand, type FabricControlAcceptance } from "../topology/control-plane.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../topology/types.js";
import type { FabricAgentRunner } from "../config.js";
import type { ResidencyClient } from "../residency/client.js";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { FabricParticipantStaleError } from "../topology/host-leases.js";
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
  const peer = participants.peers?.().find((candidate) => candidate.id === id);
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

/** Distinguishes a pre-publication lease wait from an uncertain published ACK. */
class LeaseResolutionStale extends FabricParticipantStaleError {}

/** Only lookup can request lease recovery; owner ACKs and handlers must never replay. */
class LeaseResolutionRequired extends Error {
  constructor(readonly targetId: string, readonly original: Error) { super(original.message, { cause: original }); }
}

export class AgentMessageRouter {
  readonly #taskReturnAddress = readTaskReturnAddress();
  constructor(
    readonly manager: Pick<AgentManager, "status" | "steer" | "followUp" | "stop">,
    readonly actorManager: Pick<ActorManager, "identity" | "status" | "validateDirectMessage" | "tell" | "ask" | "stop" | "steerRemote" | "resolveBinding" | "resolveActivationBinding"> & { owns?: (id: string) => boolean },
    readonly mainAgent: Pick<FabricMainAgentTarget, "matches" | "local" | "id" | "deliverAgent" | "interactive">,
    readonly participants: Pick<FabricParticipantSource, "get" | "scheduleRefresh" | "writeStalled" | "lastKnown"> & Partial<Pick<FabricParticipantSource, "peers" | "list" | "lineageAlive" | "routingUnavailable" | "refreshRoutingView" | "resolveRoutingLease" | "retainedRouteAllowed">>,
    readonly control: Pick<FabricControlPlane, "request"> | undefined,
    readonly resolvePiRunBinding: (binding: FabricActorRunBinding, runner: FabricAgentRunner, context: FabricInvocationContext, requiredPin?: boolean) => FabricActorRunBinding | Promise<FabricActorRunBinding>,
    readonly residency?: Pick<ResidencyClient, "ensureActor" | "hostId"> & { options: { config: { rootId: string; meshRoot: string } } },
    readonly spawner = boundAgentSpawner(),
  ) {}
  #get(id: string): FabricParticipantInfo | undefined {
    // Discovery and admission share this directory/root. A cached negative may predate the
    // bridge's first presence refresh: re-read its files + state before declaring it unknown.
    // lastKnown also reads fresh, but deliberately discards newly live records (smarty-dev#2377).
    const participant = this.#directoryRead(() =>
      this.participants.get(id) ?? this.participants.get(id, undefined, { fresh: true }));
    const reason = !participant ? this.#directoryUnavailable() : undefined;
    if (reason) throw new FabricDirectoryUnavailableError(reason);
    return participant;
  }

  #unknownParticipant(id: string, label?: string): Error {
    const error = this.#directoryRead(() => unknownParticipant(this.participants, id, label));
    return this.participants.resolveRoutingLease && error.message.startsWith("Unknown ")
      ? new LeaseResolutionRequired(id, error) : error;
  }

  async #resolveRoutingLease(id: string): Promise<boolean | undefined> {
    try { return await this.participants.resolveRoutingLease?.(id); }
    catch (error) {
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

  async #withDirectory<T>(operation: () => Promise<T>, id?: string, recoverLease = true): Promise<T> {
    const reason = this.#directoryUnavailable();
    if (reason) {
      if (recoverLease && id && await this.#resolveRoutingLease(id) === false) throw this.#unknownParticipant(id);
      await this.#refreshDirectory(reason);
    }
    try {
      return await operation();
    } catch (error) {
      // Lease recovery is pre-publication only. A failed ACK is never replayed here.
      if (recoverLease && error instanceof LeaseResolutionRequired && this.participants.resolveRoutingLease) {
        if (await this.#resolveRoutingLease(error.targetId)) return operation();
        throw error.original;
      }
      // A read can fail after the preflight. Retry resolution once, never delivery after
      // an ACK/control error: only pre-publication directory failures have this class.
      if (!(error instanceof FabricDirectoryUnavailableError) || reason || !this.participants.refreshRoutingView) throw error;
      if (recoverLease && id && await this.#resolveRoutingLease(id) === false) throw this.#unknownParticipant(id);
      await this.#refreshDirectory(error.message);
      return operation();
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

  #rootRouteSnapshot(id: string): FabricParticipantInfo | undefined {
    const cached = this.#directoryRead(() => this.participants.get(id));
    // Keep a mirrored root's original bridge for the control plane's fresh admission check.
    if (cached?.kind === "root" && cached.remoteHost) return cached;
    const fresh = this.#directoryRead(() => this.participants.get(id, undefined, { fresh: true })) ?? this.#lapsedRoot(id);
    if (cached?.kind === "root") {
      // Refresh native lifecycle state only under the same authority. A replacement mirror
      // with the same id must never turn a private native delivery into bridge publication.
      if (!fresh || fresh.kind !== "root" || fresh.remoteHost || fresh.id !== cached.id ||
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
    // A published name survives an ordinary lease lapse just like its exact session id.
    // Use fresh raw presence, but add only eligible retained native roots to the live set.
    const matches = this.participants.list
      ? this.#directoryRead(() => this.participants.list!({ scope: "project", kinds: ["root"], includeStale: true, fresh: true }))
      .filter((participant) => participant.name === target)
      : [];
    if (matches.length > 1) {
      throw new Error(`Ambiguous Fabric participant: ${id} (${matches.map((participant) => participant.id).sort().join(", ")}); use an exact id`);
    }
    const root = matches[0];
    // Presence without current owner authority still makes a selector ambiguous. Never
    // silently choose a same-named replacement just because one lease is unavailable.
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
  withStopDirectory<T>(id: string, operation: () => Promise<T>): Promise<T> {
    return this.isProcessOwnedTarget(id) ? operation() : this.#withDirectory(operation, id);
  }

  /** Normalize late stop-route reads just like message-route reads. */
  resolveStopParticipant(id: string): FabricParticipantInfo | undefined {
    return this.#get(id);
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
      triggerTurn?: boolean;
      binding?: FabricActorRunBinding;
      deadlineMs?: number;
      idempotencyKey?: string;
    } = {},
  ): Promise<FabricAgentMessageResult> {
    // Resolve the child's bound target before recovery so retries retain the same
    // actor ownership/fence checks rather than trying to recover the alias itself.
    if (id === "spawner") {
      if (!this.spawner) throw new Error("This worker has no bound Fabric spawner; specify an explicit reply target");
      id = this.spawner.id;
    }
    // Capture before routing yields; a queued incoming turn cannot change this send.
    if (context) context = snapshotFabricInvocation(context);
    options = { ...options, idempotencyKey: options.idempotencyKey ?? randomUUID(), principal: context ? invocationFabricPrincipal(context) : undefined };
    // Task-local `main` remains its immutable immediate return address.
    if (id.trim() === "main" && this.#taskReturnAddress?.spawnerId) id = this.#taskReturnAddress.spawnerId;
    const provenLocal = this.isProcessOwnedTarget(id);
    let result: FabricAgentMessageResult;
    try {
      result = provenLocal
        ? await this.#route(id, message, data, kind, context, options, true)
        : await this.#withDurableRecovery(id, recovering =>
          this.#withDirectory(() => this.#route(id, message, data, kind, context, options, false, !recovering), id, !recovering));
    } catch (error) {
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
      triggerTurn?: boolean;
      binding?: FabricActorRunBinding;
      deadlineMs?: number;
      idempotencyKey?: string;
    } = {},
    provenLocal = false,
    recoverLease = true,
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
    const remoteRoot = isMain || provenLocal ? undefined : this.#rootRouteSnapshot(id);
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
        return this.mainAgent.deliverAgent({
          from: options.from ?? this.actorManager.identity,
          verification: "mesh", // In-process registered producer, not a received command.
          principal: options.principal,
          message,
          delivery: kind,
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
          ...(data === undefined ? {} : { data }),
        });
      }
      let participant = remoteRoot ?? this.#rootRouteSnapshot(this.mainAgent.id);
      if (!participant) {
        throw this.participants.writeStalled?.() ?? this.#unknownParticipant(this.mainAgent.id, "Fabric Main participant");
      }
      if (participant.stale && this.participants.resolveRoutingLease) {
        if (!await this.#resolveRoutingLease(participant.id)) throw this.#unknownParticipant(participant.id);
        const refreshed = this.#rootRouteSnapshot(participant.id);
        if (!refreshed || refreshed.ownerHostId !== participant.ownerHostId || refreshed.ownerIdentityId !== participant.ownerIdentityId) {
          throw new FabricRouteAuthorityError(participant.id);
        }
        participant = refreshed;
      }
      if (participant.interactive === false) throw new FabricParticipantNonInteractiveError(participant.id);
      if (!participant.capabilities.includes(kind)) throw unsupported(participant, kind);
      if (!this.control || participant.controlProtocol === "legacy") {
        return context?.signal || options.principal
          ? this.actorManager.steerRemote(participant.id, message, kind, data, options.principal, context?.signal)
          : this.actorManager.steerRemote(participant.id, message, kind, data);
      }
      return this.control.request(
        participant.ownerHostId,
        participant.id,
        kind,
        {
          principal: options.principal,
          message,
          data,
          // Carry the local Main default across runtime generations (#3015).
          ...(kind === "followUp"
            ? { triggerTurn: options.triggerTurn ?? true }
            : typeof options.triggerTurn === "boolean" ? { triggerTurn: options.triggerTurn } : {}),
        },
        participant.ownerIdentityId,
        {
          ...(context?.signal ? { signal: context.signal } : {}),
          routedRemoteHost: participant.remoteHost ?? null,
          ...(options.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {}),
          ...(participant.status === "reloading" && typeof participant.reloadUntil === "number"
            ? { timeoutMs: Math.max(1, participant.reloadUntil - Date.now()) } : {}),
        },
      );
    }

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
    let remoteAgent = this.#get(id);
    if (remoteAgent?.kind === "agent" && !remoteAgent.local) {
      const nativeTerminalTask = remoteAgent.runner === "pi" && remoteAgent.transport === "process" && terminalAgentStatuses.has(remoteAgent.status);
      if (nativeTerminalTask) {
        // Terminal tasks advertise no ingress capability. Their still-live exact
        // owner must nevertheless answer with the typed final receipt refusal.
        // Revalidate authority; stale/withdrawn/replaced owners never gain a route.
        const fresh = this.#directoryRead(() => this.participants.get(remoteAgent!.id, undefined, { fresh: true }));
        if (!fresh || fresh.stale || fresh.kind !== "agent" || fresh.runner !== "pi" || fresh.transport !== "process" ||
            !terminalAgentStatuses.has(fresh.status) || fresh.startedAt !== remoteAgent.startedAt || fresh.ownerHostId !== remoteAgent.ownerHostId ||
            fresh.ownerIdentityId !== remoteAgent.ownerIdentityId || fresh.rootId !== remoteAgent.rootId ||
            fresh.remoteHost !== remoteAgent.remoteHost) throw new FabricRouteAuthorityError(remoteAgent.id);
        remoteAgent = fresh;
      }
      if (!nativeTerminalTask && !remoteAgent.capabilities.includes(kind)) throw new Error(`Fabric participant ${remoteAgent.id} does not support ${kind}`);
      if (!this.control) throw new Error("Fabric control plane is unavailable");
      context?.activity?.({ type: "entity", id: remoteAgent.id, kind: "agent", name: remoteAgent.name });
      return this.control.request(
        remoteAgent.ownerHostId,
        remoteAgent.id,
        kind,
        { message, data, principal: options.principal },
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
        ...(binding ? { overrides: binding } : {}),
      });
      return { queued: true, messageId: result.messageId, routed: "local" };
    }
    if (!participant) throw new Error(`Fabric actor ${actor!.id} has no live execution owner`);
    if (!participant.capabilities.includes(kind)) throw unsupported(participant, kind);
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
    if (!this.control || participant.controlProtocol === "legacy") {
      if (needsBinding) {
        throw new Error(`Fabric actor owner ${participant.ownerHostId} has no binding control channel`);
      }
      return context?.signal || options.principal
          ? this.actorManager.steerRemote(participant.id, message, kind, data, options.principal, context?.signal)
          : this.actorManager.steerRemote(participant.id, message, kind, data);
    }
    return this.control.request(
      participant.ownerHostId,
      participant.id,
      kind,
      {
        principal: options.principal,
        message,
        data,
        ...(typeof options.triggerTurn === "boolean"
          ? { triggerTurn: options.triggerTurn }
          : {}),
        ...(needsBinding && resolvedBinding ? { binding: resolvedBinding } : {}),
        ...(ownRoot ? { bindingProvenance: { kind: "owner-defaults" as const, rootId: this.mainAgent.id } } : {}),
      },
      participant.ownerIdentityId,
      { idempotencyKey: options.idempotencyKey, routedRemoteHost: participant.remoteHost ?? null, ...(context?.signal ? { signal: context.signal } : {}) },
    );
  }

  async acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
    verification?: "mesh" | "bridge",
  ): Promise<FabricControlAcceptance> {
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
          { provenance, ...controlActorBindingOptions(command, from, actor.rootId, this.participants.get(from.id)?.rootId) },
        );
        return { accepted: true, messageId: result.id, result };
      } catch (error) {
        return {
          accepted: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }
    if (this.mainAgent.local && this.mainAgent.matches(command.targetId)) {
      if (this.#localMainNonInteractive()) {
        return { accepted: false, error: new FabricParticipantNonInteractiveError(this.mainAgent.id).message };
      }
      let result: FabricAgentMessageResult;
      try {
        result = this.mainAgent.deliverAgent({
        from,
        ...(verification === undefined ? {} : { verification }),
        principal: provenance?.principal,
        message,
        delivery: command.operation,
        deliveryId: command.commandId,
        ...(typeof command.triggerTurn === "boolean"
          ? { triggerTurn: command.triggerTurn }
          : {}),
        ...(command.data === undefined ? {} : { data: command.data }),
        });
      } catch (error) {
        // A full followUp queue, for example: the sender sees why.
        return { accepted: false, error: error instanceof Error ? error.message : String(error) };
      }
      return {
        accepted: true,
        messageId: result.messageId,
        ...(typeof result.triggered === "boolean" ? { triggered: result.triggered } : {}),
        ...(typeof result.reason === "string" ? { reason: result.reason } : {}),
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
      const result = this.actorManager.tell(actor.id, message, command.data, { provenance, ...options });
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
  async resolveActorMessageTarget(id: string): Promise<ReturnType<AgentMessageRouter["resolveActorTarget"]>> {
    return this.#withDirectory(() => this.#resolveActorMessageTarget(id), id);
  }

  async #resolveActorMessageTarget(id: string, recoverLease = true): Promise<ReturnType<AgentMessageRouter["resolveActorTarget"]>> {
    if (id.trim().startsWith("session:")) throw this.#unknownParticipant(id, "Fabric Main participant");
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
  async resolveParticipantFresh(id: string): Promise<FabricParticipantInfo | undefined> {
    return this.#withDirectory(async () => {
      const known = this.#get(id);
      if (known || !this.participants.resolveRoutingLease) return known;
      if (!await this.#resolveRoutingLease(id)) throw this.#unknownParticipant(id);
      return this.#get(id);
    }, id);
  }

  /** Async callers revalidate an overdue/uninitialized view before resolving ownership. */
  async resolveActorTargetFresh(id: string): Promise<ReturnType<AgentMessageRouter["resolveActorTarget"]>> {
    return this.#withDirectory(async () => this.resolveActorTarget(id), id);
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
    const participant = this.#get(actor?.id ?? id);
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
