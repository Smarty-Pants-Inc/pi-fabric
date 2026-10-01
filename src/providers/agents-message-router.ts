import { invocationFabricPrincipal, snapshotFabricInvocation, fabricTurnProvenance, type FabricPrincipal } from "../fabric-provenance.js";
import type { AgentManager } from "../agents/manager.js";
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
import path from "node:path";
import { kernelFenceAvailable } from "../residency/file-lock.js";
import { processAlive } from "../storage/scratch.js";

// MeshStore's default stale window; recovery waits, never weakens mesh locking.
export const RESIDENT_MESH_STALE_WINDOW_MS = 30_000;

// A quiesced root keeps heartbeating with no capabilities while it shuts down; "does not support"
// read as a broken session (smarty-dev#1113).
const unsupported = (participant: { id: string; status?: string }, kind: string): Error =>
  participant.status === "stopping"
    ? new Error(`Fabric participant ${participant.id} is shutting down; its session will relaunch or end. Retry after it restarts.`)
    : new Error(`Fabric participant ${participant.id} does not support ${kind}`);

// A root whose lease lapsed this recently may still be live: its heartbeat can be late
// under mesh lock contention or a busy event loop (smarty-dev#447). A reply still goes to
// its owner host, which acknowledges it when alive; a gone host leaves the outcome unknown.
// ponytail: 5 min covers every lease flap seen in the fleet; a longer lapse reads as ended.
const LAPSED_ROOT_REPLY_WINDOW_MS = 5 * 60_000;
// A Pi session id (8-4-4-4-12). Actor and agent ids are 32 hex with no dashes, so they never match.
const SESSION_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// Route messages using only the ownership, delivery, and binding ports needed here.
// Says why a target cannot be resolved; the prefix stays "Unknown <label>: <id>".
export const unknownParticipant = (
  participants: Pick<FabricParticipantSource, "lastKnown">,
  id: string,
  label = "Fabric participant",
): Error => {
  const known = participants.lastKnown?.(id);
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

export class AgentMessageRouter {
  constructor(
    readonly manager: Pick<AgentManager, "status" | "steer" | "followUp" | "stop">,
    readonly actorManager: Pick<ActorManager, "identity" | "status" | "validateDirectMessage" | "tell" | "ask" | "stop" | "steerRemote" | "resolveBinding"> & { owns?: (id: string) => boolean },
    readonly mainAgent: Pick<FabricMainAgentTarget, "matches" | "local" | "id" | "deliverAgent">,
    readonly participants: Pick<FabricParticipantSource, "get" | "scheduleRefresh" | "writeStalled" | "lastKnown">,
    readonly control: Pick<FabricControlPlane, "request"> | undefined,
    readonly resolvePiRunBinding: (binding: FabricActorRunBinding, runner: FabricAgentRunner, context: FabricInvocationContext) => FabricActorRunBinding | Promise<FabricActorRunBinding>,
    readonly residency?: Pick<ResidencyClient, "ensureActor" | "hostId"> & { options: { config: { rootId: string; meshRoot: string } } },
  ) {}
  #recentlyLapsedRoot(id: string): FabricParticipantInfo | undefined {
    // A write-stalled mesh explains the lapse, and delivery needs the mesh: report the stall.
    if (this.participants.writeStalled?.()) return undefined;
    const known = this.participants.lastKnown?.(id);
    if (!known || known.participant.kind !== "root" || known.lapsedMs > LAPSED_ROOT_REPLY_WINDOW_MS) return undefined;
    // Reload leases are a hard bound, not an ordinary heartbeat flap; an exit is never routable.
    if (["reloading", "stopping"].includes(known.participant.status)) return undefined;
    // A mirrored lease lapses when the mesh bridge stops: nothing would carry the reply, so the
    // sender gets the lapse error at once instead of an acknowledgement timeout (smarty-dev#2004).
    if (known.participant.remoteHost) return undefined;
    return known.participant;
  }

  #rootRouteSnapshot(id: string): FabricParticipantInfo | undefined {
    const cached = this.participants.get(id);
    // Keep a mirrored root's original bridge for the control plane's fresh admission check.
    if (cached?.kind === "root" && cached.remoteHost) return cached;
    const fresh = this.participants.get(id, undefined, { fresh: true });
    if (cached?.kind === "root") {
      // Refresh native lifecycle state only under the same authority. A replacement mirror
      // with the same id must never turn a private native delivery into bridge publication.
      if (!fresh || fresh.kind !== "root" || fresh.remoteHost || fresh.id !== cached.id ||
        fresh.rootId !== cached.rootId || fresh.ownerHostId !== cached.ownerHostId ||
        fresh.ownerIdentityId !== cached.ownerIdentityId) throw new FabricRouteAuthorityError(id);
      return fresh;
    }
    return fresh ?? this.#recentlyLapsedRoot(id);
  }

  // A bare session UUID addresses its Main `session:<uuid>` when no participant has exactly
  // that id (smarty-dev#1729). Only the same UUID is tried: never a guess across ids.
  #sessionTarget(id: string): string {
    const bare = id.trim();
    if (!SESSION_UUID.test(bare) || this.participants.get(bare)) return id;
    const session = `session:${bare}`;
    return this.mainAgent.matches(session) || this.participants.get(session) || this.#recentlyLapsedRoot(session)
      ? session
      : id;
  }

  /** Use the same exact session-UUID alias resolution as delivery when grouping lifecycle sources. */
  isLocalMainTarget(id: string): boolean {
    return this.mainAgent.local && this.mainAgent.matches(this.#sessionTarget(id));
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
    } = {},
  ): Promise<FabricAgentMessageResult> {
    // Capture before routing yields; a queued incoming turn cannot change this send.
    if (context) context = snapshotFabricInvocation(context);
    options = { ...options, principal: context ? invocationFabricPrincipal(context) : undefined };
    const result = await this.#withDurableRecovery(id, () => this.#route(id, message, data, kind, context, options));
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

  /** Only this root's non-owned durable actors can wait out a dead local holder.
   * Unknown/live holders and unrelated routing errors retain the ordinary failure path. */
  async #withDurableRecovery<T>(id: string, operation: () => Promise<T>): Promise<T> {
    let deadline = Date.now() + RESIDENT_MESH_STALE_WINDOW_MS + 10_000;
    for (;;) {
      try { return await operation(); }
      catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "FABRIC_MESH_LOCK_TIMEOUT") ||
            !this.residency || !kernelFenceAvailable()) throw error;
        const { actor, participant } = this.resolveActorTarget(id);
        if ((actor?.residency ?? participant?.residency) !== "durable" ||
            (actor && (this.actorManager.owns?.(actor.id) ?? participant?.local)) ||
            !(actor?.rootId === this.residency.options.config.rootId || participant?.ownerHostId === this.residency.hostId)) throw error;
        let owner: string;
        try { owner = fs.readFileSync(path.join(this.residency.options.config.meshRoot, ".lock", "owner"), "utf8"); }
        catch { throw error; }
        const [, pidText, createdText] = owner.trim().split("\n");
        const pid = Number(pidText), createdAt = Number(createdText);
        if (!Number.isSafeInteger(pid) || pid <= 0 || processAlive(pid) ||
            !Number.isFinite(createdAt) || createdAt < 0 || createdAt > Date.now()) throw error;
        deadline = Math.min(deadline, createdAt + RESIDENT_MESH_STALE_WINDOW_MS + 10_000);
        const now = Date.now();
        const waitMs = Math.max(100, createdAt + RESIDENT_MESH_STALE_WINDOW_MS - now);
        // Leave a full mesh write-timeout budget for the final attempt. Never extend for a new holder.
        if (now + waitMs + 10_000 > deadline) throw error;
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
    } = {},
  ): Promise<FabricAgentMessageResult> {
    context?.signal?.throwIfAborted();
    const provenance = fabricTurnProvenance(options.from ?? this.actorManager.identity, kind, "mesh", options.principal);
    id = this.#sessionTarget(id);
    const isMain = this.mainAgent.matches(id);
    const remoteRoot = isMain ? undefined : this.#rootRouteSnapshot(id);
    // Project members include peer roots, not just this host's Main and actors.
    // Resolve their current owner through the same capability/control path.
    if (isMain || remoteRoot?.kind === "root") {
      if (isMain && this.mainAgent.local) {
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
      const participant = remoteRoot ?? this.#rootRouteSnapshot(this.mainAgent.id);
      if (!participant) {
        throw this.participants.writeStalled?.() ?? unknownParticipant(this.participants, this.mainAgent.id, "Fabric Main participant");
      }
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
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
        },
        participant.ownerIdentityId,
        {
          ...(context?.signal ? { signal: context.signal } : {}),
          routedRemoteHost: participant.remoteHost ?? null,
          ...(participant.status === "reloading" && typeof participant.reloadUntil === "number"
            ? { timeoutMs: Math.max(1, participant.reloadUntil - Date.now()) } : {}),
        },
      );
    }

    // Local one-shot agent: forward between its turns via the worker's
    // steer.jsonl channel, preserving the child's accumulated context.
    try {
      const status = this.manager.status(id);
      context?.activity?.({ type: "entity", id, kind: "agent", name: status.name });
      const result =
        kind === "steer"
          ? this.manager.steer(id, message, data, provenance)
          : this.manager.followUp(id, message, data, provenance);
      return { queued: true, messageId: result.messageId, routed: "local" };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
    }

    // An agent another host owns (a durable child in its spawner's resident host, or a peer's
    // task agent) takes steer and follow-up through its owner (smarty-dev#1323).
    const remoteAgent = this.participants.get(id);
    if (remoteAgent?.kind === "agent" && !remoteAgent.local) {
      if (!remoteAgent.capabilities.includes(kind)) throw new Error(`Fabric participant ${remoteAgent.id} does not support ${kind}`);
      if (!this.control) throw new Error("Fabric control plane is unavailable");
      context?.activity?.({ type: "entity", id: remoteAgent.id, kind: "agent", name: remoteAgent.name });
      return this.control.request(
        remoteAgent.ownerHostId,
        remoteAgent.id,
        kind,
        { message, data, principal: options.principal },
        remoteAgent.ownerIdentityId,
        { routedRemoteHost: remoteAgent.remoteHost ?? null, ...(context?.signal ? { signal: context.signal } : {}) },
      );
    }

    // Persistent actors consume both delivery modes through their serial mailbox.
    this.actorManager.validateDirectMessage(message, data);
    let target: { actor?: FabricActorInfo; participant?: FabricParticipantInfo };
    try {
      target = await this.resolveActorMessageTarget(id);
    } catch (error) {
      if (error instanceof Error && /Unknown Fabric actor/.test(error.message)) {
        throw this.participants.writeStalled?.() ?? unknownParticipant(this.participants, id);
      }
      throw error;
    }
    const { actor, participant } = target;
    const localActor = Boolean(actor && (this.actorManager.owns?.(actor.id) ?? (!participant || participant.local)));
    const binding = options.binding && context && localActor
      ? await this.resolvePiRunBinding(options.binding, actor!.runner, context)
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
      { routedRemoteHost: participant.remoteHost ?? null, ...(context?.signal ? { signal: context.signal } : {}) },
    );
  }

  async acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
    verification?: "mesh" | "bridge",
  ): Promise<FabricControlAcceptance> {
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
        const ownership = this.participants.get(actor.id);
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
        const ownership = this.participants.get(actor.id);
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
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
        return { accepted: false, error: error instanceof Error ? error.message : String(error) };
      }
    }
    try {
      const actor = this.actorManager.status(command.targetId);
      const ownership = this.participants.get(actor.id);
      if (ownership && !ownership.local) {
        return { accepted: false, error: `Participant ${actor.id} is owned by ${ownership.ownerHostId}` };
      }
      const result = this.actorManager.tell(
        actor.id,
        message,
        command.data,
        { provenance, ...controlActorBindingOptions(command, from, actor.rootId, this.participants.get(from.id)?.rootId) },
      );
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
    const target = this.resolveActorTarget(id);
    const { actor, participant } = target;
    if (this.residency && (actor?.residency ?? participant?.residency) === "durable" &&
        !(actor && (this.actorManager.owns?.(actor.id) ?? participant?.local)) &&
        (actor?.rootId === this.residency.options.config.rootId || participant?.ownerHostId === this.residency.hostId)) {
      if (!kernelFenceAvailable()) {
        if (!participant) throw new Error(`Fabric actor ${actor!.id} is owned by another host`);
        return target;
      }
      await this.#withDurableRecovery(id, () => this.residency!.ensureActor(actor?.id ?? participant!.id));
      return this.resolveActorTarget(actor?.id ?? participant!.id);
    }
    return target;
  }

  resolveActorTarget(id: string): {
    actor?: FabricActorInfo;
    participant?: FabricParticipantInfo;
  } {
    let actor: FabricActorInfo | undefined;
    try {
      actor = this.actorManager.status(id);
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) throw error;
    }
    const participant = this.participants.get(actor?.id ?? id);
    if (!actor && (!participant || participant.kind !== "actor")) {
      throw new Error(`Unknown Fabric actor: ${id}`);
    }
    return {
      ...(actor ? { actor } : {}),
      ...(participant?.kind === "actor" ? { participant } : {}),
    };
  }

}
