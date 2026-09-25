import type { AgentManager } from "../agents/manager.js";
import type { ActorManager } from "../actors/manager.js";
import type { FabricActorInfo, FabricActorRunBinding } from "../actors/types.js";
import type { FabricAgentMessageResult, FabricMainAgentTarget } from "../main-agent.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { FabricInvocationContext } from "../protocol.js";
import type { FabricControlPlane, FabricControlCommand, FabricControlAcceptance } from "../topology/control-plane.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../topology/types.js";
import type { FabricAgentRunner } from "../config.js";

// A root whose lease lapsed this recently may still be live: its heartbeat can be late
// under mesh lock contention or a busy event loop (smarty-dev#447). A reply still goes to
// its owner host, which acknowledges it when alive; a gone host leaves the outcome unknown.
// ponytail: 5 min covers every lease flap seen in the fleet; a longer lapse reads as ended.
const LAPSED_ROOT_REPLY_WINDOW_MS = 5 * 60_000;
// Route messages using only the ownership, delivery, and binding ports needed here.
// Says why a target cannot be resolved; the prefix stays "Unknown <label>: <id>".
export const unknownParticipant = (
  participants: Pick<FabricParticipantSource, "lastKnown">,
  id: string,
  label = "Fabric participant",
): Error => {
  const known = participants.lastKnown?.(id);
  if (!known) {
    return new Error(
      `Unknown ${label}: ${id} (no record on this mesh root: the session has ended, ` +
        "has not joined yet, or uses another mesh root)",
    );
  }
  const when = Number.isFinite(known.lapsedMs)
    ? `its lease lapsed ${Math.round(known.lapsedMs / 1000)} s ago`
    : "its host is gone or was replaced";
  return new Error(`Unknown ${label}: ${id} (${when}, so the session has probably ended)`);
};

export class AgentMessageRouter {
  constructor(
    readonly manager: Pick<AgentManager, "status" | "steer" | "followUp" | "stop">,
    readonly actorManager: Pick<ActorManager, "identity" | "status" | "validateDirectMessage" | "tell" | "ask" | "stop" | "steerRemote" | "resolveBinding">,
    readonly mainAgent: Pick<FabricMainAgentTarget, "matches" | "local" | "id" | "deliverAgent">,
    readonly participants: Pick<FabricParticipantSource, "get" | "scheduleRefresh" | "writeStalled" | "lastKnown">,
    readonly control: Pick<FabricControlPlane, "request"> | undefined,
    readonly resolvePiRunBinding: (binding: FabricActorRunBinding, runner: FabricAgentRunner, context: FabricInvocationContext) => FabricActorRunBinding,
  ) {}
  #recentlyLapsedRoot(id: string): FabricParticipantInfo | undefined {
    // A write-stalled mesh explains the lapse, and delivery needs the mesh: report the stall.
    if (this.participants.writeStalled?.()) return undefined;
    const known = this.participants.lastKnown?.(id);
    if (!known || known.participant.kind !== "root" || known.lapsedMs > LAPSED_ROOT_REPLY_WINDOW_MS) return undefined;
    return known.participant;
  }

  async routeMessage(
    id: string,
    message: string,
    data: unknown,
    kind: "steer" | "followUp",
    context?: FabricInvocationContext,
    options: {
      from?: MeshIdentity;
      triggerTurn?: boolean;
      binding?: FabricActorRunBinding;
    } = {},
  ): Promise<FabricAgentMessageResult> {
    const isMain = this.mainAgent.matches(id);
    const remoteRoot = isMain ? undefined : this.participants.get(id) ?? this.#recentlyLapsedRoot(id);
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
          message,
          delivery: kind,
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
          ...(data === undefined ? {} : { data }),
        });
      }
      const participant = remoteRoot ?? this.participants.get(this.mainAgent.id) ??
        this.#recentlyLapsedRoot(this.mainAgent.id);
      if (!participant) {
        throw this.participants.writeStalled?.() ?? unknownParticipant(this.participants, this.mainAgent.id, "Fabric Main participant");
      }
      if (!participant.capabilities.includes(kind)) {
        throw new Error(`Fabric participant ${participant.id} does not support ${kind}`);
      }
      if (!this.control || participant.controlProtocol === "legacy") {
        return this.actorManager.steerRemote(participant.id, message, kind, data);
      }
      return this.control.request(
        participant.ownerHostId,
        participant.id,
        kind,
        {
          message,
          data,
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
        },
        participant.ownerIdentityId,
      );
    }

    // Local one-shot agent: forward between its turns via the worker's
    // steer.jsonl channel, preserving the child's accumulated context.
    try {
      const status = this.manager.status(id);
      context?.activity?.({ type: "entity", id, kind: "agent", name: status.name });
      const result =
        kind === "steer"
          ? this.manager.steer(id, message, data)
          : this.manager.followUp(id, message, data);
      return { queued: true, messageId: result.messageId, routed: "local" };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
    }

    // Persistent actors consume both delivery modes through their serial mailbox.
    this.actorManager.validateDirectMessage(message, data);
    let target: { actor?: FabricActorInfo; participant?: FabricParticipantInfo };
    try {
      target = this.resolveActorTarget(id);
    } catch (error) {
      if (error instanceof Error && /Unknown Fabric actor/.test(error.message)) {
        throw this.participants.writeStalled?.() ?? unknownParticipant(this.participants, id);
      }
      throw error;
    }
    const { actor, participant } = target;
    const localActor = Boolean(actor && (!participant || participant.local));
    const binding = options.binding && context && localActor
      ? this.resolvePiRunBinding(options.binding, actor!.runner, context)
      : options.binding;
    if (actor && localActor) {
      context?.activity?.({ type: "entity", id: actor.id, kind: "actor", name: actor.name });
      const result = this.actorManager.tell(actor.id, message, data, {
        ...(binding ? { overrides: binding } : {}),
      });
      return { queued: true, messageId: result.messageId, routed: "local" };
    }
    if (!participant) throw new Error(`Fabric actor ${actor!.id} has no live execution owner`);
    if (!participant.capabilities.includes(kind)) {
      throw new Error(`Fabric participant ${participant.id} does not support ${kind}`);
    }
    const sessionBinding = actor?.binding;
    const resolvedBinding = actor
      ? this.actorManager.resolveBinding(actor.id, binding)
      : binding;
    const needsBinding = Boolean(
      resolvedBinding?.model ||
        resolvedBinding?.thinking ||
        sessionBinding?.model ||
        sessionBinding?.thinking,
    );
    if (needsBinding && !participant.capabilities.includes("actor-bindings")) {
      throw new Error(`Fabric actor owner ${participant.ownerHostId} does not support session bindings`);
    }
    if (!this.control || participant.controlProtocol === "legacy") {
      if (needsBinding) {
        throw new Error(`Fabric actor owner ${participant.ownerHostId} has no binding control channel`);
      }
      return this.actorManager.steerRemote(participant.id, message, kind, data);
    }
    return this.control.request(
      participant.ownerHostId,
      participant.id,
      kind,
      {
        message,
        data,
        ...(typeof options.triggerTurn === "boolean"
          ? { triggerTurn: options.triggerTurn }
          : {}),
        ...(needsBinding && resolvedBinding ? { binding: resolvedBinding } : {}),
      },
      participant.ownerIdentityId,
    );
  }

  async acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
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
          command.binding !== undefined ? { binding: command.binding } : {},
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
      const result = this.mainAgent.deliverAgent({
        from,
        message,
        delivery: command.operation,
        ...(typeof command.triggerTurn === "boolean"
          ? { triggerTurn: command.triggerTurn }
          : {}),
        ...(command.data === undefined ? {} : { data: command.data }),
      });
      return { accepted: true, messageId: result.messageId };
    }
    try {
      this.manager.status(command.targetId);
      const result =
        command.operation === "steer"
          ? this.manager.steer(command.targetId, message, command.data)
          : this.manager.followUp(command.targetId, message, command.data);
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
        command.binding !== undefined ? { binding: command.binding } : {},
      );
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) {
        return { accepted: false, error: error instanceof Error ? error.message : String(error) };
      }
    }
    return { accepted: false, error: `Owner does not control Fabric participant ${command.targetId}` };
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
