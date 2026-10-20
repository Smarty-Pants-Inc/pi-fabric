import { waitForSandboxFinalAnswer } from "../../agents/completion-inbox.js";
import type { FabricControlPlane } from "../../topology/control-plane.js";
import type { ParticipantView } from "../../topology/participant-view.js";
import { presenceValue } from "../../topology/participant-view.js";
import type { FabricParticipantSource } from "../../topology/types.js";
import type { FabricTransport } from "../types.js";
import type { SandboxMesh, SandboxParticipant, SandboxTaskResult } from "./types.js";

interface ChildRoute {
  runId: string;
  ownerHostId: string;
  ownerIdentityId: string;
  ownerIncarnation: string;
}

const SANDBOX_CONTROL_PROTOCOL = "pi-fabric-sandbox-stop-v1";
const SANDBOX_STOP_TIMEOUT_MS = 45_000;

export interface SandboxFabricMeshOptions {
  transport: FabricTransport;
  participants: Pick<FabricParticipantSource, "get" | "list">;
  participantView: ParticipantView;
  control: Pick<FabricControlPlane, "request">;
  canSend: () => boolean;
}

/** Compose sandbox control with the activation's existing NATS carrier and directory. */
export const createSandboxFabricMesh = (options: SandboxFabricMeshOptions): SandboxMesh => {
  const { transport, participants, participantView, control, canSend } = options;
  if (transport.kind !== "nats") throw new Error("Sandbox control requires the existing NATS carrier");
  const routes = new Map<string, ChildRoute>();

  const resolveChild = (id: string, runId: string): SandboxParticipant | undefined => {
    const participant = participants.get(id, Date.now(), { fresh: true });
    if (!participant || participant.id !== id || participant.kind !== "root" || participant.interactive === false ||
      participant.placementRunId !== runId || !participant.ownerHostId || !participant.ownerIdentityId || !participant.ownerIncarnation) return undefined;
    let route;
    try { route = participantView.resolve(id); } catch { return undefined; }
    if (!route || !route.live || !route.capabilities.includes("followUp") ||
      route.ownerHostId !== participant.ownerHostId || route.ownerIdentityId !== participant.ownerIdentityId ||
      route.ownerIncarnation !== participant.ownerIncarnation) return undefined;
    routes.set(id, { runId, ownerHostId: route.ownerHostId, ownerIdentityId: route.ownerIdentityId, ownerIncarnation: route.ownerIncarnation });
    return { id, placementRunId: runId };
  };

  const resolveFromDirectory = (runId: string): SandboxParticipant | undefined => {
    try {
      for (const participant of participants.list({ kinds: ["root"], fresh: true })) {
        if (participant.placementRunId !== runId) continue;
        const resolved = resolveChild(participant.id, runId);
        if (resolved) return resolved;
      }
    } catch { /* The NATS directory is not ready yet; its shared watch will wake this wait. */ }
    return undefined;
  };

  const waitForKnownRecord = async (id: string, runId: string, signal: AbortSignal): Promise<SandboxParticipant | undefined> => {
    for (;;) {
      signal.throwIfAborted();
      const changed = participantView.whenChanged(id, signal);
      void changed.catch(() => undefined);
      const resolved = resolveChild(id, runId);
      if (resolved) return resolved;
      await changed;
    }
  };

  return {
    async waitForPresence(runId, { timeoutMs, signal }) {
      if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 90_000) throw new Error("Invalid sandbox presence timeout");
      const operationSignal = signal ?? AbortSignal.timeout(timeoutMs);
      const initial = resolveFromDirectory(runId);
      if (initial) return initial;
      const presence = transport.kv("presence");
      for await (const event of presence.watch<unknown>(">", operationSignal)) {
        operationSignal.throwIfAborted();
        if (event.type === "put") {
          const record = presenceValue(event.entry);
          if (record?.kind === "root" && record.placementRunId === runId) {
            const resolved = await waitForKnownRecord(record.id, runId, operationSignal);
            if (resolved) return resolved;
          }
        } else if (event.type === "ready") {
          const resolved = resolveFromDirectory(runId);
          if (resolved) return resolved;
        }
      }
      operationSignal.throwIfAborted();
      throw new Error("NATS presence watch ended before the sandbox child joined");
    },

    async deliverTask(childSession, text, signal) {
      if (!canSend()) throw new Error("Fabric NATS control plane is not ready for sandbox task delivery");
      const bound = routes.get(childSession);
      const participant = participants.get(childSession, Date.now(), { fresh: true });
      const route = participantView.resolve(childSession);
      if (!bound || !participant || participant.kind !== "root" || participant.placementRunId !== bound.runId ||
        !route?.live || route.ownerHostId !== bound.ownerHostId || route.ownerIdentityId !== bound.ownerIdentityId ||
        route.ownerIncarnation !== bound.ownerIncarnation || !route.capabilities.includes("followUp")) {
        throw new Error("Sandbox child presence or owner changed before task delivery");
      }
      await control.request(route.ownerHostId, childSession, "followUp", {
        ownerIncarnation: route.ownerIncarnation,
        message: text,
        triggerTurn: true,
        data: { sandboxControl: { protocol: SANDBOX_CONTROL_PROTOCOL, runId: bound.runId } },
      }, route.ownerIdentityId, { ...(signal ? { signal } : {}) });
    },

    async stop(childSession, runId) {
      const bound = routes.get(childSession);
      if (!bound || bound.runId !== runId) return { confirmed: false, reason: "Sandbox stop refused: child session is not bound to this run id" };
      if (!canSend()) return { confirmed: false, reason: "Sandbox stop refused: Fabric NATS control plane is not ready" };
      const participant = participants.get(childSession, Date.now(), { fresh: true });
      let route;
      try { route = participantView.resolve(childSession); } catch { return { confirmed: false, reason: "Sandbox stop refused: child route is unavailable" }; }
      if (!participant || participant.id !== childSession || participant.kind !== "root" || participant.interactive === false ||
        participant.placementRunId !== runId || participant.ownerHostId !== bound.ownerHostId ||
        participant.ownerIdentityId !== bound.ownerIdentityId || participant.ownerIncarnation !== bound.ownerIncarnation ||
        !route?.live || route.ownerHostId !== bound.ownerHostId || route.ownerIdentityId !== bound.ownerIdentityId ||
        route.ownerIncarnation !== bound.ownerIncarnation || !route.capabilities.includes("followUp")) {
        return { confirmed: false, reason: "Sandbox stop refused: run binding or current child owner changed" };
      }
      try {
        const receipt = await control.request(route.ownerHostId, childSession, "stop", {
          ownerIncarnation: route.ownerIncarnation,
          data: { sandboxControl: { protocol: SANDBOX_CONTROL_PROTOCOL, runId } },
        }, route.ownerIdentityId, { timeoutMs: SANDBOX_STOP_TIMEOUT_MS });
        if (receipt.acknowledged !== true) return { confirmed: false, reason: "Sandbox stop was not acknowledged by the bound child owner" };
        const current = participants.get(childSession, Date.now(), { fresh: true });
        const currentRoute = participantView.resolve(childSession);
        if (!current || current.kind !== "root" || current.placementRunId !== runId ||
          current.ownerHostId !== bound.ownerHostId || current.ownerIdentityId !== bound.ownerIdentityId ||
          current.ownerIncarnation !== bound.ownerIncarnation || !currentRoute?.live ||
          currentRoute.ownerHostId !== bound.ownerHostId || currentRoute.ownerIdentityId !== bound.ownerIdentityId ||
          currentRoute.ownerIncarnation !== bound.ownerIncarnation) {
          return { confirmed: false, reason: "Sandbox stop was acknowledged, but the bound child route changed before confirmation" };
        }
        routes.delete(childSession);
        return { confirmed: true };
      } catch (error) {
        return { confirmed: false, reason: error instanceof Error ? error.message : "Sandbox stop acknowledgement failed" };
      }
    },

    awaitResult(childSession, runId, deadline, signal): Promise<SandboxTaskResult> {
      const bound = routes.get(childSession);
      if (!bound || bound.runId !== runId) return Promise.reject(new Error("Sandbox result return address is not bound to this child"));
      const result = waitForSandboxFinalAnswer(childSession, runId, deadline, signal);
      void result.finally(() => {
        if (routes.get(childSession)?.runId === runId) routes.delete(childSession);
      }).catch(() => undefined);
      return result;
    },
  };
};
