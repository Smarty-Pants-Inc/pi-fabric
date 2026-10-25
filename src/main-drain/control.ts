import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { closeMainInbox, mainInboxDrainGate, reopenMainInbox } from "./gate.js";
import { fenceNativeInput, observeNativeInput, releaseNativeInput } from "./native-port.js";
import type { MainDrainActorRow, MainDrainOp, MainDrainReply } from "./protocol.js";

/** What the drain control needs from the live Main runtime. Getters: a reload replaces each one. */
export interface MainDrainHost {
  rootId: string;
  sessionId: string;
  meshRoot: string;
  pi: ExtensionAPI;
  context(): ExtensionContext | undefined;
  main(): { drainSnapshot(): { held: number; handed: number; journalOpen: boolean; halted: boolean; reloading: boolean } } | undefined;
  actors(): MainDrainActors | undefined;
  agents(): { list(): ReadonlyArray<{ id: string; status: string }> } | undefined;
}
export interface MainDrainActors {
  listOwned(): ReadonlyArray<{ id: string; name: string; status: string; rootId?: string; residency?: string }>;
  stop(id: string, beforeCommit: undefined, wait: boolean, path: "actors.stop", options: { explicit: true }): Promise<unknown>;
}

const TERMINAL_ACTOR = new Set(["stopped", "failed"]);
const TERMINAL_AGENT = new Set(["completed", "failed", "stopped", "timed_out"]);
const code = (error: unknown, fallback: string): string => {
  const value = typeof error === "object" && error !== null ? (error as { code?: unknown }).code : undefined;
  return typeof value === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(value) ? value : fallback;
};
const residency = (value: unknown): MainDrainActorRow["residency"] =>
  value === undefined || value === "session" ? "session" : value === "durable" ? "durable" : "unknown";

/**
 * D3 seat drain (smarty-dev#15310): the owner's drain of THIS Main, run only for the local
 * owner-only channel (server.ts). It never deletes or acknowledges a message.
 * - drain:  close the Fabric inbox to new deliveries (retryable refusal) and owner-stop this root's
 *   own session-residency actors (explicit stop: it refuses an actor that still holds queued work).
 *   Once Fabric holds no follow-up for Pi, take the native input fence. Idempotent.
 * - status: observe only.
 * - exit:   when fully drained, accept a clean Main exit (raw Pod resume-drain); otherwise refuse with codes.
 * - reopen: release the fence this process took and reopen the inbox (a failed drain, same process).
 * Durable actors are reported, never stopped.
 */
export class MainDrainControl {
  #exitAccepted = false;
  constructor(readonly host: MainDrainHost) {}

  async handle(op: MainDrainOp): Promise<MainDrainReply> {
    const { rootId } = this.host;
    const refused: Array<{ id: string; code: string }> = [];
    if (op === "drain") {
      closeMainInbox(rootId);
      await this.#stopSessionActors(refused);
      const fabric = this.#mainSnapshot();
      if (fabric && fabric.held === 0 && fabric.handed === 0) await fenceNativeInput(this.host.pi, this.host.context(), rootId);
    } else if (op === "reopen") {
      releaseNativeInput(rootId);
      reopenMainInbox(rootId);
    }
    const reply = this.#reply(op, refused);
    if (op === "exit") {
      reply.exitRefused = this.#exitRefusals(reply);
      reply.exiting = reply.exitRefused.length === 0;
      if (reply.exiting) this.#exitAccepted = true;
    }
    return reply;
  }

  /** Called by the server once the exit reply is flushed. */
  commitExit(): void {
    if (!this.#exitAccepted) return;
    const ctx = this.host.context();
    // Pi's orderly shutdown: the held fence stays held; nothing queued remains (checked above).
    setTimeout(() => { try { ctx?.shutdown(); } catch { /* the context retired: the process is going anyway */ } }, 100);
  }

  #mainSnapshot(): ReturnType<NonNullable<ReturnType<MainDrainHost["main"]>>["drainSnapshot"]> | undefined {
    try { return this.host.main()?.drainSnapshot(); } catch { return undefined; }
  }

  #owned(): MainDrainActorRow[] | undefined {
    const actors = this.host.actors();
    if (!actors) return undefined;
    try {
      return actors.listOwned().filter(actor => actor.rootId === this.host.rootId)
        .map(actor => ({ id: actor.id, name: actor.name, status: actor.status, residency: residency(actor.residency) }));
    } catch { return undefined; }
  }

  async #stopSessionActors(refused: Array<{ id: string; code: string }>): Promise<void> {
    const actors = this.host.actors();
    for (const actor of this.#owned() ?? []) {
      if (actor.residency !== "session" || TERMINAL_ACTOR.has(actor.status)) continue;
      try { await actors!.stop(actor.id, undefined, false, "actors.stop", { explicit: true }); }
      catch (error) { refused.push({ id: actor.id, code: code(error, "STOP_REFUSED") }); }
    }
  }

  #reply(op: MainDrainOp, stopRefused: Array<{ id: string; code: string }>): MainDrainReply {
    const { rootId, sessionId, meshRoot } = this.host;
    const gate = mainInboxDrainGate(rootId);
    const fabric = this.#mainSnapshot();
    const owned = this.#owned();
    let running: Array<{ id: string; status: string }> | undefined;
    try {
      const agents = this.host.agents();
      running = agents?.list().filter(agent => !TERMINAL_AGENT.has(agent.status)).map(agent => ({ id: agent.id, status: agent.status }));
    } catch { running = undefined; }
    return {
      v: 1, ok: true, op, error: null, sessionId, rootId, meshRoot, pid: process.pid,
      inbox: { closed: gate !== undefined, closedAt: gate?.closedAt ?? null, held: fabric?.held ?? null, handed: fabric?.handed ?? null,
        journalOpen: fabric?.journalOpen ?? null, halted: fabric?.halted ?? null, reloading: fabric?.reloading ?? null },
      native: observeNativeInput(this.host.pi, this.host.context(), rootId),
      actors: { known: owned !== undefined, owned: owned ?? [], stopRefused },
      agents: { known: running !== undefined, running: running ?? [] },
      exiting: false, exitRefused: [],
    };
  }

  #exitRefusals(reply: MainDrainReply): string[] {
    const reasons: string[] = [];
    if (!this.host.context()) reasons.push("CONTEXT_GONE");
    if (!reply.inbox.closed) reasons.push("INBOX_OPEN");
    if (reply.inbox.held !== 0 || reply.inbox.handed !== 0) reasons.push("FABRIC_FOLLOWUPS_PENDING");
    if (!reply.native.inputsFenced) reasons.push("NATIVE_NOT_FENCED");
    if (reply.native.drained !== true || reply.native.queued !== 0 || reply.native.inputAdmissionCount !== 0) reasons.push("NATIVE_NOT_DRAINED");
    if (reply.native.idle !== true) reasons.push("NATIVE_RUN_ACTIVE");
    if (!reply.actors.known || reply.actors.owned.some(actor => actor.residency !== "durable" && !TERMINAL_ACTOR.has(actor.status))) {
      reasons.push("SESSION_ACTORS_LIVE");
    }
    if (!reply.agents.known || reply.agents.running.length > 0) reasons.push("TASK_AGENTS_RUNNING");
    return reasons;
  }
}
