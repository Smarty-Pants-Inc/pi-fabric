import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { bindOrMatch, clearBinding, currentBinding, podBindingAllowed, proveHostBinding, type MainDrainBinding,
  type MainDrainBindingRequest } from "./binding.js";
import { assertMainInboxOpen, closeMainInbox, mainInboxDrainGate, reopenMainInbox } from "./gate.js";
import { fenceNativeInput, nativeFenceHeld, observeNativeInput, releaseNativeInput, type NativeInputObservation } from "./native-port.js";
import type { MainDrainActorRow, MainDrainIngressProbe, MainDrainOp, MainDrainReply } from "./protocol.js";

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
  /** Tests only: the Main's own environment and /proc. */
  environment?: NodeJS.ProcessEnv;
  procRoot?: string;
}
export interface MainDrainActors {
  listOwned(): ReadonlyArray<{ id: string; name: string; status: string; rootId?: string; residency?: string }>;
  drainQuiescence(id: string): string[];
  drainWorker(id: string): { runId: string; pid: number; startTime: string | undefined } | null | undefined;
  stop(id: string, beforeCommit: undefined, wait: boolean, path: "actors.stop", options: { explicit: true }): Promise<unknown>;
}
/** The caller as the kernel reports it (SO_PEERCRED) and its /proc start time; undefined: not verifiable. */
export interface MainDrainPeer { pid: number; uid: number }

const TERMINAL_AGENT = new Set(["completed", "failed", "stopped", "timed_out"]);
const STOP_WAIT_MS = 10_000;
const code = (error: unknown, fallback: string): string => {
  const value = typeof error === "object" && error !== null ? (error as { code?: unknown }).code : undefined;
  return typeof value === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(value) ? value : fallback;
};
const residency = (value: unknown): MainDrainActorRow["residency"] =>
  value === undefined || value === "session" ? "session" : value === "durable" ? "durable" : "unknown";

/**
 * The ingress table (challenge r1 M7). Each row's probe calls the SAME gate function that live
 * admission calls, so a dry run and a live attempt cannot disagree. Fabric-gate rows run
 * `assertMainInboxOpen` (main-agent.ts deliverAgent / deliverUser / receiveInboxItem,
 * agents/manager.ts spawn, actors/manager.ts create). Native-fence rows read Pi's own fence state:
 * user typing, Herdr feed prompts and the user input that lifts an actor halt all enter Pi's native
 * admission, which refuses once it is fenced.
 */
export const MAIN_DRAIN_INGRESS: ReadonlyArray<{ path: string; mechanism: MainDrainIngressProbe["mechanism"] }> = [
  { path: "fabric-inbox.deliverAgent", mechanism: "fabric-gate" },         // peer Mains, factory events, control plane, bridge
  { path: "fabric-inbox.peer-steer-wake", mechanism: "fabric-gate" },      // gate precedes the halted-Main wake
  { path: "fabric-inbox.resident-delivery", mechanism: "fabric-gate" },    // residency client deliveries (record kept on refusal)
  { path: "fabric-inbox.agent-completion-journal", mechanism: "fabric-gate" },
  { path: "fabric-inbox.rotation-carrier", mechanism: "fabric-gate" },     // receiveInboxItem
  { path: "fabric-inbox.legacy-steer-relay", mechanism: "fabric-gate" },   // best-effort legacy fabric.steer (sender is not told)
  { path: "dashboard.deliverUser", mechanism: "fabric-gate" },
  { path: "agents.spawn", mechanism: "fabric-gate" },
  { path: "actors.create", mechanism: "fabric-gate" },
  { path: "native.user-input", mechanism: "native-fence" },                // TUI/RPC input, incl. the input that lifts an actor halt
  { path: "native.herdr-feed-prompt", mechanism: "native-fence" },         // org feed: herdr agent prompt into the pane
  { path: "native.agent-completion-inbox", mechanism: "native-fence" },    // completion-inbox pi.sendMessage
  { path: "native.follow-up-alarm", mechanism: "native-fence" },           // pi.sendMessage steer alarm
  { path: "native.actor-output", mechanism: "native-fence" },              // deliverActorToMain pi.sendMessage
];

export const probeIngress = (rootId: string, native: NativeInputObservation): MainDrainIngressProbe[] =>
  MAIN_DRAIN_INGRESS.map(({ path, mechanism }) => {
    // Pi's own fence state, whoever holds it: a fence this process lost still refuses input.
    if (mechanism === "native-fence") return { path, mechanism, admits: native.fenceApi && native.piInputsFenced !== null ? !native.piInputsFenced : null };
    try { assertMainInboxOpen(rootId); return { path, mechanism, admits: true }; } catch { return { path, mechanism, admits: false }; }
  });

/**
 * D3 seat drain (smarty-dev#15310, challenge r1): the owner's drain of THIS Main, for the local
 * owner-only channel (server.ts) only. It never deletes or acknowledges a message.
 * - drain (bound): close the Fabric gates (inbox, dashboard, spawn, create). Once Fabric holds nothing
 *   for Pi, take the native fence. Once fenced with no running agent run, owner-stop each session actor
 *   that is QUIESCED (B3), waiting for its run to end. Idempotent for the same binding.
 * - undo (same binding, B1): release this process's fence, reopen the gates, clear the binding; the
 *   reply's probes are the canary. After a reload the old fence handle cannot reopen: restartRequired.
 * - exit (pod binding, M9): when fully drained, accept a clean Main exit.
 * - status, probe: observe only.
 * Durable actors are reported, never stopped.
 */
export class MainDrainControl {
  #exitAccepted = false;
  constructor(readonly host: MainDrainHost) {}

  /** Authorize, then run one op. Returns a refusal code instead of throwing. */
  async handle(op: MainDrainOp, request: MainDrainBindingRequest | null, peer: MainDrainPeer | undefined, uid: number): Promise<MainDrainReply | string> {
    const { rootId } = this.host;
    let binding: MainDrainBinding | undefined;
    if (op === "drain" || op === "undo" || op === "exit") {
      if (!request) return "BINDING_REQUIRED";
      const kind = request.podUid === null ? "host" : "pod";
      if (kind === "pod" && !podBindingAllowed(this.host.environment)) return "POD_BINDING_OUTSIDE_POD";
      if (op === "exit" && kind !== "pod") return "EXIT_REQUIRES_POD_BINDING";
      if (kind === "host") {
        const proof = proveHostBinding(request, this.host.sessionId, peer, uid, this.host.procRoot);
        if (proof.refusal) return proof.refusal;
      }
      if (op === "drain") {
        const bound = bindOrMatch(rootId, request, kind);
        if (typeof bound === "string") return bound;
        binding = bound;
      } else {
        const existing = currentBinding(rootId);
        if (existing && (existing.run !== request.run || existing.operationId !== request.operationId || existing.kind !== kind)) {
          return "DRAIN_BOUND_ELSEWHERE";
        }
        binding = existing;
        if (op === "exit" && !binding) return "EXIT_WITHOUT_DRAIN";
      }
    }
    let undo: MainDrainReply["undo"] = null;
    if (op === "drain") await this.#drain();
    if (op === "undo") undo = this.#undo();
    const reply = this.#reply(op, undo);
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
    // Pi's orderly shutdown; the fence stays held, and nothing queued remains (checked above).
    setTimeout(() => { try { ctx?.shutdown(); } catch { /* the context retired: the process is going anyway */ } }, 100);
  }

  async #drain(): Promise<void> {
    const { rootId } = this.host;
    closeMainInbox(rootId);
    const fabric = this.#mainSnapshot();
    if (fabric && fabric.held === 0 && fabric.handed === 0 && !nativeFenceHeld(rootId)) {
      await fenceNativeInput(this.host.pi, this.host.context(), rootId);
    }
    // Stop actors last, only once input is fenced and no agent run is live: a refused drain then stops nothing.
    const native = observeNativeInput(this.host.pi, this.host.context(), rootId);
    const running = this.#runningAgents();
    if (!native.inputsFenced || running === undefined || running.length > 0) return;
    const actors = this.host.actors();
    for (const actor of this.#owned() ?? []) {
      if (actor.residency !== "session" || actor.status === "stopped" || !actor.quiescence || actor.quiescence.length) continue;
      const stop = actors!.stop(actor.id, undefined, true, "actors.stop", { explicit: true });
      await Promise.race([stop.catch(() => undefined), new Promise(resolve => setTimeout(resolve, STOP_WAIT_MS))]);
    }
  }

  #undo(): NonNullable<MainDrainReply["undo"]> {
    const { rootId } = this.host;
    const held = nativeFenceHeld(rootId);
    releaseNativeInput(rootId);
    const inboxReopened = reopenMainInbox(rootId) || mainInboxDrainGate(rootId) === undefined;
    clearBinding(rootId);
    // The process-global handle survives a reload, and Pi's original release() still reopens its fence.
    // Only a LOST handle (Pi still fenced, nothing held here) needs a restart.
    const native = observeNativeInput(this.host.pi, this.host.context(), rootId);
    const restartRequired = native.fenceApi && native.piInputsFenced !== false;
    return { fenceReleased: held && !restartRequired, inboxReopened, restartRequired };
  }

  #mainSnapshot(): ReturnType<NonNullable<ReturnType<MainDrainHost["main"]>>["drainSnapshot"]> | undefined {
    try { return this.host.main()?.drainSnapshot(); } catch { return undefined; }
  }

  #runningAgents(): Array<{ id: string; status: string }> | undefined {
    try {
      return this.host.agents()?.list().filter(agent => !TERMINAL_AGENT.has(agent.status)).map(agent => ({ id: agent.id, status: agent.status }));
    } catch { return undefined; }
  }

  #owned(): MainDrainActorRow[] | undefined {
    const actors = this.host.actors();
    if (!actors) return undefined;
    try {
      return actors.listOwned().filter(actor => actor.rootId === this.host.rootId).map(actor => {
        const kind = residency(actor.residency);
        let quiescence: string[] | null = null;
        let worker: MainDrainActorRow["worker"] = "unknown";
        try { quiescence = actors.drainQuiescence(actor.id); } catch { quiescence = null; }
        try {
          const record = actors.drainWorker(actor.id);
          worker = record === null ? null : record === undefined ? "unknown" : { runId: record.runId, pid: record.pid, startTime: record.startTime ?? null };
        } catch { worker = "unknown"; }
        const disposition = kind === "durable" ? "durable-blocks-drain" : kind === "unknown" ? "unknown-residency"
          : actor.status === "stopped" ? "stopped-for-good" : quiescence === null ? "unknown-quiescence"
            : quiescence.length ? `waiting:${quiescence.join(",")}` : "quiesced";
        return { id: actor.id, name: actor.name, status: actor.status, residency: kind, quiescence, worker, disposition };
      });
    } catch { return undefined; }
  }

  #reply(op: MainDrainOp, undo: MainDrainReply["undo"]): MainDrainReply {
    const { rootId, sessionId, meshRoot } = this.host;
    const gate = mainInboxDrainGate(rootId);
    const fabric = this.#mainSnapshot();
    const owned = this.#owned();
    const running = this.#runningAgents();
    const binding = currentBinding(rootId);
    const native = observeNativeInput(this.host.pi, this.host.context(), rootId);
    return {
      v: 1, ok: true, op, error: null, sessionId, rootId, meshRoot, pid: process.pid,
      binding: binding ? { kind: binding.kind, run: binding.run, operationId: binding.operationId, phase: binding.phase,
        podUid: binding.podUid, boundAt: binding.boundAt, fenceGeneration: binding.fenceGeneration } : null,
      inbox: { closed: gate !== undefined, closedAt: gate?.closedAt ?? null, held: fabric?.held ?? null, handed: fabric?.handed ?? null,
        journalOpen: fabric?.journalOpen ?? null, halted: fabric?.halted ?? null, reloading: fabric?.reloading ?? null },
      native,
      probes: probeIngress(rootId, native),
      actors: { known: owned !== undefined, owned: owned ?? [] },
      agents: { known: running !== undefined, running: running ?? [] },
      undo, exiting: false, exitRefused: [],
    };
  }

  #exitRefusals(reply: MainDrainReply): string[] {
    const reasons: string[] = [];
    if (!this.host.context()) reasons.push("CONTEXT_GONE");
    if (!reply.inbox.closed) reasons.push("INBOX_OPEN");
    if (reply.inbox.held !== 0 || reply.inbox.handed !== 0) reasons.push("FABRIC_FOLLOWUPS_PENDING");
    if (!reply.native.inputsFenced || reply.probes.some(probe => probe.admits !== false)) reasons.push("NATIVE_NOT_FENCED");
    if (reply.native.drained !== true || reply.native.queued !== 0 || reply.native.inputAdmissionCount !== 0) reasons.push("NATIVE_NOT_DRAINED");
    if (reply.native.idle !== true) reasons.push("NATIVE_RUN_ACTIVE");
    if (!reply.actors.known || reply.actors.owned.some(actor => actor.residency !== "durable" && actor.status !== "stopped")) {
      reasons.push("SESSION_ACTORS_LIVE");
    }
    if (!reply.agents.known || reply.agents.running.length > 0) reasons.push("TASK_AGENTS_RUNNING");
    return reasons;
  }
}
