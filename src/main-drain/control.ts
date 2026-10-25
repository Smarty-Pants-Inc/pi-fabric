import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { bindOrMatch, clearBinding, currentBinding, podBindingAllowed, provePodCaller, proveHostBinding, sameOperation,
  type MainDrainBinding, type MainDrainBindingRequest } from "./binding.js";
import { assertMainInboxOpen, closeMainInbox, mainInboxDrainGate, reopenMainInbox } from "./gate.js";
import { fenceNativeInput, nativeFenceHeld, observeNativeInput, releaseNativeInput, type NativeInputObservation } from "./native-port.js";
import { drainSlot, peekDrainSlot } from "./slot.js";
import type { MainDrainActorRow, MainDrainIngressProbe, MainDrainOp, MainDrainReply } from "./protocol.js";

/** What the drain control needs from the live Main runtime. Getters: a reload replaces each one. */
export interface MainDrainHost {
  rootId: string;
  sessionId: string;
  meshRoot: string;
  pi: ExtensionAPI;
  context(): ExtensionContext | undefined;
  main(): MainDrainMain | undefined;
  actors(): MainDrainActors | undefined;
  agents(): MainDrainAgents | undefined;
  /** The in-memory completion inbox's unconfirmed count (no residency); undefined when there is no inbox. */
  completions?(): number | undefined;
  /** Tests only: the Main's own environment and /proc. */
  environment?: NodeJS.ProcessEnv;
  procRoot?: string;
}
export interface MainDrainMain {
  drainSnapshot(): { held: number; handed: number; heldIds: string[]; handedIds: string[]; journalOpen: boolean; halted: boolean; reloading: boolean };
  /** deliverAgent's own admission gates, dry run. */
  probeAdmission(): boolean;
}
export interface MainDrainActors {
  listOwned(): ReadonlyArray<{ id: string; name: string; status: string; rootId?: string; residency?: string }>;
  drainQuiescence(id: string): string[];
  drainWorker(id: string): { runId: string; pid: number; startTime: string | undefined } | null | undefined;
  stop(id: string, beforeCommit: undefined, wait: boolean, path: "actors.stop", options: { explicit: true }): Promise<unknown>;
}
export interface MainDrainAgents {
  list(): ReadonlyArray<{ id: string; status: string }>;
  workerIdentity?(runId: string): { pid: number; startTime: string | undefined } | undefined;
}
/** The caller as the kernel reports it (SO_PEERCRED); undefined: not verifiable. */
export interface MainDrainPeer { pid: number; uid: number }

const TERMINAL_AGENT = new Set(["completed", "failed", "stopped", "timed_out"]);
const STOP_WAIT_MS = 10_000;
const residency = (value: unknown): MainDrainActorRow["residency"] =>
  value === undefined || value === "session" ? "session" : value === "durable" ? "durable" : "unknown";

/**
 * The ingress table (DRAIN-DESIGN.r1 §1.9, its 14 rows plus three). Each row's probe calls the
 * SAME gate function as live admission, so a dry run and a live attempt cannot disagree:
 * - fabric-gate: MainAgentController.probeAdmission(), which runs deliverAgent's own gates
 *   (#admissionGates), or for spawn/create/composer the assertMainInboxOpen they call;
 * - native-fence: Pi's own fence state (getInputState().inputsFenced, the field _assertInputsOpen checks);
 * - both: the gate AND the fence; hold: code-lead's route hold, observed by A, never probed here.
 * `refusal` says what the producer sees: visible (a typed FABRIC_MAIN_DRAINING or Pi refusal), retried
 * (the record stays and is delivered later), dropped, or unknown. "Visible refusal", not "exactly once",
 * for every producer that does not retry.
 */
export const MAIN_DRAIN_INGRESS: ReadonlyArray<{ path: string; row: string; mechanism: MainDrainIngressProbe["mechanism"];
  refusal: "visible" | "retried" | "dropped" | "unknown" | "uncovered-precondition" }> = [
  { path: "control.followUp-steer", row: "1", mechanism: "fabric-gate", refusal: "visible" },
  { path: "in-process.producer", row: "2", mechanism: "fabric-gate", refusal: "visible" },
  { path: "resident-host.delivery", row: "3", mechanism: "fabric-gate", refusal: "retried" },
  { path: "completion.journal", row: "4a", mechanism: "fabric-gate", refusal: "retried" },
  { path: "completion.inbox", row: "4b", mechanism: "native-fence", refusal: "retried" },
  { path: "follow-up-alarm", row: "5", mechanism: "native-fence", refusal: "dropped" },
  { path: "dashboard.composer", row: "6", mechanism: "both", refusal: "visible" },
  { path: "tui.keyboard", row: "7", mechanism: "native-fence", refusal: "unknown" },
  { path: "feed.herdr-prompt", row: "8", mechanism: "native-fence", refusal: "uncovered-precondition" },
  { path: "peer-steer-wake", row: "9", mechanism: "fabric-gate", refusal: "visible" },
  { path: "actor-halt-lift", row: "10", mechanism: "native-fence", refusal: "unknown" },
  { path: "legacy-fabric-steer", row: "11", mechanism: "fabric-gate", refusal: "dropped" },
  { path: "extension.sendMessage", row: "12", mechanism: "native-fence", refusal: "unknown" },
  { path: "session-replacement-reload", row: "13", mechanism: "both", refusal: "visible" },
  { path: "code-route", row: "14", mechanism: "hold", refusal: "retried" },
  { path: "rotation-carrier", row: "1b", mechanism: "fabric-gate", refusal: "retried" },
  { path: "agents.spawn", row: "s1", mechanism: "fabric-gate", refusal: "visible" },
  { path: "actors.create", row: "s2", mechanism: "fabric-gate", refusal: "visible" },
];

/** Dry-run verdicts. Fabric-gate paths through `mainAdmits` (deliverAgent's gates) or the gate function itself. */
export const probeIngress = (rootId: string, native: NativeInputObservation, mainAdmits: boolean | null): MainDrainIngressProbe[] => {
  const gateOpen = (): boolean => { try { assertMainInboxOpen(rootId); return true; } catch { return false; } };
  const nativeOpen = native.fenceApi && native.piInputsFenced !== null ? !native.piInputsFenced : null;
  return MAIN_DRAIN_INGRESS.map(({ path, mechanism }) => {
    if (mechanism === "hold") return { path, mechanism, admits: null, fabric: null };
    if (mechanism === "native-fence") return { path, mechanism, admits: nativeOpen, fabric: null };
    const fabric = path === "agents.spawn" || path === "actors.create" || path === "dashboard.composer" ? gateOpen() : mainAdmits;
    if (mechanism === "fabric-gate") return { path, mechanism, admits: fabric, fabric };
    // both: admitted only through BOTH gates; a refusal by either is a refusal. `fabric` keeps the Fabric half.
    return { path, mechanism, fabric,
      admits: fabric === false || nativeOpen === false ? false : fabric === true && nativeOpen === true ? true : null };
  });
};

/**
 * D3 seat drain (smarty-dev#15310, DRAIN-DESIGN.r1): the owner's drain of THIS Main, for the
 * local owner-only channel (server.ts) only. It never deletes or acknowledges a message.
 * - drain (bound; = begin + fence-native, idempotent per (operationId, generation)): close the Fabric
 *   gate (also spawn/create/composer), which still admits the drain's OWN children until the fence
 *   (r2 N1). Once Fabric holds nothing for Pi, no agent run is live, no completion is unconfirmed and Pi
 *   is drained and idle, take the native fence. If Fabric still holds items once fenced, release ONLY
 *   the native fence (§1.11 4d).
 * - stop-actors (bound; r2 N2): the LAST act. With the fence held and all else 0, and only if EVERY
 *   session actor is quiesced, owner-stop them all (wait=true; B3), recording each one's public spec.
 * - undo (same run, operationId AND generation, r2 N6 ii; = release): release the fence through the
 *   kept handle, reopen the gate, clear the record. The reply's probes are the canary. Only a LOST
 *   handle means restart required. Actors this operation stopped are not recreated here (stage 1): the
 *   undo reports actorsRestored:false, which the seam folds into admissionRestored.
 * - exit (pod binding, §1.13): when fully drained, accept a clean Main exit.
 * - status, probe: observe only. Durable actors are reported, never stopped.
 */
export class MainDrainControl {
  #exitAccepted = false;
  constructor(readonly host: MainDrainHost) {}

  /** Authorize, then run one op. Returns a refusal code instead of throwing. */
  async handle(op: MainDrainOp, request: MainDrainBindingRequest | null, peer: MainDrainPeer | undefined, uid: number): Promise<MainDrainReply | string> {
    const { rootId } = this.host;
    if (op === "drain" || op === "stop-actors" || op === "undo" || op === "exit") {
      if (!request) return "BINDING_REQUIRED";
      // An accepted exit is final: no undo can reopen admission in the shutdown grace, which would admit
      // input that the shutdown then cuts off after the exit reply already claimed the fence held.
      if (peekDrainSlot(rootId)?.exitingAt !== undefined) return "EXITING";
      const kind = request.podUid === null ? "host" : "pod";
      if (kind === "pod") {
        if (!podBindingAllowed(this.host.environment)) return "POD_BINDING_OUTSIDE_POD";
        const refusal = provePodCaller(peer, uid, this.host.procRoot);
        if (refusal) return refusal;
      }
      if (op === "exit" && kind !== "pod") return "EXIT_REQUIRES_POD_BINDING";
      let lock: { dev: string; ino: string } | null = null;
      if (kind === "host") {
        const proof = proveHostBinding(request, this.host.sessionId, peer, uid, this.host.procRoot);
        if (proof.refusal) return proof.refusal;
        lock = proof.lock;
      }
      if (op === "drain") {
        const fresh = currentBinding(rootId) === undefined;
        const bound = bindOrMatch(rootId, request, kind, lock);
        if (typeof bound === "string") return bound;
        if (fresh) this.#begin();
      } else {
        // Only the SAME operation, holding the SAME lock, may act on a standing record: another operation's fence is UNKNOWN.
        const existing: MainDrainBinding | undefined = currentBinding(rootId);
        if (existing && !sameOperation(existing, request, kind, lock)) return "DRAIN_BOUND_ELSEWHERE";
        if (op !== "undo" && !existing) return "NOT_BOUND";
      }
    }
    let undo: MainDrainReply["undo"] = null;
    let stopRefused: string | null = null;
    if (op === "drain") await this.#drain();
    if (op === "stop-actors") stopRefused = await this.#stopActors();
    if (op === "undo") undo = this.#undo();
    const reply = this.#reply(op, undo);
    reply.stopRefused = stopRefused;
    if (op === "exit") {
      reply.exitRefused = this.#exitRefusals(reply);
      reply.exiting = reply.exitRefused.length === 0;
      if (reply.exiting) { this.#exitAccepted = true; drainSlot(rootId).exitingAt = Date.now(); }
    }
    return reply;
  }

  /** Called by the server once the exit reply is flushed. */
  commitExit(): void {
    if (!this.#exitAccepted) return;
    const ctx = this.host.context();
    // Pi's orderly shutdown; the fence stays held, and nothing queued remains (checked above).
    // A short grace lets the exec'd CLI print its receipt line and exit 0 before the container stops.
    setTimeout(() => { try { ctx?.shutdown(); } catch { /* the context retired: the process is going anyway */ } }, 1_000);
  }

  /** The first bind: record the drain's own children (r2 N1) and start a fresh ledger (N8). */
  #begin(): void {
    const slot = drainSlot(this.host.rootId);
    slot.children = { runIds: new Set((this.#agentRuns() ?? []).map(run => run.id)), actorIds: new Set(this.#liveSessionActorIds()) };
  }

  #liveSessionActorIds(): string[] {
    return (this.#owned() ?? []).filter(actor => actor.residency === "session" && actor.status !== "stopped").map(actor => actor.id);
  }

  /** Fence held, Fabric empty, no live run, no unconfirmed completion, Pi drained and idle. */
  #allElseDrained(): boolean {
    const fabric = this.#mainSnapshot();
    const runs = this.#agentRuns();
    const native = observeNativeInput(this.host.pi, this.host.context(), this.host.rootId);
    let completions: number | undefined;
    try { completions = this.host.completions ? this.host.completions() ?? 0 : undefined; } catch { completions = undefined; }
    return fabric !== undefined && fabric.held === 0 && fabric.handed === 0 && runs !== undefined &&
      runs.every(run => TERMINAL_AGENT.has(run.status)) && completions === 0 &&
      native.fenceApi && native.drained === true && native.idle === true;
  }

  async #stopActors(): Promise<string | null> {
    const { rootId } = this.host;
    const native = observeNativeInput(this.host.pi, this.host.context(), rootId);
    if (!native.inputsFenced) return "NOT_FENCED";
    if (!this.#allElseDrained()) return "NOT_DRAINED";
    const actors = this.host.actors();
    const owned = this.#owned();
    if (!actors || !owned) return "ACTORS_UNKNOWN";
    const live = owned.filter(actor => actor.residency === "session" && actor.status !== "stopped");
    if (owned.some(actor => actor.residency !== "session" && actor.residency !== "durable")) return "ACTORS_UNKNOWN";
    // All or none: one actor that is not quiesced stops nothing (r2 N2, N9).
    if (live.some(actor => actor.quiescence === null || actor.quiescence.length > 0)) return "ACTORS_NOT_QUIESCED";
    const slot = drainSlot(rootId);
    const specs = new Map(actors.listOwned().map(actor => [actor.id, actor as unknown as Record<string, unknown>]));
    for (const actor of live) {
      // Re-check the predicate at the moment of the stop: a quiesced actor has nothing to drop.
      let reasons: string[];
      try { reasons = actors.drainQuiescence(actor.id); } catch { return "ACTORS_UNKNOWN"; }
      if (reasons.length) return "ACTORS_NOT_QUIESCED";
      (slot.stopped ??= []).push({ id: actor.id, spec: structuredClone(specs.get(actor.id) ?? { id: actor.id }), stoppedAt: Date.now() });
      slot.children?.actorIds.delete(actor.id);
      const stop = actors.stop(actor.id, undefined, true, "actors.stop", { explicit: true });
      await Promise.race([stop.catch(() => undefined), new Promise(resolve => setTimeout(resolve, STOP_WAIT_MS))]);
    }
    return null;
  }

  async #drain(): Promise<void> {
    const { rootId } = this.host;
    closeMainInbox(rootId);
    const slot = drainSlot(rootId);
    if (slot.children) slot.children.actorIds = new Set(this.#liveSessionActorIds());
    const fabric = this.#mainSnapshot();
    const fabricEmpty = fabric !== undefined && fabric.held === 0 && fabric.handed === 0;
    if (nativeFenceHeld(rootId)) {
      // Fenced, but Fabric still holds items for Pi: they cannot flush through a fence. Lift ONLY the fence.
      if (!fabricEmpty) releaseNativeInput(rootId);
      return;
    }
    if (this.#allElseDrained()) await fenceNativeInput(this.host.pi, this.host.context(), rootId);
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
    // A fence-capable host must READ open (a missing context or unreadable state is unknown, never open).
    const restartRequired = native.capability && !(native.fenceApi && native.piInputsFenced === false);
    // Stage 1 does not recreate a stopped actor: every one this operation stopped must be active again.
    const owned = this.#owned();
    const stopped = peekDrainSlot(rootId)?.stopped ?? [];
    const actorsRestored = owned !== undefined && stopped.every(entry => owned.some(actor => actor.id === entry.id && actor.status !== "stopped"));
    return { fenceReleased: held && !restartRequired, inboxReopened, restartRequired, actorsRestored };
  }

  #mainSnapshot(): ReturnType<MainDrainMain["drainSnapshot"]> | undefined {
    try { return this.host.main()?.drainSnapshot(); } catch { return undefined; }
  }

  #agentRuns(): Array<{ id: string; status: string; worker: { pid: number; startTime: string | null } | null }> | undefined {
    try {
      const agents = this.host.agents();
      if (!agents) return undefined;
      return agents.list().map(agent => {
        const worker = agents.workerIdentity?.(agent.id);
        return { id: agent.id, status: agent.status, worker: worker ? { pid: worker.pid, startTime: worker.startTime ?? null } : null };
      });
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
    const runs = this.#agentRuns();
    const binding = currentBinding(rootId);
    const native = observeNativeInput(this.host.pi, this.host.context(), rootId);
    let mainAdmits: boolean | null;
    try { mainAdmits = this.host.main()?.probeAdmission() ?? null; } catch { mainAdmits = null; }
    let completions: number | null;
    try { completions = this.host.completions ? this.host.completions() ?? 0 : null; } catch { completions = null; }
    return {
      v: 1, ok: true, op, error: null, sessionId, rootId, meshRoot, pid: process.pid,
      binding: binding ? { kind: binding.kind, run: binding.run, operationId: binding.operationId, generation: binding.generation,
        phase: binding.phase, podUid: binding.podUid, boundAt: binding.boundAt, lock: binding.lock } : null,
      inbox: { closed: gate !== undefined, closedAt: gate?.closedAt ?? null, held: fabric?.held ?? null, handed: fabric?.handed ?? null,
        heldIds: fabric?.heldIds ?? null, handedIds: fabric?.handedIds ?? null,
        journalOpen: fabric?.journalOpen ?? null, halted: fabric?.halted ?? null, reloading: fabric?.reloading ?? null },
      native,
      probes: probeIngress(rootId, native, mainAdmits),
      actors: { known: owned !== undefined, owned: owned ?? [] },
      agents: { known: runs !== undefined, running: (runs ?? []).filter(run => !TERMINAL_AGENT.has(run.status)).map(({ id, status }) => ({ id, status })),
        workers: (runs ?? []).flatMap(run => run.worker ? [{ id: run.id, status: run.status, ...run.worker }] : []) },
      completions: { inboxPending: completions },
      admitted: [...(peekDrainSlot(rootId)?.admitted ?? [])], ledger: [...(peekDrainSlot(rootId)?.ledger ?? [])],
      ledgerOverflow: peekDrainSlot(rootId)?.ledgerOverflow ?? 0,
      stopped: [...(peekDrainSlot(rootId)?.stopped ?? [])], stopRefused: null,
      undo, exiting: false, exitRefused: [],
    };
  }

  #exitRefusals(reply: MainDrainReply): string[] {
    const reasons: string[] = [];
    if (!this.host.context()) reasons.push("CONTEXT_GONE");
    if (!reply.inbox.closed) reasons.push("INBOX_OPEN");
    if (reply.inbox.held !== 0 || reply.inbox.handed !== 0) reasons.push("FABRIC_FOLLOWUPS_PENDING");
    if (!reply.native.inputsFenced || reply.probes.some(probe => probe.mechanism !== "hold" && probe.admits !== false)) reasons.push("NATIVE_NOT_FENCED");
    if (reply.native.drained !== true || reply.native.queued !== 0 || reply.native.inputAdmissionCount !== 0) reasons.push("NATIVE_NOT_DRAINED");
    if (reply.native.idle !== true) reasons.push("NATIVE_RUN_ACTIVE");
    if (!reply.actors.known || reply.actors.owned.some(actor => actor.residency !== "durable" && actor.status !== "stopped")) {
      reasons.push("SESSION_ACTORS_LIVE");
    }
    if (!reply.agents.known || reply.agents.running.length > 0) reasons.push("TASK_AGENTS_RUNNING");
    if (reply.completions.inboxPending !== 0 && reply.completions.inboxPending !== null) reasons.push("COMPLETIONS_PENDING");
    return reasons;
  }
}
