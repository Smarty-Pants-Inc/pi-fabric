import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { bindOrMatch, clearBinding, currentBinding, podBindingAllowed, procWorkerExited, provePodCaller, proveHostBinding, sameOperation,
  type MainDrainBinding, type MainDrainBindingRequest } from "./binding.js";
import { assertMainInboxOpen, closeMainInbox, mainInboxDrainGate, reopenMainInbox } from "./gate.js";
import { fenceNativeInput, nativeFenceHeld, observeNativeInput, releaseNativeInput, type NativeInputObservation } from "./native-port.js";
import { drainSlot, peekDrainSlot } from "./slot.js";
import type { MainDrainActorRow, MainDrainIngressProbe, MainDrainOp, MainDrainReply } from "./protocol.js";
import type { FabricActorRequest } from "../actors/types.js";
import type { FabricThinking } from "../thinking.js";
import { actorLiveBinding, actorRestoreRequest, assertCutoverRestore, readDrainActors, sameActorConfiguration, sameActorSpec,
  saveDrainActor, type DrainActorSpec } from "./actor-restore.js";

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
  /** The owner incarnation this Main's presence records carry (ParticipantDirectory.ownerIncarnation). */
  ownerIncarnation?(): string | undefined;
  /** Tests only: the Main's own environment and /proc, and the per-actor stop bound. */
  environment?: NodeJS.ProcessEnv;
  procRoot?: string;
  stopWaitMs?: number;
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
  stop(id: string, beforeCommit: ((id: string) => void) | undefined, wait: boolean, path: "actors.stop", options: { explicit: true }): Promise<unknown>;
  drainSpec?(id: string): Record<string, unknown>;
  create?(request: FabricActorRequest): Promise<{ id: string }>;
  setModel?(id: string, model: string | undefined, scope: "session", beforeCommit: undefined, modelReason?: string): Promise<unknown>;
  setThinking?(id: string, thinking: FabricThinking | undefined, scope: "session"): Promise<unknown>;
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
 * - both: the gate AND the fence; hold: the code route's hold, observed by A, never probed here.
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
 *   kept handle, reopen the gate, recreate saved actors through the ordinary owner create path, then
 *   clear the binding only after exact spec/live-binding readback. The reply's probes are the canary.
 * - restore-actors: after activation, the new Main performs the same recreation from the RUN's saved
 *   rows without lifting a fence. Kubernetes drain pattern: cordon, evict within the quiescence budget,
 *   wait for termination with grace, uncordon on failure. Durable actors are the disruption budget.
 * - exit (pod binding, §1.13): when fully drained, accept a clean Main exit.
 * - status, probe: observe only. Durable actors are reported, never stopped.
 */
export class MainDrainControl {
  #exitAccepted = false;
  constructor(readonly host: MainDrainHost) {}

  /** Authorize, then run one op. Returns a refusal code instead of throwing. */
  async handle(op: MainDrainOp, request: MainDrainBindingRequest | null, peer: MainDrainPeer | undefined, uid: number): Promise<MainDrainReply | string> {
    const { rootId } = this.host;
    if (op === "drain" || op === "stop-actors" || op === "undo" || op === "restore-actors" || op === "exit") {
      if (!request) return "BINDING_REQUIRED";
      if ((op === "restore-actors") !== (request.phase === "drain-restore")) return "PHASE_MISMATCH";
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
        if (op !== "undo" && op !== "restore-actors" && !existing) return "NOT_BOUND";
        if (op === "restore-actors" && (kind !== "host" || existing || mainInboxDrainGate(rootId) || nativeFenceHeld(rootId) ||
          observeNativeInput(this.host.pi, this.host.context(), rootId).piInputsFenced !== false)) return "RESTORE_REQUIRES_NEW_MAIN";
      }
    }
    let undo: MainDrainReply["undo"] = null;
    let stopRefused: string | null = null;
    if (op === "drain") await this.#drain();
    if (op === "stop-actors") stopRefused = await this.#stopActors(request!);
    if (op === "undo") undo = await this.#undo(request!);
    if (op === "restore-actors") {
      try { assertCutoverRestore(request!, rootId); } catch { return "RESTORE_NOT_AUTHORIZED"; }
      const actorsRestored = await this.#restoreActors(request!);
      undo = { fenceReleased: false, inboxReopened: mainInboxDrainGate(rootId) === undefined, restartRequired: false, actorsRestored };
    }
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
    slot.stopped = []; slot.stopAttempted = []; slot.restored = [];
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

  async #stopActors(request: MainDrainBindingRequest): Promise<string | null> {
    const { rootId } = this.host;
    if (peekDrainSlot(rootId)?.pendingStop) return "ACTOR_STOP_PENDING";
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
    // Never stop something we cannot recreate. Save ALL complete rows + resolved live pins before the first effect.
    if (live.length && (!actors.create || !actors.drainSpec)) return "ACTORS_UNRESTORABLE";
    const specs = new Map<string, Record<string, unknown>>();
    try {
      for (const actor of live) {
        const spec = actors.drainSpec!(actor.id);
        actorRestoreRequest(spec);
        const binding = actorLiveBinding(spec);
        if (spec.rootId !== rootId || ((binding.model !== spec.model || binding.modelReason !== spec.modelReason) && !actors.setModel) ||
          (binding.thinking !== spec.thinking && !actors.setThinking)) return "ACTORS_UNRESTORABLE";
        saveDrainActor(request, rootId, { id: actor.id, spec, stoppedAt: Date.now() });
        specs.set(actor.id, spec);
      }
    } catch { return "ACTORS_UNRESTORABLE"; }
    for (const actor of live) {
      // Re-check the predicate at the moment of the stop: a quiesced actor has nothing to drop.
      let reasons: string[];
      try { reasons = actors.drainQuiescence(actor.id); } catch { return "ACTORS_UNKNOWN"; }
      if (reasons.length) return "ACTORS_NOT_QUIESCED";
      // Attempted first (an undo must find it active again, stopped or not); stopped only once proven (#1607 P1).
      const entry = { id: actor.id, spec: structuredClone(specs.get(actor.id)!), stoppedAt: Date.now() };
      if (!(slot.stopAttempted ??= []).some(known => known.id === actor.id)) slot.stopAttempted.push(entry);
      slot.children?.actorIds.delete(actor.id);
      const wait = this.host.stopWaitMs ?? STOP_WAIT_MS;
      const deadline = Date.now() + wait;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const pending = actors.stop(actor.id, () => {
        if (actors.drainQuiescence(actor.id).length) throw new Error("ACTORS_NOT_QUIESCED");
      }, true, "actors.stop", { explicit: true });
      slot.pendingStop = pending;
      void pending.finally(() => { if (slot.pendingStop === pending) delete slot.pendingStop; }).catch(() => undefined);
      const outcome = await Promise.race([
        pending.then(() => "stopped" as const, () => "failed" as const),
        new Promise<"timeout">(resolve => { timer = setTimeout(() => resolve("timeout"), wait); }),
      ]);
      if (timer) clearTimeout(timer);
      // A failed or slow stop ends this request: no further actor is stopped, and the caller refuses and undoes.
      if (outcome === "failed") return "ACTOR_STOP_FAILED";
      if (outcome === "timeout") return "ACTOR_STOP_TIMEOUT";
      if (!(await this.#workerGone(actor.id, deadline))) return "ACTOR_STOP_UNPROVEN";
      entry.stoppedAt = Date.now();
      (slot.stopped ??= []).push(entry);
    }
    return null;
  }

  /**
   * The worker-death proof: the actor's last worker is gone (never launched, or its pid now has another start time
   * or no /proc entry), polled until `deadline`. An unknown worker is never proven gone.
   */
  async #workerGone(id: string, deadline: number): Promise<boolean> {
    const procRoot = this.host.procRoot ?? "/proc";
    for (;;) {
      let worker: ReturnType<MainDrainActors["drainWorker"]>;
      try { worker = this.host.actors()?.drainWorker(id); } catch { worker = undefined; }
      if (worker === null) return true;
      if (worker !== undefined && worker.startTime !== undefined) {
        if (procWorkerExited(worker.pid, worker.startTime, procRoot) === true) return true;
      }
      if (Date.now() >= deadline) return false;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
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

  async #undo(request: MainDrainBindingRequest): Promise<NonNullable<MainDrainReply["undo"]>> {
    const { rootId } = this.host;
    const held = nativeFenceHeld(rootId);
    releaseNativeInput(rootId);
    const inboxReopened = reopenMainInbox(rootId) || mainInboxDrainGate(rootId) === undefined;
    // The process-global handle survives a reload, and Pi's original release() still reopens its fence.
    // Only a LOST handle (Pi still fenced, nothing held here) needs a restart.
    const native = observeNativeInput(this.host.pi, this.host.context(), rootId);
    // A fence-capable host must READ open (a missing context or unreadable state is unknown, never open).
    const restartRequired = native.capability && !(native.fenceApi && native.piInputsFenced === false);
    const actorsRestored = await this.#restoreActors(request);
    if (actorsRestored) clearBinding(rootId);
    return { fenceReleased: held && !restartRequired, inboxReopened, restartRequired, actorsRestored };
  }

  /** Re-create, never revive an explicitly stopped actor or copy its authority around the owner path. */
  async #restoreActors(request: MainDrainBindingRequest): Promise<boolean> {
    const slot = drainSlot(this.host.rootId);
    // A timed-out stop may still commit. Never report an actor restored ahead of that late stop.
    if (slot.pendingStop) return false;
    let saved: DrainActorSpec[];
    try {
      saved = [...new Map([...readDrainActors(request, this.host.rootId), ...(slot.stopAttempted ?? []),
        ...(slot.stopped ?? [])].map(entry => [entry.id, entry])).values()];
    } catch { return false; }
    if (!saved.length) return true;
    const actors = this.host.actors();
    if (!actors) return false;
    let restored = true;
    for (const entry of saved) {
      try {
        const rows = actors.listOwned().filter(row => row.rootId === this.host.rootId);
        let active = rows.find(row => row.id === entry.id && row.status !== "stopped") ??
          rows.find(row => row.name === entry.spec.name && row.status !== "stopped");
        if (!actors.drainSpec || !actors.create) throw new Error("ACTORS_UNRESTORABLE");
        if (active && !sameActorConfiguration(entry.spec, actors.drainSpec(active.id))) throw new Error("ACTOR_SPEC_CONFLICT");
        if (!active) {
          const original = rows.find(row => row.id === entry.id);
          if (!original || original.status !== "stopped" || (request.phase !== "drain-restore" &&
            !(await this.#workerGone(entry.id, Date.now())))) throw new Error("ACTOR_STOP_UNPROVEN");
          const created = await actors.create(actorRestoreRequest(entry.spec));
          active = actors.listOwned().find(row => row.id === created.id && row.rootId === this.host.rootId && row.status !== "stopped");
          if (!active) throw new Error("ACTOR_CREATE_UNPROVEN");
        }
        const binding = actorLiveBinding(entry.spec);
        const current = actorLiveBinding(actors.drainSpec(active.id));
        if (binding.model !== current.model || binding.modelReason !== current.modelReason) {
          if (!actors.setModel) throw new Error("ACTOR_BINDING_UNRESTORABLE");
          await actors.setModel(active.id, binding.model, "session", undefined, binding.modelReason);
        }
        if (binding.thinking !== current.thinking) {
          if (!actors.setThinking) throw new Error("ACTOR_BINDING_UNRESTORABLE");
          await actors.setThinking(active.id, binding.thinking, "session");
        }
        if (!sameActorSpec(entry.spec, actors.drainSpec(active.id))) throw new Error("ACTOR_RESTORE_MISMATCH");
        const result = { id: entry.id, restoredId: active.id };
        slot.restored ??= [];
        if (!slot.restored.some(row => row.id === entry.id)) slot.restored.push(result);
      } catch { restored = false; }
    }
    return restored;
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
          : actor.status === "stopped" ? peekDrainSlot(this.host.rootId)?.stopAttempted?.some(row => row.id === actor.id)
            ? "stopped-awaiting-restore" : "stopped-for-good" : quiescence === null ? "unknown-quiescence"
            : quiescence.length ? `waiting:${quiescence.join(",")}` : "quiesced";
        return { id: actor.id, name: actor.name, status: actor.status, residency: kind, quiescence, worker, disposition };
      });
    } catch { return undefined; }
  }

  /** Read-only: the live session's file and leaf (Pi's ReadonlySessionManager) and the presence owner incarnation. */
  #session(): MainDrainReply["session"] {
    let file: string | null = null;
    let leafId: string | null = null;
    let ownerIncarnation: string | null = null;
    try {
      const manager = this.host.context()?.sessionManager;
      const sessionFile = manager?.getSessionFile();
      file = typeof sessionFile === "string" && sessionFile ? sessionFile : null;
      const leaf = manager?.getLeafId();
      leafId = typeof leaf === "string" && leaf ? leaf : null;
    } catch { file = null; leafId = null; }
    try {
      const incarnation = this.host.ownerIncarnation?.();
      ownerIncarnation = typeof incarnation === "string" && incarnation ? incarnation : null;
    } catch { ownerIncarnation = null; }
    return { file, leafId, ownerIncarnation };
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
      v: 1, ok: true, op, error: null, sessionId, rootId, meshRoot, pid: process.pid, session: this.#session(),
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
      stopped: [...(peekDrainSlot(rootId)?.stopped ?? [])], stopAttempted: [...(peekDrainSlot(rootId)?.stopAttempted ?? [])], stopRefused: null,
      restored: [...(peekDrainSlot(rootId)?.restored ?? [])], undo, exiting: false, exitRefused: [],
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
