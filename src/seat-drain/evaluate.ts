import fs from "node:fs";
import path from "node:path";
import type { SessionPendingInventory } from "../cli/session-pending.js";
import { procStartTime } from "../main-drain/binding.js";
import { MAIN_DRAIN_INGRESS } from "../main-drain/control.js";
import type { MainDrainReply } from "../main-drain/protocol.js";

/**
 * D3 seat drain (smarty-dev#15310): ONE place that turns observations into A's six `drain` keys.
 * MEASURED = read by the seam itself (files, /proc). ASSERTED = reported by the Main being drained.
 * Unknown always counts as not drained: a missing count is 1, a missing flag is false.
 */
export interface DrainObserved {
  sessionId: string;
  inputAdmission: number;
  inputsFenced: boolean;
  queued: number;
  admitted: number;
  childrenTerminal: boolean;
}
export interface CompletionScan { pending: number; attempts: number; unknown: number }
export interface DrainEvaluation {
  drained: boolean;
  observed: DrainObserved;
  /** Fixed codes with ids; empty exactly when drained. */
  reasons: string[];
  /** A halted Main never flushes held follow-ups: stop waiting. */
  haltedWithHeld: boolean;
  parts: {
    measured: { fabricHeld: number; fabricHanded: number; heldUnion: number; handedUnion: number; unfinishedRoutes: number; inventoryComplete: boolean;
      completions: CompletionScan; workers: Array<{ id: string; pid: number; exited: boolean | null }> };
    asserted: { fabricHeld: number; fabricHanded: number; nativeQueued: number; nativeAdmitted: number; nativeRunActive: boolean;
      nativeBusy: boolean; fenceApi: boolean; probesRefused: boolean; inboxClosed: boolean; bound: boolean };
  };
}

const known = (value: unknown): number => Number.isSafeInteger(value) && (value as number) >= 0 ? value as number : 1;
const KEY = /^[A-Za-z0-9._-]{1,200}\.json$/;

/** Read-only scan of the agent-completion journal for this root (residency/enqueueCompletion custody). */
export const scanCompletions = (meshRoot: string, rootId: string): CompletionScan => {
  const directory = path.join(meshRoot, "agent-completions");
  const result: CompletionScan = { pending: 0, attempts: 0, unknown: 0 };
  const read = (file: string): Record<string, unknown> | undefined => {
    let fd: number | undefined;
    try {
      fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | (fs.constants.O_NONBLOCK ?? 0));
      const stat = fs.fstatSync(fd);
      if (!stat.isFile() || stat.size > 16 * 1024 * 1024) return undefined;
      const value: unknown = JSON.parse(fs.readFileSync(fd, "utf8"));
      return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
    } catch { return undefined; } finally { if (fd !== undefined) fs.closeSync(fd); }
  };
  const names = (dir: string): string[] | undefined => {
    try {
      const stat = fs.lstatSync(dir, { throwIfNoEntry: false });
      if (!stat) return [];
      if (!stat.isDirectory()) return undefined;
      return fs.readdirSync(dir).sort();
    } catch { return undefined; }
  };
  const recipientRoot = (value: Record<string, unknown> | undefined): string | undefined => {
    const recipient = value?.recipient as Record<string, unknown> | undefined;
    return typeof recipient?.rootId === "string" ? recipient.rootId : undefined;
  };
  const top = names(directory);
  if (!top) { result.unknown++; return result; }
  for (const name of top) {
    const file = path.join(directory, name);
    if (!name.endsWith(".json")) continue;
    if (!KEY.test(name)) { result.unknown++; continue; }
    const value = read(file);
    const root = recipientRoot(value);
    if (root === undefined) { result.unknown++; continue; }
    if (root !== rootId) continue;
    // Consumed only by a VALID receipt for this envelope's run (no-follow read, its own id and fields); an
    // absent receipt is pending, and anything else is unknown, never acknowledged (stage 1 review finding 3).
    const receiptFile = path.join(directory, "receipts", name);
    let present = true;
    try { present = fs.lstatSync(receiptFile, { throwIfNoEntry: false }) !== undefined; } catch { result.unknown++; continue; }
    if (!present) { result.pending++; continue; }
    const runId = (value?.result as Record<string, unknown> | undefined)?.id;
    const receipt = read(receiptFile);
    if (typeof runId !== "string" || !runId || receipt?.id !== runId || typeof receipt.sessionId !== "string" || !receipt.sessionId ||
      typeof receipt.consumedAt !== "number" || !Number.isFinite(receipt.consumedAt) || receipt.consumedAt <= 0) result.unknown++;
  }
  const attempts = names(path.join(directory, "attempts"));
  if (!attempts) { result.unknown++; return result; }
  for (const name of attempts) {
    if (!name.endsWith(".json")) continue;
    const value = read(path.join(directory, "attempts", name));
    const root = recipientRoot(value);
    if (root === undefined) result.unknown++;
    else if (root === rootId) result.attempts++;
  }
  return result;
};

/**
 * drain-recheck (after v1 is terminal): filesystem only, through the session-pending inventory.
 * No Main answers any more, so this is all MEASURED; the factory fence already proved writer death.
 */
export const evaluateRecheck = (sessionId: string, inventory: SessionPendingInventory | undefined, completions: CompletionScan): {
  observed: { sessionId: string; complete: boolean; queued: number; admitted: number; childrenTerminal: boolean }; reasons: string[];
} => {
  const rootId = `session:${sessionId}`;
  const complete = inventory?.complete === true;
  const reasons: string[] = [];
  const queued = complete ? known(inventory.followUps.held) : 1;
  const admitted = complete ? known(inventory.followUps.handed) + known(inventory.routing.unfinishedRoutes) : 1;
  if (!complete) reasons.push("inventory-incomplete");
  for (const actor of complete ? inventory.actors.filter(row => row.rootId === rootId) : []) {
    if (actor.residency !== "session") reasons.push(`${actor.residency}-actor:${actor.id}`);
    else if (actor.status !== "stopped" || actor.inFlightRun || actor.preparing || actor.removal ||
      actor.queued !== 0 || actor.inFlight !== 0 || (actor.childCompletionEntries ?? 1) !== 0) reasons.push(`live-actor:${actor.id}:${actor.status}`);
  }
  if (completions.pending) reasons.push(`completion-pending:${completions.pending}`);
  if (completions.attempts) reasons.push(`agent-attempt:${completions.attempts}`);
  if (completions.unknown) reasons.push(`completion-unknown:${completions.unknown}`);
  const childrenTerminal = complete && reasons.length === 0;
  if (queued) reasons.push(`queued:${queued}`);
  if (admitted) reasons.push(`admitted:${admitted}`);
  return { observed: { sessionId, complete, queued, admitted, childrenTerminal }, reasons };
};

const canonical = (value: string): string | undefined => { try { return fs.realpathSync(value); } catch { return undefined; } };

export const evaluateDrain = (input: {
  sessionId: string;
  meshRoot: string;
  reply: MainDrainReply | undefined;
  inventory: SessionPendingInventory | undefined;
  completions: CompletionScan;
  procRoot?: string;
}): DrainEvaluation => {
  const { sessionId, meshRoot, reply, inventory, completions } = input;
  const rootId = `session:${sessionId}`;
  const reasons: string[] = [];
  const complete = inventory?.complete === true;
  const fileHeld = complete ? known(inventory.followUps.held) : 1;
  const fileHanded = complete ? known(inventory.followUps.handed) : 1;
  const routes = complete ? known(inventory.routing.unfinishedRoutes) : 1;
  const native = reply?.native;
  const memHeld = known(reply?.inbox.held);
  const memHanded = known(reply?.inbox.handed);
  // DRAIN-DESIGN.r1 §1.6: the journal (measured) and the Main's own lists (asserted), de-duplicated by id.
  const fileIds = complete ? inventory.followUps.ids ?? [] : undefined;
  const fileHandedIds = complete ? inventory.followUps.handedIds : undefined;
  const union = (a: string[] | undefined, b: string[] | null | undefined, fallback: number): number =>
    a !== undefined && Array.isArray(b) ? new Set([...a, ...b]).size : fallback;
  const heldUnion = union(fileIds?.filter(id => !fileHandedIds?.includes(id)), reply?.inbox.heldIds, Math.max(fileHeld, memHeld));
  const handedUnion = union(fileHandedIds, reply?.inbox.handedIds, Math.max(fileHanded, memHanded));
  const nativeQueued = !native ? 1 : native.fenceApi ? known(native.queued) : native.pendingMessages === false ? 0 : 1;
  const nativeAdmitted = !native ? 1 : native.fenceApi ? known(native.inputAdmissionCount)
    : native.promptPending === false && native.pendingMessages === false ? 0 : 1;
  const nativeRunActive = native?.idle !== true;
  const nativeBusy = native?.fenceApi === true && native.drained !== true;
  // Every probed row (all but the code route's hold, which A observes) must refuse.
  const probesRefused = reply !== undefined && reply.probes.length === MAIN_DRAIN_INGRESS.length &&
    MAIN_DRAIN_INGRESS.filter(row => row.mechanism !== "hold")
      .every(row => reply.probes.some(probe => probe.path === row.path && probe.mechanism === row.mechanism && probe.admits === false));
  const inputsFenced = native?.fenceApi === true && native.inputsFenced === true && probesRefused && reply!.inbox.closed && reply!.binding !== null;
  // queued: held journal items + unfinished reroutes + Pi's queue; admitted: handed items + Pi's admissions,
  // plus 1 for a live turn and 1 when Pi's own drained check fails.
  const queued = heldUnion + routes + nativeQueued + (complete ? 0 : 1);
  const admitted = handedUnion + nativeAdmitted + (nativeRunActive ? 1 : 0) + (nativeBusy ? 1 : 0);

  if (!reply) reasons.push("main-unreachable");
  else {
    if (reply.sessionId !== sessionId || reply.rootId !== rootId) reasons.push("main-identity-mismatch");
    const ours = canonical(meshRoot);
    if (!ours || canonical(reply.meshRoot) !== ours) reasons.push("mesh-root-mismatch");
  }
  if (!complete) reasons.push("inventory-incomplete");
  if (!inputsFenced) reasons.push(native?.fenceApi ? "inputs-not-fenced" : "native-fence-api-unavailable");
  if (queued) reasons.push(`queued:${queued}`);
  if (admitted) reasons.push(`admitted:${admitted}`);

  // childrenTerminal (challenge r1 B2/B4): stored `stopped` AND the worker's exit proven from this
  // host's own launch record (pid + start time, re-read here from /proc), or the row is gone with no
  // custody left (session-pending complete:true). Durable rows block. Every agent run is terminal and
  // every completion is consumed.
  const children: string[] = [];
  const workers: Array<{ id: string; pid: number; exited: boolean | null }> = [];
  if (!complete) children.push("inventory-incomplete");
  if (!reply?.actors.known) children.push("actors-unknown");
  const owned = new Map((reply?.actors.owned ?? []).map(row => [row.id, row]));
  for (const actor of complete ? inventory.actors.filter(row => row.rootId === rootId) : []) {
    if (actor.residency === "durable") { children.push(`durable-actor:${actor.id}`); continue; }
    if (actor.residency !== "session") { children.push(`unknown-residency:${actor.id}`); continue; }
    if (actor.status !== "stopped") { children.push(`live-actor:${actor.id}:${actor.status}`); continue; }
    if (actor.inFlightRun || actor.preparing || actor.removal) { children.push(`actor-pending:${actor.id}`); continue; }
    if (actor.queued !== 0 || actor.inFlight !== 0 || (actor.childCompletionEntries ?? 1) !== 0) { children.push(`actor-work:${actor.id}`); continue; }
    const row = owned.get(actor.id);
    if (!row) { children.push(`actor-not-owned:${actor.id}`); continue; }
    if (row.worker === null) continue;                  // never launched a worker: nothing to outlive
    if (row.worker === "unknown" || row.worker.startTime === null) { children.push(`worker-unverified:${actor.id}`); continue; }
    const now = procStartTime(row.worker.pid, input.procRoot);
    const exited = now === undefined ? (fs.existsSync(path.join(input.procRoot ?? "/proc", String(row.worker.pid))) ? null : true)
      : now !== row.worker.startTime;
    workers.push({ id: actor.id, pid: row.worker.pid, exited });
    if (exited !== true) children.push(`worker-alive:${actor.id}`);
  }
  for (const row of owned.values()) {
    if (row.status !== "stopped" && !(complete && inventory.actors.some(actor => actor.id === row.id))) children.push(`unrecorded-actor:${row.id}`);
  }
  if (!reply?.agents.known) children.push("agents-unknown");
  for (const agent of reply?.agents.running ?? []) children.push(`agent-running:${agent.id}`);
  // A terminal run's local worker must be gone too (pid + start time from the launch record).
  for (const run of reply?.agents.workers ?? []) {
    if (run.startTime === null) { children.push(`agent-worker-unverified:${run.id}`); continue; }
    const now = procStartTime(run.pid, input.procRoot);
    const exited = now === undefined ? (fs.existsSync(path.join(input.procRoot ?? "/proc", String(run.pid))) ? null : true) : now !== run.startTime;
    workers.push({ id: run.id, pid: run.pid, exited });
    if (exited !== true) children.push(`agent-worker-alive:${run.id}`);
  }
  const inboxPending = reply?.completions.inboxPending;
  if (inboxPending !== 0) children.push(`completion-inbox:${inboxPending ?? "unknown"}`);
  if (completions.pending) children.push(`completion-pending:${completions.pending}`);
  if (completions.attempts) children.push(`agent-attempt:${completions.attempts}`);
  if (completions.unknown) children.push(`completion-unknown:${completions.unknown}`);
  const childrenTerminal = children.length === 0;
  reasons.push(...children.map(reason => `children:${reason}`));

  const inputAdmission = native?.inputAdmission ?? 0;
  if (inputAdmission !== 1) reasons.push("input-admission-not-1");
  const observed: DrainObserved = { sessionId, inputAdmission, inputsFenced, queued, admitted, childrenTerminal };
  const halted = reply?.inbox.halted === true || (complete && inventory.followUps.halted === true);
  return {
    drained: reasons.length === 0,
    observed, reasons,
    haltedWithHeld: halted && heldUnion > 0,
    parts: {
      measured: { fabricHeld: fileHeld, fabricHanded: fileHanded, heldUnion, handedUnion, unfinishedRoutes: routes, inventoryComplete: complete,
        completions, workers },
      asserted: { fabricHeld: memHeld, fabricHanded: memHanded, nativeQueued, nativeAdmitted, nativeRunActive, nativeBusy,
        fenceApi: native?.fenceApi === true, probesRefused, inboxClosed: reply?.inbox.closed === true, bound: reply?.binding != null },
    },
  };
};
