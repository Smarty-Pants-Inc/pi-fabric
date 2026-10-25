import { randomBytes } from "node:crypto";
import fs from "node:fs";
import type net from "node:net";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { inventorySessionPending, type SessionPendingInventory } from "../cli/session-pending.js";
import { procStartTime, type MainDrainBindingRequest } from "../main-drain/binding.js";
import { callMainDrain, mainDrainSocket, MainDrainClientError } from "../main-drain/client.js";
import { MAIN_DRAIN_INGRESS } from "../main-drain/control.js";
import { mainDrainSocketPath, type MainDrainReply } from "../main-drain/protocol.js";
import { evaluateDrain, scanCompletions, type DrainEvaluation } from "./evaluate.js";
import { fixedKubectl, kubectl, readPod, verifyKubeconfig, type KubeAccess } from "./kube.js";
import { interpreterIdentity, parseRequest, proveSeatLock, readOwned, readSealedRun, resolveSeatMeshRoot, SeamGap, sha256,
  socketPeer, verifyRunDir, writeOnce, type SealedRun, type SeamRequest } from "./run.js";

/**
 * fabric-v2's D3 `drain` seam (smarty-dev#15310 prep item 13). Contract: SEAT.md §3,
 * DRAIN-PHASE-CONTRACT.md rev 3 (sha256 c67041c7…) and DRAIN-DESIGN.r1 (sha256 7ecdac72…).
 *
 *   <exe> --run-dir RUN --phase drain|drain-undo|drain-recheck|resume-drain   stdin: request   stdout: ONE receipt
 *
 * Results are truthful: `pass` only with the phase's passing values, otherwise `refused` with the same
 * closed set. A valid receipt is exit 0 with empty stderr. A lock-proof or binding failure prints NO
 * receipt and exits 2 (UNKNOWN), in every phase, before anything is fenced.
 * - drain: pre-read; intent; bind the v1 Main (seat-lock proof) and close its gates; wait (600 s,
 *   monotonic from seam start) until every count is 0 and every child terminal, two passes in a row.
 *   Otherwise undo (≤60 s) and canary, then `refused` {sessionId, reason, inputsFenced-after-undo}.
 * - drain-undo: release through the kept handle and reopen; the canary decides admissionRestored.
 * - drain-recheck: after v1 is terminal, the filesystem-only session-pending inventory.
 * - resume-drain: the raw Pod (stage 2, kubectl exec); fails closed until then.
 * Nothing is deleted or acknowledged. Evidence is private, write-once, under RUN.
 */
const OWNER = "fabric-v2";
const DEADLINE_MS = 600_000;
const UNDO_BUDGET_MS = 60_000;
const POLL_MS = 1_000;
const MAX_REQUEST = 64 * 1024;
const PHASES = new Set(["drain", "drain-undo", "drain-recheck", "resume-drain"]);
const INTENT = "fabric-v2-drain-intent.json";
/** Stop-intent files per drain: one per new set of actors to stop (a retried request reuses the last one). */
const MAX_STOP_CALLS = 20;
/** Stop refusals a later pass can clear by waiting; any other stop refusal ends the drain (refused, then undone). */
const RETRYABLE_STOP = new Set(["NOT_FENCED", "NOT_DRAINED", "ACTORS_NOT_QUIESCED"]);
const OUTCOME = "fabric-v2-drain-outcome.json";
const HEX64 = /^[0-9a-f]{64}$/;
export const DRAIN_REFUSAL_REASONS = ["pending-timeout", "children-live", "durable-actor-present", "inventory-incomplete",
  "fence-unavailable", "lock-proof-refused", "request-binding-refused", "main-unreachable"] as const;
export type DrainRefusalReason = typeof DRAIN_REFUSAL_REASONS[number];
const RESIDUALS = [
  "SEAT.md §3 A6: a same-UID writer could alter this seam or its user-owned interpreter between hash and exec",
  "native input fence: fail-closed until Pi ships the extension-facing fence (PI-FENCE-API.md)",
  "A-side: no persisted fence across a v1 restart; drain-recheck is the post-stop recount",
  "non-retrying producers get a visible FABRIC_MAIN_DRAINING refusal, not exactly-once delivery",
  "ingress row 8 (feed herdr prompt): a Pi refusal is invisible to the feed; an A/runbook precondition",
];

export interface SeamIo {
  stdin: () => Buffer;
  stdout: (text: string) => void;
  stderr: (text: string) => void;
  env: NodeJS.ProcessEnv;
}
export interface SeamDeps {
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  deadlineMs?: number;
  undoBudgetMs?: number;
  python?: string;
  procRoot?: string;
  uid?: number;
  /** Tests only: the kubectl client (production uses the fixed root-owned one) and the in-image CLI path (null: unconfirmed). */
  kubectl?: string;
  podCli?: string | null;
  /** Tests only: see the error behind a fixed GAP code. */
  onError?: (error: unknown) => void;
}
interface Context {
  runDir: string; request: SeamRequest; meshRoot: string; lockFd: number; uid: number; io: SeamIo; deps: SeamDeps;
  now: () => number; sleep: (ms: number) => Promise<void>; deadline: number; started: number; requestSha256: string;
  /** The sealed run's `seams.fence.owner`: the owner a fence receipt must name (never a constant here). */
  fenceOwner?: string;
}
/** A binding failure discovered after the seam started talking: still no receipt, exit 2. */
class BindingGap extends SeamGap {}

const json = (value: unknown): Buffer => Buffer.from(`${JSON.stringify(value, null, 1)}\n`);
const receipt = (request: SeamRequest, result: "pass" | "refused", observed: Record<string, unknown>): string =>
  JSON.stringify({ schema: "d3-seam-receipt-v1", run: request.run, phase: request.phase, owner: OWNER, result, observed });
const clientCode = (error: unknown): string =>
  error instanceof MainDrainClientError || error instanceof SeamGap ? error.code : "MAIN_DRAIN_CALL_FAILED";
const INTERPRETER_TOKEN = ["__FABRIC_SEAT_DRAIN", "INTERPRETER_SHA256__"].join("_");

/**
 * Self-install: `#!<this node's absolute path>` + this bundle, with that interpreter's sha256 embedded
 * (the entry refuses any other interpreter), written once, 0500. Prints the path and both hashes.
 */
export const install = (argv: string[], io: SeamIo, self: string): number => {
  if (argv.length !== 3 || argv[1] !== "--out" || !path.isAbsolute(argv[2]!)) { io.stderr("usage: --install --out <absolute path>\n"); return 2; }
  const source = fs.readFileSync(self, "utf8");
  const body = source.startsWith("#!") ? source.slice(source.indexOf("\n") + 1) : source;
  const node = interpreterIdentity();
  const quoted = JSON.stringify(INTERPRETER_TOKEN);
  if (!node.sha256 || body.split(quoted).length !== 2) { io.stderr("install: interpreter unhashable or bundle token missing\n"); return 2; }
  const bytes = Buffer.from(`#!${node.execPath}\n${body.replace(quoted, JSON.stringify(node.sha256))}`);
  const fd = fs.openSync(argv[2]!, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o500);
  try { fs.writeSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  io.stdout(`${JSON.stringify({ path: argv[2], sha256: sha256(bytes), owner: OWNER, interpreter: node,
    residual: node.rootOwned ? null : RESIDUALS[0] })}\n`);
  return 0;
};

export const main = async (argv: string[], io: SeamIo, deps: SeamDeps = {}): Promise<number> => {
  const now = deps.now ?? (() => performance.now());
  const started = now();
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
  const deadline = started + Math.min(deps.deadlineMs ?? DEADLINE_MS, DEADLINE_MS);
  const uid = deps.uid ?? process.getuid!();
  try {
    if (argv.length !== 4 || argv[0] !== "--run-dir" || argv[2] !== "--phase" || !PHASES.has(argv[3]!)) throw new SeamGap("usage");
    const runDir = argv[1]!;
    const phase = argv[3]!;
    const stdin = io.stdin();
    if (stdin.length > MAX_REQUEST) throw new SeamGap("request-too-large");
    const request = parseRequest(stdin, phase);
    verifyRunDir(runDir, uid);
    const sealed = readSealedRun(runDir, request, uid);
    const lockFd = proveSeatLock(runDir, io.env.D3_SEAT_LOCK_FD, uid, deps.python);
    if (phase === "resume-drain") {
      return await resumeDrain({ runDir, request, meshRoot: "", lockFd, uid, io, deps, now, sleep, deadline, started,
        requestSha256: sha256(stdin) }, sealed);
    }
    let meshRoot: string | undefined;
    try { meshRoot = resolveSeatMeshRoot(sealed.journal, request.sessionId, uid); }
    catch (error) {
      // Only drain has a refusal for it (DRAIN-DESIGN.r1 §1.2): an unreadable mesh root is inventory-incomplete.
      if (phase !== "drain" || !(error instanceof SeamGap)) throw error;
    }
    const seams = sealed.run.seams as Record<string, { owner?: unknown } | undefined> | undefined;
    const fenceOwner = typeof seams?.fence?.owner === "string" && seams.fence.owner ? seams.fence.owner : undefined;
    const context: Context = { runDir, request, meshRoot: meshRoot ?? "", lockFd, uid, io, deps, now, sleep, deadline, started,
      requestSha256: sha256(stdin), ...(fenceOwner ? { fenceOwner } : {}) };
    if (phase === "drain") {
      if (Object.keys(request.inputs).join(",") !== "operationId") throw new SeamGap("inputs-invalid");
      if (meshRoot === undefined) return refuse(context, "inventory-incomplete", false);
      return await drain(context);
    }
    if (phase === "drain-undo") return await drainUndo(context);
    if (Object.keys(request.inputs).join(",") !== "operationId") throw new SeamGap("inputs-invalid");
    return recheck(context);
  } catch (error) {
    const code = error instanceof SeamGap ? error.code : "seam-internal";
    deps.onError?.(error);
    io.stderr(`GAP: ${code} [owner: ${OWNER}]\n`);
    return 2;
  }
};

const refuse = (context: Context, reason: DrainRefusalReason, inputsFenced: boolean): number => {
  context.io.stdout(receipt(context.request, "refused", { sessionId: context.request.sessionId, reason, inputsFenced }));
  return 0;
};

const binding = (context: Context, phase: MainDrainBindingRequest["phase"], generation: string): MainDrainBindingRequest => ({
  runDir: context.runDir, run: context.request.run, runSha256: context.request.runSha256, phase,
  operationId: context.request.operationId, generation, lockFd: context.lockFd, podUid: null,
});

/**
 * DRAIN-DESIGN.r1 §1.7: the server must be the Main that owns this session's inbox: its SO_PEERCRED
 * uid is ours, and its pid and /proc start time are those in `main-followups/<sid>.owner.json`.
 */
const verifyServer = (context: Context) => async (client: net.Socket): Promise<string | undefined> => {
  let owner: Record<string, unknown> | undefined;
  try {
    owner = JSON.parse(readOwned(path.join(context.meshRoot, "main-followups", `${encodeURIComponent(context.request.sessionId)}.owner.json`),
      context.uid, 64 * 1024, undefined, "owner-unreadable").toString("utf8")) as Record<string, unknown>;
  } catch { return "MAIN_DRAIN_OWNER_UNREADABLE"; }
  const peer = await socketPeer(client, context.deps.python);
  if (!peer || peer.uid !== context.uid) return "MAIN_DRAIN_SERVER_UNVERIFIED";
  if (owner.pid !== peer.pid || owner.rootId !== `session:${context.request.sessionId}` || owner.retired === true ||
    (typeof owner.host === "string" && owner.host !== os.hostname()) || typeof owner.processStartedAt !== "string" ||
    procStartTime(peer.pid, context.deps.procRoot) !== owner.processStartedAt) return "MAIN_DRAIN_SERVER_NOT_OWNER";
  return undefined;
};

const call = async (context: Context, op: "drain" | "stop-actors" | "undo" | "probe", bound: MainDrainBindingRequest | null, timeoutMs: number):
  Promise<{ reply?: MainDrainReply; error?: string; written?: boolean }> => {
  try {
    const socket = mainDrainSocket(context.meshRoot, context.request.sessionId, context.uid);
    const reply = await callMainDrain(socket, context.request.sessionId, op, bound, Math.max(1_000, Math.min(timeoutMs, 30_000)),
      verifyServer(context));
    // The Main must name this run's mesh root and root; another one is a binding failure (rev 3: no receipt).
    let ours: string | undefined, theirs: string | undefined;
    try { ours = fs.realpathSync(context.meshRoot); theirs = fs.realpathSync(reply.meshRoot); } catch { /* compared below */ }
    if (!ours || ours !== theirs || reply.rootId !== `session:${context.request.sessionId}`) {
      // Read-only ops mutate nothing: a mismatch there is a binding failure (no receipt). After a mutating op,
      // the drain must still undo, so it is an ordinary error the loop refuses on.
      if (op === "probe") throw new BindingGap("main-binding-mismatch");
      return { error: "MAIN_DRAIN_BINDING_MISMATCH", written: true };
    }
    return { reply, written: true };
  } catch (error) {
    if (error instanceof BindingGap) throw error;
    return { error: clientCode(error), written: error instanceof MainDrainClientError && error.written };
  }
};

const inventory = (context: Context): SessionPendingInventory | undefined => {
  try { return inventorySessionPending(context.meshRoot, context.request.sessionId); } catch { return undefined; }
};

/** B1's canary, through the SAME gate functions as live admission: every gate admits, nothing is bound, native is open. */
const canary = (probe: MainDrainReply | undefined): { inboxOpen: boolean; inputsFenced: boolean } => {
  // Every Fabric-gated row, the combined ones included, must admit through its Fabric half (re-review finding 2);
  // the native half is judged by `inputsFenced` below.
  const inboxOpen = probe !== undefined && probe.binding === null && probe.inbox.closed === false &&
    MAIN_DRAIN_INGRESS.filter(row => row.mechanism === "fabric-gate" || row.mechanism === "both")
      .every(row => probe.probes.some(entry => entry.path === row.path && entry.mechanism === row.mechanism && entry.fabric === true));
  // A host without the fence capability can never have been fenced by this operation. A fence-capable host must
  // READ open: a missing context or unreadable state is unknown, so still fenced (stage 1 review finding 4).
  const inputsFenced = probe === undefined || (probe.native.capability !== false &&
    !(probe.native.fenceApi && probe.native.piInputsFenced === false));
  return { inboxOpen, inputsFenced };
};

/**
 * Undo (when a binding was taken), then probe; ≤60 s. admissionRestored folds in no-restart-needed AND
 * (r2 N2) every actor this operation stopped being active again. A refusal of the undo's binding (another
 * operation's fence, no lock) is a binding failure: no receipt (contract rev 4 §2).
 */
const undoAndProbe = async (context: Context, generation: string | undefined, phase: MainDrainBindingRequest["phase"],
  stoppedIds: readonly string[] = []) => {
  const until = context.now() + Math.min(context.deps.undoBudgetMs ?? UNDO_BUDGET_MS, UNDO_BUDGET_MS);
  const attempts: string[] = [];
  let undo: MainDrainReply | undefined;
  let probe: MainDrainReply | undefined;
  let undone = generation === undefined;
  while (context.now() < until) {
    if (!undone) {
      const result = await call(context, "undo", binding(context, phase, generation!), until - context.now());
      if (result.error?.startsWith("MAIN_DRAIN_REFUSED")) throw new BindingGap(`undo-refused:${result.error.slice(19).toLowerCase()}`);
      if (result.error) attempts.push(`undo:${result.error}`); else { undo = result.reply; undone = true; }
    }
    if (undone) {
      const result = await call(context, "probe", null, until - context.now());
      if (result.error) attempts.push(`probe:${result.error}`); else { probe = result.reply; break; }
    }
    await context.sleep(Math.min(POLL_MS, Math.max(0, until - context.now())));
  }
  const seen = canary(probe);
  const restartRequired = !undone || undo?.undo?.restartRequired === true || seen.inputsFenced;
  const stopped = new Set([...stoppedIds, ...(undo?.stopped ?? []).map(entry => entry.id)]);
  const actorsRestored = undo?.undo?.actorsRestored !== false && [...stopped].every(id =>
    probe?.actors.owned.some(actor => actor.id === id && actor.status !== "stopped") === true);
  return { undo, probe, attempts: attempts.slice(-20), ...seen, restartRequired, actorsRestored,
    admissionRestored: seen.inboxOpen && !restartRequired && actorsRestored };
};

const evidence = (context: Context, name: string, value: Record<string, unknown>): string =>
  writeOnce(context.runDir, name, json({ ...value, owner: OWNER, run: context.request.run, sessionId: context.request.sessionId,
    operationId: context.request.operationId, elapsedMs: Math.round(context.now() - context.started) }));

/** The fixed reason for a not-drained outcome, in DRAIN-DESIGN.r1 §1.10's order. */
export const refusalReason = (evaluation: DrainEvaluation | undefined, everReplied: boolean): DrainRefusalReason => {
  if (!everReplied || !evaluation || evaluation.reasons.includes("main-unreachable")) return "main-unreachable";
  if (evaluation.reasons.some(reason => reason.startsWith("children:durable-actor:"))) return "durable-actor-present";
  if (evaluation.reasons.includes("native-fence-api-unavailable")) return "fence-unavailable";
  if (evaluation.reasons.includes("inventory-incomplete")) return "inventory-incomplete";
  if (!evaluation.observed.childrenTerminal) return "children-live";
  if (evaluation.observed.queued || evaluation.observed.admitted) return "pending-timeout";
  return "fence-unavailable";
};

const drain = async (context: Context): Promise<number> => {
  const { request, meshRoot } = context;
  const rootId = `session:${request.sessionId}`;
  // §1.11 step 2: a measured pre-read. An owned durable actor refuses before anything is touched.
  const pre = inventory(context);
  if (pre?.complete && pre.actors.some(actor => actor.rootId === rootId && actor.residency === "durable")) {
    return refuse(context, "durable-actor-present", false);
  }
  const generation = randomBytes(16).toString("hex");
  const socket = mainDrainSocketPath(meshRoot, request.sessionId);
  let socketInode: string | null = null;
  try { socketInode = String(fs.lstatSync(socket).ino); } catch { /* absent: the drain will report main-unreachable */ }
  // M12: the intent is durable BEFORE the first control call, so a SIGKILLed seam leaves the operation for drain-undo.
  const intentSha256 = evidence(context, INTENT, {
    schema: "fabric-v2-drain-intent-v2", phase: "drain", runSha256: request.runSha256, generation,
    requestSha256: context.requestSha256,
    measured: { meshRoot, socket, socketInode, interpreter: interpreterIdentity(), seam: seamIdentity(), startedAt: new Date().toISOString(),
      t0MonotonicMs: context.started, main: pre?.followUps.owner ?? null },
    deadlineS: Math.min(context.deps.deadlineMs ?? DEADLINE_MS, DEADLINE_MS) / 1000, undoBudgetS: UNDO_BUDGET_MS / 1000, residuals: RESIDUALS,
  });
  // A read-only look first: a Main that names another mesh root or root is a binding failure before anything closes.
  await call(context, "probe", null, Math.min(30_000, context.deadline - context.now()));
  const passes: Array<{ atMs: number; drained: boolean; error: string | null; reasons: string[] }> = [];
  const steps: Array<{ atMs: number; step: string; detail: unknown; basis: "asserted" | "measured" }> = [];
  const step = (name: string, detail: unknown, basis: "asserted" | "measured" = "asserted"): void => {
    steps.push({ atMs: Math.round(context.now() - context.started), step: name, detail, basis });
  };
  let previous = false;
  let bound = false;
  let fenced = false;
  let stopAsked = false;
  const specs = new Map<string, Record<string, unknown>>();
  let stopCalls = 0;
  const intendedStops = new Set<string>();
  const withSpecs = (stopped: MainDrainReply["stopped"]) => stopped.map(entry => ({ ...entry,
    registryRow: specs.get(entry.id) ?? null, basis: { spec: "asserted", registryRow: "measured" } }));
  // Confirmed stops plus attempted ones (a stop sent but not proven): both must be recreated or active for an undo.
  const touched = (reply: MainDrainReply | undefined): MainDrainReply["stopped"] => {
    const all = new Map<string, MainDrainReply["stopped"][number]>();
    for (const entry of [...(reply?.stopAttempted ?? []), ...(reply?.stopped ?? [])]) all.set(entry.id, entry);
    return [...all.values()];
  };
  // Whether any drain request reached the Main: if none did, nothing was closed or fenced.
  let everSent = false;
  const dispositions = new Map<string, string>();
  let last: { reply: MainDrainReply | undefined; inventory: SessionPendingInventory | undefined; evaluation: DrainEvaluation } | undefined;
  let success = false;
  for (;;) {
    const result = await call(context, "drain", binding(context, "drain", generation), context.deadline - context.now());
    if (result.written) everSent = true;
    if (!bound) {
      // Before any gate closed, a lock-proof or binding refusal is UNKNOWN: no receipt (contract rev 3).
      if (result.error?.startsWith("MAIN_DRAIN_REFUSED")) throw new BindingGap(`main-refused:${result.error.slice(19).toLowerCase()}`);
      if (result.reply?.binding) { bound = true; step("begin", { binding: result.reply.binding, mainPid: result.reply.pid }); }
    }
    if (result.reply) {
      for (const actor of result.reply.actors.owned) {
        if (dispositions.get(actor.id) !== actor.disposition) {
          dispositions.set(actor.id, actor.disposition);
          step("actor", { id: actor.id, status: actor.status, disposition: actor.disposition, worker: actor.worker });
        }
      }
      if (result.reply.native.inputsFenced !== fenced) { fenced = result.reply.native.inputsFenced; step(fenced ? "fence" : "fence-released", result.reply.native); }
    }
    const files = inventory(context);
    const evaluation = evaluateDrain({ sessionId: request.sessionId, meshRoot, reply: result.reply, inventory: files,
      completions: scanCompletions(meshRoot, rootId), ...(context.deps.procRoot ? { procRoot: context.deps.procRoot } : {}) });
    last = { reply: result.reply ?? last?.reply, inventory: files, evaluation };
    passes.push({ atMs: Math.round(context.now() - context.started), drained: evaluation.drained, error: result.error ?? null,
      reasons: evaluation.reasons.slice(0, 50) });
    if (passes.length > 200) passes.splice(1, 1);
    if (evaluation.drained && previous) { success = true; break; }
    previous = evaluation.drained;
    // r2 N2: actors are stopped ONLY as the last act: the fence is held and nothing but live session actors remains.
    if (fenced && !stopAsked && evaluation.reasons.length > 0 && evaluation.reasons.every(reason => reason.startsWith("children:live-actor:"))) {
      stopAsked = true;
      // Record each live actor's FULL registry row (instructions included) before the stop: the spec an operator
      // (or a later recreation) needs, measured from the file, private to RUN.
      for (const actor of files?.actors ?? []) {
        if (actor.rootId !== rootId || actor.status === "stopped" || specs.has(actor.id)) continue;
        try {
          const registry = JSON.parse(readOwned(actor.registry, context.uid, 64 * 1024 * 1024, undefined, "registry-unreadable").toString("utf8")) as
            { actors?: Array<Record<string, unknown>> };
          const row = registry.actors?.find(entry => entry.id === actor.id);
          if (row) specs.set(actor.id, row);
        } catch { /* recorded as missing: the evidence then carries only the Main's public spec */ }
      }
      // #1607 P1: the rows are durable BEFORE the stop request, so a crash after it still leaves them for drain-undo
      // and the operator. A new write-once file only when this request names an actor no earlier file named: a
      // retried request (a retryable refusal such as ACTORS_NOT_QUIESCED) is already covered and costs no file, so
      // the cap counts distinct actor sets, never the 600 s wait (RFL P2).
      const toStop = (files?.actors ?? []).filter(actor => actor.rootId === rootId && actor.status !== "stopped");
      if (stopCalls === 0 || toStop.some(actor => !intendedStops.has(actor.id))) {
        if (stopCalls >= MAX_STOP_CALLS) break;
        stopCalls += 1;
        for (const actor of toStop) intendedStops.add(actor.id);
        evidence(context, `fabric-v2-drain-stop-${stopCalls}-intent.json`, { schema: "fabric-v2-drain-stop-intent-v1", generation,
          actors: toStop.map(actor => ({ id: actor.id, registryRow: specs.get(actor.id) ?? null, basis: "measured" })) });
      }
      const stop = await call(context, "stop-actors", binding(context, "drain", generation), context.deadline - context.now());
      step("stop-actors", { error: stop.error ?? null, refused: stop.reply?.stopRefused ?? null, stopped: stop.reply?.stopped ?? [] });
      if (stop.reply) last = { ...last!, reply: stop.reply };
      // Not all quiesced yet, or not fenced or drained at that moment: ask again on a later pass. A failed or unproven
      // stop (or unknown actors) is not retried: the drain refuses and undoes (#1607 P1).
      if (stop.reply?.stopRefused && RETRYABLE_STOP.has(stop.reply.stopRefused)) stopAsked = false;
      else if (stop.error || stop.reply?.stopRefused) break;
      if (context.deadline - context.now() <= POLL_MS) break;
      await context.sleep(POLL_MS);
      continue;
    }
    // Waiting cannot fix these: no Main at all, a durable actor, no fence API, a halted Main holding follow-ups.
    if ((!bound && result.error) || evaluation.haltedWithHeld || evaluation.reasons.includes("native-fence-api-unavailable") ||
      evaluation.reasons.some(reason => reason.startsWith("children:durable-actor:")) || context.deadline - context.now() <= POLL_MS) break;
    await context.sleep(POLL_MS);
  }
  const final = last!;
  step("probes", final.reply?.probes ?? null);
  if (success) {
    evidence(context, OUTCOME, {
      schema: "fabric-v2-drain-outcome-v2", result: "pass", intentSha256, observed: final.evaluation.observed, steps,
      measured: { inventory: final.inventory ?? null, ...final.evaluation.parts.measured },
      asserted: { reply: final.reply ?? null, ...final.evaluation.parts.asserted }, passes,
      dispositions: [...dispositions].map(([id, disposition]) => ({ id, disposition, basis: "asserted" })),
      stoppedActors: withSpecs(touched(final.reply)), stopIntended: [...intendedStops], admitted: final.reply?.admitted ?? [],
      ledger: final.reply?.ledger ?? [],
      ingress: MAIN_DRAIN_INGRESS,
    });
    context.io.stdout(receipt(request, "pass", { ...final.evaluation.observed }));
    return 0;
  }
  const reason = refusalReason(final.evaluation, bound || final.reply !== undefined);
  const ledger = final.reply?.ledger ?? [];
  const admitted = final.reply?.admitted ?? [];
  const stoppedActors = touched(final.reply);
  if (!everSent) {
    // begin never reached the Main (refused before the request was written): nothing to undo.
    evidence(context, OUTCOME, {
      schema: "fabric-v2-drain-outcome-v2", result: "refused", reason, intentSha256, reasons: final.evaluation.reasons,
      wouldBeObserved: final.evaluation.observed, steps, passes, beginSent: false, basis: "measured",
      undo: { admissionRestored: true, restartRequired: false, neverSent: true }, ingress: MAIN_DRAIN_INGRESS,
    });
    return refuse(context, reason, false);
  }
  const undone = await undoAndProbe(context, generation, "drain", [...new Set([...stoppedActors.map(entry => entry.id), ...intendedStops])]);
  step("undo", { admissionRestored: undone.admissionRestored, restartRequired: undone.restartRequired, attempts: undone.attempts });
  evidence(context, OUTCOME, {
    schema: "fabric-v2-drain-outcome-v2", result: "refused", reason, intentSha256, reasons: final.evaluation.reasons,
    wouldBeObserved: final.evaluation.observed, steps,
    measured: { inventory: final.inventory ?? null, ...final.evaluation.parts.measured,
      canary: { inboxOpen: undone.inboxOpen, inputsFenced: undone.inputsFenced } },
    asserted: { reply: final.reply ?? null, ...final.evaluation.parts.asserted, undo: undone.undo?.undo ?? null, probe: undone.probe ?? null },
    passes, dispositions: [...dispositions].map(([id, disposition]) => ({ id, disposition, basis: "asserted" })), ingress: MAIN_DRAIN_INGRESS,
    stoppedActors: withSpecs(stoppedActors), stopIntended: [...intendedStops], actorsRestored: undone.actorsRestored, admitted, ledger,
    undo: { admissionRestored: undone.admissionRestored, restartRequired: undone.restartRequired, attempts: undone.attempts },
  });
  // inputsFenced is the state AFTER the undo: false only when the canary proved admission restored (or nothing was bound).
  return refuse(context, reason, !undone.admissionRestored);
};

/** Any directory entry, a dangling symlink included: only ENOENT is absence (re-review finding 1). */
const present = (file: string): boolean => {
  try { return fs.lstatSync(file, { throwIfNoEntry: false }) !== undefined; } catch { return true; }
};

/** The drain intent (absent: no drain reached its intent in this run). Its operationId must be the request's. */
const readIntent = (context: Context): { generation: string; sha256: string } | undefined => {
  const file = path.join(context.runDir, INTENT);
  if (!present(file)) return undefined;
  let value: Record<string, unknown>;
  let bytes: Buffer;
  try {
    bytes = readOwned(file, context.uid, 1024 * 1024, { mode: 0o600 }, "intent-unreadable");
    value = JSON.parse(bytes.toString("utf8")) as Record<string, unknown>;
  } catch { throw new SeamGap("intent-unreadable"); }
  const measured = value.measured as Record<string, unknown> | undefined;
  if (value.schema !== "fabric-v2-drain-intent-v2" || value.run !== context.request.run || value.sessionId !== context.request.sessionId ||
    typeof value.generation !== "string" || !/^[0-9a-f]{32}$/.test(value.generation) || measured?.meshRoot !== context.meshRoot) {
    throw new SeamGap("intent-invalid");
  }
  if (value.operationId !== context.request.operationId) throw new SeamGap("operation-id-mismatch");
  return { generation: value.generation, sha256: sha256(bytes) };
};

/**
 * The actors a drain was about to stop, from its stop-intent files (written O_EXCL BEFORE each stop request, so a
 * SIGKILLed seam still leaves every row an operator may need). This operation's only; a foreign one is UNKNOWN.
 */
const stopIntents = (context: Context): Array<{ id: string; registryRow: unknown }> => {
  const rows: Array<{ id: string; registryRow: unknown }> = [];
  for (let index = 1; index <= MAX_STOP_CALLS; index++) {
    const file = path.join(context.runDir, `fabric-v2-drain-stop-${index}-intent.json`);
    if (!present(file)) break;
    let value: Record<string, unknown>;
    try { value = JSON.parse(readOwned(file, context.uid, 64 * 1024 * 1024, { mode: 0o600 }, "stop-intent-unreadable").toString("utf8")) as Record<string, unknown>; }
    catch { throw new SeamGap("stop-intent-unreadable"); }
    if (value.schema !== "fabric-v2-drain-stop-intent-v1" || value.operationId !== context.request.operationId || value.run !== context.request.run ||
      !Array.isArray(value.actors)) throw new SeamGap("stop-intent-invalid");
    for (const actor of value.actors as Array<Record<string, unknown>>) {
      if (typeof actor?.id !== "string") throw new SeamGap("stop-intent-invalid");
      rows.push({ id: actor.id, registryRow: actor.registryRow ?? null });
    }
  }
  return rows;
};

/**
 * A's stored `drain` receipt, byte-exact under RUN/receipts/, hashing to the given value: this run, phase
 * drain, owner fabric-v2, this session. A `pass` is accepted only while RUN/receipts/fence.json is absent
 * (contract rev 4 §2, N6 iii: A rejected the pass, or died before `fence`).
 */
const verifyDrainReceipt = (context: Context, expected: string): "pass" | "refused" => {
  const bytes = readOwned(path.join(context.runDir, "receipts", "drain.json"), context.uid, 1024 * 1024, undefined, "drain-receipt-unreadable");
  if (sha256(bytes) !== expected) throw new SeamGap("drain-receipt-sha256-mismatch");
  let value: Record<string, unknown>;
  try { value = JSON.parse(bytes.toString("utf8")) as Record<string, unknown>; } catch { throw new SeamGap("drain-receipt-invalid"); }
  const observed = value.observed as Record<string, unknown> | undefined;
  if (value.schema !== "d3-seam-receipt-v1" || value.run !== context.request.run || value.phase !== "drain" || value.owner !== OWNER ||
    (value.result !== "refused" && value.result !== "pass") || observed?.sessionId !== context.request.sessionId) throw new SeamGap("drain-receipt-invalid");
  if (value.result === "pass" && present(path.join(context.runDir, "receipts", "fence.json"))) throw new SeamGap("drain-pass-after-fence");
  return value.result;
};

/** A's stored `fence` receipt: this run, phase fence, the sealed run's fence owner, a pass (re-review finding 3). */
const verifyFenceReceipt = (context: Context): void => {
  if (!context.fenceOwner) throw new SeamGap("fence-owner-unpinned");
  const bytes = readOwned(path.join(context.runDir, "receipts", "fence.json"), context.uid, 4 * 1024 * 1024, undefined, "fence-receipt-missing");
  let value: Record<string, unknown>;
  try { value = JSON.parse(bytes.toString("utf8")) as Record<string, unknown>; } catch { throw new SeamGap("fence-receipt-invalid"); }
  // A's own receipt acceptance shape: exactly these six keys, and the fence seam's owner value.
  if (typeof value !== "object" || value === null || Object.keys(value).sort().join(",") !== "observed,owner,phase,result,run,schema" ||
    value.schema !== "d3-seam-receipt-v1" || value.run !== context.request.run || value.phase !== "fence" || value.owner !== context.fenceOwner ||
    value.result !== "pass" || typeof value.observed !== "object" || value.observed === null) throw new SeamGap("fence-receipt-invalid");
};

/** The drain outcome's records (absent: the drain never wrote one). */
const drainOutcome = (context: Context): { dispositions: Array<{ id: string; disposition: string }>; stopped: string[]; result: string | null;
  beginSent: boolean; intentSha256: string | null } | undefined => {
  const file = path.join(context.runDir, OUTCOME);
  if (!present(file)) return undefined;
  let value: Record<string, unknown>;
  try { value = JSON.parse(readOwned(file, context.uid, 64 * 1024 * 1024, { mode: 0o600 }, "outcome-unreadable").toString("utf8")) as Record<string, unknown>; }
  catch { throw new SeamGap("outcome-unreadable"); }
  if (value.schema !== "fabric-v2-drain-outcome-v2" || value.operationId !== context.request.operationId || value.run !== context.request.run ||
    value.sessionId !== context.request.sessionId || value.owner !== OWNER || (value.result !== "pass" && value.result !== "refused")) {
    throw new SeamGap("outcome-invalid");
  }
  // A passing outcome carries exactly the passing drain values (truthful results).
  const observed = value.observed as Record<string, unknown> | undefined;
  if (value.result === "pass" && !(observed?.sessionId === context.request.sessionId && observed.inputAdmission === 1 &&
    observed.inputsFenced === true && observed.queued === 0 && observed.admitted === 0 && observed.childrenTerminal === true)) {
    throw new SeamGap("outcome-invalid");
  }
  const dispositions = Array.isArray(value.dispositions) ? value.dispositions as Array<{ id: string; disposition: string }> : [];
  const stopped = Array.isArray(value.stoppedActors) ? (value.stoppedActors as Array<{ id: string }>).map(entry => entry.id) : [];
  // Only the explicit, measured `beginSent: false` (no drain request was ever written to the Main) counts as never sent.
  return { dispositions, stopped, result: typeof value.result === "string" ? value.result : null, beginSent: value.beginSent !== false,
    intentSha256: typeof value.intentSha256 === "string" ? value.intentSha256 : null };
};

/** The v1 Main's inbox owner record (pid and kernel start time), measured from main-followups/<sid>.owner.json. */
const ownerIdentity = (context: Context): { pid: number; processStartedAt: string } | null => {
  try {
    const owner = JSON.parse(readOwned(path.join(context.meshRoot, "main-followups", `${encodeURIComponent(context.request.sessionId)}.owner.json`),
      context.uid, 64 * 1024, undefined, "owner-unreadable").toString("utf8")) as Record<string, unknown>;
    return Number.isSafeInteger(owner.pid) && typeof owner.processStartedAt === "string"
      ? { pid: owner.pid as number, processStartedAt: owner.processStartedAt } : null;
  } catch { return null; }
};

const drainUndo = async (context: Context): Promise<number> => {
  const inputs = context.request.inputs;
  const sha = inputs.drainReceiptSha256;
  const attempt = inputs.attempt;
  if (Object.keys(inputs).sort().join(",") !== "attempt,drainReceiptSha256,operationId" || !Number.isSafeInteger(attempt) ||
    (attempt as number) < 1 || (attempt as number) > 999 || (sha !== null && (typeof sha !== "string" || !HEX64.test(sha)))) {
    throw new SeamGap("inputs-invalid");
  }
  const drainResult = sha !== null ? verifyDrainReceipt(context, sha) : undefined;
  // null: only the operator path, after a drain that left no receipt (contract rev 4 §2).
  if (sha === null && present(path.join(context.runDir, "receipts", "drain.json"))) throw new SeamGap("drain-receipt-exists");
  const names = { intent: `fabric-v2-drain-undo-${attempt}-intent.json`, outcome: `fabric-v2-drain-undo-${attempt}-outcome.json` };
  // Idempotent per (operationId, phase, attempt): a finished attempt reprints its receipt; a crashed one is UNKNOWN.
  if (present(path.join(context.runDir, names.outcome))) {
    const done = JSON.parse(readOwned(path.join(context.runDir, names.outcome), context.uid, 64 * 1024 * 1024, { mode: 0o600 },
      "undo-outcome-unreadable").toString("utf8")) as Record<string, unknown>;
    if (done.operationId !== context.request.operationId || typeof done.receipt !== "string") throw new SeamGap("undo-outcome-invalid");
    context.io.stdout(done.receipt);
    return 0;
  }
  if (present(path.join(context.runDir, names.intent))) throw new SeamGap("undo-attempt-unfinished");
  const main = ownerIdentity(context);
  // Attempts are contiguous (stage 1 review finding 8): n > 1 follows an UNKNOWN attempt n-1 (its intent without an
  // outcome), or a refused one followed by a PROVEN v1 restart (another owner pid or start time). Never after a pass.
  if ((attempt as number) > 1) {
    const previous = `fabric-v2-drain-undo-${(attempt as number) - 1}`;
    if (!present(path.join(context.runDir, `${previous}-intent.json`))) throw new SeamGap("undo-attempt-out-of-order");
    const prior = path.join(context.runDir, `${previous}-outcome.json`);
    if (present(prior)) {
      const done = JSON.parse(readOwned(prior, context.uid, 64 * 1024 * 1024, { mode: 0o600 }, "undo-outcome-unreadable").toString("utf8")) as
        Record<string, unknown>;
      if (done.result === "pass") throw new SeamGap("undo-already-passed");
      // Only a closed-set HANDLE-LOST refusal {inputsFenced: true, admissionRestored: false} is retried (contract rev 4
      // N6 i), and only after a PROVEN restart: both owner records known and different.
      const seen = done.observed as Record<string, unknown> | undefined;
      if (done.result !== "refused" || seen?.inputsFenced !== true || seen.admissionRestored !== false) throw new SeamGap("undo-retry-not-handle-lost");
      const before = done.main as { pid?: unknown; processStartedAt?: unknown } | null | undefined;
      if (!main || !before || typeof before.pid !== "number" || typeof before.processStartedAt !== "string" ||
        (before.pid === main.pid && before.processStartedAt === main.processStartedAt)) {
        throw new SeamGap("undo-retry-without-restart");
      }
    }
  }
  const intent = readIntent(context);
  // A pass receipt proves the drain wrote its intent and fenced; a missing intent then contradicts it (finding 5).
  if (drainResult === "pass" && !intent) throw new SeamGap("drain-pass-without-intent");
  const outcome = drainOutcome(context);
  // #1607 B1: a drain that refused before any drain request reached the Main (main-unreachable, a binding refused
  // before it was written) wrote its intent, then an outcome saying so. That outcome, bound to THIS intent's bytes and
  // a refused result, proves nothing was fenced, exactly as an absent intent does (r2 N3).
  const neverSent = intent !== undefined && outcome !== undefined && outcome.result === "refused" && outcome.beginSent === false &&
    outcome.intentSha256 === intent.sha256;
  const attempted = stopIntents(context);
  const undoIntentSha256 = evidence(context, names.intent, { schema: "fabric-v2-drain-undo-intent-v2", attempt, drainReceiptSha256: sha,
    generation: intent?.generation ?? null, measured: { startedAt: new Date().toISOString(), interpreter: interpreterIdentity(),
      drainIntentPresent: intent !== undefined, drainBeginNeverSent: neverSent, stopIntents: attempted.map(entry => entry.id) } });
  let observed: { sessionId: string; inputsFenced: boolean; admissionRestored: boolean };
  let detail: Record<string, unknown>;
  if (!intent || neverSent) {
    // r2 N3: the intent is written O_EXCL before `begin`; without it, or with the outcome above, nothing was fenced.
    observed = { sessionId: context.request.sessionId, inputsFenced: false, admissionRestored: true };
    detail = { basis: "measured", neverFenced: true, evidence: intent ? "outcome-begin-never-sent" : "no-intent" };
  } else {
    // Every actor this operation was about to stop counts, whether or not its stop was confirmed.
    const undone = await undoAndProbe(context, intent.generation, "drain-undo", [...new Set([...(outcome?.stopped ?? []), ...attempted.map(entry => entry.id)])]);
    observed = { sessionId: context.request.sessionId, inputsFenced: undone.inputsFenced, admissionRestored: undone.admissionRestored };
    detail = { measured: { canary: { inboxOpen: undone.inboxOpen, inputsFenced: undone.inputsFenced } },
      asserted: { undo: undone.undo?.undo ?? null, probe: undone.probe ?? null },
      restartRequired: undone.restartRequired, actorsRestored: undone.actorsRestored, attempts: undone.attempts };
  }
  const passed = !observed.inputsFenced && observed.admissionRestored;
  const text = receipt(context.request, passed ? "pass" : "refused", observed);
  evidence(context, names.outcome, { schema: "fabric-v2-drain-undo-outcome-v2", attempt, result: passed ? "pass" : "refused",
    intentSha256: undoIntentSha256, observed, receipt: text, main, ...detail });
  context.io.stdout(text);
  return 0;
};

/** drain-recheck (after `fence`, v1 terminal): the session-pending inventory, filesystem only. */
const recheck = (context: Context): number => {
  // Lineage (stage 1 review finding 5): this operation's drain intent, its PASSING outcome and A's fence receipt.
  if (!readIntent(context)) throw new SeamGap("intent-missing");
  if (drainOutcome(context)?.result !== "pass") throw new SeamGap("drain-not-passed");
  verifyFenceReceipt(context);
  const intentSha256 = evidence(context, "fabric-v2-drain-recheck-intent.json", { schema: "fabric-v2-drain-recheck-intent-v1",
    measured: { meshRoot: context.meshRoot, startedAt: new Date().toISOString() } });
  // The in-process `pi-fabric session pending`: exit 0 is complete; exit 1 is complete:false; a throw is any other exit.
  const files = inventorySessionPending(context.meshRoot, context.request.sessionId);
  const totals = files.totals;
  // r2 N4: a row is stopped only when stopped for good (explicit or directive) AND it is in the drain's
  // disposition list. A host-exit stop revives on a same-session start, so it is NOT stopped.
  const known = new Set((drainOutcome(context)?.dispositions ?? []).map(entry => entry.id));
  const stoppedForGood = (actor: { id: string; status: string; stopReason: string | null }): boolean =>
    actor.status === "stopped" && (actor.stopReason === "explicit" || actor.stopReason === "directive") && known.has(actor.id);
  const observed = {
    sessionId: context.request.sessionId,
    complete: files.complete,
    totals: { actors: totals.actors, followUps: totals.followUps, queued: totals.queued, inFlight: totals.inFlight,
      settledWindows: totals.settledWindows, pendingDeliveryOutcomes: totals.pendingDeliveryOutcomes, childCompletionEntries: totals.childCompletionEntries },
    actorsNotStopped: files.actors.filter(actor => !stoppedForGood(actor)).length,
  };
  const passed = observed.complete && Number.isSafeInteger(totals.actors) && (totals.actors ?? -1) >= 0 && observed.actorsNotStopped === 0 &&
    (["followUps", "queued", "inFlight", "settledWindows", "pendingDeliveryOutcomes", "childCompletionEntries"] as const).every(key => totals[key] === 0);
  evidence(context, "fabric-v2-drain-recheck-outcome.json", {
    schema: "fabric-v2-drain-recheck-outcome-v1", result: passed ? "pass" : "refused", intentSha256, observed, basis: "measured", inventory: files,
    completions: scanCompletions(context.meshRoot, `session:${context.request.sessionId}`),
  });
  context.io.stdout(receipt(context.request, passed ? "pass" : "refused", observed));
  return 0;
};

/**
 * The in-image pi-fabric CLI that `kubectl exec` runs inside the raw Pod (fixed argv, ruling 10-25).
 * CONFIRMED 10-25 by the seat image's owner (read-only, on the then-current image family): pi-fabric 0.112.0's CLI is
 * /opt/pi-fabric/dist/cli/index.js (mode 0555), with /usr/local/bin/pi-fabric pointing to it (DRAIN-DESIGN.r2 Q5).
 * The seam still checks `test -x` on it inside the Pod first: cut 2 builds a new base and must re-confirm it.
 * Tests inject SeamDeps.podCli (null: unconfirmed).
 */
const POD_FABRIC_CLI = { path: "/opt/pi-fabric/dist/cli/index.js", confirmed: true } as const;
const POD_NAME = /^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$/;
const POD_UID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * resume-drain (DRAIN-DESIGN.r1 §1.13, challenge r1 M9): drain the raw v2 Pod's Main through `kubectl exec`
 * of the in-image `pi-fabric session drain --exit`, bound to the pod UID. The same zero and children checks
 * run inside the Pod; a not-drained Main undoes itself there. `exited` is the pi container's own terminated
 * exitCode 0, and the pod UID must match the input pin before and after. Truthful: pass only with all three.
 */
const resumeDrain = async (context: Context, sealed: SealedRun): Promise<number> => {
  const { request } = context;
  const pod = request.inputs.pod as Record<string, unknown> | undefined;
  if (Object.keys(request.inputs).sort().join(",") !== "operationId,pod" || typeof pod !== "object" || pod === null ||
    Object.keys(pod).sort().join(",") !== "name,uid" || typeof pod.uid !== "string" || !POD_UID.test(pod.uid) ||
    typeof pod.name !== "string" || !POD_NAME.test(pod.name)) throw new SeamGap("inputs-invalid");
  const kube = sealed.inputs.kube as Record<string, unknown> | null | undefined;
  if (typeof kube !== "object" || kube === null || typeof kube.kubeconfig !== "string" || !path.isAbsolute(kube.kubeconfig) ||
    typeof kube.server !== "string" || typeof kube.caSha256 !== "string" || !HEX64.test(kube.caSha256)) throw new SeamGap("kube-unpinned");
  const access: KubeAccess = { kubectl: context.deps.kubectl ?? fixedKubectl(), kubeconfig: kube.kubeconfig, context: sealed.run.context as string,
    namespace: sealed.run.namespace as string, server: kube.server, caSha256: kube.caSha256, home: sealed.seat.home };
  const remaining = (): number => Math.max(0, context.deadline - context.now());
  await verifyKubeconfig(access, Math.min(30_000, remaining()));
  const before = await readPod(access, pod.name, Math.min(30_000, remaining()));
  if (before && before.uid !== pod.uid) throw new SeamGap("pod-uid-mismatch");
  // The sealed run's target node (re-review finding 4): the raw Pod A created there, nowhere else.
  const target = sealed.run.target as Record<string, unknown> | undefined;
  if (typeof target?.node !== "string" || !target.node) throw new SeamGap("target-node-unpinned");
  if (before && before.nodeName !== target.node) throw new SeamGap("pod-node-mismatch");
  const generation = randomBytes(16).toString("hex");
  const intentSha256 = evidence(context, "fabric-v2-resume-drain-intent.json", { schema: "fabric-v2-resume-drain-intent-v1", generation,
    podUid: pod.uid, requestSha256: context.requestSha256, measured: { kubectl: access.kubectl, server: access.server, before: before ?? null,
      interpreter: interpreterIdentity(), startedAt: new Date().toISOString() } });
  const cli = context.deps.podCli !== undefined ? context.deps.podCli : POD_FABRIC_CLI.confirmed ? POD_FABRIC_CLI.path : null;
  let reason: string | null = null;
  let exec: Record<string, unknown> | undefined;
  let execAttempted = false;
  let execCode: number | null = null;
  // The CLI's last stdout line, bounded, kept even when it is a refusal ({format, error}) for triage.
  let podLine: string | null = null;
  if (!before) reason = "pod-unreadable";
  else if (!cli) reason = "pod-cli-unconfirmed";
  else if (!before.piRunning || !before.meshRoot) reason = before.piRunning ? "pod-mesh-root-unknown" : "pod-not-running";
  else if ((await kubectl(access, ["-n", access.namespace, "exec", pod.name, "-c", "pi", "--", "test", "-x", cli],
    Math.min(30_000, remaining()))).code !== 0) reason = "pod-cli-missing";
  else {
    const deadlineS = Math.max(1, Math.min(540, Math.floor(remaining() / 1000) - 30));
    execAttempted = true;
    const result = await kubectl(access, ["-n", access.namespace, "exec", pod.name, "-c", "pi", "--", cli, "session", "drain", request.sessionId,
      "--mesh", before.meshRoot, "--exit", "--pod-uid", pod.uid, "--run", request.run, "--run-sha256", request.runSha256,
      "--operation-id", request.operationId, "--generation", generation, "--deadline-s", String(deadlineS)], remaining());
    execCode = result.code;
    const line = result.stdout.trim().split("\n").at(-1) ?? "";
    podLine = line.slice(0, 4096);
    try { exec = JSON.parse(line) as Record<string, unknown>; } catch { exec = undefined; }
    if (!exec || exec.format !== 1 || exec.sessionId !== request.sessionId || exec.op !== "exit") { reason = "pod-drain-unreadable"; exec = undefined; }
    // The complete exit response, with a zero kubectl exit (re-review finding 5).
    else if (result.code !== 0 || exec.drained !== true || exec.exiting !== true || exec.inputsFenced !== true) {
      reason = exec.exiting === true ? "pod-exit-incomplete" : "pod-not-drained";
    }
  }
  // After ANY attempted exec, re-read the Pod: the pi container's own exit status, polled until it terminates or the
  // bound ends (only when it said it is exiting); the UID must not change.
  let after = before;
  if (execAttempted) {
    for (;;) {
      after = await readPod(access, pod.name, Math.min(30_000, Math.max(1_000, remaining())));
      if (!after || after.piTerminated || exec?.exiting !== true || remaining() <= 2_000) break;
      await context.sleep(2_000);
    }
  }
  const exited = execAttempted && after?.uid === pod.uid && after.piTerminated?.exitCode === 0;
  // Fence state is the CLI's own report; when an attempted exec gave no complete report it is unknown, so fenced.
  const inputsFenced = execAttempted ? exec === undefined || exec.inputsFenced !== false : false;
  if (!reason && !exited) reason = after?.uid === pod.uid ? "pod-exit-unconfirmed" : "pod-uid-changed";
  if (execAttempted && after && after.uid !== pod.uid) reason = "pod-uid-changed";
  const observed = { podUid: pod.uid, inputsFenced, exited };
  const passed = reason === null && inputsFenced && exited;
  evidence(context, "fabric-v2-resume-drain-outcome.json", { schema: "fabric-v2-resume-drain-outcome-v1", result: passed ? "pass" : "refused",
    reason, intentSha256, observed, measured: { before: before ?? null, after: after ?? null, execCode }, asserted: { podDrain: exec ?? null, podLine } });
  context.io.stdout(receipt(request, passed ? "pass" : "refused", observed));
  return 0;
};

const seamIdentity = (): { path: string; sha256: string | null } => {
  const file = process.argv[1] ?? "";
  try { return { path: file, sha256: sha256(fs.readFileSync(file)) }; } catch { return { path: file, sha256: null }; }
};
