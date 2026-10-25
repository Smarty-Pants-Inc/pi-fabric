import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { inventorySessionPending, type SessionPendingInventory } from "../cli/session-pending.js";
import type { MainDrainBindingRequest } from "../main-drain/binding.js";
import { callMainDrain, mainDrainSocket, MainDrainClientError } from "../main-drain/client.js";
import { MAIN_DRAIN_INGRESS } from "../main-drain/control.js";
import { mainDrainSocketPath, type MainDrainReply } from "../main-drain/protocol.js";
import { evaluateDrain, scanCompletions, type DrainEvaluation } from "./evaluate.js";
import { interpreterIdentity, parseRequest, proveSeatLock, readOwned, readSealedRun, resolveSeatMeshRoot, SeamGap, sha256,
  verifyRunDir, writeOnce, type SeamRequest } from "./run.js";

/**
 * fabric-v2's D3 `drain` seam (smarty-dev#15310 prep item 13). Contract: SEAT.md §3 and
 * DRAIN-PHASE-CONTRACT.md (sha256 800e7790…), challenge r1 and the 10-25 rulings.
 *
 *   <exe> --run-dir RUN --phase drain|drain-undo|drain-recheck|resume-drain   stdin: request   stdout: ONE receipt
 *
 * - drain: bind the v1 Main to this run (seat-lock proof), close its gates, then wait (600 s,
 *   monotonic from seam start) until every count is 0 and every child is terminal, two passes in a row.
 *   On success: a `pass` receipt. Otherwise: undo (≤60 s), canary, evidence, then a `refused`
 *   receipt {sessionId, reason, inputsFenced} where inputsFenced is the state AFTER the undo.
 * - drain-undo: the intent's operationId releases the fence and reopens the gates; the canary
 *   decides admissionRestored. Idempotent per operationId.
 * - drain-recheck: after v1 is terminal, the filesystem-only session-pending inventory.
 * - resume-drain: the raw Pod (stage 2, kubectl exec); fails closed until then.
 * Nothing is deleted or acknowledged. Evidence is private and write-once under RUN.
 * Exit 2 with no receipt means UNKNOWN: an unparseable request, or evidence that could not be written.
 */
const OWNER = "fabric-v2";
const DEADLINE_MS = 600_000;
const UNDO_BUDGET_MS = 60_000;
const POLL_MS = 1_000;
const MAX_REQUEST = 64 * 1024;
const PHASES = new Set(["drain", "drain-undo", "drain-recheck", "resume-drain"]);
const INTENT = "fabric-v2-drain-intent.json";
const BOUND = "fabric-v2-drain-bound.json";
const HEX64 = /^[0-9a-f]{64}$/;
export const DRAIN_REFUSAL_REASONS = ["pending-timeout", "children-live", "durable-actor-present", "inventory-incomplete",
  "fence-unavailable", "lock-proof-refused", "request-binding-refused", "main-unreachable"] as const;
export type DrainRefusalReason = typeof DRAIN_REFUSAL_REASONS[number];
const RESIDUALS = [
  "SEAT.md §3 A6: a same-UID writer could alter this seam or its user-owned interpreter between hash and exec",
  "native input fence: fail-closed until Pi ships the extension-facing fence (PI-FENCE-API.md)",
  "A-side (principal-lead): the persistent fence across a v1 restart; drain-recheck is the post-stop recount",
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
}
interface Context {
  runDir: string; request: SeamRequest; meshRoot: string; lockFd: number; uid: number; io: SeamIo; deps: SeamDeps;
  now: () => number; sleep: (ms: number) => Promise<void>; deadline: number; started: number;
}

const json = (value: unknown): Buffer => Buffer.from(`${JSON.stringify(value, null, 1)}\n`);
const receipt = (request: Pick<SeamRequest, "run" | "phase">, result: "pass" | "refused", observed: Record<string, unknown>): string =>
  JSON.stringify({ schema: "d3-seam-receipt-v1", run: request.run, phase: request.phase, owner: OWNER, result, observed });
const clientCode = (error: unknown): string =>
  error instanceof MainDrainClientError || error instanceof SeamGap ? error.code : "MAIN_DRAIN_CALL_FAILED";

/** Self-install: write `#!<this node>` + this bundle, write-once 0500; print path and hashes. */
const install = (argv: string[], io: SeamIo): number => {
  if (argv.length !== 3 || argv[1] !== "--out" || !path.isAbsolute(argv[2]!)) { io.stderr("usage: --install --out <absolute path>\n"); return 2; }
  const self = fs.readFileSync(__filename, "utf8");
  const body = self.startsWith("#!") ? self.slice(self.indexOf("\n") + 1) : self;
  const node = interpreterIdentity();
  const bytes = Buffer.from(`#!${node.execPath}\n${body}`);
  const fd = fs.openSync(argv[2]!, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o500);
  try { fs.writeSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  io.stdout(`${JSON.stringify({ path: argv[2], sha256: sha256(bytes), owner: OWNER, interpreter: node,
    residual: node.rootOwned ? null : RESIDUALS[0] })}\n`);
  return 0;
};

const BINDING_GAPS = /^(run-dir|request|seat-run|seat-journal|seat-inputs|executor|source-journal|fabric-config|mesh-root|inputs)/;

export const main = async (argv: string[], io: SeamIo, deps: SeamDeps = {}): Promise<number> => {
  if (argv[0] === "--install") return install(argv, io);
  const now = deps.now ?? (() => performance.now());
  const started = now();
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
  const deadline = started + Math.min(deps.deadlineMs ?? DEADLINE_MS, DEADLINE_MS);
  const uid = deps.uid ?? process.getuid!();
  let request: SeamRequest | undefined;
  try {
    if (argv.length !== 4 || argv[0] !== "--run-dir" || argv[2] !== "--phase" || !PHASES.has(argv[3]!)) throw new SeamGap("usage");
    const runDir = argv[1]!;
    const phase = argv[3]!;
    const stdin = io.stdin();
    if (stdin.length > MAX_REQUEST) throw new SeamGap("request-too-large");
    request = parseRequest(stdin, phase);
    verifyRunDir(runDir, uid);
    const sealed = readSealedRun(runDir, request, uid);
    let lockFd: number;
    try { lockFd = proveSeatLock(runDir, io.env.D3_SEAT_LOCK_FD, uid, deps.python); }
    catch (error) {
      // Without the lock nothing is written under RUN and the Main is never contacted.
      if (phase === "drain" && error instanceof SeamGap) return refuse(io, request, "lock-proof-refused", false);
      throw error;
    }
    if (phase === "resume-drain") return resumeDrainUnavailable(runDir, request, io);
    const meshRoot = resolveSeatMeshRoot(sealed.journal, request.sessionId, uid);
    const context: Context = { runDir, request, meshRoot, lockFd, uid, io, deps, now, sleep, deadline, started };
    if (phase === "drain") {
      if (Object.keys(request.inputs).length !== 0) throw new SeamGap("inputs-invalid");
      return await drain(context);
    }
    if (phase === "drain-undo") return await drainUndo(context);
    if (Object.keys(request.inputs).length !== 0) throw new SeamGap("inputs-invalid");
    return recheck(context);
  } catch (error) {
    const code = error instanceof SeamGap ? error.code : "seam-internal";
    // A drain whose binding cannot be verified touched nothing: a valid refusal opens A's abort path.
    if (request?.phase === "drain" && BINDING_GAPS.test(code)) return refuse(io, request, "request-binding-refused", false);
    io.stderr(`GAP: ${code} [owner: ${OWNER}]\n`);
    return 2;
  }
};

const refuse = (io: SeamIo, request: SeamRequest, reason: DrainRefusalReason, inputsFenced: boolean): number => {
  io.stdout(receipt(request, "refused", { sessionId: request.sessionId, reason, inputsFenced }));
  return 0;
};

const binding = (context: Context, phase: MainDrainBindingRequest["phase"], operationId: string): MainDrainBindingRequest => ({
  runDir: context.runDir, run: context.request.run, runSha256: context.request.runSha256, phase, operationId, lockFd: context.lockFd, podUid: null,
});

const call = async (context: Context, op: "drain" | "undo" | "probe", bound: MainDrainBindingRequest | null, timeoutMs: number):
  Promise<{ reply?: MainDrainReply; error?: string }> => {
  try {
    const socket = mainDrainSocket(context.meshRoot, context.request.sessionId, context.uid);
    return { reply: await callMainDrain(socket, context.request.sessionId, op, bound, Math.max(1_000, Math.min(timeoutMs, 30_000))) };
  } catch (error) { return { error: clientCode(error) }; }
};

const inventory = (context: Context): SessionPendingInventory | undefined => {
  try { return inventorySessionPending(context.meshRoot, context.request.sessionId); } catch { return undefined; }
};

/** B1's canary through the SAME gate functions as live admission: every gate admits, nothing is bound, native is open. */
const canary = (probe: MainDrainReply | undefined): { inboxOpen: boolean; inputsFenced: boolean } => {
  const inboxOpen = probe !== undefined && probe.binding === null && probe.inbox.closed === false &&
    MAIN_DRAIN_INGRESS.filter(row => row.mechanism === "fabric-gate")
      .every(row => probe.probes.some(entry => entry.path === row.path && entry.admits === true));
  // Without the fence API this process never fenced: native admission is open.
  const inputsFenced = probe === undefined || (probe.native.fenceApi && probe.native.piInputsFenced !== false);
  return { inboxOpen, inputsFenced };
};

/** Undo through the binding (when one was taken), then probe. ≤60 s. admissionRestored folds in no-restart-needed. */
const undoAndProbe = async (context: Context, operationId: string | undefined, phase: MainDrainBindingRequest["phase"]) => {
  const until = context.now() + Math.min(context.deps.undoBudgetMs ?? UNDO_BUDGET_MS, UNDO_BUDGET_MS);
  const attempts: string[] = [];
  let undo: MainDrainReply | undefined;
  let probe: MainDrainReply | undefined;
  let undone = operationId === undefined;
  while (context.now() < until) {
    if (!undone) {
      const result = await call(context, "undo", binding(context, phase, operationId!), until - context.now());
      if (result.error) attempts.push(`undo:${result.error}`); else { undo = result.reply; undone = true; }
    }
    if (undone) {
      const result = await call(context, "probe", null, until - context.now());
      if (result.error) attempts.push(`probe:${result.error}`); else { probe = result.reply; break; }
    }
    if (attempts.at(-1)?.includes("REFUSED")) break;          // a refused binding does not change on retry
    await context.sleep(Math.min(POLL_MS, Math.max(0, until - context.now())));
  }
  const seen = canary(probe);
  const restartRequired = !undone || undo?.undo?.restartRequired === true || seen.inputsFenced;
  return { undo, probe, attempts: attempts.slice(-20), ...seen, restartRequired, admissionRestored: seen.inboxOpen && !restartRequired };
};

const evidence = (context: Context, name: string, value: Record<string, unknown>): string =>
  writeOnce(context.runDir, name, json({ ...value, owner: OWNER, run: context.request.run, sessionId: context.request.sessionId,
    elapsedMs: Math.round(context.now() - context.started) }));

/** The fixed reason for a not-drained outcome, in a fixed precedence. */
export const refusalReason = (evaluation: DrainEvaluation, lastError: string | undefined, everReplied: boolean): DrainRefusalReason => {
  const error = lastError ?? "";
  if (/^MAIN_DRAIN_REFUSED_(PEER_|LOCK_)/.test(error)) return "lock-proof-refused";
  if (/^MAIN_DRAIN_REFUSED/.test(error) || evaluation.reasons.some(reason => reason === "main-identity-mismatch" || reason === "mesh-root-mismatch")) {
    return "request-binding-refused";
  }
  if (!everReplied || evaluation.reasons.includes("main-unreachable")) return "main-unreachable";
  if (evaluation.reasons.some(reason => reason.startsWith("children:durable-actor:"))) return "durable-actor-present";
  if (evaluation.reasons.includes("inventory-incomplete")) return "inventory-incomplete";
  if (evaluation.reasons.includes("native-fence-api-unavailable")) return "fence-unavailable";
  if (evaluation.observed.queued || evaluation.observed.admitted) return "pending-timeout";
  if (!evaluation.observed.childrenTerminal) return "children-live";
  return "fence-unavailable";
};

const drain = async (context: Context): Promise<number> => {
  const { request, meshRoot } = context;
  const operationId = randomBytes(16).toString("hex");
  // M12: the intent is durable BEFORE the first control call, so a SIGKILLed seam leaves its operationId for drain-undo.
  const intentSha256 = evidence(context, INTENT, {
    schema: "fabric-v2-drain-intent-v1", phase: "drain", runSha256: request.runSha256, operationId,
    measured: { meshRoot, socket: mainDrainSocketPath(meshRoot, request.sessionId), interpreter: interpreterIdentity(),
      seam: seamIdentity(), startedAt: new Date().toISOString() },
    deadlineS: Math.min(context.deps.deadlineMs ?? DEADLINE_MS, DEADLINE_MS) / 1000, undoBudgetS: UNDO_BUDGET_MS / 1000, residuals: RESIDUALS,
  });
  const rootId = `session:${request.sessionId}`;
  const passes: Array<{ atMs: number; drained: boolean; error: string | null; reasons: string[] }> = [];
  let previous = false;
  let everReplied = false;
  let boundWritten = false;
  let last: { reply: MainDrainReply | undefined; error: string | undefined; inventory: SessionPendingInventory | undefined;
    evaluation: DrainEvaluation } | undefined;
  let success = false;
  for (;;) {
    const result = await call(context, "drain", binding(context, "drain", operationId), context.deadline - context.now());
    if (result.reply) everReplied = true;
    if (result.reply?.binding && !boundWritten) {
      // The Main's fence generation for this operation, for an operator drain-undo.
      evidence(context, BOUND, { schema: "fabric-v2-drain-bound-v1", operationId, basis: "asserted", binding: result.reply.binding,
        mainPid: result.reply.pid });
      boundWritten = true;
    }
    const files = inventory(context);
    const evaluation = evaluateDrain({ sessionId: request.sessionId, meshRoot, reply: result.reply, inventory: files,
      completions: scanCompletions(meshRoot, rootId), ...(context.deps.procRoot ? { procRoot: context.deps.procRoot } : {}) });
    last = { reply: result.reply ?? last?.reply, error: result.error, inventory: files, evaluation };
    passes.push({ atMs: Math.round(context.now() - context.started), drained: evaluation.drained, error: result.error ?? null,
      reasons: evaluation.reasons.slice(0, 50) });
    if (passes.length > 200) passes.splice(1, 1);
    if (evaluation.drained && previous) { success = true; break; }
    previous = evaluation.drained;
    // Waiting cannot fix these: a refused binding, a durable actor, no fence API, a halted Main holding follow-ups.
    if (result.error?.startsWith("MAIN_DRAIN_REFUSED") || evaluation.haltedWithHeld ||
      evaluation.reasons.includes("native-fence-api-unavailable") ||
      evaluation.reasons.some(reason => reason.startsWith("children:durable-actor:")) || context.deadline - context.now() <= POLL_MS) break;
    await context.sleep(POLL_MS);
  }
  const final = last!;
  if (success) {
    evidence(context, "fabric-v2-drain-evidence.json", {
      schema: "fabric-v2-drain-evidence-v1", result: "drained", operationId, intentSha256, observed: final.evaluation.observed,
      measured: { inventory: final.inventory ?? null, ...final.evaluation.parts.measured },
      asserted: { reply: final.reply ?? null, ...final.evaluation.parts.asserted }, passes,
      dispositions: final.reply?.actors.owned.map(actor => ({ id: actor.id, disposition: actor.disposition, basis: "asserted" })) ?? [],
    });
    context.io.stdout(receipt(request, "pass", { ...final.evaluation.observed }));
    return 0;
  }
  const reason = refusalReason(final.evaluation, final.error, everReplied);
  const undone = await undoAndProbe(context, operationId, "drain");
  evidence(context, "fabric-v2-drain-evidence.json", {
    schema: "fabric-v2-drain-evidence-v1", result: "refused", reason, operationId, intentSha256, reasons: final.evaluation.reasons,
    wouldBeObserved: final.evaluation.observed,
    measured: { inventory: final.inventory ?? null, ...final.evaluation.parts.measured,
      canary: { inboxOpen: undone.inboxOpen, inputsFenced: undone.inputsFenced } },
    asserted: { reply: final.reply ?? null, ...final.evaluation.parts.asserted, undo: undone.undo?.undo ?? null, probe: undone.probe ?? null },
    undo: { admissionRestored: undone.admissionRestored, restartRequired: undone.restartRequired, attempts: undone.attempts }, passes,
  });
  // inputsFenced is the state AFTER the undo: false only when the canary proved admission restored.
  return refuse(context.io, request, reason, !undone.admissionRestored);
};

/** The intent's operationId (absent: no drain ever reached the Main from this run). */
const readIntent = (context: Context): { operationId: string } | undefined => {
  const file = path.join(context.runDir, INTENT);
  if (!fs.existsSync(file)) return undefined;
  let value: Record<string, unknown>;
  try { value = JSON.parse(readOwned(file, context.uid, 1024 * 1024, { mode: 0o600 }, "intent-unreadable").toString("utf8")) as Record<string, unknown>; }
  catch { throw new SeamGap("intent-unreadable"); }
  const measured = value.measured as Record<string, unknown> | undefined;
  if (value.schema !== "fabric-v2-drain-intent-v1" || value.run !== context.request.run || value.sessionId !== context.request.sessionId ||
    typeof value.operationId !== "string" || !/^[0-9a-f]{32}$/.test(value.operationId) || measured?.meshRoot !== context.meshRoot) {
    throw new SeamGap("intent-invalid");
  }
  return { operationId: value.operationId };
};

/** A's refused drain receipt, byte-exact under RUN/receipts/, hashing to the given value. */
const verifyRefusedReceipt = (context: Context, expected: string): void => {
  const bytes = readOwned(path.join(context.runDir, "receipts", "drain.json"), context.uid, 1024 * 1024, undefined, "refused-receipt-unreadable");
  if (sha256(bytes) !== expected) throw new SeamGap("refused-receipt-sha256-mismatch");
  let value: Record<string, unknown>;
  try { value = JSON.parse(bytes.toString("utf8")) as Record<string, unknown>; } catch { throw new SeamGap("refused-receipt-invalid"); }
  const observed = value.observed as Record<string, unknown> | undefined;
  if (value.schema !== "d3-seam-receipt-v1" || value.run !== context.request.run || value.phase !== "drain" || value.owner !== OWNER ||
    value.result !== "refused" || observed?.sessionId !== context.request.sessionId) throw new SeamGap("refused-receipt-invalid");
};

/** Write-once evidence: a repeated call (idempotent per operationId) takes the next free name. */
const nextName = (context: Context, base: string): string => {
  for (let index = 0; index < 1000; index++) {
    const name = index === 0 ? `${base}.json` : `${base}-${index}.json`;
    if (!fs.existsSync(path.join(context.runDir, name))) return name;
  }
  throw new SeamGap("evidence-names-exhausted");
};

const drainUndo = async (context: Context): Promise<number> => {
  const inputs = context.request.inputs;
  const sha = inputs.refusedReceiptSha256;
  if (Object.keys(inputs).join(",") !== "refusedReceiptSha256" || (sha !== null && (typeof sha !== "string" || !HEX64.test(sha)))) {
    throw new SeamGap("inputs-invalid");
  }
  // null only on the operator path (a SIGKILLed seam wrote no receipt): the intent file alone names the operation.
  if (sha !== null) verifyRefusedReceipt(context, sha);
  const intent = readIntent(context);
  if (sha === null && !intent) throw new SeamGap("intent-missing");
  const undone = await undoAndProbe(context, intent?.operationId, "drain-undo");
  const observed = { sessionId: context.request.sessionId, inputsFenced: undone.inputsFenced, admissionRestored: undone.admissionRestored };
  evidence(context, nextName(context, "fabric-v2-drain-undo-evidence"), {
    schema: "fabric-v2-drain-undo-evidence-v1", operationId: intent?.operationId ?? null, refusedReceiptSha256: sha, observed,
    measured: { canary: { inboxOpen: undone.inboxOpen, inputsFenced: undone.inputsFenced } },
    asserted: { undo: undone.undo?.undo ?? null, probe: undone.probe ?? null },
    restartRequired: undone.restartRequired, attempts: undone.attempts,
  });
  context.io.stdout(receipt(context.request, "pass", observed));
  return 0;
};

/** drain-recheck (after `fence`, v1 terminal): the session-pending inventory, filesystem only. */
const recheck = (context: Context): number => {
  const files = inventory(context);
  const totals = files?.totals ?? { actors: null, followUps: null, queued: null, inFlight: null, settledWindows: null,
    pendingDeliveryOutcomes: null, childCompletionEntries: null };
  const observed = {
    sessionId: context.request.sessionId,
    complete: files?.complete === true,
    totals: { actors: totals.actors, followUps: totals.followUps, queued: totals.queued, inFlight: totals.inFlight,
      settledWindows: totals.settledWindows, pendingDeliveryOutcomes: totals.pendingDeliveryOutcomes, childCompletionEntries: totals.childCompletionEntries },
    // Unknown counts as not stopped: an unreadable inventory is one row we could not see.
    actorsNotStopped: files ? files.actors.filter(actor => actor.status !== "stopped").length : 1,
  };
  evidence(context, "fabric-v2-drain-recheck-evidence.json", {
    schema: "fabric-v2-drain-recheck-evidence-v1", observed, basis: "measured", inventory: files ?? null,
    completions: scanCompletions(context.meshRoot, `session:${context.request.sessionId}`),
  });
  context.io.stdout(receipt(context.request, "pass", observed));
  return 0;
};

/** Stage 2 builds the kubectl-exec path (ruling 10-25); until then the raw Pod is never reported drained. */
const resumeDrainUnavailable = (runDir: string, request: SeamRequest, io: SeamIo): number => {
  const pod = request.inputs.pod as Record<string, unknown> | undefined;
  if (Object.keys(request.inputs).join(",") !== "pod" || typeof pod !== "object" || pod === null ||
    Object.keys(pod).sort().join(",") !== "name,uid" || typeof pod.uid !== "string" || typeof pod.name !== "string") {
    throw new SeamGap("inputs-invalid");
  }
  const observed = { podUid: pod.uid, inputsFenced: false, exited: false };
  writeOnce(runDir, "fabric-v2-resume-drain-evidence.json", json({ schema: "fabric-v2-resume-drain-evidence-v1", owner: OWNER,
    run: request.run, sessionId: request.sessionId, result: "unavailable", reason: "resume-drain-channel-not-built", observed }));
  io.stdout(receipt(request, "pass", observed));
  return 0;
};

const seamIdentity = (): { path: string; sha256: string | null } => {
  const file = process.argv[1] ?? "";
  try { return { path: file, sha256: sha256(fs.readFileSync(file)) }; } catch { return { path: file, sha256: null }; }
};
