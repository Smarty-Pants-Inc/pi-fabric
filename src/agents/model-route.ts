import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { CURRENT_SESSION_VERSION } from "@earendil-works/pi-coding-agent";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { resolveAgentDir } from "../core/agent-dir.js";
import { runAbortable } from "../async-settlement.js";
import type { JevRequest, JevResponse } from "../jev/types.js";
import type { FabricThinking } from "../thinking.js";
import type { AgentRunResult } from "./types.js";

export interface RouteCandidate { model: string; effort: FabricThinking }
export interface ModelRoutingConfig {
  /** Dedicated role pins: never inferred from Fabric's default medium effort. */
  pinModel?: string;
  pinThinking?: FabricThinking;
  shadowCandidates?: RouteCandidate[];
  /** Class-scoped live permission from trusted host/project config only. */
  liveClasses?: string[];
  /** Change a class token to re-enable it after a durable revert. */
  revertReset?: Record<string, string>;
  /** Legacy false remains shadow-only for unlisted classes; true is never accepted. */
  live?: false;
}
export type RouteReason = "live-choice" | "class-reverted" | "revert-state-error" | "shadow-choice" | "excluded-protected" | "excluded-unknown" | "excluded-class" |
  "judgment-agent" | "low-confidence" | "jev-error" | "jev-timeout" | "malformed" | "invalid-candidates" | "record-failed";
export interface ModelRouteDecision extends RouteCandidate {
  decisionId: string;
  mode: "shadow" | "live" | "judgment";
  /** Host-config reset generation, never supplied by the launch caller. */
  revertReset?: string;
  routeClass: string;
  parentSessionId: string;
  /** Durable activation identity; never inferred from task text. */
  actorId?: string;
  activationId?: string;
  /** Accepted exception for the dispatched pin; never sent to Choice inference. */
  modelReason?: string;
  pin: RouteCandidate;
  candidates: RouteCandidate[];
  shadowChoice: RouteCandidate;
  confidence: number | null;
  probability: number | null;
  reasonCode: RouteReason;
  latencyMs: number;
}
export type RouteEvaluate = (request: JevRequest, signal: AbortSignal) => Promise<JevResponse>;
export const ROUTE_DEADLINE_MS = 2_500;
export const ROUTE_THRESHOLD = 0.90;
export const allocateDecisionId = (): string => randomUUID().replaceAll("-", "");
export const ROUTABLE_CLASSES: readonly string[] = ["bounded-lookup", "status-groom",
  "task:merge-additive", "task:ci-test-fixture", "task:exact-checks"];

/** A shadow Choice is never execution authority. Judgment keeps its existing pin. */
export function routeLaunchCandidate(decision: ModelRouteDecision): RouteCandidate {
  return decision.mode === "live" ? { model: decision.model, effort: decision.effort } : decision.pin;
}

/** One finite Choice. No prompt, task text, history, credentials or generated reason leaves Fabric. */
export async function decideModelRoute(input: {
  routeClass: string; protected: unknown; pin: RouteCandidate; candidates: RouteCandidate[];
  parentSessionId: string; candidatesValid?: boolean; actorId?: string; activationId?: string; modelReason?: string;
  live?: boolean; revertReset?: string;
}, evaluate: RouteEvaluate, signal?: AbortSignal): Promise<ModelRouteDecision> {
  const started = performance.now();
  signal?.throwIfAborted();
  const candidates = [input.pin, ...input.candidates].filter((candidate, index, all) =>
    all.findIndex(other => other.model === candidate.model && other.effort === candidate.effort) === index);
  const decision: ModelRouteDecision = {
    ...input.pin, decisionId: allocateDecisionId(), mode: "shadow",
    routeClass: input.routeClass, parentSessionId: input.parentSessionId, pin: { ...input.pin },
    ...(input.modelReason !== undefined ? { modelReason: input.modelReason } : {}),
    ...(input.actorId ? { actorId: input.actorId, activationId: input.activationId } : {}),
    candidates, shadowChoice: { ...input.pin }, confidence: null, probability: null,
    reasonCode: "jev-error", latencyMs: 0,
  };
  if (input.protected === true) decision.reasonCode = "excluded-protected";
  else if (input.protected !== false) decision.reasonCode = "excluded-unknown";
  else if (!ROUTABLE_CLASSES.includes(input.routeClass)) decision.reasonCode = "excluded-class";
  else if (input.candidatesValid === false) decision.reasonCode = "invalid-candidates";
  else {
    if (input.live === true) { decision.mode = "live"; decision.revertReset = input.revertReset ?? ""; }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ROUTE_DEADLINE_MS);
    const deadline = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
    const criteria = Object.fromEntries(candidates.map((candidate, index) => [`candidate-${index}`, { model: candidate.model, effort: candidate.effort }]));
    try {
      const response = await runAbortable(deadline, () => evaluate({
        state: { routeClass: input.routeClass, mode: "shadow", protection: "clear" },
        questions: { route: { type: "choice", instructions: "Choose the cheapest model and effort suitable for the explicitly declared bounded class: bounded-lookup is lookup/extraction; status-groom is status checks/grooming; merge-additive is additive merge work; ci-test-fixture is CI fixture work; exact-checks is execution of exact specified checks. Never review, security or audit. Choose only from the finite candidates.", criteria } },
      }, deadline));
      const answer = response?.answers?.route;
      const validProbability = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1;
      const chosen = answer?.type === "choice" ? Object.keys(criteria).indexOf(answer.choice) : -1;
      if (answer?.type !== "choice" || chosen < 0 || !validProbability(answer.confidence) ||
        !answer.probabilities || Object.keys(answer.probabilities).length !== candidates.length ||
        !Object.keys(criteria).every(key => validProbability(answer.probabilities[key])) ||
        Math.abs(Object.values(answer.probabilities).reduce((sum, value) => sum + value, 0) - 1) > 0.01) {
        decision.reasonCode = "malformed";
      } else {
        decision.confidence = answer.confidence;
        decision.probability = answer.probabilities[answer.choice]!;
        // Retain the raw finite choice even when thresholds fail; model/effort is the fallback.
        decision.shadowChoice = { ...candidates[chosen]! };
        if (decision.confidence >= ROUTE_THRESHOLD && decision.probability >= ROUTE_THRESHOLD) {
          Object.assign(decision, decision.shadowChoice);
          decision.reasonCode = decision.mode === "live" ? "live-choice" : "shadow-choice";
        } else decision.reasonCode = "low-confidence";
      }
    } catch (error) {
      signal?.throwIfAborted();
      decision.reasonCode = controller.signal.aborted || (error instanceof Error && /timed out|timeout/i.test(error.message)) ? "jev-timeout" :
        error instanceof Error && /invalid|typed response|oversized/i.test(error.message) ? "malformed" : "jev-error";
    } finally { clearTimeout(timer); }
  }
  signal?.throwIfAborted();
  decision.latencyMs = Math.round((performance.now() - started) * 100) / 100;
  return decision;
}

export function routeHeader(decision: ModelRouteDecision): string {
  return `${decision.routeClass}/${encodeURIComponent(decision.model).replace(/[!'()*]/g, character => `%${character.charCodeAt(0).toString(16).toUpperCase()}`)}-${decision.effort}/${decision.reasonCode}:${decision.decisionId}`;
}

/** Same host trust boundary for ledger writes and revert-journal reads. */
function routeDirectory(file: string): { directory: string; created: string[] } {
  const directory = path.dirname(path.resolve(file));
  const ancestors: string[] = [];
  const created: string[] = [];
  for (let current = directory; current !== path.dirname(current); current = path.dirname(current)) ancestors.unshift(current);
  for (const current of ancestors) {
    try { fs.mkdirSync(current, { mode: 0o700 }); created.push(current); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    const stat = fs.lstatSync(current);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("Unsafe routing ledger directory");
  }
  for (const current of ancestors.slice(-2)) {
    const stat = fs.lstatSync(current);
    if (process.platform !== "win32" && (stat.uid !== process.getuid?.() || (stat.mode & 0o022) !== 0)) throw new Error("Unsafe routing ledger directory ownership or permissions");
  }
  return { directory, created };
}

/** Host-owned storage only. Reject links before mkdir, and special files before writing. */
export function appendRouteRecord(file: string, record: object): void {
  const text = `${JSON.stringify(record)}\n`;
  if (Buffer.byteLength(text) > 64 * 1024) throw new Error("Routing record exceeds byte limit");
  const { directory, created } = routeDirectory(file);
  const flags = fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT | fs.constants.O_NONBLOCK | (fs.constants.O_NOFOLLOW ?? 0);
  // lstat also enforces rejection on platforms without O_NOFOLLOW.
  try { if (!fs.lstatSync(file).isFile()) throw new Error("Unsafe routing ledger endpoint"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  const fd = fs.openSync(file, flags, 0o600);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.nlink !== 1 || stat.size + Buffer.byteLength(text) > 64 * 1024 * 1024 ||
      (process.platform !== "win32" && (stat.uid !== process.getuid?.() || (stat.mode & 0o077) !== 0))) throw new Error("Unsafe or oversized routing ledger");
    fs.writeFileSync(fd, text, "utf8"); fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  if (process.platform !== "win32") {
    // Persist the newly created file and directory entries without following links.
    for (const current of new Set([directory, ...created.map(current => path.dirname(current))])) {
      const dirFd = fs.openSync(current, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
      try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); }
    }
  }
}

interface RouteStateEvent {
  type: "result" | "quality"; routeClass: string; reset: string; decisionId: string;
  status?: string; routeQuality?: "pass" | "fail"; at: number;
}
const routeStateFile = () => path.join(resolveAgentDir(), "fabric", "model-routing-state.jsonl");

/** No symlinks, FIFOs, foreign ownership, hardlinks or unbounded journal reads. */
function readRouteRecords(file: string): Array<Record<string, unknown>> {
  routeDirectory(file);
  let fd: number;
  try {
    if (!fs.lstatSync(file).isFile()) throw new Error("Unsafe routing state endpoint");
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NONBLOCK | (fs.constants.O_NOFOLLOW ?? 0));
  }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.nlink !== 1 || stat.size > 64 * 1024 * 1024 ||
      (process.platform !== "win32" && (stat.uid !== process.getuid?.() || (stat.mode & 0o077) !== 0))) throw new Error("Unsafe routing state");
    return fs.readFileSync(fd, "utf8").split("\n").filter(Boolean).map(line => {
      const value: unknown = JSON.parse(line);
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Malformed routing state");
      return value as Record<string, unknown>;
    });
  } finally { fs.closeSync(fd); }
}

function readRouteState(): Array<Record<string, unknown>> {
  const rows = readRouteRecords(routeStateFile());
  for (const row of rows) {
    if ((row.type !== "result" && row.type !== "quality") || typeof row.routeClass !== "string" || !ROUTABLE_CLASSES.includes(row.routeClass) ||
      typeof row.reset !== "string" || row.reset.length > 128 || typeof row.decisionId !== "string" || !row.decisionId ||
      typeof row.at !== "number" || !Number.isFinite(row.at) ||
      (row.routeQuality !== undefined && row.routeQuality !== "pass" && row.routeQuality !== "fail") ||
      (row.type === "quality" && row.routeQuality === undefined) ||
      (row.type === "result" && !["completed", "failed", "stopped", "timed_out"].includes(String(row.status)))) {
      throw new Error("Malformed routing state event");
    }
  }
  return rows;
}

function revertReason(rows: Array<Record<string, unknown>>, routeClass: string, reset: string): "quality-fail" | "consecutive-failures" | undefined {
  const seen = new Set<string>();
  let failures = 0;
  // Storage repair can append an older success after newer failures. Fold the
  // original terminal order, never the journal's retry/append order.
  for (const row of [...rows].sort((a, b) => Number(a.at) - Number(b.at))) {
    if (row.routeClass !== routeClass || row.reset !== reset) continue;
    if (row.routeQuality === "fail") return "quality-fail";
    if (row.type !== "result" || typeof row.decisionId !== "string" || seen.has(row.decisionId)) continue;
    seen.add(row.decisionId);
    if (!["completed", "failed", "stopped", "timed_out"].includes(String(row.status))) throw new Error("Malformed routing outcome state");
    failures = row.status === "completed" ? 0 : failures + 1;
    if (failures >= 2) return "consecutive-failures";
  }
  return undefined;
}

export interface RouteQualityReceipt { decision: ModelRouteDecision; ledger: string; runId: string }
interface PendingQuality extends RouteQualityReceipt { type: "pending"; receiptId: string; routeQuality: "pass" | "fail"; at: number }
const qualityFile = () => path.join(resolveAgentDir(), "fabric", "model-routing-quality.jsonl");
interface PendingRouteSave extends RouteQualityReceipt {
  type: "pending"; receiptId: string; at: number;
  result: { status?: string; routeQuality?: "pass" | "fail" };
  record?: Record<string, unknown>;
}
const safetyFile = () => path.join(resolveAgentDir(), "fabric", "model-routing-pending.jsonl");
// A refused first append must not leave its only assertion in a process-local map.
// This independent journal is also checked by every new owner at admission.
const refusedFile = () => path.join(resolveAgentDir(), "fabric", "model-routing-refused.jsonl");
// Write-ahead safety obligations precede BOTH quality-intent and terminal writes.
// Every execution owner reads/replays this host-owned journal at admission.
const unsavedSafety = new Map<string, Map<string, PendingRouteSave>>();
const safetyObligations = (journal: string): Map<string, PendingRouteSave> => {
  let pending = unsavedSafety.get(journal);
  if (!pending) unsavedSafety.set(journal, pending = new Map());
  return pending;
};

function beginRouteSave(intent: PendingRouteSave): void {
  safetyObligations(safetyFile()).set(intent.receiptId, intent);
  try {
    if (!readRouteRecords(safetyFile()).some(row => row.type === "pending" && row.receiptId === intent.receiptId)) appendRouteRecord(safetyFile(), intent);
  } catch (error) {
    try {
      if (!readRouteRecords(refusedFile()).some(row => row.type === "pending" && row.receiptId === intent.receiptId)) appendRouteRecord(refusedFile(), intent);
    } catch {
      // Even a record-size refusal has a bounded durable class fence. If all
      // storage is unavailable, the admission probes below fail closed too.
      appendRouteRecord(refusedFile(), { type: "refused", receiptId: intent.receiptId,
        routeClass: intent.decision.routeClass, reset: intent.decision.revertReset ?? "", at: intent.at });
    }
    throw error;
  }
}

function commitRouteSave(intent: PendingRouteSave): void {
  const { decision, ledger, result } = intent;
  if (intent.record) {
    if (!readRouteRecords(ledger).some(row => row.type === "outcome" && row.decisionId === decision.decisionId)) {
      appendRouteRecord(ledger, intent.record);
    }
    saveRouteOutcome(decision, result, ledger, Number(intent.record.at));
  } else {
    const quality: PendingQuality = { decision, ledger, runId: intent.runId, type: "pending",
      receiptId: intent.receiptId, routeQuality: result.routeQuality!, at: intent.at };
    const rows = readRouteRecords(qualityFile());
    if (!rows.some(row => row.type === "pending" && row.receiptId === intent.receiptId)) appendRouteRecord(qualityFile(), quality);
    if (!rows.some(row => row.type === "committed" && row.receiptId === intent.receiptId)) commitQuality(quality);
  }
  if (!readRouteRecords(safetyFile()).some(row => row.type === "committed" && row.receiptId === intent.receiptId)) {
    appendRouteRecord(safetyFile(), { type: "committed", receiptId: intent.receiptId, at: Date.now() });
  }
  safetyObligations(safetyFile()).delete(intent.receiptId);
}

function replayRouteSaves(routeClass: string, reset: string): boolean {
  const rows = [...readRouteRecords(safetyFile()), ...readRouteRecords(refusedFile())];
  const pending = new Map<string, PendingRouteSave>();
  const refused = new Set<string>();
  const committed = new Set<string>();
  for (const row of rows) {
    if (typeof row.receiptId !== "string" || !row.receiptId || typeof row.at !== "number" || !Number.isFinite(row.at) ||
      (row.type !== "pending" && row.type !== "committed" && row.type !== "refused")) throw new Error("Malformed routing safety fence");
    if (row.type === "refused") {
      if (typeof row.routeClass !== "string" || !ROUTABLE_CLASSES.includes(row.routeClass) ||
        typeof row.reset !== "string" || row.reset.length > 128) throw new Error("Malformed routing refusal fence");
      if (row.routeClass === routeClass && row.reset === reset) refused.add(row.receiptId);
      continue;
    }
    if (row.type === "committed") { committed.add(row.receiptId); continue; }
    const intent = row as unknown as PendingRouteSave;
    if (!intent.decision || intent.decision.mode !== "live" || !ROUTABLE_CLASSES.includes(intent.decision.routeClass) ||
      typeof intent.decision.decisionId !== "string" || !intent.decision.decisionId ||
      (intent.decision.revertReset !== undefined && (typeof intent.decision.revertReset !== "string" || intent.decision.revertReset.length > 128)) ||
      typeof intent.ledger !== "string" || typeof intent.runId !== "string" || !intent.result ||
      (intent.result.routeQuality !== undefined && intent.result.routeQuality !== "pass" && intent.result.routeQuality !== "fail") ||
      (intent.record ? intent.record.type !== "outcome" || intent.record.decisionId !== intent.decision.decisionId ||
        intent.record.runId !== intent.runId || typeof intent.record.at !== "number" || !Number.isFinite(intent.record.at) ||
        intent.record.status !== intent.result.status ||
        !["completed", "failed", "stopped", "timed_out"].includes(String(intent.result.status)) :
        intent.result.status !== undefined || intent.result.routeQuality === undefined)) throw new Error("Malformed routing safety intent");
    pending.set(intent.receiptId, intent);
  }
  for (const [id, intent] of safetyObligations(safetyFile())) if (!pending.has(id)) pending.set(id, intent);
  let fenced = [...refused].some(id => !committed.has(id) && !pending.has(id));
  for (const intent of pending.values()) {
    if (committed.has(intent.receiptId) || intent.decision.routeClass !== routeClass || (intent.decision.revertReset ?? "") !== reset) continue;
    try { beginRouteSave(intent); commitRouteSave(intent); } catch { fenced = true; }
  }
  return fenced;
}

function commitQuality(intent: PendingQuality): void {
  const { decision, ledger, runId, routeQuality } = intent;
  if (!readRouteRecords(ledger).some(row => row.type === "quality" && row.receiptId === intent.receiptId)) {
    appendRouteRecord(ledger, { type: "quality", receiptId: intent.receiptId, decisionId: decision.decisionId,
      runId, routeClass: decision.routeClass, routeQuality, at: intent.at });
  }
  saveRouteOutcome(decision, { routeQuality }, ledger, intent.at);
  appendRouteRecord(qualityFile(), { type: "committed", receiptId: intent.receiptId, at: Date.now() });
}

export function reportRouteQualityReceipt(receipt: RouteQualityReceipt, quality: "pass" | "fail"): void {
  if (quality !== "pass" && quality !== "fail") throw new Error("Invalid routeQuality");
  if (receipt.decision.mode !== "live") {
    appendRouteRecord(receipt.ledger, { type: "quality", decisionId: receipt.decision.decisionId, runId: receipt.runId,
      routeClass: receipt.decision.routeClass, routeQuality: quality, at: Date.now() });
    return; // Shadow/judgment feedback is audit-only, never a live safety obligation.
  }
  const intent: PendingRouteSave = { ...receipt, type: "pending", receiptId: allocateDecisionId(),
    result: { routeQuality: quality }, at: Date.now() };
  beginRouteSave(intent); // Discoverable before the FIRST quality journal write can fail.
  commitRouteSave(intent);
}

/** A bounded, host-owned receipt survives successful actor handle cleanup. */
export function readRouteQualityReceipt(file: string): RouteQualityReceipt | undefined {
  const rows = readRouteRecords(file);
  if (!rows.length) return undefined;
  if (rows.length !== 1) throw new Error("Malformed route quality receipt");
  const receipt = rows[0] as unknown as RouteQualityReceipt;
  if (typeof receipt.runId !== "string" || typeof receipt.ledger !== "string" ||
    !receipt.decision || typeof receipt.decision.decisionId !== "string" ||
    typeof receipt.decision.routeClass !== "string" || !/^[a-z][a-z0-9:-]{0,63}$/.test(receipt.decision.routeClass) ||
    !["live", "shadow", "judgment"].includes(receipt.decision.mode)) throw new Error("Malformed route quality receipt");
  return receipt;
}

export function isRouteClassReverted(routeClass: string, reset = ""): boolean {
  // Read state first: unsafe/corrupt state cannot be repaired by replaying intent.
  let rows = readRouteState();
  // If even the write-ahead journal cannot accept intents, every owner refuses
  // live admission (including a fresh process with no local failed-write map).
  for (const journal of [safetyFile(), refusedFile()]) {
    const fd = fs.openSync(journal, fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT |
      fs.constants.O_NONBLOCK | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try {
      const stat = fs.fstatSync(fd);
      // Reserve the maximum accepted intent AND its commit record. Open/fsync
      // alone succeeds on a full journal and is not write-ahead capacity.
      if (!stat.isFile() || stat.nlink !== 1 || stat.size > 64 * 1024 * 1024 - 2 * 64 * 1024 ||
        (process.platform !== "win32" && (stat.uid !== process.getuid?.() || (stat.mode & 0o077) !== 0))) {
        throw new Error("Routing write-ahead storage is unsafe or capacity exhausted");
      }
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
  }
  let fenced = replayRouteSaves(routeClass, reset);
  rows = readRouteState();
  const journal = qualityFile();
  const receipts = readRouteRecords(journal);
  const committed = new Set<string>();
  const pending = new Map<string, PendingQuality>();
  for (const row of receipts) {
    if (typeof row.receiptId !== "string" || !row.receiptId || typeof row.at !== "number" || !Number.isFinite(row.at) ||
      (row.type !== "pending" && row.type !== "committed")) throw new Error("Malformed quality fence");
    if (row.type === "committed") committed.add(row.receiptId);
    else {
      const intent = row as unknown as PendingQuality;
      if (!intent.decision || intent.decision.mode !== "live" || !ROUTABLE_CLASSES.includes(intent.decision.routeClass) ||
        (intent.decision.revertReset !== undefined && (typeof intent.decision.revertReset !== "string" || intent.decision.revertReset.length > 128)) ||
        typeof intent.decision.decisionId !== "string" || typeof intent.ledger !== "string" || typeof intent.runId !== "string" ||
        (intent.routeQuality !== "pass" && intent.routeQuality !== "fail")) throw new Error("Malformed quality intent");
      pending.set(intent.receiptId, intent);
    }
  }
  // Replay pre-write-ahead quality receipts too (installed generations may retain them).
  for (const intent of pending.values()) {
    if (committed.has(intent.receiptId) || intent.decision.mode !== "live" ||
      intent.decision.routeClass !== routeClass || (intent.decision.revertReset ?? "") !== reset) continue;
    try {
      commitQuality(intent);
      rows = readRouteState();
    } catch { fenced = true; } // Retry at the next admission; never acknowledge pending safety.
  }
  return fenced || revertReason(rows, routeClass, reset) !== undefined;
}

/** Append-only replay makes counters durable and avoids process-local lost updates.
 * Retries deduplicate by decisionId. Reset generations isolate old in-flight outcomes. */
function saveRouteOutcome(decision: ModelRouteDecision, result: { status?: string; routeQuality?: "pass" | "fail" }, ledger: string, at: number): void {
  if (decision.mode !== "live") return;
  const file = routeStateFile();
  const reset = decision.revertReset ?? "";
  const type = result.status ? "result" : "quality";
  let rows = readRouteState();
  if (!rows.some(row => row.type === type && row.decisionId === decision.decisionId &&
    (type !== "quality" || row.routeQuality === result.routeQuality))) {
    const event: RouteStateEvent = { type, routeClass: decision.routeClass, reset, decisionId: decision.decisionId, ...result, at };
    appendRouteRecord(file, event);
    rows = readRouteState();
  }
  const reason = revertReason(rows, decision.routeClass, reset);
  if (reason && !readRouteRecords(ledger).some(row => row.type === "revert" && row.routeClass === decision.routeClass && row.reset === reset)) {
    appendRouteRecord(ledger, { type: "revert", routeClass: decision.routeClass, reset, reason,
      decisionId: decision.decisionId, at: Date.now() });
  }
}

export function prepareRouteDispatch(decision: ModelRouteDecision, cwd: string | undefined, runDirectory: string, childId: string, options: { ledger?: string; decisionRecorded?: boolean } = {}): {
  header: string; sessionFile?: string; bindSession: (cwd: string) => string; reportQuality: (quality: "pass" | "fail") => void; outcome: (result: Pick<AgentRunResult, "status"> & Partial<AgentRunResult>) => void;
} {
  const file = options.ledger ?? path.join(resolveAgentDir(), "fabric", "model-routing.jsonl");
  let sessionFile: string | undefined;
  // Record before admission; seed only once the run's final worktree is known.
  // A seed write failure must never fall back to a different working directory.
  const bindSession = (finalCwd: string): string => {
    fs.mkdirSync(runDirectory, { recursive: true, mode: 0o700 });
    const file = path.join(runDirectory, "route-session.jsonl");
    fs.writeFileSync(file, `${JSON.stringify({ type: "session", version: CURRENT_SESSION_VERSION,
      id: childId, timestamp: new Date().toISOString(), cwd: finalCwd })}\n`, { mode: 0o600 });
    sessionFile = file;
    return file;
  };
  try {
    // Direct callers may already know the final cwd; manager binds after worktree creation.
    fs.mkdirSync(runDirectory, { recursive: true, mode: 0o700 });
    if (cwd !== undefined) bindSession(cwd);
    if (!options.decisionRecorded) appendRouteRecord(file, { type: "decision", ...decision, childSessionId: decision.actorId ? null : childId, childAgentId: childId, runId: childId, at: Date.now() });
    writeJsonAtomic(path.join(runDirectory, "route-quality-receipt.json"), { decision, ledger: file, runId: childId }, { durable: true });
  } catch (error) {
    // Judgment dispatch is fail-closed; only shadow routing may fall back.
    if (decision.mode === "judgment") throw error;
    decision.reasonCode = "record-failed";
    decision.mode = "shadow";
    Object.assign(decision, decision.pin);
    // Shadow failures never block pinned work. The header still carries the failed record's ID.
  }
  let appended = false;
  let qualityCommitted = false;
  let pendingRecord: Record<string, unknown> | undefined;
  let safetyIntent: PendingRouteSave | undefined;
  const pendingFile = path.join(runDirectory, "pending-route-outcome.json");
  return {
    header: routeHeader(decision), ...(sessionFile ? { sessionFile } : {}), bindSession,
    reportQuality(quality) {
      if (quality !== "pass" && quality !== "fail") throw new Error("Invalid routeQuality");
      reportRouteQualityReceipt({ decision, ledger: file, runId: childId }, quality);
    },
    outcome(result) {
      if (appended) return;
      pendingRecord ??= { type: "outcome", decisionId: decision.decisionId, childSessionId: decision.actorId ? null : childId, runId: childId,
        ...(decision.actorId ? { actorId: decision.actorId, activationId: decision.activationId } : {}),
        status: result.status, admittedModel: result.admittedModel ?? (result.status === "completed" ? result.model ?? null : null),
        admittedEffort: result.admittedThinking ?? (result.status === "completed" ? result.thinking ?? null : null),
        observedModel: result.model ?? null,
        ...(decision.modelReason !== undefined ? { modelReason: decision.modelReason } : {}),
        tokens: result.usage ?? null, routeQuality: result.routeQuality ?? null, reasonCode: decision.reasonCode,
        // Capture at the original terminal callback, not at a later storage retry.
        // Sub-millisecond precision preserves same-tick results across owners.
        at: performance.timeOrigin + performance.now() };
      try {
        if (decision.mode === "live") {
          safetyIntent ??= { type: "pending", receiptId: allocateDecisionId(), decision, ledger: file, runId: childId,
            result: { status: result.status, ...(result.routeQuality ? { routeQuality: result.routeQuality } : {}) },
            record: pendingRecord, at: Number(pendingRecord.at) };
          beginRouteSave(safetyIntent);
        }
        if (result.routeQuality && !qualityCommitted) {
          reportRouteQualityReceipt({ decision, ledger: file, runId: childId }, result.routeQuality);
          qualityCommitted = true;
        }
        if (safetyIntent) commitRouteSave(safetyIntent);
        else appendRouteRecord(file, pendingRecord);
      }
      catch (error) {
        // Preserve the exact join across close/reload, including rejected pre-worker spawns.
        // The manager also keeps its in-memory obligation if this storage write fails.
        try { writeJsonAtomic(pendingFile, { ledger: file, record: pendingRecord }, { durable: true, renameRetries: 1 }); } catch { /* manager retains and surfaces the failure */ }
        throw error;
      }
      appended = true;
      pendingRecord = undefined;
      try { fs.unlinkSync(pendingFile); } catch { /* the durable ledger is authoritative */ }
    },
  };
}
