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
  parentSessionId: string; candidatesValid?: boolean; actorId?: string; activationId?: string;
  live?: boolean; revertReset?: string;
}, evaluate: RouteEvaluate, signal?: AbortSignal): Promise<ModelRouteDecision> {
  const started = performance.now();
  signal?.throwIfAborted();
  const candidates = [input.pin, ...input.candidates].filter((candidate, index, all) =>
    all.findIndex(other => other.model === candidate.model && other.effort === candidate.effort) === index);
  const decision: ModelRouteDecision = {
    ...input.pin, decisionId: allocateDecisionId(), mode: "shadow",
    routeClass: input.routeClass, parentSessionId: input.parentSessionId, pin: { ...input.pin },
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
  for (const row of rows) {
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

export function isRouteClassReverted(routeClass: string, reset = ""): boolean {
  return revertReason(readRouteState(), routeClass, reset) !== undefined;
}

/** Append-only replay makes counters durable and avoids process-local lost updates.
 * Retries deduplicate by decisionId. Reset generations isolate old in-flight outcomes. */
function saveRouteOutcome(decision: ModelRouteDecision, result: { status?: string; routeQuality?: "pass" | "fail" }, ledger: string): void {
  if (decision.mode !== "live") return;
  const file = routeStateFile();
  const reset = decision.revertReset ?? "";
  const type = result.status ? "result" : "quality";
  let rows = readRouteState();
  if (!rows.some(row => row.type === type && row.decisionId === decision.decisionId &&
    (type !== "quality" || row.routeQuality === result.routeQuality))) {
    const event: RouteStateEvent = { type, routeClass: decision.routeClass, reset, decisionId: decision.decisionId, ...result, at: Date.now() };
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
  } catch (error) {
    // Judgment dispatch is fail-closed; only shadow routing may fall back.
    if (decision.mode === "judgment") throw error;
    decision.reasonCode = "record-failed";
    decision.mode = "shadow";
    Object.assign(decision, decision.pin);
    // Shadow failures never block pinned work. The header still carries the failed record's ID.
  }
  let appended = false;
  let pendingRecord: object | undefined;
  const pendingFile = path.join(runDirectory, "pending-route-outcome.json");
  return {
    header: routeHeader(decision), ...(sessionFile ? { sessionFile } : {}), bindSession,
    reportQuality(quality) {
      if (quality !== "pass" && quality !== "fail") throw new Error("Invalid routeQuality");
      appendRouteRecord(file, { type: "quality", decisionId: decision.decisionId, runId: childId,
        routeClass: decision.routeClass, routeQuality: quality, at: Date.now() });
      saveRouteOutcome(decision, { routeQuality: quality }, file);
    },
    outcome(result) {
      if (appended) return;
      pendingRecord ??= { type: "outcome", decisionId: decision.decisionId, childSessionId: decision.actorId ? null : childId, runId: childId,
        ...(decision.actorId ? { actorId: decision.actorId, activationId: decision.activationId } : {}),
        status: result.status, admittedModel: result.admittedModel ?? (result.status === "completed" ? result.model ?? null : null),
        admittedEffort: result.admittedThinking ?? (result.status === "completed" ? result.thinking ?? null : null),
        observedModel: result.model ?? null,
        tokens: result.usage ?? null, routeQuality: result.routeQuality ?? null, reasonCode: decision.reasonCode, at: Date.now() };
      try {
        appendRouteRecord(file, pendingRecord);
        saveRouteOutcome(decision, { status: result.status, ...(result.routeQuality ? { routeQuality: result.routeQuality } : {}) }, file);
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
