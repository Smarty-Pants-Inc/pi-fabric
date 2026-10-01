import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { CONFIG_DIR_NAME, CURRENT_SESSION_VERSION } from "@earendil-works/pi-coding-agent";
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
}
export type RouteReason = "shadow-choice" | "excluded-protected" | "excluded-unknown" | "excluded-class" |
  "low-confidence" | "jev-error" | "jev-timeout" | "malformed" | "invalid-candidates" | "record-failed";
export interface ModelRouteDecision extends RouteCandidate {
  decisionId: string;
  mode: "shadow";
  routeClass: string;
  parentSessionId: string;
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

/** One finite Choice. No prompt, task text, history, credentials or generated reason leaves Fabric. */
export async function decideModelRoute(input: {
  routeClass: string; protected: unknown; pin: RouteCandidate; candidates: RouteCandidate[];
  parentSessionId: string; candidatesValid?: boolean;
}, evaluate: RouteEvaluate, signal?: AbortSignal): Promise<ModelRouteDecision> {
  const started = performance.now();
  signal?.throwIfAborted();
  const candidates = [input.pin, ...input.candidates].filter((candidate, index, all) =>
    all.findIndex(other => other.model === candidate.model && other.effort === candidate.effort) === index);
  const decision: ModelRouteDecision = {
    ...input.pin, decisionId: randomUUID().replaceAll("-", ""), mode: "shadow",
    routeClass: input.routeClass, parentSessionId: input.parentSessionId, pin: { ...input.pin },
    candidates, shadowChoice: { ...input.pin }, confidence: null, probability: null,
    reasonCode: "jev-error", latencyMs: 0,
  };
  if (input.protected === true) decision.reasonCode = "excluded-protected";
  else if (input.protected !== false) decision.reasonCode = "excluded-unknown";
  else if (input.routeClass !== "bounded-lookup") decision.reasonCode = "excluded-class";
  else if (input.candidatesValid === false) decision.reasonCode = "invalid-candidates";
  else {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ROUTE_DEADLINE_MS);
    const deadline = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
    const criteria = Object.fromEntries(candidates.map((candidate, index) => [`candidate-${index}`, { model: candidate.model, effort: candidate.effort }]));
    try {
      const response = await runAbortable(deadline, () => evaluate({
        state: { routeClass: input.routeClass, mode: "shadow", protection: "clear" },
        questions: { route: { type: "choice", instructions: "Choose the cheapest model and effort suitable for a bounded lookup with a checkable result. This is a shadow judgment, not permission to change the role pin.", criteria } },
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
          decision.reasonCode = "shadow-choice";
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

/** O_APPEND + fsync before launch: retained outside the ephemeral agent run directory. */
export function appendRouteRecord(file: string, record: object): void {
  const directory = path.dirname(file);
  const created = fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const fd = fs.openSync(file, "a", 0o600);
  try { fs.writeFileSync(fd, `${JSON.stringify(record)}\n`, "utf8"); fs.fsyncSync(fd); }
  finally { fs.closeSync(fd); }
  // File fsync alone need not persist its new directory entry. Flush newly created
  // ancestors too on hosts that support directory fsync (Windows flushes the file).
  if (process.platform !== "win32") {
    const stop = created ? path.dirname(created) : directory;
    for (let current = directory; ; current = path.dirname(current)) {
      const dirFd = fs.openSync(current, "r");
      try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); }
      if (current === stop) break;
    }
  }
}

export function prepareRouteDispatch(decision: ModelRouteDecision, cwd: string, runDirectory: string, childId: string): {
  header: string; sessionFile?: string; outcome: (result: Pick<AgentRunResult, "status"> & Partial<AgentRunResult>) => void;
} {
  const file = path.join(cwd, CONFIG_DIR_NAME, "fabric", "model-routing.jsonl");
  let sessionFile: string | undefined;
  try {
    // Seed a real native Pi session so the decision's child ID is not a guessed transport ID.
    fs.mkdirSync(runDirectory, { recursive: true, mode: 0o700 });
    const childSessionFile = path.join(runDirectory, "route-session.jsonl");
    fs.writeFileSync(childSessionFile, `${JSON.stringify({ type: "session", version: CURRENT_SESSION_VERSION,
      id: childId, timestamp: new Date().toISOString(), cwd })}\n`, { mode: 0o600 });
    sessionFile = childSessionFile;
    appendRouteRecord(file, { type: "decision", ...decision, childSessionId: childId, childAgentId: childId, at: Date.now() });
  } catch {
    decision.reasonCode = "record-failed";
    Object.assign(decision, decision.pin);
    // Shadow failures never block pinned work. The header still carries the failed record's ID.
  }
  let appended = false;
  return {
    header: routeHeader(decision), ...(sessionFile ? { sessionFile } : {}),
    outcome(result) {
      if (appended) return;
      appendRouteRecord(file, { type: "outcome", decisionId: decision.decisionId, childSessionId: childId,
        status: result.status, admittedModel: result.admittedModel ?? (result.status === "completed" ? result.model ?? null : null),
        admittedEffort: result.admittedThinking ?? (result.status === "completed" ? result.thinking ?? null : null),
        observedModel: result.model ?? null,
        tokens: result.usage ?? null, reasonCode: decision.reasonCode, at: Date.now() });
      appended = true;
    },
  };
}
