// Per-call model routing, SHADOW ONLY (smarty-dev#2890, #6062). Loaded on first use when
// agents.modelRouting.perCall.mode is "shadow"; never imported at startup or when "off".
// It decides SIMPLE or MAIN for each LLM call and appends one ledger line per call. It has no
// handle on Pi's ExtensionAPI and never switches the model: the call runs exactly as without it.
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { resolveAgentDir } from "../core/agent-dir.js";
import type { JevClient } from "../jev/client.js";
import type { FabricJevConfig } from "../jev/config.js";
import type { JevRequest, JevResponse } from "../jev/types.js";
import { appendRouteRecord, ROUTE_DEADLINE_MS, ROUTE_THRESHOLD } from "./model-route.js";
import type { PerCallRoutingConfig } from "./per-call-config.js";

/** The gateway use that budgets, validates and meters these questions (see the PR notes). */
export const PER_CALL_GATEWAY_USE = "percall_route";
export const PER_CALL_LEDGER = "per-call-routing.jsonl";
export const PER_CALL_RETENTION_DAYS = 7;
const CACHE_LIMIT = 256;

export type PerCallDecision = "simple" | "main";
export type PerCallRule =
  | "user-message" | "no-tool-results" | "author-phase" | "context-over-limit" | "read-only-tools"
  | "ambiguous-jev-off" | "ambiguous-jev-budget" | "ambiguous-jev-error" | "ambiguous-jev-timeout"
  | "jev-noul" | "jev-cache";
export interface PerCallSettings { perCall?: PerCallRoutingConfig | undefined; jev: FabricJevConfig }

interface ToolCallLike { type?: unknown; name?: unknown; arguments?: unknown }
interface MessageLike {
  role?: unknown; content?: unknown; provider?: unknown; model?: unknown; isError?: unknown;
  usage?: { input?: unknown; output?: unknown; cacheRead?: unknown; cacheWrite?: unknown; cacheWrite1h?: unknown; totalTokens?: unknown; cost?: { total?: unknown } };
  stopReason?: unknown; responseModel?: unknown;
}

/** Facts about the upcoming call, read from the transcript Pi is about to send. No text is kept. */
export interface StepFacts {
  /** The messages after the last assistant message are all tool results (no new user input). */
  toolResultsOnly: boolean;
  /** What follows the last assistant message when it is not tool results only. */
  newInput: "user" | "other" | "none";
  /** 1 = the previous assistant turn used an edit/write tool; null = none in the window. */
  authorTurnsAgo: number | null;
  contextTokens: number;
  /** Sorted unique operation labels of the previous assistant turn's tool calls. */
  ops: string[];
  /** Every tool call of the previous turn was a read, list or search. */
  readOnly: boolean;
  toolCalls: number;
  errorResults: number;
}

const AUTHOR_TOOLS = new Set(["edit", "write", "multiedit", "multi_edit", "apply_patch", "notebook_edit", "notebookedit",
  "str_replace_editor", "str_replace_based_edit_tool"]);
const READ_TOOLS = new Set(["read", "grep", "find", "ls", "glob"]);
const CODE_READ_OPS = new Set(["read", "grep", "find", "ls"]);
const CODE_AUTHOR_OPS = new Set(["edit", "write"]);
const CODE_OTHER = /\b(?:agents|mcp|extensions|tools|state|memory|mesh|jev|schema|compact|cache)\.[A-Za-z_$]/;

/** One tool call's operation labels. fabric_exec programs are read for their `pi.*` calls only. */
export function toolCallOps(call: ToolCallLike): { ops: string[]; author: boolean; readOnly: boolean } {
  const name = typeof call.name === "string" ? call.name.toLowerCase() : "unknown";
  if (name === "fabric_exec" || name.endsWith("__fabric_exec") || name.endsWith("_fabric_exec")) {
    const args = call.arguments && typeof call.arguments === "object" ? call.arguments as Record<string, unknown> : {};
    const code = typeof args.code === "string" ? args.code : "";
    const piOps = [...new Set([...code.matchAll(/\bpi\.([A-Za-z_]+)\s*\(/g)].map(match => match[1]!.toLowerCase()))].sort();
    const other = CODE_OTHER.test(code);
    const ops = [...piOps.map(op => `pi.${op}`), ...(other ? ["fabric:other"] : [])];
    return {
      ops: ops.length ? ops : ["fabric:none"],
      author: piOps.some(op => CODE_AUTHOR_OPS.has(op)),
      readOnly: piOps.length > 0 && !other && piOps.every(op => CODE_READ_OPS.has(op)),
    };
  }
  return { ops: [name], author: AUTHOR_TOOLS.has(name), readOnly: READ_TOOLS.has(name) };
}

const toolCalls = (message: MessageLike): ToolCallLike[] =>
  Array.isArray(message.content) ? (message.content as ToolCallLike[]).filter(part => part?.type === "toolCall") : [];

export function stepFacts(messages: readonly unknown[], contextTokens: number, authorTurns: number): StepFacts {
  const list = messages as readonly MessageLike[];
  let last = -1;
  for (let i = list.length - 1; i >= 0; i--) if (list[i]?.role === "assistant") { last = i; break; }
  const trailing = list.slice(last + 1);
  const toolResultsOnly = last >= 0 && trailing.length > 0 && trailing.every(message => message?.role === "toolResult");
  const newInput = toolResultsOnly ? "none" : trailing.some(message => message?.role === "user") || last < 0 ? "user" : "other";
  let authorTurnsAgo: number | null = null;
  for (let i = last, seen = 0; i >= 0 && seen < authorTurns; i--) {
    if (list[i]?.role !== "assistant") continue;
    seen++;
    if (toolCalls(list[i]!).some(call => toolCallOps(call).author)) { authorTurnsAgo = seen; break; }
  }
  const calls = last >= 0 ? toolCalls(list[last]!) : [];
  const perCall = calls.map(toolCallOps);
  return {
    toolResultsOnly, newInput, authorTurnsAgo, contextTokens,
    ops: [...new Set(perCall.flatMap(call => call.ops))].sort(),
    readOnly: perCall.length > 0 && perCall.every(call => call.readOnly),
    toolCalls: calls.length,
    errorResults: trailing.filter(message => message?.role === "toolResult" && message.isError === true).length,
  };
}

/** The zero-cost local rules, in order. `null` decision = ambiguous: only these steps may ask Jev. */
export function classifyStep(facts: StepFacts, config: Pick<PerCallRoutingConfig, "maxContextTokens">):
  { decision: PerCallDecision; rule: PerCallRule } | { decision: null; rule: "ambiguous" } {
  if (!facts.toolResultsOnly) return { decision: "main", rule: facts.newInput === "user" ? "user-message" : "no-tool-results" };
  if (facts.authorTurnsAgo !== null) return { decision: "main", rule: "author-phase" };
  if (facts.contextTokens > config.maxContextTokens) return { decision: "main", rule: "context-over-limit" };
  if (facts.readOnly && facts.errorResults === 0) return { decision: "simple", rule: "read-only-tools" };
  return { decision: null, rule: "ambiguous" };
}

const contextBucket = (tokens: number): number => tokens <= 1000 ? 0 : Math.ceil(Math.log2(tokens / 1000));

/** The cache key and Jev state: operation labels and counts only, never transcript text. */
export function stepShape(facts: StepFacts): { key: string; state: { [key: string]: string | number | boolean | string[] } } {
  const state = {
    step: "next-call-after-tool-results",
    ops: facts.ops.slice(0, 16),
    toolCalls: Math.min(facts.toolCalls, 8),
    failedResults: facts.errorResults > 0,
    contextKTokensUpTo: 2 ** contextBucket(facts.contextTokens),
  };
  return { key: createHash("sha256").update(JSON.stringify(state)).digest("hex").slice(0, 16), state };
}

export const PER_CALL_QUESTION = "A coding agent's previous model call ran the tool operations listed in the state; their results are "
  + "now in context and no new user message arrived. Only operation labels and counts are given. Is the NEXT model "
  + "call simple enough for a smaller model: digesting these results, choosing an obvious next read, list or status "
  + "call, or summarizing, rather than code authoring, debugging or real reasoning? Answer false when the labels do "
  + "not establish simplicity.";

export function perCallJevRequest(facts: StepFacts, model?: string): JevRequest {
  return {
    ...(model ? { model } : {}),
    state: stepShape(facts).state,
    questions: { simple: { type: "noul", instructions: PER_CALL_QUESTION,
      criteria: { true: "simple: digest results or make an obvious read/list/status call", false: "not simple or not established: keep the main model" } } },
  };
}

const finite = (value: unknown): number => typeof value === "number" && Number.isFinite(value) ? value : 0;
const modelKey = (message: MessageLike | undefined): string | null =>
  message && typeof message.provider === "string" && typeof message.model === "string" ? `${message.provider}/${message.model}` : null;

/** A conservative estimate (characters / 4) when Pi has no usage-based context figure yet. */
export function estimateContextTokens(messages: readonly unknown[]): number {
  let chars = 0;
  for (const message of messages) { try { chars += JSON.stringify(message)?.length ?? 0; } catch { /* unserializable part */ } }
  return Math.ceil(chars / 4);
}

const utcDay = (date: Date): string => date.toISOString().slice(0, 10);
const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** Daily rotation by the active file's last-write UTC day; rotated files older than `keepDays` are removed. */
export function rotatePerCallLedger(file: string, now: Date, keepDays = PER_CALL_RETENTION_DAYS, prune = true): boolean {
  const directory = path.dirname(file);
  const base = path.basename(file, ".jsonl");
  let rotated = false;
  let stat: fs.Stats | undefined;
  try { stat = fs.lstatSync(file); } catch { stat = undefined; }
  if (stat?.isFile() && utcDay(stat.mtime) !== utcDay(now)) {
    const day = utcDay(stat.mtime);
    let target = path.join(directory, `${base}.${day}.jsonl`);
    for (let n = 1; fs.existsSync(target); n++) target = path.join(directory, `${base}.${day}.${n}.jsonl`);
    fs.renameSync(file, target);
    rotated = true;
  }
  if (!prune && !rotated) return false;
  const cutoff = utcDay(new Date(now.getTime() - keepDays * 86_400_000));
  const pattern = new RegExp(`^${escapeRegExp(base)}\\.(\\d{4}-\\d{2}-\\d{2})(?:\\.\\d+)?\\.jsonl$`);
  let names: string[] = [];
  try { names = fs.readdirSync(directory); } catch { /* nothing to prune */ }
  for (const name of names) {
    const match = pattern.exec(name);
    if (match && match[1]! <= cutoff) {
      try { if (fs.lstatSync(path.join(directory, name)).isFile()) fs.unlinkSync(path.join(directory, name)); } catch { /* best effort */ }
    }
  }
  return rotated;
}

export interface PerCallUsage { input: number; output: number; cacheRead: number; cacheWrite: number; cacheWrite1h?: number; totalTokens: number; costUsd: number }
export interface PerCallRecord {
  v: 1; type: "per-call"; mode: "shadow"; at: string;
  sessionId: string; turnIndex: number; callIndex: number;
  decision: PerCallDecision; decidedBy: "rule" | "jev"; rule: PerCallRule;
  stepShape: string | null; jev: { noul: number; cached: boolean; latencyMs: number } | null;
  contextTokens: number; contextTokensSource: "pi-usage" | "estimate"; maxContextTokens: number;
  model: string | null; previousCallModel: string | null; sameAsPrevious: boolean | null;
  responseModel: string | null; stopReason: string | null;
  /** Provider usage of THIS call, from the assistant message; null if the call produced none. */
  usage: PerCallUsage | null;
}

export interface PerCallContextSnapshot {
  sessionId: string; turnIndex: number; messages: readonly unknown[]; model: string | null; contextTokens: number | null;
}
export type PerCallEvaluate = (request: JevRequest, signal: AbortSignal) => Promise<JevResponse>;
export interface PerCallRouterOptions {
  settings: () => PerCallSettings | undefined;
  /** Ledger path; defaults to <agentDir>/fabric/per-call-routing.jsonl at write time. */
  ledger?: string;
  /** Test seam; production builds Fabric's JevClient (gateway transport when jev.gatewaySocket is set). */
  evaluate?: PerCallEvaluate;
  now?: () => Date;
}
type Verdict = Pick<PerCallRecord, "decision" | "decidedBy" | "rule" | "jev">;
interface Pending { record: Omit<PerCallRecord, keyof Verdict | "usage" | "responseModel" | "stopReason">; verdict: Promise<Verdict> }

export class PerCallShadowRouter {
  readonly #pending = new Map<string, Pending>();
  readonly #tracked = new Set<Promise<void>>();
  readonly #cache = new Map<string, number>();
  readonly #inflight = new Map<string, Promise<number>>();
  readonly #jevCalls = new Map<string, number>();
  readonly #callIndex = new Map<string, number>();
  readonly #abort = new AbortController();
  #writes: Promise<void> = Promise.resolve();
  #client: Promise<JevClient> | undefined;
  #pruned = false;
  /** Jev questions actually sent (cache hits and refusals excluded); for tests and diagnostics. */
  jevQuestions = 0;
  constructor(readonly options: PerCallRouterOptions) {}

  /** Before each LLM call. Synchronous rules; a Jev question for an ambiguous step runs off the call path. */
  onContext(snapshot: PerCallContextSnapshot): void {
    const settings = this.options.settings();
    const config = settings?.perCall;
    if (!settings || config?.mode !== "shadow") return;
    this.#close(snapshot.sessionId);
    const known = snapshot.contextTokens !== null && Number.isFinite(snapshot.contextTokens) && snapshot.contextTokens >= 0;
    const contextTokens = known ? Math.round(snapshot.contextTokens!) : estimateContextTokens(snapshot.messages);
    const facts = stepFacts(snapshot.messages, contextTokens, config.authorTurns);
    const ruled = classifyStep(facts, config);
    const list = snapshot.messages as readonly MessageLike[];
    let previous: MessageLike | undefined;
    for (let i = list.length - 1; i >= 0; i--) if (list[i]?.role === "assistant") { previous = list[i]; break; }
    const previousCallModel = modelKey(previous);
    const callIndex = (this.#callIndex.get(snapshot.sessionId) ?? 0) + 1;
    this.#callIndex.set(snapshot.sessionId, callIndex);
    const shape = ruled.decision === null ? stepShape(facts) : undefined;
    this.#pending.set(snapshot.sessionId, {
      record: { v: 1, type: "per-call", mode: "shadow", at: (this.options.now?.() ?? new Date()).toISOString(),
        sessionId: snapshot.sessionId, turnIndex: snapshot.turnIndex, callIndex,
        stepShape: shape?.key ?? null, contextTokens, contextTokensSource: known ? "pi-usage" : "estimate",
        maxContextTokens: config.maxContextTokens, model: snapshot.model, previousCallModel,
        sameAsPrevious: previousCallModel === null || snapshot.model === null ? null : previousCallModel === snapshot.model },
      verdict: ruled.decision !== null
        ? Promise.resolve({ decision: ruled.decision, decidedBy: "rule", rule: ruled.rule, jev: null })
        : this.#askJev(snapshot.sessionId, facts, shape!.key, settings, config),
    });
  }

  /** After the call: attach the provider usage and write the line. */
  onMessageEnd(sessionId: string, message: unknown): void {
    const value = message as MessageLike | undefined;
    if (value?.role !== "assistant") return;
    this.#close(sessionId, value);
  }

  #close(sessionId: string, message?: MessageLike): void {
    const pending = this.#pending.get(sessionId);
    if (!pending) return;
    this.#pending.delete(sessionId);
    const usage = message?.usage;
    const tail = {
      responseModel: message ? (typeof message.responseModel === "string" ? message.responseModel : modelKey(message)) : null,
      stopReason: typeof message?.stopReason === "string" ? message.stopReason : null,
      usage: usage ? {
        input: finite(usage.input), output: finite(usage.output), cacheRead: finite(usage.cacheRead), cacheWrite: finite(usage.cacheWrite),
        ...(typeof usage.cacheWrite1h === "number" ? { cacheWrite1h: finite(usage.cacheWrite1h) } : {}),
        totalTokens: finite(usage.totalTokens), costUsd: finite(usage.cost?.total),
      } : null,
    };
    this.#track(this.#append(pending.verdict.then(verdict => ({ ...pending.record, ...verdict, ...tail }))));
  }

  #track(work: Promise<void>): void {
    const settled = work.catch(() => undefined);
    this.#tracked.add(settled);
    void settled.then(() => this.#tracked.delete(settled));
  }

  /** Appends in call order (a Jev verdict is bounded by ROUTE_DEADLINE_MS); a ledger failure is
   * dropped: shadow logging never affects the session. */
  #append(pending: Promise<PerCallRecord>): Promise<void> {
    this.#writes = this.#writes.then(async () => {
      let record: PerCallRecord;
      try { record = await pending; } catch { return; }
      try {
        const file = this.options.ledger ?? path.join(resolveAgentDir(), "fabric", PER_CALL_LEDGER);
        const now = this.options.now?.() ?? new Date();
        fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
        // Prune on the first write of this process and whenever the day turns over.
        rotatePerCallLedger(file, now, PER_CALL_RETENTION_DAYS, !this.#pruned);
        this.#pruned = true;
        appendRouteRecord(file, record);
      } catch { /* shadow only */ }
    });
    return this.#writes;
  }

  async #askJev(sessionId: string, facts: StepFacts, key: string, settings: PerCallSettings, config: PerCallRoutingConfig): Promise<Verdict> {
    const fallback = (rule: PerCallRule): Verdict => ({ decision: "main", decidedBy: "rule", rule, jev: null });
    const verdict = (noul: number, cached: boolean, latencyMs: number): Verdict => ({
      decision: noul >= ROUTE_THRESHOLD ? "simple" : "main", decidedBy: "jev", rule: cached ? "jev-cache" : "jev-noul",
      jev: { noul, cached, latencyMs } });
    const cached = this.#cache.get(key);
    if (cached !== undefined) {
      this.#cache.delete(key); this.#cache.set(key, cached);
      return verdict(cached, true, 0);
    }
    const evaluate = this.#evaluator(settings, config);
    if (!evaluate) return fallback("ambiguous-jev-off");
    const started = performance.now();
    const inflight = this.#inflight.get(key);
    if (inflight) {
      try { return verdict(await inflight, true, Math.round(performance.now() - started)); } catch { return fallback("ambiguous-jev-error"); }
    }
    const used = this.#jevCalls.get(sessionId) ?? 0;
    if (used >= config.jevMaxCallsPerSession) return fallback("ambiguous-jev-budget");
    this.#jevCalls.set(sessionId, used + 1);
    this.jevQuestions++;
    const deadline = AbortSignal.any([this.#abort.signal, AbortSignal.timeout(ROUTE_DEADLINE_MS)]);
    const asking = (async () => {
      const response = await evaluate(perCallJevRequest(facts, settings.jev.model), deadline);
      const answer = response?.answers?.simple;
      if (answer?.type !== "noul" || !Number.isFinite(answer.noul) || answer.noul < 0 || answer.noul > 1) throw new Error("malformed");
      return answer.noul;
    })();
    this.#inflight.set(key, asking);
    try {
      const noul = await asking;
      this.#cache.set(key, noul);
      while (this.#cache.size > CACHE_LIMIT) this.#cache.delete(this.#cache.keys().next().value!);
      return verdict(noul, false, Math.round(performance.now() - started));
    } catch (error) {
      return fallback(deadline.aborted || (error instanceof Error && /timed out|timeout/i.test(error.message)) ? "ambiguous-jev-timeout" : "ambiguous-jev-error");
    } finally { this.#inflight.delete(key); }
  }

  #evaluator(settings: PerCallSettings, config: PerCallRoutingConfig): PerCallEvaluate | undefined {
    if (config.jev === "off" || !settings.jev.enabled || config.jevMaxCallsPerSession === 0) return undefined;
    if (this.options.evaluate) return this.options.evaluate;
    // "gateway" never falls back to a direct, key-holding client.
    if (config.jev === "gateway" && !settings.jev.gatewaySocket) return undefined;
    const jev = settings.jev;
    return async (request, signal) => {
      const client = await (this.#client ??= (async () => {
        const [{ JevClient }, { resolveJevModelRoute }] = await Promise.all([import("../jev/client.js"), import("../jev/routes.js")]);
        return new JevClient(jev, undefined, undefined, resolveJevModelRoute(jev.model).route);
      })());
      if (config.jev === "gateway" && !client.viaGateway) throw new Error("Jev gateway not configured");
      return client.evaluate(request, signal, { use: PER_CALL_GATEWAY_USE });
    };
  }

  /** Writes every open call (without usage), waits for pending lines, and stops Jev questions. */
  async close(): Promise<void> {
    for (const sessionId of [...this.#pending.keys()]) this.#close(sessionId);
    const bound = new Promise<void>(resolve => { const t = setTimeout(resolve, ROUTE_DEADLINE_MS + 500); t.unref?.(); });
    await Promise.race([Promise.allSettled([...this.#tracked]).then(() => this.#writes), bound]);
    this.#abort.abort();
    const client = await this.#client?.catch(() => undefined);
    await client?.drainCredentials();
  }
}
