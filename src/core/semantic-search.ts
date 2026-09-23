import { createHash } from "node:crypto";
import type { JevRequest, JevResponse } from "../jev/types.js";
import { sanitizeMcpRefPart } from "../ref-names.js";
import { scoreActionSearch, type ResolvedFabricAction } from "./action-registry.js";

export const DEFAULT_SEMANTIC_CANDIDATE_LIMIT = 127;
export const DEFAULT_SEMANTIC_MIN_PROBABILITY = 0.2;
const DESCRIPTION_BYTES = 512;

export type SemanticSearchErrorCode =
  | "disabled"
  | "empty_query"
  | "invalid_search_mode"
  | "credential_missing"
  | "authentication_failed"
  | "invalid_response"
  | "timeout"
  | "aborted"
  | "rate_limited"
  | "service_unavailable";

export type SemanticSearchBackend =
  | {
      requested: "semantic";
      used: "semantic";
      degraded: false;
      model: string;
      usage: { inputTokens: number; outputTokens: number };
      abstained: boolean;
    }
  | {
      requested: "semantic";
      used: "lexical";
      degraded: true;
      reason: "timeout" | "rate_limited" | "service_unavailable";
    };

export type SemanticSearchResult =
  | { ok: true; actions: ResolvedFabricAction[]; backend: SemanticSearchBackend }
  | { ok: false; error: { code: SemanticSearchErrorCode; message: string } };

export interface SemanticSearchEvaluator {
  (request: JevRequest, signal: AbortSignal): Promise<JevResponse>;
}

interface Candidate {
  id: string;
  action: ResolvedFabricAction;
  description: string;
  server: string;
}

const truncateUtf8 = (value: string, maxBytes: number): string => {
  const bytes = Buffer.from(value, "utf8");
  if (bytes.length <= maxBytes) return value;
  let end = maxBytes;
  while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end -= 1;
  return bytes.subarray(0, end).toString("utf8");
};

const stableHash = (value: string): string =>
  createHash("sha256").update(value).digest("hex");

export const mcpServerOf = (action: ResolvedFabricAction): string | undefined => {
  if (action.provider !== "mcp") return undefined;
  if (action.name.startsWith("$")) return undefined;
  if (typeof action.namespace === "string" && action.namespace.length > 0) return action.namespace;
  const separator = action.name.indexOf(".");
  return separator > 0 ? action.name.slice(0, separator) : undefined;
};

const blockedSet = (blockedServers: readonly string[]): Set<string> => {
  const blocked = new Set<string>();
  for (const server of blockedServers) {
    const trimmed = server.trim();
    if (!trimmed) continue;
    blocked.add(trimmed);
    blocked.add(sanitizeMcpRefPart(trimmed));
  }
  return blocked;
};

export const isMcpServerBlocked = (
  server: string | undefined,
  blockedServers: readonly string[],
): boolean => {
  if (!server) return false;
  const blocked = blockedSet(blockedServers);
  return blocked.has(server) || blocked.has(sanitizeMcpRefPart(server));
};

const candidateServer = (action: ResolvedFabricAction): string =>
  mcpServerOf(action) ?? action.provider;

const rankLexical = (query: string, actions: ResolvedFabricAction[]): ResolvedFabricAction[] =>
  actions
    .map((action) => ({ action, score: scoreActionSearch(query, action) }))
    .filter((entry) => entry.score > 0)
    .sort(
      (left, right) =>
        right.score - left.score || left.action.ref.localeCompare(right.action.ref),
    )
    .map((entry) => entry.action);

const roundRobinNonLexical = (
  query: string,
  actions: ResolvedFabricAction[],
): ResolvedFabricAction[] => {
  const buckets = new Map<string, ResolvedFabricAction[]>();
  for (const action of actions) {
    const key = candidateServer(action);
    const bucket = buckets.get(key) ?? [];
    bucket.push(action);
    buckets.set(key, bucket);
  }
  const servers = [...buckets.keys()].sort((left, right) => left.localeCompare(right));
  for (const bucket of buckets.values()) {
    bucket.sort(
      (left, right) =>
        stableHash(query + left.ref).localeCompare(stableHash(query + right.ref)) ||
        left.ref.localeCompare(right.ref),
    );
  }
  if (servers.length > 1) {
    const start = Number.parseInt(stableHash(query).slice(0, 8), 16) % servers.length;
    servers.push(...servers.splice(0, start));
  }
  const result: ResolvedFabricAction[] = [];
  let index = 0;
  while (result.length < actions.length) {
    let added = false;
    for (const server of servers) {
      const match = buckets.get(server)?.[index];
      if (match) {
        result.push(match);
        added = true;
      }
    }
    if (!added) break;
    index += 1;
  }
  return result;
};

export const selectSemanticCandidates = (
  query: string,
  actions: ResolvedFabricAction[],
  blockedServers: readonly string[],
  limit: number,
): Candidate[] => {
  const eligible = actions.filter((action) => !isMcpServerBlocked(mcpServerOf(action), blockedServers));
  let selected = eligible;
  if (eligible.length > limit) {
    const lexical = rankLexical(query, eligible);
    const lexicalCount = Math.floor(limit / 2);
    const first = lexical.slice(0, lexicalCount);
    const lexicalRefs = new Set(lexical.map((action) => action.ref));
    const broad = roundRobinNonLexical(
      query,
      eligible.filter((action) => !lexicalRefs.has(action.ref)),
    );
    selected = [...first, ...broad.slice(0, limit - first.length)];
  }
  return selected.map((action, index) => ({
    id: `c${index}`,
    action,
    description: truncateUtf8(action.description ?? "", DESCRIPTION_BYTES),
    server: candidateServer(action),
  }));
};

export const classifyJevSearchFailure = (error: unknown): SemanticSearchResult => {
  const message = error instanceof Error ? error.message : String(error);
  if (/cancelled or timed out/i.test(message)) {
    return { ok: false, error: { code: "timeout", message } };
  }
  if (/\baborted\b/i.test(message)) {
    return { ok: false, error: { code: "aborted", message } };
  }
  if (/rate limited/i.test(message) || /HTTP 429/.test(message) || /HTTP 529/.test(message)) {
    return { ok: false, error: { code: "rate_limited", message } };
  }
  if (
    /HTTP 5\d\d/.test(message) ||
    /network request failed/i.test(message) ||
    /empty response/i.test(message)
  ) {
    return { ok: false, error: { code: "service_unavailable", message } };
  }
  if (/credential/i.test(message)) {
    return { ok: false, error: { code: "credential_missing", message } };
  }
  if (/HTTP 401/.test(message) || /HTTP 403/.test(message) || /authentication/i.test(message)) {
    return { ok: false, error: { code: "authentication_failed", message } };
  }
  return { ok: false, error: { code: "invalid_response", message } };
};

const lexicalFallback = (
  query: string,
  actions: ResolvedFabricAction[],
  reason: "timeout" | "rate_limited" | "service_unavailable",
): SemanticSearchResult => ({
  ok: true,
  actions: rankLexical(query, actions),
  backend: { requested: "semantic", used: "lexical", degraded: true, reason },
});

export const semanticSearchActions = async (options: {
  query: string;
  actions: ResolvedFabricAction[];
  blockedServers?: readonly string[];
  candidateLimit?: number;
  minProbability?: number;
  signal: AbortSignal;
  evaluate: SemanticSearchEvaluator;
}): Promise<SemanticSearchResult> => {
  const query = options.query.normalize("NFKC").trim();
  if (!query) {
    return { ok: false, error: { code: "empty_query", message: "Semantic search query cannot be empty." } };
  }
  const blockedServers = options.blockedServers ?? [];
  const limit = Math.max(
    2,
    Math.min(Math.floor(options.candidateLimit ?? DEFAULT_SEMANTIC_CANDIDATE_LIMIT), DEFAULT_SEMANTIC_CANDIDATE_LIMIT),
  );
  const minProbability = options.minProbability ?? DEFAULT_SEMANTIC_MIN_PROBABILITY;
  const candidates = selectSemanticCandidates(query, options.actions, blockedServers, limit);
  if (candidates.length === 0) {
    return {
      ok: true,
      actions: [],
      backend: {
        requested: "semantic",
        used: "semantic",
        degraded: false,
        model: "",
        usage: { inputTokens: 0, outputTokens: 0 },
        abstained: true,
      },
    };
  }
  const request: JevRequest = {
    state: {
      query,
      candidates: candidates.map((candidate) => ({
        id: candidate.id,
        path: candidate.action.ref,
        name: candidate.action.name,
        server: candidate.server,
        description: candidate.description,
      })),
    },
    questions: {
      match: {
        type: "choice",
        instructions: "Rank which tool best matches the query. Choose none when no tool is suitable.",
        criteria: Object.fromEntries([
          ...candidates.map((candidate) => [candidate.id, { path: candidate.action.ref }]),
          ["none", { noSuitableTool: true }],
        ]),
      },
    },
  };
  let response: JevResponse;
  try {
    response = await options.evaluate(request, options.signal);
  } catch (error) {
    const failure = classifyJevSearchFailure(error);
    if (
      failure.ok === false &&
      (failure.error.code === "timeout" ||
        failure.error.code === "rate_limited" ||
        failure.error.code === "service_unavailable")
    ) {
      return lexicalFallback(query, options.actions, failure.error.code);
    }
    return failure;
  }
  const answer = response.answers.match;
  if (!answer || answer.type !== "choice") {
    return { ok: false, error: { code: "invalid_response", message: "Jev returned an invalid semantic search response." } };
  }
  const ranked = candidates
    .map((candidate) => ({
      candidate,
      probability: answer.probabilities[candidate.id] ?? 0,
    }))
    .sort(
      (left, right) =>
        right.probability - left.probability || left.candidate.id.localeCompare(right.candidate.id),
    );
  const top = ranked[0];
  const noneProbability = answer.probabilities.none ?? 0;
  const abstained =
    answer.choice === "none" ||
    !top ||
    noneProbability >= top.probability ||
    top.probability < minProbability;
  return {
    ok: true,
    actions: abstained ? [] : ranked.map(({ candidate }) => candidate.action),
    backend: {
      requested: "semantic",
      used: "semantic",
      degraded: false,
      model: response.model,
      usage: {
        inputTokens: response.usage.input_tokens,
        outputTokens: response.usage.output_tokens,
      },
      abstained,
    },
  };
};
