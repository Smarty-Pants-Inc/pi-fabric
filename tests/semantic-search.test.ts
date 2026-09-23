import { describe, expect, it, vi } from "vitest";
import type { JevRequest, JevResponse } from "../src/jev/types.js";
import type { ResolvedFabricAction } from "../src/core/action-registry.js";
import {
  classifyJevSearchFailure,
  selectSemanticCandidates,
  semanticSearchActions,
  type SemanticSearchEvaluator,
} from "../src/core/semantic-search.js";

const action = (
  ref: string,
  description: string,
  extra: Partial<ResolvedFabricAction> = {},
): ResolvedFabricAction => {
  const [provider, ...rest] = ref.split(".");
  const name = rest.join(".") || ref;
  return {
    name,
    description,
    inputSchema: { type: "object", properties: {} },
    risk: "read",
    ref,
    provider: provider ?? "demo",
    ...extra,
  };
};

const mcpAction = (
  server: string,
  tool: string,
  description: string,
): ResolvedFabricAction =>
  action(`mcp.${server}.${tool}`, description, {
    provider: "mcp",
    namespace: server,
    name: `${server}.${tool}`,
    risk: "network",
  });

const actions = (): ResolvedFabricAction[] => [
  mcpAction("demo", "tool_0", "Search invoices"),
  mcpAction("demo", "tool_1", "Unrelated capability 1"),
  mcpAction("demo", "tool_2", "Unrelated capability 2"),
  mcpAction("other", "weather", "Forecast conditions"),
];

const choiceEvaluator = (
  selectRef: string | "none",
  scores?: Record<string, number>,
): SemanticSearchEvaluator =>
  vi.fn(async (request): Promise<JevResponse> => {
    const candidates = (request.state as { candidates: Array<{ id: string; path: string }> }).candidates;
    const labels = [...candidates.map((candidate) => candidate.id), "none"];
    const selected =
      selectRef === "none"
        ? "none"
        : candidates.find((candidate) => candidate.path === selectRef)?.id ?? "none";
    const probabilities = Object.fromEntries(
      labels.map((label) => [label, scores?.[label] ?? (label === selected ? 0.8 : 0.01)]),
    );
    return {
      model: "jev-test",
      answers: {
        match: { type: "choice", choice: selected, confidence: 0.9, probabilities },
      },
      usage: { input_tokens: 12, output_tokens: 3 },
    };
  });

describe("semantic action search", () => {
  it("ranks by probabilities and omits schemas from Jev state", async () => {
    const evaluator = choiceEvaluator("mcp.other.weather");
    const result = await semanticSearchActions({
      query: "umbrella planning",
      actions: actions(),
      signal: new AbortController().signal,
      evaluate: evaluator,
    });
    expect(result).toMatchObject({
      ok: true,
      backend: { requested: "semantic", used: "semantic", degraded: false, abstained: false, model: "jev-test" },
    });
    if (!result.ok) throw new Error("expected ok");
    expect(result.actions[0]?.ref).toBe("mcp.other.weather");
    const sent = vi.mocked(evaluator).mock.calls[0]![0] as JevRequest;
    const candidates = (sent.state as { candidates: Array<Record<string, unknown>> }).candidates;
    expect(candidates.every((candidate) => /^c\d+$/.test(String(candidate.id)))).toBe(true);
    expect(JSON.stringify(candidates)).not.toContain("inputSchema");
  });

  it("keeps blocked MCP servers out of Jev state", async () => {
    const evaluator = choiceEvaluator("none");
    await semanticSearchActions({
      query: "anything",
      actions: actions(),
      blockedServers: ["other"],
      signal: new AbortController().signal,
      evaluate: evaluator,
    });
    const sent = vi.mocked(evaluator).mock.calls[0]![0] as JevRequest;
    const candidates = (sent.state as { candidates: Array<{ server: string }> }).candidates;
    expect(candidates.map((candidate) => candidate.server)).not.toContain("other");
    expect(candidates.some((candidate) => candidate.server === "demo")).toBe(true);
  });

  it("allows uncached future MCP servers unless they are blocked", async () => {
    const evaluator = choiceEvaluator("mcp.upcoming.ping");
    const result = await semanticSearchActions({
      query: "ping upcoming",
      actions: [...actions(), mcpAction("upcoming", "ping", "Health check")],
      blockedServers: [],
      signal: new AbortController().signal,
      evaluate: evaluator,
    });
    if (!result.ok) throw new Error("expected ok");
    expect(result.actions[0]?.ref).toBe("mcp.upcoming.ping");
  });

  it("caps at 127 and reserves a deterministic nonlexical recovery lane", async () => {
    const many = Array.from({ length: 140 }, (_, index) =>
      mcpAction("demo", `tool_${index}`, index < 70 ? `needle match ${index}` : `Unrelated ${index}`),
    );
    const sent: Array<Array<{ path: string; description: string }>> = [];
    const inspect: SemanticSearchEvaluator = async (request) => {
      sent.push((request.state as { candidates: Array<{ path: string; description: string }> }).candidates);
      return choiceEvaluator("none")(request, new AbortController().signal);
    };
    await semanticSearchActions({
      query: "needle",
      actions: many,
      candidateLimit: 127,
      signal: new AbortController().signal,
      evaluate: inspect,
    });
    await semanticSearchActions({
      query: "needle",
      actions: many,
      candidateLimit: 127,
      signal: new AbortController().signal,
      evaluate: inspect,
    });
    expect(sent[0]).toHaveLength(127);
    expect(sent[0]!.map((candidate) => candidate.path)).toEqual(sent[1]!.map((candidate) => candidate.path));
    expect(sent[0]!.slice(63).some((candidate) => !candidate.description.includes("needle"))).toBe(true);
  });

  it("abstains for none or below-threshold probability", async () => {
    const ranked = await semanticSearchActions({
      query: "query",
      actions: actions(),
      signal: new AbortController().signal,
      evaluate: choiceEvaluator("mcp.demo.tool_0", { c0: 0.3, c1: 0.7, c2: 0.2, c3: 0.1, none: 0.05 }),
    });
    if (!ranked.ok) throw new Error("expected ok");
    expect(ranked.actions.map((entry) => entry.ref).slice(0, 2)).toEqual([
      "mcp.demo.tool_1",
      "mcp.demo.tool_0",
    ]);
    const none = await semanticSearchActions({
      query: "query",
      actions: actions(),
      signal: new AbortController().signal,
      evaluate: choiceEvaluator("none"),
    });
    expect(none).toMatchObject({ ok: true, actions: [], backend: { abstained: true } });
    const below = await semanticSearchActions({
      query: "query",
      actions: actions(),
      minProbability: 0.9,
      signal: new AbortController().signal,
      evaluate: choiceEvaluator("mcp.demo.tool_0"),
    });
    expect(below).toMatchObject({ ok: true, actions: [], backend: { abstained: true } });
  });

  it("degrades only availability failures", async () => {
    for (const [message, reason] of [
      ["Jev request cancelled or timed out", "timeout"],
      ["TypeSafe HTTP 429: rate limited; back off before retrying", "rate_limited"],
      ["Jev network request failed", "service_unavailable"],
    ] as const) {
      const result = await semanticSearchActions({
        query: "invoices",
        actions: actions(),
        signal: new AbortController().signal,
        evaluate: async () => {
          throw new Error(message);
        },
      });
      expect(result).toMatchObject({
        ok: true,
        backend: { requested: "semantic", used: "lexical", degraded: true, reason },
      });
      if (!result.ok) throw new Error("expected ok");
      expect(result.actions[0]?.ref).toBe("mcp.demo.tool_0");
    }
    const hard = await semanticSearchActions({
      query: "invoices",
      actions: actions(),
      signal: new AbortController().signal,
      evaluate: async () => {
        throw new Error("Jev credentials unavailable: set TYPESAFE_API_KEY");
      },
    });
    expect(hard).toMatchObject({ ok: false, error: { code: "credential_missing" } });
  });

  it("rejects an empty semantic query without evaluating", async () => {
    const evaluator = choiceEvaluator("none");
    const result = await semanticSearchActions({
      query: "   ",
      actions: actions(),
      signal: new AbortController().signal,
      evaluate: evaluator,
    });
    expect(result).toMatchObject({ ok: false, error: { code: "empty_query" } });
    expect(evaluator).not.toHaveBeenCalled();
  });

  it("classifies Jev failures without leaking response bodies", () => {
    expect(classifyJevSearchFailure(new Error("TypeSafe HTTP 401")).ok).toBe(false);
    expect(selectSemanticCandidates("needle", actions(), ["demo"], 10).every((candidate) => candidate.server !== "demo")).toBe(true);
  });
});
