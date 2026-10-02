import { describe, expect, it } from "vitest";
import { normalizeAgentRunRequest } from "../src/agents/request.js";

const defaults = {
  runner: "pi" as const,
  timeoutMs: 60_000,
  models: {
    aliases: {
      shallow: { targets: ["google/gemini-2.5-flash"], thinking: "low" as const },
      flat: { targets: ["openai/gpt-5-mini"] },
    },
  },
};

describe("parent run inheritance (#2490)", () => {
  const parent = { ...defaults, model: "shallow", inheritedModel: { provider: "cliproxyapi", id: "gpt-6.1-sol" }, inheritedThinking: "max" as const };
  it("inherits the actual Pi model and effort ahead of configured defaults", () => {
    expect(normalizeAgentRunRequest({ task: "review" }, parent)).toMatchObject({ model: "cliproxyapi/gpt-6.1-sol", thinking: "max" });
  });
  it("lets an explicit model or effort override inheritance independently", () => {
    expect(normalizeAgentRunRequest({ task: "review", thinking: "high" }, parent)).toMatchObject({ model: "cliproxyapi/gpt-6.1-sol", thinking: "high" });
    expect(normalizeAgentRunRequest({ task: "review", model: "shallow" }, parent)).toMatchObject({ model: "shallow", thinking: "low" });
  });
  it("does not forward a Pi parent binding to another runner", () => {
    const request = normalizeAgentRunRequest({ task: "review", runner: "claude" }, parent);
    expect(request.model).toBeUndefined();
    expect(request.thinking).toBeUndefined();
  });
});

describe("resident retry keys", () => {
  it("preserves caller-supplied keys through run request normalization", () => {
    expect(normalizeAgentRunRequest({ task: "t", residency: "durable", idempotencyKey: "retry-1" }, defaults))
      .toMatchObject({ residency: "durable", idempotencyKey: "retry-1" });
    expect(normalizeAgentRunRequest({ task: "t" }, defaults)).not.toHaveProperty("idempotencyKey");
  });
});

describe("alias thinking levels", () => {
  it("applies an alias default when the run names the alias", () => {
    const request = normalizeAgentRunRequest({ task: "t", model: "shallow" }, defaults);
    expect(request.model).toBe("shallow");
    expect(request.thinking).toBe("low");
  });

  it("lets an explicit call or actor level win over the alias default", () => {
    const request = normalizeAgentRunRequest(
      { task: "t", model: "shallow", thinking: "xhigh" },
      defaults,
    );
    expect(request.thinking).toBe("xhigh");
  });

  it("carries no thinking level for plain chains or unknown selectors", () => {
    expect(normalizeAgentRunRequest({ task: "t", model: "flat" }, defaults).thinking).toBeUndefined();
    expect(
      normalizeAgentRunRequest({ task: "t", model: "google/gemini-2.5-pro" }, defaults).thinking,
    ).toBeUndefined();
    expect(normalizeAgentRunRequest({ task: "t", model: "shallow" }, { runner: "pi", timeoutMs: 1 }).thinking)
      .toBeUndefined();
  });

  it("applies the alias of the configured default model", () => {
    // The configured agents.model is applied by the manager, but the alias
    // level it names still decides the run's effort.
    const request = normalizeAgentRunRequest({ task: "t" }, { ...defaults, model: "shallow" });
    expect(request.model).toBeUndefined();
    expect(request.thinking).toBe("low");
  });
});
