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
