import { describe, expect, it } from "vitest";
import { agentServiceDescriptors } from "../src/agents/service-schema.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";

// These are model-visible action contracts, not implementation comments: stale
// cap/error guidance directs callers away from the supported alpha wait path.
const surfaces = [
  { name: "native", descriptors: AGENTS_ACTION_DESCRIPTORS },
  { name: "hosted", descriptors: agentServiceDescriptors() },
] as const;

describe.each(surfaces)("$name alpha wait descriptions", ({ name, descriptors }) => {
  it.each(["wait", "join"])("%s explains both modes without changing the input contract", (action) => {
    const descriptor = descriptors.find((entry) => entry.name === action)!;
    const schema = descriptor.inputSchema as {
      properties: { timeoutMs: { description: string } };
    };
    for (const text of [descriptor.description, schema.properties.timeoutMs.description]) {
      expect(text).toMatch(/defaults?(?: to)? 5 min/i);
      expect(text).toMatch(/alpha enabled[^.]*explicit[^.]*may exceed 5 min/i);
      expect(text).toMatch(/alpha enabled[^.]*returns live status/i);
      expect(text).toMatch(/alpha disabled[^.]*capped at 5 min[^.]*throw/i);
      expect(text).toMatch(/alpha defaults ON[^.]*SMARTY_ALPHA is unset/i);
      expect(text).toContain("disabled only with exact SMARTY_ALPHA=0 and no ~/.config/smarty/bash-guard.disabled marker");
      expect(text).toMatch(/marker overrides 0/i);
      expect(text).toMatch(/child keeps running/i);
      expect(text).toMatch(/pending/i);
      expect(text).toMatch(/caller cancellation and execution deadlines remain active/i);
      expect(text).toMatch(/only a terminal result is consumed/i);
      expect(text).not.toContain("default and at most 5 min");
    }
    expect(descriptor.inputSchema).toEqual({
      type: "object",
      properties: {
        id: name === "hosted" ? { type: "string", minLength: 1 } : { type: "string" },
        timeoutMs: { type: "number", minimum: 1_000, description: expect.any(String) },
      },
      required: ["id"],
      additionalProperties: false,
    });
    if (action === "join") expect(descriptor.description).toContain("Alias for agents.wait");
  });

  it("keeps wait and join argument schemas identical", () => {
    expect(descriptors.find((entry) => entry.name === "join")!.inputSchema)
      .toEqual(descriptors.find((entry) => entry.name === "wait")!.inputSchema);
  });
});

describe("wait description surface boundaries", () => {
  it.each(["wait", "join"])("native %s preserves Main's bound and pending completion ownership", (action) => {
    const descriptor = AGENTS_ACTION_DESCRIPTORS.find((entry) => entry.name === action)!;
    const schema = descriptor.inputSchema as {
      properties: { timeoutMs: { description: string } };
    };
    for (const text of [descriptor.description, schema.properties.timeoutMs.description]) {
      expect(text).toMatch(/interactive native Main[^.]*60 s[^.]*execution-deadline reserve[^.]*both modes/i);
      expect(text).toContain("waitTimedOut: true");
      expect(text).toMatch(/a pending return does not acknowledge completion or suppress its notification/i);
    }
  });

  it.each(["wait", "join"])("hosted %s does not inherit interactive Main or native result-envelope promises", (action) => {
    const descriptor = agentServiceDescriptors().find((entry) => entry.name === action)!;
    const schema = descriptor.inputSchema as {
      properties: { timeoutMs: { description: string } };
    };
    for (const text of [descriptor.description, schema.properties.timeoutMs.description]) {
      expect(text).toMatch(/hosted calls do not infer an interactive Main or apply its 60 s cap/i);
      expect(text).not.toContain("waitTimedOut");
      expect(text).not.toMatch(/acknowledge.*result|suppress.*notification/i);
    }
  });
});
