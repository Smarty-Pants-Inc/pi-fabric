import path from "node:path";
import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";
import { Value } from "typebox/value";
import { AgentInputError, normalizeAgentRequires } from "../src/agents/input-validation.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { agentServiceDescriptors, normalizeAgentServiceRequest } from "../src/agents/service-schema.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { AgentInputError as PublicAgentInputError } from "../src/agents.js";
import { GUEST_TYPE_DECLARATIONS } from "../src/runtime/guest-types.js";
import { typeCheckFabricCode } from "../src/runtime/type-checker.js";

const defaults = { runner: "pi" as const, timeoutMs: 1000 };
const absolute = path.resolve("required-input");
const root = path.parse(absolute).root;

describe("agent input declarations", () => {
  it("preserves omission and empty lists and snapshots valid absolute paths", () => {
    expect(normalizeAgentRequires(undefined)).toBeUndefined();
    expect(normalizeAgentRequires([])).toEqual([]);
    const requires = [absolute];
    const normalized = normalizeAgentRunRequest({ task: "x", needs: ["local"], requires }, defaults);
    requires[0] = "relative";
    expect(normalized).toMatchObject({ needs: ["local"], requires: [absolute] });
    expect(normalizeAgentRunRequest({ task: "x" }, defaults)).not.toHaveProperty("requires");
  });

  it.each([
    ["non-array", absolute],
    ["null", null],
    ["non-string", [1]],
    ["relative", ["relative/path"]],
    ["empty", [""]],
    ["NUL", [absolute + "\0"]],
    ["too many", Array(65).fill(absolute)],
    ["sparse", new Array(1)],
    ["ASCII bytes", [root + "x".repeat(4097)]],
    ["UTF-8 bytes", [root + "é".repeat(2048)]],
  ])("refuses %s requires with the typed unlaunched error", (_label, requires) => {
    const invalid = () => normalizeAgentRunRequest({ task: "x", requires }, defaults);
    expect(invalid).toThrow(AgentInputError);
    try { invalid(); } catch (error) {
      expect(error).toMatchObject({ name: "AgentInputError", code: "FABRIC_AGENT_INPUT_ERROR", field: "requires", launchOutcome: "unlaunched" });
    }
  });

  it("accepts the exact count and UTF-8 byte boundaries", () => {
    expect(normalizeAgentRequires(Array(64).fill(absolute))).toHaveLength(64);
    const ascii = root + "x".repeat(4096 - Buffer.byteLength(root));
    const unicode = root + "é".repeat(Math.floor((4096 - Buffer.byteLength(root)) / 2));
    expect(Buffer.byteLength(ascii)).toBe(4096);
    expect(normalizeAgentRequires([ascii, unicode])).toEqual([ascii, unicode]);
  });

  it.each(["spawn", "run"])("registers requires on native and hosted %s", name => {
    for (const descriptors of [AGENTS_ACTION_DESCRIPTORS, agentServiceDescriptors()]) {
      const schema = descriptors.find(entry => entry.name === name)!.inputSchema;
      const properties = (schema as { properties: Record<string, unknown> }).properties;
      expect(properties.requires).toMatchObject({ type: "array", maxItems: 64, items: { type: "string", maxLength: 4096 } });
      expect(Value.Check(schema, { task: "x", requires: [absolute], needs: ["local"] })).toBe(true);
      expect(Value.Check(schema, { task: "x", requires: Array(65).fill(absolute) })).toBe(false);
    }
  });

  it("exports the same typed error and accepts requires in guest spawn/run programs", () => {
    expect(PublicAgentInputError).toBe(AgentInputError);
    const result = typeCheckFabricCode('await agents.spawn({ task: "x", needs: ["local"], requires: ["/absolute/input"] }); await agents.run({ task: "x", requires: [] });', GUEST_TYPE_DECLARATIONS, true);
    expect(result.errors).toEqual([]);
    expect(result.javascript).toBeDefined();
  });

  it("forwards hosted requests without checking existence on the caller", () => {
    expect(normalizeAgentServiceRequest({ task: "x", requires: [absolute], needs: ["local"] })).toMatchObject({ requires: [absolute], needs: ["local"] });
    expect(() => normalizeAgentServiceRequest({ task: "x", requires: ["relative"] })).toThrow(AgentInputError);
  });
});
