import { describe, expect, it } from "vitest";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";

const schemaFor = (name: string) => {
  const descriptor = AGENTS_ACTION_DESCRIPTORS.find((action) => action.name === name);
  if (!descriptor) throw new Error(`Missing agents.${name} descriptor`);
  return descriptor.inputSchema as {
    properties: Record<string, unknown>;
    additionalProperties: boolean;
  };
};

// Recorded real catch: agents-provider.test.ts had a stale exact recursive
// schema expectation. Direct descriptor checks cover the documented boolean
// without that suite's local #4664 product-deletion exclusion; no guard bypass.
describe("task spawn schema contract", () => {
  it.each(["run", "spawn"])("agents.%s exposes the documented recursive boolean", (name) => {
    const schema = schemaFor(name);
    expect(schema.properties.recursive).toEqual({
      type: "boolean",
      description: "Grant the child agents.run/spawn. Tasks are leaf by default: a task not launched recursive: true is refused with TaskSpawnDeniedError (FABRIC_TASK_SPAWN_DENIED). Setting it on your own child grants the caller nothing.",
    });
    expect(schema.additionalProperties).toBe(false);
  });

  it.each(["handoff", "create", "createActor"])("agents.%s does not expose recursive", (name) => {
    const schema = schemaFor(name);
    expect(schema.properties).not.toHaveProperty("recursive");
    expect(schema.additionalProperties).toBe(false);
  });

  it.each(["run", "spawn"])("agents.%s preserves the one-shot cwd schema", (name) => {
    expect(schemaFor(name).properties.cwd).toEqual({
      type: "string",
      description: "Filesystem execution directory for leaf or recursive Pi runs; relative paths resolve from the caller cwd. Does not change project/mesh ownership or grant target project trust.",
    });
  });

  it.each(["handoff", "create", "createActor"])("agents.%s still does not expose cwd", (name) => {
    expect(schemaFor(name).properties).not.toHaveProperty("cwd");
  });
});
