import { describe, expect, it } from "vitest";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { CPYTHON_CHILD_SOURCE } from "../src/runtime/cpython-child-source.js";
import { guestTypeDeclarations } from "../src/runtime/guest-types.js";
import { GUEST_SETUP, QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

// AgentsProvider.invoke implements every descriptor, and the audit projection,
// docs, and arg repair all spell the same refs. Only the TypeScript prelude
// curates a literal binding table, so a missed entry degrades into
// "agents.x is not a function" at runtime instead of a type error — and the
// Python kernel's dynamic proxy hides the asymmetry from parity checks.
const slice = (source: string, start: string, end: string): string => {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  if (from === -1 || to === -1) throw new Error(`Missing block: ${start}`);
  return source.slice(from, to);
};

const names = (block: string, pattern: RegExp): Set<string> =>
  new Set([...block.matchAll(pattern)].map((match) => (match[1] ?? "").replaceAll('"', "")));

const IMPLEMENTED = AGENTS_ACTION_DESCRIPTORS.map((descriptor) => descriptor.name);

describe("guest agents surface", () => {
  it("binds every implemented action in the TypeScript prelude", () => {
    const agents = slice(GUEST_SETUP, "globalThis.agents = Object.freeze({", "\n});");
    const bound = names(agents, /^ {2}"?([A-Za-z_$][\w$]*)"?:/gm);
    expect(IMPLEMENTED.filter((name) => !bound.has(name))).toEqual([]);
  });

  it("declares every implemented action in the guest types", () => {
    const api = slice(guestTypeDeclarations(true), "interface FabricAgentsApi {", "\n}");
    const declared = names(api, /^ {2}"?([A-Za-z_$][\w$]*)"?\(/gm);
    expect(IMPLEMENTED.filter((name) => !declared.has(name))).toEqual([]);
  });

  it("routes the template actions through the host bridge", async () => {
    const calls: Array<{ ref: string; args: unknown }> = [];
    const result = await new QuickJsRuntime().execute(
      `const imported = await agents.import({ name: "reviewer", as: "auditor" });
       const exported = await agents.export({ id: "actor-1", overwrite: true });
       const unbound = ["sessions", "compact", "setTools", "setDeliveryPolicy", "clearMessages", "import", "export"]
         .filter((name) => typeof agents[name] !== "function");
       return { imported, exported, unbound };`,
      async (ref, args) => {
        calls.push({ ref, args });
        return { ref };
      },
      { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 },
    );

    expect(result.terminationReason).toBe("completed");
    expect(calls).toEqual([
      { ref: "agents.import", args: { name: "reviewer", as: "auditor" } },
      { ref: "agents.export", args: { id: "actor-1", overwrite: true } },
    ]);
    expect(result.value).toMatchObject({ unbound: [] });
  });

  it("keeps the Python kernel's dynamic agents proxy in place", () => {
    expect(CPYTHON_CHILD_SOURCE).toContain('"agents"');
  });
});
