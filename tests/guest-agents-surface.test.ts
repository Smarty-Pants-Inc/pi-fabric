import { describe, expect, it } from "vitest";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { CPYTHON_CHILD_SOURCE } from "../src/runtime/cpython-child-source.js";
import { GUEST_TYPE_DECLARATIONS, guestTypeDeclarations } from "../src/runtime/guest-types.js";
import { GUEST_SETUP, QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { typeCheckFabricCode } from "../src/runtime/type-checker.js";

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

  it("forwards the actors scope so global templates and project actors stay distinct", async () => {
    const calls: Array<{ ref: string; args: unknown }> = [];
    const result = await new QuickJsRuntime().execute(
      `const project = await agents.actors();
       const explicit = await agents.actors({ scope: "project" });
       const templates = await agents.actors({ scope: "global" });
       return { project, explicit, templates };`,
      async (ref, args) => {
        calls.push({ ref, args });
        return (args as { scope?: string }).scope === "global" ? ["template"] : ["actor"];
      },
      { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 },
    );

    expect(result.terminationReason).toBe("completed");
    expect(calls).toEqual([
      { ref: "agents.actors", args: {} },
      { ref: "agents.actors", args: { scope: "project" } },
      { ref: "agents.actors", args: { scope: "global" } },
    ]);
    expect(result.value).toEqual({ project: ["actor"], explicit: ["actor"], templates: ["template"] });
  });

  it("types a stored template's validWhile as serialized source, not a callable", () => {
    const read = typeCheckFabricCode(
      `const [template] = await agents.actors({ scope: "global" });
       const exported = await agents.export({ id: "actor-1" });
       const sources: string[] = [template.validWhile?.source, exported.validWhile?.source];
       const actors = await agents.actors();
       return { sources, status: actors[0]?.status };`,
      GUEST_TYPE_DECLARATIONS,
    );
    expect(read.errors).toEqual([]);

    for (const call of [
      `const [template] = await agents.actors({ scope: "global" });
       return template.validWhile?.({} as never);`,
      `const exported = await agents.export({ id: "actor-1" });
       return exported.validWhile?.({} as never);`,
    ]) {
      expect(typeCheckFabricCode(call, GUEST_TYPE_DECLARATIONS).errors.map((error) => error.message))
        .toEqual([expect.stringContaining("not callable")]);
    }
  });

  // review/astra F2 on #73: role and project are part of the guest contract, not only the host's.
  it("types role and project on peers, sessions and the project agent", () => {
    const read = typeCheckFabricCode(
      `const lead: Pick<Awaited<ReturnType<typeof agents.projectAgent>>, "id" | "role" | "project"> = await agents.projectAgent();
       const peers: Array<Pick<FabricPeerInfo, "id" | "role" | "project">> = await agents.peers();
       const sessions = await agents.sessions();
       const roles: Array<string | undefined> = sessions.map((session) => session.role);
       return { lead, peers, roles, project: sessions[0]?.project };`,
      GUEST_TYPE_DECLARATIONS,
    );
    expect(read.errors).toEqual([]);
  });

  it("keeps the Python kernel's dynamic agents proxy in place", () => {
    expect(CPYTHON_CHILD_SOURCE).toContain('"agents"');
  });
});
