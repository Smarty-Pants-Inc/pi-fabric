import fs from "node:fs";
import { describe, expect, it } from "vitest";
import { validateCatalogArgs } from "../src/core/action-arguments.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { TypeScriptKernelRuntime } from "../src/runtime/typescript-kernel.js";

// smarty-dev#459: agents called the messaging verbs with `to`, positionally,
// or with `sessionId`. These tests pin the accepted shapes and the rejections,
// and run every documented messaging snippet against the real guest types and
// the provider's argument preparation and schema validation.

const MESSAGING = ["ask", "tell", "steer", "followUp"] as const;
const SKILL_FILES = [
  "skillsets/typescript/fabric-exec/SKILL.md",
  "skillsets/typescript/fabric-exec/references/agents.md",
];

// prepareArguments reads no instance state; exercise the real provider method.
const provider = Object.create(AgentsProvider.prototype) as AgentsProvider;
const schemaOf = (action: string) =>
  AGENTS_ACTION_DESCRIPTORS.find((descriptor) => descriptor.name === action)!.inputSchema as Record<string, unknown>;

/** The registry path for one agents.* call: provider preparation, then schema validation. */
const admit = (action: string, args: Record<string, unknown>): Record<string, unknown> => {
  const prepared = provider.prepareArguments(action, args) as Record<string, unknown>;
  const checked = validateCatalogArgs(`agents.${action}`, schemaOf(action), prepared, undefined);
  if (checked.invalid) throw new Error(`agents.${action}: ${checked.invalid}`);
  return checked.args;
};

const kernel = new TypeScriptKernelRuntime("quickjs");

const stub = (action: string): unknown => {
  switch (action) {
    case "main": return { id: "session:main", kind: "main", name: "Main" };
    case "members": return [{ id: "session:peer", kind: "root" }];
    case "peers": return [{ id: "session:peer", kind: "root" }];
    case "spawn":
    case "run": return { id: "agent-1", name: "worker" };
    case "status": return { id: "agent-1", text: "rotating refresh tokens" };
    case "ask": return { id: "message-1", text: "ok" };
    default: return { queued: true, messageId: "message-1" };
  }
};

/** Type-check then run a guest program; agents.* calls go through {@link admit}. */
const run = async (source: string, fullCodeMode = false) => {
  const { code, checked } = kernel.prepare(source, fullCodeMode, [], {}, [], true);
  if (checked.errors.length > 0) return { typeErrors: checked.errors.map((error) => error.message), calls: [] };
  const calls: Array<{ ref: string; args: Record<string, unknown> }> = [];
  const result = await kernel.execute(code, async (ref, rawArgs) => {
    const action = ref.replace(/^agents\./, "");
    const args = (rawArgs ?? {}) as Record<string, unknown>;
    const admitted = (MESSAGING as readonly string[]).includes(action) ? admit(action, args) : args;
    calls.push({ ref, args: admitted });
    return stub(action);
  }, { timeoutMs: 10_000, memoryLimitBytes: 64 * 1024 * 1024 });
  return { typeErrors: [] as string[], calls, result };
};

const fencedBlocks = (markdown: string) =>
  [...markdown.matchAll(/^```(?:ts|typescript)(?:[ \t]+([^\n]*))?\n([\s\S]*?)\n```/gm)]
    .filter((match) => !(match[1] ?? "").split(/\s+/).includes("host"))
    .map((match) => ({ code: match[2]!, line: markdown.slice(0, match.index).split("\n").length }));

const messagingCall = /agents\.(ask|tell|steer|followUp)\(/g;

describe("agents messaging target shapes (#459)", () => {
  it("maps `to` onto id for every messaging verb, and keeps id as is", () => {
    for (const action of MESSAGING) {
      expect(admit(action, { to: "session:a", message: "m" })).toEqual({ id: "session:a", message: "m" });
      expect(admit(action, { id: "session:a", message: "m" })).toEqual({ id: "session:a", message: "m" });
      expect(admit(action, { id: "session:a", to: "session:a", message: "m" })).toEqual({ id: "session:a", message: "m" });
    }
  });

  it("rejects two different targets", () => {
    for (const action of MESSAGING) {
      expect(() => admit(action, { id: "session:a", to: "session:b", message: "m" }))
        .toThrow(`agents.${action} got two targets, id "session:a" and to "session:b"`);
    }
  });

  it("rejects sessionId with the exact fix, with or without another target", () => {
    for (const action of MESSAGING) {
      expect(() => admit(action, { sessionId: "01a0", message: "m" }))
        .toThrow(`agents.${action} has no sessionId field: use id: 'session:01a0'`);
      expect(() => admit(action, { sessionId: "session:01a0", message: "m" }))
        .toThrow("use id: 'session:01a0'");
      expect(() => admit(action, { id: "session:01a0", sessionId: "01a0", message: "m" }))
        .toThrow("has no sessionId field");
    }
  });

  it("still rejects unknown fields and a missing target", () => {
    expect(() => admit("followUp", { to: "session:a", message: "m", foo: 1 })).toThrow("/foo");
    expect(() => admit("steer", { message: "m" })).toThrow("agents.steer:");
    expect(() => admit("steer", { id: "session:a", message: "m", model: "x" })).toThrow("/model");
  });

  it("leaves non-messaging actions alone", () => {
    expect(provider.prepareArguments("status", { to: "x" })).not.toHaveProperty("id", "x");
  });

  it("type-checks and runs the positional and `to` forms in the guest", async () => {
    for (const fullCodeMode of [false, true]) {
      const run1 = await run([
        'await agents.steer("session:a", "s");',
        'await agents.followUp({ to: "session:a", message: "f" });',
        'await agents.tell("actor-1", "t");',
        'const reply = await agents.ask({ id: "actor-1", message: "q", thinking: "low" });',
        "return reply.text;",
      ].join("\n"), fullCodeMode);
      expect(run1.typeErrors).toEqual([]);
      expect(run1.calls).toEqual([
        { ref: "agents.steer", args: { id: "session:a", message: "s" } },
        { ref: "agents.followUp", args: { id: "session:a", message: "f" } },
        { ref: "agents.tell", args: { id: "actor-1", message: "t" } },
        { ref: "agents.ask", args: { id: "actor-1", message: "q", thinking: "low" } },
      ]);
    }
  });

  it("names the sessionId fix and rejects other wrong shapes at type-check", async () => {
    const session = await run('await agents.followUp({ sessionId: "01a0", message: "m" });');
    expect(session.typeErrors.join("\n")).toContain("use id: 'session:<sessionId>'");
    expect((await run('await agents.followUp({ message: "m" });')).typeErrors).not.toEqual([]);
    expect((await run('await agents.steer({ to: "a", message: "m", foo: 1 });')).typeErrors.join("\n")).toContain("'foo'");
    expect((await run('await agents.steer("a");')).typeErrors).not.toEqual([]);
  });

  it("type-checks and runs every documented messaging snippet", async () => {
    let snippets = 0;
    for (const file of SKILL_FILES) {
      for (const block of fencedBlocks(fs.readFileSync(file, "utf8"))) {
        const expected = [...block.code.matchAll(messagingCall)].map((match) => `agents.${match[1]}`);
        if (expected.length === 0) continue;
        snippets += 1;
        for (const fullCodeMode of [false, true]) {
          const where = `${file}:${block.line} (fullCodeMode ${fullCodeMode})`;
          const outcome = await run(block.code, fullCodeMode);
          expect(outcome.typeErrors, where).toEqual([]);
          expect(outcome.result?.error, where).toBeUndefined();
          // Every documented messaging call ran, so the stubs reached each branch.
          const ran = outcome.calls.map((call) => call.ref).filter((ref) => expected.includes(ref));
          expect(ran.sort(), where).toEqual([...expected].sort());
        }
      }
    }
    expect(snippets).toBeGreaterThan(0);
  });

  it("documents only declared fields in messaging signatures", () => {
    let spans = 0;
    for (const file of SKILL_FILES) {
      const markdown = fs.readFileSync(file, "utf8");
      for (const match of markdown.matchAll(/`agents\.(ask|tell|steer|followUp)\(\{([^}`]*)\}\)`/g)) {
        spans += 1;
        const declared = Object.keys((schemaOf(match[1]!).properties ?? {}) as Record<string, unknown>);
        const fields = match[2]!.split(",").map((field) => field.trim().replace(/\?$/, "").split(":")[0]!.trim()).filter(Boolean);
        expect(fields.filter((field) => !declared.includes(field)), `${file}: ${match[0]}`).toEqual([]);
      }
    }
    expect(spans).toBeGreaterThan(0);
  });
});
