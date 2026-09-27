import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ActionRegistry, type FabricCallAudit } from "../src/core/action-registry.js";
import type { FabricProvider } from "../src/protocol.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { setActiveRepairCompiler } from "../src/repairs/active.js";
import { RepairCompiler } from "../src/repairs/compiler.js";
import { TypeScriptKernelRuntime } from "../src/runtime/typescript-kernel.js";

// smarty-dev#459: agents called the messaging verbs with `to`, positionally,
// or with `sessionId`. These tests pin the accepted shapes and the rejections
// through the real ActionRegistry (with the repair compiler attached), and run
// every documented messaging snippet against the real guest types.

const MESSAGING = ["ask", "tell", "steer", "followUp"] as const;
const SKILL_FILES = [
  "skillsets/typescript/fabric-exec/SKILL.md",
  "skillsets/typescript/fabric-exec/references/agents.md",
];

// The argument hooks, list and describe read no instance state; use the real
// provider methods and record what would reach invoke.
const real = Object.create(AgentsProvider.prototype) as AgentsProvider;
const schemaOf = (action: string) =>
  AGENTS_ACTION_DESCRIPTORS.find((descriptor) => descriptor.name === action)!.inputSchema as Record<string, unknown>;
const invoked: Array<{ action: string; args: Record<string, unknown> }> = [];
const agentsProvider: FabricProvider = {
  name: "agents",
  description: "agents",
  list: (request, context) => real.list(request, context),
  describe: (name, context) => real.describe(name, context),
  guardArguments: (name, args) => real.guardArguments(name, args),
  prepareArguments: (name, args) => real.prepareArguments(name, args),
  async invoke(action, args) {
    invoked.push({ action, args });
    return { queued: true, messageId: "m-1" };
  },
};
const context = {
  cwd: process.cwd(),
  signal: undefined,
  parentToolCallId: "parent",
  nestedToolCallId: "nested",
  extensionContext: {} as ExtensionContext,
  update() {},
  approve: async () => {},
  audits: [] as FabricCallAudit[],
  maxResultChars: 10_000,
};

let registry: ActionRegistry;
let compiler: RepairCompiler;
const tmp: string[] = [];
beforeEach(() => {
  const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-msg-target-"));
  tmp.push(agentDir);
  compiler = new RepairCompiler({ agentDir });
  compiler.setCatalogSurface({ providers: ["agents"], capturedTools: [] });
  setActiveRepairCompiler(compiler);
  registry = new ActionRegistry();
  registry.register(agentsProvider);
  invoked.length = 0;
});
afterEach(() => {
  setActiveRepairCompiler(undefined);
  for (const dir of tmp.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

/** One agents.* call through the registry; returns the arguments that reached invoke. */
const admit = async (action: string, args: Record<string, unknown>): Promise<Record<string, unknown>> => {
  const before = invoked.length;
  await registry.invoke(`agents.${action}`, args, context);
  expect(invoked.length).toBe(before + 1);
  return invoked.at(-1)!.args;
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
    const admitted = (MESSAGING as readonly string[]).includes(action) ? await admit(action, args) : args;
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
  it("maps `to` onto id for every messaging verb, and keeps id as is", async () => {
    for (const action of MESSAGING) {
      expect(await admit(action, { to: "session:a", message: "m" })).toEqual({ id: "session:a", message: "m" });
      expect(await admit(action, { id: "session:a", message: "m" })).toEqual({ id: "session:a", message: "m" });
      expect(await admit(action, { id: "session:a", to: "session:a", message: "m" })).toEqual({ id: "session:a", message: "m" });
    }
  });

  it("rejects two different targets before invoke", async () => {
    for (const action of MESSAGING) {
      await expect(admit(action, { id: "session:a", to: "session:b", message: "m" }))
        .rejects.toThrow(`agents.${action} got two targets, id "session:a" and to "session:b"`);
    }
    expect(invoked).toEqual([]);
  });

  it("rejects sessionId with the exact fix, before repair and invoke", async () => {
    for (const action of MESSAGING) {
      await expect(admit(action, { sessionId: "01a0", message: "m" }))
        .rejects.toThrow(`agents.${action} has no sessionId field: use id: 'session:01a0'`);
      await expect(admit(action, { sessionId: "session:01a0", message: "m" }))
        .rejects.toThrow("use id: 'session:01a0'");
      await expect(admit(action, { id: "session:01a0", sessionId: "01a0", message: "m" }))
        .rejects.toThrow("has no sessionId field");
    }
    expect(invoked).toEqual([]);
    // The rejection ran before generic repair, so nothing learned sessionId -> id.
    expect(compiler.repairs).toEqual([]);
  });

  it("rejects sessionId even when a sessionId -> id repair is already promoted (astra F1)", async () => {
    for (const action of MESSAGING) {
      const ref = `agents.${action}`;
      const declared = Object.keys(schemaOf(action).properties as Record<string, unknown>);
      compiler.observeInvalidArgs(ref, { sessionId: "01a0", message: "m" }, declared, "extra", { extraKeys: ["sessionId"] });
      expect(compiler.repairs).toContainEqual({ kind: "keyAlias", ref, from: "sessionId", to: "id" });
      await expect(admit(action, { sessionId: "01a0", message: "m" }))
        .rejects.toThrow(`agents.${action} has no sessionId field: use id: 'session:01a0'`);
      // Counterexample: the documented id form and the to alias still reach invoke.
      expect(await admit(action, { id: "session:01a0", message: "m" })).toEqual({ id: "session:01a0", message: "m" });
      expect(await admit(action, { to: "session:01a0", message: "m" })).toEqual({ id: "session:01a0", message: "m" });
    }
    expect(invoked.map((call) => call.action)).toEqual(MESSAGING.flatMap((action) => [action, action]));
  });

  it("still rejects unknown fields and a missing target", async () => {
    await expect(admit("followUp", { to: "session:a", message: "m", foo: 1 })).rejects.toThrow("/foo");
    await expect(admit("steer", { message: "m" })).rejects.toThrow("Invalid arguments for agents.steer");
    await expect(admit("steer", { id: "session:a", message: "m", model: "x" })).rejects.toThrow("/model");
    expect(invoked).toEqual([]);
  });

  it("leaves non-messaging actions alone", () => {
    expect(real.guardArguments("status", { sessionId: "x", to: "y" })).toEqual({ sessionId: "x", to: "y" });
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
