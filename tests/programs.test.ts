import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig, type FabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { handleFabricProgramRunEvent, PROGRAM_RUN_MESSAGE_TYPE, runFabricProgramsCommand, type ProgramHostDeps } from "../src/programs/host.js";
import { hostProgramRunSource, programSourceWithInput, pythonLiteral } from "../src/programs/source.js";
import { canonicalProgramJson, programDigest, ProgramStore, programsDirectory } from "../src/programs/store.js";
import type { FabricActionDescriptor, FabricProgramRunReplyV1 } from "../src/protocol.js";
import { ProgramsProvider } from "../src/providers/programs-provider.js";
import { availablePythonBackends } from "./fixtures/python-backends.js";
import { rmTempSync } from "./fixtures/temp-cleanup.js";

// Host runs resolve the store from the session cwd, like the provider.
const projectRoot = process.env.PI_FABRIC_PROJECT_ROOT;
beforeAll(() => { delete process.env.PI_FABRIC_PROJECT_ROOT; });
afterAll(() => { if (projectRoot !== undefined) process.env.PI_FABRIC_PROJECT_ROOT = projectRoot; });
const roots: string[] = [];
const registries: ActionRegistry[] = [];
afterEach(async () => {
  await Promise.all(registries.splice(0).map((registry) => registry.close()));
  for (const root of roots.splice(0)) rmTempSync(root);
});
const temp = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-programs-"));
  roots.push(root);
  return root;
};

const demoDescriptor = (name: string): FabricActionDescriptor => ({
  name, description: `demo ${name}`, risk: "read",
  inputSchema: { type: "object", additionalProperties: true, properties: {} },
});

const fixture = (kernel: "typescript" | "python" = "typescript") => {
  const cwd = temp();
  const store = new ProgramStore(programsDirectory(cwd));
  const registry = new ActionRegistry();
  registries.push(registry);
  const demo = vi.fn(async (name: string, args: Record<string, unknown>) => ({ called: name, args }));
  registry.register({
    name: "demo", description: "demo",
    async list() { return [demoDescriptor("allowed"), demoDescriptor("blocked")]; },
    async describe(name) { return ["allowed", "blocked"].includes(name) ? demoDescriptor(name) : undefined; },
    invoke: demo,
  });
  const jevRun = vi.fn(async (_name: string, args: Record<string, unknown>) => ({ state: "completed", result: args.input }));
  registry.register({
    name: "jev", description: "fake jev",
    async list() { return [{ ...demoDescriptor("run"), risk: "execute" as const }]; },
    async describe(name) { return name === "run" ? { ...demoDescriptor("run"), risk: "execute" as const } : undefined; },
    invoke: jevRun,
  });
  const config: FabricConfig = normalizeFabricConfig({
    fullCodeMode: false,
    executor: { kernel, memoryLimitBytes: 256 * 1024 * 1024, ...(kernel === "python" ? { pythonRuntime: "monty" } : {}) },
  });
  let service: FabricExecutionService | undefined;
  registry.register(new ProgramsProvider(store, () => kernel, (id) => service?.nestedProgramRunner(id)));
  service = new FabricExecutionService(registry, config);
  const context = {
    cwd, hasUI: false,
    sessionManager: { getSessionId: () => "programs-test", getSessionFile: () => undefined },
    ui: { notify: vi.fn() },
  } as unknown as ExtensionContext;
  const run = (code: string, parentToolCallId = "programs-call") =>
    service!.execute({ code, signal: undefined, parentToolCallId, context, onPartial() {} });
  return { cwd, store, registry, demo, jevRun, config, service, context, run };
};

describe("content-addressed program store", () => {
  it("keeps the digest stable across key order and independent of name and description", () => {
    expect(canonicalProgramJson({ b: 1, a: { d: [2, { y: 1, x: 2 }], c: undefined } })).toBe('{"a":{"d":[2,{"x":2,"y":1}]},"b":1}');
    const left = programDigest({ kind: "fabric", kernel: "typescript", code: "return 1;", inputSchema: { type: "object", properties: { a: { type: "string" } } } });
    const right = programDigest({ inputSchema: { properties: { a: { type: "string" } }, type: "object" }, code: "return 1;", kernel: "typescript", kind: "fabric" });
    expect(left).toBe(right);
    expect(left).toMatch(/^[0-9a-f]{64}$/);
    expect(programDigest({ kind: "fabric", kernel: "python", code: "return 1;", inputSchema: { type: "object", properties: { a: { type: "string" } } } })).not.toBe(left);
  });

  it("saves idempotently as a candidate and refuses the same content under another name", async () => {
    const { store } = fixture();
    const first = await store.save({ name: "triage", code: "return 1;", description: "first" }, "typescript");
    const again = await store.save({ name: "triage", code: "return 1;", description: "changed" }, "typescript");
    expect(first.created).toBe(true);
    expect(again.created).toBe(false);
    expect(again.record).toEqual(first.record);
    expect(first.record).toMatchObject({ version: 1, name: "triage", kind: "fabric", kernel: "typescript", status: "candidate", description: "first" });
    expect(fs.existsSync(path.join(store.directory, `${first.record.digest}.json`))).toBe(true);
    await expect(store.save({ name: "other", code: "return 1;" }, "typescript")).rejects.toThrow(/already saved as triage@/);
    for (const name of ["", "Upper", "-dash", "a b", "x".repeat(65)]) {
      await expect(store.save({ name, code: "return 1;" }, "typescript")).rejects.toThrow(/Invalid program name/);
    }
    await expect(store.save({ name: "empty", code: "  " }, "typescript")).rejects.toThrow(/non-empty code/);
    await expect(store.save({ name: "bad", code: "x", inputSchema: [] as unknown }, "typescript")).rejects.toThrow(/inputSchema/);
    await expect(store.save({ name: "mixed", code: "x", jevProgram: {} }, "typescript")).rejects.toThrow(/not jevProgram/);
  });

  it("resolves names, digest prefixes, and full digests; promotion and retirement steer bare names", async () => {
    const { store } = fixture();
    const one = (await store.save({ name: "flow", code: "return 1;" }, "typescript")).record;
    await new Promise((resolve) => setTimeout(resolve, 2));
    const two = (await store.save({ name: "flow", code: "return 2;" }, "typescript")).record;
    expect((await store.resolve("flow")).digest).toBe(two.digest);
    expect((await store.resolve(`flow@${one.digest.slice(0, 12)}`)).digest).toBe(one.digest);
    expect((await store.resolve(one.digest)).digest).toBe(one.digest);
    await expect(store.resolve(`flow@${one.digest.slice(0, 11)}`)).rejects.toThrow(/at least 12/);
    await expect(store.resolve("flow@ffffffffffff")).rejects.toThrow(/Unknown program/);
    await expect(store.resolve("missing")).rejects.toThrow(/Unknown program/);
    await expect(store.resolve("flow", { requirePromoted: true })).rejects.toThrow(/no promoted version/);
    await store.promote(`flow@${one.digest.slice(0, 12)}`);
    expect((await store.resolve("flow")).digest).toBe(one.digest);
    expect((await store.resolve("flow", { requirePromoted: true })).digest).toBe(one.digest);
    await expect(store.resolve(two.digest, { requirePromoted: true })).rejects.toThrow(/candidate, not promoted/);
    await store.retire(one.digest);
    expect((await store.resolve("flow")).digest).toBe(two.digest);
    await store.retire("flow");
    await expect(store.resolve("flow")).rejects.toThrow(/all retired/);
    expect((await store.list({ name: "flow" })).map((entry) => entry.status)).toEqual(["retired", "retired"]);
    expect((await store.list({ status: "retired" })).map((entry) => entry.ref)).toEqual([
      `flow@${two.digest.slice(0, 12)}`, `flow@${one.digest.slice(0, 12)}`,
    ]);
  });

  it("serializes concurrent saves into one index and reaps a stale lock", async () => {
    const { store } = fixture();
    const names = Array.from({ length: 8 }, (_, index) => `n${index}`);
    await Promise.all(names.map((name, index) => store.save({ name, code: `return ${index};` }, "typescript")));
    expect((await store.list()).map((entry) => entry.name).sort()).toEqual(names);
    const lock = path.join(store.directory, ".lock");
    fs.mkdirSync(lock);
    const old = new Date(Date.now() - 60_000);
    fs.utimesSync(lock, old, old);
    await store.save({ name: "after-crash", code: "return 'recovered';" }, "typescript");
    expect(fs.existsSync(lock)).toBe(false);
    expect((await store.resolve("after-crash")).name).toBe("after-crash");
  });

  it("fails closed on a record whose content no longer matches its digest", async () => {
    const { store } = fixture();
    const { record } = await store.save({ name: "safe", code: "return 1;" }, "typescript");
    const file = path.join(store.directory, `${record.digest}.json`);
    fs.writeFileSync(file, JSON.stringify({ ...record, code: "return 2;" }));
    await expect(store.resolve("safe")).rejects.toThrow(/does not match its content digest/);
  });

  it("renders input prefixes and host run sources for both kernels", () => {
    expect(pythonLiteral({ a: [true, false, null, 1.5, "q\"\n"] })).toBe('{"a": [True, False, None, 1.5, "q\\"\\n"]}');
    expect(programSourceWithInput("return input.a;", "typescript", { a: 1 })).toBe('const input: any = {"a":1}; return input.a;');
    expect(programSourceWithInput("return input", "python", undefined)).toBe("input = None\nreturn input");
    expect(hostProgramRunSource("python", { ref: "r", input: { x: true }, requirePromoted: true }))
      .toBe('return await programs.run(ref="r", input={"x": True}, requirePromoted=True)');
  });
});

describe("programs provider", () => {
  it("exposes no promotion or retirement action", async () => {
    const { registry, run } = fixture();
    const names = (await registry.list({ provider: "programs" }, {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "list", nestedToolCallId: "list",
      extensionContext: {} as ExtensionContext, update() {},
    })).map((action) => action.ref).sort();
    expect(names).toEqual(["programs.get", "programs.list", "programs.run", "programs.save"]);
    for (const action of ["promote", "retire"]) {
      const result = await run(`return await tools.call({ ref: "programs.${action}", args: { ref: "x" } });`);
      expect(result.success).toBe(false);
    }
  });

  it("saves through the provider and runs nested with input, trace, and logs", async () => {
    const { run, demo } = fixture();
    const saved = await run(`return await programs.save({ name: "echo", code: "console.log('inner'); const r = await tools.call({ ref: 'demo.allowed', args: { n: input.n } }); return { n: input.n * 2, r };", inputSchema: { type: "object", required: ["n"], properties: { n: { type: "number" } } } });`);
    expect(saved.success, saved.error).toBe(true);
    const ref = (saved.value as { ref: string }).ref;
    expect(ref).toMatch(/^echo@[0-9a-f]{12}$/);
    const result = await run(`return await programs.run({ ref: "echo", input: { n: 21 } });`);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual({ n: 42, r: { called: "allowed", args: { n: 21 } } });
    expect(demo).toHaveBeenCalledOnce();
    expect(result.logs.some((line) => /^\[echo@[0-9a-f]{64}\] inner$/.test(line))).toBe(true);
    const programOperation = result.trace.operations.find((operation) => operation.ref === "fabric.program.run");
    expect(programOperation).toMatchObject({ outcome: "succeeded", args: { program: expect.stringMatching(/^echo@[0-9a-f]{64}$/) } });
    expect(result.trace.operations.map((operation) => operation.ref)).toEqual(["programs.run", "fabric.program.run", "demo.allowed"]);

    const invalid = await run(`return await programs.run({ ref: "echo", input: { n: "x" } });`);
    expect(invalid.success).toBe(false);
    expect(invalid.error).toMatch(/Invalid input for program echo@/);
    const missing = await run(`return await programs.run({ ref: "echo" });`);
    expect(missing.success).toBe(false);
    const promoted = await run(`return await programs.run({ ref: "echo", requirePromoted: true, input: { n: 1 } });`);
    expect(promoted.error).toMatch(/no promoted version/);
  });

  it("never widens the caller's capability view", async () => {
    const f = fixture();
    await f.store.save({ name: "probe", code: "return await tools.call({ ref: 'demo.blocked', args: {} });" }, "typescript");
    await f.store.save({ name: "ok", code: "return await tools.call({ ref: 'demo.allowed', args: {} });" }, "typescript");
    const lease = await f.registry.acquireCapabilityView(["programs.run", "demo.allowed"], {
      cwd: f.cwd, signal: undefined, parentToolCallId: "pin", nestedToolCallId: "pin",
      extensionContext: f.context, update() {},
    });
    expect(lease.satisfied).toBe(true);
    f.service.setCapabilityView(lease.view!);
    try {
      expect((await f.run(`return await programs.run({ ref: "ok" });`)).value).toEqual({ called: "allowed", args: {} });
      const blocked = await f.run(`return await programs.run({ ref: "probe" });`);
      expect(blocked.success).toBe(false);
      expect(f.demo).toHaveBeenCalledTimes(1);
      const save = await f.run(`return await programs.save({ name: "nope", code: "return 1;" });`);
      expect(save.success).toBe(false);
    } finally {
      f.service.setCapabilityView(undefined);
      await lease.release();
    }
  });

  it("refuses kernel mismatches and runs outside an execution, and bounds recursion", async () => {
    const f = fixture();
    await f.store.save({ name: "py", kernel: "python", code: "return 1" }, "typescript");
    expect((await f.run(`return await programs.run({ ref: "py" });`)).error).toMatch(/is a python program; this session's kernel is typescript/);
    const provider = new ProgramsProvider(f.store, () => "typescript", () => undefined);
    await expect(provider.invoke("run", { ref: "py" }, { parentToolCallId: "none", signal: undefined } as never)).rejects.toThrow(/only inside a fabric_exec program/);
    await expect(provider.invoke("promote", { ref: "py" }, { parentToolCallId: "none" } as never)).rejects.toThrow(/Unknown programs action/);
    await f.store.save({ name: "loop", code: "return await programs.run({ ref: 'loop' });" }, "typescript");
    const loop = await f.run(`return await programs.run({ ref: "loop" });`);
    expect(loop.success).toBe(false);
    expect(loop.error).toMatch(/nested program budget exhausted/);
  });

  it("round-trips jev programs through the jev provider path", async () => {
    const f = fixture();
    const jevProgram = { name: "triage", code: "return input;", inputSchema: { type: "object" }, outputSchema: { type: "object" }, requires: [] };
    const saved = await f.run(`return await programs.save({ name: "triage-jev", kind: "jev", jevProgram: ${JSON.stringify(jevProgram)} });`);
    expect(saved.success, saved.error).toBe(true);
    const got = await f.run(`return await programs.get({ ref: "triage-jev" });`);
    expect(got.value).toMatchObject({ kind: "jev", jevProgram, status: "candidate" });
    expect((got.value as { kernel?: string }).kernel).toBeUndefined();
    const ran = await f.run(`return await programs.run({ ref: "triage-jev", input: { ticket: 7 } });`);
    expect(ran.success, ran.error).toBe(true);
    expect(ran.value).toEqual({ state: "completed", result: { ticket: 7 } });
    expect(f.jevRun).toHaveBeenCalledWith("run", { program: jevProgram, input: { ticket: 7 } }, expect.anything());
    expect(ran.trace.operations.map((operation) => operation.ref)).toEqual(["programs.run", "fabric.program.run", "jev.run"]);
  });

  it.skipIf(!availablePythonBackends.monty)("runs Python programs nested with an input global", async () => {
    const f = fixture("python");
    await f.store.save({ name: "pyecho", code: "r = await tools.call(ref='demo.allowed', args={'v': input['v']})\nreturn {'v': input['v'] + 1, 'r': r}" }, "python");
    const result = await f.run(`return await programs.run(ref="pyecho", input={"v": 1})`);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual({ v: 2, r: { called: "allowed", args: { v: 1 } } });
  });
});

describe.skipIf(!availablePythonBackends.monty)("fabric-graph Python skill programs", () => {
  const fences = [...fs.readFileSync("skillsets/python/fabric-graph/SKILL.md", "utf8").matchAll(/```python\r?\n([\s\S]*?)\r?\n```/g)].map((match) => match[1]!);
  const execute = async (index: number, payloads: Record<string, string>, host: (ref: string, args: Record<string, unknown>) => unknown) => {
    const { MontyRuntime } = await import("../src/runtime/monty-runtime.js");
    const calls: string[] = [];
    const result = await new MontyRuntime().execute(fences[index]!, async (ref, args) => {
      const target = ref === "fabric.$call" ? args.ref as string : ref;
      calls.push(target);
      return host(target, ref === "fabric.$call" ? (args.args ?? {}) as Record<string, unknown> : args);
    }, { timeoutMs: 5_000, memoryLimitBytes: 64 * 1024 * 1024, strings: payloads });
    expect(result.terminationReason, result.error).toBe("completed");
    return { value: result.value as Record<string, unknown>, calls };
  };

  it("runs the in-session, seed, and node programs", async () => {
    expect(fences).toHaveLength(3);
    const inSession = await execute(0, { target: "tests/a.test.ts" }, (ref) =>
      ref === "agents.run" ? { status: "completed", text: "APPROVE" } : { ok: true, output: "" });
    expect(inSession.value).toMatchObject({ status: "success" });
    const seeded = await execute(1, { run: "r1" }, (ref, args) => ref === "agents.create" ? { id: "actor-1", name: args.name } : { key: "k", version: 1 });
    expect(seeded.value).toMatchObject({ run: "r1", key: "runs/r1/graph", actor: "actor-1" });
    expect(seeded.calls).toEqual(["mesh.put", "agents.create", "agents.tell"]);
    let graph: Record<string, unknown> = { node: "approve", status: "ready", results: {} };
    const node = (ref: string, args: Record<string, unknown>) => {
      if (ref === "mesh.get") return { key: "runs/r1/graph", value: graph, version: 3 };
      if (ref === "mesh.put") { graph = args.value as Record<string, unknown>; return { version: 4 }; }
      if (ref === "decisions.raise") return { id: "dec_00000001" };
      if (ref === "decisions.wait") return { status: "answered", answer: { optionId: "yes" } };
      if (ref === "mesh.self") return { id: "actor-1" };
      if (ref === "programs.run") return { ok: true };
      return { queued: true };
    };
    expect((await execute(2, { run: "r1" }, node)).value).toMatchObject({ status: "waiting", decisionId: "dec_00000001" });
    expect((await execute(2, { run: "r1" }, node)).value).toMatchObject({ status: "approved" });
    expect(graph).toMatchObject({ node: "release", status: "ready" });
    const released = await execute(2, { run: "r1" }, node);
    expect(released.calls).toContain("programs.run");
    expect(graph).toMatchObject({ node: "done", status: "done" });
  });
});

describe("host program runs", () => {
  const hostDeps = (f: ReturnType<typeof fixture>) => {
    const sendMessage = vi.fn();
    const deps: ProgramHostDeps = {
      state: { ensure: async () => undefined, config: f.config, execution: f.service, registry: f.registry } as unknown as ProgramHostDeps["state"],
      pi: { sendMessage } as unknown as ProgramHostDeps["pi"],
    };
    return { deps, sendMessage };
  };

  it("replies to the program run event and records invokedBy host", async () => {
    const f = fixture();
    await f.store.save({ name: "hello", code: "return { greeting: 'hi ' + input.who };" }, "typescript");
    const { deps, sendMessage } = hostDeps(f);
    const replies: FabricProgramRunReplyV1[] = [];
    {
      await handleFabricProgramRunEvent({ ref: "hello", input: { who: "host" }, reply: (result: FabricProgramRunReplyV1) => replies.push(result) }, { ...deps, context: f.context });
      expect(replies).toHaveLength(1);
      expect(replies[0]).toMatchObject({ ok: true, program: expect.stringMatching(/^hello@[0-9a-f]{64}$/), value: { greeting: "hi host" } });
      expect(sendMessage).toHaveBeenCalledOnce();
      const [message, options] = sendMessage.mock.calls[0]!;
      expect(options).toEqual({ triggerTurn: false });
      expect(message).toMatchObject({ customType: PROGRAM_RUN_MESSAGE_TYPE, display: true, details: { invokedBy: "host", success: true } });
      const operation = message.details.trace.operations.find((entry: { ref: string }) => entry.ref === "fabric.program.run");
      expect(operation.args).toEqual({ program: replies[0]!.ok ? replies[0]!.program : "", invokedBy: "host" });

      await handleFabricProgramRunEvent({ ref: "missing", reply: (result: FabricProgramRunReplyV1) => replies.push(result) }, { ...deps, context: f.context });
      expect(replies[1]).toMatchObject({ ok: false, error: expect.stringMatching(/Unknown program/) });
      await handleFabricProgramRunEvent({ ref: 7, reply: (result: FabricProgramRunReplyV1) => replies.push(result) }, { ...deps, context: f.context });
      expect(replies[2]).toMatchObject({ ok: false, error: expect.stringMatching(/ref must be/) });
      await handleFabricProgramRunEvent({ ref: "hello", reply: (result: FabricProgramRunReplyV1) => replies.push(result) }, { ...deps, context: undefined });
      expect(replies[3]).toMatchObject({ ok: false, error: expect.stringMatching(/No active Pi session/) });
      const aborted = new AbortController();
      aborted.abort();
      await handleFabricProgramRunEvent({ ref: "hello", input: { who: "x" }, signal: aborted.signal, reply: (result: FabricProgramRunReplyV1) => replies.push(result) }, { ...deps, context: f.context });
      expect(replies[4]).toMatchObject({ ok: false });
    }
  });

  it("lists, promotes, retires and runs through the slash command", async () => {
    const f = fixture();
    const { deps, sendMessage } = hostDeps(f);
    {
      const store = f.store;
      const { record } = await store.save({ name: "sum", code: "return input.a + input.b;", description: "adds" }, "typescript");
      const notify = f.context.ui.notify as ReturnType<typeof vi.fn>;
      await runFabricProgramsCommand(deps, f.context, "programs", "");
      expect(notify).toHaveBeenLastCalledWith(expect.stringContaining(`sum@${record.digest.slice(0, 12)} [candidate] fabric/typescript`), "info");
      await runFabricProgramsCommand(deps, f.context, "programs", " promote sum");
      expect((await store.resolve("sum")).status).toBe("promoted");
      await runFabricProgramsCommand(deps, f.context, "run", ' sum {"a": 2, "b": 3}');
      expect(notify).toHaveBeenLastCalledWith(expect.stringContaining("finished"), "info");
      expect(sendMessage.mock.calls.at(-1)![0].content).toContain("5");
      await runFabricProgramsCommand(deps, f.context, "run", " sum {bad json");
      expect(notify).toHaveBeenLastCalledWith(expect.stringContaining("not valid JSON"), "error");
      await runFabricProgramsCommand(deps, f.context, "programs", " retire sum");
      expect((await store.resolve(record.digest)).status).toBe("retired");
      await runFabricProgramsCommand(deps, f.context, "run", " sum");
      expect(notify).toHaveBeenLastCalledWith(expect.stringContaining("failed"), "error");
    }
  });
});
