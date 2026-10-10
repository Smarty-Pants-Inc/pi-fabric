import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import {
  actorBashTimeout, applyRunBashDefaults, BASH_IDLE_MARKER, bashIdleSeconds, DEFAULT_ACTOR_BASH_TIMEOUT_S, DEFAULT_BASH_IDLE_S,
  MAX_ACTOR_BASH_TIMEOUT_S,
} from "../src/guards/actor-bash-timeout.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { parseBashTimeoutSeconds } from "../src/actors/manager.js";
import { foregroundWaitRefusal } from "../src/guards/foreground-wait.js";
import piFabric from "../src/index.js";
import { MeshStore } from "../src/mesh/store.js";
import { parseWorkerOptions } from "../src/worker/options.js";

describe("actor bash timeout (smarty-dev#2184)", () => {
  const actor = { PI_FABRIC_ACTOR_ID: "actor:a" };

  it("gives an actor run's bash call without a timeout the 600 s default", () => {
    expect(DEFAULT_ACTOR_BASH_TIMEOUT_S).toBe(600);
    expect(actorBashTimeout(actor, undefined)).toBe(600);
  });

  it("leaves non-actor sessions and explicit timeouts alone", () => {
    expect(actorBashTimeout({}, undefined)).toBeUndefined();
    expect(actorBashTimeout(actor, 30)).toBeUndefined();
  });

  it("smarty-dev#6137: a worker-launched task run gets the cap and a 180 s idle limit; a Main gets neither", () => {
    const task = { PI_FABRIC_BASH_IDLE_S: "180" };
    expect(DEFAULT_BASH_IDLE_S).toBe(180);
    expect(actorBashTimeout(task, undefined)).toBe(600);
    expect(bashIdleSeconds(task)).toBe(180);
    expect(bashIdleSeconds({ PI_FABRIC_BASH_IDLE_S: "45" })).toBe(45);
    expect(bashIdleSeconds({ PI_FABRIC_BASH_IDLE_S: "0" })).toBeUndefined();
    expect(bashIdleSeconds({ PI_FABRIC_BASH_IDLE_S: "junk" })).toBe(180);
    expect(bashIdleSeconds(actor)).toBe(180);
    const explicit = { command: "git log --all -S needle", timeout: 1000 };
    applyRunBashDefaults(task, explicit);
    expect(explicit.timeout).toBe(1000);
    if (process.platform === "win32") {
      expect(explicit).toEqual({ command: "git log --all -S needle", timeout: 1000 });
    } else {
      expect(explicit.command.startsWith(`${BASH_IDLE_MARKER}\n`)).toBe(true);
    }
    expect(bashIdleSeconds({})).toBeUndefined();
    const main = { command: "sleep 300" };
    applyRunBashDefaults({}, main);
    expect(main).toEqual({ command: "sleep 300" });
    for (const detached of [{ background: true }, { monitor: { delivery: "wake" } }]) {
      const input = { command: "npm run dev", ...detached };
      applyRunBashDefaults(task, input);
      expect(input).toEqual({ command: "npm run dev", ...detached, timeout: 600 });
    }
    const windows = { command: "sleep 300" };
    applyRunBashDefaults(task, windows, "win32");
    expect(windows).toEqual({ command: "sleep 300", timeout: 600 });
  });

  it.each(["linux", "darwin", "win32"] as const)("wraps marker-prefixed untrusted text and trusts only hook-set args metadata on %s", (platform) => {
    const env = { PI_FABRIC_BASH_IDLE_S: "2" };
    const command = `${BASH_IDLE_MARKER}\nsleep 300`;
    const input = { command };
    applyRunBashDefaults(env, input, platform);
    if (platform === "win32") {
      // smarty-dev#6137: Windows gets the total cap only, with no POSIX wrapper or metadata.
      expect(input).toEqual({ command, timeout: 600 });
      expect(Object.getOwnPropertySymbols(input)).toEqual([]);
      applyRunBashDefaults(env, input, platform);
      const replay = JSON.parse(JSON.stringify(input)) as typeof input;
      applyRunBashDefaults(env, replay, platform);
      expect(replay).toEqual({ command, timeout: 600 });
      return;
    }
    expect(input.command).not.toBe(command);
    expect(input.command).toContain(`\n${command}\n`);
    const wrapped = input.command;
    applyRunBashDefaults(env, input, platform);
    expect(input.command).toBe(wrapped);
    expect(Object.keys(input)).toEqual(["command", "timeout"]);
    const replay = JSON.parse(JSON.stringify(input)) as typeof input;
    applyRunBashDefaults(env, replay, platform);
    expect(replay.command).not.toBe(wrapped);
  });

  it.each([actor, { PI_FABRIC_BASH_IDLE_S: "180" }])("keeps Windows total-cap defaults, overrides and opt-outs for %j", (env) => {
    for (const [override, expected] of [[undefined, 600], ["45", 45], ["0", undefined]] as const) {
      const run = { ...env, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: override };
      const input = { command: "sleep 300" };
      applyRunBashDefaults(run, input, "win32");
      expect(input).toEqual({ command: "sleep 300", ...(expected === undefined ? {} : { timeout: expected }) });
      for (const timeout of [0, 5, 1000]) {
        const explicit = { command: "sleep 300", timeout };
        applyRunBashDefaults(run, explicit, "win32");
        expect(explicit).toEqual({ command: "sleep 300", timeout });
      }
    }
    const main = { command: "sleep 300" };
    applyRunBashDefaults({}, main, "win32");
    expect(main).toEqual({ command: "sleep 300" });
  });

  it("smarty-dev#6137: the wrapped command trips no shell guard and spawn validates bashIdleSeconds", async () => {
    const guards = await import("../src/core/pattern-kill.js");
    const input = { command: "ls" };
    applyRunBashDefaults({ PI_FABRIC_BASH_IDLE_S: "180" }, input);
    expect(guards.killsByPattern(input.command)).toBe(false);
    expect(guards.wipesTmp(input.command)).toBe(false);
    expect(foregroundWaitRefusal(input.command, 600)).toBeUndefined();
    const defaults = { runner: "pi" as const, timeoutMs: 60_000 };
    expect(normalizeAgentRunRequest({ task: "t", bashIdleSeconds: 0 }, defaults).bashIdleSeconds).toBe(0);
    expect(normalizeAgentRunRequest({ task: "t", bashIdleSeconds: 45 }, defaults).bashIdleSeconds).toBe(45);
    for (const bad of [-1, 1.5, "60", 2_147_484]) {
      expect(() => normalizeAgentRunRequest({ task: "t", bashIdleSeconds: bad }, defaults)).toThrow(/bashIdleSeconds/);
    }
    const spawn = AGENTS_ACTION_DESCRIPTORS.find((action) => action.name === "spawn")!;
    expect(spawn.inputSchema).toMatchObject({ properties: { bashIdleSeconds: { type: "integer", minimum: 0 } } });
  });

  it("takes the actor's override, and 0 turns the default off", () => {
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "45" }, undefined)).toBe(45);
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "0" }, undefined)).toBeUndefined();
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "junk" }, undefined)).toBe(600);
  });

  it("smarty-dev#2339 F3: bounds the override to Pi's whole-second timer limit", () => {
    expect(parseBashTimeoutSeconds(2_147_483)).toBe(2_147_483);
    expect(() => parseBashTimeoutSeconds(2_147_484)).toThrow(/at most 2147483/);
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "2147483" }, undefined)).toBe(2_147_483);
    for (const raw of ["2147484", "1e20", "Infinity"]) {
      expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: raw }, undefined)).toBe(DEFAULT_ACTOR_BASH_TIMEOUT_S);
    }
    expect(parseBashTimeoutSeconds(0)).toBe(0);
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "0" }, undefined)).toBeUndefined();
    expect(MAX_ACTOR_BASH_TIMEOUT_S).toBe(2_147_483);
  });

  it("smarty-dev#2339 F3: advertises the creation schema's maximum", () => {
    const create = AGENTS_ACTION_DESCRIPTORS.find((action) => action.name === "create")!;
    expect(create.inputSchema).toMatchObject({ properties: { bashTimeoutSeconds: { type: "integer", minimum: 0, maximum: 2_147_483 } } });
  });

  it("validates bashTimeoutSeconds", () => {
    expect(parseBashTimeoutSeconds(0)).toBe(0);
    for (const bad of [-1, 1.5, "60", null]) expect(() => parseBashTimeoutSeconds(bad)).toThrow(/non-negative integer/);
  });

  it("the index hook judges the foreground wait before injecting, so refusals are unchanged", () => {
    // The injected 600 s would cap an unbounded wait to 600 s; the guard still sees no timeout.
    expect(foregroundWaitRefusal("while true; do sleep 5; done", undefined)).toBeDefined();
    expect(foregroundWaitRefusal("ls", undefined)).toBeUndefined();
  });
});

describe("actor bashTimeoutSeconds plumbing (smarty-dev#2184)", () => {
  const roots: string[] = [];
  const closers: Array<() => Promise<void>> = [];
  afterEach(async () => {
    for (const close of closers.splice(0)) await close();
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it("passes an actor's bashTimeoutSeconds to its run's worker and keeps it across a restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-bash-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const identity = { id: "session:test", name: "main", kind: "main" as const, sessionId: "test" };
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const make = () => {
      const manager = new ActorManager("test", identity, mesh, meshConfig, agents, () => {}, { actorRoot: path.join(root, "actors"), persistent: true });
      closers.unshift(() => manager.close());
      return manager;
    };
    closers.push(() => agents.close());
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const actors = make();
    const created = await actors.create({ name: "bash", instructions: "Keep quiet", extensions: false, tools: [], transport: "process", bashTimeoutSeconds: 42 });
    await actors.ask(created.id, "first");
    const options = parseWorkerOptions(["node", "worker.js", ...launch.mock.calls[0]![0].workerArguments]);
    expect(options).toMatchObject({ actorId: created.id, bashTimeoutSeconds: 42 });
    await actors.close();
    launch.mockClear();
    const restored = make();
    await restored.ask(created.id, "second");
    const again = parseWorkerOptions(["node", "worker.js", ...launch.mock.calls[0]![0].workerArguments]);
    expect(again.bashTimeoutSeconds).toBe(42);
  });

  it("smarty-dev#2339 F3: rejects creation above the maximum, accepts the maximum and zero", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-bash-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    closers.push(() => agents.close());
    const actors = new ActorManager("test", { id: "session:test", name: "main", kind: "main", sessionId: "test" }, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, { actorRoot: path.join(root, "actors") });
    closers.unshift(() => actors.close());
    await expect(actors.create({ name: "too-large", instructions: "x", bashTimeoutSeconds: 2_147_484 })).rejects.toThrow(/bashTimeoutSeconds.*at most 2147483/);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    for (const seconds of [2_147_483, 0]) {
      const created = await actors.create({ name: `valid-${seconds}`, instructions: "x", extensions: false, tools: [], transport: "process", bashTimeoutSeconds: seconds });
      await actors.ask(created.id, "probe");
      const options = parseWorkerOptions(["node", "worker.js", ...launch.mock.calls.at(-1)![0].workerArguments]);
      expect(options.bashTimeoutSeconds).toBe(seconds);
    }
  });

  it("rejects an invalid bashTimeoutSeconds at create", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-bash-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    closers.push(() => agents.close());
    const actors = new ActorManager("test", { id: "session:test", name: "main", kind: "main", sessionId: "test" }, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, { actorRoot: path.join(root, "actors") });
    closers.unshift(() => actors.close());
    await expect(actors.create({ name: "bad", instructions: "x", extensions: false, tools: [], bashTimeoutSeconds: -5 })).rejects.toThrow(/non-negative integer/);
  });
});

describe("Fabric bash tool_call hook in an actor run (smarty-dev#2184)", () => {
  afterEach(() => vi.unstubAllEnvs());

  const bashCall = async (input: Record<string, unknown>) => {
    const handlers = new Map<string, Array<(...args: unknown[]) => unknown>>();
    const pi = {
      events: { emit: vi.fn(), on: vi.fn(() => () => undefined) },
      getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []),
      on: vi.fn((event: string, handler: (...args: unknown[]) => unknown) => {
        handlers.set(event, [...handlers.get(event) ?? [], handler]);
      }),
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
    } as unknown as ExtensionAPI;
    await piFabric(pi);
    const event = { type: "tool_call", toolName: "bash", toolCallId: "call-1", input };
    const results: unknown[] = [];
    for (const handler of handlers.get("tool_call") ?? []) {
      try { results.push(await handler(event, { cwd: process.cwd(), hasUI: false })); } catch { /* other hooks need a live session */ }
    }
    for (const shutdown of handlers.get("session_shutdown") ?? []) await shutdown();
    return { input, blocked: results.some((result) => (result as { block?: boolean } | undefined)?.block) };
  };

  it("sets the default timeout on an actor's bash call and keeps refusing long foreground waits", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "actor:a");
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", undefined);
    expect((await bashCall({ command: "ls" })).input.timeout).toBe(600);
    expect((await bashCall({ command: "ls", timeout: 5 })).input.timeout).toBe(5);
    expect((await bashCall({ command: "while true; do sleep 5; done" })).blocked).toBe(true);
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", "0");
    expect((await bashCall({ command: "ls" })).input.timeout).toBeUndefined();
  });

  it("smarty-dev#2339 F3: injects the maximum and falls back for an oversized override", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "actor:a");
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", "2147483");
    expect((await bashCall({ command: "ls" })).input.timeout).toBe(2_147_483);
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", "2147484");
    expect((await bashCall({ command: "ls" })).input.timeout).toBe(600);
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", "0");
    expect((await bashCall({ command: "ls" })).input.timeout).toBeUndefined();
  });

  it("leaves a Main session's bash call alone (smarty-dev#6137: no cap, no idle watchdog)", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_BASH_IDLE_S", undefined);
    expect((await bashCall({ command: "ls" })).input).toEqual({ command: "ls" });
  });

  it("smarty-dev#6137: gives a task agent's bash call the cap and the idle watchdog after the guards", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", undefined);
    vi.stubEnv("PI_FABRIC_BASH_IDLE_S", "180");
    const { input } = await bashCall({ command: "git log --all -S needle" });
    expect(input.timeout).toBe(600);
    if (process.platform === "win32") {
      expect(input).toEqual({ command: "git log --all -S needle", timeout: 600 });
    } else {
      expect(String(input.command).startsWith(`${BASH_IDLE_MARKER}\n`)).toBe(true);
      expect(input.command).toContain("\ngit log --all -S needle\n");
    }
    expect((await bashCall({ command: "while true; do sleep 5; done" })).blocked).toBe(true);
    const explicit = (await bashCall({ command: "ls", timeout: 5 })).input;
    expect(explicit.timeout).toBe(5);
    if (process.platform === "win32") {
      expect(explicit).toEqual({ command: "ls", timeout: 5 });
    } else {
      expect(String(explicit.command).startsWith(`${BASH_IDLE_MARKER}\n`)).toBe(true);
    }
  });
});

// Review finding 4: a native-tool actor (extensions: false) runs Pi with --no-extensions, so the
// Fabric hook above never loads there. The worker loads a timeout-only hook with -e instead.
describe("timeout-only actor bash hook (smarty-dev#2184)", () => {
  const roots: string[] = [];
  const closers: Array<() => Promise<void>> = [];
  afterEach(async () => {
    vi.unstubAllEnvs();
    for (const close of closers.splice(0)) await close();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  const hookCall = async (input: Record<string, unknown>, toolName = "bash") => {
    const { default: actorBashHook } = await import("../src/guards/actor-bash-hook.js");
    const handlers: Array<(event: unknown) => unknown> = [];
    actorBashHook({ on: (name: string, handler: (event: unknown) => unknown) => { if (name === "tool_call") handlers.push(handler); } } as unknown as ExtensionAPI);
    expect(handlers).toHaveLength(1);
    await handlers[0]!({ type: "tool_call", toolName, toolCallId: "call-1", input });
    return input;
  };

  it("sets the default, keeps an explicit timeout, and 0 means none", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "actor:a");
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", undefined);
    expect((await hookCall({ command: "sleep 30" })).timeout).toBe(600);
    expect((await hookCall({ command: "sleep 30", timeout: 5 })).timeout).toBe(5);
    expect((await hookCall({ path: "x" }, "read")).timeout).toBeUndefined();
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", "2");
    expect((await hookCall({ command: "sleep 30" })).timeout).toBe(2);
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", "0");
    expect((await hookCall({ command: "sleep 30" })).timeout).toBeUndefined();
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_BASH_IDLE_S", undefined);
    expect((await hookCall({ command: "sleep 30" })).timeout).toBeUndefined();
  });

  it("smarty-dev#6137: wraps once on POSIX and leaves Windows command text alone across hooks", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_BASH_IDLE_S", "180");
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", "0");
    const input = await hookCall({ command: "sleep 30" });
    const once = { ...input };
    expect(once.timeout).toBeUndefined();
    if (process.platform === "win32") {
      expect(once).toEqual({ command: "sleep 30" });
    } else {
      expect(String(once.command).startsWith(`${BASH_IDLE_MARKER}\n`)).toBe(true);
    }
    expect(await hookCall(input)).toEqual(once);
    // The second real hook shares the exact args object, not a caller-supplied copy of its text.
    applyRunBashDefaults(process.env, input);
    expect(input).toEqual(once);
    const replay = await hookCall({ ...once });
    if (process.platform === "win32") {
      expect(replay).toEqual(once);
    } else {
      expect(replay).not.toEqual(once);
    }
    vi.stubEnv("PI_FABRIC_BASH_IDLE_S", "0");
    expect(await hookCall({ command: "sleep 30" })).toEqual({ command: "sleep 30" });
  });

  it("the real worker loads it into native-tool actor and task runs with --no-extensions (smarty-dev#6137)", { timeout: 20_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-bash-hook-"));
    roots.push(root);
    const fakePi = path.join(root, "fake-pi.mjs");
    fs.writeFileSync(fakePi, [
      "#!/usr/bin/env node",
      "import readline from 'node:readline';",
      "const send = (event) => process.stdout.write(JSON.stringify(event) + '\\n');",
      "const text = JSON.stringify({ argv: process.argv.slice(2), timeout: process.env.PI_FABRIC_ACTOR_BASH_TIMEOUT_S, idle: process.env.PI_FABRIC_BASH_IDLE_S });",
      "const message = { role: 'assistant', content: [{ type: 'text', text }], provider: 'fake', model: 'fake', usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0 }, stopReason: 'stop' };",
      "let started = false;",
      "readline.createInterface({ input: process.stdin }).on('line', (line) => {",
      "  if (started || !line.trim()) return; started = true;",
      "  send({ type: 'response', command: 'prompt', success: true }); send({ type: 'agent_start' });",
      "  send({ type: 'message_end', message }); send({ type: 'turn_end', message, toolResults: [] });",
      "  send({ type: 'agent_end', messages: [message], willRetry: false }); send({ type: 'agent_settled' });",
      "});",
      "process.stdin.on('end', () => setTimeout(() => process.exit(0), 5));",
    ].join("\n"), { mode: 0o755 });
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"), piBinary: fakePi, runRoot: path.join(root, "runs"),
    });
    closers.push(() => agents.close());
    const surface = async (actorId?: string, bashIdleSeconds?: number) => {
      const result = await agents.run({
        task: "report", transport: "process", runner: "pi", extensions: false, tools: ["bash"], timeoutMs: 10_000,
        ...(actorId ? { actorId, bashTimeoutSeconds: 2 } : {}),
        ...(bashIdleSeconds !== undefined ? { bashIdleSeconds } : {}),
      });
      expect(result.status).toBe("completed");
      return JSON.parse(result.text) as { argv: string[]; timeout?: string; idle?: string };
    };
    const hook = path.resolve("src/guards/actor-bash-hook.ts");
    const actor = await surface("actor:native");
    expect(actor.argv).toContain("--no-extensions");
    expect(actor.argv[actor.argv.indexOf(hook) - 1]).toBe("-e");
    expect(actor.argv[actor.argv.indexOf("--tools") + 1]).toBe("bash"); // tools are not widened
    expect(actor.timeout).toBe("2");
    expect(actor.idle).toBe(String(DEFAULT_BASH_IDLE_S));
    const task = await surface();
    expect(task.argv[task.argv.indexOf(hook) - 1]).toBe("-e");
    expect(task).toMatchObject({ idle: "180" });
    expect(task.timeout).toBeUndefined();
    expect(await surface(undefined, 0)).toMatchObject({ idle: "0" });
    expect(await surface(undefined, 45)).toMatchObject({ idle: "45" });
  });
});

// smarty-dev#2339 F4: actor B's unset override must not inherit actor A's timeout.
describe("nested actor bash timeout isolation (smarty-dev#2339 F4)", () => {
  const roots: string[] = [];
  const closers: Array<() => Promise<void>> = [];
  afterEach(async () => {
    vi.unstubAllEnvs();
    for (const close of closers.splice(0)) await close();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it.each(["0", "2"])("clears parent timeout %s in the real worker and keeps the child's own 7 s override", { timeout: 20_000 }, async (inherited) => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "actor:parent");
    vi.stubEnv("PI_FABRIC_ACTOR_BASH_TIMEOUT_S", inherited);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-nested-actor-bash-"));
    roots.push(root);
    const fakePi = path.join(root, "fake-pi.mjs");
    fs.writeFileSync(fakePi, [
      "#!/usr/bin/env node",
      "import readline from 'node:readline';",
      "const send = (event) => process.stdout.write(JSON.stringify(event) + '\\n');",
      "const text = JSON.stringify({ actorId: process.env.PI_FABRIC_ACTOR_ID, timeout: process.env.PI_FABRIC_ACTOR_BASH_TIMEOUT_S });",
      "const message = { role: 'assistant', content: [{ type: 'text', text }], provider: 'fake', model: 'fake', usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0 }, stopReason: 'stop' };",
      "let started = false;",
      "readline.createInterface({ input: process.stdin }).on('line', (line) => {",
      "  if (started || !line.trim()) return; started = true;",
      "  send({ type: 'response', command: 'prompt', success: true }); send({ type: 'agent_start' });",
      "  send({ type: 'message_end', message }); send({ type: 'turn_end', message, toolResults: [] });",
      "  send({ type: 'agent_end', messages: [message], willRetry: false }); send({ type: 'agent_settled' });",
      "});",
      "process.stdin.on('end', () => setTimeout(() => process.exit(0), 5));",
    ].join("\n"), { mode: 0o755 });
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"), piBinary: fakePi, runRoot: path.join(root, "runs"),
    });
    closers.push(() => agents.close());
    const surface = async (bashTimeoutSeconds?: number) => {
      const result = await agents.run({
        task: "report", transport: "process", runner: "pi", extensions: false, tools: ["bash"], timeoutMs: 10_000,
        actorId: "actor:child",
        ...(bashTimeoutSeconds === undefined ? {} : { bashTimeoutSeconds }),
      });
      expect(result.status).toBe("completed");
      return JSON.parse(result.text) as { actorId: string; timeout?: string };
    };
    const defaults = await surface();
    expect(defaults.actorId).toBe("actor:child");
    expect(defaults).not.toHaveProperty("timeout");
    expect(actorBashTimeout({ PI_FABRIC_ACTOR_ID: defaults.actorId, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: defaults.timeout }, undefined)).toBe(600);
    const override = await surface(7);
    expect(override.timeout).toBe("7");
    expect(actorBashTimeout({ PI_FABRIC_ACTOR_ID: override.actorId, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: override.timeout }, undefined)).toBe(7);
  });
});
