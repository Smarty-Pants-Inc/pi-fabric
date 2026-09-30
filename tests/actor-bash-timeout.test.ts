import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { actorBashTimeout, DEFAULT_ACTOR_BASH_TIMEOUT_S } from "../src/guards/actor-bash-timeout.js";
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

  it("takes the actor's override, and 0 turns the default off", () => {
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "45" }, undefined)).toBe(45);
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "0" }, undefined)).toBeUndefined();
    expect(actorBashTimeout({ ...actor, PI_FABRIC_ACTOR_BASH_TIMEOUT_S: "junk" }, undefined)).toBe(600);
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

  it("leaves a non-actor session's bash call alone", async () => {
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    expect((await bashCall({ command: "ls" })).input.timeout).toBeUndefined();
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
    expect((await hookCall({ command: "sleep 30" })).timeout).toBeUndefined();
  });

  it("the real worker loads it into a native-tool actor run with --no-extensions, and only into actor runs", { timeout: 20_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-bash-hook-"));
    roots.push(root);
    const fakePi = path.join(root, "fake-pi.mjs");
    fs.writeFileSync(fakePi, [
      "#!/usr/bin/env node",
      "import readline from 'node:readline';",
      "const send = (event) => process.stdout.write(JSON.stringify(event) + '\\n');",
      "const text = JSON.stringify({ argv: process.argv.slice(2), timeout: process.env.PI_FABRIC_ACTOR_BASH_TIMEOUT_S });",
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
    const surface = async (actorId?: string) => {
      const result = await agents.run({
        task: "report", transport: "process", runner: "pi", extensions: false, tools: ["bash"], timeoutMs: 10_000,
        ...(actorId ? { actorId, bashTimeoutSeconds: 2 } : {}),
      });
      expect(result.status).toBe("completed");
      return JSON.parse(result.text) as { argv: string[]; timeout?: string };
    };
    const hook = path.resolve("src/guards/actor-bash-hook.ts");
    const actor = await surface("actor:native");
    expect(actor.argv).toContain("--no-extensions");
    expect(actor.argv[actor.argv.indexOf(hook) - 1]).toBe("-e");
    expect(actor.argv[actor.argv.indexOf("--tools") + 1]).toBe("bash"); // tools are not widened
    expect(actor.timeout).toBe("2");
    expect((await surface()).argv).not.toContain(hook);
  });
});
