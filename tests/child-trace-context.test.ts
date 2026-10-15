import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentHandleInfo, AgentRunRequest } from "../src/agents/types.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { requestChildTraceContext } from "../src/agents/trace-context-request.js";
import { readTraceContext } from "../src/agents/trace-context.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import {
  FABRIC_CHILD_TRACE_CONTEXT_EVENT,
  type FabricChildTraceContextRequestV1,
  type FabricInvocationContext,
  type FabricTraceContextV1,
} from "../src/protocol.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { parseWorkerOptions } from "../src/worker/options.js";

const TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const OTHER = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  delete process.env.FAKE_PI_BEHAVIOR;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (prefix: string): string => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  roots.push(root);
  return root;
};

// Records every launch's argv beside the status file; "RETRY" fails the first attempt retryably.
const workerSource = `
import fs from "node:fs";
import path from "node:path";
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index].slice(2), process.argv[index + 1]);
const statusFile = args.get("status-file");
const task = fs.readFileSync(args.get("task-file"), "utf8");
const directory = path.dirname(statusFile);
fs.mkdirSync(directory, { recursive: true });
const attempt = fs.readdirSync(directory).filter(name => name.startsWith("argv-")).length + 1;
fs.writeFileSync(path.join(directory, "argv-" + attempt + ".json"), JSON.stringify(Object.fromEntries(args)));
const failed = task.includes("RETRY") && attempt === 1;
const now = Date.now();
fs.writeFileSync(statusFile, JSON.stringify({
  id: args.get("id"), name: args.get("name"), task, status: failed ? "failed" : "completed",
  runner: args.get("runner"), transport: args.get("transport"), cwd: process.cwd(),
  startedAt: now, updatedAt: now, finishedAt: now, turns: failed ? 0 : 1, toolCalls: 0, text: failed ? "" : "done", exitCode: 0,
  ...(failed ? { error: "No API key found for openai-codex" } : {}),
  usage: { input: failed ? 0 : 1, output: failed ? 0 : 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
}));
fs.writeFileSync(args.get("log-file"), JSON.stringify({ type: "agent_start" }) + "\\n");
`;

const createManager = (options: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const root = tempRoot("pi-fabric-trace-context-");
  const workerPath = path.join(root, "worker.mjs");
  fs.writeFileSync(workerPath, workerSource);
  const runRoot = path.join(root, "runs");
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000 }, {
    workerPath, runRoot, fullCodeMode: false, ...options,
  });
  managers.push(manager);
  const launches = (id: string): Record<string, string>[] => fs.readdirSync(path.join(runRoot, id))
    .filter(name => name.startsWith("argv-")).sort()
    .map(name => JSON.parse(fs.readFileSync(path.join(runRoot, id, name), "utf8")) as Record<string, string>);
  return { manager, runRoot, launches };
};

describe("W3C trace context values", () => {
  it("accepts a traceparent with an optional tracestate and keeps nothing else", () => {
    expect(readTraceContext({ traceparent: TRACEPARENT })).toEqual({ traceparent: TRACEPARENT });
    expect(readTraceContext({ traceparent: TRACEPARENT, tracestate: "vendor=a,other=b", PATH: "/evil", env: { X: "1" } }))
      .toEqual({ traceparent: TRACEPARENT, tracestate: "vendor=a,other=b" });
  });

  it.each([
    ["no object", TRACEPARENT],
    ["an array", [{ traceparent: TRACEPARENT }]],
    ["a missing traceparent", { tracestate: "vendor=a" }],
    ["uppercase hex", { traceparent: TRACEPARENT.toUpperCase() }],
    ["the forbidden version", { traceparent: `ff${TRACEPARENT.slice(2)}` }],
    ["an all-zero trace id", { traceparent: `00-${"0".repeat(32)}-00f067aa0ba902b7-01` }],
    ["an all-zero parent id", { traceparent: `00-4bf92f3577b34da6a3ce929d0e0e4736-${"0".repeat(16)}-01` }],
    ["trailing content", { traceparent: `${TRACEPARENT}\nEVIL=1` }],
  ])("rejects %s", (_label, value) => {
    expect(readTraceContext(value)).toBeUndefined();
  });

  it.each([
    "not-a-list", "vendor=a,vendor=b", "vendor=a , vendor=b", "", "vendor=a,,other=b", "vendor=a,",
    "Vendor=a", "1vendor=a", "ven.dor=a", "vendor =a", "@vendor=a", "_tenant@vendor=a",
    "ten.ant@vendor=a", "tenant@1vendor=a", "tenant@ven.dor=a", "tenant@vendor@other=a",
    `${"a".repeat(257)}=v`, `${"a".repeat(242)}@v=a`, `t@${"a".repeat(15)}=v`,
    "vendor=", "vendor=   ", `vendor=${"a".repeat(257)}`, "vendor=a=b", "vendor=a,b",
    `a=${"x".repeat(255)},b=${"y".repeat(253)}`, 7, "vendor=é",
    Array.from({ length: 33 }, (_, i) => `v${i}=a`).join(","),
    ...Array.from({ length: 33 }, (_, i) => `vendor=a${String.fromCharCode(i === 32 ? 127 : i)}b`),
    "vendor=a,\tother=b", "\tvendor=a", "vendor=a\n",
  ])("drops invalid state %j but keeps the parent and strips unknown fields", tracestate => {
    expect(readTraceContext({ traceparent: TRACEPARENT, tracestate, PATH: "/evil", env: { EVIL: "1" } }))
      .toEqual({ traceparent: TRACEPARENT });
  });

  it.each([
    "vendor=a,other=b", " vendor=a  ,  other=b ", "1tenant@vendor=opaque Value!~", "a0_*/-=value",
    `${"a".repeat(256)}=v`, `${"a".repeat(241)}@${"s".repeat(14)}=v`,
    `vendor=${"a".repeat(256)}`, `vendor= ${"a".repeat(255)}`,
    `a=${"x".repeat(255)},b=${"y".repeat(252)}`,
    Array.from({ length: 32 }, (_, i) => `v${i}=a`).join(","),
  ])("keeps valid state verbatim: %s", tracestate => {
    expect(readTraceContext({ traceparent: TRACEPARENT, tracestate }))
      .toEqual({ traceparent: TRACEPARENT, tracestate });
  });
});

describe("child trace context event", () => {
  const run = { runId: "run-1", runner: "pi", name: "Probe", parentToolCallId: "call_outer", nestedToolCallId: "fabric_nested" };

  it("offers the run to listeners and takes the first synchronous reply", () => {
    const seen: unknown[] = [];
    const emit = vi.fn((event: string, value: unknown) => {
      seen.push(event, { ...(value as object) });
      const request = value as FabricChildTraceContextRequestV1;
      request.reply({ traceparent: TRACEPARENT, tracestate: "vendor=a" });
      request.reply({ traceparent: OTHER });
    });
    expect(requestChildTraceContext(emit, run)).toEqual({ traceparent: TRACEPARENT, tracestate: "vendor=a" });
    expect(seen).toEqual([FABRIC_CHILD_TRACE_CONTEXT_EVENT, { version: 1, ...run, reply: expect.any(Function) }]);
  });

  it("drops bad state without throwing or aborting later synchronous replies", () => {
    let reachedSecondReply = false;
    const context = requestChildTraceContext((_event, value) => {
      const request = value as FabricChildTraceContextRequestV1;
      request.reply({ traceparent: TRACEPARENT, tracestate: "not-a-list" });
      request.reply({ traceparent: OTHER, tracestate: "vendor=valid" });
      reachedSecondReply = true;
    }, run);
    expect(reachedSecondReply).toBe(true);
    expect(context).toEqual({ traceparent: TRACEPARENT });
  });

  it("returns nothing when no listener replies, and omits identifiers the launch does not have", () => {
    const emit = vi.fn();
    expect(requestChildTraceContext(emit, { runner: "claude", name: "Actor turn" })).toBeUndefined();
    expect(Object.keys(emit.mock.calls[0]![1] as object).sort()).toEqual(["name", "reply", "runner", "version"]);
  });

  it("tells the listener about an invalid or late reply and launches without one", async () => {
    let late: FabricChildTraceContextRequestV1 | undefined;
    const invalid = requestChildTraceContext((_event, value) => {
      late = value as FabricChildTraceContextRequestV1;
      expect(() => late!.reply({ traceparent: "not-a-traceparent" })).toThrow("Invalid Pi Fabric child trace context");
    }, run);
    expect(invalid).toBeUndefined();
    expect(() => late!.reply({ traceparent: TRACEPARENT })).toThrow(/synchronously/);
  });

  it("never lets a throwing listener block a launch", () => {
    expect(requestChildTraceContext(() => { throw new Error("tracer down"); }, run)).toBeUndefined();
  });
});

describe("agent manager trace context", () => {
  it("asks once per launch and hands the reply to the worker", async () => {
    const asked: unknown[] = [];
    const { manager, launches } = createManager({
      resolveChildTraceContext: (launch) => {
        asked.push(launch);
        return { traceparent: TRACEPARENT, tracestate: "vendor=a" };
      },
    });
    const result = await manager.run({
      task: "one", name: "Probe", runner: "pi", transport: "process",
      traceOrigin: { parentToolCallId: "call_outer", nestedToolCallId: "fabric_nested" },
    });
    expect(asked).toEqual([{ runId: result.id, runner: "pi", name: "Probe", parentToolCallId: "call_outer", nestedToolCallId: "fabric_nested" }]);
    expect(launches(result.id).map(args => JSON.parse(args["trace-context"]!)))
      .toEqual([{ traceparent: TRACEPARENT, tracestate: "vendor=a" }]);
  });

  it("launches with the parent after a listener replies with invalid state", async () => {
    const { manager, launches } = createManager({
      resolveChildTraceContext: launch => requestChildTraceContext((_event, value) => {
        const request = value as FabricChildTraceContextRequestV1;
        request.reply({ traceparent: TRACEPARENT, tracestate: "not-a-list" });
        request.reply({ traceparent: OTHER, tracestate: "vendor=valid" });
      }, launch),
    });
    const result = await manager.run({ task: "one", transport: "process" });
    expect(result.status).toBe("completed");
    expect(JSON.parse(launches(result.id)[0]!["trace-context"]!)).toEqual({ traceparent: TRACEPARENT });
  });

  it("passes no trace context when nobody replies", async () => {
    const { manager, launches } = createManager({ resolveChildTraceContext: () => undefined });
    const result = await manager.run({ task: "one", transport: "process" });
    expect(launches(result.id)).toHaveLength(1);
    expect(launches(result.id)[0]).not.toHaveProperty("trace-context");
  });

  it("asks again before a relaunch and keeps the earlier context when nobody replies", async () => {
    const replies: (FabricTraceContextV1 | undefined)[] = [{ traceparent: TRACEPARENT }, { traceparent: OTHER }];
    const asked: unknown[] = [];
    const { manager, launches } = createManager({
      resolveChildTraceContext: (launch) => {
        asked.push(launch);
        return replies.shift();
      },
    });
    const refreshed = await manager.run({ task: "RETRY refreshed", name: "Retry", runner: "pi", transport: "process", traceOrigin: { parentToolCallId: "call_outer" } });
    expect(refreshed.status).toBe("completed");
    expect(launches(refreshed.id).map(args => JSON.parse(args["trace-context"]!).traceparent)).toEqual([TRACEPARENT, OTHER]);
    expect(asked).toEqual(Array.from({ length: 2 }, () => ({ runId: refreshed.id, runner: "pi", name: "Retry", parentToolCallId: "call_outer" })));

    replies.push({ traceparent: TRACEPARENT });
    const kept = await manager.run({ task: "RETRY kept", transport: "process" });
    expect(launches(kept.id).map(args => JSON.parse(args["trace-context"]!).traceparent)).toEqual([TRACEPARENT, TRACEPARENT]);
  });

  it("uses a forwarded context as is, for hosts without the session's extensions", async () => {
    const resolveChildTraceContext = vi.fn(() => ({ traceparent: OTHER }));
    const { manager, launches, runRoot } = createManager({ resolveChildTraceContext });
    const result = await manager.run({ task: "one", transport: "process", residency: "durable", traceContext: { traceparent: TRACEPARENT } });
    expect(JSON.parse(launches(result.id)[0]!["trace-context"]!)).toEqual({ traceparent: TRACEPARENT });
    expect(resolveChildTraceContext).not.toHaveBeenCalled();

    const before = fs.readdirSync(runRoot);
    await expect(manager.spawn({ task: "one", transport: "process", traceContext: { traceparent: "forged\nPATH=/evil" } }))
      .rejects.toThrow("Invalid forwarded trace context");
    expect(fs.readdirSync(runRoot)).toEqual(before);
  });

  it("resolves the context a session forwards before the resident host mints the run id", () => {
    const asked: unknown[] = [];
    const { manager } = createManager({
      resolveChildTraceContext: (launch) => {
        asked.push(launch);
        return { traceparent: TRACEPARENT };
      },
    });
    const forwarded = manager.forwardedTraceContext({
      task: "Durable work\nmore", runner: "claude", residency: "durable", traceOrigin: { nestedToolCallId: "fabric_nested" },
    });
    expect(forwarded).toEqual({ traceparent: TRACEPARENT });
    expect(asked).toEqual([{ runner: "claude", name: "Durable work", nestedToolCallId: "fabric_nested" }]);
  });

  it("keeps trace fields out of every agent action schema", () => {
    for (const descriptor of AGENTS_ACTION_DESCRIPTORS) {
      const properties = (descriptor.inputSchema as { properties?: Record<string, unknown> }).properties ?? {};
      expect(properties, descriptor.name).not.toHaveProperty("traceContext");
      expect(properties, descriptor.name).not.toHaveProperty("traceOrigin");
    }
    // A program that passes them anyway never reaches a launch with them.
    const request = normalizeAgentRunRequest(
      { task: "t", traceContext: { traceparent: OTHER }, traceOrigin: { parentToolCallId: "forged" } },
      { ...DEFAULT_FABRIC_CONFIG.agents, models: DEFAULT_FABRIC_CONFIG.models },
    );
    expect(request).not.toHaveProperty("traceContext");
    expect(request).not.toHaveProperty("traceOrigin");
  });
});

describe("agents provider trace context", () => {
  const context = {
    cwd: process.cwd(), signal: undefined, parentToolCallId: "call_outer", nestedToolCallId: "fabric_nested",
    extensionContext: { modelRegistry: { getAvailable: () => [], find: () => undefined } },
    update() {}, activity() {},
  } as unknown as FabricInvocationContext;
  const handle: AgentHandleInfo = { id: "probe", name: "probe", status: "running", runner: "pi", transport: "process", cwd: process.cwd() };
  type ProviderArgs = ConstructorParameters<typeof AgentsProvider>;
  const setup = () => {
    const asked: unknown[] = [];
    const { manager } = createManager({
      resolveChildTraceContext: (launch) => {
        asked.push(launch);
        return { traceparent: TRACEPARENT };
      },
    });
    const spawn = vi.spyOn(manager, "spawn").mockResolvedValue(handle);
    vi.spyOn(manager, "detachSignal").mockImplementation(() => {});
    const residency = { spawnAgent: vi.fn(async (_request: AgentRunRequest) => handle), ensureHost: vi.fn(async () => {}) };
    const provider = new AgentsProvider(
      manager, {} as ProviderArgs[1], {} as ProviderArgs[2], {} as ProviderArgs[3],
      { scheduleRefresh() {}, async refresh() {} } as unknown as ProviderArgs[4],
      undefined, {} as ProviderArgs[6], () => false, residency as unknown as ProviderArgs[8], false,
    );
    return { provider, spawn, residency, asked };
  };

  it("names the calling tool calls on session launches and leaves the question to the manager", async () => {
    const { provider, spawn, asked } = setup();
    await provider.invoke("spawn", { task: "Session work", runner: "pi" }, context);
    expect(spawn.mock.calls[0]![0]).toMatchObject({ traceOrigin: { parentToolCallId: "call_outer", nestedToolCallId: "fabric_nested" } });
    expect(spawn.mock.calls[0]![0]).not.toHaveProperty("traceContext");
    expect(asked).toEqual([]);
  });

  it("asks this session's listeners before forwarding a durable spawn to the resident host", async () => {
    const { provider, spawn, residency, asked } = setup();
    await provider.invoke("spawn", { task: "Durable work", runner: "pi", residency: "durable", traceContext: { traceparent: OTHER } }, context);
    expect(asked).toEqual([{ runner: "pi", name: "Durable work", parentToolCallId: "call_outer", nestedToolCallId: "fabric_nested" }]);
    expect(residency.spawnAgent.mock.calls[0]![0]).toMatchObject({ residency: "durable", traceContext: { traceparent: TRACEPARENT } });
    expect(spawn).not.toHaveBeenCalled();
  });
});

describe("worker trace context", () => {
  const base = [
    "node", "worker.js", "--id", "w", "--name", "n", "--runner", "pi", "--task-file", "t",
    "--status-file", "s", "--lifecycle-file", "l", "--log-file", "g", "--cwd", "/",
    "--pi-binary", "pi", "--claude-binary", "c", "--veda-binary", "v", "--veda-backend", "b",
    "--veda-persona", "p", "--timeout-ms", "1", "--depth", "1", "--full-code-mode", "false",
    "--extensions", "false", "--tools", "[]", "--granted-risks", "[]", "--transport", "process",
  ];

  it("parses valid parents, drops bad state, and refuses invalid parents", () => {
    expect(parseWorkerOptions(base)).not.toHaveProperty("traceContext");
    expect(parseWorkerOptions([...base, "--trace-context", JSON.stringify({ traceparent: TRACEPARENT, tracestate: "vendor=a" })]).traceContext)
      .toEqual({ traceparent: TRACEPARENT, tracestate: "vendor=a" });
    expect(parseWorkerOptions([...base, "--trace-context", JSON.stringify({ traceparent: TRACEPARENT, PATH: "/evil" })]).traceContext)
      .toEqual({ traceparent: TRACEPARENT });
    for (const tracestate of ["not-a-list", "vendor=a,vendor=b", "a\nb", "vendor=a,\tother=b", ""]) {
      expect(parseWorkerOptions([...base, "--trace-context", JSON.stringify({ traceparent: TRACEPARENT, tracestate, EVIL: "1" })]).traceContext)
        .toEqual({ traceparent: TRACEPARENT });
    }
    for (const forged of ["{", "{}", JSON.stringify({ traceparent: "x" })]) {
      expect(() => parseWorkerOptions([...base, "--trace-context", forged])).toThrow("Invalid worker trace context");
    }
  });

  // The source worker selects Bun, whose native cleanup cannot inherit our Node deletion guard.
  // Keep that variant hosted-only; the built Node worker exercises the real child path locally.
  const workers = ["src/worker.ts", "dist/worker.js"].filter(worker => worker.startsWith("src/") || fs.existsSync(path.resolve(worker)));
  for (const worker of workers) {
  it.skipIf(worker.startsWith("src/") && process.env.GITHUB_ACTIONS !== "true")(`exports the context to the child and otherwise leaves its environment alone (${worker})`, async () => {
    process.env.FAKE_PI_BEHAVIOR = "child-contract";
    // The multiplexer or resident host that owns the worker may carry a stale context.
    vi.stubEnv("TRACEPARENT", OTHER);
    vi.stubEnv("TRACESTATE", "stale=1");
    const contexts: (FabricTraceContextV1 | undefined)[] = [{ traceparent: TRACEPARENT }, { traceparent: TRACEPARENT, tracestate: "not-a-list" }, { traceparent: TRACEPARENT, tracestate: "vendor=a" }, undefined];
    const root = tempRoot("pi-fabric-trace-worker-");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 20_000 }, {
      workerPath: path.resolve(worker),
      piBinary: path.resolve("tests/fixtures/fake-pi.mjs"),
      runRoot: path.join(root, "runs"),
      fullCodeMode: false,
      resolveChildTraceContext: () => contexts.shift(),
    });
    managers.push(manager);
    const reports: { traceparent: string | null; tracestate: string | null }[] = [];
    for (let index = 0; index < 4; index++) {
      const result = await manager.run({ task: "report", runner: "pi", transport: "process", extensions: false });
      expect(result.status, result.error).toBe("completed");
      const { traceparent, tracestate } = JSON.parse(result.text) as { traceparent: string | null; tracestate: string | null };
      reports.push({ traceparent, tracestate });
    }
    expect(reports).toEqual([
      { traceparent: TRACEPARENT, tracestate: null },
      { traceparent: TRACEPARENT, tracestate: null },
      { traceparent: TRACEPARENT, tracestate: "vendor=a" },
      { traceparent: OTHER, tracestate: "stale=1" },
    ]);
  }, 60_000);
  }
});
