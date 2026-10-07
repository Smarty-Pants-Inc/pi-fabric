import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { DEFAULT_JEV_CONFIG } from "../src/jev/config.js";
import type { JevRequest, JevResponse } from "../src/jev/types.js";
import { DEFAULT_PER_CALL_ROUTING, parsePerCallRouting, type PerCallRoutingConfig } from "../src/agents/per-call-config.js";
import {
  classifyStep, PerCallShadowRouter, rotatePerCallLedger, stepFacts, type PerCallRecord, type PerCallSettings,
} from "../src/agents/per-call-route.js";

const loaded = vi.hoisted(() => vi.fn());
const MAIN = { provider: "cliproxyapi-anthropic", model: "claude-opus-5.5" };
const user = (text = "do it") => ({ role: "user", content: text, timestamp: 1 });
const call = (name: string, args: Record<string, unknown> = {}) => ({ name, arguments: args });
const assistant = (calls: Array<{ name: string; arguments: Record<string, unknown> }>, model = MAIN) => ({
  role: "assistant", ...model, api: "anthropic-messages", stopReason: calls.length ? "toolUse" : "stop", timestamp: 2,
  content: calls.map((c, i) => ({ type: "toolCall", id: `t${i}`, name: c.name, arguments: c.arguments })),
  usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
});
const result = (isError = false) => ({ role: "toolResult", toolCallId: "t0", toolName: "x", content: [], isError, timestamp: 3 });
const config = (over: Partial<PerCallRoutingConfig> = {}): PerCallRoutingConfig => ({ ...DEFAULT_PER_CALL_ROUTING, mode: "shadow", ...over });
const classify = (messages: unknown[], tokens = 10_000, over: Partial<PerCallRoutingConfig> = {}) =>
  classifyStep(stepFacts(messages, tokens, config(over).authorTurns), config(over));

describe("per-call rule classifier", () => {
  it("a new user message is MAIN (user-message)", () => {
    expect(classify([user()])).toEqual({ decision: "main", rule: "user-message" });
    expect(classify([user(), assistant([call("read")]), result(), user("next")])).toEqual({ decision: "main", rule: "user-message" });
  });
  it("a step not ending in tool results only is MAIN (no-tool-results)", () => {
    expect(classify([user(), assistant([])])).toEqual({ decision: "main", rule: "no-tool-results" });
    expect(classify([user(), assistant([call("read")]), result(), { role: "custom", customType: "x", content: "" }]))
      .toEqual({ decision: "main", rule: "no-tool-results" });
  });
  it("an edit or write in the last N=3 turns is MAIN (author-phase), older ones are not", () => {
    const reads = [assistant([call("read")]), result()];
    expect(classify([user(), assistant([call("edit")]), result(), ...reads])).toEqual({ decision: "main", rule: "author-phase" });
    expect(classify([user(), assistant([call("fabric_exec", { code: "await pi.write({path:'a',text:'b'})" })]), result(), ...reads]))
      .toEqual({ decision: "main", rule: "author-phase" });
    // Four assistant turns back: outside N=3.
    expect(classify([user(), assistant([call("write")]), result(), ...reads, ...reads, ...reads]))
      .toEqual({ decision: "simple", rule: "read-only-tools" });
    expect(classify([user(), assistant([call("write")]), result(), ...reads, ...reads, ...reads], 10_000, { authorTurns: 4 }))
      .toEqual({ decision: "main", rule: "author-phase" });
  });
  it("a context over the size limit is MAIN (context-over-limit)", () => {
    const messages = [user(), assistant([call("read")]), result()];
    expect(classify(messages, 100_001)).toEqual({ decision: "main", rule: "context-over-limit" });
    expect(classify(messages, 100_000)).toEqual({ decision: "simple", rule: "read-only-tools" });
  });
  it("read/list/search tool results under the limit are SIMPLE (read-only-tools)", () => {
    expect(classify([user(), assistant([call("read"), call("grep"), call("ls")]), result(), result()])).toEqual({ decision: "simple", rule: "read-only-tools" });
    expect(classify([user(), assistant([call("fabric_exec", { code: "return [await pi.read('a'), await pi.grep('x','src')]" })]), result()]))
      .toEqual({ decision: "simple", rule: "read-only-tools" });
  });
  it("anything that could write is an author signal: bash, unknown tools, non-read pi ops, aliases and computed access", () => {
    const after = (c: ReturnType<typeof call>) => classify([user(), assistant([c]), result()]);
    const fabric = (code: string) => after(call("fabric_exec", { code }));
    const author = { decision: "main", rule: "author-phase" };
    expect(after(call("bash", { command: "echo x > f" }))).toEqual(author);
    expect(after(call("bash", { command: "ls" }))).toEqual(author);
    for (const name of ["task", "webfetch", "mcp__x__read", "pi", "unknown_tool"]) expect(after(call(name))).toEqual(author);
    // Aliased and computed pi writes evade a literal `pi.<op>(` scan; they must still count.
    expect(fabric("const run = pi['bash']; await run({cmd:'echo x > f'})")).toEqual(author);
    expect(fabric("const { bash } = pi; await bash({cmd:'echo x > f'})")).toEqual(author);
    expect(fabric("const p = pi; await p.write({path:'f', text:'x'})")).toEqual(author);
    expect(fabric("await pi[\"write\"]({path:'f', text:'x'})")).toEqual(author);
    expect(fabric("const op = 'edit'; await pi[op]({path:'f', edits:[]})")).toEqual(author);
    expect(fabric("await pi?.edit({path:'f', edits:[]})")).toEqual(author);
    expect(fabric("const r = pi.read; await pi.read('a'); await r.call(pi, 'b')")).toEqual(author);
    expect(fabric("await pi.bash({cmd:'ls'})")).toEqual(author);
    expect(fabric("await pi.read('a'); await agents.run({})")).toEqual(author);
    expect(fabric("await tools.call({ ref: 'pi.write', args: {} })")).toEqual(author);
    expect(fabric("await globalThis['p'+'i'].write({})")).toEqual(author);
    expect(fabric("await pi.grep('pi', 'src')")).toEqual(author); // a quoted "pi" is not proven harmless: conservative
    // Literal reads stay read-only, including paths that merely contain "pi".
    expect(fabric("return [await pi.read('/lanes/pi-fabric/src/pi/x.ts'), await pi . grep('TODO', 'src')]"))
      .toEqual({ decision: "simple", rule: "read-only-tools" });
  });
  it("only failed reads or operation-free programs are ambiguous (the only Jev candidates)", () => {
    expect(classify([user(), assistant([call("read")]), result(true)]).rule).toBe("ambiguous");
    expect(classify([user(), assistant([call("fabric_exec", { code: "return await pi.grep('x','src')" })]), result(true)]).rule).toBe("ambiguous");
    expect(classify([user(), assistant([call("fabric_exec", { code: "return 1 + 1" })]), result()]).rule).toBe("ambiguous");
  });
});

let dir: string;
beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), "per-call-")); });
afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); vi.unstubAllEnvs(); });

const settingsFor = (over: Partial<PerCallRoutingConfig> = {}): PerCallSettings => ({ perCall: config(over), jev: { ...DEFAULT_JEV_CONFIG } });
const readLedger = (file: string): PerCallRecord[] =>
  fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as PerCallRecord);
const noul = (p: number): JevResponse => ({ model: "jev-1.13.0", answers: { simple: { type: "noul", noul: p } }, usage: { input_tokens: 0, output_tokens: 0 } });
const snapshot = (messages: unknown[], sessionId = "s1", contextTokens: number | null = 20_000) =>
  ({ sessionId, turnIndex: 4, messages, model: `${MAIN.provider}/${MAIN.model}`, contextTokens });

describe("per-call shadow ledger", () => {
  it("writes one line per call with every field, including the provider usage", async () => {
    const ledger = path.join(dir, "fabric", "per-call-routing.jsonl");
    const router = new PerCallShadowRouter({ settings: () => settingsFor(), ledger, evaluate: async () => noul(0.99) });
    router.onContext(snapshot([user(), assistant([call("read")]), result()]));
    router.onMessageEnd("s1", { ...assistant([], { provider: "cliproxyapi-anthropic", model: "claude-opus-5.5" }), stopReason: "stop",
      usage: { input: 1200, output: 80, cacheRead: 9000, cacheWrite: 300, cacheWrite1h: 0, totalTokens: 10580, cost: { total: 0.0123 } } });
    router.onContext(snapshot([user("new task")], "s1", null));
    await router.close();
    const [first, second] = readLedger(ledger);
    expect(Object.keys(first!).sort()).toEqual(["at", "callIndex", "contextTokens", "contextTokensSource", "decidedBy", "decision", "jev",
      "maxContextTokens", "mode", "model", "previousCallModel", "responseModel", "rule", "sameAsPrevious", "sessionId", "stepShape",
      "stopReason", "turnIndex", "type", "usage", "v"]);
    expect(first).toMatchObject({ v: 1, type: "per-call", mode: "shadow", sessionId: "s1", turnIndex: 4, callIndex: 1,
      decision: "simple", decidedBy: "rule", rule: "read-only-tools", jev: null, contextTokens: 20_000, contextTokensSource: "pi-usage",
      model: "cliproxyapi-anthropic/claude-opus-5.5", previousCallModel: "cliproxyapi-anthropic/claude-opus-5.5", sameAsPrevious: true,
      responseModel: "cliproxyapi-anthropic/claude-opus-5.5", stopReason: "stop",
      usage: { input: 1200, output: 80, cacheRead: 9000, cacheWrite: 300, cacheWrite1h: 0, totalTokens: 10580, costUsd: 0.0123 } });
    // An open call is written at close without usage; an estimate replaces a missing Pi figure.
    expect(second).toMatchObject({ callIndex: 2, decision: "main", rule: "user-message", contextTokensSource: "estimate",
      previousCallModel: null, sameAsPrevious: null, usage: null, responseModel: null });
    expect(fs.statSync(ledger).mode & 0o777).toBe(0o600);
  });

  it("records a model change against the previous call", async () => {
    const ledger = path.join(dir, "fabric", "per-call-routing.jsonl");
    const router = new PerCallShadowRouter({ settings: () => settingsFor(), ledger });
    router.onContext(snapshot([user(), assistant([call("read")], { provider: "cliproxyapi", model: "gpt-6-luna" }), result()]));
    await router.close();
    expect(readLedger(ledger)[0]).toMatchObject({ previousCallModel: "cliproxyapi/gpt-6-luna", sameAsPrevious: false });
  });

  it("rotates daily and keeps seven days", () => {
    const ledger = path.join(dir, "per-call-routing.jsonl");
    const now = new Date("2026-10-07T12:00:00Z");
    fs.writeFileSync(ledger, "{}\n");
    fs.utimesSync(ledger, new Date("2026-10-06T23:00:00Z"), new Date("2026-10-06T23:00:00Z"));
    for (const day of ["2026-09-29", "2026-09-30", "2026-10-01", "2026-10-05"]) fs.writeFileSync(path.join(dir, `per-call-routing.${day}.jsonl`), "{}\n");
    fs.writeFileSync(path.join(dir, "unrelated.2026-01-01.jsonl"), "");
    expect(rotatePerCallLedger(ledger, now)).toBe(true);
    expect(fs.readdirSync(dir).sort()).toEqual(["per-call-routing.2026-10-01.jsonl", "per-call-routing.2026-10-05.jsonl",
      "per-call-routing.2026-10-06.jsonl", "unrelated.2026-01-01.jsonl"]);
    fs.writeFileSync(ledger, "{}\n");
    expect(rotatePerCallLedger(ledger, now)).toBe(false);
    expect(fs.existsSync(ledger)).toBe(true);
  });
});

describe("Jev for ambiguous steps only, within budget", () => {
  const failed = (name: string, file = "make.txt") => [user(), assistant([call(name, { path: file })]), result(true)];
  it("never asks Jev for rule-decided steps; asks once per step shape and caches it", async () => {
    const ledger = path.join(dir, "fabric", "per-call-routing.jsonl");
    const evaluate = vi.fn(async (_request: JevRequest) => noul(0.95));
    const router = new PerCallShadowRouter({ settings: () => settingsFor(), ledger, evaluate });
    router.onContext(snapshot([user()]));
    router.onContext(snapshot([user(), assistant([call("read")]), result()]));
    router.onContext(snapshot([user(), assistant([call("edit")]), result(), ...failed("read").slice(1)]));
    expect(evaluate).not.toHaveBeenCalled();
    router.onContext(snapshot(failed("read")));
    router.onContext(snapshot(failed("read", "make test.txt"), "s2"));
    await router.close();
    expect(evaluate).toHaveBeenCalledTimes(1);
    const request = evaluate.mock.calls[0]![0];
    // Operation labels and counts only: no transcript text, no command text.
    expect(JSON.stringify(request.state)).not.toMatch(/make|do it/);
    expect(request.questions.simple?.type).toBe("noul");
    const rows = readLedger(ledger);
    expect(rows.map(row => [row.decidedBy, row.rule, row.decision])).toEqual([
      ["rule", "user-message", "main"], ["rule", "read-only-tools", "simple"], ["rule", "author-phase", "main"],
      ["jev", "jev-noul", "simple"], ["jev", "jev-cache", "simple"],
    ]);
    expect(rows[3]!.jev).toMatchObject({ noul: 0.95, cached: false });
    expect(rows[4]!.stepShape).toBe(rows[3]!.stepShape);
  });

  it("respects the per-session budget and fails safe to MAIN", async () => {
    const ledger = path.join(dir, "fabric", "per-call-routing.jsonl");
    const evaluate = vi.fn(async () => noul(0.2));
    const router = new PerCallShadowRouter({ settings: () => settingsFor({ jevMaxCallsPerSession: 2 }), ledger, evaluate });
    for (const name of ["read", "grep", "find"]) router.onContext(snapshot(failed(name)));
    await router.close();
    expect(evaluate).toHaveBeenCalledTimes(2);
    expect(readLedger(ledger).map(row => [row.decidedBy, row.rule, row.decision])).toEqual([
      ["jev", "jev-noul", "main"], ["jev", "jev-noul", "main"], ["rule", "ambiguous-jev-budget", "main"],
    ]);
  });

  it("a Jev failure or gateway-less config leaves the step MAIN, decided by rule", async () => {
    const ledger = path.join(dir, "fabric", "per-call-routing.jsonl");
    const failing = new PerCallShadowRouter({ settings: () => settingsFor(), ledger, evaluate: async () => { throw new Error("Jev gateway refused: cap"); } });
    failing.onContext(snapshot(failed("read")));
    await failing.close();
    // No gatewaySocket: the default "gateway" mode never falls back to a direct, key-holding client.
    const offline = new PerCallShadowRouter({ settings: () => settingsFor(), ledger });
    offline.onContext(snapshot(failed("read")));
    await offline.close();
    expect(offline.jevQuestions).toBe(0);
    expect(readLedger(ledger).map(row => row.rule)).toEqual(["ambiguous-jev-error", "ambiguous-jev-off"]);
  });
});

describe("the 2.5 s routing deadline bounds all Jev work", () => {
  const failedRead = () => [user(), assistant([call("read")]), result(true)];
  it("an evaluator that never answers (and ignores its signal) is cut off at the deadline and logged MAIN", async () => {
    const ledger = path.join(dir, "fabric", "per-call-routing.jsonl");
    let seen: { signal: AbortSignal; deadline: number; at: number } | undefined;
    const router = new PerCallShadowRouter({ settings: () => settingsFor(), ledger,
      evaluate: (_request, signal, deadline) => { seen = { signal, deadline, at: Date.now() }; return new Promise<JevResponse>(() => undefined); } });
    const started = Date.now();
    router.onContext(snapshot(failedRead()));
    await router.close();
    const elapsed = Date.now() - started;
    expect(elapsed).toBeGreaterThanOrEqual(2_400);
    expect(elapsed).toBeLessThan(3_400);
    expect(seen!.deadline - seen!.at).toBeLessThanOrEqual(2_500);
    expect(seen!.signal.aborted).toBe(true);
    expect(readLedger(ledger).map(row => [row.decidedBy, row.rule, row.decision])).toEqual([["rule", "ambiguous-jev-timeout", "main"]]);
  });

  it.skipIf(process.platform === "win32")("a real gateway socket that never answers: expiresAt is the deadline, the socket is destroyed, nothing is left open", async () => {
    const sockDir = fs.mkdtempSync(path.join(os.tmpdir(), "jev-gw-"));
    fs.chmodSync(sockDir, 0o700);
    const socket = path.join(sockDir, "jev-gateway.sock");
    const lines: Array<{ line: string; at: number }> = [];
    let opened = 0;
    let closed = 0;
    const server = net.createServer(conn => {
      opened++;
      conn.setEncoding("utf8");
      let buffer = "";
      conn.on("data", chunk => { buffer += chunk; if (buffer.includes("\n")) lines.push({ line: buffer.slice(0, buffer.indexOf("\n")), at: Date.now() }); });
      conn.on("close", () => { closed++; });
      // Never answers.
    });
    await new Promise<void>(resolve => server.listen(socket, resolve));
    fs.chmodSync(socket, 0o600);
    try {
      const ledger = path.join(dir, "fabric", "per-call-routing.jsonl");
      const settings = (): PerCallSettings => ({ perCall: config({ jev: "gateway" }),
        jev: { ...DEFAULT_JEV_CONFIG, enabled: true, model: "jev-1.13.0", gatewaySocket: socket, requestTimeoutMs: 120_000 } });
      const router = new PerCallShadowRouter({ settings, ledger });
      const started = Date.now();
      router.onContext(snapshot(failedRead()));
      await router.close();
      expect(Date.now() - started).toBeLessThan(3_400);
      expect(router.jevQuestions).toBe(1);
      expect(router.openEvaluations).toBe(0);
      expect(readLedger(ledger).map(row => [row.decidedBy, row.rule, row.decision])).toEqual([["rule", "ambiguous-jev-timeout", "main"]]);
      expect(lines).toHaveLength(1);
      const sent = JSON.parse(lines[0]!.line) as { op: string; use: string; expiresAt: number };
      expect(sent).toMatchObject({ op: "systemone", use: "percall_route" });
      // The gateway's own work lifetime is the routing deadline, not jev.requestTimeoutMs (120 s here).
      expect(sent.expiresAt - started).toBeLessThanOrEqual(2_500 + 50);
      await new Promise<void>(resolve => setTimeout(resolve, 50));
      expect([opened, closed]).toEqual([1, 1]);
      expect(await new Promise<number>((resolve, reject) => server.getConnections((error, count) => error ? reject(error) : resolve(count)))).toBe(0);
    } finally {
      await new Promise<void>(resolve => server.close(() => resolve()));
      fs.rmSync(sockDir, { recursive: true, force: true });
    }
  });
});

describe("lazy, shadow-only hook", () => {
  beforeEach(() => {
    vi.resetModules();
    loaded.mockClear();
    vi.doMock("../src/agents/per-call-route.js", async () => {
      loaded();
      return vi.importActual<typeof import("../src/agents/per-call-route.js")>("../src/agents/per-call-route.js");
    });
  });
  afterEach(() => { vi.doUnmock("../src/agents/per-call-route.js"); });

  const harness = async (settings: () => PerCallSettings | undefined) => {
    type Handler = (event: unknown, context: ExtensionContext) => unknown;
    const handlers = new Map<string, Handler[]>();
    const setModel = vi.fn(async () => true);
    const pi = { on: (name: string, handler: Handler) => handlers.set(name, [...handlers.get(name) ?? [], handler]), setModel,
      setThinkingLevel: vi.fn() } as unknown as ExtensionAPI;
    const { registerLazyPerCallRouter } = await import("../src/agents/per-call-route-hook.js");
    registerLazyPerCallRouter(pi, settings);
    const ctxSetModel = vi.fn();
    const context = { sessionManager: { getSessionId: () => "lazy-session" }, model: { provider: MAIN.provider, id: MAIN.model },
      getContextUsage: () => ({ tokens: 5000, contextWindow: 200_000, percent: 2.5 }), setModel: ctxSetModel } as unknown as ExtensionContext;
    const emit = async (name: string, event: unknown) => {
      const results = [];
      for (const handler of handlers.get(name) ?? []) results.push(await handler(event, context));
      return results;
    };
    return { handlers, emit, setModel, ctxSetModel };
  };
  const turn = async (h: Awaited<ReturnType<typeof harness>>) => {
    const messages = [user(), assistant([call("read")]), result()];
    const before = structuredClone(messages);
    const results = [
      ...await h.emit("turn_start", { type: "turn_start", turnIndex: 7, timestamp: 1 }),
      ...await h.emit("context", { type: "context", messages }),
      ...await h.emit("message_end", { type: "message_end", message: assistant([]) }),
    ];
    expect(messages).toEqual(before);
    return results;
  };

  it("mode off (the default): registers cheap hooks and never loads the router", async () => {
    for (const settings of [() => undefined, () => ({ jev: DEFAULT_JEV_CONFIG }), () => settingsFor({ mode: "off" })]) {
      const h = await harness(settings);
      expect([...h.handlers.keys()].sort()).toEqual(["context", "message_end", "session_shutdown", "turn_start"]);
      expect((await turn(h)).every(value => value === undefined)).toBe(true);
      await h.emit("session_shutdown", { type: "session_shutdown" });
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    expect(loaded).not.toHaveBeenCalled();
  });

  it("mode shadow: loads once, logs, returns nothing and never calls setModel", async () => {
    vi.stubEnv("PI_CODING_AGENT_DIR", dir);
    const h = await harness(() => settingsFor());
    expect((await turn(h)).every(value => value === undefined)).toBe(true);
    expect((await turn(h)).every(value => value === undefined)).toBe(true);
    await h.emit("session_shutdown", { type: "session_shutdown" });
    expect(loaded).toHaveBeenCalledTimes(1);
    expect(h.setModel).not.toHaveBeenCalled();
    expect(h.ctxSetModel).not.toHaveBeenCalled();
    const rows = readLedger(path.join(dir, "fabric", "per-call-routing.jsonl"));
    expect(rows.map(row => [row.sessionId, row.turnIndex, row.callIndex, row.decision, row.usage?.totalTokens])).toEqual([
      ["lazy-session", 7, 1, "simple", 2], ["lazy-session", 7, 2, "simple", 2],
    ]);
    // A closed router is not reused: the next session in this process gets a fresh one (call index restarts).
    await turn(h);
    await h.emit("session_shutdown", { type: "session_shutdown" });
    expect(readLedger(path.join(dir, "fabric", "per-call-routing.jsonl")).map(row => row.callIndex)).toEqual([1, 2, 1]);
  });

  it("the router and its hook have no model-switch call at all", () => {
    for (const file of ["src/agents/per-call-route.ts", "src/agents/per-call-route-hook.ts"]) {
      expect(fs.readFileSync(path.join(__dirname, "..", file), "utf8")).not.toMatch(/\bsetModel\s*\(|setThinkingLevel\s*\(/);
    }
  });
});

describe("per-call config", () => {
  it("defaults to off, accepts shadow, refuses live and unknown values", () => {
    expect(normalizeFabricConfig({}).agents.modelRouting?.perCall).toBeUndefined();
    expect(parsePerCallRouting(undefined).mode).toBe("off");
    expect(normalizeFabricConfig({ agents: { modelRouting: { perCall: { mode: "shadow" } } } }).agents.modelRouting?.perCall)
      .toEqual({ ...DEFAULT_PER_CALL_ROUTING, mode: "shadow" });
    expect(() => normalizeFabricConfig({ agents: { modelRouting: { perCall: { mode: "live" } } } })).toThrow(/shadow only/);
    expect(() => parsePerCallRouting({ mode: "on" })).toThrow(/perCall.mode/);
    expect(() => parsePerCallRouting({ mode: "shadow", authorTurns: 0 })).toThrow(/authorTurns/);
    expect(() => parsePerCallRouting({ jev: "always" })).toThrow(/perCall.jev/);
  });
});
