import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricConfig } from "../src/config.js";
import { ApprovalController, FabricSessionApprovals } from "../src/core/approval-controller.js";
import { FabricAutoApprovalClassifier } from "../src/core/auto-approval-classifier.js";
import type { FabricRegistryInvocationContext } from "../src/core/action-registry.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { registerFabricPrincipalCapture } from "../src/fabric-provenance.js";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import { captureProcessCloseReceipts } from "./helpers/process-receipts.js";
import { FABRIC_RUN_ROOT_PREFIX, markRunRootActive, markRunRootClosed, markUnresolvedWorker, sweepTempRunRoots } from "../src/storage/retention.js";

const workerPath = path.resolve("dist/worker.js");
const pin = { model: "openai-codex/gpt-5.6-sol", effort: "high" as const };
const cheap = { model: "runinfra/glm-5-3-flash", effort: "medium" as const };
const response = { model: "jev-latest", answers: { route: { type: "choice", choice: "candidate-1", confidence: .95, probabilities: { "candidate-0": .05, "candidate-1": .95 } } }, usage: { input_tokens: 1, output_tokens: 1 } };
const roots: string[] = [];
const runtimes: FabricRuntimeState[] = [];
afterEach(async () => {
  await Promise.all(runtimes.splice(0).map(runtime => runtime.shutdown()));
  vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

async function setup(network: FabricConfig["approvals"]["network"] = "allow", attributed = false, jev?: { credentialCommand: string[] }, approvalModel?: string) {
  expect(fs.existsSync(workerPath), "Build the real worker before running these regressions").toBe(true);
  const receipts = captureProcessCloseReceipts();
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-followups-")); roots.push(cwd);
  const runRoot = path.join(cwd, FABRIC_RUN_ROOT_PREFIX + "followups");
  const agentDir = path.join(cwd, "agent");
  fs.mkdirSync(path.join(cwd, ".pi"));
  const scenario = path.join(cwd, "scenario"); fs.writeFileSync(scenario, "success");
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", runRoot);
  vi.stubEnv("PI_FABRIC_PI_BINARY", path.resolve("tests/fixtures/fake-pi-route.mjs"));
  vi.stubEnv("FAKE_MODEL_SCENARIO", scenario);
  vi.stubEnv("PI_FABRIC_GRANTED_RISKS", "");
  const credentials = vi.spyOn(JevCredentials.prototype, "resolve");
  const evaluate = vi.spyOn(JevClient.prototype, "evaluate");
  const http = vi.fn(async () => new Response(JSON.stringify(response), { status: 200 }));
  vi.stubGlobal("fetch", http);
  const models = [{ provider: "openai-codex", id: "gpt-5.6-sol" }, { provider: "runinfra", id: "glm-5-3-flash" }];
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { find: (provider: string, id: string) => models.find(model => model.provider === provider && model.id === id), getAvailable: () => models, getAll: () => models, getApiKeyAndHeaders: vi.fn(async () => ({ ok: true, apiKey: "offline-model-key", headers: {} })), getApiKeyForProvider: vi.fn(async () => "offline-test-key") },
    sessionManager: { getSessionId: () => "followups-parent", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn(), select: vi.fn(async () => undefined) },
  } as unknown as ExtensionContext;
  const config = normalizeFabricConfig({
    fullCodeMode: true, approvals: { agent: "allow", read: "allow", network, ...(approvalModel ? { model: approvalModel } : {}) }, ...(jev ? { jev } : {}),
    agents: { enabled: true, retainRuns: true, extensions: false, timeoutMs: 5000, modelRouting: { shadowCandidates: [cheap] } },
    mcp: { enabled: false, cache: { enabled: false } }, mesh: { enabled: false }, memory: { enabled: false }, records: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
  const handlers = new Map<string, (event: unknown, context: ExtensionContext) => void>();
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn(), on: (name: string, handler: (event: unknown, context: ExtensionContext) => void) => handlers.set(name, handler) } as unknown as ExtensionAPI;
  registerFabricPrincipalCapture(pi);
  if (attributed) handlers.get("context")!({ messages: [{ role: "user", provenance: { v: 1, channel: "voice", turnId: "test-turn", receivedAt: new Date().toISOString(), principal: { id: "test-caller", binding: "voice-call" } } }] }, context);
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { worker: workerPath, extension: path.resolve("dist/index.js"), residentHost: path.resolve("dist/resident-host.js"), skills: cwd } });
  runtimes.push(runtime);
  await runtime.initialize(context, config);
  const approval = new ApprovalController(runtime.config.approvals, context, runtime.sessionApprovals);
  const invocation: FabricRegistryInvocationContext = {
    cwd, signal: undefined, parentToolCallId: "followups-registry", nestedToolCallId: "followups-spawn", extensionContext: context, update() {},
    approve: (action, args) => approval.approve(action, args), audits: [], maxResultChars: 100000,
  };
  const spawn = async (signal?: AbortSignal) => {
    const handle = await runtime.registry.invoke("agents.spawn", { task: "harmless bounded lookup", model: "auto", routeClass: "bounded-lookup", pinModel: pin.model, pinThinking: pin.effort, protected: false, transport: "process" }, { ...invocation, signal }) as { id: string; routeDecision: { reasonCode: string; model: string; effort: string } };
    const result = await runtime.registry.invoke("agents.join", { id: handle.id }, { ...invocation, nestedToolCallId: "followups-join" }) as { id: string; status: string; admittedModel: string; admittedThinking: string };
    expect(result).toMatchObject({ status: "completed", admittedModel: pin.model, admittedThinking: pin.effort });
    // Windows may return the logical result before native close. Artifact
    // collection fixtures require the captured close, not a polling allowance.
    await receipts.wait(handle.id);
    const rows = fs.readFileSync(path.join(agentDir, "fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({ type: "outcome", decisionId: rows[0].decisionId, status: "completed" });
    return { handle, result, rows, run: path.join(runRoot, handle.id) };
  };
  return { cwd, runRoot, runtime, context, invocation, spawn, credentials, evaluate, http };
}

describe("SR-5 shadow routing obeys current ordinary Jev network approval through the real registry", () => {
  it.each(["deny", "ask"] as const)("agent allowed / network %s records pinned fallback without Jev evaluation, credentials or HTTP", async network => {
    const fixture = await setup(network);
    expect(await fixture.runtime.registry.describe("agents.spawn", fixture.invocation)).toMatchObject({ risk: "agent" });
    expect(await fixture.runtime.registry.describe("jev.evaluate", fixture.invocation)).toMatchObject({ risk: "network" });
    const { handle, rows } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...pin, reasonCode: "jev-error" });
    expect(rows[0]).toMatchObject({ ...pin, shadowChoice: pin, reasonCode: "jev-error" });
    expect(fixture.evaluate).not.toHaveBeenCalled();
    expect(fixture.credentials).not.toHaveBeenCalled();
    expect(fixture.context.modelRegistry.getApiKeyForProvider).not.toHaveBeenCalled();
    expect(fixture.http).not.toHaveBeenCalled();
  });
  it("an unapproved interactive ask records fallback without any queue or dialog work", async () => {
    const fixture = await setup("ask");
    Object.assign(fixture.context, { hasUI: true, mode: "rpc" });
    const serialize = vi.spyOn(fixture.runtime.sessionApprovals, "serialize");
    // Even a UI that would grant the entire session must never be consulted.
    vi.mocked(fixture.context.ui.select).mockResolvedValue("Allow network access for this session");
    const { handle } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...pin, reasonCode: "jev-error" });
    expect(serialize).not.toHaveBeenCalled(); expect(fixture.context.ui.select).not.toHaveBeenCalled();
    expect(fixture.runtime.sessionApprovals.approvedRisks.has("network")).toBe(false);
    expect(fixture.evaluate).not.toHaveBeenCalled(); expect(fixture.credentials).not.toHaveBeenCalled(); expect(fixture.http).not.toHaveBeenCalled();
  });
  it("ask with a network session grant from an ordinary tool still routes without a new dialog", async () => {
    const fixture = await setup("ask"); Object.assign(fixture.context, { hasUI: true, mode: "rpc" });
    vi.mocked(fixture.context.ui.select).mockResolvedValue("Allow network access for this session");
    const action = await fixture.runtime.registry.describe("jev.evaluate", fixture.invocation);
    await fixture.invocation.approve(action, {});
    expect(fixture.runtime.sessionApprovals.approvedRisks.has("network")).toBe(true);
    const serialize = vi.spyOn(fixture.runtime.sessionApprovals, "serialize");
    const { handle } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...cheap, reasonCode: "shadow-choice" });
    expect(serialize).not.toHaveBeenCalled(); expect(fixture.context.ui.select).toHaveBeenCalledOnce();
    expect(fixture.evaluate).toHaveBeenCalledOnce(); expect(fixture.http).toHaveBeenCalledOnce();
  });
  it("retiring the Jev generation cancels routing and joins an uncancellable credential resolver", async () => {
    const fixture = await setup();
    let release!: (key: string) => void;
    vi.mocked(fixture.context.modelRegistry.getApiKeyForProvider!).mockImplementation(() => new Promise<string>(resolve => { release = resolve; }));
    const spawned = fixture.spawn();
    let reloaded: Promise<unknown> | undefined;
    try {
      await vi.waitFor(() => expect(fixture.credentials).toHaveBeenCalledOnce());
      const signal = fixture.credentials.mock.calls[0]![0];
      let settled = false;
      reloaded = fixture.runtime.registry.invoke("components.reload", { id: "fabric.provider.jev" }, { ...fixture.invocation, approve: async () => {} }).then(value => { settled = true; return value; });
      await vi.waitFor(() => expect(signal.aborted).toBe(true));
      expect(settled).toBe(false); // retirement still owes the real credential operation
      expect(fixture.http).not.toHaveBeenCalled();
      release("offline-test-key");
      await reloaded;
      const { handle } = await spawned;
      expect(handle.routeDecision).toMatchObject({ ...pin, reasonCode: "jev-error" });
      expect(fixture.http).not.toHaveBeenCalled();
      expect(fixture.runtime.components.status("fabric.provider.jev").state).toBe("active");
    } finally { release?.("offline-test-key"); await Promise.allSettled([spawned, ...(reloaded ? [reloaded] : [])]); }
  });
  it("caller cancellation before routing finishes never starts pinned work or HTTP", async () => {
    const fixture = await setup();
    let release!: (key: string) => void;
    vi.mocked(fixture.context.modelRegistry.getApiKeyForProvider!).mockImplementation(() => new Promise<string>(resolve => { release = resolve; }));
    const controller = new AbortController();
    const dispatch = vi.spyOn(fixture.runtime.agents, "spawn");
    const pending = fixture.runtime.registry.invoke("agents.spawn", { task: "harmless bounded lookup", model: "auto", routeClass: "bounded-lookup", pinModel: pin.model, pinThinking: pin.effort, protected: false, transport: "process" }, { ...fixture.invocation, signal: controller.signal }).then(() => "dispatched", () => "cancelled");
    try {
      await vi.waitFor(() => expect(fixture.credentials).toHaveBeenCalledOnce());
      controller.abort(new Error("test caller cancelled"));
      expect(await pending).toBe("cancelled");
      expect(fixture.credentials.mock.calls[0]![0].aborted).toBe(true);
      expect(dispatch).not.toHaveBeenCalled(); expect(fixture.http).not.toHaveBeenCalled();
    } finally { controller.abort(); release?.("offline-test-key"); await pending; await fixture.runtime.shutdown(); }
  });
  it("authorized network evaluates once but still dispatches the pin", async () => {
    const fixture = await setup();
    const { handle, rows } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...cheap, reasonCode: "shadow-choice" });
    expect(rows[0]).toMatchObject({ ...cheap, reasonCode: "shadow-choice", pin });
    expect(fixture.evaluate).toHaveBeenCalledOnce(); expect(fixture.credentials).toHaveBeenCalledOnce(); expect(fixture.http).toHaveBeenCalledOnce();
    expect(fixture.evaluate.mock.calls[0]?.[0]).not.toHaveProperty("task");
  });
  it("a live config reload from allow to deny is honored before routing", async () => {
    const fixture = await setup();
    fixture.runtime.reloadConfig(fixture.context, normalizeFabricConfig({ ...fixture.runtime.config, approvals: { ...fixture.runtime.config.approvals, network: "deny" } }));
    const { handle } = await fixture.spawn();
    expect(handle.routeDecision.reasonCode).toBe("jev-error");
    expect(fixture.evaluate).not.toHaveBeenCalled(); expect(fixture.credentials).not.toHaveBeenCalled(); expect(fixture.http).not.toHaveBeenCalled();
  });
});

describe("SR-9 internal ask scope cut with held ordinary approvals through the real registry", () => {
  it.each([
    ["queue", "none"], ["dialog", "none"],
    ["queue", "caller"], ["dialog", "caller"],
    ["queue", "deadline"], ["dialog", "deadline"],
    ["queue", "reload"], ["dialog", "reload"],
  ] as const)("held %s / %s: no internal queue, dialog, late network grant or Jev work", async (held, boundary) => {
    const fixture = await setup("ask"); Object.assign(fixture.context, { hasUI: true, mode: "rpc" });
    let releaseHolder!: () => void;
    const holderGate = new Promise<void>(resolve => { releaseHolder = resolve; });
    const select = vi.mocked(fixture.context.ui.select);
    // Only an unrelated ordinary write dialog may exist. Its eventual session
    // grant remains valid; there is no internal network dialog to answer late.
    select.mockImplementation(async () => { await holderGate; return "Allow write access for this session"; });
    const holder = held === "queue"
      ? fixture.runtime.sessionApprovals.serialize(() => holderGate)
      : new ApprovalController({ ...fixture.runtime.config.approvals, write: "ask" }, fixture.context, fixture.runtime.sessionApprovals).approve({
        ref: "test.write", provider: "test", name: "write", description: "Ordinary write", inputSchema: {}, risk: "write",
      });
    if (held === "dialog") await vi.waitFor(() => expect(select).toHaveBeenCalledOnce());
    const serialize = vi.spyOn(fixture.runtime.sessionApprovals, "serialize");
    const controller = new AbortController();
    const dispatch = vi.spyOn(fixture.runtime.agents, "spawn");
    let routeSignal: AbortSignal | undefined;
    let releaseDescriptor!: () => void;
    const descriptorGate = new Promise<void>(resolve => { releaseDescriptor = resolve; });
    const originalDescribe = fixture.runtime.registry.describe.bind(fixture.runtime.registry);
    const describe = vi.spyOn(fixture.runtime.registry, "describe").mockImplementation(async (ref, context) => {
      const action = await originalDescribe(ref, context);
      if (ref === "jev.evaluate" && boundary !== "none") {
        routeSignal = context.signal;
        await descriptorGate;
      }
      return action;
    });
    const spawned = fixture.spawn(controller.signal).then(value => ({ value }), error => ({ error }));
    let reloaded: Promise<unknown> | undefined;
    try {
      if (boundary !== "none") {
        await vi.waitFor(() => expect(routeSignal).toBeDefined());
        if (boundary === "caller") controller.abort(new Error("SR-9 caller cancelled"));
        else if (boundary === "reload") {
          reloaded = fixture.runtime.registry.invoke("components.reload", { id: "fabric.provider.jev" }, { ...fixture.invocation, approve: async () => {} });
          await reloaded;
        }
        await vi.waitFor(() => expect(routeSignal!.aborted).toBe(true), { timeout: 3500 });
      }
      // The route must settle while the unrelated queue/dialog is still held.
      const result = await spawned;
      if ("error" in result) {
        expect(boundary).toBe("caller"); expect(String(result.error)).toContain("SR-9 caller cancelled");
        expect(dispatch).not.toHaveBeenCalled();
      } else {
        expect(boundary).not.toBe("caller");
        expect(result.value.handle.routeDecision).toMatchObject({ ...pin, reasonCode: boundary === "deadline" ? "jev-timeout" : "jev-error" });
      }
      releaseDescriptor();
      // Join the actual delayed descriptor/authorization continuation, not just
      // its cancellation waiter, before checking for late effects.
      await Promise.allSettled(describe.mock.results.filter(result => result.type === "return").map(result => result.value));
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(serialize).not.toHaveBeenCalled();
      expect(select).toHaveBeenCalledTimes(held === "dialog" ? 1 : 0);
      expect(fixture.runtime.sessionApprovals.approvedRisks.has("network")).toBe(false);
      expect(fixture.evaluate).not.toHaveBeenCalled(); expect(fixture.credentials).not.toHaveBeenCalled();
      expect(fixture.context.modelRegistry.getApiKeyForProvider).not.toHaveBeenCalled(); expect(fixture.http).not.toHaveBeenCalled();
      releaseHolder(); await holder;
      // A later ordinary request is not blocked by a phantom internal slot.
      const drained = vi.fn(async () => {});
      await fixture.runtime.sessionApprovals.serialize(drained);
      expect(drained).toHaveBeenCalledOnce();
      expect(select).toHaveBeenCalledTimes(held === "dialog" ? 1 : 0);
      expect(fixture.runtime.sessionApprovals.approvedRisks.has("network")).toBe(false);
      expect(fixture.runtime.sessionApprovals.approvedRisks.has("write")).toBe(held === "dialog");
    } finally {
      releaseDescriptor(); releaseHolder();
      await Promise.allSettled([holder, spawned, ...(reloaded ? [reloaded] : [])]);
    }
  }, 15000);
});

describe("SR-8 internal shadow routing refuses automatic network approval", () => {
  const userTurn = [{ type: "message", message: { role: "user", content: "please run a harmless bounded lookup" } }];
  it.each([false, true])("network auto + Jev approval model + user evidence (hasUI %s) records pinned fallback with zero classifier, credential or HTTP work", async hasUI => {
    const classify = vi.spyOn(FabricAutoApprovalClassifier.prototype, "classify");
    const fixture = await setup("auto", false, undefined, "pi-fabric/typesafe/jev-latest");
    Object.assign(fixture.context, { hasUI });
    Object.assign(fixture.context.sessionManager, { getBranch: () => userTurn });
    expect(fixture.runtime.config.approvals).toMatchObject({ network: "auto", model: "pi-fabric/typesafe/jev-latest" });
    const { handle, rows } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...pin, reasonCode: "jev-error" });
    expect(rows[0]).toMatchObject({ ...pin, shadowChoice: pin, reasonCode: "jev-error" });
    expect(classify).not.toHaveBeenCalled();
    expect(fixture.context.ui.select).not.toHaveBeenCalled();
    expect(fixture.evaluate).not.toHaveBeenCalled(); expect(fixture.credentials).not.toHaveBeenCalled();
    expect(fixture.context.modelRegistry.getApiKeyForProvider).not.toHaveBeenCalled();
    expect(fixture.http).not.toHaveBeenCalled();
  });
  it("the ordinary auto-approval classifier is unchanged for normal tool calls under the same policy", async () => {
    const fixture = await setup("auto", false, undefined, "pi-fabric/typesafe/jev-latest");
    const classifier = { classify: vi.fn(async () => ({ decision: "allow" as const, reason: "test", model: "stub" })) } as unknown as FabricAutoApprovalClassifier;
    const action = await fixture.runtime.registry.describe("jev.evaluate", fixture.invocation);
    await new ApprovalController(fixture.runtime.config.approvals, fixture.context, new FabricSessionApprovals(), classifier).approve(action, {});
    expect(classifier.classify).toHaveBeenCalledOnce();
    await expect(new ApprovalController(fixture.runtime.config.approvals, fixture.context, new FabricSessionApprovals(), classifier, undefined, undefined, true).approve(action, {}))
      .rejects.toThrow(/automatic network approval is unsupported for internal routing/);
    expect(classifier.classify).toHaveBeenCalledOnce();
  });
});

describe("SR-7 Windows command-backed credential scope cut through the real registry", () => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const original = JevCredentials.prototype.resolve;
  async function windowsFixture(envKey: string) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-win32-cmd-")); roots.push(dir);
    const marker = path.join(dir, "spawned");
    const credentialCommand = [process.execPath, "-e", `require("fs").writeFileSync(${JSON.stringify(marker)}, "x"); console.log("offline-command-key")`];
    const fixture = await setup("allow", false, { credentialCommand });
    vi.stubEnv("TYPESAFE_API_KEY", envKey);
    vi.mocked(fixture.context.modelRegistry.getApiKeyForProvider!).mockResolvedValue(undefined);
    // Force win32 only for credential resolution: worker dispatch stays native.
    fixture.credentials.mockImplementation(function (this: JevCredentials, signal: AbortSignal) {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      return original.call(this, signal).finally(() => Object.defineProperty(process, "platform", platform));
    });
    return { fixture, marker };
  }
  afterEach(() => { Object.defineProperty(process, "platform", platform); });
  it("status reports a command-only credential as unsupported on Windows without resolving it", () => {
    const credentials = new JevCredentials(["credential-helper"], {}, undefined);
    Object.defineProperty(process, "platform", { ...platform, value: "linux" });
    expect(credentials.status()).toMatchObject({ configured: true, source: "command" });
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    expect(credentials.status()).toEqual({ configured: false, source: "command-unsupported", verified: false });
  });
  it("command-only credentials record the pinned fallback with zero spawns and zero Jev HTTP", async () => {
    const { fixture, marker } = await windowsFixture("");
    const { handle, rows } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...pin, reasonCode: "jev-error" });
    expect(rows[0]).toMatchObject({ ...pin, reasonCode: "jev-error" });
    expect(fixture.credentials).toHaveBeenCalledOnce();
    await expect(fixture.credentials.mock.results[0]!.value).rejects.toThrow(/unsupported on Windows/);
    expect(fs.existsSync(marker), "the credential command must never start on Windows").toBe(false);
    expect(fixture.http).not.toHaveBeenCalled();
  });
  it("an environment credential is still used on Windows even with a command configured", async () => {
    const { fixture, marker } = await windowsFixture("offline-env-key");
    const { handle } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...cheap, reasonCode: "shadow-choice" });
    expect(fixture.http).toHaveBeenCalledOnce();
    const [, init] = fixture.http.mock.calls[0] as unknown as [string, RequestInit];
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer offline-env-key");
    expect(fs.existsSync(marker)).toBe(false);
  });
  it.skipIf(process.platform === "win32")("control: on POSIX the same command fixture really runs and routes", async () => {
    const { fixture, marker } = await windowsFixture("");
    fixture.credentials.mockImplementation(function (this: JevCredentials, signal: AbortSignal) { return original.call(this, signal); });
    const { handle } = await fixture.spawn();
    expect(handle.routeDecision).toMatchObject({ ...cheap, reasonCode: "shadow-choice" });
    expect(fs.existsSync(marker)).toBe(true);
  });
});

const deliveryId = "01234567-89ab-4cde-8fab-0123456789ab.json";
function expire(fixture: Awaited<ReturnType<typeof setup>>) {
  const now = Date.now(); markRunRootActive(fixture.runRoot, now - 10000); markRunRootClosed(fixture.runRoot, now, true);
  return sweepTempRunRoots({ tempRoot: fixture.cwd, now: now + 20000, oneShotRunRetentionMs: 10000, orphanedTempRunRetentionMs: 10000 });
}
describe("SR-6 real routed worker artifacts are safely retained or collected", () => {
  it.each(["empty", "nonempty"])("collects a closed expired successful run with provenance and %s deliveries", async contents => {
    const fixture = await setup("allow", contents === "nonempty");
    const { run } = await fixture.spawn();
    expect(fs.statSync(path.join(run, "task.txt.provenance.json")).isFile()).toBe(true);
    const deliveries = path.join(run, "deliveries"); expect(fs.statSync(deliveries).isDirectory()).toBe(true);
    // The real worker writes the envelope; the fake RPC peer intentionally does
    // not consume /fabric-delivery, leaving a normal worker-owned JSON artifact.
    const envelopes = fs.readdirSync(deliveries);
    expect(envelopes).toHaveLength(contents === "nonempty" ? 1 : 0);
    if (contents === "nonempty") {
      expect(envelopes[0]).toMatch(/^[0-9a-f-]{36}\.json$/);
      expect(JSON.parse(fs.readFileSync(path.join(deliveries, envelopes[0]!), "utf8"))).toMatchObject({ message: "harmless bounded lookup", provenance: { principal: { id: "test-caller", binding: "voice-call" } } });
    }
    await fixture.runtime.shutdown();
    if (contents === "nonempty") {
      // Main retains unconsumed ingress, even a known worker-owned envelope.
      // Prove that veto before simulating the native hook's consumption.
      expect(expire(fixture)).toEqual({ removedRuns: [], removedRoots: [] });
      expect(fs.existsSync(run)).toBe(true);
      fs.unlinkSync(path.join(deliveries, envelopes[0]!));
    }
    const result = expire(fixture);
    expect(result.removedRuns).toEqual([run]); expect(result.removedRoots).toEqual([fixture.runRoot]);
    expect(fs.existsSync(run)).toBe(false);
  });
  it.each(["pending", "live", "unresolved", "unknown", "unknown-sidecar", "delivery-unknown", "delivery-directory", "delivery-symlink", "delivery-hardlink", "deliveries-symlink", "provenance-symlink", "provenance-hardlink", "provenance-directory"])("keeps the %s veto with known artifacts present", async veto => {
    const fixture = await setup(); const { run } = await fixture.spawn(); await fixture.runtime.shutdown();
    const deliveries = path.join(run, "deliveries"); const provenance = path.join(run, "task.txt.provenance.json");
    const target = path.join(fixture.cwd, "unowned-content"); fs.writeFileSync(target, "must survive");
    if (veto === "pending") fs.writeFileSync(path.join(run, "pending-route-outcome.json"), "{}");
    if (veto === "live") { const file = path.join(run, "status.json"); const record = JSON.parse(fs.readFileSync(file, "utf8")); fs.writeFileSync(file, JSON.stringify({ ...record, transport: "process", sessionId: String(process.pid) })); }
    if (veto === "unresolved") markUnresolvedWorker(run, "worker not joined");
    if (veto === "unknown") fs.writeFileSync(path.join(run, "unknown.txt"), "keep");
    if (veto === "unknown-sidecar") fs.writeFileSync(path.join(run, "task.txt.other.provenance.json"), "{}");
    if (veto === "delivery-unknown") fs.writeFileSync(path.join(deliveries, "unknown.txt"), "keep");
    if (veto === "delivery-directory") fs.mkdirSync(path.join(deliveries, deliveryId));
    if (veto === "delivery-symlink") fs.symlinkSync(target, path.join(deliveries, deliveryId));
    if (veto === "delivery-hardlink") fs.linkSync(target, path.join(deliveries, deliveryId));
    if (veto === "deliveries-symlink") { fs.rmdirSync(deliveries); const directory = path.join(fixture.cwd, "external-directory"); fs.mkdirSync(directory); fs.symlinkSync(directory, deliveries, "junction"); }
    if (veto.startsWith("provenance-")) { fs.unlinkSync(provenance); if (veto === "provenance-directory") fs.mkdirSync(provenance); else if (veto === "provenance-symlink") fs.symlinkSync(target, provenance); else fs.linkSync(target, provenance); }
    expect(expire(fixture)).toEqual({ removedRuns: [], removedRoots: [] });
    expect(fs.existsSync(run)).toBe(true); expect(fs.readFileSync(target, "utf8")).toBe("must survive");
  });
});
