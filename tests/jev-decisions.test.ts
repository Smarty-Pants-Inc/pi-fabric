import { afterEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import { JevFabricServe } from "../src/jev-fabric/serve.js";
import type { DurableShellBridge } from "../src/jev-fabric/bridge.js";
import { JevProvider } from "../src/providers/jev-provider.js";
import type { DecisionProfiles, DecisionProvider, DecisionRequest, DecisionResolution, DecisionResult } from "../src/jev/decision-types.js";
import { decisionEnvironment } from "../src/jev/decisions.js";
import { callProgram, jevContext, launch } from "./jev-test-helpers.js";

const presets: DecisionProvider[] = [
  { provider: "typesafe", api: "system-one", defaultModel: "jev-latest", endpoint: "https://api.typesafe.ai/v1/systemone", credentialEnv: "TYPESAFE_API_KEY", imageSupport: "unsupported", generated: false },
  { provider: "openai", api: "openai-decisions", defaultModel: "gpt-6-luna", endpoint: "https://api.openai.com/v1/decisions", credentialEnv: "OPENAI_API_KEY", imageSupport: "supported", generated: false },
  { provider: "openrouter", api: "system-one", defaultModel: "typesafe/jev-1.13", endpoint: "https://openrouter.ai/api/v1/systemone", credentialEnv: "OPENROUTER_API_KEY", imageSupport: "unsupported", generated: false },
  { provider: "vercel", api: "system-one", defaultModel: "typesafe-ai/jev", endpoint: "https://ai-gateway.vercel.sh/typesafe/v1/systemone", credentialEnv: "AI_GATEWAY_API_KEY", imageSupport: "unsupported", generated: false },
  { provider: "cloudflare-workers-ai", api: "cloudflare", defaultModel: "typesafe/jev", endpoint: "https://api.cloudflare.com/client/v4/accounts/abc/ai/run", credentialEnv: "CLOUDFLARE_API_TOKEN", imageSupport: "model-dependent", generated: false },
  { provider: "llama.cpp", api: "llama-cpp", defaultModel: "local", endpoint: "http://127.0.0.1:8080", credentialEnv: "LLAMA_API_KEY", imageSupport: "unsupported", generated: false },
  { provider: "custom", api: "", defaultModel: "", endpoint: "", credentialEnv: "DECISION_API_KEY", imageSupport: "model-dependent", generated: false },
];
const request: DecisionRequest = { state: { log: "synthetic" }, questions: { ok: { type: "boolean", instructions: "Healthy?" } } };
const profiles: DecisionProfiles = { version: 1, defaultProfile: "vision", profiles: { vision: { provider: "openai", model: "gpt-6-luna" }, edge: { provider: "custom", api: "system-one", model: "edge", endpoint: "https://trusted.example/v2/decide" } } };
const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => { for (const close of cleanups.splice(0)) await close(); vi.restoreAllMocks(); vi.unstubAllEnvs(); });

function setup(jev: Record<string, unknown> = {}) {
  const config = normalizeFabricConfig({ approvals: { network: "allow", execute: "allow", read: "allow" }, jev });
  const registry = new ActionRegistry();
  const events: string[] = [];
  const sent: Array<{ op: string; fields: Record<string, unknown> }> = [];
  const connections: JevFabricServe[] = [];
  const open = vi.spyOn(JevFabricServe, "open").mockImplementation(async (_binary, options) => {
    const doc = options.env?.DECISION_PROFILES ? JSON.parse(options.env.DECISION_PROFILES) as DecisionProfiles : undefined;
    let evaluations = 0;
    const serve = {
      banner: { protocol: 2, version: "test", features: ["decisions", "decision-targets"] }, closed: false,
      close: vi.fn(async () => { serve.closed = true; }),
      request: vi.fn(async (op: string, fields: Record<string, unknown>) => {
        if (serve.closed) throw new Error("closed");
        events.push(op); sent.push({ op, fields });
        if (op === "decisionProviders") return structuredClone(presets);
        const r = fields.request as DecisionRequest;
        const profile = r.profile ?? (r.provider ? undefined : options.env?.DECISION_PROFILE ?? doc?.defaultProfile);
        const target = { ...(profile ? doc?.profiles[profile] : {}), ...r };
        const preset = presets.find(p => p.provider === target.provider) ?? presets[0]!;
        const resolution: DecisionResolution = { target: { provider: preset.provider, api: target.api ?? (preset.api || "system-one"), model: target.model ?? preset.defaultModel, endpoint: target.endpoint ?? preset.endpoint, allowLocal: target.allowLocal ?? false, allowGenerated: target.allowGenerated ?? false, ...(target.providerOptions ? { providerOptions: target.providerOptions } : {}) }, credential: { env: preset.credentialEnv, resolved: false }, imageSupport: preset.imageSupport, generated: preset.generated, ...(profile ? { profile } : {}) };
        if (op === "resolveDecision") return { ...resolution, evidenceMustNotLeak: r.state };
        if (++evaluations > options.evaluations!) throw new Error("native shared evaluation budget");
        if (op === "jev") return { model: "jev-latest", answers: { ok: { type: "noul", noul: 0.9 } }, usage: { input_tokens: 2, output_tokens: 1 } };
        if (op !== "decide") throw new Error("unexpected operation");
        return {
          status: r.state === "error" ? "error" : "ok", provider: resolution.target.provider, api: resolution.target.api, model: resolution.target.model,
          answers: { ok: { type: "refusal", reason: "kept verbatim" } }, usage: { input_tokens: r.state === "unknown" ? null : 7, output_tokens: 2 },
          raw: { unrecognized: 9007199254740993 }, rawJson: '{ "unrecognized":9007199254740993, "p":0.123456789012345678901 }' + (r.state === "big" ? " ".repeat(80_000) : ""), rawComplete: true,
          provenance: { kind: "native", custom: "kept" }, budget: { exhausted: r.state === "unknown" || r.state === "exhausted" || evaluations === options.evaluations, reason: r.state === "unknown" ? "unknown-usage" : "" },
          ...(r.state === "error" ? { error: { code: 400, message: "retained" }, httpStatus: 400 } : {}),
        } satisfies DecisionResult;
      }),
    };
    connections.push(serve as unknown as JevFabricServe);
    return serve as unknown as JevFabricServe;
  });
  const resolve = vi.fn(async () => ({ path: "/mock/jev-fabric" }));
  const fetcher = vi.fn(async () => new Response(JSON.stringify({ model: "jev-latest", answers: { ok: { type: "noul", noul: 0.8 } }, usage: { input_tokens: 1, output_tokens: 1 } })));
  const provider = new JevProvider({ registry, config, jevFabric: { home: "/tmp", options: { cwd: "/tmp" }, resolve } as unknown as DurableShellBridge }, new JevClient(config.jev, fetcher as typeof fetch, new JevCredentials([], { TYPESAFE_API_KEY: "legacy-test-key" })));
  registry.register(provider); cleanups.push(() => registry.close());
  const auth = vi.fn(async (id: string) => { events.push(`auth:${id}`); return `synthetic-${id}`; });
  const ctx = jevContext();
  const models = [
    { provider: "openrouter", id: "typesafe/jev-1.13", name: "Jev", api: "typesafe-system-one", baseUrl: "https://openrouter.ai/api/v1", input: ["text", "image"] },
    { provider: "openai", id: "gpt-6-luna", name: "Luna", api: "openai-decisions", baseUrl: "https://api.openai.com/v1", input: ["text", "image"] },
    { provider: "llama.cpp", id: "local", name: "Local", api: "llama-cpp-classify", baseUrl: "http://127.0.0.1:8080/v1", input: ["text"] },
    { provider: "other", id: "custom", name: "Unknown adapter", api: "unknown", baseUrl: "https://other.example", input: ["text"] },
  ];
  ctx.extensionContext.modelRegistry = { getApiKeyForProvider: auth, getProviderAuthStatus: () => ({ configured: true }), getModelsOfType: () => models } as unknown as typeof ctx.extensionContext.modelRegistry;
  const invoke = (action: string, args: unknown = {}) => provider.invoke(action, args as Record<string, unknown>, ctx);
  return { provider, config, registry, ctx, auth, events, sent, connections, open, resolve, fetcher, invoke, models };
}

describe("lossless decision integration (offline)", () => {
  it("preserves error/refusal/rawJson/provenance without legacy validation", async () => {
    const s = setup();
    const result = await s.invoke("decide", { ...request, state: "error", provider: "openai" }) as DecisionResult;
    expect(result).toMatchObject({ status: "error", answers: { ok: { type: "refusal" } }, provenance: { custom: "kept" }, httpStatus: 400 });
    expect(result.rawJson).toContain("9007199254740993");
    expect(s.events.indexOf("resolveDecision")).toBeLessThan(s.events.indexOf("auth:openai"));
    expect(s.auth).toHaveBeenCalledWith("openai");
    expect(s.fetcher).not.toHaveBeenCalled();
  });
  it("shares one session connection across direct target changes and invocation end", async () => {
    const s = setup({ maxEvaluations: 2 });
    await s.invoke("decide", { ...request, provider: "openai" });
    await s.provider.invocationEnded(s.ctx.parentToolCallId);
    s.ctx.parentToolCallId = "different-main-call";
    const last = await s.invoke("decide", { ...request, provider: "openrouter" }) as DecisionResult;
    expect(last.budget.exhausted).toBe(true);
    await expect(s.invoke("decide", request)).rejects.toThrow("budget");
    expect(s.open).toHaveBeenCalledOnce();
    expect(s.resolve).toHaveBeenCalledWith("decisions");
    // Legacy direct remains in-process, independent of the decision session budget.
    await s.invoke("evaluate", { state: "legacy", questions: { ok: { type: "noul", instructions: "Healthy?" } } });
    expect(s.fetcher).toHaveBeenCalledOnce();
  });
  it("resolves profiles/selector offline, snapshots config and rejects a conflicting provider", async () => {
    const s = setup({ decisionProfiles: profiles, decisionProfile: "edge" });
    s.config.jev.decisionProfile = "vision";
    expect(await s.invoke("resolveDecision", { request })).toMatchObject({ profile: "edge", target: { provider: "custom", endpoint: "https://trusted.example/v2/decide" }, credential: { resolved: false } });
    const explicit = await s.invoke("resolveDecision", { request: { ...request, provider: "openai" } });
    expect(explicit).not.toHaveProperty("profile");
    expect(explicit).not.toHaveProperty("evidenceMustNotLeak");
    await expect(s.invoke("decide", { ...request, profile: "vision", provider: "openrouter" })).rejects.toThrow("disagree");
    expect(s.auth).not.toHaveBeenCalled();
    expect(s.open.mock.calls[0]![1].env?.DECISION_PROFILE).toBe("edge");
  });
  it.each(["https://attacker.example/v1/decisions", "https://api.openai.com/evil", "https://api.openai.com/v1/decisions/", "https://api.openai.com/v1/decisions?token=bad"])("rejects full-route override %s before keys", async endpoint => {
    const s = setup();
    await expect(s.invoke("decide", { ...request, provider: "openai", endpoint })).rejects.toThrow("exactly match");
    expect(s.auth).not.toHaveBeenCalled();
    expect(s.sent.some(c => c.op === "decide")).toBe(false);
  });
  it("permits trusted profile and registered routes but not arbitrary custom targets", async () => {
    const s = setup({ decisionProfiles: profiles });
    await s.invoke("decide", { ...request, profile: "edge" });
    expect(s.auth).toHaveBeenLastCalledWith("custom");
    s.models.push({ provider: "private", id: "deployment", name: "Private", api: "openai-decisions", baseUrl: "https://registered.example/v2", input: ["text"] });
    await s.invoke("decide", { ...request, provider: "private", model: "deployment", endpoint: "https://registered.example/v2/decisions" });
    expect(s.auth).toHaveBeenLastCalledWith("private");
    await expect(s.invoke("decide", { ...request, provider: "custom", api: "system-one", model: "bad", endpoint: "https://attacker.example" })).rejects.toThrow("trusted profile");
  });
  it("projects a registered Pi-only profile to custom while retaining its original login owner", async () => {
    const s = setup({ decisionProfiles: { version: 1, profiles: { neural: { provider: "neuralwatt", model: "jev", allowLocal: false, allowGenerated: false }, router: { provider: "openrouter", providerOptions: { allow_fallbacks: false, sort: "price" } } } } });
    s.models.push({ provider: "neuralwatt", id: "jev", name: "Neuralwatt Jev", api: "typesafe-system-one", baseUrl: "https://neuralwatt.example/v1", input: ["text"] });
    const resolved = await s.invoke("resolveDecision", { request: { ...request, profile: "neural" } });
    expect(resolved).toMatchObject({ profile: "neural", target: { provider: "custom", api: "system-one", endpoint: "https://neuralwatt.example/v1/systemone", model: "jev", allowLocal: false, allowGenerated: false } });
    await s.invoke("decide", { ...request, profile: "neural" });
    expect(s.auth.mock.calls.map(c => c[0])).toEqual(["neuralwatt"]);
    for (const call of s.sent.filter(c => c.op === "resolveDecision" || c.op === "decide")) {
      expect(call.fields.request).not.toHaveProperty("profile");
      expect(call.fields.request).toMatchObject({ provider: "custom", allowLocal: false, allowGenerated: false });
    }
    await s.invoke("decide", { ...request, profile: "router", providerOptions: { sort: "latency" } });
    expect((s.sent.filter(c => c.op === "decide").at(-1)!.fields.request as DecisionRequest).providerOptions).toEqual({ sort: "latency" });
  });
  it("never guesses another login owner from a native custom profile's model or endpoint", async () => {
    const endpoint = "https://neuralwatt.example/v1/systemone";
    const s = setup({ decisionProfiles: { version: 1, profiles: { native: { provider: "custom", api: "system-one", model: "jev", endpoint }, pi: { provider: "neuralwatt", api: "system-one", model: "jev", endpoint } } } });
    s.models.push({ provider: "neuralwatt", id: "jev", name: "Neuralwatt Jev", api: "typesafe-system-one", baseUrl: "https://neuralwatt.example/v1", input: ["text"] });
    s.auth.mockResolvedValue(undefined as never); vi.stubEnv("DECISION_API_KEY", undefined);
    await expect(s.invoke("decide", { ...request, profile: "native" })).rejects.toThrow("selected provider");
    expect(s.auth.mock.calls.map(c => c[0])).toEqual(["custom"]);
    s.auth.mockResolvedValue("synthetic-neuralwatt");
    await s.invoke("decide", { ...request, profile: "pi" });
    expect(s.auth).toHaveBeenLastCalledWith("neuralwatt");
  });
  it("discovers catalog/adapter intersection without executing credential resolvers", async () => {
    const s = setup();
    const models = await s.invoke("models") as Array<Record<string, unknown>>;
    expect(models[0]).toMatchObject({ model: { provider: "openrouter" }, supported: true, imageSupport: "unsupported", credentials: { configured: true, verified: false } });
    expect(models[1]).toMatchObject({ imageSupport: "supported" });
    expect(models[2]).toMatchObject({ logits: true, generated: false });
    expect(models[3]).toMatchObject({ supported: false });
    expect(await s.invoke("decisionProviders")).toEqual(presets);
    expect(s.auth).not.toHaveBeenCalled();
    expect(s.sent.every(c => c.op === "decisionProviders" || c.op === "resolveDecision")).toBe(true);
  });
  it.each(["typesafe", "vercel-ai-gateway", "cloudflare-workers-ai"])("uses only selected-provider auth for %s", async provider => {
    const s = setup();
    await s.invoke("decide", { ...request, provider });
    expect(s.auth.mock.calls.map(c => c[0])).toEqual([provider === "typesafe" ? "jev" : provider]);
  });
  it("fails without native features instead of falling back", async () => {
    const s = setup();
    await s.invoke("decisionProviders");
    s.connections[0]!.banner.features = ["decisions"];
    await expect(s.invoke("decide", request)).rejects.toThrow("decision-targets");
    expect(s.auth).not.toHaveBeenCalled(); expect(s.fetcher).not.toHaveBeenCalled();
  });
  it("retains unknown usage and closes the shared inference budget", async () => {
    const s = setup();
    const result = await s.invoke("decide", { ...request, state: "unknown" }) as DecisionResult;
    expect(result.usage.input_tokens).toBeNull();
    expect(result.rawJson).toBeTruthy();
    await expect(s.invoke("decide", { ...request, provider: "openai" })).rejects.toThrow("budget");
    expect(s.open).toHaveBeenCalledOnce();
  });
  it("shares manager and native budgets between legacy and lossless calls", async () => {
    const s = setup({ maxEvaluations: 2 });
    const run = await callProgram(s.provider, "run", launch(`
      await jev.evaluate({state:"legacy",questions:{ok:{type:"noul",instructions:"Healthy?"}}});
      const result = await jev.decide(input);
      return {raw:result.rawJson, exhausted:result.budget.exhausted};
    `, { requires: ["jev.evaluate", "jev.decide"], limits: { maxEvaluations: 2 } }, request as never), s.ctx);
    expect(run).toMatchObject({ state: "completed", evaluations: 2, usage: { input_tokens: 9, output_tokens: 3 }, result: { exhausted: true } });
    expect(s.sent.filter(c => ["jev", "decide"].includes(c.op)).map(c => c.op)).toEqual(["jev", "decide"]);
    expect(s.fetcher).not.toHaveBeenCalled(); expect(s.open).toHaveBeenCalledOnce();
  });
  it("returns exhausted evidence inside a program, then blocks either inference API", async () => {
    const s = setup();
    const run = await callProgram(s.provider, "run", launch(`
      const result = await jev.decide(input);
      return {raw:result.rawJson, usage:result.usage};
    `, { requires: ["jev.decide"] }, { ...request, state: "unknown" } as never), s.ctx);
    expect(run).toMatchObject({ state: "completed", usage: { input_tokens: null, output_tokens: 2 }, result: { usage: { input_tokens: null } } });
    const next = await callProgram(s.provider, "run", launch(`
      await jev.decide(input);
      await jev.evaluate({state:"legacy",questions:{ok:{type:"noul",instructions:"Healthy?"}}}); return null;
    `, { requires: ["jev.decide", "jev.evaluate"] }, { ...request, state: "unknown" } as never), s.ctx);
    expect(next).toMatchObject({ state: "failed", evaluations: 1, error: expect.stringContaining("budget") });
  });
  it("allows full evidence inspection in the guest but preserves the explicit 32 KiB output cap", async () => {
    const s = setup();
    const inspect = await callProgram(s.provider, "run", launch('return (await jev.decide(input)).rawJson!.length;', { requires: ["jev.decide"] }, { ...request, state: "big" } as never), s.ctx);
    expect(inspect.state).toBe("completed"); expect(inspect.result).toBeGreaterThan(80_000);
    const output = await callProgram(s.provider, "run", launch('return await jev.decide(input);', { requires: ["jev.decide"] }, { ...request, state: "big" } as never), s.ctx);
    expect(output).toMatchObject({ state: "failed", error: expect.stringContaining("Program output exceeds 32768") });
  });
  it("blocks maxEvaluations:0 before opening a connection or resolving keys", async () => {
    const s = setup();
    const run = await callProgram(s.provider, "run", launch('return await jev.decide(input);', { requires: ["jev.decide"], limits: { maxEvaluations: 0 } }, request as never), s.ctx);
    expect(run).toMatchObject({ state: "failed", evaluations: 0 }); expect(s.open).not.toHaveBeenCalled(); expect(s.auth).not.toHaveBeenCalled();
  });
  it("updates the selector without resetting direct budgets or retargeting a running program", async () => {
    const s = setup({ decisionProfiles: profiles, maxEvaluations: 2 });
    const spawned = await callProgram(s.provider, "spawn", launch('await program.sleep(100); return (await jev.resolveDecision({request:input})).profile;', { requires: ["jev.resolveDecision"] }, request as never), s.ctx);
    s.provider.setDecisionProfile("edge");
    expect(await s.invoke("resolveDecision", { request })).toMatchObject({ profile: "edge" });
    expect(await s.provider.manager.wait(spawned.id)).toMatchObject({ state: "completed", result: "vision" });
    await s.invoke("decide", request);
    s.provider.setDecisionProfile(null);
    expect(await s.invoke("resolveDecision", { request })).toMatchObject({ profile: "vision" });
    const last = await s.invoke("decide", request) as DecisionResult;
    expect(last.budget.exhausted).toBe(true);
    s.provider.setDecisionProfile("edge");
    await expect(s.invoke("decide", request)).rejects.toThrow("budget");
    expect(s.open).toHaveBeenCalledTimes(2); // one program, one enduring direct session
    expect(() => s.provider.setDecisionProfile("")).toThrow("known decision profile");
  });
  it("never falls back to another provider's key or the legacy credential command", async () => {
    const s = setup({ credentialCommand: ["/must-not-run-for-openai"] });
    vi.stubEnv("OPENAI_API_KEY", undefined);
    s.auth.mockResolvedValue(undefined as never);
    await expect(s.invoke("decide", { ...request, provider: "openai" })).rejects.toThrow("selected provider");
    expect(s.auth.mock.calls.map(c => c[0])).toEqual(["openai"]);
    expect(s.sent.some(c => c.op === "decide")).toBe(false);
  });
  it("honors per-call auth refresh and retains request image/options fields", async () => {
    const s = setup();
    const images = [{ type: "image", data: "aGk=", mimeType: "image/png" }];
    s.auth.mockResolvedValueOnce("first-key").mockResolvedValueOnce("second-key");
    await s.invoke("decide", { ...request, provider: "openai", images, providerOptions: { safety_identifier: "test" } });
    await s.invoke("decide", { ...request, provider: "openai" });
    const calls = s.sent.filter(c => c.op === "decide");
    expect(calls.map(c => c.fields.credential)).toEqual(["first-key", "second-key"]);
    expect(calls[0]!.fields.request).toMatchObject({ images, providerOptions: { safety_identifier: "test" } });
  });
  it("exposes read-only discovery through guest capability grants without inference", async () => {
    const s = setup();
    const run = await callProgram(s.provider, "run", launch('return {models:(await jev.models()).length, providers:(await jev.decisionProviders()).length, profile:(await jev.resolveDecision({request:input})).profile ?? null};', { requires: ["jev.models", "jev.decisionProviders", "jev.resolveDecision"], limits: { maxEvaluations: 0 } }, request as never), s.ctx);
    expect(run).toMatchObject({ state: "completed", evaluations: 0, result: { models: 4, providers: presets.length, profile: null } });
    expect(s.auth).not.toHaveBeenCalled();
  });
  it("removes ambient target/key/command inheritance and passes only trusted profiles", () => {
    const config = normalizeFabricConfig({ jev: { decisionProfiles: profiles, decisionProfile: "vision" } }).jev;
    expect(decisionEnvironment(config, { DECISION_PROVIDER: "bad", DECISION_ENDPOINT: "bad", DECISION_MODEL: "bad", DECISION_CREDENTIAL_COMMAND: "bad", DECISION_PROFILES_FILE: "/untrusted/profiles", OPENAI_API_KEY: "secret", DECISION_PROFILE: "bad", DECISION_PROFILES: "bad" })).toEqual({ DECISION_PROFILES: JSON.stringify(profiles), DECISION_PROFILE: "vision" });
  });
});
