import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import type { FabricRegistryInvocationContext } from "../src/core/action-registry.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { alive, commandFixture, credentialRequest, credentialResponse, delay } from "./jev-command-test-helpers.js";

describe("SR-7 runtime-state command-backed credential retirement", () => {
  it.skipIf(process.platform === "win32").each(["reload", "shutdown"] as const)("%s does not join a live resolver; reload uses fresh-generation credentials", async boundary => {
    const fixture = commandFixture();
    for (const name of ["TYPESAFE_API_KEY", "JEV_API_KEY", "PI_FABRIC_GRANTED_RISKS"]) vi.stubEnv(name, "");
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(fixture.root, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", fixture.root);
    vi.stubEnv("PI_FABRIC_PI_BINARY", path.resolve("tests/fixtures/fake-pi-route.mjs"));
    const scenario = path.join(fixture.root, "scenario"); fs.writeFileSync(scenario, "success");
    vi.stubEnv("FAKE_MODEL_SCENARIO", scenario);
    const http = vi.fn(async (_url: string | URL | Request, _options?: RequestInit) => new Response(JSON.stringify(credentialResponse)));
    vi.stubGlobal("fetch", http);
    const context = {
      cwd: fixture.root, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { find: (provider: string, id: string) => ({ provider, id }), getAvailable: () => [{ provider: "openai-codex", id: "gpt-5.6-sol" }, { provider: "runinfra", id: "glm-5-3-flash" }], getAll: () => [], getApiKeyAndHeaders: vi.fn(async () => ({ ok: true, apiKey: "offline-model-key", headers: {} })), getApiKeyForProvider: vi.fn(async () => undefined) },
      sessionManager: { getSessionId: () => "sr7-parent", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
      ui: { setStatus: vi.fn(), notify: vi.fn(), select: vi.fn(async () => undefined) },
    } as unknown as ExtensionContext;
    const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn(), on: vi.fn() } as unknown as ExtensionAPI;
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { worker: path.resolve("dist/worker.js"), extension: path.resolve("dist/index.js"), residentHost: path.resolve("dist/resident-host.js"), skills: fixture.root } });
    const config = normalizeFabricConfig({
      fullCodeMode: true, approvals: { agent: "allow", read: "allow", network: "allow" },
      jev: { enabled: true, credentialCommand: fixture.command, requestTimeoutMs: 20000 },
      agents: { enabled: true, extensions: false, timeoutMs: 5000, modelRouting: { shadowCandidates: [{ model: "runinfra/glm-5-3-flash", effort: "medium" }] } }, mcp: { enabled: false, cache: { enabled: false } }, mesh: { enabled: false }, memory: { enabled: false }, records: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
    });
    const invocation: FabricRegistryInvocationContext = { cwd: fixture.root, signal: undefined, parentToolCallId: "sr7-registry", nestedToolCallId: "sr7-evaluate", extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 10000 };
    let pending: Promise<unknown> | undefined;
    let retired: Promise<unknown> | undefined;
    try {
      await runtime.initialize(context, config);
      // Shadow evaluation carries the generation owner's signal separately from caller cancellation.
      pending = runtime.registry.invoke("agents.spawn", { task: "harmless bounded lookup", model: "auto", routeClass: "bounded-lookup", pinModel: "openai-codex/gpt-5.6-sol", pinThinking: "high", protected: false, transport: "process" }, invocation).then(value => value, error => String(error));
      const pid = await fixture.ready();
      let joined = false;
      retired = (boundary === "shutdown" ? runtime.shutdown() : runtime.registry.invoke("components.reload", { id: "fabric.provider.jev" }, { ...invocation, nestedToolCallId: "sr7-reload" })).then(value => { joined = true; return value; });
      await vi.waitFor(() => expect(fs.existsSync(fixture.terminated)).toBe(true), { timeout: 4000, interval: 10 });
      await delay(50);
      expect(alive(pid)).toBe(true);
      expect(joined, "owner retirement must still owe the actual child exit").toBe(false);
      expect(http).not.toHaveBeenCalled();
      await retired;
      expect(alive(pid)).toBe(false); expect(joined).toBe(true);
      const spawned = await pending;
      if (boundary === "reload") {
        expect(spawned).toMatchObject({ routeDecision: { reasonCode: "jev-error" } });
        await runtime.registry.invoke("agents.join", { id: (spawned as { id: string }).id }, { ...invocation, nestedToolCallId: "sr7-agent-join" });
      } else expect(String(spawned)).not.toContain("FAKE_SECRET");
      if (boundary === "reload") {
        expect(runtime.components.status("fabric.provider.jev").state).toBe("active");
        fs.writeFileSync(fixture.fresh, "fresh generation");
        expect(await runtime.registry.invoke("jev.evaluate", credentialRequest, { ...invocation, nestedToolCallId: "sr7-fresh" })).toEqual(credentialResponse);
        expect(http).toHaveBeenCalledOnce();
        expect(http.mock.calls[0]?.[1]).toMatchObject({ headers: { Authorization: "Bearer offline-fresh-key" } });
      }
    } finally {
      await fixture.cleanup();
      await Promise.allSettled([...(pending ? [pending] : []), ...(retired ? [retired] : [])]);
      await runtime.shutdown();
      vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.unstubAllEnvs();
    }
  });
});
