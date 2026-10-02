import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentHandleInfo } from "../src/agents/types.js";

describe("immutable route pins through the real runtime registry", () => {
  it.each(["startup", "resume"])("R3 refuses an unavailable original pin on %s despite a similar authenticated model", async phase => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-recovery-"));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    let available = [{ provider: "provider", id: "model-a" }];
    const auth = vi.fn(async () => {
      // The original pin disappears between attempts, but an authenticated near-match remains.
      available = [{ provider: "provider", id: "model-a1" }];
      return { ok: true, apiKey: "offline-probe", headers: {} };
    });
    const refresh = vi.fn();
    const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "high", sendMessage: vi.fn(), appendEntry: vi.fn(), on: vi.fn(() => () => {}) } as unknown as ExtensionAPI;
    const context = { cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => available, find: () => available[0], getApiKeyAndHeaders: auth, refresh },
      sessionManager: { getSessionId: () => "route-recovery", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({ fullCodeMode: true, jev: { enabled: false }, mcp: { enabled: false }, mesh: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false }, agents: { enabled: true, retainRuns: true }, approvals: { agent: "allow", execute: "allow", read: "allow", network: "deny" } });
    const fixture = path.join(cwd, "unused.mjs"); fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: fixture, worker: path.resolve(phase === "startup" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs"), residentHost: fixture, skills: cwd } });
    const invocation = { cwd, signal: undefined, parentToolCallId: "route", nestedToolCallId: "route", extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32768 };
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    let runRoot: string | undefined;
    try {
      await runtime.initialize(context, config);
      const handle = await runtime.registry.invoke("agents.spawn", { task: phase === "startup" ? "Recover startup" : "RESUME_AFTER_CRASH", model: "auto", pinModel: "provider/model-a", pinThinking: "high", routeClass: "bounded-lookup", protected: true }, invocation) as AgentHandleInfo;
      runRoot = path.dirname(runtime.agents.runDirectory(handle.id)!);
      const result = await runtime.agents.wait(handle.id);
      expect(launch).toHaveBeenCalledTimes(1);
      expect(auth).toHaveBeenCalledTimes(1);
      expect(refresh).toHaveBeenCalledOnce();
      expect(result.status).toBe("failed");
      expect(result.error).toContain('Role pin "provider/model-a" is not available');
      expect(available).toEqual([{ provider: "provider", id: "model-a1" }]);
      const rows = fs.readFileSync(path.join(cwd, "agent/fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows[1]).toMatchObject({ status: "failed", admittedModel: null, admittedEffort: null });
    } finally {
      await runtime.shutdown();
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      if (runRoot) fs.rmSync(runRoot, { recursive: true, force: true });
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  }, 30000);
});
