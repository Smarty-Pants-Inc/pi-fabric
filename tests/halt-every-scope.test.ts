import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorManager } from "../src/actors/manager.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import type { JevRunInfo } from "../src/jev/types.js";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe("Escape halt reaches every scope (smarty-dev#7706)", () => {
  it("halts the scopes before and after a failing scope and reports the failure", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-halt-every-"));
    fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn(), on: vi.fn(() => () => {}) } as unknown as ExtensionAPI;
    const context = {
      cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
      sessionManager: { getSessionId: () => "halt-every", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({
      fullCodeMode: true, mcp: { enabled: false, cache: { enabled: false } }, mesh: { enabled: true },
      agents: { enabled: false }, memory: { enabled: false }, residency: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false }, approvals: { agent: "allow", execute: "allow", read: "allow", network: "deny" },
    });
    const fixture = path.join(cwd, "unused.mjs");
    fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), {
      paths: { extension: fixture, worker: fixture, residentHost: fixture, skills: cwd },
    });
    const invocation = {
      cwd, signal: undefined, parentToolCallId: "halt", nestedToolCallId: "halt",
      extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32_768,
    };
    try {
      await runtime.initialize(context, config);
      // Scope 3: a live Jev observer.
      const observer = await runtime.registry.invoke("jev.spawn", {
        input: null, observe: { events: ["turn_end"], delivery: "steer" },
        program: { name: "observer", code: "while(true) await program.nextEvent();", requires: ["jev.advise"], inputSchema: {}, outputSchema: {} },
      }, invocation) as JevRunInfo;
      const observerDone = runtime.registry.invoke("jev.wait", { id: observer.id }, invocation);
      // Scope 2: the directory's secondary actor scope cannot save its cancellation.
      const managers: ActorManager[] = [];
      const haltAll = ActorManager.prototype.haltAll;
      vi.spyOn(ActorManager.prototype, "haltAll").mockImplementation(function (this: ActorManager) {
        managers.push(this);
        if (!(this instanceof ActorDirectory)) throw new Error("Fabric actor halt was not durably acknowledged: session storage unavailable");
        return haltAll.call(this);
      });

      expect(() => runtime.haltAdvisors()).toThrow(/not durably acknowledged: session storage unavailable/);

      // Scope 1 (the directory's primary scope) halted; scope 2 was tried.
      expect(managers).toHaveLength(2);
      expect(managers[0]).toBeInstanceOf(ActorDirectory);
      expect(Object.getOwnPropertyDescriptor(ActorManager.prototype, "halted")!.get!.call(managers[0])).toBe(true);
      // Scope 3 halted after the failure.
      expect(await observerDone).toMatchObject({ state: "cancelled" });
    } finally {
      await runtime.shutdown();
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  }, 20_000);
});
