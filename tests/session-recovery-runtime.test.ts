// Keep reason: data safety. Real Fabric runtime composition with a broker/B2 fixture;
// this does not prove native exclusion, interruption replay, or cross-host qualification.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import type { SessionRecoveryActivation } from "../src/recovery/l7-interfaces.js";
import * as transportModule from "../src/transport/index.js";
import { kvWorld } from "./helpers/bridge-nats-kv.js";

const roots: string[] = [], runtimes: FabricRuntimeState[] = [];
afterEach(async () => {
  await Promise.all(runtimes.splice(0).map(runtime => runtime.shutdown("exit")));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-runtime-")); roots.push(root);
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("SMARTY_AGENT", undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "physical-agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root); vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "runs"));
  const sessionId = "retained-runtime", sessionFile = path.join(root, "session.jsonl"), stateDirectory = path.join(root, "retained");
  fs.mkdirSync(stateDirectory);
  fs.writeFileSync(sessionFile, JSON.stringify({ type: "session", version: 3, id: sessionId, cwd: root, timestamp: new Date().toISOString() }) + "\n");
  const model = { provider: "fixture", id: "recovery", name: "recovery fixture", api: "openai-completions",
    contextWindow: 100_000, maxTokens: 1_000, reasoning: false, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
  const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
    getSessionName: () => sessionId, appendEntry() {}, sendMessage() {} } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => false, hasPendingMessages: () => false, model,
    modelRegistry: { getAvailable: () => [model], getAll: () => [model], find: () => model,
      getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "fixture" }) },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => sessionFile,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} } } as unknown as ExtensionContext;
  const verifyPredecessor = vi.fn<SessionRecoveryActivation["identity"]["verifyPredecessor"]>(async () => {});
  const activation: SessionRecoveryActivation = {
    binding: { sessionFile, stateDirectory, transportMachine: "machine-a", evidence: {
      sessionId, predecessorReceipt: { bytes: "{}", sha256: "0".repeat(64) },
      predecessorPodId: "old-pod", predecessorBootId: "old-boot", successorPodId: "new-pod", successorBootId: "new-boot",
      exclusionProof: { kind: "b1-fence", observedAt: Date.now(), detail: "fixture only" },
    } }, identity: { validate: async () => { throw new Error("B2 was already validated by the startup owner"); }, verifyPredecessor },
  };
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { sessionRecovery: activation }); runtimes.push(runtime);
  const world = kvWorld();
  const opened = vi.spyOn(transportModule, "openFabricTransport").mockResolvedValue(world.transport("machine-a", `session:${sessionId}`));
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: path.join(root, "mesh"), stateBackend: "file",
      nats: { machine: "machine-a", operatorMachine: "fabric-ops", servers: ["tls://nats.example.test:4222"],
        custodyStream: "CUSTODY", livenessStream: "LIVENESS", pathBMachines: ["machine-a"], tls: { ca: "fixture-only" } } },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } });
  return { root, context, activation, runtime, world, opened, config, verifyPredecessor, stateDirectory };
}

describe("portable session runtime binding", () => {
  it("activates the exact retained state root and B2 proof before reporting live ownership", async () => {
    const f = fixture();
    expect(() => f.runtime.assertSessionRecoveryActive()).toThrow("lost its live owner lease");
    await f.runtime.initialize(f.context, f.config);
    f.runtime.assertSessionRecoveryActive();
    expect(f.verifyPredecessor).toHaveBeenCalledOnce();
    expect(f.verifyPredecessor.mock.calls[0]?.[0]).toBe(f.activation.binding);
    expect(fs.readdirSync(f.stateDirectory).some(name => /^transport-host-lease-.*\.json$/.test(name))).toBe(true);
    const physicalRoot = path.join(f.root, "physical-agent", "fabric");
    const physicalFiles = fs.existsSync(physicalRoot) ? fs.readdirSync(physicalRoot, { recursive: true }) : [];
    expect(physicalFiles.filter(name => /^transport-host-lease-.*\.json$/.test(path.basename(String(name))))).toEqual([]);
  });

  it("rejects a different session file before opening transport or validating a predecessor", async () => {
    const f = fixture();
    f.runtime.setSessionRecovery({ ...f.activation, binding: { ...f.activation.binding, sessionFile: path.join(f.root, "another.jsonl") } });
    await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow("exact retained session");
    expect(f.opened).not.toHaveBeenCalled(); expect(f.verifyPredecessor).not.toHaveBeenCalled();
  });

  it("rejects the successor's physical-machine identity instead of adopting it", async () => {
    const f = fixture();
    f.opened.mockResolvedValue(f.world.transport("machine-b", `session:${f.context.sessionManager.getSessionId()}`));
    await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow("changed the retained transport machine");
    expect(f.verifyPredecessor).not.toHaveBeenCalled();
    expect(f.world.writes).toEqual([]);
  });
});
