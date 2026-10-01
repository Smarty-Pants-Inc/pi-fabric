import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import type { FabricActorLog } from "../src/actors/types.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry, type FabricRegistryInvocationContext } from "../src/core/action-registry.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const options = { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 };
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0)) await cleanup();
});
const unused = (): never => { throw new Error("Unexpected metadata collaborator call"); };

// Actual guest -> registry/schema -> provider -> ActorManager -> shared FD reader.
// Main/participant collaborators are idle metadata ports; no worker/model is launched.
const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-quickjs-log-"));
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:guest-cursor", name: "main", kind: "main", sessionId: "guest-cursor" };
  const main: FabricMainAgentTarget = { id: identity.id, local: true, matches: () => false, info: unused, deliverAgent: unused };
  const participants: FabricParticipantSource = {
    list: () => [], get: () => undefined, self: unused, peers: () => [],
    async refresh() {}, scheduleRefresh() {},
  };
  const actors = new ActorManager("guest-cursor", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, unused, {
    actorRoot: path.join(root, "actors"), mainAgent: main,
  });
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
  const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), main, participants, undefined, lifecycle);
  const registry = new ActionRegistry();
  registry.register(provider);
  cleanups.push(async () => {
    try { await registry.close(); } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
  const actor = await actors.create({ name: "cursor-reader", instructions: "Idle log fixture; do not run." });
  const file = actor.sessionFile!;
  const context: FabricRegistryInvocationContext = {
    cwd: root, signal: undefined, parentToolCallId: "guest-cursor", nestedToolCallId: "guest-cursor-nested",
    extensionContext: {} as ExtensionContext, update() {}, approve: async () => {}, audits: [], maxResultChars: 100_000,
  };
  const runtime = new QuickJsRuntime();
  const execute = (code: string) => runtime.execute(code, (ref, args, signal) => registry.invoke(ref, args, { ...context, signal }), options);
  return { file, actor, execute };
};
const records = (owner: string, count = 6) => Array.from({ length: count }, (_, index) => JSON.stringify({ owner, index })).join("\n") + "\n";
const catchLog = (args: Record<string, unknown>) => `
try {
  const page = await agents.log(${JSON.stringify(args)});
  return { unexpectedPage: page };
} catch (error) {
  return { name: error.name, message: error.message, isError: error instanceof Error };
}`;

describe("QuickJS public generation-bound agents.log", () => {
  it("keeps cursor-stale on an unbound cursor in the actual guest", async () => {
    const { file, actor, execute } = await setup();
    fs.writeFileSync(file, records("original"));
    const result = await execute(`
const page = await agents.log({ id: ${JSON.stringify(actor.id)}, type: "session", lines: 2 });
try {
  return await agents.log({ id: ${JSON.stringify(actor.id)}, type: "session", lines: 2, before: page.sessionBefore });
} catch (error) {
  return { name: error.name, message: error.message, isError: error instanceof Error };
}`);
    expect(result.error).toBeUndefined();
    expect(result.value).toEqual({ name: "cursor-stale", message: expect.stringContaining("Unbound log cursor"), isError: true });
  });

  it("allows same-inode append, rejects atomic replacement without reading bytes, and restarts with the correct generation", async () => {
    const { file, actor, execute } = await setup();
    fs.writeFileSync(file, records("original"));
    const first = await execute(`return agents.log({ id: ${JSON.stringify(actor.id)}, type: "session", lines: 2 });`);
    expect(first.error).toBeUndefined();
    const page = first.value as FabricActorLog;
    expect(page.sessionBefore).toEqual(expect.any(Number));
    expect(page.sessionGeneration).toEqual(expect.any(String));
    const bound = { id: actor.id, type: "session", lines: 2, before: page.sessionBefore, beforeGeneration: page.sessionGeneration };
    fs.appendFileSync(file, `${JSON.stringify({ owner: "original", index: 6 })}\n`);
    const appended = await execute(`return agents.log(${JSON.stringify(bound)});`);
    expect(appended.error).toBeUndefined();
    expect((appended.value as FabricActorLog).sessionGeneration).toBe(page.sessionGeneration);
    expect((appended.value as FabricActorLog).session.map((line) => line.parsed)).toEqual([{ owner: "original", index: 2 }, { owner: "original", index: 3 }]);

    fs.writeFileSync(`${file}.owned-replacement`, records("replacement", 10));
    fs.renameSync(`${file}.owned-replacement`, file);
    const read = vi.spyOn(fs, "readSync");
    try {
      const stale = await execute(catchLog(bound));
      expect(stale.error).toBeUndefined();
      expect(stale.value).toEqual({ name: "cursor-stale", message: expect.stringContaining("Log generation changed"), isError: true });
      expect(read).not.toHaveBeenCalled();
    } finally { read.mockRestore(); }
    const fresh = await execute(`return agents.log({ id: ${JSON.stringify(actor.id)}, type: "session", lines: 2 });`);
    expect(fresh.error).toBeUndefined();
    expect((fresh.value as FabricActorLog).sessionGeneration).not.toBe(page.sessionGeneration);
    expect((fresh.value as FabricActorLog).session.map((line) => line.parsed)).toEqual([{ owner: "replacement", index: 8 }, { owner: "replacement", index: 9 }]);
    const ordinary = await execute(catchLog({ id: "missing-owned-fixture", type: "session", lines: 2 }));
    expect(ordinary.error).toBeUndefined();
    expect(ordinary.value).toEqual({ name: "Error", message: expect.stringContaining("Unknown Fabric agent"), isError: true });
  });

  it.each([new RangeError("bounded"), Object.assign(new Error("wait bounded"), { name: "AgentWaitBoundError" })])("preserves other host Error names without copying arbitrary properties: %s", async (error) => {
    Object.assign(error, { secret: "not-guest-data" });
    const result = await new QuickJsRuntime().execute(`
try { await tools.call({ ref: "owned.error", args: {} }); }
catch (error) { return { name: error.name, message: error.message, isError: error instanceof Error, extra: typeof error.secret }; }
`, async () => { throw error; }, options);
    expect(result.error).toBeUndefined();
    expect(result.value).toEqual({ name: error.name, message: error.message, isError: true, extra: "undefined" });
  });
});
