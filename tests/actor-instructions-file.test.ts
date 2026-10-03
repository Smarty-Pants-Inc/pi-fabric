import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { resolveActorInstructions, MAX_ACTOR_INSTRUCTIONS_FILE_BYTES } from "../src/actors/instructions-file.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const digest = (bytes: string | Buffer) => createHash("sha256").update(bytes).digest("hex");
const text = "\ufeffOwn this role — café 🚀.\r\nKeep the final newline.\n";
const fileSource = (instructionsFile: string, bytes = text) => ({ instructionsFile, sha256: digest(bytes) });

const fixture = async (durable = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-instructions-file-"));
  const localRoot = path.join(root, "main-factory");
  const factory = path.join(root, "resident-factory");
  fs.mkdirSync(localRoot); fs.mkdirSync(factory);
  const identity = { id: "session:instructions", name: "Main", kind: "main" as const, sessionId: "instructions" };
  const meshRoot = path.join(root, "mesh");
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, maxEventBytes: 1024 * 1024, actorPollMs: 20 };
  const mesh = new MeshStore(meshRoot, meshConfig.maxEventBytes, 100);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  participants.registerSource(() => [{
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"],
    cwd: root, sessionId: identity.sessionId, startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
  }]);
  await participants.start();
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, instructionsRoot: localRoot }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  const localActorRoot = path.join(root, "main-actors");
  const actors = new ActorManager(identity.sessionId, identity, mesh, meshConfig, agents, () => {}, { actorRoot: localActorRoot, persistent: true });
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "resident-actors"), residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, instructionsRoot: factory }, mesh: meshConfig,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const mainAgent = { id: identity.id, local: true, matches: (id: string) => id === identity.id } as FabricMainAgentTarget;
  const host = durable ? new ResidentHost(config, () => {}) : undefined;
  if (host) await host.start();
  const client = host ? new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 5000, mainAgent }) : undefined;
  const globalActors = new GlobalActorRegistry(root, meshConfig.maxEventBytes);
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
  const provider = new AgentsProvider(agents, actors, globalActors, mainAgent, participants, undefined, lifecycle, () => false, client, false);
  const context: FabricInvocationContext = { cwd: root, signal: undefined, extensionContext: { modelRegistry: { getAvailable: () => [{ provider: "fixture", id: "visible" }] } } as unknown as ExtensionContext,
    parentToolCallId: "instructions-probe", nestedToolCallId: "instructions-probe", update() {} };
  const owner = host?.actors ?? actors;
  const allowed = durable ? factory : localRoot;
  const create = (args: Record<string, unknown>) => provider.invoke("createActor", { name: "role", ...(durable ? { residency: "durable", model: "fixture/visible" } : {}), ...args }, context) as Promise<FabricActorInfo>;
  return { root, allowed, localRoot, factory, owner, provider, create, context, globalActors, client, config, identity, host,
    close: async () => {
      await provider.close(); await lifecycle.close(); await actors.close(); await agents.close();
      await client?.close(); await host?.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true });
    },
  };
};

const rejectionCases = (root: string, allowed: string) => {
  const good = path.join(allowed, "role.md"); fs.writeFileSync(good, text);
  const outside = path.join(root, "outside.md"); fs.writeFileSync(outside, text);
  const sibling = `${allowed}-sibling`; fs.mkdirSync(sibling); fs.writeFileSync(path.join(sibling, "role.md"), text);
  const link = path.join(allowed, "escape.md"); fs.symlinkSync(outside, link);
  const directoryLink = path.join(allowed, "escape-dir"); fs.symlinkSync(sibling, directoryLink, "dir");
  const large = path.join(allowed, "large.md"); fs.writeFileSync(large, "x".repeat(MAX_ACTOR_INSTRUCTIONS_FILE_BYTES + 1));
  const directory = path.join(allowed, "directory"); fs.mkdirSync(directory);
  const invalid = path.join(allowed, "invalid.md"); const bytes = Buffer.from([0xff, 0xfe]); fs.writeFileSync(invalid, bytes);
  return [
    ["outside", fileSource(outside), /outside/],
    ["prefix sibling", fileSource(path.join(sibling, "role.md")), /outside/],
    ["traversal", fileSource(`${allowed}/../${path.basename(allowed)}/role.md`), /traversal/],
    ["symlink escape", fileSource(link), /outside/],
    ["directory symlink escape", fileSource(path.join(directoryLink, "role.md")), /outside/],
    ["oversize", fileSource(large), /512 KB/],
    ["mismatch", { ...fileSource(good), sha256: "0".repeat(64) }, /digest mismatch/],
    ["missing", fileSource(path.join(allowed, "missing.md")), /ENOENT/],
    ["nonregular", fileSource(directory), /regular file/],
    ["invalid UTF-8", { instructionsFile: invalid, sha256: digest(bytes) }, /UTF-8/],
    ["both", { ...fileSource(good), instructions: "inline" }, /not both/],
    ["incomplete pair", { instructionsFile: good }, /requires/],
  ] as const;
};

for (const durable of [false, true]) describe(`${durable ? "resident" : "Main"} verified actor instructions (#3819)`, () => {
  it("applies exact bytes for create and setter, reports digest, and never rereads a stored reference", async () => {
    const state = await fixture(durable);
    try {
      const file = path.join(state.allowed, "role.md"); fs.writeFileSync(file, text);
      const actor = await state.create(fileSource(file));
      expect(actor.instructionsDigest).toBe(digest(text));
      expect(state.owner.instructions(actor.id)).toBe(text);
      fs.unlinkSync(file);
      expect(state.owner.status(actor.id).instructionsDigest).toBe(digest(text));
      const updated = `${text}Updated owner instructions.\n`; fs.writeFileSync(file, updated);
      const result = await state.provider.invoke("setInstructions", { id: actor.id, ...fileSource(file, updated) }, state.context) as FabricActorInfo;
      expect(result.instructionsDigest).toBe(digest(updated));
      expect(state.owner.instructions(actor.id)).toBe(updated);
      const registry = fs.readFileSync(path.join(durable ? state.config.actorRoot : path.join(state.root, "main-actors"), "actors.json"), "utf8");
      expect(registry).not.toContain("instructionsFile"); expect(registry).not.toContain(file);
      // The resident root differs from Main's: Main must forward, not read here.
      if (durable) expect(() => resolveActorInstructions(fileSource(file, updated), state.localRoot)).toThrow(/outside/);
      await state.provider.invoke("setInstructions", { id: actor.id, instructions: "inline still works", replace: true }, state.context);
      expect(state.owner.instructions(actor.id)).toBe("inline still works");
    } finally { await state.close(); }
  });

  it("refuses every bad source with no setter mutation or replacement create", async () => {
    const state = await fixture(durable);
    try {
      const actor = await state.create({ instructions: text });
      const before = state.owner.status(actor.id);
      const registryPath = path.join(durable ? state.config.actorRoot : path.join(state.root, "main-actors"), "actors.json");
      const registry = fs.readFileSync(registryPath, "utf8");
      for (const [label, source, error] of rejectionCases(state.root, state.allowed)) {
        await expect(state.provider.invoke("setInstructions", { id: actor.id, replace: true, ...source }, state.context), label).rejects.toThrow(error);
        await expect(state.create(source), label).rejects.toThrow(error);
        expect(state.owner.status(actor.id), label).toEqual(before);
        expect(state.owner.listOwned(), label).toHaveLength(1);
        expect(fs.readFileSync(registryPath, "utf8"), label).toBe(registry);
      }
    } finally { await state.close(); }
  });

  it("keeps the shrink guard authoritative and accepts replace", async () => {
    const state = await fixture(durable);
    try {
      const actor = await state.create({ instructions: "x".repeat(100) });
      const file = path.join(state.allowed, "tiny.md"); fs.writeFileSync(file, "tiny");
      await expect(state.provider.invoke("setInstructions", { id: actor.id, ...fileSource(file, "tiny") }, state.context)).rejects.toThrow(/replace: true/);
      expect(state.owner.instructions(actor.id)).toBe("x".repeat(100));
      await state.provider.invoke("setInstructions", { id: actor.id, ...fileSource(file, "tiny"), replace: true }, state.context);
      expect(state.owner.status(actor.id).instructionsDigest).toBe(digest("tiny"));
    } finally { await state.close(); }
  });
});

it("uses the default factory/current realpath, allows an in-root symlink and the exact 512 KiB boundary", async () => {
  const state = await fixture();
  const home = vi.spyOn(os, "homedir").mockReturnValue(state.root);
  try {
    const defaultPath = path.join(state.root, ".local/share/smarty-dev/factory/current");
    fs.mkdirSync(path.dirname(defaultPath), { recursive: true }); fs.symlinkSync(state.allowed, defaultPath, "dir");
    const bytes = "x".repeat(MAX_ACTOR_INSTRUCTIONS_FILE_BYTES);
    const file = path.join(state.allowed, "boundary.md"); fs.writeFileSync(file, bytes);
    const link = path.join(defaultPath, "inside.md"); fs.symlinkSync(file, link);
    expect(resolveActorInstructions(fileSource(link, bytes))).toBe(bytes);
    expect(resolveActorInstructions(fileSource("~/.local/share/smarty-dev/factory/current/inside.md", bytes))).toBe(bytes);
    expect(resolveActorInstructions(fileSource(file, bytes), state.allowed)).toBe(bytes);
  } finally { home.mockRestore(); await state.close(); }
});

it("resolves global template sources locally and reports the verified digest without persisting the reference", async () => {
  const state = await fixture();
  try {
    const file = path.join(state.allowed, "template.md"); fs.writeFileSync(file, text);
    const created = await state.create({ scope: "global", ...fileSource(file) });
    expect(created.instructionsDigest).toBe(digest(text));
    expect(state.globalActors.resolve(created.id)?.instructions).toBe(text);
    const updated = `${text}New template.\n`; fs.writeFileSync(file, updated);
    const result = await state.provider.invoke("setInstructions", { id: created.id, scope: "global", ...fileSource(file, updated) }, state.context) as FabricActorInfo;
    expect(result.instructionsDigest).toBe(digest(updated));
    const before = state.globalActors.resolve(created.id);
    await expect(state.provider.invoke("setInstructions", { id: created.id, scope: "global", ...fileSource(file) }, state.context)).rejects.toThrow(/mismatch/);
    expect(state.globalActors.resolve(created.id)).toEqual(before);
    fs.unlinkSync(file);
    expect(state.globalActors.resolve(created.id)?.instructions).toBe(updated);
    expect(state.globalActors.resolve(created.id)).not.toHaveProperty("instructionsFile");
  } finally { await state.close(); }
});
it("routes the file pair unchanged through the resident proxy client", async () => {
  const state = await fixture(true);
  try {
    const file = path.join(state.factory, "proxy.md"); fs.writeFileSync(file, text);
    const proxy = new ResidentActorClient(state.config.meshRoot, state.config.rootId, 5000);
    const actor = await proxy.createActor({ name: "proxy", residency: "durable", ...fileSource(file), model: "fixture/visible" });
    expect(actor.instructionsDigest).toBe(digest(text));
    const after = `${text}Proxy update.\n`; fs.writeFileSync(file, after);
    const result = await proxy.setActor({ operation: "setInstructions", id: actor.id, ...fileSource(file, after) }, undefined,
      { identity: state.identity, hostId: state.identity.id });
    expect(result.instructionsDigest).toBe(digest(after));
  } finally { await state.close(); }
});

it("exercises public guest createActor/create and setter schema admission end to end", async () => {
  const state = await fixture();
  try {
    const file = path.join(state.allowed, "guest.md"); fs.writeFileSync(file, text);
    const registry = new ActionRegistry(); registry.register(state.provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG); config.approvals.agent = "allow";
    const service = new FabricExecutionService(registry, config);
    const source = JSON.stringify(fileSource(file));
    const result = await service.execute({ code: `const a = await agents.createActor({ name: "guest", ...${source} });
      const b = await agents.create({ name: "compat", instructions: "Inline" });
      const updated = await agents.setInstructions({ id: b.id, ...${source} });
      return [a.instructionsDigest, updated.instructionsDigest];`,
      signal: undefined, parentToolCallId: "guest-instructions", context: { cwd: state.root, hasUI: false } as ExtensionContext, onPartial() {} });
    expect(result.success).toBe(true); expect(result.value).toEqual([digest(text), digest(text)]);
    const budgeted = await service.execute({ code: `await agents.createActor({ name: "budget-one", instructions: "Inline" });
      return await agents.createActor({ name: "budget-two", instructions: "Inline" });`,
      maxAgentCalls: 1, signal: undefined, parentToolCallId: "alias-budget", context: { cwd: state.root, hasUI: false } as ExtensionContext, onPartial() {} });
    expect(budgeted.success).toBe(false); expect(budgeted.error).toContain("agent budget exhausted");
    expect(state.owner.listOwned()).toHaveLength(3);
    await state.owner.remove(state.owner.listOwned().find(actor => actor.name === "budget-one")!.id);
    for (const name of ["create", "createActor", "setInstructions"]) {
      const target = name === "setInstructions" ? { id: state.owner.listOwned()[0]!.id } : { name: "bad-guest" };
      const invalid = await service.execute({ code: `return await tools.call({ ref: "agents.${name}", args: ${JSON.stringify({ ...target, ...fileSource(file), instructions: "both" })} });`,
        signal: undefined, parentToolCallId: "bad-source", context: { cwd: state.root, hasUI: false } as ExtensionContext, onPartial() {} });
      expect(invalid.success).toBe(false);
      expect(state.owner.listOwned()).toHaveLength(2);
    }
  } finally { await state.close(); }
});
