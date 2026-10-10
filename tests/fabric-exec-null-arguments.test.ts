import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricActionDescriptor, FabricProvider } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { actionArgNormalizer } from "../src/providers/arg-normalization.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup();
});
const unused = (): never => { throw new Error("Unexpected metadata collaborator call"); };

const execution = (registry: ActionRegistry, cwd: string) => {
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.fullCodeMode = false;
  config.executor.kernel = "typescript";
  config.executor.runtime = "quickjs";
  config.approvals.agent = "allow";
  const service = new FabricExecutionService(registry, config);
  return (code: string) => service.execute({
    code, signal: undefined, parentToolCallId: "null-arguments",
    context: { cwd, hasUI: false } as ExtensionContext, onPartial() {},
  });
};

// Real fabric_exec service -> type checker -> QuickJS guest -> registry ->
// AgentsProvider.prepareArguments -> ActorManager. No inference/worker runs.
const setupActor = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-null-arguments-"));
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:null-arguments", name: "main", kind: "main", sessionId: "null-arguments" };
  const main: FabricMainAgentTarget = { id: identity.id, local: true, matches: () => false, info: unused, deliverAgent: unused };
  const participants: FabricParticipantSource = {
    list: () => [], get: () => undefined, self: unused, peers: () => [],
    async refresh() {}, scheduleRefresh() {},
  };
  const actors = new ActorManager("null-arguments", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, unused, {
    actorRoot: path.join(root, "actors"), mainAgent: main,
  });
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
  const registry = new ActionRegistry();
  registry.register(new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), main, participants, undefined, lifecycle));
  registry.register(new MeshProvider(mesh, identity, participants));
  cleanups.push(async () => {
    try { await registry.close(); } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
  const actor = await actors.create({
    name: "null-controls", instructions: "Idle actor; do not run.",
    coalesceKey: "payload.number", activationFilter: ["hold"],
  });
  return { actor, actors, execute: execution(registry, root) };
};

describe("fabric_exec explicit null arguments (#8309)", () => {
  it("clears a coalesce key through the real guest and reads the cleared actor info", async () => {
    const { actor, actors, execute } = await setupActor();
    const result = await execute(`
const id = ${JSON.stringify(actor.id)};
const before = await agents.actorStatus({ id });
const cleared = await agents.setCoalesceKey({ id, coalesceKey: null });
const after = await agents.actorStatus({ id });
const reset = await agents.setCoalesceKey({ id, coalesceKey: "payload.pull_request.number" });
const resetStatus = await agents.actorStatus({ id });
return {
  before: before.coalesceKey,
  clearedHasKey: Object.prototype.hasOwnProperty.call(cleared, "coalesceKey"),
  afterHasKey: Object.prototype.hasOwnProperty.call(after, "coalesceKey"),
  reset: reset.coalesceKey, resetStatus: resetStatus.coalesceKey,
};`);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual({
      before: "payload.number", clearedHasKey: false, afterHasKey: false,
      reset: "payload.pull_request.number", resetStatus: "payload.pull_request.number",
    });
    expect(actors.status(actor.id).coalesceKey).toBe("payload.pull_request.number");
    expect(result.audits.find((audit) => audit.ref === "agents.setCoalesceKey")?.args).toMatchObject({ coalesceKey: null });
  });

  it("also clears the required nullable activation filter and accepts a new filter", async () => {
    const { actor, execute } = await setupActor();
    const result = await execute(`
const id = ${JSON.stringify(actor.id)};
const cleared = await agents.setActivationFilter({ id, activationFilter: null });
const after = await agents.actorStatus({ id });
const reset = await agents.setActivationFilter({ id, activationFilter: ["hold"] });
return {
  clearedHasFilter: Object.prototype.hasOwnProperty.call(cleared, "activationFilter"),
  afterHasFilter: Object.prototype.hasOwnProperty.call(after, "activationFilter"),
  reset: reset.activationFilter,
};`);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual({ clearedHasFilter: false, afterHasFilter: false, reset: ["hold"] });
    expect(result.audits.find((audit) => audit.ref === "agents.setActivationFilter")?.args).toMatchObject({ activationFilter: null });
  });

  it("rejects a missing coalesce key at validation without changing the actor", async () => {
    const { actor, actors, execute } = await setupActor();
    // tools.call deliberately exercises host validation, not a type-check rejection.
    const result = await execute(`
try { await tools.call({ ref: "agents.setCoalesceKey", args: { id: ${JSON.stringify(actor.id)} } }); }
catch (error) { return error.message; }`);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toMatch(/Invalid arguments for agents.setCoalesceKey:.*coalesceKey/);
    expect(actors.status(actor.id).coalesceKey).toBe("payload.number");
  });

  it("keeps null in open mesh state and event payload schemas", async () => {
    const { execute } = await setupActor();
    const result = await execute(`
const written = await mesh.put({ key: "proof/null-value", value: null });
const read = await mesh.get({ key: "proof/null-value" });
const event = await mesh.publish({ topic: "proof.null-payload", data: null });
return {
  written: written.value, read: read.value,
  eventHasData: Object.prototype.hasOwnProperty.call(event, "data"), data: event.data,
};`);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual({ written: null, read: null, eventHasData: true, data: null });
  });

  it("preserves null in arbitrary action schemas, aliases and nested payloads", async () => {
    const nullableSchemas = [
      { type: ["string", "null"] },
      { anyOf: [{ type: "string" }, { type: "null" }] },
      { oneOf: [{ type: "string" }, { type: "null" }] },
      { enum: ["value", null] },
      { const: null },
      {},
    ];
    const descriptors: FabricActionDescriptor[] = nullableSchemas.map((property, index) => ({
      name: `echo${index}`, description: "Return validated arguments", risk: "read",
      inputSchema: {
        type: "object", additionalProperties: false, required: ["requiredValue", "nested"],
        properties: {
          requiredValue: property, optionalValue: property,
          absentOption: { type: "string" },
          nested: { type: "object", properties: { items: { type: "array", items: property } }, required: ["items"], additionalProperties: false },
        },
      },
    }));
    const normalize = actionArgNormalizer(() => descriptors);
    const registry = new ActionRegistry();
    const provider: FabricProvider = {
      name: "nullable", description: "Nullable argument contract",
      list: async () => descriptors,
      describe: async (name) => descriptors.find((descriptor) => descriptor.name === name),
      prepareArguments: (name, args) => normalize(name, args),
      invoke: async (_name, args) => args,
    };
    registry.register(provider);
    cleanups.push(() => registry.close());
    const result = await execution(registry, process.cwd())(`
const results = [];
for (let i = 0; i < ${nullableSchemas.length}; i++) {
  results.push(await tools.call({ ref: "nullable.echo" + i, args: {
    required_value: null, optional_value: null, absent_option: null, nested: { items: [null] },
  } }));
}
return results;`);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toEqual(nullableSchemas.map(() => ({ requiredValue: null, optionalValue: null, nested: { items: [null] } })));
  });
});
