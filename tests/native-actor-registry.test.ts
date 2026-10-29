// Keep: trust boundary + durable-data safety. Actual registry load/save/reconstruct and
// directory routing; older-binary/mixed-writer execution remains a parent acceptance gate.
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import type { FabricActorRequest, GlobalActorDefinition } from "../src/actors/types.js";
import { createNativeActorFixture, nativePosix, type NativeActorFixture } from "./helpers/native-actor-ingress.js";

const fixtures: NativeActorFixture[] = [];
const fixture = async (...args: Parameters<typeof createNativeActorFixture>) => {
  const f = await createNativeActorFixture(...args); fixtures.push(f); return f;
};
afterEach(async () => { vi.restoreAllMocks(); await Promise.all(fixtures.splice(0).map(f => f.close())); });
const ordinary: FabricActorRequest = { name: "ordinary", instructions: "Keep normal model behavior", runner: "pi",
  model: "cliproxyapi/gpt-6.1-sol", events: ["turn_end"], topics: ["ordinary.topic"] };

describe.skipIf(!nativePosix)("native registry and marker quarantine", () => {
  it("reconstructs the same exact ID/marker and leaves explicit stopped rows stopped", async () => {
    const f = await fixture();
    await f.actors.stop(f.binding.actorId);
    await f.actors.close();
    const spies = f.modelSpies(), next = f.openManager();
    const result = await next.ensureNativeActor(f.binding);
    expect(result.id).toBe(f.binding.actorId); expect(result.runner).toBe("native"); expect(result.status).toBe("stopped");
    expect(next.definition.bind(next, result.id)).toThrow(/Native/);
    expect(next.instructions.bind(next, result.id)).toThrow(/Native/);
    expect(f.rows()[0]?.instructions).toBeNull();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it("recognizes declarations without callbacks but cannot materialize or execute in observational Main", async () => {
    const f = await fixture(); await f.actors.close();
    const spies = f.modelSpies();
    const next = f.openManager({ nativeHandlers: [], claimResidency: "session" },
      { id: f.binding.rootId, name: "actual-main-observer", kind: "main", sessionId: f.binding.rootId.slice(8) });
    expect(next.isNativeActor(f.binding.actorId)).toBe(true);
    expect(next.status(f.binding.actorId).runner).toBe("native");
    expect(next.owns(f.binding.actorId)).toBe(false);
    await expect(next.ensureNativeActor(f.binding)).rejects.toThrow(/Native/);
    expect(() => next.ask(f.binding.actorId, "rpc", {})).toThrow(/owned by another host/);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it.each([
    ["unknown kind", { execution: { kind: "future", version: 1 } }],
    ["unknown version", { execution: { kind: "native", version: 2, handlerKey: "rpc" } }],
    ["null marker", { execution: null }],
    ["wrong handler", { execution: { kind: "native", version: 1, handlerKey: "different" } }],
    ["wrong root", { rootId: "session:99999999-9999-4999-8999-999999999999" }],
    ["wrong scope", { scope: "project" }],
    ["subscriptions", { topics: ["forged.topic"], events: ["turn_end"] }],
    ["model policy", { model: "forged/model" }],
    ["model-shaped sentinel", { instructions: "Convert to Pi", runner: "pi" }],
  ])("quarantines %s before model defaults and preserves its opaque row during unrelated saves", async (_label, patch) => {
    const f = await fixture(); await f.actors.close();
    const raw = { ...f.rows()[0], ...patch, futureOpaque: { keep: ["EXACT", null, { x: 2 }] } };
    f.writeRows([raw]);
    const next = f.openManager();
    expect(next.isNativeActor(f.binding.actorId)).toBe(true); expect(next.list()).toEqual([]);
    await expect(next.ensureNativeActor(f.binding)).rejects.toThrow(/Native/);
    vi.spyOn(f.agents, "prepareModelWithReason").mockResolvedValue({ fallback: false });
    const normal = await next.create(ordinary);
    expect(normal.runner).toBe("pi");
    expect(f.rows().find(row => row.id === f.binding.actorId)).toEqual(raw);
  });

  it("reserves a configured ID even after both native markers have been removed", async () => {
    const f = await fixture(); await f.actors.close();
    const { execution: _execution, ...before } = f.rows()[0]!;
    const downgraded = { ...before, runner: "pi", instructions: "Must never become a model" };
    f.writeRows([downgraded]);
    const spies = f.modelSpies(), next = f.openManager();
    expect(next.list()).toEqual([]); expect(next.isNativeActor(f.binding.actorId)).toBe(true);
    await expect(next.ensureNativeActor(f.binding)).rejects.toThrow(/Native/);
    expect(() => next.ask(f.binding.actorId, "rpc", {})).toThrow();
    expect(f.rows()).toEqual([downgraded]);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it.each([false, true])("prepasses duplicate model/native IDs regardless of row order (model first=%s)", async modelFirst => {
    const f = await fixture(); await f.actors.close();
    const native = f.rows()[0]!;
    const model: Record<string, unknown> = { ...native, runner: "pi", instructions: "No downgrade" };
    delete model.execution;
    const rows = modelFirst ? [model, native] : [native, model];
    f.writeRows(rows);
    const next = f.openManager();
    expect(next.list()).toEqual([]); expect(next.isNativeActor(f.binding.actorId)).toBe(true);
    await expect(next.ensureNativeActor(f.binding)).rejects.toThrow(/Native/);
    vi.spyOn(f.agents, "prepareModelWithReason").mockResolvedValue({ fallback: false });
    await next.create(ordinary);
    expect(f.rows().filter(row => row.id === f.binding.actorId)).toEqual(rows);
  });

  it("preserves malformed unknown foreign marker/history fields without hydrating or compacting them", async () => {
    const f = await fixture(); await f.actors.close();
    const raw = { id: "e".repeat(32), runner: "pi", instructions: "opaque foreign", execution: { kind: "future", v: 7 },
      rootId: "session:99999999-9999-4999-8999-999999999999", name: "foreign", createdAt: 1,
      instructionsFile: "not-a-reference", messageHistory: { future: true }, messages: [{ no: "known envelope" }],
      unknownField: { nested: [null, false, "RETAIN"] } };
    f.writeRows([...f.rows(), raw]);
    const next = f.openManager();
    expect(next.isNativeActor(raw.id)).toBe(true);
    vi.spyOn(f.agents, "prepareModelWithReason").mockResolvedValue({ fallback: false });
    await next.create(ordinary);
    expect(f.rows().find(row => row.id === raw.id)).toEqual(raw);
    expect(fs.existsSync(path.join(f.actorRoot, raw.id, "registry"))).toBe(false);
  });

  it("routes ensure only to the session manager even when the directory defaults to project", async () => {
    const f = await fixture({ ensure: false }); await f.actors.close();
    const project = path.join(f.root, "project-actors"), session = path.join(f.root, "session-actors");
    const directory = new ActorDirectory([f.binding.rootId.slice(8), f.identity, f.mesh, f.meshConfig, f.agents, () => {}, f.managerOptions],
      { project, session }, "project");
    f.managers.push(directory);
    await directory.ensureNativeActor(f.binding);
    expect(directory.status(f.binding.actorId).scope).toBe("session");
    expect(directory.isNativeActor(f.binding.actorId)).toBe(true);
    expect(fs.existsSync(path.join(project, "actors.json"))).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(session, "actors.json"), "utf8")).actors[0].runner).toBe("native");
    await expect(directory.ensureNativeActor({ ...f.binding, scope: "project" } as never)).rejects.toThrow(/Native/);
  });

  it("refuses another registry's same ID before native birth, without adopting or rewriting it", async () => {
    const f = await fixture({ ensure: false }); await f.actors.close();
    const project = path.join(f.root, "occupied-project"), session = path.join(f.root, "empty-session");
    fs.mkdirSync(project, { recursive: true });
    const foreign = { id: f.binding.actorId, name: f.binding.name, rootId: "session:99999999-9999-4999-8999-999999999999",
      runner: "pi", instructions: "Never adopt", createdAt: 1, future: { retain: true } };
    fs.writeFileSync(path.join(project, "actors.json"), JSON.stringify({ format: 1, actors: [foreign] }));
    const directory = new ActorDirectory([f.binding.rootId.slice(8), f.identity, f.mesh, f.meshConfig, f.agents, () => {}, f.managerOptions],
      { project, session }, "project");
    f.managers.push(directory);
    await expect(directory.ensureNativeActor(f.binding)).rejects.toThrow(/Native/);
    expect(JSON.parse(fs.readFileSync(path.join(project, "actors.json"), "utf8")).actors).toEqual([foreign]);
    expect(fs.existsSync(path.join(session, "actors.json"))).toBe(false);
  });

  it("rejects public create/template/import/export marker stripping before model preparation", async () => {
    const f = await fixture(), spies = f.modelSpies();
    const marked = Object.defineProperty({ ...ordinary }, "execution", { value: undefined });
    await expect(f.actors.create(marked)).rejects.toThrow(/Native/);
    const registry = new GlobalActorRegistry(path.join(f.root, "global"), f.mesh.maxEventBytes);
    expect(() => registry.create({ ...ordinary, runner: "native" } as never)).toThrow(/execution markers/);
    expect(() => registry.toRequest({ ...f.rows()[0], instructions: "strip me" } as unknown as GlobalActorDefinition)).toThrow(/execution markers/);
    expect(() => f.actors.definition(f.binding.actorId)).toThrow(/Native/);
    await expect(new ActorRegistryStore(f.actorRoot).restoreInlineForDowngrade()).rejects.toThrow(/Native/);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });
});
