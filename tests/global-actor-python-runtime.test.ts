import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import type { FabricActorRequest } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, type FabricPythonRuntime } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const request: FabricActorRequest = {
  name: "python-template", instructions: "Idle Python actor.", runner: "pi", kernel: "python", extensions: true,
};
const setup = (inherited: FabricPythonRuntime = "monty") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-global-python-runtime-"));
  roots.push(root);
  const manager = new AgentManager(root, structuredClone(DEFAULT_FABRIC_CONFIG.agents), {
    runRoot: path.join(root, "runs"), pythonRuntime: () => inherited,
  });
  managers.push(manager);
  return {
    root, manager, registry: new GlobalActorRegistry(root, 64 * 1024),
    file: path.join(root, "fabric", "actors", "global-actors.json"),
  };
};
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Keep reason: data safety at the persisted template/import boundary.
// All registry I/O and runtime policy resolution are real; no actor is activated.
describe("global actor Python runtime preservation", () => {
  it.each(["cpython", "monty"] as const)("preserves explicit %s instead of the opposite default through create, resolve, disk and import request", (pythonRuntime) => {
    const inherited = pythonRuntime === "cpython" ? "monty" : "cpython";
    const { root, manager, registry, file } = setup(inherited);
    expect(manager.resolvePythonRuntime()).toBe(inherited);
    const created = registry.create({ ...request, pythonRuntime });
    expect(created.pythonRuntime).toBe(pythonRuntime);
    expect(registry.resolve(created.id)?.pythonRuntime).toBe(pythonRuntime);
    expect(registry.toRequest(created, "imported-python")).toMatchObject({ name: "imported-python", kernel: "python", pythonRuntime });
    expect(JSON.parse(fs.readFileSync(file, "utf8")).actors[0].pythonRuntime).toBe(pythonRuntime);
    const reloaded = new GlobalActorRegistry(root, 64 * 1024);
    const definition = reloaded.resolve(created.id)!;
    expect(definition.pythonRuntime).toBe(pythonRuntime);
    expect(manager.resolvePythonRuntime(reloaded.toRequest(definition).pythonRuntime)).toBe(pythonRuntime);
  });

  it.each(["cpython", "monty"] as const)("retains %s on omitted update and persists an explicit override", (pythonRuntime) => {
    const { root, registry } = setup();
    const created = registry.create({ ...request, pythonRuntime });
    expect(registry.update(created.id, { instructions: "Still idle." }).pythonRuntime).toBe(pythonRuntime);
    const override = pythonRuntime === "cpython" ? "monty" : "cpython";
    expect(registry.update(created.id, { pythonRuntime: override }).pythonRuntime).toBe(override);
    const reloaded = new GlobalActorRegistry(root, 64 * 1024);
    expect(reloaded.toRequest(reloaded.resolve(created.id)!).pythonRuntime).toBe(override);
    expect(reloaded.update(created.id, { instructions: "Unrelated update." }).pythonRuntime).toBe(override);
  });

  it.each(["cpython", "monty"] as const)("leaves legacy omission unresolved so import inherits %s", (inherited) => {
    const { root, manager, registry } = setup(inherited);
    const created = registry.create(request);
    expect(created).not.toHaveProperty("pythonRuntime");
    expect(registry.update(created.id, { instructions: "Updated legacy template." })).not.toHaveProperty("pythonRuntime");
    const reloaded = new GlobalActorRegistry(root, 64 * 1024);
    const definition = reloaded.resolve(created.id)!;
    expect(definition).not.toHaveProperty("pythonRuntime");
    const imported = reloaded.toRequest(definition);
    expect(imported).not.toHaveProperty("pythonRuntime");
    expect(manager.resolvePythonRuntime(imported.pythonRuntime)).toBe(inherited);
  });

  it.each(["pypy", "", null, false, 1, {}, []].map(value => [value]))("rejects supplied invalid runtime %j on create and update without changing stored bytes", (invalid) => {
    const { registry, file } = setup();
    const created = registry.create({ ...request, pythonRuntime: "cpython" });
    const before = fs.readFileSync(file, "utf8");
    const pythonRuntime = invalid as FabricPythonRuntime;
    expect(() => registry.create({ ...request, name: "invalid-template", pythonRuntime })).toThrow("Invalid inherited Fabric Python runtime");
    expect(() => registry.update(created.id, { pythonRuntime })).toThrow("Invalid inherited Fabric Python runtime");
    expect(fs.readFileSync(file, "utf8")).toBe(before);
    expect(registry.resolve(created.id)?.pythonRuntime).toBe("cpython");
  });

  it.each(["pypy", "", null, false, 1, {}, []].map(value => [value]))("refuses invalid stored runtime %j rather than importing a fallback or overwriting the file", (pythonRuntime) => {
    const { root, registry, file } = setup();
    registry.create(request);
    const stored = JSON.parse(fs.readFileSync(file, "utf8"));
    stored.actors[0].pythonRuntime = pythonRuntime;
    fs.writeFileSync(file, JSON.stringify(stored));
    const before = fs.readFileSync(file, "utf8");
    const reloaded = new GlobalActorRegistry(root, 64 * 1024);
    expect(reloaded.resolve(request.name)).toBeUndefined();
    expect(() => reloaded.create({ ...request, name: "another-template" })).toThrow("Invalid inherited Fabric Python runtime");
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });
});
