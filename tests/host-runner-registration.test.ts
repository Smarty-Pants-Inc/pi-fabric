import vm from "node:vm";
import { createSyntheticSourceInfo, type RegisteredTool, type ToolDefinition } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { Type } from "typebox";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { HOST_EXTENSION_RUNNER_SYMBOL, installRegisteredToolCapture } from "../src/capture/interceptor.js";
import { HOST_EXTENSION_RUNNER_KEY, registerHostExtensionRunner } from "../src/worker/host-runner-registration.js";

// smarty-dev#8606 (review-fleet-2 P1): the native task entry registers its SDK's
// ExtensionRunner once, before task code runs. Task code must not be able to
// delete or replace it before Fabric's capture reads it.

class TaskEntryRunner {
  extensions: Array<{ tools: Map<string, RegisteredTool> }> = [];
  getAllRegisteredTools(): RegisteredTool[] {
    return this.extensions.flatMap((extension) => [...extension.tools.values()]);
  }
  getActiveTools(): string[] { return []; }
  createContext(): object { return {}; }
  async emit(): Promise<void> {}
  async emitToolCall(): Promise<undefined> { return undefined; }
  async emitToolResult(): Promise<undefined> { return undefined; }
}

const fabricTool = (): ToolDefinition<any, any, any> => ({
  name: "fabric_exec", label: "fabric_exec", description: "fabric_exec",
  parameters: Type.Object({}),
  execute: async () => ({ content: [{ type: "text", text: "ok" }], details: {} }),
});

describe("host ExtensionRunner registration (#8606)", () => {
  it("uses the key the capture reads", () => {
    expect(HOST_EXTENSION_RUNNER_KEY).toBe(HOST_EXTENSION_RUNNER_SYMBOL);
  });

  it("cannot be deleted, reassigned or redefined by later strict or sloppy code", () => {
    const context = vm.createContext({});
    const target = vm.runInContext("globalThis", context) as object;
    const Runner = vm.runInContext("(class Runner {})", context) as object;
    registerHostExtensionRunner(Runner, target);
    const key = `Symbol.for(${JSON.stringify(HOST_EXTENSION_RUNNER_KEY.description)})`;
    for (const attack of [
      `delete globalThis[${key}]`,
      `globalThis[${key}] = class Bogus {}`,
      `Object.defineProperty(globalThis, ${key}, { value: class Bogus {} })`,
    ]) {
      // The context realm has its own TypeError constructor; compare by name.
      // A contextified global's interceptor reports a strict-mode delete as
      // false instead of throwing; the real globalThis test below sees it throw.
      let thrown: unknown;
      let result: unknown;
      try { result = vm.runInContext(`"use strict"; ${attack}`, context); } catch (error) { thrown = error; }
      if (thrown === undefined) expect(result, attack).toBe(false);
      else expect((thrown as Error).name, attack).toBe("TypeError");
      expect(vm.runInContext(`globalThis[${key}]`, context), attack).toBe(Runner);
    }
    // Sloppy mode fails silently and leaves it unchanged.
    expect(vm.runInContext(`delete globalThis[${key}]`, context)).toBe(false);
    vm.runInContext(`globalThis[${key}] = class Bogus {}`, context);
    expect(vm.runInContext(`Reflect.deleteProperty(globalThis, ${key})`, context)).toBe(false);
    expect(vm.runInContext(`Reflect.set(globalThis, ${key}, 1)`, context)).toBe(false);
    expect(vm.runInContext(`globalThis[${key}]`, context)).toBe(Runner);
    expect(Object.getOwnPropertyDescriptor(target, HOST_EXTENSION_RUNNER_KEY)).toEqual({
      value: Runner, writable: false, configurable: false, enumerable: false,
    });
    expect(Object.getOwnPropertySymbols(target).filter((s) => Object.prototype.propertyIsEnumerable.call(target, s))).toEqual([]);
    // A second registration cannot win either.
    expect(() => registerHostExtensionRunner(class Bogus {}, target)).toThrow(/already registered/);
    expect(vm.runInContext(`globalThis[${key}]`, context)).toBe(Runner);
  });

  // Locks this test file's realm for good: keep it last and alone in this file.
  it("stays locked on globalThis and capture still patches the registered runner", async () => {
    registerHostExtensionRunner(TaskEntryRunner);
    const registry = globalThis as Record<PropertyKey, unknown>;
    expect(() => { delete registry[HOST_EXTENSION_RUNNER_SYMBOL]; }).toThrow(TypeError);
    expect(() => { registry[HOST_EXTENSION_RUNNER_SYMBOL] = class Bogus {}; }).toThrow(TypeError);
    expect(() => Object.defineProperty(registry, HOST_EXTENSION_RUNNER_SYMBOL, { value: class Bogus {} })).toThrow(TypeError);
    expect(registry[HOST_EXTENSION_RUNNER_SYMBOL]).toBe(TaskEntryRunner);

    const definition = fabricTool();
    const catalog = new CapturedToolCatalog();
    const controller = await installRegisteredToolCapture({ anchorDefinition: definition, catalog });
    try {
      const runner = new TaskEntryRunner();
      runner.extensions = [{ tools: new Map([["fabric_exec", {
        definition, sourceInfo: createSyntheticSourceInfo("/extensions/pi-fabric/index.ts", { source: "test" }),
      }]]) }];
      expect(catalog.runner).toBeUndefined();
      runner.getAllRegisteredTools();
      expect(catalog.runner).toBe(runner);
    } finally {
      controller.dispose();
    }
  });
});
