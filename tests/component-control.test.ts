import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FabricComponentCatalog } from "../src/components/catalog.js";
import { FabricComponentConfiguration, watchComponentConfiguration } from "../src/components/configuration.js";
import { FabricComponentControl, type ComponentChangePlan, type ComponentChangeRequest } from "../src/components/control.js";
import { FabricComponentLoader } from "../src/components/loader.js";
import { FabricComponentSupervisor } from "../src/components/supervisor.js";
import type { FabricComponentDefinition, FabricComponentEntry } from "../src/components/types.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { ComponentsProvider } from "../src/providers/components-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });
const entry = (value: string, id = "demo"): FabricComponentEntry => ({ id, component: id, config: { value } });

async function harness(initial: FabricComponentEntry[] = []) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-component-control-"));
  const agentDir = path.join(root, "agent");
  fs.mkdirSync(agentDir);
  fs.mkdirSync(path.join(root, ".pi"));
  const globalFile = path.join(agentDir, "fabric.json");
  const projectFile = path.join(root, ".pi", "fabric.json");
  fs.writeFileSync(globalFile, JSON.stringify({ ui: { custom: true }, components: initial }), { mode: 0o600 });
  let trusted = true;
  const store = new FabricComponentConfiguration({ cwd: root, agentDir, projectTrusted: () => trusted });
  const context: FabricInvocationContext = {
    cwd: root, signal: undefined, parentToolCallId: "control-test", nestedToolCallId: "control-test",
    extensionContext: {} as ExtensionContext, update() {},
  };
  const registry = new ActionRegistry();
  const catalog = new FabricComponentCatalog();
  const supervisor = new FabricComponentSupervisor(registry, { invocationContext: () => context });
  const loader = new FabricComponentLoader(catalog, supervisor);
  const closed = vi.fn();
  const activated = vi.fn();
  let duringActivation: (() => void) | undefined;
  const definition = (name: string): FabricComponentDefinition<{ value: string }> => ({
    name, provides: [name],
    configSchema: { type: "object", properties: { value: { type: "string", minLength: 1 } }, required: ["value"], additionalProperties: false },
    activate(component, config) {
      activated(name, config.value);
      duringActivation?.();
      if (config.value === "fail") throw new Error("activation failed");
      const descriptor = { name: "get", description: "Read generation value", inputSchema: { type: "object", additionalProperties: false }, risk: "read" as const };
      component.provide({
        name, description: name,
        async list() { return [descriptor]; },
        async describe(action) { return action === "get" ? descriptor : undefined; },
        async invoke() { return config.value; },
        async close() { closed(name, config.value); },
      });
    },
  });
  catalog.register(definition("demo"));
  catalog.register(definition("other"));
  await loader.reconcile(initial);
  const control = new FabricComponentControl(loader, { store });
  const provider = new ComponentsProvider(loader, control);
  registry.register(provider);
  registry.setUnavailableResolver(name => loader.unavailableProviderMessage(name));
  const call = (ref: string, args: Record<string, unknown> = {}, invocation = context) => registry.invoke(ref, args, {
    ...invocation, approve: async () => {}, audits: [], maxResultChars: 32_768,
  });
  const plan = (request: ComponentChangeRequest) => call("components.plan", request as Record<string, unknown>) as Promise<ComponentChangePlan>;
  const apply = async (request: ComponentChangeRequest) => {
    const planned = await plan(request);
    return call("components.apply", { ...planned.request, expectedRevision: planned.revision });
  };
  cleanup.push(async () => { await control.close(); await loader.close(); await registry.close(); fs.rmSync(root, { recursive: true, force: true }); });
  return { root, globalFile, projectFile, store, context, registry, catalog, loader, control, provider, closed, activated, definition, call, plan, apply,
    trust: (value: boolean) => { trusted = value; }, onActivate: (fn: (() => void) | undefined) => { duringActivation = fn; } };
}

describe("live component control", () => {
  it("describes schemas before activation and adds a session instance without file writes", async () => {
    const h = await harness();
    const before = fs.readFileSync(h.globalFile, "utf8");
    expect(await h.call("components.describe", { component: "demo" })).toMatchObject({ configSchema: { required: ["value"] }, instances: [] });
    await expect(h.registry.describe("demo.get", h.context)).rejects.toThrow("components.plan");
    await expect(h.plan({ entries: [{ id: "demo", component: "demo", config: { value: 2 } }] })).rejects.toThrow("Invalid config");
    expect(h.activated).not.toHaveBeenCalled();
    const planned = await h.plan({ entries: [entry("one")] });
    expect(planned.changes).toEqual([{ id: "demo", operation: "add", component: "demo", requirements: [], provisions: ["demo"] }]);
    expect(h.activated).not.toHaveBeenCalled();
    await h.call("components.apply", { ...planned.request, expectedRevision: planned.revision });
    expect(await h.call("demo.get")).toBe("one");
    expect(fs.readFileSync(h.globalFile, "utf8")).toBe(before);
    expect(h.control.configuration().sessionOverrides).toEqual(["demo"]);
  });

  it("replaces/removes live bindings while committed views drain and unrelated components stay unchanged", async () => {
    const h = await harness([entry("old"), entry("untouched", "other")]);
    const pinned = await h.registry.acquireCapabilityView(["demo.get"], h.context);
    const otherRevision = h.loader.status("other").revision;
    try {
      await h.apply({ entries: [entry("new")] });
      expect(await h.call("demo.get")).toBe("new");
      expect(await h.call("demo.get", {}, { ...h.context, capabilityView: pinned.view! })).toBe("old");
      expect(h.closed).not.toHaveBeenCalledWith("demo", "old");
      await h.apply({ remove: ["demo"] });
      await expect(h.call("demo.get")).rejects.toThrow("no instance");
      expect(await h.call("demo.get", {}, { ...h.context, capabilityView: pinned.view! })).toBe("old");
      expect(h.loader.status("other").revision).toBe(otherRevision);
      expect(await h.call("other.get")).toBe("untouched");
    } finally { await pinned.release(); }
    expect(h.closed).toHaveBeenCalledWith("demo", "old");
  });

  it("rejects stale and concurrent plans instead of losing updates", async () => {
    const h = await harness();
    const a = await h.plan({ entries: [entry("a")] });
    const b = await h.plan({ entries: [entry("b")] });
    const results = await Promise.allSettled([a, b].map(plan => h.call("components.apply", { ...plan.request, expectedRevision: plan.revision })));
    expect(results.map(result => result.status)).toEqual(["fulfilled", "rejected"]);
    expect(await h.call("demo.get")).toBe("a");
    expect(String((results[1] as PromiseRejectedResult).reason)).toContain("stale");
  });

  it("bounds session removal masks instead of growing an unbounded overlay", async () => {
    const h = await harness();
    await h.apply({ remove: Array.from({ length: 256 }, (_, index) => `masked-${index}`) });
    await expect(h.plan({ remove: ["overflow"] })).rejects.toThrow("256 session component overrides");
    await h.apply({ reset: ["masked-0"], entries: [entry("one")] });
    expect(await h.call("demo.get")).toBe("one");
  });

  it("serializes legacy reload with apply and invalidates earlier plans", async () => {
    const h = await harness([entry("old")]);
    const plan = await h.plan({ entries: [entry("new")] });
    const reload = h.call("components.reload", { id: "demo" });
    const apply = h.call("components.apply", { ...plan.request, expectedRevision: plan.revision });
    await reload;
    await expect(apply).rejects.toThrow("stale");
    expect(await h.call("demo.get")).toBe("old");
  });

  it("restores prior instances and leaves disk untouched after a multi-entry activation failure", async () => {
    const h = await harness([entry("old")]);
    const before = fs.readFileSync(h.globalFile, "utf8");
    await expect(h.apply({ scope: "global", entries: [entry("new"), entry("fail", "other")] })).rejects.toThrow("activation failed");
    expect(await h.call("demo.get")).toBe("old");
    expect(h.registry.has("other")).toBe(false);
    expect(fs.readFileSync(h.globalFile, "utf8")).toBe(before);
    expect(h.control.configuration().error).toContain("activation failed");
  });

  it("persists only explicit scopes and preserves unrelated settings", async () => {
    const h = await harness();
    await h.apply({ scope: "global", entries: [entry("persisted")] });
    expect(JSON.parse(fs.readFileSync(h.globalFile, "utf8"))).toEqual({ ui: { custom: true }, components: [entry("persisted")] });
    if (process.platform !== "win32") expect(fs.statSync(h.globalFile).mode & 0o777).toBe(0o600);
    expect(await h.call("demo.get")).toBe("persisted");
    await h.apply({ scope: "project", entries: [entry("project")] });
    expect(await h.call("demo.get")).toBe("project");
    const shadowed = await h.plan({ scope: "global", entries: [entry("global")] });
    expect(shadowed.warnings.join(" ")).toContain("shadows");
    await h.call("components.apply", { ...shadowed.request, expectedRevision: shadowed.revision });
    expect(await h.call("demo.get")).toBe("project");
  });

  it("reports untrusted project configuration without parsing it or falling back on writes", async () => {
    const h = await harness();
    fs.writeFileSync(h.projectFile, "not json and must not be parsed");
    h.trust(false);
    await h.call("components.reconcile");
    const listed = await h.call("components.list") as { configuration: { warnings: string[]; sources: unknown[] } };
    expect(listed.configuration.warnings.join(" ")).toContain("not trusted");
    expect(listed.configuration.sources).toContainEqual(expect.objectContaining({ scope: "project", trusted: false, present: true }));
    await expect(h.plan({ scope: "project", entries: [entry("bad")] })).rejects.toThrow("untrusted");
    await h.apply({ entries: [entry("session")] });
    expect(await h.call("demo.get")).toBe("session");
    expect(fs.readFileSync(h.projectFile, "utf8")).toBe("not json and must not be parsed");
  });

  it("keeps session overrides across disk changes and resets them explicitly", async () => {
    const h = await harness([entry("disk")]);
    await h.apply({ entries: [entry("session")] });
    fs.writeFileSync(h.globalFile, JSON.stringify({ components: [entry("disk-new")] }));
    await h.call("components.reconcile");
    expect(await h.call("demo.get")).toBe("session");
    await h.apply({ reset: ["demo"] });
    expect(await h.call("demo.get")).toBe("disk-new");
    expect(h.control.configuration().sessionOverrides).toEqual([]);
  });

  it("fails closed on malformed live files without repairing or losing the last working state", async () => {
    const h = await harness([entry("good")]);
    const original = fs.readFileSync(h.globalFile, "utf8");
    for (const invalid of ["{", JSON.stringify({ components: "wrong" }), JSON.stringify({ components: [{ id: "demo" }] })]) {
      fs.writeFileSync(h.globalFile, invalid);
      await expect(h.call("components.reconcile")).rejects.toThrow();
      expect(await h.call("demo.get")).toBe("good");
      expect(fs.readFileSync(h.globalFile, "utf8")).toBe(invalid);
    }
    fs.writeFileSync(h.globalFile, original);
    await h.call("components.reconcile");
    expect(h.control.configuration().error).toBeUndefined();
  });

  it("reconciles files that changed between bootstrap and control-plane construction", async () => {
    const h = await harness([entry("bootstrap")]);
    fs.writeFileSync(h.globalFile, JSON.stringify({ components: [entry("latest")] }));
    const control = new FabricComponentControl(h.loader, { store: h.store });
    try {
      await control.reconcile();
      expect(await h.call("demo.get")).toBe("latest");
    } finally { await control.close(); }
  });

  it("keeps the bootstrap manifest available when the first live source read is damaged", async () => {
    const h = await harness([entry("bootstrapped")]);
    fs.writeFileSync(h.globalFile, "{");
    const control = new FabricComponentControl(h.loader, { store: h.store });
    try {
      expect(control.configuration().error).toContain("Invalid JSON");
      expect(await h.call("demo.get")).toBe("bootstrapped");
      fs.writeFileSync(h.globalFile, JSON.stringify({ components: [entry("repaired")] }));
      await control.reconcile();
      expect(await h.call("demo.get")).toBe("repaired");
    } finally { await control.close(); }
  });

  it("detects file/catalog changes between plan and apply", async () => {
    const h = await harness();
    const plan = await h.plan({ entries: [entry("planned")] });
    fs.writeFileSync(h.globalFile, JSON.stringify({ components: [], ui: { changed: true } }));
    await expect(h.call("components.apply", { ...plan.request, expectedRevision: plan.revision })).rejects.toThrow("stale");
    const second = await h.plan({ entries: [entry("planned")] });
    h.catalog.register(h.definition("demo"), { overwrite: true });
    await expect(h.call("components.apply", { ...second.request, expectedRevision: second.revision })).rejects.toThrow("stale");
    expect(h.activated).not.toHaveBeenCalled();
  });

  it("compensates activation when a concurrent file edit prevents persistence", async () => {
    const h = await harness([entry("old")]);
    h.onActivate(() => {
      h.onActivate(undefined);
      fs.writeFileSync(h.globalFile, JSON.stringify({ components: [entry("external")] }));
    });
    await expect(h.apply({ scope: "global", entries: [entry("candidate")] })).rejects.toThrow("configuration changed");
    expect(await h.call("demo.get")).toBe("old");
    expect(JSON.parse(fs.readFileSync(h.globalFile, "utf8")).components).toEqual([entry("external")]);
    await h.call("components.reconcile");
    expect(await h.call("demo.get")).toBe("external");
  });

  it("does not grant configuration authority to pinned guests or permit built-in overrides", async () => {
    const h = await harness();
    await expect(h.plan({ entries: [{ id: "fabric.provider.pi", component: "demo", config: { value: "bad" } }] })).rejects.toThrow("reserved");
    const plan = await h.plan({ entries: [entry("new")] });
    const lease = await h.registry.acquireCapabilityView(["components.apply", "components.reconcile"], h.context);
    try {
      const pinned = { ...h.context, capabilityView: lease.view! };
      await expect(h.call("components.apply", { ...plan.request, expectedRevision: plan.revision }, pinned)).rejects.toThrow("unrestricted host");
      await expect(h.call("components.reconcile", {}, pinned)).rejects.toThrow("unrestricted host");
    } finally { await lease.release(); }
    expect(h.activated).not.toHaveBeenCalled();
  });

  it("exposes only complete planned operations and checks cancellation before activation", async () => {
    const h = await harness();
    await expect(h.plan({ entries: [entry("a")], remove: ["demo"] })).rejects.toThrow("only once");
    await expect(h.call("components.apply", { entries: [entry("a")] })).rejects.toThrow("expectedRevision");
    const plan = await h.plan({ entries: [entry("a")] });
    await expect(h.control.apply({ ...plan.request, expectedRevision: plan.revision }, AbortSignal.abort())).rejects.toThrow();
    expect(h.activated).not.toHaveBeenCalled();
  });

  it("validates direct supervisor replacements and all transaction configs before activation", async () => {
    const h = await harness([entry("old")]);
    const revision = h.loader.status("demo").revision;
    h.activated.mockClear();
    await expect(h.loader.supervisor.replace("demo", { id: "demo", component: "demo", config: {} }, h.catalog.get("demo")!.definition)).rejects.toThrow("Invalid config");
    expect(() => h.loader.reconcile([entry("new"), { id: "other", component: "other", config: {} }])).toThrow("Invalid config");
    expect(h.activated).not.toHaveBeenCalled();
    expect(h.loader.status("demo").revision).toBe(revision);
    expect(await h.call("demo.get")).toBe("old");
    await h.apply({ entries: [{ id: "demo", component: "demo", disabled: true }] });
    expect(h.registry.has("demo")).toBe(false);
    await expect(h.apply({ entries: [{ id: "demo", component: "demo" }] })).rejects.toThrow("Invalid config");
  });

  it("rejects lifecycle re-entry rather than deadlocking the control queue", async () => {
    const h = await harness();
    h.catalog.register({
      name: "reentrant", requires: ["components.plan"],
      async activate(context) {
        await context.call("components.plan", { entries: [entry("nested")] });
      },
    });
    await expect(h.apply({ entries: [{ id: "reentrant", component: "reentrant" }] })).rejects.toThrow();
    await h.apply({ entries: [entry("healthy")] });
    expect(await h.call("demo.get")).toBe("healthy");
  }, 2000);

  // smarty-dev#883: the watcher took its baseline on its first asynchronous poll, so a
  // replacement made right after watching started was never reported (this test flaked, 2 in 5
  // under load, waiting for the first reconcile).
  it("reports a replacement made right after watching starts", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-watch-"));
    cleanup.push(async () => fs.rmSync(dir, { recursive: true, force: true }));
    const file = path.join(dir, "components.json");
    fs.writeFileSync(file, JSON.stringify({ components: [] }));
    for (let attempt = 0; attempt < 5; attempt++) {
      const changed = vi.fn();
      const stop = watchComponentConfiguration([file], changed);
      cleanup.push(async () => stop());
      fs.writeFileSync(`${file}.tmp`, JSON.stringify({ components: [], attempt }));
      fs.renameSync(`${file}.tmp`, file);                                 // before any poll ran
      await vi.waitFor(() => expect(changed).toHaveBeenCalledTimes(1), { timeout: 3_000, interval: 20 });
      stop();
    }
  });

  it("reconciles atomic file replacements automatically and stops watching on disposal", async () => {
    const h = await harness();
    const changed = vi.fn(() => { void h.control.reconcile(); });
    const stop = watchComponentConfiguration(h.store.paths, changed);
    cleanup.push(async () => stop());
    const write = (entries: FabricComponentEntry[]) => {
      const temp = `${h.globalFile}.tmp`;
      fs.writeFileSync(temp, JSON.stringify({ components: entries }));
      fs.renameSync(temp, h.globalFile);
    };
    write([entry("one")]);
    await vi.waitFor(() => expect(h.registry.has("demo")).toBe(true), { timeout: 3000 });
    write([entry("two")]);
    await vi.waitFor(async () => expect(await h.call("demo.get")).toBe("two"), { timeout: 3000 });
    write([]);
    await vi.waitFor(() => expect(h.registry.has("demo")).toBe(false), { timeout: 3000 });
    stop();
    const calls = changed.mock.calls.length;
    write([entry("after-stop")]);
    await new Promise(resolve => setTimeout(resolve, 400));
    expect(changed).toHaveBeenCalledTimes(calls);
    expect(h.registry.has("demo")).toBe(false);
  });
});
