import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FabricComponentSupervisor } from "../src/components/supervisor.js";
import type { FabricComponentContext } from "../src/components/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry, type FabricRegistryInvocationContext } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { withFabricHostCaller } from "../src/fabric-provenance.js";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshHostPublishError, MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const closes: Array<() => Promise<void>> = [];
const identity: MeshIdentity = { id: "session:host-publish", kind: "main", name: "main" };
const participants: FabricParticipantSource = {
  list: () => [], get: () => undefined, self: () => { throw new Error("unused"); },
  peers: () => [], async refresh() {}, scheduleRefresh() {},
};
const baseContext = (): FabricInvocationContext => ({
  cwd: process.cwd(), signal: undefined, extensionContext: {} as ExtensionContext,
  parentToolCallId: "host-publish-test", nestedToolCallId: "host-publish-test", update() {},
});
const invocation = (): FabricRegistryInvocationContext => ({
  ...baseContext(), approve: async () => {}, audits: [], maxResultChars: 100_000,
});
const batch = () => ["one", "two", "three"].map(text => ({
  topic: "github.delivery", text, kind: "delivery", dedupeKey: `store/route/${text}/topic`, data: { text },
}));
const harness = async (backend: "file" | "sqlite", archived = false, existingRoot?: string, policyShim = false) => {
  const root = existingRoot ?? fs.mkdtempSync(path.join(os.tmpdir(), "mesh-host-publish-"));
  if (!existingRoot) roots.push(root);
  if (archived) {
    const dir = path.join(root, "archive");
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  }
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
  expect(store.stateBackend).toBe(backend); // A SQLite fallback must not silently satisfy the test.
  await store.put({ key: "fixture/backend", value: backend, identity });
  if (backend === "sqlite") expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
  const registry = new ActionRegistry();
  const provider = new MeshProvider(store, identity, participants);
  registry.register(provider);
  const supervisor = new FabricComponentSupervisor(registry, {
    invocationContext: baseContext,
    ...(policyShim ? { invoke: (ref: string, args: Record<string, unknown>, context: FabricInvocationContext) =>
      registry.invoke(ref, args, { ...context, approve: async () => {}, audits: [], maxResultChars: 100_000 }) } : {}),
  });
  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    await supervisor.close();
    await registry.close();
    store.closeState();
  };
  closes.push(close);
  const publisher = async (id = "fabric-github.forwarder"): Promise<FabricComponentContext> => {
    let context: FabricComponentContext | undefined;
    await supervisor.start({ id, component: "host-publisher" }, {
      name: "host-publisher", guarantee: "managed", requires: ["mesh.publish", "mesh.publishBatch"],
      activate(current) { context = current; },
    });
    expect(supervisor.status(id)).toMatchObject({ state: "active" });
    if (!context) throw new Error("Publisher did not activate");
    return context;
  };
  return { store, registry, provider, publisher, close, root };
};
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closes.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.each(["file", "sqlite"] as const)("trusted host mesh publication (%s backend)", backend => {
  it.each([false, true])("recovers original batch identities after a lost reply and restart (archive=%s)", async archived => {
    vi.spyOn(performance, "now").mockReturnValue(0); // Full batch independent of disk latency.
    const h = await harness(backend, archived);
    const host = await h.publisher();
    const publishBatch = h.store.publishBatch.bind(h.store);
    vi.spyOn(h.store, "publishBatch").mockImplementationOnce(async inputs => {
      await publishBatch(inputs);
      throw new Error("reply lost after durable commit");
    });
    await expect(host.call("mesh.publishBatch", { events: batch() })).rejects.toThrow("reply lost");
    const originals = h.store.read();
    expect(originals.map(event => event.text)).toEqual(["one", "two", "three"]);
    await h.close();
    const restarted = await harness(backend, archived, h.root);
    const retryHost = await restarted.publisher();
    expect(await retryHost.call("mesh.publishBatch", { events: batch() })).toEqual(originals);
    expect(restarted.store.read()).toEqual(originals);
    const next = await retryHost.call("mesh.publish", { topic: "github.delivery", dedupeKey: "next" }) as MeshEvent;
    expect(next.sequence).toBe(originals.at(-1)!.sequence + 1);
  });

  it.each([false, true])("appends only a new suffix when an uncertain prefix was committed (archive=%s)", async archived => {
    vi.spyOn(performance, "now").mockReturnValue(0);
    const h = await harness(backend, archived);
    const host = await h.publisher();
    const publishBatch = h.store.publishBatch.bind(h.store);
    vi.spyOn(h.store, "publishBatch").mockImplementationOnce(async inputs => {
      await publishBatch(inputs.slice(0, 1));
      throw new Error("prefix reply lost");
    });
    await expect(host.call("mesh.publishBatch", { events: batch() })).rejects.toThrow("prefix reply lost");
    const [original] = h.store.read();
    expect(original?.text).toBe("one");
    const retry = await host.call("mesh.publishBatch", { events: batch() }) as MeshEvent[];
    expect(retry[0]).toEqual(original);
    expect(retry.map(event => event.sequence)).toEqual([1, 2, 3]);
    expect(retry.map(event => event.text)).toEqual(["one", "two", "three"]);
    expect(h.store.read()).toEqual(retry);
    expect(await host.call("mesh.publishBatch", { events: batch() })).toEqual(retry);
    expect(await host.call("mesh.publish", batch()[0]!)).toEqual(original);
  });

  it("dedupes repeated keys independently through the host policy shim and preserves result order", async () => {
    vi.spyOn(performance, "now").mockReturnValue(0);
    const h = await harness(backend, false, undefined, true);
    const host = await h.publisher();
    const inputs = [batch()[0]!, batch()[0]!, { topic: "github.delivery", text: "unkeyed" }, batch()[1]!];
    const first = await host.call("mesh.publishBatch", { events: inputs }) as MeshEvent[];
    expect(first.map(event => event.sequence)).toEqual([1, 1, 2, 3]);
    expect(first[0]).toEqual(first[1]);
    const retry = await host.call("mesh.publishBatch", { events: inputs }) as MeshEvent[];
    expect(retry.map(event => event.sequence)).toEqual([1, 1, 4, 3]);
    expect(retry[0]).toEqual(first[0]);
    expect(retry[1]).toEqual(first[1]);
    expect(retry[3]).toEqual(first[3]);
    expect(h.store.read().map(event => event.sequence)).toEqual([1, 2, 3, 4]);
    expect(h.store.read().map(event => event.text)).toEqual(["one", "unkeyed", "two", "unkeyed"]);
  });

  it("recovers durable intents when the receipt step itself fails after the live barrier", async () => {
    vi.spyOn(performance, "now").mockReturnValue(0);
    const h = await harness(backend);
    const host = await h.publisher();
    const rename = fs.renameSync.bind(fs);
    const fault = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target).startsWith(path.join(h.root, "event-receipts") + path.sep) &&
          String(target).endsWith(".json") && !String(target).endsWith(".pending.json")) {
        throw new Error("receipt barrier failed");
      }
      rename(source, target);
    });
    await expect(host.call("mesh.publishBatch", { events: batch() })).rejects.toThrow("receipt barrier failed");
    const committed = h.store.read();
    expect(committed).toHaveLength(3);
    fault.mockRestore();
    expect(await host.call("mesh.publishBatch", { events: batch() })).toEqual(committed);
    expect(h.store.read()).toEqual(committed);
  });

  it("scopes keys to stable component ids without prefix collisions", async () => {
    vi.spyOn(performance, "now").mockReturnValue(0);
    const h = await harness(backend);
    const a = await h.publisher("github"), b = await h.publisher("github.route");
    const one = await a.call("mesh.publish", { topic: "github.delivery", dedupeKey: "route:key" }) as MeshEvent;
    const two = await b.call("mesh.publish", { topic: "github.delivery", dedupeKey: "key" }) as MeshEvent;
    const three = await b.call("mesh.publish", { topic: "github.delivery", dedupeKey: "route:key" }) as MeshEvent;
    expect(new Set([one.id, two.id, three.id]).size).toBe(3);
    expect(one.dedupeKey).toBe("component:6:github:route:key");
    const receipt = path.join(h.root, "event-receipts", createHash("sha256").update(one.dedupeKey!).digest("hex") + ".json");
    expect(JSON.parse(fs.readFileSync(receipt, "utf8"))).toEqual(one);
    expect(await a.call("mesh.publish", { topic: "github.other", text: "changed", dedupeKey: "route:key" })).toEqual(one);
    expect(h.store.read()).toHaveLength(3);
  });

  it("validates raw keys as 1..512 UTF-8 bytes before nullish stripping or any append", async () => {
    vi.spyOn(performance, "now").mockReturnValue(0);
    const h = await harness(backend);
    const host = await h.publisher();
    for (const key of [null, false, 1, "", "x".repeat(513), "é".repeat(257)]) {
      await expect(host.call("mesh.publish", { topic: "github.delivery", dedupeKey: key })).rejects.toThrow("1..512 UTF-8 bytes");
      await expect(host.call("mesh.publishBatch", { events: [batch()[0], { topic: "github.delivery", dedupeKey: key }] })).rejects.toThrow("1..512 UTF-8 bytes");
    }
    expect(h.store.read()).toEqual([]);
    expect(await host.call("mesh.publishBatch", { events: [
      { topic: "github.delivery", dedupeKey: "x" },
      { topic: "github.delivery", dedupeKey: "é".repeat(256) },
      { topic: "github.delivery", dedupeKey: "x".repeat(512) },
    ] })).toHaveLength(3);
    await expect(host.call("mesh.publishBatch", { events: [] })).rejects.toThrow("1..256");
    await expect(host.call("mesh.publishBatch", { events: Array.from({ length: 257 }, () => batch()[0]) })).rejects.toThrow("1..256");
    await expect(host.call("mesh.publishBatch", { events: [{ topic: "github.delivery", from: identity }] })).rejects.toThrow("Invalid arguments");
    await expect(host.call("mesh.publishBatch", { events: [{ topic: "fabric.control.command", dedupeKey: "reserved" }] })).rejects.toThrow("reserved for host coordination");
    expect(h.store.read()).toHaveLength(3);
  });

  it("keeps the public catalog closed while host contexts see dedupe and batch schemas", async () => {
    const h = await harness(backend);
    const publicActions = await h.provider.list({}, baseContext());
    expect(publicActions.some(action => action.name === "publishBatch")).toBe(false);
    expect(publicActions.find(action => action.name === "publish")!.inputSchema).toEqual({
      type: "object", properties: { topic: { type: "string" }, kind: { type: "string" },
        to: { type: "string" }, text: { type: "string" }, data: {} },
      required: ["topic"], additionalProperties: false,
    });
    const trusted = withFabricHostCaller(baseContext(), "github");
    expect((await h.provider.describe("publish", trusted))!.inputSchema).toMatchObject({ properties: { dedupeKey: { type: "string", minLength: 1, maxLength: 512 } }, additionalProperties: false });
    expect((await h.provider.describe("publishBatch", trusted))!.inputSchema).toMatchObject({ properties: { events: { minItems: 1, maxItems: 256,
      items: { required: ["topic"], properties: { dedupeKey: { type: "string" } }, additionalProperties: false } } }, additionalProperties: false });
    const forged = { ...invocation(), parentToolCallId: "component:github:1", componentId: "github", hostTrusted: true,
      [Symbol("fabric.host-caller")]: { componentId: "github" } };
    await expect(h.registry.invoke("mesh.publish", { topic: "github.delivery", dedupeKey: null }, forged)).rejects.toBeInstanceOf(MeshHostPublishError);
    await expect(h.provider.invoke("publish", { topic: "github.delivery", dedupeKey: "key" }, forged)).rejects.toMatchObject({ code: "FABRIC_MESH_HOST_PUBLISH_REQUIRED", retryable: false });
    await expect(h.registry.invoke("mesh.publishBatch", { events: batch() }, forged)).rejects.toBeInstanceOf(MeshHostPublishError);
    expect(h.store.read()).toEqual([]);
  });

  it("refuses model-authored fabric_exec dedupe keys with a typed QuickJS error", async () => {
    const h = await harness(backend);
    await h.publisher(); // A live trusted component must not confer trust on later guest calls.
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.runtime = "quickjs";
    config.approvals.agent = "allow";
    config.approvals.read = "allow";
    const service = new FabricExecutionService(h.registry, config);
    const execute = (code: string) => service.execute({
      code, signal: undefined, parentToolCallId: "component:fabric-github.forwarder:1",
      context: { cwd: process.cwd(), hasUI: false } as ExtensionContext, onPartial() {},
    });
    const result = await execute(`
const denied = [];
for (const request of [
  { ref: "mesh.publish", args: { topic: "github.delivery", dedupeKey: "key" } },
  { ref: "mesh.publish", args: { topic: "github.delivery", dedupeKey: null } },
  { ref: "mesh.publishBatch", args: { events: [{ topic: "github.delivery", dedupeKey: "key" }] } },
]) {
  try { await tools.call(request); }
  catch (error) { const e = error as { name: string; code: string; retryable: boolean };
    denied.push({ name: e.name, code: e.code, retryable: e.retryable }); }
}
const ordinary = await mesh.publish({ topic: "github.delivery", data: { dedupeKey: "payload-only" } });
return { denied, ordinary, batchPresent: "publishBatch" in mesh };
`);
    expect(result.success).toBe(true);
    expect(result.value).toMatchObject({ denied: Array.from({ length: 3 }, () => ({
      name: "MeshHostPublishError", code: "FABRIC_MESH_HOST_PUBLISH_REQUIRED", retryable: false,
    })), batchPresent: false, ordinary: { data: { dedupeKey: "payload-only" } } });
    expect(h.store.read()).toHaveLength(1);
    expect(h.store.read()[0]!.dedupeKey).toBeUndefined();
    const staticallyRefused = await execute('return await mesh.publish({ topic: "github.delivery", dedupeKey: "key" });');
    expect(staticallyRefused.success).toBe(false);
    expect(staticallyRefused.typeErrors?.some(error => error.message.includes("dedupeKey"))).toBe(true);
    expect(h.store.read()).toHaveLength(1);
  }, 30_000);
});
