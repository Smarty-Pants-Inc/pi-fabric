import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { STATE_BACKEND_FACTORIES, ShadowStateBackend, SqliteStateBackend,
  type StateBackend } from "../src/mesh/state-backend.js";
import { freezePrefixEntries, type PrefixWitness } from "../src/mesh/prefix-observation.js";
import { StateFile } from "../src/mesh/state-file.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Keep: CSEAM-1's recorded real catch/data-safety boundary. Exercise the REAL facade plus FILE,
// native Shadow and native SQLite legacy reads. Instance-local optional-proof probes establish a
// saved pair and count forbidden post-close entry; they do NOT prove concrete backend observations.
// The separately labelled NATS fault port proves wiring/once/error parity, NOT real JetStream.
const prefix = "residency/deliveries/";
const identity: MeshIdentity = { id: "closed-prefix-test", name: "closed-prefix-test", kind: "main" };
type Observe = NonNullable<StateBackend["observePrefix"]>;
const stores: MeshStore[] = [];
const roots: string[] = [];
const restores: Array<() => void> = [];
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-closed-prefix-"));
  roots.push(root);
  return root;
};
const openNative = (kind: "file" | "shadow" | "sqlite") => {
  const mesh = new MeshStore(tempRoot(), 64 * 1024, 1_000, { stateBackend: kind });
  stores.push(mesh);
  // No availability override, factory mock, skip or FILE-fallback-as-native-coverage.
  expect(mesh.stateBackend).toBe(kind);
  expect(mesh.stateBackendHandle).toBeInstanceOf(kind === "file" ? StateFile
    : kind === "shadow" ? ShadowStateBackend : SqliteStateBackend);
  return mesh;
};
const seed = async (mesh: MeshStore) => {
  await mesh.writeBatch({ identity, ops: [
    { kind: "put", key: prefix + "own/z", value: { nested: [{ payload: ["own", 2] }] } },
    { kind: "put", key: "residency/configs/outside", value: { excluded: true } },
    { kind: "put", key: prefix + "foreign/a", value: { nested: [{ payload: ["foreign", 1] }] } },
  ] });
};
const registerPair = (mesh: MeshStore, authority: StateBackend) => {
  const previous = Object.getOwnPropertyDescriptor(authority, "observePrefix");
  const observe = vi.fn<Observe>().mockReturnValue({ kind: "snapshot", witness: Object.freeze({}),
    entries: freezePrefixEntries(authority.listAll(prefix)) });
  Object.defineProperty(authority, "observePrefix", { configurable: true, writable: true, value: observe });
  restores.push(() => {
    if (previous) Object.defineProperty(authority, "observePrefix", previous);
    else Reflect.deleteProperty(authority, "observePrefix");
  });
  const saved = mesh.observePrefix(prefix);
  expect(saved.kind).toBe("snapshot");
  if (saved.kind !== "snapshot") throw new Error("expected registered facade pair before close");
  expect(observe).toHaveBeenCalledExactlyOnceWith(prefix, undefined);
  return { witness: saved.witness, observe };
};
const errorFrom = (operation: () => unknown): Error => {
  try { operation(); } catch (error) {
    if (!(error instanceof Error)) throw new Error("expected a canonical Error instance");
    return error;
  }
  throw new Error("expected the legacy closed read to fail");
};
const expectClosedUnknown = (mesh: MeshStore, previous?: PrefixWitness) => {
  expect(mesh.observePrefix(prefix, previous)).toEqual({ kind: "unknown", reason: "closed-prefix-observation-facade" });
};

afterEach(async () => {
  try {
    for (const mesh of stores.slice().reverse()) mesh.closeState();
    for (const mesh of stores) {
      const state = mesh.stateBackendHandle;
      if (state instanceof ShadowStateBackend) await state.flush();
    }
  } finally {
    stores.length = 0;
    for (const restore of restores.splice(0).reverse()) restore();
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("CSEAM-1: closed facade UNKNOWN leaves exactly one legacy caller fallback", () => {
  it.each(["file", "shadow"] as const)("real %s retains the global legacy selection with zero optional reads and one fallback scan", async kind => {
    const mesh = openNative(kind), state = mesh.stateBackendHandle;
    await seed(mesh);
    if (state instanceof ShadowStateBackend) {
      await state.flush();
      expect(state.diagnostics().shadowFailures).toBe(0);
      expect(state.diagnostics().shadowApplied).toBeGreaterThan(0);
      expect(fs.existsSync(state.shadow.database)).toBe(true);
    }
    const legacy = mesh.listAll(prefix);
    expect(legacy.map(entry => entry.key)).toEqual([prefix + "foreign/a", prefix + "own/z"]);
    const authority = state instanceof ShadowStateBackend ? state.file : state;
    expect(authority).toBeInstanceOf(StateFile);
    const pair = registerPair(mesh, authority);
    mesh.closeState();
    pair.observe.mockClear();
    const backendScan = vi.spyOn(state, "listAll");
    const fileScan = state instanceof ShadowStateBackend ? vi.spyOn(state.file, "listAll") : backendScan;
    const shadowObserve = state instanceof ShadowStateBackend ? vi.spyOn(state, "observePrefix") : undefined;
    const mirrorScan = state instanceof ShadowStateBackend ? vi.spyOn(state.shadow, "listAll") : undefined;

    expectClosedUnknown(mesh, pair.witness);
    expect(backendScan).not.toHaveBeenCalled();
    expect(fileScan).not.toHaveBeenCalled();
    expect(pair.observe).not.toHaveBeenCalled();
    if (shadowObserve) expect(shadowObserve).not.toHaveBeenCalled();

    // UNKNOWN discards the saved pair; this is precisely the existing caller's listAll(prefix),
    // with no fresh:true, stronger options, retry or read used merely to make an assertion.
    const selected = mesh.listAll(prefix);
    expect(selected).toEqual(legacy); // all payload/writer/version/timestamp fields and ordering
    expect(backendScan).toHaveBeenCalledExactlyOnceWith(prefix, {});
    expect(fileScan).toHaveBeenCalledExactlyOnceWith(prefix, {});
    expect(pair.observe).not.toHaveBeenCalled();
    if (shadowObserve) expect(shadowObserve).not.toHaveBeenCalled();
    if (mirrorScan) expect(mirrorScan).not.toHaveBeenCalled();
    expect(Object.isFrozen(selected)).toBe(false);
    expect(Object.isFrozen(selected[0]!.value)).toBe(false);
  });

  it("native SQLite keeps the independent baseline closed error through only one caller fallback", async () => {
    const baseline = openNative("sqlite"), mesh = openNative("sqlite");
    await seed(baseline);
    await seed(mesh); // forces a real native database open/write, not just a selector label
    baseline.closeState();
    // Capture on an INDEPENDENT store: no baseline/assertion read is charged to the measured one.
    const legacyError = errorFrom(() => baseline.listAll(prefix));
    const state = mesh.stateBackendHandle;
    const pair = registerPair(mesh, state);
    mesh.closeState();
    pair.observe.mockClear();
    const scan = vi.spyOn(state, "listAll");

    expectClosedUnknown(mesh, pair.witness);
    expect(pair.observe).not.toHaveBeenCalled();
    expect(scan).not.toHaveBeenCalled();
    const caught = errorFrom(() => mesh.listAll(prefix)); // the ONE existing fallback/error budget
    expect(caught.constructor).toBe(legacyError.constructor);
    expect(caught.name).toBe(legacyError.name);
    expect(caught.message).toBe(legacyError.message);
    expect(scan).toHaveBeenCalledExactlyOnceWith(prefix, {});
    expect(scan.mock.results[0]?.type).toBe("throw");
    expect(scan.mock.results[0]?.value).toBe(caught); // exact backend instance, no wrapping/catch-as-UNKNOWN
    expect(pair.observe).not.toHaveBeenCalled();
  });

  it("typed NATS closed fault port preserves one fallback/error instance (NOT real JetStream proof)", () => {
    const fault = new Error("Fabric mesh NATS state backend is closed");
    let closed = false;
    const scan = vi.fn<StateBackend["listAll"]>(() => {
      if (closed) throw fault;
      return [];
    });
    const observe = vi.fn<Observe>(() => {
      if (closed) throw fault;
      return { kind: "unknown", reason: "typed-NATS-fault-port" };
    });
    // A complete typed backend fault port, not a fake claimed to be native NATS or a projector.
    const port: StateBackend = {
      kind: "nats", listAll: scan, observePrefix: observe,
      get: () => undefined, list: () => [], listAllShared: () => [],
      stateToken: () => ({}), stateStamp: () => undefined, cachedStateStamp: () => undefined,
      readCacheMs: 0, backgroundReadCacheMs: 0, readCacheRemainingMs: 0,
      put: async () => { throw new Error("no writer in typed fault port"); },
      delete: async () => { throw new Error("no writer in typed fault port"); },
      writeBatch: async () => { throw new Error("no writer in typed fault port"); },
      confirmWritable: async () => { throw new Error("no writer in typed fault port"); },
      withWriteFence: operation => operation(), sync: async () => undefined,
      dropCache: () => undefined, diagnostics: () => ({ kind: "nats" }), close: () => { closed = true; },
    };
    vi.spyOn(STATE_BACKEND_FACTORIES.nats, "create").mockReturnValueOnce(port);
    const mesh = new MeshStore(tempRoot(), 64 * 1024, 1_000, { stateBackend: "nats" });
    stores.push(mesh);
    expect(mesh.stateBackendHandle).toBe(port);
    mesh.closeState();

    expectClosedUnknown(mesh);
    expect(scan).not.toHaveBeenCalled();
    expect(observe).not.toHaveBeenCalled();
    const caught = errorFrom(() => mesh.listAll(prefix));
    expect(caught).toBe(fault);
    expect(scan).toHaveBeenCalledExactlyOnceWith(prefix, {});
    expect(observe).not.toHaveBeenCalled();
  });
});
