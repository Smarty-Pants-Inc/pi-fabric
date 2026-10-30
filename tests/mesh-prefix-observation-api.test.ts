import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { STATE_BACKEND_FACTORIES, type StateBackend, type MeshStateBackendKind } from "../src/mesh/state-backend.js";
import type { MeshStateEntry } from "../src/mesh/state-file.js";
import { bindPrefixObservation, freezePrefixEntries, PrefixObservationCache,
  type PrefixObservation, type PrefixWitness } from "../src/mesh/prefix-observation.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Keep: data-safety/trust boundary. Exercise the actual facade and runtime normalization without
// opening SQLite, writing FILE/shadow state, or preparing/connecting any NATS backend.
const prefix = "residency/deliveries/";
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "main", sessionId: "session:test" };
const stores: MeshStore[] = [];
const entry = (key = prefix + "foreign/a"): MeshStateEntry => ({ key, version: 1, updatedAt: 1,
  updatedBy: { ...identity }, value: { nested: [{ list: [1, 2] }] } });
const entries = () => freezePrefixEntries([entry()]);
const witness = () => Object.freeze({}) as PrefixWitness;
const snapshot = (result: PrefixObservation) => {
  expect(result.kind).toBe("snapshot");
  if (result.kind !== "snapshot") throw new Error("expected complete snapshot");
  return result;
};
const backend = (kind: MeshStateBackendKind = "file"): StateBackend => ({
  kind, get: () => undefined, list: () => [entry()], listAll: () => [entry()], listAllShared: () => [entry()],
  stateToken: () => ({}), stateStamp: () => undefined, cachedStateStamp: () => undefined,
  readCacheMs: 0, backgroundReadCacheMs: 0, readCacheRemainingMs: 0,
  put: async () => { throw new Error("no writer in API proof"); },
  delete: async () => { throw new Error("no writer in API proof"); },
  writeBatch: async () => { throw new Error("no writer in API proof"); },
  confirmWritable: async () => { throw new Error("no writer in API proof"); },
  withWriteFence: operation => operation(), sync: async () => undefined,
  dropCache: () => undefined, diagnostics: () => ({ kind }), close: () => undefined,
});
const facade = (state = backend()) => {
  vi.spyOn(STATE_BACKEND_FACTORIES.file, "create").mockReturnValueOnce(state);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prefix-api-"));
  const mesh = new MeshStore(root, 64 * 1024, 1000, { stateBackend: "file" });
  stores.push(mesh);
  return mesh;
};
afterEach(() => {
  for (const mesh of stores.splice(0).reverse()) mesh.closeState();
  vi.restoreAllMocks();
});

describe("standalone optional prefix API (no SQLite internals)", () => {
  it.each(["file", "shadow", "sqlite", "nats"] as const)("%s absence returns UNKNOWN, leaving exact listAll semantics to the caller", kind => {
    const state = backend(kind); // Full StateBackend compiles with no observePrefix property.
    const scan = vi.spyOn(state, "listAll");
    const mesh = facade(state);
    expect(mesh.observePrefix(prefix)).toEqual({ kind: "unknown", reason: "unsupported-prefix-observation-backend" });
    expect(scan).not.toHaveBeenCalled();
    const options = { fresh: true };
    const mutable = mesh.listAll(prefix, options);
    expect(scan).toHaveBeenCalledExactlyOnceWith(prefix, options);
    expect(mutable).toEqual([entry()]);
    expect(Object.isFrozen(mutable)).toBe(false);
    expect(Object.isFrozen(mutable[0]!.value)).toBe(false);
  });

  it("borrows an owned deep-frozen sequence once; hits never clone, decode, sort or refreeze it", () => {
    const state = backend();
    const sequence = entries(), token = witness();
    const observe = vi.fn<NonNullable<StateBackend["observePrefix"]>>()
      .mockReturnValueOnce({ kind: "snapshot", witness: token, entries: sequence })
      .mockReturnValue({ kind: "unchanged", witness: token });
    state.observePrefix = observe;
    const mesh = facade(state);
    const first = snapshot(mesh.observePrefix(prefix));
    expect(first.entries).toBe(sequence);
    const value = first.entries[0]!.value as { nested: Array<{ list: number[] }> };
    expect(() => value.nested[0]!.list.push(3)).toThrow(TypeError);
    expect(() => { (first.entries[0]!.updatedBy as { id: string }).id = "forged"; }).toThrow(TypeError);
    const bytes = JSON.stringify(sequence);
    const parse = vi.spyOn(JSON, "parse"), clone = vi.spyOn(globalThis, "structuredClone");
    const freeze = vi.spyOn(Object, "freeze"), sort = vi.spyOn(Array.prototype, "sort");
    let previous = first.witness;
    for (let index = 0; index < 3; index++) {
      const result = mesh.observePrefix(prefix, previous);
      if (result.kind !== "unchanged") throw new Error("expected paired unchanged");
      previous = result.witness;
    }
    expect(parse).not.toHaveBeenCalled();
    expect(clone).not.toHaveBeenCalled();
    expect(sort).not.toHaveBeenCalled();
    expect(freeze.mock.calls.every(([value]) => value !== null && typeof value === "object" && Object.keys(value).length === 0)).toBe(true);
    expect(JSON.stringify(first.entries)).toBe(bytes);
    expect(observe.mock.calls.slice(1).every(([, previous]) => previous === token)).toBe(true);
  });

  it("independently binds each facade/prefix; forged, evicted and reopened handles never forward authority", () => {
    const state = backend(), token = witness(), sequence = entries();
    const observe = vi.fn<NonNullable<StateBackend["observePrefix"]>>()
      .mockReturnValue({ kind: "snapshot", witness: token, entries: sequence });
    state.observePrefix = observe;
    const a = facade(state), b = facade(state);
    const first = snapshot(a.observePrefix(prefix));
    b.observePrefix(prefix, first.witness);
    expect(observe.mock.lastCall?.[1]).toBeUndefined();
    a.observePrefix(prefix, witness());
    expect(observe.mock.lastCall?.[1]).toBeUndefined();
    a.observePrefix("other/", first.witness);
    expect(observe.mock.lastCall?.[1]).toBeUndefined();
    const saved = snapshot(a.observePrefix(prefix));
    for (let index = 0; index < 33; index++) {
      const other = `other/${index}/`;
      observe.mockReturnValueOnce({ kind: "snapshot", witness: token, entries: freezePrefixEntries([entry(other + "a")]) });
      snapshot(a.observePrefix(other));
    }
    a.observePrefix(prefix, saved.witness);
    expect(observe.mock.lastCall?.[1]).toBeUndefined();
    a.closeState();
    const count = observe.mock.calls.length;
    expect(a.observePrefix(prefix, saved.witness).kind).toBe("unknown");
    expect(observe).toHaveBeenCalledTimes(count);
    facade(state).observePrefix(prefix, saved.witness);
    expect(observe.mock.lastCall?.[1]).toBeUndefined();
  });

  it("exhaustively rejects malformed or unpaired runtime results and discards lost proof atomically", () => {
    const state = backend(), observe = vi.fn<NonNullable<StateBackend["observePrefix"]>>();
    state.observePrefix = observe;
    const mesh = facade(state), token = witness(), sequence = entries();
    const badValues: unknown[] = [null, undefined, [], 0, {}, { kind: "new-kind" }, { kind: "snapshot" },
      { kind: "snapshot", witness: token, entries: [] },
      { kind: "snapshot", witness: {}, entries: sequence },
      { kind: "snapshot", witness: token, entries: Object.freeze([{ ...sequence[0], version: NaN }]) },
      { kind: "snapshot", witness: token, entries: freezePrefixEntries([entry("other/a")]) },
      { kind: "snapshot", witness: token, entries: freezePrefixEntries([entry(prefix + "z"), entry(prefix + "a")]) },
      { kind: "unchanged", witness: token }, { get kind() { throw new Error("must not invoke getter"); } }];
    for (const bad of badValues) {
      observe.mockReturnValueOnce(bad as PrefixObservation);
      expect(mesh.observePrefix(prefix, token).kind).toBe("unknown");
      expect(observe.mock.lastCall?.[1]).toBeUndefined();
    }
    observe.mockReturnValueOnce({ kind: "snapshot", witness: token, entries: sequence });
    const saved = snapshot(mesh.observePrefix(prefix));
    observe.mockReturnValueOnce({ kind: "unknown", reason: "lost-proof" });
    expect(mesh.observePrefix(prefix, saved.witness)).toEqual({ kind: "unknown", reason: "lost-proof" });
    observe.mockReturnValueOnce({ kind: "unchanged", witness: token });
    expect(mesh.observePrefix(prefix, saved.witness).kind).toBe("unknown");
    expect(observe.mock.lastCall?.[1]).toBeUndefined();
    expect(bindPrefixObservation(new PrefixObservationCache<PrefixWitness>(), prefix, token,
      () => ({ kind: "unchanged", witness: token })).kind).toBe("unknown");
  });

  it("preserves live canonical errors once; post-close errors come only from the caller's fallback", () => {
    const state = backend(), fault = new Error("canonical busy/corruption/closed");
    const observe = vi.fn(() => { throw fault; });
    state.observePrefix = observe;
    const scan = vi.spyOn(state, "listAll"), mesh = facade(state);
    expect(() => mesh.observePrefix(prefix)).toThrow(fault);
    expect(observe).toHaveBeenCalledTimes(1);
    expect(scan).not.toHaveBeenCalled();
    mesh.closeState();
    scan.mockImplementation(() => { throw fault; });
    // The old internal closed-read probe was not an immutable legacy contract: on FILE/Shadow it
    // succeeded and caused two scans. Retirement returns UNKNOWN; only the caller's fallback reads.
    expect(mesh.observePrefix(prefix)).toEqual({ kind: "unknown", reason: "closed-prefix-observation-facade" });
    expect(scan).not.toHaveBeenCalled();
    let caught: unknown;
    try { mesh.listAll(prefix); } catch (error) { caught = error; }
    expect(caught).toBe(fault);
    expect(scan).toHaveBeenCalledExactlyOnceWith(prefix, {});
    expect(observe).toHaveBeenCalledTimes(1);
  });
});

describe("facade predecessor failure regression", () => {
  it("invalidates a registered predecessor on canonical error before retry, without a second scan or changed error", () => {
    const state = backend(), token = witness(), sequence = entries();
    const fault = new Error("canonical prefix read failed");
    const observe = vi.fn<NonNullable<StateBackend["observePrefix"]>>()
      .mockReturnValueOnce({ kind: "snapshot", witness: token, entries: sequence })
      .mockImplementationOnce(() => { throw fault; })
      .mockReturnValue({ kind: "snapshot", witness: token, entries: sequence });
    state.observePrefix = observe;
    const scan = vi.spyOn(state, "listAll"), mesh = facade(state);
    try {
      const saved = snapshot(mesh.observePrefix(prefix));
      let caught: unknown;
      try { mesh.observePrefix(prefix, saved.witness); } catch (error) { caught = error; }
      expect(caught).toBe(fault);
      expect(observe).toHaveBeenCalledTimes(2);
      expect(observe.mock.calls[1]).toEqual([prefix, token]);
      snapshot(mesh.observePrefix(prefix, saved.witness));
      expect(observe).toHaveBeenCalledTimes(3);
      expect(observe.mock.lastCall).toEqual([prefix, undefined]);
      expect(scan).not.toHaveBeenCalled();
    } finally {
      mesh.closeState();
      fs.rmSync(mesh.root, { recursive: true, force: true });
    }
  });
});
