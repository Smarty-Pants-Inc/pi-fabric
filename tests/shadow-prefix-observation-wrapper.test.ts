import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { createStateBackend, ShadowStateBackend, type StateBackend } from "../src/mesh/state-backend.js";
import { freezePrefixEntries, type PrefixEntries, type PrefixObservation,
  type PrefixWitness } from "../src/mesh/prefix-observation.js";
import type { MeshIdentity, MeshStateEntry } from "../src/mesh/store.js";

// Keep: trust boundary/data safety. REAL factory-selected Shadow, with ONLY its FILE optional-proof
// method supplied by a typed test seam until the FILE owner's implementation is integrated.
// This is NOT FILE canonical/physical/ctime/ABA proof, full Shadow integration, or CPU evidence.
const prefix = "residency/deliveries/";
const identity: MeshIdentity = { id: "wrapper-test", name: "wrapper-test", kind: "main" };
type Observe = NonNullable<StateBackend["observePrefix"]>;
const roots: string[] = [];
const backends: StateBackend[] = [];
const restores: Array<() => void> = [];
const row = (key: string): MeshStateEntry => ({ key, version: 1, updatedAt: 1,
  updatedBy: { ...identity }, value: { nested: [{ payload: [1, { complete: true }] }] } });
const token = (): PrefixWitness => Object.freeze({});
const snapshot = (observation: PrefixObservation) => {
  expect(observation.kind).toBe("snapshot");
  if (observation.kind !== "snapshot") throw new Error("expected complete snapshot");
  return observation;
};
const unchanged = (observation: PrefixObservation) => {
  expect(observation.kind).toBe("unchanged");
  if (observation.kind !== "unchanged") throw new Error("expected paired unchanged");
  return observation;
};
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-shadow-prefix-wrapper-"));
  roots.push(root);
  return root;
};
const open = (root = tempRoot()): ShadowStateBackend => {
  const context = { root, maxEventBytes: 64 * 1024, maxReadEvents: 1_000,
    lock: new MeshLock(root, {}, () => undefined) };
  // No factory mocks, availability override, fallback-as-coverage, or skip. A non-native runner FAILS.
  const backend = createStateBackend(context, { stateBackend: "shadow", shadowVerifyMs: 0 });
  backends.push(backend);
  expect(backend.kind).toBe("shadow");
  expect(backend).toBeInstanceOf(ShadowStateBackend);
  if (!(backend instanceof ShadowStateBackend)) throw new Error(`native Shadow required; got ${backend.kind}`);
  return backend;
};
const installFileSeam = (backend: ShadowStateBackend, observePrefix: Observe | undefined) => {
  const previous = Object.getOwnPropertyDescriptor(backend.file, "observePrefix");
  const seam: Pick<StateBackend, "observePrefix"> = observePrefix ? { observePrefix } : {};
  Object.defineProperty(backend.file, "observePrefix", {
    configurable: true, writable: true, value: seam.observePrefix,
  });
  restores.push(() => {
    if (previous) Object.defineProperty(backend.file, "observePrefix", previous);
    else Reflect.deleteProperty(backend.file, "observePrefix");
  });
};
const fileProof = (backend: ShadowStateBackend) => {
  // Intentionally UNBOUNDED inner registrations: inner eviction must not mask a missing Shadow cap.
  // The graph is prepared once per prefix. Hits advance the inner witness, without walking the graph.
  const current = new Map<string, { witness: PrefixWitness; entries: PrefixEntries }>();
  const sequences = new Map<string, PrefixEntries>();
  const observe = vi.fn<Observe>((requested, previous) => {
    const saved = current.get(requested);
    const witness = token();
    if (previous !== undefined && saved?.witness === previous) {
      current.set(requested, { witness, entries: saved.entries });
      return { kind: "unchanged", witness };
    }
    let entries = sequences.get(requested);
    if (!entries) {
      entries = freezePrefixEntries([row(requested + "foreign/a"), row(requested + "own/a")]);
      sequences.set(requested, entries);
    }
    current.set(requested, { witness, entries });
    return { kind: "snapshot", witness, entries };
  });
  installFileSeam(backend, observe);
  return { observe, current, sequences };
};
const thrownBy = (operation: () => unknown): unknown => {
  try { operation(); } catch (error) { return error; }
  return undefined;
};

afterEach(async () => {
  // Join only our mirrors, and restore every instance-local test seam and spy even on assertion failure.
  try {
    for (const backend of backends.slice().reverse()) backend.close();
    for (const backend of backends) if (backend instanceof ShadowStateBackend) await backend.flush();
  } finally {
    backends.length = 0;
    for (const restore of restores.splice(0).reverse()) restore();
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("native Shadow prefix wrapper with an explicit FILE optional-proof seam", () => {
  it("wraps, never exposes, the inner witness and borrows the same complete frozen sequence", () => {
    const backend = open(), proof = fileProof(backend);
    const first = snapshot(backend.observePrefix(prefix));
    const sequence = proof.sequences.get(prefix)!;
    expect(first.entries).toBe(sequence);
    expect(first.entries.map(entry => entry.key)).toEqual([prefix + "foreign/a", prefix + "own/a"]);
    expect(first.witness).not.toBe(proof.current.get(prefix)!.witness);
    expect(Object.isFrozen(first.witness)).toBe(true);
    const bytes = JSON.stringify(sequence);
    const value = sequence[0]!.value as { nested: Array<{ payload: unknown[] }> };
    expect(() => value.nested[0]!.payload.push("mutated")).toThrow(TypeError);
    expect(() => { (sequence[0]!.updatedBy as { id: string }).id = "mutated"; }).toThrow(TypeError);

    const nodes = new Set<unknown>();
    const collect = (node: unknown): void => {
      if (node === null || typeof node !== "object" || nodes.has(node)) return;
      nodes.add(node);
      for (const child of Object.values(node)) collect(child);
    };
    collect(sequence);
    const parse = vi.spyOn(JSON, "parse"), stringify = vi.spyOn(JSON, "stringify");
    const clone = vi.spyOn(globalThis, "structuredClone"), sort = vi.spyOn(Array.prototype, "sort");
    const freeze = vi.spyOn(Object, "freeze");
    const enumeration = [vi.spyOn(Object, "values"), vi.spyOn(Object, "entries"), vi.spyOn(Object, "keys"),
      vi.spyOn(Object, "getOwnPropertyDescriptors"), vi.spyOn(Object, "getOwnPropertyNames"), vi.spyOn(Reflect, "ownKeys")];
    const pairs: Array<{ outer: PrefixWitness; inner: PrefixWitness; forwarded: PrefixWitness | undefined }> = [];
    let previous = first.witness;
    try {
      for (let index = 0; index < 3; index++) {
        const inner = proof.current.get(prefix)!.witness;
        const result = backend.observePrefix(prefix, previous);
        if (result.kind !== "unchanged") throw new Error("valid paired hit must remain unchanged");
        pairs.push({ outer: result.witness, inner, forwarded: proof.observe.mock.lastCall?.[1] });
        previous = result.witness;
      }
      // Capture before matchers inspect objects. Empty witness/result shells are allowed, entry graphs are not.
      const work = { parse: parse.mock.calls.length, stringify: stringify.mock.calls.length,
        clone: clone.mock.calls.length, sort: sort.mock.calls.length,
        graphFreeze: freeze.mock.calls.filter(([node]) => nodes.has(node)).length,
        graphEnumeration: enumeration.reduce((count, spy) => count + spy.mock.calls.filter(([node]) => nodes.has(node)).length, 0) };
      parse.mockRestore(); stringify.mockRestore(); clone.mockRestore(); sort.mockRestore(); freeze.mockRestore();
      for (const spy of enumeration) spy.mockRestore();
      expect(work).toEqual({ parse: 0, stringify: 0, clone: 0, sort: 0, graphFreeze: 0, graphEnumeration: 0 });
    } finally {
      parse.mockRestore(); stringify.mockRestore(); clone.mockRestore(); sort.mockRestore(); freeze.mockRestore();
      for (const spy of enumeration) spy.mockRestore();
    }
    expect(pairs).toHaveLength(3);
    expect(pairs.every(pair => pair.forwarded === pair.inner && pair.outer !== pair.inner)).toBe(true);
    expect(new Set([first.witness, ...pairs.map(pair => pair.outer)]).size).toBe(4);
    expect(first.entries).toBe(sequence);
    expect(JSON.stringify(sequence)).toBe(bytes);
  });

  it("rejects direct FILE, cross-Shadow, forged, wrong-prefix and superseded wrappers to one cold call", () => {
    const root = tempRoot(), a = open(root), b = open(root);
    const proof = fileProof(a), otherProof = fileProof(b);
    const direct = snapshot(proof.observe(prefix)).witness;
    const foreign = snapshot(b.observePrefix(prefix)).witness;
    const wrongPrefix = snapshot(a.observePrefix("other/")).witness;
    const stale = snapshot(a.observePrefix(prefix)).witness;
    snapshot(a.observePrefix(prefix)); // retires that prefix's earlier wrapper
    for (const previous of [direct, foreign, token(), wrongPrefix, stale]) {
      const count = proof.observe.mock.calls.length;
      snapshot(a.observePrefix(prefix, previous));
      expect(proof.observe).toHaveBeenCalledTimes(count + 1);
      expect(proof.observe.mock.lastCall).toEqual([prefix, undefined]);
    }
    expect(otherProof.observe).toHaveBeenCalledTimes(1);
  });

  it("retains 32 exact prefixes but evicts the oldest wrapper even when FILE still recognizes it", () => {
    const retained = open(), retainedProof = fileProof(retained);
    const atCapacity = snapshot(retained.observePrefix(prefix));
    for (let index = 0; index < 31; index++) snapshot(retained.observePrefix(`retained/${index}/`));
    const retainedInner = retainedProof.current.get(prefix)!.witness;
    unchanged(retained.observePrefix(prefix, atCapacity.witness));
    expect(retainedProof.observe.mock.lastCall).toEqual([prefix, retainedInner]);

    const backend = open(), proof = fileProof(backend);
    const saved = snapshot(backend.observePrefix(prefix));
    const inner = proof.current.get(prefix)!.witness;
    let last: Extract<PrefixObservation, { kind: "snapshot" }> = saved;
    for (let index = 0; index < 31; index++) last = snapshot(backend.observePrefix(`capacity/${index}/`));
    unchanged(backend.observePrefix("capacity/30/", last.witness)); // the 32nd binding survives
    expect(proof.observe.mock.lastCall?.[1]).toBeDefined();
    snapshot(backend.observePrefix("capacity/32/")); // the 33rd distinct prefix evicts the oldest
    expect(proof.current.get(prefix)!.witness).toBe(inner); // no inner eviction can explain a cold call
    snapshot(backend.observePrefix(prefix, saved.witness));
    expect(proof.observe.mock.lastCall).toEqual([prefix, undefined]);
  });

  it.each(["unknown", "error"] as const)("invalidates before the inner %s call, preserving one error and no fallback scan", outcome => {
    const backend = open(), proof = fileProof(backend);
    const saved = snapshot(backend.observePrefix(prefix)), inner = proof.current.get(prefix)!.witness;
    const fault = new Error("canonical FILE busy/corruption/retirement");
    const uncertain: PrefixObservation = { kind: "unknown", reason: "explicit-test-proof-uncertainty" };
    const fileScan = vi.spyOn(backend.file, "listAll"), sharedScan = vi.spyOn(backend.file, "listAllShared");
    const wrapperScan = vi.spyOn(backend, "listAll");
    let nested: PrefixObservation | undefined, throws = 0;
    proof.observe.mockImplementationOnce(requested => {
      // Re-entrant observation is an inspection probe: it must not unwrap the OLD registration
      // while the outer canonical call is in progress. This is NOT an automatic retry.
      proof.observe.mockReturnValueOnce(uncertain);
      nested = backend.observePrefix(requested, saved.witness);
      if (outcome === "error") { throws++; throw fault; }
      return uncertain;
    });
    if (outcome === "error") expect(thrownBy(() => backend.observePrefix(prefix, saved.witness))).toBe(fault);
    else expect(backend.observePrefix(prefix, saved.witness)).toEqual(uncertain);
    expect(nested).toEqual(uncertain);
    expect(throws).toBe(outcome === "error" ? 1 : 0);
    expect(proof.observe).toHaveBeenCalledTimes(3); // cold + outer + explicit re-entrant probe, no retries
    expect(proof.observe.mock.calls[1]).toEqual([prefix, inner]);
    expect(proof.observe.mock.calls[2]).toEqual([prefix, undefined]);
    snapshot(backend.observePrefix(prefix, saved.witness));
    expect(proof.observe).toHaveBeenCalledTimes(4);
    expect(proof.observe.mock.lastCall).toEqual([prefix, undefined]);
    expect(fileScan).not.toHaveBeenCalled();
    expect(sharedScan).not.toHaveBeenCalled();
    expect(wrapperScan).not.toHaveBeenCalled();
  });

  it("returns UNKNOWN for missing capability or unpaired inner unchanged, never inventing a sequence", () => {
    const backend = open();
    installFileSeam(backend, undefined);
    expect(backend.observePrefix(prefix).kind).toBe("unknown");
    const observe = vi.fn<Observe>().mockReturnValue({ kind: "unchanged", witness: token() });
    installFileSeam(backend, observe);
    expect(backend.observePrefix(prefix).kind).toBe("unknown");
    expect(observe).toHaveBeenCalledExactlyOnceWith(prefix, undefined);
    const proof = fileProof(backend);
    const cold = snapshot(backend.observePrefix(prefix));
    proof.observe.mockReturnValueOnce({ kind: "unknown", reason: "proof-lost" });
    expect(backend.observePrefix(prefix, cold.witness).kind).toBe("unknown");
    proof.observe.mockReturnValueOnce({ kind: "unchanged", witness: token() });
    expect(backend.observePrefix(prefix, cold.witness).kind).toBe("unknown");
    expect(proof.observe.mock.lastCall).toEqual([prefix, undefined]);
  });

  it("clears before dropCache and retires before close; reopening the same root starts cold", () => {
    const root = tempRoot(), backend = open(root), proof = fileProof(backend);
    const saved = snapshot(backend.observePrefix(prefix));
    let duringDrop: PrefixObservation | undefined;
    const drop = vi.spyOn(backend.file, "dropCache").mockImplementationOnce(() => {
      proof.observe.mockReturnValueOnce({ kind: "unknown", reason: "drop-inspection" });
      duringDrop = backend.observePrefix(prefix, saved.witness);
    });
    backend.dropCache();
    expect(drop).toHaveBeenCalledTimes(1);
    expect(duringDrop?.kind).toBe("unknown");
    expect(proof.observe.mock.lastCall).toEqual([prefix, undefined]);
    const fresh = snapshot(backend.observePrefix(prefix, saved.witness));
    expect(proof.observe.mock.lastCall).toEqual([prefix, undefined]);
    const close = backend.shadow.close.bind(backend.shadow);
    let duringClose: PrefixObservation | undefined;
    vi.spyOn(backend.shadow, "close").mockImplementationOnce(() => {
      duringClose = backend.observePrefix(prefix, fresh.witness);
      close();
    });
    const count = proof.observe.mock.calls.length;
    backend.close();
    expect(duringClose?.kind).toBe("unknown");
    expect(backend.observePrefix(prefix, fresh.witness).kind).toBe("unknown");
    expect(proof.observe).toHaveBeenCalledTimes(count);
    const reopened = open(root), newProof = fileProof(reopened);
    snapshot(reopened.observePrefix(prefix, fresh.witness));
    expect(newProof.observe).toHaveBeenCalledExactlyOnceWith(prefix, undefined);
  });

  it.each([false, true])("mirror failure (permanently disabled=%s) is neither proof nor an observation driver", async disabled => {
    const backend = open(), proof = fileProof(backend);
    const fault = Object.assign(new Error(disabled ? "test mirror unsupported" : "test mirror busy"),
      disabled ? { code: "FABRIC_MESH_STATE_UNSUPPORTED" } : { errcode: 5 });
    const failedMirror = vi.spyOn(backend.shadow, "writeBatch").mockRejectedValue(fault);
    // This is a REAL FILE write through native Shadow. It establishes mirror failure/disabled state,
    // NOT a physical proof for the deliberately independent optional-proof fixture below.
    await backend.put({ key: "mirror-fixture/a", value: "authoritative", identity });
    await backend.flush();
    expect(backend.file.get("mirror-fixture/a")?.value).toBe("authoritative");
    expect(failedMirror).toHaveBeenCalledTimes(1);
    expect(backend.diagnostics().shadowFailures).toBe(1);
    expect(backend.diagnostics().fallback).toBe(disabled ? fault.message : undefined);
    failedMirror.mockClear();
    const guards = [vi.spyOn(backend, "verify"), vi.spyOn(backend, "repair"),
      vi.spyOn(backend.shadow, "get"), vi.spyOn(backend.shadow, "list"), vi.spyOn(backend.shadow, "listAll"),
      vi.spyOn(backend.shadow, "listAllShared"), vi.spyOn(backend.shadow, "stateToken"),
      vi.spyOn(backend.shadow, "stateStamp"), vi.spyOn(backend.shadow, "confirmWritable"),
      vi.spyOn(backend.file, "listAll"), vi.spyOn(backend.file, "listAllShared"), vi.spyOn(backend.file, "stateStamp")];
    const first = snapshot(backend.observePrefix(prefix));
    unchanged(backend.observePrefix(prefix, first.witness));
    proof.observe.mockReturnValueOnce({ kind: "unknown", reason: "FILE-proof-unavailable" });
    expect(backend.observePrefix(prefix).kind).toBe("unknown");
    await backend.flush(); // any improperly enqueued observation work must settle before checking
    expect(failedMirror).not.toHaveBeenCalled();
    for (const guard of guards) expect(guard).not.toHaveBeenCalled();
    expect(backend.diagnostics().shadowFailures).toBe(1);
  });
});
