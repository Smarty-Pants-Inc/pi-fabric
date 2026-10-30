import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { encodeMeshStateMovedMarker } from "../src/mesh/backend-fence.js";
import { MeshShadowNotReconciledError, ShadowStateBackend, STATE_BACKEND_FACTORIES,
  type DeepReadonly, type MeshBatchOperation, type MeshStateEntry, type PrefixObservation,
  type PrefixWitness, type StateBackend } from "../src/mesh/state-backend.js";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";

// Keep reason: data safety and the FILE/mirror/backend/facade witness trust boundaries (#17515).
// These are executable acceptance tests for the proposed §2 seam, NOT skipped compatibility tests.
// They intentionally require the pending FILE + Shadow + MeshStore observation implementation.
// No test claims remote visibility, client-drain effects, CPU savings, or arbitrary raw-writer proof.
const PREFIX = "residency/deliveries/";
const CONFIG_PREFIX = "config/";
const writer: MeshIdentity = { id: "writer-file", name: "file writer", kind: "agent" };
const peer: MeshIdentity = { id: "writer-peer", name: "peer writer", kind: "actor" };
const roots: string[] = [];
const stores: MeshStore[] = [];
type Snapshot = Extract<PrefixObservation, { kind: "snapshot" }>;

const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-shadow-prefix-"));
  roots.push(root);
  return root;
};

const open = (root: string, kind: "file" | "shadow", options: MeshStoreOptions = {}): MeshStore => {
  // The existing factory accepts CreateStateBackendOptions; the facade forwards this test-only
  // extra option structurally. Disable the pre-existing verifier, not a new observation timer.
  const configured = { ...options, stateBackend: kind, shadowVerifyMs: 0 };
  const store = new MeshStore(root, 64 * 1024, 7, configured);
  stores.push(store);
  expect(store.stateBackend).toBe(kind); // A native-SQLite fallback must not silently pass as Shadow.
  return store;
};

const shadowOf = (store: MeshStore): ShadowStateBackend => {
  const backend = store.stateBackendHandle;
  if (!(backend instanceof ShadowStateBackend)) throw new Error(`expected Shadow, got ${backend.kind}`);
  return backend;
};

const snapshot = (observed: PrefixObservation): Snapshot => {
  expect(observed.kind).toBe("snapshot");
  if (observed.kind !== "snapshot") throw new Error(`expected snapshot, got ${observed.kind}`);
  return observed;
};

const unchanged = (observed: PrefixObservation): PrefixWitness => {
  expect(observed.kind).toBe("unchanged");
  if (observed.kind !== "unchanged") throw new Error(`expected unchanged, got ${observed.kind}`);
  expect("entries" in observed).toBe(false);
  return observed.witness;
};

const keyFor = (index: number): string =>
  `${PREFIX}${index < 360 ? "foreign" : "own"}/delivery-${String(index).padStart(3, "0")}`;

const payloadFor = (index: number) => ({
  completionId: `completion-${index}`,
  root: index < 360 ? "/fixture/foreign" : "/fixture/own",
  session: `fixture-session-${index}`,
  from: { id: `sender-${index}`, name: "sender", kind: "actor" },
  principal: { id: "principal", claims: [{ scope: ["deliver", "retain"] }] },
  data: { labels: ["before", { nested: [index, null, true] }] },
});

const fixture = async (count = 3) => {
  const root = tempRoot();
  const fileWriter = open(root, "file");
  const ops: MeshBatchOperation[] = Array.from({ length: count }, (_, index) => ({
    kind: "put", key: keyFor(index), value: payloadFor(index), identity: index % 2 ? peer : writer,
  }));
  ops.push(...Array.from({ length: count === 387 ? 181 : 2 }, (_, index): MeshBatchOperation => ({
    kind: "put", key: `${CONFIG_PREFIX}recipient-${index}`, value: { enabled: false, index },
  })));
  await fileWriter.writeBatch({ identity: writer, ops });
  const expected = fileWriter.listAll(PREFIX, { fresh: true });
  const store = open(root, "shadow", { readCacheMs: 60_000, backgroundReadCacheMs: 60_000 });
  return { root, fileWriter, store, backend: shadowOf(store), expected };
};

const graphOf = (root: unknown): Set<object> => {
  const graph = new Set<object>();
  const visit = (value: unknown): void => {
    if (value === null || typeof value !== "object" || graph.has(value)) return;
    graph.add(value);
    for (const child of Object.values(value)) visit(child);
  };
  visit(root);
  return graph;
};

const isDeliveryEntry = (value: unknown): boolean => value !== null && typeof value === "object"
  && "key" in value && typeof value.key === "string" && value.key.startsWith(PREFIX);
const includesSelection = (value: unknown): boolean => {
  if (isDeliveryEntry(value)) return true;
  if (Array.isArray(value)) return value.some(isDeliveryEntry);
  if (value === null || typeof value !== "object" || !("entries" in value)) return false;
  const entries = value.entries;
  return entries !== null && typeof entries === "object" && Object.values(entries).some(isDeliveryEntry);
};

// Instrument just the synchronous observation, never assertions/setup. These count selection
// materialization mechanisms, not FILE bytes hashed/read, all possible traversals, or CPU time.
const selectionWork = (run: () => PrefixObservation, saved: readonly DeepReadonly<MeshStateEntry>[]) => {
  const graph = graphOf(saved);
  const parse = vi.spyOn(JSON, "parse");
  const stringify = vi.spyOn(JSON, "stringify");
  const clone = vi.spyOn(globalThis, "structuredClone");
  const sort = vi.spyOn(Array.prototype, "sort");
  const values = vi.spyOn(Object, "values");
  const freeze = vi.spyOn(Object, "freeze");
  try {
    const observed = run();
    // Save raw records before classification: includesSelection itself enumerates objects.
    const parsed = [...parse.mock.results];
    const serialized = [...stringify.mock.calls];
    const cloned = [...clone.mock.calls];
    const sorted = [...sort.mock.contexts];
    const enumerated = [...values.mock.results];
    const frozen = [...freeze.mock.calls];
    return { observed, counts: {
      parsedSelections: parsed.filter(result => result.type === "return" && includesSelection(result.value)).length,
      serializedSelections: serialized.filter(([value]) => includesSelection(value)).length,
      clonedSelections: cloned.filter(([value]) => includesSelection(value)).length,
      sortedSelections: sorted.filter(includesSelection).length,
      enumeratedSelections: enumerated.filter(result => result.type === "return" && includesSelection(result.value)).length,
      frozenSelectionNodes: frozen.filter(([value]) => (value !== null && typeof value === "object" && graph.has(value)) || includesSelection(value)).length,
    } };
  } finally {
    parse.mockRestore();
    stringify.mockRestore();
    clone.mockRestore();
    sort.mockRestore();
    values.mockRestore();
    freeze.mockRestore();
  }
};

const noSelectionWork = (counts: ReturnType<typeof selectionWork>["counts"]): void => {
  expect(counts).toEqual({ parsedSelections: 0, serializedSelections: 0, clonedSelections: 0,
    sortedSelections: 0, enumeratedSelections: 0, frozenSelectionNodes: 0 });
};

const forbidMirrorUse = (backend: ShadowStateBackend): void => {
  const fail = (): never => { throw new Error("prefix observation consulted or drove the non-authoritative mirror"); };
  vi.spyOn(backend.shadow, "get").mockImplementation(fail);
  vi.spyOn(backend.shadow, "list").mockImplementation(fail);
  vi.spyOn(backend.shadow, "listAll").mockImplementation(fail);
  vi.spyOn(backend.shadow, "listAllShared").mockImplementation(fail);
  vi.spyOn(backend.shadow, "stateToken").mockImplementation(fail);
  vi.spyOn(backend.shadow, "stateStamp").mockImplementation(fail);
  vi.spyOn(backend.shadow, "cachedStateStamp").mockImplementation(fail);
  const mirror: StateBackend = backend.shadow;
  if (typeof mirror.observePrefix === "function") vi.spyOn(mirror, "observePrefix").mockImplementation(fail);
  vi.spyOn(backend.shadow, "writeBatch").mockImplementation(fail);
  vi.spyOn(backend.shadow, "confirmWritable").mockImplementation(fail);
  vi.spyOn(backend, "verify").mockImplementation(fail);
  vi.spyOn(backend, "repair").mockImplementation(fail);
};

beforeEach(() => { vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", ""); });
afterEach(async () => {
  // Restore guards before joining mirrors; all deferred test mirrors are released in finally.
  vi.restoreAllMocks();
  for (const store of stores) {
    const backend = store.stateBackendHandle;
    if (backend instanceof ShadowStateBackend) await backend.flush();
  }
  for (const store of stores.splice(0).reverse()) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
});

describe("Shadow prefix observation: FILE authority", () => {
  it.each(["absent", "present"] as const)("keeps FILE authoritative with the optional mirror observer %s", async mode => {
    const { store, backend, expected } = await fixture();
    const mirror: StateBackend = backend.shadow;
    const original = Object.getOwnPropertyDescriptor(mirror, "observePrefix");
    Object.defineProperty(mirror, "observePrefix", { configurable: true, writable: true,
      value: mode === "present" ? (): never => { throw new Error("unguarded optional mirror observation"); } : undefined });
    try {
      if (mode === "absent") expect(mirror.observePrefix).toBeUndefined();
      else expect(typeof mirror.observePrefix).toBe("function");
      forbidMirrorUse(backend);
      if (mode === "present") {
        const observeMirror = vi.mocked(mirror.observePrefix!);
        expect(() => observeMirror(PREFIX)).toThrow("prefix observation consulted or drove the non-authoritative mirror");
        observeMirror.mockClear();
      }
      const first = snapshot(store.observePrefix(PREFIX));
      expect(first.entries).toEqual(expected);
      unchanged(store.observePrefix(PREFIX, first.witness));
      if (mode === "present") expect(mirror.observePrefix).not.toHaveBeenCalled();
    } finally {
      // Restore the spy before removing the test-only method; afterEach must not retain it.
      vi.restoreAllMocks();
      if (original) Object.defineProperty(mirror, "observePrefix", original);
      else delete mirror.observePrefix;
    }
  });

  it("cold-selects all 387 full own/foreign rows in localeCompare order before any mirror reconcile", async () => {
    const { store, backend, expected } = await fixture(387);
    expect(backend.shadow.listAll("")).toEqual([]);
    expect(backend.diagnostics().shadowApplied).toBe(0);
    forbidMirrorUse(backend);
    const first = snapshot(store.observePrefix(PREFIX));
    expect(first.entries).toEqual(expected);
    expect(first.entries).toHaveLength(387); // maxReadEvents is deliberately only 7.
    expect(first.entries.filter(entry => entry.key.startsWith(`${PREFIX}foreign/`))).toHaveLength(360);
    expect(first.entries.filter(entry => entry.key.startsWith(`${PREFIX}own/`))).toHaveLength(27);
    expect(first.entries.map(entry => entry.key)).toEqual(expected.map(entry => entry.key).sort((a, b) => a.localeCompare(b)));
    for (const entry of first.entries) {
      expect(entry.version).toBeGreaterThan(0);
      expect(entry.updatedAt).toBeGreaterThan(0);
      expect(entry.updatedBy).toEqual(expected.find(candidate => candidate.key === entry.key)!.updatedBy);
    }
    expect(backend.diagnostics().shadowApplied).toBe(0);
  });

  it("deep-freezes the owned selection and escaping nested identities/payloads without freezing listAll clones", async () => {
    const { store, fileWriter } = await fixture();
    const first = snapshot(store.observePrefix(PREFIX));
    for (const node of graphOf(first.entries)) expect(Object.isFrozen(node)).toBe(true);
    const before = JSON.stringify(first.entries);
    const escaped = first.entries[0]!.value as ReturnType<typeof payloadFor>;
    expect(() => { escaped.data.labels.push("poison"); }).toThrow(TypeError);
    expect(() => { escaped.from.id = "poison"; }).toThrow(TypeError);
    expect(() => { escaped.principal.claims[0]!.scope[0] = "poison"; }).toThrow(TypeError);
    expect(() => { (first.entries[0]!.updatedBy as MeshIdentity).name = "poison"; }).toThrow(TypeError);
    expect(() => { (first.entries as unknown as MeshStateEntry[]).push({} as MeshStateEntry); }).toThrow(TypeError);
    const mutable = fileWriter.listAll(PREFIX);
    expect(Object.isFrozen(mutable[0]!.updatedBy)).toBe(false);
    (mutable[0]!.value as ReturnType<typeof payloadFor>).data.labels.push("allowed local mutation");
    unchanged(store.observePrefix(PREFIX, first.witness));
    expect(JSON.stringify(first.entries)).toBe(before);
    expect(fileWriter.listAll(PREFIX)).not.toEqual(mutable);
  });

  it("takes three proven stable hits without selection materialization, mirror work, writes or new timers", async () => {
    const { store, backend } = await fixture(387);
    const first = snapshot(store.observePrefix(PREFIX));
    forbidMirrorUse(backend);
    const fail = (): never => { throw new Error("read-only observation attempted a writer or timer"); };
    vi.spyOn(backend.file, "put").mockImplementation(fail);
    vi.spyOn(backend.file, "delete").mockImplementation(fail);
    vi.spyOn(backend.file, "writeBatch").mockImplementation(fail);
    vi.spyOn(backend.file, "confirmWritable").mockImplementation(fail);
    let previous = first.witness;
    for (let index = 0; index < 3; index += 1) {
      const timeout = vi.spyOn(globalThis, "setTimeout").mockImplementation(fail);
      const interval = vi.spyOn(globalThis, "setInterval").mockImplementation(fail);
      let measured: ReturnType<typeof selectionWork>;
      try { measured = selectionWork(() => store.observePrefix(PREFIX, previous), first.entries); }
      finally { timeout.mockRestore(); interval.mockRestore(); }
      previous = unchanged(measured.observed);
      noSelectionWork(measured.counts);
    }
  });

  it("takes three proven unrelated-commit hits, then snapshots a FILE prefix change despite a warm TTL", async () => {
    const { store, backend, fileWriter } = await fixture(387);
    const first = snapshot(store.observePrefix(PREFIX));
    forbidMirrorUse(backend);
    let previous = first.witness;
    for (let index = 0; index < 3; index += 1) {
      await fileWriter.put({ key: `${CONFIG_PREFIX}recipient-${index}`, value: { enabled: true, index }, identity: peer });
      const measured = selectionWork(() => store.observePrefix(PREFIX, previous), first.entries);
      previous = unchanged(measured.observed);
      noSelectionWork(measured.counts);
    }
    await fileWriter.put({ key: keyFor(0), value: { changed: true }, identity: peer });
    const changed = snapshot(store.observePrefix(PREFIX, previous));
    expect(changed.entries).toEqual(fileWriter.listAll(PREFIX, { fresh: true }));
    expect(changed.entries[0]!.value).toEqual({ changed: true });
    expect(first.entries[0]!.value).toEqual(payloadFor(0));
  });

  it("does not mistake key/version-only or equal-value rewrites for an unchanged full selection", async () => {
    const { store, fileWriter } = await fixture(1);
    const first = snapshot(store.observePrefix(PREFIX));
    await fileWriter.put({ key: keyFor(0), value: payloadFor(0), identity: peer });
    const second = snapshot(store.observePrefix(PREFIX, first.witness));
    expect(second.entries[0]!.value).toEqual(first.entries[0]!.value);
    expect(second.entries[0]!.version).toBeGreaterThan(first.entries[0]!.version);
    expect(second.entries[0]!.updatedBy).toEqual(peer);
    await fileWriter.delete({ key: keyFor(0) });
    const empty = snapshot(store.observePrefix(PREFIX, second.witness));
    expect(empty.entries).toEqual([]);
    await fileWriter.put({ key: keyFor(0), value: payloadFor(0), identity: writer });
    const recreated = snapshot(store.observePrefix(PREFIX, empty.witness));
    expect(recreated.entries[0]!.version).toBeGreaterThan(second.entries[0]!.version);
  });

  it("does not turn an unreconciled empty mirror into authority for a negative selection", async () => {
    const { store, backend, fileWriter } = await fixture(0);
    const empty = snapshot(store.observePrefix(PREFIX));
    expect(empty.entries).toEqual([]);
    expect(backend.shadow.listAll("")).toEqual([]);
    await fileWriter.put({ key: keyFor(0), value: payloadFor(0), identity: writer });
    forbidMirrorUse(backend);
    const arrived = snapshot(store.observePrefix(PREFIX, empty.witness));
    expect(arrived.entries.map(entry => entry.key)).toEqual([keyFor(0)]);
    expect(backend.diagnostics().shadowApplied).toBe(0);
  });

  it("sees FILE arrivals while a mirror is pending and after that mirror fails", async () => {
    const { store, backend } = await fixture(0);
    const empty = snapshot(store.observePrefix(PREFIX));
    let failMirror!: (error: Error) => void;
    const pending = new Promise<never>((_resolve, reject) => { failMirror = reject; });
    const mirror = vi.spyOn(backend.shadow, "writeBatch").mockImplementationOnce(() => pending);
    try {
      await store.put({ key: keyFor(0), value: payloadFor(0), identity: writer });
      expect(mirror).toHaveBeenCalledOnce();
      const whilePending = snapshot(store.observePrefix(PREFIX, empty.witness));
      expect(whilePending.entries.map(entry => entry.key)).toEqual([keyFor(0)]);
      failMirror(new Error("injected mirror busy"));
      await backend.flush();
      expect(backend.diagnostics().shadowFailures).toBe(1);
      expect(backend.shadow.listAll("")).toEqual([]);
      unchanged(store.observePrefix(PREFIX, whilePending.witness));
    } finally {
      failMirror(new Error("release test mirror"));
      await backend.flush();
    }
  });

  it("propagates a committed FILE write into observation even when the mirror is permanently unavailable", async () => {
    const root = tempRoot();
    const fileWriter = open(root, "file");
    await fileWriter.put({ key: `${CONFIG_PREFIX}seed`, value: { enabled: false }, identity: writer });
    fs.writeFileSync(path.join(root, "state-shadow"), "not a directory");
    const store = open(root, "shadow");
    const backend = shadowOf(store);
    const empty = snapshot(store.observePrefix(PREFIX));
    expect(empty.entries).toEqual([]);
    await store.put({ key: keyFor(0), value: payloadFor(0), identity: writer });
    await backend.flush();
    expect(backend.diagnostics().shadowFailures).toBeGreaterThan(0);
    const full = snapshot(store.observePrefix(PREFIX, empty.witness));
    expect(full.entries.map(entry => entry.key)).toEqual([keyFor(0)]);
    unchanged(store.observePrefix(PREFIX, full.witness));
  });

  it("does not use a successful mirror to miss later FILE-only arrivals or retain a FILE-deleted row", async () => {
    const { store, backend, fileWriter } = await fixture(0);
    await store.put({ key: keyFor(0), value: payloadFor(0), identity: writer });
    await backend.flush();
    expect(backend.diagnostics().shadowApplied).toBeGreaterThan(0);
    expect(backend.shadow.get(keyFor(0))).toBeDefined();
    const first = snapshot(store.observePrefix(PREFIX));
    await fileWriter.delete({ key: keyFor(0) });
    await fileWriter.put({ key: keyFor(1), value: payloadFor(1), identity: peer });
    expect(backend.shadow.get(keyFor(0))).toBeDefined();
    expect(backend.shadow.get(keyFor(1))).toBeUndefined();
    forbidMirrorUse(backend);
    const changed = snapshot(store.observePrefix(PREFIX, first.witness));
    expect(changed.entries).toEqual(fileWriter.listAll(PREFIX, { fresh: true }));
    expect(changed.entries.map(entry => entry.key)).toEqual([keyFor(1)]);
  });

  it("allows a FILE hit despite mirror-only divergence; repair cannot certify or invalidate FILE equality", async () => {
    const { store, backend } = await fixture(1);
    await backend.repair();
    const first = snapshot(store.observePrefix(PREFIX));
    await backend.shadow.delete({ key: keyFor(0) });
    await backend.shadow.put({ key: `${PREFIX}mirror-only/phantom`, value: { phantom: true }, identity: peer });
    expect((await backend.verify()).map(difference => difference.key)).toEqual(expect.arrayContaining([keyFor(0), `${PREFIX}mirror-only/phantom`]));
    await backend.verify();
    expect(backend.diagnostics().divergences).toBeGreaterThan(0);
    forbidMirrorUse(backend);
    const previous = unchanged(store.observePrefix(PREFIX, first.witness));
    expect(first.entries.map(entry => entry.key)).toEqual([keyFor(0)]);
    vi.restoreAllMocks();
    await backend.repair();
    expect(await backend.verify()).toEqual([]);
    forbidMirrorUse(backend);
    unchanged(store.observePrefix(PREFIX, previous));
  });

  it("still observes a FILE commit whose afterCommit callback rejects, without consulting mirror success", async () => {
    const { store, backend } = await fixture(0);
    const empty = snapshot(store.observePrefix(PREFIX));
    await expect(store.writeBatch({ identity: writer,
      ops: [{ kind: "put", key: keyFor(0), value: payloadFor(0) }],
      afterCommit: () => { throw new Error("committed callback failed"); },
    })).rejects.toThrow("committed callback failed");
    await backend.flush();
    const changed = snapshot(store.observePrefix(PREFIX, empty.witness));
    expect(changed.entries.map(entry => entry.key)).toEqual([keyFor(0)]);
  });
});

describe("Shadow prefix observation: optional proof and canonical errors", () => {
  it("returns FILE UNKNOWN without stamping fallback entries, and the previous Shadow witness restarts cold", async () => {
    const { store, backend, expected } = await fixture();
    const first = snapshot(backend.observePrefix(PREFIX));
    const observeFile = vi.spyOn(backend.file, "observePrefix");
    observeFile.mockReturnValueOnce({ kind: "unknown", reason: "injected unproven canonical binding" });
    const unproven = backend.observePrefix(PREFIX, first.witness);
    expect(unproven).toEqual({ kind: "unknown", reason: "injected unproven canonical binding" });
    expect("witness" in unproven).toBe(false);
    expect("entries" in unproven).toBe(false);
    expect(store.listAll(PREFIX)).toEqual(expected); // Today's fallback remains usable and mutable.
    snapshot(backend.observePrefix(PREFIX, first.witness));
    expect(observeFile.mock.calls.at(-1)).toEqual([PREFIX, undefined]);
  });

  it("does not publish an unassociated inner unchanged response as a new negative witness", async () => {
    const { backend } = await fixture();
    const observeFile = vi.spyOn(backend.file, "observePrefix").mockReturnValueOnce({ kind: "unchanged", witness: {} });
    const observed = backend.observePrefix(PREFIX, {});
    expect(observed.kind).toBe("unknown");
    expect("witness" in observed).toBe(false);
    expect("entries" in observed).toBe(false);
    expect(observeFile.mock.calls[0]).toEqual([PREFIX, undefined]);
  });

  it("propagates the exact canonical error once, does not retry via listAll, and publishes no new binding", async () => {
    const { backend } = await fixture();
    const first = snapshot(backend.observePrefix(PREFIX));
    const failed = new Error("injected canonical FILE read failure");
    const observeFile = vi.spyOn(backend.file, "observePrefix").mockImplementationOnce(() => { throw failed; });
    const fallback = vi.spyOn(backend.file, "listAll");
    let caught: unknown;
    try { backend.observePrefix(PREFIX, first.witness); } catch (error) { caught = error; }
    expect(caught).toBe(failed);
    expect(observeFile).toHaveBeenCalledOnce();
    expect(fallback).not.toHaveBeenCalled();
    snapshot(backend.observePrefix(PREFIX, first.witness));
    expect(observeFile.mock.calls.at(-1)).toEqual([PREFIX, undefined]);
  });

  it("does not certify tolerant malformed canonical bytes as an empty snapshot or a reusable hit", async () => {
    const { store, root } = await fixture(1);
    const first = snapshot(store.observePrefix(PREFIX));
    fs.writeFileSync(path.join(root, "state.json"), '{"format":1,"entries":{"broken":');
    const unproven = store.observePrefix(PREFIX, first.witness);
    expect(unproven.kind).toBe("unknown");
    expect("witness" in unproven).toBe(false);
    expect("entries" in unproven).toBe(false);
    expect(store.listAll(PREFIX, { fresh: true })).toEqual([]); // Legacy tolerant scan, NOT certified absence.
    expect(store.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
  });

  it("preserves the canonical moved-marker refusal rather than calling it UNKNOWN or empty", async () => {
    const { store, root } = await fixture(1);
    const first = snapshot(store.observePrefix(PREFIX));
    fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateMovedMarker(3));
    expect(() => store.listAll(PREFIX, { fresh: true })).toThrow(/was moved to state\.db/);
    expect(() => store.observePrefix(PREFIX, first.witness)).toThrow(/was moved to state\.db/);
  });

  it.each(["atomic replacement", "in-place replacement"] as const)(
    "does not reuse a copied UUID/version/timestamp across %s with changed payload and writer", async mode => {
      const { store, root, backend, fileWriter } = await fixture(1);
      const first = snapshot(store.observePrefix(PREFIX));
      const statePath = path.join(root, "state.json");
      const originalStat = fs.statSync(statePath);
      const raw = JSON.parse(fs.readFileSync(statePath, "utf8")) as {
        readGeneration?: string; entries: Record<string, MeshStateEntry>;
      };
      const generation = raw.readGeneration;
      const entry = raw.entries[keyFor(0)]!;
      (entry.value as ReturnType<typeof payloadFor>).data.labels[0] = "after!";
      entry.updatedBy = peer;
      const changedText = JSON.stringify(raw);
      if (mode === "atomic replacement") {
        const replacement = path.join(root, "owned-copy-marker.tmp");
        fs.writeFileSync(replacement, changedText);
        fs.renameSync(replacement, statePath);
      } else fs.writeFileSync(statePath, changedText);
      fs.utimesSync(statePath, originalStat.atime, originalStat.mtime);
      expect(fs.statSync(statePath).size).toBe(originalStat.size);
      expect(raw.readGeneration).toBe(generation);
      forbidMirrorUse(backend);
      const changed = snapshot(store.observePrefix(PREFIX, first.witness));
      expect(changed.entries).toEqual(fileWriter.listAll(PREFIX, { fresh: true }));
      expect(changed.entries[0]!.version).toBe(first.entries[0]!.version);
      expect(changed.entries[0]!.updatedAt).toBe(first.entries[0]!.updatedAt);
      expect(changed.entries[0]!.updatedBy).toEqual(peer);
      expect((changed.entries[0]!.value as ReturnType<typeof payloadFor>).data.labels[0]).toBe("after!");
      // No assertion that later mixed raw/protocol writes have a supported incremental proof.
    },
  );

  it("permits a conservative snapshot/UNKNOWN for a legacy unmarked file, never an old negative hit", async () => {
    const { store, root, fileWriter } = await fixture(0);
    const empty = snapshot(store.observePrefix(PREFIX));
    const statePath = path.join(root, "state.json");
    const raw = JSON.parse(fs.readFileSync(statePath, "utf8")) as {
      readGeneration?: string; readJournalHash?: string; entries: Record<string, MeshStateEntry>;
    };
    delete raw.readGeneration;
    delete raw.readJournalHash;
    raw.entries[keyFor(0)] = { key: keyFor(0), value: payloadFor(0), version: 99, updatedAt: 1234, updatedBy: peer };
    fs.writeFileSync(statePath, JSON.stringify(raw));
    const observed = store.observePrefix(PREFIX, empty.witness);
    expect(observed.kind).not.toBe("unchanged");
    if (observed.kind === "snapshot") expect(observed.entries).toEqual(fileWriter.listAll(PREFIX, { fresh: true }));
    else expect(store.listAll(PREFIX, { fresh: true }).map(entry => entry.key)).toEqual([keyFor(0)]);
  });

  it("never treats failed initial reconciliation as evidence against FILE rows", async () => {
    const { store, backend, expected } = await fixture(3);
    vi.spyOn(backend.shadow, "writeBatch").mockRejectedValue(new Error("injected persistent mirror failure"));
    await expect(backend.verify()).rejects.toBeInstanceOf(MeshShadowNotReconciledError);
    expect(backend.diagnostics().shadowFailures).toBeGreaterThan(0);
    const first = snapshot(store.observePrefix(PREFIX));
    expect(first.entries).toEqual(expected);
    unchanged(store.observePrefix(PREFIX, first.witness));
  });
});

describe("Shadow prefix observation: exact backend and facade lifetimes", () => {
  it("cold-snapshots forged, FILE, other-prefix, cross-Shadow and foreign-root witnesses", async () => {
    const { store, backend, root, expected } = await fixture();
    const secondShadow = shadowOf(open(root, "shadow"));
    const foreign = await fixture(1);
    const legitimate = snapshot(backend.observePrefix(PREFIX));
    const candidates: { label: string; issue: () => PrefixWitness }[] = [
      { label: "forged", issue: () => ({}) },
      { label: "copied shell", issue: () => ({ ...legitimate.witness }) },
      { label: "prototype shell", issue: () => Object.create(legitimate.witness) as object },
      { label: "direct FILE", issue: () => snapshot(backend.file.observePrefix(PREFIX)).witness },
      { label: "same-root other Shadow", issue: () => snapshot(secondShadow.observePrefix(PREFIX)).witness },
      { label: "foreign root", issue: () => snapshot(foreign.backend.observePrefix(PREFIX)).witness },
      { label: "wrong prefix", issue: () => snapshot(backend.observePrefix(CONFIG_PREFIX)).witness },
      { label: "facade not backend", issue: () => snapshot(store.observePrefix(PREFIX)).witness },
    ];
    for (const { label, issue } of candidates) {
      // Issue immediately before use: a defective bare forwarder must not escape this test
      // merely because some other cold observation has already superseded the inner token.
      const observed = backend.observePrefix(PREFIX, issue());
      expect(observed.kind, label).toBe("snapshot");
      expect(snapshot(observed).entries, label).toEqual(expected);
    }
  });

  it("does not allow a raw backend witness to act as MeshStore facade authority", async () => {
    const { store, backend, expected } = await fixture();
    const raw = snapshot(backend.observePrefix(PREFIX));
    expect(snapshot(store.observePrefix(PREFIX, raw.witness)).entries).toEqual(expected);
  });

  it("binds each facade independently even when two MeshStores share the exact same prepared Shadow backend", async () => {
    const { store, backend, root, expected } = await fixture();
    vi.spyOn(STATE_BACKEND_FACTORIES.shadow, "create").mockReturnValue(backend);
    const secondFacade = open(root, "shadow");
    expect(secondFacade.stateBackendHandle).toBe(store.stateBackendHandle);
    const first = snapshot(store.observePrefix(PREFIX));
    const second = snapshot(secondFacade.observePrefix(PREFIX, first.witness));
    expect(second.entries).toEqual(expected); // A bare facade forwarder incorrectly hits here.
    const rebound = snapshot(store.observePrefix(PREFIX, second.witness));
    expect(rebound.entries).toEqual(expected);
    unchanged(store.observePrefix(PREFIX, rebound.witness));
  });

  it.each(["Shadow reset", "FILE reset"] as const)("invalidates witnesses on %s", async reset => {
    const { backend, expected } = await fixture();
    const first = snapshot(backend.observePrefix(PREFIX));
    if (reset === "Shadow reset") backend.dropCache();
    else backend.file.dropCache();
    expect(snapshot(backend.observePrefix(PREFIX, first.witness)).entries).toEqual(expected);
  });

  it("invalidates a Shadow witness evicted by its bounded 32-prefix registry", async () => {
    const { backend, expected } = await fixture();
    const first = snapshot(backend.observePrefix(PREFIX));
    // Supplement the real-store cases with known empty inner snapshots. This tests only the
    // wrapper's capacity, not whether FILE can prove every arbitrary namespace efficiently.
    const observeFile = vi.spyOn(backend.file, "observePrefix");
    for (let index = 0; index < 32; index += 1) {
      observeFile.mockReturnValueOnce({ kind: "snapshot", witness: Object.freeze({}), entries: Object.freeze([]) });
      snapshot(backend.observePrefix(`config/evict-${index}/`));
    }
    expect(snapshot(backend.observePrefix(PREFIX, first.witness)).entries).toEqual(expected);
    expect(observeFile.mock.calls.at(-1)).toEqual([PREFIX, undefined]);
  });

  it("cannot reuse a closed Shadow witness through its still-readable FILE member or a reopened Shadow", async () => {
    const { backend, root, expected } = await fixture();
    const first = snapshot(backend.observePrefix(PREFIX));
    backend.close();
    // Existing Shadow reads after close still delegate to FILE. The optional proof is retired.
    expect(backend.listAll(PREFIX, { fresh: true })).toEqual(expected);
    expect(backend.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
    expect(backend.observePrefix(PREFIX).kind).toBe("unknown");
    const reopened = shadowOf(open(root, "shadow"));
    expect(snapshot(reopened.observePrefix(PREFIX, first.witness)).entries).toEqual(expected);
  });

  it("never reuses a closed facade witness after reopening the same root", async () => {
    const { store, root, expected } = await fixture();
    const first = snapshot(store.observePrefix(PREFIX));
    store.closeState();
    // Capture only the call: an assertion failure must not be mistaken for an allowed lifecycle error.
    let closedResult: PrefixObservation | undefined;
    let closedError: unknown;
    try { closedResult = store.observePrefix(PREFIX, first.witness); } catch (error) { closedError = error; }
    if (closedError !== undefined) expect(closedError).toBeInstanceOf(Error);
    else expect(closedResult?.kind).toBe("unknown");
    const reopened = open(root, "shadow");
    expect(snapshot(reopened.observePrefix(PREFIX, first.witness)).entries).toEqual(expected);
  });
});
