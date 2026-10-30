// Keep reason: data safety/trust boundary. A stale negative witness can strand durable deliveries; native
// reconnect/reset/gap/failure paths must not be certified by a fake JetStream implementation.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createConnection, createServer, type AddressInfo, type Socket } from "node:net";
import { afterEach, describe, expect, vi } from "vitest";
import {
  closeNatsStateBackend, MeshStateNatsRefusedError, NatsStateBackend, natsStreamName,
  type JetStreamPort, type NatsProjectionConsumer,
} from "../src/mesh/state-backend-nats.js";
import { connectJetStream } from "../src/mesh/state-backend-nats-client.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";
import type { MeshStateEntry } from "../src/mesh/state-file.js";
import { MeshStore, type PrefixEntries, type PrefixWitness } from "../src/mesh/store.js";
import { itFor, natsMonitorUrl, natsServerUrl, prepareNatsRoot, reportNatsSkip } from "./helpers/nats-state-server.js";
import { captureNatsConsumer, waitForNatsConsumerReady } from "./helpers/nats-consumer-ready.js";
import { parseJsAck } from "./helpers/nats-wire-faults.js";

const prefix = "residency/deliveries/";
const identity: MeshIdentity = { id: "prefix-writer", name: "prefix-writer", kind: "agent" };
const foreignWriter: MeshIdentity = { id: "foreign-writer", name: "foreign-writer", kind: "agent" };
const backends: NatsStateBackend[] = [];
const ports: JetStreamPort[] = [];
const roots: string[] = [];
const relays: ObservationRelay[] = [];
const stores: MeshStore[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const mesh of stores.splice(0).reverse()) mesh.closeState();
  for (const backend of backends.splice(0)) backend.close();
  for (const root of roots.splice(0)) closeNatsStateBackend(root);
  for (const port of ports.splice(0)) await port.close().catch(() => undefined);
  for (const relay of relays.splice(0)) await relay.stop();
  // No product delete/purge or filesystem cleanup is executed here. The private test-fleet owner reaps fixtures.
});
const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const bounded = async <T,>(promise: Promise<T>, label: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out`)), 10_000);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-prefix-observation-"));
  roots.push(root);
  const writer = await prepareNatsRoot(root);
  return { root, writer, stream: natsStreamName(root) };
};
const replica = async (root: string, server = natsServerUrl!, transform?: (port: JetStreamPort) => void) => {
  const port = await connectJetStream([server], { monitoringUrl: natsMonitorUrl! });
  ports.push(port);
  transform?.(port);
  const stream = natsStreamName(root);
  const captured = await captureNatsConsumer(server, stream, () => NatsStateBackend.open({ port, stream,
    subject: `${stream}.state`, snapshotSubject: `${stream}.snapshot`, maxEventBytes: 1 << 20, snapshotEvery: 0 }));
  backends.push(captured.value);
  await waitForNatsConsumerReady(server, stream, captured.consumer);
  return { backend: captured.value, port };
};
const snapshot = (backend: Pick<NatsStateBackend, "observePrefix">, selectionPrefix = prefix, previous?: PrefixWitness) => {
  const result = backend.observePrefix(selectionPrefix, previous);
  if (result.kind !== "snapshot") throw new Error(`expected snapshot, got ${result.kind}`);
  return result;
};
const waitForEntry = async (backend: NatsStateBackend, key: string, value: unknown) => {
  const changed = deferred<void>();
  const check = () => { if (backend.get(key)?.value === value) changed.resolve(); };
  const unsubscribe = backend.subscribeStateChanges(check);
  try { check(); await bounded(changed.promise, `projection of ${key}`); }
  finally { unsubscribe(); }
};
const seed = async (writer: NatsStateBackend) => {
  await writer.writeBatch({ identity, ops: [
    ...Array.from({ length: 387 }, (_, index) => ({ kind: "put" as const,
      key: `${prefix}${index < 360 ? "foreign" : "own"}/${String(index).padStart(3, "0")}`,
      value: { pending: true, from: { ...identity }, principal: { id: "principal", scopes: ["read"] },
        data: { index, nested: [{ retained: true }] } } })),
    ...Array.from({ length: 181 }, (_, index) => ({ kind: "put" as const, key: `residency/configs/${index}`, value: { enabled: true } })),
  ] });
};

const facade = (root: string, backend: NatsStateBackend) => {
  const mesh = new MeshStore(root, 1 << 20, 1_000, { stateBackend: "nats" });
  stores.push(mesh);
  expect(mesh.stateBackend).toBe("nats");
  expect(mesh.stateBackendHandle).toBe(backend); // actual prepared factory, not a factory mock
  return mesh;
};
// Only the contract's entry-selection pattern, NOT a residency/drain implementation or effect proof.
const selection = (mesh: MeshStore) => {
  let saved: { witness: PrefixWitness; entries: PrefixEntries } | undefined;
  return {
    get saved() { return saved; },
    read(): PrefixEntries {
      const observed = mesh.observePrefix(prefix, saved?.witness);
      switch (observed.kind) {
        case "snapshot": saved = { witness: observed.witness, entries: observed.entries }; return saved.entries;
        case "unchanged":
          if (!saved) throw new Error("unchanged without an associated complete sequence");
          saved = { witness: observed.witness, entries: saved.entries };
          return saved.entries;
        case "unknown": saved = undefined; return mesh.listAll(prefix);
      }
    },
  };
};
const publishState = async (root: string, port: JetStreamPort, record: object, id: string) => {
  const stream = natsStreamName(root), base = await port.lastSequence(stream);
  await port.publish(`${stream}.state`, Buffer.from(JSON.stringify({ format: 1, ...record })),
    { expectLastSequence: base, msgID: id });
};
// Instance-local capability-absence probe; the backend, projection, reads and errors remain native.
const removeObservation = (backend: NatsStateBackend) => {
  const descriptor = Object.getOwnPropertyDescriptor(backend, "observePrefix");
  Object.defineProperty(backend, "observePrefix", { configurable: true, writable: true, value: undefined });
  return () => {
    if (descriptor) Object.defineProperty(backend, "observePrefix", descriptor);
    else Reflect.deleteProperty(backend, "observePrefix");
  };
};
const thrownBy = (operation: () => unknown): unknown => {
  try { operation(); } catch (error) { return error; }
  return undefined;
};

/** Plain loopback transport fault relay; it owns only its sockets, not the parent's real server. Native ordered
 * reset tries to DELETE its old ephemeral consumer: record/censor that API request, never execute local deletion.
 * All other frames are byte-for-byte forwarded, except one explicitly selected inbound stream delivery. */
class ObservationRelay {
  readonly url: string;
  accepts = 0;
  dropped = 0;
  dropSequence: number | undefined;
  readonly consumerNames = new Set<string>();
  readonly recordedDeletes: string[] = [];
  readonly subjects: string[] = [];
  private readonly sockets = new Set<Socket>();
  private constructor(private readonly target: URL, private readonly listener: ReturnType<typeof createServer>) {
    this.url = `nats://127.0.0.1:${(listener.address() as AddressInfo).port}`;
  }
  static async open(): Promise<ObservationRelay> {
    let relay!: ObservationRelay;
    const listener = createServer(client => relay.accept(client));
    await new Promise<void>((resolve, reject) => { listener.once("error", reject); listener.listen(0, "127.0.0.1", resolve); });
    relay = new ObservationRelay(new URL(natsServerUrl!), listener);
    relays.push(relay);
    return relay;
  }
  private track(socket: Socket): Socket {
    this.sockets.add(socket);
    socket.on("error", () => undefined);
    socket.once("close", () => this.sockets.delete(socket));
    return socket;
  }
  private accept(client: Socket): void {
    this.accepts++;
    this.track(client);
    const upstream = this.track(createConnection({ host: this.target.hostname, port: Number(this.target.port) }));
    client.once("close", () => upstream.destroy());
    upstream.once("close", () => client.destroy());
    this.pipe(client, upstream, true);
    this.pipe(upstream, client, false);
  }
  private pipe(source: Socket, destination: Socket, outbound: boolean): void {
    let pending = Buffer.alloc(0);
    source.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      for (;;) {
        const end = pending.indexOf("\r\n");
        if (end < 0) return;
        const words = pending.subarray(0, end).toString().split(/ +/);
        const payload = ["PUB", "HPUB", "MSG", "HMSG"].includes(words[0]!);
        const size = payload ? Number(words.at(-1)) : 0;
        if (!Number.isSafeInteger(size) || size < 0 || size > 2 ** 22) throw new Error("invalid relay frame size");
        const length = end + 2 + (payload ? size + 2 : 0);
        if (pending.length < length) return;
        const frame = pending.subarray(0, length);
        pending = pending.subarray(length);
        const subject = words[1] ?? "";
        if (outbound && payload) {
          this.subjects.push(subject);
          if (/^\$JS\..*\.(?:DELETE|PURGE)(?:\.|$)/.test(subject)) { this.recordedDeletes.push(subject); continue; }
        }
        const reply = !outbound && words.length === (words[0] === "HMSG" ? 6 : 5) ? words[3] : undefined;
        const ack = reply ? parseJsAck(reply) : undefined;
        if (ack) {
          this.consumerNames.add(ack.consumer);
          if (ack.streamSequence === this.dropSequence) { this.dropSequence = undefined; this.dropped++; continue; }
        }
        destination.write(frame);
      }
    });
  }
  sever(): void { for (const socket of this.sockets) socket.destroy(); }
  async stop(): Promise<void> {
    this.sever();
    await new Promise<void>((resolve, reject) => this.listener.close(error => error ? reject(error) : resolve()));
  }
}

describe("NATS prefix observation — real JetStream", () => {
  reportNatsSkip(); // unconfigured generic runs visibly skip; REQUIRED=1 is enforced by the helper
  const it = itFor("nats");

  it("two MeshStores sharing a prepared native backend reject foreign facade witnesses and safely resnapshot after registry interference", async () => {
    const { root, writer } = await setup();
    await seed(writer);
    const a = facade(root, writer), b = facade(root, writer);
    const observe = vi.spyOn(writer, "observePrefix");
    const first = snapshot(a);
    const inner = observe.mock.results[0]!.value as ReturnType<NatsStateBackend["observePrefix"]>;
    if (inner.kind !== "snapshot") throw new Error("expected native snapshot");
    const hit = a.observePrefix(prefix, first.witness);
    expect(hit.kind).toBe("unchanged");
    if (hit.kind !== "unchanged") throw new Error("expected own paired hit");
    const foreign = snapshot(b, prefix, hit.witness);
    expect(observe.mock.lastCall).toEqual([prefix, undefined]);
    expect(foreign.entries).toEqual(first.entries);
    // B's cold native call replaced the ONE backend registry slot for this prefix. A still has
    // its own valid facade pair, but its now-stale native predecessor must resnapshot, not hit.
    const interfered = snapshot(a, prefix, hit.witness);
    expect(observe.mock.lastCall).toEqual([prefix, inner.witness]);
    expect(interfered.entries).toEqual(first.entries);
    const backToB = snapshot(b, prefix, interfered.witness);
    expect(observe.mock.lastCall).toEqual([prefix, undefined]);
    snapshot(a, prefix, backToB.witness);
    expect(observe.mock.lastCall).toEqual([prefix, undefined]);
    expect(first.witness).not.toBe(inner.witness);
    a.closeState(); // closes the shared native handle, not just A's wrapper
    expect(a.observePrefix(prefix, first.witness)).toEqual({ kind: "unknown", reason: "closed-prefix-observation-facade" });
    expect(() => b.observePrefix(prefix, backToB.witness)).toThrow(/backend is closed/);
    const reopened = await prepareNatsRoot(root);
    expect(reopened).not.toBe(writer);
    const c = facade(root, reopened), reopenedObserve = vi.spyOn(reopened, "observePrefix");
    expect(snapshot(c, prefix, first.witness).entries).toEqual(first.entries);
    expect(reopenedObserve).toHaveBeenCalledExactlyOnceWith(prefix, undefined);
  });

  it("native facade selections retain complete frozen values across stable/unrelated applies and see matching apply before callbacks", async () => {
    const { root, writer } = await setup();
    await seed(writer);
    const mesh = facade(root, writer), selected = selection(mesh);
    const first = selected.read(), bytes = JSON.stringify(first);
    expect(first).toHaveLength(387);
    expect(first).toEqual(writer.listAll(prefix)); // complete value, writer, timestamp, version and order
    expect(first.filter(entry => entry.key.startsWith(prefix + "foreign/"))).toHaveLength(360);
    expect(first.filter(entry => entry.key.startsWith(prefix + "own/"))).toHaveLength(27);
    const assertFrozen = (node: unknown): void => {
      if (node === null || typeof node !== "object") return;
      expect(Object.isFrozen(node)).toBe(true);
      for (const child of Object.values(node)) assertFrozen(child);
    };
    assertFrozen(first);
    const nested = first[0]!.value as { principal: { scopes: string[] }; data: { nested: { retained: boolean }[] } };
    expect(() => nested.principal.scopes.push("forged")).toThrow(TypeError);
    expect(() => { nested.data.nested[0]!.retained = false; }).toThrow(TypeError);
    expect(() => { (first[0]!.updatedBy as MeshIdentity).id = "forged"; }).toThrow(TypeError);
    for (let index = 0; index < 3; index++) expect(selected.read()).toBe(first);
    const { backend: remote } = await replica(root);
    await remote.put({ key: "residency/configs/0", value: "unrelated", identity });
    await waitForEntry(writer, "residency/configs/0", "unrelated");
    expect(selected.read()).toBe(first);
    const callback = deferred<PrefixEntries>();
    const unsubscribe = mesh.subscribeStateChanges(() => callback.resolve(selected.read()));
    try {
      await remote.put({ key: prefix + "foreign/live", value: { from: foreignWriter, data: { nested: ["arrival"] } }, identity: foreignWriter });
      const applied = await bounded(callback.promise, "facade apply-before-callback");
      expect(applied).not.toBe(first);
      expect(applied).toHaveLength(388);
      expect(applied).toEqual(writer.listAll(prefix));
      expect(applied.find(entry => entry.key === prefix + "foreign/live")).toMatchObject({ updatedBy: foreignWriter,
        value: { from: foreignWriter, data: { nested: ["arrival"] } } });
      assertFrozen(applied);
      expect(selected.read()).toBe(applied);
      expect(JSON.stringify(first)).toBe(bytes);
    } finally { unsubscribe(); }
  });

  it.each(["unknown", "absent"] as const)("native facade %s capability discards both saved values and witness for exactly today's one full scan", async mode => {
    const { root, writer } = await setup();
    await seed(writer);
    const mesh = facade(root, writer), selected = selection(mesh), observe = vi.spyOn(writer, "observePrefix");
    const first = selected.read();
    expect(selected.saved).toBeDefined();
    await mesh.put({ key: prefix + "foreign/after-saved-selection", value: { retained: ["new"] }, identity: foreignWriter });
    const expected = writer.listAll(prefix);
    let restore: () => void = () => undefined;
    let port: JetStreamPort | undefined;
    if (mode === "unknown") {
      ({ port } = await replica(root));
      await publishState(root, port, { type: "import-begin", epoch: 7 }, "facade-import-begin");
      await vi.waitFor(() => expect(writer.diagnostics().nats!.status).toBe("importing"));
    } else restore = removeObservation(writer);
    const meshScan = vi.spyOn(mesh, "listAll"), scan = vi.spyOn(writer, "listAll");
    try {
      const fallback = selected.read();
      expect(fallback).toEqual(expected);
      expect(fallback).not.toEqual(first); // reusing the pre-arrival selection is a real stale miss
      expect(selected.saved).toBeUndefined(); // neither fallback values nor witness become proof
      expect(Object.isFrozen(fallback)).toBe(false);
      expect(meshScan).toHaveBeenCalledExactlyOnceWith(prefix); // no fresh:true or added options
      expect(scan).toHaveBeenCalledExactlyOnceWith(prefix, {});
      expect(observe).toHaveBeenCalledTimes(mode === "unknown" ? 2 : 1); // no post-fallback stamp
      if (mode === "unknown") {
        await publishState(root, port!, { type: "import-commit", epoch: 7 }, "facade-import-commit");
        await vi.waitFor(() => expect(writer.diagnostics().nats!.status).toBe("ready"));
      } else restore();
      meshScan.mockClear(); scan.mockClear();
      expect(selected.read()).toEqual(expected);
      expect(selected.saved).toBeDefined();
      expect(observe.mock.lastCall).toEqual([prefix, undefined]); // later capability starts cold
      expect(meshScan).not.toHaveBeenCalled();
      expect(scan).not.toHaveBeenCalled();
    } finally { restore(); }
  });

  it("native fatal observation and absent/closed full-scan failures propagate canonical errors, never successful empty selections", async () => {
    const { root, writer } = await setup();
    await writer.put({ key: prefix + "retained", value: { pending: true }, identity });
    const mesh = facade(root, writer), selected = selection(mesh);
    expect(selected.read()).toHaveLength(1);
    const { port } = await replica(root);
    await publishState(root, port, { type: "batch", id: "facade-bad-base", base: -1, highWater: 0, changes: [] }, "facade-bad-base");
    await vi.waitFor(() => expect(writer.diagnostics().nats!.failure).toMatch(/committed on sequence -1/));
    const canonical = thrownBy(() => writer.listAll(prefix)); // canonical full-scan baseline before counted calls
    expect(canonical).toBeInstanceOf(MeshStateNatsRefusedError);
    const observe = vi.spyOn(writer, "observePrefix"), meshScan = vi.spyOn(mesh, "listAll"), scan = vi.spyOn(writer, "listAll");
    expect(thrownBy(() => selected.read())).toBe(canonical);
    expect(observe).toHaveBeenCalledTimes(1);
    expect(meshScan).not.toHaveBeenCalled();
    expect(scan).not.toHaveBeenCalled(); // no UNKNOWN downgrade, second read or retry budget
    const restore = removeObservation(writer);
    try {
      expect(thrownBy(() => selected.read())).toBe(canonical);
      expect(selected.saved).toBeUndefined();
      expect(meshScan).toHaveBeenCalledExactlyOnceWith(prefix);
      expect(scan).toHaveBeenCalledExactlyOnceWith(prefix, {});
      expect(scan.mock.results[0]?.type).toBe("throw");
      expect(scan.mock.results[0]?.value).toBe(canonical);
      expect(observe).toHaveBeenCalledTimes(1);
    } finally { restore(); }
    mesh.closeState();
    meshScan.mockClear(); scan.mockClear(); observe.mockClear();
    const closed = thrownBy(() => selected.read());
    expect(closed).toBeInstanceOf(Error);
    expect((closed as Error).message).toBe("Fabric mesh NATS state backend is closed");
    expect(selected.saved).toBeUndefined();
    expect(meshScan).toHaveBeenCalledExactlyOnceWith(prefix);
    expect(scan).toHaveBeenCalledExactlyOnceWith(prefix, {});
    expect(scan.mock.results[0]?.type).toBe("throw");
    expect(scan.mock.results[0]?.value).toBe(closed);
    expect(observe).not.toHaveBeenCalled(); // closed facade leaves the ONE legacy error to fallback
  });

  it("certifies only complete empty/cold/live projections; freezes an owned graph without changing legacy read behavior", async () => {
    const { writer } = await setup();
    const empty = snapshot(writer);
    expect(empty.entries).toEqual([]);
    expect(writer.observePrefix(prefix, empty.witness).kind).toBe("unchanged");
    await seed(writer);
    const first = snapshot(writer, prefix, empty.witness);
    expect(first.entries).toHaveLength(387);
    expect(first.entries).toEqual(writer.listAll(prefix));
    const nested = first.entries[0]!.value as { data: { nested: { retained: boolean }[] }; from: MeshIdentity };
    expect(Object.isFrozen(first.entries)).toBe(true);
    expect(Object.isFrozen(first.entries[0])).toBe(true);
    expect(Object.isFrozen(first.entries[0]!.updatedBy)).toBe(true);
    expect(Object.isFrozen(nested.from)).toBe(true);
    expect(Object.isFrozen(nested.data.nested[0])).toBe(true);
    expect(() => { nested.data.nested[0]!.retained = false; }).toThrow(TypeError);
    const mutable = writer.listAll(prefix);
    (mutable[0]!.value as typeof nested).data.nested[0]!.retained = false;
    expect(Object.isFrozen(writer.listAllShared(prefix)[0])).toBe(false);
    expect((writer.get(first.entries[0]!.key)!.value as typeof nested).data.nested[0]!.retained).toBe(true);
    const { backend: cold } = await replica(roots.at(-1)!);
    expect(snapshot(cold).entries).toEqual(first.entries);
    expect(writer.observePrefix(prefix, first.witness)).toEqual({ kind: "unchanged", witness: first.witness });
  });

  it("387 deliveries/181 configs: three stable hits and three unrelated-commit hits do no selection traversal/sort/clone/decode/freeze or port calls", async () => {
    const { root, writer } = await setup();
    await seed(writer);
    const { backend, port } = await replica(root);
    const first = snapshot(backend);
    const assertCheapHit = () => {
      const model = backend.stateToken() as { entries: Map<string, MeshStateEntry> };
      const forbidden = () => { throw new Error("selection work on unchanged hit"); };
      const spies = [
        vi.spyOn(model.entries, "keys").mockImplementation(forbidden),
        vi.spyOn(model.entries, "values").mockImplementation(forbidden),
        vi.spyOn(model.entries, "entries").mockImplementation(forbidden),
        vi.spyOn(model.entries, Symbol.iterator).mockImplementation(forbidden),
        vi.spyOn(model.entries, "forEach").mockImplementation(forbidden),
        vi.spyOn(model.entries, "get").mockImplementation(forbidden),
        vi.spyOn(Array.prototype, "sort").mockImplementation(forbidden),
        vi.spyOn(JSON, "parse").mockImplementation(forbidden),
        vi.spyOn(JSON, "stringify").mockImplementation(forbidden),
        vi.spyOn(Object, "freeze").mockImplementation(forbidden),
        ...(["server", "ensureStream", "lastSequence", "firstSequence", "streamExists", "getMessage", "lastBySubject", "purge", "publish", "consume", "close"] as const)
          .map(name => vi.spyOn(port, name).mockImplementation(forbidden)),
      ];
      let hit: ReturnType<NatsStateBackend["observePrefix"]>;
      try { hit = backend.observePrefix(prefix, first.witness); }
      finally { for (const spy of spies) spy.mockRestore(); }
      expect(hit!).toEqual({ kind: "unchanged", witness: first.witness });
    };
    for (let hit = 0; hit < 3; hit++) assertCheapHit();
    for (let commit = 0; commit < 3; commit++) {
      await writer.put({ key: "residency/configs/0", value: commit, identity });
      await waitForEntry(backend, "residency/configs/0", commit);
      assertCheapHit();
    }
    expect(backend.listAll(prefix)).toEqual(first.entries);
  });

  it("invalidates matching payload/writer changes before callbacks, including same-version replacements, without granting stale CAS", async () => {
    const { root, writer, stream } = await setup();
    const key = `${prefix}own/item`;
    await writer.put({ key, value: { nested: [1] }, identity });
    const { backend, port } = await replica(root);
    const first = snapshot(backend);
    const callback = deferred<ReturnType<NatsStateBackend["observePrefix"]>>();
    const unsubscribe = backend.subscribeStateChanges(() => callback.resolve(backend.observePrefix(prefix, first.witness)));
    try {
      // The stream protocol permits identical version numbers: key/version-only evidence is insufficient.
      await port.publish(`${stream}.state`, Buffer.from(JSON.stringify({ format: 1, type: "batch", id: "same-version",
        base: writer.diagnostics().nats!.appliedSeq, highWater: first.entries[0]!.version,
        changes: [{ key, put: { value: { nested: [2] }, version: first.entries[0]!.version,
          updatedAt: first.entries[0]!.updatedAt, updatedBy: foreignWriter } }] })),
      { expectLastSequence: writer.diagnostics().nats!.appliedSeq, msgID: "same-version" });
      const observed = await bounded(callback.promise, "apply-before-callback");
      expect(observed.kind).toBe("snapshot");
      if (observed.kind !== "snapshot") throw new Error("missing callback snapshot");
      expect(observed.entries[0]).toMatchObject({ value: { nested: [2] }, updatedBy: foreignWriter });
      expect(first.entries[0]).toMatchObject({ value: { nested: [1] }, updatedBy: identity });
      const beforeOwnCommit = snapshot(writer);
      let atCommit: ReturnType<NatsStateBackend["observePrefix"]> | undefined;
      await writer.writeBatch({ identity, ops: [{ kind: "put", key, value: { nested: [3] } }],
        afterCommit: () => { atCommit = writer.observePrefix(prefix, beforeOwnCommit.witness); } });
      expect(atCommit?.kind).toBe("snapshot");
      await vi.waitFor(() => expect(backend.get(key)?.value).toEqual({ nested: [3] }), { timeout: 5_000 });
      await expect(backend.put({ key, value: "stale", identity, ifVersion: first.entries[0]!.version })).rejects.toThrow(/compare-and-swap failed/);
      expect(backend.listAll(prefix)).toEqual(writer.listAll(prefix));
    } finally { unsubscribe(); }
  });

  it("rejects forged/wrong-prefix/wrong-backend/evicted/reset/reopened witnesses", async () => {
    const { root, writer } = await setup();
    await seed(writer);
    let first = snapshot(writer);
    expect(snapshot(writer, prefix, Object.freeze({})).entries).toEqual(first.entries);
    first = snapshot(writer);
    expect(snapshot(writer, "residency/configs/", first.witness).entries).toHaveLength(181);
    const { backend } = await replica(root);
    expect(snapshot(backend, prefix, first.witness).entries).toEqual(first.entries);
    writer.dropCache();
    first = snapshot(writer);
    // Exactly 33 distinct prefixes: retaining the oldest here would exceed the 32-prefix bound.
    for (let index = 0; index < 32; index++) snapshot(writer, `unused/${index}/`);
    expect(snapshot(writer, prefix, first.witness).entries).toEqual(first.entries);
    first = snapshot(writer);
    writer.dropCache();
    expect(snapshot(writer, prefix, first.witness).entries).toEqual(first.entries);
    const beforeClose = snapshot(backend);
    backend.close();
    expect(() => backend.observePrefix(prefix, beforeClose.witness)).toThrow(/closed/);
    const { backend: reopened } = await replica(root);
    expect(snapshot(reopened, prefix, beforeClose.witness).entries).toEqual(first.entries);
  });

  it("unsupported or throwing optional bindings stay UNKNOWN with the exact legacy full projection available", async () => {
    const { root, writer } = await setup();
    await seed(writer);
    for (const throws of [false, true]) {
      const { backend } = await replica(root, natsServerUrl!, port => {
        const consume = port.consume;
        port.consume = async (...args) => {
          const native = await consume(...args);
          return { stop: () => native.stop(), ...(throws ? { observationBinding: () => { throw new Error("optional proof unavailable"); } } : {}) };
        };
      });
      expect(backend.observePrefix(prefix)).toMatchObject({ kind: "unknown" });
      expect(backend.observePrefix(prefix, {})).toMatchObject({ kind: "unknown" });
      expect(backend.listAll(prefix)).toEqual(writer.listAll(prefix));
    }
  });

  it("import epoch/status transitions cannot reuse a witness; canonical fatal projector errors remain errors", async () => {
    const { root, writer, stream } = await setup();
    await seed(writer);
    const { backend, port } = await replica(root);
    const first = snapshot(backend);
    const publish = async (record: object, id: string) => {
      const base = await port.lastSequence(stream);
      await port.publish(`${stream}.state`, Buffer.from(JSON.stringify({ format: 1, ...record })), { expectLastSequence: base, msgID: id });
    };
    await publish({ type: "import-begin", epoch: 7 }, "begin");
    await vi.waitFor(() => expect(backend.diagnostics().nats!.status).toBe("importing"));
    expect(backend.observePrefix(prefix, first.witness)).toMatchObject({ kind: "unknown" });
    expect(backend.listAll(prefix)).toEqual(first.entries);
    await expect(backend.put({ key: "refused", value: 1, identity })).rejects.toBeInstanceOf(MeshStateNatsRefusedError);
    await publish({ type: "import-commit", epoch: 7 }, "commit");
    await vi.waitFor(() => expect(backend.diagnostics().nats!.status).toBe("ready"));
    expect(snapshot(backend, prefix, first.witness).entries).toEqual(first.entries);
    // A canonical illegal batch, not an optional-proof fault. Neither route may return UNKNOWN or empty.
    await publish({ type: "batch", id: "bad-base", base: -1, highWater: 0, changes: [] }, "bad-base");
    await vi.waitFor(() => expect(backend.diagnostics().nats!.failure).toMatch(/committed on sequence -1/));
    let canonical: unknown;
    try { backend.listAll(prefix); } catch (error) { canonical = error; }
    expect(canonical).toBeInstanceOf(MeshStateNatsRefusedError);
    expect(() => backend.observePrefix(prefix, first.witness)).toThrow(canonical as Error);
  });

  it("a real-stream apply gap is UNKNOWN during recovery, preserves today's old local reads, then snapshots the complete replay", async () => {
    const { root, writer } = await setup();
    await writer.put({ key: `${prefix}retained`, value: 1, identity });
    const restarting = deferred<void>();
    const release = deferred<void>();
    let skip = false;
    let holdRestart = false;
    let bufferReplay = false;
    const replay: (() => void)[] = [];
    const { backend } = await replica(root, natsServerUrl!, port => {
      const consume = port.consume;
      port.consume = async (stream, afterSeq, onMessage, onError): Promise<NatsProjectionConsumer> => {
        if (holdRestart) { holdRestart = false; restarting.resolve(); await release.promise; bufferReplay = true; }
        return consume(stream, afterSeq, message => {
          if (skip) { skip = false; return; }
          if (bufferReplay) { replay.push(() => onMessage(message)); return; }
          onMessage(message);
        }, onError);
      };
    });
    const first = snapshot(backend);
    try {
      skip = true;
      await writer.put({ key: `${prefix}missed`, value: 2, identity });
      holdRestart = true;
      await writer.put({ key: `${prefix}gap`, value: 3, identity });
      await bounded(restarting.promise, "recovery consumer");
      expect(backend.observePrefix(prefix, first.witness)).toMatchObject({ kind: "unknown" });
      expect(backend.listAll(prefix)).toEqual(first.entries); // no freshness strengthening or certified empty
      release.resolve();
      await vi.waitFor(() => expect(backend.diagnostics().nats!.resyncing).toBe(false));
      // A new native consumer exists but has not supplied its complete initialization head yet.
      // Merely finishing the recovery setup must not certify the old selection as unchanged or empty.
      expect(backend.observePrefix(prefix, first.witness)).toMatchObject({ kind: "unknown" });
      expect(backend.listAll(prefix)).toEqual(first.entries);
      await vi.waitFor(() => expect(replay).toHaveLength(2));
      bufferReplay = false;
      for (const apply of replay) apply();
      await waitForEntry(backend, `${prefix}gap`, 3);
      const recovered = snapshot(backend, prefix, first.witness);
      expect(recovered.entries).toEqual(writer.listAll(prefix));
      expect(recovered.entries).toHaveLength(3);
      expect(backend.observePrefix(prefix, recovered.witness).kind).toBe("unchanged");
    } finally { release.resolve(); }
  });

  it("a native transport reconnect invalidates an otherwise unchanged prefix, without touching the parent's server", async () => {
    const { root, writer } = await setup();
    await writer.put({ key: `${prefix}retained`, value: 1, identity });
    const relay = await ObservationRelay.open();
    const { backend } = await replica(root, relay.url);
    const first = snapshot(backend);
    const accepts = relay.accepts;
    relay.sever();
    await writer.put({ key: "unrelated/reconnect", value: true, identity });
    await waitForEntry(backend, "unrelated/reconnect", true);
    expect(relay.accepts).toBeGreaterThan(accepts);
    // Do NOT observe in the disconnected interval: clearing via UNKNOWN would mask missing reconnect binding.
    expect(snapshot(backend, prefix, first.witness).entries).toEqual(first.entries);
    expect(relay.recordedDeletes).toEqual([]);
  }, 20_000);

  it("a native ordered-consumer reset invalidates an unchanged prefix before callbacks; old-consumer DELETE is only recorded", async () => {
    const { root, writer } = await setup();
    await writer.put({ key: `${prefix}retained`, value: 1, identity });
    const relay = await ObservationRelay.open();
    const { backend } = await replica(root, relay.url);
    const first = snapshot(backend);
    relay.dropSequence = backend.diagnostics().nats!.appliedSeq + 1;
    await writer.put({ key: "unrelated/reset-one", value: 1, identity });
    await writer.put({ key: "unrelated/reset-two", value: 2, identity });
    await waitForEntry(backend, "unrelated/reset-two", 2);
    expect(relay.dropped).toBe(1);
    expect(relay.consumerNames.size).toBeGreaterThan(1);
    expect(relay.recordedDeletes).toHaveLength(1);
    expect(relay.recordedDeletes[0]).toMatch(/^\$JS\.API\.CONSUMER\.DELETE\./);
    expect(snapshot(backend, prefix, first.witness).entries).toEqual(first.entries);
    expect(backend.listAll(prefix)).toEqual(writer.listAll(prefix));
  }, 20_000);
});
