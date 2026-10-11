import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  applyNatsMessage, closeNatsStateBackend, MeshStateNatsBusyError, MeshStateNatsRefusedError, NatsStateBackend,
  openNatsStateBackend, preparedNatsStateBackend, type JetStreamPort,
} from "../src/mesh/state-backend-nats.js";
import { createStateBackend, MeshStateBackendNotBuiltError } from "../src/mesh/state-backend.js";
import { MeshBatchConflictError } from "../src/mesh/state-file.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";
import type { MeshStoreContext } from "../src/mesh/mesh-lock.js";
import { fakeJetStream, type FakeJetStreamServer } from "./helpers/nats-jetstream-fake.js";

// smarty-dev#7504 phase 1: the single-host JetStream state backend (docs/mesh-backends.md option (c)) against an
// in-process fake of the five JetStream operations it uses. The real server runs in phase 2's conformance job.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) closeNatsStateBackend(root); });
const root = () => { const value = path.join(os.tmpdir(), `nats-state-${Math.random().toString(36).slice(2)}`); roots.push(value); return value; };
const open = (server: FakeJetStreamServer, extra: Partial<Parameters<typeof openNatsStateBackend>[0]> = {}) =>
  openNatsStateBackend({ root: root(), port: server.port(), stream: "S", lockTimeoutMs: 2_000, ...extra });
const encode = (record: unknown) => Buffer.from(JSON.stringify(record));
const settle = () => new Promise<void>(resolve => setImmediate(resolve));

describe("NATS state backend (smarty-dev#7504 phase 1)", () => {
  it("puts, reads its own write at once, lists in key order, deletes with a CAS tombstone", async () => {
    const backend = await open(fakeJetStream());
    const a = await backend.put({ key: "ns/b", value: 2, identity });
    await backend.put({ key: "ns/a", value: 1, identity });
    expect(backend.get("ns/b")).toMatchObject({ key: "ns/b", value: 2, version: a.version });
    expect(backend.listAll("ns/").map(entry => entry.key)).toEqual(["ns/a", "ns/b"]);
    expect(backend.list("ns/", 1).map(entry => entry.key)).toEqual(["ns/a"]);
    const deleted = await backend.delete({ key: "ns/b", ifVersion: a.version });
    expect(deleted.deleted).toBe(true);
    expect(backend.get("ns/b")).toBeUndefined();
    // The tombstone keeps the key's version: a put that expects "absent and never seen" (0) conflicts.
    await expect(backend.put({ key: "ns/b", value: 3, identity, ifVersion: 0 })).rejects.toThrow();
    await backend.put({ key: "ns/b", value: 3, identity, ifVersion: deleted.version! });
    expect(backend.get("ns/b")?.value).toBe(3);
  });

  it("runs writeBatch atomically: prepare sees the snapshot, an aborting conflict writes nothing, callbacks run", async () => {
    const server = fakeJetStream();
    const backend = await open(server);
    await backend.put({ key: "k/a", value: 1, identity });
    const before = server.messages.length;
    await expect(backend.writeBatch({ identity, ops: [{ kind: "put", key: "k/b", value: 2 }, { kind: "put", key: "k/a", value: 9, ifVersion: 999 }] }))
      .rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(server.messages.length).toBe(before);
    let effects: { changed: readonly string[] } | undefined;
    const results = await backend.writeBatch({ identity, ops: [],
      prepare: view => [{ kind: "put", key: "k/c", value: (view.get("k/a")?.value as number) + 1 }],
      commitOutbox: effect => { effects = effect; } });
    expect(results).toEqual([{ key: "k/c", applied: true, version: expect.any(Number) }]);
    expect(backend.get("k/c")?.value).toBe(2);
    expect(effects?.changed).toEqual(["k/c"]);
    expect(server.messages.length).toBe(before + 1);
  });

  it("projector: applying a message twice, or a redelivery, changes nothing; a second host replays to the same state", async () => {
    const server = fakeJetStream();
    const backend = await open(server);
    for (let index = 0; index < 5; index++) await backend.put({ key: `p/${index}`, value: index, identity });
    await backend.delete({ key: "p/2" });
    const once = applyNatsMessage({ seq: 0, entries: new Map(), versions: new Map(), tombstones: [], highWater: 0, status: "ready", recent: new Map() }, server.messages[0]!, 100);
    expect(applyNatsMessage(once, server.messages[0]!, 100)).toBe(once);
    const replica = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", maxEventBytes: 1 << 20 });
    expect(replica.listAll("p/")).toEqual(backend.listAll("p/"));
    expect(replica.stateStamp()?.split(":").at(-1)).toBe(String(server.messages.length));
    replica.close();
  });

  it("a lost ack is retried with the same message id: one stored message, one commit, never applied twice", async () => {
    const server = fakeJetStream();
    const backend = await open(server);
    await backend.put({ key: "d/count", value: 0, identity });
    let lose = true;
    server.beforePublish = () => { if (lose) { lose = false; return "lose-ack"; } };
    // The retry fails the expected sequence (the real server checks it before its duplicate window); the batch is
    // then found in the stream by its id, so the increment is not run a second time.
    let prepares = 0;
    await backend.writeBatch({ identity, ops: [], prepare: view => { prepares++; return [{ kind: "put", key: "d/count", value: (view.get("d/count")?.value as number) + 1 }]; } });
    expect(prepares).toBe(1);
    expect(server.messages.length).toBe(2);
    expect(backend.get("d/count")?.value).toBe(1);
  });

  it("a publish that never gets an answer is an unknown outcome, not a retryable busy error", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { lockTimeoutMs: 30 });
    // Every publish times out before the server stores anything.
    server.beforePublish = () => { throw Object.assign(new Error("timeout: no responders"), { transient: true }); };
    await expect(backend.put({ key: "u/a", value: 1, identity })).rejects.toThrow(/OUTCOME_UNKNOWN/);
    expect(server.messages.length).toBe(0);
  });

  it("a final publish error (an oversized payload, a permission) is thrown at once, never retried", async () => {
    const server = fakeJetStream();
    const backend = await open(server);
    let publishes = 0;
    server.beforePublish = () => { publishes++; throw new Error("MAX_PAYLOAD_EXCEEDED"); };
    await expect(backend.put({ key: "m/a", value: 1, identity })).rejects.toThrow(/MAX_PAYLOAD_EXCEEDED/);
    expect(publishes).toBe(1);
  });

  it("a broken projector delivery fails closed: reads and writes throw", async () => {
    const server = fakeJetStream();
    const backend = await open(server);
    await backend.put({ key: "x/a", value: 1, identity });
    server.breakDelivery(new Error("consumer deleted"));
    expect(() => backend.get("x/a")).toThrow(/consumer deleted/);
    await expect(backend.put({ key: "x/b", value: 1, identity })).rejects.toThrow(/consumer deleted/);
    expect(backend.diagnostics().nats?.failure).toMatch(/consumer deleted/);
  });

  it("CAS retry: another writer commits between prepare and publish; the batch runs again on the new state", async () => {
    const server = fakeJetStream();
    const first = await open(server);
    const second = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", maxEventBytes: 1 << 20 });
    await first.put({ key: "c/count", value: 0, identity });
    await new Promise(resolve => setTimeout(resolve, 5));
    let injected = false;
    server.beforePublish = () => {
      if (injected) return;
      injected = true;
      // The other writer's commit lands at the stream head first (its own expected sequence held).
      server.append(Buffer.from(JSON.stringify({ format: 1, type: "batch", base: server.messages.length, highWater: 1_000,
        changes: [{ key: "c/count", put: { value: 10, version: 1_000, updatedAt: 1, updatedBy: identity } }] })));
    };
    let prepares = 0;
    await second.writeBatch({ identity, ops: [], prepare: view => { prepares++; return [{ kind: "put", key: "c/count", value: (view.get("c/count")?.value as number) + 1 }]; } });
    expect(prepares).toBe(2);
    expect(second.get("c/count")?.value).toBe(11);
    await settle();
    expect(first.get("c/count")?.value).toBe(11);
    expect(second.diagnostics().nats?.casRetries).toBe(1);
    // A conflicting ifVersion on the key the other writer changed aborts after the catch-up.
    delete server.beforePublish;
    const stale = first.get("c/count")!.version;
    await second.put({ key: "c/count", value: 12, identity });
    await expect(first.put({ key: "c/count", value: 13, identity, ifVersion: stale })).rejects.toBeInstanceOf(MeshBatchConflictError);
    second.close();
  });

  it("orders state tokens: a token reads its own state after later writes; stamps advance with the stream", async () => {
    const backend = await open(fakeJetStream());
    await backend.put({ key: "t/a", value: 1, identity });
    const token = backend.stateToken();
    const stamp = backend.stateStamp()!;
    await backend.put({ key: "t/a", value: 2, identity });
    expect(backend.get("t/a", { snapshot: token })?.value).toBe(1);
    expect(backend.get("t/a")?.value).toBe(2);
    const seq = (value: string) => Number(value.split(":").at(-1));
    expect(seq(backend.stateStamp()!)).toBeGreaterThan(seq(stamp));
  });

  it("a fresh read refuses (busy, retryable) while the projector lags the stream", async () => {
    const server = fakeJetStream();
    const backend = await open(server);
    await backend.put({ key: "f/a", value: 1, identity });
    // Another host commits while this host's projector is held back; confirmWritable learns the stream's head.
    server.pause();
    server.append(encode({ format: 1, type: "batch", base: 1, highWater: 50,
      changes: [{ key: "f/a", put: { value: 5, version: 50, updatedAt: 1, updatedBy: identity } }] }));
    await backend.confirmWritable();
    expect(() => backend.get("f/a", { fresh: true })).toThrow(MeshStateNatsBusyError);
    expect(backend.get("f/a")?.value).toBe(1); // a non-fresh read serves the applied model
    server.resume();
    await settle(); await settle();
    expect(backend.get("f/a", { fresh: true })).toMatchObject({ value: 5 });
  });

  describe("refusals (never a fallback to file)", () => {
    it.each([
      ["a server older than 2.11", { version: "2.10.22", syncAlways: true }, /older than 2\.11/],
      ["a server that does not fsync every write", { version: "2.14.7", syncAlways: false }, /sync_always/],
      ["a server that does not report its sync policy", { version: "2.14.7", syncAlways: undefined }, /sync_always/],
    ])("refuses %s", async (_name, facts, reason) => {
      const server = fakeJetStream();
      Object.assign(server, facts);
      await expect(open(server)).rejects.toThrow(reason);
    });

    it("accepts a periodic-sync server only when the operator says so", async () => {
      const server = fakeJetStream();
      server.syncAlways = false;
      await expect(open(server, { acceptPeriodicSync: true })).resolves.toBeInstanceOf(NatsStateBackend);
    });

    it("refuses a retired stream, and reads and writes once it is retired", async () => {
      const server = fakeJetStream();
      const backend = await open(server);
      await backend.put({ key: "r/a", value: 1, identity });
      server.append(encode({ format: 1, type: "retire", epoch: 2 }));
      await settle();
      expect(() => backend.get("r/a")).toThrow(MeshStateNatsRefusedError);
      await expect(backend.put({ key: "r/b", value: 1, identity })).rejects.toThrow(MeshStateNatsRefusedError);
      await expect(open(server)).rejects.toThrow(/retired/);
    });

    it("refuses writes while the stream is importing", async () => {
      const server = fakeJetStream();
      server.append(encode({ format: 1, type: "import-begin", epoch: 1 }));
      const backend = await open(server);
      await expect(backend.put({ key: "i/a", value: 1, identity })).rejects.toThrow(/importing/);
    });

    it("fails closed on a batch that could not have passed the CAS (a protocol violation)", async () => {
      const server = fakeJetStream();
      const backend = await open(server);
      server.append(encode({ format: 1, type: "batch", base: 7, highWater: 1, changes: [] }));
      await settle();
      expect(() => backend.get("v/a")).toThrow(/committed on sequence 7/);
    });

    it("the selector's nats factory returns only a prepared backend; an unprepared root refuses", async () => {
      const server = fakeJetStream();
      const backend = await open(server);
      const prepared = roots.at(-1)!;
      expect(preparedNatsStateBackend(prepared)).toBe(backend);
      const context = (at: string) => ({ root: at, maxEventBytes: 1 << 20, maxReadEvents: 100, lock: undefined as never }) as MeshStoreContext;
      expect(createStateBackend(context(prepared), { stateBackend: "nats" })).toBe(backend);
      expect(() => createStateBackend(context(root()), { stateBackend: "nats" })).toThrow(MeshStateBackendNotBuiltError);
    });

    it("a write that keeps losing the CAS gives up as busy (retryable), within its budget", async () => {
      const server = fakeJetStream();
      const backend = await open(server, { lockTimeoutMs: 50 });
      server.beforePublish = () => {
        server.append(encode({ format: 1, type: "batch", base: server.messages.length, highWater: server.messages.length + 1, changes: [] }));
      };
      await expect(backend.put({ key: "b/a", value: 1, identity })).rejects.toBeInstanceOf(MeshStateNatsBusyError);
    });
  });

  it("serves a MeshStore opened with stateBackend nats on a prepared root", async () => {
    const { MeshStore } = await import("../src/mesh/store.js");
    const fs = await import("node:fs");
    const meshRoot = root();
    fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
    await openNatsStateBackend({ root: meshRoot, port: fakeJetStream().port(), stream: "M" });
    const store = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend: "nats" });
    try {
      const written = await store.put({ key: "shared/a", value: { n: 1 }, identity });
      expect(store.get("shared/a")).toMatchObject({ value: { n: 1 }, version: written.version });
      expect(store.listAll("shared/").map(entry => entry.key)).toEqual(["shared/a"]);
      expect(fs.existsSync(`${meshRoot}/state.json`)).toBe(false);
    } finally { store.closeState(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
  });

  it("loads no NATS client unless a backend is opened without a port", async () => {
    const port: JetStreamPort = fakeJetStream().port();
    await open({ ...fakeJetStream(), port: () => port } as unknown as FakeJetStreamServer);
    expect(Object.keys(await import("../src/mesh/state-backend-nats.js"))).not.toContain("connectJetStream");
  });
});
