import os from "node:os";
import path from "node:path";
import semver from "semver";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  applyNatsMessage, assertNatsServerSupported, assertNatsTransportSecure, closeNatsStateBackend, MeshStateNatsBusyError, MeshStateNatsInsecureError, MeshStateNatsOutcomeUnknownError,
  MAX_MESSAGE_BYTES, MIN_SERVER, natsVersionAtLeastForTest, MeshStateNatsLimitError, MeshStateNatsNotDurableError, MeshStateNatsRefusedError, MeshStateNatsSnapshotIntegrityError, MeshStateNatsUnavailableError,
  guardNatsPort, NatsStateBackend, natsStreamName, NatsWrongLastSequenceError, openNatsStateBackend, preparedNatsStateBackend, redactNatsUrl, redactNatsUrls, type JetStreamPort,
} from "../src/mesh/state-backend-nats.js";
import { createStateBackend, MeshStateBackendNotBuiltError } from "../src/mesh/state-backend.js";
import { MeshBatchConflictError } from "../src/mesh/state-file.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";
import type { MeshStoreContext } from "../src/mesh/mesh-lock.js";
import { expectSyncDurableOrRefused, meshSyncRefusalCode } from "./helpers/mesh-sync-platform.js";
import { fakeJetStream, type FakeJetStreamServer } from "./helpers/nats-jetstream-fake.js";

// smarty-dev#7504 phase 1: the single-host JetStream state backend (docs/mesh-backends.md option (c)) against an
// in-process fake of the five JetStream operations it uses. The real server runs in phase 2's conformance job.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) closeNatsStateBackend(root); });
const root = () => { const value = path.join(os.tmpdir(), `nats-state-${Math.random().toString(36).slice(2)}`); roots.push(value); return value; };
const open = (server: FakeJetStreamServer, extra: Partial<Parameters<typeof openNatsStateBackend>[0]> = {}) =>
  openNatsStateBackend({ root: root(), port: server.port(), lockTimeoutMs: 2_000, ...extra });
const encode = (record: unknown) => Buffer.from(JSON.stringify(record));
/** A stub resolver (no network): `localhost` is loopback, `nats.example.com` is not, anything else does not resolve. */
const resolver = async (host: string) => {
  if (host === "localhost") return [{ address: "127.0.0.1", family: 4 }, { address: "::1", family: 6 }];
  if (host === "nats.example.com") return [{ address: "10.1.1.1", family: 4 }];
  throw Object.assign(new Error(`getaddrinfo ENOTFOUND ${host}`), { code: "ENOTFOUND" });
};
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
    const once = applyNatsMessage({ seq: 0, entries: new Map(), versions: new Map(), tombstones: [], highWater: 0, status: "ready", epoch: 0, snapshotEnd: 0, snapshotDigest: "" }, server.messages[0]!, 100);
    expect(applyNatsMessage(once, server.messages[0]!, 100)).toBe(once);
    const replica = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });
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

  it("a lost ack followed by more than 4,096 foreign batches still reconciles exactly: prepare once, no busy, no double apply", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { lockTimeoutMs: 20_000 });
    await backend.put({ key: "d/count", value: 0, identity });
    let lose = true;
    server.beforePublish = () => { if (lose) { lose = false; return "lose-ack"; } };
    // My batch is stored; then 5,000 other batches commit before my retry reconciles.
    server.afterStore = () => {
      delete server.afterStore;
      for (let index = 0; index < 5_000; index++) server.append(encode({ format: 1, type: "batch", id: `other-${index}`, base: server.messages.length,
        highWater: 10_000 + index, changes: [{ key: `f/${index % 10}`, put: { value: index, version: 10_000 + index, updatedAt: 1, updatedBy: identity } }] }));
    };
    let prepares = 0;
    await backend.writeBatch({ identity, ops: [], prepare: view => { prepares++; return [{ kind: "put", key: "d/count", value: (view.get("d/count")?.value as number) + 1 }]; } });
    expect(prepares).toBe(1);
    expect(server.messages.length).toBe(5_002);
    expect(backend.get("d/count")?.value).toBe(1);
    expect(backend.diagnostics().nats?.casRetries).toBe(0);
  });

  it("a CAS failure after a lost ack whose batch was NOT stored runs prepare again (another writer took the slot)", async () => {
    const server = fakeJetStream();
    const backend = await open(server);
    await backend.put({ key: "d/count", value: 0, identity });
    let calls = 0;
    server.beforePublish = () => {
      if (++calls !== 1) return;
      // Another writer commits first, then my first attempt gets no answer and is not stored.
      server.append(encode({ format: 1, type: "batch", id: "other", base: server.messages.length, highWater: 50_000,
        changes: [{ key: "d/other", put: { value: 1, version: 50_000, updatedAt: 1, updatedBy: identity } }] }));
      throw Object.assign(new Error("timeout"), { transient: true });
    };
    let prepares = 0;
    await backend.writeBatch({ identity, ops: [], prepare: view => { prepares++; return [{ kind: "put", key: "d/count", value: (view.get("d/count")?.value as number) + 1 }]; } });
    expect(prepares).toBe(2);
    expect(backend.get("d/count")?.value).toBe(1);
    expect(server.messages.length).toBe(3);
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
    const second = await NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });
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
    await expect(first.put({ key: "c/count", value: 13, identity, ifVersion: stale })).rejects.toThrow(/compare-and-swap failed for c\/count/);
    second.close();
  });

  it("a single put or delete CAS conflict throws what the file backend throws (a plain Error, the same message); writeBatch keeps MeshBatchConflictError", async () => {
    const { MeshStore } = await import("../src/mesh/store.js");
    const fs = await import("node:fs");
    const fileRoot = root();
    fs.mkdirSync(fileRoot, { recursive: true, mode: 0o700 });
    const file = new MeshStore(fileRoot, 64 * 1024, 100, { stateBackend: "file" });
    const backend = await open(fakeJetStream());
    const failure = async (write: () => Promise<unknown>) => { try { await write(); } catch (error) { return error as Error; } throw new Error("no conflict"); };
    try {
      await file.put({ key: "a", value: 1, identity });
      await backend.put({ key: "a", value: 1, identity });
      const expected = await failure(() => file.put({ key: "a", value: 2, identity, ifVersion: 0 }));
      const put = await failure(() => backend.put({ key: "a", value: 2, identity, ifVersion: 0 }));
      expect(put.constructor).toBe(expected.constructor);
      expect(put).not.toBeInstanceOf(MeshBatchConflictError);
      expect(put.message).toBe(expected.message);
      const expectedDelete = await failure(() => file.delete({ key: "a", ifVersion: 99 }));
      const deleted = await failure(() => backend.delete({ key: "a", ifVersion: 99 }));
      expect(deleted.constructor).toBe(expectedDelete.constructor);
      expect(deleted.message).toBe(expectedDelete.message);
      await expect(backend.writeBatch({ identity, ops: [{ kind: "put", key: "a", value: 3, ifVersion: 0 }] })).rejects.toBeInstanceOf(MeshBatchConflictError);
    } finally { file.closeState(); fs.rmSync(fileRoot, { recursive: true, force: true }); }
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

  it("sync is a no-op barrier: an acked commit is already durable at the server (sync_always), no JetStream call", async () => {
    const port = fakeJetStream().port();
    const backend = await openNatsStateBackend({ root: root(), port, lockTimeoutMs: 2_000 });
    await backend.put({ key: "ns/a", value: 1, identity });
    const calls = port.calls.length;
    await expect(backend.sync(0)).resolves.toBeUndefined();
    expect(port.calls.length).toBe(calls);
  });

  it.each([false, undefined])("sync rejects as not durable when acceptPeriodicSync opened a server with sync_always %s", async syncAlways => {
    const server = fakeJetStream();
    server.syncAlways = syncAlways;
    const backend = await open(server, { acceptPeriodicSync: true });
    await backend.put({ key: "ns/a", value: 1, identity });
    const error = await backend.sync(1_000).then(() => undefined, (failure: unknown) => failure);
    expect(error).toBeInstanceOf(MeshStateNatsNotDurableError);
    expect(error).toMatchObject({ code: "FABRIC_MESH_STATE_NATS_NOT_DURABLE", retryable: false, syncAlways });
    expect((error as Error).message).toMatch(/durability is not confirmed.*acceptPeriodicSync.*not durable/);
    // A later sync_always change at the server does not upgrade a backend opened without it: open records the fact.
    server.syncAlways = true;
    await expect(backend.sync(0)).rejects.toBeInstanceOf(MeshStateNatsNotDurableError);
    // The backend itself stays usable: only the durability claim is refused.
    expect(backend.get("ns/a")?.value).toBe(1);
  });

  it("mesh.sync reports not durable on a periodic-sync server, and synced on a sync_always one", async () => {
    const { MeshStore } = await import("../src/mesh/store.js");
    const { MeshProvider } = await import("../src/providers/mesh-provider.js");
    const { withFabricHostCaller } = await import("../src/fabric-provenance.js");
    const fs = await import("node:fs");
    const host = withFabricHostCaller({
      cwd: process.cwd(), signal: undefined, extensionContext: {} as never,
      parentToolCallId: "nats-sync", nestedToolCallId: "nats-sync", update() {},
    }, "fabric-github.forwarder");
    const participants = { list: () => [], get: () => undefined, self: () => { throw new Error("unused"); },
      peers: () => [], async refresh() {}, scheduleRefresh() {} };
    for (const syncAlways of [false, true]) {
      const meshRoot = root();
      fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
      const server = fakeJetStream();
      server.syncAlways = syncAlways;
      await openNatsStateBackend({ root: meshRoot, port: server.port(), acceptPeriodicSync: !syncAlways });
      const store = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend: "nats" });
      try {
        const provider = new MeshProvider(store, identity, participants as never);
        await store.put({ key: "shared/a", value: 1, identity });
        const event = await store.publish({ topic: "ops.sync", from: identity });
        if (syncAlways) {
          await expectSyncDurableOrRefused(store.sync(), { through: event.sequence });
          await expectSyncDurableOrRefused(provider.invoke("sync", {}, host), { synced: true, through: event.sequence });
        } else {
          // The event barrier runs first: on win32 it refuses before the NATS state barrier is reached.
          const code = meshSyncRefusalCode("FABRIC_MESH_STATE_NATS_NOT_DURABLE");
          await expect(store.sync()).rejects.toMatchObject({ code, retryable: false });
          await expect(provider.invoke("sync", {}, host)).rejects.toMatchObject({
            code, message: expect.stringContaining("not durable") });
        }
      } finally { store.closeState(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
    }
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
      ["a server older than 2.14.7", { version: "2.10.22", syncAlways: true }, /older than 2\.14\.7/],
      // pi-fabric#918's cases under #937's strict-semver floor: both refuse as below it.
      ["a prerelease of the minimum (pi-fabric#918)", { version: "2.14.7-rc.1", syncAlways: true }, /older than 2\.14\.7/],
      ["a version with a suffix", { version: "2.14.7junk", syncAlways: true }, /older than 2\.14\.7/],
      ["a server that does not fsync every write", { version: "2.14.7", syncAlways: false }, /sync_always/],
      ["a server that does not report its sync policy", { version: "2.14.7", syncAlways: undefined }, /sync_always/],
    ])("refuses %s", async (_name, facts, reason) => {
      const server = fakeJetStream();
      Object.assign(server, facts);
      await expect(open(server)).rejects.toThrow(reason);
    });

    // pi-fabric#918's shared gate (open and migration) applies #937's strict-semver floor (its parity test is above).
    it("the version rule: at or above 2.14.7 by strict semver; below it, a prerelease of it or an unparseable version fails closed", () => {
      for (const version of ["2.14.7", "2.14.10", "3.0.0", "v2.14.7", "2.15.0-rc.1"]) expect(() => assertNatsServerSupported(version)).not.toThrow();
      for (const version of ["2.11.0", "2.14.6", "2.14.7-rc.1", "2.14.7junk", "2.14", "2.14.7.1", ""]) {
        expect(() => assertNatsServerSupported(version), version).toThrow(/older than 2\.14\.7/);
      }
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

    it("follows the lifecycle table: begin, commit with its epoch, retire; then nothing", async () => {
      const server = fakeJetStream();
      const backend = await open(server);
      server.append(encode({ format: 1, type: "import-begin", epoch: 1 }));
      await settle();
      expect(backend.diagnostics().nats?.status).toBe("importing");
      server.append(encode({ format: 1, type: "import-commit", epoch: 1 }));
      await settle();
      expect(backend.diagnostics().nats?.status).toBe("ready");
      await backend.put({ key: "l/a", value: 1, identity });
      server.append(encode({ format: 1, type: "retire", epoch: 2 }));
      await settle();
      expect(backend.diagnostics().nats?.status).toBe("retired");
      expect(backend.diagnostics().nats?.failure).toBeUndefined();
    });

    it.each([
      ["import-commit after retire (it would reopen the stream)", [{ type: "retire", epoch: 2 }, { type: "import-commit", epoch: 2 }]],
      ["import-commit without import-begin", [{ type: "import-commit", epoch: 1 }]],
      ["import-commit with another epoch", [{ type: "import-begin", epoch: 1 }, { type: "import-commit", epoch: 2 }]],
      ["import-begin while importing", [{ type: "import-begin", epoch: 1 }, { type: "import-begin", epoch: 2 }]],
      ["retire with a stale epoch", [{ type: "retire", epoch: 0 }]],
      ["retire after retire", [{ type: "retire", epoch: 2 }, { type: "retire", epoch: 3 }]],
      ["import-begin after retire", [{ type: "retire", epoch: 2 }, { type: "import-begin", epoch: 3 }]],
      ["a batch after retire", [{ type: "retire", epoch: 2 }, { type: "batch", id: "late", base: 1, highWater: 1, changes: [] }]],
    ])("fails closed on an illegal lifecycle record: %s", async (_name, records) => {
      const server = fakeJetStream();
      const backend = await open(server);
      for (const record of records) server.append(encode({ format: 1, ...record }));
      await settle(); await settle();
      expect(backend.diagnostics().nats?.failure).toMatch(/illegal while|after the stream was retired/);
      expect(() => backend.get("x")).toThrow(MeshStateNatsRefusedError);
      // A host opening this stream refuses too.
      await expect(open(server)).rejects.toBeInstanceOf(MeshStateNatsRefusedError);
    });

    it("refuses plaintext to a non-loopback server or monitoring URL, before connecting; loopback stays plaintext", async () => {
      // No port: these would load the client and connect; the refusal comes first. A stub resolver: no network.
      for (const servers of [["nats://10.0.0.5:4222"], ["nats.example.com:4222"], ["127.0.0.1:4222", "nats://192.168.1.2:4222"]]) {
        await expect(openNatsStateBackend({ root: root(), servers, lookup: resolver })).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
      }
      await expect(openNatsStateBackend({ root: root(), servers: ["tls://127.0.0.1:4222"], monitoringUrl: "http://10.0.0.5:8222", lookup: resolver }))
        .rejects.toBeInstanceOf(MeshStateNatsInsecureError);
      expect(await assertNatsTransportSecure("tls://nats.example.com:4222", "tls:", resolver)).toBe("tls://nats.example.com:4222");
      expect(await assertNatsTransportSecure("https://nats.example.com:8222", "https:", resolver)).toBe("https://nats.example.com:8222");
      for (const loopback of ["nats://127.0.0.1:4222", "127.9.8.7:4222", "nats://localhost:4222", "nats://[::1]:4222"]) {
        await expect(assertNatsTransportSecure(loopback, "tls:", resolver)).resolves.toBeTypeOf("string");
      }
      expect(await assertNatsTransportSecure("http://127.0.0.1:8222", "https:", resolver)).toBe("http://127.0.0.1:8222");
      // Not loopback: a name that only starts like one (it resolves elsewhere, or not at all).
      await expect(assertNatsTransportSecure("nats://127.0.0.1.example.com:4222", "tls:", resolver)).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
    });

    it("derives distinct streams for roots whose old truncated names collided", () => {
      const oldName = (at: string) => `FABRIC_MESH_${Buffer.from(path.resolve(at)).toString("base64url").replace(/[^A-Za-z0-9_]/g, "_").slice(-48)}`;
      const tail = "/shared/mesh/root/with/a/long/common/suffix/state";
      const [left, right] = [`/srv/a1${tail}`, `/srv/b2${tail}`];
      expect(oldName(left)).toBe(oldName(right));
      expect(natsStreamName(left)).not.toBe(natsStreamName(right));
      expect(natsStreamName(left)).toMatch(/^FABRIC_MESH_[0-9a-f]{32}$/);
      expect(natsStreamName(left)).toBe(natsStreamName(`/srv/a1/x/..${tail}`));
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
    await openNatsStateBackend({ root: meshRoot, port: fakeJetStream().port() });
    const store = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend: "nats" });
    try {
      const written = await store.put({ key: "shared/a", value: { n: 1 }, identity });
      expect(store.get("shared/a")).toMatchObject({ value: { n: 1 }, version: written.version });
      expect(store.listAll("shared/").map(entry => entry.key)).toEqual(["shared/a"]);
      expect(fs.existsSync(`${meshRoot}/state.json`)).toBe(false);
      await expectSyncDurableOrRefused(store.sync(), { through: expect.any(Number) });
    } finally { store.closeState(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
  });

  it("loads no NATS client unless a backend is opened without a port", async () => {
    const port: JetStreamPort = fakeJetStream().port();
    await open({ ...fakeJetStream(), port: () => port } as unknown as FakeJetStreamServer);
    expect(Object.keys(await import("../src/mesh/state-backend-nats.js"))).not.toContain("connectJetStream");
  });
});

describe("NATS state snapshots and purge: bounded replay (smarty-dev#7504)", () => {
  const replicaOf = (server: FakeJetStreamServer) =>
    NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });

  it("a snapshot purges the stream below it; a host opening later replays from it, not from sequence 1", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    for (let index = 0; index < 20; index++) await backend.put({ key: `s/${index % 7}`, value: index, identity });
    await backend.delete({ key: "s/3" });
    const versionOf4 = backend.get("s/4")!.version;
    expect(await backend.compact()).toBe(true);
    expect(server.firstSeq).toBe(22); // 21 batches purged; the chunk, the end record remain
    await backend.put({ key: "s/after", value: "x", identity });
    const port = server.port();
    const replica = await NatsStateBackend.open({ port, stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });
    expect(replica.listAll("s/")).toEqual(backend.listAll("s/"));
    expect(replica.diagnostics().nats).toMatchObject({ appliedSeq: server.messages.length, snapshotSeq: 23 });
    // The tombstone survives the snapshot: a put that expects "never seen" still conflicts.
    await expect(replica.put({ key: "s/3", value: 1, identity, ifVersion: 0 })).rejects.toThrow(/compare-and-swap failed for s\/3/);
    await replica.put({ key: "s/4", value: "r", identity, ifVersion: versionOf4 });
    replica.close();
  });

  it("a large model spans several chunks and round-trips exactly", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    const big = "\"q\\".repeat(30_000);
    for (let index = 0; index < 6; index++) await backend.put({ key: `big/${index}`, value: { big, index }, identity });
    expect(await backend.compact()).toBe(true);
    const end = JSON.parse(Buffer.from(server.messages.at(-1)!.data).toString()) as { type: string; chunks: number };
    expect(end).toMatchObject({ type: "snapshot-end" });
    expect(end.chunks).toBeGreaterThan(3);
    const replica = await replicaOf(server);
    expect(replica.listAll("big/")).toEqual(backend.listAll("big/"));
    replica.close();
  });

  it("a writer committing inside the snapshot chain abandons the snapshot: no purge, no lost write", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    const other = await replicaOf(server);
    for (let index = 0; index < 5; index++) await backend.put({ key: `r/${index}`, value: index, identity });
    expect(await backend.compact()).toBe(true);
    await backend.put({ key: "r/5", value: 5, identity });
    let publishes = 0;
    server.beforePublish = () => {
      if (++publishes !== 2) return;
      // The other writer's batch lands after the snapshot's chunk, before its end record.
      server.append(encode({ format: 1, type: "batch", id: "other", base: server.messages.length, highWater: 10_000,
        changes: [{ key: "r/other", put: { value: 1, version: 10_000, updatedAt: 1, updatedBy: identity } }] }));
    };
    expect(await backend.compact()).toBe(false);
    delete server.beforePublish;
    const floor = server.firstSeq;
    await backend.put({ key: "r/next", value: 1, identity });
    expect(backend.get("r/other")?.value).toBe(1);
    // No purge happened; the abandoned chunk is inert: a host opening now replays from the older, complete snapshot.
    expect(server.firstSeq).toBe(floor);
    const before = await replicaOf(server);
    expect(before.listAll("r/")).toEqual(backend.listAll("r/"));
    before.close();
    expect(await backend.compact()).toBe(true);
    const replica = await replicaOf(server);
    expect(replica.listAll("r/")).toEqual(backend.listAll("r/"));
    replica.close(); other.close();
  });

  it("a purge racing a writer loses nothing: the write after the snapshot stays above the purge", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    const writer = await replicaOf(server);
    for (let index = 0; index < 5; index++) await backend.put({ key: `w/${index}`, value: index, identity });
    let racing: Promise<unknown> | undefined;
    server.beforePurge = () => { racing = writer.put({ key: "w/racer", value: "kept", identity }); };
    expect(await backend.compact()).toBe(true);
    await racing;
    await backend.put({ key: "w/last", value: 1, identity });
    expect(backend.get("w/racer")?.value).toBe("kept");
    const replica = await replicaOf(server);
    expect(replica.listAll("w/")).toEqual(backend.listAll("w/"));
    expect(replica.get("w/racer")?.value).toBe("kept");
    replica.close(); writer.close();
  });

  it("a replica that lagged behind a purge reloads from the snapshot and converges", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    await backend.put({ key: "l/0", value: 0, identity });
    const replicaPort = server.port();
    const replica = await NatsStateBackend.open({ port: replicaPort, stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });
    replicaPort.pause(); // only the replica's deliveries stall
    for (let index = 1; index < 6; index++) await backend.put({ key: `l/${index}`, value: index, identity });
    expect(await backend.compact()).toBe(true);
    await backend.put({ key: "l/after", value: 1, identity });
    replicaPort.resume(); // its queued messages below the snapshot are gone: a gap
    for (let tries = 0; tries < 200 && replica.diagnostics().nats!.appliedSeq < server.messages.length; tries++) await settle();
    expect(replica.diagnostics().nats).toMatchObject({ appliedSeq: server.messages.length, resyncs: 1 });
    expect(replica.listAll("l/")).toEqual(backend.listAll("l/"));
    await replica.put({ key: "l/from-replica", value: 1, identity });
    expect(backend.get("l/from-replica")?.value).toBe(1);
    replica.close();
  });

  it("a lagging replica that installs a newer snapshot on re-sync notifies its state listeners once for the install", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    await backend.put({ key: "n/0", value: 0, identity });
    const replicaPort = server.port();
    const replica = await NatsStateBackend.open({ port: replicaPort, stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });
    const originalSeq = replica.diagnostics().nats!.appliedSeq;
    let calls = 0;
    const unsubscribe = replica.subscribeStateChanges(() => { calls++; });
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    let atInstall: { calls: number; keys: string[] } | undefined;
    try {
      replicaPort.pause();
      for (let index = 1; index < 4; index++) await backend.put({ key: `n/${index}`, value: index, identity });
      expect(await backend.compact()).toBe(true);
      await backend.put({ key: "n/after", value: 1, identity });
      // Hold the re-sync's replacement consumer: only the snapshot install has happened, no replay can mask it.
      server.beforeConsume = async afterSeq => {
        if (afterSeq <= originalSeq || atInstall) return;
        await settle();
        atInstall = { calls, keys: replica.listAll("n/").map(entry => entry.key) };
        await held;
      };
      expect(calls).toBe(0);
      replicaPort.resume(); // the purge leaves a gap: the replica re-syncs from the snapshot
      for (let tries = 0; tries < 200 && !atInstall; tries++) await settle();
      expect(atInstall).toEqual({ calls: 1, keys: ["n/0", "n/1", "n/2", "n/3"] });
      release();
      for (let tries = 0; tries < 200 && replica.diagnostics().nats!.appliedSeq < server.messages.length; tries++) await settle();
      expect(replica.listAll("n/")).toEqual(backend.listAll("n/"));
    } finally {
      release();
      delete server.beforeConsume;
      unsubscribe();
      replica.close();
    }
  });

  it("a lost ack whose stored batch was purged before the lookup is an unknown outcome, never a rerun", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    await backend.put({ key: "u/count", value: 0, identity });
    let lose = true;
    server.beforePublish = () => { if (lose) { lose = false; return "lose-ack"; } };
    server.afterStore = seq => {
      delete server.afterStore;
      server.append(encode({ format: 1, type: "batch", id: "other", base: server.messages.length, highWater: 90_000, changes: [] }));
      void server.port().purge("S", seq + 2);
    };
    let prepares = 0;
    await expect(backend.writeBatch({ identity, ops: [], prepare: view => { prepares++; return [{ kind: "put", key: "u/count", value: (view.get("u/count")?.value as number) + 1 }]; } }))
      .rejects.toBeInstanceOf(MeshStateNatsOutcomeUnknownError);
    expect(prepares).toBe(1);
  });

  it("refuses a stream purged without a snapshot (it cannot be replayed)", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    await backend.put({ key: "z/a", value: 1, identity });
    await backend.put({ key: "z/b", value: 1, identity });
    await server.port().purge("S", 2);
    await expect(replicaOf(server)).rejects.toBeInstanceOf(MeshStateNatsRefusedError);
  });

  it("snapshotEvery takes a snapshot in the background after a write", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 5 });
    for (let index = 0; index < 6; index++) await backend.put({ key: `e/${index}`, value: index, identity });
    for (let tries = 0; tries < 50 && backend.diagnostics().nats!.snapshots === 0; tries++) await settle();
    expect(backend.diagnostics().nats!.snapshots).toBe(1);
    expect(server.firstSeq).toBeGreaterThan(1);
    const replica = await replicaOf(server);
    expect(replica.listAll("e/")).toEqual(backend.listAll("e/"));
    replica.close();
  });
});

describe("NATS state snapshots: integrity, bounds, repeated purges (pi-fabric#907 security round 1)", () => {
  type Options = Partial<Parameters<typeof NatsStateBackend.open>[0]>;
  const replicaOf = (server: FakeJetStreamServer, extra: Options = {}) =>
    NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20, ...extra });
  const record = (server: FakeJetStreamServer, seq: number) => JSON.parse(Buffer.from(server.messages[seq - 1]!.data).toString()) as Record<string, unknown>;
  const rewrite = (server: FakeJetStreamServer, seq: number, change: (value: Record<string, unknown>) => void) => {
    const value = record(server, seq);
    change(value);
    server.messages[seq - 1]!.data = new Uint8Array(Buffer.from(JSON.stringify(value)));
  };
  /** A stream with a 3-chunk snapshot; returns the sequences of its chunks and its end record. */
  const snapshotted = async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    const big = "x".repeat(50_000);
    for (let index = 0; index < 4; index++) await backend.put({ key: `i/${index}`, value: { big, index }, identity });
    await backend.put({ key: "i/epoch", value: 1, identity });
    expect(await backend.compact()).toBe(true);
    const end = server.messages.length;
    const chunks = (record(server, end) as { chunks: number }).chunks;
    expect(chunks).toBeGreaterThanOrEqual(3);
    return { server, backend, end, first: end - chunks, chunks };
  };

  it("an intact snapshot loads (the counter-case)", async () => {
    const { server, backend } = await snapshotted();
    const replica = await replicaOf(server);
    expect(replica.listAll("i/")).toEqual(backend.listAll("i/"));
    replica.close();
  });

  it("refuses a snapshot with one altered byte in a chunk", async () => {
    const { server, first } = await snapshotted();
    rewrite(server, first + 1, value => { value.part = (value.part as string).replace("xxxx", "xxxy"); });
    await expect(replicaOf(server)).rejects.toBeInstanceOf(MeshStateNatsSnapshotIntegrityError);
  });

  it("refuses a snapshot with a dropped chunk", async () => {
    const { server, first } = await snapshotted();
    server.hidden.add(first + 1);
    await expect(replicaOf(server)).rejects.toThrow(/a chunk of the snapshot ending at \d+ is missing/);
  });

  it("refuses a forged chunk with valid ids (the same snapshot id and index)", async () => {
    const { server, first } = await snapshotted();
    const length = (record(server, first + 1).part as string).length;
    rewrite(server, first + 1, value => { value.part = "y".repeat(length); });
    await expect(replicaOf(server)).rejects.toThrow(/does not match its digest/);
  });

  it("refuses a header with a wrong epoch (or status): the lifecycle is bound into the header", async () => {
    const { server, end } = await snapshotted();
    rewrite(server, end, value => { (value.header as { epoch: number }).epoch = 7; });
    // Round 2: the epoch is bound into the digest, so the altered header fails the digest itself.
    await expect(replicaOf(server)).rejects.toThrow(/does not match its digest/);
    rewrite(server, end, value => { (value.header as { epoch: number; status: string }).epoch = 0; (value.header as { status: string }).status = "importing"; });
    await expect(replicaOf(server)).rejects.toBeInstanceOf(MeshStateNatsSnapshotIntegrityError);
  });

  it("a live replica fails closed on a published snapshot that misstates the stream, or breaks the chain", async () => {
    const { server, backend, end } = await snapshotted();
    const replica = await replicaOf(server);
    // A second, well-formed snapshot of a DIFFERENT state (its digest is self-consistent): every live projector refuses it.
    const forged = JSON.stringify({ entries: [], versions: [], tombstones: [], highWater: 1, status: "ready", epoch: 0 });
    const digest = (await import("node:crypto")).createHash("sha256").update(forged).digest("hex");
    const base = server.messages.length;
    server.append(encode({ format: 1, type: "snapshot-chunk", snapshot: "forged", index: 0, part: forged }));
    server.append(encode({ format: 1, type: "snapshot-end", snapshot: "forged", base, first: base + 1, chunks: 1,
      header: { digest, prev: (record(server, end).header as { digest: string }).digest, bytes: forged.length, entries: 0, status: "ready", epoch: 0 } }), "S.snapshot");
    await settle(); await settle();
    expect(() => replica.get("i/0")).toThrow(MeshStateNatsSnapshotIntegrityError);
    expect(() => backend.get("i/0")).toThrow(MeshStateNatsSnapshotIntegrityError);
    // The documented boundary: a WHOLE snapshot forged self-consistently by a stream writer (the same trust as
    // forging a batch) is caught by every live projector, as above, not by a host that opens only after it.
    replica.close();
  });

  it("bounds: refuses a snapshot over the chunk, byte or entry limit, a forged oversized chunk, and a long replay", async () => {
    const { server, first } = await snapshotted();
    // 3+ chunks of ~66 KB: a 100 KB state bound allows 2 chunks and ~100 KB.
    await expect(replicaOf(server, { maxStateBytes: 100_000 })).rejects.toThrow(/chunks \(limit 2\)/);
    await expect(replicaOf(server, { maxStateBytes: 200_000 })).rejects.toThrow(/bytes \(limit 200000\)/);
    await expect(replicaOf(server, { maxStateBytes: 64 })).rejects.toBeInstanceOf(MeshStateNatsLimitError);
    // entries: 5 entries; a bound whose entry limit is 4 (4 × 32 bytes) but large enough for the bytes is impossible
    // by construction, so check the entry bound on the header directly with a forged count.
    const end = server.messages.length;
    rewrite(server, end, value => { (value.header as { entries: number }).entries = 10_000_000; });
    await expect(replicaOf(server)).rejects.toThrow(/entries \(limit/);
    rewrite(server, end, value => { (value.header as { entries: number }).entries = 5; });
    rewrite(server, first, value => { value.part = (value.part as string) + "z".repeat(64 * 1024); });
    await expect(replicaOf(server)).rejects.toThrow(/characters \(limit 65536\)/);
    // The replay after the snapshot.
    const fresh = fakeJetStream();
    const writer = await open(fresh, { snapshotEvery: 0 });
    for (let index = 0; index < 12; index++) await writer.put({ key: `r/${index}`, value: index, identity });
    await expect(replicaOf(fresh, { maxReplay: 10 })).rejects.toThrow(/replay 12 messages after sequence 0 \(limit 10\)/);
    await expect(replicaOf(fresh, { maxReplay: 12 })).resolves.toBeInstanceOf(NatsStateBackend);
  });

  it("a second purge between the snapshot load and the consumer start is not dropped: the re-sync loops and converges", async () => {
    const server = fakeJetStream();
    const writer = await open(server, { snapshotEvery: 0 });
    await writer.put({ key: "g/0", value: 0, identity });
    const replicaPort = server.port();
    const replica = await NatsStateBackend.open({ port: replicaPort, stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });
    replicaPort.pause();
    for (let index = 1; index < 4; index++) await writer.put({ key: `g/${index}`, value: index, identity });
    expect(await writer.compact()).toBe(true);
    await writer.put({ key: "g/after-1", value: 1, identity });
    // The replica's re-sync loads snapshot 1; before its consumer starts, the writer snapshots and purges again.
    let injected = 0;
    server.beforeConsume = async () => {
      if (injected++ > 0) return;
      await writer.put({ key: "g/after-2", value: 2, identity });
      expect(await writer.compact()).toBe(true);
      await writer.put({ key: "g/after-3", value: 3, identity });
    };
    replicaPort.resume();
    for (let tries = 0; tries < 300 && replica.diagnostics().nats!.appliedSeq < server.messages.length; tries++) await settle();
    expect(replica.diagnostics().nats).toMatchObject({ appliedSeq: server.messages.length, resyncing: false });
    expect(replica.diagnostics().nats!.resyncs).toBeGreaterThanOrEqual(2);
    expect(replica.listAll("g/")).toEqual(writer.listAll("g/"));
    replica.close();
  });

  it("purges that keep outrunning the re-sync make the backend unavailable: reads and writes fail, never stale", async () => {
    const server = fakeJetStream();
    const writer = await open(server, { snapshotEvery: 0 });
    await writer.put({ key: "h/0", value: 0, identity });
    const replicaPort = server.port();
    const replica = await NatsStateBackend.open({ port: replicaPort, stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20 });
    replicaPort.pause();
    await writer.put({ key: "h/1", value: 1, identity });
    expect(await writer.compact()).toBe(true);
    await writer.put({ key: "h/2", value: 2, identity });
    let round = 0;
    server.beforeConsume = async () => {
      await writer.put({ key: `h/round-${round++}`, value: round, identity });
      expect(await writer.compact()).toBe(true);
      await writer.put({ key: `h/later-${round}`, value: round, identity });
    };
    replicaPort.resume();
    for (let tries = 0; tries < 300 && replica.diagnostics().nats!.failure === undefined; tries++) await settle();
    expect(replica.diagnostics().nats!.failure).toMatch(/FABRIC_MESH_STATE_NATS_UNAVAILABLE/);
    expect(() => replica.get("h/0")).toThrow(MeshStateNatsUnavailableError);
    await expect(replica.put({ key: "h/x", value: 1, identity })).rejects.toBeInstanceOf(MeshStateNatsUnavailableError);
    delete server.beforeConsume;
    replica.close();
  });
});

describe("NATS state snapshots: review round 2 (pi-fabric#907)", () => {
  type Options = Partial<Parameters<typeof NatsStateBackend.open>[0]>;
  const replicaOf = (server: FakeJetStreamServer, extra: Options = {}) =>
    NatsStateBackend.open({ port: server.port(), stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20, ...extra });
  const json = (server: FakeJetStreamServer, seq: number) => JSON.parse(Buffer.from(server.messages[seq - 1]!.data).toString()) as Record<string, unknown>;
  const rewrite = (server: FakeJetStreamServer, seq: number, change: (value: Record<string, unknown>) => void) => {
    const value = json(server, seq);
    change(value);
    server.messages[seq - 1]!.data = new Uint8Array(Buffer.from(JSON.stringify(value)));
  };
  const snapshotted = async (extra: Partial<Parameters<typeof openNatsStateBackend>[0]> = {}) => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0, ...extra });
    for (let index = 0; index < 4; index++) await backend.put({ key: `v/${index}`, value: { index, pad: "p".repeat(30_000) }, identity });
    expect(await backend.compact()).toBe(true);
    return { server, backend, end: server.messages.length };
  };

  it("the digest binds the header: a valid body under an altered prev, or an altered base, is refused", async () => {
    const { server, end } = await snapshotted();
    const valid = json(server, end);
    rewrite(server, end, value => { (value.header as { prev: string }).prev = "a".repeat(64); });
    await expect(replicaOf(server)).rejects.toThrow(/does not match its digest/);
    server.messages[end - 1]!.data = new Uint8Array(Buffer.from(JSON.stringify(valid)));
    rewrite(server, end, value => { value.base = (value.base as number) - 1; value.first = (value.first as number) - 1; });
    await expect(replicaOf(server)).rejects.toBeInstanceOf(MeshStateNatsSnapshotIntegrityError);
    server.messages[end - 1]!.data = new Uint8Array(Buffer.from(JSON.stringify(valid)));
    await expect(replicaOf(server)).resolves.toBeInstanceOf(NatsStateBackend); // the counter-case
  });

  it("a live projector refuses an end record whose sequence fields do not match where it is (a replayed valid end record)", async () => {
    const { server, backend, end } = await snapshotted();
    server.append(server.messages[end - 1]!.data, "S.snapshot"); // the same, valid, end record again, later
    await settle(); await settle();
    expect(() => backend.get("v/0")).toThrow(MeshStateNatsSnapshotIntegrityError);
  });

  it("an emoji across the 64 Ki chunk boundary round-trips byte-identical, and the chunk byte counts sum to the body", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    // The body starts `{"entries":[{"key":"e/a","value":"` (34 code units): the emoji's high surrogate lands at 65535,
    // the last unit of chunk 0, so a plain slice would split the pair.
    const value = `${"x".repeat(65_535 - 34)}\u{1F600}${"y".repeat(10)}`;
    await backend.put({ key: "e/a", value, identity });
    expect(await backend.compact()).toBe(true);
    const end = server.messages.length;
    const header = json(server, end).header as { bytes: number };
    const chunks = server.messages.slice(-1 - (json(server, end) as { chunks: number }).chunks, -1).map(message => (JSON.parse(Buffer.from(message.data).toString()) as { part: string }).part);
    expect(chunks[0]!.length).toBe(65_535); // moved back off the high surrogate
    expect(chunks.reduce((sum, part) => sum + Buffer.byteLength(part, "utf8"), 0)).toBe(header.bytes);
    expect(chunks.join("")).toContain("\u{1F600}");
    const replica = await replicaOf(server);
    expect(replica.get("e/a")?.value).toBe(value);
    replica.close();
  });

  it("a live end record claiming 2^52 bytes is refused against the configured bound, before any entry is serialized", async () => {
    const { server, backend, end } = await snapshotted();
    const forged = json(server, end);
    (forged.header as { bytes: number }).bytes = 2 ** 52;
    const stringify = vi.spyOn(JSON, "stringify");
    server.append(new Uint8Array(Buffer.from(JSON.stringify(forged))), "S.snapshot");
    const before = stringify.mock.calls.length;
    await settle(); await settle();
    // No entry of the model was serialized for it (an entry is an object with a `key`).
    expect(stringify.mock.calls.slice(before).filter(([value]) => typeof (value as { key?: unknown })?.key === "string")).toHaveLength(0);
    stringify.mockRestore();
    expect(() => backend.get("v/0")).toThrow(MeshStateNatsLimitError);
    expect(backend.diagnostics().nats?.failure).toMatch(/claims 4503599627370496 bytes/);
  });

  it("compact() verifies what the stream stored before it purges: a corrupt publish leaves the stream unpurged", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0 });
    for (let index = 0; index < 3; index++) await backend.put({ key: `c/${index}`, value: index, identity });
    server.beforePublish = data => {
      const text = Buffer.from(data).toString();
      if (text.includes('"snapshot-chunk"')) data.set(Buffer.from(text.replace('\\"c/1\\"', '\\"c/9\\"')));
    };
    expect(await backend.compact()).toBe(false);
    delete server.beforePublish;
    expect(server.firstSeq).toBe(1); // nothing purged: the full history stays
    await expect(replicaOf(server)).rejects.toBeInstanceOf(MeshStateNatsSnapshotIntegrityError); // and nobody loads it
  });

  it("compact() refuses an over-bound state while serializing it, never building its whole string", async () => {
    const server = fakeJetStream();
    const backend = await open(server, { snapshotEvery: 0, maxStateBytes: 4_000 });
    for (let index = 0; index < 200; index++) await backend.put({ key: `o/${String(index).padStart(3, "0")}`, value: "v".repeat(80), identity });
    const stringify = vi.spyOn(JSON, "stringify");
    expect(await backend.compact()).toBe(false);
    // ~40 entries fit in 4 000 bytes: the serializer stopped there, not after all 200.
    expect(stringify.mock.calls.length).toBeLessThan(60);
    stringify.mockRestore();
    expect(server.firstSeq).toBe(1);
  });

  it("the replay cap counts writes made during the snapshot load, and refuses over it (LIMIT)", async () => {
    const { server } = await snapshotted();
    const port = server.port();
    let appended = false;
    const racing = { ...port, lastBySubject: async (stream: string, subject: string) => {
      const found = await port.lastBySubject(stream, subject);
      if (!appended) {
        appended = true;
        for (let index = 0; index < 15; index++) server.append(encode({ format: 1, type: "batch", id: `w${index}`, base: server.messages.length,
          highWater: 90_000 + index, changes: [{ key: `w/${index}`, put: { value: index, version: 90_000 + index, updatedAt: 1, updatedBy: identity } }] }));
      }
      return found;
    } };
    await expect(NatsStateBackend.open({ port: racing, stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20, maxReplay: 10 }))
      .rejects.toThrow(/would replay \d+ messages after sequence \d+ \(limit 10\)/); // the 15 writes, plus the snapshot's own records
  });

  it("a re-sync is bounded by the same replay cap: over it, the backend fails closed (LIMIT)", async () => {
    const server = fakeJetStream();
    const writer = await open(server, { snapshotEvery: 0 });
    await writer.put({ key: "q/0", value: 0, identity });
    const replicaPort = server.port();
    const replica = await NatsStateBackend.open({ port: replicaPort, stream: "S", subject: "S.state", snapshotSubject: "S.snapshot", maxEventBytes: 1 << 20, maxReplay: 10 });
    replicaPort.pause();
    await writer.put({ key: "q/1", value: 1, identity });
    expect(await writer.compact()).toBe(true);
    for (let index = 0; index < 15; index++) await writer.put({ key: `q/later-${index}`, value: index, identity });
    replicaPort.resume();
    for (let tries = 0; tries < 300 && replica.diagnostics().nats!.failure === undefined; tries++) await settle();
    expect(replica.diagnostics().nats!.failure).toMatch(/FABRIC_MESH_STATE_NATS_LIMIT: the projector would replay \d+ messages .*\(limit 10\)/);
    expect(() => replica.get("q/0")).toThrow(MeshStateNatsLimitError);
    replica.close();
  });

  it("bounds a chunk's RAW size before decoding it", async () => {
    const { server, end } = await snapshotted();
    const first = end - (json(server, end) as { chunks: number }).chunks;
    server.messages[first - 1]!.data = new Uint8Array(500_000).fill(0x7b); // not even JSON: never decoded
    await expect(replicaOf(server)).rejects.toThrow(/bytes before decoding \(limit \d+\)/);
  });

  it("the server enforces the message cap: a new stream gets max_msg_size; a looser stream or max_payload refuses", async () => {
    const created = fakeJetStream();
    await open(created);
    expect(created.streamMaxMsgSize).toBe(MAX_MESSAGE_BYTES);
    for (const loose of [-1, 4 * MAX_MESSAGE_BYTES]) {
      const server = fakeJetStream();
      server.streamMaxMsgSize = loose;
      await expect(open(server)).rejects.toThrow(/allows messages of .* \(limit 1048576\); set its max_msg_size/);
    }
    const payload = fakeJetStream();
    payload.maxPayload = 8 * MAX_MESSAGE_BYTES;
    await expect(open(payload)).rejects.toThrow(/max_payload is 8388608 bytes/);
    const backend = await open(fakeJetStream(), { maxEventBytes: 4 * MAX_MESSAGE_BYTES });
    await expect(backend.put({ key: "big/a", value: "x".repeat(MAX_MESSAGE_BYTES + 10), identity })).rejects.toBeInstanceOf(MeshStateNatsLimitError);
  });

  it("refuses a non-loopback server that does not require authentication; loopback, or auth required, opens", async () => {
    const open_ = (server: FakeJetStreamServer, servers: string[]) => openNatsStateBackend({ root: root(), port: server.port(), servers, lookup: resolver });
    await expect(open_(fakeJetStream(), ["tls://nats.example.com:4222"])).rejects.toThrow(/does not require authentication/);
    const authed = fakeJetStream();
    authed.authRequired = true;
    await expect(open_(authed, ["tls://nats.example.com:4222"])).resolves.toBeInstanceOf(NatsStateBackend);
    await expect(open_(fakeJetStream(), ["nats://127.0.0.1:4222"])).resolves.toBeInstanceOf(NatsStateBackend);
  });
});

describe("JetStream backend P2s (smarty-dev#9663)", () => {
  it("item 1: exempts plaintext only when every resolved address is loopback, and pins the connection to it", async () => {
    const offHost = async () => [{ address: "10.0.0.5", family: 4 }];
    const mixed = async () => [{ address: "127.0.0.1", family: 4 }, { address: "10.0.0.5", family: 4 }];
    const failing = async (): Promise<{ address: string; family: number }[]> => { throw new Error("ENOTFOUND"); };
    for (const lookup of [offHost, mixed, failing]) {
      await expect(assertNatsTransportSecure("nats://localhost:4222", "tls:", lookup)).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
      await expect(openNatsStateBackend({ root: root(), servers: ["nats://localhost:4222"], lookup })).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
    }
    // The name is never trusted for itself: what it resolves to is, and the URL then names that address.
    expect(await assertNatsTransportSecure("nats://localhost:4222", "tls:", async () => [{ address: "127.0.0.1", family: 4 }]))
      .toBe("nats://127.0.0.1:4222");
    expect(await assertNatsTransportSecure("http://localhost:8222", "https:", async () => [{ address: "::1", family: 6 }]))
      .toBe("http://[::1]:8222/");
  });

  it("item 2: an error never carries URL userinfo or query tokens", async () => {
    const url = "nats://alice:s3cret@10.0.0.5:4222/?token=abc123";
    const error = await openNatsStateBackend({ root: root(), servers: [url], lookup: resolver }).catch(caught => caught as Error);
    expect(error).toBeInstanceOf(MeshStateNatsInsecureError);
    for (const secret of ["alice", "s3cret", "abc123"]) {
      expect(String(error)).not.toContain(secret);
      expect((error as MeshStateNatsInsecureError).url).not.toContain(secret);
    }
    // The client's connect and /varz errors go through the same redaction.
    const clientMessage = redactNatsUrls("connect ECONNREFUSED nats://bob:pw9@127.0.0.1:4222 (https://mon:pw8@10.0.0.6:8222/varz?auth=t0k)");
    for (const secret of ["bob", "pw9", "mon", "pw8", "t0k"]) expect(clientMessage).not.toContain(secret);
    expect(redactNatsUrl("tls://user:pass@nats.example.com:4222")).toBe("tls://***@nats.example.com:4222");
  });

  it("item 3: the stream is always the root's hashed one; no override can make two roots share it", async () => {
    const server = fakeJetStream();
    const first = root();
    const second = root();
    const override = { stream: "SHARED" } as unknown as Partial<Parameters<typeof openNatsStateBackend>[0]>;
    await openNatsStateBackend({ ...override, root: first, port: server.port() });
    await openNatsStateBackend({ ...override, root: second, port: server.port() });
    expect([...server.streams].sort()).toEqual([natsStreamName(first), natsStreamName(second)].sort());
    expect(server.streams.has("SHARED")).toBe(false);
  });

  it("item 4: refuses a root whose state is in a stream under the old (c03c829f) name, never replaces it", async () => {
    const server = fakeJetStream();
    const at = root();
    const legacy = `FABRIC_MESH_${Buffer.from(path.resolve(at)).toString("base64url").replace(/[^A-Za-z0-9_]/g, "_").slice(-48)}`;
    server.streams.add(legacy);
    const port = server.port();
    await expect(openNatsStateBackend({ root: at, port })).rejects.toThrow(new RegExp(`${legacy}.*old \\(c03c829f\\) name`));
    expect(port.calls).not.toContain("ensureStream"); // the hashed stream is never created beside it
    expect(server.streams.has(natsStreamName(at))).toBe(false);
  });

  it("item 5: refuses a server older than 2.14.7, accepts 2.14.7", async () => {
    for (const version of ["2.14.6", "v2.11.0", "2.13.99"]) {
      const server = fakeJetStream();
      server.version = version;
      await expect(open(server)).rejects.toThrow(/older than 2\.14\.7/);
    }
    const server = fakeJetStream();
    server.version = "2.14.7";
    await expect(open(server)).resolves.toBeInstanceOf(NatsStateBackend);
  });
});

describe("JetStream backend: round 1 (pi-fabric#937)", () => {
  const secrets = ["alice", "s3cret", "tok123", "frag456"];
  /** Every text an error can show: String(), message, stack, url and address fields, its cause chain. */
  const texts = (error: unknown, depth = 0): string[] => {
    if (!(error instanceof Error) || depth > 5) return [String(error)];
    const record = error as unknown as Record<string, unknown>;
    return [String(error), error.message, error.stack ?? "", String(record.url ?? ""), String(record.address ?? ""),
      ...("cause" in error && error.cause !== undefined ? texts(error.cause, depth + 1) : [])];
  };
  const expectNoSecret = (values: string[]) => { for (const value of values) for (const secret of secrets) expect(value).not.toContain(secret); };
  const credentialed = "nats://alice:s3cret@10.0.0.5:4222/?token=tok123#token=frag456";
  const leaky = (label: string) => Object.assign(new Error(`${label} failed at ${credentialed}`),
    { code: "E_LEAKY", url: credentialed, address: "tls://alice:s3cret@10.0.0.6:4222", cause: new Error(`inner ${credentialed}`) });

  it("SEC P1-1: no fragment, query, userinfo or unparseable-URL credential reaches MeshStateNatsInsecureError", async () => {
    for (const url of ["nats://10.0.0.5:4222/#token=frag456", "nats://10.0.0.5:4222/?token=tok123", "nats://alice:s3cret@10.0.0.5:4222",
      "nats://alice:s3cret@[bad/?token=tok123#frag456"]) {
      const error = await openNatsStateBackend({ root: root(), servers: [url], lookup: resolver }).catch(caught => caught as Error);
      expect(error, url).toBeInstanceOf(MeshStateNatsInsecureError);
      expectNoSecret([...texts(error), (error as MeshStateNatsInsecureError).url]);
    }
    expect(redactNatsUrl("tls://nats.example.com:4222/#token=frag456")).toBe("tls://nats.example.com:4222/#***");
    expect(redactNatsUrl("nats://alice:s3cret@[bad/?token=tok123")).toBe("nats://*** (unparseable URL)");
    expect(redactNatsUrls(`connect to ${credentialed} failed`)).not.toMatch(/alice|s3cret|tok123|frag456/);
  });

  it("SEC P1-2: the client port's stream, publish and consumer errors leave it redacted, class and code kept", async () => {
    // Stream operation (open): the error the port throws.
    const streamServer = fakeJetStream();
    const streamPort = streamServer.port();
    streamPort.ensureStream = async () => { throw leaky("ensureStream"); };
    const opened = await openNatsStateBackend({ root: root(), port: guardNatsPort(streamPort) }).catch(caught => caught as Error);
    expect(opened).toBeInstanceOf(Error);
    expect((opened as Error & { code?: string }).code).toBe("E_LEAKY");
    expectNoSecret(texts(opened));

    // Publish: a final (not transient) error keeps its class.
    class PublishFailure extends Error {}
    const server = fakeJetStream();
    const port = server.port();
    const publish = port.publish.bind(port);
    let failPublish = false;
    port.publish = async (...args) => {
      if (failPublish) throw Object.assign(new PublishFailure(`publish failed at ${credentialed}`), { url: credentialed, cause: leaky("ack") });
      return publish(...args);
    };
    const backend = await openNatsStateBackend({ root: root(), port: guardNatsPort(port) });
    await backend.put({ key: "x/a", value: 1, identity });
    failPublish = true;
    const published = await backend.put({ key: "x/b", value: 1, identity }).catch(caught => caught as Error);
    expect(published).toBeInstanceOf(Error);
    expectNoSecret(texts(published));

    // Consumer: the error the port hands to onError, then every read, write and diagnostics line.
    failPublish = false;
    server.breakDelivery(leaky("consumer"));
    const read = (() => { try { backend.get("x/a"); return undefined; } catch (caught) { return caught as Error; } })();
    expect(read).toBeInstanceOf(Error);
    expect((read as Error & { code?: string }).code).toBe("E_LEAKY");
    expectNoSecret(texts(read));
    expectNoSecret(texts(await backend.put({ key: "x/c", value: 1, identity }).catch(caught => caught as Error)));
    expectNoSecret([String(backend.diagnostics().nats?.failure)]);
  });

  it("the version floor without the semver package (pi-fabric#937 CI): 2.14.10 passes 2.14.7, 2.9.99 refuses, and it agrees with semver", async () => {
    for (const version of ["2.14.10", "2.14.100", "2.20.0", "10.0.0"]) {
      const server = fakeJetStream();
      server.version = version;
      await expect(open(server), version).resolves.toBeInstanceOf(NatsStateBackend);
    }
    for (const version of ["2.9.99", "2.13.99", "1.99.99", "2.14.6"]) {
      const server = fakeJetStream();
      server.version = version;
      await expect(open(server), version).rejects.toThrow(/older than 2\.14\.7/);
    }
    // Parity with the semver package (the rule this replaced), on the edges a hand parser gets wrong.
    const corpus = ["2.14.7", "v2.14.7", "vv2.14.7", " 2.14.7 ", "2.14.7+build.1", "2.14.7-rc.1", "2.14.7-rc.01", "2.14.7-0", "2.14.8-rc.1",
      "2.14.10", "2.9.99", "02.14.7", "2.014.7", "2.14.07", "2.14", "2.14.7.1", "2.14.7-", "2.14.7+", "2.14.7-a..b", "2.14.7-x-y.1",
      "3.0.0-alpha+exp.sha.5114f85", "2.15.0-rc.1", "9007199254740992.0.0", "", "garbage", `2.14.7-${"a".repeat(300)}`];
    for (const version of corpus) {
      const reference = semver.valid(version.replace(/^v/, ""));
      const expected = reference !== null && semver.gte(reference, "2.14.7");
      expect(natsVersionAtLeastForTest(version, MIN_SERVER), JSON.stringify(version)).toBe(expected);
    }
  });

  it("CODE P2: a prerelease is below its release: 2.14.7-rc.1 and -beta refused, 2.14.7 and 2.14.8 accepted", async () => {
    for (const version of ["2.14.7-rc.1", "2.14.7-beta", "v2.14.7-rc.1", "2.14", "garbage"]) {
      const server = fakeJetStream();
      server.version = version;
      await expect(open(server), version).rejects.toThrow(/older than 2\.14\.7/);
    }
    for (const version of ["2.14.7", "v2.14.7", "2.14.8", "2.15.0-rc.1", "3.0.0"]) {
      const server = fakeJetStream();
      server.version = version;
      await expect(open(server), version).resolves.toBeInstanceOf(NatsStateBackend);
    }
  });
});

describe("JetStream backend: round 2 (pi-fabric#937)", () => {
  const base32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  const seed = `SU${base32.repeat(2).slice(0, 56)}`;
  const userKey = `UA${base32.repeat(2).slice(3, 57)}`;
  const jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5In0.eyJzdWIiOiJVQUJDIn0.c2lnbmF0dXJlLXNlY3JldA";
  const creds = "-----BEGIN NATS USER JWT-----\nCREDSJWTBODY\n------END NATS USER JWT------\n\n" +
    "************************* IMPORTANT *************************\n-----BEGIN USER NKEY SEED-----\nCREDSSEEDBODY\n------END USER NKEY SEED------\n";
  const bare = [`token=tok111`, `auth_token: tok222`, `pass=pw333`, `password=pw444`, `nkey=nk555`, `jwt=jw666`, `seed=sd777`,
    `credentials=cr888`, `"token":"tok999"`, `user seed ${seed}`, `key ${userKey}`, `bearer ${jwt}`, creds].join(" ");
  const secrets = ["tok111", "tok222", "pw333", "pw444", "nk555", "jw666", "sd777", "cr888", "tok999", seed, userKey, jwt,
    "CREDSJWTBODY", "CREDSSEEDBODY", "alice", "s3cret"];
  const expectNoSecret = (label: string, values: unknown[]) => {
    for (const value of values) for (const secret of secrets) expect(String(value), `${label}: ${secret}`).not.toContain(secret);
  };
  const credentialed = "nats://alice:s3cret@10.0.0.5:4222";

  it("P1-1: a TLS hostname is remote whatever a later lookup says (DNS rebinding); plaintext stays pinned to its one lookup", async () => {
    // The client's own resolution (at connect) sees off-host; every later answer is loopback.
    let calls = 0;
    const rebinding = async (_host: string) => calls++ === 0 ? [{ address: "10.9.9.9", family: 4 }] : [{ address: "127.0.0.1", family: 4 }];
    await rebinding("nats.rebind.example"); // what the client got when it connected
    await expect(openNatsStateBackend({ root: root(), port: fakeJetStream().port(), servers: ["tls://nats.rebind.example:4222"], lookup: rebinding }))
      .rejects.toThrow(/does not require authentication/);
    await expect(openNatsStateBackend({ root: root(), port: fakeJetStream().port(), servers: ["tls://localhost:4222"], lookup: rebinding }))
      .rejects.toThrow(/does not require authentication/);
    // Still local: a loopback IP literal, and a plaintext name pinned (by its one lookup) to a loopback address.
    await expect(openNatsStateBackend({ root: root(), port: fakeJetStream().port(), servers: ["tls://127.0.0.1:4222"], lookup: rebinding }))
      .resolves.toBeInstanceOf(NatsStateBackend);
    await expect(openNatsStateBackend({ root: root(), port: fakeJetStream().port(), servers: ["nats://localhost:4222"], lookup: resolver }))
      .resolves.toBeInstanceOf(NatsStateBackend);
  });

  it("P1-2: bare tokens, passwords, nkeys, JWTs and .creds blocks never reach a message, a stack or diagnostics", async () => {
    expectNoSecret("redactNatsUrls", [redactNatsUrls(bare)]);
    const server = fakeJetStream();
    const backend = await openNatsStateBackend({ root: root(), port: guardNatsPort(server.port()) });
    await backend.put({ key: "x/a", value: 1, identity });
    server.breakDelivery(new Error(`consumer failed: ${bare}`));
    const read = (() => { try { backend.get("x/a"); return undefined; } catch (caught) { return caught as Error; } })();
    expect(read).toBeInstanceOf(Error);
    expect(read!.message).toContain("consumer failed");
    expectNoSecret("read", [String(read), read!.message, read!.stack]);
    expectNoSecret("diagnostics", [backend.diagnostics().nats?.failure]);
  });

  it("P1-3: AggregateError members, non-Error throws and apiError() leave the port redacted; prototype and flags kept", async () => {
    const thrown = async (value: unknown): Promise<Error & Record<string, unknown>> => {
      const port = fakeJetStream().port();
      port.purge = async () => { throw value; };
      return await guardNatsPort(port).purge("S", 1).then(() => { throw new Error("no throw"); }, caught => caught as Error & Record<string, unknown>);
    };
    const all = (error: Error & Record<string, unknown>) => [String(error), error.stack, JSON.stringify(error.cause), JSON.stringify(error.errors),
      JSON.stringify((error.errors as unknown[] | undefined)?.map(inner => [String(inner), (inner as Error).stack])),
      JSON.stringify(Object.entries(error)), typeof error.apiError === "function" ? JSON.stringify((error.apiError as () => unknown)()) : ""];

    const inner = Object.assign(new Error(`inner at ${credentialed} ${bare}`), { url: credentialed });
    const aggregate = await thrown(new AggregateError([inner, `string member token=tok111`], `all servers failed ${bare}`));
    expect(aggregate).toBeInstanceOf(AggregateError);
    expect((aggregate.errors as unknown[]).length).toBe(2);
    expectNoSecret("aggregate", all(aggregate));

    const fromString = await thrown(`connect ${credentialed} ${bare}`);
    expect(fromString).toBeInstanceOf(Error);
    expectNoSecret("string", all(fromString));

    const fromObject = await thrown({ url: credentialed, token: "tok111", nested: { password: "pw444" } });
    expect(fromObject).toBeInstanceOf(Error);
    expectNoSecret("object", all(fromObject));

    const api = Object.assign(new Error(`api error ${bare}`), { code: "500", apiError: () => ({ code: 500, err_code: 10059, description: `stream at ${credentialed} ${bare}` }) });
    const fromApi = await thrown(api);
    expect((fromApi.apiError as () => { err_code: number }).call(fromApi).err_code).toBe(10059);
    expect(fromApi.code).toBe("500");
    expectNoSecret("apiError", all(fromApi));

    // Kept: the class (the backend's CAS retry tests instanceof) and the number and boolean fields.
    const wrong = await thrown(Object.assign(new NatsWrongLastSequenceError(7, `at ${credentialed}`), { transient: true }));
    expect(wrong).toBeInstanceOf(NatsWrongLastSequenceError);
    expect([wrong.expected, wrong.transient, wrong.code]).toEqual([7, true, "FABRIC_MESH_NATS_WRONG_LAST_SEQUENCE"]);
    expectNoSecret("wrong", all(wrong));
  });

  it("the sanitizer is total: a throwing toString, getter or proxy never throws out of it", async () => {
    const hostile = { toString() { throw new Error(`toString ${credentialed}`); }, toJSON() { throw new Error("toJSON"); } };
    const port = fakeJetStream().port();
    const rejection = (pending: Promise<void>) =>
      pending.then(() => { throw new Error("no throw"); }, (caught: unknown) => caught as Error & { code?: unknown });
    port.purge = async () => { throw hostile; };
    const fromObject = await rejection(guardNatsPort(port).purge("S", 1));
    expect(fromObject).toBeInstanceOf(Error);
    expect(fromObject.message).toBe("[object Object]"); // round 3: an object is never stringified, toString never called
    port.purge = async () => { throw { get [Symbol.toStringTag]() { throw new Error(`tag ${credentialed}`); } }; };
    expect((await rejection(guardNatsPort(port).purge("S", 1))).message).toBe("[unprintable error]");

    const field = Object.assign(new Error("x"), { code: hostile });
    Object.defineProperty(field, "message", { get() { throw new Error(`getter ${credentialed}`); } });
    Object.defineProperty(field, "name", { value: hostile });
    port.purge = async () => { throw field; };
    const fromFields = await rejection(guardNatsPort(port).purge("S", 1));
    expect(fromFields).toBeInstanceOf(Error);
    expect([fromFields.name, fromFields.code]).toEqual(["[object Object]", "[object Object]"]);
    expectNoSecret("fields", [String(fromFields), fromFields.stack]);

    const trap = new Proxy(new Error("proxied"), { get() { throw new Error(`trap ${credentialed}`); }, ownKeys() { throw new Error("ownKeys"); } });
    port.purge = async () => { throw trap; };
    const fromProxy = await rejection(guardNatsPort(port).purge("S", 1));
    expect(fromProxy).toBeInstanceOf(Error);
    expectNoSecret("proxy", [String(fromProxy), fromProxy.stack]);

    // A non-Error value handed to onError (the consumer path) is sanitized the same way.
    const seen: unknown[] = [];
    const consuming = fakeJetStream().port();
    consuming.consume = async (_stream, _after, _onMessage, onError) => { onError?.(hostile); onError?.(`string ${credentialed} token=tok111`); return { stop: async () => undefined }; };
    await guardNatsPort(consuming).consume("S", 0, () => undefined, error => seen.push(error));
    expect(seen.map(error => (error as Error).message)).toEqual(["[object Object]", expect.stringContaining("string nats://")]);
    expectNoSecret("onError", seen.map(error => String(error)));
  });
});

describe("JetStream backend: round 3 (pi-fabric#937)", () => {
  const jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5In0.eyJzdWIiOiJVQUJDIn0.c2lnbmF0dXJlLXRocmVl";
  const secrets = ["abc999", "Zm9vOmJhcjk5", jwt, "ck111", "ck222", "sec333", "ak444", "px555"];
  const rejected = async (value: unknown): Promise<Error & Record<string, unknown>> => {
    const port = fakeJetStream().port();
    port.purge = async () => { throw value; };
    return await guardNatsPort(port).purge("S", 1).then(() => { throw new Error("no throw"); }, (caught: unknown) => caught as Error & Record<string, unknown>);
  };
  const expectNoSecret = (label: string, error: Error & Record<string, unknown>) => {
    const views = [String(error), error.stack, error.message, JSON.stringify(error), JSON.stringify(Object.entries(error)), JSON.stringify(error.cause)];
    for (const view of views) for (const secret of secrets) expect(String(view), `${label}: ${secret}`).not.toContain(secret);
  };

  it("P1: a non-Error rejection is never serialized: only its type and allowlisted fields; values redacted by pattern too", async () => {
    const authorization = await rejected({ authorization: "Bearer abc999", status: 401 });
    expect(authorization.message).toBe("[object Object] {status: 401}");
    expectNoSecret("authorization", authorization);

    const unknownKey = await rejected({ whatever: jwt, name: "AuthFailure", code: "E_AUTH" });
    expect(unknownKey.message).toBe("[object Object] {name: AuthFailure, code: E_AUTH}");
    expectNoSecret("unknown key", unknownKey);

    const bearer = await rejected("upstream said: Bearer abc999 (and Basic Zm9vOmJhcjk5)");
    expect(bearer.message).toBe("upstream said: Bearer *** (and Basic ***)");
    expectNoSecret("string", bearer);

    // A name or code that is itself a credential is redacted; a long code is dropped.
    expectNoSecret("name", await rejected({ name: "Bearer abc999", code: "x".repeat(65) + "ak444" }));

    // The new keys, by name, in any text (an Error's message here).
    const keyed = await rejected(new Error(`proxy-authorization: px555 cookie: a=ck111; b=ck222\nsecret=sec333 api_key=ak444 Authorization: Bearer abc999`));
    expect(keyed.message).toContain("cookie: ***");
    expectNoSecret("keys", keyed);
  });
});

describe("JetStream backend: round 4 (pi-fabric#937)", () => {
  it("P2 (a): every spelling of an IPv4-mapped loopback address is loopback; a mapped off-host address is not", async () => {
    for (const host of ["::ffff:7f00:1", "::ffff:7f00:0001", "::FFFF:7F00:1", "::ffff:127.0.0.1", "0:0:0:0:0:ffff:7f00:1", "::ffff:7fff:fffe", "0:0:0:0:0:0:0:1"]) {
      await expect(assertNatsTransportSecure(`nats://[${host}]:4222`, "tls:", resolver), host).resolves.toBeTypeOf("string");
      // The auth check counts it as local too: no auth required.
      await expect(openNatsStateBackend({ root: root(), port: fakeJetStream().port(), servers: [`tls://[${host}]:4222`], lookup: resolver }), host)
        .resolves.toBeInstanceOf(NatsStateBackend);
    }
    for (const host of ["::ffff:a00:1", "::ffff:8000:1", "::ffff:10.0.0.1", "::2"]) {
      await expect(assertNatsTransportSecure(`nats://[${host}]:4222`, "tls:", resolver), host).rejects.toBeInstanceOf(MeshStateNatsInsecureError);
    }
  });

  it("P2 (b): MIN_SERVER is frozen: a write throws and the floor stays 2.14.7", async () => {
    expect(Object.isFrozen(MIN_SERVER)).toBe(true);
    expect(() => { (MIN_SERVER as number[])[1] = 0; }).toThrow(TypeError);
    expect([...MIN_SERVER]).toEqual([2, 14, 7]);
    const server = fakeJetStream();
    server.version = "2.14.6";
    await expect(open(server)).rejects.toThrow(/older than 2\.14\.7/);
  });
});
