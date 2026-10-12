import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
import { importMeshStateToNats, MeshNatsMigrationLimitError, rollbackMeshStateFromNats, type MeshNatsMigrationOptions } from "../src/mesh/backend-migration-nats.js";
import { importMeshState, MeshBackendRefusedError, meshSnapshotDigest, type MeshBackendStep } from "../src/mesh/backend-migration.js";
import { closeNatsStateBackend, natsStreamName, openNatsStateBackend, type JetStreamPort } from "../src/mesh/state-backend-nats.js";
import { connectJetStream } from "../src/mesh/state-backend-nats-client.js";
import { decodeMeshStateFile } from "../src/mesh/state-file.js";
import { MeshStore, type MeshBatchOperation, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";
import { itFor, natsMonitorUrl, natsServerUrl, reportNatsSkip } from "./helpers/nats-state-server.js";

// smarty-dev#9670: file -> nats -> file, with digests, and a crash at every step (docs/mesh-backends.md, Migration).
// Each case runs on the in-process fake and, when NATS_SERVER_URL and NATS_MONITOR_URL are set, on a real nats-server.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
afterEach(() => {
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed */ }
  for (const root of roots.splice(0)) { closeNatsStateBackend(root); fs.rmSync(root, { recursive: true, force: true }); }
});
const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-nats-migration-${label}-`));
  roots.push(root);
  return root;
};

/** A file root with entries, nested values and CAS tombstones. */
const seedFileRoot = async (root: string): Promise<string> => {
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  stores.push(store);
  const ops: MeshBatchOperation[] = Array.from({ length: 30 }, (_, index) => ({
    kind: "put", key: `seed/ns${index % 3}/${index}`, value: { index, nested: { list: [index, "x"], flag: index % 2 === 0 } },
  }));
  await store.writeBatch({ identity, ops });
  await store.put({ key: "seed/solo", value: "plain", identity });
  for (const index of [3, 7, 11]) await store.delete({ key: `seed/ns${index % 3}/${index}` });
  return fileDigest(root);
};
const fileDigest = (root: string): string => meshSnapshotDigest(decodeMeshStateFile(path.join(root, "state.json"), 64 * 1024 * 1024, false));

type Target = "fake" | "nats";
/** One connection target per test: a fresh fake server, or the real one (each test's fresh root has its own stream,
 * derived from the root: pi-fabric#937). */
const connection = (target: Target): MeshNatsMigrationOptions => {
  if (target === "fake") return { port: fakeJetStream().port() };
  return { servers: [natsServerUrl!], monitoringUrl: natsMonitorUrl! };
};
/** A target whose port a closed nats backend may have closed: a new port on the same fake server each time. */
/** A raw JetStream port on the target (the fake's, or a real connection). */
const rawPort = async (options: MeshNatsMigrationOptions): Promise<JetStreamPort> =>
  options.port ?? connectJetStream(options.servers!);
const reconnectable = (target: Target): () => MeshNatsMigrationOptions => {
  if (target === "nats") { const options = connection(target); return () => options; }
  const server = fakeJetStream();
  return () => ({ port: server.port() });
};
const crashAt = (step: MeshBackendStep): Pick<MeshNatsMigrationOptions, "onStep"> =>
  ({ onStep: (at) => { if (at === step) throw new Error(`crash at ${step}`); } });

describe.each(["fake", "nats"] as const)("file <-> nats migration on the %s server (smarty-dev#9670)", (target) => {
  const it = itFor(target);
  const targetKind = target;
  if (target === "nats") reportNatsSkip();

  it("round trip: the stream, a nats store and the exported state.json all carry the file digest", async () => {
    const root = tempRoot("roundtrip");
    const digest = await seedFileRoot(root);
    const connect = reconnectable(target);
    const options = connect();
    const imported = await importMeshStateToNats(root, options);
    expect(imported).toMatchObject({ digest, epoch: 1, entries: 28, tombstones: 3, converged: false });
    expect(readMeshStateNatsMarker(root)).toMatchObject({ natsStream: imported.stream, epoch: 1 });
    expect(meshSnapshotDigest(decodeMeshStateFile(path.join(root, "state.json.nats-cutover-1"), 64 * 1024 * 1024, false))).toBe(digest);
    // A file-mode store refuses the moved root; a nats store on the stream reads the imported state and writes.
    expect(() => new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" }).get("seed/solo")).toThrow(/moved to NATS stream/);
    await openNatsStateBackend({ root, ...(options.port ? { port: options.port } : { servers: options.servers!, monitoringUrl: options.monitoringUrl! }) });
    const nats = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "nats" });
    stores.push(nats);
    expect(nats.get("seed/solo")?.value).toBe("plain");
    expect(nats.get("seed/ns0/3")).toBeUndefined();
    const written = await nats.put({ key: "after/nats", value: { on: "nats" }, identity });
    expect(written.version).toBeGreaterThan(0);
    nats.closeState();
    closeNatsStateBackend(root);

    const after = connect();
    const back = await rollbackMeshStateFromNats(root, after);
    expect(back).toMatchObject({ stream: imported.stream, epoch: 2, entries: 29, tombstones: 3, converged: false });
    expect(fileDigest(root)).toBe(back.digest);
    // A rerun only verifies, while state.json still holds the stream's state.
    expect(await rollbackMeshStateFromNats(root, connect())).toMatchObject({ digest: back.digest, converged: true });
    const file = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    stores.push(file);
    expect(file.get("after/nats")?.value).toEqual({ on: "nats" });
    expect(file.get("seed/solo")?.value).toBe("plain");
    // The versions and the high-water clock came back: the nats write keeps its version, a new key continues the clock,
    // and a tombstoned key's CAS still sees its tombstone version.
    expect(file.get("after/nats")?.version).toBe(written.version);
    expect((await file.put({ key: "fresh/after", value: 1, identity })).version).toBe(written.version + 1);
    await expect(file.put({ key: "seed/ns0/3", value: 1, identity, ifVersion: 0 })).rejects.toThrow(/compare-and-swap/);
    // The stream is retired: a nats store refuses it, and a rerun of the rollback only verifies.
    await expect(openNatsStateBackend({ root, ...(after.port ? { port: after.port } : { servers: after.servers!, monitoringUrl: after.monitoringUrl! }) }))
      .rejects.toThrow(/retired/);
  }, 60_000);

  it("a compacted stream (a snapshot, then a purge below it, pi-fabric#907) still rolls back with every write", async () => {
    const root = tempRoot("compacted");
    await seedFileRoot(root);
    const connect = reconnectable(targetKind);
    const imported = await importMeshStateToNats(root, connect());
    const options = connect();
    const backend = await openNatsStateBackend({ root, snapshotEvery: 0,
      ...(options.port ? { port: options.port } : { servers: options.servers!, monitoringUrl: options.monitoringUrl! }) });
    for (let index = 0; index < 5; index++) await backend.put({ key: `after/${index}`, value: index, identity });
    expect(await backend.compact()).toBe(true);
    await backend.put({ key: "after/last", value: "tail", identity });
    closeNatsStateBackend(root);
    const raw = await rawPort(connect());
    try { expect(await raw.firstSequence(imported.stream)).toBeGreaterThan(1); } finally { if (targetKind === "nats") await raw.close(); }
    const back = await rollbackMeshStateFromNats(root, connect());
    expect(back).toMatchObject({ epoch: 2, entries: 28 + 6 });
    const file = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    stores.push(file);
    expect(file.get("after/last")?.value).toBe("tail");
    expect(file.get("after/4")?.value).toBe(4);
    expect(file.get("seed/solo")?.value).toBe("plain");
    expect(fileDigest(root)).toBe(back.digest);
  }, 60_000);

  it("an import and a rollback with no writes in between give back the exact file digest", async () => {
    const root = tempRoot("exact");
    const digest = await seedFileRoot(root);
    const options = connection(target);
    await importMeshStateToNats(root, options);
    expect((await rollbackMeshStateFromNats(root, options)).digest).toBe(digest);
    expect(fileDigest(root)).toBe(digest);
  }, 60_000);

  it.each(["nats-import-begin", "nats-import-chunk", "nats-import-verify", "nats-copy", "nats-marker", "nats-import-commit"] as const)(
    "a crash at %s: the rerun converges on the same stream and digest", async (step) => {
      const root = tempRoot(`crash-${step}`);
      const digest = await seedFileRoot(root);
      const options = connection(target);
      await expect(importMeshStateToNats(root, { ...options, ...crashAt(step) })).rejects.toThrow(`crash at ${step}`);
      // The marker is the switch: before it, state.json is still the file state; from it on, file mode refuses.
      const markerSteps: MeshBackendStep[] = ["nats-marker", "nats-import-commit"];
      expect(readMeshStateNatsMarker(root) !== undefined).toBe(markerSteps.includes(step));
      if (!markerSteps.includes(step)) expect(fileDigest(root)).toBe(digest);
      const rerun = await importMeshStateToNats(root, options);
      expect(rerun).toMatchObject({ digest, epoch: 1, converged: step === "nats-import-commit" });
      expect(await importMeshStateToNats(root, options)).toMatchObject({ converged: true });
      expect((await rollbackMeshStateFromNats(root, options)).digest).toBe(digest);
      expect(fileDigest(root)).toBe(digest);
    }, 60_000);

  it.each(["nats-retire", "nats-export-temp", "nats-replace"] as const)("a rollback crash at %s: the rerun converges", async (step) => {
    const root = tempRoot(`rollback-${step}`);
    const digest = await seedFileRoot(root);
    const options = connection(target);
    await importMeshStateToNats(root, options);
    await expect(rollbackMeshStateFromNats(root, { ...options, ...crashAt(step) })).rejects.toThrow(`crash at ${step}`);
    expect(readMeshStateNatsMarker(root) !== undefined).toBe(step !== "nats-replace");
    expect(await rollbackMeshStateFromNats(root, options)).toMatchObject({ digest, epoch: 2, converged: step === "nats-replace" });
    expect(fileDigest(root)).toBe(digest);
  }, 60_000);

  it("a multi-chunk state (about 2.5 MB) crashed after its second chunk resumes and keeps the digest", async () => {
    const root = tempRoot("chunks");
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    stores.push(store);
    const blob = "x".repeat(4 * 1024);
    for (let batch = 0; batch < 6; batch++) {
      await store.writeBatch({ identity, ops: Array.from({ length: 100 }, (_, index) => ({ kind: "put" as const, key: `big/${batch}/${index}`, value: { blob } })) });
    }
    const digest = fileDigest(root);
    const options = connection(target);
    let chunks = 0;
    await expect(importMeshStateToNats(root, { ...options, onStep: (step) => { if (step === "nats-import-chunk" && ++chunks === 2) throw new Error("crash at chunk 2"); } }))
      .rejects.toThrow("crash at chunk 2");
    let resumed = 0;
    const rerun = await importMeshStateToNats(root, { ...options, onStep: (step) => { if (step === "nats-import-chunk") resumed++; } });
    expect(rerun).toMatchObject({ digest, entries: 600 });
    expect(resumed).toBeGreaterThanOrEqual(2); // the stored chunks are verified, only the rest are published
    expect((await rollbackMeshStateFromNats(root, options)).digest).toBe(digest);
  }, 120_000);

  it("a link pre-created at the kept-copy path refuses the import and its target stays unchanged; a link at the old rollback name is never followed", async () => {
    const root = tempRoot("links");
    const digest = await seedFileRoot(root);
    const outside = tempRoot("outside");
    const victim = path.join(outside, "victim.txt");
    fs.writeFileSync(victim, "keep me");
    fs.symlinkSync(victim, path.join(root, "state.json.nats-cutover-1"));
    await expect(importMeshStateToNats(root, reconnectable(targetKind)())).rejects.toThrow(/nats-cutover-1 is not a regular file of this user/);
    expect(fs.readFileSync(victim, "utf8")).toBe("keep me");
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(fileDigest(root)).toBe(digest);

    // A regular kept copy that holds another state refuses too.
    fs.unlinkSync(path.join(root, "state.json.nats-cutover-1"));
    fs.writeFileSync(path.join(root, "state.json.nats-cutover-1"), fs.readFileSync(path.join(root, "state.json")));
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    stores.push(store);
    await store.put({ key: "changed/after/copy", value: 1, identity });
    store.closeState();
    await expect(importMeshStateToNats(root, reconnectable(targetKind)())).rejects.toThrow(/nats-cutover-1 exists and holds another state/);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();

    const clean = tempRoot("links-rollback");
    const cleanDigest = await seedFileRoot(clean);
    const connect = reconnectable(targetKind);
    await importMeshStateToNats(clean, connect());
    fs.symlinkSync(victim, path.join(clean, "state.json.nats-rollback-2.tmp"));
    expect((await rollbackMeshStateFromNats(clean, connect())).digest).toBe(cleanDigest);
    expect(fs.readFileSync(victim, "utf8")).toBe("keep me");
    expect(fileDigest(clean)).toBe(cleanDigest);
  }, 60_000);

  it("LIMIT: an over-cap stream refuses the rollback and stays unretired; an over-cap source publishes nothing", async () => {
    const root = tempRoot("limits");
    const digest = await seedFileRoot(root);
    const connect = reconnectable(targetKind);
    await importMeshStateToNats(root, connect());
    await expect(rollbackMeshStateFromNats(root, { ...connect(), maxStateBytes: 1_024 })).rejects.toThrow(/^LIMIT: FABRIC_MESH_NATS_MIGRATION_LIMIT: .*maxStateBytes/);
    await expect(rollbackMeshStateFromNats(root, { ...connect(), maxReplay: 2 })).rejects.toBeInstanceOf(MeshNatsMigrationLimitError);
    expect(readMeshStateNatsMarker(root)).toBeDefined();
    // Not retired: a nats store still opens on the stream, and a rollback in bounds then succeeds.
    const options = connect();
    const stream = readMeshStateNatsMarker(root)!.natsStream;
    expect(stream).toBe(natsStreamName(root));
    const backend = await openNatsStateBackend({ root, ...(options.port ? { port: options.port } : { servers: options.servers!, monitoringUrl: options.monitoringUrl! }) });
    expect(backend.diagnostics().nats?.status).toBe("ready");
    closeNatsStateBackend(root);
    expect((await rollbackMeshStateFromNats(root, connect())).digest).toBe(digest);

    const over = tempRoot("limits-source");
    await seedFileRoot(over); // 3 tombstones
    const sourceOptions = reconnectable(targetKind)();
    await expect(importMeshStateToNats(over, { ...sourceOptions, maxStateTombstones: 2 })).rejects.toThrow(/LIMIT: .*3 tombstones/);
    expect(readMeshStateNatsMarker(over)).toBeUndefined();
    expect((await importMeshStateToNats(over, sourceOptions)).converged).toBe(false); // the stream was left empty
  }, 60_000);

  it("maxReplay counts the retire record: head == maxReplay refuses before retiring, head == maxReplay - 1 rolls back", async () => {
    const root = tempRoot("replay-boundary");
    const digest = await seedFileRoot(root);
    const connect = reconnectable(targetKind);
    await importMeshStateToNats(root, connect());
    const options = connect();
    const backend = await openNatsStateBackend({ root,
      ...(options.port ? { port: options.port } : { servers: options.servers!, monitoringUrl: options.monitoringUrl! }) });
    const head = backend.diagnostics().nats!.appliedSeq;
    closeNatsStateBackend(root);
    await expect(rollbackMeshStateFromNats(root, { ...connect(), maxReplay: head })).rejects.toThrow(/^LIMIT: .*with the retire record it exceeds maxReplay/);
    expect(readMeshStateNatsMarker(root)).toBeDefined(); // not retired, the marker stands, nothing exported
    expect((await rollbackMeshStateFromNats(root, { ...connect(), maxReplay: head + 1 })).digest).toBe(digest);
    expect(fileDigest(root)).toBe(digest);
  }, 60_000);

  it("a corrupted kept-copy write is caught by its read-back before state.json becomes the marker", async () => {
    const root = tempRoot("torn-copy");
    const digest = await seedFileRoot(root);
    const original = fs.writeSync;
    let corrupted = 0;
    const spy = vi.spyOn(fs, "writeSync").mockImplementation(((descriptor: number, data: Buffer, ...rest: unknown[]) => {
      // The kept copy is the only state envelope this import writes: flip one value, same length, still JSON.
      if (Buffer.isBuffer(data) && corrupted === 0 && data.includes("\"plain\"")) {
        corrupted++;
        data = Buffer.from(data.toString("utf8").replace("\"plain\"", "\"plaiN\""), "utf8");
      }
      return (original as (...args: unknown[]) => number)(descriptor, data, ...rest);
    }) as typeof fs.writeSync);
    try {
      await expect(importMeshStateToNats(root, reconnectable(targetKind)())).rejects.toThrow(/does not read back as the source/);
    } finally { spy.mockRestore(); }
    expect(corrupted).toBe(1);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(fileDigest(root)).toBe(digest);
  }, 60_000);

  it("one entry larger than the server's max payload refuses before import-begin: the stream never enters importing", async () => {
    const root = tempRoot("max-payload");
    const store = new MeshStore(root, 2 * 1024 * 1024, 1_000, { stateBackend: "file" });
    stores.push(store);
    await store.put({ key: "big/one", value: "x".repeat(1_100_000), identity });
    store.closeState();
    const digest = fileDigest(root);
    const connect = reconnectable(targetKind);
    const options = connect();
    await expect(importMeshStateToNats(root, options)).rejects.toThrow(/^LIMIT: .*more than the server's max payload 1048576/);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(fileDigest(root)).toBe(digest);
    const probe = connect();
    const backend = await openNatsStateBackend({ root,
      ...(probe.port ? { port: probe.port } : { servers: probe.servers!, monitoringUrl: probe.monitoringUrl! }) });
    expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 0, status: "ready" });
    closeNatsStateBackend(root);
  }, 60_000);

  it("refuses a sqlite root, and a rerun whose state.json changed after a crash before the marker", async () => {
    const sqliteRoot = tempRoot("sqlite");
    await seedFileRoot(sqliteRoot);
    await importMeshState(sqliteRoot);
    await expect(importMeshStateToNats(sqliteRoot, connection(target))).rejects.toThrow(/roll it back to file first/);

    const root = tempRoot("changed");
    await seedFileRoot(root);
    const options = connection(target);
    await expect(importMeshStateToNats(root, { ...options, ...crashAt("nats-import-chunk") })).rejects.toThrow("crash at");
    const file = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    stores.push(file);
    await file.put({ key: "late/writer", value: 1, identity });
    await expect(importMeshStateToNats(root, options)).rejects.toBeInstanceOf(MeshBackendRefusedError);
    // A partial leftover (still importing, no marker): it may be deleted, after checking nothing serves the root.
    await expect(importMeshStateToNats(root, options)).rejects.toThrow(/leftover of a failed import .*After checking that no Fabric process serves this root from NATS, delete stream \S+ on the server and rerun/);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(file.get("late/writer")?.value).toBe(1);
  }, 60_000);
});

// pi-fabric#918: a stream that holds committed (here retired) state may be the root's live NATS mesh: the refusal never
// advises deleting it.
it("refuses to import over a stream with committed state, and never advises deleting it", async () => {
  const server = fakeJetStream();
  const root = tempRoot("committed");
  await seedFileRoot(root);
  const imported = await importMeshStateToNats(root, { port: server.port() });
  await rollbackMeshStateFromNats(root, { port: server.port() });
  expect(readMeshStateNatsMarker(root)).toBeUndefined();
  const refusal = importMeshStateToNats(root, { port: server.port() });
  await expect(refusal).rejects.toThrow(new RegExp(`this root already has NATS state in stream ${imported.stream} \\(retired at epoch 2, \\d+ messages\\); an import never overwrites it$`));
  await expect(refusal).rejects.not.toThrow(/delete/);
});

it("refuses a nats-server older than the backend's minimum before any write: no marker, state.json unchanged", async () => {
  const server = fakeJetStream();
  server.version = "2.10.22";
  const root = tempRoot("old-server");
  const digest = await seedFileRoot(root);
  const bytes = fs.readFileSync(path.join(root, "state.json"));
  await expect(importMeshStateToNats(root, { port: server.port() })).rejects.toThrow(/nats-server 2\.10\.22 is older than 2\.14\.7/);
  expect(readMeshStateNatsMarker(root)).toBeUndefined();
  expect(fs.readFileSync(path.join(root, "state.json"))).toEqual(bytes);
  expect(fileDigest(root)).toBe(digest);
  expect(server.messages).toHaveLength(0);
  expect(fs.readdirSync(root).filter(name => name.startsWith("state.json."))).toEqual([]);
});

it("rollback is the escape hatch: a NATS root on a server below 2.14.7 (or a prerelease of it) still rolls back (pi-fabric#918 round 4)", async () => {
  for (const version of ["2.10.22", "2.14.7-rc.1"]) {
    const server = fakeJetStream();
    const root = tempRoot("old-server-rollback");
    const digest = await seedFileRoot(root);
    await importMeshStateToNats(root, { port: server.port() });
    server.version = version; // the server was downgraded (or reports a prerelease) after the switch
    await expect(importMeshStateToNats(tempRoot("old-server-import"), { port: server.port() })).rejects.toThrow(/older than 2\.14\.7/);
    const back = await rollbackMeshStateFromNats(root, { port: server.port() });
    expect(back).toMatchObject({ digest, epoch: 2, converged: false });
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(fileDigest(root)).toBe(digest);
  }
});

it("refuses a server that does not report sync_always", async () => {
  const server = fakeJetStream();
  server.syncAlways = false;
  const root = tempRoot("periodic");
  await seedFileRoot(root);
  await expect(importMeshStateToNats(root, { port: server.port() })).rejects.toThrow(/sync_always/);
  expect(readMeshStateNatsMarker(root)).toBeUndefined();
});
