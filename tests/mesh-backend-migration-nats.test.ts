import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
import { importMeshStateToNats, rollbackMeshStateFromNats, type MeshNatsMigrationOptions } from "../src/mesh/backend-migration-nats.js";
import { importMeshState, MeshBackendRefusedError, meshSnapshotDigest, type MeshBackendStep } from "../src/mesh/backend-migration.js";
import { closeNatsStateBackend, openNatsStateBackend } from "../src/mesh/state-backend-nats.js";
import { decodeMeshStateFile } from "../src/mesh/state-file.js";
import { MeshStore, type MeshBatchOperation, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream } from "./helpers/nats-jetstream-fake.js";
import { itFor, natsMonitorUrl, natsServerUrl, natsTestStream, reportNatsSkip } from "./helpers/nats-state-server.js";

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
/** One connection target per test: a fresh fake server, or a fresh stream on the real one. */
const connection = (target: Target): MeshNatsMigrationOptions => {
  if (target === "fake") return { port: fakeJetStream().port() };
  return { servers: [natsServerUrl!], monitoringUrl: natsMonitorUrl!, stream: natsTestStream() };
};
/** A target whose port a closed nats backend may have closed: a new port on the same fake server each time. */
const reconnectable = (target: Target): () => MeshNatsMigrationOptions => {
  if (target === "nats") { const options = connection(target); return () => options; }
  const server = fakeJetStream();
  return () => ({ port: server.port() });
};
const crashAt = (step: MeshBackendStep): Pick<MeshNatsMigrationOptions, "onStep"> =>
  ({ onStep: (at) => { if (at === step) throw new Error(`crash at ${step}`); } });

describe.each(["fake", "nats"] as const)("file <-> nats migration on the %s server (smarty-dev#9670)", (target) => {
  const it = itFor(target);
  if (target === "nats") reportNatsSkip();

  it("round trip: the stream, a nats store and the exported state.json all carry the file digest", async () => {
    const root = tempRoot("roundtrip");
    const digest = await seedFileRoot(root);
    const connect = reconnectable(target);
    const options = connect();
    const imported = await importMeshStateToNats(root, options);
    expect(imported).toMatchObject({ digest, epoch: 1, entries: 28, tombstones: 3, converged: false });
    expect(readMeshStateNatsMarker(root)).toMatchObject({ stream: imported.stream, epoch: 1 });
    expect(meshSnapshotDigest(decodeMeshStateFile(path.join(root, "state.json.nats-cutover-1"), 64 * 1024 * 1024, false))).toBe(digest);
    // A file-mode store refuses the moved root; a nats store on the stream reads the imported state and writes.
    expect(() => new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" }).get("seed/solo")).toThrow(/moved to NATS stream/);
    await openNatsStateBackend({ root, stream: imported.stream, ...(options.port ? { port: options.port } : { servers: options.servers!, monitoringUrl: options.monitoringUrl! }) });
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
    await expect(openNatsStateBackend({ root, stream: imported.stream, ...(after.port ? { port: after.port } : { servers: after.servers!, monitoringUrl: after.monitoringUrl! }) }))
      .rejects.toThrow(/retired/);
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
    await expect(importMeshStateToNats(root, options)).rejects.toThrow(/name a fresh stream/);
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(file.get("late/writer")?.value).toBe(1);
  }, 60_000);
});

it("refuses a server that does not report sync_always", async () => {
  const server = fakeJetStream();
  server.syncAlways = false;
  const root = tempRoot("periodic");
  await seedFileRoot(root);
  await expect(importMeshStateToNats(root, { port: server.port() })).rejects.toThrow(/sync_always/);
  expect(readMeshStateNatsMarker(root)).toBeUndefined();
});
