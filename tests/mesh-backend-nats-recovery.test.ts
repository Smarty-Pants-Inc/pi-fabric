import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readMeshStateNatsMarker } from "../src/mesh/backend-fence.js";
import { abandonMeshNatsImport, importMeshStateToNats, rollbackMeshStateFromNats, type MeshNatsMigrationOptions } from "../src/mesh/backend-migration-nats.js";
import { meshSnapshotDigest, type MeshBackendStep } from "../src/mesh/backend-migration.js";
import { closeNatsStateBackend, natsStreamName, natsSwitchStreamName } from "../src/mesh/state-backend-nats.js";
import { decodeMeshStateFile } from "../src/mesh/state-file.js";
import { MeshStore, type MeshBatchOperation, type MeshIdentity } from "../src/mesh/store.js";
import { fakeJetStream, type FakeJetStreamServer } from "./helpers/nats-jetstream-fake.js";

// LD-02 (a rollback after the marker removes ITS intent, never another switch's) and LD-03 (abandon fails closed on a
// malformed marker or an ambiguous state.json, before any stream call). The one-stream fake ignores stream names, so a
// NEW switch's import runs on a fresh fake server while the old server and its records are kept.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
afterEach(() => {
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed */ }
  for (const root of roots.splice(0)) {
    closeNatsStateBackend(root);
    try { fs.chmodSync(path.join(root, "state.json"), 0o600); } catch { /* absent */ }
    fs.rmSync(root, { recursive: true, force: true });
  }
});
const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-nats-recovery-${label}-`));
  roots.push(root);
  return root;
};
const seedFileRoot = async (root: string): Promise<string> => {
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  stores.push(store);
  const ops: MeshBatchOperation[] = Array.from({ length: 12 }, (_, index) => ({ kind: "put", key: `seed/${index}`, value: { index } }));
  await store.writeBatch({ identity, ops });
  await store.delete({ key: "seed/3" });
  return fileDigest(root);
};
const fileDigest = (root: string): string => meshSnapshotDigest(decodeMeshStateFile(path.join(root, "state.json"), 64 * 1024 * 1024, false));
const crashAt = (step: MeshBackendStep): Pick<MeshNatsMigrationOptions, "onStep"> =>
  ({ onStep: (at) => { if (at === step) throw new Error(`crash at ${step}`); } });
const intentPath = (root: string): string => path.join(root, "state.json.nats-intent");
const readIntentFile = (root: string): { switchId: string; stream: string } =>
  JSON.parse(fs.readFileSync(intentPath(root), "utf8")) as { switchId: string; stream: string };
/** Another valid, root-bound intent: a newer staged switch (not this rollback's). */
const writeOtherIntent = (root: string): string => {
  const switchId = "a".repeat(32);
  const text = JSON.stringify({ switchId, stream: natsSwitchStreamName(root, switchId) });
  fs.rmSync(intentPath(root), { force: true });
  fs.writeFileSync(intentPath(root), text, { mode: 0o600 });
  return text;
};

/** Leaves the root after the marker but before import-commit: a crash, or the readiness gate refusing the commit. */
const stopAfterMarker = async (root: string, server: FakeJetStreamServer, how: "crash" | "gate"): Promise<void> => {
  const options: MeshNatsMigrationOptions = how === "crash" ? crashAt("nats-marker")
    : { beforeCommit: (check) => { if (check.phase === "nats-import-commit") throw new Error("unready at nats-import-commit"); } };
  await expect(importMeshStateToNats(root, { port: server.port(), ...options })).rejects.toThrow(how === "crash" ? "crash at nats-marker" : "unready");
  expect(readMeshStateNatsMarker(root)).toBeDefined();
  expect(fs.existsSync(intentPath(root))).toBe(true);
};

describe("LD-02: a rollback after the marker removes its own intent (never another switch's)", () => {
  it.each(["crash", "gate"] as const)("stopped after the marker (%s): rollback removes the intent; a new import picks a NEW stream", async (how) => {
    const root = tempRoot(`ld02-${how}`);
    const digest = await seedFileRoot(root);
    const old = fakeJetStream();
    await stopAfterMarker(root, old, how);
    const oldIntent = readIntentFile(root);
    const rolled = await rollbackMeshStateFromNats(root, { port: old.port() });
    expect(rolled).toMatchObject({ stream: oldIntent.stream, digest, converged: false });
    expect(fileDigest(root)).toBe(digest);
    expect(fs.existsSync(intentPath(root))).toBe(false);
    // The old server keeps the retired stream's records; the next import allocates a new switch on a new stream.
    const oldMessages = old.messages.length;
    const fresh = fakeJetStream();
    fresh.streamExists = false;
    const again = await importMeshStateToNats(root, { port: fresh.port() });
    expect(again).toMatchObject({ digest, converged: false });
    expect(again.stream).not.toBe(oldIntent.stream);
    expect(again.stream.startsWith(`${natsStreamName(root)}_`)).toBe(true);
    expect(readMeshStateNatsMarker(root)?.natsStream).toBe(again.stream);
    expect(old.messages.length).toBe(oldMessages);
  }, 60_000);

  it("a rollback crashed after nats-replace: the rerun with the switch id converges and removes the owned intent", async () => {
    const root = tempRoot("ld02-converged");
    const digest = await seedFileRoot(root);
    const server = fakeJetStream();
    await stopAfterMarker(root, server, "crash");
    const intent = readIntentFile(root);
    await expect(rollbackMeshStateFromNats(root, { port: server.port(), ...crashAt("nats-replace") })).rejects.toThrow("crash at nats-replace");
    expect(readMeshStateNatsMarker(root)).toBeUndefined();
    expect(fs.existsSync(intentPath(root))).toBe(true); // the crash came before the cleanup
    const rerun = await rollbackMeshStateFromNats(root, { port: server.port(), switchId: intent.switchId });
    expect(rerun).toMatchObject({ stream: intent.stream, digest, epoch: 2, converged: true });
    expect(fs.existsSync(intentPath(root))).toBe(false);
    expect(fileDigest(root)).toBe(digest);
  }, 60_000);

  it("counterexample: an unrelated (newer) intent is never removed, by a converged rerun or a completing rollback", async () => {
    // Converged: verifying an older finished rollback while a different switch is staged keeps that switch's intent.
    const root = tempRoot("ld02-unrelated");
    const digest = await seedFileRoot(root);
    const server = fakeJetStream();
    await stopAfterMarker(root, server, "crash");
    const intent = readIntentFile(root);
    await expect(rollbackMeshStateFromNats(root, { port: server.port(), ...crashAt("nats-replace") })).rejects.toThrow("crash at nats-replace");
    const other = writeOtherIntent(root);
    expect(await rollbackMeshStateFromNats(root, { port: server.port(), switchId: intent.switchId })).toMatchObject({ converged: true, digest });
    expect(fs.readFileSync(intentPath(root), "utf8")).toBe(other);

    // Completing: the marker names this rollback's stream, the intent another switch: the rollback completes, the intent stays.
    const second = tempRoot("ld02-unrelated-complete");
    const secondDigest = await seedFileRoot(second);
    const secondServer = fakeJetStream();
    await stopAfterMarker(second, secondServer, "crash");
    const secondOther = writeOtherIntent(second);
    expect(await rollbackMeshStateFromNats(second, { port: secondServer.port() })).toMatchObject({ converged: false, digest: secondDigest });
    expect(fs.readFileSync(intentPath(second), "utf8")).toBe(secondOther);
  }, 60_000);

  it("an invalid intent refuses the rollback before the stream is retired", async () => {
    const root = tempRoot("ld02-invalid");
    await seedFileRoot(root);
    const server = fakeJetStream();
    await stopAfterMarker(root, server, "crash");
    fs.rmSync(intentPath(root));
    fs.writeFileSync(intentPath(root), JSON.stringify({ switchId: "b".repeat(32), stream: "FABRIC_MESH_other" }), { mode: 0o600 });
    const before = server.messages.length;
    await expect(rollbackMeshStateFromNats(root, { port: server.port() })).rejects.toThrow(/is not a valid intent/);
    expect(server.messages.length).toBe(before);
    expect(readMeshStateNatsMarker(root)).toBeDefined();
  }, 60_000);
});

describe("LD-03: abandon fails closed on a malformed marker or an ambiguous state.json, before any stream call", () => {
  /** A failed import: the intent and this switch's importing stream (epoch 1, matching switch id): abandon's authority. */
  const leftover = async (label: string): Promise<{ root: string; server: FakeJetStreamServer; original: Buffer }> => {
    const root = tempRoot(label);
    await seedFileRoot(root);
    const server = fakeJetStream();
    server.streamExists = false;
    await expect(importMeshStateToNats(root, { port: server.port(), ...crashAt("nats-import-chunk") })).rejects.toThrow("crash at");
    expect(server.streamExists).toBe(true);
    return { root, server, original: fs.readFileSync(path.join(root, "state.json")) };
  };
  const snapshotOf = (root: string, server: FakeJetStreamServer) => ({
    state: fs.readFileSync(path.join(root, "state.json")).toString("base64"),
    intent: fs.readFileSync(intentPath(root), "utf8"),
    messages: server.messages.map(message => Buffer.from(message.data).toString("base64")),
    exists: server.streamExists,
  });
  const expectRefusedUntouched = async (root: string, server: FakeJetStreamServer, pattern: RegExp): Promise<void> => {
    const before = snapshotOf(root, server);
    const port = server.port();
    await expect(abandonMeshNatsImport(root, { port })).rejects.toThrow(pattern);
    expect(port.calls).not.toContain("deleteStream");
    expect(port.calls).not.toContain("streamInfo");
    expect(snapshotOf(root, server)).toEqual(before);
  };

  it("a malformed marker with a valid intent and the matching importing stream refuses: no delete, nothing changed", async () => {
    const { root, server } = await leftover("ld03-malformed");
    const statePath = path.join(root, "state.json");
    fs.rmSync(statePath);
    fs.writeFileSync(statePath, JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 1, at: new Date().toISOString(), natsStream: "not a stream!" }));
    await expectRefusedUntouched(root, server, /moved marker with an invalid natsStream/);
  }, 60_000);

  it.each([
    ["truncated JSON", (bytes: Buffer) => bytes.subarray(0, Math.floor(bytes.byteLength / 2))],
    ["an empty file", () => Buffer.alloc(0)],
    ["a valid JSON document that is not state", () => Buffer.from(JSON.stringify({ hello: "world" }))],
  ] as const)("an ambiguous state.json (%s) refuses: no delete, nothing changed", async (_label, damage) => {
    const { root, server, original } = await leftover("ld03-ambiguous");
    fs.writeFileSync(path.join(root, "state.json"), damage(original));
    await expectRefusedUntouched(root, server, /is not a readable file state/);
  }, 60_000);

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("an unreadable state.json (EACCES) refuses, never swallowed", async () => {
    const { root, server } = await leftover("ld03-eacces");
    const before = server.messages.length;
    fs.chmodSync(path.join(root, "state.json"), 0o000);
    const port = server.port();
    await expect(abandonMeshNatsImport(root, { port })).rejects.toThrow(/EACCES|is not a readable file state/);
    expect(port.calls).not.toContain("deleteStream");
    expect(server.messages.length).toBe(before);
    expect(fs.existsSync(intentPath(root))).toBe(true);
  }, 60_000);

  it("counterexample: the failed import's valid file state still abandons; an absent state.json (crash before the read) too", async () => {
    const { root, server } = await leftover("ld03-valid");
    const intent = readIntentFile(root);
    expect(await abandonMeshNatsImport(root, { port: server.port() })).toEqual({ root, stream: intent.stream, deleted: true });
    expect(fs.existsSync(intentPath(root))).toBe(false);

    const absent = await leftover("ld03-absent");
    const absentIntent = readIntentFile(absent.root);
    fs.rmSync(path.join(absent.root, "state.json"));
    expect(await abandonMeshNatsImport(absent.root, { port: absent.server.port() })).toEqual({ root: absent.root, stream: absentIntent.stream, deleted: true });
  }, 60_000);
});
