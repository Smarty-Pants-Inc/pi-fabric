import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AtomicFileWriter } from "../src/core/atomic-write.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { MeshStore, type MeshStateEntry } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFiles, writeParticipantFile, writeParticipantFileIf } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const root = () => { const value = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-soft-writes-")); roots.push(value); return value; };
afterEach(() => { vi.restoreAllMocks(); for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true }); });
const identity = { id: "session:idle", name: "main", kind: "main" as const, sessionId: "idle" };
const key = "topology/participants/" + createHash("sha256").update(identity.id).digest("hex");
const presence = (): MeshStateEntry => ({ key, version: 1, updatedAt: 1, updatedBy: identity, value: {
  format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id,
  ownerIdentityId: identity.id, name: "idle", status: "idle", runner: "pi", transport: "host",
  capabilities: ["fabric"], startedAt: 1, updatedAt: 1, controlProtocol: "v1", label: "P-1",
} satisfies FabricParticipantRecord });

describe("unchanged atomic bytes", () => {
  it("soft-state writes skip unchanged bytes, while durable writes always replace and sync", () => {
    const file = path.join(root(), "state.json"), writer = new AtomicFileWriter(file);
    writer.write('{"status":"idle"}');
    const writes = vi.spyOn(fs, "writeFileSync"), renames = vi.spyOn(fs, "renameSync"), syncs = vi.spyOn(fs, "fsyncSync");
    for (let i = 0; i < 100; i++) expect(writer.write('{"status":"idle"}')).toBe(false);
    expect(writes).not.toHaveBeenCalled(); expect(renames).not.toHaveBeenCalled(); expect(syncs).not.toHaveBeenCalled();
    expect(writer.write('{"status":"running"}')).toBe(true);
    expect(writes).toHaveBeenCalledTimes(1); expect(renames).toHaveBeenCalledTimes(1); expect(syncs).not.toHaveBeenCalled();
    writes.mockClear(); renames.mockClear(); syncs.mockClear();
    expect(writer.write('{"status":"running"}', { durable: true })).toBe(true);
    expect(writes).toHaveBeenCalledTimes(1); expect(renames).toHaveBeenCalledTimes(1); expect(syncs).toHaveBeenCalled();
  });

  it("durable equal bytes are never accepted as an existing durability receipt", () => {
    const file = path.join(root(), "fence.json"), writer = new AtomicFileWriter(file);
    writer.write("accepted", { durable: true });
    const syncs = vi.spyOn(fs, "fsyncSync");
    expect(writer.write("accepted", { durable: true })).toBe(true);
    expect(syncs).toHaveBeenCalled();
    fs.rmSync(file);
    syncs.mockClear();
    expect(writer.write("accepted", { durable: true })).toBe(true); expect(syncs).toHaveBeenCalled();
  });

  it("does not mistake equal post-rename bytes after a failed barrier for acceptance", () => {
    const file = path.join(root(), "fence.json"), writer = new AtomicFileWriter(file);
    writer.write("old", { durable: true });
    const sync = fs.fsyncSync.bind(fs); let calls = 0;
    const syncs = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (++calls === 2 && process.platform !== "win32") throw new Error("directory barrier failed");
      sync(fd);
    });
    if (process.platform !== "win32") expect(() => writer.write("new", { durable: true })).toThrow("directory barrier failed");
    syncs.mockImplementation(sync); syncs.mockClear();
    expect(writer.write("new", { durable: true })).toBe(true); expect(syncs).toHaveBeenCalled();
  });
});

describe("registry classifications", () => {
  it("skips confirmed unchanged snapshots; every changed whole-registry image fsyncs", () => {
    const store = new ActorRegistryStore(root()); const actor = { id: "actor", rootId: "owner", residency: "session", status: "idle" };
    store.write([actor]);
    const writes = vi.spyOn(fs, "writeFileSync"), syncs = vi.spyOn(fs, "fsyncSync");
    for (let i = 0; i < 100; i++) store.write([actor]);
    expect(writes).not.toHaveBeenCalled(); expect(syncs).not.toHaveBeenCalled();
    store.write([{ ...actor, status: "running" }], { durable: false }); expect(writes).toHaveBeenCalledTimes(1); expect(syncs).toHaveBeenCalled();
    writes.mockClear();
    const adopted = { ...actor, rootId: "adopter", adoptedAt: 1 };
    store.write([adopted]); expect(writes).toHaveBeenCalledTimes(1); expect(syncs).toHaveBeenCalled();
    syncs.mockClear();
    store.write([{ ...adopted, status: "running" }]);
    expect(syncs).toHaveBeenCalled(); // Replacement still carries accepted adoption authority.
  });
});

describe("participant presence", () => {
  it("skips byte-identical records; changed fields rename once without fsync", () => {
    const meshRoot = root(), entry = presence(); writeParticipantFile(meshRoot, entry);
    const writes = vi.spyOn(fs, "writeFileSync"), renames = vi.spyOn(fs, "renameSync"), syncs = vi.spyOn(fs, "fsyncSync");
    for (let i = 0; i < 100; i++) writeParticipantFile(meshRoot, entry);
    expect(writes).not.toHaveBeenCalled(); expect(renames).not.toHaveBeenCalled(); expect(syncs).not.toHaveBeenCalled();
    writeParticipantFile(meshRoot, { ...entry, value: { ...(entry.value as object), status: "running" } });
    expect(writes).toHaveBeenCalledTimes(1); expect(renames).toHaveBeenCalledTimes(1); expect(syncs).not.toHaveBeenCalled();
  });

  it("keeps migration publication durable, including equal bytes recovered from disk", async () => {
    const mesh = new MeshStore(root(), 65536, 100), entry = presence(); writeParticipantFile(mesh.root, entry);
    const syncs = vi.spyOn(fs, "fsyncSync");
    expect(await writeParticipantFileIf(mesh, key, () => entry, { durable: true })).toBe(true);
    expect(syncs).toHaveBeenCalled(); expect(readParticipantFiles(mesh.root)).toEqual([entry]);
  });

  it("a heartbeat restores a soft presence file lost on crash, and unchanged heartbeats never republish it", async () => {
    const mesh = new MeshStore(root(), 65536, 100);
    await mesh.put({ key: "topology/liveness", value: { version: 1, hostLeases: "files", participants: "files" }, identity });
    const directory = new ParticipantDirectory(mesh, { enabled: true, identity, hostId: identity.id, rootId: identity.id, reapDeadHosts: false });
    directory.registerSource(() => [presence().value as FabricParticipantRecord]);
    try {
      await directory.refresh();
      const file = path.join(mesh.root, "participants", key.slice("topology/participants/".length) + ".json");
      const write = vi.spyOn(fs, "writeFileSync");
      for (let i = 0; i < 10; i++) await directory.refresh();
      expect(write.mock.calls.filter(([target]) => String(target).startsWith(file))).toHaveLength(0);
      fs.rmSync(file); expect(readParticipantFiles(mesh.root)).toEqual([]);
      await directory.refresh();
      expect(write.mock.calls.filter(([target]) => String(target).startsWith(file))).toHaveLength(1);
      expect(directory.get(identity.id, Date.now(), { fresh: true })).toMatchObject({ stale: false, ownerHostId: identity.id, status: "idle" });
    } finally { await directory.close(); }
  });
});
