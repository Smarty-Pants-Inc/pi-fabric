import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME, meshCustodyMode } from "../src/mesh/custody-lock.js";

// smarty-dev#6477 L5: file custody takes its own lock instead of the single mesh .lock.
const roots: string[] = [];
const store = (): MeshStore => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-"));
  roots.push(root);
  return new MeshStore(path.join(root, "mesh"), 64 * 1024, 500, { lockTimeoutMs: 300 });
};
const from: MeshIdentity = { id: "session:custody", sessionId: "custody", name: "custody", kind: "main" };
const owned = (mesh: MeshStore, name: string): boolean => fs.existsSync(path.join(mesh.root, name, "owner"));
// A complete, live owner receipt without an incarnation: never recoverable, like a running holder.
const holdAsLiveProcess = (mesh: MeshStore, name: string): void => {
  fs.mkdirSync(path.join(mesh.root, name), { mode: 0o700 });
  fs.writeFileSync(path.join(mesh.root, name, "owner"), `legacy\n${process.pid}\n${Date.now()}\n`);
};

afterEach(() => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("mesh file custody lock", () => {
  it("defaults to the transition-safe dual mode; only an explicit 'own' drops the mesh lock", () => {
    expect(meshCustodyMode(undefined)).toBe("dual");
    expect(meshCustodyMode("")).toBe("dual");
    expect(meshCustodyMode("mesh")).toBe("dual");
    expect(meshCustodyMode(" OWN ")).toBe("own");
  });

  it("dual: holds the custody lock and then the mesh lock, and releases both", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "");
    const mesh = store();
    const seen = await mesh.custody(() => [owned(mesh, MESH_CUSTODY_LOCK_NAME), owned(mesh, ".lock")]);
    expect(seen).toEqual([true, true]);
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
  });

  it("dual: a pre-L5 process holding only the mesh lock still excludes custody (mixed release)", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    const mesh = store();
    holdAsLiveProcess(mesh, ".lock");
    const operation = vi.fn();
    await expect(mesh.custody(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(operation).not.toHaveBeenCalled();
    // The custody lock taken first is released on the mesh timeout.
    expect(fs.existsSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME))).toBe(false);
  });

  it("own: custody no longer blocks a concurrent mesh publish or state write", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    const mesh = store();
    const other = new MeshStore(mesh.root, 64 * 1024, 500, { lockTimeoutMs: 300 });
    // A long custody holder (another process's inbox reroute, say) keeps custody...
    const release = await acquireMeshCustodyLock(mesh.root);
    try {
      const started = Date.now();
      // ...while publishes and state writes proceed on the mesh lock without waiting for it.
      await expect(other.publish({ topic: "fleet.work.test", kind: "probe", from, text: "not queued behind custody" }))
        .resolves.toMatchObject({ topic: "fleet.work.test" });
      await expect(other.put({ key: "custody/probe", value: 1, identity: from })).resolves.toMatchObject({ key: "custody/probe" });
      expect(Date.now() - started).toBeLessThan(5_000);
      // Custody itself stays exclusive.
      await expect(other.custody(() => "entered", 50)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    } finally { release(); }
    // And a custody section no longer holds the mesh lock.
    await expect(other.custody(() => [owned(mesh, MESH_CUSTODY_LOCK_NAME), fs.existsSync(path.join(mesh.root, ".lock"))]))
      .resolves.toEqual([true, false]);
    expect(other.read({ topic: "fleet.work.test" })).toHaveLength(1);
  });

  it("dual: by contrast, a held mesh lock blocks custody while it would not block in own mode", async () => {
    const mesh = store();
    holdAsLiveProcess(mesh, ".lock");
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    await expect(mesh.custody(() => "own")).resolves.toBe("own");
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "dual");
    await expect(mesh.custody(() => "dual")).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
  });

  it("recovers a dead holder's custody lock at once, and keeps a live holder's", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    const mesh = store();
    holdAsLiveProcess(mesh, MESH_CUSTODY_LOCK_NAME);
    await expect(mesh.custody(() => "live", 50)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    fs.rmSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME), { recursive: true });
    const dead = spawnSync(process.execPath, ["-e", ""]).pid;
    fs.mkdirSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME), { mode: 0o700 });
    fs.writeFileSync(path.join(mesh.root, MESH_CUSTODY_LOCK_NAME, "owner"), `dead\n${dead}\n${Date.now()}\n`);
    await expect(mesh.custody(() => "recovered", 1_000)).resolves.toBe("recovered");
  });

  it("a registry-fenced withTryLock budget bounds the custody wait", async () => {
    vi.stubEnv("PI_FABRIC_MESH_CUSTODY_LOCK", "own");
    const mesh = store();
    const release = await acquireMeshCustodyLock(mesh.root);
    try {
      const started = Date.now();
      await expect(mesh.withTryLock(() => mesh.custody(() => "entered"), 0)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(Date.now() - started).toBeLessThan(1_000);
    } finally { release(); }
  });
});
