import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { assertFileStateWritable, cutoverMeshState, MeshBackendFenceError, resolveMeshStateSource } from "../src/mesh/backend-migration.js";
import { projectorDatabaseRoot, StateProjector } from "../src/mesh/state-projector.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// pi-fabric#631: in shadow mode the projector's database never claims backend authority, so the
// L4b fence (pi-fabric#627) keeps file-mode writers running; a real cutover still refuses them.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const projectors: StateProjector[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-projector-fence-${label}-`));
  roots.push(root);
  return root;
};

afterEach(async () => {
  for (const projector of projectors.splice(0)) await projector.stop();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("shadow projector and the backend fence (pi-fabric#631)", () => {
  it("a running shadow projector leaves file-mode writes allowed; a real cutover refuses them", async () => {
    const root = tempRoot("shadow");
    const file = new MeshStore(root, 64 * 1024, 1_000);
    await file.put({ key: "k/before", value: 1, identity });
    const projector = await StateProjector.open({ root, verifyMs: 0, statusMs: 0, pollMs: 5 });
    projectors.push(projector);
    projector.run();
    await projector.tick();

    // The shadow database is not <root>/state.db, so the fence still reads state.json as the authority.
    expect(projectorDatabaseRoot(root)).not.toBe(path.resolve(root));
    expect(fs.existsSync(path.join(projectorDatabaseRoot(root), "state.db"))).toBe(true);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(() => assertFileStateWritable(root)).not.toThrow();
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "file" });
    for (let step = 0; step < 5; step += 1) await file.put({ key: `k/${step}`, value: step, identity });
    await projector.tick();
    expect(await projector.verify()).toEqual([]);
    expect(await file.get("k/4")).toMatchObject({ value: 4 });
    await projector.stop();

    // The real cutover commits backend=sqlite in <root>/state.db: file-mode writers are refused.
    await cutoverMeshState(root, { assumeNoWriters: true });
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "sqlite", backend: "sqlite" });
    expect(() => assertFileStateWritable(root)).toThrow(MeshBackendFenceError);
    await expect(new MeshStore(root, 64 * 1024, 1_000).put({ key: "k/late", value: 1, identity })).rejects.toThrow(MeshBackendFenceError);
  });

  it("refuses a shadow database in the mesh root, where it would be read as the cutover flag", async () => {
    const root = tempRoot("in-root");
    await new MeshStore(root, 64 * 1024, 1_000).put({ key: "k/a", value: 1, identity });
    await expect(StateProjector.open({ root, databaseRoot: root, verifyMs: 0, statusMs: 0 })).rejects.toThrow(/must not live in the mesh root/);
    await expect(StateProjector.open({ root, databaseRoot: path.join(root, "."), mode: "shadow", verifyMs: 0, statusMs: 0 })).rejects.toThrow(/must not live in the mesh root/);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    await new MeshStore(root, 64 * 1024, 1_000).put({ key: "k/b", value: 2, identity });
  });
});
