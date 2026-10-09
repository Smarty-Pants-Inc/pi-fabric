import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#7815: the switch refuses unless every registered reader proved it can read sqlite.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

const fileRoot = async (): Promise<string> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-readiness-"));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "seed/a", value: { n: 1 }, identity });
  await store.put({ key: "seed/b", value: "two", identity });
  store.closeState();
  return root;
};

const run = async (argv: string[]): Promise<{ code: number; out: string; err: string }> => {
  let out = "";
  let err = "";
  const code = await main(argv, { census: async () => ({ writers: [] }), stdout: (text) => { out += text; }, stderr: (text) => { err += text; } });
  return { code, out, err };
};

/** A reader as the Python factory registers it: a hand-written proof of file only (docs/mesh-backend.md). */
const registerFileOnly = (root: string, name: string): void => {
  fs.mkdirSync(path.join(root, "readers"), { recursive: true });
  fs.writeFileSync(path.join(root, "readers", `${name}.json`), JSON.stringify({
    name, version: "factory-1", backends: ["file"], provedAt: new Date().toISOString(), provedEpoch: 0 }));
};

const backendOf = (root: string): string => fs.readFileSync(path.join(root, "state.json"), "utf8").includes("entries") ? "file" : "moved";

describe("fabric-mesh-backend readiness gate", () => {
  it("refuses the cutover while a registered reader lacks sqlite, and changes nothing", async () => {
    const root = await fileRoot();
    registerFileOnly(root, "factory");
    const result = await run(["cutover", "--root", root]);
    expect(result.code).toBe(3);
    expect(result.err).toContain("reader factory not ready: missing sqlite (has file)");
    expect(result.err).toContain("readers not ready for sqlite: factory");
    expect(backendOf(root)).toBe("file");
    expect(fs.existsSync(path.join(root, "backend-switches.jsonl"))).toBe(false);
  });

  it("passes once every reader proved sqlite with a real read of a migrated scratch copy", async () => {
    const root = await fileRoot();
    registerFileOnly(root, "factory");
    const proved = await run(["reader-proof", "--root", root, "--name", "factory", "--backend", "sqlite", "--reader-version", "factory-2", "--json"]);
    expect(proved.code).toBe(0);
    expect(JSON.parse(proved.out)).toMatchObject({ ok: true, entries: 2, proof: { name: "factory", version: "factory-2", backends: ["file", "sqlite"], provedEpoch: 0 } });
    expect(await run(["reader-proof", "--root", root, "--name", "bridge", "--backend", "sqlite"])).toMatchObject({ code: 0 });
    // The proof read a scratch copy: the live root is untouched.
    expect(backendOf(root)).toBe("file");
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);

    const switched = await run(["cutover", "--root", root, "--json"]);
    expect(switched.code).toBe(0);
    expect(JSON.parse(switched.out)).toMatchObject({ ok: true, epoch: 1, readers: { ready: ["bridge", "factory"], acceptedUnready: [] } });
    expect(backendOf(root)).toBe("moved");
    // Proofs at epoch 0 are stale now: a later switch from file needs fresh ones.
    const record = JSON.parse(fs.readFileSync(path.join(root, "backend-switches.jsonl"), "utf8").trim());
    expect(record).toMatchObject({ command: "cutover", backend: "sqlite", epoch: 1, readersReady: ["bridge", "factory"], acceptedUnready: [] });
  });

  it("records an --accept-unready override and refuses an override that misses a reader", async () => {
    const root = await fileRoot();
    registerFileOnly(root, "factory");
    fs.writeFileSync(path.join(root, "readers", "old.json"), JSON.stringify({ name: "old", version: "1", backends: ["sqlite"], provedAt: "2026-01-01T00:00:00Z", provedEpoch: 0 }));
    fs.writeFileSync(path.join(root, "readers", "broken.json"), "{");
    const partial = await run(["cutover", "--root", root, "--accept-unready", "factory"]);
    expect(partial.code).toBe(3);
    expect(partial.err).toContain("readers not ready for sqlite: broken");
    expect(backendOf(root)).toBe("file");

    const accepted = await run(["cutover", "--root", root, "--accept-unready", "factory,broken"]);
    expect(accepted.code).toBe(0);
    expect(accepted.err).toContain("reader factory not ready: missing sqlite (has file) (accepted by --accept-unready)");
    expect(backendOf(root)).toBe("moved");
    const record = JSON.parse(fs.readFileSync(path.join(root, "backend-switches.jsonl"), "utf8").trim());
    expect(record.readersReady).toEqual(["old"]);
    expect(record.acceptedUnready.map((reader: { name: string }) => reader.name)).toEqual(["broken", "factory"]);
  });

  it("marks a proof from an earlier epoch stale", async () => {
    const root = await fileRoot();
    expect((await run(["reader-proof", "--root", root, "--name", "factory", "--backend", "sqlite"])).code).toBe(0);
    expect((await run(["cutover", "--root", root])).code).toBe(0);
    expect((await run(["rollback", "--root", root])).code).toBe(0);
    const again = await run(["cutover", "--root", root]);
    expect(again.code).toBe(3);
    expect(again.err).toContain("reader factory not ready: stale: proved at epoch 0, mesh is at 2");
  });
});
