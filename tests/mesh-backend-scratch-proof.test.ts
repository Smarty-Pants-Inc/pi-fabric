import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MeshBackendRefusedError } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#9404: docs/mesh-backend.md "Any other reader" step 3 tells a non-Fabric reader (the factory) to import a
// copy of state.json in an empty scratch directory. On main the readiness gate refused there (the reader being
// proved has no proof yet; the import's own new state.db reads as fabric@unknown), and a refused import left state.db
// beside the unchanged file-format state.json.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const dirs: string[] = [];
const savedRelease = process.env.PI_FABRIC_RELEASE_SHA;
beforeEach(() => { process.env.PI_FABRIC_RELEASE_SHA = "fabric-rel-a"; });
afterEach(() => {
  if (savedRelease === undefined) delete process.env.PI_FABRIC_RELEASE_SHA; else process.env.PI_FABRIC_RELEASE_SHA = savedRelease;
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const tempDir = (label: string): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-scratch-${label}-`)); dirs.push(dir); return dir; };
const fileRoot = async (): Promise<string> => {
  const root = tempDir("mesh");
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "seed/a", value: { n: 1 }, identity });
  await store.put({ key: "seed/b", value: "two", identity });
  store.closeState();
  return root;
};
/** A release root as the installer leaves it, for the factory's trusted install root. */
const installRoot = (release: string): string => {
  const root = tempDir("install");
  fs.mkdirSync(path.join(root, "releases", release), { recursive: true });
  fs.symlinkSync(path.join("releases", release), path.join(root, "current"));
  return root;
};
/** The real CLI entry; the factory is the built-in reader; no other process is scanned as a state.db holder. */
const cli = async (argv: string[], io: Parameters<typeof main>[1] = {}) => {
  let out = "", err = "";
  const code = await main(argv, { procScan: { listPids: () => [] }, gateCensus: () => ({ writers: [], unknown: [] }), ...io,
    stdout: text => { out += text; }, stderr: text => { err += text; } });
  return { code, out, err };
};
/** Step 3 verbatim: copy state.json into an empty scratch directory (created private, 0700). */
const scratchCopy = (mesh: string): string => {
  const scratch = tempDir("copy");
  fs.chmodSync(scratch, 0o700);
  fs.copyFileSync(path.join(mesh, "state.json"), path.join(scratch, "state.json"));
  return scratch;
};
const dbFiles = (root: string) => fs.readdirSync(root).filter(name => name.startsWith("state.db"));

describe.skipIf(process.platform === "win32")("a non-Fabric reader's documented proof steps (smarty-dev#9404)", () => {
  it("the documented steps, run through the real CLI, give a proof the gate accepts", async () => {
    const mesh = await fileRoot();
    const factory = { name: "factory", installRoot: installRoot("factory-rel-1") };
    // 1. release; 2. provedEpoch from status --json.
    const release = path.basename(fs.realpathSync(path.join(factory.installRoot, "current")));
    const status = await cli(["status", "--root", mesh, "--json"]);
    const provedEpoch = JSON.parse(status.out).epoch as number;
    // 3. the scratch import.
    const scratch = scratchCopy(mesh);
    const imported = await cli(["import", "--root", scratch, "--scratch-proof", "--json"], { builtinReaders: [factory] });
    expect(imported.code, imported.err).toBe(0);
    const result = JSON.parse(imported.out);
    expect(result).toMatchObject({ ok: true, scratchProof: true, entries: 2 });
    // 4. read every entry from the scratch copy with a SQLite read path; the count must equal the import's entries.
    const reader = new MeshStore(scratch, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    expect(reader.listAll("seed/")).toHaveLength(result.entries);
    reader.closeState();
    // 5. the proof, installed under the fence.
    const proofFile = path.join(tempDir("proof"), "factory.json");
    fs.writeFileSync(proofFile, JSON.stringify({ name: "factory", implementation: "python-factory", version: "factory-1", release,
      backends: ["file", "sqlite"], provedAt: new Date().toISOString(), provedEpoch }), { mode: 0o600 });
    const proved = await cli(["reader-proof", "--root", mesh, "--name", "factory", "--proof-file", proofFile]);
    expect(proved.code, proved.err).toBe(0);
    // The gate accepts the factory's proof: the live switch runs with no --accept-unready.
    const switched = await cli(["import", "--root", mesh, "--json"], { builtinReaders: [factory] });
    expect(switched.code, switched.err).toBe(0);
    expect(JSON.parse(switched.out).readers).toMatchObject({ ready: expect.arrayContaining(["factory"]), acceptedUnready: [] });
  });

  it("a refused import leaves no state.db and an unchanged state.json", async () => {
    const mesh = await fileRoot();
    const scratch = scratchCopy(mesh);
    const before = fs.readFileSync(path.join(scratch, "state.json"));
    // The gate refuses under the fence, after the import opened its own state.db (main's fabric@unknown case).
    const refused = await cli(["import", "--root", scratch, "--accept-unready", "factory", "--json"], {
      builtinReaders: [{ name: "factory", installRoot: installRoot("factory-rel-1") }],
      options: { beforeCommit: () => { throw new MeshBackendRefusedError("readers not ready for sqlite (under the fence): fabric@unknown"); } },
    });
    expect(refused.code).toBe(3);
    expect(dbFiles(scratch)).toEqual([]);
    expect(fs.readFileSync(path.join(scratch, "state.json")).equals(before)).toBe(true);
    expect(JSON.parse(refused.out).rolledBack).toEqual(expect.arrayContaining(["state.db"]));
  });

  it.each([
    ["a readers/ directory", (root: string) => fs.mkdirSync(path.join(root, "readers"), { mode: 0o700 })],
    ["a writer census", (root: string) => fs.writeFileSync(path.join(root, ".writer-census"), "")],
    ["a switch record", (root: string) => fs.writeFileSync(path.join(root, "backend-switches.jsonl"), "")],
    ["an actors/ directory", (root: string) => fs.mkdirSync(path.join(root, "actors"))],
    ["mode 0755", (root: string) => fs.chmodSync(root, 0o755)],
  ])("--scratch-proof refuses a live-looking root with %s and changes nothing", async (_name, liven) => {
    const mesh = await fileRoot();
    const root = scratchCopy(mesh);
    liven(root);
    const listing = fs.readdirSync(root).sort();
    const refused = await cli(["import", "--root", root, "--scratch-proof", "--json"]);
    expect(refused.code).toBe(3);
    expect(JSON.parse(refused.out)).toMatchObject({ ok: false, refused: true, scratchProof: false, error: expect.stringMatching(/--scratch-proof/) });
    expect(fs.readdirSync(root).sort()).toEqual(listing);
  });

  it("--scratch-proof refuses the live mesh itself", async () => {
    const mesh = await fileRoot();
    fs.chmodSync(mesh, 0o700);
    const refused = await cli(["import", "--root", mesh, "--scratch-proof", "--json"]);
    expect(refused.code).toBe(3);
    expect(dbFiles(mesh)).toEqual([]);
  });
});
