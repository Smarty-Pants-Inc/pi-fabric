import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MeshBackendRefusedError } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#9404 (pi-fabric#884 round 1): a non-Fabric reader's proof step imports a scratch COPY of state.json without
// the readiness gate. The CLI makes that copy itself (`import --scratch-proof-from <state.json>`), so the gate is never
// skipped on a caller-named root: a private directory is not proof of a copy (CODE P1).
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
const mode = (file: string) => fs.statSync(file).mode & 0o777;

describe.skipIf(process.platform === "win32")("a non-Fabric reader's documented proof steps (smarty-dev#9404)", () => {
  it("the documented steps, run through the real CLI, give a proof the gate accepts", async () => {
    const mesh = await fileRoot();
    const factory = { name: "factory", installRoot: installRoot("factory-rel-1") };
    const release = path.basename(fs.realpathSync(path.join(factory.installRoot, "current")));
    const provedEpoch = JSON.parse((await cli(["status", "--root", mesh, "--json"])).out).epoch as number;
    // 3. the CLI makes and imports its own scratch copy of the live state.json.
    const imported = await cli(["import", "--scratch-proof-from", path.join(mesh, "state.json"), "--json"], { builtinReaders: [factory] });
    expect(imported.code, imported.err).toBe(0);
    const result = JSON.parse(imported.out);
    expect(result).toMatchObject({ ok: true, scratchProof: true, entries: 2, root: expect.any(String) });
    dirs.push(result.root);
    // 4. read every entry with a SQLite read path; then remove the scratch copy.
    const reader = new MeshStore(result.root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    expect(reader.listAll("seed/")).toHaveLength(result.entries);
    reader.closeState();
    fs.rmSync(result.root, { recursive: true, force: true });
    // 5. the proof, installed under the fence; the gate then accepts the live switch with no override.
    const proofFile = path.join(tempDir("proof"), "factory.json");
    fs.writeFileSync(proofFile, JSON.stringify({ name: "factory", implementation: "python-factory", version: "factory-1", release,
      backends: ["file", "sqlite"], provedAt: new Date().toISOString(), provedEpoch }), { mode: 0o600 });
    expect((await cli(["reader-proof", "--root", mesh, "--name", "factory", "--proof-file", proofFile])).code).toBe(0);
    const switched = await cli(["import", "--root", mesh, "--json"], { builtinReaders: [factory] });
    expect(switched.code, switched.err).toBe(0);
    expect(JSON.parse(switched.out).readers).toMatchObject({ ready: expect.arrayContaining(["factory"]), acceptedUnready: [] });
  });

  it("--scratch-proof-from makes its own 0700 directory in --scratch-parent, holding the imported state.db; the source is unchanged", async () => {
    const mesh = await fileRoot();
    const parent = tempDir("parent");
    const before = fs.readFileSync(path.join(mesh, "state.json"));
    const imported = await cli(["import", "--scratch-proof-from", path.join(mesh, "state.json"), "--scratch-parent", parent, "--json"]);
    expect(imported.code, imported.err).toBe(0);
    const { root } = JSON.parse(imported.out);
    expect(path.dirname(root)).toBe(parent);
    expect(mode(root)).toBe(0o700);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
    expect(fs.readFileSync(path.join(mesh, "state.json")).equals(before)).toBe(true);
    expect(fs.readdirSync(mesh).filter(name => name.startsWith("state.db"))).toEqual([]);
  });

  it("a refused scratch import removes its own directory", async () => {
    const mesh = await fileRoot();
    const parent = tempDir("parent");
    const refused = await cli(["import", "--scratch-proof-from", path.join(mesh, "state.json"), "--scratch-parent", parent, "--json"], {
      options: { beforeCommit: () => { throw new MeshBackendRefusedError("refused under the fence"); } } });
    expect(refused.code).toBe(3);
    expect(JSON.parse(refused.out)).toMatchObject({ ok: false, refused: true, scratchProof: false, removed: true });
    // Only the shared per-user mesh lock helper the fence keeps beside a root remains, not the scratch directory.
    expect(fs.readdirSync(parent).filter(name => name.startsWith("fabric-scratch-proof-"))).toEqual([]);
  });

  it("never skips the gate on a caller-named root: a 0700 directory holding only state.json is not accepted as a copy", async () => {
    const mesh = await fileRoot();
    const lookalike = tempDir("lookalike");
    fs.chmodSync(lookalike, 0o700);
    fs.copyFileSync(path.join(mesh, "state.json"), path.join(lookalike, "state.json"));
    const factory = { name: "factory", installRoot: installRoot("factory-rel-1") };
    // The former --root form is gone.
    expect((await cli(["import", "--root", lookalike, "--scratch-proof", "--json"], { builtinReaders: [factory] })).code).toBe(2);
    expect((await cli(["import", "--root", lookalike, "--scratch-proof-from", path.join(lookalike, "state.json")], { builtinReaders: [factory] })).code).toBe(2);
    // A plain import of that root meets the gate: the factory has no proof.
    const gated = await cli(["import", "--root", lookalike, "--json"], { builtinReaders: [factory] });
    expect(gated.code).toBe(3);
    expect(gated.err).toMatch(/readers not ready for sqlite/);
  });

  it.each([
    ["a group-writable --scratch-parent", async (parent: string) => { fs.chmodSync(parent, 0o775); return parent; }],
    ["a symlinked --scratch-parent", async (parent: string) => { const link = path.join(tempDir("link"), "parent"); fs.symlinkSync(parent, link); return link; }],
  ])("refuses %s and creates nothing", async (_name, prepare) => {
    const mesh = await fileRoot();
    const parent = tempDir("parent");
    const given = await prepare(parent);
    const refused = await cli(["import", "--scratch-proof-from", path.join(mesh, "state.json"), "--scratch-parent", given, "--json"]);
    expect(refused.code).toBe(3);
    expect(JSON.parse(refused.out).error).toMatch(/--scratch-parent chain/);
    expect(fs.readdirSync(parent)).toEqual([]);
  });

  it("refuses a state.json source that is a symlink", async () => {
    const mesh = await fileRoot();
    const link = path.join(tempDir("src"), "state.json");
    fs.symlinkSync(path.join(mesh, "state.json"), link);
    const parent = tempDir("parent");
    const refused = await cli(["import", "--scratch-proof-from", link, "--scratch-parent", parent, "--json"]);
    expect(refused.code).not.toBe(0);
    expect(fs.readdirSync(parent)).toEqual([]);
  });
});
