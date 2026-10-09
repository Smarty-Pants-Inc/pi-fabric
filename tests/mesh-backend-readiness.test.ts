import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { meshBackendStatus } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { readerReadiness, type ReaderProof } from "../src/mesh/reader-proof.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#7815: the switch refuses unless every registered reader proved it can read sqlite.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

const tempDir = (label: string): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-readiness-${label}-`));
  dirs.push(dir);
  return dir;
};

const fileRoot = async (): Promise<string> => {
  const root = tempDir("mesh");
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "seed/a", value: { n: 1 }, identity });
  await store.put({ key: "seed/b", value: "two", identity });
  store.closeState();
  return root;
};

/** A release root as the installer leaves it: releases/<sha> and current -> releases/<sha>. */
const installRoot = (release: string): string => {
  const root = tempDir("install");
  install(root, release);
  return root;
};
const install = (root: string, release: string): void => {
  fs.mkdirSync(path.join(root, "releases", release), { recursive: true });
  fs.rmSync(path.join(root, "current"), { force: true });
  fs.symlinkSync(path.join("releases", release), path.join(root, "current"));
};

/** A proof as a non-Fabric reader (the Python factory) writes it itself (docs/mesh-backend.md). */
const writeProof = (root: string, name: string, fields: Partial<ReaderProof> & Record<string, unknown> = {}): void => {
  fs.mkdirSync(path.join(root, "readers"), { recursive: true });
  const proof = { name, implementation: "python-factory", version: "factory-1", installRoot: fields.installRoot ?? installRoot("r1"),
    release: "r1", backends: ["file", "sqlite"], provedAt: new Date().toISOString(), provedEpoch: 0, ...fields };
  fs.writeFileSync(path.join(root, "readers", `${name}.json`), JSON.stringify(proof));
};

const run = async (argv: string[], options: Record<string, unknown> = {}): Promise<{ code: number; out: string; err: string }> => {
  let out = "";
  let err = "";
  const code = await main(argv, { census: async () => ({ writers: [] }), options, stdout: (text) => { out += text; }, stderr: (text) => { err += text; } });
  return { code, out, err };
};

const records = (root: string): Array<Record<string, unknown>> => {
  const file = path.join(root, "backend-switches.jsonl");
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>) : [];
};
const backendOf = async (root: string): Promise<string> => (await meshBackendStatus(root)).backend;

describe("fabric-mesh-backend readiness gate", () => {
  it("refuses the cutover while a registered reader lacks sqlite, and changes nothing", async () => {
    const root = await fileRoot();
    writeProof(root, "factory", { backends: ["file"] });
    const result = await run(["cutover", "--root", root]);
    expect(result.code).toBe(3);
    expect(result.err).toContain("reader factory not ready: missing sqlite (has file)");
    expect(await backendOf(root)).toBe("none");
    expect(records(root)).toEqual([]);
  });

  it("passes once every reader proved sqlite, and records intent and outcome with one switchId", async () => {
    const root = await fileRoot();
    writeProof(root, "factory");
    const fabric = installRoot("fabric-sha1");
    const proved = await run(["reader-proof", "--root", root, "--name", "fabric", "--backend", "sqlite", "--install-root", fabric, "--json"]);
    expect(proved.code, proved.err).toBe(0);
    expect(JSON.parse(proved.out)).toMatchObject({ ok: true, entries: 2,
      proof: { name: "fabric", implementation: "fabric-meshstore", installRoot: fabric, release: "fabric-sha1", backends: ["sqlite"], provedEpoch: 0 } });
    // The proof read a scratch copy: the live root is untouched.
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);

    const switched = await run(["cutover", "--root", root, "--json"]);
    expect(switched.code, switched.err).toBe(0);
    expect(JSON.parse(switched.out)).toMatchObject({ ok: true, epoch: 1, readers: { ready: ["fabric", "factory"], acceptedUnready: [] } });
    expect(await backendOf(root)).toBe("sqlite");
    const [intent, outcome] = records(root);
    expect(intent).toMatchObject({ phase: "intent", command: "cutover", fromEpoch: 0, readersReady: ["fabric", "factory"], acceptedUnready: [], acceptedEmptyRegistry: false });
    expect(outcome).toMatchObject({ switchId: intent!.switchId, phase: "outcome", ok: true, epoch: 1, underFence: true });
  });

  it("lets reader-proof prove only readers whose read path is Fabric's MeshStore", async () => {
    const root = await fileRoot();
    writeProof(root, "factory", { backends: ["file"] });
    const refused = await run(["reader-proof", "--root", root, "--name", "factory", "--backend", "sqlite", "--install-root", installRoot("x")]);
    expect(refused.code).toBe(1);
    expect(refused.err).toContain("must write its own proof");
    writeProof(root, "bridge", { implementation: "fabric-meshstore", installRoot: installRoot("b1"), backends: ["file"] });
    expect((await run(["reader-proof", "--root", root, "--name", "bridge", "--backend", "sqlite"])).code).toBe(0);
    expect(readerReadiness(root, "sqlite", 0).ready).toEqual(["bridge"]);
  });

  it("records an --accept-unready override in the intent and refuses an override that misses a reader", async () => {
    const root = await fileRoot();
    writeProof(root, "factory", { backends: ["file"] });
    writeProof(root, "old");
    fs.writeFileSync(path.join(root, "readers", "broken.json"), "{");
    const partial = await run(["cutover", "--root", root, "--accept-unready", "factory"]);
    expect(partial.code).toBe(3);
    expect(partial.err).toContain("readers not ready for sqlite: broken (invalid JSON");
    expect(records(root)).toEqual([]);

    const accepted = await run(["cutover", "--root", root, "--accept-unready", "factory,broken"]);
    expect(accepted.code, accepted.err).toBe(0);
    expect(accepted.err).toContain("reader factory not ready: missing sqlite (has file) (accepted by --accept-unready)");
    const [intent, outcome] = records(root);
    expect(intent).toMatchObject({ phase: "intent", readersReady: ["old"] });
    expect((intent!.acceptedUnready as Array<{ name: string }>).map(reader => reader.name)).toEqual(["broken", "factory"]);
    expect(outcome).toMatchObject({ switchId: intent!.switchId, ok: true });
  });

  it("requires provedEpoch to equal the epoch and a fresh provedAt: future, stale and over-age proofs are unready", async () => {
    const root = await fileRoot();
    const hour = 60 * 60 * 1000;
    writeProof(root, "future-epoch", { provedEpoch: 1 });
    writeProof(root, "over-age", { provedAt: new Date(Date.now() - 25 * hour).toISOString() });
    writeProof(root, "future-time", { provedAt: new Date(Date.now() + 10 * 60 * 1000).toISOString() });
    writeProof(root, "skew-ok", { provedAt: new Date(Date.now() + 2 * 60 * 1000).toISOString() });
    writeProof(root, "fraction", { provedEpoch: 0.5 });
    writeProof(root, "relative", { installRoot: "relative/root" });
    writeProof(root, "extra", { trusted: true });
    const reasons = Object.fromEntries(readerReadiness(root, "sqlite", 0).unready.map(reader => [reader.name, reader.reason]));
    expect(reasons).toEqual({
      "future-epoch": "future epoch: proved at epoch 1, mesh is at 0",
      "over-age": expect.stringContaining("over-age"),
      "future-time": expect.stringContaining("is in the future"),
      fraction: "invalid proof: provedEpoch",
      relative: "invalid proof: installRoot must be an absolute path",
      extra: "invalid proof: unknown field trusted",
    });
    expect(readerReadiness(root, "sqlite", 0).ready).toEqual(["skew-ok"]);

    // Stale: proved at epoch 0, then cutover (1) and rollback (2).
    const other = await fileRoot();
    const fabric = installRoot("f1");
    expect((await run(["reader-proof", "--root", other, "--name", "fabric", "--backend", "sqlite", "--install-root", fabric])).code).toBe(0);
    expect((await run(["cutover", "--root", other])).code).toBe(0);
    expect((await run(["rollback", "--root", other])).code).toBe(0);
    const again = await run(["cutover", "--root", other]);
    expect(again.code).toBe(3);
    expect(again.err).toContain("reader fabric not ready: stale: proved at epoch 0, mesh is at 2");
  });

  it("counts a listed reader it cannot read as unready, never skipped", async () => {
    const root = await fileRoot();
    writeProof(root, "good");
    fs.symlinkSync(path.join(root, "nowhere.json"), path.join(root, "readers", "ghost.json"));
    fs.mkdirSync(path.join(root, "readers", "dir.json"));
    const readiness = readerReadiness(root, "sqlite", 0);
    expect(readiness.ready).toEqual(["good"]);
    expect(readiness.unready).toEqual([
      { name: "dir", reason: "not a regular file (other)" },
      { name: "ghost", reason: "not a regular file (symlink)" },
    ]);
    const result = await run(["cutover", "--root", root]);
    expect(result.code).toBe(3);
    expect(result.err).toContain("reader ghost not ready: not a regular file (symlink)");
  });

  it("refuses an absent or empty registry unless --accept-empty-registry, and records that override", async () => {
    const root = await fileRoot();
    const absent = await run(["cutover", "--root", root]);
    expect(absent.code).toBe(3);
    expect(absent.err).toContain("no reader registered (no readers/ directory)");
    fs.mkdirSync(path.join(root, "readers"));
    const empty = await run(["cutover", "--root", root]);
    expect(empty.code).toBe(3);
    expect(empty.err).toContain("no reader registered (readers/ is empty)");
    expect(await backendOf(root)).toBe("none");

    const accepted = await run(["cutover", "--root", root, "--accept-empty-registry"]);
    expect(accepted.code, accepted.err).toBe(0);
    expect(records(root)[0]).toMatchObject({ phase: "intent", acceptedEmptyRegistry: true, readersReady: [] });
  });

  it("re-checks under the fence: a proof changed after the scan refuses the commit with nothing switched", async () => {
    const root = await fileRoot();
    writeProof(root, "factory");
    const result = await run(["cutover", "--root", root], {
      // Runs under the fence right before the CLI's own re-check: the reader withdraws its sqlite proof.
      beforeCommit: () => writeProof(root, "factory", { backends: ["file"] }),
    });
    expect(result.code).toBe(3);
    expect(result.err).toContain("readers not ready for sqlite (under the fence, before the commit): factory (missing sqlite");
    expect(await backendOf(root)).toBe("file");
    expect(JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8"))).toHaveProperty("entries");
    const [intent, outcome] = records(root);
    expect(outcome).toMatchObject({ switchId: intent!.switchId, phase: "outcome", ok: false, refused: true });
  });

  it("refuses when the switch record cannot be written", async () => {
    const root = await fileRoot();
    writeProof(root, "factory");
    fs.mkdirSync(path.join(root, "backend-switches.jsonl"));
    const result = await run(["cutover", "--root", root]);
    expect(result.code).toBe(3);
    expect(result.err).toContain("cannot write the switch record");
    expect(await backendOf(root)).toBe("none");
  });

  it("binds a proof to the installed release: an upgrade since the proof is unready", async () => {
    const root = await fileRoot();
    const factory = installRoot("r1");
    writeProof(root, "factory", { installRoot: factory, release: "r1" });
    expect(readerReadiness(root, "sqlite", 0).ready).toEqual(["factory"]);
    install(factory, "r2");
    const result = await run(["cutover", "--root", root]);
    expect(result.code).toBe(3);
    expect(result.err).toContain("reader factory not ready: release mismatch: proved r1, installed r2");
    fs.rmSync(path.join(factory, "current"));
    expect(readerReadiness(root, "sqlite", 0).unready[0]!.reason).toContain("installed release unresolvable");
  });
});
