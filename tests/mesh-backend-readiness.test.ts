import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { meshBackendStatus, type MeshBackendOptions } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { openNodeSqlite } from "../src/mesh/state-sqlite.js";
import { leaseHelperPath, probeStateDbLeases, proveFabricReader, procLocksKey, readerReadiness, requiredReaders, type InstalledReader, type LeaseProbeOptions, type ProcScanOptions, type ReaderProof } from "../src/mesh/reader-proof.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#7815: the switch refuses unless every REQUIRED reader (the inventory: live Fabric releases,
// built-in installed readers, --require-reader) proved it can read sqlite.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const dirs: string[] = [];
const savedRelease = process.env.PI_FABRIC_RELEASE_SHA;
beforeEach(() => { process.env.PI_FABRIC_RELEASE_SHA = "fabric-rel-a"; });
afterEach(() => {
  if (savedRelease === undefined) delete process.env.PI_FABRIC_RELEASE_SHA; else process.env.PI_FABRIC_RELEASE_SHA = savedRelease;
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

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
const install = (root: string, release: string): void => {
  fs.mkdirSync(path.join(root, "releases", release), { recursive: true });
  fs.rmSync(path.join(root, "current"), { force: true });
  fs.symlinkSync(path.join("releases", release), path.join(root, "current"));
};
const installRoot = (release: string): string => {
  const root = tempDir("install");
  install(root, release);
  return root;
};

/** The proof the Python factory makes with its own read code (docs/mesh-backend.md). */
const factoryProof = (fields: Partial<ReaderProof> & Record<string, unknown> = {}): Record<string, unknown> => ({
  name: "factory", implementation: "python-factory", version: "factory-1", release: "r1", backends: ["file", "sqlite"],
  provedAt: new Date().toISOString(), provedEpoch: 0, ...fields });
const placeProof = (root: string, proof: Record<string, unknown>): void => {
  fs.mkdirSync(path.join(root, "readers"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, "readers", `${String(proof.name)}.json`), JSON.stringify(proof), { mode: 0o600 });
};

type Writer = { pid: number; release: string; mode: string };
interface Harness { census: () => Promise<{ writers: Writer[] }>; builtins: InstalledReader[]; writers: Writer[] | (() => Writer[]); unknown: Writer[] | (() => Writer[]); options: Partial<MeshBackendOptions>;
  /** Default: scan no other pid (this host's own ssh sessions would be ambiguous holders). */
  procScan: ProcScanOptions;
  leaseProbe: LeaseProbeOptions }
const run = async (argv: string[], harness: Partial<Harness> = {}): Promise<{ code: number; out: string; err: string }> => {
  let out = "";
  let err = "";
  const writers = (): Writer[] => typeof harness.writers === "function" ? harness.writers() : harness.writers ?? [];
  const code = await main(argv, { census: harness.census ?? (async () => ({ writers: writers() })), gateCensus: () => ({ writers: writers(),
    unknown: typeof harness.unknown === "function" ? harness.unknown() : harness.unknown ?? [] }),
    builtinReaders: harness.builtins ?? [],
    procScan: harness.procScan ?? { listPids: () => [] }, ...(harness.leaseProbe ? { leaseProbe: harness.leaseProbe } : {}), options: harness.options ?? {}, stdout: (text) => { out += text; }, stderr: (text) => { err += text; } });
  return { code, out, err };
};

/** Installs a factory proof through the CLI (under the fence), as the Python writer must. */
const proveFactory = async (root: string, fields: Partial<ReaderProof> = {}): Promise<number> => {
  const file = path.join(tempDir("proof"), "factory.json");
  fs.writeFileSync(file, JSON.stringify(factoryProof(fields)));
  return (await run(["reader-proof", "--root", root, "--name", "factory", "--proof-file", file])).code;
};

const records = (root: string): Array<Record<string, unknown>> => {
  const file = path.join(root, "backend-switches.jsonl");
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>) : [];
};
const backendOf = async (root: string): Promise<string> => (await meshBackendStatus(root)).backend;

// ponytail: these tests need proofs, which fail closed without POSIX owners (Windows ACLs: smarty-dev#7548);
// the fail-closed behaviour itself is covered below on every platform.
describe.skipIf(process.platform === "win32")("fabric-mesh-backend readiness gate", () => {
  it("requires the built-in installed reader: no proof, or a proof without sqlite, refuses with nothing changed", async () => {
    const root = await fileRoot();
    const builtins = [{ name: "factory", installRoot: installRoot("r1") }];
    const none = await run(["cutover", "--root", root], { builtins });
    expect(none.code).toBe(3);
    expect(none.err).toContain("factory (no proof from installed reader factory (release r1))");
    expect(await proveFactory(root, { backends: ["file"] })).toBe(0);
    const fileOnly = await run(["cutover", "--root", root], { builtins });
    expect(fileOnly.code).toBe(3);
    expect(fileOnly.err).toContain("reader factory not ready: missing sqlite (has file)");
    expect(await backendOf(root)).toBe("none");
    expect(records(root)).toEqual([]);
    // Not installed on this host: not required.
    expect(requiredReaders([], [], [{ name: "factory", installRoot: path.join(root, "absent") }], [])).toEqual([]);
  });

  it("passes when every required reader proved sqlite, re-checks before both commits, and records intent and outcome", async () => {
    const root = await fileRoot();
    const builtins = [{ name: "factory", installRoot: installRoot("r1") }];
    const writers = [{ pid: 4242, release: "fabric-rel-a", mode: "sqlite" }];
    expect(await proveFactory(root)).toBe(0);
    const proved = await run(["reader-proof", "--root", root, "--backend", "sqlite", "--json"]);
    expect(proved.code, proved.err).toBe(0);
    expect(JSON.parse(proved.out)).toMatchObject({ ok: true, entries: 2,
      proof: { name: "fabric-fabric-rel-a", implementation: "fabric-meshstore", release: "fabric-rel-a", backends: ["sqlite"], provedEpoch: 0 } });
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);

    const switched = await run(["cutover", "--root", root, "--json"], { builtins, writers });
    expect(switched.code, switched.err).toBe(0);
    expect(JSON.parse(switched.out)).toMatchObject({ ok: true, epoch: 1,
      readers: { ready: ["factory", "fabric@fabric-rel-a"], acceptedUnready: [], checkedUnderFence: ["importing", "sqlite"] } });
    expect(await backendOf(root)).toBe("sqlite");
    const [intent, outcome] = records(root);
    expect(intent).toMatchObject({ phase: "intent", command: "cutover", fromEpoch: 0, toEpoch: 1, readersReady: ["factory", "fabric@fabric-rel-a"] });
    expect(outcome).toMatchObject({ switchId: intent!.switchId, phase: "outcome", ok: true, checkedUnderFence: ["importing", "sqlite"] });
  });

  it("refuses a live Fabric writer on a release with no proof, and one without a release", async () => {
    const root = await fileRoot();
    expect((await run(["reader-proof", "--root", root, "--backend", "sqlite"])).code).toBe(0); // fabric-rel-a
    const old = await run(["cutover", "--root", root], { writers: [{ pid: 7, release: "fabric-rel-old", mode: "file" }] });
    expect(old.code).toBe(3);
    expect(old.err).toContain("fabric@fabric-rel-old (no proof from installed reader fabric release fabric-rel-old (live: pid 7 file))");
    const anonymous = await run(["cutover", "--root", root], { writers: [{ pid: 8, release: "unknown", mode: "sqlite" }] });
    expect(anonymous.code).toBe(3);
    expect(anonymous.err).toContain("fabric@unknown (live writer evidence without an attributable release: pid 8 sqlite)");
    // Evidence the census cannot attribute (a pid-only lock owner, a torn record) is required too.
    const lockOwner = await run(["cutover", "--root", root], { unknown: [{ pid: 9, release: "unknown", mode: "unknown lock-owner .lock" }] });
    expect(lockOwner.code).toBe(3);
    expect(lockOwner.err).toContain("fabric@unknown (live writer evidence without an attributable release: pid 9 unknown lock-owner .lock)");
    expect(await backendOf(root)).toBe("none");
    // --accept-unready is the override, recorded in the intent before the switch.
    const accepted = await run(["cutover", "--root", root, "--accept-unready", "fabric@unknown"], { writers: [{ pid: 8, release: "unknown", mode: "sqlite" }] });
    expect(accepted.code, accepted.err).toBe(0);
    expect(records(root)[0]).toMatchObject({ phase: "intent", acceptedUnready: [{ name: "fabric@unknown" }] });
  });

  it("lets reader-proof prove only this Fabric release, and install only non-Fabric proofs", async () => {
    const root = await fileRoot();
    expect((await run(["reader-proof", "--root", root, "--backend", "sqlite", "--name", "factory"])).code).toBe(1);
    const file = path.join(tempDir("proof"), "p.json");
    fs.writeFileSync(file, JSON.stringify(factoryProof({ implementation: "fabric-meshstore" })));
    const reserved = await run(["reader-proof", "--root", root, "--name", "factory", "--proof-file", file]);
    expect(reserved.code).toBe(1);
    expect(reserved.err).toContain("reserved");
    fs.writeFileSync(file, JSON.stringify({ ...factoryProof(), name: "fabric-x" }));
    expect((await run(["reader-proof", "--root", root, "--name", "fabric-x", "--proof-file", file])).code).toBe(1);
  });

  it("takes the install root from the inventory: a decoy installRoot in the proof is ignored", async () => {
    const root = await fileRoot();
    const trusted = installRoot("r1");
    const decoy = installRoot("r9");
    const builtins = [{ name: "factory", installRoot: trusted }];
    expect(await proveFactory(root, { installRoot: decoy, release: "r9" })).toBe(0);
    const refused = await run(["cutover", "--root", root], { builtins });
    expect(refused.code).toBe(3);
    expect(refused.err).toContain(`release mismatch: proved r9, installed r1 (${trusted})`);
    expect(await proveFactory(root, { installRoot: decoy, release: "r1" })).toBe(0);
    expect((await run(["cutover", "--root", root], { builtins })).code).toBe(0);
  });

  // ponytail: POSIX mode bits and uid only; Windows has neither (the gate does not check owners there).
  // Windows ACL checks of the registry: smarty-dev#7548.
  it.skipIf(process.platform === "win32")("refuses a registry that is group/other-writable or foreign-owned, and a proof file that is", async () => {
    const root = await fileRoot();
    const factory = installRoot("r1");
    const required = requiredReaders([], [], [{ name: "factory", installRoot: factory }], []);
    placeProof(root, factoryProof());
    expect(readerReadiness(root, "sqlite", required, { fromEpoch: 0, toEpoch: 1 }).ready).toEqual(["factory"]);
    expect(() => readerReadiness(root, "sqlite", required, { fromEpoch: 0, toEpoch: 1 }, Date.now(), process.geteuid!() + 1))
      .toThrow(/owned by uid \d+, not \d+/);
    fs.chmodSync(path.join(root, "readers", "factory.json"), 0o666);
    expect(readerReadiness(root, "sqlite", required, { fromEpoch: 0, toEpoch: 1 }).unready[0]!.reason).toMatch(/proof file is group\/other-writable/);
    fs.chmodSync(path.join(root, "readers"), 0o777);
    const result = await run(["cutover", "--root", root, "--accept-unready", "factory"], { builtins: [{ name: "factory", installRoot: factory }] });
    expect(result.code).toBe(3);
    expect(result.err).toContain("is group/other-writable (mode 777)");
    expect(await backendOf(root)).toBe("none");
  });

  it("checks schema, epoch and age: future, stale and over-age proofs are unready", async () => {
    const root = await fileRoot();
    const hour = 60 * 60 * 1000;
    const names = ["future-epoch", "stale", "over-age", "future-time", "skew-ok", "fraction", "extra"];
    const required = requiredReaders([], [], [], names.map(name => ({ name, installRoot: installRoot("r1") })));
    placeProof(root, factoryProof({ name: "future-epoch", provedEpoch: 2 }));
    placeProof(root, factoryProof({ name: "stale", provedEpoch: 0 }));
    placeProof(root, factoryProof({ name: "over-age", provedEpoch: 1, provedAt: new Date(Date.now() - 25 * hour).toISOString() }));
    placeProof(root, factoryProof({ name: "future-time", provedEpoch: 1, provedAt: new Date(Date.now() + 10 * 60 * 1000).toISOString() }));
    placeProof(root, factoryProof({ name: "skew-ok", provedEpoch: 1, provedAt: new Date(Date.now() + 2 * 60 * 1000).toISOString() }));
    placeProof(root, factoryProof({ name: "fraction", provedEpoch: 1.5 }));
    placeProof(root, factoryProof({ name: "extra", provedEpoch: 1, trusted: true }));
    const readiness = readerReadiness(root, "sqlite", required, { fromEpoch: 1, toEpoch: 2 });
    expect(Object.fromEntries(readiness.unready.map(reader => [reader.name, reader.reason]))).toEqual({
      stale: "stale: proved at epoch 0, the switch starts at 1",
      "over-age": expect.stringContaining("over-age"),
      "future-time": expect.stringContaining("is in the future"),
      fraction: "invalid proof: provedEpoch",
      extra: "invalid proof: unknown field trusted",
    });
    // provedEpoch 2 = toEpoch: proved during this switch (a crashed importing); 3 would be future.
    expect(readiness.ready).toEqual(["future-epoch", "skew-ok"]);
    expect(readerReadiness(root, "sqlite", required, { fromEpoch: 0, toEpoch: 1 }).unready.find(reader => reader.name === "future-epoch")!.reason)
      .toBe("future epoch: proved at epoch 2, the switch starts at 0");
  });

  it("counts a broken-symlink proof of a required reader as unready", async () => {
    const root = await fileRoot();
    fs.mkdirSync(path.join(root, "readers"), { mode: 0o700 });
    fs.symlinkSync(path.join(root, "nowhere.json"), path.join(root, "readers", "factory.json"));
    const result = await run(["cutover", "--root", root], { builtins: [{ name: "factory", installRoot: installRoot("r1") }] });
    expect(result.code).toBe(3);
    expect(result.err).toContain("reader factory not ready: not a regular file (symlink)");
  });

  it("re-checks under the fence before the sqlite commit: a proof removed or replaced after the scan refuses, and the rerun is gated", async () => {
    const root = await fileRoot();
    const builtins = [{ name: "factory", installRoot: installRoot("r1") }];
    expect(await proveFactory(root)).toBe(0);
    const proofFile = path.join(root, "readers", "factory.json");
    const removed = await run(["cutover", "--root", root], { builtins,
      options: { beforeCommit: (commit) => { if (commit.phase === "sqlite") fs.rmSync(proofFile); } } });
    expect(removed.code).toBe(3);
    expect(removed.err).toContain("readers not ready for sqlite (under the fence, before the sqlite commit): factory (no proof from installed reader factory");
    expect(await backendOf(root)).toBe("importing");
    const [intent, outcome] = records(root);
    expect(outcome).toMatchObject({ switchId: intent!.switchId, phase: "outcome", ok: false, refused: true });

    // The crashed `importing` rerun is gated too; a replacement without sqlite is seen.
    expect((await run(["cutover", "--root", root], { builtins })).code).toBe(3);
    expect(await proveFactory(root)).toBe(0);
    const replaced = await run(["cutover", "--root", root], { builtins,
      options: { beforeCommit: (commit) => { if (commit.phase === "sqlite") fs.writeFileSync(proofFile, JSON.stringify(factoryProof({ backends: ["file"] }))); } } });
    expect(replaced.code).toBe(3);
    expect(replaced.err).toContain("factory (missing sqlite (has file))");
    expect(await proveFactory(root)).toBe(0);
    const done = await run(["cutover", "--root", root, "--json"], { builtins });
    expect(done.code, done.err).toBe(0);
    expect(JSON.parse(done.out).readers.checkedUnderFence).toEqual(["sqlite"]);
    expect(await backendOf(root)).toBe("sqlite");
  });

  it("gates the rerun of an import that crashed at backend=importing", async () => {
    const root = await fileRoot();
    const builtins = [{ name: "factory", installRoot: installRoot("r1") }];
    expect(await proveFactory(root)).toBe(0);
    const crashed = await run(["cutover", "--root", root], { builtins,
      options: { onStep: (step) => { if (step === "import-commit") throw new Error("killed"); } } });
    expect(crashed.code).toBe(1);
    expect(await backendOf(root)).toBe("importing");
    fs.rmSync(path.join(root, "readers", "factory.json"));
    const rerun = await run(["cutover", "--root", root], { builtins });
    expect(rerun.code).toBe(3);
    expect(rerun.err).toContain("no proof from installed reader factory");
    expect(await proveFactory(root)).toBe(0);
    const done = await run(["cutover", "--root", root, "--json"], { builtins });
    expect(done.code, done.err).toBe(0);
    expect(JSON.parse(done.out).readers.checkedUnderFence).toEqual(["importing", "sqlite"]);
  });

  it("recomputes the inventory under the fence: a writer on a new release that appears before the commit refuses it", async () => {
    const root = await fileRoot();
    expect((await run(["reader-proof", "--root", root, "--backend", "sqlite"])).code).toBe(0); // fabric-rel-a
    let live: Writer[] = [{ pid: 7, release: "fabric-rel-a", mode: "sqlite" }];
    const result = await run(["cutover", "--root", root], { writers: () => live,
      options: { beforeCommit: () => { live = [...live, { pid: 11, release: "fabric-rel-new", mode: "sqlite" }]; } } });
    expect(result.code).toBe(3);
    expect(result.err).toContain("(under the fence, before the importing commit): fabric@fabric-rel-new (no proof from installed reader fabric release fabric-rel-new");
    expect(await backendOf(root)).toBe("file");
  });

  // ponytail: the holder scan reads /proc and /proc/locks, Linux only (smarty-dev#7936); off Linux it
  // fails closed, covered by "fails closed off Linux" below.
  it.skipIf(process.platform !== "linux")("ignores the gate's own state.db connection: no override needed, its fds recorded", async () => {
    const root = await fileRoot();
    // Every pid of this host is scanned except the gate's own; listPids keeps other users' sessions out.
    const result = await run(["cutover", "--root", root, "--json"], { procScan: { listPids: () => [process.pid] } });
    expect(result.code, result.err).toBe(0);
    const outcome = records(root).at(-1)!;
    expect(outcome).toMatchObject({ phase: "outcome", ok: true, acceptedUnready: [] });
    expect((outcome.ownStateDbFds as string[]).some(fd => fd.includes(`${root}/state.db `))).toBe(true);
  });

  /**
   * A child that opens `<root>/state.db` (a plain fd, or a node:sqlite connection) when the parent sends
   * "open", answers "ready" over IPC, and exits on "exit". It stays alive on the IPC channel: no timers.
   */
  const holderChild = (root: string, kind: "fd" | "sqlite") => {
    const file = JSON.stringify(path.join(root, "state.db"));
    const child = spawn(process.execPath, ["-e", `
      let held;
      process.on("message", (message) => {
        if (message === "exit") process.exit(0);
        if (message !== "open") return;
        ${kind === "fd" ? `held = require("node:fs").openSync(${file}, "r");`
          : `const { DatabaseSync } = require("node:sqlite"); held = new DatabaseSync(${file}); held.prepare("SELECT count(*) AS n FROM meta").get();`}
        process.send("ready");
      });`], { stdio: ["ignore", "ignore", "ignore", "ipc"] });
    /** Sends "open" and awaits the ONE "ready" reply, with one deadline. */
    const open = (): Promise<void> => new Promise((resolve, reject) => {
      const deadline = setTimeout(() => reject(new Error("holder child not ready within 10 s")), 10_000);
      child.once("message", (message) => { clearTimeout(deadline); if (message === "ready") resolve(); else reject(new Error(`holder child: ${String(message)}`)); });
      child.send("open");
    });
    const close = (): Promise<void> => new Promise((resolve) => {
      if (child.exitCode !== null || !child.connected) { child.kill("SIGKILL"); resolve(); return; }
      const deadline = setTimeout(() => { child.kill("SIGKILL"); resolve(); }, 5_000);
      child.once("exit", () => { clearTimeout(deadline); resolve(); });
      child.send("exit");
    });
    return { child, open, close };
  };

  /**
   * A root left at backend=importing by a crash (state.db exists, in WAL mode). The rerun's census hook runs
   * under the fence before the redo commit, so a holder opened there appears after the preflight.
   */
  const crashedImport = async (): Promise<string> => {
    const root = await fileRoot();
    const crashed = await run(["cutover", "--root", root], { options: { onStep: (step) => { if (step === "import-commit") throw new Error("killed"); } } });
    expect(crashed.code).toBe(1);
    expect(await backendOf(root)).toBe("importing");
    return root;
  };
  const denied = (pid: number) => (dir: string): string[] => {
    if (dir === `/proc/${pid}/fd`) throw Object.assign(new Error("denied"), { code: "EACCES" });
    return fs.readdirSync(dir);
  };

  // ponytail: the holder scan reads /proc and /proc/locks, Linux only (smarty-dev#7936); off Linux it
  // fails closed, covered by "fails closed off Linux" below.
  it.skipIf(process.platform !== "linux")("refuses a second process that opens state.db between the preflight and the commit, naming its pid", async () => {
    const root = await crashedImport();
    const holder = holderChild(root, "fd");
    try {
      const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [process.pid, holder.child.pid!] },
        census: async () => { await holder.open(); return { writers: [] }; } });
      expect(result.code).toBe(3);
      expect(result.err).toContain(`(under the fence, before the importing commit): holder@${holder.child.pid} (foreign state.db holder: pid ${holder.child.pid} (`);
      expect(result.err).toContain(`) holds ${root}/state.db)`);
      expect(await backendOf(root)).toBe("importing");
    } finally { await holder.close(); }
  });

  // ponytail: the holder scan reads /proc and /proc/locks, Linux only (smarty-dev#7936); off Linux it
  // fails closed, covered by "fails closed off Linux" below.
  it.skipIf(process.platform !== "linux")("finds a SQLite connection whose /proc/<pid>/fd is unreadable through /proc/locks alone", async () => {
    const root = await crashedImport();
    const holder = holderChild(root, "sqlite");
    try {
      const result = await run(["cutover", "--root", root], {
        procScan: { listPids: () => [process.pid, holder.child.pid!], readFdDir: denied(holder.child.pid!) },
        census: async () => { await holder.open(); return { writers: [] }; } });
      expect(result.code).toBe(3);
      expect(result.err).toMatch(new RegExp(`holder@${holder.child.pid} \\(foreign state\\.db holder: pid ${holder.child.pid} \\(\\S+\\) holds a POSIX lock on ${root.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/state\\.db`));
      expect(await backendOf(root)).toBe("importing");
    } finally { await holder.close(); }
  });

  // smarty-dev#7936: the write-lease probe. Real holders are python3 children (os.open, prctl, mmap);
  // the fd scan is narrowed to no pid so that only the lease probe can see them.
  // On Linux CI (ubuntu-latest builds first) these never skip: a missing python3 or helper fails them there.
  const leaseSkip = process.platform !== "linux" ? "not Linux" : process.env.CI ? ""
    : spawnSync("python3", ["-c", "pass"]).status !== 0 ? "python3 missing"
      : !fs.existsSync(leaseHelperPath()) ? `lease helper ${leaseHelperPath()} missing (bun run build)` : "";
  const BIN = path.resolve("bin/fabric-mesh-backend");
  const binSkip = leaseSkip || (process.env.CI || fs.existsSync(path.resolve("dist/mesh/mesh-backend-cli.js")) ? "" : "dist/ not built (bun run build)");
  /** A python3 child that holds `file` open (no lock), says "ready" on stdout and waits on stdin. */
  const pythonHolder = async (file: string, mode: "fd" | "nondumpable" | "mmap"): Promise<{ child: ChildProcess; release: () => Promise<void> }> => {
    const code = `
import ctypes, mmap, os, sys
if sys.argv[2] == "nondumpable": ctypes.CDLL(None).prctl(4, 0, 0, 0, 0)
fd = os.open(sys.argv[1], os.O_RDWR)
if sys.argv[2] == "mmap":
    held = mmap.mmap(fd, 1); os.close(fd)
print("ready", flush=True)
sys.stdin.readline()`;
    const child = spawn("python3", ["-c", code, file, mode], { stdio: ["pipe", "pipe", "inherit"] });
    await new Promise<void>((resolve, reject) => {
      const deadline = setTimeout(() => reject(new Error("python3 holder not ready within 10 s")), 10_000);
      child.stdout!.once("data", (data: Buffer) => { clearTimeout(deadline); if (String(data).startsWith("ready")) resolve(); else reject(new Error(String(data))); });
    });
    const release = (): Promise<void> => new Promise((resolve) => {
      if (child.exitCode !== null) { resolve(); return; }
      const deadline = setTimeout(() => { child.kill("SIGKILL"); resolve(); }, 5_000);
      child.once("exit", () => { clearTimeout(deadline); resolve(); });
      child.stdin!.end("\n");
    });
    return { child, release };
  };

  for (const mode of ["fd", "nondumpable", "mmap"] as const) {
    it.skipIf(leaseSkip !== "")(`refuses a lock-free ${mode} holder of state.db through the lease probe alone${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
      const root = await crashedImport();
      const holder = await pythonHolder(path.join(root, "state.db"), mode);
      try {
        const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] } });
        expect(result.code).toBe(3);
        expect(result.err).toContain(`holder@lease:state.db (unidentified holder of ${root}/state.db (a write lease was refused: another process has it open; smarty-dev#7936))`);
        expect(await backendOf(root)).toBe("importing");
        // Accept-only, by name.
        expect((await run(["cutover", "--root", root, "--accept-unready", "fabric@unknown"], { procScan: { listPids: () => [] } })).code).toBe(3);
      } finally { await holder.release(); }
    });
  }

  it.skipIf(leaseSkip !== "")(`passes with nobody holding state.db, and the probe releases its lease at once${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    expect(probeStateDbLeases(root)).toEqual([]);
    // A second open right after the probe does not wait for a lease break (lease-break-time is 45 s).
    const started = Date.now();
    const opened = spawnSync("python3", ["-c", "import os, sys; os.close(os.open(sys.argv[1], os.O_RDWR)); print('opened')", path.join(root, "state.db")],
      { encoding: "utf8", timeout: 5_000 });
    expect(opened.stdout.trim()).toBe("opened");
    expect(Date.now() - started).toBeLessThan(3_000);
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] } });
    expect(result.code, result.err).toBe(0);
    expect(await backendOf(root)).toBe("sqlite");
  });

  it.skipIf(leaseSkip !== "")(`runs the REAL helper: free on an unheld file (exit 0), held while another process has it open (exit 3)${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const file = path.join(tempDir("lease"), "state.db");
    fs.writeFileSync(file, "x", { mode: 0o600 });
    const free = spawnSync(leaseHelperPath(), [file], { encoding: "utf8", timeout: 10_000 });
    expect({ status: free.status, stdout: free.stdout }).toEqual({ status: 0, stdout: `free ${file}\n` });
    const holder = await pythonHolder(file, "fd");
    try {
      const held = spawnSync(leaseHelperPath(), [file], { encoding: "utf8", timeout: 10_000 });
      expect({ status: held.status, stdout: held.stdout }).toEqual({ status: 3, stdout: `held ${file}\n` });
    } finally { await holder.release(); }
  });

  it.skipIf(binSkip !== "")(`end to end through the operator's bin: a lock-free holder refuses, without it the switch is ready${binSkip ? ` (skipped: ${binSkip})` : ""}`, async () => {
    const root = await crashedImport();
    // The operator's entry point (package bin -> dist), as a child; the real census, fd scan, /proc/locks and lease probe.
    const cli = (...argv: string[]) => spawnSync(process.execPath, [BIN, ...argv], { encoding: "utf8", timeout: 60_000,
      env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
    const holder = await pythonHolder(path.join(root, "state.db"), "nondumpable");
    try {
      const refused = cli("cutover", "--root", root, "--accept-unready", "factory,fabric@unknown");
      expect(refused.status).toBe(3);
      expect(refused.stderr).toContain(`holder@lease:state.db (unidentified holder of ${root}/state.db`);
    } finally { await holder.release(); }
    const ready = cli("cutover", "--root", root, "--accept-unready", "factory,fabric@unknown");
    expect(ready.status, ready.stderr).toBe(0);
    expect(ready.stdout).toMatch(/^cutover done: backend=sqlite epoch 1 \(from 0\)/);
  });

  it.skipIf(leaseSkip !== "")(`fails closed on helper integrity: a tampered helper, even with a matching forged manifest.json${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const copy = tempDir("helper");
    const helper = path.join(copy, "fabric-mesh-lease");
    fs.copyFileSync(leaseHelperPath(), helper);
    fs.chmodSync(helper, 0o700);
    expect(probeStateDbLeases(root, { helper })).toEqual([]); // the untouched copy matches the embedded digest
    fs.appendFileSync(helper, "\0");
    // A forged manifest.json beside it that matches the tampered bytes changes nothing.
    const forged = createHash("sha256").update(fs.readFileSync(helper)).digest("hex");
    fs.writeFileSync(path.join(copy, "manifest.json"), JSON.stringify({ compiler: "gcc version 0", helpers: { "fabric-mesh-lease": forged } }));
    const tampered = await run(["cutover", "--root", root], { procScan: { listPids: () => [] }, leaseProbe: { helper } });
    expect(tampered.code).toBe(3);
    expect(tampered.err).toContain(`holder@lease-probe (state.db lease probe unavailable (smarty-dev#7936): helper integrity: ${helper} sha256 ${forged} does not match the embedded digest `);
    // A symlinked helper is refused (O_NOFOLLOW), and so is a build without an embedded digest.
    const link = path.join(copy, "link");
    fs.symlinkSync(leaseHelperPath(), link);
    expect(probeStateDbLeases(root, { helper: link })[0]!.reason).toContain(`helper integrity: ${link} cannot be opened (ELOOP)`);
    expect(probeStateDbLeases(root, { expectedDigest: "" })[0]!.reason).toContain("helper integrity: no embedded digest in this build");
  });

  it.skipIf(leaseSkip !== "")(`executes the hashed fd: a helper swapped at its path after the hash never runs${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const copy = tempDir("helper");
    const helper = path.join(copy, "fabric-mesh-lease");
    fs.copyFileSync(leaseHelperPath(), helper);
    fs.chmodSync(helper, 0o700);
    const file = path.join(root, "state.db");
    const holder = await pythonHolder(file, "fd");
    try {
      // The swapped file would claim every file free; the verified helper reports the real holder.
      const swap = (): void => {
        const fake = path.join(copy, "fake");
        fs.writeFileSync(fake, `#!/bin/sh\nfor f in "$@"; do echo "free $f"; done\n`, { mode: 0o700 });
        fs.renameSync(fake, helper);
      };
      const holders = probeStateDbLeases(root, { helper, afterHash: swap });
      expect(holders.map(item => item.name)).toEqual(["holder@lease:state.db"]);
      expect(fs.readFileSync(helper, "utf8")).toContain("#!/bin/sh"); // the path really holds the swapped file
    } finally { await holder.release(); }
    // Without /proc/self/fd there is no exec at all: fail closed.
    expect(probeStateDbLeases(root, { helper: leaseHelperPath(), procRoot: tempDir("noproc"), leasesEnable: "1" })[0]!.reason)
      .toContain("helper integrity: /proc/self/fd exec is unavailable");
  });

  // ponytail: the remaining fail-closed branches (not Linux, leases-enable=0, a missing helper) cannot run end to
  // end on a Linux CI host with leases enabled and the helper built, so they are injected here.
  it.skipIf(process.platform !== "linux")("fails closed when the lease probe cannot run: a missing helper, leases disabled, not Linux", async () => {
    const root = await crashedImport();
    const missing = await run(["cutover", "--root", root], { procScan: { listPids: () => [] }, leaseProbe: { helper: path.join(root, "no-helper") } });
    expect(missing.code).toBe(3);
    expect(missing.err).toContain(`holder@lease-probe (state.db lease probe unavailable (smarty-dev#7936): helper ${path.join(root, "no-helper")} is missing (bun run build))`);
    expect(probeStateDbLeases(root, { leasesEnable: "0" })[0]!.reason).toContain("/proc/sys/fs/leases-enable is 0");
    expect(probeStateDbLeases(root, { platform: "darwin" })[0]!.reason).toContain("not Linux");
    const accepted = await run(["cutover", "--root", root, "--accept-unready", "holder@lease-probe"],
      { procScan: { listPids: () => [] }, leaseProbe: { helper: path.join(root, "no-helper") } });
    expect(accepted.code, accepted.err).toBe(0);
  });

  // ponytail: the holder scan reads /proc and /proc/locks, Linux only (smarty-dev#7936); off Linux it
  // fails closed, covered by "fails closed off Linux" below.
  it.skipIf(process.platform !== "linux")("does not count an unreadable /proc/<pid>/fd without a lock on state.db as a holder", async () => {
    const root = await fileRoot();
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [process.ppid], readFdDir: denied(process.ppid) } });
    expect(result.code, result.err).toBe(0);
    expect(records(root).at(-1)).toMatchObject({ ok: true, acceptedUnready: [] });
  });

  // ponytail: the holder scan reads /proc and /proc/locks, Linux only (smarty-dev#7936); off Linux it
  // fails closed, covered by "fails closed off Linux" below.
  it.skipIf(process.platform !== "linux")("fails closed when /proc/locks is unreadable: an unreadable /proc/<pid>/fd is then ambiguous", async () => {
    const root = await fileRoot();
    const other = process.ppid;
    const procScan: ProcScanOptions = { listPids: () => [other], readFdDir: denied(other),
      readLocks: () => { throw Object.assign(new Error("EACCES: /proc/locks"), { code: "EACCES" }); } };
    const refused = await run(["cutover", "--root", root], { procScan });
    expect(refused.code).toBe(3);
    expect(refused.err).toContain(`holder@${other} (ambiguous state.db holder: pid ${other} `);
    expect(refused.err).toContain("/proc/locks unusable (EACCES: /proc/locks)");
    expect((await run(["cutover", "--root", root, "--accept-unready", "fabric@unknown"], { procScan })).code).toBe(3);
    expect((await run(["cutover", "--root", root, "--accept-unready", `holder@${other}`], { procScan })).code).toBe(0);
    // An unparsable /proc/locks fails closed the same way.
    const garbled = await fileRoot();
    const result = await run(["cutover", "--root", garbled], { procScan: { ...procScan, readLocks: () => "not a lock line\n" } });
    expect(result.code).toBe(3);
    expect(result.err).toContain("/proc/locks unusable (unparsable /proc/locks line: not a lock line)");
  });

  // ponytail: the holder scan reads /proc and /proc/locks, Linux only (smarty-dev#7936); off Linux it
  // fails closed, covered by "fails closed off Linux" below.
  it.skipIf(process.platform !== "linux")("refuses a lock entry with pid -1 (an OFD lock) on state.db as ambiguous", async () => {
    const root = await fileRoot();
    const readLocks = (): string => {
      const stat = fs.statSync(path.join(root, "state.db"), { bigint: true });
      const [major, minor, ino] = procLocksKey(stat.dev, stat.ino).split(":");
      return `${fs.readFileSync("/proc/locks", "utf8")}99: OFDLCK ADVISORY  READ -1 ${BigInt(major!).toString(16).padStart(2, "0")}:${BigInt(minor!).toString(16).padStart(2, "0")}:${ino} 128 128\n`;
    };
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [], readLocks } });
    expect(result.code).toBe(3);
    expect(result.err).toContain(`holder@-1 (ambiguous state.db holder: a OFDLCK lock on ${root}/state.db by pid -1, which does not resolve to a process)`);
  });

  it("fails closed off Linux: state.db evidence stays fabric@unknown and only the override passes", async () => {
    const root = await fileRoot();
    const unknown = [{ pid: 0, release: "unknown", mode: `unknown state-database ${root}/state.db-wal` }];
    const refused = await run(["cutover", "--root", root], { unknown, procScan: { platform: "darwin" } });
    expect(refused.code).toBe(3);
    expect(refused.err).toContain("fabric@unknown (live writer evidence without an attributable release: pid 0 unknown state-database");
    expect((await run(["cutover", "--root", root, "--accept-unready", "fabric@unknown"], { unknown, procScan: { platform: "darwin" } })).code).toBe(0);
    // On Linux the same census evidence is resolved by the holder scan instead (here: nobody else).
    const linux = await fileRoot();
    expect((await run(["cutover", "--root", linux], { unknown, procScan: { platform: "linux", listPids: () => [] } })).code).toBe(0);
  });

  it("reserves built-in reader names: --require-reader only adds readers", async () => {
    const root = await fileRoot();
    for (const name of ["factory", "fabric@x", "fabric-y"]) {
      const result = await run(["cutover", "--root", root, "--require-reader", `${name}=/decoy`]);
      expect(result.code).toBe(2);
      expect(result.err).toContain(`reserved reader name ${name}`);
    }
    const trusted = installRoot("r1");
    expect(requiredReaders([], [], [{ name: "factory", installRoot: trusted }], [{ name: "factory", installRoot: "/decoy" }]))
      .toEqual([{ name: "factory", kind: "installed", installRoot: trusted, builtin: true }]);
  });

  // ponytail: POSIX mode bits and uid only; Windows has neither (the gate does not check owners there).
  // Windows ACL checks of the registry: smarty-dev#7548.
  it.skipIf(process.platform === "win32")("writes a proof only into a private readers/ directory", async () => {
    const root = await fileRoot();
    const elsewhere = tempDir("elsewhere");
    fs.symlinkSync(elsewhere, path.join(root, "readers"));
    const linked = await run(["reader-proof", "--root", root, "--backend", "sqlite"]);
    expect(linked.code).toBe(1);
    expect(linked.err).toContain("is not a directory (or a symlink): proof not written");
    expect(fs.readdirSync(elsewhere)).toEqual([]);
    fs.rmSync(path.join(root, "readers"));
    fs.mkdirSync(path.join(root, "readers"), { mode: 0o700 });
    fs.chmodSync(path.join(root, "readers"), 0o777);
    expect(await proveFactory(root)).toBe(1);
    fs.chmodSync(path.join(root, "readers"), 0o700);
    expect(await proveFactory(root)).toBe(0);
  });

  it("refuses when the switch record cannot be written", async () => {
    const root = await fileRoot();
    fs.mkdirSync(path.join(root, "backend-switches.jsonl"));
    const result = await run(["cutover", "--root", root]);
    expect(result.code).toBe(3);
    expect(result.err).toContain("cannot write the switch record");
    expect(await backendOf(root)).toBe("none");
  });

  it("binds a proof to the installed release: an upgrade since the proof is unready; --require-reader adds a reader", async () => {
    const root = await fileRoot();
    const factory = installRoot("r1");
    expect(await proveFactory(root)).toBe(0);
    install(factory, "r2");
    const result = await run(["cutover", "--root", root], { builtins: [{ name: "factory", installRoot: factory }] });
    expect(result.code).toBe(3);
    expect(result.err).toContain("reader factory not ready: release mismatch: proved r1, installed r2");
    const forwarder = installRoot("f1");
    const extra = await run(["cutover", "--root", root, "--require-reader", `forwarder=${forwarder}`]);
    expect(extra.code).toBe(3);
    expect(extra.err).toContain("no proof from installed reader forwarder (release f1)");
  });
});

describe("fabric-mesh-backend readiness gate: proofs that cannot be trusted", () => {
  it.skipIf(process.platform === "win32")("refuses a proof whose read the epoch did not bracket, and writes nothing", async () => {
    const root = await fileRoot();
    expect((await run(["cutover", "--root", root])).code).toBe(0); // epoch 1; reader-proof then reads in place
    const bump = (): void => {
      const db = openNodeSqlite(path.join(root, "state.db"));
      try { db.prepare("UPDATE meta SET value = ? WHERE name = 'epoch'").run(7); } finally { db.close(); }
    };
    await expect(proveFabricReader(root, { backend: "sqlite", afterRead: bump })).rejects.toThrow("the mesh epoch changed during the read (1 -> 7): proof not written");
    expect(fs.existsSync(path.join(root, "readers"))).toBe(false);
  });

  it("fails closed without process.geteuid (Windows): reader-proof, cutover and import refuse, and no override passes", async () => {
    const root = await fileRoot();
    const factory = installRoot("r1");
    const builtins = [{ name: "factory", installRoot: factory }];
    const withProof = await fileRoot();
    placeProof(withProof, factoryProof());
    // An UNSAFE (group/world-writable) readers/ as well.
    const unsafe = await fileRoot();
    placeProof(unsafe, factoryProof());
    fs.chmodSync(path.join(unsafe, "readers"), 0o777);
    const saved = Object.getOwnPropertyDescriptor(process, "geteuid");
    Object.defineProperty(process, "geteuid", { value: undefined, configurable: true, writable: true });
    try {
      const write = await run(["reader-proof", "--root", root, "--backend", "sqlite"]);
      expect(write.code).toBe(1);
      expect(write.err).toContain("unverifiable: this platform has no POSIX owner (smarty-dev#7548): proof not written");
      expect(fs.existsSync(path.join(root, "readers"))).toBe(false);
      const file = path.join(tempDir("proof"), "factory.json");
      fs.writeFileSync(file, JSON.stringify(factoryProof()));
      expect((await run(["reader-proof", "--root", root, "--name", "factory", "--proof-file", file])).code).toBe(1);
      const everyName = "factory,fabric@unknown,fabric@fabric-rel-a,holder@1,holder@-1";
      for (const [mesh, command] of [[root, "cutover"], [root, "import"], [withProof, "cutover"], [unsafe, "cutover"], [unsafe, "import"]] as const) {
        const refused = await run([command, "--root", mesh, "--accept-unready", everyName],
          { builtins, writers: [{ pid: 9, release: "fabric-rel-a", mode: "sqlite" }], unknown: [{ pid: 0, release: "unknown", mode: "unknown lock-owner" }] });
        expect(refused.code).toBe(3);
        expect(refused.err).toContain("the readiness gate cannot verify reader proofs on this platform (no POSIX owner; Windows ACL checks: smarty-dev#7548)");
        expect(await backendOf(mesh)).toBe("none");
      }
    } finally {
      if (saved) Object.defineProperty(process, "geteuid", saved);
    }
  });
});
