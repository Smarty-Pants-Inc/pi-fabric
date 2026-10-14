import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { meshBackendStatus, type MeshBackendOptions } from "../src/mesh/backend-migration.js";
import { MESH_SWITCH_OVERRIDE_ALARM_TOPIC, main } from "../src/mesh/mesh-backend-cli.js";
import { openNodeSqlite } from "../src/mesh/state-sqlite.js";
import { leaseHelperPath, unsafeChain, probeStateDbLeases, proveFabricReader, procLocksKey, readerReadiness, requiredReaders, type InstalledReader, type LeaseProbeOptions, type ProcScanOptions, type ReaderProof } from "../src/mesh/reader-proof.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#7815: the switch refuses unless every REQUIRED reader (the inventory: live Fabric releases,
// built-in installed readers, --require-reader) proved it can read sqlite.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const SWITCH_OWNER = "session:0a1b2c3d-0000-4000-8000-00000000c943";
const dirs: string[] = [];
const savedRelease = process.env.PI_FABRIC_RELEASE_SHA;
beforeEach(() => {
  process.env.PI_FABRIC_RELEASE_SHA = "fabric-rel-a";
  // Hermetic alarm recipient (smarty-dev#9436): a launcher's SMARTY_LEAD_SESSION or PI_FABRIC_MESH_ALARM_TO must not
  // leak in. Tests whose accepted-unready switch is expected to succeed set PI_FABRIC_MESH_ALARM_TO themselves.
  vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", undefined);
  vi.stubEnv("SMARTY_LEAD_SESSION", undefined);
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
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
const switchOverrideAlarms = (root: string) => {
  const store = new MeshStore(root, 256 * 1024, 500, { stateBackend: "file" });
  try { return store.read({ topic: MESH_SWITCH_OVERRIDE_ALARM_TOPIC }); }
  finally { store.closeState(); }
};

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

  it("publishes one addressed override alarm after a committed switch, and none when no reader is overridden", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
    const root = await crashedImport();
    const accepted = await run(["cutover", "--root", root, "--json", "--accept-unready", "holder@lease-probe"], { leaseProbe: { platform: "darwin" } });
    expect(accepted.code, accepted.err).toBe(0);
    const json = JSON.parse(accepted.out) as { readers: { switchId: string; acceptedUnready: Array<{ name: string; reason: string }> } };
    expect(new Set(json.readers.acceptedUnready.map(reader => reader.name))).toEqual(new Set(["holder@lease-probe"]));
    const alarms = switchOverrideAlarms(root);
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ topic: MESH_SWITCH_OVERRIDE_ALARM_TOPIC, kind: "mesh-switch-override", to: SWITCH_OWNER,
      data: { switchId: json.readers.switchId, acceptedUnready: [{ name: "holder@lease-probe", reason: expect.stringContaining("not Linux") }] } });
    expect(alarms[0]!.data).not.toHaveProperty("root");
    expect(alarms[0]!.text).toContain(json.readers.switchId);

    const readyRoot = await fileRoot();
    const ready = await run(["cutover", "--root", readyRoot, "--json"]);
    expect(ready.code, ready.err).toBe(0);
    expect(switchOverrideAlarms(readyRoot)).toEqual([]);
  });

  it("alarms an override accepted at the importing commit even when the reader is ready by the sqlite commit, once across both commits and not on a verified rerun", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
    const root = await fileRoot();
    const builtins = [{ name: "factory", installRoot: installRoot("r1") }];
    expect(await proveFactory(root)).toBe(0);
    const proof = path.join(root, "readers", "factory.json");
    const switched = await run(["cutover", "--root", root, "--json", "--accept-unready", "factory"], { builtins,
      options: { beforeCommit: commit => {
        fs.writeFileSync(proof, JSON.stringify(factoryProof({ backends: commit.phase === "importing" ? ["file"] : ["file", "sqlite"] })));
      } } });
    expect(switched.code, switched.err).toBe(0);
    const json = JSON.parse(switched.out);
    expect(json.readers).toMatchObject({ acceptedUnready: [], checkedUnderFence: ["importing", "sqlite"] });
    expect(switchOverrideAlarms(root)).toHaveLength(1);
    expect(switchOverrideAlarms(root)[0]).toMatchObject({ data: { switchId: json.readers.switchId,
      acceptedUnready: [{ name: "factory", reason: "missing sqlite (has file)" }] } });
    const rerun = await run(["cutover", "--root", root, "--json", "--accept-unready", "factory"], { builtins });
    expect(rerun.code, rerun.err).toBe(0);
    expect(switchOverrideAlarms(root)).toHaveLength(1);
  });

  it("returns non-zero when the post-commit override alarm fails, without changing the committed switch record", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
    const root = await crashedImport();
    const originalPublish = MeshStore.prototype.publish;
    let committedRecord = "";
    let committedMarker = "";
    const publish = vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
      if (input.topic === MESH_SWITCH_OVERRIDE_ALARM_TOPIC) {
        committedRecord = fs.readFileSync(path.join(root, "backend-switches.jsonl"), "utf8");
        committedMarker = fs.readFileSync(path.join(root, "state.json"), "utf8");
        throw new Error("event log unwritable");
      }
      return await originalPublish.call(this, input);
    });
    const failed = await run(["cutover", "--root", root, "--json", "--accept-unready", "holder@lease-probe"], { leaseProbe: { platform: "darwin" } });
    expect(failed.code).toBe(1);
    expect(failed.err).toContain(`${MESH_SWITCH_OVERRIDE_ALARM_TOPIC} publish failed after committed switch`);
    expect(failed.err).toContain("accepted readers: holder@lease-probe");
    expect(failed.err.split("\n").filter(line => line.includes(`${MESH_SWITCH_OVERRIDE_ALARM_TOPIC} publish failed`))).toHaveLength(1);
    expect(publish).toHaveBeenCalledOnce();
    expect(JSON.parse(failed.out)).toMatchObject({ ok: false, committed: true, error: "event log unwritable" });
    expect(await backendOf(root)).toBe("sqlite");
    expect(fs.readFileSync(path.join(root, "backend-switches.jsonl"), "utf8")).toBe(committedRecord);
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(committedMarker);
    const [intent, outcome] = records(root).slice(-2);
    expect(outcome).toMatchObject({ phase: "outcome", ok: true, switchId: intent!.switchId });
    expect((outcome!.acceptedUnready as Array<{ name: string }>).some(reader => reader.name === "holder@lease-probe")).toBe(true);
    expect(switchOverrideAlarms(root)).toEqual([]);
  });

  it("still publishes the override alarm when the best-effort outcome append fails, warning without changing exit 0", async () => {
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
    const root = await crashedImport();
    const record = path.join(root, "backend-switches.jsonl");
    let failOutcome = false;
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: string | number, mode?: fs.Mode) => {
      if (failOutcome && String(file) === record && flags === "a") throw new Error("outcome append unwritable");
      return open(file, flags, mode);
    }) as typeof fs.openSync);
    const switched = await run(["cutover", "--root", root, "--json", "--accept-unready", "holder@lease-probe"], {
      leaseProbe: { platform: "darwin" }, options: { beforeCommit: commit => { if (commit.phase === "sqlite") failOutcome = true; } } });
    expect(switched.code, switched.err).toBe(0);
    const json = JSON.parse(switched.out);
    expect(switched.err).toContain(`could not append the switch outcome ${json.readers.switchId}: outcome append unwritable`);
    expect(switched.err.split("\n").filter(line => line.includes("could not append the switch outcome"))).toHaveLength(1);
    expect(await backendOf(root)).toBe("sqlite");
    expect(records(root).at(-1)).toMatchObject({ phase: "intent", switchId: json.readers.switchId });
    expect(switchOverrideAlarms(root)).toHaveLength(1);
    expect(switchOverrideAlarms(root)[0]).toMatchObject({ to: SWITCH_OWNER, data: { switchId: json.readers.switchId,
      acceptedUnready: [{ name: "holder@lease-probe", reason: expect.stringContaining("not Linux") }] } });
  });

  it("reports a missing override alarm recipient as post-commit failure, never as a broadcast", async () => {
    // No recipient at all (beforeEach unset both); an empty value counts as unset too.
    expect(process.env.PI_FABRIC_MESH_ALARM_TO).toBeUndefined();
    expect(process.env.SMARTY_LEAD_SESSION).toBeUndefined();
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", "");
    vi.stubEnv("SMARTY_LEAD_SESSION", "");
    const root = await crashedImport();
    const publish = vi.spyOn(MeshStore.prototype, "publish");
    const failed = await run(["cutover", "--root", root, "--json", "--accept-unready", "holder@lease-probe"], { leaseProbe: { platform: "darwin" } });
    expect(failed.code).toBe(1);
    expect(failed.err).toContain("no owning-lead alarm recipient");
    expect(failed.err).toContain("accepted readers: holder@lease-probe");
    expect(failed.err.split("\n").filter(line => line.includes(`${MESH_SWITCH_OVERRIDE_ALARM_TOPIC} publish failed`))).toHaveLength(1);
    expect(JSON.parse(failed.out)).toMatchObject({ ok: false, committed: true });
    expect(await backendOf(root)).toBe("sqlite");
    expect(records(root).at(-1)).toMatchObject({ ok: true, phase: "outcome" });
    expect(publish).not.toHaveBeenCalled();
    expect(switchOverrideAlarms(root)).toEqual([]);
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
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
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
    const stateJson = fs.readFileSync(path.join(root, "state.json"));
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    const result = await run(["cutover", "--root", root], { writers: () => live,
      options: { beforeCommit: () => { live = [...live, { pid: 11, release: "fabric-rel-new", mode: "sqlite" }]; } } });
    expect(result.code).toBe(3);
    expect(result.err).toContain("(under the fence, before the importing commit): fabric@fabric-rel-new (no proof from installed reader fabric release fabric-rel-new");
    // smarty-dev#9501: the refusal precedes the importing commit and this run created state.db, so it is removed under
    // the fence: status backend "none" means no state.db (the file reader stays). Without a held directory fd
    // (macOS smarty-dev#11066) the database is kept, at backend=file.
    expect(fs.readFileSync(path.join(root, "state.json")).equals(stateJson)).toBe(true);
    if (result.err.includes("no-held-dir-fd")) {
      expect(await backendOf(root)).toBe("file");
    } else {
      expect(fs.readdirSync(root).filter(name => /^state\.db(?:-wal|-shm|-journal)?$/.test(name))).toEqual([]);
      expect(await meshBackendStatus(root)).toMatchObject({ backend: "none", epoch: 0, reader: { source: "file" } });
    }
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

  // pi-fabric#881 (SEC + Astra P1): a lock-free, non-dumpable opener landing between the probe's F_UNLCK and its close
  // used to merge into the probe's still-unread IN_OPEN. The probe now reads its own IN_OPEN before the lease, so this
  // open is a new event. End to end through the operator's bin; the helper's test pause point places the open there.
  it.skipIf(binSkip !== "")(`end to end: a lock-free opener between the probe's F_UNLCK and its close is refused${binSkip ? ` (skipped: ${binSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const pause = tempDir("pause");
    const child = spawn(process.execPath, [BIN, "cutover", "--root", root, "--accept-unready", "factory,fabric@unknown"], {
      env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "", FABRIC_MESH_LEASE_TEST_PAUSE_AFTER_UNLCK: pause }, stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr!.on("data", (data: Buffer) => { stderr += String(data); });
    const exited = new Promise<number | null>(resolve => child.once("exit", resolve));
    const started = Date.now();
    while (!fs.existsSync(path.join(pause, "paused"))) {
      if (Date.now() - started > 20_000) throw new Error(`the helper never paused after F_UNLCK: ${stderr}`);
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    const holder = await pythonHolder(path.join(root, "state.db"), "nondumpable");
    try {
      fs.writeFileSync(path.join(pause, "go"), "");
      expect(await exited, stderr).toBe(3);
      expect(stderr).toContain(`holder@lease:state.db (unidentified holder of ${root}/state.db (1 open(s) since the lease probe; smarty-dev#8116))`);
    } finally { await holder.release(); }
  }, 60_000);

  // pi-fabric#881: if the probe's own IN_OPEN never arrives, nothing can be counted exactly: fail closed.
  it.skipIf(leaseSkip !== "")(`fails closed when the probe's own IN_OPEN is not seen${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    vi.stubEnv("FABRIC_MESH_LEASE_TEST_IGNORE_PROBE_OPEN", "1");
    try {
      const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] } });
      expect(result.code, result.err).toBe(3);
      expect(result.err).toContain(`holder@lease-probe (state.db lease probe unavailable (smarty-dev#7936): unexpected helper output: unwatched ${root}/state.db)`);
      expect(await backendOf(root)).toBe("importing");
    } finally { vi.unstubAllEnvs(); }
  }, 60_000);

  it.skipIf(binSkip !== "")(`end to end through the operator's bin: a lock-free holder refuses, without it the switch is ready${binSkip ? ` (skipped: ${binSkip})` : ""}`, async () => {
    // The host's real inventory may hold an unready installed reader (factory): its accepted override then alarms.
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
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

  // smarty-dev#8145 item 3, STRESS coverage: swaps during whole CLI runs through the operator's bin on a copy of the
  // built package; the probe-window overlap is not asserted. The deterministic proof of the hash-to-exec window is the
  // unit test "executes the hashed fd: a helper swapped at its path after the hash never runs" (afterHash hook).
  it.skipIf(binSkip !== "")(`stress: a helper swapped at its path during whole CLI runs never reports a held state.db free${binSkip ? ` (skipped: ${binSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const pkg = tempDir("pkg");
    fs.cpSync(path.resolve("dist"), path.join(pkg, "dist"), { recursive: true });
    fs.cpSync(path.resolve("bin"), path.join(pkg, "bin"), { recursive: true });
    const helper = path.join(pkg, "dist/native/fabric-mesh-lease");
    // A racing child swaps a fake ("free" for every file) and the real helper at the path, without pause.
    const racer = spawn(process.execPath, ["-e", `
const fs = require("node:fs");
const [helper] = process.argv.slice(1);
fs.linkSync(helper, helper + ".real");
fs.writeFileSync(helper + ".fake", '#!/bin/sh\\nfor f in "$@"; do echo "free $f"; done\\n', { mode: 0o700 });
// Hard links, not rewrites: a swap is two syscalls, so it lands often inside the hash-to-exec window.
// Every 64 swaps it publishes its count: it shows swaps went on during a CLI run (not inside the probe window).
for (let i = 0; ; i++) {
  fs.linkSync(helper + (i % 2 ? ".real" : ".fake"), helper + ".next");
  fs.renameSync(helper + ".next", helper);
  if (i % 64 === 0) fs.writeFileSync(helper + ".swaps", String(i));
}`, helper], { stdio: "ignore" });
    const swaps = (): number => { try { return Number(fs.readFileSync(`${helper}.swaps`, "utf8")); } catch { return -1; } };
    const holder = await pythonHolder(path.join(root, "state.db"), "nondumpable");
    try {
      const started = Date.now();
      while (swaps() < 64) {
        if (Date.now() - started > 10_000) throw new Error("the racer did not start swapping");
        await new Promise(resolve => setTimeout(resolve, 5));
      }
      let overlapped = 0;
      for (let run = 0; run < 8; run++) {
        const before = swaps();
        const result = spawnSync(process.execPath, [path.join(pkg, "bin/fabric-mesh-backend"), "cutover", "--root", root,
          "--accept-unready", "factory,fabric@unknown"], { encoding: "utf8", timeout: 60_000, env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
        expect(result.status, result.stderr).toBe(3);
        // Refused by the real verdict, or fail closed (the swapped bytes fail the hash): never "free".
        expect(result.stderr).toMatch(/cutover refused: readers not ready for sqlite: holder@lease(:state\.db \(unidentified holder|-probe \(state\.db lease probe unavailable)/);
        // Swaps went on during the run (stress: swaps during whole CLI runs; the probe-window overlap is not asserted).
        if (swaps() > before) overlapped++;
      }
      // Under a loaded host a short run may see no published count; at least one run must have seen swaps.
      expect(overlapped).toBeGreaterThan(0);
      expect(await backendOf(root)).toBe("importing");
    } finally {
      racer.kill("SIGKILL");
      await holder.release();
    }
  }, 120_000);

  // smarty-dev#8145 round 1 (SEC P1): the whole real chain up to the install root, not only the helper's directory.
  it.skipIf(leaseSkip !== "")(`fails closed on a group-writable or symlinked directory between the helper and the install root${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const pkg = tempDir("chain");
    const native = path.join(pkg, "dist/native");
    fs.mkdirSync(native, { recursive: true, mode: 0o700 });
    const helper = path.join(native, "fabric-mesh-lease");
    fs.copyFileSync(leaseHelperPath(), helper);
    fs.chmodSync(helper, 0o700);
    expect(probeStateDbLeases(root, { helper, trustRoot: pkg })).toEqual([]);
    const dist = fs.realpathSync(path.join(pkg, "dist"));
    fs.chmodSync(dist, 0o770);
    expect(probeStateDbLeases(root, { helper, trustRoot: pkg })[0]).toMatchObject({ name: "holder@lease-probe",
      reason: expect.stringContaining(`helper integrity: ${dist} is group/other-writable (mode 770)`) });
    fs.chmodSync(dist, 0o700);
    // dist/native as a symlink to a directory outside the package: its real path is not under the install root.
    const elsewhere = tempDir("elsewhere");
    fs.renameSync(native, path.join(elsewhere, "native"));
    fs.symlinkSync(path.join(elsewhere, "native"), native);
    expect(probeStateDbLeases(root, { helper, trustRoot: pkg })[0]).toMatchObject({ name: "holder@lease-probe",
      reason: expect.stringContaining(`is outside the install root ${fs.realpathSync(pkg)}`) });
  });

  // pi-fabric#880 review round 2: above the install root up to the first root-owned, root-only (or sticky) ancestor.
  // pi-fabric#880 review round 7 (SEC P1): the walk above the install root stopped at the FIRST root-owned 0755 dir, so
  // a writable parent above it could rename it and replace the package. Root ownership cannot be made in a test: the
  // ancestors above a scratch tree are an injected lstat. Both the bin's shipped check (its <chain-check> text, run
  // as is) and the bundle's unsafeChain must agree.
  describe("the chain above the install root walks to / (pi-fabric#880 round 7)", () => {
    const fake = (uid: number, mode: number): fs.Stats =>
      ({ uid, mode, dev: 0, ino: 0, isDirectory: () => true, isSymbolicLink: () => false, isFile: () => false }) as unknown as fs.Stats;
    /** A scratch package; every ancestor above `base` is fake: `first` just above it, `higher` for all others. */
    const layout = (first: fs.Stats, higher: (at: string) => fs.Stats) => {
      const base = fs.realpathSync(tempDir("above"));
      const pkg = path.join(base, "pkg");
      fs.mkdirSync(path.join(pkg, "dist/native"), { recursive: true, mode: 0o700 });
      const file = path.join(pkg, "dist/native/fabric-mesh-lease");
      fs.writeFileSync(file, "x", { mode: 0o700 });
      const inside = (at: string) => at === base || at.startsWith(`${base}${path.sep}`);
      const lstat = (at: string): fs.Stats => inside(at) ? fs.lstatSync(at) : at === path.dirname(base) ? first : higher(at);
      return { pkg, file, lstat, top: path.dirname(path.dirname(base)) };
    };
    const source = fs.readFileSync(path.resolve("bin/fabric-mesh-backend"), "utf8");
    const region = source.slice(source.indexOf("// <chain-check>"), source.indexOf("// </chain-check>"));
    const shim = new Function("fs", "path", "process", `${region}\nreturn { unsafeAbove };`)(fs, path, process) as
      { unsafeAbove: (root: string, lstat: (at: string) => fs.Stats, euid?: number) => string | undefined };
    const bundle = (file: string, pkg: string, lstat: (at: string) => fs.Stats) => unsafeChain(file, pkg, undefined, lstat);

    it.skipIf(process.platform === "win32")("refuses a package under a root-owned 0755 dir whose parent is group/other-writable", () => {
      const { pkg, file, lstat, top } = layout(fake(0, 0o40755), at => at === top ? fake(0, 0o40777) : fake(0, 0o40755));
      expect(shim.unsafeAbove(pkg, lstat)).toBe(`${top} is group/other-writable (mode 777)`);
      expect(bundle(file, pkg, lstat)).toBe(`${top} is group/other-writable (mode 777)`);
    });

    it.skipIf(process.platform === "win32")("passes a root-owned sticky dir (like /tmp) with our own entry below it", () => {
      const { pkg, file, lstat } = layout(fake(0, 0o41777), () => fake(0, 0o40755));
      expect(shim.unsafeAbove(pkg, lstat)).toBeUndefined();
      expect(bundle(file, pkg, lstat)).toBeUndefined();
    });
  });

  it.skipIf(leaseSkip !== "")(`fails closed on a group-writable, non-sticky parent of the install root${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const parent = tempDir("parent");
    const pkg = path.join(parent, "pkg");
    fs.mkdirSync(path.join(pkg, "dist/native"), { recursive: true, mode: 0o700 });
    const helper = path.join(pkg, "dist/native/fabric-mesh-lease");
    fs.copyFileSync(leaseHelperPath(), helper);
    fs.chmodSync(helper, 0o700);
    const real = fs.realpathSync(parent);
    fs.chmodSync(real, 0o770);
    expect(probeStateDbLeases(root, { helper, trustRoot: pkg })[0]).toMatchObject({ name: "holder@lease-probe",
      reason: expect.stringContaining(`helper integrity: ${real} is group/other-writable (mode 770)`) });
    // A 0755 parent passes, and so do the test directory's own ancestors (/tmp is root-owned and sticky).
    fs.chmodSync(real, 0o755);
    expect(probeStateDbLeases(root, { helper, trustRoot: pkg })).toEqual([]);
  });

  // pi-fabric#880 review round 3 (SEC P1): a swapped-in bundle drops any check it carries, so the operator's shim checks
  // the chain BEFORE it imports the bundle. The fake bundle leaves a marker if any of its code runs.
  it.skipIf(binSkip !== "")(`the operator's bin refuses before loading a bundle under a group-writable parent${binSkip ? ` (skipped: ${binSkip})` : ""}`, async () => {
    // The host's real inventory may hold an unready installed reader (factory): its accepted override then alarms.
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
    const root = await crashedImport();
    const parent = tempDir("shim");
    const pkg = path.join(parent, "pkg");
    fs.cpSync(path.resolve("dist"), path.join(pkg, "dist"), { recursive: true });
    fs.cpSync(path.resolve("bin"), path.join(pkg, "bin"), { recursive: true });
    const bundle = path.join(pkg, "dist/mesh/mesh-backend-cli.js");
    const real = fs.readFileSync(bundle);
    const marker = path.join(parent, "bundle-ran");
    fs.writeFileSync(bundle, `import fs from "node:fs";\nfs.writeFileSync(${JSON.stringify(marker)}, "ran");\nexport const main = async () => 0;\n`, { mode: 0o600 });
    const cli = () => spawnSync(process.execPath, [path.join(pkg, "bin/fabric-mesh-backend"), "cutover", "--root", root,
      "--accept-unready", "factory,fabric@unknown"], { encoding: "utf8", timeout: 60_000, env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
    const parentReal = fs.realpathSync(parent);
    fs.chmodSync(parentReal, 0o770);
    const refused = cli();
    expect(refused.status, refused.stderr).toBe(3);
    expect(refused.stderr).toBe(`fabric-mesh-backend: refused before loading: helper integrity: ${parentReal} is group/other-writable (mode 770)\n`);
    expect(fs.existsSync(marker)).toBe(false);                 // no bundle code ran
    // Under a 0755 parent, with the real bundle back, the real switch passes.
    fs.chmodSync(parentReal, 0o755);
    fs.writeFileSync(bundle, real);
    const ready = cli();
    expect(ready.status, ready.stderr).toBe(0);
    expect(await backendOf(root)).toBe("sqlite");
  });

  // pi-fabric#880 review round 4 (CODE P1): a symlink at or below the package root could be swapped after the check.
  it.skipIf(binSkip !== "")(`the operator's bin refuses a symlinked bundle or bundle directory inside the package${binSkip ? ` (skipped: ${binSkip})` : ""}`, async () => {
    // The host's real inventory may hold an unready installed reader (factory): its accepted override then alarms.
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
    const root = await crashedImport();
    const pkg = path.join(tempDir("links"), "pkg");
    fs.cpSync(path.resolve("dist"), path.join(pkg, "dist"), { recursive: true });
    fs.cpSync(path.resolve("bin"), path.join(pkg, "bin"), { recursive: true });
    const cli = () => spawnSync(process.execPath, [path.join(pkg, "bin/fabric-mesh-backend"), "cutover", "--root", root,
      "--accept-unready", "factory,fabric@unknown"], { encoding: "utf8", timeout: 60_000, env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
    const pkgReal = fs.realpathSync(pkg);
    // (1) The bundle is a symlink, in a group-writable dist/mesh, to an in-root copy (here a fake that leaves a marker).
    const mesh = path.join(pkgReal, "dist/mesh");
    const bundle = path.join(mesh, "mesh-backend-cli.js");
    const copy = path.join(pkgReal, "dist/copy");
    fs.mkdirSync(copy, { mode: 0o700 });
    const marker = path.join(pkgReal, "..", "bundle-ran");
    fs.writeFileSync(path.join(copy, "cli.js"), `import fs from "node:fs";\nfs.writeFileSync(${JSON.stringify(marker)}, "ran");\nexport const main = async () => 0;\n`, { mode: 0o600 });
    const real = fs.readFileSync(bundle);
    fs.unlinkSync(bundle);
    fs.symlinkSync(path.join(copy, "cli.js"), bundle);
    fs.chmodSync(mesh, 0o770);
    // Walking down from the root, the writable dist/mesh refuses first; with it fixed, the symlinked bundle does.
    const writable = cli();
    expect(writable.status, writable.stderr).toBe(3);
    expect(writable.stderr).toBe(`fabric-mesh-backend: refused before loading: helper integrity: ${mesh} is group/other-writable (mode 770)\n`);
    fs.chmodSync(mesh, 0o700);
    const linked = cli();
    expect(linked.status, linked.stderr).toBe(3);
    expect(linked.stderr).toBe(`fabric-mesh-backend: refused before loading: helper integrity: ${bundle} is a symlink inside the install root\n`);
    expect(fs.existsSync(marker)).toBe(false);
    fs.unlinkSync(bundle);
    fs.writeFileSync(bundle, real, { mode: 0o600 });
    // (2) dist/mesh itself is an in-root symlink to the real directory.
    fs.renameSync(mesh, path.join(pkgReal, "dist/mesh-real"));
    fs.symlinkSync(path.join(pkgReal, "dist/mesh-real"), mesh);
    const dir = cli();
    expect(dir.status, dir.stderr).toBe(3);
    expect(dir.stderr).toBe(`fabric-mesh-backend: refused before loading: helper integrity: ${mesh} is a symlink inside the install root\n`);
    fs.unlinkSync(mesh);
    fs.renameSync(path.join(pkgReal, "dist/mesh-real"), mesh);
    // (2b) dist/ itself is an in-root symlink to a trusted copy: refused as a symlink, never resolved.
    const dist = path.join(pkgReal, "dist");
    fs.renameSync(dist, path.join(pkgReal, "dist-real"));
    fs.symlinkSync(path.join(pkgReal, "dist-real"), dist);
    const top = cli();
    expect(top.status, top.stderr).toBe(3);
    expect(top.stderr).toBe(`fabric-mesh-backend: refused before loading: helper integrity: ${dist} is a symlink inside the install root\n`);
    fs.unlinkSync(dist);
    fs.renameSync(path.join(pkgReal, "dist-real"), dist);
    // (3) The normal layout passes.
    const ready = cli();
    expect(ready.status, ready.stderr).toBe(0);
    expect(await backendOf(root)).toBe("sqlite");
  });

  // pi-fabric#880 review round 5 (CODE P1): launched through a symlinked package-root alias with the symlink-preserving
  // flags, the bin must still load the CANONICAL bundle it checked, never the alias (which another uid could swap).
  it.skipIf(binSkip !== "")(`the operator's bin loads the canonical bundle when launched through a package-root alias${binSkip ? ` (skipped: ${binSkip})` : ""}`, () => {
    const parent = tempDir("alias");
    const pkg = path.join(parent, "pkg");
    fs.cpSync(path.resolve("dist"), path.join(pkg, "dist"), { recursive: true });
    fs.cpSync(path.resolve("bin"), path.join(pkg, "bin"), { recursive: true });
    // The bundle records the URL it was loaded as.
    const seen = path.join(parent, "loaded-as");
    fs.writeFileSync(path.join(pkg, "dist/mesh/mesh-backend-cli.js"),
      `import fs from "node:fs";\nfs.writeFileSync(${JSON.stringify(seen)}, import.meta.url);\nexport const main = async () => 0;\n`, { mode: 0o600 });
    const alias = path.join(parent, "alias");
    fs.symlinkSync(pkg, alias);
    const result = spawnSync(process.execPath, [path.join(alias, "bin/fabric-mesh-backend"), "--help"], { encoding: "utf8", timeout: 60_000,
      env: { ...process.env, NODE_OPTIONS: "--preserve-symlinks --preserve-symlinks-main" } });
    expect(result.status, result.stderr).toBe(0);
    expect(fs.readFileSync(seen, "utf8")).toBe(pathToFileURL(path.join(fs.realpathSync(pkg), "dist/mesh/mesh-backend-cli.js")).href);
  });

  it.skipIf(binSkip !== "")(`end to end: a group-writable directory of the gate's own bundle refuses the switch${binSkip ? ` (skipped: ${binSkip})` : ""}`, async () => {
    // The host's real inventory may hold an unready installed reader (factory): its accepted override then alarms.
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
    const root = await crashedImport();
    const pkg = tempDir("bundle");
    fs.cpSync(path.resolve("dist"), path.join(pkg, "dist"), { recursive: true });
    fs.cpSync(path.resolve("bin"), path.join(pkg, "bin"), { recursive: true });
    const cli = () => spawnSync(process.execPath, [path.join(pkg, "bin/fabric-mesh-backend"), "cutover", "--root", root,
      "--accept-unready", "factory,fabric@unknown"], { encoding: "utf8", timeout: 60_000, env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
    // dist/mesh holds the gate (mesh-backend-cli.js), not the helper: only the bundle chain covers it.
    const mesh = fs.realpathSync(path.join(pkg, "dist/mesh"));
    fs.chmodSync(mesh, 0o775);
    const refused = cli();
    expect(refused.status, refused.stderr).toBe(3);
    // Since round 3 the shim refuses first, before the bundle loads; the bundle's own check stays as defence in depth.
    expect(refused.stderr).toBe(`fabric-mesh-backend: refused before loading: helper integrity: ${mesh} is group/other-writable (mode 775)\n`);
    fs.chmodSync(mesh, 0o700);
    const ready = cli();
    expect(ready.status, ready.stderr).toBe(0);
  });

  // smarty-dev#8116: a holder that opens AFTER the fenced lease probe and before the commit. The python3 child
  // waits for a trigger file, so the switch's own onStep (between its open of state.db and the commit) runs it.
  const lateHolder = async (file: string, mode: "open" | "link"): Promise<{ trigger: () => void; release: () => Promise<void> }> => {
    const scratch = tempDir("late");
    const code = `
import ctypes, os, sys, time
ctypes.CDLL(None).prctl(4, 0, 0, 0, 0)
print("ready", flush=True)
while not os.path.exists(sys.argv[2] + "/go"): time.sleep(0.005)
if sys.argv[3] == "link":
    os.link(sys.argv[1], sys.argv[2] + "/alias"); fd = os.open(sys.argv[2] + "/alias", os.O_RDWR); os.unlink(sys.argv[2] + "/alias")
else:
    fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
open(sys.argv[2] + "/opened", "w").close()
sys.stdin.readline()`;
    const child = spawn("python3", ["-c", code, file, scratch, mode], { stdio: ["pipe", "pipe", "inherit"] });
    await new Promise<void>((resolve, reject) => {
      const deadline = setTimeout(() => reject(new Error("python3 holder not ready within 10 s")), 10_000);
      child.stdout!.once("data", () => { clearTimeout(deadline); resolve(); });
    });
    const trigger = (): void => {
      fs.writeFileSync(path.join(scratch, "go"), "");
      const until = Date.now() + 10_000;
      while (!fs.existsSync(path.join(scratch, "opened"))) {
        if (Date.now() > until) throw new Error("late holder did not open");
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
      }
    };
    const release = (): Promise<void> => new Promise((resolve) => {
      if (child.exitCode !== null) { resolve(); return; }
      const deadline = setTimeout(() => { child.kill("SIGKILL"); resolve(); }, 5_000);
      child.once("exit", () => { clearTimeout(deadline); resolve(); });
      child.stdin!.end("\n");
    });
    return { trigger, release };
  };
  // pi-fabric#881 (Astra P1): no netting of closes. Two foreign processes open state.db back to back inside the window
  // and one closes again: the other still holds it, so the switch refuses (an open is never cancelled by a close).
  it.skipIf(leaseSkip !== "")(`refuses two back-to-back foreign openers of which one closes again${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const file = path.join(root, "state.db");
    const holder = await lateHolder(file, "open");
    try {
      const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
        leaseProbe: { beforeReport: () => {
          holder.trigger();
          spawnSync("python3", ["-c", "import os, sys; os.close(os.open(sys.argv[1], os.O_RDONLY))", file], { timeout: 10_000 });
        } } });
      expect(result.code, result.err).toBe(3);
      expect(result.err).toContain(`holder@lease:state.db (unidentified holder of ${file} (2 open(s) since the lease probe; smarty-dev#8116))`);
    } finally { await holder.release(); }
  });

  // pi-fabric#881 (Astra P2 #3): nothing is subtracted for the parent: a dup() by the parent plus a foreign opener
  // reports every open, and the verdict (opens > probed) refuses.
  it.skipIf(leaseSkip !== "")(`a dup by the parent plus a foreign opener is refused, never subtracted${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, () => {
    const dir = tempDir("dup");
    const file = path.join(dir, "state.db");
    fs.writeFileSync(file, "x", { mode: 0o600 });
    // python3 is the PARENTPID: it starts the helper, opens the file once and dup()s it; a foreign child opens it too.
    const script = `
import os, subprocess, sys, time
helper, file, control = sys.argv[1], sys.argv[2], sys.argv[3]
out = open(control + ".out", "w")
child = subprocess.Popen([helper, "--watch", str(os.getpid()), control, file], stdout=out, pass_fds=())
while "watching" not in open(control + ".out").read(): time.sleep(0.01)
fd = os.open(file, os.O_RDWR); os.dup(fd)
foreign = subprocess.Popen([sys.executable, "-c", "import os, sys, time; os.open(sys.argv[1], os.O_RDONLY); print('ready', flush=True); time.sleep(30)", file], stdout=subprocess.PIPE)
foreign.stdout.readline()
open(control + "/go1", "w").close()
while "end 1" not in open(control + ".out").read(): time.sleep(0.01)
print(open(control + ".out").read(), flush=True)
foreign.kill(); child.kill()`;
    const control = path.join(dir, "control");
    fs.mkdirSync(control, { mode: 0o700 });
    const result = spawnSync("python3", ["-c", script, leaseHelperPath(), file, control], { encoding: "utf8", timeout: 30_000 });
    // opens: the probe's own, the parent's one open, the foreign open; own: ONE description although two fds.
    // opens: the probe's own, the parent's open (its dup makes no event), the foreign open: 3 > PROBED 1, refused.
    expect(result.stdout, result.stderr).toContain(`opens 3 1 ${file}\n`);
  });

  // pi-fabric#881: the window ends BEFORE the switch's first own open of the files, so no own open can merge with a
  // foreign one inside it (by construction): the report is taken before SQLite opens state.db.
  it.skipIf(leaseSkip !== "")(`takes the watch's final report before the switch's first own open of the files${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const order: string[] = [];
    // Every own open of a watched file: Node's fs opens (the create, the pinned fd) and SQLite's (the opener).
    const openSync = fs.openSync;
    const spy = vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (/state\.db(-wal|-shm)?$/.test(String(file))) order.push(`open ${path.basename(String(file))}`);
      return (openSync as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
      leaseProbe: { afterReport: () => { order.push("report"); } },
      options: { open: (file: string) => { order.push(`open ${path.basename(file)}`); return openNodeSqlite(file); } } });
    expect(result.code, result.err).toBe(0);
    spy.mockRestore();
    expect(order[0]).toBe("report");
    expect(order.filter(step => step === "report")).toHaveLength(1);
    expect(order.slice(1).every(step => step.startsWith("open "))).toBe(true);
  });

  const lateCases = [
    { name: "state.db", mode: "open", why: "1 open(s) since the lease probe" },
    // -shm does not exist at the probe: the foreign holder creates it inside the window, seen by name.
    { name: "state.db-shm", mode: "open", why: "1 open(s) since the lease probe" },
    // A hard link elsewhere, opened and removed again: the inode watch still sees the open.
    { name: "state.db", mode: "link", why: "opened through another name (a hard link) since the lease probe" },
  ] as const;
  for (const late of lateCases) {
    it.skipIf(leaseSkip !== "")(`refuses a lock-free, non-dumpable holder that opens ${late.name} (${late.mode}) after the lease probe and before the commit (smarty-dev#8116)${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
      const root = await crashedImport();
      if (late.name === "state.db-shm") expect(fs.existsSync(path.join(root, late.name))).toBe(false);
      const holder = await lateHolder(path.join(root, late.name), late.mode);
      try {
        const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
          leaseProbe: { beforeReport: () => holder.trigger() } });
        expect(result.code, result.err).toBe(3);
        expect(result.err).toContain(`holder@lease:${late.name} (unidentified holder of ${root}/${late.name} (${late.why}; smarty-dev#8116))`);
        expect(await backendOf(root)).toBe("importing");
      } finally { await holder.release(); }
      // Without the late holder the same switch is ready: its own SQLite fds are not holders.
      const ready = await run(["cutover", "--root", root], { procScan: { listPids: () => [] } });
      expect(ready.code, ready.err).toBe(0);
      expect(await backendOf(root)).toBe("sqlite");
    });
  }

  // smarty-dev#9473: a mode, owner or link-count change (IN_ATTRIB) on a watched component fails closed like an open.
  const attribCases = [
    { name: "a chmod of state.db", change: (root: string) => fs.chmodSync(path.join(root, "state.db"), 0o640),
      undo: (root: string) => fs.chmodSync(path.join(root, "state.db"), 0o600) },
    // Only the inode watch sees this: the new name is in a directory nobody watches.
    { name: "a hard link to state.db in an unwatched directory", change: (root: string) => fs.linkSync(path.join(root, "state.db"), path.join(root, "..", `${path.basename(root)}-link`)),
      undo: (root: string) => fs.rmSync(path.join(root, "..", `${path.basename(root)}-link`), { force: true }) },
  ] as const;
  for (const attrib of attribCases) {
    it.skipIf(leaseSkip !== "")(`refuses ${attrib.name} after the lease probe (smarty-dev#9473)${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
      const root = await crashedImport();
      const mode = fs.statSync(path.join(root, "state.db")).mode & 0o777;
      try {
        const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
          leaseProbe: { beforeReport: () => attrib.change(root) } });
        expect(result.code, result.err).toBe(3);
        expect(result.err).toContain(`holder@lease:state.db (unidentified holder of ${root}/state.db (its mode, owner or link count changed since the lease probe; smarty-dev#8116))`);
        expect(await backendOf(root)).toBe("importing");
      } finally { attrib.undo(root); fs.chmodSync(path.join(root, "state.db"), mode); }
      // Without the change the same switch is ready.
      const ready = await run(["cutover", "--root", root], { procScan: { listPids: () => [] } });
      expect(ready.code, ready.err).toBe(0);
    });
  }

  // pi-fabric#881 Astra round 4 (P1): a hard link out, the original name unlinked, then the retained link opened before
  // the final report. The inode stays alive with no IN_DELETE_SELF; the removed name must not end counting silently.
  it.skipIf(leaseSkip !== "")(`refuses a state.db whose name is unlinked while a hard link elsewhere is opened (pi-fabric#881 round 4)${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const db = path.join(root, "state.db");
    const link = path.join(root, "..", `${path.basename(root)}-retained`);
    let fd: number | undefined;
    try {
      const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
        leaseProbe: { beforeReport: () => { fs.linkSync(db, link); fs.unlinkSync(db); fd = fs.openSync(link, "r+"); } } });
      expect(result.code, result.err).toBe(3);
      expect(result.err).toContain(`holder@lease:state.db (unidentified holder of ${db} (its name was removed since the lease probe (a hard link may keep the inode open); smarty-dev#8116))`);
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
      fs.rmSync(link, { force: true });
    }
    // Refused, never committed: the switch record ends in a refused outcome. (The old store lived on only through the
    // retained link: exactly the detached store the rule refuses to switch away from. The ready path is the tests above.)
    expect(records(root).at(-1)).toMatchObject({ phase: "outcome", ok: false, refused: true });
  });

  it.skipIf(leaseSkip !== "")(`refuses a chmod of a watched state-shadow/ after the lease probe (smarty-dev#9473)${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const shadow = path.join(root, "state-shadow");
    fs.mkdirSync(shadow, { mode: 0o700 });
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
      leaseProbe: { beforeReport: () => fs.chmodSync(shadow, 0o750) } });
    expect(result.code, result.err).toBe(3);
    expect(result.err).toContain("the lease watch reported a watched directory's mode, owner or link count changed (smarty-dev#8116)");
    expect(await backendOf(root)).toBe("importing");
    fs.chmodSync(shadow, 0o700);
    const ready = await run(["cutover", "--root", root], { procScan: { listPids: () => [] } });
    expect(ready.code, ready.err).toBe(0);
  });

  // smarty-dev#8116 review round 1 (SEC P1): a watched directory swapped for a new one during the window. The new
  // state-shadow/ (and any SQLite file in it) is unwatched, so the counts are void: refuse.
  it.skipIf(leaseSkip !== "")(`refuses when a watched state-shadow/ is moved away and replaced after the lease probe${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const shadow = path.join(root, "state-shadow");
    fs.mkdirSync(shadow, { mode: 0o700 });
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
      leaseProbe: { beforeReport: () => {
        fs.renameSync(shadow, path.join(root, "state-shadow.old"));
        fs.mkdirSync(shadow, { mode: 0o700 });
        fs.writeFileSync(path.join(shadow, "state.db"), "");
      } } });
    expect(result.code, result.err).toBe(3);
    expect(result.err).toContain("holder@lease-probe (state.db lease probe unavailable (smarty-dev#7936): the lease watch reported a watched directory or file moved, deleted or replaced (smarty-dev#8116))");
    expect(await backendOf(root)).toBe("importing");
    // With the directory left alone the same switch is ready.
    const ready = await run(["cutover", "--root", root], { procScan: { listPids: () => [] } });
    expect(ready.code, ready.err).toBe(0);
  });

  // The real helper: a state-shadow/ that did not exist at the probe appears during the window (unwatched): refuse.
  it.skipIf(leaseSkip !== "")(`refuses when a state-shadow/ appears after the lease probe${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    expect(fs.existsSync(path.join(root, "state-shadow"))).toBe(false);
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
      leaseProbe: { beforeReport: () => { fs.mkdirSync(path.join(root, "state-shadow"), { mode: 0o700 }); } } });
    expect(result.code, result.err).toBe(3);
    expect(result.err).toContain("holder@lease-probe (state.db lease probe unavailable (smarty-dev#7936): the lease watch reported a new state-shadow directory (smarty-dev#8116))");
    expect(await backendOf(root)).toBe("importing");
  });

  // pi-fabric#881 review round 2 (SEC P1): the missing state-shadow/ appears as a SYMLINK to a directory elsewhere, and
  // a holder opens state.db through it; or a pre-made directory is renamed into place. Both refuse.
  it.skipIf(leaseSkip !== "")(`refuses when a missing state-shadow/ appears as a symlink and a holder opens state.db through it${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const outside = tempDir("outside");
    fs.writeFileSync(path.join(outside, "state.db"), "");
    const holder = await lateHolder(path.join(root, "state-shadow", "state.db"), "open");
    try {
      const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
        leaseProbe: { beforeReport: () => {
          fs.symlinkSync(outside, path.join(root, "state-shadow"));
          holder.trigger();
        } } });
      expect(result.code, result.err).toBe(3);
      expect(result.err).toContain("holder@lease-probe (state.db lease probe unavailable (smarty-dev#7936): the lease watch reported a new state-shadow directory (smarty-dev#8116))");
      expect(await backendOf(root)).toBe("importing");
    } finally { await holder.release(); }
  });

  it.skipIf(leaseSkip !== "")(`refuses when a pre-made directory is renamed into place as state-shadow/${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const premade = path.join(root, "premade");
    fs.mkdirSync(premade, { mode: 0o700 });
    fs.writeFileSync(path.join(premade, "state.db"), "");
    const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] },
      leaseProbe: { beforeReport: () => { fs.renameSync(premade, path.join(root, "state-shadow")); } } });
    expect(result.code, result.err).toBe(3);
    expect(result.err).toContain("the lease watch reported a new state-shadow directory (smarty-dev#8116)");
  });

  // ponytail: an inotify queue overflow needs root (fs.inotify.max_queued_events); a fake helper with a matching
  // digest stands in for the helper's report, so the gate's handling of it is what this proves.
  for (const report of ["overflow", "silent"] as const) {
    it.skipIf(leaseSkip !== "")(`fails closed when the lease watch reports ${report}${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
      const root = await crashedImport();
      const helper = path.join(tempDir("fake"), "fabric-mesh-lease");
      fs.writeFileSync(helper, `#!/bin/sh
if [ "$1" = --watch ]; then control=$3; shift 3; fi
for f in "$@"; do if [ -e "$f" ]; then echo "free $f"; else echo "absent $f"; fi; done
[ -n "$control" ] || exit 0
echo watching
while [ ! -e "$control/go1" ]; do sleep 0.01; done
${report === "silent" ? "" : `echo ${report}; echo "end 1"`}
exec sleep 30
`, { mode: 0o700 });
      const expectedDigest = createHash("sha256").update(fs.readFileSync(helper)).digest("hex");
      const started = Date.now();
      const result = await run(["cutover", "--root", root], { procScan: { listPids: () => [] }, leaseProbe: { helper, expectedDigest } });
      expect(result.code, result.err).toBe(3);
      expect(result.err).toContain(`holder@lease-probe (state.db lease probe unavailable (smarty-dev#7936): ${report === "overflow"
        ? "the lease watch reported lost events (inotify queue overflow)" : "the lease watch did not report"} (smarty-dev#8116))`);
      expect(await backendOf(root)).toBe("importing");
      if (report === "silent") expect(Date.now() - started).toBeLessThan(30_000);
    }, 60_000);
  }

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

  // smarty-dev#8145 item 2: another user who can write the helper or its directory could change what runs.
  it.skipIf(leaseSkip !== "")(`fails closed on a group- or other-writable helper or helper directory${leaseSkip ? ` (skipped: ${leaseSkip})` : ""}`, async () => {
    const root = await crashedImport();
    const copy = tempDir("helper");
    const helper = path.join(copy, "fabric-mesh-lease");
    fs.copyFileSync(leaseHelperPath(), helper);
    fs.chmodSync(copy, 0o700);
    fs.chmodSync(helper, 0o700);
    expect(probeStateDbLeases(root, { helper })).toEqual([]);
    fs.chmodSync(helper, 0o720);
    expect(probeStateDbLeases(root, { helper })[0]).toMatchObject({ name: "holder@lease-probe",
      reason: expect.stringContaining(`helper integrity: ${helper} is group/other-writable (mode 720)`) });
    fs.chmodSync(helper, 0o700);
    fs.chmodSync(copy, 0o703);
    try {
      const refused = await run(["cutover", "--root", root], { procScan: { listPids: () => [] }, leaseProbe: { helper } });
      expect(refused.code).toBe(3);
      expect(refused.err).toContain(`helper integrity: ${copy} is group/other-writable (mode 703)`);
    } finally { fs.chmodSync(copy, 0o700); }
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
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
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
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
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
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", SWITCH_OWNER);
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
