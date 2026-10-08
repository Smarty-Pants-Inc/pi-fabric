import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as writerCensus from "../src/mesh/writer-census.js";
import { census } from "../src/mesh/writer-census.js";
import { MeshStore, censusRecordFileName, meshProcessStartedAt, pruneDeadCensusRecords } from "../src/mesh/store.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { meshWriterLeaseRecord, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";

const ownStartedAt = (): number => Math.floor(Date.now() - process.uptime() * 1000);
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const writeRecord = (mesh: string, record: Record<string, unknown>): string => {
  const directory = path.join(mesh, ".writer-census");
  fs.mkdirSync(directory, { recursive: true });
  const file = path.join(directory, typeof record.host === "string" && Number.isSafeInteger(record.startedAt)
    ? censusRecordFileName(record.host, Number(record.pid), Number(record.startedAt))
    : `${String(record.pid)}-${String(record.startedAt)}-${String(record.lockProtocol ?? "")}${String(record.stateBackend ?? "")}.json`);
  fs.writeFileSync(file, JSON.stringify({ format: 1, releaseSha: "abc123", lockProtocol: 2, stateBackend: "sqlite", ...record }));
  return file;
};

const CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import path from "node:path";
const [root] = process.argv.slice(1);
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
new MeshStore(root, 4096, 100, { stateBackend: "file" });
process.stdout.write(JSON.stringify(fs.readdirSync(path.join(root, ".writer-census"))) + "\\n");
`;

// Another host's writer with this child's pid and start time is already recorded in the shared mesh.
const COLLIDING_CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import path from "node:path";
const [root, other] = process.argv.slice(1);
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore, meshProcessStartedAt, censusRecordFileName } = await jiti.import("./src/mesh/store.ts");
const directory = path.join(root, ".writer-census");
fs.mkdirSync(directory, { recursive: true });
fs.writeFileSync(path.join(directory, censusRecordFileName(other, process.pid, meshProcessStartedAt)), JSON.stringify({ format: 1,
  pid: process.pid, host: other, releaseSha: "abc123", lockProtocol: 2, stateBackend: "sqlite", startedAt: meshProcessStartedAt }));
new MeshStore(root, 4096, 100, { stateBackend: "file" });
process.stdout.write(JSON.stringify({ pid: process.pid, startedAt: meshProcessStartedAt, names: fs.readdirSync(directory).sort() }) + "\\n");
`;

const runChild = async (script: string, args: string[]): Promise<{ code: number | null; stdout: string; stderr: string; pid: number }> => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", script, ...args],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
  let stdout = "", stderr = "";
  child.stdout.on("data", chunk => { stdout += chunk; });
  child.stderr.on("data", chunk => { stderr += chunk; });
  const code = await new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
  return { code, stdout, stderr, pid: child.pid! };
};

const roots: string[] = [];
const root = (): string => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-writer-census-"));
  roots.push(value);
  return value;
};

// Makes a file or directory unreadable: chmod 000 on POSIX (unless root, who reads anyway), else
// an EACCES from the fs calls the census makes on that exact path.
const denied: string[] = [];
const deniedPaths = new Set<string>();
const denyRead = (target: string): void => {
  if (process.platform !== "win32" && process.getuid?.() !== 0) {
    fs.chmodSync(target, 0o000);
    denied.push(target);
    return;
  }
  deniedPaths.add(path.resolve(target));
  if (vi.isMockFunction(fs.readdirSync)) return;
  const eacces = (file: unknown): Error => Object.assign(new Error(`EACCES: permission denied, '${String(file)}'`), { code: "EACCES" });
  const blocked = (file: unknown): boolean => deniedPaths.has(path.resolve(String(file)));
  const readdir = fs.readdirSync, readFile = fs.readFileSync;
  vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    if (blocked(file)) throw eacces(file);
    return (readdir as (...args: unknown[]) => unknown)(file, ...rest);
  }) as typeof fs.readdirSync);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    if (blocked(file)) throw eacces(file);
    return (readFile as (...args: unknown[]) => unknown)(file, ...rest);
  }) as typeof fs.readFileSync);
};

afterEach(() => {
  vi.restoreAllMocks();
  deniedPaths.clear();
  for (const target of denied.splice(0)) fs.chmodSync(target, 0o700);
  for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true });
});

describe("mesh writer census", () => {
  it("registers each MeshStore process with its actual lock and backend modes", async () => {
    const mesh = root();
    const previous = process.env.PI_FABRIC_RELEASE_SHA;
    process.env.PI_FABRIC_RELEASE_SHA = "build-sha";
    try {
      new MeshStore(mesh, 4096, 100, { lockProtocol: 2, stateBackend: "file" });
      await new Promise(resolve => setTimeout(resolve, 0));
      const result = await census(mesh);
      expect(result.writers).toContainEqual(expect.objectContaining({ pid: process.pid, releaseSha: "build-sha",
        lockProtocol: 2, stateBackend: "file", source: "process-record" }));
      expect(result.unknown).toEqual([]);
    } finally {
      if (previous === undefined) delete process.env.PI_FABRIC_RELEASE_SHA;
      else process.env.PI_FABRIC_RELEASE_SHA = previous;
    }
  });

  it("reports a live per-process record with its release and backend mode", async () => {
    const mesh = root();
    const directory = path.join(mesh, ".writer-census");
    fs.mkdirSync(directory);
    const writer = { pid: process.pid, host: os.hostname(), releaseSha: "abc123", lockProtocol: 2,
      stateBackend: "sqlite", startedAt: ownStartedAt() };
    fs.writeFileSync(path.join(directory, `${process.pid}.json`), JSON.stringify({ format: 1, ...writer }));
    const result = await census(mesh);
    expect(result.unknown).toEqual([]);
    expect(result.unknown).toEqual([]);
    expect(result.writers).toContainEqual(expect.objectContaining({ ...writer, source: "process-record" }));
  });

  it("reads writer metadata from the existing host lease", async () => {
    const mesh = root();
    const now = Date.now();
    const writer = { pid: process.pid, host: "lease-host", releaseSha: "release-456", lockProtocol: 2,
      stateBackend: "shadow", startedAt: now - 1000 };
    writeHostLease(mesh, { id: "host:lease", rootId: "session:lease", identityId: "identity:lease",
      updatedAt: now, expiresAt: now + 60_000, writer });
    const result = await census(mesh);
    expect(result.unknown).toEqual([]);
    expect(result.writers).toContainEqual(expect.objectContaining({ ...writer, source: "host-lease", name: "host:lease" }));
  });

  it("flags a live pre-census owner record as an unknown writer", async () => {
    const mesh = root();
    fs.mkdirSync(path.join(mesh, ".lock"));
    fs.writeFileSync(path.join(mesh, ".lock", "owner"), `old-token\n${process.pid}\n${Date.now()}\n`);
    const queue = meshLockQueueDirectory(mesh);
    fs.mkdirSync(queue, { recursive: true });
    const ticket = `${"2".repeat(24)}-${process.pid}-01234567-89ab-cdef-0123-456789abcdef`;
    fs.writeFileSync(path.join(queue, ticket), "");
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ pid: process.pid, source: "lock-owner", name: "old-token",
      evidence: expect.arrayContaining(["lock-owner:old-token", `lock-ticket:${ticket}`]) }));
  });

  it("counts live pre-census queue tickets by ticket name", async () => {
    const mesh = root();
    const queue = meshLockQueueDirectory(mesh);
    fs.mkdirSync(queue, { recursive: true });
    const name = `${"1".repeat(24)}-${process.pid}-01234567-89ab-cdef-0123-456789abcdef`;
    fs.writeFileSync(path.join(queue, name), "");
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ pid: process.pid, source: "lock-ticket", name }));
  });
  it("deletes this host's record of a dead process", async () => {
    const mesh = root();
    const pid = deadPid();
    const file = writeRecord(mesh, { pid, host: os.hostname(), startedAt: ownStartedAt() });
    const result = await census(mesh);
    expect(result.writers.find(writer => writer.pid === pid)).toBeUndefined();
    expect(result.unknown).toEqual([]);
    expect(fs.existsSync(file)).toBe(false);
  });

  it("removes its own record when the process exits", async () => {
    const mesh = root();
    const { code, stdout, stderr, pid } = await runChild(CHILD, [mesh]);
    expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
    const prefix = censusRecordFileName(os.hostname(), pid, 0).replace(/0\.json$/, "");
    expect(JSON.parse(stdout)).toEqual([expect.stringMatching(/^.+-\d+-\d+\.json$/)]);
    expect(JSON.parse(stdout)[0].startsWith(prefix)).toBe(true);
    expect(fs.readdirSync(path.join(mesh, ".writer-census"))).toEqual([]);
  }, 30_000);

  it("keeps two hosts' writers with the same pid and start time apart; each exit removes only its own", async () => {
    const mesh = root();
    const other = "other-host.example";
    const { code, stdout, stderr, pid } = await runChild(COLLIDING_CHILD, [mesh, other]);
    expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
    const reported = JSON.parse(stdout) as { pid: number; startedAt: number; names: string[] };
    const mine = censusRecordFileName(os.hostname(), pid, reported.startedAt);
    const theirs = censusRecordFileName(other, pid, reported.startedAt);
    expect(mine).not.toBe(theirs);
    // The later writer still records itself (no shared file to skip on) ...
    expect(reported.names).toEqual([mine, theirs].sort());
    // ... and its exit removes only its own record, never the other host's.
    expect(fs.readdirSync(path.join(mesh, ".writer-census"))).toEqual([theirs]);
  }, 30_000);

  it("prunes only this host's record files, even when another host's looks dead here", async () => {
    const mesh = root();
    const startedAt = meshProcessStartedAt - 3_600_000;
    // Same pid as this live process with a mismatching start time, but another host's: kept.
    const theirs = writeRecord(mesh, { pid: process.pid, host: "other-host.example", startedAt });
    const dead = deadPid();
    // A name in this host's namespace whose content names another host: kept.
    const directory = path.join(mesh, ".writer-census");
    const mislabelled = path.join(directory, censusRecordFileName(os.hostname(), dead, startedAt));
    fs.writeFileSync(mislabelled, JSON.stringify({ format: 1, pid: dead, host: "other-host.example", releaseSha: "abc123",
      lockProtocol: 2, stateBackend: "sqlite", startedAt }));
    const mineDead = writeRecord(mesh, { pid: deadPid(), host: os.hostname(), startedAt });
    pruneDeadCensusRecords(mesh);
    expect(fs.existsSync(theirs)).toBe(true);
    expect(fs.existsSync(mislabelled)).toBe(true);
    expect(fs.existsSync(mineDead)).toBe(false);
  });

  it.each([
    { label: "start time 0", startedAt: 0 },
    { label: "a negative start time", startedAt: -5 },
    { label: "a fractional start time", startedAt: 1.5 },
    { label: "a string start time", startedAt: "yesterday" },
    { label: "no start time", startedAt: undefined },
  ])("retains this host's live-pid record with $label and counts it unknown", async ({ startedAt }) => {
    const mesh = root();
    const directory = path.join(mesh, ".writer-census");
    fs.mkdirSync(directory, { recursive: true });
    const file = path.join(directory, `invalid-start-${process.pid}.json`);
    fs.writeFileSync(file, JSON.stringify({ format: 1, pid: process.pid, host: os.hostname(), releaseSha: "abc123",
      lockProtocol: 2, stateBackend: "sqlite", ...(startedAt === undefined ? {} : { startedAt }) }));
    pruneDeadCensusRecords(mesh);
    const result = await census(mesh);
    expect(fs.existsSync(file)).toBe(true);
    expect(result.unknown).not.toEqual([]);
    const record = result.unknown.find(writer => writer.source === "process-record");
    expect(record).toEqual(expect.objectContaining({ pid: process.pid, host: os.hostname(), name: path.basename(file) }));
    expect(record).not.toHaveProperty("startedAt");
  });

  it.each([
    { label: "no host", host: undefined },
    { label: "an empty host", host: "" },
    { label: "a numeric host", host: 7 },
  ])("retains a dead pid's record with $label and counts it unknown", async ({ host }) => {
    const mesh = root();
    const pid = deadPid();
    const directory = path.join(mesh, ".writer-census");
    fs.mkdirSync(directory, { recursive: true });
    const file = path.join(directory, `hostless-${pid}.json`);
    fs.writeFileSync(file, JSON.stringify({ format: 1, pid, releaseSha: "abc123", lockProtocol: 2, stateBackend: "sqlite",
      startedAt: Date.now() - 1000, ...(host === undefined ? {} : { host }) }));
    const queue = meshLockQueueDirectory(mesh);
    fs.mkdirSync(queue, { recursive: true });
    const ticket = `${"4".repeat(24)}-${pid}-01234567-89ab-cdef-0123-456789abcdef`;
    fs.writeFileSync(path.join(queue, ticket), "");
    const result = await census(mesh);
    expect(fs.existsSync(file)).toBe(true);
    expect(result.unknown).not.toEqual([]);
    const record = result.unknown.find(writer => writer.source === "process-record");
    expect(record).toEqual(expect.objectContaining({ pid, name: path.basename(file) }));
    expect(record).not.toHaveProperty("host");
    // Its lock ticket is not dismissed as this host's dead pid either.
    expect(record?.evidence).toContain(`lock-ticket:${ticket}`);
  });

  it.each([
    { label: "lock protocol 3", fields: { lockProtocol: 3 }, absent: "lockProtocol" },
    { label: "lock protocol 0", fields: { lockProtocol: 0 }, absent: "lockProtocol" },
    { label: "a string lock protocol", fields: { lockProtocol: "2" }, absent: "lockProtocol" },
    { label: "an unsupported backend", fields: { stateBackend: "redis" }, absent: "stateBackend" },
    { label: "an empty backend", fields: { stateBackend: "" }, absent: "stateBackend" },
    { label: "a backend weaker than one it lists", fields: { stateBackend: "file", stateBackends: ["file", "sqlite"] }, absent: "stateBackend" },
    { label: "a backend list without its backend", fields: { stateBackend: "sqlite", stateBackends: ["file"] }, absent: "stateBackend" },
    { label: "a non-list of backends", fields: { stateBackends: "sqlite" }, absent: "stateBackend" },
    { label: "a protocol newer than one it lists", fields: { lockProtocol: 2, lockProtocols: [1, 2] }, absent: "lockProtocol" },
    { label: "an unknown release", fields: { releaseSha: "unknown" }, absent: undefined },
    { label: "an empty release", fields: { releaseSha: "" }, absent: "releaseSha" },
  ])("counts a live writer with $label unknown", async ({ fields, absent }) => {
    const mesh = root();
    const file = writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt(), ...fields });
    const result = await census(mesh);
    expect(fs.existsSync(file)).toBe(true);
    expect(result.unknown).not.toEqual([]);
    const record = result.unknown.find(writer => writer.source === "process-record");
    expect(record).toEqual(expect.objectContaining({ pid: process.pid, host: os.hostname() }));
    if (absent) expect(record).not.toHaveProperty(absent);
  });

  it.each([
    { label: "lock protocol 3", fields: { lockProtocol: 3 } },
    { label: "an unsupported backend", fields: { stateBackend: "redis" } },
  ])("drops lease writer metadata with $label", async ({ fields }) => {
    const mesh = root();
    const now = Date.now();
    writeHostLease(mesh, { id: "host:unsupported", rootId: "session:u", identityId: "identity:u", updatedAt: now,
      expiresAt: now + 60_000, writer: { pid: process.pid, host: "lease-host", releaseSha: "abc123", lockProtocol: 2,
        stateBackend: "sqlite", startedAt: now - 1000, ...fields } as never });
    expect(readHostLeases(mesh).get("host:unsupported")?.writer).toBeUndefined();
    const result = await census(mesh);
    expect(result.unknown).toContainEqual({ source: "host-lease", name: "host:unsupported", reason: "incomplete or unsupported writer metadata" });
  });

  it("counts an unparsable or pid-less record unknown and keeps it", async () => {
    const mesh = root();
    const directory = path.join(mesh, ".writer-census");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "torn.json"), "{");
    fs.writeFileSync(path.join(directory, "pidless.json"), JSON.stringify({ format: 1, host: os.hostname(), startedAt: ownStartedAt() }));
    const result = await census(mesh);
    expect(result.unknown).toEqual(expect.arrayContaining([
      { source: "process-record", name: "torn.json", reason: `${path.join(directory, "torn.json")}: invalid` },
      { source: "process-record", name: "pidless.json", reason: `${path.join(directory, "pidless.json")}: invalid` }]));
    expect(fs.readdirSync(directory).sort()).toEqual(["pidless.json", "torn.json"]);
  });


  it("never drops another host's record, and counts it unknown without its unexpired lease", async () => {
    const mesh = root();
    const pid = deadPid();
    const record = { pid, host: "other-host.example", startedAt: Date.now() - 1000 };
    const file = writeRecord(mesh, record);
    const first = await census(mesh);
    expect(first.unknown).not.toEqual([]);
    expect(first.writers).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));
    expect(first.unknown).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));
    expect(fs.existsSync(file)).toBe(true);

    const now = Date.now();
    const writer = { ...record, releaseSha: "abc123", lockProtocol: 2, stateBackend: "sqlite" };
    writeHostLease(mesh, { id: "host:other", rootId: "session:other", identityId: "identity:other",
      updatedAt: now, expiresAt: now + 60_000, writer });
    const leased = await census(mesh);
    expect(leased.unknown).toEqual([]);
    expect(leased.writers).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));

    writeHostLease(mesh, { id: "host:other", rootId: "session:other", identityId: "identity:other",
      updatedAt: now - 120_000, expiresAt: now - 60_000, writer });
    const expired = await census(mesh);
    expect(expired.unknown).not.toEqual([]);
    expect(expired.unknown).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));
  });

  it.each([
    { label: "an empty host and start time 0", host: "", startedAt: 0 },
    { label: "an empty host", host: "", startedAt: Date.now() - 1000 },
    { label: "start time 0", host: "lease-host", startedAt: 0 },
    { label: "a negative start time", host: "lease-host", startedAt: -1 },
  ])("drops lease writer metadata with $label, and reports the lease unknown", async ({ host, startedAt }) => {
    const mesh = root();
    const now = Date.now();
    const writer = { pid: process.pid, host, releaseSha: "release-456", lockProtocol: 2, stateBackend: "sqlite", startedAt };
    writeHostLease(mesh, { id: "host:bad", rootId: "session:bad", identityId: "identity:bad",
      updatedAt: now, expiresAt: now + 60_000, writer });
    expect(readHostLeases(mesh).get("host:bad")?.writer).toBeUndefined();
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual({ source: "host-lease", name: "host:bad", reason: "incomplete or unsupported writer metadata" });
  });

  it("counts a process record with an empty host or non-positive start time as unknown", async () => {
    const mesh = root();
    const now = Date.now();
    const empty = deadPid();
    const emptyFile = writeRecord(mesh, { pid: empty, host: "", startedAt: 0 });
    // Even under an unexpired, valid lease for the same writer, start time 0 stays absent.
    const zero = deadPid();
    writeRecord(mesh, { pid: zero, host: "other-host.example", startedAt: 0 });
    writeHostLease(mesh, { id: "host:other", rootId: "session:other", identityId: "identity:other",
      updatedAt: now, expiresAt: now + 60_000, writer: { pid: zero, host: "other-host.example", releaseSha: "abc123",
        lockProtocol: 2, stateBackend: "sqlite", startedAt: now - 1000 } });
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    const unknown = result.unknown.filter(writer => writer.source === "process-record");
    expect(unknown).toHaveLength(2);
    const bad = unknown.find(writer => writer.pid === empty)!;
    expect(bad).not.toHaveProperty("host");
    expect(bad).not.toHaveProperty("startedAt");
    expect(unknown.find(writer => writer.pid === zero)).not.toHaveProperty("startedAt");
    expect(fs.existsSync(emptyFile)).toBe(true);
  });

  it("does not let a later process's lease (same host and pid, other start time) vouch for a stale record", async () => {
    const mesh = root();
    const now = Date.now();
    const pid = deadPid();
    const stale = { pid, host: "other-host.example", startedAt: now - 3_600_000 };
    writeRecord(mesh, stale);
    const writer = { ...stale, releaseSha: "abc123", lockProtocol: 2, stateBackend: "sqlite", startedAt: now - 1000 };
    writeHostLease(mesh, { id: "host:reused", rootId: "session:reused", identityId: "identity:reused",
      updatedAt: now, expiresAt: now + 60_000, writer });
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toEqual([expect.objectContaining({ ...stale, source: "process-record" })]);
    // The new process is listed in its own right, not merged into the stale record.
    expect(result.writers).toContainEqual(expect.objectContaining({ ...writer, source: "host-lease", name: "host:reused" }));
    expect(result.writers.filter(item => item.pid === pid)).toHaveLength(2);
  });

  it("merges this process's lease and record when both carry its exact start time", async () => {
    const mesh = root();
    new MeshStore(mesh, 4096, 100, { lockProtocol: 2, stateBackend: "file" });
    const now = Date.now();
    writeHostLease(mesh, { id: "host:self", rootId: "session:self", identityId: "identity:self",
      updatedAt: now, expiresAt: now + 60_000, writer: meshWriterLeaseRecord(2, "file", meshProcessStartedAt) });
    const result = await census(mesh);
    expect(result.writers.filter(item => item.pid === process.pid)).toEqual([
      expect.objectContaining({ host: os.hostname(), startedAt: meshProcessStartedAt, source: "process-record" })]);
  });

  it("opens a sqlite or shadow backend when the census record cannot be written (advisory, smarty-dev#6982)", async () => {
    const mesh = root();
    // A regular file where the record directory belongs: every attempt fails.
    fs.writeFileSync(path.join(mesh, ".writer-census"), "");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    for (const stateBackend of ["sqlite", "shadow"] as const) {
      mkdir.mockClear();
      const store = new MeshStore(mesh, 4096, 100, { stateBackend });
      try {
        expect(store.stateBackend).toBe(stateBackend);
        expect(mkdir.mock.calls.filter(([dir]) => String(dir).endsWith(".writer-census"))).toHaveLength(2);
        await store.put({ key: "census/advisory", value: stateBackend, identity: { id: "t", name: "t", kind: "agent" } });
        // The unrecorded writer shows up in the report instead.
        expect((await census(mesh)).unknown).not.toEqual([]);
      } finally { store.closeState(); }
    }
  });

  it("retries a failed census record write once and opens the sqlite backend", async () => {
    const mesh = root();
    const real = fs.renameSync;
    let failures = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to).includes(".writer-census") && failures++ === 0) throw Object.assign(new Error("ENOSPC"), { code: "ENOSPC" });
      return real(from, to);
    });
    const store = new MeshStore(mesh, 4096, 100, { stateBackend: "sqlite" });
    try {
      expect(store.stateBackend).toBe("sqlite");
      expect(failures).toBe(2);
      const result = await census(mesh);
      expect(result.writers).toContainEqual(expect.objectContaining({ pid: process.pid, stateBackend: "sqlite",
        source: "process-record" }));
      expect(fs.readdirSync(path.join(mesh, ".writer-census")).filter(name => name.endsWith(".tmp"))).toEqual([]);
    } finally {
      store.closeState();
    }
  });

  it("widens this process's record to every backend it opened, and keeps it until the last store closes", async () => {
    const mesh = root();
    const file = path.join(mesh, ".writer-census", censusRecordFileName(os.hostname(), process.pid, meshProcessStartedAt));
    const plain = new MeshStore(mesh, 4096, 100, { lockProtocol: 2, stateBackend: "file" });
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(expect.objectContaining({ lockProtocol: 2, stateBackend: "file" }));
    const sqlite = new MeshStore(mesh, 4096, 100, { lockProtocol: 1, stateBackend: "sqlite" });
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(expect.objectContaining({ pid: process.pid,
      stateBackend: "sqlite", stateBackends: ["file", "sqlite"], lockProtocol: 1, lockProtocols: [1, 2] }));
    const own = (await census(mesh)).writers.filter(writer => writer.pid === process.pid);
    // The cutover sees a sqlite writer (which takes no .lock), not the earlier file store.
    expect(own).toEqual([expect.objectContaining({ source: "process-record", stateBackend: "sqlite",
      stateBackends: ["file", "sqlite"], lockProtocol: 1 })]);
    // A third store of a weaker backend never narrows the record.
    const third = new MeshStore(mesh, 4096, 100, { lockProtocol: 2, stateBackend: "file" });
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(expect.objectContaining({ stateBackend: "sqlite", lockProtocol: 1 }));
    sqlite.closeState();
    sqlite.closeState();
    third.closeState();
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(expect.objectContaining({ stateBackend: "sqlite" }));
    plain.closeState();
    expect(fs.existsSync(file)).toBe(false);
  });

  it("keeps the record while a store opened through a symlink/junction alias of the root is still open", async () => {
    const mesh = root();
    const alias = path.join(root(), "alias");
    fs.symlinkSync(mesh, alias, process.platform === "win32" ? "junction" : "dir");
    const file = path.join(mesh, ".writer-census", censusRecordFileName(os.hostname(), process.pid, meshProcessStartedAt));
    const direct = new MeshStore(mesh, 4096, 100, { stateBackend: "file" });
    const aliased = new MeshStore(alias, 4096, 100, { stateBackend: "sqlite" });
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(expect.objectContaining({ stateBackend: "sqlite" }));
    direct.closeState();
    expect(fs.existsSync(file)).toBe(true);
    for (const view of [mesh, alias]) {
      expect((await census(view)).writers).toContainEqual(expect.objectContaining({ pid: process.pid,
        source: "process-record", stateBackend: "sqlite" }));
    }
    aliased.closeState();
    expect(fs.existsSync(file)).toBe(false);
    // And in the other order: the alias store closes first, the root store keeps the record.
    const viaAlias = new MeshStore(alias, 4096, 100, { stateBackend: "sqlite" });
    const viaRoot = new MeshStore(mesh, 4096, 100, { stateBackend: "file" });
    viaAlias.closeState();
    expect(fs.existsSync(file)).toBe(true);
    viaRoot.closeState();
    expect(fs.existsSync(file)).toBe(false);
  });

  it.each([
    { label: "corrupt", text: "{not json" },
    { label: "another start time", text: JSON.stringify({ format: 1, pid: process.pid, host: os.hostname(), releaseSha: "abc123",
      lockProtocol: 2, stateBackend: "file", startedAt: meshProcessStartedAt + 1 }) },
    { label: "an unsupported backend", text: JSON.stringify({ format: 1, pid: process.pid, host: os.hostname(), releaseSha: "abc123",
      lockProtocol: 2, stateBackend: "redis", startedAt: meshProcessStartedAt }) },
  ])("neither trusts nor overwrites an existing $label record, and still opens the store", ({ text }) => {
    const mesh = root();
    const directory = path.join(mesh, ".writer-census");
    fs.mkdirSync(directory);
    const file = path.join(directory, censusRecordFileName(os.hostname(), process.pid, meshProcessStartedAt));
    fs.writeFileSync(file, text);
    for (const stateBackend of ["sqlite", "shadow"] as const) {
      const store = new MeshStore(mesh, 4096, 100, { stateBackend });
      expect(store.stateBackend).toBe(stateBackend);
      store.closeState();
    }
    expect(new MeshStore(mesh, 4096, 100, { stateBackend: "file" }).stateBackend).toBe("file");
    expect(fs.readFileSync(file, "utf8")).toBe(text);
  });

  it("keeps a lock ticket whose pid belongs to another host's writer", async () => {
    const mesh = root();
    const pid = deadPid();
    writeRecord(mesh, { pid, host: "other-host.example", startedAt: Date.now() - 1000 });
    const queue = meshLockQueueDirectory(mesh);
    fs.mkdirSync(queue, { recursive: true });
    const ticket = `${"3".repeat(24)}-${pid}-01234567-89ab-cdef-0123-456789abcdef`;
    fs.writeFileSync(path.join(queue, ticket), "");
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ pid, host: "other-host.example",
      evidence: [`lock-ticket:${ticket}`] }));
  });

  it("treats a missing census directory on a root without SQLite state as an old release, and reports nothing", async () => {
    const result = await census(root());
    expect(result).toEqual({ writers: [], unknown: [] });
  });

  // smarty-dev#6982: the census is advisory. Every row is one way a review round (pi-fabric#638
  // rounds 3-9, and the #6982 audit) found hidden evidence; the report must list it as unknown.
  const identity = { id: "census-test", name: "census-test", kind: "agent" } as const;
  const sqliteStore = async (mesh: string, backend: "sqlite" | "shadow" = "sqlite"): Promise<MeshStore> => {
    const store = new MeshStore(mesh, 4096, 100, { stateBackend: backend });
    await store.put({ key: "census/probe", value: 1, identity });
    return store;
  };
  const ownRecord = (mesh: string): string =>
    path.join(mesh, ".writer-census", censusRecordFileName(os.hostname(), process.pid, meshProcessStartedAt));
  const sqliteResidue = (mesh: string, database = "state.db"): void => {
    fs.mkdirSync(path.dirname(path.join(mesh, database)), { recursive: true });
    for (const suffix of ["", "-wal", "-shm"]) fs.writeFileSync(path.join(mesh, database + suffix), "");
  };
  const incarnation = (): { bootId: string; startTicks: number } => {
    const stat = fs.readFileSync("/proc/self/stat", "utf8");
    return { bootId: fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim(),
      startTicks: Number(stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]) };
  };
  const ownWriter = (result: Awaited<ReturnType<typeof census>>) =>
    result.writers.filter(writer => writer.pid === process.pid && writer.source === "process-record");
  const proofCases: { label: string; reportsUnknown: boolean; linux?: boolean;
    setup: (mesh: string, close: Array<() => void>) => Promise<void> | void;
    check?: (result: Awaited<ReturnType<typeof census>>, mesh: string) => void }[] = [
    { label: "legacy root: no census, no SQLite state", reportsUnknown: false, setup: () => {} },
    { label: "a live sqlite writer with its record (the proof)", reportsUnknown: false,
      setup: async (mesh, close) => { const store = await sqliteStore(mesh); close.push(() => store.closeState()); },
      check: result => expect(ownWriter(result)).toEqual([expect.objectContaining({ stateBackend: "sqlite" })]) },
    { label: "round 3: a file writer that later opened sqlite reports sqlite", reportsUnknown: false,
      setup: async (mesh, close) => {
        const plain = new MeshStore(mesh, 4096, 100, { stateBackend: "file" });
        const store = await sqliteStore(mesh);
        close.push(() => plain.closeState(), () => store.closeState());
      },
      check: result => expect(ownWriter(result)).toEqual([expect.objectContaining({ stateBackend: "sqlite",
        stateBackends: ["file", "sqlite"] })]) },
    { label: "round 4: a sqlite writer through a symlink alias outlives the root's store", reportsUnknown: false,
      setup: async (mesh, close) => {
        const alias = path.join(root(), "alias");
        fs.symlinkSync(mesh, alias, process.platform === "win32" ? "junction" : "dir");
        const direct = new MeshStore(mesh, 4096, 100, { stateBackend: "file" });
        const aliased = await sqliteStore(alias);
        direct.closeState();
        close.push(() => aliased.closeState());
      },
      check: result => expect(ownWriter(result)).toEqual([expect.objectContaining({ stateBackend: "sqlite" })]) },
    { label: "round 5: an unreadable census directory", reportsUnknown: true,
      setup: mesh => { writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt() }); denyRead(path.join(mesh, ".writer-census")); } },
    { label: "round 5: an unreadable record", reportsUnknown: true,
      setup: mesh => denyRead(writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt() })) },
    { label: "round 6: a live sqlite writer's census directory removed", reportsUnknown: true,
      setup: async (mesh, close) => {
        const store = await sqliteStore(mesh);
        close.push(() => store.closeState());
        fs.rmSync(path.join(mesh, ".writer-census"), { recursive: true });
      },
      check: (result, mesh) => expect(result.unknown).toEqual(expect.arrayContaining([
        expect.objectContaining({ source: "process-record", reason: expect.stringMatching(/ENOENT on a root with state\.db/) }),
        expect.objectContaining({ source: "state-database", name: path.join(mesh, "state.db-wal") })])) },
    { label: "a live shadow writer's census directory removed", reportsUnknown: true,
      setup: async (mesh, close) => {
        const store = await sqliteStore(mesh, "shadow");
        close.push(() => store.closeState());
        fs.rmSync(path.join(mesh, ".writer-census"), { recursive: true });
      } },
    { label: "a root with state.db but no census at all", reportsUnknown: true, setup: mesh => { fs.writeFileSync(path.join(mesh, "state.db"), ""); } },
    { label: "a root with a shadow database but no census at all", reportsUnknown: true, setup: mesh => { fs.mkdirSync(path.join(mesh, "state-shadow")); } },
    { label: "a live sqlite writer's record deleted", reportsUnknown: true,
      setup: async (mesh, close) => {
        const store = await sqliteStore(mesh);
        close.push(() => store.closeState());
        fs.rmSync(ownRecord(mesh));
      } },
    { label: "a live sqlite writer's record renamed out of the .json namespace", reportsUnknown: true,
      setup: async (mesh, close) => {
        const store = await sqliteStore(mesh);
        close.push(() => store.closeState());
        fs.renameSync(ownRecord(mesh), `${ownRecord(mesh)}.bak`);
      } },
    { label: "a partial (torn) record", reportsUnknown: true,
      setup: mesh => { fs.mkdirSync(path.join(mesh, ".writer-census")); fs.writeFileSync(path.join(mesh, ".writer-census", "torn.json"), "{\"format\":1,\"pid\""); } },
    { label: "a live writer's record write still in flight", reportsUnknown: true,
      setup: mesh => { fs.mkdirSync(path.join(mesh, ".writer-census")); fs.writeFileSync(`${ownRecord(mesh)}.0123456789abcdef.tmp`, "{"); } },
    { label: "a dead writer's torn temporary (removed)", reportsUnknown: false,
      setup: mesh => {
        fs.mkdirSync(path.join(mesh, ".writer-census"));
        fs.writeFileSync(path.join(mesh, ".writer-census", `${censusRecordFileName(os.hostname(), deadPid(), 1)}.0123456789abcdef.tmp`), "{");
      },
      check: (_, mesh) => expect(fs.readdirSync(path.join(mesh, ".writer-census"))).toEqual([]) },
    { label: "clock skew: a live writer's wall-clock start an hour off is not proof of death", reportsUnknown: false,
      setup: mesh => { writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt() - 3_600_000,
        ...(process.platform === "linux" ? incarnation() : {}) }); },
      check: result => expect(ownWriter(result)).toHaveLength(1) },
    { label: "pid reuse: same boot, another start tick, is a dead record", reportsUnknown: false, linux: true,
      setup: mesh => { writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt(),
        ...incarnation(), startTicks: incarnation().startTicks + 1 }); },
      check: (result, mesh) => {
        expect(ownWriter(result)).toEqual([]);
        expect(fs.readdirSync(path.join(mesh, ".writer-census"))).toEqual([]);
      } },
    { label: "pid reuse: another boot is a dead record", reportsUnknown: false, linux: true,
      setup: mesh => { writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt(),
        ...incarnation(), bootId: "00000000-0000-0000-0000-000000000000" }); },
      check: result => expect(ownWriter(result)).toEqual([]) },
    { label: "SQLite connection files left by a crashed or unrecorded writer", reportsUnknown: true, setup: mesh => sqliteResidue(mesh) },
    { label: "shadow connection files without a shadow writer", reportsUnknown: true,
      setup: mesh => sqliteResidue(mesh, path.join("state-shadow", "state.db")) },
    { label: "mixed: a legacy sqlite writer's open database beside a recorded file writer", reportsUnknown: true,
      setup: mesh => { sqliteResidue(mesh); writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt(), stateBackend: "file" }); } },
    { label: "round 8: a pre-census remote writer's lock owner and ticket, pid not live here, no metadata", reportsUnknown: true,
      setup: mesh => {
        const pid = deadPid();
        const token = "01234567-89ab-cdef-0123-456789abcdef";
        fs.mkdirSync(path.join(mesh, ".lock"));
        fs.writeFileSync(path.join(mesh, ".lock", "owner"), `${token}\n${pid}\n${Date.now()}\n`);
        const queue = meshLockQueueDirectory(mesh);
        fs.mkdirSync(queue, { recursive: true });
        fs.writeFileSync(path.join(queue, `${"5".repeat(24)}-${pid}-${token}`), "");
      },
      check: result => {
        const [writer] = result.unknown;
        expect(result.unknown).toHaveLength(1);
        expect(writer).toEqual(expect.objectContaining({ source: "lock-owner", name: "01234567-89ab-cdef-0123-456789abcdef",
          reason: expect.stringMatching(/is not live on .* and no writer record or lease names it/) }));
        expect(writer?.evidence).toEqual(["lock-owner:01234567-89ab-cdef-0123-456789abcdef",
          `lock-ticket:${"5".repeat(24)}-${writer?.pid}-01234567-89ab-cdef-0123-456789abcdef`]);
      } },
    { label: "round 9: a lock owner whose pid matches a live recorded local writer is ambiguous", reportsUnknown: true,
      setup: mesh => {
        writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt(), stateBackend: "file" });
        fs.mkdirSync(path.join(mesh, ".lock"));
        fs.writeFileSync(path.join(mesh, ".lock", "owner"), `maybe-remote\n${process.pid}\n${Date.now()}\n`);
      },
      check: result => {
        // Reported against the local writer as a guess, and as unknown, never silently attributed.
        expect(ownWriter(result)).toEqual([expect.objectContaining({ evidence: ["lock-owner:maybe-remote"] })]);
        expect(result.unknown).toEqual([{ pid: process.pid, source: "lock-owner", name: "maybe-remote",
          reason: expect.stringMatching(/attributed by pid only; pid-only lock evidence cannot be attributed safely/) }]);
      } },
    { label: "mixed: a legacy live lock owner beside a recorded new writer", reportsUnknown: true,
      setup: mesh => {
        writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt(), stateBackend: "file" });
        fs.mkdirSync(path.join(mesh, ".lock"));
        fs.writeFileSync(path.join(mesh, ".lock", "owner"), `legacy\n${process.ppid}\n${Date.now()}\n`);
      } },
  ];

  it.each(proofCases)("report: $label -> unknown evidence $reportsUnknown", async ({ reportsUnknown, linux, setup, check }) => {
    if (linux && process.platform !== "linux") return;
    const mesh = root();
    const close: Array<() => void> = [];
    // A record without a known release is unknown: stores of this process name one.
    vi.stubEnv("PI_FABRIC_RELEASE_SHA", "build-sha");
    try {
      await setup(mesh, close);
      const result = await census(mesh);
      expect(result.unknown.length > 0, JSON.stringify(result.unknown)).toBe(reportsUnknown);
      for (const item of result.unknown) expect(item.reason, JSON.stringify(item)).toEqual(expect.any(String));
      expect(Object.keys(result).sort()).toEqual(["unknown", "writers"]);
      check?.(result, mesh);
    } finally {
      for (const done of close) done();
      vi.unstubAllEnvs();
    }
  });

  it("reports unknown evidence when the census directory exists but cannot be listed", async () => {
    const mesh = root();
    const directory = path.join(mesh, ".writer-census");
    writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt() });
    denyRead(directory);
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ source: "process-record", name: directory,
      reason: `${directory}: EACCES` }));
  });

  it("reports unknown evidence when a census record exists but cannot be read", async () => {
    const mesh = root();
    const file = writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt() });
    denyRead(file);
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ source: "process-record", name: path.basename(file),
      reason: `${file}: EACCES` }));
    expect(fs.existsSync(file)).toBe(true);
  });

  it("reports unknown evidence when the host-lease directory or a lease cannot be read, or a lease is invalid", async () => {
    const mesh = root();
    const now = Date.now();
    writeHostLease(mesh, { id: "host:denied", rootId: "session:denied", identityId: "identity:denied",
      updatedAt: now, expiresAt: now + 60_000 });
    const leases = path.join(mesh, "host-leases");
    const [leaseFile] = fs.readdirSync(leases).map(name => path.join(leases, name));
    denyRead(leaseFile!);
    fs.writeFileSync(path.join(leases, "torn.json"), "{");
    let result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toEqual(expect.arrayContaining([
      expect.objectContaining({ source: "host-lease", reason: `${leaseFile}: EACCES` }),
      expect.objectContaining({ source: "host-lease", reason: `${path.join(leases, "torn.json")}: invalid` })]));
    const other = root();
    fs.mkdirSync(path.join(other, "host-leases"));
    denyRead(path.join(other, "host-leases"));
    result = await census(other);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ source: "host-lease",
      reason: `${path.join(other, "host-leases")}: EACCES` }));
  });

  it("reports unknown evidence when the lock owner is unreadable or names no valid pid", async () => {
    const mesh = root();
    const owner = path.join(mesh, ".lock", "owner");
    fs.mkdirSync(path.dirname(owner));
    fs.writeFileSync(owner, "token\nnot-a-pid\n");
    let result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ source: "lock-owner", reason: `${owner}: invalid` }));
    fs.writeFileSync(owner, `token\n${deadPid()}\n${Date.now()}\n`);
    denyRead(owner);
    result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ source: "lock-owner", reason: `${owner}: EACCES` }));
  });

  it("reports unknown evidence when the lock queue exists but cannot be listed", async () => {
    const mesh = root();
    const queue = meshLockQueueDirectory(mesh);
    fs.mkdirSync(queue, { recursive: true });
    denyRead(queue);
    const result = await census(mesh);
    expect(result.unknown).not.toEqual([]);
    expect(result.unknown).toContainEqual(expect.objectContaining({ source: "lock-ticket", reason: `${queue}: EACCES` }));
  });

  it("is advisory: no exported gate and no verdict in the report (smarty-dev#6982)", async () => {
    expect(Object.keys(writerCensus).sort()).toEqual(["census"]);
    const mesh = root();
    fs.mkdirSync(path.join(mesh, ".lock"));
    fs.writeFileSync(path.join(mesh, ".lock", "owner"), `token\n${process.pid}\n${Date.now()}\n`);
    writeRecord(mesh, { pid: deadPid(), host: "other-host.example", startedAt: Date.now() - 1000 });
    for (const result of [await census(root()), await census(mesh)]) {
      expect(Object.keys(result).sort()).toEqual(["unknown", "writers"]);
      for (const key of ["clean", "safe", "ok", "isClean", "safeToCutover"]) expect(result).not.toHaveProperty(key);
      for (const value of Object.values(result)) expect(typeof value).not.toBe("boolean");
    }
  });

  it("changes no store behaviour on unknown evidence, and nothing in src consumes the census", async () => {
    const mesh = root();
    fs.mkdirSync(path.join(mesh, ".writer-census"));
    fs.writeFileSync(path.join(mesh, ".writer-census", "torn.json"), "{");
    writeRecord(mesh, { pid: deadPid(), host: "other-host.example", startedAt: Date.now() - 1000 });
    expect((await census(mesh)).unknown.length).toBeGreaterThan(0);
    for (const stateBackend of ["file", "sqlite", "shadow"] as const) {
      const store = new MeshStore(mesh, 4096, 100, { stateBackend });
      try {
        await store.put({ key: `census/${stateBackend}`, value: 1, identity });
        expect(store.get(`census/${stateBackend}`)?.value).toBe(1);
      } finally { store.closeState(); }
    }
    // No startup, CLI or migration path imports the census: it can only be called for a report.
    const importers = (dir: string): string[] => fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) return importers(file);
      return /\.[cm]?ts$/.test(entry.name) && file !== path.join("src", "mesh", "writer-census.ts") &&
        /writer-census\.js["']/.test(fs.readFileSync(file, "utf8")) ? [file] : [];
    });
    expect(importers("src")).toEqual([]);
  });
});
