import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
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

afterEach(() => {
  vi.restoreAllMocks();
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
    expect(result.clean).toBe(true);
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
    expect(result.clean).toBe(true);
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
    expect(result.clean).toBe(false);
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
    expect(result.clean).toBe(false);
    expect(result.unknown).toContainEqual(expect.objectContaining({ pid: process.pid, source: "lock-ticket", name }));
  });
  it("deletes this host's record of a dead process", async () => {
    const mesh = root();
    const pid = deadPid();
    const file = writeRecord(mesh, { pid, host: os.hostname(), startedAt: ownStartedAt() });
    const result = await census(mesh);
    expect(result.writers.find(writer => writer.pid === pid)).toBeUndefined();
    expect(result.clean).toBe(true);
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
    expect(result.clean).toBe(false);
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
    expect(result.clean).toBe(false);
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
    { label: "an unknown release", fields: { releaseSha: "unknown" }, absent: undefined },
    { label: "an empty release", fields: { releaseSha: "" }, absent: "releaseSha" },
  ])("counts a live writer with $label unknown", async ({ fields, absent }) => {
    const mesh = root();
    const file = writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt(), ...fields });
    const result = await census(mesh);
    expect(fs.existsSync(file)).toBe(true);
    expect(result.clean).toBe(false);
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
    expect(result.unknown).toContainEqual({ source: "host-lease", name: "host:unsupported" });
  });

  it("counts an unparsable or pid-less record unknown and keeps it", async () => {
    const mesh = root();
    const directory = path.join(mesh, ".writer-census");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "torn.json"), "{");
    fs.writeFileSync(path.join(directory, "pidless.json"), JSON.stringify({ format: 1, host: os.hostname(), startedAt: ownStartedAt() }));
    const result = await census(mesh);
    expect(result.unknown).toEqual(expect.arrayContaining([{ source: "process-record", name: "torn.json" },
      { source: "process-record", name: "pidless.json" }]));
    expect(fs.readdirSync(directory).sort()).toEqual(["pidless.json", "torn.json"]);
  });

  it.runIf(process.platform === "linux")("treats a live pid with another start time as a reused, dead record", async () => {
    const mesh = root();
    const file = writeRecord(mesh, { pid: process.pid, host: os.hostname(), startedAt: ownStartedAt() - 3_600_000 });
    const result = await census(mesh);
    expect(result.writers.find(writer => writer.source === "process-record")).toBeUndefined();
    expect(fs.existsSync(file)).toBe(false);
  });

  it("never drops another host's record, and counts it unknown without its unexpired lease", async () => {
    const mesh = root();
    const pid = deadPid();
    const record = { pid, host: "other-host.example", startedAt: Date.now() - 1000 };
    const file = writeRecord(mesh, record);
    const first = await census(mesh);
    expect(first.clean).toBe(false);
    expect(first.writers).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));
    expect(first.unknown).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));
    expect(fs.existsSync(file)).toBe(true);

    const now = Date.now();
    const writer = { ...record, releaseSha: "abc123", lockProtocol: 2, stateBackend: "sqlite" };
    writeHostLease(mesh, { id: "host:other", rootId: "session:other", identityId: "identity:other",
      updatedAt: now, expiresAt: now + 60_000, writer });
    const leased = await census(mesh);
    expect(leased.clean).toBe(true);
    expect(leased.writers).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));

    writeHostLease(mesh, { id: "host:other", rootId: "session:other", identityId: "identity:other",
      updatedAt: now - 120_000, expiresAt: now - 60_000, writer });
    const expired = await census(mesh);
    expect(expired.clean).toBe(false);
    expect(expired.unknown).toContainEqual(expect.objectContaining({ ...record, source: "process-record" }));
  });

  it.each([
    { label: "an empty host and start time 0", host: "", startedAt: 0 },
    { label: "an empty host", host: "", startedAt: Date.now() - 1000 },
    { label: "start time 0", host: "lease-host", startedAt: 0 },
    { label: "a negative start time", host: "lease-host", startedAt: -1 },
  ])("drops lease writer metadata with $label, so census is not clean", async ({ host, startedAt }) => {
    const mesh = root();
    const now = Date.now();
    const writer = { pid: process.pid, host, releaseSha: "release-456", lockProtocol: 2, stateBackend: "sqlite", startedAt };
    writeHostLease(mesh, { id: "host:bad", rootId: "session:bad", identityId: "identity:bad",
      updatedAt: now, expiresAt: now + 60_000, writer });
    expect(readHostLeases(mesh).get("host:bad")?.writer).toBeUndefined();
    const result = await census(mesh);
    expect(result.clean).toBe(false);
    expect(result.unknown).toContainEqual({ source: "host-lease", name: "host:bad" });
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
    expect(result.clean).toBe(false);
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
    expect(result.clean).toBe(false);
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

  it("refuses a sqlite or shadow backend when the census record cannot be written, after one retry", () => {
    const mesh = root();
    // A regular file where the record directory belongs: every attempt fails.
    fs.writeFileSync(path.join(mesh, ".writer-census"), "");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    for (const stateBackend of ["sqlite", "shadow"] as const) {
      mkdir.mockClear();
      expect(() => new MeshStore(mesh, 4096, 100, { stateBackend }))
        .toThrow(new RegExp(`cannot record this process in the writer census.*refusing to open the ${stateBackend} state backend`));
      expect(mkdir.mock.calls.filter(([dir]) => String(dir).endsWith(".writer-census"))).toHaveLength(2);
    }
    // File mode stays available: its writes take .lock, which the census counts.
    expect(new MeshStore(mesh, 4096, 100, { stateBackend: "file" }).stateBackend).toBe("file");
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

  it("keeps a lock ticket whose pid belongs to another host's writer", async () => {
    const mesh = root();
    const pid = deadPid();
    writeRecord(mesh, { pid, host: "other-host.example", startedAt: Date.now() - 1000 });
    const queue = meshLockQueueDirectory(mesh);
    fs.mkdirSync(queue, { recursive: true });
    const ticket = `${"3".repeat(24)}-${pid}-01234567-89ab-cdef-0123-456789abcdef`;
    fs.writeFileSync(path.join(queue, ticket), "");
    const result = await census(mesh);
    expect(result.clean).toBe(false);
    expect(result.unknown).toContainEqual(expect.objectContaining({ pid, host: "other-host.example",
      evidence: [`lock-ticket:${ticket}`] }));
  });
});
