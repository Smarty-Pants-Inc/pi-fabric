import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { census } from "../src/mesh/writer-census.js";
import { MeshStore } from "../src/mesh/store.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { readHostLeases, writeHostLease } from "../src/topology/host-leases.js";

const ownStartedAt = (): number => Math.floor(Date.now() - process.uptime() * 1000);
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const writeRecord = (mesh: string, record: Record<string, unknown>): string => {
  const directory = path.join(mesh, ".writer-census");
  fs.mkdirSync(directory, { recursive: true });
  const file = path.join(directory, `${String(record.pid)}-${String(record.startedAt)}.json`);
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

const roots: string[] = [];
const root = (): string => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-writer-census-"));
  roots.push(value);
  return value;
};

afterEach(() => {
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
    const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, mesh],
      { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
    let stdout = "", stderr = "";
    child.stdout.on("data", chunk => { stdout += chunk; });
    child.stderr.on("data", chunk => { stderr += chunk; });
    const code = await new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
    expect(JSON.parse(stdout)).toEqual([expect.stringMatching(new RegExp(`^${child.pid}-\\d+\\.json$`))]);
    expect(fs.readdirSync(path.join(mesh, ".writer-census"))).toEqual([]);
  }, 30_000);

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
