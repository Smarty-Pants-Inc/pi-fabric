import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { census } from "../src/mesh/writer-census.js";
import { MeshStore } from "../src/mesh/store.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { writeHostLease } from "../src/topology/host-leases.js";

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
    const writer = { pid: process.pid, host: "test-host", releaseSha: "abc123", lockProtocol: 2,
      stateBackend: "sqlite", startedAt: Date.now() - 1000 };
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
});
