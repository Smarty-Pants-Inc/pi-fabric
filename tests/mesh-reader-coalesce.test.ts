import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const roots: string[] = [];
const identity = { id: "session:writer", name: "writer", kind: "main" as const };
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-reader-coalesce-"));
  roots.push(root);
  const file = path.join(root, "state.json");
  let active = false;
  const reader = new MeshStore(root, 64 * 1024, 100, {
    readCacheMs: RUNTIME_MESH_READ_CACHE_MS, readActive: () => active,
  });
  const replace = (value: unknown, generation: string | null = randomUUID()) => {
    const key = "status";
    const state = { ...(generation ? { readGeneration: generation } : {}), format: 1, entries: {
      [key]: { key, version: 1, updatedAt: Date.now(), updatedBy: identity, value },
    } };
    fs.writeFileSync(`${file}.tmp`, JSON.stringify(state));
    fs.renameSync(`${file}.tmp`, file);
  };
  const reads = vi.spyOn(fs, "readFileSync");
  const count = () => reads.mock.calls.filter(([target]) => String(target) === file).length;
  return { root, file, reader, replace, count, setActive: (value: boolean) => { active = value; } };
};

describe("idle reader coalescing (smarty-dev#4383)", () => {
  it.each(["minted", "legacy", "copied"])("coalesces 3.6 Hz %s replacements without sliding the 5 s bound", format => {
    vi.useFakeTimers({ now: 1_000_000 });
    const s = setup();
    const marker = randomUUID();
    const write = (n: number) => s.replace(n, format === "legacy" ? null : format === "copied" ? marker : randomUUID());
    write(0);
    expect(s.reader.get("status")?.value).toBe(0);
    const initial = s.count();
    for (let tick = 1; tick <= 216; tick++) {
      vi.setSystemTime(1_000_000 + Math.round(tick * 1000 / 3.6));
      write(tick);
      // Same observers used by lifecycle polls, topology and UI; stat changes cannot override TTL.
      s.reader.stateStamp(); s.reader.cachedStateStamp(true);
      s.reader.stateToken(); s.reader.listAll("status"); s.reader.listAllShared("status");
    }
    expect(s.count() - initial).toBeLessThanOrEqual(12); // <= once/5 s, vs 216 changed files.
    expect(s.reader.get("status")?.value).toBe(216);
  });

  it("sees an idle change at the exact window boundary despite repeated cache hits", () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const s = setup(); s.replace("before");
    expect(s.reader.get("status")?.value).toBe("before");
    vi.advanceTimersByTime(1); s.replace("after");
    const count = s.count();
    for (let elapsed = 100; elapsed < 5_000; elapsed += 100) {
      vi.setSystemTime(1_000_000 + elapsed);
      s.reader.cachedStateStamp(true);
      expect(s.reader.get("status")?.value).toBe("before");
    }
    expect(s.count()).toBe(count);
    vi.setSystemTime(1_005_000);
    expect(s.reader.get("status")?.value).toBe("after");
    expect(s.count()).toBe(count + 1);
  });

  it("bypasses coalescing on active/pending-operation demand and resumes idle reuse", () => {
    const s = setup(); s.replace("before"); s.reader.get("status");
    s.replace("turn"); s.setActive(true);
    expect(s.reader.readCacheMs).toBe(0);
    expect(s.reader.get("status")?.value).toBe("turn");
    s.replace("pending");
    expect(s.reader.listAll("status")[0]?.value).toBe("pending");
    s.setActive(false);
    expect(s.reader.readCacheMs).toBe(5_000);
    const count = s.count(); s.replace("idle");
    expect(s.reader.get("status")?.value).toBe("pending");
    expect(s.count()).toBe(count);
  });

  it("never coalesces fresh ownership/delivery snapshots, including legacy copied generations", () => {
    const s = setup(); const marker = randomUUID(); s.replace("old-owner", marker);
    s.reader.get("status"); s.replace("new-owner", marker);
    expect(s.reader.get("status")?.value).toBe("old-owner");
    expect(s.reader.get("status", { fresh: true })?.value).toBe("new-owner");
    s.replace("revoked", marker);
    expect(s.reader.listAll("status", { fresh: true })[0]?.value).toBe("revoked");
    s.replace("latest", marker);
    const snapshot = s.reader.stateToken({ fresh: true });
    expect(s.reader.listAllShared("status", { snapshot })[0]?.value).toBe("latest");
  });

  it("CAS uses canonical versions under the write lock while the idle view is stale", async () => {
    const s = setup(); const writer = new MeshStore(s.root, 64 * 1024, 100);
    await writer.put({ key: "status", value: "before", identity });
    const old = s.reader.get("status")!;
    await writer.put({ key: "status", value: "after", identity });
    expect(s.reader.get("status")).toEqual(old);
    await expect(s.reader.put({ key: "status", value: "incorrect", identity, ifVersion: old.version }))
      .rejects.toThrow("compare-and-swap");
    expect(s.reader.get("status")?.value).toBe("after");
  });

  it("participant ownership reads see a revoked host while idle views are cached", async () => {
    const s = setup(); const writer = new MeshStore(s.root, 64 * 1024, 100);
    const directory = new ParticipantDirectory(s.reader, {
      enabled: true, hostId: "observer", rootId: "observer", identity, heartbeatMs: 60_000, leaseMs: 120_000,
    });
    const hash = (id: string) => createHash("sha256").update(id).digest("hex");
    const hostKey = `topology/hosts/${hash("h")}`;
    await writer.writeBatch({ identity, ops: [
      { kind: "put", key: hostKey, value: { format: 1, id: "h", rootId: "peer", identity,
        pid: process.pid, cwd: s.root, startedAt: Date.now(), updatedAt: Date.now(), expiresAt: Date.now() + 120_000 } },
      { kind: "put", key: `topology/participants/${hash("peer")}`, value: {
        format: 1, id: "peer", kind: "root", rootId: "peer", ownerHostId: "h", ownerIdentityId: identity.id,
        name: "peer", status: "idle", runner: "pi", transport: "host", capabilities: [], controlProtocol: "v1",
        startedAt: Date.now(), updatedAt: Date.now(),
      } },
    ] });
    expect(directory.get("peer")?.ownerHostId).toBe("h"); // A real live participant, not an invalid row.
    await writer.delete({ key: hostKey });
    expect(directory.get("peer")?.ownerHostId).toBe("h"); // Idle observation still has the old snapshot.
    const get = vi.spyOn(s.reader, "get");
    expect(directory.get("peer", undefined, { fresh: true })).toBeUndefined();
    expect(get).toHaveBeenCalledWith(hostKey, { fresh: true }); // Ownership/delivery port reads canonical.
    expect(s.reader.get(hostKey)).toBeUndefined();
  });
});
