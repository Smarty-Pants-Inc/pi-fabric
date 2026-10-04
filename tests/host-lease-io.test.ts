import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { hostLeaseRenewalInterval, readHostLease, removeHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { decideHandover, writeHandoverImmutable } from "../src/residency/handover.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
const root = () => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lease-io-"));
  roots.push(value);
  return value;
};
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true });
});
const directory = (meshRoot: string, id = "host:a", options: { heartbeatMs?: number; leaseMs?: number } = {}) => {
  const value = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
    enabled: true, hostId: "host:a", rootId: id, identity: { id, name: "Main", kind: "main" },
    reapDeadHosts: false, ...options,
  });
  directories.push(value);
  return value;
};

describe("soft host-lease I/O", () => {
  it("never fsyncs initial publication, renewals, or a replacement identity", () => {
    const meshRoot = root();
    const sync = vi.spyOn(fs, "fsyncSync");
    const rename = vi.spyOn(fs, "renameSync");
    const initial = { id: "host:a", rootId: "root:a", identityId: "identity:a", updatedAt: 1_000, expiresAt: 16_000 };
    writeHostLease(meshRoot, initial);
    writeHostLease(meshRoot, { ...initial, updatedAt: 6_000, expiresAt: 21_000 });
    const replacement = { ...initial, rootId: "root:b", identityId: "identity:b", updatedAt: 22_000, expiresAt: 37_000 };
    writeHostLease(meshRoot, replacement);
    expect(sync).not.toHaveBeenCalled();
    expect(rename).toHaveBeenCalledTimes(3);
    expect(readHostLease(meshRoot, initial.id)).toEqual(replacement);
    const dir = path.join(meshRoot, "host-leases");
    expect(fs.readdirSync(dir)).toHaveLength(1);
    expect(rename.mock.calls.every(([from, to]) => String(from).endsWith(".tmp") && String(to).endsWith(".json"))).toBe(true);
  });

  it("the real directory publication path stays barrier-free and re-acquires a lost/lapsed lease", async () => {
    const meshRoot = root();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const sync = vi.spyOn(fs, "fsyncSync");
    const owner = directory(meshRoot);
    owner.registerSource(() => [{
      format: 1, id: "host:a", kind: "root", rootId: "host:a",
      ownerHostId: "host:a", ownerIdentityId: "host:a", name: "Main", status: "idle",
      runner: "pi", transport: "host", capabilities: ["fabric"], cwd: meshRoot,
      startedAt: now, updatedAt: now, pendingMessages: false, controlProtocol: "v1",
    }]);
    await owner.start();
    const first = readHostLease(meshRoot, "host:a")!;
    expect(first.expiresAt - first.updatedAt).toBe(15_000);
    expect(owner.list({ scope: "project", fresh: true }).map(record => record.id)).toContain("host:a");
    now = first.expiresAt + 1;
    expect(owner.list({ scope: "project", fresh: true }).map(record => record.id)).not.toContain("host:a");
    expect(readHostLease(meshRoot, "host:a")!.expiresAt).toBeLessThan(now);
    removeHostLease(meshRoot, "host:a"); // Simulate a crash-lost soft-state publication.
    await owner.refresh();
    expect(readHostLease(meshRoot, "host:a")!.expiresAt).toBe(now + 15_000);
    expect(owner.list({ scope: "project", fresh: true }).map(record => record.id)).toContain("host:a");
    // A new incarnation can publish liveness; this is not a durable custody grant.
    const replacement = directory(meshRoot, "host:b");
    await replacement.start();
    expect(readHostLease(meshRoot, "host:a")!.identityId).toBe("host:b");
    expect(sync).not.toHaveBeenCalled();
  });

  it("wires stable per-host jitter into the timer without changing default TTL", async () => {
    const timers = vi.spyOn(globalThis, "setInterval");
    const meshRoot = root();
    await directory(meshRoot).start();
    expect(timers.mock.calls.some(([, ms]) => ms === hostLeaseRenewalInterval(5_000, "host:a"))).toBe(true);
    expect(readHostLease(meshRoot, "host:a")!.expiresAt - readHostLease(meshRoot, "host:a")!.updatedAt).toBe(15_000);
  });

  it("keeps jitter within ±20% and spreads a fleet of 128 host identities", () => {
    const values = Array.from({ length: 128 }, (_, index) => hostLeaseRenewalInterval(30_000, `host:${index}`));
    for (const [index, value] of values.entries()) {
      expect(value).toBeGreaterThanOrEqual(24_000);
      expect(value).toBeLessThanOrEqual(36_000);
      expect(value).toBe(hostLeaseRenewalInterval(30_000, `host:${index}`));
    }
    expect(new Set(values).size).toBe(128);
    expect(Math.min(...values)).toBeLessThan(27_000);
    expect(Math.max(...values)).toBeGreaterThan(33_000);
  });

  it("raises undersized configured TTL to three nominal renewal intervals", async () => {
    const meshRoot = root();
    await directory(meshRoot, "host:a", { heartbeatMs: 30_000, leaseMs: 1_000 }).start();
    const lease = readHostLease(meshRoot, "host:a")!;
    expect(lease.expiresAt - lease.updatedAt).toBe(90_000);
  });

  it("retains fsync for immutable acquisition snapshots and takeover custody decisions", () => {
    const meshRoot = root();
    const sync = vi.spyOn(fs, "fsyncSync");
    writeHandoverImmutable(path.join(meshRoot, "launch.json"), { generation: "initial" });
    expect(sync).toHaveBeenCalled();
    sync.mockClear();
    decideHandover(meshRoot, { id: "takeover", state: "custody" });
    expect(sync).toHaveBeenCalled();
  });
});
