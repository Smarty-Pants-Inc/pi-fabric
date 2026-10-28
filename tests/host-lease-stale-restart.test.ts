import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readPhysicalHostIdentity } from "../src/core/atomic-write.js";
import { diagnostics } from "../src/core/diagnostics.js";
import * as lockIdentity from "../src/core/lock-identity.js";
import { MeshStore } from "../src/mesh/store.js";
import { HostLeaseLockBusyError, withHostLeaseLock } from "../src/topology/host-lease-lock.js";

// Keep reason: data safety and the real pod-restart incident (smarty-dev#17082). Exercise the
// actual JSON receipt, filesystem rename/evidence and MeshStore per-host custody boundary.
// Linux-only: these cases use real procfs start ticks and a real PID-namespace inode, not a fixture.
const roots: string[] = [], stores: MeshStore[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  // Only roots created by this suite are removed, never a shared mesh or retained session traces.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-stale-restart-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend: "file" }); stores.push(mesh);
  await mesh.prepareLock();
  const identity = await lockIdentity.readLockIdentity();
  expect(await lockIdentity.completeLockIdentity(identity)).toBe(true);
  const file = path.join(root, "host-leases", "host.json");
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, "{}\n"); // Use the per-host gate, not the unrelated additions gate.
  const lock = path.join(root, "host-lease-locks", "host");
  const seed = (fields: Record<string, unknown> = {}, complete = true) => {
    fs.mkdirSync(lock, { recursive: true });
    const raw = JSON.stringify({ token: randomUUID(), host: os.hostname(), pid: process.pid,
      physical: readPhysicalHostIdentity(), ...(complete ? identity : {}), ...fields });
    fs.writeFileSync(path.join(lock, "owner"), raw);
    fs.writeFileSync(path.join(lock, "evidence"), "original holder evidence\n");
    const ancient = new Date("2020-01-02T03:04:05Z"); fs.utimesSync(lock, ancient, ancient);
    return { raw, stat: fs.lstatSync(lock) };
  };
  const asides = () => fs.existsSync(path.dirname(lock))
    ? fs.readdirSync(path.dirname(lock)).filter(name => name.startsWith("host.dead.") && !name.endsWith(".recovery.json"))
      .map(name => path.join(path.dirname(lock), name)).sort() : [];
  return { root, mesh, file, lock, identity, seed, asides };
};

type Fixture = Awaited<ReturnType<typeof fixture>>;
const expectEvidence = (f: Fixture, seed: ReturnType<Fixture["seed"]>, reason: string) => {
  const retained = f.asides(); expect(retained).toHaveLength(1);
  const aside = retained[0]!;
  expect(path.basename(aside)).toMatch(/^host\.dead\.2020-01-02T03-04-05-000Z\.[a-f0-9]{64}$/);
  expect(fs.readFileSync(path.join(aside, "owner"), "utf8")).toBe(seed.raw);
  expect(fs.readFileSync(path.join(aside, "evidence"), "utf8")).toBe("original holder evidence\n");
  const moved = fs.lstatSync(aside);
  expect({ dev: moved.dev, ino: moved.ino }).toEqual({ dev: seed.stat.dev, ino: seed.stat.ino });
  const record = JSON.parse(fs.readFileSync(`${aside}.recovery.json`, "utf8"));
  expect(record).toMatchObject({ format: 1, kind: "stale-lock-observation", source: f.lock, aside, reason,
    owner: seed.raw, dev: String(seed.stat.dev), ino: String(seed.stat.ino), recovererPid: process.pid });
  expect(Number.isFinite(Date.parse(record.observedAt))).toBe(true);
  expect(record.identity).toMatchObject({ pid: JSON.parse(seed.raw).pid });
  return aside;
};

const anotherBoot = (identity: lockIdentity.LockIdentity) => {
  const bootId = identity.bootId === "11111111-1111-1111-1111-111111111111"
    ? "22222222-2222-2222-2222-222222222222" : "11111111-1111-1111-1111-111111111111";
  const parts = identity.namespace!.split(":"); parts[2] = bootId;
  return { bootId, namespace: parts.join(":") };
};
const anotherNamespace = (identity: lockIdentity.LockIdentity) => {
  const parts = identity.namespace!.split(":"); parts[3] = String(BigInt(parts[3]!) + 1n);
  return { namespace: parts.join(":") };
};

describe.skipIf(process.platform !== "linux")("host-lease-locks restart recovery", () => {
  it("writes boot, PID namespace and native start into the existing JSON owner", async () => {
    const f = await fixture();
    await expect(withHostLeaseLock(f.mesh, f.file, () => {
      const owner = JSON.parse(fs.readFileSync(path.join(f.lock, "owner"), "utf8"));
      expect(owner).toMatchObject({ host: os.hostname(), ...f.identity });
      expect(owner.token).toEqual(expect.any(String));
      return "committed";
    })).resolves.toBe("committed");
    expect(fs.existsSync(f.lock)).toBe(false);
    expect(f.asides()).toEqual([]); // A normal release is not a stale-recovery observation.
  });

  it.each(["other-boot", "other-namespace", "dead-pid", "reused-pid"] as const)(
    "recovers a complete %s receipt without waiting for its lease and retains its original directory", async reason => {
      const f = await fixture();
      const fields = reason === "other-boot" ? anotherBoot(f.identity)
        : reason === "other-namespace" ? anotherNamespace(f.identity)
        : reason === "dead-pid" ? { pid: 999_999_999 }
        : { start: String(BigInt(f.identity.start!) + 1n) };
      if (reason === "dead-pid") expect(() => process.kill(999_999_999, 0)).toThrowError();
      const seed = f.seed({ ...fields, incarnationToken: "unexpired-old-incarnation", expiresAt: Date.now() + 60_000 });
      const operation = vi.fn(() => "recovered");
      await expect(withHostLeaseLock(f.mesh, f.file, operation, { timeoutMs: 2_000 })).resolves.toBe("recovered");
      expect(operation).toHaveBeenCalledOnce();
      expect(fs.existsSync(f.lock)).toBe(false);
      expectEvidence(f, seed, reason);
    });

  it.each([false, true])("never steals a live same-start holder, even with an expired %s foreign lease", async foreign => {
    const f = await fixture();
    const seed = f.seed({ incarnationToken: "expired", expiresAt: 1,
      ...(foreign ? { host: "previous-pod-name", physical: { machineId: "foreign", bootId: f.identity.bootId } } : {}) });
    const operation = vi.fn();
    await expect(withHostLeaseLock(f.mesh, f.file, operation, { timeoutMs: 40 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.lock, "owner"), "utf8")).toBe(seed.raw);
    expect(f.asides()).toEqual([]);
  });

  it("does not use an expired lease when a complete identity verdict is unknown", async () => {
    const f = await fixture();
    const seed = f.seed({ host: "previous-pod", physical: undefined, incarnationToken: "expired", expiresAt: 1 });
    const kill = process.kill.bind(process);
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === f.identity.pid) throw Object.assign(new Error("permission denied"), { code: "EPERM" });
      return kill(pid, signal);
    });
    const operation = vi.fn();
    await expect(withHostLeaseLock(f.mesh, f.file, operation, { timeoutMs: 40 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.lock, "owner"), "utf8")).toBe(seed.raw);
    expect(f.asides()).toEqual([]);
  });

  it("keeps a zero-budget publication attempt non-recovering", async () => {
    const f = await fixture(); const seed = f.seed(anotherBoot(f.identity)); const operation = vi.fn();
    await expect(withHostLeaseLock(f.mesh, f.file, operation, { timeoutMs: 0 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.lock, "owner"), "utf8")).toBe(seed.raw);
    expect(f.asides()).toEqual([]);
  });

  it("holds real MeshStore custody throughout an awaited recovery verdict and excludes a concurrent contender", async () => {
    const f = await fixture(); const seed = f.seed(anotherNamespace(f.identity));
    const peer = new MeshStore(f.root, 65_536, 100, { stateBackend: "file" }); stores.push(peer); await peer.prepareLock();
    let enter!: () => void, resume!: () => void;
    const entered = new Promise<void>(resolve => { enter = resolve; });
    const paused = new Promise<void>(resolve => { resume = resolve; });
    const realVerdict = lockIdentity.lockIdentityVerdict;
    vi.spyOn(lockIdentity, "lockIdentityVerdict").mockImplementation(async (...args) => {
      const verdict = await realVerdict(...args);
      if (args[0].namespace === JSON.parse(seed.raw).namespace) { enter(); await paused; }
      return verdict;
    });
    const firstOperation = vi.fn(() => "first"), secondOperation = vi.fn(() => "second");
    const first = withHostLeaseLock(f.mesh, f.file, firstOperation, { timeoutMs: 2_000 })
      .then(value => ({ value }), (error: unknown) => ({ error }));
    try {
      await Promise.race([entered, first.then(() => { throw new Error("first contender settled before identity verdict"); })]);
      expect(fs.readFileSync(path.join(f.lock, "owner"), "utf8")).toBe(seed.raw);
      const unrelated = vi.fn();
      await expect(peer.leaseCustody(f.file, unrelated, 0)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(unrelated).not.toHaveBeenCalled();
      await expect(withHostLeaseLock(peer, f.file, secondOperation, { timeoutMs: 1_000 }))
        .rejects.toBeInstanceOf(HostLeaseLockBusyError);
      expect(secondOperation).not.toHaveBeenCalled(); expect(firstOperation).not.toHaveBeenCalled();
      resume(); expect(await first).toEqual({ value: "first" });
      expect(firstOperation).toHaveBeenCalledOnce();
      expectEvidence(f, seed, "other-namespace");
    } finally { resume(); await first; }
  });

  it("keeps earlier aside bytes and observations intact across a second stale recovery", async () => {
    const f = await fixture(); const first = f.seed(anotherBoot(f.identity));
    await withHostLeaseLock(f.mesh, f.file, () => undefined, { timeoutMs: 2_000 });
    const originalAside = expectEvidence(f, first, "other-boot");
    const originalObservation = fs.readFileSync(`${originalAside}.recovery.json`, "utf8");
    const second = f.seed(anotherNamespace(f.identity));
    await withHostLeaseLock(f.mesh, f.file, () => undefined, { timeoutMs: 2_000 });
    expect(f.asides()).toHaveLength(2);
    expect(fs.readFileSync(path.join(originalAside, "owner"), "utf8")).toBe(first.raw);
    expect(fs.readFileSync(`${originalAside}.recovery.json`, "utf8")).toBe(originalObservation);
    const newAside = f.asides().find(aside => aside !== originalAside)!;
    expect(fs.readFileSync(path.join(newAside, "owner"), "utf8")).toBe(second.raw);
    expect(JSON.parse(fs.readFileSync(`${newAside}.recovery.json`, "utf8")).reason).toBe("other-namespace");
  });

  it.each([
    { name: "local live", fields: { expiresAt: 1, incarnationToken: "expired" }, recovered: false },
    { name: "foreign unexpired dead", fields: { host: "foreign", physical: undefined, pid: 999_999_999,
      expiresAt: Number.MAX_SAFE_INTEGER, incarnationToken: "unexpired" }, recovered: false },
    { name: "foreign expired", fields: { host: "foreign", physical: undefined, expiresAt: 1,
      incarnationToken: "expired" }, recovered: true },
    { name: "local dead", fields: { pid: 999_999_999 }, recovered: true },
  ])("preserves legacy $name recovery policy and emits a warning", async ({ fields, recovered }) => {
    const f = await fixture(); const seed = f.seed(fields, false); const warning = vi.spyOn(diagnostics, "warn");
    const operation = vi.fn(() => "legacy-policy");
    const attempt = withHostLeaseLock(f.mesh, f.file, operation, { timeoutMs: 100 });
    if (recovered) {
      await expect(attempt).resolves.toBe("legacy-policy"); expect(operation).toHaveBeenCalledOnce();
      const aside = f.asides()[0]!; expect(f.asides()).toHaveLength(1);
      expect(fs.readFileSync(path.join(aside, "owner"), "utf8")).toBe(seed.raw);
      expect(JSON.parse(fs.readFileSync(`${aside}.recovery.json`, "utf8"))).toMatchObject({ owner: seed.raw, source: f.lock });
    } else {
      await expect(attempt).rejects.toBeInstanceOf(HostLeaseLockBusyError); expect(operation).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(f.lock, "owner"), "utf8")).toBe(seed.raw); expect(f.asides()).toEqual([]);
    }
    expect(warning.mock.calls.some(([, message]) => String(message).includes("legacy lock owner") && String(message).includes(f.lock))).toBe(true);
  });

  it.each(["start", "bootId", "namespace"])("keeps the legacy lease rule when JSON identity is missing %s", async missing => {
    const f = await fixture();
    const seed = f.seed({ [missing]: undefined, host: "foreign", physical: undefined,
      incarnationToken: "expired", expiresAt: 1 });
    const warning = vi.spyOn(diagnostics, "warn");
    await expect(withHostLeaseLock(f.mesh, f.file, () => "partial", { timeoutMs: 100 })).resolves.toBe("partial");
    const aside = expectEvidence(f, seed, "unknown");
    expect(fs.readFileSync(path.join(aside, "owner"), "utf8")).toBe(seed.raw);
    expect(warning.mock.calls.some(([, message]) => String(message).includes(f.lock))).toBe(true);
  });

  it("also warns on a legacy receipt during a zero-budget refusal", async () => {
    const f = await fixture(); f.seed({}, false); const warning = vi.spyOn(diagnostics, "warn");
    await expect(withHostLeaseLock(f.mesh, f.file, () => undefined)).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(warning.mock.calls.some(([, message]) => String(message).includes(f.lock))).toBe(true);
  });
});
