import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import {
  currentOwnerIdentity,
  localityLockToken,
  lockTokenLocality,
  ownerLocalityTag,
  readPhysicalHostIdentity,
  SHORT_LOCK_MAX_HOLD_MS,
  unknownNamespaceLockLiveness,
} from "../src/core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { MeshStore } from "../src/mesh/store.js";

// PR #864 review P1: a lock owner in another PID namespace or host is invisible to the local
// kill(pid, 0) probe. ESRCH there is not death; such owners stay protected (unknown), while
// confirmed local death, PID reuse and an earlier boot of this host still recover.
const roots: string[] = [];
const temp = (prefix: string): string => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(value);
  return value;
};
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true });
});

const self = currentOwnerIdentity();
const uuid = "00000000-0000-4000-8000-000000000000";
const tagged = (place: Partial<typeof self> = {}): string => `${uuid}-${ownerLocalityTag({ ...self, ...place })}`;
const foreignNamespace = (): string => tagged({ pidNamespace: "pid:[4026599999]" });
const foreignHost = (): string => tagged({ hostname: `${self.hostname}-elsewhere` });
// PR #864 P1: same host and boot, but the writer's PID namespace is unknown.
const unknownNamespace = (): string =>
  `${uuid}-${ownerLocalityTag({ hostname: self.hostname, ...(self.bootId === undefined ? {} : { bootId: self.bootId }) })}`;
const previousBoot = (): string => tagged({ bootId: "11111111-2222-4333-8444-555555555555" });
const deadPid = (): number => spawnSync(process.execPath, ["-e", ""]).pid!;
const esrch = (): void => {
  vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("not visible here"), { code: "ESRCH" }); });
};

describe("lock token locality", () => {
  it("classifies local, foreign, previous-boot and untagged tokens", () => {
    expect(lockTokenLocality(localityLockToken())).toBe("local");
    expect(localityLockToken()).toMatch(/^[0-9a-f-]{36}-[0-9a-f-]{18}$/);
    // Mesh lock-queue ticket names accept only this alphabet.
    expect(localityLockToken()).toMatch(/^[a-f0-9-]+$/);
    expect(lockTokenLocality(foreignNamespace())).toBe("foreign");
    expect(lockTokenLocality(foreignHost())).toBe("foreign");
    expect(lockTokenLocality(uuid)).toBe("untagged");
    expect(lockTokenLocality("legacy")).toBe("untagged");
    expect(lockTokenLocality(undefined)).toBe("untagged");
    const withBoot = { hostname: "node-a", pidNamespace: "pid:[1]", bootId: "boot-a" };
    const own = ownerLocalityTag(withBoot);
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag({ ...withBoot, bootId: "boot-b" })}`, own)).toBe("previous-boot");
    // Another namespace from an earlier boot of this host is still dead (no process outlives its boot).
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag({ ...withBoot, bootId: "boot-b", pidNamespace: "pid:[2]" })}`, own)).toBe("previous-boot");
    // Same boot, another namespace: unknown. An unknown boot on either side never proves an earlier boot.
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag({ ...withBoot, pidNamespace: "pid:[2]" })}`, own)).toBe("foreign");
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag({ hostname: "node-a", pidNamespace: "pid:[1]" })}`, own)).toBe("foreign");
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag({ ...withBoot, hostname: "node-b", bootId: "boot-b" })}`, own)).toBe("foreign");
    // PR #864 P1: an unknown namespace on either side is never local; an earlier boot still is dead.
    const noNamespace = { hostname: "node-a", bootId: "boot-a" };
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag(noNamespace)}`, ownerLocalityTag(noNamespace))).toBe("foreign");
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag(noNamespace)}`, own)).toBe("foreign");
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag(withBoot)}`, ownerLocalityTag(noNamespace))).toBe("foreign");
    expect(lockTokenLocality(`${uuid}-${ownerLocalityTag({ ...noNamespace, bootId: "boot-b" })}`, ownerLocalityTag(noNamespace))).toBe("previous-boot");
  });
});

describe("unknown-namespace lock heartbeat (PR #864 P1)", () => {
  it("judges a same-host unknown-namespace token by its creation time and the short-lock hold ceiling", () => {
    const withBoot = { hostname: "node-a", pidNamespace: "pid:[1]", bootId: "boot-a" };
    const noNamespace = { hostname: "node-a", bootId: "boot-a" };
    const now = 1_700_000_000_000;
    const at = (created: number, owner: object, reader: object, staleMs?: number) =>
      unknownNamespaceLockLiveness(`${uuid}-${ownerLocalityTag(owner as typeof withBoot)}`, created,
        { now, ...(staleMs === undefined ? {} : { staleMs }) }, ownerLocalityTag(reader as typeof withBoot));
    expect(at(now - SHORT_LOCK_MAX_HOLD_MS - 1, noNamespace, withBoot)).toBe("dead");
    expect(at(now - SHORT_LOCK_MAX_HOLD_MS - 1, withBoot, noNamespace)).toBe("dead");
    expect(at(now - SHORT_LOCK_MAX_HOLD_MS - 1, noNamespace, noNamespace)).toBe("dead");
    expect(at(now - SHORT_LOCK_MAX_HOLD_MS, noNamespace, withBoot)).toBe("alive");
    expect(at(now - 60_000, noNamespace, withBoot)).toBe("alive");
    // A caller's longer stale bound only extends the ceiling, never shortens it.
    expect(at(now - SHORT_LOCK_MAX_HOLD_MS - 1, noNamespace, withBoot, SHORT_LOCK_MAX_HOLD_MS * 2)).toBe("alive");
    expect(at(now - SHORT_LOCK_MAX_HOLD_MS - 1, noNamespace, withBoot, 100)).toBe("dead");
    // Not this rule: another host, a known other namespace, local, previous boot, untagged.
    expect(at(0, { ...noNamespace, hostname: "node-b" }, withBoot)).toBeUndefined();
    expect(at(0, { ...withBoot, pidNamespace: "pid:[2]" }, withBoot)).toBeUndefined();
    expect(at(0, withBoot, withBoot)).toBeUndefined();
    expect(at(0, { ...noNamespace, bootId: "boot-b" }, withBoot)).toBeUndefined();
    expect(unknownNamespaceLockLiveness("legacy", 0)).toBeUndefined();
    expect(at(Number.NaN, noNamespace, withBoot)).toBeUndefined();
  });
});

describe("mesh lock recovery across PID namespaces", () => {
  const holdLock = (store: MeshStore, owner: string): string => {
    const lockPath = path.join(store.root, ".lock");
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), owner);
    return lockPath;
  };

  it.each([1, 2] as const)("protocol %s never detaches a foreign owner whose PID is invisible here", async (lockProtocol) => {
    const store = new MeshStore(temp("pi-fabric-locality-mesh-"), 64 * 1024, 100, { lockProtocol, lockTimeoutMs: 150 });
    for (const token of [foreignNamespace(), foreignHost(), unknownNamespace()]) {
      const owner = `${token}\n${deadPid()}\n${Date.now() - 60_000}\n`;
      const lock = holdLock(store, owner);
      const operation = vi.fn();
      const error = await store.exclusive(operation).catch((caught: unknown) => caught);
      expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(String((error as Error).message)).toContain("another host or PID namespace");
      expect(operation).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."))).toEqual([]);
      fs.rmSync(lock, { recursive: true });
    }
  });

  it.each([1, 2] as const)("protocol %s reaps an unknown-namespace owner whose heartbeat is stale, never by ESRCH", async (lockProtocol) => {
    const store = new MeshStore(temp("pi-fabric-locality-mesh-"), 64 * 1024, 100, { lockProtocol, lockTimeoutMs: 1_000 });
    const pid = 123456;
    const owner = `${unknownNamespace()}\n${pid}\n${Date.now() - SHORT_LOCK_MAX_HOLD_MS - 5_000}\n`;
    holdLock(store, owner);
    const kill = vi.spyOn(process, "kill").mockImplementation((target) => {
      if (target === pid) throw Object.assign(new Error("not visible here"), { code: "ESRCH" });
      return true;
    });
    await expect(store.exclusive(() => "recovered")).resolves.toBe("recovered");
    // The heartbeat decided: the owner's PID was never probed.
    expect(kill.mock.calls.filter(([target]) => target === pid)).toEqual([]);
    expect(fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."))).toHaveLength(1);
    expect(fs.existsSync(path.join(store.root, ".lock"))).toBe(false);
  });

  it("still recovers a confirmed-dead local owner and an owner from an earlier boot of this host", async () => {
    const store = new MeshStore(temp("pi-fabric-locality-mesh-"), 64 * 1024, 100, { lockTimeoutMs: 1_000 });
    holdLock(store, `${localityLockToken()}\n${deadPid()}\n${Date.now()}\n`);
    await expect(store.exclusive(() => "local-dead")).resolves.toBe("local-dead");
    // Untagged (older) owners keep the plain ESRCH probe.
    holdLock(store, `legacy\n${deadPid()}\n${Date.now()}\n`);
    await expect(store.exclusive(() => "legacy-dead")).resolves.toBe("legacy-dead");
    if (self.bootId === undefined) return;
    // Our own live PID, but recorded by an earlier boot: a stranger, not the owner.
    holdLock(store, `${previousBoot()}\n${process.pid}\n${Date.now()}\n`);
    await expect(store.exclusive(() => "previous-boot")).resolves.toBe("previous-boot");
  });
});

describe("actor registry lock recovery across PID namespaces", () => {
  const setup = () => {
    const actorRoot = path.join(temp("pi-fabric-locality-registry-"), "actors");
    const lockPath = path.join(actorRoot, "actors.json.lock");
    fs.mkdirSync(lockPath, { recursive: true });
    return { store: new ActorRegistryStore(actorRoot), lockPath };
  };

  it("never reaps a foreign owner whose PID is invisible here, even when stale", async () => {
    for (const token of [foreignNamespace(), foreignHost(), unknownNamespace()]) {
      const { store, lockPath } = setup();
      const owner = `${token}\n123456\n${Date.now() - 120_000}\n`;
      fs.writeFileSync(path.join(lockPath, "owner"), owner);
      const old = new Date(Date.now() - 120_000);
      fs.utimesSync(lockPath, old, old);
      esrch();
      const operation = vi.fn();
      await expect(store.withLock(operation, 100)).rejects.toThrow();
      expect(operation).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(owner);
      vi.restoreAllMocks();
    }
  });

  it("reaps an unknown-namespace owner whose heartbeat is stale, and the registry recovers", async () => {
    const { store, lockPath } = setup();
    const owner = `${unknownNamespace()}\n123456\n${Date.now() - SHORT_LOCK_MAX_HOLD_MS - 5_000}\n`;
    fs.writeFileSync(path.join(lockPath, "owner"), owner);
    esrch();
    await expect(store.withLock(() => "recovered", 1_000)).resolves.toBe("recovered");
    expect(fs.readdirSync(path.dirname(lockPath)).filter(name => name.startsWith("actors.json.lock.dead."))).toHaveLength(1);
    // The recovered lock is released and usable again.
    expect(fs.existsSync(lockPath)).toBe(false);
    await expect(store.withLock(() => "again", 1_000)).resolves.toBe("again");
  });

  it("still recovers a confirmed-dead local owner and keeps same-namespace PID-reuse recovery", async () => {
    const dead = setup();
    fs.writeFileSync(path.join(dead.lockPath, "owner"), `${localityLockToken()}\n123456\n${Date.now()}\n`);
    esrch();
    await expect(dead.store.withLock(() => "recovered", 1_000)).resolves.toBe("recovered");
    vi.restoreAllMocks();
    if (process.platform !== "linux") return;
    const reused = setup();
    fs.writeFileSync(path.join(reused.lockPath, "owner"), `${localityLockToken()}\n${process.pid}\n${Date.now()}\n0\n`);
    await expect(reused.store.withLock(() => "reused", 1_000)).resolves.toBe("reused");
  });
});

describe("custody lock recovery across PID namespaces", () => {
  const holdCustody = (domain: string, owner: string): string => {
    const lock = path.join(domain, MESH_CUSTODY_LOCK_NAME);
    fs.mkdirSync(lock, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(lock, "owner"), owner);
    return lock;
  };

  it("refuses a foreign owner and recovers a dead local one", async () => {
    const domain = temp("pi-fabric-locality-custody-");
    const owner = `${foreignNamespace()}\n${deadPid()}\n${Date.now() - 60_000}\n`;
    const lock = holdCustody(domain, owner);
    await expect(acquireMeshCustodyLock(domain, 100, { ownIncarnation: undefined }))
      .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    fs.rmSync(lock, { recursive: true });
    holdCustody(domain, `${localityLockToken()}\n${deadPid()}\n${Date.now()}\n`);
    const release = await acquireMeshCustodyLock(domain, 1_000, { ownIncarnation: undefined });
    release();
  });

  it.skipIf(readPhysicalHostIdentity() === undefined)("host-lease gate: machine and boot evidence do not cover another PID namespace", async () => {
    const physical = readPhysicalHostIdentity()!;
    const gate = (token: string): string => `${token}\n${deadPid()}\n${Date.now() - 120_000}\n\n${physical.machineId}\n${physical.bootId}\n`;
    const age = (lock: string): void => {
      const old = new Date(Date.now() - 120_000);
      fs.utimesSync(path.join(lock, "owner"), old, old);
      fs.utimesSync(lock, old, old);
    };
    const domain = temp("pi-fabric-locality-gate-");
    const owner = gate(foreignNamespace());
    const lock = holdCustody(domain, owner);
    age(lock);
    await expect(acquireMeshCustodyLock(domain, 100, { hostQualified: true, ownIncarnation: undefined }))
      .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    fs.rmSync(lock, { recursive: true });
    age(holdCustody(domain, gate(localityLockToken())));
    const release = await acquireMeshCustodyLock(domain, 1_000, { hostQualified: true, ownIncarnation: undefined });
    release();
  });
});
