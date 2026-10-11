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
  procfsShowsOwner,
  procfsStartTicks,
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

  it("keeps a stale owner alive while our procfs shows its PID with the recorded start (PR #864)", () => {
    const noNamespace = { hostname: "node-a", bootId: "boot-a" };
    const reader = ownerLocalityTag({ hostname: "node-a", pidNamespace: "pid:[1]", bootId: "boot-a" });
    const token = `${uuid}-${ownerLocalityTag(noNamespace)}`;
    const now = 1_700_000_000_000;
    const stale = now - SHORT_LOCK_MAX_HOLD_MS - 1;
    const judge = (procfs: string | undefined, start?: string, created = stale) =>
      unknownNamespaceLockLiveness(token, created, { now, pid: 4242, start, procfsStart: () => procfs }, reader);
    // Visible with the recorded start, or visible and the record carries none: alive.
    expect(judge("777", "777")).toBe("alive");
    expect(judge("777")).toBe("alive");
    expect(judge("777", "")).toBe("alive");
    // Visible but its start is unreadable here: cannot be excluded, alive.
    expect(judge("", "777")).toBe("alive");
    // Another start under the same PID is a stranger; not visible at all: the age decides.
    expect(judge("778", "777")).toBe("dead");
    expect(judge(undefined, "777")).toBe("dead");
    expect(judge(undefined)).toBe("dead");
    // Fresh stays alive without consulting procfs.
    expect(unknownNamespaceLockLiveness(token, now - 1_000, { now, pid: 4242, procfsStart: () => { throw new Error("probed"); } }, reader)).toBe("alive");
    // procfsShowsOwner on the real procfs: this process is visible with its own start.
    if (process.platform === "linux") {
      expect(procfsStartTicks(process.pid)).toBe(ownStartTicks());
      expect(procfsShowsOwner(process.pid, ownStartTicks())).toBe(true);
      expect(procfsShowsOwner(process.pid, "1")).toBe(false);
      expect(procfsShowsOwner(deadPid(), undefined)).toBe(false);
    }
  });
});

// PR #864: the unknown-namespace reap path keeps every protection a live owner had. Recovery
// runs only under the identity-bound rename fence after re-reading the judged inode and owner
// record, and positive procfs evidence outranks the creation-time heartbeat.
const ownStartTicks = (): string => {
  const stat = fs.readFileSync("/proc/self/stat", "utf8");
  return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]!;
};
const staleAt = (): number => Date.now() - SHORT_LOCK_MAX_HOLD_MS - 5_000;

describe("unknown-namespace recovery keeps live owners protected (PR #864)", () => {
  const holdMesh = (store: MeshStore, owner: string): string => {
    const lockPath = path.join(store.root, ".lock");
    fs.mkdirSync(lockPath, { mode: 0o700 });
    fs.writeFileSync(path.join(lockPath, "owner"), owner);
    return lockPath;
  };
  const meshFences = (store: MeshStore): string[] => fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."));
  const registry = () => {
    const actorRoot = path.join(temp("pi-fabric-recovery-registry-"), "actors");
    const lockPath = path.join(actorRoot, "actors.json.lock");
    fs.mkdirSync(lockPath, { recursive: true });
    return { store: new ActorRegistryStore(actorRoot), lockPath, actorRoot };
  };
  const registryFences = (actorRoot: string): string[] =>
    fs.readdirSync(actorRoot).filter(name => name.startsWith("actors.json.lock.dead."));

  it.each([1, 2] as const)("mesh protocol %s never reaps a known-foreign-namespace owner (fresh or stale) or a fresh unknown-namespace owner", async (lockProtocol) => {
    const store = new MeshStore(temp("pi-fabric-recovery-mesh-"), 64 * 1024, 100, { lockProtocol, lockTimeoutMs: 150 });
    for (const [token, created] of [[foreignNamespace(), Date.now() - 1_000], [foreignNamespace(), staleAt()],
      [unknownNamespace(), Date.now() - 1_000]] as const) {
      const owner = `${token}\n${deadPid()}\n${created}\n`;
      const lock = holdMesh(store, owner);
      esrch();
      const operation = vi.fn();
      await expect(store.exclusive(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(operation).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(meshFences(store)).toEqual([]);
      vi.restoreAllMocks();
      fs.rmSync(lock, { recursive: true });
    }
  });

  it.skipIf(process.platform !== "linux").each([1, 2] as const)("mesh protocol %s never reaps a stale unknown-namespace owner our procfs shows with its recorded start", async (lockProtocol) => {
    const store = new MeshStore(temp("pi-fabric-recovery-mesh-"), 64 * 1024, 100, { lockProtocol, lockTimeoutMs: 150 });
    for (const owner of [
      `${unknownNamespace()}\n${process.pid}\n${staleAt()}\n${ownStartTicks()}\n`,
      // No recorded start (protocol 1 wire): visibility alone keeps it.
      `${unknownNamespace()}\n${process.pid}\n${staleAt()}\n`,
    ]) {
      const lock = holdMesh(store, owner);
      const operation = vi.fn();
      await expect(store.exclusive(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(operation).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(meshFences(store)).toEqual([]);
      fs.rmSync(lock, { recursive: true });
    }
    // Control: the same visible PID with another recorded start is a stranger; the heartbeat reaps.
    holdMesh(store, `${unknownNamespace()}\n${process.pid}\n${staleAt()}\n1\n`);
    const reaper = new MeshStore(store.root, 64 * 1024, 100, { lockProtocol, lockTimeoutMs: 1_000 });
    await expect(reaper.exclusive(() => "reaped")).resolves.toBe("reaped");
    expect(meshFences(store)).toHaveLength(1);
  });

  it("actor registry never reaps a known-foreign-namespace owner (fresh or stale) or a fresh unknown-namespace owner", async () => {
    for (const [token, created] of [[foreignNamespace(), Date.now() - 1_000], [foreignNamespace(), staleAt()],
      [unknownNamespace(), Date.now() - 1_000]] as const) {
      const { store, lockPath, actorRoot } = registry();
      const owner = `${token}\n${deadPid()}\n${created}\n`;
      fs.writeFileSync(path.join(lockPath, "owner"), owner);
      const old = new Date(Date.now() - 120_000);
      fs.utimesSync(lockPath, old, old);
      esrch();
      const operation = vi.fn();
      await expect(store.withLock(operation, 100)).rejects.toThrow();
      expect(operation).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(owner);
      expect(registryFences(actorRoot)).toEqual([]);
      vi.restoreAllMocks();
    }
  });

  it.skipIf(process.platform !== "linux")("actor registry never reaps a stale unknown-namespace owner our procfs shows with its recorded start", async () => {
    for (const owner of [
      `${unknownNamespace()}\n${process.pid}\n${staleAt()}\n${ownStartTicks()}\n`,
      `${unknownNamespace()}\n${process.pid}\n${staleAt()}\n`,
    ]) {
      const { store, lockPath, actorRoot } = registry();
      fs.writeFileSync(path.join(lockPath, "owner"), owner);
      const operation = vi.fn();
      await expect(store.withLock(operation, 100)).rejects.toThrow();
      expect(operation).not.toHaveBeenCalled();
      expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(owner);
      expect(registryFences(actorRoot)).toEqual([]);
    }
    const control = registry();
    fs.writeFileSync(path.join(control.lockPath, "owner"), `${unknownNamespace()}\n${process.pid}\n${staleAt()}\n1\n`);
    await expect(control.store.withLock(() => "reaped", 1_000)).resolves.toBe("reaped");
    expect(registryFences(control.actorRoot)).toHaveLength(1);
  });

  it("actor registry: two concurrent recoverers of one stale unknown-namespace owner reap it once", async () => {
    const { store: first, lockPath, actorRoot } = registry();
    const second = new ActorRegistryStore(actorRoot);
    const stale = `${unknownNamespace()}\n${deadPid()}\n${staleAt()}\n`;
    fs.writeFileSync(path.join(lockPath, "owner"), stale);
    const rename = fs.renameSync;
    let paused = false;
    let pausedError: NodeJS.ErrnoException | undefined;
    let successor: string | undefined;
    let releaseSecond: (() => void) | undefined;
    let secondHeld: Promise<void> | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (!paused && String(from) === lockPath) {
        // The first recoverer judged the stale owner and passed its inode/owner re-read. Paused at
        // the rename, a second recoverer reaps the same owner and takes the lock (synchronously,
        // up to its critical section).
        paused = true;
        secondHeld = second.withLock(() => new Promise<void>((resolve) => { releaseSecond = resolve; }), 1_000);
        successor = fs.readFileSync(path.join(lockPath, "owner"), "utf8");
        try { rename(from, to); } catch (error) { pausedError = error as NodeJS.ErrnoException; throw error; }
        return;
      }
      rename(from, to);
    });
    const firstRun = first.withLock(() => "first", 3_000);
    expect(paused).toBe(true);
    // The paused rename met the identity-bound fence and failed closed; the successor kept its lock.
    expect(["ENOTEMPTY", "EEXIST"]).toContain(pausedError?.code);
    expect(successor).not.toBe(stale);
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(successor);
    const fences = registryFences(actorRoot);
    expect(fences).toHaveLength(1);
    expect(fs.readFileSync(path.join(actorRoot, fences[0]!, "owner"), "utf8")).toBe(stale);
    // The first recoverer waits for the live successor, then acquires normally.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(successor);
    releaseSecond!();
    await secondHeld;
    await expect(firstRun).resolves.toBe("first");
    expect(registryFences(actorRoot)).toHaveLength(1);
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
    // A PID our procfs does not show: positive evidence cannot keep it alive.
    const pid = deadPid();
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
    const owner = `${unknownNamespace()}\n${deadPid()}\n${Date.now() - SHORT_LOCK_MAX_HOLD_MS - 5_000}\n`;
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
