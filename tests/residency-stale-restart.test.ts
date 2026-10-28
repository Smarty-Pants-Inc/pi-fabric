// Real-filesystem regression for the factory-0 PV restart incident (smarty-dev#17082).
// Kernel flock, not mutable PID metadata, is the residency trust boundary.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import * as lockIdentity from "../src/core/lock-identity.js";
import * as atomic from "../src/core/atomic-write.js";
import { ownPidNamespace, pidNamespaceTestHooks } from "../src/core/pid-namespace.js";
import { ResidentHost, ResidentHostAlreadyRunning } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { superviseWake } from "../src/residency/launcher.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { RESIDENT_HOST_FORMAT, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentOwnerLive, residentUnacknowledgedDeliveries } from "../src/residency/wake-index.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import * as processUtils from "../src/agents/transports/process-utils.js";

const roots: string[] = [];
const hosts: ResidentHost[] = [];
const clients: ResidencyClient[] = [];
const meshes: MeshStore[] = [];
afterEach(async () => {
  await Promise.all(clients.splice(0).map(client => client.close()));
  await Promise.all(hosts.splice(0).map(host => host.close()));
  for (const mesh of meshes.splice(0)) mesh.closeState();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  pidNamespaceTestHooks.reset();
  // Only directories created by this suite are ever removed.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = (canonical = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-residency-stale-restart-"));
  roots.push(root);
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:stale-restart", sessionId: "stale-restart",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: canonical ? path.join(root, "mesh", "residency", "stale-restart") : path.join(root, "resident"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, residentIdleExitMs: 60_000 },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention }, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = () => { const value = new ResidentHost(config); hosts.push(value); return value; };
  return { config, host, file: (name: string) => path.join(config.residencyRoot, name) };
};
const json = (file: string) => JSON.parse(fs.readFileSync(file, "utf8"));
const identity = async () => {
  const pidNamespace = (await ownPidNamespace())!;
  expect(pidNamespace).toMatch(/^linux:/);
  return { pid: process.pid, processStartTime: processStartTime(process.pid)!, pidNamespace,
    bootId: pidNamespace.split(":")[2]!, token: "previous-generation" };
};
const staleIdentity = async (kind: "other-boot" | "other-namespace" | "dead-pid" | "reused-pid") => {
  const value = await identity();
  // A genuinely live, DIFFERENT pid makes the old owner.json PID-only veto observable.
  value.pid = process.ppid;
  value.processStartTime = processStartTime(process.ppid)!;
  if (kind === "other-boot") {
    const old = value.bootId === "00000000-0000-0000-0000-000000000001"
      ? "00000000-0000-0000-0000-000000000002" : "00000000-0000-0000-0000-000000000001";
    value.pidNamespace = value.pidNamespace.replace(value.bootId, old);
    value.bootId = old;
  } else if (kind === "other-namespace") {
    value.pidNamespace = value.pidNamespace.replace(/:\d+$/, ":999999999999999999");
  } else if (kind === "dead-pid") {
    value.pid = 2147483647; value.processStartTime = "1";
  } else value.processStartTime = String(BigInt(value.processStartTime) + 1n);
  return value;
};
const recoveryRecords = (f: ReturnType<typeof fixture>) => fs.readdirSync(f.config.residencyRoot)
  .filter(name => name.endsWith(".recovery.json")).map(name => ({ name, ...json(f.file(name)) }));

// expected-skip: non-linux: these regressions require Linux procfs and the actual util-linux flock fence.
describe.skipIf(process.platform !== "linux")("residency stale restart on a real filesystem", () => {
  it("publishes boot, namespace, pid and birth in both host.lock and owner.json", async () => {
    const f = fixture(); const host = f.host();
    await host.start();
    const own = await identity();
    for (const name of ["host.lock", "owner.json"]) {
      expect(json(f.file(name))).toMatchObject({ ...own, token: host.generation });
    }
  });

  it.each(["other-boot", "other-namespace", "dead-pid", "reused-pid"] as const)(
    "recovers %s records without provenance, preserving bytes and the flock inode", async kind => {
      const f = fixture(); const previous = await staleIdentity(kind);
      const bytes = JSON.stringify(previous);
      for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), bytes);
      const inode = fs.statSync(f.file("host.lock")).ino;
      const host = f.host();
      await host.start();
      expect(host.actors).toBeDefined();
      expect(json(f.file("owner.json")).token).toBe(host.generation);
      expect(fs.statSync(f.file("host.lock")).ino).toBe(inode);
      const recovered = recoveryRecords(f);
      expect(recovered).toHaveLength(2);
      for (const name of ["host.lock", "owner.json"]) {
        const receipt = recovered.find(record => record.sourcePath === f.file(name));
        expect(receipt).toMatchObject({ reason: kind, previousOwner: previous });
        expect(receipt.name).toMatch(/\d{4}-\d\d-\d\dT.*\.recovery\.json$/);
        expect(Number.isFinite(Date.parse(receipt.recoveredAt))).toBe(true);
        expect(fs.readFileSync(receipt.preservedPath, "utf8")).toBe(bytes);
      }
      const stamp = fs.statSync(f.file("host.lock"), { bigint: true });
      expect(json(f.file("host-fence.json"))).toMatchObject({ dev: String(stamp.dev), ino: String(stamp.ino) });
      await host.close();
      expect(fs.statSync(f.file("host.lock")).ino).toBe(inode);
    });

  it.each(["other-boot", "other-namespace", "reused-pid"] as const)(
    "does not let established owner.json with %s and a currently live pid block restart", async kind => {
      const f = fixture(); const first = f.host(); await first.start(); await first.close();
      const inode = fs.statSync(f.file("host.lock")).ino;
      const previous = JSON.stringify(await staleIdentity(kind));
      fs.writeFileSync(f.file("owner.json"), previous);
      const next = f.host(); await next.start();
      expect(json(f.file("owner.json")).token).toBe(next.generation);
      expect(fs.statSync(f.file("host.lock")).ino).toBe(inode);
      const recovered = recoveryRecords(f).find(record => record.sourcePath === f.file("owner.json"));
      expect(fs.readFileSync(recovered.preservedPath, "utf8")).toBe(previous);
    });

  it.each(["other-boot", "other-namespace", "dead-pid", "reused-pid"] as const)(
    "never renames or steals an actively flocked inode despite %s lock AND owner metadata", async kind => {
    const f = fixture(); const live = f.host(); await live.start();
    const inode = fs.statSync(f.file("host.lock")).ino;
    const bytes = JSON.stringify(await staleIdentity(kind));
    for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), bytes);
    const contender = f.host();
    await expect(contender.start()).rejects.toBeInstanceOf(ResidentHostAlreadyRunning);
    expect(contender.actors).toBeUndefined();
    expect(recoveryRecords(f)).toEqual([]);
    expect(fs.statSync(f.file("host.lock")).ino).toBe(inode);
    for (const name of ["host.lock", "owner.json"]) expect(fs.readFileSync(f.file(name), "utf8")).toBe(bytes);
  });

  it("refuses a live same-namespace owner even if its kernel flock is absent", async () => {
    const f = fixture(); const previous = await identity();
    previous.pid = process.ppid; previous.processStartTime = processStartTime(process.ppid)!;
    const bytes = JSON.stringify(previous);
    for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), bytes);
    const host = f.host();
    await expect(host.start()).rejects.toBeInstanceOf(ResidentHostAlreadyRunning);
    expect(host.actors).toBeUndefined(); expect(recoveryRecords(f)).toEqual([]);
    for (const name of ["host.lock", "owner.json"]) expect(fs.readFileSync(f.file(name), "utf8")).toBe(bytes);
  });

  it("allows exactly one of two concurrent real starters to recover the stale generation", async () => {
    const f = fixture(); const bytes = JSON.stringify(await staleIdentity("other-namespace"));
    for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), bytes);
    const inode = fs.statSync(f.file("host.lock")).ino;
    const contenders = [f.host(), f.host()];
    const outcomes = await Promise.allSettled(contenders.map(host => host.start()));
    expect(outcomes.filter(outcome => outcome.status === "fulfilled")).toHaveLength(1);
    const loser = outcomes.findIndex(outcome => outcome.status === "rejected");
    expect((outcomes[loser] as PromiseRejectedResult).reason).toBeInstanceOf(ResidentHostAlreadyRunning);
    expect(contenders[loser]!.actors).toBeUndefined();
    expect(recoveryRecords(f)).toHaveLength(2);
    expect(fs.statSync(f.file("host.lock")).ino).toBe(inode);
    expect(json(f.file("owner.json")).token).toBe(contenders[1 - loser]!.generation);
  });

  it.each(["other-boot", "other-namespace", "reused-pid"] as const)(
    "holdsRoot recognizes the exact %s predecessor during startup failure before owner publication", async kind => {
      const f = fixture(); const bytes = JSON.stringify(await staleIdentity(kind));
      for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), bytes);
      // Admission fails AFTER acquisition, before replacing the old owner.json.
      fs.writeFileSync(f.file("watchdog-custody.json"), "uncertain watchdog custody");
      const own = await identity(); const host = f.host(); const held: boolean[] = [];
      host.beforeRelease = (_failure, owned) => {
        expect(fs.readFileSync(f.file("owner.json"), "utf8")).toBe(bytes);
        held.push(owned());
        // A mutable live successor record must NOT inherit the cached stale verdict.
        const successor = { ...own, pid: process.ppid, processStartTime: processStartTime(process.ppid),
          token: "replacement-generation" };
        fs.writeFileSync(f.file("owner.json"), JSON.stringify(successor));
        held.push(owned());
      };
      await expect(host.start()).rejects.toThrow(/watchdog custody/);
      expect(held).toEqual([true, false]);
      expect(recoveryRecords(f)).toHaveLength(2);
      expect(host.actors).toBeUndefined();
    });

  it.each(["other-boot", "other-namespace"] as const)(
    "exclusive-create fallback moves the %s claim aside on the real filesystem", async kind => {
      const f = fixture(); const previous = JSON.stringify(await staleIdentity(kind));
      const own = await identity();
      for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), previous);
      const inode = fs.statSync(f.file("host.lock")).ino;
      fs.writeFileSync(f.file("watchdog-custody.json"), "uncertain watchdog custody");
      // Only the platform/identity reader is adapted. Public start(), wx, rename,
      // snapshots and root-fence revalidation all use the actual filesystem.
      vi.spyOn(lockIdentity, "readLockIdentity").mockResolvedValue({ pid: own.pid, start: own.processStartTime,
        bootId: own.bootId, namespace: own.pidNamespace });
      vi.spyOn(process, "platform", "get").mockReturnValue("win32");
      const host = f.host();
      let observed: { held: boolean; owner: unknown; inode: number } | undefined;
      host.beforeRelease = (_failure, owned) => {
        observed = { held: owned(), owner: json(f.file("host.lock")), inode: fs.statSync(f.file("host.lock")).ino };
      };
      await expect(host.start()).rejects.toThrow(/watchdog custody/);
      expect(observed).toMatchObject({ held: true, owner: { ...own, token: host.generation } });
      expect(observed!.inode).not.toBe(inode);
      const recovered = recoveryRecords(f);
      expect(recovered).toHaveLength(2);
      const lock = recovered.find(record => record.sourcePath === f.file("host.lock"));
      expect(lock.action).toBe("move-aside");
      expect(fs.statSync(lock.preservedPath).ino).toBe(inode);
      expect(fs.readFileSync(lock.preservedPath, "utf8")).toBe(previous);
      expect(fs.readdirSync(f.config.residencyRoot).filter(name => name.startsWith("host.lock.recover."))).toHaveLength(1);
    });

  it.each(["dead", "live"] as const)(
    "exclusive-create recovery %s claimant after publishing intent but before stale rename", async state => {
      const f = fixture(); const previous = JSON.stringify(await staleIdentity("other-boot"));
      const own = await identity();
      for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), previous);
      const judged = fs.lstatSync(f.file("host.lock"), { bigint: true });
      const digest = createHash("sha256").update(`${judged.dev}:${judged.ino}:${previous}`).digest("hex");
      const guard = f.file(`host.lock.recover.${digest}`);
      fs.mkdirSync(guard);
      const recoveryOwner = JSON.stringify({ ...own, token: "interrupted-recovery", startedAt: 1,
        ...(state === "dead" ? { pid: 2147483647, processStartTime: "1" } : {}) });
      fs.writeFileSync(path.join(guard, "owner"), recoveryOwner);
      const intent = JSON.stringify({ sourcePath: f.file("host.lock"), reason: "other-boot", previousOwner: JSON.parse(previous) });
      fs.writeFileSync(path.join(guard, "intent.json"), intent);
      fs.writeFileSync(f.file("watchdog-custody.json"), "uncertain watchdog custody");
      vi.spyOn(lockIdentity, "readLockIdentity").mockResolvedValue({ pid: own.pid, start: own.processStartTime,
        bootId: own.bootId, namespace: own.pidNamespace });
      vi.spyOn(process, "platform", "get").mockReturnValue("win32");
      const host = f.host(); const held: boolean[] = [];
      host.beforeRelease = (_failure, owned) => { held.push(owned()); };
      if (state === "dead") {
        await expect(host.start()).rejects.toThrow(/watchdog custody/);
        expect(held).toEqual([true]);
        const receipt = recoveryRecords(f).find(record => record.source === guard);
        expect(receipt).toMatchObject({ reason: "dead-pid" });
        expect(fs.readFileSync(path.join(receipt.aside, "intent.json"), "utf8")).toBe(intent);
        expect(fs.readFileSync(path.join(receipt.aside, "owner"), "utf8")).toBe(recoveryOwner);
        const lock = recoveryRecords(f).find(record => record.sourcePath === f.file("host.lock"));
        expect(fs.statSync(lock.preservedPath, { bigint: true }).ino).toBe(judged.ino);
        expect(json(path.join(guard, "owner")).token).toBe(host.generation);
      } else {
        await expect(host.start()).rejects.toBeInstanceOf(ResidentHostAlreadyRunning);
        expect(held).toEqual([]);
        expect(recoveryRecords(f)).toEqual([]);
        expect(fs.readFileSync(path.join(guard, "owner"), "utf8")).toBe(recoveryOwner);
        expect(fs.readFileSync(path.join(guard, "intent.json"), "utf8")).toBe(intent);
        for (const name of ["host.lock", "owner.json"]) expect(fs.readFileSync(f.file(name), "utf8")).toBe(previous);
      }
    });

  it.each(["darwin", "win32"] as const)(
    "prepares native birth outside locks instead of explicitly supplying undefined on %s", async platform => {
      const f = fixture();
      const bootId = "00000000-0000-0000-0000-000000000001";
      const machine = "00000000-0000-0000-0000-000000000002";
      const pidNamespace = platform === "darwin" ? `darwin:${machine}:${bootId}:host` : `win32:${machine}:123456:host`;
      const birth = platform === "darwin" ? "darwin:Mon Jan  1 00:00:00 2024" : "win32:123457";
      pidNamespaceTestHooks.set(pidNamespace);
      vi.spyOn(process, "platform", "get").mockReturnValue(platform);
      const prepare = vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(birth);
      fs.writeFileSync(f.file("watchdog-custody.json"), "uncertain watchdog custody");
      const host = f.host(); let observed: unknown;
      host.beforeRelease = () => { observed = json(f.file("host.lock")); };
      await expect(host.start()).rejects.toThrow(/watchdog custody/);
      expect(prepare).toHaveBeenCalledOnce();
      expect(observed).toMatchObject({ pid: process.pid, processStartTime: birth,
        pidNamespace, bootId: pidNamespace.split(":")[2] });
    });

  it("logs legacy fallback if native cross-platform birth preparation is unavailable", async () => {
    const f = fixture(); const pidNamespace = "win32:00000000-0000-0000-0000-000000000002:123456:host";
    pidNamespaceTestHooks.set(pidNamespace);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const prepare = vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue(undefined);
    const warning = vi.spyOn(diagnostics, "warn");
    fs.writeFileSync(f.file("watchdog-custody.json"), "uncertain watchdog custody");
    const host = f.host(); let observed: unknown;
    host.beforeRelease = () => { observed = json(f.file("host.lock")); };
    await expect(host.start()).rejects.toThrow(/watchdog custody/);
    expect(prepare).toHaveBeenCalledOnce();
    expect(observed).toMatchObject({ pid: process.pid, pidNamespace });
    expect(observed).not.toHaveProperty("processStartTime");
    expect(warning.mock.calls.some(call => /legacy/i.test(String(call[1])) && /host\.lock/.test(String(call[1])))).toBe(true);
  });

  it.each(["other-boot", "other-namespace", "reused-pid"] as const)(
    "public ensureHost crosses the launch boundary and actual acquisition for a %s ready owner", async kind => {
      const f = fixture(); const previous = { ...(await staleIdentity(kind)), format: RESIDENT_HOST_FORMAT,
        hostId: residentHostId(f.config.rootId), startedAt: 1, readyAt: 1 };
      const bytes = JSON.stringify(previous);
      for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), bytes);
      const inode = fs.statSync(f.file("host.lock")).ino;
      const mesh = new MeshStore(f.config.meshRoot, 65_536, 100); meshes.push(mesh);
      const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: f.config.rootId,
        rootId: f.config.rootId, identity: { id: f.config.rootId, name: "Main", kind: "main" } });
      const client = new ResidencyClient({ config: f.config, mesh, participants,
        mainAgent: { local: false } as FabricMainAgentTarget, startupTimeoutMs: 1_000 });
      clients.push(client);
      const host = f.host(); const stop = vi.fn(async () => {});
      // Substitute ONLY the detached-process boundary: client ensureHost and
      // the launched host's actual start/acquisition/readiness all remain real.
      const spawn = vi.spyOn(processUtils, "spawnDetached").mockImplementation(async (_entry, argv) => {
        const launchToken = argv[argv.indexOf("--launch-token") + 1]!;
        vi.stubEnv("PI_FABRIC_RESIDENT_LAUNCH_TOKEN", launchToken);
        await host.start();
        return { pid: process.pid, closed: new Promise<void>(() => {}), stop, isAlive: async () => true,
          lostContact: () => undefined, waitForClose: async () => {} };
      });
      const owner = await client.ensureHost();
      expect(spawn).toHaveBeenCalledOnce(); expect(stop).not.toHaveBeenCalled();
      expect(owner.token).toBe(host.generation); expect(owner.pid).toBe(process.pid);
      expect(owner.bootId).toBe((await identity()).bootId);
      expect(fs.statSync(f.file("host.lock")).ino).toBe(inode);
      expect(recoveryRecords(f)).toHaveLength(2);
      expect(host.actors).toBeDefined();
    });

  it("public ensureHost adopts the actual live host without launch or changing its inode/owner bytes", async () => {
    const f = fixture(); const host = f.host(); await host.start();
    const bytes = fs.readFileSync(f.file("owner.json"), "utf8"); const inode = fs.statSync(f.file("host.lock")).ino;
    const mesh = new MeshStore(f.config.meshRoot, 65_536, 100); meshes.push(mesh);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: f.config.rootId,
      rootId: f.config.rootId, identity: { id: f.config.rootId, name: "Main", kind: "main" } });
    const client = new ResidencyClient({ config: f.config, mesh, participants,
      mainAgent: { local: false } as FabricMainAgentTarget, startupTimeoutMs: 1_000 });
    clients.push(client);
    const spawn = vi.spyOn(processUtils, "spawnDetached").mockRejectedValue(new Error("must not launch over a live host"));
    expect((await client.ensureHost()).token).toBe(host.generation);
    expect(spawn).not.toHaveBeenCalled(); expect(recoveryRecords(f)).toEqual([]);
    expect(fs.readFileSync(f.file("owner.json"), "utf8")).toBe(bytes);
    expect(fs.statSync(f.file("host.lock")).ino).toBe(inode);
  });

  it.each(["other-boot", "other-namespace", "live"] as const)(
    "actual publication indexing and launcher wake do not confuse a %s owner with a colliding PID", async kind => {
      const f = fixture(true);
      const previous = kind === "live" ? await identity() : await staleIdentity(kind);
      const owner = { ...previous, format: RESIDENT_HOST_FORMAT, hostId: residentHostId(f.config.rootId), startedAt: 1, readyAt: 1 };
      for (const name of ["host.lock", "owner.json"]) fs.writeFileSync(f.file(name), JSON.stringify(owner));
      fs.writeFileSync(f.file("wake-routes.json"), JSON.stringify({ format: 1, rootId: f.config.rootId,
        hostId: residentHostId(f.config.rootId), configJson: canonicalResidentWakeConfig(f.config),
        actors: [{ id: "listener", name: "listener", topics: ["restart.delivery"] }] }));
      // Real mesh publication/indexing, with automatic process launch disabled.
      const mesh = new MeshStore(f.config.meshRoot, 65_536, 100, { canWakeResidents: () => false }); meshes.push(mesh);
      const event = await mesh.publish({ topic: "restart.delivery", from: { id: "publisher", name: "publisher", kind: "main" } });
      const pending = residentUnacknowledgedDeliveries(mesh.root);
      expect(residentOwnerLive(f.config.residencyRoot)).toBe(kind === "live");
      if (kind === "live") expect(pending.size).toBe(0);
      else expect(pending.get(f.config.residencyRoot)?.id).toBe(event.id);
      let launches = 0;
      await superviseWake(f.file("config.json"), async () => {
        launches++;
        const host = f.host(); await host.start(); await host.close();
      });
      expect(launches).toBe(kind === "live" ? 0 : 1);
      expect(recoveryRecords(f).filter(record => record.sourcePath === f.file("host.lock"))).toHaveLength(kind === "live" ? 0 : 1);
    });

  it("keeps incomplete identity on the legacy policy and logs the warning", async () => {
    const f = fixture(); const first = f.host(); await first.start(); await first.close();
    const previous = await staleIdentity("other-boot");
    const { pidNamespace: _missing, ...incomplete } = previous;
    fs.writeFileSync(f.file("owner.json"), JSON.stringify(incomplete));
    const warning = vi.spyOn(diagnostics, "warn");
    await expect(f.host().start()).rejects.toBeInstanceOf(ResidentHostAlreadyRunning);
    expect(warning.mock.calls.some(call => /legacy/i.test(String(call[1])) && /owner\.json/.test(String(call[1])))).toBe(true);
    expect(json(f.file("owner.json"))).toEqual(incomplete);
    expect(recoveryRecords(f)).toEqual([]);
  });

  it("retains conservative legacy refusal, with a logged warning rather than silently removing bytes", async () => {
    const f = fixture(); const bytes = JSON.stringify({ pid: 2147483647, token: "legacy" });
    fs.writeFileSync(f.file("host.lock"), bytes);
    const warning = vi.spyOn(diagnostics, "warn"); const host = f.host();
    await expect(host.start()).rejects.toThrow(/legacy|uncertain/);
    expect(warning.mock.calls.some(call => /legacy/i.test(String(call[1])) && /host\.lock/.test(String(call[1])))).toBe(true);
    expect(fs.readFileSync(f.file("host.lock"), "utf8")).toBe(bytes);
    expect(recoveryRecords(f)).toEqual([]);
  });
});
