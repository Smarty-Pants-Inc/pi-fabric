import { createHash, randomUUID } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshCustodyUnrecoverableError as PublicCustodyError } from "../src/mesh.js";
import { readPhysicalHostIdentity, readPhysicalMachineId, isMeshLockTimeout } from "../src/core/atomic-write.js";
import { acquireMeshCustodyLock, CUSTODY_LOCK_STALE_MS, MeshCustodyUnrecoverableError } from "../src/mesh/custody-lock.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { HostLeaseLockBusyError } from "../src/topology/host-lease-lock.js";
import { hostLeasePath, readHostLeaseCurrent, renewHostLease, withOwnedHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { runTreeExitVeto } from "../src/storage/retention.js";

// smarty-dev#8132: machine locality survives missing boot/process identity. All proof files
// are inside Vitest's private TMPDIR, never the live fleet mesh or a shared /tmp probe root.
const machineId = "a".repeat(32);
const realReadFile = fs.readFileSync;
const roots: string[] = [], stores: MeshStore[] = [];
const identity: MeshIdentity = { id: "fallback-owner", name: "owner", kind: "agent" };
beforeEach(() => {
  vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
    const file = String(args[0]);
    if (file === "/etc/machine-id") return `${machineId}\n`;
    if (file === "/proc/sys/kernel/random/boot_id" || /^\/proc\/\d+\/stat$/.test(file)) {
      throw Object.assign(new Error("kernel identity unavailable"), { code: "EACCES" });
    }
    return realReadFile(...args);
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  for (const mesh of stores.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = (stateBackend: "file" | "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-custody-fallback-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend }); stores.push(mesh);
  const now = Date.now();
  const lease: FabricHostLease = { id: "fallback-host", rootId: "fallback-root", identityId: identity.id,
    incarnationToken: randomUUID(), startedAt: 1, updatedAt: now, expiresAt: now + 120_000 };
  return { root, mesh, lease };
};
const domainOf = (root: string, hostId: string) => path.join(root, "host-lease-commits",
  createHash("sha256").update(path.basename(hostLeasePath(root, hostId))).digest("hex"));
const deadPid = (): number => {
  const child = spawnSync(process.execPath, ["-e", ""], { timeout: 5_000 });
  expect(child.error).toBeUndefined(); expect(child.status).toBe(0);
  expect(() => process.kill(child.pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
  return child.pid;
};
const age = (lock: string): void => {
  const old = new Date(Date.now() - CUSTODY_LOCK_STALE_MS - 5_000);
  fs.utimesSync(path.join(lock, "owner"), old, old); fs.utimesSync(lock, old, old);
};
// Use the production writer to capture the complete degraded-identity receipt, then model
// a crash with a positively dead local child pid. No guessed pid and no running child remain.
const gate = async (root: string, hostId: string, pid: number | string, stale = true) => {
  const domain = domainOf(root, hostId), lock = path.join(domain, "custody.lock");
  const release = await acquireMeshCustodyLock(domain, 0, { hostQualified: true, ownIncarnation: undefined });
  let fields: string[];
  try { fields = fs.readFileSync(path.join(lock, "owner"), "utf8").split("\n"); }
  finally { release(); }
  fields![1] = String(pid);
  const owner = fields!.join("\n");
  fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), owner);
  if (stale) age(lock);
  return { domain, lock, owner };
};
const fences = (domain: string) => fs.readdirSync(domain).filter(name => name.startsWith("custody.lock.dead."));

describe.each(["file", "sqlite"] as const)("%s missing-identity custody recovery", backend => {
  it("retains machine locality in a receipt without boot or process-start identity", async () => {
    const s = setup(backend), held = await gate(s.root, s.lease.id, process.pid);
    expect(readPhysicalHostIdentity()).toBeUndefined(); expect(readPhysicalMachineId()).toBe(machineId);
    expect(held.owner.split("\n")).toEqual([expect.any(String), String(process.pid), expect.any(String), "", machineId, "", ""]);
  });

  it.skipIf(process.platform === "win32")("SIGKILLs a real host-qualified custody holder, recovers once, and never steals it live (POSIX SIGKILL is unavailable on Windows)", async () => {
    const s = setup(backend), domain = domainOf(s.root, s.lease.id), lock = path.join(domain, "custody.lock");
    // This child takes MeshStore.leaseCustody, not a fabricated owner directory. Only
    // kernel-identity reads are fault-injected; the receipt, lock, PID and death are real.
    const child = spawn(process.execPath, ["--input-type=module", "-e", `
      import fs from "node:fs";
      import { createJiti } from "jiti";
      import { pathToFileURL } from "node:url";
      const [root, file, backend, machineId] = process.argv.slice(1);
      const read = fs.readFileSync;
      fs.readFileSync = (...args) => {
        const file = String(args[0]);
        if (file === "/etc/machine-id") return machineId + "\\n";
        if (file === "/proc/sys/kernel/random/boot_id" || /^\\/proc\\/\\d+\\/stat$/.test(file)) {
          throw Object.assign(new Error("kernel identity unavailable"), { code: "EACCES" });
        }
        return read(...args);
      };
      globalThis[Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures")] = "create";
      const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
      const { MeshStore } = await jiti.import("./src/mesh/store.ts");
      const mesh = new MeshStore(root, 65536, 100, { stateBackend: backend });
      // Finite safety bound even if the parent fails; IPC keeps the holder alive.
      setTimeout(() => process.exit(2), 15000);
      await mesh.leaseCustody(file, async () => {
        process.send({ held: true, pid: process.pid });
        await new Promise(() => {});
      }, 0, { ownIncarnation: undefined });
    `, s.root, hostLeasePath(s.root, s.lease.id), backend, machineId], {
      cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe", "ipc"],
    });
    let stderr = "";
    child.stderr!.on("data", chunk => { stderr += chunk; });
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject); child.once("close", (code, signal) => resolve({ code, signal }));
    });
    exited.catch(() => undefined);
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Custody child readiness timed out: ${stderr}`)), 10_000);
        const finish = (error?: Error) => { clearTimeout(timer); if (error) reject(error); else resolve(); };
        child.once("error", finish);
        child.once("close", () => finish(new Error(`Custody child exited before readiness: ${stderr}`)));
        child.once("message", message => {
          try { expect(message).toEqual({ held: true, pid: child.pid }); finish(); }
          catch (error) { finish(error as Error); }
        });
      });
      const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
      expect(owner.split("\n")).toEqual([expect.any(String), String(child.pid), expect.any(String), "", machineId, "", ""]);
      age(lock); // Age is accelerated; it must not make the real live holder recoverable.
      for (let attempt = 0; attempt < 2; attempt++) {
        await expect(renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
      }
      expect(child.exitCode).toBeNull(); expect(child.signalCode).toBeNull();
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(fences(domain)).toEqual([]); expect(readHostLeaseCurrent(s.root, s.lease.id)).toBeUndefined();
      expect(child.kill("SIGKILL")).toBe(true);
      expect(await exited).toEqual({ code: null, signal: "SIGKILL" });
      expect(() => process.kill(child.pid!, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
      await renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined });
      expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(s.lease);
      expect(fences(domain)).toHaveLength(1); expect(fs.existsSync(lock)).toBe(false);
      expect(fs.readFileSync(path.join(domain, fences(domain)[0]!, "owner"), "utf8")).toBe(owner);
      const renewed = { ...s.lease, updatedAt: s.lease.updatedAt + 1_000, expiresAt: s.lease.expiresAt + 1_000 };
      await renewHostLease(s.mesh, renewed, { ownIncarnation: undefined });
      expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(renewed); expect(fences(domain)).toHaveLength(1);
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exited;
    }
  }, 20_000);

  it("requires ESRCH AND stale files, recovers once, resumes claims and renews after restart", async () => {
    const s = setup(backend), pid = deadPid(), held = await gate(s.root, s.lease.id, pid, false);
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toBeUndefined(); expect(fences(held.domain)).toEqual([]);
    age(held.lock);
    await renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined });
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(s.lease); expect(fences(held.domain)).toHaveLength(1);
    expect(fs.existsSync(held.lock)).toBe(false);
    s.mesh.closeState();
    const restarted = new MeshStore(s.root, 65_536, 100, { stateBackend: backend }); stores.push(restarted);
    const renewed = { ...s.lease, updatedAt: s.lease.updatedAt + 1_000, expiresAt: s.lease.expiresAt + 1_000 };
    await renewHostLease(restarted, renewed, { ownIncarnation: undefined });
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(renewed); expect(fences(held.domain)).toHaveLength(1);
    await expect(withOwnedHostLease(restarted, renewed, () => "owned", { ownIncarnation: undefined })).resolves.toBe("owned");
  });

  it.each(["missing", "unknown"] as const)("custody recovery never admits %s descendant identity to cleanup before confirmed exit", async state => {
    const s = setup(backend), pid = deadPid(), held = await gate(s.root, s.lease.id, pid);
    const run = path.join(s.root, "runs", "parent"), child = path.join(run, "nested", "child");
    fs.mkdirSync(child, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: String(pid) }));
    const statusFile = path.join(child, "status.json"), record = {
      status: "completed", transport: "process", ...(state === "unknown" ? { sessionId: "not-a-pid" } : {}),
    };
    fs.writeFileSync(statusFile, JSON.stringify(record));
    // This is the same strict tree-wide helper used by public offline cleanup.
    const veto = () => runTreeExitVeto(run, 0, undefined, true);
    expect(readPhysicalHostIdentity()).toBeUndefined();
    expect(veto()).toMatch(/exit is unconfirmed: unknown descendant identity/);
    await renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined });
    expect(fences(held.domain)).toHaveLength(1); // Degraded custody DID recover.
    expect(veto()).toMatch(/exit is unconfirmed: unknown descendant identity/);
    expect(JSON.parse(fs.readFileSync(statusFile, "utf8"))).toEqual(record);
    // A checked exited PID, not lock recovery or terminal status, removes the veto.
    fs.writeFileSync(statusFile, JSON.stringify({ ...record, sessionId: String(pid) }));
    expect(veto()).toBeUndefined();
  });

  it.each(["live", "EPERM"] as const)("never steals an ancient %s holder without identity", async status => {
    const s = setup(backend), held = await gate(s.root, s.lease.id, process.pid);
    const realKill = process.kill;
    const kill = vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (status === "EPERM" && pid === process.pid && signal === 0) {
        throw Object.assign(new Error("not permitted"), { code: "EPERM" });
      }
      return realKill(pid, signal);
    });
    for (let attempt = 0; attempt < 2; attempt++) {
      await expect(renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    }
    expect(kill).toHaveBeenCalledWith(process.pid, 0); expect(fences(held.domain)).toEqual([]);
    expect(fs.readFileSync(path.join(held.lock, "owner"), "utf8")).toBe(held.owner);
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toBeUndefined();
  });

  it.each(["", "not-a-pid"])("raises an exported, alarmed custody-unrecoverable for owner pid %j", async pid => {
    const s = setup(backend), held = await gate(s.root, s.lease.id, pid), alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
    const kill = vi.spyOn(process, "kill");
    const error = await renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined }).catch((e: unknown) => e);
    expect(PublicCustodyError).toBe(MeshCustodyUnrecoverableError); expect(error).toBeInstanceOf(PublicCustodyError);
    expect(error).toMatchObject({ code: "FABRIC_MESH_CUSTODY_UNRECOVERABLE", kind: "custody-unrecoverable", lock: held.lock, retryable: false });
    expect(isMeshLockTimeout(error)).toBe(false); expect(alarm).toHaveBeenCalledExactlyOnceWith(expect.stringContaining(`custody-unrecoverable: Fabric custody-unrecoverable: ${held.lock}`));
    expect(kill).not.toHaveBeenCalled(); expect(fs.readFileSync(path.join(held.lock, "owner"), "utf8")).toBe(held.owner);
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toBeUndefined();
  });

  it("does not recover an old namespace with a freshly rewritten owner", async () => {
    const s = setup(backend), held = await gate(s.root, s.lease.id, deadPid());
    const fresh = new Date(); fs.utimesSync(path.join(held.lock, "owner"), fresh, fresh);
    const kill = vi.spyOn(process, "kill");
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(kill).not.toHaveBeenCalled(); expect(fences(held.domain)).toEqual([]);
  });

  it.each(["foreign", "unknown"])("never PID-probes an ancient %s machine receipt", async locality => {
    const s = setup(backend), held = await gate(s.root, s.lease.id, deadPid());
    const fields = held.owner.split("\n"); fields[4] = locality === "foreign" ? "b".repeat(32) : "";
    fs.writeFileSync(path.join(held.lock, "owner"), fields.join("\n")); age(held.lock);
    const kill = vi.spyOn(process, "kill");
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(kill).not.toHaveBeenCalled(); expect(fences(held.domain)).toEqual([]);
  });

  it("keeps the identity-named recovery fence when another recoverer sees the dead receipt", async () => {
    const s = setup(backend), held = await gate(s.root, s.lease.id, deadPid());
    const releases = await Promise.all([1, 2].map(async () => {
      const release = await acquireMeshCustodyLock(held.domain, 1_000, { hostQualified: true, ownIncarnation: undefined });
      release(); return release;
    }));
    expect(releases).toHaveLength(2); expect(fences(held.domain)).toHaveLength(1);
    expect(fs.readFileSync(path.join(held.domain, fences(held.domain)[0]!, "owner"), "utf8")).toBe(held.owner);
  });

  const reaperFixture = async () => {
    const s = setup(backend), now = Date.now(), target = { ...s.lease, id: "expired-target", expiresAt: now - 7 * 3_600_000 };
    await renewHostLease(s.mesh, s.lease, { claim: true, ownIncarnation: undefined });
    await renewHostLease(s.mesh, target, { claim: true, ownIncarnation: undefined });
    const key = "topology/hosts/" + createHash("sha256").update(target.id).digest("hex");
    await s.mesh.put({ key, identity, value: { id: target.id, rootId: target.rootId, identity,
      incarnationToken: target.incarnationToken, expiresAt: target.expiresAt } });
    const reap = () => reapDeadHostRecords(s.mesh, identity, { ownHostId: s.lease.id, now,
      withCommitFence: operation => withOwnedHostLease(s.mesh, s.lease, operation, { ownIncarnation: undefined }) });
    return { ...s, target, key, reap };
  };

  it("resumes target reaping after dead local custody recovery without identity", async () => {
    const s = await reaperFixture(), held = await gate(s.root, s.target.id, deadPid());
    expect(await s.reap()).toBe(1); expect(await s.reap()).toBe(0);
    expect(readHostLeaseCurrent(s.root, s.target.id)).toBeUndefined(); expect(s.mesh.get(s.key, { fresh: true })).toBeUndefined();
    expect(fences(held.domain)).toHaveLength(1); expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(s.lease);
  });

  it("propagates missing-pid target alarms instead of silently returning zero reaped records", async () => {
    const s = await reaperFixture(), held = await gate(s.root, s.target.id, ""), before = s.mesh.listAll("", { fresh: true });
    const alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
    await expect(s.reap()).rejects.toMatchObject({ kind: "custody-unrecoverable", lock: held.lock });
    expect(alarm).toHaveBeenCalledOnce(); expect(s.mesh.listAll("", { fresh: true })).toEqual(before);
    expect(readHostLeaseCurrent(s.root, s.target.id)).toEqual(s.target);
  });
});
