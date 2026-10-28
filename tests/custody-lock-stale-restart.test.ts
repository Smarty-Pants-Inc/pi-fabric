import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { acquireMeshCustodyLock, CUSTODY_LOCK_STALE_MS } from "../src/mesh/custody-lock.js";
import { MeshStore } from "../src/mesh/store.js";
import { hostLeasePath } from "../src/topology/host-leases.js";
import { localFilesystemTestHooks, pidNamespaceTestHooks } from "../src/core/pid-namespace.js";

// Data-safety/incident regression: real filesystem and production leaseCustody host-commit path.
// Keep successful holders and all probe traces; cleanup APIs are recorded, never executed by this suite.
const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "custody-stale-restart-")); roots.push(directory); return directory; };
afterAll(() => {
  if (process.env.STALELOCK_TEST_TRACE_DIR) for (const directory of roots) {
    const entries: Record<string, string> = {};
    const snapshot = (relative: string): void => {
      for (const entry of fs.readdirSync(path.join(directory, relative), { withFileTypes: true })) {
        const name = path.join(relative, entry.name);
        if (entry.isDirectory()) { entries[`${name}/`] = "directory"; snapshot(name); }
        else entries[name] = fs.readFileSync(path.join(directory, name)).toString("base64");
      }
    };
    snapshot("");
    fs.writeFileSync(path.join(process.env.STALELOCK_TEST_TRACE_DIR, `${path.basename(directory)}.trace.json`), JSON.stringify({ root: directory, encoding: "base64", entries }));
  }
});
const nativeRead = fs.readFileSync;
const never = new Promise<never>(() => {});
beforeEach(() => { vi.spyOn(fs, "rmSync").mockImplementation(() => {}); });
afterEach(() => { vi.restoreAllMocks(); localFilesystemTestHooks.reset(); pidNamespaceTestHooks.reset(); });
type Protocol = 1 | 2 | 3 | undefined;
const acquire = async (directory: string, protocol: Protocol = undefined, hostQualified = false, timeout = 250): Promise<string> => {
  await acquireMeshCustodyLock(directory, timeout, { hostQualified, ...(protocol === undefined ? {} : { lockProtocol: protocol }) });
  return fs.readFileSync(path.join(directory, "custody.lock", "owner"), "utf8");
};
const deadPid = (): number => {
  const pid = Number(fs.readFileSync("/proc/sys/kernel/pid_max", "utf8").trim()) + 1;
  expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" })); return pid;
};
const change = (owner: string, reason: string): string => {
  const fields = owner.split("\n");
  if (reason === "other-boot") {
    fields[5] = fields[5] === "ffffffff-ffff-ffff-ffff-ffffffffffff" ? "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" : "ffffffff-ffff-ffff-ffff-ffffffffffff";
    const ns = fields[6]!.split(":"); ns[2] = fields[5]; fields[6] = ns.join(":");
  } else if (reason === "other-namespace") {
    const ns = fields[6]!.split(":"); ns[3] = String(BigInt(ns[3]!) + 1n); fields[6] = ns.join(":");
  } else if (reason === "dead-pid") fields[1] = String(deadPid());
  else if (reason === "reused-pid") fields[3] = String(BigInt(fields[3]!) + 1n);
  return fields.join("\n");
};
const plant = (directory: string, owner: string): void => {
  fs.mkdirSync(path.join(directory, "custody.lock"), { recursive: true });
  fs.writeFileSync(path.join(directory, "custody.lock", "owner"), owner, { mode: 0o600 });
};
const retained = (directory: string, owner: string, reason?: string): string => {
  const asides = fs.readdirSync(directory, { withFileTypes: true }).filter(entry => entry.isDirectory() && entry.name.startsWith("custody.lock.dead."));
  expect(asides).toHaveLength(1);
  const aside = path.join(directory, asides[0]!.name);
  expect(fs.readFileSync(path.join(aside, "owner"), "utf8")).toBe(owner);
  if (reason) {
    expect(asides[0]!.name).toMatch(/^custody\.lock\.dead\.\d{4}-\d\d-\d\dT.+\.[a-f0-9]{64}$/);
    expect(JSON.parse(fs.readFileSync(`${aside}.recovery.json`, "utf8"))).toMatchObject({
      kind: "stale-lock-observation", source: path.join(directory, "custody.lock"), aside, reason, owner,
    });
  }
  return aside;
};
const leaseSetup = (protocol: Protocol = undefined) => {
  const directory = root();
  const mesh = new MeshStore(directory, 65_536, 100, { stateBackend: "file", ...(protocol === undefined ? {} : { lockProtocol: protocol }) });
  const file = hostLeasePath(directory, "restart-regression-host");
  const domain = path.join(directory, "host-lease-commits", createHash("sha256").update(path.basename(file)).digest("hex").slice(0, 16));
  const hold = (timeout = 250): Promise<string> => new Promise((resolve, reject) => {
    void mesh.leaseCustody(file, async () => {
      resolve(fs.readFileSync(path.join(domain, "custody.lock", "owner"), "utf8")); return never;
    }, timeout).catch(reject);
  });
  return { directory, mesh, file, domain, hold };
};

describe.skipIf(process.platform !== "linux")("restart-aware custody locks (real filesystem; traces retained)", () => {
  it.each([undefined, 1, 2, 3] as const)("protocol %s writes complete owners for ordinary AND host-qualified custody", async protocol => {
    for (const qualified of [false, true]) {
      const owner = await acquire(root(), protocol, qualified), fields = owner.split("\n");
      const boot = nativeRead("/proc/sys/kernel/random/boot_id", "utf8").trim();
      const inode = /^pid:\[(\d+)\]$/.exec(fs.readlinkSync("/proc/self/ns/pid"))![1];
      const stat = nativeRead(`/proc/${process.pid}/stat`, "utf8");
      expect(fields).toHaveLength(9); expect(fields[7]).toBe("fabric-lock-owner-v1");
      expect(fields[1]).toBe(String(process.pid)); expect(fields[5]).toBe(boot);
      expect(fields[6]).toMatch(new RegExp(`^linux:[^:]+:${boot}:${inode}$`));
      expect(fields[3]).toBe(stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]);
      expect([4, 5, 6, 7, 8]).not.toContain(fields.length);
    }
  });

  it.each([undefined, 1, 2, 3] as const)("protocol %s recovers all stale cases through REAL MeshStore.leaseCustody", async protocol => {
    const source = await leaseSetup(protocol).hold();
    localFilesystemTestHooks.set(0x794c7630); // PV/overlay is NOT a veto for the complete new format.
    for (const reason of ["other-boot", "other-namespace", "dead-pid", "reused-pid"]) {
      const s = leaseSetup(protocol), owner = change(source, reason); plant(s.domain, owner);
      const winner = await s.hold(1_000);
      expect(s.domain).toMatch(/host-lease-commits[/\\][a-f0-9]{16}$/);
      expect(winner).not.toBe(owner); retained(s.domain, owner, reason);
      expect(fs.readFileSync(path.join(s.domain, "custody.lock", "owner"), "utf8")).toBe(winner);
    }
  });

  it.each([undefined, 1, 2, 3] as const)("protocol %s never steals the live same-start host-commit holder", async protocol => {
    const s = leaseSetup(protocol), owner = await s.hold();
    const old = new Date(Date.now() - 86_400_000); fs.utimesSync(path.join(s.domain, "custody.lock"), old, old);
    await expect(s.hold(60)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(path.join(s.domain, "custody.lock", "owner"), "utf8")).toBe(owner);
    expect(fs.readdirSync(s.domain).filter(name => name.startsWith("custody.lock.dead."))).toEqual([]);
  });

  it("two simultaneous custody takeovers have exactly one winner while it holds", async () => {
    const directory = root(), owner = change(await acquire(root(), 3), "other-namespace"); plant(directory, owner);
    const results = await Promise.allSettled([acquire(directory, 3, false, 150), acquire(directory, 3, false, 150)]);
    expect(results.filter(result => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter(result => result.status === "rejected")).toHaveLength(1);
    const winner = results.find(result => result.status === "fulfilled") as PromiseFulfilledResult<string>;
    retained(directory, owner, "other-namespace");
    expect(fs.readFileSync(path.join(directory, "custody.lock", "owner"), "utf8")).toBe(winner.value);
  });

  it.each([false, true])("the deterministic fence stops a paused cleaner from detaching a replacement (incomplete=%s)", async incomplete => {
    const directory = root(), fields = change(await acquire(root(), 3), "dead-pid").split("\n");
    if (incomplete) fields[3] = "";
    const owner = fields.join("\n"); plant(directory, owner);
    const replacement = await acquire(root(), 3), lock = path.join(directory, "custody.lock"), rename = fs.renameSync;
    let raced = false;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (!raced && String(from) === lock && String(to).startsWith(`${lock}.dead.`)) {
        raced = true; rename(from, to); fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), replacement);
        vi.spyOn(Date, "now").mockReturnValue(Date.now() + 86_400_000);
      }
      return rename(from, to);
    });
    await expect(acquire(directory, incomplete ? 1 : 3, false, 0)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(raced).toBe(true); retained(directory, owner, "dead-pid");
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(replacement);
  });

  it("retains legacy dead-owner recovery with a warning", async () => {
    const directory = root(), owner = `legacy\n${deadPid()}\n${Date.now()}\n`; plant(directory, owner);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await acquire(directory, 1);
    expect(warn.mock.calls.flat().join("\n")).toMatch(/legacy lock owner.*legacy recovery policy/);
    retained(directory, owner);
  });

  it.each(["boot", "start"] as const)("production-written denied %s identity preserves other known host-qualified metadata and degraded recovery", async denied => {
    vi.resetModules();
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      const file = String(args[0]);
      if ((denied === "boot" && file === "/proc/sys/kernel/random/boot_id") ||
        (denied === "start" && /^\/proc\/\d+\/stat$/.test(file))) throw Object.assign(new Error("identity denied"), { code: "EACCES" });
      return nativeRead(...args);
    });
    const { acquireMeshCustodyLock: degradedAcquire } = await import("../src/mesh/custody-lock.js");
    const directory = root(); await degradedAcquire(directory, 0, { hostQualified: true, lockProtocol: 2 });
    const lock = path.join(directory, "custody.lock"), ownerPath = path.join(lock, "owner");
    const fields = fs.readFileSync(ownerPath, "utf8").split("\n");
    expect(fields[7]).toBe("fabric-lock-owner-v1"); expect(fields[4]).toMatch(/^[a-f0-9]{32}$/);
    if (denied === "boot") { expect(fields[3]).toMatch(/^\d+$/); expect(fields[5]).toBe(""); expect(fields[6]).toBe(""); }
    else { expect(fields[3]).toBe(""); expect(fields[5]).toBe(nativeRead("/proc/sys/kernel/random/boot_id", "utf8").trim()); expect(fields[6]).toMatch(/^linux:/); }
    fields[1] = String(deadPid()); const owner = fields.join("\n"); fs.writeFileSync(ownerPath, owner);
    if (denied === "boot") {
      const old = new Date(Date.now() - CUSTODY_LOCK_STALE_MS - 5_000);
      fs.utimesSync(ownerPath, old, old); fs.utimesSync(lock, old, old);
    }
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await degradedAcquire(directory, 0, { hostQualified: true, lockProtocol: 2 });
    retained(directory, owner, "dead-pid"); expect(warn.mock.calls.flat().join("\n")).toMatch(/legacy lock owner/);
  });

  it("a production-written INCOMPLETE host-commit owner keeps the machine/age/ESRCH fallback and warns", async () => {
    vi.resetModules();
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      const file = String(args[0]);
      if (file === "/proc/sys/kernel/random/boot_id" || /^\/proc\/\d+\/stat$/.test(file)) throw Object.assign(new Error("identity denied"), { code: "EACCES" });
      return nativeRead(...args);
    });
    const { MeshStore: DegradedStore } = await import("../src/mesh/store.js");
    const directory = root(), mesh = new DegradedStore(directory, 65_536, 100, { stateBackend: "file", lockProtocol: 2 });
    const file = hostLeasePath(directory, "degraded-restart-host");
    const domain = path.join(directory, "host-lease-commits", createHash("sha256").update(path.basename(file)).digest("hex").slice(0, 16));
    const produced = await new Promise<string>((resolve, reject) => {
      void mesh.leaseCustody(file, async () => { resolve(fs.readFileSync(path.join(domain, "custody.lock", "owner"), "utf8")); return never; }, 0,
        { ownIncarnation: undefined }).catch(reject);
    });
    const fields = produced.split("\n");
    expect(fields).toHaveLength(9); expect(fields[7]).toBe("fabric-lock-owner-v1");
    expect(fields[3]).toBe(""); expect(fields[5]).toBe(""); expect(fields[6]).toBe("");
    expect(fields[4]).toMatch(/^[a-f0-9]{32}$/); // Known machine locality survived the denied boot read.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const old = new Date(Date.now() - CUSTODY_LOCK_STALE_MS - 5_000), lock = path.join(domain, "custody.lock");
    fs.utimesSync(path.join(lock, "owner"), old, old); fs.utimesSync(lock, old, old);
    await expect(mesh.leaseCustody(file, () => "must not enter", 0, { ownIncarnation: undefined })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(produced); // Even ancient incomplete LIVE owners stay.
    fields[1] = String(deadPid()); const dead = fields.join("\n"); fs.writeFileSync(path.join(lock, "owner"), dead);
    await expect(mesh.leaseCustody(file, () => "must wait for owner age", 0, { ownIncarnation: undefined })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    fs.utimesSync(path.join(lock, "owner"), old, old); fs.utimesSync(lock, old, old);
    const winner = await new Promise<string>((resolve, reject) => {
      void mesh.leaseCustody(file, async () => { resolve(fs.readFileSync(path.join(lock, "owner"), "utf8")); return never; }, 0,
        { ownIncarnation: undefined }).catch(reject);
    });
    expect(winner).not.toBe(dead); retained(domain, dead, "dead-pid");
    expect(warn.mock.calls.flat().join("\n")).toMatch(/legacy lock owner/);
  });
});
