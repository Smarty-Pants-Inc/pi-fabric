import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as atomicWrite from "../src/core/atomic-write.js";
import { lockIdentityVerdict, readLockIdentity, readLockIdentitySync } from "../src/core/lock-identity.js";
import * as namespaces from "../src/core/pid-namespace.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { holdMeshFenceSync, MeshFenceBusyError } from "../src/mesh/fence-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

// Data-safety regression for smarty-dev#17082: inspect BOTH actual synchronous fence owners, then feed
// their bytes to the real async acquirers. Record cleanup syscalls instead of deleting probe evidence.
const roots: string[] = [];
const root = (): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fence-identity-"));
  roots.push(directory);
  return directory;
};
const nativeRead = fs.readFileSync;
const never = new Promise<never>(() => {});
const names = ["custody.lock", ".lock"] as const;
type Name = typeof names[number];
const ownerAt = (directory: string, name: Name): string => fs.readFileSync(path.join(directory, name, "owner"), "utf8");
const capture = (): Record<Name, string> => {
  const directory = root();
  return holdMeshFenceSync(directory, 0, () => Object.fromEntries(names.map(name => [name, ownerAt(directory, name)])) as Record<Name, string>);
};
beforeEach(() => {
  namespaces.pidNamespaceTestHooks.resetReader();
  vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  vi.spyOn(fs, "unlinkSync").mockImplementation(() => {});
  vi.spyOn(fs, "rmdirSync").mockImplementation(() => {});
});
afterEach(() => { vi.restoreAllMocks(); namespaces.pidNamespaceTestHooks.resetReader(); });
afterAll(() => {
  if (!process.env.STALELOCK_TEST_TRACE_DIR) return;
  for (const directory of roots) {
    const entries: Record<string, string> = {};
    const snapshot = (relative: string): void => {
      for (const entry of fs.readdirSync(path.join(directory, relative), { withFileTypes: true })) {
        const name = path.join(relative, entry.name);
        if (entry.isDirectory()) { entries[`${name}/`] = "directory"; snapshot(name); }
        else entries[name] = fs.readFileSync(path.join(directory, name)).toString("base64");
      }
    };
    snapshot("");
    fs.writeFileSync(path.join(process.env.STALELOCK_TEST_TRACE_DIR, `${path.basename(directory)}.trace.json`),
      JSON.stringify({ root: directory, encoding: "base64", entries }));
  }
});
const holdAsync = async (directory: string, name: Name): Promise<string> => {
  if (name === "custody.lock") {
    await acquireMeshCustodyLock(directory, 1_000); // Keep the successful holder (and all evidence) until worker exit.
    return ownerAt(directory, name);
  }
  return new Promise((resolve, reject) => {
    void new MeshLock(directory, { lockProtocol: 1 }, () => {}).withLockAcrossAwait(async () => {
      resolve(ownerAt(directory, name));
      return never;
    }, 1_000).catch(reject);
  });
};
const stale = (owner: string, reason: string): string => {
  const fields = owner.split("\n");
  if (reason === "other-boot") {
    fields[5] = fields[5] === "ffffffff-ffff-ffff-ffff-ffffffffffff" ? "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" : "ffffffff-ffff-ffff-ffff-ffffffffffff";
    const namespace = fields[6]!.split(":"); namespace[2] = fields[5]; fields[6] = namespace.join(":");
  } else if (reason === "other-namespace") {
    const namespace = fields[6]!.split(":"); namespace[3] = String(BigInt(namespace[3]!) + 1n); fields[6] = namespace.join(":");
  } else if (reason === "reused-pid") fields[3] = String(BigInt(fields[3]!) + 1n);
  else {
    const pid = Number(nativeRead("/proc/sys/kernel/pid_max", "utf8").trim()) + 1;
    expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
    fields[1] = String(pid);
  }
  return fields.join("\n");
};
const plant = (directory: string, name: Name, owner: string): void => {
  fs.mkdirSync(path.join(directory, name));
  fs.writeFileSync(path.join(directory, name, "owner"), owner, { mode: 0o600 });
};

describe.skipIf(process.platform !== "linux")("synchronous mesh fence restart identity (native evidence; retained traces)", () => {
  it("writes the same complete nine-field wire to BOTH owners while the real callback holds, excluding a nested holder", () => {
    const directory = root();
    const boot = nativeRead("/proc/sys/kernel/random/boot_id", "utf8").trim();
    const inode = /^pid:\[(\d+)\]$/.exec(fs.readlinkSync("/proc/self/ns/pid"))![1];
    const stat = nativeRead(`/proc/${process.pid}/stat`, "utf8");
    const start = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
    let machine = "-";
    try { const id = nativeRead("/etc/machine-id", "utf8").trim(); if (/^[a-f0-9]{32}$/.test(id)) machine = id; } catch { /* canonical unknown */ }
    const commands = [vi.spyOn(childProcess, "execFile"), vi.spyOn(childProcess, "execFileSync"),
      vi.spyOn(childProcess, "spawn"), vi.spyOn(childProcess, "spawnSync")];
    const result = holdMeshFenceSync(directory, 0, () => {
      const owners = names.map(name => ownerAt(directory, name));
      for (const owner of owners) {
        const fields = owner.split("\n");
        expect(fields).toHaveLength(9);
        expect(fields[0]).toMatch(/^[a-f0-9-]{36}$/);
        expect(fields[1]).toBe(String(process.pid));
        expect(Number(fields[2])).toBeGreaterThan(0);
        expect(fields.slice(3)).toEqual([start, "", boot, `linux:${machine}:${boot}:${inode}`, "fabric-lock-owner-v1", ""]);
      }
      expect(owners[0]!.split("\n")[0]).not.toBe(owners[1]!.split("\n")[0]);
      expect(() => holdMeshFenceSync(directory, 0, () => { throw new Error("nested holder entered"); })).toThrow(MeshFenceBusyError);
      expect(names.map(name => ownerAt(directory, name))).toEqual(owners);
      return "held-result";
    });
    expect(result).toBe("held-result");
    for (const command of commands) expect(command).not.toHaveBeenCalled();
  });

  it("uses the async reader's exact namespace spelling so noncanonical machine IDs cannot make a live sync holder stale", async () => {
    namespaces.pidNamespaceTestHooks.resetReader();
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      if (String(args[0]) === "/etc/machine-id") return "ABCDEF0123456789ABCDEF0123456789\n";
      return nativeRead(...args);
    });
    const identity = readLockIdentitySync();
    expect(identity.namespace).toBe(await namespaces.ownPidNamespace());
    expect(identity.namespace).toMatch(/^linux:-:/);
    expect(await lockIdentityVerdict(identity, Date.now() + 1_000)).toBe("live");
    const otherBoot = identity.bootId === "ffffffff-ffff-ffff-ffff-ffffffffffff" ? "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" : "ffffffff-ffff-ffff-ffff-ffffffffffff";
    const changed = { ...identity, bootId: otherBoot, namespace: identity.namespace!.replace(identity.bootId!, otherBoot) };
    expect(await lockIdentityVerdict(changed, Date.now() + 1_000)).toBe("other-boot");
  });

  it("shares the prepared immutable namespace even if machine-id changes before a synchronous acquisition", async () => {
    namespaces.pidNamespaceTestHooks.resetReader();
    const prepared = await readLockIdentity();
    const machine = prepared.namespace!.split(":")[1] === "a".repeat(32) ? "b".repeat(32) : "a".repeat(32);
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) =>
      String(args[0]) === "/etc/machine-id" ? `${machine}\n` : nativeRead(...args));
    const synchronous = readLockIdentitySync();
    expect(synchronous.namespace).toBe(prepared.namespace);
    expect(await lockIdentityVerdict(synchronous, Date.now() + 1_000)).toBe("live");
  });

  it.each(["boot", "start", "namespace", "machine"])("denied %s evidence stays explicit (canonical unknown machine is '-'), with a legacy warning when incomplete", denied => {
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      const file = String(args[0]);
      if ((denied === "boot" && file === "/proc/sys/kernel/random/boot_id") ||
        (denied === "start" && file === `/proc/${process.pid}/stat`) ||
        (denied === "machine" && file === "/etc/machine-id")) throw Object.assign(new Error("identity denied"), { code: "EACCES" });
      return nativeRead(...args);
    });
    if (denied === "namespace") vi.spyOn(fs, "readlinkSync").mockImplementation(() => { throw Object.assign(new Error("namespace denied"), { code: "EACCES" }); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    for (const owner of Object.values(capture())) {
      const fields = owner.split("\n");
      expect(fields).toHaveLength(9); expect(fields[7]).toBe("fabric-lock-owner-v1");
      expect(fields[3] === "").toBe(denied === "start");
      expect(fields[5] === "").toBe(denied === "boot");
      expect(fields[6] === "").toBe(denied === "boot" || denied === "namespace");
      if (denied === "machine") expect(fields[6]).toMatch(/^linux:-:/);
    }
    if (denied !== "machine") expect(warn.mock.calls.flat().join("\n")).toMatch(/legacy lock owner.*legacy recovery policy/);
    else expect(warn).not.toHaveBeenCalled();
  });

  for (const name of names) {
    it.each(["other-boot", "other-namespace", "dead-pid", "reused-pid"])(`async ${name} recovers a stale sync %s owner, retaining exact bytes and JSON`, async reason => {
      const source = capture()[name];
      expect(source.split("\n")).toHaveLength(9);
      const owner = stale(source, reason), directory = root();
      plant(directory, name, owner);
      const winner = await holdAsync(directory, name);
      expect(winner).not.toBe(owner);
      expect(ownerAt(directory, name)).toBe(winner);
      const asides = fs.readdirSync(directory, { withFileTypes: true }).filter(entry => entry.isDirectory() && entry.name.startsWith(`${name}.dead.`));
      expect(asides).toHaveLength(1);
      expect(asides[0]!.name).toMatch(/\.dead\.\d{4}-\d\d-\d\dT.+\.[a-f0-9]{64}$/);
      const aside = path.join(directory, asides[0]!.name);
      expect(fs.readFileSync(path.join(aside, "owner"), "utf8")).toBe(owner);
      expect(JSON.parse(fs.readFileSync(`${aside}.recovery.json`, "utf8"))).toMatchObject({
        kind: "stale-lock-observation", source: path.join(directory, name), aside, reason, owner,
        identity: { pid: Number(owner.split("\n")[1]), start: owner.split("\n")[3], bootId: owner.split("\n")[5], namespace: owner.split("\n")[6] },
      });
    });

    it(`the synchronous ${name} acquirer does NOT recover a provably stale holder`, () => {
      const owner = stale(capture()[name], "other-boot"), directory = root();
      plant(directory, name, owner);
      expect(() => holdMeshFenceSync(directory, 0, () => { throw new Error("stale recovery entered"); })).toThrow(MeshFenceBusyError);
      expect(ownerAt(directory, name)).toBe(owner);
      expect(fs.readdirSync(directory).filter(entry => entry.includes(".dead."))).toEqual([]);
    });
  }
});

describe("synchronous fence prepared identity and idle imports", () => {
  it.each(["darwin", "win32"] as const)("reuses prepared %s identity without starting ANY native command, and retains UNKNOWN", async platform => {
    const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    const boot = platform === "darwin" ? "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" : "123456789";
    const namespace = `${platform}:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:${boot}:host`;
    const start = platform === "darwin" ? "darwin:Wed Oct 28 12:00:00 2026" : "win32:987654321";
    const knownNamespace = vi.spyOn(namespaces, "ownPidNamespaceSync").mockReturnValue(namespace);
    const knownStart = vi.spyOn(atomicWrite, "knownOwnProcessIncarnation").mockReturnValue(start);
    await readLockIdentity({ start, prepared: true }); // Populate the lazy module handle on an ordinary prepared path.
    const commands = [vi.spyOn(childProcess, "execFile"), vi.spyOn(childProcess, "execFileSync"),
      vi.spyOn(childProcess, "spawn"), vi.spyOn(childProcess, "spawnSync")];
    Object.defineProperty(process, "platform", { ...descriptor, value: platform });
    try {
      expect(readLockIdentitySync()).toEqual({ pid: process.pid, start, bootId: boot, namespace });
      for (const owner of Object.values(capture())) expect(owner.split("\n").slice(3)).toEqual([start, "", boot, namespace, "fabric-lock-owner-v1", ""]);
      knownNamespace.mockReturnValue(undefined); knownStart.mockReturnValue(undefined);
      expect(readLockIdentitySync()).toEqual({ pid: process.pid, start: undefined, bootId: undefined, namespace: undefined });
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      for (const owner of Object.values(capture())) expect(owner.split("\n").slice(3)).toEqual(["", "", "", "", "fabric-lock-owner-v1", ""]);
      expect(warn.mock.calls.flat().join("\n")).toMatch(/legacy lock owner/);
      for (const command of commands) expect(command).not.toHaveBeenCalled();
    } finally { Object.defineProperty(process, "platform", descriptor); }
  });

  it("cold import does not read native identity or start a child before acquisition", async () => {
    vi.resetModules();
    const reads = vi.spyOn(fs, "readFileSync"), links = vi.spyOn(fs, "readlinkSync");
    const commands = [vi.spyOn(childProcess, "execFile"), vi.spyOn(childProcess, "execFileSync"),
      vi.spyOn(childProcess, "spawn"), vi.spyOn(childProcess, "spawnSync")];
    await import("../src/mesh/fence-lock.js");
    expect(reads.mock.calls.filter(call => /^\/proc\//.test(String(call[0])) || String(call[0]) === "/etc/machine-id")).toEqual([]);
    expect(links.mock.calls.filter(call => String(call[0]) === "/proc/self/ns/pid")).toEqual([]);
    for (const command of commands) expect(command).not.toHaveBeenCalled();
  });
});
