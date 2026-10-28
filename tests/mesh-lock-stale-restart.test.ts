import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { localFilesystemTestHooks, pidNamespaceTestHooks } from "../src/core/pid-namespace.js";

// Data-safety regression for smarty-dev#17082: real directories/owners/renames, no agent or child process.
// Hold successful acquisitions until worker exit. Record (do not execute) cleanup calls, retaining ALL probe traces.
const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-stale-restart-")); roots.push(directory); return directory; };
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
beforeEach(() => { vi.spyOn(fs, "rmSync").mockImplementation(() => {}); });
afterEach(() => { vi.restoreAllMocks(); localFilesystemTestHooks.reset(); pidNamespaceTestHooks.reset(); });
const never = new Promise<never>(() => {});
type Protocol = 1 | 2 | 3 | undefined;
const hold = async (directory: string, protocol: Protocol = undefined, timeout = 250): Promise<string> => {
  const lock = new MeshLock(directory, { ...(protocol === undefined ? {} : { lockProtocol: protocol }) }, () => {});
  await lock.ownIncarnation; // Explicit short acquisition retains the production preparation barrier.
  return new Promise((resolve, reject) => {
    void lock.withLockAcrossAwait(async () => {
      resolve(fs.readFileSync(path.join(directory, ".lock", "owner"), "utf8"));
      return never;
    }, timeout).catch(reject);
  });
};
const deadPid = (): number => {
  const pid = Number(fs.readFileSync("/proc/sys/kernel/pid_max", "utf8").trim()) + 1;
  expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
  return pid;
};
const plant = (directory: string, owner: string): void => {
  fs.mkdirSync(path.join(directory, ".lock"));
  fs.writeFileSync(path.join(directory, ".lock", "owner"), owner, { mode: 0o600 });
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
const retained = (directory: string, owner: string, reason: string): string => {
  const entries = fs.readdirSync(directory, { withFileTypes: true });
  const asides = entries.filter(entry => entry.isDirectory() && entry.name.startsWith(".lock.dead."));
  expect(asides).toHaveLength(1);
  expect(asides[0]!.name).toMatch(/^\.lock\.dead\.\d{4}-\d\d-\d\dT.+\.[a-f0-9]{64}$/);
  const aside = path.join(directory, asides[0]!.name);
  expect(fs.readFileSync(path.join(aside, "owner"), "utf8")).toBe(owner);
  expect(JSON.parse(fs.readFileSync(`${aside}.recovery.json`, "utf8"))).toMatchObject({
    kind: "stale-lock-observation", source: path.join(directory, ".lock"), aside, reason, owner,
  });
  return aside;
};

describe.skipIf(process.platform !== "linux")("restart-aware mesh lock (real filesystem; traces retained)", () => {
  it.each([undefined, 1, 2, 3] as const)("protocol %s writes all identity fields and excludes every older parser", async protocol => {
    const directory = root(), fields = (await hold(directory, protocol)).split("\n");
    const boot = nativeRead("/proc/sys/kernel/random/boot_id", "utf8").trim();
    const inode = /^pid:\[(\d+)\]$/.exec(fs.readlinkSync("/proc/self/ns/pid"))![1];
    const stat = nativeRead(`/proc/${process.pid}/stat`, "utf8");
    expect(fields).toHaveLength(9); expect(fields[7]).toBe("fabric-lock-owner-v1");
    expect(fields[1]).toBe(String(process.pid)); expect(fields[5]).toBe(boot);
    expect(fields[6]).toMatch(new RegExp(`^linux:[^:]+:${boot}:${inode}$`));
    expect(fields[3]).toBe(stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]);
    expect([4, 5, 6, 7, 8]).not.toContain(fields.length);
  });

  it.each([undefined, 1, 2, 3] as const)("protocol %s recovers each explicit stale case, including PV/overlay", async protocol => {
    const source = await hold(root(), protocol);
    // A new complete receipt deliberately bypasses the OLD protocol-3 filesystem gate.
    localFilesystemTestHooks.set(0x794c7630);
    for (const reason of ["other-boot", "other-namespace", "dead-pid", "reused-pid"]) {
      const directory = root(), owner = change(source, reason); plant(directory, owner);
      const winner = await hold(directory, protocol, 1_000);
      expect(winner).not.toBe(owner); retained(directory, owner, reason);
      expect(fs.readFileSync(path.join(directory, ".lock", "owner"), "utf8")).toBe(winner);
    }
  });

  it.each([undefined, 1, 2, 3] as const)("protocol %s never steals a live same-start holder, however old", async protocol => {
    const directory = root(), owner = await hold(directory, protocol);
    const old = new Date(Date.now() - 86_400_000);
    fs.utimesSync(path.join(directory, ".lock"), old, old);
    await expect(hold(directory, protocol, 60)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(path.join(directory, ".lock", "owner"), "utf8")).toBe(owner);
    expect(fs.readdirSync(directory).filter(name => name.startsWith(".lock.dead."))).toEqual([]);
  });

  it("two simultaneous takeovers have exactly one winner while that winner holds", async () => {
    const directory = root(), owner = change(await hold(root(), 3), "other-boot"); plant(directory, owner);
    const results = await Promise.allSettled([hold(directory, 3, 150), hold(directory, 3, 150)]);
    expect(results.filter(result => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter(result => result.status === "rejected")).toHaveLength(1);
    const winner = results.find(result => result.status === "fulfilled") as PromiseFulfilledResult<string>;
    retained(directory, owner, "other-boot");
    expect(fs.readFileSync(path.join(directory, ".lock", "owner"), "utf8")).toBe(winner.value);
  });

  it.each([false, true])("a paused cleaner cannot detach a live replacement across a date change (incomplete=%s)", async incomplete => {
    const directory = root(), fields = change(await hold(root(), 3), "dead-pid").split("\n");
    if (incomplete) fields[3] = "";
    const owner = fields.join("\n"); plant(directory, owner);
    const replacement = await hold(root(), 3), lockPath = path.join(directory, ".lock"), rename = fs.renameSync;
    let raced = false;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (!raced && String(from) === lockPath && String(to).startsWith(`${lockPath}.dead.`)) {
        raced = true;
        // The competing cleaner wins the original rename, then a live successor publishes before ours resumes.
        rename(from, to); fs.mkdirSync(lockPath); fs.writeFileSync(path.join(lockPath, "owner"), replacement);
        vi.spyOn(Date, "now").mockReturnValue(Date.now() + 86_400_000);
      }
      return rename(from, to);
    });
    const lock = new MeshLock(directory, { lockProtocol: incomplete ? 1 : 3 }, () => {});
    expect(await lock.clearStaleLock(path.join(lockPath, "owner"), Date.now() + 1_000)).toBe(false);
    expect(raced).toBe(true); retained(directory, owner, "dead-pid");
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe(replacement);
  });

  it("checks expected.namespace and retires dependent receipts BEFORE detaching a new-format owner", async () => {
    const directory = root(), owner = change(await hold(root(), 3), "dead-pid"); plant(directory, owner);
    const lock = new MeshLock(directory, { lockProtocol: 3 }, () => {}), ownerPath = path.join(directory, ".lock", "owner");
    const beforeRename = vi.fn(() => { expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner); fs.writeFileSync(path.join(directory, "retired-ticket"), "retired\n"); });
    expect(await lock.clearStaleLock(ownerPath, Date.now() + 1_000, {}, { owner, namespace: "wrong", beforeRename })).toBe(false);
    expect(beforeRename).not.toHaveBeenCalled();
    expect(await lock.clearStaleLock(ownerPath, Date.now() + 1_000, {}, { owner, namespace: owner.split("\n")[6]!, beforeRename })).toBe(true);
    expect(beforeRename).toHaveBeenCalledOnce(); expect(fs.readFileSync(path.join(directory, "retired-ticket"), "utf8")).toBe("retired\n");
    retained(directory, owner, "dead-pid");
  });

  it("bounded acquisitions reuse a protocol-3 namespace preparation that settled UNKNOWN", async () => {
    const namespaces = await import("../src/core/pid-namespace.js");
    const own = vi.spyOn(namespaces, "ownPidNamespace").mockResolvedValue(undefined);
    vi.spyOn(namespaces, "ownPidNamespaceSync").mockReturnValue(undefined);
    const directory = root(), lock = new MeshLock(directory, { lockProtocol: 3 }, () => {});
    await lock.ownIncarnation;
    const failure = Object.assign(new Error("recorded publication refusal"), { code: "EROFS" });
    vi.spyOn(fs, "mkdtempSync").mockImplementation(() => { throw failure; });
    for (let attempt = 0; attempt < 2; attempt++) {
      await expect(lock.withTryLock(() => lock.withLock(() => "not entered"), 0)).rejects.toBe(failure);
    }
    expect(own).toHaveBeenCalledOnce();
  });

  it("retains legacy recovery behavior with a logged warning", async () => {
    const directory = root(), owner = `legacy\n${deadPid()}\n${Date.now()}\n`; plant(directory, owner);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await hold(directory, 1);
    expect(warn.mock.calls.flat().join("\n")).toMatch(/legacy lock owner.*legacy recovery policy/);
    const aside = fs.readdirSync(directory, { withFileTypes: true }).find(entry => entry.isDirectory() && entry.name.startsWith(".lock.dead."))!;
    expect(fs.readFileSync(path.join(directory, aside.name, "owner"), "utf8")).toBe(owner);
  });

  it.each(["boot", "start", "both"] as const)("production-written denied %s identity follows protocol-1 degraded recovery with a warning", async denied => {
    // Fresh production readers, so a previously memoized successful read cannot mask the injected read denial.
    vi.resetModules();
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
      const file = String(args[0]);
      if ((denied !== "start" && file === "/proc/sys/kernel/random/boot_id") ||
        (denied !== "boot" && /^\/proc\/\d+\/stat$/.test(file))) throw Object.assign(new Error("identity denied"), { code: "EACCES" });
      return nativeRead(...args);
    });
    const { MeshLock: DegradedLock } = await import("../src/mesh/mesh-lock.js");
    const directory = root();
    const produced = await new Promise<string>((resolve, reject) => {
      void new DegradedLock(directory, {}, () => {}).withLockAcrossAwait(async () => {
        resolve(fs.readFileSync(path.join(directory, ".lock", "owner"), "utf8")); return never;
      }).catch(reject);
    });
    expect(produced.split("\n")[7]).toBe("fabric-lock-owner-v1");
    if (denied !== "boot") expect(produced.split("\n")[3]).toBe("");
    else expect(produced.split("\n")[3]).toMatch(/^\d+$/);
    if (denied !== "start") expect(produced.split("\n")[5]).toBe("");
    else expect(produced.split("\n")[5]).toBe(nativeRead("/proc/sys/kernel/random/boot_id", "utf8").trim());
    const fields = produced.split("\n"); fields[1] = String(deadPid());
    fs.writeFileSync(path.join(directory, ".lock", "owner"), fields.join("\n"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const lock = new DegradedLock(directory, {}, () => {});
    expect(await lock.clearStaleLock(path.join(directory, ".lock", "owner"), Date.now() + 1_000)).toBe(true);
    expect(warn.mock.calls.flat().join("\n")).toMatch(/legacy lock owner/);
    retained(directory, fields.join("\n"), "dead-pid");
  });
});
