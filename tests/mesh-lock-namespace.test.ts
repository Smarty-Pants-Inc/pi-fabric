import childProcess from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshLockForeignHolderError, readPhysicalMachineId } from "../src/core/atomic-write.js";
import {
  createPidNamespaceReader, localFilesystemTestHooks, onLocalFilesystem, ownPidNamespace, pidNamespaceTestHooks, pidNamespaceVerdict,
  validPidNamespace,
} from "../src/core/pid-namespace.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

// smarty-dev#9569 option (c): a lock record of protocol 3 names its holder's PID namespace, and a reclaimer uses pid
// evidence (ESRCH, another incarnation) only for a holder of its own namespace. A container or VM sharing the mesh
// root sees ESRCH for a LIVE foreign holder; reclaiming it would let two writers into the section.
const roots: string[] = [];
// The roots of these tests are host-local (ext4) on every CI OS, so the namespace logic runs the same on macOS and
// Windows runners, whose real rule fails closed (pi-fabric#923 round 3); the local-filesystem tests override this.
beforeEach(() => { localFilesystemTestHooks.set(0xEF53); });
afterEach(() => {
  vi.restoreAllMocks();
  pidNamespaceTestHooks.reset();
  localFilesystemTestHooks.reset();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-ns-lock-"));
  roots.push(root);
  return root;
};
const MACHINE = "0123456789abcdef0123456789abcdef";
const BOOT = "5a5a5a5a-5a5a-4a5a-8a5a-5a5a5a5a5a5a";
const ns = (boot = BOOT, machine = MACHINE, pidns = 4026531836): string => `linux:${machine}:${boot}:${pidns}`;
/** A container on the same kernel: the same machine and boot, another PID namespace. */
const container = (pidns = 4026532999): string => ns(BOOT, MACHINE, pidns);
/** A pid that is certainly not running: a child that has exited. */
const deadPid = (): number => childProcess.spawnSync(process.execPath, ["-e", ""]).pid!;
/** The P1 view: this live pid answers ESRCH, as a holder in another PID namespace does. */
const looksDead = (pid: number) => {
  const kill = process.kill.bind(process);
  vi.spyOn(process, "kill").mockImplementation(((target: number, signal?: string | number) => {
    if (target === pid && (signal === 0 || signal === undefined)) throw Object.assign(new Error("kill ESRCH"), { code: "ESRCH" });
    return kill(target, signal as never);
  }) as typeof process.kill);
};
const meshRecord = (pid: number, namespace: string | null, start = "") => namespace === null
  ? `${randomUUID()}\n${pid}\n${Date.now()}\n` : `${randomUUID()}\n${pid}\n${Date.now()}\n${start}\n${namespace}\n`;
const plant = (lock: string, record: string): void => {
  fs.mkdirSync(lock, { mode: 0o700 });
  fs.writeFileSync(path.join(lock, "owner"), record, { mode: 0o600 });
};
const contend = (root: string, protocol: 1 | 2 | 3 = 3) =>
  new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 400 }, () => undefined).withLock(() => "entered");

describe("PID namespace verdicts (smarty-dev#9569)", () => {
  const own = ns("11111111-1111-4111-8111-111111111111");
  it.each([
    ["a legacy record (no namespace line): today's pid rule", null, own, "pid"],
    ["our own namespace", own, own, "pid"],
    ["another PID namespace on the same kernel (a container)", ns("11111111-1111-4111-8111-111111111111", MACHINE, 4026532999), own, "foreign"],
    ["another host, or a VM (another machine id)", ns("11111111-1111-4111-8111-111111111111", "fedcba9876543210fedcba9876543210"), own, "foreign"],
    ["another boot id on THIS machine id (a running VM clone looks the same): unproven", ns("22222222-2222-4222-8222-222222222222"), own, "unproven"],
    ["a Windows namespace, even an equal one (silos share it)", "win32:9f0b7c3e-1d2a-4b5c-8e7f-0a1b2c3d4e5f:1:host", "win32:9f0b7c3e-1d2a-4b5c-8e7f-0a1b2c3d4e5f:1:host", "unproven"],
    ["another boot, but our machine id is unknown: cannot prove it", ns("22222222-2222-4222-8222-222222222222"), ns("11111111-1111-4111-8111-111111111111", "-"), "foreign"],
    ["another machine, another boot", ns(randomUUID(), "fedcba9876543210fedcba9876543210"), own, "foreign"],
    ["a malformed record namespace", "linux:oops", own, "unproven"],
    ["an empty record namespace (its holder could not read its own)", "", own, "unproven"],
    ["our own namespace is unknown", own, undefined, "unproven"],
  ] as const)("%s", (_name, record, mine, verdict) => {
    expect(pidNamespaceVerdict(record, mine)).toBe(verdict);
  });

  it("reads this Linux process's namespace, and the darwin and Windows forms from their native commands", async () => {
    if (process.platform === "linux") expect(validPidNamespace(await ownPidNamespace(), "linux")).toBe(true);
    const darwin = createPidNamespaceReader({ platform: "darwin", run: async (executable) =>
      executable.endsWith("sysctl") ? "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE\n"
        : '+-o J316sAP  <class IOPlatformExpertDevice>\n  "IOPlatformUUID" = "12345678-1234-1234-1234-123456789ABC"\n' });
    expect(await darwin.own()).toBe("darwin:12345678-1234-1234-1234-123456789ABC:AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE:host");
    const windows = createPidNamespaceReader({ platform: "win32", systemRoot: "C:\\Windows",
      run: async () => "9f0b7c3e-1d2a-4b5c-8e7f-0a1b2c3d4e5f:638640000000000000\r\n" });
    expect(await windows.own()).toBe("win32:9f0b7c3e-1d2a-4b5c-8e7f-0a1b2c3d4e5f:638640000000000000:host");
    // Malformed output is UNKNOWN, never a guess.
    expect(await createPidNamespaceReader({ platform: "darwin", run: async () => "garbage" }).own()).toBeUndefined();
    expect(await createPidNamespaceReader({ platform: "win32", systemRoot: "C:\\Windows", run: async () => { throw new Error("no powershell"); } }).own()).toBeUndefined();
  });
});

describe("the mesh .lock under protocol 3 (smarty-dev#9569)", () => {
  it("writes a six-field record naming its namespace, which a reader from before protocol 3 never reclaims", async () => {
    const own = ns();
    pidNamespaceTestHooks.set(own);
    const root = tempRoot();
    let record = "";
    await new MeshLock(root, { lockProtocol: 3, lockTimeoutMs: 1_000 }, () => undefined)
      .withLock(() => { record = fs.readFileSync(path.join(root, ".lock", "owner"), "utf8"); });
    const fields = record.split("\n");
    expect(fields).toHaveLength(6);
    expect(fields[1]).toBe(String(process.pid));
    expect(fields[4]).toBe(own);
    // main's validOwner (mesh-lock.ts before protocol 3), verbatim: only 4 or 5 fields may be reclaimed.
    const oldValidOwner = (owner: string): boolean => {
      const old = owner.split("\n");
      const [token, pidText, createdText] = old;
      const pid = Number(pidText);
      return owner.endsWith("\n") && (old.length === 4 || old.length === 5) && !!token && Number.isSafeInteger(pid) && pid > 0 &&
        createdText !== undefined && createdText.trim() !== "" && Number.isFinite(Number(createdText));
    };
    expect(oldValidOwner(record)).toBe(false);
    expect(oldValidOwner(meshRecord(1234, null))).toBe(true);
    // ...and main's custody parser accepts only 4/5 (or 7, host-qualified) fields.
    expect([4, 5].includes(fields.length)).toBe(false);
  });

  it("never reclaims a LIVE holder of another namespace that looks dead from here (the P1 race); typed refusal", async () => {
    pidNamespaceTestHooks.set(ns());
    const root = tempRoot();
    const holder = childProcess.spawn(process.execPath, ["-e", "setTimeout(() => {}, 30000)"], { stdio: "ignore" });
    try {
      const record = meshRecord(holder.pid!, container());
      plant(path.join(root, ".lock"), record);
      looksDead(holder.pid!);
      const refused = await contend(root).catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(MeshLockForeignHolderError);
      expect((refused as MeshLockForeignHolderError).foreignCode).toBe("FABRIC_MESH_LOCK_FOREIGN_HOLDER");
      expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(record); // the holder keeps its lock
    } finally { holder.kill(); }
  });

  it("the same live holder in a LEGACY record IS reclaimed: today's bug, which protocol 3 removes (M5)", async () => {
    pidNamespaceTestHooks.set(ns());
    const root = tempRoot();
    const holder = childProcess.spawn(process.execPath, ["-e", "setTimeout(() => {}, 30000)"], { stdio: "ignore" });
    try {
      plant(path.join(root, ".lock"), meshRecord(holder.pid!, null));
      looksDead(holder.pid!);
      await expect(contend(root, 2)).resolves.toBe("entered");
    } finally { holder.kill(); }
  });

  it("reclaims a dead holder of OUR namespace at once (the counter-case); a dead legacy one only under protocol 1/2", async () => {
    const own = ns();
    pidNamespaceTestHooks.set(own);
    const root = tempRoot();
    plant(path.join(root, ".lock"), meshRecord(deadPid(), own));
    await expect(contend(root)).resolves.toBe("entered");
    const legacy = tempRoot();
    plant(path.join(legacy, ".lock"), meshRecord(deadPid(), null));
    await expect(contend(legacy, 2)).resolves.toBe("entered");
  });

  it("pi-fabric#923: under protocol 3 a LIVE legacy holder that looks dead is NOT reclaimed, automatically or by the safe lock-recover; the unsafe override reclaims it with an audit line", async () => {
    pidNamespaceTestHooks.set(ns());
    const root = tempRoot();
    const holder = childProcess.spawn(process.execPath, ["-e", "setTimeout(() => {}, 30000)"], { stdio: "ignore" });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const record = meshRecord(holder.pid!, null);
      plant(path.join(root, ".lock"), record);
      looksDead(holder.pid!);
      const refused = await contend(root).catch((error: unknown) => error) as MeshLockForeignHolderError;
      expect(refused.foreignCode).toBe("FABRIC_MESH_LOCK_UNPROVEN_HOLDER");
      // The error and the ALARM line name the lock and the exact recovery command.
      const token = record.split("\n")[0]!;
      expect(refused.recoveryArgv).toEqual(["fabric-mesh-backend", "lock-recover", "--root", root, "--lock", "mesh", "--holder", token]);
      expect(refused.recovery).toContain(`--lock mesh --holder ${token}`);
      expect(refused.message).toContain(path.join(root, ".lock"));
      expect(warn.mock.calls.flat().join("\n")).toMatch(/\[pi-fabric\] ALARM FABRIC_MESH_LOCK_UNPROVEN_HOLDER: .*lock-recover/);
      expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(record);
      const { recoverMeshLock, MeshLockRecoverRefusedError } = await import("../src/mesh/lock-recover.js");
      await expect(recoverMeshLock(root, { lock: "mesh", holder: token, alive: () => false })).rejects.toBeInstanceOf(MeshLockRecoverRefusedError);
      expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(record);
      const forced = await recoverMeshLock(root, { lock: "mesh", holder: token, unsafeHolderStopped: true });
      expect(forced).toMatchObject({ mode: "unsafe", verdict: "unproven" });
      const audit = JSON.parse(fs.readFileSync(path.join(root, "lock-recoveries.jsonl"), "utf8").trim()) as { mode: string; holder: string; operator: Record<string, unknown> };
      expect(audit).toMatchObject({ mode: "unsafe", holder: token, operator: { pid: process.pid, ppid: process.ppid, host: os.hostname() } });
      expect(audit.operator.user).toEqual(expect.any(String));
      expect(audit.operator.parentExecutable).toEqual(expect.any(String));
      expect(audit.operator).not.toHaveProperty("parentCommand");
    } finally { holder.kill(); }
  });

  it("pi-fabric#923 round 2: a running VM clone (the same machine id, another boot id) whose pid is ESRCH here is NOT reclaimed", async () => {
    pidNamespaceTestHooks.set(ns("33333333-3333-4333-8333-333333333333"));
    const root = tempRoot();
    const clone = childProcess.spawn(process.execPath, ["-e", "setTimeout(() => {}, 30000)"], { stdio: "ignore" });
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const record = meshRecord(clone.pid!, ns("44444444-4444-4444-8444-444444444444"));
      plant(path.join(root, ".lock"), record);
      looksDead(clone.pid!);
      await expect(contend(root)).rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
      expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(record);
    } finally { clone.kill(); }
  });

  it.each([
    ["overlay (its backing is not checked)", 0x794C7630], ["nfs", 0x6969], ["cifs/smb", 0xFF534D42], ["9p (WSL, VM shares)", 0x01021997], ["virtiofs", 0x6A656A63], ["fuse", 0x65735546],
    ["ceph", 0x00C36400], ["gfs2", 0x01161970], ["ocfs2", 0x7461636F], ["unreadable", undefined],
  ] as const)("pi-fabric#923 round 3: a dead holder of OUR namespace on a %s root is NOT reclaimed (a VM clone with our identity can share it)", async (_name, type) => {
    const own = ns();
    pidNamespaceTestHooks.set(own);
    localFilesystemTestHooks.set(type);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const root = tempRoot();
    const record = meshRecord(deadPid(), own);
    plant(path.join(root, ".lock"), record);
    await expect(contend(root)).rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
    const { recoverMeshLock, MeshLockRecoverRefusedError } = await import("../src/mesh/lock-recover.js");
    await expect(recoverMeshLock(root, { lock: "mesh", holder: record.split("\n")[0]!, alive: () => false })).rejects.toBeInstanceOf(MeshLockRecoverRefusedError);
    expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(record);
  });

  it("pi-fabric#923 round 5: a root swapped between its filesystem check and the reclaim refuses (mesh, custody, safe lock-recover)", async () => {
    const own = ns();
    pidNamespaceTestHooks.set(own);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    // The swap: the checked root moves away, a NEW directory takes its path, and the SAME lock directory (same inode,
    // same owner bytes) moves into it. Only the held root fd can tell.
    // The new root is on shared storage (nfs here), as an attacker would want: a later, consistent check refuses it too.
    const swap = (root: string, name: string) => {
      const moved = `${root}-moved`;
      fs.renameSync(root, moved);
      fs.mkdirSync(root, { mode: 0o700 });
      fs.renameSync(path.join(moved, name), path.join(root, name));
      roots.push(moved);
      localFilesystemTestHooks.set(0x6969);
    };
    const swapOnLivenessProbe = (pid: number, root: string, name: string) => {
      const kill = process.kill.bind(process);
      let swapped = false;
      vi.spyOn(process, "kill").mockImplementation(((target: number, signal?: string | number) => {
        if (target === pid && !swapped) { swapped = true; swap(root, name); }
        return kill(target, signal as never);
      }) as typeof process.kill);
    };
    // mesh .lock
    const meshRoot = tempRoot();
    const meshPid = deadPid();
    const meshOwner = meshRecord(meshPid, own);
    plant(path.join(meshRoot, ".lock"), meshOwner);
    swapOnLivenessProbe(meshPid, meshRoot, ".lock");
    await expect(contend(meshRoot)).rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
    expect(fs.readFileSync(path.join(meshRoot, ".lock", "owner"), "utf8")).toBe(meshOwner);
    vi.mocked(process.kill).mockRestore();
    localFilesystemTestHooks.set(0xEF53);
    // custody.lock
    const custodyRoot = tempRoot();
    const custodyPid = deadPid();
    const custodyOwner = `${randomUUID()}\n${custodyPid}\n${Date.now()}\n\n${own}\n`;
    plant(path.join(custodyRoot, MESH_CUSTODY_LOCK_NAME), custodyOwner);
    swapOnLivenessProbe(custodyPid, custodyRoot, MESH_CUSTODY_LOCK_NAME);
    await expect(acquireMeshCustodyLock(custodyRoot, 300, { lockProtocol: 3, ownIncarnation: undefined })).rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
    expect(fs.readFileSync(path.join(custodyRoot, MESH_CUSTODY_LOCK_NAME, "owner"), "utf8")).toBe(custodyOwner);
    vi.mocked(process.kill).mockRestore();
    localFilesystemTestHooks.set(0xEF53);
    // safe lock-recover (its liveness probe is the `alive` seam)
    const { recoverMeshLock, MeshLockRecoverRefusedError } = await import("../src/mesh/lock-recover.js");
    const recoverRoot = tempRoot();
    const recoverOwner = meshRecord(deadPid(), own);
    plant(path.join(recoverRoot, ".lock"), recoverOwner);
    const refused = await recoverMeshLock(recoverRoot, { lock: "mesh", holder: recoverOwner.split("\n")[0]!,
      alive: () => { swap(recoverRoot, ".lock"); return false; } }).catch((error: unknown) => error);
    expect(refused).toBeInstanceOf(MeshLockRecoverRefusedError);
    expect((refused as Error).message).toMatch(/was replaced after its filesystem was checked/);
    expect(fs.readFileSync(path.join(recoverRoot, ".lock", "owner"), "utf8")).toBe(recoverOwner);
  });

  it("pi-fabric#923 round 3: the local-filesystem allowlist, and the real rule on this host", () => {
    localFilesystemTestHooks.reset();
    // This test host's TMPDIR: ext4 on the fleet's Linux hosts (local), so pid evidence is allowed there; never off Linux.
    if (process.platform !== "linux") expect(onLocalFilesystem(os.tmpdir())).toBe(false);
    const any = tempRoot();
    for (const local of [0xEF53, 0x58465342, 0x9123683E, 0x01021994, 0x2FC12FC1, 0xF2F52010]) {
      localFilesystemTestHooks.set(local);
      expect(onLocalFilesystem(any)).toBe(true);
    }
    localFilesystemTestHooks.set(0x794C7630);
    expect(onLocalFilesystem(any)).toBe(false); // overlay
    localFilesystemTestHooks.set(0xEF53);
    expect(onLocalFilesystem(path.join(any, "missing"))).toBe(false); // a root that cannot be opened
  });

  it.runIf(process.platform === "linux")("pi-fabric#923 round 4: statfs runs on the PINNED root (its /proc/self/fd entry), never on the path; unreadable is unproven", () => {
    localFilesystemTestHooks.reset();
    const root = tempRoot();
    const local = [0xEF53, 0x58465342, 0x9123683E, 0x01021994, 0x2FC12FC1, 0xF2F52010].includes(Number(fs.statfsSync(root).type));
    expect(onLocalFilesystem(root)).toBe(local); // the real answer for this host's TMPDIR
    const spied = vi.spyOn(fs, "statfsSync");
    onLocalFilesystem(root);
    expect(spied.mock.calls.map(([target]) => String(target))).toEqual([expect.stringMatching(/^\/proc\/self\/fd\/\d+$/)]);
    expect(spied.mock.calls.map(([target]) => String(target))).not.toContain(root);
    spied.mockImplementation(() => { throw Object.assign(new Error("EACCES"), { code: "EACCES" }); });
    expect(onLocalFilesystem(root)).toBe(false);
    // A root that is a symlink is not followed (O_NOFOLLOW): unproven.
    spied.mockRestore();
    const link = path.join(tempRoot(), "link");
    fs.symlinkSync(root, link);
    expect(onLocalFilesystem(link)).toBe(false);
  });

  it("pi-fabric#923 round 4: the recovery command is quoted for the platform's shell (POSIX and PowerShell), and given as argv", async () => {
    const { renderRecoveryCommand } = await import("../src/core/atomic-write.js");
    const refused = new MeshLockForeignHolderError(" (x)", 1, 0, "unproven", "", "/srv/it's here/.lock", "tok'$(touch pwned)");
    expect(refused.recoveryArgv).toEqual(["fabric-mesh-backend", "lock-recover", "--root", "/srv/it's here", "--lock", "mesh", "--holder", "tok'$(touch pwned)"]);
    expect(renderRecoveryCommand(refused.recoveryArgv, "linux"))
      .toBe("fabric-mesh-backend lock-recover --root '/srv/it'\\''s here' --lock mesh --holder 'tok'\\''$(touch pwned)'");
    expect(renderRecoveryCommand(["fabric-mesh-backend", "lock-recover", "--root", "C:\\Users\\o'neil\\mesh", "--holder", "t'k"], "win32"))
      .toBe("fabric-mesh-backend lock-recover --root 'C:\\Users\\o''neil\\mesh' --holder 't''k'");
  });

  it("never reclaims a dead holder of another namespace: an operator recovers it", async () => {
    pidNamespaceTestHooks.set(ns());
    const root = tempRoot();
    const record = meshRecord(deadPid(), container());
    plant(path.join(root, ".lock"), record);
    await expect(contend(root)).rejects.toBeInstanceOf(MeshLockForeignHolderError);
    expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(record);
  });

  it("an unknown own namespace, or a record whose holder could not read its own, is never reclaimed (unproven)", async () => {
    const root = tempRoot();
    plant(path.join(root, ".lock"), meshRecord(deadPid(), container()));
    pidNamespaceTestHooks.set(undefined);
    const unknown = await contend(root).catch((error: unknown) => error);
    expect((unknown as MeshLockForeignHolderError).foreignCode).toBe("FABRIC_MESH_LOCK_UNPROVEN_HOLDER");
    pidNamespaceTestHooks.set(ns());
    const blank = tempRoot();
    plant(path.join(blank, ".lock"), meshRecord(deadPid(), ""));
    await expect(contend(blank)).rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
  });

  it("pi-fabric#923 round 2: a Windows namespace never proves a holder dead (silos share one), even when it equals ours", async () => {
    const windows = "win32:9f0b7c3e-1d2a-4b5c-8e7f-0a1b2c3d4e5f:638640000000000000:host";
    pidNamespaceTestHooks.set(windows);
    const root = tempRoot();
    plant(path.join(root, ".lock"), meshRecord(deadPid(), windows));
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    await expect(contend(root)).rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
  });
});

describe("custody.lock under protocol 3 (smarty-dev#9569): plain and host-qualified, every branch", () => {
  const custodyRecord = (pid: number, namespace: string, hostQualified: boolean, created = Date.now()) => hostQualified
    ? `${randomUUID()}\n${pid}\n${created}\n\n${readPhysicalMachineId() ?? ""}\n\n${namespace}\n`
    : `${randomUUID()}\n${pid}\n${created}\n\n${namespace}\n`;

  it("pi-fabric#923 round 2: under protocol 3 a legacy host-qualified holder with another boot id (a VM clone) is unproven; protocol 2 keeps its boot rule", async () => {
    const { readPhysicalHostIdentity } = await import("../src/core/atomic-write.js");
    const physical = readPhysicalHostIdentity();
    if (!physical?.machineId) return; // needs this host's machine and boot ids
    pidNamespaceTestHooks.set(ns());
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const otherBoot = randomUUID();
    const legacyHostQualified = `${randomUUID()}\n${process.pid}\n${Date.now()}\n\n${physical.machineId}\n${otherBoot}\n`; // 7 fields, v2
    const root = tempRoot();
    plant(path.join(root, MESH_CUSTODY_LOCK_NAME), legacyHostQualified);
    await expect(acquireMeshCustodyLock(root, 300, { lockProtocol: 3, hostQualified: true, ownIncarnation: undefined }))
      .rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
    // Protocol 2 is unchanged (main's previous-boot rule).
    const release = await acquireMeshCustodyLock(root, 1_000, { lockProtocol: 2, hostQualified: true, ownIncarnation: undefined });
    release();
    // A plain legacy record (no machine or boot id) under protocol 3: unproven, even with a dead pid.
    const plain = tempRoot();
    plant(path.join(plain, MESH_CUSTODY_LOCK_NAME), `${randomUUID()}\n${deadPid()}\n${Date.now()}\n`);
    await expect(acquireMeshCustodyLock(plain, 300, { lockProtocol: 3, ownIncarnation: undefined })).rejects.toThrow(/FABRIC_MESH_LOCK_UNPROVEN_HOLDER/);
  });

  it("writes the namespace line in both shapes (6 and 8 fields)", async () => {
    const own = ns();
    pidNamespaceTestHooks.set(own);
    for (const hostQualified of [false, true]) {
      const root = tempRoot();
      const release = await acquireMeshCustodyLock(root, 1_000, { lockProtocol: 3, hostQualified, ownIncarnation: undefined });
      const fields = fs.readFileSync(path.join(root, MESH_CUSTODY_LOCK_NAME, "owner"), "utf8").split("\n");
      expect(fields).toHaveLength(hostQualified ? 8 : 6);
      expect(fields[hostQualified ? 6 : 4]).toBe(own);
      release();
    }
  });

  it.each([false, true])("hostQualified=%s: a foreign holder is refused, typed; our own dead one is reclaimed", async hostQualified => {
    if (hostQualified && !readPhysicalMachineId()) return; // host-qualified custody needs a machine id
    const own = ns();
    pidNamespaceTestHooks.set(own);
    const old = Date.now() - 120_000; // past the 30 s custody staleness of the unknown-boot branch
    const foreign = tempRoot();
    const record = custodyRecord(deadPid(), container(), hostQualified, old);
    plant(path.join(foreign, MESH_CUSTODY_LOCK_NAME), record);
    const lock = path.join(foreign, MESH_CUSTODY_LOCK_NAME);
    fs.utimesSync(path.join(lock, "owner"), old / 1000, old / 1000);
    fs.utimesSync(lock, old / 1000, old / 1000);
    await expect(acquireMeshCustodyLock(foreign, 300, { lockProtocol: 3, hostQualified, ownIncarnation: undefined }))
      .rejects.toBeInstanceOf(MeshLockForeignHolderError);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(record);
    const mine = tempRoot();
    plant(path.join(mine, MESH_CUSTODY_LOCK_NAME), custodyRecord(deadPid(), own, hostQualified, old));
    const ownLock = path.join(mine, MESH_CUSTODY_LOCK_NAME);
    fs.utimesSync(path.join(ownLock, "owner"), old / 1000, old / 1000);
    fs.utimesSync(ownLock, old / 1000, old / 1000);
    const release = await acquireMeshCustodyLock(mine, 1_000, { lockProtocol: 3, hostQualified, ownIncarnation: undefined });
    release();
  });
});

describe("fabric-mesh-backend lock-recover (smarty-dev#9569 D2): safe form, labelled unsafe override", () => {
  const cli = async (args: string[]) => {
    const { main } = await import("../src/mesh/mesh-backend-cli.js");
    let out = "", err = "";
    const code = await main(args, { stdout: (text: string) => { out += text; }, stderr: (text: string) => { err += text; } } as Parameters<typeof main>[1]);
    return { code, out, err };
  };
  const tokenOf = (record: string) => record.split("\n")[0]!;

  it("recovers a dead holder of our namespace, named by its token; refuses another token", async () => {
    pidNamespaceTestHooks.set(ns());
    const root = tempRoot();
    const record = meshRecord(deadPid(), ns());
    plant(path.join(root, ".lock"), record);
    const wrong = await cli(["lock-recover", "--root", root, "--lock", "mesh", "--holder", "someone-else", "--json"]);
    expect(wrong.code).toBe(3);
    const done = await cli(["lock-recover", "--root", root, "--lock", "mesh", "--holder", tokenOf(record), "--json"]);
    expect(done.code).toBe(0);
    expect(JSON.parse(done.out)).toMatchObject({ ok: true, mode: "safe", verdict: "pid" });
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });

  it("refuses a foreign holder in the safe form; the unsafe override recovers it and says so", async () => {
    pidNamespaceTestHooks.set(ns());
    const root = tempRoot();
    const record = meshRecord(deadPid(), container());
    plant(path.join(root, MESH_CUSTODY_LOCK_NAME), record);
    const refused = await cli(["lock-recover", "--root", root, "--lock", "custody", "--holder", tokenOf(record)]);
    expect(refused.code).toBe(3);
    expect(refused.err).toMatch(/another PID namespace/);
    const forced = await cli(["lock-recover", "--root", root, "--lock", "custody", "--holder", tokenOf(record), "--unsafe-holder-stopped", "--json"]);
    expect(forced.code).toBe(0);
    expect(JSON.parse(forced.out)).toMatchObject({ mode: "unsafe", verdict: "foreign" });
    expect(forced.err).toMatch(/ALARM UNSAFE override by .* two writers can run.*; audit \{/s); // the parent command line may hold newlines
  });

  it("never recovers a holder that is running here, even with our namespace", async () => {
    const own = ns();
    pidNamespaceTestHooks.set(own);
    const root = tempRoot();
    const record = meshRecord(process.pid, own);
    plant(path.join(root, ".lock"), record);
    const refused = await cli(["lock-recover", "--root", root, "--lock", "mesh", "--holder", tokenOf(record)]);
    expect(refused.code).toBe(3);
    expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(record);
  });

  it("pi-fabric#923 round 4: on Windows the audit is only created exclusively; an existing one refuses (nothing renamed)", async () => {
    const { recoverMeshLock, MeshLockRecoverRefusedError } = await import("../src/mesh/lock-recover.js");
    const own = ns();
    pidNamespaceTestHooks.set(own);
    const root = tempRoot();
    const first = meshRecord(deadPid(), own);
    plant(path.join(root, ".lock"), first);
    await expect(recoverMeshLock(root, { lock: "mesh", holder: first.split("\n")[0]!, platform: "win32" })).resolves.toMatchObject({ recovered: true });
    const second = meshRecord(deadPid(), own);
    plant(path.join(root, ".lock"), second);
    const refused = await recoverMeshLock(root, { lock: "mesh", holder: second.split("\n")[0]!, platform: "win32" }).catch((error: unknown) => error);
    expect(refused).toBeInstanceOf(MeshLockRecoverRefusedError);
    expect((refused as Error).message).toMatch(/already exists; on Windows it is only created, never appended/);
    expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(second); // refused before the rename
    expect(fs.readFileSync(path.join(root, "lock-recoveries.jsonl"), "utf8").trim().split("\n")).toHaveLength(1);
  });

  it.runIf(process.platform === "linux" && fs.existsSync(path.join(process.cwd(), "dist/mesh/mesh-backend-cli.js")))(
    "pi-fabric#923 round 4: a secret in the parent's command line appears in neither the audit nor the output (built CLI)", async () => {
      const root = tempRoot();
      const record = meshRecord(deadPid(), container(4026539999)); // foreign to the child: only the unsafe override acts
      plant(path.join(root, ".lock"), record);
      const script = path.join(root, "run.mjs");
      fs.writeFileSync(script, `import { main } from ${JSON.stringify(path.join(process.cwd(), "dist/mesh/mesh-backend-cli.js"))};\n` +
        `process.exitCode = await main(["lock-recover", "--root", ${JSON.stringify(root)}, "--lock", "mesh", "--holder", ${JSON.stringify(record.split("\n")[0])}, "--unsafe-holder-stopped", "--json"]);\n`);
      // The parent is a shell whose OWN arguments carry the secret; `; :` keeps it from exec-ing into node.
      const result = childProcess.spawnSync("/bin/sh", ["-c", `"${process.execPath}" "$1"; :`, "sh", script, "--api-token=SECRET-923-r4"],
        { encoding: "utf8", timeout: 30_000 });
      const audit = fs.readFileSync(path.join(root, "lock-recoveries.jsonl"), "utf8");
      expect(audit).toMatch(/"parentExecutable":"[^"]*sh"/);
      for (const text of [audit, result.stdout, result.stderr]) expect(text).not.toContain("SECRET-923-r4");
    });

  it("pi-fabric#923 round 2: the audit fails closed (nothing renamed) on a symlink to /dev/null or a 0666 file, and rotates at its cap", async () => {
    const { recoverMeshLock, MeshLockRecoverRefusedError } = await import("../src/mesh/lock-recover.js");
    const own = ns();
    pidNamespaceTestHooks.set(own);
    const recover = async (setup: (audit: string) => void, extra: { auditMaxBytes?: number } = {}) => {
      const root = tempRoot();
      const record = meshRecord(deadPid(), own);
      plant(path.join(root, ".lock"), record);
      const audit = path.join(root, "lock-recoveries.jsonl");
      setup(audit);
      const result = await recoverMeshLock(root, { lock: "mesh", holder: record.split("\n")[0]!, ...extra }).catch((error: unknown) => error);
      return { root, audit, result, held: fs.existsSync(path.join(root, ".lock", "owner")) };
    };
    if (process.platform === "win32") {
      // Windows takes the create-only branch: any existing audit file refuses, before the rename. The mode, symlink,
      // FIFO and rotate-at-cap cases do not apply there; the Windows audit DACL is cut to smarty-dev#9842.
      const existing = await recover(audit => fs.writeFileSync(audit, ""));
      expect(existing.result).toBeInstanceOf(MeshLockRecoverRefusedError);
      expect((existing.result as Error).message).toMatch(/already exists; on Windows it is only created/);
      expect(existing.held).toBe(true); // nothing renamed
      return;
    }
    const linked = await recover(audit => fs.symlinkSync("/dev/null", audit));
    expect(linked.result).toBeInstanceOf(MeshLockRecoverRefusedError);
    expect(linked.held).toBe(true); // refused BEFORE the rename
    // Round 3: a FIFO planted at the audit path never blocks the open (O_NONBLOCK), and is refused.
    const fifo = await recover(audit => { childProcess.execFileSync("mkfifo", ["-m", "600", audit]); });
    expect(fifo.result).toBeInstanceOf(MeshLockRecoverRefusedError);
    expect(fifo.held).toBe(true);
    const open = await recover(audit => { fs.writeFileSync(audit, ""); fs.chmodSync(audit, 0o666); });
    expect((open.result as Error).message).toMatch(/not a private regular file \(mode 666/);
    expect(open.held).toBe(true);
    const full = await recover(audit => fs.writeFileSync(audit, `${"x".repeat(500)}\n`, { mode: 0o600 }), { auditMaxBytes: 600 });
    expect(full.result).toMatchObject({ recovered: true });
    expect(fs.readFileSync(`${full.audit}.1`, "utf8")).toBe(`${"x".repeat(500)}\n`); // rotated once
    expect(fs.readFileSync(full.audit, "utf8").trim().split("\n")).toHaveLength(1);
    expect(fs.statSync(full.audit).mode & 0o777).toBe(0o600);
  });

  it("usage: lock-recover needs --lock and --holder; other commands refuse its flags", async () => {
    expect((await cli(["lock-recover", "--root", tempRoot(), "--lock", "mesh"])).code).toBe(2);
    expect((await cli(["status", "--root", tempRoot(), "--holder", "x"])).code).toBe(2);
  });
});

// M6 (the design's literal acceptance): a holder stopped right after its release's owner check, its directory renamed
// away and a successor published. Linux releases through the pinned fd (#889). Off Linux, until the native helper of
// option (d)/(b) lands, the release by name still removes the successor: the tracked residual of smarty-dev#9569.
describe.runIf(process.platform !== "linux")("M6 off Linux: the forced-rename release race (tracked residual)", () => {
  it.fails("a reclaimed holder's release never removes its successor's lock", async () => {
    const root = tempRoot();
    const lock = path.join(root, ".lock");
    const successor = "successor-token\n424242\n1\n";
    const mesh = new MeshLock(root, { lockProtocol: 3, lockTimeoutMs: 1_000 }, () => undefined);
    await mesh.withLockAcrossAwait(async () => {
      const record = fs.readFileSync(path.join(lock, "owner"), "utf8");
      const read = fs.readFileSync;
      let fired = false;
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
        const result = (read as (...args: unknown[]) => unknown)(file, options);
        if (!fired && String(file).endsWith(`${path.sep}owner`) && result === record) {
          fired = true;
          fs.renameSync(lock, `${lock}.dead.reclaimed`);
          fs.mkdirSync(lock, { mode: 0o700 });
          fs.writeFileSync(path.join(lock, "owner"), successor, { mode: 0o600 });
        }
        return result;
      }) as typeof fs.readFileSync);
    });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor);
  });
});
