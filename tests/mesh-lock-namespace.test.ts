import childProcess from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockForeignHolderError, readPhysicalMachineId } from "../src/core/atomic-write.js";
import {
  createPidNamespaceReader, ownPidNamespace, pidNamespaceTestHooks, pidNamespaceVerdict, validPidNamespace,
} from "../src/core/pid-namespace.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

// smarty-dev#9569 option (c): a lock record of protocol 3 names its holder's PID namespace, and a reclaimer uses pid
// evidence (ESRCH, another incarnation) only for a holder of its own namespace. A container or VM sharing the mesh
// root sees ESRCH for a LIVE foreign holder; reclaiming it would let two writers into the section.
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  pidNamespaceTestHooks.reset();
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
    ["another boot of THIS machine: dead whatever its pid names now", ns("22222222-2222-4222-8222-222222222222"), own, "previous-boot"],
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

  it("reclaims a dead holder of OUR namespace at once (the counter-case), and a dead legacy one as today", async () => {
    const own = ns();
    pidNamespaceTestHooks.set(own);
    const root = tempRoot();
    plant(path.join(root, ".lock"), meshRecord(deadPid(), own));
    await expect(contend(root)).resolves.toBe("entered");
    const legacy = tempRoot();
    plant(path.join(legacy, ".lock"), meshRecord(deadPid(), null));
    await expect(contend(legacy)).resolves.toBe("entered");
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

  it("reclaims a holder from an earlier boot of this machine even though its pid now names a live process", async () => {
    const own = ns("33333333-3333-4333-8333-333333333333");
    pidNamespaceTestHooks.set(own);
    const root = tempRoot();
    // Our own pid: alive, and its incarnation matches nothing recorded. An earlier boot's holder is dead regardless.
    plant(path.join(root, ".lock"), meshRecord(process.pid, ns("44444444-4444-4444-8444-444444444444")));
    await expect(contend(root)).resolves.toBe("entered");
  });
});

describe("custody.lock under protocol 3 (smarty-dev#9569): plain and host-qualified, every branch", () => {
  const custodyRecord = (pid: number, namespace: string, hostQualified: boolean, created = Date.now()) => hostQualified
    ? `${randomUUID()}\n${pid}\n${created}\n\n${readPhysicalMachineId() ?? ""}\n\n${namespace}\n`
    : `${randomUUID()}\n${pid}\n${created}\n\n${namespace}\n`;

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
    expect(forced.err).toMatch(/UNSAFE override: .* two writers can run/);
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
