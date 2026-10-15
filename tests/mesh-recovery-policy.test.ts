import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WindowsMeshRecoveryEntry } from "../src/storage/windows-mesh-recovery.js";

// Keep: data safety and cross-owner trust boundary (#9569 E/C/D, T6/T7/T8).
// Public MeshLock/custody dispatch + REAL in-process filesystem; native is recorded and REFUSES without mutating.
// Injected platform/metadata/evidence are policy tests ONLY, never NTFS or native crash/sharing proof.
const native = vi.hoisted(() => ({ prepare: vi.fn(), release: vi.fn(), owner: vi.fn(), debris: vi.fn() }));
const evidence = vi.hoisted(() => ({ incarnation: vi.fn(), physical: vi.fn(), machine: vi.fn(), events: [] as string[] }));
vi.mock("../src/storage/windows-mesh-release.js", async importOriginal => ({
  windowsLogicalPathUnits: (await importOriginal<typeof import("../src/storage/windows-mesh-release.js")>()).windowsLogicalPathUnits,
  prepareWindowsMeshCapability: native.prepare,
  releaseWindowsMeshLock: native.release,
}));
vi.mock("../src/storage/windows-mesh-recovery.js", () => ({
  retireWindowsMeshOwner: native.owner, removeWindowsMeshDebris: native.debris,
}));
vi.mock("../src/core/atomic-write.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/core/atomic-write.js")>()),
  ownProcessIncarnation: () => Promise.resolve("win32:100"),
  processIncarnation: evidence.incarnation,
  readPhysicalHostIdentity: evidence.physical,
  readPhysicalMachineId: evidence.machine,
}));
vi.mock("../src/core/pid-namespace.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/core/pid-namespace.js")>()),
  ownPidNamespace: () => Promise.resolve("win32:01234567-89ab-cdef-0123-456789abcdef:100:host"),
  ownNamespaceForReclaim: () => Promise.resolve("win32:01234567-89ab-cdef-0123-456789abcdef:100:host"),
}));
const { MeshLock } = await import("../src/mesh/mesh-lock.js");
const { acquireMeshCustodyLock, MeshCustodyUnrecoverableError } = await import("../src/mesh/custody-lock.js");
const { MeshLegacyDebrisUnrecoverableError } = await import("../src/mesh/legacy-debris.js");
const scratch = fs.realpathSync(os.tmpdir());
const roots: string[] = [];
const capability = Object.freeze({ absolutePath: "C:\\retained\\fabric-win-remove.exe", digest: "a".repeat(64), meshAbi: 1 as const });
const UUID = "01234567-89ab-cdef-0123-456789abcdef";
const UUID2 = "12345678-9abc-def0-1234-56789abcdef0";
const MACHINE = "a".repeat(32);
const BOOT = "01234567-89ab-cdef-0123-456789abcdef";
const OLD_BOOT = "12345678-9abc-def0-1234-56789abcdef0";
const DEAD = 987654321;
const LIVE = 987654322;
const released = `owner.released.${UUID}`;
const plain = (pid = DEAD, start?: string): string => `${UUID}\n${pid}\n1\n${start ? `${start}\n` : ""}`;
const qualified = (pid = DEAD, boot = BOOT, ns?: string): string =>
  `${UUID}\n${pid}\n1\nwin32:1\n${MACHINE}\n${boot}\n${ns === undefined ? "" : `${ns}\n`}`;
type Domain = "mesh" | "custody";
const fixture = (domain: Domain): { root: string; lock: string; owner: string } => {
  const root = fs.mkdtempSync(path.join(scratch, "fabric-9569-policy-"));
  roots.push(root);
  const lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
  fs.mkdirSync(lock);
  return { root, lock, owner: path.join(lock, "owner") };
};
const aged = (file: string): void => {
  const old = new Date(Date.now() - 60_000); fs.utimesSync(file, old, old);
};
const acquire = async (domain: Domain, root: string, operation = vi.fn(() => "entered"),
  options: { hostQualified?: boolean; lockProtocol?: 1 | 2 | 3; timeoutMs?: number } = {}): Promise<unknown> => {
  if (domain === "mesh") {
    const mesh = new MeshLock(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: 2000, staleLockMs: 100 }, () => undefined);
    if (mesh.ownIncarnation) await mesh.ownIncarnation;
    return mesh.withLock(operation, options.timeoutMs ?? 2000);
  }
  const release = await acquireMeshCustodyLock(root, options.timeoutMs ?? 2000, {
    hostQualified: options.hostQualified ?? false, lockProtocol: options.lockProtocol ?? 1, ownIncarnation: "win32:100",
  });
  const result = operation(); release(); return result;
};
const protectedError = (category?: string) => expect.objectContaining({
  code: "FABRIC_MESH_LEGACY_DEBRIS_UNRECOVERABLE", kind: "legacy-debris-unrecoverable", retryable: false,
  ...(category ? { category } : {}),
});

beforeEach(() => {
  vi.spyOn(process, "platform", "get").mockReturnValue("win32");
  vi.spyOn(os, "type").mockReturnValue("Windows_NT");
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  evidence.events.length = 0;
  evidence.incarnation.mockReset().mockResolvedValue("win32:100");
  evidence.physical.mockReset().mockReturnValue({ machineId: MACHINE, bootId: BOOT });
  evidence.machine.mockReset().mockReturnValue(MACHINE);
  native.prepare.mockReset().mockReturnValue(capability);
  native.release.mockReset().mockReturnValue({ outcome: "released", cleanup: "clean", retiredPath: "recorded-only" });
  native.owner.mockReset().mockReturnValue({ outcome: "refused", reason: "recorded-share-denial" });
  native.debris.mockReset().mockReturnValue({ outcome: "refused", reason: "recorded-share-denial" });
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (signal !== 0) throw new Error("policy test must never signal a process");
    evidence.events.push(`pid:${pid}`);
    if (pid === DEAD) throw Object.assign(new Error("recorded ESRCH"), { code: "ESRCH" });
    return true;
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) {
    if (path.dirname(root) !== scratch || !path.basename(root).startsWith("fabric-9569-policy-")) throw new Error("unsafe test cleanup");
    fs.rmSync(root, { recursive: true, force: true });
  }
});

for (const domain of ["mesh", "custody"] as const) describe(`${domain}: actual public E/C/D policy dispatch (recorded native)`, () => {
  it("E recovers a genuinely absent aged owner with atomic empty rmdir, not either native owner/debris mode", async () => {
    const f = fixture(domain); aged(f.lock);
    const remove = vi.spyOn(fs, "rmdirSync");
    await expect(acquire(domain, f.root)).resolves.toBe("entered");
    expect(remove.mock.calls.some(call => call[0] === f.lock)).toBe(true);
    expect(native.owner).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
    expect(fs.readFileSync(f.owner, "utf8").endsWith("\n")).toBe(true);
  });

  it("E retains the original grace for fresh empty directories", async () => {
    const f = fixture(domain);
    await expect(acquire(domain, f.root, undefined, { timeoutMs: 0 })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readdirSync(f.lock)).toEqual([]);
    expect(native.owner).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
  });

  it("E's final EMPTY syscall preserves a late protocol-1 initializer owner", async () => {
    const f = fixture(domain); aged(f.lock);
    const late = plain(LIVE);
    const original = fs.rmdirSync.bind(fs);
    let intercepted = false;
    vi.spyOn(fs, "rmdirSync").mockImplementation((file: fs.PathLike) => {
      if (file === f.lock && !intercepted) { intercepted = true; fs.writeFileSync(f.owner, late, { flag: "wx" }); }
      return original(file);
    });
    const operation = vi.fn(() => "must not enter");
    await expect(acquire(domain, f.root, operation, { timeoutMs: 0 })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(intercepted).toBe(true); expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(f.owner, "utf8")).toBe(late);
    expect(native.owner).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
  });

  it.each(["", `${UUID}\n${DEAD}\n1`, `${UUID}\nnot-pid\n1\n`])("a torn owner is never treated as E absence (%j)", async torn => {
    const f = fixture(domain); fs.writeFileSync(f.owner, torn); aged(f.lock);
    await expect(acquire(domain, f.root, undefined, { timeoutMs: 0 })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(f.owner, "utf8")).toBe(torn);
    expect(native.owner).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
  });

  it("C sends original full R, initial bigint directory+owner S and the captured capability; no by-name rename", async () => {
    const f = fixture(domain); const record = plain(); fs.writeFileSync(f.owner, record);
    const directory = fs.lstatSync(f.lock, { bigint: true }), owner = fs.lstatSync(f.owner, { bigint: true });
    const rename = vi.spyOn(fs, "renameSync");
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError("refused:recorded-share-denial"));
    expect(native.owner).toHaveBeenCalledTimes(1);
    const [lock, full, dirS, ownerS, cap, budget] = native.owner.mock.calls[0]!;
    expect([lock, full, dirS.dev, dirS.ino, ownerS.dev, ownerS.ino, ownerS.mtimeNs, ownerS.size])
      .toEqual([f.lock, record, directory.dev, directory.ino, owner.dev, owner.ino, owner.mtimeNs, owner.size]);
    expect(cap).toBe(capability); expect(budget).toBeGreaterThan(0); expect(budget).toBeLessThanOrEqual(2000);
    expect(rename.mock.calls.filter(call => call[0] === f.lock)).toEqual([]);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(record);
    expect(native.debris).not.toHaveBeenCalled();
  });

  it("C keeps exact >2^53 initial ids (recorded metadata ONLY; native refuses, no mutation)", async () => {
    const f = fixture(domain); fs.writeFileSync(f.owner, plain());
    const realLstat = fs.lstatSync.bind(fs), realFstat = fs.fstatSync.bind(fs);
    const originalOwner = realLstat(f.owner, { bigint: true });
    const dirId = 9007199254740993n, ownerId = 9007199254740997n;
    const withId = (stat: fs.BigIntStats, ino: bigint): fs.BigIntStats => Object.assign(Object.create(stat), { ino });
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: { bigint?: boolean }) => {
      const stat = realLstat(file, options as never);
      if (options?.bigint && (file === f.lock || file === f.owner)) return withId(stat as unknown as fs.BigIntStats, file === f.lock ? dirId : ownerId);
      return stat;
    }) as typeof fs.lstatSync);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) => {
      const stat = realFstat(fd, options as never);
      if (options?.bigint && (stat as unknown as fs.BigIntStats).ino === originalOwner.ino) return withId(stat as unknown as fs.BigIntStats, ownerId);
      return stat;
    }) as typeof fs.fstatSync);
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError());
    expect(native.owner.mock.calls[0]![2].ino).toBe(dirId);
    expect(native.owner.mock.calls[0]![3].ino).toBe(ownerId);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(plain());
  });

  it("C never resnapshots owner S after awaited death evidence, even if the same bytes were reinstalled", async () => {
    const f = fixture(domain); const record = plain(LIVE, "win32:1"); fs.writeFileSync(f.owner, record);
    const initial = fs.lstatSync(f.owner, { bigint: true });
    let resume!: (value: string) => void, entered!: () => void;
    const reached = new Promise<void>(resolve => { entered = resolve; });
    evidence.incarnation.mockImplementation(() => { entered(); return new Promise<string>(resolve => { resume = resolve; }); });
    const pending = acquire(domain, f.root);
    await reached;
    fs.renameSync(f.owner, path.join(f.root, "saved-original-owner"));
    fs.writeFileSync(f.owner, record, { flag: "wx" });
    const replacement = fs.lstatSync(f.owner, { bigint: true });
    expect(replacement.ino).not.toBe(initial.ino);
    resume("win32:2");
    await expect(pending).rejects.toEqual(protectedError());
    expect(native.owner.mock.calls[0]![1]).toBe(record);
    expect(native.owner.mock.calls[0]![3].ino).toBe(initial.ino);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(record);
  });

  it("C rejects a different full-R successor installed while death evidence awaited; never submits B as old authority", async () => {
    const f = fixture(domain); const record = plain(LIVE, "win32:1"); fs.writeFileSync(f.owner, record);
    let entered!: () => void;
    const reached = new Promise<void>(resolve => { entered = resolve; });
    const successor = `${UUID2}\n${process.pid}\n1\n`;
    evidence.incarnation.mockImplementation(async () => { entered(); await Promise.resolve(); fs.writeFileSync(f.owner, successor); return "win32:2"; });
    const pending = acquire(domain, f.root, undefined, { timeoutMs: 50 });
    await reached;
    await expect(pending).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(native.owner).not.toHaveBeenCalled();
    expect(fs.readFileSync(f.owner, "utf8")).toBe(successor);
  });

  it("C preserves proof with any extra entry, never grants that entry the canonical owner's death authority", async () => {
    const f = fixture(domain); fs.writeFileSync(f.owner, plain()); fs.writeFileSync(path.join(f.lock, "unknown"), "protected");
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError("owner-with-extra-entries"));
    expect(native.owner).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
    expect(fs.readFileSync(f.owner, "utf8")).toBe(plain());
    expect(fs.readFileSync(path.join(f.lock, "unknown"), "utf8")).toBe("protected");
  });

  it("D sends every independently dead leaf and exact snapshots ALL-before-ANY, child-first, with top L last", async () => {
    const f = fixture(domain);
    fs.mkdirSync(path.join(f.lock, "r.old", "r.empty"), { recursive: true });
    const first = path.join(f.lock, released), second = path.join(f.lock, "r.old", "owner");
    fs.writeFileSync(first, plain()); fs.writeFileSync(second, plain(DEAD, "win32:1"));
    native.debris.mockImplementation((lock, entries) => {
      expect(evidence.events.filter(event => event === `pid:${DEAD}`)).toHaveLength(2);
      expect(fs.readFileSync(first, "utf8")).toBe(plain()); expect(fs.readFileSync(second, "utf8")).toBe(plain(DEAD, "win32:1"));
      expect(lock).toBe(f.lock);
      expect((entries as WindowsMeshRecoveryEntry[]).filter(entry => entry.record !== undefined)).toHaveLength(2);
      return { outcome: "refused", reason: "recorded-share-denial" };
    });
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError());
    expect(native.debris).toHaveBeenCalledTimes(1); expect(native.owner).not.toHaveBeenCalled();
    const [lock, entries, cap] = native.debris.mock.calls[0]! as [string, WindowsMeshRecoveryEntry[], unknown];
    expect(lock).toBe(f.lock); expect(cap).toBe(capability); expect(entries.at(-1)!.path).toBe(f.lock);
    for (let i = 0; i < entries.length - 1; i++) {
      expect(entries.findIndex(entry => entry.path === path.dirname(entries[i]!.path))).toBeGreaterThan(i);
      expect(typeof entries[i]!.stat.ino).toBe("bigint");
    }
  });

  it("D empty r.* trees need no PID proof and are never recursively removed by JS", async () => {
    const f = fixture(domain); fs.mkdirSync(path.join(f.lock, "r.empty", "r.inner"), { recursive: true });
    const rm = vi.spyOn(fs, "rmSync"), rename = vi.spyOn(fs, "renameSync");
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError());
    expect(native.debris).toHaveBeenCalledTimes(1);
    expect(native.debris.mock.calls[0]![1].every((entry: WindowsMeshRecoveryEntry) => entry.record === undefined)).toBe(true);
    expect(evidence.events).toEqual([]);
    expect(rm.mock.calls.some(call => call[0] === f.lock)).toBe(false);
    expect(rename.mock.calls.some(call => call[0] === f.lock)).toBe(false);
    expect(fs.existsSync(path.join(f.lock, "r.empty", "r.inner"))).toBe(true);
  });

  it.each([
    ["raw", "payload", "unknown-file"], ["short released name", "owner.released.short", "unknown-file"],
    ["torn", released, "torn-record"], ["live displaced", released, "live-owner"],
    ["oversize", released, "oversize-record"],
  ])("D protects %s with typed path/category, not contention timeout", async (kind, name, category) => {
    const f = fixture(domain); const file = path.join(f.lock, name);
    const content = kind === "torn" ? "partial" : kind === "live displaced" ? plain(LIVE) : kind === "oversize" ? "x".repeat(4097) : "raw";
    fs.writeFileSync(file, content); aged(f.lock);
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError(category));
    expect(native.debris).not.toHaveBeenCalled(); expect(native.owner).not.toHaveBeenCalled();
    expect(fs.readFileSync(file, "utf8")).toBe(content);
  });

  it("D rejects a link/junction without reading or changing its target", async () => {
    const f = fixture(domain), outside = path.join(f.root, "outside");
    fs.mkdirSync(outside); fs.writeFileSync(path.join(outside, "sentinel"), "retain");
    fs.symlinkSync(outside, path.join(f.lock, "r.link"), "junction");
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError("link-or-reparse"));
    expect(native.debris).not.toHaveBeenCalled(); expect(evidence.events).toEqual([]);
    expect(fs.readFileSync(path.join(outside, "sentinel"), "utf8")).toBe("retain");
  });

  it("D inaccessible leaf read is typed and preserves every record", async () => {
    const f = fixture(domain), file = path.join(f.lock, released); fs.writeFileSync(file, plain());
    const open = fs.openSync.bind(fs);
    const opening = vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
      if (target === file) throw Object.assign(new Error("recorded inaccessible read"), { code: "EACCES" });
      return open(target, flags, mode);
    });
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError("inaccessible:EACCES"));
    expect(native.debris).not.toHaveBeenCalled(); expect(evidence.events).toEqual([]);
    opening.mockRestore();
    expect(fs.readFileSync(file, "utf8")).toBe(plain());
  });

  it("D validates the whole candidate before any evidence or deletion: dead owner plus raw dependent payload is protected", async () => {
    const f = fixture(domain); fs.mkdirSync(path.join(f.lock, "r.old"));
    fs.writeFileSync(path.join(f.lock, "r.old", "owner"), plain());
    fs.writeFileSync(path.join(f.lock, "r.old", "raw"), "unjudged");
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError("unknown-file"));
    expect(evidence.events).toEqual([]); expect(native.debris).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.lock, "r.old", "owner"), "utf8")).toBe(plain());
  });

  it("D mixed dead/live leaves never submit even the dead subset to native", async () => {
    const f = fixture(domain);
    fs.writeFileSync(path.join(f.lock, released), plain());
    const other = path.join(f.lock, `owner.released.${UUID2}`); fs.writeFileSync(other, plain(LIVE));
    await expect(acquire(domain, f.root)).rejects.toEqual(protectedError("live-owner"));
    expect(evidence.events).toContain(`pid:${DEAD}`); expect(evidence.events).toContain(`pid:${LIVE}`);
    expect(native.debris).not.toHaveBeenCalled(); expect(native.owner).not.toHaveBeenCalled();
    expect(fs.readFileSync(other, "utf8")).toBe(plain(LIVE));
    expect(fs.readFileSync(path.join(f.lock, released), "utf8")).toBe(plain());
  });

  it.each(["refused", "helper-unavailable/not-started", "recovery-outcome-unknown"])(
    "D %s sharing/partial counterexample never falls back or silently replays a partial inventory", async outcome => {
      const f = fixture(domain); fs.writeFileSync(path.join(f.lock, released), plain());
      native.debris.mockReturnValue({ outcome, reason: "recorded interfering handle / partial output" });
      const rename = vi.spyOn(fs, "renameSync"), rm = vi.spyOn(fs, "rmSync");
      const operation = vi.fn();
      const error = await acquire(domain, f.root, operation).catch(error => error);
      expect(error).toBeInstanceOf(MeshLegacyDebrisUnrecoverableError);
      if (!(error instanceof MeshLegacyDebrisUnrecoverableError)) throw error;
      expect(error).toMatchObject({ lock: f.lock, path: f.lock, retryable: false });
      expect(error.category).toBe(`${outcome}:recorded interfering handle / partial output`);
      expect(operation).not.toHaveBeenCalled(); expect(native.debris).toHaveBeenCalledTimes(1);
      expect(rename.mock.calls.some(call => call[0] === f.lock)).toBe(false);
      expect(rm.mock.calls.some(call => call[0] === f.lock)).toBe(false);
      expect(fs.readFileSync(path.join(f.lock, released), "utf8")).toBe(plain());
    });

  it.each([1, 2, 3] as const)("D protects a Windows protocol-3 record under caller protocol %i, despite ESRCH", async lockProtocol => {
    const f = fixture(domain);
    const record = `${UUID}\n${DEAD}\n1\nwin32:1\nwin32:01234567-89ab-cdef-0123-456789abcdef:100:host\n`;
    fs.writeFileSync(path.join(f.lock, released), record);
    await expect(acquire(domain, f.root, undefined, { lockProtocol })).rejects.toEqual(protectedError("unproven"));
    expect(evidence.events).toEqual([]); expect(native.debris).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.lock, released), "utf8")).toBe(record);
  });

  it("D protects legacy v1/v2 when viewed by a protocol-3 reclaimer", async () => {
    const f = fixture(domain); fs.writeFileSync(path.join(f.lock, released), plain());
    await expect(acquire(domain, f.root, undefined, { lockProtocol: 3 })).rejects.toEqual(protectedError("unproven"));
    expect(evidence.events).toEqual([]); expect(native.debris).not.toHaveBeenCalled();
  });

  it("siblings, pending paths and retained F markers do NOT block a free canonical lock and are never swept", async () => {
    const f = fixture(domain); fs.rmdirSync(f.lock);
    const siblings = ["r.sibling", `${path.basename(f.lock)}.released.${UUID}`, `${path.basename(f.lock)}.dead.${"a".repeat(64)}`, `${path.basename(f.lock)}.pending.other`];
    for (const name of siblings) {
      const dir = path.join(f.root, name); fs.mkdirSync(dir);
      fs.writeFileSync(path.join(dir, name.includes(".dead.") ? ".recovery-fence" : "unproven"), "retain");
    }
    await expect(acquire(domain, f.root)).resolves.toBe("entered");
    for (const name of siblings) expect(fs.readdirSync(path.join(f.root, name))).toHaveLength(1);
    expect(native.owner).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
  });

  it("does not add recursive D on non-Windows", async () => {
    vi.mocked(os.type).mockReturnValue("Linux");
    const f = fixture(domain); fs.writeFileSync(path.join(f.lock, released), plain()); aged(f.lock);
    await expect(acquire(domain, f.root, undefined, { timeoutMs: 0 })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(native.prepare).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(f.lock, released), "utf8")).toBe(plain());
  });
});

describe("host-qualified custody policy is preserved", () => {
  it("E genuinely empty and aged is allowed BEFORE missing-pid alarm", async () => {
    const f = fixture("custody"); aged(f.lock);
    await expect(acquire("custody", f.root, undefined, { hostQualified: true })).resolves.toBe("entered");
    expect(native.owner).not.toHaveBeenCalled(); expect(native.debris).not.toHaveBeenCalled();
  });
  it.each(["", "torn", `${UUID}\n\n1\n`])("a present malformed host receipt %j stays protected/alarmed, not E", async record => {
    const f = fixture("custody"); fs.writeFileSync(f.owner, record); aged(f.lock);
    await expect(acquire("custody", f.root, undefined, { hostQualified: true, timeoutMs: 0 })).rejects.toBeInstanceOf(MeshCustodyUnrecoverableError);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(record);
  });
  it("C unknown-boot still requires old metadata AND ESRCH; no live expiry or incarnation shortcut", async () => {
    evidence.physical.mockReturnValue(undefined);
    const f = fixture("custody"); fs.writeFileSync(f.owner, qualified(DEAD, ""));
    await expect(acquire("custody", f.root, undefined, { hostQualified: true, timeoutMs: 0 })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(native.owner).not.toHaveBeenCalled();
    aged(f.owner); aged(f.lock);
    await expect(acquire("custody", f.root, undefined, { hostQualified: true })).rejects.toEqual(protectedError());
    expect(native.owner).toHaveBeenCalledTimes(1); expect(evidence.incarnation).not.toHaveBeenCalled();
  });
  it("D foreign machine record is protected independently of filename/dead pid", async () => {
    const f = fixture("custody"); const record = qualified().replace(MACHINE, "b".repeat(32));
    fs.writeFileSync(path.join(f.lock, released), record);
    await expect(acquire("custody", f.root, undefined, { hostQualified: true })).rejects.toEqual(protectedError("foreign-host-or-unproven"));
    expect(native.debris).not.toHaveBeenCalled(); expect(evidence.events).toEqual([]);
    expect(fs.readFileSync(path.join(f.lock, released), "utf8")).toBe(record);
  });
  it("D same-machine previous-boot remains admissible for legacy protocol 1 only", async () => {
    const f = fixture("custody"); fs.writeFileSync(path.join(f.lock, released), qualified(LIVE, OLD_BOOT));
    await expect(acquire("custody", f.root, undefined, { hostQualified: true, lockProtocol: 1 })).rejects.toEqual(protectedError());
    expect(native.debris).toHaveBeenCalledTimes(1); expect(evidence.events).toEqual([]);
  });
  it("D protocol 3 NEVER infers previous-boot death, including matching machine and dead PID", async () => {
    const f = fixture("custody"); const record = qualified(DEAD, OLD_BOOT);
    fs.writeFileSync(path.join(f.lock, released), record);
    await expect(acquire("custody", f.root, undefined, { hostQualified: true, lockProtocol: 3 })).rejects.toEqual(protectedError("unproven"));
    expect(native.debris).not.toHaveBeenCalled(); expect(evidence.events).toEqual([]);
    expect(fs.readFileSync(path.join(f.lock, released), "utf8")).toBe(record);
  });
});

describe("mesh E receipt wire/path budget", () => {
  it("new receipt uses f and original full F hash; existing old F/.recovery-fence remains byte-for-byte untouched", async () => {
    const f = fixture("mesh"); aged(f.lock);
    const stat = fs.lstatSync(f.lock, { bigint: true });
    const fence = `${f.lock}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:`).digest("hex")}`;
    await expect(acquire("mesh", f.root)).resolves.toBe("entered");
    expect(fs.readdirSync(fence)).toEqual(["f"]); expect(fs.readFileSync(path.join(fence, "f"), "utf8")).toBe("1\n");
    const logicalRoot = `C:\\${"x".repeat(147)}`;
    expect(logicalRoot.length).toBe(150);
    expect(`${logicalRoot}\\.lock.dead.${"a".repeat(64)}\\f`.length).toBeLessThan(240);
    expect(`${logicalRoot}\\.lock.dead.${"a".repeat(64)}\\.recovery-fence`.length).toBeGreaterThanOrEqual(240);

    const old = fixture("mesh"); aged(old.lock);
    const oldS = fs.lstatSync(old.lock, { bigint: true });
    const oldF = `${old.lock}.dead.${createHash("sha256").update(`${oldS.dev}:${oldS.ino}:`).digest("hex")}`;
    fs.mkdirSync(oldF); fs.writeFileSync(path.join(oldF, ".recovery-fence"), "original-marker\n");
    await expect(acquire("mesh", old.root)).resolves.toBe("entered");
    expect(fs.readdirSync(oldF)).toEqual([".recovery-fence"]);
    expect(fs.readFileSync(path.join(oldF, ".recovery-fence"), "utf8")).toBe("original-marker\n");
  });
});
