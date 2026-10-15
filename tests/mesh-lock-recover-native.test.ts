import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Keep: B-SEC-01 cross-owner data safety. REAL private filesystem; recorded native NEVER mutates/deletes.
// Platform/evidence mocks prove operator dispatch and authority preservation, not NTFS handle semantics.
const native = vi.hoisted(() => ({ prepare: vi.fn(), owner: vi.fn() }));
const evidence = vi.hoisted(() => ({ own: vi.fn(), verdict: vi.fn() }));
vi.mock("../src/storage/windows-mesh-release.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/storage/windows-mesh-release.js")>()),
  prepareWindowsMeshCapability: native.prepare,
}));
vi.mock("../src/storage/windows-mesh-recovery.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/storage/windows-mesh-recovery.js")>()),
  retireWindowsMeshOwner: native.owner,
}));
vi.mock("../src/core/pid-namespace.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/core/pid-namespace.js")>()),
  ownPidNamespace: evidence.own, reclaimVerdict: evidence.verdict,
}));
const { recoverMeshLock, MeshLockRecoverRefusedError } = await import("../src/mesh/lock-recover.js");
const { encodeMeshOwnerRecoveryRequest, windowsMeshRecoveryFence } = await import("../src/storage/windows-mesh-recovery.js");
const scratch = fs.realpathSync.native(os.tmpdir());
const roots: string[] = [];
const capability = Object.freeze({ absolutePath: "C:\\retained\\fabric-win-remove.exe", digest: "a".repeat(64), meshAbi: 1 as const });
const namespace = "win32:01234567-89ab-cdef-0123-456789abcdef:100:host";
const a = `original-holder\n987654321\n123\nwin32:1\n${namespace}\n`;
const b = `successor-holder\n${process.pid}\n456\nwin32:2\n${namespace}\n`;
type Domain = "mesh" | "custody";
const fixture = (domain: Domain, record = a) => {
  const root = fs.mkdtempSync(path.join(scratch, "fabric-operator-native-")); roots.push(root);
  const lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock"), owner = path.join(lock, "owner");
  fs.mkdirSync(lock); fs.writeFileSync(owner, record, { flag: "wx" });
  return { root, lock, owner, audit: path.join(root, "lock-recoveries.jsonl"),
    directory: fs.lstatSync(lock, { bigint: true }), ownerStat: fs.lstatSync(owner, { bigint: true }) };
};
const identity = (file: string) => { const s = fs.lstatSync(file, { bigint: true }); return [s.dev, s.ino]; };
const forced = (domain: Domain, root: string, extra = {}) => recoverMeshLock(root, {
  lock: domain, holder: "original-holder", unsafeHolderStopped: true, ...extra,
});
const assertOriginal = (f: ReturnType<typeof fixture>) => {
  expect(native.owner).toHaveBeenCalledTimes(1);
  const [lock, record, dir, owner, cap] = native.owner.mock.calls[0]!;
  expect([lock, record, dir.dev, dir.ino, owner.dev, owner.ino, owner.mtimeNs, owner.size])
    .toEqual([f.lock, a, f.directory.dev, f.directory.ino, f.ownerStat.dev, f.ownerStat.ino, f.ownerStat.mtimeNs, f.ownerStat.size]);
  expect(typeof dir.ino).toBe("bigint"); expect(typeof owner.ino).toBe("bigint"); expect(cap).toBe(capability);
};

beforeEach(() => {
  vi.spyOn(process, "platform", "get").mockReturnValue("win32");
  vi.spyOn(os, "type").mockReturnValue("Windows_NT");
  native.prepare.mockReset().mockReturnValue(capability);
  native.owner.mockReset().mockReturnValue({ outcome: "refused", reason: "judged original no longer present" });
  evidence.own.mockReset().mockResolvedValue(namespace);
  evidence.verdict.mockReset().mockReturnValue({ verdict: "unproven" });
});
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) {
    if (path.dirname(root) !== scratch || !path.basename(root).startsWith("fabric-operator-native-")) throw new Error("unsafe fixture cleanup");
    fs.rmSync(root, { recursive: true, force: true });
  }
});

for (const domain of ["mesh", "custody"] as const) describe(`${domain}: operator native recovery (recorded)`, () => {
  it("B-SEC-01 delayed unsafe operator recovery after recorded W4 preserves published B and original authority", async () => {
    const f = fixture(domain), fence = windowsMeshRecoveryFence(f.lock, f.directory, a);
    const retired = path.join(f.root, `r.${"a".repeat(64)}`);
    const write = fs.writeFileSync.bind(fs), rename = fs.renameSync.bind(fs);
    const renameSpy = vi.spyOn(fs, "renameSync");
    let fired = false, bDirectory: bigint[] = [], bOwner: bigint[] = [];
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      const result = write(file, data, options); // REAL exclusive audit creation completed before W4.
      if (file === f.audit && !fired) {
        fired = true;
        expect(options).toMatchObject({ flag: "wx" });
        expect(JSON.parse(fs.readFileSync(f.audit, "utf8"))).toMatchObject({ holder: "original-holder", mode: "unsafe" });
        rename(f.lock, retired); // recorded W4, separate from the native MOCK (which never mutates).
        fs.mkdirSync(f.lock); write(f.owner, b, { flag: "wx" });
        bDirectory = identity(f.lock); bOwner = identity(f.owner);
        expect(fs.existsSync(fence)).toBe(false); // W4 uses r.*, not old A's F.
      }
      return result;
    });
    const result = await forced(domain, f.root).catch(error => error);
    expect(fired).toBe(true);
    expect(fs.existsSync(f.owner), "B must remain at canonical L after the delayed operator resumes").toBe(true);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(b);
    expect(identity(f.lock)).toEqual(bDirectory); expect(identity(f.owner)).toEqual(bOwner);
    expect(fs.readFileSync(path.join(retired, "owner"), "utf8")).toBe(a);
    expect(fs.existsSync(fence)).toBe(false);
    expect(result).toBeInstanceOf(MeshLockRecoverRefusedError);
    expect(result.message).toContain("refused");
    assertOriginal(f);
    expect(renameSpy.mock.calls.filter(call => call[0] === f.lock)).toEqual([]);
    const audit = JSON.parse(fs.readFileSync(f.audit, "utf8"));
    expect(audit).toMatchObject({ holder: "original-holder", holderNamespace: namespace, mode: "unsafe", operator: { pid: process.pid, ppid: process.ppid } });
    expect(audit.operator).not.toHaveProperty("parentCommand");
  });

  it("captures exact owner S/full R and capability BEFORE awaiting namespace judgment, never resnapshots same-byte replacement", async () => {
    const f = fixture(domain);
    let entered!: () => void, resume!: (value: string) => void;
    const reached = new Promise<void>(resolve => { entered = resolve; });
    evidence.own.mockImplementation(() => {
      expect(native.prepare).toHaveBeenCalledTimes(1);
      entered(); return new Promise<string>(resolve => { resume = resolve; });
    });
    const pending = forced(domain, f.root);
    await reached;
    fs.renameSync(f.owner, path.join(f.root, "saved-original-owner"));
    fs.writeFileSync(f.owner, a, { flag: "wx" });
    expect(identity(f.owner)).not.toEqual([f.ownerStat.dev, f.ownerStat.ino]);
    resume(namespace);
    await expect(pending).rejects.toBeInstanceOf(MeshLockRecoverRefusedError);
    assertOriginal(f);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(a);
  });

  it.each(["refused", "helper-unavailable/not-started", "recovery-outcome-unknown"])(
    "%s is an honest typed refusal, one native call and no JS rename/retry", async outcome => {
      const f = fixture(domain), rename = vi.spyOn(fs, "renameSync");
      native.owner.mockReturnValue({ outcome, reason: "recorded terminal detail" });
      const result = await forced(domain, f.root).catch(error => error);
      expect(result).toBeInstanceOf(MeshLockRecoverRefusedError);
      expect(result.message).toContain(outcome); expect(result.message).toContain("recorded terminal detail");
      if (outcome === "recovery-outcome-unknown") expect(result.message).not.toMatch(/nothing was (?:done|recovered)/);
      assertOriginal(f); expect(native.prepare).toHaveBeenCalledTimes(1);
      expect(rename.mock.calls.filter(call => call[0] === f.lock)).toEqual([]);
      expect(fs.readFileSync(f.owner, "utf8")).toBe(a);
      expect(fs.existsSync(f.audit)).toBe(true);
    });

  it("occupied F is retained with its identity and contents, native refusal never falls back", async () => {
    const f = fixture(domain), fence = windowsMeshRecoveryFence(f.lock, f.directory, a);
    fs.mkdirSync(fence); fs.writeFileSync(path.join(fence, "sentinel"), "permanent");
    const before = identity(fence);
    native.owner.mockReturnValue({ outcome: "refused", reason: "commit: destination-exists" });
    await expect(forced(domain, f.root)).rejects.toThrow(/refused.*destination-exists/);
    assertOriginal(f); expect(identity(fence)).toEqual(before);
    expect(fs.readFileSync(path.join(fence, "sentinel"), "utf8")).toBe("permanent");
    expect(fs.readFileSync(f.owner, "utf8")).toBe(a);
  });

  it("audit failure prevents native mutation and preserves original proof", async () => {
    const f = fixture(domain); fs.writeFileSync(f.audit, "existing audit\n", { flag: "wx" });
    await expect(forced(domain, f.root)).rejects.toThrow(/already exists/);
    expect(native.owner).not.toHaveBeenCalled();
    expect(identity(f.lock)).toEqual([f.directory.dev, f.directory.ino]);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(a);
    expect(fs.readFileSync(f.audit, "utf8")).toBe("existing audit\n");
  });

  it("capability preparation failure is typed, with no audit/native/fallback", async () => {
    const f = fixture(domain), rename = vi.spyOn(fs, "renameSync");
    native.prepare.mockImplementation(() => { throw new Error("recorded unavailable bootstrap"); });
    await expect(forced(domain, f.root)).rejects.toThrow(/FABRIC_MESH_LOCK_RECOVER_REFUSED.*helper-unavailable\/not-started.*recorded unavailable bootstrap/);
    expect(native.owner).not.toHaveBeenCalled(); expect(fs.existsSync(f.audit)).toBe(false);
    expect(rename.mock.calls.filter(call => call[0] === f.lock)).toEqual([]);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(a);
  });

  it("holder token and namespace/death policy still refuse before audit or native", async () => {
    const f = fixture(domain), alive = vi.fn(() => false);
    await expect(recoverMeshLock(f.root, { lock: domain, holder: "wrong", unsafeHolderStopped: true })).rejects.toThrow(/not wrong/);
    await expect(recoverMeshLock(f.root, { lock: domain, holder: "original-holder", alive })).rejects.toThrow(/cannot prove/);
    expect(alive).not.toHaveBeenCalled(); expect(native.owner).not.toHaveBeenCalled(); expect(fs.existsSync(f.audit)).toBe(false);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(a);
  });

  it("only naming rounds >2^53 ids to legacy Number decimal; native frame keeps exact original authority", async () => {
    const f = fixture(domain), realLstat = fs.lstatSync.bind(fs), realFstat = fs.fstatSync.bind(fs);
    const dirId = 9007199254740993n, ownerId = 9007199254740997n;
    const withId = (s: fs.BigIntStats, ino: bigint): fs.BigIntStats => Object.assign(Object.create(s), { ino });
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: { bigint?: boolean }) => {
      const stat = realLstat(file, options as never);
      if (options?.bigint && (file === f.lock || file === f.owner)) return withId(stat as unknown as fs.BigIntStats, file === f.lock ? dirId : ownerId);
      return stat;
    }) as typeof fs.lstatSync);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) => {
      const stat = realFstat(fd, options as never);
      if (options?.bigint && (stat as unknown as fs.BigIntStats).ino === f.ownerStat.ino) return withId(stat as unknown as fs.BigIntStats, ownerId);
      return stat;
    }) as typeof fs.fstatSync);
    await expect(forced(domain, f.root)).rejects.toBeInstanceOf(MeshLockRecoverRefusedError);
    const [, record, directory, owner] = native.owner.mock.calls[0]!;
    expect(directory.ino).toBe(dirId); expect(owner.ino).toBe(ownerId);
    const winLock = `C:\\mesh\\${domain === "mesh" ? ".lock" : "custody.lock"}`;
    const expected = `${winLock}.dead.${createHash("sha256").update(`${Number(directory.dev)}:${Number(dirId)}:${a}`).digest("hex")}`;
    expect(windowsMeshRecoveryFence(winLock, directory, record)).toBe(expected);
    const frame = encodeMeshOwnerRecoveryRequest(winLock, record, directory, owner);
    const fields: string[] = [];
    for (let offset = 5; offset < frame.length;) {
      const length = Number.parseInt(frame.subarray(offset, offset + 8).toString("ascii"), 16); offset += 8;
      fields.push(frame.subarray(offset, offset + length).toString("utf8")); offset += length;
    }
    expect(fields[1]).toBe(path.win32.toNamespacedPath(expected));
    expect(fields[3]).toBe(dirId.toString(16)); expect(fields[5]).toBe(ownerId.toString(16)); expect(fields[8]).toBe(a);
  });

  it("options.platform is audit-only: non-Windows safe recovery keeps legacy Number-decimal high-id fence and never prepares native", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("linux"); vi.mocked(os.type).mockReturnValue("Linux");
    evidence.verdict.mockReturnValue({ verdict: "pid" });
    const f = fixture(domain), alive = vi.fn(() => false), realLstat = fs.lstatSync.bind(fs);
    const dirId = 9007199254740993n;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: { bigint?: boolean }) => {
      const stat = realLstat(file, options as never);
      if (file === f.lock) return Object.assign(Object.create(stat), { ino: options?.bigint ? dirId : Number(dirId) });
      return stat;
    }) as typeof fs.lstatSync);
    const expected = `${f.lock}.dead.${createHash("sha256").update(`${Number(f.directory.dev)}:${Number(dirId)}:${a}`).digest("hex")}`;
    const result = await recoverMeshLock(f.root, { lock: domain, holder: "original-holder", alive, platform: "win32" });
    expect(result).toMatchObject({ recovered: true, mode: "safe", verdict: "pid", fence: expected });
    expect(alive).toHaveBeenCalledWith(987654321);
    expect(native.prepare).not.toHaveBeenCalled(); expect(native.owner).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(expected, "owner"), "utf8")).toBe(a);
    expect(fs.existsSync(f.lock)).toBe(false);
  });

  it("options.platform cannot bypass actual Windows native routing through a POSIX audit", async () => {
    const f = fixture(domain), rename = vi.spyOn(fs, "renameSync");
    await expect(forced(domain, f.root, { platform: "linux" })).rejects.toBeInstanceOf(MeshLockRecoverRefusedError);
    assertOriginal(f); expect(rename.mock.calls.filter(call => call[0] === f.lock)).toEqual([]);
    expect(JSON.parse(fs.readFileSync(f.audit, "utf8"))).toMatchObject({ holder: "original-holder", mode: "unsafe" });
  });

  it("non-Windows unsafe warning and operator attribution remain unchanged", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("linux"); vi.mocked(os.type).mockReturnValue("Linux");
    const f = fixture(domain);
    const result = await forced(domain, f.root);
    expect(result.warning).toContain("UNSAFE override by");
    expect(result.warning).toContain("If it is still running, its release can remove its successor's lock and two writers can run.");
    expect(result.audit).toMatchObject({ holder: "original-holder", verdict: "unproven", mode: "unsafe", operator: { pid: process.pid } });
    expect(native.prepare).not.toHaveBeenCalled(); expect(native.owner).not.toHaveBeenCalled();
  });
});
