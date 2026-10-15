import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Keep: B-SEC-02 data safety at the actual operator CLI receiver. REAL private held record/audit;
// recorded native NEVER mutates/deletes or starts a child. This is classification proof, not NTFS proof.
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
const { main } = await import("../src/mesh/mesh-backend-cli.js");
const { recoverMeshLock, MeshLockRecoverRefusedError } = await import("../src/mesh/lock-recover.js");
const { windowsMeshRecoveryFence } = await import("../src/storage/windows-mesh-recovery.js");
const scratch = fs.realpathSync.native(os.tmpdir());
const roots: string[] = [];
const capability = Object.freeze({ absolutePath: "C:\\retained\\fabric-win-remove.exe", digest: "a".repeat(64), meshAbi: 1 as const });
const namespace = "win32:01234567-89ab-cdef-0123-456789abcdef:100:host";
const record = `original-holder\n987654321\n123\nwin32:1\n${namespace}\n`;
type Domain = "mesh" | "custody";
const snapshot = (file: string) => {
  const s = fs.lstatSync(file, { bigint: true });
  return [s.dev, s.ino, s.mtimeNs, s.size];
};
const fixture = (domain: Domain) => {
  const root = fs.mkdtempSync(path.join(scratch, "fabric-cli-outcome-")); roots.push(root);
  const lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock"), owner = path.join(lock, "owner");
  fs.mkdirSync(lock); fs.writeFileSync(owner, record, { flag: "wx", mode: 0o600 });
  const directory = fs.lstatSync(lock, { bigint: true }), ownerStat = fs.lstatSync(owner, { bigint: true });
  return { root, lock, owner, directory, ownerStat, directoryBefore: snapshot(lock), ownerBefore: snapshot(owner),
    audit: path.join(root, "lock-recoveries.jsonl"), fence: windowsMeshRecoveryFence(lock, directory, record) };
};
const invoke = async (f: ReturnType<typeof fixture>, domain: Domain, json = true) => {
  let stdout = "", stderr = "";
  const exit = await main(["lock-recover", "--root", f.root, "--lock", domain, "--holder", "original-holder",
    "--unsafe-holder-stopped", ...(json ? ["--json"] : [])], {
    stdout: text => { stdout += text; }, stderr: text => { stderr += text; },
  });
  return { exit, stdout, stderr };
};
const assertPreserved = (f: ReturnType<typeof fixture>) => {
  expect(native.prepare).toHaveBeenCalledTimes(1); expect(native.owner).toHaveBeenCalledTimes(1);
  const [lock, fullRecord, directory, owner, cap] = native.owner.mock.calls[0]!;
  expect(lock).toBe(f.lock); expect(fullRecord).toBe(record); expect(cap).toBe(capability);
  expect([directory.dev, directory.ino, directory.mtimeNs, directory.size]).toEqual(f.directoryBefore);
  expect([owner.dev, owner.ino, owner.mtimeNs, owner.size]).toEqual(f.ownerBefore);
  expect(typeof directory.ino).toBe("bigint"); expect(typeof owner.ino).toBe("bigint");
  expect(snapshot(f.lock)).toEqual(f.directoryBefore); expect(snapshot(f.owner)).toEqual(f.ownerBefore);
  expect(fs.readFileSync(f.owner, "utf8")).toBe(record);
  expect(fs.readdirSync(f.lock)).toEqual(["owner"]); expect(fs.existsSync(f.fence)).toBe(false);
  const audit = JSON.parse(fs.readFileSync(f.audit, "utf8"));
  expect(audit).toMatchObject({ root: f.root, lock: f.lock, holder: "original-holder", holderPid: 987654321,
    holderNamespace: namespace, mode: "unsafe", verdict: "unproven", operator: { pid: process.pid, ppid: process.ppid } });
  expect(audit.operator).not.toHaveProperty("parentCommand");
};

beforeEach(() => {
  vi.spyOn(process, "platform", "get").mockReturnValue("win32");
  vi.spyOn(os, "type").mockReturnValue("Windows_NT");
  native.prepare.mockReset().mockReturnValue(capability);
  native.owner.mockReset().mockReturnValue({ outcome: "recovery-outcome-unknown", reason: "recorded missing terminal; not a refused result" });
  evidence.own.mockReset().mockResolvedValue(namespace);
  evidence.verdict.mockReset().mockReturnValue({ verdict: "unproven" });
  vi.spyOn(childProcess, "spawnSync").mockImplementation(() => { throw new Error("a recorded receiver test must never start native"); });
});
afterEach(() => {
  expect(childProcess.spawnSync).not.toHaveBeenCalled();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) {
    if (path.dirname(root) !== scratch || !path.basename(root).startsWith("fabric-cli-outcome-")) throw new Error("unsafe fixture cleanup");
    fs.rmSync(root, { recursive: true, force: true });
  }
});

for (const domain of ["mesh", "custody"] as const) describe(`${domain}: actual operator CLI outcome receiver`, () => {
  it("B-SEC-02 unknown native recovery is JSON unknown, never refused/no-mutation, with error exit 1", async () => {
    const f = fixture(domain), rename = vi.spyOn(fs, "renameSync");
    const result = await invoke(f, domain);
    assertPreserved(f); // Preserve original S/full R, audit and proof even on the EE3 counterexample.
    expect(rename.mock.calls.filter(call => call[0] === f.lock)).toEqual([]);
    const json = JSON.parse(result.stdout);
    expect(json.refused).not.toBe(true); // EE3 fails here: the receiver falsely emits refused:true.
    expect(json).toMatchObject({ command: "lock-recover", ok: false, recoveryOutcome: "recovery-outcome-unknown",
      unknown: true, mutationMayHaveOccurred: true, code: "FABRIC_MESH_LOCK_RECOVER_REFUSED" });
    expect(result.stderr).toContain("recovery-outcome-unknown");
    expect(result.stderr).toMatch(/mutation may have occurred/i);
    expect(result.stderr).not.toContain("lock-recover refused:");
    expect(result.stderr).not.toMatch(/nothing was (?:done|recovered)|no mutation/i);
    expect(result.exit).toBe(1);
  });

  it("unknown native recovery without --json is still an explicit mutation-possible error, not a refusal", async () => {
    const f = fixture(domain), result = await invoke(f, domain, false);
    assertPreserved(f);
    expect(result.stdout).toBe(""); expect(result.exit).toBe(1);
    expect(result.stderr).toContain("recovery-outcome-unknown");
    expect(result.stderr).toMatch(/mutation may have occurred/i);
    expect(result.stderr).not.toContain("lock-recover refused:");
  });

  it.each(["refused", "helper-unavailable/not-started"] as const)(
    "%s native precommit control remains refused:true with exit 3 even when reason mentions unknown", async outcome => {
      const f = fixture(domain);
      native.owner.mockReturnValue({ outcome, reason: "recorded precommit refusal; unknown is only text here" });
      const result = await invoke(f, domain);
      assertPreserved(f);
      const json = JSON.parse(result.stdout);
      expect(json).toMatchObject({ command: "lock-recover", ok: false, refused: true, code: "FABRIC_MESH_LOCK_RECOVER_REFUSED" });
      expect(json.unknown).not.toBe(true); expect(json.mutationMayHaveOccurred).not.toBe(true);
      expect(result.stderr).toContain("lock-recover refused:"); expect(result.exit).toBe(3);
    });

  it("capability not-started control remains a typed precommit refusal without audit or native call", async () => {
    const f = fixture(domain);
    native.prepare.mockImplementation(() => { throw new Error("recorded helper unavailable"); });
    const result = await invoke(f, domain);
    expect(result.exit).toBe(3); expect(JSON.parse(result.stdout)).toMatchObject({ ok: false, refused: true });
    expect(result.stderr).toContain("helper-unavailable/not-started");
    expect(native.owner).not.toHaveBeenCalled(); expect(fs.existsSync(f.audit)).toBe(false);
    expect(snapshot(f.lock)).toEqual(f.directoryBefore); expect(snapshot(f.owner)).toEqual(f.ownerBefore);
    expect(fs.readFileSync(f.owner, "utf8")).toBe(record);
  });

  it("actual recoverMeshLock carries structured unknown while preserving the existing error class and code", async () => {
    const f = fixture(domain);
    const error = await recoverMeshLock(f.root, { lock: domain, holder: "original-holder", unsafeHolderStopped: true }).catch(error => error);
    assertPreserved(f);
    expect(error).toBeInstanceOf(MeshLockRecoverRefusedError);
    expect(error).toMatchObject({ name: "MeshLockRecoverRefusedError", code: "FABRIC_MESH_LOCK_RECOVER_REFUSED",
      recoveryOutcome: "recovery-outcome-unknown" });
    expect(new MeshLockRecoverRefusedError("existing caller")).toMatchObject({ recoveryOutcome: "refused" });
  });
});

it("B-CODE-01 actual --help distinguishes application-write fencing, Windows successor protection and macOS by-name release", async () => {
  let stdout = "", stderr = "";
  expect(await main(["--help"], { stdout: text => { stdout += text; }, stderr: text => { stderr += text; } })).toBe(0);
  expect(stderr).toBe("");
  expect(stdout).toContain("does not fence a live holder's application writes: two writers can run");
  expect(stdout).toContain("Windows native recovery protects the successor's lock");
  expect(stdout).toContain("On macOS, a live holder's by-name release can remove its successor's lock");
  expect(stdout).toContain("Linux uses fd-pinned release");
  expect(stdout).toContain("1 error (including recovery-outcome-unknown: mutation may have occurred)");
});
