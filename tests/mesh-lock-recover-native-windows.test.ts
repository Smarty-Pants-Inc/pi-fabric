import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
// Deliberate VALUE import: excludes this REAL native/deleting suite from local admission (#13001).
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import { ownPidNamespace } from "../src/core/pid-namespace.js";
import { MeshLockRecoverRefusedError, recoverMeshLock } from "../src/mesh/lock-recover.js";
import {
  forgetWindowsMeshCapability, prepareWindowsMeshCapability, releaseWindowsMeshLock, windowsMeshReleaseTestHooks,
  type WindowsMeshCapability, type WindowsMeshReleaseChildSample,
} from "../src/storage/windows-mesh-release.js";
import { windowsMeshRecoveryFence, windowsMeshRecoveryTestHooks } from "../src/storage/windows-mesh-recovery.js";

// Keep: B-SEC-01 real operator -> native C cross-owner safety, with actual W4 release and successor publication.
// Expected skip off hosted Windows is NOT NTFS proof. No platform/native/evidence mocks, Bun or .ts children.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const SKIP_REASON = "actual guarded operator recovery, W4 and NTFS require hosted Windows and the digest-pinned helper";
type Domain = "mesh" | "custody";
type RecoverySample = { absolutePath: string; digest: string; mode: "owner" | "debris"; outcome: string };
const roots: string[] = [];
const temp = (): string => {
  const dir = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fop-"))); roots.push(dir); return dir;
};
const identity = (file: string) => { const stat = fs.lstatSync(file, { bigint: true }); return [stat.dev, stat.ino]; };
const ownerSnapshot = (file: string) => {
  const s = fs.lstatSync(file, { bigint: true });
  return { dev: s.dev, ino: s.ino, mtimeNs: s.mtimeNs, size: s.size, record: fs.readFileSync(file, "utf8") };
};
/** Plain Node only, no fixture file/grandchildren/signals. Synchronous finite child is terminally joined BEFORE
 * its PID is used as an actually stopped holder; the Windows namespace still requires the unsafe assertion. */
const stoppedOwner = async () => {
  const child = spawnSync("node", ["-e", "process.stdout.write(String(process.pid))"], {
    encoding: "utf8", timeout: 5000, windowsHide: true, maxBuffer: 1024,
  });
  expect(child.error).toBeUndefined(); expect(child.status).toBe(0); expect(child.signal).toBeNull();
  const pid = Number(child.stdout);
  expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
  const namespace = await ownPidNamespace(); expect(namespace).toMatch(/^win32:/);
  const token = randomUUID();
  return { token, record: `${token}\n${pid}\n123\n\n${namespace}\n`, namespace };
};
const publish = (lock: string, record: string): void => {
  fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), record, { flag: "wx" });
};

for (const domain of ["mesh", "custody"] as const) describe.skipIf(!hosted)(
  `hosted Windows ${domain} operator recovery (skip reason: ${SKIP_REASON})`, () => {
    let root: string, lock: string, auditFile: string, cap: WindowsMeshCapability;
    let recoveries: RecoverySample[], releases: WindowsMeshReleaseChildSample[];
    beforeEach(() => {
      root = temp(); lock = path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
      auditFile = path.join(root, "lock-recoveries.jsonl"); recoveries = []; releases = [];
      forgetWindowsMeshCapability();
      windowsMeshReleaseTestHooks.cacheRoot = path.join(temp(), "cache");
      const bootstrap = windowsHelperBootstrap(); expect(bootstrap).toBeDefined();
      cap = prepareWindowsMeshCapability(); expect(cap.digest).toBe(bootstrap!.digest);
      windowsMeshReleaseTestHooks.afterChild = sample => { releases.push(sample); };
      windowsMeshRecoveryTestHooks.afterChild = sample => { recoveries.push(sample); };
    });
    afterEach(() => {
      // All Node/helper children are synchronous, finite and already joined. No live fixture writer shares cache.
      vi.restoreAllMocks();
      for (const key of Object.keys(windowsMeshRecoveryTestHooks)) delete (windowsMeshRecoveryTestHooks as Record<string, unknown>)[key];
      for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
      forgetWindowsMeshCapability();
      for (const dir of roots.splice(0)) {
        if (!path.basename(dir).startsWith("fop-")) throw new Error("unsafe fixture cleanup");
        fs.rmSync(dir, { recursive: true, force: true });
      }
    });

    it("B-SEC-01 delayed unsafe operator recovery after real W4 preserves published B and native refuses original", async () => {
      const a = await stoppedOwner(); publish(lock, a.record);
      const original = fs.lstatSync(lock, { bigint: true }), fence = windowsMeshRecoveryFence(lock, original, a.record);
      const b = `${randomUUID()}\n${process.pid}\n456\n\n${a.namespace}\n`;
      const write = fs.writeFileSync.bind(fs), rename = vi.spyOn(fs, "renameSync");
      let fired = false, bDirectory: bigint[] = [], bProof: ReturnType<typeof ownerSnapshot> | undefined;
      vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
        const result = write(file, data, options); // REAL wx audit creation, AFTER final JS check.
        if (file === auditFile && !fired) {
          fired = true; expect(options).toMatchObject({ flag: "wx" });
          expect(JSON.parse(fs.readFileSync(auditFile, "utf8"))).toMatchObject({ holder: a.token, mode: "unsafe" });
          const release = releaseWindowsMeshLock(lock, a.record, cap); // actual native W4 uses r.*, NOT F
          expect(release.outcome).toBe("released");
          expect(fs.existsSync(lock)).toBe(false); expect(fs.existsSync(fence)).toBe(false);
          publish(lock, b); bDirectory = identity(lock); bProof = ownerSnapshot(path.join(lock, "owner"));
        }
        return result;
      });
      const result = await recoverMeshLock(root, { lock: domain, holder: a.token, unsafeHolderStopped: true }).catch(error => error);
      expect(fired).toBe(true);
      expect(fs.existsSync(path.join(lock, "owner")), "B must stay canonical after the delayed operator resumes").toBe(true);
      expect(identity(lock)).toEqual(bDirectory); expect(ownerSnapshot(path.join(lock, "owner"))).toEqual(bProof);
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(b);
      expect(fs.existsSync(fence)).toBe(false);
      expect(result).toBeInstanceOf(MeshLockRecoverRefusedError); expect(result.message).toContain("refused");
      expect(recoveries).toEqual([{ absolutePath: cap.absolutePath, digest: cap.digest, mode: "owner", outcome: "refused" }]);
      expect(releases).toHaveLength(1); expect(releases[0]).toMatchObject({ absolutePath: cap.absolutePath, digest: cap.digest, outcome: "released", attempt: 1 });
      expect(rename.mock.calls.filter(call => call[0] === lock)).toEqual([]);
      expect(JSON.parse(fs.readFileSync(auditFile, "utf8"))).toMatchObject({ holder: a.token, holderNamespace: a.namespace, mode: "unsafe", operator: { pid: process.pid } });
    }, 30_000);

    it("an actually stopped intact original recovers via native C, retaining exact legacy F and full owner identity", async () => {
      const a = await stoppedOwner(); publish(lock, a.record);
      const original = fs.lstatSync(lock, { bigint: true }), proof = ownerSnapshot(path.join(lock, "owner"));
      const fence = windowsMeshRecoveryFence(lock, original, a.record), rename = vi.spyOn(fs, "renameSync");
      const recovered = await recoverMeshLock(root, { lock: domain, holder: a.token, unsafeHolderStopped: true });
      expect(recovered).toMatchObject({ recovered: true, mode: "unsafe", verdict: "unproven", fence, holderNamespace: a.namespace });
      expect(recovered.warning).toContain("UNSAFE override by");
      expect(recovered.warning).toContain("native recovery does not fence its application writes and two writers can run");
      expect(recovered.warning).not.toContain("its release can remove its successor's lock");
      expect(fs.existsSync(lock)).toBe(false);
      expect(identity(fence)).toEqual([original.dev, original.ino]); expect(fs.readdirSync(fence)).toEqual(["owner"]);
      expect(ownerSnapshot(path.join(fence, "owner"))).toEqual(proof);
      expect(recoveries).toEqual([{ absolutePath: cap.absolutePath, digest: cap.digest, mode: "owner", outcome: "recovered" }]);
      expect(releases).toEqual([]); expect(rename.mock.calls.filter(call => call[0] === lock)).toEqual([]);
      expect(JSON.parse(fs.readFileSync(auditFile, "utf8"))).toEqual(recovered.audit);
      // A new canonical owner never causes retained F to be collected or altered.
      const b = `${randomUUID()}\n${process.pid}\n456\n`; publish(lock, b);
      expect(ownerSnapshot(path.join(fence, "owner"))).toEqual(proof);
    }, 30_000);

    it("occupied F refuses natively, preserving both canonical proof and permanent fence", async () => {
      const a = await stoppedOwner(); publish(lock, a.record);
      const original = fs.lstatSync(lock, { bigint: true }), proof = ownerSnapshot(path.join(lock, "owner"));
      const fence = windowsMeshRecoveryFence(lock, original, a.record); fs.mkdirSync(fence);
      fs.writeFileSync(path.join(fence, "sentinel"), "permanent"); const fenceId = identity(fence);
      await expect(recoverMeshLock(root, { lock: domain, holder: a.token, unsafeHolderStopped: true })).rejects.toThrow(/refused.*destination-exists/);
      expect(identity(lock)).toEqual([original.dev, original.ino]); expect(ownerSnapshot(path.join(lock, "owner"))).toEqual(proof);
      expect(identity(fence)).toEqual(fenceId); expect(fs.readFileSync(path.join(fence, "sentinel"), "utf8")).toBe("permanent");
      expect(recoveries).toEqual([{ absolutePath: cap.absolutePath, digest: cap.digest, mode: "owner", outcome: "refused" }]);
    }, 30_000);

    it("existing audit refuses before any native recovery child, preserving original proof", async () => {
      const a = await stoppedOwner(); publish(lock, a.record); fs.writeFileSync(auditFile, "prior audit\n", { flag: "wx" });
      const proof = ownerSnapshot(path.join(lock, "owner")), original = identity(lock);
      await expect(recoverMeshLock(root, { lock: domain, holder: a.token, unsafeHolderStopped: true })).rejects.toThrow(/already exists/);
      expect(identity(lock)).toEqual(original); expect(ownerSnapshot(path.join(lock, "owner"))).toEqual(proof);
      expect(recoveries).toEqual([]); expect(releases).toEqual([]); expect(fs.readFileSync(auditFile, "utf8")).toBe("prior audit\n");
    }, 30_000);
  },
);
