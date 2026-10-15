import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { windowsMeshCacheFixture } from "./windows-mesh-cache-fixture.js";
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import { processIncarnation } from "../src/core/atomic-write.js";
import { ownPidNamespace } from "../src/core/pid-namespace.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { MeshLegacyDebrisUnrecoverableError } from "../src/mesh/legacy-debris.js";
import {
  forgetWindowsMeshCapability, prepareWindowsMeshCapability, windowsMeshReleaseTestHooks,
  type WindowsMeshCapability, type WindowsMeshReleaseChildSample,
} from "../src/storage/windows-mesh-release.js";
import { windowsMeshRecoveryTestHooks } from "../src/storage/windows-mesh-recovery.js";

// HOSTED WINDOWS ONLY; expected off-platform skip, NOT a passing NTFS claim. The verified-remove VALUE import above
// deliberately excludes this deleting suite from local admission (#13001). No platform/native/death mocks or Bun/.ts
// children. Keep: data safety at the actual public E/C/D -> native boundary (#9569 T6/T7), not storage-only dispatch.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const SKIP = "real public acquisition, digest-pinned native helper and NTFS require hosted Windows";
type Domain = "mesh" | "custody";
type RecoverySample = { absolutePath: string; digest: string; mode: "owner" | "debris"; outcome: string };
type Joined = {
  child: ChildProcessWithoutNullStreams;
  ready: Promise<void>;
  done: Promise<{ code: number | null; signal: NodeJS.Signals | null; error: Error | undefined }>;
  closed(): boolean;
};
const children: Joined[] = [];
const roots: string[] = [];
const temp = (): string => {
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fpr-")));
  roots.push(root); return root;
};

/** Register synchronously, finitely bound, and join close (not just exit/stdout). All writers are plain PATH Node. */
const liveNode = (): Joined => {
  const child = spawn("node", ["-e", "process.stdin.on('end', () => process.exit(0)); process.stdin.resume(); process.stdout.write('ready\\n');"], {
    stdio: "pipe", windowsHide: true, timeout: 20_000,
  });
  let closed = false, failure: Error | undefined;
  const done = new Promise<{ code: number | null; signal: NodeJS.Signals | null; error: Error | undefined }>(resolve => {
    child.on("error", error => { failure = error; });
    child.on("close", (code, signal) => { closed = true; resolve({ code, signal, error: failure }); });
  });
  const ready = new Promise<void>((resolve, reject) => {
    let output = "";
    child.stdout.on("data", (bytes: Buffer) => { output += bytes.toString("utf8"); if (output.includes("ready\n")) resolve(); });
    child.on("error", reject);
    child.on("close", () => { if (!output.includes("ready\n")) reject(failure ?? new Error("Node fixture closed before ready")); });
  });
  child.stderr.resume(); child.stdin.on("error", () => { /* terminal receipt reports a failed child */ });
  const entry = { child, ready, done, closed: () => closed };
  children.push(entry); // before any await or assertion
  return entry;
};
interface OwnerFixture { token: string; pid: number; created: number; start: string | undefined; record: string; child: Joined }
const nodeOwner = async (protocol: 1 | 2, dead: boolean): Promise<OwnerFixture> => {
  const entry = liveNode();
  await entry.ready;
  const pid = entry.child.pid!;
  expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
  const start = protocol === 2 ? await processIncarnation(pid, 4000) : undefined;
  if (protocol === 2) expect(start).toMatch(/^win32:\d+$/); // actual creation identity, not fabricated evidence
  const token = randomUUID(), created = Date.now();
  const record = `${token}\n${pid}\n${created}\n${start ? `${start}\n` : ""}`;
  if (dead) {
    entry.child.stdin.end();
    expect(await entry.done).toEqual({ code: 0, signal: null, error: undefined });
    // Actual terminal join is the fixture authority; an unexpected PID reuse fails safely, never grants removal.
    expect(() => process.kill(pid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
  } else expect(process.kill(pid, 0)).toBe(true);
  return { token, pid, created, start, record, child: entry };
};
const old = (file: string): void => { const time = new Date(Date.now() - 60_000); fs.utimesSync(file, time, time); };
const lockAt = (root: string, domain: Domain): string => path.join(root, domain === "mesh" ? ".lock" : "custody.lock");
const snapshot = (file: string) => {
  const stat = fs.lstatSync(file, { bigint: true });
  return { dev: stat.dev, ino: stat.ino, size: stat.size, mtimeNs: stat.mtimeNs, bytes: fs.readFileSync(file) };
};
const publicAcquire = async (domain: Domain, root: string, protocol: 1 | 2, operation: () => string,
  hostQualified = false): Promise<string> => {
  if (domain === "mesh") return new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 10_000 }, () => undefined).withLock(operation);
  const release = await acquireMeshCustodyLock(root, 10_000, { lockProtocol: protocol, hostQualified });
  try { return operation(); } finally { release(); }
};

for (const domain of ["mesh", "custody"] as const) for (const protocol of [1, 2] as const) {
  describe.skipIf(!hosted)(`hosted Windows public ${domain} protocol ${protocol} E/C/D (${SKIP})`, () => {
    let root: string, capability: WindowsMeshCapability;
    let recoveries: RecoverySample[], releases: WindowsMeshReleaseChildSample[];
    beforeEach(() => {
      root = temp(); recoveries = []; releases = [];
      forgetWindowsMeshCapability();
      const cache = windowsMeshCacheFixture("fpr-");
      roots.push(cache);
      windowsMeshReleaseTestHooks.cacheRoot = path.join(cache, "cache");
      windowsMeshReleaseTestHooks.afterChild = sample => { releases.push(sample); };
      windowsMeshRecoveryTestHooks.afterChild = sample => { recoveries.push(sample); };
      const bootstrap = windowsHelperBootstrap(); // real value import also keeps this suite detector-excluded
      expect(bootstrap).toBeDefined();
      capability = prepareWindowsMeshCapability();
      expect(capability.digest).toBe(bootstrap!.digest);
      expect(createHash("sha256").update(fs.readFileSync(capability.absolutePath)).digest("hex")).toBe(capability.digest);
    });
    afterEach(async () => {
      // Closed fixture cohort only: native calls are synchronous/joined; first stop+JOIN every owned Node before
      // clearing hooks/cache or deleting a fixture. No pending helper or external writer shares this private cache.
      for (const entry of children) if (!entry.closed()) entry.child.kill();
      await Promise.all(children.splice(0).map(entry => entry.done));
      for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
      for (const key of Object.keys(windowsMeshRecoveryTestHooks)) delete (windowsMeshRecoveryTestHooks as Record<string, unknown>)[key];
      forgetWindowsMeshCapability();
      for (const dir of roots.splice(0)) {
        if (!path.basename(dir).startsWith("fpr-")) throw new Error("unsafe fixture cleanup");
        fs.rmSync(dir, { recursive: true, force: true });
      }
    }, 30_000);
    const assertCapturedNative = (mode?: "owner" | "debris"): void => {
      expect(recoveries).toHaveLength(mode ? 1 : 0);
      if (mode) expect(recoveries[0]).toEqual({ absolutePath: capability.absolutePath, digest: capability.digest, mode, outcome: "recovered" });
      expect(releases).toHaveLength(1);
      expect(releases[0]).toMatchObject({ absolutePath: capability.absolutePath, digest: capability.digest, outcome: "released", attempt: 1 });
    };

    it("E aged genuine-empty canonical enters and releases, with no C/D child", async () => {
      const lock = lockAt(root, domain); fs.mkdirSync(lock); old(lock);
      const directory = fs.lstatSync(lock, { bigint: true });
      expect(await publicAcquire(domain, root, protocol, () => "entered")).toBe("entered");
      expect(fs.existsSync(lock)).toBe(false);
      assertCapturedNative();
      if (domain === "mesh") {
        const fence = `${lock}.dead.${createHash("sha256").update(`${Number(directory.dev)}:${Number(directory.ino)}:`).digest("hex")}`;
        expect(fs.readdirSync(fence)).toEqual(["f"]); expect(fs.readFileSync(path.join(fence, "f"), "utf8")).toBe("1\n");
      }
    }, 30_000);

    if (domain === "custody") it("E host-qualified genuine-empty precedes missing-pid alarm and enters", async () => {
      const lock = lockAt(root, domain); fs.mkdirSync(lock); old(lock);
      expect(await publicAcquire(domain, root, protocol, () => "host-qualified entered", true)).toBe("host-qualified entered");
      expect(fs.existsSync(lock)).toBe(false); assertCapturedNative();
    }, 30_000);

    it("C joined dead canonical owner enters through real native retirement, retaining exact permanent F/full R", async () => {
      const owner = await nodeOwner(protocol, true);
      const lock = lockAt(root, domain); fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), owner.record, { flag: "wx" });
      const initial = fs.lstatSync(lock, { bigint: true });
      const fence = `${lock}.dead.${createHash("sha256").update(`${Number(initial.dev)}:${Number(initial.ino)}:${owner.record}`).digest("hex")}`;
      const originalOwner = snapshot(path.join(lock, "owner"));
      expect(await publicAcquire(domain, root, protocol, () => "entered")).toBe("entered");
      assertCapturedNative("owner");
      expect(fs.existsSync(lock)).toBe(false); expect(fs.readdirSync(fence)).toEqual(["owner"]);
      expect(snapshot(path.join(fence, "owner"))).toEqual(originalOwner);
      expect(fs.readFileSync(path.join(fence, "owner"), "utf8")).toBe(owner.record);
      // A second actual acquisition ignores the retained nonempty fence, never collects or mutates it.
      expect(await publicAcquire(domain, root, protocol, () => "again")).toBe("again");
      expect(recoveries).toHaveLength(1); expect(releases).toHaveLength(2);
      expect(snapshot(path.join(fence, "owner"))).toEqual(originalOwner);
    }, 30_000);

    it("D independently joined dead direct/nested owners enter via real debris native child; siblings remain nonblocking", async () => {
      const a = await nodeOwner(protocol, true), b = await nodeOwner(protocol, true);
      const lock = lockAt(root, domain); fs.mkdirSync(lock);
      fs.writeFileSync(path.join(lock, `owner.released.${randomUUID()}`), a.record, { flag: "wx" });
      fs.mkdirSync(path.join(lock, "r.legacy", "r.empty"), { recursive: true });
      fs.writeFileSync(path.join(lock, "r.legacy", "owner"), b.record, { flag: "wx" });
      const sibling = path.join(root, "r.nonblocking"); fs.mkdirSync(sibling); fs.writeFileSync(path.join(sibling, "unknown"), "retain");
      expect(await publicAcquire(domain, root, protocol, () => "entered")).toBe("entered");
      assertCapturedNative("debris"); expect(fs.existsSync(lock)).toBe(false);
      expect(fs.readFileSync(path.join(sibling, "unknown"), "utf8")).toBe("retain");
      expect(fs.readdirSync(root).some(name => name.startsWith(`${path.basename(lock)}.dead.`))).toBe(false); // D never detaches L
    }, 30_000);

    it("D mixed actual dead/live leaves refuses ALL-before-ANY, preserving both exact file identities/bytes", async () => {
      const dead = await nodeOwner(protocol, true), live = await nodeOwner(protocol, false);
      const lock = lockAt(root, domain); fs.mkdirSync(lock); fs.mkdirSync(path.join(lock, "r.legacy"));
      const deadFile = path.join(lock, `owner.released.${randomUUID()}`), liveFile = path.join(lock, "r.legacy", "owner");
      fs.writeFileSync(deadFile, dead.record); fs.writeFileSync(liveFile, live.record);
      const beforeDead = snapshot(deadFile), beforeLive = snapshot(liveFile);
      let entered = false;
      await expect(publicAcquire(domain, root, protocol, () => { entered = true; return "unsafe"; }))
        .rejects.toMatchObject({ code: "FABRIC_MESH_LEGACY_DEBRIS_UNRECOVERABLE", kind: "legacy-debris-unrecoverable", retryable: false, path: liveFile, category: "live-owner" });
      expect(entered).toBe(false); expect(recoveries).toEqual([]); expect(releases).toEqual([]);
      expect(snapshot(deadFile)).toEqual(beforeDead); expect(snapshot(liveFile)).toEqual(beforeLive);
      expect(process.kill(live.pid, 0)).toBe(true); // age and neighboring death never grant its authority
    }, 30_000);

    it.each(["raw", "torn", "protocol3-unproven"] as const)("D %s produces actionable typed preservation, never a child or application entry", async kind => {
      const dead = await nodeOwner(protocol, true);
      const lock = lockAt(root, domain); fs.mkdirSync(lock);
      const file = path.join(lock, kind === "raw" ? "unknown-payload" : `owner.released.${randomUUID()}`);
      const ns = kind === "protocol3-unproven" ? await ownPidNamespace() : undefined;
      const record = kind === "raw" ? "unproven dependent payload" : kind === "torn" ? dead.record.slice(0, -1)
        : `${dead.token}\n${dead.pid}\n${dead.created}\n${dead.start ?? ""}\n${ns ?? ""}\n`;
      fs.writeFileSync(file, record); old(lock);
      const before = snapshot(file); let entered = false;
      const error = await publicAcquire(domain, root, protocol, () => { entered = true; return "unsafe"; }).catch(error => error);
      expect(error).toBeInstanceOf(MeshLegacyDebrisUnrecoverableError);
      expect(error).toMatchObject({ code: "FABRIC_MESH_LEGACY_DEBRIS_UNRECOVERABLE", retryable: false, lock, path: file,
        category: kind === "raw" ? "unknown-file" : kind === "torn" ? "torn-record" : "unproven" });
      expect(entered).toBe(false); expect(recoveries).toEqual([]); expect(releases).toEqual([]); expect(snapshot(file)).toEqual(before);
    }, 30_000);

    it("C dead canonical owner plus extra content refuses before native, preserving the complete original proof", async () => {
      const dead = await nodeOwner(protocol, true);
      const lock = lockAt(root, domain); fs.mkdirSync(lock);
      const owner = path.join(lock, "owner"), extra = path.join(lock, `owner.released.${randomUUID()}`);
      fs.writeFileSync(owner, dead.record); fs.writeFileSync(extra, "unjudged");
      const before = snapshot(owner), beforeExtra = snapshot(extra);
      await expect(publicAcquire(domain, root, protocol, () => "unsafe"))
        .rejects.toMatchObject({ code: "FABRIC_MESH_LEGACY_DEBRIS_UNRECOVERABLE", category: "owner-with-extra-entries", lock, retryable: false });
      expect(recoveries).toEqual([]); expect(releases).toEqual([]);
      expect(snapshot(owner)).toEqual(before); expect(snapshot(extra)).toEqual(beforeExtra);
    }, 30_000);
  });
}
