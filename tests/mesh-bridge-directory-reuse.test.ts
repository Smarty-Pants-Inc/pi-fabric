// Keep reason: F1's recorded lost native lease notification (judge b82), plus participant ownership/liveness safety.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshStateEntry } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Only the strict natural-collision probe needs ZFS. Its inability to exercise a
// collision is a failure on ZFS, never a caught failure or a successful no-op.
const zfsSkipReason = (() => {
  try {
    const type = fs.statfsSync(os.tmpdir()).type;
    return Number(type) === 0x2fc12fc1 ? undefined
      : `ZFS required; os.tmpdir() filesystem type is 0x${Number(type).toString(16)}; natural directory-stamp collision not exercised`;
  } catch (error) {
    return `ZFS required; statfs(os.tmpdir()) unavailable: ${error instanceof Error ? error.message : String(error)}; natural directory-stamp collision not exercised`;
  }
})();
if (zfsSkipReason === undefined) {
  await import("./mesh-bridge-directory-reuse.probe.js");
} else {
  it.skip(`exact b82 natural directory-stamp collision probe: ${zfsSkipReason}`, () => {});
}

const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const leafIdentity = (file: string) => {
  const stat = fs.statSync(file, { bigint: true });
  return `${stat.dev}:${stat.ino}`;
};

// The ONLY injected operation: statSync of this exact parent. Preserve native
// Stats/BigIntStats (including methods and numeric types); all other calls pass
// through with their original options, including leaf stats and missing paths.
const holdParentStat = (directory: string) => {
  const stat = fs.statSync.bind(fs);
  const normal = stat(directory);
  const bigint = stat(directory, { bigint: true });
  let reads = 0;
  const spy = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
    if (typeof target === "string" && path.resolve(target) === directory) {
      reads++;
      return options?.bigint ? bigint : normal;
    }
    return stat(target, options);
  }) as typeof fs.statSync);
  return { reads: () => reads, restore: () => spy.mockRestore() };
};

describe.each(["natural", "held-identical"] as const)("native atomic replacement with %s parent metadata", parentMode => {
  it.each(["lease", "participant"] as const)("reports the actual %s replacement through the SQLite subscription", async kind => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-directory-reuse-"));
    let store: MeshStore | undefined;
    let close: (() => void) | undefined;
    let rawWatch: fs.FSWatcher | undefined;
    let heldParent: ReturnType<typeof holdParentStat> | undefined;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
      const now = Date.now();
      const id = "session:directory-reuse-00000000";
      const identity = { id, name: "directory-reuse", kind: "main" as const, sessionId: "directory-reuse-00000000" };
      const incarnationToken = "directory-reuse-owner";
      const host = { format: 1, id, rootId: id, identity, incarnationToken,
        startedAt: now - 1_000, updatedAt: now, expiresAt: now + 30_000 };
      await store.put({ key: key("hosts", id), identity, value: host });
      const lease = { id, rootId: id, identityId: id, incarnationToken,
        startedAt: host.startedAt, updatedAt: now, expiresAt: now + 60_000 };
      writeHostLease(root, lease);
      const participant = { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
        ownerIncarnation: incarnationToken, name: identity.name, status: "idle", runner: "pi", transport: "host",
        capabilities: ["steer", "followUp"], sessionId: identity.sessionId,
        startedAt: host.startedAt, updatedAt: now, controlProtocol: "v1" };
      const entry: MeshStateEntry = { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: participant };
      writeParticipantFile(root, entry);
      const side = new StoreBridgeSide(store, "remote");
      const initial = await side.presence();
      expect(store.stateBackend).toBe("sqlite");
      expect(store.get(key("hosts", id), { fresh: true })).toMatchObject({ updatedBy: identity, value: host });
      expect(initial.hosts).toHaveLength(1);
      expect(initial.hosts[0]).toMatchObject({ record: { id, rootId: id, identity, incarnationToken }, expiresAt: lease.expiresAt });
      expect(initial.participants).toEqual([participant]);
      // File-only native participant, not a manually installed shared projection.
      expect(store.get(entry.key, { fresh: true })).toBeUndefined();

      const file = kind === "lease" ? hostLeasePath(root, id)
        : path.join(root, "participants", `${entry.key.split("/").at(-1)}.json`);
      const directory = path.dirname(file);
      const beforeLeaf = leafIdentity(file);
      if (parentMode === "held-identical") heldParent = holdParentStat(directory);
      const beforeParent = fs.statSync(directory, { bigint: true });
      const beforeNormalParent = fs.statSync(directory);
      const baselineReads = heldParent?.reads() ?? 0;
      const changes: string[] = [], failures: string[] = [], rawEvents: string[] = [];
      const expected = kind === "lease" ? "lease" : "presence";
      rawWatch = fs.watch(directory, (event, name) => rawEvents.push(`${event}:${String(name)}`));
      // Native watch delivery drives success; one bounded deadline detects silence.
      // No polling, fake clock, manual refresh, or arbitrary settling sleep.
      let notify!: () => void;
      let fail!: (error: Error) => void;
      const notified = new Promise<void>((resolve, reject) => { notify = resolve; fail = reject; });
      close = side.subscribeChanges(change => {
        changes.push(change);
        if (change === expected) notify();
      }, error => { failures.push(error.message); fail(error); });
      if (heldParent) expect(heldParent.reads()).toBeGreaterThan(baselineReads);
      const updatedAt = Date.now();
      const renewed = { ...lease, updatedAt, expiresAt: lease.expiresAt + 30_001 };
      const updatedParticipant = { ...participant, status: "running", updatedAt };
      const updatedEntry = { ...entry, version: entry.version + 1, updatedAt, value: updatedParticipant };
      if (kind === "lease") writeHostLease(root, renewed);
      else writeParticipantFile(root, updatedEntry);
      expect(leafIdentity(file)).not.toBe(beforeLeaf);
      if (heldParent) {
        expect(fs.statSync(directory, { bigint: true })).toEqual(beforeParent);
        expect(fs.statSync(directory)).toEqual(beforeNormalParent);
      }
      const replacementReads = heldParent?.reads() ?? 0;
      // Product notifications use the real fixed 100 ms coalescing window; this
      // larger failure deadline allows native delivery on the 2-core CI runners.
      timeout = setTimeout(() => fail(new Error(`No ${expected} notification after actual ${kind} replacement; changes=${JSON.stringify(changes)}, native=${JSON.stringify(rawEvents)}`)), 2_000);
      await notified;
      clearTimeout(timeout);
      timeout = undefined;
      expect(failures).toEqual([]);
      expect(changes.filter(change => change === "lease" || change === "presence")).toEqual([expected]);
      expect(rawEvents.some(event => event.endsWith(`:${path.basename(file)}`))).toBe(true);
      if (heldParent) {
        expect(heldParent.reads()).toBeGreaterThan(replacementReads);
        expect(fs.statSync(directory, { bigint: true })).toEqual(beforeParent);
        expect(fs.statSync(directory)).toEqual(beforeNormalParent);
      }

      // Fresh canonical state is checked only AFTER the notification, so the
      // assertion cannot prime/repair the subscription's observation cache.
      const fresh = await side.presence();
      expect(readHostLease(root, id)).toMatchObject(kind === "lease" ? renewed : lease);
      expect(JSON.parse(fs.readFileSync(path.join(root, "participants", `${entry.key.split("/").at(-1)}.json`), "utf8")))
        .toMatchObject(kind === "participant" ? updatedEntry : entry);
      expect(fresh.hosts).toHaveLength(1);
      expect(fresh.hosts[0]).toMatchObject({ record: { id, rootId: id, identity, incarnationToken,
        updatedAt: kind === "lease" ? renewed.updatedAt : lease.updatedAt },
        expiresAt: kind === "lease" ? renewed.expiresAt : lease.expiresAt });
      expect(fresh.participants).toEqual([kind === "participant" ? updatedParticipant : participant]);
      expect(store.get(entry.key, { fresh: true })).toBeUndefined();
    } finally {
      if (timeout !== undefined) clearTimeout(timeout);
      close?.();
      rawWatch?.close();
      heldParent?.restore();
      store?.closeState();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
