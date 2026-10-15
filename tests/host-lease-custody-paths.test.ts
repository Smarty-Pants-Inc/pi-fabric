import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { MeshStore } from "../src/mesh/store.js";
import { hostLeasePath } from "../src/topology/host-leases.js";

const roots: string[] = [], stores: MeshStore[] = [];
// Root + separator + host-lease-commits/<16hex>/r.<64hex>/owner must be strictly below 240 UTF-16 units.
const retiredOwnerSuffix = `\\${path.win32.join("host-lease-commits", "0".repeat(16), `r.${"0".repeat(64)}`, "owner")}`;
const windowsRootMax = 239 - retiredOwnerSuffix.length;
afterEach(() => {
  vi.restoreAllMocks();
  for (const mesh of stores.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = (rootLength?: number) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "lease-path-")); roots.push(base);
  if (rootLength !== undefined) expect(base.length + 1).toBeLessThan(rootLength);
  const root = rootLength === undefined ? base : path.join(base, "r".repeat(rootLength - base.length - 1));
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend: "file" }); stores.push(mesh);
  const file = hostLeasePath(root, "path-budget-host");
  const hash = createHash("sha256").update(path.basename(file)).digest("hex");
  const commits = path.join(root, "host-lease-commits");
  return { root, mesh, file, commits, legacy: path.join(commits, hash), compact: path.join(commits, hash.slice(0, 16)) };
};

describe("host lease custody path compatibility", () => {
  it("keeps acquisition, contention and release paths under 240 at the maximum Windows root (150 elsewhere)", async () => {
    const rootLength = process.platform === "win32" ? windowsRootMax : 150;
    const s = setup(rootLength);
    expect(s.root).toHaveLength(rootLength);
    expect(windowsRootMax).toBe(130);
    if (process.platform === "win32") expect(s.root.length + retiredOwnerSuffix.length).toBe(239);
    // Observe transient paths too: a walk after release would miss the overlong staging owner.
    const staging = vi.spyOn(fs, "mkdtempSync"), writes = vi.spyOn(fs, "writeFileSync"), renames = vi.spyOn(fs, "renameSync");
    const rejected = vi.fn();
    await expect(s.mesh.leaseCustody(s.file, async () => {
      expect(fs.existsSync(path.join(s.compact, MESH_CUSTODY_LOCK_NAME, "owner"))).toBe(true);
      expect(fs.existsSync(s.legacy)).toBe(false);
      const receipt = fs.readFileSync(path.join(s.compact, MESH_CUSTODY_LOCK_NAME, "owner"), "utf8");
      expect(receipt.split("\n")[0]).toMatch(/^[a-f0-9-]{36}$/); // Ownership UUID is not truncated.
      await expect(s.mesh.leaseCustody(s.file, rejected)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      return "held";
    })).resolves.toBe("held");
    expect(rejected).not.toHaveBeenCalled();
    expect(fs.readdirSync(s.commits)).toEqual([path.basename(s.compact)]);
    expect(fs.existsSync(path.join(s.compact, MESH_CUSTODY_LOCK_NAME))).toBe(false);
    expect(staging).toHaveBeenCalledTimes(2);
    const created = [
      ...staging.mock.results.filter(result => result.type === "return").map(result => String(result.value)),
      ...writes.mock.calls.map(([file]) => String(file)),
      // Renaming a directory creates both its new name and its existing owner child path.
      ...renames.mock.calls.flatMap(([, to]) => [String(to), path.join(String(to), "owner")]),
    ];
    expect(created.length).toBeGreaterThan(0);
    const longest = created.reduce((a, b) => a.length >= b.length ? a : b);
    expect(longest.length, longest).toBeLessThan(240);
    for (const result of staging.mock.results) {
      if (result.type === "return") expect(path.basename(String(result.value))).toMatch(/^custody\.lock\.p\.[a-f0-9]{8}\.[A-Za-z0-9]{6}$/);
    }
    console.info(`Custody path budget: mesh root=${s.root.length}, longest created path=${longest.length}, limit<240`);
  });

  it.runIf(process.platform === "win32")("refuses bound+1 before publishing an owner, with a typed path-budget reason", async () => {
    const s = setup(windowsRootMax + 1);
    expect(s.root.length + retiredOwnerSuffix.length).toBe(240);
    const staging = vi.spyOn(fs, "mkdtempSync");
    const writes = vi.spyOn(fs, "writeFileSync");
    const operation = vi.fn();
    const error: unknown = await s.mesh.leaseCustody(s.file, operation).then(() => undefined, error => error);
    expect(error).toBeInstanceOf(RangeError);
    expect((error as RangeError).message).toMatch(/^Windows mesh lock owner path is too long \(240 logical UTF-16 units\):/);
    expect(operation).not.toHaveBeenCalled();
    expect(staging).not.toHaveBeenCalled();
    expect(writes.mock.calls.some(([file]) => String(file).startsWith(s.compact + path.sep) && path.basename(String(file)) === "owner")).toBe(false);
    expect(fs.existsSync(path.join(s.compact, MESH_CUSTODY_LOCK_NAME))).toBe(false);
  });

  it("prefers an existing legacy domain even when the compact domain also exists, and respects its live holder", async () => {
    const s = setup();
    fs.mkdirSync(s.legacy, { recursive: true }); fs.mkdirSync(s.compact);
    const release = await acquireMeshCustodyLock(s.legacy, 0, { hostQualified: true });
    const operation = vi.fn();
    try {
      await expect(s.mesh.leaseCustody(s.file, operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(operation).not.toHaveBeenCalled();
      expect(fs.readdirSync(s.compact)).toEqual([]);
    } finally { release(); }
    await expect(s.mesh.leaseCustody(s.file, () => fs.existsSync(path.join(s.legacy, MESH_CUSTODY_LOCK_NAME, "owner"))))
      .resolves.toBe(true);
    expect(fs.readdirSync(s.compact)).toEqual([]);
  });

  it("keeps using an idle legacy directory instead of silently moving its gate", async () => {
    const s = setup(); fs.mkdirSync(s.legacy, { recursive: true });
    for (let attempt = 0; attempt < 2; attempt++) {
      await expect(s.mesh.leaseCustody(s.file, () => fs.existsSync(path.join(s.legacy, MESH_CUSTODY_LOCK_NAME, "owner"))))
        .resolves.toBe(true);
    }
    expect(fs.existsSync(s.compact)).toBe(false);
  });
});
