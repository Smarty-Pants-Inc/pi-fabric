import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { acquireNativeMainStartupFence, assertNativeMainCleanClose, nativeMainCleanCloseReceipt,
  nativeMainProcessRecord } from "../src/residency/main-startup-fence.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-main-close-")); roots.push(meshRoot);
  const rootId = "session:owner", project = path.join(meshRoot, "actors"), session = path.join(project, "new-storage");
  return { meshRoot, rootId, project, session, receipt: nativeMainCleanCloseReceipt(meshRoot, rootId),
    process: nativeMainProcessRecord(meshRoot, rootId), acquire: () => acquireNativeMainStartupFence(meshRoot, rootId, [project, session]) };
};
const read = (file: string) => JSON.parse(fs.readFileSync(file, "utf8"));

describe.skipIf(process.platform !== "linux")("native Main positive clean-close custody", () => {
  it("fresh admission commits to a private proof, publishing it only at explicit clean close", async () => {
    const h = fixture(), release = await h.acquire();
    try {
      const owner = read(h.process);
      expect(owner.legacyOwnershipUnknown).toBe(false);
      expect(owner.generation).toMatch(/^[a-f0-9]{64}$/);
      expect(owner.closeProof).toBeUndefined(); expect(fs.existsSync(h.receipt)).toBe(false);
      expect(() => assertNativeMainCleanClose(h.meshRoot, h.rootId)).toThrow(/clean.close/);
      release.cleanClose();
      const receipt = read(h.receipt);
      expect(receipt.generation).toBe(owner.generation);
      expect(createHash("sha256").update(receipt.closeProof).digest("hex")).toBe(owner.closeCommitment);
      expect(assertNativeMainCleanClose(h.meshRoot, h.rootId)).toBe(owner.generation);
    } finally { release(); }
  });

  it("ordinary release never certifies inferred death", async () => {
    const h = fixture(), release = await h.acquire(); release();
    expect(fs.existsSync(h.receipt)).toBe(false);
    expect(() => release.cleanClose()).toThrow(/already released/);
    expect(() => assertNativeMainCleanClose(h.meshRoot, h.rootId)).toThrow(/clean.close/);
  });

  it("new admission invalidates close and rejects old-generation proof even with forged current generation", async () => {
    const h = fixture(), first = await h.acquire();
    let old: Record<string, unknown>;
    try { first.cleanClose(); old = read(h.receipt); } finally { first(); }
    const second = await h.acquire();
    try {
      const owner = read(h.process);
      expect(fs.existsSync(h.receipt)).toBe(false); expect(owner.generation).not.toBe(old!.generation);
      fs.writeFileSync(h.receipt, JSON.stringify(old!));
      expect(() => assertNativeMainCleanClose(h.meshRoot, h.rootId)).toThrow(/clean.close/);
      fs.writeFileSync(h.receipt, JSON.stringify({ ...old!, generation: owner.generation }));
      expect(() => assertNativeMainCleanClose(h.meshRoot, h.rootId)).toThrow(/clean.close/);
      fs.writeFileSync(h.receipt, JSON.stringify({ ...old!, generation: owner.generation, closeProof: owner.closeCommitment }));
      expect(() => assertNativeMainCleanClose(h.meshRoot, h.rootId)).toThrow(/clean.close/);
      second.cleanClose(); expect(assertNativeMainCleanClose(h.meshRoot, h.rootId)).toBe(owner.generation);
    } finally { second(); }
  });

  it("legacy rows only in a different storage session cannot be blessed by successor clean close", async () => {
    const h = fixture(), old = path.join(h.project, "old-storage"); fs.mkdirSync(old, { recursive: true });
    fs.writeFileSync(path.join(h.project, "actors.json"), JSON.stringify({ format: 1, actors: [] }));
    fs.writeFileSync(path.join(old, "actors.json"), JSON.stringify({ format: 1, actors: [{ id: "a".repeat(32), rootId: h.rootId }] }));
    const release = await h.acquire();
    try {
      expect(read(h.process).legacyOwnershipUnknown).toBe(true); release.cleanClose();
      expect(fs.existsSync(h.receipt)).toBe(false);
    } finally { release(); }
    const successor = await h.acquire();
    try { expect(read(h.process).legacyOwnershipUnknown).toBe(true); successor.cleanClose(); expect(fs.existsSync(h.receipt)).toBe(false); }
    finally { successor(); }
  });

  it("expired file-only legacy presence cannot bless an empty actor/root snapshot", async () => {
    const h = fixture(), leaseDir = path.join(h.meshRoot, "host-leases"); fs.mkdirSync(leaseDir);
    const name = createHash("sha256").update(h.rootId).digest("hex").slice(0, 32);
    fs.writeFileSync(path.join(leaseDir, `${name}.json`), JSON.stringify({ format: 1, id: h.rootId,
      rootId: h.rootId, identityId: h.rootId, updatedAt: 1, expiresAt: 2 }));
    const release = await h.acquire();
    try { expect(read(h.process).legacyOwnershipUnknown).toBe(true); release.cleanClose(); expect(fs.existsSync(h.receipt)).toBe(false); }
    finally { release(); }
  });

  it("changed generation custody cannot publish a close receipt, even after writer draining", async () => {
    const h = fixture(), release = await h.acquire();
    try {
      fs.writeFileSync(h.process, JSON.stringify({ ...read(h.process), generation: "f".repeat(64) }));
      expect(() => release.cleanClose()).toThrow(/custody changed/);
      expect(fs.existsSync(h.receipt)).toBe(false);
    } finally { release(); }
  });

  it("pre-existing root evidence without actor rows remains unknown", async () => {
    const h = fixture(); fs.mkdirSync(path.dirname(h.process), { recursive: true });
    fs.writeFileSync(path.join(path.dirname(h.process), "owner.json"), JSON.stringify({ pid: process.pid }));
    const release = await h.acquire();
    try { expect(read(h.process).legacyOwnershipUnknown).toBe(true); release.cleanClose(); expect(fs.existsSync(h.receipt)).toBe(false); }
    finally { release(); }
  });
});
