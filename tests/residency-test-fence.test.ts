import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { FileLockBusy, lockFile } from "../src/residency/file-lock.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
afterEach(() => vi.restoreAllMocks());

describe("unsupported native residency versus in-process test contract", () => {
  it("explicit test fence excludes a second owner until exact descriptor close and never replaces the inode", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-test-fence-"));
    const file = path.join(root, "host.lock");
    installInProcessResidentFence(true);
    let first: number | undefined;
    let following: number | undefined;
    try {
      first = await lockFile(file, 0);
      const inode = fs.statSync(file).ino;
      await expect(lockFile(file, 0)).rejects.toBeInstanceOf(FileLockBusy);
      fs.closeSync(first); first = undefined;
      following = await lockFile(file, 0);
      expect(fs.statSync(file).ino).toBe(inode);
    } finally {
      if (first !== undefined) fs.closeSync(first);
      if (following !== undefined) fs.closeSync(following);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform !== "linux")("product Windows start with no POSIX UID fails closed before managers and owner publication", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-windows-unsupported-"));
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:unsupported", sessionId: "unsupported", cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    const host = new ResidentHost(config);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    vi.spyOn(process, "getuid").mockReturnValue(undefined as never);
    try {
      await expect(host.start()).rejects.toThrow(/not a regular file owned/);
      expect(host.actors).toBeUndefined();
      expect(host.agents).toBeUndefined();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
