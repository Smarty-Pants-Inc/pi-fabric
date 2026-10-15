import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { pinImportRoot } from "../src/mesh/owned-import-files.js";
import * as stateGate from "../src/mesh/state-gate.js";

// The integration race in mesh-backend-refusal-cleanup.test.ts can reuse the WAL inode only on some filesystems.
// This probe catches the ownership-lifetime regression even when the allocator happens to pick a different inode.
describe.skipIf(process.platform !== "linux")("import inode guards (smarty-dev#11604)", () => {
  it.each([false, true])("keeps creation fds alive until cleanup ends (failed SQLite close: %s)", (closeFailed) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inode-guard-"));
    const created: number[] = [];
    const realOpen = fs.openSync;
    const realClose = fs.closeSync;
    const closed: number[] = [];
    const unregisterPins: Array<() => void> = [];
    const registerPin = stateGate.registerStateFileOwnershipPin;
    const files = pinImportRoot(root);
    vi.spyOn(stateGate, "registerStateFileOwnershipPin").mockImplementation((fd) => {
      const unregister = registerPin(fd);
      unregisterPins.push(unregister);
      return unregister;
    });
    vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
      const fd = realOpen(target, flags, mode);
      if (typeof flags === "number" && (flags & fs.constants.O_EXCL) !== 0) created.push(fd);
      return fd;
    });
    vi.spyOn(fs, "closeSync").mockImplementation((fd) => { closed.push(fd); realClose(fd); });
    try {
      expect(files.pinned).toBe(true);
      expect(files.create("state.db")).toBe(true);
      files.createSidecars();
      expect(created).toHaveLength(3);
      expect(closed.filter(fd => created.includes(fd))).toEqual([]);
      for (const fd of created) expect(fs.fstatSync(fd).isFile()).toBe(true);
      if (closeFailed) {
        const report = files.keepAfterFailure("close-failed", new Error("injected SQLite close failure"));
        expect(report.retained).toEqual(["state.db", "state.db-wal", "state.db-shm"]);
      } else {
        const report = files.removeAfterRefusal();
        expect(report.reason).toBe("quarantined-after-refusal");
        expect(report.retained).toHaveLength(3);
        // Moving an owned sidecar is still allowed. Its guard remains live through the identity re-check.
        for (const fd of created) expect(fs.fstatSync(fd).isFile()).toBe(true);
      }
      files.close();
      expect(closed.filter(fd => created.includes(fd))).toEqual(closeFailed ? [] : created);
      if (!closeFailed) for (const fd of created) expect(() => fs.fstatSync(fd)).toThrow();
    } finally {
      files.close();
      vi.restoreAllMocks();
      // A failed-close CLI keeps the guards until process exit to preserve SQLite's POSIX locks. No connection is
      // open in this probe, so the fixture can close its own guards without dropping any SQLite locks.
      if (closeFailed) for (const fd of new Set(created)) if (!closed.includes(fd)) realClose(fd);
      // The fixture simulates process exit for a failed close; its freed fd numbers must not stay hidden from a
      // later SQLite descriptor proof in this worker. Normal-close disposers are already called and idempotent.
      for (const unregister of unregisterPins) unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
