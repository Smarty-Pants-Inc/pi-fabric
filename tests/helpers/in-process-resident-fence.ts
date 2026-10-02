import fs from "node:fs";
import { vi } from "vitest";
import * as fileLock from "../../src/residency/file-lock.js";

/**
 * Explicit test adapter for isolated, same-process POSIX hosts without flock.
 * Windows host tests use the real exclusive-create claim; durable Windows
 * residency/automatic crash recovery remains unsupported.
 * Linux tests still exercise the real kernel fence by default. Closing the exact
 * descriptor releases the test claim, just as host death/close releases flock.
 */
export const installInProcessResidentFence = (force = false): void => {
  if (!force && (process.platform === "win32" || (process.platform === "linux" && process.getuid?.() !== undefined))) return;
  if (vi.isMockFunction(fileLock.lockFile)) return;
  const held = new Map<string, number>();
  const close = fs.closeSync.bind(fs);
  vi.spyOn(fs, "closeSync").mockImplementation(fd => {
    close(fd);
    for (const [file, owned] of held) if (owned === fd) held.delete(file);
  });
  vi.spyOn(fileLock, "lockFile").mockImplementation(async file => {
    if (held.has(file)) throw new fileLock.FileLockBusy(`test resident fence busy: ${file}`);
    const fd = fs.openSync(file, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
    held.set(file, fd);
    return fd;
  });
};
