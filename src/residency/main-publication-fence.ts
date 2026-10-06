import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { lockFile } from "./file-lock.js";

/** Root-scoped startup/generation fence, independent of registry and mesh locks.
 * Main's initial lease and generation publication and operator mutations all use
 * this inode. Never unlink it: a waiting publisher must not lock a second inode.
 * Non-Linux operator death proofs are unavailable; ordinary Main startup there
 * retains its existing behavior (explicit force-live does not use this fence).
 */
export async function withMainPublicationFence<T>(
  meshRoot: string, rootId: string, operation: () => T | Promise<T>, waitSeconds = 10,
): Promise<T> {
  if (process.platform !== "linux") return operation();
  const directory = path.join(meshRoot, "host-leases");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, createHash("sha256").update(rootId).digest("hex").slice(0, 32) + ".main.lock");
  const fd = await lockFile(file, waitSeconds, true);
  try { return await operation(); }
  finally { fs.closeSync(fd); }
}
