import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { windowsDataRoot } from "./windows-temp-root.js";

/** Shared POSIX namespace custody policy. Validate top-down before any mkdir;
 * never repair existing permissions. Sticky is allowed only on ancestors.
 *
 * umask-002 hosts (#369 r9): a user-owned, group-writable (not other-writable)
 * ancestor is accepted only below a private boundary, an earlier user-owned
 * ancestor that is either owner-only ((mode & 0o077) === 0, e.g. 0700) or
 * neither writable nor traversable by group or other ((mode & 0o033) === 0)
 * with the same gid as that group-writable directory (smarty-dev#4010 N1).
 * A 0701/0705 home lets any user traverse, so it is never a boundary. Without
 * such a boundary, or for another user's directory, an other-writable bit, or
 * the final root itself, refusal is unchanged. */
export const posixDataRoot = (root: string, options: { create?: boolean; asAncestor?: boolean } = {}): string => {
  if (!path.isAbsolute(root)) throw new Error("Fabric data root must be an absolute path");
  const fail = (directory: string, reason: string): never => {
    throw new Error(`PI_FABRIC_TMPDIR is unsafe: ${directory} ${reason}`);
  };
  const directory = path.resolve(root);
  const chain: string[] = [];
  for (let current = directory; ; current = path.dirname(current)) {
    chain.unshift(current);
    if (path.dirname(current) === current) break;
  }
  const uid = process.getuid!();
  // ownerSealed: an owner-only ancestor seals every group below it. sealedGids:
  // a no-traverse ancestor seals only group-writable directories of its own gid.
  let ownerSealed = false;
  const sealedGids = new Set<number>();
  for (const current of chain) {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(current); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || !options.create) throw error;
      try { fs.mkdirSync(current, { mode: 0o700 }); }
      catch (creationError) {
        if ((creationError as NodeJS.ErrnoException).code !== "EEXIST") throw creationError;
      }
      stat = fs.lstatSync(current);
    }
    if (stat.isSymbolicLink() || !stat.isDirectory()) fail(current, "is not a real directory");
    const final = current === directory && !options.asAncestor;
    if (stat.uid !== uid && (final || stat.uid !== 0)) fail(current, "is owned by another user");
    const groupOnlyBelowBoundary = !final && (ownerSealed || sealedGids.has(stat.gid)) && stat.uid === uid && (stat.mode & 0o002) === 0;
    if ((stat.mode & 0o022) !== 0 && (final || (stat.mode & 0o1000) === 0) && !groupOnlyBelowBoundary) fail(current, "is writable by other users");
    if (stat.uid === uid && (stat.mode & 0o077) === 0) ownerSealed = true;
    else if (stat.uid === uid && (stat.mode & 0o033) === 0) sealedGids.add(stat.gid);
  }
  return directory;
};

/** File data only: socket/pipe paths must keep their short transport-specific roots. */
export const fabricDataRoot = (): string => {
  const root = process.env.PI_FABRIC_TMPDIR;
  if (!root) {
    const selected = os.tmpdir();
    if (process.platform === "win32") return selected;
    if (!path.isAbsolute(selected)) throw new Error("OS temporary directory must be an absolute path");
    const normalized = path.resolve(selected);
    // Only the platform's standard aliases are canonicalized. A project/env
    // symlink (including one below /var/folders) still fails the full chain.
    const canonical = process.platform === "darwin"
      ? normalized.replace(/^\/var(?=\/|$)/, "/private/var").replace(/^\/tmp(?=\/|$)/, "/private/tmp")
      : normalized;
    if (canonical !== selected && fs.realpathSync(selected) !== canonical) {
      throw new Error("Unsafe OS temporary directory alias");
    }
    // OS temp is an allocation ancestor, not an explicit final data root: the
    // root-owned sticky /private/tmp or /tmp namespace is valid for mkdtemp.
    return posixDataRoot(canonical, { asAncestor: true });
  }
  if (process.platform === "win32") return windowsDataRoot(root);
  if (!path.isAbsolute(root)) throw new Error("PI_FABRIC_TMPDIR must be an absolute path");
  return posixDataRoot(root, { create: true });
};
