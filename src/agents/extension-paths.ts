import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { AgentInputError } from "../host-compatibility.js";
import type { AgentExtensionPathReceipt } from "./types.js";

const MAX_AGENT_EXTENSION_PATHS = 4;
const MAX_MANIFEST_ENTRIES = 10_000;
const MAX_MANIFEST_BYTES = 256 * 1024 * 1024;
const WRITE_BY_OTHERS = 0o022;

const fail = (index: number | undefined, reason: string): never => {
  const slot = index === undefined ? "extensionPaths" : `extensionPaths[${index}]`;
  throw new AgentInputError("extensionPaths", `Invalid agents.spawn ${slot}: ${reason}; no worker started`);
};

const hasParentSegment = (value: string): boolean => value.split(/[\\/]/).includes("..");

const strictInside = (root: string, candidate: string): boolean => {
  const relative = path.relative(root, candidate);
  return relative !== "" && relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
};

const safeOwner = (stats: fs.Stats, label: string, index: number | undefined): void => {
  const uid = process.getuid?.();
  if (uid === undefined) fail(index, "the host cannot verify file ownership");
  if (stats.uid !== uid) fail(index, `${label} is not owned by the current UID`);
}

const safeStats = (stats: fs.Stats, label: string, index: number | undefined): void => {
  safeOwner(stats, label, index);
  if ((stats.mode & WRITE_BY_OTHERS) !== 0) fail(index, `${label} is group- or world-writable`);
}

const safePathChain = (root: string, target: string, index: number | undefined): void => {
  let current = target;
  for (;;) {
    safeStats(statPath(current, index, `path component ${current}`), `path component ${current}`, index);
    if (current === root) return;
    const parent = path.dirname(current);
    if (parent === current || (!strictInside(root, parent) && parent !== root)) {
      fail(index, "path escaped its configured root while checking ownership");
    }
    current = parent;
  }
}

const digest = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");

const realpath = (candidate: string, index: number | undefined, label: string): string => {
  try { return fs.realpathSync.native(candidate); }
  catch (error) { return fail(index, `${label} cannot be resolved (${error instanceof Error ? error.message : String(error)})`); }
};

const statPath = (candidate: string, index: number | undefined, label: string): fs.Stats => {
  try { return fs.statSync(candidate); }
  catch (error) { return fail(index, `${label} cannot be inspected (${error instanceof Error ? error.message : String(error)})`); }
};

const manifestHash = (directory: string, roots: readonly string[], index: number): string => {
  const entries: Array<[string, string, string?]> = [];
  let byteCount = 0;
  const activeDirectories = new Set<string>();
  const visit = (candidate: string, relative: string): void => {
    if (entries.length >= MAX_MANIFEST_ENTRIES) fail(index, `package manifest exceeds ${MAX_MANIFEST_ENTRIES} entries`);
    let linkStats: fs.Stats;
    try { linkStats = fs.lstatSync(candidate); }
    catch (error) { return fail(index, `package changed while hashing (${error instanceof Error ? error.message : String(error)})`); }

    if (linkStats.isSymbolicLink()) {
      // Symlink mode bits are always 0777 on POSIX and do not grant write access;
      // its protected parent directory and the real target carry the policy.
      safeOwner(linkStats, `package symlink ${relative}`, index);
      const target = realpath(candidate, index, `package symlink ${relative}`);
      if (!roots.some(root => strictInside(root, target))) fail(index, `package symlink ${relative} escapes the allowed roots`);
      entries.push([relative, "symlink", path.relative(directory, target).split(path.sep).join("/")]);
      visit(target, relative);
      return;
    }

    const canonical = realpath(candidate, index, `package entry ${relative || "."}`);
    if (!roots.some(root => canonical === root || strictInside(root, canonical))) {
      fail(index, `package entry ${relative || "."} escapes the allowed roots`);
    }
    const stats = statPath(canonical, index, `package entry ${relative || "."}`);
    safeStats(stats, `package entry ${relative || "."}`, index);
    if (stats.isDirectory()) {
      if (activeDirectories.has(canonical)) fail(index, `package symlink cycle at ${relative || "."}`);
      activeDirectories.add(canonical);
      entries.push([relative, "directory"]);
      let names: string[];
      try { names = fs.readdirSync(canonical).sort(); }
      catch (error) { return fail(index, `package directory ${relative || "."} cannot be read (${error instanceof Error ? error.message : String(error)})`); }
      for (const name of names) visit(path.join(canonical, name), relative ? `${relative}/${name}` : name);
      activeDirectories.delete(canonical);
      return;
    }
    if (!stats.isFile()) fail(index, `package entry ${relative || "."} is not a regular file or directory`);
    byteCount += stats.size;
    if (byteCount > MAX_MANIFEST_BYTES) fail(index, `package manifest exceeds ${MAX_MANIFEST_BYTES} bytes`);
    let bytes: Buffer;
    try { bytes = fs.readFileSync(canonical); }
    catch (error) { return fail(index, `package file ${relative} cannot be read (${error instanceof Error ? error.message : String(error)})`); }
    entries.push([relative, "file", digest(bytes)]);
  };
  visit(directory, "");
  const compare = (left: string, right: string): number => left < right ? -1 : left > right ? 1 : 0;
  entries.sort((left, right) => compare(left[0], right[0]) || compare(left[1], right[1]) || compare(left[2] ?? "", right[2] ?? ""));
  return digest(JSON.stringify(entries));
};

/** Resolve, authorize, and hash extension paths on the owning host before admission. */
export const validateAgentExtensionPaths = (
  value: unknown,
  configuredRoots: readonly string[] | undefined,
  cwd: string,
): AgentExtensionPathReceipt[] => {
  if (!Array.isArray(value)) return fail(undefined, `expected at most ${MAX_AGENT_EXTENSION_PATHS} non-empty paths`);
  if (value.length > MAX_AGENT_EXTENSION_PATHS || value.some(entry => typeof entry !== "string" || !entry.trim() || entry.length > 4096 || /[\u0000-\u001f\u007f]/u.test(entry))) {
    return fail(undefined, `expected at most ${MAX_AGENT_EXTENSION_PATHS} non-empty paths`);
  }
  const requestedPaths = value as string[];
  if (!configuredRoots?.length) return fail(undefined, "agents.extensionRoots is empty or unset");
  const roots = configuredRoots.map((configured, index) => {
    if (typeof configured !== "string" || !configured.trim() || hasParentSegment(configured)) {
      return fail(undefined, `agents.extensionRoots[${index}] must be a path without '..'`);
    }
    const lexical = path.resolve(cwd, configured);
    const canonical = realpath(lexical, undefined, `agents.extensionRoots[${index}]`);
    const stats = statPath(canonical, undefined, `agents.extensionRoots[${index}]`);
    if (!stats.isDirectory()) fail(undefined, `agents.extensionRoots[${index}] is not a directory`);
    safeStats(stats, `agents.extensionRoots[${index}]`, undefined);
    return { lexical, canonical };
  });
  const realRoots = [...new Set(roots.map(root => root.canonical))];
  const receipts: AgentExtensionPathReceipt[] = [];
  const seen = new Set<string>();

  for (const [index, requested] of requestedPaths.entries()) {
    if (hasParentSegment(requested)) fail(index, "'..' path segments are not allowed");
    const lexical = path.resolve(cwd, requested);
    const configuredRoot = roots.find(root => strictInside(root.lexical, lexical));
    if (!configuredRoot) fail(index, "path is not strictly inside a configured extension root");
    const canonical = realpath(lexical, index, "path");
    if (!realRoots.some(root => strictInside(root, canonical))) {
      fail(index, "path is not strictly inside a realpath'd extension root (symlink escape refused)");
    }
    if (seen.has(canonical)) fail(index, "duplicate extension path");
    seen.add(canonical);
    const stats = statPath(canonical, index, "path");
    const containingRoot = realRoots.find(root => strictInside(root, canonical));
    if (!containingRoot) return fail(index, "path is not inside a realpath'd extension root");
    safePathChain(containingRoot, canonical, index);
    safeStats(stats, "path", index);
    let sha256: string;
    if (stats.isFile()) {
      if (stats.size > MAX_MANIFEST_BYTES) fail(index, `extension file exceeds ${MAX_MANIFEST_BYTES} bytes`);
      let bytes: Buffer;
      try { bytes = fs.readFileSync(canonical); }
      catch (error) { return fail(index, `file cannot be read (${error instanceof Error ? error.message : String(error)})`); }
      sha256 = digest(bytes);
    } else if (stats.isDirectory()) {
      const packageFile = path.join(canonical, "package.json");
      let packageStats: fs.Stats;
      try { packageStats = fs.statSync(packageFile); }
      catch { return fail(index, "directory is not a package (package.json is missing)"); }
      if (!packageStats.isFile()) fail(index, "directory is not a package (package.json is not a regular file)");
      sha256 = manifestHash(canonical, realRoots, index);
    } else {
      return fail(index, "path must resolve to a regular file or package directory");
    }
    receipts.push({ path: canonical, sha256 });
  }
  return receipts;
};
