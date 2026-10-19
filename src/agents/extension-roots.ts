import fs from "node:fs";
import path from "node:path";

const WRITE_BY_OTHERS = 0o022;
const STICKY = 0o1000;

const reasonForPathComponent = (candidate: string, directoryOnly: boolean): string | undefined => {
  let stats: fs.Stats;
  try { stats = fs.lstatSync(candidate); }
  catch (error) { return `path component ${candidate} cannot be inspected (${error instanceof Error ? error.message : String(error)})`; }
  if (stats.isSymbolicLink()) return `path component ${candidate} is a symlink`;
  if (directoryOnly && !stats.isDirectory()) return `path component ${candidate} is not a directory`;
  const uid = process.getuid?.();
  if (uid === undefined) return "the host cannot verify file ownership";
  if (stats.uid !== 0 && stats.uid !== uid) return `path component ${candidate} is not owned by root or the current UID`;
  const safeStickyDirectory = stats.isDirectory() && stats.uid === 0 && (stats.mode & STICKY) !== 0;
  if ((stats.mode & WRITE_BY_OTHERS) !== 0 && !safeStickyDirectory) {
    return `path component ${candidate} is group- or world-writable`;
  }
  return undefined;
};

/** Check each lexical component, without following a symlinked parent. */
const inspectPathChain = (absolute: string, directoryOnly: boolean): string | undefined => {
  const root = path.parse(absolute).root;
  if (!root) return `path ${absolute} is not absolute`;
  let current = root;
  let reason = reasonForPathComponent(current, true);
  if (reason) return reason;
  const parts = path.relative(root, absolute).split(path.sep).filter(Boolean);
  for (const [index, part] of parts.entries()) {
    current = path.join(current, part);
    reason = reasonForPathComponent(current, directoryOnly || index < parts.length - 1);
    if (reason) return reason;
  }
  return undefined;
};

/** Validate a resolved absolute chain while checking every component with lstat. */
export const assertSafeAgentExtensionPathChain = (absolute: string, label: string): void => {
  const resolved = path.resolve(absolute);
  const reason = inspectPathChain(resolved, false);
  if (reason) throw new Error(`${label}: ${reason}`);
};

/** Canonicalize host-owned extension roots and refuse unsafe ancestors at config load. */
export const resolveAgentExtensionRoots = (
  configuredRoots: readonly string[] | undefined,
  cwd: string,
): string[] => {
  if (!configuredRoots?.length) return [];
  return [...new Set(configuredRoots.map((configured, index) => {
    if (typeof configured !== "string" || !configured.trim() || configured.length > 4096 ||
      /[\u0000-\u001f\u007f]/u.test(configured) || configured.split(/[\\/]/).includes("..")) {
      throw new Error(`Invalid agents.extensionRoots[${index}]: expected a path without '..' or control characters`);
    }
    const lexical = path.resolve(cwd, configured);
    let canonical: string;
    try { canonical = fs.realpathSync.native(lexical); }
    catch (error) {
      throw new Error(`Invalid agents.extensionRoots[${index}]: path cannot be resolved (${error instanceof Error ? error.message : String(error)})`);
    }
    for (const candidate of new Set([lexical, canonical])) {
      const reason = inspectPathChain(candidate, true);
      if (reason) throw new Error(`Invalid agents.extensionRoots[${index}]: ${reason}`);
    }
    return canonical;
  }))];
};
