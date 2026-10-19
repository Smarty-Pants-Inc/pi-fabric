import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { AgentInputError } from "../host-compatibility.js";
import type { FabricAgentExtensionPin } from "../config.js";
import type { AgentExtensionPathReceipt } from "./types.js";
import { assertSafeAgentExtensionPathChain, resolveAgentExtensionPins, resolveAgentExtensionRoots } from "./extension-roots.js";

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
};

const safeStats = (stats: fs.Stats, label: string, index: number | undefined): void => {
  safeOwner(stats, label, index);
  if ((stats.mode & WRITE_BY_OTHERS) !== 0) fail(index, `${label} is group- or world-writable`);
};

const digest = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");

const realpath = (candidate: string, index: number | undefined, label: string): string => {
  try { return fs.realpathSync.native(candidate); }
  catch (error) { return fail(index, `${label} cannot be resolved (${error instanceof Error ? error.message : String(error)})`); }
};

const statPath = (candidate: string, index: number | undefined, label: string): fs.Stats => {
  try { return fs.statSync(candidate); }
  catch (error) { return fail(index, `${label} cannot be inspected (${error instanceof Error ? error.message : String(error)})`); }
};

const manifestHash = (directory: string, roots: readonly string[] | undefined, index: number): string => {
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
      if (roots && !roots.some(root => strictInside(root, target))) fail(index, `package symlink ${relative} escapes the allowed roots`);
      try { assertSafeAgentExtensionPathChain(target, `package symlink ${relative} target`); }
      catch (error) { fail(index, error instanceof Error ? error.message : String(error)); }
      entries.push([relative, "symlink", path.relative(directory, target).split(path.sep).join("/")]);
      visit(target, relative);
      return;
    }

    const canonical = realpath(candidate, index, `package entry ${relative || "."}`);
    if (roots && !roots.some(root => canonical === root || strictInside(root, canonical))) {
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

const hashExtensionPath = (canonical: string, roots: readonly string[] | undefined, index: number): string => {
  const stats = statPath(canonical, index, "path");
  safeStats(stats, "path", index);
  if (stats.isFile()) {
    if (stats.size > MAX_MANIFEST_BYTES) fail(index, `extension file exceeds ${MAX_MANIFEST_BYTES} bytes`);
    let bytes: Buffer;
    try { bytes = fs.readFileSync(canonical); }
    catch (error) { return fail(index, `file cannot be read (${error instanceof Error ? error.message : String(error)})`); }
    return digest(bytes);
  }
  if (stats.isDirectory()) {
    const packageFile = path.join(canonical, "package.json");
    let packageStats: fs.Stats;
    try { packageStats = fs.statSync(packageFile); }
    catch { return fail(index, "directory is not a package (package.json is missing)"); }
    if (!packageStats.isFile()) fail(index, "directory is not a package (package.json is not a regular file)");
    return manifestHash(canonical, roots, index);
  }
  return fail(index, "path must resolve to a regular file or package directory");
};

/** Resolve an ID or exact host-pinned path, then verify its pin before admission. */
export const validateAgentExtensionPaths = (
  value: unknown,
  configuredPins: readonly FabricAgentExtensionPin[] | undefined,
  configuredRoots: readonly string[] | undefined,
  cwd: string,
): AgentExtensionPathReceipt[] => {
  if (!Array.isArray(value)) return fail(undefined, `expected at most ${MAX_AGENT_EXTENSION_PATHS} extension IDs or exact paths`);
  if (value.length > MAX_AGENT_EXTENSION_PATHS || value.some(entry => typeof entry !== "string" || !entry.trim() || entry.length > 4096 || /[\u0000-\u001f\u007f]/u.test(entry))) {
    return fail(undefined, `expected at most ${MAX_AGENT_EXTENSION_PATHS} extension IDs or exact paths`);
  }
  if (!configuredPins?.length) return fail(undefined, "host agents.extensionPins is empty or unset; roots alone do not authorize extensions");
  let realRoots: string[];
  try { realRoots = configuredRoots?.length ? resolveAgentExtensionRoots(configuredRoots, cwd) : []; }
  catch (error) { return fail(undefined, error instanceof Error ? error.message : String(error)); }
  const receipts: AgentExtensionPathReceipt[] = [];
  const seen = new Set<string>();
  for (const [index, requested] of (value as string[]).entries()) {
    if (hasParentSegment(requested)) fail(index, "'..' path segments are not allowed");
    const pin = configuredPins.find(candidate => candidate.id === requested || candidate.path === requested);
    if (!pin) return fail(index, "value is not an ID or exact path in the host agents.extensionPins allow-list");
    let resolvedPin: FabricAgentExtensionPin | undefined;
    try { [resolvedPin] = resolveAgentExtensionPins([pin]); }
    catch (error) { return fail(index, error instanceof Error ? error.message : String(error)); }
    if (!resolvedPin) return fail(index, "host pin could not be resolved");
    const canonical = realpath(resolvedPin.path, index, "pinned extension path");
    if (canonical !== resolvedPin.path) fail(index, "pinned path no longer equals its realpath");
    if (realRoots.length > 0 && !realRoots.some(root => strictInside(root, canonical))) {
      fail(index, "pinned path is outside the optional agents.extensionRoots constraint");
    }
    if (seen.has(canonical)) fail(index, "duplicate extension pin");
    seen.add(canonical);
    try { assertSafeAgentExtensionPathChain(canonical, `pinned path component ${canonical}`); }
    catch (error) { fail(index, error instanceof Error ? error.message : String(error)); }
    const actual = hashExtensionPath(canonical, realRoots.length > 0 ? realRoots : undefined, index);
    if (actual !== resolvedPin.sha256) {
      fail(index, `extension ${resolvedPin.id} does not match its host-pinned SHA-256 (expected ${resolvedPin.sha256}, got ${actual})`);
    }
    receipts.push({ path: canonical, sha256: resolvedPin.sha256 });
  }
  return receipts;
};

/** Re-hash the admitted bytes in the worker immediately before starting Pi. */
export const verifyAgentExtensionPathReceipts = (receipts: readonly AgentExtensionPathReceipt[]): void => {
  for (const [index, receipt] of receipts.entries()) {
    const canonical = realpath(receipt.path, index, "admitted extension path");
    if (canonical !== receipt.path) fail(index, "path no longer resolves to its admitted canonical path");
    try { assertSafeAgentExtensionPathChain(canonical, `admitted path component ${canonical}`); }
    catch (error) { fail(index, error instanceof Error ? error.message : String(error)); }
    const actual = hashExtensionPath(canonical, undefined, index);
    if (actual !== receipt.sha256) {
      throw new Error(`Explicit extension changed after admission before Pi load: ${canonical} (expected SHA-256 ${receipt.sha256}, got ${actual}); refusing to start Pi`);
    }
  }
};
