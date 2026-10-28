import fs from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { windowsSecurityPowerShell } from "../storage/windows-powershell-command.js";
import path from "node:path";
import { syncDirectoryChain, writeJsonAtomic } from "../core/atomic-write.js";
import { addCloneFirstWorktree } from "./clone-first-worktree.js";
import { executeFile } from "./transports/process-utils.js";
import { fabricWorktreePath } from "./worktree-paths.js";

export interface CleanupRootCustody { source: string; destination: string; receipt: string; }

/** A verified shadow is NOT whole-object custody or permission to consume results. */
export interface CleanupCustodyDeferredStatus extends CleanupRootCustody {
  state: "held-in-place" | "copy-failed";
  originalRetained: true;
  verifiedCopyPath?: string;
}
export class CleanupCustodyDeferredError extends Error {
  readonly code = "CLEANUP_CUSTODY_DEFERRED";
  constructor(readonly cleanupStatus: CleanupCustodyDeferredStatus, cause?: unknown) {
    super(`Cleanup custody deferred (${cleanupStatus.state}): original retained at ${cleanupStatus.source}; external evidence: ${cleanupStatus.receipt}`, { cause });
    this.name = "CleanupCustodyDeferredError";
  }
}

/** Resolve even a not-yet-created archive through its existing physical ancestors. */
const physicalArchive = (directory: string): string => {
  // Normalizing '..' before resolving a symlink changes its physical meaning.
  // Refuse that ambiguous spelling rather than certify a different namespace.
  if (directory.split(/[\\/]+/).includes("..")) throw new Error("Custody path must not contain '..'");
  const tail: string[] = [];
  let current = path.resolve(directory);
  for (;;) {
    try { return path.join(fs.realpathSync.native(current), ...tail); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      // A dangling link is not a missing directory that we may create/adopt.
      if (fs.lstatSync(current, { throwIfNoEntry: false })?.isSymbolicLink()) throw error;
      const parent = path.dirname(current);
      if (parent === current) throw error;
      tail.unshift(path.basename(current)); current = parent;
    }
  }
};

/** All ancestors matter: a worktree can itself live inside another repository. */
const outermostGitRoot = (directory: string): string | undefined => {
  let outermost: string | undefined;
  for (let current = directory; ; current = path.dirname(current)) {
    if (fs.lstatSync(path.join(current, ".git"), { throwIfNoEntry: false }) ||
      (fs.lstatSync(path.join(current, "HEAD"), { throwIfNoEntry: false })?.isFile() &&
        fs.lstatSync(path.join(current, "objects"), { throwIfNoEntry: false })?.isDirectory() &&
        fs.lstatSync(path.join(current, "refs"), { throwIfNoEntry: false })?.isDirectory())) outermost = current;
    if (path.dirname(current) === current) return outermost;
  }
};

const externalArchive = (requested: string): string => {
  let archive = physicalArchive(requested);
  const seen = new Set<string>();
  for (;;) {
    const root = outermostGitRoot(archive);
    if (!root) return archive;
    if (seen.has(root) || path.dirname(root) === root) {
      throw Object.assign(new Error(`No external Git-free custody location for ${requested}; original root retained`), { code: "EXDEV" });
    }
    seen.add(root);
    const key = createHash("sha256").update(archive).digest("hex").slice(0, 20);
    archive = physicalArchive(path.join(path.dirname(root), ".pi-fabric-trace-archive", key));
  }
};
const existingDirectory = (directory: string): string => {
  for (let current = directory; ; current = path.dirname(current)) {
    const stat = fs.statSync(current, { throwIfNoEntry: false });
    if (stat) {
      if (!stat.isDirectory()) throw new Error(`Custody ancestor is not a directory: ${current}`);
      return current;
    }
    if (path.dirname(current) === current) throw new Error(`Custody ancestor missing: ${directory}`);
  }
};

/** Cheap filesystem metadata only; no mkdir, native command, engine or timer.
 * Prefer an external same-device namespace, including a safe source sibling and
 * the configured state area. Recheck at actual custody; mounts can change. */
export const cleanupArchiveOutsideGit = (requested: string, source?: string): string => {
  const first = externalArchive(requested);
  if (!source) return first;
  const parent = physicalArchive(path.dirname(source));
  const sourceDevice = fs.lstatSync(source, { throwIfNoEntry: false })?.dev ?? fs.statSync(existingDirectory(parent)).dev;
  const state = process.env.XDG_STATE_HOME ?? process.env.LOCALAPPDATA;
  const candidates = [first, path.join(parent, ".pi-fabric-trace-archive"),
    ...(state ? [path.join(state, "pi-fabric", "trace-archive")] : [])];
  for (const candidate of candidates) {
    try {
      const external = externalArchive(candidate);
      if (fs.statSync(existingDirectory(external)).dev === sourceDevice) return external;
    } catch (error) {
      // A refused optional sibling/state location cannot make the requested
      // external evidence location unsafe or authorize deleting the original.
      if (candidate === first) throw error;
    }
  }
  return first;
};

const protectCleanupContainer = (container: string): void => {
  if (process.platform !== "win32") {
    fs.chmodSync(container, 0o700);
    const stat = fs.lstatSync(container);
    if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== process.getuid?.() || (stat.mode & 0o777) !== 0o700) {
      throw new Error("Custody container owner/type/mode read-back mismatch");
    }
    return;
  }
  // chmod(0700) does not establish Windows privacy. Protect this fresh container's
  // inheritable DACL for the current account and SYSTEM, then verify exact bytes.
  const command = windowsSecurityPowerShell(String.raw`
$directory = $env:PI_FABRIC_CLEANUP_CONTAINER
$item = Get-Item -Force -LiteralPath $directory
if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'custody container is a reparse point' }
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [System.Security.AccessControl.DirectorySecurity]::new()
$acl.SetOwner($user)
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @($user.Value, 'S-1-5-18')) {
  $acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new(
    [System.Security.Principal.SecurityIdentifier]::new($sid),
    [System.Security.AccessControl.FileSystemRights]::FullControl,
    [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit,
    [System.Security.AccessControl.PropagationFlags]::None,
    [System.Security.AccessControl.AccessControlType]::Allow))
}
Set-Acl -LiteralPath $directory -AclObject $acl
$read = Get-Acl -LiteralPath $directory
if (-not $read.AreAccessRulesProtected -or $read.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -ne $user.Value) { throw 'custody DACL owner/protection mismatch' }
$actual = [System.Security.AccessControl.RawSecurityDescriptor]::new($read.GetSecurityDescriptorBinaryForm(), 0)
$expected = [System.Security.AccessControl.RawSecurityDescriptor]::new($acl.GetSecurityDescriptorBinaryForm(), 0)
if ($null -eq $actual.DiscretionaryAcl) { throw 'custody DACL is null' }
$a = [byte[]]::new($actual.DiscretionaryAcl.BinaryLength)
$e = [byte[]]::new($expected.DiscretionaryAcl.BinaryLength)
$actual.DiscretionaryAcl.GetBinaryForm($a, 0)
$expected.DiscretionaryAcl.GetBinaryForm($e, 0)
if ([Convert]::ToBase64String($a) -cne [Convert]::ToBase64String($e)) { throw 'custody DACL read-back mismatch' }
`, { ...process.env, PI_FABRIC_CLEANUP_CONTAINER: container });
  execFileSync(command.file, command.args, { env: command.env, timeout: 15_000, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
};

/** Bounded Linux byte-shadow boundary: current-owner ordinary files/directories
 * only; no links, special files, shared hardlinks, special mode bits or writable
 * group/other. Linux POSIX ACL effective write grants are bounded by that mask.
 * Copies are private 0600/0700, NOT metadata/ACL replicas. Other platforms' ACLs
 * are unproven here and refused. File reads use O_NOATIME; directory inventory
 * may update access times. No original bytes/names/owner/modes are written.
 * This detects observed changes, not an atomic snapshot of a concurrent writer;
 * the original remains authoritative forever, including all later FD appends. */
const copyCleanupShadow = (source: string, destination: string): Array<{ path: string; sha256?: string }> => {
  if (process.platform !== "linux" || !process.getuid || !fs.constants.O_NOATIME) {
    throw new Error("Cross-device copy refused: source ACL/atime semantics unsupported on this platform");
  }
  const uid = process.getuid(), maxEntries = 4096, maxBytes = 64 * 1024 * 1024;
  type Entry = { relative: string; identity: string; directory: boolean; size: number };
  const inspect = (file: string, relative: string): Entry => {
    const stat = fs.lstatSync(file, { bigint: true });
    if ((!stat.isFile() && !stat.isDirectory()) || stat.uid !== BigInt(uid) ||
      (stat.mode & 0o7022n) !== 0n || (stat.isFile() && stat.nlink !== 1n)) {
      throw new Error(`Cross-device copy refused: unsupported type/owner/mode/hardlink at ${relative || "."}`);
    }
    return { relative, directory: stat.isDirectory(), size: Number(stat.size),
      identity: [stat.dev, stat.ino, stat.mode, stat.uid, stat.gid, stat.nlink, stat.size, stat.mtimeNs, stat.ctimeNs].join(":") };
  };
  const inventory = (): Entry[] => {
    const rows: Entry[] = []; let bytes = 0;
    const walk = (file: string, relative: string): void => {
      if (rows.length >= maxEntries) throw new Error("Cross-device copy entry bound exceeded");
      const entry = inspect(file, relative); rows.push(entry);
      if (entry.directory) {
        for (const name of fs.readdirSync(file).sort()) walk(path.join(file, name), path.join(relative, name));
      } else if ((bytes += entry.size) > maxBytes) throw new Error("Cross-device copy byte bound exceeded");
    };
    walk(source, ""); return rows;
  };
  const before = inventory();
  const manifest: Array<{ path: string; sha256?: string }> = [];
  const buffer = Buffer.alloc(64 * 1024);
  for (const entry of before) {
    const from = path.join(source, entry.relative), to = path.join(destination, entry.relative);
    if (inspect(from, entry.relative).identity !== entry.identity) throw new Error("Source changed before copy");
    if (entry.directory) { fs.mkdirSync(to, { mode: 0o700 }); fs.chmodSync(to, 0o700); manifest.push({ path: entry.relative }); continue; }
    const input = fs.openSync(from, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NOATIME);
    try {
      const opened = fs.fstatSync(input, { bigint: true });
      if ([opened.dev, opened.ino, opened.mode, opened.uid, opened.gid, opened.nlink, opened.size, opened.mtimeNs, opened.ctimeNs].join(":") !== entry.identity) throw new Error("Source replaced before copy open");
      const output = fs.openSync(to, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
      const hash = createHash("sha256"); let copied = 0;
      try {
        for (;;) {
          const count = fs.readSync(input, buffer, 0, buffer.length, null);
          if (!count) break;
          if ((copied += count) > entry.size) throw new Error("Source grew during copy");
          hash.update(buffer.subarray(0, count));
          for (let offset = 0; offset < count;) {
            const written = fs.writeSync(output, buffer, offset, count - offset);
            if (!written) throw new Error("Copy made no write progress");
            offset += written;
          }
        }
        if (copied !== entry.size) throw new Error("Source shortened during copy");
        fs.fchmodSync(output, 0o600); fs.fsyncSync(output);
      } finally { fs.closeSync(output); }
      // Independent descriptor and full read, not a hash of the write buffer.
      const readback = fs.openSync(to, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
      const verified = createHash("sha256"); let read = 0;
      try {
        const stat = fs.fstatSync(readback);
        if (!stat.isFile() || stat.uid !== uid || (stat.mode & 0o777) !== 0o600 || stat.nlink !== 1 || stat.size !== copied) throw new Error("Copy read-back owner/mode/type mismatch");
        for (;;) { const count = fs.readSync(readback, buffer, 0, buffer.length, null); if (!count) break; read += count; if (read > copied) throw new Error("Copy grew during read-back"); verified.update(buffer.subarray(0, count)); }
      } finally { fs.closeSync(readback); }
      const digest = hash.digest("hex");
      if (read !== copied || verified.digest("hex") !== digest) throw new Error("Copy independent SHA256 read-back mismatch");
      if (inspect(from, entry.relative).identity !== entry.identity) throw new Error("Source changed during copy");
      manifest.push({ path: entry.relative, sha256: digest });
    } finally { fs.closeSync(input); }
  }
  if (JSON.stringify(inventory()) !== JSON.stringify(before)) throw new Error("Source tree changed during copy");
  // Sync every copied directory, not just the top entry, before its receipt.
  for (const entry of before.filter(row => row.directory).reverse()) syncDirectoryChain(path.join(destination, entry.relative));
  syncDirectoryChain(path.dirname(destination));
  return manifest;
};

/** Whole-object rename when possible; a verified EXDEV shadow always defers cleanup. */
export const retainCleanupRoot = (
  source: string, archive: string, id: string, reason = "agent cleanup whole-root custody",
): CleanupRootCustody | undefined => {
  if (!fs.lstatSync(source, { throwIfNoEntry: false })) return undefined;
  const relative = path.relative(path.resolve(source), path.resolve(archive));
  if (relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))) {
    throw new Error(`Cleanup custody archive must be outside source: ${source} -> ${archive}`);
  }
  archive = cleanupArchiveOutsideGit(archive, source);
  const physicalSource = path.join(physicalArchive(path.dirname(source)), path.basename(source));
  const physicalRelative = path.relative(physicalSource, archive);
  if (physicalRelative === "" || (physicalRelative !== ".." && !physicalRelative.startsWith(`..${path.sep}`) && !path.isAbsolute(physicalRelative))) {
    throw new Error(`Cleanup custody archive must be physically outside source: ${source} -> ${archive}`);
  }
  fs.mkdirSync(archive, { recursive: true, mode: 0o700 });
  if (outermostGitRoot(physicalArchive(archive))) throw new Error("Custody archive acquired a Git ancestor");
  if (process.platform !== "win32") {
    const uid = process.getuid?.();
    for (let current = archive; ; current = path.dirname(current)) {
      const stat = fs.lstatSync(current);
      // A trusted sticky temp parent cannot replace another owner's entry.
      if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.uid !== uid && stat.uid !== 0) ||
        ((stat.mode & 0o022) !== 0 && (stat.mode & 0o1000) === 0)) {
        throw new Error(`Custody archive ancestor is not owner-controlled: ${current}`);
      }
      if (path.dirname(current) === current) break;
    }
  }
  const container = fs.mkdtempSync(path.join(archive, "cleanup-"));
  protectCleanupContainer(container);
  const move = { source, destination: path.join(container, "tree"), receipt: path.join(container, "custody.json") };
  const prepared = { format: 1, id, reason, source, destination: move.destination, time: Date.now(), state: "prepared" };
  const save = (state: string, error?: unknown): void => writeJsonAtomic(move.receipt, {
    ...prepared, time: Date.now(), state, ...(error === undefined ? {} : { error: String(error) }),
  }, { durable: true, newline: true });
  // Keep an immutable prepared receipt even if publication of a later state fails.
  // Durable writes sync the file AND all containing directory entries through
  // the existing parent/root (Windows skips unsupported directory barriers).
  writeJsonAtomic(path.join(container, "custody.prepared.json"), prepared, { durable: true, newline: true });
  save("prepared");
  try {
    // A fresh container's never-used child cannot clobber earlier custody.
    fs.renameSync(source, move.destination);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    // Unexpected EXDEV despite equal device metadata is a refusal, not evidence
    // that every external same-filesystem candidate has been exhausted.
    if (code === "EXDEV" && fs.lstatSync(source).dev !== fs.statSync(container).dev) {
      let status: CleanupCustodyDeferredStatus = { ...move, state: "copy-failed", originalRetained: true };
      try {
        const manifest = copyCleanupShadow(source, move.destination);
        writeJsonAtomic(move.receipt, { ...prepared, time: Date.now(), state: "held-in-place",
          originalRetained: true, verifiedCopyPath: move.destination, manifest,
          copyBoundary: "linux-owned-regular-tree; private-byte-shadow; no inode/ACL/snapshot guarantee" }, { durable: true, newline: true });
        status = { ...move, state: "held-in-place", originalRetained: true, verifiedCopyPath: move.destination };
      } catch (copyError) {
        try { writeJsonAtomic(move.receipt, { ...prepared, time: Date.now(), ...status, error: String(copyError) }, { durable: true, newline: true }); } catch { /* Keep immutable preparation and partial evidence. */ }
        const failure = new CleanupCustodyDeferredError(status, copyError);
        console.warn(`[pi-fabric] ${failure.message}; ${String(copyError)}`); throw failure;
      }
      const deferred = new CleanupCustodyDeferredError(status, error);
      console.warn(`[pi-fabric] ${deferred.message}; verified shadow only, NOT cleaned`); throw deferred;
    }
    try { save("rename-failed", error); } catch { /* Durable prepared paths remain. */ }
    const refusal = new Error(`Cleanup custody refused${code ? ` (${code})` : ""}: ${source} -> ${move.destination}; no copy/delete fallback: ${String(error)}`, { cause: error });
    console.warn(`[pi-fabric] ${refusal.message}`);
    throw refusal;
  }
  try {
    // Make the held entry durable BEFORE the separate source unlink barrier
    // or a fallible moved-receipt publication. A failure never rolls back.
    syncDirectoryChain(path.dirname(move.destination));
    syncDirectoryChain(path.dirname(source));
    save("moved");
  } catch (error) {
    // No rollback, unlink, or canonical-path retry after the move committed.
    const failure = Object.assign(new Error(`Cleanup root retained at ${move.destination} (from ${source}); custody receipt update failed: ${String(error)}`, { cause: error }), { retainedRoot: move });
    console.warn(`[pi-fabric] ${failure.message}; prepared receipt: ${path.join(container, "custody.prepared.json")}`);
    throw failure;
  }
  console.warn(`[pi-fabric] cleanup retained whole root: ${source} -> ${move.destination}; receipt: ${move.receipt}`);
  return move;
};

export interface WorktreeLease {
  gitRoot: string;
  path: string;
  /** Effective child cwd inside the generated worktree. */
  cwd: string;
  branch: string;
  /** Commit the worktree branch started from. */
  baseRef?: string;
}

/** Settlement snapshot of a generated worktree. */
export interface AgentWorktreeResult {
  path: string;
  branch?: string;
  baseRef?: string;
  /** Sorted repository-relative paths, at most MAX_CHANGED_FILES. */
  changedFiles: string[];
  diffstat: { files: number; insertions: number; deletions: number };
  kept: boolean;
  diffError?: string;
}

const MAX_CHANGED_FILES = 500;
const GIT_SUMMARY_TIMEOUT_MS = 15_000;
const MAX_COUNTED_BYTES = 1024 * 1024;
const SETUP_TIMEOUT_MS = 10 * 60_000;
const SETUP_OUTPUT_TAIL_CHARS = 2_000;

// Untracked text files count as insertions; binary or large files count as 0.
const countLines = (file: string): number => {
  try {
    const stat = fs.statSync(file);
    if (!stat.isFile() || stat.size > MAX_COUNTED_BYTES) return 0;
    const content = fs.readFileSync(file);
    if (content.length === 0 || content.includes(0)) return 0;
    let lines = 0;
    for (const byte of content) if (byte === 0x0a) lines++;
    return content[content.length - 1] === 0x0a ? lines : lines + 1;
  } catch {
    return 0;
  }
};

const safeLabel = (value: string): string =>
  value
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 30) || "agent";

const isInside = (root: string, target: string): boolean => {
  const relative = path.relative(root, target);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
};

const worktreePrefixParts = (prefix: string): string[] | undefined => {
  const parts = prefix.split(/[\\/]+/).filter(Boolean);
  return parts.every((part) => part !== "." && part !== "..") ? parts : undefined;
};

export class WorktreeManager {
  readonly #leases = new Map<string, WorktreeLease>();
  readonly #archives = new Map<string, string>();

  async create(
    id: string,
    cwd: string,
    name: string,
    preserveSourceSubdirectory = false,
  ): Promise<WorktreeLease> {
    let gitRoot: string;
    let sourcePrefix = "";
    try {
      const [root, prefix] = await Promise.all([
        executeFile("git", ["rev-parse", "--show-toplevel"], { cwd }),
        preserveSourceSubdirectory
          ? executeFile("git", ["rev-parse", "--show-prefix"], { cwd })
          : Promise.resolve({ stdout: "" }),
      ]);
      const output = root.stdout.trim();
      if (!output) throw new Error("Git did not return a worktree root");
      gitRoot = fs.realpathSync(output);
      sourcePrefix = prefix.stdout.trim();
    } catch {
      throw new Error("Worktree isolation requires a Git repository");
    }
    const branch = `pi-fabric/${safeLabel(name)}-${id.slice(0, 8)}`;
    const worktreePath = fabricWorktreePath(gitRoot, id);
    // Initialization preflight is metadata-only; no eager PowerShell or engines.
    const archive = cleanupArchiveOutsideGit(`${path.dirname(worktreePath)}.trace-archive`, worktreePath);
    await addCloneFirstWorktree({
      gitRoot,
      dest: worktreePath,
      branch: { flag: "-b", name: branch },
      startPoint: "HEAD",
    });
    const canonicalWorktreePath = fs.realpathSync(worktreePath);
    const baseRef = await executeFile("git", ["rev-parse", "HEAD"], { cwd: worktreePath, timeoutMs: GIT_SUMMARY_TIMEOUT_MS })
      .then((result) => result.stdout.trim() || undefined, () => undefined);
    const prefix = preserveSourceSubdirectory ? worktreePrefixParts(sourcePrefix) : undefined;
    let effectiveCwd = canonicalWorktreePath;
    if (prefix && prefix.length > 0) {
      // Git reports its own worktree-relative prefix with `/` on every platform.
      // This avoids comparing independently canonicalized Windows paths, whose
      // volume/casing representation can differ even for the same directory.
      const candidate = path.resolve(canonicalWorktreePath, ...prefix);
      try {
        const canonicalCandidate = fs.realpathSync(candidate);
        if (fs.statSync(canonicalCandidate).isDirectory() && isInside(canonicalWorktreePath, canonicalCandidate)) {
          effectiveCwd = canonicalCandidate;
        }
      } catch {
        // The selected subdirectory may be untracked or absent from HEAD;
        // use the valid worktree root in that case.
      }
    }
    const lease = { gitRoot, path: worktreePath, cwd: effectiveCwd, branch, ...(baseRef ? { baseRef } : {}) };
    this.#leases.set(id, lease);
    this.#archives.set(id, archive);
    return lease;
  }

  get(id: string): WorktreeLease | undefined {
    return this.#leases.get(id);
  }

  /** Run a setup command in the worktree root; failure throws with the output tail. */
  async setup(id: string, command: string): Promise<void> {
    const lease = this.#leases.get(id);
    if (!lease) throw new Error(`Unknown Fabric worktree: ${id}`);
    const [file, args] = process.platform === "win32"
      ? [process.env.ComSpec || "cmd.exe", ["/d", "/s", "/c", command]]
      : ["/bin/sh", ["-c", command]];
    try {
      await executeFile(file, args, { cwd: lease.path, timeoutMs: SETUP_TIMEOUT_MS });
    } catch (error) {
      const failure = error as Error & { stdout?: string; stderr?: string; code?: unknown; killed?: boolean };
      const output = `${failure.stdout ?? ""}${failure.stderr ?? ""}`.trim();
      const tail = output.length > SETUP_OUTPUT_TAIL_CHARS ? `…${output.slice(-SETUP_OUTPUT_TAIL_CHARS)}` : output;
      const status = failure.killed ? "timed out" : `exited with ${String(failure.code ?? "an error")}`;
      throw new Error(`Worktree setup command ${status}${tail ? `:\n${tail}` : ""}`);
    }
  }

  /** Bounded diff against the base commit, including untracked files; git failures become diffError. */
  async summarize(id: string): Promise<AgentWorktreeResult | undefined> {
    const lease = this.#leases.get(id);
    if (!lease) return undefined;
    const result: AgentWorktreeResult = {
      path: lease.path,
      branch: lease.branch,
      ...(lease.baseRef ? { baseRef: lease.baseRef } : {}),
      changedFiles: [],
      diffstat: { files: 0, insertions: 0, deletions: 0 },
      kept: fs.existsSync(lease.path),
    };
    if (!result.kept) return result;
    try {
      const options = { cwd: lease.path, timeoutMs: GIT_SUMMARY_TIMEOUT_MS };
      const [tracked, untracked] = await Promise.all([
        executeFile("git", ["-c", "core.quotepath=off", "diff", "--numstat", "--no-renames", lease.baseRef ?? "HEAD", "--"], options),
        executeFile("git", ["-c", "core.quotepath=off", "ls-files", "--others", "--exclude-standard"], options),
      ]);
      const files = new Set<string>();
      for (const line of tracked.stdout.split("\n")) {
        const match = /^(-|\d+)\t(-|\d+)\t(.+)$/.exec(line);
        if (!match) continue;
        files.add(match[3]!);
        if (match[1] !== "-") result.diffstat.insertions += Number(match[1]);
        if (match[2] !== "-") result.diffstat.deletions += Number(match[2]);
      }
      for (const file of untracked.stdout.split("\n")) {
        if (!file || files.has(file)) continue;
        files.add(file);
        if (files.size <= MAX_CHANGED_FILES) result.diffstat.insertions += countLines(path.join(lease.path, file));
      }
      result.diffstat.files = files.size;
      result.changedFiles = [...files].sort().slice(0, MAX_CHANGED_FILES);
    } catch (error) {
      result.diffError = (error instanceof Error ? error.message : String(error)).split("\n", 1)[0]!.slice(0, 500);
    }
    return result;
  }

  async cleanup(id: string, deleteBranch = false): Promise<boolean> {
    const lease = this.#leases.get(id);
    if (!lease) return false;
    // Common to explicit cleanup AND automatic prelaunch/setup rollback. Never
    // run Git remove/prune here: even after rename those are not metadata-only.
    const deferred = `Git worktree registration cleanup deferred: ${JSON.stringify(lease.gitRoot)} still registers ${JSON.stringify(lease.path)}; no remove or prune was attempted.${deleteBranch ? ` Git branch deletion deferred: ${lease.branch} was not deleted.` : ""}`;
    try {
      retainCleanupRoot(lease.path, this.#archives.get(id) ?? `${path.dirname(lease.path)}.trace-archive`, id);
      this.#leases.delete(id); this.#archives.delete(id);
      console.warn(`[pi-fabric] cleanup ${id}: ${deferred}`);
    } catch (error) {
      // A post-move receipt failure ends path ownership too: a retry must not
      // capture a new canonical occupant. A pre-move refusal keeps the lease.
      if (error instanceof Error && "retainedRoot" in error) { this.#leases.delete(id); this.#archives.delete(id); }
      // Automatic rollback catches exceptions; keep refusal and Git debt visible.
      console.warn(`[pi-fabric] worktree cleanup refused for ${id}: ${String(error)}; ${deferred}`);
      throw error;
    }
    return true;
  }
}
