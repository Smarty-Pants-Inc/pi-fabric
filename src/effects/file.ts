import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import { constants } from "node:fs";
import { lstat, open, rename } from "node:fs/promises";
import type { FileHandle } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { dirname, isAbsolute, join, parse, relative, resolve, sep } from "node:path";
import { TextDecoder } from "node:util";
import type { EffectAdapterControls } from "./failpoints.js";

// This bounded sink requires exclusive volume access and exclusion of out-of-band
// writers. A pathname/lease check is not a kernel fencing primitive. The parent
// owns durable INTENT/RESULT records and must not advance path history while an
// intent is pending. No ordinary Pi writer (or in-place fallback) is used here.
export const MAX_EFFECT_FILE_BYTES = 4 * 1024 * 1024;
const FORMAT = "fabric-file-effect-v1";
const HASH = /^[a-f0-9]{64}$/;

export type EffectFileState = Readonly<{
  exists: boolean;
  sha256: string | null;
  mode: number | null;
  size: number;
}>;

export interface PreparedFileEffect {
  readonly format: typeof FORMAT;
  readonly workspaceRoot: string;
  readonly path: string;
  readonly tool: "write" | "edit";
  readonly displayPath: string;
  readonly replacementCount: number;
  readonly pre: EffectFileState;
  readonly post: EffectFileState;
  readonly desiredBase64: string;
  readonly preparedResult: Readonly<{
    content: readonly Readonly<{ type: "text"; text: string }>[];
    details: Readonly<Record<string, never>>;
  }>;
  readonly integrity: string;
}

export type FileEffectOutcome = {
  outcome: "committed" | "not-committed" | "uncertain";
  result?: AgentToolResult<unknown>;
  reason?: string;
};

type Identity = { dev: bigint; ino: bigint };
type DirectoryIdentity = Identity & { path: string };
type Snapshot = {
  state: EffectFileState;
  bytes: Buffer;
  identity?: Identity;
  stamp?: string;
};

export function effectBytesHash(bytes: Uint8Array | string): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function fail(message: string): never {
  throw new Error(`EFFECT_FILE_UNSAFE: ${message}`);
}

function validText(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length > MAX_EFFECT_FILE_BYTES || /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value)) {
    fail(`${name} must be a valid Unicode string within the certified size bound`);
  }
  return value;
}

/** Lexical validation only; inspectEffectPath also checks every on-disk ancestor. */
export function resolveEffectPath(workspaceRoot: string, inputPath: string): { workspaceRoot: string; path: string; absolutePath: string } {
  validText(workspaceRoot, "workspaceRoot");
  validText(inputPath, "path");
  if (!isAbsolute(workspaceRoot) || resolve(workspaceRoot) !== workspaceRoot || workspaceRoot === parse(workspaceRoot).root) fail("workspaceRoot must be a canonical absolute non-root directory");
  // Absolute paths have exactly one permitted initial empty component. Reject
  // normalization/traversal instead of silently changing the authorized target.
  const components = (inputPath.startsWith("/") ? inputPath.slice(1) : inputPath).split("/");
  if (!inputPath || inputPath.includes("\0") || inputPath.includes("\\") || components.some((part) => !part || part === "." || part === "..")) fail("non-canonical path or traversal");
  const absolutePath = isAbsolute(inputPath) ? inputPath : join(workspaceRoot, inputPath);
  const path = relative(workspaceRoot, absolutePath);
  if (!path || path === ".." || path.startsWith(`..${sep}`) || isAbsolute(path)) fail("target must be strictly beneath workspaceRoot");
  return { workspaceRoot, path, absolutePath };
}

function requireNoFollow(): void {
  if (!constants.O_NOFOLLOW || !constants.O_DIRECTORY) fail("this platform cannot enforce no-follow directory/file opens");
}

function identityEqual(a: Identity, b: Identity): boolean {
  return a.dev === b.dev && a.ino === b.ino;
}

async function inspectDirectories(absolutePath: string): Promise<DirectoryIdentity[]> {
  requireNoFollow();
  const parent = dirname(absolutePath);
  const root = parse(parent).root;
  const paths = [root];
  let current = root;
  for (const part of relative(root, parent).split(sep).filter(Boolean)) {
    current = join(current, part);
    paths.push(current);
  }
  const identities: DirectoryIdentity[] = [];
  for (const path of paths) {
    const stat = await lstat(path, { bigint: true });
    if (!stat.isDirectory() || stat.isSymbolicLink()) fail(`ancestor is not a real directory: ${path}`);
    identities.push({ path, dev: stat.dev, ino: stat.ino });
  }
  return identities;
}

async function sameDirectories(expected: DirectoryIdentity[]): Promise<void> {
  for (const prior of expected) {
    const stat = await lstat(prior.path, { bigint: true });
    if (!stat.isDirectory() || !identityEqual(prior, stat)) fail(`ancestor changed: ${prior.path}`);
  }
}

function stamp(stat: { size: bigint; mtimeNs: bigint; ctimeNs: bigint; mode: bigint; nlink: bigint }): string {
  return `${stat.size}/${stat.mtimeNs}/${stat.ctimeNs}/${stat.mode}/${stat.nlink}`;
}

async function readSnapshot(absolutePath: string, sync = false): Promise<Snapshot> {
  let handle: FileHandle;
  try {
    // lstat before open avoids blocking on FIFOs/devices; O_NONBLOCK also closes
    // the lstat/open race. fstat, not lstat, authorizes reading the opened file.
    const entry = await lstat(absolutePath, { bigint: true });
    if (!entry.isFile() || entry.nlink !== 1n) fail("target must be a regular, single-link file");
    handle = await open(absolutePath, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { state: { exists: false, sha256: null, mode: null, size: 0 }, bytes: Buffer.alloc(0) };
    throw error;
  }
  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile() || before.nlink !== 1n || before.size > BigInt(MAX_EFFECT_FILE_BYTES)) fail("target is nonregular, linked or too large");
    // A bounded positional read never allocates from a concurrently growing file.
    const bytes = Buffer.alloc(Number(before.size));
    let offset = 0;
    while (offset < bytes.length) {
      const read = await handle.read(bytes, offset, bytes.length - offset, offset);
      if (!read.bytesRead) fail("target changed while reading");
      offset += read.bytesRead;
    }
    if (sync) await handle.sync();
    const after = await handle.stat({ bigint: true });
    const entry = await lstat(absolutePath, { bigint: true });
    if (!identityEqual(before, after) || !identityEqual(after, entry) || !entry.isFile() || entry.nlink !== 1n || stamp(before) !== stamp(after) || stamp(entry) !== stamp(after)) fail("target changed while reading");
    return {
      bytes,
      state: { exists: true, sha256: effectBytesHash(bytes), mode: Number(after.mode & 0o7777n), size: bytes.length },
      identity: { dev: after.dev, ino: after.ino },
      stamp: stamp(after),
    };
  } finally {
    await handle.close();
  }
}

/** Shared by the fixed read/hash adapter; never follows symlinks or creates directories. */
export async function inspectEffectPath(workspaceRoot: string, inputPath: string): Promise<{ absolutePath: string; path: string; state: EffectFileState }> {
  const target = resolveEffectPath(workspaceRoot, inputPath);
  const dirs = await inspectDirectories(target.absolutePath);
  const current = await readSnapshot(target.absolutePath);
  await sameDirectories(dirs);
  return { absolutePath: target.absolutePath, path: target.path, state: current.state };
}

const targetLocks = new Map<string, Promise<void>>();
async function withTargetLock<T>(target: string, operation: () => Promise<T>): Promise<T> {
  const prior = targetLocks.get(target) ?? Promise.resolve();
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  const tail = prior.then(() => held);
  targetLocks.set(target, tail);
  await prior;
  try {
    return await operation();
  } finally {
    release();
    if (targetLocks.get(target) === tail) targetLocks.delete(target);
  }
}

function stateEqual(a: EffectFileState, b: EffectFileState): boolean {
  return a.exists === b.exists && a.sha256 === b.sha256 && a.mode === b.mode && a.size === b.size;
}

function resultFor(tool: "write" | "edit", path: string, count: number): PreparedFileEffect["preparedResult"] {
  return { content: [{ type: "text", text: tool === "write" ? `Successfully wrote to ${path}` : `Successfully replaced ${count} block(s) in ${path}.` }], details: {} };
}

function portableResult(intent: PreparedFileEffect): AgentToolResult<unknown> {
  return { content: intent.preparedResult.content.map((part) => ({ ...part })), details: {} };
}

function freezeIntent(intent: PreparedFileEffect): PreparedFileEffect {
  Object.freeze(intent.pre);
  Object.freeze(intent.post);
  intent.preparedResult.content.forEach(Object.freeze);
  Object.freeze(intent.preparedResult.content);
  Object.freeze(intent.preparedResult.details);
  Object.freeze(intent.preparedResult);
  return Object.freeze(intent);
}

type IntentCore = Omit<PreparedFileEffect, "integrity">;
function seal(core: IntentCore): PreparedFileEffect {
  return freezeIntent({ ...core, integrity: effectBytesHash(JSON.stringify(core)) });
}

function exactKeys(args: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(args).some((key) => !keys.includes(key)) || keys.some((key) => !Object.hasOwn(args, key))) fail("unsupported argument shape");
}

export async function prepareFileEffect(workspaceRoot: string, tool: "write" | "edit", args: Record<string, unknown>): Promise<PreparedFileEffect> {
  if (tool !== "write" && tool !== "edit") fail("unsupported tool");
  exactKeys(args, tool === "write" ? ["path", "content"] : ["path", "edits"]);
  const displayPath = validText(args.path, "path");
  const target = resolveEffectPath(workspaceRoot, displayPath);
  // Snapshot arguments before any await; caller mutation cannot change preparation.
  let content: string | undefined;
  let edits: { oldText: string; newText: string }[] = [];
  if (tool === "write") content = validText(args.content, "content");
  else {
    if (!Array.isArray(args.edits) || !args.edits.length || args.edits.length > 128) fail("edits must contain 1..128 literal replacements");
    edits = args.edits.map((entry) => {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) fail("invalid edit");
      exactKeys(entry as Record<string, unknown>, ["oldText", "newText"]);
      const oldText = validText(entry.oldText, "oldText");
      if (!oldText.length) fail("empty oldText is not supported");
      return { oldText, newText: validText(entry.newText, "newText") };
    });
  }
  return withTargetLock(target.absolutePath, async () => {
    const dirs = await inspectDirectories(target.absolutePath);
    const pre = await readSnapshot(target.absolutePath);
    await sameDirectories(dirs);
    if (tool === "edit") {
      if (!pre.state.exists) fail("cannot edit an absent file");
      let original: string;
      try { original = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(pre.bytes); }
      catch { fail("literal edit requires valid UTF-8"); }
      const matches = edits.map((edit) => {
        const start = original.indexOf(edit.oldText);
        if (start < 0 || original.indexOf(edit.oldText, start + 1) >= 0) fail("oldText must occur exactly once in original bytes");
        return { ...edit, start, end: start + edit.oldText.length };
      }).sort((a, b) => a.start - b.start);
      for (let i = 1; i < matches.length; i++) if (matches[i]!.start < matches[i - 1]!.end) fail("overlapping edits");
      let cursor = 0;
      content = "";
      for (const edit of matches) {
        content += original.slice(cursor, edit.start) + edit.newText;
        cursor = edit.end;
      }
      content += original.slice(cursor);
    }
    const desired = Buffer.from(content!, "utf8");
    if (desired.length > MAX_EFFECT_FILE_BYTES) fail("desired bytes exceed the certified bound");
    return seal({
      format: FORMAT, workspaceRoot: target.workspaceRoot, path: target.path, tool, displayPath,
      replacementCount: edits.length, pre: pre.state,
      post: { exists: true, sha256: effectBytesHash(desired), mode: pre.state.exists ? pre.state.mode : 0o644, size: desired.length },
      desiredBase64: desired.toString("base64"), preparedResult: resultFor(tool, displayPath, edits.length),
    });
  });
}

function checkedIntent(value: PreparedFileEffect): PreparedFileEffect {
  // Copy/sanitize all fields before awaiting, including intents loaded from JSON.
  // The checksum detects corruption, not malicious changes by the trusted parent.
  if (!value || value.format !== FORMAT || (value.tool !== "write" && value.tool !== "edit")) fail("unsupported/corrupt prepared intent");
  const target = resolveEffectPath(value.workspaceRoot, value.path);
  if (target.path !== value.path || resolveEffectPath(value.workspaceRoot, value.displayPath).path !== value.path) fail("intent path mismatch");
  if (!Number.isSafeInteger(value.replacementCount) || value.replacementCount < 0 || value.replacementCount > 128 || (value.tool === "write" ? value.replacementCount !== 0 : value.replacementCount === 0)) fail("invalid replacement count");
  function state(input: EffectFileState): EffectFileState {
    if (!input || typeof input.exists !== "boolean" || !Number.isSafeInteger(input.size) || input.size < 0 || input.size > MAX_EFFECT_FILE_BYTES) fail("invalid file state");
    if (input.exists ? typeof input.sha256 !== "string" || !HASH.test(input.sha256) || !Number.isInteger(input.mode) || input.mode! < 0 || input.mode! > 0o7777 : input.sha256 !== null || input.mode !== null || input.size !== 0) fail("invalid hash/mode/existence");
    return { exists: input.exists, sha256: input.sha256, mode: input.mode, size: input.size };
  }
  const pre = state(value.pre), post = state(value.post);
  if (!post.exists || (value.tool === "edit" && !pre.exists) || post.mode !== (pre.exists ? pre.mode : 0o644)) fail("invalid post-state");
  if (typeof value.desiredBase64 !== "string" || value.desiredBase64.length > Math.ceil(MAX_EFFECT_FILE_BYTES / 3) * 4) fail("invalid stored bytes");
  const bytes = Buffer.from(value.desiredBase64, "base64");
  if (bytes.toString("base64") !== value.desiredBase64 || bytes.length !== post.size || effectBytesHash(bytes) !== post.sha256) fail("stored bytes/hash mismatch");
  const preparedResult = resultFor(value.tool, value.displayPath, value.replacementCount);
  const savedResult = value.preparedResult;
  if (!savedResult || !Array.isArray(savedResult.content) || savedResult.content.length !== 1 || savedResult.content[0]?.type !== "text" || savedResult.content[0]?.text !== preparedResult.content[0]!.text || !savedResult.details || typeof savedResult.details !== "object" || Array.isArray(savedResult.details) || Object.keys(savedResult.details).length !== 0) fail("corrupt prepared result");
  const clean = seal({ format: FORMAT, workspaceRoot: target.workspaceRoot, path: target.path, tool: value.tool, displayPath: value.displayPath, replacementCount: value.replacementCount, pre, post, desiredBase64: value.desiredBase64, preparedResult });
  if (clean.integrity !== value.integrity) fail("intent integrity mismatch");
  return clean;
}

async function directoryHandle(path: string, dirs: DirectoryIdentity[]): Promise<FileHandle> {
  const handle = await open(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat({ bigint: true });
    const expected = dirs.find((entry) => entry.path === path);
    if (!expected || !identityEqual(expected, stat)) fail("parent directory changed");
    return handle;
  } catch (error) { await handle.close(); throw error; }
}

async function commitChecked(intent: PreparedFileEffect, controls: EffectAdapterControls): Promise<AgentToolResult<unknown>> {
  const absolutePath = join(intent.workspaceRoot, intent.path);
  await controls.assertLive();
  const dirs = await inspectDirectories(absolutePath);
  const current = await readSnapshot(absolutePath);
  await sameDirectories(dirs);
  const durableDirectories: FileHandle[] = [];
  const syncDirectories = async () => {
    // Bottom-up barriers cover the complete provisioned namespace, not just the
    // final directory entry. Directories outside the workspace are checked for
    // aliases above, but are not owned/mutated by this sink.
    for (const handle of [...durableDirectories].reverse()) await handle.sync();
  };
  try {
    for (const entry of dirs) {
      if (entry.path === intent.workspaceRoot || entry.path.startsWith(`${intent.workspaceRoot}${sep}`)) {
        durableDirectories.push(await directoryHandle(entry.path, dirs));
      }
    }
    if (stateEqual(current.state, intent.post)) {
      // Lost ACK (or a genuine no-op): establish durability, return the original
      // prepared result, and crucially do not replace the destination inode.
      const confirmed = await readSnapshot(absolutePath, true);
      if (!stateEqual(confirmed.state, intent.post)) fail("post-state changed during confirmation");
      await syncDirectories();
      await sameDirectories(dirs);
      await controls.failpoint?.("destination-committed", { path: absolutePath, sha256: intent.post.sha256, reconciled: true });
      return portableResult(intent);
    }
    if (!stateEqual(current.state, intent.pre)) fail("target matches neither stored pre-state nor post-state; uncertain");
    await controls.assertLive();
    const tempPath = join(dirname(absolutePath), `.fabric-effect-${randomUUID()}.tmp`);
    // Never remove failed staging files: they are retained crash/I/O evidence.
    const temp = await open(tempPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    try {
      await temp.writeFile(Buffer.from(intent.desiredBase64, "base64"));
      await temp.chmod(intent.post.mode!);
      await temp.sync();
    } finally { await temp.close(); }
    await controls.failpoint?.("file-temp-synced", { path: absolutePath, tempPath });
    await controls.assertLive();
    await sameDirectories(dirs);
    const beforeRename = await readSnapshot(absolutePath);
    if (!stateEqual(beforeRename.state, intent.pre) || (current.identity && (!beforeRename.identity || !identityEqual(current.identity, beforeRename.identity) || current.stamp !== beforeRename.stamp))) fail("target changed before rename; uncertain");
    await controls.assertLive();
    await rename(tempPath, absolutePath);
    await controls.failpoint?.("file-renamed", { path: absolutePath, tempPath });
    await syncDirectories();
    const confirmed = await readSnapshot(absolutePath, true);
    await sameDirectories(dirs);
    if (!stateEqual(confirmed.state, intent.post)) fail("destination readback mismatch; uncertain");
    await controls.failpoint?.("destination-committed", { path: absolutePath, sha256: intent.post.sha256 });
    return portableResult(intent);
  } finally {
    // Close every pinned handle even if a close itself fails.
    const closed = await Promise.allSettled(durableDirectories.map((handle) => handle.close()));
    const failed = closed.find((entry) => entry.status === "rejected");
    if (failed?.status === "rejected") throw failed.reason;
  }
}

export async function commitFileEffect(intent: PreparedFileEffect, controls: EffectAdapterControls): Promise<AgentToolResult<unknown>> {
  const stored = checkedIntent(intent);
  return withTargetLock(join(stored.workspaceRoot, stored.path), () => commitChecked(stored, controls));
}

export async function reconcileFileEffect(intent: PreparedFileEffect, controls: EffectAdapterControls): Promise<FileEffectOutcome> {
  try {
    return { outcome: "committed", result: await commitFileEffect(intent, controls) };
  } catch (error) {
    // An I/O/ownership error is never evidence of absence. Keep the intent/temp
    // evidence pending; the parent must close admission rather than blind redo.
    return { outcome: "uncertain", reason: error instanceof Error ? error.message : String(error) };
  }
}
