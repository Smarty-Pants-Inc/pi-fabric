import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";

export const MAX_ACTOR_INSTRUCTIONS_FILE_BYTES = 512 * 1024;

/** Unresolved input: only the owning host may read a file source. */
export type FabricActorInstructionsSource =
  | { instructions: string; instructionsFile?: never; sha256?: never }
  | { instructions?: never; instructionsFile: string; sha256: string };

export const actorInstructionsSource = (args: {
  instructions?: unknown; instructionsFile?: unknown; sha256?: unknown;
}): FabricActorInstructionsSource => {
  if (args.instructions !== undefined) {
    if (args.instructionsFile !== undefined || args.sha256 !== undefined) {
      throw new Error("Give inline instructions OR instructionsFile with sha256, not both");
    }
    if (typeof args.instructions !== "string") throw new Error("instructions must be a string");
    return { instructions: args.instructions };
  }
  if (typeof args.instructionsFile !== "string" || !args.instructionsFile ||
    typeof args.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(args.sha256)) {
    throw new Error("instructionsFile requires a path and a lowercase 64-hex sha256 digest");
  }
  return { instructionsFile: args.instructionsFile, sha256: args.sha256 };
};

const homePath = (input: string): string => /^~[\/\\]/.test(input) ? path.join(os.homedir(), input.slice(2)) : input;

const within = (root: string, file: string): boolean => {
  const relative = path.relative(root, file);
  return relative !== "" && relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
};

/** First-use only, bounded snapshot. Never persist or forward resolved file paths. */
export const resolveActorInstructions = (source: FabricActorInstructionsSource, configuredRoot?: string): string => {
  const checked = actorInstructionsSource(source);
  if (checked.instructions !== undefined) return checked.instructions;
  // Node has no portable openat / reparse-safe directory walk. Do not fall
  // back to pathname-only checks on Windows or other non-Linux hosts.
  if (process.platform !== "linux") throw new Error("instructionsFile requires Linux with /proc/self/fd; use inline instructions on this platform");
  // Inspect the caller spelling before homePath's path.join can erase '..'.
  if (checked.instructionsFile.split(/[\\/]/).includes("..")) throw new Error("instructionsFile must not contain '..' traversal");
  const file = homePath(checked.instructionsFile);
  const defaultRoot = path.join(os.homedir(), ".local/share/smarty-dev/factory/current");
  const rootInput = configuredRoot ?? defaultRoot;
  const root = fs.realpathSync(homePath(rootInput));
  const resolved = fs.realpathSync(file);
  if (!within(root, resolved)) throw new Error("instructionsFile is outside the allowed instructions root");
  const expected = fs.statSync(resolved);
  if (!expected.isFile()) throw new Error("instructionsFile must be a regular file");
  if (expected.size > MAX_ACTOR_INSTRUCTIONS_FILE_BYTES) throw new Error("instructionsFile exceeds 512 KB");
  const expectedRoot = fs.statSync(root);
  const handles: number[] = [];
  try {
    // Pin the canonical trusted root once. /proc/self/fd provides Node's Linux
    // equivalent of openat: the kernel follows our pinned descriptor, not a
    // mutable pathname to its directory. Never follow caller components.
    const rootFd = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
    handles.push(rootFd);
    const rootStat = fs.fstatSync(rootFd);
    if (!rootStat.isDirectory() || rootStat.dev !== expectedRoot.dev || rootStat.ino !== expectedRoot.ino ||
      fs.realpathSync(`/proc/self/fd/${rootFd}`) !== root) {
      throw new Error("instructionsFile allowed root changed");
    }
    const components = path.relative(root, resolved).split(path.sep);
    let fd = rootFd;
    for (let index = 0; index < components.length; index++) {
      const final = index === components.length - 1;
      fd = fs.openSync(`/proc/self/fd/${fd}/${components[index]}`,
        fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK |
        (final ? 0 : fs.constants.O_DIRECTORY));
      handles.push(fd);
    }
    // Regular-file/inode checks also refuse final swaps and raced FIFOs without
    // blocking. The no-follow walk, not another pathname observation, contains
    // the read even if an ancestor is swapped and restored between checks.
    if (fs.realpathSync(`/proc/self/fd/${rootFd}`) !== root) throw new Error("instructionsFile allowed root changed");
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.dev !== expected.dev || stat.ino !== expected.ino || fs.realpathSync(file) !== resolved) {
      throw new Error("instructionsFile changed or is not a regular file");
    }
    if (stat.size > MAX_ACTOR_INSTRUCTIONS_FILE_BYTES) throw new Error("instructionsFile exceeds 512 KB");
    // One bounded content read (including a sentinel byte for concurrent growth).
    const buffer = Buffer.alloc(MAX_ACTOR_INSTRUCTIONS_FILE_BYTES + 1);
    let length = 0;
    while (length < buffer.length) {
      const read = fs.readSync(fd, buffer, length, buffer.length - length, null);
      if (read === 0) break;
      length += read;
    }
    if (length > MAX_ACTOR_INSTRUCTIONS_FILE_BYTES) throw new Error("instructionsFile exceeds 512 KB");
    const bytes = buffer.subarray(0, length);
    if (createHash("sha256").update(bytes).digest("hex") !== checked.sha256) {
      throw new Error("instructionsFile sha256 digest mismatch");
    }
    const text = bytes.toString("utf8");
    // Actor instructions are strings. Refuse lossy decoding rather than report
    // a digest different from the verified bytes (no BOM/newline normalization).
    if (!Buffer.from(text, "utf8").equals(bytes)) throw new Error("instructionsFile must contain valid UTF-8");
    return text;
  } finally {
    for (const fd of handles.reverse()) fs.closeSync(fd);
  }
};

export const assertActorInstructionReplacement = (current: string | undefined, instructions: string, replace?: boolean): void => {
  if (replace !== true && current !== undefined && instructions.length * 5 < current.length) {
    throw new Error(`Refusing setInstructions: new instructions (${instructions.length} chars) are more than 80% shorter than the current ${current.length} chars; pass replace: true to replace them`);
  }
};
