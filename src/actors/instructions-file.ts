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
  const file = homePath(checked.instructionsFile);
  // Do not silently normalize an explicitly traversing caller path.
  if (file.split(/[\\/]/).includes("..")) throw new Error("instructionsFile must not contain '..' traversal");
  const defaultRoot = path.join(os.homedir(), ".local/share/smarty-dev/factory/current");
  const rootInput = configuredRoot ?? defaultRoot;
  const root = fs.realpathSync(homePath(rootInput));
  const resolved = fs.realpathSync(file);
  if (!within(root, resolved)) throw new Error("instructionsFile is outside the allowed instructions root");
  const expected = fs.statSync(resolved);
  if (!expected.isFile()) throw new Error("instructionsFile must be a regular file");
  if (expected.size > MAX_ACTOR_INSTRUCTIONS_FILE_BYTES) throw new Error("instructionsFile exceeds 512 KB");
  // Refuse a final-component swap and avoid blocking on a raced FIFO. fstat pins
  // the checked inode; a second realpath check also catches parent-link swaps.
  const fd = fs.openSync(resolved, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
  try {
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
  } finally { fs.closeSync(fd); }
};

export const assertActorInstructionReplacement = (current: string | undefined, instructions: string, replace?: boolean): void => {
  if (replace !== true && current !== undefined && instructions.length * 5 < current.length) {
    throw new Error(`Refusing setInstructions: new instructions (${instructions.length} chars) are more than 80% shorter than the current ${current.length} chars; pass replace: true to replace them`);
  }
};
