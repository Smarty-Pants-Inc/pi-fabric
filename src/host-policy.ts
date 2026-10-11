import fs from "node:fs";
import path from "node:path";

// Production bundles substitute the fixed root-owned path below. Vitest alone
// supplies private fixture paths; neither environment nor fabric.json selects it.
declare const __FABRIC_HOST_POLICY_PATH__: string;
export const HOST_POLICY_PATH = typeof __FABRIC_HOST_POLICY_PATH__ === "undefined"
  ? "/etc/smarty/fabric-policy.json" : __FABRIC_HOST_POLICY_PATH__;

export type HostPolicy =
  | { status: "valid"; document: Record<string, unknown> }
  | { status: "missing" }
  | { status: "invalid" };

let warnedMissing = false;
let warnedInvalid = false;
let warnedAgentDisabled = false;

export const warnAgentLandlockDisabled = (): void => {
  if (warnedAgentDisabled) return;
  warnedAgentDisabled = true;
  console.warn(`[pi-fabric] agent-dir executor.landlock.disabled is not trusted host policy; ignored; provision ${HOST_POLICY_PATH} as root`);
};

const trustedMode = (stat: fs.Stats): boolean => stat.uid === 0 && (stat.mode & 0o022) === 0;

/** Validate all consumed authority fields before any grant acquires authority. */
const validatePolicy = (document: Record<string, unknown>): void => {
  const object = (value: unknown, name: string): Record<string, unknown> => {
    if (value === undefined) return {};
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${name} must be an object`);
    return value as Record<string, unknown>;
  };
  const landlock = object(object(document.executor, "executor").landlock, "executor.landlock");
  if (landlock.mode !== undefined && landlock.mode !== "off" && landlock.mode !== "enforce") {
    throw new Error("executor.landlock.mode must be off or enforce");
  }
  for (const key of ["disabled", "allowEscape"]) {
    if (landlock[key] !== undefined && typeof landlock[key] !== "boolean") {
      throw new Error(`executor.landlock.${key} must be a boolean`);
    }
  }
  const agents = object(document.agents, "agents");
  if (agents.processSlice !== undefined && (typeof agents.processSlice !== "string"
    || !/^[a-zA-Z0-9_.-]+\.slice$/.test(agents.processSlice))) throw new Error("agents.processSlice must be a slice name");
  const requireReason = object(agents.modelPolicy, "agents.modelPolicy").requireReason;
  for (const [name, value] of [["agents.deniedModels", agents.deniedModels], ["agents.modelPolicy.requireReason", requireReason]] as const) {
    if (value !== undefined && (!Array.isArray(value) || value.some(item => typeof item !== "string"))) {
      throw new Error(`${name} must be an array of strings`);
    }
  }
};

/** Read-only authority source. Reject symlinks, unsafe ancestry and open races. */
export const readHostPolicy = (): HostPolicy => {
  let descriptor: number | undefined;
  let foundFile = false;
  try {
    const before = fs.lstatSync(HOST_POLICY_PATH);
    foundFile = true;
    if (!before.isFile()) throw new Error("policy is not a regular file (symlinks are forbidden)");
    if (before.uid !== 0) throw new Error("policy owner is not root (uid 0)");
    if (!trustedMode(before)) throw new Error("policy mode permits group/world writes");
    for (let directory = path.dirname(HOST_POLICY_PATH);;) {
      const stat = fs.lstatSync(directory);
      if (!stat.isDirectory() || !trustedMode(stat)) throw new Error(`unsafe policy directory: ${directory}`);
      const parent = path.dirname(directory);
      if (parent === directory) break;
      directory = parent;
    }
    // O_NONBLOCK also prevents a regular-file -> FIFO race from hanging open.
    if (typeof fs.constants.O_NOFOLLOW !== "number") throw new Error("O_NOFOLLOW unavailable");
    descriptor = fs.openSync(HOST_POLICY_PATH,
      fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
    const opened = fs.fstatSync(descriptor);
    if (!opened.isFile() || !trustedMode(opened) || opened.dev !== before.dev || opened.ino !== before.ino) {
      throw new Error("policy changed while opening");
    }
    if (!Number.isSafeInteger(opened.size) || opened.size < 0) throw new Error("invalid policy size");
    // Root must publish by atomic rename, never rewrite this inode in place.
    // A short but valid JSON prefix must not acquire authority during a rewrite.
    const bytes = Buffer.alloc(opened.size);
    let count = 0;
    while (count < bytes.length) {
      const read = fs.readSync(descriptor, bytes, count, bytes.length - count, count);
      if (read === 0) break;
      count += read;
    }
    const after = fs.fstatSync(descriptor);
    if (count !== opened.size || !after.isFile() || !trustedMode(after)
      || after.size !== opened.size || after.dev !== opened.dev || after.ino !== opened.ino
      || after.mtimeMs !== opened.mtimeMs || after.ctimeMs !== opened.ctimeMs
      || after.uid !== opened.uid || after.mode !== opened.mode) {
      throw new Error("policy changed while reading or read was incomplete");
    }
    const document: unknown = JSON.parse(bytes.toString("utf8"));
    if (!document || typeof document !== "object" || Array.isArray(document)) throw new Error("policy must be a JSON object");
    validatePolicy(document as Record<string, unknown>);
    return { status: "valid", document: document as Record<string, unknown> };
  } catch (error) {
    // Only ENOENT before observing the file means missing. Any other failure
    // is unprovable policy and requires strict enforcement, not the off default.
    const missing = !foundFile && (error as NodeJS.ErrnoException).code === "ENOENT";
    if (missing) {
      if (!warnedMissing) {
        warnedMissing = true;
        console.warn(`[pi-fabric] ${HOST_POLICY_PATH} is missing; agent-dir authority relaxations are disabled. Hosts must provision root-owned policy.`);
      }
      return { status: "missing" };
    }
    if (!warnedInvalid) {
      warnedInvalid = true;
      const reason = error instanceof Error ? error.message : String(error);
      console.warn(`[pi-fabric] ignoring untrusted or unreadable host policy at ${HOST_POLICY_PATH}: ${reason}; strict Landlock enforce, no authority relaxations or escapes`);
    }
    return { status: "invalid" };
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }
};
