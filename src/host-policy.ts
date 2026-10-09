import fs from "node:fs";
import path from "node:path";

// Production bundles substitute the fixed root-owned path below. Vitest alone
// supplies private fixture paths; neither environment nor fabric.json selects it.
declare const __FABRIC_HOST_POLICY_PATH__: string;
export const HOST_POLICY_PATH = typeof __FABRIC_HOST_POLICY_PATH__ === "undefined"
  ? "/etc/smarty/fabric-policy.json" : __FABRIC_HOST_POLICY_PATH__;

export type HostPolicy =
  | { status: "valid"; document: Record<string, unknown> }
  | { status: "missing" | "invalid" };

let warnedMissing = false;
let warnedInvalid = false;
let warnedAgentDisabled = false;

export const warnAgentLandlockDisabled = (): void => {
  if (warnedAgentDisabled) return;
  warnedAgentDisabled = true;
  console.warn(`[pi-fabric] agent-dir executor.landlock.disabled is not trusted host policy; ignored; provision ${HOST_POLICY_PATH} as root`);
};

const trustedMode = (stat: fs.Stats): boolean => stat.uid === 0 && (stat.mode & 0o022) === 0;

/** Read-only authority source. Reject symlinks, unsafe ancestry and open races. */
export const readHostPolicy = (): HostPolicy => {
  let descriptor: number | undefined;
  let foundFile = false;
  try {
    const before = fs.lstatSync(HOST_POLICY_PATH);
    foundFile = true;
    if (!before.isFile() || !trustedMode(before)) throw new Error("unsafe policy file");
    for (let directory = path.dirname(HOST_POLICY_PATH);;) {
      const stat = fs.lstatSync(directory);
      if (!stat.isDirectory() || !trustedMode(stat)) throw new Error("unsafe policy directory");
      const parent = path.dirname(directory);
      if (parent === directory) break;
      directory = parent;
    }
    // O_NONBLOCK also prevents a regular-file -> FIFO race from hanging open.
    if (typeof fs.constants.O_NOFOLLOW !== "number") throw new Error("O_NOFOLLOW unavailable");
    descriptor = fs.openSync(HOST_POLICY_PATH,
      fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
    const opened = fs.fstatSync(descriptor); // Exactly one fstat on the opened fd.
    if (!opened.isFile() || !trustedMode(opened) || opened.dev !== before.dev || opened.ino !== before.ino) {
      throw new Error("policy changed while opening");
    }
    const document: unknown = JSON.parse(fs.readFileSync(descriptor, "utf8"));
    if (!document || typeof document !== "object" || Array.isArray(document)) throw new Error("policy must be a JSON object");
    return { status: "valid", document: document as Record<string, unknown> };
  } catch (error) {
    // No valid root policy ever falls back to agent authority. Missing and
    // existing-but-untrusted policies both fail safe.
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
      console.warn(`[pi-fabric] ignoring untrusted or unreadable host policy at ${HOST_POLICY_PATH}; agent-dir authority relaxations are disabled`);
    }
    return { status: "invalid" };
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }
};
