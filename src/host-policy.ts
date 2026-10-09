import fs from "node:fs";
import path from "node:path";

// Replaced by the bundler. Neither the environment nor fabric.json can select
// this path or the rollout gate. Vitest supplies private fixture paths only.
declare const __FABRIC_HOST_POLICY_PATH__: string;
declare const __FABRIC_REQUIRE_HOST_POLICY__: boolean;
export const HOST_POLICY_PATH = typeof __FABRIC_HOST_POLICY_PATH__ === "undefined"
  ? "/etc/smarty/fabric-policy.json" : __FABRIC_HOST_POLICY_PATH__;
export const REQUIRE_HOST_POLICY = typeof __FABRIC_REQUIRE_HOST_POLICY__ === "undefined"
  ? false : __FABRIC_REQUIRE_HOST_POLICY__;

export type HostPolicy =
  | { status: "valid"; document: Record<string, unknown> }
  | { status: "missing" | "invalid" };

let warnedMissing = false;
let warnedInvalid = false;
let warnedAgentDisabled = false;

export const warnAgentLandlockDisabled = (legacy: boolean): void => {
  if (warnedAgentDisabled) return;
  warnedAgentDisabled = true;
  console.warn(`[pi-fabric] agent-dir executor.landlock.disabled is not trusted host policy; ${legacy
    ? "honoured only during the missing-host-policy rollout window"
    : "ignored; provision /etc/smarty/fabric-policy.json as root"}`);
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
    // Only an absent policy is eligible for legacy compatibility. Existing but
    // untrusted/unreadable/malformed policy never falls back to agent authority.
    const missing = !foundFile && (error as NodeJS.ErrnoException).code === "ENOENT";
    if (missing) {
      if (!warnedMissing) {
        warnedMissing = true;
        console.warn(`[pi-fabric] ${HOST_POLICY_PATH} is missing; ${REQUIRE_HOST_POLICY
          ? "agent-dir authority relaxations are disabled"
          : "retaining legacy agent-dir host policy during rollout"}. Hosts must provision root-owned policy; the follow-up release disables this compatibility window.`);
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

export const legacyHostPolicyAllowed = (policy: HostPolicy): boolean =>
  policy.status === "missing" && !REQUIRE_HOST_POLICY;
