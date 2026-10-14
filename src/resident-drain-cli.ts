import fs from "node:fs";
import path from "node:path";
import { assertResidentChannelOwned, resolveResidentDirectory, type ResidentChannelOwnerProbe } from "./actors-cli.js";
import { residentHostId, residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "./residency/protocol.js";
import { processStartTime, residentProcessAlive } from "./residency/process-identity.js";

const usage = "Usage: fabric-resident-drain --resident <id-or-prefix-or-directory> --mesh-root <DIR> --expected-release <REAL_OLD_ROOT> --target-release <REAL_NEW_ROOT>";
const help = `${usage}

Read-only, fail-closed preflight; this command cannot drain or switch a resident.
All four flags are required. No environment or live-mesh default is used.
Release roots must be absolute, canonical, existing directories (no symlink aliases).
The expected release is checked against immutable owner.json launch fields, never
against the desired release in mutable config.json. Legacy owners without releaseRoot,
configDigest and a birth-bound handover launcher identity lack launch attestation.
Even attested hosts are refused with DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE:
no current legacy request closes admission and checkpoints accepted work.
setActivationFilter skips/consumes input rather than holding it; internal
pauseForRelease is unavailable externally; releaseChange is unconditionally deferred.
There is no unsafe bypass: no actor stop/control, shutdown, signals, custody-marker
removal, resident-root writes, ensureHost, reensure or target launch is attempted.
A scan, free lock, absent owner or empty process group is not a drain/exit proof.
unchanged:true means this command did not mutate state, not that the concurrent
service is frozen. transitionAttempted:false means no transition was attempted.
Observed fields are a read-time snapshot, not a concurrency fence. configDigest
is checked for structural shape only; its contents are not recomputed/validated.`;

const options = ["--resident", "--mesh-root", "--expected-release", "--target-release"] as const;
type Option = typeof options[number];

export interface ResidentDrainReceipt {
  ok: boolean;
  phase: "preflight";
  outcome: "refused" | "help";
  code: string;
  reason: string;
  resident: string | null;
  expected: string | null;
  target: string | null;
  /** This command did not mutate state; concurrent service activity is not frozen. */
  unchanged: true;
  transitionAttempted: false;
  /** Read-time owner/config observations, not a lock or complete launch validation. */
  observed?: {
    rootId: string | null;
    hostId: string | null;
    pid: number | null;
    processStartTime: string | null;
    token: string | null;
    loadedRelease: { releaseRoot: string | null; fabricExtensionPath: string | null };
  };
  rollback: { status: "not-needed"; reason: "no-transition-attempted" };
  elapsedMs: number;
  help?: string;
}

class Refusal extends Error {
  constructor(readonly code: string, reason: string) { super(reason); }
}

function refuse(code: string, reason: string): never { throw new Refusal(code, reason); }
function message(error: unknown): string { return error instanceof Error ? error.message : String(error); }

function realReleaseRoot(value: string, flag: string): string {
  try {
    if (!path.isAbsolute(value) || value !== fs.realpathSync(value) || !fs.statSync(value).isDirectory()) {
      refuse("INVALID_RELEASE_ROOT", `${flag} must name an absolute, canonical, existing release directory: ${value}`);
    }
    return value;
  } catch (error) {
    if (error instanceof Refusal) throw error;
    return refuse("INVALID_RELEASE_ROOT", `Cannot verify ${flag}: ${message(error)}`);
  }
}

function exactBirth(pid: unknown, birth: unknown): boolean {
  return typeof pid === "number" && Number.isSafeInteger(pid) && pid > 0 &&
    typeof birth === "string" && /^\d+$/.test(birth) &&
    processStartTime(pid) === birth && residentProcessAlive(pid, birth);
}

function realFile(file: unknown): file is string {
  return typeof file === "string" && path.isAbsolute(file) &&
    file === fs.realpathSync(file) && fs.statSync(file).isFile();
}

/** No transition is implemented: even structurally valid launch identity must refuse. */
export async function main(argv: string[], io: { out: (text: string) => void } = {
  out: text => process.stdout.write(text),
}, ownerProbe?: ResidentChannelOwnerProbe): Promise<number> {
  const started = Date.now();
  const values: Partial<Record<Option, string>> = {};
  let resident: string | null = null;
  let expected: string | null = null;
  let target: string | null = null;
  let observed: ResidentDrainReceipt["observed"];
  const receipt = (ok: boolean, code: string, reason: string): ResidentDrainReceipt => ({
    ok, phase: "preflight", outcome: ok ? "help" : "refused", code, reason,
    resident: resident ?? values["--resident"] ?? null,
    expected: expected ?? values["--expected-release"] ?? null,
    target: target ?? values["--target-release"] ?? null,
    unchanged: true, transitionAttempted: false, ...(observed ? { observed } : {}),
    rollback: { status: "not-needed", reason: "no-transition-attempted" },
    elapsedMs: Math.max(0, Date.now() - started),
  });
  try {
    if (argv.length === 1 && (argv[0] === "--help" || argv[0] === "-h")) {
      io.out(JSON.stringify({ ...receipt(true, "HELP", "Read-only preflight; no transition is available"), help }) + "\n");
      return 0;
    }
    for (let index = 0; index < argv.length; index++) {
      const option = argv[index] as Option;
      const value = argv[index + 1];
      if (!options.includes(option) || values[option] !== undefined || !value?.trim() || value.startsWith("-")) {
        refuse("INVALID_ARGUMENTS", usage);
      }
      values[option] = value;
      index++;
    }
    if (options.some(option => values[option] === undefined)) refuse("INVALID_ARGUMENTS", `All four flags are required. ${usage}`);
    expected = realReleaseRoot(values["--expected-release"]!, "--expected-release");
    target = realReleaseRoot(values["--target-release"]!, "--target-release");
    if (process.platform !== "linux") refuse("PROCESS_IDENTITY_UNAVAILABLE", "Drain preflight requires birth-bound Linux process identity");
    let meshRoot: string;
    try { meshRoot = fs.realpathSync(values["--mesh-root"]!); }
    catch (error) { return refuse("INVALID_MESH_ROOT", `Cannot resolve explicit --mesh-root: ${message(error)}`); }
    try { resident = resolveResidentDirectory(values["--resident"]!, meshRoot); }
    catch (error) { return refuse("RESIDENT_SELECTOR_INVALID", message(error)); }
    try {
      assertResidentChannelOwned([resident, path.join(resident, "config.json"), path.join(resident, "owner.json"),
        path.join(resident, "requests"), path.join(resident, "responses")], ownerProbe);
    } catch (error) { return refuse("RESIDENT_CHANNEL_UNOWNED", message(error)); }
    let config: ResidentHostConfig;
    let owner: ResidentHostOwner;
    try {
      config = JSON.parse(fs.readFileSync(path.join(resident, "config.json"), "utf8")) as ResidentHostConfig;
      owner = JSON.parse(fs.readFileSync(path.join(resident, "owner.json"), "utf8")) as ResidentHostOwner;
      if (!config || typeof config !== "object" || !owner || typeof owner !== "object") throw new Error("Config and owner must be objects");
    } catch (error) { return refuse("RESIDENT_STATE_INVALID", `Cannot read resident identity: ${message(error)}`); }
    // Record only fields actually read from config/owner, never desired release
    // selectors. These observations do not freeze or attest concurrent service.
    observed = {
      rootId: typeof config.rootId === "string" ? config.rootId : null,
      hostId: typeof owner.hostId === "string" ? owner.hostId : null,
      pid: typeof owner.pid === "number" ? owner.pid : null,
      processStartTime: typeof owner.processStartTime === "string" ? owner.processStartTime : null,
      token: typeof owner.token === "string" ? owner.token : null,
      loadedRelease: {
        releaseRoot: typeof owner.releaseRoot === "string" ? owner.releaseRoot : null,
        fabricExtensionPath: typeof owner.fabricExtensionPath === "string" ? owner.fabricExtensionPath : null,
      },
    };
    try {
      if (config.format !== 1 || owner.format !== 1 || typeof config.rootId !== "string" || !config.rootId.trim() ||
          typeof config.meshRoot !== "string" || typeof config.residencyRoot !== "string" ||
          owner.hostId !== residentHostId(config.rootId) || typeof owner.token !== "string" || !owner.token.trim() ||
          fs.realpathSync(config.meshRoot) !== meshRoot || fs.realpathSync(config.residencyRoot) !== resident ||
          fs.realpathSync(residentRoot(meshRoot, config.rootId)) !== resident) {
        refuse("RESIDENT_IDENTITY_MISMATCH", "Resident directory/config/owner/explicit mesh binding mismatch");
      }
    } catch (error) {
      if (error instanceof Refusal) throw error;
      return refuse("RESIDENT_IDENTITY_MISMATCH", `Cannot verify resident mesh binding: ${message(error)}`);
    }
    // Check both immutable loaded-path fields before the capability check so a
    // legacy owner can still prove a mismatch. Never use config.fabricExtensionPath.
    if ((owner.releaseRoot !== undefined && owner.releaseRoot !== expected) ||
        (owner.fabricExtensionPath !== undefined && owner.fabricExtensionPath !== path.join(expected, "dist/index.js"))) {
      refuse("EXPECTED_RELEASE_MISMATCH", "Immutable owner releaseRoot/fabricExtensionPath does not match --expected-release");
    }
    const launcher = owner.handover?.launcher;
    if (!owner.releaseRoot || !owner.fabricExtensionPath || !owner.configDigest || !launcher) {
      refuse("MISSING_LAUNCH_ATTESTATION", "Missing immutable releaseRoot, fabricExtensionPath, configDigest or handover launcher identity; legacy loaded paths alone cannot authorize drain");
    }
    if (typeof owner.processStartTime !== "string" || !/^\d+$/.test(owner.processStartTime)) {
      refuse("RESIDENT_BIRTH_MISSING", "Resident owner has no valid Linux process birth; PID-only identity is insufficient");
    }
    if (!exactBirth(owner.pid, owner.processStartTime)) {
      refuse("RESIDENT_BIRTH_MISMATCH", "Resident PID is not the exact live process recorded by owner.json (birth unavailable or mismatched)");
    }
    if (owner.handover?.abi !== "fabric-resident-1" || !/^[0-9a-f]{64}$/.test(owner.configDigest) ||
        typeof launcher.token !== "string" || !launcher.token.trim() ||
        typeof launcher.processStartTime !== "string" || !/^\d+$/.test(launcher.processStartTime) ||
        !Number.isSafeInteger(launcher.pid) || launcher.pid <= 0 ||
        launcher.entry !== path.join(expected, "dist/residency/pi-entry.js") ||
        typeof launcher.runtime !== "string" || !launcher.runtime) {
      refuse("MISSING_LAUNCH_ATTESTATION", "Malformed configDigest or handover launcher identity; complete immutable launch attestation is required");
    }
    if (!exactBirth(launcher.pid, launcher.processStartTime)) {
      refuse("LAUNCHER_BIRTH_MISMATCH", "Handover launcher PID is not its exact recorded live Linux birth");
    }
    try {
      if (!realFile(owner.fabricExtensionPath) || !realFile(launcher.entry) || !realFile(launcher.runtime)) {
        refuse("MISSING_LAUNCH_ATTESTATION", "Immutable launch entry/extension/runtime must be canonical existing files");
      }
    } catch (error) {
      if (error instanceof Refusal) throw error;
      return refuse("MISSING_LAUNCH_ATTESTATION", `Cannot verify immutable launch files: ${message(error)}`);
    }
    refuse("DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE", "Structural launch identity is available (configDigest shape only, digest not recomputed/validated). No current legacy request closes admission and checkpoints accepted work. setActivationFilter skips/consumes rather than holds; pauseForRelease is internal and unavailable externally; releaseChange is unconditionally deferred. No safe drain/exit proof or unsafe bypass is available; no transition attempted.");
  } catch (error) {
    io.out(JSON.stringify(receipt(false, error instanceof Refusal ? error.code : "PREFLIGHT_READ_FAILED", message(error))) + "\n");
    return 1;
  }
}
