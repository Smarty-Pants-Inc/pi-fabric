import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { createHash } from "node:crypto";
import { ResidentActorClient } from "./residency/actor-client.js";
import { RESIDENT_DRAIN_ABI, residentDrainReceiptPath, type ResidentDrainIntent, type ResidentDrainReceipt as EndpointDrainReceipt } from "./residency/drain.js";
import { assertHandoverTopology, mainGenerationCurrent, mainGenerationPath, residentLaunchSpec,
  type ResidentMainGeneration, type ResidentLaunchSpec } from "./residency/handover.js";
import { hostLeasePath, readHostLeaseCurrent } from "./topology/host-leases.js";
import type { FabricParticipantRecord } from "./topology/types.js";
import type { MeshStateEntry } from "./mesh/store.js";
import { assertResidentChannelOwned, resolveResidentDirectory, type ResidentChannelOwnerProbe } from "./actors-cli.js";
import { residentHostId, residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "./residency/protocol.js";
import { processStartTime, residentProcessAlive } from "./residency/process-identity.js";
import { ContainmentUnavailableError, type AttemptLaunchReceipt } from "./residency/attempt-containment-contract.js";
import { readResidentLaunchAttestation } from "./residency/launch-attestation.js";

const usage = "Usage: fabric-resident-drain --resident <id-or-prefix-or-directory> --mesh-root <DIR> --expected-release <REAL_OLD_ROOT> --target-release <REAL_NEW_ROOT>";
const help = `${usage}

Fail-closed cooperative drain; this command cannot switch a resident.
A loaded host must advertise fabric-resident-drain-1 and releaseDrain. With a
healthy immutable launch snapshot and the existing live owning Main attestation,
this command asks A to hold/checkpoint/resume, then waits for its durable receipt.
Containment is unavailable: A remains serving, B is never launched, and the result
is nonzero containment-unavailable. This is partial delivery, not release success.
All four flags are required. No environment or live-mesh default is used.
Release roots must be absolute, canonical, existing directories (no symlink aliases).
The expected release is checked against immutable owner.json launch fields, never
against the desired release in mutable config.json. Legacy owners without releaseRoot,
configDigest and a birth-bound handover launcher identity lack launch attestation.
Legacy attested hosts are refused with DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE:
no current legacy request closes admission and checkpoints accepted work.
setActivationFilter skips/consumes input rather than holding it; internal
pauseForRelease is unavailable externally; releaseChange is unconditionally deferred.
There is no unsafe bypass: no actor stop/control, shutdown, signals, custody-marker
removal, ensureHost, reensure or target launch is attempted. Unsupported preflight
never writes the resident root; negotiated control uses only the request channel.
A scan, free lock, absent owner or empty process group is not a drain/exit proof.
unchanged:true means this command did not mutate state, not that the concurrent
service is frozen. transitionAttempted:false means no transition was attempted.
Observed fields are a read-time snapshot, not a concurrency fence. Legacy
configDigest is checked for structural shape only. Negotiated drain recomputes it
from launch-<digest>.json and the loaded old artifacts, never desired config.json.
When owner.json advertises launchAttestation, its exact launcher-written private
record must match before dispatch and throughout receipt observation. An absent
advertisement preserves cooperative uncontained hold/checkpoint/resume and legacy
refusal; a file alone never grants containment or remote custody. The CLI has no
native host IPC and cannot certify handshake, membership or whole-attempt exit.
After dispatch, uncertainty is needs-operator, never unchanged or release success.
Public JSON paths are relative to the explicit mesh root. Authentication tokens,
Main nonces and private config are never printed; tokens use SHA-256 fingerprints.`;

const options = ["--resident", "--mesh-root", "--expected-release", "--target-release"] as const;
type Option = typeof options[number];

export interface ResidentDrainReceipt {
  ok: boolean;
  phase: "preflight" | EndpointDrainReceipt["phase"];
  outcome: "refused" | "help" | EndpointDrainReceipt["outcome"];
  code: string;
  reason: string;
  resident: string | null;
  expected: string | null;
  target: string | null;
  /** This command did not mutate state; concurrent service activity is not frozen. */
  unchanged: boolean;
  transitionAttempted: boolean;
  drain?: ResidentDrainPublicEvidence;
  /** Read-time owner/config observations, not a lock or complete launch validation. */
  observed?: {
    rootId: string | null;
    hostId: string | null;
    pid: number | null;
    processStartTime: string | null;
    tokenFingerprint: string | null;
    loadedRelease: { releaseRoot: string | null; fabricExtensionPath: string | null };
  };
  rollback: { status: "not-needed" | "not-attempted"; reason: string };
  elapsedMs: number;
  help?: string;
}

export interface ResidentDrainPublicEvidence {
  id: string;
  rootId: string;
  owner: { pid: number; processStartTime: string; hostId: string; tokenFingerprint: string };
  launcher: { pid: number; processStartTime: string; tokenFingerprint: string };
  previous: { releaseRoot: string; configDigest: string };
  target: { releaseRoot: string; configDigest: string };
  terminal: boolean;
  admissionHeld: boolean;
  checkpointReadback: boolean;
  at: number;
  deadlineAt: number;
  resumedAt?: number;
}

const fingerprint = (token: string): string => `sha256:${createHash("sha256").update(token).digest("hex").slice(0, 16)}`;

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

const DRAIN_TIMEOUT_MS = 120_000;
const REQUEST_TIMEOUT_MS = 30_000;
const RECEIPT_GRACE_MS = 1_000;

/** Explicit in-process test seam; no CLI flag or environment can replace authority. */
export interface ResidentDrainDependencies {
  client?: (meshRoot: string, rootId: string, timeoutMs: number) => Pick<ResidentActorClient, "releaseDrain">;
  timeoutMs?: number;
}

function readOwnedJson<T>(file: string, probe?: ResidentChannelOwnerProbe): T {
  assertResidentChannelOwned([file], probe);
  if (!fs.lstatSync(file).isFile()) throw new Error(`Not a regular resident identity file: ${file}`);
  return JSON.parse(fs.readFileSync(file, "utf8")) as T;
}

function owningMain(meshRoot: string, root: string, previous: ResidentLaunchSpec, target: string,
  probe?: ResidentChannelOwnerProbe): { main: ResidentMainGeneration; caller: ResidentDrainIntent["caller"]; watch: string[] } {
  const main = readOwnedJson<ResidentMainGeneration>(mainGenerationPath(root), probe);
  if (!main || typeof main.nonce !== "string" || !main.nonce.trim() || main.nonce.startsWith("closed:") ||
      main.rootId !== previous.config.rootId || main.sessionId !== previous.config.sessionId ||
      ![previous.releaseRoot, target].includes(main.releaseRoot) || !exactBirth(main.pid, main.processStartTime) || !mainGenerationCurrent(root, main)) {
    throw new Error("No current birth-bound owning Main generation for this target release");
  }
  const hash = createHash("sha256").update(main.rootId).digest("hex");
  const participants = path.join(meshRoot, "participants");
  // Native records only. Reading MeshStore would register a writer census and
  // violate the refusal/no-effects contract; missing native authority refuses.
  assertResidentChannelOwned([meshRoot, participants], probe);
  const entry = readOwnedJson<MeshStateEntry & { format: number }>(path.join(participants, `${hash}.json`), probe);
  const participant = entry?.value as FabricParticipantRecord | undefined;
  const identity = entry?.updatedBy;
  if (entry?.format !== 1 || entry.key !== `topology/participants/${hash}` ||
      !Number.isFinite(entry.updatedAt) || entry.updatedAt > Date.now() ||
      !participant || participant.format !== 1 || participant.kind !== "root" || participant.remoteHost !== undefined ||
      participant.id !== main.rootId || participant.rootId !== main.rootId || participant.sessionId !== main.sessionId ||
      participant.ownerIdentityId !== main.rootId || typeof participant.ownerHostId !== "string" || !participant.ownerHostId ||
      !["idle", "running"].includes(participant.status) ||
      participant.mainProcess?.host !== os.hostname() || participant.mainProcess.pid !== main.pid ||
      participant.mainProcess.startTime !== main.processStartTime ||
      !identity || identity.id !== main.rootId || identity.kind !== "main" || identity.sessionId !== main.sessionId ||
      identity.verified !== undefined || typeof identity.name !== "string" || !identity.name.trim()) {
    throw new Error("No verified local owning Main root participant identity");
  }
  const leasePath = hostLeasePath(meshRoot, participant.ownerHostId);
  assertResidentChannelOwned([path.dirname(leasePath), leasePath], probe);
  const lease = readHostLeaseCurrent(meshRoot, participant.ownerHostId);
  const now = Date.now();
  if (!lease || lease.id !== participant.ownerHostId || lease.rootId !== main.rootId || lease.identityId !== main.rootId ||
      lease.expiresAt <= now || lease.session?.id !== main.sessionId || lease.session.expiresAt <= now ||
      lease.reloadUntil !== undefined) {
    throw new Error("Owning Main participant has no current matching session lease");
  }
  return { main, caller: { identity: structuredClone(identity), hostId: participant.ownerHostId },
    watch: [participants, path.dirname(leasePath)] };
}

function sameOwner(root: string, owner: ResidentHostOwner, intent: ResidentDrainIntent,
  launchAttestation: AttemptLaunchReceipt | undefined): boolean {
  // Compare with the initially pinned observation as well as the private record;
  // swapping both owner and record cannot silently replace the negotiated attempt.
  if (launchAttestation === undefined ? owner.launchAttestation !== undefined :
      owner.launchAttestation === undefined ||
      Object.entries(launchAttestation).some(([key, value]) => owner.launchAttestation![key as keyof AttemptLaunchReceipt] !== value)) return false;
  if (owner.launchAttestation !== undefined) readResidentLaunchAttestation(root, owner.launchAttestation);
  return !!owner && owner.releaseRoot === intent.expected.releaseRoot && owner.configDigest === intent.expected.configDigest &&
    owner.fabricExtensionPath === path.join(intent.expected.releaseRoot, "dist/index.js") &&
    owner.pid === intent.expected.owner.pid && owner.processStartTime === intent.expected.owner.processStartTime &&
    owner.token === intent.expected.owner.token && owner.hostId === intent.expected.owner.hostId &&
    owner.handover?.abi === "fabric-resident-1" &&
    ["pid", "processStartTime", "token", "entry", "runtime"].every(key =>
      owner.handover!.launcher[key as keyof typeof intent.expected.launcher] === intent.expected.launcher[key as keyof typeof intent.expected.launcher]) &&
    exactBirth(owner.pid, owner.processStartTime) && exactBirth(intent.expected.launcher.pid, intent.expected.launcher.processStartTime);
}

function checkDrain(receipt: EndpointDrainReceipt, id: string, intent: ResidentDrainIntent, previous: ResidentLaunchSpec, started: number): void {
  if (!receipt || receipt.abi !== RESIDENT_DRAIN_ABI || receipt.id !== id || receipt.rootId !== intent.main.rootId ||
      !receipt.owner || Object.entries(intent.expected.owner).some(([key, value]) => receipt.owner[key as keyof typeof receipt.owner] !== value) ||
      !receipt.launcher || ["pid", "processStartTime", "token", "entry", "runtime"].some(key =>
        receipt.launcher[key as keyof typeof receipt.launcher] !== intent.expected.launcher[key as keyof typeof receipt.launcher]) ||
      JSON.stringify(receipt.previous) !== JSON.stringify(previous) || JSON.stringify(receipt.target) !== JSON.stringify(intent.target) ||
      JSON.stringify(receipt.main) !== JSON.stringify(intent.main) ||
      !Number.isSafeInteger(receipt.at) || receipt.at < started || receipt.at > Date.now() || !Number.isSafeInteger(receipt.deadlineAt) ||
      receipt.deadlineAt < started || (receipt.terminal !== true && receipt.deadlineAt < receipt.at) || typeof receipt.admissionHeld !== "boolean" ||
      typeof receipt.code !== "string" || !/^[A-Z][A-Z0-9_]{0,79}$/.test(receipt.code) || typeof receipt.reason !== "string" || !receipt.reason) {
    throw new Error("Drain receipt correlation/owner/launch/Main evidence is uncertain");
  }
}

/** Legacy refusal is read-only; only an attested cooperative endpoint may hold admission. */
export async function main(argv: string[], io: { out: (text: string) => void } = {
  out: text => process.stdout.write(text),
}, ownerProbe?: ResidentChannelOwnerProbe, dependencies: ResidentDrainDependencies = {}): Promise<number> {
  const started = Date.now();
  const values: Partial<Record<Option, string>> = {};
  let resident: string | null = null;
  let expected: string | null = null;
  let target: string | null = null;
  let observed: ResidentDrainReceipt["observed"];
  let dispatched = false;
  const privateTokens = new Set<string>();
  const publicPath = (file: string | null): string | null => file === null ? null :
    path.relative(path.resolve(values["--mesh-root"] ?? process.cwd()), path.resolve(file)) || ".";
  const publicText = (text: string): string => {
    for (const token of privateTokens) text = text.split(token).join("[redacted]");
    // Error paths are diagnostic data too. Never expose an absolute filesystem
    // path embedded in an fs/launch validation error's free-form message.
    return text.replace(/(?<![\w./-])\/(?:[^\s\"'`,;()[\]{}<>:]+\/)*[^\s\"'`,;()[\]{}<>:]+/g, file => publicPath(file)!);
  };
  const emit = (result: ResidentDrainReceipt): void => {
    const safe = { ...result, resident: publicPath(result.resident), expected: publicPath(result.expected), target: publicPath(result.target),
      code: publicText(result.code), reason: publicText(result.reason),
      ...(result.observed ? { observed: { ...result.observed, loadedRelease: {
        releaseRoot: publicPath(result.observed.loadedRelease.releaseRoot),
        fabricExtensionPath: publicPath(result.observed.loadedRelease.fabricExtensionPath) } } } : {}) };
    io.out(JSON.stringify(safe, (_key, value: unknown) => typeof value === "string" ? publicText(value) : value) + "\n");
  };
  const publicDrain = (drain: EndpointDrainReceipt): ResidentDrainPublicEvidence => ({
    id: drain.id, rootId: drain.rootId,
    owner: { pid: drain.owner.pid, processStartTime: drain.owner.processStartTime, hostId: drain.owner.hostId,
      tokenFingerprint: fingerprint(drain.owner.token) },
    launcher: { pid: drain.launcher.pid, processStartTime: drain.launcher.processStartTime,
      tokenFingerprint: fingerprint(drain.launcher.token) },
    previous: { releaseRoot: publicPath(drain.previous.releaseRoot)!, configDigest: drain.previous.digest },
    target: { releaseRoot: publicPath(drain.target.releaseRoot)!, configDigest: drain.target.digest },
    terminal: drain.terminal === true, admissionHeld: drain.admissionHeld, checkpointReadback: drain.checkpointReadback === true,
    at: drain.at, deadlineAt: drain.deadlineAt, ...(drain.resumedAt === undefined ? {} : { resumedAt: drain.resumedAt }),
  });
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
      emit({ ...receipt(true, "HELP", "Cooperative drain with fail-closed legacy preflight; no target launch is available"), help });
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
    } catch { return refuse("RESIDENT_STATE_INVALID", "Cannot read resident identity: config/owner state is unavailable or invalid"); }
    for (const token of [owner.token, owner.handover?.launcher?.token]) {
      if (typeof token === "string" && token) privateTokens.add(token);
    }
    // Record only fields actually read from config/owner, never desired release
    // selectors. These observations do not freeze or attest concurrent service.
    observed = {
      rootId: typeof config.rootId === "string" ? config.rootId : null,
      hostId: typeof owner.hostId === "string" ? owner.hostId : null,
      pid: typeof owner.pid === "number" ? owner.pid : null,
      processStartTime: typeof owner.processStartTime === "string" ? owner.processStartTime : null,
      tokenFingerprint: typeof owner.token === "string" ? fingerprint(owner.token) : null,
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
    // Optional only for existing uncontained cooperative hosts. An advertised
    // receipt must have an authentic private launcher record; never reconstruct it
    // from owner paths/timestamps or treat an orphan record as native custody.
    const launchAttestation = owner.launchAttestation === undefined ? undefined :
      readResidentLaunchAttestation(resident, owner.launchAttestation);
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
    if (owner.drain?.abi !== RESIDENT_DRAIN_ABI || !Array.isArray(owner.commands) ||
        !owner.commands.includes("releaseDrain") || owner.requestFence !== 1) {
      refuse("DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE", "Structural launch identity is available (configDigest shape only, digest not recomputed/validated). No current legacy request closes admission and checkpoints accepted work. setActivationFilter skips/consumes rather than holds; pauseForRelease is internal and unavailable externally; releaseChange is unconditionally deferred. No safe drain/exit proof or unsafe bypass is available; no transition attempted.");
    }
    let previous: ResidentLaunchSpec;
    try {
      const loaded = readOwnedJson<ResidentHostConfig>(path.join(resident, `launch-${owner.configDigest}.json`), ownerProbe);
      previous = residentLaunchSpec(loaded, launcher.entry, launcher.runtime);
      if (previous.digest !== owner.configDigest || previous.releaseRoot !== expected || loaded.rootId !== config.rootId ||
          loaded.meshRoot !== meshRoot || loaded.residencyRoot !== resident || expected === target) {
        throw new Error("Immutable loaded snapshot does not match owner/config/topology or target equals current release");
      }
    } catch { return refuse("LOADED_SNAPSHOT_INVALID", "Immutable loaded launch snapshot/artifact cannot be verified against the owner digest"); }
    let targetSpec: ResidentLaunchSpec;
    try {
      targetSpec = residentLaunchSpec({ ...structuredClone(previous.config),
        fabricExtensionPath: path.join(target, "dist/index.js"), workerPath: path.join(target, "dist/worker.js") },
      path.join(target, "dist/residency/pi-entry.js"), previous.runtime);
      assertHandoverTopology(previous, targetSpec);
    } catch { return refuse("TARGET_SNAPSHOT_INVALID", "Target artifact/config cannot be pinned without changing the loaded topology"); }
    let authority: ReturnType<typeof owningMain>;
    try { authority = owningMain(meshRoot, resident, previous, target, ownerProbe); }
    catch { return refuse("MAIN_AUTHORITY_UNAVAILABLE", "No current birth-bound local owning Main generation, root participant and session lease"); }
    const timeoutMs = dependencies.timeoutMs ?? DRAIN_TIMEOUT_MS;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > DRAIN_TIMEOUT_MS) refuse("INVALID_ARGUMENTS", "Drain deadline must be finite and bounded");
    const intent: ResidentDrainIntent = { caller: authority.caller, main: authority.main, target: targetSpec,
      expected: { releaseRoot: expected, configDigest: previous.digest,
        owner: { pid: owner.pid, processStartTime: owner.processStartTime!, token: owner.token, hostId: owner.hostId }, launcher }, timeoutMs };
    const requestStarted = Date.now();
    const requestTimeoutMs = Math.min(timeoutMs, REQUEST_TIMEOUT_MS);
    let deadline = requestStarted + requestTimeoutMs;
    const abort = new AbortController();
    let dirty = true;
    let wake: (() => void) | undefined;
    let watchError: Error | undefined;
    const notify = (): void => { dirty = true; wake?.(); };
    const watchers: fs.FSWatcher[] = [];
    let timer = setTimeout(() => { abort.abort(); notify(); }, requestTimeoutMs);
    try {
      // Subscribe before dispatch and read after every event: no lost-receipt
      // window, no polling, and a ref'd deadline for the standalone CLI.
      for (const directory of [resident, ...authority.watch]) {
        const watcher = fs.watch(directory, { persistent: false }, notify);
        watcher.on("error", error => { watchError = error; notify(); });
        watchers.push(watcher);
      }
      const current = readOwnedJson<ResidentHostOwner>(path.join(resident, "owner.json"), ownerProbe);
      if (!sameOwner(resident, current, intent, launchAttestation) || current.drain?.abi !== RESIDENT_DRAIN_ABI ||
          !current.commands?.includes("releaseDrain") || current.requestFence !== 1) {
        refuse("RESIDENT_IDENTITY_MISMATCH", "Resident owner/capability changed before dispatch; no request was sent");
      }
      const currentMain = owningMain(meshRoot, resident, previous, target, ownerProbe);
      if (JSON.stringify(currentMain.main) !== JSON.stringify(intent.main) ||
          JSON.stringify(currentMain.caller) !== JSON.stringify(intent.caller)) {
        refuse("MAIN_AUTHORITY_UNAVAILABLE", "Owning Main changed before dispatch; no request was sent");
      }
      const client = dependencies.client?.(meshRoot, config.rootId, requestTimeoutMs) ??
        new ResidentActorClient(meshRoot, config.rootId, requestTimeoutMs);
      dispatched = true;
      const response = await client.releaseDrain(intent, abort.signal);
      const initial = response.drain;
      if (!response.ok || !initial || typeof initial.id !== "string" ||
          !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(initial.id)) {
        throw new Error("Resident returned no correlated cooperative drain receipt");
      }
      checkDrain(initial, initial.id, intent, previous, requestStarted);
      if (abort.signal.aborted || initial.deadlineAt > requestStarted + requestTimeoutMs + timeoutMs ||
          initial.deadlineAt > Date.now() + timeoutMs) throw new Error("Unbounded or expired drain handshake");
      // The resident starts its finite drain clock on pickup, not publication.
      // Keep the handshake budget separate, then allow one bounded receipt grace
      // for the resident's natural timeout/resume receipt to reach durable disk.
      clearTimeout(timer);
      deadline = initial.deadlineAt + RECEIPT_GRACE_MS;
      timer = setTimeout(notify, Math.max(0, deadline - Date.now()));
      const receiptPath = residentDrainReceiptPath(resident, initial.id);
      const expired = (): void => {
        if (Date.now() >= deadline) throw new Error("Finite drain deadline expired; A resume/terminal state is unconfirmed");
      };
      for (;;) {
        if (watchError) throw watchError;
        if (!dirty && Date.now() >= deadline) dirty = true;
        if (!dirty) {
          await new Promise<void>(resolve => { wake = resolve; if (dirty) resolve(); });
          wake = undefined;
          continue;
        }
        dirty = false;
        if (!sameOwner(resident, readOwnedJson<ResidentHostOwner>(path.join(resident, "owner.json"), ownerProbe), intent, launchAttestation)) {
          throw new Error("Exact resident owner changed or exited during drain; no successor is authorized by this CLI");
        }
        const currentMain = owningMain(meshRoot, resident, previous, target, ownerProbe);
        if (JSON.stringify(currentMain.main) !== JSON.stringify(intent.main) ||
            JSON.stringify(currentMain.caller) !== JSON.stringify(intent.caller)) throw new Error("Owning Main authority changed during drain");
        let durable: EndpointDrainReceipt;
        try { durable = readOwnedJson<EndpointDrainReceipt>(receiptPath, ownerProbe); }
        catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") { expired(); continue; } throw error; }
        checkDrain(durable, initial.id, intent, previous, requestStarted);
        if (durable.deadlineAt !== initial.deadlineAt || durable.at < initial.at) throw new Error("Drain receipt deadline/sequence changed");
        // Always inspect durable terminal evidence once at the deadline before
        // reporting uncertainty; a same-turn timer must not hide a resume.
        if (!durable.terminal && ["held", "checkpointed"].includes(durable.phase) && ["held", "checkpointed"].includes(durable.outcome)) { expired(); continue; }
        if (durable.terminal !== true || !(["refused", "held", "checkpointed", "needs-operator", "rolled-back"].includes(durable.phase)) ||
            !["containment-unavailable", "resumed", "refused", "needs-operator"].includes(durable.outcome) ||
            (durable.outcome === "needs-operator" && durable.phase !== "needs-operator") ||
            (durable.outcome === "refused" && (durable.phase !== "refused" || durable.admissionHeld)) ||
            (["resumed", "containment-unavailable"].includes(durable.outcome) && (durable.admissionHeld ||
              !Number.isSafeInteger(durable.resumedAt) || durable.resumedAt! < requestStarted || durable.resumedAt! > Date.now())) ||
            (durable.outcome === "containment-unavailable" && (durable.phase !== "checkpointed" || durable.checkpointReadback !== true))) {
          throw new Error("Unexpected or still-held terminal drain state; release success/resume cannot be claimed");
        }
        emit({ ...receipt(false, durable.code, `Cooperative drain returned ${durable.outcome}; no target launch was authorized.`),
          phase: durable.phase, outcome: durable.outcome, unchanged: false, transitionAttempted: true, drain: publicDrain(durable),
          rollback: { status: "not-attempted", reason: "no-target-launch-authorized" } });
        return 1;
      }
    } finally {
      clearTimeout(timer);
      for (const watcher of watchers) watcher.close();
    }
  } catch (error) {
    const result = receipt(false, error instanceof Refusal || error instanceof ContainmentUnavailableError ? error.code : "PREFLIGHT_READ_FAILED", message(error));
    emit(dispatched ? { ...result, phase: "needs-operator", outcome: "needs-operator", code: "DRAIN_OUTCOME_UNKNOWN",
      reason: "Finite drain deadline expired or terminal evidence is uncertain; inspect the private resident receipt. No target launch was authorized.",
      unchanged: false, transitionAttempted: true, rollback: { status: "not-attempted", reason: "terminal-state-unconfirmed" } } : result);
    return 1;
  }
}
