import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic, syncDirectoryChain } from "../core/atomic-write.js";
import { findExecutable } from "../agents/transports/process-utils.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";
import type { ResidentActorCaller, ResidentHostConfig, ResidentHostOwner } from "./protocol.js";

// Capability rollout is deliberately fail-closed: B cannot retrofit custody into
// a running pre-protocol A launcher. These hosts require the installer drain.
export const RESIDENT_HANDOVER_ABI = "fabric-resident-1" as const;
export const HANDOVER_STARTUP_MS = 30_000;
export const HANDOVER_DRAIN_MS = 120_000;
export interface ResidentProcessIdentity { pid: number; processStartTime: string; }
export interface ResidentLauncherIdentity extends ResidentProcessIdentity {
  token: string;
  entry: string;
  runtime: string;
}
export interface ResidentMainGeneration extends ResidentProcessIdentity {
  nonce: string;
  rootId: string;
  sessionId: string;
  releaseRoot: string;
}
export interface ResidentLaunchSpec {
  abi: typeof RESIDENT_HANDOVER_ABI;
  releaseRoot: string;
  entry: string;
  runtime: string;
  artifact: string;
  config: ResidentHostConfig;
  digest: string;
}
export interface ResidentReleaseIntent {
  caller: ResidentActorCaller;
  main: ResidentMainGeneration;
  target: ResidentLaunchSpec;
}
export interface ResidentHandoverPlan extends ResidentReleaseIntent {
  id: string;
  rootId: string;
  old: Pick<ResidentHostOwner, "pid" | "processStartTime" | "token" | "hostId">;
  launcher: ResidentLauncherIdentity;
  previous: ResidentLaunchSpec;
  createdAt: number;
}
export interface ResidentHandoverState {
  plan: ResidentHandoverPlan;
  phase: "preparing" | "custody" | "released" | "starting" | "complete" | "fallback" | "cancelled" | "blocked";
  at: number;
  error?: string;
}
export const handoverPath = (root: string): string => path.join(root, "handover.json");
export const mainGenerationPath = (root: string): string => path.join(root, "main-generation.json");
export const handoverCustodyPath = (root: string, id: string): string => path.join(root, `handover-${id}.custody.json`);
export const handoverDecisionPath = (root: string, id: string): string => path.join(root, `handover-${id}.decision.json`);
export interface ResidentHandoverDecision { id: string; state: "custody" | "cancelled"; }
export const handoverOutcomePath = (root: string, target: ResidentLaunchSpec): string =>
  path.join(root, `handover-outcome-${createHash("sha256").update(target.releaseRoot).update(target.artifact).digest("hex")}.json`);
export const launchSnapshotPath = (root: string, spec: ResidentLaunchSpec): string => path.join(root, `launch-${spec.digest}.json`);
export function readHandoverJson<T>(file: string): T | undefined {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
}
export function handoverActive(state: ResidentHandoverState | undefined): boolean {
  return !!state && !["complete", "fallback", "cancelled"].includes(state.phase);
}
export function exactResidentProcess(identity: ResidentProcessIdentity | undefined): boolean {
  return !!identity && Number.isSafeInteger(identity.pid) && identity.pid > 0 &&
    typeof identity.processStartTime === "string" &&
    processStartTime(identity.pid) === identity.processStartTime &&
    residentProcessAlive(identity.pid, identity.processStartTime);
}
export const specDigest = (value: Omit<ResidentLaunchSpec, "digest">): string =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");

function fixedFile(file: string): string {
  const absolute = path.resolve(file);
  if (fs.realpathSync(absolute) !== absolute || !fs.statSync(absolute).isFile()) {
    throw new Error(`Resident handover requires a retained, real artifact path: ${file}`);
  }
  return absolute;
}
function pinBinary(binary: string, releaseRoot: string, required = false): string {
  if (path.isAbsolute(binary) && !fs.existsSync(binary) && !required) return path.resolve(binary);
  const found = path.isAbsolute(binary) ? binary : findExecutable(binary);
  if (!found) {
    if (required) throw new Error(`Resident handover cannot resolve binary: ${binary}`);
    // Freeze known absence too: a later PATH change/install must not silently
    // supply a different runner to the A fallback. This missing .js path belongs
    // to the sealed closure; creating it later invalidates the artifact digest.
    return path.join(releaseRoot, "dist/residency", `unavailable-${createHash("sha256").update(binary).digest("hex")}.js`);
  }
  return fs.realpathSync(found);
}
/** Hash the whole package-local JS closure, not only a cheap entrypoint. First use only. */
function artifactDigest(root: string): string {
  const hash = createHash("sha256");
  const walk = (directory: string): void => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name);
      const stat = fs.lstatSync(file);
      if (stat.isSymbolicLink()) throw new Error(`Resident release contains a mutable symlink: ${file}`);
      if (stat.isDirectory()) walk(file);
      else if (/\.[cm]?js$/.test(name)) hash.update(path.relative(root, file)).update(fs.readFileSync(file));
    }
  };
  walk(path.join(root, "dist"));
  return hash.digest("hex");
}
export function residentLaunchSpec(config: ResidentHostConfig, entry: string, runtime = process.execPath): ResidentLaunchSpec {
  const releaseRoot = path.resolve(path.dirname(entry), "../..");
  fixedFile(entry);
  if (path.resolve(config.fabricExtensionPath) !== path.join(releaseRoot, "dist/index.js") ||
      path.resolve(config.workerPath) !== path.join(releaseRoot, "dist/worker.js") ||
      path.resolve(entry) !== path.join(releaseRoot, "dist/residency/pi-entry.js")) {
    throw new Error("Resident launch specification mixes releases");
  }
  fixedFile(config.fabricExtensionPath); fixedFile(config.workerPath);
  const pinned = structuredClone(config);
  pinned.piBinary = pinBinary(config.piBinary, releaseRoot, true);
  pinned.claudeBinary = pinBinary(config.claudeBinary, releaseRoot);
  pinned.vedaBinary = pinBinary(config.vedaBinary, releaseRoot);
  const value = { abi: RESIDENT_HANDOVER_ABI, releaseRoot, entry: path.resolve(entry),
    runtime: fixedFile(fs.realpathSync(runtime)), artifact: artifactDigest(releaseRoot), config: pinned };
  return { ...value, digest: specDigest(value) };
}
export function validateLaunchSpec(spec: ResidentLaunchSpec): void {
  if (!spec || spec.abi !== RESIDENT_HANDOVER_ABI || typeof spec.digest !== "string") throw new Error("Unsupported resident handover ABI");
  const { digest, ...value } = spec;
  if (specDigest(value) !== digest || residentLaunchSpec(spec.config, spec.entry, spec.runtime).digest !== digest) {
    throw new Error("Resident handover artifact/config snapshot changed");
  }
}
export function assertPreviousLaunchSpec(loaded: ResidentLaunchSpec, previous: ResidentLaunchSpec): void {
  const frozen = (config: ResidentHostConfig): string => JSON.stringify(Object.fromEntries(Object.entries(config)
    .filter(([key]) => !["kernel", "pythonRuntime", "piModels", "modelGuidance"].includes(key))));
  if (loaded.releaseRoot !== previous.releaseRoot || loaded.entry !== previous.entry || loaded.runtime !== previous.runtime ||
      loaded.artifact !== previous.artifact || frozen(loaded.config) !== frozen(previous.config)) {
    throw new Error("Resident fallback is not the healthy loaded A launch specification");
  }
}

export function assertHandoverTopology(previous: ResidentLaunchSpec, target: ResidentLaunchSpec): void {
  for (const key of ["rootId", "sessionId", "cwd", "projectRoot", "meshRoot", "actorRoot", "sessionActorRoot", "residencyRoot", "role", "project"] as const) {
    if (previous.config[key] !== target.config[key]) throw new Error(`Resident handover cannot migrate ${key}`);
  }
  if (previous.config.mesh.actorScope !== target.config.mesh.actorScope ||
      previous.config.mesh.lockProtocol !== target.config.mesh.lockProtocol) throw new Error("Resident handover cannot migrate mesh topology");
}
/** Hard-link CAS publishes complete immutable bytes; no empty-file or overwrite window. */
export function writeHandoverImmutable(file: string, value: unknown): void {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
  try {
    writeJsonAtomic(temporary, value, { durable: true });
    try { fs.linkSync(temporary, file); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST" || JSON.stringify(readHandoverJson(file)) !== JSON.stringify(value)) throw error;
    }
    syncDirectoryChain(path.dirname(file));
  } finally { fs.rmSync(temporary, { force: true }); }
}
export function decideHandover(root: string, decision: ResidentHandoverDecision): ResidentHandoverDecision {
  const file = handoverDecisionPath(root, decision.id);
  try { writeHandoverImmutable(file, decision); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  const winner = readHandoverJson<ResidentHandoverDecision>(file);
  if (winner?.id !== decision.id || !["custody", "cancelled"].includes(winner.state)) {
    throw new Error("Resident custody/cancellation decision is uncertain");
  }
  return winner;
}

export function writeLaunchSnapshot(root: string, spec: ResidentLaunchSpec): string {
  const file = launchSnapshotPath(root, spec);
  writeHandoverImmutable(file, spec.config);
  return file;
}
export function writeHandoverState(root: string, plan: ResidentHandoverPlan, phase: ResidentHandoverState["phase"], error?: string): ResidentHandoverState {
  const state: ResidentHandoverState = { plan, phase, at: Date.now(), ...(error ? { error } : {}) };
  writeJsonAtomic(handoverPath(root), state, { durable: true });
  return state;
}
export function mainGenerationCurrent(root: string, main: ResidentMainGeneration): boolean {
  const recorded = readHandoverJson<ResidentMainGeneration>(mainGenerationPath(root));
  return JSON.stringify(recorded) === JSON.stringify(main) && exactResidentProcess(main);
}
export function ownHandoverPlan(plan: ResidentHandoverPlan, owner: ResidentHostOwner | undefined, launcher: ResidentLauncherIdentity, childPid: number | undefined): boolean {
  return !!owner && owner.pid === childPid && owner.pid === plan.old.pid && owner.token === plan.old.token &&
    owner.processStartTime === plan.old.processStartTime && owner.hostId === plan.old.hostId &&
    JSON.stringify(owner.handover?.launcher) === JSON.stringify(launcher) && JSON.stringify(plan.launcher) === JSON.stringify(launcher) &&
    plan.previous.releaseRoot === owner.releaseRoot &&
    plan.launcher.pid === launcher.pid && plan.launcher.processStartTime === launcher.processStartTime &&
    exactResidentProcess(plan.launcher) && exactResidentProcess(owner as ResidentProcessIdentity) &&
    plan.rootId === plan.previous.config.rootId && plan.rootId === plan.target.config.rootId &&
    plan.previous.releaseRoot !== plan.target.releaseRoot;
}
