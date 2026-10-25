import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { drainSlot, peekDrainSlot, pruneDrainSlot, type MainDrainSlotBinding } from "./slot.js";

/**
 * D3 seat drain (smarty-dev#15310) M10: bind each mutating drain request to ONE seat run, and
 * prove that the caller holds that run's exclusive seat lock (SEAT.md §2).
 *
 * Node can neither receive SCM_RIGHTS nor flock, so the Main proves the same facts from the kernel:
 * - the caller's pid comes from SO_PEERCRED;
 * - `/proc/<pid>/fd/<lockFd>` is the same inode as `<runDir>/seat.lock`;
 * - `/proc/<pid>/fdinfo/<lockFd>` shows an exclusive FLOCK on that inode through that open file
 *   description. So a fresh open cannot take it, and the holder's reentrant flock succeeds.
 * The FD number alone grants nothing. The same-UID raw bypass remains SEAT.md §3 A6's accepted residual.
 *
 * A pod binding (raw Pod resume-drain) carries no host lock. The Main accepts it only when its OWN
 * environment is a Kubernetes container (KUBERNETES_SERVICE_HOST), and binds to the pod UID that the
 * seam re-reads from the API before and after.
 */
export type MainDrainPhase = "drain" | "drain-undo" | "resume-drain";
export interface MainDrainBindingRequest {
  runDir: string | null;
  run: string;
  runSha256: string;
  phase: MainDrainPhase;
  operationId: string;
  /** The seam-minted fence generation, recorded in its intent file before the first control call. */
  generation: string;
  lockFd: number | null;
  podUid: string | null;
}
/** The drain record (one per Main): the run and operation that hold its gate and fence. */
export type MainDrainBinding = MainDrainSlotBinding;

const HEX32 = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const POD_UID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MAX_RUN_BYTES = 1024 * 1024;
const PHASES = new Set<MainDrainPhase>(["drain", "drain-undo", "resume-drain"]);

/** Exact keys and shapes; anything else is refused. */
export const parseBindingRequest = (value: unknown): MainDrainBindingRequest | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const binding = value as Record<string, unknown>;
  if (Object.keys(binding).sort().join(",") !== "generation,lockFd,operationId,phase,podUid,run,runDir,runSha256") return undefined;
  const { runDir, run, runSha256, phase, operationId, generation, lockFd, podUid } = binding;
  if (typeof run !== "string" || !HEX32.test(run) || typeof runSha256 !== "string" || !HEX64.test(runSha256) ||
    typeof operationId !== "string" || !HEX32.test(operationId) || typeof generation !== "string" || !HEX32.test(generation) ||
    !PHASES.has(phase as MainDrainPhase)) return undefined;
  if (runDir !== null && (typeof runDir !== "string" || !path.isAbsolute(runDir) || runDir.length > 4096)) return undefined;
  if (lockFd !== null && (!Number.isSafeInteger(lockFd) || (lockFd as number) < 0 || (lockFd as number) > 1_048_576)) return undefined;
  if (podUid !== null && (typeof podUid !== "string" || !POD_UID.test(podUid))) return undefined;
  // A host binding has a run directory and a lock; a pod binding has a pod UID and neither.
  const host = runDir !== null && lockFd !== null && podUid === null;
  const pod = runDir === null && lockFd === null && podUid !== null;
  if (!host && !pod) return undefined;
  return { runDir: runDir as string | null, run, runSha256, phase: phase as MainDrainPhase, operationId, generation,
    lockFd: lockFd as number | null, podUid: podUid as string | null };
};

export const currentBinding = (rootId: string): MainDrainBinding | undefined => peekDrainSlot(rootId)?.binding;
export const clearBinding = (rootId: string): void => {
  const slot = peekDrainSlot(rootId);
  if (slot) delete slot.binding;
  pruneDrainSlot(rootId);
};

/**
 * Bind (first call) or match (same run, operation and generation: idempotent). A different run or
 * operation while a binding stands is refused: one fence per Main.
 */
export const bindOrMatch = (rootId: string, request: MainDrainBindingRequest, kind: "host" | "pod", lock: { dev: string; ino: string } | null,
  now = Date.now()): MainDrainBinding | string => {
  const slot = drainSlot(rootId);
  const existing = slot.binding;
  if (existing) return sameOperation(existing, request, kind, lock) ? existing : "DRAIN_BOUND_ELSEWHERE";
  const binding: MainDrainBinding = { kind, run: request.run, runSha256: request.runSha256, operationId: request.operationId,
    generation: request.generation, phase: request.phase, podUid: request.podUid, boundAt: now,
    runDir: request.runDir, lock };
  slot.binding = binding;
  slot.admitted = [];
  slot.ledger = [];
  slot.ledgerOverflow = 0;
  return binding;
};

/**
 * The SAME operation: run, its bytes, operationId, generation, kind and pod UID, and for a host binding the SAME
 * canonical RUN and the SAME seat-lock inode proved at bind (a copied RUN with its own lock cannot replay the tuple).
 */
export const sameOperation = (existing: MainDrainBinding, request: MainDrainBindingRequest, kind: "host" | "pod",
  lock: { dev: string; ino: string } | null): boolean =>
  existing.run === request.run && existing.runSha256 === request.runSha256 && existing.operationId === request.operationId &&
  existing.generation === request.generation && existing.kind === kind && existing.podUid === request.podUid &&
  existing.runDir === request.runDir && (existing.lock === null ? lock === null : lock !== null && existing.lock.dev === lock.dev &&
    existing.lock.ino === lock.ino);

const readNoFollow = (file: string, uid: number, limit: number): Buffer => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | (fs.constants.O_NONBLOCK ?? 0));
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid || stat.nlink !== 1 || stat.size > limit) throw new Error("unsafe");
    return fs.readFileSync(fd);
  } finally { fs.closeSync(fd); }
};

/** glibc dev_t decoding (gnu_dev_major/minor), as /proc/locks prints MAJOR:MINOR in hex. */
const devParts = (dev: bigint): [bigint, bigint] => [
  ((dev >> 8n) & 0xfffn) | ((dev >> 32n) & ~0xfffn),
  (dev & 0xffn) | ((dev >> 12n) & ~0xffn),
];

/** /proc/<pid>/stat field 22 (starttime, clock ticks since boot); undefined when unreadable. */
export const procStartTime = (pid: number, procRoot = "/proc"): string | undefined => {
  try {
    const stat = fs.readFileSync(path.join(procRoot, String(pid), "stat"), "utf8");
    // comm (field 2) may contain spaces and parentheses: split after its LAST ')'.
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    const start = fields[19];
    return start && /^[0-9]+$/.test(start) ? start : undefined;
  } catch { return undefined; }
};

export interface HostBindingProof {
  /** Fixed refusal code, or null when every check passed. */
  refusal: string | null;
  lock: { dev: string; ino: string } | null;
  peerStartTime?: string;
}

/**
 * The run's sealed record names this session, and the peer holds the run's exclusive seat lock
 * through the FD it names. `procRoot` exists for tests only.
 */
export const proveHostBinding = (request: MainDrainBindingRequest, sessionId: string, peer: { pid: number; uid: number } | undefined,
  uid: number, procRoot = "/proc"): HostBindingProof => {
  const refuse = (refusal: string): HostBindingProof => ({ refusal, lock: null });
  if (!request.runDir || request.lockFd === null) return refuse("BINDING_NOT_HOST");
  // The SO_PEERCRED uid must be this Main's own effective uid.
  if (!peer || peer.uid !== uid || !Number.isSafeInteger(peer.pid) || peer.pid <= 0) return refuse("PEER_UNVERIFIED");
  // The pid's start time brackets the /proc reads below: a reused pid refuses.
  const startedBefore = procStartTime(peer.pid, procRoot);
  if (!startedBefore) return refuse("PEER_UNVERIFIED");
  try {
    if (fs.realpathSync(request.runDir) !== request.runDir) return refuse("RUN_DIR_NOT_CANONICAL");
    const directory = fs.lstatSync(request.runDir);
    if (!directory.isDirectory() || directory.uid !== uid || (directory.mode & 0o7777) !== 0o700) return refuse("RUN_DIR_UNSAFE");
  } catch { return refuse("RUN_DIR_UNREADABLE"); }
  let run: Record<string, unknown>;
  try {
    const bytes = readNoFollow(path.join(request.runDir, "seat-run.json"), uid, MAX_RUN_BYTES);
    if (createHash("sha256").update(bytes).digest("hex") !== request.runSha256) return refuse("RUN_SHA256_MISMATCH");
    run = JSON.parse(bytes.toString("utf8")) as Record<string, unknown>;
  } catch { return refuse("RUN_UNREADABLE"); }
  if (typeof run !== "object" || run === null || run.schema !== "d3-seat-run-v1" || run.run !== request.run || run.sessionId !== sessionId) {
    return refuse("RUN_MISMATCH");
  }
  let lock: fs.BigIntStats;
  try {
    lock = fs.lstatSync(path.join(request.runDir, "seat.lock"), { bigint: true });
    if (!lock.isFile() || lock.uid !== BigInt(uid) || lock.nlink !== 1n) return refuse("LOCK_FILE_UNSAFE");
  } catch { return refuse("LOCK_FILE_UNREADABLE"); }
  const [major, minor] = devParts(lock.dev);
  const identity = { dev: `${major.toString(16).padStart(2, "0")}:${minor.toString(16).padStart(2, "0")}`, ino: lock.ino.toString() };
  try {
    const held = fs.statSync(path.join(procRoot, String(peer.pid), "fd", String(request.lockFd)), { bigint: true });
    if (held.dev !== lock.dev || held.ino !== lock.ino) return refuse("LOCK_FD_OTHER_FILE");
    const info = fs.readFileSync(path.join(procRoot, String(peer.pid), "fdinfo", String(request.lockFd)), "utf8");
    const exclusive = info.split("\n").some(line => {
      const match = /^lock:\s+\d+:\s+FLOCK\s+ADVISORY\s+WRITE\s+\d+\s+([0-9a-f]+):([0-9a-f]+):(\d+)\s/.exec(line);
      return match !== null && BigInt(`0x${match[1]}`) === major && BigInt(`0x${match[2]}`) === minor && match[3] === identity.ino;
    });
    if (!exclusive) return refuse("LOCK_NOT_HELD");
  } catch { return refuse("LOCK_FD_UNREADABLE"); }
  if (procStartTime(peer.pid, procRoot) !== startedBefore) return refuse("PEER_PID_REUSED");
  return { refusal: null, lock: identity, peerStartTime: startedBefore };
};

/**
 * A pod binding carries no lock: inside the Pod any same-UID process (the Main's own tools included) could
 * present one. Until a trusted pod-identity and caller proof exists (stage 1 review finding 1), pod bindings
 * are REFUSED. `unproven` is a test-only override; production never sets it. Even then, only a Main whose own
 * environment is a Kubernetes container honours one.
 */
export const POD_BINDING_PROOF = false;
export const podBindingAllowed = (environment: NodeJS.ProcessEnv = process.env, unproven = false): boolean =>
  (POD_BINDING_PROOF || unproven) && typeof environment.KUBERNETES_SERVICE_HOST === "string" && environment.KUBERNETES_SERVICE_HOST.length > 0;
