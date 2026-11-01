import fs from "node:fs";
import type net from "node:net";
import os from "node:os";
import path from "node:path";
import { loadBridgeNatsSeatPins, type BridgeNatsSeatPins } from "../mesh/bridge-nats-identity.js";
import { procStartTime } from "../main-drain/binding.js";
import { callMainDrain, mainDrainSocket } from "../main-drain/client.js";
import { ENTRY_ID, entryFacts, journalBranch, JournalBranchError, SUFFIX_MAX, type AppendedEntry } from "../seat-drain/journal-branch.js";
import { fixedKubectl, kubectl, readPod, verifyKubeconfig, type KubeAccess, type PodView } from "./kube.js";
import { podJournalArgv, podOwnerArgv } from "./pod-journal.js";
import { interpreterIdentity, parseRequest, proveSeatLock, readOwned, readSealedRun, resolveSeatMeshRoot, SeamGap, sha256,
  socketPeer, verifyRunDir, writeOnce, type SealedRun, type SeamRequest } from "../seat-drain/run.js";
import { natsLinkAllowListConfig } from "../mesh/bridge-nats-config.js";
import { redactNatsError } from "../mesh/state-backend-nats.js";
import { applyPlan, planPublish, planRestore, readLinkConfig, rollbackAuthority, withConfigLock, type LinkConfigFile, type ReadOptions, type RollbackAuthority,
  type SeatEntry, type WriterPlan } from "./allow-writer.js";
import { awaitRoutable, LINK_UNIT, natsRecords, otherRoutesBack, reloadLink, routeTable, type LinkDeps } from "./link.js";

/**
 * fabric-v2's D3 `bridge` seam (principal-seat move): phases `resume`, `publish` and `rc-restore-route`. A runs it as a
 * pinned executable with the drain seam's envelope: `--run-dir RUN --phase P`, the request on stdin, the held seat
 * lock in D3_SEAT_LOCK_FD (the same three-step proof), ONE receipt on stdout with exit 0 and empty stderr. A lock or
 * binding failure prints no receipt and exits 2 with one `GAP: <code> [owner: fabric-v2]` line (UNKNOWN).
 *
 * - resume: read-only. Pin the raw Pod's UID/node and rendered `--session`; exec the image's Python with a fixed
 *   stdlib script to read exact journal bytes/hash/stat. Derive the last-entry leaf and suffix facts here. A independently
 *   re-derives these from its sealed copy plus the bounded suffix. No 0.112.0 `session drain` CLI exists.
 * - publish: change this seat's A' allow-list entry, restart the pinned link, then re-check every other live route.
 *   Require live NATS presence on the seat machine and its hub mirror with the SAME incarnation as the Pod's local
 *   NATS projection, whose mainProcess PID/start time and comm=pi are checked inside the pinned pi container.
 *   Pod UID/node and container restartCount/startedAt/containerID stay unchanged across reads; no incarnation fallback.
 * - rc-restore-route: the reverse write (exactly what publish recorded), the restart and the route re-check, no live
 *   v2 presence for the seat, and the relaunched v1 writer is the live owner of the session.
 *
 * Every phase writes `fabric-v2-bridge-<phase>-intent.json` (O_EXCL, before any effect) and `-outcome.json`. A second
 * run of a phase on the same RUN is refused (exit 2), so a phase never runs twice.
 */

const OWNER = "fabric-v2";
const PHASES = new Set(["resume", "publish", "rc-restore-route"]);
const MAX_REQUEST = 64 * 1024;
const DEADLINE_MS = 840_000;
const PUBLISH_SEAT_ROUTE_WAIT_MS = 780_000;
const RESTORE_SEAT_ROUTE_WAIT_MS = 820_000;
const OTHER_ROUTES_WAIT_MS = 480_000;
const HEX64 = /^[0-9a-f]{64}$/;
const HEX32 = /^[0-9a-f]{32}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const POD_NAME = /^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$/;
const MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;
export const RESIDUALS = [
  "A6 (A's seat contract): a same-UID writer could alter this seam, its interpreter or the link config between check and use",
  "the link restart briefly interrupts every route the link carries; the seam re-checks each one afterwards",
  "resume proves rendered journal bytes, not the Main's in-memory leaf; A independently re-derives the seven resume fields",
  "publish incarnation == pod UID is a proof chain (API pod/container -> exec -> local projection + live pi PID/start -> NATS incarnation)",
];

export interface BridgeIo {
  stdin: () => Buffer;
  stdout: (text: string) => void;
  stderr: (text: string) => void;
  env: NodeJS.ProcessEnv;
}
export interface BridgePins {
  /** The nats-link config file this seam may change (pinned at install). */
  linkConfig: string;
  /** The link's systemd --user unit (pinned at install). */
  linkUnit: string;
}
export interface BridgeDeps extends LinkDeps {
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  deadlineMs?: number;
  python?: string;
  procRoot?: string;
  uid?: number;
  /** Tests only: the kubectl client. The in-Pod Python argv is fixed. */
  kubectl?: string;
  /** Legacy test-injection API retained for callers; never used to select or execute a Pod CLI. */
  podCli?: string | null;
  /** Tests only: the org.json seat pins (production: the link config's own orgJson, as `nats-link start`). */
  seatPins?: () => BridgeNatsSeatPins;
  /** Tests only: see the error behind a fixed GAP code. */
  onError?: (error: unknown) => void;
  /** Tests only: the real-broker test's link state directory, counted as durable (allow-writer ReadOptions). */
  durableStateDir?: string;
}
interface ExecObservation { execCode: number | null; execStdout: string; execStderr: string }
interface Context {
  runDir: string; request: SeamRequest; sealed: SealedRun; lockFd: number; uid: number; io: BridgeIo; deps: BridgeDeps; pins: BridgePins;
  now: () => number; sleep: (ms: number) => Promise<void>; deadline: number; started: number; requestSha256: string;
  execs: ExecObservation[];
}

const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const keys = (value: Record<string, unknown>): string => Object.keys(value).sort().join(",");
const receipt = (request: SeamRequest, result: "pass" | "refused", observed: Record<string, unknown>): string =>
  JSON.stringify({ schema: "d3-seam-receipt-v1", run: request.run, phase: request.phase, owner: OWNER, result, observed });
const errorDetails = (error: unknown): { errorClass: string; errorMessage: string } => {
  const safe = redactNatsError(error);
  return { errorClass: safe.name.slice(0, 500), errorMessage: safe.message.slice(0, 500) };
};
// Redact BEFORE bounding bytes or selecting a line: a quoted credential can cross either boundary.
const execText = (text: string): string => {
  // A transport capture can end inside a quoted value; the shared sanitizer expects its closing quote.
  const safe = redactNatsError(text.replace(/([=:]\s*)(?:"[^"]*$|'[^']*$)/g, "$1***")).message
    // The shared NATS sanitizer removes credentials, not local paths. Keep only file names here.
    .replace(/(["'])(?:[A-Za-z]:[\\/]|~[\\/]|\.{1,2}[\\/]|[\\/])(?:(?!\1)[^\r\n])*\1/g,
      file => `${file[0]}${path.win32.basename(file.slice(1, -1)) || "[path]"}${file[0]}`)
    .replace(/(^|[\s"'=(,:])(?:[A-Za-z]:[\\/]|~[\\/]|\.{1,2}[\\/]|[\\/])[^\s"'<>),;]*/g,
      (file: string, prefix: string) => `${prefix}${path.win32.basename(file.slice(prefix.length)) || "[path]"}`)
    .replace(/[\u0000-\u0008\u000b-\u001f\u007f]/g, "?");
  const bytes = Buffer.from(safe, "utf8");
  let end = Math.min(bytes.length, 500);
  // Never decode an incomplete UTF-8 character into a replacement that would exceed the byte budget.
  while (end < bytes.length && (bytes[end]! & 0xc0) === 0x80) end -= 1;
  return bytes.subarray(0, end).toString("utf8");
};
const execStdout = (text: string): string => {
  const first = text.split(/\r?\n/, 1)[0] ?? "";
  try {
    const parsed: unknown = JSON.parse(first);
    // Decode the JSON error before redaction (its embedded quotes may be JSON-escaped).
    if (object(parsed) && typeof parsed.error === "string") return execText(JSON.stringify({ error: execText(parsed.error) }));
    // Unknown JSON fields can contain escaped or nested credentials: reuse the sanitizer's object allowlist.
    return execText(redactNatsError(parsed).message);
  } catch { /* non-JSON transport output: redact the whole capture before retaining its first line */ }
  return execText(text).split(/\r?\n/, 1)[0] ?? "";
};
const execEvidence = (context: Context, value: Record<string, unknown>): Record<string, unknown> => {
  const last = context.execs.at(-1);
  if (!last) return {};
  const failed = [...context.execs].reverse().find(read => read.execCode !== 0);
  return { measured: { ...(object(value.measured) ? value.measured : {}), ...last, execs: context.execs },
    ...(failed ? { errorClass: "KubectlExecError", errorMessage: JSON.stringify({ execCode: failed.execCode,
      stdout: failed.execStdout, stderr: failed.execStderr }) } : {}) };
};
const evidence = (context: Context, name: string, value: Record<string, unknown>): string =>
  writeOnce(context.runDir, `fabric-v2-bridge-${context.request.phase}-${name}.json`, Buffer.from(`${JSON.stringify({
    ...value, ...(name === "outcome" && value.result !== "pass" ? {
      errorClass: value.errorClass ?? null, errorMessage: value.errorMessage ?? null } : {}),
    ...(name === "outcome" ? execEvidence(context, value) : {}),
    owner: OWNER, run: context.request.run, phase: context.request.phase, sessionId: context.request.sessionId,
    requestSha256: context.requestSha256, elapsedMs: Math.round(context.now() - context.started) }, null, 1)}\n`));
const clock = (context: Context, budgetMs = Number.POSITIVE_INFINITY) => ({
  now: context.now, sleep: context.sleep, deadline: Math.min(context.deadline, context.now() + budgetMs) });

/** kubectl floors its timeout at 1 s: never start another read with less phase time left than that. */
const podReadBudget = (context: Context, maximum: number): number => {
  const remaining = context.deadline - context.now();
  if (remaining < 1_000) throw new SeamGap("phase-deadline");
  return Math.min(maximum, remaining);
};

export const main = async (argv: string[], io: BridgeIo, pins: BridgePins | undefined, deps: BridgeDeps = {}): Promise<number> => {
  const now = deps.now ?? (() => performance.now());
  const started = now();
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
  const deadline = started + Math.min(deps.deadlineMs ?? DEADLINE_MS, DEADLINE_MS);
  const uid = deps.uid ?? process.getuid!();
  try {
    if (argv.length !== 4 || argv[0] !== "--run-dir" || argv[2] !== "--phase" || !PHASES.has(argv[3]!)) throw new SeamGap("usage");
    if (!pins || !path.isAbsolute(pins.linkConfig) || !LINK_UNIT.test(pins.linkUnit)) throw new SeamGap("seam-not-installed");
    const runDir = argv[1]!;
    const phase = argv[3]!;
    const stdin = io.stdin();
    if (stdin.length > MAX_REQUEST) throw new SeamGap("request-too-large");
    const request = parseRequest(stdin, phase, { operationId: false });
    verifyRunDir(runDir, uid);
    const sealed = readSealedRun(runDir, request, uid, "bridge");
    const lockFd = proveSeatLock(runDir, io.env.D3_SEAT_LOCK_FD, uid, deps.python);
    const context: Context = { runDir, request, sealed, lockFd, uid, io, deps, pins, now, sleep, deadline, started, requestSha256: sha256(stdin), execs: [] };
    if (phase === "resume") return await resume(context);
    if (phase === "publish") return await publish(context);
    return await restoreRoute(context);
  } catch (error) {
    const code = error instanceof SeamGap ? error.code : "seam-internal";
    deps.onError?.(error);
    io.stderr(`GAP: ${code} [owner: ${OWNER}]\n`);
    return 2;
  }
};

// ---------------------------------------------------------------- the Pod's Main, read through kubectl exec

const kubeAccess = (context: Context): KubeAccess => {
  const kube = context.sealed.inputs.kube;
  if (!object(kube) || typeof kube.kubeconfig !== "string" || !path.isAbsolute(kube.kubeconfig) || typeof kube.server !== "string" ||
    typeof kube.caSha256 !== "string" || !HEX64.test(kube.caSha256)) throw new SeamGap("kube-unpinned");
  return { kubectl: context.deps.kubectl ?? fixedKubectl(), kubeconfig: kube.kubeconfig, context: context.sealed.run.context as string,
    namespace: context.sealed.run.namespace as string, server: kube.server, caSha256: kube.caSha256, home: context.sealed.seat.home };
};

const podPin = (value: unknown): { name: string; uid: string } => {
  if (!object(value) || keys(value) !== "name,uid" || typeof value.name !== "string" || !POD_NAME.test(value.name) ||
    typeof value.uid !== "string" || !UUID.test(value.uid)) throw new SeamGap("inputs-invalid");
  return { name: value.name, uid: value.uid };
};

const targetNode = (context: Context): string => {
  const target = context.sealed.run.target;
  if (!object(target) || typeof target.node !== "string" || !target.node) throw new SeamGap("target-node-unpinned");
  return target.node;
};

export interface PodIdentity {
  rootId: string; pid: number; ownerHostId: string; ownerIdentityId: string; ownerIncarnation: string;
  machine: string; processStartedAt: string; projectionKey: string; projectionRevision: number;
}

const samePi = (before: PodView, after: PodView): boolean => before.piRestartCount !== null && before.piStartedAt !== null &&
  before.piContainerId !== null && after.piRestartCount === before.piRestartCount && after.piStartedAt === before.piStartedAt &&
  after.piContainerId === before.piContainerId;
const onlyPiWrites = (pod: PodView): boolean => pod.pvcWriters.length === 1 && pod.pvcWriters[0] === "pi";

/**
 * Pin the Pod and its running container, read with fixed Python argv, then re-check the same container.
 * Resume reads only journal bytes; publish reads the image's own local NATS projection + live /proc identity.
 */
const podIdentity = async (context: Context, access: KubeAccess, pin: { name: string; uid: string },
  seal?: { leafId: string; journalBytes: number; journalSha256: string }, measured = { observations: 0, waitedMs: 0 }) => {
  measured.observations += 1;
  let before = await readPod(access, pin.name, podReadBudget(context, 30_000));
  if (before && before.uid !== pin.uid) throw new SeamGap("pod-uid-mismatch");
  if (before && before.nodeName !== targetNode(context)) throw new SeamGap("pod-node-mismatch");
  if (context.now() >= context.deadline) throw new SeamGap("phase-deadline");
  // Only the pinned Pod's not-yet-running container is polled; no unavailable or failed read is retried.
  if (before && !before.piRunning && before.piTerminated === null) {
    const wait = clock(context, 180_000);
    const started = wait.now();
    try {
      while (before && !before.piRunning && before.piTerminated === null && wait.now() < wait.deadline) {
        await wait.sleep(Math.min(1_000, wait.deadline - wait.now()));
        const remaining = wait.deadline - wait.now();
        // kubectl floors timeouts at 1 s. A late wake or exhausted wait must not start another API read.
        if (remaining < 1_000) break;
        measured.observations += 1;
        before = await readPod(access, pin.name, podReadBudget(context, Math.min(30_000, remaining)));
        if (before && before.uid !== pin.uid) throw new SeamGap("pod-uid-mismatch");
        if (before && before.nodeName !== targetNode(context)) throw new SeamGap("pod-node-mismatch");
        if (context.now() >= context.deadline) throw new SeamGap("phase-deadline");
      }
    } finally {
      measured.waitedMs += Math.round(wait.now() - started);
    }
  }
  let reason: string | null = null;
  let identity: PodIdentity | null = null;
  let journal: JournalRead | null = null;
  let line: string | null = null;
  let execCode: number | null = null;
  let journalError: string | null = null;
  const rendered = sessionArg(before ?? null);
  if (!before) reason = "pod-unreadable";
  else if (!before.piRunning) reason = "pod-not-running";
  else if (!samePi(before, before)) reason = "pod-container-identity-unknown";
  else if (!onlyPiWrites(before)) reason = "pod-pvc-writers-unproved";
  else if (seal && (!rendered || !path.isAbsolute(rendered))) reason = "pod-session-arg-unknown";
  else if (!seal && !before.meshRoot) reason = "pod-mesh-root-unknown";
  else {
    const argv = seal ? podJournalArgv(rendered!, seal.journalBytes) : podOwnerArgv(before.meshRoot!, context.request.sessionId);
    const result = await kubectl(access, ["-n", access.namespace, "exec", pin.name, "-c", "pi", "--", ...argv],
      podReadBudget(context, 120_000));
    execCode = result.code;
    // Record immediately: parsing, the after-read API call or a later route wait may throw.
    context.execs.push({ execCode, execStdout: execStdout(result.stdout),
      execStderr: execText(result.stderr) });
    const rawLine = result.stdout.trim();
    line = execStdout(rawLine);
    let parsed: unknown;
    try { parsed = JSON.parse(rawLine); } catch { parsed = undefined; }
    if (object(parsed) && typeof parsed.error === "string") journalError = execText(parsed.error);
    if (seal) {
      journal = result.code === 0 ? journalRead(parsed, seal) : null;
      if (!journal) reason = "pod-journal-unreadable";
    } else {
      if (result.code !== 0 || !object(parsed) || parsed.rootId !== `session:${context.request.sessionId}` ||
        !Number.isSafeInteger(parsed.pid) || (parsed.pid as number) < 1 ||
        parsed.ownerIdentityId !== parsed.rootId || typeof parsed.ownerHostId !== "string" || !parsed.ownerHostId ||
        typeof parsed.ownerIncarnation !== "string" || !parsed.ownerIncarnation || parsed.ownerIncarnation.length > 128 ||
        typeof parsed.machine !== "string" || typeof parsed.processStartedAt !== "string" || !/^[1-9][0-9]*$/.test(parsed.processStartedAt) ||
        typeof parsed.projectionKey !== "string" || !Number.isSafeInteger(parsed.projectionRevision) || (parsed.projectionRevision as number) < 1) {
        reason = "pod-identity-unreadable";
      } else identity = parsed as unknown as PodIdentity;
    }
  }
  const after = before?.piRunning && execCode !== null ? await readPod(access, pin.name, podReadBudget(context, 30_000)) : undefined;
  if (!reason && context.now() >= context.deadline) throw new SeamGap("phase-deadline");
  if (before?.piRunning && execCode !== null) {
    if (!after || after.uid !== pin.uid) reason = "pod-uid-changed";
    else if (after.nodeName !== targetNode(context)) reason = "pod-node-mismatch";
    else if (!after.piRunning) reason = "pod-not-running";
    else if (!samePi(before, after)) reason = "pod-restarted-during-read";
    else if (!onlyPiWrites(after) || sessionArg(after) !== rendered || after.meshRoot !== before.meshRoot ||
      after.natsMachine !== before.natsMachine) reason = "pod-spec-changed";
  }
  return { before: before ?? null, after: after ?? null, identity, journal, journalError, reason, line, execCode,
    observations: measured.observations, waitedMs: measured.waitedMs };
};

// Evidence is a diagnostic copy, never the raw object (which may carry unvalidated extra fields).
// Keep the raw identity above for route comparisons; sanitization must not change the protocol's verdict.
const podAssertion = (read: Awaited<ReturnType<typeof podIdentity>>) => ({
  identity: read.identity === null ? null : {
    rootId: execText(read.identity.rootId), pid: read.identity.pid,
    ownerHostId: execText(read.identity.ownerHostId), ownerIdentityId: execText(read.identity.ownerIdentityId),
    ownerIncarnation: execText(read.identity.ownerIncarnation), machine: execText(read.identity.machine),
    processStartedAt: execText(read.identity.processStartedAt), projectionKey: execText(read.identity.projectionKey),
    projectionRevision: read.identity.projectionRevision,
  }, podLine: read.line,
});

interface JournalRead {
  leafId: string; entries: number; sealOnBranch: boolean; truncated: boolean;
  appendedSinceSeal: AppendedEntry[] | null;
  journalSha256: string; journalBytes: number; sealedSha256: string | null; suffixBytes: number; suffixBase64: string | null; mtimeNs: string;
}

/** Only bytes/stat facts come from Python. Pi's last-entry leaf and suffix facts are derived here, never asserted by a CLI. */
const journalRead = (value: unknown, seal: { leafId: string; journalBytes: number; journalSha256: string }): JournalRead | null => {
  if (!object(value) || keys(value) !== "mtimeNs,prefixSha256,sha256,size,suffixBase64" ||
    !Number.isSafeInteger(value.size) || (value.size as number) < seal.journalBytes ||
    typeof value.sha256 !== "string" || !HEX64.test(value.sha256) ||
    typeof value.prefixSha256 !== "string" || !HEX64.test(value.prefixSha256) ||
    typeof value.mtimeNs !== "string" || !/^[0-9]+$/.test(value.mtimeNs) ||
    typeof value.suffixBase64 !== "string" || value.suffixBase64.length > 4 * Math.ceil(SUFFIX_MAX / 3)) return null;
  if (value.prefixSha256 !== seal.journalSha256) throw new SeamGap("journal-prefix-mismatch");
  const suffix = Buffer.from(value.suffixBase64, "base64");
  if (suffix.toString("base64") !== value.suffixBase64 || suffix.length > SUFFIX_MAX ||
    !Buffer.from(suffix.toString("utf8"), "utf8").equals(suffix)) return null;
  let entries: AppendedEntry[];
  try {
    entries = suffix.toString("utf8").split("\n").filter(line => line.trim()).map(line => {
      const fact = entryFacts(line);
      if (!fact) throw new Error("session header in suffix");
      return fact;
    });
  } catch { throw new SeamGap("journal-suffix-invalid"); }
  const chained = entries.every((entry, index) => entry.parentId === (index === 0 ? seal.leafId : entries[index - 1]!.id));
  return { leafId: entries.at(-1)?.id ?? seal.leafId, entries: entries.length, sealOnBranch: chained, truncated: false,
    appendedSinceSeal: entries, journalSha256: value.sha256, journalBytes: value.size as number, sealedSha256: value.prefixSha256,
    suffixBytes: suffix.length, suffixBase64: value.suffixBase64, mtimeNs: value.mtimeNs };
};

/** A's final seal (A at 7d2e09af+, rule T-FIX3): exactly these seven keys. */
const FINAL_SEAL_KEYS = "journal,journalBytes,journalSha256,leafId,run,schema,sealHasThinkingEntry";

/**
 * One final seal's shape: exactly the seven keys, each of its type; `sealHasThinkingEntry` is true or false only
 * (missing, a string, a number or null is invalid).
 */
const validSeal = (context: Context, seal: unknown): seal is Record<string, unknown> & { journalSha256: string; journalBytes: number;
  leafId: string; sealHasThinkingEntry: boolean } =>
  object(seal) && keys(seal) === FINAL_SEAL_KEYS && seal.schema === "d3-seat-final-seal-v1" && seal.run === context.request.run &&
  seal.journal === context.sealed.journal && typeof seal.journalSha256 === "string" && HEX64.test(seal.journalSha256) &&
  Number.isSafeInteger(seal.journalBytes) && (seal.journalBytes as number) > 0 && typeof seal.leafId === "string" &&
  ENTRY_ID.test(seal.leafId) && (seal.sealHasThinkingEntry === true || seal.sealHasThinkingEntry === false);

/**
 * A's final seal, from RUN itself: RUN/receipts/final-seal.json, whose sha256 is the journal's ONE `final-seal`
 * record, for this run and the sealed source journal, and identical (all seven keys) to the request's finalSeal. Both
 * copies must be valid seals (`final-seal-invalid`) before they are compared (`final-seal-mismatch`).
 */
const finalSealOf = (context: Context, requested: unknown):
  { journalSha256: string; journalBytes: number; leafId: string; sealHasThinkingEntry: boolean } => {
  const bytes = readOwned(path.join(context.runDir, "receipts", "final-seal.json"), context.uid, 64 * 1024, undefined, "final-seal-unreadable");
  const records = readOwned(path.join(context.runDir, "seat-journal.jsonl"), context.uid, 64 * 1024 * 1024, undefined, "seat-journal-unreadable")
    .toString("utf8").split("\n").filter(line => line.trim()).map(line => { try { return JSON.parse(line) as Record<string, unknown>; } catch { return {}; } })
    .filter(record => record.event === "final-seal");
  if (records.length !== 1 || records[0]!.sha256 !== sha256(bytes)) throw new SeamGap("final-seal-unjournaled");
  let seal: unknown;
  try { seal = JSON.parse(bytes.toString("utf8")); } catch { throw new SeamGap("final-seal-invalid"); }
  if (!validSeal(context, seal) || !validSeal(context, requested)) throw new SeamGap("final-seal-invalid");
  if (!FINAL_SEAL_KEYS.split(",").every(key => requested[key] === seal[key])) throw new SeamGap("final-seal-mismatch");
  return { journalSha256: seal.journalSha256, journalBytes: seal.journalBytes, leafId: seal.leafId,
    sealHasThinkingEntry: seal.sealHasThinkingEntry };
};

/**
 * Pi (031c7499 sdk.js:135, :346-349) appends exactly one thinking_level_change for an existing conversation whose
 * branch has none, before AgentSession construction and session_start; the file gets it at once. Fabric's first
 * NATS connect is later, during activation, before its identity socket opens (fabric-runtime-state.ts). This is
 * ONLY a missing-append wait after a readable identity, not a retry of unavailable identity or NATS startup.
 * Give that existing wait the 600 s startup allowance, sharing the phase's original deadline, never adding to it.
 */
const APPEND_WAIT_MS = 600_000;
const APPEND_POLL_MS = 2_000;
// Keep b4's conservative full-read allowance: two API reads, fixed exec, and 30 s to record the coherent outcome.
const APPEND_READ_MS = 30_000 + 120_000 + 30_000 + 30_000;

/**
 * Every entry id of the sealed journal, from the seat host's own sealed copy (the seal's `journal`). Its bytes must be
 * the seal's (size, sha256, last line's id); the branch reader refuses a cycle or a missing parent in it.
 */
const sealedEntryIds = (context: Context, seal: { journalSha256: string; journalBytes: number; leafId: string }): Set<string> => {
  const ids = new Set<string>();
  let read: ReturnType<typeof journalBranch>;
  try { read = journalBranch(context.sealed.journal, seal.leafId, seal.journalBytes, seal.journalBytes, ids); }
  catch (error) {
    if (error instanceof JournalBranchError && error.code === "JOURNAL_TOO_LARGE") throw new SeamGap("journal-prefix-mismatch");
    throw new SeamGap("source-journal-unreadable");
  }
  if (read.journalBytes !== seal.journalBytes || read.journalSha256 !== seal.journalSha256 || read.leafId !== seal.leafId) {
    throw new SeamGap("journal-prefix-mismatch");
  }
  return ids;
};

const sessionArg = (pod: PodView | null): string | null => {
  const args = pod?.piArgs;
  const index = args ? args.indexOf("--session") : -1;
  return args && args.filter(arg => arg === "--session").length === 1 && index >= 0 &&
    typeof args[index + 1] === "string" ? args[index + 1]! : null;
};

// ---------------------------------------------------------------- resume

const resume = async (context: Context): Promise<number> => {
  const { inputs, sessionId } = context.request;
  // A sends its whole final seal (require_final_seal_unchanged): the seven keys, checked against RUN's own copy.
  if (keys(inputs) !== "finalSeal,pod") throw new SeamGap("inputs-invalid");
  const pin = podPin(inputs.pod);
  const seal = finalSealOf(context, inputs.finalSeal);
  const access = kubeAccess(context);
  await verifyKubeconfig(access, podReadBudget(context, 30_000));
  const intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-resume-intent-v1", pod: pin, finalSeal: inputs.finalSeal,
    measured: { kubectl: access.kubectl, server: access.server, interpreter: interpreterIdentity(), startedAt: new Date().toISOString() },
    residuals: RESIDUALS });
  const measured = { finalSeal: seal, observations: 0, waitedMs: 0 };
  let outcome: Record<string, unknown> = { measured };
  let failed = false;
  let output = "";
  try {
    let read = await podIdentity(context, access, pin, seal, measured);
    // Only the one state that Pi's startup append ends is re-observed: the exact sealed bytes, nothing after them.
    const awaitingAppend = (): boolean => {
      const rendered = sessionArg(read.before);
      const journal = read.journal;
      return !seal.sealHasThinkingEntry && read.reason === null && rendered !== null &&
        journal !== null && journal.leafId === seal.leafId &&
        journal.journalBytes === seal.journalBytes && journal.journalSha256 === seal.journalSha256 &&
        journal.sealedSha256 === seal.journalSha256 && journal.sealOnBranch && !journal.truncated &&
        journal.suffixBytes === 0 && journal.suffixBase64 === "" && journal.appendedSinceSeal?.length === 0;
    };
    const wait = clock(context, APPEND_WAIT_MS);
    while (awaitingAppend() && wait.now() < wait.deadline) {
      await wait.sleep(Math.min(APPEND_POLL_MS, wait.deadline - wait.now()));
      // Do not let another full identity read consume the phase's last budget and bypass the missing-append outcome.
      // A late wake also stops polling; the last coherent observation still gets its normal checks below.
      if (wait.deadline - wait.now() <= APPEND_READ_MS) break;
      read = await podIdentity(context, access, pin, seal, measured);
    }
    const rendered = sessionArg(read.before);
    const journal = read.journal;
    const gap = (code: string): never => {
      outcome = { result: "unknown", gap: code,
        measured: { before: read.before, after: read.after, renderedSession: rendered, execCode: read.execCode, journal, ...measured },
        asserted: podAssertion(read) };
      throw new SeamGap(code);
    };
    // Fixed reader failures with a precise byte-level cause remain UNKNOWN, never a pass.
    if (read.journalError && ["journal-suffix-too-large", "journal-prefix-mismatch", "journal-changed-during-read"].includes(read.journalError)) {
      gap(read.journalError);
    }
    let reason = read.reason;
    let appended: AppendedEntry[] = [];
    if (!reason && journal) {
      // The journal itself must still begin with exactly the sealed bytes, and grew by at most 64 KiB: no receipt otherwise.
      if (journal.sealedSha256 !== seal.journalSha256) gap("journal-prefix-mismatch");
      if (journal.suffixBytes > SUFFIX_MAX || journal.suffixBase64 === null) gap("journal-suffix-too-large");
      // The sealed leaf must be the current leaf or an ancestor: otherwise the branch diverged after the seal.
      if (!journal.sealOnBranch) gap("branch-diverged");
      // The seam's own measure of the suffix: its bytes are the size the CLI reports, and its entries ARE the appended
      // branch entries, in order, the first one's parent the sealed leaf.
      const suffix = Buffer.from(journal.suffixBase64!, "base64");
      if (suffix.length !== journal.suffixBytes || journal.journalBytes !== seal.journalBytes + suffix.length ||
        (suffix.length === 0 && journal.journalSha256 !== seal.journalSha256)) gap("journal-suffix-inconsistent");
      try {
        appended = suffix.toString("utf8").split("\n").filter(line => line.trim()).map(line => entryFacts(line)!)
          .filter((entry): entry is AppendedEntry => entry !== null);
      } catch { gap("journal-suffix-invalid"); }
      // Every appended entry is NEW: not an id of the sealed journal (read from the host's own sealed copy, never the
      // Pod's report) and not repeated in the tail. A reused id can re-point a sealed entry under Pi's later-id-wins index.
      if (appended.length > 0) {
        const sealedIds = sealedEntryIds(context, seal);
        const tail = new Set<string>();
        for (const entry of appended) {
          if (sealedIds.has(entry.id) || tail.has(entry.id)) gap("resume-tail-id-reused");
          tail.add(entry.id);
        }
      }
      // With new ids, a chain from the sealed leaf through the tail in order has no cycle and ends at the reported leaf.
      const chained = appended.every((entry, index) => entry.parentId === (index === 0 ? seal.leafId : appended[index - 1]!.id));
      if (journal.truncated || !chained || !sameEntries(appended, journal.appendedSinceSeal ?? []) ||
        journal.leafId !== (appended.at(-1)?.id ?? seal.leafId)) gap("journal-suffix-not-the-branch");
    }
    if (!reason && !rendered) reason = "pod-session-arg-unknown";
    // 0.112.0 exposes no Main-drain identity. The rendered path and exact bytes are A's resume contract;
    // A independently re-derives hash, leaf, appended entries and the thinking-entry rule from its sealed copy.
    // Only a coherent identity and unchanged journal can mean the startup append is still missing. Do not mask a
    // readable Main's session/leaf mismatch with this GAP, even when its journal is a byte-identical sealed copy.
    if (!reason && !seal.sealHasThinkingEntry && appended.length === 0) gap("resume-thinking-append-missing");
    const observed = { sessionId, journal: rendered ?? "", leafId: journal?.leafId ?? "", podUid: pin.uid,
      appendedSinceSeal: appended, journalSha256: journal?.journalSha256 ?? "", suffixBase64: journal?.suffixBase64 ?? "" };
    const passed = reason === null;
    outcome = { result: passed ? "pass" : "refused", reason,
      observed, measured: { before: read.before, after: read.after, renderedSession: rendered, execCode: read.execCode, journal,
        ...measured }, asserted: podAssertion(read) };
    output = receipt(context.request, passed ? "pass" : "refused", observed);
  } catch (error) {
    failed = true;
    outcome = { ...outcome, ...(outcome.result === "unknown" ? {} : {
      result: "refused", reason: error instanceof SeamGap ? error.code : "seam-internal" }), ...errorDetails(error) };
    throw error;
  } finally {
    try {
      evidence(context, "outcome", { schema: "fabric-v2-bridge-resume-outcome-v1", ...outcome, intentSha256 });
    } catch (error) {
      // Preserve the original operation's GAP if its outcome cannot be persisted either.
      if (!failed) throw error;
    }
  }
  context.io.stdout(output);
  return 0;
};

const sameEntries = (left: readonly AppendedEntry[], right: readonly AppendedEntry[]): boolean =>
  left.length === right.length && left.every((entry, index) => entry.id === right[index]!.id && entry.parentId === right[index]!.parentId &&
    entry.type === right[index]!.type && entry.thinkingLevel === right[index]!.thinkingLevel);

// ---------------------------------------------------------------- publish

const seatPins = (context: Context, orgJson: string | undefined): BridgeNatsSeatPins => {
  try { return context.deps.seatPins ? context.deps.seatPins() : loadBridgeNatsSeatPins(orgJson); }
  catch { throw new SeamGap("seat-pins-unreadable"); }
};

const readOptions = (context: Context): ReadOptions =>
  context.deps.durableStateDir === undefined ? {} : { durableStateDir: context.deps.durableStateDir };
const lockOptions = (context: Context) => ({ sleep: context.sleep, now: context.now,
  ...(context.deps.python === undefined ? {} : { python: context.deps.python }) });

/**
 * The seat's NATS machine, from the run-pinned principal NATS binding (A's T-FIX6: top-level `natsMachine` in the sealed
 * run; never `instance`, which is the Herdr agent). Absent or malformed refuses.
 */
const seatMachine = (context: Context): string => {
  const machine = context.sealed.run.natsMachine;
  if (machine === undefined) throw new SeamGap("seat-nats-machine-unpinned");
  if (typeof machine !== "string" || !MACHINE.test(machine)) throw new SeamGap("seat-nats-machine-invalid");
  return machine;
};

type Gate = Record<string, unknown> | null;

/**
 * The route, judged through the link's REAL identity gate plus the records it rests on: the gate resolves the seat on
 * `side` as `live` (an id with a second record, live or not, is ambiguous there and refuses); the hub's record is the
 * expected kind (native, or this link's mirror) and live; the NATS records for the id are exactly ONE, on
 * `natsMachine`, live; and both carry `incarnation`. Only a separately proved legacy v1 writer may use the FILE
 * exception: the link's gate and native FILE record must agree on any incarnation they expose, or both omit it.
 */
const routeHolds = (gate: Gate, receiptValue: Record<string, unknown>, want: { side: "file" | "nats"; fileRemoteHost: string | null;
  natsMachine: string; incarnation: string | null; legacySessionId?: string }): boolean => {
  const file = object(receiptValue.file) ? receiptValue.file : undefined;
  if (gate === null || gate.status !== "live" || gate.side !== want.side ||
    (want.side !== "file" && gate.machine !== want.natsMachine) ||
    file === undefined || (file.remoteHost ?? null) !== want.fileRemoteHost || file.live !== true) return false;
  if (want.legacySessionId !== undefined) {
    if (want.side !== "file" || receiptValue.side !== "file" || want.fileRemoteHost !== null ||
      gate.key !== `session:${want.legacySessionId}` || gate.id !== `session:${want.legacySessionId}`) return false;
    return want.incarnation === null
      ? file.ownerIncarnation == null && gate.ownerIncarnation == null
      : (file.ownerIncarnation == null || file.ownerIncarnation === want.incarnation) &&
        (gate.ownerIncarnation == null || gate.ownerIncarnation === want.incarnation) &&
        (file.ownerIncarnation === want.incarnation || gate.ownerIncarnation === want.incarnation);
  }
  const nats = object(receiptValue.nats) && Array.isArray(receiptValue.nats.records) ? (receiptValue.nats.records as unknown[]).filter(object) : [];
  return want.incarnation !== null && file.ownerIncarnation === want.incarnation &&
    nats.length === 1 && nats[0]!.machine === want.natsMachine && nats[0]!.live === true && nats[0]!.ownerIncarnation === want.incarnation;
};

const planEvidence = (plan: WriterPlan) => ({ change: plan.change, beforeSha256: plan.before.sha256, afterSha256: plan.afterSha256,
  natsEntry: plan.natsEntry, fileEntry: plan.fileEntry, beforeHadNats: plan.beforeHadNats, beforeBytes: plan.before.bytes.toString("utf8"),
  afterBytes: plan.after?.toString("utf8") ?? null, file: plan.before.file });

const authorityEvidence = (authority: RollbackAuthority | null) => authority === null ? null : {
  beforeSha256: authority.beforeSha256, afterSha256: authority.afterSha256, beforeBytesBase64: authority.beforeBytes.toString("base64"),
  natsEntry: authority.natsEntry, fileEntry: authority.fileEntry, beforeHadNats: authority.beforeHadNats,
};

const publish = async (context: Context): Promise<number> => {
  const { inputs, sessionId } = context.request;
  if (keys(inputs) !== "owner" || !object(inputs.owner) || keys(inputs.owner) !== "claim,pod,sandbox,template") throw new SeamGap("inputs-invalid");
  const pin = podPin(inputs.owner.pod);
  const machine = seatMachine(context);
  const access = kubeAccess(context);
  await verifyKubeconfig(access, Math.min(30_000, context.deadline - context.now()));
  // A first, unlocked read: the seat pins, a dry plan (refuses early) and the routes the link carries now.
  const first = readLinkConfig(context.pins.linkConfig, readOptions(context));
  const pins = seatPins(context, first.config.orgJson);
  planPublish(first, pins, { sessionId, machine });
  const routesBefore = await routeTable(first.config, context.deps);
  if (!routesBefore) throw new SeamGap("link-status-unreadable");
  const measuredPod = { kubectl: access.kubectl, server: access.server, interpreter: interpreterIdentity(), startedAt: new Date().toISOString() };
  let plan: WriterPlan | null = null;
  let post: LinkConfigFile | null = null;
  let intentSha256: string | null = null;
  let outcome: Record<string, unknown> = { written: false };
  let failed = false;
  let output = "";
  try {
    // The managed Pod, read-only BEFORE any write; also look for its own NATS projection.
    const read = await podIdentity(context, access, pin);
    let reason = read.reason;
    // piRunning precedes Fabric activation. The existing route wait waits for publication and re-reads the local copy.
    if (reason === "pod-identity-unreadable" && read.execCode === 1 && read.journalError === "pod-owner-unreadable") reason = null;
    if (!reason && (read.before?.natsMachine !== machine || (read.identity !== null && read.identity.machine !== machine))) {
      reason = read.before?.natsMachine ? "pod-nats-machine-mismatch" : "pod-nats-machine-unknown";
    }
    let liveRead = read;
    outcome = { written: false, measured: { before: read.before, after: read.after,
      observations: read.observations, waitedMs: read.waitedMs }, asserted: podAssertion(read) };
    if (reason) {
      intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-publish-intent-v1", owner: inputs.owner, machine, plan: null,
        rollback: null, unit: context.pins.linkUnit, routesBefore, measured: measuredPod, residuals: RESIDUALS });
    } else {
      // ONE config-scoped lock across read, plan, intent, check, rename and read-back.
      const locked = await withConfigLock(context.pins.linkConfig, () => {
        const current = readLinkConfig(context.pins.linkConfig, readOptions(context));
        const planned = planPublish(current, pins, { sessionId, machine });
        // Durable BEFORE the write: keep its sha even if applyPlan throws before the callback returns.
        intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-publish-intent-v1", owner: inputs.owner, machine,
          plan: planEvidence(planned), rollback: authorityEvidence(rollbackAuthority(planned)), unit: context.pins.linkUnit, routesBefore,
          measured: measuredPod, residuals: RESIDUALS });
        outcome.written = planned.change === "none" ? false : null;
        const after = applyPlan(planned);
        outcome.written = planned.change !== "none";
        return { planned, after };
      }, lockOptions(context));
      plan = locked.planned;
      post = locked.after;
    }
    const written = plan !== null && plan.change !== "none";
    let reload: Awaited<ReturnType<typeof reloadLink>> | null = null;
    let others: Awaited<ReturnType<typeof otherRoutesBack>> | null = null;
    let routed: Awaited<ReturnType<typeof awaitRoutable>> | null = null;
    let presence: Awaited<ReturnType<typeof natsRecords>> = [];
    const incarnationSource = "pod-local-nats-projection-and-nats-presence";
    // Everything after the write reads the config ON DISK now (re-read and re-validated by applyPlan).
    if (!reason && written) {
      reload = await reloadLink(post!.config, context.pins.linkUnit, context.sealed.seat, context.deps, clock(context, 120_000));
      if (!reload.statusOk) reason = "link-not-reloaded";
    }
    if (!reason) {
      routed = await awaitRoutable(post!.config, sessionId, context.deps, clock(context, PUBLISH_SEAT_ROUTE_WAIT_MS), async (gate, value) => {
        presence = (await natsRecords(post!.config, context.deps)).filter(record => record.id === `session:${sessionId}`);
        const record = presence.length === 1 ? presence[0] : undefined;
        if (!record || record.machine !== machine || record.live !== true || !record.ownerIncarnation ||
          !routeHolds(gate, value, { side: "nats", fileRemoteHost: post!.config.link, natsMachine: machine,
            incarnation: record.ownerIncarnation })) return false;
        if (context.deadline - context.now() < 1_000) return false;
        liveRead = await podIdentity(context, access, pin);
        if (liveRead.reason !== null || !liveRead.identity) return false;
        if (!samePi(read.before!, liveRead.before!)) throw new SeamGap("pod-restarted-during-publish");
        return liveRead.identity.machine === machine && record.ownerIncarnation === liveRead.identity.ownerIncarnation &&
          record.ownerHostId === liveRead.identity.ownerHostId && record.ownerIdentityId === liveRead.identity.ownerIdentityId;
      });
      if (!routed.ok) reason = routed.gate !== null && routed.gate.status !== "live" ? "seat-route-refused-by-gate" : "seat-not-routable";
    }
    if (written) {
      // Every other route the link carried live before the restart must be live again, whatever this seat's result.
      others = await otherRoutesBack(post!.config, routesBefore, `session:${sessionId}`, context.deps, clock(context, OTHER_ROUTES_WAIT_MS));
      if (!reason && others.down.length > 0) reason = "other-routes-down";
    }
    // A known precheck refusal needs no final read, especially after the startup wait used the phase's last time.
    const final = reason === null && context.deadline - context.now() >= 1_000
      ? await readPod(access, pin.name, podReadBudget(context, 30_000)) : undefined;
    if (!reason && (!final || final.uid !== pin.uid || !final.piRunning)) reason = "pod-uid-changed";
    if (!reason && (final!.nodeName !== targetNode(context) || !samePi(read.before!, final!) || !onlyPiWrites(final!))) {
      reason = "pod-restarted-during-publish";
    }
    const route = `v2@${pin.uid}:${(post?.sha256 ?? first.sha256).slice(0, 12)}`;
    const observed = { sessionId, incarnation: pin.uid, route };
    const passed = reason === null;
    outcome = { result: passed ? "pass" : "refused", reason,
      observed, written, measured: { before: read.before, after: read.after, final: final ?? null, configSha256: post?.sha256 ?? null, reload,
        otherRoutes: others, gate: routed?.gate ?? null, receipt: routed?.receipt ?? null, presence, incarnationSource,
        observations: read.observations, waitedMs: read.waitedMs, ownerReadBefore: liveRead.before, ownerReadAfter: liveRead.after },
      asserted: podAssertion(liveRead) };
    output = receipt(context.request, passed ? "pass" : "refused", observed);
  } catch (error) {
    failed = true;
    outcome = { ...outcome, result: "refused", reason: error instanceof SeamGap ? error.code : "seam-internal", ...errorDetails(error) };
    if (intentSha256 === null) {
      // A failed Pod/startup read made no config change but still gets an intent/outcome pair.
      // An existing intent is never replaced (a repeated phase remains refused).
      try { intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-publish-intent-v1", owner: inputs.owner, machine,
        plan: null, rollback: null, unit: context.pins.linkUnit, routesBefore, measured: measuredPod, residuals: RESIDUALS }); } catch { /* keep original GAP */ }
    }
    throw error;
  } finally {
    if (intentSha256 !== null) {
      try {
        evidence(context, "outcome", { schema: "fabric-v2-bridge-publish-outcome-v1", ...outcome, intentSha256 });
      } catch (error) {
        // Preserve the original operation's GAP if its outcome cannot be persisted either.
        if (!failed) throw error;
      }
    }
  }
  context.io.stdout(output);
  return 0;
};

// ---------------------------------------------------------------- rc-restore-route

/** What publish recorded it wrote (its intent's rollback authority): the only change rc-restore-route may undo. */
const publishedAuthority = (context: Context): RollbackAuthority | null => {
  const file = path.join(context.runDir, "fabric-v2-bridge-publish-intent.json");
  let present: boolean;
  try { present = fs.lstatSync(file, { throwIfNoEntry: false }) !== undefined; } catch { present = true; }
  if (!present) return null;
  let value: Record<string, unknown>;
  try { value = JSON.parse(readOwned(file, context.uid, 8 * 1024 * 1024, { mode: 0o600 }, "publish-intent-unreadable").toString("utf8")) as Record<string, unknown>; }
  catch { throw new SeamGap("publish-intent-unreadable"); }
  if (value.schema !== "fabric-v2-bridge-publish-intent-v1" || value.run !== context.request.run || value.sessionId !== context.request.sessionId ||
    !Object.hasOwn(value, "rollback")) throw new SeamGap("publish-intent-invalid");
  const rollback = value.rollback;
  // A publish that wrote nothing (no change, or refused before the write) gives no rollback authority.
  if (rollback === null) return null;
  if (!object(rollback) || typeof rollback.beforeSha256 !== "string" || !HEX64.test(rollback.beforeSha256) ||
    typeof rollback.afterSha256 !== "string" || !HEX64.test(rollback.afterSha256) || typeof rollback.beforeBytesBase64 !== "string" ||
    !object(rollback.natsEntry) || typeof rollback.beforeHadNats !== "boolean" ||
    (rollback.fileEntry !== null && (!object(rollback.fileEntry) || !Number.isSafeInteger(rollback.fileEntry.index) || !object(rollback.fileEntry.entry)))) {
    throw new SeamGap("publish-intent-invalid");
  }
  const beforeBytes = Buffer.from(rollback.beforeBytesBase64, "base64");
  if (sha256(beforeBytes) !== rollback.beforeSha256) throw new SeamGap("publish-intent-invalid");
  return { beforeSha256: rollback.beforeSha256, afterSha256: rollback.afterSha256, beforeBytes,
    natsEntry: rollback.natsEntry as unknown as SeatEntry, fileEntry: rollback.fileEntry as { index: number; entry: Record<string, unknown> } | null,
    beforeHadNats: rollback.beforeHadNats };
};

/** The v1 Main on this host, read over its owner-only channel, the server checked against the session's owner record. */
const localIdentity = async (context: Context, meshRoot: string) => {
  const sessionId = context.request.sessionId;
  let owner: Record<string, unknown> | undefined;
  try {
    owner = JSON.parse(readOwned(path.join(meshRoot, "main-followups", `${encodeURIComponent(sessionId)}.owner.json`), context.uid, 64 * 1024,
      undefined, "owner-unreadable").toString("utf8")) as Record<string, unknown>;
  } catch { owner = undefined; }
  const verify = async (client: net.Socket): Promise<string | undefined> => {
    if (!owner) return "MAIN_DRAIN_OWNER_UNREADABLE";
    const peer = await socketPeer(client, context.deps.python);
    if (!peer || peer.uid !== context.uid) return "MAIN_DRAIN_SERVER_UNVERIFIED";
    if (owner.pid !== peer.pid || owner.rootId !== `session:${sessionId}` || owner.retired === true ||
      (typeof owner.host === "string" && owner.host !== os.hostname()) || typeof owner.processStartedAt !== "string" ||
      procStartTime(peer.pid, context.deps.procRoot) !== owner.processStartedAt) return "MAIN_DRAIN_SERVER_NOT_OWNER";
    return undefined;
  };
  try {
    const reply = await callMainDrain(mainDrainSocket(meshRoot, sessionId, context.uid), sessionId, "status", null, 30_000, verify);
    if (reply.rootId !== `session:${sessionId}`) return { error: "MAIN_DRAIN_SERVER_NOT_OWNER", owner: owner ?? null };
    return { reply, owner: owner ?? null };
  } catch (error) {
    return { error: object(error) && typeof error.code === "string" ? error.code : "MAIN_DRAIN_UNAVAILABLE", owner: owner ?? null };
  }
};

const restoreRoute = async (context: Context): Promise<number> => {
  const { inputs, sessionId } = context.request;
  const writer = inputs.writer;
  if (keys(inputs) !== "parentRun,parentRunSha256,recoveryRun,writer" || typeof inputs.parentRun !== "string" || !HEX32.test(inputs.parentRun) ||
    typeof inputs.parentRunSha256 !== "string" || !HEX64.test(inputs.parentRunSha256) || typeof inputs.recoveryRun !== "string" ||
    !HEX32.test(inputs.recoveryRun) || !object(writer) || keys(writer) !== "pid,sessionFile,sessionId,singleWriter" ||
    writer.sessionId !== sessionId || writer.singleWriter !== true || !Number.isSafeInteger(writer.pid) || (writer.pid as number) <= 1 ||
    typeof writer.sessionFile !== "string" || !path.isAbsolute(writer.sessionFile)) throw new SeamGap("inputs-invalid");
  if (inputs.parentRun !== context.request.run || inputs.parentRunSha256 !== context.request.runSha256) throw new SeamGap("inputs-invalid");
  const machine = seatMachine(context);
  const first = readLinkConfig(context.pins.linkConfig, readOptions(context));
  // The v1 Main must answer on the same mesh the link mirrors: the seat's mesh root IS the link's file mesh.
  const meshRoot = resolveSeatMeshRoot(context.sealed.journal, sessionId, context.uid);
  let linkMesh: string;
  try { linkMesh = fs.realpathSync(first.config.file.mesh); } catch { throw new SeamGap("link-mesh-unreadable"); }
  if (linkMesh !== meshRoot) throw new SeamGap("seat-mesh-not-link-mesh");
  const pins = seatPins(context, first.config.orgJson);
  const authority = publishedAuthority(context);
  planRestore(first, pins, sessionId, authority);
  const routesBefore = await routeTable(first.config, context.deps);
  if (!routesBefore) throw new SeamGap("link-status-unreadable");
  let intentSha256: string | null = null;
  let outcome: Record<string, unknown> = {};
  let failed = false;
  let output = "";
  try {
    // ONE config-scoped lock across read, plan, intent, check, rename and read-back.
    const locked = await withConfigLock(context.pins.linkConfig, () => {
      const current = readLinkConfig(context.pins.linkConfig, readOptions(context));
      const planned = planRestore(current, pins, sessionId, authority);
      // Keep the durable intent even if applyPlan throws before the lock callback returns.
      intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-rc-restore-route-intent-v1", writer, machine,
        plan: planEvidence(planned), unit: context.pins.linkUnit, routesBefore,
        measured: { meshRoot, interpreter: interpreterIdentity(), startedAt: new Date().toISOString() }, residuals: RESIDUALS });
      // A failed write/read-back may already have swapped the bytes; do not claim it left them unchanged.
      outcome.written = planned.change === "none" ? false : null;
      const after = applyPlan(planned);
      outcome.written = planned.change !== "none";
      return { planned, after };
    }, lockOptions(context));
    const { planned: plan, after: post } = locked;
    let reason: string | null = null;
    let reload: Awaited<ReturnType<typeof reloadLink>> | null = null;
    let others: Awaited<ReturnType<typeof otherRoutesBack>> | null = null;
    if (plan.change !== "none") {
      // The v2 seat's old mirror row on the hub is the one expected leftover once its entry is gone.
      const leftover = `mirror not on allow.nats: session:${sessionId}`;
      reload = await reloadLink(post.config, context.pins.linkUnit, context.sealed.seat, context.deps, clock(context, 120_000),
        problem => problem === leftover);
      if (!reload.statusOk) reason = "link-not-reloaded";
      others = await otherRoutesBack(post.config, routesBefore, `session:${sessionId}`, context.deps, clock(context, OTHER_ROUTES_WAIT_MS));
      if (!reason && others.down.length > 0) reason = "other-routes-down";
    }
    // v1: the relaunched writer is this session's live inbox owner (pid + start time), with the session file A named.
    const local = await localIdentity(context, meshRoot);
    const start = procStartTime(writer.pid as number, context.deps.procRoot) ?? null;
    const v1 = "reply" in local ? local.reply : undefined;
    // b7221c04's status predates `session`. Absence is legacy; a present malformed value is never legacy.
    const legacy = v1 !== undefined && !Object.hasOwn(v1, "session");
    const v1Session = v1 && object(v1.session) ? v1.session : null;
    const reportedIncarnation = v1Session?.ownerIncarnation;
    let incarnation = typeof reportedIncarnation === "string" && reportedIncarnation ? reportedIncarnation : null;
    let incarnationSource = legacy ? "unavailable-v1-b7221c04" : "main-drain-session";
    if (!reason && !v1) reason = "v1-unreachable";
    if (!reason && (v1!.pid !== writer.pid || v1!.rootId !== `session:${sessionId}` || start === null ||
      local.owner?.pid !== writer.pid || local.owner?.rootId !== `session:${sessionId}` ||
      local.owner?.processStartedAt !== start)) reason = "v1-writer-mismatch";
    // Legacy has no server-reported session file: the verified live owner's record must name A's recovered file.
    if (!reason && legacy && local.owner?.sessionFile !== writer.sessionFile) reason = "v1-session-file-mismatch";
    if (!reason && !legacy && v1Session === null) reason = "v1-session-unreadable";
    if (!reason && !legacy && v1Session!.file !== writer.sessionFile) reason = "v1-session-file-mismatch";
    if (!reason && !legacy && incarnation === null) reason = "v1-incarnation-unknown";
    let routed: Awaited<ReturnType<typeof awaitRoutable>> | null = null;
    let v2Live: Array<Record<string, unknown>> = [];
    // Whether the seat is on the post-state's allow.file (its v1 entry, put back or never moved).
    const entries = natsLinkAllowListConfig(post.config).entries;
    const listed = entries.some(entry => entry.side === "file" && entry.kind === "root" && entry.id === `session:${sessionId}`);
    const noPublishNoop = authority === null && plan.change === "none" && listed &&
      !entries.some(entry => entry.side === "nats" && (entry.id === `session:${sessionId}` ||
        (entry.kind === "actor" && entry.rootId === `session:${sessionId}`)));
    if (!reason) {
      routed = await awaitRoutable(post.config, sessionId, context.deps, clock(context, RESTORE_SEAT_ROUTE_WAIT_MS), (gate, value) => {
        const file = object(value.file) ? value.file : undefined;
        if (legacy) {
          // No activationId guess: use the link's FILE incarnation when available, otherwise record its absence.
          const fileIncarnation = file?.ownerIncarnation;
          const gateIncarnation = gate?.ownerIncarnation;
          incarnation = typeof fileIncarnation === "string" && fileIncarnation ? fileIncarnation
            : typeof gateIncarnation === "string" && gateIncarnation ? gateIncarnation : null;
          incarnationSource = incarnation === null ? "unavailable-v1-b7221c04" : "link-file-route";
          return routeHolds(gate, value, { side: "file", fileRemoteHost: null, natsMachine: post.config.nats.machine,
            incarnation, legacySessionId: sessionId });
        }
        // Current protocol: preserve the Main's session file/incarnation proof and the link's NATS mirror check.
        if (listed) return routeHolds(gate, value, { side: "file", fileRemoteHost: null, natsMachine: post.config.nats.machine, incarnation });
        return file !== undefined && (file.remoteHost ?? null) === null && file.live === true && file.ownerIncarnation === incarnation;
      });
      if (!routed.ok) reason = "v1-not-routable";
      // A no-publish FILE no-op still proves v1 above, but has no v2 publication to audit here.
      if (!noPublishNoop) {
        // No v2 left: no live NATS presence for the session on any machine but the link's own (its mirror of v1).
        const records = await natsRecords(post.config, context.deps);
        v2Live = records.filter(record => record.id === `session:${sessionId}` && record.live && record.machine !== post.config.nats.machine)
          .map(record => ({ machine: record.machine, ownerIncarnation: record.ownerIncarnation }));
        if (!reason && v2Live.length > 0) reason = "v2-presence-live";
      }
    }
    const route = `v1@${writer.pid as number}:${start ?? "unknown"}`;
    const observed = { sessionId, route };
    const passed = reason === null;
    outcome = { result: passed ? "pass" : "refused", ...(reason === null ? {} : { reason }),
      observed, written: plan.change !== "none", measured: { configSha256: post.sha256, reload, otherRoutes: others,
        gate: routed?.gate ?? null, receipt: routed?.receipt ?? null, v2Live, writerStart: start, owner: local.owner,
        incarnation, incarnationSource }, asserted: { v1: v1 ? { pid: v1.pid, rootId: v1.rootId, session: v1Session } : null,
        error: "error" in local ? local.error : null } };
    output = receipt(context.request, passed ? "pass" : "refused", observed);
  } catch (error) {
    failed = true;
    outcome = { ...outcome, result: "refused", reason: error instanceof SeamGap ? error.code : "seam-internal", ...errorDetails(error) };
    throw error;
  } finally {
    if (intentSha256 !== null) {
      try {
        evidence(context, "outcome", { schema: "fabric-v2-bridge-rc-restore-route-outcome-v1", ...outcome, intentSha256 });
      } catch (error) {
        // Preserve the original operation's GAP if its outcome cannot be persisted either.
        if (!failed) throw error;
      }
    }
  }
  context.io.stdout(output);
  return 0;
};

// ---------------------------------------------------------------- install

const TOKENS = {
  interpreter: ["__FABRIC_SEAT_BRIDGE", "INTERPRETER_SHA256__"].join("_"),
  linkConfig: ["__FABRIC_SEAT_BRIDGE", "LINK_CONFIG__"].join("_"),
  linkUnit: ["__FABRIC_SEAT_BRIDGE", "LINK_UNIT__"].join("_"),
};

/**
 * Self-install: `#!<this node>` + this bundle, with three values pinned into the bytes (so A's seams.bridge sha256
 * covers them): the interpreter's sha256, the ONE nats-link config file this seam may change, and the link's
 * systemd --user unit. Written once, 0500.
 */
export const install = (argv: string[], io: BridgeIo, self: string): number => {
  const usage = "usage: --install --out <absolute path> --link-config <absolute path> --link-unit <name>.service\n";
  if (argv.length !== 7 || argv[1] !== "--out" || argv[3] !== "--link-config" || argv[5] !== "--link-unit") { io.stderr(usage); return 2; }
  const [out, linkConfig, linkUnit] = [argv[2]!, argv[4]!, argv[6]!];
  if (!path.isAbsolute(out) || !path.isAbsolute(linkConfig) || path.normalize(linkConfig) !== linkConfig || !LINK_UNIT.test(linkUnit)) {
    io.stderr(usage); return 2;
  }
  try { readLinkConfig(linkConfig); } catch (error) {
    io.stderr(`install: ${error instanceof SeamGap ? error.code : "link-config-invalid"}\n`);
    return 2;
  }
  const source = fs.readFileSync(self, "utf8");
  let body = source.startsWith("#!") ? source.slice(source.indexOf("\n") + 1) : source;
  const node = interpreterIdentity();
  if (!node.sha256) { io.stderr("install: interpreter unhashable\n"); return 2; }
  for (const [token, value] of [[TOKENS.interpreter, node.sha256], [TOKENS.linkConfig, linkConfig], [TOKENS.linkUnit, linkUnit]] as const) {
    const quoted = JSON.stringify(token);
    if (body.split(quoted).length !== 2) { io.stderr("install: bundle token missing\n"); return 2; }
    body = body.replace(quoted, JSON.stringify(value));
  }
  const bytes = Buffer.from(`#!${node.execPath}\n${body}`);
  const fd = fs.openSync(out, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o500);
  try { fs.writeSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  io.stdout(`${JSON.stringify({ path: out, sha256: sha256(bytes), owner: OWNER, linkConfig, linkUnit, interpreter: node,
    residual: node.rootOwned ? null : RESIDUALS[0] })}\n`);
  return 0;
};
