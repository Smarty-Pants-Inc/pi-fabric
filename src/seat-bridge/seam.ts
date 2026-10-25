import fs from "node:fs";
import type net from "node:net";
import os from "node:os";
import path from "node:path";
import { loadBridgeNatsSeatPins, type BridgeNatsSeatPins } from "../mesh/bridge-nats-identity.js";
import { procStartTime } from "../main-drain/binding.js";
import { callMainDrain, mainDrainSocket } from "../main-drain/client.js";
import { ENTRY_ID, entryFacts, SUFFIX_MAX, type AppendedEntry } from "../seat-drain/journal-branch.js";
import { fixedKubectl, kubectl, POD_FABRIC_CLI, readPod, verifyKubeconfig, type KubeAccess, type PodView } from "../seat-drain/kube.js";
import { interpreterIdentity, parseRequest, proveSeatLock, readOwned, readSealedRun, resolveSeatMeshRoot, SeamGap, sha256,
  socketPeer, verifyRunDir, writeOnce, type SealedRun, type SeamRequest } from "../seat-drain/run.js";
import { applyPlan, planPublish, planRestore, readLinkConfig, type SeatEntry, type WriterPlan } from "./allow-writer.js";
import { awaitRoutable, LINK_UNIT, natsRecords, otherRoutesBack, reloadLink, routeTable, type LinkDeps } from "./link.js";

/**
 * fabric-v2's D3 `bridge` seam (smarty-dev#15310): phases `resume`, `publish` and `rc-restore-route`. A runs it as a
 * pinned executable with the drain seam's envelope: `--run-dir RUN --phase P`, the request on stdin, the held seat
 * lock in D3_SEAT_LOCK_FD (the same three-step proof), ONE receipt on stdout with exit 0 and empty stderr. A lock or
 * binding failure prints no receipt and exits 2 with one `GAP: <code> [owner: fabric-v2]` line (UNKNOWN).
 *
 * - resume: read-only. The raw Pod (API: UID pin, sealed target node, its rendered `--session`) and, through
 *   `kubectl exec` of the in-image CLI's server-verified `--identity` read, the resumed Main's session file and
 *   current leaf. Pass only when all equal what A pinned.
 * - publish: the A' link allow-list entry for this seat (the pinned config writer, allow-writer.ts), a restart of the
 *   pinned link unit, then read-backs: the link reloaded with no problem; every other route it carried is live again;
 *   the seat's NATS presence on its own Path B machine and the hub mirror, live, with the SAME owner incarnation the
 *   managed Pod's Main reports from inside that Pod (whose UID the API reads before and after). That chain is why
 *   `incarnation` may be reported as the managed Pod UID: a Main's presence incarnation is a per-process random id.
 * - rc-restore-route: the reverse write (exactly what publish recorded), the restart and the route re-check, no live
 *   v2 presence for the seat, and the relaunched v1 writer is the live owner of the session.
 *
 * Every phase writes `fabric-v2-bridge-<phase>-intent.json` (O_EXCL, before any effect) and `-outcome.json`. A second
 * run of a phase on the same RUN is refused (exit 2), so a phase never runs twice.
 */

const OWNER = "fabric-v2";
const PHASES = new Set(["resume", "publish", "rc-restore-route"]);
const MAX_REQUEST = 64 * 1024;
const DEADLINE_MS = 600_000;
const HEX64 = /^[0-9a-f]{64}$/;
const HEX32 = /^[0-9a-f]{32}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const POD_NAME = /^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$/;
const MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;
export const RESIDUALS = [
  "A6 (A's seat contract): a same-UID writer could alter this seam, its interpreter or the link config between check and use",
  "the link restart briefly interrupts every route the link carries; the seam re-checks each one afterwards",
  "incarnation == pod UID is a proof chain (API pod UID -> exec in that Pod -> server-verified Main -> presence incarnation)",
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
  /** Tests only: the kubectl client and the in-image CLI path (null: unconfirmed). */
  kubectl?: string;
  podCli?: string | null;
  /** Tests only: the org.json seat pins (production: the link config's own orgJson, as `nats-link start`). */
  seatPins?: () => BridgeNatsSeatPins;
  /** Tests only: see the error behind a fixed GAP code. */
  onError?: (error: unknown) => void;
}
interface Context {
  runDir: string; request: SeamRequest; sealed: SealedRun; lockFd: number; uid: number; io: BridgeIo; deps: BridgeDeps; pins: BridgePins;
  now: () => number; sleep: (ms: number) => Promise<void>; deadline: number; started: number; requestSha256: string;
}

const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const keys = (value: Record<string, unknown>): string => Object.keys(value).sort().join(",");
const receipt = (request: SeamRequest, result: "pass" | "refused", observed: Record<string, unknown>): string =>
  JSON.stringify({ schema: "d3-seam-receipt-v1", run: request.run, phase: request.phase, owner: OWNER, result, observed });
const evidence = (context: Context, name: string, value: Record<string, unknown>): string =>
  writeOnce(context.runDir, `fabric-v2-bridge-${context.request.phase}-${name}.json`, Buffer.from(`${JSON.stringify({
    ...value, owner: OWNER, run: context.request.run, phase: context.request.phase, sessionId: context.request.sessionId,
    requestSha256: context.requestSha256, elapsedMs: Math.round(context.now() - context.started) }, null, 1)}\n`));
const clock = (context: Context, budgetMs = Number.POSITIVE_INFINITY) => ({
  now: context.now, sleep: context.sleep, deadline: Math.min(context.deadline, context.now() + budgetMs) });

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
    const context: Context = { runDir, request, sealed, lockFd, uid, io, deps, pins, now, sleep, deadline, started, requestSha256: sha256(stdin) };
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
  rootId: string; pid: number; meshRoot: string; sessionFile: string | null; leafId: string | null; ownerIncarnation: string | null;
}

/**
 * The pinned Pod (API: UID, the sealed target node, pi running), then the in-image CLI's `--identity` read inside it
 * (the CLI verifies its server against the session's owner record and SO_PEERCRED), then the Pod again: same UID,
 * still running. Read-only throughout.
 */
const podIdentity = async (context: Context, access: KubeAccess, pin: { name: string; uid: string },
  seal?: { leafId: string; journalBytes: number }) => {
  const remaining = (): number => Math.max(0, context.deadline - context.now());
  const before = await readPod(access, pin.name, Math.min(30_000, remaining()));
  if (before && before.uid !== pin.uid) throw new SeamGap("pod-uid-mismatch");
  if (before && before.nodeName !== targetNode(context)) throw new SeamGap("pod-node-mismatch");
  const cli = context.deps.podCli !== undefined ? context.deps.podCli : POD_FABRIC_CLI.confirmed ? POD_FABRIC_CLI.path : null;
  let reason: string | null = null;
  let identity: PodIdentity | null = null;
  let journal: JournalRead | null = null;
  let line: string | null = null;
  let execCode: number | null = null;
  if (!before) reason = "pod-unreadable";
  else if (!cli) reason = "pod-cli-unconfirmed";
  else if (!before.piRunning) reason = "pod-not-running";
  else if (!before.meshRoot) reason = "pod-mesh-root-unknown";
  else if ((await kubectl(access, ["-n", access.namespace, "exec", pin.name, "-c", "pi", "--", "test", "-x", cli],
    Math.min(30_000, remaining()))).code !== 0) reason = "pod-cli-missing";
  else {
    const result = await kubectl(access, ["-n", access.namespace, "exec", pin.name, "-c", "pi", "--", cli, "session", "drain",
      context.request.sessionId, "--mesh", before.meshRoot, "--identity",
      ...(seal ? ["--since", seal.leafId, "--sealed-bytes", String(seal.journalBytes)] : [])], Math.min(120_000, remaining()));
    execCode = result.code;
    line = (result.stdout.trim().split("\n").at(-1) ?? "").slice(0, 1024 * 1024);
    let parsed: Record<string, unknown> | undefined;
    try { parsed = JSON.parse(line) as Record<string, unknown>; } catch { parsed = undefined; }
    const value = parsed && object(parsed.identity) ? parsed.identity : undefined;
    if (result.code !== 0 || !parsed || parsed.format !== 1 || parsed.op !== "identity" || parsed.sessionId !== context.request.sessionId || !value ||
      value.rootId !== `session:${context.request.sessionId}` || !Number.isSafeInteger(value.pid) || typeof value.meshRoot !== "string") {
      reason = "pod-identity-unreadable";
    } else {
      const text = (key: string): string | null => typeof value[key] === "string" && (value[key] as string).length > 0 &&
        (value[key] as string).length <= 4096 ? value[key] as string : null;
      identity = { rootId: value.rootId as string, pid: value.pid as number, meshRoot: value.meshRoot as string,
        sessionFile: text("sessionFile"), leafId: text("leafId"), ownerIncarnation: text("ownerIncarnation") };
      if (seal) {
        journal = journalRead(parsed!.journal);
        if (!journal) reason = "pod-journal-unreadable";
      }
    }
  }
  const after = before ? await readPod(access, pin.name, Math.min(30_000, Math.max(1_000, remaining()))) : undefined;
  if (!reason && (!after || after.uid !== pin.uid)) reason = "pod-uid-changed";
  if (!reason && !after!.piRunning) reason = "pod-not-running";
  return { before: before ?? null, after: after ?? null, identity, journal, reason, line, execCode, cli };
};

interface JournalRead {
  leafId: string; entries: number; sealOnBranch: boolean; truncated: boolean;
  appendedSinceSeal: AppendedEntry[] | null;
  journalSha256: string; journalBytes: number; sealedSha256: string | null; suffixBytes: number; suffixBase64: string | null;
}

/** The in-Pod CLI's journal read, shape-checked (closed keys per appended entry). */
const journalRead = (value: unknown): JournalRead | null => {
  if (!object(value) || typeof value.leafId !== "string" || !ENTRY_ID.test(value.leafId) || !Number.isSafeInteger(value.entries) ||
    typeof value.sealOnBranch !== "boolean" || typeof value.truncated !== "boolean" ||
    typeof value.journalSha256 !== "string" || !HEX64.test(value.journalSha256) || !Number.isSafeInteger(value.journalBytes) ||
    !(value.sealedSha256 === null || (typeof value.sealedSha256 === "string" && HEX64.test(value.sealedSha256))) ||
    !Number.isSafeInteger(value.suffixBytes) || (value.suffixBytes as number) < 0 ||
    !(value.suffixBase64 === null || (typeof value.suffixBase64 === "string" && value.suffixBase64.length <= 4 * Math.ceil(SUFFIX_MAX / 3) &&
      /^[A-Za-z0-9+/]*={0,2}$/.test(value.suffixBase64)))) return null;
  const appended = value.appendedSinceSeal;
  if (value.sealOnBranch ? !Array.isArray(appended) : appended !== null) return null;
  const entries = (appended ?? []) as unknown[];
  if (!entries.every(entry => object(entry) && keys(entry) === "id,parentId,thinkingLevel,type" && typeof entry.id === "string" &&
    ENTRY_ID.test(entry.id) && (entry.parentId === null || (typeof entry.parentId === "string" && ENTRY_ID.test(entry.parentId))) &&
    typeof entry.type === "string" && entry.type.length <= 64 && (entry.thinkingLevel === null ||
      (entry.type === "thinking_level_change" && typeof entry.thinkingLevel === "string" && entry.thinkingLevel.length <= 64)))) return null;
  return { leafId: value.leafId, entries: value.entries as number, sealOnBranch: value.sealOnBranch, truncated: value.truncated,
    appendedSinceSeal: appended === null ? null : (entries as AppendedEntry[])
      .map(entry => ({ id: entry.id, parentId: entry.parentId, type: entry.type, thinkingLevel: entry.thinkingLevel })),
    journalSha256: value.journalSha256, journalBytes: value.journalBytes as number, sealedSha256: value.sealedSha256 as string | null,
    suffixBytes: value.suffixBytes as number, suffixBase64: value.suffixBase64 as string | null };
};

/**
 * A's final seal, from RUN itself: RUN/receipts/final-seal.json, whose sha256 is the journal's ONE `final-seal`
 * record, for this run and the sealed source journal, and equal to the request's finalSeal.
 */
const finalSealOf = (context: Context, requested: Record<string, unknown>): { journalSha256: string; journalBytes: number; leafId: string } => {
  const bytes = readOwned(path.join(context.runDir, "receipts", "final-seal.json"), context.uid, 64 * 1024, undefined, "final-seal-unreadable");
  const records = readOwned(path.join(context.runDir, "seat-journal.jsonl"), context.uid, 64 * 1024 * 1024, undefined, "seat-journal-unreadable")
    .toString("utf8").split("\n").filter(line => line.trim()).map(line => { try { return JSON.parse(line) as Record<string, unknown>; } catch { return {}; } })
    .filter(record => record.event === "final-seal");
  if (records.length !== 1 || records[0]!.sha256 !== sha256(bytes)) throw new SeamGap("final-seal-unjournaled");
  let seal: Record<string, unknown>;
  try { seal = JSON.parse(bytes.toString("utf8")) as Record<string, unknown>; } catch { throw new SeamGap("final-seal-invalid"); }
  if (!object(seal) || keys(seal) !== "journal,journalBytes,journalSha256,leafId,run,schema" || seal.schema !== "d3-seat-final-seal-v1" ||
    seal.run !== context.request.run || seal.journal !== context.sealed.journal || typeof seal.journalSha256 !== "string" ||
    !HEX64.test(seal.journalSha256) || !Number.isSafeInteger(seal.journalBytes) || (seal.journalBytes as number) <= 0 ||
    typeof seal.leafId !== "string" || !ENTRY_ID.test(seal.leafId)) throw new SeamGap("final-seal-invalid");
  if (seal.journalSha256 !== requested.journalSha256 || seal.journalBytes !== requested.journalBytes || seal.leafId !== requested.leafId) {
    throw new SeamGap("final-seal-mismatch");
  }
  return { journalSha256: seal.journalSha256, journalBytes: seal.journalBytes as number, leafId: seal.leafId };
};

const sessionArg = (pod: PodView | null): string | null => {
  const args = pod?.piArgs;
  const index = args ? args.indexOf("--session") : -1;
  return args && index >= 0 && typeof args[index + 1] === "string" ? args[index + 1]! : null;
};

// ---------------------------------------------------------------- resume

const resume = async (context: Context): Promise<number> => {
  const { inputs, sessionId } = context.request;
  if (keys(inputs) !== "finalSeal,pod" || !object(inputs.finalSeal) || keys(inputs.finalSeal) !== "journalBytes,journalSha256,leafId") {
    throw new SeamGap("inputs-invalid");
  }
  const pin = podPin(inputs.pod);
  const seal = finalSealOf(context, inputs.finalSeal);
  const access = kubeAccess(context);
  await verifyKubeconfig(access, Math.min(30_000, context.deadline - context.now()));
  const intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-resume-intent-v1", pod: pin, finalSeal: inputs.finalSeal,
    measured: { kubectl: access.kubectl, server: access.server, interpreter: interpreterIdentity(), startedAt: new Date().toISOString() },
    residuals: RESIDUALS });
  const read = await podIdentity(context, access, pin, seal);
  const rendered = sessionArg(read.before);
  const journal = read.journal;
  const gap = (code: string): never => {
    evidence(context, "outcome", { schema: "fabric-v2-bridge-resume-outcome-v1", result: "unknown", gap: code, intentSha256,
      measured: { before: read.before, after: read.after, renderedSession: rendered, execCode: read.execCode, journal, finalSeal: seal },
      asserted: { identity: read.identity, podLine: read.line } });
    throw new SeamGap(code);
  };
  let reason = read.reason;
  let appended: AppendedEntry[] = [];
  if (!reason && journal) {
    // The journal itself must still begin with exactly the sealed bytes, and grew by at most 64 KiB: no receipt otherwise.
    if (journal.sealedSha256 !== seal.journalSha256) gap("journal-prefix-mismatch");
    if (journal.suffixBytes > SUFFIX_MAX || journal.suffixBase64 === null) gap("journal-suffix-too-large");
    // The sealed leaf must be the current leaf or an ancestor: otherwise the branch diverged after the seal.
    if (!journal.sealOnBranch) gap("branch-diverged");
    // The seam's own measure of the suffix: its bytes are the size the CLI reports, and its entries ARE the appended
    // branch entries, in order, the first one's parent the sealed leaf (so the CLI's branch read is checked here).
    const suffix = Buffer.from(journal.suffixBase64!, "base64");
    if (suffix.length !== journal.suffixBytes || journal.journalBytes !== seal.journalBytes + suffix.length) gap("journal-suffix-inconsistent");
    try {
      appended = suffix.toString("utf8").split("\n").filter(line => line.trim()).map(line => entryFacts(line)!)
        .filter((entry): entry is AppendedEntry => entry !== null);
    } catch { gap("journal-suffix-invalid"); }
    const chained = appended.every((entry, index) => entry.parentId === (index === 0 ? seal.leafId : appended[index - 1]!.id));
    if (!chained || !sameEntries(appended, journal.appendedSinceSeal ?? []) ||
      journal.leafId !== (appended.at(-1)?.id ?? seal.leafId)) gap("journal-suffix-not-the-branch");
  }
  if (!reason && !rendered) reason = "pod-session-arg-unknown";
  if (!reason && read.identity!.sessionFile !== rendered) reason = "journal-mismatch";
  // Rule T-FIX3: the TRUE current leaf, read from the session file; the Main's own leaf must agree with it.
  if (!reason && read.identity!.leafId !== journal!.leafId) reason = "leaf-memory-file-mismatch";
  const observed = { sessionId, journal: read.identity?.sessionFile ?? "", leafId: journal?.leafId ?? "", podUid: pin.uid,
    appendedSinceSeal: appended, journalSha256: journal?.journalSha256 ?? "", suffixBase64: journal?.suffixBase64 ?? "" };
  const passed = reason === null;
  evidence(context, "outcome", { schema: "fabric-v2-bridge-resume-outcome-v1", result: passed ? "pass" : "refused", reason, intentSha256,
    observed, measured: { before: read.before, after: read.after, renderedSession: rendered, execCode: read.execCode, journal,
      finalSeal: seal }, asserted: { identity: read.identity, podLine: read.line } });
  context.io.stdout(receipt(context.request, passed ? "pass" : "refused", observed));
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

const seatMachine = (context: Context): string => {
  const machine = context.sealed.run.instance;
  if (typeof machine !== "string" || !MACHINE.test(machine)) throw new SeamGap("seat-machine-invalid");
  return machine;
};

/**
 * The route the receipt shows, judged here (not by its overall verdict, which also counts an old, non-live NATS key the
 * link left behind): the identity is on `side`; ONE live file-mesh record (native, or this link's mirror) and exactly
 * ONE live NATS record, on `natsMachine`, both with `incarnation`.
 */
const routedTo = (value: Record<string, unknown>, want: { side: "file" | "nats"; fileRemoteHost: string | null; natsMachine: string;
  incarnation: string }): boolean => {
  const file = object(value.file) ? value.file : undefined;
  const nats = object(value.nats) && Array.isArray(value.nats.records) ? (value.nats.records as unknown[]).filter(object) : [];
  const live = nats.filter(record => record.live === true);
  return value.side === want.side && file !== undefined && (file.remoteHost ?? null) === want.fileRemoteHost && file.live === true &&
    file.ownerIncarnation === want.incarnation && live.length === 1 && live[0]!.machine === want.natsMachine &&
    live[0]!.ownerIncarnation === want.incarnation;
};

const planEvidence = (plan: WriterPlan) => ({ change: plan.change, beforeSha256: plan.before.sha256, afterSha256: plan.afterSha256,
  natsEntry: plan.natsEntry, fileEntry: plan.fileEntry, beforeBytes: plan.before.bytes.toString("utf8"),
  afterBytes: plan.after?.toString("utf8") ?? null, file: plan.before.file });

const publish = async (context: Context): Promise<number> => {
  const { inputs, sessionId } = context.request;
  if (keys(inputs) !== "owner" || !object(inputs.owner) || keys(inputs.owner) !== "claim,pod,sandbox,template") throw new SeamGap("inputs-invalid");
  const pin = podPin(inputs.owner.pod);
  const machine = seatMachine(context);
  const access = kubeAccess(context);
  await verifyKubeconfig(access, Math.min(30_000, context.deadline - context.now()));
  const config = readLinkConfig(context.pins.linkConfig);
  const pins = seatPins(context, config.config.orgJson);
  const plan = planPublish(config, pins, { sessionId, machine });
  const routesBefore = await routeTable(config.config, context.deps);
  if (!routesBefore) throw new SeamGap("link-status-unreadable");
  // Durable BEFORE any effect: the exact write, so rc-restore-route can undo exactly this and nothing else.
  const intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-publish-intent-v1", owner: inputs.owner, machine,
    plan: planEvidence(plan), unit: context.pins.linkUnit, routesBefore,
    measured: { kubectl: access.kubectl, server: access.server, interpreter: interpreterIdentity(), startedAt: new Date().toISOString() },
    residuals: RESIDUALS });
  const read = await podIdentity(context, access, pin);
  let reason = read.reason;
  if (!reason && !read.identity!.ownerIncarnation) reason = "pod-incarnation-unknown";
  let written = false;
  let reload: Awaited<ReturnType<typeof reloadLink>> | null = null;
  let others: Awaited<ReturnType<typeof otherRoutesBack>> | null = null;
  let routed: Awaited<ReturnType<typeof awaitRoutable>> | null = null;
  // The write happens only for a verified managed Pod whose Main reports its incarnation.
  if (!reason) {
    applyPlan(plan);
    written = plan.change !== "none";
    if (written) {
      reload = await reloadLink(config.config, context.pins.linkUnit, context.sealed.seat, context.deps, clock(context, 120_000));
      if (!reload.statusOk) reason = "link-not-reloaded";
    }
  }
  if (!reason) {
    const incarnation = read.identity!.ownerIncarnation;
    routed = await awaitRoutable(config.config, sessionId, context.deps, clock(context, 120_000), value =>
      routedTo(value, { side: "nats", fileRemoteHost: config.config.link, natsMachine: machine, incarnation: incarnation! }));
    if (!routed.ok) reason = "seat-not-routable";
  }
  if (written) {
    // Every other route the link carried live before the restart must be live again, whatever this seat's result.
    others = await otherRoutesBack(config.config, routesBefore, `session:${sessionId}`, context.deps, clock(context, 120_000));
    if (!reason && others.down.length > 0) reason = "other-routes-down";
  }
  const final = await readPod(access, pin.name, Math.min(30_000, Math.max(1_000, context.deadline - context.now())));
  if (!reason && (!final || final.uid !== pin.uid || !final.piRunning)) reason = "pod-uid-changed";
  const route = `v2@${pin.uid}:${(plan.afterSha256 ?? plan.before.sha256).slice(0, 12)}`;
  const observed = { sessionId, incarnation: pin.uid, route };
  const passed = reason === null;
  evidence(context, "outcome", { schema: "fabric-v2-bridge-publish-outcome-v1", result: passed ? "pass" : "refused", reason, intentSha256,
    observed, written, measured: { before: read.before, after: read.after, final: final ?? null, reload, otherRoutes: others,
      receipt: routed?.receipt ?? null }, asserted: { identity: read.identity, podLine: read.line } });
  context.io.stdout(receipt(context.request, passed ? "pass" : "refused", observed));
  return 0;
};

// ---------------------------------------------------------------- rc-restore-route

/** What publish recorded it would write (its intent): the only change rc-restore-route may undo. */
const publishedWrite = (context: Context): { natsEntry: SeatEntry | null; fileEntry: { index: number; entry: Record<string, unknown> } | null } => {
  const file = path.join(context.runDir, "fabric-v2-bridge-publish-intent.json");
  let present: boolean;
  try { present = fs.lstatSync(file, { throwIfNoEntry: false }) !== undefined; } catch { present = true; }
  if (!present) return { natsEntry: null, fileEntry: null };
  let value: Record<string, unknown>;
  try { value = JSON.parse(readOwned(file, context.uid, 4 * 1024 * 1024, { mode: 0o600 }, "publish-intent-unreadable").toString("utf8")) as Record<string, unknown>; }
  catch { throw new SeamGap("publish-intent-unreadable"); }
  const plan = object(value.plan) ? value.plan : undefined;
  if (value.schema !== "fabric-v2-bridge-publish-intent-v1" || value.run !== context.request.run || value.sessionId !== context.request.sessionId ||
    !plan || !object(plan.natsEntry) || (plan.fileEntry !== null && !object(plan.fileEntry))) throw new SeamGap("publish-intent-invalid");
  const fileEntry = plan.fileEntry as { index?: unknown; entry?: unknown } | null;
  if (fileEntry && (!Number.isSafeInteger(fileEntry.index) || !object(fileEntry.entry))) throw new SeamGap("publish-intent-invalid");
  return { natsEntry: plan.natsEntry as unknown as SeatEntry,
    fileEntry: fileEntry ? { index: fileEntry.index as number, entry: fileEntry.entry as Record<string, unknown> } : null };
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
  const config = readLinkConfig(context.pins.linkConfig);
  // The v1 Main must answer on the same mesh the link mirrors: the seat's mesh root IS the link's file mesh.
  const meshRoot = resolveSeatMeshRoot(context.sealed.journal, sessionId, context.uid);
  let linkMesh: string;
  try { linkMesh = fs.realpathSync(config.config.file.mesh); } catch { throw new SeamGap("link-mesh-unreadable"); }
  if (linkMesh !== meshRoot) throw new SeamGap("seat-mesh-not-link-mesh");
  const pins = seatPins(context, config.config.orgJson);
  const published = publishedWrite(context);
  const plan = planRestore(config, pins, { sessionId, ...published });
  const routesBefore = await routeTable(config.config, context.deps);
  if (!routesBefore) throw new SeamGap("link-status-unreadable");
  const machine = seatMachine(context);
  const intentSha256 = evidence(context, "intent", { schema: "fabric-v2-bridge-rc-restore-route-intent-v1", writer, machine,
    plan: planEvidence(plan), unit: context.pins.linkUnit, routesBefore,
    measured: { meshRoot, interpreter: interpreterIdentity(), startedAt: new Date().toISOString() }, residuals: RESIDUALS });
  let reason: string | null = null;
  applyPlan(plan);
  let reload: Awaited<ReturnType<typeof reloadLink>> | null = null;
  let others: Awaited<ReturnType<typeof otherRoutesBack>> | null = null;
  if (plan.change !== "none") {
    // The v2 seat's old mirror row on the hub is the one expected leftover once its entry is gone.
    const leftover = `mirror not on allow.nats: session:${sessionId}`;
    reload = await reloadLink(config.config, context.pins.linkUnit, context.sealed.seat, context.deps, clock(context, 120_000),
      problem => problem === leftover);
    if (!reload.statusOk) reason = "link-not-reloaded";
    others = await otherRoutesBack(config.config, routesBefore, `session:${sessionId}`, context.deps, clock(context, 120_000));
    if (!reason && others.down.length > 0) reason = "other-routes-down";
  }
  // v1: the relaunched writer is this session's live inbox owner (pid + start time), with the session file A named.
  const local = await localIdentity(context, meshRoot);
  const start = procStartTime(writer.pid as number, context.deps.procRoot) ?? null;
  const v1 = "reply" in local ? local.reply : undefined;
  if (!reason && !v1) reason = "v1-unreachable";
  if (!reason && (v1!.pid !== writer.pid || start === null || local.owner?.processStartedAt !== start)) reason = "v1-writer-mismatch";
  if (!reason && v1!.session.file !== writer.sessionFile) reason = "v1-session-file-mismatch";
  if (!reason && !v1!.session.ownerIncarnation) reason = "v1-incarnation-unknown";
  let routed: Awaited<ReturnType<typeof awaitRoutable>> | null = null;
  let v2Live: Array<Record<string, unknown>> = [];
  if (!reason) {
    const incarnation = v1!.session.ownerIncarnation!;
    routed = await awaitRoutable(config.config, sessionId, context.deps, clock(context, 120_000), value => {
      // With the seat's file entry restored, the link mirrors v1 into NATS under its own machine: that must route too.
      if (published.fileEntry) return routedTo(value, { side: "file", fileRemoteHost: null, natsMachine: config.config.nats.machine, incarnation });
      const file = object(value.file) ? value.file : undefined;
      return file !== undefined && (file.remoteHost ?? null) === null && file.live === true && file.ownerIncarnation === incarnation;
    });
    if (!routed.ok) reason = "v1-not-routable";
    // No v2 left: no live NATS presence for the session on any machine but the link's own (its mirror of v1).
    const records = await natsRecords(config.config, context.deps);
    v2Live = records.filter(record => record.id === `session:${sessionId}` && record.live && record.machine !== config.config.nats.machine)
      .map(record => ({ machine: record.machine, ownerIncarnation: record.ownerIncarnation }));
    if (!reason && v2Live.length > 0) reason = "v2-presence-live";
  }
  const route = `v1@${writer.pid as number}:${start ?? "unknown"}`;
  const observed = { sessionId, route };
  const passed = reason === null;
  evidence(context, "outcome", { schema: "fabric-v2-bridge-rc-restore-route-outcome-v1", result: passed ? "pass" : "refused", reason,
    intentSha256, observed, written: plan.change !== "none", measured: { reload, otherRoutes: others, receipt: routed?.receipt ?? null, v2Live,
      writerStart: start, owner: local.owner }, asserted: { v1: v1 ? { pid: v1.pid, rootId: v1.rootId, session: v1.session } : null,
      error: "error" in local ? local.error : null } });
  context.io.stdout(receipt(context.request, passed ? "pass" : "refused", observed));
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
