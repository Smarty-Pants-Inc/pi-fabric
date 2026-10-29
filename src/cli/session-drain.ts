import fs from "node:fs";
import type net from "node:net";
import os from "node:os";
import path from "node:path";
import { callMainDrain, mainDrainSocket, MainDrainClientError } from "../main-drain/client.js";
import { procStartTime, type MainDrainBindingRequest } from "../main-drain/binding.js";
import { socketPeer } from "../seat-drain/run.js";
import type { MainDrainReply } from "../main-drain/protocol.js";
import { evaluateDrain, scanCompletions } from "../seat-drain/evaluate.js";
import { ENTRY_ID, journalBranch, JournalBranchError } from "../seat-drain/journal-branch.js";
import { inventorySessionPending } from "./session-pending.js";

const USAGE = `Usage:
  pi-fabric session drain <session-uuid | session:uuid> --mesh <meshRoot> --status | --probe | --identity [--since <entry id> [--sealed-bytes <n>]]
  pi-fabric session drain <session-uuid | session:uuid> --mesh <meshRoot> --exit --pod-uid <uid>
      --run <32 hex> --run-sha256 <64 hex> --operation-id <32 hex> --generation <32 hex> [--deadline-s <1-600>]

Owner-only D3 seat drain of THIS host's live Main (smarty-dev#15310), through its local socket
<parent>/.<mesh>.main-drain/, the mesh root's private sibling (mode 0700, the Main's own OS user). It is never
reachable from the mesh.
  --status  observe only;  --probe  the dry-run ingress probes
  --identity  read-only, server verified (owner record + SO_PEERCRED): the Main's session file, current leaf
            and presence owner incarnation (the D3 bridge seam's read); with --since, the session file read
            read-only: its last entry, whether <entry id> is on the current branch, the entries after it, the
            file's sha256, the sha256 of its first <n> bytes and the bytes after them (at most 64 KiB, base64)
  --exit    raw-Pod resume-drain, inside the Pod: bind to the pod UID, drain (bound wait), run the
            same zero and children checks as the host seam, then ask the Main for a clean exit
The host seat's drain and undo run through the pinned seam (fabric-seat-drain), which proves the
seat lock. Nothing is deleted or acknowledged. Output is one JSON line; exit 0 only when the op completed.`;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX32 = /^[0-9a-f]{32}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const DEFAULT_DEADLINE_S = 540;
const POLL_MS = 1_000;
class UsageError extends Error {}

interface CliIo {
  stdout: Pick<NodeJS.WritableStream, "write">;
  stderr: Pick<NodeJS.WritableStream, "write">;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  uid?: number;
  /** Tests only: the /proc root for the server's start time. */
  procRoot?: string;
}

/**
 * The server must be the Main that owns this session's inbox, exactly as the host seam checks it (DRAIN-DESIGN.r1
 * §1.7): its SO_PEERCRED uid is ours, and its pid and /proc start time are those in main-followups/<sid>.owner.json.
 * A stale socket of another Main, or of another session, is refused before any request is written.
 */
const verifyServer = (mesh: string, sessionId: string, uid: number, procRoot?: string) =>
  async (client: net.Socket): Promise<string | undefined> => {
    let owner: Record<string, unknown>;
    try {
      owner = JSON.parse(fs.readFileSync(path.join(mesh, "main-followups", `${encodeURIComponent(sessionId)}.owner.json`), "utf8")) as Record<string, unknown>;
    } catch { return "MAIN_DRAIN_OWNER_UNREADABLE"; }
    const peer = await socketPeer(client);
    if (!peer || peer.uid !== uid) return "MAIN_DRAIN_SERVER_UNVERIFIED";
    if (owner.pid !== peer.pid || owner.rootId !== `session:${sessionId}` || owner.retired === true ||
      (typeof owner.host === "string" && owner.host !== os.hostname()) || typeof owner.processStartedAt !== "string" ||
      procStartTime(peer.pid, procRoot) !== owner.processStartedAt) return "MAIN_DRAIN_SERVER_NOT_OWNER";
    return undefined;
  };

const VALUE_FLAGS = new Set(["--mesh", "--pod-uid", "--run", "--run-sha256", "--operation-id", "--generation", "--deadline-s", "--since", "--sealed-bytes"]);
const MODE_FLAGS = new Set(["--status", "--probe", "--identity", "--exit", "--help"]);

export const runSessionDrainCli = async (
  argv: string[], io: CliIo = { stdout: process.stdout, stderr: process.stderr },
): Promise<number> => {
  try {
    const positionals: string[] = [];
    const values = new Map<string, string>();
    const modes = new Set<string>();
    for (let index = 0; index < argv.length; index++) {
      const arg = argv[index]!;
      if (!arg.startsWith("-")) { positionals.push(arg); continue; }
      if (VALUE_FLAGS.has(arg) && !values.has(arg)) {
        const value = argv[++index];
        if (!value || value.startsWith("-")) throw new UsageError(`${arg} requires a value`);
        values.set(arg, value);
      } else if (MODE_FLAGS.has(arg) && !modes.has(arg)) modes.add(arg);
      else throw new UsageError("Unknown or repeated option");
    }
    if (modes.has("--help")) { io.stdout.write(`${USAGE}\n`); return 0; }
    const sessionId = positionals[0]?.replace(/^session:/, "");
    if (positionals.length !== 1 || !sessionId || !UUID.test(sessionId)) throw new UsageError("Expected exactly one canonical session UUID or session:UUID");
    const mesh = values.get("--mesh");
    if (!mesh || !path.isAbsolute(mesh)) throw new UsageError("--mesh is required and must be absolute; there is no environment default");
    if (modes.size !== 1) throw new UsageError("Choose exactly one of --status, --probe, --identity, --exit");
    const mode = [...modes][0]!;
    const exitValues = ["--pod-uid", "--run", "--run-sha256", "--operation-id", "--generation"];
    if (mode !== "--exit" && (exitValues.some(flag => values.has(flag)) || values.has("--deadline-s"))) throw new UsageError("Binding options belong to --exit");
    const since = values.get("--since");
    if (since !== undefined && (mode !== "--identity" || !ENTRY_ID.test(since))) throw new UsageError("--since <entry id> belongs to --identity");
    const sealedText = values.get("--sealed-bytes");
    if (sealedText !== undefined && (since === undefined || !/^[0-9]{1,12}$/.test(sealedText))) {
      throw new UsageError("--sealed-bytes <n> belongs to --identity --since");
    }
    const sealedBytes = sealedText === undefined ? 0 : Number(sealedText);
    const uid = io.uid ?? process.geteuid?.();
    if (uid === undefined) throw new MainDrainClientError("MAIN_DRAIN_UNSUPPORTED_PLATFORM");
    const socket = mainDrainSocket(mesh, sessionId, uid);
    // The pod caller proof's named residual: a same-UID ptrace of this process is blocked only at YAMA ptrace_scope >= 1.
    let ptraceScope: string | null = null;
    try { ptraceScope = fs.readFileSync("/proc/sys/kernel/yama/ptrace_scope", "utf8").trim(); } catch { ptraceScope = null; }
    const emit = (value: Record<string, unknown>, ok: boolean): number => {
      io.stdout.write(`${JSON.stringify({ format: 1, sessionId, ptraceScope, ...value })}\n`);
      return ok ? 0 : 1;
    };
    if (mode === "--status" || mode === "--probe") {
      const op = mode === "--status" ? "status" : "probe";
      return emit({ op, reply: await callMainDrain(socket, sessionId, op, null) }, true);
    }
    if (mode === "--identity") {
      // Read-only, but its answer is evidence: the server must be this session's inbox owner (as --exit checks).
      const reply = await callMainDrain(socket, sessionId, "status", null, 30_000, verifyServer(mesh, sessionId, uid, io.procRoot));
      if (reply.rootId !== `session:${sessionId}`) throw new MainDrainClientError("MAIN_DRAIN_SERVER_NOT_OWNER");
      const identity = { rootId: reply.rootId, pid: reply.pid, meshRoot: reply.meshRoot, sessionFile: reply.session?.file ?? null,
        leafId: reply.session?.leafId ?? null, ownerIncarnation: reply.session?.ownerIncarnation ?? null };
      // The session file itself, read-only (rule T-FIX3): never the Main's memory for what was appended.
      let journal: Record<string, unknown> | null = null;
      if (since !== undefined) {
        if (identity.sessionFile === null) journal = { error: "JOURNAL_UNKNOWN" };
        else {
          try { journal = { ...journalBranch(identity.sessionFile, since, sealedBytes) }; }
          catch (error) { journal = { error: error instanceof JournalBranchError ? error.code : "JOURNAL_UNREADABLE" }; }
        }
      }
      return emit({ op: "identity", identity, ...(since !== undefined ? { journal } : {}) }, identity.sessionFile !== null &&
        identity.leafId !== null && identity.ownerIncarnation !== null && (since === undefined || (journal !== null && !("error" in journal))));
    }
    const podUid = values.get("--pod-uid") ?? "";
    const run = values.get("--run") ?? "";
    const runSha256 = values.get("--run-sha256") ?? "";
    const operationId = values.get("--operation-id") ?? "";
    const generation = values.get("--generation") ?? "";
    if (!UUID.test(podUid) || !HEX32.test(run) || !HEX64.test(runSha256) || !HEX32.test(operationId) || !HEX32.test(generation)) {
      throw new UsageError("--exit needs --pod-uid <uid> --run <32 hex> --run-sha256 <64 hex> --operation-id <32 hex> --generation <32 hex>");
    }
    const deadlineText = values.get("--deadline-s") ?? String(DEFAULT_DEADLINE_S);
    const deadlineS = Number(deadlineText);
    if (!/^[0-9]{1,3}$/.test(deadlineText) || deadlineS < 1 || deadlineS > 600) throw new UsageError("--deadline-s must be 1-600");
    const binding: MainDrainBindingRequest = { runDir: null, run, runSha256, phase: "resume-drain", operationId, generation, lockFd: null, podUid };
    const now = io.now ?? Date.now;
    const sleep = io.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
    const deadline = now() + deadlineS * 1000;
    const verify = verifyServer(mesh, sessionId, uid, io.procRoot);
    const callBound = async (op: "drain" | "stop-actors" | "undo" | "exit"): Promise<MainDrainReply> => {
      const value = await callMainDrain(socket, sessionId, op, binding, 30_000, verify);
      if (value.rootId !== `session:${sessionId}`) throw new MainDrainClientError("MAIN_DRAIN_SERVER_NOT_OWNER");
      return value;
    };
    // M9: the SAME zero and children checks as the host seam, measured inside the Pod.
    const check = (reply: MainDrainReply) => evaluateDrain({ sessionId, meshRoot: mesh, reply,
      inventory: (() => { try { return inventorySessionPending(mesh, sessionId); } catch { return undefined; } })(),
      completions: scanCompletions(mesh, `session:${sessionId}`) });
    let reply = await callBound("drain");
    let evaluation = check(reply);
    let previous = false;
    // The last stop refusal, reported in the result line (RFL P2: never dropped).
    let stopRefused: string | null = null;
    while (!(evaluation.drained && previous) && !evaluation.haltedWithHeld && now() + POLL_MS < deadline) {
      previous = evaluation.drained;
      await sleep(POLL_MS);
      // r2 N2: the raw Pod's session actors are stopped only as the last act, exactly as on the host.
      if (reply.native.inputsFenced && evaluation.reasons.length > 0 && evaluation.reasons.every(reason => reason.startsWith("children:live-actor:"))) {
        const stop = await callBound("stop-actors");
        stopRefused = stop.stopRefused;
        // A failed, unproven or unknown stop is not retried: the Pod's Main undoes below (#1607 P1).
        if (stop.stopRefused && !["NOT_FENCED", "NOT_DRAINED", "ACTORS_NOT_QUIESCED"].includes(stop.stopRefused)) break;
      }
      reply = await callBound("drain");
      evaluation = check(reply);
    }
    if (!evaluation.drained) {
      // Not drained: the Pod's Main undoes itself here (release, reopen); the seam reports refused.
      const undo = await callBound("undo").catch(() => undefined);
      return emit({ op: "exit", drained: false, exiting: false,
        inputsFenced: !undo || undo.undo?.restartRequired === true ||
          (undo.native.capability && !(undo.native.fenceApi && undo.native.piInputsFenced === false)),
        reasons: evaluation.reasons, observed: evaluation.observed, reply, undo: undo?.undo ?? null, stopRefused }, false);
    }
    const exit = await callBound("exit");
    return emit({ op: "exit", drained: true, exiting: exit.exiting, inputsFenced: evaluation.observed.inputsFenced && exit.native.inputsFenced,
      exitRefused: exit.exitRefused, observed: evaluation.observed, reply: exit, stopRefused }, exit.exiting);
  } catch (error) {
    const usage = error instanceof UsageError;
    const code = error instanceof MainDrainClientError ? error.code : "MAIN_DRAIN_UNAVAILABLE";
    io[usage ? "stderr" : "stdout"].write(`${JSON.stringify({ format: 1, error: usage ? "usage" : code,
      ...(usage ? { message: (error as Error).message, usage: USAGE } : {}) })}\n`);
    return usage ? 2 : 1;
  }
};
