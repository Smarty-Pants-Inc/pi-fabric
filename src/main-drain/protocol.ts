import { createHash } from "node:crypto";
import path from "node:path";
import { parseBindingRequest, type MainDrainBindingRequest } from "./binding.js";
import type { NativeInputObservation } from "./native-port.js";

/**
 * D3 seat drain (smarty-dev#15310): the local, owner-only wire protocol between a drain client
 * (`pi-fabric session drain`, `fabric-seat-drain`) and this host's live Main.
 *
 * Transport: one Unix-domain stream socket per Main session at `<meshRoot>/main-drain/<24 hex>.sock`.
 * The directory is mode 0700 and owned by the Main's OS user, so the kernel lets only that user
 * (or root) connect. It is never reachable from the mesh, NATS or a bridge. One request line, one
 * reply line, then close. Mutating ops also carry a run binding proved against the seat lock (binding.ts).
 */
export const MAIN_DRAIN_PROTOCOL = 1;
export const MAIN_DRAIN_DIRECTORY = "main-drain";
export const MAIN_DRAIN_MAX_REQUEST_BYTES = 4 * 1024;
export const MAIN_DRAIN_MAX_REPLY_BYTES = 1024 * 1024;
/** Linux sun_path holds 107 bytes plus the terminator. */
export const MAIN_DRAIN_MAX_SOCKET_PATH = 107;
/**
 * status, probe: read-only, no binding.
 * drain: bind, close the gates, take the native fence once everything else is drained.
 * stop-actors: the LAST act (challenge r2 N2): with the fence held and all else 0, stop every quiesced session actor.
 * undo: the same (run, operationId, generation) releases the fence and reopens the gates (B1, N6 ii).
 * exit: a pod binding only; a fully drained raw-Pod Main exits cleanly.
 */
export const MAIN_DRAIN_OPS = ["status", "probe", "drain", "stop-actors", "undo", "exit"] as const;
export type MainDrainOp = typeof MAIN_DRAIN_OPS[number];
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const mainDrainSocketName = (sessionId: string): string =>
  `${createHash("sha256").update(`pi-fabric-main-drain-v1\0${sessionId}`).digest("hex").slice(0, 24)}.sock`;
export const mainDrainSocketPath = (meshRoot: string, sessionId: string): string =>
  path.join(meshRoot, MAIN_DRAIN_DIRECTORY, mainDrainSocketName(sessionId));

export interface MainDrainRequest { v: 1; op: MainDrainOp; sessionId: string; binding: MainDrainBindingRequest | null }

/** Exact keys, canonical lowercase session UUID, known op; a binding exactly when the op mutates. */
export const parseMainDrainRequest = (line: string): MainDrainRequest | undefined => {
  let value: unknown;
  try { value = JSON.parse(line); } catch { return undefined; }
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const request = value as Record<string, unknown>;
  if (Object.keys(request).sort().join(",") !== "binding,op,sessionId,v" || request.v !== MAIN_DRAIN_PROTOCOL ||
    typeof request.sessionId !== "string" || !UUID.test(request.sessionId) ||
    !(MAIN_DRAIN_OPS as readonly unknown[]).includes(request.op)) return undefined;
  const op = request.op as MainDrainOp;
  const mutating = op === "drain" || op === "stop-actors" || op === "undo" || op === "exit";
  const binding = request.binding === null ? null : parseBindingRequest(request.binding);
  if (binding === undefined || mutating !== (binding !== null)) return undefined;
  return { v: 1, op, sessionId: request.sessionId, binding };
};

/** One row per ingress path into this Main (challenge r1 M7): its gate and a dry-run denial probe. */
export interface MainDrainIngressProbe {
  path: string;
  mechanism: "fabric-gate" | "native-fence" | "both" | "hold";
  /** Would a NEW input on this path be admitted now? null: unknown (counts as admitted). */
  admits: boolean | null;
}

export interface MainDrainActorRow {
  id: string;
  name: string;
  status: string;
  residency: "session" | "durable" | "unknown";
  /** Why it is not quiesced (empty: safe to stop); null when unknown. */
  quiescence: string[] | null;
  /** This host's own launch record of the actor's last worker; null: no run recorded; "unknown": no record here. */
  worker: { runId: string; pid: number; startTime: string | null } | null | "unknown";
  /** challenge r1 M5: what happens to it. */
  disposition: string;
}

/** One reply line. Private evidence: ids, counts and codes, never message text. Everything here is ASSERTED by the Main. */
export interface MainDrainReply {
  v: 1;
  ok: boolean;
  op: MainDrainOp | null;
  error: string | null;
  sessionId: string;
  rootId: string;
  meshRoot: string;
  pid: number;
  binding: { kind: "host" | "pod"; run: string; operationId: string; generation: string; phase: string; podUid: string | null;
    boundAt: number } | null;
  inbox: { closed: boolean; closedAt: number | null; held: number | null; handed: number | null; heldIds: string[] | null;
    handedIds: string[] | null; journalOpen: boolean | null; halted: boolean | null; reloading: boolean | null };
  native: NativeInputObservation;
  probes: MainDrainIngressProbe[];
  actors: { known: boolean; owned: MainDrainActorRow[] };
  /** running: non-terminal runs; workers: every run with this host's own launch record, for the seam's /proc proof. */
  agents: { known: boolean; running: Array<{ id: string; status: string }>;
    workers: Array<{ id: string; status: string; pid: number; startTime: string | null }> };
  /** The in-memory completion inbox (no residency): unconfirmed completions; null when unknown. */
  completions: { inboxPending: number | null };
  /** N1: the drain's own children admitted through the gate; N8: refused inputs (no text). Both for the bound run. */
  admitted: Array<{ at: number; path: string; fromId: string | null; id: string | null }>;
  ledger: Array<{ at: number; path: string; fromId: string | null; id: string | null; textSha256: string | null }>;
  /** N2: the session actors this operation stopped, each with its recorded public spec, and whether a stop was refused. */
  stopped: Array<{ id: string; spec: Record<string, unknown>; stoppedAt: number }>;
  stopRefused: string | null;
  /** undo only. restartRequired: the fence could not be lifted through this process's handle; actorsRestored:
   * every actor this operation stopped is active again (stage 1 cannot recreate: false when any was stopped). */
  undo: { fenceReleased: boolean; inboxReopened: boolean; restartRequired: boolean; actorsRestored: boolean } | null;
  /** exit only: the Main accepted a clean exit and shuts down right after this reply. */
  exiting: boolean;
  /** exit only: why the Main refused to exit (fixed codes). */
  exitRefused: string[];
}
