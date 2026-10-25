import { createHash } from "node:crypto";
import path from "node:path";
import type { NativeInputObservation } from "./native-port.js";

/**
 * D3 seat drain (smarty-dev#15310): the local, owner-only wire protocol between a drain client
 * (`pi-fabric session drain`, `bin/fabric-seat-drain`) and this host's live Main.
 *
 * Transport: one Unix-domain stream socket per Main session at
 * `<meshRoot>/main-drain/<24 hex>.sock`. The directory is mode 0700 and owned by the Main's OS
 * user, so the kernel lets only that user (or root) connect. The socket is never reachable
 * from the mesh, NATS or a bridge. One request line, one reply line, then close.
 * `bin/fabric-seat-drain` re-implements these constants; keep the two in step.
 */
export const MAIN_DRAIN_PROTOCOL = 1;
export const MAIN_DRAIN_DIRECTORY = "main-drain";
export const MAIN_DRAIN_MAX_REQUEST_BYTES = 4 * 1024;
export const MAIN_DRAIN_MAX_REPLY_BYTES = 1024 * 1024;
/** Linux sun_path holds 107 bytes plus the terminator. */
export const MAIN_DRAIN_MAX_SOCKET_PATH = 107;
export const MAIN_DRAIN_OPS = ["status", "drain", "exit", "reopen"] as const;
export type MainDrainOp = typeof MAIN_DRAIN_OPS[number];
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const mainDrainSocketName = (sessionId: string): string =>
  `${createHash("sha256").update(`pi-fabric-main-drain-v1\0${sessionId}`).digest("hex").slice(0, 24)}.sock`;
export const mainDrainSocketPath = (meshRoot: string, sessionId: string): string =>
  path.join(meshRoot, MAIN_DRAIN_DIRECTORY, mainDrainSocketName(sessionId));

export interface MainDrainRequest { v: 1; op: MainDrainOp; sessionId: string }

/** Exact keys, canonical lowercase session UUID, known op. Anything else is refused. */
export const parseMainDrainRequest = (line: string): MainDrainRequest | undefined => {
  let value: unknown;
  try { value = JSON.parse(line); } catch { return undefined; }
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const keys = Object.keys(value).sort();
  const request = value as Record<string, unknown>;
  if (keys.join(",") !== "op,sessionId,v" || request.v !== MAIN_DRAIN_PROTOCOL || typeof request.sessionId !== "string" ||
    !UUID.test(request.sessionId) || !(MAIN_DRAIN_OPS as readonly unknown[]).includes(request.op)) return undefined;
  return { v: 1, op: request.op as MainDrainOp, sessionId: request.sessionId };
};

export interface MainDrainActorRow { id: string; name: string; status: string; residency: "session" | "durable" | "unknown" }

/** One reply line. Private evidence: ids and counts, never message text. */
export interface MainDrainReply {
  v: 1;
  ok: boolean;
  op: MainDrainOp | null;
  error: string | null;
  sessionId: string;
  rootId: string;
  meshRoot: string;
  pid: number;
  inbox: { closed: boolean; closedAt: number | null; held: number | null; handed: number | null; journalOpen: boolean | null;
    halted: boolean | null; reloading: boolean | null };
  native: NativeInputObservation;
  actors: { known: boolean; owned: MainDrainActorRow[]; stopRefused: Array<{ id: string; code: string }> };
  agents: { known: boolean; running: Array<{ id: string; status: string }> };
  /** exit only: the Main accepted a clean exit and shuts down right after this reply. */
  exiting: boolean;
  /** exit only: why the Main refused to exit (fixed codes). */
  exitRefused: string[];
}
