import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import type { MainDrainBindingRequest } from "./binding.js";
import { MAIN_DRAIN_DIRECTORY, MAIN_DRAIN_MAX_REPLY_BYTES, MAIN_DRAIN_MAX_SOCKET_PATH, mainDrainSocketName,
  type MainDrainOp, type MainDrainReply } from "./protocol.js";

/** A fixed-code client failure: never a payload, a path or an OS message. */
export class MainDrainClientError extends Error {
  constructor(readonly code: string) { super(code); this.name = "MainDrainClientError"; }
}

/**
 * The Main's socket, refused unless its directory is a real, uid-owned 0700 directory whose
 * ancestors are root- or uid-owned and not group/other-writable (unless sticky), and the socket
 * itself is a uid-owned socket. A planted socket elsewhere is never contacted.
 */
export const mainDrainSocket = (meshRoot: string, sessionId: string, uid: number): string => {
  let directory: string;
  try { directory = path.join(fs.realpathSync(meshRoot), MAIN_DRAIN_DIRECTORY); } catch { throw new MainDrainClientError("MAIN_DRAIN_MESH_ROOT_UNREADABLE"); }
  try {
    const stat = fs.lstatSync(directory);
    if (!stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o7777) !== 0o700) throw new MainDrainClientError("MAIN_DRAIN_DIRECTORY_UNSAFE");
    for (let current = path.dirname(directory); ; current = path.dirname(current)) {
      const parent = fs.lstatSync(current);
      if (!parent.isDirectory() || (parent.uid !== 0 && parent.uid !== uid) || ((parent.mode & 0o022) !== 0 && (parent.mode & 0o1000) === 0)) {
        throw new MainDrainClientError("MAIN_DRAIN_DIRECTORY_UNSAFE");
      }
      if (path.dirname(current) === current) break;
    }
  } catch (error) {
    if (error instanceof MainDrainClientError) throw error;
    throw new MainDrainClientError("MAIN_DRAIN_UNREACHABLE");
  }
  const socket = path.join(directory, mainDrainSocketName(sessionId));
  if (Buffer.byteLength(socket) > MAIN_DRAIN_MAX_SOCKET_PATH) throw new MainDrainClientError("MAIN_DRAIN_SOCKET_PATH_TOO_LONG");
  let entry: fs.Stats;
  try { entry = fs.lstatSync(socket); } catch { throw new MainDrainClientError("MAIN_DRAIN_UNREACHABLE"); }
  if (!entry.isSocket() || entry.uid !== uid) throw new MainDrainClientError("MAIN_DRAIN_SOCKET_UNSAFE");
  return socket;
};

/** One request, one reply line. A refusal rejects with MAIN_DRAIN_REFUSED_<code>. */
export const callMainDrain = (socket: string, sessionId: string, op: MainDrainOp, binding: MainDrainBindingRequest | null,
  timeoutMs = 30_000): Promise<MainDrainReply> =>
  new Promise((resolve, reject) => {
    const client = net.connect(socket);
    let buffer = Buffer.alloc(0);
    let done = false;
    const finish = (error?: Error, reply?: MainDrainReply): void => {
      if (done) return;
      done = true;
      client.destroy();
      if (error) reject(error); else resolve(reply!);
    };
    client.setTimeout(timeoutMs, () => finish(new MainDrainClientError("MAIN_DRAIN_TIMEOUT")));
    client.once("error", () => finish(new MainDrainClientError("MAIN_DRAIN_UNREACHABLE")));
    client.once("connect", () => client.write(`${JSON.stringify({ v: 1, op, sessionId, binding })}\n`));
    client.on("data", (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length > MAIN_DRAIN_MAX_REPLY_BYTES) { finish(new MainDrainClientError("MAIN_DRAIN_REPLY_TOO_LARGE")); return; }
      const newline = buffer.indexOf(0x0a);
      if (newline < 0) return;
      let value: Partial<MainDrainReply> | null;
      try { value = JSON.parse(buffer.subarray(0, newline).toString("utf8")) as Partial<MainDrainReply> | null; }
      catch { finish(new MainDrainClientError("MAIN_DRAIN_REPLY_INVALID")); return; }
      if (typeof value !== "object" || value === null || value.v !== 1) { finish(new MainDrainClientError("MAIN_DRAIN_REPLY_INVALID")); return; }
      if (value.ok !== true) {
        finish(new MainDrainClientError(typeof value.error === "string" && /^[A-Z_]{1,64}$/.test(value.error)
          ? `MAIN_DRAIN_REFUSED_${value.error}` : "MAIN_DRAIN_REFUSED"));
        return;
      }
      if (value.sessionId !== sessionId || value.op !== op || typeof value.native !== "object" || !Array.isArray(value.probes)) {
        finish(new MainDrainClientError("MAIN_DRAIN_REPLY_MISMATCH"));
        return;
      }
      finish(undefined, value as MainDrainReply);
    });
    client.once("end", () => finish(new MainDrainClientError("MAIN_DRAIN_REPLY_INVALID")));
  });
