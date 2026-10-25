import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import type { MainDrainBindingRequest } from "./binding.js";
import { MAIN_DRAIN_MAX_REPLY_BYTES, mainDrainDirectory, mainDrainSocketName, type MainDrainOp, type MainDrainReply } from "./protocol.js";

/** A fixed-code client failure: never a payload, a path or an OS message. */
export class MainDrainClientError extends Error {
  /** written: the request reached the socket, so the Main may have acted on it. */
  constructor(readonly code: string, readonly written = false) { super(code); this.name = "MainDrainClientError"; }
}

/**
 * The Main's socket, refused unless its directory is a real, uid-owned 0700 directory whose
 * ancestors are root- or uid-owned and not group/other-writable (unless sticky), and the socket
 * itself is a uid-owned socket. A planted socket elsewhere is never contacted.
 */
export const mainDrainSocket = (meshRoot: string, sessionId: string, uid: number): string => {
  let directory: string;
  let mesh: fs.Stats;
  try {
    const root = fs.realpathSync(meshRoot);
    mesh = fs.statSync(root);
    directory = mainDrainDirectory(root);
  } catch { throw new MainDrainClientError("MAIN_DRAIN_MESH_ROOT_UNREADABLE"); }
  try {
    const stat = fs.lstatSync(directory);
    // The mesh root's own sibling: on the same filesystem as the mesh root, which this uid owns.
    if (!stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o7777) !== 0o700 || stat.dev !== mesh.dev || mesh.uid !== uid) {
      throw new MainDrainClientError("MAIN_DRAIN_DIRECTORY_UNSAFE");
    }
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
  let entry: fs.Stats;
  try { entry = fs.lstatSync(socket); } catch { throw new MainDrainClientError("MAIN_DRAIN_UNREACHABLE"); }
  if (!entry.isSocket() || entry.uid !== uid) throw new MainDrainClientError("MAIN_DRAIN_SOCKET_UNSAFE");
  return socket;
};

/**
 * One request, one reply line. A refusal rejects with MAIN_DRAIN_REFUSED_<code>. `verifyServer`, when
 * given, checks the connected server's kernel identity BEFORE the request is written; a code refuses.
 */
export const callMainDrain = (socket: string, sessionId: string, op: MainDrainOp, binding: MainDrainBindingRequest | null,
  timeoutMs = 30_000, verifyServer?: (client: net.Socket) => Promise<string | undefined>): Promise<MainDrainReply> =>
  new Promise((resolve, reject) => {
    // Connect through a held descriptor of the (re-checked) directory: no sun_path length limit, no swap.
    const directory = path.dirname(socket);
    let fd: number;
    try {
      fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
    } catch { reject(new MainDrainClientError("MAIN_DRAIN_UNREACHABLE")); return; }
    try {
      const held = fs.fstatSync(fd);
      const named = fs.lstatSync(directory);
      if (held.dev !== named.dev || held.ino !== named.ino || held.uid !== process.geteuid?.() || (held.mode & 0o7777) !== 0o700) {
        throw new Error("unsafe");
      }
    } catch { fs.closeSync(fd); reject(new MainDrainClientError("MAIN_DRAIN_DIRECTORY_UNSAFE")); return; }
    const client = net.connect(`/proc/self/fd/${fd}/${path.basename(socket)}`);
    let buffer = Buffer.alloc(0);
    let done = false;
    let written = false;
    const finish = (error?: MainDrainClientError, reply?: MainDrainReply): void => {
      if (done) return;
      done = true;
      client.destroy();
      try { fs.closeSync(fd); } catch { /* closed */ }
      if (error) reject(written && !error.written ? new MainDrainClientError(error.code, true) : error); else resolve(reply!);
    };
    client.setTimeout(timeoutMs, () => finish(new MainDrainClientError("MAIN_DRAIN_TIMEOUT")));
    client.once("error", () => finish(new MainDrainClientError("MAIN_DRAIN_UNREACHABLE")));
    client.once("connect", () => {
      const write = (): void => { if (!done) { written = true; client.write(`${JSON.stringify({ v: 1, op, sessionId, binding })}\n`); } };
      if (!verifyServer) { write(); return; }
      verifyServer(client).then(refusal => refusal ? finish(new MainDrainClientError(refusal)) : write(),
        () => finish(new MainDrainClientError("MAIN_DRAIN_SERVER_UNVERIFIED")));
    });
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
        finish(new MainDrainClientError(typeof value.error === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(value.error)
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
