import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { diagnostics } from "../core/diagnostics.js";
import type { MainDrainControl } from "./control.js";
import { MAIN_DRAIN_DIRECTORY, MAIN_DRAIN_MAX_REQUEST_BYTES, MAIN_DRAIN_MAX_SOCKET_PATH, mainDrainSocketName,
  parseMainDrainRequest, type MainDrainOp } from "./protocol.js";

const MAX_CONNECTIONS = 4;
const REQUEST_TIMEOUT_MS = 5_000;

/** Every directory from `/` to `directory` is root- or uid-owned and not group/other-writable unless sticky. */
const safeChain = (directory: string, uid: number): boolean => {
  for (let current = directory; ; current = path.dirname(current)) {
    const stat = fs.lstatSync(current);
    if (!stat.isDirectory() || (stat.uid !== 0 && stat.uid !== uid) || ((stat.mode & 0o022) !== 0 && (stat.mode & 0o1000) === 0)) return false;
    if (path.dirname(current) === current) return true;
  }
};

interface Pinned { directory: string; socket: string; dev: number; ino: number }

/** The private socket directory: real path, uid-owned, exactly 0700, safe ancestors. Throws a fixed code. */
const pinDirectory = (meshRoot: string, uid: number): Pinned => {
  const root = fs.realpathSync(meshRoot);
  const directory = path.join(root, MAIN_DRAIN_DIRECTORY);
  try { fs.mkdirSync(directory, { mode: 0o700 }); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw new Error("MAIN_DRAIN_DIRECTORY_UNAVAILABLE");
  }
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o7777) !== 0o700 || !safeChain(directory, uid)) {
    throw new Error("MAIN_DRAIN_DIRECTORY_UNSAFE");
  }
  return { directory, socket: "", dev: stat.dev, ino: stat.ino };
};

const stillPinned = (pinned: Pinned, uid: number): boolean => {
  try {
    const stat = fs.lstatSync(pinned.directory);
    return stat.isDirectory() && stat.dev === pinned.dev && stat.ino === pinned.ino && stat.uid === uid && (stat.mode & 0o7777) === 0o700;
  } catch { return false; }
};

/** A leftover socket is removed only when it is ours (uid) and nobody answers on it. */
const clearStaleSocket = (socket: string, uid: number): Promise<void> => new Promise((resolve, reject) => {
  let stat: fs.Stats | undefined;
  try { stat = fs.lstatSync(socket); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") { resolve(); return; }
    reject(new Error("MAIN_DRAIN_SOCKET_UNREADABLE")); return;
  }
  if (!stat.isSocket() || stat.uid !== uid) { reject(new Error("MAIN_DRAIN_SOCKET_FOREIGN")); return; }
  const probe = net.connect(socket);
  probe.once("connect", () => { probe.destroy(); reject(new Error("MAIN_DRAIN_SOCKET_IN_USE")); });
  probe.once("error", (error: NodeJS.ErrnoException) => {
    if (error.code !== "ECONNREFUSED") { reject(new Error("MAIN_DRAIN_SOCKET_IN_USE")); return; }
    try {
      const again = fs.lstatSync(socket);
      if (again.ino === stat!.ino && again.dev === stat!.dev) fs.unlinkSync(socket);
      resolve();
    } catch { reject(new Error("MAIN_DRAIN_SOCKET_UNREADABLE")); }
  });
});

/**
 * D3 seat drain (smarty-dev#15310): the owner-only local channel into this live Main.
 *
 * Authorization is the kernel's: the socket sits in a 0700 directory owned by this process's
 * uid (checked at listen and again at every connection), so only the same OS user or root can
 * connect. No mesh, NATS or bridge path reaches it, and nothing in a request names a sender.
 * The request carries no authority beyond its op; the session id must be this Main's own.
 * Linux only; elsewhere the drain is unavailable, which every client reports as not drained.
 */
export class MainDrainServer {
  #server: net.Server | undefined;
  #pinned: Pinned | undefined;
  #socketIdentity: { dev: number; ino: number } | undefined;
  #connections = 0;
  #queue: Promise<unknown> = Promise.resolve();
  #closed = false;

  constructor(readonly control: MainDrainControl, readonly options: { platform?: NodeJS.Platform; uid?: number } = {}) {}

  get socketPath(): string | undefined { return this.#pinned?.socket; }

  /** Resolves "listening" or a fixed unavailable code; it never throws. */
  async start(): Promise<string> {
    try {
      const platform = this.options.platform ?? process.platform;
      const uid = this.options.uid ?? process.getuid?.();
      if (platform !== "linux" || uid === undefined) return "MAIN_DRAIN_UNSUPPORTED_PLATFORM";
      const pinned = pinDirectory(this.control.host.meshRoot, uid);
      pinned.socket = path.join(pinned.directory, mainDrainSocketName(this.control.host.sessionId));
      if (Buffer.byteLength(pinned.socket) > MAIN_DRAIN_MAX_SOCKET_PATH) return "MAIN_DRAIN_SOCKET_PATH_TOO_LONG";
      await clearStaleSocket(pinned.socket, uid);
      const server = net.createServer({ allowHalfOpen: false });
      server.maxConnections = MAX_CONNECTIONS;
      server.on("connection", socket => this.#serve(socket, uid));
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(pinned.socket, () => { server.off("error", reject); resolve(); });
      });
      if (this.#closed) { server.close(); return "MAIN_DRAIN_CLOSED"; }
      fs.chmodSync(pinned.socket, 0o600);
      const stat = fs.lstatSync(pinned.socket);
      if (!stat.isSocket() || stat.uid !== uid || !stillPinned(pinned, uid)) { server.close(); return "MAIN_DRAIN_SOCKET_UNSAFE"; }
      server.on("error", error => diagnostics.warn("src/main-drain/server.ts", "[pi-fabric] main drain channel error", error));
      this.#server = server;
      this.#pinned = pinned;
      this.#socketIdentity = { dev: stat.dev, ino: stat.ino };
      return "listening";
    } catch (error) {
      const code = error instanceof Error && /^MAIN_DRAIN_[A-Z_]+$/.test(error.message) ? error.message : "MAIN_DRAIN_START_FAILED";
      diagnostics.warn("src/main-drain/server.ts", `[pi-fabric] main drain channel unavailable: ${code}`);
      return code;
    }
  }

  async close(): Promise<void> {
    this.#closed = true;
    const server = this.#server;
    this.#server = undefined;
    if (!server) return;
    await new Promise<void>(resolve => server.close(() => resolve()));
    const pinned = this.#pinned;
    const identity = this.#socketIdentity;
    if (!pinned || !identity) return;
    try {
      const stat = fs.lstatSync(pinned.socket);
      if (stat.dev === identity.dev && stat.ino === identity.ino) fs.unlinkSync(pinned.socket);
    } catch { /* already gone */ }
  }

  #serve(socket: net.Socket, uid: number): void {
    if (this.#connections >= MAX_CONNECTIONS || !this.#pinned || !stillPinned(this.#pinned, uid)) { socket.destroy(); return; }
    this.#connections++;
    socket.once("close", () => { this.#connections--; });
    socket.setTimeout(REQUEST_TIMEOUT_MS, () => socket.destroy());
    socket.on("error", () => socket.destroy());
    let buffer = Buffer.alloc(0);
    let handled = false;
    socket.on("data", (chunk: Buffer) => {
      if (handled) return;
      buffer = Buffer.concat([buffer, chunk]);
      const newline = buffer.indexOf(0x0a);
      if (newline < 0) {
        if (buffer.length > MAIN_DRAIN_MAX_REQUEST_BYTES) { handled = true; this.#refuse(socket, null, "REQUEST_TOO_LARGE"); }
        return;
      }
      handled = true;
      const request = newline <= MAIN_DRAIN_MAX_REQUEST_BYTES ? parseMainDrainRequest(buffer.subarray(0, newline).toString("utf8")) : undefined;
      if (!request) { this.#refuse(socket, null, "REQUEST_INVALID"); return; }
      if (request.sessionId !== this.control.host.sessionId) { this.#refuse(socket, request.op, "SESSION_MISMATCH"); return; }
      // One op at a time, in arrival order: the fence and the actor stops never race.
      const run = this.#queue.then(() => this.control.handle(request.op));
      this.#queue = run.catch(() => undefined);
      run.then(reply => {
        socket.end(`${JSON.stringify(reply)}\n`, () => { if (reply.exiting) this.control.commitExit(); });
      }, () => this.#refuse(socket, request.op, "INTERNAL"));
    });
  }

  #refuse(socket: net.Socket, op: MainDrainOp | null, error: string): void {
    socket.end(`${JSON.stringify({ v: 1, ok: false, op, error })}\n`);
  }
}
