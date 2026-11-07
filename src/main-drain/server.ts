import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { diagnostics } from "../core/diagnostics.js";
import { peerCredentials } from "../records/peer.js";
import type { MainDrainControl, MainDrainPeer } from "./control.js";
import { drainSlot, pruneDrainSlot } from "./slot.js";
import { MAIN_DRAIN_MAX_REQUEST_BYTES, mainDrainDirectory, mainDrainSocketName, parseMainDrainRequest, type MainDrainOp } from "./protocol.js";

const MAX_CONNECTIONS = 4;
const REQUEST_TIMEOUT_MS = 5_000;
const OPERATION_TIMEOUT_MS = 25_000;

/** Every directory from `/` to `directory` is root- or uid-owned and not group/other-writable unless sticky. */
const safeChain = (directory: string, uid: number): boolean => {
  for (let current = directory; ; current = path.dirname(current)) {
    const stat = fs.lstatSync(current);
    if (!stat.isDirectory() || (stat.uid !== 0 && stat.uid !== uid) || ((stat.mode & 0o022) !== 0 && (stat.mode & 0o1000) === 0)) return false;
    if (path.dirname(current) === current) return true;
  }
};

/**
 * The socket directory, pinned by an open descriptor. Its sockaddr is `/proc/self/fd/<fd>/<name>`:
 * the kernel resolves it through the held directory, so the mesh root's length never meets the
 * 107-byte sun_path limit, and a swapped directory cannot be bound or reached.
 */
interface Pinned { directory: string; socket: string; address: string; fd: number; dev: number; ino: number }

/**
 * The private socket directory, the mesh root's sibling: real path, uid-owned, exactly 0700, safe ancestors, on the
 * SAME filesystem as the mesh root, which this uid owns. Throws a fixed code.
 */
const pinDirectory = (meshRoot: string, uid: number): Pinned => {
  const root = fs.realpathSync(meshRoot);
  const mesh = fs.statSync(root);
  if (!mesh.isDirectory() || mesh.uid !== uid) throw new Error("MAIN_DRAIN_DIRECTORY_UNSAFE");
  const directory = mainDrainDirectory(root);
  try { fs.mkdirSync(directory, { mode: 0o700 }); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw new Error("MAIN_DRAIN_DIRECTORY_UNAVAILABLE");
  }
  let fd: number;
  try { fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW); }
  catch { throw new Error("MAIN_DRAIN_DIRECTORY_UNSAFE"); }
  const held = fs.fstatSync(fd);
  const named = fs.lstatSync(directory);
  if (!held.isDirectory() || held.uid !== uid || (held.mode & 0o7777) !== 0o700 || named.ino !== held.ino || named.dev !== held.dev ||
    held.dev !== mesh.dev || !safeChain(directory, uid)) {
    fs.closeSync(fd);
    throw new Error("MAIN_DRAIN_DIRECTORY_UNSAFE");
  }
  return { directory, socket: "", address: "", fd, dev: held.dev, ino: held.ino };
};

const stillPinned = (pinned: Pinned, uid: number): boolean => {
  try {
    const held = fs.fstatSync(pinned.fd);
    const named = fs.lstatSync(pinned.directory);
    return held.dev === pinned.dev && held.ino === pinned.ino && named.dev === pinned.dev && named.ino === pinned.ino &&
      held.uid === uid && (held.mode & 0o7777) === 0o700;
  } catch { return false; }
};

/** A leftover socket is removed only when it is ours (uid) and nobody answers on it. */
const clearStaleSocket = (socket: string, address: string, uid: number): Promise<void> => new Promise((resolve, reject) => {
  let stat: fs.Stats | undefined;
  try { stat = fs.lstatSync(socket); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") { resolve(); return; }
    reject(new Error("MAIN_DRAIN_SOCKET_UNREADABLE")); return;
  }
  if (!stat.isSocket() || stat.uid !== uid) { reject(new Error("MAIN_DRAIN_SOCKET_FOREIGN")); return; }
  const probe = net.connect(address);
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

  constructor(readonly control: MainDrainControl, readonly options: {
    platform?: NodeJS.Platform; uid?: number;
    /** Tests only: the SO_PEERCRED reader. */
    peer?: (socket: net.Socket) => Promise<MainDrainPeer | undefined>;
    /** Tests may shorten the absolute request budget, never extend the production cap. */
    operationTimeoutMs?: number;
  } = {}) {}

  get socketPath(): string | undefined { return this.#pinned?.socket; }

  /** Resolves "listening" or a fixed unavailable code; it never throws. */
  async start(): Promise<string> {
    try {
      const platform = this.options.platform ?? process.platform;
      const uid = this.options.uid ?? process.geteuid?.();
      if (platform !== "linux" || uid === undefined) return "MAIN_DRAIN_UNSUPPORTED_PLATFORM";
      const pinned = pinDirectory(this.control.host.meshRoot, uid);
      const name = mainDrainSocketName(this.control.host.sessionId);
      pinned.socket = path.join(pinned.directory, name);
      pinned.address = `/proc/self/fd/${pinned.fd}/${name}`;
      const fail = (code: string): string => { fs.closeSync(pinned.fd); return code; };
      try { await clearStaleSocket(pinned.socket, pinned.address, uid); } catch (error) { fs.closeSync(pinned.fd); throw error; }
      const server = net.createServer({ allowHalfOpen: false });
      server.maxConnections = MAX_CONNECTIONS;
      server.on("connection", socket => this.#serve(socket, uid));
      try {
        await new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(pinned.address, () => { server.off("error", reject); resolve(); });
        });
      } catch { return fail("MAIN_DRAIN_LISTEN_FAILED"); }
      if (this.#closed) { server.close(); return fail("MAIN_DRAIN_CLOSED"); }
      fs.chmodSync(pinned.socket, 0o600);
      const stat = fs.lstatSync(pinned.socket);
      if (!stat.isSocket() || stat.uid !== uid || !stillPinned(pinned, uid)) { server.close(); return fail("MAIN_DRAIN_SOCKET_UNSAFE"); }
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
    try { fs.closeSync(pinned.fd); } catch { /* already closed */ }
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
      socket.setTimeout(0);
      const request = newline <= MAIN_DRAIN_MAX_REQUEST_BYTES ? parseMainDrainRequest(buffer.subarray(0, newline).toString("utf8")) : undefined;
      if (!request) { this.#refuse(socket, null, "REQUEST_INVALID"); return; }
      const configured = this.options.operationTimeoutMs;
      const wait = typeof configured === "number" && Number.isFinite(configured) && configured > 0
        ? Math.min(configured, OPERATION_TIMEOUT_MS) : OPERATION_TIMEOUT_MS;
      const deadline = Date.now() + wait;
      let timedOut = false;
      const expired = (): boolean => timedOut || Date.now() >= deadline;
      let timer: ReturnType<typeof setTimeout>;
      const timeout = new Promise<"OPERATION_TIMEOUT">(resolve => {
        timer = setTimeout(() => { timedOut = true; resolve("OPERATION_TIMEOUT"); }, wait);
      });
      // Queue wait shares the same absolute budget. Replies (including timeouts/refusals) stay in arrival order.
      const run = this.#queue.then(async () => {
        if (expired()) return "OPERATION_TIMEOUT";
        if (request.sessionId !== this.control.host.sessionId) return "SESSION_MISMATCH";
        const slot = request.binding ? drainSlot(this.control.host.rootId) : undefined;
        if (slot?.pendingOperation) return "OPERATION_PENDING";
        // Defer ALL work until the process-global slot owns it, including the peer await. A reload/close
        // can retire this server, but cannot admit a second mutation while this promise remains unsettled.
        const work = Promise.resolve().then(async () => {
          if (expired()) return "OPERATION_TIMEOUT";
          // Every binding needs SO_PEERCRED (the fixed isolated interpreter in records/peer.ts).
          const peer = request.binding ? await (this.options.peer ?? peerCredentials)(socket).catch(() => undefined) : undefined;
          if (expired()) return "OPERATION_TIMEOUT"; // A late peer proof must never start effects.
          return this.control.handle(request.op, request.binding, peer, uid);
        });
        if (slot) {
          slot.pendingOperation = work;
          const clear = (): void => {
            if (slot.pendingOperation === work) {
              delete slot.pendingOperation;
              pruneDrainSlot(this.control.host.rootId);
            }
          };
          // The timeout only releases the reply queue; it cannot cancel handle's side effects.
          void work.then(clear, clear);
        }
        return Promise.race([work, timeout]);
      });
      this.#queue = run.then(reply => {
        clearTimeout(timer);
        if (socket.destroyed) return;
        if (expired()) { this.#refuse(socket, request.op, "OPERATION_TIMEOUT"); return; }
        if (typeof reply === "string") { this.#refuse(socket, request.op, reply); return; }
        socket.end(`${JSON.stringify(reply)}\n`, () => {
          if (reply.exiting && !expired() && !socket.destroyed) this.control.commitExit();
        });
      }, () => {
        clearTimeout(timer);
        if (!socket.destroyed) this.#refuse(socket, request.op, expired() ? "OPERATION_TIMEOUT" : "INTERNAL");
      }).catch(() => undefined);
    });
  }

  #refuse(socket: net.Socket, op: MainDrainOp | null, error: string): void {
    socket.end(`${JSON.stringify({ v: 1, ok: false, op, error })}\n`);
  }
}
