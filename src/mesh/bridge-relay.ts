import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { peerCredentials, type PeerInfo } from "../records/peer.js";
import type { BridgePublish } from "./bridge.js";
import { FABRIC_IDEMPOTENCY_KEY_REUSED, FabricIdempotencyKeyReusedError } from "./control-idempotency.js";

export const FABRIC_RELAY_DESTINATION_UNAVAILABLE = "FABRIC_RELAY_DESTINATION_UNAVAILABLE";

/** A lost receipt can follow delivery. This error deliberately makes no non-execution claim. */
export class BridgeRelayUnavailableError extends Error {
  readonly code = FABRIC_RELAY_DESTINATION_UNAVAILABLE;
  readonly retryable = true;
  constructor() {
    super("Bridge relay destination unavailable");
    this.name = "BridgeRelayUnavailableError";
  }
}

/** Internal callback guard only; never serialized or supplied by a relay event. */
export interface BridgeRelayDispatchBudget {
  readonly signal: AbortSignal;
  /** Rechecks the ORIGINAL connection deadline/cancellation and authenticated source binding. */
  remaining: () => number;
}

const NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/;
const DEADLINE_MS = 4_000;
const MAX_FRAME_BYTES = 8 * 1024 * 1024;
const MAX_METADATA_BYTES = 8 * 1024;
const MAX_CONNECTIONS = 8;
const MAX_DELIVERIES = 4;
// Linux O_PATH pins a pathname socket's inode without opening it as a stream.
const O_PATH = 0x200000;
const DIR_FLAGS = fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW;
const READ_FLAGS = fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK;
const RUN_FLAGS = new Set([
  "mesh", "name", "remote", "cursor", "lock-protocol", "max-state-bytes", "call-timeout-ms",
  "log-level", "factory-rate", "factory-burst", "factory-bytes", "factory-byte-burst",
  "ssh", "ssh-key", "ssh-port", "ssh-known-hosts",
]);
const unavailable = (): BridgeRelayUnavailableError => new BridgeRelayUnavailableError();
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const exactKeys = (value: Record<string, unknown>, keys: string[]): boolean =>
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const sameInode = (a: fs.Stats, b: fs.Stats | undefined): boolean =>
  !!b && a.dev === b.dev && a.ino === b.ino;
const closeFd = (fd: number): void => { try { fs.closeSync(fd); } catch { /* already closed */ } };
const socketName = (remote: string): string => `relay-${remote}.sock`;
const registrationName = (remote: string): string => `relay-${remote}.json`;

interface Pin { fd: number; stat: fs.Stats }
interface Directory {
  fd: number;
  route: string;
  check: () => void;
  close: () => void;
}
interface Binding {
  localName: string;
  remoteName: string;
  meshRoot: string;
  bridgeScriptPath: string;
  nodePath: string;
}
interface Registration extends Binding {
  version: 1;
  pid: number;
  startTicks: string;
  bootId: string;
  socketDev: number;
  socketIno: number;
}
interface ProcIdentity extends Binding { pid: number; startTicks: string }
interface RegisteredPeer { credentials: PeerInfo; identity: ProcIdentity }

/** Every component is opened no-follow, held, and checked again before any pathname commit. */
const pinDirectory = (uid: number): Directory => {
  const configured = process.env.XDG_RUNTIME_DIR;
  if (configured !== undefined && (!configured || !path.isAbsolute(configured))) throw unavailable();
  const home = path.resolve(os.homedir());
  const base = configured === undefined ? path.join(home, ".local", "state") : path.resolve(configured);
  const parts = path.join(base, "mesh-bridge", "relay").split(path.sep).filter(Boolean);
  const baseDepth = base.split(path.sep).filter(Boolean).length;
  const homeDepth = home.split(path.sep).filter(Boolean).length;
  const createFrom = configured === undefined ? homeDepth : baseDepth;
  const pins: Array<Pin & { at: string; strict: boolean; own: boolean }> = [];
  const validate = (stat: fs.Stats, strict: boolean, own: boolean): void => {
    // Root-owned sticky ancestors (e.g. /tmp) cannot unlink another uid's owned entry.
    const stickyRoot = stat.uid === 0 && (stat.mode & 0o1000) !== 0;
    if (!stat.isDirectory() || (stat.uid !== uid && stat.uid !== 0) ||
      ((stat.mode & 0o022) !== 0 && !stickyRoot) ||
      (own && stat.uid !== uid) || (strict && (stat.mode & 0o7777) !== 0o700)) throw unavailable();
  };
  try {
    const root = fs.openSync("/", DIR_FLAGS);
    const rootStrict = configured !== undefined && baseDepth === 0;
    const rootOwn = rootStrict || (configured === undefined && homeDepth === 0);
    pins.push({ fd: root, stat: fs.fstatSync(root), at: "/", strict: rootStrict, own: rootOwn });
    validate(pins[0]!.stat, rootStrict, rootOwn);
    for (let index = 0; index < parts.length; index++) {
      const parent = pins[pins.length - 1]!;
      const at = `/proc/self/fd/${parent.fd}/${parts[index]!}`;
      let fd: number;
      try { fd = fs.openSync(at, DIR_FLAGS); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT" || index < createFrom) throw error;
        try { fs.mkdirSync(at, { mode: 0o700 }); }
        catch (mkdirError) { if ((mkdirError as NodeJS.ErrnoException).code !== "EEXIST") throw mkdirError; }
        fd = fs.openSync(at, DIR_FLAGS);
      }
      const strict = index >= baseDepth || (configured !== undefined && index === baseDepth - 1);
      const own = strict || (configured === undefined && index === homeDepth - 1);
      const pin = { fd, stat: fs.fstatSync(fd), at, strict, own };
      pins.push(pin);
      validate(pin.stat, strict, own);
      if (!sameInode(pin.stat, fs.lstatSync(at))) throw unavailable();
    }
    const last = pins[pins.length - 1]!;
    const route = `/proc/self/fd/${last.fd}`;
    const check = (): void => {
      for (const pin of pins) {
        const now = fs.fstatSync(pin.fd);
        validate(now, pin.strict, pin.own);
        if (!sameInode(pin.stat, now) || !sameInode(now, fs.lstatSync(pin.at))) throw unavailable();
      }
      if (!sameInode(last.stat, fs.statSync(`${route}/.`))) throw unavailable();
    };
    check();
    return { fd: last.fd, route, check, close: () => { for (const pin of pins.reverse()) closeFd(pin.fd); } };
  } catch {
    for (const pin of pins.reverse()) closeFd(pin.fd);
    throw unavailable();
  }
};

const readBounded = (fd: number, maximum: number): Buffer => {
  const buffer = Buffer.allocUnsafe(maximum + 1);
  let length = 0;
  for (;;) {
    const count = fs.readSync(fd, buffer, length, buffer.length - length, null);
    if (!count) return buffer.subarray(0, length);
    length += count;
    if (length > maximum) throw unavailable();
  }
};
const procText = (file: string, maximum: number): string => {
  const fd = fs.openSync(file, READ_FLAGS);
  try {
    const bytes = readBounded(fd, maximum);
    const text = bytes.toString("utf8");
    if (!Buffer.from(text, "utf8").equals(bytes)) throw unavailable();
    return text;
  } finally { closeFd(fd); }
};
const bootId = (): string => {
  const id = procText("/proc/sys/kernel/random/boot_id", 128).trim();
  if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(id)) throw unavailable();
  return id;
};
const startTicks = (pid: number): string => {
  const stat = procText(`/proc/${pid}/stat`, MAX_METADATA_BYTES);
  const end = stat.lastIndexOf(")");
  if (!stat.startsWith(`${pid} (`) || end < 0) throw unavailable();
  // The tail starts at field 3; starttime is field 22. comm may itself contain ')'.
  const ticks = stat.slice(end + 1).trim().split(/\s+/)[19];
  if (!ticks || !/^[1-9][0-9]{0,23}$/.test(ticks)) throw unavailable();
  return ticks;
};
const canonical = (value: string, cwd: string): string => fs.realpathSync(path.resolve(cwd, value));

/** Main script is argv[1], not a matching token in flags or in the transport command. */
const inspectProcess = (pid: number, uid: number, expected: Binding, remote?: string): ProcIdentity => {
  if (!Number.isSafeInteger(pid) || pid <= 0 || fs.statSync(`/proc/${pid}`).uid !== uid) throw unavailable();
  const before = startTicks(pid);
  const nodePath = fs.realpathSync(`/proc/${pid}/exe`);
  const cwd = fs.realpathSync(`/proc/${pid}/cwd`);
  const raw = procText(`/proc/${pid}/cmdline`, 64 * 1024);
  if (!raw.endsWith("\0")) throw unavailable();
  const argv = raw.slice(0, -1).split("\0");
  if (!argv[0] || !argv[1] || argv[1].startsWith("-") || argv[2] !== "run" || nodePath !== expected.nodePath ||
    canonical(argv[1], cwd) !== expected.bridgeScriptPath) throw unavailable();
  const flags = new Map<string, string>();
  for (let index = 3; index < argv.length; index += 2) {
    if (argv[index] === "--") break;
    const flag = argv[index]!;
    const value = argv[index + 1];
    const name = flag.slice(2);
    if (!flag.startsWith("--") || !RUN_FLAGS.has(name) || flags.has(name) || !value) throw unavailable();
    flags.set(name, value);
  }
  const mesh = flags.get("mesh");
  const sourceRemote = flags.get("remote");
  if (!mesh || !sourceRemote || !NAME_PATTERN.test(sourceRemote) || sourceRemote === expected.localName ||
    flags.get("name") !== expected.localName || (remote !== undefined && sourceRemote !== remote) ||
    canonical(mesh, cwd) !== expected.meshRoot || !flags.get("cursor")) throw unavailable();
  if (startTicks(pid) !== before || fs.realpathSync(`/proc/${pid}/exe`) !== nodePath ||
    procText(`/proc/${pid}/cmdline`, 64 * 1024) !== raw || fs.statSync(`/proc/${pid}`).uid !== uid) throw unavailable();
  return { ...expected, pid, startTicks: before, remoteName: sourceRemote };
};

const pinSocket = (directory: Directory, name: string, uid: number): Pin => {
  const at = `${directory.route}/${name}`;
  const fd = fs.openSync(at, O_PATH | fs.constants.O_NOFOLLOW);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isSocket() || stat.uid !== uid || (stat.mode & 0o7777) !== 0o600 || stat.nlink !== 1 ||
      !sameInode(stat, fs.lstatSync(at))) throw unavailable();
    return { fd, stat };
  } catch { closeFd(fd); throw unavailable(); }
};
const readRegistration = (directory: Directory, remote: string, uid: number): { pin: Pin; value: Registration } => {
  const at = `${directory.route}/${registrationName(remote)}`;
  const fd = fs.openSync(at, READ_FLAGS);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid || (stat.mode & 0o7777) !== 0o600 || stat.nlink !== 1 ||
      stat.size <= 0 || stat.size > MAX_METADATA_BYTES || !sameInode(stat, fs.lstatSync(at))) throw unavailable();
    const value: unknown = JSON.parse(readBounded(fd, MAX_METADATA_BYTES).toString("utf8"));
    if (!object(value) || !exactKeys(value, ["version", "pid", "startTicks", "bootId", "localName", "remoteName",
      "meshRoot", "bridgeScriptPath", "nodePath", "socketDev", "socketIno"]) || value.version !== 1 ||
      !Number.isSafeInteger(value.pid) || (value.pid as number) <= 0 ||
      typeof value.startTicks !== "string" || !/^[1-9][0-9]{0,23}$/.test(value.startTicks) ||
      typeof value.bootId !== "string" || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value.bootId) ||
      typeof value.localName !== "string" || !NAME_PATTERN.test(value.localName) ||
      value.remoteName !== remote || typeof value.meshRoot !== "string" ||
      typeof value.bridgeScriptPath !== "string" || typeof value.nodePath !== "string" ||
      !Number.isSafeInteger(value.socketDev) || !Number.isSafeInteger(value.socketIno) ||
      !sameInode(stat, fs.lstatSync(at))) throw unavailable();
    return { pin: { fd, stat }, value: value as unknown as Registration };
  } catch { closeFd(fd); throw unavailable(); }
};
const sameBinding = (a: Binding, b: Binding): boolean => a.localName === b.localName && a.remoteName === b.remoteName &&
  a.meshRoot === b.meshRoot && a.bridgeScriptPath === b.bridgeScriptPath && a.nodePath === b.nodePath;

const validatePeer = (credentials: PeerInfo, expected: Binding, directory: Directory, uid: number,
  currentBoot: string, remote?: string, connectedSocket?: Pin): ProcIdentity => {
  if (credentials.uid !== uid) throw unavailable();
  directory.check();
  const identity = inspectProcess(credentials.pid, uid, expected, remote);
  const registration = readRegistration(directory, identity.remoteName, uid);
  let socket: Pin | undefined;
  try {
    socket = pinSocket(directory, socketName(identity.remoteName), uid);
    const value = registration.value;
    if (!sameBinding(value, identity) || value.pid !== credentials.pid || value.startTicks !== identity.startTicks ||
      value.bootId !== currentBoot || value.socketDev !== socket.stat.dev || value.socketIno !== socket.stat.ino ||
      (connectedSocket !== undefined && !sameInode(socket.stat, fs.fstatSync(connectedSocket.fd))) ||
      startTicks(credentials.pid) !== identity.startTicks) throw unavailable();
    directory.check();
    return identity;
  } finally { closeFd(registration.pin.fd); if (socket) closeFd(socket.fd); }
};

/** A move to a fresh private slot makes deletion conditional on the MOVED inode, not a racy pre-unlink stat.
 * If a replacement wins the rename race it is never deleted; a no-clobber link restores its name if possible.
 * Hostile same-uid mutation inside the private slot remains the accepted account-integrity threat. */
const removePinned = (directory: Directory, owned: Array<{ name: string; pin: Pin }>, uid: number): void => {
  directory.check();
  const present = owned.filter(({ name, pin }) => sameInode(fs.fstatSync(pin.fd),
    fs.lstatSync(`${directory.route}/${name}`, { throwIfNoEntry: false })));
  if (!present.length) return;
  const slot = fs.mkdtempSync(`${directory.route}/.retire-`);
  const slotFd = fs.openSync(slot, DIR_FLAGS);
  const slotStat = fs.fstatSync(slotFd);
  try {
    if (!slotStat.isDirectory() || slotStat.uid !== uid || (slotStat.mode & 0o7777) !== 0o700 ||
      !sameInode(slotStat, fs.lstatSync(slot))) throw unavailable();
    for (const { name, pin } of present) {
      const from = `${directory.route}/${name}`;
      const moved = `/proc/self/fd/${slotFd}/${name}`;
      directory.check();
      if (!sameInode(fs.fstatSync(pin.fd), fs.lstatSync(from, { throwIfNoEntry: false }))) continue;
      try { fs.renameSync(from, moved); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; throw error; }
      const found = fs.lstatSync(moved);
      if (!sameInode(fs.fstatSync(pin.fd), found)) {
        try { fs.linkSync(moved, from); } catch { /* never overwrite a winner; retain the replacement in the slot */ }
        throw unavailable();
      }
      if (!sameInode(slotStat, fs.fstatSync(slotFd)) || !sameInode(slotStat, fs.lstatSync(slot)) ||
        !sameInode(fs.fstatSync(pin.fd), fs.lstatSync(moved))) throw unavailable();
      fs.unlinkSync(moved);
    }
    if (sameInode(slotStat, fs.lstatSync(slot))) fs.rmdirSync(slot);
  } finally { closeFd(slotFd); }
};

/** Never reap an unknown socket, a live registration, or a record for another mesh/release/link. */
const reclaimStale = (directory: Directory, expected: Binding, uid: number, currentBoot: string): void => {
  const socketAt = `${directory.route}/${socketName(expected.remoteName)}`;
  const recordAt = `${directory.route}/${registrationName(expected.remoteName)}`;
  const socketExists = fs.lstatSync(socketAt, { throwIfNoEntry: false });
  const recordExists = fs.lstatSync(recordAt, { throwIfNoEntry: false });
  if (!socketExists && !recordExists) return;
  if (!socketExists || !recordExists) throw unavailable();
  const record = readRegistration(directory, expected.remoteName, uid);
  let socket: Pin | undefined;
  try {
    socket = pinSocket(directory, socketName(expected.remoteName), uid);
    const old = record.value;
    if (!sameBinding(old, expected) || old.socketDev !== socket.stat.dev || old.socketIno !== socket.stat.ino) throw unavailable();
    if (old.bootId === currentBoot) {
      try { if (startTicks(old.pid) === old.startTicks) throw unavailable(); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    }
    // Remove the socket before the registration. O_EXCL publication still arbitrates simultaneous starters.
    removePinned(directory, [{ name: socketName(expected.remoteName), pin: socket },
      { name: registrationName(expected.remoteName), pin: record.pin }], uid);
  } finally { closeFd(record.pin.fd); if (socket) closeFd(socket.fd); }
};

const encode = (value: unknown): Buffer => {
  const text = JSON.stringify(value);
  const length = Buffer.byteLength(text);
  if (!length || length > MAX_FRAME_BYTES) throw unavailable();
  const frame = Buffer.allocUnsafe(length + 4);
  frame.writeUInt32BE(length, 0);
  frame.write(text, 4, length, "utf8");
  return frame;
};
const READY = encode({ version: 1, ready: true });
const OK = encode({ version: 1, ok: true });
const REFUSED = encode({ version: 1, ok: false, code: FABRIC_RELAY_DESTINATION_UNAVAILABLE });
const IDEMPOTENCY_REFUSED = encode({ version: 1, ok: false, code: FABRIC_IDEMPOTENCY_KEY_REUSED });
const parse = (buffer: Buffer): unknown => {
  const text = buffer.toString("utf8");
  if (!Buffer.from(text, "utf8").equals(buffer)) throw unavailable();
  return JSON.parse(text);
};

/** DELIVER is the only envelope. Topic/principal/origin/destination authority belongs to deliver(). */
const publishShape = (event: unknown): event is BridgePublish => {
  if (!object(event) || typeof event.topic !== "string" || !event.topic || typeof event.kind !== "string" || !event.kind ||
    typeof event.to !== "string" || !event.to.trim() || !object(event.from) ||
    typeof event.from.id !== "string" || !event.from.id || typeof event.from.name !== "string" ||
    !["main", "actor", "agent"].includes(event.from.kind as string) ||
    (event.text !== undefined && typeof event.text !== "string") || !object(event.data) || !object(event.data.bridge)) return false;
  const stamp = event.data.bridge;
  return typeof stamp.from === "string" && !!stamp.from && typeof stamp.id === "string" && !!stamp.id &&
    (stamp.via === undefined || typeof stamp.via === "string");
};

/** Header and body allocations are bounded; EOF on DELIVER/receipt rejects extra frames before dispatch. */
const readFrame = (socket: net.Socket, maximum: number, requireEof: boolean): Promise<Buffer> =>
  new Promise((resolve, reject) => {
    const header = Buffer.allocUnsafe(4);
    let headerLength = 0;
    let body: Buffer | undefined;
    let bodyLength = 0;
    const cleanup = (): void => {
      socket.pause();
      socket.off("data", onData); socket.off("end", onEnd); socket.off("error", onError); socket.off("close", onError);
    };
    const fail = (): void => { cleanup(); reject(unavailable()); };
    const succeed = (): void => { cleanup(); resolve(body!); };
    const onError = (): void => fail();
    const onEnd = (): void => { if (body && bodyLength === body.length) succeed(); else fail(); };
    const onData = (chunk: Buffer): void => {
      let offset = 0;
      if (headerLength < 4) {
        const count = Math.min(4 - headerLength, chunk.length);
        chunk.copy(header, headerLength, 0, count); headerLength += count; offset += count;
        if (headerLength < 4) return;
        const length = header.readUInt32BE(0);
        if (!length || length > maximum) { fail(); return; }
        body = Buffer.allocUnsafe(length);
      }
      const count = Math.min(body!.length - bodyLength, chunk.length - offset);
      chunk.copy(body!, bodyLength, offset, offset + count); bodyLength += count; offset += count;
      if (offset !== chunk.length) { fail(); return; }
      if (bodyLength === body!.length && !requireEof) succeed();
    };
    socket.on("data", onData); socket.once("end", onEnd); socket.once("error", onError); socket.once("close", onError);
    if (socket.destroyed || socket.readableEnded) fail(); else socket.resume();
  });

/** One absolute deadline covers connect, BOTH authentications, handshake, body, callback and receipt. */
const deadline = (socket: net.Socket): BridgeRelayDispatchBudget & { wait: <T>(work: Promise<T>) => Promise<T>; finish: () => void } => {
  const now = (): number => Number(process.hrtime.bigint() / 1_000_000n);
  const expires = now() + DEADLINE_MS;
  let rejectFailure!: (error: BridgeRelayUnavailableError) => void;
  const failure = new Promise<never>((_, reject) => { rejectFailure = reject; });
  // A close between two waits must not make an unobserved rejection.
  void failure.catch(() => undefined);
  const controller = new AbortController();
  const fail = (): void => { controller.abort(); rejectFailure(unavailable()); socket.destroy(); };
  const timer = setTimeout(fail, DEADLINE_MS);
  socket.once("error", fail); socket.once("close", fail);
  return {
    signal: controller.signal,
    wait: work => Promise.race([failure, work]),
    remaining: () => { const left = expires - now(); if (left <= 0 || socket.destroyed || controller.signal.aborted) throw unavailable(); return left; },
    finish: () => { controller.abort(); clearTimeout(timer); socket.off("error", fail); socket.off("close", fail); },
  };
};
const writeFrame = (socket: net.Socket, frame: Buffer, end: boolean): Promise<void> => new Promise((resolve, reject) => {
  const done = (error?: Error | null): void => { if (error) reject(unavailable()); else resolve(); };
  if (end) socket.end(frame, () => done()); else socket.write(frame, done);
});

export async function startBridgeRelay(options: {
  localName: string;
  remoteName: string;
  meshRoot: string;
  bridgeScriptPath: string;
  deliver: (sourceRemote: string, event: BridgePublish, budget: BridgeRelayDispatchBudget) => Promise<void>;
  log?: (line: string) => void;
}): Promise<{ send: (destinationRemote: string, event: BridgePublish) => Promise<void>; close: () => Promise<void> }> {
  let directory: Directory | undefined;
  let bindFd: number | undefined;
  let ownedSocket: Pin | undefined;
  let ownedRegistration: Pin | undefined;
  const incoming = new Set<net.Socket>();
  const outgoing = new Set<net.Socket>();
  const deliveries = new Set<Promise<void>>();
  // A disconnected socket does not cancel the Python helper. Retain its slot until it settles.
  const authentications = new Set<Promise<PeerInfo | undefined>>();
  const credentialsFor = (socket: net.Socket, limit: ReturnType<typeof deadline>): Promise<PeerInfo | undefined> => {
    if (authentications.size >= MAX_CONNECTIONS) throw unavailable();
    const work = peerCredentials(socket, limit.remaining());
    authentications.add(work);
    void work.then(() => authentications.delete(work), () => authentications.delete(work));
    return limit.wait(work);
  };
  const server = net.createServer({ allowHalfOpen: true, pauseOnConnect: true });
  server.maxConnections = MAX_CONNECTIONS;
  server.on("error", () => undefined); // Never log payload-bearing callback or system errors.
  let closed = false;
  let ready = false;
  let closing: Promise<void> | undefined;
  const shutdown = (): Promise<void> => closing ??= (async () => {
    closed = true; ready = false;
    // libuv unlinks its bound pathname on close. Invalidate its dedicated directory-fd route FIRST,
    // so automatic cleanup cannot delete a replacement; the independent directory/socket pins survive.
    if (bindFd !== undefined) { closeFd(bindFd); bindFd = undefined; }
    const stopped = new Promise<void>(resolve => {
      if (!server.listening) { resolve(); return; }
      server.close(() => resolve());
    });
    for (const socket of [...incoming, ...outgoing]) socket.destroy();
    await stopped;
    let failed = false;
    if (directory) {
      try {
        const owned: Array<{ name: string; pin: Pin }> = [];
        if (ownedSocket) owned.push({ name: socketName(options.remoteName), pin: ownedSocket });
        if (ownedRegistration) owned.push({ name: registrationName(options.remoteName), pin: ownedRegistration });
        removePinned(directory, owned, process.getuid!());
      } catch { failed = true; }
    }
    if (ownedRegistration) closeFd(ownedRegistration.fd);
    if (ownedSocket) closeFd(ownedSocket.fd);
    directory?.close();
    if (failed) throw unavailable();
    // Cancellation fences only NEW dispatch. Already sent work may complete after receipt
    // loss; its slot remains occupied until settlement and bridge teardown owns the transport.
  })().catch(() => { throw unavailable(); });
  try {
    if (process.platform !== "linux" || typeof process.getuid !== "function" || process.release.name !== "node" ||
      !fs.constants.O_DIRECTORY || !fs.constants.O_NOFOLLOW || !NAME_PATTERN.test(options.localName) ||
      !NAME_PATTERN.test(options.remoteName) || options.localName === options.remoteName || typeof options.deliver !== "function") throw unavailable();
    const uid = process.getuid();
    const expected: Binding = {
      localName: options.localName, remoteName: options.remoteName,
      meshRoot: fs.realpathSync(options.meshRoot), bridgeScriptPath: fs.realpathSync(options.bridgeScriptPath),
      nodePath: fs.realpathSync(process.execPath),
    };
    if (path.basename(expected.bridgeScriptPath) !== "mesh-bridge" || path.basename(path.dirname(expected.bridgeScriptPath)) !== "bin" ||
      !fs.statSync(expected.bridgeScriptPath).isFile() || !fs.statSync(expected.meshRoot).isDirectory() ||
      !/^node(?:js)?$/.test(path.basename(expected.nodePath))) throw unavailable();
    const currentBoot = bootId();
    const own = inspectProcess(process.pid, uid, expected, options.remoteName);
    const pinned = directory = pinDirectory(uid);
    reclaimStale(pinned, expected, uid, currentBoot);
    pinned.check();
    bindFd = fs.openSync(`${pinned.route}/.`, DIR_FLAGS);
    if (!sameInode(fs.fstatSync(pinned.fd), fs.fstatSync(bindFd))) throw unavailable();
    const bindAt = `/proc/self/fd/${bindFd}/${socketName(options.remoteName)}`;
    if (Buffer.byteLength(bindAt) >= 108) throw unavailable();
    server.on("connection", socket => {
      socket.on("error", () => undefined);
      if (closed || !ready || incoming.size + outgoing.size >= MAX_CONNECTIONS) { socket.destroy(); return; }
      incoming.add(socket);
      const limit = deadline(socket);
      void (async () => {
        try {
          const credentials = await credentialsFor(socket, limit);
          if (!credentials) throw unavailable();
          const identity = validatePeer(credentials, expected, pinned, uid, currentBoot);
          if (identity.remoteName === expected.remoteName || closed || !ready) throw unavailable();
          const peer: RegisteredPeer = { credentials, identity };
          // READY proves this server accepted the registered real source before any body is sent.
          await limit.wait(writeFrame(socket, READY, false));
          const value = parse(await limit.wait(readFrame(socket, MAX_FRAME_BYTES, true)));
          if (!object(value) || !exactKeys(value, ["version", "sourceRemote", "event"]) || value.version !== 1 ||
            value.sourceRemote !== peer.identity.remoteName || !publishShape(value.event) || closed || !ready ||
            deliveries.size >= MAX_DELIVERIES) throw unavailable();
          const fresh = validatePeer(peer.credentials, expected, pinned, uid, currentBoot, peer.identity.remoteName);
          if (fresh.startTicks !== peer.identity.startTicks) throw unavailable();
          const budget: BridgeRelayDispatchBudget = { signal: limit.signal, remaining: () => {
            limit.remaining();
            if (closed || !ready) throw unavailable();
            const current = validatePeer(peer.credentials, expected, pinned, uid, currentBoot, peer.identity.remoteName);
            if (current.startTicks !== peer.identity.startTicks) throw unavailable();
            return limit.remaining();
          } };
          budget.remaining();
          const delivery = Promise.resolve(options.deliver(fresh.remoteName, value.event, budget));
          deliveries.add(delivery);
          void delivery.then(() => deliveries.delete(delivery), () => deliveries.delete(delivery));
          await limit.wait(delivery);
          if (closed || !ready) throw unavailable();
          limit.remaining();
          await limit.wait(writeFrame(socket, OK, true));
        } catch (error) {
          if (!socket.destroyed && !socket.writableEnded) {
            const refusal = object(error) && error.code === FABRIC_IDEMPOTENCY_KEY_REUSED ? IDEMPOTENCY_REFUSED : REFUSED;
            // Fixed metadata only. Log every attempted negative frame, including an
            // ambiguous write, without trusting payload-bearing callback/system errors.
            try { options.log?.(`relay NAK stage=ipc reason=${refusal === IDEMPOTENCY_REFUSED ? "idempotency-conflict" : "destination-unavailable"}`); } catch { /* diagnostic only */ }
            try { await limit.wait(writeFrame(socket, refusal, true)); } catch { /* fixed refusal or connection loss only */ }
          }
        } finally { limit.finish(); socket.destroy(); incoming.delete(socket); }
      })();
    });
    await new Promise<void>((resolve, reject) => {
      const fail = (): void => { server.off("listening", listen); reject(unavailable()); };
      const listen = (): void => { server.off("error", fail); resolve(); };
      server.once("error", fail); server.once("listening", listen);
      server.listen(bindAt);
    });
    // Pin before chmod or registration. O_PATH follows no final symlink; chmod uses that held inode.
    const socketFd = fs.openSync(`${pinned.route}/${socketName(options.remoteName)}`, O_PATH | fs.constants.O_NOFOLLOW);
    ownedSocket = { fd: socketFd, stat: fs.fstatSync(socketFd) };
    if (!ownedSocket.stat.isSocket() || ownedSocket.stat.uid !== uid ||
      !sameInode(ownedSocket.stat, fs.lstatSync(`${pinned.route}/${socketName(options.remoteName)}`))) throw unavailable();
    fs.chmodSync(`/proc/self/fd/${socketFd}`, 0o600);
    ownedSocket.stat = fs.fstatSync(socketFd);
    if ((ownedSocket.stat.mode & 0o7777) !== 0o600 || ownedSocket.stat.nlink !== 1) throw unavailable();
    pinned.check();
    const recordAt = `${pinned.route}/${registrationName(options.remoteName)}`;
    const recordFd = fs.openSync(recordAt, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
    ownedRegistration = { fd: recordFd, stat: fs.fstatSync(recordFd) };
    if (!ownedRegistration.stat.isFile() || ownedRegistration.stat.uid !== uid ||
      (ownedRegistration.stat.mode & 0o7777) !== 0o600 || ownedRegistration.stat.nlink !== 1 ||
      !sameInode(ownedRegistration.stat, fs.lstatSync(recordAt))) throw unavailable();
    const record: Registration = { ...expected, version: 1, pid: process.pid, startTicks: own.startTicks,
      bootId: currentBoot, socketDev: ownedSocket.stat.dev, socketIno: ownedSocket.stat.ino };
    fs.writeFileSync(recordFd, JSON.stringify(record));
    fs.fsyncSync(recordFd);
    ownedRegistration.stat = fs.fstatSync(recordFd);
    pinned.check();
    if (startTicks(process.pid) !== own.startTicks ||
      !sameInode(ownedRegistration.stat, fs.lstatSync(recordAt)) ||
      !sameInode(ownedSocket.stat, fs.lstatSync(`${pinned.route}/${socketName(options.remoteName)}`))) throw unavailable();
    ready = true;

    return {
      send: async (destinationRemote, event) => {
        let socket: net.Socket | undefined;
        let destination: Pin | undefined;
        let limit: ReturnType<typeof deadline> | undefined;
        try {
          if (closed || !ready || !NAME_PATTERN.test(destinationRemote) || destinationRemote === expected.remoteName ||
            destinationRemote === expected.localName || incoming.size + outgoing.size >= MAX_CONNECTIONS || !publishShape(event)) throw unavailable();
          pinned.check();
          destination = pinSocket(pinned, socketName(destinationRemote), uid);
          socket = new net.Socket({ allowHalfOpen: true });
          socket.pause(); socket.on("error", () => undefined);
          outgoing.add(socket);
          limit = deadline(socket);
          const linked = socket;
          await limit.wait(new Promise<void>(resolve => {
            linked.once("connect", resolve);
            linked.connect(`${pinned.route}/${socketName(destinationRemote)}`);
          }));
          const credentials = await credentialsFor(socket, limit);
          if (!credentials) throw unavailable();
          const identity = validatePeer(credentials, expected, pinned, uid, currentBoot, destinationRemote, destination);
          const hello = parse(await limit.wait(readFrame(socket, 1024, false)));
          if (!object(hello) || !exactKeys(hello, ["version", "ready"]) || hello.version !== 1 || hello.ready !== true || closed) throw unavailable();
          // Authentication and the source-accepted handshake precede even body serialization.
          const fresh = validatePeer(credentials, expected, pinned, uid, currentBoot, destinationRemote, destination);
          if (fresh.startTicks !== identity.startTicks) throw unavailable();
          limit.remaining();
          const frame = encode({ version: 1, sourceRemote: expected.remoteName, event });
          limit.remaining();
          const [, receipt] = await limit.wait(Promise.all([
            writeFrame(socket, frame, true), readFrame(socket, 1024, true),
          ]));
          const result = parse(receipt);
          if (object(result) && exactKeys(result, ["version", "ok", "code"]) && result.version === 1 && result.ok === false &&
            result.code === FABRIC_IDEMPOTENCY_KEY_REUSED) throw new FabricIdempotencyKeyReusedError();
          if (!object(result) || !exactKeys(result, ["version", "ok"]) || result.version !== 1 || result.ok !== true) throw unavailable();
        } catch (error) {
          if (object(error) && error.code === FABRIC_IDEMPOTENCY_KEY_REUSED) throw new FabricIdempotencyKeyReusedError();
          throw unavailable();
        }
        finally {
          limit?.finish();
          if (socket) { socket.destroy(); outgoing.delete(socket); }
          if (destination) closeFd(destination.fd);
        }
      },
      close: shutdown,
    };
  } catch {
    await shutdown().catch(() => undefined);
    throw unavailable();
  }
}
