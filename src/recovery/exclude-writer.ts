/** Host-only, first-use REQUIRED pre-open composition. No registration or startup hooks.
 *
 * These ports are a provisional integration contract, NOT an authenticity ABI. The host
 * must supply authoritative existing identity-store reads/native CAS, the B1 controller's
 * verified capability, and actual mount/native-handle readers. In particular, accepting
 * this structural interface does not authenticate a fixture or a JSON fence assertion.
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import type { RecoveryPreOpenBinding } from "@earendil-works/pi-coding-agent";
import type {
  TransportHostLease, TransportHostLeasePredecessorSnapshot, TransportHostLeaseStartupSuccessor,
} from "../topology/host-leases.js";
import type { PortableIdentityAdapter, PortableSessionBinding } from "./l7-interfaces.js";

export interface PortableVolumeIdentity {
  readonly pvcUid: string;
  readonly csiHandle: string;
  readonly filesystemUuid: string;
}
export interface PortableTransportIdentity {
  readonly machine: string;
  readonly hostId: string;
  readonly rootId: string;
  readonly identityId: string;
  readonly incarnation: string;
}
export interface PortablePrivateLock {
  readonly kind: "transport-receipt" | "b6";
  readonly directory: string;
  readonly protocol: 2 | 3;
  readonly sessionPrivate: boolean;
}
export interface PortableRequiredRecord {
  readonly mode: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly volume: PortableVolumeIdentity;
  readonly stateDirectory: string;
  readonly b5File: string;
  readonly b6Directory: string;
  readonly storeId: string;
  /** Existing provisioned custody directory and its exact fixed files, not NEW paths. */
  readonly custodyDirectory: string;
  readonly generationFile: string;
  readonly lockFile: string;
  readonly transport: PortableTransportIdentity;
  readonly locks: readonly PortablePrivateLock[];
  readonly highWater: number;
  /** Opaque backend-native CAS token. No symbol/string/number backend is assumed. */
  readonly revision: unknown;
}
export interface PortableCompletedFence {
  /** Discriminator only. Authenticity still belongs to the actual B1 reader. */
  readonly kind: "b1-fence";
  readonly completed: boolean;
  readonly receivingPodUid: string;
  readonly attachment: string;
  readonly volume: PortableVolumeIdentity;
  /** Positive B1 supersession only, not a lease/observation timeout. */
  readonly signal: AbortSignal;
  assertCurrent(): void;
}
export interface PortableNativeCustody {
  readonly signal: AbortSignal;
  /** The actual held native handle's tuple, not a second epoch minted by this module. */
  readonly nativeTuple: object;
  /** Must synchronously validate the actual pinned native handle AND mounted attachment. */
  assertCurrent(): void;
  close(): void;
}
export interface PortableBorrowedLease {
  readonly lease: TransportHostLease;
  readonly tuple: PortableTransportIdentity & { readonly localStateDir: string };
}
export type PortableIdentityCasReply =
  | { readonly status: "confirmed"; readonly snapshot: PortableRequiredRecord }
  | { readonly status: "conflict" | "unknown" };
export interface PortablePreOpenInput {
  readonly bootstrap: {
    readonly identityKey: string;
    readonly sessionId: string;
    readonly sessionFile: string;
    readonly volume: PortableVolumeIdentity;
    readonly self: { readonly podUid: string; readonly attachment: string };
    readonly generationFile: string;
    readonly lockFile: string;
    readonly signal: AbortSignal;
    readonly deadlineMs: number;
  };
  readonly identityRecord: {
    /** Authoritative off-volume read of the EXISTING provisioned record. */
    readRequired(key: string, signal: AbortSignal): Promise<PortableRequiredRecord | undefined>;
    /** Native conditional update of this same record, preserving its other identity fields. */
    casHighWater(snapshot: PortableRequiredRecord, next: number, signal: AbortSignal): Promise<PortableIdentityCasReply>;
  };
  readonly b1: {
    /** Return only the controller-verified, completed, current attachment capability.
     * Its predecessor set MUST cover ALL actual PVC attachment writers since the last
     * receipt (including failed receivers), not a candidate list. B1 must first kill all
     * tasks, durably revoke storage, and confirm exit; A2 requires deletion or durable
     * restart suppression BEFORE confirming exit, surviving remount/reattach attempts.
     * API deletion, NotReady, timeout detach, boot changes and TTL are not completion.
     * These are the port's obligations, not an invented `allFenced: true` field.
     */
    readCompletedForSelf(required: PortableRequiredRecord, self: PortablePreOpenInput["bootstrap"]["self"], signal: AbortSignal): Promise<PortableCompletedFence | undefined>;
  };
  readonly nativeCustody: {
    /** Read the ACTUAL mounted volume/attachment, independently of bootstrap and B1 DTOs. */
    readMountedAtOpen(signal: AbortSignal): Promise<{ readonly volume: PortableVolumeIdentity; readonly attachment: string }>;
    hold(required: PortableRequiredRecord, b1: PortableCompletedFence, signal: AbortSignal): Promise<PortableNativeCustody>;
  };
  readonly leaseOwner: {
    /** The trusted host's sole renewal owner constructs/acquires ONE real lease using
     * exactly required.transport/stateDirectory and this startupSuccessor. It retains
     * renewal ownership through rebinds. The returned frozen tuple attests its actual
     * constructor inputs (the existing lease keeps most of them private).
     * Borrowers must NOT acquire/renew/release independently. The renewal owner must
     * stop its cadence on this lease's release signal. Ports must settle native
     * acquisition/driver work on cancellation; timeout never proves native join.
     */
    acquireOnce(options: { required: PortableRequiredRecord; startupSuccessor: TransportHostLeaseStartupSuccessor; signal: AbortSignal }): Promise<PortableBorrowedLease>;
  };
  /** Optional ACTUAL post-open/header/native identity adapter. Missing means refusal;
   * never derive physical predecessor Pod/boot identities from logical hostId. */
  readonly identity?: PortableIdentityAdapter;
}
export interface PortablePreOpenBinding {
  readonly preOpen: RecoveryPreOpenBinding;
  readonly identity: PortableIdentityAdapter;
  borrowLease(): PortableBorrowedLease;
  closeAfterNativeJoin(joined: Promise<void>): Promise<void>;
}

/** Host-free classification only, not an SDK capability or custody proof. */
class RequiredHistoryIncompleteError extends Error {
  readonly code = "RECOVERY_HISTORY_INCOMPLETE";
}
const refused = (message: string, cause?: unknown): Error =>
  cause instanceof RequiredHistoryIncompleteError
    ? new RequiredHistoryIncompleteError(`Portable pre-open refused: ${message}`, { cause })
    : new Error(`Portable pre-open refused: ${message}`, { cause });
const digest = (bytes: string | Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const text = (value: unknown): value is string => typeof value === "string" && value.length > 0 &&
  value.length <= 4096 && !/[\x00-\x1f\x7f]/.test(value);
const exactPath = (value: unknown): value is string => text(value) && path.isAbsolute(value) &&
  value !== path.parse(value).root && !value.endsWith(path.sep) && path.normalize(value) === value;
const sameVolume = (a: PortableVolumeIdentity, b: PortableVolumeIdentity): boolean =>
  a?.pvcUid === b?.pvcUid && a?.csiHandle === b?.csiHandle && a?.filesystemUuid === b?.filesystemUuid;
const validVolume = (v: PortableVolumeIdentity): boolean => !!v && [v.pvcUid, v.csiHandle, v.filesystemUuid].every(text);
const transportKeys = ["machine", "hostId", "rootId", "identityId", "incarnation"] as const;
const sameTransport = (a: PortableTransportIdentity, b: PortableTransportIdentity): boolean =>
  !!a && !!b && transportKeys.every(key => a[key] === b[key]);
const below = (file: string, root: string): boolean => file === root || file.startsWith(`${root}${path.sep}`);
const receiptPath = (r: PortableRequiredRecord): string => path.join(r.stateDirectory,
  `transport-host-lease-${digest(r.transport.hostId).slice(0, 32)}.json`);
const isSignal = (value: AbortSignal): boolean => !!value && typeof value.throwIfAborted === "function" &&
  typeof value.addEventListener === "function";

/** No retained-path IO here: (a) must finish even before a write-capable open. */
function checkedRequired(r: PortableRequiredRecord | undefined, b: PortablePreOpenInput["bootstrap"]): PortableRequiredRecord {
  if (!r || r.mode !== "REQUIRED" || r.sessionId !== b.sessionId || r.sessionFile !== b.sessionFile ||
    !validVolume(r.volume) || !sameVolume(r.volume, b.volume) || !text(r.storeId) ||
    ![r.sessionFile, r.stateDirectory, r.b5File, r.b6Directory].every(exactPath) ||
    !r.transport || !transportKeys.every(key => text(r.transport[key])) ||
    !/^[a-z0-9][a-z0-9-]{0,31}$/.test(r.transport.machine) ||
    !Number.isSafeInteger(r.highWater) || r.highWater < 0 || !Object.hasOwn(r, "revision")) {
    throw refused("off-volume REQUIRED/session/path/volume/transport identity is missing or contradictory");
  }
  if (![r.custodyDirectory, r.generationFile, r.lockFile].every(exactPath) ||
    r.generationFile !== path.join(r.custodyDirectory, "custody.gen") ||
    r.lockFile !== path.join(r.custodyDirectory, "custody.ofd") ||
    b.generationFile !== r.generationFile || b.lockFile !== r.lockFile) {
    throw new RequiredHistoryIncompleteError("Portable pre-open refused: REQUIRED custody paths are missing or misrouted");
  }
  const files = [r.sessionFile, r.b5File, r.generationFile, r.lockFile];
  if (new Set(files).size !== files.length || below(r.stateDirectory, r.b6Directory) || below(r.b6Directory, r.stateDirectory) ||
    files.some(file => below(file, r.stateDirectory) || below(file, r.b6Directory)) ||
    files.some((file, i) => files.some((other, j) => i !== j && below(file, other)))) {
    throw refused("retained private roots and fixed files overlap");
  }
  const allowed = {
    "transport-receipt": { directory: path.join(`${receiptPath(r)}.custody`, "custody.lock"), protocol: 2 },
    b6: { directory: path.join(r.b6Directory, "custody.lock"), protocol: 3 },
  } as const;
  if (!Array.isArray(r.locks) || r.locks.length !== 2 || new Set(r.locks.map(lock => lock.kind)).size !== 2 ||
    r.locks.some((lock: PortablePrivateLock) => !lock || !Object.hasOwn(allowed, lock.kind) || !lock.sessionPrivate ||
      !exactPath(lock.directory) || lock.directory !== allowed[lock.kind].directory || lock.protocol !== allowed[lock.kind].protocol)) {
    throw refused("lock manifest is not the exact private protocol-2 receipt/protocol-3 B6 inventory");
  }
  // Copy the closed projection, retaining the opaque native revision unchanged.
  return Object.freeze({ mode: r.mode, sessionId: r.sessionId, sessionFile: r.sessionFile,
    volume: Object.freeze({ pvcUid: r.volume.pvcUid, csiHandle: r.volume.csiHandle, filesystemUuid: r.volume.filesystemUuid }),
    stateDirectory: r.stateDirectory, b5File: r.b5File, b6Directory: r.b6Directory, storeId: r.storeId,
    custodyDirectory: r.custodyDirectory, generationFile: r.generationFile, lockFile: r.lockFile,
    transport: Object.freeze({ machine: r.transport.machine, hostId: r.transport.hostId, rootId: r.transport.rootId,
      identityId: r.transport.identityId, incarnation: r.transport.incarnation }),
    locks: Object.freeze(r.locks.map(lock => Object.freeze({ ...lock }))), highWater: r.highWater, revision: r.revision });
}
function sameRequired(a: PortableRequiredRecord, b: PortableRequiredRecord): boolean {
  return a.mode === b.mode && a.sessionId === b.sessionId && a.sessionFile === b.sessionFile && sameVolume(a.volume, b.volume) &&
    a.stateDirectory === b.stateDirectory && a.b5File === b.b5File && a.b6Directory === b.b6Directory && a.storeId === b.storeId &&
    a.custodyDirectory === b.custodyDirectory && a.generationFile === b.generationFile && a.lockFile === b.lockFile &&
    sameTransport(a.transport, b.transport) && a.locks.length === b.locks.length && a.locks.every((lock, i) => {
      const other = b.locks[i]!;
      return lock.kind === other.kind && lock.directory === other.directory && lock.protocol === other.protocol && lock.sessionPrivate === other.sessionPrivate;
    });
}

// All directory traversal is descriptor-relative, nofollow, Linux host-only. Keep the
// complete original parent route pinned and check it again at each effect boundary.
const directoryFlags = fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW;
const readFlags = fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK;
const metadataFlags = 0x200000 | fs.constants.O_NOFOLLOW; // Linux O_PATH, never content access.
const fdPath = (fd: number): string => `/proc/self/fd/${fd}`;
const inode = (a: fs.BigIntStats, b: fs.BigIntStats): boolean => a.dev === b.dev && a.ino === b.ino;
/** Actual kernel mount identity; callers cannot provide or override it. */
function mountId(fd: number): string {
  const stat = fs.fstatSync(fd, { bigint: true });
  const info = fs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8");
  const mount = /^mnt_id:\s+([1-9][0-9]*)$/m.exec(info)?.[1];
  const ino = /^ino:\s+([0-9]+)$/m.exec(info)?.[1];
  if (info.length > 16_384 || !mount || !ino || BigInt(ino) !== stat.ino) throw refused("unverifiable metadata mount descriptor");
  return mount;
}
interface DirectoryRoute {
  readonly fd: number;
  at(name: string): string;
  assertCurrent(): void;
  close(): void;
}
function pinDirectory(directory: string): DirectoryRoute {
  if (process.platform !== "linux" || !path.isAbsolute(directory) || path.normalize(directory) !== directory) {
    throw refused("descriptor-relative nofollow custody requires a canonical Linux route");
  }
  const pins: { file: string; fd: number; stat: fs.BigIntStats; mount: string }[] = [];
  const close = (): void => { for (const pin of pins.splice(0).reverse()) fs.closeSync(pin.fd); };
  try {
    const parts = directory.split(path.sep).filter(Boolean);
    let file = path.parse(directory).root;
    const open = (name: string, target: string): void => {
      const fd = fs.openSync(target, directoryFlags);
      try {
        const stat = fs.fstatSync(fd, { bigint: true });
        const atName = fs.lstatSync(name, { bigint: true });
        if (!stat.isDirectory() || atName.isSymbolicLink() || !inode(stat, atName)) throw refused("directory route changed during pin");
        pins.push({ file: name, fd, stat, mount: mountId(fd) });
      } catch (error) { fs.closeSync(fd); throw error; }
    };
    open(file, file);
    for (const part of parts) {
      const parent = pins.at(-1)!;
      file = path.join(file, part);
      open(file, path.join(fdPath(parent.fd), part));
    }
    const fd = pins.at(-1)!.fd;
    const route: DirectoryRoute = {
      fd,
      at(name) {
        if (!text(name) || name === "." || name === ".." || path.basename(name) !== name) throw refused("invalid descriptor-relative basename");
        return path.join(fdPath(fd), name);
      },
      assertCurrent() {
        if (!pins.length) throw refused("directory route closed");
        let previous: number | undefined;
        try {
          for (const pin of pins) {
            const fresh: number = fs.openSync(previous === undefined ? pin.file : path.join(fdPath(previous), path.basename(pin.file)), directoryFlags);
            try {
              const named = fs.fstatSync(fresh, { bigint: true });
              if (!named.isDirectory() || !inode(pin.stat, named) || mountId(fresh) !== pin.mount ||
                !inode(pin.stat, fs.fstatSync(pin.fd, { bigint: true })) || mountId(pin.fd) !== pin.mount) {
                throw refused("pinned parent inode or mount route replaced");
              }
            } catch (error) { fs.closeSync(fresh); throw error; }
            if (previous !== undefined) fs.closeSync(previous);
            previous = fresh;
          }
        } finally { if (previous !== undefined) fs.closeSync(previous); }
      },
      close,
    };
    route.assertCurrent();
    return route;
  } catch (error) { close(); throw error; }
}
interface PinnedFile {
  readonly fd: number;
  readonly stat: fs.BigIntStats;
  readonly bytes: Buffer;
  assertAt(file: string): void;
  close(): void;
}
function pinFile(file: string, maxBytes: number, expectedMount: string): PinnedFile {
  const fd = fs.openSync(file, readFlags);
  try {
    const stat = fs.fstatSync(fd, { bigint: true });
    if (!stat.isFile() || stat.nlink !== 1n || stat.size < 0n || stat.size > BigInt(maxBytes) || mountId(fd) !== expectedMount) throw refused("unsafe retained metadata file or mount");
    const bytes = fs.readFileSync(fd);
    if (bytes.length !== Number(stat.size)) throw refused("metadata size changed during read");
    let closed = false;
    const pinned: PinnedFile = { fd, stat, bytes,
      assertAt(name) {
        if (closed) throw refused("metadata pin closed");
        const namedFd = fs.openSync(name, metadataFlags);
        try {
          const current = fs.fstatSync(fd, { bigint: true }), named = fs.fstatSync(namedFd, { bigint: true });
          if (!current.isFile() || !named.isFile() || current.nlink !== 1n || named.nlink !== 1n ||
            mountId(fd) !== expectedMount || mountId(namedFd) !== expectedMount ||
            !inode(stat, current) || !inode(stat, named) || current.size !== BigInt(bytes.length) || named.size !== current.size ||
            !readExactly(fd, bytes.length).equals(bytes)) throw refused("metadata inode/raw bytes changed");
        } finally { fs.closeSync(namedFd); }
      },
      close() { if (!closed) { closed = true; fs.closeSync(fd); } },
    };
    pinned.assertAt(file);
    return pinned;
  } catch (error) { fs.closeSync(fd); throw error; }
}
function readExactly(fd: number, length: number): Buffer {
  const bytes = Buffer.alloc(length + 1);
  let offset = 0;
  while (offset < bytes.length) {
    const count = fs.readSync(fd, bytes, offset, bytes.length - offset, offset);
    if (!count) break;
    offset += count;
  }
  return bytes.subarray(0, offset);
}
function writeAll(fd: number, bytes: Buffer): void {
  let offset = 0;
  while (offset < bytes.length) {
    const count = fs.writeSync(fd, bytes, offset, bytes.length - offset, offset);
    if (count <= 0) throw refused("metadata write made no progress");
    offset += count;
  }
}

/** REQUIRED presence is not provisioning. Before B1/OFD, inspect only nofollow
 * routes and file metadata; the immutable B6 identity bytes are read only under
 * the guard. B5/native files may be atomically rewritten by their actual owners:
 * check current safe existence, never pin their old inode or interpret B5 JSON. */
interface RequiredStores {
  assertCurrent(): void;
  validateGuardedIdentity(): void;
  close(): void;
}
function checkRequiredStores<T>(check: () => T): T {
  try { return check(); }
  catch (cause) {
    if (cause instanceof RequiredHistoryIncompleteError) throw cause;
    throw new RequiredHistoryIncompleteError("Portable pre-open refused: required retained stores are not current", { cause });
  }
}
function pinRequiredStores(required: PortableRequiredRecord): RequiredStores {
  const routes = new Map<string, DirectoryRoute>();
  let identity: PinnedFile | undefined;
  const close = (): void => {
    try { identity?.close(); } finally { for (const route of [...routes.values()].reverse()) route.close(); routes.clear(); }
  };
  try {
    const route = (directory: string): DirectoryRoute => {
      let held = routes.get(directory);
      if (!held) { held = pinDirectory(directory); routes.set(directory, held); }
      return held;
    };
    const session = route(path.dirname(required.sessionFile)), expectedMount = mountId(session.fd);
    const sameMount = (held: DirectoryRoute): void => {
      if (mountId(held.fd) !== expectedMount) throw refused("required retained directory is not on the bound session mount");
    };
    // REQUIRED roots are compared before even looking up their leaves. Descriptor
    // stability alone is not equality to the B1-fenced session's actual mount.
    const files = [required.sessionFile, required.b5File, receiptPath(required)].map(file => {
      const parent = route(path.dirname(file)); sameMount(parent);
      return { parent, name: path.basename(file) };
    });
    const b6 = route(required.b6Directory); sameMount(b6);
    const transport = route(`${receiptPath(required)}.custody`); sameMount(transport);
    // recovery-journal.ts's actual immutable identity, not a new seal or frame.
    const identityName = b6.at("journal.identity");
    const regular = (file: string): fs.BigIntStats => {
      const fd = fs.openSync(file, metadataFlags);
      try {
        const stat = fs.fstatSync(fd, { bigint: true });
        if (!stat.isFile() || stat.nlink !== 1n || mountId(fd) !== expectedMount) throw refused("required retained file is unsafe or not on the bound session mount");
        return stat;
      } finally { fs.closeSync(fd); }
    };
    const privateLock = (parent: DirectoryRoute): void => {
      const name = parent.at("custody.lock");
      if (fs.lstatSync(name, { throwIfNoEntry: false }) === undefined) return;
      const fd = fs.openSync(name, directoryFlags);
      try {
        if (mountId(fd) !== expectedMount) throw refused("required private lock is not on the bound session mount");
        const owner = path.join(fdPath(fd), "owner");
        if (fs.lstatSync(owner, { throwIfNoEntry: false }) !== undefined) regular(owner);
      } finally { fs.closeSync(fd); }
    };
    const initialIdentity = regular(identityName);
    const stores: RequiredStores = {
      assertCurrent() {
        checkRequiredStores(() => {
          for (const held of routes.values()) { held.assertCurrent(); sameMount(held); }
          for (const file of files) regular(file.parent.at(file.name));
          if (!inode(initialIdentity, regular(identityName))) throw refused("required B6 store identity replaced");
          // Metadata-only inventory of the existing journal's exact leaf classes.
          // A leaf bind mount can differ even when its directory/dev stays the same.
          for (const name of fs.readdirSync(fdPath(b6.fd))) {
            if (name.startsWith("raw-") || name.startsWith("commit-")) regular(b6.at(name));
          }
          privateLock(b6); privateLock(transport);
          identity?.assertAt(identityName);
        });
      },
      validateGuardedIdentity() {
        checkRequiredStores(() => {
          stores.assertCurrent();
          const retained = pinFile(identityName, 64 * 1024, expectedMount);
          try {
            // Exact existing RecoveryJournal version/storeId encoding; zero frames
            // is valid. Full journal recovery/foreign-owner admission remains later.
            if (!retained.bytes.equals(Buffer.from(`${JSON.stringify({ version: 1, storeId: required.storeId })}\n`)) ||
              !inode(initialIdentity, retained.stat)) throw refused("required B6 store identity is incomplete or contradictory");
            retained.assertAt(identityName); identity = retained;
          } catch (error) { retained.close(); throw error; }
          stores.assertCurrent();
        });
      },
      close,
    };
    stores.assertCurrent();
    return stores;
  } catch (cause) {
    close();
    if (cause instanceof RequiredHistoryIncompleteError) throw cause;
    throw new RequiredHistoryIncompleteError("Portable pre-open refused: required retained store preflight failed", { cause });
  }
}

/** Linux O_PATH inspects an existing inode/mount without content access, FIFO
 * blocking, OFD acquisition or creation. O_NOFOLLOW rejects symlink metadata.
 * The authoritative session path anchors the actual mount; st_dev alone misses
 * same-device bind mounts. The native attachment reader still authenticates its
 * volume/attachment, not this metadata helper or any caller-provided stat DTO. */
function pinCustodyPaths(required: PortableRequiredRecord): RequiredStores {
  const routes: DirectoryRoute[] = [];
  const pins: { fd: number; stat: fs.BigIntStats; mount: string; name: string }[] = [];
  const close = (): void => {
    for (const pin of pins.splice(0).reverse()) fs.closeSync(pin.fd);
    for (const route of routes.splice(0).reverse()) route.close();
  };
  try {
    const session = pinDirectory(path.dirname(required.sessionFile)); routes.push(session);
    const custody = pinDirectory(required.custodyDirectory); routes.push(custody);
    const expectedMount = mountId(session.fd);
    const regular = (fd: number): fs.BigIntStats => {
      const stat = fs.fstatSync(fd, { bigint: true });
      if (!stat.isFile() || stat.nlink !== 1n || mountId(fd) !== expectedMount) throw refused("REQUIRED file is not on the bound session mount");
      return stat;
    };
    const sessionCurrent = (): void => {
      const fd = fs.openSync(session.at(path.basename(required.sessionFile)), metadataFlags);
      try { regular(fd); } finally { fs.closeSync(fd); }
    };
    if (mountId(custody.fd) !== expectedMount) throw refused("REQUIRED custody directory is not on the bound session mount");
    sessionCurrent();
    for (const file of [required.generationFile, required.lockFile]) {
      const name = path.basename(file), fd = fs.openSync(custody.at(name), metadataFlags);
      try { pins.push({ fd, stat: regular(fd), mount: mountId(fd), name }); }
      catch (error) { fs.closeSync(fd); throw error; }
    }
    const held: RequiredStores = {
      assertCurrent() {
        checkRequiredStores(() => {
          for (const route of routes) route.assertCurrent();
          if (mountId(session.fd) !== expectedMount || mountId(custody.fd) !== expectedMount) throw refused("bound custody mount changed");
          sessionCurrent();
          for (const pin of pins) {
            const current = fs.openSync(custody.at(pin.name), metadataFlags);
            try {
              if (!inode(pin.stat, regular(pin.fd)) || !inode(pin.stat, regular(current)) ||
                mountId(pin.fd) !== pin.mount || mountId(current) !== pin.mount) throw refused("bound custody file replaced");
            } finally { fs.closeSync(current); }
          }
        });
      },
      validateGuardedIdentity() { held.assertCurrent(); },
      close,
    };
    held.assertCurrent(); return held;
  } catch (cause) {
    close();
    if (cause instanceof RequiredHistoryIncompleteError) throw cause;
    throw new RequiredHistoryIncompleteError("Portable pre-open refused: REQUIRED custody mount/path preflight failed", { cause });
  }
}

/** REQUIRED never provisions the guard, even if it disappears at the open syscall.
 * Reuse the immutable kernel implementation's route/ABI/description validators,
 * but open its already-provisioned fixed file WITHOUT O_CREAT. The public optional
 * primitive below retains its original create behavior and exact frozen bytes. */
async function acquireRequiredVolumeGuard(file: string): Promise<{ assertCurrent(): void; close(): void }> {
  const uid = process.getuid?.();
  if (process.platform !== "linux" || !["x64", "arm64"].includes(process.arch) || uid === undefined) throw ofdFailure("UNSUPPORTED");
  const route = ofdOpenRoute(file, false, uid);
  try {
    const fd = route.ofdFds.at(-1), initial = route.ofdDescriptors.at(-1);
    if (fd === undefined || !initial) throw ofdFailure("ROUTE_UNSAFE");
    const result = ofdSpawnSync("/usr/bin/python3", ["-I", "-B", "-c", ofdAcquireProgram], {
      stdio: ["ignore", "pipe", "pipe", fd], env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C" },
      encoding: "utf8", timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 4_096,
    });
    if (result.error || result.signal !== null || result.status !== 0 || result.stdout !== "portable-ofd-v1\n" || result.stderr !== "") throw ofdFailure("ACQUIRE_FAILED");
    const locked = ofdInspect(fd); ofdValidateFile(locked, uid);
    if (!ofdSameIdentity(initial, locked) || !ofdHasWholeFileLock(locked)) throw ofdFailure("LOCK_NOT_HELD");
    fs.fsyncSync(fd);
    const parent = route.ofdFds.at(-2);
    if (parent === undefined) throw ofdFailure("ROUTE_UNSAFE");
    fs.fsyncSync(parent);
    let closed = false, invalid = false;
    const guard = {
      assertCurrent(): void {
        if (closed || invalid) throw ofdFailure("NOT_CURRENT");
        try {
          const now = ofdInspect(fd); ofdValidateFile(now, uid);
          if (!ofdSameIdentity(locked, now) || !ofdHasWholeFileLock(now)) throw ofdFailure("LOCK_LOST");
          const current = ofdOpenRoute(file, false, uid);
          try { ofdCompareRoute(route.ofdDescriptors, current.ofdDescriptors); } finally { ofdReleaseRoute(current); }
          const after = ofdInspect(fd);
          if (!ofdSameIdentity(locked, after) || !ofdHasWholeFileLock(after)) throw ofdFailure("LOCK_LOST");
        } catch (cause) { invalid = true; throw cause; }
      },
      close(): void {
        if (closed) return; closed = true;
        try {
          const now = ofdInspect(fd);
          if (ofdSameIdentity(locked, now) && ofdHasWholeFileLock(now)) fs.closeSync(fd);
        } catch { /* Never close an unrelated/reused description. */ }
      },
    };
    guard.assertCurrent(); route.ofdFds.pop(); return guard;
  } finally { ofdReleaseRoute(route); }
}

interface HeldGeneration {
  readonly value: number;
  assertCurrent(): void;
  close(): void;
}
/** (d) A single durable gap; h+1 reconciliation MUST NOT increment again. The
 * resulting inode/bytes and parent route remain pinned across native CAS and open. */
function advanceGeneration(file: string, highWater: number, check: () => void): HeldGeneration {
  const parent = checkRequiredStores(() => pinDirectory(path.dirname(file)));
  let original: PinnedFile | undefined;
  let writable: number | undefined;
  let retained: PinnedFile | undefined;
  let handedOff = false;
  try {
    check(); checkRequiredStores(() => parent.assertCurrent());
    const name = parent.at(path.basename(file)), expectedMount = mountId(parent.fd);
    original = checkRequiredStores(() => pinFile(name, 32, expectedMount));
    const wire = original.bytes.toString("ascii");
    if (!/^(?:0|[1-9][0-9]*)\n$/.test(wire) || !original.bytes.equals(Buffer.from(wire, "ascii"))) throw refused("malformed custody.gen");
    const disk = Number(wire.slice(0, -1));
    if (!Number.isSafeInteger(disk) || disk < highWater || disk > highWater + 1 || highWater >= Number.MAX_SAFE_INTEGER) {
      throw refused("custody.gen is reverted, too far ahead, or exhausted");
    }
    const next = disk === highWater ? highWater + 1 : disk;
    const bytes = Buffer.from(`${next}\n`);
    const originalPin = original;
    check(); checkRequiredStores(() => { parent.assertCurrent(); originalPin.assertAt(name); });
    // No O_CREAT, O_TRUNC, symlink or fresh-session fallback. Classify only the
    // generation metadata operations, never the B1/native/check/CAS/lease ports.
    writable = checkRequiredStores(() => fs.openSync(name, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK));
    const writeFd = writable;
    checkRequiredStores(() => {
      if (!inode(originalPin.stat, fs.fstatSync(writeFd, { bigint: true })) || mountId(writeFd) !== expectedMount) throw refused("generation write handle changed");
    });
    check(); checkRequiredStores(() => { parent.assertCurrent(); originalPin.assertAt(name); });
    if (disk === highWater) { writeAll(writable, bytes); fs.ftruncateSync(writable, bytes.length); }
    fs.fsyncSync(writable);
    fs.fsyncSync(parent.fd);
    check();
    checkRequiredStores(() => {
      parent.assertCurrent();
      const after = fs.lstatSync(name, { bigint: true });
      if (!after.isFile() || after.isSymbolicLink() || after.nlink !== 1n || !inode(originalPin.stat, after) ||
        !inode(after, fs.fstatSync(writeFd, { bigint: true })) || !readExactly(writeFd, bytes.length).equals(bytes)) {
        throw refused("generation durability/readback changed");
      }
    });
    retained = checkRequiredStores(() => pinFile(name, 32, expectedMount));
    if (!retained.bytes.equals(bytes) || !inode(retained.stat, original.stat)) throw new RequiredHistoryIncompleteError("durable generation pin changed");
    const pin = retained;
    let released = false;
    const generation: HeldGeneration = {
      value: next,
      assertCurrent() { checkRequiredStores(() => { parent.assertCurrent(); pin.assertAt(name); }); },
      close() { if (!released) { released = true; try { pin.close(); } finally { parent.close(); } } },
    };
    generation.assertCurrent(); handedOff = true;
    return generation;
  } finally {
    if (writable !== undefined) fs.closeSync(writable);
    original?.close();
    if (!handedOff) { retained?.close(); parent.close(); }
  }
}

/** (a) -> (g), once per trusted host invocation. Runtime rebind borrows this lifetime. */
export function createPortablePreOpenBinding(input: PortablePreOpenInput): PortablePreOpenBinding {
  const b = Object.freeze({ ...input.bootstrap, volume: Object.freeze({ ...input.bootstrap.volume }), self: Object.freeze({ ...input.bootstrap.self }) });
  const actualIdentity = input.identity;
  const startupAbort = new AbortController();
  let startupTimer: ReturnType<typeof setTimeout> | undefined;
  // This is a one-shot bootstrap deadline, never an exclusion/renewal lease. Ports
  // must settle their actual native work on cancellation; timeout is not join proof.
  const operationSignal = AbortSignal.any([b.signal, startupAbort.signal]);
  let required: PortableRequiredRecord | undefined;
  let fence: PortableCompletedFence | undefined;
  let native: PortableNativeCustody | undefined;
  let nativeTuple: object | undefined;
  let ofd: { assertCurrent(): void; close(): void } | undefined;
  let generation: HeldGeneration | undefined;
  let stores: RequiredStores | undefined;
  let custody: RequiredStores | undefined;
  let borrowed: PortableBorrowedLease | undefined;
  let prepared: Promise<{ assertCurrent(): void }> | undefined;
  let ready = false, closed = false, closingRequested = false;
  let failure: Error | undefined, revoked: Error | undefined;
  let closing: Promise<void> | undefined;
  const routes: DirectoryRoute[] = [];
  const listeners: { signal: AbortSignal; listener: () => void }[] = [];
  const latch = (cause: unknown): Error => {
    revoked ??= refused("positive physical/native custody loss (sticky)", cause);
    startupAbort.abort(revoked);
    return revoked;
  };
  const observe = (signal: AbortSignal): void => {
    const listener = (): void => { latch(signal.reason); };
    listeners.push({ signal, listener }); signal.addEventListener("abort", listener, { once: true });
    if (signal.aborted) listener();
  };
  const assertPhysical = (): void => {
    if (closed) throw refused("binding closed after native join");
    if (revoked) throw revoked;
    try {
      fence?.signal.throwIfAborted(); fence?.assertCurrent();
      if (fence && (fence.kind !== "b1-fence" || fence.completed !== true || fence.receivingPodUid !== b.self.podUid ||
        fence.attachment !== b.self.attachment || !sameVolume(fence.volume, b.volume))) {
        throw refused("B1 self/attachment receipt changed");
      }
      native?.signal.throwIfAborted(); native?.assertCurrent();
      if (native && native.nativeTuple !== nativeTuple) throw refused("native tuple replaced without native rebind");
      custody?.assertCurrent();
      ofd?.assertCurrent(); generation?.assertCurrent();
      // Required roots also occur in routes: preserve their specific classification
      // before the redundant generic route check can obscure the store fault.
      stores?.assertCurrent();
      for (const route of routes) route.assertCurrent();
    } catch (cause) { throw latch(cause); }
  };
  const checkStartup = (): void => {
    if (closingRequested) throw refused("host lifetime closed admission during bootstrap");
    operationSignal.throwIfAborted();
    if (!Number.isFinite(b.deadlineMs) || Date.now() >= b.deadlineMs) throw refused("bootstrap deadline elapsed");
    assertPhysical();
  };
  // This public Pi projection intentionally contains EXACTLY assertCurrent. A routing
  // lease's recoverable TTL lapse is NOT physical supersession and is not consulted.
  const current = Object.freeze({ assertCurrent(): void {
    if (!ready || failure) throw failure ?? refused("binding is not openable");
    b.signal.throwIfAborted(); assertPhysical();
  } });
  const checkFence = (value: PortableCompletedFence | undefined): PortableCompletedFence => {
    if (!value || value.kind !== "b1-fence" || value.completed !== true || value.receivingPodUid !== b.self.podUid || value.attachment !== b.self.attachment ||
      !sameVolume(value.volume, required!.volume) || !isSignal(value.signal) || typeof value.assertCurrent !== "function") {
      throw refused("B1 is not completed for self/current volume attachment");
    }
    value.signal.throwIfAborted(); value.assertCurrent();
    return value;
  };
  const checkMounted = async (): Promise<void> => {
    checkStartup();
    const mounted = await input.nativeCustody.readMountedAtOpen(operationSignal);
    checkStartup();
    if (!mounted || !sameVolume(mounted.volume, required!.volume) || !sameVolume(mounted.volume, fence!.volume) ||
      mounted.attachment !== b.self.attachment || mounted.attachment !== fence!.attachment) {
      throw latch(refused("actual mounted volume/attachment differs from REQUIRED and B1"));
    }
  };
  const prepare = async (): Promise<{ assertCurrent(): void }> => {
    if (!text(b.identityKey) || !text(b.sessionId) || !validVolume(b.volume) || !text(b.self.podUid) || !text(b.self.attachment) ||
      ![b.sessionFile, b.generationFile, b.lockFile].every(exactPath) || !isSignal(b.signal)) throw refused("invalid trusted bootstrap");
    checkStartup();
    const remaining = b.deadlineMs - Date.now();
    if (remaining > 2_147_483_647) throw refused("bootstrap deadline is outside timer range");
    startupTimer = setTimeout(() => startupAbort.abort(refused("bootstrap deadline elapsed")), Math.max(1, remaining));
    startupTimer.unref?.();
    // (a): this is the ONLY port invoked before the closed identity checks.
    const initial = await input.identityRecord.readRequired(b.identityKey, operationSignal);
    checkStartup(); required = checkedRequired(initial, b);
    // (a): closed off-volume identity FIRST. No content reads, create/open-new,
    // journal custody acquisition, or retained-path mutation in this preflight.
    custody = pinCustodyPaths(required);
    stores = pinRequiredStores(required);
    checkStartup();
    // (b): controller verification, then independently read the actual mount.
    checkStartup();
    const completed = await input.b1.readCompletedForSelf(required, b.self, operationSignal);
    checkStartup(); fence = checkFence(completed); observe(fence.signal);
    await checkMounted(); checkStartup();
    // (c): the actual native handle is necessary but NEVER substitutes for our real OFD.
    const held = await input.nativeCustody.hold(required, fence, operationSignal);
    native = held; nativeTuple = held?.nativeTuple;
    if (!held || !isSignal(held.signal) || typeof held.assertCurrent !== "function" || typeof held.close !== "function" ||
      !held.nativeTuple || typeof held.nativeTuple !== "object") throw refused("actual native custody handle missing");
    observe(held.signal); checkStartup();
    checkStartup();
    try { ofd = await acquireRequiredVolumeGuard(required.lockFile); }
    catch (cause) {
      // A vanished/replaced REQUIRED path is a history fault; an otherwise intact
      // file's actual OFD/ABI/acquisition failure keeps its unrelated classification.
      custody.assertCurrent(); throw cause;
    }
    checkStartup(); await checkMounted(); checkStartup();
    // Pin the manifest's parent routes without reading native/B5/B6 history.
    for (const directory of new Set([path.dirname(required.sessionFile), path.dirname(required.b5File), required.stateDirectory, required.b6Directory])) {
      checkStartup(); routes.push(pinDirectory(directory)); checkStartup();
    }
    // Recheck the complete route and actual B6 identity under held B1/native/OFD,
    // before generation/CAS, private reclaim, lease or any native history open.
    checkStartup(); stores.validateGuardedIdentity(); checkStartup();
    // (d): durable disk first; conflicts and unknowns NEVER restore/decrement it.
    generation = advanceGeneration(required.generationFile, required.highWater, checkStartup);
    const next = generation.value;
    checkStartup();
    const originalSnapshot = checkedRequired(initial, b);
    if (!sameRequired(required, originalSnapshot) || originalSnapshot.highWater !== required.highWater) {
      throw refused("original native CAS snapshot mutated during bootstrap");
    }
    const reply = await input.identityRecord.casHighWater(initial!, next, operationSignal);
    checkStartup();
    if (!reply || reply.status === "conflict") throw refused("identity native CAS conflict");
    let updated: PortableRequiredRecord | undefined;
    if (reply.status === "confirmed") updated = reply.snapshot;
    else if (reply.status === "unknown") {
      checkStartup(); updated = await input.identityRecord.readRequired(b.identityKey, operationSignal); checkStartup();
    } else throw refused("invalid native CAS outcome");
    const confirmed = checkedRequired(updated, b);
    if (!sameRequired(required, confirmed) || confirmed.highWater !== next) throw refused("native CAS outcome is unresolved or another identity");
    required = confirmed;
    await checkMounted(); checkStartup();
    // (e): no generic force-clear, shared mesh/root reclaim or owner PID inference.
    for (const lock of required.locks) reclaimPortableLock(lock, required, checkStartup);
    checkStartup();
    // (f): the real lease owns exact receipt custody, wait, reservation and create-only KV.
    let predecessorChecked = false;
    const startupSuccessor: TransportHostLeaseStartupSuccessor = Object.freeze({
      async verifyPredecessor(snapshot: TransportHostLeasePredecessorSnapshot, signal: AbortSignal): Promise<void> {
        checkStartup(); signal.throwIfAborted();
        if (!snapshot?.receipt || typeof snapshot.bytes !== "string" || !snapshot.bytes ||
          snapshot.receipt.ownerHostId !== required!.transport.hostId || snapshot.receipt.incarnation === required!.transport.incarnation) {
          throw refused("REQUIRED startup needs an exact different-incarnation predecessor receipt");
        }
        // Compare the ORIGINAL bytes under the existing lease's receipt custody. Do not
        // reserialize, reserve elsewhere, or infer a physical Pod from this logical hostId.
        const parent = pinDirectory(required!.stateDirectory);
        let receipt: PinnedFile | undefined;
        try {
          parent.assertCurrent(); receipt = pinFile(parent.at(path.basename(receiptPath(required!))), 4096, mountId(parent.fd));
          if (!receipt.bytes.equals(Buffer.from(snapshot.bytes, "utf8"))) throw refused("predecessor snapshot differs from exact retained bytes");
          checkStartup(); receipt.assertAt(parent.at(path.basename(receiptPath(required!)))); parent.assertCurrent();
          signal.throwIfAborted(); predecessorChecked = true;
        } finally { receipt?.close(); parent.close(); }
      },
    });
    const acquired = await input.leaseOwner.acquireOnce({ required, startupSuccessor, signal: operationSignal });
    const expectedTuple = Object.freeze({ ...required.transport, localStateDir: required.stateDirectory });
    // The opt-in recovery bundle and extension can contain different class copies.
    // Constructor identity is NOT provenance. The trusted sole leaseOwner supplies the
    // actual instance; verify its immutable creation tuple and existing live-state API,
    // then retain that exact object in this host-owned closure, never a serialized DTO.
    if (!acquired || !acquired.lease || !acquired.tuple || !Object.isFrozen(acquired.tuple) ||
      Object.keys(acquired.tuple).length !== transportKeys.length + 1 ||
      !sameTransport(acquired.tuple, expectedTuple) || acquired.tuple.localStateDir !== expectedTuple.localStateDir ||
      acquired.lease.self?.machine !== expectedTuple.machine || acquired.lease.self?.hostId !== expectedTuple.hostId ||
      acquired.lease.key !== `${expectedTuple.machine}.h.${digest(expectedTuple.hostId).slice(0, 32)}` ||
      acquired.lease.owner !== `${expectedTuple.hostId}#${expectedTuple.incarnation}` ||
      !isSignal(acquired.lease.signal) || ["live", "remainingMs", "recovering", "release", "renew"].some(key =>
        typeof acquired.lease[key as "live" | "remainingMs" | "recovering" | "release" | "renew"] !== "function")) {
      throw refused("lease owner returned a different instance/immutable creation tuple; not ours to release");
    }
    // From this point cleanup may act on ONLY this exact owned instance, after join.
    borrowed = Object.freeze({ lease: acquired.lease, tuple: acquired.tuple });
    checkStartup(); acquired.lease.signal.throwIfAborted();
    if (!predecessorChecked || !Number.isSafeInteger(acquired.lease.revision) || acquired.lease.revision <= 0 ||
      !acquired.lease.live() || acquired.lease.remainingMs() <= 0 || acquired.lease.recovering() || acquired.lease.rootRecheck) {
      throw refused("the existing real lease has not completed live acquisition");
    }
    // (g): retain all physical resources until actual native join, not TTL/failed open.
    await checkMounted(); checkStartup();
    // Initial admission needs a live acquired lease at the FINAL await boundary.
    // Later TTL suspension remains recoverable routing admission, never revocation
    // of the returned physical/native assertion or an automatic guard release.
    acquired.lease.signal.throwIfAborted();
    if (!acquired.lease.live() || acquired.lease.recovering() || acquired.lease.rootRecheck) {
      throw refused("transport lease is not live/root-checked at pre-open return");
    }
    ready = true; current.assertCurrent(); return current;
  };
  const requireReady = (): PortableRequiredRecord => { current.assertCurrent(); return required!; };
  const checkProjection = (projection: PortableSessionBinding): void => {
    const r = requireReady();
    if (!projection || projection.sessionFile !== r.sessionFile || projection.stateDirectory !== r.stateDirectory ||
      projection.transportMachine !== r.transport.machine || projection.evidence?.sessionId !== r.sessionId ||
      projection.evidence?.successorPodId !== b.self.podUid || projection.evidence?.exclusionProof?.kind !== "b1-fence") {
      throw refused("actual post-open identity adapter returned another binding");
    }
  };
  const identity: PortableIdentityAdapter = Object.freeze({
    async validate(context: Parameters<PortableIdentityAdapter["validate"]>[0]) {
      requireReady();
      if (!actualIdentity) throw refused("PORTABLE_IDENTITY_ADAPTER_REQUIRED: real post-open/header adapter not supplied");
      const projection = await actualIdentity.validate(context);
      requireReady(); checkProjection(projection); return projection;
    },
    async verifyPredecessor(binding: PortableSessionBinding, snapshot: TransportHostLeasePredecessorSnapshot, signal: AbortSignal) {
      requireReady(); signal.throwIfAborted(); checkProjection(binding);
      if (!actualIdentity) throw refused("PORTABLE_IDENTITY_ADAPTER_REQUIRED: physical predecessor projection unavailable");
      await actualIdentity.verifyPredecessor(binding, snapshot, signal);
      signal.throwIfAborted(); requireReady(); checkProjection(binding);
    },
  });
  return Object.freeze({
    preOpen: Object.freeze({ excludeWriter(request: { sessionFile: string }): Promise<{ assertCurrent(): void }> {
      if (!request || request.sessionFile !== b.sessionFile) return Promise.reject(refused("contradictory exact session target"));
      if (closingRequested || closed) return Promise.reject(refused("host lifetime is closing"));
      if (failure) return Promise.reject(failure);
      if (!prepared) prepared = prepare()
        .catch(cause => { failure = refused("pre-open attempt failed; no automatic retry/admission", cause); throw failure; })
        .finally(() => { if (startupTimer !== undefined) clearTimeout(startupTimer); startupTimer = undefined; });
      return prepared.then(handle => { handle.assertCurrent(); return handle; });
    } }),
    identity,
    borrowLease() { requireReady(); return borrowed!; },
    closeAfterNativeJoin(joined: Promise<void>): Promise<void> {
      if (closed) return closing ?? Promise.resolve();
      if (closing) return closing;
      closingRequested = true;
      closing = (async () => {
        // A rejected/cancelled wait is NOT positive join. Retain custody in that case.
        await joined;
        try { await prepared; } catch { /* Failed startup can still hold resources. */ }
        closed = true; ready = false;
        try { if (borrowed?.lease) await borrowed.lease.release(); }
        finally {
          for (const { signal, listener } of listeners.splice(0)) signal.removeEventListener("abort", listener);
          try { native?.close(); } finally {
            try { ofd?.close(); } finally {
              try { generation?.close(); } finally {
                try { stores?.close(); } finally {
                  try { custody?.close(); } finally { for (const route of routes.splice(0).reverse()) route.close(); }
                }
              }
            }
          }
        }
      })();
      void closing.catch(() => { if (!closed) closing = undefined; });
      return closing;
    },
  });
}

/** Closed wire of the two actual existing custody-lock publishers; no PID-death test. */
function validPrivateOwner(bytes: Buffer, protocol: 2 | 3): boolean {
  const wire = bytes.toString("utf8");
  if (!bytes.equals(Buffer.from(wire, "utf8")) || !wire.endsWith("\n")) return false;
  const fields = wire.split("\n");
  const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
  const positive = (value: string | undefined): boolean => !!value && /^[1-9][0-9]*$/.test(value) && Number.isSafeInteger(Number(value));
  const machine = fields[4] ?? "";
  if (fields.length !== (protocol === 2 ? 7 : 8) || fields.at(-1) !== "" || !uuid.test(fields[0] ?? "") ||
    !positive(fields[1]) || !positive(fields[2]) || !/^[0-9]+$/.test(fields[3] ?? "") ||
    !(/^[0-9a-f]{32}$/.test(machine) || /^host:[^\s\x00-\x1f]+$/.test(machine)) || !uuid.test(fields[5] ?? "")) return false;
  // The publisher's namespace boot UUID is the full 8-4-4-4-12 wire, not a prefix.
  if (protocol === 3 && !/^linux:(?:[0-9a-f]{32}|-):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}:[0-9]+$/.test(fields[6] ?? "")) return false;
  return true;
}

/** Create only OUR immutable reclaim receipt; never rewrite an existing result. */
function retainReceipt(parent: DirectoryRoute, name: string, bytes: Buffer): void {
  parent.assertCurrent();
  let fd: number;
  try { fd = fs.openSync(parent.at(name), fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600); }
  catch (error) {
    if ((error as NodeJS.ErrnoException)?.code !== "EEXIST") throw error;
    const retained = pinFile(parent.at(name), 64 * 1024, mountId(parent.fd));
    try {
      if (!retained.bytes.equals(bytes)) throw refused("retained reclaim receipt identity conflict");
      retained.assertAt(parent.at(name)); fs.fsyncSync(retained.fd); fs.fsyncSync(parent.fd);
      parent.assertCurrent(); retained.assertAt(parent.at(name));
    } finally { retained.close(); }
    return;
  }
  try {
    const expected = fs.fstatSync(fd, { bigint: true });
    writeAll(fd, bytes); fs.fsyncSync(fd); fs.fsyncSync(parent.fd);
    const readback = pinFile(parent.at(name), 64 * 1024, mountId(parent.fd));
    try {
      if (!inode(expected, readback.stat) || !readback.bytes.equals(bytes)) throw refused("new reclaim receipt publication changed");
      parent.assertCurrent(); readback.assertAt(parent.at(name));
    } finally { readback.close(); }
  } finally { fs.closeSync(fd); }
}

/** (e) Only this invocation's positively B1+OFD-fenced private owners. B1 covers ALL
 * earlier PVC writers, including failed receivers: no new candidate election/list here.
 * The target stays nonempty forever, making a stale replay unable to overwrite it.
 */
function reclaimPortableLock(lock: PortablePrivateLock, required: PortableRequiredRecord, check: () => void): void {
  check();
  const parent = pinDirectory(path.dirname(lock.directory));
  let directory: number | undefined, owner: PinnedFile | undefined;
  try {
    check(); parent.assertCurrent();
    const name = path.basename(lock.directory), source = parent.at(name);
    finishRetainedReclaims(parent, lock, required, check);
    check(); parent.assertCurrent();
    // Exact absence is acceptable for a private lock, not for REQUIRED history/receipt.
    if (fs.lstatSync(source, { throwIfNoEntry: false }) === undefined) return;
    directory = fs.openSync(source, directoryFlags);
    const dirStat = fs.fstatSync(directory, { bigint: true });
    if (!dirStat.isDirectory()) throw refused("private custody lock is not a directory");
    owner = pinFile(path.join(fdPath(directory), "owner"), 16 * 1024, mountId(parent.fd));
    if (!validPrivateOwner(owner.bytes, lock.protocol)) throw refused("private custody lock has an unsupported/malformed exact owner wire");
    const ownerStat = owner.stat;
    const identity = `${dirStat.dev}:${dirStat.ino}:${ownerStat.dev}:${ownerStat.ino}:${digest(owner.bytes)}`;
    const targetName = `${name}.portable-evidence.${digest(identity)}`;
    const target = parent.at(targetName);
    const verify = (at: string): void => {
      parent.assertCurrent();
      const named = fs.lstatSync(at, { bigint: true });
      if (!named.isDirectory() || named.isSymbolicLink() || !inode(dirStat, named) ||
        !inode(dirStat, fs.fstatSync(directory!, { bigint: true }))) throw refused("reclaim directory inode changed");
      owner!.assertAt(path.join(fdPath(directory!), "owner"));
      owner!.assertAt(path.join(at, "owner"));
    };
    const receipt = Buffer.from(`${JSON.stringify({ version: 1, kind: lock.kind, protocol: lock.protocol,
      sessionId: required.sessionId, canonical: lock.directory, evidence: path.join(path.dirname(lock.directory), targetName),
      directory: { dev: String(dirStat.dev), ino: String(dirStat.ino) },
      owner: { dev: String(ownerStat.dev), ino: String(ownerStat.ino), bytesBase64: owner.bytes.toString("base64"), sha256: digest(owner.bytes) },
      volume: required.volume })}\n`);
    check(); verify(source);
    // Durable intent retains the originally judged raw bytes even if the final race
    // changes the moved owner. This is a narrowly scoped reclaim receipt, not a log.
    retainReceipt(parent, `${targetName}.reclaim.json`, receipt);
    if (fs.lstatSync(target, { throwIfNoEntry: false }) !== undefined) throw refused("retained evidence target already exists; never overwrite it");
    check(); verify(source); // IMMEDIATELY before rename, both inode pins AND raw bytes.
    fs.renameSync(source, target); // Existing nonempty target refuses atomically; no force/fallback.
    // Immediately inspect what actually moved, not just the pre-rename observation.
    // On ANY mismatch keep the evidence AND any newly published canonical successor.
    let mismatch: unknown;
    try {
      verify(target);
      if (fs.lstatSync(source, { throwIfNoEntry: false }) !== undefined) throw refused("canonical successor appeared during reclaim");
      check();
    } catch (error) { mismatch = error; }
    // Flush the actual moved directory as well as our pinned original, even on a
    // directory-swap race. No cleanup may restore/remove through the canonical name.
    const moved = fs.openSync(target, directoryFlags);
    try { fs.fsyncSync(owner.fd); fs.fsyncSync(directory); fs.fsyncSync(moved); fs.fsyncSync(parent.fd); }
    finally { fs.closeSync(moved); }
    if (mismatch) throw refused("last-check/rename reclaim race; all evidence retained", mismatch);
    check(); verify(target);
    retainReceipt(parent, `${targetName}.confirmed.json`, reclaimConfirmation(receipt, identity));
    check(); verify(target);
  } finally { owner?.close(); if (directory !== undefined) fs.closeSync(directory); parent.close(); }
}

/** A reclaim receipt records exact retained bytes, NEVER authority. Its identity is
 * independent of the receiver/high-water, so a crash BEFORE rename can retry the same
 * operation after another B1 fence without conflicting with a prior receiver's receipt. */
function reclaimConfirmation(receipt: Buffer, identity: string): Buffer {
  return Buffer.from(`${JSON.stringify({ version: 1, reclaimSha256: digest(receipt), evidenceIdentity: identity })}\n`);
}
function finishRetainedReclaims(parent: DirectoryRoute, lock: PortablePrivateLock,
  required: PortableRequiredRecord, check: () => void): void {
  // Metadata-only scan of the ONE manifest-listed private parent, after B1+OFD.
  // No history/global mesh traversal, deletion, restoration or new authority store.
  const pattern = /^custody\.lock\.portable-evidence\.([0-9a-f]{64})\.reclaim\.json$/;
  for (const name of fs.readdirSync(fdPath(parent.fd)).filter(name => pattern.test(name)).sort()) {
    check(); parent.assertCurrent();
    const receipt = pinFile(parent.at(name), 64 * 1024, mountId(parent.fd));
    let directory: number | undefined, owner: PinnedFile | undefined;
    try {
      // These are our closed immutable metadata receipts, not the original owner's
      // wire (which is kept as exact bytesBase64 and compared, never reserialized).
      const r = JSON.parse(receipt.bytes.toString("utf8")) as {
        version: number; kind: string; protocol: number; sessionId: string; canonical: string; evidence: string;
        directory: { dev: string; ino: string };
        owner: { dev: string; ino: string; bytesBase64: string; sha256: string };
        volume: PortableVolumeIdentity;
      };
      const decimal = (value: unknown): value is string => typeof value === "string" && /^(?:0|[1-9][0-9]*)$/.test(value);
      if (!r || Object.keys(r).length !== 9 || r.version !== 1 || r.kind !== lock.kind || r.protocol !== lock.protocol ||
        r.sessionId !== required.sessionId || r.canonical !== lock.directory || !sameVolume(r.volume, required.volume) ||
        !r.directory || Object.keys(r.directory).length !== 2 || !decimal(r.directory.dev) || !decimal(r.directory.ino) ||
        !r.owner || Object.keys(r.owner).length !== 4 || !decimal(r.owner.dev) || !decimal(r.owner.ino) ||
        typeof r.owner.bytesBase64 !== "string" || typeof r.owner.sha256 !== "string") {
        throw refused("retained reclaim receipt identity malformed");
      }
      const bytes = Buffer.from(r.owner.bytesBase64, "base64");
      const identity = `${r.directory.dev}:${r.directory.ino}:${r.owner.dev}:${r.owner.ino}:${digest(bytes)}`;
      const targetName = `${path.basename(lock.directory)}.portable-evidence.${digest(identity)}`;
      if (bytes.toString("base64") !== r.owner.bytesBase64 || !validPrivateOwner(bytes, lock.protocol) ||
        digest(bytes) !== r.owner.sha256 || name !== `${targetName}.reclaim.json` ||
        r.evidence !== path.join(path.dirname(lock.directory), targetName) ||
        !receipt.bytes.equals(Buffer.from(`${JSON.stringify(r)}\n`))) throw refused("retained reclaim receipt bytes/hash changed");
      const evidence = parent.at(targetName);
      const detached = fs.lstatSync(evidence, { throwIfNoEntry: false }) !== undefined;
      // No target yet = crash before rename. Verify the original is STILL canonical;
      // the caller can then complete its ordinary exact-pin rename under current B1.
      const observed = detached ? evidence : parent.at(path.basename(lock.directory));
      directory = fs.openSync(observed, directoryFlags);
      const stat = fs.fstatSync(directory, { bigint: true });
      owner = pinFile(path.join(fdPath(directory), "owner"), 16 * 1024, mountId(parent.fd));
      const verify = (): void => {
        check(); parent.assertCurrent(); receipt.assertAt(parent.at(name));
        const named = fs.lstatSync(observed, { bigint: true });
        if (!named.isDirectory() || named.isSymbolicLink() || !inode(stat, named) ||
          String(stat.dev) !== r.directory.dev || String(stat.ino) !== r.directory.ino ||
          String(owner!.stat.dev) !== r.owner.dev || String(owner!.stat.ino) !== r.owner.ino ||
          !owner!.bytes.equals(bytes)) throw refused("retained reclaim outcome unresolved; preserve all owners");
        owner!.assertAt(path.join(observed, "owner")); owner!.assertAt(path.join(fdPath(directory!), "owner"));
      };
      verify();
      if (detached) {
        // Crash after rename but before confirmation: complete the same file and
        // directory barriers. NEVER rename or remove whatever canonical names now.
        fs.fsyncSync(owner.fd); fs.fsyncSync(directory); fs.fsyncSync(parent.fd); verify();
        retainReceipt(parent, `${targetName}.confirmed.json`, reclaimConfirmation(receipt.bytes, identity));
        verify();
      }
    } finally { owner?.close(); if (directory !== undefined) fs.closeSync(directory); receipt.close(); }
  }
}

// M2(c,g) insertion fragment. Kernel custody only: this is NOT B1 authority.
import ofdFs from "node:fs";
import ofdPath from "node:path";
import { spawnSync as ofdSpawnSync } from "node:child_process";

// The child receives the parent's actual open file description as fd 3. It
// neither opens a pathname nor stays alive to hold the lock. Never unlock here:
// exiting closes only the child's reference; the parent retains the OFD lock.
const ofdAcquireProgram = `
import ctypes, fcntl, os, platform, sys
class OfdFlock(ctypes.Structure):
    _fields_ = [("l_type", ctypes.c_short), ("l_whence", ctypes.c_short),
                ("l_start", ctypes.c_longlong), ("l_len", ctypes.c_longlong),
                ("l_pid", ctypes.c_int)]
if (sys.platform != "linux" or platform.machine() not in ("x86_64", "aarch64")
        or sys.byteorder != "little" or ctypes.sizeof(ctypes.c_void_p) != 8
        or ctypes.sizeof(ctypes.c_long) != 8 or ctypes.sizeof(ctypes.c_int) != 4
        or ctypes.sizeof(ctypes.c_short) != 2 or ctypes.sizeof(OfdFlock) != 32
        or ctypes.alignment(OfdFlock) != 8
        or [getattr(OfdFlock, name).offset for name, _ in OfdFlock._fields_] != [0, 2, 8, 16, 24]
        or not hasattr(fcntl, "F_OFD_SETLK")):
    raise RuntimeError("unsupported OFD flock ABI or named command")
lock = OfdFlock(fcntl.F_WRLCK, os.SEEK_SET, 0, 0, 0)
fcntl.fcntl(3, fcntl.F_OFD_SETLK, bytes(lock))
sys.stdout.write("portable-ofd-v1\\n")
`;

type ofdDescriptor = {
  ofdStat: ofdFs.BigIntStats;
  ofdMount: string;
  ofdFlags: string;
  ofdLocks: string[];
};
type ofdPinnedRoute = { ofdFds: number[]; ofdDescriptors: ofdDescriptor[] };

function ofdFailure(reason: string): Error {
  return new Error(`PORTABLE_VOLUME_OFD_${reason}`);
}

/** fdinfo is description-specific. fstat alone cannot detect same-inode reuse. */
function ofdInspect(fd: number): ofdDescriptor {
  const ofdStat = ofdFs.fstatSync(fd, { bigint: true });
  const ofdText = ofdFs.readFileSync(`/proc/self/fdinfo/${fd}`, "utf8");
  const ofdMount = /^mnt_id:\s+([1-9][0-9]*)$/m.exec(ofdText)?.[1];
  const ofdFlags = /^flags:\s+([0-7]+)$/m.exec(ofdText)?.[1];
  const ofdInode = /^ino:\s+([0-9]+)$/m.exec(ofdText)?.[1];
  if (ofdText.length > 16_384 || !ofdMount || !ofdFlags || !ofdInode || BigInt(ofdInode) !== ofdStat.ino) {
    throw ofdFailure("FDINFO_INVALID");
  }
  return { ofdStat, ofdMount, ofdFlags, ofdLocks: ofdText.split("\n").filter(ofdLine => ofdLine.startsWith("lock:")) };
}

function ofdSameIdentity(left: ofdDescriptor, right: ofdDescriptor): boolean {
  return left.ofdStat.dev === right.ofdStat.dev && left.ofdStat.ino === right.ofdStat.ino &&
    left.ofdMount === right.ofdMount && left.ofdFlags === right.ofdFlags;
}

function ofdValidateFile(info: ofdDescriptor, uid: number): void {
  const ofdStat = info.ofdStat;
  if (!ofdStat.isFile() || ofdStat.uid !== BigInt(uid) || ofdStat.nlink !== 1n || (ofdStat.mode & 0o7777n) !== 0o600n) {
    throw ofdFailure("FILE_UNSAFE");
  }
}

/** Verify a whole-file WRITE OFDLCK attached to THIS description, not a fresh-open conflict. */
function ofdHasWholeFileLock(info: ofdDescriptor): boolean {
  if (info.ofdLocks.length !== 1) return false;
  const ofdMatch = /^lock:\s+[0-9]+:\s+OFDLCK\s+ADVISORY\s+WRITE\s+-1\s+([0-9a-fA-F]+):([0-9a-fA-F]+):([0-9]+)\s+0\s+EOF\s*$/.exec(info.ofdLocks[0]!);
  if (!ofdMatch) return false;
  // Linux's 64-bit dev_t encoding (major/minor in /proc are hexadecimal).
  const ofdDevice = info.ofdStat.dev;
  const ofdMajor = ((ofdDevice >> 8n) & 0xfffn) | ((ofdDevice >> 32n) & 0xfffff000n);
  const ofdMinor = (ofdDevice & 0xffn) | ((ofdDevice >> 12n) & 0xffffff00n);
  return BigInt(`0x${ofdMatch[1]!}`) === ofdMajor && BigInt(`0x${ofdMatch[2]!}`) === ofdMinor &&
    BigInt(ofdMatch[3]!) === info.ofdStat.ino;
}

// These descriptors never escape a synchronous walk. In particular, do not
// retain directory fd numbers across calls and later risk closing a reused fd.
function ofdReleaseRoute(route: ofdPinnedRoute): void {
  for (let ofdIndex = route.ofdFds.length - 1; ofdIndex >= 0; ofdIndex--) {
    try { ofdFs.closeSync(route.ofdFds[ofdIndex]!); } catch { /* Linux close must not be retried. */ }
  }
  route.ofdFds.length = 0;
}

/**
 * Pin EVERY route component while walking, from an opened root, without following
 * a user-supplied symlink. /proc/self/fd/<directory>/<single-component> is the
 * Linux dirfd-relative open bridge (Node has no openat); only that deliberate
 * procfs magic-link prefix is followed. O_NOFOLLOW applies to each supplied
 * component. O_NONBLOCK prevents an unsafe FIFO/device path from blocking before
 * fstat rejects it. No mkdir, chmod, truncate, rename, unlink, or fallback lock.
 */
function ofdOpenRoute(file: string, create: boolean, uid: number): ofdPinnedRoute {
  const ofdRoute: ofdPinnedRoute = { ofdFds: [], ofdDescriptors: [] };
  const ofdDirectoryFlags = ofdFs.constants.O_RDONLY | ofdFs.constants.O_DIRECTORY |
    ofdFs.constants.O_NOFOLLOW | ofdFs.constants.O_NONBLOCK;
  try {
    const ofdRoot = ofdFs.openSync("/", ofdDirectoryFlags);
    ofdRoute.ofdFds.push(ofdRoot);
    const ofdRootInfo = ofdInspect(ofdRoot);
    if (!ofdRootInfo.ofdStat.isDirectory() || ofdRootInfo.ofdStat.nlink === 0n) throw ofdFailure("ROUTE_UNSAFE");
    ofdRoute.ofdDescriptors.push(ofdRootInfo);
    const ofdParts = file.slice(1).split("/");
    for (let ofdIndex = 0; ofdIndex < ofdParts.length; ofdIndex++) {
      const ofdFinal = ofdIndex === ofdParts.length - 1;
      const ofdParent = ofdRoute.ofdFds[ofdRoute.ofdFds.length - 1]!;
      const ofdName = `/proc/self/fd/${ofdParent}/${ofdParts[ofdIndex]!}`;
      const ofdFlags = ofdFinal
        ? ofdFs.constants.O_RDWR | ofdFs.constants.O_NOFOLLOW | ofdFs.constants.O_NONBLOCK |
          (create ? ofdFs.constants.O_CREAT : 0)
        : ofdDirectoryFlags;
      const ofdFd = ofdFs.openSync(ofdName, ofdFlags, 0o600);
      ofdRoute.ofdFds.push(ofdFd);
      const ofdInfo = ofdInspect(ofdFd);
      if (ofdFinal) ofdValidateFile(ofdInfo, uid);
      else if (!ofdInfo.ofdStat.isDirectory() || ofdInfo.ofdStat.nlink === 0n) throw ofdFailure("ROUTE_UNSAFE");
      ofdRoute.ofdDescriptors.push(ofdInfo);
    }
    return ofdRoute;
  } catch (ofdError) {
    ofdReleaseRoute(ofdRoute);
    throw ofdError;
  }
}

function ofdCompareRoute(expected: readonly ofdDescriptor[], current: readonly ofdDescriptor[]): void {
  if (expected.length !== current.length || expected.some((ofdInfo, ofdIndex) => !ofdSameIdentity(ofdInfo, current[ofdIndex]!))) {
    throw ofdFailure("ROUTE_CHANGED");
  }
}

/**
 * Same-node kernel backstop for M2(c,g), NOT receipt/attachment authority.
 * Exactly one ephemeral isolated Python invocation sets a real inherited-fd
 * F_OFD_SETLK; the retained parent fd owns the lock after that child is reaped.
 * All checks are synchronous: no polling, resident helper, lease, or test seam.
 */
export async function acquirePortableVolumeGuard(file: string): Promise<{ assertCurrent(): void; close(): void }> {
  const ofdUid = process.getuid?.();
  if (process.platform !== "linux" || !["x64", "arm64"].includes(process.arch) || ofdUid === undefined) {
    throw ofdFailure("UNSUPPORTED");
  }
  if (typeof file !== "string" || !ofdPath.isAbsolute(file) || ofdPath.normalize(file) !== file ||
    file === "/" || file.includes("\0") || file.endsWith("/") || file.slice(1).split("/").some(ofdPart => !ofdPart || ofdPart === "." || ofdPart === "..")) {
    throw ofdFailure("PATH_INVALID");
  }
  // All ancestors are pinned during acquisition; future assertions reopen and
  // pin the complete route and compare EVERY original inode and mount ID.
  // Keeping the locked file fd also pins its inode/mount between assertions.
  const ofdInitialRoute = ofdOpenRoute(file, true, ofdUid);
  try {
    const ofdFd = ofdInitialRoute.ofdFds[ofdInitialRoute.ofdFds.length - 1]!;
    const ofdInitial = ofdInitialRoute.ofdDescriptors[ofdInitialRoute.ofdDescriptors.length - 1]!;
    const ofdResult = ofdSpawnSync("/usr/bin/python3", ["-I", "-B", "-c", ofdAcquireProgram], {
      stdio: ["ignore", "pipe", "pipe", ofdFd],
      env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C" },
      encoding: "utf8", timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 4_096,
    });
    if (ofdResult.error || ofdResult.signal !== null || ofdResult.status !== 0 ||
      ofdResult.stdout !== "portable-ofd-v1\n" || ofdResult.stderr !== "") {
      throw ofdFailure("ACQUIRE_FAILED");
    }
    const ofdLocked = ofdInspect(ofdFd);
    ofdValidateFile(ofdLocked, ofdUid);
    if (!ofdSameIdentity(ofdInitial, ofdLocked) || !ofdHasWholeFileLock(ofdLocked)) throw ofdFailure("LOCK_NOT_HELD");
    // Persist the fixed inode, without altering its contents or replacing it.
    ofdFs.fsyncSync(ofdFd);
    ofdFs.fsyncSync(ofdInitialRoute.ofdFds[ofdInitialRoute.ofdFds.length - 2]!);
    let ofdClosed = false;
    let ofdInvalid = false;
    const ofdGuard = {
      assertCurrent(): void {
        if (ofdClosed || ofdInvalid) throw ofdFailure("NOT_CURRENT");
        try {
          const ofdNow = ofdInspect(ofdFd);
          ofdValidateFile(ofdNow, ofdUid);
          if (!ofdSameIdentity(ofdLocked, ofdNow) || !ofdHasWholeFileLock(ofdNow)) throw ofdFailure("LOCK_LOST");
          const ofdCurrentRoute = ofdOpenRoute(file, false, ofdUid);
          try { ofdCompareRoute(ofdInitialRoute.ofdDescriptors, ofdCurrentRoute.ofdDescriptors); }
          finally { ofdReleaseRoute(ofdCurrentRoute); }
          // The path check must not substitute for description-specific custody.
          const ofdAfter = ofdInspect(ofdFd);
          if (!ofdSameIdentity(ofdLocked, ofdAfter) || !ofdHasWholeFileLock(ofdAfter)) throw ofdFailure("LOCK_LOST");
        } catch (ofdError) {
          ofdInvalid = true; // Positive invalidation is permanent, even if the route is restored.
          throw ofdError;
        }
      },
      close(): void {
        if (ofdClosed) return;
        ofdClosed = true;
        // Path loss/revocation does NOT release the original inode until close.
        // Conversely, stat equality alone must never close a reused same-inode
        // descriptor: require the original mount/flags AND its own OFDLCK.
        try {
          const ofdNow = ofdInspect(ofdFd);
          if (ofdSameIdentity(ofdLocked, ofdNow) && ofdHasWholeFileLock(ofdNow)) ofdFs.closeSync(ofdFd);
        } catch { /* Already closed/reused or unverifiable: never guess and close an unrelated fd. */ }
      },
    };
    ofdGuard.assertCurrent(); // Recheck the entire route after the helper/fsyncs.
    ofdInitialRoute.ofdFds.pop(); // Transfer ONLY the locked description to the guard.
    return ofdGuard;
  } finally {
    // No await or escaping callback precedes this transfer, so every remaining
    // fd here is one opened by this synchronous acquisition, even on failure.
    ofdReleaseRoute(ofdInitialRoute);
  }
}

