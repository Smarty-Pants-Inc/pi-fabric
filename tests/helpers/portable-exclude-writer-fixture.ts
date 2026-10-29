// Decision-only REQUIRED/native-CAS/B1/native-hold ports. NOT an authenticity ABI or
// real native exclusion proof. Real gen/history/receipt/lock bytes and existing openers below.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { vi } from "vitest";
import { TransportHostLease, type TransportHostLeaseOptions, type TransportHostLeaseStartupSuccessor,
  type TransportHostLeasePredecessorSnapshot } from "../../src/topology/host-leases.js";
import { KvConflictError, type TransportKv, type TransportKvEntry, type TransportKvEvent } from "../../src/transport/types.js";
import { acquireMeshCustodyLock } from "../../src/mesh/custody-lock.js";
import { openRecoveryJournal } from "../../src/recovery/recovery-journal.js";
import type { PortableIdentityAdapter } from "../../src/recovery/l7-interfaces.js";

// Exact existing Pi projection: no lifecycle, generation, signal or lease fields here.
export interface PreOpen { excludeWriter(request: { sessionFile: string }): Promise<{ assertCurrent(): void }> }
export interface Volume { pvcUid: string; csiHandle: string; filesystemUuid: string }
export interface LockEntry { kind: "transport-receipt" | "b6"; directory: string; protocol: 2 | 3; sessionPrivate: boolean }
export interface RequiredRecord {
  mode: string; sessionId: string; sessionFile: string; volume: Volume;
  stateDirectory: string; b5File: string; b6Directory: string; storeId: string;
  custodyDirectory: string; generationFile: string; lockFile: string;
  transport: { machine: string; hostId: string; rootId: string; identityId: string; incarnation: string };
  locks: LockEntry[]; highWater: number; revision: symbol;
}
export interface CompletedFence {
  kind: "b1-fence"; completed: boolean; receivingPodUid: string; attachment: string; volume: Volume;
  readonly signal: AbortSignal; assertCurrent(): void;
}
export interface DecisionGuard {
  readonly signal: AbortSignal; readonly nativeTuple: object;
  assertCurrent(): void; close(): void;
}
export type CasReply = { status: "confirmed"; snapshot: RequiredRecord } | { status: "conflict" | "unknown" };
export interface BorrowedLease {
  readonly lease: TransportHostLease;
  readonly tuple: RequiredRecord["transport"] & { localStateDir: string };
}
export interface Binding {
  preOpen: PreOpen; identity: PortableIdentityAdapter;
  borrowLease(): BorrowedLease;
  closeAfterNativeJoin(joined: Promise<void>): Promise<void>;
}
export interface FactoryInput {
  bootstrap: { identityKey: string; sessionId: string; sessionFile: string; volume: Volume;
    self: { podUid: string; attachment: string }; generationFile: string; lockFile: string;
    signal: AbortSignal; deadlineMs: number };
  identityRecord: {
    readRequired(key: string, signal: AbortSignal): Promise<RequiredRecord | undefined>;
    casHighWater(snapshot: RequiredRecord, next: number, signal: AbortSignal): Promise<CasReply>;
  };
  b1: { readCompletedForSelf(required: RequiredRecord, self: FactoryInput["bootstrap"]["self"], signal: AbortSignal): Promise<CompletedFence | undefined> };
  nativeCustody: {
    // Decision-only live mount reader; production must obtain this from the actual mounted attachment.
    readMountedAtOpen(signal: AbortSignal): Promise<{ volume: Volume; attachment: string }>;
    hold(required: RequiredRecord, b1: CompletedFence, signal: AbortSignal): Promise<DecisionGuard>;
  };
  leaseOwner: { acquireOnce(options: { required: RequiredRecord; startupSuccessor: TransportHostLeaseStartupSuccessor; signal: AbortSignal }): Promise<BorrowedLease> };
}
export type CreateBinding = (input: FactoryInput) => Binding;
export const deferred = <T = void>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
};

// Minimal synthetic broker, not an identity store, stream, log or TTL-authority substitute.
class DecisionKv implements TransportKv {
  readonly ttlMs = 15_000;
  readonly entries = new Map<string, TransportKvEntry<unknown>>();
  readonly writes: number[] = [];
  sequence = 0;
  ack: Promise<void> | undefined;
  async get<V>(key: string): Promise<TransportKvEntry<V> | undefined> { return this.entries.get(key) as TransportKvEntry<V> | undefined; }
  async put<V>(key: string, owner: string, value: V, revision: number): Promise<number> {
    this.writes.push(revision);
    if ((this.entries.get(key)?.revision ?? 0) !== revision) throw new KvConflictError(key, revision);
    const entry = { key, owner, machine: key.split(".")[0]!, value, revision: ++this.sequence };
    this.entries.set(key, entry); await this.ack; return entry.revision;
  }
  async delete(key: string, revision: number): Promise<boolean> {
    if (this.entries.get(key)?.revision !== revision) return false;
    return this.entries.delete(key);
  }
  async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    yield { type: "ready" };
    if (!signal.aborted) await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }));
  }
}
export const ownerSnapshot = (directory: string) => ({ directory,
  dir: fs.lstatSync(directory), owner: fs.lstatSync(path.join(directory, "owner")),
  bytes: fs.readFileSync(path.join(directory, "owner")),
});
export const treeBytes = (root: string): Record<string, string> => {
  const result: Record<string, string> = {};
  const visit = (dir: string): void => {
    for (const name of fs.readdirSync(dir).sort()) {
      const file = path.join(dir, name), stat = fs.lstatSync(file), key = path.relative(root, file);
      if (stat.isDirectory()) { result[key] = "directory"; visit(file); }
      else result[key] = stat.isSymbolicLink() ? `symlink:${fs.readlinkSync(file)}` : fs.readFileSync(file).toString("base64");
    }
  };
  visit(root); return result;
};

export async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "portable-exclude-writer-"));
  const state = path.join(root, "state"), b6 = path.join(root, "b6");
  fs.mkdirSync(state); fs.mkdirSync(b6);
  const sessionFile = path.join(root, "session.jsonl"), b5File = path.join(root, "b5-retained.json");
  fs.writeFileSync(sessionFile, '{"type":"session","id":"session:retained"}\n{"type":"message","text":"retained"}\n');
  fs.writeFileSync(b5File, '{"fixture":"retained B5 bytes, NOT a B5 validator"}\n');
  // Local gen wire is provisional ASCII safe-integer + LF, not an existing production format.
  const generationFile = path.join(root, "custody.gen"); fs.writeFileSync(generationFile, "7\n");
  const lockFile = path.join(root, "custody.ofd"); fs.writeFileSync(lockFile, "", { mode: 0o600 });
  const initial = await openRecoveryJournal({ directory: b6, storeId: "store:retained" });
  await initial.append("kept", Buffer.from("B6 exact retained value"), { deadlineMs: Date.now() + 5_000 });
  await initial.close();
  const volume: Volume = { pvcUid: "pvc:original", csiHandle: "csi:original", filesystemUuid: "fs:original" };
  const transport = { machine: "portable", hostId: "session:retained", rootId: "root:retained", identityId: "identity:retained", incarnation: "receiver" };
  const kv = new DecisionKv(), leases: TransportHostLease[] = [], bindings: Binding[] = [];
  const gates: (() => void)[] = [];
  let clock = 0;
  const leaseOptions = (incarnation: string): TransportHostLeaseOptions => ({ kv, self: { machine: transport.machine, hostId: transport.hostId },
    rootId: transport.rootId, identityId: transport.identityId, incarnation, localStateDir: state, monotonic: () => clock });
  const old = new TransportHostLease(leaseOptions("predecessor")); leases.push(old);
  await old.acquire(); kv.entries.clear(); kv.writes.length = 0;
  const receiptFile = path.join(state, `transport-host-lease-${createHash("sha256").update(transport.hostId).digest("hex").slice(0, 32)}.json`);
  const receiptBytes = `\n ${JSON.stringify(JSON.parse(fs.readFileSync(receiptFile, "utf8")), null, 2)}\n`;
  fs.writeFileSync(receiptFile, receiptBytes);
  const locks: LockEntry[] = [
    { kind: "transport-receipt", directory: path.join(`${receiptFile}.custody`, "custody.lock"), protocol: 2, sessionPrivate: true },
    { kind: "b6", directory: path.join(b6, "custody.lock"), protocol: 3, sessionPrivate: true },
  ];
  let record: RequiredRecord | undefined = { mode: "REQUIRED", sessionId: "session:retained", sessionFile, volume: { ...volume },
    stateDirectory: state, b5File, b6Directory: b6, storeId: "store:retained",
    custodyDirectory: root, generationFile, lockFile, transport, locks, highWater: 7, revision: Symbol("native-revision") };
  const abort = new AbortController(), fenceAbort = new AbortController(), nativeAbort = new AbortController();
  let nativeFault: string | undefined, fenceFault = false, held = false, closed = 0, acquired = 0;
  let mounted = { volume: { ...volume }, attachment: "attachment:current" };
  const events: string[] = [], cas: { next: number; disk: string; before: number }[] = [], snapshots: TransportHostLeasePredecessorSnapshot[] = [];
  const nativeTuple = Object.freeze({ fixtureOnly: "not a native RecoveryEpoch" });
  const guard: DecisionGuard = { signal: nativeAbort.signal, nativeTuple,
    assertCurrent() { events.push("native:check"); if (!held || nativeFault || nativeAbort.signal.aborted) throw new Error(nativeFault ?? "native not held"); },
    close() { closed++; held = false; events.push("native:close"); } };
  let fence: CompletedFence | undefined = { kind: "b1-fence", completed: true, receivingPodUid: "pod:self", attachment: "attachment:current", volume: { ...volume }, signal: fenceAbort.signal,
    assertCurrent() { if (fenceFault || fenceAbort.signal.aborted) throw new Error("positive B1 supersession"); } };
  let mode: "ok" | "conflict" | "unknown" | "unknown-committed" = "ok", unreadable = false, holdFails = false;
  let borrowed: BorrowedLease | undefined;
  const input: FactoryInput = {
    bootstrap: { identityKey: "provisioned:existing", sessionId: "session:retained", sessionFile, volume, self: { podUid: "pod:self", attachment: "attachment:current" },
      generationFile, lockFile, signal: abort.signal, deadlineMs: Date.now() + 5_000 },
    identityRecord: {
      async readRequired() { events.push("identity:read"); if (unreadable) throw new Error("identity unavailable"); return record && { ...record, volume: { ...record.volume } }; },
      async casHighWater(snapshot, next, signal) {
        events.push(`cas:${next}`); signal.throwIfAborted();
        cas.push({ next, disk: fs.readFileSync(generationFile, "utf8"), before: record!.highWater });
        if (snapshot.revision !== record?.revision || mode === "conflict") return { status: "conflict" };
        if (mode === "unknown") { unreadable = true; return { status: "unknown" }; }
        record = { ...record!, highWater: next, revision: Symbol("next-native-revision") };
        return mode === "unknown-committed" ? { status: "unknown" } : { status: "confirmed", snapshot: record };
      },
    },
    b1: { async readCompletedForSelf() { events.push("b1:read"); return fence; } },
    nativeCustody: {
      async readMountedAtOpen() { events.push("mount:read-at-open"); return { ...mounted, volume: { ...mounted.volume } }; },
      // This decision port is NOT the raw OFD guard; the factory must also acquire that real guard.
      async hold() { events.push("native:hold"); if (holdFails) throw new Error("native custody hold refused"); held = true; return guard; },
    },
    leaseOwner: { async acquireOnce({ required, startupSuccessor, signal }) {
      events.push("lease:acquire"); acquired++;
      if (borrowed) throw new Error("second lease acquisition");
      const lease = new TransportHostLease({ kv, self: { machine: required.transport.machine, hostId: required.transport.hostId },
        rootId: required.transport.rootId, identityId: required.transport.identityId, incarnation: required.transport.incarnation,
        localStateDir: required.stateDirectory, monotonic: () => clock, startupSuccessor: {
        async verifyPredecessor(snapshot, operationSignal) {
          snapshots.push(snapshot); events.push("lease:validate");
          await startupSuccessor.verifyPredecessor(snapshot, operationSignal);
        },
      } }); leases.push(lease); await lease.acquire(signal);
      return borrowed = { lease, tuple: Object.freeze({ ...required.transport, localStateDir: required.stateDirectory }) };
    } },
  };
  const historyOpens = { native: 0, b5: 0, b6: 0 };
  return { root, state, b6, sessionFile, b5File, generationFile, lockFile, receiptFile, receiptBytes, locks, kv, events, cas, snapshots, input, guard, nativeTuple, historyOpens, abort,
    gate() { const gate = deferred(); gates.push(gate.resolve); return gate; },
    renewOwnedLease() { if (!borrowed) throw new Error("lease not acquired"); return borrowed.lease.renew(); },
    confirmOwnedRootRecheck() { if (!borrowed) throw new Error("lease not acquired"); borrowed.lease.clearRootRecheck(); },
    get mounted() { return mounted; }, set mounted(value: typeof mounted) { mounted = value; },
    get record() { return record; }, set record(value: RequiredRecord | undefined) { record = value; },
    get fence() { return fence; }, set fence(value: CompletedFence | undefined) { fence = value; },
    set casMode(value: typeof mode) { mode = value; }, set unreadable(value: boolean) { unreadable = value; }, set holdFails(value: boolean) { holdFails = value; },
    get acquired() { return acquired; }, get closed() { return closed; }, get held() { return held; },
    set clock(value: number) { clock = value; },
    revoke(kind: "native" | "mount" | "b1") { if (kind === "b1") { fenceFault = true; fenceAbort.abort(); } else { nativeFault = kind; nativeAbort.abort(); } },
    loseObservation(kind: "native" | "mount" | "b1") { if (kind === "b1") fenceFault = true; else nativeFault = kind; },
    restoreObservation() { nativeFault = undefined; fenceFault = false; },
    bind(create: CreateBinding) { const binding = create(input); bindings.push(binding); return binding; },
    async consume(binding: Binding) {
      const current = await binding.preOpen.excludeWriter({ sessionFile }); current.assertCurrent();
      historyOpens.native++; const native = fs.readFileSync(sessionFile);
      current.assertCurrent(); historyOpens.b5++; const b5 = fs.readFileSync(b5File);
      current.assertCurrent(); historyOpens.b6++;
      const journal = await openRecoveryJournal({ directory: b6, storeId: "store:retained", required: true });
      try { return { current, native, b5, b6: journal.get("kept")!.bytes }; } finally { await journal.close(); }
    },
    async foreignLock(entry: LockEntry) {
      const release = await acquireMeshCustodyLock(path.dirname(entry.directory), 0, { hostQualified: true, lockProtocol: entry.protocol });
      const file = path.join(entry.directory, "owner"), fields = fs.readFileSync(file, "utf8").split("\n");
      fields[4] = "host:foreign-predecessor";
      if (entry.protocol === 3) fields[6] = "linux:ffffffffffffffffffffffffffffffff:00000000-0000-4000-8000-000000000001:123";
      fs.writeFileSync(file, fields.join("\n")); release();
      fs.writeFileSync(path.join(entry.directory, "retained-marker"), "do not delete");
      return ownerSnapshot(entry.directory);
    },
    async dispose() {
      for (const release of gates) release(); // Release only this fixture's deferred test gates, even on assertion failure.
      for (const binding of bindings) await binding.closeAfterNativeJoin(Promise.resolve()).catch(() => undefined);
      for (const lease of leases) await lease.release().catch(() => false);
      fs.rmSync(root, { recursive: true, force: true }); // Only this fixture's mkdtemp tree; no trace cleanup.
    },
  };
}
export type Fixture = Awaited<ReturnType<typeof fixture>>;

// Observe actual FS attempts, not a mocked get() or a claimed Main/native history proof.
export function watchRetainedIO(s: Fixture) {
  const reads: string[] = [], writes: string[] = [];
  const filePath = (value: unknown): string => {
    if (value instanceof URL) return decodeURIComponent(value.pathname);
    if (Buffer.isBuffer(value)) return value.toString();
    if (typeof value === "string") return value;
    const fd = typeof value === "number" ? value : value && typeof value === "object" && "fd" in value ? value.fd : undefined;
    if (typeof fd === "number") { try { return fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { /* not a tracked fd */ } }
    return "";
  };
  const retained = (value: unknown): void => {
    const file = filePath(value);
    if (file === s.sessionFile || file === s.b5File || file === s.b6 || file.startsWith(`${s.b6}${path.sep}`)) reads.push(file);
  };
  const spies = (["readFileSync", "readdirSync", "readSync", "readvSync", "readFile", "readdir", "read", "readv"] as const).map(key => {
    const original = fs[key];
    return vi.spyOn(fs, key).mockImplementation(((...args: unknown[]) => { retained(args[0]); return Reflect.apply(original, fs, args); }) as never);
  });
  // Observe real product write attempts (including rejected/undone writes), not just end-state bytes.
  const write = (value: unknown): void => {
    const file = filePath(value);
    if (file === s.root || file.startsWith(`${s.root}${path.sep}`)) writes.push(file);
  };
  const mutations = ["writeFileSync", "appendFileSync", "writeSync", "writevSync", "truncateSync", "ftruncateSync", "mkdirSync", "mkdtempSync", "renameSync",
    "unlinkSync", "rmSync", "rmdirSync", "chmodSync", "fchmodSync", "chownSync", "fchownSync", "lchownSync", "linkSync", "symlinkSync", "utimesSync", "futimesSync", "lutimesSync",
    "writeFile", "appendFile", "write", "writev", "truncate", "ftruncate", "mkdir", "mkdtemp", "rename", "unlink", "rm", "rmdir",
    "chmod", "fchmod", "chown", "fchown", "lchown", "link", "symlink", "utimes", "futimes", "lutimes"] as const;
  const mutationSpies = mutations.map(<K extends typeof mutations[number]>(key: K) => {
    const original = fs[key];
    return vi.spyOn(fs, key).mockImplementation(((...args: unknown[]) => {
      write(args[0]); if (key === "renameSync" || key === "linkSync" || key === "symlinkSync" || key === "rename" || key === "link" || key === "symlink") write(args[1]);
      return Reflect.apply(original, fs, args);
    }) as never);
  });
  // Capture write-capable opens too: a truncate/create need not reach writeFileSync.
  const observeOpen = (file: unknown, flags: unknown): void => {
    retained(file);
    if (typeof flags === "string" ? /[wa+]/.test(flags) : typeof flags === "number" &&
      (flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_APPEND)) !== 0) write(file);
  };
  const openSpies = (["openSync", "open"] as const).map(key => {
    const original = fs[key];
    return vi.spyOn(fs, key).mockImplementation(((...args: unknown[]) => {
      observeOpen(args[0], args[1]); return Reflect.apply(original, fs, args);
    }) as never);
  });
  const promiseReadSpies = (["readFile", "readdir"] as const).map(key => {
    const original = fs.promises[key];
    return vi.spyOn(fs.promises, key).mockImplementation(((...args: unknown[]) => {
      retained(args[0]); return Reflect.apply(original, fs.promises, args);
    }) as never);
  });
  const handleSpies: { mockRestore(): void }[] = [];
  const realPromiseOpen = fs.promises.open;
  const promiseOpenSpy = vi.spyOn(fs.promises, "open").mockImplementation(async (file, flags, mode) => {
    observeOpen(file, flags); const handle = await realPromiseOpen(file, flags, mode);
    for (const key of ["read", "readFile", "readv", "write", "writeFile", "writev", "appendFile", "truncate", "chmod", "chown", "utimes"] as const) {
      const original = handle[key];
      handleSpies.push(vi.spyOn(handle, key).mockImplementation(((...args: unknown[]) => {
        if (key === "read" || key === "readFile" || key === "readv") retained(file); else write(file);
        return Reflect.apply(original, handle, args);
      }) as never));
    }
    return handle;
  });
  const copySpies = (["copyFileSync", "cpSync", "copyFile", "cp"] as const).map(key => {
    const original = fs[key];
    return vi.spyOn(fs, key).mockImplementation(((...args: unknown[]) => {
      retained(args[0]); write(args[1]); return Reflect.apply(original, fs, args);
    }) as never);
  });
  const promiseCopySpies = (["copyFile", "cp"] as const).map(key => {
    const original = fs.promises[key];
    return vi.spyOn(fs.promises, key).mockImplementation(((...args: unknown[]) => {
      retained(args[0]); write(args[1]); return Reflect.apply(original, fs.promises, args);
    }) as never);
  });
  const promiseSpies = (["writeFile", "appendFile", "truncate", "mkdir", "mkdtemp", "rename", "unlink", "rm", "rmdir", "chmod", "chown", "lchown", "link", "symlink", "utimes", "lutimes"] as const).map(key => {
    const original = fs.promises[key];
    return vi.spyOn(fs.promises, key).mockImplementation(((...args: unknown[]) => {
      write(args[0]); if (key === "rename" || key === "link" || key === "symlink") write(args[1]);
      return Reflect.apply(original, fs.promises, args);
    }) as never);
  });
  return { reads, writes, stop() {
    for (const spy of [...handleSpies, promiseOpenSpy, ...openSpies, ...spies, ...mutationSpies, ...promiseReadSpies, ...copySpies, ...promiseCopySpies, ...promiseSpies]) spy.mockRestore();
  } };
}
