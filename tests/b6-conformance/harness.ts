// TEST ONLY. Real B6 store/journal; injected native fixture evidence is NOT native proof.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { vi } from "vitest";
import { openRecoveryDurabilityStore, type RecoveryDurabilityStore } from "../../src/recovery/durability-adapter.js";
import type { RecoveryJournal } from "../../src/recovery/durability-adapter-journal.js";
import type {
  B6CommitGate, B6CommittedEntryObservation, B6DurabilityHarness, B6HarnessObservation,
  B6NativeDescriptorFixtureOptions, B6RecoveryState, B6RegistrationObservation,
  RecoveryDurabilityAdapter, RecoveryNativeEntryRegistrationRequest,
} from "../contracts/l7-b6-durability-v1/contract.js";

export type InstallJournalDecorator = (directory: string, decorate: (journal: RecoveryJournal) => RecoveryJournal) => () => void;
const hash = (value: Uint8Array | string): string => createHash("sha256").update(value).digest("hex");
const ENTRY_PREFIX = "recovery-durability:v1:entry:";
const tuple = (request: { sessionId: string; deliveryId: string; recordKind: string }): string =>
  JSON.stringify([request.sessionId, request.deliveryId, request.recordKind]);
const cloneRegistration = (request: RecoveryNativeEntryRegistrationRequest): RecoveryNativeEntryRegistrationRequest => ({
  ...request, location: { ...request.location }, bytes: Uint8Array.from(request.bytes), flushReceipt: { ...request.flushReceipt },
});
// Deadline and signal bound a call, not the trusted descriptor's identity. Key values, not
// object identity: a detached identical descriptor/receipt is the same catalogue evidence.
const proofKey = (request: RecoveryNativeEntryRegistrationRequest): string => {
  const receipt = request.flushReceipt;
  return JSON.stringify([
    tuple(request), request.entryId, request.location.file, request.location.offset, request.location.length,
    Buffer.from(request.bytes).toString("base64"), request.bytesSha256,
    receipt.kind === "file-fsync"
      ? [receipt.kind, receipt.file, receipt.bytesThrough, receipt.namespaceDurable]
      : [receipt.kind, receipt.file, receipt.bytes, receipt.sha256],
  ]);
};
const coverage = (request: RecoveryNativeEntryRegistrationRequest): number =>
  request.flushReceipt.kind === "file-fsync" ? request.flushReceipt.bytesThrough : request.flushReceipt.bytes;

/** Numeric observations of real Node fs calls directed at the bound native path. Deny
 * before the syscall so even a regression cannot open/read/write that file. All other fs
 * calls delegate unchanged, including the real journal's file and namespace fsyncs.
 * The current production closure uses synchronous fs; callback and promise path APIs are
 * guarded too. fd calls are attributed using actual successful open/close observations. */
export function monitorNativePiFileIo(sessionFile: string): {
  counts: { opens: number; reads: number; writes: number; fsyncs: number };
  restore(): void;
} {
  const counts = { opens: 0, reads: 0, writes: 0, fsyncs: 0 };
  const descriptors = new Map<number, string>();
  const restores: Array<() => void> = [];
  type Fn = (...args: unknown[]) => unknown;
  const filename = (value: unknown): string | undefined => {
    if (typeof value === "number") return descriptors.get(value);
    if (value instanceof URL) return path.resolve(fileURLToPath(value));
    if (Buffer.isBuffer(value)) return path.resolve(value.toString());
    return typeof value === "string" ? path.resolve(value) : undefined;
  };
  const check = (value: unknown, kind: keyof typeof counts): void => {
    if (filename(value) !== sessionFile) return;
    counts[kind]++;
    throw new Error(`B6 attempted forbidden native Pi file I/O: ${kind}`);
  };
  const wrap = (object: object, name: string, decorate: (original: Fn) => Fn): void => {
    const target = object as Record<string, Fn>;
    const original = target[name];
    if (typeof original !== "function") throw new Error(`Missing fs instrumentation seam: ${name}`);
    const spy = vi.spyOn(target, name).mockImplementation(decorate(original));
    restores.push(() => spy.mockRestore());
  };
  try {
    wrap(fs, "openSync", original => (...args) => {
      check(args[0], "opens");
      const fd = Reflect.apply(original, fs, args) as number;
      const file = filename(args[0]);
      if (file !== undefined) descriptors.set(fd, file);
      return fd;
    });
    wrap(fs, "closeSync", original => (...args) => {
      try { return Reflect.apply(original, fs, args); }
      finally { descriptors.delete(args[0] as number); }
    });
    wrap(fs, "open", original => (...args) => {
      check(args[0], "opens");
      const file = filename(args[0]);
      const callback = args.at(-1);
      if (typeof callback === "function") args[args.length - 1] = (...result: unknown[]) => {
        if (!result[0] && typeof result[1] === "number" && file !== undefined) descriptors.set(result[1], file);
        return Reflect.apply(callback, undefined, result);
      };
      return Reflect.apply(original, fs, args);
    });
    wrap(fs, "close", original => (...args) => {
      const fd = args[0] as number;
      const callback = args.at(-1);
      if (typeof callback === "function") args[args.length - 1] = (...result: unknown[]) => {
        if (!result[0]) descriptors.delete(fd);
        return Reflect.apply(callback, undefined, result);
      };
      return Reflect.apply(original, fs, args);
    });
    for (const [kind, names] of [
      ["reads", ["readSync", "readvSync", "readFileSync", "read", "readv", "readFile", "createReadStream", "statSync", "lstatSync", "stat", "lstat"]],
      ["writes", ["writeSync", "writevSync", "writeFileSync", "appendFileSync", "write", "writev", "writeFile", "appendFile", "createWriteStream", "truncateSync", "truncate", "ftruncateSync", "ftruncate"]],
      ["fsyncs", ["fsyncSync", "fdatasyncSync", "fsync", "fdatasync"]],
    ] as const) {
      for (const name of names) wrap(fs, name, original => (...args) => {
        check(args[0], kind);
        return Reflect.apply(original, fs, args);
      });
    }
    wrap(fs.promises, "open", original => (...args) => {
      check(args[0], "opens");
      // No native FileHandle can escape this guard. Non-native handles stay untouched.
      return Reflect.apply(original, fs.promises, args);
    });
    for (const [kind, names] of [
      ["reads", ["readFile", "stat", "lstat"]],
      ["writes", ["writeFile", "appendFile", "truncate"]],
    ] as const) {
      for (const name of names) wrap(fs.promises, name, original => (...args) => {
        check(args[0], kind);
        return Reflect.apply(original, fs.promises, args);
      });
    }
  } catch (error) {
    for (const restore of restores.reverse()) restore();
    throw error;
  }
  return { counts, restore() { for (const restore of restores.splice(0).reverse()) restore(); } };
}

interface Gate extends B6CommitGate { wait(): Promise<void> }
interface Frame {
  version: number; storeId: string; seq: number; key: string; bytes: string; digest: string;
}
interface RetainedBinding {
  version: number; sessionId: string; deliveryId: string; recordKind: "control-seen" | "carrier"; bytes: string;
}

export async function createRealDurabilityHarness(install: InstallJournalDecorator): Promise<B6DurabilityHarness> {
  // This exact mkdtemp is the sole cleanup ceiling. The harness creates no other directory;
  // production itself creates/releases its real custody lock beneath this directory.
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "b6-real-conformance-"));
  const directoryStat = fs.lstatSync(directory);
  const sessionFile = path.join(directory, "native-pi-must-never-be-opened.jsonl");
  const sessionId = `b6-real-session-${randomUUID()}`;
  const storeId = `b6-real-store-${randomUUID()}`;
  const deliveryId = `b6-real-delivery-${randomUUID()}`;
  let clock = 1_900_000_000_000;
  const originalInputDeadlineMs = clock + 120_000;
  const io = monitorNativePiFileIo(sessionFile);
  const nowSpy = vi.spyOn(Date, "now").mockImplementation(() => clock);
  const trusted = new Map<string, Uint8Array>();
  // Observational metadata ONLY. The exact production Binding/Frame formats have NO
  // persistent deadline field. Never add one, or claim this map was recovered from disk.
  const originalDeadlines = new Map<string, number>();
  const registrations = new Map<string, RecoveryNativeEntryRegistrationRequest>();
  const inflight = new Set<Promise<unknown>>();
  const fixtureErrors: unknown[] = [];
  const gates = new Set<Gate>();
  let activeGate: Gate | undefined;
  let loseAck = false;
  let nextEntry = 0;
  let closed = false;
  let heldIdentity = false;
  let store: RecoveryDurabilityStore | undefined;
  const identity = path.join(directory, "journal.identity");
  const savedIdentity = path.join(directory, "fixture-held-journal.identity");
  const track = <T>(pending: Promise<T>): Promise<T> => {
    inflight.add(pending);
    void pending.then(() => inflight.delete(pending), () => inflight.delete(pending));
    return pending;
  };
  const current = (): RecoveryDurabilityStore => {
    if (!store) throw new Error("Real B6 harness has no constructed store");
    return store;
  };
  const uninstall = install(directory, journal => ({
    async append(key, bytes, bounds) {
      // This is a lower-layer boundary, NOT an adapter outcome engine. No fake queue,
      // sequence, deadline/abort check or journal result: after release every call enters
      // the original FileRecoveryJournal queue with the ORIGINAL deadline and signal.
      // Concurrent calls share the same pause, so FIFO promise reactions enter that real
      // queue in arrival order instead of letting a retry overtake the paused append.
      const gate = key.startsWith(ENTRY_PREFIX) ? activeGate : undefined;
      if (gate) await gate.wait();
      const result = await journal.append(key, bytes, bounds);
      if (loseAck && key.startsWith(ENTRY_PREFIX) && !result.existing) {
        loseAck = false;
        // The real raw segment AND commit witness already exist and were fsynced. Lose
        // only this lower-layer acknowledgement; the real adapter maps unknown to unavailable.
        throw new Error("B6 test lower-layer acknowledgement lost AFTER real committed append");
      }
      return result;
    },
    get: key => journal.get(key),
    entries: () => journal.entries(),
    close: () => journal.close(),
  }));
  const options = {
    directory, sessionId, sessionFile, storeId,
    verifyNativeEntry: async (request: RecoveryNativeEntryRegistrationRequest, signal: AbortSignal): Promise<boolean> => {
      if (signal.aborted) return false;
      const prefix = trusted.get(proofKey(request));
      if (!prefix || prefix.length === 0) return false;
      const end = request.location.offset + request.bytes.length;
      if (!Buffer.from(prefix.subarray(request.location.offset, end)).equals(Buffer.from(request.bytes))) return false;
      const receipt = request.flushReceipt;
      return receipt.kind === "file-fsync"
        ? receipt.bytesThrough === prefix.length && receipt.namespaceDurable === true
        : receipt.bytes === prefix.length && receipt.sha256 === hash(prefix);
    },
  };

  /** Observation reads immutable REAL raw files and their REAL zero-byte hash witnesses,
   * independently of adapter success, registrations, cached reads and lost ACKs. Binding
   * seq 1 participates in nextSeq, but is not an application/control/carrier observation. */
  const diskRecords = (): Frame[] => {
    const records: Frame[] = [];
    for (const name of fs.readdirSync(directory)) {
      const match = /^raw-(\d{16})-([a-f0-9-]{36})\.jsonl$/.exec(name);
      if (!match) continue;
      const raw = fs.readFileSync(path.join(directory, name));
      const witness = path.join(directory, `commit-${match[1]}-${match[2]}-${hash(raw)}`);
      if (!fs.existsSync(witness)) continue; // not committed; do not invent a record
      if (fs.readFileSync(witness).length !== 0 || raw.at(-1) !== 10) throw new Error("Invalid real commit witness/frame");
      const frame = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw)) as Frame;
      const digest = hash(JSON.stringify([frame.version, frame.storeId, frame.seq, frame.key, frame.bytes]));
      if (frame.version !== 1 || frame.storeId !== storeId || !Number.isSafeInteger(frame.seq) || frame.seq <= 0 ||
        String(frame.seq).padStart(16, "0") !== match[1] || frame.digest !== digest ||
        Buffer.from(frame.bytes, "base64").toString("base64") !== frame.bytes ||
        !raw.equals(Buffer.from(`${JSON.stringify(frame)}\n`))) throw new Error("Invalid real retained frame identity/hash/encoding");
      records.push(frame);
    }
    records.sort((a, b) => a.seq - b.seq);
    if (records.some((frame, index) => frame.seq !== index + 1)) throw new Error("Incomplete real committed sequence history");
    return records;
  };
  const observe = (): B6HarnessObservation => {
    if (fixtureErrors.length) throw new AggregateError(fixtureErrors, "B6 fixture staging/gate failed");
    const records = diskRecords();
    const committed: B6CommittedEntryObservation[] = [];
    for (const frame of records) {
      if (!frame.key.startsWith(ENTRY_PREFIX)) continue;
      const binding = JSON.parse(Buffer.from(frame.bytes, "base64").toString("utf8")) as RetainedBinding;
      const key = tuple(binding);
      const originalDeadlineMs = originalDeadlines.get(key);
      if (binding.version !== 1 || binding.sessionId !== sessionId || frame.key !== ENTRY_PREFIX + key ||
        (binding.recordKind !== "carrier" && binding.recordKind !== "control-seen") || originalDeadlineMs === undefined) {
        throw new Error("Real committed tuple lacks valid binding or original fixture deadline metadata");
      }
      committed.push({
        sessionId: binding.sessionId, deliveryId: binding.deliveryId, recordKind: binding.recordKind,
        seq: frame.seq, bytes: Uint8Array.from(Buffer.from(binding.bytes, "base64")), originalDeadlineMs,
      });
    }
    const accepted: B6RegistrationObservation[] = [...registrations.values()].map(request => ({
      sessionId: request.sessionId, deliveryId: request.deliveryId, entryId: request.entryId,
      location: { ...request.location }, bytes: Uint8Array.from(request.bytes), bytesSha256: request.bytesSha256,
      flushReceipt: { ...request.flushReceipt },
    }));
    return {
      storeId, sessionId, sessionFile, committed, registrations: accepted,
      nextSeq: (records.at(-1)?.seq ?? 0) + 1, nativePiFileIo: { ...io.counts },
    };
  };
  const releaseGates = (): void => { for (const gate of [...gates]) gate.release(); };
  const restoreIdentity = (): void => {
    if (heldIdentity) { fs.renameSync(savedIdentity, identity); heldIdentity = false; }
  };
  const rebuild = async (state: B6RecoveryState): Promise<void> => {
    if (closed) throw new Error("Real B6 harness is closed");
    releaseGates();
    await Promise.allSettled([...inflight]);
    await current().close(); // settle the REAL queue and release actual custody first
    restoreIdentity();
    registrations.clear(); // volatile accepted-call observations are not persisted receipts
    const retained = new Set(diskRecords().filter(row => row.key.startsWith(ENTRY_PREFIX)).map(row => row.key.slice(ENTRY_PREFIX.length)));
    for (const key of originalDeadlines.keys()) if (!retained.has(key)) originalDeadlines.delete(key);
    // Both non-authoritative states use the genuinely CLOSED production adapter. There is
    // no availability flag in front of any public method and no synthetic refusal result.
    if (state === "recovered") store = await openRecoveryDurabilityStore({ ...options, required: true });
  };
  const adapter: RecoveryDurabilityAdapter = {
    awaitDurableAppend(request) {
      // Production stageControlSeen snapshots/stages synchronously before returning its
      // promise. Delegate immediately: an extra fixture await here would turn the queued
      // abort/deadline cases into early-refusal tests instead of reaching the journal gate.
      return track(current().awaitDurableAppend(request));
    },
    readDurableEntry(request) { return track(current().readDurableEntry(request)); },
    registerNativeEntry(request) {
      const snapshot = cloneRegistration(request);
      return track(current().registerNativeEntry(request).then(result => {
        if (result.registered) {
          const key = tuple(snapshot);
          if (!originalDeadlines.has(key)) originalDeadlines.set(key, snapshot.deadlineMs);
          const previous = registrations.get(key);
          if (!previous || coverage(snapshot) > coverage(previous)) registrations.set(key, snapshot);
        }
        return result; // exactly the REAL adapter result, not a reconstructed outcome
      }));
    },
  };
  const cleanupDirectory = (): void => {
    const stat = fs.lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink() || stat.dev !== directoryStat.dev || stat.ino !== directoryStat.ino) {
      throw new Error("Refusing cleanup: test-created mkdtemp directory identity changed");
    }
    fs.rmSync(directory, { recursive: true, force: true }); // exact test-created ceiling only
  };
  const close = async (): Promise<void> => {
    if (closed) return;
    closed = true;
    releaseGates();
    try {
      await Promise.allSettled([...inflight]);
      await store?.close();
      restoreIdentity();
      if (fs.existsSync(path.join(directory, "custody.lock"))) throw new Error("Real B6 custody was not released");
      if (fixtureErrors.length) throw new AggregateError(fixtureErrors, "B6 fixture staging/gate failed");
      if (Object.values(io.counts).some(count => count !== 0)) throw new Error("B6 attempted native Pi file I/O");
    } finally {
      uninstall();
      nowSpy.mockRestore();
      io.restore();
      cleanupDirectory();
    }
  };

  try {
    // The production constructor legitimately establishes seq-1 session/file binding, then
    // relinquishes custody. Initial unknown reads use this real closed adapter, not fake data.
    store = await openRecoveryDurabilityStore(options);
    await store.close();
    return {
      adapter, sessionId, sessionFile, storeId, deliveryId, originalInputDeadlineMs,
      now: () => clock,
      setNow(epochMs) { if (!Number.isFinite(epochMs)) throw new Error("Nonfinite fixture clock"); clock = epochMs; },
      advanceNow(milliseconds) { if (!Number.isFinite(milliseconds)) throw new Error("Nonfinite fixture advance"); clock += milliseconds; },
      seedControlSeen(input) {
        const request = { sessionId, deliveryId: input.deliveryId ?? deliveryId, recordKind: "control-seen" as const };
        const key = tuple(request);
        if (!originalDeadlines.has(key)) originalDeadlines.set(key, input.originalDeadlineMs);
        const pending = track(current().stageControlSeen({ ...request, bytes: Uint8Array.from(input.bytes) }));
        void pending.catch(error => { fixtureErrors.push(error); });
      },
      nativeDescriptorFixture(fixture: B6NativeDescriptorFixtureOptions = {}) {
        const entryId = fixture.entryId ?? `b6-native-fixture-${++nextEntry}`;
        const json = fixture.json ?? JSON.stringify({ type: "message", id: entryId, message: { role: "user", content: "native fixture bytes: é" } });
        const bytes = Buffer.from(`${json}\n`, "utf8");
        const prefix = Buffer.from(fixture.prefixBytes ?? Buffer.from('{"fixture":"native-prefix"}\n'));
        const committedPrefix = Buffer.concat([prefix, bytes, Buffer.from(fixture.trailingCommittedBytes ?? new Uint8Array())]);
        const flushReceipt = fixture.receiptKind === "owned-journal-commit"
          ? { kind: "owned-journal-commit" as const, file: sessionFile, bytes: committedPrefix.length, sha256: hash(committedPrefix) }
          : { kind: "file-fsync" as const, file: sessionFile, bytesThrough: committedPrefix.length, namespaceDurable: true as const };
        const request: RecoveryNativeEntryRegistrationRequest = {
          sessionId, deliveryId: fixture.deliveryId ?? deliveryId, recordKind: "carrier", entryId,
          location: { file: sessionFile, offset: prefix.length, length: bytes.length - 1 },
          bytes: Uint8Array.from(bytes), bytesSha256: hash(bytes), flushReceipt,
          deadlineMs: fixture.deadlineMs ?? originalInputDeadlineMs,
        };
        // EXPLICIT TRUSTED FIXTURE CATALOGUE. Nothing reads or writes the native path;
        // these constructed native receipts are not native persistence/activation evidence.
        if (fixture.trusted !== false) trusted.set(proofKey(request), Uint8Array.from(committedPrefix));
        return request;
      },
      recover: (state = "recovered") => rebuild(state),
      reopen: (state = "recovered") => rebuild(state),
      invalidateAvailability() {
        if (!heldIdentity) { fs.renameSync(identity, savedIdentity); heldIdentity = true; }
        // The REAL journal detects/latches inventory loss on its next get/append. No fake
        // state switch can turn an unavailable read into an authoritative missing tuple.
      },
      gateNextCommit() {
        if (activeGate) throw new Error("Only one real append gate may be held");
        let enter!: () => void;
        let rejectEnter!: (error: Error) => void;
        let release!: () => void;
        let released = false;
        let entered = false;
        let expired: Error | undefined;
        const arrival = new Promise<void>((resolve, reject) => { enter = resolve; rejectEnter = reject; });
        void arrival.catch(() => undefined);
        const pause = new Promise<void>(resolve => { release = resolve; });
        const gate: Gate = {
          entered: arrival,
          async wait() { if (!entered) { entered = true; enter(); } await pause; if (expired) throw expired; },
          release() {
            if (released) return;
            released = true;
            clearTimeout(timer);
            if (activeGate === gate) activeGate = undefined;
            gates.delete(gate);
            release();
          },
        };
        // Normal event is explicit release()/close(). This one-shot real-timer ceiling
        // fails the fixture on a missed event; it neither polls nor simulates an ABI refusal.
        const timer = setTimeout(() => {
          expired = new Error("B6 real-journal append gate exceeded its 5s fixture bound");
          fixtureErrors.push(expired);
          if (!entered) rejectEnter(expired);
          gate.release();
        }, 5_000);
        gates.add(gate);
        activeGate = gate;
        return gate;
      },
      loseNextWriteAck() { loseAck = true; },
      observe, close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}
