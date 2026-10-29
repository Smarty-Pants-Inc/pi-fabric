// TEST ONLY: in-memory behavior double. It is not a production permission,
// durable-store, native-provenance or crash/recovery implementation.
import { createHash } from "node:crypto";
import type {
  RecoveryDurabilityAdapter, RecoveryDurableAppendRequest, RecoveryDurableAppendResult,
  RecoveryDurableEntryRequest, RecoveryDurableEntryResult,
  RecoveryNativeEntryRegistrationRequest, RecoveryNativeEntryRegistrationResult,
  RecoveryNativeFlushReceipt,
} from "../../../src/recovery/l7-interfaces.js";
import type {
  B6CommitGate, B6CommittedEntryObservation, B6DurabilityHarnessFactory,
  B6DurabilityHarness, B6HarnessObservation, B6NativeDescriptorFixtureOptions,
  B6RecoveryState, B6RegistrationObservation,
} from "./contract.js";

const UNKNOWN_ACK_MESSAGE = "test-only B6 write acknowledgement lost after commit";
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const bytes = (value: Uint8Array): Uint8Array => new Uint8Array(value);
const concat = (...parts: Uint8Array[]): Uint8Array => {
  const length = parts.reduce((total, part) => total + part.length, 0);
  const result = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) { result.set(part, offset); offset += part.length; }
  return result;
};
const hash = (value: Uint8Array): string => createHash("sha256").update(value).digest("hex");
const NATIVE_ENTRY_PREFIX = "pi-native-entry:v1:";
const derivedNativeDeliveryId = (sessionId: string, entryId: string): string =>
  NATIVE_ENTRY_PREFIX + hash(encoder.encode(JSON.stringify([sessionId, entryId])));
const sameBytes = (a: Uint8Array, b: Uint8Array): boolean =>
  a.length === b.length && a.every((value, index) => value === b[index]);
const tupleKey = (value: { sessionId: string; deliveryId: string; recordKind: "control-seen" | "carrier" }): string =>
  JSON.stringify([value.sessionId, value.deliveryId, value.recordKind]);
const cloneReceipt = (receipt: RecoveryNativeFlushReceipt): RecoveryNativeFlushReceipt =>
  receipt.kind === "file-fsync" ? { ...receipt } : { ...receipt };
const receiptCoverage = (receipt: RecoveryNativeFlushReceipt): number =>
  receipt.kind === "file-fsync" ? receipt.bytesThrough : receipt.bytes;

interface ControlSeed {
  readonly bytes: Uint8Array;
  readonly originalDeadlineMs: number;
}
interface StoredEntry extends B6CommittedEntryObservation {}
interface StoredRegistration {
  readonly request: RecoveryNativeEntryRegistrationRequest;
  readonly binding: string;
  readonly coverage: number;
}
interface TrustedNativeDescriptor {
  readonly sessionId: string;
  readonly deliveryId: string;
  readonly entryId: string;
  readonly file: string;
  readonly offset: number;
  readonly length: number;
  readonly bytes: Uint8Array;
  readonly bytesSha256: string;
  readonly receipt: RecoveryNativeFlushReceipt;
  /** Exact trusted fixture prefix covered by the native flush evidence. */
  readonly committedPrefix: Uint8Array;
}
interface GateState {
  readonly entered: Promise<void>;
  readonly wait: () => Promise<void>;
  readonly release: () => void;
}

const cloneRegistration = (request: RecoveryNativeEntryRegistrationRequest): RecoveryNativeEntryRegistrationRequest => ({
  sessionId: request.sessionId,
  deliveryId: request.deliveryId,
  recordKind: "carrier",
  entryId: request.entryId,
  location: { ...request.location },
  bytes: bytes(request.bytes),
  bytesSha256: request.bytesSha256,
  flushReceipt: cloneReceipt(request.flushReceipt),
  deadlineMs: request.deadlineMs,
});
const registrationBinding = (request: RecoveryNativeEntryRegistrationRequest): string => JSON.stringify([
  request.sessionId, request.deliveryId, request.recordKind, request.entryId,
  request.location.file, request.location.offset, request.location.length,
  request.bytesSha256, Array.from(request.bytes),
]);
const trustedDescriptorKey = (request: RecoveryNativeEntryRegistrationRequest): string => {
  const receipt = request.flushReceipt;
  const receiptIdentity = receipt.kind === "file-fsync"
    ? [receipt.kind, receipt.file, receipt.bytesThrough, receipt.namespaceDurable]
    : [receipt.kind, receipt.file, receipt.bytes, receipt.sha256];
  return JSON.stringify([
    request.sessionId, request.deliveryId, request.recordKind, request.entryId,
    request.location.file, request.location.offset, request.location.length,
    request.bytesSha256, Array.from(request.bytes), receiptIdentity,
  ]);
};
const sameBinding = (a: RecoveryNativeEntryRegistrationRequest, b: RecoveryNativeEntryRegistrationRequest): boolean =>
  registrationBinding(a) === registrationBinding(b);

let harnessOrdinal = 0;

export function createTestOnlyFakeHarness(): B6DurabilityHarness {
  const ordinal = ++harnessOrdinal;
  const sessionId = `test-only-b6-session-${ordinal}`;
  const sessionFile = `/test-only/b6/session-${ordinal}.jsonl`;
  const storeId = `test-only-b6-store-${ordinal}`;
  const deliveryId = `test-only-b6-delivery-${ordinal}`;
  let currentTime = 1_900_000_000_000 + ordinal * 100_000;
  const originalInputDeadlineMs = currentTime + 120_000;

  // The fake's retained log and native fixture evidence are distinct from its
  // reconstructible view. A cache miss never grants negative authority.
  const committedLog = new Map<string, StoredEntry>();
  const committedRegistrations = new Map<string, StoredRegistration>();
  let recoveredEntries = new Map<string, StoredEntry>();
  // Registrations remain volatile correlation until the matching carrier barrier commits.
  let recoveredRegistrations = new Map<string, StoredRegistration>();
  const controlSeeds = new Map<string, ControlSeed>();
  const poisonedTuples = new Set<string>();
  const trustedDescriptors = new Map<string, TrustedNativeDescriptor>();
  const queuedGates: GateState[] = [];
  const allGates = new Set<GateState>();
  let nextEntryId = 0;
  let nextSeq = 1;
  let available = true;
  let recovered = false;
  let bound = true;
  let closed = false;
  let loseAck = false;
  let writeTail: Promise<void> = Promise.resolve();

  const bindingFailure = (requestSessionId: string): "superseded" | "store-unavailable" | undefined => {
    if (closed || !bound || requestSessionId !== sessionId) return "superseded";
    if (!available || !recovered) return "store-unavailable";
    return undefined;
  };
  const deadlineFailure = (deadlineMs: number, signal?: AbortSignal): "aborted" | "deadline" | undefined => {
    if (signal?.aborted) return "aborted";
    if (!Number.isFinite(deadlineMs) || currentTime >= deadlineMs) return "deadline";
    return undefined;
  };
  const appendFailure = (request: RecoveryDurableAppendRequest, effectiveDeadlineMs: number): RecoveryDurableAppendResult | undefined => {
    const binding = bindingFailure(request.sessionId);
    if (binding === "superseded" || poisonedTuples.has(tupleKey(request))) return { durable: false, reason: "superseded" };
    if (binding) return { durable: false, reason: binding };
    const deadline = deadlineFailure(effectiveDeadlineMs, request.signal);
    if (deadline) return { durable: false, reason: deadline };
    return undefined;
  };
  const registrationFailure = (request: RecoveryNativeEntryRegistrationRequest): RecoveryNativeEntryRegistrationResult | undefined => {
    const binding = bindingFailure(request.sessionId);
    const tuple = tupleKey(request);
    if (binding === "superseded" || poisonedTuples.has(tuple)) return { registered: false, reason: "superseded" };
    if (binding) return { registered: false, reason: binding };
    const control = controlSeeds.get(tupleKey({ sessionId, deliveryId: request.deliveryId, recordKind: "control-seen" }));
    const deadlineMs = Math.min(request.deadlineMs, control?.originalDeadlineMs ?? request.deadlineMs);
    const deadline = deadlineFailure(deadlineMs, request.signal);
    if (deadline) return { registered: false, reason: deadline };
    if (control && request.deadlineMs > control.originalDeadlineMs) return { registered: false, reason: "deadline" };
    return undefined;
  };

  const validateTrustedDescriptor = (
    request: RecoveryNativeEntryRegistrationRequest,
    trusted: TrustedNativeDescriptor | undefined,
  ): boolean => {
    if (!trusted || request.sessionId !== sessionId || request.sessionId !== trusted.sessionId ||
      request.deliveryId !== trusted.deliveryId || request.recordKind !== "carrier" ||
      request.entryId !== trusted.entryId || request.location.file !== sessionFile ||
      request.location.file !== trusted.file || request.location.offset !== trusted.offset ||
      request.location.length !== trusted.length || request.bytes.length !== request.location.length + 1 ||
      request.bytes.at(-1) !== 0x0a || !sameBytes(request.bytes, trusted.bytes) ||
      request.bytesSha256 !== trusted.bytesSha256 || hash(request.bytes) !== request.bytesSha256 ||
      trusted.committedPrefix.length === 0) return false;

    let decoded: unknown;
    try { decoded = JSON.parse(decoder.decode(request.bytes.subarray(0, -1))); }
    catch { return false; }
    if (!decoded || typeof decoded !== "object" || !("id" in decoded) ||
      (decoded as { id?: unknown }).id !== request.entryId) return false;
    if (request.deliveryId.startsWith(NATIVE_ENTRY_PREFIX) &&
      request.deliveryId !== derivedNativeDeliveryId(request.sessionId, request.entryId)) return false;

    const end = request.location.offset + request.location.length + 1;
    if (!Number.isSafeInteger(request.location.offset) || request.location.offset < 0 ||
      !Number.isSafeInteger(request.location.length) || request.location.length < 0 ||
      end > trusted.committedPrefix.length) return false;

    const receipt = request.flushReceipt;
    if (receipt.kind !== trusted.receipt.kind) return false;
    if (receipt.kind === "file-fsync") {
      if (trusted.receipt.kind !== "file-fsync") return false;
      return receipt.file === sessionFile && receipt.file === trusted.file &&
        receipt.namespaceDurable === true && receipt.bytesThrough >= end &&
        receipt.bytesThrough === trusted.receipt.bytesThrough &&
        trusted.committedPrefix.length === receipt.bytesThrough;
    }
    if (trusted.receipt.kind !== "owned-journal-commit") return false;
    return receipt.file === sessionFile && receipt.file === trusted.file &&
      Number.isSafeInteger(receipt.bytes) && receipt.bytes > 0 && receipt.bytes >= end &&
      receipt.bytes === trusted.committedPrefix.length &&
      /^[a-f0-9]{64}$/.test(receipt.sha256) && receipt.sha256 === hash(trusted.committedPrefix) &&
      receipt.bytes === trusted.receipt.bytes && receipt.sha256 === trusted.receipt.sha256;
  };

  const gateNextCommit = (): B6CommitGate => {
    if (closed) throw new Error("B6 harness binding is closed");
    if (allGates.size > 0) throw new Error("Only one bounded commit gate may be active");
    let enteredResolve!: () => void;
    let releasedResolve!: () => void;
    let didRelease = false;
    let didEnter = false;
    const entered = new Promise<void>(resolve => { enteredResolve = resolve; });
    const released = new Promise<void>(resolve => { releasedResolve = resolve; });
    const state: GateState = {
      entered,
      wait: async () => {
        if (!didEnter) { didEnter = true; enteredResolve(); }
        await released;
      },
      release: () => {
        if (didRelease) return;
        didRelease = true;
        releasedResolve();
        allGates.delete(state);
      },
    };
    queuedGates.push(state);
    allGates.add(state);
    return { entered, release: state.release };
  };
  const enqueue = <T>(run: () => Promise<T>): Promise<T> => {
    const result = writeTail.then(run);
    writeTail = result.then(() => undefined, () => undefined);
    return result;
  };
  const effectiveAppendDeadline = (request: RecoveryDurableAppendRequest): number => {
    const seed = controlSeeds.get(tupleKey({ sessionId: request.sessionId, deliveryId: request.deliveryId, recordKind: "control-seen" }));
    if (request.recordKind === "control-seen") {
      return Math.min(request.deadlineMs, seed?.originalDeadlineMs ?? request.deadlineMs);
    }
    const registration = recoveredRegistrations.get(tupleKey({ sessionId: request.sessionId, deliveryId: request.deliveryId, recordKind: "carrier" }));
    return Math.min(request.deadlineMs, seed?.originalDeadlineMs ?? Number.POSITIVE_INFINITY,
      registration?.request.deadlineMs ?? Number.POSITIVE_INFINITY);
  };
  const append = (request: RecoveryDurableAppendRequest): Promise<RecoveryDurableAppendResult> => {
    const effectiveDeadline = effectiveAppendDeadline(request);
    const early = appendFailure(request, effectiveDeadline);
    if (early) return Promise.resolve(early);
    return enqueue(async () => {
      const deadline = effectiveAppendDeadline(request);
      const failure = appendFailure(request, deadline);
      if (failure) return failure;
      const key = tupleKey(request);
      const existing = recoveredEntries.get(key);
      if (existing) return { durable: true, seq: existing.seq };
      const seed = controlSeeds.get(tupleKey({ sessionId, deliveryId: request.deliveryId, recordKind: "control-seen" }));
      const registration = recoveredRegistrations.get(tupleKey({ sessionId, deliveryId: request.deliveryId, recordKind: "carrier" }));
      if (request.recordKind === "control-seen" && !seed) return { durable: false, reason: "not-found" };
      if (request.recordKind === "carrier" && !registration) return { durable: false, reason: "not-found" };

      const gate = queuedGates.shift();
      if (gate) await gate.wait();
      const afterGateDeadline = effectiveAppendDeadline(request);
      const afterGateFailure = appendFailure(request, afterGateDeadline);
      if (afterGateFailure) return afterGateFailure;
      const committed = recoveredEntries.get(key);
      if (committed) return { durable: true, seq: committed.seq };

      // A carrier registration is correlation only; the actual append commit below is
      // the sole point that creates a B6 sequence and makes readDurableEntry found:true.
      const sourceBytes = request.recordKind === "control-seen" ? seed!.bytes : registration!.request.bytes;
      const sourceDeadline = request.recordKind === "control-seen"
        ? seed!.originalDeadlineMs
        : Math.min(registration!.request.deadlineMs, seed?.originalDeadlineMs ?? registration!.request.deadlineMs);
      const entry: StoredEntry = {
        sessionId, deliveryId: request.deliveryId, recordKind: request.recordKind,
        seq: nextSeq++, bytes: bytes(sourceBytes), originalDeadlineMs: sourceDeadline,
      };
      committedLog.set(key, entry);
      recoveredEntries.set(key, { ...entry, bytes: bytes(entry.bytes) });
      if (request.recordKind === "carrier") committedRegistrations.set(key, {
        request: cloneRegistration(registration!.request), binding: registration!.binding, coverage: registration!.coverage,
      });
      if (loseAck) {
        loseAck = false;
        throw new Error(UNKNOWN_ACK_MESSAGE);
      }
      return { durable: true, seq: entry.seq };
    });
  };

  const adapter: RecoveryDurabilityAdapter = {
    awaitDurableAppend: append,
    async readDurableEntry(request: RecoveryDurableEntryRequest): Promise<RecoveryDurableEntryResult> {
      const failure = bindingFailure(request.sessionId);
      const key = tupleKey(request);
      if (failure === "superseded" || poisonedTuples.has(key)) return { found: false, reason: "superseded" };
      if (failure) return { found: false, reason: failure };
      const entry = recoveredEntries.get(key);
      return entry
        ? { found: true, seq: entry.seq, bytes: bytes(entry.bytes) }
        : { found: false, reason: "not-found" };
    },
    async registerNativeEntry(request: RecoveryNativeEntryRegistrationRequest): Promise<RecoveryNativeEntryRegistrationResult> {
      const failure = registrationFailure(request);
      if (failure) return failure;
      const tuple = tupleKey(request);
      const trusted = trustedDescriptors.get(trustedDescriptorKey(request));
      if (!validateTrustedDescriptor(request, trusted)) {
        return { registered: false, reason: "conflict" };
      }
      const existing = recoveredRegistrations.get(tuple);
      if (!existing) {
        const storedRequest = cloneRegistration(request);
        const stored: StoredRegistration = {
          request: storedRequest, binding: registrationBinding(storedRequest), coverage: receiptCoverage(storedRequest.flushReceipt),
        };
        recoveredRegistrations.set(tuple, stored);
        return { registered: true, existing: false };
      }
      if (!sameBinding(existing.request, request)) {
        poisonedTuples.add(tuple);
        return { registered: false, reason: "conflict" };
      }
      const coverage = receiptCoverage(request.flushReceipt);
      if (coverage > existing.coverage) {
        const storedRequest = cloneRegistration({ ...request, deadlineMs: existing.request.deadlineMs });
        const stronger: StoredRegistration = {
          request: storedRequest, binding: existing.binding, coverage,
        };
        recoveredRegistrations.set(tuple, stronger);
        if (committedLog.has(tuple)) committedRegistrations.set(tuple, stronger);
      }
      return { registered: true, existing: true };
    },
  };

  const rebuild = (state: B6RecoveryState): void => {
    if (closed) return;
    bound = true;
    recoveredEntries = new Map();
    recoveredRegistrations = new Map();
    if (state === "unavailable") {
      available = false;
      recovered = false;
      return;
    }
    available = true;
    if (state === "unknown") {
      recovered = false;
      return;
    }
    recovered = true;
    for (const [key, value] of committedLog) recoveredEntries.set(key, { ...value, bytes: bytes(value.bytes) });
    for (const [key, value] of committedRegistrations) recoveredRegistrations.set(key, {
      request: cloneRegistration(value.request), binding: value.binding, coverage: value.coverage,
    });
    nextSeq = Math.max(0, ...[...committedLog.values()].map(value => value.seq)) + 1;
  };

  const seedControlSeen = (input: {
    readonly deliveryId?: string;
    readonly bytes: Uint8Array;
    readonly originalDeadlineMs: number;
  }): void => {
    if (closed) throw new Error("B6 harness binding is closed");
    if (input.originalDeadlineMs !== originalInputDeadlineMs) throw new Error("Control fixture must retain the harness original deadline");
    const id = input.deliveryId ?? deliveryId;
    const key = tupleKey({ sessionId, deliveryId: id, recordKind: "control-seen" });
    const old = controlSeeds.get(key);
    if (old) {
      if (old.originalDeadlineMs !== input.originalDeadlineMs || !sameBytes(old.bytes, input.bytes)) {
        throw new Error("Control-seen fixture cannot be restamped or overwritten");
      }
      return;
    }
    controlSeeds.set(key, { bytes: bytes(input.bytes), originalDeadlineMs: input.originalDeadlineMs });
  };

  const nativeDescriptorFixture = (options: B6NativeDescriptorFixtureOptions = {}): RecoveryNativeEntryRegistrationRequest => {
    if (closed) throw new Error("B6 harness binding is closed");
    const entryId = options.entryId ?? `test-only-native-entry-${++nextEntryId}`;
    const nativeDeliveryId = options.deliveryId ?? deliveryId;
    const json = options.json ?? JSON.stringify({ id: entryId, message: { role: "user", content: "test-only native carrier" } });
    const prefix = bytes(options.prefixBytes ?? encoder.encode('{"testOnly":"native-prefix"}\n'));
    const trailing = bytes(options.trailingCommittedBytes ?? new Uint8Array());
    const serializedEntry = encoder.encode(`${json}\n`);
    const offset = prefix.length;
    const committedPrefix = concat(prefix, serializedEntry, trailing);
    const deadlineMs = options.deadlineMs ?? originalInputDeadlineMs;
    const receiptKind = options.receiptKind ?? "file-fsync";
    const flushReceipt: RecoveryNativeFlushReceipt = receiptKind === "file-fsync"
      ? { kind: "file-fsync", file: sessionFile, bytesThrough: committedPrefix.length, namespaceDurable: true }
      : { kind: "owned-journal-commit", file: sessionFile, bytes: committedPrefix.length, sha256: hash(committedPrefix) };
    const request: RecoveryNativeEntryRegistrationRequest = {
      sessionId, deliveryId: nativeDeliveryId, recordKind: "carrier", entryId,
      location: { file: sessionFile, offset, length: serializedEntry.length - 1 },
      bytes: bytes(serializedEntry), bytesSha256: hash(serializedEntry), flushReceipt, deadlineMs,
    };
    // This out-of-band fixture grant models trusted native provenance. It is keyed by
    // exact descriptor+receipt value, separate from structural JSON/hash validation, so
    // detached copies can repeat it while an unproved shape-only fixture remains refused.
    if (options.trusted !== false) trustedDescriptors.set(trustedDescriptorKey(request), {
      sessionId, deliveryId: nativeDeliveryId, entryId, file: sessionFile,
      offset, length: serializedEntry.length - 1, bytes: bytes(serializedEntry),
      bytesSha256: hash(serializedEntry), receipt: cloneReceipt(flushReceipt),
      committedPrefix: bytes(committedPrefix),
    });
    return request;
  };

  const observe = (): B6HarnessObservation => {
    const committed: B6CommittedEntryObservation[] = [...committedLog.values()]
      .sort((a, b) => a.seq - b.seq)
      .map(value => ({ ...value, bytes: bytes(value.bytes) }));
    const registrations: B6RegistrationObservation[] = [...recoveredRegistrations.values()].map(({ request }) => ({
      sessionId: request.sessionId, deliveryId: request.deliveryId, entryId: request.entryId,
      location: { ...request.location }, bytes: bytes(request.bytes), bytesSha256: request.bytesSha256,
      flushReceipt: cloneReceipt(request.flushReceipt),
    }));
    const maxSeq = Math.max(0, ...committed.map(value => value.seq));
    return {
      storeId, sessionId, sessionFile, committed, registrations,
      nextSeq: maxSeq + 1,
      nativePiFileIo: { opens: 0, reads: 0, writes: 0, fsyncs: 0 },
    };
  };

  const harness: B6DurabilityHarness = {
    adapter, sessionId, sessionFile, storeId, deliveryId, originalInputDeadlineMs,
    now: () => currentTime,
    setNow(epochMs) {
      if (!Number.isFinite(epochMs)) throw new Error("Harness clock requires finite epoch milliseconds");
      currentTime = epochMs;
    },
    advanceNow(milliseconds) {
      if (!Number.isFinite(milliseconds)) throw new Error("Harness clock advance must be finite");
      currentTime += milliseconds;
    },
    seedControlSeen,
    nativeDescriptorFixture,
    async recover(state = "recovered") { rebuild(state); },
    async reopen(state = "recovered") { rebuild(state); },
    invalidateAvailability() {
      if (closed) return;
      available = false;
      recovered = false;
      recoveredEntries = new Map();
      recoveredRegistrations = new Map();
    },
    gateNextCommit,
    loseNextWriteAck() { loseAck = true; },
    observe,
    async close() {
      if (closed) return;
      closed = true;
      bound = false;
      for (const gate of [...allGates]) gate.release();
      await writeTail;
      recoveredEntries.clear();
      recoveredRegistrations.clear();
      committedRegistrations.clear();
      committedLog.clear();
      controlSeeds.clear();
      poisonedTuples.clear();
    },
  };
  return harness;
}

export const createTestOnlyFakeHarnessFactory: B6DurabilityHarnessFactory = {
  name: "test-only in-memory B6 durability fake",
  create: createTestOnlyFakeHarness,
};
