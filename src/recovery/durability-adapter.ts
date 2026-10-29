import { createHash } from "node:crypto";
import path from "node:path";
import { diagnostics } from "../core/diagnostics.js";
import type {
  RecoveryDurabilityAdapter, RecoveryDurableAppendRequest, RecoveryDurableAppendResult,
  RecoveryDurableEntryRequest, RecoveryDurableEntryResult, RecoveryNativeEntryRegistrationRequest,
  RecoveryNativeEntryRegistrationResult, RecoveryNativeEntryLocation,
} from "./l7-interfaces.js";
import { openRecoveryJournal, RecoveryJournalError, type RecoveryJournal, type RecoveryJournalOptions } from "./durability-adapter-journal.js";

/** Trusted host/native port, invoked AFTER native persistenceReleased, never from inside
 * native persistence. Must establish exact session/file and receipt origin, and for owned
 * receipts the actual NONEMPTY committed append and trusted PREFIX hash/coverage. A receipt
 * shape, later stat/currentReceipt or entry-only hash is NOT that proof. B6 never opens Pi
 * files. The callback gets detached bytes and an abort/deadline-bounded signal; it must honor
 * cancellation and must not reenter native persistence. true is an explicit trust decision. */
export type RecoveryNativeEntryVerifier = (request: RecoveryNativeEntryRegistrationRequest, signal: AbortSignal) => Promise<boolean>;
export interface RecoveryDurabilityStoreOptions extends RecoveryJournalOptions {
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly verifyNativeEntry: RecoveryNativeEntryVerifier;
  /** Constructor's binding commit budget; defaults to ten seconds. Not an input deadline. */
  readonly deadlineMs?: number;
  readonly signal?: AbortSignal;
}
export interface RecoveryControlSeenStageRequest { readonly sessionId: string; readonly deliveryId: string; readonly bytes: Uint8Array }

const PREFIX = "recovery-durability:v1:";
const BINDING = `${PREFIX}binding`;
const RESERVED = "pi-native-entry:v1:";
const sha256 = (bytes: Uint8Array | string): string => createHash("sha256").update(bytes).digest("hex");
const tuple = (request: RecoveryDurableEntryRequest): string => `${PREFIX}entry:${JSON.stringify([request.sessionId, request.deliveryId, request.recordKind])}`;
const boundedReason = (request: { readonly deadlineMs: number; readonly signal?: AbortSignal }): "aborted" | "deadline" | undefined =>
  request.signal?.aborted ? "aborted" : !Number.isFinite(request.deadlineMs) || Date.now() >= request.deadlineMs ? "deadline" : undefined;
const failureReason = (error: unknown): "deadline" | "aborted" | "store-unavailable" | "superseded" => error instanceof RecoveryJournalError ? error.reason : "store-unavailable";
type NativeIdentity = Pick<RecoveryNativeEntryRegistrationRequest, "sessionId" | "deliveryId" | "recordKind" | "entryId" | "location" | "bytes" | "bytesSha256">;
interface NativeBinding { readonly entryId: string; readonly location: RecoveryNativeEntryLocation; readonly bytesSha256: string }
interface Binding {
  readonly version: 1; readonly sessionId: string; readonly deliveryId: string; readonly recordKind: "control-seen" | "carrier";
  readonly bytes: string; readonly native?: NativeBinding;
}
const encode = (binding: Binding): Uint8Array => Buffer.from(JSON.stringify(binding));
const identical = (left: Binding, right: Binding): boolean => Buffer.from(encode(left)).equals(encode(right));
const entryBytes = (binding: Binding): Uint8Array => Uint8Array.from(Buffer.from(binding.bytes, "base64"));
const cloneNative = (request: RecoveryNativeEntryRegistrationRequest): RecoveryNativeEntryRegistrationRequest => ({
  sessionId: request.sessionId, deliveryId: request.deliveryId, recordKind: request.recordKind, entryId: request.entryId,
  location: Object.freeze({ file: request.location.file, offset: request.location.offset, length: request.location.length }),
  bytes: Uint8Array.from(request.bytes), bytesSha256: request.bytesSha256,
  flushReceipt: Object.freeze({ ...request.flushReceipt }), deadlineMs: request.deadlineMs,
  ...(request.signal ? { signal: request.signal } : {}),
});

/** Standalone retained store. Registration/staging is NOT ACK; only awaitDurableAppend
 * commits its copied bytes to the authoritative journal. No startup hooks, native writers,
 * input admission, off-host replication or Kubernetes/gateway activation are installed. */
export class RecoveryDurabilityStore implements RecoveryDurabilityAdapter {
  private readonly staged = new Map<string, Binding>();
  private readonly conflicts = new Set<string>();
  private readonly nativeEntries = new Map<string, string>();
  private constructor(private readonly journal: RecoveryJournal, private readonly options: RecoveryDurabilityStoreOptions) {}
  static async open(options: RecoveryDurabilityStoreOptions): Promise<RecoveryDurabilityStore> {
    if (!options.sessionId || typeof options.sessionId !== "string" || typeof options.sessionFile !== "string" || !path.isAbsolute(options.sessionFile) || typeof options.verifyNativeEntry !== "function") throw new RecoveryJournalError("store-unavailable", "Recovery store requires bound session/absolute file and a trusted native verifier");
    // Capture options once: caller mutation cannot change identity or the native trust port.
    const fixed = Object.freeze({ ...options });
    const journal = await openRecoveryJournal(fixed);
    try {
      const bytes = Buffer.from(JSON.stringify({ version: 1, storeId: fixed.storeId, sessionId: fixed.sessionId, sessionFile: fixed.sessionFile }));
      const existing = journal.get(BINDING);
      if (fixed.required && !existing) throw new RecoveryJournalError("store-unavailable", "Required recovery session binding is missing");
      if (existing && !Buffer.from(existing.bytes).equals(bytes)) throw new RecoveryJournalError("superseded", "Recovery store session/file binding conflicts");
      await journal.append(BINDING, bytes, { deadlineMs: fixed.deadlineMs ?? Date.now() + 10_000, ...(fixed.signal ? { signal: fixed.signal } : {}) });
      const store = new RecoveryDurabilityStore(journal, fixed);
      store.recover();
      return store;
    } catch (error) {
      await journal.close();
      if (error instanceof RecoveryJournalError) throw error;
      throw new RecoveryJournalError("store-unavailable", "Recovery durability store reconstruction is incomplete", { cause: error });
    }
  }

  private validTuple(request: RecoveryDurableEntryRequest): boolean {
    return request.sessionId === this.options.sessionId && typeof request.deliveryId === "string" && request.deliveryId.length > 0 &&
      (request.recordKind === "control-seen" || request.recordKind === "carrier");
  }
  private ensureAvailable(): void { if (!this.journal.get(BINDING)) throw new RecoveryJournalError("store-unavailable", "Recovery store binding is unavailable"); }
  private decode(bytes: Uint8Array, key: string): Binding {
    const value: Binding = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    if (!value || value.version !== 1 || !this.validTuple(value) || tuple(value) !== key || typeof value.bytes !== "string" ||
      Buffer.from(value.bytes, "base64").toString("base64") !== value.bytes || !Buffer.from(encode(value)).equals(Buffer.from(bytes))) throw new Error("Invalid retained recovery tuple");
    // Retained B6 commits already crossed trusted native verification on registration.
    // Revalidate immutable identity only; never manufacture native flush evidence on read.
    if (value.recordKind === "control-seen" ? value.native !== undefined : !value.native || !this.structuralNativeBinding({
      sessionId: value.sessionId, deliveryId: value.deliveryId, recordKind: "carrier", ...value.native, bytes: entryBytes(value),
    })) throw new Error("Invalid retained recovery entry binding");
    return value;
  }
  /** Checks all retained adapter records during open; other journal namespaces are untouched. */
  private recover(): void {
    for (const record of this.journal.entries()) {
      if (!record.key.startsWith(`${PREFIX}entry:`)) continue;
      const value = this.decode(record.bytes, record.key);
      if (value.native) {
        const existing = this.nativeEntries.get(value.native.entryId);
        if (existing && existing !== record.key) throw new Error("Retained native entry is bound to multiple tuples");
        this.nativeEntries.set(value.native.entryId, record.key);
      }
    }
  }
  private current(key: string): Binding | undefined {
    const record = this.journal.get(key);
    return record ? this.decode(record.bytes, key) : this.staged.get(key);
  }
  private conflict(key: string): void {
    if (this.conflicts.has(key)) return;
    this.conflicts.add(key);
    try { diagnostics.warn("src/recovery/durability-store.ts", "Immutable recovery entry binding conflict; original retained, tuple superseded"); } catch { /* diagnostics cannot weaken refusal */ }
  }
  async stageControlSeen(request: RecoveryControlSeenStageRequest): Promise<{ staged: true; existing: boolean }> {
    const snapshot = Uint8Array.from(request.bytes);
    this.ensureAvailable();
    const entry = { sessionId: request.sessionId, deliveryId: request.deliveryId, recordKind: "control-seen" as const };
    if (!this.validTuple(entry)) throw new RecoveryJournalError("superseded", "Control record does not match bound session");
    const key = tuple(entry);
    const binding: Binding = { version: 1, ...entry, bytes: Buffer.from(snapshot).toString("base64") };
    const previous = this.current(key);
    if (this.conflicts.has(key) || previous && !identical(previous, binding)) {
      this.conflict(key); throw new RecoveryJournalError("superseded", "Conflicting immutable control record");
    }
    this.staged.set(key, binding);
    return { staged: true, existing: previous !== undefined };
  }
  private structuralNativeBinding(request: NativeIdentity): boolean {
    const { location, bytes } = request;
    if (!this.validTuple(request) || request.recordKind !== "carrier" || typeof request.entryId !== "string" || !request.entryId ||
      location.file !== this.options.sessionFile || !Number.isSafeInteger(location.offset) || location.offset < 0 ||
      !Number.isSafeInteger(location.length) || location.length <= 0 || !Number.isSafeInteger(location.offset + location.length + 1) ||
      bytes.length !== location.length + 1 || bytes.at(-1) !== 10 || !/^[a-f0-9]{64}$/.test(request.bytesSha256) || sha256(bytes) !== request.bytesSha256) return false;
    try {
      const entry: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, -1)));
      if (!entry || typeof entry !== "object" || !("id" in entry) || entry.id !== request.entryId) return false;
    } catch { return false; }
    if (request.deliveryId.startsWith(RESERVED) && request.deliveryId !== RESERVED + sha256(JSON.stringify([request.sessionId, request.entryId]))) return false;
    return true;
  }
  private structuralNative(request: RecoveryNativeEntryRegistrationRequest): boolean {
    if (!this.structuralNativeBinding(request)) return false;
    const receipt = request.flushReceipt;
    if (receipt.file !== this.options.sessionFile) return false;
    const through = request.location.offset + request.location.length + 1;
    return receipt.kind === "file-fsync" ? Number.isSafeInteger(receipt.bytesThrough) && receipt.bytesThrough >= through && receipt.namespaceDurable === true
      : receipt.kind === "owned-journal-commit" && Number.isSafeInteger(receipt.bytes) && receipt.bytes > 0 && receipt.bytes >= through && /^[a-f0-9]{64}$/.test(receipt.sha256);
  }
  private async verified(request: RecoveryNativeEntryRegistrationRequest): Promise<boolean | "deadline" | "aborted"> {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let onAbort: (() => void) | undefined;
    const stopped = new Promise<"deadline" | "aborted">((resolve) => {
      onAbort = () => { controller.abort(); resolve("aborted"); };
      request.signal?.addEventListener("abort", onAbort, { once: true });
      const deadline = (): void => {
        const remaining = request.deadlineMs - Date.now();
        if (remaining > 0) { timer = setTimeout(deadline, Math.min(2_147_483_647, remaining)); return; }
        controller.abort(); resolve("deadline");
      };
      timer = setTimeout(deadline, Math.min(2_147_483_647, Math.max(0, request.deadlineMs - Date.now())));
    });
    try {
      const reason = boundedReason(request);
      if (reason) return reason;
      const supplied = Object.freeze({ ...cloneNative(request), signal: controller.signal });
      return await Promise.race([Promise.resolve().then(() => this.options.verifyNativeEntry(supplied, controller.signal)), stopped]);
    } finally {
      if (timer) clearTimeout(timer);
      if (onAbort) request.signal?.removeEventListener("abort", onAbort);
    }
  }
  async registerNativeEntry(request: RecoveryNativeEntryRegistrationRequest): Promise<RecoveryNativeEntryRegistrationResult> {
    try {
      const snapshot = cloneNative(request); // every mutable field captured before verifier await
      const reason = boundedReason(snapshot);
      if (reason) return { registered: false, reason };
      this.ensureAvailable();
      if (!this.structuralNative(snapshot)) return { registered: false, reason: "conflict" };
      const key = tuple(snapshot);
      const binding: Binding = {
        version: 1, sessionId: snapshot.sessionId, deliveryId: snapshot.deliveryId, recordKind: "carrier", bytes: Buffer.from(snapshot.bytes).toString("base64"),
        native: { entryId: snapshot.entryId, location: snapshot.location, bytesSha256: snapshot.bytesSha256 },
      };
      const checkConflict = (): boolean => {
        const previous = this.current(key);
        const other = this.nativeEntries.get(snapshot.entryId);
        if (this.conflicts.has(key) || previous && !identical(previous, binding) || other && other !== key) {
          this.conflict(key); if (other) this.conflict(other); return true;
        }
        return false;
      };
      if (checkConflict()) return { registered: false, reason: "conflict" };
      const verified = await this.verified(snapshot);
      if (verified === "deadline" || verified === "aborted") return { registered: false, reason: verified };
      if (verified !== true) return { registered: false, reason: "conflict" };
      const late = boundedReason(snapshot);
      if (late) return { registered: false, reason: late };
      this.ensureAvailable();
      if (checkConflict()) return { registered: false, reason: "conflict" };
      const existing = this.current(key) !== undefined;
      this.staged.set(key, binding); this.nativeEntries.set(snapshot.entryId, key);
      return { registered: true, existing };
    } catch (error) { return { registered: false, reason: failureReason(error) }; }
  }
  async awaitDurableAppend(request: RecoveryDurableAppendRequest): Promise<RecoveryDurableAppendResult> {
    // Request objects are caller-owned too; do not re-read identity after an await.
    const snapshot = { sessionId: request.sessionId, deliveryId: request.deliveryId, recordKind: request.recordKind, deadlineMs: request.deadlineMs, ...(request.signal ? { signal: request.signal } : {}) };
    const reason = boundedReason(snapshot);
    if (reason) return { durable: false, reason };
    if (!this.validTuple(snapshot)) return { durable: false, reason: "superseded" };
    const key = tuple(snapshot);
    try {
      this.ensureAvailable();
      if (this.conflicts.has(key)) return { durable: false, reason: "superseded" };
      const binding = this.current(key);
      if (!binding) return { durable: false, reason: "not-found" };
      const result = await this.journal.append(key, encode(binding), snapshot);
      this.ensureAvailable();
      if (this.conflicts.has(key)) return { durable: false, reason: "superseded" };
      const late = boundedReason(snapshot);
      if (late) return { durable: false, reason: late };
      return { durable: true, seq: result.seq };
    } catch (error) { return { durable: false, reason: failureReason(error) }; }
  }
  async readDurableEntry(request: RecoveryDurableEntryRequest): Promise<RecoveryDurableEntryResult> {
    if (!this.validTuple(request)) return { found: false, reason: "superseded" };
    try {
      this.ensureAvailable();
      const key = tuple(request);
      if (this.conflicts.has(key)) return { found: false, reason: "superseded" };
      const record = this.journal.get(key);
      if (!record) return { found: false, reason: "not-found" };
      return { found: true, seq: record.seq, bytes: entryBytes(this.decode(record.bytes, key)) };
    } catch { return { found: false, reason: "store-unavailable" }; }
  }
  async close(): Promise<void> { await this.journal.close(); }
}

/** REQUIRED open must recover both the journal identity and its exact bound session/file.
 * No cached object or absent/incomplete binding can create negative authority. */
export async function openRecoveryDurabilityStore(options: RecoveryDurabilityStoreOptions): Promise<RecoveryDurabilityStore> {
  return RecoveryDurabilityStore.open(options);
}
