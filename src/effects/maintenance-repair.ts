import path from "node:path";
import { TextDecoder } from "node:util";
import { EffectContractError } from "./contracts.js";
import type { EffectRecord, RecoveryOwner } from "./contracts.js";
import { readCertifiedToolResult } from "./extension.js";
import type { CertifiedEffectRecordOptions } from "./extension.js";
import type { EffectStore } from "./store.js";
import type { MaintenanceRepairRecordBody, RepairFrontierEntry, StoredMaintenanceRepair } from "./repair-records.js";
import type { B4NativeEvidenceRefV1, NativeAcceptedRecoveryInput, RepairReceiptView, RepairRef } from "./repair-contracts.js";
import type { RecoveryDurabilityAdapter, SettledRecoveryEffect } from "../recovery/l7-interfaces.js";
import { canonicalEncode, sha256Bytes } from "./slots.js";

/** Structural read-only subset of Pi's real SessionManager. Full runtime entries
 * are retained in native property order; an index signature would exclude Pi's ABI. */
export interface RepairNativeEntry { readonly id: string; readonly parentId: string | null; readonly type: string }
export interface RepairNativeLocation { readonly file: string; readonly offset: number; readonly length: number }
export interface RepairNativeScan {
  readonly sessionId: string;
  readonly file: string;
  readonly bytes: number;
  readonly entries: readonly {
    readonly entryId: string; readonly location: RepairNativeLocation;
    readonly bytes: Uint8Array; readonly terminated: boolean;
  }[];
  readonly issues: readonly {
    readonly location: RepairNativeLocation; readonly bytes: Uint8Array;
    readonly reason: "invalid-header" | "invalid-entry" | "duplicate-entry" | "missing-final-lf" | "commit-unproven";
  }[];
}
export interface RepairNativeReader {
  getLeafId(): string | null;
  getBranch(fromId?: string): readonly RepairNativeEntry[];
  inspectRecoveryEntries(): RepairNativeScan;
}
export interface RepairBasis {
  readonly originalInputId: string;
  readonly records: readonly EffectRecord[];
  readonly frontier: readonly RepairFrontierEntry[];
}
export interface RepairCarrierBinding {
  readonly entryId: string; readonly deliveryId: string; readonly bytesSha256: string; readonly seq: number;
}
/** Trusted host-derived occurrence and original metadata, not a public JSON grant. */
export interface BoundRepairOriginal {
  readonly input: Readonly<NativeAcceptedRecoveryInput>;
  readonly originalAdmissionId: string;
  readonly originalPayloadSha256: string;
  readonly originalDeadlineMs: number;
  readonly originalCarrierEntryId: string;
  readonly executionBranchEntryId: string;
  readonly recoveryGeneration: string;
  readonly carriers: readonly RepairCarrierBinding[];
}
export interface MaintenanceRepairEngineOptions {
  readonly owner: RecoveryOwner;
  readonly store: EffectStore;
  readonly native: RepairNativeReader;
  readonly durability: Pick<RecoveryDurabilityAdapter, "readDurableEntry">;
  readonly readNativeToolResult: NonNullable<CertifiedEffectRecordOptions["readNativeToolResult"]>;
  /** Real synchronous native owner/binding assertion, including epoch revocation. */
  assertCurrent(): void;
}

const REQUIRED = "RECOVERY_MAINTENANCE_REPAIR_REQUIRED";
const required = (message: string): never => { throw new EffectContractError(REQUIRED, message); };
const object = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
const text = (v: unknown): v is string => typeof v === "string" && v.length > 0;
const hash = (v: unknown): v is string => typeof v === "string" && /^[a-f0-9]{64}$/.test(v);
const integer = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;
const freeze = <T>(v: T): T => {
  if (v !== null && typeof v === "object") { for (const child of Object.values(v)) freeze(child); Object.freeze(v); }
  return v;
};
const copy = <T>(v: T): T => JSON.parse(canonicalEncode(v)) as T;
const nativeCopy = <T>(v: T): T => {
  // Validate losslessness without using the sorted encoding as the native snapshot.
  canonicalEncode(v);
  return JSON.parse(JSON.stringify(v)) as T;
};
const refFor = (record: Pick<StoredMaintenanceRepair, "view" | "repairReceiptSha256">): RepairRef => freeze({
  originalInputId: record.view.originalInputId, recoveryGeneration: record.view.recoveryGeneration,
  executionBranchEntryId: record.view.executionBranchEntryId, repairReceiptSha256: record.repairReceiptSha256,
});
const refKey = (ref: RepairRef): string => {
  try {
    const encoded = canonicalEncode(ref); // Reject accessors/proxies before reading fields.
    if (!object(ref) || Reflect.ownKeys(ref).length !== 4 ||
        !Object.keys(ref).every(k => ["originalInputId", "recoveryGeneration", "executionBranchEntryId", "repairReceiptSha256"].includes(k)) ||
        !text(ref.originalInputId) || !text(ref.recoveryGeneration) || !text(ref.executionBranchEntryId) || !hash(ref.repairReceiptSha256)) {
      return required("A closed, exact four-field retained repair reference is required");
    }
    return encoded;
  } catch { return required("Malformed repair reference"); }
};
const frontier = (records: readonly EffectRecord[]): readonly RepairFrontierEntry[] => records.map(record => ({
  key: record.intent.key, sequence: record.transitions.at(-1)?.sequence ?? 0,
  digest: record.transitions.at(-1)?.digest ?? record.intentDigest,
})).sort((a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0);
const terminal = (record: EffectRecord) => record.transitions.find(row => row.state === "RESULT");
const definitive = (records: readonly EffectRecord[]): void => {
  if (records.some(record => !terminal(record) || !["committed", "not-committed"].includes(terminal(record)!.outcome ?? ""))) {
    required("Every issued effect of this original must already have a definitive RESULT");
  }
};
const same = (a: unknown, b: unknown): boolean => canonicalEncode(a) === canonicalEncode(b);
const historyHash = (branch: readonly RepairNativeEntry[]): string =>
  sha256Bytes(JSON.stringify(branch.map(entry => [entry.id, sha256Bytes(JSON.stringify(entry) + "\n")])));
const nativeDelivery = (session: string, id: string): string => "pi-native-entry:v1:" + sha256Bytes(JSON.stringify([session, id]));
interface ScannedEntry {
  readonly entry: RepairNativeEntry;
  readonly bytes: Buffer;
  readonly location: RepairNativeLocation;
  readonly bytesSha256: string;
}
interface Snapshot {
  readonly branch: readonly RepairNativeEntry[];
  readonly history: string;
  readonly entries: ReadonlyMap<string, ScannedEntry>;
}
const messageOf = (entry: RepairNativeEntry): Record<string, unknown> | undefined => {
  const value: unknown = entry;
  return entry.type === "message" && object(value) && object(value.message) ? value.message : undefined;
};
const userText = (message: Record<string, unknown>): string | undefined => {
  if (message.role !== "user") return undefined;
  if (typeof message.content === "string") return message.content;
  if (Array.isArray(message.content)) {
    const texts = message.content.filter(block => object(block) && block.type === "text");
    if (texts.length === 1 && object(texts[0]) && typeof texts[0].text === "string" && message.content.every(block => object(block) &&
        (block.type === "text" || (block.type === "image" && typeof block.data === "string" && typeof block.mimeType === "string")))) return texts[0].text;
  }
  return undefined; // Never guess how multiple/transformed text blocks join.
};
const nativeTypes = new Set(["message", "thinking_level_change", "model_change", "usage", "compaction", "branch_summary",
  "custom", "custom_message", "context_edit", "label", "session_info"]);

/** Read-only qualification and one immutable receipt. No B5 import, sink,
 * native writer/recovery, B6 registration/barrier, scheduler or admission policy. */
export function createMaintenanceRepairEngine(options: MaintenanceRepairEngineOptions) {
  const { owner, store, native, durability, readNativeToolResult } = options;
  if (store.owner !== owner) throw new EffectContractError("EFFECT_OWNER_MISMATCH", "Repair must share the exact EffectStore owner");
  const sessionId = owner.logicalSessionId, orgId = owner.orgId, file = owner.sessionFile;
  const index = new Map<string, Readonly<RepairReceiptView>>();
  const invalidateRepairResolution = (): void => { index.clear(); };
  const assertCurrent = (): void => {
    try {
      const returned: unknown = options.assertCurrent();
      if (returned !== undefined || owner.logicalSessionId !== sessionId || owner.orgId !== orgId || owner.sessionFile !== file) {
        throw new EffectContractError("EFFECT_OWNER_STALE", "Repair requires the current synchronous native binding");
      }
    } catch (error) { invalidateRepairResolution(); throw error; }
  };
  const storage = async <T>(operation: () => Promise<T>): Promise<T> => {
    try { return await operation(); }
    catch (error) {
      // Ordinary candidate/frontier/J refusals must not destroy a historical ref.
      if (!(error instanceof EffectContractError) || error.code !== REQUIRED) invalidateRepairResolution();
      throw error;
    }
  };
  const readRepairBasis = async (originalInputId: string): Promise<Readonly<RepairBasis>> => {
    assertCurrent();
    if (!text(originalInputId)) return required("Original input identity is required");
    return storage(() => store.withSession("settle", tx => {
      const records = tx.list().filter(record => record.intent.acceptedInput.id === originalInputId);
      return freeze(copy({ originalInputId, records, frontier: frontier(records) }));
    }));
  };
  const retained = (originalInputId: string) => storage(() => store.withSession("settle", tx => {
    tx.list(); // Validate the complete namespace, including retained receipts.
    return tx.readMaintenanceRepair(originalInputId);
  }));
  const snapshot = (leaf: string, requireCurrent: boolean): Snapshot => {
    assertCurrent();
    try {
      if (requireCurrent && native.getLeafId() !== leaf) return required("Joined native leaf advanced before qualification");
      const branch = nativeCopy(native.getBranch(leaf));
      const scan = native.inspectRecoveryEntries();
      if (scan.sessionId !== sessionId || scan.file !== file || !path.isAbsolute(scan.file) ||
          !integer(scan.bytes) || scan.bytes === 0 || scan.issues.length !== 0) return required("Native scan is incomplete or belongs to another session/file");
      const entries = new Map<string, ScannedEntry>();
      let through = 0;
      for (const candidate of scan.entries) {
        const bytes = Buffer.from(candidate.bytes), location = { ...candidate.location };
        if (!text(candidate.entryId) || entries.has(candidate.entryId) || candidate.terminated !== true ||
            location.file !== file || !integer(location.offset) || !integer(location.length) || location.length === 0 ||
            location.offset === 0 || location.offset < through || !Number.isSafeInteger(location.offset + bytes.length) ||
            location.offset + bytes.length > scan.bytes || bytes.length !== location.length + 1 || bytes.at(-1) !== 10 ||
            bytes.subarray(0, -1).includes(10) || bytes.includes(13)) return required("Invalid native identity, UTF-8 byte coordinates or LF framing");
        // First offset follows the header. Thereafter there can be no unclassified hole.
        if (through !== 0 && location.offset !== through) return required("Unclassified native byte range");
        let entry: unknown;
        try { entry = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, -1))); }
        catch { return required("Native entry is not strict UTF-8 JSON"); }
        if (!object(entry) || entry.id !== candidate.entryId || !nativeTypes.has(String(entry.type)) ||
            (entry.parentId !== null && (!text(entry.parentId) || !entries.has(entry.parentId))) ||
            typeof entry.timestamp !== "string" || JSON.stringify(entry) + "\n" !== bytes.toString("utf8")) {
          return required("Native entry is not exact lossless serializer evidence");
        }
        const parsed = entry as unknown as RepairNativeEntry;
        entries.set(candidate.entryId, { entry: parsed, bytes, location, bytesSha256: sha256Bytes(bytes) });
        through = location.offset + bytes.length;
      }
      if (entries.size === 0 || through !== scan.bytes || branch.length === 0 || branch.at(-1)?.id !== leaf) return required("Incomplete native scan/branch");
      let parent: string | null = null;
      const seen = new Set<string>();
      for (const entry of branch) {
        if (!text(entry.id) || seen.has(entry.id) || entry.parentId !== parent ||
            JSON.stringify(entries.get(entry.id)?.entry) !== JSON.stringify(entry)) return required("Native branch does not match exact retained scan bytes");
        seen.add(entry.id); parent = entry.id;
      }
      return { branch, history: historyHash(branch), entries };
    } catch (error) {
      // A leaf advance is ordinary; a scan/owner/read failure is an observed
      // loss of native authority and must invalidate previously derived usability.
      if (!(error instanceof EffectContractError) || error.code !== REQUIRED || native.getLeafId() === leaf || !requireCurrent) invalidateRepairResolution();
      throw error;
    }
  };
  // Success relies on B6's EARLIER genuine registration provenance + committed
  // log recovery/barrier. This read proves neither a native flush nor independent
  // coordinates: those remain the bound read-only native owner's custody domain.
  const durable = async (entry: ScannedEntry, deliveryId: string, expected?: RepairCarrierBinding): Promise<B4NativeEvidenceRefV1> => {
    let read: Awaited<ReturnType<RecoveryDurabilityAdapter["readDurableEntry"]>>;
    try { read = await durability.readDurableEntry({ sessionId, deliveryId, recordKind: "carrier" }); }
    catch (error) { invalidateRepairResolution(); throw error; }
    if (!read.found) { invalidateRepairResolution(); return required(`B6 committed evidence unavailable: ${read.reason}`); }
    if (!integer(read.seq) || !Buffer.from(read.bytes).equals(entry.bytes) ||
        (expected && (expected.entryId !== entry.entry.id || expected.bytesSha256 !== entry.bytesSha256 || expected.seq !== read.seq))) {
      invalidateRepairResolution(); return required("B6 committed bytes/sequence differ from retained native/B5 evidence");
    }
    return { entryId: entry.entry.id, deliveryId, recordKind: "carrier", file: entry.location.file,
      offset: entry.location.offset, length: entry.location.length, bytesSha256: entry.bytesSha256, b6Sequence: read.seq };
  };
  const qualify = async (binding: Readonly<BoundRepairOriginal>, records: readonly EffectRecord[], leaf: string,
    observed: Snapshot, historical?: RepairReceiptView): Promise<RepairReceiptView> => {
    definitive(records);
    const start = observed.branch.findIndex(entry => entry.id === binding.originalCarrierEntryId);
    const anchor = observed.branch.findIndex(entry => entry.id === binding.executionBranchEntryId);
    if (start < 0 || anchor < 0 || anchor > start) return required("Original carrier D and its execution anchor A are not authenticated ancestors of J");
    const originalCarrier = observed.entries.get(binding.originalCarrierEntryId)!;
    const originalMessage = messageOf(originalCarrier.entry);
    const originalText = originalMessage && userText(originalMessage);
    if (originalText === undefined || sha256Bytes(originalText) !== binding.originalPayloadSha256) return required("Native original carrier text differs from retained original");
    const occurrences = binding.carriers.filter(carrier => carrier.deliveryId === binding.input.deliveryId);
    const occurrence = occurrences.length === 1 ? observed.entries.get(occurrences[0]!.entryId) : undefined;
    const occurrenceMessage = occurrence && messageOf(occurrence.entry);
    const occurrenceText = occurrenceMessage && userText(occurrenceMessage);
    if (!occurrence || !observed.branch.some(entry => entry.id === occurrence.entry.id) || occurrenceText !== binding.input.text ||
        sha256Bytes(binding.input.text) !== binding.input.payloadSha256) return required("Supplied occurrence has no exact native user-carrier ancestry/text binding");
    // Approved Q3: D..J plus all retained physical-alias evidence; Set collapses
    // suffix/alias duplicates, then exact native offsets define the sole order.
    const protectedIds = new Set(observed.branch.slice(start).map(entry => entry.id));
    const settlements: SettledRecoveryEffect[] = [];
    const calls = new Set<string>();
    for (const record of [...records].sort((a, b) => a.intent.slot.ordinal - b.intent.slot.ordinal ||
      (a.intent.key < b.intent.key ? -1 : a.intent.key > b.intent.key ? 1 : 0))) {
      const intent = record.intent, result = terminal(record)!;
      if (intent.acceptedInput.digest !== binding.originalPayloadSha256 ||
          ![binding.originalCarrierEntryId, binding.executionBranchEntryId].includes(intent.acceptedInput.nativeEntryId)) {
        return required("B4 intent differs from the authenticated original native binding");
      }
      const aliases = [{ toolCallId: intent.tool.originalToolCallId, parentToolCallId: intent.tool.parentToolCallId,
        assistantId: intent.tool.nativeAssistantEntryId }, ...record.transitions.filter(row => row.state === "ALIAS").map(row => ({
        toolCallId: row.toolCallId, parentToolCallId: row.parentToolCallId,
        assistantId: object(row.receipt) && text(row.receipt.nativeAssistantEntryId) ? row.receipt.nativeAssistantEntryId : undefined,
      }))];
      let normalizedResult: string | undefined;
      for (const alias of aliases) {
        if (alias.parentToolCallId !== undefined) return required("Q6: actual retained nested-parent materialization/join is not available");
        if (calls.has(alias.toolCallId)) return required("Ambiguous physical call identity across retained original effects");
        calls.add(alias.toolCallId);
        const callEntries = [...observed.entries.values()].filter(candidate => {
          const message = messageOf(candidate.entry);
          return message?.role === "assistant" && Array.isArray(message.content) && message.content.some(block =>
            object(block) && block.type === "toolCall" && block.id === alias.toolCallId);
        });
        const resultEntries = [...observed.entries.values()].filter(candidate => {
          const message = messageOf(candidate.entry);
          return message?.role === "toolResult" && message.toolCallId === alias.toolCallId;
        });
        if (callEntries.length !== 1 || resultEntries.length !== 1) return required("Missing or ambiguous original physical call/result materialization");
        const callEntry = callEntries[0]!, resultEntry = resultEntries[0]!;
        const callMessage = messageOf(callEntry.entry)!;
        const blocks = (callMessage.content as unknown[]).filter(block => object(block) && block.type === "toolCall" && block.id === alias.toolCallId);
        const call = blocks[0];
        const toolName = intent.tool.canonicalRef.startsWith("pi.") ? intent.tool.canonicalRef.slice(3) : intent.tool.canonicalRef;
        if (blocks.length !== 1 || !object(call) || call.name !== toolName || !Object.hasOwn(call, "arguments") || !same(call.arguments, intent.canonicalArguments) ||
            (alias.assistantId !== undefined && alias.assistantId !== callEntry.entry.id) || resultEntry.location.offset <= callEntry.location.offset) {
          return required("Native call identity/arguments/order differ from retained intent/alias");
        }
        // A result must belong to the call's actual ancestry, not just have a matching id.
        let ancestor: ScannedEntry | undefined = resultEntry;
        while (ancestor && ancestor.entry.id !== callEntry.entry.id && ancestor.entry.parentId !== null) ancestor = observed.entries.get(ancestor.entry.parentId);
        if (ancestor?.entry.id !== callEntry.entry.id) return required("Native result is not descended from its actual call");
        const message = messageOf(resultEntry.entry)!;
        if (message.toolName !== toolName) return required("Native result tool differs from retained intent");
        const descriptor: SettledRecoveryEffect = { key: intent.key, toolCallId: alias.toolCallId,
          outcome: result.outcome as "committed" | "not-committed", resultBytesHash: sha256Bytes(JSON.stringify(message)) };
        let joined: Awaited<ReturnType<typeof readCertifiedToolResult>>;
        try { joined = await readCertifiedToolResult({ owner, store, readNativeToolResult }, descriptor); }
        catch (error) {
          if (error instanceof EffectContractError && ["EFFECT_RESULT_UNAVAILABLE", "EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR"].includes(error.code)) {
            return required("Saved original/result materialization cannot authenticate a native join");
          }
          invalidateRepairResolution(); throw error;
        }
        if (joined.source !== "native-entry" || joined.postHook !== true || JSON.stringify(joined.message) !== JSON.stringify(message)) {
          return required("Actual post-hook sink-to-native join is required; adapter originals are not native evidence");
        }
        const { toolCallId: _callId, timestamp: _timestamp, ...rest } = joined.message;
        const normalized = canonicalEncode(rest);
        if (normalizedResult !== undefined && normalizedResult !== normalized) return required("Same-K physical aliases disagree on original result bytes");
        normalizedResult = normalized;
        settlements.push(descriptor); protectedIds.add(callEntry.entry.id); protectedIds.add(resultEntry.entry.id);
      }
    }
    const protectedEntries = [...protectedIds].map(id => observed.entries.get(id)!).sort((a, b) => a.location.offset - b.location.offset);
    const carriers = new Map<string, RepairCarrierBinding>();
    for (const carrier of binding.carriers) {
      if (!text(carrier.entryId) || !text(carrier.deliveryId) || carrier.deliveryId.startsWith("pi-native-entry:v1:") ||
          !hash(carrier.bytesSha256) || !integer(carrier.seq) || carriers.has(carrier.entryId)) return required("Ambiguous retained user-carrier correlation");
      carriers.set(carrier.entryId, carrier);
    }
    if (carriers.get(binding.originalCarrierEntryId)?.deliveryId !== binding.originalAdmissionId) return required("Original D has no exact retained admission correlation");
    const nativeEntries: B4NativeEvidenceRefV1[] = [];
    for (const entry of protectedEntries) {
      const user = messageOf(entry.entry)?.role === "user";
      const carrier = user ? carriers.get(entry.entry.id) : undefined;
      if (user && !carrier) return required("Unknown user carrier in the protected native suffix");
      const deliveryId = carrier?.deliveryId ?? nativeDelivery(sessionId, entry.entry.id);
      nativeEntries.push(await durable(entry, deliveryId, carrier));
    }
    const view: RepairReceiptView = { version: 1, sessionId, originalInputId: binding.input.originalInputId,
      recoveryGeneration: binding.recoveryGeneration, executionBranchEntryId: binding.executionBranchEntryId,
      joinedLeafEntryId: leaf, joinedHistorySha256: observed.history,
      protectedRangeSha256: sha256Bytes(canonicalEncode(["fabric.b4.protected-range", 1,
        nativeEntries.map(entry => [entry.entryId, entry.bytesSha256])])),
      protectedEntryIds: nativeEntries.map(entry => entry.entryId), settlements, nativeEntries };
    if (historical && !same(view, historical)) return required("Retained repair differs from requalified native/B6 history");
    return freeze(copy(view));
  };
  const recheckNative = (leaf: string, observed: Snapshot, view: RepairReceiptView, requireCurrent: boolean): void => {
    const after = snapshot(leaf, requireCurrent);
    if (after.history !== observed.history || view.nativeEntries.some(entry => {
      const candidate = after.entries.get(entry.entryId);
      return !candidate || candidate.bytesSha256 !== entry.bytesSha256 || !same(candidate.location, { file: entry.file, offset: entry.offset, length: entry.length });
    })) { invalidateRepairResolution(); required("Native evidence changed during external qualification"); }
  };
  const prefix = (record: StoredMaintenanceRepair, current: readonly EffectRecord[]): readonly EffectRecord[] => {
    const byKey = new Map(current.map(row => [row.intent.key, row]));
    return record.frontier.map(saved => {
      const row = byKey.get(saved.key);
      if (!row || (saved.sequence === 0 ? row.intentDigest : row.transitions[saved.sequence - 1]?.digest) !== saved.digest) {
        invalidateRepairResolution(); return required("Retained per-original frontier is not an authenticated immutable prefix");
      }
      return freeze({ ...row, transitions: row.transitions.slice(0, saved.sequence) });
    });
  };
  const prepareRepairResolution = async (ref: RepairRef): Promise<void> => {
    const key = refKey(ref); assertCurrent();
    const record = await retained(ref.originalInputId);
    if (!record || refKey(refFor(record)) !== key || sha256Bytes(canonicalEncode(record.view)) !== ref.repairReceiptSha256 || record.view.sessionId !== sessionId) {
      return required("No exact retained receipt authenticates this reference");
    }
    const basis = await readRepairBasis(ref.originalInputId);
    const records = prefix(record, basis.records);
    const observed = snapshot(record.view.joinedLeafEntryId, false);
    if (record.view.nativeEntries.some(entry => !observed.entries.has(entry.entryId))) {
      invalidateRepairResolution(); return required("Retained protected native evidence is missing");
    }
    const original = observed.entries.get(record.originalCarrierEntryId);
    const originalText = original && messageOf(original.entry) && userText(messageOf(original.entry)!);
    if (typeof originalText !== "string") { invalidateRepairResolution(); return required("Retained original carrier has no authentic text"); }
    const binding: BoundRepairOriginal = { input: { sessionId, originalInputId: ref.originalInputId,
      admissionId: record.originalAdmissionId, deliveryId: record.originalAdmissionId, kind: "initial", text: originalText,
      payloadSha256: record.originalPayloadSha256, deadlineMs: record.originalDeadlineMs, consumption: { kind: "round" } },
      originalAdmissionId: record.originalAdmissionId, originalPayloadSha256: record.originalPayloadSha256,
      originalDeadlineMs: record.originalDeadlineMs, originalCarrierEntryId: record.originalCarrierEntryId,
      executionBranchEntryId: ref.executionBranchEntryId, recoveryGeneration: ref.recoveryGeneration,
      carriers: record.view.nativeEntries.filter(entry => messageOf(observed.entries.get(entry.entryId)!.entry)?.role === "user").map(entry => ({
        entryId: entry.entryId, deliveryId: entry.deliveryId, bytesSha256: entry.bytesSha256, seq: entry.b6Sequence,
      })) };
    let view: RepairReceiptView;
    try { view = await qualify(binding, records, record.view.joinedLeafEntryId, observed, record.view); }
    catch (error) { invalidateRepairResolution(); throw error; }
    recheckNative(view.joinedLeafEntryId, observed, view, false);
    // Reread immutable storage/prefix after the external awaits, with no native reentry.
    await storage(() => store.withSession("settle", tx => {
      const current = tx.list().filter(row => row.intent.acceptedInput.id === ref.originalInputId);
      const saved = tx.readMaintenanceRepair(ref.originalInputId);
      if (!saved || saved.recordSha256 !== record.recordSha256) {
        invalidateRepairResolution(); return required("Retained receipt changed during preparation");
      }
      prefix(saved, current);
    }));
    assertCurrent(); index.set(key, view);
  };
  const resolveRepair = (ref: RepairRef): Readonly<RepairReceiptView> => {
    const key = refKey(ref); assertCurrent();
    const view = index.get(key);
    if (!view || view.sessionId !== sessionId || sha256Bytes(canonicalEncode(view)) !== ref.repairReceiptSha256) return required("Repair resolver is cold or reference is unauthenticated");
    return view;
  };
  const certifyRepair = async (supplied: Readonly<BoundRepairOriginal>, executionLeafId: string | null): Promise<RepairRef> => {
    assertCurrent();
    const binding = freeze(copy(supplied));
    if (!text(executionLeafId) || binding.input.sessionId !== sessionId || !text(binding.input.originalInputId) ||
        !text(binding.recoveryGeneration) || !text(binding.executionBranchEntryId) || !text(binding.originalAdmissionId) ||
        !hash(binding.originalPayloadSha256) || !Number.isSafeInteger(binding.originalDeadlineMs) || binding.originalDeadlineMs <= 0) {
      return required("A complete authenticated original and actual non-null joined leaf are required");
    }
    const basis = await readRepairBasis(binding.input.originalInputId);
    definitive(basis.records);
    const old = await retained(binding.input.originalInputId);
    if (old) {
      const ref = refFor(old);
      if (old.originalAdmissionId !== binding.originalAdmissionId || old.originalPayloadSha256 !== binding.originalPayloadSha256 ||
          old.originalDeadlineMs !== binding.originalDeadlineMs || old.originalCarrierEntryId !== binding.originalCarrierEntryId ||
          ref.recoveryGeneration !== binding.recoveryGeneration || ref.executionBranchEntryId !== binding.executionBranchEntryId) {
        return required("Original already has a different immutable repair binding");
      }
      await prepareRepairResolution(ref);
      // J2 refuses fresh production but leaves the historical resolver intact.
      if (old.view.joinedLeafEntryId !== executionLeafId || !same(old.frontier, basis.frontier)) return required("Original already has a receipt at a different joined view/frontier");
      const observed = snapshot(executionLeafId, true);
      if (observed.history !== old.view.joinedHistorySha256) return required("Current joined history differs from immutable repair");
      return ref;
    }
    const observed = snapshot(executionLeafId, true);
    const view = await qualify(binding, basis.records, executionLeafId, observed);
    recheckNative(executionLeafId, observed, view, true);
    const body: MaintenanceRepairRecordBody = { format: "fabric.maintenance-repair-receipt", version: 1, orgId, logicalSessionId: sessionId,
      originalAdmissionId: binding.originalAdmissionId, originalPayloadSha256: binding.originalPayloadSha256,
      originalDeadlineMs: binding.originalDeadlineMs, originalCarrierEntryId: binding.originalCarrierEntryId,
      frontier: basis.frontier, view, repairReceiptSha256: sha256Bytes(canonicalEncode(view)) };
    const published = await storage(() => store.withSession("settle", tx => {
      // SAME lock as publication: no native, B5, callback or nested store entry here.
      const current = tx.list().filter(row => row.intent.acceptedInput.id === binding.input.originalInputId);
      definitive(current);
      if (!same(frontier(current), basis.frontier)) return required("Original key set/frontier changed before final publication");
      return tx.publishMaintenanceRepair(body);
    }));
    recheckNative(executionLeafId, observed, view, true);
    assertCurrent();
    const ref = refFor(published);
    index.set(refKey(ref), freeze(copy(published.view)));
    return ref;
  };
  return Object.freeze({ readRepairBasis, certifyRepair, prepareRepairResolution, resolveRepair, invalidateRepairResolution });
}
export type MaintenanceRepairEngine = ReturnType<typeof createMaintenanceRepairEngine>;
