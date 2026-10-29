import { createHash } from "node:crypto";
import type { EffectIntent, EffectOutcome, EffectSlot, JsonValue } from "../../../src/effects/contracts.js";
import { canonicalEncode, manifestDigest, mintEffectKey } from "../../../src/effects/slots.js";
import type { AcceptedRecoveryInput, B4EffectRecoveryProviderV1, B4RepairReceiptV1 } from "./contract.js";

export const SESSION = "contract-session";
export const INPUT = "original-input";
export const GENERATION = "b5-durable-generation";
export const accepted: AcceptedRecoveryInput = {
  sessionId: SESSION, originalInputId: INPUT, admissionId: "admission", deliveryId: "delivery",
  kind: "initial", text: "original input", payloadSha256: createHash("sha256").update("original input").digest("hex"),
  deadlineMs: 1793404800000, consumption: { kind: "round" },
};
export const hash = (bytes: string | Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
export const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
export const canonicalCopy = <T>(value: T): T => JSON.parse(canonicalEncode(value)) as T;
export function fail(code: string): never { throw Object.assign(new Error(code), { code }); }
export const immutable = <T>(value: T): T => {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) immutable(child);
    Object.freeze(value);
  }
  return value;
};
export interface Scenario {
  readonly intents?: readonly EffectIntent[];
  readonly outcomes?: Readonly<Record<string, EffectOutcome>>;
  readonly committedBeforeAck?: readonly string[];
  readonly alias?: { readonly key: string; readonly toolCallId: string };
  readonly corrupt?: boolean;
  readonly lostAck?: boolean;
  readonly pin?: string | null;
  readonly fileState?: "pre" | "post" | "neither";
  readonly shellState?: "matching" | "changed";
}
/** Fixture controls, not provider methods. Both adapters receive identical scenarios.
 * Missing real methods are deliberately not shimmed into a false GREEN. */
export interface ConformanceHarness {
  provider: Partial<B4EffectRecoveryProviderV1>;
  issue(intent: EffectIntent): Promise<void>;
  reopen(): Promise<void>;
  destination(): readonly string[];
  observedIntents(): readonly EffectIntent[];
  generations(): readonly string[];
  ledgerDigest(): Promise<string>;
  /** Ordered native-shaped branch at the joined leaf, observed independently of the receipt. */
  joinedBranch(): readonly JoinedEntry[];
  advanceJoinedLeaf(): void;
  fileSnapshot(): { bytes: string; identity: string };
  dispose(): Promise<void>;
}
export type ConformanceFactory = (scenario: Scenario) => Promise<ConformanceHarness>;

export function intent(id = "write", ordinal = 0, predecessors: readonly string[] = [], peer = false): EffectIntent {
  const payloadText = peer ? canonicalEncode({ text: "original peer bytes" }) : `original-${id}`;
  const payload = { encoding: "base64" as const, bytes: Buffer.from(payloadText).toString("base64"), digest: hash(payloadText) };
  const slot: EffectSlot = {
    id, ordinal, kind: peer ? "contract-peer" : "contract-file", canonicalRef: peer ? "agents.tell" : "pi.write",
    adapterVersion: "1", canonicalTarget: peer ? "logical-recipient" : `/retained/${id}`,
    predecessors, canonicalArguments: peer ? { text: "original peer bytes" } : { path: id, content: payloadText }, constraints: {},
  };
  const tuple = [1, "contract-org", SESSION, INPUT, id, slot.kind] as const;
  return {
    format: "fabric.effect-intent", version: 1, key: mintEffectKey(tuple), keyTuple: tuple,
    manifestDigest: manifestDigest({ version: 1, slots: [{ ...slot, predecessors: [] }] }),
    acceptedInput: { id: INPUT, digest: hash("original input"), nativeEntryId: "A" },
    slot, tool: { canonicalRef: slot.canonicalRef, adapterVersion: "1", originalToolCallId: `call-${id}`, nativeAssistantEntryId: "assistant" },
    canonicalArguments: slot.canonicalArguments, canonicalTarget: slot.canonicalTarget, payload,
    ...(peer ? { message: { logicalRecipient: slot.canonicalTarget, immutableBody: { text: "original peer bytes" },
      bodyDigest: payload.digest, firstOfferedAt: "2026-10-01T00:00:00.000Z", originalDeadlineAt: "2026-10-01T00:01:00.000Z",
      envelopeIdentity: { commandId: "original-command" } } } : {}),
    originAttempt: { podUID: "old-pod", bootId: "old-boot", ownerIncarnation: "old-owner", authorityRevision: "old-revision" },
  };
}
export function originalResult(value: EffectIntent): JsonValue {
  // Complete deliberately non-sorted native envelope exposes canonical-vs-native hashing.
  return { role: "toolResult", toolCallId: value.tool.originalToolCallId, toolName: "write",
    content: [{ type: "text", text: "original result" }], isError: false, timestamp: 12345 };
}
export type JoinedEntry = Readonly<{ id: string; [key: string]: unknown }>;
export function joinedBranch(joinedLeafEntryId = "J"): readonly JoinedEntry[] {
  // Deliberately non-sorted property order and UTF-8 content: exact Pi history bytes.
  return [{ id: "A", parentId: null, type: "message", message: { role: "user", content: "original input" } },
    { id: joinedLeafEntryId, parentId: "A", type: "message", message: { role: "assistant", content: "joined λ" } }];
}
export function joinedEvidence(settlements: B4RepairReceiptV1["settlements"], joinedLeafEntryId = "J"): B4RepairReceiptV1 {
  const branch = joinedBranch(joinedLeafEntryId);
  const raw = JSON.stringify(branch[branch.length - 1]) + "\n";
  // P agent-session.ts:1757-1768, not receipt canonicalEncode or an opaque fixture string.
  const joinedHistorySha256 = hash(JSON.stringify(branch.map(entry => [entry.id, hash(JSON.stringify(entry) + "\n")])));
  return { version: 1, sessionId: SESSION, originalInputId: INPUT, recoveryGeneration: GENERATION,
    executionBranchEntryId: "A", joinedLeafEntryId, joinedHistorySha256,
    protectedRangeSha256: hash("original calls and exact results"), protectedEntryIds: ["assistant", "J"], settlements,
    nativeEntries: [{ entryId: "J", deliveryId: "pi-native-entry:v1:fixture", recordKind: "carrier", file: "/retained/session.jsonl",
      offset: 0, length: Buffer.byteLength(raw) - 1, bytesSha256: hash(raw), b6Sequence: 7 }] };
}
