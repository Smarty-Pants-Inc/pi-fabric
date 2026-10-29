import { createHash, randomUUID } from "node:crypto";
import type { EffectIntent, EffectOutcome, EffectSlot, JsonValue } from "../../../src/effects/contracts.js";
import { canonicalEncode, manifestDigest, mintEffectKey } from "../../../src/effects/slots.js";
import type { AcceptedRecoveryInput, B4EffectRecoveryProviderV1, B4RepairReceiptV1 } from "./contract.js";

export const SESSION = "contract-session";
export const INPUT = "original-input";
/** Stand-in defaults only, never a B5 pin or an expected production identity. */
export const GENERATION = `probe-generation:${randomUUID()}`;
export const INPUT_LIFETIME_MS = 120_000;
export function createAcceptedInput(now: () => number = Date.now): AcceptedRecoveryInput {
  const admissionId = `probe-admission:${randomUUID()}`;
  return {
    sessionId: SESSION, originalInputId: INPUT, admissionId, deliveryId: admissionId,
    kind: "initial", text: "original input", payloadSha256: createHash("sha256").update("original input").digest("hex"),
    deadlineMs: now() + INPUT_LIFETIME_MS, consumption: { kind: "round" },
  };
}
export const accepted = createAcceptedInput();
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
  readonly stallSink?: boolean;
  readonly resultSource?: "native-entry" | "adapter-original";
  readonly fileState?: "pre" | "post" | "neither";
  readonly shellState?: "matching" | "changed";
}
/** Fixture controls, not provider methods. Both adapters receive identical scenarios.
 * Missing real methods are deliberately not shimmed into a false GREEN. */
export interface ConformanceHarness {
  provider: Partial<B4EffectRecoveryProviderV1>;
  /** Independent input/pin observations, never derived from the B4 ref/receipt.
   * Real B5 compositions read selection, retained delivery and ensureRecoveryGeneration.
   * B4-only boundary probes and fakes must label their stand-in identities as such. */
  recoveryIdentity(): RecoveryIdentity;
  issue(intent: EffectIntent): Promise<void>;
  reopen(): Promise<void>;
  destination(): readonly string[];
  observedIntents(): readonly EffectIntent[];
  generations(): readonly string[];
  ledgerDigest(): Promise<string>;
  /** Ordered native-shaped branch at the joined leaf, observed independently of the receipt. */
  joinedBranch(): readonly JoinedEntry[];
  readonly sinkGate: SinkGate;
  advanceJoinedLeaf(nextLeafId?: string): void;
  fileSnapshot(): { bytes: string; identity: string };
  dispose(): Promise<void>;
}
export type ConformanceFactory = (scenario: Scenario) => Promise<ConformanceHarness>;

export interface FixtureIdentity {
  readonly acceptedInput: Readonly<AcceptedRecoveryInput>;
  readonly executionBranchEntryId: string;
}
export interface RecoveryIdentity extends FixtureIdentity {
  readonly recoveryGeneration: string;
}
export const defaultIdentity: FixtureIdentity = { acceptedInput: accepted, executionBranchEntryId: "A" };

/** A deterministic event gate, not a timing assumption. Both real and fake sink
 * probes await it before returning their outcome; C13 releases it in finally. */
export interface SinkGate {
  readonly entered: Promise<void>;
  enter(): Promise<void>;
  release(): void;
}
export function createSinkGate(stall = false): SinkGate {
  let entered!: () => void;
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const observed = new Promise<void>(resolve => { entered = resolve; });
  if (!stall) release();
  return { entered: observed, async enter() { entered(); await gate; }, release };
}

export function intent(id = "write", ordinal = 0, predecessors: readonly string[] = [], peer = false,
  identity: FixtureIdentity = defaultIdentity): EffectIntent {
  const payloadText = peer ? canonicalEncode({ text: "original peer bytes" }) : `original-${id}`;
  const payload = { encoding: "base64" as const, bytes: Buffer.from(payloadText).toString("base64"), digest: hash(payloadText) };
  const slot: EffectSlot = {
    id, ordinal, kind: peer ? "contract-peer" : "contract-file", canonicalRef: peer ? "agents.tell" : "pi.write",
    adapterVersion: "1", canonicalTarget: peer ? "logical-recipient" : `/retained/${id}`,
    predecessors, canonicalArguments: peer ? { text: "original peer bytes" } : { path: id, content: payloadText }, constraints: {},
  };
  const input = identity.acceptedInput;
  const tuple = [1, "contract-org", input.sessionId, input.originalInputId, id, slot.kind] as const;
  return {
    format: "fabric.effect-intent", version: 1, key: mintEffectKey(tuple), keyTuple: tuple,
    manifestDigest: manifestDigest({ version: 1, slots: [{ ...slot, predecessors: [] }] }),
    acceptedInput: { id: input.originalInputId, digest: input.payloadSha256, nativeEntryId: identity.executionBranchEntryId },
    slot, tool: { canonicalRef: slot.canonicalRef, adapterVersion: "1", originalToolCallId: `call-${id}`, nativeAssistantEntryId: "assistant" },
    canonicalArguments: slot.canonicalArguments, canonicalTarget: slot.canonicalTarget, payload,
    ...(peer ? { message: { logicalRecipient: slot.canonicalTarget, immutableBody: { text: "original peer bytes" },
      bodyDigest: payload.digest, firstOfferedAt: "2026-10-01T00:00:00.000Z", originalDeadlineAt: "2026-10-01T00:01:00.000Z",
      envelopeIdentity: { commandId: "original-command" } } } : {}),
    originAttempt: { podUID: "old-pod", bootId: "old-boot", ownerIncarnation: "old-owner", authorityRevision: "old-revision" },
  };
}
export function adapterOriginalResult(value: EffectIntent): JsonValue {
  // Stored adapter original has no native timestamp. Never synthesize one.
  return { role: "toolResult", toolCallId: value.tool.originalToolCallId, toolName: value.tool.canonicalRef.slice(3),
    content: [{ type: "text", text: "original result" }], isError: false };
}
export function originalResult(value: EffectIntent): JsonValue {
  // Complete deliberately non-sorted native envelope exposes canonical-vs-native hashing.
  return { ...adapterOriginalResult(value) as object, timestamp: 12345 };
}
export type JoinedEntry = Readonly<{ id: string; [key: string]: unknown }>;
export function joinedBranch(joinedLeafEntryId = "J", identity: FixtureIdentity = defaultIdentity): readonly JoinedEntry[] {
  // Deliberately non-sorted property order and UTF-8 content: exact Pi history bytes.
  const anchor = identity.executionBranchEntryId;
  return [{ id: anchor, parentId: null, type: "message", message: { role: "user", content: identity.acceptedInput.text } },
    { id: joinedLeafEntryId, parentId: anchor, type: "message", message: { role: "assistant", content: "joined λ" } }];
}
export function joinedEvidence(settlements: B4RepairReceiptV1["settlements"], joinedLeafEntryId = "J",
  identity: FixtureIdentity = defaultIdentity, generation = GENERATION): B4RepairReceiptV1 {
  const branch = joinedBranch(joinedLeafEntryId, identity);
  const raw = JSON.stringify(branch[branch.length - 1]) + "\n";
  // P agent-session.ts:1757-1768, not receipt canonicalEncode or an opaque fixture string.
  const joinedHistorySha256 = hash(JSON.stringify(branch.map(entry => [entry.id, hash(JSON.stringify(entry) + "\n")])));
  return { version: 1, sessionId: identity.acceptedInput.sessionId, originalInputId: identity.acceptedInput.originalInputId, recoveryGeneration: generation,
    executionBranchEntryId: identity.executionBranchEntryId, joinedLeafEntryId, joinedHistorySha256,
    protectedRangeSha256: hash("original calls and exact results"), protectedEntryIds: ["assistant", joinedLeafEntryId], settlements,
    nativeEntries: [{ entryId: joinedLeafEntryId, deliveryId: `pi-native-entry:v1:fixture:${joinedLeafEntryId}`, recordKind: "carrier", file: "/retained/session.jsonl",
      offset: 0, length: Buffer.byteLength(raw) - 1, bytesSha256: hash(raw), b6Sequence: 7 }] };
}
