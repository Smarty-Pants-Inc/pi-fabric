import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { prepareScriptedInput } from "../../../src/recovery/input-admission.js";
import type { EffectIntent, EffectOutcome, EffectSlot, JsonValue } from "../../../src/effects/contracts.js";
import { canonicalEncode, manifestDigest, mintEffectKey } from "../../../src/effects/slots.js";
import type { AcceptedRecoveryInput, B4EffectRecoveryProviderV1, B4RepairReceiptV1 } from "./contract.js";

export const SESSION = "contract-session";
export const INPUT = "original-input";
/** Stand-in defaults only, never a B5 pin or an expected production identity. */
export const GENERATION = `probe-generation:${randomUUID()}`;
export const INPUT_LIFETIME_MS = 120_000;
/** Ask B5 itself for a sender's admission ID; never approximate its grammar/hash.
 * This temporary sender outbox is not a retained receiver admission or a B5 pin. */
export function scriptedAdmissionId(input: Pick<AcceptedRecoveryInput, "sessionId" | "originalInputId" | "text" | "deadlineMs">,
  senderId: string): string {
  const root = mkdtempSync(join(tmpdir(), "b4-contract-sender-"));
  try {
    return prepareScriptedInput({ root, from: { id: senderId, name: "Contract sender", kind: "main" },
      targetId: `session:${input.sessionId}`, message: input.text, delivery: "followUp", triggerTurn: true,
      data: { scriptedInput: { version: 1, inputId: input.originalInputId, sessionId: input.sessionId, deadlineAt: input.deadlineMs } },
    }).idempotencyKey;
  } finally { rmSync(root, { recursive: true, force: true }); }
}
export function createAcceptedInput(now: () => number = Date.now,
  ids: { sessionId: string; originalInputId: string } = { sessionId: SESSION, originalInputId: INPUT }): AcceptedRecoveryInput {
  const input = { ...ids, text: "original input", deadlineMs: now() + INPUT_LIFETIME_MS };
  const admissionId = scriptedAdmissionId(input, `probe-sender:${randomUUID()}`);
  return { ...input, admissionId, deliveryId: admissionId, kind: "initial",
    payloadSha256: createHash("sha256").update(input.text).digest("hex"), consumption: { kind: "round" } };
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
/** Resolve only after the harness has obtained its independent input/pin/anchor.
 * Existing consumer-owned concrete scenarios remain supported. */
export type ScenarioSource = Scenario | ((h: Pick<ConformanceHarness, "recoveryIdentity">) => Scenario);
export type ConformanceFactory = (scenario: ScenarioSource) => Promise<ConformanceHarness>;
export function resolveScenario(source: ScenarioSource, h: Pick<ConformanceHarness, "recoveryIdentity">): Scenario {
  return typeof source === "function" ? source(h) : source;
}

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
  // P cf3cf677: session-manager.ts:2579–2588 (entry order/ISO timestamp),
  // agent-session.ts:5097–5101 (user text blocks/numeric message timestamp).
  // Fixed probe time, not a claim of a native write or recovered timestamp.
  const timestamp = "2026-10-29T00:00:00.000Z";
  return [{ type: "message", id: anchor, parentId: null, timestamp,
    message: { role: "user", content: [{ type: "text", text: identity.acceptedInput.text }], timestamp: Date.parse(timestamp) } },
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
