import { Buffer } from "node:buffer";
import fs from "node:fs";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  EffectContractError,
  type EffectAttemptIdentity,
  type EffectIntent,
  type EffectManifest,
  type EffectRecord,
  type EffectSink,
  type EffectSlot,
  type SettledEffect,
  type EffectTransitionInput,
  type JsonValue,
  type RecoveryOwner,
} from "../src/effects/contracts.js";
import { EffectStore } from "../src/effects/store.js";
import {
  canonicalEncode,
  intentBindingDigest,
  manifestDigest,
  mintEffectKey,
  resolveEffectSlot,
  sha256Bytes,
  validateIntent,
} from "../src/effects/slots.js";
import {
  bindEffectReconciler,
  createEffectReconciler,
  reconcilePending,
  readEffectResult,
  readToolResult,
  settlePendingIntents,
} from "../src/effects/reconcile.js";

const ownedRoots: string[] = [];
const timestamp = "2026-01-02T03:04:05.000Z";
const baseAttempt: EffectAttemptIdentity = {
  podUID: "pod-original",
  bootId: "boot-original",
  ownerIncarnation: "incarnation-original",
  authorityRevision: "revision-original",
};

afterEach(() => {
  for (const root of ownedRoots.splice(0)) {
    // Every root in this list came from this file's own mkdtempSync call.
    rmSync(root, { recursive: true, force: true });
  }
});

function expectContractCode(action: () => unknown, code: string): unknown {
  let thrown: unknown;
  try { action(); } catch (error) { thrown = error; }
  expect(thrown).toBeInstanceOf(EffectContractError);
  expect(thrown).toMatchObject({ code });
  return thrown;
}

async function expectRejectedCode(promise: Promise<unknown>, code: string): Promise<void> {
  let thrown: unknown;
  try { await promise; } catch (error) { thrown = error; }
  expect(thrown).toBeInstanceOf(EffectContractError);
  expect(thrown).toMatchObject({ code });
}

function makeOwner(root = mkdtempSync(join(tmpdir(), "pi-fabric-effects-store-"))): RecoveryOwner {
  ownedRoots.push(root);
  const sessionRoot = join(root, "session");
  const workspaceRoot = join(root, "workspace");
  mkdirSync(sessionRoot, { recursive: true, mode: 0o700 });
  mkdirSync(workspaceRoot, { recursive: true, mode: 0o700 });
  const sessionFile = join(root, "session.json");
  writeFileSync(sessionFile, "{}", { mode: 0o600 });
  return {
    orgId: "org-test",
    logicalSessionId: "session-test",
    sessionRoot: realpathSync(sessionRoot),
    workspaceRoot: realpathSync(workspaceRoot),
    sessionFile,
    pvcUID: "pvc-test",
    exclusionReceipt: "exclusive-owner-receipt",
    attempt: baseAttempt,
    signal: new AbortController().signal,
    assertLive() {},
  };
}

type SlotSeed = {
  readonly id: string;
  readonly ordinal: number;
  readonly kind?: string;
  readonly canonicalRef?: string;
  readonly adapterVersion?: string;
  readonly canonicalTarget?: string;
  readonly predecessors?: readonly string[];
  readonly canonicalArguments?: JsonValue;
  readonly constraints?: JsonValue;
  readonly phase?: string;
};

type IntentOptions = {
  readonly slots?: readonly SlotSeed[];
  readonly slotId?: string;
  readonly inputId?: string;
  readonly toolCallId?: string;
  readonly parentToolCallId?: string;
  readonly payloadText?: string;
  readonly records?: readonly EffectRecord[];
};

function slotSeed(id: string, ordinal: number, canonicalArguments: JsonValue = { path: "result.txt" }, extra: Partial<SlotSeed> = {}): SlotSeed {
  return {
    id,
    ordinal,
    kind: "file.write",
    canonicalRef: "fabric.file.write",
    adapterVersion: "adapter-v1",
    predecessors: [],
    canonicalArguments,
    constraints: {},
    ...extra,
  };
}

function manifestFor(owner: RecoveryOwner, seeds: readonly SlotSeed[]): EffectManifest {
  return {
    version: 1,
    slots: seeds.map((seed): EffectSlot => ({
      id: seed.id,
      ordinal: seed.ordinal,
      kind: seed.kind ?? "file.write",
      canonicalRef: seed.canonicalRef ?? "fabric.file.write",
      adapterVersion: seed.adapterVersion ?? "adapter-v1",
      canonicalTarget: seed.canonicalTarget ?? join(owner.workspaceRoot, "result.txt"),
      predecessors: seed.predecessors ?? [],
      canonicalArguments: seed.canonicalArguments ?? { path: "result.txt" },
      constraints: seed.constraints ?? {},
      ...(seed.phase === undefined ? {} : { phase: seed.phase }),
    })),
  };
}

function makeIntent(owner: RecoveryOwner, options: IntentOptions = {}): EffectIntent {
  const seeds = options.slots ?? [slotSeed("file-draft/0", 0)];
  const manifest = manifestFor(owner, seeds);
  const inputId = options.inputId ?? "accepted-input-original";
  const acceptedInput = {
    id: inputId,
    digest: sha256Bytes("accepted input bytes"),
    nativeEntryId: "native-assistant-entry-original",
    manifestDigest: manifestDigest(manifest),
    manifest,
  };
  const selected = manifest.slots.find((candidate) => candidate.id === (options.slotId ?? manifest.slots[0]?.id));
  if (!selected) throw new Error("test fixture selected an absent manifest slot");
  const slot = resolveEffectSlot({
    acceptedInput,
    kind: selected.kind,
    canonicalRef: selected.canonicalRef,
    adapterVersion: selected.adapterVersion,
    canonicalTarget: selected.canonicalTarget,
    canonicalArguments: selected.canonicalArguments,
    ...(selected.phase === undefined ? {} : { phase: selected.phase }),
    records: options.records ?? [],
  });
  const tuple = [1, owner.orgId, owner.logicalSessionId, inputId, slot.id, slot.kind] as const;
  const bytes = Buffer.from(options.payloadText ?? "stored original bytes", "utf8");
  const payload = {
    encoding: "base64" as const,
    bytes: bytes.toString("base64"),
    digest: sha256Bytes(bytes),
  };
  const target = slot.canonicalTarget;
  const callId = options.toolCallId ?? `physical-call-${slot.id}`;
  const tool = {
    canonicalRef: slot.canonicalRef,
    adapterVersion: slot.adapterVersion,
    originalToolCallId: callId,
    ...(options.parentToolCallId === undefined ? {} : { parentToolCallId: options.parentToolCallId }),
    nativeAssistantEntryId: acceptedInput.nativeEntryId,
  };
  const preparedResult: JsonValue = { path: target, content: options.payloadText ?? "stored original bytes" };
  return {
    format: "fabric.effect-intent",
    version: 1,
    key: mintEffectKey(tuple),
    keyTuple: tuple,
    manifestDigest: acceptedInput.manifestDigest,
    acceptedInput: { id: acceptedInput.id, digest: acceptedInput.digest, nativeEntryId: acceptedInput.nativeEntryId },
    slot,
    tool,
    canonicalArguments: slot.canonicalArguments,
    payload,
    canonicalTarget: target,
    file: {
      preExists: false,
      preHash: null,
      preMode: null,
      postHash: payload.digest,
      postMode: 0o644,
      desiredBytes: payload,
      preparedResult,
    },
    originAttempt: owner.attempt,
  };
}

function transition(owner: RecoveryOwner, input: Partial<EffectTransitionInput> & Pick<EffectTransitionInput, "state" | "toolCallId">): EffectTransitionInput {
  return {
    state: input.state,
    toolCallId: input.toolCallId,
    attempt: input.attempt ?? owner.attempt,
    at: input.at ?? timestamp,
    ...(input.parentToolCallId === undefined ? {} : { parentToolCallId: input.parentToolCallId }),
    recoveryGeneration: input.recoveryGeneration ?? "recovery-generation-test-α",
    ...(input.outcome === undefined ? {} : { outcome: input.outcome }),
    ...(input.result === undefined ? {} : { result: input.result }),
    ...(input.resultBytesHash === undefined ? {} : { resultBytesHash: input.resultBytesHash }),
    ...(input.receipt === undefined ? {} : { receipt: input.receipt }),
    ...(input.reason === undefined ? {} : { reason: input.reason }),
  };
}

function resultHash(result: JsonValue): string {
  return sha256Bytes(canonicalEncode(result));
}

async function bind(store: EffectStore, intent: EffectIntent): Promise<EffectRecord> {
  return store.withSession("new-intent", (tx) => tx.bind(intent));
}

async function persistResult(
  owner: RecoveryOwner,
  store: EffectStore,
  intent: EffectIntent,
  result: JsonValue = intent.file?.preparedResult ?? { ok: true },
  aliases: readonly { readonly toolCallId: string; readonly parentToolCallId?: string }[] = [],
): Promise<EffectRecord> {
  return store.withSession("settle", (tx) => {
    for (const alias of aliases) {
      tx.append(intent.key, transition(owner, {
        state: "ALIAS",
        toolCallId: alias.toolCallId,
        ...(alias.parentToolCallId === undefined ? {} : { parentToolCallId: alias.parentToolCallId }),
      }));
    }
    tx.append(intent.key, transition(owner, { state: "ATTEMPT", toolCallId: intent.tool.originalToolCallId }));
    return tx.append(intent.key, transition(owner, {
      state: "RESULT",
      toolCallId: intent.tool.originalToolCallId,
      ...(intent.tool.parentToolCallId === undefined ? {} : { parentToolCallId: intent.tool.parentToolCallId }),
      outcome: "committed",
      result,
      resultBytesHash: resultHash(result),
    }));
  });
}

function keyDirectory(owner: RecoveryOwner, key: string): string {
  return join(owner.sessionRoot, "effects", key);
}

function transitionDirectory(owner: RecoveryOwner, key: string): string {
  return join(keyDirectory(owner, key), "transitions");
}

function settledFor(result: { readonly settled: readonly { readonly key: string; readonly outcome: string; readonly toolCallId: string }[] }, key: string) {
  return result.settled.filter((item) => item.key === key);
}

function fileSink(
  owner: RecoveryOwner,
  onCall: (intent: EffectIntent) => void = () => undefined,
  onWrite: (intent: EffectIntent) => void = () => undefined,
): EffectSink {
  return {
    kind: "file.write",
    adapterVersion: "adapter-v1",
    async reconcile(intent, context) {
      if (context.owner !== owner) throw new Error("sink received a different recovery owner");
      onCall(intent);
      const file = intent.file;
      if (!file) return { outcome: "uncertain", reason: "missing prepared file bytes" };
      const desired = Buffer.from(file.desiredBytes.bytes, "base64");
      let present: Buffer | undefined;
      try {
        present = readFileSync(intent.canonicalTarget);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
      if (!present) {
        onWrite(intent);
        writeFileSync(intent.canonicalTarget, desired, { mode: file.postMode });
        present = readFileSync(intent.canonicalTarget);
      }
      if (!present.equals(desired)) return { outcome: "uncertain", reason: "destination is neither the stored pre-state nor post-state" };
      return {
        outcome: "committed",
        result: file.preparedResult,
        resultBytesHash: resultHash(file.preparedResult),
        receipt: { observedPostHash: sha256Bytes(present) },
      };
    },
  };
}

describe("effect slots and canonical bindings", () => {
  it("encodes strict JSON canonically, rejects non-JSON and invalid Unicode, and preserves valid Unicode", () => {
    expect(canonicalEncode({ z: "💩", a: "é", nested: { z: 1, b: [true, null] } })).toBe(
      '{"a":"é","nested":{"b":[true,null],"z":1},"z":"💩"}',
    );
    expect(canonicalEncode([1, "é", { beta: 2, alpha: 1 }])).toBe('[1,"é",{"alpha":1,"beta":2}]');

    const cycle: { self?: unknown } = {};
    cycle.self = cycle;
    const sparse: unknown[] = new Array(2);
    for (const invalid of [
      undefined,
      { value: undefined },
      NaN,
      Infinity,
      -Infinity,
      1n,
      () => 1,
      Symbol("not-json"),
      sparse,
      cycle,
      new Date(0),
      "\ud800",
      { "\udfff": "invalid key" },
    ]) {
      expectContractCode(() => canonicalEncode(invalid), "EFFECT_CANONICAL_INVALID");
    }
  });

  it("matches fixed SHA-256/key vectors and excludes physical call identities from K", () => {
    expect(sha256Bytes("abc")).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    const tuple = [1, "org", "session", "input", "file-draft/0", "write"] as const;
    expect(canonicalEncode(tuple)).toBe('[1,"org","session","input","file-draft/0","write"]');
    expect(mintEffectKey(tuple)).toBe("fe424a9387593f933a65c3c02c23e3e99e6d031c8fc8957de231ab6425d11766");

    const owner = makeOwner();
    const original = makeIntent(owner, { toolCallId: "call-before-restart" });
    const replayed = makeIntent(owner, { toolCallId: "call-after-restart" });
    expect(replayed.keyTuple).toEqual(original.keyTuple);
    expect(replayed.key).toBe(original.key);
    expect(mintEffectKey(original.keyTuple)).toBe(mintEffectKey(replayed.keyTuple));
  });

  it("keeps distinct same-kind/same-target manifest slots distinct and refuses indistinguishable selection", () => {
    const owner = makeOwner();
    const slots = [
      slotSeed("file-draft/0", 0, { path: "result.txt", content: "draft" }),
      slotSeed("file-edit/1", 1, { path: "result.txt", content: "revised" }),
    ];
    const first = makeIntent(owner, { slots, slotId: "file-draft/0" });
    const second = makeIntent(owner, { slots, slotId: "file-edit/1" });
    expect(first.slot.kind).toBe(second.slot.kind);
    expect(first.canonicalTarget).toBe(second.canonicalTarget);
    expect(first.key).not.toBe(second.key);

    const ambiguous = [
      slotSeed("candidate-a", 0, { path: "result.txt", content: "same" }),
      slotSeed("candidate-b", 1, { path: "result.txt", content: "same" }),
    ];
    const ambiguousManifest = manifestFor(owner, ambiguous);
    const acceptedInput = {
      id: "accepted-input-ambiguous",
      digest: sha256Bytes("accepted input bytes"),
      nativeEntryId: "native-entry-ambiguous",
      manifestDigest: manifestDigest(ambiguousManifest),
      manifest: ambiguousManifest,
    };
    expectContractCode(() => resolveEffectSlot({
      acceptedInput,
      kind: "file.write",
      canonicalRef: "fabric.file.write",
      adapterVersion: "adapter-v1",
      canonicalTarget: ambiguousManifest.slots[0]!.canonicalTarget,
      canonicalArguments: { path: "result.txt", content: "same" },
      records: [],
    }), "EFFECT_SLOT_AMBIGUOUS");
  });

  it.skipIf(process.platform === "win32")("enforces predecessors, replays a historical slot after later results, and rejects changed requests", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const slots = [
      slotSeed("draft", 0, { path: "result.txt", content: "draft" }),
      slotSeed("edit", 1, { path: "result.txt", content: "final" }, { predecessors: ["draft"] }),
    ];
    const draft = makeIntent(owner, { slots, slotId: "draft", toolCallId: "draft-call" });
    const editSeed = slots[1]!;
    const acceptedInput = {
      id: draft.acceptedInput.id,
      digest: draft.acceptedInput.digest,
      nativeEntryId: draft.acceptedInput.nativeEntryId,
      manifestDigest: draft.manifestDigest,
      manifest: manifestFor(owner, slots),
    };
    expectContractCode(() => resolveEffectSlot({
      acceptedInput,
      kind: editSeed.kind ?? "file.write",
      canonicalRef: editSeed.canonicalRef ?? "fabric.file.write",
      adapterVersion: editSeed.adapterVersion ?? "adapter-v1",
      canonicalTarget: draft.slot.canonicalTarget,
      canonicalArguments: editSeed.canonicalArguments ?? { path: "result.txt" },
      records: [],
    }), "EFFECT_SLOT_PREDECESSOR");

    await bind(store, draft);
    await persistResult(owner, store, draft, { revision: "draft-result" });
    const edit = makeIntent(owner, { slots, slotId: "edit", toolCallId: "edit-call", records: await store.withSession("settle", (tx) => tx.list()) });
    await bind(store, edit);
    await persistResult(owner, store, edit, { revision: "final-result" });

    const history = await store.withSession("settle", (tx) => tx.list());
    const replayedDraft = resolveEffectSlot({
      acceptedInput,
      kind: "file.write",
      canonicalRef: "fabric.file.write",
      adapterVersion: "adapter-v1",
      canonicalTarget: draft.slot.canonicalTarget,
      canonicalArguments: slots[0]!.canonicalArguments!,
      records: history,
    });
    expect(replayedDraft.id).toBe("draft");
    expect(() => resolveEffectSlot({
      acceptedInput,
      kind: "file.write",
      canonicalRef: "fabric.file.write",
      adapterVersion: "adapter-v1",
      canonicalTarget: draft.slot.canonicalTarget,
      canonicalArguments: { path: "result.txt", content: "different request" },
      records: history,
    })).toThrow();
  });

  it.skipIf(process.platform === "win32")("reports a changed request against one bound slot with its original durable result", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const slots = [slotSeed("one-slot", 0, { path: "result.txt", content: "original request" })];
    const intent = makeIntent(owner, { slots, slotId: "one-slot" });
    const originalResult: JsonValue = { error: { code: "EIO", message: "original saved failure" } };
    await bind(store, intent);
    await persistResult(owner, store, intent, originalResult);
    const manifest = manifestFor(owner, slots);
    const acceptedInput = {
      id: intent.acceptedInput.id,
      digest: intent.acceptedInput.digest,
      nativeEntryId: intent.acceptedInput.nativeEntryId,
      manifestDigest: intent.manifestDigest,
      manifest,
    };
    const records = await store.withSession("settle", (tx) => tx.list());
    const error = expectContractCode(() => resolveEffectSlot({
      acceptedInput,
      kind: intent.slot.kind,
      canonicalRef: intent.slot.canonicalRef,
      adapterVersion: intent.slot.adapterVersion,
      canonicalTarget: intent.slot.canonicalTarget,
      canonicalArguments: { path: "result.txt", content: "changed request" },
      records,
    }), "EFFECT_SLOT_CONFLICT");
    expect(error).toMatchObject({ original: { key: intent.key, result: originalResult } });
  });

  it.skipIf(process.platform === "win32")("refuses a cancelled input's unbound slot but replays and settles its already-issued intent", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const slots = [
      slotSeed("issued-before-cancel", 0, { path: "result.txt", content: "issued" }),
      slotSeed("unstarted-after-cancel", 1, { path: "result.txt", content: "unstarted" }),
    ];
    const intent = makeIntent(owner, { slots, slotId: "issued-before-cancel", payloadText: "issued payload bytes" });
    await bind(store, intent);
    const manifest = manifestFor(owner, slots);
    const acceptedInput = {
      id: intent.acceptedInput.id,
      digest: intent.acceptedInput.digest,
      nativeEntryId: intent.acceptedInput.nativeEntryId,
      manifestDigest: intent.manifestDigest,
      manifest,
      cancellationRef: "durable-cancellation-reference",
    };
    const records = await store.withSession("settle", (tx) => tx.list());
    expect(resolveEffectSlot({
      acceptedInput,
      kind: intent.slot.kind,
      canonicalRef: intent.slot.canonicalRef,
      adapterVersion: intent.slot.adapterVersion,
      canonicalTarget: intent.slot.canonicalTarget,
      canonicalArguments: intent.slot.canonicalArguments,
      records,
    })).toEqual(intent.slot);
    expectContractCode(() => resolveEffectSlot({
      acceptedInput,
      kind: intent.slot.kind,
      canonicalRef: intent.slot.canonicalRef,
      adapterVersion: intent.slot.adapterVersion,
      canonicalTarget: intent.slot.canonicalTarget,
      canonicalArguments: slots[1]!.canonicalArguments!,
      records,
    }), "EFFECT_INPUT_CLOSED");

    const settled = await createEffectReconciler({ owner, store, sinks: [fileSink(owner)] })
      .reconcilePending("recovery-generation-cancelled-input");
    expect(settled.uncertain).toBe(0);
    expect(settledFor(settled, intent.key).some((entry) => entry.outcome === "committed")).toBe(true);
    expect(readFileSync(intent.canonicalTarget, "utf8")).toBe("issued payload bytes");
  });

  it("binds manifest and intent digests and detects payload tampering", () => {
    const owner = makeOwner();
    const intent = makeIntent(owner);
    const reorderedManifest: EffectManifest = {
      version: 1,
      slots: [{
        ...intent.slot,
        canonicalArguments: { path: "result.txt" },
        constraints: {},
      }],
    };
    expect(manifestDigest(reorderedManifest)).toBe(intent.manifestDigest);
    expect(intentBindingDigest(JSON.parse(JSON.stringify(intent)) as EffectIntent)).toBe(intentBindingDigest(intent));
    expect(() => validateIntent(intent)).not.toThrow();

    const changedPayload = {
      ...intent,
      payload: { ...intent.payload, bytes: Buffer.from("tampered").toString("base64") },
    } as EffectIntent;
    expect(() => validateIntent(changedPayload)).toThrow();
  });
});

describe.skipIf(process.platform === "win32")("retained effect store (POSIX durability)", () => {
  it("persists canonical intent/transition JSON, reloads it, and preserves physical aliases", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner, { toolCallId: "call-original", parentToolCallId: "parent-original" });
    const rebound = makeIntent(owner, { toolCallId: "call-replayed-on-successor", parentToolCallId: "parent-original" });
    expect(store.owner).toBe(owner);
    const record = await store.withSession("new-intent", (tx) => {
      tx.bind(intent);
      const intentPath = join(keyDirectory(owner, intent.key), "intent.json");
      expect(existsSync(intentPath)).toBe(true);
      const rawIntent = readFileSync(intentPath, "utf8");
      expect(rawIntent.trim()).toBe(canonicalEncode(JSON.parse(rawIntent)));
      return tx.bind(rebound);
    });
    expect(record.intent.tool.originalToolCallId).toBe("call-original");
    expect(record.intent.key).toBe(intent.key);
    expect(record.transitions.some((entry) => entry.state === "ALIAS" && entry.toolCallId === "call-replayed-on-successor")).toBe(true);

    const files = readdirSync(transitionDirectory(owner, intent.key)).filter((file) => file.endsWith(".json"));
    expect(files.length).toBeGreaterThan(0);
    for (const file of files) {
      const raw = readFileSync(join(transitionDirectory(owner, intent.key), file), "utf8");
      expect(raw.trim()).toBe(canonicalEncode(JSON.parse(raw)));
    }
    const reloaded = new EffectStore(owner);
    const loaded = await reloaded.withSession("settle", (tx) => tx.read(intent.key));
    expect(loaded).toEqual(record);
    expect((await reloaded.withSession("settle", (tx) => tx.list())).map((item) => item.intent.key)).toContain(intent.key);
  });

  it("rejects changed payload for an already-bound key without replacing its immutable intent", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const original = makeIntent(owner, { payloadText: "first immutable payload" });
    const changed = makeIntent(owner, { payloadText: "different payload, same slot and K" });
    expect(changed.key).toBe(original.key);
    await bind(store, original);
    await expectRejectedCode(bind(store, changed), "EFFECT_SLOT_CONFLICT");
    const saved = await store.withSession("settle", (tx) => tx.read(original.key));
    expect(saved.intent.payload).toEqual(original.payload);
  });

  it("treats adapterData as immutable binding data and rejects changed preparation metadata", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const base = makeIntent(owner);
    const original: EffectIntent = { ...base, adapterData: { fileIdentity: { device: 7, inode: "inode-a" }, preparedMode: 0o644 } };
    const changed: EffectIntent = { ...base, adapterData: { fileIdentity: { device: 7, inode: "inode-b" }, preparedMode: 0o644 } };
    expect(changed.key).toBe(original.key);
    expect(intentBindingDigest(changed)).not.toBe(intentBindingDigest(original));
    await bind(store, original);
    await expectRejectedCode(bind(store, changed), "EFFECT_SLOT_CONFLICT");
    const saved = await store.withSession("settle", (tx) => tx.read(original.key));
    expect(saved.intent.adapterData).toEqual(original.adapterData);
  });

  it("preserves staging evidence and distinguishes absent intent from a corrupt acknowledged intent", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    await bind(store, intent);
    const staging = join(transitionDirectory(owner, intent.key), ".staging-preserve-me.tmp");
    writeFileSync(staging, "partial durable-write evidence", { mode: 0o600 });
    await new EffectStore(owner).withSession("settle", (tx) => tx.read(intent.key));
    expect(readFileSync(staging, "utf8")).toBe("partial durable-write evidence");

    const absentKey = "f".repeat(64);
    const absentDir = keyDirectory(owner, absentKey);
    mkdirSync(join(absentDir, "transitions"), { recursive: true, mode: 0o700 });
    writeFileSync(join(absentDir, "transitions", ".partial.tmp"), "unacknowledged staging evidence");
    await expect(new EffectStore(owner).withSession("settle", (tx) => tx.read(absentKey))).rejects.toThrow();
    expect(existsSync(join(absentDir, "transitions", ".partial.tmp"))).toBe(true);

    const intentPath = join(keyDirectory(owner, intent.key), "intent.json");
    const acknowledged = JSON.parse(readFileSync(intentPath, "utf8")) as Record<string, unknown>;
    acknowledged.canonicalTarget = "/tampered/target";
    writeFileSync(intentPath, canonicalEncode(acknowledged), { mode: 0o600 });
    await expect(new EffectStore(owner).withSession("settle", (tx) => tx.read(intent.key))).rejects.toThrow();
  });

  it("ignores unpublished staging, grants no sink authority, then permits a fresh validated bind", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    const directory = keyDirectory(owner, intent.key);
    const transitions = transitionDirectory(owner, intent.key);
    mkdirSync(transitions, { recursive: true, mode: 0o700 });
    const intentStage = join(directory, "pre-intent-stage.tmp");
    const transitionStage = join(transitions, "pre-transition-stage.tmp");
    writeFileSync(intentStage, "unpublished intent bytes", { mode: 0o600 });
    writeFileSync(transitionStage, "unpublished transition bytes", { mode: 0o600 });

    expect(await store.withSession("settle", (tx) => tx.list())).toEqual([]);
    let sinkCalls = 0;
    const reconciled = await createEffectReconciler({
      owner,
      store,
      sinks: [fileSink(owner, () => { sinkCalls += 1; })],
    }).reconcilePending("recovery-generation-unpublished-stage");
    expect(reconciled.uncertain).toBe(0);
    expect(reconciled.settled).toEqual([]);
    expect(sinkCalls).toBe(0);
    expect(existsSync(intent.canonicalTarget)).toBe(false);

    const bound = await bind(store, intent);
    expect(bound.intent.key).toBe(intent.key);
    expect(readFileSync(intentStage, "utf8")).toBe("unpublished intent bytes");
    expect(readFileSync(transitionStage, "utf8")).toBe("unpublished transition bytes");
    expect((await store.withSession("settle", (tx) => tx.read(intent.key))).transitions).toEqual([]);
  });

  it("fails closed when an acknowledged intent or transition tail is deleted but its publication witness remains", async () => {
    const intentOwner = makeOwner();
    const intentStore = new EffectStore(intentOwner);
    const intent = makeIntent(intentOwner);
    await bind(intentStore, intent);
    const intentDir = keyDirectory(intentOwner, intent.key);
    const intentWitness = readdirSync(intentDir).find((name) => name.startsWith(".published-intent.json-"));
    expect(intentWitness).toBeDefined();
    unlinkSync(join(intentDir, "intent.json"));
    await expectRejectedCode(intentStore.withSession("settle", (tx) => tx.list()), "EFFECT_STORE_UNCERTAIN");
    expect(existsSync(join(intentDir, intentWitness!))).toBe(true);

    const tailOwner = makeOwner();
    const tailStore = new EffectStore(tailOwner);
    const tailIntent = makeIntent(tailOwner);
    await bind(tailStore, tailIntent);
    await tailStore.withSession("settle", (tx) => tx.append(tailIntent.key, transition(tailOwner, {
      state: "ATTEMPT",
      toolCallId: tailIntent.tool.originalToolCallId,
    })));
    const tailDir = transitionDirectory(tailOwner, tailIntent.key);
    const transitionFile = join(tailDir, "000000000001.json");
    const tailWitness = readdirSync(tailDir).find((name) => name.startsWith(".published-000000000001.json-"));
    expect(tailWitness).toBeDefined();
    unlinkSync(transitionFile);
    await expectRejectedCode(tailStore.withSession("settle", (tx) => tx.list()), "EFFECT_STORE_UNCERTAIN");
    expect(existsSync(join(tailDir, tailWitness!))).toBe(true);
  });

  it("keeps a durable intent with no transition tail pending and detects chain/result tampering", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    await bind(store, intent);
    expect((await store.withSession("settle", (tx) => tx.read(intent.key))).transitions).toEqual([]);
    await store.withSession("settle", (tx) => {
      tx.append(intent.key, transition(owner, { state: "ATTEMPT", toolCallId: intent.tool.originalToolCallId }));
      tx.append(intent.key, transition(owner, { state: "ALIAS", toolCallId: "middle-transition-alias" }));
      tx.append(intent.key, transition(owner, { state: "ATTEMPT", toolCallId: intent.tool.originalToolCallId }));
    });
    const files = readdirSync(transitionDirectory(owner, intent.key)).filter((file) => file.endsWith(".json")).sort();
    expect(files).toHaveLength(3);
    unlinkSync(join(transitionDirectory(owner, intent.key), files[1]!));
    await expect(new EffectStore(owner).withSession("settle", (tx) => tx.read(intent.key))).rejects.toThrow();

    const owner2 = makeOwner();
    const store2 = new EffectStore(owner2);
    const intent2 = makeIntent(owner2);
    await bind(store2, intent2);
    await persistResult(owner2, store2, intent2, { value: "hash-linked result" });
    const linkedFiles = readdirSync(transitionDirectory(owner2, intent2.key)).filter((file) => file.endsWith(".json")).sort();
    const finalPath = join(transitionDirectory(owner2, intent2.key), linkedFiles[linkedFiles.length - 1]!);
    const finalRecord = JSON.parse(readFileSync(finalPath, "utf8")) as Record<string, unknown>;
    finalRecord.digest = "0".repeat(64);
    writeFileSync(finalPath, canonicalEncode(finalRecord), { mode: 0o600 });
    await expect(new EffectStore(owner2).withSession("settle", (tx) => tx.read(intent2.key))).rejects.toThrow();

    const owner3 = makeOwner();
    const store3 = new EffectStore(owner3);
    const intent3 = makeIntent(owner3);
    await bind(store3, intent3);
    await persistResult(owner3, store3, intent3, { value: "hash-verified result" });
    const resultFiles = readdirSync(transitionDirectory(owner3, intent3.key)).filter((file) => file.endsWith(".json")).sort();
    const resultPath = join(transitionDirectory(owner3, intent3.key), resultFiles[resultFiles.length - 1]!);
    const changedResult = JSON.parse(readFileSync(resultPath, "utf8")) as Record<string, unknown>;
    changedResult.resultBytesHash = "0".repeat(64);
    const body = { ...changedResult };
    delete body.digest;
    changedResult.digest = sha256Bytes(canonicalEncode(body));
    writeFileSync(resultPath, canonicalEncode(changedResult), { mode: 0o600 });
    await expect(new EffectStore(owner3).withSession("settle", (tx) => tx.read(intent3.key))).rejects.toThrow();
  });

  it("latches new-intent after an fsync failure until a complete settled reread", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    let bindFailure: unknown;
    let followupFailure: unknown;
    const fsync = vi.spyOn(fs, "fsyncSync").mockImplementation((_fd: number): void => {
      throw new Error("injected namespace fsync failure");
    });
    try {
      await store.withSession("new-intent", (tx) => {
        try { tx.bind(intent); } catch (error) { bindFailure = error; }
        try { tx.list(); } catch (error) { followupFailure = error; }
      });
    } finally {
      fsync.mockRestore();
    }
    expectContractCode(() => { throw bindFailure; }, "EFFECT_STORE_UNCERTAIN");
    expectContractCode(() => { throw followupFailure; }, "EFFECT_STORE_UNCERTAIN");
    await expectRejectedCode(bind(store, intent), "EFFECT_ADMISSION_CLOSED");

    expect(await store.withSession("settle", (tx) => tx.list())).toEqual([]);
    const rebound = await bind(store, intent);
    expect(rebound.intent.key).toBe(intent.key);
    expect((await store.withSession("settle", (tx) => tx.read(intent.key))).intent).toEqual(intent);
  });

  it("serializes parallel transactions across store instances for one retained session", async () => {
    const owner = makeOwner();
    const firstStore = new EffectStore(owner);
    const secondStore = new EffectStore(owner);
    const intent = makeIntent(owner);
    await bind(firstStore, intent);
    let releaseFirst!: () => void;
    let firstEntered!: () => void;
    let secondEntered!: () => void;
    const gate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const entered = new Promise<void>((resolve) => { firstEntered = resolve; });
    const secondStarted = new Promise<void>((resolve) => { secondEntered = resolve; });

    const first = firstStore.withSession("settle", async (tx) => {
      firstEntered();
      await gate;
      return tx.append(intent.key, transition(owner, { state: "ALIAS", toolCallId: "serialized-first" }));
    });
    await entered;
    let secondCallbackEntered = false;
    const second = secondStore.withSession("settle", (tx) => {
      secondCallbackEntered = true;
      secondEntered();
      return tx.append(intent.key, transition(owner, { state: "ALIAS", toolCallId: "serialized-second" }));
    });
    await Promise.resolve();
    await Promise.resolve();
    expect(secondCallbackEntered).toBe(false);
    releaseFirst();
    await Promise.all([first, second]);
    await secondStarted;

    const record = await new EffectStore(owner).withSession("settle", (tx) => tx.read(intent.key));
    const tail = record.transitions.slice(-2);
    expect(tail.map((entry) => entry.toolCallId)).toEqual(["serialized-first", "serialized-second"]);
    expect(tail[1]!.sequence).toBe(tail[0]!.sequence + 1);
    expect(tail[1]!.previousDigest).toBe(tail[0]!.digest);
  });
});

describe.skipIf(process.platform === "win32")("effect recovery from persisted intents (POSIX durability)", () => {
  it("reads exact durable structured/error result bytes only through the original call or a physical alias", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner, { toolCallId: "native-write-call", parentToolCallId: "fabric-exec-parent" });
    const originalResult: JsonValue = {
      isError: true,
      content: [{ type: "text", text: "write failed" }, { type: "json", data: { code: "EIO", errno: -5 } }],
      error: { name: "NativeToolError", message: "permission denied", details: { path: "result.txt", retryable: false } },
      unicode: "café / 雪",
    };
    const hash = resultHash(originalResult);
    await bind(store, intent);
    await persistResult(owner, store, intent, originalResult, [{
      toolCallId: "captured-physical-alias",
      parentToolCallId: "fabric-exec-parent",
    }]);

    expect(await readEffectResult(owner, store, intent.key, "native-write-call", hash)).toEqual(originalResult);
    expect(await readEffectResult(owner, store, intent.key, "captured-physical-alias", hash)).toEqual(originalResult);
    await expectRejectedCode(readEffectResult(owner, store, intent.key, "captured-physical-alias", "0".repeat(64)), "EFFECT_RESULT_UNAVAILABLE");
    await expectRejectedCode(readEffectResult(owner, store, intent.key, "missing-physical-alias", hash), "EFFECT_CALL_ALIAS_MISMATCH");
    await expectRejectedCode(readEffectResult(owner, store, intent.key, "fabric-exec-parent", hash), "EFFECT_CALL_ALIAS_MISMATCH");

    const pendingIntent = makeIntent(owner, { inputId: "another-accepted-input", toolCallId: "pending-native-call" });
    await bind(store, pendingIntent);
    await expectRejectedCode(
      readEffectResult(owner, store, pendingIntent.key, "pending-native-call", resultHash(pendingIntent.file!.preparedResult)),
      "EFFECT_RESULT_UNAVAILABLE",
    );
  });

  it("materializes only the exact original native error result and refuses invalid descriptors/content-only payloads", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner, { toolCallId: "native-write-call" });
    const nativeError: JsonValue = {
      role: "toolResult",
      toolCallId: "native-write-call",
      toolName: "write",
      content: [{ type: "text", text: "Error: permission denied" }],
      isError: true,
      timestamp: 1_735_000_000_000,
    };
    const hash = resultHash(nativeError);
    await bind(store, intent);
    await persistResult(owner, store, intent, nativeError);
    const effect: SettledEffect = { key: intent.key, toolCallId: "native-write-call", outcome: "committed", resultBytesHash: hash };
    expect(await readToolResult(owner, store, effect)).toEqual(nativeError);
    expect(await createEffectReconciler({ owner, store, sinks: [] }).readToolResult(effect)).toEqual(nativeError);
    await expectRejectedCode(readToolResult(owner, store, { ...effect, resultBytesHash: "0".repeat(64) }), "EFFECT_RESULT_UNAVAILABLE");
    await expectRejectedCode(readToolResult(owner, store, { ...effect, toolCallId: "unrecorded-call" }), "EFFECT_CALL_ALIAS_MISMATCH");
    await expectRejectedCode(readToolResult(owner, store, { ...effect, outcome: "not-committed" }), "EFFECT_RESULT_OUTCOME_MISMATCH");

    const contentOnlyIntent = makeIntent(owner, { inputId: "content-only-input", toolCallId: "content-only-call" });
    await bind(store, contentOnlyIntent);
    const contentOnly: JsonValue = { content: [{ type: "text", text: "plain content is not a native result" }] };
    await persistResult(owner, store, contentOnlyIntent, contentOnly);
    await expectRejectedCode(readToolResult(owner, store, {
      key: contentOnlyIntent.key,
      toolCallId: contentOnlyIntent.tool.originalToolCallId,
      outcome: "committed",
      resultBytesHash: resultHash(contentOnly),
    }), "EFFECT_NATIVE_RESULT_INVALID");

    const nestedIntent = makeIntent(owner, {
      inputId: "nested-result-input",
      toolCallId: "nested-child-call",
      parentToolCallId: "fabric-exec-parent",
    });
    const nestedNative: JsonValue = {
      role: "toolResult",
      toolCallId: "nested-child-call",
      toolName: "write",
      content: [{ type: "text", text: "child succeeded" }],
      isError: false,
      timestamp: 1_735_000_000_001,
    };
    await bind(store, nestedIntent);
    await persistResult(owner, store, nestedIntent, nestedNative, [{
      toolCallId: "nested-child-alias",
      parentToolCallId: "fabric-exec-parent",
    }]);
    const nestedHash = resultHash(nestedNative);
    expect(await readEffectResult(owner, store, nestedIntent.key, "nested-child-alias", nestedHash)).toEqual(nestedNative);
    await expectRejectedCode(readToolResult(owner, store, {
      key: nestedIntent.key,
      toolCallId: "nested-child-alias",
      outcome: "committed",
      resultBytesHash: nestedHash,
    }), "EFFECT_NESTED_RESULT_REQUIRES_PARENT_REPAIR");
    await expectRejectedCode(readToolResult(owner, store, {
      key: nestedIntent.key,
      toolCallId: "fabric-exec-parent",
      outcome: "committed",
      resultBytesHash: nestedHash,
    }), "EFFECT_CALL_ALIAS_MISMATCH");
  });

  it("returns a durable result without calling the sink even after the destination later changes", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner, { payloadText: "original committed bytes" });
    await bind(store, intent);
    await persistResult(owner, store, intent, { truthful: "historical result" });
    writeFileSync(intent.canonicalTarget, "a later legitimate edit");
    let sinkCalls = 0;
    const reconciler = createEffectReconciler({ owner, store, sinks: [fileSink(owner, () => { sinkCalls += 1; })] });

    const result = await reconciler.reconcilePending("recovery-generation-7");
    expect(sinkCalls).toBe(0);
    expect(readFileSync(intent.canonicalTarget, "utf8")).toBe("a later legitimate edit");
    expect(settledFor(result, intent.key).some((entry) => entry.outcome === "committed")).toBe(true);
    expect(result.repairs.some((repair) => repair.key === intent.key && canonicalEncode(repair.result) === canonicalEncode({ truthful: "historical result" }))).toBe(true);
  });

  it("settles a pending pre-mutation intent from stored bytes and recognizes a post-commit/pre-result crash", async () => {
    const beforeOwner = makeOwner();
    const beforeStore = new EffectStore(beforeOwner);
    const beforeIntent = makeIntent(beforeOwner, { payloadText: "persisted intent bytes" });
    await bind(beforeStore, beforeIntent);
    const regeneratedBytes = "different bytes a restarted model might suggest";
    let observedStoredBytes = "";
    const beforeSink: EffectSink = {
      kind: beforeIntent.slot.kind,
      adapterVersion: beforeIntent.slot.adapterVersion,
      async reconcile(intent) {
        observedStoredBytes = Buffer.from(intent.payload.bytes, "base64").toString("utf8");
        return fileSink(beforeOwner).reconcile(intent, { owner: beforeOwner, recoveryGeneration: "recovery-generation-8" });
      },
    };
    const before = await createEffectReconciler({ owner: beforeOwner, store: beforeStore, sinks: [beforeSink] }).reconcilePending("recovery-generation-8");
    expect(regeneratedBytes).not.toBe(observedStoredBytes);
    expect(observedStoredBytes).toBe("persisted intent bytes");
    expect(readFileSync(beforeIntent.canonicalTarget, "utf8")).toBe("persisted intent bytes");
    expect(settledFor(before, beforeIntent.key).some((entry) => entry.outcome === "committed")).toBe(true);

    const afterOwner = makeOwner();
    const afterStore = new EffectStore(afterOwner);
    const afterIntent = makeIntent(afterOwner, { payloadText: "already committed original bytes" });
    await bind(afterStore, afterIntent);
    writeFileSync(afterIntent.canonicalTarget, Buffer.from(afterIntent.payload.bytes, "base64"));
    let sinkCalls = 0;
    let substantiveWrites = 0;
    const after = await createEffectReconciler({
      owner: afterOwner,
      store: afterStore,
      sinks: [fileSink(afterOwner, () => { sinkCalls += 1; }, () => { substantiveWrites += 1; })],
    }).reconcilePending("recovery-generation-9");
    expect(sinkCalls).toBe(1);
    expect(substantiveWrites).toBe(0); // matching post-state is verified, not rewritten
    expect(readFileSync(afterIntent.canonicalTarget, "utf8")).toBe("already committed original bytes");
    expect(settledFor(after, afterIntent.key).some((entry) => entry.outcome === "committed")).toBe(true);
  });

  it("keeps unknown adapters and sink errors uncertain rather than claiming completion", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    await bind(store, intent);

    const unknown = await createEffectReconciler({ owner, store, sinks: [] }).reconcilePending("recovery-generation-10");
    expect(unknown.uncertain).toBe(1);
    expect(unknown.unresolved.some((entry) => entry.key === intent.key)).toBe(true);
    expect(settledFor(unknown, intent.key).some((entry) => entry.outcome === "uncertain")).toBe(true);

    const errorOwner = makeOwner();
    const errorStore = new EffectStore(errorOwner);
    const errorIntent = makeIntent(errorOwner);
    await bind(errorStore, errorIntent);
    const failingSink: EffectSink = {
      kind: errorIntent.slot.kind,
      adapterVersion: errorIntent.slot.adapterVersion,
      async reconcile() { throw new Error("destination state unavailable"); },
    };
    const errored = await createEffectReconciler({ owner: errorOwner, store: errorStore, sinks: [failingSink] }).reconcilePending("recovery-generation-11");
    expect(errored.uncertain).toBe(1);
    expect(errored.unresolved.some((entry) => entry.key === errorIntent.key)).toBe(true);
    expect(settledFor(errored, errorIntent.key).some((entry) => entry.outcome === "uncertain")).toBe(true);
  });

  it("allows an uncertain same-key intent to reconcile successfully on a later recovery", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    await bind(store, intent);
    let calls = 0;
    const retryingSink: EffectSink = {
      kind: intent.slot.kind,
      adapterVersion: intent.slot.adapterVersion,
      async reconcile() {
        calls += 1;
        if (calls === 1) return { outcome: "uncertain", reason: "first destination observation was indeterminate" };
        const result: JsonValue = { outcome: "committed-on-retry" };
        return { outcome: "committed", result, resultBytesHash: resultHash(result) };
      },
    };
    const reconciler = createEffectReconciler({ owner, store, sinks: [retryingSink] });
    const first = await reconciler.reconcilePending("recovery-generation-12");
    expect(first.uncertain).toBe(1);
    expect(settledFor(first, intent.key).some((entry) => entry.outcome === "uncertain")).toBe(true);
    const second = await reconciler.reconcilePending("recovery-generation-13");
    expect(calls).toBe(2);
    expect(second.uncertain).toBe(0);
    expect(settledFor(second, intent.key).some((entry) => entry.outcome === "committed")).toBe(true);
  });

  it("projects durable physical aliases without changing K or inventing parent returns", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const slots = [
      slotSeed("nested-a", 0, { path: "result.txt", child: "a" }),
      slotSeed("nested-b", 1, { path: "result.txt", child: "b" }),
    ];
    const first = makeIntent(owner, { slots, slotId: "nested-a", toolCallId: "nested-call-a", parentToolCallId: "fabric-exec-parent" });
    const second = makeIntent(owner, { slots, slotId: "nested-b", toolCallId: "nested-call-b", parentToolCallId: "fabric-exec-parent" });
    await bind(store, first);
    await bind(store, second);
    await persistResult(owner, store, first, { child: "a-result" }, [{ toolCallId: "nested-call-a-recovered", parentToolCallId: "fabric-exec-parent" }]);
    await persistResult(owner, store, second, { child: "b-result" }, [{ toolCallId: "nested-call-b-recovered", parentToolCallId: "fabric-exec-parent" }]);

    let sinkCalls = 0;
    const result = await createEffectReconciler({
      owner,
      store,
      sinks: [fileSink(owner, () => { sinkCalls += 1; })],
    }).reconcilePending("recovery-generation-14");
    expect(sinkCalls).toBe(0);
    expect(result.uncertain).toBe(0);
    for (const [intent, alias] of [[first, "nested-call-a-recovered"], [second, "nested-call-b-recovered"]] as const) {
      expect(intent.key).toBe(mintEffectKey(intent.keyTuple));
      expect(result.settled.some((entry) => entry.key === intent.key && entry.toolCallId === alias && entry.outcome === "committed")).toBe(true);
      expect(result.repairs.some((repair) => repair.key === intent.key && repair.toolCallId === intent.tool.originalToolCallId && repair.parentToolCallId === "fabric-exec-parent")).toBe(true);
    }
    expect(result.repairs.some((repair) => repair.toolCallId === "fabric-exec-parent")).toBe(false);
    expect(result.repairs.filter((repair) => repair.parentToolCallId === "fabric-exec-parent")
      .map(repair => repair.toolCallId).sort()).toEqual([
      "nested-call-a", "nested-call-a-recovered", "nested-call-b", "nested-call-b-recovered",
    ]);
  });

  it("requires explicit bind/unbind for the named settle API and never supplies an ambient fallback", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    await bind(store, intent);
    const generation = "recovery-generation-α-16";
    const sink: EffectSink = {
      kind: intent.slot.kind,
      adapterVersion: intent.slot.adapterVersion,
      async reconcile(_intent, context) {
        expect(context.recoveryGeneration).toBe(generation);
        const result: JsonValue = { recovered: "original" };
        return { outcome: "committed", result, resultBytesHash: resultHash(result) };
      },
    };

    await expect(settlePendingIntents(owner.logicalSessionId, generation)).rejects.toThrow();
    const unbind = bindEffectReconciler({ owner, store, sinks: [sink] });
    try {
      await expect(settlePendingIntents("wrong-session", generation)).rejects.toThrow();
      expect(() => bindEffectReconciler({ owner, store, sinks: [sink] })).toThrow();
      const result = await settlePendingIntents(owner.logicalSessionId, generation);
      expect(result.uncertain).toBe(0);
      expect(result.settled).toEqual(expect.arrayContaining([
        expect.objectContaining({ key: intent.key, outcome: "committed", toolCallId: intent.tool.originalToolCallId }),
      ]));
    } finally {
      unbind();
    }
    await expect(settlePendingIntents(owner.logicalSessionId, generation)).rejects.toThrow();
  });

  it("joins concurrent duplicate reconciliation into one sink result and rejects wrong session/generation", async () => {
    const owner = makeOwner();
    const store = new EffectStore(owner);
    const intent = makeIntent(owner);
    await bind(store, intent);
    let releaseSink!: () => void;
    let sinkEntered!: () => void;
    const gate = new Promise<void>((resolve) => { releaseSink = resolve; });
    const entered = new Promise<void>((resolve) => { sinkEntered = resolve; });
    let calls = 0;
    const sink: EffectSink = {
      kind: intent.slot.kind,
      adapterVersion: intent.slot.adapterVersion,
      async reconcile() {
        calls += 1;
        sinkEntered();
        await gate;
        const result: JsonValue = { committed: true };
        return { outcome: "committed", result, resultBytesHash: resultHash(result) };
      },
    };
    const reconciler = createEffectReconciler({ owner, store, sinks: [sink] });
    const first = reconciler.reconcilePending("recovery-generation-15");
    await entered;
    const duplicate = reconciler.reconcilePending("recovery-generation-15");
    await Promise.resolve();
    await Promise.resolve();
    expect(calls).toBe(1);
    releaseSink();
    const [firstResult, duplicateResult] = await Promise.all([first, duplicate]);
    expect(calls).toBe(1);
    expect(firstResult).toEqual(duplicateResult);
    const durable = await store.withSession("settle", (tx) => tx.read(intent.key));
    expect(durable.transitions.filter((entry) => entry.state === "RESULT")).toHaveLength(1);

    await expect(reconciler.settlePendingIntents("another-session", "recovery-generation-15")).rejects.toThrow();
    await expect(reconcilePending(owner, store, [], "")).rejects.toThrow();
    await expect(reconcilePending(owner, store, [], "\ud800")).rejects.toThrow();
  });
});

describe.skipIf(process.platform !== "win32")("unsupported Windows effect-store profile", () => {
  it("refuses namespace transactions instead of claiming POSIX directory durability", async () => {
    const owner = makeOwner();
    let completed = false;
    let refusal: unknown;
    try {
      const store = new EffectStore(owner);
      await store.withSession("settle", (tx) => tx.list());
      completed = true;
    } catch (error) {
      refusal = error;
    }
    expect(completed).toBe(false);
    expect(refusal).toBeInstanceOf(EffectContractError);
    expect(["EFFECT_INVALID_OWNER", "EFFECT_STORE_UNCERTAIN"]).toContain((refusal as EffectContractError).code);
  });
});
