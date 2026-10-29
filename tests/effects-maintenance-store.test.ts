import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { EffectIntent, EffectRecord, RecoveryOwner } from "../src/effects/contracts.js";
import type { RepairReceiptView } from "../src/effects/repair-contracts.js";
import type { MaintenanceRepairRecordBody, StoredMaintenanceRepair } from "../src/effects/repair-records.js";
import { canonicalEncode, manifestDigest, mintEffectKey } from "../src/effects/slots.js";
import { EffectStore } from "../src/effects/store.js";
import type { EffectStoreTransaction } from "../src/effects/store.js";

// Keep reason: retained-volume data safety and otherwise expensive crash cuts.
// These are PURE STORAGE DATA FIXTURES. No B5 admission, native-Pi authentication,
// B6 durability, producer qualification or end-to-end maintenance is simulated.
const roots: string[] = [];
const ORIGINAL = "original-input/雪";
const ORIGINAL_HASH = digest("original admitted text");
const at = "2026-10-30T12:00:00.000Z";

function digest(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function owner(): RecoveryOwner {
  const root = fs.mkdtempSync(path.join(tmpdir(), "fabric-maintenance-store-"));
  roots.push(root);
  const sessionRoot = path.join(root, "session");
  const workspaceRoot = path.join(root, "workspace");
  fs.mkdirSync(sessionRoot, { mode: 0o700 });
  fs.mkdirSync(workspaceRoot, { mode: 0o700 });
  const sessionFile = path.join(root, "native.jsonl");
  fs.writeFileSync(sessionFile, "storage fixture only\n", { mode: 0o600 });
  return {
    orgId: "org-storage", logicalSessionId: "session-storage", sessionRoot: fs.realpathSync(sessionRoot),
    workspaceRoot: fs.realpathSync(workspaceRoot), sessionFile, pvcUID: "retained-volume-storage",
    exclusionReceipt: "storage-test-owner", signal: new AbortController().signal,
    attempt: { podUID: "pod", bootId: "boot", ownerIncarnation: "owner", authorityRevision: "revision" },
    assertLive() {},
  };
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) {
    // Only this file's own mkdtempSync allocations; never session/worktree/home roots.
    fs.rmSync(root, { recursive: true, force: true });
  }
});

function repairKey(o: RecoveryOwner, original = ORIGINAL): string {
  return digest(canonicalEncode([1, o.orgId, o.logicalSessionId, original]));
}

function namespace(o: RecoveryOwner): string {
  return path.join(o.sessionRoot, "effects", "repairs-v1");
}

function directory(o: RecoveryOwner, original = ORIGINAL): string {
  return path.join(namespace(o), repairKey(o, original));
}

function receiptFile(o: RecoveryOwner, original = ORIGINAL): string {
  return path.join(directory(o, original), "receipt.json");
}

function names(o: RecoveryOwner, original = ORIGINAL): string[] {
  return fs.readdirSync(directory(o, original)).sort();
}

function rehash(body: MaintenanceRepairRecordBody, changes: Partial<MaintenanceRepairRecordBody> = {}): MaintenanceRepairRecordBody {
  const changed = { ...body, ...changes };
  return { ...changed, repairReceiptSha256: digest(canonicalEncode(changed.view)) };
}

function withView(body: MaintenanceRepairRecordBody, changes: Partial<RepairReceiptView>): MaintenanceRepairRecordBody {
  return rehash(body, { view: { ...body.view, ...changes } });
}

function envelope(body: MaintenanceRepairRecordBody): StoredMaintenanceRepair {
  return { ...body, recordSha256: digest(canonicalEncode(body)) };
}

function candidate(o: RecoveryOwner, records: readonly EffectRecord[] = [], original = ORIGINAL): MaintenanceRepairRecordBody {
  const nativeEntries = [
    { entryId: "carrier-D", deliveryId: "delivery-D", recordKind: "carrier" as const, file: o.sessionFile,
      offset: 0, length: 41, bytesSha256: digest("storage carrier bytes\n"), b6Sequence: 11 },
    { entryId: "joined-J", deliveryId: "delivery-J", recordKind: "carrier" as const, file: o.sessionFile,
      offset: 42, length: 77, bytesSha256: digest("storage joined bytes\n"), b6Sequence: 13 },
  ];
  const view: RepairReceiptView = {
    version: 1, sessionId: o.logicalSessionId, originalInputId: original, recoveryGeneration: "opaque-g-α",
    executionBranchEntryId: "parent-A-not-a-protected-carrier", joinedLeafEntryId: "joined-J",
    joinedHistorySha256: digest("storage-only history digest"),
    protectedRangeSha256: digest(canonicalEncode(["fabric.b4.protected-range", 1,
      nativeEntries.map(entry => [entry.entryId, entry.bytesSha256])])),
    protectedEntryIds: nativeEntries.map(entry => entry.entryId),
    settlements: records.map(record => ({ key: record.intent.key, toolCallId: record.intent.tool.originalToolCallId,
      outcome: "committed" as const, resultBytesHash: digest("storage-only native-message hash") })),
    nativeEntries,
  };
  return {
    format: "fabric.maintenance-repair-receipt", version: 1, orgId: o.orgId, logicalSessionId: o.logicalSessionId,
    originalAdmissionId: "original-admission", originalPayloadSha256: ORIGINAL_HASH,
    originalDeadlineMs: 1_800_000_000_000, originalCarrierEntryId: "carrier-D",
    frontier: records.map(record => ({ key: record.intent.key, sequence: record.transitions.length,
      digest: record.transitions.at(-1)?.digest ?? record.intentDigest })).sort((a, b) => a.key < b.key ? -1 : 1),
    view, repairReceiptSha256: digest(canonicalEncode(view)),
  };
}

function intent(o: RecoveryOwner, original = ORIGINAL, slotId = "slot-0"): EffectIntent {
  const slot = {
    id: slotId, ordinal: 0, kind: "storage.fixture", canonicalRef: "storage.fixture", adapterVersion: "v1",
    canonicalTarget: path.join(o.workspaceRoot, slotId), predecessors: [], canonicalArguments: { slotId }, constraints: {},
  };
  const tuple = [1, o.orgId, o.logicalSessionId, original, slotId, slot.kind] as const;
  return {
    format: "fabric.effect-intent", version: 1, key: mintEffectKey(tuple), keyTuple: tuple,
    manifestDigest: manifestDigest({ version: 1, slots: [slot] }),
    acceptedInput: { id: original, digest: ORIGINAL_HASH, nativeEntryId: "retained-input-entry" }, slot,
    tool: { canonicalRef: slot.canonicalRef, adapterVersion: slot.adapterVersion,
      originalToolCallId: `call-${slotId}`, nativeAssistantEntryId: "retained-assistant-entry" },
    canonicalArguments: slot.canonicalArguments, canonicalTarget: slot.canonicalTarget,
    payload: { encoding: "base64", bytes: Buffer.from("storage payload").toString("base64"), digest: digest("storage payload") },
    originAttempt: o.attempt,
  };
}

async function issued(o: RecoveryOwner, store: EffectStore, original = ORIGINAL, slotId = "slot-0", terminal = true): Promise<EffectRecord> {
  const input = intent(o, original, slotId);
  await store.withSession("new-intent", tx => tx.bind(input));
  return store.withSession("settle", tx => terminal ? tx.append(input.key, {
    state: "RESULT", toolCallId: input.tool.originalToolCallId, attempt: o.attempt, at,
    outcome: "committed", result: { retained: "original adapter result" },
  }) : tx.read(input.key));
}

async function publish(store: EffectStore, body: MaintenanceRepairRecordBody): Promise<StoredMaintenanceRepair> {
  return store.withSession("settle", tx => tx.publishMaintenanceRepair(body));
}

async function read(store: EffectStore, original = ORIGINAL): Promise<StoredMaintenanceRepair | undefined> {
  return store.withSession("settle", tx => tx.readMaintenanceRepair(original));
}

function expectCode(action: () => unknown, code: string): void {
  let thrown: unknown;
  try { action(); } catch (error) { thrown = error; }
  expect(thrown).toMatchObject({ code });
}

function diskRewrite(o: RecoveryOwner, value: unknown): void {
  // An explicit corruption fixture within our temporary allocation. writeFileSync
  // preserves the inode/hard-link witness, so schema/hash checks must detect it.
  fs.writeFileSync(receiptFile(o), canonicalEncode(value), { mode: 0o600 });
}

describe.skipIf(process.platform === "win32")("maintenance receipt storage (POSIX retained namespace)", () => {
  it("reads a missing namespace without creating it, including beneath an existing effects directory", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const before = fs.readdirSync(o.sessionRoot);
    expect(await read(store)).toBeUndefined();
    expect(await store.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([]);
    expect(await store.withSession("settle", tx => tx.list())).toEqual([]);
    expect(fs.readdirSync(o.sessionRoot)).toEqual(before);
    fs.mkdirSync(path.join(o.sessionRoot, "effects"), { mode: 0o700 });
    expect(await read(store)).toBeUndefined();
    expect(fs.readdirSync(path.join(o.sessionRoot, "effects"))).toEqual([]);
    expect(fs.existsSync(namespace(o))).toBe(false);
  });

  it("publishes a zero-effect immutable canonical receipt, reopens it, and separates both hash domains", async () => {
    const o = owner();
    const body = candidate(o);
    const stored = await publish(new EffectStore(o), body);
    expect(stored).toEqual(envelope(body));
    expect(stored.repairReceiptSha256).toBe(digest(canonicalEncode(body.view)));
    expect(stored.recordSha256).toBe(digest(canonicalEncode(body)));
    expect(stored.recordSha256).not.toBe(stored.repairReceiptSha256);
    expect(stored.repairReceiptSha256).not.toBe(digest(canonicalEncode(body.view) + "\n"));
    expect(stored.recordSha256).not.toBe(digest(canonicalEncode(body) + "\n"));
    expect(fs.readFileSync(receiptFile(o), "utf8")).toBe(canonicalEncode(stored));
    const retainedNames = names(o);
    expect(retainedNames).toHaveLength(3);
    const witness = retainedNames.find(name => name.startsWith(".published-receipt.json-"))!;
    const stage = retainedNames.find(name => name.startsWith("receipt.json."))!;
    expect(witness).toBeTruthy();
    expect(stage).toBeTruthy();
    for (const name of retainedNames) {
      expect(fs.statSync(path.join(directory(o), name)).ino).toBe(fs.statSync(receiptFile(o)).ino);
      expect(fs.statSync(path.join(directory(o), name)).mode & 0o7777).toBe(0o600);
    }
    expect(fs.statSync(namespace(o)).mode & 0o7777).toBe(0o700);
    expect(fs.statSync(directory(o)).mode & 0o7777).toBe(0o700);
    expect(stored.view.executionBranchEntryId).not.toBe(stored.view.joinedLeafEntryId);
    expect(Object.isFrozen(stored)).toBe(true);
    expect(Object.isFrozen(stored.view.nativeEntries[0])).toBe(true);
    expect(Object.isFrozen(stored.frontier)).toBe(true);
    expect(() => Object.assign(stored.view.nativeEntries[0]!, { offset: 99 })).toThrow();
    expect(await read(new EffectStore(o))).toEqual(stored);
    expect(await new EffectStore(o).withSession("settle", tx => tx.list())).toEqual([]);
    expect(await new EffectStore(o).withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([stored]);
  });

  it("preserves an original carrier's zero B6 sequence through publish, read, reopen and exact replay", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const record = await issued(o, store);
    const base = candidate(o, [record]);
    const body = withView(base, { nativeEntries: base.view.nativeEntries.map((entry, index) =>
      index === 0 ? { ...entry, b6Sequence: 0 } : entry) });
    const expected = envelope(body);
    expect(body.view.nativeEntries[0]?.entryId).toBe(body.originalCarrierEntryId);
    expect(body.view.protectedRangeSha256).toBe(base.view.protectedRangeSha256);
    const stored = await publish(store, body);
    expect(stored).toEqual(expected);
    expect(stored.view.nativeEntries.map(entry => [entry.entryId, entry.deliveryId, entry.b6Sequence])).toEqual([
      ["carrier-D", "delivery-D", 0], ["joined-J", "delivery-J", 13],
    ]);
    expect(stored.repairReceiptSha256).toBe(digest(canonicalEncode(body.view)));
    expect(stored.recordSha256).toBe(digest(canonicalEncode(body)));
    const oneBased = withView(body, { nativeEntries: body.view.nativeEntries.map((entry, index) =>
      index === 0 ? { ...entry, b6Sequence: 1 } : entry) });
    expect(stored.repairReceiptSha256).not.toBe(oneBased.repairReceiptSha256);
    expect(stored.recordSha256).not.toBe(envelope(oneBased).recordSha256);
    const bytes = canonicalEncode(expected);
    const before = names(o);
    const identity = fs.statSync(receiptFile(o));
    expect(fs.readFileSync(receiptFile(o), "utf8")).toBe(bytes);
    expect(await read(store)).toEqual(expected);
    const reopened = new EffectStore(o);
    expect(await read(reopened)).toEqual(expected);
    expect(await reopened.withSession("settle", tx => tx.listMaintenanceRepairs())).toEqual([expected]);
    expect(await publish(reopened, body)).toEqual(expected);
    expect(fs.readFileSync(receiptFile(o), "utf8")).toBe(bytes);
    expect(fs.statSync(receiptFile(o)).ino).toBe(identity.ino);
    expect(names(o)).toEqual(before);
  });

  it("rereads and namespace-barriers an exact replay without another stage, inode or byte change", async () => {
    const o = owner();
    const body = candidate(o);
    const first = await publish(new EffectStore(o), body);
    const before = names(o);
    const identity = fs.statSync(receiptFile(o));
    const fsync = vi.spyOn(fs, "fsyncSync");
    const second = await publish(new EffectStore(o), JSON.parse(canonicalEncode(body)) as MaintenanceRepairRecordBody);
    expect(second).toEqual(first);
    expect(second).not.toBe(first);
    expect(fsync.mock.calls.length).toBeGreaterThan(0);
    expect(names(o)).toEqual(before);
    expect(fs.statSync(receiptFile(o)).ino).toBe(identity.ino);
    expect(fs.readFileSync(receiptFile(o), "utf8")).toBe(canonicalEncode(first));
  });

  it("conflicts on changed metadata, view/ref or frontier without replacing bytes or latching admission", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const record = await issued(o, store);
    const body = candidate(o, [record]);
    const first = await publish(store, body);
    const before = names(o);
    const aliased = await store.withSession("settle", tx => tx.append(record.intent.key, {
      state: "ALIAS", toolCallId: "later-physical-alias", attempt: o.attempt, at,
    }));
    const changes = [
      { ...body, originalAdmissionId: "another-admission" },
      { ...body, originalDeadlineMs: body.originalDeadlineMs + 1 },
      withView(body, { recoveryGeneration: "different-g" }),
      withView(body, { executionBranchEntryId: "another-A" }),
      withView(body, { joinedHistorySha256: digest("another history") }),
      { ...body, frontier: candidate(o, [aliased]).frontier },
    ];
    for (const changed of changes) await expect(publish(store, changed)).rejects.toMatchObject({ code: "EFFECT_REPAIR_CONFLICT" });
    expect(await read(store)).toEqual(first);
    expect(names(o)).toEqual(before);
    expect(fs.readFileSync(receiptFile(o), "utf8")).toBe(canonicalEncode(first));
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o, "next-original")))).resolves.toBeDefined();
  });

  it("retains authenticated frontier prefixes after new aliases/keys and unrelated unresolved work", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const record = await issued(o, store);
    const stored = await publish(store, candidate(o, [record]));
    await store.withSession("settle", tx => tx.append(record.intent.key, {
      state: "ALIAS", toolCallId: "after-receipt", attempt: o.attempt, at,
    }));
    await issued(o, store, ORIGINAL, "later-key");
    await issued(o, store, "different-original", "uncertain-peer", false);
    expect(await read(new EffectStore(o))).toEqual(stored);
    expect(await store.withSession("settle", tx => tx.list())).toHaveLength(3);
    expect(stored.frontier[0]?.sequence).toBe(1);
  });

  it("rejects a wrong original tuple, prefix sequence/digest or original payload without inventing a receipt", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const record = await issued(o, store);
    const body = candidate(o, [record]);
    const entry = body.frontier[0]!;
    for (const changed of [
      withView(body, { originalInputId: "foreign-original" }),
      { ...body, originalPayloadSha256: digest("changed original text") },
      { ...body, frontier: [{ ...entry, sequence: entry.sequence + 1 }] },
      { ...body, frontier: [{ ...entry, digest: digest("wrong prefix") }] },
    ]) await expect(publish(store, changed)).rejects.toMatchObject({ code: "EFFECT_REPAIR_CONFLICT" });
    expect(fs.existsSync(namespace(o))).toBe(false);
    await expect(publish(store, body)).resolves.toEqual(envelope(body));
  });

  it("hashes opaque originals into paths rather than interpreting separators or traversal", async () => {
    const o = owner();
    const original = "../../not-a-directory/雪";
    const body = candidate(o, [], original);
    const store = new EffectStore(o);
    await publish(store, body);
    expect(fs.readdirSync(namespace(o))).toEqual([repairKey(o, original)]);
    expect(await read(new EffectStore(o), original)).toEqual(envelope(body));
    expect(fs.existsSync(path.join(o.sessionRoot, "not-a-directory"))).toBe(false);
  });

  const invalidBodies: readonly [string, (body: MaintenanceRepairRecordBody) => unknown][] = [
    ["extra body field", b => ({ ...b, status: "prepared" })],
    ["storage self-hash in body", b => ({ ...b, recordSha256: digest("self") })],
    ["missing body field", b => { const { originalAdmissionId: _omitted, ...rest } = b; return rest; }],
    ["wrong org", b => ({ ...b, orgId: "foreign-org" })],
    ["wrong owner session", b => ({ ...b, logicalSessionId: "foreign-session" })],
    ["wrong view session", b => withView(b, { sessionId: "foreign-session" })],
    ["wrong format", b => ({ ...b, format: "fabric.prepare" })],
    ["wrong version", b => ({ ...b, version: 2 })],
    ["empty admission", b => ({ ...b, originalAdmissionId: "" })],
    ["bad original hash", b => ({ ...b, originalPayloadSha256: "A".repeat(64) })],
    ["fractional deadline", b => ({ ...b, originalDeadlineMs: 0.5 })],
    ["view field", b => rehash(b, { view: { ...b.view, repairReceiptSha256: "forbidden" } as RepairReceiptView })],
    ["empty generation", b => withView(b, { recoveryGeneration: "" })],
    ["bad view hash domain", b => ({ ...b, repairReceiptSha256: digest(canonicalEncode(b.view) + "\n") })],
    ["frontier field", b => ({ ...b, frontier: [{ key: digest("k"), sequence: 0, digest: digest("d"), state: "RESULT" }] })],
    ["bad frontier key", b => ({ ...b, frontier: [{ key: "../unsafe", sequence: 0, digest: digest("d") }] })],
    ["negative frontier sequence", b => ({ ...b, frontier: [{ key: digest("k"), sequence: -1, digest: digest("d") }] })],
    ["sequence exceeds file domain", b => ({ ...b, frontier: [{ key: digest("k"), sequence: 1_000_000_000_000, digest: digest("d") }] })],
    ["bad frontier digest", b => ({ ...b, frontier: [{ key: digest("k"), sequence: 0, digest: "bad" }] })],
    ["native field", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, flush: true })) })],
    ["relative native path", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, file: "relative.jsonl" })) })],
    ["different native path", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, file: e.file + ".different" })) })],
    ["unnormalized native path", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, file: path.dirname(e.file) + "/./native.jsonl" })) })],
    ["negative offset", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, offset: -1 })) })],
    ["overlapping entries", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, offset: 0 })) })],
    ["empty length", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, length: 0 })) })],
    ["unsafe range", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, offset: Number.MAX_SAFE_INTEGER })) })],
    ["negative sequence", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, b6Sequence: -1 })) })],
    ["fractional sequence", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, b6Sequence: 1.5 })) })],
    ["unsafe sequence", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, b6Sequence: Number.MAX_SAFE_INTEGER + 1 })) })],
    ["duplicate durable sequence", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, b6Sequence: 7 })) })],
    ["duplicate zero durable sequence", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, b6Sequence: 0 })) })],
    ["wrong kind", b => withView(b, { nativeEntries: b.view.nativeEntries.map(e => ({ ...e, recordKind: "receipt" as "carrier" })) })],
    ["misaligned ids", b => withView(b, { protectedEntryIds: [...b.view.protectedEntryIds].reverse() })],
    ["missing protected entries", b => withView(b, { protectedEntryIds: [], nativeEntries: [] })],
    ["wrong protected hash", b => withView(b, { protectedRangeSha256: digest("wrong encoding") })],
    ["missing original carrier", b => ({ ...b, originalCarrierEntryId: "absent-D" })],
    ["missing joined carrier", b => withView(b, { joinedLeafEntryId: "absent-J" })],
    ["settlement outside frontier", b => withView(b, { settlements: [{ key: digest("k"), toolCallId: "call", outcome: "committed", resultBytesHash: digest("r") }] })],
  ];

  it.each(invalidBodies)("rejects a closed-schema/identity/hash violation: %s", async (_name, change) => {
    const o = owner();
    const store = new EffectStore(o);
    await expect(publish(store, change(candidate(o)) as MaintenanceRepairRecordBody)).rejects.toMatchObject({ code: "EFFECT_REPAIR_INVALID" });
    expect(fs.existsSync(path.join(o.sessionRoot, "effects"))).toBe(false);
    await expect(publish(store, candidate(o))).resolves.toBeDefined();
  });

  it("rejects lossy JSON/accessors before evaluation or namespace mutation", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const getter = vi.fn(() => "should not execute");
    const withGetter = { ...candidate(o) };
    Object.defineProperty(withGetter, "originalAdmissionId", { enumerable: true, get: getter });
    for (const input of [withGetter, { ...candidate(o), extra: undefined }, { ...candidate(o), originalAdmissionId: "\ud800" },
      { ...candidate(o), frontier: new Array(1) }]) {
      await expect(publish(store, input as MaintenanceRepairRecordBody)).rejects.toMatchObject({ code: "EFFECT_CANONICAL_INVALID" });
    }
    expect(getter).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(o.sessionRoot, "effects"))).toBe(false);
  });

  it("checks settlement closed fields, definitive outcomes, per-key consistency and sorted unique frontier", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const record = await issued(o, store);
    const body = candidate(o, [record]);
    const settlement = body.view.settlements[0]!;
    const row = body.frontier[0]!;
    for (const invalid of [
      // Deliberately malformed unknown input: retain the extra runtime field for closed-schema rejection.
      withView(body, { settlements: [{ ...settlement, source: "native-entry" }] as unknown as RepairReceiptView["settlements"] }),
      withView(body, { settlements: [{ ...settlement, outcome: "uncertain" }] }),
      withView(body, { settlements: [{ key: settlement.key, toolCallId: settlement.toolCallId, outcome: "committed" }] }),
      withView(body, { settlements: [{ ...settlement, resultBytesHash: "bad" }] }),
      withView(body, { settlements: [settlement, settlement] }),
      withView(body, { settlements: [settlement, { ...settlement, toolCallId: "alias", outcome: "not-committed" }] }),
      withView(body, { settlements: [] }),
      { ...body, frontier: [row, row] },
      { ...body, frontier: [{ ...row, key: "f".repeat(64) }, { ...row, key: "0".repeat(64) }] },
    ]) await expect(publish(store, invalid)).rejects.toMatchObject({ code: "EFFECT_REPAIR_INVALID" });
    expect(fs.existsSync(namespace(o))).toBe(false);
    await expect(publish(store, body)).resolves.toBeDefined();
  });

  const diskCorruptions: readonly [string, (stored: StoredMaintenanceRepair) => unknown][] = [
    ["extra envelope field", s => ({ ...s, status: "final" })],
    ["wrong record hash domain", s => ({ ...s, recordSha256: s.repairReceiptSha256 })],
    ["wrong view hash domain", s => { const { recordSha256: _hash, ...b } = s; return envelope({ ...b, repairReceiptSha256: s.recordSha256 }); }],
    ["changed metadata without envelope rehash", s => ({ ...s, originalAdmissionId: "corrupted" })],
    ["closed view with recomputed hashes", s => {
      const { recordSha256: _hash, ...b } = s;
      return envelope(rehash(b, { view: { ...b.view, unknown: true } as RepairReceiptView }));
    }],
    ["owner mismatch with recomputed hashes", s => { const { recordSha256: _hash, ...b } = s; return envelope({ ...b, orgId: "other-org" }); }],
    ["path key mismatch with recomputed hashes", s => { const { recordSha256: _hash, ...b } = s; return envelope(withView(b, { originalInputId: "other-original" })); }],
  ];

  it.each(diskCorruptions)("replay detects stored corruption and latches admission: %s", async (_name, change) => {
    const o = owner();
    const stored = await publish(new EffectStore(o), candidate(o));
    diskRewrite(o, change(stored));
    const reopened = new EffectStore(o);
    await expect(reopened.withSession("settle", tx => tx.list())).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    await expect(reopened.withSession("new-intent", tx => tx.bind(intent(o)))).rejects.toMatchObject({ code: "EFFECT_ADMISSION_CLOSED" });
  });

  it("detects noncanonical stored bytes even when all parsed hashes still match", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const stored = await publish(store, candidate(o));
    fs.writeFileSync(receiptFile(o), JSON.stringify(stored, null, 2), { mode: 0o600 });
    await expect(read(store)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
  });

  it("keeps a missing canonical target uncertain while its retained witness survives, then recovers only that exact inode", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const body = candidate(o);
    const stored = await publish(store, body);
    const witness = path.join(directory(o), names(o).find(name => name.startsWith(".published-receipt.json-"))!);
    fs.unlinkSync(receiptFile(o));
    const before = names(o);
    await expect(read(new EffectStore(o))).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    await expect(publish(store, body)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    expect(names(o)).toEqual(before);
    expect(fs.existsSync(witness)).toBe(true);
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).rejects.toMatchObject({ code: "EFFECT_ADMISSION_CLOSED" });
    fs.linkSync(witness, receiptFile(o));
    expect(await read(store)).toEqual(stored);
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).resolves.toBeDefined();
  });

  it("rejects an identical-byte replacement inode that no longer matches its witness", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const stored = await publish(store, candidate(o));
    fs.unlinkSync(receiptFile(o));
    fs.writeFileSync(receiptFile(o), canonicalEncode(stored), { mode: 0o600 });
    await expect(read(store)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
  });

  it.each(["effects", "repairs", "key", "receipt", "witness", "stage"] as const)("fails closed for wrong mode on %s", async which => {
    const o = owner();
    const store = new EffectStore(o);
    await publish(store, candidate(o));
    const file = which === "effects" ? path.join(o.sessionRoot, "effects") : which === "repairs" ? namespace(o) :
      which === "key" ? directory(o) : which === "receipt" ? receiptFile(o) : path.join(directory(o),
        names(o).find(name => which === "witness" ? name.startsWith(".published-") : name.startsWith("receipt.json."))!);
    fs.chmodSync(file, which === "effects" || which === "repairs" || which === "key" ? 0o755 : 0o644);
    await expect(store.withSession("settle", tx => tx.list())).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).rejects.toMatchObject({ code: "EFFECT_ADMISSION_CLOSED" });
  });

  it.each(["effects", "repairs", "key", "receipt"] as const)("rejects a symlink at the %s namespace boundary", async which => {
    const o = owner();
    const store = new EffectStore(o);
    await publish(store, candidate(o));
    const target = which === "effects" ? path.join(o.sessionRoot, "effects") : which === "repairs" ? namespace(o) :
      which === "key" ? directory(o) : receiptFile(o);
    const retained = path.join(path.dirname(o.sessionRoot), `retained-${which}`);
    fs.renameSync(target, retained);
    fs.symlinkSync(retained, target, which === "receipt" ? "file" : "dir");
    await expect(store.withSession("settle", tx => tx.list())).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    expect(fs.existsSync(retained)).toBe(true);
  });

  it.each(["unknown.json", ".published-unknown.tmp", `.published-intent.json-${randomUUID()}.tmp`])(
    "rejects an unknown committed name or wrong witness family: %s", async name => {
      const o = owner();
      const store = new EffectStore(o);
      await publish(store, candidate(o));
      fs.linkSync(receiptFile(o), path.join(directory(o), name));
      await expect(read(store)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    },
  );

  it("rejects unknown repair directory names and a file in place of a repair namespace", async () => {
    for (const corruption of ["bad-key", "not-a-directory"] as const) {
      const o = owner();
      const store = new EffectStore(o);
      if (corruption === "bad-key") {
        fs.mkdirSync(namespace(o), { recursive: true, mode: 0o700 });
        fs.mkdirSync(path.join(namespace(o), "not-a-hash"), { mode: 0o700 });
      } else {
        fs.mkdirSync(path.join(o.sessionRoot, "effects"), { mode: 0o700 });
        fs.writeFileSync(namespace(o), "not a directory", { mode: 0o600 });
      }
      await expect(store.withSession("settle", tx => tx.list())).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    }
  });

  it("ignores only ordinary unpublished regular stages and retains them through a real publication", async () => {
    const o = owner();
    const store = new EffectStore(o);
    fs.mkdirSync(directory(o), { recursive: true, mode: 0o700 });
    const stage = path.join(directory(o), "unpublished.tmp");
    fs.writeFileSync(stage, "not canonical and not authority", { mode: 0o600 });
    expect(await read(store)).toBeUndefined();
    expect(await store.withSession("settle", tx => tx.list())).toEqual([]);
    await publish(store, candidate(o));
    expect(fs.readFileSync(stage, "utf8")).toBe("not canonical and not authority");
    expect(await read(store)).toEqual(envelope(candidate(o)));
  });

  it.each(["symlink", "wrong-mode"] as const)("still validates a non-authoritative unpublished stage: %s", async corruption => {
    const o = owner();
    const store = new EffectStore(o);
    fs.mkdirSync(directory(o), { recursive: true, mode: 0o700 });
    const stage = path.join(directory(o), "unpublished.tmp");
    if (corruption === "symlink") fs.symlinkSync(o.sessionFile, stage, "file");
    else fs.writeFileSync(stage, "unpublished", { mode: 0o644 });
    await expect(store.withSession("settle", tx => tx.list())).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
  });

  it("does not clobber a different canonical winner at the hard-link publication boundary", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const body = candidate(o);
    const winner = { ...body, originalAdmissionId: "different-winning-admission" };
    const originalLink = fs.linkSync.bind(fs);
    let collided = false;
    const link = vi.spyOn(fs, "linkSync").mockImplementation((source, target): void => {
      if (String(target) === receiptFile(o) && !collided) {
        collided = true;
        fs.writeFileSync(target, canonicalEncode(envelope(winner)), { mode: 0o600, flag: "wx" });
        originalLink(target, path.join(directory(o), `.published-receipt.json-${randomUUID()}.tmp`));
      }
      originalLink(source, target); // real EEXIST: the losing stage must never overwrite
    });
    try { await expect(publish(store, body)).rejects.toMatchObject({ code: "EFFECT_REPAIR_CONFLICT" }); }
    finally { link.mockRestore(); }
    expect(collided).toBe(true);
    expect(await read(store)).toEqual(envelope(winner));
    const loser = names(o).find(name => name.startsWith("receipt.json."))!;
    expect(fs.readFileSync(path.join(directory(o), loser), "utf8")).toBe(canonicalEncode(envelope(body)));
    expect(fs.readFileSync(receiptFile(o), "utf8")).toBe(canonicalEncode(envelope(winner)));
    expect(fs.statSync(path.join(directory(o), loser)).ino).not.toBe(fs.statSync(receiptFile(o)).ino);
  });

  it("cannot clear storage uncertainty merely because all effect records have RESULT", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const record = await issued(o, store);
    const stored = await publish(store, candidate(o, [record]));
    const bytes = fs.readFileSync(receiptFile(o), "utf8");
    diskRewrite(o, { ...stored, recordSha256: digest("corrupt envelope") });
    await expect(read(store)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    await expect(store.withSession("settle", tx => tx.read(record.intent.key))).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o, "next")))).rejects.toMatchObject({ code: "EFFECT_ADMISSION_CLOSED" });
    fs.writeFileSync(receiptFile(o), bytes, { mode: 0o600 });
    expect(await store.withSession("settle", tx => tx.list())).toHaveLength(1);
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o, "next")))).resolves.toBeDefined();
  });

  it("detects a changed retained frontier prefix even if a transition is rehashed consistently", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const record = await issued(o, store);
    await publish(store, candidate(o, [record]));
    const transitionFile = path.join(o.sessionRoot, "effects", record.intent.key, "transitions", "000000000001.json");
    const { digest: _old, ...oldBody } = record.transitions[0]!;
    const changed = { ...oldBody, reason: "tampered but valid transition" };
    fs.writeFileSync(transitionFile, canonicalEncode({ ...changed, digest: digest(canonicalEncode(changed)) }), { mode: 0o600 });
    await expect(read(new EffectStore(o))).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
  });

  it.each(["file", "stage-namespace", "published-namespace"] as const)("retains evidence and recovers an unknown ACK at the %s barrier", async cut => {
    const o = owner();
    const store = new EffectStore(o);
    const body = candidate(o);
    fs.mkdirSync(directory(o), { recursive: true, mode: 0o700 });
    const originalFsync = fs.fsyncSync.bind(fs);
    let injected = false;
    const failure = vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number): void => {
      const isFile = fs.fstatSync(fd).isFile();
      const hasStage = names(o).some(name => name.startsWith("receipt.json."));
      const hasReceipt = fs.existsSync(receiptFile(o));
      if (!injected && (cut === "file" ? isFile : !isFile && hasStage && (cut === "published-namespace" ? hasReceipt : !hasReceipt))) {
        injected = true;
        throw new Error(`injected ${cut} durability failure`);
      }
      originalFsync(fd);
    });
    let afterFailure: string[] = [];
    try {
      await expect(publish(store, body)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
      expect(injected).toBe(true);
      afterFailure = names(o);
      expect(afterFailure.some(name => name.startsWith("receipt.json."))).toBe(true);
    } finally { failure.mockRestore(); }
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).rejects.toMatchObject({ code: "EFFECT_ADMISSION_CLOSED" });
    if (cut === "published-namespace") {
      expect(afterFailure).toHaveLength(3);
      expect(await publish(new EffectStore(o), body)).toEqual(envelope(body));
      expect(names(o)).toEqual(afterFailure); // exact retry has no additional stage
    } else {
      expect(await read(store)).toBeUndefined(); // qualify the retained unpublished namespace
      await publish(store, body);
      expect(names(o)).toHaveLength(afterFailure.length + 3);
      expect(names(o)).toEqual(expect.arrayContaining(afterFailure));
    }
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).resolves.toBeDefined();
  });

  it("recovers publication before witness creation without another staging file", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const body = candidate(o);
    const originalLink = fs.linkSync.bind(fs);
    const link = vi.spyOn(fs, "linkSync").mockImplementation((source, target): void => {
      if (path.basename(String(target)).startsWith(".published-receipt.json-")) throw new Error("lost ACK before witness creation");
      originalLink(source, target);
    });
    try { await expect(publish(store, body)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" }); }
    finally { link.mockRestore(); }
    expect(fs.existsSync(receiptFile(o))).toBe(true);
    const before = names(o);
    expect(before).toHaveLength(2);
    expect(await publish(new EffectStore(o), body)).toEqual(envelope(body));
    const after = names(o);
    expect(after.filter(name => name.startsWith("receipt.json."))).toEqual(before.filter(name => name.startsWith("receipt.json.")));
    expect(after).toHaveLength(3); // recovery completes the witness, never another stage
    const witness = after.find(name => name.startsWith(".published-receipt.json-"))!;
    expect(fs.statSync(path.join(directory(o), witness)).ino).toBe(fs.statSync(receiptFile(o)).ino);
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).resolves.toBeDefined();
    fs.unlinkSync(receiptFile(o));
    await expect(read(store)).rejects.toMatchObject({ code: "EFFECT_STORE_UNCERTAIN" });
  });

  it("does not return an exact replay when its reread barrier fails; the latch survives caught failure", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const body = candidate(o);
    await publish(store, body);
    const before = names(o);
    const fsync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("replay namespace unavailable"); });
    try {
      await store.withSession("settle", tx => {
        expectCode(() => tx.publishMaintenanceRepair(body), "EFFECT_STORE_UNCERTAIN");
        expectCode(() => tx.listMaintenanceRepairs(), "EFFECT_STORE_UNCERTAIN");
      });
    } finally { fsync.mockRestore(); }
    expect(names(o)).toEqual(before);
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).rejects.toMatchObject({ code: "EFFECT_ADMISSION_CLOSED" });
    expect(await publish(store, body)).toEqual(envelope(body));
    expect(names(o)).toEqual(before);
  });

  it("retains the receipt after an outside-store consumer failure without creating a global latch", async () => {
    const o = owner();
    const store = new EffectStore(o);
    const body = candidate(o);
    await expect(store.withSession("settle", tx => {
      tx.publishMaintenanceRepair(body);
      throw new Error("consumer stopped after publication");
    })).rejects.toThrow("consumer stopped after publication");
    expect(await read(store)).toEqual(envelope(body));
    await expect(store.withSession("new-intent", tx => tx.bind(intent(o)))).resolves.toBeDefined();
  });

  it("serializes receipt publishers across stores and rejects escaped synchronous operations", async () => {
    const o = owner();
    const a = new EffectStore(o);
    const b = new EffectStore(o);
    const body = candidate(o);
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const ready = new Promise<void>(resolve => { entered = resolve; });
    let escaped!: EffectStoreTransaction;
    const first = a.withSession("settle", async tx => {
      escaped = tx;
      const result = tx.publishMaintenanceRepair(body);
      entered();
      await gate;
      return result;
    });
    await ready;
    const before = names(o);
    let secondEntered = false;
    const second = b.withSession("settle", tx => { secondEntered = true; return tx.publishMaintenanceRepair(body); });
    await Promise.resolve();
    expect(secondEntered).toBe(false);
    release();
    const results = await Promise.all([first, second]);
    expect(results[0]).toEqual(results[1]);
    expect(names(o)).toEqual(before);
    for (const action of [() => escaped.readMaintenanceRepair(ORIGINAL), () => escaped.listMaintenanceRepairs(),
      () => escaped.publishMaintenanceRepair(body)]) {
      expectCode(action, "EFFECT_TRANSACTION_CLOSED");
    }
  });
});
