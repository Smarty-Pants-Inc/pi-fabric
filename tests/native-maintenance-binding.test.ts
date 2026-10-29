// Keep: maintenance gates must never become input permission or infer durability from registration.
// These are composition tests, not B5's store or physical host-stop qualification.
import { createHash } from "node:crypto";
import { createEventBus } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import type { NativeRecoveryAppend, SessionRecoveryBinding } from "@earendil-works/pi-coding-agent";
import { createNativeMaintenanceBinding, installNativeMaintenanceBinding, type NativeRecoveryHost } from "../src/recovery/native-maintenance-binding.js";
import { registerSessionRecoveryAdapters, sessionRecoveryAdapters, type AcceptedRecoveryMaintenance, type MaintenanceRecord, type RecoveryDurabilityAdapter, type RecoveryEpoch, type RecoveryMaintenanceAdapter, type SessionRecoveryAdapters } from "../src/recovery/l7-interfaces.js";
const hash = (bytes: string | Uint8Array) => createHash("sha256").update(bytes).digest("hex");
function fixture(remainingMs = 60_000) {
  let currentEpoch: RecoveryEpoch = { processIncarnationId: "incarnation", bindingId: "binding", epoch: 1 };
  const epoch = { ...currentEpoch };
  const token: AcceptedRecoveryMaintenance = { admissionKind: "maintenance", sessionId: "s", requestId: "request", epoch, operationSha256: hash("operation"), permitRevision: 1, deadlineMs: Date.now() + remainingMs };
  const record: MaintenanceRecord = { request: { version: 1, admissionKind: "maintenance", sessionId: "s", requestId: "request", epoch, operation: { kind: "compact", reason: "manual" }, baseLeafId: "before", baseHistorySha256: hash("history"), viewSha256: hash("view"), issuedAtMs: Math.min(Date.now(), token.deadlineMs - 1000), deadlineMs: token.deadlineMs }, operationSha256: token.operationSha256, revision: 2, activeEpoch: epoch, permitRevision: 1, state: "ACTIVE" };
  let inPrivateNativeCommit = false;
  const permittedDescriptors = new Map<string, NativeRecoveryAppend>();
  const host: NativeRecoveryHost = {
    sessionId: "s", sessionFile: "/retained/session.jsonl",
    assertCurrent: value => { if (JSON.stringify(value) !== JSON.stringify(currentEpoch)) throw Object.assign(new Error("stale"), { code: "RECOVERY_EPOCH_REVOKED" }); },
    advanceEpoch: vi.fn(),
    assertNativeDescriptor: entry => { if (permittedDescriptors.get(entry.entryId) !== entry) throw new Error("untrusted native descriptor"); },
    assertMaintenanceScope: () => { if (!inPrivateNativeCommit) throw new Error("RECOVERY_MAINTENANCE_UNBOUND"); },
    deliver: vi.fn(async () => {}), reconcile: vi.fn(async () => {}), reconsiderMain: vi.fn(),
    authorizeInput: vi.fn(async () => {}), projectInput: vi.fn(async messages => messages), projectMaintenance: vi.fn(async messages => messages),
    validateCompaction: vi.fn(async () => {}), repairMaintenance: vi.fn(async () => { throw new Error("not used"); }),
    recordInputOutcome: vi.fn(async () => ({ durable: true as const, status: "recorded" as const, revision: 1 })), getCancelTargets: () => [], validateTerminal: async () => ({ complete: true }),
  };
  const notUsed = () => { throw new Error("not used"); };
  const maintenance: RecoveryMaintenanceAdapter = {
    read: vi.fn(async () => ({ ok: true as const, value: { revision: 2, record: structuredClone(record) } })),
    assertMaintenanceGate: vi.fn((value, purpose, view) => { if (value !== token) throw new Error("RECOVERY_MAINTENANCE_UNBOUND"); if (purpose !== "compact-summary" || view !== record.request.viewSha256) throw new Error("RECOVERY_MAINTENANCE_PURPOSE_UNSUPPORTED"); }),
    admitMaintenance: notUsed, beginMaintenance: notUsed, prepareMaintenance: notUsed, consumeMaintenance: notUsed, adoptMaintenance: notUsed, listPending: notUsed, withMaintenanceAttempt: notUsed, cancelMaintenance: notUsed, reconcileAfterJoin: notUsed, captureTaskAbort: notUsed, releaseLostMaintenance: notUsed,
  };
  const committed = new Map<string, { bytes: Uint8Array; seq: number }>(), staged = new Map<string, Uint8Array>();
  const durability: RecoveryDurabilityAdapter = {
    registerNativeEntry: vi.fn(async request => { staged.set(request.deliveryId, Uint8Array.from(request.bytes)); return { registered: true as const, existing: false }; }),
    awaitDurableAppend: vi.fn(async request => { const bytes = staged.get(request.deliveryId); if (!bytes) throw new Error("unregistered"); committed.set(request.deliveryId, { bytes, seq: committed.size + 1 }); return { durable: true as const, seq: committed.get(request.deliveryId)!.seq }; }),
    readDurableEntry: vi.fn(async request => { const value = committed.get(request.deliveryId); return value ? { found: true as const, ...value } : { found: false as const, reason: "not-found" as const }; }),
  };
  const binding = createNativeMaintenanceBinding({ host, maintenance, durability });
  const control = (id: string, type = "model_change", extra: Record<string, unknown> = {}) => {
    const bytes = Buffer.from(JSON.stringify({ id, parentId: null, type, timestamp: "2026-10-28T00:00:00Z", provider: "fixture", modelId: "physical", ...extra }) + "\n");
    const entry: NativeRecoveryAppend = { sessionId: "s", entryId: id, bytes, bytesSha256: hash(bytes), location: { file: host.sessionFile, offset: 0, length: bytes.length - 1 }, flushReceipt: { kind: "file-fsync", file: host.sessionFile, bytesThrough: bytes.length, namespaceDurable: true }, authority: { kind: "control" } };
    permittedDescriptors.set(id, entry); return entry;
  };
  const providers: SessionRecoveryAdapters = { maintenance, identity: { validate: notUsed, verifyPredecessor: notUsed }, inputs: { readRecoveryInput: notUsed, admitContinuation: notUsed }, effects: { settlePendingIntents: notUsed, readToolResult: notUsed } };
  return { epoch, token, record, host, maintenance, durability, binding, control, committed, providers, setEpoch: (value: RecoveryEpoch) => { currentEpoch = value; }, enterCommit: () => { inPrivateNativeCommit = true; } };
}
describe("native maintenance composition", () => {
  it("discovers maintenance with the other providers; partial, malformed and conflicting composition refuses", () => {
    const f = fixture(), pi = { events: createEventBus() };
    registerSessionRecoveryAdapters(pi, { identity: f.providers.identity, inputs: f.providers.inputs, effects: f.providers.effects });
    expect(() => sessionRecoveryAdapters(pi)).toThrow("maintenance");
    const unsubscribe = registerSessionRecoveryAdapters(pi, { maintenance: f.maintenance });
    expect(sessionRecoveryAdapters(pi)?.maintenance).toBe(f.maintenance);
    const conflict = registerSessionRecoveryAdapters(pi, { maintenance: { ...f.maintenance } });
    expect(() => sessionRecoveryAdapters(pi)).toThrow("Conflicting session recovery maintenance providers"); conflict(); unsubscribe();
    registerSessionRecoveryAdapters(pi, { maintenance: {} as never }); expect(() => sessionRecoveryAdapters(pi)).toThrow("Malformed session recovery maintenance API");
  });
  it("registers before providers exist but resolves after all factories; missing base ports never become no-ops", () => {
    const f = fixture(); let registered: SessionRecoveryBinding | undefined;
    const pi = { events: createEventBus(), registerSessionRecovery: (value: SessionRecoveryBinding) => { registered = value; } };
    installNativeMaintenanceBinding(pi, { host: f.host, durability: f.durability }); expect(registered).toBeDefined();
    expect(() => registered!.advanceEpoch?.(f.epoch)).toThrow("RECOVERY_MAINTENANCE_UNBOUND");
    registerSessionRecoveryAdapters(pi, f.providers); registered!.advanceEpoch?.(f.epoch); expect(f.host.advanceEpoch).toHaveBeenCalledOnce();
    expect(() => createNativeMaintenanceBinding({ host: { ...f.host, reconsiderMain: undefined } as never, maintenance: f.maintenance, durability: f.durability })).toThrow("RECOVERY_BINDING_INCOMPLETE");
  });
  it("separates maintenance purpose/scope from input permission and rejects changed epochs", async () => {
    const f = fixture();
    await expect(f.binding.beforeProvider({ input: f.token, epoch: f.epoch, purpose: "compact-summary" })).resolves.toEqual({ permitted: true });
    expect(f.host.authorizeInput).not.toHaveBeenCalled();
    await expect(f.binding.beforeProvider({ input: { ...f.token }, epoch: f.epoch, purpose: "compact-summary" })).rejects.toThrow("RECOVERY_MAINTENANCE_UNBOUND");
    await expect(f.binding.beforeProvider({ input: f.token, epoch: f.epoch, purpose: "turn" })).rejects.toThrow("RECOVERY_MAINTENANCE_PURPOSE_UNSUPPORTED");
    f.setEpoch({ ...f.epoch, epoch: 2 }); await expect(f.binding.projectContext([], f.token, f.epoch)).rejects.toMatchObject({ code: "RECOVERY_EPOCH_REVOKED" });
  });
  it("barriers authority-free native controls and requires read-only existing B6 proof on reopen", async () => {
    const f = fixture(), entry = f.control("control");
    const receipts = await f.binding.commitNativeEntries([entry], undefined, f.epoch);
    expect(receipts).toEqual([{ entryId: entry.entryId, bytesSha256: entry.bytesSha256, seq: 1 }]);
    expect(f.durability.registerNativeEntry).toHaveBeenCalledOnce(); expect(f.durability.awaitDurableAppend).toHaveBeenCalledOnce();
    await f.binding.verifyNativeEntries([entry], f.epoch); expect(f.durability.registerNativeEntry).toHaveBeenCalledOnce();
    f.committed.clear(); await expect(f.binding.verifyNativeEntries([entry], f.epoch)).rejects.toThrow("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
    await expect(f.binding.commitNativeEntries([{ ...entry, authority: undefined } as never], undefined, f.epoch)).rejects.toThrow();
  });
  it("copied operation data cannot perform a native plan commit outside the native private scope", async () => {
    const f = fixture();
    await expect(f.binding.commitNativeEntries([], { ...f.token }, f.epoch)).rejects.toThrow("RECOVERY_MAINTENANCE_UNBOUND");
    expect(f.durability.registerNativeEntry).not.toHaveBeenCalled();
  });
  it("settles PREPARED after inference expiry with a separate I/O budget, without reopening inference", async () => {
    const f = fixture(-1000), originalDeadline = f.token.deadlineMs;
    const checkpoint = f.control("checkpoint", "custom", { customType: "pi-maintenance:checkpoint:v1", data: { requestId: f.token.requestId } });
    const plan = { requestId: f.token.requestId, operationSha256: f.token.operationSha256, outcome: "cancelled", entries: [{ entryId: checkpoint.entryId, parentId: null, bytesBase64: Buffer.from(checkpoint.bytes).toString("base64"), bytesSha256: checkpoint.bytesSha256 }] };
    const prepare = f.control("prepare", "custom", { customType: "pi-maintenance:prepare:v1", data: { plan } });
    f.record.state = "PREPARED"; f.record.plan = { entryId: prepare.entryId, bytesSha256: prepare.bytesSha256, planSha256: hash(JSON.stringify(plan)) };
    const register = vi.mocked(f.durability.registerNativeEntry).getMockImplementation()!;
    vi.mocked(f.durability.registerNativeEntry).mockImplementation(request => { expect(request.deadlineMs).toBeGreaterThan(Date.now()); return register(request); });
    f.enterCommit(); await expect(f.binding.commitNativeEntries([prepare, checkpoint], f.token, f.epoch)).resolves.toHaveLength(2);
    expect(vi.mocked(f.maintenance.read).mock.calls[0]![1]?.deadlineMs).toBeGreaterThan(Date.now());
    expect(f.token.deadlineMs).toBe(originalDeadline); expect(f.record.request.deadlineMs).toBe(originalDeadline);
    expect(f.maintenance.assertMaintenanceGate).not.toHaveBeenCalled();
    vi.mocked(f.maintenance.assertMaintenanceGate).mockImplementation(() => { throw new Error("RECOVERY_MAINTENANCE_DEADLINE"); });
    await expect(f.binding.beforeProvider({ input: f.token, epoch: f.epoch, purpose: "compact-summary" })).rejects.toThrow("RECOVERY_MAINTENANCE_DEADLINE");
  });
  it("registration success is not a durable barrier or an authoritative byte read", async () => {
    const f = fixture(), entry = f.control("not-durable");
    vi.mocked(f.durability.awaitDurableAppend).mockResolvedValue({ durable: false, reason: "store-unavailable" });
    await expect(f.binding.commitNativeEntries([entry], undefined, f.epoch)).rejects.toThrow("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
    expect(f.committed.size).toBe(0);
  });
});
