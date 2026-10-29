import { createHash } from "node:crypto";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { sessionRecoveryAdapters } from "./l7-interfaces.js";
import type { AcceptedRecoveryInput, SessionRecoveryBinding, SessionRecoveryContext, NativeRecoveryAppend } from "@earendil-works/pi-coding-agent";
import type { AcceptedRecoveryMaintenance, MaintenanceRecord, RecoveryDurabilityAdapter, RecoveryEpoch, RecoveryGateSubject, RecoveryMaintenanceAdapter, ProviderPurpose } from "./l7-interfaces.js";

// Recovery I/O has its own bounded budget. Never restamp the immutable inference deadline.
const NATIVE_RECOVERY_IO_TIMEOUT_MS = 10_000;
const hash = (bytes: string | Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const nativeId = (sessionId: string, entryId: string) => "pi-native-entry:v1:" + hash(JSON.stringify([sessionId, entryId]));
const equalEpoch = (a: RecoveryEpoch, b: RecoveryEpoch) => a.processIncarnationId === b.processIncarnationId && a.bindingId === b.bindingId && a.epoch === b.epoch;
function refuse(code: string): never { throw Object.assign(new Error(code), { code }); }

/** Native/B2/B4 base ports. They are mandatory, not invented admission/repair/terminal
 * implementations. In particular reconciliation is storage/repair only while B5 owns a
 * maintenance reservation; B5's release edge is the sole post-join input driver. */
export interface NativeRecoveryHost {
  readonly sessionId: string;
  readonly sessionFile: string;
  assertCurrent(epoch: RecoveryEpoch): void;
  advanceEpoch(epoch: RecoveryEpoch): void;
  assertNativeDescriptor(entry: NativeRecoveryAppend): void;
  /** Native private shared attempt; permits settlement/WAL only after computation is revoked. */
  assertMaintenanceScope(token: Readonly<AcceptedRecoveryMaintenance>): void;
  deliver(context: SessionRecoveryContext): Promise<void>;
  reconcile(context: SessionRecoveryContext, reason: string): Promise<void>;
  reconsiderMain(context: SessionRecoveryContext): void | Promise<void>;
  authorizeInput(input: Readonly<AcceptedRecoveryInput>, purpose: ProviderPurpose, epoch: RecoveryEpoch): Promise<void>;
  projectInput(messages: AgentMessage[], input: Readonly<AcceptedRecoveryInput>, epoch: RecoveryEpoch): Promise<AgentMessage[]>;
  /** Resolve the immutable source/range/repair pin, never use a borrowed input permit. */
  projectMaintenance(messages: AgentMessage[], record: Readonly<MaintenanceRecord>, epoch: RecoveryEpoch): Promise<AgentMessage[]>;
  validateCompaction: SessionRecoveryBinding["validateCompaction"];
  repairMaintenance: NonNullable<SessionRecoveryBinding["repairMaintenance"]>;
  recordInputOutcome: SessionRecoveryBinding["recordInputOutcome"];
  getCancelTargets: SessionRecoveryBinding["getCancelTargets"];
  validateTerminal: SessionRecoveryBinding["validateTerminal"];
}

/** [D] native base-binding/lease/repair host and B6 are hard dependencies, not stubs. */
export type NativeMaintenanceInstallOptions = Omit<Parameters<typeof createNativeMaintenanceBinding>[0], "maintenance">;

/** Register before SDK construction; resolve after all extension factories registered. */
export function installNativeMaintenanceBinding(
  pi: Pick<ExtensionAPI, "events" | "registerSessionRecovery">,
  options: NativeMaintenanceInstallOptions,
): SessionRecoveryBinding {
  let resolved: SessionRecoveryBinding | undefined;
  const resolve = (): SessionRecoveryBinding => {
    if (resolved) return resolved;
    const adapters = sessionRecoveryAdapters(pi);
    if (!adapters) refuse("RECOVERY_MAINTENANCE_UNBOUND");
    return resolved = createNativeMaintenanceBinding({ ...options, maintenance: adapters.maintenance });
  };
  const binding = new Proxy({} as SessionRecoveryBinding, { get: (_target, key) => Reflect.get(resolve(), key) });
  if (typeof pi.registerSessionRecovery !== "function") refuse("RECOVERY_NATIVE_BINDING_REQUIRED");
  pi.registerSessionRecovery(binding);
  return binding;
}

/** The B3 composition of the real B5 port and unchanged B6 registration/barrier/read.
 * A token-shaped JSON value is never enough: B5's private scope checks every compute
 * gate; the post-join settlement path requires the exact ledger PREPARED plan bytes. */
export function createNativeMaintenanceBinding(options: {
  host: NativeRecoveryHost; maintenance: RecoveryMaintenanceAdapter; durability: RecoveryDurabilityAdapter;
}): SessionRecoveryBinding {
  const { host, maintenance, durability } = options;
  for (const key of ["assertCurrent", "advanceEpoch", "assertNativeDescriptor", "assertMaintenanceScope", "deliver", "reconcile", "reconsiderMain", "authorizeInput", "projectInput", "projectMaintenance", "validateCompaction", "repairMaintenance", "recordInputOutcome", "getCancelTargets", "validateTerminal"] as const) {
    if (typeof host?.[key] !== "function") refuse("RECOVERY_BINDING_INCOMPLETE");
  }
  for (const key of ["admitMaintenance", "beginMaintenance", "prepareMaintenance", "consumeMaintenance", "adoptMaintenance", "read", "listPending", "withMaintenanceAttempt", "assertMaintenanceGate", "cancelMaintenance", "reconcileAfterJoin", "captureTaskAbort", "releaseLostMaintenance"] as const) {
    if (typeof maintenance?.[key] !== "function") refuse("RECOVERY_BINDING_INCOMPLETE");
  }
  if (!durability || typeof durability.registerNativeEntry !== "function" || typeof durability.awaitDurableAppend !== "function" || typeof durability.readDurableEntry !== "function") refuse("RECOVERY_BINDING_INCOMPLETE");
  const current = (epoch: RecoveryEpoch) => host.assertCurrent(epoch);
  const recordFor = async (subject: Extract<RecoveryGateSubject, { admissionKind: "maintenance" }>, epoch: RecoveryEpoch, ioDeadlineMs = subject.deadlineMs): Promise<MaintenanceRecord> => {
    current(epoch);
    if (!equalEpoch(subject.epoch, epoch) || subject.sessionId !== host.sessionId) refuse("RECOVERY_EPOCH_REVOKED");
    const read = await maintenance.read(subject, { deadlineMs: ioDeadlineMs });
    current(epoch);
    if (!read.ok || !read.value.record) return refuse("RECOVERY_MAINTENANCE_UNCERTAIN");
    const record = read.value.record;
    if (!equalEpoch(record.activeEpoch, epoch) || record.permitRevision !== subject.permitRevision || record.operationSha256 !== subject.operationSha256 || record.request.deadlineMs !== subject.deadlineMs) refuse("RECOVERY_MAINTENANCE_TOKEN_REUSED");
    return record;
  };
  const maintenanceSubject = (subject: RecoveryGateSubject): subject is Extract<RecoveryGateSubject, { admissionKind: "maintenance" }> => "admissionKind" in subject && subject.admissionKind === "maintenance";
  const refFor = (entry: NativeRecoveryAppend): string => {
    const parsed = JSON.parse(Buffer.from(entry.bytes).toString("utf8"));
    return parsed.type === "message" && parsed.message?.role === "user" && typeof parsed.metadata?.piRecovery?.deliveryId === "string"
      ? parsed.metadata.piRecovery.deliveryId : nativeId(host.sessionId, entry.entryId);
  };
  const checkDescriptor = (entry: NativeRecoveryAppend): void => {
    host.assertNativeDescriptor(entry);
    if (entry.sessionId !== host.sessionId || entry.location.file !== host.sessionFile || entry.bytes.length !== entry.location.length + 1 || entry.bytes.at(-1) !== 10 || hash(entry.bytes) !== entry.bytesSha256) refuse("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
    const parsed = JSON.parse(Buffer.from(entry.bytes).toString("utf8"));
    if (parsed.id !== entry.entryId) refuse("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
  };
  const readEntry = async (entry: NativeRecoveryAppend, epoch: RecoveryEpoch) => {
    current(epoch); checkDescriptor(entry);
    const found = await durability.readDurableEntry({ sessionId: host.sessionId, deliveryId: refFor(entry), recordKind: "carrier" });
    current(epoch);
    if (!found.found || !Buffer.from(found.bytes).equals(Buffer.from(entry.bytes))) return refuse("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
    return { entryId: entry.entryId, bytesSha256: entry.bytesSha256, seq: found.seq };
  };
  const binding: SessionRecoveryBinding = {
    maintenance,
    advanceEpoch: epoch => host.advanceEpoch(epoch),
    deliver: context => host.deliver(context),
    reconcile: (context, reason) => host.reconcile(context, reason),
    reconsiderMain: context => { current(context.epoch); return host.reconsiderMain(context); },
    repairMaintenance: (input, leaf) => host.repairMaintenance(input, leaf),
    recordInputOutcome: request => host.recordInputOutcome(request),
    getCancelTargets: () => host.getCancelTargets(),
    validateTerminal: (input, entries) => host.validateTerminal(input, entries),
    beforeProvider: async ({ input, epoch, purpose, signal }) => {
      current(epoch); signal?.throwIfAborted();
      if (maintenanceSubject(input)) {
        const record = await recordFor(input, epoch);
        maintenance.assertMaintenanceGate(input, purpose, record.request.viewSha256);
      } else {
        if (purpose !== "turn" && purpose !== "compact-summary") refuse("RECOVERY_MAINTENANCE_PURPOSE_UNSUPPORTED");
        await host.authorizeInput(input, purpose, epoch);
      }
      current(epoch); signal?.throwIfAborted(); return { permitted: true };
    },
    projectContext: async (messages, subject, epoch) => {
      current(epoch);
      if (!maintenanceSubject(subject)) {
        const projected = await host.projectInput(structuredClone(messages), subject, epoch); current(epoch); return structuredClone(projected);
      }
      const record = await recordFor(subject, epoch);
      const purpose = record.request.operation.kind === "tree-nav" ? "tree-summary" : "compact-summary";
      maintenance.assertMaintenanceGate(subject, purpose, record.request.viewSha256);
      const projected = await host.projectMaintenance(structuredClone(messages), record, epoch);
      await recordFor(subject, epoch); maintenance.assertMaintenanceGate(subject, purpose, record.request.viewSha256);
      return structuredClone(projected);
    },
    validateCompaction: async (subject, preparation, cut, epoch) => {
      current(epoch);
      if (maintenanceSubject(subject)) {
        const record = await recordFor(subject, epoch);
        if (record.request.operation.kind === "tree-nav") refuse("RECOVERY_MAINTENANCE_KIND_MISMATCH");
        maintenance.assertMaintenanceGate(subject, "compact-summary", record.request.viewSha256);
      }
      await host.validateCompaction(subject, preparation, cut, epoch); current(epoch);
      if (maintenanceSubject(subject)) {
        const record = await recordFor(subject, epoch); maintenance.assertMaintenanceGate(subject, "compact-summary", record.request.viewSha256);
      }
    },
    commitNativeEntries: async (entries, subject, epoch) => {
      current(epoch);
      const deadlineMs = subject && !maintenanceSubject(subject) ? subject.deadlineMs : Date.now() + NATIVE_RECOVERY_IO_TIMEOUT_MS;
      let allowed: Map<string, string> | undefined;
      if (subject && maintenanceSubject(subject)) {
        host.assertMaintenanceScope(subject);
        const record = await recordFor(subject, epoch, deadlineMs);
        host.assertMaintenanceScope(subject);
        if ((record.state !== "PREPARED" && record.state !== "CONSUMED") || !record.plan) refuse("RECOVERY_MAINTENANCE_UNCERTAIN");
        const prepare = entries.find(entry => entry.entryId === record.plan!.entryId);
        if (!prepare || prepare.bytesSha256 !== record.plan.bytesSha256) refuse("RECOVERY_MAINTENANCE_UNCERTAIN");
        const parsed = JSON.parse(Buffer.from(prepare.bytes).toString("utf8"));
        const plan = parsed.data?.plan;
        if (parsed.customType !== "pi-maintenance:prepare:v1" || !plan || plan.requestId !== subject.requestId || plan.operationSha256 !== record.operationSha256 || hash(JSON.stringify(plan)) !== record.plan.planSha256) refuse("RECOVERY_MAINTENANCE_UNCERTAIN");
        allowed = new Map<string, string>(plan.entries.map((entry: { entryId: string; bytesSha256: string }) => [entry.entryId, entry.bytesSha256]));
        allowed.set(prepare.entryId, prepare.bytesSha256);
        if (allowed.size !== entries.length) refuse("RECOVERY_MAINTENANCE_UNCERTAIN");
      }
      const receipts: Array<{ entryId: string; bytesSha256: string; seq: number }> = [];
      for (const entry of entries) {
        current(epoch); checkDescriptor(entry);
        if (allowed) { if (allowed.get(entry.entryId) !== entry.bytesSha256) refuse("RECOVERY_MAINTENANCE_CONFLICT"); }
        else if (!subject) { if (entry.authority?.kind !== "control" || !["model_change", "thinking_level_change", "label", "session_info", "custom", "usage"].includes(JSON.parse(Buffer.from(entry.bytes).toString("utf8")).type)) refuse("RECOVERY_MAINTENANCE_UNBOUND"); }
        else if (entry.authority?.kind !== "input" || entry.authority.input.deliveryId !== (subject as AcceptedRecoveryInput).deliveryId) refuse("RECOVERY_MAINTENANCE_UNBOUND");
        const request = { sessionId: host.sessionId, deliveryId: refFor(entry), recordKind: "carrier" as const, entryId: entry.entryId,
          location: { ...entry.location }, bytes: Uint8Array.from(entry.bytes), bytesSha256: entry.bytesSha256, flushReceipt: { ...entry.flushReceipt }, deadlineMs };
        const registered = await durability.registerNativeEntry(request); current(epoch);
        if (!registered.registered) refuse("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
        const durable = await durability.awaitDurableAppend(request); current(epoch);
        if (!durable.durable) refuse("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
        const receipt = await readEntry(entry, epoch);
        if (receipt.seq !== durable.seq) refuse("RECOVERY_NATIVE_DURABILITY_UNCONFIRMED");
        receipts.push(receipt);
      }
      return receipts;
    },
    verifyNativeEntries: async (entries, epoch) => { for (const entry of entries) await readEntry(entry, epoch); },
    validateMaintenanceReceipts: async (record, entries, epoch) => {
      current(epoch);
      if (record.request.sessionId !== host.sessionId || record.state !== "CONSUMED" || !record.completion || !record.plan) refuse("RECOVERY_MAINTENANCE_UNCERTAIN");
      for (const expected of record.completion.entries) {
        const entry = entries.find(value => value.entryId === expected.entryId);
        if (!entry || entry.bytesSha256 !== expected.bytesSha256 || (await readEntry(entry, epoch)).seq !== expected.seq) refuse("RECOVERY_MAINTENANCE_UNCERTAIN");
      }
    },
  };
  return binding;
}
