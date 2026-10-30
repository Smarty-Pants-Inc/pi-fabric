// TEST ONLY. Bounded observed-B model; no production decoder, physical fence or durability.
import { createHash, randomUUID } from "node:crypto";
import type {
  ActualMountTarget, Assurance, B1ErrorCode, B1PortFailure, B1StoragePorts,
  ControllerRecordRef, CurrentMountGenerationRequest, EngineIncarnation, EvidenceRef,
  GenerationRef, MountGeneration, PredecessorInventory, ReceiverIdentity, ReceiverReceipt,
  RevokeGenerationRequest, RevokeReceipt, StorageIdentity,
} from "./contract.js";
import type {
  B1Fact, B1Harness, B1MountFault, B1Observation, B1Scenario, B1TeardownItem,
} from "./harness.js";

const copy = <T>(value: T): T => structuredClone(value);
const sha256 = (bytes: string): string => createHash("sha256").update(bytes, "utf8").digest("hex");

/** Local fixture encoding, NOT a proposed/frozen production wire encoding. */
function canonical(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((entry: unknown) => canonical(entry)).join(",")}]`;
  if (typeof value === "object") {
    return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, child]: [string, unknown]) => `${JSON.stringify(key)}:${canonical(child)}`).join(",")}}`;
  }
  throw new Error("Test-only canonical payload contains an unsupported value");
}
function semantics(req: RevokeGenerationRequest): string {
  const { requestSha256: _hash, deadlineUnixMs: _deadline, ...semantic } = req;
  return canonical(semantic);
}
const same = (a: unknown, b: unknown): boolean => canonical(a) === canonical(b);
const teardownItems: readonly B1TeardownItem[] = ["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8"];
const factKeys: readonly B1Fact[] = ["inventoryComplete", "restartDisabled", "killIssued", "workloadsExited",
  "oldPathRetired", "oldReplicasExited", "oldIoDrained", "driverOutcomeKnown", "nodeReachable",
  "coverageContinuous", "newReplicasAfterCut"];

class ModelFailure extends Error implements B1PortFailure {
  override readonly name = "B1PortFailure";
  readonly operationId?: string;
  readonly durableState?: ControllerRecordRef;
  readonly evidence: readonly EvidenceRef[];
  constructor(readonly code: B1ErrorCode, readonly disposition: "refused" | "held", message: string,
    context: { operationId?: string; durableState?: ControllerRecordRef; evidence: readonly EvidenceRef[] }) {
    super(message);
    if (context.operationId !== undefined) this.operationId = context.operationId;
    if (context.durableState !== undefined) this.durableState = copy(context.durableState);
    this.evidence = copy(context.evidence);
  }
}
interface ModelFd {
  role: "lock" | "generation";
  mountId: string;
  namespaceInode: string;
  deviceLifetimeId: string;
  filesystemUUID: string;
  inode: string;
}
interface ModelMount {
  storage: StorageIdentity;
  self: ReceiverIdentity;
  target: ActualMountTarget;
  acquisitionId: string;
  engine: EngineIncarnation;
  actual: MountGeneration["actualMount"];
  lock: ModelFd | undefined;
  generation: ModelFd | undefined;
  lineageKnown: boolean;
  observerAvailable: boolean;
  observerFresh: boolean;
  replicas: readonly { id: string; startedAfterCut: boolean }[];
  attachObservation: EvidenceRef;
}
interface Operation {
  operationId: string;
  requestSha256: string;
  semanticPayloadBytes: string;
  reservationRevision: string;
  state: "held" | "committed";
  restartRevocationPersisted: boolean;
  killSubmitted: boolean;
  retirementRequested: boolean;
  receipt: RevokeReceipt | undefined;
  payloadBytes: string | undefined;
}

export function createTestOnlyFakeHarness(scenario: B1Scenario = {}): B1Harness {
  let id = `test-only-b1-${randomUUID()}`;
  const storage: StorageIdentity = {
    clusterUID: `${id}:cluster`, identityRecordUID: `${id}:identity`, sessionId: `${id}:session`,
    pvcUID: `${id}:pvc`, pvUID: `${id}:pv`, csiDriver: "driver.longhorn.io",
    volumeHandle: `${id}:volume`, filesystemUUID: `${id}:filesystem`,
  };
  let oldReceiver: ReceiverIdentity = { podUID: `${id}:old-pod`, nodeUID: `${id}:old-node`, nodeName: "old-node" };
  let receiver: ReceiverIdentity = { podUID: `${id}:receiver-pod`, nodeUID: `${id}:receiver-node`, nodeName: "receiver-node" };
  const engineFor = (label: string, nodeUID: string): EngineIncarnation => ({
    engineObjectUID: `${id}:${label}-engine-object`, engineName: `${label}-engine`,
    instanceManagerPodUID: `${id}:${label}-im`, processUUID: `${id}:${label}-process`,
    nodeUID, nodeBootId: `${id}:${label}-boot`,
  });
  let oldEngine = engineFor("old", oldReceiver.nodeUID), engine = engineFor("receiver", receiver.nodeUID);
  let acquisitionId = `${id}:acquisition`, operationId = `${id}:operation`, reservationRevision = "7";
  let target: ActualMountTarget = { bootstrapHandle: `${id}:authenticated-bootstrap-handle`,
    protectedRoot: "/test-only/protected", lockRelativePath: "custody/lock", generationRelativePath: "custody/custody.gen" };
  let bootstrapExecutionId = `${id}:bootstrap-execution`;
  const evidence = new Map<string, { ref: EvidenceRef; payloadBytes: string; payload: unknown }>();
  // Bounded helper-memory replies only, not protected/controller storage or reopen durability.
  const mountReplies = new Map<string, { ref: EvidenceRef; payload: unknown }>();
  // Distinct test-only context, never a controller/helper authentication implementation.
  const retainEvidence = (kind: string, data: unknown): EvidenceRef => {
    const payload = { version: 1, kind, trustDomain: "test-only", data: copy(data) };
    const payloadBytes = canonical(payload), digest = sha256(payloadBytes);
    const ref = { uri: `test-only-memory://${id}/${kind}/${digest}`, sha256: digest };
    evidence.set(ref.uri, { ref, payloadBytes, payload });
    return copy(ref);
  };
  const recordFor = (label: string, bytes: string, revision: string): ControllerRecordRef => ({
    uri: `test-only-memory://${id}/record/${label}`, sha256: sha256(bytes), namespace: "test-only",
    name: label, uid: `${id}:${label}`, resourceVersion: revision,
  });
  let anchor = recordFor("previous-receipt", canonical({ kind: "test-only-previous-receipt", storage }), "6");
  const newAnchor = recordFor("new-volume-seal", canonical({ kind: "test-only-new-volume-seal", storage }), "1");
  let granted: GenerationRef = { acquisitionId: `${id}:old-acquisition`, receiver: oldReceiver,
    engine: oldEngine, applicationGeneration: "9007199254740992", ticketId: `${id}:old-node-ticket` };
  let volume = scenario.volume ?? "required";
  let actualPredecessors: readonly GenerationRef[] = volume === "required" ? [copy(granted)] : [];
  // Includes an ungranted/failed candidate, deliberately not just GenerationRef receivers.
  let actualPodUIDs: readonly string[] = volume === "required" ? [oldReceiver.podUID, `${id}:failed-candidate-pod`] : [];
  const historicalNodeNames = new Set<string>();
  const retiredCandidatePodUIDs: string[] = [];
  const completedOperations = new Map<string, Operation>();
  const supersededOperations = new Set<string>();
  let actualWorkloadTaskIds: readonly string[] = volume === "required" ? [`${id}:old-task`, `${id}:old-child-task`] : [];
  let enumeratedWorkloadTaskIds = copy(actualWorkloadTaskIds);
  const enumerationFor = (): EvidenceRef => retainEvidence("fabric.b1.inventory", {
    storage, operationId, acquisitionId, since: volume === "required" ? anchor : newAnchor,
    generations: actualPredecessors, podUIDs: actualPodUIDs, workloadTasks: enumeratedWorkloadTaskIds,
    quarantinedNodeNames: [...historicalNodeNames, oldReceiver.nodeName].sort(), retiredCandidatePodUIDs,
    oldReplicas: volume === "required" ? [`${id}:old-replica-1`, `${id}:old-replica-2`] : [],
  });
  let enumeration = enumerationFor();
  let restartRef = retainEvidence("test-only.restart-observation", { storage, operationId });
  let killRef = retainEvidence("test-only.kill-observation", { storage, operationId });
  const plan = retainEvidence("fabric.b1.qualification", { profile: "b1-v1-observed", state: "candidate-test",
    limitation: "in-memory fixture only; no installed platform or real verifier" });
  const assurance: Assurance = { profile: "b1-v1-observed", staleDriverOperations: "not-prevented",
    lateBlockWrites: "not-guaranteed", qualification: { state: "candidate-test", plan } };
  const facts: Record<B1Fact, boolean> = {
    inventoryComplete: false, restartDisabled: false, killIssued: false, workloadsExited: false,
    oldPathRetired: false, oldReplicasExited: false, oldIoDrained: false, driverOutcomeKnown: true,
    nodeReachable: true, coverageContinuous: true, newReplicasAfterCut: true,
  };
  const teardown: Record<B1TeardownItem, boolean> = { T1: false, T2: false, T3: false, T4: false,
    T5: false, T6: false, T7: false, T8: false };
  for (const key of factKeys) if (scenario.facts?.[key] !== undefined) facts[key] = scenario.facts[key];
  for (const key of teardownItems) if (scenario.teardown?.[key] !== undefined) teardown[key] = scenario.teardown[key];
  let revision = BigInt(reservationRevision), operation: Operation | undefined;
  let attachGrant: B1Observation["attachGrant"] = null;
  let actualAttachment: B1Observation["actualAttachment"] = volume === "required" ? "predecessor" : "none";
  let receiverScheduled = false, mounted: ModelMount | undefined, contained = false;
  let candidateAttachSubmitted = false, failNextAttach = false;
  let receiverReceipt: ReceiverReceipt | null = null, receiverReceiptPayloadBytes: string | null = null;
  let custodyValue = scenario.custodyValue ?? "9007199254740993";
  let applicationBytes = "test-only acknowledged prefix\n", loseAck = false, closed = false;
  const progress = (): void => { revision += 1n; };
  function fail(code: B1ErrorCode, disposition: "refused" | "held", message: string): never {
    throw new ModelFailure(code, disposition, message, {
      ...(operation ? { operationId: operation.operationId,
        durableState: recordFor("operation", canonical({ ...operation, receipt: operation.receipt ?? null,
          payloadBytes: operation.payloadBytes ?? null }), revision.toString()) } : {}),
      evidence: [enumeration],
    });
  }
  const assertLive = (): void => {
    if (closed) fail("B1_OBSERVER_UNAVAILABLE", "held", "Test-only harness handle is closed");
  };
  const policy = (profile: string): void => {
    if (profile === "r3-storage-fence") fail("B1_POLICY_UNSATISFIED", "refused", "Observed B cannot supply A's execution fence/barrier");
    if (profile !== "b1-v1-observed") fail("B1_UNSUPPORTED_PROFILE", "refused", "Unknown assurance profile");
  };
  const canonicalCounter = (value: string): bigint => {
    if (!/^(0|[1-9][0-9]*)$/.test(value) || value.trim() !== value) {
      fail("B1_GENERATION_INVALID", "refused", "Counter must be canonical nonnegative decimal");
    }
    return BigInt(value);
  };
  const inventory = (): PredecessorInventory => ({
    since: volume === "required" ? { kind: "receipt", record: copy(anchor) } : { kind: "new-volume-seal", record: copy(newAnchor) },
    enumeration: copy(enumeration), generations: copy(actualPredecessors), podUIDs: copy(actualPodUIDs),
    restartDisabled: copy(restartRef), killIssued: copy(killRef),
  });
  const assertInventory = (req: RevokeGenerationRequest): void => {
    const expected = inventory();
    if (!facts.inventoryComplete || !same(req.predecessor, expected)) {
      fail("B1_PREDECESSOR_SET_INCOMPLETE", "held", "Request is not the independently retained complete predecessor inventory");
    }
    // A new task cannot be covered by editing a boolean or the hashed request (§8.1.5).
    if (!same(actualWorkloadTaskIds, enumeratedWorkloadTaskIds)) {
      fail("B1_PREDECESSOR_SET_INCOMPLETE", "held", "New task invalidates immutable coverage; drain and transfer the held slot");
    }
    if (!actualPodUIDs.length && volume !== "sealed-new") {
      fail("B1_PREDECESSOR_SET_INCOMPLETE", "held", "An empty inventory requires a fixture-provisioned sealed NEW anchor");
    }
  };
  const revoke = async (req: RevokeGenerationRequest): Promise<RevokeReceipt> => {
    assertLive();
    if (req.version !== 1 || !req.operationId.trim() || !Number.isFinite(req.deadlineUnixMs)
      || !Number.isSafeInteger(req.deadlineUnixMs) || req.deadlineUnixMs < 0) {
      fail("B1_INVALID_ARGUMENT", "refused", "Invalid revoke version, operation or finite absolute deadline");
    }
    const bytes = semantics(req);
    if (req.requestSha256 !== sha256(bytes)) fail("B1_INVALID_ARGUMENT", "refused", "Semantic request digest mismatch");
    const historical = completedOperations.get(req.operationId);
    if (historical?.receipt) {
      if (historical.semanticPayloadBytes !== bytes || historical.requestSha256 !== req.requestSha256) {
        fail("B1_OPERATION_CONFLICT", "refused", "Historical replay changed semantic bytes");
      }
      return copy(historical.receipt);
    }
    if (supersededOperations.has(req.operationId)) fail("B1_STALE_GENERATION", "refused", "Uncommitted operation was terminally superseded under the held slot");
    if (operation) {
      if (operation.operationId !== req.operationId) {
        fail(operation.state === "committed" ? "B1_STALE_GENERATION" : "B1_OPERATION_CONFLICT", "refused", "Volume lifecycle slot remains owned by the original operation");
      }
      if (operation.semanticPayloadBytes !== bytes || operation.requestSha256 !== req.requestSha256) {
        fail("B1_OPERATION_CONFLICT", "refused", "Changed semantic members under retained operation identity");
      }
      // Historical authenticated readback precedes invocation deadline/fresh observations.
      if (operation.receipt) return copy(operation.receipt);
    } else {
      policy(req.requiredProfile); // No reservation/progress/evidence write before stronger-policy refusal.
      if (!same(req.storage, storage)) fail("B1_STORAGE_IDENTITY_MISMATCH", "refused", "Not the model's actual storage identity");
      if (req.reservedReceiver.nodeUID === oldReceiver.nodeUID || req.reservedReceiver.nodeName === oldReceiver.nodeName) {
        fail("B1_SAME_NODE_REUSE_UNQUALIFIED", "refused", "Immediate old node/ticket reuse is not qualified for B");
      }
      // Permanent per-volume/per-epoch keys: not just the immediate predecessor (§4.1).
      if (historicalNodeNames.has(req.reservedReceiver.nodeName)) {
        fail("B1_SAME_NODE_REUSE_UNQUALIFIED",
          "refused", "Historical or failed-candidate node-name key remains quarantined");
      }
      if (req.operationId !== operationId || req.expectedControllerRevision !== reservationRevision) {
        fail("B1_STALE_GENERATION", "refused", "Request does not identify the independently reserved S1 revision");
      }
      if (!same(req.reservedReceiver, receiver) || req.reservedAcquisitionId !== acquisitionId) {
        fail("B1_NOT_AUTHORIZED", "refused", "Caller cannot replace the fixture's reserved receiver/acquisition");
      }
      operation = { operationId, requestSha256: req.requestSha256, semanticPayloadBytes: bytes,
        reservationRevision, state: "held", restartRevocationPersisted: false, killSubmitted: false,
        retirementRequested: false, receipt: undefined, payloadBytes: undefined };
      progress();
    }
    policy(req.requiredProfile);
    if (Date.now() >= req.deadlineUnixMs) fail("B1_DEADLINE_HELD", "held", "Deadline retains operation and successor gate");
    assertInventory(req);
    if (!facts.nodeReachable) fail("B1_NODE_UNREACHABLE", "held", "Independent node observation is unavailable");
    if (!operation.restartRevocationPersisted) { operation.restartRevocationPersisted = true; progress(); }
    if (!facts.restartDisabled) fail("B1_RESTART_NOT_DISABLED", "held", "Caller restart reference cannot create observed restart suppression");
    if (!operation.killSubmitted) { operation.killSubmitted = true; progress(); }
    if (!facts.killIssued) fail("B1_KILL_NOT_CONFIRMED", "held", "Independent pinned-containment kill acknowledgement missing");
    if (!operation.retirementRequested) { operation.retirementRequested = true; progress(); }
    if (!facts.driverOutcomeKnown) fail("B1_DRIVER_OUTCOME_UNKNOWN", "held", "Submitted driver work has unknown terminal outcome");
    if (!facts.oldPathRetired || !facts.oldReplicasExited || !facts.oldIoDrained) {
      fail("B1_TEARDOWN_INCOMPLETE", "held", "Old initiator/replica/I-O facts are not retired independently");
    }
    if (!facts.workloadsExited) fail("B1_TASKS_STILL_ALIVE", "held", "Kill submission is not actual workload/descendant exit");
    if (!facts.coverageContinuous || teardownItems.some(item => !teardown[item])) {
      fail("B1_TEARDOWN_INCOMPLETE", "held", "T1–T8 and continuous coverage are conjunctive");
    }
    const cut = (revision + 1n).toString();
    const teardownRefs = teardownItems.map(item => retainEvidence("fabric.b1.teardown", {
      item, storage, operationId, acquisitionId, cut, retiredGenerations: actualPredecessors,
      exitedPodUIDs: actualPodUIDs, oldReplicasExited: facts.oldReplicasExited,
      oldPathRetired: facts.oldPathRetired, oldIoDrained: facts.oldIoDrained,
    }));
    const payload: Omit<RevokeReceipt, "record"> = { version: 1, kind: "fabric.b1.revoke", operationId,
      requestSha256: req.requestSha256, storage: copy(storage), reservedReceiver: copy(receiver),
      reservedAcquisitionId: acquisitionId, revoked: copy(actualPredecessors), exitedPodUIDs: copy(actualPodUIDs),
      teardown: teardownRefs, assurance: copy(assurance) };
    const payloadBytes = canonical(payload), record = recordFor("revoke-receipt", payloadBytes, cut);
    operation.receipt = { ...payload, record }; operation.payloadBytes = payloadBytes; operation.state = "committed";
    attachGrant = { receiver: copy(receiver), acquisitionId, revokeReceiptHash: record.sha256 };
    actualAttachment = "none"; progress();
    if (loseAck) { loseAck = false; fail("B1_DRIVER_OUTCOME_UNKNOWN", "held", "Test-only acknowledgement loss AFTER receipt/grant commit"); }
    return copy(operation.receipt);
  };
  const retirementStillComplete = (): boolean => factKeys.every(key => key === "newReplicasAfterCut" || facts[key])
    && teardownItems.every(item => teardown[item]);
  const fdOnMount = (fd: ModelFd, m: ModelMount): boolean => fd.mountId === m.actual.mountId
    && fd.namespaceInode === m.actual.mountNamespaceInode && fd.deviceLifetimeId === m.actual.deviceLifetimeId
    && fd.filesystemUUID === m.storage.filesystemUUID && fd.inode.length > 0;
  const readMount = async (req: CurrentMountGenerationRequest): Promise<MountGeneration> => {
    assertLive(); policy(req.requiredProfile);
    if (req.version !== 1 || !req.challengeNonce.trim() || !Number.isSafeInteger(req.deadlineUnixMs) || req.deadlineUnixMs < 0) {
      fail("B1_INVALID_ARGUMENT", "refused", "Version, fresh caller challenge and finite absolute deadline are required");
    }
    if (Date.now() >= req.deadlineUnixMs) fail("B1_DEADLINE_HELD", "held", "Mount observation deadline expired");
    if (req.target.bootstrapHandle !== target.bootstrapHandle) fail("B1_NOT_AUTHORIZED", "refused", "Unknown authenticated helper handle");
    if (!same(req.target, target)) fail("B1_MOUNT_NOT_FOUND", "refused", "Target root and both metadata roles must match fixture enrollment");
    if (contained) fail("B1_MOUNT_CHANGED", "refused", "Raw/unexpected lifecycle mutation was contained, not prevented by B");
    const m = mounted;
    if (!m || actualAttachment !== "receiver") fail("B1_MOUNT_NOT_FOUND", "refused", "No actual receiver mount exists");
    if (!m.observerAvailable) fail("B1_OBSERVER_UNAVAILABLE", "held", "Independent observer unavailable");
    if (!m.observerFresh) fail("B1_MOUNT_LINEAGE_UNKNOWN", "held", "Stale observation has no continuous fresh lineage");
    if (!same(m.storage, storage)) fail("B1_STORAGE_IDENTITY_MISMATCH", "refused", "Actual mounted storage differs from enrolled storage");
    if (!same(m.self, receiver)) fail("B1_SELF_IDENTITY_MISMATCH", "refused", "Actual peer self differs from the reserved receiver");
    if (!same(m.target, target) || m.actual.bootstrapExecutionId !== bootstrapExecutionId) {
      fail("B1_SELF_IDENTITY_MISMATCH", "refused", "Actual root/bootstrap execution differs from pinned enrollment");
    }
    if (!m.lineageKnown || !attachGrant || !receiverReceipt || m.acquisitionId !== attachGrant.acquisitionId
      || !same(m.attachObservation, receiverReceipt.attachObservation)
      || receiverReceipt.bootstrapExecutionId !== m.actual.bootstrapExecutionId) {
      fail("B1_MOUNT_LINEAGE_UNKNOWN", "held", "Missing actual attach/acquisition/receiver-receipt lineage");
    }
    if (!retirementStillComplete()) fail("B1_UNEXPECTED_ATTACHMENT", "held", "Independent retirement/coverage facts changed after S6");
    if (!same(m.engine, engine) || m.actual.deviceLifetimeId !== `${id}:receiver-device-lifetime`) {
      fail("B1_MOUNT_CHANGED", "refused", "Recycled device name or changed engine does not preserve incarnation");
    }
    if (!m.replicas.length || m.replicas.some(replica => !replica.startedAfterCut)) {
      fail("B1_MOUNT_LINEAGE_UNKNOWN", "held", "Actual successor connections include an old/pre-cut replica");
    }
    if (!m.lock || !m.generation || !fdOnMount(m.lock, m) || !fdOnMount(m.generation, m)
      || m.lock.inode === m.generation.inode) {
      fail("B1_METADATA_OFF_VOLUME", "refused", "Each independently opened metadata FD must bind to the exact root mount");
    }
    const value = canonicalCounter(custodyValue).toString();
    const custody = { value, acquisitionId, recordSha256: sha256(canonical({ storage, acquisitionId, value })),
      lockInode: m.lock.inode, bothFilesOnBoundMount: true as const };
    // Challenge-specific evidence lives only in helper reply memory; no storage/topology mutation.
    const observationPayload = { version: 1, kind: "fabric.b1.mount-observation", trustDomain: "test-only",
      data: { challengeNonce: req.challengeNonce, storage: m.storage, self: m.self, acquisitionId: m.acquisitionId,
        actualMount: m.actual, lockFd: m.lock, generationFd: m.generation, custody,
        attachObservation: m.attachObservation, replicas: m.replicas, nativeEpoch: { kind: "absent" } } };
    const observationBytes = canonical(observationPayload);
    const observation = { uri: `test-only-memory://${id}/mount/${req.challengeNonce}`, sha256: sha256(observationBytes) };
    if (!mountReplies.has(observation.uri) && mountReplies.size >= 128) {
      fail("B1_OBSERVER_UNAVAILABLE", "held", "Bounded test-only helper reply memory is full");
    }
    const prior = mountReplies.get(observation.uri);
    if (prior && !same(prior.ref, observation)) fail("B1_INVALID_ARGUMENT", "refused", "Caller must use a fresh challenge for changed observations");
    mountReplies.set(observation.uri, { ref: observation, payload: observationPayload });
    return { version: 1, challengeNonce: req.challengeNonce, storage: copy(m.storage), self: copy(m.self),
      acquisitionId: m.acquisitionId, engine: copy(m.engine), actualMount: copy(m.actual), custody,
      assurance: copy(assurance), observation, attachObservation: copy(m.attachObservation) };
  };
  // Per-handle liveness is separate from retained model state; no global store/registry.
  let invalidateHandle: () => void = () => {};
  const makePorts = (): B1StoragePorts => {
    let valid = true;
    invalidateHandle = () => { valid = false; };
    const current = (): void => {
      assertLive(); if (!valid) fail("B1_OBSERVER_UNAVAILABLE", "held", "Superseded test-only client handle");
    };
    return { async revokeGeneration(req) { current(); return revoke(copy(req)); },
      async currentMountGeneration(req) { current(); return readMount(copy(req)); } };
  };
  let ports = makePorts();
  const attach = (): void => {
    assertLive();
    if (!operation?.receipt || !attachGrant) fail("B1_TEARDOWN_INCOMPLETE", "held", "Successor remains scheduling-gated before committed S6 grant");
    if (contained) fail("B1_STALE_GENERATION", "refused", "Historical receipt/grant cannot authorize another attach after containment");
    if (!retirementStillComplete()) fail("B1_UNEXPECTED_ATTACHMENT", "held", "Independent retirement/coverage facts changed after S6");
    if (mounted) return;
    if (!facts.newReplicasAfterCut) fail("B1_MOUNT_LINEAGE_UNKNOWN", "held", "Successor cannot connect old/pre-cut replica incarnations");
    candidateAttachSubmitted = true;
    if (failNextAttach) {
      failNextAttach = false; receiverScheduled = true; facts.coverageContinuous = false; progress();
      fail("B1_DRIVER_OUTCOME_UNKNOWN", "held", "TEST-ONLY submitted S7 attach failed before engine/mount; positively reconcile before slot transfer");
    }
    const actual: MountGeneration["actualMount"] = { bootstrapExecutionId, containerId: `${id}:receiver-container`,
      mountNamespaceInode: "4101", mountId: "5101", deviceMajorMinor: "8:32", deviceLifetimeId: `${id}:receiver-device-lifetime`,
      iqn: `${id}:iqn`, portal: "192.0.2.10:3260", lun: "1", backingSocketInode: "6101",
      enginePidStartIdentity: `${id}:receiver-pid-start`, mountPinHandle: `${id}:mount-pin`,
      observerId: `${id}:observer`, observerSequence: "12" };
    const fd = (role: "lock" | "generation", inode: string): ModelFd => ({ role, inode,
      mountId: actual.mountId, namespaceInode: actual.mountNamespaceInode,
      deviceLifetimeId: actual.deviceLifetimeId, filesystemUUID: storage.filesystemUUID });
    const replicas = [{ id: `${id}:new-replica-1`, startedAfterCut: true }, { id: `${id}:new-replica-2`, startedAfterCut: true }];
    const attachObservation = retainEvidence("fabric.b1.attach-observation", { storage, receiver, acquisitionId,
      revokeReceiptHash: operation.receipt.record.sha256, cut: operation.receipt.record.resourceVersion,
      engine, actual, replicas, nativeEpoch: { kind: "absent" } });
    mounted = { storage: copy(storage), self: copy(receiver), target: copy(target), acquisitionId,
      engine: copy(engine), actual, lock: fd("lock", "7101"), generation: fd("generation", "7102"),
      lineageKnown: true, observerAvailable: true, observerFresh: true, replicas, attachObservation };
    const receiverPayload: Omit<ReceiverReceipt, "record"> = { version: 1, kind: "fabric.b1.receiver",
      storage: copy(mounted.storage), receiver: copy(mounted.self), bootstrapExecutionId: mounted.actual.bootstrapExecutionId,
      acquisitionId: mounted.acquisitionId, engine: copy(mounted.engine), revokeReceipt: copy(operation.receipt.record),
      attachObservation: copy(mounted.attachObservation), assurance: copy(assurance) };
    receiverReceiptPayloadBytes = canonical(receiverPayload);
    receiverReceipt = { ...receiverPayload, record: recordFor("receiver-receipt", receiverReceiptPayloadBytes, (revision + 1n).toString()) };
    actualAttachment = "receiver"; receiverScheduled = true; progress();
  };
  const mountFault = (fault: B1MountFault): void => {
    assertLive(); const m = mounted;
    if (!m) fail("B1_MOUNT_NOT_FOUND", "refused", "Mount faults need an actual mounted fixture");
    switch (fault) {
      case "wrong-self": m.self = { ...m.self, podUID: `${id}:unrelated-pod` }; break;
      case "wrong-storage": m.storage = { ...m.storage, pvcUID: `${id}:unrelated-pvc` }; break;
      case "wrong-bootstrap": m.actual = { ...m.actual, bootstrapExecutionId: `${id}:restarted-bootstrap` }; break;
      case "wrong-root": m.target = { ...m.target, protectedRoot: "/test-only/unrelated-root" }; break;
      case "lineage-unknown": m.lineageKnown = false; break;
      case "lock-off-volume": if (m.lock) m.lock.mountId = "container-root-mount"; break;
      case "generation-off-volume": if (m.generation) m.generation.deviceLifetimeId = "container-root-device"; break;
      case "lock-fd-missing": m.lock = undefined; break;
      case "generation-fd-missing": m.generation = undefined; break;
      case "stale-observation": m.observerFresh = false; break;
      case "observer-unavailable": m.observerAvailable = false; break;
      case "engine-changed": m.engine = engineFor("unplanned", receiver.nodeUID); break;
      case "device-reused": m.actual = { ...m.actual, deviceLifetimeId: `${id}:reused-dev-t-new-lifetime` }; break;
      case "old-replica": m.replicas = [{ id: `${id}:old-replica-1`, startedAfterCut: false }]; break;
      // §9 #8: receipt can name an earlier bootstrap even while the actual/pinned
      // helper execution is current. Independent of the wrong-bootstrap topology fault.
      case "stale-receiver-receipt": {
        if (receiverReceipt) {
          const { record: _record, ...prior } = receiverReceipt;
          const stale = { ...prior, bootstrapExecutionId: `${id}:old-bootstrap` };
          receiverReceiptPayloadBytes = canonical(stale);
          receiverReceipt = { ...stale, record: recordFor("stale-receiver-receipt", receiverReceiptPayloadBytes, revision.toString()) };
        }
        break;
      }
      case "stale-acquisition": m.acquisitionId = granted.acquisitionId; break;
      case "foreign-acquisition": m.acquisitionId = `${id}:foreign-acquisition`; break;
      case "attach-observation-mismatch": m.attachObservation = retainEvidence("fabric.b1.attach-observation", {
        storage, receiver, acquisitionId, wrongCut: true,
      }); break;
      case "lock-wrong-namespace": if (m.lock) m.lock.namespaceInode = "foreign-namespace"; break;
      case "generation-wrong-namespace": if (m.generation) m.generation.namespaceInode = "foreign-namespace"; break;
      case "lock-wrong-filesystem": if (m.lock) m.lock.filesystemUUID = "foreign-filesystem"; break;
      case "generation-wrong-filesystem": if (m.generation) m.generation.filesystemUUID = "foreign-filesystem"; break;
      case "same-metadata-inode": if (m.lock && m.generation) m.generation.inode = m.lock.inode; break;
    }
  };
  // TEST-ONLY controller continuation of the design's existing state table, not a new B1 port.
  const nextReservation = (next: ReceiverIdentity, failed: boolean): void => {
    assertLive();
    if (!operation || contained || (failed ? mounted !== undefined || !retirementStillComplete()
      : !mounted || !receiverReceipt || !operation.receipt)) {
      fail("B1_OPERATION_CONFLICT", "refused", "Slot transfer requires an attached receiver or positively drained failed candidate");
    }
    const previous = operation;
    if (previous.receipt) completedOperations.set(previous.operationId, copy(previous));
    else supersededOperations.add(previous.operationId);
    if (failed) {
      // No fictitious engine/GenerationRef for a candidate that never mounted (§8.2).
      // A merely gated S1 candidate has not used a CSI key. A submitted S7 candidate has.
      if (candidateAttachSubmitted) historicalNodeNames.add(receiver.nodeName);
      retiredCandidatePodUIDs.push(receiver.podUID);
      actualPodUIDs = [...actualPodUIDs, receiver.podUID];
      if (previous.receipt) anchor = copy(previous.receipt.record);
    } else {
      historicalNodeNames.add(oldReceiver.nodeName);
      oldReceiver = copy(receiver); oldEngine = copy(engine);
      granted = { acquisitionId, receiver: copy(receiver), engine: copy(engine), applicationGeneration: custodyValue,
        ticketId: `${id}:receiver-node-ticket` };
      actualPredecessors = [copy(granted)]; actualPodUIDs = [receiver.podUID];
      actualWorkloadTaskIds = [`${id}:receiver-task`, `${id}:receiver-child-task`];
      // Above preconditions establish this controller-owned post-attach receipt.
      if (!receiverReceipt) fail("B1_OPERATION_CONFLICT", "refused", "Attached predecessor needs retained receiver evidence");
      anchor = copy(receiverReceipt.record);
    }
    // One CAS transfers the continuously held slot. Older operations remain readback/tombstones.
    progress(); reservationRevision = revision.toString();
    id = `${id}:next`; receiver = copy(next);
    acquisitionId = `${id}:acquisition`; operationId = `${id}:operation`;
    engine = engineFor("receiver", receiver.nodeUID);
    target = { ...target, bootstrapHandle: `${id}:authenticated-bootstrap-handle` };
    bootstrapExecutionId = `${id}:bootstrap-execution`;
    operation = undefined; attachGrant = null; mounted = undefined; receiverReceipt = null; receiverReceiptPayloadBytes = null;
    receiverScheduled = false; candidateAttachSubmitted = false; failNextAttach = false;
    actualAttachment = actualPredecessors.length ? "predecessor" : "none";
    volume = "required";
    for (const key of factKeys) facts[key] = ["driverOutcomeKnown", "nodeReachable", "coverageContinuous", "newReplicasAfterCut"].includes(key);
    for (const item of teardownItems) teardown[item] = false;
    enumeratedWorkloadTaskIds = copy(actualWorkloadTaskIds); enumeration = enumerationFor();
    restartRef = retainEvidence("test-only.restart-observation", { storage, operationId });
    killRef = retainEvidence("test-only.kill-observation", { storage, operationId });
  };
  const observe = (): B1Observation => {
    const operationView = operation ? { operationId: operation.operationId, requestSha256: operation.requestSha256,
      semanticPayloadBytes: operation.semanticPayloadBytes, reservationRevision: operation.reservationRevision,
      state: operation.state, restartRevocationPersisted: operation.restartRevocationPersisted,
      killSubmitted: operation.killSubmitted, retirementRequested: operation.retirementRequested } : null;
    const retainedReceipt = operation?.receipt && operation.payloadBytes
      ? { payloadBytes: operation.payloadBytes, record: operation.receipt.record } : null;
    const evidenceView = [...evidence.values()].map(({ ref, payloadBytes }) => ({ ref, payloadBytes }));
    const view = { profile: "b1-v1-observed" as const, trustDomain: "test-only" as const,
      controllerRevision: revision.toString(), facts, teardown, actualPredecessors, actualPodUIDs, actualWorkloadTaskIds,
      quarantinedNodeNames: [...new Set([...historicalNodeNames, oldReceiver.nodeName])].sort(), retiredCandidatePodUIDs,
      reservation: { operationId, acquisitionId, receiver, revision: reservationRevision },
      operation: operationView, attachGrant, retainedReceipt, receiverReceipt, receiverReceiptPayloadBytes,
      evidence: evidenceView, actualAttachment,
      receiverScheduled, receiverMounted: mounted !== undefined, candidateAttachSubmitted, contained, custodyValue,
      custodyRecordPayloadBytes: canonical({ storage, acquisitionId, value: custodyValue }), applicationBytes };
    return copy({ ...view, storageSnapshot: canonical({ ...view, mounted: mounted ? {
      ...mounted, lock: mounted.lock ?? null, generation: mounted.generation ?? null,
    } : null, volume }) });
  };
  return {
    get ports() { return ports; },
    revokeRequest() {
      assertLive(); const req: RevokeGenerationRequest = { version: 1, operationId, requestSha256: "",
        storage: copy(storage), expectedControllerRevision: reservationRevision, predecessor: inventory(),
        reservedReceiver: copy(receiver), reservedAcquisitionId: acquisitionId, requiredProfile: "b1-v1-observed",
        deadlineUnixMs: Date.now() + 60_000 };
      return { ...req, requestSha256: sha256(semantics(req)) };
    },
    mountRequest() { assertLive(); return { version: 1, target: copy(target), challengeNonce: `test-only-challenge-${randomUUID()}`,
      requiredProfile: "b1-v1-observed", deadlineUnixMs: Date.now() + 60_000 }; },
    observe,
    setFact(key, value) { assertLive(); facts[key] = value; },
    setTeardown(item, value) { assertLive(); teardown[item] = value; },
    discoverWorkloadTask() {
      assertLive(); actualWorkloadTaskIds = [...actualWorkloadTaskIds, `${id}:new-task-${actualWorkloadTaskIds.length}`];
      facts.inventoryComplete = false; // Immutable enumeration/semantic request is intentionally unchanged.
    },
    reserveNextReceiver(next) { nextReservation(next, false); },
    retireFailedCandidateAndReserve(next) { nextReservation(next, true); },
    completeRetirement() {
      assertLive();
      for (const key of factKeys) facts[key] = true;
      for (const key of teardownItems) teardown[key] = true;
      actualAttachment = "none";
    },
    provisionNewVolume(sealed) {
      assertLive(); if (operation) fail("B1_OPERATION_CONFLICT", "refused", "NEW fixture provisioning cannot replace retained REQUIRED/operation history");
      volume = sealed ? "sealed-new" : "unsealed-new"; actualPredecessors = []; actualPodUIDs = [];
      actualWorkloadTaskIds = []; enumeratedWorkloadTaskIds = [];
      enumeration = enumerationFor(); actualAttachment = "none";
    },
    attachReservedReceiver: attach,
    failNextReceiverAttach() { assertLive(); failNextAttach = true; },
    setMountFault: mountFault,
    setCustodyValue(value) { assertLive(); custodyValue = value; },
    incrementCustody() { assertLive(); custodyValue = (canonicalCounter(custodyValue) + 1n).toString(); },
    executeRawStaleAction(action) {
      assertLive(); const m = mounted;
      if (!m || !attachGrant) fail("B1_MOUNT_NOT_FOUND", "refused", "Raw replay control requires a current receiver");
      // Deliberately do NOT check an invented driver epoch: stock B cannot enforce one.
      if (action === "detach") { actualAttachment = "none"; mounted = undefined; }
      if (action === "reattach-old") { actualAttachment = "predecessor"; m.engine = copy(oldEngine);
        m.actual = { ...m.actual, deviceLifetimeId: `${id}:old-device-lifetime` }; }
      if (action === "old-write") applicationBytes += "test-only OLD-ORIGIN mutation AFTER S6\n";
      contained = true; progress();
    },
    loseNextRevokeAck() { assertLive(); loseAck = true; },
    reopen() { assertLive(); invalidateHandle(); ports = makePorts(); },
    readEvidence(ref) {
      const found = evidence.get(ref.uri) ?? mountReplies.get(ref.uri);
      if (!found || !same(ref, found.ref)) fail("B1_NOT_AUTHORIZED", "refused", "Not an exact retained test-only evidence reference");
      return copy(found.payload);
    },
    close() { invalidateHandle(); closed = true; },
  };
}
