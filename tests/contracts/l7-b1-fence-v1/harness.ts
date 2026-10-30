import type {
  B1StoragePorts, ControllerRecordRef, CurrentMountGenerationRequest, EvidenceRef,
  GenerationRef, ReceiverIdentity, ReceiverReceipt, RevokeGenerationRequest, StorageIdentity,
  ActualMountTarget, EngineIncarnation, MountGeneration,
} from "./contract.js";

/** TEST CONTROLS, not additional B1 ports or an authority/serialized evidence ABI. */
export type B1TeardownItem = "T1" | "T2" | "T3" | "T4" | "T5" | "T6" | "T7" | "T8";
export type B1Fact =
  | "inventoryComplete" | "restartDisabled" | "killIssued" | "workloadsExited"
  | "oldPathRetired" | "oldReplicasExited" | "oldIoDrained" | "driverOutcomeKnown"
  | "nodeReachable" | "coverageContinuous" | "newReplicasAfterCut";
export type B1MountFault =
  | "wrong-self" | "wrong-storage" | "wrong-bootstrap" | "wrong-root"
  | "lineage-unknown" | "lock-off-volume" | "generation-off-volume"
  | "lock-fd-missing" | "generation-fd-missing" | "stale-observation"
  | "observer-unavailable" | "engine-changed" | "device-reused" | "old-replica"
  | "stale-acquisition" | "foreign-acquisition" | "attach-observation-mismatch"
  | "stale-receiver-receipt"
  | "lock-wrong-namespace" | "generation-wrong-namespace"
  | "lock-wrong-filesystem" | "generation-wrong-filesystem" | "same-metadata-inode";

/** Only fixture provisioning. No option can select or simulate A assurance. */
export interface B1Scenario {
  readonly volume?: "required" | "sealed-new" | "unsealed-new";
  readonly custodyValue?: string;
  readonly facts?: Readonly<Partial<Record<B1Fact, boolean>>>;
  readonly teardown?: Readonly<Partial<Record<B1TeardownItem, boolean>>>;
}

export interface B1Observation {
  readonly profile: "b1-v1-observed";
  readonly trustDomain: "test-only";
  readonly controllerRevision: string;
  readonly facts: Readonly<Record<B1Fact, boolean>>;
  readonly teardown: Readonly<Record<B1TeardownItem, boolean>>;
  readonly actualPredecessors: readonly GenerationRef[];
  readonly actualPodUIDs: readonly string[];
  readonly actualWorkloadTaskIds: readonly string[];
  /** Per-volume node-name keys retained for this operating epoch (§4.1/§9 #4). */
  readonly quarantinedNodeNames: readonly string[];
  readonly retiredCandidatePodUIDs: readonly string[];
  readonly reservation: Readonly<{
    operationId: string; acquisitionId: string; receiver: ReceiverIdentity; revision: string;
  }>;
  readonly operation: Readonly<{
    operationId: string;
    requestSha256: string;
    semanticPayloadBytes: string;
    reservationRevision: string;
    state: "held" | "committed";
    restartRevocationPersisted: boolean;
    killSubmitted: boolean;
    retirementRequested: boolean;
  }> | null;
  readonly attachGrant: Readonly<{
    receiver: ReceiverIdentity;
    acquisitionId: string;
    revokeReceiptHash: string;
  }> | null;
  readonly retainedReceipt: Readonly<{
    payloadBytes: string;
    record: ControllerRecordRef;
  }> | null;
  /** Produced by the fixture controller after actual attach, NEVER by revokeGeneration. */
  readonly receiverReceipt: ReceiverReceipt | null;
  readonly receiverReceiptPayloadBytes: string | null;
  readonly evidence: readonly Readonly<{ ref: EvidenceRef; payloadBytes: string }>[];
  readonly actualAttachment: "predecessor" | "none" | "receiver";
  readonly receiverScheduled: boolean;
  readonly receiverMounted: boolean;
  readonly candidateAttachSubmitted: boolean;
  readonly contained: boolean;
  readonly custodyValue: string;
  /** Independently provisioned fixture bytes, not reconstructed from a port reply. */
  readonly custodyRecordPayloadBytes: string;
  readonly applicationBytes: string;
  /** Exact model storage/topology fingerprint, not an authoritative production read. */
  readonly storageSnapshot: string;
}

/** Fixture-local authenticated observer inputs. These are NOT a proposed production
 * evidence schema or another port. Required members make field enumeration exhaustive;
 * conformance walks every object/array member rather than maintaining a field allow-list. */
export interface B1EvidenceBinding { storage: StorageIdentity; operationId: string }
export interface B1InventoryInput extends B1EvidenceBinding {
  acquisitionId: string;
  since: ControllerRecordRef;
  generations: readonly GenerationRef[];
  podUIDs: readonly string[];
  workloadTasks: readonly string[];
  quarantinedNodeNames: readonly string[];
  retiredCandidatePodUIDs: readonly string[];
  oldReplicas: readonly string[];
}
export interface B1FdInput {
  role: "lock" | "generation";
  mountId: string; namespaceInode: string; deviceLifetimeId: string; filesystemUUID: string; inode: string;
}
export interface B1MountedInput {
  storage: StorageIdentity; self: ReceiverIdentity; target: ActualMountTarget;
  acquisitionId: string; engine: EngineIncarnation; actual: MountGeneration["actualMount"];
  lock: B1FdInput | undefined; generation: B1FdInput | undefined;
  lineageKnown: boolean; observerAvailable: boolean; observerFresh: boolean;
  replicas: readonly { id: string; startedAfterCut: boolean }[];
  attachObservation: EvidenceRef;
}
export interface B1TeardownInput extends B1EvidenceBinding {
  item: B1TeardownItem;
  acquisitionId: string;
  cut: string;
  retiredGenerations: readonly GenerationRef[];
  exitedPodUIDs: readonly string[];
  oldReplicasExited: boolean;
  oldPathRetired: boolean;
  oldIoDrained: boolean;
}
export interface B1FenceInputs {
  inventory: B1InventoryInput;
  restart: B1EvidenceBinding;
  kill: B1EvidenceBinding;
  factBindings: Record<B1Fact, { operationId: string; acquisitionId: string }>;
  teardownBindings: Record<B1TeardownItem, { operationId: string; acquisitionId: string }>;
  workloadTasks: readonly string[];
  /** Read-only B1 data, NOT an M2 high-water authority. Any canonical counter is observable. */
  custodyValue: string;
  teardown: readonly B1TeardownInput[];
  receiverReceipt: ReceiverReceipt | null;
  mount: B1MountedInput | null;
}

export interface B1Harness {
  readonly ports: B1StoragePorts;
  revokeRequest(): RevokeGenerationRequest;
  /** Allocates a fresh CALLER challenge, not an expected-engine assertion. */
  mountRequest(): CurrentMountGenerationRequest;
  observe(): B1Observation;
  /** Detached observer-input snapshot, only for field-level negative controls. */
  fenceInputs(): B1FenceInputs;
  /** Replace ONE observer-input member, not a port result or private assertion.
   * Evidence payloads are retained under a fresh fixture-authenticated reference so
   * the test exercises semantic binding, not merely a broken SHA-256. */
  injectFenceField(path: readonly string[], value: unknown): void;
  setFact(fact: B1Fact, value: boolean): void;
  setTeardown(item: B1TeardownItem, value: boolean): void;
  /** Adds a real model task without rewriting the immutable request's coverage. */
  discoverWorkloadTask(): void;
  /** Controller fixture S0–S3 for another handover, retaining per-volume history. */
  reserveNextReceiver(receiver: ReceiverIdentity): void;
  /** Positive executor drain + terminal Pod retirement + atomic slot transfer.
   * Models a failed candidate's plausibly submitted node key, even without an engine.
   * Requires independently complete retirement; never an operator-safe port override. */
  retireFailedCandidateAndReserve(receiver: ReceiverIdentity): void;
  /** Fixture-only independent observations; no port call fabricates these facts. */
  completeRetirement(): void;
  /** Before the operation only: provision an explicitly NEW empty volume fixture. */
  provisionNewVolume(sealed: boolean): void;
  /** Simulates the controller-owned S7 path, requires the exact committed S6 grant. */
  attachReservedReceiver(): void;
  /** Next S7 actually submits a candidate's raw attach but fails before an engine/mount.
   * Retirement observations must be completed again before terminal slot transfer. */
  failNextReceiverAttach(): void;
  /** One topology/provenance corruption of the already-mounted receiver fixture. */
  setMountFault(fault: B1MountFault): void;
  /** On-volume fixture controls, NEVER performed by either read-only mount call. */
  setCustodyValue(value: string): void;
  incrementCustody(): void;
  /** A real model mutation under B: intentionally NOT execution-fenced. */
  executeRawStaleAction(action: "detach" | "reattach-old" | "old-write"): void;
  /** Next successful commit is visible but its acknowledgement is lost/held. */
  loseNextRevokeAck(): void;
  /** Replaces only the client handle; retains in-memory model state, NOT crash proof. */
  reopen(): void;
  readEvidence(ref: EvidenceRef): unknown;
  close(): void;
}

export interface B1HarnessFactory {
  readonly name: string;
  create(): B1Harness | Promise<B1Harness>;
}

