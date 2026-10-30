import type {
  B1StoragePorts, ControllerRecordRef, CurrentMountGenerationRequest, EvidenceRef,
  GenerationRef, ReceiverIdentity, ReceiverReceipt, RevokeGenerationRequest,
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

export interface B1Harness {
  readonly ports: B1StoragePorts;
  revokeRequest(): RevokeGenerationRequest;
  /** Allocates a fresh CALLER challenge, not an expected-engine assertion. */
  mountRequest(): CurrentMountGenerationRequest;
  observe(): B1Observation;
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

