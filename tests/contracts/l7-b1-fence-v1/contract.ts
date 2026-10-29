export type UID = string;
export type Sha256 = string;
// Runtime decoder requires canonical nonnegative decimal; compare as bigint.
export type Counter = string;
export type FenceProfile = "b1-v1-observed" | "r3-storage-fence";

export interface StorageIdentity {
  readonly clusterUID: UID;
  readonly identityRecordUID: UID;
  readonly sessionId: string;
  readonly pvcUID: UID;
  readonly pvUID: UID;
  readonly csiDriver: "driver.longhorn.io";
  readonly volumeHandle: string;
  readonly filesystemUUID: string;
}
export interface ReceiverIdentity {
  readonly podUID: UID;
  readonly nodeUID: UID;
  readonly nodeName: string; // routing only; not identity
}
export interface EvidenceRef {
  readonly uri: string;
  readonly sha256: Sha256;
}
export interface ControllerRecordRef extends EvidenceRef {
  readonly namespace: string;
  readonly name: string;
  readonly uid: UID;
  readonly resourceVersion: string;
}
export interface EngineIncarnation {
  readonly engineObjectUID: UID;
  readonly engineName: string;
  readonly instanceManagerPodUID: UID;
  readonly processUUID: string;
  readonly nodeUID: UID;
  readonly nodeBootId: string;
}
export interface GenerationRef {
  readonly acquisitionId: string; // Fabric a, never a CSI ticket generation
  readonly receiver: ReceiverIdentity;
  readonly engine: EngineIncarnation;
  // Null only for a candidate that never finished M2; not permission to omit it.
  readonly applicationGeneration: Counter | null;
  readonly ticketId: string;
}
export type QualificationBinding =
  | { readonly state: "candidate-test"; readonly plan: EvidenceRef }
  | { readonly state: "qualified"; readonly report: EvidenceRef;
      readonly platformFingerprint: Sha256 };
export type Assurance =
  | {
      readonly profile: "b1-v1-observed";
      readonly staleDriverOperations: "not-prevented";
      readonly lateBlockWrites: "not-guaranteed";
      readonly qualification: QualificationBinding;
    }
  | {
      readonly profile: "r3-storage-fence";
      readonly staleDriverOperations: "rejected-at-execution";
      readonly lateBlockWrites: "excluded-by-barrier";
      readonly qualification: QualificationBinding;
      readonly driverFenceEpoch: string;
      readonly backendBarrier: EvidenceRef;
    };
export interface PredecessorInventory {
  readonly since:
    | { readonly kind: "receipt"; readonly record: ControllerRecordRef }
    | { readonly kind: "new-volume-seal"; readonly record: ControllerRecordRef };
  readonly enumeration: EvidenceRef; // includes failed/ungranted candidates
  readonly generations: readonly GenerationRef[];
  readonly podUIDs: readonly UID[];
  readonly restartDisabled: EvidenceRef;
  readonly killIssued: EvidenceRef;
}
export interface RevokeGenerationRequest {
  readonly version: 1;
  readonly operationId: string;
  readonly requestSha256: Sha256;
  readonly storage: StorageIdentity;
  readonly expectedControllerRevision: string;
  readonly predecessor: PredecessorInventory;
  readonly reservedReceiver: ReceiverIdentity;
  readonly reservedAcquisitionId: string;
  readonly requiredProfile: FenceProfile;
  readonly deadlineUnixMs: number; // liveness bound, never fence evidence
}
export interface RevokeReceipt {
  readonly version: 1;
  readonly kind: "fabric.b1.revoke";
  readonly operationId: string;
  readonly requestSha256: Sha256;
  readonly storage: StorageIdentity;
  readonly reservedReceiver: ReceiverIdentity;
  readonly reservedAcquisitionId: string;
  readonly revoked: readonly GenerationRef[];
  readonly exitedPodUIDs: readonly UID[];
  readonly teardown: readonly EvidenceRef[]; // complete T1–T8
  readonly assurance: Assurance;
  readonly record: ControllerRecordRef; // immutable exact retained bytes
}
export interface ActualMountTarget {
  // Handle issued/authenticated by the node helper; not a caller PID assertion.
  readonly bootstrapHandle: string;
  readonly protectedRoot: string;
  readonly lockRelativePath: string;
  readonly generationRelativePath: string;
}
export interface CurrentMountGenerationRequest {
  readonly version: 1;
  readonly target: ActualMountTarget;
  readonly challengeNonce: string;
  readonly requiredProfile: FenceProfile;
  readonly deadlineUnixMs: number;
  // Deliberately no expected engine UUID to echo as the answer.
}
export interface MountGeneration {
  readonly version: 1;
  readonly challengeNonce: string;
  readonly storage: StorageIdentity;
  readonly self: ReceiverIdentity;
  readonly acquisitionId: string;
  readonly engine: EngineIncarnation;
  readonly actualMount: {
    readonly bootstrapExecutionId: string;
    readonly containerId: string;
    readonly mountNamespaceInode: string;
    readonly mountId: string;
    readonly deviceMajorMinor: string;
    readonly deviceLifetimeId: string; // helper-observed, not just dev_t
    readonly iqn: string;
    readonly portal: string;
    readonly lun: string;
    readonly backingSocketInode: string;
    readonly enginePidStartIdentity: string;
    readonly mountPinHandle: string;
    readonly observerId: string;
    readonly observerSequence: string;
  };
  readonly custody: {
    readonly value: Counter;
    readonly acquisitionId: string;
    readonly recordSha256: Sha256;
    readonly lockInode: string;
    readonly bothFilesOnBoundMount: true;
  };
  readonly assurance: Assurance;
  readonly observation: EvidenceRef;
  readonly attachObservation: EvidenceRef;
}
export interface B1StoragePorts {
  revokeGeneration(req: RevokeGenerationRequest): Promise<RevokeReceipt>;
  currentMountGeneration(req: CurrentMountGenerationRequest): Promise<MountGeneration>;
}
// Produced by the controller after attach, not by the workload or revoke port.
export interface ReceiverReceipt {
  readonly version: 1;
  readonly kind: "fabric.b1.receiver";
  readonly storage: StorageIdentity;
  readonly receiver: ReceiverIdentity;
  readonly bootstrapExecutionId: string;
  readonly acquisitionId: string;
  readonly engine: EngineIncarnation;
  readonly revokeReceipt: ControllerRecordRef;
  readonly attachObservation: EvidenceRef;
  readonly assurance: Assurance;
  readonly record: ControllerRecordRef;
}
export type B1ErrorCode =
  | "B1_INVALID_ARGUMENT" | "B1_NOT_AUTHORIZED" | "B1_POLICY_UNSATISFIED"
  | "B1_UNSUPPORTED_PROFILE" | "B1_SAME_NODE_REUSE_UNQUALIFIED"
  | "B1_OPERATION_CONFLICT" | "B1_STALE_GENERATION"
  | "B1_PREDECESSOR_SET_INCOMPLETE" | "B1_RESTART_NOT_DISABLED"
  | "B1_KILL_NOT_CONFIRMED" | "B1_TASKS_STILL_ALIVE"
  | "B1_DRIVER_OUTCOME_UNKNOWN" | "B1_TEARDOWN_INCOMPLETE"
  | "B1_UNEXPECTED_ATTACHMENT" | "B1_NODE_UNREACHABLE"
  | "B1_MOUNT_NOT_FOUND" | "B1_MOUNT_CHANGED" | "B1_MOUNT_LINEAGE_UNKNOWN"
  | "B1_STORAGE_IDENTITY_MISMATCH" | "B1_SELF_IDENTITY_MISMATCH"
  | "B1_METADATA_OFF_VOLUME" | "B1_GENERATION_INVALID"
  | "B1_OBSERVER_UNAVAILABLE" | "B1_DEADLINE_HELD";
export interface B1PortFailure extends Error {
  readonly code: B1ErrorCode;
  readonly disposition: "refused" | "held";
  readonly operationId?: string;
  readonly durableState?: ControllerRecordRef;
  readonly evidence: readonly EvidenceRef[];
}
