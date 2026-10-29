import type {
  RecoveryDurabilityAdapter, RecoveryDurableAppendRequest, RecoveryDurableAppendResult,
  RecoveryDurableEntryRequest, RecoveryDurableEntryResult,
  RecoveryNativeEntryLocation, RecoveryNativeFlushReceipt,
  RecoveryNativeEntryRegistrationRequest, RecoveryNativeEntryRegistrationResult,
} from "../../../src/recovery/l7-interfaces.js";

/** Exact B6 public ABI, re-exported from production without a test-side mirror. */
export type {
  RecoveryDurabilityAdapter, RecoveryDurableAppendRequest, RecoveryDurableAppendResult,
  RecoveryDurableEntryRequest, RecoveryDurableEntryResult,
  RecoveryNativeEntryLocation, RecoveryNativeFlushReceipt,
  RecoveryNativeEntryRegistrationRequest, RecoveryNativeEntryRegistrationResult,
};

export type B6RecoveryState = "unknown" | "unavailable" | "recovered";
export type B6ReceiptKind = RecoveryNativeFlushReceipt["kind"];

export interface B6CommitGate {
  /** Resolves when the next append reaches its pre-commit boundary. */
  readonly entered: Promise<void>;
  /** Releases the bounded test barrier. Idempotent; close() also releases it. */
  release(): void;
}

export interface B6NativeDescriptorFixtureOptions {
  readonly deliveryId?: string;
  readonly entryId?: string;
  /** JSON text without the serializer's final LF. Defaults to a native-entry fixture. */
  readonly json?: string;
  readonly prefixBytes?: Uint8Array;
  /** Later committed bytes covered by the native flush receipt, after this entry. */
  readonly trailingCommittedBytes?: Uint8Array;
  readonly receiptKind?: B6ReceiptKind;
  readonly deadlineMs?: number;
  /** Build matching bytes/receipt without adding them to the fixture trust catalogue. */
  readonly trusted?: boolean;
}

export interface B6CommittedEntryObservation {
  readonly sessionId: string;
  readonly deliveryId: string;
  readonly recordKind: "control-seen" | "carrier";
  readonly seq: number;
  readonly bytes: Uint8Array;
  /** Original input/lifecycle deadline retained for this tuple, not a retry timestamp. */
  readonly originalDeadlineMs: number;
}

export interface B6RegistrationObservation {
  readonly sessionId: string;
  readonly deliveryId: string;
  readonly entryId: string;
  readonly location: RecoveryNativeEntryLocation;
  readonly bytes: Uint8Array;
  readonly bytesSha256: string;
  readonly flushReceipt: RecoveryNativeFlushReceipt;
}

export interface B6HarnessObservation {
  readonly storeId: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly committed: readonly B6CommittedEntryObservation[];
  readonly registrations: readonly B6RegistrationObservation[];
  readonly nextSeq: number;
  /** B6 never opens, reads, writes or fsyncs Pi's native session file. */
  readonly nativePiFileIo: Readonly<{ opens: number; reads: number; writes: number; fsyncs: number }>;
}

/** Test harness only. These controls are not added to RecoveryDurabilityAdapter. */
export interface B6DurabilityHarness {
  readonly adapter: RecoveryDurabilityAdapter;
  /** Exact REQUIRED binding pinned for this harness and every reopen. */
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly storeId: string;
  readonly deliveryId: string;
  readonly originalInputDeadlineMs: number;

  now(): number;
  setNow(epochMs: number): void;
  advanceNow(milliseconds: number): void;

  /** Seeds exact control-seen bytes and their original deadline; no production setter exists. */
  seedControlSeen(input: {
    readonly deliveryId?: string;
    readonly bytes: Uint8Array;
    readonly originalDeadlineMs: number;
  }): void;

  /**
   * Builds serializer bytes and a native flush receipt, and optionally records fixture-only
   * provenance keyed by exact descriptor+receipt value. Detached copies can repeat trusted
   * evidence; trust is not inferred from JSON/hash alone.
   */
  nativeDescriptorFixture(options?: B6NativeDescriptorFixtureOptions): RecoveryNativeEntryRegistrationRequest;

  /** A successful state reconstructs authority from the fake committed log, not a cache. */
  recover(state?: B6RecoveryState): Promise<void>;
  /** Simulates a new binding to the same exact session file/store identity. */
  reopen(state?: B6RecoveryState): Promise<void>;
  invalidateAvailability(): void;

  /** Holds one append immediately before commit; callers must release in finally/close. */
  gateNextCommit(): B6CommitGate;
  /** Next actual commit is published, then its promise rejects with an unknown-ACK error. */
  loseNextWriteAck(): void;

  observe(): B6HarnessObservation;
  close(): Promise<void>;
}

export interface B6DurabilityHarnessFactory {
  readonly name: string;
  create(): B6DurabilityHarness | Promise<B6DurabilityHarness>;
}
