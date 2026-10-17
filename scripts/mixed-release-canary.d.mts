export interface CanaryOptions {
  new: string;
  old: string[];
  receipt?: string;
  holdMs: number;
  baselineControl: boolean;
}

export interface CapabilityCitation {
  file: string;
  line: number;
  text: string;
  sha256: string;
}

export interface RelayCapability {
  supported: boolean;
  reason?: string;
  evidence: CapabilityCitation[];
}

export interface ReleaseIdentity {
  dir: string;
  sha: string;
  identitySource: string;
  version: string;
  bridgeSha256: string;
  relayCapability: RelayCapability;
}

export interface ReceiptCheck {
  name: string;
  status: "PASS" | "FAIL";
  evidence: unknown;
}

export interface ReceiptEvaluation {
  ok: boolean;
  releaseGate: boolean;
  checks: ReceiptCheck[];
}

export interface AuthorityOptions {
  authoritySnapshot?: true;
  hub: string;
  hubSha: string;
  work: string;
  workSha: string;
  candidate?: string;
  candidateSha?: string;
  /** Candidate hub-only controls remain diagnostic. Live acceptance requires BOTH sides. */
  candidateBothSides?: boolean;
  candidateWork?: string;
  candidateWorkSha?: string;
  liveWriters?: boolean;
  hubWriter?: string;
  hubWriterSha?: string;
  workWriter?: string;
  workWriterSha?: string;
  candidateWorkWriter?: string;
  candidateWorkWriterSha?: string;
  /** SHA-256 of immutable operator-capture-pair (or explicit synthetic-fixture) manifest bytes. */
  captureManifest?: string | Record<string, any>;
  captureManifestSha256?: string;
  signal?: AbortSignal;
  hubSnapshot: string | AuthoritySnapshot;
  workSnapshot: string | AuthoritySnapshot;
  passes?: number;
  timeoutMs?: number;
  synthetic?: boolean;
  evidenceDir?: string;
  receipt?: string;
}
export interface AuthoritySnapshot {
  schemaVersion: 1;
  site: "hub" | "work";
  name: string;
  peer: string;
  backend: "sqlite" | "file";
  collectedAt: number;
  stateMetadata: Record<string, any>;
  stateEntries: Array<Record<string, any>>;
  participantFiles: Array<Record<string, any>>;
  leaseFiles: Array<Record<string, any>>;
  cursors: Array<Record<string, any>>;
  counts?: Record<string, any>;
}
export interface AuthorityDiagnosticContext {
  partialDiagnostic: true;
  label: "partial diagnostic: participant/lease coverage missing; mixed-era (hub 16:30 live, work 12:40 snapshot)";
  incidentFaithful: false;
  acceptanceEligible: false;
  missingAuthority: Array<"hub.participantFiles" | "hub.leaseFiles" | "work.participantFiles" | "work.leaseFiles">;
  material: "mixed-era";
}
export interface AuthorityResult {
  diagnosticContext?: AuthorityDiagnosticContext;
  captureAge?: Record<string, Record<string, string | number | null>>;
  mode: "authority-snapshot";
  releaseGate: false;
  rolloutPermission: false;
  ok: boolean;
  outcome: "BLOCKED" | "RED" | "NON_REPRO" | "PASS";
  acceptance: boolean;
  coverageSlice: "LIVE_NATIVE_WRITERS" | "CAPTURED_ONLY_DIAGNOSTIC";
  scope: "producer-boundary compatibility; principal session bootstrap and auth policy are not exercised";
  diagnosticComplete?: boolean;
  evidenceKind: "SYNTHETIC" | "CAPTURED_AUTHORITY";
  primaryOutcome?: "BLOCKED" | "RED" | "NON_REPRO" | "PASS";
  limitedCoverage?: boolean;
  arms: Array<Record<string, any>>;
  evidenceRoot?: string;
  failure?: { message: string };
  startedAt: number;
  finishedAt: number;
  elapsedMs: number;
}
export interface AuthorityWriterPlanEntry {
  site: "hub" | "work";
  role: "sandbox-principal" | "sandbox-main";
  rootId: string;
  name: string;
}
export function classifyAuthorityCoverage(snapshots: Partial<Record<"hub" | "work", AuthoritySnapshot>>): AuthorityDiagnosticContext | undefined;
export function evaluateAuthorityAcceptance(result: Pick<AuthorityResult, "diagnosticContext" | "arms">, synthetic?: boolean): boolean;
export function runAuthorityCanary(options: AuthorityOptions): Promise<AuthorityResult>;
export function runLiveAuthorityCanary(options: AuthorityOptions): Promise<AuthorityResult>;
export function validateCaptureProvenance(options: AuthorityOptions, snapshots: Record<"hub" | "work", AuthoritySnapshot>): Record<string, any>;
export function createAuthorityWriterPlan(snapshots: Record<"hub" | "work", AuthoritySnapshot>): AuthorityWriterPlanEntry[];
export function evaluateAuthorityWriterCoverage(evidence?: { producers?: Array<Record<string, any>>; plan?: AuthorityWriterPlanEntry[]; expiry?: Array<Record<string, any>>;
  passes?: Array<Record<string, any>>; inFlightAtMeasurementEnd?: number[]; requestedPasses?: number }): Record<string, any>;
/** Emits only causal compiled expiry evidence bound to the original capture and global clock translation. */
export function collectNaturalExpiry(snapshots: Record<"hub" | "work", AuthoritySnapshot>, deltaMs: number,
  observed: Record<"hub" | "work", Array<Record<string, any>>>, producers: Array<Record<string, any>>,
  readers: Record<string, any>, arm: Record<string, any>): Array<Record<string, any>>;
export function sanitizeAuthorityEvidence<T>(value: T): T;
export function validateAuthoritySnapshot(snapshot: unknown, site: "hub" | "work"): AuthoritySnapshot;
export function rebaseAuthoritySnapshot(snapshot: AuthoritySnapshot, replayAt: number): { snapshot: AuthoritySnapshot; rebase: Record<string, any> };
export function authorityPredicates(presence: Record<string, any>, authority: Record<string, any>, peer: string, fromHub: boolean, reserved?: string[]): Array<Record<string, any>>;
export function matchAuthorityRefusal(error: Record<string, any>, predicates: Array<Record<string, any>>): Record<string, any>;
export function parseArgs(argv: ["--authority-snapshot", ...string[]]): AuthorityOptions;
export function parseArgs(argv: string[]): CanaryOptions;
export function validateInputs(options: CanaryOptions): { candidate: ReleaseIdentity; old: ReleaseIdentity[] };
/**
 * Pure, fail-closed judgment over collected evidence. A normal gate requires a complete
 * directed plan/ACK matrix, a continuous unrestarted hold, successful cleanup, and NEW's
 * before/after hub-route readback for the tokenless legacy root. Baseline-control evidence may be valid, but
 * must never be marked as a release gate.
 */
export function evaluateReceipt(evidence?: Record<string, unknown>): ReceiptEvaluation;
