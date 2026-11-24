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

export function parseArgs(argv: string[]): CanaryOptions;
export function validateInputs(options: CanaryOptions): { candidate: ReleaseIdentity; old: ReleaseIdentity[] };
/**
 * Pure, fail-closed judgment over collected evidence. A normal gate requires a complete
 * directed plan/ACK matrix, a continuous unrestarted hold, successful cleanup, and NEW's
 * before/after hub-route readback for the tokenless legacy root. Baseline-control evidence may be valid, but
 * must never be marked as a release gate.
 */
export function evaluateReceipt(evidence?: Record<string, unknown>): ReceiptEvaluation;
