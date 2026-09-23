export interface CapturePatchInput {
  repo: string;
  baseline: string;
  out: string;
}
export interface CapturedPatch {
  patchPath: string;
  sha256: string;
  bytes: number;
}
export function capturePatch(input: CapturePatchInput): Promise<CapturedPatch>;
