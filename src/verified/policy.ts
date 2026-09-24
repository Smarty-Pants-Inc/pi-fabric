import { BEND_NAT_MAX } from "./nat.js";
import {
  cutAccepted, chunkAccepted, certificateAccepted, summaryWithin, sampleWithin,
  type BendList, type Span,
} from "./generated/kernel.js";

export {
  unknownConflict, knownConflict, transitionCurrent, canClose, cleanupState, pointerCurrent,
  coverageComplete, useNormalized, headReadable, consume, lineageSelected,
} from "./generated/kernel.js";

const isNatural = (value: number): boolean => Number.isSafeInteger(value) && value >= 0;
const isArithmeticNatural = (value: number): boolean => isNatural(value) && value <= BEND_NAT_MAX;

export const bendList = <T>(values: readonly T[]): BendList<T> => {
  let list: BendList<T> = { $: "Nil" };
  for (let index = values.length - 1; index >= 0; index--) {
    list = { $: "Con", head: values[index]!, tail: list };
  }
  return list;
};

export { boundedEffectResources } from "./resources.js";

export const acceptSummaryBounds = (bytes: number, byteLimit: number, projected: number, target: number): boolean =>
  [bytes, byteLimit, projected, target].every(isNatural) &&
  summaryWithin(BigInt(bytes), BigInt(byteLimit), BigInt(projected), BigInt(target));

export const acceptSampleAccounting = (total: number, retained: number, omitted: number, limit: number): boolean =>
  [total, retained, omitted, limit].every(isArithmeticNatural) && retained + omitted <= BEND_NAT_MAX &&
  sampleWithin(BigInt(total), BigInt(retained), BigInt(omitted), BigInt(limit));

export interface CutSpan {
  first: number;
  last: number;
  hasCall: boolean;
  hasResult: boolean;
}

export interface CutCandidate {
  eligible: boolean;
  afterPrevious: boolean;
  retained: number;
  budget: number;
  boundary: number;
}

/** The producer may reject cheaply, but cannot publish a cut without this check.
 * Batches bound backend non-tail list construction; every source span is checked. */
export const acceptCompactionCut = (candidate: CutCandidate, spans: Iterable<CutSpan>): boolean => {
  if (![candidate.retained, candidate.budget, candidate.boundary].every(isNatural) ||
      typeof candidate.eligible !== "boolean" || typeof candidate.afterPrevious !== "boolean") return false;
  const base = {
    $: "Cut" as const, eligible: candidate.eligible, afterPrevious: candidate.afterPrevious,
    retained: BigInt(candidate.retained), budget: BigInt(candidate.budget), boundary: BigInt(candidate.boundary),
  };
  if (!cutAccepted({ ...base, spans: { $: "Nil" } })) return false;
  let batch: Span[] = [];
  for (const span of spans) {
    if (!isNatural(span.first) || !isNatural(span.last) || span.first > span.last ||
        typeof span.hasCall !== "boolean" || typeof span.hasResult !== "boolean") return false;
    batch.push({ $: "Span", first: BigInt(span.first), last: BigInt(span.last), paired: span.hasCall && span.hasResult });
    if (batch.length === 128) {
      if (!cutAccepted({ ...base, spans: bendList(batch) })) return false;
      batch = [];
    }
  }
  return cutAccepted({ ...base, spans: bendList(batch) });
};

export interface TextRange { start: number; end: number; total: number; complete: boolean }
export const acceptMemoryChunk = (
  range: TextRange, length: number, expectedStart: number, expectedTotal: number,
): boolean => {
  if (![range.start, range.end, range.total, length, expectedStart, expectedTotal].every(isArithmeticNatural) || range.start + length > BEND_NAT_MAX ||
      typeof range.complete !== "boolean") return false;
  return chunkAccepted({
    $: "Chunk", start: BigInt(range.start), end: BigInt(range.end), total: BigInt(range.total),
    length: BigInt(length), expectedStart: BigInt(expectedStart), expectedTotal: BigInt(expectedTotal),
    complete: range.complete,
  });
};

/** A fixed ordered tuple prevents a missing check from becoming an empty,
 * vacuously accepted certificate. Messages are diagnostics, never authority. */
export const assertCertificateFacts = (
  facts: readonly [CertificateFact, CertificateFact, CertificateFact, CertificateFact, CertificateFact, CertificateFact, CertificateFact, CertificateFact],
): void => {
  if (facts.length !== 8 || facts.some((fact) => typeof fact.valid !== "boolean")) {
    throw new Error("Invalid Schema certificate evidence");
  }
  if (!certificateAccepted(bendList(facts.map((fact) => fact.valid)))) {
    throw new Error(facts.find((fact) => !fact.valid)?.error ?? "Schema certificate rejected");
  }
};
interface CertificateFact { valid: boolean; error: string }
