import Base
import ./resources.bend as R

# This module is the executable policy, not a reference implementation.
def all(xs: +List<Bool>) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{False{}, t}:
      False{}
    case Con{True{}, t}:
      all(t)

def footprintConditions(count: Nat, limit: Nat, valid: Bool) -> +List<Bool>:
  [valid, Nat.is_le(count, limit)]

def footprintFits(count: Nat, limit: Nat, valid: Bool) -> Bool:
  all(footprintConditions(count, limit, valid))

def unknownConflict(lu: Bool, luo: Bool, lo: Bool, ru: Bool, ruo: Bool, ro: Bool) -> Bool:
  (lu && (luo || ro)) || (ru && (ruo || lo))

def transitionConditions(retired: Bool, epoch: Bool, owner: Bool, closed: Bool) -> +List<Bool>:
  [Bool.not(retired), epoch, owner, Bool.not(closed)]

def transitionCurrent(retired: Bool, epoch: Bool, owner: Bool, closed: Bool) -> Bool:
  all(transitionConditions(retired, epoch, owner, closed))

def closeConditions(retiring: Bool, owner: Bool, retained: Bool, inflight: Bool) -> +List<Bool>:
  [retiring, Bool.not(owner), Bool.not(retained), Bool.not(inflight)]

def canClose(retiring: Bool, owner: Bool, retained: Bool, inflight: Bool) -> Bool:
  all(closeConditions(retiring, owner, retained, inflight))

def cleanupState(failed: Bool, diverted: Bool) -> U32:
  match failed diverted:
    case True{} _:
      2
    case False{} True{}:
      1
    case False{} False{}:
      0

type Span is Data:
  Span{first: Nat, last: Nat, paired: Bool}

def spanSafe(+boundary: Nat, span: Span) -> Bool:
  match span:
    case Span{first, last, paired}:
      Bool.not(paired) || Nat.is_le(boundary, first) || Nat.is_lt(last, boundary)

def spanConditions(spans: +List<Span>, +boundary: Nat) -> +List<Bool>:
  match spans:
    case Nil{}:
      Nil{}
    case Con{h, t}:
      spanSafe(boundary, h) <> spanConditions(t, boundary)

def spansSafe(spans: +List<Span>, boundary: Nat) -> Bool:
  all(spanConditions(spans, boundary))

type Cut is Data:
  Cut{eligible: Bool, afterPrevious: Bool, retained: Nat, budget: Nat, boundary: Nat, spans: +List<Span>}

def cutConditions(cut: Cut) -> +List<Bool>:
  match cut:
    case Cut{eligible, afterPrevious, retained, budget, boundary, spans}:
      [eligible, afterPrevious, Nat.is_le(retained, budget), spansSafe(spans, boundary)]

def cutAccepted(cut: Cut) -> Bool:
  all(cutConditions(cut))

def lineageSelected(allBranches: Bool, member: Bool) -> Bool:
  allBranches || member

def pointerCurrent(source: Bool, lineage: Bool) -> Bool:
  all([source, lineage])

type Chunk is Data:
  Chunk{start: Nat, end: Nat, total: Nat, expectedStart: Nat, expectedTotal: Nat, length: Nat, complete: Bool}

def chunkConditions(chunk: Chunk) -> +List<Bool>:
  match chunk:
    case Chunk{+start, +end, +total, expectedStart, expectedTotal, length, +complete}:
      [Nat.is_eq(start, expectedStart), Nat.is_eq(total, expectedTotal), Nat.is_le(start, end), Nat.is_le(end, total), Nat.is_eq(length, Nat.sub(end, start)), Bool.not(Bool.xor(complete, Nat.is_eq(end, total))), complete || Nat.is_lt(start, end)]

def chunkAccepted(chunk: Chunk) -> Bool:
  all(chunkConditions(chunk))

def coverageComplete(sourceComplete: Bool, truncated: Bool) -> Bool:
  all([sourceComplete, Bool.not(truncated)])

def normalizationConditions(canonical: Bool, plan: Bool, changed: Bool, accepted: Bool) -> +List<Bool>:
  [Bool.not(canonical), plan, changed, accepted]

def useNormalized(canonical: Bool, plan: Bool, changed: Bool, accepted: Bool) -> Bool:
  all(normalizationConditions(canonical, plan, changed, accepted))

def headReadable(sequence: Bool, version: Bool, committed: Bool, pending: Bool, marker: Bool) -> Bool:
  all([sequence, version, committed || (pending && marker)])

def certificateAccepted(conditions: +List<Bool>) -> Bool:
  all(conditions)

def consume(active: Bool) -> Bool & Bool:
  (active, False{})

def knownConflict(shared: Bool, leftOrdered: Bool, rightOrdered: Bool) -> Bool:
  shared && (leftOrdered || rightOrdered)

def summaryWithin(bytes: Nat, byteLimit: Nat, projected: Nat, target: Nat) -> Bool:
  all([Nat.is_le(bytes, byteLimit), Nat.is_le(projected, target)])

def sampleWithin(total: Nat, retained: Nat, omitted: Nat, limit: Nat) -> Bool:
  +kept = retained
  all([Nat.is_eq(total, Nat.add(kept, omitted)), Nat.is_le(kept, limit)])

def resourceSource(names: +List<+List<Nat>>) -> R.Scope:
  R.source(names)

def resourceNormalize(original: R.Scope, candidate: +List<+List<Nat>>) -> R.Scope:
  R.normalize(original, candidate)

def resourceConflict(left: R.Scope, right: R.Scope, lo: Bool, ro: Bool) -> Bool:
  R.conflict(left, right, lo, ro)

def resourceGroups(left: +List<R.Effect>, right: +List<R.Effect>) -> Bool:
  R.groups(left, right, False{})

type Kernel is Type:
  Kernel{resourceSource: +List<+List<Nat>> -> R.Scope, resourceNormalize: R.Scope -> +List<+List<Nat>> -> R.Scope, resourceConflict: R.Scope -> R.Scope -> Bool -> Bool -> Bool, resourceGroups: +List<R.Effect> -> +List<R.Effect> -> Bool, knownConflict: Bool -> Bool -> Bool -> Bool, summaryWithin: Nat -> Nat -> Nat -> Nat -> Bool, sampleWithin: Nat -> Nat -> Nat -> Nat -> Bool, footprintFits: Nat -> Nat -> Bool -> Bool, unknownConflict: Bool -> Bool -> Bool -> Bool -> Bool -> Bool -> Bool, transitionCurrent: Bool -> Bool -> Bool -> Bool -> Bool, canClose: Bool -> Bool -> Bool -> Bool -> Bool, cleanupState: Bool -> Bool -> U32, spansSafe: +List<Span> -> Nat -> Bool, cutAccepted: Cut -> Bool, lineageSelected: Bool -> Bool -> Bool, pointerCurrent: Bool -> Bool -> Bool, chunkAccepted: Chunk -> Bool, coverageComplete: Bool -> Bool -> Bool, useNormalized: Bool -> Bool -> Bool -> Bool -> Bool, headReadable: Bool -> Bool -> Bool -> Bool -> Bool -> Bool, certificateAccepted: +List<Bool> -> Bool, consume: Bool -> Bool & Bool}

def kernel() -> Kernel:
  Kernel{resourceSource, resourceNormalize, resourceConflict, resourceGroups, knownConflict, summaryWithin, sampleWithin, footprintFits, unknownConflict, transitionCurrent, canClose, cleanupState, spansSafe, cutAccepted, lineageSelected, pointerCurrent, chunkAccepted, coverageComplete, useNormalized, headReadable, certificateAccepted, consume}

# The CLI only emits JS for a runnable main. The build bridge removes its two
# invocation statements and exports kernel(); no IO is executed at import.
def main() -> IO(Kernel):
  IO.pure(Kernel, kernel())
