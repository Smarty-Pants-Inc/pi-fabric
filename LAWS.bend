import ./proofs/resources.bend as R
import ./proofs/resource-spec.bend as RS
import Base
import ./proofs/kernel.bend as K
import ./proofs/state-plans.bend as SP
import ./proofs/state-spec.bend as SS
import ./proofs/state-kernel.bend as SK

import ./proofs/provider-plans.bend as DP
import ./proofs/provider-spec.bend as DS
import ./proofs/provider-kernel.bend as DK

import ./proofs/authority-kernel.bend as AK
import ./proofs/authority-state.bend as A
import ./proofs/authority-spec.bend as AS

law authority_issue_exact:
  for +grants: +List<+List<Nat>>
  {AK.authorityIssue(grants) == A.Active{grants} : A.Authority}

law authority_issue_live:
  for +grants: +List<+List<Nat>>
  {AK.authorityLive(AK.authorityIssue(grants)) == True{} : Bool}

law authority_use_safe:
  for +authority: A.Authority
  for +grant: +List<Nat>
  for allowed: {AK.authorityAllows(authority, grant) == True{} : Bool}
  AS.CanUse(authority, grant)

law authority_use_complete:
  for +authority: A.Authority
  for +grant: +List<Nat>
  for member: AS.CanUse(authority, grant)
  {AK.authorityAllows(authority, grant) == True{} : Bool}

law authority_derive_safe:
  for +parent: A.Authority
  for +candidate: +List<+List<Nat>>
  AS.DerivationSafe(parent, candidate, AK.authorityDerive(parent, candidate))

law authority_derive_complete:
  for +parent: A.Authority
  for +candidate: +List<+List<Nat>>
  for subset: AS.CanDerive(parent, candidate)
  AS.DerivationAccepted(candidate, AK.authorityDerive(parent, candidate))

law authority_child_use_safe:
  for +parent: A.Authority
  for +candidate: +List<+List<Nat>>
  for +grant: +List<Nat>
  for used: {AK.authorityAllows(AK.authorityDerive(parent, candidate), grant) == True{} : Bool}
  AS.CanUse(parent, grant)

law authority_released_refused:
  for +authority: A.Authority
  for +grant: +List<Nat>
  {AK.authorityAllows(AK.authorityRelease(authority), grant) == False{} : Bool}

law authority_released_cannot_derive:
  for +authority: A.Authority
  for +candidate: +List<+List<Nat>>
  {AK.authorityDerive(AK.authorityRelease(authority), candidate) == A.Released{} : A.Authority}

law authority_released_not_live:
  for +authority: A.Authority
  {AK.authorityLive(AK.authorityRelease(authority)) == False{} : Bool}

law provider_plan_safe:
  for +ticket: DP.Ticket
  for +key: +List<Nat>
  for +observed: +List<Nat>
  for +stopped: Bool
  DS.Safe(ticket, key, observed, stopped, DP.plan(DK.providerTake(ticket, key, observed, stopped)))

law provider_ticket_consumed:
  for +ticket: DP.Ticket
  for +key: +List<Nat>
  for +observed: +List<Nat>
  for +stopped: Bool
  DS.Consumed(ticket, DP.next(DK.providerTake(ticket, key, observed, stopped)))

law provider_replay_refused:
  for +ticket: DP.Ticket
  for +key: +List<Nat>
  for +observed: +List<Nat>
  for +stopped: Bool
  for +again: +List<Nat>
  for +revision: +List<Nat>
  {DP.plan(DK.providerTake(DP.next(DK.providerTake(ticket, key, observed, stopped)), again, revision, False{})) == SP.Denied{} : SP.Plan}

law provider_revocation_consumes:
  for +ticket: DP.Ticket
  DS.Consumed(ticket, DK.providerRevoke(ticket))

law provider_revoked_refused:
  for +ticket: DP.Ticket
  for +key: +List<Nat>
  for +observed: +List<Nat>
  for +stopped: Bool
  {DP.plan(DK.providerTake(DK.providerRevoke(ticket), key, observed, stopped)) == SP.Denied{} : SP.Plan}

law provider_canceled_refused:
  for +ticket: DP.Ticket
  for +key: +List<Nat>
  for +observed: +List<Nat>
  {DP.plan(DK.providerTake(ticket, key, observed, True{})) == SP.Denied{} : SP.Plan}

law provider_admission_complete:
  for +key: +List<Nat>
  for +revision: +List<Nat>
  for +payload: +List<Nat>
  {DP.plan(DK.providerTake(DP.Ticket{SP.Grant{True{}, key}, revision, payload}, key, revision, False{})) == SP.Write{key, revision, payload} : SP.Plan}

law state_read_safe:
  for +grant: SP.Grant
  for +key: +List<Nat>
  for +stopped: Bool
  SS.ReadSafe(grant, key, stopped, SK.stateRead(grant, key, stopped))

law state_write_safe:
  for +grant: SP.Grant
  for +key: +List<Nat>
  for +expected: +List<Nat>
  for +observed: +List<Nat>
  for +value: +List<Nat>
  for +stopped: Bool
  SS.WriteSafe(grant, key, expected, observed, value, stopped, SK.stateWrite(grant, key, expected, observed, value, stopped))

law state_read_complete:
  for +key: +List<Nat>
  {SK.stateRead(SP.Grant{True{}, key}, key, False{}) == SP.Read{key} : SP.Plan}

law state_write_complete:
  for +key: +List<Nat>
  for +revision: +List<Nat>
  for +value: +List<Nat>
  {SK.stateWrite(SP.Grant{True{}, key}, key, revision, revision, value, False{}) == SP.Write{key, revision, value} : SP.Plan}

law state_stale_refused:
  for +grant: SP.Grant
  for +key: +List<Nat>
  for +expected: +List<Nat>
  for +observed: +List<Nat>
  for +value: +List<Nat>
  for +stopped: Bool
  for different: {expected != observed : +List<Nat>}
  {SK.stateWrite(grant, key, expected, observed, value, stopped) == SP.Denied{} : SP.Plan}

law state_revoked_read:
  for +grant: SP.Grant
  for +key: +List<Nat>
  for +stopped: Bool
  {SK.stateRead(SK.stateRevoke(grant), key, stopped) == SP.Denied{} : SP.Plan}

law state_revoked_write:
  for +grant: SP.Grant
  for +key: +List<Nat>
  for +expected: +List<Nat>
  for +observed: +List<Nat>
  for +value: +List<Nat>
  for +stopped: Bool
  {SK.stateWrite(SK.stateRevoke(grant), key, expected, observed, value, stopped) == SP.Denied{} : SP.Plan}

law state_canceled_read:
  for +grant: SP.Grant
  for +key: +List<Nat>
  {SK.stateRead(grant, key, True{}) == SP.Denied{} : SP.Plan}

law state_canceled_write:
  for +grant: SP.Grant
  for +key: +List<Nat>
  for +expected: +List<Nat>
  for +observed: +List<Nat>
  for +value: +List<Nat>
  {SK.stateWrite(grant, key, expected, observed, value, True{}) == SP.Denied{} : SP.Plan}


# Specification boundary: review changes here independently from implementation.
# Predicates below spell out requirements rather than importing the kernel's
# condition builders. Host observations, validators, hashing, CAS, and compiler
# correctness remain explicit assumptions, not propositions proved here.

def AllTrue(xs: +List<Bool>) -> Type:
  match xs:
    case Nil{}:
      Unit
    case Con{h, t}:
      {h == True{} : Bool} & AllTrue(t)

def SpanAllowed(+boundary: Nat, span: K.Span) -> Bool:
  match span:
    case K.Span{first, last, paired}:
      Bool.not(paired) || Nat.is_le(boundary, first) || Nat.is_lt(last, boundary)

def SpanRequirements(spans: +List<K.Span>, +boundary: Nat) -> +List<Bool>:
  match spans:
    case Nil{}:
      Nil{}
    case Con{h, t}:
      SpanAllowed(boundary, h) <> SpanRequirements(t, boundary)

def CutValid(cut: K.Cut) -> Type:
  match cut:
    case K.Cut{eligible, afterPrevious, retained, budget, boundary, spans}:
      {eligible == True{} : Bool} & ({afterPrevious == True{} : Bool} & ({Nat.is_le(retained, budget) == True{} : Bool} & AllTrue(SpanRequirements(spans, boundary))))

def ChunkRequirements(chunk: K.Chunk) -> +List<Bool>:
  match chunk:
    case K.Chunk{+start, +end, +total, expectedStart, expectedTotal, length, +complete}:
      [Nat.is_eq(start, expectedStart), Nat.is_eq(total, expectedTotal), Nat.is_le(start, end), Nat.is_le(end, total), Nat.is_eq(length, Nat.sub(end, start)), Bool.not(Bool.xor(complete, Nat.is_eq(end, total))), complete || Nat.is_lt(start, end)]

law all_sound:
  for +xs: +List<Bool>
  for p: {K.all(xs) == True{} : Bool}
  AllTrue(xs)

law all_complete:
  for +xs: +List<Bool>
  for p: AllTrue(xs)
  {K.all(xs) == True{} : Bool}

law footprint_sound:
  for +count: Nat
  for +limit: Nat
  for +valid: Bool
  for p: {K.footprintFits(count, limit, valid) == True{} : Bool}
  AllTrue([valid, Nat.is_le(count, limit)])

law unknown_conflict_symmetric:
  for +lu: Bool
  for +luo: Bool
  for +lo: Bool
  for +ru: Bool
  for +ruo: Bool
  for +ro: Bool
  {K.unknownConflict(lu, luo, lo, ru, ruo, ro) == K.unknownConflict(ru, ruo, ro, lu, luo, lo) : Bool}

law transition_sound:
  for +retired: Bool
  for +epoch: Bool
  for +owner: Bool
  for +closed: Bool
  for p: {K.transitionCurrent(retired, epoch, owner, closed) == True{} : Bool}
  AllTrue([Bool.not(retired), epoch, owner, Bool.not(closed)])

law close_sound:
  for +retiring: Bool
  for +owner: Bool
  for +retained: Bool
  for +inflight: Bool
  for p: {K.canClose(retiring, owner, retained, inflight) == True{} : Bool}
  AllTrue([retiring, Bool.not(owner), Bool.not(retained), Bool.not(inflight)])

law cleanup_failure_quarantines:
  for diverted: Bool
  {K.cleanupState(True{}, diverted) == 2 : U32}

law span_sound:
  for +boundary: Nat
  for +span: K.Span
  for p: {K.spanSafe(boundary, span) == True{} : Bool}
  {SpanAllowed(boundary, span) == True{} : Bool}

law spans_sound:
  for +spans: +List<K.Span>
  for +boundary: Nat
  for p: {K.spansSafe(spans, boundary) == True{} : Bool}
  AllTrue(SpanRequirements(spans, boundary))

law cut_sound:
  for +cut: K.Cut
  for p: {K.cutAccepted(cut) == True{} : Bool}
  CutValid(cut)

law pointer_sound:
  for +source: Bool
  for +lineage: Bool
  for p: {K.pointerCurrent(source, lineage) == True{} : Bool}
  AllTrue([source, lineage])

law chunk_sound:
  for +chunk: K.Chunk
  for p: {K.chunkAccepted(chunk) == True{} : Bool}
  AllTrue(ChunkRequirements(chunk))

law coverage_sound:
  for +complete: Bool
  for +truncated: Bool
  for p: {K.coverageComplete(complete, truncated) == True{} : Bool}
  AllTrue([complete, Bool.not(truncated)])

law normalization_sound:
  for +canonical: Bool
  for +plan: Bool
  for +changed: Bool
  for +accepted: Bool
  for p: {K.useNormalized(canonical, plan, changed, accepted) == True{} : Bool}
  AllTrue([Bool.not(canonical), plan, changed, accepted])

law canonical_identity:
  for plan: Bool
  for changed: Bool
  for accepted: Bool
  {K.useNormalized(True{}, plan, changed, accepted) == False{} : Bool}

law pending_without_marker_hidden:
  for sequence: Bool
  for version: Bool
  {K.headReadable(sequence, version, False{}, True{}, False{}) == False{} : Bool}

law committed_head_visible:
  for pending: Bool
  for marker: Bool
  {K.headReadable(True{}, True{}, True{}, pending, marker) == True{} : Bool}

law certificate_sound:
  for +conditions: +List<Bool>
  for p: {K.certificateAccepted(conditions) == True{} : Bool}
  AllTrue(conditions)

law consume_once:
  {K.consume(False{}) == (False{}, False{}) : Bool & Bool}

law consume_retires:
  for active: Bool
  {K.consume(active) == (active, False{}) : Bool & Bool}

law unknown_conflict_exact:
  for +lu: Bool
  for +luo: Bool
  for +lo: Bool
  for +ru: Bool
  for +ruo: Bool
  for +ro: Bool
  {K.unknownConflict(lu, luo, lo, ru, ruo, ro) == ((lu && (luo || ro)) || (ru && (ruo || lo))) : Bool}

law active_lineage_only:
  for member: Bool
  {K.lineageSelected(False{}, member) == member : Bool}

law explicit_all_lineages:
  for member: Bool
  {K.lineageSelected(True{}, member) == True{} : Bool}

law known_conflict_exact:
  for +shared: Bool
  for +left: Bool
  for +right: Bool
  {K.knownConflict(shared, left, right) == (shared && (left || right)) : Bool}

law summary_bounds:
  for +bytes: Nat
  for +byteLimit: Nat
  for +projected: Nat
  for +target: Nat
  for p: {K.summaryWithin(bytes, byteLimit, projected, target) == True{} : Bool}
  AllTrue([Nat.is_le(bytes, byteLimit), Nat.is_le(projected, target)])

law sample_accounting:
  for +total: Nat
  for +retained: Nat
  for +omitted: Nat
  for +limit: Nat
  for p: {K.sampleWithin(total, retained, omitted, limit) == True{} : Bool}
  AllTrue([Nat.is_eq(total, Nat.add(retained, omitted)), Nat.is_le(retained, limit)])

law head_sound:
  for +sequence: Bool
  for +version: Bool
  for +committed: Bool
  for +pending: Bool
  for +marker: Bool
  for p: {K.headReadable(sequence, version, committed, pending, marker) == True{} : Bool}
  AllTrue([sequence, version, committed || (pending && marker)])

# Positive obligations prevent replacing these acceptance decisions with deny-all.
law footprint_complete:
  for +count: Nat
  for +limit: Nat
  for +valid: Bool
  for p: AllTrue([valid, Nat.is_le(count, limit)])
  {K.footprintFits(count, limit, valid) == True{} : Bool}

law transition_complete:
  for +retired: Bool
  for +epoch: Bool
  for +owner: Bool
  for +closed: Bool
  for p: AllTrue([Bool.not(retired), epoch, owner, Bool.not(closed)])
  {K.transitionCurrent(retired, epoch, owner, closed) == True{} : Bool}

law close_complete:
  for +retiring: Bool
  for +owner: Bool
  for +retained: Bool
  for +inflight: Bool
  for p: AllTrue([retiring, Bool.not(owner), Bool.not(retained), Bool.not(inflight)])
  {K.canClose(retiring, owner, retained, inflight) == True{} : Bool}

law normalization_complete:
  for +canonical: Bool
  for +plan: Bool
  for +changed: Bool
  for +accepted: Bool
  for p: AllTrue([Bool.not(canonical), plan, changed, accepted])
  {K.useNormalized(canonical, plan, changed, accepted) == True{} : Bool}

law chunk_complete:
  for +chunk: K.Chunk
  for p: AllTrue(ChunkRequirements(chunk))
  {K.chunkAccepted(chunk) == True{} : Bool}

law certificate_complete:
  for +conditions: +List<Bool>
  for p: AllTrue(conditions)
  {K.certificateAccepted(conditions) == True{} : Bool}

# Full declarations enter this checker. Candidate production has no authority.
law resource_refinement:
  for +original: R.Scope
  for +candidate: +List<+List<Nat>>
  RS.Refines(original, K.resourceNormalize(original, candidate))

law resource_output_valid:
  for +original: R.Scope
  for +candidate: +List<+List<Nat>>
  RS.OutputValid(K.resourceNormalize(original, candidate))

law resource_conflict_exact:
  for +left: R.Scope
  for +right: R.Scope
  for +lo: Bool
  for +ro: Bool
  for p: {K.resourceConflict(left, right, lo, ro) == True{} : Bool}
  RS.Conflicts(left, right, lo, ro)

law resource_conflict_complete:
  for +left: R.Scope
  for +right: R.Scope
  for +lo: Bool
  for +ro: Bool
  for p: RS.Conflicts(left, right, lo, ro)
  {K.resourceConflict(left, right, lo, ro) == True{} : Bool}

law resource_conflict_preserved:
  for +left: R.Scope
  for +right: R.Scope
  for +lp: +List<+List<Nat>>
  for +rp: +List<+List<Nat>>
  for +lo: Bool
  for +ro: Bool
  for p: {K.resourceConflict(left, right, lo, ro) == True{} : Bool}
  {K.resourceConflict(K.resourceNormalize(left, lp), K.resourceNormalize(right, rp), lo, ro) == True{} : Bool}

law resource_precision:
  for +original: +List<+List<Nat>>
  for +candidate: +List<+List<Nat>>
  for quality: RS.Quality(candidate)
  for forward: RS.Subset(original, candidate)
  for backward: RS.Subset(candidate, original)
  {K.resourceNormalize(R.Exact{original}, candidate) == R.Exact{candidate} : R.Scope}

law resource_identity:
  for +names: +List<+List<Nat>>
  for quality: RS.Quality(names)
  {K.resourceNormalize(R.Exact{names}, names) == R.Exact{names} : R.Scope}

law resource_recheck_idempotent:
  for +original: R.Scope
  for +candidate: +List<+List<Nat>>
  {K.resourceNormalize(K.resourceNormalize(original, candidate), candidate) == K.resourceNormalize(original, candidate) : R.Scope}

law resource_groups_exact:
  for +left: +List<R.Effect>
  for +right: +List<R.Effect>
  {K.resourceGroups(left, right) == RS.Groups(left, right) : Bool}

law resource_source_refinement:
  for +names: +List<+List<Nat>>
  RS.Refines(R.Exact{names}, K.resourceSource(names))

law resource_source_valid:
  for +names: +List<+List<Nat>>
  RS.SourceValid(K.resourceSource(names))

law resource_pair_sound:
  for +left: R.Effect
  for +right: R.Effect
  for p: {R.pairConflict(left, right) == True{} : Bool}
  RS.EffectConflicts(left, right)

law resource_pair_complete:
  for +left: R.Effect
  for +right: R.Effect
  for p: RS.EffectConflicts(left, right)
  {R.pairConflict(left, right) == True{} : Bool}

law resource_source_complete:
  for +names: +List<+List<Nat>>
  for p: RS.SourceValid(R.Exact{names})
  {K.resourceSource(names) == R.Exact{names} : R.Scope}
