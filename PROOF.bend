import ./proofs/storage-proof.bend as StorageProof
import ./LAWS.bend as Laws
import ./proofs/authority-proof.bend as AP
import ./proofs/lifecycle-proof.bend as LP
import ./proofs/resource-proof.bend as RP
import ./proofs/resources.bend as R
import Base
import ./proofs/state-plans.bend as SP
import ./proofs/state-proof.bend as SPP
import ./proofs/kernel.bend as K
import ./proofs/provider-plans.bend as DP

def Laws.authority_issue_exact(grants):
  AP.issueExact(grants)

def Laws.authority_issue_live(grants):
  {==}

def Laws.authority_use_safe(authority, grant, allowed):
  AP.useSafe(authority, grant, allowed)

def Laws.authority_use_complete(authority, grant, member):
  AP.useAccepted(authority, grant, member)

def Laws.authority_derive_safe(parent, candidate):
  AP.deriveSafe(parent, candidate)

def Laws.authority_derive_complete(parent, candidate, subset):
  AP.deriveAccepted(parent, candidate, subset)

def Laws.authority_child_use_safe(parent, candidate, grant, used):
  AP.childUseSafe(parent, candidate, grant, used)

def Laws.authority_released_refused(authority, grant):
  AP.releasedCannotUse(authority, grant)

def Laws.authority_released_cannot_derive(authority, candidate):
  AP.releasedCannotDerive(authority, candidate)

def Laws.authority_released_not_live(authority):
  AP.releaseNotLive(authority)




def Laws.state_read_safe(grant, key, stopped):
  SPP.readSelected(grant, key, stopped, SP.allowed(grant, key, stopped), {==})

def Laws.state_write_safe(grant, key, expected, observed, value, stopped):
  SPP.writeSelected(grant, key, expected, observed, value, stopped, SP.allowed(grant, key, stopped) && R.nameEqual(expected, observed, True{}), {==})

def Laws.state_read_complete(key):
  %Equal.sym(Bool, R.nameEqual(key, key, True{}), True{}, RP.nameRefl(key)) : {SP.selectRead(_, key) == SP.Read{key} : SP.Plan}
  {==}

def Laws.state_write_complete(key, revision, value):
  %Equal.sym(Bool, R.nameEqual(key, key, True{}), True{}, RP.nameRefl(key)) : {SP.selectWrite(_ && R.nameEqual(revision, revision, True{}), key, revision, value) == SP.Write{key, revision, value} : SP.Plan}
  %Equal.sym(Bool, R.nameEqual(revision, revision, True{}), True{}, RP.nameRefl(revision)) : {SP.selectWrite(_, key, revision, value) == SP.Write{key, revision, value} : SP.Plan}
  {==}

def Laws.state_stale_refused(grant, key, expected, observed, value, stopped, different):
  SPP.staleSelected(grant, key, expected, observed, value, stopped, SP.allowed(grant, key, stopped) && R.nameEqual(expected, observed, True{}), {==}, different)

def Laws.state_revoked_read(grant, key, stopped):
  match grant:
    case SP.Grant{active, granted}:
      {==}

def Laws.state_revoked_write(grant, key, expected, observed, value, stopped):
  match grant:
    case SP.Grant{active, granted}:
      {==}

def Laws.state_canceled_read(grant, key):
  match grant:
    case SP.Grant{False{}, granted}:
      {==}
    case SP.Grant{True{}, granted}:
      {==}

def Laws.state_canceled_write(grant, key, expected, observed, value):
  match grant:
    case SP.Grant{False{}, granted}:
      {==}
    case SP.Grant{True{}, granted}:
      {==}

def Laws.provider_plan_safe(ticket, key, observed, stopped):
  match ticket:
    case DP.Ticket{grant, expected, payload}:
      Laws.state_write_safe(grant, key, expected, observed, payload, stopped)

def Laws.provider_ticket_consumed(ticket, key, observed, stopped):
  match ticket:
    case DP.Ticket{SP.Grant{active, granted}, expected, payload}:
      ({==}, ({==}, ({==}, {==})))

def Laws.provider_replay_refused(ticket, key, observed, stopped, again, revision):
  match ticket:
    case DP.Ticket{SP.Grant{active, granted}, expected, payload}:
      {==}

def Laws.provider_revocation_consumes(ticket):
  match ticket:
    case DP.Ticket{SP.Grant{active, granted}, expected, payload}:
      ({==}, ({==}, ({==}, {==})))

def Laws.provider_revoked_refused(ticket, key, observed, stopped):
  match ticket:
    case DP.Ticket{SP.Grant{active, granted}, expected, payload}:
      {==}

def Laws.provider_canceled_refused(ticket, key, observed):
  match ticket:
    case DP.Ticket{grant, expected, payload}:
      Laws.state_canceled_write(grant, key, expected, observed, payload)

def Laws.provider_admission_complete(key, revision, payload):
  Laws.state_write_complete(key, revision, payload)

def discriminate(b: Bool) -> Type:
  match b:
    case False{}:
      Unit
    case True{}:
      Empty

def falseTrue(e: {False{} == True{} : Bool}) -> Empty:
  %e : discriminate(_)
  Unit{}

def bothTrue(+a: Bool, +b: Bool, p: {Bool.and(a, b) == True{} : Bool}) -> {a == True{} : Bool} & {b == True{} : Bool}:
  match a b:
    case False{} _:
      Empty.absurd({False{} == True{} : Bool} & {b == True{} : Bool}, falseTrue(p))
    case True{} False{}:
      Empty.absurd({True{} == True{} : Bool} & {False{} == True{} : Bool}, falseTrue(p))
    case True{} True{}:
      ({==}, {==})

def Laws.all_sound(xs, p):
  match xs:
    case Nil{}:
      Unit{}
    case Con{False{}, t}:
      Empty.absurd(Laws.AllTrue(False{} <> t), falseTrue(p))
    case Con{True{}, t}:
      ({==}, Laws.all_sound(t, p))

def Laws.all_complete(xs, p):
  match xs:
    case Nil{}:
      {==}
    case Con{False{}, t}:
      (hp, tp) = p
      Empty.absurd({K.all(False{} <> t) == True{} : Bool}, falseTrue(hp))
    case Con{True{}, t}:
      (hp, tp) = p
      Laws.all_complete(t, tp)

def Laws.footprint_sound(count, limit, valid, p):
  Laws.all_sound([valid, Nat.is_le(count, limit)], p)

def Laws.transition_sound(retired, epoch, owner, closed, p):
  Laws.all_sound([Bool.not(retired), epoch, owner, Bool.not(closed)], p)

def Laws.close_sound(retiring, owner, retained, inflight, p):
  Laws.all_sound([retiring, Bool.not(owner), Bool.not(retained), Bool.not(inflight)], p)

def Laws.cleanup_failure_quarantines(diverted):
  {==}

def Laws.span_sound(boundary, span, p):
  match span:
    case K.Span{first, last, paired}:
      p

def spanEvidence(+spans: +List<K.Span>, +boundary: Nat, p: Laws.AllTrue(K.spanConditions(spans, boundary))) -> Laws.AllTrue(Laws.SpanRequirements(spans, boundary)):
  match spans:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      (hp, tp) = p
      (Laws.span_sound(boundary, h, hp), spanEvidence(t, boundary, tp))

def Laws.spans_sound(spans, boundary, p):
  spanEvidence(spans, boundary, Laws.all_sound(K.spanConditions(spans, boundary), p))

def cutEvidence(+cut: K.Cut, p: Laws.AllTrue(K.cutConditions(cut))) -> Laws.CutValid(cut):
  match cut:
    case K.Cut{eligible, afterPrevious, retained, budget, boundary, spans}:
      (ep, rest) = p
      (ap, rest) = rest
      (bp, rest) = rest
      (sp, done) = rest
      (ep, (ap, (bp, Laws.spans_sound(spans, boundary, sp))))

def Laws.cut_sound(cut, p):
  cutEvidence(cut, Laws.all_sound(K.cutConditions(cut), p))

def Laws.pointer_sound(source, lineage, p):
  Laws.all_sound([source, lineage], p)

def Laws.chunk_sound(chunk, p):
  match chunk:
    case K.Chunk{start, end, total, expectedStart, expectedTotal, length, complete}:
      Laws.all_sound(Laws.ChunkRequirements(chunk), p)

def Laws.coverage_sound(complete, truncated, p):
  Laws.all_sound([complete, Bool.not(truncated)], p)

def Laws.normalization_sound(canonical, plan, changed, accepted, p):
  Laws.all_sound([Bool.not(canonical), plan, changed, accepted], p)

def Laws.canonical_identity(plan, changed, accepted):
  {==}

def Laws.pending_without_marker_hidden(sequence, version):
  match sequence version:
    case False{} _:
      {==}
    case True{} False{}:
      {==}
    case True{} True{}:
      {==}

def Laws.committed_head_visible(pending, marker):
  {==}

def Laws.certificate_sound(conditions, p):
  Laws.all_sound(conditions, p)

def Laws.consume_once():
  {==}

def Laws.consume_retires(active):
  {==}


def Laws.unknown_conflict_symmetric(lu, luo, lo, ru, ruo, ro):
  match lu luo lo ru ruo ro:
    case False{} False{} False{} False{} False{} False{}:
      {==}
    case True{} False{} False{} False{} False{} False{}:
      {==}
    case False{} True{} False{} False{} False{} False{}:
      {==}
    case True{} True{} False{} False{} False{} False{}:
      {==}
    case False{} False{} True{} False{} False{} False{}:
      {==}
    case True{} False{} True{} False{} False{} False{}:
      {==}
    case False{} True{} True{} False{} False{} False{}:
      {==}
    case True{} True{} True{} False{} False{} False{}:
      {==}
    case False{} False{} False{} True{} False{} False{}:
      {==}
    case True{} False{} False{} True{} False{} False{}:
      {==}
    case False{} True{} False{} True{} False{} False{}:
      {==}
    case True{} True{} False{} True{} False{} False{}:
      {==}
    case False{} False{} True{} True{} False{} False{}:
      {==}
    case True{} False{} True{} True{} False{} False{}:
      {==}
    case False{} True{} True{} True{} False{} False{}:
      {==}
    case True{} True{} True{} True{} False{} False{}:
      {==}
    case False{} False{} False{} False{} True{} False{}:
      {==}
    case True{} False{} False{} False{} True{} False{}:
      {==}
    case False{} True{} False{} False{} True{} False{}:
      {==}
    case True{} True{} False{} False{} True{} False{}:
      {==}
    case False{} False{} True{} False{} True{} False{}:
      {==}
    case True{} False{} True{} False{} True{} False{}:
      {==}
    case False{} True{} True{} False{} True{} False{}:
      {==}
    case True{} True{} True{} False{} True{} False{}:
      {==}
    case False{} False{} False{} True{} True{} False{}:
      {==}
    case True{} False{} False{} True{} True{} False{}:
      {==}
    case False{} True{} False{} True{} True{} False{}:
      {==}
    case True{} True{} False{} True{} True{} False{}:
      {==}
    case False{} False{} True{} True{} True{} False{}:
      {==}
    case True{} False{} True{} True{} True{} False{}:
      {==}
    case False{} True{} True{} True{} True{} False{}:
      {==}
    case True{} True{} True{} True{} True{} False{}:
      {==}
    case False{} False{} False{} False{} False{} True{}:
      {==}
    case True{} False{} False{} False{} False{} True{}:
      {==}
    case False{} True{} False{} False{} False{} True{}:
      {==}
    case True{} True{} False{} False{} False{} True{}:
      {==}
    case False{} False{} True{} False{} False{} True{}:
      {==}
    case True{} False{} True{} False{} False{} True{}:
      {==}
    case False{} True{} True{} False{} False{} True{}:
      {==}
    case True{} True{} True{} False{} False{} True{}:
      {==}
    case False{} False{} False{} True{} False{} True{}:
      {==}
    case True{} False{} False{} True{} False{} True{}:
      {==}
    case False{} True{} False{} True{} False{} True{}:
      {==}
    case True{} True{} False{} True{} False{} True{}:
      {==}
    case False{} False{} True{} True{} False{} True{}:
      {==}
    case True{} False{} True{} True{} False{} True{}:
      {==}
    case False{} True{} True{} True{} False{} True{}:
      {==}
    case True{} True{} True{} True{} False{} True{}:
      {==}
    case False{} False{} False{} False{} True{} True{}:
      {==}
    case True{} False{} False{} False{} True{} True{}:
      {==}
    case False{} True{} False{} False{} True{} True{}:
      {==}
    case True{} True{} False{} False{} True{} True{}:
      {==}
    case False{} False{} True{} False{} True{} True{}:
      {==}
    case True{} False{} True{} False{} True{} True{}:
      {==}
    case False{} True{} True{} False{} True{} True{}:
      {==}
    case True{} True{} True{} False{} True{} True{}:
      {==}
    case False{} False{} False{} True{} True{} True{}:
      {==}
    case True{} False{} False{} True{} True{} True{}:
      {==}
    case False{} True{} False{} True{} True{} True{}:
      {==}
    case True{} True{} False{} True{} True{} True{}:
      {==}
    case False{} False{} True{} True{} True{} True{}:
      {==}
    case True{} False{} True{} True{} True{} True{}:
      {==}
    case False{} True{} True{} True{} True{} True{}:
      {==}
    case True{} True{} True{} True{} True{} True{}:
      {==}

def Laws.unknown_conflict_exact(lu, luo, lo, ru, ruo, ro):
  {==}

def Laws.active_lineage_only(member):
  {==}

def Laws.explicit_all_lineages(member):
  {==}

def Laws.known_conflict_exact(shared, left, right):
  {==}

def Laws.summary_bounds(bytes, byteLimit, projected, target, p):
  Laws.all_sound([Nat.is_le(bytes, byteLimit), Nat.is_le(projected, target)], p)

def Laws.sample_accounting(total, retained, omitted, limit, p):
  Laws.all_sound([Nat.is_eq(total, Nat.add(retained, omitted)), Nat.is_le(retained, limit)], p)

def Laws.head_sound(sequence, version, committed, pending, marker, p):
  Laws.all_sound([sequence, version, committed || (pending && marker)], p)

def Laws.footprint_complete(count, limit, valid, p):
  Laws.all_complete([valid, Nat.is_le(count, limit)], p)

def Laws.transition_complete(retired, epoch, owner, closed, p):
  Laws.all_complete([Bool.not(retired), epoch, owner, Bool.not(closed)], p)

def Laws.close_complete(retiring, owner, retained, inflight, p):
  Laws.all_complete([retiring, Bool.not(owner), Bool.not(retained), Bool.not(inflight)], p)

def Laws.normalization_complete(canonical, plan, changed, accepted, p):
  Laws.all_complete([Bool.not(canonical), plan, changed, accepted], p)

def Laws.chunk_complete(chunk, p):
  match chunk:
    case K.Chunk{start, end, total, expectedStart, expectedTotal, length, complete}:
      Laws.all_complete(Laws.ChunkRequirements(chunk), p)

def Laws.certificate_complete(conditions, p):
  Laws.all_complete(conditions, p)

def Laws.resource_refinement(original, candidate):
  RP.normalizationRefinement(original, candidate)

def Laws.resource_output_valid(original, candidate):
  RP.normalizationValid(original, candidate)

def Laws.resource_conflict_exact(left, right, lo, ro, p):
  RP.conflictSound(left, right, lo, ro, p)

def Laws.resource_conflict_complete(left, right, lo, ro, p):
  RP.conflictComplete(left, right, lo, ro, p)

def Laws.resource_conflict_preserved(left, right, lp, rp, lo, ro, p):
  RP.conflictComplete(R.normalize(left, lp), R.normalize(right, rp), lo, ro, RP.preserveEvidence(left, right, lp, rp, lo, ro, RP.conflictSound(left, right, lo, ro, p)))

def Laws.resource_precision(original, candidate, quality, forward, backward):
  RP.normalizePrecise(original, candidate, quality, forward, backward)

def Laws.resource_identity(names, quality):
  RP.normalizeIdentity(names, quality)

def Laws.resource_recheck_idempotent(original, candidate):
  RP.normalizeRecheck(original, candidate)

def Laws.resource_groups_exact(left, right):
  RP.groupsExact(left, right, False{})

def Laws.resource_source_refinement(names):
  RP.sourceRefinesSelected(R.nonempty(names) && R.identitiesValid(names, True{}), names)

def Laws.resource_source_valid(names):
  RP.sourceValidSelected(R.nonempty(names) && R.identitiesValid(names, True{}), names, {==})

def Laws.resource_pair_sound(left, right, p):
  RP.pairSound(left, right, p)

def Laws.resource_pair_complete(left, right, p):
  RP.pairComplete(left, right, p)

def Laws.resource_source_complete(names, p):
  RP.sourceComplete(names, p)
