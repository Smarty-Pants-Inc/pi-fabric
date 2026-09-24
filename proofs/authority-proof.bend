import Base
import ./authority-state.bend as A
import ./authority-spec.bend as S
import ./resources.bend as R
import ./resource-proof.bend as P
import ./resource-spec.bend as Set

# Closed universal laws over the executable state transitions. These do not
# prove JS issuer ownership, serialization, descriptor hashing, or event delivery.
def useSafe(+authority: A.Authority, +grant: +List<Nat>, p: {A.allows(authority, grant) == True{} : Bool}) -> S.CanUse(authority, grant):
  match authority:
    case A.Released{}:
      P.falseTrue(p)
    case A.Active{grants}:
      P.memberEvidence(grant, grants, p)

def useAccepted(+authority: A.Authority, +grant: +List<Nat>, p: S.CanUse(authority, grant)) -> {A.allows(authority, grant) == True{} : Bool}:
  match authority:
    case A.Released{}:
      Empty.absurd({False{} == True{} : Bool}, p)
    case A.Active{grants}:
      P.memberComplete(grants, grant, False{}, p)

def selectedSafe(+grants: +List<+List<Nat>>, +candidate: +List<+List<Nat>>, +accepted: Bool, p: {R.covered(candidate, grants, True{}) == accepted : Bool}) -> S.DerivationSafe(A.Active{grants}, candidate, A.select(accepted, candidate)):
  match accepted:
    case False{}:
      Unit{}
    case True{}:
      (P.coverage(candidate, grants, p), {==})

def deriveSafe(+parent: A.Authority, +candidate: +List<+List<Nat>>) -> S.DerivationSafe(parent, candidate, A.derive(parent, candidate)):
  match parent:
    case A.Released{}:
      Unit{}
    case A.Active{grants}:
      selectedSafe(grants, candidate, R.covered(candidate, grants, True{}), {==})

def deriveAccepted(+parent: A.Authority, +candidate: +List<+List<Nat>>, p: S.CanDerive(parent, candidate)) -> S.DerivationAccepted(candidate, A.derive(parent, candidate)):
  match parent:
    case A.Released{}:
      Empty.absurd(S.DerivationAccepted(candidate, A.Released{}), p)
    case A.Active{grants}:
      %Equal.sym(Bool, R.covered(candidate, grants, True{}), True{}, P.coveredComplete(candidate, grants, True{}, {==}, p)) : {A.select(_, candidate) == A.Active{candidate} : A.Authority}
      {==}

def derivedUseAccepted(+parent: A.Authority, +candidate: +List<+List<Nat>>, +grant: +List<Nat>, subset: S.CanDerive(parent, candidate), member: Set.Member(grant, candidate)) -> {A.allows(A.derive(parent, candidate), grant) == True{} : Bool}:
  %Equal.sym(A.Authority, A.derive(parent, candidate), A.Active{candidate}, deriveAccepted(parent, candidate, subset)) : {A.allows(_, grant) == True{} : Bool}
  P.memberComplete(candidate, grant, False{}, member)

def childUseSelected(+grants: +List<+List<Nat>>, +candidate: +List<+List<Nat>>, +grant: +List<Nat>, +accepted: Bool, p: {R.covered(candidate, grants, True{}) == accepted : Bool}, used: {A.allows(A.select(accepted, candidate), grant) == True{} : Bool}) -> Set.Member(grant, grants):
  match accepted:
    case False{}:
      Empty.absurd(Set.Member(grant, grants), P.falseTrue(used))
    case True{}:
      P.subsetMember(candidate, grants, grant, P.memberEvidence(grant, candidate, used), P.coverage(candidate, grants, p))

def childUseSafe(+parent: A.Authority, +candidate: +List<+List<Nat>>, +grant: +List<Nat>, used: {A.allows(A.derive(parent, candidate), grant) == True{} : Bool}) -> S.CanUse(parent, grant):
  match parent:
    case A.Released{}:
      P.falseTrue(used)
    case A.Active{grants}:
      childUseSelected(grants, candidate, grant, R.covered(candidate, grants, True{}), {==}, used)

def releasedCannotUse(+authority: A.Authority, +grant: +List<Nat>) -> {A.allows(A.release(authority), grant) == False{} : Bool}:
  {==}

def releasedCannotDerive(+authority: A.Authority, +candidate: +List<+List<Nat>>) -> {A.derive(A.release(authority), candidate) == A.Released{} : A.Authority}:
  {==}

def releaseNotLive(+authority: A.Authority) -> {A.live(A.release(authority)) == False{} : Bool}:
  {==}

def issueExact(+grants: +List<+List<Nat>>) -> {A.issue(grants) == A.Active{grants} : A.Authority}:
  {==}
