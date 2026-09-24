import Base
import ./authority-state.bend as A
import ./resource-spec.bend as Set

# Independent proof-relevant safety and acceptance specifications. Set.Member
# and Set.Subset use full propositional list equality, not Boolean predicates.
def CanUse(authority: A.Authority, grant: +List<Nat>) -> Type:
  match authority:
    case A.Released{}:
      Empty
    case A.Active{grants}:
      Set.Member(grant, grants)

def CanDerive(parent: A.Authority, candidate: +List<+List<Nat>>) -> Type:
  match parent:
    case A.Released{}:
      Empty
    case A.Active{grants}:
      Set.Subset(candidate, grants)

def DerivationSafe(+parent: A.Authority, candidate: +List<+List<Nat>>, child: A.Authority) -> Type:
  match child:
    case A.Released{}:
      Unit
    case A.Active{actual}:
      CanDerive(parent, actual) & {actual == candidate : +List<+List<Nat>>}

# Positive acceptance is exact output equality, ruling out deny-all kernels.
def DerivationAccepted(candidate: +List<+List<Nat>>, child: A.Authority) -> Type:
  {child == A.Active{candidate} : A.Authority}
