import Base
import ./resources.bend as R

# One grant is the complete UTF-16 text encoding of the canonical tuple
# [ref, provider, binding identity, generation, descriptor identity]. The JS
# tuple codec is trusted. No length bounds, prefixes, or hashes are computed here.
type Authority is Data:
  Released{}
  Active{grants: +List<+List<Nat>>}

def issue(grants: +List<+List<Nat>>) -> Authority:
  Active{grants}

def live(authority: Authority) -> Bool:
  match authority:
    case Released{}:
      False{}
    case Active{grants}:
      True{}

def allows(authority: Authority, grant: +List<Nat>) -> Bool:
  match authority:
    case Released{}:
      False{}
    case Active{grants}:
      R.member(grants, grant, False{})

def select(accepted: Bool, candidate: +List<+List<Nat>>) -> Authority:
  match accepted:
    case False{}:
      Released{}
    case True{}:
      Active{candidate}

def derive(parent: Authority, +candidate: +List<+List<Nat>>) -> Authority:
  match parent:
    case Released{}:
      Released{}
    case Active{grants}:
      select(R.covered(candidate, grants, True{}), candidate)

def release(authority: Authority) -> Authority:
  Released{}
