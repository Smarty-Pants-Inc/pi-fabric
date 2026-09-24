import Base

# Names are complete UTF-16 code-unit lists. Empty names also encode malformed
# non-string resources. Neither serialization nor this checker truncates input.
type Scope is Data:
  Unknown{}
  Exact{names: +List<+List<Nat>>}

type Effect is Data:
  Quiet{}
  Effect{ordered: Bool, scope: Scope}

def nameEqual(xs: +List<Nat>, ys: +List<Nat>, equal: Bool) -> Bool:
  match xs ys:
    case Nil{} Nil{}:
      equal
    case Nil{} Con{h, t}:
      False{}
    case Con{h, t} Nil{}:
      False{}
    case Con{a, at} Con{b, bt}:
      nameEqual(at, bt, equal && Nat.is_eq(a, b))

def nameNonempty(name: +List<Nat>) -> Bool:
  match name:
    case Nil{}:
      False{}
    case Con{h, t}:
      True{}

def nameBounded(name: +List<Nat>, remaining: Nat) -> Bool:
  match name remaining:
    case Nil{} _:
      True{}
    case Con{h, t} 0n:
      False{}
    case Con{h, t} 1n+p:
      nameBounded(t, p)

def identityValid(+name: +List<Nat>) -> Bool:
  nameNonempty(name) && Bool.not(nameEqual(name, [42n], True{}))

def nameValid(+name: +List<Nat>) -> Bool:
  nameNonempty(name) && (nameBounded(name, 256n) && Bool.not(nameEqual(name, [42n], True{})))

def member(xs: +List<+List<Nat>>, +name: +List<Nat>, found: Bool) -> Bool:
  match xs:
    case Nil{}:
      found
    case Con{h, t}:
      member(t, name, found || nameEqual(name, h, True{}))

def covered(xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, good: Bool) -> Bool:
  match xs:
    case Nil{}:
      good
    case Con{h, t}:
      covered(t, ys, good && member(ys, h, False{}))

def bounded(xs: +List<+List<Nat>>, remaining: Nat) -> Bool:
  match xs remaining:
    case Nil{} _:
      True{}
    case Con{h, t} 0n:
      False{}
    case Con{h, t} 1n+p:
      bounded(t, p)

def namesValid(xs: +List<+List<Nat>>, good: Bool) -> Bool:
  match xs:
    case Nil{}:
      good
    case Con{h, t}:
      namesValid(t, good && nameValid(h))

def identitiesValid(xs: +List<+List<Nat>>, good: Bool) -> Bool:
  match xs:
    case Nil{}:
      good
    case Con{h, t}:
      identitiesValid(t, good && identityValid(h))

def nonempty(xs: +List<+List<Nat>>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      True{}

def select(accepted: Bool, candidate: +List<+List<Nat>>) -> Scope:
  match accepted:
    case False{}:
      Unknown{}
    case True{}:
      Exact{candidate}

def source(+names: +List<+List<Nat>>) -> Scope:
  select(nonempty(names) && identitiesValid(names, True{}), names)

def quality(+candidate: +List<+List<Nat>>) -> Bool:
  nonempty(candidate) && (bounded(candidate, 64n) && namesValid(candidate, True{}))

def whenQuality(ok: Bool, +original: +List<+List<Nat>>, +candidate: +List<+List<Nat>>) -> Bool:
  match ok:
    case False{}:
      False{}
    case True{}:
      covered(original, candidate, True{}) && covered(candidate, original, True{})

def accepted(original: +List<+List<Nat>>, +candidate: +List<+List<Nat>>) -> Bool:
  whenQuality(quality(candidate), original, candidate)

def normalize(original: Scope, +candidate: +List<+List<Nat>>) -> Scope:
  match original:
    case Unknown{}:
      Unknown{}
    case Exact{names}:
      select(accepted(names, candidate), candidate)

def overlapNames(xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, found: Bool) -> Bool:
  match xs:
    case Nil{}:
      found
    case Con{h, t}:
      overlapNames(t, ys, found || member(ys, h, False{}))

def overlap(left: Scope, right: Scope) -> Bool:
  match left right:
    case Unknown{} _:
      True{}
    case Exact{xs} Unknown{}:
      True{}
    case Exact{xs} Exact{ys}:
      overlapNames(xs, ys, False{})

def conflict(left: Scope, right: Scope, lo: Bool, ro: Bool) -> Bool:
  (lo || ro) && overlap(left, right)

def pairConflict(left: Effect, right: Effect) -> Bool:
  match left right:
    case Quiet{} _:
      False{}
    case Effect{lo, ls} Quiet{}:
      False{}
    case Effect{lo, ls} Effect{ro, rs}:
      conflict(ls, rs, lo, ro)

def against(ys: +List<Effect>, +left: Effect, found: Bool) -> Bool:
  match ys:
    case Nil{}:
      found
    case Con{h, t}:
      against(t, left, found || pairConflict(left, h))

def groups(xs: +List<Effect>, +ys: +List<Effect>, found: Bool) -> Bool:
  match xs:
    case Nil{}:
      found
    case Con{h, t}:
      groups(t, ys, found || against(ys, h, False{}))
