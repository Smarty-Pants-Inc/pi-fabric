import Base
import ./resources.bend as R

# Independent, proof-relevant set semantics, not the executable Boolean checker.
def Member(+name: +List<Nat>, xs: +List<+List<Nat>>) -> Type:
  match xs:
    case Nil{}:
      Empty
    case Con{h, t}:
      {name == h : +List<Nat>} | Member(name, t)

def Subset(xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>) -> Type:
  match xs:
    case Nil{}:
      Unit
    case Con{h, t}:
      Member(h, ys) & Subset(t, ys)

def AtMost(xs: +List<+List<Nat>>, limit: Nat) -> Type:
  match xs limit:
    case Nil{} _:
      Unit
    case Con{h, t} 0n:
      Empty
    case Con{h, t} 1n+p:
      AtMost(t, p)

def Nonempty(xs: +List<+List<Nat>>) -> Type:
  match xs:
    case Nil{}:
      Empty
    case Con{h, t}:
      Unit

def NameNonempty(name: +List<Nat>) -> Type:
  match name:
    case Nil{}:
      Empty
    case Con{h, t}:
      Unit

def NameAtMost(name: +List<Nat>, limit: Nat) -> Type:
  match name limit:
    case Nil{} _:
      Unit
    case Con{h, t} 0n:
      Empty
    case Con{h, t} 1n+p:
      NameAtMost(t, p)

def NameValid(+name: +List<Nat>) -> Type:
  NameNonempty(name) & (NameAtMost(name, 256n) & {name != [42n] : +List<Nat>})

def NamesValid(xs: +List<+List<Nat>>) -> Type:
  match xs:
    case Nil{}:
      Unit
    case Con{h, t}:
      NameValid(h) & NamesValid(t)

def Quality(+names: +List<+List<Nat>>) -> Type:
  Nonempty(names) & (AtMost(names, 64n) & NamesValid(names))

def OutputValid(scope: R.Scope) -> Type:
  match scope:
    case R.Unknown{}:
      Unit
    case R.Exact{names}:
      Quality(names)

def Refines(original: R.Scope, normalized: R.Scope) -> Type:
  match original normalized:
    case R.Unknown{} R.Exact{ys}:
      Empty
    case R.Exact{xs} R.Exact{ys}:
      Subset(xs, ys) & Subset(ys, xs)
    case _ R.Unknown{}:
      Unit

def Shared(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>) -> Type:
  &name: +List<Nat> -> Member(name, xs) & Member(name, ys)

def Overlap(left: R.Scope, right: R.Scope) -> Type:
  match left right:
    case R.Unknown{} _:
      Unit
    case R.Exact{xs} R.Unknown{}:
      Unit
    case R.Exact{xs} R.Exact{ys}:
      Shared(xs, ys)

def Conflicts(+left: R.Scope, +right: R.Scope, lo: Bool, ro: Bool) -> Type:
  ({lo == True{} : Bool} | {ro == True{} : Bool}) & Overlap(left, right)

def Against(ys: +List<R.Effect>, +left: R.Effect) -> Bool:
  match ys:
    case Nil{}:
      False{}
    case Con{h, t}:
      R.pairConflict(left, h) || Against(t, left)

def Groups(xs: +List<R.Effect>, +ys: +List<R.Effect>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{h, t}:
      Against(ys, h) || Groups(t, ys)

def IdentityValid(+name: +List<Nat>) -> Type:
  NameNonempty(name) & {name != [42n] : +List<Nat>}

def IdentitiesValid(xs: +List<+List<Nat>>) -> Type:
  match xs:
    case Nil{}:
      Unit
    case Con{h, t}:
      IdentityValid(h) & IdentitiesValid(t)

def SourceValid(scope: R.Scope) -> Type:
  match scope:
    case R.Unknown{}:
      Unit
    case R.Exact{names}:
      Nonempty(names) & IdentitiesValid(names)

def EffectConflicts(left: R.Effect, right: R.Effect) -> Type:
  match left right:
    case R.Quiet{} _:
      Empty
    case R.Effect{lo, ls} R.Quiet{}:
      Empty
    case R.Effect{lo, ls} R.Effect{ro, rs}:
      Conflicts(ls, rs, lo, ro)
