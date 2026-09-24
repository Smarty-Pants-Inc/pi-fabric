import Base
import ./resources.bend as R
import ./resource-spec.bend as S

# Helpers are proof-only; none are linked into the executable artifact.
def discriminate(b: Bool) -> Type:
  match b:
    case False{}:
      Unit
    case True{}:
      Empty

def falseTrue(p: {False{} == True{} : Bool}) -> Empty:
  %p : discriminate(_)
  Unit{}

def orLeft(+a: Bool, +b: Bool, p: {a == True{} : Bool}) -> {a || b == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({False{} || b == True{} : Bool}, falseTrue(p))
    case True{}:
      {==}

def orRight(+a: Bool, +b: Bool, p: {b == True{} : Bool}) -> {a || b == True{} : Bool}:
  match a:
    case False{}:
      p
    case True{}:
      {==}

def orCases(+a: Bool, +b: Bool, p: {a || b == True{} : Bool}) -> {a == True{} : Bool} | {b == True{} : Bool}:
  match a:
    case False{}:
      Inr{p}
    case True{}:
      Inl{{==}}

def andCases(+a: Bool, +b: Bool, p: {a && b == True{} : Bool}) -> {a == True{} : Bool} & {b == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({False{} == True{} : Bool} & {b == True{} : Bool}, falseTrue(p))
    case True{}:
      ({==}, p)

def andMake(+a: Bool, +b: Bool, ap: {a == True{} : Bool}, bp: {b == True{} : Bool}) -> {a && b == True{} : Bool}:
  match a:
    case False{}:
      Empty.absurd({False{} && b == True{} : Bool}, falseTrue(ap))
    case True{}:
      bp

def natRefl(+n: Nat) -> {Nat.is_eq(n, n) == True{} : Bool}:
  match n:
    case 0n:
      {==}
    case 1n+p:
      natRefl(p)

def natExact(+a: Nat, +b: Nat, p: {Nat.is_eq(a, b) == True{} : Bool}) -> {a == b : Nat}:
  match a b:
    case 0n 0n:
      {==}
    case 0n 1n+bp:
      Empty.absurd({0n == 1n+bp : Nat}, falseTrue(p))
    case 1n+ap 0n:
      Empty.absurd({1n+ap == 0n : Nat}, falseTrue(p))
    case 1n+ap 1n+bp:
      %natExact(ap, bp, p) : {1n+ap == 1n+_ : Nat}
      {==}

def second(-A: Type, -B: Type, p: A & B) -> B:
  (a, b) = p
  b

def andUse(-T: Type, +a: Bool, +b: Bool, p: {a && b == True{} : Bool}, k: {a == True{} : Bool} -> {b == True{} : Bool} -> T) -> T:
  match a:
    case False{}:
      Empty.absurd(T, falseTrue(p))
    case True{}:
      k({==}, p)

def consEqual(+a: Nat, +at: +List<Nat>, +b: Nat, +bt: +List<Nat>, head: {a == b : Nat}, tail: {at == bt : +List<Nat>}) -> {a <> at == b <> bt : +List<Nat>}:
  %tail : {a <> at == b <> _ : +List<Nat>}
  %head : {a <> at == _ <> at : +List<Nat>}
  {==}

def nameReflWith(+xs: +List<Nat>, +equal: Bool, p: {equal == True{} : Bool}) -> {R.nameEqual(xs, xs, equal) == True{} : Bool}:
  match xs:
    case Nil{}:
      p
    case Con{h, t}:
      nameReflWith(t, equal && Nat.is_eq(h, h), andMake(equal, Nat.is_eq(h, h), p, natRefl(h)))

def nameRefl(+xs: +List<Nat>) -> {R.nameEqual(xs, xs, True{}) == True{} : Bool}:
  nameReflWith(xs, True{}, {==})

def nameSoundStep(+a: Nat, +at: +List<Nat>, +b: Nat, +bt: +List<Nat>, +equal: Bool, p: {equal && Nat.is_eq(a, b) == True{} : Bool} & {at == bt : +List<Nat>}) -> {equal == True{} : Bool} & {a <> at == b <> bt : +List<Nat>}:
  (qp, tp) = p
  andUse({equal == True{} : Bool} & {a <> at == b <> bt : +List<Nat>}, equal, Nat.is_eq(a, b), qp, ep => hp => (ep, consEqual(a, at, b, bt, natExact(a, b, hp), tp)))

def nameSound(+xs: +List<Nat>, +ys: +List<Nat>, +equal: Bool, p: {R.nameEqual(xs, ys, equal) == True{} : Bool}) -> {equal == True{} : Bool} & {xs == ys : +List<Nat>}:
  match xs ys:
    case Nil{} Nil{}:
      (p, {==})
    case Nil{} Con{h, t}:
      Empty.absurd({equal == True{} : Bool} & {Nil{} == h <> t : +List<Nat>}, falseTrue(p))
    case Con{h, t} Nil{}:
      Empty.absurd({equal == True{} : Bool} & {h <> t == Nil{} : +List<Nat>}, falseTrue(p))
    case Con{a, at} Con{b, bt}:
      nameSoundStep(a, at, b, bt, equal, nameSound(at, bt, equal && Nat.is_eq(a, b), p))

def nameExact(+xs: +List<Nat>, +ys: +List<Nat>, p: {R.nameEqual(xs, ys, True{}) == True{} : Bool}) -> {xs == ys : +List<Nat>}:
  second({True{} == True{} : Bool}, {xs == ys : +List<Nat>}, nameSound(xs, ys, True{}, p))

def nameFromEq(+xs: +List<Nat>, +ys: +List<Nat>, p: {xs == ys : +List<Nat>}) -> {R.nameEqual(xs, ys, True{}) == True{} : Bool}:
  %p : {R.nameEqual(xs, _, True{}) == True{} : Bool}
  nameRefl(xs)

def notBoth(+b: Bool, np: {Bool.not(b) == True{} : Bool}, p: {b == True{} : Bool}) -> Empty:
  match b:
    case False{}:
      falseTrue(p)
    case True{}:
      falseTrue(np)

def nameDifferent(+xs: +List<Nat>, +ys: +List<Nat>, p: {Bool.not(R.nameEqual(xs, ys, True{})) == True{} : Bool}) -> {xs != ys : +List<Nat>}:
  e => notBoth(R.nameEqual(xs, ys, True{}), p, nameFromEq(xs, ys, e))

def nameInequality(+xs: +List<Nat>, +ys: +List<Nat>, +same: Bool, p: {R.nameEqual(xs, ys, True{}) == same : Bool}, neq: {xs != ys : +List<Nat>}) -> {Bool.not(same) == True{} : Bool}:
  match same:
    case False{}:
      {==}
    case True{}:
      Empty.absurd({False{} == True{} : Bool}, neq(nameExact(xs, ys, p)))

def nameBoundSound(+xs: +List<Nat>, +limit: Nat, p: {R.nameBounded(xs, limit) == True{} : Bool}) -> S.NameAtMost(xs, limit):
  match xs limit:
    case Nil{} _:
      Unit{}
    case Con{h, t} 0n:
      falseTrue(p)
    case Con{h, t} 1n+l:
      nameBoundSound(t, l, p)

def nameBoundComplete(+xs: +List<Nat>, +limit: Nat, p: S.NameAtMost(xs, limit)) -> {R.nameBounded(xs, limit) == True{} : Bool}:
  match xs limit:
    case Nil{} _:
      {==}
    case Con{h, t} 0n:
      Empty.absurd({False{} == True{} : Bool}, p)
    case Con{h, t} 1n+l:
      nameBoundComplete(t, l, p)

def nameNonemptySound(+xs: +List<Nat>, p: {R.nameNonempty(xs) == True{} : Bool}) -> S.NameNonempty(xs):
  match xs:
    case Nil{}:
      falseTrue(p)
    case Con{h, t}:
      Unit{}

def nameNonemptyComplete(+xs: +List<Nat>, p: S.NameNonempty(xs)) -> {R.nameNonempty(xs) == True{} : Bool}:
  match xs:
    case Nil{}:
      Empty.absurd({False{} == True{} : Bool}, p)
    case Con{h, t}:
      {==}

def nameValidSound(+name: +List<Nat>, p: {R.nameValid(name) == True{} : Bool}) -> S.NameValid(name):
  andUse(S.NameValid(name), R.nameNonempty(name), R.nameBounded(name, 256n) && Bool.not(R.nameEqual(name, [42n], True{})), p, np => rest =>
    andUse(S.NameValid(name), R.nameBounded(name, 256n), Bool.not(R.nameEqual(name, [42n], True{})), rest, bp => sp =>
      (nameNonemptySound(name, np), (nameBoundSound(name, 256n, bp), nameDifferent(name, [42n], sp)))))

def nameValidComplete(+name: +List<Nat>, p: S.NameValid(name)) -> {R.nameValid(name) == True{} : Bool}:
  (np, rest) = p
  (bp, neq) = rest
  andMake(R.nameNonempty(name), R.nameBounded(name, 256n) && Bool.not(R.nameEqual(name, [42n], True{})), nameNonemptyComplete(name, np),
    andMake(R.nameBounded(name, 256n), Bool.not(R.nameEqual(name, [42n], True{})), nameBoundComplete(name, 256n, bp), nameInequality(name, [42n], R.nameEqual(name, [42n], True{}), {==}, neq)))

def memberTrue(+xs: +List<+List<Nat>>, +name: +List<Nat>) -> {R.member(xs, name, True{}) == True{} : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      memberTrue(t, name)

def memberFound(+xs: +List<+List<Nat>>, +name: +List<Nat>, +found: Bool, p: {found == True{} : Bool}) -> {R.member(xs, name, found) == True{} : Bool}:
  match found:
    case False{}:
      Empty.absurd({R.member(xs, name, False{}) == True{} : Bool}, falseTrue(p))
    case True{}:
      memberTrue(xs, name)

def memberComplete(+xs: +List<+List<Nat>>, +name: +List<Nat>, +found: Bool, p: S.Member(name, xs)) -> {R.member(xs, name, found) == True{} : Bool}:
  match xs:
    case Nil{}:
      Empty.absurd({found == True{} : Bool}, p)
    case Con{h, t}:
      match p:
        case Inl{hp}:
          %hp : {R.member(t, name, found || R.nameEqual(name, _, True{})) == True{} : Bool}
          memberFound(t, name, found || R.nameEqual(name, name, True{}), orRight(found, R.nameEqual(name, name, True{}), nameRefl(name)))
        case Inr{tp}:
          memberComplete(t, name, found || R.nameEqual(name, h, True{}), tp)

def memberHeadCases(+name: +List<Nat>, +h: +List<Nat>, +t: +List<+List<Nat>>, +found: Bool, p: {found == True{} : Bool} | {R.nameEqual(name, h, True{}) == True{} : Bool}) -> {found == True{} : Bool} | S.Member(name, h <> t):
  match p:
    case Inl{fp}:
      Inl{fp}
    case Inr{hp}:
      Inr{Inl{nameExact(name, h, hp)}}

def memberLift(+name: +List<Nat>, +h: +List<Nat>, +t: +List<+List<Nat>>, +found: Bool, p: {found || R.nameEqual(name, h, True{}) == True{} : Bool} | S.Member(name, t)) -> {found == True{} : Bool} | S.Member(name, h <> t):
  match p:
    case Inl{hp}:
      memberHeadCases(name, h, t, found, orCases(found, R.nameEqual(name, h, True{}), hp))
    case Inr{tp}:
      Inr{Inr{tp}}

def memberSound(+xs: +List<+List<Nat>>, +name: +List<Nat>, +found: Bool, p: {R.member(xs, name, found) == True{} : Bool}) -> {found == True{} : Bool} | S.Member(name, xs):
  match xs:
    case Nil{}:
      Inl{p}
    case Con{h, t}:
      memberLift(name, h, t, found, memberSound(t, name, found || R.nameEqual(name, h, True{}), p))

def memberNotFound(+name: +List<Nat>, +xs: +List<+List<Nat>>, p: {False{} == True{} : Bool} | S.Member(name, xs)) -> S.Member(name, xs):
  match p:
    case Inl{fp}:
      Empty.absurd(S.Member(name, xs), falseTrue(fp))
    case Inr{mp}:
      mp

def memberEvidence(+name: +List<Nat>, +xs: +List<+List<Nat>>, p: {R.member(xs, name, False{}) == True{} : Bool}) -> S.Member(name, xs):
  memberNotFound(name, xs, memberSound(xs, name, False{}, p))

def coveredStep(+h: +List<Nat>, +t: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +good: Bool, p: {good && R.member(ys, h, False{}) == True{} : Bool} & S.Subset(t, ys)) -> {good == True{} : Bool} & S.Subset(h <> t, ys):
  (gp, tp) = p
  andUse({good == True{} : Bool} & S.Subset(h <> t, ys), good, R.member(ys, h, False{}), gp, gp => hp => (gp, (memberEvidence(h, ys, hp), tp)))

def coveredSound(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +good: Bool, p: {R.covered(xs, ys, good) == True{} : Bool}) -> {good == True{} : Bool} & S.Subset(xs, ys):
  match xs:
    case Nil{}:
      (p, Unit{})
    case Con{h, t}:
      coveredStep(h, t, ys, good, coveredSound(t, ys, good && R.member(ys, h, False{}), p))

def coverage(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, p: {R.covered(xs, ys, True{}) == True{} : Bool}) -> S.Subset(xs, ys):
  second({True{} == True{} : Bool}, S.Subset(xs, ys), coveredSound(xs, ys, True{}, p))

def coveredComplete(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +good: Bool, gp: {good == True{} : Bool}, p: S.Subset(xs, ys)) -> {R.covered(xs, ys, good) == True{} : Bool}:
  match xs:
    case Nil{}:
      gp
    case Con{h, t}:
      (hp, tp) = p
      coveredComplete(t, ys, good && R.member(ys, h, False{}), andMake(good, R.member(ys, h, False{}), gp, memberComplete(ys, h, False{}, hp)), tp)

def subsetMember(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +name: +List<Nat>, member: S.Member(name, xs), subset: S.Subset(xs, ys)) -> S.Member(name, ys):
  match xs:
    case Nil{}:
      Empty.absurd(S.Member(name, ys), member)
    case Con{h, t}:
      match member:
        case Inl{e}:
          (hp, tp) = subset
          %Equal.sym(+List<Nat>, name, h, e) : S.Member(_, ys)
          hp
        case Inr{mp}:
          (hp, tp) = subset
          subsetMember(t, ys, name, mp, tp)

def subsetCons(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +h: +List<Nat>, p: S.Subset(xs, ys)) -> S.Subset(xs, h <> ys):
  match xs:
    case Nil{}:
      Unit{}
    case Con{x, t}:
      (xp, tp) = p
      (Inr{xp}, subsetCons(t, ys, h, tp))

def subsetRefl(+xs: +List<+List<Nat>>) -> S.Subset(xs, xs):
  match xs:
    case Nil{}:
      Unit{}
    case Con{h, t}:
      (Inl{{==}}, subsetCons(t, t, h, subsetRefl(t)))

def boundedSound(+xs: +List<+List<Nat>>, +limit: Nat, p: {R.bounded(xs, limit) == True{} : Bool}) -> S.AtMost(xs, limit):
  match xs limit:
    case Nil{} _:
      Unit{}
    case Con{h, t} 0n:
      falseTrue(p)
    case Con{h, t} 1n+l:
      boundedSound(t, l, p)

def boundedComplete(+xs: +List<+List<Nat>>, +limit: Nat, p: S.AtMost(xs, limit)) -> {R.bounded(xs, limit) == True{} : Bool}:
  match xs limit:
    case Nil{} _:
      {==}
    case Con{h, t} 0n:
      Empty.absurd({False{} == True{} : Bool}, p)
    case Con{h, t} 1n+l:
      boundedComplete(t, l, p)

def nonemptySound(+xs: +List<+List<Nat>>, p: {R.nonempty(xs) == True{} : Bool}) -> S.Nonempty(xs):
  match xs:
    case Nil{}:
      falseTrue(p)
    case Con{h, t}:
      Unit{}

def nonemptyComplete(+xs: +List<+List<Nat>>, p: S.Nonempty(xs)) -> {R.nonempty(xs) == True{} : Bool}:
  match xs:
    case Nil{}:
      Empty.absurd({False{} == True{} : Bool}, p)
    case Con{h, t}:
      {==}

def namesStep(+h: +List<Nat>, +t: +List<+List<Nat>>, +good: Bool, p: {good && R.nameValid(h) == True{} : Bool} & S.NamesValid(t)) -> {good == True{} : Bool} & S.NamesValid(h <> t):
  (gp, tp) = p
  andUse({good == True{} : Bool} & S.NamesValid(h <> t), good, R.nameValid(h), gp, gp => hp => (gp, (nameValidSound(h, hp), tp)))

def namesSound(+xs: +List<+List<Nat>>, +good: Bool, p: {R.namesValid(xs, good) == True{} : Bool}) -> {good == True{} : Bool} & S.NamesValid(xs):
  match xs:
    case Nil{}:
      (p, Unit{})
    case Con{h, t}:
      namesStep(h, t, good, namesSound(t, good && R.nameValid(h), p))

def namesComplete(+xs: +List<+List<Nat>>, +good: Bool, gp: {good == True{} : Bool}, p: S.NamesValid(xs)) -> {R.namesValid(xs, good) == True{} : Bool}:
  match xs:
    case Nil{}:
      gp
    case Con{h, t}:
      (hp, tp) = p
      namesComplete(t, good && R.nameValid(h), andMake(good, R.nameValid(h), gp, nameValidComplete(h, hp)), tp)

def qualitySound(+xs: +List<+List<Nat>>, p: {R.quality(xs) == True{} : Bool}) -> S.Quality(xs):
  andUse(S.Quality(xs), R.nonempty(xs), R.bounded(xs, 64n) && R.namesValid(xs, True{}), p, np => rest =>
    andUse(S.Quality(xs), R.bounded(xs, 64n), R.namesValid(xs, True{}), rest, bp => vp =>
      (nonemptySound(xs, np), (boundedSound(xs, 64n, bp), second({True{} == True{} : Bool}, S.NamesValid(xs), namesSound(xs, True{}, vp))))))

def qualityComplete(+xs: +List<+List<Nat>>, p: S.Quality(xs)) -> {R.quality(xs) == True{} : Bool}:
  (np, rest) = p
  (bp, vp) = rest
  andMake(R.nonempty(xs), R.bounded(xs, 64n) && R.namesValid(xs, True{}), nonemptyComplete(xs, np), andMake(R.bounded(xs, 64n), R.namesValid(xs, True{}), boundedComplete(xs, 64n, bp), namesComplete(xs, True{}, {==}, vp)))

def whenQualityExact(+ok: Bool, +xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>) -> {R.whenQuality(ok, xs, ys) == ok && (R.covered(xs, ys, True{}) && R.covered(ys, xs, True{})) : Bool}:
  match ok:
    case False{}:
      {==}
    case True{}:
      {==}

def acceptedView(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, p: {R.accepted(xs, ys) == True{} : Bool}) -> {R.quality(ys) && (R.covered(xs, ys, True{}) && R.covered(ys, xs, True{})) == True{} : Bool}:
  %whenQualityExact(R.quality(ys), xs, ys) : {_ == True{} : Bool}
  p

def acceptedSound(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, p: {R.accepted(xs, ys) == True{} : Bool}) -> S.Quality(ys) & (S.Subset(xs, ys) & S.Subset(ys, xs)):
  andUse(S.Quality(ys) & (S.Subset(xs, ys) & S.Subset(ys, xs)), R.quality(ys), R.covered(xs, ys, True{}) && R.covered(ys, xs, True{}), acceptedView(xs, ys, p), qp => cp =>
    andUse(S.Quality(ys) & (S.Subset(xs, ys) & S.Subset(ys, xs)), R.covered(xs, ys, True{}), R.covered(ys, xs, True{}), cp, fp => bp =>
      (qualitySound(ys, qp), (coverage(xs, ys, fp), coverage(ys, xs, bp)))))

def acceptedComplete(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, quality: S.Quality(ys), forward: S.Subset(xs, ys), backward: S.Subset(ys, xs)) -> {R.accepted(xs, ys) == True{} : Bool}:
  %Equal.sym(Bool, R.accepted(xs, ys), R.quality(ys) && (R.covered(xs, ys, True{}) && R.covered(ys, xs, True{})), whenQualityExact(R.quality(ys), xs, ys)) : {_ == True{} : Bool}
  andMake(R.quality(ys), R.covered(xs, ys, True{}) && R.covered(ys, xs, True{}), qualityComplete(ys, quality), andMake(R.covered(xs, ys, True{}), R.covered(ys, xs, True{}), coveredComplete(xs, ys, True{}, {==}, forward), coveredComplete(ys, xs, True{}, {==}, backward)))

def sharedTail(+h: +List<Nat>, +t: +List<+List<Nat>>, +ys: +List<+List<Nat>>, p: S.Shared(t, ys)) -> S.Shared(h <> t, ys):
  (name, both) = p
  (lp, rp) = both
  (name, (Inr{lp}, rp))

def overlapHead(+h: +List<Nat>, +t: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +found: Bool, p: {found == True{} : Bool} | {R.member(ys, h, False{}) == True{} : Bool}) -> {found == True{} : Bool} | S.Shared(h <> t, ys):
  match p:
    case Inl{fp}:
      Inl{fp}
    case Inr{hp}:
      Inr{(h, (Inl{{==}}, memberEvidence(h, ys, hp)))}

def overlapLift(+h: +List<Nat>, +t: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +found: Bool, p: {found || R.member(ys, h, False{}) == True{} : Bool} | S.Shared(t, ys)) -> {found == True{} : Bool} | S.Shared(h <> t, ys):
  match p:
    case Inl{hp}:
      overlapHead(h, t, ys, found, orCases(found, R.member(ys, h, False{}), hp))
    case Inr{tp}:
      Inr{sharedTail(h, t, ys, tp)}

def overlapSound(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +found: Bool, p: {R.overlapNames(xs, ys, found) == True{} : Bool}) -> {found == True{} : Bool} | S.Shared(xs, ys):
  match xs:
    case Nil{}:
      Inl{p}
    case Con{h, t}:
      overlapLift(h, t, ys, found, overlapSound(t, ys, found || R.member(ys, h, False{}), p))

def overlapNotFound(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, p: {False{} == True{} : Bool} | S.Shared(xs, ys)) -> S.Shared(xs, ys):
  match p:
    case Inl{fp}:
      Empty.absurd(S.Shared(xs, ys), falseTrue(fp))
    case Inr{sp}:
      sp

def overlapTrue(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>) -> {R.overlapNames(xs, ys, True{}) == True{} : Bool}:
  match xs:
    case Nil{}:
      {==}
    case Con{h, t}:
      overlapTrue(t, ys)

def overlapFound(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +found: Bool, p: {found == True{} : Bool}) -> {R.overlapNames(xs, ys, found) == True{} : Bool}:
  match found:
    case False{}:
      Empty.absurd({R.overlapNames(xs, ys, found) == True{} : Bool}, falseTrue(p))
    case True{}:
      overlapTrue(xs, ys)

def overlapMember(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +name: +List<Nat>, +found: Bool, lp: S.Member(name, xs), rp: S.Member(name, ys)) -> {R.overlapNames(xs, ys, found) == True{} : Bool}:
  match xs:
    case Nil{}:
      Empty.absurd({found == True{} : Bool}, lp)
    case Con{h, t}:
      match lp:
        case Inl{e}:
          %e : {R.overlapNames(t, ys, found || R.member(ys, _, False{})) == True{} : Bool}
          overlapFound(t, ys, found || R.member(ys, name, False{}), orRight(found, R.member(ys, name, False{}), memberComplete(ys, name, False{}, rp)))
        case Inr{tp}:
          overlapMember(t, ys, name, found || R.member(ys, h, False{}), tp, rp)

def overlapComplete(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, p: S.Shared(xs, ys)) -> {R.overlapNames(xs, ys, False{}) == True{} : Bool}:
  (name, both) = p
  (lp, rp) = both
  overlapMember(xs, ys, name, False{}, lp, rp)

def overlapAllSound(+left: R.Scope, +right: R.Scope, p: {R.overlap(left, right) == True{} : Bool}) -> S.Overlap(left, right):
  match left right:
    case R.Unknown{} _:
      Unit{}
    case R.Exact{xs} R.Unknown{}:
      Unit{}
    case R.Exact{xs} R.Exact{ys}:
      overlapNotFound(xs, ys, overlapSound(xs, ys, False{}, p))

def overlapAllComplete(+left: R.Scope, +right: R.Scope, p: S.Overlap(left, right)) -> {R.overlap(left, right) == True{} : Bool}:
  match left right:
    case R.Unknown{} _:
      {==}
    case R.Exact{xs} R.Unknown{}:
      {==}
    case R.Exact{xs} R.Exact{ys}:
      overlapComplete(xs, ys, p)

def conflictSound(+left: R.Scope, +right: R.Scope, +lo: Bool, +ro: Bool, p: {R.conflict(left, right, lo, ro) == True{} : Bool}) -> S.Conflicts(left, right, lo, ro):
  andUse(S.Conflicts(left, right, lo, ro), lo || ro, R.overlap(left, right), p, op => sp => (orCases(lo, ro, op), overlapAllSound(left, right, sp)))

def orEvidence(+a: Bool, +b: Bool, p: {a == True{} : Bool} | {b == True{} : Bool}) -> {a || b == True{} : Bool}:
  match p:
    case Inl{ap}:
      orLeft(a, b, ap)
    case Inr{bp}:
      orRight(a, b, bp)

def conflictComplete(+left: R.Scope, +right: R.Scope, +lo: Bool, +ro: Bool, p: S.Conflicts(left, right, lo, ro)) -> {R.conflict(left, right, lo, ro) == True{} : Bool}:
  (op, sp) = p
  andMake(lo || ro, R.overlap(left, right), orEvidence(lo, ro, op), overlapAllComplete(left, right, sp))

def selectedRefines(+decision: Bool, +original: +List<+List<Nat>>, +candidate: +List<+List<Nat>>, p: {R.accepted(original, candidate) == decision : Bool}) -> S.Refines(R.Exact{original}, R.select(decision, candidate)):
  match decision:
    case False{}:
      Unit{}
    case True{}:
      second(S.Quality(candidate), S.Subset(original, candidate) & S.Subset(candidate, original), acceptedSound(original, candidate, p))

def first(-A: Type, -B: Type, p: A & B) -> A:
  (a, b) = p
  a

def selectedValid(+decision: Bool, +original: +List<+List<Nat>>, +candidate: +List<+List<Nat>>, p: {R.accepted(original, candidate) == decision : Bool}) -> S.OutputValid(R.select(decision, candidate)):
  match decision:
    case False{}:
      Unit{}
    case True{}:
      first(S.Quality(candidate), S.Subset(original, candidate) & S.Subset(candidate, original), acceptedSound(original, candidate, p))

def normalizationRefinement(+original: R.Scope, +candidate: +List<+List<Nat>>) -> S.Refines(original, R.normalize(original, candidate)):
  match original:
    case R.Unknown{}:
      Unit{}
    case R.Exact{xs}:
      selectedRefines(R.accepted(xs, candidate), xs, candidate, {==})

def normalizationValid(+original: R.Scope, +candidate: +List<+List<Nat>>) -> S.OutputValid(R.normalize(original, candidate)):
  match original:
    case R.Unknown{}:
      Unit{}
    case R.Exact{xs}:
      selectedValid(R.accepted(xs, candidate), xs, candidate, {==})

def sharedRefines(+xs: +List<+List<Nat>>, +nx: +List<+List<Nat>>, +ys: +List<+List<Nat>>, +ny: +List<+List<Nat>>, left: S.Subset(xs, nx) & S.Subset(nx, xs), right: S.Subset(ys, ny) & S.Subset(ny, ys), p: S.Shared(xs, ys)) -> S.Shared(nx, ny):
  (lf, lb) = left
  (rf, rb) = right
  (+name, both) = p
  (lp, rp) = both
  (name, (subsetMember(xs, nx, name, lp, lf), subsetMember(ys, ny, name, rp, rf)))

def overlapRefines(+left: R.Scope, +nl: R.Scope, +right: R.Scope, +nr: R.Scope, lp: S.Refines(left, nl), rp: S.Refines(right, nr), p: S.Overlap(left, right)) -> S.Overlap(nl, nr):
  match left nl right nr:
    case _ R.Unknown{} _ _:
      Unit{}
    case _ R.Exact{nx} _ R.Unknown{}:
      Unit{}
    case R.Unknown{} R.Exact{xs} _ R.Exact{ys}:
      Empty.absurd(S.Shared(xs, ys), lp)
    case R.Exact{xs} R.Exact{nx} R.Unknown{} R.Exact{ny}:
      Empty.absurd(S.Shared(nx, ny), rp)
    case R.Exact{xs} R.Exact{nx} R.Exact{ys} R.Exact{ny}:
      sharedRefines(xs, nx, ys, ny, lp, rp, p)

def preserveEvidence(+left: R.Scope, +right: R.Scope, +lp: +List<+List<Nat>>, +rp: +List<+List<Nat>>, +lo: Bool, +ro: Bool, p: S.Conflicts(left, right, lo, ro)) -> S.Conflicts(R.normalize(left, lp), R.normalize(right, rp), lo, ro):
  (op, sp) = p
  (op, overlapRefines(left, R.normalize(left, lp), right, R.normalize(right, rp), normalizationRefinement(left, lp), normalizationRefinement(right, rp), sp))

def normalizePrecise(+xs: +List<+List<Nat>>, +ys: +List<+List<Nat>>, quality: S.Quality(ys), forward: S.Subset(xs, ys), backward: S.Subset(ys, xs)) -> {R.normalize(R.Exact{xs}, ys) == R.Exact{ys} : R.Scope}:
  %Equal.sym(Bool, R.accepted(xs, ys), True{}, acceptedComplete(xs, ys, quality, forward, backward)) : {R.select(_, ys) == R.Exact{ys} : R.Scope}
  {==}

def normalizeIdentity(+xs: +List<+List<Nat>>, quality: S.Quality(xs)) -> {R.normalize(R.Exact{xs}, xs) == R.Exact{xs} : R.Scope}:
  normalizePrecise(xs, xs, quality, subsetRefl(xs), subsetRefl(xs))

def recheckSelected(+decision: Bool, +original: +List<+List<Nat>>, +candidate: +List<+List<Nat>>, p: {R.accepted(original, candidate) == decision : Bool}) -> {R.normalize(R.select(decision, candidate), candidate) == R.select(decision, candidate) : R.Scope}:
  match decision:
    case False{}:
      {==}
    case True{}:
      normalizeIdentity(candidate, first(S.Quality(candidate), S.Subset(original, candidate) & S.Subset(candidate, original), acceptedSound(original, candidate, p)))

def normalizeRecheck(+original: R.Scope, +candidate: +List<+List<Nat>>) -> {R.normalize(R.normalize(original, candidate), candidate) == R.normalize(original, candidate) : R.Scope}:
  match original:
    case R.Unknown{}:
      {==}
    case R.Exact{xs}:
      recheckSelected(R.accepted(xs, candidate), xs, candidate, {==})

def orFalse(+a: Bool) -> {a || False{} == a : Bool}:
  match a:
    case False{}:
      {==}
    case True{}:
      {==}

def orAssoc(+a: Bool, +b: Bool, +c: Bool) -> {(a || b) || c == a || (b || c) : Bool}:
  match a:
    case False{}:
      {==}
    case True{}:
      {==}

def againstExact(+ys: +List<R.Effect>, +left: R.Effect, +found: Bool) -> {R.against(ys, left, found) == found || S.Against(ys, left) : Bool}:
  match ys:
    case Nil{}:
      Equal.sym(Bool, found || False{}, found, orFalse(found))
    case Con{h, t}:
      %orAssoc(found, R.pairConflict(left, h), S.Against(t, left)) : {R.against(t, left, found || R.pairConflict(left, h)) == _ : Bool}
      againstExact(t, left, found || R.pairConflict(left, h))

def groupsExact(+xs: +List<R.Effect>, +ys: +List<R.Effect>, +found: Bool) -> {R.groups(xs, ys, found) == found || S.Groups(xs, ys) : Bool}:
  match xs:
    case Nil{}:
      Equal.sym(Bool, found || False{}, found, orFalse(found))
    case Con{h, t}:
      %againstExact(ys, h, False{}) : {R.groups(t, ys, found || R.against(ys, h, False{})) == found || (_ || S.Groups(t, ys)) : Bool}
      %orAssoc(found, R.against(ys, h, False{}), S.Groups(t, ys)) : {R.groups(t, ys, found || R.against(ys, h, False{})) == _ : Bool}
      groupsExact(t, ys, found || R.against(ys, h, False{}))

def identitySound(+name: +List<Nat>, p: {R.identityValid(name) == True{} : Bool}) -> S.IdentityValid(name):
  andUse(S.IdentityValid(name), R.nameNonempty(name), Bool.not(R.nameEqual(name, [42n], True{})), p, np => sp => (nameNonemptySound(name, np), nameDifferent(name, [42n], sp)))

def identitiesStep(+h: +List<Nat>, +t: +List<+List<Nat>>, +good: Bool, p: {good && R.identityValid(h) == True{} : Bool} & S.IdentitiesValid(t)) -> {good == True{} : Bool} & S.IdentitiesValid(h <> t):
  (gp, tp) = p
  andUse({good == True{} : Bool} & S.IdentitiesValid(h <> t), good, R.identityValid(h), gp, gp => hp => (gp, (identitySound(h, hp), tp)))

def identitiesSound(+xs: +List<+List<Nat>>, +good: Bool, p: {R.identitiesValid(xs, good) == True{} : Bool}) -> {good == True{} : Bool} & S.IdentitiesValid(xs):
  match xs:
    case Nil{}:
      (p, Unit{})
    case Con{h, t}:
      identitiesStep(h, t, good, identitiesSound(t, good && R.identityValid(h), p))

def sourceValidSelected(+decision: Bool, +names: +List<+List<Nat>>, p: {R.nonempty(names) && R.identitiesValid(names, True{}) == decision : Bool}) -> S.SourceValid(R.select(decision, names)):
  match decision:
    case False{}:
      Unit{}
    case True{}:
      andUse(S.SourceValid(R.Exact{names}), R.nonempty(names), R.identitiesValid(names, True{}), p, np => vp => (nonemptySound(names, np), second({True{} == True{} : Bool}, S.IdentitiesValid(names), identitiesSound(names, True{}, vp))))

def sourceRefinesSelected(+decision: Bool, +names: +List<+List<Nat>>) -> S.Refines(R.Exact{names}, R.select(decision, names)):
  match decision:
    case False{}:
      Unit{}
    case True{}:
      (subsetRefl(names), subsetRefl(names))

def pairSound(+left: R.Effect, +right: R.Effect, p: {R.pairConflict(left, right) == True{} : Bool}) -> S.EffectConflicts(left, right):
  match left right:
    case R.Quiet{} _:
      falseTrue(p)
    case R.Effect{lo, ls} R.Quiet{}:
      falseTrue(p)
    case R.Effect{lo, ls} R.Effect{ro, rs}:
      conflictSound(ls, rs, lo, ro, p)

def pairComplete(+left: R.Effect, +right: R.Effect, p: S.EffectConflicts(left, right)) -> {R.pairConflict(left, right) == True{} : Bool}:
  match left right:
    case R.Quiet{} _:
      Empty.absurd({False{} == True{} : Bool}, p)
    case R.Effect{lo, ls} R.Quiet{}:
      Empty.absurd({False{} == True{} : Bool}, p)
    case R.Effect{lo, ls} R.Effect{ro, rs}:
      conflictComplete(ls, rs, lo, ro, p)

def identityComplete(+name: +List<Nat>, p: S.IdentityValid(name)) -> {R.identityValid(name) == True{} : Bool}:
  (np, neq) = p
  andMake(R.nameNonempty(name), Bool.not(R.nameEqual(name, [42n], True{})), nameNonemptyComplete(name, np), nameInequality(name, [42n], R.nameEqual(name, [42n], True{}), {==}, neq))

def identitiesComplete(+xs: +List<+List<Nat>>, +good: Bool, gp: {good == True{} : Bool}, p: S.IdentitiesValid(xs)) -> {R.identitiesValid(xs, good) == True{} : Bool}:
  match xs:
    case Nil{}:
      gp
    case Con{h, t}:
      (hp, tp) = p
      identitiesComplete(t, good && R.identityValid(h), andMake(good, R.identityValid(h), gp, identityComplete(h, hp)), tp)

def sourceComplete(+names: +List<+List<Nat>>, p: S.SourceValid(R.Exact{names})) -> {R.source(names) == R.Exact{names} : R.Scope}:
  (np, vp) = p
  %Equal.sym(Bool, R.nonempty(names) && R.identitiesValid(names, True{}), True{}, andMake(R.nonempty(names), R.identitiesValid(names, True{}), nonemptyComplete(names, np), identitiesComplete(names, True{}, {==}, vp))) : {R.select(_, names) == R.Exact{names} : R.Scope}
  {==}
