import Base
import ./state-plans.bend as C
import ./state-spec.bend as S
import ./resources.bend as R
import ./resource-proof.bend as P

def notTrue(+b: Bool, p: {Bool.not(b) == True{} : Bool}) -> {b == False{} : Bool}:
  match b:
    case False{}:
      {==}
    case True{}:
      Empty.absurd({True{} == False{} : Bool}, P.falseTrue(p))

def authority(+grant: C.Grant, +key: +List<Nat>, +stopped: Bool, p: {C.allowed(grant, key, stopped) == True{} : Bool}) -> S.Authority(grant, key, stopped):
  match grant:
    case C.Grant{active, granted}:
      P.andUse(S.Authority(grant, key, stopped), active, Bool.not(stopped) && R.nameEqual(granted, key, True{}), p, ap => rest =>
        P.andUse(S.Authority(grant, key, stopped), Bool.not(stopped), R.nameEqual(granted, key, True{}), rest, np => kp =>
          (ap, (notTrue(stopped, np), P.nameExact(granted, key, kp)))))

def readSelected(+grant: C.Grant, +key: +List<Nat>, +stopped: Bool, +good: Bool, p: {C.allowed(grant, key, stopped) == good : Bool}) -> S.ReadSafe(grant, key, stopped, C.selectRead(good, key)):
  match good:
    case False{}:
      Unit{}
    case True{}:
      (authority(grant, key, stopped, p), {==})

def writeSelected(+grant: C.Grant, +key: +List<Nat>, +expected: +List<Nat>, +observed: +List<Nat>, +value: +List<Nat>, +stopped: Bool, +good: Bool, p: {C.allowed(grant, key, stopped) && R.nameEqual(expected, observed, True{}) == good : Bool}) -> S.WriteSafe(grant, key, expected, observed, value, stopped, C.selectWrite(good, key, expected, value)):
  match good:
    case False{}:
      Unit{}
    case True{}:
      P.andUse(S.WriteSafe(grant, key, expected, observed, value, stopped, C.Write{key, expected, value}), C.allowed(grant, key, stopped), R.nameEqual(expected, observed, True{}), p, ap => ep =>
        (authority(grant, key, stopped, ap), (P.nameExact(expected, observed, ep), ({==}, ({==}, {==})))))

def staleSelected(+grant: C.Grant, +key: +List<Nat>, +expected: +List<Nat>, +observed: +List<Nat>, +value: +List<Nat>, +stopped: Bool, +good: Bool, p: {C.allowed(grant, key, stopped) && R.nameEqual(expected, observed, True{}) == good : Bool}, different: {expected != observed : +List<Nat>}) -> {C.selectWrite(good, key, expected, value) == C.Denied{} : C.Plan}:
  match good:
    case False{}:
      {==}
    case True{}:
      P.andUse({C.Write{key, expected, value} == C.Denied{} : C.Plan}, C.allowed(grant, key, stopped), R.nameEqual(expected, observed, True{}), p, ap => ep =>
        Empty.absurd({C.Write{key, expected, value} == C.Denied{} : C.Plan}, different(P.nameExact(expected, observed, ep))))
