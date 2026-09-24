import Base
import ./storage-plans.bend as C
import ./storage-spec.bend as S
import ./storage-kernel.bend as K
import ./resource-proof.bend as P

# Proof-only helpers, never linked into the executable artifact.
def revisionEq(+ah: Nat, +al: Nat, +bh: Nat, +bl: Nat, hp: {ah == bh : Nat}, lp: {al == bl : Nat}) -> {C.Revision{ah, al} == C.Revision{bh, bl} : C.Revision}:
  %hp : {C.Revision{ah, al} == C.Revision{_, bl} : C.Revision}
  %lp : {C.Revision{ah, al} == C.Revision{ah, _} : C.Revision}
  {==}

def revisionExact(+left: C.Revision, +right: C.Revision, p: {C.equal(left, right) == True{} : Bool}) -> {left == right : C.Revision}:
  match left right:
    case C.Revision{ah, al} C.Revision{bh, bl}:
      P.andUse({C.Revision{ah, al} == C.Revision{bh, bl} : C.Revision}, Nat.is_eq(ah, bh), Nat.is_eq(al, bl), p, hp => lp => revisionEq(ah, al, bh, bl, P.natExact(ah, bh, hp), P.natExact(al, bl, lp)))

def revisionRefl(+revision: C.Revision) -> {C.equal(revision, revision) == True{} : Bool}:
  match revision:
    case C.Revision{high, low}:
      P.andMake(Nat.is_eq(high, high), Nat.is_eq(low, low), P.natRefl(high), P.natRefl(low))

def casSound(+expected: C.Expectation, +actual: C.Revision, p: {C.matches(expected, actual) == True{} : Bool}) -> S.CAS(expected, actual):
  match expected:
    case C.Any{}:
      Unit{}
    case C.At{version}:
      revisionExact(version, actual, p)

def casComplete(+expected: C.Expectation, +actual: C.Revision, p: S.CAS(expected, actual)) -> {C.matches(expected, actual) == True{} : Bool}:
  match expected:
    case C.Any{}:
      {==}
    case C.At{version}:
      %p : {C.equal(version, _) == True{} : Bool}
      revisionRefl(version)

def roomSound(+version: C.Revision, p: {C.room(version) == True{} : Bool}) -> S.Room(version):
  match version:
    case C.Revision{high, low}:
      P.andUse(S.Room(version), Nat.is_le(high, 2097151n) && Nat.is_le(low, 4294967295n), Nat.is_lt(high, 2097151n) || Nat.is_lt(low, 4294967295n), p, bounds => spare =>
        P.andUse(S.Room(version), Nat.is_le(high, 2097151n), Nat.is_le(low, 4294967295n), bounds, hp => lp =>
          (hp, (lp, P.orCases(Nat.is_lt(high, 2097151n), Nat.is_lt(low, 4294967295n), spare)))))

def roomComplete(+version: C.Revision, p: S.Room(version)) -> {C.room(version) == True{} : Bool}:
  match version:
    case C.Revision{high, low}:
      (hp, rest) = p
      (lp, spare) = rest
      P.andMake(Nat.is_le(high, 2097151n) && Nat.is_le(low, 4294967295n), Nat.is_lt(high, 2097151n) || Nat.is_lt(low, 4294967295n), P.andMake(Nat.is_le(high, 2097151n), Nat.is_le(low, 4294967295n), hp, lp), P.orEvidence(Nat.is_lt(high, 2097151n), Nat.is_lt(low, 4294967295n), spare))

def carrySafe(+high: Nat, +low: Nat, +full: Bool, p: {Nat.is_eq(low, 4294967295n) == full : Bool}) -> S.Successor(C.Revision{high, low}, C.carry(full, high, low)):
  match full:
    case False{}:
      Inl{(p, {==})}
    case True{}:
      Inr{(P.natExact(low, 4294967295n, p), {==})}

def successor(+version: C.Revision) -> S.Successor(version, C.next(version)):
  match version:
    case C.Revision{high, low}:
      carrySafe(high, low, Nat.is_eq(low, 4294967295n), {==})

def baseRoomSound(+present: Bool, +version: C.Revision, +highWater: C.Revision, p: {C.room(C.base(present, version, highWater)) == True{} : Bool}) -> S.Room(S.Base(present, version, highWater)):
  match present:
    case False{}:
      roomSound(highWater, p)
    case True{}:
      roomSound(version, p)

def baseRoomComplete(+present: Bool, +version: C.Revision, +highWater: C.Revision, p: S.Room(S.Base(present, version, highWater))) -> {C.room(C.base(present, version, highWater)) == True{} : Bool}:
  match present:
    case False{}:
      roomComplete(highWater, p)
    case True{}:
      roomComplete(version, p)

def baseSuccessor(+present: Bool, +version: C.Revision, +highWater: C.Revision) -> S.Successor(S.Base(present, version, highWater), C.next(C.base(present, version, highWater))):
  match present:
    case False{}:
      successor(highWater)
    case True{}:
      successor(version)

def putSelected(+key: +List<Nat>, +expected: C.Expectation, +value: +List<Nat>, +identity: +List<Nat>, +present: Bool, +version: C.Revision, +highWater: C.Revision, +room: Bool, cp: S.CAS(expected, version), rp: {C.canGrow(C.base(present, version, highWater), highWater) == room : Bool}) -> S.Safe(C.Request{key, expected, C.Put{value, identity}}, C.Slot{present, version, highWater}, C.grow(room, key, C.Put{value, identity}, C.base(present, version, highWater), highWater)):
  match room:
    case False{}:
      Unit{}
    case True{}:
      P.andUse(S.Safe(C.Request{key, expected, C.Put{value, identity}}, C.Slot{present, version, highWater}, C.grow(True{}, key, C.Put{value, identity}, C.base(present, version, highWater), highWater)), C.room(C.base(present, version, highWater)), C.room(highWater), rp, bp => hp =>
        (cp, (baseRoomSound(present, version, highWater, bp), (roomSound(highWater, hp), ({==}, (({==}, {==}), (baseSuccessor(present, version, highWater), successor(highWater))))))))

def deleteSelected(+key: +List<Nat>, +expected: C.Expectation, +version: C.Revision, +highWater: C.Revision, +room: Bool, cp: S.CAS(expected, version), rp: {C.canGrow(version, highWater) == room : Bool}) -> S.Safe(C.Request{key, expected, C.Delete{}}, C.Slot{True{}, version, highWater}, C.grow(room, key, C.Delete{}, version, highWater)):
  match room:
    case False{}:
      Unit{}
    case True{}:
      P.andUse(S.Safe(C.Request{key, expected, C.Delete{}}, C.Slot{True{}, version, highWater}, C.grow(True{}, key, C.Delete{}, version, highWater)), C.room(version), C.room(highWater), rp, bp => hp =>
        (cp, (roomSound(version, bp), (roomSound(highWater, hp), ({==}, (Unit{}, ({==}, (successor(version), successor(highWater)))))))))

def applySafe(+key: +List<Nat>, +expected: C.Expectation, +change: C.Change, +present: Bool, +version: C.Revision, +highWater: C.Revision, cp: S.CAS(expected, version)) -> S.Safe(C.Request{key, expected, change}, C.Slot{present, version, highWater}, C.apply(key, change, present, version, highWater)):
  match change present:
    case C.Delete{} False{}:
      (cp, (Unit{}, {==}))
    case C.Delete{} True{}:
      deleteSelected(key, expected, version, highWater, C.canGrow(version, highWater), cp, {==})
    case C.Put{value, identity} _:
      putSelected(key, expected, value, identity, present, version, highWater, C.canGrow(C.base(present, version, highWater), highWater), cp, {==})

def admittedSafe(+key: +List<Nat>, +expected: C.Expectation, +change: C.Change, +present: Bool, +version: C.Revision, +highWater: C.Revision, +ok: Bool, p: {C.matches(expected, version) == ok : Bool}) -> S.Safe(C.Request{key, expected, change}, C.Slot{present, version, highWater}, C.admit(ok, key, change, present, version, highWater)):
  match ok:
    case False{}:
      Unit{}
    case True{}:
      applySafe(key, expected, change, present, version, highWater, casSound(expected, version, p))

# Closed universal safety for the actual exported production reducer.
def storage_safe(+request: C.Request, +slot: C.Slot) -> S.Safe(request, slot, K.storageTransition(request, slot)):
  match request slot:
    case C.Request{key, expected, change} C.Slot{present, version, highWater}:
      admittedSafe(key, expected, change, present, version, highWater, C.matches(expected, version), {==})

# Independent positive witnesses make an always-deny reducer fail the laws.
def storage_put_complete(+key: +List<Nat>, +expected: C.Expectation, +value: +List<Nat>, +identity: +List<Nat>, +present: Bool, +version: C.Revision, +highWater: C.Revision, cp: S.CAS(expected, version), rp: S.Room(S.Base(present, version, highWater)), hp: S.Room(highWater)) -> S.PutAccepted(key, value, identity, S.Base(present, version, highWater), highWater, K.storageTransition(C.Request{key, expected, C.Put{value, identity}}, C.Slot{present, version, highWater})):
  %Equal.sym(Bool, C.matches(expected, version), True{}, casComplete(expected, version, cp)) : S.PutAccepted(key, value, identity, S.Base(present, version, highWater), highWater, C.admit(_, key, C.Put{value, identity}, present, version, highWater))
  %Equal.sym(Bool, C.canGrow(C.base(present, version, highWater), highWater), True{}, P.andMake(C.room(C.base(present, version, highWater)), C.room(highWater), baseRoomComplete(present, version, highWater, rp), roomComplete(highWater, hp))) : S.PutAccepted(key, value, identity, S.Base(present, version, highWater), highWater, C.grow(_, key, C.Put{value, identity}, C.base(present, version, highWater), highWater))
  (C.next(C.base(present, version, highWater)), (C.next(highWater), ((baseSuccessor(present, version, highWater), successor(highWater)), {==})))

def storage_delete_complete(+key: +List<Nat>, +expected: C.Expectation, +version: C.Revision, +highWater: C.Revision, cp: S.CAS(expected, version), rp: S.Room(version), hp: S.Room(highWater)) -> S.DeleteAccepted(key, version, highWater, K.storageTransition(C.Request{key, expected, C.Delete{}}, C.Slot{True{}, version, highWater})):
  %Equal.sym(Bool, C.matches(expected, version), True{}, casComplete(expected, version, cp)) : S.DeleteAccepted(key, version, highWater, C.admit(_, key, C.Delete{}, True{}, version, highWater))
  %Equal.sym(Bool, C.canGrow(version, highWater), True{}, P.andMake(C.room(version), C.room(highWater), roomComplete(version, rp), roomComplete(highWater, hp))) : S.DeleteAccepted(key, version, highWater, C.grow(_, key, C.Delete{}, version, highWater))
  (C.next(version), (C.next(highWater), ((successor(version), successor(highWater)), {==})))

def storage_absent_delete_complete(+key: +List<Nat>, +expected: C.Expectation, +version: C.Revision, +highWater: C.Revision, cp: S.CAS(expected, version)) -> {K.storageTransition(C.Request{key, expected, C.Delete{}}, C.Slot{False{}, version, highWater}) == C.Unchanged{} : C.Plan}:
  %Equal.sym(Bool, C.matches(expected, version), True{}, casComplete(expected, version, cp)) : {C.admit(_, key, C.Delete{}, False{}, version, highWater) == C.Unchanged{} : C.Plan}
  {==}

def staleSelected(+key: +List<Nat>, +expected: C.Revision, +change: C.Change, +present: Bool, +version: C.Revision, +highWater: C.Revision, +ok: Bool, p: {C.equal(expected, version) == ok : Bool}, different: {expected != version : C.Revision}) -> {C.admit(ok, key, change, present, version, highWater) == C.Conflict{} : C.Plan}:
  match ok:
    case False{}:
      {==}
    case True{}:
      Empty.absurd({C.apply(key, change, present, version, highWater) == C.Conflict{} : C.Plan}, different(revisionExact(expected, version, p)))

def storage_stale_refused(+key: +List<Nat>, +expected: C.Revision, +change: C.Change, +present: Bool, +version: C.Revision, +highWater: C.Revision, different: {expected != version : C.Revision}) -> {K.storageTransition(C.Request{key, C.At{expected}, change}, C.Slot{present, version, highWater}) == C.Conflict{} : C.Plan}:
  staleSelected(key, expected, change, present, version, highWater, C.equal(expected, version), {==}, different)

# Either key or global clock exhaustion independently forbids mutation.
def noGrowth(+version: C.Revision, +highWater: C.Revision, +room: Bool, p: {C.canGrow(version, highWater) == room : Bool}, full: (S.Room(version) & S.Room(highWater)) -> Empty) -> {room == False{} : Bool}:
  match room:
    case False{}:
      {==}
    case True{}:
      P.andUse({True{} == False{} : Bool}, C.room(version), C.room(highWater), p, bp => hp =>
        Empty.absurd({True{} == False{} : Bool}, full((roomSound(version, bp), roomSound(highWater, hp)))))

def noBaseGrowth(+present: Bool, +version: C.Revision, +highWater: C.Revision, full: (S.Room(S.Base(present, version, highWater)) & S.Room(highWater)) -> Empty) -> {C.canGrow(C.base(present, version, highWater), highWater) == False{} : Bool}:
  match present:
    case False{}:
      noGrowth(highWater, highWater, C.canGrow(highWater, highWater), {==}, full)
    case True{}:
      noGrowth(version, highWater, C.canGrow(version, highWater), {==}, full)

def storage_put_overflow(+key: +List<Nat>, +expected: C.Expectation, +value: +List<Nat>, +identity: +List<Nat>, +present: Bool, +version: C.Revision, +highWater: C.Revision, cp: S.CAS(expected, version), full: (S.Room(S.Base(present, version, highWater)) & S.Room(highWater)) -> Empty) -> {K.storageTransition(C.Request{key, expected, C.Put{value, identity}}, C.Slot{present, version, highWater}) == C.Exhausted{} : C.Plan}:
  %Equal.sym(Bool, C.matches(expected, version), True{}, casComplete(expected, version, cp)) : {C.admit(_, key, C.Put{value, identity}, present, version, highWater) == C.Exhausted{} : C.Plan}
  %Equal.sym(Bool, C.canGrow(C.base(present, version, highWater), highWater), False{}, noBaseGrowth(present, version, highWater, full)) : {C.grow(_, key, C.Put{value, identity}, C.base(present, version, highWater), highWater) == C.Exhausted{} : C.Plan}
  {==}

def storage_delete_overflow(+key: +List<Nat>, +expected: C.Expectation, +version: C.Revision, +highWater: C.Revision, cp: S.CAS(expected, version), full: (S.Room(version) & S.Room(highWater)) -> Empty) -> {K.storageTransition(C.Request{key, expected, C.Delete{}}, C.Slot{True{}, version, highWater}) == C.Exhausted{} : C.Plan}:
  %Equal.sym(Bool, C.matches(expected, version), True{}, casComplete(expected, version, cp)) : {C.admit(_, key, C.Delete{}, True{}, version, highWater) == C.Exhausted{} : C.Plan}
  %Equal.sym(Bool, C.canGrow(version, highWater), False{}, noGrowth(version, highWater, C.canGrow(version, highWater), {==}, full)) : {C.grow(_, key, C.Delete{}, version, highWater) == C.Exhausted{} : C.Plan}
  {==}

# Negative probes: wrapper -> Conflict; CAS -> True; ignore high CAS limb;
# reuse old key version or clock; absent base -> version (ABA); wrong carry/low
# increment; swap key/value/identity; bypass canGrow; refuse absent no-op.
