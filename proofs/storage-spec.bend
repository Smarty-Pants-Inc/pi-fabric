import Base
import ./storage-plans.bend as C

# Independent propositions: no implementation predicate/transition calls.
# Revision denotes high * 2^32 + low. The host supplies canonical safe snapshots
# and a persisted highWater covering every previously issued revision. Legacy
# migration can seed only retained history; already lost history is unknowable.
# The host must preserve the clock across eviction. OS locks, rename/crash
# durability, authority and multi-write transactions are NOT proved here.
def CAS(expected: C.Expectation, actual: C.Revision) -> Type:
  match expected:
    case C.Any{}:
      Unit
    case C.At{version}:
      {version == actual : C.Revision}

def Room(version: C.Revision) -> Type:
  match version:
    case C.Revision{high, low}:
      {Nat.is_le(high, 2097151n) == True{} : Bool} & ({Nat.is_le(low, 4294967295n) == True{} : Bool} & ({Nat.is_lt(high, 2097151n) == True{} : Bool} | {Nat.is_lt(low, 4294967295n) == True{} : Bool}))

def Successor(+before: C.Revision, after: C.Revision) -> Type:
  match before:
    case C.Revision{high, low}:
      ({Nat.is_eq(low, 4294967295n) == False{} : Bool} & {after == C.Revision{high, 1n+low} : C.Revision}) | ({low == 4294967295n : Nat} & {after == C.Revision{1n+high, 0n} : C.Revision})

# Allocation base is specified independently from the implementation selector.
def Base(present: Bool, version: C.Revision, highWater: C.Revision) -> C.Revision:
  match present:
    case False{}:
      highWater
    case True{}:
      version

def PutPayload(change: C.Change, value: +List<Nat>, identity: +List<Nat>) -> Type:
  match change:
    case C.Delete{}:
      Empty
    case C.Put{v, i}:
      {value == v : +List<Nat>} & {identity == i : +List<Nat>}

def IsDelete(change: C.Change) -> Type:
  match change:
    case C.Delete{}:
      Unit
    case C.Put{v, i}:
      Empty

def Safe(request: C.Request, slot: C.Slot, plan: C.Plan) -> Type:
  match request slot:
    case C.Request{key, expected, change} C.Slot{present, version, highWater}:
      match plan:
        case C.Conflict{}:
          Unit
        case C.Exhausted{}:
          Unit
        case C.Unchanged{}:
          CAS(expected, version) & (IsDelete(change) & {present == False{} : Bool})
        case C.PutNext{k, v, i, next, clock}:
          CAS(expected, version) & (Room(Base(present, version, highWater)) & (Room(highWater) & ({k == key : +List<Nat>} & (PutPayload(change, v, i) & (Successor(Base(present, version, highWater), next) & Successor(highWater, clock))))))
        case C.DeleteNext{k, next, clock}:
          CAS(expected, version) & (Room(version) & (Room(highWater) & ({k == key : +List<Nat>} & (IsDelete(change) & ({present == True{} : Bool} & (Successor(version, next) & Successor(highWater, clock)))))))

# Positive witnesses require actual key and clock successors, not a hypothetical
# successful branch. Neither predicate mentions implementation successor code.
def PutAccepted(+key: +List<Nat>, +value: +List<Nat>, +identity: +List<Nat>, +before: C.Revision, +highWater: C.Revision, +plan: C.Plan) -> Type:
  &after: C.Revision -> &clock: C.Revision -> (Successor(before, after) & Successor(highWater, clock)) & {plan == C.PutNext{key, value, identity, after, clock} : C.Plan}

def DeleteAccepted(+key: +List<Nat>, +before: C.Revision, +highWater: C.Revision, +plan: C.Plan) -> Type:
  &after: C.Revision -> &clock: C.Revision -> (Successor(before, after) & Successor(highWater, clock)) & {plan == C.DeleteNext{key, after, clock} : C.Plan}
