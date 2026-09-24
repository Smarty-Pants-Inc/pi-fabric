import Base

# Canonical radix-2^32 limbs keep arithmetic below Bend's 48-bit Nat runtime cap.
# The host encodes only safe JS revisions; room also rejects noncanonical limbs.
type Revision is Data:
  Revision{high: Nat, low: Nat}

type Expectation is Data:
  Any{}
  At{version: Revision}

type Change is Data:
  Put{value: +List<Nat>, identity: +List<Nat>}
  Delete{}

type Request is Data:
  Request{key: +List<Nat>, expected: Expectation, change: Change}

type Slot is Data:
  Slot{present: Bool, version: Revision, highWater: Revision}

type Plan is Data:
  Conflict{}
  Exhausted{}
  Unchanged{}
  PutNext{key: +List<Nat>, value: +List<Nat>, identity: +List<Nat>, version: Revision, highWater: Revision}
  DeleteNext{key: +List<Nat>, version: Revision, highWater: Revision}

def equal(left: Revision, right: Revision) -> Bool:
  match left right:
    case Revision{ah, al} Revision{bh, bl}:
      Nat.is_eq(ah, bh) && Nat.is_eq(al, bl)

def matches(expected: Expectation, actual: Revision) -> Bool:
  match expected:
    case Any{}:
      True{}
    case At{version}:
      equal(version, actual)

def room(+version: Revision) -> Bool:
  match version:
    case Revision{high, low}:
      (Nat.is_le(high, 2097151n) && Nat.is_le(low, 4294967295n)) && (Nat.is_lt(high, 2097151n) || Nat.is_lt(low, 4294967295n))

def carry(full: Bool, high: Nat, low: Nat) -> Revision:
  match full:
    case False{}:
      Revision{high, 1n+low}
    case True{}:
      Revision{1n+high, 0n}

def next(+version: Revision) -> Revision:
  match version:
    case Revision{high, low}:
      carry(Nat.is_eq(low, 4294967295n), high, low)

# Live revisions remain +1 for existing certificate consumers. An absent key
# allocates above the persistent store-wide clock, even after tombstone eviction.
def base(present: Bool, version: Revision, highWater: Revision) -> Revision:
  match present:
    case False{}:
      highWater
    case True{}:
      version

def canGrow(version: Revision, highWater: Revision) -> Bool:
  room(version) && room(highWater)

def grow(available: Bool, key: +List<Nat>, change: Change, version: Revision, highWater: Revision) -> Plan:
  match available:
    case False{}:
      Exhausted{}
    case True{}:
      match change:
        case Put{value, identity}:
          PutNext{key, value, identity, next(version), next(highWater)}
        case Delete{}:
          DeleteNext{key, next(version), next(highWater)}

def apply(key: +List<Nat>, change: Change, +present: Bool, +version: Revision, +highWater: Revision) -> Plan:
  match change present:
    case Delete{} False{}:
      Unchanged{}
    case Delete{} True{}:
      grow(canGrow(version, highWater), key, Delete{}, version, highWater)
    case Put{value, identity} _:
      grow(canGrow(base(present, version, highWater), highWater), key, Put{value, identity}, base(present, version, highWater), highWater)

def admit(ok: Bool, key: +List<Nat>, change: Change, present: Bool, version: Revision, highWater: Revision) -> Plan:
  match ok:
    case False{}:
      Conflict{}
    case True{}:
      apply(key, change, present, version, highWater)

def reduce(request: Request, +slot: Slot) -> Plan:
  match request slot:
    case Request{key, expected, change} Slot{present, version, highWater}:
      admit(matches(expected, version), key, change, present, version, highWater)
