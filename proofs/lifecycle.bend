import Base

type Phase is Data:
  Staged{}
  Active{}
  Retiring{}
  Closing{}
  Closed{}
  Failed{}

type Life is Data:
  Life{phase: Phase, owner: Bool, holds: Nat, calls: Nat, revoked: Bool}

type BindingEvent is Data:
  Inspect{}
  Activate{}
  Retire{}
  DropOwner{}
  Revoke{}
  Retain{cleanup: Bool}
  Release{}
  Begin{cleanup: Bool}
  End{}
  Close{}
  Complete{}
  Fail{}

type Command is Data:
  Denied{}
  Granted{}
  StartClose{}

type Outcome is Data:
  Outcome{next: Life, command: Command}

def open(phase: Phase, revoked: Bool, cleanup: Bool) -> Bool:
  match phase:
    case Closing{}:
      False{}
    case Closed{}:
      False{}
    case Failed{}:
      cleanup
    case _:
      cleanup || Bool.not(revoked)

def retiring(phase: Phase) -> Phase:
  match phase:
    case Staged{}:
      Retiring{}
    case Active{}:
      Retiring{}
    case _:
      phase

def selectBegin(good: Bool, life: Life) -> Outcome:
  match good life:
    case False{} _:
      Outcome{life, Denied{}}
    case True{} Life{phase, owner, holds, calls, revoked}:
      Outcome{Life{phase, owner, holds, 1n+calls, revoked}, Granted{}}

def selectRetain(good: Bool, life: Life) -> Outcome:
  match good life:
    case False{} _:
      Outcome{life, Denied{}}
    case True{} Life{phase, owner, holds, calls, revoked}:
      Outcome{Life{phase, owner, 1n+holds, calls, revoked}, Granted{}}

def selectInspect(good: Bool, life: Life) -> Outcome:
  match good:
    case False{}:
      Outcome{life, Denied{}}
    case True{}:
      Outcome{life, Granted{}}

def step(event: BindingEvent, life: Life) -> Outcome:
  match event life:
    case Inspect{} Life{+phase, +owner, +holds, +calls, +revoked}:
      selectInspect(open(phase, revoked, False{}), life)
    case Activate{} Life{Staged{}, owner, holds, calls, revoked}:
      Outcome{Life{Active{}, owner, holds, calls, revoked}, Granted{}}
    case Activate{} Life{Active{}, owner, holds, calls, revoked}:
      Outcome{life, Granted{}}
    case Activate{} _:
      Outcome{life, Denied{}}
    case Retire{} Life{+phase, +owner, +holds, +calls, +revoked}:
      Outcome{Life{retiring(phase), owner, holds, calls, revoked}, Granted{}}
    case DropOwner{} Life{+phase, +owner, +holds, +calls, +revoked}:
      Outcome{Life{phase, False{}, holds, calls, revoked}, Granted{}}
    case Revoke{} Life{+phase, +owner, +holds, +calls, +revoked}:
      Outcome{Life{retiring(phase), False{}, holds, calls, True{}}, Granted{}}
    case Retain{cleanup} Life{+phase, +owner, +holds, +calls, +revoked}:
      selectRetain(open(phase, revoked, cleanup), life)
    case Release{} Life{phase, owner, 0n, calls, revoked}:
      Outcome{life, Denied{}}
    case Release{} Life{phase, owner, 1n+holds, calls, revoked}:
      Outcome{Life{phase, owner, holds, calls, revoked}, Granted{}}
    case Begin{cleanup} Life{+phase, +owner, +holds, +calls, +revoked}:
      selectBegin(open(phase, revoked, cleanup), life)
    case End{} Life{phase, owner, holds, 0n, revoked}:
      Outcome{life, Denied{}}
    case End{} Life{phase, owner, holds, 1n+calls, revoked}:
      Outcome{Life{phase, owner, holds, calls, revoked}, Granted{}}
    case Close{} Life{Retiring{}, False{}, 0n, 0n, revoked}:
      Outcome{Life{Closing{}, False{}, 0n, 0n, revoked}, StartClose{}}
    case Close{} _:
      Outcome{life, Denied{}}
    case Complete{} Life{Closing{}, owner, holds, calls, revoked}:
      Outcome{Life{Closed{}, owner, holds, calls, revoked}, Granted{}}
    case Complete{} _:
      Outcome{life, Denied{}}
    case Fail{} Life{Closed{}, owner, holds, calls, revoked}:
      Outcome{life, Denied{}}
    case Fail{} Life{+phase, +owner, +holds, +calls, +revoked}:
      Outcome{Life{Failed{}, False{}, holds, calls, True{}}, Granted{}}

def bindingStep(life: Life, event: BindingEvent) -> Outcome:
  step(event, life)
