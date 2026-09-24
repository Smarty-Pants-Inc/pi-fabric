import Base
import ./lifecycle-kernel.bend as K
import ./lifecycle.bend as L

# Independent obligations over input and output records. Host promises, opaque
# identities and delivery of completion events are not encoded as proved facts.
def Open(phase: L.Phase, revoked: Bool, cleanup: Bool) -> Type:
  match phase:
    case L.Closing{}:
      Empty
    case L.Closed{}:
      Empty
    case L.Failed{}:
      {cleanup == True{} : Bool}
    case _:
      {cleanup == True{} : Bool} | {revoked == False{} : Bool}

def BeginSafe(+before: L.Life, cleanup: Bool, outcome: L.Outcome) -> Type:
  match before outcome:
    case L.Life{phase, owner, holds, calls, revoked} L.Outcome{after, L.Granted{}}:
      Open(phase, revoked, cleanup) & {after == L.Life{phase, owner, holds, 1n+calls, revoked} : L.Life}
    case _ L.Outcome{after, L.Denied{}}:
      {after == before : L.Life}
    case _ L.Outcome{after, L.StartClose{}}:
      Empty

def RetainSafe(+before: L.Life, cleanup: Bool, outcome: L.Outcome) -> Type:
  match before outcome:
    case L.Life{phase, owner, holds, calls, revoked} L.Outcome{after, L.Granted{}}:
      Open(phase, revoked, cleanup) & {after == L.Life{phase, owner, 1n+holds, calls, revoked} : L.Life}
    case _ L.Outcome{after, L.Denied{}}:
      {after == before : L.Life}
    case _ L.Outcome{after, L.StartClose{}}:
      Empty

def CloseSafe(+before: L.Life, outcome: L.Outcome) -> Type:
  match before outcome:
    case L.Life{phase, owner, holds, calls, revoked} L.Outcome{after, L.StartClose{}}:
      {phase == L.Retiring{} : L.Phase} & ({owner == False{} : Bool} & ({holds == 0n : Nat} & ({calls == 0n : Nat} & {after == L.Life{L.Closing{}, False{}, 0n, 0n, revoked} : L.Life})))
    case _ L.Outcome{after, L.Denied{}}:
      {after == before : L.Life}
    case _ L.Outcome{after, L.Granted{}}:
      Empty

def InspectSafe(+before: L.Life, outcome: L.Outcome) -> Type:
  match before outcome:
    case L.Life{phase, owner, holds, calls, revoked} L.Outcome{after, L.Granted{}}:
      Open(phase, revoked, False{}) & {after == before : L.Life}
    case _ L.Outcome{after, L.Denied{}}:
      {after == before : L.Life}
    case _ L.Outcome{after, L.StartClose{}}:
      Empty

law inspect_safe:
  for +life: L.Life
  InspectSafe(life, K.bindingStep(life, L.Inspect{}))

law inspect_complete:
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  {K.bindingStep(L.Life{L.Active{}, owner, holds, calls, False{}}, L.Inspect{}) == L.Outcome{L.Life{L.Active{}, owner, holds, calls, False{}}, L.Granted{}} : L.Outcome}

law begin_safe:
  for +life: L.Life
  for +cleanup: Bool
  BeginSafe(life, cleanup, K.bindingStep(life, L.Begin{cleanup}))

law retain_safe:
  for +life: L.Life
  for +cleanup: Bool
  RetainSafe(life, cleanup, K.bindingStep(life, L.Retain{cleanup}))

# Completeness follows the independent Open predicate for every phase, not
# just the Active/non-revoked examples below.
law begin_open_complete:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  for +cleanup: Bool
  for p: Open(phase, revoked, cleanup)
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Begin{cleanup}) == L.Outcome{L.Life{phase, owner, holds, 1n+calls, revoked}, L.Granted{}} : L.Outcome}

law retain_open_complete:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  for +cleanup: Bool
  for p: Open(phase, revoked, cleanup)
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Retain{cleanup}) == L.Outcome{L.Life{phase, owner, 1n+holds, calls, revoked}, L.Granted{}} : L.Outcome}

law inspect_open_complete:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  for p: Open(phase, revoked, False{})
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Inspect{}) == L.Outcome{L.Life{phase, owner, holds, calls, revoked}, L.Granted{}} : L.Outcome}

law close_safe:
  for +life: L.Life
  CloseSafe(life, K.bindingStep(life, L.Close{}))

law close_complete:
  for +revoked: Bool
  {K.bindingStep(L.Life{L.Retiring{}, False{}, 0n, 0n, revoked}, L.Close{}) == L.Outcome{L.Life{L.Closing{}, False{}, 0n, 0n, revoked}, L.StartClose{}} : L.Outcome}

law begin_complete:
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  {K.bindingStep(L.Life{L.Active{}, owner, holds, calls, False{}}, L.Begin{False{}}) == L.Outcome{L.Life{L.Active{}, owner, holds, 1n+calls, False{}}, L.Granted{}} : L.Outcome}

law release_exact:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, 1n+holds, calls, revoked}, L.Release{}) == L.Outcome{L.Life{phase, owner, holds, calls, revoked}, L.Granted{}} : L.Outcome}

law end_exact:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, 1n+calls, revoked}, L.End{}) == L.Outcome{L.Life{phase, owner, holds, calls, revoked}, L.Granted{}} : L.Outcome}

def RetiredPhase(phase: L.Phase) -> L.Phase:
  match phase:
    case L.Staged{}:
      L.Retiring{}
    case L.Active{}:
      L.Retiring{}
    case _:
      phase

law revoke_preserves_work:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Revoke{}) == L.Outcome{L.Life{RetiredPhase(phase), False{}, holds, calls, True{}}, L.Granted{}} : L.Outcome}

law drop_owner_preserves_work:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.DropOwner{}) == L.Outcome{L.Life{phase, False{}, holds, calls, revoked}, L.Granted{}} : L.Outcome}

law retire_preserves_work:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Retire{}) == L.Outcome{L.Life{RetiredPhase(phase), owner, holds, calls, revoked}, L.Granted{}} : L.Outcome}

law retain_complete:
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  {K.bindingStep(L.Life{L.Active{}, owner, holds, calls, False{}}, L.Retain{False{}}) == L.Outcome{L.Life{L.Active{}, owner, 1n+holds, calls, False{}}, L.Granted{}} : L.Outcome}

law release_underflow_refused:
  for +phase: L.Phase
  for +owner: Bool
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, 0n, calls, revoked}, L.Release{}) == L.Outcome{L.Life{phase, owner, 0n, calls, revoked}, L.Denied{}} : L.Outcome}

law end_underflow_refused:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, 0n, revoked}, L.End{}) == L.Outcome{L.Life{phase, owner, holds, 0n, revoked}, L.Denied{}} : L.Outcome}

law quarantine_preserves_work:
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{L.Retiring{}, owner, holds, calls, revoked}, L.Fail{}) == L.Outcome{L.Life{L.Failed{}, False{}, holds, calls, True{}}, L.Granted{}} : L.Outcome}

law cleanup_after_revocation:
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  {K.bindingStep(L.Life{L.Retiring{}, owner, holds, calls, True{}}, L.Begin{True{}}) == L.Outcome{L.Life{L.Retiring{}, owner, holds, 1n+calls, True{}}, L.Granted{}} : L.Outcome}

def Activation(phase: L.Phase, owner: Bool, holds: Nat, calls: Nat, revoked: Bool) -> L.Outcome:
  match phase:
    case L.Staged{}:
      L.Outcome{L.Life{L.Active{}, owner, holds, calls, revoked}, L.Granted{}}
    case L.Active{}:
      L.Outcome{L.Life{L.Active{}, owner, holds, calls, revoked}, L.Granted{}}
    case _:
      L.Outcome{L.Life{phase, owner, holds, calls, revoked}, L.Denied{}}

def Completion(phase: L.Phase, owner: Bool, holds: Nat, calls: Nat, revoked: Bool) -> L.Outcome:
  match phase:
    case L.Closing{}:
      L.Outcome{L.Life{L.Closed{}, owner, holds, calls, revoked}, L.Granted{}}
    case _:
      L.Outcome{L.Life{phase, owner, holds, calls, revoked}, L.Denied{}}

def Failure(phase: L.Phase, owner: Bool, holds: Nat, calls: Nat, revoked: Bool) -> L.Outcome:
  match phase:
    case L.Closed{}:
      L.Outcome{L.Life{L.Closed{}, owner, holds, calls, revoked}, L.Denied{}}
    case _:
      L.Outcome{L.Life{L.Failed{}, False{}, holds, calls, True{}}, L.Granted{}}

law activation_exact:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Activate{}) == Activation(phase, owner, holds, calls, revoked) : L.Outcome}

law completion_exact:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Complete{}) == Completion(phase, owner, holds, calls, revoked) : L.Outcome}

law failure_exact:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  {K.bindingStep(L.Life{phase, owner, holds, calls, revoked}, L.Fail{}) == Failure(phase, owner, holds, calls, revoked) : L.Outcome}

law closing_refuses_work:
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  for +revoked: Bool
  for +cleanup: Bool
  {K.bindingStep(L.Life{L.Closing{}, owner, holds, calls, revoked}, L.Begin{cleanup}) == L.Outcome{L.Life{L.Closing{}, owner, holds, calls, revoked}, L.Denied{}} : L.Outcome}

law revoked_refuses_work:
  for +phase: L.Phase
  for +owner: Bool
  for +holds: Nat
  for +calls: Nat
  {K.bindingStep(L.Life{phase, owner, holds, calls, True{}}, L.Begin{False{}}) == L.Outcome{L.Life{phase, owner, holds, calls, True{}}, L.Denied{}} : L.Outcome}
