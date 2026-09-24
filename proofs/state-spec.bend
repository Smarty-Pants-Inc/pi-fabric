import Base
import ./state-plans.bend as C

# Independent propositions: no calls to implementation predicates.
def Authority(grant: C.Grant, key: +List<Nat>, stopped: Bool) -> Type:
  match grant:
    case C.Grant{active, granted}:
      {active == True{} : Bool} & ({stopped == False{} : Bool} & {granted == key : +List<Nat>})

def ReadSafe(grant: C.Grant, +key: +List<Nat>, stopped: Bool, plan: C.Plan) -> Type:
  match plan:
    case C.Denied{}:
      Unit
    case C.Read{actual}:
      Authority(grant, key, stopped) & {actual == key : +List<Nat>}
    case C.Write{k, e, v}:
      Empty

def WriteSafe(grant: C.Grant, +key: +List<Nat>, +expected: +List<Nat>, observed: +List<Nat>, value: +List<Nat>, stopped: Bool, plan: C.Plan) -> Type:
  match plan:
    case C.Denied{}:
      Unit
    case C.Read{k}:
      Empty
    case C.Write{actual, revision, payload}:
      Authority(grant, key, stopped) & ({expected == observed : +List<Nat>} & ({actual == key : +List<Nat>} & ({revision == expected : +List<Nat>} & {payload == value : +List<Nat>})))
