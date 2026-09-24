import Base
import ./provider-plans.bend as P
import ./state-plans.bend as S
import ./state-spec.bend as SS

def Safe(ticket: P.Ticket, key: +List<Nat>, observed: +List<Nat>, stopped: Bool, plan: S.Plan) -> Type:
  match ticket:
    case P.Ticket{grant, expected, payload}:
      SS.WriteSafe(grant, key, expected, observed, payload, stopped, plan)

def Consumed(before: P.Ticket, after: P.Ticket) -> Type:
  match before after:
    case P.Ticket{S.Grant{active, key}, expected, payload} P.Ticket{S.Grant{live, target}, revision, value}:
      {live == False{} : Bool} & ({target == key : +List<Nat>} & ({revision == expected : +List<Nat>} & {value == payload : +List<Nat>}))
