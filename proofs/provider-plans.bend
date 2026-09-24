import Base
import ./state-plans.bend as S

type Ticket is Data:
  Ticket{grant: S.Grant, expected: +List<Nat>, payload: +List<Nat>}

type Outcome is Data:
  Outcome{next: Ticket, plan: S.Plan}

def take(ticket: Ticket, key: +List<Nat>, observed: +List<Nat>, stopped: Bool) -> Outcome:
  match ticket:
    case Ticket{+grant, +expected, +payload}:
      Outcome{Ticket{S.revoke(grant), expected, payload}, S.writePlan(grant, key, expected, observed, payload, stopped)}

def revoke(ticket: Ticket) -> Ticket:
  match ticket:
    case Ticket{grant, expected, payload}:
      Ticket{S.revoke(grant), expected, payload}

def next(outcome: Outcome) -> Ticket:
  match outcome:
    case Outcome{ticket, plan}:
      ticket

def plan(outcome: Outcome) -> S.Plan:
  match outcome:
    case Outcome{ticket, plan}:
      plan
