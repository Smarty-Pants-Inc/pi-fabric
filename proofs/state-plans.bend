import Base
import ./resources.bend as R

type Grant is Data:
  Grant{active: Bool, key: +List<Nat>}

type Plan is Data:
  Denied{}
  Read{key: +List<Nat>}
  Write{key: +List<Nat>, expected: +List<Nat>, value: +List<Nat>}

def allowed(grant: Grant, key: +List<Nat>, stopped: Bool) -> Bool:
  match grant:
    case Grant{active, granted}:
      active && (Bool.not(stopped) && R.nameEqual(granted, key, True{}))

def selectRead(good: Bool, key: +List<Nat>) -> Plan:
  match good:
    case False{}:
      Denied{}
    case True{}:
      Read{key}

def selectWrite(good: Bool, key: +List<Nat>, expected: +List<Nat>, value: +List<Nat>) -> Plan:
  match good:
    case False{}:
      Denied{}
    case True{}:
      Write{key, expected, value}

def readPlan(grant: Grant, +key: +List<Nat>, stopped: Bool) -> Plan:
  selectRead(allowed(grant, key, stopped), key)

def writePlan(grant: Grant, +key: +List<Nat>, +expected: +List<Nat>, observed: +List<Nat>, value: +List<Nat>, stopped: Bool) -> Plan:
  selectWrite(allowed(grant, key, stopped) && R.nameEqual(expected, observed, True{}), key, expected, value)

def revoke(grant: Grant) -> Grant:
  match grant:
    case Grant{active, key}:
      Grant{False{}, key}
