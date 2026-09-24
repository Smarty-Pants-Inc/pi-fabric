import Base
import ./state-plans.bend as C

def stateRead(grant: C.Grant, key: +List<Nat>, stopped: Bool) -> C.Plan:
  C.readPlan(grant, key, stopped)

def stateWrite(grant: C.Grant, key: +List<Nat>, expected: +List<Nat>, observed: +List<Nat>, value: +List<Nat>, stopped: Bool) -> C.Plan:
  C.writePlan(grant, key, expected, observed, value, stopped)

def stateRevoke(grant: C.Grant) -> C.Grant:
  C.revoke(grant)

type StateKernel is Type:
  StateKernel{read: C.Grant -> +List<Nat> -> Bool -> C.Plan, write: C.Grant -> +List<Nat> -> +List<Nat> -> +List<Nat> -> +List<Nat> -> Bool -> C.Plan, revoke: C.Grant -> C.Grant}

def main() -> IO(StateKernel):
  IO.pure(StateKernel, StateKernel{stateRead, stateWrite, stateRevoke})
