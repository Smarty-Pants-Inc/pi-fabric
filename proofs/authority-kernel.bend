import Base
import ./authority-state.bend as A

def authorityIssue(grants: +List<+List<Nat>>) -> A.Authority:
  A.issue(grants)

def authorityLive(authority: A.Authority) -> Bool:
  A.live(authority)

def authorityAllows(authority: A.Authority, grant: +List<Nat>) -> Bool:
  A.allows(authority, grant)

def authorityDerive(parent: A.Authority, candidate: +List<+List<Nat>>) -> A.Authority:
  A.derive(parent, candidate)

def authorityRelease(authority: A.Authority) -> A.Authority:
  A.release(authority)

type AuthorityKernel is Type:
  AuthorityKernel{issue: +List<+List<Nat>> -> A.Authority, live: A.Authority -> Bool, allows: A.Authority -> +List<Nat> -> Bool, derive: A.Authority -> +List<+List<Nat>> -> A.Authority, release: A.Authority -> A.Authority}

def main() -> IO(AuthorityKernel):
  IO.pure(AuthorityKernel, AuthorityKernel{authorityIssue, authorityLive, authorityAllows, authorityDerive, authorityRelease})
