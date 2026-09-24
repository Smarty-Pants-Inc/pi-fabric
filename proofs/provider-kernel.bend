import Base
import ./provider-plans.bend as P

def providerTake(ticket: P.Ticket, key: +List<Nat>, observed: +List<Nat>, stopped: Bool) -> P.Outcome:
  P.take(ticket, key, observed, stopped)

def providerRevoke(ticket: P.Ticket) -> P.Ticket:
  P.revoke(ticket)

type ProviderKernel is Type:
  ProviderKernel{take: P.Ticket -> +List<Nat> -> +List<Nat> -> Bool -> P.Outcome, revoke: P.Ticket -> P.Ticket}

def main() -> IO(ProviderKernel):
  IO.pure(ProviderKernel, ProviderKernel{providerTake, providerRevoke})
