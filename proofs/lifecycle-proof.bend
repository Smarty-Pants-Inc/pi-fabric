import Base
import ./lifecycle-kernel.bend as K
import ./lifecycle.bend as L
import ./lifecycle-spec.bend as S
import ./resource-proof.bend as P
import ./state-proof.bend as SP

def openCases(+revoked: Bool, +cleanup: Bool, p: {cleanup == True{} : Bool} | {Bool.not(revoked) == True{} : Bool}) -> {cleanup == True{} : Bool} | {revoked == False{} : Bool}:
  match p:
    case Inl{yes}:
      Inl{yes}
    case Inr{no}:
      Inr{SP.notTrue(revoked, no)}

def openSound(+phase: L.Phase, +revoked: Bool, +cleanup: Bool, p: {L.open(phase, revoked, cleanup) == True{} : Bool}) -> S.Open(phase, revoked, cleanup):
  match phase:
    case L.Closing{}:
      P.falseTrue(p)
    case L.Closed{}:
      P.falseTrue(p)
    case L.Failed{}:
      p
    case L.Staged{}:
      openCases(revoked, cleanup, P.orCases(cleanup, Bool.not(revoked), p))
    case L.Active{}:
      openCases(revoked, cleanup, P.orCases(cleanup, Bool.not(revoked), p))
    case L.Retiring{}:
      openCases(revoked, cleanup, P.orCases(cleanup, Bool.not(revoked), p))

def notRevoked(+revoked: Bool, p: {revoked == False{} : Bool}) -> {Bool.not(revoked) == True{} : Bool}:
  %Equal.sym(Bool, revoked, False{}, p) : {Bool.not(_) == True{} : Bool}
  {==}

def openEvidence(+revoked: Bool, +cleanup: Bool, p: {cleanup == True{} : Bool} | {revoked == False{} : Bool}) -> {cleanup || Bool.not(revoked) == True{} : Bool}:
  match p:
    case Inl{yes}:
      P.orLeft(cleanup, Bool.not(revoked), yes)
    case Inr{no}:
      P.orRight(cleanup, Bool.not(revoked), notRevoked(revoked, no))

def openComplete(+phase: L.Phase, +revoked: Bool, +cleanup: Bool, p: S.Open(phase, revoked, cleanup)) -> {L.open(phase, revoked, cleanup) == True{} : Bool}:
  match phase:
    case L.Closing{}:
      Empty.absurd({False{} == True{} : Bool}, p)
    case L.Closed{}:
      Empty.absurd({False{} == True{} : Bool}, p)
    case L.Failed{}:
      p
    case L.Staged{}:
      openEvidence(revoked, cleanup, p)
    case L.Active{}:
      openEvidence(revoked, cleanup, p)
    case L.Retiring{}:
      openEvidence(revoked, cleanup, p)

def beginSelected(+phase: L.Phase, +owner: Bool, +holds: Nat, +calls: Nat, +revoked: Bool, +cleanup: Bool, +good: Bool, p: {L.open(phase, revoked, cleanup) == good : Bool}) -> S.BeginSafe(L.Life{phase, owner, holds, calls, revoked}, cleanup, L.selectBegin(good, L.Life{phase, owner, holds, calls, revoked})):
  match good:
    case False{}:
      {==}
    case True{}:
      (openSound(phase, revoked, cleanup, p), {==})

def retainSelected(+phase: L.Phase, +owner: Bool, +holds: Nat, +calls: Nat, +revoked: Bool, +cleanup: Bool, +good: Bool, p: {L.open(phase, revoked, cleanup) == good : Bool}) -> S.RetainSafe(L.Life{phase, owner, holds, calls, revoked}, cleanup, L.selectRetain(good, L.Life{phase, owner, holds, calls, revoked})):
  match good:
    case False{}:
      {==}
    case True{}:
      (openSound(phase, revoked, cleanup, p), {==})

def inspectSelected(+phase: L.Phase, +owner: Bool, +holds: Nat, +calls: Nat, +revoked: Bool, +good: Bool, p: {L.open(phase, revoked, False{}) == good : Bool}) -> S.InspectSafe(L.Life{phase, owner, holds, calls, revoked}, L.selectInspect(good, L.Life{phase, owner, holds, calls, revoked})):
  match good:
    case False{}:
      {==}
    case True{}:
      (openSound(phase, revoked, False{}, p), {==})

def S.inspect_safe(life):
  match life:
    case L.Life{phase, owner, holds, calls, revoked}:
      inspectSelected(phase, owner, holds, calls, revoked, L.open(phase, revoked, False{}), {==})

def S.inspect_complete(owner, holds, calls):
  {==}

def S.begin_safe(life, cleanup):
  match life:
    case L.Life{phase, owner, holds, calls, revoked}:
      beginSelected(phase, owner, holds, calls, revoked, cleanup, L.open(phase, revoked, cleanup), {==})

def S.retain_safe(life, cleanup):
  match life:
    case L.Life{phase, owner, holds, calls, revoked}:
      retainSelected(phase, owner, holds, calls, revoked, cleanup, L.open(phase, revoked, cleanup), {==})

def S.begin_open_complete(phase, owner, holds, calls, revoked, cleanup, p):
  %Equal.sym(Bool, L.open(phase, revoked, cleanup), True{}, openComplete(phase, revoked, cleanup, p)) : {L.selectBegin(_, L.Life{phase, owner, holds, calls, revoked}) == L.Outcome{L.Life{phase, owner, holds, 1n+calls, revoked}, L.Granted{}} : L.Outcome}
  {==}

def S.retain_open_complete(phase, owner, holds, calls, revoked, cleanup, p):
  %Equal.sym(Bool, L.open(phase, revoked, cleanup), True{}, openComplete(phase, revoked, cleanup, p)) : {L.selectRetain(_, L.Life{phase, owner, holds, calls, revoked}) == L.Outcome{L.Life{phase, owner, 1n+holds, calls, revoked}, L.Granted{}} : L.Outcome}
  {==}

def S.inspect_open_complete(phase, owner, holds, calls, revoked, p):
  %Equal.sym(Bool, L.open(phase, revoked, False{}), True{}, openComplete(phase, revoked, False{}, p)) : {L.selectInspect(_, L.Life{phase, owner, holds, calls, revoked}) == L.Outcome{L.Life{phase, owner, holds, calls, revoked}, L.Granted{}} : L.Outcome}
  {==}

def S.close_safe(life):
  match life:
    case L.Life{L.Retiring{}, False{}, 0n, 0n, revoked}:
      ({==}, ({==}, ({==}, ({==}, {==}))))
    case L.Life{L.Staged{}, owner, holds, calls, revoked}:
      {==}
    case L.Life{L.Active{}, owner, holds, calls, revoked}:
      {==}
    case L.Life{L.Retiring{}, True{}, holds, calls, revoked}:
      {==}
    case L.Life{L.Retiring{}, False{}, 1n+holds, calls, revoked}:
      {==}
    case L.Life{L.Retiring{}, False{}, 0n, 1n+calls, revoked}:
      {==}
    case L.Life{L.Closing{}, owner, holds, calls, revoked}:
      {==}
    case L.Life{L.Closed{}, owner, holds, calls, revoked}:
      {==}
    case L.Life{L.Failed{}, owner, holds, calls, revoked}:
      {==}

def S.close_complete(revoked):
  {==}

def S.begin_complete(owner, holds, calls):
  {==}

def S.release_exact(phase, owner, holds, calls, revoked):
  {==}

def S.end_exact(phase, owner, holds, calls, revoked):
  {==}

def S.revoke_preserves_work(phase, owner, holds, calls, revoked):
  match phase:
    case L.Staged{}:
      {==}
    case L.Active{}:
      {==}
    case L.Retiring{}:
      {==}
    case L.Closing{}:
      {==}
    case L.Closed{}:
      {==}
    case L.Failed{}:
      {==}

def S.drop_owner_preserves_work(phase, owner, holds, calls, revoked):
  {==}

def S.retire_preserves_work(phase, owner, holds, calls, revoked):
  match phase:
    case L.Staged{}:
      {==}
    case L.Active{}:
      {==}
    case L.Retiring{}:
      {==}
    case L.Closing{}:
      {==}
    case L.Closed{}:
      {==}
    case L.Failed{}:
      {==}

def S.retain_complete(owner, holds, calls):
  {==}

def S.release_underflow_refused(phase, owner, calls, revoked):
  {==}

def S.end_underflow_refused(phase, owner, holds, revoked):
  {==}

def S.quarantine_preserves_work(owner, holds, calls, revoked):
  {==}

def S.cleanup_after_revocation(owner, holds, calls):
  {==}

def S.activation_exact(phase, owner, holds, calls, revoked):
  match phase:
    case L.Staged{}:
      {==}
    case L.Active{}:
      {==}
    case L.Retiring{}:
      {==}
    case L.Closing{}:
      {==}
    case L.Closed{}:
      {==}
    case L.Failed{}:
      {==}

def S.completion_exact(phase, owner, holds, calls, revoked):
  match phase:
    case L.Staged{}:
      {==}
    case L.Active{}:
      {==}
    case L.Retiring{}:
      {==}
    case L.Closing{}:
      {==}
    case L.Closed{}:
      {==}
    case L.Failed{}:
      {==}

def S.failure_exact(phase, owner, holds, calls, revoked):
  match phase:
    case L.Staged{}:
      {==}
    case L.Active{}:
      {==}
    case L.Retiring{}:
      {==}
    case L.Closing{}:
      {==}
    case L.Closed{}:
      {==}
    case L.Failed{}:
      {==}

def S.closing_refuses_work(owner, holds, calls, revoked, cleanup):
  {==}

def S.revoked_refuses_work(phase, owner, holds, calls):
  match phase:
    case L.Staged{}:
      {==}
    case L.Active{}:
      {==}
    case L.Retiring{}:
      {==}
    case L.Closing{}:
      {==}
    case L.Closed{}:
      {==}
    case L.Failed{}:
      {==}
