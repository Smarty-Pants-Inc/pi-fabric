# B5/Main generation-pin checks (moved C21/C22)

Import `runGenerationPinConformance(factory)` from `conformance.ts` in the
B5RecoveryAdmissionV1/Main composition test entrypoint. The factory drives the
real B5 owner/coordinator, not `B4EffectRecoveryProviderV1` directly.

- C21: absent pin **with prior generation evidence** must refuse before B4 work;
  a fresh original without evidence can legitimately allocate its first pin.
- C22: after settlement/lost ACK/reopen, B5 rejects a different g before changing
  its ledger, admitting another continuation/repair, or invoking B4 again.
- Source: R3 §7.1:148–154; B5 checks both `admitRecoveryContinuation` and
  `recordRecoveryRepair`. CoS round-2 ruling selects B5/Main ownership.

**Not yet bound/run against B5 in this B4 delivery.** Owner: fv2-l7-main with
fv2-l7-b5fix; binding and real pin checks due **2026-10-30 18:00Z**. The full
R3 C1 crash-cut matrix remains theirs; these two moved assertions alone do not
prove that matrix. No fake B5 enforcement is hidden inside the B4 fake, and no
skip or default-pass test disguises the missing composition execution.
