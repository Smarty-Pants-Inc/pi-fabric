# B4 effect-recovery provider v1 — K1 test-only contract

One co-versioned file set. Import `B4EffectRecoveryProviderV1` from `contract.ts`,
`createFake` from `fake.ts`, and **the same** `runConformance(provider)` from
`conformance.ts`. Do not copy the suite into Main or another producer lane.

`runConformance` registers Vitest tests. Its argument is a fixture factory
(`ConformanceFactory` in `fixtures.ts`), which creates an isolated provider plus
retained-state/destination controls. These controls are **not** provider methods.
The fake implements the complete interface; the real factory exposes the
unmodified `createCertifiedEffectRecordPort`, including its missing/incompatible
methods, without shimming failures into successes.

```sh
B4_CONFORMANCE_TARGET=fake bunx --no-install vitest run tests/contracts/l7-b4-effect-recovery-provider-v1/provider.test.ts --reporter=verbose
B4_CONFORMANCE_TARGET=real bunx --no-install vitest run tests/contracts/l7-b4-effect-recovery-provider-v1/provider.test.ts --reporter=verbose
bunx --no-install vitest run tests/contracts/l7-b4-effect-recovery-provider-v1/consumer.test.ts tests/contracts/l7-b4-effect-recovery-provider-v1/resolver-errors.test.ts --reporter=verbose
```

`provider.test.ts` runs 33 cases per target (C01–C35 except moved C21/C22).
`consumer.test.ts` adds four fake configurability flows; `resolver-errors.test.ts`
runs the same 33 cases with resolver messages that omit the error code.
C13 forces a sink await; C35 reads the adapter-original path with no timestamp.

The default runs both. Real-provider failures remain ordinary failing tests,
not skips or expected failures. The selected integration head does not yet
implement the entire frozen R3/B3 contract. This draft must not be called G2 GREEN.

## Scope and ownership

- Existing B3 consumer seam: `settlePendingIntents` and `readToolResult`.
- Existing Pi host seam: `repairMaintenance(input, executionLeafId)`. Pi's general
  host interface makes it optional; this repair-capable profile requires it.
- New R3 resolver: synchronous `resolveRepair(ref)`; no effect/provider await in
  a B5 transaction. Receipt hash uses `canonicalEncode(view)`. Native tool-result
  hashes use exact `JSON.stringify(message)` bytes. These are different hashes.
- G4: B4 never mints a generation. B5/Main owns pin creation and refusal, not
  B4. C21/C22 moved to `../l7-b5-main-generation-v1/conformance.ts`, exported as
  `runGenerationPinConformance`; Main/B5 must bind/run it by 2026-10-30 18:00Z.
  The B4 fake has no hidden pin check. Its receipt generation is fixture input,
  not proof of B5 admission. See that suite's README for unexecuted coverage.
- Receipt `viewSha256` was removed in v2 because Pi's request digest contains
  the receipt ref. `joinedHistorySha256` is pinned to Pi's base-history encoding;
  C23 checks exact ordered JSON.stringify entry bytes, inner LF, and outer hash.
- K10 is DECIDED: pins are per `(sessionId, originalInputId)`; different originals
  may have different g values. Each repair/continuation must carry its own original's
  pin; B5 refuses cross-original, unpinned or reminted g with a coded error asserted
  by `.code`. B4 stays pin-agnostic. Real + fake assertions belong to the B5/Main
  composition suite in `../l7-b5-admission-v1/`, not this B4 suite; owner
  fv2-l7-b5fix, due 2026-10-30 18:00Z. See B4-CONTRACT.md §4 rule 5 for the
  recorded decision and unexecuted B5/Main coverage.

## Consumer-owned identities (K9)

```ts
const h = await createFake({ intents: consumerIntents }, {
  sessionId: input.sessionId,
  acceptedInput: input,                  // complete AcceptedRecoveryInput
  executionBranchEntryId: originalA,
  joinedLeafEntryId: actualJ,
  pin: retainedB5Generation,             // fixture evidence, not B4 validation
});
const provider: B4EffectRecoveryProviderV1 = h.provider; // no Partial cast
// settle -> read -> repairMaintenance(input, actualJ) -> resolveRepair(ref)
await h.dispose();
```

`createFakeProvider(scenario, options)` is the direct full-provider accessor when
no retained-state controls are needed. `advanceJoinedLeaf(nextId)` supports any
consumer leaf ID. Defaults preserve existing fixture-only consumers. Main must
still execute its own B5 generation checks; this fake deliberately will not
perform those checks for it.

The fake is an **in-memory component stand-in**, not a durable producer, genuine
repair receipt, installed native host or B1 witness. Reopen in fake tests retains
model state; it is not a process-crash test. The real factory reopens real B4
records on disk. C28–C32 use the real file and fixed shell adapters. Other sink
and native-entry boundaries are controlled probes. In particular C12 is **not**
a real peer/control late-request or cache-expiry qualification; B5/transport
integration and the shipped S3/S7/S8 flows must prove those. The explicit
before/after-deadline and before/after-cache-expiry both-side matrix is in
B4-CONTRACT.md §7: owner B5/control sender + transport receiver, Main integrating,
due 2026-10-30 18:00Z; it is not executed in this delivery.

All files live under `tests/`, outside `tsconfig.build.json` and production entry
points. There is no production registration, runtime fallback, new package
export or change to B4 behavior. Passing the fake never enables REQUIRED.

The authoritative source-pinned narrative and conflicts are co-versioned here:
[`B4-CONTRACT.md`](./B4-CONTRACT.md). Verify its frozen bytes from this directory
with `sha256sum -c B4-CONTRACT.sha256`. V2 corrects K1–K4; v3 applies K5–K9.
The round-2 `PHASE1.md` (v2) and `RESULT.md` (v3) record commit/hash and per-test evidence. CoS owns PR
body updates; the builder is authorized only to push this branch.
