# B4 effect-recovery provider v1 — K1 test-only contract

One co-versioned file set. Import `B4EffectRecoveryProviderV1` from `contract.ts`,
`createFake` from `fake.ts`, and **the same** `runConformance(provider)` from
`conformance.ts`. Do not copy the suite into Main or another producer lane.

`runConformance` registers Vitest tests. Its argument is a fixture factory
(`ConformanceFactory` in `fixtures.ts`), which creates an isolated provider plus
retained-state/destination controls. A `ScenarioSource` may be a concrete consumer
scenario or a callback receiving the harness's `recoveryIdentity()` accessor;
resolve the callback after acquiring that independent identity and before issuing
intents. These controls are **not** provider methods.
The fake implements the complete interface; the real factory exposes the
unmodified `createCertifiedEffectRecordPort`, including its missing/incompatible
methods, without shimming failures into successes.

```sh
B4_CONFORMANCE_TARGET=fake bunx --no-install vitest run tests/contracts/l7-b4-effect-recovery-provider-v1/provider.test.ts --reporter=verbose
B4_CONFORMANCE_TARGET=real bunx --no-install vitest run tests/contracts/l7-b4-effect-recovery-provider-v1/provider.test.ts --reporter=verbose
bunx --no-install vitest run tests/contracts/l7-b4-effect-recovery-provider-v1/consumer.test.ts tests/contracts/l7-b4-effect-recovery-provider-v1/resolver-errors.test.ts tests/contracts/l7-b4-effect-recovery-provider-v1/identities.test.ts --reporter=verbose
```

`provider.test.ts` runs 34 cases per target (C01–C36 except moved C21/C22).
`consumer.test.ts` adds four fake configurability flows; `resolver-errors.test.ts`
runs the same 34 cases with resolver messages that omit the error code.
`identities.test.ts` runs the same 34 cases with distinct non-default identities.
C13 forces a sink await; C35 reads the adapter-original path with no timestamp.
C36 binds the admission to the host's retained B5 admission record of that
original, keyed by `(originalInputId, admissionId)`. It refuses an inconsistent
pair, a consistent wrong pair (`admissionId = deliveryId = W` not retained for
that original), and the right admission with a wrong delivery, each with
`RECOVERY_CONFLICT` and no retained-state change; the valid input still repairs.
W is the ID B5's `prepareScriptedInput(...).idempotencyKey` derives for a
second sender of the same session/original input, text and deadline. Preparing
that sender's outbox does not retain W in the receiver's admission record.
Stand-in admissions also use B5's derivation; pair consistency, B5 grammar,
namespace and length alone are not admission authentication. C36 also refuses
v7's consistent malformed pair and right-admission/malformed-delivery input
(`"<A>:wrong"`, where A is the retained admission ID), next to the B5-derived
checks. Each refusal immediately checks the unchanged ledger digest; unfamiliar
ID forms must not bypass the retained admission lookup. V7d additionally runs
each of those five wrong inputs on its own fresh provider/harness, followed by
its valid repair/resolve check. The original sequence stays too, preserving
refusal-budget and poisoning coverage. One foreign-form sample, `legacy-admission`
(no B5 prefix, shorter than a B5 ID), is refused both as a consistent pair and as
a delivery with the right admission, on separate fresh harnesses with the same
code/digest/valid-repair checks. All of this stays inside C36: no new case number,
no title/order change, and no earlier refusal can hide an isolated arm.

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
  Per the final CoS scope ruling, those sibling C21/C22 cases are not changed
  here: their remaining `GENERATION` / `intent()` fixture coupling belongs to
  fv2-l7-main2 + fv2-l7-b5r2. This is not a C21/C22 execution or identity-fix claim.
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
  Standing settlement obligations remain under K10: the two-argument settlement
  signature cannot express a different generation per original in one pass.
  Main owns the per-original hold versus integration decision. Do not stamp
  unrelated pending intents with one selected original's g.

## Harness-supplied identities (Ch1 and N1)

Every `ConformanceHarness` must expose `recoveryIdentity()` returning
`{ acceptedInput, recoveryGeneration, executionBranchEntryId }`. The expected
values are independent of the B4 ref/view. Real B5 compositions supply the actual
selection's admissionId/deliveryId, the retained delivery anchor, and g returned
by `ensureRecoveryGeneration`; never seed a pin or mock the UUID allocator.
The fake supplies its stand-in values through the same accessor. Every shared
scenario intent now takes session, original and A from that accessor before it
is issued, including dependent/peer intents, alias keys and dedupe keys. Each
seeded intent is also checked against the harness observations. A native anchor
need not equal the fake's default `"A"`. The shared repair cases check deliveryId == admissionId and observed settlement g == pin g,
then retain all ref/view/hash/reopen/forgery/J2 assertions unconditionally.

`createAcceptedInput(now, { sessionId, originalInputId })` derives the stand-in
admission with B5 itself and captures a deadline 120 seconds after the harness clock;
`FakeOptions.now` and `RealOptions.now` configure stand-in clocks. Reopen does not
renew that deadline. Consumer P2 also checks a clock in 2031.
`createReal(scenario, { recoveryIdentity })` accepts observations from real B5.
Without that option it remains the explicitly labelled B4-only boundary probe:
the contract base has no B5 allocator. The v6 real B5 proof uses combined head
`dc1808ddb03fcf3fdfaf7d6059389e509efe6cd5` with this contract merged in scratch
(the merge is not pushed), not fabricated B5 state on the old base. Native
owner/delivery fixtures remain explicitly probes, not native/B6 qualification;
missing B4 repair methods stay RED. Round-5 `RESULT.md` retains the wiring,
per-case v5 comparison, exact counts and mutant evidence; no production file changes.

## Committed real-B5 target (N2)

`real-b5.ts` and `real-b5.test.ts` are the round-4 composition harness, now part
of this contract source set. Run on the combined branch with v6 merged, not on
the contract-only F base: that base lacks the real B5 allocator/admission exports.
No fake, pin seeding or compatibility shim substitutes for missing B5 functions.

```sh
bunx --no-install vitest run tests/contracts/l7-b4-effect-recovery-provider-v1/real-b5.test.ts --testTimeout=60000 --reporter=verbose --reporter=json --outputFile.json=/tmp/b4-real-b5.json
```

This runs all 34 cases through actual B5 admission, selection, generation pin,
delivery, interruption and retained reopen, then the unmodified real B4 port.
The verbose/JSON reporters retain per-case statuses, including ordinary REDs
for the absent real repair methods. The 60-second runner budget does not extend
the original-input deadline (120 seconds). Native owner/delivery are still
`probe:native-Pi`; this is not native/B6 qualification. The user carrier uses Pi's
native entry/message shape, including timestamps, and B5's delivery hash covers
exactly its UTF-8 `JSON.stringify(entry) + "\n"` bytes (sequence 1). The source-fix
branch/scan probe must use those same bytes and sequence; it must not manufacture
new bytes to match a retained hash. Shape source: Pi `cf3cf677`,
`packages/coding-agent/src/core/session-manager.ts:2579–2588` and
`agent-session.ts:5097–5101`. See round-5 `RESULT.md`
for the per-case comparison with v5 and the additional distinct-anchor proof.

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
perform those checks for it. K9's `consumer.test.ts` fixture derives its admission
with B5's `prepareScriptedInput` through `scriptedAdmissionId`; it no longer uses
the non-B5 literal `s4-admission`. This is still sender-derived stand-in data,
not a retained receiver admission.

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
with `sha256sum -c B4-CONTRACT.sha256`. Contract revisions are distinct from the
unchanged provider ABI `V1`:

- v2: K1–K4, contract SHA-256 prefix `38e0f907`.
- v3: K5–K9, contract SHA-256 prefix `878cdd86`.
- Historical K10 text `1c4446be` and its settlement-obligations correction
  `a118f496` are cited by contract hash, not assigned a shared version label.
- v5: Ch1, contract SHA-256
  `5999e635b3554acd9d9b5843eb66479d4479db198384c53479501a98819e91fe`.
- v6: N1/N2/N4/N5, contract SHA-256
  `7f0dad114164261e28f44b58fe9425d48d7ce6f49f380c49d8aa758b7419ba9a`.
- v7: C1 retained admission binding in C36 only, contract SHA-256
  `0a90d30a6ed6e54f4691e8e57cd8e8480d4b055179258632d9f55db9aa2b28a9`.
  Round-6 `RESULT.md` records fake, prior mutants (including judge A1 under all
  four identity setups), and per-case real-B5 evidence on a scratch merge onto
  `dc1808ddb03fcf3fdfaf7d6059389e509efe6cd5`; no production changes or G2 claim.

- v7b: B5-derived second-sender and stand-in admissions, plus native-shaped
  user carrier (CoS carrier addendum), contract SHA-256
  `3b6d45d79b48ed74f567a8028d9300caeaa12b89213f75a39bee8a37d7810447`.
  Round-7 `RESULT.md` records mutant detection in fake and real-B5 identity
  setups, the unchanged real-B5 failure map on `dc1808dd`, and the separate
  scratch composition with B4 source fix `8c39803c`. No production change;
  independent acceptance belongs to the CoS judge.

- v7c: restores both malformed-ID C36 checks alongside the B5-derived checks,
  each with its ledger-digest assertion (R7 cause C3), contract SHA-256
  `304fe8563e079ceec2982dff076bafdd44663445dbada460db1f6ac148226b3b`.
  Round-7c `RESULT.md` records builder evidence. No other case or production change;
  independent acceptance remains with the CoS judge.

- v7d: fresh-harness isolation for every C36 wrong input, one foreign-form
  sample in pair/delivery positions, and a B5-derived K9 consumer admission,
  contract SHA-256
  `6eb2cae5c68a47e79ab3167b57c06f90a8de776c8d157e77f0972839a2d7ea82`.
  The original C36 sequence and all five refusal/digest pairs remain. Round-7d
  `RESULT.md` records builder evidence, not independent acceptance. No production
  change; still 34 cases with unchanged titles and order.

The round-2 `PHASE1.md` (v2) and `RESULT.md` (v3), round-4 `RESULT.md` (v5),
and round-5 `RESULT.md` (v6) record commit/hash and per-test evidence. CoS owns
PR body updates; the builder is authorized only to push this branch.
