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
```

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
- G4: no random generation, no pin replacement. The fake's pin comes from the
  fixture representing B5's durable original-input pin. Real B5 binding is still
  Main's integration work; the real harness does not invent it.
- The multi-original-pin case is OPEN for fv2-l7-main; the two-argument settlement
  signature cannot express a different generation per original in one pass.

The fake is an **in-memory component stand-in**, not a durable producer, genuine
repair receipt, installed native host or B1 witness. Reopen in fake tests retains
model state; it is not a process-crash test. The real factory reopens real B4
records on disk. C28–C32 use the real file and fixed shell adapters. Other sink
and native-entry boundaries are controlled probes. In particular C12 is **not**
a real peer/control late-request or cache-expiry qualification; B5/transport
integration and the shipped S3/S7/S8 flows must prove those.

All files live under `tests/`, outside `tsconfig.build.json` and production entry
points. There is no production registration, runtime fallback, new package
export or change to B4 behavior. Passing the fake never enables REQUIRED.

The authoritative source-pinned narrative and conflicts are in the requester's
`B4-CONTRACT.md` delivery artifact, identified by SHA-256 in the draft PR body.
