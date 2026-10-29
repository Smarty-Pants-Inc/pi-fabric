# L7 B5 admission v1 — test-only C1/C2 contract

Stable **source-tree** import: `tests/contracts/l7-b5-admission-v1/index.js`
(TypeScript resolves the adjacent `.ts`). This is not a production module or an
npm package export; keep this directory in the same checkout layout as `src/`.
It requires Vitest. It adds no runtime, scheduler, protocol or native authority.

```ts
import {
  createTestOnlyFakeHarness, runConformance,
  type B5Admission, type AdmissionHarnessFactory,
} from "./contracts/l7-b5-admission-v1/index.js";

runConformance({ name: "consumer fake", create: createTestOnlyFakeHarness });
// For a consumer test (inside Vitest):
const harness = createTestOnlyFakeHarness();
try {
  harness.admission.initializeNewRecoverySession(harness.initialization);
  harness.requireIdentity();
  harness.bindRequired();
  await harness.seedJoinedOriginal();
  const admission: B5Admission = harness.admission;
  const g = await admission.ensureRecoveryGeneration(harness.sessionId, harness.originalInputId);
  await admission.recordRecoveryRepair(harness.sessionId, harness.repairFor(g));
  await admission.admitContinuation({ sessionId: harness.sessionId,
    originalInputId: harness.originalInputId, recoveryGeneration: g, text: "exact bytes" });
} finally { harness.close(); }
```

## Actual public API covered

`B5Admission` reuses exported production signatures with `import type`/`Pick`,
not a copied ledger or arbitrary native fixture. It includes:

- `readRecoveryInput(sessionId, io?) -> Promise<RecoveryInputSnapshot>`;
- `ensureRecoveryGeneration(sessionId, originalInputId, io?) -> Promise<string>`;
- `admitContinuation({sessionId, originalInputId, recoveryGeneration, text}) ->
  Promise<{admissionId, existing}>`;
- `recordInputOutcome(request) -> Promise<RecoveryInputOutcomeResult>`;
- standalone `recordRecoveryRepair(sessionId, ref, io?) -> Promise<void>`;
- standalone synchronous `initializeNewRecoverySession(request, io?) -> void`;
- `maintenance.read(key, io?)` / `maintenance.listPending(sessionId, limit, io?)`,
  using the actual `MaintenanceResult` union, **not** thrown input-error semantics.

`RecoveryIo` has optional **absolute Unix-ms** `deadlineMs` and `signal`.
Production defaults remain unchanged (10 s transaction I/O; this suite uses the
unchanged Vitest 15 s per-case timeout, with no retries). The original input's
absolute deadline is never renewed by rebind or continuation. No generated UUID
format is promised: consumers must treat `g` and admission IDs as opaque strings.

Required setup: provision an existing unaliased root, then call the initializer
**before any binding** with `{mode:"NEW", root, sessionId, recovery, assertNew}`.
`assertNew():void` must synchronously positively assert authoritative NEW/unadmitted
identity, not infer it from absence. `recovery` supplies a full current native
epoch, pod/boot IDs, `historyComplete:true`, synchronous positive exclusion via
`assertCurrent()`, and `reconsiderInput()`. Existing targets always conflict,
including an identical revision-zero seal. Binding/reopening REQUIRED never calls
the initializer. External custody/identity owners remain responsible for actual
single-writer exclusion and authoritative NEW -> REQUIRED publication.

Allocate g for the current open original **before** generation-dependent work.
It is pinned to that original's immutable admission, not its process/binding epoch.
A missing pin refuses continuation/repair with an `Error` named
`RecoveryAdmissionError`, string `.code = "RECOVERY_HISTORY_INCOMPLETE"` and
nonempty `.message`; a different g or changed retained continuation/repair bytes
uses `RECOVERY_CONFLICT`. Exact retained retries, including g lookup after terminal
outcome, do not mutate/rearm work. The suite checks these public error properties,
not message wording or cross-copy `instanceof` identity.

Missing REQUIRED history produces `readRecoveryInput(...).complete === false`
(with the actual empty incomplete snapshot), and maintenance read/list return
`{ok:false, code:"RECOVERY_HISTORY_INCOMPLETE", message}`. This is **not** a clean
session or permission to create a new store. Other I/O/deadline/authority errors
must not be converted to clean success; this narrow suite does not enumerate them.

A thrown write can mean **unknown ACK after publish**, not absence. Retry
`ensureRecoveryGeneration` under current custody to perform real read/barriers and
reuse its retained g; do not mint a caller g. Exact continuation retry returns the
retained admission with `existing:true`. After unknown NEW seal ACK, never overwrite
a visible target: bind read-only under custody and read the exact retained seal.
Only still-positively-NEW absence may use explicit initialization. These are the
existing B5 rules, not a new retry protocol.

## Harness, suite and evidence boundary

`runConformance(admission: AdmissionHarnessFactory):void` registers nine Vitest
cases. A factory provides `{name, create}`; **each case** gets a fresh independent
harness and closes it in `finally`. `AdmissionHarness` exposes only test setup,
rebind, observation, deletion and one-write unknown-ACK injection. `observe()` is
an opaque exact-storage mutation detector, **not** a successful authoritative read
or a production permission. No consumer needs a `RecoverySessionLedger` copy.

`real.ts:createRealB5Harness` is intentionally a separate import: the stable index
does not import real B5 runtime/fs modules. It uses an actual unique temp root,
explicit real initialization, real B5 queue/select/delivery/outcome/join/rebind
calls and real checksummed ledger fsync/rename/readback. A narrow, restored
`fs.renameSync` spy performs the actual rename, then throws once to simulate lost
ACK; reads/barriers are never stubbed. The seed supplies a synchronous fixture
repair validator, native delivery references and an already-resolved fixture join.
It never pretends those receipts are genuine native effects. `close()` unbinds
and removes only its allocated temp root; no subprocesses are started.

`createTestOnlyFakeHarness` is an in-memory consumer double exposing the same
**narrow** surface, joined-original setup, rebind, terminal cancellation, pin,
repair, continuation and lost-ACK controls. Fake IDs deliberately say `test-only`;
`durable:true` is a **simulated API result**, never a durability receipt. It supports
`user-cancel`/`lifecycle-interrupt`, and refuses `settled` (it cannot authenticate a
native witness). Maintenance read/list cover an empty store and history refusal;
there is no maintenance execution engine. Use a new fake per consumer test and
close it. It does not claim to implement every production validation, arbitrary
input admission/selection/backlogs, every malformed request or error interleaving.

Both implementations run the **same suite** in
`tests/l7-b5-admission-contract.test.ts`. Real B5 cases are explicitly skipped on
Windows because production directory durability fails closed there; the fake runs
portably. Skipped real coverage is not durability proof. Existing ordinary
non-REQUIRED compatibility and full C1/C2/native process suites remain separate.
This suite proves neither native process crashes, power-loss, B4/B6 effects,
repair production, A-vs-J receipt resolution, retry-slot exhaustion, real custody,
manifest/mount identity nor end-to-end S6 behavior. Passing the fake is consumer
logic evidence only, not product/native qualification.

Run just this file (once, serial, default deadlines):

```sh
CI=false /home/paul/smarty/smarty-pants/bin/smarty-test-env -- \
  bunx vitest run tests/l7-b5-admission-contract.test.ts --no-file-parallelism --retry 0
```
