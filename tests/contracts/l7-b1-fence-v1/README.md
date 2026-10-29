# L7 B1 fence v1 — test-only callable contract (v2 conformance)

This directory defines a source-tree, test-only TypeScript contract for the two
B1 storage ports. Its common types cover both profile shapes, but the fake only
models the weaker observed-B candidate-test profile. This is not a production
adapter, production verifier, runtime service, scheduler, or new wire protocol.

## Stable import and example

From a test file directly under `tests/`, import through the stable index below.
Do not import the sibling implementation files or production modules directly.

```ts
import { it } from "vitest";
import {
  createTestOnlyFakeHarness,
  runConformance,
  type B1HarnessFactory,
  type B1StoragePorts,
} from "./contracts/l7-b1-fence-v1/index.js";

const fakeFactory: B1HarnessFactory = {
  name: "test-only B1 observed-profile fake",
  create: () => createTestOnlyFakeHarness(),
};
runConformance(fakeFactory);

it("lets a consumer call the two-port surface", async () => {
  const harness = createTestOnlyFakeHarness();
  try {
    const ports: B1StoragePorts = harness.ports;
    // TEST-ONLY fixture control: supply independent retirement observations.
    harness.completeRetirement();
    const revoke = await ports.revokeGeneration(harness.revokeRequest());
    // S6 grants but does NOT attach; this is the TEST-ONLY fixture-controller S7 path.
    harness.attachReservedReceiver();
    const mount = await ports.currentMountGeneration(harness.mountRequest());
    // Assert only behavior and fields supplied by the test contract.
    void revoke;
    void mount;
  } finally {
    harness.close();
  }
});
```

`runConformance` registers the shared Vitest cases for the supplied
`{ name, create }` factory. Each case should receive an isolated harness and
close it in `finally`. Its fixed consumer surface is `ports`, `revokeRequest()`,
`mountRequest()`, `observe()`, and `close()`. The harness also exposes typed
TEST-ONLY fixture/controller, fault, reopen, and evidence controls, including
`completeRetirement()` and `attachReservedReceiver()`; these are not additional
B1 storage ports or production authority. The positive path
is four distinct steps: independently complete retirement observations, call
`revokeGeneration`, attach through the fixture-controller S7 control, then call
`currentMountGeneration`. S6's revoke grant alone does not attach or produce a
`ReceiverReceipt`.

Run the package's conformance test through the standard environment wrapper,
with the approved lane-private scratch directory passed to the test process
(the wrapper itself uses its normal short-lived `/tmp` allocation):

```sh
CI=false /home/paul/smarty/smarty-pants/bin/smarty-test-env -- \
  env TMPDIR="${TMPDIR:?set an owned scratch directory first}" \
  bunx --no-install vitest run tests/contracts/l7-b1-fence-v1/conformance.test.ts \
    --no-file-parallelism --retry 0
```

Use an owned private scratch directory. The documentation makes no test-count
or pass claim; retain run-specific logs separately.

## Contract and profile boundary

`contract.ts` is the unchanged 188-line common TypeScript contract, byte-equal
to §7 of `/srv/scratch/paul/design-b1ports-in/design-final.md`.
Its SHA-256 is
`6196b186765724633594a140fdbb7a342d1b3a022646064caa23ed54d1a33e54`.
The contract has exactly two callable ports:

- `revokeGeneration(req)` returns a `RevokeReceipt`.
- `currentMountGeneration(req)` returns a `MountGeneration`.

The common types retain both `FenceProfile` variants and their distinct
`Assurance` shapes. `b1-v1-observed` explicitly says stale driver operations
are `not-prevented` and late block writes are `not-guaranteed`. The stronger
`r3-storage-fence` shape says stale driver operations are
`rejected-at-execution` and late block writes are `excluded-by-barrier`, and
carries the driver-fence epoch and backend-barrier reference. Those A-profile
types are present for shared A/B compatibility; their presence does not mean
the fake implements or establishes A.

The fake provides only an observed-B `candidate-test` model. An initial
request requiring `r3-storage-fence` is refused with
`B1_POLICY_UNSATISFIED` before effects; changing the semantic profile of a
retained operation conflicts with `B1_OPERATION_CONFLICT`, without upgrade or
effects. The fake does not upgrade candidate-test evidence to `qualified` and
must not manufacture a stronger assurance result, trusted receipt, or
production authorization.

The request's `deadlineUnixMs` is a liveness bound, not fence evidence. The
design's semantic request identity excludes that deadline, so an exact retry
may resume with a renewed deadline but cannot change semantic request fields.
`currentMountGeneration` is a read-only challenge using a fresh nonce; it does
not receive an expected engine UUID to echo, and it is not permission to attach,
format, repair, or mutate custody state. The design also distinguishes refusal
from hold: `B1PortFailure` extends `Error` with `.code`, `.disposition`
(`"refused" | "held"`), optional `.operationId` and `.durableState`, and an
`.evidence` array. Invalid/authentication/policy/identity/conflict conditions
are refusals; live work, unknown driver outcomes, incomplete coverage,
unreachable observers, missing teardown, and deadline conditions are held.
These are contract semantics, not a statement that this fake validates the
production evidence chain.

The interface has no third public attach-authority port. The receipt and
controller-owned attach grant described in the design do not turn this fake
into a controller, CSI adapter, mount observer, or verifier. The index uses
type-only re-exports for the shared contract; its runtime exports are only the
test conformance registrar and test-only fake factory.

## V2 strength and fixture controls

The shared cases call only `B1StoragePorts` for the behavior under test, with
independent setup/fault observations supplied by `B1Harness`. They do not import
the fake or use its implementation functions. A different implementation can
run them by supplying the same fixture controls and evidence view. The local
fixture encoding remains test-only, not the frozen production §8.2 schemas.

| Design clause | Added cases / modeled fault family |
|---|---|
| §8.1.3, §8.3 receipt/grant identity | `revoke receipt binds exact storage, reservation and revoked inventory`: exact storage, reserved self/a, predecessor list (no omission/duplication), exited Pods and grant hash. |
| §4.2 S8/S10, §8.2 acquisition/attach binding | Independently observed `stale-acquisition`, `foreign-acquisition`, `attach-observation-mismatch`; custody acquisition and device/record/FD reply binding. |
| §8.1.5, §9 #2 rechecks | Post-effect `killIssued`, `restartDisabled`, `nodeReachable`; a newly discovered task invalidates immutable coverage, even if a later boolean claims it complete. |
| §4.1, §9 #3 exact held replay | `exact held retry at … leaves the complete observed state unchanged` at restart, kill, path-retirement and workload-exit holds, including renewed deadline and client reopen. |
| §8.1.1/6/7 decoding | Both ports reject versions 0/2 and NaN/negative/fractional/infinite/unsafe deadlines; empty/whitespace operation IDs; unknown mount profile. |
| §4.1, §8.2, §9 #4 quarantine | B→C→B; historical node-name recreation; N=2/N=3 exhaustion after N−1 handovers; actually submitted failed attach with no engine; N=2 failed-candidate exhaustion; historical receipt readback cannot grant a later candidate. |
| §5/§6/§8.2 independent metadata binding | Each FD's namespace/filesystem, same-inode alias, both target relative paths, exact reservation equality, custody record bytes/lock inode/device lifetime, and changed observation under reused nonce. |

`reserveNextReceiver` advances the fixture controller to another S1 reservation
on the same volume while retaining per-epoch node-name keys. It is not a third
port. `failNextReceiverAttach` makes S7 submit an attach only after S6, then fail
before creating an engine/mount. That node's key remains quarantined even after
positive retirement. A merely gated candidate consumes no raw CSI key but still
remains in the Pod inventory when abandoned. No fictitious `GenerationRef` is
created for an engine-less candidate.

`retireFailedCandidateAndReserve` supplies fixture-only positive executor drain,
terminal candidate retirement and one held-slot CAS transfer. Unknown retirement
refuses transfer; old uncommitted calls then refuse as superseded. This models
§8.1.5's escape from invalid immutable coverage without editing a request or
releasing the slot. Completed operations retain exact historical readback, never
a grant for the next candidate. These are in-memory observations, not proof of
real Kubernetes Pod-CAS, executor termination or atomic durable storage.

`custodyRecordPayloadBytes` exposes independently provisioned fixture bytes for
reply-hash assertions, rather than trusting the returned hash or regenerating
it from returned custody fields. Metadata checks compare each FD's own evidence
and actual attach lineage. No quarantine reset is offered: quiet time, client
reopen and receipt readback cannot erase history; §11.2 maintenance is unmodeled.

## Suite scope and explicit non-proofs

The shared suite is a deterministic, test-level exercise of the callable port
boundary and the fake's modeled behavior. It is not the full stateful system
model or live qualification in design-final.md §9. Consult `conformance.ts` for
the exact registered cases; this README intentionally does not assert a test
count or a passing result.

In particular, neither the fake nor this suite proves:

- that the production verifier accepts only authenticated controller/helper
  evidence, enforces trusted profile policy, or rejects fabricated test JSON;
- actual T1–T8 predecessor teardown, kernel task exit, CSI/Longhorn behavior,
  storage identity, mounted-engine/replica lineage, or any physical fence;
- prevention of raw stale driver operations, a late-write barrier, exclusion
  of old block writes, or the design's data-plane safety invariant. The fake's
  TEST-ONLY `executeRawStaleAction` control can mutate modeled state after S6
  (detach, reattach-old, or old-origin write); a subsequent current-mount read
  refuses/contains the altered state. This is not proof of a qualified B data
  barrier;
- that `reopen()` does more than replace the client handle while retaining
  in-process model memory. It is not process-death, fsync-durability,
  keeper-handoff, or crash-recovery evidence;
- real on-volume custody lock/generation behavior, bound-file descriptors,
  off-volume identity CAS, M2 crash recovery, or mounted-volume observation;
- integration with Fabric, native recovery, B4/B5, or the shipped executable;
- actual zero-loss/effective-once behavior, A4 acceptance, qualification,
  design-final §9 / F1–F12, or a whole-cohort 120-second result.

A fake result or test JSON is never production authority. Source inspection and
this package alone are not qualification evidence. The package adds no
production code, runtime authority, scheduler, or protocol. The parent owns
integration and independent acceptance. Run-specific test/typecheck/mutation
results belong in the handoff evidence, not in production-qualification claims.

## Design status and hashes

The design input is
`/srv/scratch/paul/design-b1ports-in/design-final.md`, SHA-256
`00f1ad54b95b18f86e99529fe9bb5201bedcc6bcac2530a9951168803e643e40`.
It records the final designer revision after the second/final challenge and
retains the round-2 common contract unchanged. This v2 strengthening keeps that
contract and the seven-file package path; it does not assert production-owner
acceptance, schema/fixture freeze, a production verifier, or qualification.
