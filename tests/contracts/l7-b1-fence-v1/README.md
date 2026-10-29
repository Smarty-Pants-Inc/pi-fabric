# L7 B1 fence v1 — test-only callable contract

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
  env TMPDIR=/srv/scratch/paul/fv2-l7-excl-tmp-YODgmi \
  bunx --no-install vitest run tests/contracts/l7-b1-fence-v1/conformance.test.ts \
    --no-file-parallelism --retry 0
```

Use a currently approved private scratch directory if that example path is no
longer available. The documentation makes no test-count or pass claim.

## Contract and profile boundary

`contract.ts` is the 188-line common TypeScript contract copied byte-for-byte
from `/srv/scratch/paul/fv2-b1contract-in/common-contract.ts`.
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

## Suite scope and explicit non-proofs

The shared suite is a deterministic, test-level exercise of the callable port
boundary and the fake's modeled behavior. It is not the full stateful system
model or live qualification in design-r2.md §9. Consult `conformance.ts` for
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
  design-r2 §9 / F1–F12, or a whole-cohort 120-second result.

A fake result or test JSON is never production authority. Source inspection and
this package alone are not qualification evidence. The package adds no
production code, runtime authority, scheduler, or protocol. The parent owns
integration and all checks; no test, typecheck, build, or runtime result is
reported here.

## Design status and hashes

The design input is
`/srv/scratch/paul/fv2-b1contract-in/design-r2.md`, SHA-256
`61fc3a2d8870a9cb44d3b3a33100b51283a5e1625fd77faae45481eb5a860f8f`.
It is a designer proposal, not an accepted production contract. As of this
handoff, the design remains in its final challenge round; no final challenge
verdict or production-owner acceptance is asserted here. If that round changes
the shared contract, publish a v2 package rather than silently changing v1.
