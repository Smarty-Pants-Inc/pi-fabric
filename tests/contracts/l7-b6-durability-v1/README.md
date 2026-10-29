# L7 B6 durability v1 — test-only contract

Stable source-tree entrypoint: `tests/contracts/l7-b6-durability-v1/index.js`
(TypeScript resolves the adjacent `.ts`). Vitest is required. This directory is
not a production module or package export and changes no production registration,
store, journal, or serialized format.

The B6 adapter, request, result, location, flush-receipt, and native-registration
types are re-exported directly from `src/recovery/l7-interfaces.ts`; this package
does not mirror or widen that ABI. Harness controls below exist only in tests.
In particular, the public append request has no arbitrary bytes argument:
`seedControlSeen` supplies exact control-seen fixture bytes and the original
input deadline without inventing a production setter.

## Run the fake suite or compose a consumer

```ts
import {
  createTestOnlyFakeHarness,
  runConformance,
} from "./contracts/l7-b6-durability-v1/index.js";

runConformance({ name: "B6 test-only fake", create: createTestOnlyFakeHarness });
```

The executable registration is `fake.test.ts`. Main can run the same suite
against a composed implementation by supplying its own `B6DurabilityHarness`
factory; every Vitest case gets an independent harness and closes it in
`finally`. A real harness should report numeric native-Pi-file I/O counters so
the suite can assert all four are zero rather than baking a fake-only literal
counter type into the harness contract.

## Exported harness surface

`B6DurabilityHarness` exposes the exact bound `sessionId`, `sessionFile`, and
`storeId`; the `RecoveryDurabilityAdapter`; a deterministic clock; exact
control-seen byte/deadline seeding; native descriptor fixture generation;
recover/reopen states (`unknown`, `unavailable`, `recovered`); availability
invalidation; a one-append pre-commit gate; lost-ACK injection; and observations
of retained entries, sequences, registrations, and native file I/O. The helper that generates native descriptors records fixture
provenance separately from descriptor shape. Its trust catalogue is keyed by
exact descriptor plus receipt values, so an authorized detached copy remains
valid while `trusted: false` produces equally shaped bytes without proof.

The fake models the relevant contract boundaries:

- registration is correlation only; the barrier commit creates the sequence
  and makes exact bytes readable;
- uncommitted registrations are discarded on recovery and must be re-emitted by
  the native owner; committed carriers recover with the same sequence;
- control-seen uses harness-seeded bytes and its original deadline, while a
  carrier uses its validated registration/lifecycle deadline when no matching
  control seed exists. Carrier append has no control-seen ordering prerequisite;
  the harness seed only supplies bytes for an explicit control-seen append;
- ordinary user-carrier IDs and derived native-entry IDs are isolated by the
  exact tuple. A derived ID is
  `pi-native-entry:v1:` plus the lower-case SHA-256 of UTF-8
  `JSON.stringify([sessionId, entryId])`; derived maintenance/control/terminal
  entries need no invented input or control-seen append;
- mutable request/fixture bytes are copied; registration bindings are
  immutable; deadline/signal changes do not change native-entry identity; later
  valid larger-prefix receipts can strengthen evidence without changing the entry
  or barrier sequence;
- unknown availability never grants negative authority. Reads return
  `not-found` only after successful committed-log recovery; loss remains
  `store-unavailable` until recovery succeeds;
- abort and deadlines are checked before and while waiting for the commit gate,
  and a lost ACK is reconciled from the exact recovered committed sequence.

The contract suite checks allowed typed refusal results rather than imposing a
new precedence among simultaneously invalid, aborted, expired, unavailable, or
superseded conditions. Wrong-session append/read return `superseded`; a valid
registration identity conflict also leaves that tuple's later barrier/read
`superseded`. Wrong-session or structural registration rejection may be
`conflict` or `superseded` as the frozen ABI permits. The harness does not invent
a global supersede/epoch operation that the public port does not expose.

## Keep reason and limits

Keep this suite for the **native trust boundary and acknowledged-work data
safety**. The fake is an in-memory behavioral double, not evidence that an
adapter actually writes/fsyncs a store, validates a real native flush receipt,
recovers after process or machine crash, survives power loss, owns the correct
session volume, or activates REQUIRED composition. Its trusted-provenance hook,
commit log, and recovery states are fixture behavior only. Zero fake Pi-file
I/O does not certify a real adapter. A passing fake suite cannot qualify the
native serializer, store/journal durability, crash recovery, or REQUIRED
activation; those require the composed real harness and independent/native
integration evidence.
