# Durable recovery evidence

Recovery evidence is an opt-in host API at `pi-fabric/recovery`. Importing the
ordinary Fabric extension does not open a store or activate recovery. The store
implements the [post-append contract](session-recovery.md#native-integration-still-required);
it does not replace the native session writer, an effect destination's deduper, or
the input-admission owner.

## What completion means

An observation such as `message_end` is not proof that native bytes were appended.
The native writer captures the exact serialized entry, location and flush receipt
at its own successful append. After native persistence has released, the host
registers that descriptor with the bound durability store and awaits
`awaitDurableAppend`. Registration alone is not an acknowledgement. Only a
successful durable barrier allows application or broker acknowledgement.

`persistRecoveryAttemptCompletion` composes raw result-event retention, native
registration and this barrier. It copies caller-owned buffers before awaiting
anything, uses the original deadline/signal, and returns `ackEligible: true` only
after both the raw event and native evidence are durable. A rejected or late
barrier leaves the raw outcome retained. The helper does not send an ACK, replay a
tool or assert that an uncertain business effect succeeded. Invoke it only from
the host's post-release drain; do not await it inside native persistence.

The host must supply a trusted verifier for native append provenance. In
particular, an owned-journal receipt hashes a committed prefix, not merely the
individual entry. A structurally plausible receipt or a later file stat is not
proof of that append. B6 does not open, scan, append to or fsync a native Pi file.
It persists its own copied evidence bytes.

The tuple `(sessionId, deliveryId, recordKind)` has one immutable binding and a
stable journal sequence. Repeating it after a crash returns the same sequence;
changed bytes do not replace the first binding. A caller that crashed after the
append but before seeing its acknowledgement must reconcile this tuple rather
than create a new delivery identity.

## Retained storage, not a second workflow service

The recovery journal is an append-only evidence store on the session's retained
storage. It has no scheduler, task queue, effect replay engine or independent
ownership service. Writers use existing custody primitives. Native old-writer
exclusion and portable custody remain the host's responsibility; a foreign lock
is not permission to take over.

A successful append includes the file and namespace durability boundary. All
physical attempts and their state/receipt events are retained separately. Logical
outcomes may dedupe at a destination, but raw attempts must not be collapsed into
one successful logical row. Torn original prefixes remain evidence; recovery does
not truncate them to make a file parse. No retention or deletion API is supplied.

Each raw segment has an exact hash-named commit witness; acknowledgement requires
both file and namespace barriers. Recovery checks every witness target and the
complete sequence history before granting authority. An incomplete witnessed range
refuses recovery. A torn segment without a witness was never acknowledged under
this protocol: its bytes remain untouched, and new appends use a new segment. A
complete append whose reply was lost is reflushed and adopted at its original
sequence, never appended a second time.

`required: true` must reopen the exact expected store rather than create an empty
replacement. An unavailable or incompletely recovered store cannot authoritatively
report an absent entry. A failed write invalidates the running handle; recover
through a successful open rather than treating cached negative results as truth.

## Off-victim inventory

Freeze the expected logical-session/input/attempt denominator before the fault.
Do not derive it from successful rows. Read retained attempts and raw trace bytes
from a verified source other than the victim, while executing the inventory from
another observer. Keep original and replacement attempt identities, incomplete
prefixes, retry counts, missing sources and unresolved evidence in the result.
No fallback to the stopped host is permitted.

A host label or remote-looking path is not independent-storage proof. The caller
must connect the inventory reader to an actual off-victim retained source and
verify that source's identity and access path. Missing or contradictory evidence
prevents a complete inventory. The library does not claim that a local filesystem
fixture establishes distributed replication, physical-host exclusion or recovery
latency.

## Gateway request joins

The [gateway source contract](recovery-gateway-joins.md) describes immutable
request-ID joins, retries and incomplete capture. Correlation is explicit, never
inferred from model text or timing proximity. Interrupted model invocations may
repeat; their request IDs and billing evidence remain separate from the scripted
business-effect ledger. No exactly-once provider-billing claim is made.

## Qualification boundary

The filesystem tests exercise the real retained store, its durability barrier,
crash/reopen behavior and immutable evidence consumers. They are not proof of the
normal native Pi acknowledgement path until its owning runtime supplies:

- post-persistence-release append descriptors for the exact bound session/file;
- trusted ordinary-file or owned-journal append provenance verification;
- acknowledgement and input-drain wiring that awaits the barrier;
- effect adapters that append intent before dispatch, record every physical
  attempt and reconcile destination outcomes before continuation;
- a verified off-victim source for the entire frozen cohort and raw traces;
- the gateway owner's complete request capture through the documented source port.

Absent native wiring leaves live Pi-path proof open. Absent gateway capture leaves
the live request join open. Neither absence is silently converted into passing
qualification. Host-stop recovery, exclusive storage transfer, a full cohort and
its end-to-end recovery deadline require separate runtime execution evidence.
