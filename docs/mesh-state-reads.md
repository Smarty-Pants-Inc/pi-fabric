# Mesh state read gates

Refs smarty-dev#4383.

## Readers

`get`, `list`, `listAll`, `listAllShared`, `stateToken`, UI stamp observation and strict
routing validation use a process-local canonical snapshot. Stores at the same resolved
state path share parsed reader snapshots; abandoned roots use weak references and the
root-name index is capped at 64. Write transactions never mutate a shared reader snapshot.

Ordinary runtime reads are exact on change, including during live invocations. Resident
host participant-file lists preserve their explicitly configured legacy observation TTL
without a floor; resident shared-state reads remain exact on change;
ownership, pruning, registry merges, routing and admission use canonical fresh reads.
No background floor applies to state bindings, ownership, routing, admission or delivery.
Only explicit `{ background: true }` display observations use `backgroundReadCacheMs`,
configured from `mesh.idleReadCoalesceMs` (default 5 seconds), with a 1-second floor.
Active turns and pending messages shorten that background-only window to 1 second.
Schema hypothesis, verification and commit state bindings explicitly request `{ fresh: true }`.
Fresh reads bypass the age window, not the physical-generation gate. The existing explicit
`MeshStoreOptions.readCacheMs` TTL remains supported for callers that choose it; it has
no new floor. Runtime/resident stores do not opt into it. Resident participant-file
list observations retain a separate explicit TTL; fresh authority lists bypass it.

The opt-in consumers are dashboard participant/peer listings, dashboard mesh entries and
UI cached-state stamp observation. Directory namespaces and participant files inherit the
opt-in only for those display listings. Heartbeat/publication preparation does **not** opt
in: migration policy, legacy reader compatibility and ownership affect decisions, even
though the refresh runs in the background.
Actor mesh watches poll on the leading event after a quiet actor cadence, and continuous
notifications inside a fixed `max(1000, mesh.actorPollMs)` window collapse into one
trailing poll. Startup, explicit and idle polls never suppress a new leading event or
slide the trailing deadline. Windows and unsupported-watcher fallback polling retain
`mesh.actorPollMs`, as on main. Explicit scheduling and paged catch-up remain prompt.
Residency delivery drains use the same interval floor; participant change refreshes already coalesce for 1 second.

Unchanged canonical metadata reuses the parse instead
of repeatedly opening/decoding megabytes. Metadata includes device, inode, size and
nanosecond mtime/ctime. One bounded 192-byte header observes the UUID and optional journal
hash when that physical identity changes. Namespace signal indexes also reuse their
parsed index while their physical identity is unchanged.

An explicit successful `confirmWritable` requires canonical revalidation on the next read
and starts a new fixed idle window, without throwing away an unchanged payload. Ordinary
cache hits do not slide the deadline. Participant indexes preserve unchanged entry
identities; on full-parse fallback they compare entry bytes, rather than allowing unchanged
version/timestamp labels to conceal a legacy name/ownership edit.

A strict readability check only shares a snapshot known to come from a valid canonical
envelope. A tolerant dashboard parse of `{}`/damaged bytes cannot certify routing absence.
Size budgets still apply before any shared or incremental snapshot is served. Full parses
validate both pre-read and post-read physical identities; a copied-marker replacement during
a read cannot label the earlier payload with the replacement's metadata.

## Optional incremental journal

New writes may publish `state.read-journal.jsonl` under the existing mesh write lock.
`readGeneration` remains the canonical first-field UUID. The optional second-field
`readJournalHash` commits a SHA-256 delta-chain head in the same atomic canonical payload.
Older builds ignore these fields and the sidecar; writer cadence and CAS revisions do not
change. `MeshStoreOptions.writeReadJournal: false` is available for compatibility probes.

A delta carries changed entries, revision updates/evictions, changed tombstone order and
the envelope metadata. It binds its predecessor's UUID, physical identity and chain hash.
Its `canonicalPayloadHash` also commits SHA-256 of the exact canonical payload bytes with
only the second-field `readJournalHash` omitted to avoid a self-referential hash. That payload
hash is inside the delta body covered by the canonical `readJournalHash`, not merely the
sidecar's recomputable outer checksum.

Readers replay from their existing snapshot and read only appended **journal** bytes after a
successful replay. Before publishing a replayed snapshot, they additionally stream/hash the
actual canonical bytes in 64 KiB chunks and require the chain-bound payload hash to match.
Descriptor and path physical identities must remain pinned before/after verification, and
the final UUID, canonical chain hash and endpoint metadata must all match. Missing bindings,
I/O failures, short reads, concurrent replacements and endpoint-only sidecar forgeries fall
back to a canonical parse. Recomputing the payload hash in a forged sidecar changes the
chain head and cannot match a copied canonical marker. Unchanged entry objects survive
verified replay, keeping derived directory indexes inexpensive.
Cursors advance only through the consumed UTF-8 prefix, not a later record appended after
the reader captured its canonical endpoint. Changed entry encodings are reused by the
canonical payload, namespace signal and journal, rather than traversing values again.

History rotates atomically at 2 MiB; an individual record is capped at 256 KiB. Missing,
stale, truncated, corrupt, oversized, disconnected or mismatched journals fall back to a
canonical read. A failed sidecar append cannot fail a committed write or hide a commit.
If the optional chain head would exceed the canonical byte budget, omit the head and
record rather than rejecting an otherwise fitting write.
Rotation or an older writer can break the chain; the next successful full read reestablishes
the base. Records from older journal writers without a payload binding also fall back. Older
writers copying an existing UUID still invalidate the physical endpoint; retargeting terminal
metadata and recomputing its outer checksum cannot override a changed canonical payload.

The metadata gate assumes cooperating replacements change the physical file identity.
An external in-place writer reproducing the entire nanosecond identity and UUID is outside
that gate. If an adapter cannot supply high-resolution identity, fresh reads retain the
conservative full-payload fallback. Write-side CAS/revision reducers still read actual
canonical bytes under the lock; their source and ABI are unchanged.

## Live leases and host retention

A failed heartbeat write does not certify death. An already-admitted live directory keeps
its ownership/incarnation and renews only that host's file liveness on failure. It does not
advance `confirmedAt`, clear the outage, grant initial admission or enable consumption.
Other senders retain #484's existing routing grace and retryable-stale behavior; a lease
lapse is not an explicit lineage-close receipt or permission to inherit work.

Native Mains and resident brokers both advertise `topology/hosts/` records. Existing needed
directory writes compact at most 64 host records whose effective matching lease expired
more than 6 hours ago. Selection and the final file-lease check happen under the existing
write; a returning host/incarnation is retained. Own/recent/malformed records are retained.
This path does not touch participants, deliveries, completion claims, result journals,
residency directories or lineage receipts, and creates no new timer/standalone write.

## Diagnosis and measurements

#468 (`ec455d4d`) is an ancestor of RC2 (`e7bc3f24`). It coalesced idle observers, but public
fresh reads still reparsed every call and each `confirmWritable` discarded the cache.
Participant ownership, actor forwarding/status consumers and root routing can therefore
bypass the idle-only savings. Root-inbox ordinary reads already use the shared store;
real inbox checkpoint writes still incur their authoritative locked read.

A 3.2 MB snapshot alone costs about 0.64 MB/s at one read per 5 seconds, so the earlier
0.48 MB/s number is not a size-independent bound. On ryzen2, an installed offline Pi with
684 seeded participant records/110 hosts and an independent ~3 Hz writer measured about
0.75 MB/s in the empty-actor ordinary-idle profile. The field's exact 16.7 MB/s was NOT
reproduced from size/write rate alone. An explicitly synthetic 5 Hz authoritative-reader
replay inside that installed Pi reproduced the amplification (~17 MB/s) on both RC2.1's
`e7bc3f24 + 652a4af8` equivalent and main `5e67a966`.

Retained task artifacts contain the before/after table, caller stacks and reproducible
probe. Rates use `/proc/PID/io` **rchar** (logical bytes, decimal MB), not cached-page
`read_bytes`; CPU uses `/proc/PID/stat` user+system ticks. Probes are inference-free,
foreground, nice-19, isolated-home/root runs after startup warmup. Synthetic observer
replay is evidence of the bypass, not a claim to reproduce the fleet's unprovided exact
actor/forward configuration. Mixed older writers without a journal remain on the bounded
canonical/coalescing fallback. The terminal trust-gap fix requires a canonical byte verification
per consumed physical generation even with new writers: journals save parsing/traversal and
unchanged-entry invalidation, not the bytes of that verification. The earlier <0.5 MB/s journal
claim does not apply to this adversarial copied-marker contract. Repeated observations of an
unchanged verified generation still reuse the shared snapshot without reopening the payload.
