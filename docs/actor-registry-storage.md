# Actor registry storage

`actors/<root>/actors.json` remains a format-1 metadata registry. Direct readers
(residency, retention and ownership fences) still see current ids, custody,
configuration and status. **Instructions always remain inline**, including large
instructions. Only growing message histories move out. Each externalized row retains an empty
inline `messages` array so old loaders can accept and save it safely. Bounded
filter-skip-only journals remain inline soft telemetry until substantive history
exists, preserving the existing no-fsync filter poll contract.

## History and crash safety

`<actor-id>/registry/messages.jsonl` is append-only. Each transaction contains
new messages and a backwards reference to its accepted predecessor. The
registry's `messageHistory` selects an exact byte range and records the active
count (at most 100). Only the last 100 are loaded, at first history use. Listing,
status, registry saves, idle reloads and foreign-row merges do not read history
on the normal new-release layout. Older messages stay archived in the same JSONL
file. A reset starts a new active history without deleting its earlier archive.
Existing actor/root retention and removal delete these files together with the
actor directory; this change adds no periodic archive rewrite or retention job.

An append is synced, including namespace barriers, before publishing its
reference, and **before the registry lock is taken** (smarty-dev#6477 L7). A
save prepares outside custody: it appends every new transaction at its
speculative offset, fsyncs the log and its namespace, then reads the bytes back.
Another appender that won the tail makes that preparation invalid; its bytes
stay behind as an unreferenced archive and the save re-selects under the lock.
Each changed checkpoint and the new registry are also written to fsynced
`*.prepared` temp files before custody. Under the registry lock a save then only
checks the registry generation and the log identities (a `stat`), renames the
registry, runs the registry's one directory barrier (durable saves), and renames
the changed checkpoints. The checkpoint directory barriers run right after the
lock is released and before the save is acknowledged. Only a save that already
lost a race prepares, and therefore appends, under the lock.

A crash at any point leaves the registry selecting only complete, durable
payloads: before the registry rename the new bytes and temp files are
unreferenced; after it they were already fsynced. Checkpoints are renamed after
the durable registry rename, so after a crash a checkpoint can lag the registry
but never lead it. `tests/actor-registry-crash-points.test.ts` crashes at every
file-system step and checks this, plus the barrier order under the lock.

New payload heads, custody and removal decisions are committed with the
existing durable atomic registry rename and rollback protocol. Before
acknowledging a save, the writer also checkpoints each changed accepted head to
`<actor-id>/registry/messages-head.json`. This small checkpoint is independent of unknown registry fields:
it survives a legacy owned-row save that removes `messageHistory`. Unchanged
checkpoints are not rewritten. A checkpoint rename failure under the lock rolls
back changed checkpoints and the registry; later commits never select an
abandoned append. A checkpoint directory barrier that fails after the lock was
released is retried once. If it still fails, the save takes the **slow path**
(pi-fabric#590 security round): it reacquires the registry lock and retries the
barrier under it. Older releases know nothing about owed barriers; the registry
lock is the only thing every supported writer honours, so no writer of any
release can commit on top of the unsynced checkpoint while the slow path runs.
The lock is released only after the barrier succeeded (the save is then
acknowledged normally) or after the commit was rolled back. If the barrier still
fails under the lock and the registry is still this commit, the registry is
durably restored to its prior bytes and the checkpoints are restored (best
effort; their directories stay owed). `update()` then rejects with a retryable
`ActorRegistryCheckpointBarrierError` whose `rolledBack` is set and whose
`committed` is undefined: nothing is acknowledged, the store's cached snapshot is
invalidated, and the manager re-appends the messages on retry. The fast path is
unchanged: a barrier that succeeds runs outside the lock.
Only if another writer committed in the short interval between the release and
the reacquisition (the generation moved) can the commit no longer be rolled back:
the error then carries the committed value, so the manager acknowledges exactly
the appended prefix instead of appending it again, and the barrier stays owed.
The next update from that store retries every owed barrier before it selects or
writes anything and, while one still fails, rejects without committing. A
removed actor directory owes no barrier. `tests/actor-registry-crash-points.test.ts`
proves the slow path with an older-release-shaped writer (drops
`messageHistory`) and an OS-crash model in which unsynced checkpoint renames are
lost: the older writer never commits over an unsynced checkpoint and recovery
keeps every acknowledged message.
A crash may leave stray `messages-head.json.*.prepared` temp files; they are
never read.

Explicit registry references take precedence over checkpoints, so interrupted
publication remains readable by new releases. If a process dies between the
registry rename and completion of checkpoint publication, an old release may
obscure that **unacknowledged** head; the previously checkpointed, acknowledged
history remains recoverable. Checkpoint completion is part of acknowledgment,
not background work. A leading newline isolates a torn append; readers select
exact committed ranges rather than parsing the whole growing log. Invalid or
truncated history/checkpoints are errors, not empty guessed histories to save
back. Paths are derived from actor ids, never from a stored path.

Only rebuildable status/time changes are coalesced in a five-second window.
Creation, stop/start, messages, instructions/configuration, adoption, removals
and explicit durable release checkpoints bypass it. Registry preparation saves
also bypass it: worker admission must observe the lock/save failure immediately. A pending save merges under
the registry lock using fresh ownership and foreign rows. Shutdown cancels the
timer, joins a save already running, then flushes the latest owned state. Presence
publication remains immediate; this window is not an authority/lease cache.

## Migration, mixed releases and downgrade

Inline format-1 registries still load. The first save archives **all** embedded
legacy messages, even when more than 100 exist, before replacing the registry
with metadata. Foreign inline rows migrate without losing unknown fields.
Instruction sidecars from the initial PR layout are supported for reading and
hydrated back inline on the next save; no new instruction sidecars are created.

Release `6b15d905` accepts format 1, a string `instructions` and an array
`messages`. Its store preserves unknown fields when directly saving raw records,
but its manager constructs owned rows from known fields only, dropping new
references. Therefore a stub-only layout was unsafe for mixed-release saves.
The current layout needs **no restore helper for preservation during rollout**:
old releases retain the original inline instructions, and their empty messages
save cannot delete the history file or its independent checkpoint. The new reader
recovers the accepted checkpoint when `messageHistory` is absent and merges old
inline additions by message id and direction, retaining the normal last-100 ring.
A missing reference or empty legacy stub is never interpreted as a history reset.
New-release `clearMessages` carries an explicit reset intent, including across
registry reloads. An old release cannot clear externalized history by saving `[]`.

The owned-save probe passes for **both `6b15d905` and `195e5ac9`**: each old
manager saves the full inline instructions and drops the selecting history
reference, after which this release recovers the checkpoint and all 100 active
messages; the 150-message archive remains byte-identical. **`e3ccd9d1` remains
unverified**: it was absent after fetching the upstream main, advertised branches
and tags, and upstream would not resolve the abbreviated revision directly.
Do not extend the two-release compatibility claim to an unidentified binary.

For a root containing any unverified old writer, use a coordinated cutover:
quiesce every Main, resident host and actor-owning process using that registry
root before the first compacting save; inventory their executable/bundle commit
identities and verify their stopped process identities and released registry/
residency locks. Restart only this release or newer against that root. A
post-install benchmark is not evidence that old writers have stopped. For
rollback with an old owner's full history view, stop all root writers again,
run the locked inline restore below, verify the restored instructions and active
ring, then resume the old release. This step is required even when an old-owned
row has removed both `messageHistory` and `instructionsFile`: the accepted ring
may then survive only in `messages-head.json`, and the helper hydrates it through
the same checkpoint-aware reader before writing the old layout. For the two
proven releases, concurrent saves preserve payloads without that helper, but old
owners still cannot *see* the
externalized ring until inline restoration.

Old releases do not themselves display externalized history. To give a fully
downgraded owner the last 100 messages inline, use the required helper:

```sh
# Stop every writer for this root before restoring the full inline view.
bun scripts/actor-registry-downgrade.ts /absolute/path/to/actors/root
```

The helper locks the registry, hydrates instructions and the last 100 active
messages (including checkpoint-only rows), removes reference fields, and durably
atomically restores format 1. Corrupt or truncated checkpoints fail closed before
any registry bytes change. Sidecar archives and checkpoints remain, including
legacy entries beyond the old
loader's 100-message ring. Re-upgrading merges the inline ring back without
truncating the archive. No old release is expected to understand archived history
outside its existing ring semantics.
