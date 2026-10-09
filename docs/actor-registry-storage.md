# Actor registry storage

`actors/<root>/actors.json` remains a format-1 metadata registry. Direct readers
(residency, retention and ownership fences) still see current ids, custody,
configuration and status. **Instructions always remain inline**, including large
instructions. Only growing message histories move out. Each externalized row retains an empty
inline `messages` array so old loaders can accept and save it safely. Bounded
filter-skip-only journals remain inline soft telemetry until substantive history
exists, preserving the existing no-fsync filter poll contract.

## Decoded read cache

Stores for the same normalized path share an immutable decoded view, bounded to
64 least-recently-read paths and released when the owning manager closes. Every
read opens the file and checks its descriptor's device, inode, size, nanosecond
mtime and ctime. Zero inode or timestamp identities are unproven and always
re-read; atomic replacements remain bound to the descriptor actually opened.

Nonzero timestamps can still be coarse (up to a two-second quantum). Each
cached generation therefore records its wall-clock read-start time. Its bytes
are reusable only when the descriptor mtime is **strictly older** than that
recorded time minus two seconds. Recent, exactly-two-second-old and future
mtimes re-read and re-validate JSON rather than returning cached bytes, even if
the identity key is unchanged. A racy entry never becomes trusted just because
time passes: the next read must first decode it again and record a new read
time. Once the file has settled, subsequent reads hit. This adds no timer or
background poll and preserves the device/inode/size/mtime/ctime key.

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
reference. New payload heads, custody and removal decisions are committed
immediately with the existing durable atomic registry rename and rollback
protocol. Before acknowledging a save, the writer also durably checkpoints each
changed accepted head to `<actor-id>/registry/messages-head.json`, under the same
registry lock. This small checkpoint is independent of unknown registry fields:
it survives a legacy owned-row save that removes `messageHistory`. Unchanged
checkpoints are not rewritten. A checkpoint failure rolls back changed
checkpoints and the registry; later commits never select an abandoned append.

Explicit registry references take precedence over checkpoints, so interrupted
publication remains readable by new releases. If a process dies between the
registry rename and completion of checkpoint publication, an old release may
obscure that **unacknowledged** head; the previously checkpointed, acknowledged
history remains recoverable. Checkpoint completion is part of acknowledgment,
not background work. A leading newline isolates a torn append; readers select
exact committed ranges and do not parse the whole growing log. Invalid or
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
