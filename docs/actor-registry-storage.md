# Actor registry storage

`actors/<root>/actors.json` remains a format-1 metadata registry. Direct readers
(residency, retention and ownership fences) still see current ids, custody,
configuration and status. It no longer embeds growing message histories. Short
instructions (at most 1 KiB) remain inline; longer instructions are immutable
`<actor-id>/registry/instructions-<sha256>.txt` files. The metadata contains an
`instructionsFile` digest and a small string compatibility stub.

## History and crash safety

`<actor-id>/registry/messages.jsonl` is append-only. Each transaction contains
new messages and a backwards reference to its accepted predecessor. The
registry's `messageHistory` selects an exact byte range and records the active
count (at most 100). Only the last 100 are loaded, at first history use. Listing,
status, registry saves, idle reloads and foreign-row merges do not read history.
Older messages stay archived in the same JSONL file. A reset starts a new active
history without deleting its earlier archive. Existing actor/root retention and
removal delete these files together with the actor directory; this change adds
no periodic archive rewrite or new retention job.

An append or instruction file is synced, including namespace barriers, before
publishing its reference. New payload heads, custody and removal decisions are
committed immediately with the existing durable atomic registry rename and
rollback protocol. A failed/unpublished append may leave archived bytes, but
later commits link to the previous accepted head, never the abandoned tail. A
leading newline isolates a torn tail; history readers select exact committed
ranges rather than parsing the whole growing log. Invalid/truncated history is
an error, not an empty guessed history to save back. No path in a stored reference
is trusted: paths are derived from the actor id and instruction digest.

Only rebuildable status/time changes are coalesced in a five-second window.
Creation, stop/start, messages, instructions/configuration, adoption, removals
and explicit durable release checkpoints bypass it. A pending save merges under
the registry lock using fresh ownership and foreign rows. Shutdown cancels the
timer, joins a save already running, then flushes the latest owned state. Presence
publication remains immediate; this window is not an authority/lease cache.

## Migration and downgrade

Inline format-1 registries still load. The first save archives **all** embedded
legacy messages, even when more than 100 exist, before replacing the registry
with metadata. Instructions move only when larger than 1 KiB. Foreign inline
rows migrate without losing unknown fields. Migration/new references require
both payload and registry durability barriers.

Release `6b15d905` accepts format 1, a string `instructions` and an array
`messages`; it ignores the new reference fields. The string/empty-array stubs
prevent a read-time crash, but **do not provide transparent downgrade behavior**:
it sees no retained messages and, for large instructions, only an upgrade warning.
Its next owned-row save would drop references. Do not run an old release on the
new layout without restoring inline records first:

```sh
# Stop every writer for this root; use the new checkout before downgrading.
bun scripts/actor-registry-downgrade.ts /absolute/path/to/actors/root
```

The helper locks the registry, hydrates original instructions and the last 100
active messages, removes reference fields, and durably atomically restores
format 1. It fails without guessing if a referenced file is missing/corrupt.
Sidecar archives remain, including legacy entries beyond the old loader's
100-message ring. Re-upgrading migrates the inline records again. No old release
is expected to understand archived history outside its existing ring semantics.
