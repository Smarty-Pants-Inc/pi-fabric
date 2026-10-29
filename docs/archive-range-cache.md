# Archive range reads and retiring old hints

Range reads write no files. Fine-grained hints have a process-wide 32 MiB / 128-day
LRU, stored as packed Float64 buffers (32 bytes per block, with separately charged
object/string overhead). An independent 4 MiB / 4,096-day LRU keeps only sequence
extrema and an exact segment-identity fingerprint per day, not all the range blocks. Eviction from the
fine tier does not discard day bounds. A range outside a day is skipped without
opening or parsing any segment, after checking the names, segment stats, directory
and seal generation. Bounds are derived from fenced scans, never SEAL or digest
counters or old range sidecars. Unknown extrema disable the skip. Reads and retries
reuse summaries across archive objects; a cold process must scan once. These are
retained-cache accounting caps, not total heap or RSS limits. Summary eviction is
safe but can require another scan on archives exceeding the summary budget.

## One-time migration before backing up sealed days

Old software left `.range-<sha256>.json` and `.ranges.json` in day directories. New
readers ignore them, but a strict seal backup still refuses those directories until
they are retired. Run this explicit maintenance once for each archive root:

```sh
bun scripts/retire-archive-range-sidecars.ts ABSOLUTE_ARCHIVE_ROOT > retirement-receipt.json
```

Stop old versions that produce range sidecars before starting. Current readers and
publishers may remain online: only already-sealed days are eligible. Do not allow
concurrent administrative renames, repairs, unsealing, or hostile path replacement.
The archive root must be a trusted local directory on one filesystem. A changed
source or seal fails verification; investigate instead of suppressing that error.

The migration moves only the exact legacy names (the range suffix is 64 lowercase
hex digits), using atomic rename to an exclusive fresh directory:

```text
ARCHIVE_ROOT/retired-range-sidecars/YYYY-MM-DD-UNIQUE/ORIGINAL_NAME
```

It does not delete or overwrite anything, follow symlinks, or modify event data,
digests, abort markers, seals, or unsealed days. Sidecar symlinks themselves move;
their targets are not opened. Hard links retain their inode and link count.
Before each rename, source device/inode/mode/link count/size/nanosecond timestamps,
content SHA-256 (or symlink text), seal identity and real-directory ancestry are
checked. After each rename the same inode, bytes, metadata and seal are checked
again (ctime may change because of rename). Source and destination directories are
synced on POSIX. No destination name existed: its parent was exclusively created.
A failure stops the migration without deleting or rolling back anything; a
post-rename failure may leave the preserved file in the retired directory. Inspect
both paths named by the error. Pathname checks detect ordinary races, not an
adversary controlling directory ancestry between syscalls.

Rerunning with no remaining sidecars moves nothing and creates nothing. Interrupted
runs can be rerun: already-retired files stay untouched, with no completion marker
that could hide unfinished days. Open event-file descriptors remain valid across
sidecar renames. Directory-identity fences make an overlapping range snapshot retry,
not return a partial page. The retired tree is outside the sealed day namespace;
keep it with the backup as rollback evidence. To roll back, stop sidecar writers,
verify the receipt identities/digests and that original names are absent, then
rename the preserved files back. Never overwrite a newly created original name.
