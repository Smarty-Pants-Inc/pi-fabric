# Session pending inventory

```sh
pi-fabric session pending <session-uuid | session:uuid> [--mesh <meshRoot>] [--json]
pi-fabric session pending --help
```

`pi-fabric session pending` loads `src/cli/session-pending.ts`
(`runSessionPendingCli([sessionId, ...flags], io?)`) on first use. It is the
file-mesh drain inventory for a seat move (smarty-dev#15310 D3, design §3a).
Mesh selection is `--mesh`, then `$PI_FABRIC_MESH_ROOT`. An explicit root is
required: importing `resolveMeshRoot` from `src/participants-cli.ts` also imports
`MeshStore` and `ParticipantDirectory` and their runtime dependency graphs.
Unknown/repeated flags, extra arguments, and non-UUID sessions are usage errors.
`--json` is accepted for consistency; inventories are **always one JSON object**.

## JSON fields

- `format: 1`, `checkedAt` (ISO timestamp at completion), absolute `meshRoot`,
  bare `sessionId`, and `rootId: "session:<uuid>"` identify the observation.
- `basis: "recorded-files-not-live-state"`, `complete`, and `unknown: string[]`
  distinguish an inventory of readable files from an incomplete observation.
- `followUps`: `file`, `present` (boolean, or null when existence is unreadable),
  `count` (null if unknown), optional `ids` and `oldestAt` (epoch milliseconds).
  Counts include **all journal records**, including unconfirmed handovers; the
  command does not reconcile delivered receipts or follow successor mailboxes.
  An absent journal is `present: false, count: 0`.
  `owner` is absent/null or an allowlisted owner record: root/session/identity IDs,
  PID, host, activation ID, process start token, and retirement flag. No session
  path, display name, launcher grants, or message content is copied from it.
- `routing`: `successorRootId` (null if not recorded), count of routes involving
  this root, and `unfinishedRoutes` (routes without `done: true`). These are
  recorded metadata, not a claim of successful delivery or current ownership.
- `actors`: owned registry rows selected by **exact `rootId`**, with `id`, `name`,
  `scope` (project/session storage), `registry`, `rootId`, recorded `status`,
  `queueFiles`, `queued`, `inFlight`, `settledWindows`,
  `pendingDeliveryOutcomes`, and `childCompletionEntries`.
  Both `<mesh>/actors/actors.json` and first-level session registries are scanned;
  folder names alone do not prove ownership (Fabric session IDs can differ).
  Foreign-root rows are excluded. Unattributable/corrupt rows are unknown, not
  silently omitted as empty.
- `queued` counts unique checkpoint item IDs **without** `resumed: true`;
  `inFlight` counts IDs with that marker. A retained predecessor copy is counted
  once, with the resumed marker taking precedence. All lineage queue files of
  each owned actor are inspected, without adoption, cancellation, or replay.
  The marker means in-flight/**restored** custody, not necessarily a live worker.
  Settled-window and delivery-outcome counts are retained bookkeeping rows (not
  deduplicated across files). They may represent additional pending obligations.
- `childCompletionEntries` counts envelope/source/abandon names, **not** reconciled
  pending results. Any nonempty child-completion directory refuses with an
  unknown reason, even if it contains only receipt tombstones. The real
  `pending()` method takes locks, recovers archives, and acknowledges/removes
  files, so it cannot be used by this reader.
- `totals`: actors, follow-ups, queued/in-flight items, settled windows, delivery
  outcomes, and child entries. Totals are null whenever the inventory is
  incomplete; unreadable per-actor counts are also null. Missing optional
  recorded namespaces/checkpoints have zero recorded entries, not proof of
  zero live work.

## Exit codes and safety limits

- **0**: inventory printed (it may contain pending work); `--help` prints usage.
- **1**: refusal, missing mesh, unreadable/unknown-shaped state, changed observed
  files, oversized files (>64 MiB), unsupported child reconciliation, or unsafe
  file types. A JSON inventory with `unknown` or a JSON error object is printed
  on **stdout**. Parser/OS error text is not echoed, to avoid leaking content.
- **2**: usage error; a JSON error object with usage is printed on **stderr**.

**Read-only guarantee:** only filesystem stat/list/read operations are used.
No store constructors, lock creation, mkdir, repairs, migrations, acknowledgments,
rewrites, or deletion. Symlink state files/namespaces are refused. Observed
metadata of this root's state is checked again for changes: its journal, owner
and successor files, its routes (all routes are read twice, retrying a file
replaced mid-read; any change to the set naming this root refuses), its own and
owning registries, its actors'
directories and queues, and this root's checkpoint paths in every other actor
directory. Shared namespaces (the mesh root, `main-followups/`, `routes/`,
`actors/` and other sessions' registries) are listed without change tracking,
so other sessions working in a shared mesh do not fail the inventory. This is
not a transactional snapshot. Quiesce this session's writers before relying on it. Tests compare every fixture path,
size, mode, and nanosecond mtime before/after, including refusal cases.

**Not move authorization.** The tool cannot see in-memory Pi queues, undurable
actor sends, live preparation/run details, archive retry maps, unprocessed mesh
traffic, or other runtime capabilities. Registry writers do not normally persist
`inFlightRun`, `preparing`, or `pendingRunArchives`, so this command does not
invent those fields or infer live run IDs from status. A zero recorded count or
exit 0 never proves a seat move safe: missing capability, unreconciled work,
undurable continuation, or unsupported moves must STOP. Nothing is cleared or
recreated by this command.
