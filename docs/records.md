# Records: the org's durable record on its Node

`records.*` is the record layer of [smarty-dev#754](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/754)
("the record on the Smarty Node, GitHub as an adapter"). Issues, statuses, decisions, asks, answers, results and
handoffs are **records**: append-only rows in the org's own PostgreSQL database, never updated or deleted. An edit is
a new record that `supersedes` the old one. The mesh carries each record live ("commit, then nudge"); PostgreSQL
holds it.

The layer is off by default. It adds one dependency, the [`pg`](https://node-postgres.com) driver (MIT). Only the
records service loads it.

## Architecture: the records service (C10)

Database authority lives in one place: the org's **records service**.

- **Its own OS user.** The service runs as `<org>-records`, which owns the org's PostgreSQL cluster. The cluster
  listens only on a 0700 socket directory. `pg_hba` admits that OS user alone, through peer authentication and the
  `records` ident map, and rejects everything else, TCP included.
- **Its own database role.** The service logs in as `records_service`, a member of `fabric_records_writer`. That
  role has INSERT and SELECT on the tables, and EXECUTE on the SECURITY DEFINER functions that change mutable state:
  cursors, publication acks, claims. It has no UPDATE, DELETE, TRUNCATE or DDL.
- **Migration at install only.** Migrations run as the cluster owner (`postgres`), only at install
  (`service-main.js migrate`).
- **A socket for the org's agents.** Fabric reaches the service over a unix socket in a setgid 2750 directory that
  belongs to the agents' group. Every call carries a **per-principal token**, and the service derives the caller's
  principal from it, never from the payload.
  - A Main (`session:<uuid>`) or an actor (its 32-hex id) registers its own participant id once, with an
    enrollment nonce it saved (0600, `<agent dir>/fabric/records-credentials/`) before sending. The first claim
    wins. A retry with the same nonce recovers a credential whose response was lost; any other claim is refused.
    Nothing proves the first claimant is the id's owner (see Trust boundary).
  - **Reserved identities** are never registrable: the installer grants them (`--operator ROLE:ID`) in the
    service's config and issues their credentials. An operator id can never be a session or actor id. A role needs
    both the config's grant and an issue for that role (recorded with the principal). The **importer** and the **mirror** run as `<org>-records`, and
    their credentials stay in its 0700 `/var/lib/<org>-records/credentials`. The **relay** publishes nudges on the
    org's mesh, which only the org user can write, so its credential goes to the org user (0600,
    `~/.config/<org>-records/relay.json`, Fabric's `records.relayCredentialFile`).
- **Roles in the service's config.** The `importer`, `mirror` and `relay` roles are granted in the service's own
  configuration (`/etc/<org>-records/service.json`), which the org's agents cannot write.
- **Nudges stay in Fabric.** Fabric keeps the publication relay, because the service cannot write the org's mesh. It
  also keeps the records inbox and the watchdog's wakes and alarms, and it talks to the service for everything else.
  A client that disconnects, or cancels a call, cancels that call on the service too.
- **Publication and alarm authority is the relay's.** Only the relay role may claim, ack, fail or release a
  publication, or claim an alarm, and only its own live claim. An alarm claim is honored only while its condition
  holds. A process without the relay credential still delivers by cursor and wakes its own root. A malformed frame
  ends only its own connection.
- **Responses are byte-bounded.** Every response stays near 768 KiB, and each collection continues explicitly:
  - pages, inbox replays and histories return `next`;
  - `records.get`'s fold fields are cut to 16 KiB (listed in `state.truncated`), each fold collection gets a share,
    and `state.more` gives a cursor per collection to page with `records.fold`;
  - `records.list` pages by bytes too, and each item carries at most 20 statuses and 20 open asks, with
    `statusCount` and `openAskCount`;
  - a nudge claim carries at most 2 KiB of text per record, and a recipient (`data.to`) is at most 256 characters of
    `A-Z a-z 0-9 . _ @ : / -`, checked at append; a claim that would not fit returns fewer rows and `more`.

  Every protocol line is at most 1 MiB, checked on the whole line wherever the stream splits.
- **The theft audit.** The service asks the kernel (SO_PEERCRED) who is on each connection. It stores the pid, uid,
  gid and command line with each append (`record_peers`, never in the record). When one principal's token is used
  by two live processes within an hour, it writes a one-line `alarm` to its status file.

`scripts/records-paul-steps.sh` installs all of it in one idempotent run with `--dry-run`: the user, the cluster,
pg_hba and ident, the units, the migration, and credential issuance. The service runs from a self-contained bundle
(`dist/records-service/service-main.mjs`, every dependency inlined). The installer takes the approved sha256 of
that bundle and of the node binary as required arguments (`--bundle-sha256`, `--node-sha256`; `--print-digests`
shows them for a build). Before it changes anything, it copies both once into a fresh root-only staging directory,
verifies the staged copies, and installs and runs only those bytes, in root-owned `/opt/<org>-records`. So nothing
an agent can write, before or during the install, is ever run as root or as the records user. Every path comes from
its arguments (`--package-root`, `--node`), so the script also works when copied elsewhere, such as
`/run/smarty-step.sh`. Everything written into the org user's home is written as that user. `--help` shows each
root step, what a rerun does, the success line and the rollback. Archiving is off until the WAL-G step. Until then
`max_wal_size = 1GB` is PostgreSQL's soft checkpoint target, not a hard quota: `pg_wal` can pass it under heavy
writes, and step 9 prints its size. PostgreSQL comes only from the distro package path, and its binaries and
every ancestor directory must be root-owned and not group- or world-writable before anything there is run.

The service's privileged `ExecStartPre=+/usr/bin/install -d -m 2750` creates or repairs its socket
directory with owner `<org>-records` and the org user's primary group on every start, including after
`/run` is wiped at reboot. It deliberately does not use `RuntimeDirectory=` for this directory:
systemd reapplies that setting's mode and the service's own group before each command. The service
removes a stale socket before listening, but refuses a live listener. Installer step 9 connects as the
org user (5 s timeout); permission denied or any other connection failure prevents the success line.
Rollback still stops the units, removes their files, and removes both socket directories.

## Configuration

Fabric (`.pi/fabric.json`) names only the socket:

```json
{ "records": { "enabled": true, "socket": "/run/smarty-pants-records/records.sock", "alarmTo": "org" } }
```

`credentialFile` names an operator-issued credential (the importer's or the mirror's); a process with one does not
register itself. Nothing in a caller's configuration grants a role or reaches the database.

The service (`/etc/<org>-records/service.json`, written by the install script) holds the rest:

```json
{
  "org": "smarty-pants", "origin": "dev1", "socket": "/run/smarty-pants-records/records.sock",
  "database": { "host": "/run/smarty-pants-records-pg", "port": 5433, "database": "records", "user": "records_service" },
  "migration": { "host": "/run/smarty-pants-records-pg", "port": 5433, "database": "records", "user": "postgres" },
  "roles": { "importer": ["importer:github"], "mirror": [] },
  "mirror": { "enabled": false, "repos": ["Smarty-Pants-Inc/smarty-dev"] },
  "admission": {
    "targets": [
      { "name": "m4max", "command": ["wal-g", "--config", "/etc/wal-g/m4max.yaml", "wal-verify", "integrity", "--json"] },
      { "name": "b2", "command": ["wal-g", "--config", "/etc/wal-g/b2.yaml", "wal-verify", "integrity", "--json"] }
    ],
    "alarmSeconds": 120, "refuseSeconds": 300, "refreshMs": 30000
  },
  "anchorExport": { "directory": "/var/lib/smarty-pants-records/anchors", "intervalMs": 300000 },
  "statusFile": "/var/lib/smarty-pants-records/status/smarty-pants.status.json"
}
```

- `origin` names this Node. Sequences are per origin.
- `mirror.enabled` writes the GitHub mirror's outbox rows. With it off, no rows are written and the record is the
  only copy. A repository outside `mirror.repos` gets rows in state `skipped`.
- `admission.targets` turns on C2 admission; without targets it is off. WAL-G is set up in its own authorized step.
  Until then, archiving is off (`archive_mode = off`): WAL is recycled after each checkpoint, and nothing is copied
  off this host.

## Guest API

```ts
const receipt = await records.append({
  ref: "Smarty-Pants-Inc/smarty-dev#754", kind: "status", key: "record-layer:status:1",
  text: "Building the record layer", data: { state: "in progress", eta: "~08:00Z PR" },
});
// { id, sequence, origin, topic: "record/Smarty-Pants-Inc/smarty-dev/754", ref, key, createdAt }

let cursor = 0; // your saved processing cursor
const page = await records.read({ after: cursor, limit: 100 });   // { records, next, frontier, origin }
const { state, history, next } = await records.get({ ref: "Smarty-Pants-Inc/smarty-dev#754" });
const board = await records.list({ repo: "Smarty-Pants-Inc/smarty-dev", open: true });
const health = await records.status();
```

### append

`records.append({ ref?, repo?, kind, key, text?, data?, supersedes? })` returns a receipt **only after the commit**
(`synchronous_commit` on). It is one transaction under a per-org advisory lock, so commit order equals `sequence`.

- **`key` is required (C3).** It is unique per (org, author, key), with the payload's hash. An identical retry
  returns the original receipt; a different payload under the same key is refused (`RECORD_KEY_CONFLICT`). After a
  timeout or any error, retry with the same key.
- **The author is the authenticated caller** (the session's or actor's participant id), never a payload field. A
  caller with the **importer** role may set `author` (for example `github:paul`) and must set `data.via`; `data.via`
  and `data.githubId` are refused from anyone else. Only the **mirror** role may append `mirror` records.
- `repo` without `ref` creates an issue (`kind: "issue"`, `data.title` required) and allocates its immutable
  Node-native ref, `Owner/repo#L<n>` (C11).
- `supersedes` names a record of the same kind on the same ref. Only its author may supersede an author-owned record
  (every kind except `issue`, which is shared: a stage command supersedes it).
- Record ids in `data` (`ask`, `mirrorOf`) are stored lowercase.
- A cancelled call (its `fabric_exec` aborted or timed out, or the service closing) starts no further step, and an
  abort before COMMIT destroys its connection, so nothing it started commits later. Only an abort during COMMIT itself
  leaves the outcome unknown: retry with the same key. Every transaction has a 30 s lock and 60 s statement bound.
- Unknown kinds and fields are refused. Text is at most 64 KiB, data at most 64 KiB.

| Kind | data | Fold |
|---|---|---|
| `issue` | title, body, owner, acceptance, labels, nextAction, stage | each field's newest value is the issue |
| `status` | eta (a string or `[{stage, at}]`), state (`in progress`, `waiting`, `blocked`, `done`), waitOn | the newest per author |
| `comment` | deleted? (text required) | history |
| `decision` | by, where (text required) | listed until superseded |
| `ask` | to (required), class, minutes (text required) | open until an `answer` names it |
| `answer` | ask (record id), outcome (`answered` or `withdrawn`) | closes the ask |
| `handoff` | to (required), key | history; reaches `to`'s inbox |
| `link` | pr, issue, commit, url, forge, number | listed |
| `close` / `reopen` | reason | the newest sets `open` |
| `mirror` | mirrorOf, target, url, githubId, state, attempts, error | the newest per mirrored record |

### read, get, list

- `records.read({ after, limit?, origin?, ref?, kind?, to? })` pages **events** in commit order after a processing
  cursor, up to the committed frontier, from one snapshot. `next` is the cursor to save **after** you act on the
  page. Every consumer that must not miss a record reads this way (C4).
- `records.get({ ref, after?, limit? })` returns the ref's fold (`title`, `owner`, `stage`, `open`, `statuses` per
  author, `decisions`, `openAsks`, `links`, `mirror`) and its history, oldest first; page with `after: next`. When
  `state.more` is present, a collection continues: page it with
  `records.fold({ ref, part, after: state.more[part] })` until a page has no `next`.
- `records.list({ org?, repo?, open?, owner?, hasOpenAsk?, updatedSince?, limit?, after? })` is a **view query** for
  boards and alarms, newest update first, with up to 20 authors' statuses and 20 open asks per item (and their
  counts). It is never a delivery path.

## The hash chain and anchors (#754 R3)

The record is append-only and tamper-evident. Each record's `prev_hash` is the SHA-256 of the org's previous
record's canonical bytes: the UTF-8 of the stored row as JSON with sorted keys, every column in its PostgreSQL text
form (`created_at` as its exact epoch, `extract(epoch FROM created_at)::text`: seconds with six decimals, era-complete,
`Infinity`/`-Infinity` spelled out), NULL as `null`. The append sets it inside its transaction,
under the per-org lock; migration v3 backfilled the rows before it. The first record's `prev_hash` is NULL.

- `records.anchor()` (or `service-main anchor --config FILE`) returns `{ org, seq, hash, at }` for the last record.
  The service also publishes it without a socket caller, as described below. The backup adapter copies the
  export to every backup target: at least every 5 min on the admission target, daily on the others. A copy
  off the host is what makes a rewrite of the whole chain detectable.
- `records.verify({ anchors })` (or `service-main verify-chain --config FILE --anchors FILE`, exit 0 clean, 1 broken,
  3 unanchored) recomputes the chain from one snapshot and reports the first `break` (`org`, `seq`, `reason`
  `prev_hash` or `gap`, `expected`, `found`), checks each anchor (the row at that seq exists with that hash), and
  reports `unanchored: { from, to }` for rows after the latest anchor. It scans the whole table: a row of another org
  or origin is a break (`unexpected org at seq N`). While such a row is in the table, every other call that reads or
  appends records (read, get, fold, list, byIds, anchor, append, the relay's claim) refuses with `RECORD_INTEGRITY`. One verify runs at a time;
  another caller meanwhile gets a retryable `RECORD_BUSY` at once (no queue). It takes at most 10000 anchors; the
  result counts them (`anchors: { checked, passed, failed }`) and lists only the first 20 failures (`failedAnchors`), so
  it stays small. The empty-chain anchor `{ seq: 0, hash: null }` (before the first record) is valid and vouches for
  nothing; any other null or malformed hash is refused. `ok` means no break and every anchor holds;
  `clean` also needs no unanchored rows. An edit after the latest anchor that no later row covers shows only as
  unanchored, so a result is clean only when it is anchored through the last row.

### Service-owned anchor export (#1754 / backup F1)

The records service, running as `<org>-records` (for this org, `smarty-pants-records`), computes an anchor
**at startup and every five minutes by default**, reusing its idle watchdog tick and `RecordStore.anchor`.
`anchorExport.directory` and `anchorExport.intervalMs` are configured **only** in the protected service
configuration, not Fabric's caller config; restart the service to apply changes. Intervals are bounded to
1 second–24 hours and tick timing is best-effort, not a real-time deadline. A database that has not changed
(`seq` and `hash` identical) does not produce another line. A changed hash at the same sequence is retained.
The installed config defaults to `/var/lib/<org>-records/anchors`. Existing configs without `anchorExport`
but with `statusFile` automatically publish to `anchors/` beneath that status file's directory; without
either a status path or an explicit export directory, no export is configured. The authenticated `status`
response and the existing public status JSON both name `anchorExport.directory`, `intervalMs`, the last
exported anchor (`last`), and any publication `error`. Tick failures retry on the next tick; a startup
publication failure prevents startup. Archive admission being disabled does not disable anchor export.

The factory's unattended **user** backup worker reads/copies `anchors-000000000001.jsonl`,
`anchors-000000000002.jsonl`, … from that directory, in numbered order, and retains all segments. It
needs **no principal, credential, sudo, database access or service config access**. Ignore dotfiles
(the service's publisher lock and any unpublished temporary files). The export contains no token or record
payload. Each UTF-8, newline-terminated JSON line is exactly the existing anchor format:

```json
{"org":"smarty-pants","seq":42,"hash":"<64 lowercase hex SHA-256 digest>","at":"2026-09-30T12:00:00.000Z"}
```

`seq` is the database sequence, `at` is the computation's ISO timestamp, and `hash` is the digest.
An empty database exports `{ org, seq: 0, hash: null, at }`. Each segment has **at most 9,999 anchors**.
Only the latest incomplete segment grows; full/abandoned segments are immutable. New segments are
written to a temporary file, fsynced and atomically renamed; existing ones use `O_APPEND` and fsync.
The directory is fsynced after publication. No earlier line or numbered segment is ever rewritten or
deleted. The directory is owned by the records user and mode **0755**; segments are owned by that user
and mode **0644**, explicitly set regardless of the service's umask. Parent directories must also permit
factory traversal (the installed records home is 0755). The factory can read, but cannot write, this export.

A crash/short write can leave a trailing fragment without a newline. Readers must take **only the complete,
newline-terminated prefix** of their snapshot; never parse or discard a malformed *complete* line. On
restart the service leaves the fragment byte-for-byte unchanged, seals that segment, and publishes later
anchors in a new numbered segment. A crash before rename leaves only an ignored dotfile; a crash after
publication is deduplicated from the retained anchor. Copying a growing segment also requires this prefix
rule, and backups must not lose the retained fragments/earlier bytes. To verify, feed each segment's complete
prefix separately to the existing `verify-chain --anchors FILE` (as the records user, or via an authenticated
caller); do not concatenate an unbounded history into one request. Every segment must have `ok: true`
(no chain break and all its anchors pass); the newest segment must additionally have `clean: true` to
vouch through the database frontier. Older segments may correctly report exit 3 / `unanchored` for later
rows. Socket authentication and the existing `anchor`/`verify` methods are unchanged.

## Commit, then nudge

Each append writes a `publication` row in its transaction. After the commit, a relay publishes it on the mesh topic
`record/<owner>/<repo>/<n>` (kind `record.<kind>`, `to` = `data.to` when present) and marks it published only after
the mesh holds it. A crash anywhere in between leaves the row unpublished, and the next relay run (the next append,
the watchdog, or the next process) publishes it again. A nudge is at least once; deduplicate by `data.id`.

## The records inbox and the idle watchdog (C4)

A root session reconciles the records addressed to it (`data.to` is its participant id or session name: asks and
handoffs) in the same hooks as the mesh root inbox: at every turn start, and when a completed run settles. Its
processing cursor lives in the database's `consumers` table. A batch is pending until the session's own entries hold
its message, so delivery is at least once and a stop before the write loses nothing. A consumer starts at the present.

The watchdog runs every `watchdogMs` (and `admission.refreshMs`). It republishes unpublished nudges, refreshes the
archive frontier, and compares each consumer's cursor with the records addressed to it. A consumer that lags more
than `consumerLagSeconds` (2 min) is woken when it is this process's root, and otherwise gets an `ops.records` event
of kind `records.consumer-lag`, at most once per 10 minutes per consumer across every process (claimed in the
`consumers` row).

## C2 admission: the off-host recoverable frontier

An acknowledged record is safe on the Node once committed. Off the Node it is safe once continuous WAL archiving has
it. The guarantee is the **gap-free off-host recoverable frontier**: the end of the contiguous `FOUND` prefix that
`wal-g wal-verify integrity --json` reports. A `MISSING_*` range stops it, even when newer segments are present.
pgBackRest's `check` does not verify that live chain, so only WAL-G is implemented, behind the
`ArchiveFrontierProvider` interface.

Each target's check runs in one process per refresh interval for the whole database: the process that claims the
target's `archive_checks` row runs `wal-g`, and every other process reads the stored frontier. A failed check keeps the
last good frontier and records the error.

A record is covered only when the frontier passes its **recovery bound**: the WAL insert position read after its
COMMIT, stored in `record_bounds` (never the position before the insert, since a transaction can begin in one segment
and commit in the next). A crash between a commit and its bound leaves one record unbounded; the next append, check or
service start bounds it with the current insert position, which is past its commit. The lag is how long the oldest
uncovered record (or insert-position sample) has waited; the bounds persist, so a restarted gate still sees it. With two targets,
the fresher frontier counts, so one network cut does not stop records. A provider that stops answering keeps its last
good frontier, so the lag grows: the gate fails closed.

- At `alarmSeconds` (2 min) the gate publishes an `ops.records` event of kind `records.archive-lag`, and writes the
  status file (`<mesh root>/records/<org>.status.json`, or `statusFile`) that the factory check reads.
- Past `refuseSeconds` (5 min), `records.append` refuses new records with the distinct, retryable error
  `record archive lagging N s; retry with the same key` (code `RECORD_ARCHIVE_LAGGING`). Retry by key; never rewrite
  the record. An identical retry of an already committed record still returns its receipt.

## Tables and grants

| Table | What | Service grants |
|---|---|---|
| `records` | the envelope: id, org, origin, seq, ref, kind, author, created_at, text, data, supersedes, key, payload_hash; CHECKs keep ids in data lowercase | INSERT, SELECT only; a trigger refuses UPDATE, DELETE and TRUNCATE for every role |
| `outbox` | dev-lead's #1481 row plus record_id, edit_of, request_key; unique (record_id, owner, repo, target_thread) and (owner, request_key); states pending, posted, refused, skipped, unknown | INSERT, SELECT (the drainer will update state through the service) |
| `publication` | the mesh nudge per record | INSERT, SELECT; claim, ack, fail and release by function |
| `record_bounds` | each record's recovery bound (C2) | INSERT, SELECT |
| `consumers` | processing cursors | SELECT; open and save by function |
| `archive_checks` | the last archive frontier per target, and who checks it now | SELECT; claim and record by function |
| `alarms` | one alarm per key per window | SELECT; claim by function |
| `principals` | registered and issued principals (token and nonce hashes) | INSERT, SELECT; re-enrollment by function |
| `record_peers` | the kernel-reported peer process of each append (the theft audit) | INSERT, SELECT |

Views: `current_issue`, `current_statuses`, `open_asks`, `current_links`, `current_decisions`, `mirror_state`,
`live_records`. The service logs in as `records_service`, and every transaction also runs as `fabric_records_writer` (`SET LOCAL ROLE`).

The outbox row of a status names the author's first status record on the ref in `edit_of`, and a superseding record
names the root of its `supersedes` chain, so the mirror edits one GitHub object. Each mirrored body ends with
`<!-- smarty-record:<id> -->`. An imported record (with `data.via`) is never mirrored back. An issue update PATCHes
only the fields it carries (title, labels, body); one that changes nothing the forge shows (a stage command) gets a
`skipped` row. Creating an issue (`repo`, no `ref`) POSTs the whole issue.

## Trust boundary

C10 closes direct SQL, DDL, trigger removal, self-assigned roles and reserved identities (a real OS boundary:
separate OS user, peer-only pg_hba, a 0700 socket directory). It does not authenticate same-uid session identities:
any process running as the org's OS user can read that user's tokens, or register a known session id before its
owner does, and write as it. Such writes are attributed, not proven, until per-agent OS users (#820 stage 2+), and
every append records its kernel-reported peer process in `record_peers` for audit.

- **Closed by the records service (C10).** An ordinary agent, running as the org's OS user, cannot reach
  PostgreSQL. `pg_hba` rejects it, and the socket directory belongs to the records user, mode 0700. It cannot run
  DDL, remove the append-only trigger, give itself a role, or register a reserved identity (importer, mirror,
  relay). The tests show these on a cluster started with the install's own pg_hba and ident.
- **Protected: reserved identities.** The importer and the mirror run as `<org>-records`, with credentials only in
  that user's 0700 directory, so an org agent cannot read them.
- **Attributed, not authenticated: same-uid session identities, until #820's per-agent users.**
  - Agents that share the org's OS user can read one another's memory and credential files.
  - A process can register a known session id before its owner does.
  - The relay's credential is the org user's.

  Within one org, a session's authorship is attribution. The audit makes theft visible (the peer process of each
  append, and the token-reuse alarm) but does not prevent it.
- **Why a bad relay is bounded.** A publication is only the live nudge. Every consumer reconciles by `records.read`
  cursor at turn start and settle (C4), so a forged or dropped ack delays delivery and never loses a record (tested).
- Across orgs, #820's per-org OS users are the boundary.

## Remote hosts (C10, later)

The socket protocol (`src/records/protocol.ts`) is the endpoint a remote host will use over Tailscale, with the same
per-principal tokens. `RecordsBackend` and `RecordsOps` are the interfaces both the local client and the service
serve.

## Not in this layer

- The mirror's drainer (dev-lead), the GitHub importer, the board sync and `smarty log` read and write through this
  API.
- Work generations for statuses (C14) and forge alias resolution for refs are not folded yet.
- Replication (stage 2) is PostgreSQL logical replication, set up with the second Node.
