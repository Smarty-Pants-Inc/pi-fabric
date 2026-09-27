# Records: the org's durable record on its Node

`records.*` is the record layer of [smarty-dev#754](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/754)
("the record on the Smarty Node, GitHub as an adapter"). Issues, statuses, decisions, asks, answers, results and
handoffs are **records**: append-only rows in the org's own PostgreSQL database, never updated or deleted. An edit is
a new record that `supersedes` the old one. The mesh carries each record live ("commit, then nudge"); PostgreSQL
holds it.

The layer is off by default. It adds one dependency, the [`pg`](https://node-postgres.com) driver (MIT), which is
loaded only when a session first uses `records.*` or reconciles its records inbox.

## Configuration

```json
{
  "records": {
    "enabled": true,
    "org": "smarty-pants",
    "origin": "dev1",
    "connection": { "host": "/run/smarty/smarty-pants/pg", "port": 5432, "database": "records", "user": "smarty-pants" },
    "migrate": true,
    "mirror": { "enabled": false, "repos": ["Smarty-Pants-Inc/smarty-dev"] },
    "importers": [],
    "mirrors": [],
    "admission": {
      "targets": [
        { "name": "m4max", "command": ["wal-g", "--config", "/etc/wal-g/m4max.yaml", "wal-verify", "integrity", "--json"] },
        { "name": "b2", "command": ["wal-g", "--config", "/etc/wal-g/b2.yaml", "wal-verify", "integrity", "--json"] }
      ],
      "alarmSeconds": 120,
      "refuseSeconds": 300,
      "refreshMs": 30000
    },
    "alarmTo": "org"
  }
}
```

- **One database per org** (C13), run by the org's OS user, reached over a 0700 Unix socket directory (`host`). No
  password belongs in this file; use the socket's peer authentication.
- `origin` names this Node (default: the host name). Sequences are per origin.
- `migrate` applies the schema at first use. It needs a role that may create tables and roles (the cluster owner).
- `mirror.enabled` writes the GitHub mirror's outbox rows. With it off, no rows are written and the record is the
  only copy. A repository outside `mirror.repos` gets rows in state `skipped`.
- `importers` and `mirrors` list participant ids with those roles (below).
- `admission.targets` turns on C2 admission; without targets it is off. This PR installs no archiver: standing up
  WAL-G and each org's cluster is shared infrastructure and a separate, authorized step.

## Guest API

```ts
const receipt = await records.append({
  ref: "Smarty-Pants-Inc/smarty-dev#754", kind: "status", key: "record-layer:status:1",
  text: "Building the record layer", data: { state: "in progress", eta: "~08:00Z PR" },
});
// { id, sequence, origin, topic: "record/Smarty-Pants-Inc/smarty-dev/754", ref, key, createdAt }

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
- `supersedes` names a record of the same kind on the same ref.
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
  author, `decisions`, `openAsks`, `links`, `mirror`) and its history, oldest first; page with `after: next`.
- `records.list({ org?, repo?, open?, owner?, hasOpenAsk?, updatedSince?, limit?, after? })` is a **view query** for
  boards and alarms, newest update first, with each author's status and the open asks. It is never a delivery path.

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
of kind `records.consumer-lag`, at most once per 10 minutes per consumer.

## C2 admission: the off-host recoverable frontier

An acknowledged record is safe on the Node once committed. Off the Node it is safe once continuous WAL archiving has
it. The guarantee is the **gap-free off-host recoverable frontier**: the end of the contiguous `FOUND` prefix that
`wal-g wal-verify integrity --json` reports. A `MISSING_*` range stops it, even when newer segments are present.
pgBackRest's `check` does not verify that live chain, so only WAL-G is implemented, behind the
`ArchiveFrontierProvider` interface.

The lag is how long the oldest record (or insert-position sample) past the frontier has waited. With two targets,
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
| `records` | the envelope: id, org, origin, seq, ref, kind, author, created_at, text, data, supersedes, key, payload_hash | INSERT, SELECT only; a trigger refuses UPDATE, DELETE and TRUNCATE for every role |
| `outbox` | dev-lead's #1481 row plus record_id, edit_of, request_key; unique (record_id, owner, repo, target_thread) and (owner, request_key); states pending, posted, refused, skipped, unknown | INSERT, SELECT (the mirror role also UPDATE) |
| `publication` | the mesh nudge per record | INSERT, SELECT, UPDATE |
| `consumers` | processing cursors | INSERT, SELECT, UPDATE |

Views: `current_issue`, `current_statuses`, `open_asks`, `current_links`, `current_decisions`, `mirror_state`,
`live_records`. Every service transaction runs as `fabric_records_writer` (`SET LOCAL ROLE`).

The outbox row of a status names the author's first status record on the ref in `edit_of`, and a superseding record
names the root of its `supersedes` chain, so the mirror edits one GitHub object. Each mirrored body ends with
`<!-- smarty-record:<id> -->`. An imported record (with `data.via`) is never mirrored back.

## The remote endpoint (C10)

`RecordsBackend` (`src/records/store.ts`) is the interface both the local service and the future remote records
endpoint serve: `append`, `read`, `get` and `list`, each with a `RecordsPrincipal` the server derives from the
caller. The endpoint (a follow-up) will authenticate a per-principal token issued by the org's mesh and call the
same backend; a payload never names its principal.

## Not in this layer

- The mirror's drainer (dev-lead), the GitHub importer, the board sync and `smarty log` read and write through this
  API.
- Work generations for statuses (C14) and forge alias resolution for refs are not folded yet.
- Replication (stage 2) is PostgreSQL logical replication, set up with the second Node.
