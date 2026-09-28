# Records reference

`records` is the org's durable record on its Node (PostgreSQL, behind the org's records service), when
`.pi/fabric.json` enables it. Records are
append-only; an edit is a new record with `supersedes`. The author is always the calling participant. Full contract:
the package's `docs/records.md`.

## Append

```ts
const receipt = await records.append({
  ref: "Smarty-Pants-Inc/smarty-dev#754", kind: "status", key: "my-lane:status:3",
  text: "PR open, waiting on review", data: { state: "waiting", waitOn: "pi-fabric-review-astra", eta: "~08:00Z merged" },
});
```

- `key` is required. Choose it before the first try and **retry with the same key** after any error or timeout: an
  identical retry returns the original receipt; a changed payload under the key is refused. Use a new key for a new
  record.
- `record archive lagging N s; retry with the same key` is retryable: wait, then retry with the same key.
- Kinds: `issue` (`repo` without `ref` creates one), `status`, `comment`, `decision`, `ask` (`data.to` required),
  `answer` (`data.ask`, `data.outcome`), `handoff` (`data.to`), `link`, `close`, `reopen`. Unknown kinds and fields
  are refused.

## Read

- `records.read({ after: cursor, limit })` pages events in commit order up to the committed frontier; save `next`
  as your cursor only **after** acting on the page.
- `records.get({ ref })` returns `{ state, history, next? }`: `state.statuses[author]`, `state.openAsks`,
  `state.decisions`, `state.links`, `state.open`, and the issue fields. If `state.more` is set, a collection
  continues: `records.fold({ ref, part, after: state.more[part] })`, then each page's `next`.
- `records.list({ repo?, open?, owner?, hasOpenAsk?, updatedSince? })` is a view query, never a delivery path.
- `records.status()` shows the frontier, unpublished nudges and archive admission.
- `records.anchor()` gives `{ org, seq, hash }` of the last record; `records.verify({ anchors })` recomputes the hash
  chain and checks those anchors: `ok` (no break, anchors hold), `clean` (also no `unanchored` rows), `summary`.

## Trust boundary

`records.*` reaches the org's records service over its socket. The service runs as its own OS user and derives your
principal from your session's credential. You cannot reach the database or take a reserved role (importer, mirror,
relay). Agents that share the org's OS user could still steal each other's credentials, so within one org a
session's authorship is attribution, not authentication, until per-agent OS users (#820).

Records addressed to your session (`data.to` is your participant id or session name) arrive by themselves at your
next turn start, at least once.
