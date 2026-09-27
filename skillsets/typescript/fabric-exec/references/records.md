# Records reference

`records` is the org's durable record on its Node (PostgreSQL), when `.pi/fabric.json` enables it. Records are
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
  `state.decisions`, `state.links`, `state.open`, and the issue fields.
- `records.list({ repo?, open?, owner?, hasOpenAsk?, updatedSince? })` is a view query, never a delivery path.
- `records.status()` shows the frontier, unpublished nudges and archive admission.

## Trust boundary

The writer runs in your own Fabric process, so it is not a boundary against a caller with the same OS user: within
one org, authorship is attribution, not authentication. Records are off by default, and fleet use waits for the C10
records service (smarty-dev#1546).

Records addressed to your session (`data.to` is your participant id or session name) arrive by themselves at your
next turn start, at least once.
