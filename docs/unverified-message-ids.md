# Advisory identifiers in outgoing coordination text

`agents.steer`, `agents.followUp` (both the standalone and hosted provider scopes),
and textual `mesh.publish` sends check whether the **sender has read** each
recognized identifier. A missing read does not refuse or suppress delivery.
The sender receipt gains an optional one-line `notice`, and the exact same line
is appended to the delivered/durable text after a blank line:

```text
unverified ids: abc1234, comment 1234567890
```

A checker/import/history failure produces `unverified ids: check failed` in both
places and still attempts the send. Normal delivery failures, capability gates,
reserved mesh topics and payload limits retain their existing behavior. Targets,
structured `data`, and host-authored lifecycle deliveries are not checked. The
standalone `tell` alias uses the same follow-up routing and advisory behavior.

## Precise, conservative classes

Matching is case-insensitive; identifiers are deduplicated in first-mention order.
Except the explicit URL fragment, identifiers must not touch a word character or
hyphen on either side. Prose hex such as `decafed` or unqualified `abc1234` is not
an outgoing SHA candidate.

| Class | Outgoing form |
| --- | --- |
| Fabric session | `session:` followed by an 8-4-4-4-12 hex UUID; or a bare UUIDv7 beginning `01a0` (version nibble `7`, variant nibble `8`, `9`, `a` or `b`) |
| Actor/run | Exactly 32 hex digits, bare or prefixed `actor:` / `run:` |
| Git SHA | 7–40 hex digits only after the whole word `sha`, `commit`, `head`, `base`, `revision`, or `rev`, separated by horizontal whitespace or `:` / `=` (optional JSON key/value quotes); or alone inside a single pair of inline backticks, not a triple-backtick fence |
| GitHub comment | 9–10 decimal digits after `#issuecomment-`, or `comment` with zero or more horizontal-space / `:` / `=` / `#` / `-` separators |
| PID | 1–10 decimal digits after `pid`, with those same separators |

An explicitly SHA-qualified 32-hex token is a SHA abbreviation, not also an
actor/run at that position. Raw full SHAs and ordinary issue numbers are not
outgoing candidates. A read may contain a raw identifier (e.g. git stdout or
JSON API output). SHA abbreviations must be exact tokens or prefixes of a
7–40-hex read token; a raw 32-hex actor prefix alone is not SHA evidence, whereas
a SHA-qualified 32-hex read is. Comment/PID labels are normalized in notices.

## Read evidence and bounds

The checker uses the host's existing indexed `sessionManager.getLeafId()` /
`getEntry(id)` API, walking the sender's current branch newest-first. It never
copies all entries or the full branch, reads session files, starts a subprocess,
or queries a network/mesh/GitHub service to validate a guess.

Evidence is visible text from finalized `toolResult` messages, incoming Pi
user-role steer/follow-up messages, and received custom/inbox messages. Thus
actual sender-session reads of `agents.list`, `agents.peers`, `agents.create`,
git output, or API output do not acquire a warning. Assistant text, thinking,
tool arguments, system messages, custom state, hidden `details`, branch summaries
and compaction summaries are not positive evidence. Hidden nested return values
that have not entered the sender's finalized history are not reads under this
contract; return the relevant tool output before a subsequent send.

A send receipt is not a read of the sender's guesses. Send-only Fabric action
metadata is used only to **exclude** evidence, never to establish it. In mixed
JSON / Fabric-generated YAML results, receipt nodes with the advisory `notice`
are excluded without dropping sibling reads; YAML's hoisted raw sections follow
those nodes. A separate later tool read of durable marked text *is* evidence.
Self-delivered messages are excluded using the sender's session/participant
identity; batched follow-ups filter each visible self-authored envelope rather
than treating the whole batch as if its first sender wrote it.

Hard data/work bounds:

- At most 500 parent-linked history entries and 2 MiB of UTF-8 text.
- At most 2,000 content blocks, plus an aggregate 2,000 trace/receipt metadata
  items across the entire walk (not a fresh allowance per entry).
- At most 256 KiB outgoing text and 128 distinct candidate identifiers.
- An incomplete final history block is not evidence (clipping could hide a
  receipt marker or invent a token boundary). Older/out-of-budget reads can
  therefore produce a conservative warning. Corrupt/missing history, cycles,
  unavailable APIs, and parser/message/identifier budget errors fail open.

No-candidate ordinary text returns unchanged without loading the checker or
accessing history. The provider helper only holds the cheap lexical prefilter;
`coordination/unverified-ids.js` is a stable first-use lazy build entry. Registration
and idle hooks do not load it. Candidate-union filtering avoids scanning every
unrelated history block once per outgoing identifier.

This is bounded **read provenance**, not proof that an identifier exists, is
current, belongs to the named repository, or is authorized. Arbitrary guest
transformations of tool output cannot supply a stronger provenance guarantee.
The deeper existence resolver remains in the readonly-reviewed
`smarty-pants/bin/smarty-status` `unresolved` implementation: it checks local git
objects/org mirrors and GitHub commits/comment IDs. No such lookup is added to
Fabric's send path (Smarty-Pants-Inc/smarty-dev#1612, #2175).

## Verification

`tests/unverified-ids-provider.test.ts` replays nine **synthetic**, not historical,
#1612-class wrong identifiers across five surfaces: standalone steer/followUp,
hosted steer/followUp, and mesh publish. It covers actual session reads, untouched
no-ID text, failure-open sends, self/receipt echoes, all standalone local/remote
routing branches, hosted child/peer ports and actual local durable mesh writes.
`tests/unverified-ids.test.ts` covers boundaries, budgets and mixed-format reads;
`tests/unverified-ids-startup.test.ts` covers cold registration, idle hooks,
no-ID sends and first use. Guest types expose the optional receipt notice.

Run `bun scripts/benchmark-message-identifiers.ts` for first-use, no-ID, nine-ID
and 128-ID measurements over 10 KiB messages / 2 MiB history. Timings are measured
rather than flaky CI limits. Build artifacts keep the checker outside the exact
startup closure and preserve the existing budgets; compare startup with fresh,
host-preloaded processes using `bun run benchmark:startup BASE FIX`.
