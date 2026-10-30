# A' bridge (NATS <-> file mesh): Tier 1 dry run and rollback

smarty-dev#15310, pi-fabric lane L3. The A' link is Tier 1 work (identity and access): before its first
live start on Ryzen 1 it needs the named security pass and this dry run. The pass and the rules it judges
are in [bridge-nats-threat-model.md](bridge-nats-threat-model.md); the CLI is documented in
[mesh-bridge-nats.md](mesh-bridge-nats.md).

The dry run answers one question with the real inputs, writing nothing: **if the link started now, which
identities would it carry each way, which would it refuse, and why?** The rollback section is the way
back from the live start that follows.

## What it reads and what it never writes

| Reads | Never writes |
|---|---|
| the link config (`nats-link` config file) and its allow section | no NATS KV key (no presence, no lease) |
| org.json (seat pins), the same check `start` makes | no NATS publish, no inbox consumer |
| Ryzen 1's file mesh presence, read-only (as `fabric-participants`) | no file-mesh record, event or lease |
| the org broker's KV presence and leases, with the bridge machine's credential | no ledger, no cursor, no lock, no state directory |

## Before you run it

1. The reviewed PR 1 release is installed on Ryzen 1 (a release install, rollback recorded on
   smarty-dev#15310 first), and its `bin/nats-link` is the one you run.
2. The link config file exists, is ours, `0600`, and not a symlink. Its `nats.credentials` names the
   bridge machine's `user.creds` (`0600`, ours) for machine `ryzen1-bridge`; every `allow.nats` entry
   carries its `machine`. Never print the credential.
3. Set these once in the shell you run from (all values are paths or names, no secrets):

   ```sh
   FABRIC=$HOME/.local/share/smarty-dev/fabric/releases/<PR 1 merge sha>
   CONFIG=$HOME/.config/smarty/nats-link/v2.json
   STATE=$(jq -r .stateDir "$CONFIG")
   MACHINE=$(jq -r .nats.machine "$CONFIG")
   ```

## The dry run

### 1. Prove the starting state (nothing of the link exists yet)

```sh
test ! -e "$STATE/ledger" && test ! -e "$STATE/cursor.json" && echo "no link state"
"$FABRIC/bin/nats-link" status --config "$CONFIG" | jq '{mirrors: (.mirrors | length)}'
"$FABRIC/bin/nats-link" roots --config "$CONFIG" --include-stale --kind root --kind actor --kind agent --kind provider |
  jq --arg m "$MACHINE" '[.[] | select(.machine == $m)] | length'
```

Expected (`status` exits 1 here and lists `no cursor: the link has never started with this stateDir`;
that is the starting state):

```text
no link state
{"mirrors":0}
0
```

The second line counts records this link mirrors into Ryzen 1's mesh; the third counts KV presence under
the bridge machine.

### 2. Run it

```sh
"$FABRIC/bin/nats-link" dry-run --config "$CONFIG" > dry-run.json; echo "exit $?"
jq '{link, natsMachine, live, refused}' dry-run.json
jq -r '.rows[] | [.side, .kind, .key, (.principal // "-"), .status, (.machine // "-"), (.receive | join(","))] | @tsv' dry-run.json
```

Exit codes:

- `0`: every allow entry resolved live.
- `1`: the report is complete but at least one entry is refused or unresolved; or the broker could not be
  read, in which case standard output is empty and standard error names the unavailable code. Nothing
  was written in either case.
- `2`: the config, its file checks or the org.json seat pins refused; nothing was contacted.

### 3. Expected output

`dry-run.json` is one `BridgeNatsDryRunReport` (`src/mesh/bridge-nats-identity.ts`, `bridgeNatsDryRun`).
With the PR 1 canary allow-list (one Ryzen 1 lead, Paul's seat on the file side, one supervisor actor,
one moved lead and Kate's seat on v2), the shape is:

```json
{
  "v": 1,
  "link": "v2",
  "natsMachine": "ryzen1-bridge",
  "at": "2026-10-27T12:05:00.000Z",
  "rows": [
    { "side": "file", "key": "session:<lead uuid>", "kind": "root", "mirrorTo": "nats",
      "send": ["followUp", "steer"], "receive": ["followUp", "steer"], "status": "live", "id": "session:<lead uuid>" },
    { "side": "file", "key": "session:<Paul org uuid>", "kind": "root", "principal": "Paul", "mirrorTo": "nats",
      "send": ["followUp"], "receive": ["followUp", "cancel"], "status": "live", "id": "session:<Paul org uuid>" },
    { "side": "file", "key": "actor:session:<lead uuid>/supervisor", "kind": "actor", "mirrorTo": "nats",
      "send": ["steer", "followUp"], "receive": [], "status": "live", "id": "<32 hex>" },
    { "side": "nats", "key": "session:<moved lead uuid>", "kind": "root", "mirrorTo": "file",
      "send": ["followUp", "steer", "cancel"], "receive": ["followUp", "steer"], "status": "live",
      "id": "session:<moved lead uuid>", "machine": "<moved lead machine>" },
    { "side": "nats", "key": "session:<Kate org uuid>", "kind": "root", "principal": "Kate", "mirrorTo": "file",
      "send": ["followUp"], "receive": ["followUp", "cancel"], "status": "live",
      "id": "session:<Kate org uuid>", "machine": "<Kate's machine>" }
  ],
  "live": 5,
  "refused": 0
}
```

How to read it:

- `status: "live"` means the entry's identity is live on its home side right now, unambiguous, not
  answering on the other side, and allow-listed there: the link would mirror it to `mirrorTo`.
- Any other `status` is the stable refusal code the live link would use, with `reason`:
  `BRIDGE_NATS_TARGET_UNAVAILABLE` (not live, stale, or not on its pinned machine),
  `BRIDGE_NATS_AMBIGUOUS` (the id has records on two NATS machines or answers on both sides),
  `BRIDGE_NATS_SIDE_MISMATCH` (the id lives on the other side), `BRIDGE_NATS_NOT_ALLOWED`, or
  `unresolved` (an actor entry without an id pin that the listing did not find).
- A seat row must show `receive` within `followUp, cancel`; a moved lead or Ryzen 1 row within
  `followUp, steer, ask, cancel`. Every `nats` row is pinned to a `machine`. Anything else would not have
  parsed (exit `2`).
- A refused row is not a failure of the dry run; it is what the pass and the operator must look at before
  the live start. Kate's seat refused as `BRIDGE_NATS_AMBIGUOUS`, for example, means a second machine
  holds a record for her session id: stop and find it before going live.

### 4. Prove it wrote nothing

Repeat step 1. The output must be identical: `no link state`, `{"mirrors":0}`, `0`.

## The live start this dry run precedes

Only after the named pass and a dry run with no unexplained refusal, start the link's unit (ops-lead
names it; this page uses `smarty-nats-link@v2.service`, running `nats-link start --config "$CONFIG"` with
`Restart=on-failure` and `TimeoutStopSec=10`):

```sh
systemctl --user start smarty-nats-link@v2.service
"$FABRIC/bin/nats-link" status --config "$CONFIG" | jq '{ok, mirrors: (.mirrors | length), problems}'
```

`mirrors` should equal the dry run's live `nats` rows and `problems` should be empty; `status` reports any
mirror the allow-list does not name as a problem.

## Event refusals are not link failures

A malformed or unauthorized outbound command, ACK or translated topic is a **per-event refusal**.
The link records `{kind: "refused", reason, code?}` on the existing ledger's `toNats` event-level
`seen` receipt, keyed by the source event id. This is not an owner's ACK and does not settle a command.
Only after the receipt is fsync'd does publication resolve as refused and MeshBridge checkpoint past
that event. The next event can cross; a refused event is counted as dropped, never as delivered.
The link logs `toNats: refused <topic> <event id>: <reason>` once on the first decision. A restart
reuses the receipt without retrying or re-logging the event, including a replay before the cursor
checkpoint. Receipts use the ledger's existing 24-hour horizon and compaction; keep the ledger as
well as the cursor during a restart or seat move. If a refusal receipt cannot be persisted, the
source cursor stays put and the link retries with its existing backoff.

This includes an ACK with no boolean `accepted`, and a relay/infrastructure refusal ACK: the latter
has an `accepted` field but is deliberately not an **owner** outcome. Such an ACK must not acquire
owner authority just to pass the carrier. Verified command senders still receive the existing
best-effort, deduped `notRun` refusal ACK when the carrier can safely construct one.

A **link-level safety failure still stops the link**: a fenced port (`FABRIC_BRIDGE_NATS_FENCED`),
invalid port identity/envelope binding (`FABRIC_TRANSPORT_INVALID`), denied credentials/permissions
(`FABRIC_TRANSPORT_DENIED`), terminal inbox/watch failures, or a foreign link identity at mirror time.
These are not `NatsCarrierRefusal` and cannot be downgraded to event drops by a message containing
“not allowed”. Transient transport outages, uncertain PubAcks and recognized retryable presence gaps retain
the event and retry; they are neither permanent event refusals nor link safety failures.

## Rollback

The link changes only three things: KV presence and lease records under its own machine, mirror records
with `remoteHost: <link>` in Ryzen 1's file mesh, and its own state directory. Nothing else on either side
is written, so the rollback is:

1. **Withdraw.**

   ```sh
   "$FABRIC/bin/nats-link" withdraw --config "$CONFIG"; echo "exit $?"
   ```

   It stops the running link (SIGTERM to the process in the state directory's `link.lock`, checked by its
   start time, waiting up to ten seconds), takes the lock itself, then deletes every lease and presence
   key under the bridge machine (leases first, so NATS stops routing to Ryzen 1 identities at once) and
   every record this link mirrored into the file mesh. It prints what it stopped and deleted and any
   `problems`; exit `0` only when the link is stopped and both sides are clean. If the link does not
   stop, it withdraws nothing: stop the unit (`systemctl --user stop smarty-nats-link@v2.service`; a
   requested stop exits 0 and is not restarted) and run it again.
2. **If withdraw cannot reach the broker,** stopping the unit is still a complete rollback, only slower:
   - NATS stops routing to Ryzen 1 identities within 15 s, because a NATS route needs a live presence and
     lease pair and the lease TTL is 15 s (`KV_LEASE_TTL_MS`). Presence records alone stay up to 600 s
     but do not route.
   - Ryzen 1 stops routing to v2 identities within 15 s: a mirror authorizes traffic only while the
     bridge's observation is fresh (`MIRROR_FRESH_MS`). A sender gets `FABRIC_PEER_LEASE_STALE`,
     retryable, and nothing is published.
3. **Verify.** Run the two counting commands of dry-run step 1 (`status` mirrors, `roots` under the bridge
   machine): both must print zero. The state directory remains (step 4).
4. **Keep `"$STATE/ledger"`.** Never delete it as part of a rollback. It holds the exactly-once claims of
   commands already carried; a later start reuses it so no command crosses twice. It is private (`0700`
   directory, `0600` file) and expires its own records after 24 h.
5. **Nothing else to restore.** Before A', a moved lead was not reachable from Ryzen 1 at all; after the
   rollback that is the state again. No Ryzen 1 lead, seat, supervisor or factory config was changed by
   the link.

### How the rollback is tested

- **Unit (green):** `tests/bridge-nats-ledger.test.ts` shows the ledger survives a stop and a killed
  holder process with every resolved claim, refuses a second writer, and recovers a dead holder's lock;
  `tests/bridge-nats-identity.test.ts` "bridgeNatsDryRun" shows the report is computed from presence
  reads alone; L5's `tests/bridge-nats-cli.test.ts` covers `dry-run` (no state directory created) and
  `withdraw`.
- **Real broker (L4, on the integration branch before the pass starts):** start the link between a
  temporary file mesh and a local `nats-server` (throwaway user), carry one followUp each way, then run
  `withdraw` and assert: no live route either way; zero KV records under the bridge machine and zero
  `remoteHost: <link>` records in the file mesh; other machines' KV keys and native file records
  unchanged; a restart reusing the ledger delivers nothing twice.

## Dependencies this page names

- The systemd unit name `smarty-nats-link@<link>.service` and the config path
  `~/.config/smarty/nats-link/<link>.json` are proposals; ops-lead owns the unit.
- **Credentials:** the `ryzen1-bridge` machine credential (Paul's access item) and Ryzen 1 to broker
  reachability. Without them, run the same steps against the test-org broker.
