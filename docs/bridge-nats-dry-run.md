# A' bridge (NATS <-> file mesh): Tier 1 dry run and rollback

smarty-dev#15310, pi-fabric lane L3. The A' link is Tier 1 work (identity and access): before its first
live start on Ryzen 1 it needs the named security pass and this dry run. The pass and the rules it judges
are in [bridge-nats-threat-model.md](bridge-nats-threat-model.md).

The dry run answers one question with the real inputs, writing nothing: **if the link started now, which
identities would it carry each way, and which would it refuse, and why?** The rollback section is the
tested way back from the live start that follows.

## What it reads and what it never writes

| Reads | Never writes |
|---|---|
| the link config (`nats-link` config file) and its allow section | no NATS KV key (no presence, no lease) |
| org.json (seat pins) | no NATS publish, no inbox consumer |
| Ryzen 1's file mesh presence, read-only (as `fabric-participants`) | no file-mesh record, event or lease |
| the org broker's KV presence and leases, with the bridge machine's credential (read grants of the standard Path B template) | no ledger, no cursor, no state directory |

## Before you run it

1. The reviewed PR 1 release is installed on Ryzen 1 (a release install, rollback recorded on
   smarty-dev#15310 first), and its `bin/nats-link` is the one you run.
2. The link config file exists, is ours, `0600`, and not a symlink; its `nats.credentials` names the
   bridge machine's `user.creds` (`0600`, ours) for machine `ryzen1-bridge`. Never print the credential.
3. Set these once in the shell you run from (all values are paths or names, no secrets):

   ```sh
   FABRIC=$HOME/.local/share/smarty-dev/fabric/releases/<PR 1 merge sha>
   CONFIG=$HOME/.config/smarty/nats-link/v2.json
   MESH=$(jq -r .file.mesh "$CONFIG")
   LINK=$(jq -r .link "$CONFIG")
   STATE=$(jq -r .stateDir "$CONFIG")
   ```

## The dry run

### 1. Prove the starting state (nothing of the link exists yet)

```sh
test ! -e "$STATE/ledger" && echo "no ledger"
"$FABRIC/bin/fabric-participants" --mesh "$MESH" --include-stale | jq --arg link "$LINK" '[.[] | select(.remoteHost == $link)] | length'
"$FABRIC/bin/nats-link" status --config "$CONFIG" | jq '{running, natsMirror, fileMirror}'
```

Expected:

```text
no ledger
0
{"running":false,"natsMirror":0,"fileMirror":0}
```

### 2. Run it

```sh
"$FABRIC/bin/nats-link" dry-run --config "$CONFIG" > dry-run.json; echo "exit $?"
jq '{link, natsMachine, live, refused}' dry-run.json
jq -r '.rows[] | [.side, .kind, .key, (.principal // "-"), .status, (.machine // "-"), (.receive | join(","))] | @tsv' dry-run.json
```

Exit codes: `0` every allow entry resolved live; `1` the report is complete but at least one entry is
refused or unresolved; `2` the config, the credential file checks or the org.json seat pins refused
(nothing was contacted); `3` the broker could not be read (nothing was written).

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
  `followUp, steer, ask, cancel`. Anything else would not have parsed (exit `2`).
- A refused row is not an error of the dry run; it is what the pass and the operator must look at before
  the live start. Kate's seat refused as `BRIDGE_NATS_AMBIGUOUS`, for example, means a second machine
  holds a record for her session id: stop and find it before going live.

### 4. Prove it wrote nothing

Repeat step 1. The output must be identical (`no ledger`, `0`, `running: false` with zero mirrors).

## The live start this dry run precedes

Only after the named pass and a dry run with no unexplained refusal:

```sh
systemctl --user start smarty-nats-link@v2.service
"$FABRIC/bin/nats-link" status --config "$CONFIG" | jq '{running, natsMirror, fileMirror}'
```

`natsMirror` and `fileMirror` should equal the dry run's live `file` and `nats` row counts.

## Rollback

The link changes only three things: KV presence and lease records under its own machine
`ryzen1-bridge`, mirror records with `remoteHost: <link>` in Ryzen 1's file mesh, and its own state
directory. Nothing else on either side is written, so the rollback is:

1. **Stop the link.**

   ```sh
   systemctl --user stop smarty-nats-link@v2.service
   ```

   The link fences mirror writes and closes its ledger. From this moment:
   - NATS stops routing to Ryzen 1 identities within 15 s: a NATS route needs a live presence and lease
     pair, and the lease TTL is 15 s (`KV_LEASE_TTL_MS`). Presence records alone stay up to 600 s but do
     not route.
   - Ryzen 1 stops routing to v2 identities within 15 s: a mirror authorizes traffic only while the
     bridge's observation is fresh (`MIRROR_FRESH_MS`). A sender gets `FABRIC_PEER_LEASE_STALE`, retryable,
     and nothing is published.
2. **Remove the mirrors now instead of waiting for their TTL** (optional, same end state):

   ```sh
   "$FABRIC/bin/nats-link" withdraw --config "$CONFIG"
   ```

   It deletes only the bridge machine's own KV presence and lease records (the grant allows nothing else)
   and only the file-mesh mirror records whose `remoteHost` is this link.
3. **Verify.** Repeat step 1 of the dry run: `0` mirrors in the file mesh, `running: false`,
   `natsMirror: 0`, `fileMirror: 0`. The ledger directory remains (step 4).
4. **Keep `"$STATE/ledger"`.** Never delete it as part of a rollback. It holds the exactly-once claims of
   commands already carried; a later start reuses it so no command crosses twice. It is private (`0700`
   directory, `0600` file) and expires its own records after 24 h.
5. **Restore the previous routes.** Before A', a moved lead was not reachable from Ryzen 1 at all; after
   the rollback that is the state again. Nothing else needs restoring: no Ryzen 1 lead, seat, supervisor
   or factory config was changed by the link.

### How the rollback is tested

- **Unit (green now):** `tests/bridge-nats-ledger.test.ts` shows the ledger survives a stop and a crash
  with every resolved claim, refuses a second writer, and recovers a dead holder's lock;
  `tests/bridge-nats-identity.test.ts` "bridgeNatsDryRun" shows the dry-run report is computed from
  presence reads alone.
- **Real broker (L4, must be green on the PR head before the pass starts):** start the link between a
  temporary file mesh and a local `nats-server` (throwaway user, `createPathBHarness`), carry one
  followUp each way, then run the rollback steps above and assert: no live route either way within 15 s
  of stop; `withdraw` leaves zero KV records under the bridge machine and zero `remoteHost: <link>`
  records in the file mesh; nothing outside those was written (KV keys of other machines and native file
  records are byte-identical before and after); a restart reusing the ledger delivers nothing twice.

## Dependencies this page names

- **L5** (`nats-link` CLI): `dry-run --config` (prints the report, exits `0|1|2|3`, opens nothing for
  writing: no state directory creation, no consumer), `status --config` with `running`, `natsMirror`,
  `fileMirror`, and `withdraw --config`. The systemd unit name `smarty-nats-link@<link>.service` and the
  config path `~/.config/smarty/nats-link/<link>.json` are this page's proposal; ops-lead owns the unit.
- **L4**: the real-broker rollback test above.
- **Credentials**: the `ryzen1-bridge` machine credential (Paul's access item) and Ryzen 1 to broker
  reachability. Without them, run the same steps against the test-org broker.
