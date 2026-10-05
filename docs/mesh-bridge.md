# Mesh bridge (v1)

`bin/mesh-bridge` links the Fabric mesh of this host (the hub, Dev1) to the mesh of one remote host
over an ssh stdio transport (smarty-dev#2004). Each host keeps its own mesh; the bridge republishes
existing events and mirrors root presence. There is no new store or protocol.

## What crosses

- `fabric.control.command` and `fabric.control.ack`, `fleet.work.*`, and `ops.owner` with kind
  `pr.wake`, when the recipient (`to`) is a live root or host native to the other side.
- Presence: each side's live native hosts and root participants go into the other side's state at
  the same keys, with `remoteHost: <side name>` and the source identity as `updatedBy`. A mirrored
  lease lasts one source TTL from the local observation, capped at 15 s. It does not use
  the source's absolute expiry. File-only heartbeats carry their effective renewal time. Presence
  is refreshed on every bridge (re)connect before startup completes, then every 5 s even without
  event traffic. Each pass reads the current native records, including file-only roots in
  `participants/`, rather than replaying a saved projection set. A lapsed mirror is refreshed
  and revalidated once before an event is refused. A source that stops renewing can remain mirrored
  for at most twice its TTL from its last renewal (at most one extra capped TTL after the last live
  observation). When the bridge stops, its mirrors lapse within 15 s. On a clean stop or
  transport failure it deletes them at once.
- Nothing else. This excludes `github.*`, actor output, state and cache keys, and agent and actor
  participants.

## Rules

- Each bridged event keeps its `from`, `to`, `kind` and `text`. It carries
  `data.bridge = {from: <side>, id: <original event id>}`, and an event with a `bridge` field is
  never forwarded again.
- Per direction the cursor records the last handled source sequence plus a generation-tagged
  byte offset. Polls tail from that offset without depending on shared sequence-read hints.
  Idle polls read one boundary byte per side and do not rewrite checkpoints or read routing
  authority. Offsets checkpoint only a fully handled page; each forwarded event still checkpoints
  its source sequence and destination mark. On restart the bridge skips ids already bridged after
  that mark, so a crash does not forward an event twice. Old sequence-only cursor files migrate
  on first use; old v1 agents fall back to sequence reads when they do not advertise tail support.
  A rewritten log invalidates the offset generation and reconciles by sequence (from the archive
  when available) before returning to byte tails.
- The remote is not trusted. An event from it crosses only when all of these are true:
  - its sender is a live native participant or host identity of the remote;
  - no hub record, live or not, uses that id;
  - its recipient is native to the hub.
- A remote identity is bound to the one link that mirrored it first (the `remoteHost` on its record).
  Each link reserves every id it did not mirror: native records (live or not) and other links'
  mirrors. It reserves host ids, identity ids, root ids, participant ids and session ids, but not
  labels, which each mesh mints and which collide by design. It also reserves every native
  participant name. A remote root must have a canonical id (`session:<session id>`). A native
  Main never accepts a `session:` name other than its own id (RootInbox), so native name delivery
  and bridged ids are disjoint by construction.
- Authority comes from ownership after the mirror step, not from the peer's claims: for each page it
  forwards, the bridge rereads the records this link holds now and checks them against the current
  reservations. A claim refused or lost in the same pass (to another link or to a new native) gets no
  traffic, and its events are dropped. Each event is checked again at commit. An inbound event is
  published only if, under the mesh lock that commits it, its sender (and an ack's target) is
  still a live mirror of this link. An outbound event is sent only while this link still holds its
  recipient. While a backlog is forwarded, presence and leases are renewed when due. A remote event whose sender, or an ack whose `targetId`, is
  not bound to this link is dropped. Before refusing a lapsed mirror (including expiry while
  waiting for publication), one bounded, single-flight presence pass refreshes and revalidates
  authority. It never substitutes peer claims for ownership or changes a captured destination.
- The hub routes to the remote only by canonical ids: host id, identity id, participant id and root
  id. Labels, session ids and names are never addresses across.
- A mirror never replaces a native record or another bridge's mirror. It writes by
  compare-and-swap only. Each reconciliation compares property-order-independent content
  digests against the destination's current records under its mesh lock: unchanged participant
  records do not rewrite shared state, but expired/pruned projections can be restored even when
  the source set is unchanged. Host file leases renew separately; compatibility state lease
  checkpoints remain bounded to their existing cadence.
- Each remote call has a deadline (`--call-timeout-ms`, default 30 s). A remote that misses it
  closes the transport. On stop, the bridge fences the loop, gives the remote withdrawal a bounded
  wait, withdraws the local mirrors regardless, and then reaps the transport child
  (SIGTERM, then SIGKILL after 2 s).
- A read page stays under 8 MiB of events, so it always fits one 16 MiB frame. An event that alone
  is over the budget is skipped and logged with its id and sequence, and the cursor moves on.
  Presence snapshots (both the replies and the mirror requests) hold whole hosts with their roots
  up to the same budget. Hosts past the budget are not mirrored, so they lapse, and the bridge logs
  the count. A `bridgedIds` reply holds at most 50,000 ids per page.
- Local mirror writes run one at a time behind a fence. On withdrawal, the bridge fences first,
  waits for a write in flight, then deletes, so no record or lease comes back after stop.
- The bridge checks its arguments and opens the local mesh before it starts the transport. From the
  spawn on, one `finally` withdraws the mirrors and reaps the child. A transport that cannot start
  (a missing or non-executable ssh) fails the bridge with a named error and exit status 1.
- The agent (the remote end) serves only the bridge operations. It applies the allow-list and
  stamps its pinned `--peer` name on everything it writes, whatever the hub sends.

## Control deadlines and retention

Commands addressed to a validated remote participant use `mesh.bridgeControlTimeoutMs` (default
30,000 ms, configurable from 30,000 to 300,000), or a longer requested timeout. The deadline is
stamped at publication commit and includes outbound bridge queueing. Senders allow an additional
15 s for the ACK return leg; mirrored steer/followUp admission waits are bounded by that same
window plus grace, even if publication blocks or the lease renews. Cancellation uses the bridge
window too. Native commands keep their existing timeout. A missing ACK or a lapsed mirror cannot
prove non-delivery: errors say the outcome is unknown and a retry may deliver twice. Only an
authenticated owner's explicit `notRun` rejection permits the existing bounded message retry.

The live `events.jsonl` already compacts after a publish takes it past 64 MiB, retaining up to
16 MiB of complete recent lines and incrementing `generation`. This is not an age-based sweep:
without a new publish an oversized legacy file remains unchanged. Stores with `event-archive.json`
also keep append-only per-topic/day archives; those have no total size or retention bound here.
Slow bridges can reconcile removed live events only when that archive is enabled. A proposed
future archive bound (not implemented) is a minimum acknowledged sequence across registered bridge
cursors, plus an outage/retention safety margin: delete only sealed segments entirely below that
floor, and never reset event sequences or log generations. Age/size deletion alone would break
lagging cursor recovery. A hard byte quota also needs backpressure: if pinned segments prevent
reclamation, reject new publishes and do not delete unread history. A permanently offline
link requires explicit retirement, not silent cursor invalidation.

## Running it

On the remote, pin the agent to the bridge's dedicated key in `~/.ssh/authorized_keys`:

    command="node /path/to/pi-fabric/bin/mesh-bridge agent --mesh /path/to/project/.pi/fabric/mesh --peer dev1",restrict ssh-ed25519 AAAA... mesh-bridge@dev1

On the hub, run one bridge per remote host as the fleet user, under the service manager:

    mesh-bridge run --mesh ~/proj/.pi/fabric/mesh --name dev1 --remote forge \
      --cursor ~/.local/state/mesh-bridge/forge.json --ssh forge --ssh-key ~/.ssh/mesh-bridge

Both modes accept `--max-state-bytes BYTES` for meshes larger than the default 32 MiB
shared-state read ceiling. Set it explicitly on each side that needs the larger capacity;
it must be a safe integer of at least 524,288 bytes. This changes only the state read ceiling,
not event/frame limits or the default. For example, `--max-state-bytes 67108864` admits a
50 MiB `state.json` while retaining a 64 MiB barrier.

`--ssh-port` and `--ssh-known-hosts FILE` (which also sets `StrictHostKeyChecking=yes`) reach a
host without a global `~/.ssh/config` entry.

When the bridge dies, the other side's copies stay live until their lease lapses, at most 15 s
(about 13 s on average). A steer or followUp sent in that window is not delivered; it fails when
its acknowledgement times out (the control plane's bounded wait), and nothing hangs. After the
lapse, senders get the lease-lapsed error at once.

The bridge exits with status 1 when the transport closes. The service manager restarts it, and it
resumes from the cursor file. `-- COMMAND...` replaces ssh with any stdio transport (tests use a
local agent process).
