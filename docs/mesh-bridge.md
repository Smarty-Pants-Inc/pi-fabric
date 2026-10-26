# Mesh bridge (v1)

`bin/mesh-bridge` links the Fabric mesh of this host (the hub, Dev1) to the mesh of one remote host
over an ssh stdio transport (smarty-dev#2004). Each host keeps its own mesh; the bridge republishes
existing events and mirrors root presence. The additive publish-receipt hello capability reports destination append timestamps; older peers keep the existing publication RPC.

## What crosses

- `fabric.control.command` and `fabric.control.ack`, well-formed `fleet.work.<name>` topics,
  `ops.owner` with kind `pr.wake`, the session-addressed delivery-age and legacy-baseline
  alarm/recovery events, and the versioned `fabric.alarm.bridge-stall` alarm when addressed to a
  canonical live session on the other side. A legacy peer without an acknowledged complete
  presence baseline is paged after more than two minutes using monotonic elapsed time; a transient
  publish failure is retried after one second, alarm/recovery publishes are bounded to 15 seconds,
  and an acknowledged complete baseline emits recovery. Partial-presence-capable
  peers are not paged; without a configured mesh-alarm recipient the structured mesh-outage journal
  records the episode. A well-formed work topic is at most 128 characters of dot-separated,
  non-empty ASCII segments of `[A-Za-z0-9_-]` (a dot-led repo such as `.github` is the one
  exception), so every work topic that crosses also passes the mesh's and `mesh_answers`' topic
  pattern. `fleet.work.`, `fleet.work..1`, `fleet.work.a..b.1`, a trailing dot, NUL, space,
  lookalike or other non-ASCII character does not cross to anyone.
- For bridged `fabric.control.command` and `fabric.control.ack` events, each direction samples
  origin `createdAt` through the destination's durable append timestamp, plus the destination RPC
  receipt latency. It logs one event-driven line per nonempty minute:
  `bridge delivery-age v1 peer=<peer> minute=<RFC3339-UTC> toRemote count=<n> deliveryP50Ms=<ms|none> deliveryMaxMs=<ms|none> ackP50Ms=<ms|none> ackMaxMs=<ms|none> cursorLagSequences=<n> cursorLagSeconds=<s> toLocal ...`.
  Cursor lag is sampled from the source head minus that direction's persisted cursor, with age in
  seconds from the oldest sampled event. A delivery age over 30 seconds, an RPC receipt latency
  over 30 seconds, or a command appended after its `deadlineAt` emits one `ops.owner` alarm per
  directional episode and one recovery event after timely traffic resumes. This signal does not
  set bridge stall state, withhold `WATCHDOG=1`, close the link, or exit. Peers that advertise the
  additive `publishReceipts` hello capability provide exact append timestamps; older peers retain
  their original publish response shape and the bridge uses receipt time as the append-time fallback.
- From the remote to the hub only: a work topic of the exact form `fleet.work.<repo>.<n>` (a
  non-empty repo of `[A-Za-z0-9._-]`, not `.` or `..`, and a decimal number with no sign or leading
  zero) and the allow-listed `ops.owner` kinds, addressed to the hub factory's role, the exact
  string `to: "factory"` (smarty-dev#9352). The factory on the hub reads these by topic; the event
  lands on the hub's mesh on the same topic with `to` unchanged. `fleet.work.`, `fleet.work.<repo>`,
  `fleet.work.inbox-receipts`, `factory-lead`, `factory` with a NUL or space, and a non-string `to`
  do not cross. No other recipient name, no other topic, and nothing from the hub to the remote is
  widened. `mesh.publish` accepts this literal role address for those topics on any host even
  though it is not a participant: the hub factory reads it locally, and a spoke's bridge carries
  it to the hub. Other unknown targets still fail before publication.
- From the remote to the hub only: unaddressed `wait` and `eta` records on a parsed
  `fleet.work.<repo>.<n>` topic also reach the factory (smarty-dev#9349). The hub copy gains
  `to: "factory"` to keep the existing bridge wire format. Public `mesh.publish` argument preparation
  keeps its existing optional-null behavior: `to: null` means omission, just like leaving out `to`.
  Non-null malformed recipients still fail public schema validation. The raw bridge publication
  wire has no argument repair: `to: null` or another non-string recipient is invalid and is not
  converted into the unaddressed route. Unaddressed `progress`, acknowledgments and other chatter
  stay local; an explicit unrelated recipient does not gain this exception.
- These factory events are rate bound per link, in events and in bytes: a token bucket
  of 30 events a minute, burst 60 (`--factory-rate PER_MINUTE`, `--factory-burst N`), and one of
  512 KiB a minute, burst 1 MiB, of the events as published (`--factory-bytes BYTES_PER_MINUTE`,
  `--factory-byte-burst BYTES`). An event bigger than the byte burst waits for a full bucket and
  leaves a debt, so it is never stuck. Events to a hub-native recipient spend from neither. Over the bound nothing is dropped: the inbound drain stops at that event with the
  cursor before it (the link's later inbound events wait behind it, in order), no pass rereads the
  remote until the next token is due, and one timer then retries it. A line at most once a minute
  reports it: `toLocal: factory rate bound (30/min, burst 60; 524288 bytes/min, burst 1048576): N
  deferrals; waiting at <topic> <id>, retry in N ms; nothing dropped`.
- Presence: each side's live native hosts and root participants go into the other side's state at
  the same keys, with `remoteHost: <side name>` and the source identity as `updatedBy`. A mirrored
  lease carries the native renewal and expiry plus the source clock at which they were read
  (`mirror.sourceUpdatedAt`, `sourceExpiresAt`, `sourceObservedAt`). Its local `observedAt` is
  captured before the mirror's dispatch/write wait, not when a delayed write committed. Source lease
  age is judged on the source clock; bridge freshness lasts 15 s on the receiving clock. A
  root is lease-stale only when that host's bridge observation is fresh and its sampled native
  lease is old: routing refuses it immediately, naming that remote host, without waiting or
  publishing. A stale bridge, missing required observation, or expired translated authority
  instead leaves the source lease unconfirmed. Before publication, routing waits once, at most
  15 s, for a fresh same-owner renewal; without one it returns retryable `FABRIC_PEER_LEASE_STALE`,
  naming the host, and publishes nothing. After publication, `FABRIC_BRIDGE_STALE` retains the
  unknown-delivery warning; it never claims that the remote Main's lease lapsed. New projections advertise `mirrorLease: 1` and
  require matching identity/incarnation metadata; missing or malformed metadata cannot fall
  back to an old state expiry. Older projections without the `mirrorLease` marker retain their
  bounded compatibility expiry: an unexpired projection still routes, while an expired one takes
  the same single renewal wait. Remove this exception once every bridge runs a release that writes
  `mirrorLease`.
  Presence is refreshed on every bridge (re)connect before startup completes, then on store notifications
  and actual lease deadlines, not a periodic poll. Each pass reads the current native records, including file-only roots in
  `participants/`, and does not replay a saved projection set. A lapsed mirror is refreshed
  and revalidated once before an event is refused. A source that stops renewing can remain mirrored
  for at most twice its TTL from its last renewal (at most one extra capped TTL after the last live
  observation). Without a fresh bridge sample, roots remain discoverable with `bridgeStale: true`
  for a precise routing error, but do not authorize bridged traffic. Traffic retains its stricter
  translated TTL and reobserves short-lease origins before forwarding. Clean stop or transport
  failure leaves persisted projections in place to expire by their lease TTL. A later fresh snapshot
  reconciles origins that no longer report a live native lease.
- Nothing else crosses beyond the routes above. This excludes `github.*`, actor output, state and
  cache keys, and agent and actor participants.

## Rules

- Each bridged event keeps its `kind` and `text`, and its `to` except for the unaddressed factory
  records above. An event from the hub keeps its `from`. An
  event from the remote carries, as `from`, its sender's identity (name, kind, session id) from
  the presence record this link admitted it by, never the one its body claims; under the commit
  lock the hub also refuses one whose identity is not that sender's mirrored record (pi-fabric#876). It carries
  `data.bridge = {from: <side>, id: <original event id>}`, and an event with a `bridge` field is
  never forwarded again.
- Per direction the cursor records the last handled source sequence plus a generation-tagged
  byte offset. Change notifications trigger tails from that offset without depending on shared
  sequence-read hints. An empty notification-capable bridge has no recurring timer, event read
  or checkpoint write.
  Offsets checkpoint only a fully handled page; each forwarded event still checkpoints
  its source sequence and destination mark. On restart the bridge skips ids already bridged after
  that mark, so a crash does not forward an event twice. Old sequence-only cursor files migrate
  on first use; explicit manual `step()` calls can still read old v1 agents without tail support.
  Continuous `run()` negotiates change notifications. A new-to-new pair does not poll; when a
  remote agent explicitly reports pre-capability protocol v1, only that peer's legacy path retains
  the existing 250 ms poll and logs the compatibility mode once. Protocol v2 requires `changes:true`:
  a v2 peer that omits or denies it is refused, never silently downgraded. New clients request v2 in
  `hello`; unversioned old clients still receive a v1-compatible reply from new agents, without
  unsolicited frames unless requested. The reported version is pinned for that transport. This is a
  transitional host-by-host rollout path, to remove after the fleet-wide install under
  https://github.com/Smarty-Pants-Inc/smarty-dev/issues/7886.
  A rewritten log invalidates the offset generation and reconciles by sequence (from the archive
  when available) before returning to byte tails.
- The mirrored-lease alarm still reuses the existing `host-leases/<host-hash>.json` observation
  (`mirror.observedAt`). With mirrored roots, existing source/lease deadlines can renew that
  observation without event traffic. The owner's committed presence-maintenance pass checks
  for more than three missed 15 s freshness intervals while the locally recorded bridge process
  is still the same live incarnation on this machine and boot. It publishes one `ops.owner`
  `bridge.lease.alarm` naming the physical link and lease-lapse age; a fresh lease clears/re-arms
  that episode and emits `bridge.lease.recovered`. Both use the existing mesh-health session
  recipient (`PI_FABRIC_MESH_ALARM_TO`, otherwise `SMARTY_LEAD_SESSION`); without one the
  existing structured mesh-outage journal sink carries the alarm. This owner sweep never
  refreshes a lease, probes a link or restarts a unit. A link with no mirrored roots has no
  lease to lose and keeps the empty-idle contract. Historical leases without local
  bridge-process evidence are not monitored until that link completes a new mirror pass.
  **This lease alarm does not prove the event path is healthy:** a one-way event-stream stall
  can leave presence/mirror RPCs working, renewing `mirror.observedAt` and keeping the lease
  alarm quiet. If the whole peer stops answering presence/mirror calls, the observation stops
  renewing and the existing lease alarm can fire after its freshness deadline.
- The bridge process probes the peer every 15 s (remote probe deadline 5 s). Agents advertising
  `responsivePing: true` answer from memory, independently of their store worker; this liveness
  reply permits `/usr/bin/systemd-notify WATCHDOG=1` but never counts as a delivery receipt or a
  current remote head. Known pending work, including a busy tail, retains its directional progress
  deadline. Older peers retain both-head probes, which permit watchdog notifications when at least
  one direction is current or has recent cursor progress. If the peer probe stops answering for
  30 s, or a legacy both-head probe finds neither direction current/progressing for that long,
  the bridge emits one `fabric.alarm.bridge-stall` event and stops notifying systemd. A direction
  with no acknowledged
  (`toRemote`) or received/handled (`toLocal`) cursor progress for 180 s while the other direction
  remains current/progressing emits the same alarm once and exits with status 1. The event is
  addressed only to the configured mesh-alarm session recipient.
- Nothing notifies systemd before the link is up. A first pass refused as busy (another link's
  pass holds a shared custody gate) backs off and starts again. It logs the first refusal, each
  change of refusal type, and once a minute. A start still refused 60 s after the bridge began
  linking (two thirds of `WATCHDOG_USEC` when systemd sets a shorter watchdog) exits with
  status 1. Its one `mesh-bridge: stopped: Bridge start still refused after ...` line on stderr
  names the last refusal, before the watchdog would kill the bridge without a reason.

  The versioned alarm contract is `topic: "fabric.alarm.bridge-stall"`, `kind: "bridge-stall"`,
  with typed `data` exactly `{ v: 1, host: string, peer: string,
  direction: "toRemote" | "toLocal" | "both", lastOkToRemote: string | null,
  lastOkToLocal: string | null, stallId: string, reason: string, at: string }`. The last-ok
  `host` is the local `--name`; `peer` is the configured remote name. The last-ok values and
  `at` are RFC3339 UTC; last-ok is `null` before that direction has a successful observation.
  `stallId` is a decimal `process.hrtime.bigint()` monotonic-clock value, guarded
  strictly increasing within the running process (monotonic across service restarts on the same
  host boot). `direction` names the stalled direction, or `both` when a complete probe/path has
  stalled.

  Before notifications stop or a directional exit, the bridge writes one reason line to stderr
  and atomically overwrites the per-peer reason file. Its absolute path is the absolute cursor
  path plus `.last-stall-reason`: for a cursor at `/absolute/state/<peer>.cursor.json`, use
  `/absolute/state/<peer>.cursor.json.last-stall-reason`. The single-line format
  is `mesh-bridge: stall v1 host=<host> peer=<peer> direction=<toRemote|toLocal|both>
  at=<RFC3339-UTC> lastOkToRemote=<RFC3339-UTC|never>
  lastOkToLocal=<RFC3339-UTC|never> stallId=<monotonic-decimal> reason=<one-line-reason>
  restartCount=unknown`. Each overwrite writes the complete line to a unique same-directory
  temporary file, fsyncs it, atomically replaces the reason file and syncs the containing directory
  where supported; recovery/start replaces the old reason with an empty file. The bridge cannot know systemd's
  restart count; `ExecStopPost` can read `$SERVICE_RESULT` (`watchdog` or `exit-code`) and this file to page. The operator must keep the
  service type unchanged and use this drop-in:

  ```ini
  [Service]
  NotifyAccess=all
  WatchdogSec=90
  TimeoutStopSec=10
  ```

  `NotifyAccess=all` is required because the notifier is a child process. A requested SIGTERM or
  SIGINT stops sync and forwarding, fences local mirror writes, ends remote stdin cleanly, and exits
  with status 0 within five seconds. If the SSH transport has not exited two seconds after stdin
  closes, the bridge terminates it; a second signal forces an immediate status-0 exit. Keep the
  service type unchanged. After installing the drop-in and the operator-owned `ExecStopPost` hook,
  run `systemctl daemon-reload`, restart that peer's bridge unit, and verify effective
  `NotifyAccess`, `WatchdogUSec`, and `TimeoutStopUSec` with `systemctl show`.
  The service cgroup must contain both the bridge and its SSH transport. The hook pages based on
  `$SERVICE_RESULT` (`watchdog` or `exit-code`) and reads the reason file. If no alarm session is
  configured, the bridge cannot safely address a mesh event; the stderr/reason-file signal remains
  available to the hook. Directional-stall exits rely on the installed systemd `Restart=` policy;
  unit restart configuration belongs to ops-lead under smarty-dev#13620, and the two-exits-in-30-minutes
  alarm belongs in the alarm router, not this bridge.
- The remote is not trusted. An event from it crosses only when all of these are true:
  - its sender is a live native participant or host identity of the remote;
  - no hub record, live or not, uses that id;
  - its recipient is native to the hub, or it is a `fleet.work.*` or allow-listed `ops.owner`
    event addressed exactly to `factory`, or an unaddressed `wait`/`eta` issue record on a parsed
    `fleet.work.<repo>.<n>` topic. The session-addressed `fabric.alarm.bridge-stall` event can
    also cross to its native hub recipient; the sender checks, commit-time ownership fence and
    frame budget still apply.
- A remote identity is bound to the one link that mirrored it first (the `remoteHost` on its record).
  Each link reserves every id it did not mirror: native records (live or not) and other links'
  mirrors. It reserves host ids, identity ids, root ids, participant ids and session ids, but not
  labels, which each mesh mints and which collide by design. It also reserves every native
  participant name. A remote root must have a canonical id (`session:<session id>`). A native
  Main never accepts a `session:` name other than its own id (RootInbox), so native name delivery
  and bridged ids are disjoint by construction.
- The one exception is a hub handoff (smarty-dev#9074, below): a Main moved off the hub keeps its
  session id, and its own dead native records would reserve that id forever. A handoff in force
  waives that identity's own ids (id, session id, name) on exactly the link it names, and reserves
  them on every other link. Every other reservation is unchanged.
- Authority comes from ownership after the mirror step, not from the peer's claims: for each page it
  forwards, the bridge rereads the records this link holds now and checks them against the current
  reservations. A claim refused or lost in the same pass (to another link or to a new native) gets no
  traffic, and its events are dropped. Each event is checked again at commit. An inbound event is
  published only if, under the mesh lock that commits it, its sender (and an ack's target) is
  still a live mirror of this link. An outbound event is sent only while this link still holds its
  recipient. While a backlog is forwarded, presence and leases are renewed when due. A remote event whose sender, or an ack whose `targetId`, is
  not bound to this link is dropped. Before refusing a lapsed mirror (including expiry while
  waiting for publication), one bounded, single-flight presence pass refreshes and revalidates
  authority. During notification-driven `run()`, traffic with live committed ownership does not
  join an already-running presence pass merely because a preemptive refresh is due. This also
  applies after the mirror deadline while an open transport retains that mirror's response slot
  and has negotiated `independentRpc`: the failed pass may have settled, but live committed
  traffic still uses the independent lane. Page, batch and relay checks still reread canonical
  ownership; a missing/new or expired member and a commit-time ownership refusal still take the
  existing blocking refresh and revalidation path, even while that slot remains pending. If
  that explicit refresh joins a pass already in flight, it settles that pass and requests or
  joins one new snapshot before revalidating: an older sample cannot establish
  fresh absence for a newly observed event. The same bound applies to retained-membership
  retries: at most the old pass plus one new pass, with no polling or deadline increase.
  Manual `step()`, startup and shutdown keep their blocking presence behavior. This is
  scheduling isolation, not permission to forward from an uncommitted presence claim.
  It never substitutes peer claims for ownership or changes a captured destination.
- The hub routes to the remote only by canonical ids: host id, identity id, participant id and root
  id. Labels, session ids and names are never addresses across.
- A mirror never replaces a native record or another bridge's mirror. It writes by
  compare-and-swap only. Each reconciliation compares property-order-independent content
  digests against the destination's current records under its mesh lock: unchanged participant
  records do not rewrite shared state. Expired/pruned projections can still be restored even when
  the source set is unchanged. Host file leases renew separately; compatibility state lease
  checkpoints remain bounded to their existing cadence.
- Each remote call has a deadline (`--call-timeout-ms`, default 30 s). A missed mirror answer
  degrades that link and keeps the bridge open. One mirror response slot stays in flight;
  the bridge queues no additional mirror or presence RPCs behind it and retries with 1 s to
  30 s exponential backoff. New agents advertise the additive hello capability
  `independentRpc: true` (with either hello mode/version). Once negotiated, traffic/read RPCs
  may proceed while a mirror remains pending, including after its deadline. A peer without
  that capability retains the old global pending-mirror gate for every RPC. Hello/subscription
  requests are also refused while that slot is occupied, because they share the topology lane.
  A late answer opens the gate for a new presence snapshot; it does not renew the old snapshot's
  freshness. The agent stamps mirror arrival time before either lane's dispatch wait.
  Non-mirror timeouts, transport/watch failures and permanent late mirror rejections remain
  fatal. On stop, the bridge fences future local mirror writes and ends remote stdin cleanly.
  Persisted mirrors expire by lease TTL. The hub reaps a transport
  child after clean EOF grace (SIGTERM after 2 s, then SIGKILL 500 ms later if needed).
- An allow-listed event that is not addressed across (its recipient is not on the other side) is
  not forwarded. Each one is a debug line (`--log-level debug`), and the bridge logs at most one
  count line a minute, `bridge: N events not addressed across (topics: ...; toRemote N, toLocal N (to
  factory N))`, where `to factory` counts remote events addressed to `factory` that did not cross.
  The line is event-driven: the next such drop or forward after the minute emits it; no timer runs.
- A read page stays under 8 MiB of events, so it always fits one 16 MiB frame. An event that alone
  is over the budget is skipped and logged with its id and sequence, and the cursor moves on.
  Presence snapshots (both the replies and the mirror requests) hold whole hosts with their roots
  up to the same budget. Hosts past the budget are not mirrored, so they lapse, and the bridge logs
  the count. A `bridgedIds` reply holds at most 50,000 ids per page.
- Local mirror writes run one at a time behind a fence. On explicit withdrawal, the bridge fences
  first, waits for a write in flight, then deletes, so no record or lease comes back after withdrawal.
- The bridge checks its arguments and opens the local mesh before it starts the transport. From the
  spawn on, one `finally` fences local mirror writes and reaps the child; it leaves persisted mirrors
  to expire by lease TTL. A transport that cannot start (a missing or non-executable ssh) fails the
  bridge with a named error and exit status 1.
- The agent (the remote end) serves only the bridge operations. It applies the allow-list and
  stamps its pinned `--peer` name on everything it writes, whatever the hub sends.
- The hub and agent watch the mesh directory and bind watches for `participants/` and
  `host-leases/`, including their creation and atomic replacement. SQLite WAL updates are
  notifications too. Watchers are installed before snapshots, and changes during a pass are
  latched for the next drain. Notifications are hints: canonical reads and ownership fences
  still decide delivery. A watch failure on a notification-capable link stops it, so it never
  silently drops events. Remote change frames are negotiated with `hello` and do not occupy
  either agent request lane; there is no hanging wait request in front of a publish.
- The agent has two independent serial RPC lanes, extending the independent-direction
  scheduling in smarty-dev#9751: hello/subscription, presence and mirror share the topology
  lane; all reads, recovery queries and publications share the traffic lane. Hello installs
  watches before any later topology snapshot. A held topology service cannot by itself hold
  traffic behind it; publications still retain FIFO order, including for the same target.
  Both lanes retain the same dispatch validation, mesh locks and ownership fences, so actual
  store contention can still delay traffic. In `mesh-bridge agent`, one worker owns the store,
  both lanes, directory watches and topology scans. The stdio thread never opens the store: it
  answers `ping` from memory with a `busy` flag, and serves a bounded, exact-cursor tail mailbox.
  A hub opts into busy-tail replies with `busyTail: true` in hello; a successful negotiation
  advertises the same capability. Without that opt-in, tails return ordinary completed pages
  from the worker, never an unexpected busy page. For opted-in hubs, a tail admitted while
  worker work is pending replies immediately with `busy: true` and unchanged sequence/offset.
  An idle admission gives the worker 50 ms to return its page in that same RPC; if still pending,
  it returns busy and retains the read. A completed retained read wakes the drain when ready.
  Busy pages are not caught-up or delivery evidence; the hub uses notifications and a bounded
  safety retry without advancing its durable cursor. Publication receipts still wait for durable writes, and their existing fatal
  30 s default deadline is unchanged. Mirror arrival time is captured before the worker queue.
  Input shutdown disposes watches and skips queued work; after a two-second cleanup grace the
  stdio owner requests worker termination. A native SQLite call can delay actual termination,
  promise settlement and process exit until that call returns; the grace is not a hard shutdown
  bound. Debug RPC timing is opt-in, contains no event bodies, and reports `queueDepth` as the
  number of earlier requests in that request's worker lane at
  admission, including its in-service request, not work in the independent lane.

## Control deadlines and retention

Commands addressed to a validated remote participant use `mesh.bridgeControlTimeoutMs` (default
30,000 ms, configurable from 30,000 to 300,000), or a longer requested timeout. A pre-publication
freshness wait is separate: one notification-driven renewal wait of at most 15 s, pinned to the
selected host, identity, root and bridge link. A renewal revalidates the route; a different owner
or link cannot redirect the attempt. A fresh source-expired sample skips that wait and refuses
immediately. No renewal produces `FABRIC_PEER_LEASE_STALE` (`retryable: true`), with no publication.
The control deadline is stamped at publication commit and includes outbound bridge queueing. Senders allow an additional
15 s for the ACK return leg; mirrored steer/followUp admission waits are bounded by that same
window plus grace, even if publication blocks or the lease renews. Cancellation uses the bridge
window too. Native commands keep their existing timeout. A missing ACK or a lapsed mirror cannot
prove non-delivery: errors say the outcome is unknown and a retry may deliver twice. This also
covers a publication already handed to the store whose result is still pending: it may commit
after the local request settles, so the sender retains its captured destination and issues the
existing best-effort cancellation, never a lookup replay. Only an authenticated owner's explicit
`notRun` rejection permits the existing bounded message retry.

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

    command="node /path/to/pi-fabric/bin/mesh-bridge agent --mesh /path/to/project/.pi/fabric/mesh --peer hub-a",restrict ssh-ed25519 AAAA... mesh-bridge@hub-a

On the hub, run one bridge per remote host as the fleet user, under the service manager:

    mesh-bridge run --mesh ~/proj/.pi/fabric/mesh --name hub-a --remote peer-b \
      --cursor ~/.local/state/mesh-bridge/peer-b.cursor.json --ssh peer-b --ssh-key ~/.ssh/mesh-bridge

Both modes accept `--max-state-bytes BYTES` for meshes larger than the default 32 MiB
shared-state read ceiling. Set it explicitly on each side that needs the larger capacity;
it must be a safe integer of at least 524,288 bytes. This changes only the state read ceiling,
not event/frame limits or the default. For example, `--max-state-bytes 67108864` admits a
50 MiB `state.json` while retaining a 64 MiB barrier.

`--ssh-port` and `--ssh-known-hosts FILE` (which also sets `StrictHostKeyChecking=yes`) reach a
host without a global `~/.ssh/config` entry.

### Expected outbound roots (no injection)

Add the independent, default-off `--emit-expected-roots` flag to **run mode only** to publish
an expected-root receipt without injecting any malformed presence. Enable this flag alone on
**all six hubs**; reserve the partial-probe flag below for **epyc1 only**:

```sh
mesh-bridge run --mesh ~/proj/.pi/fabric/mesh --name hub-a --remote peer-b \
  --cursor ~/.local/state/mesh-bridge/peer-b.cursor.json --emit-expected-roots \
  --ssh peer-b --ssh-key ~/.ssh/mesh-bridge
```

The receipt is `<dirname(cursorPath)>/<remoteName>/probe-roots.json`, normally
`~/.local/state/mesh-bridge/<peer>/probe-roots.json`. Its top-level fields are:

- `version: 1` and `peer`: the configured remote name.
- `roots`: full, sorted canonical root IDs of the **actual ACKed outbound mirror projection**,
  after admission and transport adaptation, not a raw source scan. Legacy partial completion
  can include baseline-held roots omitted from the source cycle.
- `count`: the length of `roots`.
- `digest`: the hexadecimal SHA-256 digest of `JSON.stringify(roots)` (the sorted array).
- `cycleTime`: the source cycle's start time as an ISO UTC timestamp.
- `partial`: whether the source cycle was partial (`true`) or complete (`false`), even when a
  legacy receiver was sent a completed projection without a partial marker.
- `cycles`: the existing probe history, `[{cycle, roots, digest}]`; empty for emitter-only runs.
  Later emitter updates in the same process retain every successfully recorded probe cycle.

Emitter-only writes are attempted at most once per **30 seconds per peer**, including failures,
when a new mirror ACK is available. This adds no timer, injection, or mirror throttling. A legacy
partial skipped before its first complete baseline has no ACK and produces no receipt. Each
write atomically replaces the receipt using a same-directory temporary file plus rename; files
are mode **0600**, with the per-peer directory created mode **0700**. Receipt writes are bounded
to 8 MiB. Write failures log at most once per minute per peer and **never fail presence mirroring
or the bridge loop**; a previous receipt may therefore be stale, so check `cycleTime`.
With neither flag, the receipt path is not written and no receipt sorting, hashing or filesystem
work is done. Remove the emitter flag and restart to stop emission; existing files remain.

### Bounded partial probe (epyc1 only)

For a bounded hub-side diagnostic of legacy partial-presence completion on **epyc1 only**, add
`--partial-probe-after-baseline` to `mesh-bridge run` (default 30 cycles), or use
`--partial-probe-after-baseline=N` for an integer from 1 through the hard cap of 50.
The probe is off by default and only activates for a peer that does not support partial
presence, after that same peer ACKs a complete baseline. Each active sync adds one malformed
participant with a non-string id to a fresh in-memory local snapshot immediately before admission;
the normal classifier drops it and naturally marks the scan partial. The malformed row is never
persisted or forwarded, and the existing legacy completion and ownership/lease fences still apply.
After each successful mirror ACK, the bridge atomically retains all completed cycles' full sorted
outbound root IDs and SHA-256 digests in `<dirname(cursorPath)>/<remoteName>/probe-roots.json`
(up to 8 MiB), then logs `presence PROBE:` with the cycle, drop count, legacy completion count,
root count, digest and absolute `rootsFile` path. The receipt also has the top-level expected-root
fields documented above. Probe cycles write after **every ACK**, bypassing the emitter's 30-second
limit, even when `--emit-expected-roots` is absent. With both flags, each probe cycle makes only
one write containing both the complete cycle history and current expected-root metadata. A failed
probe receipt never claims that cycle: it safely disables further probe injection/recording for
the process lifetime, emits a bounded warning, and leaves mirroring and the bridge loop running.
The independent emitter, if enabled, can continue after its attempt limit permits. After N
successfully recorded ACK-backed cycles the probe logs a disable notice and stops injecting for
the process lifetime. Roll back by removing the probe flag and restarting the bridge; retained
receipts remain available for inspection. Agent mode is unchanged. The emitter and probe are
independent: enabling the emitter alone never activates the epyc1-only injection.

When a bridge stops providing fresh observations, its authority expires within 15 s. A retained
root is not proof that the Main ended. Before publication, routing waits once for renewal and
then returns `FABRIC_PEER_LEASE_STALE` if freshness remains unknown; the attempt was not published.
After publication, `FABRIC_BRIDGE_STALE` preserves the unknown-outcome warning because a delayed
command or ACK may still arrive; it must not trigger a lookup retry or automatic replay. Only a
fresh source observation of an old native lease can report a lease lapse immediately. The command's
published admission/ACK budget is unchanged.
The dead-root activation filter follows the same distinction: a stale bridge or a live source
sample is doubt/alive, not owner death. Only a fresh observation of a source-expired root,
past its existing ten-minute grace and without live native participant evidence, permits a
skip. Required but missing/malformed mirror metadata fails open.

## Moving a Main off the hub (handoff)

A Main that moves from the hub to a remote host keeps its session id. Its native records on the
hub (host record, root participant, left-over agents) reserve that id, live or not, so the hub's
bridge refuses the moved Main ("sender claims a hub identity") until the host reaper removes them
hours later. After the hub's Pi for that session has stopped, run on the hub, with the hub
bridge's mesh root, `--lock-protocol` and environment:

    mesh-bridge handoff --mesh ~/proj/.pi/fabric/mesh --session <session id> --link <remote name> [--operator NAME]

It refuses, writing nothing, while the hub still holds the identity: a live host lease, Main
session lease or reload lease; a recorded Main or lease-writer process that is not proven gone from
this host; or another link holding a live mirror of it. Otherwise it removes the dead native's host
record and root participant (record and file), and in the same state commit writes the handoff
record `topology/handoffs/<sha256(id)>` with the link, time, operator and hub host. Output is one
JSON object (`ok`, `message`, the record, what it retired and what it observed); exit 0 handed off,
1 refused, 2 usage. A usage error (a missing or malformed flag, a path that is not a mesh root, a
bad session id) writes nothing and is one JSON object too: `ok: false`, `message: "usage: ..."`
and the command's synopsis in `usage`. `handoff-list` shows every handoff and whether it is in
force; `handoff-revoke` removes one.

The handoff is the only authority: local write access to the hub's mesh. The bridge writes only
mirror records and bridged events there, and agents cannot write `topology/` keys through the
mesh provider, so no remote party can write one. A native that answers to the id again (any
native host or participant record, live or not, or a live native lease) voids the handoff at once,
and the next mirror pass of any hub bridge deletes it; a later move needs a new handoff.

The bridge exits with status 1 when the transport closes. The service manager restarts it, and it
resumes from the cursor file. `-- COMMAND...` replaces ssh with any stdio transport (tests use a
local agent process).
