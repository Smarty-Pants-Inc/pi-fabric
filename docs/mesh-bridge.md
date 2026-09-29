# Mesh bridge (v1)

`bin/mesh-bridge` links the Fabric mesh of this host (the hub, Dev1) to the mesh of one remote host
over an ssh stdio transport (smarty-dev#2004). Each host keeps its own mesh; the bridge republishes
existing events and mirrors root presence. There is no new store or protocol.

## What crosses

- `fabric.control.command` and `fabric.control.ack`, `fleet.work.*`, and `ops.owner` with kind
  `pr.wake`, when the recipient (`to`) is a live root or host native to the other side.
- Presence: each side's live native hosts and root participants go into the other side's state at
  the same keys, with `remoteHost: <side name>` and the source identity as `updatedBy`. A mirrored
  lease is `min(source expiry, now + 15 s)` and is renewed every 5 s. When the bridge stops, its
  mirrors lapse within 15 s. On a clean stop or a transport failure it deletes them at once.
- Nothing else. This excludes `github.*`, actor output, state and cache keys, and agent and actor
  participants.

## Rules

- Each bridged event keeps its `from`, `to`, `kind` and `text`. It carries
  `data.bridge = {from: <side>, id: <original event id>}`, and an event with a `bridge` field is
  never forwarded again.
- Per direction the cursor is the last source sequence handled. It is saved in the cursor file,
  and also after each forwarded event with the destination sequence of that event. On restart the
  bridge skips the source ids already bridged after that mark, so a crash does not forward an
  event twice.
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
  not bound to this link is dropped.
- The hub routes to the remote only by canonical ids: host id, identity id, participant id and root
  id. Labels, session ids and names are never addresses across.
- A mirror never replaces a native record or another bridge's mirror. It writes by
  compare-and-swap only.
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

## Running it

On the remote, pin the agent to the bridge's dedicated key in `~/.ssh/authorized_keys`:

    command="node /path/to/pi-fabric/bin/mesh-bridge agent --mesh /path/to/project/.pi/fabric/mesh --peer dev1",restrict ssh-ed25519 AAAA... mesh-bridge@dev1

On the hub, run one bridge per remote host as the fleet user, under the service manager:

    mesh-bridge run --mesh ~/proj/.pi/fabric/mesh --name dev1 --remote forge \
      --cursor ~/.local/state/mesh-bridge/forge.json --ssh forge --ssh-key ~/.ssh/mesh-bridge

`--ssh-port` and `--ssh-known-hosts FILE` (which also sets `StrictHostKeyChecking=yes`) reach a
host without a global `~/.ssh/config` entry.

When the bridge dies, the other side's copies stay live until their lease lapses, at most 15 s
(about 13 s on average). A steer or followUp sent in that window is not delivered; it fails when
its acknowledgement times out (the control plane's bounded wait), and nothing hangs. After the
lapse, senders get the lease-lapsed error at once.

The bridge exits with status 1 when the transport closes. The service manager restarts it, and it
resumes from the cursor file. `-- COMMAND...` replaces ssh with any stdio transport (tests use a
local agent process).
