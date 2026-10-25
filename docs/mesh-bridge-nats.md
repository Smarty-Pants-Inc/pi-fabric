# NATS link (file mesh ↔ NATS)

`bin/nats-link` (also `pi-fabric nats-link`) runs and inspects one link between this host's Fabric
file mesh (the hub) and an org's NATS broker (smarty-dev#15310). It lets a Main that moved to a NATS
runtime keep talking to Mains and actors that stay on the hub's file mesh, both ways, without either
side changing carrier.

The link is the file bridge of [mesh-bridge.md](mesh-bridge.md) with a NATS side: `MeshBridge` with
the hub's mesh as its local side and a NATS `BridgeSide` as its remote side. Everything that page says
about cursors, presence mirrors, ownership fences, the stall watchdog and the stop budget applies. This
page covers what is different: the NATS side, the allow-list, the config file and the CLI. The trust
model is in [bridge-nats-threat-model.md](bridge-nats-threat-model.md).

## What the link is on each side

- **On NATS** it is one Path B machine, for example `ryzen1-bridge`, with the standard agent grant: it
  publishes into other machines' inboxes only as itself, consumes only its own inbox, and writes
  presence and leases only under its own machine's KV keys. Every hub identity it carries is mirrored
  into KV presence under that machine. A NATS receiver checks a sender's presence against the
  credential machine of the delivery, exactly as for any runtime.
- **On the file mesh** it is one bridge link, named by `link` (for example `v2`). Every NATS identity it
  carries is mirrored into the hub's mesh as a host and root (or actor) record with `remoteHost: <link>`,
  so hub readers such as `smarty-org-target` and `review_gate.live_roots` see it as a live mirrored
  root, the same way they see a work host's roots.
- Its own identity on both sides is `bridge:<link>` (`bridgeNatsLinkIdentity`). It is never on the
  allow-list and never a target; it signs only what the link originates itself (topic deliveries).
- A command the link refuses from a verified hub sender is answered on the hub with a `notRun` ACK
  authored as the mirrored owner it was addressed to, with the link stamp (`data.bridge.from` = the link),
  because a hub sender accepts a mirrored owner's ACK only in that form. An unverified sender gets no
  answer: its command is dropped and logged.

## What crosses (PR 1)

Only identities named in the config's allow-list cross, and only the operations their destination class
takes. The class comes from the allow entry (`kind`, `principal`, list), never from a message:

| Destination | Takes |
|---|---|
| principal seat (a root entry with `principal`, either side) | `followUp`, `cancel` |
| moved lead (a root entry in `allow.nats`) | `followUp`, `steer`, `ask` (and its answer), `cancel` |
| hub root or actor (an entry in `allow.file`) | `followUp`, `steer`, `ask` (and its answer), `cancel` |
| anything else (a NATS actor, an unlisted id) | nothing: refused before publish with a `BRIDGE_NATS_*` code |

`stop`, `setModel` and `setThinking` never cross. An ACK crosses only back to the verified requester of a
command the link claimed. Addressed `fleet.work.*` and allow-listed `ops.owner` events on the hub for a
NATS root become one keyed `followUp` to it, from `bridge:<link>`, when the entry lists the topic in
`receiveTopics`.

`github.*` never crosses.

## What crosses (PR 2)

### NATS events republished into the hub (N4)

A Main on NATS publishes its topic events to the org's event stream (`FABRIC_EV_<ORG>`), not to any file
mesh. With `nats.events: true`, the link reads that stream and republishes a `fleet.work.*` event into the
hub's mesh on the same topic, so the hub factory and every file-mesh reader see it as they see a hub event.

- Only events from an `allow.nats` entry that lists the topic in `republishTopics` (default none) cross.
- The sender is the KV presence record its `from.id` resolves to: exactly one live root on an org machine
  other than the link's own. An event subject names no source machine, so this is the only binding.
- The republished event is authored as `bridge:<link>`. It keeps the verified sender and the source in
  its `data.bridge` stamp.
- Exactly once by the stream sequence and the ledger: a redelivery after a crash is recognized and
  appended once. The stream message is acknowledged only after the durable file append.
- No loops: an event that already crossed a bridge (`data.bridge`), or that claims an identity the link
  mirrors from the hub, is never republished.
- It needs the events grant on the link's NATS user (FABRIC_EV consume), a grant beyond the standard
  agent template. `start` refuses a config with `nats.events: true` when the credential does not carry
  it, and the config refuses `republishTopics` without `nats.events: true`.

### Spoke relays

A root on a work host's file mesh (a spoke bridged to this hub by `mesh-bridge`) reaches a NATS root, and
gets its answers, through the hub's existing relay between bridge processes: the NATS link is one more
relay side.

- The spoke root is listed in `allow.file` with `viaSpoke: "<spoke link>"`, the name of the hub's bridge
  to that spoke. On the hub its record is that bridge's mirror (`remoteHost` = the spoke link), and its
  identity comes only from that mirror's verified presence, never from the relayed message.
- The same destination classes and operations apply as for a hub root.

The counters of both legs are in `nats-link status`.

## Identity rules

- A sender is mapped only from the verified presence of the side its message came from: the hub's
  native records for a file sender, KV presence on the delivery's credential machine for a NATS sender.
  The `from` in a message body is a lookup key, never an identity.
- A `commandId` is mapped per hop (`bridgeNatsMappedCommandId`: link, direction, verified sender,
  original id), so one sender can never read another sender's stored outcome. The link's ledger keeps
  the mapping both ways and maps the owner's ACK back.
- The source is acknowledged only after the destination's durable append. A redelivery after a crash or
  a lost ACK replays the recorded outcome; it never runs the command twice.
- The allow-list is read once at start and never widened. A principal seat's id must equal that
  principal's `orgInstance.fabricSession` in org.json, and an id that is a principal's bound session must
  be declared as that seat; `start` refuses otherwise, before it opens anything.

## Config file

One JSON object, passed as `--config FILE`. It holds names and paths only, never a credential's bytes.
The file must be a regular file owned by the user running the link (or root), not group- or
other-writable, in a directory others cannot write: it decides who may cross. Unknown keys are refused
at every level, so a misspelt field never falls back to a default.

```json
{
  "version": 1,
  "name": "hub-a",
  "link": "v2",
  "file": { "mesh": "/srv/fabric/mesh", "lockProtocol": 2 },
  "stateDir": "/srv/nats-link/v2",
  "orgJson": "/srv/org/org.json",
  "nats": {
    "servers": ["tls://nats.example.test:4222"],
    "machine": "hub-a-bridge",
    "credentials": "/srv/nats-link/secrets/hub-a-bridge.creds",
    "caFile": "/srv/nats-link/secrets/ca.pem",
    "org": "example-org",
    "custodyStream": "FABRIC_CUSTODY",
    "livenessStream": "FABRIC_LIVENESS",
    "pathBMachines": ["hub-a-bridge", "sandbox1"],
    "operatorMachine": "fabric-ops"
  },
  "allow": {
    "file": [
      { "kind": "root", "id": "session:00000000-0000-7000-8000-00000000000b", "principal": "Owner",
        "receive": ["followUp", "cancel"] },
      { "kind": "actor", "rootId": "session:00000000-0000-7000-8000-00000000000c", "name": "review",
        "send": ["followUp", "steer"] }
    ],
    "nats": [
      { "kind": "root", "id": "session:00000000-0000-7000-8000-00000000000a", "principal": "Second",
        "machine": "sandbox1", "send": ["followUp", "cancel"], "receive": ["followUp", "cancel"],
        "receiveTopics": ["ops.owner"] },
      { "kind": "root", "id": "session:00000000-0000-7000-8000-00000000000d", "machine": "sandbox1",
        "send": ["followUp", "steer", "ask", "cancel"], "receive": ["followUp", "steer", "ask", "cancel"],
        "receiveTopics": ["fleet.work.*"], "note": "a lead that moved to v2" }
    ]
  }
}
```

| Field | Meaning |
|---|---|
| `name` | This hub's bridge name (MeshBridge `localName`). |
| `link` | The link name: the `remoteHost` of every NATS record mirrored into the file mesh. Must differ from `name`. |
| `file.mesh` | Absolute path of an existing mesh root (it holds `state.json`, `state.db` or `events.jsonl`; a wrong path fails, never becomes a new mesh). |
| `file.lockProtocol` | 1, 2 or 3, as every other writer of that mesh uses (default 1). |
| `file.stateBackend`, `file.maxStateBytes` | Optional, as `mesh-bridge run` (`--max-state-bytes`). Absent: the root's moved marker or the environment selects the backend. |
| `stateDir` | Absolute, private (0700) directory of the link's cursor (`cursor.json`), stall reason, ledger (`ledger/`) and lock (`link.lock`). Not inside the mesh root. One link per state directory. |
| `orgJson` | Absolute path of the org.json whose principal seats pin the allow-list. Absent: the installed factory's org.json, then the org workspace's. A missing, unsafe or malformed org.json refuses the start. |
| `nats.servers` | `tls://<DNS name>:<port>` URLs (no numeric addresses). |
| `nats.machine` | The link's own Path B machine. Must be in `pathBMachines`, never an operator machine. |
| `nats.credentials`, `nats.caFile` | Absolute paths. The credential must be a regular file owned by the user running the link, mode 0600 or 0400. |
| `nats.org`, `custodyStream`, `livenessStream`, `pathBMachines`, `operatorMachine` | The org's Path B policy, as `mesh.nats` in `fabric.json`. They must match the scope the credential was minted with. |
| `nats.events` | Default false. True reads the org's event stream for N4 republishing; needs `org` and the events grant on the link's NATS user. |
| `nats.inboxLanes` | Default 8. |
| `allow.file`, `allow.nats` | Who may cross; see below. |
| `callTimeoutMs` | Deadline of each NATS-side call (default 30,000; 1,000 to 300,000). |
| `logLevel` | `info` (default) or `debug`. |

The NATS settings are taken from this file only. `PI_FABRIC_MESH_NATS` and `fabric.json` are never
merged in: the link's machine identity comes from its own config.

### Allow entries

`allow.file` lists identities native on the hub's file mesh; the link mirrors them into NATS KV, so v2
can reach them and they may send to v2. `allow.nats` lists identities native on the broker; the link
mirrors them into the hub's mesh, so the hub can reach them and they may send to the hub. An identity
appears once, on one side.

- A root: `{ "kind": "root", "id": "session:<lowercase uuid>" }`, with `principal` when it is that
  principal's org seat.
- An actor: `{ "kind": "actor", "rootId": "session:<uuid>", "name": "<actor name>" }`, with an optional
  `id` pin (32 lowercase hex). The actor must be live under that root with that name.
- `send`: operations it may send across (default `["followUp"]`; `[]` for none).
- `receive`: operations it may receive from across, within its destination class (default
  `["followUp"]` where the class takes it; a NATS actor receives nothing).
- `receiveTopics`: roots only; exact topics or `prefix.*`.
- `republishTopics`: `allow.nats` roots only (PR 2, N4); the `fleet.work.*` topics of this identity's
  NATS-published events that the link republishes into the hub's mesh. Default none; needs
  `nats.events: true`.
- `viaSpoke`: `allow.file` roots only (PR 2); the hub's `mesh-bridge` link to the spoke this root lives on.
- `principals`: Fabric principal ids whose authority this sender may carry across; any other is stripped.
- `machine`: required on every `allow.nats` entry, and only there; pins the identity to one org machine in
  `pathBMachines` (never the link's own). Without a pin, another machine could register that id while the
  real owner is offline.
- `note`: for operators; never authority.

## Commands

Every command except `start` prints one JSON object (`roots`: one array) on stdout and diagnostics on
stderr. Exit 0 means ok, 1 not ok or unknown (never 0 on missing data), 2 a usage or config error.

### start

    nats-link start --config FILE

Checks the config and its files (credential private and ours, CA present, mesh existing, `stateDir`
private; created 0700 when absent), checks the allow-list against org.json's principal seats, takes the
state directory's lock (`link.lock`, with its pid and process start time; a second start on the same
directory exits 2 with `FABRIC_NATS_LINK_BUSY`, a lock whose process is gone is replaced), then runs the
link in the foreground until SIGTERM or SIGINT. A requested stop exits 0 after the link stops (at most
five seconds); a second signal exits at once. When the link ends by itself (lost broker, failed transport)
it exits 1 and the service manager restarts it; it resumes from its cursor and ledger.

Run it under the service manager as the fleet user, one unit per link, with `TimeoutStopSec=10` and
`Restart=on-failure`. Add `NotifyAccess=all` and `WatchdogSec=90`, as for `mesh-bridge run`, only with a
release whose link sends `WATCHDOG=1` (MeshBridge does when its `notifyWatchdog` hook is wired):
without it, systemd would kill a healthy link.

### dry-run

    nats-link dry-run --config FILE

Reads both sides' presence and prints, for every allow entry, what it resolves to now and where the link
would mirror it (`bridgeNatsDryRun`): `status` is `live` or the refusal code that would apply. It checks
the same org.json seats as `start`, writes nothing, creates no state directory and starts nothing. Exit 0
when every entry is live. Use it before a canary: with the real config, it shows each listed identity
verified on its home side, without carrying any traffic.

### withdraw

    nats-link withdraw --config FILE

The rollback. It stops the running link (SIGTERM to the process in `link.lock`, checked by its start time,
waiting up to ten seconds), takes the lock itself, then deletes everything the link mirrored:

- on NATS, every lease and presence key under the link's own machine, leases first, so routing sees the
  hub's roots gone at once (the link writes nowhere else; other machines' keys are never touched);
- on the hub's mesh, every record mirrored under the link's name, as `mesh-bridge` withdraws a link.

It prints `stopped`, the NATS keys deleted, the file records removed and any `problems`; exit 0 only when
the link is stopped and both sides are clean. If the link does not stop, it withdraws nothing (the link
would mirror them again). A key that changed between the snapshot and its delete means a writer is still
active and is reported. Run the unit with `Restart=on-failure` (a requested stop exits 0 and is not
restarted), or stop the unit first. Without a withdraw, a stopped link's NATS mirrors stop routing when
their 15 s leases lapse, and its file mirrors expire by their lease TTL.

### status

    nats-link status --config FILE

Reads the link's state and the hub's mesh: the cursor (`toRemote`, `toLocal`, age), the stall reason,
every record this link mirrors into the mesh, and each allow entry as the mesh shows it (a hub entry
native, a spoke root as its spoke link's mirror, a NATS entry as this link's mirror; `present`, `live`).
`problems` lists what is wrong; exit 0 when it is empty. A mirror under this link that the allow-list does
not name is a problem: the link must never carry it.

It also shows the running link's crossing counters, from `<stateDir>/counters.json`, which the link
writes at most once a second and on stop (cumulative across restarts; `since` is when counting began):

- `counters.values`: `control.toNats`, `control.toFile`, `control.refused`, `topic.toNats`,
  `topic.refused`, `republish.accepted`, `republish.duplicate`, `republish.refused`, `relay.toNats`,
  `relay.toFile` and `relay.refused`, with `counters.last` naming when each last moved;
- `republish`: whether `nats.events` is on, which senders list `republishTopics`, and the accepted,
  duplicate and refused counts;
- `relay`: the spoke roots (`viaSpoke`) and the toNats, toFile and refused counts.

Counters are an operator signal only: exactly-once and authority live in the ledger and the gates. A
snapshot written by another link is a problem.

### receipt

    nats-link receipt --config FILE --command ID [--after SEQ]

The hub-side evidence for one command id: every `fabric.control.command` event with that id (`from`,
`to`, owner incarnation, `data.bridge` stamp), every cancellation of it, and every ACK. Verdict
`acknowledged` (exit 0) needs exactly one command event and ACKs that agree (an identical replayed ACK is
still one outcome); `pending`, `duplicate`, `conflict`, `orphan-ack` and `not-found` exit 1. `--after`
starts the scan after a sequence. For a command that crossed from NATS, give the hub-side (mapped) id.

    nats-link receipt --config FILE --session ID [--nats]

The route evidence for one root: its hub record (native for an `allow.file` root; this link's mirror for
an `allow.nats` root), any hub handoff of it, and with `--nats` its NATS presence records. Verdict
`routable` (exit 0) needs a live record on each side read, a NATS record on the right machine (the link's
own for a hub root, an org machine for a NATS root), one record only, and the same owner incarnation on
both sides.

### roots

    nats-link roots [--config FILE] [--include-stale] [--kind root|actor|agent|provider]...
    nats-link roots [--servers URLS] [--machine M] [--creds FILE] [--ca FILE] ...

Lists NATS presence by the rule routing uses: a presence record and its host lease on the same machine,
host, identity, root and incarnation, not terminal. An id with records on two machines is ambiguous and
left out. With `--config` it reads as the link's machine; without, as the calling runtime, from
`PI_FABRIC_MESH_NATS` (the `mesh.nats` JSON a NATS Main is launched with) or the `PI_FABRIC_NATS_*`
variables, with the flags winning field by field. It opens a verified connection and the presence and
lease buckets only: no inbox, consumer, lease or presence of its own. It never prints `[]` without a
complete snapshot of both buckets (exit 1, `FABRIC_NATS_ROOTS_UNAVAILABLE`).

Each row: `id`, `kind`, `name`, `sessionId`, `rootId`, `machine` (from the KV key, never the record),
`ownerHostId`, `ownerIdentityId`, `ownerIncarnation`, `status`, `capabilities`, `live`.

This is the reader the org's tools use on a NATS host: `smarty-org-target` checks a seat's liveness with
it, and a hub root that the link mirrors shows here under the link's machine.

## Notes

- `status`, `receipt`, `roots` and `dry-run` open the mesh read-only, as `fabric-participants` does: they
  write no state or event. Opening a `MeshStore` still records the advisory writer census.
- `pi-fabric nats send` speaks as its own CLI identity, not the caller's Main. That identity is not on any
  allow-list, so a `nats send` from v2 to a hub target is refused by the link (a `BRIDGE_NATS_*` code,
  not run). Cross-link messages from a NATS Main use that Main's own `agents.followUp`.
- The mixed-carrier proof for a seat move (design §3a): `dry-run` before traffic, then for each keyed
  followUp `receipt --command` on the hub and `receipt --session --nats` for the destination root;
  `withdraw` to roll back.
