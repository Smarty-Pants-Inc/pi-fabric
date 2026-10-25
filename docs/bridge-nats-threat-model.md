# A' bridge (NATS <-> file mesh): threat model for the named security pass

smarty-dev#15310 (D3 gate 3), pi-fabric lane L3. This page is the brief for the named security pass
(Luna max) on the A' link: credential, identity and access-grant work, Tier 1. It names what the link
may do, what it must refuse, where the code enforces each rule, and the risk that remains.

Status of the code it describes (PR 1):

- Written and unit-tested by L3: the contract `src/mesh/bridge-nats-types.ts`, the identity and
  allow-list gate `src/mesh/bridge-nats-identity.ts`, and the durable ledger
  `src/mesh/bridge-nats-ledger.ts` (tests: `tests/bridge-nats-types.test.ts`,
  `tests/bridge-nats-identity.test.ts`, `tests/bridge-nats-ledger.test.ts`).
- Written by other lanes, judged against the rules here: L1's raw NATS port `BridgeNatsPort`
  (`src/mesh/bridge-nats-port.ts`, with `-presence.ts`, `-actors.ts`, `-roots.ts`), L2's
  `NatsCarrierSide`, topic translators and link (`src/mesh/bridge-nats-carrier.ts`, `-file.ts`,
  `-topics.ts`, `-link.ts`), L5's config, operations and CLI (`src/mesh/bridge-nats-config.ts`,
  `-ops.ts`, `src/mesh-bridge-nats-cli.ts`, `bin/nats-link`, `docs/mesh-bridge-nats.md`). Where this
  page says "L1 must" or "L2 must", the pass checks that code for it. L1's event-stream consumer
  (`src/mesh/bridge-nats-events.ts`, N4) is PR 2 and not wired in PR 1.
- PR 2 adds the relay between bridges for work-host lanes (R13) and the republish of a v2 Main's own
  `fleet.work`/`ops` events into the file mesh (N4, R14). Without them, topic routes cross file -> NATS
  only and a work-host lane cannot reach a v2 lead.

## 1. What the link is

One process on Ryzen 1 joins two meshes:

- **the file mesh** on Ryzen 1 (the hub), through the existing `MeshBridge` with its local
  `StoreBridgeSide`. Every Ryzen 1 lead, the org seats that stay there, the supervisors and
  review-astra live here.
- **the org NATS broker**, through L2's `NatsCarrierSide` over L1's `BridgeNatsPort`, logged in as one
  Path B machine, `ryzen1-bridge`. Moved leads and moved principal seats (Kate's v2 Main first) live here.

It mirrors allow-listed presence each way and carries control commands (followUp, steer, ask, cancel)
and their ACKs. File identities appear on NATS as KV presence under `ryzen1-bridge`; NATS identities
appear in the file mesh as mirror records with `remoteHost: <link>`.

## 2. Assets

| Asset | Why it matters |
|---|---|
| A principal's org seat (the session org.json binds to Paul, Kate, Marisela) | Messages to it are messages to a principal. smarty-dev's org_asks.py, paul_queue.py, smarty-org-target and smarty-send all trust `orgInstance.fabricSession` as that principal's sender id. |
| Command authority over a Main | steer and ask drive a lead's turns; a forged steer is an agent acting under someone else's name. |
| ACKs and ask answers | An ask answer is the target's output; delivered to the wrong requester it is a data leak. |
| Message bodies in the ledger | The ledger keeps command bodies (for same-bytes resend) and ACK data. |
| The `ryzen1-bridge` credential | The bridge speaks for every file identity it mirrors; the NATS side trusts that credential's presence. |
| The allow-list and org.json | They decide who may cross and which ids are principal seats. |

## 3. Who is trusted for what

- **Ryzen 1 same-UID processes** are trusted as today: a native event's `from` on the file mesh is set
  by the publishing host process (the existing same-UID tool boundary, #820 residual). The link adds no
  new file-side authority; it only reads native presence and publishes as `MeshBridge` already does.
- **A NATS machine** is trusted for presence under its own KV prefix and for what it publishes from its
  own credential: the broker binds the last subject token of `fabric.in.<dst>.<host>.<kind>.<target>.<src>`
  to the publishing credential, and NATS receivers accept a sender only if its live presence is on that
  same machine (`control-plane.ts` `#natsSenderVerified`).
- **`ryzen1-bridge`** gets ONLY the standard Path B agent template (`src/mesh/nats-auth/permissions.ts`
  `pathBRuntime`): publish `fabric.in.*.*.*.*.ryzen1-bridge`, KV writes under
  `$KV.fabric_presence.ryzen1-bridge.>` and `$KV.fabric_leases.ryzen1-bridge.>`, its own inbox
  `FABRIC_IN_ryzen1-bridge`. It never publishes with another machine as the source and never writes
  another machine's KV keys. Any need beyond that template goes to Paul first.
- **Message bodies are never trusted for identity.** A `from`, `data`, `text` or body field is at most a
  lookup key.

## 4. Rules and where they are enforced

### R1. Identity comes only from the presence of the side the message came from

`BridgeNatsIdentity.verifySender(side, claimedId, presence, other, sourceMachine)` looks the claimed id
up in that side's presence and builds the crossing identity (`id`, `name`, `kind`, `sessionId`, owner,
incarnation, machine) from the presence record alone. It refuses:

- no live record, or an observation older than 15 s (`BRIDGE_NATS_SENDER_UNVERIFIED`);
- on NATS, a record on a machine other than the delivery's credential-bound source machine
  (`BRIDGE_NATS_SENDER_UNVERIFIED`), a missing source machine, and any delivery whose source machine is
  `ryzen1-bridge` itself (`BRIDGE_NATS_SIDE_MISMATCH`: the link's own mirror echoing back);
- an id with records on two NATS machines, live or not (`BRIDGE_NATS_AMBIGUOUS`; the NATS directory
  treats it the same way), an id that also has this link's mirror record next to a native one, and an
  id that answers natively on both sides;
- an id that exists on this side only as this link's mirror of the other side (`BRIDGE_NATS_SIDE_MISMATCH`);
- on the file side, another link's mirror (PR 1 has no relay);
- an actor whose owner root is not live with it (on NATS: on the same machine);
- a root whose `rootId` is not its own canonical `session:<uuid>`, and any `agent` participant.

`allow()` accepts only identity objects this gate instance issued (a `WeakSet`), at most 15 s old: a
caller cannot build a `BridgeNatsVerifiedIdentity` literal or reuse a stale one. Tests:
`tests/bridge-nats-identity.test.ts` "senders and targets come only from presence" and "refuses
identities it did not issue".

L2 must call `verifySender` for every command and `verifyTarget` for its destination before any publish
in both directions, and publish the crossing `from` as `bridgeNatsMeshIdentity(identity)`, never the
event's own `from`.

### R2. The allow-list is static, explicit and frozen

`parseBridgeNatsAllowSection` parses the link config's `allow` section once at start: root entries by
exact canonical id (`session:<lowercase uuid>`), actor entries by owner root plus name (optional id pin),
each in the list of its home side (`allow.file` or `allow.nats`). Unknown keys, duplicate identities, an
identity on both lists, malformed ids (upper case, bare uuid, trailing newline), an `allow.nats` entry
without a `machine` pin, and machine pins in the file list or on the bridge's own machine are refused.
Every NATS identity is pinned to the machine it must be live on (integrator decision, threat T3). The result is deep-frozen; there is no add or
remove API. L5's `loadNatsLinkConfig` refuses a config file that is a symlink, not ours (or root's), or
group/other-writable.

### R3. Allow-list per destination class, enforced before publish

The class comes from the allow-list entry (`kind` plus `principal`, and its list), never from the message
(`bridgeNatsDestinationClass`, `BRIDGE_NATS_CLASS_RECEIVE`):

| Destination class | Allowed ops |
|---|---|
| principal seat (org.json-bound principal session) | followUp, cancel |
| moved lead (allow-listed lead Main on v2) | followUp, steer, ask (and its answer), cancel |
| Ryzen 1 root or actor (file side) | followUp, steer, ask (and its answer), cancel, ACK |
| anything else | refused before publish, with a stable `BRIDGE_NATS_*` code |

How the code reads this table:

- `seat` = a root entry with `principal` (on either list). `movedLead` = a root on `allow.nats` without
  `principal`. `fileParticipant` = a root or actor on `allow.file` without `principal`. `refused` = an
  actor on `allow.nats`.
- The cap is applied twice: at parse (an entry whose `receive` names an op outside its class is a config
  error) and in `allow()` before the entry's own lists (`BRIDGE_NATS_DESTINATION_CLASS_REFUSED`). Inside
  the class, the sender's `send` and the target's `receive` must both list the op
  (`BRIDGE_NATS_OPERATION_NOT_ALLOWED`).
- stop, setModel and setThinking never cross.
- "ask (and its answer)": the answer travels in the ACK. An ACK is not a destination grant: it crosses
  only back to the verified requester of a command the ledger claimed (R7), so a seat that sends a
  followUp still receives its ACK.
- A topic route (N3) arrives as a followUp, so its target's class must allow followUp.

Test: "applies the destination class table from the entry alone".

### R4. Principal seats match org.json exactly

`loadBridgeNatsSeatPins` reads org.json (the installed factory's, else the org workspace's, as
`smarty-org-target`) with smarty-send's strict rules: every principal has a complete, unique
orgInstance with a lowercase-uuid `fabricSession`, and the product owner is one of them; anything else
fails closed. The file must be ours or root's and not group/other-writable. Then:

- a seat entry (`principal: P`) must have `id == "session:" + P.orgInstance.fabricSession` exactly;
- an entry whose id is any bound session of an org instance (its fabricSession or a keeper-pin rotation)
  must declare that principal, so the seat class applies; a rotation can never be a declared seat;
- `createBridgeNatsIdentityGate` runs this check, so no gate exists over a contradicting allow-list.

A moved seat keeps its session id (handoff), so the smarty-dev pins still hold on both sides.

### R5. Principal authority is carried only when listed, otherwise stripped

A command's `principal` (`{id, binding}`, from its envelope, never from data) crosses only if the
sender's entry lists that principal id in `principals`; otherwise `allow()` returns `principal: "strip"`
and L2 must publish without it. Stripping removes authority; it never adds any. A malformed principal is
stripped. Topic routes never carry a principal.

### R6. commandId is mapped per hop

The far side sees `bridgeNatsMappedCommandId(link, direction, verifiedSenderId, originalCommandId)`, and
a cancel's `cancelCommandId` maps the same way. A receiver replays a stored outcome, including an ask
result, to whatever `replyTo` arrives on a commandId collision; with the verified sender inside the
mapping, a second sender that reuses another sender's commandId gets a different far-side id and never
the first sender's answer (L4's test "a second NATS sender re-using another sender's commandId gets NO
answer"). The ledger stores the map both ways (`lookup` by mapped id, `lookupOriginal` by direction,
requester and original id); the ACK goes back with the original id to the `replyTo` recorded at claim,
never to an address taken from the ACK.

### R7. Exactly once across the hop, and ACKs bound to their claim

- `ledger.claim()` is durable before L2 publishes. A second claim of the same mapped id returns
  `duplicate` (never publish again; replay the recorded state or ACK), `conflict` (different body digest:
  refuse with `BRIDGE_NATS_ID_CONFLICT`, record unchanged) or `mismatch` (same digest from another source
  machine, requester, replyTo or target: refuse with `BRIDGE_NATS_CLAIM_MISMATCH`).
- Envelope ids are deterministic (`bridgeNatsEnvelopeId(link, fileEventId)`, also the Nats-Msg-Id), and
  the claim keeps the exact resolved body: a resend after an uncertain PubAck or a restart uses the same
  id and bytes. The destination dedupes it (broker window, then the receiver's 24 h seen claim).
- `verifyAck(claim, ack)` accepts an ACK only from the command's destination side, for the claimed mapped
  id and target, from the claimed owner identity; on NATS also from the target's machine, with the same
  body digest and the envelope id `ack:<claimed envelope id>` (`BRIDGE_NATS_ACK_UNBOUND` otherwise).
  L2 must pass only native, unbridged file ACKs to it.
- `ledger.settle()` records the first owner ACK; a replay or a different later ACK changes nothing.

### R8. The source is acknowledged only after the destination's durable append

L2 must ack a NATS delivery only after `MeshBridge` read past it (the file append is durable) or after a
ledger replay of an already-appended command; a file event's command counts as sent only on a PubAck.
The ledger itself resolves every mutation only after its line is fsync'd.

### R9. No loops, no re-forwarding

An event that already crossed a bridge (`data.bridge`) is never forwarded again (`MeshBridge` rule;
`BRIDGE_NATS_BRIDGED_AGAIN`). The link's own mirrors are never senders or targets on the side that holds
them (R1).

### R10. Topic routes: file -> NATS only, to roots that ask for them

An addressed `fleet.work.*` or allow-listed `ops.owner` event on the file mesh may reach a moved root
only if that root's entry lists the topic in `receiveTopics` (exact, or `prefix.*`). L2 delivers it as a
followUp from the link's own identity `bridge:<link>` (L1 keeps it live in KV under `ryzen1-bridge`),
keyed by `bridgeNatsTopicIdempotencyKey(link, eventId)`, under the file bridge's factory rate bounds.
A non-participant publisher never sends a control command across.

### R11. Ledger privacy and integrity

The ledger directory is created 0700 and its file 0600; the link refuses a directory or file another
user owns or that group/others can read. Every created directory entry is made durable in its parent,
and a failed directory fsync is an error, never success.

One writer: the mesh's host-qualified custody lock (`src/mesh/custody-lock.ts`) inside the ledger
directory, pinned to pid, process incarnation, machine and boot. A dead holder (same machine, pid dead or
reused, or a previous boot) is recovered by renaming its lock to a name derived from that judged lock, so
two recoverers cannot displace a live successor; a live, foreign or unprovable holder is never displaced.

Each line carries a checksum, and every byte of every line is written (short writes are completed) before
its fsync. Load cuts at most ONE unfinished append (one line without its newline, or one final damaged
line): appends are sequential and fail-stop, so a longer damaged suffix, or damage before valid records,
is corruption and refuses to open. A write or fsync failure, or an unconfirmed compaction rename, stops
all further appends until a restart.

### R12. A refused command always gets an answer, and only its verified sender gets it

Integrator decision F (coordination notes 10 and 11). A command the link refuses before publishing
(a gate refusal, or a ledger `refuse` from `bridgeNatsClaimAction`) is answered at once, so the sender
never waits for its timeout:

- **To a file sender:** a `fabric.control.ack` with `accepted: false`, `notRun: true`, the stable
  `BRIDGE_NATS_*` code and the original commandId, addressed to the sender's `replyTo`. It is authored as
  the mirrored owner identity of the refused target, with the link stamp `data.bridge.from == <link>`,
  and it carries no `ownerIncarnation` (the link cannot attest an owner epoch). A file sender accepts a
  mirrored owner's ACK only from that `ownerIdentityId` with that stamp
  (`control-plane.ts` `#authenticatedAcknowledgement`); `bridge:<link>` is accepted only for relay
  refusals with relay codes, so it would leave the sender waiting. L2 writes it through
  `StoreBridgeSide.publish` under the held fence on that owner: it is published only while this link
  holds that owner's mirror.
- **To a NATS sender:** a rejection `ack:<envelope id>` with `notRun: true`, the stable code and the
  sender's own body digest, from the targeted file identity's KV mirror (finding D).
- **An unverified sender gets nothing:** the command is dropped (NATS: `term`) and logged. An answer
  addressed to an unproven identity could deliver a refusal, and its `notRun` permission to resend, to
  whoever claimed that id.
- **Only before publish:** `notRun` tells the sender a resend cannot deliver twice. The link writes it
  only for a command it never published; a command it carried is answered only by its owner's ACK (R7).

### R13. PR 2 relay: a work-host lane reaches a v2 lead only through the hub's relay, as its spoke mirror

A lane on a work host lives in its spoke's file mesh; the spoke's own `mesh-bridge` link mirrors it into
Ryzen 1's mesh (`remoteHost: <spoke>`). The existing `MeshBridge` relay carries a command between two hub
links over the hub's relay IPC (`#relayCommand`, `deliverRelay`) and returns its ACK through the hub mesh
(`relayRecipients`, only the outcome fields). The A' link joins that relay as one more peer.

- **Allow-list:** a spoke lane is an `allow.file` root with `viaSpoke: <spoke link>`, which must name a
  configured spoke link. It cannot be a principal seat or a NATS entry. Destination class `spokeRoot`:
  followUp, steer, ask (and its answer), cancel, the same as a Ryzen 1 root (integrator decision).
- **Identity:** such a root is verified ONLY as the hub's direct mirror held by its spoke link
  (`remoteHost == viaSpoke`, no `bridgeVia`), live and fresh. It is refused next to a native record or
  another link's mirror of the same id, and when the same id answers on NATS (a spoofed spoke sender).
  `MeshBridge.deliverRelay` also requires the authenticated relay peer to be that same spoke link
  (`relayOrigin(sender) == sourceRemote`, `data.bridge.from == sourceRemote`).
- **Relay peer authentication** (`src/mesh/bridge-relay.ts`, the security-relevant change of PR 2): a
  relay peer was only `<release>/bin/mesh-bridge run --mesh M --name H --remote R --cursor C`, judged from
  `/proc` (uid, start ticks, exe, cwd, cmdline) and its registration. It may now also be
  `<release>/bin/nats-link start --config FILE`, where:
  - the script is in the SAME release bin directory as this process's own entry (another release never
    matches), run by the same node binary and uid;
  - argv is exactly `start --config FILE`, FILE absolute and normalized;
  - FILE is a regular file (not a symlink) owned by this uid with no group or other bits (0600 or
    stricter), and parses as a link config (`loadNatsLinkConfig`);
  - its `name` must be this hub's name, its `link` the requested remote, its `file.mesh` this mesh root,
    exactly as `mesh-bridge`'s `--name`, `--remote` and `--mesh` must;
  - the peer's own entry script is part of its identity, so its registration must name the same script.
  `mesh-bridge run` peers are judged exactly as before. Tests:
  `tests/bridge-nats-relay-identity.test.ts` (real processes).
- **No `src/mesh/bridge.ts` hunk** is needed: `MeshBridge` already re-exports other links' hub mirrors to
  its remote with `bridgeVia`, relays commands, and returns relayed ACKs with only the outcome fields.
- **L2 must:** start the relay server for the A' link (`startBridgeRelay` with `bin/nats-link`) and pass
  `MeshBridge` its `relay`; for a relayed command, verify the sender with `verifySender` (which accepts
  `viaSpoke` roots only as above) and use `bridgeNatsRelayEnvelopeId(link, spoke, spokeEventId)`; set
  `destinationRemoteHost` to the spoke for a NATS command whose target is a spoke root; keep allow-listed
  spoke roots in the KV mirror. **L1 must** write those spoke roots under `ryzen1-bridge` only, with
  `remoteHost`/`bridgeVia` stripped (KV presence refuses them).

### R14. PR 2 N4: a v2 Main's own events are republished as the link, never as the Main

A v2 Main publishes `fleet.work.*` and owner events to the org's `FABRIC_EV_<ORG>` stream. The link reads
the allow-listed subjects (L1's consume leg) and republishes them into Ryzen 1's mesh on the same topic.

- **Allow-list:** an `allow.nats` root lists `republishTopics` (exact or `prefix.*`, `fleet.work.*` and
  `ops.*` only; default none). L5's config refuses them unless `nats.events` is true. The events grant is
  a NEW grant for the bridge machine (integrator: raised with hosts-lead and org); the link's start is
  gated on it.
- **Identity, weaker than R1:** an events subject carries NO source machine, and the events grant lets any
  events-granted machine publish any `fabric.ev.<org>.>` subject. `verifyEventSender` can therefore only
  require the claimed sender to be ONE live, fresh KV root on its pinned machine (never the bridge's),
  allow-listed on NATS. It marks the identity `presenceOnly`; `allow()` refuses such an identity for any
  control command or topic route.
- **Authorship:** `allowRepublish` allows only that root's `republishTopics` and returns the link's own
  identity `bridge:<link>` as the author. The claimed sender is attribution only (L1 records it beside the
  link stamp), never the event's `from`.
- **Exactly once and no loops:** by stream sequence plus the ledger; an event that already crossed a bridge
  (`data.bridge`), or that claims a file identity this link mirrors, never crosses back (L1).

## 5. Threats

| # | Threat | Mitigation | Residual |
|---|---|---|---|
| T1 | A message body or `from` names another identity | R1: lookup key only; identity rebuilt from presence | none known |
| T2 | A NATS runtime publishes as a Ryzen 1 lead | The broker binds the source token; NATS receivers check presence machine == credential machine; Ryzen 1 identities live only under `ryzen1-bridge`; the gate refuses a NATS sender whose entry is on `allow.file` | none known |
| T3 | A compromised NATS machine registers presence for a Ryzen 1 lead's or a seat's id under its own prefix | NATS receivers and the gate treat an id on two machines as ambiguous and refuse it; every `allow.nats` entry is pinned to its machine (required), so a seat or moved lead seen on any other machine is refused, even while the real one is offline | **Availability:** that machine can make the id unroutable on NATS. **Integrity while the bridge does not mirror a Ryzen 1 id** (the Ryzen 1 lead is offline): NATS receivers may accept the forged id as that lead; it still cannot cross to the file side (it is on the wrong list). This is a property of Path B presence today, not new here. |
| T4 | The `ryzen1-bridge` credential is stolen | Standard template only; credential file 0600, ours (L5 preflight); the allow-list limits the honest link | **HIGH by design:** the holder can mint KV presence for any id under `ryzen1-bridge` and NATS receivers will accept commands from it. Filed as security-gap smarty-dev#16633: let NATS receivers bound which ids a bridge machine may present (core change, not in A'). |
| T5 | A sender fishes another sender's ask answer by reusing its commandId | R6 mapping; ACK returned to the claimed `replyTo` | none known |
| T6 | Duplicate delivery after a crash, an uncertain PubAck or a redelivery | R7 claim before publish, deterministic ids, same bytes, receiver seen claims, ledger `seen` for deliveries | A command past the 24 h horizon is no longer deduped; receivers refuse it as expired |
| T7 | A forged or misrouted ACK | R7 `verifyAck` | none known |
| T8 | Echo loops between the two mirrors | R9; R1 side checks | none known |
| T9 | Principal authority smuggled across | R5 carry only when listed, else strip | An allow-list that lists a principal id for a sender trusts that sender with it |
| T10 | steer, stop or setModel into a principal's org seat | R3 class cap (parse and before publish); R4 exact pins | none known |
| T11 | The allow-list or org.json is edited by someone else | Owner and mode checks on both files; no runtime widening | Same-UID processes can edit them (the existing same-UID boundary) |
| T12 | Ledger disclosure or tampering | R11 modes and owner checks; checksums | Checksums detect damage, not a same-UID forger |
| T13 | Ledger crash inconsistency | R11 fsync before visible, complete writes, namespace barriers, one-append tail cut, refusal on longer damage, fail-stop | A disk that loses fsync'd data is out of scope |
| T14 | Stale presence authorizes traffic | 15 s freshness on observations and issued identities | Within 15 s a just-stopped Main can still be targeted; its owner refuses a stale incarnation |
| T15 | Topic injection into a moved lead | R10 receiveTopics, followUp only, rate bounds, never control | A file publisher can post allowed topics to a root that asked for them (as today on the file mesh) |
| T16 | Flooding across the link | Topic routes spend the file bridge's factory rate buckets (L2); past 1,000,000 live records the ledger refuses new claims and drops none | Control commands from an allow-listed sender have no new per-sender rate bound in PR 1 |
| T17 | The link answers in a mirrored owner's name | R12: only for a command it refused before any publish, with `notRun` and a `BRIDGE_NATS_*` code, no owner epoch, under the held fence on that owner, to a verified sender only | A link bug that refused after publishing would let the sender's resend deliver twice; L2's tests must show every refusal precedes the claim's publish |
| T18 | A process impersonates a relay peer to send or receive a spoke's commands | R13: `/proc` identity (uid, start ticks, exe, cmdline) of the same release's `mesh-bridge run` or `nats-link start --config FILE`, a private config whose names and mesh must match, and the peer's registration | A same-UID process can still edit the config or the registration (the accepted same-UID boundary, as for `mesh-bridge` today) |
| T19 | A spoofed spoke sender reaches a v2 lead | R13: only the hub's direct mirror held by the named spoke link, refused next to a native or other mirror or when the id answers on NATS; `deliverRelay` requires the relay peer to be that spoke link | none known beyond the spoke link's own trust |
| T20 | An events-granted machine posts `fleet.work` events as a moved lead | R14: presence-only sender on its pinned machine, authored as `bridge:<link>`, only the root's `republishTopics`, never control | **Accepted residual** (integrator): an events-granted machine can claim a live root's id; the machine pin narrows it. Same family as security-gap smarty-dev#16633 (bounding which ids a machine may present) |

## 6. Checklist for the pass

1. `bridge-nats-identity.ts`: every refusal path in R1, R3, R4, R5 and `verifyAck`; that `allow()` cannot
   be satisfied by a constructed identity; that class caps apply before entry lists.
2. `bridge-nats-ledger.ts`: durability order (fsync before visible), load rules, fail-stop, lock takeover
   conditions, file modes, horizon.
3. L2: the gate runs before every publish in both directions; a refusal becomes a `notRun` rejection ACK
   as R12 states (or, for an unverified sender, a dropped event and a log line), never a publish; a
   `BridgeNatsNativePublisher` is built only for a
   native, unbridged file event (verification `mesh`, no `data.bridge`); the crossing `from` is the verified identity;
   principal strip is applied; ACKs are mapped back through the ledger to the claimed `replyTo`; NATS
   deliveries are acked only after the durable file append.
4. L1: publishes only as `ryzen1-bridge`; writes KV only under its own prefixes; `presenceView()` returns
   every record for an id on every machine (the gate's ambiguity rule depends on it); the machine comes
   from the KV key, never a JSON field.
5. L5: config and credential file checks; the org.json path; `dry-run` writes nothing and creates no state
   directory; `withdraw` deletes only the bridge machine's own KV keys and this link's file mirrors.
6. The real-broker E2E (L4): one keyed followUp each way, a dropped ACK and a forced disconnect give one
   consumption; a second sender reusing a commandId gets no answer; a seat refuses steer; an
   un-listed NATS root is refused. PR 2: a spoke root's followUp reaches a v2 lead and its ACK comes back;
   a spoofed spoke sender is refused; an N4 republish appears once with no loop; a presence-only identity
   is refused for control.
7. PR 2 relay peer authentication (`src/mesh/bridge-relay.ts`): every refusal listed in R13, and that a
   `mesh-bridge run` peer is judged exactly as before.

## 7. Residual risks to record

- T4 (bridge credential speaks for any id it presents): security-gap smarty-dev#16633, receiver-side
  bounds on bridge-machine identities.
- T20 (an events-granted machine can claim a live root's id on `FABRIC_EV`): accepted for PR 2, same family
  as smarty-dev#16633; republished events are authored as the link and carry no authority.
- T3 availability: a compromised NATS machine can make a pinned id unroutable; it cannot impersonate it
  across the link.
- The same-UID boundary on Ryzen 1 (T11, T12) is unchanged by the link.

## 8. Dry run and rollback

The Tier 1 dry run, its exact command, expected output and tested rollback are in
`docs/bridge-nats-dry-run.md`.
