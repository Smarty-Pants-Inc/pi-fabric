# A' bridge (NATS <-> file mesh): threat model for the named security pass

smarty-dev#15310 (D3 gate 3), pi-fabric lane L3. This page is the brief for the named security pass
(Luna max) on the A' link: credential, identity and access-grant work, Tier 1. It names what the link
may do, what it must refuse, where the code enforces each rule, and the risk that remains.

Status of the code it describes (PR 1):

- Written and unit-tested by L3: the contract `src/mesh/bridge-nats-types.ts`, the identity and
  allow-list gate `src/mesh/bridge-nats-identity.ts`, and the durable ledger
  `src/mesh/bridge-nats-ledger.ts` (tests: `tests/bridge-nats-types.test.ts`,
  `tests/bridge-nats-identity.test.ts`, `tests/bridge-nats-ledger.test.ts`).
- Written by other lanes, judged against the rules here: L1's raw NATS port (`BridgeNatsPort`), L2's
  `NatsCarrierSide` and link (`src/mesh/bridge-nats-file.ts`, `src/mesh/bridge-nats-link.ts`), L5's
  config and CLI (`src/mesh/bridge-nats-config.ts`). Where this page says "L1 must" or "L2 must", the
  pass checks that code for it.
- Not in PR 1: the relay between bridges for work-host lanes (N2) and the v2 -> factory `fleet.work`
  republish (N4). Until PR 2, topic routes cross file -> NATS only.

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
user owns or that group/others can read. One writer: a lock pinned to pid, process incarnation, machine
and boot; a lock is taken over only when its holder is provably gone (same machine and boot, pid dead or
reused, or the machine rebooted); a foreign or unprovable holder is never displaced. Each line carries a
checksum. Load cuts only an unfinished final append; damage before valid records refuses to open; a
write or fsync failure stops all further appends until a restart.

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
| T13 | Ledger crash inconsistency | R11 fsync before visible, tail cut, refusal on damage, fail-stop | A disk that loses fsync'd data is out of scope |
| T14 | Stale presence authorizes traffic | 15 s freshness on observations and issued identities | Within 15 s a just-stopped Main can still be targeted; its owner refuses a stale incarnation |
| T15 | Topic injection into a moved lead | R10 receiveTopics, followUp only, rate bounds, never control | A file publisher can post allowed topics to a root that asked for them (as today on the file mesh) |
| T16 | Flooding across the link | Topic routes spend the file bridge's factory rate buckets (L2); the ledger refuses new claims past 1,000,000 live records instead of dropping any | Control commands from an allow-listed sender have no new per-sender rate bound in PR 1 |

## 6. Checklist for the pass

1. `bridge-nats-identity.ts`: every refusal path in R1, R3, R4, R5 and `verifyAck`; that `allow()` cannot
   be satisfied by a constructed identity; that class caps apply before entry lists.
2. `bridge-nats-ledger.ts`: durability order (fsync before visible), load rules, fail-stop, lock takeover
   conditions, file modes, horizon.
3. L2: the gate runs before every publish in both directions; a refusal becomes a `notRun` rejection ACK
   (or a dropped event and a log line), never a publish; a `BridgeNatsNativePublisher` is built only for a
   native, unbridged file event (verification `mesh`, no `data.bridge`); the crossing `from` is the verified identity;
   principal strip is applied; ACKs are mapped back through the ledger to the claimed `replyTo`; NATS
   deliveries are acked only after the durable file append.
4. L1: publishes only as `ryzen1-bridge`; writes KV only under its own prefixes; `presenceView()` returns
   every record for an id on every machine (the gate's ambiguity rule depends on it); the machine comes
   from the KV key, never a JSON field.
5. L5: config and credential file checks; org.json path; `--dry-run` writes nothing.
6. The real-broker E2E (L4): one keyed followUp each way, a dropped ACK and a forced disconnect give one
   consumption; a second sender reusing a commandId gets no answer; a seat refuses steer; an
   un-listed NATS root is refused.

## 7. Residual risks to record

- T4 (bridge credential speaks for any id it presents): security-gap smarty-dev#16633, receiver-side
  bounds on bridge-machine identities.
- T3 availability: a compromised NATS machine can make a pinned id unroutable; it cannot impersonate it
  across the link.
- The same-UID boundary on Ryzen 1 (T11, T12) is unchanged by the link.

## 8. Dry run and rollback

The Tier 1 dry run, its exact command, expected output and tested rollback are in
`docs/bridge-nats-dry-run.md`.
