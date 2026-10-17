# NATS Path B latency probe

The probe compares three real control paths:

- `FILE-fixed`: `mesh.transport=file`, FILE state.
- `SQLite-fixed`: `mesh.transport=file`, SQLite state (the worker refuses a silent fallback).
- `NATS Path B`: `mesh.transport=nats`, with hub and work state backends selected independently.

Each role is a separate `FabricRuntimeState` Main process with its own `PI_CODING_AGENT_DIR`. NATS roles may use externally seeded `hub` and `work` roots; the worker never removes those roots. FILE transport requires both Main processes to use one shared root because the FILE control path is the `events.jsonl` carrier.

## Run

Local authenticated test server (the fixture sets JetStream `sync_interval: always`):

```sh
bun scripts/benchmark-nats-path-b.ts --backend NATS-Path-B --n 20 --local-test-server --state-backend-hub file --state-backend-work file --load-shape steady --counterprobe
```

Repeat with `--n 50` and with `--state-backend-hub sqlite --state-backend-work sqlite` for the SQLite state run. Local results are explicitly labelled `LOCAL TEST SERVER`; they are not Intel 1's production-server measurements.

External NATS server and seeded roots:

```sh
bun scripts/benchmark-nats-path-b.ts --backend NATS-Path-B --n 50 --server tls://nats.example.net:4222 --owner-machine hub-a --sender-machine work-a --owner-credentials /secure/path/hub.creds --sender-credentials /secure/path/work.creds --ca-file /secure/path/ca.pem --mesh-root hub=/seeded/hub --mesh-root work=/seeded/work --state-backend-hub sqlite --state-backend-work file --load-shape burst
```

Use `--load-shape steady` for approximately 14 aggregate sends/second, or `--load-shape burst` for a synchronized burst. `--sender-processes` can set the number of distinct sender Main processes; a burst defaults to one per send. `--runtime-config PATH` passes the remaining runtime settings through to every role while the probe applies each role's root, transport, and state backend. Once the presence-projection runtime config is available, supply it with `--runtime-config PATH --presence-projection enabled`. Without it the result is labelled `pre-projection`, and `barClaimable` is false: the presence projection adds store-lock writes on both bar hosts, so only paired runs with that projection enabled can claim the bar.

`--hold-owner-admission` installs a worker-side hold before the owner's real `AgentMessageRouter` path. The probe observes that the sender has not returned and that the Main journal is unchanged, then releases the hold with a file. The worker also accepts `SIGUSR1` while waiting. This is test-worker behavior, not a production runtime setting.

The supported measurement path is `agents.followUp` to a remote Main. Actor `ask` is intentionally outside this slice: although the repository has a faux provider (`tests/mesh-bridge.test.ts`, around line 724), this separate-process harness does not wire a deterministic AgentManager session/model run. That case needs its own real queue/control test and is not inferred from a fake receiver.

## Metric contract and durability ordering

The primary field is exactly **`send -> admission-ACK (sender clock)`**: in the sender process, monotonic time immediately before the public `agents.followUp` invocation through the time that invocation resolves with `acknowledged: true`. Both timestamps use the sender's `process.hrtime.bigint()` clock. It is an upper bound on send-to-committed-admission, so it is conservative. Cross-host comparisons use only this metric; the probe's current workers are local same-host processes.

The same-host secondary is the owner's `main-followups/<sid>.json` durable commit timestamp minus the sender invocation timestamp. The commit event is emitted only after `MainAgentController.#save` returns from `writeFileAtomic(..., { durable: true })`.

The FILE path ordering is:

1. `AgentMessageRouter.acceptControl` calls `MainAgentController.deliverAgent` with the logical delivery ID.
2. The Main adds the item and saves the follow-up journal before returning accepted.
3. `FabricControlPlane.#executeClaimedCommand` awaits the handler, saves the accepted outcome, and only then publishes the FILE ACK; the sender resolves on that ACK.

The NATS path has the same handler ordering: `#settleNatsCommand` awaits the owner handler, persists its accepted outcome, and then sends the ACK. The receiver awaits that completion before acknowledging its JetStream delivery. The sender's NATS request settles only on the accepted ACK. Thus both primary paths put the accepted ACK after the Main journal commit. The `agents.ask` path is not included in this claim.

Source anchors (refresh line numbers when code moves):

- Main delivery and durable journal: `src/providers/agents-message-router.ts` (`acceptControl` Main branch), `src/main-agent.ts` (`deliverAgent`, `#save`), and `src/core/atomic-write.ts` (`writeFileAtomic` durable file/rename/directory barriers).
- FILE control: `src/topology/control-plane.ts` (`#executeClaimedCommand`, `#publishAcknowledgement`); `src/transport/file.ts` (`FileControlReader.drain`).
- NATS control: `src/topology/control-plane.ts` (`#settleNatsCommand`, `#sendNatsAcknowledgement`, `#receiveNats`, sender acknowledgement settlement); `src/transport/nats/inbox.ts` (`send`, `delivery.ack`).

## JSON evidence

Each `perSend` record includes `commandId`, Main `messageId`, `logicalAdmissionCount`, invocation and accepted-ACK sender timestamps, primary latency, sender command PubAck, owner accepted-ACK PubAck (or FILE ACK append), owner command delivery ACK-of-ACK, owner journal commit, and the same-host secondary. Null stage fields mean that carrier does not expose that stage. The run object includes N, backend/state backend, placement, load shape and server label. Counts include refusals, redeliveries, duplicate admissions, lost messages, and ACK timeouts, alongside median/p95/p99.

For NATS, the probe hashes the sender/work root's `events.jsonl` and configured mesh event archive before and after the sends. It also hashes the owner/hub root, and snapshots each Main's `topology/inbox/<hash>` cursor state. The probe expects those event-log/archive hashes and cursors to be unchanged. The FILE counterprobe records matching `fabric.control.command` and `fabric.control.ack` appends in the shared `events.jsonl`.

The archive evidence hashes every regular file under the absolute directory named by `<mesh-root>/event-archive.json`, in sorted relative-path order, along with the config file hash. No event archive path is included in output.

## Participant KV view for the seeder

The probe does not seed or mutate the private participant view. Each Main runtime publishes its own live host lease and participant presence through the authenticated `KvParticipantDirectory`; sender readiness waits until its own participant view resolves the live owner.

The NATS transport's private buckets are `fabric_presence` and `fabric_leases` (`src/transport/nats/kv.ts`). A participant presence record is read with the authenticated `TransportKv` surface, `transport.kv("presence").get(key)`, not by using the FILE participant directory. Its key is:

```text
<machine>.p.<sha256(participantId).hex.slice(0,32)>
```

The stored JSON has the KV wrapper `{ "owner": "<participantId>#<incarnation>", "value": <FabricParticipantRecord> }`; the KV revision is the CAS token. The companion host lease is in `fabric_leases` under `<machine>.h.<sha256(hostId).hex.slice(0,32)>`, with the same `{owner,value}` wrapper and a 15-second TTL. `src/topology/participant-view.ts` validates these key hashes, owner strings, record identities, and live lease before making a route available. Seeded records must match those contracts; the runtime owner still publishes/renews its own live lease and presence.

## NATS semantics referenced

- [JetStream concepts](https://docs.nats.io/nats-concepts/jetstream): streams persist messages and consumers track delivery/ack state.
- [Streams](https://docs.nats.io/nats-concepts/jetstream/streams): stream retention and publish acknowledgements.
- [Consumers](https://docs.nats.io/nats-concepts/jetstream/consumers): explicit consumer acknowledgements and redelivery.
- [Headers](https://docs.nats.io/nats-concepts/jetstream/headers): message-ID-based duplicate detection used by the transport publisher.
