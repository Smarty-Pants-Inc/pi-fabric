# NATS consumer lifetimes

Hosts that only send messages can leave abandoned inbox durable consumers behind.
`inactive_threshold` bounds abandoned consumer state; it is not a hard lifetime for a consumer with live pulls.

## Inventory

The following is the complete durable creation inventory at base
`7ac1f5dcfc2d1b624657bbc5fa0114a31f99f155`. Line numbers refer to that base, before the lifetime patch.
There are four creation statements and six durable naming families; the shared inbox statement serves three paths.

| Consumer | Creation path | Previous lifetime | Policy in this change |
| --- | --- | --- | --- |
| Ordinary host `rt_<hash(hostId)>` | `src/transport/nats/inbox.ts:193–200,277–280` → `src/transport/nats/provision.ts:167` | Unlimited, even when never pulled | Seven days for session/other stable hosts; one hour for `runtime:` hosts |
| Temporary child/CLI `rt_<hash(hostId)>` | Same shared statement; `src/agents/child-env.ts:25–36`, `src/mesh/nats-auth/send-cli.ts:455–468` opt in | Default one hour; configured 300–86,400 seconds; best-effort deletion on clean close | Unchanged explicit temporary-host policy |
| Bridge inbox `br_<hash(link identity)>` | `src/mesh/bridge-nats-port.ts:119–149` → same shared statement | Unlimited | Seven-day shared default |
| Actor Fabric/GitHub `ev_<hash(actorId)>`, `gh_<hash(actorId)>` | `src/transport/nats/events-consume.ts:197–247`, create at 231 | Unlimited; mutable config also writes zero | Unchanged: independent durable delivery cursors |
| Event bridge `bev_<hash(link identity)>` | `src/mesh/bridge-nats-events.ts:206–239`, create at 225 | Unlimited | Unchanged: new consumer starts at `DeliverNew` or explicit `startAt` |
| Advisory operator `transport_dlq_operator` | `src/transport/nats/dead-letter.ts:79`, bootstrap caller `src/transport/nats/admin.ts:123–134` | Unlimited fixed singleton | Unchanged: operator is bind-only |

KV watchers (`src/transport/nats/kv.ts:338–344`) use SDK ordered **ephemeral push** consumers, not durables.
State projectors (`src/mesh/state-backend-nats-client.ts:397–443`) use ordered **ephemeral pull** consumers with a
10,000-millisecond initial inactivity threshold and best-effort cleanup of abandoned handles. The SDK's ordered
consumer options are milliseconds; raw durable `ConsumerConfig.inactive_threshold` values are **nanoseconds**.
Session channels are core NATS subscriptions, not JetStream consumers.

## Why these bounds

Seven days lets a normal Main or inbox bridge survive a weekend and several offline days without losing its
broker cursor. It matches the default inbox stream message age. Runtime identities are shorter-lived; one hour
matches the existing temporary-child default. Explicit temporary-host settings still take precedence.
These are supported inactivity windows, not a promise to preserve a broker cursor after a longer outage.

The patch does not change stream retention, subject filters, durable names, envelope serialization, ACK behavior,
USER.INFO verification or permissions. Reconnecting compatible legacy inbox consumers are updated **in place**,
with only `inactive_threshold` supplied as the update delta. Ownership and the other policy invariants must pass
before migration, and the updated configuration is read back. No delete/recreate is used for migration.

The pinned `@nats-io/jetstream` 3.4.0 implementation of `ConsumerAPI.update` reads the current consumer and sends
`action: update` through the same `$JS.API.CONSUMER.CREATE.<stream>.<name>.<filter>` request path already authorized
for inbox creation. No new UPDATE-subject grant is necessary. Dormant legacy durables whose hosts never return
are not retroactively repaired by a source patch; inventory or one-time administrative cleanup is separate work.

## Rollout and tested downgrade

This is an owner-by-owner rollout, **not** a promise that an old binary can open a migrated durable.
A host migrates only its own exact-name/owner-checked inbox. Hosts that keep running the frozen
`f33937156f587f2479c651ae301de44ec0f0592a` or `ca4b1482119748394df5125c2a8d2853676f7e04`
code do not migrate their inboxes. Do not reuse a migrated host identity with an old binary without
first restoring its threshold: the frozen `ca4b1482` helper rejects a positive threshold when its
normal-host policy omits the field. Forward healing in the new helper cannot change that old binary.

**Before downgrading a host whose old policy was unbounded** (ordinary/default-runtime `rt_`, or `br_`),
restore zero using the procedure below. **Do not zero-reset explicitly temporary `ephemeralHost` inboxes.**
Their original positive `ephemeralInactiveSeconds` setting is unchanged by this release; retain that setting
and they need no reset. The frozen old helper requires that explicit positive threshold and rejects zero.
A `runtime:` prefix alone does not imply explicit temporary-host configuration: check the host's original
`ephemeralHost` setting, not just the current threshold or ID.

For previously unbounded inboxes:

1. Stop/quiesce that host's new-code process through its normal owner-controlled lifecycle. Keep it
   stopped during restoration; a running new helper can heal the threshold back to the finite policy.
   Do not downgrade or start the old binary yet.
2. From a checkout of this release with its dependencies installed, run
   `bun tools/nats-inbox-downgrade.ts` using that host's existing machine-scoped runtime credentials,
   TLS CA/server addresses, and exact host ID. Supply the existing policy via `PI_FABRIC_MESH_NATS`,
   or `PI_FABRIC_NATS_CUSTODY_STREAM`, `PI_FABRIC_NATS_LIVENESS_STREAM`, and `PI_FABRIC_NATS_MACHINES`.
   No bootstrap/operator credentials or new grants are needed. `$CREDS_FILE` is an absolute file
   path, not credential contents. Preview is read-only and is the default:

   ```sh
   bun tools/nats-inbox-downgrade.ts --host-id "$HOST_ID" --kind rt --machine "$MACHINE" --creds "$CREDS_FILE" --ca "$CA_FILE" --servers "$NATS_SERVERS"
   ```

3. Check the preview's computed stream/durable and current/target thresholds, then apply:

   ```sh
   bun tools/nats-inbox-downgrade.ts --host-id "$HOST_ID" --kind rt --machine "$MACHINE" --creds "$CREDS_FILE" --ca "$CA_FILE" --servers "$NATS_SERVERS" --apply
   ```

   For a bridge, **also** restore its `br_` inbox with the same command and `--kind br`; the host ID
   must be its exact `bridge:<link>` identity. If the bridge also owns an ordinary `rt_` inbox, restore
   that with `--kind rt` too. `br_` ownership is checked against the link, not a session description.
   Add `--org "$ORG"` if the host uses a non-default NATS org.
4. Require a successful JSON receipt reporting threshold zero before starting the old binary. If an
   apply fails or its outcome is ambiguous, keep the host stopped and run the read-only preview to
   reconcile; never assume success or delete/recreate the durable as a fallback. The tool is not a
   fleet sweep and does not change actor/event/operator durables.

The tool validates the computed name, exact owner description, filter and all non-lifetime inbox
policy invariants before writing. Its only update delta is `{ inactive_threshold: 0 }`, followed by an
independent read-back and validation. It never creates, deletes or resets a consumer. An already-zero
consumer needs no write. A missing/expired durable is refused: this tool cannot restore a cursor
already lost to expiry. Re-enabling the new code later migrates the threshold forward again.

`tests/nats-inbox-downgrade.test.ts` exercises the real TLS/USER.INFO CLI path on NATS 2.14.7 with
unchanged runtime grants: the byte-pinned frozen old helper creates a legacy consumer; the new helper
migrates it; the old helper honestly rejects it; dry-run does not mutate it; applying the tool restores
zero; and the old helper reopens it with creation identity, ACK floor and pending state intact. A separate
frozen-old explicit-positive counterexample preserves the unchanged temporary-host policy and shows why
zero-reset must not be used for that case.

## Reconnect and replay boundary

Every host inbox delivery generation calls `ensureTransportConsumer` before binding. Verified native reconnect
aborts the old generation; hard-close recovery reopens the transport. Missing consumers are created again, and
the bridge inbox has the same ensure-before-bind recovery loop. Re-verification remains mandatory before
consuming or settling buffered messages.

A re-created inbox uses `DeliverPolicy.All`. Inbox streams use **Limits** retention, so deleting a consumer does
not itself delete retained messages. Pending retained traffic can therefore be delivered after consumer expiry.
Stream age/size limits still apply: this is not indefinite offline retention.

**Expiry destroys the broker ACK cursor.** `All` also replays retained records that were already ACKed, TERMed or
exhausted; their delivery counts restart. An ordinary reconnect or threshold-only migration preserves that cursor;
re-creation does not. The ten-minute publisher deduplication window does not deduplicate consumer replay.
The application control inbox prevents repeated command execution independently of JetStream publication dedupe:

- `src/topology/control-plane.ts` stores a local `control-seen` claim keyed by stable host and `commandId` **before**
  invoking the handler. The retained outcome binds target, requester, source machine and body digest. Replayed
  commands return that outcome; a claim without an outcome returns **indeterminate**, never a second execution.
- Claims survive through the command deadline plus 24 hours. Once an old claim can be collected, the unchanged
  command deadline check refuses execution. An old owner incarnation is also refused before claiming new work.
- The bridge command ledger binds original/mapped command IDs and retains them through the deadline plus 24 hours;
  the destination control inbox has the same admission protections. The bridge's ordinary event-seen receipts are
  only 24 hours, so they are not an eternal generic replay guarantee.

The real Main-to-actor ask regression in `tests/nats-auth-path-b-actor-ask.test.ts` deletes the private fixture's
already-ACKed owner inbox cursor, waits for both retained commands to be ACKed again after recovery, and checks
that native messages, runs, model calls and session bytes are unchanged. That is application-effect dedupe, not
absence of broker redelivery. `tests/control-plane-nats-replay.test.ts` separately covers durable outcome replay
across owner restart.

The transport itself is not an exactly-once delivery API. Do not claim universal duplicate-free replay after expiry.
Legacy inbox streams with
`max_age: 0` and streams configured for more than seven days can retain already-ACKed history beyond the default
inactivity window.

## Why stateful event/operator durables are excluded

Adding a threshold to these consumers alone would change delivery guarantees:

- Actor consumers restart at `All` or their original `eventsStartAt`, not a persisted broker resume point. Actor
  queues keep only 4,096 recent event-ID receipts. Older replay can repeat completed activations, and a missing
  consumer on a live connection currently leaves a sticky delivery failure with no automatic recovery.
- The event bridge defaults to `DeliverNew` on creation. Expiring it would skip messages published during the outage.
  Replacing that with `All` would replay accepted history instead; neither is equivalent to retaining its cursor.
- The advisory operator deliberately cannot create consumers. Expiring the fixed singleton would require bootstrap
  intervention or a permission expansion, neither of which belongs in this leak fix.

Bounding those cursors safely needs a separate delivery/resume design or an explicit changed replay contract.
A threshold-only patch cannot provide both lossless and duplicate-free delivery after deleting the only cursor.

## Verification

Use the real Path B fixture with machine-scoped credentials to check create, legacy threshold-only update and
missing-consumer recovery. Preserve the verified reconnect/ACK and KV-resnapshot regressions introduced by frozen
`f33937156f587f2479c651ae301de44ec0f0592a` (`tests/nats-reconnect-auth.test.ts`, `tests/nats-kv-watch.test.ts`).
Tested against NATS 2.14.7 (the CI pin) and 2.11.17.
A passing fixture run is version-specific compatibility evidence, not proof that existing fleet consumers have
already migrated. Deployment and any one-time cleanup are separate from this source change.
