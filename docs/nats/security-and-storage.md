# Accounts, subjects, storage and limits

## Identity and account boundary

All client, route and leaf connections require TLS 1.3 and certificate verification.
Client/leaf listeners also use `verify_and_map: true`: the certificate's DNS SAN
maps to the exact configured NATS user. There is no anonymous user, password,
bearer token, `no_auth_user`, blanket response permission or unauthenticated leaf
account override. DNS SANs are identities here, not public DNS requirements.
Transport and route CAs are separate; only three route peers receive route keys.
Do not trust an ordinary host client certificate as a route certificate.

`FABRIC` is the application account on cores, with JetStream enabled and bounded.
Each leaf has a local `FABRIC` account, JetStream disabled, with only its own
`fabric.<host>` user. Its outgoing mTLS identity `leaf.<host>` maps into the cores'
`FABRIC` account with the **same host ACL**, so weakening a local leaf ACL does not
grant another host's root at the hub. `SYS` exists only on cores; its
`fabric.sys` user can publish `$SYS.REQ.>` and subscribe `$SYS.>` and its own
`_INBOX.sys.>`. System monitoring identity is not an application user.

Per-host certificate identities are distinct from process/session owner identity.
Shared-UID processes can use that UID's certificate; neither Tailscale nor mTLS
proves a session's intrinsic owner. The Fabric adapter must retain leases,
recipient/custody checks, session binding, CAS, dedupe, and unknown-outcome rules.
Replies and NATS protocol/API messages must not be accepted as authority-bearing
Fabric control envelopes without those checks.

## Per-host permission table

For host `H`, both `fabric.H` and its core-side `leaf.H` allow only:

| Direction | Subjects |
|---|---|
| Publish | `fabric.root.H.>`, `$KV.STATE_H.>`, `$JS.fleet.API.$KV.STATE_H.>`, `_INBOX.H.>` |
| Shared publish | `fabric.fleet.work.H.>`, `fabric.fleet.presence.H.>`, `fabric.fleet.request.H.>` |
| Subscribe | `fabric.root.H.>`, `$KV.STATE_H.>`, `_INBOX.H.>` |
| Shared subscribe | `fabric.fleet.work.>`, `fabric.fleet.presence.>`, `fabric.hub.delivery.H.>` |
| Own stream APIs (publish) | `STREAM.INFO`, `STREAM.MSG.GET`, `CONSUMER.INFO`, `CONSUMER.MSG.NEXT`, each under `$JS.fleet.API` or native core alias `$JS.API` with **exact** stream `ROOT_H` or `KV_STATE_H` |
| Own consumer ACKs (publish) | `$JS.ACK.ROOT_H.>`, `$JS.ACK.KV_STATE_H.>`, and their domain-prefixed `$JS.ACK.fleet.*.<stream>.>` forms |

No host has another host's root/KV/inbox, stream administration, consumer creation,
`$SYS`, shared-topic impersonation (`work.otherhost`), or hub-control publication.
There is no `>` allow for a host. Native SDKs must configure a host-prefixed inbox;
the default random `_INBOX.<random>` is deliberately not authorized.

**Consumer provisioning is an ops operation.** A host-created push consumer can
choose an arbitrary `deliver_subject` and inject messages into another principal's
subject. Hosts therefore cannot call either consumer-create API, even for their
own stream. Ops provisions pull-only, R3, explicit-ACK durable consumers and audits
stream/filter/durable ownership. KV watchers must use such a provisioned consumer;
unmodified SDKs that dynamically create watchers are not yet adapter-conformant.
Hosts cannot subscribe to `$JS.API.>` or use direct gets: `allow_direct: false`.

The hub `fabric.hub` identity is delivered **only on Ryzen 1**, never a leaf. It
publishes/subscribes `fabric.hub.>`, `fabric.fleet.>` and `_INBOX.hub.>`; this includes
`fabric.hub.control.>` (hub-only) and addressed `fabric.hub.delivery.H.>` (hub
publishes, only H may consume). Root/KV storage operations remain the adapter's
per-host principals; the hub certificate does not get unrestricted root access.
Cores remap domain-qualified APIs to `$JS.API` **before** the user ACL check;
leaves require the domain alias. Both host aliases remain identically scoped.
The administrative `fabric.ops` identity publishes only `$JS.fleet.API.>`,
`$JS.API.>`, `$JS.ACK.>` and `_INBOX.ops.>` and subscribes only `_INBOX.ops.>`. It is not a
second traffic/hub principal, but API administration is powerful: ops can create
consumers or restore streams and is a trusted maintenance credential. Never
attach it to an agent, gateway or leaf. System and ops certificates stay under
owner-controlled maintenance delivery, not generic host application profiles.

## Payload and retention budget

The task reports the largest real state value at about **100 KB** and event at
about **64 KB**. No live Fabric values are read. The smoke uses conservative
102,400-byte state and 65,536-byte event bodies. `max_payload = 262144` (256 KiB)
on **every server and stream** leaves room for JSON/base64 (100 KiB -> about
136.5 KiB), subject-independent envelopes and headers. The adapter must reject an
encoded payload plus headers above this limit; do not silently raise it for a
pathological state. NATS' payload limit is not an event-format validation limit.

| Resource | Bound |
|---|---|
| Core JetStream server | memory 512 MiB, file 30 GiB, `sync_interval: always` |
| FABRIC account | memory 256 MiB, file 20 GiB, 64 streams, 256 consumers |
| Per-host root stream | file, R3, 256 MiB, 100,000 messages, 7-day retention, 16 consumers |
| Per-host state KV stream | file, R3, 128 MiB, 10 revisions/key, no age expiry, 16 consumers |
| FLEET stream | file, R3, 1 GiB, 500,000 messages, 7-day retention, 64 consumers |
| Network | 8 MiB max pending/client, core 2,048 clients, leaf 512 clients; 5 s write deadline |

Nine generated host slots (including optional `epyc1`, not proven enrolled locally)
plus FLEET use 19 streams and a logical max about 4.375 GiB. R3 makes roughly
13.125 GiB aggregate data before Raft/index overhead. Account/server quota
accounting is not a disk reservation; provision **at least 40 GiB free per core**
and alert at 60/75/85% disk, quota, and stream occupancy. Persistent `discard: new`
fails writes instead of evicting old state under byte exhaustion; root/FLEET age
retention is intentional. KV history trims older revisions per key; current state
has no time expiry. No memory-backed authoritative streams are proposed.

Store roots are independent local filesystems at
`~/.local/state/smarty/nats/<host>/jetstream`. Never share or live-copy a Raft store.
Require working fsync and no volatile write-cache lie. R3 plus `sync_interval:
always` is a quorum durability policy; loopback SIGKILL is not a power-loss proof,
and it says nothing about three independent disks or tailnet failure domains.
Duplicate windows (120 seconds for event streams) require stable message IDs and
do not replace permanent Fabric dedupe or prove exactly-once side effects.

Generated production paths are Linux source examples. Re-render on the target or
with its native `--tls-dir` and `--store-root` (macOS home/Windows absolute paths),
then select only that host's config. Native hostd execution/admission is a gate.

## Upstream contracts (reference only; no upstream contact)

- [TLS and certificate identity mapping](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/tls)
- [Subject permissions](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/authorization)
- [JetStream clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering)
- [JetStream domains and leaf nodes](https://docs.nats.io/running-a-nats-service/configuration/leafnodes/jetstream_leafnodes)
- [Official v2.14.7 release](https://github.com/nats-io/nats-server/releases/tag/v2.14.7)

The parser and live protocol smoke, not prose alone, verify the selected version's
actual behavior. Domain-aware KV writes use `$JS.fleet.API.$KV.STATE_H.<key>` across
a disabled-JetStream leaf; reads use the exact-domain stream API. Do not turn on
leaf JetStream merely to make a default-domain SDK work.
