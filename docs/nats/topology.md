# Topology and operations boundary

## Proposed R3 cluster

```
Fabric control plane (unchanged): Ryzen 1
     local mTLS client :4222
              |
  core-ryzen1 <------> core-ryzen2 <------> core-ryzen4
      |   <------------------------------>   |
      +------- fabric-r3 / domain fleet ------+
      each: local persistent JetStream, R3 streams/KV, fsync always
              ^          ^          ^
              | one leaf connection; three failover URLs
          Tailscale + mutual TLS :7422
              |
  leaves: ryzen3, ryzen5, m4, i9, m5 (+ conditional fifth-host slot)
      each: local mTLS clients :4222; JetStream disabled
```

Ryzen 2 and Ryzen 4 are **proposed** second/third quorum peers: both are named
agent/lane targets in the locally present
`smarty-dev/setup/factory/work-hosts.json`. Paul/hosts-lead approve the pair and independent power/storage/network failure domains before
admission. This is not authority to move coordinator sessions or the application
control plane. The JetStream Raft metadata/stream leader may be any core; a Raft
leader election does not relocate the Fabric hub, credentials custodian or owner.

| Host | NATS role | Core tailnet endpoint (dated local inventory, reverify before install) |
|---|---|---|
| ryzen1 | R3 core + Fabric hub/control plane | 100.78.221.70 |
| ryzen2 | R3 core + local agent clients | 100.103.29.1 |
| ryzen4 | R3 core + local agent clients | 100.105.145.68 |
| ryzen3 | leaf | outgoing only; no new public DNS assumption |
| ryzen5 | leaf | outgoing only; endpoint approval does not need an invented IP |
| epyc1 | conditional leaf example, **not proven enrolled** | fifth-host approval/inventory gate; do not install or create credentials based on this template |
| m4 / i9 | native macOS / Windows leaf | native hostd lifecycle/admission gate |
| m5 | owner's client Mac leaf | **design only; explicit Paul install approval required** |

Inventory authority: [smarty-dev#2237](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/2237),
[#3752](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/3752),
[#4382](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/4382),
[#1867](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/1867);
local `smarty-dev/docs/infrastructure.md` and `docs/fleet-host-naming.md` read
2026-10-09. The supplied task explicitly pins Ryzen 1 as control plane. No remote
inventory or network activation was performed. Add any newly admitted fleet host
as a leaf with its own certificate identities and bounded host namespace; never
reuse another host's identity.

## Network and storage

- Cores route to both other cores on tailnet TCP 6222. They accept leaves on their
  **tailnet IP only**, TCP 7422. No public listener, no wildcard bind, no gateway,
  no supercluster. Transport and route TLS have separate trust roots.
- Agent clients use their local server on `127.0.0.1:4222` with mTLS. Monitoring is
  `127.0.0.1:8222` only, unauthenticated/read-only: do not proxy or expose it.
- Leaves initiate one outbound connection with all three core URLs as a failover
  pool; do not open three simultaneous account links (loops/duplicate delivery).
  Leaves expose neither routes nor an inbound leaf listener.
- Tailnet ACL proposal: core-pair TCP 6222 only; admitted leaf source -> each core
  TCP 7422 only. No fleet-wide 4222/8222 grant and no firewall changes in this task.
  Tailscale is the network boundary, **not** a replacement for mTLS/user ACLs.
- Each core keeps an independent local disk `~/.local/state/smarty/nats/<host>/jetstream`.
  Never share a directory, NFS/CIFS/ZFS network export, or copy a live Raft store.
  Leaves have no authoritative JetStream store, cache replay or disconnected queue.

## Five existing mesh-bridge transports

The task calls for replacing **five** mesh-bridge transports. The locally present
work-host map (checkout and factory/current) names only `ryzen2`, `ryzen3`,
`ryzen4`, `ryzen5`, not five. This is an inventory discrepancy, not permission to
invent the fifth host/unit. NATS replaces all five recorded transport relationships
after the owner reconciles that slot. Ryzen 2/4 use their local cores; Ryzen 3/5
use leaves. The retained `epyc1` leaf/config/credential slot is a **conditional
example from the interrupted run**, not observed enrollment. If the fifth host
has another name, change its generated identity/subjects after owner approval.
Macs/Windows are additional fleet leaves, not proof of another bridge unit.
Do not create redundant leaves on core hosts just to make a count of five.

The locally present service inventory explicitly names
`smarty-mesh-bridge@ryzen4.service`; it does not prove the other four actual IDs.
Other targets may have a template instance, legacy wrapper or hostd child. Capture the actual ID, owner, cursor and pinned
forced-command slot for **all five** before cutover. Do not infer names or disable
services by a wildcard. After the adapter migration fence and acceptance, the owner
stops only those five recorded bridges. Keep their cursor/known-host/release files
and forced-command slots through the rollback window. No bridge is stopped here.

Leaf transport alone does not reproduce v1 bridge authority checks. U1/U3 must
preserve intrinsic owner identity, recipient binding, leases, custody/CAS, command
ACKs, canonical root IDs, reservation collision rules, event dedupe and unknown
outcomes; see [pi-fabric mesh-bridge](../mesh-bridge.md) and
[smarty-dev#2045](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/2045).
A host-authenticated subject prefix is not proof of a particular session's owner.
No raw event claim may acquire hub authority.

## Failure model

Three cores / R3 need two voting members. One lost node or one isolated minority
is tolerated; two lost cores lose metadata and stream write availability. With
one core down, do not restart or replace another. Leaves are **not** Raft voters.
A lost leaf/hub path fails the operation within a bounded deadline; it must never
silently queue commands locally or fall back to a writable SQLite fork.

`sync_interval: always` applies to file-backed JetStream (including Raft writes).
An acknowledged publish has a quorum durability contract, not exactly-once
application execution. Disk/controller fsync honesty, sudden power-loss behavior,
WAN latency and Tailscale partition behavior are not proved by a loopback test.
Replication is not backup. No control-plane failover orchestration is added.
