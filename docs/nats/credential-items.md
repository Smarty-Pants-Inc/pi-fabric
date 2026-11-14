# Credential item approval list (no store access performed)

Refs smarty-dev#6477. **Paul/hosts-lead approval is required before creation,
delivery or fleet activation.** This is a list of stable logical item IDs to
create, not invented 1Password provider IDs. After authorized creation, record the
actual item IDs in private profiles/host receipts. None have been created or read
by this task. Local guides are `smarty-dev/docs/credentials.md` and
`.agents/skills/smarty-credentials/SKILL.md`; use the existing **Smarty Development**
vault, not a new vault or secret loader. Its immutable vault ID and all
provider-issued item IDs belong only in private profiles/host receipts, not Git.

One canonical item per credential, no omnibus bundle. Titles follow
`NATS — <purpose> (<scope>)`. Tags: `service/nats`, `use/service`, `org/smarty`,
and `host/<host>` for host material. Notes record consumer, SAN/EKU, scope,
expiration, rotation owner, issue and approved delivery boundary.

## Exact proposed logical IDs

Trust material / signing authority (custody must be approved, not daemon-loaded):

- `nats-ca-transport` — NATS — transport signing authority (fleet); separate CA
  signing key and public certificate/chain. Never deliver signing key to hosts.
- `nats-ca-route` — NATS — route signing authority (R3 peers); separate CA from
  ordinary application transport. Never deliver signing key to hosts.

Server transport certificates (one item each; certificate/chain + private key):

- `nats-server-ryzen1`
- `nats-server-ryzen2`
- `nats-server-ryzen4`
- `nats-server-ryzen3`
- `nats-server-ryzen5`
- `nats-server-m4`
- `nats-server-i9`
- `nats-server-m5` — explicit Paul device-install consent in addition to this list
- `nats-server-epyc1` — **conditional** fifth-host slot; no local enrollment evidence

Server EKU `serverAuth`; SANs include local `127.0.0.1` and each core's approved
Tailscale IP/DNS. Leaves need only local listener names. No public wildcard cert.

Route peer credentials (EKU `serverAuth,clientAuth`, route CA only):

- `nats-route-ryzen1`
- `nats-route-ryzen2`
- `nats-route-ryzen4`

SANs cover each route's approved tailnet address. Rendered names are
`route-<host>.pem/.key`; do not reuse an application key for routes.

Per-host application credentials (EKU `clientAuth`, exact single identity DNS SAN):

- `nats-client-ryzen1` — SAN `fabric.ryzen1`
- `nats-client-ryzen2` — SAN `fabric.ryzen2`
- `nats-client-ryzen4` — SAN `fabric.ryzen4`
- `nats-client-ryzen3` — SAN `fabric.ryzen3`
- `nats-client-ryzen5` — SAN `fabric.ryzen5`
- `nats-client-m4` — SAN `fabric.m4`
- `nats-client-i9` — SAN `fabric.i9`
- `nats-client-m5` — SAN `fabric.m5`
- `nats-client-epyc1` — SAN `fabric.epyc1`, conditional slot only

Outgoing leaf credentials (EKU `clientAuth`, core-side bounded host ACL):

- `nats-leaf-ryzen3` — SAN `leaf.ryzen3`
- `nats-leaf-ryzen5` — SAN `leaf.ryzen5`
- `nats-leaf-m4` — SAN `leaf.m4`
- `nats-leaf-i9` — SAN `leaf.i9`
- `nats-leaf-m5` — SAN `leaf.m5`
- `nats-leaf-epyc1` — SAN `leaf.epyc1`, conditional slot only

Privileged application/monitor identities (distinct items, transport CA):

- `nats-client-hub-ryzen1` — SAN `fabric.hub`; only Ryzen 1 Fabric coordinator
- `nats-client-ops` — SAN `fabric.ops`; maintenance/API administration only
- `nats-client-system` — SAN `fabric.sys`; system checks, not application traffic

## Delivery and rotation gate

Use existing approved scoped credentials delivery, ID-based private references,
and native service-account enrollment. Hosts-lead must confirm each host's actual
store enrollment and consumer boundary before selecting profiles. Do not add a
per-project loader, broad environment export, credential cache daemon, or borrow
Ryzen 1's account for an unenrolled host. A service account's vault scope does not
itself prove host principal isolation.

Files for nats-server are an unavoidable **owner-approved scoped service runtime**
materialization, not committed config: private TLS directory mode 0700, keys mode
0600 (Windows equivalent ACL), atomic writes, no stdout values, no key in argv,
no global environment. Public trust chains may be shared; host keys may not.
Exactly one host's server/client/leaf keys are delivered there. Route keys go to
cores only; hub goes to Ryzen 1 only; ops/system are separately scoped. CA signing
keys stay with the approved signing custodian. The task supplies no new delivery
implementation and no private-key bytes. If the existing mechanism cannot stage
files securely, activation is BLOCKED until its owner provides a reviewed route.

Propose 30-day leaf/client/server lifetimes, seven-day expiry alarm, owner-led
rotation. Rotate one identity/node at a time; verify new mTLS mapping and ACL
rejection, bounded reload/restart, unchanged quorum, then retire the old item
revision. Changing trust roots requires overlap of old/new public trust on every
consumer before leaf/route cert rotation. SAN-name mapping is not an individual
certificate revocation list: deleting an item does not revoke a still-valid
certificate. Revocation/short-lifetime handling and same-UID custody are explicit
security-pass/owner gates; do not claim them proved by synthetic smoke.

Test certificate/key generation lives only in `smoke.py` and `$TMPDIR`; neither
test PEMs, private keys, CSRs nor actual provider IDs go in the repository/bundle.
