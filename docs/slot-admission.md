# Slot admission CLI

`/fabric import` keeps ordinary global-template imports unchanged. A protected
materialization must carry the exact claim revision and a stable retry key:

```text
/fabric import TEMPLATE [as NAME] \
  --slot-claim CLAIM_KEY \
  --slot-version POSITIVE_SAFE_INTEGER \
  --idempotency-key RETRY_KEY
```

`--slot-claim` and `--slot-version` are all-or-none. The revision must be a
positive safe integer. Claim and retry keys must be nonempty, stable,
non-whitespace keys of at most 256 characters. The CLI does not infer,
refresh, steal, or persist a claim token in the reusable global template.

Native admission belongs to the actor boundary, not this parser. An exact
unchanged retry must use the same template contents, name, claim key/revision,
and idempotency key. The CLI never automatically retries or compensates a
refusal or uncertainty by deleting claims, admissions, or incumbent actors.
After commit/projection or response uncertainty, retain that identity and ask
the native owner to reconcile; do not refresh the token or create a successor.

The supported enforced deployment is a fresh local SQLite authority under a
root-owned trusted policy/bootstrap. Existing SQLite authorities and NATS
remain allowed but **unenforced**: native status reports
`enforcement: 'unenforced'`, reason `existing-authority` or `nats-backend`, and
`gap: 'smarty-dev#7991'`. Those modes retain the birth-admission gap; passing
these flags does not enable enforcement. Existing-authority ledger
import/enforcement cutover is not supported here (smarty-dev#13036); the NATS
remainder is smarty-dev#13035. Native binding details belong to the reviewed
#7991 binding design and trusted bootstrap contract, not to request syntax.

## Native fresh bootstrap (Linux)

The installed native module is `dist/actors/slot-authority.js`; it exports
`initializeFreshSlotAuthority`, `slotPhysicalHostIdentity`, and
`slotRoutesDigest`. It is not a guest API or an enforcement toggle.

Before any runtime attaches, the installer atomically publishes a `slotAdmission`
plan in root-owned `/etc/smarty/fabric-policy.json`: protocol
`slot-admission-v1`, reviewed `routes`, and an `authorities` binding with canonical
`meshRoot`, `registryRoots`, authenticated Main `rootIds`, a non-reused
`authorityGeneration`, `hubRootId`, `physicalHostIdentity`, and `routesDigest`.
The plan has no `databaseFileIdentity`. All named authority/registry paths must be
fresh and empty. Call `initializeFreshSlotAuthority(meshRoot)` as the runtime uid,
retain its returned binding, then have the installer atomically publish that
complete binding before business attach. Every launcher/registry writer must use
the protocol-capable release from that first attach. An existing file or a crash
orphan is refused, never inferred fresh or automatically imported.

Resident creation uses its existing registration/request fence and the same
native receiver admission seam. Origin-to-resident native attestation was dropped
by the org ruling of 2026-10-15 04:45Z; it is not a deferred delivery gate.
