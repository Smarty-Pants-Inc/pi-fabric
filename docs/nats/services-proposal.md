# Hostd registration proposal (not activated)

The locally present `smarty-dev/docs/services.md` is authoritative: new background
glue goes into one Smarty host daemon, not another enabled systemd unit or timer.
Its existing `setup/factory/hostd-children.md` and `org-keeper.md` describe factory
mode `factory.services`, startup-only admission, detached/adopted scopes and
rollback. The example org-keeper unit confirms numeric `RestartSec` and
`TimeoutStopSec`. This task does not change the org's service inventory or install
a factory release; the following is the inventory/spec carrier for that owner.

## Proposed inventory entries for smarty-dev/docs/services.md

| Proposed child | Hosts | Code / admission / operations | Purpose |
|---|---|---|---|
| `nats-core` | Ryzen 1, Ryzen 2, Ryzen 4 | Fabric / hosts-lead with net-lead / hosts-lead | One official pinned NATS server, R3, local file store, TLS routes/leaf listeners, Ryzen 1 application control plane unchanged |
| `nats-leaf` | Ryzen 3, Ryzen 5, m4, i9, m5; approved fifth-host slot | Fabric / native hostd owner + hosts-lead / hosts-lead; Paul authorizes m5 | One local mTLS client endpoint and one outgoing failover leaf link; JetStream disabled |

Status: **proposed, not installed/registered/enabled**. The owner must merge these
entries into smarty-dev's canonical inventory before adding a background child.
Exactly **one** role per host. Do not add a leaf beside a core to increase a count.
Do not create a new timer or supervisor process.

`hostd/nats-{core,leaf}-child.json` is the registration object. The corresponding
`smarty-nats-*.service` is a **hostd strict child specification**, not an independent
unit: no `[Install]`, dependencies, unsupported hardening keys or shell wrapper.
The reviewed factory release must carry the paths referenced by the registration
(`docs/nats/hostd/...`); if paths move at integration, update the registration and
verify exact installed bytes before admission.

`ExecStart` resolves `%h`, launches the pinned `nats/current/nats-server` directly
with the selected host's `~/.config/smarty/nats/server.conf`. Versioned release
selection belongs to the existing install owner, not this document. Production
config templates contain no private key data but point to approved scoped runtime
TLS files; do not enable the child without the credentials/disk/ACL gate.

## Lifecycle and checks

- `Restart=always`, `RestartSec=5` (plain seconds), `TimeoutStopSec=40`.
  Hostd owns process group/scope and crash-loop backoff/status. No second restart
  loop. Capture the installed hostd version's actual backoff limit/behavior in the
  admission receipt; only the initial five-second policy is specified here.
- Planned shutdown sends SIGTERM, waits for NATS store flush and at most 40 s;
  forced termination is an alarm and recovery event. Lame duck is available to
  the operator (30 s duration, 10 s grace), not automatically asserted by the unit.
- Existing factory checks/schedules should observe child state, TLS expiry,
  disk/quota, healthz, meta/stream/consumer leader and current replica lag. A
  running PID or open port alone is not readiness. Do not add a new health timer.
- Do not restart more than one core concurrently. Drop/restart of a registered
  child is distinct from adding a child; there is **no add-child CLI** in the
  documented factory mode.

## Owner admission/rollback steps

1. Land the reviewed source carrier, obtain security/adapter/failure-domain approval,
   install the exact reviewed factory/NATS artifacts through existing owner routes,
   and record previous release/config/child PIDs privately. Nothing here executes
   those steps.
2. Verify strict parser acceptance on the installed hostd version. Current source
   checks confirm supported example-shape only: that is **not** an actual native
   hostd parser/reboot test. Linux syntax is not a proof for macOS or Windows.
3. Stage native absolute TLS/store paths and exactly one host config, then
   `nats-server -t -c <selected-config>`. Check mode, SANs, addresses and free disk.
4. Atomically merge only the approved role registration into existing
   `factory.services`, preserving other keys/children, mode 0600. The owner runs
   the documented hostd startup admission/restart, with detach/adopt, **never
   drain**. Confirm every unrelated child keeps its PID and only this role appears.
5. Confirm process executable/release/PID, listeners, full NATS/JetStream health,
   successful host-scoped roundtrip and denied foreign subjects. Prove hostd
   restart and host reboot recovery through the native host owner's route.
6. To disable: owner `smarty-hostd drop nats-core` (or `nats-leaf`), remove only that
   durable registration, owner-restart/adopt hostd, confirm no role child/listener
   and unchanged unrelated PIDs. Keep stores, bridge cursors and credential
   receipts. Application rollback follows [runbook](runbook.md), not this process
   stop alone. Binary rollback is one core at a time only after store-format
   compatibility is established; never run an older incompatible binary on a
   newer store or delete data to force startup.

m5 remains design-only until Paul's explicit install consent. The optional fifth
host remains excluded from admission until its identity/inventory is reconciled.
