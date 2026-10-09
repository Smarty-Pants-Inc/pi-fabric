# U2 acceptance boundary and local checks

Refs [smarty-dev#6477](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/6477).
This is a **design/config carrier with local transport proof**, not an activated
fleet, finished Fabric backend or production durability claim.

| Requested check | Source and proof |
|---|---|
| Ryzen 1 control plane; 3-peer R3 + other hosts leaf | `topology.md`, concrete core/leaf configs; Ryzen 2/4 are approval candidates; fifth bridge target unresolved in dated local inventory |
| `sync_interval: always`, file stores, payload/quota limits | `render.py`, `configs/streams.json`, `security-and-storage.md`; same renderer produces local smoke configs |
| TLS, accounts, per-host ACLs and hub-only traffic | same generated ACLs + real mTLS smoke, own-host put/get/ACK, foreign/hub/system and host-created-consumer rejection; route CA separate |
| Existing credentials store, approval item IDs, no values | `credential-items.md` exact logical IDs; provider IDs/delivery stay private; no store access or test PEM/key tracked |
| Server/leaf restart policy | two `hostd/*-child.json` registrations and strict child-spec `.service` carriers; compare approved local org-keeper shape; no registration/install claimed |
| Bring-up/health/backup/replacement/SQLite rollback | `runbook.md`; no invented config flag or automatic writable fallback; snapshots/isolated restore and inverse migrator are explicit operator/backend gates |
| Local 3-core + 2-leaf proof | official v2.14.7, 102400-byte KV + 65536-byte event, R3 consumer ACK, metadata leader SIGKILL, surviving data/meta quorum, post-loss leaf writes/gets and R3 consumer ACK; all children reaped, scratch fixtures removed |
| Source/artifact checks | `check.py` deterministic generation, complete host ACL matrix, store/TLS/child shape, local doc links, no TLS material. Fresh `bun run build` attempted: worktree lacks dependencies; scratch retry with existing sibling dependencies passed proof-artifact/typecheck, then BLOCKED by missing `cc`. No tools/dependencies installed. |

## Reproduce the bounded local test

Follow the public release integrity instructions in [runbook](runbook.md), then:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/nats/render.py --out docs/nats/configs
PYTHONDONTWRITEBYTECODE=1 python3 docs/nats/check.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/nats/smoke.py \
  --server "$TMPDIR/nats-release/nats-server-v2.14.7-linux-amd64/nats-server" \
  --out "$TASK_OUT/smoke" --base-port 24800
bun run build
```

No pytest/package/service/CLI installation is needed. The smoke's tiny stdlib
protocol client is test-only, not a new production NATS client. Use an unused
port range; the supervisor preflights every listener and never terminates an
existing listener. TLS keys, CSRs, test certs and Raft stores stay under a fresh
private `$TMPDIR` subdirectory and are removed after all five child processes are
waited for. Retained logs/configs contain paths and synthetic body sizes, not TLS
material or real Fabric values. Ordinary SIGTERM/SIGINT cancellation goes through
`finally` cleanup. The three servers model three peers on **one machine**, not
independent fleet failure domains.

## Gates before fleet admission/cutover

- Paul/hosts-lead approve quorum peers, tailnet policy, disk/fsync/failure domains,
  item list, credential custody/revocation and the fifth bridge target/actual IDs.
  `epyc1` is conditional; do not treat a template as inventory.
- Native hostd strict-parser/adoption/restart/reboot proof on Linux/macOS/Windows;
  Paul's m5 install consent; exact reviewed installed artifact/config hashes.
- Real Tailscale disconnect/partition/disk failure, sustained R3 fsync latency,
  full fleet stream/KV/consumer placement and payload/headroom acceptance.
- Off-node encrypted snapshots and isolated R3 restore; cross-stream consistent
  authority epoch and dedupe/ACK recovery. No restore proof is claimed locally.
- U1/U3 Fabric adapter conformance, own inbox/domain/pull-consumer behavior,
  intrinsic owner/recipient/custody/CAS/lease and side-effect dedupe; tested
  forward/inverse migration and writer fences. No dual-write backend selection.

Task artifacts contain the exact smoke result/logs, official **last** SHA256SUMS
and archive integrity check, source/check/build output, PR draft and commit/bundle
receipt. The interrupted run's old timeout is not used as a success claim; the
continuation reruns and retains its own evidence. Actual outcomes are in the
handoff `result.md`, not inferred from this reproduction recipe.
