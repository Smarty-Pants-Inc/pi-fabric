# NATS cluster/ops proposal — U2

Refs [smarty-dev#6477](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/6477).
**Status: design + locally tested source carrier, not a fleet activation.**
Ryzen 1 stays the Fabric control plane. This change does not implement the Fabric
NATS backend, flip a mesh setting, enroll a credential, or install anything.

Read in order:

1. [Topology, failure model and five bridge replacements](topology.md)
2. [Accounts, host permissions, limits and storage](security-and-storage.md)
3. [Credential item approval list](credential-items.md) — logical IDs only;
   provider-issued IDs must be recorded privately after authorized creation
4. [Hostd service registration proposal](services-proposal.md)
5. [Bring-up, health, snapshots, replacement and SQLite rollback](runbook.md)
6. [Acceptance ledger and local evidence](acceptance.md)

## Executable artifacts

- `render.py`: one stdlib renderer for both production and smoke. The smoke changes
  only addresses/ports, TLS/store paths and the number of leafs. Account users,
  subject ACLs, JetStream domain, `sync_interval: always`, and limits are identical.
- `configs/core-{ryzen1,ryzen2,ryzen4}.conf`: concrete proposed core configs.
- `configs/leaf-{ryzen3,ryzen5,m4,i9,m5}.conf`: fleet leaf configs, no JetStream.
  `leaf-epyc1.conf` is a conditional fifth-host example only; local inventory has
  four work-host targets while the task requires replacing five bridges.
- `configs/streams.json`: R3 root streams, per-host state KV streams, shared stream.
- `hostd/*`: strict child specs and registration fragments; no new enabled unit.
- `smoke.py`: foreground supervisor; test-only certificates and keys stay in
  `$TMPDIR`, no real Fabric values or credentials are accessed. Every server is
  stopped and waited for in `finally`; ephemeral store/key dirs are removed.
- `check.py`: deterministic generation, ACL, limit and strict child-spec checks.
- `test_smoke.py`: stdlib regressions for total RPC/PONG deadlines despite PING
  traffic, explicit no-responder failure, and cancellation escaping retry loops.
  Live smoke also rejects foreign host ACLs at the core independently of local
  leaf ACLs, transport-signed application certs on route listeners, and declared
  payloads above 256 KiB. Read-only failover probes use fresh bounded connections;
  state/event writes and the metadata mutation are never implicitly retried.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/nats/render.py --out docs/nats/configs
PYTHONDONTWRITEBYTECODE=1 python3 docs/nats/check.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s docs/nats -p test_smoke.py
# Pass the official SHA256SUMS-verified binary, not a global unpinned installation:
PYTHONDONTWRITEBYTECODE=1 python3 docs/nats/smoke.py \
  --server "$TMPDIR/nats-release/nats-server-v2.14.7-linux-amd64/nats-server" \
  --out "$TASK_OUT/smoke" --base-port 22000
```

Only the local smoke is exercised here. Tailscale reachability, fleet disks,
service admission/reboot, certificate delivery, backup restore to another host,
Fabric adapter conformance, migration fences and load acceptance remain gates.
The task host reports `qs32729` (Linux x86-64); it is not asserted to be Ryzen 1.
No SSH, credentials-store access, GitHub API, push or fleet install was used.
Only public official NATS release assets were downloaded, as requested. Standing
decisions are taken from the provided task and locally present Smarty
service/credential/infrastructure docs, cited in the relevant sections.
