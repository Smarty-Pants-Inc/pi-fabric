# Mixed-release bridge release gate

Before a Fabric rollout, run the canary against the **built candidate** and every release that will remain connected to the hub. A static compatibility audit is not a substitute. The canary runs locally; it does not contact fleet hosts or use SSH.

From this repository, with Linux and Node 24 or newer:

```sh
node scripts/mixed-release-canary.mjs --new "$NEW" --old "$OLD_A" --old "$OLD_B" --receipt "$RECEIPT"
```

The command must run under `smarty-test-env` (or another short private `TMPDIR`): production AF_UNIX relay sockets have a pathname limit, and setup refuses an overlong scratch path. If using `smarty-test-env`, invoke it with a wrapper that copies the receipt and its raw scratch evidence before EXIT cleanup; retain the original receipt bytes and a SHA-256 mapping to the archived files.

`NEW`, `OLD_A`, and `OLD_B` are absolute paths to built pi-fabric release trees containing `src/mesh/bridge.ts`, `dist/`, `bin/mesh-bridge`, and their dependencies. Source and compiled artifacts must agree about relay capability; the receipt cites both. Repeat `--old` for additional releases. Each tree needs an exact Git HEAD or a full-SHA artifact-directory/build identity. `RECEIPT` is the output JSON path. Do not point these variables at mesh state directories. Installed release trees are inputs only: the canary must not modify them.

A release owner may proceed only after the **full gate** exits zero and its JSON receipt reports PASS for every required check. Keep the receipt and the scratch evidence it names with the release record. A baseline control is diagnostic evidence, never rollout permission. Pre-existing unsupported relay pairs are recorded explicitly with the release that lacks the capability; they cannot waive uptime, direct hub delivery, or legacy roots reaching newer spokes.

## What crosses the real boundary

- One private scratch directory contains a SQLite hub and a SQLite spoke for every supplied release, including NEW. Runtime sockets, bridge cursors, logs, and registration state stay under that directory.
- Every hub-side runner uses NEW's `bin/mesh-bridge run`. Its `-- COMMAND` starts the corresponding spoke release's real `bin/mesh-bridge agent` over stdio.
- Each release's compiled participant directory registers its own root and host with a lease. The receipt records the writer's actual token behavior; no incarnation fields are stripped to manufacture a legacy writer.
- A real token-bearing registration is first mirrored to NEW. The same root subsequently arrives tokenless from a genuine historical writer, through the hub. Persisted token history must be observed before this transition; otherwise the gate has not reproduced the incident.
- Every measured bridge, including a pre-relay release's link, must link and remain up continuously for at least 60 seconds, without a restart. Every spoke's roots must appear at the hub and every relay-capable spoke. Tokenless legacy roots must reach NEW through the hub without refusal.
- Hub-to-spoke and spoke-to-hub `followUp` delivery is required for every release. Every directed pair of different spokes whose agent releases both support relay also sends a `followUp` through the production control plane. Destination handler observations and correlated ACKs must establish exactly-once delivery. Missing required pairs, duplicate delivery, refused ACKs, an early exit, or missing history fail the gate.
- A pair involving a pre-relay release is recorded as `unsupported (pre-existing)`, identifying that release and citing the source file and line that establishes its capability. The canary does not substitute a newer agent or infer support from version ordering. Unsupported pairs must not crash or wedge a link.
- The stale-history case must be named in warning evidence without terminating the fixed bridge. The receipt distinguishes a canary diagnostic from a production bridge log message.

The scratch topology is disposable; fleet mesh directories are never inputs or destinations. Retain its evidence until the release decision is recorded, then remove only the private directory that invocation created.

## Historical regression

Refs Smarty-Pants-Inc/smarty-dev#14111 and Refs Smarty-Pants-Inc/smarty-dev#14279.

The regression target is `9c644fd2ab93f3fed3c0ee51e28e96f70a2a7bf5`. Run the same full gate with that release as NEW: it must exit nonzero because tokenless relayed presence conflicts with persisted incarnation authority. A setup failure, missing build, or unrelated unsupported protocol is not proof of that regression. Repeat with the strip-relayed-guard candidate and retain both receipts.

Fresh registration behavior must be measured rather than inferred from a host's installed version. In local compiled-release probes, `c42f7f34125abe2ebbe660758253c9ec4d7765dd` writes a token, while `41a73f8588e5351d9409736f725fee218810b78c` writes none. The latter also needs lock protocol 1 or 2 and predates cross-spoke relay. Its unsupported cross-spoke pairs are not a new-release regression; its direct hub delivery, uninterrupted link, and tokenless roots reaching newer spokes remain mandatory. The canary reports these differences instead of manufacturing tokenless c42f registrations or claiming unsupported routes worked.
