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
- The historical tokenless spoke has two separate genuine roots, each registered by its own compiled `ParticipantDirectory` and control worker on that **same spoke mesh**. Its primary root is fresh on NEW (raw host, lease, participant and directory-owner readbacks must all be absent before the measured NEW link). That primary remains in the normal topology, presence and direct-control plan. It must reach NEW through the hub, with `remoteHost` equal to the historical spoke name, within the 25-second presence bound, before and after the hold.
- A second, diagnostic history sentinel is not an extra topology node or a replacement for the primary. A candidate-native registrar at the hub first registers that sentinel's ID with a real token; a real seed bridge mirrors it to NEW. The owned seed link is hard-stopped, the seed registrar closes, and the actual NEW host/lease/participant mirror is retained. The historical spoke's native tokenless sentinel then reaches the hub through its real agent. The first measured NEW link therefore sees **both** the stale sentinel and the independent fresh primary.
- Every measured bridge, including a pre-relay release's link, must remain up continuously for at least 60 seconds, with zero restarts. Every normal primary root must appear at the hub and every relay-capable spoke. The historied sentinel intentionally must **not** rebind on NEW: a tokenless downgrade or replay cannot verify token-bearing history. NEW must preserve the previous mirror's token, exact host/lease/participant values and raw owner/epoch; lease staleness itself is allowed.
- Hub-to-spoke and spoke-to-hub `followUp` delivery is required for every release. Every directed pair of different spokes whose agent releases both support relay also sends a `followUp` through the production control plane. Destination handler observations and correlated ACKs must establish exactly-once delivery. Missing required pairs, duplicate delivery, refused ACKs, an early exit, or missing history fail the gate.
- A pair involving a pre-relay release is recorded as `unsupported (pre-existing)`, identifying that release and citing the source file and line that establishes its capability. The canary does not substitute a newer agent or infer support from version ordering. Unsupported pairs must not crash or wedge a link.
- The sentinel's stale-history case must produce root-specific **production** warning evidence without terminating the link. The receipt names the diagnostic `STALE_INCARNATION_HISTORY`, keeps the raw warning lines and reports their occurrence count. One named case does not mean production logs exactly once across repeated reconciliation passes; a harness-invented warning cannot pass.
- During the hold, NEW invokes its actual compiled `FabricControlPlane.request` (`followUp`) against the live historical sentinel. It uses NEW's observed directory owner, epoch and route, even though that is the retained hub mirror; it never forces the old-spoke route, replaces authority or bypasses fencing. The receipt records invocation, begin/end/elapsed time, intended target, actual owner, raw observed presence, and the actual return or error (`name`, `code`, `message` verbatim). The ordinary request timeout is 20 seconds plus up to 15 seconds ACK grace: the outcome must return within 35 seconds. The special worker RPC has 45 seconds of transport headroom, not a shorter public timeout.
- Case (b) passes only for delivered + matching authenticated destination handler and ACK, or a named visible **actual public request** refusal/error within that budget. A named public route/lease error such as `FabricBridgeStaleError` is a visible sender outcome, even if its verbatim message warns that delivery is unknown; that does not establish delivery. A generic ACK timeout, no result, harness/precheck error, a claimed send/ACK with no destination handler, duplicate sentinel delivery, or changed retained authority fails. The native old sentinel is read back live at the attempt and afterward; its separate final handler/command/ACK tally is retained even on refusal. It may be delivered at most once. These diagnostic tallies never waive or replace the normal all-pairs controls.

The scratch topology is disposable; fleet mesh directories are never inputs or destinations. Retain its evidence until the release decision is recorded, then remove only the private directory that invocation created.

## Capture retained authority without fleet content

Fresh registrars are not a substitute for the authority already held by a fleet mesh. The clean-mesh gate above missed the retained-authority failure reported in Refs Smarty-Pants-Inc/smarty-dev#14111. Same-root sibling tokens are one demonstrated fixture trigger, not yet a claim about the cause of a particular fleet stop. Do not infer tokenless input from the installed release: capture the actual authority and inspect the presence computed on each pass.

After independent safety review of the exact collector bytes, the release owner runs the standalone Node 24+ collector locally on each authorized source host. It needs no package installation. From the verified checkout root, on the hub:

```sh
OUTDIR=$(mktemp -d)
node scripts/fleet-authority-snapshot.mjs --mesh /home/paul/.local/share/smarty-dev/fabric-mesh/fleet --site hub --name dev1 --peer ryzen2 --cursor /home/paul/.local/state/mesh-bridge/ryzen2/cursor.json --cursor /home/paul/.local/state/mesh-bridge/intel1/cursor.json --output "$OUTDIR/hub.json"
```

On Ryzen 2, with the same reviewed CLI and library files:

```sh
OUTDIR=$(mktemp -d)
node scripts/fleet-authority-snapshot.mjs --mesh /home/paul/.local/share/smarty-dev/fabric-mesh/fleet --site work --name ryzen2 --peer dev1 --output "$OUTDIR/work.json"
```

Only the generated sanitized JSON files may be sent to the canary host. Never copy a mesh database, canonical state file, WAL/SHM, raw lease/participant files, events, actors, or cursor backups. The `fabric-release` files beside the cursors are not authority inputs. The Intel 1 hub cursor is an observation, not a substitute for Intel 1 work-side authority; these two captures cover the dev1–Ryzen 2 pairing only.

The allowlist projects host records, participant ownership joins, handoffs, liveness policy, separate participant/lease files, mirror observations, and explicitly named cursor fields. All incarnation/owner/process token values and superseded-token keys become stable `h:<32 hex>` SHA-256 prefixes. Canonical IDs and root joins remain exact; arbitrary display names become safe aliases. The field census is in `counts`, including dropped fields under fixed schema labels and SQLite page/WAL-frame counts before and after. No message bodies, event data, actor content, raw tokens, or arbitrary field names are printed. The CLI writes a new mode-0600 file outside the source mesh and prints the same sanitized JSON; it refuses overwrites and reports fixed error codes without source values.

SQLite is opened read-only with `query_only` and one read transaction; no immutable, backup, checkpoint, or write-capable fallback is used. Normal SQLite SHM reader-index maintenance is permitted by the owning lead's scoped ruling, not an authority-data write. FILE collection reads canonical `state.json`, lexically skipping unrelated value subtrees instead of parsing/exporting private bodies. Source file identities/listings are checked for change; a DB transaction plus independent files is not a globally atomic fleet snapshot.

Unsupported schemas/IDs, unknown included-record containers, unsafe paths, changing files, and exceeded bounds fail closed; there is no sampling or truncation. Bounds are 32 MiB canonical source/projected output, 256 KiB per authority value/file, 10,000 authority rows or files per class, 256 array/history items, and 32 explicit cursors. Observed truncated-hash collisions fail; an undetected hypothetical cross-site 128-bit collision remains a limitation. Imported process fields are observations, never proof that a fleet PID is alive on the replay host.

## Historical regression

Refs Smarty-Pants-Inc/smarty-dev#14111 and Refs Smarty-Pants-Inc/smarty-dev#14279.

The regression target is `9c644fd2ab93f3fed3c0ee51e28e96f70a2a7bf5`. Run the same full gate with that release as NEW: it must exit nonzero because tokenless relayed presence conflicts with persisted incarnation authority. A setup failure, missing build, or unrelated unsupported protocol is not proof of that regression. Repeat with the strip-relayed-guard candidate and retain both receipts.

Fresh registration behavior must be measured rather than inferred from a host's installed version. In local compiled-release probes, `c42f7f34125abe2ebbe660758253c9ec4d7765dd` writes a token, while `41a73f8588e5351d9409736f725fee218810b78c` writes none. The latter also needs lock protocol 1 or 2 and predates cross-spoke relay. Its unsupported cross-spoke pairs are not a new-release regression; its primary's direct hub delivery, uninterrupted link, and independent fresh tokenless primary reaching newer spokes remain mandatory. Skipping the separate historied sentinel is intended security behavior, not a waiver for a missing fresh route. Report case (b)'s observed public outcome verbatim alongside the receipt, whether delivered or refused. The canary reports these differences instead of manufacturing tokenless c42f registrations or claiming unsupported routes worked.
