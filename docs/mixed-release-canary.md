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
- Case (b) passes only for delivered + matching authenticated destination handler and ACK, or a named visible **actual public request** refusal/error within that budget. The classifier requires the actual request source, matching target and an exact public name/code pair: `FabricBridgeStaleError`/`FABRIC_BRIDGE_STALE`, `FabricPeerLeaseStaleError`/`FABRIC_PEER_LEASE_STALE`, `FabricParticipantStaleError`/`FABRIC_PARTICIPANT_STALE`, `FabricControlStaleIncarnationError`/`FABRIC_CONTROL_STALE_INCARNATION`, `FabricControlIncarnationRequiredError`/`FABRIC_CONTROL_INCARNATION_REQUIRED`, or `FabricRelayDestinationUnavailableError`/`FABRIC_RELAY_DESTINATION_UNAVAILABLE`. An arbitrary named exception such as `TypeError` is not refusal evidence. A permitted route/lease error is a visible sender outcome even if its verbatim message warns that delivery is unknown; that does not establish delivery. A generic ACK timeout, no result, harness/precheck error, a claimed send/ACK with no destination handler, duplicate sentinel delivery, or changed retained authority fails. The native old sentinel is read back live at the attempt and afterward; its separate final handler/command/ACK tally is retained even on refusal. It may be delivered at most once. These diagnostic tallies never waive or replace the normal all-pairs controls.

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

Only the generated sanitized JSON files may be sent to the canary host. Never copy a mesh database, canonical state file, WAL/SHM, raw lease/participant files, events, actors, or cursor backups. The `fabric-release` files beside the cursors are not authority inputs. The Intel 1 hub cursor is an observation, not a substitute for Intel 1 work-side authority; these two captures cover the dev1–Ryzen 2 pairing only. For an Intel 1 pair, use the reviewed work-name allowlist entry `--site work --name intel1 --peer dev1` and collect a NEW hub snapshot with `--peer intel1`. Do not rewrite an existing Ryzen 2 snapshot's peer metadata or reuse its bytes as an Intel pair.

The allowlist projects host records, participant ownership joins, handoffs, liveness policy, separate participant/lease files, mirror observations, and explicitly named cursor fields. All incarnation/owner/process token values and superseded-token keys become stable `h:<32 hex>` SHA-256 prefixes. Canonical IDs and root joins remain exact; arbitrary display names become safe aliases. The field census is in `counts`, including dropped fields under fixed schema labels and SQLite page/WAL-frame counts before and after. No message bodies, event data, actor content, raw tokens, or arbitrary field names are printed. The CLI writes a new mode-0600 file outside the source mesh and prints the same sanitized JSON; it refuses overwrites and reports fixed error codes without source values.

SQLite is opened read-only with `query_only` and one read transaction per attempt; no immutable, backup, checkpoint, or write-capable fallback is used. Normal SQLite SHM reader-index maintenance is permitted by the owning lead's scoped ruling, not an authority-data write. FILE collection reads canonical `state.json`, lexically skipping unrelated value subtrees instead of parsing/exporting private bodies. Source file identities/listings are checked for change; a DB transaction plus independent files is not a globally atomic fleet snapshot. Collection and census retry the WHOLE capture at most three times ONLY for `SOURCE_CHANGED`, each with fresh context/transaction and no merged rows or counters. Success reports `attempts`; exhaustion returns only `{"error":"SOURCE_CHANGED","attempts":3}`, with no snapshot/report output. A failed census is not an exhaustive walk.

IDs use explicit source-traced shapes: `session:<uuid>`, bare 32-hex actor/agent IDs, `resident:<24hex>`, `runtime:<uuid>`, and the two named `session:actor-{outbox,mailbox}-exact-owner:<1..13hex>` CI roots. Session-ID fields accept UUIDs or those two exact CI names, not arbitrary names. Only participant `entry.value.sessionId` additionally permits canonical positive decimal process-session metadata, without leading zero and at most `4294967295`; it is not an authority ID or proof of a live PID. Only `entry.updatedBy.id` additionally accepts `^bridge:[a-z0-9-]{1,32}$`, literal `spawn-router`, and `^maintenance:[a-z0-9-]{1,40}$`; those forms remain rejected for host, root, owner, and lease identities. An unsupported ID reports only the fixed schema `field`, `recordKind`, and redacted `shape` in its stderr error JSON; it never prints the source ID, arbitrary prefix, path, or native error. `participant:` is a UI entity ID only; the release owner's shape-only readback found none in persisted authority, so it remains rejected rather than guessed. A successful fixture run does not establish that every live fleet ID is supported; rerun the reviewed collector and use its safe diagnostic if it fails.

Use `--census` with the same source arguments and a separate output file to inspect every fixed authority-ID slot in one pass, rather than discovering one unsupported ID per collection attempt. For example, on Ryzen 2:

```sh
node scripts/fleet-authority-snapshot.mjs --census --mesh /home/paul/.local/share/smarty-dev/fabric-mesh/fleet --site work --name ryzen2 --peer dev1 --output "$OUTDIR/work-census.json"
```

The census emits only grouped `{field, recordKind, shape, count}` diagnostics and aggregate field/SQLite counts, never authority records or private bodies. A clean census has `complete:true` and exits zero. Unsupported IDs produce the full aggregate report with `complete:false` and exit one; ordinary collection still stops at its first error without creating an output file. Structural errors, source changes, unsafe paths, and bounds remain hard failures: a missing report is not evidence of an exhaustive walk. A census report is not an authority snapshot and cannot be used as a replay input.

Unsupported schemas/IDs, unknown included-record containers, unsafe paths, changing files, and exceeded bounds fail closed; there is no sampling or truncation. Bounds are 32 MiB canonical source/projected output, 256 KiB per authority value/file, 10,000 authority rows or files per class, 256 array/history items, and 32 explicit cursors. Observed truncated-hash collisions fail; an undetected hypothetical cross-site 128-bit collision remains a limitation. Imported process fields are observations, never proof that a fleet PID is alive on the replay host.

## Replay the captured pair with live native writers

The opt-in `--authority-snapshot --live-writers` experiment is separate from the full gate above. Without `--live-writers`, captured-only replay is diagnostic and cannot satisfy the requested release experiment. Both arms start independently from the same original sanitized snapshot bytes, retaining authority/history and applying one global time translation. Imported process observations are not treated as live processes. Handoffs remain blocked without an owned process mapping.

Pin all four roles independently:

| Arm | Hub bridge | Work agent bridge | Hub native writers | Work native writers |
| --- | --- | --- | --- | --- |
| RED | `9c644fd2ab93f3fed3c0ee51e28e96f70a2a7bf5` | `0fc8ca5d706dfb85519719ebfcc12fadfd63696c` | `c42f7f34125abe2ebbe660758253c9ec4d7765dd` | `c42f7f34125abe2ebbe660758253c9ec4d7765dd` |
| PASS | RC2 exact full SHA | Same RC2 SHA | `c42f7f34125abe2ebbe660758253c9ec4d7765dd` | Same RC2 SHA |

Real inputs require an externally pinned `operator-capture-pair` manifest and per-site receipt files. The manifest binds `pairId`, each site's `site`, `name`, `backend`, `collectedAt`, the original snapshot-byte `captureSha256`, and `collectorReceipt:{path,sha256}`. Each receipt repeats that site's binding. Derive these values and hashes in code from the original files. Receipt paths resolve against the manifest directory. The caller's externally pinned manifest is a trust input, not proof of operator authentication. Object reserialization is not a substitute for original-byte provenance.

An authority population is empty only when BOTH state-host rows and lease files are absent. State-only inputs (state-host rows with no leases) may execute as a partial diagnostic, but never become incident-faithful or acceptance evidence. A valid population with state hosts plus matching leases and zero participant files is full, as is a pure lease-only population; participant-file cardinality never makes a population partial. Coverage matching is by the host and lease `id`, `rootId`, and `identityId`, without requiring incarnation or start-time equality, and the matching lease's effective expiry (`max(expiresAt,reloadUntil)`) must be strictly after that snapshot's `collectedAt`. Missing lease coverage is classified from the inputs; a manifest cannot claim it complete. The separate `diagnosticContext` has the exact label `partial diagnostic: participant/lease coverage missing; mixed-era (hub 16:30 live, work 12:40 snapshot)`, `partialDiagnostic:true`, `incidentFaithful:false`, `acceptanceEligible:false`, `material:"mixed-era"`, and the fixed missing `site.field` names. An optional manifest context must match it exactly. The producer scope stays unchanged. The recollector's timestamp is not the historical acquisition time: report collection skew and captured deadline ages, retain the single global translation, and never re-age the old authority or invent absent files. The 12:40 work reconstruction has no matching 12:40 hub capture.

With built release directories and the captured files supplied by the release owner:

```sh
node scripts/mixed-release-canary.mjs --authority-snapshot --live-writers \
  --hub "$HUB9C" --hub-sha 9c644fd2ab93f3fed3c0ee51e28e96f70a2a7bf5 \
  --work "$WORK0FC" --work-sha 0fc8ca5d706dfb85519719ebfcc12fadfd63696c \
  --hub-writer "$C42" --hub-writer-sha c42f7f34125abe2ebbe660758253c9ec4d7765dd \
  --work-writer "$C42" --work-writer-sha c42f7f34125abe2ebbe660758253c9ec4d7765dd \
  --hub-snapshot "$HUB_SNAPSHOT" --work-snapshot "$WORK_SNAPSHOT" \
  --capture-manifest "$CAPTURE_MANIFEST" --capture-manifest-sha256 "$MANIFEST_SHA256" \
  --candidate-both-sides --candidate "$RC2" --candidate-sha "$RC2_SHA" \
  --candidate-work "$RC2" --candidate-work-sha "$RC2_SHA" \
  --candidate-work-writer "$RC2" --candidate-work-writer-sha "$RC2_SHA" \
  --passes 3 --timeout-ms 60000 --receipt "$RECEIPT"
```

Use the same short-private-runtime wrapper and evidence-retention precautions as the full gate. The harness starts genuine compiled participant-directory producers on the seeded SQLite hub and FILE work mesh, with fresh identities disjoint from all captured reservations. It observes real heartbeats, bridge frames, both mirror orientations, native release/census attribution and teardown. It never renews or resurrects captured identities to manufacture a refusal. Receipt scope is exactly: **producer-boundary compatibility; principal session bootstrap and auth policy are not exercised**.

RED requires the actual matched stale-incarnation refusal, with root/direction and ordered predicate/history evidence. Root-only errors do not identify a sibling host uniquely. PASS requires at least three completed, continuously nonempty passes in both directions, every requested fresh origin and mirror, no bridge restart, real native renewals, and observed natural compiled-path expiry or expiry maintenance on captured state. A timestamp comparison alone does not prove expiry. Post-fatal reader observations may prove expiry but never link continuity. There is no forced TTL shortening, synthetic expiry substitution or pre-link wait to hide captured exposure. Missing coverage or cleanup is BLOCKED.

Inspect individual arm outcomes: an old-only run can retain a real `primaryOutcome:RED` while the experiment stays BLOCKED for missing RC2. `acceptance:true` requires complete provided real captured authority, primary RED and candidate PASS; synthetic fixtures and partial diagnostics never satisfy it. Omit all candidate flags for a primary-only run while RC2 is unavailable; inspect `primaryOutcome` separately from the experiment's missing-RC2 BLOCKED status. Every receipt still has `releaseGate:false` and `rolloutPermission:false`. This narrow experiment never replaces the ordinary 60-second full gate or authorizes rollout.

## Historical regression

Refs Smarty-Pants-Inc/smarty-dev#14111 and Refs Smarty-Pants-Inc/smarty-dev#14279.

The regression target is `9c644fd2ab93f3fed3c0ee51e28e96f70a2a7bf5`. Run the same full gate with that release as NEW: it must exit nonzero because tokenless relayed presence conflicts with persisted incarnation authority. A setup failure, missing build, or unrelated unsupported protocol is not proof of that regression. Repeat with the strip-relayed-guard candidate and retain both receipts.

Fresh registration behavior must be measured rather than inferred from a host's installed version. In local compiled-release probes, `c42f7f34125abe2ebbe660758253c9ec4d7765dd` writes a token, while `41a73f8588e5351d9409736f725fee218810b78c` writes none. The latter also needs lock protocol 1 or 2 and predates cross-spoke relay. Its unsupported cross-spoke pairs are not a new-release regression; its primary's direct hub delivery, uninterrupted link, and independent fresh tokenless primary reaching newer spokes remain mandatory. Skipping the separate historied sentinel is intended security behavior, not a waiver for a missing fresh route. Report case (b)'s observed public outcome verbatim alongside the receipt, whether delivered or refused. The canary reports these differences instead of manufacturing tokenless c42f registrations or claiming unsupported routes worked.
