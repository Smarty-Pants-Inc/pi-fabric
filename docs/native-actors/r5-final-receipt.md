# R5 receipt — historical R4/C1 qualification; C2 preparation is not acceptance

For [smarty-inbox #342](https://github.com/Smarty-Pants-Inc/smarty-inbox/issues/342). This transcribes Parent's verified historical handoffs and retained canonical receipts. The C2 writer did not execute these historical commands. Counts, times, exits and archive hashes are actual; commands below are **portable renderings with private absolute paths substituted**, not byte-exact raw transcripts. A hash identifies bytes, not correctness.

## Evidence boundaries

- **N-1: candidate-proven; production install unproven.** Fabric-store owns qualification/readback of the actual installed adapter and provisioner before any principal relies on the service. Candidate-install tests are not production installation.
- R4 product-fence commit: **`dff161c6b3e12c2eb6facd02d04ef112f8abbd0a`**, tree `53e540bc5859ab5f900c36f02522f2b5538746fd`. Final qualification ran on frozen precommit bytes at `084a3f77d8387ca3ad9411a35f64a15e20e0b5f1`; postcommit readback matched all 19 committed paths. It was **not rerun after commit**.
- C1: **`1c62b57279f2aec77befb93a0f7c5d9dc6d377a1`**, tree `5bcd96f7292bee588cc15f36cb211e64dda39217`. Qualification ran on frozen uncommitted bytes at parent dff161c6; postcommit Git-object/worktree readback matched the 12 qualified files. Again, no claim that the driver ran after commit.
- All later C2 startup-death, native guard, plain-CLI clean-tail refusal, discovery and prerequisite regressions are **PREPARED/UNRUN pending Parent qualification**. No historical PASS qualifies those new bytes. Earliest-bootstrap placement must be within authorized source custody; an activation-only check is not represented as earliest-bootstrap acceptance.
- Native helpers used an **UNPINNED local host compiler**, not release-qualified toolchain provenance. No SDK, installed launcher, profile, service or production actor was changed.

## Portable command vocabulary

`REPO` is the owned candidate checkout; `JOB` the retained job directory; `NODE` the explicitly supplied approved Node executable; `SDK_DIST` the caller-supplied canonical SDK directory; `TEST_ENV` the authorized test-environment executable. Approved release is **`031c749962972b0cb966e443e921d64654627b20`** and all ten source/package SHA pins in `tests/helpers/native-sdk-no-model.ts` remain authoritative. No public host path or location allow-list approves an SDK.

Historical outer wrappers used the recorded CPUQuota400%/nice10/test-env contract. A representative rendering of the R4 final command, preserving argv order, is:

```sh
systemd-run --user --scope -p CPUQuota=400% nice -n 10 "$TEST_ENV" -- "$NODE" "$JOB/verify-r4-native.mjs"
```

The retained native driver selected launcher-owned test temp and explicit SDK/Node inputs, verified frozen source pins, and invoked:

```sh
"$NODE" "$REPO/node_modules/vitest/vitest.mjs" run tests/native-owner-entry.test.ts tests/native-actor-residency.test.ts tests/native-sdk-no-model.test.ts tests/native-participant-presence.test.ts --retry=0
```

Its archive was taken **while the launcher retained whole-root custody**, then selected regular files and native manifests were independently extracted/hash-verified before cleanup. Failed runs/raw traces remain preserved. A successful worker launch or task completion is not a gate receipt.

## R4 canonical receipts

| Gate / retained job identity | Actual terminal result |
|---|---|
| Source3 `tmp.VxmHRthQdu` | PID2310220, exit0, **2026-10-26T04:31:29.352011Z → 04:33:01.492967Z**, **92.14094652188942 s**, deadline600 s. Whole types, fresh build before graph, three fixture syntax checks, **106/106 across six suites**, retry0. |
| Final fresh build | PID3687142, exit0, **07:32:43.043630Z → 07:33:02.309700Z**, **19.266068373 s**, deadline600 s. `nice -n 10 TEST_ENV -- bun run build`, inherited caps. |
| Types + host-free lazy graph `tmp.MLCqX74OuP` | PID3703848, exit0, **07:35:35.780999Z → 07:36:14.686363Z**, **38.905370275 s**, deadline300 s; whole types and graph51 passed. |
| Final native `tmp.v1FIUdILXC` | PID3712110, exit0, **07:37:20.622230Z → 07:40:13.150935Z**, **172.52870991500095 s**, deadline900 s; **21/21 = 3 actual-entry + 14 residency + 1 SDK + 3 frozen presence/routing**, retry0. |

Source3's six suites were rollback15, bridge-ledger24, native-ingress35, native-actor6, optional-durable-startup1 and build-artifacts25. These are **106**, not native acceptance or an addition to the 21 as one run. Portable child selection:

```sh
bun run typecheck
bun run build
bun run assert:lazy-graph
bunx vitest run tests/nats-auth-path-b-bridge-ledger-rollback.test.ts tests/bridge-nats-native-ingress.test.ts tests/bridge-nats-ledger.test.ts tests/native-actor.test.ts tests/optional-durable-startup.test.ts tests/build-artifacts.test.ts --retry=0
```

Source3 archive SHA-256: **`3b6c8620a1951cf87c18d44cc6f5e731802282bde56bb8753d280bb936b0e6f3`**, **1,151 verified regular files / 0 native manifests**. Final native archive: **`1053115cf9277137b716ed65653c6bbbab1608ddc25bfabbbc402177fdd59260`**, **15,748 verified regular files / 9 sealed native manifests / 68 recorded owned processes terminal**. Native source-files manifest SHA: `fbb1c943f5d0f3b5fb1d6de62d9f0b55f17d820275a6f57411190615427d64bd`.

Selected R4 source-byte pins: native-owner entry `6936a3566e346611127b6fd1ae12028e0b49da4d3d7e593e98e8d77f8bf8b523`; entry test `697cca318b39c0c1e8e3577c5a418ceb9a11e82afaa346e35ae2af553fca0b98`; helper `400dba280f33725aa8fd38ea379f09e358e02569012c4b554001960b6abdbd0f`; rollback suite `ebc5a2ae27e7c60c98e13e5108bec65e3ced58890cc20d50bcafa1ea8a5e306a`; ingress suite `15a1f24644bd2d9a48f337aa59356c455a589338d98ae54275fdec87c7159494`. These identify historical bytes, not C2.

Historical N12 **35 total = 11 N12 SOURCE + 24 bridge-ledger**, job `tmp.GeYhccIwEc`, remains separately transcribed in [n12-source-receipt.md](n12-source-receipt.md). SOURCE is not the installed-ca4 compiled-artifact valid/crash/downgrade gate.

## C1 canonical receipts

| Gate | Actual terminal result |
|---|---|
| One fresh build | PID1559690, exit0, **2026-10-26T11:10:03.142537Z → 11:10:22.533743Z**, **19.391216558869928 s**. |
| Whole types → graph51 → eight targeted suites | PID1621877, exit0, **11:19:26.861896Z → 11:21:02.410718Z**, **95.54881691606715 s**; **340 PASS / 4 declared missing-native SKIP / 344 total**, retry0. This count is not native qualification. |
| REQUIRED native24 | PID1639154, exit0, **11:22:38.212727Z → 11:26:29.209219Z**, **230.9964932599105 s**; **24 PASS = original21 + 3 prerequisite guards**, retry0. |
| Separate original21 selection | PID1680188, exit0, **11:29:50.372018Z → 11:32:56.670250Z**, **186.29823025595397 s**; **21 PASS**, three guard cases deliberately name-filtered, not native-prerequisite skips, retry0. |

Native24 archive SHA: **`8c69ad872b1e6b353c2f62360749911e4a1cca74c8d896c9d7c60d05cb8d593f`**, **15,751 regular files / 9 manifests / 68 terminal owned processes**. Original21 archive: **`8c65f6373067e0e3ab1849c0483d61aae776efffcbd92f71bef6cfd498b44aed`**, **15,748 regular files / 9 manifests / 68 terminal owned processes**. Parent independently extracted/verified all bytes and confirmed each canonical leader absent before the next phase.

C1 retained earlier failed GREEN/RED attempts, a notification-inside-test-env refusal and the original writer's timeout143. Those are not relabeled success. Final canonical packets above are separate. Source assertions were preserved; final source readback binds the actual qualified bytes to C1.

## F1 timing — observations, not a conservative enclosure or N6 SLA

The **2000 ms** monotonic regression deadline remains unchanged. Natural clock starts **before launcher SIGKILL**; held clock starts **before Main SIGCONT**, with deliberate scheduling hold separate. The same original deadline spans launcher exit, positive old-Main death, stdio close and real fenced SDK verification. A live/late Main fails; no fallback Main kill, unlock, parent-held stdin or reset clock can pass it.

| Distinct run population | Natural release verified | Held release verified |
|---|---:|---:|
| R4 final | 293.114127 ms | 312.832733 ms |
| Independent R5 captures (three runs) | 364 / 471 / 544 ms | 429 / 493 / 511 ms |
| C1 native24 | 656.856767 ms | 713.485478 ms |
| C1 separate original21 | 376.464473 ms | 347.454085 ms |

R4 final stdio/positive death were 64.099610/64.450978 ms natural and 74.387916/74.559196 ms held; deliberate hold363.762814 ms (launcher EOF held361.600196 ms). The former **324 ms conservative basis and 1676 ms arithmetic assertion are withdrawn**. This is a load-sensitive test deadline including fresh Node/SDK verification, **not a production, hung-Main or host-load guarantee**. N6 bounded join/exact pidfd kill/terminal/alarm, its deployed owner and full artifact pins remain **UNIMPLEMENTED/pre-canary REFUSE**. Report new >1000 ms observations before changing the bound.

## RULING2 / item(6) prepared coverage and residuals

Prepared Q1 cases stop the **actual Main** after a positive self-fd fence and before READY, and during the approved SDK's actual `appendFileSync` invocation. A test-only libc interposer delegates a real short write of those actual SDK bytes on the pinned session inode, records syscall/byte evidence, stops the child, and the test SIGKILLs the recorded Main. No fabricated session line, substitute SessionManager, patched SDK or fake death is admissible. Parent must prove positive PID/birth/stdio death, released custody, unchanged old prefix/pins/inodes, no second writer and actual parser/same-root restart. Until run, this remains preparation.

Product proof checks self `fdinfo` whole-file FLOCK WRITE plus held/public dev/inode, uid, regular/nlink1/0600, before native SDK session access. An unlocked descriptor to the same inode is insufficient. Old-holder next actual SDK open/append rechecks its **original** custody after pathname replacement; a deliberate same-UID contender on a new lock inode is an accepted residual, not required exclusion. Recheck-to-write TOCTOU remains.

Real plain SDK CLI plus candidate Fabric, with no fence and buffered input on an open stdin, must terminate with the typed native-fence refusal **before identity/publication/participant/Fabric append/first turn**, with exact unchanged **clean-tail** session/pin bytes. Mere thrown hook errors or silence are not proof. Upstream CLI pre-hook open/read is accepted. This is not an installed-production claim.

**Discovery isolation:** the existing owned native pin selects `WORK/sessions/owner.jsonl`, outside the ordinary selected profile's `WORK/profile/sessions` root and its encoded-cwd subdirectory. Native launch explicitly pins the canonical absolute file; the ordinary resolver gets **no native sessionDir override**, no inherited native session-dir environment selector and no custom profile sessionDir. Prepared tests use the actual approved SDK's `SessionManager.list/listAll/findById` and `main.createSessionManager` for continue/bare-ID, plus its **real resume picker** through a PTY. A legitimate named ordinary session must list/continue/resume/resolve, while native file/ID are excluded; repeat scans cover warm discovery. The selected SDK scans these roots directly; no separate persistent discovery index is relocated or asserted repaired. No principal data is scanned.

**Path/config/schema/migration:** this proves existing test-path exclusion; it is **not a deployed native configuration**. No candidate session-file pin relocation, root/header/session-ID change, source config schema change or performed migration. No production native owner has been installed/used by this lane. Deployment owner must preserve the absolute native pin outside the actual selected ordinary launcher's roots and read back its effective profile/sessionDir/index settings before use; this fixture is not that installation proof.

Explicit [smarty-dev#17003](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/17003) residuals: **(a)** Fabric-disabled plain Pi/another same-UID program can still open/append an explicitly addressed native file; **(b)** the SDK's `readEntriesFromFile` can repair a torn tail's missing newline **before Fabric's hook**, even though clean-tail refusal writes nothing. Those are separate from discovery exclusion. The owner must compare its durable ledger/byte evidence and fail closed on unexplained repair; this revision does not claim an unimplemented ledger-repair mechanism exists. This is cooperative host admission, not OS mandatory access control or malicious same-UID protection.

## Remaining owner gates

Carry **`656e218f2385e050502d6eeaf8195614ea611d2c` through final integrated head and every later fix in ONE PR/co-release**. Dependency NEVER alone; no intermediate deployment/tag or S1-only revert. Deployment-owned provision/verify MUST call the exported product fence before SDK/session access and qualify the actual installed adapter/provisioner; the fixture is not a shipped deployment provisioner. The reserved literal `runner: native` is a host-native identifier, never a registered Pi/model runner.

Q3 is an **OPEN N8 test gap**: an exactly addressed pinned ask may wait in-hand up to the default15 s publication allowance, then TERM/release **without owner ACK**, not a durable guaranteed queue. A fresh frozen public lookup may fail before publishing a frame. Existing N7 waits for a committed participant/epoch; the original no-callback cause remains UNKNOWN. N8 must exercise real post-restart READY→publication recovery and expiry, fresh and retained-route asks, ≤60 s fixed-client outcomes/correlation/no duplicate effects.

Full N8 business reply/two independent seats/response-loss+COMMIT dedupe/real reconstruction/default>600 s idle; new audited app+server/helper/client full SHA+CI, all-helper heartbeat gaps<10 s and fixed-client≤60 s; installed-ca4 **compiled-artifact** valid/crash/open/append/compact/reopen/downgrade; recovery schema/opKey query/readbacks; N6, security, retention, authorized canary and install gates remain OPEN. No future PASS, N1, production START/BOOT ENABLE, installation or rollout date is asserted.
