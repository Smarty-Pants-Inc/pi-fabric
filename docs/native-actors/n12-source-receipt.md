# N12 historical SOURCE receipt — not the installed-artifact gate

For [smarty-inbox #342](https://github.com/Smarty-Pants-Inc/smarty-inbox/issues/342), R4 N-2. Transcribed from the canonical Parent-owned job `tmp.GeYhccIwEc`; the R4 prep writer read these retained inputs, but executed no validation. The independent R4 challenger's later 35-pass run is **different and noncanonical**. Neither run is an installed-ca4 compiled-artifact receipt.

## Canonical command and terminal result

Retained private receipt identity: **job `tmp.GeYhccIwEc`**, with terminal/stdout/stderr/owned record, driver and archive manifest. Their hashes below bind the original files; private absolute paths are deliberately not public authority links.

**Portable command rendering**, not a byte-exact raw transcript. `TEST_ENV` means the approved test-environment executable, `NODE` the selected Node executable, `JOB` the retained job directory and `REPO` the extracted candidate repository. Argv order and flags are preserved; these variables substitute private paths:

```sh
systemd-run --user --scope -p CPUQuota=400% nice -n 10 "$TEST_ENV" -- "$NODE" "$JOB/verify-n12.mjs"
```

The retained `verify-n12.mjs` selected launcher-owned test temp state and ran this child command with inherited streams, cwd `REPO`, timeout 420,000 ms (same portable substitutions):

```sh
"$NODE" "$REPO/node_modules/vitest/vitest.mjs" run tests/bridge-nats-ledger.test.ts tests/nats-auth-path-b-bridge-ledger-rollback.test.ts --retry=0
```

| Terminal field | Actual value |
|---|---|
| version / PID | `1` / `3506324` (matches retained `owned.json`) |
| status / exit code | `exited` / **0** |
| started_at | `2026-10-25T22:24:00.972093+00:00` |
| ended_at | `2026-10-25T22:24:07.079320+00:00` |
| elapsed_seconds / deadline_seconds | `6.107232918962836` / `600.0` |
| Vitest | `4.1.10`, start `22:24:01`, reported duration `5.16s` |
| Exact coverage | **2 files / 35 total = 11 N12 SOURCE + 24 bridge-ledger**, all passed |

This is **not 35 N12 tests**. The wrapper archived fixture state while launcher custody remained live, read back its selected regular files, asserted retained actual append bytes existed, and propagated the Vitest status. Its manifest reports child status 0 / signal null, **1,119 archived regular files / 0 native evidence manifests**. Those numbers describe this archive, not SDK/Main/broker acceptance or an independent new archive verification by the prep writer. `stderr` retains hosted-only notices and the expected custody-unrecoverable negative-case alarm; they are not silently erased.

## Source identity and limits

The run used **dirty integration source**. The receipt/wrapper does **not** record an immutable tested Git HEAD or a complete candidate-source manifest; no tested commit is invented from a later integration commit.

The retained real append-child observations do pin the exercised candidate ledger source:

- `candidateSourceSha256`: `38e4259389adda9b7765d4f8d908f0177afbf2ccaf5761c7d8c8a11f776c2142`.
- Normal child PID `3506872`: one write / one fsync / zero truncates, 1,433 requested and persisted bytes, not visible at write/fsync, `claim-resolved-after-fsync`.
- Crash child PID `3506949`: one write / zero claim-fsync calls / zero truncates, 1,433 requested / 716 persisted bytes, `prefix-persisted-before-write-return`. The test awaits actual child close/SIGKILL; this models a process crash, not arbitrary power loss.

Read-only comparison during R4 prep: this ledger hash equals the blob at **`656e218f2385e050502d6eeaf8195614ea611d2c:src/mesh/bridge-nats-ledger.ts`** and the file at prep HEAD **`084a3f77d8387ca3ad9411a35f64a15e20e0b5f1`**. `git diff` between those commits is empty for the ledger, frozen-ca4 helper, rollback suite, bridge-ledger suite and crash fixture. That links eventual committed bytes; it does **not** prove that entire later commit was tested in this earlier dirty job. R4's helper change/new cases are not covered by this historical receipt.

The old reader was exact **SOURCE**, loaded from the pinned Git archive without shared `node_modules`, not candidate reader code:

| Old input | Pin |
|---|---|
| ca4 commit | `ca4b1482119748394df5125c2a8d2853676f7e04` |
| whole tree / src tree | `fd090a6771e33b56614eb8a3fd3ec245868360fb` / `8a5ed7c4453401b26fc8339ced304eb23da44dc0` |
| selected source/assets tar SHA-256 | `8b7407aafd0d9b3ffb0afe42b81d3ad7703382797a21568090140762e491709f` |
| old ledger SHA-256 | `02ed87aa5434d591850035a0eb91149151c62fd8711f30bb3d38760e099b60b8` |
| old custody source SHA-256 | `51cbedc15cbebdc7f8095af5d3271d10d0d7a26d59f051b3668729f77e23a197` |
| archived old bun.lock SHA-256 | `7b871eab624822df21b6d4628f420ffd09e5d7ac4b62eca5436a129fb4b63ebb` |

The R4 writer hash-read the retained extracted old ledger/custody/lock and candidate child observations. The helper still requires locally available pinned history/objects, exact tree/archive/extracted-source hashes and no dependency fallback. CI's existing full-history checkout is unchanged. R4 removes only **current-lock parity**: this source-only graph shares no packages, so the candidate `bun.lock` is not its authority. This does not relax the separate frozen-f339 helper's package prerequisites.

## What the 11 historical N12 cases establish

Candidate-generated native **and ordinary** ask/cancel claims, receipts, identity/principal/retained bodies, outcomes and event dedupe survive exact-old-SOURCE open, old append, compaction and reopen. One real candidate append crash is recovered by the old reader's built-in single-tail cut, with prior claims/dedupe preserved. Short/zero/write/fsync append faults fail-stop without a later queued append. Corrupt middle records (with and without a tail) and two damaged suffix records refuse. Originals and ambiguous outcomes are retained; no unknown work is dispatched.

The delivery/KV source and owner ACKs are synthetic. This is **no real broker, SDK/Main/native callback, production app/dedupe, installed old binary or traffic-restoration proof**. It does not cover R4 directory drift / recipe / issued-scope / whole-link additions. At that historical prep snapshot all R4 checks were **UNRUN**. Later R4 SOURCE/component receipts are separately recorded in [r5-final-receipt.md](r5-final-receipt.md); none is the installed-artifact gate.

## Evidence hashes read during prep

| Retained file | SHA-256 |
|---|---|
| `terminal.json` | `29d4f855d3a5d895d8d21218c414a08733afecef6a183e42c0a03f7eb3deb857` |
| `stdout` | `092a4692c72148943239a231dbd5e3f8150b55b09de9b812c290fb082e39aa43` |
| `verify-n12.mjs` | `a33520a29928a2ce4db0b40bcc7f4f0e4307de3ebbbbbe75528da88db9a14c94` |
| `native-fixture-evidence.manifest.json` | `d56d727351ca4817512e3fcdf274c32bf3dd06ad3edeba94e270c0bd242e34bb` |
| `source-files.manifest.json` | `d2d6d47a5236fab7669483c72c30f8544591a2c442695cc353d0599f4df6900d` |
| `native-fixture-evidence.tar.gz` | `b4b978cfa72cf8b4014f4dde969606225adf53843c93df7459f142ff487a145e` |

## Still REQUIRED / OPEN / UNRUN before rollout

The **actual installed ca4 compiled artifact**, with installation owner/full artifact digest/path provenance, must open an isolated copy of candidate-written valid bytes **and a real candidate mid-append crash copy**, preserve claims/outcomes/dedupe/receipts, then append/compact/reopen. Seal originals before either open; await actual owned-child termination. Also demonstrate middle-corruption STOP with no replay. SOURCE similarity or this receipt cannot waive that gate. Follow [recovery-runbook.md](recovery-runbook.md); merely opening with the old reader never authorizes affected traffic to resume.
