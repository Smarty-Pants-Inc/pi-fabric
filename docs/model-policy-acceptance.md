# Model-policy public-path acceptance (#2490 / PR #222)

## Pre-merge reproducible CLI proof

Build first, then run in a clean environment with a disposable HOME and TMPDIR:

```sh
bun run build
env -i PATH="$PATH" HOME="$TMPDIR/offline-home" TMPDIR="$TMPDIR" \
  node scripts/probe-model-policy-cli.mjs dist/index.js "$TASK_OUT/native-after"
```

The probe starts the **real Pi CLI in RPC mode**, loads the candidate compiled
Fabric extension, and uses actual Fabric worker processes, actor activation,
public TypeScript `fabric_exec` calls, and native `get_state` admission. Only
inference is synthetic: `tests/fixtures/model-policy-cli-provider.ts` registers
an offline keyless deterministic provider. No credentials, factory installation,
or network inference are required, read, or claimed.

The host config deliberately sets a **denied** `agents.model` and LOW effort.
Main starts on the allowed model at MAX. A task parent and an actor parent each
spawn a depth-2 child without model/effort overrides. The probe checks native
model and effort in both Main RPC state and every worker's independent
`get_state` response, as well as pre-prompt native session-change receipts.
It also catches public `agents.spawn` and `agents.create` denials, asserting the
fixed `FABRIC_MODEL_DENIED` code, unchanged task and actor registries, and no
denied worker prompt. Every actual run is awaited, the actor is removed, and
Main closes stdin and waits for its CLI to exit before deleting scratch state.

Retained output: `command.json`, `events.jsonl`, `public-tool-result.json`,
`guest-value.json`, `main-get-state-before.json`, `main-get-state-after.json`,
`native-state-receipts.jsonl`, `summary.json`, `stderr.log`, and `runs/` (including
native RPC admission responses and final status files). Pass another compiled
entry point as the first argument to compare the original head. On
`1970decefee3e04bb35b4abe444279e3f54c4f5e` the same CLI probe fails because the
public guest catches a denied error without `code`; inheritance itself already
works. The missing-evidence review finding is not a distinct behavioral bug.

## Installation-only owner gate (not satisfied by the offline probe)

Owner: **fabric-v2** for the reviewed Fabric release, **dev-lead** for trusted
factory policy adoption and the independent installed acceptance audit. No fleet
or installed-policy PASS is claimed before this gate. Complete before broad
adoption; stop rollout if any receipt below is missing or fails.

1. Record reviewed final commit, published version/artifact digest, installed
   artifact digest/version, factory policy generation, host/profile identifiers,
   and the rollback generation. Confirm the trusted host's deny list contains
   `cliproxyapi/gpt-6-astra` and `cliproxyapi/gpt-6-sol`, with replacement
   `cliproxyapi/gpt-6.1-sol`; a workspace cannot erase or change those keys.
2. In a disposable **installed** Pi session, capture its command, native parent
   model/effort, allowed Sol/MAX task and actor controls, and their spawned
   children's independent native `get_state` model/effort receipts. Compare
   children to the actual parents, not a requested handle alone.
3. Through normal installed `fabric_exec`, record explicit/alias/default denials
   for both spawn and create, `FabricModelDeniedError` + `FABRIC_MODEL_DENIED`,
   #2236/replacement text, before/after task+actor registries and run directories,
   and absence of a denied worker/task. Check both banned models. Check in-place
   Prewalk (including auto-arm) cannot switch Main or queue an executor turn.
4. Existing resident owners capture configuration: replace/restart affected
   resident owners as well as Main and children. Record old/new owner generation
   and repeat denied/default probes against the new durable owner. A Main reload
   alone is **not** this check; do not silently grandfather stale owners.
5. Retain role-default agreement with the adopted factory generation, exact
   commands and non-secret outputs, worker/actor cleanup receipts, and independent
   `ACCEPTANCE_AUDIT: PASS`. Do not use a source-level mock or the offline provider
   as evidence of fleet model-provider/config adoption.
6. Revert/abort condition: any denied model is admitted, any native child
   model/effort differs from its admitted parent without an explicit override,
   a stale owner remains, or cleanup/rollback cannot be confirmed. Quiesce the
   disposable runs and stop broad rollout; restore the recorded prior reviewed
   Fabric/factory generation and replace its resident owners. Keep the host deny
   policy and fail-closed per-request refusal in place (no silent family fallback).
   Re-run denied calls plus an allowed control, record rollback version/digest,
   native state and cleanup evidence, and leave #2490 open until a new independent
   installed audit passes.
