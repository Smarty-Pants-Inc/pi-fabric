# D3 bridge seam (`fabric-seat-bridge`): design and operator notes

This is fabric-v2's `bridge` seam for the D3 principal-seat move (smarty-dev#15310). It has three phases:
`resume`, `publish` and `rc-restore-route`. A runs it the same way as the drain seam
([d3-drain-seam.md](d3-drain-seam.md)):

```text
/proc/self/fd/N --run-dir RUN --phase resume|publish|rc-restore-route
```

The request is on stdin and the seat lock is in `D3_SEAT_LOCK_FD`, with the same three-step proof.
- A valid receipt (`pass` or `refused`) exits 0 with empty stderr.
- A lock, binding or integrity failure prints no receipt and exits 2 with one `GAP: <code> [owner: fabric-v2]`
  line. A treats that as UNKNOWN.

Each phase writes `fabric-v2-bridge-<phase>-intent.json` before any effect, and `-outcome.json` at the end. Both
are 0600, O_EXCL and read back. A second run of a phase on the same RUN stops at exit 2, so a phase never runs
twice.

## Install (per seat host)

```sh
node <pi-fabric>/dist/seat-bridge/fabric-seat-bridge.cjs --install --out /abs/path/fabric-seat-bridge \
  --link-config /abs/path/to/nats-link/v2.json --link-unit smarty-nats-link.service
```

Three values are written into the installed bytes, so the `sha256` that A pins in `seams.bridge` covers them:
- the interpreter's sha256 (as for the drain seam);
- the ONE nats-link config file the seam may change;
- the link's systemd `--user` unit.

The installer refuses a config the link's own loader refuses, and a unit name that is not `<name>.service`. The
installer and every phase also refuse a link whose `stateDir` is in a temporary store (`link-state-dir-temporary`):
`/tmp`, `/var/tmp`, `/dev/shm`, `/run`, or any agent's private temp store (a `smarty-pi-tmp` path). Temp-store
reaping must never remove a running link's cursor and ledger. An uninstalled bundle refuses every phase
(`seam-not-installed`). Nothing in a request or a packet can point the seam
at another file or unit.

## `resume` (read-only)

Inputs: `{pod: {name, uid}, finalSeal}`, where `finalSeal` is A's whole final seal, exactly seven keys:
`{schema, run, journal, journalSha256, journalBytes, leafId, sealHasThinkingEntry}` (A at 7d2e09af or later).

Observed: `{sessionId, journal, leafId, podUid, appendedSinceSeal, journalSha256, suffixBase64}`.

1. A's final seal comes from RUN itself: `RUN/receipts/final-seal.json`. Its sha256 must be the journal's one
   `final-seal` record. It and the request's `finalSeal` must each have exactly the seven keys, each of its type
   (`sealHasThinkingEntry` true or false only), schema `d3-seat-final-seal-v1`, this run and the sealed source
   journal (else `final-seal-invalid`). Then they must be identical (else `final-seal-mismatch`).
2. The raw Pod is read through the API: the pinned UID, the sealed `target.node`, pi running, and its rendered
   `--session` argument.
3. Through `kubectl exec`, the seam runs the in-image CLI read
   `pi-fabric session drain <sid> --mesh <root> --identity --since <leaf> --sealed-bytes <n>`. The CLI first checks
   its server: the SO_PEERCRED uid is its own, and the pid and start time are those in
   `main-followups/<sid>.owner.json`. Then it reports the Main's session file, leaf and presence owner incarnation.
   It reads the session file itself, read-only, and reports:
   - the last entry (the true leaf);
   - whether the sealed leaf is on the current branch;
   - the entries after the sealed leaf, each exactly `{id, parentId, type, thinkingLevel}`;
   - the file's sha256 and the sha256 of its first `<n>` bytes;
   - the bytes after them, base64, when they are at most 64 KiB.
4. The Pod is read again: same UID, pi still running.
5. If the seal has no thinking entry (`sealHasThinkingEntry: false`), the identity read is valid, and the file is
   still exactly the sealed bytes, the required startup append has not been observed: steps 2-4 are repeated every
   2 s for up to 600 s (`APPEND_WAIT_MS = DEADLINE_MS`), capped by the phase's original deadline. No other state
   is retried; an unreadable identity still refuses immediately.

No receipt (exit 2) when:
- the first `<n>` bytes do not hash to the final seal (`journal-prefix-mismatch`);
- the suffix is over 64 KiB (`journal-suffix-too-large`);
- the sealed leaf is not an ancestor of the current leaf (`branch-diverged`);
- the suffix bytes, parsed by the seam, are not exactly the reported entries chained from the sealed leaf
  (`journal-suffix-not-the-branch`). This check means the seam verifies the CLI's branch read for the appended
  part;
- an appended entry reuses an id of the sealed journal, or repeats one in the tail, or the CLI finds the resumed
  file's branch broken, by a cycle or a missing parent (`resume-tail-id-reused`). Under Pi's later-id-wins index a
  reused id can re-point a sealed entry. The sealed ids come from the seat host's own sealed copy, whose size,
  sha256 and last id must be the seal's (else `journal-prefix-mismatch`), never from the Pod's report. With new ids
  and the chain from the sealed leaf, the tail has no cycle and ends at the reported leaf;
- the seal has no thinking entry and, after the wait in step 5, nothing was appended
  (`resume-thinking-append-missing`). That state is not a completed resume (see below).

A `refused` receipt when:
- the Pod or its Main cannot be read;
- the Main runs another session file than the rendered `--session` (`journal-mismatch`);
- the Main's leaf differs from its file's last entry (`leaf-memory-file-mismatch`).

Pi core appends one `thinking_level_change` entry for an existing conversation whose branch has none: before
`AgentSession` construction and `session_start`, not after NATS contact (pinned Pi 031c7499:
`dist/core/sdk.js:135,346-349,377`). Its session manager generates a new id, parents it to the current leaf and
persists it synchronously (`dist/core/session-manager.js:1592-1600,1346-1396`). A seal without one is still valid
(older sessions), but zero appended entries does not satisfy the seam's completed-resume proof.

**Append is not first NATS contact.** Pi later binds extensions and emits `session_start`
(`dist/core/agent-session.js:5129`). Fabric's handler bootstraps and ensures the runtime (`src/extension.ts`),
which awaits `openFabricTransport` and NATS activation before `initializeMeshActivation` opens `MainDrainServer`
(`src/fabric-runtime-state.ts`). The seam's `podIdentity` calls the in-Pod `session drain --identity` CLI, which
gets the file and in-memory leaf from that local socket and reads the journal itself (`src/cli/session-drain.ts`,
`src/main-drain/control.ts`). A call before NATS activation has exposed the socket can therefore get
`pod-identity-unreadable` and refuse immediately. The missing-append retry does **not** wait for that socket or
for first NATS connect, and this window change does not fix that separate startup-readiness hazard.

**Phase cap versus startup window.** The phase clock starts at seam entry and is capped at 600 s; validation,
initial reads and every later read consume it. The append wait shares that clock: it is not another 600 s after
the first observation, nor a guarantee of 600 s from Pod launch. Each Pod/exec read recomputes its timeout from
the remaining phase time; no new read starts with less than kubectl's 1 s minimum, and an observation finishing at
or after the deadline cannot pass (`phase-deadline`, exit 2). An unchanged readable journal with the required
append still missing at the wait cap gives `resume-thinking-append-missing`. These are deadline checks and child
process timeout budgets, not an OS-level preemption guarantee for synchronous filesystem work or process teardown.
A's separate 900 s cap is not extended.

A judges `appendedSinceSeal` under its rule T-FIX3: it re-reads the host journal from its final seal, checks
`sha256(sealed bytes + decoded suffix) == journalSha256`, and that the suffix parses to exactly
`appendedSinceSeal`. The seam reports the append exactly as found. A test runs the real pinned Pi CLI on a sealed
session and checks the one append and the seam's pass over it (`D3_PINNED_PI`, skipped without it). Fake-clock
regressions cover an append at 150 s (past the old 120 s window), missing appends at the default and a shorter
phase cap, no wait for a seal with thinking metadata, and slow reads consuming the shared cap. Existing divergence
and reused-id cases still require refusal.

## `publish`

Inputs: `{owner: {template, claim, sandbox, pod: {name, uid}}}`.

Observed: `{sessionId, incarnation, route}`, where `route` is `v2@<managed pod UID>:<config sha256, 12 hex>`.

The seat's NATS machine is the sealed run's `natsMachine` (A's T-FIX6), never `instance`, which is the Herdr
agent. A run without it stops at exit 2 (`seat-nats-machine-unpinned`).

1. The seam reads the managed Pod's Main exactly as `resume` does: the API, then the `--identity` read inside the
   Pod. A Pod or Main it cannot verify, a missing owner incarnation, or a Pod whose own `PI_FABRIC_MESH_NATS`
   machine is not the run's `natsMachine`, refuses before anything is written.
2. Under the config lock (see "The A' config writer" below) it reads the pinned link config, plans the one change,
   and writes the intent: the exact change, and the rollback authority. A no-change publish records no rollback
   authority. Then it swaps the file and reads it back.
3. Every later step reads that post-write config, re-parsed from disk. The seam restarts the pinned unit; the
   link reads its allow-list only at start. The restart counts as done only when a NEW process holds the link's
   lock (another pid or start time) and `nats-link status` reports no problem.
4. The read-back, polled for at most 120 s, needs:
   - the link's REAL identity gate (`nats-link dry-run`, with the org.json seat pins) to resolve the seat as `live`
     on the `nats` side, on `natsMachine`. The gate refuses an id with a second record, live or not;
   - exactly ONE NATS presence record for the id, on `natsMachine`, live;
   - the hub's mirror of it under this link, live;
   - both carrying the owner incarnation that the managed Pod's Main reported from inside that Pod.
5. Every other route that `nats-link status` showed live before the restart must be live again.
6. The managed Pod is read once more: same UID, running.

**Why `incarnation` is the managed Pod UID.** A Main's presence incarnation is a random id per process, so no record
carries a Pod UID. The seam proves the link as a chain:
- the API reads the Pod UID before and after;
- `kubectl exec` runs inside that Pod;
- the CLI there verifies its server as this session's owner;
- that Main reports its incarnation;
- the NATS record and the hub mirror carry the same incarnation.

## `rc-restore-route` (rollback)

Inputs: `{recoveryRun, parentRun, parentRunSha256, writer: {sessionId, pid, sessionFile, singleWriter: true}}`.

Observed: `{sessionId, route}`, where `route` is `v1@<pid>:<start ticks>`.

1. The seat's mesh root, from the sealed journal and Fabric config, must be the link's `file.mesh`.
2. The change to undo is publish's recorded rollback authority in this RUN, under the config lock:
   - the file still holds publish's bytes: compare-and-swap back to the recorded old bytes, exactly;
   - the file holds the old bytes: no change;
   - anything else (another seat changed it since): an entry-level undo of exactly publish's entry, re-validated.
     The seat's nats entry must be the one publish wrote, and its old file entry goes back at its index;
   - no authority (publish wrote nothing): no change, no restart. A nats entry for the seat that publish did not
     write refuses (`seat-entry-conflict`); the seam never removes it.
3. Write, restart and reload as for publish. After a restore, the one status problem that still counts as
   reloaded is the removed seat's own old mirror row (`mirror not on allow.nats: session:<sid>`).
4. Every other route comes back, as for publish.
5. The relaunched v1 is checked over its owner-only drain channel, the server verified as above:
   - its pid is `writer.pid`;
   - that pid's start time is the owner record's;
   - its session file is `writer.sessionFile`;
   - it reports an owner incarnation.
6. The hub's live record for the session is native (not a mirror) with that incarnation. When the seat is on
   `allow.file` after the restore, the link's identity gate must resolve it `live`, and exactly one NATS record
   exists for it, the link's mirror on the link's own machine.
7. No live NATS record for the session remains on any other machine (`v2-presence-live`).

## The A' config writer (`src/seat-bridge/allow-writer.ts`)

It changes exactly ONE allow-list entry of the pinned nats-link config, the moving seat's root. It never makes a
free-form edit.

- **The path** has no symlink in any component (its real path is itself). Every directory from `/` down is root's or
  the seat user's and not group/other-writable; a sticky shared directory is allowed only above the config's own
  directory. The file is regular, root's or the seat user's, one link, not group/other-writable, at most 1 MiB.
  Every read, the temp file and the rename go through a descriptor of that verified directory, whose identity must
  still be the one the plan read.
- **One config-scoped lock** is held across read, plan, intent, check, rename and read-back by every seam write, on
  any seat. It is an `flock` on one stable file beside the config, `.<config name>.fabric-v2.lock`, which is opened
  through the verified directory and never removed. The file must be regular, have one link, belong to root or the
  seat user, and not be group- or other-writable. Node has no `flock`, so `/usr/bin/python3` takes the lock on the
  seam's own open file description. The lock is released when the seam closes that descriptor or dies, so a crash
  leaves no stale lock and nothing is ever reclaimed. A live holder is waited for, at most 30 s.
- **The entry** has one fixed shape, built only from sealed values:
  - `id` is `session:<run sessionId>`;
  - `principal` is the org.json principal whose seat is that session;
  - `machine` is the run's `natsMachine`, which must be in `nats.pathBMachines` and is never the link's own machine;
  - `send` is `["followUp"]` and `receive` is `["followUp", "cancel"]`, both reviewed constants.
- **The pre-state** decides the change:
  - the same nats entry already there means no change;
  - the seat's own file root entry is MOVED: removed from `allow.file` and the fixed entry appended to `allow.nats`;
  - no entry means it is ADDED;
  - any other entry naming the session (another nats entry, or an actor under the seat's root) refuses.
- **The candidate must pass the link's own full validation**, including the org.json seat check that `nats-link start`
  makes.
- **A deep diff of the old and the candidate must be exactly that change.** Every key outside `allow` is deep-equal.
  `allow.nats` gains or loses one element. `allow.file` is unchanged, or loses or regains only the seat's element.
  Every other entry keeps its value and order.
- **The swap is compare-and-swap.** The file must still hash to the planned old bytes, both before the temp write
  and just before the rename. The temp file is private and in the same directory. The temp file and the directory
  are fsynced, and the result is read back byte for byte. A failure before the rename leaves the old file as it was.

A publish rewrites the file as indented JSON: its values are the guarded property, and the bytes may differ in
layout only. A rollback while the file still holds publish's bytes restores the old bytes exactly, layout included.

## Tested on a real broker and link

`tests/nats-auth-path-b-bridge-seam.test.ts` (CI's `nats-auth` job, pinned nats-server) runs `publish` and
`rc-restore-route` against a real authenticated broker, the real A' link in-process and real FILE and NATS Mains. The
link's readers, its identity gate and the message paths are the product's own. It covers publish, the route through
the real gate, one message to the moved seat, the rollback to a relaunched v1 seat with the old bytes back exactly,
and one message on the v1 route. It also covers a stale mirror row: the gate refuses the seat and publish refuses.
Three things are test-only deps that the installed bundle cannot reach: kubectl, the unit restart (it restarts the
in-process link) and one exact state directory counted as durable. The test deletion guard keeps the link's state
in the test's private temp store, which the production rule refuses. That test also shows the refusal without the
exemption. `tests/seat-bridge-seam.test.ts` checks the built bundle: one call to the seam with the pins and no deps.

## Preconditions (each is checked live in the test-seat runbook)

- The link runs as the seat user under the pinned `systemd --user` unit, with `Restart=on-failure`, and its
  `stateDir` is a durable path.
- The sealed run names the seat's `natsMachine`, and that machine is in the link config's `nats.pathBMachines`.
- The link config's path has no symlink component, so the installer must be given its real path.
- The seat image carries a pi-fabric CLI with `session drain --identity`, which is this change, and
  `/usr/bin/python3`.
- `/usr/bin/systemctl` is root-owned.

## Residuals

- **A6 (A's seat contract):** a same-UID process could alter the seam, its interpreter or the link config between
  check and use. The config write is compare-and-swap, so an edit between plan and swap is refused.
- **The restart interrupts every route the link carries** for a few seconds. The link resumes from its cursor and
  ledger, and the seam re-checks each route afterwards.
- **The incarnation chain** trusts the in-image CLI's report of its server's incarnation. The CLI verifies that
  server by SO_PEERCRED and the owner record. A same-UID process in the Pod that rewrites the owner record and
  serves a fake socket is the same A6-class residual as for `resume-drain`.
- **The journal read happens in the Pod.** The seam checks the CLI's branch read against the suffix bytes, but the
  hashes of the sealed prefix and of the whole file are the CLI's measurement.
