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

Inputs: `{pod: {name, uid}, finalSeal: {journalSha256, journalBytes, leafId}}`.

Observed: `{sessionId, journal, leafId, podUid, appendedSinceSeal, journalSha256, suffixBase64}`.

1. A's final seal comes from RUN itself: `RUN/receipts/final-seal.json`. Its sha256 must be the journal's one
   `final-seal` record. It must name this run and the sealed source journal, and equal the request's `finalSeal`.
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

No receipt (exit 2) when:
- the first `<n>` bytes do not hash to the final seal (`journal-prefix-mismatch`);
- the suffix is over 64 KiB (`journal-suffix-too-large`);
- the sealed leaf is not an ancestor of the current leaf (`branch-diverged`);
- the suffix bytes, parsed by the seam, are not exactly the reported entries chained from the sealed leaf
  (`journal-suffix-not-the-branch`). This check means the seam verifies the CLI's branch read for the appended
  part.

A `refused` receipt when:
- the Pod or its Main cannot be read;
- the Main runs another session file than the rendered `--session` (`journal-mismatch`);
- the Main's leaf differs from its file's last entry (`leaf-memory-file-mismatch`).

Pi core appends one `thinking_level_change` entry on resume when the sealed branch has none. A judges
`appendedSinceSeal` under its rule T-FIX3. The seam reports it exactly as found.

## `publish`

Inputs: `{owner: {template, claim, sandbox, pod: {name, uid}}}`.

Observed: `{sessionId, incarnation, route}`, where `route` is `v2@<managed pod UID>:<config sha256, 12 hex>`.

1. The seam reads the pinned link config and plans the one change (see "The A' config writer" below). The intent,
   written before any effect, records that exact change, including the old and new bytes and the seat's old
   file entry with its index.
2. It reads the managed Pod's Main exactly as `resume` does: the API, then the `--identity` read inside the Pod. A
   Pod or Main it cannot verify, or a missing owner incarnation, refuses before anything is written.
3. It writes the config, then restarts the pinned unit. The link reads its allow-list only at start. The restart
   counts as done only when a NEW process holds the link's lock (another pid or start time) and `nats-link status`
   reports no problem.
4. The read-back, polled for at most 120 s, needs:
   - the seat's identity on the `nats` side;
   - exactly ONE live NATS presence record, on the seat's own Path B machine (`run.instance`);
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
2. The change to undo is the one publish recorded in this RUN. The nats entry must be exactly the entry publish
   wrote, and the seat's old file entry goes back at its index. With no publish write in this RUN, nothing changes:
   no write and no restart.
3. Write, restart and reload as for publish. After a restore, the one status problem that still counts as
   reloaded is the removed seat's own old mirror row (`mirror not on allow.nats: session:<sid>`).
4. Every other route comes back, as for publish.
5. The relaunched v1 is checked over its owner-only drain channel, the server verified as above:
   - its pid is `writer.pid`;
   - that pid's start time is the owner record's;
   - its session file is `writer.sessionFile`;
   - it reports an owner incarnation.
6. The hub's live record for the session is native (not a mirror) with that incarnation. When the seat's file entry
   was restored, exactly one live NATS record exists for it, the link's mirror on the link's own machine.
7. No live NATS record for the session remains on any other machine (`v2-presence-live`).

## The A' config writer (`src/seat-bridge/allow-writer.ts`)

It changes exactly ONE allow-list entry of the pinned nats-link config, the moving seat's root. It never makes a
free-form edit.

- **The entry** has one fixed shape, built only from sealed values:
  - `id` is `session:<run sessionId>`;
  - `principal` is the org.json principal whose seat is that session;
  - `machine` is `run.instance`, which must be in `nats.pathBMachines` and is never the link's own machine;
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

The writer rewrites the file as indented JSON. Its values are the guarded property, and the bytes may differ in
layout only.

## Preconditions (each is checked live in the test-seat runbook)

- The link runs as the seat user under the pinned `systemd --user` unit, with `Restart=on-failure`, and its
  `stateDir` is a durable path.
- The seat's Path B machine is in the link config's `nats.pathBMachines`.
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
