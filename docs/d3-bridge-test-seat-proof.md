# D3 bridge seam: test-seat proof runbook

Status: NOT RUN. fabric-v2 runs this on the synthetic test seat (`d3synth`) with the real nats-link (A') and the
island Pods, after the seat image is built from this change (smarty-dev#15310). Nothing here touches a real
principal's seat. The design is in [d3-bridge-seam.md](d3-bridge-seam.md); the drain seam's runbook is
[d3-drain-test-seat-proof.md](d3-drain-test-seat-proof.md), and this one reuses its §0 helpers (`j`, `k`, `hex`,
`req`, `seam`).

Every step lists its command, the expected output and what counts as FAIL. A FAIL stops the run: record the output
and use §8. Run everything as the seat user on the seat host, in one bash shell, in this order.

## 0. Variables and pins

Set the drain runbook's §0 variables first (`RUN`, `MOVE`, `CUTOVER`, `j`, `k`, `hex`, `SID`, `NS`, `KCFG`, ...),
then:

```sh
BRIDGE=$(j "$RUN/seat-run.json" seams bridge path); BRIDGESHA=$(j "$RUN/seat-run.json" seams bridge sha256)
MACHINE=$(j "$RUN/seat-run.json" natsMachine)          # A's T-FIX6; never the Herdr instance
LINKCFG=/abs/path/to/the/nats-link/config.json      # the file the bridge seam was installed with
UNIT=smarty-nats-link.service                         # the unit the bridge seam was installed with
LINK() { node "$PIF" nats-link "$@" --config "$LINKCFG"; }
userctl() { systemctl --user "$@"; }
```

| step | command | expected | FAIL |
|---|---|---|---|
| 0.1 seam pin | `sha256sum "$BRIDGE"; stat -c '%U %a' "$BRIDGE"; grep -c -F "\"$LINKCFG\"" "$BRIDGE"; grep -c -F "\"$UNIT\"" "$BRIDGE"` | the sha equals `$BRIDGESHA`; the seat user and `500`; `1`; `1` | another sha, mode or owner; a `0` (the seam is pinned to another config or unit) |
| 0.2 machine | `echo "$MACHINE"; j "$LINKCFG" nats pathBMachines` | a machine name (the run has `natsMachine`), and a list that contains it, which is not `j "$LINKCFG" nats machine` | an empty `$MACHINE`: A predates T-FIX6 and publish stops on `GAP: seat-nats-machine-unpinned`. Not in the list: publish stops on `GAP: seat-machine-not-path-b`; add it to the link's inventory first (its own reviewed step) |
| 0.2b real path | `realpath "$LINKCFG"` | exactly `$LINKCFG` | another path: a symlink component; install the seam with the real path |
| 0.3 shared mesh | `realpath "$(j "$LINKCFG" file mesh)"; echo "$MESH"` (the drain runbook's `MESH`, realpath) | the same path twice | different: rc-restore-route would stop on `GAP: seat-mesh-not-link-mesh` |
| 0.4 systemctl | `stat -c '%U %a %n' "$(readlink -f /usr/bin/systemctl)"` | `root 755 ...` | other owner or mode |
| 0.5 durable link state | `j "$LINKCFG" stateDir` | a durable path, for example `~/.local/state/smarty/nats-link/v2` | a path in `/tmp`, `/var/tmp`, `/dev/shm`, `/run` or any agent's private temp store (`smarty-pi-tmp`): temp-store reaping could remove the link's cursor and ledger. Move it first; the seam's install and every phase refuse it (`link-state-dir-temporary`) |

## 1. The link unit and the routes that share it (precondition)

```sh
userctl show "$UNIT" -p ActiveState -p SubState -p Restart -p ExecStart -p User
LINK status | python3 -I -c 'import json,sys
s=json.load(sys.stdin); print("ok", s["ok"], "problems", s["problems"])
for side in ("file","nats"):
  for row in s["allow"][side]: print(side, row["identity"], "live", row["live"])'
cat "$(j "$LINKCFG" stateDir)/link.lock"; echo
```

Expected:
- `ActiveState=active`, `SubState=running`, `Restart=on-failure`, and an `ExecStart` that runs
  `nats-link start --config $LINKCFG`;
- `ok True problems []`;
- one line per allow entry, each `live True`. **Record this list: these are the routes that share the link.** On the
  test seat they are the moving seat's own file entry (when it has one) and the other allow-listed identities;
- the lock names the link's pid, its start time and `"purpose":"start"`.

FAIL: no unit (create it first, as the link's canary does), the unit inactive, a status problem, or an entry not
live.

## 2. The Pod image (resume and publish read through it)

On a running Pod of the seat image (any before §3; the managed Pod after it):

```sh
POD="<pod name>"
k -n "$NS" exec "$POD" -c pi -- /opt/pi-fabric/dist/cli/index.js session drain --help | grep -c -- --identity
k -n "$NS" exec "$POD" -c pi -- /usr/bin/python3 -V
k -n "$NS" get pod "$POD" -o jsonpath='{.spec.containers[?(@.name=="pi")].env[?(@.name=="PI_FABRIC_MESH_NATS")].value}'; echo
```

Expected: at least `1` (this change's CLI), then `Python 3.<minor>.<patch>`, then a JSON object whose `machine` is
`$MACHINE`. FAIL: `0` (the image's pi-fabric predates the identity read: both phases would refuse
`pod-identity-unreadable`), no python3, or another machine (publish refuses `pod-nats-machine-mismatch`).

## 3. The forward run: `resume` and `publish` (A runs them)

Snapshot the link config and its routes, then run A:

```sh
cp -p "$LINKCFG" "$RUN/../link-config-before.json"; sha256sum "$LINKCFG"
python3 "$MOVE" cutover-seat --run-dir "$RUN" --run-sha256 "$RUNSHA" --inputs-sha256 "$INSHA" --execute; echo "exit=$?"
```

Expected: `exit=0`. FAIL: exit 2 with `[owner: fabric-v2]` (use §8), or another owner's GAP.

Then check the bridge receipts, the evidence and the config change:

```sh
python3 -I - "$RUN" "$LINKCFG" "$RUN/../link-config-before.json" <<'EOF'
import base64, hashlib, json, os, sys
run, cfg, before_file = sys.argv[1:4]
sealed = json.load(open(os.path.join(run, "seat-run.json")))
journal = [json.loads(l) for l in open(os.path.join(run, "seat-journal.jsonl")) if l.strip()]
seen = {(r.get("seam"), r.get("phase")): r.get("sha256") for r in journal if r.get("event") == "seam"}
def receipt(phase):
    raw = open(os.path.join(run, "receipts", phase + ".json"), "rb").read(); r = json.loads(raw)
    ok = seen.get(("bridge", phase)) == hashlib.sha256(raw).hexdigest() and r["owner"] == "fabric-v2" and r["result"] == "pass"
    print(phase, "OK" if ok else "FAIL"); return r["observed"]
seal = json.load(open(os.path.join(run, "receipts", "final-seal.json")))
r = receipt("resume")
suffix = base64.b64decode(r["suffixBase64"])
print("resume keys", "OK" if set(r) == {"sessionId", "journal", "leafId", "podUid", "appendedSinceSeal", "journalSha256", "suffixBase64"} else "FAIL")
print("resume appended", json.dumps(r["appendedSinceSeal"]), "suffix bytes", len(suffix),
      "first parent", "OK" if not r["appendedSinceSeal"] or r["appendedSinceSeal"][0]["parentId"] == seal["leafId"] else "FAIL")
p = receipt("publish")
print("publish", json.dumps(p))
ev = lambda n: json.load(open(os.path.join(run, "fabric-v2-bridge-" + n + ".json")))
plan = ev("publish-intent")["plan"]; out = ev("publish-outcome")
print("plan", plan["change"], "entry", json.dumps(plan["natsEntry"]), "moved", json.dumps(plan["fileEntry"]))
before, after = json.load(open(before_file)), json.load(open(cfg))
same = {k for k in set(before) | set(after) if k != "allow" and before.get(k) != after.get(k)}
print("config outside allow", "OK" if not same else "FAIL " + ",".join(sorted(same)))
print("config sha", "OK" if hashlib.sha256(open(cfg, "rb").read()).hexdigest() == plan["afterSha256"] else "FAIL")
print("reload", json.dumps(out["measured"]["reload"]))
print("other routes", json.dumps(out["measured"]["otherRoutes"]["checked"]), "down", json.dumps(out["measured"]["otherRoutes"]["down"]))
EOF
```

Expected:
- `resume OK`, `resume keys OK`, `publish OK`;
- `resume appended`: `[]`, or exactly one `thinking_level_change` whose `parentId` is the final seal's leaf (`first parent OK`), and
  the suffix bytes are that one line;
- `publish`: `incarnation` is the managed Pod UID and `route` starts with `v2@<that UID>:`;
- `plan move` (the seat had a file entry) or `plan add`; the entry is
  `{kind: root, id: session:<SID>, principal: <the seat's org.json principal>, machine: $MACHINE, send: [followUp], receive: [followUp, cancel]}`;
- `config outside allow OK`, `config sha OK`;
- `reload`: `restartExit 0`, `statusOk true`, a `before` and an `after` with different pids;
- `other routes`: every route recorded in §1 except the seat's own, and `down []`.

FAIL: any `FAIL`, a non-empty `down`, or more than one appended entry. More than one is A's rule to judge, but record
it.

### 3.1 The routes after the reload, read directly

```sh
LINK status | python3 -I -c 'import json,sys
s=json.load(sys.stdin); print("ok", s["ok"], "problems", s["problems"])
for side in ("file","nats"):
  for row in s["allow"][side]: print(side, row["identity"], "live", row["live"])'
LINK receipt --session "$SID" --nats
node "$PIF" nats-link roots --config "$LINKCFG" --include-stale | python3 -I -c 'import json,sys
for r in json.load(sys.stdin):
  if r["id"] == "session:'"$SID"'": print(r["machine"], r["live"], r["ownerIncarnation"])'
userctl show "$UNIT" -p ExecMainStartTimestamp -p NRestarts
```

Expected:
- `ok True problems []`, and every route from §1 live again, with the seat now under `nats`;
- the receipt names the hub mirror (`remoteHost` = the link) live, with the incarnation the publish evidence shows
  (`asserted.identity.ownerIncarnation`). Its overall verdict may be `not-routable` only because of the old v1
  mirror key: `problems` then names "several NATS presence records". Record it;
- `roots`: exactly one live line, `$MACHINE True <incarnation>`; at most one other, not live, under the link's own
  machine (the old v1 mirror key);
- the unit restarted once in this run.

FAIL: a route not live, a second live record, or another incarnation.

### 3.2 A message through the new route

From a Fabric session on the hub mesh that is on the link's allow-list as a sender, send one follow-up to
`session:<SID>` and read it back with `LINK receipt --command <commandId>` (`acknowledged`). FAIL: anything else.

## 4. Live negative probes

| probe | command | expected | FAIL |
|---|---|---|---|
| 4.1 replay | the drain runbook's `req`/`seam` helpers, `phase publish`, with the held lock: `exec 9<"$RUN/seat.lock"; flock -x -n 9; req "$(j "$RUN/seat-run.json" run)" "$RUNSHA" publish d3synth "$SID" "{\"owner\":$(j "$RUN/fabric-v2-bridge-publish-intent.json" owner)}" \| env -i PATH=/usr/bin:/bin LANG=C LC_ALL=C D3_SEAT_LOCK_FD=9 "$BRIDGE" --run-dir "$RUN" --phase publish; echo "exit=$?"; exec 9<&-` | no stdout, `GAP: evidence-write-failed [owner: fabric-v2]`, `exit=2`, and the config sha unchanged | a receipt, or a config change |
| 4.2 no lock | 4.1 without `flock` (and with `exec 9<` only) | `GAP: lock-not-held [owner: fabric-v2]`, `exit=2` | a receipt |
| 4.3 identity from a forked process | in the managed Pod: `k -n "$NS" exec "$POD" -c pi -- sh -c '/opt/pi-fabric/dist/cli/index.js session drain '"$SID"' --mesh /var/lib/fabric/mesh --identity; echo rc=$?'` | an identity line and `rc=0`: the identity read is read-only and needs no caller proof (record it) | a line with another `rootId` |

In table cells, `\|` is the shell pipe `|`.

## 5. Rollback rehearsal: `rc-restore-route`

On the synthetic seat only, after §3 and §4, run A's rollback:

```sh
python3 "$MOVE" rollback-seat --run-dir "$RUN" --execute; echo "exit=$?"
```

Expected: `exit=0`. Then:

```sh
python3 -I - "$RUN" "$LINKCFG" "$RUN/../link-config-before.json" <<'EOF'
import json, os, sys
run, cfg, before_file = sys.argv[1:4]
r = json.load(open(os.path.join(run, "receipts", "rc-restore-route.json")))
print("receipt", r["result"], json.dumps(r["observed"]))
out = json.load(open(os.path.join(run, "fabric-v2-bridge-rc-restore-route-outcome.json")))
print("reason", out["reason"], "reload", json.dumps(out["measured"]["reload"]), "v2Live", json.dumps(out["measured"]["v2Live"]))
before, after = json.load(open(before_file)), json.load(open(cfg))
print("bytes back", "OK" if open(cfg, "rb").read() == open(before_file, "rb").read() else "CHECK",
      "change", json.load(open(os.path.join(run, "fabric-v2-bridge-rc-restore-route-intent.json")))["plan"]["change"])
EOF
```

Expected: `receipt pass` with `route` = `v1@<relaunched pid>:<start ticks>`, `reason None`, `v2Live []`, and
`bytes back OK change revert`: the config is the pre-publish file, byte for byte. `CHECK` with change `restore` or
`remove` means another seat changed the file in between, and the seam undid only its own entry: compare the seat's
own entries by hand.
Repeat §3.1's status read: every route from §1 live, the seat back under `file`, and no live NATS record on
`$MACHINE`.

FAIL: refused, a live v2 record, or a route down.

## 6. The reload gap (the link canary)

The link canary measures the reload gap with a plain `userctl restart "$UNIT"`. That is the same restart the seam
runs. Record its result next to this run's `reload.waitedMs`.

## 7. Read-backs to record

Post these on smarty-dev#15310 with the run id, the UTC time and the pass or FAIL of each step:
- the bridge seam's path, sha256, mode and pins (0.1);
- the link config sha before, after publish and after rollback;
- the unit's state and restart count (§1, §3.1);
- the routes before and after each reload (§1, §3, §3.1, §5);
- the resume receipt (appended entries, journal sha, suffix size) and the final seal;
- the publish receipt, its plan (change, entry, moved entry) and its reload evidence;
- §3.1's receipt and roots lines;
- the §4 outputs;
- the rc-restore-route receipt and outcome.

## 8. Triage

| seen | meaning | owner |
|---|---|---|
| `GAP: seam-not-installed` | the bundle was run without `--install` | fabric-v2 |
| `GAP: link-config-invalid` or `-unsafe` | the pinned config fails the link's own loader, or is not a safe file | the link's operator |
| `GAP: link-state-dir-temporary` | the link's `stateDir` is in a temporary store | the link's operator |
| `GAP: seat-machine-not-path-b` | the seat's machine is missing from `nats.pathBMachines` | the link's operator |
| `GAP: seat-principal-unbound` | no org.json principal has this session as its seat | the org.json owner |
| `GAP: seat-entry-conflict` | another entry already names the seat; the seam never edits it | the link's operator |
| `GAP: link-config-changed` | the config changed between the plan and the swap; nothing was written | rerun only after reconciling |
| `GAP: final-seal-*` | A's final seal is missing, unjournaled, not a valid seven-key seal in RUN or in the request (`sealHasThinkingEntry` must be true or false), or differs from the request | A's owner |
| `GAP: journal-prefix-mismatch` | the Pod's journal no longer starts with the sealed bytes | stop: the copy is not the sealed journal |
| `GAP: branch-diverged` or `journal-suffix-*` | the resumed session moved off the sealed branch, or grew too much | stop |
| `GAP: resume-tail-id-reused` | an entry appended after the seal reuses an id of the sealed journal or of the tail, or the resumed file's branch is broken (a cycle or a missing parent). Pi's own startup append always has a new id; this is never a resume | stop |
| `GAP: resume-thinking-append-missing` | the seal has no thinking entry and, after 120 s of re-reads, the Pod's journal still holds only the sealed bytes: Pi has not opened the session (a real resume appends exactly one `thinking_level_change` at start). Check that pi runs with the rendered `--session` and has started; this is not a completed resume | fabric-v2 |
| `GAP: seat-mesh-not-link-mesh` | the v1 seat's mesh is not the link's file mesh | fabric-v2 |
| `refused`, `pod-identity-unreadable` | the Pod's CLI lacks `--identity`, or its server check failed: read `asserted.podLine` | the image owner, or fabric-v2 |
| `refused`, `link-not-reloaded` | no new lock holder, or a status problem: read `measured.reload` | the link's operator |
| `refused`, `seat-not-routable` | no single live record on the seat's machine with the Pod Main's incarnation: read `measured.receipt` | fabric-v2 |
| `refused`, `seat-route-refused-by-gate` | the link's identity gate does not resolve the seat: read `measured.gate`. An old presence row for the id on a second machine (for example a mirror row a killed link left), live or not, hides the id from the NATS directory, so the gate says `BRIDGE_NATS_TARGET_UNAVAILABLE`, "has no nats presence", even though the seat's live row is there. Check for an extra row before anything else; a presence row expires within its 10-minute TTL. The seam waits 120 s for the route and then refuses; the config change it made STAYS (it is not rolled back automatically), so roll back with `rc-restore-route` or rerun once the row is gone | fabric-v2 |
| `refused`, `pod-nats-machine-mismatch` or `-unknown` | the managed Pod's `PI_FABRIC_MESH_NATS` does not name the run's `natsMachine` | A's owner |
| `GAP: seat-nats-machine-unpinned` | the sealed run has no `natsMachine` (A predates T-FIX6) | A's owner |
| `GAP: link-config-path-unsafe` | a symlink or an unsafe directory on the config's path | the link's operator |
| `GAP: link-config-busy` | another seam write held the config lock for over 30 s | rerun only after reconciling |
| `GAP: link-config-lock-unsafe` or `link-config-lock-failed` | the lock file beside the config (`.<name>.fabric-v2.lock`) is not a private regular file with one link, was replaced while locked, or `/usr/bin/python3` could not take the lock | the link's operator |
| `refused`, `other-routes-down` | a route that shared the link did not come back: read `measured.otherRoutes.down` | fabric-v2, at once |
| `refused`, `v1-*` or `v2-presence-live` | the relaunched v1 is not the live owner, or a v2 record is still live | A's owner, fabric-v2 |
