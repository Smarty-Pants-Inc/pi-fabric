# D3 drain seam: test-seat proof runbook

Status: NOT RUN. fabric-v2 runs this on the synthetic test seat (`d3synth`) after image cut 2 (smarty-dev#15310).
Nothing here touches a real principal's seat. The operator notes are in [d3-drain-seam.md](d3-drain-seam.md).

The order is:
1. §0 pins;
2. §1 RBAC;
3. §2 the in-Pod image checks;
4. §3 live probes against the v1 Main through a probe RUN;
5. §4 the forward run;
6. §5 the in-Pod caller probe and a replay after the run;
7. §6 the read-backs to record.

Every step lists its command, the expected output and what counts as FAIL. A FAIL stops the run. Record the failing
output and use §7.

Run everything as the seat user on the seat host, in one bash shell, in this order. Commands that run inside the Pod
go through `k ... exec`.

## 0. Variables and pins

```sh
RUN=/abs/path/to/the/forward/RUN        # sealed by hosts-lead's preflight; move.py has not run on it yet
REPO=/abs/path/to/A                     # the tree with A's rev-4 setup/org/golive/move.py
PIF=/abs/path/to/pi-fabric/dist/cli/index.js   # the CLI of the pi-fabric release the v1 Main runs
j() { python3 -I -c 'import json,sys
d=json.load(open(sys.argv[1]))
for k in sys.argv[2:]: d=d[k]
print(d if isinstance(d,str) else json.dumps(d))' "$@"; }
SID=$(j "$RUN/seat-run.json" sessionId);  PRINCIPAL=$(j "$RUN/seat-run.json" principal)
CTX=$(j "$RUN/seat-run.json" context);    NS=$(j "$RUN/seat-run.json" namespace)
SEAM=$(j "$RUN/seat-run.json" seams drain path);  SEAMSHA=$(j "$RUN/seat-run.json" seams drain sha256)
KCFG=$(j "$RUN/seat-inputs.json" kube kubeconfig)
RUNSHA=$(sha256sum "$RUN/seat-run.json" | cut -d' ' -f1);  INSHA=$(sha256sum "$RUN/seat-inputs.json" | cut -d' ' -f1)
KUBECTL=$(readlink -f /usr/local/bin/kubectl || readlink -f /usr/bin/kubectl)
k() { env -i PATH=/usr/local/bin:/usr/bin:/bin LANG=C LC_ALL=C HOME="$HOME" KUBECONFIG="$KCFG" \
  "$KUBECTL" --kubeconfig "$KCFG" --context "$CTX" "$@"; }
hex() { python3 -I -c "import secrets; print(secrets.token_hex($1))"; }
uuid() { python3 -I -c 'import uuid; print(uuid.uuid4())'; }
```

`MESH` is the v1 Main's mesh root. The seam derives it from the `cwd` in the first line of the sealed source journal
(`j "$RUN/seat-run.json" source journal`). It uses `mesh.root` from `<cwd>/.pi/fabric.json`, then the agent dir's
`fabric.json`, then `<cwd>/.pi/fabric/mesh`. Set `MESH` to that path; step 0.2 checks it.

| step | command | expected | FAIL |
|---|---|---|---|
| 0.1 seam pin | `sha256sum "$SEAM"; stat -c '%U %a' "$SEAM"; head -c 200 "$SEAM" \| head -1` | the sha equals `$SEAMSHA`; the seat user and `500`; `#!/abs/node` | another sha, mode or owner |
| 0.2 v1 Main | `node "$PIF" session drain "$SID" --mesh "$MESH" --status` | exit 0 and one JSON line, described below | exit 1, or any other value |
| 0.3 A is rev 4 | `grep -c drain-operation "$REPO/setup/org/golive/seat_cutover.py"; grep -c drain-recheck "$REPO/setup/org/golive/seat_cutover.py"` | both at least 1 | `0`: this A predates rev 4, and `drain` would stop on `GAP: operation-id-invalid` |
| 0.4 host python3 | `stat -c '%U %a %n' /usr/bin/python3` | `root 755 /usr/bin/python3` | missing, or not root-owned |
| 0.5 host ptrace | `cat /proc/sys/kernel/yama/ptrace_scope` | record the value (`1` expected) | none; `0` is recorded as the A6-class residual |

In table cells, `\|` is the shell pipe `|`.

The 0.2 line must show:
- `reply.rootId` is `session:<SID>`, and `reply.meshRoot` is the realpath of `$MESH`;
- `reply.binding` is `null`, and `reply.inbox.closed` is `false`;
- `reply.native` has `inputAdmission: 1`, `capability: true`, `fenceApi: true` and `piInputsFenced: false`.
  `capability: false` means the v1 Pi lacks pi#309, and the drain would refuse `fence-unavailable`;
- `reply.actors.owned` is `[]`.

A non-empty `reply.actors.owned` means the seat has session or durable actors. A durable one refuses the drain
(`durable-actor-present`). The probe drain in §3 would stop a session one for good. On such a seat, skip §3 and
record why.

## 1. RBAC and the pinned kubeconfig (a)

```sh
k -n "$NS" auth can-i create pods/exec; echo "exit=$?"
k -n "$NS" auth can-i get pods; echo "exit=$?"
k config view --minify --raw -o json | python3 -I -c 'import base64,hashlib,json,sys
c=json.load(sys.stdin)["clusters"]; print(len(c)); c=c[0]["cluster"]; print(c["server"])
print(hashlib.sha256(base64.b64decode(c["certificate-authority-data"])).hexdigest()); print(c.get("insecure-skip-tls-verify", False))'
j "$RUN/seat-inputs.json" kube server; j "$RUN/seat-inputs.json" kube caSha256
stat -c '%U %a %n' "$KUBECTL"
```

Expected:
- `yes` and `exit=0`, twice;
- `1`, the server, the CA sha and `False`. The server and CA sha equal the two pinned lines that follow;
- `root 755 <kubectl>`.

FAIL: `no`, any error, more than one cluster, a server or CA mismatch, a TLS bypass, or a kubectl that root does not
own. Each of these makes `resume-drain` stop at exit 2 (`kubeconfig-unpinned`, `kubeconfig-ca-mismatch`,
`kubectl-unsafe`).

## 2. The cut-2 image inside a Pod (b)

Run this on a running Pod of the cut-2 seat image in `$NS`. Before the forward run, use any such Pod. After it, use
the managed Pod from §4. `resume-drain` repeats the `test -x` itself.

```sh
POD="<pod name>"
k -n "$NS" exec "$POD" -c pi -- /usr/bin/python3 -V
k -n "$NS" exec "$POD" -c pi -- sh -c 'command -v python3; stat -c "%U %a" /usr/bin/python3'
k -n "$NS" exec "$POD" -c pi -- test -x /opt/pi-fabric/dist/cli/index.js; echo "test-x=$?"
k -n "$NS" exec "$POD" -c pi -- readlink -f /usr/local/bin/pi-fabric
k -n "$NS" exec "$POD" -c pi -- node -p 'require("/opt/pi-fabric/package.json").version'
k -n "$NS" exec "$POD" -c pi -- cat /proc/sys/kernel/yama/ptrace_scope
k -n "$NS" exec "$POD" -c pi -- awk '/^PPid/ {print $2}' /proc/self/status
k -n "$NS" exec "$POD" -c pi -- sh -c 'awk "/^PPid/ {print \$2}" /proc/self/status; true'
k -n "$NS" get pod "$POD" -o jsonpath='{.spec.containers[?(@.name=="pi")].env[?(@.name=="PI_FABRIC_MESH_ROOT")].value}'; echo
```

Expected, line by line:
1. `Python 3.<minor>.<patch>` (img-g9 measured 3.12.3);
2. `/usr/bin/python3`, then `root 755`;
3. `test-x=0`;
4. `/opt/pi-fabric/dist/cli/index.js`;
5. the pi-fabric version that cut 2 pins (record it);
6. the ptrace scope (record it);
7. `0`. A process that `kubectl exec` starts has PPid 0. This is the premise of the pod caller proof;
8. a nonzero pid, because the process is a child of `sh`;
9. `/var/lib/fabric/mesh`.

FAIL:
- no python3: pod bindings refuse with `PEER_UNVERIFIED`, so `resume-drain` cannot pass;
- `test-x` is not 0;
- line 7 is not `0`: this runtime does not start exec'd processes with PPid 0, and the caller proof does not hold;
- line 8 is `0`;
- an unexpected version or mesh root.

## 3. Live probes against the v1 Main, through a probe RUN (d, host part)

Run this BEFORE the forward run, while v1 is live and idle, and while the seat gets no input for about two minutes.

The probe RUN is the operator's own run directory, a sibling of `RUN` with a new run id. It is never given to
`move.py`. The Main binds one operation at a time, and 3.10 frees it again before §4.

### 3.1 Set up the probe RUN

```sh
make_probe() { python3 -I - "$RUN" "$1" <<'EOF'
import hashlib, json, os, secrets, sys
src, dst = sys.argv[1], sys.argv[2]
os.mkdir(dst, 0o700)
run = json.load(open(os.path.join(src, "seat-run.json")))
run["run"] = secrets.token_hex(16)
run_bytes = json.dumps(run).encode()
inputs = json.load(open(os.path.join(src, "seat-inputs.json")))
inputs["run"], inputs["runSha256"] = run["run"], hashlib.sha256(run_bytes).hexdigest()
input_bytes = json.dumps(inputs).encode()
def write(name, data, mode):
    fd = os.open(os.path.join(dst, name), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    os.write(fd, data); os.fsync(fd); os.close(fd)
write("seat-run.json", run_bytes, 0o400)
write("seat-inputs.json", input_bytes, 0o400)
journal = [{"event": "begin", "run": run["run"], "runSha256": inputs["runSha256"]},
           {"event": "inputs", "sha256": hashlib.sha256(input_bytes).hexdigest()}]
write("seat-journal.jsonl", "".join(json.dumps(r) + "\n" for r in journal).encode(), 0o600)
write("seat.lock", b"", 0o600)
os.mkdir(os.path.join(dst, "receipts"), 0o700)
print(run["run"], inputs["runSha256"])
EOF
}
req() { python3 -I -c 'import json,sys
print(json.dumps({"schema":"d3-seam-request-v1","run":sys.argv[1],"runSha256":sys.argv[2],"phase":sys.argv[3],
 "principal":sys.argv[4],"sessionId":sys.argv[5],"inputs":json.loads(sys.argv[6])}), end="")' "$@"; }
seam() { env -i PATH=/usr/local/bin:/usr/bin:/bin LANG=C LC_ALL=C D3_SEAT_LOCK_FD="$2" "$SEAM" --run-dir "$1" --phase "$3"; }
probe() { node "$PIF" session drain "$SID" --mesh "$MESH" --probe | python3 -I -c 'import json,sys
r=json.load(sys.stdin)["reply"]; want=sys.argv[1]=="open"
odd=[p["path"] for p in r["probes"] if p["mechanism"]!="hold" and p["admits"] is not want]
print("closed", r["inbox"]["closed"], "piInputsFenced", r["native"]["piInputsFenced"], "binding", (r["binding"] or {}).get("run"), "odd", odd)' "$1"; }
P1=$(dirname "$RUN")/d3-drain-probe-$(hex 4); read -r P1RUN P1SHA < <(make_probe "$P1"); P1OP=$(hex 16)
exec 9<"$P1/seat.lock"; flock -x -n 9 && echo locked
```

Expected: `locked`. FAIL: the RUN parent is not writable, or flock fails.

### 3.2 Canary before the drain

```sh
probe open
```

Expected: `closed False piInputsFenced False binding None odd []`. FAIL: any row listed in `odd`.

### 3.3 Drain the v1 Main

```sh
req "$P1RUN" "$P1SHA" drain "$PRINCIPAL" "$SID" "{\"operationId\":\"$P1OP\"}" | seam "$P1" 9 drain > "$P1/receipts/drain.json"; echo "exit=$?"
cat "$P1/receipts/drain.json"; echo
```

Expected: `exit=0`, no stderr, and this receipt:

```json
{"schema":"d3-seam-receipt-v1","run":"<P1RUN>","phase":"drain","owner":"fabric-v2","result":"pass","observed":{"sessionId":"<SID>","inputAdmission":1,"inputsFenced":true,"queued":0,"admitted":0,"childrenTerminal":true}}
```

FAIL: exit 2, a `refused` receipt, or any stderr. Go to 3.10 to undo a refused drain, then use §7.

### 3.4 The canary is refused while fenced

```sh
probe closed
```

Expected: `closed True piInputsFenced True binding <P1RUN> odd []`. FAIL: any row listed in `odd`.

Optional, if a Fabric session shares the seat's mesh: send one real message from it with `fabric_exec`.

```ts
try { await agents.followUp({ id: "session:<SID>", message: "d3 drain canary <nonce>" }); return "delivered"; }
catch (error) { return error.code; }
```

Expected: `FABRIC_MAIN_DRAINING`. FAIL: `delivered`. If no session shares the mesh, record "not run".

### 3.5 A same-UID caller without the lock

```sh
exec 7<"$P1/seat.lock"
req "$P1RUN" "$P1SHA" drain-undo "$PRINCIPAL" "$SID" "{\"operationId\":\"$P1OP\",\"attempt\":1,\"drainReceiptSha256\":null}" | seam "$P1" 7 drain-undo; echo "exit=$?"
exec 7<&-
probe closed
```

Expected: no stdout, stderr `GAP: lock-not-held [owner: fabric-v2]`, `exit=2`. Then still
`closed True piInputsFenced True binding <P1RUN> odd []`. FAIL: a receipt, or the fence lifted.

### 3.6 A stale run's undo

```sh
P2=$(dirname "$RUN")/d3-drain-probe-$(hex 4); read -r P2RUN P2SHA < <(make_probe "$P2")
exec 8<"$P2/seat.lock"; flock -x -n 8
req "$P2RUN" "$P2SHA" drain-undo "$PRINCIPAL" "$SID" "{\"operationId\":\"$(hex 16)\",\"attempt\":1,\"drainReceiptSha256\":null}" | seam "$P2" 8 drain-undo; rc=$?; echo; echo "exit=$rc"
exec 8<&-
probe closed
```

Expected: a `pass` receipt for `<P2RUN>` with `"observed":{"sessionId":"<SID>","inputsFenced":false,"admissionRestored":true}`.
P2 never fenced (r2 N3), so it passes without contacting the Main. Then still
`closed True piInputsFenced True binding <P1RUN> odd []`. FAIL: the P1 fence lifted.

### 3.7 A fake inbox owner

The CLI must check the server against the owner record before it writes. This probe links the real socket into a
fake mesh whose owner record names pid 1.

```sh
F=$(dirname "$RUN")/d3-drain-fakemesh-$(hex 4); mkdir -m 700 "$F" "$F/main-drain" "$F/main-followups"
S=$(python3 -I -c 'import hashlib,sys; print(hashlib.sha256(b"pi-fabric-main-drain-v1\0"+sys.argv[1].encode()).hexdigest()[:24]+".sock")' "$SID")
ln "$MESH/main-drain/$S" "$F/main-drain/$S"
python3 -I -c 'import json,sys; d=json.load(open(sys.argv[1])); d["pid"]=1; json.dump(d,open(sys.argv[2],"w"))' \
  "$MESH/main-followups/$SID.owner.json" "$F/main-followups/$SID.owner.json"
exit_probe() { node "$PIF" session drain "$SID" --mesh "$1" --exit --pod-uid "$(uuid)" --run "$(hex 16)" --run-sha256 "$(hex 32)" \
  --operation-id "$(hex 16)" --generation "$(hex 16)" --deadline-s 5; echo "exit=$?"; }
exit_probe "$F"
```

Expected: `{"format":1,"error":"MAIN_DRAIN_SERVER_NOT_OWNER"}`, then `exit=1`.

FAIL: `MAIN_DRAIN_REFUSED_POD_BINDING_OUTSIDE_POD`. That means the CLI wrote to a server it did not verify; the host
Main refused the pod binding anyway, so nothing drained. `ln` needs `$F` on the same filesystem as `$MESH`. If the
link fails, record "not run".

### 3.8 A pod binding at a host Main

```sh
exit_probe "$MESH"
```

Expected: `{"format":1,"error":"MAIN_DRAIN_REFUSED_POD_BINDING_OUTSIDE_POD"}`, then `exit=1`. FAIL: any line with
`drained`.

### 3.9 A replay of the same drain request

```sh
req "$P1RUN" "$P1SHA" drain "$PRINCIPAL" "$SID" "{\"operationId\":\"$P1OP\"}" | seam "$P1" 9 drain; echo "exit=$?"
```

Expected: no stdout, stderr `GAP: evidence-write-failed [owner: fabric-v2]`, `exit=2`. The intent file exists, so
nothing reaches the Main a second time. FAIL: a receipt.

### 3.10 Undo, then the canary is admitted

```sh
D=$(sha256sum "$P1/receipts/drain.json" | cut -d' ' -f1)
req "$P1RUN" "$P1SHA" drain-undo "$PRINCIPAL" "$SID" "{\"operationId\":\"$P1OP\",\"attempt\":1,\"drainReceiptSha256\":\"$D\"}" | seam "$P1" 9 drain-undo; rc=$?; echo; echo "exit=$rc"
exec 9<&-
probe open
```

Expected: `exit=0`, and the receipt
`{"schema":"d3-seam-receipt-v1","run":"<P1RUN>","phase":"drain-undo","owner":"fabric-v2","result":"pass","observed":{"sessionId":"<SID>","inputsFenced":false,"admissionRestored":true}}`.
Then `closed False piInputsFenced False binding None odd []`.

FAIL: `refused`. `{inputsFenced: true, admissionRestored: false}` means restart required: restart v1 before §4. See
`drain-undo` in d3-drain-seam.md.

Keep `$P1`, `$P2` and `$F` as evidence. To clean up the fake mesh later, unlink only `$F/main-drain/$S` and the copied
owner file. Removing the link leaves the real socket in place.

## 4. The forward run (c)

Preconditions: §0 to §3 passed, and `probe open` shows `binding None`.

```sh
(cd "$REPO" && python3 setup/org/golive/move.py cutover-seat --run-dir "$RUN" --run-sha256 "$RUNSHA" --inputs-sha256 "$INSHA" --execute); echo "exit=$?"
```

Expected: `exit=0`, with A's step-14 message on stdout.

FAIL:
- exit 2 with `GAP: ... [owner: fabric-v2]` is this seam: use §7;
- exit 2 with another owner belongs to that owner;
- exit 75 means a rehearsal fault hook was set.

Then check the receipts and the evidence:

```sh
python3 -I - "$RUN" <<'EOF'
import hashlib, json, os, stat, sys
run = sys.argv[1]
sealed = json.load(open(os.path.join(run, "seat-run.json")))
journal = [json.loads(line) for line in open(os.path.join(run, "seat-journal.jsonl")) if line.strip()]
journaled = {(r.get("seam"), r.get("phase")): r.get("sha256") for r in journal if r.get("event") == "seam"}
ops = [r.get("operationId") for r in journal if r.get("event") == "drain-operation"]
def receipt(seam, phase, owner):
    raw = open(os.path.join(run, "receipts", phase + ".json"), "rb").read()
    r = json.loads(raw)
    ok = (journaled.get((seam, phase)) == hashlib.sha256(raw).hexdigest() and r["run"] == sealed["run"] and r["phase"] == phase
          and r["owner"] == owner and r["result"] == "pass" and set(r) == {"schema", "run", "phase", "owner", "result", "observed"})
    print(phase, "OK" if ok else "FAIL", json.dumps(r["observed"], sort_keys=True))
    return r["observed"]
sid = sealed["sessionId"]
d = receipt("drain", "drain", "fabric-v2")
print("drain observed", "OK" if d == {"sessionId": sid, "inputAdmission": 1, "inputsFenced": True, "queued": 0, "admitted": 0, "childrenTerminal": True} else "FAIL")
receipt("fence", "fence", "factory-lead")
rc = receipt("drain", "drain-recheck", "fabric-v2")
zero = all(rc["totals"][k] == 0 for k in ("followUps", "queued", "inFlight", "settledWindows", "pendingDeliveryOutcomes", "childCompletionEntries"))
print("drain-recheck observed", "OK" if rc["complete"] is True and rc["actorsNotStopped"] == 0 and zero else "FAIL")
rd = receipt("drain", "resume-drain", "fabric-v2")
print("resume-drain observed", "OK" if rd["inputsFenced"] is True and rd["exited"] is True else "FAIL", "podUid", rd["podUid"])
names = sorted(n for n in os.listdir(run) if n.startswith("fabric-v2-"))
want = sorted("fabric-v2-" + n + ".json" for n in ("drain-intent", "drain-outcome", "drain-recheck-intent", "drain-recheck-outcome",
                                                     "resume-drain-intent", "resume-drain-outcome"))
modes = {n: oct(stat.S_IMODE(os.lstat(os.path.join(run, n)).st_mode)) for n in names}
print("evidence", "OK" if names == want and set(modes.values()) == {"0o600"} else "FAIL", modes)
ev = lambda n: json.load(open(os.path.join(run, "fabric-v2-" + n + ".json")))
intent, outcome, resume = ev("drain-intent"), ev("drain-outcome"), ev("resume-drain-outcome")
print("operationId", "OK" if ops == [intent["operationId"]] else "CHECK", intent["operationId"], ops)
print("seam sha", intent["measured"]["seam"]["sha256"], "pinned", sealed["seams"]["drain"]["sha256"])
print("interpreter", json.dumps(intent["measured"]["interpreter"]))
print("meshRoot", intent["measured"]["meshRoot"], "main", json.dumps(intent["measured"]["main"]))
print("drain outcome", outcome["result"], "stopped", [a["id"] for a in outcome["stoppedActors"]],
      "ledger", len(outcome["ledger"]), "ledgerOverflow", outcome["asserted"]["reply"]["ledgerOverflow"])
b, a = resume["measured"]["before"], resume["measured"]["after"]
pod_ok = (b["uid"] == a["uid"] == rd["podUid"] and b["nodeName"] == sealed["target"]["node"]
          and a["piTerminated"] == {"exitCode": 0} and resume["measured"]["execCode"] == 0)
print("resume pod", "OK" if pod_ok else "FAIL", json.dumps({"before": b, "after": a, "execCode": resume["measured"]["execCode"]}))
pd = resume["asserted"]["podDrain"] or {}
print("pod drain", json.dumps({k: pd.get(k) for k in ("op", "drained", "exiting", "inputsFenced", "ptraceScope")}))
EOF
```

Expected: every line with a verdict says `OK`. Also:
- `operationId` says `CHECK` only if A's `drain-operation` record names the id with another key: compare by hand;
- the seam sha equals the pin. `null` means the seam could not re-read its own file: record that;
- `stopped` is `[]` on a seat without session actors;
- `ledgerOverflow` is `0`;
- `pod drain` shows `"op": "exit", "drained": true, "exiting": true, "inputsFenced": true`;
- `podUid` equals the raw Pod UID that A journaled at `single-owner`. Compare the two by hand.

There is no `fabric-v2-drain-undo-*` file. FAIL: any `FAIL` line, a missing or extra evidence file, or another mode.

## 5. The in-Pod caller probe, and a replay after the run (d, Pod part)

Run §2 again on the managed Pod (`POD=<managed pod>`).

The probe below targets the live managed v2 Main. If the caller proof is broken, that Main drains and exits. On the
synthetic seat that is the FAIL signal: stop the line. Run it only when §2 line 8 printed a nonzero pid.

```sh
k -n "$NS" exec "$POD" -c pi -- sh -c "/opt/pi-fabric/dist/cli/index.js session drain $SID --mesh /var/lib/fabric/mesh --exit \
  --pod-uid $(uuid) --run $(hex 16) --run-sha256 $(hex 32) --operation-id $(hex 16) --generation $(hex 16) --deadline-s 5; echo rc=\$?"
k -n "$NS" exec "$POD" -c pi -- /opt/pi-fabric/dist/cli/index.js session drain "$SID" --mesh /var/lib/fabric/mesh --status
```

Expected:
- `{"format":1,"error":"MAIN_DRAIN_REFUSED_POD_CALLER_NOT_EXEC"}`, then `rc=1`. The CLI ran as a child of `sh`, which
  has a parent inside the Pod, like any tool the Main runs;
- then a status line with `reply.binding: null` and `reply.inbox.closed: false`.

FAIL: any line with `drained` or `exiting`, or a bound or closed status.

Then, on the host, replay a request on the finished RUN without the lock:

```sh
exec 7<"$RUN/seat.lock"
req "$(j "$RUN/seat-run.json" run)" "$RUNSHA" drain-recheck "$PRINCIPAL" "$SID" "{\"operationId\":\"$(hex 16)\"}" | seam "$RUN" 7 drain-recheck; echo "exit=$?"
exec 7<&-
ls "$RUN" | wc -l
```

Expected: no stdout, stderr `GAP: lock-not-held [owner: fabric-v2]`, `exit=2`, and the same file count as before.
FAIL: a receipt, or a new file.

### Not live: covered by tests

The shipped tools give these probes no live trigger. Tests in `tests/` cover them.

| probe | why it is not live | test |
|---|---|---|
| undo after an accepted exit (`EXITING`) | the CLI has no undo op, and the window is the Main's 1 s shutdown grace | `seat-drain-resume.test.ts`: "drains, then the Main accepts a clean exit and shuts down after its reply" |
| the Main's own lock check on a host binding | only the seam sends host bindings, and it checks the lock first (3.5) | `main-drain-control.test.ts`: "refuses a same-UID caller that does not hold the seat lock" |
| a python3 planted first in the Main's PATH | it needs a writable dir in the live Main's PATH | `main-drain-control.test.ts`: "the Main's SO_PEERCRED helper is the fixed /usr/bin/python3" |
| a fake owner record in the Pod | probe 3.7 runs the same CLI code on the host, where a failure cannot drain a Pod Main | `seat-drain-resume.test.ts`: "the in-Pod CLI refuses a server that is not this session's inbox owner" |

## 6. Read-backs to record (e)

Post these on smarty-dev#15310 with the run id, the UTC time and the pass or FAIL of each step.

| what | from |
|---|---|
| seam path, sha256, mode, owner, shebang interpreter | 0.1 |
| v1 Main pid, mesh root, native capability | 0.2 |
| the host pi-fabric version | `j "$(dirname "$(dirname "$(dirname "$PIF")")")/package.json" version` |
| A's `seat_cutover.py` sha256 | `sha256sum "$REPO/setup/org/golive/seat_cutover.py"` |
| host and Pod `ptrace_scope` | 0.5, §2 |
| `can-i` answers, kubeconfig server and CA sha, kubectl owner and mode | §1 |
| Pod python3 version and owner, CLI `test -x`, Pod pi-fabric version, PPid of an exec'd process (`0`) and of an `sh` child | §2 |
| every §3 output, and the probe run ids `P1RUN` and `P2RUN` | §3 |
| the receipts, their sha256 and journaled sha256, the operationId | §4 checker |
| the evidence names and modes, the seam and interpreter identity, `stoppedActors`, ledger size and overflow | §4 checker |
| the raw Pod UID before and after, `nodeName`, the pi exit code, `execCode`, the in-Pod reply | §4 checker |
| the §5 outputs | §5 |

## 7. Triage

| seen | meaning | owner |
|---|---|---|
| `GAP: lock-not-held` in §4 | A did not pass a held lock FD | principal-lead (A) |
| `GAP: operation-id-invalid` | A predates rev 4 | principal-lead (A) |
| `GAP: seat-run-*`, `seat-inputs-*`, `seat-journal-*` | the RUN does not match the request | hosts-lead (preflight) or A |
| drain `refused`, `fence-unavailable` | the v1 Pi lacks pi#309, or a per-path denial probe did not refuse | fabric-v2 |
| drain `refused`, `durable-actor-present` or `children-live` | the seat is not quiet: read `reasons` in `fabric-v2-drain-outcome.json` | operator |
| drain `refused`, `main-unreachable` | no drain channel, or the owner record does not match the server | fabric-v2 |
| `GAP: fence-receipt-invalid` at drain-recheck | the fence receipt is not a six-key factory-lead pass for this run | factory-lead |
| resume-drain `refused` | read `reason` and `asserted.podLine` in `fabric-v2-resume-drain-outcome.json` | the rows below |
| `reason: pod-cli-missing` | `test -x` failed in the Pod | img-g9 |
| `podLine` has `MAIN_DRAIN_REFUSED_PEER_UNVERIFIED` | no `/usr/bin/python3` in the Pod | img-g9 |
| `podLine` has `MAIN_DRAIN_REFUSED_POD_CALLER_NOT_EXEC` | on A's own exec, this runtime gives exec'd processes a parent in the Pod, and the caller proof does not hold | fabric-v2 (stop) |
| `podLine` has `MAIN_DRAIN_SERVER_NOT_OWNER` | the Pod's owner record does not name the Pod Main | fabric-v2 |
| `reason: pod-exit-unconfirmed` | the pi container did not terminate with exit code 0 in time | fabric-v2 |
| `GAP: pod-node-mismatch` or `pod-uid-mismatch` | the Pod is not the sealed target | principal-lead (A) |
