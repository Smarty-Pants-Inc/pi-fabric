# D3 drain seam (`fabric-seat-drain`): operator notes

This is fabric-v2's `drain` seam for the D3 principal-seat move (smarty-dev#15310). A runs it as a pinned
executable:

```text
/proc/self/fd/N --run-dir RUN --phase drain|drain-undo|drain-recheck|drain-restore|resume-drain
```

It reads one request on stdin and writes one receipt on stdout. The contract is DRAIN-PHASE-CONTRACT rev 4.

- A valid receipt (`pass` or `refused`) always exits 0 with empty stderr.
- A lock-proof or binding failure prints no receipt and exits 2, with one `GAP: <code> [owner: fabric-v2]` line on
  stderr. A treats that as UNKNOWN.

The seam never deletes or acknowledges anything. Its evidence is private, write-once, and kept under RUN.

The live proof on the synthetic test seat is in [d3-drain-test-seat-proof.md](d3-drain-test-seat-proof.md).

## Install (per seat host)

```sh
node <pi-fabric>/dist/seat-drain/fabric-seat-drain.cjs --install --out /abs/path/fabric-seat-drain
```

The installer writes the pinned file once, mode 0500:
- the shebang names the absolute path of the node it ran under;
- that node's sha256 is embedded in the file;
- the installed seam refuses to run under any other `/proc/self/exe`.

The printed `sha256` is what goes in `seat-run.json` `seams.drain`. It differs per host.

A user-owned interpreter is the accepted same-UID residual (A6 in A's seat contract), and the evidence names it.

## What the live Main needs

The v1 Main must run a pi-fabric release that has the owner-only drain channel, `src/main-drain/`. That channel is
a Unix socket in `<parent>/.<mesh>.main-drain/`, the mesh root's private sibling: a 0700 directory owned by the seat
user, on the mesh root's own filesystem. It stays out of the mesh root, so no tool that walks the mesh meets a
socket, and a copy of the mesh root never carries one. On a seat Pod it is `/var/lib/fabric/.mesh.main-drain/`, next
to `/var/lib/fabric/mesh` on the seat volume.

What the channel creates in that directory, and nothing else (no temp, lock or pid file):
- the directory itself, mode 0700, owned by the seat user (`src/main-drain/server.ts`, `pinDirectory`, its
  `mkdirSync`);
- one Unix socket per Main session, `<24 hex>.sock` (`mainDrainSocketName` in `protocol.ts`), created by the
  server's `listen` and set to mode 0600;
- removals: a stale socket of the same name that is ours and answers nobody, at start, and the socket itself on
  close.

Clients, the seams and the in-Pod CLI only stat and connect there. It is never reachable over the
mesh, NATS or a bridge.

The Main also needs a Pi that has the extension input fence (`pi.hostCapabilities.extensionInputFence === 1`,
pi#309). Without it, `drain` refuses `fence-unavailable` at once, after its own undo, and v1 keeps admitting input.

Python 3 at `/usr/bin/python3` must be present on the seat host, for two things:
- the seat-lock proof;
- SO_PEERCRED.

The Main and the seam always run that fixed interpreter, isolated (`-I`) with a minimal env. A `python3` placed earlier
in `PATH` is never used.

## Request budgets and timeout recovery

The socket keeps its **5-second idle guard before a complete request line**. After parsing, that guard is
removed and one absolute **25-second operation budget** covers queue wait, peer verification and the entire
control call. This leaves **5 seconds** below the normal **30-second client/seam call cap**. Responses and
refusals remain ordered; a request whose budget expires while queued never starts work. Actor stop retains its
separate **10-second wait bound** and its pending-stop protection.

`OPERATION_TIMEOUT` bounds asynchronous waiting, not execution: it neither cancels side effects nor preempts a
blocked event loop. A mutation still running after timeout remains owned by the process-global drain slot,
including across an extension reload or channel close. Further mutations refuse `OPERATION_PENDING` until the
original work settles; `status` and `probe` remain available with their own bounded requests. A late
peer proof cannot start the control call, and late completion cannot send another reply or commit an exit.

Wait for the original operation **and any pending actor stop** to settle, then retry undo with the **same run,
operation id and generation**. A stuck mutation needs an **independently verified same-session Main
restart**, not merely a channel close or extension reload. Verify recovery: a timeout is not proof that work stopped. Explicitly stopped actors are **not automatically restored** by restart. On failed-cutover
fallback, use the validated ordinary owner recreation path with new actor ids and configuration/live-binding
readback. Preserve the saved `kernel` and `pythonRuntime` through ordinary create and verify both fields.
`drain-restore` is only for a successful cutover with **both PASS drain and PASS fence receipts**;
it is not a failed-cutover recovery shortcut.

## Evidence files under RUN (all 0600, O_EXCL, read back)

| file | written |
|---|---|
| `fabric-v2-drain-intent.json` | before the first control call: `operationId`, the fence `generation`, the interpreter and seam hashes, the Main's owner record |
| `fabric-v2-drain-stop-<n>-intent.json` | before stop request `n`: each actor about to be stopped, with its full registry row |
| `fabric-v2-drain-actor-<id>.json` | the Main's own durable pre-stop spec and resolved live binding, tied to the run, operation and generation |
| `fabric-v2-drain-restore-<n>-outcome.json` | each post-activation restoration attempt, including original-to-recreated ids |
| `fabric-v2-drain-outcome.json` | at the end of `drain`, containing:<br>• the result and reason;<br>• `beginSent: false` when no drain request ever reached the Main;<br>• every sub-step (begin, fence, stop-actors, probes, undo);<br>• the measured inventory;<br>• the asserted Main replies;<br>• the dispositions;<br>• `stoppedActors` (proven or attempted) with each one's full registry row, and `stopIntended`;<br>• the admitted and ledger lists |
| `fabric-v2-drain-undo-<n>-{intent,outcome}.json` | for each `drain-undo` attempt `n` |
| `fabric-v2-drain-recheck-{intent,outcome}.json` | for `drain-recheck` |
| `fabric-v2-resume-drain-{intent,outcome}.json` | for `resume-drain` |

## `drain-undo`: the operator path

A normally passes the stored `drain` receipt's sha. After a SIGKILLed seam there is no receipt. In that case the
operator runs `drain-undo` with `drainReceiptSha256: null`, which is accepted only while `RUN/receipts/drain.json`
is absent. The operator must hold the seat lock and pass the run's `operationId` with `attempt: 1`.

`drain-undo` passes without contacting the Main when this operation never fenced anything (r2 N3). Two measured
facts prove that:
- `fabric-v2-drain-intent.json` is absent; or
- the drain's outcome is a refusal with `beginSent: false`, bound to this intent's sha256. The drain wrote its intent,
  then no drain request ever reached the Main (for example `main-unreachable`).

Attempts are contiguous. Attempt `n + 1` is allowed only in two cases:
- attempt `n` is UNKNOWN: it has an intent file and no outcome file;
- attempt `n` was a handle-lost refusal (`inputsFenced: true`, `admissionRestored: false`), and v1 has since been
  restarted. The restart shows as another owner pid or start time, and both owner records must be readable.

`refused {inputsFenced: true, admissionRestored: false}` means restart required: the fence handle was lost, or v1
is unreachable. Restart v1 (a new process starts unfenced), then run the next attempt.

## Reversible actor stops (#18489, #18490, #18491)

The pattern is Kubernetes drain: cordon admission, evict within the disruption budget, wait for termination with
a grace period, and uncordon on failure. A durable actor blocks before any stop. In-flight tool calls are waited
out until native turn-end/idle within the existing 600-second deadline; a zombie worker (state `Z`) is terminal,
not an executable child. Unknown worker identity still blocks.

Session actors, including an idle resident supervisor, are stopped only as the last act: fence held, everything
else zero, every actor quiescent. Before the first stop, the Main saves each complete spec with its resolved live
binding in `fabric-v2-drain-actor-<id>.json`. The seam also keeps its measured registry-row stop intents. Missing
specs, unreadable evidence or an unavailable recreation path refuse without stopping an actor.

Undo releases the native fence, reopens admission and recreates stopped actors through the owning Main's ordinary
create path. It preserves creation settings and project defaults, reapplies the saved live model, thinking and
modelReason through session setters, and verifies the resulting spec. Supervisor authority is re-minted by that
Main, not copied from a peer. Recreated actors have new ids and fresh sessions; `reply.restored` records the map.
Already-live exact replacements are reused. A different same-name spec is a conflict, never a successful undo.

`actorsRestored: true` requires readback of every saved actor's configuration and live binding. A failed create,
binding setter or readback leaves it false and retains the binding and saved rows. A timed-out stop that has not
settled also keeps restoration false: a late stop must not kill an actor after undo claimed success. No queued
work is dropped or force-archived: a last-moment quiescence check runs inside the existing stop primitive.
Actors needing a protected slot's unreplayable admission claim are not stopped.

## Session actors after a successful cutover

After A's activation step, run the pinned seam on the new Main's host, with the same sealed RUN, its saved actor
files and the seat lock, using `--phase drain-restore`. The request has the normal envelope and only
`inputs: { operationId }`. This is a separate owned step, not `drain-undo`: it never lifts the old Main's fence.
The new Main must be reachable through the local owner socket; the RUN and source journal must resolve its mesh.
The caller must arrange those local inputs when moving between hosts or into a Pod.

Both the passing drain and the sealed fence owner's passing receipt must be present. A still-bound old Main
refuses restoration. The new Main uses the same ordinary create/setter/readback path as undo, reading durable
pre-stop rows without the old process's memory. The receipt reports
`observed: { sessionId, actorsRestored }`; only `actorsRestored: true` passes. Each attempt writes a fresh outcome.
Retries reconcile exact existing replacements and do not create duplicate supervisors. This additional phase
must be invoked by the cutover caller after activation; it is not added to A's phase scheduler by this package.

Registry rows retain downgrade-compatible top-level project defaults, explicitly labelled `projectDefaults`,
and record the owner's live/last activation model, thinking and modelReason as `resolvedBinding`. Actor listings
carry the same labelled binding. An in-flight activation keeps its launched pin; idle owner model/thinking
changes update the registry before the setter acknowledges them.

## `resume-drain` (the raw Pod)

`resume-drain` reaches the raw Pod's Main only through `kubectl exec`. It uses the fixed root-owned kubectl, the
sealed kubeconfig and context, and a minimal env. The kubeconfig must have one cluster, with the pinned server and
CA. The fixed argv is:

```text
kubectl exec <pod> -c pi -- /opt/pi-fabric/dist/cli/index.js session drain <sid> --mesh <pod PI_FABRIC_MESH_ROOT> --exit ...
```

The pod UID is checked against the input pin before and after. `exited` is the pi container's own exit status.

Pod bindings carry no seat lock. Inside the Pod, the Main accepts one only from a process that the container runtime
injected, which is what `kubectl exec` produces. Four checks prove it:
- its SO_PEERCRED uid is the Main's euid;
- it is neither the Main nor pid 1;
- `/proc/<pid>/stat` reads PPid 0 before and after;
- its start time is unchanged between those two reads.

Every process the Main or its tools fork has a parent inside the Pod's PID namespace. An orphan reparents to pid 1.

The in-pod CLI checks the server the same way the host seam does. The server's SO_PEERCRED uid must be the CLI's own,
and its pid and start time must match `main-followups/<sid>.owner.json`. Otherwise the CLI refuses before it writes a
request. An accepted exit is final: the Main refuses every later drain, undo or exit, so its fence stays held until
it shuts down.

SO_PEERCRED needs `/usr/bin/python3` in the image. The image owner measured it in the seat image's base layer, and
cut 2 asserts it. Without it, pod bindings refuse. The in-pod CLI reports YAMA `ptrace_scope`, because at scope 0 a
same-UID ptrace of the exec'd CLI is the A6-class residual. Anyone with `pods/exec` RBAC on the Pod is authorized by
design. A same-UID process that rewrites the owner record and serves a fake socket is the same A6-class residual.

The in-image CLI path is `/opt/pi-fabric/dist/cli/index.js`. The image owner confirmed it on 10-25 for pi-fabric
0.112.0, the ab409c28 image family. The seam still runs `test -x` on it inside the Pod first, because cut 2 builds a
new base and must confirm it again.

## Claims and residuals

- **Visible refusal, not exactly-once.**
  - Non-retrying producers see `FABRIC_MAIN_DRAINING` with `notRun`: peer Mains, `smarty-broadcast`,
    `smarty-prompt`, `smarty-install` and alarms. Retry after the move with a NEW message.
  - The resident host, the completion journal and the completion inbox keep their records and deliver later.
  - Legacy `fabric.steer` relays are dropped, and the ledger records them.
- **The feed's `herdr agent prompt`** cannot see a Pi refusal. Holding the feed is an A/runbook precondition for
  moving a seat that has a feed.
- **No fence persists across a v1 restart.** `drain-recheck` is the post-stop recount, and the fence seam observes
  restarters.
- **The dry-run canary** proves the gates' state, not end-to-end delivery.
