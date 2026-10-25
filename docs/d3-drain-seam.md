# D3 drain seam (`fabric-seat-drain`): operator notes

This is fabric-v2's `drain` seam for the D3 principal-seat move (smarty-dev#15310). A runs it as a pinned
executable:

```text
/proc/self/fd/N --run-dir RUN --phase drain|drain-undo|drain-recheck|resume-drain
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

## Evidence files under RUN (all 0600, O_EXCL, read back)

| file | written |
|---|---|
| `fabric-v2-drain-intent.json` | before the first control call: `operationId`, the fence `generation`, the interpreter and seam hashes, the Main's owner record |
| `fabric-v2-drain-stop-<n>-intent.json` | before stop request `n`: each actor about to be stopped, with its full registry row |
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

## Actors stopped by a drain (ruling A, 10-25): no automatic recreation

The seam stops a session actor only as its last act. By then the fence is held, everything else is 0, and every
actor is quiesced.

Before each stop request, the seam writes `fabric-v2-drain-stop-<n>-intent.json` with every actor it is about to
stop and its full registry row. A crash after the request still leaves those rows. The Main counts an actor as
stopped only after the stop resolved AND its worker is proven gone. A failed, slow or unproven stop ends the drain:
it refuses and undoes. Every attempted actor, proven or not, must be active again for `admissionRestored: true`.

If anything fails afterwards, or A rejects the pass, the seam does NOT recreate the actor. `drain-undo` then
returns `admissionRestored: false`. The operator recreates the actor by hand from the full row saved in
`fabric-v2-drain-outcome.json` → `stoppedActors[].registryRow`, or, when the outcome is missing, in
`fabric-v2-drain-stop-<n>-intent.json` → `actors[].registryRow`:

1. Read the row: `name`, `instructions`, `events`, `topics`, `delivery`, `responseMode`, `triggerTurn`, `coalesce`,
   `coalesceKey`, `dedupeKey`, `activation`, `activationFilter`, `residency`, `runner`, `model`, `thinking`, `tools`,
   `transport` and `principalScope`.
2. In the owning Main's own session, create the actor again. A stopped row with the same name does not block this:

   ```ts
   return agents.create({
     name: row.name,
     instructions: row.instructions,
     events: row.events,
     topics: row.topics,
     delivery: row.delivery,
     responseMode: row.responseMode,
     triggerTurn: row.triggerTurn,
     coalesce: row.coalesce,
     ...(row.coalesceKey ? { coalesceKey: row.coalesceKey } : {}),
     ...(row.dedupeKey ? { dedupeKey: row.dedupeKey } : {}),
     ...(row.activation ? { activation: row.activation } : {}),
     ...(row.activationFilter ? { activationFilter: row.activationFilter } : {}),
     residency: row.residency ?? "session",
     ...(row.model ? { model: row.model } : {}),
     ...(row.thinking ? { thinking: row.thinking } : {}),
     ...(row.tools ? { tools: row.tools } : {}),
   });
   ```

3. **Tier 1:** `protected`, `supervisorFor` and `principalScope` are identity and authority fields. Only the owner's
   own session may re-mint them, through its normal creation path, never a copied value from another session.
4. Read back: the new actor's name, `principalScope`, `topics` and `events` must match the row. Record the new id
   next to the old one in the run's notes. The new actor has a new id and a fresh session.

## Session actors after a SUCCESSFUL drain

The seam stops a session actor explicitly, and an explicit stop never revives. So after a successful move, any
session actor the seam stopped is GONE on v2. A seat with a session `supervisor` actor loses it this way.

The operator recreates it from the RUN row, as described above, AFTER A's `activation` step. Then the operator
reads back that the recreated actor's name, `principalScope` and `topics`/`events` match the row, and records the
new id.

If the org's own start procedure re-creates the supervisor itself, the operator's step is only that read-back.

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
