# Host-wide activation admission

Refs smarty-dev#4444 (host-wide actor activation cap)

The process transport uses a host-user-wide FIFO gate immediately before
`spawnDetached`, the same launch path used by `agents.processSlice`.
It does not release the actor's existing mailbox claim, advance execution
ordering, mark inference started, or start the worker timeout while waiting.
`actorStatus` reports `status: "waiting"`, `preparing.phase: "host-queue"` and
`hostQueue: {position, waitingSince, limit}`. Position is one-based;
waitingSince is epoch milliseconds. No queued activation is dropped by the gate.

## Configuration and rollout

Merge `config/hosts/ryzen1.fabric.json` into **Ryzen 1's trusted host agent
profile** `fabric.json` to set `agents.hostActivationLimit: 4`. This checked-in
overlay is not installed automatically and does not modify any live host.
There is no hostname branch or cap default in code. Both
`hostActivationLimit` and `hostActivationLimitScope` are stripped from workspace
config, including trusted workspaces. Invalid configured limits fail validation.

The scope defaults to `"actors"`; `"all"` also caps process-transport task agents.
Non-process transports and resident daemons are outside this activation gate.
Enabled caps require Linux and util-linux `flock`; missing support refuses the
launch instead of silently bypassing the cap. All processes for the same host
user, including resident owners and different agent profiles, must receive the
same trusted policy. An uncapped/older generation cannot participate in the gate.
Roll out the compiled generation/config to every launching process; changing a
limit while old workers are still running is not retroactive.

**Revert condition:** unset `agents.hostActivationLimit` in the trusted host
configuration and refresh launching processes. Do not delete token files while
any capped worker is live: deleting a locked inode would create a second token
namespace. For a lower limit, drain existing activations before refreshing all
launchers. This feature is host-local admission, not a distributed fleet quota.

## Lock lifetime and FIFO

The default directory is
`~/.local/share/smarty-dev/fabric-host-tokens/` (same host user across all roots).
Use a local filesystem, not an NFS/network-mounted home. A trusted embedding
can set `AgentManager`'s `hostActivationDirectory` option for isolated tests;
there is deliberately no workspace or worker-argument directory override.
The directory/files are created with modes 0700/0600.

`flock --exclusive --nonblock ... 3` locks an inherited open file description.
The launcher retains that description after the tiny helper exits. A token is
passed as worker FD 3 through `spawnDetached`, including systemd scope admission
and close-fenced fallback; the launcher closes its own reference only after
launch. The kernel releases the lock when the worker's last reference closes.
A killed launcher therefore does not free a live worker's token, and a killed
worker does not leak a token. Ordinary worker descendants do not inherit FD 3.
The cooperating worker retains this reserved descriptor for its lifetime. Its
native Pi execution host also inherits the same description as FD 3. Other
descendants do not inherit it.

A capped Pi program that calls `agents.ask`, `run`, `spawn`, `wait`/`join` or
`handoff` releases its slot before dependency admission. It stays released for
the entire Fabric program, so spawn-then-message-then-join cannot reclaim the
slot prematurely. At the outer execution fence (including errors/cancellation),
Pi reacquires the **same open description** through FIFO admission before any
subsequent inference. Parallel programs share one yield and all fence their
return on resumption. Failure to restore custody terminates that Pi execution
host rather than allowing slotless inference; the worker owns tree teardown.
Host-engine dependencies without an enclosing Fabric execution fence are
rejected before admission instead of releasing a slot with no resumption owner.
Suspended parents remain live processes but do not occupy execution capacity.
The cap bounds admitted, non-suspended workers, not blocked dependency stacks.

Replacement attempts exclude reported host-queue time from the monitor runtime
budget, just like initial launches. The native launch authority also rejects a
replacement whose remaining runtime expired during non-queue preparation.

A short `queue.lock` guards ticket reservation and token selection. `sequence`
is an append-only monotonic integer counter; a crash may leave a gap, never
reuse a published sequence. `queue.json` is the atomic live-waiter snapshot.
Only the live head tries the N `token-<index>.lock` files. Polling is async with
35–74 ms jitter, never a blocking lock wait on Main's event loop. Positions
are published only when they change, not every poll.

Tickets bind to the launcher's PID, Linux process start time and boot ID.
Every transaction prunes dead/zombie processes, reused PIDs and previous boots.
Unreadable identity for a live PID is retained conservatively. Explicit
cancellation/stop removes its own ticket under the queue lock before its wait
settles. Queue corruption or lock errors fail closed. Lock files must never be
unlinked as a cleanup strategy. The sequence counter may be archived/reset only
when no launchers/waiters/workers use the directory.

FIFO means **ticket admission**, not CPU scheduling order after independent
workers are spawned. With a limit greater than one, the OS can schedule admitted
workers differently; with limit one their observed execution order is FIFO.

## Real Pi/Fabric acceptance and deadlock regression

After a fresh build, run the offline, keyless native entry-point proof:

```sh
PI_FABRIC_TEST_PI_BINARY=/absolute/path/to/installed/pi \
  nice -n 19 bun tests/fixtures/host-activation-real-pi-proof.ts
```

Set `TMPDIR` and `TASK_OUT`. The runner creates three isolated home/profile/root
sets per case, loads the **built extension as a Pi package**, and drives the
installed Pi CLI in native RPC mode. A custom provider supplies deterministic
inference only; it does not replace Pi, construct Fabric managers, substitute
a worker, or bypass the public `fabric_exec` / `agents` surfaces. No model
service, credential, live mesh, or live token directory is used.

Rows cover cap 2 with four `agents.run` tasks (N+2), cap 4 with eight tasks across
three roots, and cap 1 with an actor awaiting another root's actor, a task using
`agents.run`, and a task using `spawn` then `wait`. Independent rows also queue
three actors and retain real public `agents.actorStatus` host-queue snapshots.
The controller temporarily holds scratch tokens only to make queuing observable.
It observes actual native worker PIDs and their token FD lock state, including
worker startup before Pi's `session_start`. Independent cases have at most N
live workers; nested cases may have a live but slotless waiting parent. Every
worker inference call must hold a slot. All native Main processes close with
exit code zero. Evidence (RPC output, native events, PID/slot samples, results,
profiles and built artifact hashes) is retained under `$TASK_OUT/real-pi/`.

`tests/host-activation-real-pi.test.ts` retains the cap-one native deadlock
regression. `tests/host-activation.test.ts` covers startup retry and mid-run
resume that queue longer than their entire remaining runtime+exit-grace budget,
and refuses an attempt whose non-queue preparation actually spends its budget.

## Isolated transport proof

After `bun run build`, run:

```sh
nice -n 19 bun tests/fixtures/host-activation-proof.ts
```

`TMPDIR` and `TASK_OUT` must be set. The proof uses three separate Main host
processes, each with real `ActorManager`, `AgentManager` and process transport,
an isolated scratch mesh and four actors. It first holds four scratch tokens,
accepts all twelve mailbox activations, verifies all twelve `actorStatus`
queue objects, then releases the blockers together. An instrumented wrapper
runs the **built real Fabric worker** with a deterministic local Pi protocol
fixture (no model service, credentials, or live mesh). It records whole-worker
start/exit events, retained token FDs, maximum concurrency and both ticket and
execution order. The raw logs and JSON result are retained under
`$TASK_OUT/three-main-proof/`; scratch storage is removed afterwards.

`tests/host-activation.test.ts` separately proves cross-process FIFO at limit
one and cap enforcement at limits one/two, worker and launcher crash behavior,
killed waiter cleanup, stop/abort/ownership revocation, PID/boot cleanup, task
scope, unset behavior, and systemd-scope admission/fallback token inheritance.
