# Opt-in task and actor model routing (shadow by default)

`agents.spawn` accepts `model: "auto"` for **new session-owned process/Pi tasks only**.
It makes one typed Jev Choice and records it. By default it launches the explicit
role pin, even when Jev recommends another candidate. Trusted host/project config
can opt individual classes into live dispatch (below); no launch argument, task
text or environment flag grants that permission. Opted-in `status-groom` process/Pi
**actor activations** use the same path. `agents.run` also accepts auto for the
three explicitly named task classes below. Mains, durable task agents, handoffs,
ordinary explicit-model calls and the fleet's direct task CLI are not routed.

```ts
const child = await agents.spawn({
  task: "Extract the public version number from the named file; report only that number.",
  model: "auto",
  routeClass: "bounded-lookup",
  transport: "process",
  runner: "pi",
  pinModel: "my-provider/gpt-6.1-sol", // registered canonical model or role alias
  pinThinking: "high",
  // Map trusted issue/PR state to this flag, NEVER infer it from task text.
  protected: false,
});
```

Bounded lookup means a checkable one-file lookup, extraction or formatting task;
not code changes, design judgment, review evidence, secrets or client data.
`status-groom` means routine checks and grooming whose output is a status line or
a no-op; the same exclusions apply. Callers
must set `protected: true` for review, security, audit, named passes and all
`needs-security-pass` work. An omitted/unknown flag or unknown class is excluded
before Jev. Protection is a caller-supplied trusted-state snapshot, **not an
authenticated label oracle**. No prompt-based override exists, and protection
always wins over a live class opt-in.

## Classes on every run record

Every new task, actor activation and handoff records `routeClass` and
`routeClassSource: "explicit" | "derived"`, independently of shadow routing. An
explicit caller/actor class wins. Otherwise host facts yield `actor:review` for
`*-review-astra`, `actor:security` for `*-security-astra`, `actor:status-groom` for
`supervisor`/`*-supervisor`, `actor:other` for other actors,
`task:<runner>:<resolved transport>` for tasks, or `handoff` for handoffs. Queued
task receipts use the requested transport until launch resolves it; portable
hosted tasks use `task:pi:hosted`. Task text
is never classification input. The existing `protected` true/false snapshot is
retained; omitted protection stays unknown. These are history fields, not routing
permission: derived classes, handoffs, review/security and protected work never
opt into routing, and all routing gates/pins remain required.
Legacy records are not backfilled.

## Pins and finite candidates

Both pins are required. Call fields take precedence over dedicated role settings
in the usual `fabric.json` configuration. The parent's model, global `agents.model`
and Fabric's default medium effort do **not** qualify as role pins. Pins resolve
only as exact registered provider/model keys or exact targets of an explicitly
configured alias, including after one registry refresh. Missing or invalid pins
refuse spawn with `ModelRoutePinError` (`MODEL_ROUTE_PIN_UNAVAILABLE`) before
inference or dispatch; ordinary non-auto fuzzy selection is unchanged. The
fallback pin is immutable. The admitted finite selection (candidate for live,
pin for shadow/fallback) is frozen for the run: initial launch, startup retry and
resume recheck exact availability and reject a changed/unavailable selection before
starting another worker. Every routed worker requires valid actual effort equal
to that selection before sending the prompt; missing, malformed or lower readback
records no verified admission. Ordinary explicit-model clamping is unchanged.

```json
{
  "agents": {
    "modelRouting": {
      "live": false,
      "pinModel": "my-provider/gpt-6.1-sol",
      "pinThinking": "high",
      "shadowCandidates": [
        { "model": "my-provider/gpt-6.1-sol", "effort": "medium" },
        { "model": "my-provider/gpt-6-luna", "effort": "medium" }
      ]
    }
  }
}
```

Omit `shadowCandidates` for the pin-only default. The pin is always included and
identical model/effort pairs are deduplicated. Shadow candidates must use exact
registered, authenticated keys visible to Main; at most 16 additional candidates
are evaluated. Unavailable candidates select the pin with `invalid-candidates`.
Configuring a shadow candidate alone grants **no permission to run it**.
Live candidates also pass the host deny policy and supported-thinking checks. Paul's rule
still applies: no Sol role at medium or below without measured parity and his
approval (Paul's floor rule, smarty-dev#2236). `agents.modelRouting.live` remains a legacy false-only field: `true` is refused,
not a global execution permission. Live routing is class-scoped through
`liveClasses`, default empty. Listing a class is a trusted policy decision, not
evidence of measured parity or Paul's floor approval.

Jev receives only class, clear protection status and finite model/effort choices:
no task text, prompts, history or free-text explanation. One request, no retry,
2.5-second absolute deadline (including credential resolution/network work).
Both confidence and chosen probability must be >= 0.90. Low confidence, malformed
answer, backend error or timeout records a fixed reason and uses the pin. Caller
cancellation still cancels launch. The existing Jev provider/client supplies the
backend; `jev.enabled: false`, Schema enforce's unavailable Jev programs, or owner
retirement yields `jev-error` and pinned dispatch rather than relaxing that gate.
Optional shadow inference also requires the current host `approvals.network` to
be explicitly `"allow"`. Agent approval, inherited/session grants, and network
`"ask"`/`"auto"`/`"deny"` do not authorize this internal call: these cases record
`jev-error` and dispatch the pin without resolving Jev credentials or sending HTTP.
This deliberately stricter opt-in does not change normal `jev.evaluate` approvals.
Evaluation captures the Jev generation's revocation signal; retirement revokes
evaluations and aborts network work. Cleanup also joins the real host credential
lookup, not just its abort-raced waiter. The host lookup has no cancellation API:
retirement stays pending until it settles, and cannot send old-generation HTTP
afterward.

The handle includes `routeDecision` with `{ model, effort, confidence, probability,
reasonCode, decisionId, ... }`. `model/effort` describes the accepted **would-be**
choice (or pin on fallback); `shadowChoice` preserves Jev's raw finite choice even
below threshold. For `mode: "live"`, `handle.model/thinking` and worker admission
use this choice; for shadow they still use the pin. `live-choice`, `class-reverted`
and `revert-state-error` distinguish live selection and revert fallbacks.

## Class-scoped live dispatch and automatic revert

Only `bounded-lookup`, `status-groom`, `task:merge-additive`,
`task:ci-test-fixture` and `task:exact-checks` pass the finite class gate.
The three task classes must be **explicitly** set on `agents.spawn/run` with
`model: "auto"`; names, task text and derived `task:pi:process` metadata never
select them. They cover additive merges, CI fixture changes and execution of
exact specified checks, respectively; they do not authorize review/security/audit.
Unknown classes and review/security/audit classes never become live even if listed
and even if a caller incorrectly supplies `protected: false`.

Trusted host or project `fabric.json` can enable **status-groom on Luna max**:

```json
{
  "jev": { "enabled": true },
  "approvals": { "network": "allow" },
  "agents": {
    "modelRouting": {
      "live": false,
      "liveClasses": ["status-groom"],
      "pinModel": "cliproxyapi/gpt-6.1-sol",
      "pinThinking": "max",
      "shadowCandidates": [
        { "model": "cliproxyapi/gpt-6-luna", "effort": "max" }
      ]
    }
  }
}
```

The actor still needs explicit model/thinking pins, `routeClass: "status-groom"`
and `protected: false`; dedicated config pins are for tasks, not replacement actor
pins. Jev chooses between the pin and Luna/max; this example permits but does not
force Luna. Non-enforced Schema, explicit pins, Jev enabled and explicit host
network allow remain mandatory. Confidence and chosen probability must both be
at least 0.90; timeout, malformed answer, invalid candidate, unavailable backend,
low confidence or recording error launches the pin and records a fixed reason.
The same `decisionId` remains in `X-Smarty-Route` and terminal outcomes, including
live candidate model/effort admission. A recording failure cannot launch an
unlogged candidate. Unlisted classes retain shadow-only behavior under `live:false`.

Terminal outcomes may contain `routeQuality: "fail" | "pass"`. A caller/actor
can also report quality after reviewing a locally owned routed run:

```ts
// Replace with the ID returned by agents.spawn/run for the locally owned routed run.
const runId = "owned-run-id";
await agents.routeOutcome({ id: runId, routeQuality: "fail" });
```

Report fail for review FAIL, red tests or a user retry; these signals are explicit
assertions, **never inferred from task text or arbitrary output**. One quality
failure or **two consecutive failed, stopped/aborted or timed-out runs** demotes
only that live class to shadow. Completed runs reset the failure streak. A pass
after a fail does not clear a demotion. Task and actor settlements share this
path. Replayed/duplicate terminal receipts do not increment the streak twice.
The API accepts an owned run ID, not an arbitrary class, decision or storage path;
the owning Main can also report a completed durable actor activation by its run ID,
after the successful worker handle has been cleaned up. The resident host authorizes
the actual Main control identity and resolves its execution-owned archived decision
receipt. Actor receipts follow the existing actor run archive retention window;
foreign Mains, nested inherited identities and unknown activations are refused.

The append-only host file `<host Pi agent dir>/fabric/model-routing-state.jsonl`
stores the per-class switch/streak and survives process restart; its trust boundary,
permissions, no-link rules and size bound match the ledger. Each demotion also
writes a `revert` row to `model-routing.jsonl`, with class, reset generation,
triggering `decisionId` and `quality-fail` or `consecutive-failures`. Unsafe or
corrupt state falls back to the pin with `revert-state-error`. Outcome storage
failures retain the existing pending receipt/retry fence rather than claiming a
durable save. Quality updates first fsync a `pending` receipt in
`model-routing-quality.jsonl`, then confirm the ledger and state updates, then fsync
a `committed` receipt before acknowledgement. An uncommitted receipt vetoes live
admission across restart and is retried at the next activation; a readable but
unwritable state journal cannot leave the class live. The decision/class/reset join
is preserved, even after successful actor cleanup.

**Rollback:** set `liveClasses: []` in trusted config and refresh Main through its
normal config reload path. The existing resident config refresh writes the overlay
and reuses the ready host. New resident activations reread that same-release,
root/session-fenced policy, including reset tokens; already-admitted model/effort
pins do not change, and no resident restart is required. **Re-enable one reverted class:** keep it
listed and change its trusted `revertReset` token, for example
`"revertReset": { "status-groom": "approved-retry-2" }`. Tokens default to `""`;
use a new, never-reused token for each reset. Old in-flight outcomes stay in their
original generation and cannot undo a new reset. Emptying/re-adding `liveClasses`
alone does not clear a durable revert. Neither reset nor opt-in overrides protection,
network/Jev gates, candidate validation or Paul's floor rule.

## Per-activation actor routing

```ts
const supervisor = await agents.create({
  name: "factory-status",
  instructions: "Check status and routine grooming; return a status line or no-op.",
  residency: "durable",
  runner: "pi",
  transport: "process",
  model: "my-provider/gpt-6.1-sol",
  thinking: "high",
  routeClass: "status-groom",
  protected: false, // trusted issue/role state; never derived from instructions
});
```

`routeClass` is fixed in the actor spec and retained by actor registry reload,
portable definition export, global templates and import. Actor routing accepts
only `status-groom`, requires explicit actor model **and** effort pins, and does
not use parent/global/default-medium settings as pins. Every dispatched activation
prepares the effective caller/project binding with the **same** exact pin and
candidate validation, `decideModelRoute` Choice, threshold and deadline as auto
spawn. Session/call binding overrides may set explicit pins; a missing/invalid
pin refuses activation before inference or launch. Decision rows identify actor,
mailbox activation and run; a new dispatch attempt has a new decision/run joined
by the activation ID. A Choice itself is never retried.

The activation launches its effective **pinned** model/effort by default. If
`status-groom` is listed in `liveClasses`, passes every gate and is not reverted,
it launches the accepted finite candidate model/effort. It retains its original persistent Pi session (no new
child header/session is seeded), while the existing per-request route header
identifies the decision. Unopted actors are unchanged. Protected actors
(`protected: true`: review/security/audit/named passes/`needs-security-pass`) record
`excluded-protected` without evaluating Jev; omitted/unknown flags record
`excluded-unknown`. Protection is the same caller-supplied trusted snapshot as
spawn, not a new authenticated label oracle. Opting in never overrides protection.

Both session and resident owners prepare decisions. Resident owners reuse the
standard Jev client lazily at first eligible activation; there is no startup or
idle inference. Their host-only policy snapshot requires Jev enabled, explicit
network allow and non-enforced Schema. Older snapshots, disabled/unavailable Jev,
network ask/auto/deny or unavailable Schema programs select the pin with
`jev-error`. No credentials are sent in an actor request or taken from its text;
resident evaluation uses the existing Jev configuration/environment, not a new
credential store. Owner close revokes calls and joins pending evaluation and
credential work. Lack of a resident backend is evidence of fallback, not a live
route or a reason to relax gates.

The existing dispatch/outcome recorder and terminal-save retry/retention fence
write a decision before launch and an outcome after settlement, joined by
`decisionId` plus `actorId`/`activationId`/`runId`. Outcomes include completed,
failed or stopped status, admitted pin when known and observed input/output/cache
and cost counters (null when unknown). Failed activations retain the same evidence
as task failures. There is no invented quality score: explicit `routeQuality` assertions drive
reverts, not prompt-text heuristics. A later parity report must join outcomes to
class and observed execution; shadow decisions alone cannot prove savings. Existing ledger write-failure behavior applies
(`record-failed`, pinned launch, retained pending terminal receipt).

## Durable join and child header

Before the transport dispatches, Fabric writes a `decision` row to
`<host Pi agent dir>/fabric/model-routing.jsonl`, outside both the workspace and
ephemeral agent run directory. The host profile (`PI_CODING_AGENT_DIR`, otherwise
`~/.pi/agent`) is the only path source; workspace configuration and requested cwd
cannot redirect it. Append-open uses `O_NOFOLLOW` and `O_NONBLOCK`, rejects links,
non-regular/hard-linked endpoints and unsafe ownership/permissions, and bounds
records to 64 KiB and the ledger to 64 MiB. Writes use append and `fsync`. This records the decision ID,
Main/child native session IDs for new tasks (actor rows instead carry `actorId`,
`activationId` and `runId`, with `childSessionId: null`), class, role pin, candidates, shadow choice,
confidence, probability, fixed reason, latency and time. A seeded native child
session binds the recorded child ID to Pi, not just to the process transport.
For worktree tasks, its header is seeded only after the final worktree cwd is
known and before launch; a failed bind settles preparation failure instead of
launching against the parent checkout. Retries and resumes retain that session
and child ID.
Once the terminal join is durable and workers have exited, `route-session.jsonl`
and `route-quality-receipt.json` are owned run artifacts collected by normal
close/expiry retention (actor decision receipts are first durably archived with
the actor run). Pending
outcomes, unresolved workers, links and unknown content still veto collection.
Terminal `outcome` rows join on `decisionId`, with status, verified admitted model
and effort, observed model, token/cache/cost counters when known and time.
Pre-admission failures record null admission, not the requested model. Confirmed
pre-worker failures (including task/schema/image writes and worktree creation)
get a terminal outcome; an unconfirmed launch is retained by the existing
manager's cleanup obligation rather than falsely reported as a completed child.
Outcome writes use the manager's terminal-save retry/retention fence. Queued
outcomes retry at most three times per settlement/cleanup/close attempt; persistent
failure surfaces a warning and retains the full terminal receipt and run files.
The exact pending ledger row is also kept in `pending-route-outcome.json` for
reconciliation after close/reload; it is removed only after a successful append
and deliberately remains outside the global sweeper's collectable-file allowlist.
Cleanup refuses collection until the outcome has been written. A process
crash before terminal settlement still requires a later outcome reconciler; PR1
does not add one or invent quality/price estimates.

If the decision write fails, pinned work still dispatches with `record-failed` in
the handle and header. If storage stays unavailable, no durable record can be
promised; the retained terminal run lets the owner diagnose/reconcile the gap.
There is no unlogged cheaper execution. Keep the state directory for Light's
later outcome join; deleting the caller's cwd does not delete this host ledger.

Each provider request in the child has:

```
X-Smarty-Route: <class>/<percent-encoded-provider-model>-<effort>/<reasonCode>:<decisionId>
```

This identifies the threshold-accepted choice (executed in live mode), or the pin on fallback.
Actual model attribution comes from the provider/model row and the outcome, not
from treating a shadow header as a live route. The header has no prompts or
free-text reasons. A standalone explicit `-e` child hook mutates Pi's
`before_provider_headers` map in place; it also loads for `extensions: false`.
Worker launch strips inherited route metadata before passing this child's own
header, so unrelated nested explicit-model tasks are not misattributed.

## Real installed-Pi live-path proof

Build this checkout, then run the isolated, keyless proof at the installed runtime:

```sh
nice -n 19 bun run build
nice -n 19 node scripts/prove-model-route-live.mjs \
  /absolute/installed/pi-coding-agent/dist/cli.js "$TMPDIR/router-proof" "$TASK_OUT/real-cli"
```

The script starts the real `pi --mode rpc` CLI with this checkout's `dist/index.js`
through `-e`, private HOME/profile/mesh, and only model/provider-boundary mocks.
Jev fetch is intercepted; native OpenAI model requests reach a loopback HTTP
server that records the actual `X-Smarty-Route` header. No Fabric API, manager,
resident host, worker, registry or header hook is substituted. The proof covers
public auto task spawn/wait and run, live durable `status-groom`, completed actor
cleanup, owning-Main quality fail with pending/committed durability, next pinned
activation, another class remaining live, and Jev error fallback. `evidence.json`
joins actual native model/effort, HTTP header and decision/outcome ledger by ID;
`transcript.jsonl`, copied routing/resident state and `summary.json` retain the
exact HEAD, bundle hash, installed Pi version and all-processes-exited receipt.
These artifacts are the pre-merge isolated entrypoint evidence, not production
model-quality, gateway metering or spending-parity acceptance. The owner must
post the exact-head evidence and separately confirm those gates before live
production enablement.

**Required before merge/install:** a named **Sol-max security pass** on the
header/identity plumbing and local ledger trust boundary. Unit tests and an
isolated real-Pi localhost capture are evidence, not that security approval or
Light's production gateway receipt/weekly quality-and-spend acceptance.
