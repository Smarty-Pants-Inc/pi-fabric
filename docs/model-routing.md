# Opt-in task and actor model routing (shadow only)

`agents.spawn` accepts `model: "auto"` for **new session-owned process/Pi tasks only**.
It makes one typed Jev Choice and records it. It **never changes the model or effort
launched**: PR1 always runs the role's explicit pin, even when Jev recommends a
cheaper candidate at high confidence. This is evidence collection, not live routing
or measured savings. Opted-in `status-groom` process/Pi **actor activations** also
collect shadow decisions (below). Mains, durable task agents, handoffs, ordinary
explicit-model calls and the fleet's direct task CLI are not routed.

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
authenticated label oracle**. No prompt-based override or cheaper live route exists.

## Pins and finite candidates

Both pins are required. Call fields take precedence over dedicated role settings
in the usual `fabric.json` configuration. The parent's model, global `agents.model`
and Fabric's default medium effort do **not** qualify as role pins. Pins resolve
only as exact registered provider/model keys or exact targets of an explicitly
configured alias, including after one registry refresh. Missing or invalid pins
refuse spawn with `ModelRoutePinError` (`MODEL_ROUTE_PIN_UNAVAILABLE`) before
inference or dispatch; ordinary non-auto fuzzy selection is unchanged. The
canonical route pin is immutable for the run: initial launch, startup retry and
resume recheck exact availability and reject a changed/unavailable pin before
starting another worker. Every routed worker requires valid actual effort equal
to its pin before sending the prompt; missing, malformed or lower readback
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
Configuring a shadow candidate grants **no permission to run it**. Paul's rule
still applies: no Sol role at medium or below without measured parity and his
approval (Paul's floor rule, smarty-dev#2236). `agents.modelRouting.live` is a
reserved, default-OFF flag: only `false` is accepted; `true` is refused, not an
execution permission. LIVE implementation belongs to PR2 and requires measured
parity plus Paul's floor approval.

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
below threshold. `handle.model/thinking` and the worker's admission still use the pin.

## Per-activation actor shadow routing

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

The activation always launches its effective **pinned** model/effort, regardless
of the would-be choice. It retains its original persistent Pi session (no new
child header/session is seeded), while the existing per-request route header
identifies the shadow decision. Unopted actors are unchanged. Protected actors
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
as task failures. There is no invented quality score: a later parity report must
join outcomes to class and would-be choice, and cannot claim cheaper execution or
savings from shadow decisions alone. Existing ledger write-failure behavior applies
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
is an owned run artifact collected by normal close/expiry retention. Pending
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

This identifies the threshold-accepted would-be choice, or the pin on fallback.
Actual model attribution comes from the provider/model row and the outcome, not
from treating a shadow header as a live route. The header has no prompts or
free-text reasons. A standalone explicit `-e` child hook mutates Pi's
`before_provider_headers` map in place; it also loads for `extensions: false`.
Worker launch strips inherited route metadata before passing this child's own
header, so unrelated nested explicit-model tasks are not misattributed.

**Required before merge/install:** a named **Sol-max security pass** on the
header/identity plumbing and local ledger trust boundary. Unit tests and an
isolated real-Pi localhost capture are evidence, not that security approval or
Light's production gateway receipt/weekly quality-and-spend acceptance.
