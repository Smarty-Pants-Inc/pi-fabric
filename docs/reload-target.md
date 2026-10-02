# Resource-target reload v1

Trusted extensions can advertise a profile resource to Fabric's existing native reload
controller through `pi.events`. Owner labels confer no authority. No public busy query exists.

## Events

Emit `pi-fabric:reload-target:v1` with string fields
`{ requestId, owner, resource, loaded, configured, reason }`.
Subscribe first to `pi-fabric:reload-target:v1:result`. Replies contain
`{ requestId, owner, resource, accepted, reason, target }`; `target` is a canonical path or `null`.
Replies can be asynchronous: profile validation loads at the first request, outside idle startup.
Correlate by a unique `requestId`, never by owner. Later invalidation can refuse that ID again.

`resource` and `loaded` are the same canonical absolute extension entrypoint that this runtime
loaded. `configured` is the canonical absolute entrypoint the producer observed in the active
profile. Fabric independently rereads that profile's `settings.json`; configured is a claim,
not authority. Paths must exist and be files ending in `.ts` or `.js`.

## Binding and admission

Register after Fabric's session start, before installation, with all three paths equal.
`accepted: true, reason: "bound-unchanged"` retains the profile-entry binding without scheduling
reload. Repeat with the same resource and loaded paths after the profile pointer changes.
A validated change returns `accepted: true, reason: "pending"`. An idle retry checks the same
gate if no further Main turn settles. Advertising never injects a command synchronously.

V1 supports exact local `extensions` entries and local `packages` entries with exact extension
manifests or index entrypoints. It conservatively refuses package filters, remote sources, glob
and directory-scan discovery. A binding retains its list slot, package name and relative
entrypoint; list length and other entries in that list must remain unchanged. Reordering or
concurrent unrelated changes require fresh enrollment in a new runtime. Explicit `-e` or
`--extension` copies and outside-profile copies never follow the profile pointer.

Pending targets use the same controller, busy count, Escape latch, aborted/error hold, settle
scheduler and native command as Fabric package changes. Native execution rechecks the target,
Main idle/compaction, settling, prompt preflight, queued messages, editor draft and global UI
holds, plus child agents, in-flight actors, shell and Jev work. Extension followups cannot lift
an explicit halt. Only user input resumes automatic admission. Each exact resource target gets
one native attempt per session, including attempts whose reload failed. Session switch and
shutdown clear bindings, pending targets, queued-command tokens and timers.

## Refusals and host limits

Refusal reasons include `invalid-request`, `no-active-session`, `missing-path`,
`noncanonical-path`, `resource-loaded-mismatch`, `explicit-extension`, `outside-profile`,
`profile-unreadable`, `profile-validation-unavailable`, `ambiguous-profile-entry`,
`ambiguous-profile-change`,
`missing-or-removed-profile-target`, `removed-profile-entry`, `configured-mismatch`,
`target-reverted`, `target-changed`, `unbound-resource`, `session-changed`,
`auto-reload-disabled` and `already-attempted`.
A busy or temporary UI hold defers an admitted target; it grants no permission to reload.

A missing safety check fails closed with `unsupported-host:<check>`. Pi 0.87.0 lacks
`isPromptPending` and `isSettling`; older hosts without the global UI query refuse changed
resource targets with `unsupported-host:global-dialog/editor-hold-query` once the two input
checks are present. Fabric detects the optional `ctx.ui.holdState()` API by capability, not
version. A host reporting `dialog`, `custom` or `editor` defers the pending target with
`ui-hold:<kind>` until the hold clears. A query failure refuses resource targets with
`unsupported-host:ui-hold-query-failed`. No UI monkey-patching is used in the adapter.

Fabric's own package reloads also defer on a supported UI query's hold or failure, including
at native command execution. Hosts without that API retain legacy Fabric-only behavior.
Work and UI holds share the existing retry and once-per-continuous-hold notice after ten
minutes; no new timer or controller is introduced. Existing opt-outs are retained.

On a completed native reload,
`ops.fabric.reloaded` includes `owner`, `resource` and canonical `target` for a resource request.
This report describes the native handoff, not an independent loaded-Code enrollment receipt.
