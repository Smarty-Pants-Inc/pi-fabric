# Inert fixture forks

A fork copies history, not authority. A fixture launcher must set
`PI_FABRIC_FIXTURE=1` **before loading Fabric**, including on resumes and reloads.
Only the exact value `1` opts in; the mode is latched until extension reload.

Fixtures require a **detected released Pi >= 0.86.0**: that host intercepts native RPC and
TUI user shell requests and fails closed when the restriction hook throws.
Older hosts (including ordinarily supported Pi 0.80.6–0.85.x), prereleases, invalid versions,
and unknown hosts/SDK entry points are refused before Fabric registers anything.
Fabric prints a clear error and terminates the process with status 1: merely
throwing from an extension factory is unsafe because Pi catches the error and
can otherwise continue without the fixture guards. Launch fixtures through a
verifiable Pi CLI entry point; this fixture-only floor does not change normal
Fabric compatibility. The launcher must preserve the marker on every reload.

Fabric then registers only read-only restriction hooks. It does not register
`fabric_exec`, commands, providers, skills, principal/auth capture, roots, control
handlers, actor workers or mailbox readers. The active tool set is `read`,
`grep`, `find`, `ls`, reasserted at session start and each model preflight.
A `tool_call` guard blocks every other name even if reactivated. User shell
execution is blocked too. Every model preflight appends a system note stating that
inherited roles and mailbox requests are history, not authority, and that the
fixture must never act for the original session.

This is **not an OS sandbox or a credential boundary**. The launcher must load
only trusted Fabric/core read tools (no acting extensions or read-tool overrides),
remove write credentials, and enforce filesystem/credential isolation itself.
Fabric does not scrub credentials or make other extensions' lifecycle hooks,
commands or direct host API calls inert. Read tools can read any path the OS
allows. The launcher must own any temporary directory it cleans up. Launcher
repair and installed joint native proof are separate acceptance requirements.

## Duplicate live roots

During normal root publication and heartbeat renames, Fabric compares live root
metadata across participant files, shared state and legacy session entries.
A duplicate non-default name or duplicate session ID under distinct root IDs
emits `fabric.topology.root-collision` with `kind: "alert"`, the reason and both
IDs. The runtime logs a warning and notifies an available UI without starting
a turn. Alerts are deduplicated per pair/match per directory generation and
capped at 1,000 unique collisions. Unnamed `main` roots do not conflict by name;
stale roots do not count.

This is the issue's **alert-only** option, not exclusive registration, process
locking, or fork-ancestry detection. Identical root IDs collapse in the directory;
concurrent processes using that exact ID require a separate process-ownership
protocol. Existing ambiguous-name routing continues to refuse `tell`, `steer`
and `followUp` before publication; exact-ID routing remains available. The alert
alone does not prevent duplicate roots from reading name-addressed work.
