# Fleet regression retention / API-era migrations

All 619 original fleet test files remain. An AST audit compares 7,219 distinct original literal/template titles against the merged tree (`fleet-test-audit.txt`); a changed title is not automatically a dropped case.

Explicitly restored original regressions:
- Short non-cooperative host calls drain before completed Node/Bun results; retain these original cases alongside new cooperative unawaited-call cancellation tests.
- Reject mutating speculative calls even if eligibility opts in; retain the original case alongside six expanded unsafe-provider/annotation cases.
- Original full-code tool visibility cases now assert Pi 1.0 **model declaration/loadout** visibility, not the active registry used for nested native calls. Their original titles remain; core/captured tools are hidden unless explicitly approved foreground policy allows them, historical model declarations cannot resurrect hidden tools, native callable registry entries remain usable, and release/orchestration preserves native tool selection.
- Retain original Pi RLM/default, stopped-worker resume and scope-debt test titles; resume now also covers explicit pi-durable and scope-debt adds exact-handle admission/custody assertions.

Reviewed/upstream title expansions (not deleted regression intents):
- Conversation metadata header: breadcrumbs moved to native border badge; no redundant header and footer stats remain checked.
- Prewalk model picker: searchable custom dialog and plain-picker fallback both retain no-configured-executor selection behavior.
- Lease-only topology re-acquisition: expanded from peer-only to both session-owned and peer-root cases.
- Tiny conversation height: editor content assertions remain; native border/badge owns breadcrumb presentation.
- Offline nested handoff depth rejection: expanded to both pi and explicit pi-durable while requiring the original executor to finish its assignment.
- Prewalk planning visibility: reviewed require-plan semantics preserve live owed-plan visibility and add immediate retirement after recording plus restoration on rearm/claim/cancel.
- Legacy peer-root relay: reviewed security hardening replaces unauthenticated legacy fallback with explicit rejection when there is no authenticated control protocol. This is intentionally stronger admission, not restoration of an unsafe fallback; authenticated routing remains covered.

Review diffs are retained in `fleet-test-review.patch` and `fleet-test-review-2.patch`. No fleet law or custody assertion was weakened to make a test pass.
