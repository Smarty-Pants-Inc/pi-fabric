# Acceptance ledger — smarty-dev#5049 org-pane chatter collapse

Fits / why: extend #546's existing display projection, rather than add another feed. This directly serves Paul's request to see the org's replies through incoming chatter, while keeping the fleet's delivery/context and native controls intact.

## Execution path traced before editing

`src/extension.ts` registers incoming renderers with `principalViewIncomingMode(state.provisionalConfig().ui.principalView)` → `src/ui/principal-view.ts` uses the existing org-role default and public tool-expansion API → `src/ui/incoming-messages.ts` checks native expansion/mode, then inbox receipts, then org-sender/principal-recipient full exceptions, then the dim one-line native carrier summary.

The fix changes only the full-exception guard. `MainAgentController.#send` still constructs the native agent carrier and `details.items/data`; the real peer's route into it is exercised in the PTY proof. The inbox carrier is made by the unchanged `rootInboxMessage` builder. No receiver, bootstrap, attribution, model-context, message transformation, public symbol, registration or configuration entry was changed.

## Concrete checks

| Check | Evidence | Result |
| --- | --- | --- |
| Exact org aliases/stamps collapse all agent/actor/mail/inbox/summary carriers regardless of sender/recipient markers | Four new cases in `tests/incoming-messages.test.ts`; 4 expected failures before fix, full suite 42 passed after | PASS |
| Mixed principal-addressed/org-labeled burst uses one dim line | Targeted regression plus real 3-followUp/mail burst | PASS |
| Delivered inbox shadows still suppress, unseen work remains, Ctrl+O reveals native details | Existing/new dedup tests and native PTY inbox/expansion | PASS |
| Other sessions keep #546 full-reply rules, including role precedence counterexample | Existing exemptions tests plus new org→project role counterexample | PASS |
| Principal input and local assistant reply remain full | Renderer registrations unchanged; real TUI screen captures | PASS |
| Exact native expansion; principal view off still native | Targeted cases and physical Ctrl+O expansion/recollapse in the PTY | PASS |
| No context or delivery mutation | JSON/LLM-byte assertions; native branch equality through toggles; complete bodies on org provider wire | PASS |
| Real incoming peer route, not self echo | Separate native Pi RPC peer; sender id differs from receiver; three native followUp receipts | PASS |
| Before/after on actual built bundles | Before source pinned to local `origin/main` c9b9a1d44cb723589bcbd54d46ed7fc7814c5993; after source 2166778f87619a89fa59892ee30a5ad451522469 | PASS |
| Existing incoming registrations/configuration stay unchanged | Same four custom types and `session_start` only; test checks no context/reply registrations; source diff is one guard | PASS |
| Fresh types/build | `typecheck.log`, `build.log`; structural build/lazy-artifact checks included | PASS |
| Visual evidence inspected | Before/after/expanded PNGs opened via read tool; contact sheet and untouched ANSI/text captures | PASS |
| Local source commit / clean tracked tree | Fleet bot commit 2166778f87619a89fa59892ee30a5ad451522469 | PASS |

## Explicit limits

This is the requested local source and proof handoff, not an installed release or Paul's live device acceptance. No child independent audit was run, as assigned. No full suite or destructive local test was run. No push, GitHub or SSH was used. Native local helpers are unpinned; the lead owns reviewed/pinned release builds, publication gates, installation and any principal-device acceptance.

Immediate rollback: Ctrl+O or `/principal-view off`. Source rollback: revert the commit and rebuild.
