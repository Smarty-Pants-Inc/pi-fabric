# Literal-only Bash safety guard

This is the scope-cut replacement for PR166's analyzer, based on main
`66856801facc1c2f2889722ac0a0aa0936af1400`.
Authority: https://github.com/Smarty-Pants-Inc/pi-fabric/pull/166#issuecomment-5930215578.
The old analyzer and its findings remain historical records on #2275; they are not
ported into this guard. The existing main `pattern-kill.ts` remains unchanged for
its legacy API/tests, but **the Bash hook no longer imports or calls it**.

## Fits

The awaited native `bash` pre-execution hook calls one small, first-use-loaded
`bashGuardRefusal(command, process.env.TMPDIR)` function. It splits literal words;
it does not evaluate assignments, variables, command substitutions, shell `-c`,
output, cwd changes, attributes, files, descriptors or producer provenance.

| Class | Admitted form | Everything else in the class |
| --- | --- | --- |
| Process signals | One bare `kill` with positive literal integer PID operands, optionally one literal `-SIGNAL` and/or `--` | Refuse with **“use the PID you recorded”**. `pkill`, `killall`, variables, substitutions, job selectors, wrappers, nested/compound commands and unproved operands get no allowance. |
| Recursive deletion | One bare `rm` using `-r`/`-R` and optional `-f`, or literal `--recursive`/`--force`, optionally `--`; **all** operands literal absolute descendants of the host session's private TMPDIR | Refuse with **“delete only inside your own TMPDIR”**. No shared root, TMPDIR equality, `..`, globs, expansions, relative paths, mixed outside operands or unproved flags. `find -delete` and `shred -r`/`-u` (including short clusters)/`--remove[=...]` cannot receive this allowance. |
| Unrelated command/DATA | Pass unchanged | No general command allowlist, arbitrary execution interpreter or output-safety claim. Literal examples in ordinary DATA argv/comments remain DATA. |

Complex protected forms are refused as whole calls rather than interpreted.
Protected names in opaque scripts/heredocs cannot earn an allowance. Command
prefix assignments cannot set the host TMPDIR or confer a literal-command grant.
Unproved `rm` argv can select recursive options and therefore refuses. This
intentionally trades complex-command precision for a small conservative policy.
Reserved-word or punctuation prefixes do not establish an unrelated-command/DATA
allowance. `rg`/`grep` with `--pre` or `--pre-glob` is execution-bearing, not inert
search DATA. Unsupported dollar quoting, continuation or heredoc syntax cannot
hide visible protected names. Known opaque executors with inner quotes/escapes
refuse rather than decoding quote-concatenated or escaped receiver spellings;
when the protected class itself is unproved, the stable signal reason is used.
Protected basenames behind a lexical relative or absolute path prefix remain
protected inside opaque execution strings; a path-regex miss is not a DATA grant.
`trap` action strings are executable text, not unrelated argv DATA. No cwd,
executable lookup, trap state or deferred-handler execution is modelled.

TMPDIR is read **at each guard call**, never from the submitted shell text. Missing,
relative, expansion-bearing, traversing or shared-root TMPDIR values confer no
recursive-delete allowance. A prefix-sharing sibling is not a descendant. Deleting
the TMPDIR itself is refused; the literal operand must be below it.

## Proof

- `tests/literal-bash-guard.test.ts`: negative/allow pairs for the two rules,
  options, all operands, path boundaries, unknown flags, TMPDIR changes, unrelated
  commands and literal DATA.
- `tests/literal-bash-guard-hook.test.ts`: actual extension registration, cold
  import/idle/non-Bash no-load, awaited first refusal, one call per valid Bash event,
  guard-time host TMPDIR, refusal before timeout mutation and retained wait gate.
- `tests/literal-bash-guard-review-fixtures.test.ts`: historical exact command bytes,
  including complex formerly-safe halves which now refuse under the scope cut.
  Fixtures are DATA only, not executable shell probes.
- `tests/literal-bash-guard-classes.test.ts`: authoritative early F1–F10 class
  witnesses and explicitly **derived** F38/F39 witnesses. The original F38/F39
  reviews supplied prose, not command code; derived examples are not represented
  as verbatim reviewer text. F11–F57 exact fixtures are in the historical matrix.
- Typecheck, fresh build, stable compiled lazy entry, unchanged startup caps,
  host-free lazy graph and cold/first-use checks. No old analyzer import is in the
  new startup closure. A same-host fresh-process startup comparison reports CPU
  and wall samples, not a flaky millisecond threshold.

- `tests/literal-bash-guard-round2.test.ts`: exact Astra F1–F3 / security S1–S4
  fixtures, explicitly derived fragmented-script spellings, and independent
  literal/inert-DATA controls.
- `tests/literal-bash-guard-round3.test.ts`: derived relative shell/eval receivers
  and EXIT/zero trap action witnesses, with independent bare-literal, non-protected
  trap and inert printed-DATA controls.

No corpus command is dispatched to Bash. Round 2 additionally requires one real
Pi 0.87.1 run on the freshly built head: only an owned PID's literal signal-zero
call is admitted; a pattern signal and outside recursive delete must refuse before
execution. Internal MAX reviews and seal chains are not acceptance gates.

## Owner gate / limits

The publishing owner decides the new PR and landing checks; the lane does not push.
The TMPDIR value must be trusted, private host/session configuration. Containment
is lexical, not filesystem or lifetime ownership: symlinks, concurrent path
replacement and hostile ambient shell aliases/functions are not proved here.
Likewise this is a mistake guard for visible protected command spellings, not a
sandbox for dynamic command names, custom script files, arbitrary executors or
other programming languages. Literal PIDs are not proof of process ownership.
Keep those isolation/ownership questions with #2313; do not revive old analyzer
certificates or imply installation/Windows/overall-CI acceptance from lane tests.
#2497 stays parked until the publishing owner explicitly resumes it.
