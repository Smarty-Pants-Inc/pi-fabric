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
| Inert DATA | Whole-literal, single bare `echo`, `printf`, `cat`, `ls`, `grep`, `rg`, `head`, `tail`, `wc`; or exact `git log`/`git status`/`git diff` | No expansion, live substitution, process substitution, backquote, shell grammar or unknown head receives this grant. Each head has a fixed inert flag/value allowlist; every unknown option forfeits DATA credit. No `printf -v`, search program/decompressor selector (`--pre`, `--pre-glob`, `--hostname-bin`, `-z`/`--search-zip`), or `sed` grant. |
| Everyday maintenance | Whole-literal nonrecursive `rm`/`rm -f` with explicit operands; or `find` with only the source's fixed non-executing predicates | No `rm -d`, recursive option, glob or expansion; no find exec/ok/delete/file-write action or unknown predicate. This narrow owner exception is not a general argv interpreter. |
| Outside these grants | Pass if no protected token is visible, including after lexical quote/escape removal in opaque syntax | Visible signal/delete tokens refuse; all shred forms refuse. Opaque syntax alone does not refuse. No unrelated-head or executor-denylist DATA credit. |

Complex protected forms are refused as whole calls without interpretation.
Signal/delete allowances require bare receiver spelling; quoted integer PID and
path operands remain supported. Fragmented receivers such as `k'i'll 123`
and `r\m -rf /x` receive no literal-command allowance.
Protected names in opaque scripts/heredocs cannot earn an allowance. Command
prefix assignments cannot set the host TMPDIR or confer a literal-command grant.
Unproved `rm` argv can select recursive options and therefore refuses. This
intentionally trades complex-command precision for a small conservative policy.
Reserved-word or punctuation prefixes do not establish an unrelated-command/DATA
allowance. `rg`/`grep` with `--pre` or `--pre-glob` is execution-bearing, not inert
search DATA. No option denylist: `inertOptions` accepts only the source's listed
single switches, clusters of listed inert switches, and explicit value-taking
selectors; a literal `--` ends options. `printf` accepts only a stdout format
(non-option first word or a literal `--`), never a destination selector. Unknown
flags reach the protected-token/opaque checks and receive no DATA credit.
Dollar quoting, live substitutions/backticks/process substitution, continuation
and heredoc syntax are allowed when no protected token is visible. Quotes and
backslash characters are removed for a second lexical scan, including substitution
bodies; line continuations are removed as a unit and dollar-quote introducers are
stripped to expose their bodies. Fragmented protected spellings still refuse.
This scan does not evaluate shell code or infer execution.

Ordinary commands such as `A=1; echo $A`, `S=a; T=b; echo "$S $T"`,
`printf '%s\n' x | ssh host 'cat'`, Python heredocs, and note appends containing
`$(date -u +%H:%MZ)` and quoted prose now pass. They do not receive inert DATA
credit; they have no protected-token evidence. Protected names in complex calls
still refuse, including `$(kill 123)`, backtick signal calls, and `$(r\m -rf /x)`.

Raw or split-word protected evidence retains the signal/delete class reason.
Evidence exposed only by the opaque quote/escape scan uses a separate reason:
**“Opaque command refused: a protected signal/delete token is visible after
removing quotes or escapes; use a supported literal command.”** It does not ask
for a recorded PID when an opaque deletion spelling triggered the guard.
The opaque scan checks both the original text, with only quote/backslash removal
and no ANSI-C decoding or truncation, and an ANSI-C decoded copy. Either scan's
protected evidence refuses. The decoded copy rewrites every `$'...'` occurrence,
including inside `$(...)`, backticks and outer double quotes, using Bash escapes
(`\t \n \v \f \r \a \b \e \E \\ \' \" \?`, `\xHH`, `\NNN`, `\uHHHH`,
`\UHHHHHHHH`, `\cX`), with a decoded NUL truncating that segment and scanning
continuing after its closing quote. This quote-blind rewrite can discard live text:
`$'` inside double quotes is literal, while a following substitution can still run.
The original scan preserves that evidence. Decoding and truncation may add refusals;
they cannot remove a refusal established by the original scan.
An undecoded escape or an unterminated `$'` beside an opaque receiver (`bash`/`sh`,
`eval`, `env`, `xargs`, `ssh` and similar wrappers) refuses with the opaque reason.
Protected lexical basenames are detected by a **forward token scan**, not a
backtracking path-prefix regex.
No execution-boundary denylist exists: `trap`, `complete`, `compgen`, unknown
heads and future executors receive no generic DATA grant. No cwd, executable
lookup, trap/completion state or deferred action is modelled.

A plain semicolon-separated maintenance list can discount the `find` token when
every segment is whole-literal and uses the fixed inert heads or non-executing
find predicates. Quoted wildcard values for `-name`/`-iname`/`-path`/`-ipath`
require a literal numeric `-maxdepth` in this list grant. For example,
`ls /some/dir ; find /some/dir -maxdepth 4 -name 'relaunch*.sh'` passes.
This does not grant the whole list: signal, rm, shred and xargs evidence still
refuses. Find delete, exec/ok, file-write and unknown predicates receive no grant.
The existing unbounded wildcard find refusal is retained. Pipes and text bodies
with Unicode arrows or dashes pass when they contain no protected evidence.

TMPDIR is read **at each guard call**, never from the submitted shell text. Missing,
relative, expansion-bearing, traversing or shared-root TMPDIR values confer no
recursive-delete allowance. A prefix-sharing sibling is not a descendant. Deleting
the TMPDIR itself is refused; the literal operand must be below it.

The per-session TMPDIR is supplied by the `smarty-role` launcher in `smarty-dev`
(Light's #3232). Sessions launched without TMPDIR get no recursive rm grant.
Launch with a private per-session TMPDIR and use a TMPDIR-scoped scratch directory;
submit its literal absolute descendant path for cleanup. Setting TMPDIR inside
the Bash command cannot establish this grant, and shared temp roots remain refused.
For the launcher side, see smarty-role's per-session TMPDIR in smarty-dev#3232.

## Proof

- `tests/literal-bash-guard.test.ts`: negative/allow pairs for the two rules,
  options, all operands, path boundaries, unknown flags, TMPDIR changes, unrelated
  commands and literal DATA; field regressions for org-note appends, quoted Light
  variables, Python heredocs, printf/SSH pipes and assignment expansion. Protected
  fragments in scripts/substitutions still refuse with accurate opaque diagnostics.
  Astra round-3 F1 covers literal `$'` in double quotes before live escaped signal/delete
  substitutions and backticks, with octal, hex, Unicode and control NUL spellings.
  Quoted NUL prose without protected tokens, nested ANSI-C and actual-NUL cases remain
  covered as scanner-only DATA.
  Org and Light 07:09-07:10Z cases cover a bounded read-only find list, rg pipeline
  and list, Unicode text bodies, and unchanged destructive find refusals.
  Playful-org field repros from smarty-dev#3230 (blind supervisor on main
  `ab8c2575`) pass: `gh api --jq` and `jq` slices, numeric arguments, whole-literal
  grep/rg regex DATA, quoted-delimiter prose heredocs and ordinary Python heredocs.
  Shell `-c` receivers such as `bash -c 'kill 123'` still refuse.
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
- `tests/literal-bash-guard-round4.test.ts`: Astra F5–F7 / security S7–S9,
  fixed-head and narrow maintenance cuts, and increasing nonmatching input sizes.
  Cost coverage has no flaky millisecond threshold; the source scans tokens once.
  Owner-directed changes to old broad allowances retain their command/ID bytes.
- `tests/literal-bash-guard-round5.test.ts`: security S10/S11 destination-writing
  and hostname-executable boundaries, fixed flags across every inert head,
  decompressor selectors, ordinary formatting/search and unchanged rm/find forms.

No corpus command is dispatched to Bash. Each native proof uses real Pi 0.87.1
and the freshly built head as its only extension. Round 4 admits only the owned
PID's literal signal-zero call and an inert grep, and refuses harmless S7/S8 forms
before execution. Internal MAX reviews and seal chains are not acceptance gates.

## Owner gate / limits

The publishing owner decides the new PR and landing checks; the lane does not push.
The TMPDIR value must be trusted, private host/session configuration. Containment
is lexical, not filesystem or lifetime ownership: symlinks, concurrent path
replacement and hostile ambient shell aliases/functions are not proved here.
Likewise this is a mistake guard for visible protected command spellings, not a
sandbox for dynamic command names, custom script files, arbitrary executors or
other programming languages. Variable-assembled receivers such as `$a$b` remain
an accepted limit: the guard never resolves variable bindings or predicts their
expansions. Literal PIDs are not proof of process ownership.
Protected receiver words outside the whole-literal inert DATA grant still refuse:
a piped search (`grep -E 'kill|pkill' f | head -n 5`), an unlisted flag
(`grep -A2 'pkill' f`), a `sed` program (`sed -n '/kill 123/p' f`; sed has `e`/`w`
execution and write commands, so it gets no DATA grant) and a heredoc body naming
a receiver (`cat > f.md <<'EOF'` with `kill`). The guard does not parse pipelines,
sed programs or heredoc bodies, so these field false positives are accepted limits.
Keep those isolation/ownership questions with #2313; do not revive old analyzer
certificates or imply installation/Windows/overall-CI acceptance from lane tests.
#2497 stays parked until the publishing owner explicitly resumes it.
