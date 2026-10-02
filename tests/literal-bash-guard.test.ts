import { describe, expect, it } from "vitest";
import { bashGuardRefusal, DELETE_REASON, OPAQUE_REASON, SIGNAL_REASON } from "../src/core/literal-bash-guard.js";

const tmpdir = "/tmp/session-literal-guard";
describe("literal-only signal and recursive-delete guard", () => {
  const signals = [
    "pkill -f worker", "killall worker", "/usr/bin/pkill worker", "killall5",
    "kill $(pgrep worker)", "kill `pgrep worker`", "kill $P", 'kill "$P"',
    "P=4242; kill $P", "kill %1", "kill worker", "kill -l", "kill 0", "kill -TERM -1",
    "kill -s TERM 4242", "kill 4242 worker", "kill 42*", "kill {42,43}",
    "pgrep worker | xargs kill", "xargs kill 4242", "sudo kill 4242",
    "sh -c 'kill 4242'", "ssh host 'pkill worker'", "env -S 'kill 4242'",
    "pki\\\nll worker", '"ki"ll $P', "\\kill $P", "kill 4242 > /outside",
    "kill 4242 && printf ok", "kill\r 4242", "kill '4242", "kill $'42'",
    "k'i'll 123", '"pk"ill worker', 'echo "$(kill 123)"', 'echo `kill 123`',
    'echo `k\'i\'ll 123`', 'cat <(p\\kill worker)',
  ];
  const deletes = [
    "rm -rf /", "rm -r /tmp", "rm -R /var/tmp", "rm --recursive /outside",
    "rm -rf $D", 'rm -rf "$D"', "D=/own; rm -rf $D", "rm -rf $(printf /own)",
    "rm -rf /tmp/session-literal-guard/../other", "rm -rf /tmp/session-literal-guard/*",
    "rm -rf '/tmp/session-literal-guard/*'", "rm -rf /tmp/session-literal-guard2/a",
    "rm -rf /tmp/session-literal-guard/a /outside", "rm -rf .local/owned",
    "rm -rf /tmp/session-literal-guard", "rm -rf /tmp/session-literal-guard/",
    "rm -rf /tmp/session-literal-guard/a > /outside", "sudo rm -rf /tmp/session-literal-guard/a",
    "find /tmp/session-literal-guard -delete", "shred -r /tmp/session-literal-guard/a",
    "shred --remove /tmp/session-literal-guard/a", "sh -c 'rm -rf /outside'",
    "rm $OPTS /outside", 'rm "$OPTS" /outside', "rm -r -- /tmp/session-literal-guard/$D",
    "rm -ri /tmp/session-literal-guard/a", "TMPDIR=/outside rm -rf /outside/a",
    'env --split-string="rm -rf /outside"', "env -iS'rm -rf /outside'", 'env -S "rm $OPTS /outside"',
    "r\\m -rf /x", "r'm' -rf /tmp/session-literal-guard/a", 'cat >(r\\m -rf /x)',
    "python3 - <<'EOF2'\nr\\m -rf /x\nEOF2",
  ];
  it.each([
    "find /x -delete", "find /x -exec rm -rf {} +", "find / -name '*.lock' -delete",
  ])("keeps destructive find actions refused: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(DELETE_REASON);
  });
  it.each([
    "ls /some/dir ; find /some/dir -maxdepth 4 -name 'relaunch*.sh'",
    "rg foo /x | rg bar ; ls /y",
    "printf 'a → b — c\\n' | cat",
    'echo "status → done — ok"',
  ])("allows org and Light 07:09–07:10Z field repro: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
  });
  it.each([
    "ls /x ; find /x -delete", "ls /x ; find /x -exec rm -rf {} +",
    "ls /x ; find /x -maxdepth 4 -execdir printf ok ;",
    "ls /x ; find /x -maxdepth 4 -fprint /outside",
    "ls /x ; find /x -maxdepth 4 -unknown",
    "ls /x ; find /x -maxdepth 4 -name relaunch*.sh",
    "ls /x ; find /x -name '*.ts' -print",
    "ls /x ; find $D -maxdepth 4 -name '*.sh'",
    "ls /x ; sh -c 'find /x -maxdepth 4 -name literal.sh'",
    "ls /x ; find /x -maxdepth 4 -name '*.sh' ; rm -rf /outside",
  ])("keeps read-only find list proof narrow: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(DELETE_REASON);
  });
  it.each([
    "echo pkill ; find /x -maxdepth 4 -name '*.sh'",
    "ls /x ; find /x -maxdepth 4 -name '*.sh' ; kill 123",
  ])("never discounts signals alongside a read-only find: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(SIGNAL_REASON);
  });
  it.each([
    "ls /x ; find /x -type f",
    "ls /x ; find /x -maxdepth 4 -name 'relaunch*.sh' -print ; git status",
    "printf '%s' 'a;b' ; find /x -maxdepth 4 -name 'relaunch*.sh'",
  ])("allows only fixed literal find maintenance lists: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
  });
  it.each(signals)("refuses signal class: %s", command => expect(bashGuardRefusal(command, tmpdir)).toBe(SIGNAL_REASON));
  it.each(deletes)("refuses delete class: %s", command => expect(bashGuardRefusal(command, tmpdir)).toBe(DELETE_REASON));
  it.each([
    "kill 4242", "kill -TERM 4242", "kill -9 4242 7777", "kill -00 4242", "kill -- 4242", "kill '4242'",
    "rm -r /tmp/session-literal-guard/a", "rm -rf /tmp/session-literal-guard/a /tmp/session-literal-guard/b",
    "rm -fr /tmp/session-literal-guard/a", "rm -R /tmp/session-literal-guard/a",
    "rm --recursive --force -- /tmp/session-literal-guard/a", "rm -rf '/tmp/session-literal-guard/with space'",
    'rm -rf "/tmp/session-literal-guard/with space"',
    "rm -r /tmp/session-literal-guard/./a", "rm -r /tmp/session-literal-guard//a",
  ])("allows a literal supported form: %s", command => expect(bashGuardRefusal(command, tmpdir)).toBeUndefined());
  it.each([
    "printf '%s' 'pkill worker; rm -rf /tmp'", "echo kill", 'echo "never use pkill"',
    "grep -n pkill README.md", "rg 'rm -rf' docs", "pgrep worker", "ls -al /tmp", "pwd",
    "printf '%s' '$P'", "rm -f /tmp/a", "rm /tmp/a", "find /tmp -type f", "shred /tmp/a",
    "echo ok; git status", "P=4242; printf '%s' $P",
  ])("passes unrelated commands and inert literal DATA: %s", command => {
    // Same historical ID/command; owner-directed round-4 cut refuses all shred forms.
    expect(bashGuardRefusal(command, tmpdir)).toBe(command === "shred /tmp/a" ? DELETE_REASON : undefined);
  });
  it.each([
    `N=/path/ORG-NOTES.md; echo "$(date -u +%H:%MZ) text containing 'restored', 'restarted', 'Sessions'" >> "$N"; echo ok`,
    'S=a; T=b; echo "$S $T"',
    "python3 - <<'EOF2'\nprint('restored Sessions')\nEOF2",
    "printf '%s\\n' x | ssh host 'cat'",
    "A=1; echo $A",
    'echo `date -u +%H:%MZ`', "cat <(printf x)", "cat >(cat)",
    "printf %s $'restored\\n'", 'echo $"Sessions"', "echo \\\nrestored",
    "ssh host 'printf %s\\n x'", "echo restoration; echo restarted; echo Sessions",
  ])("allows ordinary opaque syntax without a protected token: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
  });
  it.each([
    'echo "$(p\\kill worker)"', 'echo "$(k\'i\'ll 123)"', 'echo "`k\'i\'ll 123`"',
    'cat <(\'p"ki"ll worker\')', 'cat >(\'r"m" -rf /x\')',
    'echo "$(r\'m\' -rf /x)"', "sh -c 'p\"ki\"ll worker'", "sh -c 'r\\m -rf /x'",
    "sh -c $'p\"ki\"ll worker'", 'sh -c $"r\'m\' -rf /x"',
    "python3 - <<'EOF2'\n'r\"m\"' -rf /x\nEOF2",
  ])("refuses opaque protected spelling with an accurate reason: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(OPAQUE_REASON);
    expect(OPAQUE_REASON).not.toBe(SIGNAL_REASON);
    expect(OPAQUE_REASON).toMatch(/protected signal\/delete token/);
  });
  it.each([
    "gh api repos/o/r/pulls --jq '.[] | .head.sha[0:8]'",
    "jq -r '.items[0:8][] | .id' f.json",
    "sleep 5; date -u +%s",
    "head -n 20 f",
    "grep -E '^(kill|pkill)-like text$' f",
    "rg -n '^(kill|pkill)' src",
    "grep -E '^(foo|bar)-like text$' f",
    "cat > f.md <<'EOF'\n# Notes\n\nRun the job, then check status.\n\n```sh\nls -al /tmp\necho ok\n```\nEOF",
    "python3 - <<'EOF'\nimport json, sys\nfor i in range(3):\n    print(json.dumps({'i': i}))\nEOF",
  ])("allows playful-org field repro (smarty-dev#3230): %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
  });
  it.each([
    "bash -c 'kill 123'", 'bash -c "pkill worker"', "bash -lc 'kill 123'", "zsh -c 'kill 123'",
    "/bin/bash -c 'kill 123'", "bash -c kill\\ 123", 'bash -c "$(printf x) kill 123"',
  ])("refuses opaque shell -c receivers: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(SIGNAL_REASON);
  });
  it("refuses fragmented shell -c receivers with the opaque reason", () => {
    expect(bashGuardRefusal("bash -c 'k\\ill 123'", tmpdir)).toBe(OPAQUE_REASON);
  });
  // Astra F1: Bash decodes ANSI-C escapes into separators before the inner shell splits words.
  // Scanner-only DATA strings; never executed.
  it.each([
    "bash -c $'pkill\\tworker'", "bash -c $'rm\\t-rf /outside'", "bash -c $'pkill\\nworker'",
    "bash -c $'pkill\\x20worker'", "bash -c $'pkill\\040worker'", "bash -c $'pkill\\u0020worker'",
    "bash -c $'rm\\U00000020-rf /outside'", "sh -c $'kill\\v123'", "bash -c $'kill\\t-0 999999999'",
    "eval $'pkill\\fworker'", "bash -c $'p\\x6bill worker'", "bash -c $'\\162m -rf /outside'",
  ])("refuses ANSI-C decoded separators/letters in opaque receivers: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(OPAQUE_REASON);
  });
  // Astra round-3 F1: $' and escaped NUL are literal inside double quotes, but the
  // substitutions are live. Quote-blind decoding must not erase their protected evidence.
  // Scanner-only DATA: none of these signal/delete strings are dispatched to a shell.
  it.each(["\\000", "\\x00", "\\u0000", "\\U00000000", "\\c@"].flatMap(nul => [
    `echo "$'x${nul} $(p\\kill worker)'"`,
    `echo "$'x${nul} $(r\\m -rf /outside)'"`,
    `echo "$'x${nul} \`p\\kill worker\`'"`,
    `echo "$'x${nul} \`r\\m -rf /outside\`'"`,
  ]))("refuses protected evidence after literal double-quoted NUL spelling: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(OPAQUE_REASON);
  });
  it.each(["\\000", "\\x00", "\\u0000", "\\U00000000", "\\c@"].map(nul =>
    `echo "$'x${nul} y'"`,
  ))("allows quoted literal NUL prose without protected evidence: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
  });
  // Astra round-2 F1: outer quotes must not hide nested ANSI-C decoding, and NUL ends the segment.
  // These signal/delete witnesses are scanner DATA only, never commands to execute.
  it.each([
    "echo \"$(bash -c $'pkill\\tworker')\"",
    "echo \"$(bash -c $'rm\\t-rf /outside')\"",
    "echo \"`bash -c $'pkill\\tworker'`\"",
    "echo \"`bash -c $'rm\\t-rf /outside'`\"",
    "echo \"$'pkill\\tworker'\"", // Deliberate conservative over-decoding of literal double-quoted text.
    ...["\\000", "\\x00", "\\u0000", "\\c@"].flatMap(nul => [
      `env $'pkill${nul}suffix' -f worker`,
      `env $'rm${nul}suffix' -rf /outside`,
    ]),
    "env $'pkill\\000suff\\'ix' -f worker",
    "env $'echo\\000discard' $'pkill\\tworker'",
    "echo \"$(bash -c $'kill\\t-0 999999999')\"",
    "env $'kill\\000x' -0 999999999",
  ])("refuses nested or NUL-terminated ANSI-C protected evidence: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(OPAQUE_REASON);
  });
  it.each([
    "env $'echo\\000\\q'", "env $'echo\\000unterminated",
  ])("keeps malformed ANSI-C opaque receivers fail-closed after NUL: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(OPAQUE_REASON);
  });
  it.each([
    "bash -c $'echo\\000pkill\\tworker'", "env $'echo\\000rm\\t-rf /outside'",
    "echo \"$(bash -c $'echo\\tok')\"", "echo \"`bash -c $'echo\\tok'`\"",
  ])("drops NUL suffixes and still allows harmless nested ANSI-C text: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
  });
  it.each([
    "bash -c $'echo\\qx'", "bash -c $'echo unterminated", "sh -c $'\\xZZ'", "eval $'\\k'",
    "echo \"$(bash -c $'echo\\qx')\"", "echo \"$(bash -c $'echo unterminated)\"",
    "echo \"`bash -c $'echo\\qx'`\"", "echo \"`bash -c $'echo unterminated`\"",
  ])("refuses undecodable or unterminated ANSI-C quoting in an opaque receiver: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(OPAQUE_REASON);
  });
  it.each([
    "printf %s $'restored\\tSessions\\n'", "bash -c $'echo\\tok'", "printf %s $'\\x41\\101\\u0041\\cA'",
  ])("still allows decoded ANSI-C text without a protected token: %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
  });
  it.each([
    "grep -E 'kill|pkill' f | head -n 5", "grep -A2 'pkill' f", "sed -n '/kill 123/p' f",
    "cat > f.md <<'EOF'\nDon't kill the session.\nEOF",
  ])("keeps protected words outside the whole-literal DATA grant refused (accepted limit): %s", command => {
    expect(bashGuardRefusal(command, tmpdir)).toBe(SIGNAL_REASON);
  });
  it("keeps variable-assembled receivers as an explicit mistake-guard limit", () => {
    expect(bashGuardRefusal("$a$b 123", tmpdir)).toBeUndefined();
  });
  it.each([undefined, "", "/", "/tmp", "/var/tmp", "relative", "/tmp/session/../other", "$TMPDIR"])(
    "never grants recursive deletion from an absent/shared/unproved TMPDIR: %s", root => {
      expect(bashGuardRefusal("rm -rf /tmp/session-literal-guard/a", root)).toBe(DELETE_REASON);
      expect(bashGuardRefusal("kill 4242", root)).toBeUndefined();
    },
  );
  it("reads TMPDIR per invocation, never from a command assignment", () => {
    const command = "rm -r /tmp/session-literal-guard/a";
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
    expect(bashGuardRefusal(command, "/tmp/other-session")).toBe(DELETE_REASON);
    expect(bashGuardRefusal("TMPDIR=/tmp/session-literal-guard rm -r /tmp/session-literal-guard/a", "/tmp/other-session")).toBe(DELETE_REASON);
  });
});
