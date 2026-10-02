import { describe, expect, it } from "vitest";
import { bashGuardRefusal, DELETE_REASON, OPAQUE_REASON, SIGNAL_REASON } from "../src/core/literal-bash-guard.js";
const root = "/tmp/session-literal-guard";
// Derived witnesses for the review's described classes; all commands are scanner-only DATA.
const pairs = [
  { id: "S7 escaped signal substitution", reason: OPAQUE_REASON,
    refused: ['echo "$(p\\kill worker)"', 'printf "%s" "$(p\\kill worker)"'],
    simple: ["kill -00 4242", "echo '$(p\\kill worker)'"] },
  { id: "S7 quote-concatenated signal substitution", reason: OPAQUE_REASON,
    refused: ['echo "$(p\'ki\'ll worker)"', 'printf "%s" "$(p\'ki\'ll worker)"'],
    simple: ["kill 4242", "printf '%s' 'pkill worker'"] },
  { id: "S7 escaped deletion substitution", reason: OPAQUE_REASON,
    refused: ['echo "$(r\\m -rf /outside)"', 'printf "%s" "$(r\\m -rf /outside)"'],
    simple: ["rm -rf /tmp/session-literal-guard/a", "echo '$(r\\m -rf /outside)'"] },
  { id: "S7 quote-concatenated deletion substitution", reason: OPAQUE_REASON,
    refused: ['echo "$(r\'m\' -rf /outside)"', 'printf "%s" "$(r\'m\' -rf /outside)"', "printf '%s' \"$(r'm' -rf /outside)\""],
    simple: ["rm -r /tmp/session-literal-guard/a", "printf '%s' 'rm -rf /outside'"] },
  { id: "S8 compgen command signal action", reason: SIGNAL_REASON,
    refused: ["compgen -C 'pkill worker' -- marker", "complete -C 'pkill worker' command"],
    simple: ["compgen -A builtin -- pri", "echo 'compgen -C pkill worker'"] },
  { id: "S8 compgen command delete action", reason: DELETE_REASON,
    refused: ["compgen -C 'rm -rf /outside' -- marker", "complete -C 'rm -rf /outside' command", "compgen -C 'rm -rf /outside'"],
    simple: ["compgen -W 'alpha beta' -- a", "printf '%s' 'compgen -C rm -rf /outside'"] },
  { id: "S8 word-list signal substitution", reason: SIGNAL_REASON,
    refused: ["compgen -W '$(pkill worker)' -- marker", "complete -W '$(pkill worker)' command", "compgen -W '$(pkill worker)'"],
    simple: ["compgen -W 'alpha beta' -- a", "grep -F pkill README.md"] },
  { id: "S8 word-list delete substitution", reason: DELETE_REASON,
    refused: ["compgen -W '$(rm -rf /outside)' -- marker", "complete -W '$(rm -rf /outside)' command", "compgen -W '$(rm -rf /outside)'"],
    simple: ["compgen -W 'alpha beta' -- a", "rg 'rm -rf' docs"] },
  { id: "S8 function completion protected token", reason: SIGNAL_REASON,
    refused: ["complete -F pkill command", "compgen -F pkill -- marker"],
    simple: ["complete -p", "printf '%s' 'complete -F pkill'"] },
  { id: "structural cut: unknown receiver argv is not DATA", reason: SIGNAL_REASON,
    refused: ["unlisted pkill worker", "sed -n 'e pkill worker' file", "git log --format=pkill", "sh -c '/usr/bin/pkill worker'", "sh -c '../bin/pkill worker'"],
    simple: ["echo pkill", "git status", "git log", "git diff"] },
  { id: "structural cut: find execution / write predicates", reason: DELETE_REASON,
    refused: ["find /own -exec rm -f {} ;", "find /own -execdir printf ok ;", "find /own -ok printf ok ;", "find /own -okdir printf ok ;", "find /own -fprint /outside", "find /own -fprintf /outside '%p'", "find /own -name '*.ts' -print"],
    simple: ["find /own -type f", "find /own -name literal.ts -print", "find /own -maxdepth 1 -empty"] },
  { id: "structural cut: nonrecursive literal maintenance", reason: DELETE_REASON,
    refused: ["rm -d /outside", "rm -f $P", "rm -f /outside*", "shred /outside"],
    simple: ["rm /outside/file", "rm -f /outside/file", "rm -f -- /outside/file", "cat literal.txt", "ls /own", "head file", "tail file", "wc file"] },
];
describe("PR231 round 4 fixed inert-head admission", () => {
  for (const pair of pairs) {
    it(`${pair.id}: refusal`, () => {
      for (const command of pair.refused) expect(bashGuardRefusal(command, root), command).toBe(pair.reason);
    });
    it(`${pair.id}: SIMPLE / inert DATA allowance`, () => {
      for (const command of pair.simple) expect(bashGuardRefusal(command, root), command).toBeUndefined();
    });
  }
  // No timing thresholds: increasing nonmatching input exercises the formerly triangular suffix
  // scan. The implementation is a bounded forward token pass, not an unanchored path-prefix regex.
  it.each([1024, 8192, 65536, 262144])("S9 linear nonmatching token scan: %i bytes", size => {
    expect(bashGuardRefusal(`sh -c '${"=".repeat(size)}'`, root)).toBeUndefined();
    expect(bashGuardRefusal(`sh -c '${"x".repeat(size)}'`, root)).toBeUndefined();
    expect(bashGuardRefusal(`sh -c '${"=".repeat(size)}./x/pkill worker'`, root)).toBe(SIGNAL_REASON);
  });
});
