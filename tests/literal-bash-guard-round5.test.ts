import { describe, expect, it } from "vitest";
import { bashGuardRefusal, DELETE_REASON, SIGNAL_REASON } from "../src/core/literal-bash-guard.js";
const root = "/tmp/session-literal-guard";
// Security S10/S11 describe the class without code blocks. These are derived DATA witnesses;
// no tool, subprocess or shell dispatch occurs in this suite.
const pairs = [
  { id: "S10 separated printf destination signal code", reason: SIGNAL_REASON,
    refused: ["printf -v 'A[$(pkill worker)]' '%s' value", "printf -v 'A[$(killall worker)]' '%s' value"],
    simple: ["printf '%s' 'A[$(pkill worker)]'", "kill -00 4242"] },
  { id: "S10 attached printf destination signal code", reason: SIGNAL_REASON,
    refused: ["printf -v'A[$(pkill worker)]' '%s' value", "printf -vA '$(pkill worker)'"],
    simple: ["printf -- '%s' 'A[$(pkill worker)]'", "printf '%s' '-vA pkill worker'"] },
  { id: "S10 separated printf destination deletion code", reason: DELETE_REASON,
    refused: ["printf -v 'A[$(rm -rf /outside)]' '%s' value", "printf -v 'A[$(shred -u /outside)]' '%s' value"],
    simple: ["printf '%s' 'A[$(rm -rf /outside)]'", "rm -rf /tmp/session-literal-guard/a"] },
  { id: "S10 attached printf destination deletion code", reason: DELETE_REASON,
    refused: ["printf -v'A[$(rm -rf /outside)]' '%s' value"],
    simple: ["printf -- '%s' 'A[$(rm -rf /outside)]'", "printf '%s' '-vA rm -rf /outside'"] },
  { id: "S11 attached hostname executable", reason: SIGNAL_REASON,
    refused: ["rg --hostname-bin=killall5 --hyperlink-format='file://{host}{path}' --color=always --heading needle file", "rg --hostname-bin=/usr/sbin/killall5 needle file"],
    simple: ["rg -F needle file", "printf '%s' '--hostname-bin=killall5'"] },
  { id: "S11 separated hostname executable", reason: SIGNAL_REASON,
    refused: ["rg --hostname-bin killall5 --hyperlink-format='file://{host}{path}' --color=always --heading needle file", "rg --hostname-bin /usr/sbin/killall5 needle file"],
    simple: ["rg --fixed-strings needle file", "echo 'inert --hostname-bin killall5'"] },
  { id: "structural option cut: all search program / decompressor flags", reason: SIGNAL_REASON,
    refused: ["rg --pre=pkill needle worker", "rg --pre pkill needle worker", "rg --pre-glob=pkill needle file", "rg -z pkill file", "rg --search-zip pkill file", "grep --hostname-bin=killall5 needle file", "grep -z pkill file", "grep --search-zip pkill file"],
    simple: ["grep -F pkill file", "grep -n pkill file", "rg -ni pkill file"] },
  { id: "fixed inert flag proof, not unknown options", reason: SIGNAL_REASON,
    refused: ["echo --unknown pkill", "cat --unknown pkill", "ls --unknown pkill", "head --unknown pkill", "tail --unknown pkill", "wc --unknown pkill", "grep --unknown pkill file", "rg --unknown pkill file", "printf -x pkill"],
    simple: ["echo -n pkill", "cat -n pkill", "ls -al pkill", "head -n 1 pkill", "tail -c 2 pkill", "wc -l pkill", "grep -e pkill file", "rg -e pkill file"] },
  { id: "literal operand terminator and stdout printf data", reason: SIGNAL_REASON,
    refused: ["printf -v A pkill", "rg -z --hostname-bin=killall5 needle file"],
    simple: ["grep -- --hostname-bin=killall5 file", "rg -- --pre=pkill file", "printf -- '-vA pkill'", "printf '%s' pkill -v A", "echo -- '-v pkill'"] },
  { id: "rm/find owner exceptions remain exact", reason: DELETE_REASON,
    refused: ["rm -d /outside", "find /own -exec pkill worker ;", "find /own -fprint /outside"],
    simple: ["rm -f /outside/file", "find /own -type f", "find /own -name literal.ts -print", "git status"] },
];
describe("PR231 final round fixed inert option admission", () => {
  for (const pair of pairs) {
    it(`${pair.id}: refusal`, () => {
      for (const command of pair.refused) {
        // Signal has precedence when a find execution action visibly includes pkill.
        const expected = command === "find /own -exec pkill worker ;" ? SIGNAL_REASON : pair.reason;
        expect(bashGuardRefusal(command, root), command).toBe(expected);
      }
    });
    it(`${pair.id}: SIMPLE / inert DATA allowance`, () => {
      for (const command of pair.simple) expect(bashGuardRefusal(command, root), command).toBeUndefined();
    });
  }
  it.each(["constructor", "toString", "__proto__"])("unknown head %s gets no inherited-object allowlist entry", head => {
    expect(bashGuardRefusal(`${head} pkill worker`, root)).toBe(SIGNAL_REASON);
    expect(bashGuardRefusal(`${head} harmless`, root)).toBeUndefined();
  });
});
