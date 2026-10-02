import { describe, expect, it } from "vitest";
import { bashGuardRefusal, DELETE_REASON, SIGNAL_REASON } from "../src/core/literal-bash-guard.js";

const tmpdir = "/tmp/session-literal-guard";
// The reviewer supplied class descriptions, not verbatim command blocks. These are explicitly
// derived lexical witnesses. Commands are DATA only and are never dispatched by this suite.
const pairs = [
  { id: "S5: relative shell signal receiver", reason: SIGNAL_REASON,
    refused: ["sh -c './x/pkill worker'", "bash -c '../bin/pkill worker'", "sh -c 'bin/pkill worker'"],
    simple: ["kill -00 4242", "sh -c './x/printf marker'"] },
  { id: "S5: relative shell deletion receiver", reason: DELETE_REASON,
    refused: ["sh -c '../bin/rm -rf /outside'", "bash -c './x/rm -r /outside'", "sh -c 'bin/rm -rf /outside'"],
    simple: ["rm -rf /tmp/session-literal-guard/a", "sh -c '../bin/printf marker'"] },
  { id: "S5: eval relative signal receiver", reason: SIGNAL_REASON,
    refused: ["eval './x/pkill worker'", "eval '../bin/killall worker'"],
    simple: ["kill -TERM 4242", "eval './x/printf marker'"] },
  { id: "S5: eval relative deletion receiver", reason: DELETE_REASON,
    refused: ["eval '../bin/rm -rf /outside'", "eval './x/shred -u /outside'"],
    simple: ["rm -r /tmp/session-literal-guard/a", "eval '../bin/printf marker'"] },
  { id: "S5: lexical path characters do not hide the basename", reason: SIGNAL_REASON,
    refused: ["sh -c 'dir:one/pkill worker'", "sh -c 'ü/bin/pkill worker'"],
    simple: ["echo './x/pkill worker'", "rg '../bin/pkill' docs"] },
  { id: "S6: EXIT / zero trap signal action", reason: SIGNAL_REASON,
    refused: ["trap 'pkill worker' EXIT", "trap 'pkill worker' 0", "trap './x/pkill worker' EXIT"],
    simple: ["trap 'printf marker' EXIT", "trap 'printf marker' 0", "kill 4242"] },
  { id: "S6: EXIT / zero trap deletion action", reason: DELETE_REASON,
    refused: ["trap 'rm -rf /outside' EXIT", "trap 'rm -rf /outside' 0", "trap '../bin/rm -rf /outside' EXIT"],
    simple: ["trap - EXIT", "trap '' 0", "rm -rf /tmp/session-literal-guard/a"] },
  { id: "S6: printed action DATA stays inert", reason: SIGNAL_REASON,
    refused: ["trap 'kill 4242' EXIT", "trap 'p\"ki\"ll worker' EXIT"],
    simple: ["printf '%s' 'trap pkill worker EXIT'", "echo \"trap 'rm -rf /outside' EXIT\"", "trap -p EXIT"] },
];

describe("PR231 round 3 relative-path and deferred execution cuts", () => {
  for (const pair of pairs) {
    it(`${pair.id}: refusal`, () => {
      for (const command of pair.refused) expect(bashGuardRefusal(command, tmpdir), command).toBe(pair.reason);
    });
    it(`${pair.id}: SIMPLE / inert DATA allowance`, () => {
      for (const command of pair.simple) expect(bashGuardRefusal(command, tmpdir), command).toBeUndefined();
    });
  }
});
