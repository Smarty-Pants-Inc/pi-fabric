import { describe, expect, it } from "vitest";
import { bashGuardRefusal, DELETE_REASON, SIGNAL_REASON } from "../src/core/literal-bash-guard.js";

const tmpdir = "/tmp/session-literal-guard";
// All commands are scanner DATA; this suite never dispatches a shell or a protected operation.
const pairs: { id: string; refused: string[]; reason?: string; simple: string[] }[] = [
  { id: "Astra F1 / security S2: executable search preprocessors", reason: SIGNAL_REASON,
    refused: ["rg --pre=pkill needle worker", "rg --pre pkill needle worker", "rg --pre=/usr/bin/pkill needle worker", "rg --pre /usr/bin/pkill needle worker", "rg --pre-glob='*.txt' --pre=pkill needle worker", "grep --pre=pkill needle worker"],
    simple: ["rg needle worker", "rg 'pkill worker' docs", "grep -n pkill README.md"] },
  { id: "security S1: negation prefix", reason: SIGNAL_REASON,
    refused: ["! pkill worker", "! kill 4242"], simple: ["kill 4242", "echo '! pkill worker'"] },
  { id: "security S1: coprocess prefix", reason: SIGNAL_REASON,
    refused: ["coproc pkill worker", "coproc kill 4242"], simple: ["kill -00 4242", "printf '%s' 'coproc pkill worker'"] },
  { id: "security S1: reserved prefixes do not grant delete DATA", reason: DELETE_REASON,
    refused: ["! rm -rf /outside", "coproc rm -rf /outside", "then rm -rf /outside", "time rm -rf /outside", "if rm -rf /outside"],
    simple: ["rm -rf /tmp/session-literal-guard/a", "echo 'if then time rm -rf /outside'"] },
  { id: "Astra F2 / security S3: ANSI-C signal scripts", reason: SIGNAL_REASON,
    refused: ["sh -c $'pkill worker'", "bash -c $'pkill worker'", "eval $'pkill worker'"],
    simple: ["kill -TERM 4242", "printf '%s' 'pkill worker'"] },
  { id: "Astra F2 / security S3: ANSI-C deletion scripts", reason: DELETE_REASON,
    refused: ["sh -c $'rm -rf /outside'", "bash -c $'rm -rf /outside'", "eval $'rm -rf /outside'"],
    simple: ["rm -r /tmp/session-literal-guard/a", "echo 'rm -rf /outside'"] },
  // Derived class witnesses: the security review describes these spellings without a code block.
  { id: "security S3 derived: inner quote concatenation / escaped signal spelling", reason: SIGNAL_REASON,
    refused: ["sh -c 'p\"ki\"ll worker'", "bash -c 'p\\kill worker'", "eval 'p\"ki\"ll worker'"],
    simple: ["kill -- 4242", "echo 'p\"ki\"ll worker'"] },
  { id: "security S3 derived: fragmented deletion spelling", // Unknown class receives a stable refusal, never an inferred executable.
    refused: ["sh -c 'r\"m\" -rf /outside'", "bash -c 'r\\m -rf /outside'", "eval 'r\"m\" -rf /outside'"],
    simple: ["rm -rf '/tmp/session-literal-guard/with space'", "printf '%s' 'r\"m\" -rf /outside'"] },
  { id: "security S3: unsupported locale / continuation / heredoc script quoting", reason: SIGNAL_REASON,
    refused: ['sh -c $"pkill worker"', "sh -c pki\\\nll\\ worker", "sh -c 'cat <<END\npkill worker\nEND'"],
    simple: ["kill -9 4242", "echo 'pkill worker'"] },
  { id: "Astra F3 / security S4: short shred remove selectors", reason: DELETE_REASON,
    refused: ["shred -u /outside/important", "shred -uz /outside/important", "shred -zu /outside/important"],
    simple: ["shred /outside/important", "shred -z /outside/important"] },
  { id: "Astra F3 / security S4: long and wrapped remove selectors", reason: DELETE_REASON,
    refused: ["shred --remove /outside/important", "shred --remove=unlink /outside/important", "env shred -uz /outside/important", "sh -c 'shred -u /outside/important'"],
    simple: ["shred -z /outside/important", "echo 'shred -u /outside/important'"] },
  { id: "reserved / unknown punctuation cannot certify protected argv as DATA", reason: SIGNAL_REASON,
    refused: ["then pkill worker", "time pkill worker", "? pkill worker"],
    simple: ["printf '%s' '? pkill worker'", "rg 'coproc pkill' docs"] },
];
describe("PR231 round 2 conservative class cuts", () => {
  for (const pair of pairs) {
    it(`${pair.id}: refusal`, () => {
      for (const command of pair.refused) {
        const actual = bashGuardRefusal(command, tmpdir);
        if (pair.reason) expect(actual, command).toBe(pair.reason);
        else expect([SIGNAL_REASON, DELETE_REASON], command).toContain(actual);
      }
    });
    it(`${pair.id}: SIMPLE / inert DATA allowance`, () => {
      for (const command of pair.simple) expect(bashGuardRefusal(command, tmpdir), command).toBeUndefined();
    });
  }
});
