import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "(cd /tmp/tmp.AbC123; A=(); rm -rf *)",
  "cd /tmp/tmp.AbC123; (A=(); cd /tmp); rm -rf *",
  "cd /tmp; (A=(); cd /tmp/tmp.AbC123; rm -rf *)"
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
  const intentional = round5IntentionalState.has(command);
  const originallyRefused = original.blocked === true || original.wipe === true || original.exhausted === true || original.overall === true;
  if (!intentional && !(originallyRefused && result.shellState === true)) expect("shellState" in result, command).toBe(false);
  if (intentional || (originallyRefused && result.shellState === true)) {
    expect(Object.prototype.hasOwnProperty.call(result, "shellState"), command).toBe(true);
    expect(result, command).toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  } else if (original.overall !== undefined) {
    expect(Object.keys(result).sort(), command).toEqual(["blocked", "exhausted", "wipe"]);
    expect(result.exhausted, command).toBe(original.exhausted ?? false);
    expect(typeof result.blocked, command).toBe("boolean");
    expect(typeof result.wipe, command).toBe("boolean");
    expect(result.blocked || result.wipe, command).toBe(original.overall);
  } else {
    expect(result, command).toEqual(original);
  }
  expect(killsByPattern(command), command).toBe(result.blocked || result.shellState === true);
  expect(wipesTmp(command), command).toBe(result.wipe || result.shellState === true);
}


// Independently reported post-matrix boundaries. Every command is scanner DATA, never shell input.
// No initial-cwd/session context is supplied: relative operands rely only on an exact known cd.
// 4096 is the existing MAX_VALUE boundary, not a larger analysis quota.
const boundaries: Array<[label: string, command: string, wipe: boolean, exhausted: boolean, blocked?: boolean]> = [
  ["B-N1.01 bare ordinary assignment at 2000", `A=${"x".repeat(2000)}; :`, false, false],
  ["B-N1.02 bare ordinary assignment at MAX_VALUE 4096", `A=${"x".repeat(4096)}; :`, false, false],
  ["B-N1.03 doublequoted ordinary assignment at 2000", `A="${"x".repeat(2000)}"; :`, false, false],
  ["B-N1.04 doublequoted ordinary assignment at MAX_VALUE 4096", `A="${"x".repeat(4096)}"; :`, false, false],
  ["B-N1.05 long doublequoted inert echo at 2000", `echo "${"x".repeat(2000)}"`, false, false],
  ["B-N1.06 long doublequoted inert echo at 4096", `echo "${"x".repeat(4096)}"`, false, false],
  ["B-N1.07 unquoted-delimiter inert heredoc at 2000", `cat <<EOF\n${"x".repeat(2000)}\nEOF`, false, false],
  ["B-N1.08 unquoted-delimiter inert heredoc at 4096", `cat <<EOF\n${"x".repeat(4096)}\nEOF`, false, false],
  ["B-N1.09 bare MAX_VALUE assignment with one unchanged replacement", `A=${"x".repeat(4096)}; B=$A; :`, false, false],
  ["B-N1.10 doublequoted MAX_VALUE assignment with one unchanged replacement", `A="${"x".repeat(4096)}"; B="$A"; :`, false, false],
  // Same stored value as the allowances, but 1024 references would render four MiB.
  // The real scanner must fail closed on amplification, not merely reject an ordinary literal.
  ["B-N1.11 amplified replacement from bare MAX_VALUE assignment", `A=${"x".repeat(4096)}; B=${"$A".repeat(1024)}; :`, true, true],
  ["B-N1.12 amplified replacement from doublequoted MAX_VALUE assignment", `A="${"x".repeat(4096)}"; B="${"$A".repeat(1024)}"; :`, true, true],
  ["B-PWD.01 captured pwd under shared cwd", `cd /tmp; rm -rf "$(pwd)"/*`, true, false],
  ["B-PWD.02 captured pwd under concrete own cwd", `cd /tmp/tmp.AbC123; rm -rf "$(pwd)"/*`, false, false],
  // true and printf '' really emit nothing; the adjacent unquoted '*' remains live.
  ["B-EMPTY.01 bare true capture plus live suffix under shared cwd", `cd /tmp; rm -rf $(true)*`, true, false],
  ["B-EMPTY.02 bare true capture plus live suffix under own cwd", `cd /tmp/tmp.AbC123; rm -rf $(true)*`, false, false],
  ["B-EMPTY.03 quoted true capture plus live suffix under shared cwd", `cd /tmp; rm -rf "$(true)"*`, true, false],
  ["B-EMPTY.04 quoted true capture plus live suffix under own cwd", `cd /tmp/tmp.AbC123; rm -rf "$(true)"*`, false, false],
  ["B-EMPTY.05 bare printf empty capture plus live suffix under shared cwd", `cd /tmp; rm -rf $(printf '')*`, true, false],
  ["B-EMPTY.06 bare printf empty capture plus live suffix under own cwd", `cd /tmp/tmp.AbC123; rm -rf $(printf '')*`, false, false],
  ["B-EMPTY.07 quoted printf empty capture plus live suffix under shared cwd", `cd /tmp; rm -rf "$(printf '')"*`, true, false],
  ["B-EMPTY.08 quoted printf empty capture plus live suffix under own cwd", `cd /tmp/tmp.AbC123; rm -rf "$(printf '')"*`, false, false],
  // A=() closes an array, not the surrounding child group. Neither child cd may leak.
  ["B-ARRAY.01 array close must not prematurely restore shared child cwd", `(cd /tmp; A=(); rm -rf *)`, true, false],
  ["B-ARRAY.02 array close preserves concrete own child cwd", `(cd /tmp/tmp.AbC123; A=(); rm -rf *)`, false, false],
  ["B-ARRAY.03 parent own cwd survives child cd after empty array", `cd /tmp/tmp.AbC123; (A=(); cd /tmp); rm -rf *`, false, false],
  ["B-ARRAY.04 parent shared cwd survives child own cd after empty array", `cd /tmp; (A=(); cd /tmp/tmp.AbC123); rm -rf *`, true, false],
  ["B-ARRAY.05 parent own cwd does not hide child shared cd after empty array", `cd /tmp/tmp.AbC123; (A=(); cd /tmp; rm -rf *)`, true, false],
  ["B-ARRAY.06 parent shared cwd does not taint child own cd after empty array", `cd /tmp; (A=(); cd /tmp/tmp.AbC123; rm -rf *)`, false, false],
  ["B-POS.01 child binds field-split shared second positional", `P="/tmp/tmp.AbC123 /tmp"; bash -c 'rm -rf "$2"' _ $P`, true, false],
  ["B-POS.02 quoted child argument remains one non-root name", `P="/tmp/tmp.AbC123 /tmp"; bash -c 'rm -rf "$1"' _ "$P"`, false, false],
  ["B-POS.03 unquoted lookup capture taints later child positions", `P=$(pgrep x); bash -c 'kill "$2"' _ $P`, false, false, true],
  ["B-POS.04 later lookup capture does not taint earlier literal PID", `bash -c 'kill "$1"; echo "$2"' _ 4242 $(pgrep -c worker)`, false, false],
  ["B-POS.05 unquoted shared listing taints later child positions", `D=$(ls -d /tmp/tmp.*); bash -c 'rm -rf "$2"' _ $D`, true, false],
  ["B-POS.06 unquoted own listing preserves child allowance", `D=$(find /tmp/tmp.AbC123 -name "*.json"); bash -c 'rm -rf "$2"' _ $D`, false, false],
  ["B-POS.07 braced tenth positional names shared root", `set -- a a a a a a a a a /tmp; rm -rf "\${10}"`, true, false],
  ["B-POS.08 braced tenth positional names concrete own path", `set -- a a a a a a a a a /tmp/tmp.AbC123; rm -rf "\${10}"`, false, false],
  ["B-POS.09 braced tenth positional carries lookup provenance", `set -- 4242 4242 4242 4242 4242 4242 4242 4242 4242 $(pgrep x); kill "\${10}"`, false, false, true],
  ["B-POS.10 braced tenth positional carries recorded literal PID", `set -- 4242 4242 4242 4242 4242 4242 4242 4242 4242 4242; kill "\${10}"`, false, false],
  ["B-POS.11 unbraced ten is first positional plus literal zero", `set -- / a a a a a a a a /tmp; rm -rf "$10"`, false, false],
  ["B-POS.12 shorter set clears old tenth positional", `set -- a a a a a a a a a /tmp; set -- a; rm -rf "\${10}"`, false, false],
];

describe("guard follow-up independent boundaries", () => {
  it.each(boundaries)("%s", (_label, command, wipe, exhausted, blocked = exhausted) => {
    expectRound5Guard(command, scanCommand(command), { blocked, wipe, exhausted });
    void 0;
    void 0;
  });
});
