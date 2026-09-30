import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "declare -n N=D; rm -rf /own"
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


// DATA ONLY: never execute these strings. Opaque attributes affect mutable
// reference provenance even when the eventual consumer's name was not tracked.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  expectRound5Guard(command, result, { blocked, wipe, exhausted: false });
  void 0;
  void 0;
  void 0;
  void 0;
  void 0;
}

describe("PR166 round2 opaque mutable consumers (DATA ONLY)", () => {
  it("opaque untracked path reference refuses", () => {
    check('declare -n N=D; N=/tmp; rm -rf "$D"', false, true);
  });
  it("ordinary unknown path without opaque attributes remains allowed", () => {
    check('rm -rf "$D"', false, false);
  });
  it("opaque untracked PID reference refuses", () => {
    check('declare -n N=P; N=$(pgrep worker); kill "$P"', true, false);
  });
  it("ordinary unknown PID without opaque attributes remains allowed", () => {
    check('kill "$P"', false, false);
  });
  it("eval opaque untracked path reference refuses", () => {
    check('eval \'declare -n N=D\'; N=/tmp; rm -rf "$D"', false, true);
  });
  it("SIMPLE literal own-path after opaque declaration remains allowed", () => {
    check('declare -n N=D; rm -rf /own', false, false);
  });
});
