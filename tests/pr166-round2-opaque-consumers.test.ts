import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: never execute these strings. Opaque attributes affect mutable
// reference provenance even when the eventual consumer's name was not tracked.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  expect(Object.keys(result).sort().join(","), command).toBe("blocked,exhausted,wipe");
  expect(result.exhausted, command).toBe(false);
  expect(result.blocked, command).toBe(blocked);
  expect(result.wipe, command).toBe(wipe);
  expect(killsByPattern(command), command).toBe(result.blocked);
  expect(wipesTmp(command), command).toBe(result.wipe);
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
