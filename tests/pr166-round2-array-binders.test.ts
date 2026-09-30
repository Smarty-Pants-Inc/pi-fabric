import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: never submit these strings to a shell. Unsupported array-element
// writes must widen the base to UNKNOWN, not replace all existing elements with
// a fabricated safe scalar. No array-index interpreter is required here.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  const killWrapper = killsByPattern(command);
  const wipeWrapper = wipesTmp(command);
  expect(Object.keys(result).sort().join(","), command).toBe("blocked,exhausted,wipe");
  expect(result.exhausted, command).toBe(false);
  expect(result.blocked, command).toBe(blocked);
  expect(result.wipe, command).toBe(wipe);
  expect(killWrapper, command).toBe(result.blocked);
  expect(wipeWrapper, command).toBe(result.wipe);
}

describe("PR166 round2 named MAX F2 unsupported array binder (DATA ONLY)", () => {
  it("F2 exact existing shared-root element survives printf -v: refuse wipe", () => {
    check('D=(/tmp); printf -v \'D[1]\' \'%s\' /own; rm -rf "${D[@]}"', false, true);
  });
  it("F2 shared-root counterpart: SIMPLE own path allowance", () => {
    check('D=/own; rm -rf "$D"', false, false);
  });
  it("F2 existing lookup element survives printf -v: refuse kill", () => {
    check('P=($(pgrep worker)); printf -v \'P[1]\' %s 4242; kill "${P[@]}"', true, false);
  });
  it("F2 lookup counterpart: SIMPLE recorded PID allowance", () => {
    check('P=4242; kill "$P"', false, false);
  });
  it("redirect failure preserves shared-root possibility: refuse wipe", () => {
    check('D=/tmp; printf -v D \'%s\' /own <&9; rm -rf "$D"', false, true);
  });
  it("redirect failure counterpart: SIMPLE scalar replacement allowance", () => {
    check('D=/tmp; D=/own; rm -rf "$D"', false, false);
  });
  it("redirect failure preserves lookup possibility: refuse kill", () => {
    check('P=$(pgrep worker); printf -v P %s 4242 <&9; kill "$P"', true, false);
  });
  it("redirect failure counterpart: SIMPLE recorded PID allowance", () => {
    check('P=4242; kill "$P"', false, false);
  });
});
