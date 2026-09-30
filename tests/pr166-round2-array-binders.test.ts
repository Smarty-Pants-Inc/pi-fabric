import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([]);
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


// DATA ONLY: never submit these strings to a shell. Unsupported array-element
// writes must widen the base to UNKNOWN, not replace all existing elements with
// a fabricated safe scalar. No array-index interpreter is required here.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  const killWrapper = killsByPattern(command);
  const wipeWrapper = wipesTmp(command);
  expectRound5Guard(command, result, { blocked, wipe, exhausted: false });
  void 0;
  void 0;
  void 0;
  void 0;
  void 0;
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
