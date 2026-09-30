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


// DATA ONLY: never execute these strings. Unsupported declaration attributes
// cannot invent scalar identity or erase the provenance of possibly affected cells.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  expectRound5Guard(command, result, { blocked, wipe, exhausted: false });
  void 0;
  void 0;
  void 0;
  void 0;
  void 0;
}

describe("PR166 round2 unsupported declaration attributes (DATA ONLY)", () => {
  it("TMP exact alias write must not retain own-path approval", () => {
    check('D=/own; declare -n N=D; N=/tmp; rm -rf "$D"', false, true);
  });
  it("SIMPLE own-path counterpart remains allowed", () => {
    check('D=/own; rm -rf "$D"', false, false);
  });
  it("PID alias write must not retain recorded-PID approval", () => {
    check('P=4242; declare -n N=P; N=$(pgrep worker); kill "$P"', true, false);
  });
  it("SIMPLE recorded-PID counterpart remains allowed", () => {
    check('P=4242; kill "$P"', false, false);
  });
});
