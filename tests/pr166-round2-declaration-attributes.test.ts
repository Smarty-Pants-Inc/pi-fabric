import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: never execute these strings. Unsupported declaration attributes
// cannot invent scalar identity or erase the provenance of possibly affected cells.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  expect(Object.keys(result).sort().join(","), command).toBe("blocked,exhausted,wipe");
  expect(result.exhausted, command).toBe(false);
  expect(result.blocked, command).toBe(blocked);
  expect(result.wipe, command).toBe(wipe);
  expect(killsByPattern(command), command).toBe(result.blocked);
  expect(wipesTmp(command), command).toBe(result.wipe);
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
