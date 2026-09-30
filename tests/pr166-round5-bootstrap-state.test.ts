import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// These command strings are scanner DATA only; never execute the bootstrap fixture.
const metadata = "cat > .local/bootstrap-readonly <<'BOOT'\nenable -n readonly\nBOOT\n";
const child = "bash -c 'D=/own; readonly D; D=/tmp; rm -rf \"$D\"'";
function assertState(command: string, refused: boolean): void {
  const result = scanCommand(command);
  expect(result).toStrictEqual(refused
    ? { blocked: false, wipe: false, exhausted: false, shellState: true }
    : { blocked: false, wipe: false, exhausted: false });
  expect(Object.keys(result).sort()).toStrictEqual(refused
    ? ["blocked", "exhausted", "shellState", "wipe"]
    : ["blocked", "exhausted", "wipe"]);
  expect(Object.prototype.hasOwnProperty.call(result, "shellState")).toBe(refused);
  expect(killsByPattern(command)).toBe(refused);
  expect(wipesTmp(command)).toBe(refused);
}

describe("round5 bootstrap state assignment refusal", () => {
  it("refuses the exact Main BASH_ENV bootstrap DATA fixture", () => {
    assertState(metadata + "BASH_ENV=.local/bootstrap-readonly " + child, true);
  });
  it("allows the same written metadata and bare readonly child without bootstrap assignment", () => {
    assertState(metadata + child, false);
  });
  it("refuses direct ENV bootstrap assignment before granting receiver bindings", () => {
    assertState("ENV=.local/bootstrap-readonly bash -c 'D=/own; rm -rf \"$D\"'", true);
  });
  it("allows the plain direct own-path receiver", () => {
    assertState("bash -c 'D=/own; rm -rf \"$D\"'", false);
  });
});
