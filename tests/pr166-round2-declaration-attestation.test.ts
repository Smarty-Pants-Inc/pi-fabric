import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: declaration execution must be proved before granting parent bindings
// or new immutable attributes. Never execute these strings in a shell.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  expect(Object.keys(result).sort().join(","), command).toBe("blocked,exhausted,wipe");
  expect(result.exhausted, command).toBe(false);
  expect(result.blocked, command).toBe(blocked);
  expect(result.wipe, command).toBe(wipe);
  expect(killsByPattern(command), command).toBe(result.blocked);
  expect(wipesTmp(command), command).toBe(result.wipe);
}

describe("PR166 round2 declaration execution attestation (DATA ONLY)", () => {
  it("TMP possibly closed fd cannot grant readonly before later read", () => {
    check('D=/own; readonly D <&9; read -r D <<< /tmp; rm -rf "$D"', false, true);
  });
  it("TMP plain readonly preserves own path against later read", () => {
    check('D=/own; readonly D; read -r D <<< /tmp; rm -rf "$D"', false, false);
  });
  it("PID possibly closed fd cannot grant readonly before later read", () => {
    check('P=4242; readonly P <&9; read -r P <<< "$(pgrep worker)"; kill "$P"', true, false);
  });
  it("PID plain readonly preserves recorded PID against later read", () => {
    check('P=4242; readonly P; read -r P <<< "$(pgrep worker)"; kill "$P"', false, false);
  });
  it("TMP external wrapped readonly cannot grant parent attribute", () => {
    check('D=/own; env readonly D; read -r D <<< /tmp; rm -rf "$D"', false, true);
  });
  it("TMP direct readonly counterpart remains allowed", () => {
    check('readonly D=/own; read -r D <<< /tmp; rm -rf "$D"', false, false);
  });
  it("PID external wrapped readonly cannot grant parent attribute", () => {
    check('P=4242; env readonly P; read -r P <<< "$(pgrep worker)"; kill "$P"', true, false);
  });
  it("PID direct readonly counterpart remains allowed", () => {
    check('readonly P=4242; read -r P <<< "$(pgrep worker)"; kill "$P"', false, false);
  });
});
