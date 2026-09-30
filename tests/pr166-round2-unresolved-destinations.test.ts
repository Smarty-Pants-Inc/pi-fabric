import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: never execute these strings. Named MAX static receipt:
// .local/pr166-r2-security-dynamic-finding-pattern-kill.ts (SHA256 2d0ea3e6...).
// An unsupported destination binder must widen potentially affected cells to
// common UNKNOWN, not keep a stale safe value. No exact-destination interpreter
// is required, and unused destinations must not be used to weaken this check.
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

describe("PR166 round2 named MAX unresolved printf -v destination (DATA ONLY)", () => {
  it("EXACT destination alias must not retain own-path approval: refuse wipe", () => {
    check('D=/own; N=D; printf -v "$N" \'%s\' /tmp; rm -rf "$D"', false, true);
  });
  it("SIMPLE own-path counterpart: allow", () => {
    check('D=/own; rm -rf "$D"', false, false);
  });
  it("PID mirror destination alias must not retain recorded-PID approval: refuse kill", () => {
    check('P=4242; N=P; printf -v "$N" \'%s\' "$(pgrep worker)"; kill "$P"', true, false);
  });
  it("SIMPLE recorded-PID counterpart: allow", () => {
    check('P=4242; kill "$P"', false, false);
  });
});
