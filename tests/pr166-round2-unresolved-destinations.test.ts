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


// DATA ONLY: never execute these strings. Named MAX static receipt:
// .local/pr166-r2-security-dynamic-finding-pattern-kill.ts (SHA256 2d0ea3e6...).
// An unsupported destination binder must widen potentially affected cells to
// common UNKNOWN, not keep a stale safe value. No exact-destination interpreter
// is required, and unused destinations must not be used to weaken this check.
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
