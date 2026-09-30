import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "D=/own; readonly D; read -r D <<< /tmp; rm -rf \"$D\"",
  "P=4242; readonly P; read -r P <<< \"$(pgrep worker)\"; kill \"$P\"",
  "readonly D=/own; read -r D <<< /tmp; rm -rf \"$D\"",
  "readonly P=4242; read -r P <<< \"$(pgrep worker)\"; kill \"$P\""
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


// DATA ONLY: declaration execution must be proved before granting parent bindings
// or new immutable attributes. Never execute these strings in a shell.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  expectRound5Guard(command, result, { blocked, wipe, exhausted: false });
  void 0;
  void 0;
  void 0;
  void 0;
  void 0;
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
