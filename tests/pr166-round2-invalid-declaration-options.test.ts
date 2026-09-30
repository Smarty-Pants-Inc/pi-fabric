import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: these command strings are never executed by a shell.
function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  expect(Object.keys(result).sort().join(","), command).toBe("blocked,exhausted,wipe");
  expect(result, command).toEqual({ blocked, wipe, exhausted: false });
  expect(killsByPattern(command), command).toBe(blocked);
  expect(wipesTmp(command), command).toBe(wipe);
}

describe("PR166 round2 invalid declaration options (DATA ONLY)", () => {
  for (const option of ["-0", "--bogus", "-r0"]) {
    it(`TMP readonly ${option} cannot grant a new readonly proof`, () => {
      check(`D=/own; readonly ${option} D; read -r D <<< /tmp; rm -rf "$D"`, false, true);
    });
    it(`TMP simple readonly counterpart for ${option}`, () => {
      check('D=/own; readonly D; read -r D <<< /tmp; rm -rf "$D"', false, false);
    });
    it(`PID readonly ${option} cannot grant a new readonly proof`, () => {
      check(`P=4242; readonly ${option} P; read -r P <<< "$(pgrep worker)"; kill "$P"`, true, false);
    });
    it(`PID simple readonly counterpart for ${option}`, () => {
      check('P=4242; readonly P; read -r P <<< "$(pgrep worker)"; kill "$P"', false, false);
    });
  }
});
