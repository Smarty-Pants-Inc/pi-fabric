import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA-only regressions: these command strings must never be executed.
function expectStateRefusal(command: string): void {
  const result = scanCommand(command);
  expect(result).toStrictEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  expect(Object.prototype.hasOwnProperty.call(result, "shellState")).toBe(true);
  expect(killsByPattern(command)).toBe(true);
  expect(wipesTmp(command)).toBe(true);
}

function expectSimple(command: string): void {
  const result = scanCommand(command);
  expect(result).toStrictEqual({ blocked: false, wipe: false, exhausted: false });
  expect(Object.prototype.hasOwnProperty.call(result, "shellState")).toBe(false);
  expect(killsByPattern(command)).toBe(false);
  expect(wipesTmp(command)).toBe(false);
}

describe("PR166 round5 builtin lookup and binding state cut", () => {
  it("R1 refuses enable disabling readonly before a readonly path grant", () => {
    expectStateRefusal('enable -n readonly; D=/own; readonly D; D=/tmp; rm -rf "$D"');
  });
  it("S1 preserves direct bare readonly with an own path", () => {
    expectSimple('D=/own; readonly D; D=/tmp; rm -rf "$D"');
  });

  it("R2 refuses a function overriding readonly before a readonly path grant", () => {
    expectStateRefusal('function readonly { :; }; D=/own; readonly D; D=/tmp; rm -rf "$D"');
  });
  it("S2 preserves plain own-path scalar assignment", () => {
    expectSimple('D=/own; rm -rf "$D"');
  });

  it("R3 refuses a function overriding printf before a literal byte grant", () => {
    expectStateRefusal("function printf { echo /tmp; }; D=$(printf '%s' /own); rm -rf \"$D\"");
  });
  it("S3 preserves scalar capture of literal stdout printf for an own path", () => {
    expectSimple("D=$(printf '%s' /own); rm -rf \"$D\"");
  });

  it("R4 refuses shopt and alias receiver lookup mutation metadata", () => {
    expectStateRefusal("shopt -s expand_aliases; alias readonly=':'; D=/own; readonly D; D=/tmp; rm -rf \"$D\"");
  });
  it("S4 preserves a literal recorded PID", () => {
    expectSimple('P=4242; kill "$P"');
  });
});
