import { describe, expect, it } from "vitest";
import { scanCommand } from "../src/core/pattern-kill.js";

// DATA only: command strings are never executed. Three refusal/simple pairs.
describe("PR166 round5 unmodelled IFS mutators", () => {
  it("R1 refuses let before consumer inference", () => {
    expect(scanCommand("D=5/tmp;let IFS=5;rm -rf \"$(printf '%s' $D)\""))
      .toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  });

  it("S1 preserves an ordinary scalar own-path", () => {
    expect(scanCommand('D=/tmp/my-owned-work;rm -rf "$D"'))
      .toEqual({ blocked: false, wipe: false, exhausted: false });
  });

  it("R2 refuses getopts before consumer inference", () => {
    expect(scanCommand("D=c/tmp;getopts c IFS -c;rm -rf \"$(printf '%s' $D)\""))
      .toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  });

  it("S2 preserves a literal printf capture", () => {
    expect(scanCommand("rm -rf \"$(printf '%s' /tmp/my-owned-work)\""))
      .toEqual({ blocked: false, wipe: false, exhausted: false });
  });

  it("R3 refuses trap without interpreting its body", () => {
    expect(scanCommand("D=:/tmp;trap 'IFS=:' DEBUG;rm -rf \"$(printf '%s' $D)\""))
      .toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  });

  it("S3 preserves a literal PID", () => {
    expect(scanCommand("kill -TERM 12345"))
      .toEqual({ blocked: false, wipe: false, exhausted: false });
  });
});
