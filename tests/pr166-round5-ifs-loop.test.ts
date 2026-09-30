import { describe, expect, it } from "vitest";
import { scanCommand } from "../src/core/pattern-kill.js";

// DATA ONLY: these command strings are classifier input, never shell execution.
// Exactly two R/SIMPLE pairs; ordinary loops are not a new interpreter or ban.
const pairs = [
  {
    finding: "for IFS state write",
    refusal: `D=:/tmp; for IFS in :; do rm -rf "$(printf '%s' $D)"; done`,
    simple: `for item in .local/own; do printf '%s' "$item"; done`,
  },
  {
    finding: "control-prefixed select with quoted/escaped IFS destination",
    refusal: `if true; then select "I"\\FS in :; do printf '%s' '.local/own'; break; done <<< 1; fi`,
    simple: `if true; then for item in .local/own; do printf '%s' "$item"; done; fi`,
  },
] as const;

describe("PR166 round5 IFS loop destinations (DATA ONLY)", () => {
  for (const { finding, refusal, simple } of pairs) {
    it(`${finding}: R whole-command refusal`, () => {
      expect(scanCommand(refusal)).toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
    });
    it(`${finding}: SIMPLE A allowance`, () => {
      expect(scanCommand(simple)).toEqual({ blocked: false, wipe: false, exhausted: false });
    });
  }
});
