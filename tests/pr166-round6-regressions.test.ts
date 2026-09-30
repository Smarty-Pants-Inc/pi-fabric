import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: never execute these strings in a shell. Command IDs and bytes are frozen.
const pairs = [
  {
    id: "01", boundary: "TMP uncalled multiline assignment",
    refused: [
      ['R01a', 'D=/tmp\nprepare() {\n  D=/own\n}\nrm -rf "$D"'],
      ['R01b', 'D=/tmp\nfunction prepare {\n  D=/own\n}\nrm -rf "$D"'],
    ],
    simple: [['S01', 'D=/tmp\nD=/own\nrm -rf "$D"']],
  },
  {
    id: "02", boundary: "PID uncalled assignment with alternate function whitespace",
    refused: [
      ['R02a', 'P=$(pgrep worker)\nprepare ( )\n{\n  P=4242\n}\nkill "$P"'],
      ['R02b', 'P=$(pgrep worker)\nfunction prepare () {\n  P=4242\n}\nkill "$P"'],
    ],
    simple: [['S02', 'P=$(pgrep worker)\nP=4242\nkill "$P"']],
  },
  {
    id: "03", boundary: "TMP phantom readonly in an uncalled function",
    refused: [
      ['R03a', 'D=/own\nprepare() {\n  readonly D=/own\n}\nD=/tmp\nrm -rf "$D"'],
      ['R03b', 'D=/own\nprepare ( )\n{\n  readonly D=/own\n}\nD=/tmp\nrm -rf "$D"'],
    ],
    // The real direct declaration protects an already-safe value; no failed write is needed.
    simple: [['S03', 'D=/own; readonly D; rm -rf "$D"']],
  },
  {
    id: "04", boundary: "PID phantom readonly in an uncalled function",
    refused: [
      ['R04a', 'P=4242\nprepare() {\n  readonly P=4242\n}\nP=$(pgrep worker)\nkill "$P"'],
      ['R04b', 'P=4242\nfunction prepare {\n  readonly P=4242\n}\nP=$(pgrep worker)\nkill "$P"'],
    ],
    simple: [['S04', 'P=4242; readonly P; kill "$P"']],
  },
  {
    id: "05", boundary: "TMP unset default assignment in assignment and argv",
    refused: [
      ['R05a', 'H=${D:=/tmp}; rm -rf "$D"'],
      ['R05b', 'printf \'%s\' "${D:=/tmp}"; rm -rf "$D"'],
    ],
    simple: [['S05', 'D=/own; H=$D; rm -rf "$D"']],
  },
  {
    id: "06", boundary: "PID unset default assignment in assignment and argv",
    refused: [
      ['R06a', 'H=${P:=$(pgrep worker)}; kill "$P"'],
      ['R06b', 'printf \'%s\' "${P:=$(pgrep worker)}"; kill "$P"'],
    ],
    simple: [['S06', 'P=4242; H=$P; kill "$P"']],
  },
  {
    id: "07", boundary: "TMP non-colon default assignment in assignment and argv",
    refused: [
      ['R07a', 'H=${D=/tmp}; rm -rf "$D"'],
      ['R07b', 'printf \'%s\' "${D=/tmp}"; rm -rf "$D"'],
    ],
    simple: [['S07', 'D=/own; H=\'${D=/tmp}\'; rm -rf "$D"']],
  },
  {
    id: "08", boundary: "PID non-colon default assignment in assignment and argv",
    refused: [
      ['R08a', 'H=${P=$(pgrep worker)}; kill "$P"'],
      ['R08b', 'printf \'%s\' "${P=$(pgrep worker)}"; kill "$P"'],
    ],
    simple: [['S08', 'P=4242; H=\'${P=$(pgrep worker)}\'; kill "$P"']],
  },
  {
    id: "09", boundary: "TMP unsupported error operator in assignment and argv",
    // D is nonempty: Bash reaches the destructive consumer, not an early unset error.
    refused: [
      ['R09a', 'D=/tmp; H=${D:?missing}; rm -rf "$D"'],
      ['R09b', 'D=/tmp; printf \'%s\' "${D:?missing}"; rm -rf "$D"'],
    ],
    simple: [['S09', 'D=/own; H=\'${D:?missing}\'; rm -rf "$D"']],
  },
  {
    id: "10", boundary: "PID unsupported error operator in assignment and argv",
    refused: [
      ['R10a', 'P=$(pgrep worker); H=${P:?missing}; kill "$P"'],
      ['R10b', 'P=$(pgrep worker); printf \'%s\' "${P:?missing}"; kill "$P"'],
    ],
    simple: [['S10', 'P=4242; H=\'${P:?missing}\'; kill "$P"']],
  },
  {
    id: "11", boundary: "ANSI-C escaped path reassignment",
    refused: [
      ['R11a', "D=/tmp; D=$'\\x2f\\x74\\x6d\\x70'; rm -rf \"$D\""],
      ['R11b', "D=/tmp; D=$'\\057\\164\\155\\160'; rm -rf \"$D\""],
    ],
    simple: [['S11', "D=/tmp; D=/own; printf '%s' '\\x2f\\x74\\x6d\\x70'; rm -rf \"$D\""]],
  },
  {
    id: "12", boundary: "ANSI-C decoded printf destination option under both policies",
    refused: [
      ['R12a', "D=/own; printf $'\\x2d\\x76' D '%s' /tmp; rm -rf \"$D\""],
      ['R12b', "P=4242; printf $'\\055\\166' P '%s' \"$(pgrep worker)\"; kill \"$P\""],
    ],
    // Here -v/encoded option bytes are inert operands after the literal stdout format.
    simple: [
      ['S12a', "D=/own; printf '%s' '-v' D /tmp; rm -rf \"$D\""],
      ['S12b', "P=4242; printf '%s' '\\055\\166' P '$(pgrep worker)'; kill \"$P\""],
    ],
  },
] as const;

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

describe("PR166 round6 paired nonexecution, parameter and ANSI-C boundaries", () => {
  for (const pair of pairs) {
    it(`R${pair.id} refuses ${pair.boundary}`, () => {
      for (const [id, command] of pair.refused) {
        expectStateRefusalWithId(id, command);
      }
    });
    it(`SIMPLE-A${pair.id} preserves the simple counterpart`, () => {
      for (const [id, command] of pair.simple) {
        expectSimpleWithId(id, command);
      }
    });
  }
});

// Include the frozen command ID in failures without changing or executing command data.
function expectStateRefusalWithId(id: string, command: string): void {
  expect(() => expectStateRefusal(command), id).not.toThrow();
}

function expectSimpleWithId(id: string, command: string): void {
  expect(() => expectSimple(command), id).not.toThrow();
}
