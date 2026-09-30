import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: these strings are scanner inputs, NEVER shell/subprocess/native inputs.
// Definitions-only authoring: Main owns the one frozen f826 baseline execution.
// JS \\ represents one backslash; \n and \t represent physical LF and TAB.
type Case = readonly [id: string, command: string];
type Pair = { id: string; boundary: string; refused: readonly Case[]; simple: readonly Case[] };

const statePairs: readonly Pair[] = [
  {
    id: "01", boundary: "single-quoted preserved backslash cannot fabricate readonly",
    refused: [
      ["R01-TMP", 'D=/own\ncat <<\'E\\OF\'\nEOF\nreadonly D=/own\nE\\OF\nD=/tmp\nrm -rf "$D"'],
      ["R01-PID", 'P=4242\ncat <<\'E\\OF\'\nEOF\nreadonly P=4242\nE\\OF\nP=$(pgrep worker)\nkill "$P"'],
    ],
    simple: [
      ["S01-TMP", 'D=/tmp\ncat <<\'WORD\'\nreadonly D=/tmp\nWORD\nD=/own\nrm -rf "$D"'],
      ["S01-PID", 'P=4242\ncat <<\'WORD\'\nreadonly P=$(pgrep worker)\nWORD\nP=4343\nkill "$P"'],
    ],
  },
  {
    id: "02", boundary: "double-quoted preserved backslash cannot fabricate readonly",
    refused: [
      ["R02-TMP", 'D=/own\ncat <<"E\\OF"\nEOF\nreadonly D=/own\nE\\OF\nD=/tmp\nrm -rf "$D"'],
      ["R02-PID", 'P=4242\ncat <<"E\\OF"\nEOF\nreadonly P=4242\nE\\OF\nP=$(pgrep worker)\nkill "$P"'],
    ],
    simple: [
      ["S02-TMP", 'D=/tmp\ncat <<"WORD"\nreadonly D=/tmp\n$[D=0]\nWORD\nD=/own\nrm -rf "$D"'],
      ["S02-PID", 'P=4242\ncat <<"WORD"\nreadonly P=0\n$[P=Q]\nWORD\nP=4343\nkill "$P"'],
    ],
  },
  {
    id: "03", boundary: "escaped literal backslash in mixed header cannot fabricate readonly",
    refused: [
      ["R03-TMP", 'D=/own\ncat <<E\\\\\'OF\'\nEOF\nreadonly D=/own\nE\\OF\nD=/tmp\nrm -rf "$D"'],
      ["R03-PID", 'P=4242\ncat <<E\\\\\'OF\'\nEOF\nreadonly P=4242\nE\\OF\nP=$(pgrep worker)\nkill "$P"'],
    ],
    simple: [
      ["S03-TMP", 'D=/tmp\ncat <<WORD\nmarker\nWORD\nD=/own\nrm -rf "$D"'],
      ["S03-PID", 'P=4242\ncat <<WORD\nmarker\nWORD\nP=4343\nkill "$P"'],
    ],
  },
  {
    id: "04", boundary: "ANSI-C header cannot swallow command after real delimiter",
    refused: [
      ["R04-TMP", "cat <<$'EOF'\nmarker\nEOF\nrm -rf /tmp\n$EOF"],
      ["R04-PID", "cat <<$'EOF'\nmarker\nEOF\nP=$(pgrep worker)\nkill \"$P\"\n$EOF"],
    ],
    simple: [
      ["S04-TMP", 'D=/own\ncat <<\'EOF\'\nrm -rf /tmp\n$EOF\nEOF\nrm -rf "$D"'],
      ["S04-PID", 'P=4242\ncat <<\'EOF\'\nP=$(pgrep worker)\nkill "$P"\n$EOF\nEOF\nkill "$P"'],
    ],
  },
  {
    id: "05", boundary: "locale header bypass is unsupported even without translation",
    refused: [
      ["R05-TMP", 'cat <<$"EOF"\nmarker\nEOF\nrm -rf /tmp\n$EOF'],
      ["R05-PID", 'cat <<$"EOF"\nmarker\nEOF\nP=$(pgrep worker)\nkill "$P"\n$EOF'],
    ],
    simple: [
      ["S05-TMP", 'D=/own\ncat <<"EOF"\n EOF\nEOF \nrm -rf /tmp\nreadonly D=/tmp\nEOF\nrm -rf "$D"'],
      ["S05-PID", 'P=4242\ncat <<"EOF"\n EOF\nEOF \nreadonly P=0\n[[ P=Q -eq 0 ]]\nEOF\nkill "$P"'],
    ],
  },
  {
    id: "06", boundary: "mixed ANSI-C header cannot swallow post-delimiter execution",
    refused: [
      ["R06-TMP", "cat <<E$'OF'\nmarker\nEOF\nrm -rf /tmp\nE$OF"],
      ["R06-PID", "cat <<E$'OF'\nmarker\nEOF\nP=$(pgrep worker)\nkill \"$P\"\nE$OF"],
    ],
    simple: [
      ["S06-TMP", 'D=/tmp\nD=$(cat <<-\'WORD\'\n\t\t/own\n\t\tWORD\n)\nrm -rf "$D"'],
      ["S06-PID", 'P=4242\ncat <<-\'WORD\'\n\treadonly P=0\n\t$[P=Q]\n\tWORD\nkill "$P"'],
    ],
  },
  {
    id: "07", boundary: "double-quoted bracket arithmetic in ordinary argv writes parent",
    refused: [["R07", 'P=4242; Q=$(pgrep -n worker); : "$[P=Q]"; kill "$P"']],
    simple: [
      ["S07-singlequoted", 'P=4242; Q=$(pgrep -n worker); : \'$[P=Q]\'; kill "$P"'],
      ["S07-PIDreplacement", 'P=4242; Q=$(pgrep -n worker); P=4343; kill "$P"'],
      ["S07-expandedDATA", 'F=\'$[P=Q]\'; P=4242; : "$F"; kill "$P"'],
    ],
  },
  {
    id: "08", boundary: "unquoted bracket arithmetic in ordinary argv writes parent",
    refused: [["R08", 'P=4242; Q=$(pgrep -n worker); : $[P=Q]; kill "$P"']],
    simple: [
      ["S08-escapedbracket", 'P=4242; Q=$(pgrep -n worker); : \\$[P=Q]; kill "$P"'],
      ["S08-escapedarithmeticword", 'P=4242; : "\\$((P=0))"; kill "$P"'],
    ],
  },
  {
    id: "09", boundary: "quoted-looking bracket arithmetic in unquoted heredoc remains live",
    refused: [["R09", 'P=4242; Q=$(pgrep -n worker)\ncat <<WORD\n"$[P=Q]"\nWORD\nkill "$P"']],
    simple: [
      ["S09-quotedbody", 'P=4242; Q=$(pgrep -n worker)\ncat <<\'WORD\'\n"$[P=Q]"\nWORD\nkill "$P"'],
      ["S09-escapedbody", 'P=4242; Q=$(pgrep -n worker)\ncat <<WORD\n"\\$[P=Q]"\nWORD\nkill "$P"'],
    ],
  },
  {
    id: "10", boundary: "unquoted bracket arithmetic in heredoc writes parent",
    refused: [["R10", 'P=4242; Q=$(pgrep -n worker)\ncat <<WORD\n$[P=Q]\nWORD\nkill "$P"']],
    simple: [
      ["S10-quotedbody", 'P=4242; Q=$(pgrep -n worker)\ncat <<"WORD"\n$[P=Q]\nWORD\nkill "$P"'],
      ["S10-escapedbody", 'P=4242; Q=$(pgrep -n worker)\ncat <<WORD\n\\$[P=Q]\nWORD\nkill "$P"'],
    ],
  },
  {
    id: "11", boundary: "conditional arithmetic assignment operand writes parent even if false",
    refused: [["R11", 'P=4242; Q=$(pgrep -n worker); [[ P=Q -eq 0 ]]; kill "$P"']],
    simple: [
      ["S11-conditionaltext", 'P=4242; Q=$(pgrep -n worker); : \'[[ P=Q -eq 0 ]]\'; kill "$P"'],
      ["S11-subshell", 'P=4242; ( P=$(pgrep -n worker) ); kill "$P"'],
      ["S11-scalar", 'D=/tmp; D=/own; rm -rf "$D"'],
    ],
  },
];

// Separate original-policy group: quoted body readonly is DATA, not an allowance.
// This supported header must expose the real /tmp write after the exact terminator.
const tmpPair = {
  refused: [['R12-TMP', 'D=/own\ncat <<\'WORD\'\nreadonly D=/own\nWORD\nD=/tmp\nrm -rf "$D"']],
  simple: [['S12-executedreadonly', 'D=/own\ncat <<\'WORD\'\nmarker\nWORD\nreadonly D=/own\nD=/tmp\nrm -rf "$D"']],
} satisfies { refused: readonly Case[]; simple: readonly Case[] };

function expectVerdict(id: string, command: string, verdict: "STATE" | "TMP" | "SAFE"): void {
  const expected = verdict === "STATE"
    ? { blocked: false, wipe: false, exhausted: false, shellState: true }
    : { blocked: false, wipe: verdict === "TMP", exhausted: false };
  const result = scanCommand(command);
  expect(result, id).toStrictEqual(expected);
  expect(Object.keys(result).sort(), id).toStrictEqual(Object.keys(expected).sort());
  expect(killsByPattern(command), id).toBe(verdict === "STATE");
  expect(wipesTmp(command), id).toBe(verdict !== "SAFE");
}

describe("PR166 round8 DATA-only unsupported boundaries with SIMPLE controls", () => {
  for (const pair of statePairs) {
    it(`R${pair.id} STATE refuses ${pair.boundary}`, () => {
      for (const [id, command] of pair.refused) expectVerdict(id, command, "STATE");
    });
    it(`SIMPLE-A${pair.id} retains supported syntax and genuine inert DATA`, () => {
      for (const [id, command] of pair.simple) expectVerdict(id, command, "SAFE");
    });
  }
});

describe("PR166 round8 supported exact-header original TMP policy", () => {
  it("R12-TMP body readonly stays DATA and cannot hide real shared-root assignment", () => {
    for (const [id, command] of tmpPair.refused) expectVerdict(id, command, "TMP");
  });
  it("SIMPLE-A12 exact terminator exposes genuine executed readonly", () => {
    for (const [id, command] of tmpPair.simple) expectVerdict(id, command, "SAFE");
  });
});
