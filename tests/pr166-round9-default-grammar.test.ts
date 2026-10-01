import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY. Never dispatch these strings to a shell, subprocess or native tool.
// Four R/SIMPLE pairs (the fourth is Main's format-allowlist completion).
// Main owns execution and merged qualification.
const STATE = { blocked: false, wipe: false, exhausted: false, shellState: true };
const SAFE = { blocked: false, wipe: false, exhausted: false };

function check(rows: readonly string[], refused: boolean): void {
  for (const command of rows) {
    const expected = refused ? STATE : SAFE;
    const actual = scanCommand(command);
    expect(actual, command).toStrictEqual(expected);
    expect(Object.keys(actual).sort(), command).toStrictEqual(Object.keys(expected).sort());
    expect(killsByPattern(command), command).toBe(refused);
    expect(wipesTmp(command), command).toBe(refused);
  }
}

describe("PR166 round9 default grammar DATA-only admission", () => {
  it("R01 refuses live unquoted aggregates in original words and redirect targets", () => {
    check([
      'D=/tmp; set -- /tmp; D=$(printf \'%s\' $@); rm -rf "$D"',
      'D=/tmp; set -- /tmp; D=$(printf \'%s\' $*); rm -rf "$D"',
      ': ${@}',
      ': ${*}',
      ': ${A[@]}',
      ': ${A[*]}',
      ': > $@',
      ': < $*',
      ': > ${@}',
      ': < ${*}',
      ': > ${A[@]}',
      ': < ${A[*]}',
    ], true);
  });
  it("SIMPLE-A01 preserves individual positionals and inert aggregate DATA", () => {
    check([
      'set -- /own; D=$(printf \'%s\' "$1"); rm -rf "$D"',
      'set -- /own; D=$(printf \'%s\' ${1}); rm -rf "$D"',
      ': \'$@ $* ${@} ${*} ${A[@]} ${A[*]}\'',
      ': \\$@ \\$* \\${@} \\${*} \\${A[@]} \\${A[*]}',
      'F=\'$@ $* ${@} ${*} ${A[@]} ${A[*]}\'; : "$F"',
      'F=\'$@\'; : > "$F"',
    ], false);
  });
  it("R02 refuses unproved special parameters before state or output grants", () => {
    check([
      ': $?',
      ': $$',
      ': $#',
      ': $-',
      ': "$?"',
      ': "$$"',
      ': "$#"',
      ': "$-"',
      ': > $?',
      ': < $$',
      ': ${?}',
      ': ${#}',
    ], true);
  });
  it("SIMPLE-A02 preserves named scalars recorded $! and inert special DATA", () => {
    check([
      'D=/own; rm -rf "$D"',
      'D=/own; rm -rf "${D}"',
      'P=$!; kill "$P"',
      ': \'$? $$ $# $-\'',
      ': \\$? \\$\\$ \\$# \\$-',
      'F=\'$? $$ $# $-\'; : "$F"',
      'F=\'$?\'; : > "$F"',
    ], false);
  });
  // Main closes grammar-table row38: only %s / %% literal format conversions
  // are admitted; an UNKNOWN stdout feed cannot fence printf's %n parent write.
  it("R04 refuses unproved printf conversions before parent-state effects", () => {
    check([
      'D=0/tmp; printf "%n" IFS; D=$(printf "%s" $D); rm -rf "$D"',
      'printf "%n" P',
      'printf "%b" marker',
      'printf "%q" marker',
      'printf "%d" 4242',
    ], true);
  });
  it("SIMPLE-A04 preserves string-only literal formats and inert conversion DATA", () => {
    check([
      'printf "%s" marker',
      'printf "%%n"',
      'printf "%%s"',
      'printf "%s" "%n %b %q %d"',
      ': "%n %b %q %d"',
    ], false);
  });
  it("R03 refuses orphan and misnested compound closers", () => {
    check([
      'done',
      'fi',
      '}',
      ')',
      'if true; then :; done; fi',
      'for item in marker; do :; fi; done',
      '{ :; fi; }',
      '( :; done )',
      'if true; then :; }; fi',
      '{ :; ); }',
    ], true);
  });
  it("SIMPLE-A03 preserves proved if loop plain case and reserved-word DATA", () => {
    check([
      'if true; then :; fi',
      'for item in marker; do :; done',
      'while false; do :; done',
      'until true; do :; done',
      'case marker in marker) : ;; esac',
      '{ :; }',
      '( : )',
      ': \'done fi } )\'',
      ': \\done \\fi \\} \\)',
      'F=\'done fi } )\'; : "$F"',
      '"done"; "fi"',
    ], false);
  });
});
