import { describe, expect, it } from "vitest";
import { killsByPattern, TMP_WIPE_REASON, wipesTmp } from "../src/core/pattern-kill.js";

// smarty-dev#1998: two fleet-wide /tmp wipes on 2026-09-29, then the forms the rule refuses and
// the deletes of an agent's own exact paths it must still allow.
const refused: Array<[string, string]> = [
  ["incident 10:32Z: a Playful lane, verbatim", `rm -rf /tmp/tmp.*`],
  ["incident 05:33Z style: find over /tmp/tmp.* with -exec rm", `find /tmp/tmp.* -maxdepth 1 \\( -name c.md -o -name body.md \\) -exec rm -rf {} +`],
  ["incident 05:33Z style: find /tmp -name 'tmp.*' -exec rm", `find /tmp -maxdepth 1 -name 'tmp.*' -mmin -120 -exec rm -rf {} \\;`],
  ["incident 05:33Z style: a loop over matched dirs", `for d in /tmp/tmp.*; do [ -e "$d/c.md" ] && rm -rf "$d"; done`],
  ["incident 05:33Z style: rm of a find over /tmp", `rm -rf $(find /tmp -maxdepth 1 -name 'tmp.*' -newer /tmp/x)`],
  ["rm -f /tmp/*", `rm -f /tmp/*`],
  ["rm -r -f /tmp/tmp.?????????", `rm -r -f /tmp/tmp.?????????`],
  ["a bracket glob", `rm -rf /tmp/tmp.[A-Z]*`],
  ["/var/tmp", `rm -rf /var/tmp/*`],
  ["/tmp itself", `rm -rf /tmp`],
  ["/tmp/ itself", `rm -rf /tmp/`],
  ["a long flag and --", `rm --recursive --force -- /tmp/tmp.*`],
  ["sudo rm", `sudo rm -rf /tmp/tmp.*`],
  ["a second operand", `rm -rf /tmp/tmp.AbC123 /tmp/tmp.*`],
  ["a glob after an exact dir and ..", `rm -rf /tmp/tmp.AbC123/../*`],
  ["cd /tmp, then a relative glob", `cd /tmp && rm -rf tmp.*`],
  ["ls | xargs rm", `ls -d /tmp/tmp.* | xargs rm -rf`],
  ["find /tmp | xargs rm", `find /tmp -maxdepth 1 -name 'tmp.*' -print0 | xargs -0 rm -rf`],
  ["find /tmp -delete", `find /tmp -name '*.md' -delete`],
  ["find /var/tmp -exec rm", `find /var/tmp -type f -exec rm {} +`],
  ["find -exec sh -c rm", `find /tmp/tmp.* -exec sh -c 'rm -rf "$1"' _ {} \\;`],
  ["a variable that holds a glob", `P=/tmp/tmp.*; rm -rf $P`],
  ["a variable that holds /tmp", `T=/tmp; rm -rf "$T"/*`],
  ["a while loop fed by ls /tmp", `ls -d /tmp/tmp.* | while read d; do rm -rf "$d"; done`],
  ["bash -c", `bash -c 'rm -rf /tmp/tmp.*'`],
  ["ssh", `ssh m4max 'rm -rf /tmp/tmp.*'`],
  ["a heredoc to bash", `bash <<'EOF'\nrm -rf /tmp/tmp.*\nEOF`],
  ["after other commands", `cd ~/w && make && rm -rf /tmp/tmp.* 2>/dev/null; echo done`],
  // From fix-1993's check (smarty-dev ec08fcac, #2093).
  ["a trailing redirection", `rm -rf /tmp/tmp.* 2>/dev/null`],
  ["rm -f -- /var/tmp/x?", `rm -f -- /var/tmp/x?`],
  ["sudo rm -r /tmp/[ab]*", `sudo rm -r /tmp/[ab]*`],
  ["a glob dir with a concrete leaf", `cd ~ && rm -rf /tmp/pytest-*/x`],
  ["/bin/rm", `/bin/rm -rf /tmp/*.log`],
  ["a quoted prefix, then an unquoted glob", `rm -rf "/tmp/"tmp.*`],
  // fix-1993 allowed this as remote; the unquoted glob expands on Dev1 first, and m5/m4max are shared too.
  ["ssh with an unquoted glob", `ssh m5 rm -rf /tmp/tmp.*`],
];

const allowed: Array<[string, string]> = [
  ["an exact mktemp path", `rm -rf /tmp/tmp.AbC123`],
  ["the own dir by variable", `rm -rf "$D"`],
  ["the own dir from mktemp", `D=$(mktemp -d); cd "$D"; rm -rf "$D"`],
  ["a glob below the own dir by variable", `rm -f "$D"/*.json`],
  ["a glob below an exact tmp.XXXX dir", `rm -f /tmp/tmp.AbC123/*.md`],
  ["find below an exact dir with -delete", `find /tmp/tmp.AbC123 -name '*.log' -delete`],
  ["a quoted star is literal", `rm -f "/tmp/tmp.*"`],
  ["an escaped star is literal", `rm -f /tmp/tmp.\\*`],
  ["a relative glob outside /tmp", `rm -rf build/*`],
  ["an absolute glob outside /tmp", `rm -rf /home/paul/w/.local/*.json`],
  ["a listing of /tmp", `ls -la /tmp/tmp.*`],
  ["find /tmp without delete", `find /tmp -maxdepth 1 -name 'tmp.*' -user paul`],
  ["grep for the incident", `grep -rn 'rm -rf /tmp/tmp.*' docs/`],
  ["echo of the rule", `echo "never rm -rf /tmp/tmp.*"`],
  ["a heredoc note", `cat > note.md <<'EOF'\nrm -rf /tmp/tmp.*\nEOF`],
  ["a gh comment that names the command", `gh api repos/o/r/issues/1998/comments -f body='ran rm -rf /tmp/tmp.*'`],
  ["$TMPDIR contents", `rm -rf "$TMPDIR"/*`],
  ["cd elsewhere, then a relative glob", `cd /tmp && cd ~/w && rm -rf tmp.*`],
  ["rm of a list file's names", `cat .local/mine.txt | xargs rm -rf`],
  // From fix-1993's check (smarty-dev ec08fcac, #2093).
  ["an exact dir held in a variable", `D=/tmp/tmp.X1; rm -rf "$D"`],
  ["a single-quoted star", `rm -rf '/tmp/tmp.*'`],
  ["a double-quoted star mid-name", `rm -rf "/tmp/a*b"`],
  ["a relative glob", `rm -f ./*.log`],
  ["a glob under the home cache", `rm -rf ~/.cache/wt-*`],
  ["echo of the command, unquoted", `echo rm -rf /tmp/tmp.*`],
];

describe("tmp-wipe guard (smarty-dev#1998)", () => {
  it.each(refused)("refuses %s", (_label, command) => {
    expect(wipesTmp(command)).toBe(true);
  });

  it.each(allowed)("allows %s", (_label, command) => {
    expect(wipesTmp(command)).toBe(false);
  });

  it("does not change the kill-by-pattern verdict", () => {
    expect(killsByPattern(`rm -rf /tmp/tmp.*`)).toBe(false);
  });

  it("names the fix in its reason", () => {
    expect(TMP_WIPE_REASON).toContain(`delete only your own mktemp -d path by its exact name ("$D"), #1508/#1998`);
  });
});
