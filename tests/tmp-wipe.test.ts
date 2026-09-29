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
  // Round 1 on PR #148: review/astra 1, security S1: an assignment's quotes do not protect a later unquoted use.
  ["a single-quoted glob in a variable, expanded unquoted", `P='/tmp/tmp.*'; rm -rf $P`],
  ["a double-quoted glob in a variable, expanded unquoted", `P="/tmp/tmp.*"; rm -rf \${P}`],
  ["an escaped glob in a variable, expanded unquoted", `P=/tmp/tmp.\\*; rm -rf $P`],
  ["a quoted glob built from a variable", `T=/tmp; P="$T/tmp.*"; rm -rf $P`],
  ["an unknown value expanded unquoted next to a /tmp path", `L=$(cat .local/list); ls /tmp; rm -rf $L`],
  // review/astra 2, security S2: a local nested shell inherits the cwd.
  ["cd /tmp, then sh -c with a relative glob", `cd /tmp && sh -c 'rm -rf tmp.*'`],
  ["cd /tmp, then bash -c with a relative glob", `cd /tmp; bash -c "rm -rf *"`],
  ["cd /tmp, then eval", `cd /tmp && eval 'rm -rf tmp.*'`],
  ["cd /tmp, then a substitution", `cd /tmp && echo $(rm -rf tmp.*)`],
  // Round 2 on PR #148, security F1: a /tmp listing fed through a redirect to read or xargs.
  ["a while loop fed by a process substitution", `while read -r d; do rm -rf "$d"; done < <(find /tmp -maxdepth 1 -name 'tmp.*' -mmin -60)`],
  ["xargs fed by a process substitution", `xargs -0 rm -rf < <(find /tmp -maxdepth 1 -name 'tmp.*' -print0)`],
  ["xargs fed by a here-string", `xargs rm -rf <<< "$(ls -d /tmp/tmp.*)"`],
  ["xargs -a with a process substitution", `xargs -a <(ls -d /tmp/tmp.*) rm -rf`],
  // Security F2: an array assigned from a /tmp glob, and mapfile/readarray.
  ["an array of a /tmp glob", `dirs=(/tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  ["a spaced array of a /tmp glob", `dirs=( /tmp/tmp.* ); rm -rf "\${dirs[@]}"`],
  ["mapfile from a /tmp listing", `mapfile -t dirs < <(ls -d /tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  ["readarray from a /tmp listing", `readarray -t dirs < <(ls -d /tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  // Security F3: after cd /tmp, a listing without a path lists /tmp.
  ["cd /tmp, then ls | head | xargs rm", `cd /tmp && ls -t | head -6 | xargs rm -rf`],
  ["cd /tmp, then rm of an ls substitution", `cd /tmp && rm -rf $(ls | grep '^tmp\\.')`],
  ["cd /tmp, then find without a root | xargs rm", `cd /tmp && find -maxdepth 1 -name 'tmp.*' | xargs rm -rf`],
  ["cd /tmp, then a loop fed by ls in a process substitution", `cd /tmp && while read d; do rm -rf "$d"; done < <(ls)`],
  ["an array of a /tmp listing", `dirs=($(ls -d /tmp/tmp.*)); rm -rf "\${dirs[@]}"`],
  ["xargs --arg-file with a process substitution", `xargs --arg-file=<(ls -d /tmp/tmp.*) rm -rf`],
  // Round 3 on PR #148, security F4: redirect listings use the state at the redirect, not script entry.
  ["F4: a loop variable feeds a redirected find listing", `for t in /tmp /var/tmp; do while read -r d; do rm -rf "$d"; done < <(find "$t" -maxdepth 1 -name 'tmp.*' -mmin +60); done`],
  ["F4: an assigned root feeds redirected xargs", `T=/tmp; xargs -0 rm -rf < <(find "$T" -maxdepth 1 -name 'tmp.*' -print0)`],
  ["F4: an assigned root feeds redirected mapfile", `T=/tmp; mapfile -t dirs < <(ls -d "$T"/tmp.*); rm -rf "\${dirs[@]}"`],
  ["F4: an assigned root feeds redirected readarray", `T=/tmp; readarray -t dirs < <(ls -d "$T"/tmp.*); rm -rf "\${dirs[@]}"`],
  ["F4: cd -P to a shared root before a redirected loop", `cd -P /tmp && while read -r d; do rm -rf "$d"; done < <(ls)`],
  ["F4: cd -- to a shared root before a redirected loop", `cd -- /tmp && while read -r d; do rm -rf "$d"; done < <(ls)`],
  ["F4: cd through an assigned root before a redirected loop", `T=/tmp; cd "$T" && while read -r d; do rm -rf "$d"; done < <(ls)`],
  // Security F5: a compound producer's listing survives its closing word/operator before the pipe.
  ["F5: a loop listing both shared roots feeds xargs", `for t in /tmp /var/tmp; do find "$t" -maxdepth 1 -name 'tmp.*' -mmin +60; done | xargs rm -rf`],
  ["F5: a brace group listing both shared roots feeds xargs", `{ ls -d /tmp/tmp.*; ls -d /var/tmp/tmp.*; } | xargs rm -rf`],
  ["F5: a subshell listing a shared root feeds xargs", `(ls -d /tmp/tmp.*) | xargs rm -rf`],
  ["F5: a loop listing both shared roots feeds read", `for t in /tmp /var/tmp; do find "$t" -maxdepth 1 -name 'tmp.*' -mmin +60; done | while read -r d; do rm -rf "$d"; done`],
  ["F5: a brace group listing both shared roots feeds read", `{ ls -d /tmp/tmp.*; ls -d /var/tmp/tmp.*; } | while read -r d; do rm -rf "$d"; done`],
  ["F5: a subshell listing a shared root feeds read", `(ls -d /tmp/tmp.*) | while read -r d; do rm -rf "$d"; done`],
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
  // Round 1 on PR #148: a quoted use of a glob value is one literal name.
  ["a quoted glob value, used quoted", `P='/tmp/tmp.*'; rm -f "$P"`],
  ["single-quoted variable text is literal", `P='/tmp/*'; rm -f '$P'`],
  ["an unknown value, quoted", `L=$(cat .local/list); ls /tmp; rm -rf "$L"`],
  ["an unknown value, unquoted, no /tmp in the command", `L=$(cat .local/list); rm -rf $L`],
  // review/astra 3: a recorded mktemp dir is owned, even when mktemp names /tmp.
  ["a recorded mktemp -p /tmp dir", `D=$(mktemp -d -p /tmp); rm -rf "$D"`],
  ["a recorded mktemp -p /tmp dir, unquoted", `D=$(mktemp -d -p /tmp); rm -rf $D`],
  ["a glob inside a recorded mktemp dir", `D=$(mktemp -d -p /tmp); rm -rf "$D"/*`],
  ["a mktemp template under /tmp", `D=$(mktemp -d /tmp/wt.XXXXXX); rm -rf "$D"`],
  // review/astra 2: a nested shell under an exact owned dir; a child cd does not leak to the parent.
  ["cd to an exact dir, then sh -c with a glob", `cd /tmp/tmp.AbC123 && sh -c 'rm -rf *'`],
  ["a child cd /tmp does not move the parent", `cd ~/w && sh -c 'cd /tmp' && rm -rf tmp.*`],
  ["ssh does not inherit the local cwd", `cd /tmp && ssh m4max 'rm -rf build/*'`],
  // Round 2 on PR #148: F1 a loop over an own list file, F2 an array below the own dir, F3 ls in an own dir.
  ["a while loop over an own list file", `while read -r f; do rm -f "$D/$f"; done < "$D/list"`],
  ["an array of a glob below the own dir", `files=("$D"/*.json); rm -f "\${files[@]}"`],
  ["cd to the own dir, then ls | xargs rm", `cd "$D" && ls | xargs rm -f`],
  ["cd to an exact tmp dir, then ls | xargs rm", `cd /tmp/tmp.AbC123 && ls | xargs rm -f`],
  ["cd /tmp, then a recorded mktemp dir", `cd /tmp && D=$(mktemp -d) && rm -rf "$D"`],
  // Round 3 F4/F5 counterparts: an own-directory listing must not become a shared-root feed.
  ["F4: a while loop over an own process-substitution listing", `while read -r f; do rm -f "$D/$f"; done < <(ls "$D")`],
  ["F4: a recorded mktemp dir feeds an own redirected loop", `D=$(mktemp -d); while read -r f; do rm -f "$D/$f"; done < <(ls "$D")`],
  ["F4: a loop variable feeds redirected listings of exact own dirs", `for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do while read -r d; do rm -rf "$d"; done < <(find "$t" -maxdepth 1 -name '*.json'); done`],
  ["F4: an assigned exact own dir feeds redirected xargs", `T=/tmp/tmp.AbC123; xargs -0 rm -f < <(find "$T" -maxdepth 1 -name '*.json' -print0)`],
  ["F4: an assigned exact own dir feeds redirected mapfile", `T=/tmp/tmp.AbC123; mapfile -t files < <(ls -d "$T"/*.json); rm -f "\${files[@]}"`],
  ["F4: an assigned exact own dir feeds redirected readarray", `T=/tmp/tmp.AbC123; readarray -t files < <(ls -d "$T"/*.json); rm -f "\${files[@]}"`],
  ["F4: cd -P to an exact own dir before a redirected loop", `cd -P /tmp/tmp.AbC123 && while read -r f; do rm -f "$f"; done < <(ls)`],
  ["F4: cd -- to an exact own dir before a redirected loop", `cd -- /tmp/tmp.AbC123 && while read -r f; do rm -f "$f"; done < <(ls)`],
  ["F4: cd through an assigned exact own dir before a redirected loop", `T=/tmp/tmp.AbC123; cd "$T" && while read -r f; do rm -f "$f"; done < <(ls)`],
  ["F5: a loop listing exact own dirs feeds xargs", `for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do find "$t" -maxdepth 1 -name '*.json'; done | xargs rm -f`],
  ["F5: a brace group listing exact own dirs feeds xargs", `{ ls -d /tmp/tmp.AbC123/*.json; ls -d /var/tmp/tmp.Def456/*.json; } | xargs rm -f`],
  ["F5: a subshell listing an exact own dir feeds xargs", `(ls -d /tmp/tmp.AbC123/*.json) | xargs rm -f`],
  ["F5: a loop listing exact own dirs feeds read", `for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do find "$t" -maxdepth 1 -name '*.json'; done | while read -r f; do rm -f "$f"; done`],
  ["F5: a brace group listing exact own dirs feeds read", `{ ls -d /tmp/tmp.AbC123/*.json; ls -d /var/tmp/tmp.Def456/*.json; } | while read -r f; do rm -f "$f"; done`],
  ["F5: a subshell listing an exact own dir feeds read", `(ls -d /tmp/tmp.AbC123/*.json) | while read -r f; do rm -f "$f"; done`],
];

describe("tmp-wipe guard (smarty-dev#1998)", () => {
  it.each(refused)("refuses %s", (_label, command) => {
    expect(wipesTmp(command)).toBe(true);
  });

  it.each(allowed)("allows %s", (_label, command) => {
    expect(wipesTmp(command)).toBe(false);
  });

  // Security S3: assignment expansion is bounded (the text is only read, never run).
  it("reads a doubling assignment chain in bounded time", () => {
    let script = "A=xxxxxxxxxxxxxxxx";
    for (let i = 0; i < 40; i++) script += `; A=$A$A$A$A`;
    const start = Date.now();
    expect(wipesTmp(`${script}; rm -rf "$A"`)).toBe(false);
    expect(Date.now() - start).toBeLessThan(2000);
  });

  it("does not change the kill-by-pattern verdict", () => {
    expect(killsByPattern(`rm -rf /tmp/tmp.*`)).toBe(false);
  });

  it("names the fix in its reason", () => {
    expect(TMP_WIPE_REASON).toContain(`delete only your own mktemp -d path by its exact name ("$D"), #1508/#1998`);
  });
});
