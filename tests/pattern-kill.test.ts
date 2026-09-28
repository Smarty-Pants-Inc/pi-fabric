import { describe, expect, it } from "vitest";
import { killsByPattern, PATTERN_KILL_REASON } from "../src/core/pattern-kill.js";

// smarty-dev#774: the three real incidents verbatim, then the forms the rule refuses and the
// reads and recorded-PID kills it must still allow.
const refused: Array<[string, string]> = [
  ["incident 09-25 14:05Z: the Lucky stub test on Dev1", `pkill -f '[w]atch'`],
  ["incident 09-26 03:05Z: over ssh on m4max", `ssh m4max "pkill -f 'sleep 30' -P 1"`],
  ["incident 09-26 03:05Z: the command itself", `pkill -f 'sleep 30' -P 1`],
  ["incident 09-27 ~23:2xZ: dev-lead's own shell", `pkill -f "retry418.sh"`],
  ["incident 09-27, unquoted", `pkill -f retry418.sh`],
  ["dev-lead's earlier slip", `cd /tmp; pkill -f retry226.sh; nohup ./retry226.sh > /tmp/retry226.log 2>&1 &`],
  ["a pkill after a cat", `cat /tmp/retry278.log; pkill -f "^/bin/bash ./retry278.sh" 2>/dev/null`],
  ["killall", `killall node`],
  ["a pkill by path", `/usr/bin/pkill -U paul sleep`],
  ["a pkill behind sudo and timeout", `sudo timeout 5 pkill -9 vitest`],
  ["a pkill behind env", `env -u GH_TOKEN FOO=1 pkill -f watcher`],
  ["a pkill after &&", `make build && pkill -f server`],
  ["a pkill in a pipeline's last stage", `echo x | pkill -f y`],
  ["a pkill on the next line", `echo start\npkill -f x`],
  ["a pkill after a backslash continuation", `sleep 1 && \\\n  pkill -f x`],
  ["kill $(pgrep …)", `kill $(pgrep -f retry418.sh)`],
  ["kill -9 $(pgrep …)", `kill -9 $(pgrep -f vitest)`],
  ["kill \`pgrep …\`", "kill `pgrep -f server`"],
  ["kill \"$(pgrep …)\"", `kill "$(pgrep -f server)"`],
  ["kill $(pidof …)", `kill $(pidof node)`],
  ["kill $(ps | grep | awk)", `kill $(ps aux | grep '[s]erver' | awk '{print $2}')`],
  ["pgrep | xargs kill", `pgrep -f watch | xargs kill`],
  ["pgrep | xargs -r kill -9", `pgrep -f watch | xargs -r kill -9`],
  ["pgrep |\\n xargs kill", `pgrep -f watch |\n  xargs kill`],
  ["ps | grep | awk | xargs kill", `ps aux | grep '[w]atch' | awk '{print $2}' | xargs kill`],
  ["a captured PID list", `P=$(pgrep -f server); kill $P`],
  ["a for loop over pgrep", `for p in $(pgrep -f server); do kill "$p"; done`],
  ["a while loop fed by pgrep", `pgrep -f server | while read p; do kill $p; done`],
  ["bash -c 'pkill …'", `bash -c 'pkill -f server'`],
  ["bash -lc 'kill $(pgrep …)'", `bash -lc 'kill $(pgrep -f server)'`],
  ["sh -c with pgrep | xargs kill", `sh -c "pgrep -f x | xargs kill"`],
  ["ssh HOST 'kill $(pgrep …)'", `ssh -p 22 m4max 'kill $(pgrep -f "sleep 30")'`],
  ["ssh HOST 'pgrep … | xargs kill'", `ssh m4max 'pgrep -f sleep | xargs kill'`],
  ["ssh HOST bash -c '…'", `ssh m4max bash -c "'pkill -f sleep'"`],
  ["ssh pgrep piped into a local xargs kill", `ssh m4max pgrep -f sleep | xargs kill`],
  ["a heredoc script fed to ssh", `ssh m4max <<'EOF'\ncd /tmp\npkill -f sleep\nEOF`],
  ["a heredoc script fed to bash", `bash <<EOF\nkillall node\nEOF`],
  ["eval", `eval "pkill -f server"`],
  ["a pkill in a command substitution", `echo "$(pkill -f server)"`],
  ["a pkill in a subshell", `(cd /tmp && pkill -f x)`],
  ["a pkill in an if", `if true; then pkill -f x; fi`],
  // review/astra F1 on #105: a lookup expanded into the script a shell, eval or ssh runs.
  ["bash -c \"kill $(pgrep …)\"", `bash -c "kill $(pgrep -f worker)"`],
  ["eval \"kill $(pgrep …)\"", `eval "kill $(pgrep -f worker)"`],
  ["ssh HOST \"kill $(pgrep …)\"", `ssh HOST "kill $(pgrep -f worker)"`],
  ["ssh HOST kill $(pgrep …), unquoted", `ssh HOST kill $(pgrep -f worker)`],
  ["ssh HOST \"kill `pgrep …`\"", "ssh HOST \"kill `pgrep -f worker`\""],
  // review/astra F2 on #105: substitutions in a redirection target and an unquoted heredoc run.
  ["a redirection target", `: >"$(pkill -f worker)"`],
  ["an unquoted redirection target", `echo x > $(pkill -f worker).log`],
  ["single quotes in an unquoted heredoc", `cat <<EOF\n'$(pkill -f worker)'\nEOF`],
  ["a # line in an unquoted heredoc", `cat <<EOF\n# $(pkill -f worker)\nEOF`],
  ["a backtick in an unquoted heredoc", "cat <<EOF\nnote `pkill -f worker`\nEOF"],
  ["a lookup in an unquoted heredoc, then kill", `cat > p.txt <<EOF\n$(pgrep -f worker)\nEOF\nkill $(cat p.txt) $P`],
  // review/astra F3 on #105: a leading redirection and long or clustered wrapper options.
  ["a leading 2> redirection", `2>/dev/null pkill -f worker`],
  ["a leading >file redirection", `>out.log pkill -f worker`],
  ["sudo --user X", `sudo --user paul pkill -f worker`],
  ["sudo -Eu X", `sudo -Eu paul pkill -f worker`],
  ["env --unset X", `env --unset GH_TOKEN pkill -f worker`],
  ["env --unset=X", `env --unset=GH_TOKEN pkill -f worker`],
  ["env -S 'pkill …'", `env -S 'pkill -f worker'`],
  ["nice -n 5", `nice -n 5 pkill -f worker`],
  ["timeout 5", `timeout 5 pkill -f worker`],
  ["timeout --signal KILL 5", `timeout --signal KILL 5 pkill -f worker`],
  ["nohup", `nohup pkill -f worker &`],
  ["setsid", `setsid pkill -f worker`],
  ["command", `command pkill -f worker`],
  ["exec", `exec pkill -f worker`],
  ["xargs -r kill", `pgrep -f worker | xargs -r kill`],
  ["xargs --max-args 1 kill", `pgrep -f worker | xargs --max-args 1 kill -TERM`],
  ["a lookup kill with a trailing redirection", `kill $(pgrep -f worker) 2>/dev/null`],
];

const allowed: Array<[string, string]> = [
  ["a string that mentions pkill", `echo "never use pkill"`],
  ["a grep for pkill", `grep -rn pkill src/ docs/`],
  ["a grep for kill $(pgrep", `grep -rn 'kill $(pgrep' .`],
  ["dev-lead's note that quotes the incident",
    `P=~/.local/state/dev-lead-posts; printf -- '- %s NOTE: I ran pkill -f "retry418.sh" (name pattern, against #774)\\n' "$(date -u +%H:%MZ)" >> "$P/notes.md"`],
  ["a heredoc body that describes the incident",
    `cat > /tmp/incident.md <<'EOF'\n**What ran.** lucky-sweep ran this on m4max:\n\`\`\`\npkill -f 'sleep 30' -P 1\n\`\`\`\nEOF`],
  ["a gh api comment that names the forms",
    `gh api repos/o/r/issues/774/comments -f body='Refused: pkill, killall, kill $(pgrep x), pgrep x | xargs kill'`],
  ["a commit message", `git commit -m "fix: refuse pkill and kill \\$(pgrep)"`],
  ["kill with literal PIDs", `kill 12345 12346`],
  ["kill -TERM of a process group", `kill -TERM -- -4242`],
  ["kill %job", `sleep 100 & kill %1`],
  ["kill -0", `kill -0 12345 && echo alive`],
  ["kill -0 of a looked-up PID", `kill -0 $(pgrep -f server)`],
  ["a recorded PID from $!", `./server & PID=$!; sleep 2; kill "$PID"`],
  ["a recorded PID from a file", `kill $(cat /tmp/x/server.pid)`],
  ["a recorded PID with a redirect", `kill "$PID" 2>/dev/null || true`],
  ["pgrep alone", `pgrep -fl vitest`],
  ["pgrep -c", `pgrep -f 'retry22[678]' -c`],
  ["pgrep in a while wait", `while pgrep -f build >/dev/null; do sleep 1; done`],
  ["a check after a recorded kill", `kill $PID; sleep 1; pgrep -f server || echo gone`],
  ["a ps check before a group kill", `ps -o pid,ppid,pgid,command -p $PID; kill -- -$PGID`],
  ["a ps | grep check before a recorded kill", `ps -eo pid,pgid,args | grep server; kill -- -$PGID`],
  ["a PGID read by ps", `PGID=$(ps -o pgid= -p "$PID" | tr -d ' '); kill -TERM -- -$PGID`],
  ["smarty-reap stop", `bin/smarty-reap stop .local/reap/server.json`],
  ["smarty-reap run", `bin/smarty-reap run .local/reap/server.json -- ./server --port 8080`],
  ["ssh with a literal kill", `ssh m4max 'kill 4242'`],
  ["ssh pgrep alone", `ssh m4max "pgrep -fl 'sleep 30'"`],
  ["bash -c echo pkill", `bash -c 'echo pkill is refused'`],
  ["a comment that mentions pkill", `ls # never pkill here`],
  ["a word that contains pkill", `./pkill-guard-test.sh --dry-run`],
  ["a file named pkill", `cat docs/pkill.md`],
  ["a literal kill after a captured lookup", `P=$(pgrep -f server); kill 4242`],
  ["a comment that holds a pkill after ;", `ls # do not run: sleep 1; pkill -f x`],
  ["a recorded PID piped from ps", `ps -o pid= -p "$PID" | xargs kill`],
  ["pgrep, then a recorded kill on the next line", `pgrep -fl server\nkill "$PID"`],
  // Counterexamples for review/astra on #105.
  ["bash -c \"kill $PID\"", `bash -c "kill $PID"`],
  ["bash -c echo of a lookup", `bash -c 'echo "kill $(pgrep x) is refused"'`],
  ["eval \"kill $PID\"", `eval "kill $PID"`],
  ["ssh HOST kill of a recorded PID file", `ssh HOST "kill $(cat /tmp/w/worker.pid)"`],
  ["a redirection target with date", `: >"$(date +%s).log"`],
  ["a quoted heredoc with a substitution", `cat <<'EOF'\n'$(pkill -f worker)'\nEOF`],
  ["an unquoted heredoc that names pkill", `cat <<EOF\nnever run pkill -f worker ($(date -u +%H:%MZ))\nEOF`],
  ["a leading 2> on pgrep", `2>/dev/null pgrep -f worker`],
  ["sudo --user X kill of a literal PID", `sudo --user paul kill 4242`],
  ["env --unset X grep pkill", `env --unset GH_TOKEN grep -n pkill notes.md`],
  ["command -v pkill", `command -v pkill`],
  ["nohup with redirections", `nohup ./server > server.log 2>&1 &`],
  ["a vitest filter", `./node_modules/.bin/vitest run tests/pattern-kill.test.ts -t pkill`],
];

describe("pattern-kill guard (smarty-dev#774)", () => {
  it.each(refused)("refuses %s", (_label, command) => {
    expect(killsByPattern(command)).toBe(true);
  });

  it.each(allowed)("allows %s", (_label, command) => {
    expect(killsByPattern(command)).toBe(false);
  });

  it("names the fix in its reason", () => {
    expect(PATTERN_KILL_REASON).toMatch(/bin\/smarty-reap stop/);
    expect(PATTERN_KILL_REASON).toMatch(/kill <PID>/);
  });
});
