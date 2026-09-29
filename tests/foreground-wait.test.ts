import { describe, expect, it } from "vitest";
import { foregroundWaitRefusal, foregroundWaitSeconds } from "../src/guards/foreground-wait.js";

// smarty-dev#854: the waits seen live, and what must still run.
describe("foreground wait guard", () => {
  it.each([
    ["the 70-minute poll seen live", "for i in $(seq 1 14); do sleep 300; gh api repos/x/y/pulls/1 --jq .state; done", 4_200],
    ["one long sleep", "sleep 900; gh api repos/x/y", 900],
    ["a brace-counted loop", "for i in {1..10}; do sleep 60; done", 600],
    ["a C-style counted loop", "for ((i=0; i<12; i++)); do sleep 30; done", 360],
    ["a sleep with a unit", "sleep 6m", 360],
    ["a script run by bash -c", "bash -c 'sleep 900'", 900],
    // review/astra F1 on #71: nohup alone stays in the foreground; a timeout bounds only its command.
    ["nohup in the foreground", "nohup sleep 900", 900],
    ["a timeout followed by a long sleep", "timeout 1 true; sleep 900", 900],
    ["a background job followed by a foreground sleep", "(sleep 5) & sleep 900", 900],
    ["a background job that is then waited for", "sleep 900 & wait", 900],
    // review/astra F2 on #71: counts apply to their own loop body; nested loops multiply.
    ["a genuinely nested loop", "for i in {1..3}; do for j in {1..3}; do sleep 40; done; done", 360],
    // review/astra F4/F5 on #71: stepped and falling ranges, and timeout 0 (no deadline).
    ["a falling seq loop", "for remaining in $(seq 14 -1 1); do sleep 300; done", 4_200],
    ["a stepped seq loop", "for i in $(seq 0 60 600); do sleep 60; done", 660],
    ["a C-style loop that counts down", "for ((n=20; n>0; n--)); do sleep 30; done", 600],
    ["a long sleep under timeout 0", "timeout 0 sleep 900", 900],
  ])("refuses %s", (_name, command, seconds) => {
    expect(foregroundWaitSeconds(command)).toBe(seconds);
    expect(foregroundWaitRefusal(command)).toMatch(/Fabric refused this command: its foreground wait is about \d/);
  });

  // Acceptance audit on #854 (issuecomment-5884709683): live commands that the first guard let run for
  // 14-20 min. Verbatim from the session logs, with the tool timeout each was sent with.
  it.each([
    ["the org session's flock -w 1800 at 2026-09-28T19:14Z", "cd /home/paul/smarty/smarty-pants && flock -w 1800 ~/.local/share/smarty-dev/org-context/state/org-context-sync.lock -c \"date -u +%H:%M:%SZ; timeout 90 ~/.local/share/smarty-dev/factory/current/bin/smarty-hostd restart org-context-sync; echo rc=\\$?\"; sleep 5; pgrep -af \"org-context-sync.py\" | cut -c1-160; for p in $(pgrep -f \"setup/org-context-sync.py\"); do ps -o pid,lstart -p $p | tail -1; done", 1900, 1805],
    ["lightweight-fleet's 18-min jq poll at 2026-09-28T20:43Z", "T=~/.local/share/smarty-dev/factory/current/bin/smarty-github-app-token; for i in $(seq 1 18); do M=$(env -u GITHUB_TOKEN -u GH_TOKEN $T run Smarty-Pants-Inc -- gh api repos/Smarty-Pants-Inc/smarty-dev/pulls/1801 --jq '\"\\(.merged) \\(.merge_commit_sha)\"' 2>/dev/null); case \"$M\" in true*) echo \"$(date -u +%H:%M:%SZ) $M\"; break;; esac; sleep 60; done; echo \"last: $M\"", 1200, 1080],
    ["the org session's 14-min jq poll at 2026-09-28T20:50Z", "T=/home/paul/.local/share/smarty-dev/factory/current/bin/smarty-github-app-token; R=repos/Smarty-Pants-Inc/smarty-dev; for i in $(seq 1 14); do s=$(env -u GH_TOKEN -u GITHUB_TOKEN timeout 30 $T run Smarty-Pants-Inc -- gh api $R/pulls/1836 --jq '\"\\(.state) \\(.merged) \\(.merge_commit_sha)\"' 2>/dev/null); echo \"$(date -u +%H:%M) $s\"; case \"$s\" in *true*) break;; esac; sleep 60; done", 900, 840],
    ["the org session's 20-min jq poll at 2026-09-29T05:17Z", "T=/home/paul/.local/share/smarty-dev/factory/current/bin/smarty-github-app-token; for i in $(seq 1 20); do m=$(env -u GH_TOKEN -u GITHUB_TOKEN timeout 30 $T run Smarty-Pants-Inc -- gh api repos/Smarty-Pants-Inc/smarty-dev/pulls/1973 --jq '\"\\(.merged) \\(.merge_commit_sha) \\([.labels[].name]|join(\",\"))\"'); case $m in true*) echo \"$(date -u +%H:%MZ) $m\"; break;; esac; sleep 60; done; echo \"last: $m\"", 1300, 1200],
    ["smarty-knowledge-3-prefs's computed sleep at 2026-09-29T06:00Z", "now=$(date -u +%s); t=$(date -u -d \"06:20:30\" +%s); [ $t -gt $now ] && sleep $((t-now)); date -u +%H:%M:%S", 1500, 1500],
  ])("refuses %s", (_name, command, toolTimeoutS, seconds) => {
    expect(foregroundWaitSeconds(command, toolTimeoutS)).toBe(seconds);
    expect(foregroundWaitRefusal(command, toolTimeoutS)).toMatch(/Fabric refused this command/);
    expect(foregroundWaitRefusal(command)).toMatch(/Fabric refused this command/);
  });
  it.each([
    ["a sleep of a variable", "sleep $X"],
    ["a quoted computed sleep", "sleep \"$((deadline - $(date +%s)))\""],
    ["sleep infinity", "sleep infinity"],
    ["a flock that waits on a variable", "flock -w \"$WAIT\" /tmp/lock true"],
    ["a flock -w with an inline value", "flock --wait=900 /tmp/lock true"],
    ["a flock around a long sleep", "flock /tmp/lock sleep 900"],
  ])("refuses %s unless a timeout bounds it", (_name, command) => {
    expect(foregroundWaitRefusal(command)).toMatch(/Fabric refused this command/);
    expect(foregroundWaitRefusal(`timeout 120 ${command}`)).toBeUndefined();
    expect(foregroundWaitRefusal(command, 240)).toBeUndefined();           // the tool's timeout
  });

  it("refuses a sleep in an unbounded loop, and allows it under a timeout", () => {
    expect(foregroundWaitRefusal("while ! test -f done; do sleep 5; done")).toMatch(/an unbounded wait/);
    expect(foregroundWaitRefusal("until gh api repos/x/y/pulls/1 --jq .merged | grep -q true; do sleep 60; done"))
      .toMatch(/an unbounded wait/);
    expect(foregroundWaitSeconds("timeout 120 bash -c 'while ! test -f done; do sleep 5; done'")).toBe(120);
    expect(foregroundWaitRefusal("timeout 120 bash -c 'while ! test -f done; do sleep 5; done'")).toBeUndefined();
    expect(foregroundWaitRefusal("while ! test -f done; do sleep 5; done", 120)).toBeUndefined();   // the tool's timeout
    expect(foregroundWaitRefusal("timeout 0 bash -c 'while true; do sleep 60; done'")).toMatch(/an unbounded wait/);
    expect(foregroundWaitRefusal("for ((;;)); do sleep 1; done")).toMatch(/an unbounded wait/);
  });

  it.each([
    ["a short sleep", "sleep 30 && gh api repos/x/y", 30],
    ["a four-minute sleep", "sleep 240", 240],
    ["exactly the limit", "sleep 5m", 300],
    ["a short counted loop", "for i in {1..3}; do sleep 60; done", 180],
    ["two sequential loops", "for i in {1..3}; do sleep 30; done\nfor j in {1..3}; do sleep 30; done", 180],
    ["a sleep after a loop without one", "for i in {1..10}; do echo $i; done; sleep 60", 60],
    ["a detached poll", "setsid bash -c 'for i in $(seq 1 14); do sleep 300; done' >/dev/null 2>&1 &", 0],
    ["a nohup job in the background", "nohup bash -c 'sleep 900' >/tmp/log 2>&1 &", 0],
    ["a background job", "(sleep 900; notify) &", 0],
    ["no sleep at all", "npm run build && npm test 2>&1 | tail -5", 0],
    ["sleep as text in a heredoc", "body=$(cat <<'EOF'\nthe agent ran sleep 900 in a loop\nEOF\n); gh api x -f body=\"$body\"", 0],
    ["sleep as text in a quoted argument", "git commit -m \"fix: no more sleep 900 loops\"", 0],
    ["an if with a short branch", "if test -f x; then sleep 60; else sleep 30; fi", 60],
    ["a C-style loop with a step", "for ((elapsed=0; elapsed<240; elapsed+=30)); do sleep 30; done", 240],
    ["an empty seq range", "for i in $(seq 5 1); do sleep 900; done", 0],
    ["a stepped brace range", "for i in {0..100..25}; do sleep 60; done", 300],
    ["a C-style loop with v=v+S", "for ((i=1; i<=4; i=i+1)); do sleep 60; done", 240],
    ["a jq-interpolated poll that stays short", "for i in $(seq 1 4); do m=$(gh api x --jq '\"\\\\(.merged) \\\\(.sha)\"'); sleep 60; done", 240],
    ["an apostrophe in a heredoc body inside $(...)", "body=$(cat <<'EOF'\nit's fine; sleep 900 is text\nEOF\n); sleep 10", 10],
    ["a shift in arithmetic", "x=$((1<<3)); sleep 60", 60],
    ["a flock without -w (a free lock)", "flock ~/.local/state/versehq-write.lock git push -q origin HEAD", 0],
    ["a flock with a short wait", "flock -x -w 50 /tmp/lock -c 'sleep 30'", 80],
    ["a nonblocking flock", "flock -n /tmp/lock true", 0],
    ["fractional sleeps without a leading digit", "tmux send-keys -t q Escape; sleep .5; tmux send-keys -t q g; sleep 1.; sleep 0.25s", 1.75],
  ])("allows %s", (_name, command, seconds) => {
    expect(foregroundWaitSeconds(command)).toBe(seconds);
    expect(foregroundWaitRefusal(command)).toBeUndefined();
  });
});
