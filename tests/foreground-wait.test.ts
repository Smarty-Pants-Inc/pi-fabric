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
  ])("allows %s", (_name, command, seconds) => {
    expect(foregroundWaitSeconds(command)).toBe(seconds);
    expect(foregroundWaitRefusal(command)).toBeUndefined();
  });
});
