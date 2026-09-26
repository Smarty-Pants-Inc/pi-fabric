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
  });

  it.each([
    ["a short sleep", "sleep 30 && gh api repos/x/y"],
    ["a four-minute sleep", "sleep 240"],
    ["exactly the limit", "sleep 5m"],
    ["a short counted loop", "for i in {1..3}; do sleep 60; done"],
    ["a detached poll", "setsid bash -c 'for i in $(seq 1 14); do sleep 300; done' >/dev/null 2>&1 &"],
    ["a background job", "(sleep 900; notify) &"],
    ["no sleep at all", "npm run build && npm test"],
    ["sleep as text in a heredoc", "body=$(cat <<'EOF'\nthe agent ran sleep 900 in a loop\nEOF\n); gh api x -f body=\"$body\""],
    ["sleep as text in a quoted argument", "git commit -m \"fix: no more sleep 900 loops\""],
  ])("allows %s", (_name, command) => {
    expect(foregroundWaitRefusal(command)).toBeUndefined();
  });
});
