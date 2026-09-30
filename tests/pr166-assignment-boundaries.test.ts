import { describe, expect, it } from "vitest";
import { scanCommand, killsByPattern, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "D=/tmp; export D=/own; rm -rf \"$D\"",
  "P=$(pgrep -f worker); declare P=4242; kill \"$P\"",
  "D=/tmp; typeset D=/own; rm -rf \"$D\"",
  "D=/own; D=/tmp read -r D <<< /tmp; rm -rf \"$D\"",
  "P=4242; P=7777 read -r P < <(pgrep -f worker); kill \"$P\"",
  "D=/own; D=/tmp printf -v D '%s' /tmp; rm -rf \"$D\"",
  "P=4242; P=7777 printf -v P '%s' \"$(pgrep -f worker)\"; kill \"$P\""
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
  const intentional = round5IntentionalState.has(command);
  const originallyRefused = original.blocked === true || original.wipe === true || original.exhausted === true || original.overall === true;
  if (!intentional && !(originallyRefused && result.shellState === true)) expect("shellState" in result, command).toBe(false);
  if (intentional || (originallyRefused && result.shellState === true)) {
    expect(Object.prototype.hasOwnProperty.call(result, "shellState"), command).toBe(true);
    expect(result, command).toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  } else if (original.overall !== undefined) {
    expect(Object.keys(result).sort(), command).toEqual(["blocked", "exhausted", "wipe"]);
    expect(result.exhausted, command).toBe(original.exhausted ?? false);
    expect(typeof result.blocked, command).toBe("boolean");
    expect(typeof result.wipe, command).toBe("boolean");
    expect(result.blocked || result.wipe, command).toBe(original.overall);
  } else {
    expect(result, command).toEqual(original);
  }
  expect(killsByPattern(command), command).toBe(result.blocked || result.shellState === true);
  expect(wipesTmp(command), command).toBe(result.wipe || result.shellState === true);
}


// A1 assignment-boundary inputs are DATA ONLY. Never execute this corpus in Bash,
// a subprocess, eval, or a real tool-call hook. Each half is an independent test.
type Pair = { name: string; policy: "kill" | "tmp"; refuse: string; allow: string };
const pairs: Pair[] = [
  { name: "A1 env receiver sees temporary child binding", policy: "tmp",
    refuse: `D=/own; env D=/tmp bash -c 'rm -rf "$D"'`,
    allow: `D=/tmp; env D=/own bash -c 'rm -rf "$D"'` },
  { name: "A1 env receiver safe binding cannot overwrite caller parent", policy: "tmp",
    refuse: `D=/tmp; env D=/own bash -c 'rm -rf "$D"'; rm -rf "$D"`,
    allow: `D=/own; env D=/own bash -c 'rm -rf "$D"'; rm -rf "$D"` },
  { name: "A1 quoted NAME= diagnostic argv cannot clear unsafe value", policy: "tmp",
    refuse: `D=/tmp; echo "D=/own"; rm -rf "$D"`,
    allow: `D=/tmp; D=/own; echo "D=/own"; rm -rf "$D"` },
  { name: "A1 escaped NAME= diagnostic argv cannot clear lookup provenance", policy: "kill",
    refuse: `P=$(pgrep -f worker); echo P\\=4242; kill "$P"`,
    allow: `P=$(pgrep -f worker); P=4242; echo P\\=4242; kill "$P"` },
  { name: "A1 export actual assignment argv and safe overwrite", policy: "tmp",
    refuse: `D=/own; export D=/tmp; rm -rf "$D"`,
    allow: `D=/tmp; export D=/own; rm -rf "$D"` },
  { name: "A1 declare actual assignment argv and safe overwrite", policy: "kill",
    refuse: `P=4242; declare P=$(pgrep -f worker); kill "$P"`,
    allow: `P=$(pgrep -f worker); declare P=4242; kill "$P"` },
  { name: "A1 typeset actual assignment argv and safe overwrite", policy: "tmp",
    refuse: `D=/own; typeset D=/tmp; rm -rf "$D"`,
    allow: `D=/tmp; typeset D=/own; rm -rf "$D"` },
  { name: "A1 readonly actual assignment argv and safe overwrite", policy: "kill",
    refuse: `P=4242; readonly P=$(pgrep -f worker); kill "$P"`,
    allow: `P=$(pgrep -f worker); readonly P=4242; kill "$P"` },
  { name: "A1 read same temporary prefix destination restores outer value", policy: "tmp",
    refuse: `D=/tmp; D=/own read -r D <<< /own2; rm -rf "$D"`,
    allow: `D=/own; D=/tmp read -r D <<< /tmp; rm -rf "$D"` },
  { name: "A1 read same temporary prefix destination restores outer provenance", policy: "kill",
    refuse: `P=$(pgrep -f worker); P=7777 read -r P <<< 4242; kill "$P"`,
    allow: `P=4242; P=7777 read -r P < <(pgrep -f worker); kill "$P"` },
  { name: "A1 printf -v same temporary prefix destination restores outer value", policy: "tmp",
    refuse: `D=/tmp; D=/own printf -v D '%s' /own2; rm -rf "$D"`,
    allow: `D=/own; D=/tmp printf -v D '%s' /tmp; rm -rf "$D"` },
  { name: "A1 printf -v same temporary prefix destination restores outer provenance", policy: "kill",
    refuse: `P=$(pgrep -f worker); P=7777 printf -v P '%s' 4242; kill "$P"`,
    allow: `P=4242; P=7777 printf -v P '%s' "$(pgrep -f worker)"; kill "$P"` },
];

describe("PR166 A1 assignment boundaries — paired DATA only", () => {
  for (const pair of pairs) {
    it(`${pair.name} — refuse`, () => {
      const verdict = scanCommand(pair.refuse);
      expect(verdict.exhausted).toBe(false);
      expectRound5Guard(pair.refuse, verdict, { blocked: pair.policy === "kill", wipe: pair.policy === "tmp", exhausted: false });
    });
    it(`${pair.name} — allow`, () => {
      expectRound5Guard(pair.allow, scanCommand(pair.allow), { blocked: false, wipe: false, exhausted: false });
    });
  }
});
