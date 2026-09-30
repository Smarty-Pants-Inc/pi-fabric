import { describe, expect, it, vi } from "vitest";
import { scanCommand, killsByPattern, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "P=4242; P=4242 bash -c \"N=__pk_arg_; eval \\\"readonly \\${N}2=4242\\\"; eval \\\"readonly \\${N}3=4242\\\"; kill \\\"$P\\\"\"",
  "readonly P=4242; read -r P <<< 7777; kill \"$P\"",
  "readonly P=4242; printf -v P %s 7777; kill \"$P\"",
  "readonly D=/own; read -r D <<< /tmp; rm -rf \"$D\"",
  "readonly D=/own; printf -v D %s /tmp; rm -rf \"$D\"",
  "D=$(ls -d /tmp/tmp.*); printf -v D %s /own; rm -rf \"$D\"",
  "P=4242; eval 'readonly P'; read -r P <<<7777; kill \"$P\"",
  "eval 'readonly P=4242'; read -r P<<<7777; kill \"$P\"",
  "readonly D=/own; D=/tmp printf -v D %s /tmp; echo D\\=/tmp; rm -rf \"$D\""
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


// Command strings are DATA ONLY: never execute them in a shell or tool hook.
const pairs = [
  { name: "reported readonly read PID", policy: "kill", refuse: `readonly P=$(pgrep worker); read -r P <<< 4242; kill "$P"`, allow: `readonly P=4242; read -r P <<< 7777; kill "$P"` },
  { name: "reported readonly printf PID", policy: "kill", refuse: `readonly P=$(pgrep worker); printf -v P %s 4242; kill "$P"`, allow: `readonly P=4242; printf -v P %s 7777; kill "$P"` },
  { name: "readonly read TMP mirror", policy: "tmp", refuse: `readonly D=$(ls -d /tmp/tmp.*); read -r D <<< /own; rm -rf "$D"`, allow: `readonly D=/own; read -r D <<< /tmp; rm -rf "$D"` },
  { name: "readonly printf TMP mirror", policy: "tmp", refuse: `readonly D=$(ls -d /tmp/tmp.*); printf -v D %s /own; rm -rf "$D"`, allow: `readonly D=/own; printf -v D %s /tmp; rm -rf "$D"` },
  { name: "mutable recorded read overwrite", conservativeRefusal: true, policy: "kill", refuse: `P=4242; read -r P < <(pgrep worker); kill "$P"`, allow: `P=$(pgrep worker); read -r P < .local/recorded.pid; kill "$P"` },
  { name: "mutable concrete printf overwrite", policy: "tmp", refuse: `D=/own; printf -v D %s /tmp; rm -rf "$D"`, allow: `D=$(ls -d /tmp/tmp.*); printf -v D %s /own; rm -rf "$D"` },
  { name: "new bash sh SSH attributes are child-owned", policy: "kill", refuse: `readonly P=$(pgrep worker); bash -c 'readonly P; P=4242; kill "$P"'`, allow: `readonly P=$(pgrep worker); bash -c 'P=4242; kill "$P"'; sh -c 'P=4242; kill "$P"'; ssh host 'P=4242; kill "$P"'` },
  { name: "subshell and captures copy attributes without leaking declarations", policy: "kill", refuse: `readonly P=$(pgrep worker); (read -r P <<< 4242; kill "$P")`, allow: `P=$(pgrep worker); (readonly P); echo $(readonly P); cat <(readonly P); P=4242; kill "$P"` },
  { name: "eval creates parent readonly attribute", policy: "kill", refuse: `P=$(pgrep worker); eval 'readonly P'; read -r P <<<4242; kill "$P"`, allow: `P=4242; eval 'readonly P'; read -r P <<<7777; kill "$P"` },
  { name: "eval creates immutable binding with its own provenance", policy: "kill", refuse: `eval 'readonly P=$(pgrep worker)'; read -r P<<<4242; kill "$P"`, allow: `eval 'readonly P=4242'; read -r P<<<7777; kill "$P"` },
  { name: "conditional possible attribute keeps both binding paths", conservativeRefusal: true, policy: "kill", refuse: `P=$(pgrep worker); false && readonly P; read -r P <<< 4242; kill "$P"`, allow: `P=4242; false && readonly P; read -r P <<< 7777; kill "$P"` },
  { name: "temporary prefix and nonbinding diagnostic unchanged", policy: "tmp", refuse: `readonly D=/tmp; D=/own read -r D <<< /own; echo 'D=/own'; rm -rf "$D"`, allow: `readonly D=/own; D=/tmp printf -v D %s /tmp; echo D\\=/tmp; rm -rf "$D"` },
];

const coldPairs = [
  { name: "exact A1 caller bytes resist child alias writes", policy: "kill", refuse: String.raw`P=$(pgrep worker); P=4242 bash -c "__pk_arg_2=4242; __pk_arg_3=4242; kill \"$P\""`, allow: String.raw`P=4242; P=4242 bash -c "__pk_arg_2=4242; __pk_arg_3=4242; kill \"$P\""` },
  { name: "known parent alias-shaped literals cannot replace caller bytes", policy: "kill", refuse: String.raw`P=$(pgrep worker); __pk_arg_2=4242; __pk_arg_3=4242; P=4242 bash -c "kill \"$P\""`, allow: String.raw`P=4242; __pk_arg_2=4242; __pk_arg_3=4242; P=4242 bash -c "kill \"$P\""` },
  { name: "opaque TMP caller bytes resist child alias writes", policy: "tmp", refuse: String.raw`D=$(ls -d /tmp/tmp.*); D=/own bash -c "__pk_arg_2=/own; __pk_arg_3=/own; rm -rf \"$D\""`, allow: String.raw`D=/own; D=/own bash -c "__pk_arg_2=/own; __pk_arg_3=/own; rm -rf \"$D\""` },
  { name: "computed eval destination cannot overwrite attribution cell", policy: "kill", refuse: String.raw`P=$(pgrep worker); P=4242 bash -c "N=__pk_arg_; eval \"readonly \${N}2=4242\"; eval \"readonly \${N}3=4242\"; kill \"$P\""`, allow: String.raw`P=4242; P=4242 bash -c "N=__pk_arg_; eval \"readonly \${N}2=4242\"; eval \"readonly \${N}3=4242\"; kill \"$P\""` },
];

describe("PR166 cold first-scan caller attribution — DATA only", () => {
  for (const pair of coldPairs) for (const half of ["refuse", "allow"] as const) {
    it(`${pair.name} — ${half}`, async () => {
      vi.resetModules();
      const cold = await import("../src/core/pattern-kill.js");
      const expected = { blocked: half === "refuse" && pair.policy === "kill", wipe: half === "refuse" && pair.policy === "tmp", exhausted: false };
      // The first scan is the actual one-scan hook path. Never substitute a rescan
      // wrapper verdict: global placeholder drift previously hid this failure.
      expectRound5Guard(pair[half], cold.scanCommand(pair[half]), expected);
      void 0;
      void 0;
    });
  }
});

describe("PR166 lexically known readonly bind failures — paired DATA", () => {
  for (const pair of pairs) for (const half of ["refuse", "allow"] as const) {
    it(`${pair.name} — ${half}${"conservativeRefusal" in pair && half === "allow" ? " — conservative refusal" : ""}`, () => {
      const expected = { blocked: (half === "refuse" || ("conservativeRefusal" in pair && pair.conservativeRefusal === true)) && pair.policy === "kill", wipe: (half === "refuse" || ("conservativeRefusal" in pair && pair.conservativeRefusal === true)) && pair.policy === "tmp", exhausted: false };
      expectRound5Guard(pair[half], scanCommand(pair[half]), expected);
      void 0;
      void 0;
    });
  }
});
