import { describe, expect, it } from "vitest";
import { scanCommand } from "../src/core/pattern-kill.js";

// PR #166 R1 acceptance ledger. Every command below is scanner DATA ONLY.
// Never pass this corpus to a shell, subprocess, eval, or a real tool-call hook.
// Each refusal/allowance is a separate test: a red refusal cannot hide its green pair.
type Policy = "kill" | "tmp";
type Pair = { name: string; policy: Policy; refuse: string; allow: string };
const pairs: Pair[] = [
  { name: "A1 exact review: diagnostic NAME=value argv is not assignment", policy: "tmp",
    refuse: `D=/tmp; echo D=/own; rm -rf "$D"`,
    allow: `D=/tmp; D=/own; rm -rf "$D"` },
  { name: "A1 exact review: prefix assignment does not change caller-expanded operand", policy: "tmp",
    refuse: `D=/tmp; D=/own rm -rf "$D"`,
    allow: `D=/own; D=/tmp rm -rf "$D"` },
  { name: "A1 exact review: prefix binding is restored after true", policy: "tmp",
    refuse: `D=/tmp; D=/own true; rm -rf "$D"`,
    allow: `D=/own; D=/tmp true; rm -rf "$D"` },
  { name: "A1 exact review: printf diagnostic format must not erase PID provenance", policy: "kill",
    refuse: `P=$(pgrep -f worker); printf 'P=%s\\n' ok; kill "$P"`,
    allow: `P=$(pgrep -f worker); P=4242; printf 'P=%s\\n' ok; kill "$P"` },
  { name: "A1 kill diagnostic assignment-looking argument", policy: "kill",
    refuse: `P=$(pgrep -f worker); echo P=4242; kill "$P"`,
    allow: `P=$(pgrep -f worker); P=4242; kill "$P"` },
  { name: "A1 kill prefix argv uses caller provenance", policy: "kill",
    refuse: `P=$(pgrep -f worker); P=4242 kill "$P"`,
    allow: `P=4242; P=7777 kill "$P"` },
  { name: "A1 kill prefix restores provenance", policy: "kill",
    refuse: `P=$(pgrep -f worker); P=4242 true; kill "$P"`,
    allow: `P=4242; P=7777 true; kill "$P"` },
  { name: "A1 temporary IFS read restores whitespace caller splitting", policy: "tmp",
    refuse: `P='/own /tmp'; IFS=: read -r unused <<< ok; rm -rf $P`,
    allow: `P='/own /tmp'; IFS=:; read -r unused <<< ok; rm -rf $P` },
  { name: "A1 temporary IFS read restores colon caller splitting", policy: "tmp",
    refuse: `IFS=:; P='/own:/tmp'; IFS=, read -r unused <<< ok; rm -rf $P`,
    allow: `IFS=,; P='/own:/tmp'; IFS=: read -r unused <<< ok; rm -rf $P` },
  { name: "A1 prefix IFS does not affect current rm operand expansion", policy: "tmp",
    refuse: `P='/own /tmp'; IFS=: rm -rf $P`,
    allow: `IFS=:; P='/own /tmp'; IFS=' ' rm -rf $P` },
  { name: "A2 exact review: skipped && variable overwrite", policy: "kill",
    refuse: `P=$(pgrep -f worker); false && P=4242; kill "$P"`,
    allow: `P=$(pgrep -f worker); P=4242; kill "$P"` },
  { name: "F11 exact review: skipped && file truncation", policy: "kill",
    refuse: `pgrep -f worker > .local/pids; false && : > .local/pids; xargs -a .local/pids kill`,
    allow: `pgrep -f worker > .local/pids; : > .local/pids; xargs -a .local/pids kill` },
];

pairs.push(
  { name: "A1 prefix IFS controls read destinations, which persist after IFS restoration", policy: "tmp",
    refuse: `IFS=,; IFS=: read -r first D <<< '/own:/tmp'; rm -rf "$D"`,
    allow: `IFS=,; P='/own:/tmp'; IFS=: read -r first D <<< '/own:/own2'; rm -rf "$D"; rm -rf $P` },
  { name: "A1 temporary variable prefix read input expands in caller, destination persists", policy: "tmp",
    refuse: `D=/tmp; D=/own read -r OUT <<< "$D"; rm -rf "$OUT"`,
    allow: `D=/own; D=/tmp read -r OUT <<< "$D"; rm -rf "$OUT"` },
  { name: "A1 unrelated prefix read destination clears actual prior unsafe binding", policy: "tmp",
    refuse: `D=/own; TAG=diagnostic read -r D <<< /tmp; rm -rf "$D"`,
    allow: `D=/tmp; TAG=diagnostic read -r D <<< /own; rm -rf "$D"` },
  { name: "A1 unrelated prefix read PID destination persists", policy: "kill",
    refuse: `P=4242; TAG=diagnostic read -r P < <(pgrep -f worker); kill "$P"`,
    allow: `P=$(pgrep -f worker); TAG=diagnostic read -r P <<< 4242; kill "$P"` },
  { name: "A1 temporary variable prefix printf argv expands in caller, destination persists", policy: "tmp",
    refuse: `D=/tmp; D=/own printf -v OUT '%s' "$D"; rm -rf "$OUT"`,
    allow: `D=/own; D=/tmp printf -v OUT '%s' "$D"; rm -rf "$OUT"` },
  { name: "A1 unrelated prefix printf destination persists and replaces prior binding", policy: "tmp",
    refuse: `D=/own; TAG=diagnostic printf -v D '%s' /tmp; rm -rf "$D"`,
    allow: `D=/tmp; TAG=diagnostic printf -v D '%s' /own; rm -rf "$D"` },
  { name: "A1 temporary variable prefix printf PID argv uses caller provenance", policy: "kill",
    refuse: `P=$(pgrep -f worker); P=4242 printf -v OUT '%s' "$P"; kill "$OUT"`,
    allow: `P=4242; P=7777 printf -v OUT '%s' "$P"; kill "$OUT"` },
);

const skipped = [
  ["&&", (write: string) => `false && ${write}`],
  ["||", (write: string) => `true || ${write}`],
  ["if", (write: string) => `if false; then ${write}; fi`],
  ["zero-iteration for", (write: string) => `for unused in; do ${write}; done`],
  ["zero-iteration while", (write: string) => `while false; do ${write}; done`],
] as const;
for (const [branch, wrap] of skipped) {
  for (const policy of ["kill", "tmp"] as const) {
    const variable = policy === "kill" ? "P" : "D";
    const unsafe = policy === "kill" ? "$(pgrep -f worker)" : "/tmp";
    const safe = policy === "kill" ? "4242" : "/own";
    const consume = policy === "kill" ? `kill "$P"` : `rm -rf "$D"`;
    pairs.push({ name: `A2 ${branch} variable overwrite retains ${policy} provenance`, policy,
      refuse: `${variable}=${unsafe}; ${wrap(`${variable}=${safe}`)}; ${consume}`,
      allow: `${variable}=${unsafe}; ${variable}=${safe}; ${consume}` });
    const producer = policy === "kill" ? "pgrep -f worker" : "ls -d /tmp/tmp.*";
    const recorded = policy === "kill" ? "cat .local/server.pid" : "cat .local/owned.list";
    const file = policy === "kill" ? ".local/pids" : ".local/paths";
    const fileConsume = policy === "kill" ? `xargs -a ${file} kill` : `xargs -a ${file} rm -rf`;
    for (const [kind, write] of [["truncate", `: > ${file}`], ["recorded replacement", `${recorded} > ${file}`]] as const) {
      pairs.push({ name: `F11 ${branch} ${kind} retains ${policy} file provenance`, policy,
        refuse: `${producer} > ${file}; ${wrap(write)}; ${fileConsume}`,
        allow: `${producer} > ${file}; ${write}; ${fileConsume}` });
    }
  }
}

// Nested receivers get the same conditional lifecycle ledger; the inline shell text
// contains its own variable initialization (no assumption about exported parent vars).
for (const [branch, wrap] of skipped) {
  for (const policy of ["kill", "tmp"] as const) {
    const variable = policy === "kill" ? "P" : "D";
    const unsafe = policy === "kill" ? "$(pgrep -f worker)" : "/tmp";
    const safe = policy === "kill" ? "4242" : "/own";
    const consume = policy === "kill" ? `kill "$P"` : `rm -rf "$D"`;
    pairs.push({ name: `A2 ${branch} nested shell variable receiver (${policy})`, policy,
      refuse: `sh -c '${variable}=${unsafe}; ${wrap(`${variable}=${safe}`)}; ${consume}'`,
      allow: `sh -c '${variable}=${unsafe}; ${variable}=${safe}; ${consume}'` });
    const producer = policy === "kill" ? "pgrep -f worker" : "ls -d /tmp/tmp.*";
    const recorded = policy === "kill" ? "cat .local/server.pid" : "cat .local/owned.list";
    const file = policy === "kill" ? ".local/pids" : ".local/paths";
    const receiver = policy === "kill" ? `kill "$(cat ${file})"` : `rm -rf "$(cat ${file})"`;
    pairs.push({ name: `F11 ${branch} nested shell file capture receiver (${policy})`, policy,
      refuse: `sh -c '${producer} > ${file}; ${wrap(`${recorded} > ${file}`)}; ${receiver}'`,
      allow: `sh -c '${producer} > ${file}; ${recorded} > ${file}; ${receiver}'` });
  }
}

for (const policy of ["kill", "tmp"] as const) {
  const producer = policy === "kill" ? "pgrep -f worker" : "ls -d /tmp/tmp.*";
  const recorded = policy === "kill" ? "cat .local/server.pid" : "cat .local/owned.list";
  const file = policy === "kill" ? ".local/pids" : ".local/paths";
  const consume = policy === "kill" ? `kill "$value"` : `rm -rf "$value"`;
  for (const redirect of [">", ">>"] as const) {
    pairs.push({ name: `F12 fd3 open empty then ${redirect} producer before read (${policy})`, policy,
      refuse: `: > ${file}; { ${producer} ${redirect} ${file}; read -r -u 3 value; ${consume}; } 3< ${file}`,
      allow: `: > ${file}; { ${recorded} ${redirect} ${file}; read -r -u 3 value; ${consume}; } 3< ${file}` });
  }
  const captureConsume = policy === "kill" ? `kill "$(cat <&3)"` : `rm -rf "$(cat <&3)"`;
  for (const redirect of [">", ">>"] as const) {
    pairs.push({ name: `F12 live ${redirect} content reaches fd3 consuming capture (${policy})`, policy,
      refuse: `: > ${file}; { ${producer} ${redirect} ${file}; ${captureConsume}; } 3< ${file}`,
      allow: `: > ${file}; { ${recorded} ${redirect} ${file}; ${captureConsume}; } 3< ${file}` });
  }
  pairs.push({ name: `F12 selected fd3 capture ignores separately routed safe stdin (${policy})`, policy,
    refuse: `: > ${file}; { ${producer} >> ${file}; ${captureConsume}; } 3< ${file} < .local/recorded`,
    allow: `: > ${file}; { ${recorded} >> ${file}; ${captureConsume}; } 3< ${file} < .local/recorded` });
  pairs.push({ name: `F12 entry alias identity remains live after body alias changes and append (${policy})`, policy,
    refuse: `: > ${file}; F=${file}; G="$F"; { F=.local/other; ${producer} >> "$G"; ${captureConsume}; } 3< "$F"`,
    allow: `${recorded} > ${file}; F=${file}; G="$F"; { G=.local/other; ${producer} >> "$G"; ${captureConsume}; } 3< "$F"` });
  pairs.push({ name: `F12 independent recorded/owned fd3 ignores other unsafe file (${policy})`, policy,
    refuse: `${producer} > ${file}; { read -r -u 3 value; ${consume}; } 3< ${file}`,
    allow: `${recorded} > ${file}; { ${producer} > .local/other; read -r -u 3 value; ${consume}; } 3< ${file}` });
  pairs.push({ name: `F12 fd3 identity snapshots entry alias, not later alias (${policy})`, policy,
    refuse: `: > ${file}; F=${file}; { F=.local/other; ${producer} > ${file}; read -r -u 3 value; ${consume}; } 3< "$F"`,
    allow: `${recorded} > ${file}; F=${file}; { F=.local/other; ${producer} > "$F"; read -r -u 3 value; ${consume}; } 3< "$F"` });
  pairs.push({ name: `F12 fd3 identity snapshots entry cwd, not body cwd (${policy})`, policy,
    refuse: `cd /own; : > selected; { cd /else; ${producer} > /own/selected; read -r -u 3 value; ${consume}; } 3< selected`,
    allow: `cd /own; ${recorded} > selected; { cd /else; ${producer} > selected; read -r -u 3 value; ${consume}; } 3< selected` });
  pairs.push({ name: `F12 left-to-right copied fd0 follows fd3 identity (${policy})`, policy,
    refuse: `: > ${file}; { ${producer} > ${file}; read -r value; ${consume}; } 3< ${file} 0<&3`,
    allow: `: > ${file}; { ${recorded} > ${file}; read -r value; ${consume}; } 3< ${file} 0<&3` });
  pairs.push({ name: `F12 reverse duplication preserves original safe fd0 (${policy})`, policy,
    refuse: `${producer} > ${file}; { read -r value; ${consume}; } 3< ${file} 0<&3`,
    allow: `${producer} > ${file}; { { read -r value; ${consume}; } 0<&3 3< ${file}; } 3< .local/recorded` });
  pairs.push({ name: `F12 close fd3 does not close already copied fd0 (${policy})`, policy,
    refuse: `${producer} > ${file}; { read -r value; ${consume}; } 3< ${file} 0<&3 3<&-`,
    allow: `${recorded} > ${file}; { read -r value; ${consume}; } 3< ${file} 0<&3 3<&-` });
  pairs.push({ name: `F12 closed fd3 does not supply unsafe contents (${policy})`, policy,
    refuse: `${producer} > ${file}; { read -r -u 3 value; ${consume}; } 3< ${file}`,
    allow: `${producer} > ${file}; { read -r -u 3 value; ${consume}; } 3< ${file} 3<&-` });
  pairs.push({ name: `F12 subshell routing stays child-local (${policy})`, policy,
    refuse: `${producer} > ${file}; { ( read -r value; ${consume} ) 3< ${file} 0<&3; } < .local/recorded`,
    allow: `${producer} > ${file}; { ( : ) 3< ${file} 0<&3; read -r value; ${consume}; } < .local/recorded` });
}

describe("PR166 R1 paired DATA regressions", () => {
  for (const pair of pairs) {
    it(`${pair.name} — refuse`, () => {
      const verdict = scanCommand(pair.refuse);
      expect(verdict.exhausted).toBe(false);
      expect(pair.policy === "kill" ? verdict.blocked : verdict.wipe).toBe(true);
    });
    it(`${pair.name} — allow`, () => {
      expect(scanCommand(pair.allow)).toEqual({ blocked: false, wipe: false, exhausted: false });
    });
  }
});

const quote = (text: string): string => `'${text.replaceAll("'", "'\\''")}'`;
const nestedEval = (text: string, depth: number): string => {
  for (let i = 0; i < depth; i++) text = `eval ${quote(text)}`;
  return text;
};
const replayTree = (commands: number): string => {
  let text = `pgrep worker; ${"echo safe; ".repeat(commands)}`;
  for (let i = 0; i < 3; i++) text = `pgrep worker; echo $(${text})`;
  return text;
};

describe("PR166 R1 reader precision without relaxing amplification refusals", () => {
  it("A3 exact 500 short quotes / 9504 ASCII bytes are inert and allowed", () => {
    const args = Array.from({ length: 500 }, (_, i) => `".local/item-${String(i).padStart(4, "0")}"`);
    const command = `echo ${args.join(" ")}`;
    expect(args).toHaveLength(500);
    expect(command.length).toBe(9504); // ASCII: character count equals byte count.
    expect(scanCommand(command)).toEqual({ blocked: false, wipe: false, exhausted: false });
  });
  it("N1 unchanged aggregate-reference amplification still exhausts both policies", () => {
    const command = `A=${"x".repeat(4096)}; B=${"$A".repeat(1024)}; :`;
    expect(scanCommand(command)).toEqual({ blocked: true, wipe: true, exhausted: true });
  });
  it("N1 unchanged nested collection/replay amplification still exhausts both policies", () => {
    expect(scanCommand(replayTree(3000))).toEqual({ blocked: true, wipe: true, exhausted: true });
  });
  it("N1 same collection/replay structure with ordinary work stays allowed", () => {
    expect(scanCommand(replayTree(8))).toEqual({ blocked: false, wipe: false, exhausted: false });
  });
  it("N1 unchanged script-depth amplification still exhausts both policies", () => {
    expect(scanCommand(nestedEval("echo safe", 8))).toEqual({ blocked: true, wipe: true, exhausted: true });
  });
  it("N1 small nested recorded PID / own path stays allowed", () => {
    expect(scanCommand(nestedEval("kill 4242; rm -f .local/own-file", 3)))
      .toEqual({ blocked: false, wipe: false, exhausted: false });
  });
});
