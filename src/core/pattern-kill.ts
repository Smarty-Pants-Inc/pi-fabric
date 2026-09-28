// smarty-dev#774: on a shared host a kill by name also kills other owners' processes. Three real
// incidents: `pkill -f '[w]atch'` on Dev1 (09-25), `pkill -f 'sleep 30' -P 1` over ssh on m4max
// (09-26: BSD pkill read `-P 1` as patterns), and `pkill -f "retry418.sh"` that matched its own
// shell (09-27). The rule was in every agent's context and did not stop them, so every bash call
// that kills by pattern is refused.
//
// Refused: `pkill` and `killall` in any form, and a `kill` of PIDs that came from a name lookup
// (pgrep, pidof, ps, grep, awk): `kill $(pgrep …)`, `kill `pgrep …``, `pgrep … | xargs kill`,
// `P=$(pgrep …); kill $P`, `for p in $(pgrep …)`, `pgrep … | while read p`, also inside
// `ssh HOST '…'`, `bash -c '…'`, `eval`, `env -S` and a heredoc fed to a shell or ssh, with the
// lookups the calling shell expands into them. Allowed: `kill <literal PIDs>`, `kill %job`,
// `kill -0`, a PID file (`kill $(cat run.pid)`), a variable of unknown origin (`kill $PID` after
// `PID=$!`), `bin/smarty-reap`, and `pgrep` alone. Quoted text is data: `echo "never use pkill"`,
// `grep -n pkill` and a quoted-delimiter heredoc pass.
// Scope: this catches the kill-by-pattern forms agents actually write, against mistakes and
// injected content (threat model A, smarty-dev#820). It is not a sandbox against a deliberately
// evasive same-uid process: a script file, a binary or `python -c 'os.kill(…)'` can still kill by
// pattern; per-agent OS users (#820) are that boundary.
// ponytail: a small shell reader, not a shell. A kill hidden in a script file, an alias or a
// variable holding a command passes; revisit if agents route around it.

type Word = { text: string; subs: string[]; dynamic: boolean };
type Token = { op: string } | { word: Word } | { redirect: Word } | { heredoc: { body: string; quoted: boolean } };

const OPERATORS = ["&&", "||", ";;", "|&", "|", "&", ";", "(", ")"];

/** Reads to the character that closes `open` at `index` (just after the opener), nesting quotes. */
function readBalanced(text: string, index: number, open: string, close: string): number {
  let depth = 1;
  while (index < text.length) {
    const c = text[index]!;
    if (c === "\\") { index += 2; continue; }
    if (c === "'" && close !== "`") { const end = text.indexOf("'", index + 1); index = end < 0 ? text.length : end + 1; continue; }
    if (c === "\"" && close !== "`") { index = readDouble(text, index + 1, { subs: [] }); continue; }
    if (c === close) { depth -= 1; index += 1; if (depth === 0) return index; continue; }
    if (open !== close && c === open) depth += 1;
    index += 1;
  }
  return index;
}

/** Reads a double-quoted body from `index`; collects its `$(…)` and backtick substitutions. */
function readDouble(text: string, index: number, word: { subs: string[]; text?: string; dynamic?: boolean }): number {
  while (index < text.length && text[index] !== "\"") {
    const c = text[index]!;
    if (c === "\\") { word.text = (word.text ?? "") + (text[index + 1] ?? ""); index += 2; continue; }
    if ((c === "$" && text[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(text, start, "`", "`") : readBalanced(text, start, "(", ")");
      word.subs.push(text.slice(start, end - 1));
      word.text = (word.text ?? "") + "$";
      word.dynamic = true;
      index = end;
      continue;
    }
    if (c === "$") word.dynamic = true;
    word.text = (word.text ?? "") + c;
    index += 1;
  }
  return index + 1;
}

/** The `$(…)` and backtick substitutions an unquoted heredoc body runs; quotes and `#` are literal. */
function heredocSubs(body: string): string[] {
  const subs: string[] = [];
  for (let index = 0; index < body.length;) {
    const c = body[index]!;
    if (c === "\\") { index += 2; continue; }
    if ((c === "$" && body[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(body, start, "`", "`") : readBalanced(body, start, "(", ")");
      subs.push(body.slice(start, end - 1));
      index = end;
      continue;
    }
    index += 1;
  }
  return subs;
}

function tokenize(source: string): Token[] {
  // As in comment-cut (#104): a backslash-newline continues the line.
  const text = source.replace(/\\\r?\n/g, " ");
  const tokens: Token[] = [];
  const pending: Array<{ delimiter: string; strip: boolean; token: { heredoc: { body: string; quoted: boolean } } }> = [];
  let index = 0;
  let word: Word | undefined;
  let target = false;
  const endWord = (): void => {
    if (word) tokens.push(target ? { redirect: word } : { word });
    word = undefined;
    target = false;
  };
  const current = (): Word => (word ??= { text: "", subs: [], dynamic: false });
  while (index < text.length) {
    const c = text[index]!;
    if (c === " " || c === "\t" || c === "\r") { endWord(); index += 1; continue; }
    if (c === "\n") {
      endWord();
      index += 1;
      for (const heredoc of pending.splice(0)) {
        const lines: string[] = [];
        while (index < text.length) {
          const end = text.indexOf("\n", index);
          const line = text.slice(index, end < 0 ? text.length : end);
          index = end < 0 ? text.length : end + 1;
          if ((heredoc.strip ? line.replace(/^\t+/, "") : line).trim() === heredoc.delimiter) break;
          lines.push(line);
        }
        heredoc.token.heredoc.body = lines.join("\n");
      }
      // A newline after `|`, `&&` or `||` continues the same list (as comment-cut reads `|`-newline).
      const last = tokens.at(-1);
      if (!(last && "op" in last && ["|", "|&", "&&", "||", ";", "\n"].includes(last.op))) tokens.push({ op: "\n" });
      continue;
    }
    if (c === "#" && !word) { while (index < text.length && text[index] !== "\n") index += 1; continue; }
    if (c === "<" && text[index + 1] === "<" && text[index + 2] !== "<") {
      endWord();
      const strip = text[index + 2] === "-";
      index += strip ? 3 : 2;
      while (text[index] === " " || text[index] === "\t") index += 1;
      let raw = "";
      while (index < text.length && !/[\s;&|()<>]/.test(text[index]!)) raw += text[index++];
      const token = { heredoc: { body: "", quoted: /['"\\]/.test(raw) } };
      tokens.push(token);
      pending.push({ delimiter: raw.replace(/['"\\]/g, ""), strip, token });
      continue;
    }
    if ((c === "<" || c === ">") && text[index + 1] === "(") {
      const end = readBalanced(text, index + 2, "(", ")");
      const w = current();
      w.subs.push(text.slice(index + 2, end - 1));
      w.dynamic = true;
      index = end;
      continue;
    }
    if (c === "<" || c === ">" || (c === "&" && !word && text[index + 1] === ">")) {
      // review/astra F3 on #105: the `2` of `2>/dev/null pkill …` is a descriptor, not a word.
      if (word && !target && /^\d+$/.test(word.text)) word = undefined;
      endWord();
      while (index < text.length && /[<>&|]/.test(text[index]!)) index += 1;
      while (text[index] === " " || text[index] === "\t") index += 1;
      // review/astra F2 on #105: the target is not an argument, but its substitutions run.
      target = true;
      continue;
    }
    const operator = !word || c === ";" || c === "|" || c === "&" || c === ")" || c === "("
      ? OPERATORS.find((op) => text.startsWith(op, index))
      : undefined;
    if (operator && !(operator === "(" && word)) {
      endWord();
      tokens.push({ op: operator });
      index += operator.length;
      continue;
    }
    const w = current();
    if (c === "'") {
      const end = text.indexOf("'", index + 1);
      w.text += text.slice(index + 1, end < 0 ? text.length : end);
      index = end < 0 ? text.length : end + 1;
    } else if (c === "$" && text[index + 1] === "'") {
      const end = text.indexOf("'", index + 2);
      w.text += text.slice(index + 2, end < 0 ? text.length : end);
      index = end < 0 ? text.length : end + 1;
    } else if (c === "\"") {
      index = readDouble(text, index + 1, w);
    } else if ((c === "$" && text[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(text, start, "`", "`") : readBalanced(text, start, "(", ")");
      w.subs.push(text.slice(start, end - 1));
      w.text += "$";
      w.dynamic = true;
      index = end;
    } else if (c === "\\") {
      w.text += text[index + 1] ?? "";
      index += 2;
    } else {
      if (c === "$") w.dynamic = true;
      w.text += c;
      index += 1;
    }
  }
  endWord();
  return tokens;
}

// Words that run the next words as a command, with their options that take a separate value
// (review/astra F3 on #105: long forms too, as in `sudo --user paul pkill …`).
const PREFIXES: Record<string, string[]> = {
  sudo: ["-u", "-g", "-h", "-p", "-C", "-D", "-r", "-t", "-U", "-T", "--user", "--group", "--host", "--prompt",
    "--close-from", "--chdir", "--role", "--type", "--other-user", "--command-timeout"],
  doas: ["-u", "-C"],
  env: ["-u", "-C", "-S", "--unset", "--chdir", "--split-string"],
  timeout: ["-s", "-k", "--signal", "--kill-after"], nice: ["-n", "--adjustment"],
  ionice: ["-c", "-n", "-p", "-P", "-u", "--class", "--classdata", "--pid", "--pgid", "--uid"],
  nohup: [], setsid: [], time: ["-f", "-o", "--format", "--output"], command: [], exec: ["-a"], builtin: [],
  stdbuf: ["-i", "-o", "-e", "--input", "--output", "--error"],
  xargs: ["-I", "-n", "-P", "-L", "-s", "-d", "-E", "-a", "--max-args", "--max-procs", "--max-lines",
    "--max-chars", "--delimiter", "--eof", "--arg-file", "--process-slot-var"],
  "!": [], "{": [], then: [], do: [], else: [], if: [], elif: [], while: [], until: [],
};
const KILL_BY_NAME = new Set(["pkill", "killall", "killall5"]);
// Commands that pick processes by name: their output is not a PID the session recorded.
const LOOKUPS = new Set(["pgrep", "pidof", "ps", "grep", "egrep", "awk"]);
const SHELLS = new Set(["bash", "sh", "zsh", "dash", "ksh"]);
const SSH_VALUE_OPTIONS = new Set(["-b", "-c", "-D", "-E", "-e", "-F", "-I", "-i", "-J", "-L", "-l", "-m", "-O", "-o", "-p", "-Q", "-R", "-S", "-W", "-w", "-B"]);
// A kill argument that names no process by itself: a literal PID or group, a job, a signal.
const LITERAL = /^(?:-?\d+|%\S*|--|-[A-Za-z][A-Za-z0-9+-]*|-\d+)$/;
const VARIABLE = /\$\{?([A-Za-z_][A-Za-z0-9_]*)/g;

// `blocked`: a kill by pattern. `lookup`: runs a name lookup (LOOKUPS) anywhere.
type Verdict = { blocked: boolean; lookup: boolean };
type Command = { words: Word[]; redirects: Word[]; heredocs: Array<{ body: string; quoted: boolean }> };

/** The command words after assignments and wrappers (sudo, env, timeout, xargs, …), and whether xargs feeds them. */
function unwrap(stageWords: Word[], scripts: Array<{ text: string; source: Word }>): { words: Word[]; fedByXargs: boolean } {
  let words = stageWords;
  let fedByXargs = false;
  for (;;) {
    while (words[0] && /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[0].text)) words = words.slice(1);
    const prefix = words[0]?.text.split("/").pop() ?? "";
    const options = PREFIXES[prefix];
    if (!options) return { words, fedByXargs };
    // `command -v pkill` names the command; it does not run it.
    if (prefix === "command" && words[1] && /^-[a-zA-Z]*[vV]/.test(words[1].text)) return { words: [], fedByXargs };
    if (prefix === "xargs") fedByXargs = true;
    words = words.slice(1);
    while (words[0]?.text.startsWith("-") && words[0].text.length > 1) {
      const flag = words[0].text;
      if (flag === "--") { words = words.slice(1); break; }
      let value: string | undefined;
      let takes = false;
      // review/astra F7 on #105: remember that the option is env's split string (-S), whatever its spelling.
      let split = false;
      if (flag.startsWith("--")) {
        const [key, inline] = flag.split(/=(.*)/s);
        takes = inline === undefined && options.includes(key!);
        value = inline;
        split = key === "--split-string";
      } else {
        // review/astra F4 on #105: read a short cluster from its start. The first letter that takes a
        // value takes the rest of the word (`-uroot`), or the next word when nothing is left (`-Eu paul`).
        for (let at = 1; at < flag.length; at++) {
          if (!options.includes(`-${flag[at]}`)) continue;
          value = flag.slice(at + 1) || undefined;
          takes = value === undefined;
          split = flag[at] === "S";
          break;
        }
      }
      const source = takes ? words[1] : words[0];
      if (takes) value = source?.text;
      if (prefix === "env" && split && value && source) scripts.push({ text: value, source });
      words = words.slice(takes ? 2 : 1);
    }
    if (prefix === "timeout" && words[0]) words = words.slice(1);
  }
}

/**
 * Scans a shell script. `names` holds the variables that hold name-lookup output in the calling
 * script; `fed` is true when the calling script expanded a lookup into this script's text.
 */
function scan(script: string, depth: number, names: ReadonlySet<string> = new Set(), fed = false): Verdict {
  const verdict: Verdict = { blocked: false, lookup: false };
  if (depth > 6) return verdict;
  // Variables whose value comes from a name lookup (`P=$(pgrep …)`, `for p in $(pgrep …)`,
  // `pgrep … | while read p`). A `kill` of one is a kill by pattern; a variable of unknown origin
  // may be a PID the session recorded (`PID=$!`) and passes.
  const tainted = new Set(names);
  let stages: Command[] = [];
  let command: Command = { words: [], redirects: [], heredocs: [] };

  const nested = (text: string, feeds = false): Verdict => {
    const inner = scan(text, depth + 1, tainted, feeds);
    verdict.blocked ||= inner.blocked;
    verdict.lookup ||= inner.lookup;
    return inner;
  };
  // review/astra F1/F5 on #105: a script run by bash -c, eval or ssh gets the lookups the calling
  // shell expands into it (then any non-literal PID there is theirs, even quoted), and the
  // tainted variables it can read.
  const feedsLookup = (word: Word): boolean => fed || word.subs.some((sub) => scan(sub, depth + 1).lookup);

  const runPipeline = (): void => {
    let pipeFeed = false;
    stages.forEach((stage, position) => {
      const piped = position < stages.length - 1;
      // Substitutions in words and redirection targets run; so do those of an unquoted heredoc
      // body, where quotes and `#` are literal (review/astra F2 on #105).
      const subs = [...stage.words, ...stage.redirects].flatMap((word) => word.subs);
      for (const heredoc of stage.heredocs) if (!heredoc.quoted) subs.push(...heredocSubs(heredoc.body));
      let captured = false;
      for (const sub of subs) captured = nested(sub).lookup || captured;
      const scripts: Array<{ text: string; feeds: boolean }> = [];
      const envScripts: Array<{ text: string; source: Word }> = [];
      const { words, fedByXargs } = unwrap(stage.words, envScripts);
      // review/astra F6 on #105: an `env -S` script gets the lookups expanded into its word.
      for (const { text, source } of envScripts) scripts.push({ text, feeds: feedsLookup(source) });
      const name = words[0]?.text.split("/").pop() ?? "";
      const args = words.slice(1);
      let lookup = LOOKUPS.has(name);
      if (KILL_BY_NAME.has(name)) verdict.blocked = true;
      // Assignments and loop variables that take lookup output.
      for (const word of stage.words) {
        const assigned = /^([A-Za-z_][A-Za-z0-9_]*)=/.exec(word.text);
        if (assigned && word.subs.some((sub) => scan(sub, depth + 1).lookup)) tainted.add(assigned[1]!);
      }
      if (name === "for" && args[0] && captured) tainted.add(args[0].text);
      if (name === "read" && (pipeFeed || fed)) for (const arg of args) if (!arg.text.startsWith("-")) tainted.add(arg.text);
      if (name === "kill") {
        const probe = args.some((arg, i) => arg.text === "-0" || arg.text === "-l" || (arg.text === "-s" && args[i + 1]?.text === "0"));
        const byLookup = args.some((arg) => {
          if (LITERAL.test(arg.text) && !arg.dynamic) return false;
          // A PID file (`$(cat run.pid)`, `$(< run.pid)`) is a recorded PID; a lookup is not.
          if (arg.subs.some((sub) => scan(sub, depth + 1).lookup)) return true;
          if ([...arg.text.matchAll(VARIABLE)].some((match) => tainted.has(match[1]!))) return true;
          return fed;
        });
        if (!probe && (byLookup || (fedByXargs && pipeFeed))) verdict.blocked = true;
      }
      // review/astra F6 on #105: a heredoc script gets the lookups its unquoted body expands, even
      // inside quotes there.
      if (SHELLS.has(name) || name === "ssh") for (const heredoc of stage.heredocs) {
        const feeds = fed || (!heredoc.quoted && heredocSubs(heredoc.body).some((sub) => scan(sub, depth + 1).lookup));
        scripts.push({ text: heredoc.body, feeds });
      }
      if (SHELLS.has(name)) {
        const flag = args.findIndex((arg) => /^-[a-z]*c[a-z]*$/.test(arg.text));
        const payload = flag >= 0 ? args[flag + 1] : undefined;
        if (payload) scripts.push({ text: payload.text, feeds: feedsLookup(payload) });
      }
      if (name === "eval") scripts.push({ text: args.map((arg) => arg.text).join(" "), feeds: args.some(feedsLookup) });
      if (name === "ssh") {
        let i = 0;
        while (args[i]?.text.startsWith("-")) i += SSH_VALUE_OPTIONS.has(args[i]!.text) ? 2 : 1;
        const remote = args.slice(i + 1);
        scripts.push({ text: remote.map((arg) => arg.text).join(" "), feeds: remote.some(feedsLookup) });
      }
      for (const inner of scripts) lookup = nested(inner.text, inner.feeds).lookup || lookup;
      if (lookup) verdict.lookup = true;
      if (piped && lookup) pipeFeed = true;
    });
    stages = [];
  };

  for (const token of tokenize(script)) {
    if ("word" in token) { command.words.push(token.word); continue; }
    if ("redirect" in token) { command.redirects.push(token.redirect); continue; }
    if ("heredoc" in token) { command.heredocs.push(token.heredoc); continue; }
    if (command.words.length > 0 || command.redirects.length > 0 || command.heredocs.length > 0) stages.push(command);
    command = { words: [], redirects: [], heredocs: [] };
    if (token.op !== "|" && token.op !== "|&") runPipeline();
  }
  if (command.words.length > 0 || command.redirects.length > 0 || command.heredocs.length > 0) stages.push(command);
  runPipeline();
  return verdict;
}

/** True when the shell command kills processes by name pattern (smarty-dev#774). */
export function killsByPattern(command: string): boolean {
  return scan(command, 0).blocked;
}

export const PATTERN_KILL_REASON =
  "Blocked (smarty-dev#774): a kill by name pattern (pkill, killall, or kill of pgrep output) also kills " +
  "other owners' processes on a shared host. Start your job with `bin/smarty-reap run RECORD -- CMD` and " +
  "end it with `bin/smarty-reap stop RECORD` (only your own process group), or run `kill <PID>` with a " +
  "PID you started and recorded (for example from `$!`).";
