// smarty-dev#774: on a shared host a kill by name also kills other owners' processes. Three real
// incidents: `pkill -f '[w]atch'` on Dev1 (09-25), `pkill -f 'sleep 30' -P 1` over ssh on m4max
// (09-26: BSD pkill read `-P 1` as patterns), and `pkill -f "retry418.sh"` that matched its own
// shell (09-27). The rule was in every agent's context and did not stop them, so every bash call
// that kills by pattern is refused.
//
// Refused: `pkill` and `killall` in any form, and `kill` of PIDs that came from a name lookup
// (`pgrep`, `pidof`, or `ps` piped on): `kill $(pgrep …)`, `kill `pgrep …``, `pgrep … | xargs kill`,
// `P=$(pgrep …); kill $P`, `pgrep … | while read p; do kill $p; done`, also inside `ssh HOST '…'`,
// `bash -c '…'`, `eval` and a heredoc fed to a shell or ssh. Allowed: `kill <literal PIDs>`,
// `kill %job`, `kill -0`, `kill "$PID"` of a PID the session recorded, `bin/smarty-reap`, and
// `pgrep` alone. Quoted text is data: `echo "never use pkill"` and `grep -n pkill` pass.
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
      // review/astra F1 on #105: keep the substitution, so `bash -c "kill $(pgrep x)"` still shows it.
      word.text = (word.text ?? "") + text.slice(index, end);
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
      w.text += text.slice(index, end);
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
// A name lookup returns the PIDs of every owner's matching processes.
const LOOKUPS = new Set(["pgrep", "pidof"]);
const SEARCHES = new Set(["grep", "egrep", "awk"]);
const SHELLS = new Set(["bash", "sh", "zsh", "dash", "ksh"]);
const SSH_VALUE_OPTIONS = new Set(["-b", "-c", "-D", "-E", "-e", "-F", "-I", "-i", "-J", "-L", "-l", "-m", "-O", "-o", "-p", "-Q", "-R", "-S", "-W", "-w", "-B"]);

// `blocked`: a kill by pattern. `lookup`: runs pgrep or pidof. `search`: runs `ps … | grep`.
type Verdict = { blocked: boolean; lookup: boolean; search: boolean };
type Command = { words: Word[]; redirects: Word[]; heredocs: Array<{ body: string; quoted: boolean }> };

function scan(script: string, depth: number): Verdict {
  const verdict: Verdict = { blocked: false, lookup: false, search: false };
  if (depth > 6) return verdict;
  // A pgrep/pidof PID list captured or piped earlier in this script: a later `kill` of a
  // non-literal PID uses it (`P=$(pgrep x); kill $P`, `pgrep x | while read p; do kill $p; done`).
  // ponytail: `ps` is also how agents check a PID they recorded (`ps -o pgid= -p $PID`), so `ps`
  // counts only as `ps … | grep` fed straight into the kill (its arguments or the same pipeline).
  let tainted = false;
  let stages: Command[] = [];
  let command: Command = { words: [], redirects: [], heredocs: [] };

  const nested = (text: string): Verdict => {
    const inner = scan(text, depth + 1);
    verdict.blocked ||= inner.blocked;
    verdict.lookup ||= inner.lookup;
    return inner;
  };

  const runPipeline = (): void => {
    let pipeFeed = false;
    let sawPs = false;
    stages.forEach((stage, position) => {
      const piped = position < stages.length - 1;
      let captured = false;
      // Substitutions in words and redirection targets run; so do those of an unquoted heredoc
      // body, where quotes and `#` are literal (review/astra F2 on #105).
      const subs = [...stage.words, ...stage.redirects].flatMap((word) => word.subs);
      for (const heredoc of stage.heredocs) if (!heredoc.quoted) subs.push(...heredocSubs(heredoc.body));
      for (const sub of subs) captured = nested(sub).lookup || captured;
      const scripts: string[] = [];
      let words = stage.words;
      let fedByXargs = false;
      for (;;) {
        while (words[0] && /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[0].text)) words = words.slice(1);
        const prefix = words[0]?.text.split("/").pop() ?? "";
        const options = PREFIXES[prefix];
        if (!options) break;
        // `command -v pkill` names the command; it does not run it.
        if (prefix === "command" && words[1] && /^-[a-zA-Z]*[vV]/.test(words[1].text)) { words = []; break; }
        if (prefix === "xargs") fedByXargs = true;
        words = words.slice(1);
        while (words[0]?.text.startsWith("-") && words[0].text.length > 1) {
          const flag = words[0].text;
          if (flag === "--") { words = words.slice(1); break; }
          const [key, inline] = flag.split(/=(.*)/s);
          // A short cluster such as `-Eu paul` takes a value when its last letter does.
          const takes = inline === undefined && (options.includes(key!) || (!key!.startsWith("--") && options.includes(`-${key!.at(-1)}`)));
          const value = takes ? words[1]?.text : inline;
          if (prefix === "env" && (key === "-S" || key === "--split-string") && value) scripts.push(value);
          words = words.slice(takes ? 2 : 1);
        }
        if (prefix === "timeout" && words[0]) words = words.slice(1);
      }
      const name = words[0]?.text.split("/").pop() ?? "";
      const args = words.slice(1);
      let lookup = LOOKUPS.has(name);
      if (KILL_BY_NAME.has(name)) verdict.blocked = true;
      if (name === "kill") {
        const probe = args.some((arg, i) => arg.text === "-0" || (arg.text === "-s" && args[i + 1]?.text === "0"));
        const variable = fedByXargs || args.some((arg) => arg.dynamic);
        const ownFeed = args.some((arg) => arg.subs.some((sub) => { const inner = scan(sub, depth + 1); return inner.lookup || inner.search; }));
        if (!probe && variable && (tainted || pipeFeed || ownFeed)) verdict.blocked = true;
      }
      if (SHELLS.has(name) || name === "ssh") scripts.push(...stage.heredocs.map((heredoc) => heredoc.body));
      if (SHELLS.has(name)) {
        const flag = args.findIndex((arg) => /^-[a-z]*c[a-z]*$/.test(arg.text));
        if (flag >= 0 && args[flag + 1]) scripts.push(args[flag + 1]!.text);
      }
      if (name === "eval") scripts.push(args.map((arg) => arg.text).join(" "));
      if (name === "ssh") {
        let i = 0;
        while (args[i]?.text.startsWith("-")) i += SSH_VALUE_OPTIONS.has(args[i]!.text) ? 2 : 1;
        scripts.push(args.slice(i + 1).map((arg) => arg.text).join(" "));
      }
      for (const inner of scripts) lookup = nested(inner).lookup || lookup;
      if (lookup) verdict.lookup = true;
      if (name === "ps") sawPs = true;
      if (sawPs && SEARCHES.has(name)) verdict.search = true;
      if (captured || (lookup && piped)) tainted = true;
      if (piped && (lookup || (sawPs && SEARCHES.has(name)))) pipeFeed = true;
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
