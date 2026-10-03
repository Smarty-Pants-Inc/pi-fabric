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

// `names[i]` is the placeholder `${name}` that stands for `subs[i]` in `text`: a script run by
// bash -c, eval, ssh, env -S or a heredoc sees which operand came from which substitution.
// `pattern` is `text` with each quoted or escaped glob character (* ? [ ]) masked (GLOB_MASK), a
// double-quoted `$` as QUOTED and a single-quoted or escaped `$` as LITERAL, so the /tmp rule sees which
// globs and expansions are live (smarty-dev#1998, round 1 on PR #148).
type Word = { text: string; subs: string[]; names: string[]; dynamic: boolean; pattern: string; ansiEscaped?: boolean };
type Expansion = { subs: string[]; names: string[]; text?: string; dynamic?: boolean };

let placeholders = 0;
/** Records a substitution and returns the placeholder that stands for its output. */
function substitute(into: Expansion, sub: string): string {
  const name = `__pk_sub_${++placeholders}`;
  into.subs.push(sub);
  into.names.push(name);
  into.dynamic = true;
  return `\${${name}}`;
}
type Redirect = { redirect: Word; input: boolean; output: boolean };
type Token = { op: string } | { word: Word } | Redirect | { heredoc: { body: string; quoted: boolean } };

const OPERATORS = ["&&", "||", ";;", "|&", "|", "&", ";", "(", ")"];

/** Reads to the character that closes `open` at `index` (just after the opener), nesting quotes. */
function readBalanced(text: string, index: number, open: string, close: string): number {
  let depth = 1;
  while (index < text.length) {
    const c = text[index]!;
    if (c === "\\") { index += 2; continue; }
    if (c === "'" && close !== "`") { const end = text.indexOf("'", index + 1); index = end < 0 ? text.length : end + 1; continue; }
    if (c === "\"" && close !== "`") { index = readDouble(text, index + 1, { subs: [], names: [] }); continue; }
    if (c === close) { depth -= 1; index += 1; if (depth === 0) return index; continue; }
    if (open !== close && c === open) depth += 1;
    index += 1;
  }
  return index;
}

/** Reads a double-quoted body from `index`; collects its `$(…)` and backtick substitutions. */
function readDouble(text: string, index: number, word: Expansion): number {
  while (index < text.length && text[index] !== "\"") {
    const c = text[index]!;
    if (c === "\\") { word.text = (word.text ?? "") + (text[index + 1] ?? ""); index += 2; continue; }
    if ((c === "$" && text[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(text, start, "`", "`") : readBalanced(text, start, "(", ")");
      word.text = (word.text ?? "") + substitute(word, text.slice(start, end - 1));
      index = end;
      continue;
    }
    if (c === "$") word.dynamic = true;
    word.text = (word.text ?? "") + c;
    index += 1;
  }
  return index + 1;
}

/**
 * An unquoted heredoc body as the command that reads it receives it: each `$(…)` and backtick
 * substitution replaced by its placeholder. Quotes and `#` are literal there.
 */
function expandHeredoc(body: string): Expansion & { text: string } {
  const expansion: Expansion & { text: string } = { subs: [], names: [], text: "" };
  for (let index = 0; index < body.length;) {
    const c = body[index]!;
    // In a heredoc a backslash escapes only $, ` and \: the reader gets `\$(…)` as `$(…)`.
    if (c === "\\") { expansion.text += /[$`\\]/.test(body[index + 1] ?? "") ? body[index + 1] : body.slice(index, index + 2); index += 2; continue; }
    if ((c === "$" && body[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(body, start, "`", "`") : readBalanced(body, start, "(", ")");
      expansion.text += substitute(expansion, body.slice(start, end - 1));
      index = end;
      continue;
    }
    expansion.text += c;
    index += 1;
  }
  return expansion;
}

function tokenize(source: string): Token[] {
  // As in comment-cut (#104): a backslash-newline continues the line.
  const text = source.replace(/\\\r?\n/g, " ");
  const tokens: Token[] = [];
  const pending: Array<{ delimiter: string; strip: boolean; token: { heredoc: { body: string; quoted: boolean } } }> = [];
  let index = 0;
  let word: Word | undefined;
  let target = false;
  let input = false;
  let output = false;
  let duplicate = false;
  const endWord = (): void => {
    if (word) tokens.push(target ? { redirect: word, input,
      output: output && (!duplicate || (word.text !== "-" && !/^\d+$/.test(word.text))) } : { word });
    word = undefined;
    target = false;
  };
  const current = (): Word => (word ??= { text: "", subs: [], names: [], dynamic: false, pattern: "" });
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
      const placeholder = substitute(w, text.slice(index + 2, end - 1));
      w.text += placeholder;
      w.pattern += placeholder;
      index = end;
      continue;
    }
    if (c === "<" || c === ">" || (c === "&" && !word && text[index + 1] === ">")) {
      // review/astra F3 on #105: the `2` of `2>/dev/null pkill …` is a descriptor, not a word.
      const descriptor = word && !target && /^\d+$/.test(word.text) ? word.text : undefined;
      if (descriptor !== undefined) word = undefined;
      endWord();
      // Only an explicit stdin source overrides feed. Output and descriptor duplication do not.
      input = c === "<" && (descriptor === undefined || descriptor === "0") && text[index + 1] !== "&";
      // S4: distinguish a file write from fd duplication/closure and non-stdin inputs.
      output = c === ">" || c === "&";
      duplicate = c === ">" && text[index + 1] === "&";
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
    const before = w.text.length;
    let bare = false;
    const kind = c === "\"" ? QUOTED : (c === "$" && text[index + 1] === "(") || c === "`" ? "$" : LITERAL;
    if (c === "'") {
      const end = text.indexOf("'", index + 1);
      w.text += text.slice(index + 1, end < 0 ? text.length : end);
      index = end < 0 ? text.length : end + 1;
    } else if (c === "$" && text[index + 1] === "'") {
      const start = index + 2;
      let end = start;
      while (end < text.length && text[end] !== "'") {
        if (text[end] === "\\") { w.ansiEscaped = true; end += 2; }
        else end += 1;
      }
      w.text += text.slice(start, end);
      index = end < text.length ? end + 1 : end;
    } else if (c === "\"") {
      index = readDouble(text, index + 1, w);
    } else if ((c === "$" && text[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(text, start, "`", "`") : readBalanced(text, start, "(", ")");
      w.text += substitute(w, text.slice(start, end - 1));
      index = end;
    } else if (c === "\\") {
      w.text += text[index + 1] ?? "";
      index += 2;
    } else {
      if (c === "$") w.dynamic = true;
      w.text += c;
      index += 1;
      bare = true;
    }
    const added = w.text.slice(before);
    w.pattern += bare ? added : mask(added).replaceAll("$", kind);
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
const VARIABLE = /\$\{?([A-Za-z_][A-Za-z0-9_]*|[0-9@*])/g;

// smarty-dev#1998: deletes whose operand may be another agent's /tmp entry.
const DELETERS = new Set(["rm", "unlink", "shred"]);
// These receivers do not consume stdin; all other unsafe-fed captures need an explicit file.
const CAPTURE_NON_READERS = new Set(["", "echo", "printf", ":", "true", "false", "kill", "mktemp"]);
const TMP_ROOTS = [["tmp"], ["var", "tmp"], ["private", "tmp"], ["private", "var", "tmp"]];
// A quoted glob character maps to a private-use character, so a variable's literal value can restore it.
const GLOBS = "*?[]";
const GLOB_MASK = "\uE000\uE001\uE002\uE003";
const mask = (text: string): string => text.replace(/[*?[\]]/g, (c) => GLOB_MASK[GLOBS.indexOf(c)]!);
const unmask = (text: string): string => text.replace(/[\uE000-\uE003]/g, (c) => GLOBS[GLOB_MASK.indexOf(c)]!);
const QUOTED = "\u0002";
const LITERAL = "\u0003";
// A `$NAME` (live) or quoted `"$NAME"` reference in a pattern.
const REFERENCE = /([$\u0002])\{?([A-Za-z_][A-Za-z0-9_]*|[0-9@*])\}?/g;
const MENTIONS_TMP = /(^|[^\w.-])\/(private\/)?(var\/)?tmp(\/|\b)/;
// Security S3 on PR #148: a longer assignment value is unknown, so a doubling chain stays linear.
const MAX_VALUE = 4096;

// Local nested scripts (sh -c, eval, substitutions) inherit the cwd and variables; ssh gets neither.
// `owned` holds the placeholders of `mktemp` substitutions; `root` is the whole tool-call command.
type Context = {
  root: string; owned: Set<string>; cwd?: string | undefined; values: ReadonlyMap<string, string>;
  unknown: ReadonlySet<string>; unsafeFiles: Set<string>; capture?: boolean | undefined;
};

/**
 * True when `pattern` (a word's pattern after variable expansion) names /tmp or /var/tmp itself, or has an
 * unquoted glob (or ..) in the component directly below it: `/tmp/tmp.*`, `/tmp/*`, `/tmp`. A glob below a
 * concrete component (`/tmp/tmp.AbC123/*.md`) stays inside one dir. A relative word counts only after a known `cd`.
 */
function tmpGlob(pattern: string, cwd: string | undefined): boolean {
  const path = pattern.startsWith("/") ? pattern : cwd && pattern && !/^[~$]/.test(pattern) ? `${cwd}/${pattern}` : undefined;
  if (!path) return false;
  const parts = path.split("/").filter((part) => part !== "" && part !== ".");
  const root = TMP_ROOTS.find((r) => r.every((part, i) => parts[i] === part));
  if (!root) return false;
  const rest = parts.slice(root.length);
  return rest.length === 0 || /[*?[]/.test(rest[0]!) || rest.includes("..");
}

// `blocked`: a kill by pattern. `lookup`: runs a name lookup (LOOKUPS) anywhere.
// `wipe`: a delete over a /tmp glob (smarty-dev#1998). `tmpList`: a stage or substitution may list other agents' dirs.
type Verdict = { blocked: boolean; lookup: boolean; wipe: boolean; tmpList: boolean; sourceKnown?: boolean; uncertain?: boolean };
type Feed = { lookup: boolean; tmp: boolean };
type Command = { words: Word[]; redirects: Redirect[]; heredocs: Array<{ body: string; quoted: boolean }>; closed?: number };
type InputScope = { target: Word; start?: Word };
type SourceScopes = {
  inputs: Map<Word, InputScope>; members: Map<Word, number>; ends: Map<Token, number>; parents: Map<number, number>;
};

/** Match late stdin redirects to their loop/group, never to unrelated reads in the script. */
function sourceScopes(tokens: Token[]): SourceScopes {
  const stack: Array<{ kind: string; start: number }> = [];
  const scopes = new Map<number, { end: number; source: InputScope }>();
  const compounds = new Map<number, number>();
  const ends = new Map<Token, number>();
  const parents = new Map<number, number>();
  let head = true;
  const close = (kind: string, end: number): void => {
    if (stack.at(-1)?.kind !== kind) return;
    const scope = stack.pop()!;
    compounds.set(scope.start, end);
    ends.set(tokens[end]!, scope.start);
    const parent = stack.at(-1);
    if (parent) parents.set(scope.start, parent.start);
    let target: Word | undefined;
    for (let i = end + 1; i < tokens.length; i++) {
      const token = tokens[i]!;
      if ("redirect" in token) { if (token.input) target = token.redirect; }
      else if (!("heredoc" in token)) break;
    }
    const first = tokens[scope.start]!;
    if (target) scopes.set(scope.start, { end, source: { target, ...("word" in first ? { start: first.word } : {}) } });
  };
  tokens.forEach((token, i) => {
    if ("op" in token) {
      if (token.op === "(") stack.push({ kind: "group", start: i });
      if (token.op === ")") close("group", i);
      head = true;
    } else if ("word" in token && head) {
      const name = token.word.text;
      if (["for", "while", "until", "select"].includes(name)) stack.push({ kind: "loop", start: i });
      if (name === "{") stack.push({ kind: "brace", start: i });
      if (name === "done") close("loop", i);
      if (name === "}") close("brace", i);
      head = ["{", "do", "then", "else", "if", "elif", "!"].includes(name);
    }
  });
  const inputs = new Map<Word, InputScope>();
  const members = new Map<Word, number>();
  const active: Array<{ end: number; source: InputScope }> = [];
  const groups: Array<{ start: number; end: number }> = [];
  tokens.forEach((token, i) => {
    const scope = scopes.get(i);
    if (scope) active.push(scope);
    const end = compounds.get(i);
    if (end !== undefined) groups.push({ start: i, end });
    const current = active.at(-1);
    const group = groups.at(-1);
    if (current && i < current.end && "word" in token) inputs.set(token.word, current.source);
    if (group && i < group.end && "word" in token) members.set(token.word, group.start);
    if (current?.end === i) active.pop();
    if (group?.end === i) groups.pop();
  });
  return { inputs, members, ends, parents };
}

/** The command words after assignments and wrappers (sudo, env, timeout, xargs, …), and whether xargs feeds them. */
function unwrap(stageWords: Word[], scripts: Array<{ text: string; source: Word }>): { words: Word[]; fedByXargs: boolean; argFile: Word | undefined; uncertain: boolean } {
  let words = stageWords;
  let fedByXargs = false;
  let argFile: Word | undefined;
  let uncertain = false;
  for (;;) {
    while (words[0] && /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[0].text)) {
      // An undecoded assignment can later supply a protected path or selector.
      uncertain ||= words[0].ansiEscaped === true;
      words = words.slice(1);
    }
    // #325 S6: check every receiver, including wrappers, before raw basename lookup.
    if (words[0]?.ansiEscaped) return { words, fedByXargs, argFile, uncertain: true };
    const prefix = words[0]?.text.split("/").pop() ?? "";
    if (prefix === "coproc") {
      words = words.slice(words[2]?.text === "{" ? 2 : 1);
      continue;
    }
    const options = PREFIXES[prefix];
    if (!options) return { words, fedByXargs, argFile, uncertain };
    // `command -v pkill` names the command; it does not run it.
    if (prefix === "command" && words[1] && /^-[a-zA-Z]*[vV]/.test(words[1].text)) return { words: [], fedByXargs, argFile, uncertain };
    if (prefix === "xargs") fedByXargs = true;
    words = words.slice(1);
    while (words[0]?.text.startsWith("-") && words[0].text.length > 1) {
      const flag = words[0].text;
      if (flag === "--") { words = words.slice(1); break; }
      let value: string | undefined;
      let takes = false;
      // review/astra F7 on #105: remember that the option is env's split string (-S), whatever its spelling.
      let split = false;
      let file = false;
      if (flag.startsWith("--")) {
        const [key, inline] = flag.split(/=(.*)/s);
        takes = inline === undefined && options.includes(key!);
        value = inline;
        split = key === "--split-string";
        file = key === "--arg-file";
      } else {
        // review/astra F4 on #105: read a short cluster from its start. The first letter that takes a
        // value takes the rest of the word (`-uroot`), or the next word when nothing is left (`-Eu paul`).
        for (let at = 1; at < flag.length; at++) {
          if (!options.includes(`-${flag[at]}`)) continue;
          value = flag.slice(at + 1) || undefined;
          takes = value === undefined;
          split = flag[at] === "S";
          file = flag[at] === "a";
          break;
        }
      }
      const source = takes ? words[1] : words[0];
      uncertain ||= words[0].ansiEscaped === true || source?.ansiEscaped === true;
      if (takes) value = source?.text;
      if (prefix === "xargs" && file && value !== undefined && source) {
        argFile = { ...source, text: value, pattern: source.pattern.slice(source.text.length - value.length) };
      }
      if (prefix === "env" && split && value && source) scripts.push({ text: value, source });
      words = words.slice(takes ? 2 : 1);
    }
    if (prefix === "timeout" && words[0]) words = words.slice(1);
  }
}

/** Native argv that the receiver executes or expands again, not ordinary field text. */
function executionWords(name: string, args: Word[]): Word[] {
  if (name === "trap") {
    if (["-p", "-l"].includes(args[0]?.text ?? "")) return [];
    const action = args[args[0]?.text === "--" ? 1 : 0];
    return action && action.text !== "-" ? [action] : [];
  }
  if (name === "printf") {
    const flag = args[0];
    if (!flag?.text.startsWith("-v")) return [];
    const destination = flag.text.length > 2 ? { ...flag, text: flag.text.slice(2) } : args[1];
    return destination && destination.text.includes("[") ? [destination] : [];
  }
  if (!["rg", "complete", "compgen"].includes(name)) return [];
  const actions: Word[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg.text === "--") break;
    if (name === "rg") {
      const match = /^--(?:pre|hostname-bin)(?:=(.*))?$/s.exec(arg.text);
      if (!match) continue;
      const action = match[1] === undefined ? args[++i] : { ...arg, text: match[1] };
      if (action) actions.push(action);
    } else if (name === "complete" || name === "compgen") {
      if (!arg.text.startsWith("-")) continue;
      for (let at = 1; at < arg.text.length; at++) {
        const option = arg.text[at]!;
        if (!"oACEFGPSWX".includes(option)) continue;
        const action = at + 1 < arg.text.length ? { ...arg, text: arg.text.slice(at + 1) } : args[++i];
        if (action && "CFW".includes(option)) actions.push(action);
        break;
      }
    }
  }
  return actions;
}

/** Bash read/mapfile option values are not destinations; omitted destinations use shell defaults. */
function readDestinations(name: string, args: Word[]): string[] {
  const read = name === "read";
  const valueOptions = read ? "adinNptu" : "nOsuCcd";
  let array: string | undefined;
  let index = 0;
  while (index < args.length) {
    const flag = args[index]!.text;
    if (flag === "--") { index += 1; break; }
    if (!flag.startsWith("-") || flag.length === 1) break;
    index += 1;
    for (let at = 1; at < flag.length; at++) {
      if (!valueOptions.includes(flag[at]!)) continue;
      const value = flag.slice(at + 1) || args[index++]?.text;
      if (value === undefined) return [];
      if (read && flag[at] === "a") array = value;
      break;
    }
  }
  const destinations = read && array !== undefined ? [array] : args.slice(index).map((arg) => arg.text);
  if (destinations.length === 0) destinations.push(read ? "REPLY" : "MAPFILE");
  // read -a ignores scalar names; mapfile takes only one array name. Array elements taint the base.
  return (read ? destinations : destinations.slice(0, 1)).flatMap((destination) => {
    const match = /^([A-Za-z_][A-Za-z0-9_]*)(?:\[.*\])?$/.exec(destination);
    return match ? [match[1]!] : [];
  });
}

/**
 * Scans a shell script. `names` holds the variables and placeholders whose value comes from a name
 * lookup in the calling script (review/astra F8 on #105: per operand, never the whole script).
 */
function scan(script: string, depth: number, names: ReadonlySet<string> = new Set(), tmpIn: ReadonlySet<string> = new Set(),
  context: Context = { root: script, owned: new Set(), values: new Map(), unknown: new Set(), unsafeFiles: new Set() }, stdin?: Feed): Verdict {
  // ponytail (#325 S3): do not certify nonempty execution text that the depth bound leaves
  // unexamined. Even a printing-only tail is refused; empty tails have nothing left to execute.
  if (depth > 6) return { blocked: false, lookup: false, wipe: false, tmpList: false, uncertain: script.trim().length > 0 };
  // Security F4/F5: collect with the state at each command, then replay from the same entry state.
  // F6: retain fallback for unresolved feeds, but late redirects belong only to their own scope.
  const tokens = tokenize(script);
  const scopes = sourceScopes(tokens);
  const sources = new Map<Word, Feed | undefined>();
  const first = scanPass(tokens, scopes, sources, depth, names, tmpIn, context, { lookup: false, tmp: false }, stdin);
  if (!first.lookup && !first.tmpList) return first;
  const second = scanPass(tokens, scopes, sources, depth, names, tmpIn, context, { lookup: first.lookup, tmp: first.tmpList }, stdin);
  return {
    blocked: first.blocked || second.blocked, lookup: first.lookup || second.lookup,
    wipe: first.wipe || second.wipe, tmpList: first.tmpList || second.tmpList,
    sourceKnown: first.sourceKnown === true && second.sourceKnown === true,
    uncertain: first.uncertain === true || second.uncertain === true,
  };
}

function scanPass(tokens: Token[], scopes: SourceScopes, sources: Map<Word, Feed | undefined>,
  depth: number, names: ReadonlySet<string>, tmpIn: ReadonlySet<string>, context: Context, fed: Feed, stdin?: Feed): Verdict {
  const verdict: Verdict = { blocked: false, lookup: false, wipe: false, tmpList: false };
  // Variables whose value comes from a name lookup (`P=$(pgrep …)`, `for p in $(pgrep …)`,
  // `pgrep … | while read p`), and the placeholders of lookup substitutions. A `kill` of one is a
  // kill by pattern; a variable of unknown origin may be a PID the session recorded (`PID=$!`).
  const tainted = new Set(names);
  let stages: Command[] = [];
  let command: Command = { words: [], redirects: [], heredocs: [] };

  // smarty-dev#1998: variables and placeholders that hold a /tmp glob's matches, the literal values of
  // earlier assignments (quoted globs restored), variables whose value is a non-mktemp substitution,
  // and the directory of the last `cd` (undefined when unknown).
  const tmpNames = new Set(tmpIn);
  const values = new Map(context.values);
  const unknown = new Set(context.unknown);
  let cwd = context.cwd;

  const nested = (text: string, extra: Iterable<string> = [], tmpExtra: Iterable<string> = [], local = true, input?: Feed, capture = context.capture): Verdict => {
    const inner = scan(text, depth + 1, new Set([...tainted, ...extra]), new Set([...tmpNames, ...tmpExtra]),
      local ? { ...context, cwd, values, unknown, capture } : { ...context, cwd: undefined, values: new Map(), unknown: new Set(), unsafeFiles: new Set(), capture },
      local ? input : undefined);
    verdict.blocked ||= inner.blocked;
    verdict.lookup ||= inner.lookup;
    verdict.wipe ||= inner.wipe;
    verdict.uncertain ||= inner.uncertain === true;
    return inner;
  };
  const fromLookup = (text: string): boolean => [...text.matchAll(VARIABLE)].some((match) => tainted.has(match[1]!));
  // An unquoted `$P` expands with its value's globs live; a quoted `"$P"` is one literal name.
  const expand = (pattern: string): string =>
    pattern.replace(REFERENCE, (whole, how: string, name: string) => {
      const value = values.get(name);
      return value === undefined ? whole : how === "$" ? value : mask(value);
    });
  const tmpOperand = (pattern: string): boolean =>
    [...pattern.matchAll(REFERENCE)].some((match) => tmpNames.has(match[2]!)) || tmpGlob(expand(pattern), cwd);
  // Round 1 on PR #148: an unquoted expansion of an unknown value, in a command that names /tmp.
  const unknownOperand = (pattern: string): boolean =>
    [...pattern.matchAll(REFERENCE)].some((match) => match[1] === "$" && unknown.has(match[2]!)) && MENTIONS_TMP.test(context.root);

  // #325 F7: initial environment bindings are unknown. A live expansion followed by a
  // parent component can escape even a mktemp-owned path; inspect before resolving it.
  const expandedParent = (pattern: string): boolean =>
    [...pattern.matchAll(REFERENCE)].some((match) => /\/\.\.(\/|$)/.test(pattern.slice(match.index! + match[0].length)));

  // A known substitution output is a feed, not a known literal path for a later find root.
  const knownSubs = new Set<string>();
  const concrete = (word: Word): boolean => ![...expand(word.pattern).matchAll(REFERENCE)]
    .some((match) => !context.owned.has(match[2]!));
  const lateTargets = new Set([...scopes.inputs.values()].map((scope) => scope.target));
  const compoundKnown = new Map<number, boolean>();
  const compoundFeeds = new Map<number, Feed>();
  const recordOutput = (scope: number, known: boolean, feed?: Feed): void => {
    compoundKnown.set(scope, (compoundKnown.get(scope) ?? true) && known);
    if (feed) {
      const previous = compoundFeeds.get(scope);
      compoundFeeds.set(scope, { lookup: feed.lookup || (previous?.lookup ?? false), tmp: feed.tmp || (previous?.tmp ?? false) });
    }
  };
  // #325 S4: a concrete same-call unsafe output is not a preexisting recorded source.
  // Keep facts monotonically through collection/replay and local inline scripts; do not
  // attempt to certify overwrites, filesystem aliases, or shell state transitions.
  const fileKey = (word: Word): string | undefined => {
    const path = expand(word.pattern);
    if (word.ansiEscaped || [...path.matchAll(REFERENCE)].length > 0 || /[*?[]/.test(path)) return undefined;
    const literal = unmask(path).replaceAll(LITERAL, "$");
    const absolute = literal.startsWith("/") ? literal : cwd ? `${cwd}/${literal}` : literal;
    return (absolute.startsWith("/") ? "/" : "") + absolute.split("/").filter((part) => part !== "." && part !== "").join("/");
  };
  const inputSource = (word: Word): Feed | undefined => {
    const key = fileKey(word);
    verdict.uncertain ||= word.ansiEscaped === true || (key !== undefined && context.unsafeFiles.has(key));
    const lookup = fromLookup(word.text);
    const tmp = tmpOperand(word.pattern);
    const known = word.text !== "-" && ![...expand(word.pattern).matchAll(REFERENCE)]
      .some((match) => !context.owned.has(match[2]!) && !knownSubs.has(match[2]!));
    return lookup || tmp ? { lookup, tmp } : known ? { lookup: false, tmp: false } : undefined;
  };

  const runPipeline = (): void => {
    // An inherited fd is separate from replay's whole-script fallback. Only real stdin,
    // not unrelated whole-call lookup/listing fallback, enters a captured substitution.
    let actual = stdin;
    let pipeFeed = stdin?.lookup ?? fed.lookup;
    let pipeTmp = stdin?.tmp ?? fed.tmp;
    stages.forEach((stage, position) => {
      const piped = position < stages.length - 1;
      // An actual incoming pipe exists even when its unresolved producer is represented only
      // by replay fallback. Known owned/recorded output sets an explicit empty feed instead.
      if (position > 0 && actual === undefined) actual = { lookup: pipeFeed, tmp: pipeTmp };
      // `do while read` / `then while read` can open a more specific scope after the first word.
      let scope: InputScope | undefined;
      for (const word of stage.words) scope = scopes.inputs.get(word) ?? scope;
      if (scope && (position === 0 || (scope.start && stage.words.includes(scope.start)))) {
        // Defer this body's stdin in the collection pass; its late source is evaluated in place.
        const source = sources.has(scope.target) ? sources.get(scope.target) : { lookup: false, tmp: false };
        const input = source ?? { lookup: pipeFeed || fed.lookup, tmp: pipeTmp || fed.tmp };
        if (source !== undefined) actual = source;
        pipeFeed = input.lookup;
        pipeTmp = input.tmp;
      }
      // Substitutions in words and redirection targets run; so do those of an unquoted heredoc
      // body, where quotes and `#` are literal (review/astra F2 on #105). A lookup's placeholder
      // is tainted, so a script that receives its output knows which operand holds it.
      const heredocs = stage.heredocs.map((heredoc) => heredoc.quoted ? { text: heredoc.body, subs: [], names: [] } : expandHeredoc(heredoc.body));
      const expansions: Expansion[] = [...stage.words, ...stage.redirects.map((redirect) => redirect.redirect), ...heredocs];
      let captured = false;
      for (const expansion of expansions) expansion.subs.forEach((sub, k) => {
        // #325 S5: substitutions inherit effective stdin. Unproved consuming captures
        // below refuse rather than inventing a recorded PID or owned-path source.
        const inner = nested(sub, [], [], true, actual, true);
        if (/^\s*mktemp(\s|$)/.test(sub)) context.owned.add(expansion.names[k]!);
        const known = inner.sourceKnown && !inner.tmpList && !inner.lookup;
        if (known) knownSubs.add(expansion.names[k]!);
        // Security F3: unresolved captured output after `cd /tmp` may list /tmp; explicit sources do not.
        if (inner.tmpList || (tmpGlob(".", cwd) && !known && !/^\s*mktemp(\s|$)/.test(sub))) {
          tmpNames.add(expansion.names[k]!);
          // A redirect on `done` has no consumer in this pass; retain its feed for the replay.
          verdict.tmpList = true;
        }
        if (!inner.lookup) return;
        captured = true;
        tainted.add(expansion.names[k]!);
      });
      const scripts: Array<{ text: string; extra?: string[]; tmpExtra?: string[]; remote?: boolean; heredoc?: boolean; uncertain?: boolean }> = [];
      const envScripts: Array<{ text: string; source: Word }> = [];
      const { words, fedByXargs, argFile, uncertain } = unwrap(stage.words, envScripts);
      verdict.uncertain ||= uncertain;
      // review/astra F6 on #105: an `env -S` script, with the placeholders of its word.
      for (const { text, source } of envScripts) scripts.push({ text, uncertain: source.ansiEscaped === true });
      const name = words[0]?.text.split("/").pop() ?? "";
      const args = words.slice(1);
      // #325 S6: undecoded argv at a protected boundary is not a proven operand,
      // selector, or option. Ordinary echo/printf/grep field text remains DATA.
      if (name === "kill" || KILL_BY_NAME.has(name) || DELETERS.has(name) || name === "find" ||
        SHELLS.has(name) || name === "eval" || name === "ssh") {
        verdict.uncertain ||= args.some((arg) => arg.ansiEscaped);
      }
      // #325 S1: native actions/selectors are execution boundaries, not quoted DATA.
      for (const action of executionWords(name, args)) scripts.push({ text: action.text, uncertain: action.ansiEscaped === true });
      let lookup = LOOKUPS.has(name);
      if (KILL_BY_NAME.has(name)) verdict.blocked = true;
      // Assignments and loop variables that take lookup output.
      for (const [i, word] of stage.words.entries()) {
        const assigned = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/s.exec(word.text);
        if (assigned && fromLookup(assigned[2]!)) tainted.add(assigned[1]!);
        // Security F2: `NAME=(…)` globs its elements at once; the words after it are its elements.
        const array = /^([A-Za-z_][A-Za-z0-9_]*)=\(/.exec(word.pattern);
        if (array) {
          const elements = [word.pattern.slice(array[0].length), ...stage.words.slice(i + 1).map((w) => w.pattern)];
          if (elements.some(tmpOperand)) tmpNames.add(array[1]!);
          values.delete(array[1]!);
          break;
        }
        const value = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/s.exec(word.pattern);
        if (value) {
          const [, variable, raw] = value as unknown as [string, string, string];
          if (tmpOperand(raw)) tmpNames.add(variable);
          // An assignment does not glob: its quotes protect only the assignment, so keep the literal value.
          const literal = unmask(expand(raw.replaceAll(QUOTED, "$")));
          const subs = [...literal.matchAll(/\$\{(__pk_sub_\d+)\}/g)].map((match) => match[1]!);
          if (literal.length > MAX_VALUE || subs.some((sub) => !context.owned.has(sub))) unknown.add(variable);
          else unknown.delete(variable);
          if (literal.length > MAX_VALUE) values.delete(variable);
          else values.set(variable, literal);
        }
      }
      if (name === "for" && args[0] && args.slice(2).some((arg) => fromLookup(arg.text))) tainted.add(args[0].text);
      if (name === "for" && args[0] && args.slice(2).some((arg) => tmpOperand(arg.pattern))) tmpNames.add(args[0].text);
      // A closing group's redirect controls its body's stdin, not the group's output pipeline.
      const closing = stage.closed !== undefined || name === "done" || name === "}" || (stage.words.length === 0 &&
        stage.redirects.some((redirect) => lateTargets.has(redirect.redirect)));
      const unresolved = { lookup: pipeFeed || fed.lookup, tmp: pipeTmp || fed.tmp };
      for (const redirect of stage.redirects) {
        if (!redirect.input) continue;
        const word = redirect.redirect;
        // A tainted substitution is a real feed; a concrete file is an independent recorded source.
        // Unresolved substitutions/paths retain conservative fallback instead of inventing ownership.
        const source = inputSource(word);
        sources.set(word, source);
        verdict.lookup ||= source?.lookup ?? false;
        verdict.tmpList ||= source?.tmp ?? false;
        if (!closing) {
          const input = source ?? unresolved;
          if (source !== undefined) actual = source;
          pipeFeed = input.lookup;
          pipeTmp = input.tmp;
        }
      }
      // xargs' argument file is not its child's stdin; without -a xargs disconnects that fd.
      const receiverStdin = fedByXargs && !argFile ? { lookup: false, tmp: false } : actual;
      if (argFile) {
        const source = inputSource(argFile) ?? unresolved;
        pipeFeed = source.lookup;
        pipeTmp = source.tmp;
      }
      const reads = name === "read" || name === "mapfile" || name === "readarray";
      if (reads) for (const destination of readDestinations(name, args)) {
        if (pipeFeed) tainted.add(destination);
        if (pipeTmp) tmpNames.add(destination);
      }
      // mktemp creates one exact path; naming /tmp there does not list other agents' entries.
      // Security F3: after `cd /tmp`, a piped stage may list the cwd (`ls`, `find` without a root).
      const roots: Word[] = [];
      if (name === "find") for (const arg of args) {
        if (["-H", "-L", "-P"].includes(arg.text)) continue;
        if (arg.text.startsWith("-") || ["(", "!", ")"].includes(arg.text)) break;
        roots.push(arg);
      }
      const files = args.filter((arg) => !arg.text.startsWith("-"));
      // Only ls's value-free flags are understood here: `ls -I pattern` still lists the cwd.
      const simpleLs = args.every((arg) => !arg.text.startsWith("-") || arg.text === "--" || /^-[aAdFlLrRtU1]+$/.test(arg.text));
      const plainFind = !args.some((arg) => ["-exec", "-execdir", "-ok", "-okdir", "-printf", "-fprintf"].includes(arg.text));
      const explicit = name === "find" && plainFind ? roots : name === "cat" || (name === "ls" && simpleLs) ? files : [];
      for (const word of explicit) inputSource(word);
      const independent = !fedByXargs && !captured && !args.some((arg) => arg.text === "-") && explicit.length > 0 && explicit.every((word) =>
        word.text !== "-" && concrete(word) && !fromLookup(word.text) && !tmpOperand(word.pattern));
      // #325 S5: only explicit independent files and known non-consuming commands
      // prove that a capture cannot read inherited unsafe stdin. Refuse the rest,
      // including cat without a file, read/mapfile, and unmodelled stream filters.
      if (context.capture && (actual?.lookup || actual?.tmp) && !independent &&
        !CAPTURE_NON_READERS.has(name)) verdict.uncertain = true;
      let listsTmp = name !== "mktemp" && (stage.words.some((word) => tmpOperand(word.pattern)) ||
        ((piped || stage.redirects.some((redirect) => redirect.output)) && !independent && tmpGlob(".", cwd)));
      // An explicit producer does not read stdin. Do not clear an earlier real pipeline stage.
      if (position === 0 && piped && independent && !listsTmp && !lookup) {
        pipeFeed = false; pipeTmp = false; actual = { lookup: false, tmp: false };
      }
      // Security F1: `xargs -a <(ls /tmp/…)` reads its input from a /tmp listing.
      const xargsTmp = fedByXargs && (pipeTmp || stage.words.some((word) => tmpOperand(word.pattern)));
      if (name === "cd") {
        const target = args.find((arg) => !arg.text.startsWith("-"));
        const dir = target && expand(target.pattern);
        cwd = dir?.startsWith("/") ? dir : dir && cwd && !/^[~$]/.test(dir) ? `${cwd}/${dir}` : undefined;
      }
      // smarty-dev#1998: rm (and xargs rm) of a /tmp glob, or find over one with -delete or -exec rm.
      if (DELETERS.has(name)) {
        const end = args.findIndex((arg) => arg.text === "--");
        const operands = args.filter((arg, i) => (end >= 0 && i > end) || (!(arg.text.startsWith("-") && arg.text.length > 1) && (end < 0 || i < end)));
        const recursive = name === "rm" && args.slice(0, end < 0 ? args.length : end)
          .some((arg) => arg.text === "--recursive" || /^-[^-]*[rR]/.test(arg.text));
        if (operands.some((arg) => tmpOperand(arg.pattern) || unknownOperand(arg.pattern) ||
          (recursive && expandedParent(arg.pattern))) || xargsTmp) verdict.wipe = true;
      }
      if (name === "find") {
        const deletes = args.some((arg, i) => arg.text === "-delete" || (["-exec", "-execdir", "-ok", "-okdir"].includes(arg.text) &&
          /(^|[\s/])(rm|unlink|shred)(\s|$)/.test(args.slice(i + 1).map((a) => a.text).join(" ").split(/\s[;+](\s|$)/)[0]!)));
        if (deletes && (roots.length ? roots.some((root) => tmpOperand(root.pattern) || expandedParent(root.pattern)) : tmpGlob(".", cwd))) verdict.wipe = true;
      }
      // xargs appends its input: from a lookup, `{}` and every positional parameter hold it.
      const xargsFeed = fedByXargs && pipeFeed;
      if (name === "kill") {
        const probe = args.some((arg, i) => arg.text === "-0" || arg.text === "-l" || (arg.text === "-s" && args[i + 1]?.text === "0"));
        // A PID file (`$(cat run.pid)`, `$(< run.pid)`) is a recorded PID; a lookup is not.
        const byLookup = args.some((arg) => fromLookup(arg.text) || tainted.has(arg.text));
        if (!probe && (byLookup || xargsFeed)) verdict.blocked = true;
      }
      // review/astra F6 on #105: a heredoc script as the shell receives it, placeholders included.
      if (SHELLS.has(name) || name === "ssh") for (const heredoc of heredocs) scripts.push({ text: heredoc.text, remote: name === "ssh", heredoc: true });
      if (SHELLS.has(name)) {
        const flag = args.findIndex((arg) => /^-[a-z]*c[a-z]*$/.test(arg.text));
        const payload = flag >= 0 ? args[flag + 1] : undefined;
        if (payload) {
          // review/astra F9 on #105: `sh -c SCRIPT NAME ARGS…` binds $0, $1, … (and $@, $*) to the
          // words after the script; xargs appends its input after them.
          const extra: string[] = [];
          const tmpExtra: string[] = [];
          const positional = args.slice(flag + 2);
          positional.forEach((arg, k) => { if (fromLookup(arg.text)) extra.push(String(k), "@", "*"); });
          positional.forEach((arg, k) => { if (tmpOperand(arg.pattern)) tmpExtra.push(String(k), "@", "*"); });
          if (xargsFeed) extra.push(..."123456789@*".split(""), "{}");
          if (xargsTmp) tmpExtra.push(..."123456789@*".split(""), "{}");
          scripts.push({ text: payload.text, extra, tmpExtra, uncertain: payload.ansiEscaped === true });
        }
      }
      if (name === "eval") scripts.push({ text: args.map((arg) => arg.text).join(" "), uncertain: args.some((arg) => arg.ansiEscaped) });
      if (name === "ssh") {
        let i = 0;
        while (args[i]?.text.startsWith("-")) i += SSH_VALUE_OPTIONS.has(args[i]!.text) ? 2 : 1;
        const payload = args.slice(i + 1);
        scripts.push({ text: payload.map((arg) => arg.text).join(" "), remote: true, uncertain: payload.some((arg) => arg.ansiEscaped) });
      }
      for (const inner of scripts) {
        // ponytail (#325 S2): ANSI-C escapes are not decoded here. Refuse them only at
        // execution boundaries, rather than pretending the raw body is the script Bash runs.
        verdict.uncertain ||= inner.uncertain === true;
        const result = nested(inner.text, inner.extra, inner.tmpExtra, !inner.remote,
          inner.heredoc ? undefined : receiverStdin);
        lookup = result.lookup || lookup;
        listsTmp = result.tmpList || listsTmp;
      }
      if (lookup) verdict.lookup = true;
      // A recorded PID file is an explicit pipeline source, not the whole-script lookup fallback.
      // Preserve `pgrep …; cat run.pid | xargs kill` without clearing a real lookup in this pipeline.
      if (position === 0 && piped && name === "cat" && !captured &&
        args.some((arg) => !arg.text.startsWith("-")) && !args.some((arg) => arg.text === "-" || fromLookup(arg.text))) {
        pipeFeed = false;
        if (actual) actual = { ...actual, lookup: false };
      }
      if (piped && lookup) { pipeFeed = true; actual = { lookup: true, tmp: actual?.tmp ?? false }; }
      if (listsTmp) verdict.tmpList = true;
      if (piped && listsTmp) { pipeTmp = true; actual = { lookup: actual?.lookup ?? false, tmp: true }; }
      let known = independent && !lookup && !listsTmp;
      if (stage.closed !== undefined) {
        known = compoundKnown.get(stage.closed) === true;
        const output = compoundFeeds.get(stage.closed);
        // Compound stages span command lists: recover their real output, not replay's fallback.
        // Even known-safe inherited stdin cannot erase a group's newly generated unsafe output.
        if (piped && output && (output.lookup || output.tmp)) {
          actual = { lookup: output.lookup || (actual?.lookup ?? false), tmp: output.tmp || (actual?.tmp ?? false) };
          pipeFeed ||= output.lookup;
          pipeTmp ||= output.tmp;
        }
        // Only a wholly explicit producer group may replace fallback at its outgoing pipe.
        // A group containing operand-free cat is unresolved and retains its real inherited feed.
        if (known && piped) { pipeFeed = false; pipeTmp = false; actual = { lookup: false, tmp: false }; }
        const parent = scopes.parents.get(stage.closed);
        if (parent !== undefined) recordOutput(parent, known, output);
      } else {
        const member = stage.words[0] && scopes.members.get(stage.words[0]);
        // for/select headers do not emit output; their body producers determine the compound source.
        if (member !== undefined && name && name !== "for" && name !== "select") {
          const output = lookup || listsTmp || known || actual ? {
            lookup: lookup || (!independent && (actual?.lookup ?? false)),
            tmp: listsTmp || (!independent && (actual?.tmp ?? false)),
          } : undefined;
          recordOutput(member, known, output);
        }
      }
      const compound = stage.closed !== undefined ? compoundFeeds.get(stage.closed) : undefined;
      const unsafeOutput = lookup || listsTmp || compound?.lookup || compound?.tmp ||
        (!independent && (actual?.lookup || actual?.tmp));
      if (unsafeOutput) for (const redirect of stage.redirects) {
        if (!redirect.output) continue;
        const key = fileKey(redirect.redirect);
        if (key !== undefined) context.unsafeFiles.add(key);
        else if (redirect.redirect.ansiEscaped) verdict.uncertain = true;
      }
      verdict.sourceKnown = known;
    });
    stages = [];
  };

  for (const token of tokens) {
    if ("word" in token) {
      command.words.push(token.word);
      const closed = scopes.ends.get(token);
      if (closed !== undefined) command.closed = closed;
      continue;
    }
    if ("redirect" in token) { command.redirects.push(token); continue; }
    if ("heredoc" in token) { command.heredocs.push(token.heredoc); continue; }
    if (command.words.length > 0 || command.redirects.length > 0 || command.heredocs.length > 0 || command.closed !== undefined) stages.push(command);
    command = { words: [], redirects: [], heredocs: [] };
    if (token.op !== "|" && token.op !== "|&") runPipeline();
    const closed = scopes.ends.get(token);
    if (closed !== undefined) command.closed = closed;
  }
  if (command.words.length > 0 || command.redirects.length > 0 || command.heredocs.length > 0 || command.closed !== undefined) stages.push(command);
  runPipeline();
  return verdict;
}

/** True when the shell command kills processes by name pattern (smarty-dev#774). */
export function killsByPattern(command: string): boolean {
  const verdict = scan(command, 0);
  return verdict.blocked || verdict.uncertain === true;
}

/** True when the shell command deletes by a glob over /tmp or /var/tmp, or deletes /tmp itself (smarty-dev#1998). */
export function wipesTmp(command: string): boolean {
  const verdict = scan(command, 0);
  return verdict.wipe || verdict.uncertain === true;
}

export const TMP_WIPE_REASON =
  "Blocked (smarty-dev#1998): this deletes by a glob in /tmp or /var/tmp (or /tmp itself), which also " +
  "deletes other agents' live dirs on a shared host. Recursive deletion through .. after a path " +
  "expansion is also refused: the parent is not an owned path. Record the path when you create it " +
  "(`D=$(mktemp -d)`), then delete only your own mktemp -d path by its exact name (\"$D\"), #1508/#1998. " +
  "A glob inside that dir is fine: `rm -f \"$D\"/*.json`.";

export const PATTERN_KILL_REASON =
  "Blocked (smarty-dev#774): a kill by name pattern (pkill, killall, or kill of pgrep output) also kills " +
  "other owners' processes on a shared host. Start your job with `bin/smarty-reap run RECORD -- CMD` and " +
  "end it with `bin/smarty-reap stop RECORD` (only your own process group), or run `kill <PID>` with a " +
  "PID you started and recorded (for example from `$!`).";
