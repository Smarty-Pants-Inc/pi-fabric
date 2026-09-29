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
type Word = { text: string; subs: string[]; names: string[]; dynamic: boolean; pattern: string };
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
type Token = { op: string } | { word: Word } | { redirect: Word } | { heredoc: { body: string; quoted: boolean } };

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
  const endWord = (): void => {
    if (word) tokens.push(target ? { redirect: word } : { word });
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
    const before = w.text.length;
    let bare = false;
    const kind = c === "\"" ? QUOTED : (c === "$" && text[index + 1] === "(") || c === "`" ? "$" : LITERAL;
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
type Context = { root: string; owned: Set<string>; cwd?: string | undefined; values: ReadonlyMap<string, string>; unknown: ReadonlySet<string> };

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
// `wipe`: a delete over a /tmp glob (smarty-dev#1998). `tmpList`: a word names a /tmp glob, so its output may list other agents' dirs.
type Verdict = { blocked: boolean; lookup: boolean; wipe: boolean; tmpList: boolean };
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
 * Scans a shell script. `names` holds the variables and placeholders whose value comes from a name
 * lookup in the calling script (review/astra F8 on #105: per operand, never the whole script).
 */
function scan(script: string, depth: number, names: ReadonlySet<string> = new Set(), tmpIn: ReadonlySet<string> = new Set(),
  context: Context = { root: script, owned: new Set(), values: new Map(), unknown: new Set() }): Verdict {
  const verdict: Verdict = { blocked: false, lookup: false, wipe: false, tmpList: false };
  if (depth > 6) return verdict;
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

  const nested = (text: string, extra: Iterable<string> = [], tmpExtra: Iterable<string> = [], local = true): Verdict => {
    const inner = scan(text, depth + 1, new Set([...tainted, ...extra]), new Set([...tmpNames, ...tmpExtra]),
      local ? { ...context, cwd, values, unknown } : { ...context, cwd: undefined, values: new Map(), unknown: new Set() });
    verdict.blocked ||= inner.blocked;
    verdict.lookup ||= inner.lookup;
    verdict.wipe ||= inner.wipe;
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

  // Security F1/N5 on PR #148: a lookup or /tmp listing fed by a redirect (`done < <(…)`, `<<< "$(…)"`,
  // a heredoc) may reach any read, mapfile or xargs of this script. ponytail: whole script, not per stage.
  const tokens = tokenize(script);
  const fed = { lookup: false, tmp: false };
  const cdTmp = tmpGlob(".", cwd) || tokens.some((token, i) => {
    const next = tokens[i + 1];
    return "word" in token && token.word.text === "cd" && !!next && "word" in next && tmpGlob(next.word.pattern, cwd);
  });
  for (const token of tokens) {
    const subs = "redirect" in token ? token.redirect.subs : "heredoc" in token && !token.heredoc.quoted ? expandHeredoc(token.heredoc.body).subs : [];
    for (const sub of subs) {
      const inner = scan(sub, depth + 1, tainted, tmpNames, { ...context, cwd, values, unknown });
      fed.lookup ||= inner.lookup;
      fed.tmp ||= inner.tmpList || cdTmp;
    }
  }

  const runPipeline = (): void => {
    let pipeFeed = fed.lookup;
    let pipeTmp = fed.tmp;
    stages.forEach((stage, position) => {
      const piped = position < stages.length - 1;
      // Substitutions in words and redirection targets run; so do those of an unquoted heredoc
      // body, where quotes and `#` are literal (review/astra F2 on #105). A lookup's placeholder
      // is tainted, so a script that receives its output knows which operand holds it.
      const heredocs = stage.heredocs.map((heredoc) => heredoc.quoted ? { text: heredoc.body, subs: [], names: [] } : expandHeredoc(heredoc.body));
      const expansions: Expansion[] = [...stage.words, ...stage.redirects, ...heredocs];
      let captured = false;
      for (const expansion of expansions) expansion.subs.forEach((sub, k) => {
        const inner = nested(sub);
        if (/^\s*mktemp(\s|$)/.test(sub)) context.owned.add(expansion.names[k]!);
        // Security F3: after `cd /tmp`, any captured output (`$(ls)`) may list /tmp.
        if (inner.tmpList || (tmpGlob(".", cwd) && !/^\s*mktemp(\s|$)/.test(sub))) tmpNames.add(expansion.names[k]!);
        if (!inner.lookup) return;
        captured = true;
        tainted.add(expansion.names[k]!);
      });
      const scripts: Array<{ text: string; extra?: string[]; tmpExtra?: string[]; remote?: boolean }> = [];
      const envScripts: Array<{ text: string; source: Word }> = [];
      const { words, fedByXargs } = unwrap(stage.words, envScripts);
      // review/astra F6 on #105: an `env -S` script, with the placeholders of its word.
      for (const { text } of envScripts) scripts.push({ text });
      const name = words[0]?.text.split("/").pop() ?? "";
      const args = words.slice(1);
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
      const reads = name === "read" || name === "mapfile" || name === "readarray";
      if (reads && pipeFeed) for (const arg of args) if (!arg.text.startsWith("-")) tainted.add(arg.text);
      if (reads && pipeTmp) for (const arg of args) if (!arg.text.startsWith("-")) tmpNames.add(arg.text);
      // mktemp creates one exact path; naming /tmp there does not list other agents' entries.
      // Security F3: after `cd /tmp`, a piped stage may list the cwd (`ls`, `find` without a root).
      let listsTmp = name !== "mktemp" && (stage.words.some((word) => tmpOperand(word.pattern)) || (piped && tmpGlob(".", cwd)));
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
        if (operands.some((arg) => tmpOperand(arg.pattern) || unknownOperand(arg.pattern)) || xargsTmp) verdict.wipe = true;
      }
      if (name === "find") {
        const roots: Word[] = [];
        for (const arg of args) {
          if (["-H", "-L", "-P"].includes(arg.text)) continue;
          if (arg.text.startsWith("-") || ["(", "!", ")"].includes(arg.text)) break;
          roots.push(arg);
        }
        const deletes = args.some((arg, i) => arg.text === "-delete" || (["-exec", "-execdir", "-ok", "-okdir"].includes(arg.text) &&
          /(^|[\s/])(rm|unlink|shred)(\s|$)/.test(args.slice(i + 1).map((a) => a.text).join(" ").split(/\s[;+](\s|$)/)[0]!)));
        if (deletes && (roots.length ? roots.some((root) => tmpOperand(root.pattern)) : tmpGlob(".", cwd))) verdict.wipe = true;
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
      if (SHELLS.has(name) || name === "ssh") for (const heredoc of heredocs) scripts.push({ text: heredoc.text, remote: name === "ssh" });
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
          scripts.push({ text: payload.text, extra, tmpExtra });
        }
      }
      if (name === "eval") scripts.push({ text: args.map((arg) => arg.text).join(" ") });
      if (name === "ssh") {
        let i = 0;
        while (args[i]?.text.startsWith("-")) i += SSH_VALUE_OPTIONS.has(args[i]!.text) ? 2 : 1;
        scripts.push({ text: args.slice(i + 1).map((arg) => arg.text).join(" "), remote: true });
      }
      for (const inner of scripts) {
        const result = nested(inner.text, inner.extra, inner.tmpExtra, !inner.remote);
        lookup = result.lookup || lookup;
        listsTmp = result.tmpList || listsTmp;
      }
      if (lookup) verdict.lookup = true;
      if (piped && lookup) pipeFeed = true;
      if (listsTmp) verdict.tmpList = true;
      if (piped && listsTmp) pipeTmp = true;
    });
    stages = [];
  };

  for (const token of tokens) {
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

/** True when the shell command deletes by a glob over /tmp or /var/tmp, or deletes /tmp itself (smarty-dev#1998). */
export function wipesTmp(command: string): boolean {
  return scan(command, 0).wipe;
}

export const TMP_WIPE_REASON =
  "Blocked (smarty-dev#1998): this deletes by a glob in /tmp or /var/tmp (or /tmp itself), which also " +
  "deletes other agents' live dirs on a shared host. Record the path when you create it " +
  "(`D=$(mktemp -d)`), then delete only your own mktemp -d path by its exact name (\"$D\"), #1508/#1998. " +
  "A glob inside that dir is fine: `rm -f \"$D\"/*.json`.";

export const PATTERN_KILL_REASON =
  "Blocked (smarty-dev#774): a kill by name pattern (pkill, killall, or kill of pgrep output) also kills " +
  "other owners' processes on a shared host. Start your job with `bin/smarty-reap run RECORD -- CMD` and " +
  "end it with `bin/smarty-reap stop RECORD` (only your own process group), or run `kill <PID>` with a " +
  "PID you started and recorded (for example from `$!`).";
