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
import { GuardBudget, GuardBudgetExceeded } from "./guard-budget.js";
export { GUARD_BUDGET_REASON } from "./guard-budget.js";

type Word = { text: string; subs: string[]; names: string[]; dynamic: boolean; unprovedLiteral?: boolean | undefined; pattern: string; quoted?: boolean; assignment?: boolean; process?: boolean; provenance?: Feed };
type Expansion = { subs: string[]; names: string[]; text?: string; dynamic?: boolean; unprovedLiteral?: boolean | undefined };

let placeholders = 0;
/** Records a substitution and returns the placeholder that stands for its output. */
function substitute(into: Expansion, sub: string, budget: GuardBudget): string {
  budget.spend(sub.length + 64);
  const name = `__pk_sub_${++placeholders}`;
  into.subs.push(sub);
  into.names.push(name);
  into.dynamic = true;
  return `\${${name}}`;
}
type Redirect = { redirect: Word; input: boolean; append: boolean; here: boolean; duplicate: boolean; fd: number; both: boolean; write: boolean };
type Token = { op: string; arrayClose?: boolean } | { word: Word } | Redirect | { heredoc: { body: string; quoted: boolean } };

const OPERATORS = ["&&", "||", ";;", "|&", "|", "&", ";", "(", ")"];

/** Reads to the character that closes `open` at `index` (just after the opener), nesting quotes. */
function readBalanced(text: string, index: number, open: string, close: string, budget: GuardBudget): number {
  budget.enterReader();
  try {
  let depth = 1;
  while (index < text.length) {
    budget.spend();
    const c = text[index]!;
    if (c === "\\") { budget.spend(); index += 2; continue; }
    if (c === "'" && close !== "`") {
      index += 1;
      while (index < text.length && text[index] !== "'") { budget.spend(); index += 1; }
      budget.spend(); index += 1; continue;
    }
    if (c === "\"" && close !== "`") { index = readDouble(text, index + 1, { subs: [], names: [] }, budget); continue; }
    if (c === close) { depth -= 1; index += 1; if (depth === 0) return index; continue; }
    if (open !== close && c === open) depth += 1;
    index += 1;
  }
  return index;
  } finally { budget.leaveReader(); }
}

/** Reads a double-quoted body from `index`; collects its `$(…)` and backtick substitutions. */
function readDouble(text: string, index: number, word: Expansion, budget: GuardBudget): number {
  budget.enterReader();
  try {
  while (index < text.length && text[index] !== "\"") {
    budget.spend((word.text?.length ?? 0) + 1);
    const c = text[index]!;
    if (c === "\\") {
      budget.spend(2);
      // This reader does not model every double-quoted escape. In particular Bash
      // preserves \n in a printf format; losing its slash must never prove safe bytes.
      const unproved = !/[$`"\\\n]/.test(text[index + 1] ?? "");
      if (unproved) word.unprovedLiteral = true;
      // Keep the lexical slash even when exact output is declined. Inline receiver
      // text must not acquire different format bytes when this Word is reparsed.
      word.text = (word.text ?? "") + (unproved ? "\\" : "") + (text[index + 1] ?? ""); index += 2; continue;
    }
    if ((c === "$" && text[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(text, start, "`", "`", budget) : readBalanced(text, start, "(", ")", budget);
      budget.spend(end - start + 1);
      word.text = (word.text ?? "") + substitute(word, text.slice(start, end - 1), budget);
      index = end;
      continue;
    }
    // Batch literal runs: flattening a growing word on every character would turn an
    // ordinary 4096-byte quoted value into quadratic reader work.
    let end = index + 1;
    while (end < text.length && !/["\\`]/.test(text[end]!) && !(text[end] === "$" && text[end + 1] === "(")) { budget.spend(); end += 1; }
    budget.spend(end - index + 1);
    const fragment = text.slice(index, end);
    if (fragment.includes("$")) word.dynamic = true;
    word.text = (word.text ?? "") + fragment;
    index = end;
  }
  budget.spend();
  return index + 1;
  } finally { budget.leaveReader(); }
}

/**
 * An unquoted heredoc body as the command that reads it receives it: each `$(…)` and backtick
 * substitution replaced by its placeholder. Quotes and `#` are literal there.
 */
function expandHeredoc(body: string, budget: GuardBudget): Expansion & { text: string } {
  budget.spend(4 * body.length + 1);
  const expansion: Expansion & { text: string } = { subs: [], names: [], text: "" };
  for (let index = 0; index < body.length;) {
    budget.spend(expansion.text.length + 1);
    const c = body[index]!;
    // In a heredoc a backslash escapes only $, ` and \: the reader gets `\$(…)` as `$(…)`.
    if (c === "\\") { expansion.text += /[$`\\]/.test(body[index + 1] ?? "") ? body[index + 1] : body.slice(index, index + 2); index += 2; continue; }
    if ((c === "$" && body[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(body, start, "`", "`", budget) : readBalanced(body, start, "(", ")", budget);
      budget.spend(end - start + 1);
      expansion.text += substitute(expansion, body.slice(start, end - 1), budget);
      index = end;
      continue;
    }
    let end = index + 1;
    while (end < body.length && !/[\\`]/.test(body[end]!) && !(body[end] === "$" && body[end + 1] === "(")) end += 1;
    budget.spend(end - index + 1);
    expansion.text += body.slice(index, end);
    index = end;
  }
  return expansion;
}

function tokenize(source: string, budget: GuardBudget): Token[] {
  budget.spend(12 * source.length + 1);
  // As in comment-cut (#104): a backslash-newline continues the line.
  const text = source.replace(/\\\r?\n/g, " ");
  const tokens: Token[] = [];
  const pending: Array<{ delimiter: string; strip: boolean; token: { heredoc: { body: string; quoted: boolean } } }> = [];
  let index = 0;
  let arrayDepth = 0;
  let word: Word | undefined;
  let target = false;
  let input = false;
  let append = false;
  let here = false;
  let duplicate = false;
  let fd = 0;
  let both = false;
  let write = false;
  const endWord = (): void => {
    if (word) tokens.push(target ? { redirect: word, input, append, here, duplicate, fd, both, write } : { word });
    word = undefined;
    target = false;
  };
  const current = (): Word => (word ??= { text: "", subs: [], names: [], dynamic: false, pattern: "" });
  while (index < text.length) {
    budget.spend((word?.text.length ?? 0) + 1);
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
      const end = readBalanced(text, index + 2, "(", ")", budget);
      const w = current();
      w.process = true;
      budget.spend(end - index + 1);
      const placeholder = substitute(w, text.slice(index + 2, end - 1), budget);
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
      fd = descriptor === undefined ? c === "<" ? 0 : 1 : Number(descriptor);
      both = c === "&";
      write = c === ">" || both;
      duplicate = text[index + 1] === "&";
      here = c === "<" && text.startsWith("<<<", index);
      input = c === "<" && (descriptor === undefined || descriptor === "0") && !duplicate;
      append = text.startsWith(">>", index) || text.startsWith("&>>", index);
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
      const arrayClose = operator === ")" && arrayDepth > 0;
      if (arrayClose) arrayDepth -= 1;
      tokens.push({ op: operator, ...(arrayClose ? { arrayClose: true } : {}) });
      index += operator.length;
      continue;
    }
    const w = current();
    const before = w.text.length;
    if (c === "\"" || c === "'" || (c === "$" && text[index + 1] === "'")) w.quoted = true;
    // Assignment syntax is lexical: quoting/escaping the name or '=' makes argv DATA.
    if (!w.text.includes("=") && (w.quoted || c === "\\")) w.assignment = false;
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
      index = readDouble(text, index + 1, w, budget);
    } else if ((c === "$" && text[index + 1] === "(") || c === "`") {
      const start = index + (c === "`" ? 1 : 2);
      const end = c === "`" ? readBalanced(text, start, "`", "`", budget) : readBalanced(text, start, "(", ")", budget);
      budget.spend(end - start + 1);
      w.text += substitute(w, text.slice(start, end - 1), budget);
      index = end;
    } else if (c === "\\") {
      w.text += text[index + 1] ?? "";
      index += 2;
    } else {
      if (c === "(" && /^[A-Za-z_][A-Za-z0-9_]*\+?=$/.test(w.text)) arrayDepth += 1;
      let end = index + 1;
      while (end < text.length && !/[\s'"\\`;|&()<>]/.test(text[end]!) &&
        !(text[end] === "$" && (text[end + 1] === "(" || text[end + 1] === "'"))) end += 1;
      budget.spend(end - index + 1);
      const fragment = text.slice(index, end);
      if (w.assignment !== false && !w.text.includes("=") && /^[A-Za-z_][A-Za-z0-9_]*\+?=/.test(w.text + fragment)) w.assignment = true;
      if (fragment.includes("$")) w.dynamic = true;
      w.text += fragment;
      index = end;
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
const VARIABLE = /\$\{?([A-Za-z_][A-Za-z0-9_]*|(?<=\{)[0-9]+|[0-9@*])/g;

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
const REFERENCE = /([$\u0002])\{?([A-Za-z_][A-Za-z0-9_]*|(?<=\{)[0-9]+|[0-9@*])(?:\[[@*]\])?\}?/g;
const MENTIONS_TMP = /(^|[^\w.-])\/(private\/)?(var\/)?tmp(\/|\b)/;
// Security S3 on PR #148: a longer assignment value is unknown, so a doubling chain stays linear.
const MAX_VALUE = 4096;

// Local nested scripts (sh -c, eval, substitutions) inherit the cwd and variables; ssh gets neither.
// `owned` holds the placeholders of `mktemp` substitutions; `root` is the whole tool-call command.
type Context = { immutableCells?: ReadonlySet<string>; opaqueAttributes?: boolean; root: string; owned: Set<string>; cwd?: string | undefined; uncertainCwd?: boolean; sharedCwd?: boolean; alternatives?: ReadonlyMap<string, readonly string[]>; values: ReadonlyMap<string, string>; unknown: ReadonlySet<string>; protectedAliases?: ReadonlyMap<string, Feed>; readonlyNames?: ReadonlySet<string> | undefined; uncertainReadonly?: ReadonlySet<string> | undefined; files?: ReadonlyMap<string, Feed | undefined>; detachedFiles?: ReadonlySet<string>; unprovedRedirect?: boolean; inputs?: ReadonlyMap<number, InputBinding | undefined> | undefined; sinks?: ReadonlyMap<number, OutputSink>; lookupTail?: number | undefined; tmpTail?: number | undefined };
type PositionalTail = { lookup?: number; tmp?: number };

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
type Verdict = { opaqueAttributes?: boolean; blocked: boolean; lookup: boolean; wipe: boolean; tmpList: boolean; sourceKnown?: boolean; output?: Feed; files?: ReadonlyMap<string, Feed | undefined>; detachedFiles?: ReadonlySet<string>; readonlyNames?: ReadonlySet<string>; uncertainReadonly?: ReadonlySet<string>; readonlyState?: { values: ReadonlyMap<string, string>; alternatives: ReadonlyMap<string, readonly string[]>; unknown: ReadonlySet<string>; lookup: ReadonlySet<string>; tmp: ReadonlySet<string> } };
type Feed = { lookup: boolean; tmp: boolean; literal?: string };
type OutputSink = { file: string | undefined; targets?: readonly string[] | undefined } | "stdout" | "other";
// Opening a regular file fixes its identity, not its bytes. Literal/process feeds are
// immutable; regular-file descriptors consult the current file facts at consumption.
type InputBinding = Feed | { file: string | undefined; fallback: Feed | undefined };
// Safe provenance does NOT imply zero bytes (a recorded file may contain many names).
const EMPTY_FEED: Feed = { lookup: false, tmp: false };
const NO_OUTPUT: Feed = { lookup: false, tmp: false, literal: "" };
// Owner policy: unsupported provenance is UNKNOWN, not the generic unknown-origin
// PID/path allowance. Both destructive possibilities survive every existing feed join;
// there are deliberately no claimed bytes or mktemp ownership on this fact.
const UNKNOWN_FEED: Feed = { lookup: true, tmp: true };
// A NUL is not a shell filename. This fact conservatively records unsafe writes whose
// target identity is unresolved; a later definite overwrite clears only THAT file's fact.
const UNKNOWN_FILE = "\0unknown-file";
const mergeFeed = (a: Feed, b: Feed): Feed => ({ lookup: a.lookup || b.lookup, tmp: a.tmp || b.tmp,
  ...(a.literal !== undefined && a.literal === b.literal ? { literal: a.literal } : {}) });
function concatFeed(a: Feed, b: Feed, budget: GuardBudget): Feed {
  const merged = mergeFeed(a, b);
  if (a.literal === undefined || b.literal === undefined) return { lookup: merged.lookup, tmp: merged.tmp };
  const length = a.literal.length + b.literal.length;
  budget.spend(2 * length + 1);
  return length <= MAX_VALUE ? { ...merged, literal: a.literal + b.literal } : { lookup: merged.lookup, tmp: merged.tmp };
}

/** IFS whitespace coalesces; other delimiters retain interior empty fields. A final read
 * destination receives the original remainder, not a rejoined approximation of its fields. */
function splitFields(text: string, budget: GuardBudget, ifs?: string, limit = Infinity): string[] {
  budget.spend(text.length * ((ifs?.length ?? 0) + 5) + 1);
  const delimiter = (c: string): boolean => ifs === undefined ? c === "\u0004" || c === "\u0005" : ifs.includes(c);
  const whitespace = (c: string): boolean => ifs === undefined ? c === "\u0004" : /[ \t\n]/.test(c) && ifs.includes(c);
  const parts: string[] = [];
  let start = 0;
  while (start < text.length && whitespace(text[start]!)) start += 1;
  let index = start;
  while (index < text.length) {
    if (parts.length === limit - 1) {
      let end = text.length;
      while (end > start && whitespace(text[end - 1]!)) end -= 1;
      parts.push(text.slice(start, end));
      return parts;
    }
    if (!delimiter(text[index]!)) { index += 1; continue; }
    const end = index;
    let hard = !whitespace(text[index]!);
    index += 1;
    while (index < text.length && whitespace(text[index]!)) index += 1;
    if (!hard && index < text.length && delimiter(text[index]!)) {
      hard = true; index += 1;
      while (index < text.length && whitespace(text[index]!)) index += 1;
    }
    if (end > start || hard) parts.push(text.slice(start, end));
    start = index;
  }
  if (start < text.length) parts.push(text.slice(start));
  return parts;
}
type Command = { words: Word[]; redirects: Redirect[]; heredocs: Array<{ body: string; quoted: boolean }>; closed?: number; conditional?: boolean; continues?: boolean };
type InputScope = { target: Word; start?: Word };
type SourceScopes = {
  inputs: Map<Word, InputScope>; members: Map<Word, number>; ends: Map<Token, number>; parents: Map<number, number>; outputs: Map<number, Redirect[]>; children: Set<number>;
};

/** Match late stdin redirects to their loop/group, never to unrelated reads in the script. */
function sourceScopes(tokens: Token[], budget: GuardBudget): SourceScopes {
  budget.spend(12 * tokens.length + 1);
  const stack: Array<{ kind: string; start: number }> = [];
  const scopes = new Map<number, { end: number; source: InputScope }>();
  const compounds = new Map<number, number>();
  const ends = new Map<Token, number>();
  const parents = new Map<number, number>();
  const outputs = new Map<number, Redirect[]>();
  let head = true;
  const close = (kind: string, end: number): void => {
    if (stack.at(-1)?.kind !== kind) return;
    const scope = stack.pop()!;
    compounds.set(scope.start, end);
    ends.set(tokens[end]!, scope.start);
    const parent = stack.at(-1);
    if (parent) parents.set(scope.start, parent.start);
    let target: Word | undefined;
    const redirects: Redirect[] = [];
    for (let i = end + 1; i < tokens.length; i++) {
      budget.spend();
      const token = tokens[i]!;
      if ("redirect" in token) { if (token.input) target = token.redirect; redirects.push(token); }
      else if (!("heredoc" in token)) break;
    }
    outputs.set(scope.start, redirects);
    const first = tokens[scope.start]!;
    if (target) scopes.set(scope.start, { end, source: { target, ...("word" in first ? { start: first.word } : {}) } });
  };
  tokens.forEach((token, i) => {
    budget.spend();
    if ("op" in token) {
      if (token.op === "(") stack.push({ kind: "group", start: i });
      if (token.op === ")" && !token.arrayClose) close("group", i);
      head = true;
    } else if ("word" in token && head) {
      const name = token.word.quoted || token.word.assignment === false ? "" : token.word.text;
      if (["for", "while", "until", "select"].includes(name)) stack.push({ kind: "loop", start: i });
      if (name === "{") stack.push({ kind: "brace", start: i });
      if (name === "if") stack.push({ kind: "if", start: i });
      if (name === "done") close("loop", i);
      if (name === "}") close("brace", i);
      if (name === "fi") close("if", i);
      head = ["{", "do", "then", "else", "if", "elif", "!"].includes(name);
    }
  });
  // Only annotate unsupported child binding boundaries; do not interpret job execution.
  const children = new Set<number>();
  for (const [start, end] of compounds) {
    budget.spend();
    let after = end + 1;
    while (after < tokens.length && ("redirect" in tokens[after]! || "heredoc" in tokens[after]!)) { budget.spend(); after += 1; }
    const before = tokens[start - 1], next = tokens[after];
    if ((before && "op" in before && ["|", "|&"].includes(before.op)) ||
      (next && "op" in next && ["|", "|&", "&"].includes(next.op))) children.add(start);
  }
  const inputs = new Map<Word, InputScope>();
  const members = new Map<Word, number>();
  const active: Array<{ end: number; source: InputScope }> = [];
  const groups: Array<{ start: number; end: number }> = [];
  tokens.forEach((token, i) => {
    budget.spend();
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
  return { inputs, members, ends, parents, outputs, children };
}

/** The command words after assignments and wrappers (sudo, env, timeout, xargs, …), and whether xargs feeds them. */
function unwrap(stageWords: Word[], scripts: Array<{ text: string; source: Word }>, budget: GuardBudget, argv: (words: Word[]) => Word[]): { words: Word[]; fedByXargs: boolean; argFile: Word | undefined; chdirs: Word[]; assignments: Word[] } {
  let words = stageWords;
  let fedByXargs = false;
  let argFile: Word | undefined;
  const chdirs: Word[] = [];
  const assignments: Word[] = [];
  let environment = false;
  for (;;) {
    budget.spend(4 * words.length + 1);
    while (words[0] && (words[0].assignment || (environment && /^[A-Za-z_][A-Za-z0-9_]*\+?=/.test(words[0].text)))) {
      budget.spend(words.length + 1);
      assignments.push(words[0]);
      words = words.slice(1);
    }
    environment = false;
    const prefix = words[0]?.text.split("/").pop() ?? "";
    const options = PREFIXES[prefix];
    // Reserved words are grammar only when unquoted/unescaped, not executable-name argv.
    const quotedControl = ["!", "{", "then", "do", "else", "if", "elif", "while", "until"].includes(prefix) &&
      (words[0]?.quoted || words[0]?.assignment === false);
    if (!options || quotedControl) return { words, fedByXargs, argFile, chdirs, assignments };
    // Wrapper option values are actual argv: unquoted empty fields disappear before
    // deciding which word -C/-D consumes, and quoted values remain one field.
    if (options.length) words = argv(words);
    // `command -v pkill` names the command; it does not run it.
    if (prefix === "command" && words[1] && /^-[a-zA-Z]*[vV]/.test(words[1].text)) return { words: [], fedByXargs, argFile, chdirs, assignments };
    if (prefix === "xargs") fedByXargs = true;
    words = words.slice(1);
    while (words[0]?.text.startsWith("-") && words[0].text.length > 1) {
      budget.spend(words.length + words[0].text.length + 1);
      const flag = words[0].text;
      if (flag === "--") { words = words.slice(1); break; }
      let value: string | undefined;
      let takes = false;
      // review/astra F7 on #105: remember that the option is env's split string (-S), whatever its spelling.
      let split = false;
      let file = false;
      let chdir = false;
      if (flag.startsWith("--")) {
        const [key, inline] = flag.split(/=(.*)/s);
        takes = inline === undefined && options.includes(key!);
        value = inline;
        split = key === "--split-string";
        file = key === "--arg-file";
        chdir = key === "--chdir";
      } else {
        // review/astra F4 on #105: read a short cluster from its start. The first letter that takes a
        // value takes the rest of the word (`-uroot`), or the next word when nothing is left (`-Eu paul`).
        for (let at = 1; at < flag.length; at++) {
          if (!options.includes(`-${flag[at]}`)) continue;
          value = flag.slice(at + 1) || undefined;
          takes = value === undefined;
          split = flag[at] === "S";
          file = flag[at] === "a";
          chdir = (prefix === "env" && flag[at] === "C") || (prefix === "sudo" && flag[at] === "D");
          break;
        }
      }
      const source = takes ? words[1] : words[0];
      if (takes) value = source?.text;
      if (prefix === "xargs" && file && value !== undefined && source) {
        argFile = { ...source, text: value, pattern: source.pattern.slice(source.text.length - value.length) };
      }
      if ((prefix === "env" || prefix === "sudo") && chdir && source && value !== undefined) {
        chdirs.push({ ...source, text: value, pattern: source.pattern.slice(source.text.length - value.length) });
      }
      if (prefix === "env" && split && value && source) scripts.push({ text: value,
        source: { ...source, text: value, pattern: source.pattern.slice(source.text.length - value.length) } });
      words = words.slice(takes ? 2 : 1);
    }
    if (prefix === "timeout" && words[0]) words = words.slice(1);
    environment = prefix === "env";
  }
}

/** Bash read/mapfile option values are not destinations; omitted destinations use shell defaults. */
function readDestinations(name: string, args: Word[], budget: GuardBudget): string[] {
  budget.spend(4 * args.length + 1);
  const read = name === "read";
  const valueOptions = read ? "adinNptu" : "nOsuCcd";
  let array: string | undefined;
  let index = 0;
  while (index < args.length) {
    budget.spend(args[index]!.text.length + 1);
    const flag = args[index]!.text;
    if (flag === "--") { index += 1; break; }
    if (!flag.startsWith("-") || flag.length === 1) break;
    index += 1;
    for (let at = 1; at < flag.length; at++) {
      const option = flag[at]!;
      if (!valueOptions.includes(option)) continue;
      const value = flag.slice(at + 1) || args[index++]?.text;
      if (read && option === "a") array = value;
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
function scan(script: string, depth: number, budget: GuardBudget, names: ReadonlySet<string> = new Set(), tmpIn: ReadonlySet<string> = new Set(),
  context: Context = { root: script, owned: new Set(), values: new Map(), unknown: new Set() }, stdin?: Feed): Verdict {
  budget.spend(script.length + 1);
  if (depth > 6) throw new GuardBudgetExceeded();
  // Security F4/F5: collect with the state at each command, then replay from the same entry state.
  // F6: retain fallback for unresolved feeds, but late redirects belong only to their own scope.
  const tokens = tokenize(script, budget);
  const scopes = sourceScopes(tokens, budget);
  const sources = new Map<Word, Feed | undefined>();
  const first = scanPass(tokens, scopes, sources, depth, budget, names, tmpIn, context, { lookup: false, tmp: false }, stdin);
  if (!first.lookup && !first.tmpList) return first;
  const second = scanPass(tokens, scopes, sources, depth, budget, names, tmpIn, context, { lookup: first.lookup, tmp: first.tmpList }, stdin);
  return {
    opaqueAttributes: first.opaqueAttributes === true || second.opaqueAttributes === true,
    blocked: first.blocked || second.blocked, lookup: first.lookup || second.lookup,
    wipe: first.wipe || second.wipe, tmpList: first.tmpList || second.tmpList,
    sourceKnown: first.sourceKnown === true && second.sourceKnown === true,
    output: mergeFeed(first.output ?? EMPTY_FEED, second.output ?? EMPTY_FEED), ...(second.files ? { files: second.files } : {}),
    ...(second.detachedFiles ? { detachedFiles: second.detachedFiles } : {}),
    ...(second.readonlyNames ? { readonlyNames: second.readonlyNames, uncertainReadonly: second.uncertainReadonly!, readonlyState: second.readonlyState! } : {}),
  };
}

function scanPass(tokens: Token[], scopes: SourceScopes, sources: Map<Word, Feed | undefined>,
  depth: number, budget: GuardBudget, names: ReadonlySet<string>, tmpIn: ReadonlySet<string>, context: Context, fed: Feed, stdin?: Feed): Verdict {
  budget.spend(4 * (names.size + tmpIn.size + context.values.size + context.unknown.size + (context.files?.size ?? 0) + (context.alternatives?.size ?? 0) + (context.sinks?.size ?? 0) + (context.inputs?.size ?? 0) + (context.readonlyNames?.size ?? 0) + (context.uncertainReadonly?.size ?? 0) + (context.protectedAliases?.size ?? 0)) + tokens.length + 1);
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
  const alternatives = new Map(context.alternatives);
  const unknown = new Set(context.unknown);
  // Attributes are shell-local, unlike the conservative inherited value provenance.
  // Caller-expanded bytes are private attribution cells, not shell variables or real
  // readonly attributes. A receiver cannot rewrite their provenance through a binder.
  const protectedAliases = new Map(context.protectedAliases);
  const readonlyNames = new Set(context.readonlyNames);
  const uncertainReadonly = new Set(context.uncertainReadonly);
  // Unsupported binding attributes may redirect/transform any later mutable write.
  // Keep only a shell-local hazard, never an alias destination or type interpreter.
  let opaqueAttributes = context.opaqueAttributes === true;
  budget.spend((context.immutableCells?.size ?? 0) + 1);
  const immutableCells = new Set(context.immutableCells);
  const markReadonly = (key: string): void => {
    budget.spend(key.length + 2);
    // Each insertion is reserved on the original cumulative budget, including replay.
    readonlyNames.add(key); uncertainReadonly.delete(key);
  };
  let cwd = context.cwd;
  let uncertainCwd = context.uncertainCwd ?? false;
  let sharedCwd = context.sharedCwd ?? tmpGlob(".", cwd);
  const inTmp = (pattern: string): boolean => tmpGlob(pattern, cwd) || (uncertainCwd && sharedCwd && tmpGlob(pattern, "/tmp"));
  let directoryStack: Array<string | undefined> = [cwd];
  // Latent stack alternatives affect cwd only when a later pop/swap/rotation uses them.
  let uncertainStack = false;
  let sharedStack = false;
  const files = new Map(context.files);
  // Once a pathname is unlinked, no already-open descriptor may use its replacement
  // as proof of safe bytes. Widen this unsupported lifetime, without file generations.
  budget.spend((context.detachedFiles?.size ?? 0) + 1);
  const detachedFiles = new Set(context.detachedFiles);
  let childBinding = false;
  let redirectBinding = context.unprovedRedirect === true;
  let stream: Feed = NO_OUTPUT;
  let streamKnown = true;
  let lookupTail = context.lookupTail;
  let tmpTail = context.tmpTail;
  const tailHas = (name: string, tail: number | undefined): boolean => tail !== undefined && /^[0-9]+$/.test(name) && Number(name) >= tail;
  // Opaque attributes can affect an untracked mutable cell too. Carry the same
  // UNKNOWN possibilities at every reference lookup, not only at tracked writes.
  // Captures, positionals and caller-attribution cells are not mutable shell names;
  // only an actual definite readonly attribute exempts an ordinary shell cell.
  const opaqueReference = (name: string): boolean => {
    if (!opaqueAttributes) return false;
    budget.spend(name.length + 1);
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) && !immutableCells.has(name) && !protectedAliases.has(name) &&
      !(readonlyNames.has(name) && !uncertainReadonly.has(name));
  };
  const lookupName = (name: string): boolean => tainted.has(name) || protectedAliases.get(name)?.lookup === true || tailHas(name, lookupTail) || opaqueReference(name);
  const tmpName = (name: string): boolean => tmpNames.has(name) || protectedAliases.get(name)?.tmp === true || tailHas(name, tmpTail) || opaqueReference(name);

  const nested = (text: string, extra: readonly string[] = [], tmpExtra: readonly string[] = [], local = true, input?: Feed, positionals?: Word[], sinks?: ReadonlyMap<number, OutputSink>, tails?: PositionalTail, inputs?: ReadonlyMap<number, InputBinding | undefined>, inheritReadonly = true, sameShell = false): Verdict => {
    budget.spend(2 * (tainted.size + tmpNames.size + extra.length + tmpExtra.length + values.size + alternatives.size + unknown.size) + 1);
    const innerNames = new Set(tainted);
    const innerTmp = new Set(tmpNames);
    let innerValues: ReadonlyMap<string, string> = values;
    let innerUnknown: ReadonlySet<string> = unknown;
    if (positionals) {
      const bound = new Map(values);
      const unresolved = new Set(unknown);
      budget.spend(3 * (bound.size + unresolved.size + innerNames.size + innerTmp.size) + 1);
      for (const key of [...bound.keys(), ...unresolved, ...innerNames, ...innerTmp]) if (/^(?:[0-9]+|[@*])$/.test(key)) {
        bound.delete(key); unresolved.delete(key); innerNames.delete(key); innerTmp.delete(key);
      }
      const all: string[] = [];
      positionals.forEach((arg, i) => {
        // positionalFields already expanded these in the CALLER, before temporary env.
        const expanded = arg.pattern.replaceAll(QUOTED, "$");
        budget.spend(3 * expanded.length + 1);
        if (arg.unprovedLiteral) {
          budget.spend(8);
          unresolved.add(String(i)); innerNames.add(String(i)); innerTmp.add(String(i));
          if (i > 0) { innerNames.add("@"); innerNames.add("*"); innerTmp.add("@"); innerTmp.add("*"); }
        } else if (expanded.length <= MAX_VALUE && ![...expanded.matchAll(REFERENCE)].some((match) => !context.owned.has(match[2]!))) {
          const literal = unmask(expanded);
          bound.set(String(i), literal); if (i > 0) all.push(literal);
        } else unresolved.add(String(i));
      });
      budget.spend(3 * all.reduce((size, value) => size + value.length + 1, 0) + 1);
      const aggregate = all.join(" ");
      if (aggregate.length <= MAX_VALUE) { bound.set("@", aggregate); bound.set("*", aggregate); }
      else { unresolved.add("@"); unresolved.add("*"); }
      innerValues = bound; innerUnknown = unresolved;
    }
    for (const name of extra) innerNames.add(name);
    for (const name of tmpExtra) innerTmp.add(name);
    // A remote receiver gets no inherited input table, even when the caller has fd3.
    if (!local) budget.spend();
    const inner = scan(text, depth + 1, budget, innerNames, innerTmp,
      local ? { ...context, immutableCells, protectedAliases, cwd, uncertainCwd, sharedCwd, alternatives, values: innerValues, unknown: innerUnknown,
        opaqueAttributes: inheritReadonly && opaqueAttributes,
        readonlyNames: inheritReadonly ? readonlyNames : undefined, uncertainReadonly: inheritReadonly ? uncertainReadonly : undefined,
        files, detachedFiles, unprovedRedirect: redirectBinding, inputs: inputs ?? context.inputs,
        lookupTail: positionals ? tails?.lookup : lookupTail, tmpTail: positionals ? tails?.tmp : tmpTail,
        ...(sinks ? { sinks } : {}) } : { ...context, immutableCells, protectedAliases, cwd: undefined, uncertainCwd: false, sharedCwd: false, alternatives: new Map(), values: new Map(), unknown: new Set(), opaqueAttributes: false, readonlyNames: undefined, uncertainReadonly: undefined, files: new Map(), detachedFiles: new Set(), unprovedRedirect: redirectBinding, inputs: new Map(), sinks: new Map([[1, "stdout"]]) },
      local ? input : undefined);
    if (sameShell && inner.opaqueAttributes) {
      opaqueAttributes = true;
      markBindingsUnknown(true);
    }
    if (sameShell && inner.readonlyNames) {
      budget.spend(2 * (inner.readonlyNames.size + (inner.uncertainReadonly?.size ?? 0)) + 1);
      readonlyNames.clear(); uncertainReadonly.clear();
      for (const key of inner.readonlyNames) {
        if (protectedAliases.has(key)) continue;
        readonlyNames.add(key);
        // An eval-created attribute belongs to this shell. Transfer only its associated
        // binding, not a new general mutable-eval state model; attributes cannot be
        // imported while silently retaining a different parent value/provenance.
        const state = inner.readonlyState;
        if (!state) continue;
        budget.spend(key.length + 6);
        const value = state.values.get(key), options = state.alternatives.get(key);
        if (value === undefined) values.delete(key); else values.set(key, value);
        if (options === undefined) alternatives.delete(key); else alternatives.set(key, options);
        if (state.unknown.has(key)) unknown.add(key); else unknown.delete(key);
        if (state.lookup.has(key)) tainted.add(key); else tainted.delete(key);
        if (state.tmp.has(key)) tmpNames.add(key); else tmpNames.delete(key);
      }
      for (const key of inner.uncertainReadonly ?? []) uncertainReadonly.add(key);
    }
    verdict.blocked ||= inner.blocked;
    verdict.lookup ||= inner.lookup;
    verdict.wipe ||= inner.wipe;
    if (local && inner.files) {
      budget.spend(inner.files.size + (inner.detachedFiles?.size ?? 0) + 1);
      for (const [file, source] of inner.files) files.set(file, source);
      for (const file of inner.detachedFiles ?? []) detachedFiles.add(file);
    }
    return inner;
  };
  const fromLookup = (text: string): boolean => {
    budget.spend(3 * text.length + 1);
    return [...text.matchAll(VARIABLE)].some((match) => lookupName(match[1]!));
  };
  // Preflight the full replacement BEFORE masking or allocating expanded strings.
  const expand = (pattern: string): string => budget.replace(pattern, REFERENCE,
    (match) => values.get(match[2]!) ?? match[0],
    (value, match) => values.has(match[2]!) && match[1] !== "$" ? mask(value) : value);
  // Inline script argv expands in the CALLER. Restore outer quote/escape markers only
  // afterwards: a single-quoted '$D' is receiver code, a double-quoted "$D" is caller data.
  const receiverText = (pattern: string): string => {
    budget.spend(3 * pattern.length + 1);
    const aliases = new Map<string, string>();
    // Unknown caller bytes still carry their CALLER provenance. A temporary P=4242
    // in the receiver must not reinterpret an already-expanded lookup-selected "$P".
    for (const match of pattern.matchAll(REFERENCE)) {
      const key = match[2]!;
      if ((!lookupName(key) && !tmpName(key)) || aliases.has(key)) continue;
      let alias: string;
      do {
        // Reserve both candidate allocation and the root-text search before either.
        // Root text includes quoted/escaped spellings; conservative substring avoidance
        // also keeps literal user identifiers from acquiring synthetic provenance.
        budget.spend(context.root.length + 64);
        alias = `__pk_arg_${++placeholders}`;
      } while (values.has(alias) || alternatives.has(alias) || unknown.has(alias) || tainted.has(alias) || tmpNames.has(alias) ||
        readonlyNames.has(alias) || protectedAliases.has(alias) || context.root.includes(alias));
      budget.spend(64);
      protectedAliases.set(alias, { lookup: lookupName(key), tmp: tmpName(key) });
      aliases.set(key, `\${${alias}}`);
      if (lookupName(key)) tainted.add(alias);
      if (tmpName(key)) tmpNames.add(alias);
      if (unknown.has(key)) unknown.add(alias);
    }
    const frozen = aliases.size ? budget.replace(pattern, REFERENCE, (match) => aliases.get(match[2]!) ?? match[0]) : pattern;
    const expanded = expand(frozen);
    budget.spend(4 * expanded.length + 1);
    return unmask(expanded).replaceAll(QUOTED, "$").replaceAll(LITERAL, "$");
  };
  // Field splitting is applied only to characters supplied by an unquoted expansion, never to
  // literal/quoted spaces in a filename. Glob masks survive until each field is checked.
  const fields = (pattern: string): string[] => {
    const ifs = values.get("IFS") ?? " \t\n";
    const expanded = budget.replace(pattern, REFERENCE, (match) => values.get(match[2]!) ?? match[0], (value, match) => {
      if (!values.has(match[2]!)) return value;
      if (match[1] !== "$") return mask(value);
      budget.spend(value.length * (ifs.length + 2) + 1);
      // split("") preserves UTF-16 length, required by the length-preserving renderer.
      return value.split("").map((c) => ifs.includes(c) ? /[ \t\n]/.test(c) ? "\u0004" : "\u0005" : c).join("");
    });
    return splitFields(expanded, budget);
  };
  const tmpOperand = (pattern: string): boolean => {
    budget.spend(3 * pattern.length + 1);
    return [...pattern.matchAll(REFERENCE)].some((match) => tmpName(match[2]!)) || fields(pattern).some((field) => {
      budget.spend(4 * (field.length + (cwd?.length ?? 0)) + 1);
      return inTmp(field);
    });
  };
  const positionalFields = (words: Word[], start: number): { words: Word[]; tails: PositionalTail } => {
    const result: Word[] = [];
    const tails: PositionalTail = {};
    budget.spend(4 * words.length + 1);
    for (const word of words) {
      budget.spend(3 * word.pattern.length + 1);
      for (const match of word.pattern.matchAll(REFERENCE)) {
        // An unresolved UNQUOTED list can supply many fields. Taint only the numeric tail
        // beginning at this argument; preceding literals and quoted single fields stay exact.
        if (match[1] !== "$" || values.has(match[2]!)) continue;
        const index = start + result.length;
        if (lookupName(match[2]!)) tails.lookup = Math.min(tails.lookup ?? index, index);
        if (tmpName(match[2]!)) tails.tmp = Math.min(tails.tmp ?? index, index);
      }
      const expanded = fields(word.pattern);
      if (expanded.length === 0 && (word.quoted || word.pattern.includes(QUOTED))) expanded.push("");
      for (const pattern of expanded) {
        budget.spend(5 * pattern.length + 1);
        const provenance = {
          lookup: word.unprovedLiteral === true || fromLookup(pattern.replaceAll(QUOTED, "$")),
          tmp: word.unprovedLiteral === true || [...pattern.matchAll(REFERENCE)].some((match) => tmpName(match[2]!) && !values.has(match[2]!)) || (/[*?[]/.test(pattern) && tmpOperand(pattern)),
        };
        result.push({ text: unmask(pattern).replaceAll(QUOTED, "$"), pattern, subs: [], names: [], dynamic: word.dynamic, unprovedLiteral: word.unprovedLiteral, provenance });
      }
    }
    return { words: result, tails };
  };
  const pathKey = (word: Word): string | undefined => {
    if (word.unprovedLiteral) return undefined;
    const expanded = expand(word.pattern);
    budget.spend(6 * (expanded.length + (cwd?.length ?? 0)) + 1);
    if (!expanded || expanded === "-" || /[*?[]/.test(expanded) || [...expanded.matchAll(REFERENCE)].some((match) => !context.owned.has(match[2]!))) return undefined;
    const path = unmask(expanded.replaceAll(LITERAL, "$"));
    // An uncertain branch cwd must not map a later relative overwrite to one definite file.
    if (!path.startsWith("/") && uncertainCwd) return undefined;
    const full = path.startsWith("/") ? path : cwd ? `${cwd}/${path}` : path;
    const parts: string[] = [];
    for (const part of full.split("/")) {
      if (!part || part === ".") continue;
      if (part === ".." && parts.length && parts.at(-1) !== "..") parts.pop();
      else parts.push(part);
    }
    return `${full.startsWith("/") ? "/" : ""}${parts.join("/")}`;
  };
  const changeDir = (target?: Word): void => {
    const expanded = target && expand(target.pattern);
    budget.spend(2 * ((expanded?.length ?? 0) + (cwd?.length ?? 0)) + 1);
    const dir = expanded && unmask(expanded);
    cwd = dir?.startsWith("/") ? dir : dir && cwd && !/^[~$]/.test(dir) ? `${cwd}/${dir}` : undefined;
    if (dir?.startsWith("/")) uncertainCwd = false;
    sharedCwd = tmpGlob(".", cwd) || (uncertainCwd && sharedCwd);
  };
  // Round 1 on PR #148: an unquoted expansion of an unknown value, in a command that names /tmp.
  const unknownOperand = (pattern: string): boolean => {
    budget.spend(3 * pattern.length + context.root.length + 1);
    return [...pattern.matchAll(REFERENCE)].some((match) => match[1] === "$" && (unknown.has(match[2]!) || unknown.has("IFS"))) && MENTIONS_TMP.test(context.root);
  };

  // A known substitution output is a feed, not a known literal path for a later find root.
  const knownSubs = new Set<string>();
  const evaluated = new Map<Expansion, boolean>();
  const processFeeds = new Map<Expansion, Feed>();
  const evaluateExpansion = (expansion: Expansion, input: Feed | undefined, sinks: ReadonlyMap<number, OutputSink>, inputs: ReadonlyMap<number, InputBinding | undefined>): boolean => {
    budget.spend();
    if (evaluated.has(expansion)) return evaluated.get(expansion)!;
    if (!expansion.subs.length) return false;
    budget.spend(2 * sinks.size + expansion.subs.length + 1);
    const captureSinks = new Map(sinks);
    captureSinks.set(1, "stdout");
    let captured = false;
    expansion.subs.forEach((sub, k) => {
      const inner = nested(sub, [], [], true, input, undefined, captureSinks, undefined, inputs);
      const name = expansion.names[k]!;
      budget.spend(name.length + 1);
      immutableCells.add(name);
      if (/^\s*mktemp(\s|$)/.test(sub)) context.owned.add(name);
      const returned = inner.output ?? EMPTY_FEED;
      if ("process" in expansion && expansion.process) processFeeds.set(expansion,
        concatFeed(processFeeds.get(expansion) ?? NO_OUTPUT, returned, budget));
      const known = inner.sourceKnown && (!returned.tmp || returned.literal !== undefined) && !returned.lookup;
      if (returned.literal !== undefined) {
        budget.spend(3 * returned.literal.length + 1);
        values.set(name, returned.literal.replace(/\n+$/, ""));
      }
      if (known) knownSubs.add(name);
      else if (returned.literal === undefined) unknown.add(name);
      if ((returned.tmp && returned.literal === undefined) || (inTmp(".") && !known && !/^\s*mktemp(\s|$)/.test(sub))) {
        tmpNames.add(name); verdict.tmpList = true;
      }
      if (returned.lookup) { captured = true; tainted.add(name); }
    });
    evaluated.set(expansion, captured);
    return captured;
  };
  const concrete = (word: Word): boolean => !word.unprovedLiteral && ![...expand(word.pattern).matchAll(REFERENCE)]
    .some((match) => !context.owned.has(match[2]!) || lookupName(match[2]!) || tmpName(match[2]!));
  budget.spend(3 * scopes.inputs.size + 1);
  const lateTargets = new Set([...scopes.inputs.values()].map((scope) => scope.target));
  const compoundKnown = new Map<number, boolean>();
  const compoundFeeds = new Map<number, Feed>();
  const recordOutput = (scope: number, known: boolean, feed?: Feed): void => {
    compoundKnown.set(scope, (compoundKnown.get(scope) ?? true) && known);
    if (feed) {
      const previous = compoundFeeds.get(scope) ?? NO_OUTPUT;
      compoundFeeds.set(scope, concatFeed(previous, feed, budget));
    }
  };
  const fileLeaf = (file: string): string => file.split("/").at(-1) ?? "";
  const unknownFileFeed = (file?: string, facts = files): Feed => {
    budget.spend((file?.length ?? 0) + facts.size + 1);
    const leaf = file === undefined ? undefined : fileLeaf(file);
    let feed = facts.get(UNKNOWN_FILE) ?? EMPTY_FEED;
    for (const [key, source] of facts) {
      budget.spend(key.length + 1);
      if (source && key.startsWith(`${UNKNOWN_FILE}/`) && (leaf === undefined || key === `${UNKNOWN_FILE}/${leaf}`)) feed = mergeFeed(feed, source);
    }
    return feed;
  };
  // Conditional aliases retain bounded literal alternatives for routing precision. A
  // genuinely unresolved target still gets the global fallback, never a dropped write.
  const targetLeaves = (word: Word): readonly string[] | undefined => {
    const expanded = expand(word.pattern);
    budget.spend(6 * expanded.length + 1);
    const refs = [...expanded.matchAll(REFERENCE)];
    if (/[*?[]/.test(expanded)) return undefined;
    if (!refs.length) return [fileLeaf(unmask(expanded))];
    if (refs.length !== 1) return undefined;
    const options = alternatives.get(refs[0]![2]!);
    if (!options) return undefined;
    budget.spend(options.length + 1);
    return options.map((value) => {
      const path = budget.replace(expanded, REFERENCE, () => value);
      budget.spend(3 * path.length + 1);
      return fileLeaf(unmask(path));
    });
  };
  const possibleFileFeed = (word?: Word): Feed => {
    const candidate = word && expand(word.pattern);
    const concreteCandidate = candidate && ![...candidate.matchAll(REFERENCE)].length ? candidate : undefined;
    let feed = unknownFileFeed(concreteCandidate);
    // With a concrete relative name but uncertain cwd, unrelated basenames are not
    // possible identities. An unresolved alias can name any recorded file instead.
    const expanded = word && expand(word.pattern);
    budget.spend(6 * (expanded?.length ?? 0) + files.size + 1);
    const suffix = expanded && !expanded.startsWith("/") && ![...expanded.matchAll(REFERENCE)].length && !expanded.split("/").includes("..")
      ? unmask(expanded).split("/").filter((part) => part && part !== ".").join("/") : undefined;
    const ending = suffix && `/${suffix}`;
    for (const [file, source] of files) {
      budget.spend(file.length + 1);
      if (suffix && file !== suffix && !file.endsWith(ending!)) continue;
      if (source?.lookup || source?.tmp) feed = mergeFeed(feed, source);
    }
    return { lookup: feed.lookup, tmp: feed.tmp };
  };
  const inputSource = (word: Word): Feed | undefined => {
    // A process-substitution pipe carries producer bytes, not the file named by those bytes.
    if (word.process && processFeeds.has(word)) return processFeeds.get(word);
    const lookup = fromLookup(word.text);
    const tmp = tmpOperand(word.pattern);
    const known = word.text !== "-" && ![...expand(word.pattern).matchAll(REFERENCE)]
      .some((match) => !context.owned.has(match[2]!) && !knownSubs.has(match[2]!));
    const key = pathKey(word);
    if (key !== undefined && files.has(key)) return files.get(key);
    const possible = key === undefined ? possibleFileFeed(word) : unknownFileFeed(key);
    return lookup || tmp || possible.lookup || possible.tmp ? { lookup: lookup || possible.lookup, tmp: tmp || possible.tmp } : known ? EMPTY_FEED : undefined;
  };
  // A compound redirect is opened before its body: snapshot aliases/cwd and inherit its
  // descriptor routes. Closing syntax must not resolve a body-mutated alias or truncate again.
  const outputScopes = new Map<number, Map<number, OutputSink>>();
  const applyOutputRedirects = (sinks: Map<number, OutputSink>, redirects: Redirect[], priorUnproved = false): boolean => {
    budget.spend(4 * redirects.length + 1);
    for (const redirect of redirects) {
      if (redirect.duplicate) {
        const descriptor = unmask(expand(redirect.redirect.pattern));
        sinks.set(redirect.fd, /^\d+$/.test(descriptor) ? sinks.get(Number(descriptor)) ?? "other" : "other");
      } else if (!redirect.write) {
        sinks.set(redirect.fd, "other");
      } else {
        const key = pathKey(redirect.redirect);
        sinks.set(redirect.fd, { file: key, targets: key === undefined ? targetLeaves(redirect.redirect) : undefined });
        if (redirect.both) sinks.set(2, sinks.get(redirect.fd)!);
        if (key !== undefined && !redirect.append) {
          // A previous unproved open/dup can stop redirect processing before this
          // file is touched. Join its untouched bytes, rather than attest truncation
          // by lexical order. A first, single-file overwrite keeps its existing proof.
          const before = files.has(key) ? files.get(key) : unknownFileFeed(key);
          files.set(key, priorUnproved ? mergeFeed(before ?? EMPTY_FEED, NO_OUTPUT) : NO_OUTPUT);
        }
      }
      priorUnproved = true;
    }
    return priorUnproved;
  };
  const compoundSinks = (scope: number, input?: Feed): Map<number, OutputSink> => {
    openCompound(scope, input);
    return outputScopes.get(scope)!;
  };
  // Snapshot descriptor identity/routing at entry, then read CURRENT conservative bytes.
  // Copy tables across command-local children; dup copies a binding, not a live fd route.
  const consumeInput = (binding: InputBinding | undefined): Feed | undefined => {
    budget.spend();
    if (!binding || !("file" in binding)) return binding;
    if (binding.file === undefined) return mergeFeed(binding.fallback ?? EMPTY_FEED, possibleFileFeed());
    budget.spend(detachedFiles.size + binding.file.length + 1);
    for (const removed of detachedFiles) {
      budget.spend(2 * removed.length + 2);
      if (removed === UNKNOWN_FILE || binding.file === removed || binding.file.startsWith(`${removed}/`)) return UNKNOWN_FEED;
    }
    if (files.has(binding.file)) return files.get(binding.file);
    const possible = unknownFileFeed(binding.file);
    return possible.lookup || possible.tmp ? mergeFeed(binding.fallback ?? EMPTY_FEED, possible) : binding.fallback;
  };
  budget.spend();
  const inputScopes = new Map<number, Map<number, InputBinding | undefined>>();
  const applyInputRedirects = (inputs: Map<number, InputBinding | undefined>, redirects: Redirect[], late = false): void => {
    budget.spend(4 * redirects.length + 1);
    for (const redirect of redirects) {
      const word = redirect.redirect;
      const descriptor = redirect.duplicate && unmask(expand(word.pattern));
      const source = redirect.duplicate ? descriptor && /^\d+$/.test(descriptor) && inputs.has(Number(descriptor)) ? inputs.get(Number(descriptor)) : NO_OUTPUT :
        redirect.write ? NO_OUTPUT : late && word.subs.length && sources.has(word) ? sources.get(word) :
        redirect.here ? hereFeed(word) : inputSource(word);
      budget.spend(2);
      const regular = !redirect.duplicate && !redirect.write && !redirect.here && !word.process;
      const key = regular ? pathKey(word) : undefined;
      // An unresolved regular-file identity still refers to live possible file contents,
      // never to an immutable entry-time EMPTY_FEED after a later unsafe write.
      const binding = regular ? { file: key, fallback: inputSource(word) } : source;
      inputs.set(redirect.fd, binding);
      if (!late) sources.set(word, consumeInput(binding));
    }
  };
  const openCompound = (scope: number, input?: Feed): void => {
    const pending: number[] = [];
    let current: number | undefined = scope;
    while (current !== undefined && !inputScopes.has(current)) {
      budget.spend();
      if (pending.length >= 32) throw new GuardBudgetExceeded();
      pending.push(current); current = scopes.parents.get(current);
    }
    budget.spend(2 * ((context.inputs?.size ?? 0) + (context.sinks?.size ?? 1)) + 2);
    let inputs = current === undefined ? new Map(context.inputs) : inputScopes.get(current)!;
    let sinks = current === undefined ? new Map<number, OutputSink>(context.sinks ?? [[1, "stdout"]]) : outputScopes.get(current)!;
    if (current === undefined && !inputs.has(0)) inputs.set(0, input);
    let inheritedUnproved = context.unprovedRedirect === true;
    for (let parent = current; parent !== undefined; parent = scopes.parents.get(parent)) {
      budget.spend();
      inheritedUnproved ||= (scopes.outputs.get(parent)?.length ?? 0) > 0;
    }
    for (const item of pending.reverse()) {
      budget.spend(2 * (inputs.size + sinks.size) + 2);
      inputs = new Map(inputs); sinks = new Map(sinks);
      // Expand/open in entry state and descriptor order, once, never on the closing word.
      let priorUnproved = inheritedUnproved;
      for (const redirect of scopes.outputs.get(item) ?? []) {
        evaluateExpansion(redirect.redirect, consumeInput(inputs.get(0)), sinks, inputs);
        priorUnproved = applyOutputRedirects(sinks, [redirect], priorUnproved);
        applyInputRedirects(inputs, [redirect], true);
      }
      inheritedUnproved = priorUnproved;
      inputScopes.set(item, inputs); outputScopes.set(item, sinks);
    }
  };
  const compoundInputs = (scope: number, input?: Feed): Map<number, InputBinding | undefined> => {
    openCompound(scope, input);
    return inputScopes.get(scope)!;
  };
  const literalSource = (word: Word): Feed => word.unprovedLiteral ? UNKNOWN_FEED : ({ lookup: fromLookup(word.text), tmp: tmpOperand(word.pattern) });
  const literalFeed = (literal: string): Feed => {
    budget.spend(4 * literal.length + 1);
    const tmp = splitFields(literal, budget, " \t\n").some((field) => {
      budget.spend(4 * (field.length + (cwd?.length ?? 0)) + 1);
      return inTmp(mask(field));
    });
    return { lookup: fromLookup(literal), tmp, ...(literal.length <= MAX_VALUE ? { literal } : {}) };
  };
  const hereFeed = (word: Word): Feed => {
    const source = literalSource(word);
    if (!concrete(word)) return source;
    const expanded = expand(word.pattern);
    budget.spend(3 * expanded.length + 2);
    return { ...source, ...literalFeed(`${unmask(expanded)}\n`) };
  };
  // Only bounded string-only printf forms prove output bytes. In particular %q is
  // an unsupported transformation, even when a particular operand looks shell-safe.
  const renderPrintf = (formatWord: Word | undefined, supplied: Word[]): string | undefined => {
    budget.spend(supplied.length + 1);
    if (!formatWord || formatWord.unprovedLiteral || supplied.some((word) => word.unprovedLiteral)) return undefined;
    budget.spend(3 * formatWord.pattern.length + 1);
    if ([...formatWord.pattern.matchAll(REFERENCE)].some((match) => lookupName(match[2]!) || tmpName(match[2]!))) return undefined;
    const format = expand(formatWord.pattern);
    budget.spend(4 * format.length + 4 * supplied.length + 1);
    // Option selection (including --) is outside the literal-format subset. Do not
    // render a terminator as output, or interpret an unproved option/format boundary.
    const stringFormat = !format.startsWith("-") && !/%(?![%s])/.test(format) && !/\\(?![\\nrt])/.test(format) && ![...format.matchAll(REFERENCE)].length;
    // printf receives the caller's expanded argv, including only unquoted IFS splitting.
    const argv = positionalFields(supplied, 0).words;
    budget.spend(argv.length + 1);
    const arguments_ = argv.map((arg) => expand(arg.pattern));
    budget.spend(3 * arguments_.reduce((size, value) => size + value.length, 0) + arguments_.length + 1);
    // Shell glob selection is not literal argv. Never render its pattern as the
    // selected filenames and then let a quoted capture erase that provenance.
    if (!stringFormat || arguments_.some((value) => /[*?[]/.test(value) || unmask(value).includes("\\"))) return undefined;
    const fmt = format;
    const conversions = [...fmt.matchAll(/%%|%s/g)].filter((match) => match[0] === "%s").length;
    const repeats = conversions ? Math.max(1, Math.ceil(arguments_.length / conversions)) : 1;
    budget.spend(4 * (fmt.length * repeats + arguments_.reduce((size, value) => size + value.length, 0)) + repeats + 1);
    const chunks: string[] = [];
    let at = 0;
    for (let repeat = 0; repeat < repeats; repeat++) chunks.push(fmt.replace(/%%|%s/g, (part) => part === "%%" ? "%" : arguments_[at++] ?? ""));
    return chunks.join("").replace(/\\([nrt\\])/g, (_whole, char: string) => ({ n: "\n", r: "\r", t: "\t", "\\": "\\" })[char]!);
  };
  // UNKNOWN must reach quoted destructive consumers too. The old unknown Set alone
  // only checked unquoted TMP fallback, and could silently approve these constructs.
  const markUnknown = (variable: string): void => {
    budget.spend(variable.length + 6);
    if (immutableCells.has(variable) || protectedAliases.has(variable) || (readonlyNames.has(variable) && !uncertainReadonly.has(variable))) return;
    values.delete(variable); alternatives.delete(variable); unknown.add(variable);
    tainted.add(variable); tmpNames.add(variable);
  };
  const markBindingsUnknown = (mutableOnly = false): void => {
    budget.spend(2 * (values.size + alternatives.size + unknown.size + tainted.size + tmpNames.size) + 1);
    for (const key of new Set([...values.keys(), ...alternatives.keys(), ...unknown, ...tainted, ...tmpNames])) {
      if (!mutableOnly || /^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) markUnknown(key);
    }
  };
  const bind = (variable: string, raw: string, append = false, writableAlternative = false): void => {
    budget.spend(4 * raw.length + 1);
    if (protectedAliases.has(variable)) return;
    if (!writableAlternative && readonlyNames.has(variable)) {
      // A known failed write preserves all value/provenance facts. A possible attribute
      // joins failure with the writable path, rather than inventing either outcome.
      if (uncertainReadonly.has(variable)) {
        const prior = saveConditional();
        bind(variable, raw, append, true);
        joinConditional(prior, false);
      }
      return;
    }
    // Redirection availability/success is unproved. A possibly failed builtin or
    // assignment must not strongly replace an earlier unsafe value with safe bytes.
    if (opaqueAttributes && /^[A-Za-z_][A-Za-z0-9_]*$/.test(variable)) {
      markBindingsUnknown(true); markUnknown(variable); return;
    }
    if (childBinding || redirectBinding) { markUnknown(variable); return; }
    const lookup = fromLookup(raw.replaceAll(QUOTED, "$"));
    // A scalar assignment/printf/read does not glob its value. Exact literals are kept
    // in values and checked after the eventual operand's quote/suffix expansion. Only
    // unresolved stream provenance needs unconditional tmp taint across a quoted use.
    const tmp = [...raw.matchAll(REFERENCE)].some((match) => tmpName(match[2]!) && !values.has(match[2]!));
    if (!append) { tainted.delete(variable); tmpNames.delete(variable); alternatives.delete(variable); }
    if (lookup) tainted.add(variable);
    if (tmp) tmpNames.add(variable);
    const expanded = expand(raw.replaceAll(QUOTED, "$"));
    budget.spend(4 * (expanded.length + (append ? values.get(variable)?.length ?? 0 : 0)) + 1);
    const literal = (append ? values.get(variable) ?? "" : "") + unmask(expanded);
    const unresolved = [...literal.matchAll(REFERENCE)].some((match) => !context.owned.has(match[2]!));
    if (literal.length > MAX_VALUE || unresolved || (append && unknown.has(variable))) unknown.add(variable);
    else unknown.delete(variable);
    // Do not replay self-referential symbolic values after the stored cap discarded a
    // literal. Provenance lives in the sets; unresolved strings need not grow in the map.
    if (literal.length > MAX_VALUE || unresolved) values.delete(variable);
    else values.set(variable, literal);
  };

  // A potentially skipped command cannot strongly replace a prior variable/file fact.
  // Join after EACH command so a later consumer in the same branch sees both paths.
  const saveConditional = (): { opaqueAttributes: boolean; values: Map<string, string>; alternatives: Map<string, readonly string[]>; unknown: Set<string>; tainted: Set<string>; tmp: Set<string>; readonlyNames: Set<string>; uncertainReadonly: Set<string>; files: Map<string, Feed | undefined>; cwd: string | undefined; uncertainCwd: boolean; sharedCwd: boolean; stack: Array<string | undefined>; uncertainStack: boolean; sharedStack: boolean; lookupTail: number | undefined; tmpTail: number | undefined } => {
    budget.spend(2 * (values.size + alternatives.size + unknown.size + tainted.size + tmpNames.size + readonlyNames.size + uncertainReadonly.size + files.size) + directoryStack.length + 1);
    return { opaqueAttributes, values: new Map(values), alternatives: new Map(alternatives), unknown: new Set(unknown), tainted: new Set(tainted), tmp: new Set(tmpNames), readonlyNames: new Set(readonlyNames), uncertainReadonly: new Set(uncertainReadonly), files: new Map(files), cwd, uncertainCwd, sharedCwd, stack: [...directoryStack], uncertainStack, sharedStack, lookupTail, tmpTail };
  };
  const joinConditional = (prior: ReturnType<typeof saveConditional>, joinCwd = true): void => {
    budget.spend(2 * (readonlyNames.size + prior.readonlyNames.size + prior.uncertainReadonly.size) + 1);
    for (const key of readonlyNames) {
      if (!prior.readonlyNames.has(key) || prior.uncertainReadonly.has(key)) uncertainReadonly.add(key);
      else uncertainReadonly.delete(key);
    }
    for (const key of prior.readonlyNames) readonlyNames.add(key);
    for (const key of prior.uncertainReadonly) uncertainReadonly.add(key);
    budget.spend(4 * (prior.values.size + values.size + prior.alternatives.size + alternatives.size + prior.unknown.size + prior.tainted.size + prior.tmp.size + prior.files.size) + 1);
    for (const key of new Set([...prior.values.keys(), ...values.keys(), ...prior.alternatives.keys(), ...alternatives.keys()])) {
      const before = prior.values.get(key);
      const after = values.get(key);
      if (before === after && prior.alternatives.get(key) === alternatives.get(key)) continue;
      const beforeOptions = prior.alternatives.get(key) ?? (before === undefined ? undefined : [before]);
      const afterOptions = alternatives.get(key) ?? (after === undefined ? undefined : [after]);
      // One unknown alternative means ANY target, not just the other branch's safe name.
      budget.spend(3 * ((beforeOptions?.length ?? 0) + (afterOptions?.length ?? 0)) + 2);
      if (beforeOptions && afterOptions) alternatives.set(key, [...new Set([...beforeOptions, ...afterOptions])]);
      else alternatives.delete(key);
      // Different literals become unresolved, but their destructive alternatives remain.
      for (const [literal, base, ifs, sharedAlternative] of [[before, prior.cwd, prior.values.get("IFS"), prior.uncertainCwd && prior.sharedCwd], [after, cwd, values.get("IFS"), uncertainCwd && sharedCwd]] as const) {
        if (literal !== undefined && splitFields(literal, budget, ifs ?? " \t\n").some((field) => {
          budget.spend(4 * (field.length + (base?.length ?? 0)) + 1);
          return tmpGlob(mask(field), base) || (sharedAlternative && tmpGlob(mask(field), "/tmp"));
        })) tmpNames.add(key);
      }
      values.delete(key); unknown.add(key);
    }
    budget.spend(prior.stack.length + directoryStack.length + 3);
    if (prior.stack.length !== directoryStack.length || prior.stack.some((entry, index) => entry !== directoryStack[index])) {
      uncertainStack = true;
      for (const stack of [prior.stack, directoryStack]) for (const entry of stack) {
        budget.spend(4 * (entry?.length ?? 0) + 1);
        sharedStack ||= tmpGlob(".", entry);
      }
      sharedStack ||= (prior.uncertainCwd && prior.sharedCwd) || (uncertainCwd && sharedCwd);
    }
    uncertainStack ||= prior.uncertainStack;
    sharedStack ||= prior.sharedStack;
    if (joinCwd && (prior.cwd !== cwd || prior.uncertainCwd || uncertainCwd)) {
      sharedCwd ||= prior.sharedCwd;
      cwd = undefined; uncertainCwd = true; directoryStack = [undefined];
    }
    for (const key of prior.unknown) unknown.add(key);
    for (const key of prior.tainted) tainted.add(key);
    for (const key of prior.tmp) tmpNames.add(key);
    lookupTail = prior.lookupTail === undefined ? lookupTail : Math.min(prior.lookupTail, lookupTail ?? Infinity);
    tmpTail = prior.tmpTail === undefined ? tmpTail : Math.min(prior.tmpTail, tmpTail ?? Infinity);
    // A skipped first concrete write must not shadow earlier unresolved-target bytes.
    // Existing concrete facts already encode any definite overwrite of those alternatives.
    budget.spend(files.size + 1);
    for (const [file, after] of files) {
      budget.spend(file.length + 2);
      if (prior.files.has(file) || file.startsWith(UNKNOWN_FILE)) continue;
      const before = unknownFileFeed(file, prior.files);
      if (before.lookup || before.tmp) {
        budget.spend(2);
        files.set(file, mergeFeed(before, after ?? EMPTY_FEED));
      }
    }
    for (const [file, before] of prior.files) {
      const after = files.get(file);
      // Unknown bytes never erase a known unsafe alternative.
      files.set(file, before && after ? mergeFeed(before, after) : before?.lookup || before?.tmp ? before : after?.lookup || after?.tmp ? after : undefined);
    }
  };

  let pendingCwd: { cwd: string | undefined; uncertainCwd: boolean; sharedCwd: boolean } | undefined;
  const runPipeline = (background = false): void => {
    // The inherited fd is separate from replay's whole-script fallback. Captures and local
    // inline receivers inherit the actual fd; only consuming producers return its provenance.
    let actual = context.inputs?.has(0) ? consumeInput(context.inputs.get(0)) : stdin;
    let pipeFeed = actual?.lookup ?? fed.lookup;
    let pipeTmp = actual?.tmp ?? fed.tmp;
    stages.forEach((stage, position) => {
      budget.spend(8 * (stage.words.length + stage.redirects.length + stage.heredocs.length + 1) +
        stage.words.reduce((size, word) => size + 4 * (word.text.length + word.pattern.length), 0));
      let childScope = stage.words.reduce<number | undefined>((current, word) => scopes.members.get(word) ?? current, stage.closed);
      let compoundChild = false;
      let inheritedRedirect = context.unprovedRedirect === true;
      redirectBinding = inheritedRedirect || stage.redirects.length > 0;
      while (childScope !== undefined) {
        budget.spend();
        compoundChild ||= scopes.children.has(childScope);
        inheritedRedirect ||= (scopes.outputs.get(childScope)?.length ?? 0) > 0;
        redirectBinding ||= inheritedRedirect;
        childScope = scopes.parents.get(childScope);
      }
      childBinding = background || stages.length > 1 || compoundChild;
      const childPrior = childBinding ? saveConditional() : undefined;
      const prior = stage.conditional ? childPrior ?? saveConditional() : undefined;
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
      const parentCwd = cwd;
      const parentUncertainCwd = uncertainCwd;
      const parentSharedCwd = sharedCwd;
      const member = stage.words.reduce<number | undefined>((current, word) => scopes.members.get(word) ?? current, undefined);
      const inheritedSinks = stage.closed !== undefined ? compoundSinks(stage.closed, actual) : member !== undefined ? compoundSinks(member, actual) : undefined;
      const inputScope = stage.closed ?? member;
      const inheritedInputs = inputScope !== undefined ? compoundInputs(inputScope, actual) : context.inputs;
      budget.spend(2 * (inheritedInputs?.size ?? 0) + 2);
      const inputs = new Map(inheritedInputs);
      // A pipe replaces fd0, not any other inherited descriptor. A compound entry's
      // late redirects still override that incoming pipe, before its body is expanded.
      if (position === 0 || (scope?.start && stage.words.includes(scope.start))) {
        if (inheritedInputs?.has(0)) actual = consumeInput(inheritedInputs.get(0));
      }
      if (position > 0 || !inputs.has(0)) inputs.set(0, actual);
      pipeFeed = actual?.lookup ?? pipeFeed;
      pipeTmp = actual?.tmp ?? pipeTmp;
      // Substitutions in words and redirection targets run; so do those of an unquoted heredoc
      // body, where quotes and `#` are literal (review/astra F2 on #105). A lookup's placeholder
      // is tainted, so a script that receives its output knows which operand holds it.
      const heredocs = stage.heredocs.map((heredoc) => ({ ...(heredoc.quoted ? { text: heredoc.body, subs: [], names: [] } : expandHeredoc(heredoc.body, budget)), quoted: heredoc.quoted }));
      const expansions: Expansion[] = [...stage.words, ...stage.redirects.map((redirect) => redirect.redirect), ...heredocs];
      let captured = false;
      for (const expansion of expansions) captured = evaluateExpansion(expansion, actual,
        inheritedSinks ?? context.sinks ?? new Map([[1, "stdout"]]), inputs) || captured;
      // Command redirects are expanded/opened in the caller's cwd and binding state,
      // before env/sudo chdir or child execution. Inline children inherit these fd routes.
      budget.spend(2 * (inheritedSinks?.size ?? context.sinks?.size ?? 1) + 1);
      const stageSinks = new Map<number, OutputSink>(inheritedSinks ?? context.sinks ?? [[1, "stdout"]]);
      if (piped && stage.closed === undefined) stageSinks.set(1, "stdout");
      if (stage.closed === undefined) applyOutputRedirects(stageSinks, stage.redirects, inheritedRedirect);
      const scripts: Array<{ text: string; extra?: string[]; tmpExtra?: string[]; remote?: boolean; heredoc?: boolean; positionals?: Word[]; tails?: PositionalTail }> = [];
      const envScripts: Array<{ text: string; source: Word }> = [];
      const { words, fedByXargs, argFile, chdirs, assignments } = unwrap(stage.words, envScripts, budget, (words) => {
        const argv = positionalFields(words, 0).words;
        budget.spend(argv.reduce((size, word) => size + 2 * word.text.length + 1, 1));
        return argv.map((word) => ({ ...word, text: word.text.replaceAll(LITERAL, "$"), quoted: true }));
      });
      budget.spend(chdirs.length ? directoryStack.length + 1 : 1);
      const wrapperStack = chdirs.length ? [...directoryStack] : undefined;
      const wrapperUncertainStack = uncertainStack, wrapperSharedStack = sharedStack;
      for (const target of chdirs) changeDir(target);
      // review/astra F6 on #105: an `env -S` script, with the placeholders of its word.
      for (const { source } of envScripts) scripts.push({ text: receiverText(source.pattern) });
      const name = words[0]?.text.split("/").pop() ?? "";
      const args = words.slice(1);
      let lookup = LOOKUPS.has(name);
      if (KILL_BY_NAME.has(name)) verdict.blocked = true;
      // Only leading assignment WORDS bind. An argv diagnostic containing '=' cannot
      // erase provenance. Prefix bindings affect the child's environment, not caller argv.
      const assign = (): void => {
      for (const word of assignments) {
        budget.spend(stage.words.length + 1);
        const i = stage.words.indexOf(word);
        // Arrays glob their elements at binding time. += retains every prior element and its
        // provenance; do not mistake the subsequent array close for an unrelated assignment.
        const array = /^([A-Za-z_][A-Za-z0-9_]*)(\+)?=\(/.exec(word.pattern);
        if (array) {
          if (protectedAliases.has(array[1]!) || (readonlyNames.has(array[1]!) && !uncertainReadonly.has(array[1]!))) break;
          const elements = [word.pattern.slice(array[0].length), ...stage.words.slice(i + 1).map((w) => w.pattern)];
          budget.spend(3 * elements.reduce((size, element) => size + element.length + 1, 0) + 1);
          bind(array[1]!, (array[2] ? " " : "") + elements.join(" "), array[2] !== undefined);
          if (elements.some(tmpOperand)) tmpNames.add(array[1]!);
          if (elements.some((element) => fromLookup(element.replaceAll(QUOTED, "$")))) tainted.add(array[1]!);
          break;
        }
        const value = /^([A-Za-z_][A-Za-z0-9_]*)(\+)?=(.*)$/s.exec(word.pattern);
        if (value) {
          if (word.unprovedLiteral) markUnknown(value[1]!);
          else bind(value[1]!, value[3]!, value[2] !== undefined);
        }
      }
      };
      budget.spend(6 * assignments.length + 1);
      const savedAssignments = assignments.map((word) => {
        const key = /^([A-Za-z_][A-Za-z0-9_]*)/.exec(word.text)![1]!;
        return { key, value: values.get(key), alternatives: alternatives.get(key), unknown: unknown.has(key), lookup: tainted.has(key), tmp: tmpNames.has(key) };
      });
      const restoreAssignments = (): void => {
        budget.spend(savedAssignments.length + 1);
        for (const prior of savedAssignments) {
          if (prior.value === undefined) values.delete(prior.key); else values.set(prior.key, prior.value);
          if (prior.alternatives === undefined) alternatives.delete(prior.key); else alternatives.set(prior.key, prior.alternatives);
          if (prior.unknown) unknown.add(prior.key); else unknown.delete(prior.key);
          if (prior.lookup) tainted.add(prior.key); else tainted.delete(prior.key);
          if (prior.tmp) tmpNames.add(prior.key); else tmpNames.delete(prior.key);
        }
      };
      const temporary = <T>(action: () => T): T => {
        try { assign(); return action(); } finally { restoreAssignments(); }
      };
      const declaration = ["export", "readonly", "declare", "typeset", "local"].includes(name);
      // Unwrapping identifies dangerous consumers, not current-shell execution.
      // Only a bare direct builtin with no unproved execution boundary can prove
      // declaration writes or grant NEW readonly attributes; metadata remains data.
      let declarationProved = declaration && name !== "local" && stage.words[0] === words[0] &&
        words[0]?.text === name && !words[0]?.quoted && words[0]?.assignment !== false &&
        !assignments.length && !childBinding && !redirectBinding;
      const standalone = !name || (!declaration && assignments.some((word) => /^\w+\+?=\(/.test(word.pattern)));
      if (standalone) assign();
      // Prefixes/arrays on declarations are outside the proved assignment subset. Do
      // not restore a safe caller value over a persistent or possibly failing write.
      if (declaration) {
        for (const prior of savedAssignments) markUnknown(prior.key);
        let attribute = name === "readonly";
        let diagnostic = false;
        let options = true;
        let unsupported = false;
        // Only plain scalar readonly/export attributes are supported. Printing and
        // function-only forms do not bind scalar cells. Inspect flags before binding
        // any operand so a later option cannot grant a fabricated scalar identity.
        for (const arg of args) {
          budget.spend(arg.text.length + 1);
          // Live option/name selection is unproved too; do not resolve an option
          // interpreter and then forget the attributes on subsequent assignments.
          if ([...arg.pattern.matchAll(REFERENCE)].length) { unsupported = true; break; }
          if (arg.text === "--") break;
          // Validate every option-looking word, including numeric/long/mixed forms
          // and options following operands, without interpreting unknown flags.
          if (!/^[-+]/.test(arg.text)) continue;
          if (/^-[pfF]+$/.test(arg.text)) diagnostic = true;
          // Attribute flags are not universal builtin options. Only declare/typeset
          // have proved literal -r/-x scalar forms; local's function context is unknown.
          if (!(["declare", "typeset"].includes(name) && /^-[rx]+$/.test(arg.text)) && !/^-[pfF]+$/.test(arg.text)) unsupported = true;
        }
        if (unsupported) {
          declarationProved = false;
          diagnostic = false; // Unsupported/mixed flags cannot prove a quiet metadata form.
          opaqueAttributes = true; markBindingsUnknown(true);
        }
        for (const arg of args) {
          if (options && arg.text === "--") { options = false; continue; }
          if (options && /^[-+]/.test(arg.text)) {
            if (!unsupported && arg.text.startsWith("-") && arg.text.includes("r") && name !== "export") attribute = true;
            if (!unsupported && /^-[pfF]+$/.test(arg.text)) diagnostic = true;
            continue;
          }
          options = false;
          if (diagnostic) continue;
          // Assignment words suppress splitting; other declaration operands are actual
          // quote/escape-removed argv (including quoted/escaped NAME=value and names).
          const operands = arg.assignment ? [arg] : positionalFields([arg], 0).words;
          for (const word of operands) {
            const value = /^([A-Za-z_][A-Za-z0-9_]*)(\+)?=(.*)$/s.exec(word.pattern);
            const array = value?.[3]?.startsWith("(");
            if (value) {
              if (array || word.unprovedLiteral || !declarationProved) markUnknown(value[1]!);
              else bind(value[1]!, value[3]!, value[2] !== undefined);
            }
            const key = value?.[1] ?? (/^[A-Za-z_][A-Za-z0-9_]*$/.test(word.text) ? word.text : undefined);
            if (key && !declarationProved) markUnknown(key);
            // An unresolved declaration operand may name any existing mutable cell.
            // It cannot leave an earlier literal/ownership proof available for approval.
            if (!key && [...word.pattern.matchAll(REFERENCE)].length) markBindingsUnknown();
            if (attribute && key && declarationProved) markReadonly(key);
          }
        }
      }
      if (name === "set") {
        const end = args.findIndex((arg) => arg.text === "--");
        const operands = end >= 0 ? args.slice(end + 1) : args[0]?.text.startsWith("-") ? undefined : args;
        if (operands) {
          budget.spend(4 * operands.length + 1);
          const bound = positionalFields(operands, 1);
          const positionals = bound.words.map((word) => word.pattern);
          lookupTail = bound.tails.lookup; tmpTail = bound.tails.tmp;
          budget.spend(3 * (values.size + tainted.size + tmpNames.size + unknown.size) + 1);
          for (const key of [...values.keys(), ...tainted, ...tmpNames, ...unknown]) if (/^[1-9][0-9]*$/.test(key)) {
            values.delete(key); tainted.delete(key); tmpNames.delete(key); unknown.delete(key);
          }
          positionals.forEach((raw, i) => {
            if (bound.words[i]?.unprovedLiteral) markUnknown(String(i + 1));
            else bind(String(i + 1), raw);
            if (bound.words[i]?.provenance?.lookup) tainted.add(String(i + 1));
            if (bound.words[i]?.provenance?.tmp) tmpNames.add(String(i + 1));
          });
          budget.spend(2 * positionals.reduce((size, raw) => size + raw.length + 1, 0) + 1);
          bind("@", positionals.join(" "));
          bind("*", positionals.join(" "));
          if (positionals.some(tmpOperand) || bound.words.some((word) => word.provenance?.tmp)) { tmpNames.add("@"); tmpNames.add("*"); }
          if (bound.words.some((word) => word.provenance?.lookup)) { tainted.add("@"); tainted.add("*"); }
        }
      }
      if (name === "printf") {
        // Inspect the existing caller argv, without interpreting a destination.
        // Attached/live option selection is not the proved standalone -v subset;
        // it may write even when its option Word is an expanded scalar or disappears.
        const head = positionalFields(args, 0).words[0];
        budget.spend(3 * (head?.pattern.length ?? 0) + 1);
        if ((head?.text.startsWith("-v") && args[0]?.text !== "-v") ||
          (head && [...head.pattern.matchAll(REFERENCE)].length > 0)) {
          opaqueAttributes = true; markBindingsUnknown(true);
        }
      }
      if (name === "printf" && args[0]?.text === "-v" && args[1]) {
        const target = args[1].text;
        budget.spend(3 * (target.length + args[1].pattern.length) + 1);
        // Dynamic destinations are outside the proved scalar subset. Do not invent
        // destination resolution or silently retain safe old cells when it is unknown.
        const dynamicDestination = [...args[1].pattern.matchAll(REFERENCE)].length > 0;
        const cell = dynamicDestination ? undefined : /^([A-Za-z_][A-Za-z0-9_]*)(\[.*\])?$/.exec(target);
        const destination = cell?.[1];
        if (!destination) { opaqueAttributes = true; markBindingsUnknown(true); }
        else if (!protectedAliases.has(destination) && (!readonlyNames.has(destination) || uncertainReadonly.has(destination))) {
          const supplied = args.slice(3);
          const beforeWrite = readonlyNames.has(destination) ? saveConditional() : undefined;
          // An element write cannot replace the other real array elements. No index
          // interpreter: widen the entire affected cell, including computed indices.
          const raw = cell?.[2] ? undefined : renderPrintf(args[2], supplied);
          if (raw !== undefined) bind(destination, raw);
          else markUnknown(destination);
          if (beforeWrite) joinConditional(beforeWrite, false);
        }
      }
      if (name === "for" && args[0] && args.slice(2).some((arg) => fromLookup(arg.text))) tainted.add(args[0].text);
      if (name === "for" && args[0] && args.slice(2).some((arg) => tmpOperand(arg.pattern))) tmpNames.add(args[0].text);
      // A closing group's redirect controls its body's stdin, not the group's output pipeline.
      const closing = stage.closed !== undefined || name === "done" || name === "}" || (stage.words.length === 0 &&
        stage.redirects.some((redirect) => lateTargets.has(redirect.redirect)));
      const unresolved = { lookup: pipeFeed || fed.lookup, tmp: pipeTmp || fed.tmp };
      applyInputRedirects(inputs, stage.redirects);
      if (!closing) {
        actual = consumeInput(inputs.get(0));
        const input = actual ?? unresolved;
        pipeFeed = input.lookup; pipeTmp = input.tmp;
      }
      for (const redirect of stage.redirects) if (!redirect.write && redirect.fd === 0) {
        const source = sources.get(redirect.redirect);
        verdict.lookup ||= source?.lookup ?? false;
        verdict.tmpList ||= source?.tmp ?? false;
      }
      // xargs' argument file is not its child's stdin; without -a xargs disconnects that fd.
      const receiverStdin = fedByXargs && !argFile ? EMPTY_FEED : actual;
      if (fedByXargs && !argFile) inputs.set(0, receiverStdin);
      if (argFile) {
        const source = inputSource(argFile) ?? unresolved;
        pipeFeed = source.lookup;
        pipeTmp = source.tmp;
      }
      const reads = name === "read" || name === "mapfile" || name === "readarray";
      if (reads) {
        const destinations = readDestinations(name, positionalFields(args, 0).words, budget);
        // Reads depend on descriptor availability, consumption, timeout and failure.
        // None are proved here. Even a recorded literal must not erase an unsafe old
        // value, assert its first line repeatedly, or grant ownership to the result.
        // This also covers mapfile/readarray; option arguments are not destinations.
        for (const destination of destinations) markUnknown(destination);
        if (destinations.length === 0) markBindingsUnknown();
      }
      // mktemp creates one exact path; naming /tmp there does not list other agents' entries.
      // Security F3: after `cd /tmp`, a piped stage may list the cwd (`ls`, `find` without a root).
      const roots: Word[] = [];
      if (name === "find") for (const arg of args) {
        if (["-H", "-L", "-P"].includes(arg.text)) continue;
        if (arg.text.startsWith("-") || ["(", "!", ")"].includes(arg.text)) break;
        roots.push(arg);
      }
      const fileReader = ["cat", "head", "tail", "tac"].includes(name);
      const fileArgs: Word[] = [];
      for (let i = 0; i < args.length; i++) {
        const arg = args[i]!;
        if ((name === "head" || name === "tail") && ["-n", "-c", "--lines", "--bytes"].includes(arg.text)) { i += 1; continue; }
        if (!arg.text.startsWith("-") || arg.text === "-") fileArgs.push(arg);
      }
      // Only ls's value-free flags are understood here: `ls -I pattern` still lists the cwd.
      const simpleLs = args.every((arg) => !arg.text.startsWith("-") || arg.text === "--" || /^-[aAdFlLrRtU1]+$/.test(arg.text));
      const plainFind = !args.some((arg) => ["-exec", "-execdir", "-ok", "-okdir", "-printf", "-fprintf"].includes(arg.text));
      const explicit = name === "find" && plainFind ? roots : fileReader || (name === "ls" && simpleLs) ? fileArgs : [];
      const fileFeed = fileReader && explicit.length > 0 ? explicit.reduce<Feed | undefined>((feed, word) => {
        const source = inputSource(word);
        return feed && source ? concatFeed(feed, source, budget) : undefined;
      }, NO_OUTPUT) : undefined;
      const independent = !fedByXargs && !captured && !args.some((arg) => arg.text === "-") && explicit.length > 0 && explicit.every((word) =>
        word.text !== "-" && concrete(word) && !fromLookup(word.text) && !tmpOperand(word.pattern)) &&
        (!fileFeed || (!fileFeed.lookup && !fileFeed.tmp));
      if (fileFeed) lookup ||= fileFeed.lookup;
      let listsTmp = name !== "mktemp" && (stage.words.some((word) => tmpOperand(word.pattern)) ||
        ((piped || name === "ls" || name === "find") && !independent && inTmp("."))) || (fileFeed?.tmp ?? false);
      // An explicit producer does not read stdin. Do not clear an earlier real pipeline stage.
      if (position === 0 && piped && independent && !listsTmp && !lookup) {
        pipeFeed = false; pipeTmp = false; actual = { lookup: false, tmp: false };
      }
      // Security F1: `xargs -a <(ls /tmp/…)` reads its input from a /tmp listing.
      const xargsTmp = fedByXargs && (pipeTmp || stage.words.some((word) => tmpOperand(word.pattern)));
      if (name === "cd") {
        changeDir(positionalFields(args, 0).words.find((arg) => !arg.text.startsWith("-")));
        directoryStack[0] = cwd;
      }
      if (name === "pushd" || name === "popd") {
        budget.spend(3 * directoryStack.length + args.length + 1);
        const argv = positionalFields(args, 0).words;
        const noChange = argv.some((arg) => arg.text === "-n");
        const rotation = argv.find((arg) => /^[+-]\d+$/.test(arg.text));
        const target = argv.find((arg) => !arg.text.startsWith("-") && !/^[+]\d+$/.test(arg.text));
        if (rotation) {
          const count = Number(rotation.text.slice(1));
          const index = rotation.text[0] === "+" ? count : directoryStack.length - 1 - count;
          if (index >= 0 && index < directoryStack.length) {
            if (name === "popd") directoryStack.splice(index, 1);
            else directoryStack = [...directoryStack.slice(index), ...directoryStack.slice(0, index)];
            if (!noChange) cwd = directoryStack[0];
          }
        } else if (name === "popd") {
          if (directoryStack.length > 1) { directoryStack.splice(noChange ? 1 : 0, 1); if (!noChange) cwd = directoryStack[0]; }
        } else {
          if (target) {
            const previous = cwd, previousUncertain = uncertainCwd, previousShared = sharedCwd;
            changeDir(target);
            if (noChange) { directoryStack.splice(1, 0, cwd); cwd = previous; uncertainCwd = previousUncertain; sharedCwd = previousShared; }
            else directoryStack.unshift(cwd);
          } else if (directoryStack.length > 1) {
            [directoryStack[0], directoryStack[1]] = [directoryStack[1], directoryStack[0]];
            if (!noChange) cwd = directoryStack[0];
          }
        }
        if (!noChange && uncertainStack && (name === "popd" || rotation || !target)) {
          cwd = undefined; uncertainCwd = true; sharedCwd ||= sharedStack;
        }
      }
      // smarty-dev#1998: rm (and xargs rm) of a /tmp glob, or find over one with -delete or -exec rm.
      if (DELETERS.has(name)) {
        const end = args.findIndex((arg) => arg.text === "--");
        const operands = args.filter((arg, i) => (end >= 0 && i > end) || (!(arg.text.startsWith("-") && arg.text.length > 1) && (end < 0 || i < end)));
        if (operands.some((arg) => tmpOperand(arg.pattern) || unknownOperand(arg.pattern)) || xargsTmp) verdict.wipe = true;
        // Unlink/recreate is deliberately not modeled as a new file generation. All
        // descriptor use of that identity widens to UNKNOWN; direct newly named file
        // sources still use their own facts. An unresolved delete can detach any open file.
        for (const operand of operands) {
          const key = pathKey(operand);
          budget.spend((key?.length ?? 0) + 1);
          detachedFiles.add(key ?? UNKNOWN_FILE);
        }
      }
      if (name === "find") {
        const deletes = args.some((arg, i) => arg.text === "-delete" || (["-exec", "-execdir", "-ok", "-okdir"].includes(arg.text) &&
          /(^|[\s/])(rm|unlink|shred)(\s|$)/.test(args.slice(i + 1).map((a) => a.text).join(" ").split(/\s[;+](\s|$)/)[0]!)));
        if (deletes && (roots.length ? roots.some((root) => tmpOperand(root.pattern)) : inTmp("."))) verdict.wipe = true;
        if (deletes) { budget.spend(); detachedFiles.add(UNKNOWN_FILE); }
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
      if (SHELLS.has(name) || name === "ssh") for (const heredoc of heredocs) scripts.push({
        text: receiverText(mask(heredoc.text).replaceAll("$", heredoc.quoted ? LITERAL : "$")), remote: name === "ssh", heredoc: true,
      });
      if (SHELLS.has(name)) {
        const flag = args.findIndex((arg) => /^-[a-z]*c[a-z]*$/.test(arg.text));
        const payload = flag >= 0 ? args[flag + 1] : undefined;
        if (payload) {
          // review/astra F9 on #105: `sh -c SCRIPT NAME ARGS…` binds $0, $1, … (and $@, $*) to the
          // words after the script; xargs appends its input after them.
          const extra: string[] = [];
          const tmpExtra: string[] = [];
          budget.spend(4 * args.length + 1);
          const bound = positionalFields(args.slice(flag + 2), 0);
          const positional = bound.words;
          positional.forEach((arg, k) => { if (fromLookup(arg.text)) extra.push(String(k), "@", "*"); });
          positional.forEach((arg, k) => {
            if (tmpOperand(arg.pattern)) {
              tmpExtra.push("@", "*");
              if (/[*?[]/.test(arg.pattern) || [...arg.pattern.matchAll(REFERENCE)].some((match) => tmpName(match[2]!) && !values.has(match[2]!))) tmpExtra.push(String(k));
            }
          });
          if (xargsFeed) extra.push(..."123456789@*".split(""), "{}");
          if (xargsTmp) tmpExtra.push(..."123456789@*".split(""), "{}");
          scripts.push({ text: receiverText(payload.pattern), extra, tmpExtra, positionals: positional, tails: bound.tails });
        }
      }
      if (name === "eval") scripts.push({ text: args.map((arg) => receiverText(arg.pattern)).join(" ") });
      if (name === "ssh") {
        let i = 0;
        while (args[i]?.text.startsWith("-")) i += SSH_VALUE_OPTIONS.has(args[i]!.text) ? 2 : 1;
        scripts.push({ text: args.slice(i + 1).map((arg) => receiverText(arg.pattern)).join(" "), remote: true });
      }
      let scriptOutput: Feed = NO_OUTPUT;
      let scriptKnown = true;
      for (const inner of scripts) {
        const result = temporary(() => nested(inner.text, inner.extra, inner.tmpExtra, !inner.remote,
          inner.heredoc ? undefined : receiverStdin, inner.positionals, stageSinks, inner.tails, inputs, name === "eval", name === "eval"));
        scriptOutput = concatFeed(scriptOutput, result.output ?? EMPTY_FEED, budget);
        scriptKnown &&= result.sourceKnown === true;
        lookup = (result.output?.lookup ?? false) || lookup;
        listsTmp = (result.output?.tmp ?? false) || listsTmp;
      }
      if (lookup) verdict.lookup = true;
      // A recorded PID file is an explicit pipeline source, not the whole-script lookup fallback.
      // Preserve `pgrep …; cat run.pid | xargs kill` without clearing a real lookup in this pipeline.
      if (position === 0 && piped && name === "cat" && !captured && fileFeed && !fileFeed.lookup &&
        args.some((arg) => !arg.text.startsWith("-")) && !args.some((arg) => arg.text === "-" || fromLookup(arg.text))) {
        pipeFeed = false;
        if (actual) actual = { ...actual, lookup: false };
      }
      if (piped && lookup) { pipeFeed = true; actual = { lookup: true, tmp: actual?.tmp ?? false }; }
      if (listsTmp) verdict.tmpList = true;
      if (piped && listsTmp) { pipeTmp = true; actual = { lookup: actual?.lookup ?? false, tmp: true }; }
      // Returned stream provenance is not the script's diagnostic/fallback verdict. In
      // particular, operand-free cat consumes the real inherited fd, whereas an explicit
      // file (including one written earlier here) supplies its own independent contents.
      let output: Feed = { lookup, tmp: listsTmp };
      let known = independent || lookup || listsTmp;
      const silent = reads || DELETERS.has(name) || ["kill", "cd", "pushd", "popd", "set", ":", "true", "false", "test", "[", "for", "select", "done", "}"].includes(name) ||
        (name === "printf" && args[0]?.text === "-v") || (stage.words[0] && /^[A-Za-z_][A-Za-z0-9_]*\+?=/.test(stage.words[0].text) && words.length === 0);
      if (scripts.length) { output = scriptOutput; known = scriptKnown; }
      else if (silent) { output = NO_OUTPUT; known = true; }
      else if (name === "pwd") {
        budget.spend((cwd?.length ?? 0) + 1);
        output = cwd === undefined ? EMPTY_FEED : literalFeed(`${cwd}\n`);
        known = cwd !== undefined;
      } else if (["mktemp", "date", "id", "whoami", "hostname", "uname"].includes(name)) { output = EMPTY_FEED; known = true; }
      else if (name === "echo" || name === "printf") {
        output = args.reduce((feed, arg) => mergeFeed(feed, literalSource(arg)), EMPTY_FEED);
        known = args.every(concrete);
        let rendered: string | undefined;
        const echoArgs = name === "echo" ? positionalFields(args, 0).words : [];
        const plainEcho = name === "echo" && !args.some((arg) => arg.unprovedLiteral) && echoArgs.every((arg) => {
          const expanded = expand(arg.pattern);
          return !/[*?[]/.test(expanded) && !unmask(expanded).includes("\\");
        }) &&
          !/^-[neE]+$/.test(unmask(expand(echoArgs[0]?.pattern ?? "")));
        if (name === "printf") rendered = renderPrintf(args[0], args.slice(1));
        else if (plainEcho && args.every(concrete)) {
          // Reuse the caller argv splitter; raw expansion bytes are not echo argv.
          const parts = echoArgs.map((arg) => unmask(expand(arg.pattern)));
          budget.spend(3 * parts.reduce((size, part) => size + part.length + 1, 0) + 1);
          rendered = parts.join(" ") + "\n";
        }
        if (rendered !== undefined && ![...rendered.matchAll(REFERENCE)].some((match) =>
          !context.owned.has(match[2]!) || lookupName(match[2]!) || tmpName(match[2]!))) {
          output = { ...literalFeed(unmask(rendered)), lookup: output.lookup };
          known = true;
        } else if ((name === "printf" && rendered === undefined) || (name === "echo" && !plainEcho)) {
          output = mergeFeed(output, UNKNOWN_FEED); known = false;
        }
      } else if (fileReader) {
        output = fileFeed ?? EMPTY_FEED;
        known = explicit.length > 0 && fileFeed !== undefined;
        if (explicit.length === 0 || args.some((arg) => arg.text === "-")) {
          output = concatFeed(explicit.length > 0 ? output : NO_OUTPUT, actual ?? EMPTY_FEED, budget); known = actual !== undefined;
        }
        if (heredocs.length) {
          output = heredocs.reduce((feed, doc) => {
            budget.spend(3 * doc.text.length + 1);
            const pattern = mask(doc.text).replaceAll("$", doc.quoted ? LITERAL : "$");
            const literal = unmask(expand(pattern));
            const produced = literalFeed(literal);
            const part = { ...produced, lookup: fromLookup(pattern), tmp: tmpOperand(pattern) || produced.tmp };
            return concatFeed(feed, part, budget);
          }, NO_OUTPUT);
          known = true;
        }
        // Only plain cat is an identity reader. Options and every other reader's
        // selection/formatting are unsupported, even for a recorded input file.
        if (name !== "cat" || args.some((arg) => arg.text.startsWith("-") && !["-", "--"].includes(arg.text))) {
          output = mergeFeed(output, UNKNOWN_FEED); known = false;
        }
      } else if (!name && stage.redirects.some((redirect) => redirect.input)) {
        output = actual ?? EMPTY_FEED; known = actual !== undefined;
      } else if (name === "ls" || name === "find") {
        if (name === "ls" ? simpleLs : plainFind) known ||= cwd !== undefined;
        else { output = mergeFeed(output, UNKNOWN_FEED); known = false; }
      } else {
        // This is a small reader, not an output interpreter. Unmodeled producers
        // (grep/awk/sort/tr/sed/cut and arbitrary tools alike) cannot prove identity,
        // empty output or safe ownership. Preserve input/argument taint and widen
        // their returned provenance to UNKNOWN instead of enumerating review cases.
        output = mergeFeed(mergeFeed(output, actual ?? EMPTY_FEED), UNKNOWN_FEED);
        known = false;
      }
      if (stage.closed !== undefined) {
        known = compoundKnown.get(stage.closed) ?? true;
        output = compoundFeeds.get(stage.closed) ?? NO_OUTPUT;
      }
      // F9: apply descriptors in order. Opening stderr/fd3 does not save stdout; an
      // explicit 1>&3 does. Duplication copies the current destination, not a later fd
      // binding. A truncating open can happen even if a later redirect diverts stdout;
      // earlier unproved redirects retain its untouched-file alternative.
      const sink = stageSinks.get(1) ?? "other";
      if (typeof sink === "object" && sink.file !== undefined) {
        const recorded = known || output.lookup || output.tmp ? output : undefined;
        const previous = files.has(sink.file) ? files.get(sink.file) : unknownFileFeed(sink.file);
        files.set(sink.file, previous && recorded ? concatFeed(previous, recorded, budget) : previous?.lookup || previous?.tmp ? previous : recorded?.lookup || recorded?.tmp ? recorded : undefined);
      } else if (typeof sink === "object" && (output.lookup || output.tmp)) {
        // Never drop an unsafe producer merely because its alias/cwd target is unknown.
        // It may affect any existing/open file; definite later opens still strongly reset
        // their concrete file facts, retaining the ordinary safe-overwrite allowance.
        budget.spend(2 * files.size + 2);
        const unsafe = { lookup: output.lookup, tmp: output.tmp };
        for (const [file, previous] of files) {
          budget.spend(file.length + 1);
          if (!sink.targets || sink.targets.includes(fileLeaf(file))) files.set(file, mergeFeed(previous ?? EMPTY_FEED, unsafe));
        }
        for (const target of sink.targets ?? [undefined]) {
          budget.spend((target?.length ?? 0) + 32);
          const key = target === undefined ? UNKNOWN_FILE : `${UNKNOWN_FILE}/${target}`;
          files.set(key, mergeFeed(files.get(key) ?? EMPTY_FEED, unsafe));
        }
      }
      const emitted = sink === "stdout" ? output : NO_OUTPUT;
      const emittedKnown = sink !== "stdout" || known;
      if (piped) {
        if (emittedKnown) { actual = emitted; pipeFeed = emitted.lookup; pipeTmp = emitted.tmp; }
        else { pipeFeed ||= emitted.lookup; pipeTmp ||= emitted.tmp; actual = { lookup: pipeFeed, tmp: pipeTmp }; }
      } else {
        if (stage.closed !== undefined) {
          const parent = scopes.parents.get(stage.closed);
          if (parent !== undefined) recordOutput(parent, emittedKnown, emitted);
          else { stream = concatFeed(stream, emitted, budget); streamKnown &&= emittedKnown; }
        } else if (member !== undefined) {
          if (name !== "for" && name !== "select") recordOutput(member, emittedKnown, emitted);
        } else { stream = concatFeed(stream, emitted, budget); streamKnown &&= emittedKnown; }
      }
      if (chdirs.length) { cwd = parentCwd; uncertainCwd = parentUncertainCwd; sharedCwd = parentSharedCwd; directoryStack = wrapperStack!; uncertainStack = wrapperUncertainStack; sharedStack = wrapperSharedStack; }
      // A builtin may assign one of its own temporary prefix variables (read/printf -v).
      // Its new value must not escape that temporary binding into the calling shell.
      if (!standalone && !declaration && assignments.length) restoreAssignments();
      if (prior) {
        // A later && command is reached only after this cd succeeded. Keep that routing
        // for the chain's consumer, then join the skipped path when the chain ends.
        if (stage.continues) {
          if (!pendingCwd) pendingCwd = { cwd: prior.cwd, uncertainCwd: prior.uncertainCwd, sharedCwd: prior.sharedCwd };
          else pendingCwd.sharedCwd ||= prior.sharedCwd;
        }
        joinConditional(prior, !stage.continues);
      }
      if (childPrior) {
        // Shell-local child facts cannot grant safe parent replacements. Shared file
        // effects remain live, rather than being restored with shell state.
        budget.spend(files.size + childPrior.readonlyNames.size + childPrior.uncertainReadonly.size + 1);
        childPrior.files = new Map(files);
        joinConditional(childPrior);
        opaqueAttributes = childPrior.opaqueAttributes;
        readonlyNames.clear(); childPrior.readonlyNames.forEach((key) => readonlyNames.add(key));
        uncertainReadonly.clear(); childPrior.uncertainReadonly.forEach((key) => uncertainReadonly.add(key));
      }
      childBinding = false;
      redirectBinding = context.unprovedRedirect === true;
      if (!stage.continues && pendingCwd) {
        if (pendingCwd.cwd !== cwd || pendingCwd.uncertainCwd || uncertainCwd) {
          sharedCwd ||= pendingCwd.sharedCwd;
          cwd = undefined; uncertainCwd = true; directoryStack = [undefined];
        }
        pendingCwd = undefined;
      }
    });
    stages = [];
  };

  const groupStates: Array<{ opaqueAttributes: boolean; cwd: string | undefined; uncertainCwd: boolean; sharedCwd: boolean; stack: Array<string | undefined>; uncertainStack: boolean; sharedStack: boolean; values: Map<string, string>; alternatives: Map<string, readonly string[]>; unknown: Set<string>; tainted: Set<string>; tmpNames: Set<string>; readonlyNames: Set<string>; uncertainReadonly: Set<string>; lookupTail: number | undefined; tmpTail: number | undefined }> = [];
  // This is a conservative control-flow annotation, not shell execution. Conditions,
  // short-circuit RHSs and loop bodies may be skipped, including zero iterations.
  const controls: Array<{ kind: string; conditional: boolean }> = [];
  let shortCircuit = false;
  let statementHead = true;
  const conditional = (): boolean => shortCircuit || controls.some((control) => control.conditional);
  for (const token of tokens) {
    budget.spend(controls.length + 1);
    if ("word" in token) {
      if (statementHead) {
        const name = token.word.quoted || token.word.assignment === false ? "" : token.word.text;
        if (["if", "for", "while", "until", "select", "case"].includes(name)) controls.push({ kind: name === "if" ? "if" : name === "case" ? "case" : "loop", conditional: true });
        command.conditional ||= conditional();
        if (name === "{") controls.push({ kind: "brace", conditional: conditional() });
        const end = name === "fi" ? "if" : name === "done" ? "loop" : name === "esac" ? "case" : name === "}" ? "brace" : undefined;
        if (end && controls.at(-1)?.kind === end) controls.pop();
        statementHead = ["then", "do", "else", "{", "if", "elif", "while", "until", "!"].includes(name);
      }
      command.words.push(token.word);
      const closed = scopes.ends.get(token);
      if (closed !== undefined) command.closed = closed;
      continue;
    }
    if ("redirect" in token) { command.conditional ||= conditional(); command.redirects.push(token); continue; }
    if ("heredoc" in token) { command.heredocs.push(token.heredoc); continue; }
    // An array closer is part of the binding command, not a job/list boundary.
    // Flushing here would lose a following pipe/& and grant a parent safe overwrite.
    if (token.arrayClose) continue;
    command.continues = token.op === "&&";
    if (command.words.length > 0 || command.redirects.length > 0 || command.heredocs.length > 0 || command.closed !== undefined) stages.push(command);
    command = { words: [], redirects: [], heredocs: [] };
    statementHead = true;
    if (token.op !== "|" && token.op !== "|&") runPipeline(token.op === "&");
    if (["&&", "||"].includes(token.op)) shortCircuit = true;
    else if (!["|", "|&", "("].includes(token.op)) shortCircuit = false;
    if (token.op === "(") {
      controls.push({ kind: "group", conditional: conditional() });
      budget.spend(2 * (values.size + alternatives.size + unknown.size + tainted.size + tmpNames.size + readonlyNames.size + uncertainReadonly.size) + directoryStack.length + 1);
      groupStates.push({ opaqueAttributes, cwd, uncertainCwd, sharedCwd, stack: [...directoryStack], uncertainStack, sharedStack, lookupTail, tmpTail, values: new Map(values), alternatives: new Map(alternatives), unknown: new Set(unknown), tainted: new Set(tainted), tmpNames: new Set(tmpNames), readonlyNames: new Set(readonlyNames), uncertainReadonly: new Set(uncertainReadonly) });
    }
    if (token.op === ")" && !token.arrayClose) {
      if (controls.at(-1)?.kind === "group") controls.pop();
      const saved = groupStates.pop();
      if (saved) {
        opaqueAttributes = saved.opaqueAttributes;
        cwd = saved.cwd; uncertainCwd = saved.uncertainCwd; sharedCwd = saved.sharedCwd; directoryStack = saved.stack; uncertainStack = saved.uncertainStack; sharedStack = saved.sharedStack; lookupTail = saved.lookupTail; tmpTail = saved.tmpTail;
        values.clear(); saved.values.forEach((value, key) => values.set(key, value));
        alternatives.clear(); saved.alternatives.forEach((value, key) => alternatives.set(key, value));
        unknown.clear(); saved.unknown.forEach((key) => unknown.add(key));
        tainted.clear(); saved.tainted.forEach((key) => tainted.add(key));
        tmpNames.clear(); saved.tmpNames.forEach((key) => tmpNames.add(key));
        readonlyNames.clear(); saved.readonlyNames.forEach((key) => readonlyNames.add(key));
        uncertainReadonly.clear(); saved.uncertainReadonly.forEach((key) => uncertainReadonly.add(key));
      }
    }
    const closed = scopes.ends.get(token);
    if (closed !== undefined) command.closed = closed;
  }
  if (command.words.length > 0 || command.redirects.length > 0 || command.heredocs.length > 0 || command.closed !== undefined) stages.push(command);
  runPipeline();
  verdict.output = stream;
  verdict.sourceKnown = streamKnown;
  verdict.files = files;
  verdict.detachedFiles = detachedFiles;
  verdict.opaqueAttributes = opaqueAttributes;
  verdict.readonlyNames = readonlyNames;
  verdict.uncertainReadonly = uncertainReadonly;
  budget.spend();
  verdict.readonlyState = { values, alternatives, unknown, lookup: tainted, tmp: tmpNames };
  return verdict;
}

export type CommandGuardResult = { blocked: boolean; wipe: boolean; exhausted: boolean };

/** One cumulative budget for both verdicts, collection/replay, and all nested readers. */
export function scanCommand(command: string): CommandGuardResult {
  const budget = new GuardBudget();
  try {
    const verdict = scan(command, 0, budget);
    return { blocked: verdict.blocked, wipe: verdict.wipe, exhausted: false };
  } catch (error) {
    if (!(error instanceof GuardBudgetExceeded)) throw error;
    return { blocked: true, wipe: true, exhausted: true };
  }
}

/** True when the shell command kills processes by name pattern (smarty-dev#774). */
export function killsByPattern(command: string): boolean { return scanCommand(command).blocked; }

/** True when the shell command deletes by a glob over /tmp or /var/tmp, or deletes /tmp itself (smarty-dev#1998). */
export function wipesTmp(command: string): boolean { return scanCommand(command).wipe; }

export const TMP_WIPE_REASON =
  "Blocked (smarty-dev#1998): this deletes by a glob in /tmp or /var/tmp (or /tmp itself), which also " +
  "deletes other agents' live dirs on a shared host. Record the path when you create it " +
  "(`D=$(mktemp -d)`), then delete only your own mktemp -d path by its exact name (\"$D\"), #1508/#1998. " +
  "Use the simple form: run `D=$(mktemp -d)` in its own command, then `rm -rf \"$D\"`. " +
  "Complex reads, bindings, transformations and reused descriptors are not trusted to prove a safe path. " +
  "A glob inside that dir is fine: `rm -f \"$D\"/*.json`.";

export const PATTERN_KILL_REASON =
  "Blocked (smarty-dev#774): a kill by name pattern (pkill, killall, or kill of pgrep output) also kills " +
  "other owners' processes on a shared host. Start your job with `bin/smarty-reap run RECORD -- CMD` and " +
  "end it with `bin/smarty-reap stop RECORD` (only your own process group), or run `kill <PID>` with a " +
  "PID you started and recorded (for example from `$!`). Use a literal recorded PID or a plain " +
  "assignment from `$!`; complex reads, bindings, transformations and reused descriptors are not " +
  "trusted to prove a safe PID.";
