import { alphaModeEnabled } from "./alpha-mode.js";

// The default policy is deliberately lexical, not a shell interpreter. Do not add
// variable/source tracing, inline-script parsing, or the full guard's dependencies here.
type Word = { text: string; pattern: string; pgrep: boolean };
type Command = { words: Word[]; piped: boolean };

const KILL_REASON = "Blocked: pkill, killall, and killing pgrep output can kill unrelated processes. Use a PID you started and recorded.";
const TMP_REASON = "Blocked: recursive deletion of /tmp or /var/tmp, or a glob directly in either root, can delete unrelated files. Delete only your own exact directory.";

/** Skip one substitution without interpreting its program. */
function substitutionEnd(source: string, start: number, backtick: boolean): number {
  let depth = 1;
  let quote = "";
  for (let i = start; i < source.length; i++) {
    const c = source[i]!;
    if (c === "\\" && quote !== "'") { i++; continue; }
    if (backtick) { if (c === "`") return i; continue; }
    if (quote) { if (c === quote) quote = ""; continue; }
    if (c === "'" || c === '"') { quote = c; continue; }
    if (c === "(") depth++;
    if (c === ")" && --depth === 0) return i;
  }
  return source.length;
}

/** Read simple command words, keeping quoted globs inert and heredoc bodies opaque. */
function commands(source: string): Command[] {
  const result: Command[] = [];
  const heredocs: Array<{ delimiter: string; stripTabs: boolean }> = [];
  let words: Word[] = [];
  let word: Word | undefined;
  let quote = "";
  let redirect: { heredoc: boolean; stripTabs: boolean } | undefined;
  const current = (): Word => (word ??= { text: "", pattern: "", pgrep: false });
  const append = (text: string, literal: boolean): void => {
    current().text += text;
    current().pattern += literal ? text.replace(/[*?\[]/g, "\0") : text;
  };
  const endWord = (): void => {
    if (!word) return;
    if (redirect?.heredoc) heredocs.push({ delimiter: word.text, stripTabs: redirect.stripTabs });
    else if (!redirect) words.push(word);
    word = undefined;
    redirect = undefined;
  };
  const endCommand = (piped: boolean): void => {
    endWord();
    if (words.length) result.push({ words, piped });
    words = [];
  };

  for (let i = 0; i < source.length;) {
    const c = source[i]!;
    if (c === "'" && quote !== '"') { current(); quote = quote ? "" : "'"; i++; continue; }
    if (quote === "'") { append(c, true); i++; continue; }
    if (c === '"') { current(); quote = quote ? "" : '"'; i++; continue; }
    if (c === "\\") {
      const next = source[i + 1];
      if (next === "\n") { i += 2; continue; }
      // Within double quotes, backslash only escapes these shell metacharacters.
      if (quote && next && !/[\$`"\\]/.test(next)) { append(c, true); i++; continue; }
      append(next ?? "", true); i += 2; continue;
    }
    if ((c === "$" && source[i + 1] === "(") || c === "`") {
      const start = i + (c === "`" ? 1 : 2);
      const end = substitutionEnd(source, start, c === "`");
      // Only a direct lookup argument matters; no expansion or provenance analysis.
      current().pgrep ||= /^\s*(?:[^\s'"`$();|&]+\/)?pgrep(?:\s|$)/.test(source.slice(start, end));
      append("\0", true);
      i = Math.min(end + 1, source.length);
      continue;
    }
    if (quote) { append(c, true); i++; continue; }
    if (c === "#" && !word) {
      while (i < source.length && source[i] !== "\n") i++;
      continue;
    }
    if (c === " " || c === "\t" || c === "\r") { endWord(); i++; continue; }
    if (c === "\n") {
      endCommand(false);
      i++;
      for (const { delimiter, stripTabs } of heredocs.splice(0)) {
        while (i < source.length) {
          const end = source.indexOf("\n", i);
          const line = source.slice(i, end < 0 ? source.length : end);
          i = end < 0 ? source.length : end + 1;
          if ((stripTabs ? line.replace(/^\t+/, "") : line) === delimiter) break;
        }
      }
      continue;
    }
    if (c === "<" || c === ">") {
      // A redirection target is not an executable or an rm operand.
      if (word && /^\d+$/.test(word.text)) word = undefined;
      endWord();
      const op = /^(?:<<<|<<-?|>>|[<>]&?)/.exec(source.slice(i))![0];
      redirect = { heredoc: op === "<<" || op === "<<-", stripTabs: op === "<<-" };
      i += op.length;
      continue;
    }
    if (";|&()".includes(c)) {
      const op = source.slice(i, i + 2);
      const paired = ["&&", "||", "|&", ";;"].includes(op);
      endCommand(c === "|" && op !== "||");
      i += paired ? 2 : 1;
      continue;
    }
    append(c, false); i++;
  }
  endCommand(false);
  return result;
}

const nameOf = (word: Word | undefined): string => word?.text.split("/").at(-1) ?? "";

/** Just transparent, argument-free prefixes; scripts and wrapper option languages stay opaque. */
function invocation(words: Word[]): Word[] {
  let i = 0;
  while (words[i] && (/^[A-Za-z_][A-Za-z_0-9]*=/.test(words[i]!.text) ||
    ["!", "if", "then", "elif", "else", "do", "while", "until", "{"].includes(words[i]!.text))) i++;
  while (["command", "builtin", "exec", "nohup", "sudo", "env", "setsid"].includes(nameOf(words[i]))) {
    i++;
    while (words[i]?.text === "--" || (words[i] && /^[A-Za-z_][A-Za-z_0-9]*=/.test(words[i]!.text))) i++;
  }
  return words.slice(i);
}

function xargsInvocation(words: Word[]): Word[] {
  let i = 1;
  while (words[i]?.text.startsWith("-")) {
    const option = words[i++]!.text;
    if (option === "--") break;
    if (["-I", "-n", "-P", "-s", "-L", "-E", "-d", "--replace", "--max-args", "--max-procs", "--max-chars", "--max-lines", "--eof", "--delimiter"].includes(option)) i++;
  }
  return invocation(words.slice(i));
}

function wipesRoot(args: Word[]): boolean {
  let recursive = false;
  let options = true;
  const targets: Word[] = [];
  for (const word of args) {
    if (options && word.text === "--") { options = false; continue; }
    if (options && word.text.startsWith("-")) {
      recursive ||= word.text === "--recursive" || /^-[^-]*[rR]/.test(word.text);
    } else targets.push(word);
  }
  return recursive && targets.some(word => /^\/(?:var\/)?tmp\/*$/.test(word.text) ||
    /^\/(?:var\/)?tmp\/(?=[^/]*[*?\[])[^/]+\/?$/.test(word.pattern));
}

function narrowRefusal(command: string): string | undefined {
  let lookupPipe = false;
  let wipe = false;
  for (const stage of commands(command)) {
    const words = invocation(stage.words);
    const name = nameOf(words[0]);
    const called = name === "xargs" ? xargsInvocation(words) : words;
    const executable = nameOf(called[0]);
    if (executable === "pkill" || executable === "killall" ||
      (executable === "kill" && (lookupPipe || called.slice(1).some(word => word.pgrep)))) return KILL_REASON;
    if (executable === "rm") wipe ||= wipesRoot(called.slice(1));
    lookupPipe = stage.piped && (lookupPipe || name === "pgrep");
  }
  return wipe ? TMP_REASON : undefined;
}

/** Only an explicit zero, with no disable file, opts into the original full policy. */
export async function bashGuardRefusal(command: string, toolTimeoutS?: number): Promise<string | undefined> {
  if (alphaModeEnabled()) return narrowRefusal(command);

  const { killsByPattern, wipesTmp, PATTERN_KILL_REASON, TMP_WIPE_REASON } = await import("../core/pattern-kill.js");
  if (killsByPattern(command)) return PATTERN_KILL_REASON;
  if (wipesTmp(command)) return TMP_WIPE_REASON;
  const { foregroundWaitRefusal } = await import("./foreground-wait.js");
  return foregroundWaitRefusal(command, toolTimeoutS);
}
