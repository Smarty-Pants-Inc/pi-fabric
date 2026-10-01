import { posix } from "node:path";

export const SIGNAL_REASON = "Signal refused: use the PID you recorded (literal integer PIDs only).";
export const DELETE_REASON = "Recursive delete refused: delete only inside your own TMPDIR using literal absolute paths.";

type Word = { text: string; literal: boolean };
type Split = { words: Word[]; simple: boolean };
const SIGNALS = new Set(["kill", "pkill", "killall", "killall5"]);
const DATA = new Set(["echo", "printf", "cat", "ls", "grep", "rg", "head", "tail", "wc"]);
const DELETES = new Set(["rm", "find", "shred", "xargs"]);

/** Literal words only. No expansion, bindings, output, shell argv, or execution interpretation. */
function split(source: string): Split {
  const words: Word[] = [];
  const opaqueInput = source.includes("<<");
  let text = "", started = false, literal = true, simple = true;
  let quote = "";
  const end = (): void => {
    if (started) words.push({ text, literal });
    text = ""; started = false; literal = true;
  };
  for (let i = 0; i < source.length; i++) {
    const c = source[i]!;
    if (quote === "'") {
      if (c === "'") quote = "";
      else text += c;
      continue;
    }
    if (quote === '"') {
      if (c === '"') { quote = ""; continue; }
      if (c === "$" || c === "`") literal = false;
      if (c === "\\") {
        const next = source[++i];
        if (next === undefined || next === "\n" || next === "\r") { simple = false; literal = false; }
        else if ('"$`\\'.includes(next)) text += next;
        else text += `\\${next}`;
      } else text += c;
      continue;
    }
    if (c === " " || c === "\t") { end(); continue; }
    // Do not guess command boundaries, heredoc syntax, redirects, or continuation semantics.
    if ("\n\r;&|()<>".includes(c)) { end(); simple = false; continue; }
    if (c === "#" && !started && !opaqueInput) {
      while (i + 1 < source.length && source[i + 1] !== "\n") i++;
      continue;
    }
    started = true;
    if (c === "'" || c === '"') { quote = c; continue; }
    if (c === "\\") {
      const next = source[++i];
      if (next === "\n") { simple = false; continue; }
      if (next === undefined || next === "\r") { simple = false; literal = false; }
      else text += next;
      continue;
    }
    if ("$`*?[]{}~".includes(c)) literal = false;
    text += c;
  }
  if (quote) { simple = false; literal = false; }
  end();
  return { words, simple };
}

/** One forward pass: each lexical token/basename is considered once, without path backtracking. */
function protectedTokens(source: string): { signal: boolean; deletion: boolean } {
  let signal = false, deletion = false, start = 0, slash = -1;
  for (let i = 0; i <= source.length; i++) {
    const c = source[i];
    if (c === "/") slash = i;
    if (c !== undefined && !/\s/.test(c) && !";|&()<>\"'`=".includes(c)) continue;
    if (i > start) {
      // The attached env -S spelling is lexical evidence, not an argv/receiver interpreter.
      const token = source.slice(Math.max(start, slash + 1), i).replace(/^-[A-Za-z]*S/, "");
      signal ||= SIGNALS.has(token);
      deletion ||= DELETES.has(token);
    }
    start = i + 1; slash = -1;
  }
  return { signal, deletion };
}

const FIND_VALUES = new Set(["-type", "-name", "-iname", "-path", "-ipath", "-regex", "-iregex", "-size", "-mtime", "-mmin", "-atime", "-amin", "-ctime", "-cmin", "-user", "-group", "-uid", "-gid", "-perm", "-links", "-inum", "-maxdepth", "-mindepth"]);
const FIND_FLAGS = new Set(["-H", "-L", "-P", "-print", "-print0", "-empty", "-readable", "-writable", "-executable", "-true", "-false", "-depth", "-mount", "-xdev", "-prune", "-ls"]);

function literalFileMaintenance(words: Word[]): boolean {
  const args = words.slice(1).map(word => word.text);
  if (words[0]?.text === "rm") {
    let i = 0;
    while (args[i] === "-f") i++;
    if (args[i] === "--") i++;
    return i < args.length && args.slice(i).every(arg => arg.length > 0 && !arg.startsWith("-") && !/[$`*?\[\]{}~\r\n]/.test(arg));
  }
  if (words[0]?.text !== "find" || args.some(arg => /[$`*?\[\]{}~\r\n]/.test(arg))) return false;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (FIND_FLAGS.has(arg)) continue;
    if (FIND_VALUES.has(arg)) {
      if (!args[++i]) return false;
      continue;
    }
    if (!arg.startsWith("-") && arg.length > 0 && i === 0) continue;
    return false;
  }
  return true;
}

function literalData(words: Word[], simple: boolean): boolean {
  if (!simple || words.length === 0 || words.some(word => !word.literal)) return false;
  const head = words[0]!.text;
  if (head.includes("/")) return false;
  if (literalFileMaintenance(words)) return true;
  if (DATA.has(head)) {
    // Search preprocessors execute their argv. Never grant either attached or separated spelling.
    return !["rg", "grep"].includes(head) || !words.some(word => /^--pre(?:-glob)?(?:=|$)/.test(word.text));
  }
  // No configurable aliases, pager/exec flags, scripts or arbitrary subcommands receive DATA credit.
  return head === "git" && words.length === 2 && ["log", "status", "diff"].includes(words[1]!.text);
}

function literalKill(words: Word[]): boolean {
  if (words[0]?.text !== "kill" || words.some(word => !word.literal)) return false;
  const args = words.slice(1).map(word => word.text);
  if (/^-(?:[A-Z][A-Z0-9]*|[0-9]+)$/.test(args[0] ?? "")) args.shift();
  if (args[0] === "--") args.shift();
  return args.length > 0 && args.every(arg => /^[1-9][0-9]*$/.test(arg));
}

/** Lexical containment, not filesystem ownership: the session's TMPDIR is trusted host input. */
function ownPath(path: string, tmpdir: string | undefined): boolean {
  if (!tmpdir || !tmpdir.startsWith("/") || /[$`*?\[\]{}~\r\n]/.test(tmpdir)) return false;
  if (!path.startsWith("/") || /[$`*?\[\]{}~\r\n]/.test(path)) return false;
  if (tmpdir.split("/").includes("..") || path.split("/").includes("..")) return false;
  const root = posix.normalize(tmpdir).replace(/\/$/, "");
  // A shared temp root is not a private session TMPDIR; equality never grants its own removal.
  if (["", "/tmp", "/var/tmp", "/private/tmp", "/private/var/tmp"].includes(root)) return false;
  return posix.normalize(path).startsWith(`${root}/`) && posix.normalize(path) !== `${root}/`;
}

function literalRm(words: Word[], tmpdir: string | undefined): boolean {
  if (words[0]?.text !== "rm" || words.some(word => !word.literal)) return false;
  let index = 1, recursive = false;
  while (index < words.length && words[index]!.text.startsWith("-")) {
    const option = words[index++]!.text;
    if (option === "--") break;
    if (option === "--recursive") recursive = true;
    else if (option === "--force") { /* allowed quiet option */ }
    else if (/^-[rRf]+$/.test(option)) recursive ||= /[rR]/.test(option);
    else return false;
  }
  return recursive && index < words.length && words.slice(index).every(word => ownPath(word.text, tmpdir));
}

/**
 * Scope cut from PR166: only one literal kill or recursive rm command can receive an allowance.
 * Outside the fixed inert-head grant, visible protected tokens and opaque execution refuse.
 * No safety bytes, executor semantics or parent effects are inferred. This is a mistake guard,
 * not a sandbox: aliases, custom script files, other languages and dynamic names are not proved.
 */
export function bashGuardRefusal(command: string, tmpdir: string | undefined): string | undefined {
  const { words, simple } = split(command);
  if (literalData(words, simple) || (simple && literalKill(words)) || (simple && literalRm(words, tmpdir))) return undefined;
  const raw = protectedTokens(command);
  let signal = raw.signal, deletion = raw.deletion;
  for (const word of words) {
    const found = protectedTokens(word.text);
    signal ||= found.signal; deletion ||= found.deletion;
  }
  // No executor denylist: live substitutions/process substitutions/backticks and unsupported
  // script quoting cannot prove absence of a fragmented protected receiver. Do not decode them.
  const execution = /\$\(|`|[<>]\(|\$['"]|\\\r?\n|<</.test(command);
  const unprovedQuoting = words.some(word => /['"\\]/.test(word.text));
  if (signal) return SIGNAL_REASON;
  if (deletion) return DELETE_REASON;
  if (execution || unprovedQuoting) return SIGNAL_REASON;
  // No visible protected token or opaque execution. This is not an unrelated-command DATA grant.
  return undefined;
}
