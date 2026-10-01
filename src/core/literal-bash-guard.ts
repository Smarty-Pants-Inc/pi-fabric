import { posix } from "node:path";

export const SIGNAL_REASON = "Signal refused: use the PID you recorded (literal integer PIDs only).";
export const DELETE_REASON = "Recursive delete refused: delete only inside your own TMPDIR using literal absolute paths.";

type Word = { text: string; literal: boolean };
type Split = { words: Word[]; simple: boolean };
const SIGNALS = new Set(["kill", "pkill", "killall", "killall5"]);
const DATA = new Set(["echo", "printf", "grep", "rg"]);
// These are grammar/execution prefixes, never proof that following words are DATA.
const RESERVED = new Set(["!", "if", "then", "elif", "else", "fi", "for", "while", "until", "do", "done", "case", "in", "esac", "select", "coproc", "function", "time", "{", "}"]);
const EXECUTORS = new Set(["sudo", "doas", "env", "command", "builtin", "exec", "nice", "ionice", "timeout", "time", "nohup", "setsid", "stdbuf", "xargs", "bash", "sh", "zsh", "dash", "ksh", "ssh", "eval", "source", ".", "find"]);
const basename = (word: string): string => word.slice(word.lastIndexOf("/") + 1);

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

// An opaque script/capture mentioning a protected operation is NOT interpreted or admitted.
const SIGNAL_TEXT = /(?:^|[\s;|&()<>"'`=]|-[A-Za-z]*S)(?:\/[\w./-]+\/)?(?:kill|pkill|killall|killall5)(?=$|[\s;|&()<>"'`])/;
const DELETE_TEXT = /(?:^|[\s;|&()<>"'`=]|-[A-Za-z]*S)(?:\/[\w./-]+\/)?(?:rm|find|shred)(?=$|[\s;|&()<>"'`])/;

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
 * Complex/wrapped/nested protected forms refuse; no safety bytes or parent effects are inferred.
 * This is a mistake guard, not a sandbox: aliases, script files, other languages and dynamically
 * selected command names are outside its lexical scope. Non-protected commands pass unchanged.
 */
export function bashGuardRefusal(command: string, tmpdir: string | undefined): string | undefined {
  const { words, simple } = split(command);
  const name = basename(words[0]?.text ?? "");
  const commandHead = words[0]?.literal && /^[A-Za-z_./][A-Za-z0-9_./+-]*$/.test(words[0].text) && !RESERVED.has(name);
  const preprocessor = ["rg", "grep"].includes(name) && words.some(word => /^--pre(?:-glob)?(?:=|$)/.test(word.text));
  // Literal arguments to an unrelated command are DATA, not shell code. Known command/script
  // executors are excluded; custom scripts and other languages remain outside this lexical scope.
  if (simple && commandHead && !preprocessor && DATA.has(name) && words.every(word => !/[$`]/.test(word.text))) return undefined;
  if (simple && commandHead && !preprocessor && !words[0]!.text.includes("=") && !SIGNALS.has(name) && !["rm", "shred"].includes(name) &&
      !EXECUTORS.has(name) && words.every(word => word.literal)) return undefined;
  // Opaque execution never gets an absence-of-protected-code proof from unsupported quoting.
  // Do not decode ANSI-C/locale strings or re-lex an inline script's escaped/concatenated name.
  const rawSignal = /\b(?:kill|pkill|killall|killall5)\b/.test(command);
  const rawDelete = /\b(?:rm|find|shred|xargs)\b/.test(command);
  const unsupported = /\$['"]|\\\r?\n|<</.test(command);
  const opaqueExecutor = preprocessor || words.some(word => EXECUTORS.has(basename(word.text)) || RESERVED.has(word.text));
  if ((unsupported && (rawSignal || rawDelete)) ||
      (opaqueExecutor && (unsupported || words.some(word => /['"\\]/.test(word.text))))) {
    return rawSignal || !rawDelete ? SIGNAL_REASON : DELETE_REASON;
  }
  const signal = words.some(word => SIGNALS.has(basename(word.text)) || SIGNAL_TEXT.test(word.text));
  if (signal) return simple && literalKill(words) ? undefined : SIGNAL_REASON;
  const rm = words.some(word => basename(word.text) === "rm");
  const find = words.some(word => basename(word.text) === "find") && words.some(word => word.text === "-delete");
  const shred = words.some(word => basename(word.text) === "shred");
  // With unproved argv/syntax, a delete option or operand may become the recursive selector.
  const recursive = words.some(word => /^-[a-zA-Z]*[rR][a-zA-Z]*$/.test(word.text) || word.text.startsWith("--recursive"));
  const remove = words.some(word => /^-[a-zA-Z]*u[a-zA-Z]*$/.test(word.text) || /^--remove(?:=|$)/.test(word.text));
  const uncertain = !simple || words.some(word => !word.literal);
  const opaque = words.some(word => DELETE_TEXT.test(word.text)) &&
    (recursive || uncertain || words.some(word => /(?:[$`]|-[a-zA-Z]*[rRu]|--recursive|--remove|-delete)/.test(word.text)));
  if (find || (shred && (recursive || uncertain || remove)) ||
      (rm && (recursive || uncertain)) || opaque) {
    return simple && literalRm(words, tmpdir) ? undefined : DELETE_REASON;
  }
  return undefined;
}
