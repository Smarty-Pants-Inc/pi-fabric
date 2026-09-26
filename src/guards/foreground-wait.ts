// smarty-dev#854: agents blocked their own sessions in foreground waits (a 70-minute
// `for … seq 1 14; sleep 300` loop). A blocked session takes no steer or ask and reads as
// working, so the stall is invisible. This refuses a foreground bash command whose sleeps add up
// to more than FOREGROUND_WAIT_LIMIT_S.
// ponytail: a small shell reader, not a shell. It follows lists, pipelines, background jobs and
// `wait`, subshells and groups, `for` (counted by its word list, `seq`, `{a..b}` or a C-style
// header), `while`/`until` (unbounded when they sleep), `if`, `timeout N` (bounding its own
// command), foreground wrappers (nohup, setsid, env, nice, command, exec, time) and `bash -c`
// scripts. Heredoc bodies, quoted arguments and comments are data. A wait hidden in a script file
// or a program passes; an unknown loop list counts once. Revisit if agents route around it.

export const FOREGROUND_WAIT_LIMIT_S = 300;

type Token = { op: string } | { word: string; quoted?: string };

const UNIT_SECONDS: Record<string, number> = { "": 1, s: 1, m: 60, h: 3_600, d: 86_400 };
const DURATION = /^(\d+(?:\.\d+)?)([smhd]?)$/;
const duration = (word: string): number | undefined => {
  const match = DURATION.exec(word);
  return match ? Number(match[1]) * (UNIT_SECONDS[match[2]!] ?? 1) : undefined;
};

const tokenize = (text: string): Token[] => {
  const tokens: Token[] = [];
  const heredocs: string[] = [];
  let index = 0;
  const skipHeredocBodies = (): void => {
    for (const delimiter of heredocs.splice(0)) {
      while (index < text.length) {
        const end = text.indexOf("\n", index);
        const line = text.slice(index, end < 0 ? text.length : end);
        index = end < 0 ? text.length : end + 1;
        if (line.trim() === delimiter) break;
      }
    }
  };
  while (index < text.length) {
    const char = text[index]!;
    const next = text[index + 1];
    if (char === " " || char === "\t" || char === "\r") { index += 1; continue; }
    if (char === "\\" && next === "\n") { index += 2; continue; }
    if (char === "#") { while (index < text.length && text[index] !== "\n") index += 1; continue; }
    if (char === "\n") { tokens.push({ op: "\n" }); index += 1; skipHeredocBodies(); continue; }
    if (char === ";") { tokens.push({ op: next === ";" ? ";;" : ";" }); index += next === ";" ? 2 : 1; continue; }
    if (char === "&" && next !== ">") { tokens.push({ op: next === "&" ? "&&" : "&" }); index += next === "&" ? 2 : 1; continue; }
    if (char === "|") { tokens.push({ op: next === "|" ? "||" : "|" }); index += next === "|" || next === "&" ? 2 : 1; continue; }
    if (char === ")") { tokens.push({ op: ")" }); index += 1; continue; }
    if (char === "(" && next !== "(") { tokens.push({ op: "(" }); index += 1; continue; }
    if (char === "<" && next === "<" && text[index + 2] !== "<") {
      index += text[index + 2] === "-" ? 3 : 2;
      while (text[index] === " " || text[index] === "\t") index += 1;
      let delimiter = "";
      while (index < text.length && !/[\s;&|()<>]/.test(text[index]!)) delimiter += text[index++];
      heredocs.push(delimiter.replace(/['"\\]/g, ""));
      continue;
    }
    // A word: quotes, $(…), `…`, ${…} and ((…)) nest; redirections such as 2>&1 stay in the word.
    let word = "";
    let quoted: string | undefined;
    let parts = 0;
    while (index < text.length) {
      const c = text[index]!;
      if (/[\s;|)]/.test(c) || (c === "&" && text[index + 1] !== ">" && !/[<>]$/.test(word)) || (c === "(" && word === "" && text[index + 1] !== "(")) break;
      parts += 1;
      if (c === "'" || c === "\"") {
        const start = index + 1;
        index += 1;
        while (index < text.length && text[index] !== c) index += c === "\"" && text[index] === "\\" ? 2 : 1;
        const body = text.slice(start, Math.min(index, text.length));
        word += body;
        quoted = parts === 1 ? body : undefined;
        index += 1;
        continue;
      }
      if ((c === "$" && (text[index + 1] === "(" || text[index + 1] === "{")) || c === "`" || (c === "(" && text[index + 1] === "(")) {
        const open = c === "`" ? "`" : c === "(" ? "(" : text[index + 1]!;
        const close = open === "`" ? "`" : open === "(" ? ")" : "}";
        const start = index;
        index += c === "$" ? 2 : 1;
        let depth = 1;
        while (index < text.length && depth > 0) {
          const d = text[index]!;
          if (d === "\\") { index += 2; continue; }
          if (open !== "`" && d === open) depth += 1;
          else if (d === close) depth -= 1;
          index += 1;
        }
        word += text.slice(start, index);
        quoted = undefined;
        continue;
      }
      if (c === "\\") { word += text.slice(index, index + 2); index += 2; continue; }
      word += c;
      quoted = undefined;
      index += 1;
    }
    if (word !== "" || parts > 0) tokens.push({ word, ...(quoted !== undefined && parts === 1 ? { quoted } : {}) });
    else index += 1;
  }
  return tokens;
};

const WRAPPERS = new Set(["nohup", "setsid", "command", "exec", "time", "builtin"]);
const SHELLS = new Set(["bash", "sh", "zsh", "dash", "ksh"]);
const LIST_END = new Set(["do", "done", "then", "elif", "else", "fi", "esac", "}"]);

class WaitEstimator {
  #tokens: Token[];
  #index = 0;
  #background = 0;

  constructor(tokens: Token[]) {
    this.#tokens = tokens;
  }

  static script(text: string): number {
    return new WaitEstimator(tokenize(text)).list(new Set());
  }

  #peek(): Token | undefined {
    return this.#tokens[this.#index];
  }

  #word(): string | undefined {
    const token = this.#peek();
    return token && "word" in token ? token.word : undefined;
  }

  #op(): string | undefined {
    const token = this.#peek();
    return token && "op" in token ? token.op : undefined;
  }

  #skipSeparators(): void {
    while (this.#op() === "\n" || this.#op() === ";") this.#index += 1;
  }

  #expect(word: string): void {
    this.#skipSeparators();
    if (this.#word() === word) this.#index += 1;
  }

  /** Commands until a closing keyword (or `)` in a subshell): sequences add up. */
  list(ends: ReadonlySet<string>, closeParen = false): number {
    let total = 0;
    while (this.#index < this.#tokens.length) {
      this.#skipSeparators();
      const word = this.#word();
      if (word !== undefined && ends.has(word)) break;
      if (closeParen && this.#op() === ")") break;
      if (this.#index >= this.#tokens.length) break;
      const before = this.#index;
      const seconds = this.#andOr(ends);
      if (this.#op() === "&") {
        this.#index += 1;
        this.#background = Math.max(this.#background, seconds);
      } else {
        total += seconds;
      }
      if (this.#index === before) this.#index += 1;             // never stall on a stray token
    }
    return total;
  }

  #andOr(ends: ReadonlySet<string>): number {
    let total = this.#pipeline(ends);
    while (this.#op() === "&&" || this.#op() === "||") {
      this.#index += 1;
      this.#skipSeparators();
      total += this.#pipeline(ends);
    }
    return total;
  }

  #pipeline(ends: ReadonlySet<string>): number {
    let longest = this.#command(ends);
    while (this.#op() === "|") {
      this.#index += 1;
      this.#skipSeparators();
      longest = Math.max(longest, this.#command(ends));
    }
    return longest;
  }

  #command(ends: ReadonlySet<string>): number {
    if (this.#op() === "(") {
      this.#index += 1;
      const seconds = this.list(new Set(), true);
      if (this.#op() === ")") this.#index += 1;
      return seconds;
    }
    const word = this.#word();
    if (word === undefined || ends.has(word)) return 0;
    if (word === "{") {
      this.#index += 1;
      const seconds = this.list(new Set(["}"]));
      this.#expect("}");
      return seconds;
    }
    if (word === "for") return this.#for();
    if (word === "while" || word === "until") {
      this.#index += 1;
      const condition = this.list(new Set(["do"]));
      this.#expect("do");
      const body = this.list(new Set(["done"]));
      this.#expect("done");
      return condition + body > 0 ? Number.POSITIVE_INFINITY : 0;
    }
    if (word === "if") {
      this.#index += 1;
      let total = this.list(new Set(["then"]));
      let branch = 0;
      while (this.#index < this.#tokens.length) {
        this.#expect(this.#word() === "elif" ? "elif" : "then");
        branch = Math.max(branch, this.list(new Set(["elif", "else", "fi"])));
        this.#skipSeparators();
        const next = this.#word();
        if (next === "elif") { this.#index += 1; total += this.list(new Set(["then"])); continue; }
        if (next === "else") { this.#index += 1; branch = Math.max(branch, this.list(new Set(["fi"]))); }
        break;
      }
      this.#expect("fi");
      return total + branch;
    }
    if (word === "case") {
      while (this.#index < this.#tokens.length && this.#word() !== "esac") this.#index += 1;
      this.#index += 1;
      return 0;
    }
    const words: Token[] = [];
    while (this.#index < this.#tokens.length && this.#word() !== undefined) words.push(this.#tokens[this.#index++]!);
    return this.#simple(words);
  }

  #for(): number {
    this.#index += 1;
    const header = this.#word() ?? "";
    let count = 1;
    if (header.startsWith("((")) {
      count = cStyleIterations(header);
      this.#index += 1;
    } else {
      this.#index += 1;                                        // the loop variable
      if (this.#word() === "in") {
        this.#index += 1;
        const items: string[] = [];
        while (this.#word() !== undefined) items.push(this.#word()!), this.#index += 1;
        count = items.reduce((sum, item) => sum + loopItems(item), 0);
      }
    }
    this.#expect("do");
    const body = this.list(new Set(["done"]));
    this.#expect("done");
    return body * count;
  }

  #simple(tokens: Token[]): number {
    let rest = tokens.filter((token) => "word" in token && !/^[<>0-9&]*[<>]/.test(token.word)) as Array<{ word: string; quoted?: string }>;
    while (rest.length > 0 && /^\w+=/.test(rest[0]!.word)) rest = rest.slice(1);
    const name = rest[0]?.word;
    if (name === undefined) return 0;
    const args = rest.slice(1);
    if (name === "sleep") return args.reduce((sum, arg) => sum + (duration(arg.word) ?? 0), 0);
    if (name === "wait") {
      const waited = this.#background;
      this.#background = 0;
      return waited;
    }
    if (name === "timeout") {
      let at = 0;
      while (at < args.length && args[at]!.word.startsWith("-")) at += /^-(k|s)$|^--(kill-after|signal)$/.test(args[at]!.word) ? 2 : 1;
      const limit = duration(args[at]?.word ?? "");
      const inner = this.#simple(args.slice(at + 1));
      // `timeout 0` disables the deadline.
      return limit === undefined || limit === 0 ? inner : Math.min(limit, inner);
    }
    if (WRAPPERS.has(name)) return this.#simple(args);
    if (name === "env" || name === "nice" || name === "ionice" || name === "stdbuf") {
      let at = 0;
      while (at < args.length && (args[at]!.word.startsWith("-") || /^\w+=/.test(args[at]!.word))) {
        at += /^-(n|c|u)$/.test(args[at]!.word) ? 2 : 1;
      }
      return this.#simple(args.slice(at));
    }
    if (SHELLS.has(name.split("/").at(-1)!)) {
      const flag = args.findIndex((arg) => /^-[a-z]*c[a-z]*$/.test(arg.word));
      const script = flag >= 0 ? args[flag + 1] : undefined;
      if (script) return WaitEstimator.script(script.quoted ?? script.word);
    }
    return 0;
  }
}

// Iterations from `first` stepping by `step` while within `last` (inclusive); 0 for an empty
// range, Infinity for a step that never reaches it.
const steps = (first: number, step: number, last: number): number => {
  if (step === 0) return first === last ? Number.POSITIVE_INFINITY : 0;
  if ((step > 0 && first > last) || (step < 0 && first < last)) return 0;
  return Math.floor((last - first) / step) + 1;
};

// How many items a `for … in` word yields: seq and brace ranges are counted, other words once.
const loopItems = (item: string): number => {
  const seq = /^\$\(\s*seq\s+(-?\d+)(?:\s+(-?\d+))?(?:\s+(-?\d+))?\s*\)$/.exec(item);
  if (seq) {
    const [, a, b, c] = seq;
    if (c !== undefined) return steps(Number(a), Number(b), Number(c));      // seq FIRST INCREMENT LAST
    if (b !== undefined) return steps(Number(a), 1, Number(b));              // seq FIRST LAST
    return steps(1, 1, Number(a));                                           // seq LAST
  }
  const range = /^\{(-?\d+)\.\.(-?\d+)(?:\.\.(-?\d+))?\}$/.exec(item);
  if (range) {
    const [, a, b, s] = range;
    const step = Math.abs(Number(s ?? 1)) || 1;
    return steps(Number(a), Number(b) >= Number(a) ? step : -step, Number(b));
  }
  return 1;
};

// A C-style header `((v=A; v OP B; STEP))`: its iterations, Infinity when it never ends, and 1
// when a part is not a literal.
const cStyleIterations = (header: string): number => {
  const parts = header.replace(/^\(\(|\)\)$/g, "").split(";").map((part) => part.trim());
  if (parts.length !== 3) return 1;
  const [init, condition, update] = parts as [string, string, string];
  if (condition === "") return Number.POSITIVE_INFINITY;
  const start = /^(\w+)\s*=\s*(-?\d+)$/.exec(init);
  const test = /^(\w+)\s*(<=|<|>=|>)\s*(-?\d+)$/.exec(condition);
  if (!start || !test || test[1] !== start[1]) return 1;
  const name = start[1]!;
  const step = new RegExp(`^(?:${name}\\+\\+|\\+\\+${name})$`).test(update) ? 1
    : new RegExp(`^(?:${name}--|--${name})$`).test(update) ? -1
    : (() => {
      const compound = new RegExp(`^${name}\\s*([+-])=\\s*(\\d+)$`).exec(update)
        ?? new RegExp(`^${name}\\s*=\\s*${name}\\s*([+-])\\s*(\\d+)$`).exec(update);
      return compound ? (compound[1] === "-" ? -1 : 1) * Number(compound[2]) : undefined;
    })();
  if (step === undefined) return 1;
  const first = Number(start[2]);
  const bound = Number(test[3]);
  const operator = test[2]!;
  const last = operator === "<" ? bound - 1 : operator === ">" ? bound + 1 : bound;
  const rising = operator.startsWith("<");
  if ((rising && step <= 0) || (!rising && step >= 0)) {
    return (rising ? first <= last : first >= last) ? Number.POSITIVE_INFINITY : 0;
  }
  return steps(first, step, last);
};

/**
 * The foreground wait a bash command is expected to make, in seconds (Infinity for a sleep in an
 * unbounded loop). The tool call's own timeout bounds it.
 */
export const foregroundWaitSeconds = (command: string, toolTimeoutS?: number): number => {
  const seconds = WaitEstimator.script(command);
  return toolTimeoutS !== undefined && toolTimeoutS > 0 ? Math.min(seconds, toolTimeoutS) : seconds;
};

/** A refusal for a foreground wait over the limit, or undefined to let the command run. */
export const foregroundWaitRefusal = (command: string, toolTimeoutS?: number): string | undefined => {
  const seconds = foregroundWaitSeconds(command, toolTimeoutS);
  if (seconds <= FOREGROUND_WAIT_LIMIT_S) return undefined;
  const size = Number.isFinite(seconds) ? `about ${Math.round(seconds / 6) / 10} min` : "an unbounded wait";
  return `Fabric refused this command: its foreground wait is ${size} (limit ${FOREGROUND_WAIT_LIMIT_S / 60} min, ` +
    "smarty-dev#854). A session blocked in a wait takes no steer or ask and shows as working. Instead, start the " +
    "poll detached (setsid or nohup … &) and end your turn, wait for a mesh github.* event or an agent's " +
    "completion message, or bound a short wait with `timeout 300`.";
};
