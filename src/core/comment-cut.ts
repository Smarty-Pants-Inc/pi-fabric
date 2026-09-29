// smarty-dev#1469: a supervisor that read only the last N comments of an issue steered its lead on a
// false premise (the codex case on #1201): the owner's answer was an earlier comment. The role says
// never cut the list, but runs still did, so a directive actor's bash call that cuts a comment list
// is blocked. Ported from fabric-v2's census (.local/census-1469.py: `comment_reads` and `CUT`).

// A comment LIST of one issue or PR; `/comments/<id>` is one comment and is not a list.
const COMMENT_LIST = /\/(?:issues|pulls)\/[^/\s'"]+\/comments/g;

// The read is its own logical command: from its `gh api` (options can precede the endpoint) or the
// previous shell command, to the end of the line, the next `gh api`, or the next shell command
// (`; cmd`, `&& cmd`). A ';' inside a jq string has no space and letter after it.
const NEXT_COMMAND = /;\s+(?=[A-Za-z$(])|&&/;
const NEXT_COMMAND_ALL = new RegExp(NEXT_COMMAND.source, "g");

const CUT = new RegExp([
  // `| tail`, `| head`, `| sed -n`; `tail -n +1` and `tail +1` keep every line.
  String.raw`\|\s*(?:tail(?!\s+(?:-n\s*|--lines[=\s]\s*)?\+1\b)|head|sed\s+-n)\b`,
  // smarty-dev#967 round-5 census: an awk that keeps a subset of lines (`END{print}` keeps the last one,
  // `NR<=N` / `NR>N` keep a range). A plain field filter such as `awk -F: '$1>0'` keeps every comment.
  String.raw`\|\s*awk\b[^|]*(?:\bEND\b|\bNR\s*(?:[<>]=?|==|!=))`,
  // jq negative slice `.[-N:]` and negative index `.[-N]`.
  String.raw`\.\[\s*-\d*\s*:\s*-?\d*\s*\]`,
  String.raw`\.\[\s*-\d+\s*\]`,
  // A top-level slice `'.[0:N]'`; `.body[0:200]` or `| .[0:200]` inside the object is a string slice.
  String.raw`(?:'|"|--jq\s+)\s*\.\[\s*\d*\s*:\s*\d+\s*\]`,
  // jq `last` / `first` as filters (not an object key such as `first: (...)`), and `limit(`.
  String.raw`(?:[|'"]|--jq\s)\s*(?:last|first)\b(?!"?\s*:)`,
  String.raw`\b(?:last|first|limit)\s*\(`,
].join("|"));

/** The parts of a shell command that read an issue or PR comment list. */
export function commentListReads(command: string): string[] {
  const reads: string[] = [];
  // review/astra F1 on #104: a backslash-newline or a newline after `|` continues the same pipeline.
  const text = command.replace(/\\\r?\n/g, " ").replace(/\|[ \t]*\r?\n/g, "| ");
  for (const match of text.matchAll(COMMENT_LIST)) {
    const before = text.slice(0, match.index!);
    const previous = [...before.matchAll(NEXT_COMMAND_ALL)].at(-1);
    const start = Math.max(
      before.lastIndexOf("\n") + 1, before.lastIndexOf("gh api"), previous ? previous.index + previous[0].length : 0,
    );
    const rest = text.slice(match.index! + match[0].length);
    const next = NEXT_COMMAND.exec(rest);
    const stops = [rest.indexOf("\n"), rest.indexOf("gh api"), next ? next.index : -1].filter((index) => index >= 0);
    const end = match.index! + match[0].length + (stops.length > 0 ? Math.min(...stops) : rest.length);
    reads.push(text.slice(start, end));
  }
  return reads;
}

/** True when the command reads an issue or PR comment list and keeps only part of it. */
export function cutsCommentList(command: string): boolean {
  return commentListReads(command).some((read) => CUT.test(read));
}

export const COMMENT_CUT_REASON =
  "Blocked (smarty-dev#1469): this command cuts an issue or PR comment list (tail, head, a slice, " +
  "last or first), but the owner's answer can be any comment. Read the whole list with " +
  `--jq '.[] | {id, user: .user.login, created_at, first: (.body | split("\\n")[0])}' and no cut, ` +
  "then read the full bodies you need.";
