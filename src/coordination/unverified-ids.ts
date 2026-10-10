import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
type ReadonlySessionManager = ExtensionContext["sessionManager"];

/** This is a bounded provenance hint, not identifier existence/authority validation. */
export const MESSAGE_ID_LIMITS = {
  entries: 500,
  historyBytes: 2 * 1024 * 1024,
  messageBytes: 256 * 1024,
  identifiers: 128,
  contentBlocks: 2_000,
  metadataItems: 2_000,
  // Aggregate regex matches, span comparisons and candidate probes, including duplicates.
  messageMatchItems: 50_000,
  historyMatchItems: 50_000,
} as const;

type Identifier = { kind: "session" | "actor" | "sha" | "comment" | "pid" | "issue"; value: string; repository?: string; display: string; at: number };
const identifierKey = (id: Identifier): string => `${id.kind}:${id.repository ?? ""}:${id.value}`;
const uuid = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const bareSession = "01a0[0-9a-f]{4}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
const literal = new RegExp(`(?:#issuecomment-\\d{9,10}|(?<![\\w-])(?:session:${uuid}|${bareSession}|(?:actor:|run:)?[0-9a-f]{32}|comment[ \\t:=#-]*\\d{9,10}|pid[ \\t:=#-]*\\d{1,10}))(?![\\w-])`, "gi");
// SHAs (full or abbreviated) are not arbitrary prose hex: only exact inline backticks, or one
// of these words followed by whitespace, ':' or '=' (JSON key quoting allowed).
const shaWord = /\b(?:sha|commit|head|base|revision|rev)(?:["']?[ \t]*[:=][ \t]*["']?|[ \t]+)([0-9a-f]{7,40})(?![\w-])/gi;
const shaTick = /(?<!`)`([0-9a-f]{7,40})`(?!`)/gi;
const issueOwner = "[a-z0-9][a-z0-9-]*";
const issueRepository = `${issueOwner}/[a-z0-9_.-]+`;
// C# / F# language tokens are not repository-only references. Like bare #N,
// repository-only references need at least three digits to avoid prose noise.
const issueRepoOnly = "(?![cf]#)[a-z0-9][a-z0-9_.-]*";
const issueReference = new RegExp(`(?<![\\w./-])(?:https://github\\.com/(${issueRepository})/(?:issues|pull)/(\\d+)|(${issueRepository})#(\\d+)|(${issueRepoOnly})#(\\d{3,})|#(\\d{3,}))(?![\\w-])`, "gi");
// A hash-number span already parsed as a legacy class is not an issue, either
// in outgoing text or read evidence. Use the actual parser's spans so separators,
// digit limits and boundaries stay identical; qualified issue refs remain intact.
type LegacySpan = RegExpExecArray;
const spendMatch = (work: WorkBudget): void => {
  if (--work.remaining < 0) throw new Error("Match work budget exceeded");
};
const legacySpans = (text: string, work: WorkBudget): LegacySpan[] => {
  const spans: LegacySpan[] = [];
  for (const match of text.matchAll(literal)) {
    spendMatch(work);
    if (match[0].includes("#")) spans.push(match);
  }
  return spans;
};
function* issueMatches(text: string, regex: RegExp, getLegacy: () => readonly LegacySpan[], work: WorkBudget): Generator<RegExpExecArray> {
  let legacy: readonly LegacySpan[] | undefined;
  let cursor = 0;
  // Both matchAll streams are ordered and non-overlapping: each issue match
  // and each advanced span cost one unit. Never restart a full-array search.
  // The shared budget also caps repeated scans for different pending issues.
  for (const match of text.matchAll(regex)) {
    spendMatch(work);
    legacy ??= getLegacy();
    const start = match.index;
    const end = start + match[0].length;
    while (cursor < legacy.length) {
      spendMatch(work);
      const span = legacy[cursor]!;
      const spanStart = span.index;
      const spanEnd = spanStart + span[0].length;
      if (spanEnd <= start) { cursor++; continue; }
      if (start < spanStart || end > spanEnd) yield match;
      break;
    }
    if (cursor === legacy.length) yield match;
  }
}
// Read text can contain raw git / PID / JSON API output. Search only the
// outgoing candidates, not every token in a potentially multi-megabyte read.
const readMatcher = (id: Identifier): RegExp => {
  const start = "(?<![\\w-])";
  const end = "(?![\\w-])";
  let token: string;
  if (id.kind === "issue") {
    // Qualified reads preserve owner/repo; repo-only reads match that repo
    // under any owner, while bare reads have no repository identity at all.
    const escape = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const name = id.repository?.split("/").at(-1);
    const repository = id.repository?.includes("/") ? escape(id.repository)
      : name ? `${issueOwner}/${escape(name)}` : issueRepository;
    const repoOnly = id.value.length >= 3 && (!name || !/^[cf]$/i.test(name))
      ? `|${name ? escape(name) : issueRepoOnly}#` : "";
    const bare = id.value.length >= 3 ? "|#" : "";
    return new RegExp(`(?<![\\w./-])(?:https://github\\.com/${repository}/(?:issues|pull)/|${repository}#${repoOnly}${bare})${id.value}(?![\\w-])`, "gi");
  }
  if (id.kind === "session") token = `(?:session:)?${id.value}`;
  else if (id.kind === "comment") return new RegExp(`(?:#issuecomment-|${start}(?:comment[ \\t:=#-]*)?)${id.value}${end}`, "i");
  else if (id.kind === "pid") token = `(?:pid[ \\t:=#-]*)?${id.value}`;
  else if (id.kind === "actor") token = `(?:(?:actor|run):)?${id.value}`;
  else token = `${id.value.length === 32 ? "" : `(?![0-9a-f]{32}${end})`}${id.value}[0-9a-f]{0,${40 - id.value.length}}`;
  if (id.kind === "sha" && id.value.length < 32) {
    // A 32-hex token is normally an actor/run, but explicit SHA context in
    // a tool read disambiguates it just as it does in outgoing text.
    const tail = `[0-9a-f]{0,${40 - id.value.length}}`;
    const prefix = '\\b(?:sha|commit|head|base|revision|rev)(?:["\']?[ \\t]*[:=][ \\t]*["\']?|[ \\t]+)';
    return new RegExp(`${start}${token}${end}|${prefix}${id.value}${tail}${end}|(?<!\u0060)\u0060${id.value}${tail}\u0060(?!\u0060)`, "i");
  }
  return new RegExp(`${start}${token}${end}`, "i");
};

const identifier = (raw: string, at: number, sha = false): Identifier => {
  const lower = raw.toLowerCase();
  if (lower.startsWith("session:") || /^[0-9a-f]{8}-/.test(lower)) {
    return { kind: "session", value: lower.replace(/^session:/, ""), display: raw, at };
  }
  if (lower.startsWith("#issuecomment-") || lower.startsWith("comment")) {
    const value = lower.match(/\d+$/)![0];
    return { kind: "comment", value, display: `comment ${value}`, at };
  }
  if (lower.startsWith("pid")) {
    const value = lower.match(/\d+$/)![0];
    return { kind: "pid", value, display: `pid ${value}`, at };
  }
  return { kind: sha ? "sha" : "actor", value: lower.replace(/^(?:actor|run):/, ""), display: raw, at };
};

const candidates = (text: string, work: WorkBudget): Identifier[] => {
  const legacy: LegacySpan[] = [];
  const unique = new Map<string, Identifier>();
  const add = (id: Identifier): void => {
    const key = identifierKey(id);
    const previous = unique.get(key);
    if (!previous || id.at < previous.at) unique.set(key, id);
    if (unique.size > MESSAGE_ID_LIMITS.identifiers) throw new Error("Identifier budget exceeded");
  };
  const shaPositions = new Set<number>();
  for (const regex of [shaWord, shaTick]) {
    for (const match of text.matchAll(regex)) {
      spendMatch(work);
      const id = identifier(match[1]!, match.index + match[0].indexOf(match[1]!), true);
      if (id.value.length === 32) shaPositions.add(id.at);
      add(id);
    }
  }
  // An explicitly SHA-qualified 32-hex token is a SHA abbreviation, not also
  // an actor/run. Bare 32-hex tokens retain the actor/run interpretation.
  for (const match of text.matchAll(literal)) {
    spendMatch(work);
    const start = match.index;
    if (match[0].includes("#")) legacy.push(match);
    if (!shaPositions.has(start)) add(identifier(match[0], start));
  }
  // Ordinary legacy identifiers need no extra repository-pattern scan.
  if (text.includes("/") || /#\d{3}/.test(text)) {
    for (const match of issueMatches(text, issueReference, () => legacy, work)) {
      const repository = match[1] ?? match[3] ?? match[5];
      const value = match[2] ?? match[4] ?? match[6] ?? match[7]!;
      add({ kind: "issue", value, ...(repository ? { repository: repository.toLowerCase() } : {}), display: `${repository ?? ""}#${value}`, at: match.index });
    }
  }
  return [...unique.values()].sort((a, b) => a.at - b.at);
};

type Entry = { id?: string; parentId?: string | null; type?: string; customType?: string; content?: unknown; details?: unknown; message?: { role?: string; customType?: string; toolName?: string; content?: unknown; details?: unknown } };
type DeliveryDetails = { from?: { id?: string; sessionId?: string }; items?: unknown[] };
const sendRefs = new Set(["agents.send", "agents.steer", "agents.followUp", "agents.tell", "mesh.publish"]);

// Routing metadata may exclude a send-only result, but never supplies positive
// read evidence. Do not inspect nested args/results or copy the whole trace.
type WorkBudget = { remaining: number };
const spend = (budget: WorkBudget, items: number): void => {
  budget.remaining -= items;
  if (budget.remaining < 0) throw new Error("Read work budget exceeded");
};
const sendOnly = (message: NonNullable<Entry["message"]>, budget: WorkBudget): boolean => {
  if (sendRefs.has(message.toolName ?? "")) return true;
  if (message.toolName !== "fabric_exec") return false;
  const details = message.details as { trace?: { operations?: Array<{ ref?: string }> }; audits?: Array<{ ref?: string }> } | undefined;
  const operations = details?.trace?.operations ?? details?.audits;
  if (!Array.isArray(operations) || !operations.length) return false;
  spend(budget, operations.length);
  return operations.every(operation => sendRefs.has(operation?.ref ?? ""));
};

/** Remove receipt nodes, not whole mixed results. No optional YAML parser. */
const withoutReceipts = (text: string, budget: WorkBudget): string => {
  if (!text.includes("unverified ids:")) return text;
  if (/^\s*[\[{]/.test(text)) {
    let value: unknown;
    try { value = JSON.parse(text); } catch { /* Fabric's hoisted YAML can start with '[' in a path. */ }
    if (value !== undefined) {
      return JSON.stringify(value, (_key, part: unknown) => {
        spend(budget, 1);
        if (part && typeof part === "object" && "notice" in part &&
          typeof part.notice === "string" && part.notice.startsWith("unverified ids:")) return "[outgoing send receipt]";
        return part;
      });
    }
  }
  // Handle only the YAML skeleton emitted by formatFabricValue. A receipt's
  // multiline event text is hoisted into a separately labelled raw section.
  const sectionStart = text.search(/^--- .+ \(\d+ chars\) ---$/m);
  const skeleton = sectionStart < 0 ? text : text.slice(0, sectionStart);
  const lines = skeleton.split("\n");
  spend(budget, lines.length);
  const dropped = new Set<number>();
  const paths = new Set<string>();
  for (let i = 0; i < lines.length; i++) {
    const match = /^( *)(?:- )?notice: ["']?unverified ids:/.exec(lines[i]!);
    if (!match) continue;
    const indent = match[1]!.length;
    let start = i;
    while (start > 0 && (indent === 0 || /^ */.exec(lines[start - 1]!)![0].length >= indent)) start--;
    if (start > 0) start--; // Parent object key or array-item header.
    let end = i + 1;
    while (end < lines.length && (indent === 0 || !lines[end]!.trim() || /^ */.exec(lines[end]!)![0].length >= indent)) end++;
    for (let at = start; at < end; at++) {
      dropped.add(at);
      for (const ref of lines[at]!.matchAll(/<multi-line string, see section: ([^>]+)>/g)) paths.add(ref[1]!);
    }
  }
  if (!dropped.size) return text; // A separate read of marked text IS evidence.
  let result = lines.filter((_line, index) => !dropped.has(index)).join("\n");
  if (sectionStart >= 0) {
    const raw = text.slice(sectionStart);
    const sections = [...raw.matchAll(/^--- (.+) \(\d+ chars\) ---\n/gm)];
    spend(budget, sections.length);
    for (let index = 0; index < sections.length; index++) {
      const section = sections[index]!;
      if (!paths.has(section[1]!)) result += `\n${raw.slice(section.index, sections[index + 1]?.index)}`;
    }
  }
  return result;
};

/**
 * Inspect only this sender's recent current-branch reads via the host's indexed
 * leaf/getEntry API. No getEntries/getBranch copy, disk, subprocess, or network.
 * Assistant text, thinking, tool arguments, summaries and hidden details are
 * never evidence. Pi persists plain incoming steer/followUp as user messages.
 */
export function unverifiedMessageIds(text: string, session?: ReadonlySessionManager, senderId?: string): string[] {
  if (Buffer.byteLength(text) > MESSAGE_ID_LIMITS.messageBytes) throw new Error("Message budget exceeded");
  const ids = candidates(text, { remaining: MESSAGE_ID_LIMITS.messageMatchItems });
  if (!ids.length) return [];
  if (!session?.getLeafId || !session.getEntry) throw new Error("Sender history unavailable");
  const pending = new Map(ids.map(id => [identifierKey(id), id]));
  const matchers = new Map(ids.map(id => [id, readMatcher(id)]));
  // Values contain only hex, digits and UUID dashes (no regex metacharacters).
  // A single literal-alternative scan avoids 128 full scans of unrelated reads.
  const possibleRead = new RegExp(ids.map(id => id.value).join("|"), "i");
  let bytes = MESSAGE_ID_LIMITS.historyBytes as number;
  let blocks = MESSAGE_ID_LIMITS.contentBlocks as number;
  const work: WorkBudget = { remaining: MESSAGE_ID_LIMITS.metadataItems };
  const matchWork: WorkBudget = { remaining: MESSAGE_ID_LIMITS.historyMatchItems };
  const ownSession = session.getSessionId?.();
  const ownIds = new Set([senderId, ownSession ? `session:${ownSession}` : undefined].filter((id): id is string => Boolean(id)));
  const seen = new Set<string>();
  let leaf = session.getLeafId();

  const scan = (content: unknown, toolResult: boolean, batched: boolean, inbox: boolean): void => {
    const parts = typeof content === "string" ? [content] : Array.isArray(content) ? content : [];
    for (const part of parts) {
      if (bytes <= 0 || blocks-- <= 0 || !pending.size) break;
      const raw: string | undefined = typeof part === "string" ? part : part?.type === "text" && typeof part.text === "string" ? part.text : undefined;
      if (raw === undefined) continue;
      // Bound before copying/decoding. Discard an incomplete final block:
      // clipping could hide a receipt's notice or a self-envelope's closing tag,
      // or create an apparent token boundary that was never actually read.
      let bounded = raw.slice(0, bytes);
      const length = Buffer.byteLength(bounded);
      const clipped = raw.length > bytes || length > bytes;
      bytes -= Math.min(length, bytes);
      if (clipped) continue;
      if (toolResult) bounded = withoutReceipts(bounded, work);
      if (batched) bounded = bounded.replace(/<fabric-agent-message\b[^>]*>[\s\S]*?(?:<\/fabric-agent-message>|$)/g, block => {
        const from = /\bfrom_id="([^"]*)"/.exec(block)?.[1];
        return from && ownIds.has(from) ? "\n" : block;
      });
      if (inbox) bounded = bounded.replace(/<event\b(?:[^<>"\\]|"(?:\\.|[^"\\])*")*>[\s\S]*?(?:<\/event>|$)/g, block => {
        // Native rootInboxMessage has only details.ids. Authorship is in the
        // visible event header; filter each event, not the whole mixed batch.
        const from = /\bfrom_id=("(?:\\.|[^"\\])*")/.exec(block)?.[1];
        return from && ownIds.has(JSON.parse(from) as string) ? "\n" : block;
      });
      if (!possibleRead.test(bounded)) continue;
      const lower = /[A-F]/.test(bounded) ? bounded.toLowerCase() : bounded;
      // One ordered span index per readable block, lazily built only if an
      // issue matcher actually hits. Share it across all pending issue IDs.
      let legacy: LegacySpan[] | undefined;
      const getLegacy = (): LegacySpan[] => legacy ??= legacySpans(bounded, matchWork);
      for (const [key, id] of pending) {
        spendMatch(matchWork);
        // Most history blocks don't contain any candidate. Literal search avoids
        // an expensive regex scan of those blocks, even with a 2 MiB history.
        if (lower.includes(id.value) && (id.kind === "issue"
          ? !issueMatches(bounded, matchers.get(id)!, getLegacy, matchWork).next().done
          : matchers.get(id)!.test(bounded))) pending.delete(key);
      }
    }
  };

  for (let count = 0; leaf && count < MESSAGE_ID_LIMITS.entries && bytes > 0 && blocks > 0 && pending.size; count++) {
    if (seen.has(leaf)) throw new Error("Cyclic sender history");
    seen.add(leaf);
    const entry = session.getEntry(leaf) as Entry | undefined;
    if (!entry) throw new Error("Missing sender history entry");
    const message = entry.type === "message" ? entry.message : undefined;
    const readable = entry.type === "custom_message" || message?.role === "custom" || message?.role === "user" || message?.role === "toolResult";
    if (readable && !(message?.role === "toolResult" && sendOnly(message, work))) {
      const details = (message?.details ?? entry.details) as DeliveryDetails | undefined;
      const batched = Array.isArray(details?.items) && details.items.length > 1;
      // A single message sent to oneself is not independent read evidence.
      // Batched followUps filter each visible envelope, not just the first sender.
      const self = (details?.from?.id && ownIds.has(details.from.id)) || (ownSession && details?.from?.sessionId === ownSession);
      const inbox = (message?.customType ?? entry.customType) === "pi-fabric-inbox";
      if (!self || batched || inbox) scan(message ? message.content : entry.content, message?.role === "toolResult", batched, inbox);
    }
    leaf = entry.parentId ?? null;
  }
  return ids.filter(id => pending.has(identifierKey(id))).map(id => id.display);
}
