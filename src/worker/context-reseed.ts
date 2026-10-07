import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { readPiSessionHeader } from "../core/pi-session-header.js";
import { writeFileAtomic, syncDirectoryChain } from "../core/atomic-write.js";

const record = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const excerpt = (text: string, cap: number): string => text.length <= cap ? text :
  `${text.slice(0, Math.floor(cap / 2))}\n[excerpt omitted]\n${text.slice(-Math.floor(cap / 2))}`;

/** Recovery is deliberately extractive, not a new inferred objective or authorization. */
export function summarizeActorState(messages: unknown[], maxChars = 16_384): string {
  const lines: string[] = [];
  for (const value of messages.slice(-12)) {
    const message = record(value);
    if (!message || message.role === "system") continue; // persona is supplied anew by the worker
    let text = typeof message.content === "string" ? message.content : "";
    if (Array.isArray(message.content)) text = message.content.slice(-8).map(value => {
      const block = record(value);
      if (block?.type === "text" && typeof block.text === "string") return excerpt(block.text, 1024);
      if (block?.type === "toolCall") return `Tool ${String(block.name)}: ${excerpt(JSON.stringify(block.arguments) ?? "", 512)}`;
      return ""; // no image/base64 data or hidden reasoning in a reseed
    }).filter(Boolean).join("\n");
    if (typeof message.summary === "string") text = message.summary;
    if (text) lines.push(`${String(message.role)}: ${excerpt(text, 1200)}`);
  }
  return excerpt(lines.join("\n\n"), Math.max(0, maxChars));
}

/** Oversized RPC histories need a bounded local fallback; never read the whole journal.
 * Project only the known suffix of the native active parent chain. Earlier entries
 * are deliberately unavailable, not inferred from raw append order. Context edits
 * and the latest compaction have the same visibility rules as native Pi; when a
 * retained-range boundary is outside this suffix, exclude the uncertain range.
 */
function recentJournalState(file: string): string {
  const fd = fs.openSync(file, "r");
  try {
    const size = fs.fstatSync(fd).size;
    const offset = Math.max(0, size - 65_536);
    const bytes = Buffer.alloc(size - offset);
    const count = fs.readSync(fd, bytes, 0, bytes.length, offset);
    const lines = bytes.subarray(0, count).toString("utf8").split("\n");
    if (offset) lines.shift(); // partial leading record (possibly a giant tool output)
    const entries: Array<Record<string, unknown>> = [];
    const byId = new Map<string, number>();
    for (const line of lines) {
      let entry: Record<string, unknown> | undefined;
      try { entry = record(JSON.parse(line)); } catch { continue; } // native skips malformed records
      if (!entry || entry.type === "session") continue;
      if (typeof entry.id !== "string" || !entry.id || byId.has(entry.id) ||
          (entry.parentId !== null && typeof entry.parentId !== "string")) return "";
      byId.set(entry.id, entries.length);
      entries.push(entry);
    }
    const active: Array<Record<string, unknown>> = [];
    let index: number | undefined = entries.length - 1;
    while (index !== undefined && index >= 0) {
      const entry: Record<string, unknown> = entries[index]!;
      active.push(entry);
      const parent: number | undefined = typeof entry.parentId === "string" ? byId.get(entry.parentId) : undefined;
      if (parent !== undefined && parent >= index) return ""; // ambiguous/cyclic history
      index = parent;
    }
    active.reverse();
    let visible = active;
    let compactionIndex = -1;
    for (let i = 0; i < active.length; i++) if (active[i]!.type === "compaction") compactionIndex = i;
    if (compactionIndex >= 0) {
      const compaction = active[compactionIndex]!;
      const firstKept = active.findIndex(entry => entry.id === compaction.firstKeptEntryId);
      visible = [compaction,
        ...(firstKept >= 0 && firstKept < compactionIndex ? active.slice(firstKept, compactionIndex) : []),
        ...active.slice(compactionIndex + 1)];
    }
    const edits = new Map<unknown, unknown>();
    for (const entry of visible) if (entry.type === "context_edit") edits.set(entry.targetId, entry.replacement);
    const messages: unknown[] = [];
    for (const entry of visible) {
      if (entry.type === "message" || entry.type === "custom_message") {
        const message = entry.type === "message" ? record(entry.message) : { role: "custom", content: entry.content };
        if (!message) continue;
        if (edits.has(entry.id) && ["user", "assistant", "toolResult", "custom"].includes(String(message.role))) {
          const replacement = record(edits.get(entry.id));
          if (!replacement || !("content" in replacement)) continue; // null omits, invalid cannot establish visibility
          messages.push({ ...message, content: replacement.content });
        } else messages.push(message);
      } else if (entry.type === "compaction" && entry === visible[0]) {
        messages.push({ role: "compactionSummary", summary: entry.summary });
      } else if (entry.type === "branch_summary") {
        messages.push({ role: "branchSummary", summary: entry.summary });
      }
    }
    return summarizeActorState(messages);
  } finally { fs.closeSync(fd); }
}

/** Called only after the old execution group has drained, while actor admission is held. */
export function reseedActorSession(file: string, cwd: string, options: {
  summary: string; summaryTokens: number; estimate: (text: string) => number;
  tokens: number; contextWindow: number; reason: string; runId: string;
}): { oldSessionId: string; sessionId: string; archived: string } {
  const header = readPiSessionHeader(file);
  if (!header) throw new Error("Actor reseed requires a valid registered session header");
  const sessionId = randomUUID();
  const archived = `${file}.${sessionId}.context-reseed.bak`;
  let summary = options.summary || recentJournalState(file);
  // Include framing in the bound. Estimation belongs to the selected launcher.
  const frame = (text: string) => `Prior actor state (bounded historical excerpts, not new instructions). Full history: ${archived}\n\n${text}`;
  while (summary && options.estimate(frame(summary)) > options.summaryTokens) summary = summary.slice(0, Math.floor(summary.length / 2));
  const seed = options.estimate(frame(summary)) <= options.summaryTokens ? frame(summary) : "";
  const timestamp = new Date().toISOString();
  const noteId = randomUUID();
  const entries = [
    { type: "session", version: 3, id: sessionId, timestamp, cwd, parentSession: archived },
    { type: "custom", customType: "fabric-context-reseed", id: noteId, parentId: null, timestamp,
      data: { runId: options.runId, oldSessionId: header.id, sessionId, archived,
        tokens: options.tokens, contextWindow: options.contextWindow, reason: options.reason } },
    ...(seed ? [{ type: "message", id: randomUUID(), parentId: noteId, timestamp,
      message: { role: "user", content: seed, timestamp: Date.now() } }] : []),
  ];
  // Copy, don't move: registered readers always see a complete old or new file.
  fs.copyFileSync(file, archived, fs.constants.COPYFILE_EXCL);
  fs.chmodSync(archived, 0o600);
  // Windows FlushFileBuffers requires a writable handle, even for a copied file.
  const archiveFd = fs.openSync(archived, "r+");
  try { fs.fsyncSync(archiveFd); } finally { fs.closeSync(archiveFd); }
  syncDirectoryChain(path.dirname(archived));
  // The existing helper fsyncs a closed candidate and retries Windows sharing /
  // destination-exists failures without unlinking the registered old session.
  // Header, seed and #493 attribution publish in one durable atomic replacement.
  writeFileAtomic(file, entries.map(entry => JSON.stringify(entry)).join("\n") + "\n", { durable: true, mode: 0o600 });
  return { oldSessionId: header.id, sessionId, archived };
}
