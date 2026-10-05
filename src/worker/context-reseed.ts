import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { readPiSessionHeader } from "../core/pi-session-header.js";

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

/** Oversized RPC histories need a bounded local fallback; never read the whole journal. */
function recentJournalState(file: string): string {
  const fd = fs.openSync(file, "r");
  try {
    const size = fs.fstatSync(fd).size;
    const offset = Math.max(0, size - 65_536);
    const bytes = Buffer.alloc(size - offset);
    fs.readSync(fd, bytes, 0, bytes.length, offset);
    const lines = bytes.toString("utf8").split("\n");
    if (offset) lines.shift(); // partial leading record (possibly a giant tool output)
    const messages: unknown[] = [];
    for (const line of lines) {
      try {
        const entry = record(JSON.parse(line));
        if (entry?.type === "message") messages.push(entry.message);
        else if (entry?.type === "compaction") messages.push({ role: "compactionSummary", summary: entry.summary });
      } catch { /* partial/invalid records remain in the archive, not in model context */ }
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
  const candidate = `${file}.${sessionId}.tmp`;
  try {
    // Copy, don't move: registered readers always see a complete old or new file.
    fs.copyFileSync(file, archived, fs.constants.COPYFILE_EXCL);
    fs.chmodSync(archived, 0o600);
    const archiveFd = fs.openSync(archived, "r");
    try { fs.fsyncSync(archiveFd); } finally { fs.closeSync(archiveFd); }
    const fd = fs.openSync(candidate, "wx", 0o600);
    try {
      fs.writeFileSync(fd, entries.map(entry => JSON.stringify(entry)).join("\n") + "\n");
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
    fs.renameSync(candidate, file); // header, seed and durable attribution note publish together
    if (process.platform !== "win32") {
      const directory = fs.openSync(path.dirname(file), "r");
      try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    }
  } finally { if (fs.existsSync(candidate)) fs.unlinkSync(candidate); }
  return { oldSessionId: header.id, sessionId, archived };
}
