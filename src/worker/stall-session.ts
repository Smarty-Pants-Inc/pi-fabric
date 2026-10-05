import fs from "node:fs";
import path from "node:path";
import { parsePiSessionHeader } from "../core/pi-session-header.js";

type Entry = { id: string; parentId: string | null; type: string; message?: Record<string, unknown> };

/** Read only after the owned native writer has closed/drained. Pi may not create
 * a new session until abort finalizes its first assistant message. Never let its
 * permissive session loader turn missing/truncated history into a fresh task.
 */
export const stalledSessionResumeError = async (
  file: string, cwd: string, model: string, sessionId?: string,
): Promise<string | undefined> => {
  const input = fs.createReadStream(file, { encoding: "utf8" });
  // Native Pi writes JSON.stringify(entry) + LF. readline also treats literal
  // U+2028/U+2029 inside valid JSON strings as delimiters, corrupting history.
  const lines = (async function* () {
    let pending = "";
    for await (const chunk of input) {
      pending += chunk;
      let start = 0;
      let end: number;
      while ((end = pending.indexOf("\n", start)) !== -1) {
        const line = pending.slice(start, end);
        yield line.endsWith("\r") ? line.slice(0, -1) : line;
        start = end + 1;
      }
      pending = pending.slice(start);
    }
    if (pending) yield pending.endsWith("\r") ? pending.slice(0, -1) : pending;
  })();
  try {
    let headerSeen = false;
    const entries = new Map<string, Entry>();
    let leaf: Entry | undefined;
    for await (const line of lines) {
      if (!headerSeen) {
        const header = parsePiSessionHeader(line);
        if (!header || path.resolve(header.cwd) !== path.resolve(cwd) ||
            (sessionId !== undefined && header.id !== sessionId)) return "invalid or mismatched native session header";
        headerSeen = true;
        continue;
      }
      const value: unknown = JSON.parse(line);
      if (!value || typeof value !== "object" || Array.isArray(value)) return "invalid native session entry";
      const entry = value as Entry;
      if (typeof entry.id !== "string" || !entry.id || entries.has(entry.id) ||
          typeof entry.type !== "string" || entry.type === "session" ||
          (entry.parentId !== null && (typeof entry.parentId !== "string" || !entries.has(entry.parentId)))) {
        return "invalid native session entry chain";
      }
      if (entry.type === "message" && (!entry.message || typeof entry.message.role !== "string")) return "invalid native session message";
      entries.set(entry.id, entry);
      leaf = entry;
    }
    if (!headerSeen) return "missing native session header";
    let lastAssistant: Record<string, unknown> | undefined;
    let hasUser = false;
    for (let entry = leaf; entry; entry = entry.parentId === null ? undefined : entries.get(entry.parentId)) {
      if (entry.type !== "message") continue;
      if (!lastAssistant && entry.message?.role === "assistant") lastAssistant = entry.message;
      if (entry.message?.role === "user") hasUser = true;
    }
    if (!hasUser || !lastAssistant || lastAssistant.stopReason !== "aborted" ||
        `${String(lastAssistant.provider)}/${String(lastAssistant.model)}` !== model) {
      return "native session lacks preserved user context and the admitted aborted assistant";
    }
    return undefined;
  } catch (error) {
    return `native session unavailable or invalid: ${(error as Error).message}`;
  } finally {
    input.destroy();
  }
};
