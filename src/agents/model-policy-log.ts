import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/** Model identifiers, not arbitrary substrings; also cover native Claude runners. */
export function isClaudeModel(model: string | undefined, runner?: string): boolean {
  return runner?.toLowerCase() === "claude" ||
    (typeof model === "string" && /(?:^|[/:._-])(?:claude|opus)(?=$|[/:._-])/i.test(model));
}

/** A reason alone grants nothing: callers must require an actual issue reference. */
export function opusIssueReference(reason: unknown): string | undefined {
  if (typeof reason !== "string") return undefined;
  // Consume whole URLs so an arbitrary URL's #123 fragment cannot grant an exception.
  const candidates = /https?:\/\/[^\s<>"']+|(?:^|[\s([])(#[1-9]\d*)\b/gi;
  for (const match of reason.matchAll(candidates)) {
    if (match[1]) return match[1];
    const candidate = match[0].replace(/[.,;:)\]}]+$/, "");
    try {
      const url = new URL(candidate);
      if (/^https?:$/.test(url.protocol) && /\/issues\/[1-9]\d*\/?$/.test(url.pathname)) return candidate;
    } catch { /* Malformed URLs are not issue references. */ }
  }
  return undefined;
}

/** Best-effort host-local audit: one append write, no retries or global output. */
export function appendOpusPolicyLog(input: {
  role: string; model: string; ref: string | null; reason: string;
}): string | undefined {
  let fd: number | undefined;
  let warning: string | undefined;
  try {
    const directory = path.join(os.homedir(), ".local", "state", "smarty", "route");
    const line = Buffer.from(`${JSON.stringify({
      ts: new Date().toISOString(), role: input.role, model: input.model,
      ref: input.ref, reason: input.reason, launcher: "fabric", host: os.hostname(),
    })}\n`, "utf8");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND |
      fs.constants.O_NONBLOCK | (fs.constants.O_NOFOLLOW ?? 0);
    fd = fs.openSync(path.join(directory, "opus-exceptions.jsonl"), flags, 0o600);
    if (fs.writeSync(fd, line) !== line.length) warning = "Opus policy audit log short write; exception not fully recorded.";
  } catch {
    warning = "Opus policy audit log failed; exception not fully recorded.";
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); }
      catch { warning ??= "Opus policy audit log close failed; recording not confirmed."; }
    }
  }
  return warning;
}
