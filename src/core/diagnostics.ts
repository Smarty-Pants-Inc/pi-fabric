import fs from "node:fs";
import path from "node:path";
import { format } from "node:util";
import { resolveAgentDir } from "./agent-dir.js";

export const DIAGNOSTICS_MAX_BYTES = 1024 * 1024;
type Level = "warning" | "error" | "info";
type QueuedLine = { id: string | undefined; line: string; bytes: number };
let sessionId: string | undefined;
let pending: QueuedLine[] = [];
let pendingBytes = 0;
let draining: Promise<void> | undefined;
let exitFlushRegistered = false;

export const isPrincipalDiagnostics = (): boolean => process.env.SMARTY_ROLE?.startsWith("org-agent") === true;
export const fabricDiagnosticsFile = (id: string): string =>
  path.join(resolveAgentDir(), "fabric", "diagnostics", `${encodeURIComponent(id)}.jsonl`);

/** Shared synchronous writer for the first-use sink and the process-exit fallback. */
export const appendDiagnosticSync = (id: string, line: string, bytes: number): void => {
  let fd: number | undefined;
  try {
    const file = fabricDiagnosticsFile(id);
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    fd = fs.openSync(file, fs.constants.O_CREAT | fs.constants.O_APPEND | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.nlink !== 1) return;
    fs.fchmodSync(fd, 0o600);
    if (stat.size + bytes > DIAGNOSTICS_MAX_BYTES) {
      fs.closeSync(fd);
      fd = undefined;
      fs.renameSync(file, `${file}.1`);
      fd = fs.openSync(file, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY, 0o600);
    }
    fs.appendFileSync(fd, line);
  } catch {
    // A failed diagnostic write must not break the operation or reach the principal's pane.
  } finally {
    if (fd !== undefined) { try { fs.closeSync(fd); } catch {} }
  }
};

const drain = (): void => {
  if (draining || !pending.some(row => row.id)) return;
  // Actual first diverted diagnostic, never registration or an idle lifecycle hook.
  let loaded = false;
  draining = import("./diagnostics-log.js").then(({ appendDiagnostic }) => {
    loaded = true;
    const batch = pending;
    pending = [];
    pendingBytes = 0;
    for (const row of batch) {
      if (row.id) appendDiagnostic(row.id, row.line, row.bytes);
      else { pending.push(row); pendingBytes += row.bytes; }
    }
  }).catch(() => {
    // Keep the bounded queue for a future diagnostic to retry; never fall back to the pane.
  }).finally(() => {
    draining = undefined;
    if (loaded) drain(); // Include lines queued between the import's resolution and this continuation.
  });
};

const enqueue = (level: Level, source: string, message: string): void => {
  // JSON escaping expands each character at most sixfold, still below the file cap.
  const line = `${JSON.stringify({ at: new Date().toISOString(), level,
    source: source.slice(0, 1024), message: message.slice(0, 64 * 1024) })}\n`;
  const bytes = Buffer.byteLength(line);
  while (pending.length && pendingBytes + bytes > DIAGNOSTICS_MAX_BYTES) pendingBytes -= pending.shift()!.bytes;
  pending.push({ id: sessionId ?? (process.env.PI_FABRIC_SESSION_ID?.trim() || undefined), line, bytes });
  pendingBytes += bytes;
  if (!exitFlushRegistered) {
    exitFlushRegistered = true;
    // An exit handler cannot await the lazy import. Use the same bounded, private writer.
    process.once("exit", () => {
      for (const row of pending) if (row.id) appendDiagnosticSync(row.id, row.line, row.bytes);
      pending = [];
      pendingBytes = 0;
    });
  }
  drain();
};

export const configureFabricDiagnostics = (id: string): void => {
  if (!isPrincipalDiagnostics() || !id) return;
  sessionId = id;
  for (const row of pending) row.id ??= id;
  drain();
};

/** Await already-requested writes for checks/shutdown; does not activate an idle sink. */
export const flushFabricDiagnostics = async (): Promise<void> => { while (draining) await draining; };

const log = (level: "warning" | "error", source: string, args: unknown[]): void => {
  if (!isPrincipalDiagnostics()) {
    if (level === "warning") console.warn(...args);
    else console.error(...args);
    return;
  }
  try { enqueue(level, source, format(...args)); } catch {}
};

export const diagnostics = {
  warn: (source: string, ...args: unknown[]): void => log("warning", source, args),
  error: (source: string, ...args: unknown[]): void => log("error", source, args),
  stderr: (source: string, text: string): void => {
    if (isPrincipalDiagnostics()) log("warning", source, [text]);
    else process.stderr.write(text);
  },
};

/** Internal notifications are private by default; explicit command/approval feedback stays visible. */
export const notifyDiagnostic = (
  context: { ui: { notify(message: string, level?: Level): void }; sessionManager?: { getSessionId?(): string } },
  source: string,
  message: string,
  level: Level = "warning",
  options: { principalVisible?: boolean } = {},
): void => {
  if (!isPrincipalDiagnostics() || options.principalVisible === true) {
    context.ui.notify(message, level);
    return;
  }
  try {
    const id = context.sessionManager?.getSessionId?.();
    if (id) configureFabricDiagnostics(id);
    enqueue(level, source, message);
  } catch {}
};
