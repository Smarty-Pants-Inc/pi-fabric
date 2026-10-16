import fs from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { format } from "node:util";

// Local mirror of `getAgentDir` from @earendil-works/pi-coding-agent (0.84.2).
// Kept identical so Fabric resolves the same config directory without
// importing the host package during extension load.

const ENV_AGENT_DIR = "PI_CODING_AGENT_DIR";
export const CONFIG_DIR_NAME = ".pi";

const expandEnvDir = (envDir: string): string => {
  if (/^file:\/\//.test(envDir)) return fileURLToPath(envDir);
  if (envDir === "~") return homedir();
  if (envDir.startsWith("~/") || (process.platform === "win32" && envDir.startsWith("~\\"))) {
    return path.join(homedir(), envDir.slice(2));
  }
  return envDir;
};

export const resolveAgentDir = (envDir = process.env[ENV_AGENT_DIR]): string => {
  if (envDir) return expandEnvDir(envDir);
  return path.join(homedir(), CONFIG_DIR_NAME, "agent");
};

// Shared with self-reload and the release census. Kept in this existing cheap core module so
// code splitting does not create an additional eager startup chunk for release selectors.
export const SELF_RELOAD_COMMAND = "fabric-release-reload";

const isFabricPackage = (root: string): boolean => {
  try {
    return (JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")) as { name?: unknown }).name === "pi-fabric";
  } catch {
    return false;
  }
};

const real = (value: string): string => {
  try { return fs.realpathSync(value); } catch { return path.resolve(value); }
};

/** The Fabric package root this code loaded from: the nearest pi-fabric package.json above it. */
export const loadedFabricRoot = (moduleUrl: string): string | undefined => {
  let directory: string;
  try { directory = path.dirname(fileURLToPath(moduleUrl)); } catch { return undefined; }
  for (;;) {
    if (isFabricPackage(directory)) return real(directory);
    const parent = path.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
};

/** The local Fabric package the profile's settings.json activates, if exactly one. */
export const activeFabricRoot = (settingsPath: string): string | undefined => {
  let packages: unknown;
  try {
    packages = (JSON.parse(fs.readFileSync(settingsPath, "utf8")) as { packages?: unknown }).packages;
  } catch {
    return undefined;
  }
  if (!Array.isArray(packages)) return undefined;
  const base = path.dirname(settingsPath);
  const roots = packages.flatMap((entry) => {
    const source = typeof entry === "string" ? entry : (entry as { source?: unknown } | null)?.source;
    if (typeof source !== "string" || /^(npm|git|https?):/.test(source)) return [];
    const expanded = source.startsWith("~/") ? path.join(process.env.HOME ?? "", source.slice(2)) : source;
    const root = path.resolve(base, expanded);
    return isFabricPackage(root) ? [real(root)] : [];
  });
  return roots.length === 1 ? roots[0] : undefined;
};

export const releaseLabel = (root: string): string => path.basename(root);

// Profile-scoped diagnostics share this existing core entry, as release selectors do,
// to avoid a separate eager shared chunk. The normal file sink remains first-use lazy.
export const DIAGNOSTICS_MAX_BYTES = 1024 * 1024;
type Level = "warning" | "error" | "info";
type QueuedLine = { id: string | undefined; line: string; bytes: number };
let sessionId: string | undefined;
let pending: QueuedLine[] = [];
let pendingBytes = 0;
let draining: Promise<void> | undefined;
let exitFlushRegistered = false;

export const isPrincipalDiagnostics = (): boolean => process.env.SMARTY_ROLE?.startsWith("org-agent") === true;
/** Background progress is not a reply to a principal's command. */
export const setPrewalkStatus = (
  context: { ui: { setStatus(key: string, text: string | undefined): void } },
  text: string | undefined,
): void => context.ui.setStatus("fabric-prewalk", isPrincipalDiagnostics() ? undefined : text);

export const fabricDiagnosticsFile = (id: string): string =>
  path.join(resolveAgentDir(), "fabric", "diagnostics", `${encodeURIComponent(id)}.jsonl`);

/** Minimal synchronous fallback used only by process.exit; the normal sink stays in diagnostics-log.js. */
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

// Own-turn notices may be observed both at turn_end and agent_settled. Keep the de-duplication
// state on the resident Pi object so the two hooks share one marker without making actor notices
// globally visible. A new input/run/session clears the marker at the owning call site.
type PrincipalNoticeState = { sessionId: string | undefined; keys: Set<string> };
const PRINCIPAL_NOTICE_STATE = Symbol.for("pi-fabric.principal-notice-state.v1");
const principalNoticeStates = (): WeakMap<object, PrincipalNoticeState> =>
  ((globalThis as Record<symbol, unknown>)[PRINCIPAL_NOTICE_STATE] ??= new WeakMap<object, PrincipalNoticeState>()) as WeakMap<object, PrincipalNoticeState>;

const principalNoticeState = (
  owner: object,
  context: { sessionManager?: { getSessionId?(): string } },
): PrincipalNoticeState => {
  const states = principalNoticeStates();
  const sessionId = context.sessionManager?.getSessionId?.();
  let state = states.get(owner);
  if (!state || state.sessionId !== sessionId) {
    state = { sessionId, keys: new Set<string>() };
    states.set(owner, state);
  }
  return state;
};

/** Clear own-turn visible notice markers at a real new run/input/session boundary. */
export const resetPrincipalDiagnosticTurn = (
  owner: object,
  context: { sessionManager?: { getSessionId?(): string } },
): void => { principalNoticeState(owner, context).keys.clear(); };

/** Show one allowlisted own-turn notice, shared by Main's turn_end and settlement fallback. */
export const notifyPrincipalDiagnosticOnce = (
  owner: object,
  context: { ui: { notify(message: string, level?: Level): void }; sessionManager?: { getSessionId?(): string } },
  key: string,
  message: string,
  level: Level = "warning",
): boolean => {
  const state = principalNoticeState(owner, context);
  if (state.keys.has(key)) return false;
  try {
    context.ui.notify(message, level);
    state.keys.add(key);
    return true;
  } catch {
    return false;
  }
};
