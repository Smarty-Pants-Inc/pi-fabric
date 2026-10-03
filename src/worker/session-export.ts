import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/**
 * Usage-only Pi-format session export for external cost trackers (tokscale,
 * ccusage). Fabric subagents run with `--no-session`, so nothing they spend is
 * visible to tools that aggregate token usage from session JSONL files. When
 * enabled, the worker appends one attributed assistant usage line per turn to
 * a minimal pi-format session file under `~/.pi-fabric/agent/sessions/`
 * (or PI_FABRIC_AGENT_DIR): a `session` header, a `session_info` marker naming
 * the run as "fabricagent-<name>", and `message` entries carrying only
 * model/provider/usage — never transcript content.
 *
 * tokscale's pi parser attaches the `session_info` name to every assistant
 * message in the file (agent-level attribution), and ccusage's pi adapter
 * counts each message line toward named-store reports.
 *
 * Self-contained on purpose: this module is dynamically imported by the
 * spawned worker through plain Node with worker.ts switching the import
 * extension, so it must only use node builtins.
 */

export const FABRIC_AGENT_MARKER = "fabricagent-";

/**
 * Stable turn id shared by an actor journal entry and its usage re-export
 * (smarty-dev#3327). Pi stamps every assistant message with a millisecond
 * `timestamp` that its session journal preserves verbatim, so
 * `<journal path>#<message.timestamp>` names one model turn in both sources.
 */
export const journalTurnId = (journal: string, message: unknown): string | undefined => {
  const timestamp = (message as { timestamp?: unknown } | null | undefined)?.timestamp;
  return typeof timestamp === "number" && Number.isFinite(timestamp) ? `${journal}#${timestamp}` : undefined;
};

/**
 * Usage sum over journal and export JSONL lines that counts each turn once:
 * an export entry's `reexportOf` and a journal entry's derived turn id
 * collide, so summing both sources equals summing the journal alone.
 */
export const dedupedUsageTokens = (sources: readonly { file: string; lines: readonly unknown[] }[]): number => {
  const seen = new Set<string>();
  let total = 0;
  for (const { file, lines } of sources) {
    for (const line of lines) {
      const entry = line as { type?: unknown; reexportOf?: unknown; message?: { role?: unknown; usage?: Record<string, unknown> } };
      if (entry?.type !== "message" || entry.message?.role !== "assistant" || !entry.message.usage) continue;
      const turn = typeof entry.reexportOf === "string" ? entry.reexportOf : journalTurnId(file, entry.message);
      if (turn) {
        if (seen.has(turn)) continue;
        seen.add(turn);
      }
      const usage = entry.message.usage;
      total += ["input", "output", "cacheRead", "cacheWrite"]
        .reduce((sum, key) => sum + (typeof usage[key] === "number" ? usage[key] : 0), 0);
    }
  }
  return total;
};

export interface SessionExportUsage {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  cost: number;
}

const nonNegative = (value: number): number =>
  Number.isFinite(value) ? Math.max(0, value) : 0;

export class SessionExporter {
  readonly file: string;
  readonly #sessionId: string;
  readonly #cwd: string;
  readonly #agentName: string;
  #headerWritten = false;
  #lastEntryId: string | null = null;
  #disabled = false;

  constructor(options: { file: string; sessionId: string; cwd: string; agentName: string }) {
    this.file = options.file;
    this.#sessionId = options.sessionId;
    this.#cwd = options.cwd;
    this.#agentName = options.agentName;
  }

  /**
   * Append one attributed assistant usage line. Zero-usage pushes are skipped
   * so heartbeat-style emissions never write entries; the file and its header
   * are created lazily on the first real push so runs that never touch a model
   * leave nothing behind. Best-effort: any IO failure disables the exporter
   * rather than failing the run. `reexportOf` marks a turn the actor journal
   * already records (see journalTurnId) so usage readers can skip the copy.
   */
  push(usage: SessionExportUsage, model?: string, provider?: string, at: number = Date.now(), reexportOf?: string): void {
    if (this.#disabled) return;
    const tokens = {
      input: nonNegative(usage.input),
      output: nonNegative(usage.output),
      cacheRead: nonNegative(usage.cacheRead),
      cacheWrite: nonNegative(usage.cacheWrite),
    };
    const cost = nonNegative(usage.cost);
    const totalTokens = tokens.input + tokens.output + tokens.cacheRead + tokens.cacheWrite;
    if (totalTokens === 0 && cost === 0) return;
    try {
      this.#ensureHeader(at);
      const id = randomUUID();
      const entry = {
        type: "message",
        id,
        parentId: this.#lastEntryId,
        timestamp: new Date(at).toISOString(),
        ...(reexportOf ? { reexportOf } : {}),
        message: {
          role: "assistant",
          model: model?.trim() ? model : "unknown",
          ...(provider?.trim() ? { provider } : {}),
          usage: { ...tokens, totalTokens, cost: { total: cost } },
        },
      };
      fs.appendFileSync(this.file, `${JSON.stringify(entry)}\n`, { encoding: "utf8", mode: 0o600 });
      this.#lastEntryId = id;
    } catch {
      this.#disabled = true;
    }
  }

  #ensureHeader(at: number): void {
    if (this.#headerWritten) return;
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const timestamp = new Date(at).toISOString();
    const header = {
      type: "session",
      version: 3,
      id: this.#sessionId,
      timestamp,
      cwd: this.#cwd,
    };
    fs.writeFileSync(this.file, `${JSON.stringify(header)}\n`, { encoding: "utf8", mode: 0o600 });
    const info = {
      type: "session_info",
      id: `info_${this.#sessionId}`,
      parentId: null,
      timestamp,
      name: `${FABRIC_AGENT_MARKER}${this.#agentName}`,
    };
    fs.appendFileSync(this.file, `${JSON.stringify(info)}\n`, { encoding: "utf8", mode: 0o600 });
    this.#lastEntryId = header.id;
    this.#headerWritten = true;
  }
}
