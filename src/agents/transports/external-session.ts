import type {
  AgentTransportHandle,
  AgentTransportObservation,
  AgentTransportObservationOptions,
} from "../types.js";
import { EXTERNAL_TRANSPORT_LIVENESS_POLL_INTERVAL_MS } from "../constants.js";
import { executeFile } from "./process-utils.js";

type ExternalKind = "tmux" | "screen";
const CLI_TIMEOUT_MS = 3_000;
const unknown = (reason: string): AgentTransportObservation => ({ state: "unknown", reason });

const commandOptions = (options: AgentTransportObservationOptions = {}) => {
  if (options.signal?.aborted) throw new Error("External session command cancelled");
  const remaining = options.deadline === undefined ? CLI_TIMEOUT_MS : options.deadline - Date.now();
  if (!Number.isFinite(remaining) || remaining <= 0) throw new Error("External session command deadline expired");
  return {
    timeoutMs: Math.max(1, Math.min(CLI_TIMEOUT_MS, Math.floor(remaining))),
    killSignal: "SIGKILL" as const,
    ...(options.signal ? { signal: options.signal } : {}),
    // screen's inventory is a human-readable protocol. Unsupported/malformed
    // output remains unknown; pin its language instead of guessing translations.
    env: { ...process.env, LC_ALL: "C" },
  };
};

/** Parse only a complete successful inventory, never CLI error text. */
export const externalSessionInventory = (
  kind: ExternalKind, session: string, stdout: string,
): AgentTransportObservation => {
  const lines = stdout.replace(/\r?\n$/, "").split(/\r?\n/);
  if (kind === "tmux") {
    const names: string[] = [];
    for (const line of lines) {
      const match = /^\$\d+\|([^\r\n]+)$/.exec(line);
      if (!match) return unknown("Malformed tmux session inventory");
      names.push(match[1]!);
    }
    return { state: names.includes(session) ? "alive" : "absent" };
  }
  if (!/^There (?:is a screen|are screens) on:$/.test(lines[0] ?? "")) {
    return unknown("Malformed screen session inventory");
  }
  const trailer = /^([1-9]\d*) Sockets? in .+\.$/.exec(lines.at(-1) ?? "");
  if (!trailer) return unknown("Incomplete screen session inventory");
  const names: string[] = [];
  for (const line of lines.slice(1, -1)) {
    const match = /^\s+\d+\.([^\t\r\n]+)\t(?:\([^\r\n]*\)\t)?\((?:Attached|Detached)\)$/.exec(line);
    // Dead/unknown sockets are not an absence proof, even for another name.
    if (!match) return unknown("Uncertain screen socket inventory");
    names.push(match[1]!);
  }
  if (names.length !== Number(trailer[1])) return unknown("Incomplete screen session inventory");
  return { state: names.includes(session) ? "alive" : "absent" };
};

export const observeExternalSession = async (
  kind: ExternalKind, session: string, options?: AgentTransportObservationOptions,
): Promise<AgentTransportObservation> => {
  try {
    const args = kind === "tmux" ? ["list-sessions", "-F", "#{session_id}|#{session_name}"] : ["-ls"];
    const { stdout, stderr } = await executeFile(kind, args, commandOptions(options));
    if (stderr.trim()) return unknown(`${kind} session inventory reported a diagnostic`);
    return externalSessionInventory(kind, session, stdout);
  } catch (error) {
    // Missing/unreachable server, canceled/hung query and permission errors all
    // leave membership unknown. Never recognize absence by localized error text.
    return unknown(`${kind} session observation failed: ${error instanceof Error ? error.message : String(error)}`);
  }
};

/** Preserve the existing false + lostContact compatibility contract for managers
 * while exposing the checked result to callers that can distinguish uncertainty. */
export const externalSessionHandle = (kind: ExternalKind, session: string): AgentTransportHandle => {
  let lost: string | undefined;
  const observe = async (options?: AgentTransportObservationOptions): Promise<AgentTransportObservation> => {
    const result = await observeExternalSession(kind, session, options);
    lost = result.state === "unknown" ? result.reason : undefined;
    return result;
  };
  return {
    kind,
    // Session absence is NOT worker exit proof. Keep round-3 barriers until a
    // persistent birth identity/exit receipt has independent security review.
    relaunchable: false,
    livenessPollIntervalMs: EXTERNAL_TRANSPORT_LIVENESS_POLL_INTERVAL_MS,
    sessionId: session,
    attachCommand: kind === "tmux" ? `tmux attach-session -t ${session}` : `screen -r ${session}`,
    observe,
    lostContact: () => lost,
    async isAlive(options) { return (await observe(options)).state === "alive"; },
    async stop(options) {
      try {
        await executeFile(kind, kind === "tmux" ? ["kill-session", "-t", `=${session}`] : ["-S", session, "-X", "quit"], commandOptions(options));
      } catch { /* A stop error/acknowledgment never proves worker exit. */ }
    },
  };
};

/** A launch CLI may create the session before cancellation, timeout or a failed
 * reply. Reuse the manager's persistent unresolved-launch retention path. */
export const launchExternalSession = async (
  kind: ExternalKind, session: string, args: string[], cwd: string, signal?: AbortSignal,
): Promise<void> => {
  try {
    await executeFile(kind, args, { ...commandOptions({ ...(signal ? { signal } : {}) }), cwd });
  } catch (error) {
    throw Object.assign(new Error(`${kind} launch outcome is unknown: ${error instanceof Error ? error.message : String(error)}`), {
      launchOutcome: "unknown", cleanupPending: true, transport: kind, sessionId: session,
    });
  }
};
