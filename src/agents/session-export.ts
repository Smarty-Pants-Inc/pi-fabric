import os from "node:os";
import path from "node:path";
import { resolveAgentDir } from "../core/agent-dir.js";
import type { FabricAgentConfig } from "../config.js";

/**
 * Host-side resolution for the usage export store written by spawned workers
 * (see worker/session-export.ts). Files follow the pi-format store layout that
 * tokscale and ccusage already scan:
 *
 *   <root>/sessions/.fabric/<encoded-cwd>/<timestamp>_<runId>.jsonl
 *
 * Both trackers walk their session roots recursively, while pi's own resume
 * picker only reads its immediate "<encoded-cwd>" directory — so hosting the
 * export under the ".fabric" namespace inside pi's store makes trackers count
 * subagent usage with zero configuration while pi itself never lists these
 * files. Root resolves as:
 *
 *   PI_FABRIC_AGENT_DIR env  >  agents.sessionExportDir  >  pi's agent dir
 *
 * pi's agent dir is the parent's profile: PI_CODING_AGENT_DIR, else ~/.pi/agent. A fleet
 * profile's run exports therefore sit beside its own sessions, not in the default profile
 * (smarty-dev#847: they read as default-profile sessions).
 *
 * Prefer an isolated store instead? Set agents.sessionExportDir to
 * ~/.pi-fabric/agent and register it as a ccusage pi.stores named store.
 */

export const SESSION_EXPORT_ENV = "PI_FABRIC_AGENT_DIR";

const expandHome = (value: string): string =>
  value === "~"
    ? os.homedir()
    : value.startsWith("~/") || value.startsWith(`~${path.win32.sep}`)
      ? path.join(os.homedir(), value.slice(2))
      : value;

/**
 * Pi's exact cwd → session-subdir encoding (badlogic/pi-mono
 * getDefaultSessionDirPath): `/Users/dev/project` becomes `--Users-dev-project--`.
 * Both trackers only require the directory to sit under the scanned tree; using
 * pi's encoding keeps fabric sessions visually consistent with native ones.
 * Already-absolute inputs are encoded verbatim: resolving a POSIX path on win32
 * would prepend the current drive (`/x` → `D:\\x`) and skew the encoding, so
 * only relative inputs go through the platform resolver like pi does.
 */
export const encodeSessionExportCwd = (cwd: string): string => {
  const absolute =
    path.isAbsolute(cwd) || path.win32.isAbsolute(cwd) ? cwd : path.resolve(cwd);
  return `--${absolute.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--`;
};

/** Root of the export store, or undefined when `agents.sessionExport` is off. */
export const resolveSessionExportDir = (config: FabricAgentConfig): string | undefined => {
  if (!config.sessionExport) return undefined;
  const raw =
    process.env[SESSION_EXPORT_ENV]?.trim() ||
    config.sessionExportDir.trim() ||
    resolveAgentDir();
  return expandHome(raw);
};

/**
 * Final JSONL path for one run: `<root>/sessions/.fabric/<encoded-cwd>/<ts>_<runId>.jsonl`.
 * The ".fabric" namespace is deliberate: tokscale (walkdir) and ccusage both
 * recurse past it, but pi's per-project resume picker never descends into it.
 */
export const sessionExportFileFor = (
  root: string,
  cwd: string,
  runId: string,
  at: Date,
): string => {
  const fileTimestamp = at.toISOString().replace(/[:.]/g, "-");
  return path.join(
    root,
    "sessions",
    ".fabric",
    encodeSessionExportCwd(cwd),
    `${fileTimestamp}_${runId}.jsonl`,
  );
};
