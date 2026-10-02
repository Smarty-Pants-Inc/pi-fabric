import fs from "node:fs";
import path from "node:path";
import { resolveAgentDir } from "../core/agent-dir.js";

export interface MainReleaseProcess {
  pid: number;
  start: string;
  sessionId: string;
  loadedRoot: string;
}

/** Linux boot-relative start ticks bind a record to a process, not a reused PID. */
export const processStart = (pid: number, procRoot = "/proc"): string | undefined => {
  try {
    const stat = fs.readFileSync(path.join(procRoot, String(pid), "stat"), "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
  } catch { return undefined; }
};

export const mainReleaseRecordDir = (settingsPath = path.join(resolveAgentDir(), "settings.json")): string =>
  path.join(path.dirname(settingsPath), "fabric", "release-processes");

/** Called only when the lazy runtime initializes, not during extension registration/idle. */
export const recordMainRelease = (sessionId: string, loadedRoot: string | undefined): void => {
  if (!loadedRoot || process.env.PI_FABRIC_PARENT_RUN || process.env.PI_FABRIC_ACTOR_ID) return;
  const start = processStart(process.pid);
  if (!start) return; // The proc-based report explicitly supports Linux hosts only.
  const directory = mainReleaseRecordDir();
  const file = path.join(directory, `${process.pid}.json`);
  const temporary = `${file}.tmp`;
  try {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const record: MainReleaseProcess = { pid: process.pid, start, sessionId, loadedRoot };
    fs.writeFileSync(temporary, JSON.stringify(record), { mode: 0o600 });
    fs.renameSync(temporary, file);
  } catch {
    try { fs.unlinkSync(temporary); } catch { /* best-effort observational metadata; launch behavior is unchanged */ }
  }
};
