import fs from "node:fs";
import path from "node:path";

/**
 * Per-generation exit record (smarty-dev#7950): `<residencyRoot>/generations/<token>.exit.json`, where `<token>` is
 * the owner token the resident host published in owner.json. The host makes `generations/` while it owns the root.
 * The file is created with O_EXCL: the first writer wins and nothing ever rewrites it. The host writes its own exit
 * reason at release; the launcher writes one only for an owned child that exited without it (SIGKILL, a crash).
 * The path is private to that generation, so no write ever lands in a file a successor owns, and the writer needs
 * no ownership check before its open (the check-to-open window of smarty-dev#7770's launcher.log line).
 */
export interface ResidentGenerationExit {
  format: 1;
  generation: string;
  writer: "host" | "launcher";
  /** The host's reason, or "unreported" when the launcher saw the owned child exit without one. */
  reason: string;
  pid: number;
  at: number;
  code?: number | null;
  signal?: string | null;
}

const TOKEN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const residentGenerationsPath = (residencyRoot: string): string => path.join(residencyRoot, "generations");
export const residentGenerationExitPath = (residencyRoot: string, generation: string): string =>
  path.join(residentGenerationsPath(residencyRoot), `${generation}.exit.json`);

/** The host, while it owns the root: make `generations/`. Diagnostics only: a failure (or a foreign path, which
 * the record step refuses) never blocks the start; that generation then has no exit record. */
export const ensureResidentGenerations = (residencyRoot: string): void => {
  try { fs.mkdirSync(residentGenerationsPath(residencyRoot), { mode: 0o700 }); } catch { /* exists, or unavailable */ }
};

/**
 * Create the generation's exit record, once. Never makes a directory: a missing or foreign `generations/`
 * (no owning host made it) writes nothing. Returns what happened; never throws (diagnostics never block an exit).
 */
export const recordResidentGenerationExit = (residencyRoot: string, record: ResidentGenerationExit,
  constants: Partial<typeof fs.constants> = fs.constants): "written" | "exists" | "unavailable" => {
  if (!TOKEN.test(record.generation)) return "unavailable";
  const { O_WRONLY = 0, O_CREAT = 0, O_EXCL, O_NOFOLLOW } = constants;
  if (O_EXCL === undefined || O_NOFOLLOW === undefined) return "unavailable";
  let fd: number | undefined;
  try {
    const dir = fs.lstatSync(residentGenerationsPath(residencyRoot));
    if (!dir.isDirectory() || (process.getuid !== undefined && dir.uid !== process.getuid())) return "unavailable";
    fd = fs.openSync(residentGenerationExitPath(residencyRoot, record.generation), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600);
    fs.writeSync(fd, JSON.stringify(record));
    fs.fsyncSync(fd);
    return "written";
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EEXIST" ? "exists" : "unavailable";
  } finally { if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } } }
};
