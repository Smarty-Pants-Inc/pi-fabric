import { randomUUID } from "node:crypto";
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

/** Directory-relative path through the pinned directory fd: no second resolution of `generations/` (Node has no
 * openat/linkat; the same /proc/self/fd form as verified-remove.ts). */
const at = (dirFd: number, name: string): string => `/proc/self/fd/${dirFd}/${name}`;

/**
 * Create the generation's exit record, once, complete. Never makes a directory: a missing or foreign `generations/`
 * (no owning host made it) writes nothing.
 * 1. Open the residency root, then `generations/` relative to it, each as a handle (O_DIRECTORY|O_NOFOLLOW), and
 *    check each fd: a directory, ours, not group- or world-writable. Everything after goes through the
 *    `generations/` fd, so a swapped path cannot receive the record.
 * 2. Write the JSON to a fresh temp name (O_EXCL) and fsync it; then link(temp, final), which fails with EEXIST if a
 *    record exists: an atomic no-replace publish of a complete file. A writer killed before the link publishes
 *    nothing (the other writer's record goes in); one killed after it has published a complete record.
 * 3. Unlink the temp and fsync the directory.
 * 4. On EEXIST, validate the existing record (a complete record for this generation): "exists" if so, else
 *    "invalid" with the reason, so the caller can keep its own report elsewhere.
 * Linux only (the fd-relative form needs /proc/self/fd); elsewhere it fails closed. One stderr line when unavailable
 * or invalid; never throws (diagnostics never block an exit).
 */
export type GenerationExitOutcome = { outcome: "written" | "exists" | "unavailable" | "invalid"; reason?: string };
export const recordResidentGenerationExit = (residencyRoot: string, record: ResidentGenerationExit,
  constants: Partial<typeof fs.constants> = fs.constants): GenerationExitOutcome => {
  const refuse = (outcome: "unavailable" | "invalid", reason: string): GenerationExitOutcome => {
    try { process.stderr.write(`Fabric resident generation exit record ${outcome} (${record.generation}): ${reason}\n`); }
    catch { /* best effort */ }
    return { outcome, reason };
  };
  const unavailable = (reason: string) => refuse("unavailable", reason);
  if (!TOKEN.test(record.generation)) return unavailable("invalid generation token");
  const { O_RDONLY = 0, O_WRONLY = 0, O_CREAT = 0, O_EXCL, O_NOFOLLOW, O_DIRECTORY } = constants;
  if (process.platform !== "linux" || O_EXCL === undefined || O_NOFOLLOW === undefined || O_DIRECTORY === undefined) {
    return unavailable("needs Linux O_DIRECTORY/O_NOFOLLOW and /proc/self/fd");
  }
  const privateDir = (fd: number, name: string): string | undefined => {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || stat.uid !== process.getuid?.() || (stat.mode & 0o022) !== 0) {
      return `${name} is not a private directory owned by this user`;
    }
    const pinned = fs.statSync(`/proc/self/fd/${fd}`);
    return pinned.dev === stat.dev && pinned.ino === stat.ino ? undefined : `/proc/self/fd does not resolve ${name}`;
  };
  let rootFd: number | undefined, dirFd: number | undefined, fileFd: number | undefined, temp: string | undefined;
  try {
    rootFd = fs.openSync(residencyRoot, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    const rootRefusal = privateDir(rootFd, "the residency root");
    if (rootRefusal) return unavailable(rootRefusal);
    dirFd = fs.openSync(at(rootFd, "generations"), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    const dirRefusal = privateDir(dirFd, "generations/");
    if (dirRefusal) return unavailable(dirRefusal);
    temp = `.${record.generation}.${process.pid}.${randomUUID()}.tmp`;
    fileFd = fs.openSync(at(dirFd, temp), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600);
    fs.writeSync(fileFd, JSON.stringify(record));
    fs.fsyncSync(fileFd);
    fs.closeSync(fileFd); fileFd = undefined;
    const final = at(dirFd, `${record.generation}.exit.json`);
    let existed = false;
    try { fs.linkSync(at(dirFd, temp), final); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; existed = true; }
    fs.unlinkSync(at(dirFd, temp)); temp = undefined;
    fs.fsyncSync(dirFd);
    if (!existed) return { outcome: "written" };
    const invalid = invalidGenerationExit(final, record.generation, O_RDONLY | O_NOFOLLOW);
    return invalid ? refuse("invalid", invalid) : { outcome: "exists" };
  } catch (error) {
    return unavailable(error instanceof Error ? error.message : String(error));
  } finally {
    if (fileFd !== undefined) { try { fs.closeSync(fileFd); } catch { /* closed */ } }
    if (temp !== undefined && dirFd !== undefined) { try { fs.unlinkSync(at(dirFd, temp)); } catch { /* gone */ } }
    if (dirFd !== undefined) { try { fs.closeSync(dirFd); } catch { /* closed */ } }
    if (rootFd !== undefined) { try { fs.closeSync(rootFd); } catch { /* closed */ } }
  }
};

/** Why an existing record is not a complete record for `generation`; undefined when it is. */
const invalidGenerationExit = (file: string, generation: string, flags: number): string | undefined => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, flags);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > 64 * 1024) return "the existing record is not a small regular file";
    const value = JSON.parse(fs.readFileSync(fd, "utf8")) as Partial<ResidentGenerationExit> | null;
    if (value?.format !== 1 || value.generation !== generation || (value.writer !== "host" && value.writer !== "launcher") ||
        typeof value.reason !== "string" || !Number.isSafeInteger(value.pid) || !Number.isFinite(value.at)) {
      return "the existing record is incomplete or names another generation";
    }
    return undefined;
  } catch (error) {
    return `the existing record is unreadable: ${error instanceof Error ? error.message : String(error)}`;
  } finally { if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } } }
};
