import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/**
 * Per-generation exit record (smarty-dev#7950): `<residencyRoot>/generations/<token>.exit.json`, where `<token>` is
 * the owner token the resident host published in owner.json. The host makes `generations/` while it owns the root.
 * The record is never replaced. Linux publishes it complete and atomically; Darwin exclusively creates the final
 * file and validates it after writing (an interrupted writer can leave an invalid record). The host writes its own
 * exit reason at release; the launcher writes one only for an owned child that exited without it (SIGKILL, a crash). When that fails (the
 * host left an invalid record, or the publish itself failed), the launcher keeps its diagnostic in
 * `<token>.launcher-exit.json`, also generation-private. These paths are private to one generation, so no write
 * ever lands in a file a successor owns (never the shared error.json or launcher.log), and the writer needs no
 * ownership check before its open (the check-to-open window of smarty-dev#7770's launcher.log line).
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
  /** Launcher diagnostic file only: why the exit record could not be used. */
  diagnostic?: string;
}

const TOKEN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const EXIT = (generation: string): string => `${generation}.exit.json`;
const LAUNCHER_EXIT = (generation: string): string => `${generation}.launcher-exit.json`;
const ERROR = (generation: string): string => `${generation}.error.json`;
/** A temp file of this module for `generation`: `.<generation>.<pid>.<uuid>.tmp`. */
const tempPattern = (generation: string): RegExp =>
  new RegExp(`^\\.${generation}\\.(\\d+)\\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.tmp$`);

export const residentGenerationsPath = (residencyRoot: string): string => path.join(residencyRoot, "generations");
export const residentGenerationExitPath = (residencyRoot: string, generation: string): string =>
  path.join(residentGenerationsPath(residencyRoot), EXIT(generation));
export const residentLauncherExitPath = (residencyRoot: string, generation: string): string =>
  path.join(residentGenerationsPath(residencyRoot), LAUNCHER_EXIT(generation));
export const residentGenerationErrorPath = (residencyRoot: string, generation: string): string =>
  path.join(residentGenerationsPath(residencyRoot), ERROR(generation));

export interface ResidentGenerationError {
  format: 1;
  generation: string;
  writer: "host" | "launcher";
  error: string;
  occurredAt: number;
  pid: number;
  launcherBirth?: string;
  code?: string;
}

/** The host, while it owns the root: make `generations/`. Diagnostics only: a failure (or a foreign path, which
 * the record step refuses) never blocks the start; that generation then has no exit record. */
export const ensureResidentGenerations = (residencyRoot: string): void => {
  try { fs.mkdirSync(residentGenerationsPath(residencyRoot), { mode: 0o700 }); } catch { /* exists, or unavailable */ }
};

/** Directory-relative path through a pinned directory fd: no second resolution (Node has no openat/linkat; the same
 * /proc/self/fd form as verified-remove.ts). */
const at = (dirFd: number, name: string): string => `/proc/self/fd/${dirFd}/${name}`;

/** Why `text` is not a complete record for `generation`; undefined when it is. */
const parseRecord = (text: string): Record<string, unknown> | undefined => {
  try {
    const value: unknown = JSON.parse(text);
    return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
  } catch { return undefined; }
};
const commonRecord = (value: Record<string, unknown> | undefined, generation: string): boolean =>
  value?.format === 1 && value.generation === generation &&
  (value.writer === "host" || value.writer === "launcher") && Number.isSafeInteger(value.pid);
const invalidExitRecord = (text: string, generation: string): string | undefined => {
  const value = parseRecord(text);
  if (!value) return "the record is unreadable or not an object";
  if (!commonRecord(value, generation) || typeof value.reason !== "string" || !Number.isFinite(value.at) ||
      "error" in value || "occurredAt" in value) return "the record is incomplete or names another generation";
  return undefined;
};
const invalidErrorRecord = (text: string, generation: string): string | undefined => {
  const value = parseRecord(text);
  if (!value) return "the record is unreadable or not an object";
  if (!commonRecord(value, generation) || typeof value.error !== "string" || !Number.isFinite(value.occurredAt) ||
      "reason" in value || "at" in value) return "the record is incomplete or names another generation";
  return undefined;
};

export type GenerationExitOutcome = { outcome: "written" | "exists" | "unavailable" | "invalid"; reason?: string };

/**
 * Publish `<generation><suffix>` once in the verified `generations/` dir. On Linux:
 * 1. Open the residency root, then `generations/` relative to it, each as a handle (O_DIRECTORY|O_NOFOLLOW), and
 *    check each fd: a directory, ours, not group- or world-writable. Everything after goes through the
 *    `generations/` fd, so a swapped path cannot receive the file.
 * 2. Remove this generation's stale temp files: exact temp name, a regular file of ours, its writer pid gone.
 * 3. Write the JSON to a fresh temp name (O_EXCL), every byte (a short write is retried; no progress fails before
 *    the link), and fsync it; then link(temp, final), which fails with EEXIST if the file exists: an atomic
 *    no-replace publish. A writer killed before the link publishes nothing; one killed after it published a
 *    complete file. Unlink the temp and fsync the directory.
 * 4. Read the published (or existing) file back through the same handle and validate it.
 * 5. Never prune at publication: a stale publisher must not delete a successor's generation records.
 * Darwin has no /proc/self/fd: open and verify both directories by full path, exclusively create the final file,
 * finish the writes, then recheck each held directory's identity against its path. Never unlink by path on failure.
 * This is not atomic publication: interruption can leave a partial record, and a same-user ancestor swap can
 * redirect the create before the identity recheck refuses it. Windows stays closed without no-follow support.
 * One stderr line when unavailable or invalid; never throws (diagnostics never block an exit).
 */
const publish = (residencyRoot: string, record: ResidentGenerationExit | ResidentGenerationError, name: string,
  validate: (text: string, generation: string) => string | undefined,
  constants: Partial<typeof fs.constants>, platform: NodeJS.Platform): GenerationExitOutcome => {
  const refuse = (outcome: "unavailable" | "invalid", reason: string): GenerationExitOutcome => {
    try { process.stderr.write(`Fabric resident generation ${name} ${outcome} (${record.generation}): ${reason}\n`); }
    catch { /* best effort */ }
    return { outcome, reason };
  };
  const unavailable = (reason: string) => refuse("unavailable", reason);
  if (!TOKEN.test(record.generation)) return unavailable("invalid generation token");
  const { O_RDONLY = 0, O_WRONLY = 0, O_CREAT = 0, O_EXCL, O_NOFOLLOW, O_DIRECTORY } = constants;
  if ((platform !== "linux" && platform !== "darwin") || O_EXCL === undefined || O_NOFOLLOW === undefined || O_DIRECTORY === undefined) {
    return unavailable("needs Linux or Darwin O_DIRECTORY/O_NOFOLLOW/O_EXCL");
  }
  const uid = process.getuid?.();
  if (platform === "darwin") {
    const root = path.resolve(residencyRoot);
    const generations = residentGenerationsPath(root);
    const file = path.join(generations, name);
    const privateDir = (fd: number, label: string): string | undefined => {
      const stat = fs.fstatSync(fd);
      return !stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o022) !== 0
        ? `${label} is not a private directory owned by this user` : undefined;
    };
    let rootFd: number | undefined, dirFd: number | undefined, fileFd: number | undefined, readFd: number | undefined;
    try {
      rootFd = fs.openSync(root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
      const rootRefusal = privateDir(rootFd, "the residency root");
      if (rootRefusal) return unavailable(rootRefusal);
      dirFd = fs.openSync(generations, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
      const dirRefusal = privateDir(dirFd, "generations/");
      if (dirRefusal) return unavailable(dirRefusal);
      let existed = false;
      try { fileFd = fs.openSync(file, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; existed = true; }
      if (fileFd !== undefined) {
        const bytes = Buffer.from(JSON.stringify(record));
        for (let offset = 0, interrupted = 0; offset < bytes.length;) {
          let written: number;
          try { written = fs.writeSync(fileFd, bytes, offset, bytes.length - offset); }
          catch (error) {
            const code = (error as NodeJS.ErrnoException).code;
            if ((code === "EINTR" || code === "EAGAIN") && ++interrupted <= 3) continue;
            throw error;
          }
          if (!(written > 0)) throw new Error(`short write: ${offset} of ${bytes.length} bytes`);
          offset += written;
        }
        fs.fsyncSync(fileFd);
        fs.closeSync(fileFd); fileFd = undefined;
        try { fs.fsyncSync(dirFd); } catch { /* directory fsync is best effort on Darwin */ }
      }
      for (const [fd, fullPath, label] of [[rootFd, root, "the residency root"], [dirFd, generations, "generations/"]] as const) {
        const held = fs.fstatSync(fd);
        const current = fs.lstatSync(fullPath);
        if (!current.isDirectory() || held.dev !== current.dev || held.ino !== current.ino) {
          return unavailable(`${label} directory identity changed after publication`);
        }
      }
      readFd = fs.openSync(file, O_RDONLY | O_NOFOLLOW);
      const stat = fs.fstatSync(readFd);
      const invalid = !stat.isFile() || stat.size > 64 * 1024 ? "the record is not a small regular file"
        : validate(fs.readFileSync(readFd, "utf8"), record.generation);
      if (invalid) return refuse("invalid", `${existed ? "the existing" : "the published"} ${invalid.replace(/^the /, "")}`);
      return { outcome: existed ? "exists" : "written" };
    } catch (error) {
      return unavailable(error instanceof Error ? error.message : String(error));
    } finally {
      // Full paths may now refer to another directory: close only, never clean up a record by path.
      for (const fd of [readFd, fileFd, dirFd, rootFd]) {
        if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } }
      }
    }
  }
  const privateDir = (fd: number, label: string): string | undefined => {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || stat.uid !== uid || (stat.mode & 0o022) !== 0) return `${label} is not a private directory owned by this user`;
    const pinned = fs.statSync(`/proc/self/fd/${fd}`);
    return pinned.dev === stat.dev && pinned.ino === stat.ino ? undefined : `/proc/self/fd does not resolve ${label}`;
  };
  let rootFd: number | undefined, dirFd: number | undefined, fileFd: number | undefined, temp: string | undefined;
  try {
    rootFd = fs.openSync(residencyRoot, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    const rootRefusal = privateDir(rootFd, "the residency root");
    if (rootRefusal) return unavailable(rootRefusal);
    dirFd = fs.openSync(at(rootFd, "generations"), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    const dirRefusal = privateDir(dirFd, "generations/");
    if (dirRefusal) return unavailable(dirRefusal);
    const stale = tempPattern(record.generation);
    for (const entry of fs.readdirSync(at(dirFd, "."))) {
      const match = stale.exec(entry);
      if (!match || Number(match[1]) === process.pid) continue;
      try { process.kill(Number(match[1]), 0); continue; } // its writer may still be writing
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") continue; }
      const stat = fs.lstatSync(at(dirFd, entry), { throwIfNoEntry: false });
      if (stat?.isFile() && stat.uid === uid) fs.rmSync(at(dirFd, entry), { force: true });
    }
    temp = `.${record.generation}.${process.pid}.${randomUUID()}.tmp`;
    fileFd = fs.openSync(at(dirFd, temp), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600);
    const bytes = Buffer.from(JSON.stringify(record));
    for (let offset = 0, interrupted = 0; offset < bytes.length;) {
      let written: number;
      try { written = fs.writeSync(fileFd, bytes, offset, bytes.length - offset); }
      catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if ((code === "EINTR" || code === "EAGAIN") && ++interrupted <= 3) continue;
        throw error;
      }
      if (!(written > 0)) throw new Error(`short write: ${offset} of ${bytes.length} bytes`);
      offset += written;
    }
    fs.fsyncSync(fileFd);
    fs.closeSync(fileFd); fileFd = undefined;
    let existed = false;
    try { fs.linkSync(at(dirFd, temp), at(dirFd, name)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; existed = true; }
    fs.unlinkSync(at(dirFd, temp)); temp = undefined;
    fs.fsyncSync(dirFd);
    let invalid: string | undefined;
    let readFd: number | undefined;
    try {
      readFd = fs.openSync(at(dirFd, name), O_RDONLY | O_NOFOLLOW);
      const stat = fs.fstatSync(readFd);
      invalid = !stat.isFile() || stat.size > 64 * 1024 ? "the record is not a small regular file"
        : validate(fs.readFileSync(readFd, "utf8"), record.generation);
    } finally { if (readFd !== undefined) fs.closeSync(readFd); }
    if (invalid) return refuse("invalid", `${existed ? "the existing" : "the published"} ${invalid.replace(/^the /, "")}`);
    return { outcome: existed ? "exists" : "written" };
  } catch (error) {
    return unavailable(error instanceof Error ? error.message : String(error));
  } finally {
    if (fileFd !== undefined) { try { fs.closeSync(fileFd); } catch { /* closed */ } }
    if (temp !== undefined && dirFd !== undefined) { try { fs.unlinkSync(at(dirFd, temp)); } catch { /* gone */ } }
    if (dirFd !== undefined) { try { fs.closeSync(dirFd); } catch { /* closed */ } }
    if (rootFd !== undefined) { try { fs.closeSync(rootFd); } catch { /* closed */ } }
  }
};

/** Create the generation's exit record once (see `publish`). "exists" means a complete record for this
 * generation already stands; "invalid" names why the standing (or just published) file is not one. */
export const recordResidentGenerationExit = (residencyRoot: string, record: ResidentGenerationExit,
  constants: Partial<typeof fs.constants> = fs.constants, platform: NodeJS.Platform = process.platform): GenerationExitOutcome =>
  publish(residencyRoot, record, EXIT(record.generation), invalidExitRecord, constants, platform);

/** Publish a startup diagnostic once in `<generation>.error.json` on Linux or Darwin; fail-closed elsewhere. */
export const recordResidentGenerationError = (residencyRoot: string, record: ResidentGenerationError,
  constants: Partial<typeof fs.constants> = fs.constants, platform: NodeJS.Platform = process.platform): GenerationExitOutcome =>
  publish(residencyRoot, record, ERROR(record.generation), invalidErrorRecord, constants, platform);

/** Read only a valid generation-private error on Linux or Darwin; Windows remains closed without O_NOFOLLOW. */
export const readResidentGenerationError = (residencyRoot: string, generation: string,
  platform: NodeJS.Platform = process.platform): ResidentGenerationError | undefined => {
  if (!TOKEN.test(generation) || (platform !== "linux" && platform !== "darwin") || fs.constants.O_NOFOLLOW === undefined) return undefined;
  const file = residentGenerationErrorPath(residencyRoot, generation);
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > 64 * 1024) return undefined;
    const text = fs.readFileSync(fd, "utf8");
    if (invalidErrorRecord(text, generation)) return undefined;
    return JSON.parse(text) as ResidentGenerationError;
  } catch { return undefined; }
  finally { if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } } }
};

/**
 * The launcher, after its owned child exited: the exit record, else (the host's record is invalid, or the publish
 * failed) its diagnostic in the generation-private `<generation>.launcher-exit.json`. Never a shared root file.
 */
export const recordLauncherGenerationExit = (residencyRoot: string, record: ResidentGenerationExit,
  constants: Partial<typeof fs.constants> = fs.constants, platform: NodeJS.Platform = process.platform): GenerationExitOutcome => {
  const recorded = recordResidentGenerationExit(residencyRoot, record, constants, platform);
  if (recorded.outcome === "written" || recorded.outcome === "exists") return recorded;
  publish(residencyRoot, { ...record, diagnostic: recorded.reason ?? recorded.outcome }, LAUNCHER_EXIT(record.generation), invalidExitRecord, constants, platform);
  return recorded;
};

/** A generation's exit as recorded: the host's (or launcher's) complete exit record, else the launcher's diagnostic. */
export const readResidentGenerationExit = (residencyRoot: string, generation: string): ResidentGenerationExit | undefined => {
  if (!TOKEN.test(generation)) return undefined;
  for (const file of [residentGenerationExitPath(residencyRoot, generation), residentLauncherExitPath(residencyRoot, generation)]) {
    let fd: number | undefined;
    try {
      fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
      if (!fs.fstatSync(fd).isFile()) continue;
      const text = fs.readFileSync(fd, "utf8");
      if (!invalidExitRecord(text, generation)) return JSON.parse(text) as ResidentGenerationExit;
    } catch { /* absent or unreadable: the next */ }
    finally { if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } } }
  }
  return undefined;
};
