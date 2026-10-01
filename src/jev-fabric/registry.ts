import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/** Newest durable task records kept for re-adoption across Pi restarts. */
export const DURABLE_TASK_RECORDS = 256;
/** Directory name under `<agentDir>/fabric`; its existence gates any resume work. */
export const DURABLE_TASKS_DIRECTORY = "durable-tasks";

/**
 * What Fabric needs to reattach a jev-fabric job to its owning session after a
 * restart. The process itself lives in the jev-fabric store; this is only the
 * session binding, so a lost record orphans nothing that `tasks.external` cannot find.
 */
export interface DurableTaskRecord {
  taskId: string;
  jobId: string;
  home: string;
  ownerId: string;
  command: string;
  description?: string;
  cwd: string;
  startedAt: number;
  /** Output passed through shell middleware at launch; reattach only through middleware. */
  filtered: boolean;
  /** Private launch script to remove once the job is observed terminal. */
  scriptPath?: string;
}

const isRecord = (value: unknown): value is DurableTaskRecord => {
  if (!value || typeof value !== "object") return false;
  const r = value as Record<string, unknown>;
  return ["taskId", "jobId", "home", "ownerId", "command", "cwd"].every(key => typeof r[key] === "string")
    && typeof r.startedAt === "number" && typeof r.filtered === "boolean";
};
const safeId = (taskId: string): string => {
  if (!/^[A-Za-z0-9-]{1,128}$/.test(taskId)) throw new Error(`Invalid durable task id: ${taskId}`);
  return taskId;
};

/**
 * One private file per binding, written by rename: concurrent Pi processes
 * never read-modify-write a shared document, so no lock is needed.
 */
export class DurableTaskRegistry {
  readonly directory: string;
  constructor(stateDirectory: string) { this.directory = path.join(stateDirectory, DURABLE_TASKS_DIRECTORY); }

  #file(taskId: string): string { return path.join(this.directory, `${safeId(taskId)}.json`); }

  /** Persistent output provenance lives alongside the backend job, so another
   * session/agent directory and terminal-binding cleanup cannot erase it. */
  #policyFile(home: string, jobId: string): string {
    return path.join(home, `.fabric-output-filter-${createHash("sha256").update(jobId).digest("hex")}`);
  }

  async #persistPolicy(home: string, jobId: string, filtered: boolean): Promise<void> {
    const target = this.#policyFile(home, jobId);
    const temporary = `${target}.${process.pid}.${randomUUID()}.tmp`;
    try {
      const file = await fs.promises.open(temporary, "wx", 0o600);
      try {
        await file.writeFile(filtered ? "filtered\n" : "unfiltered\n");
        await file.sync();
      } finally { await file.close(); }
      // Publish only complete, synced provenance, never an initializing file.
      try { await fs.promises.link(temporary, target); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        const existing = await this.outputPolicy(home, jobId);
        if (existing === undefined || (filtered && !existing)) throw new Error("Conflicting durable task output policy");
      }
      const directory = await fs.promises.open(home, "r");
      try { await directory.sync(); } finally { await directory.close(); }
    } finally { await fs.promises.rm(temporary, { force: true }); }
  }

  async protect(home: string, jobId: string): Promise<void> {
    await this.#persistPolicy(home, jobId, true);
  }

  /** Persist explicit provenance for unfiltered launches too. Absence is not
   * permission: a backend can be listed before its launch receipt reaches Pi. */
  async allowUnfiltered(home: string, jobId: string): Promise<void> {
    await this.#persistPolicy(home, jobId, false);
  }

  async outputPolicy(home: string, jobId: string): Promise<boolean | undefined> {
    try {
      const policy = await fs.promises.readFile(this.#policyFile(home, jobId), "utf8");
      if (policy === "filtered\n") return true;
      if (policy === "unfiltered\n") return false;
      throw new Error("Invalid durable task output policy; refusing adoption");
    } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  }

  async isProtected(home: string, jobId: string): Promise<boolean> {
    return await this.outputPolicy(home, jobId) === true;
  }

  async add(record: DurableTaskRecord): Promise<void> {
    await fs.promises.mkdir(this.directory, { recursive: true, mode: 0o700 });
    const target = this.#file(record.taskId);
    const temporary = `${target}.${process.pid}.${randomUUID()}.tmp`;
    try {
      await fs.promises.writeFile(temporary, JSON.stringify(record), { mode: 0o600 });
      await fs.promises.rename(temporary, target);
    } finally {
      await fs.promises.rm(temporary, { force: true });
    }
    await this.#prune();
  }

  async remove(taskId: string): Promise<DurableTaskRecord | undefined> {
    const file = this.#file(taskId);
    const record = await this.#read(file);
    await fs.promises.rm(file, { force: true });
    // An empty directory keeps the next session's resume at one missing-path check.
    await fs.promises.rmdir(this.directory).catch(() => undefined);
    return record;
  }

  async #read(file: string): Promise<DurableTaskRecord | undefined> {
    try {
      const value: unknown = JSON.parse(await fs.promises.readFile(file, "utf8"));
      return isRecord(value) ? value : undefined;
    } catch {
      // A missing or damaged binding only loses reattachment; the job stays discoverable.
      return undefined;
    }
  }

  async all(): Promise<DurableTaskRecord[]> {
    let names: string[];
    try { names = await fs.promises.readdir(this.directory); } catch { return []; }
    const records = await Promise.all(names.filter(name => name.endsWith(".json")).map(name => this.#read(path.join(this.directory, name))));
    return records.filter((record): record is DurableTaskRecord => record !== undefined).sort((a, b) => a.startedAt - b.startedAt);
  }

  async owned(ownerId: string): Promise<DurableTaskRecord[]> {
    return (await this.all()).filter(record => record.ownerId === ownerId);
  }

  async #prune(): Promise<void> {
    const records = await this.all();
    for (const record of records.slice(0, Math.max(0, records.length - DURABLE_TASK_RECORDS)))
      await fs.promises.rm(this.#file(record.taskId), { force: true });
  }
}
