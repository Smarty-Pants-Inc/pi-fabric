import fs from "node:fs";
import path from "node:path";
import type { BashOperations } from "@earendil-works/pi-coding-agent";
import type { FabricShellJobHandle, FabricShellJobInfo, FabricShellJobStore } from "../core/shell-jobs.js";
import type { FabricBashMiddlewareV1 } from "../protocol.js";
import type { JevFabricCli, JevFabricListedJob } from "./client.js";
import type { DurableTaskRecord, DurableTaskRegistry } from "./registry.js";
import type { JevFabricRequirement, JevFabricResolution } from "./resolve.js";

export interface DurableShellSettings {
  /** Empty or "auto": the user's install outside the workspace, then the bundled package. */
  binary: string;
  /** Empty: JEV_FABRIC_HOME, else `<cwd>/.jev-fabric-native`. */
  home: string;
  timeoutMs: number;
}

export interface DurableShellBridgeOptions {
  cwd: string;
  agentDir: string;
  ownerId: string | undefined;
  settings: () => DurableShellSettings;
  /** The active cooperative bash middleware, re-read on every use. */
  middleware: () => FabricBashMiddlewareV1 | undefined;
}

/** jev-fabric's own `start` ceiling. */
export const JEV_FABRIC_START_MAX_MS = 24 * 3_600_000;

type Modules = {
  client: typeof import("./client.js");
  registry: DurableTaskRegistry;
  operations: typeof import("./operations.js");
  resolve: typeof import("./resolve.js");
};

/**
 * jev-fabric rejects storage paths with symlink components (macOS `/var`,
 * `/tmp`), so canonicalize the deepest existing ancestor and keep the rest.
 */
export const resolveJevFabricHome = (settings: DurableShellSettings, cwd: string): string => {
  const requested = path.resolve(cwd, settings.home || process.env.JEV_FABRIC_HOME || ".jev-fabric-native");
  const rest: string[] = [];
  for (let current = requested; ; current = path.dirname(current)) {
    try { return path.join(fs.realpathSync.native(current), ...rest); } catch {
      if (path.dirname(current) === current) return requested;
      rest.unshift(path.basename(current));
    }
  }
};

/**
 * Durable shell tasks through an external jev-fabric store. The client,
 * registry and operations load at first durable use, never at session start.
 * Processes belong to the store, so session shutdown detaches them; the owning
 * session reattaches its own tasks on the next request after a restart.
 */
export class DurableShellBridge {
  #modules: Promise<Modules> | undefined;
  readonly #resolutions = new Map<string, Promise<JevFabricResolution>>();
  #resumed: Promise<void> | undefined;

  constructor(readonly jobs: FabricShellJobStore, readonly options: DurableShellBridgeOptions) {}

  get home(): string { return resolveJevFabricHome(this.options.settings(), this.options.cwd); }
  get #stateDirectory(): string { return path.join(this.options.agentDir, "fabric"); }

  #load(): Promise<Modules> {
    this.#modules ??= Promise.all([import("./client.js"), import("./registry.js"), import("./operations.js"), import("./resolve.js")])
      .then(([client, registry, operations, resolve]) => ({
        client, operations, resolve, registry: new registry.DurableTaskRegistry(this.#stateDirectory),
      }));
    return this.#modules;
  }

  /**
   * The binary for a feature, chosen once per setting and requirement. A failed
   * resolution is not remembered, so installing jev-fabric takes effect at once.
   */
  resolve(requirement: JevFabricRequirement): Promise<JevFabricResolution> {
    DurableShellBridge.assertSupported();
    const configured = this.options.settings().binary;
    const key = `${configured}\0${requirement}`;
    let pending = this.#resolutions.get(key);
    if (!pending) {
      pending = this.#load().then(({ resolve }) => resolve.resolveJevFabric({
        configured, cwd: this.options.cwd, agentDir: this.options.agentDir, requirement,
      }));
      pending.catch(() => this.#resolutions.delete(key));
      this.#resolutions.set(key, pending);
    }
    return pending;
  }

  async #cli(): Promise<JevFabricCli> {
    const [{ client }, resolution] = await Promise.all([this.#load(), this.resolve("durable")]);
    return new client.JevFabricCli(resolution.path, this.home);
  }

  static assertSupported(): void {
    if (process.platform === "win32") throw new Error("Durable shell tasks need jev-fabric, which supports macOS and Linux only");
  }

  /** Operations for a new durable job, recorded for re-adoption once started. */
  async launch(job: FabricShellJobHandle, options: { shellPath?: string | undefined; label?: string | undefined; filtered: boolean; command: string; cwd: string; ownerId: string | undefined }): Promise<BashOperations> {
    const [{ registry, operations }, cli] = await Promise.all([this.#load(), this.#cli()]);
    const settings = this.options.settings();
    const home = this.home;
    const scriptDirectory = path.join(this.#stateDirectory, "durable-shell");
    return operations.createJevFabricBashOperations({
      cli, taskId: job.id, scriptDirectory, detached: job.detached.signal,
      shellPath: options.shellPath,
      defaultTimeoutMs: Math.min(settings.timeoutMs, JEV_FABRIC_START_MAX_MS),
      maxTimeoutMs: JEV_FABRIC_START_MAX_MS,
      label: options.label,
      onStarted: async (jobId, scriptPath) => {
        // Backend visibility precedes this callback. Adopters must refuse
        // absent/initializing provenance, including for unfiltered launches.
        try {
          if (options.filtered) await registry.protect(home, jobId);
          else await registry.allowUnfiltered(home, jobId);
        } catch (error) {
          await cli.stop(jobId);
          throw error;
        }
        // Do not publish a backend ID through task metadata until its required
        // output policy is persisted; another session can adopt a visible ID.
        if (job.durable) job.durable.jobId = jobId;
        if (!options.ownerId) return;
        // A lost binding orphans nothing: tasks.external still lists the job.
        await registry.add({
          taskId: job.id, jobId, home, ownerId: options.ownerId, command: options.command,
          ...(options.label ? { description: options.label } : {}),
          cwd: options.cwd, startedAt: job.startedAt, filtered: options.filtered, scriptPath,
        }).catch(() => undefined);
      },
      onFinished: () => this.#forget(job.id),
    });
  }

  async #forget(taskId: string): Promise<void> {
    const { registry } = await this.#load();
    const record = await registry.remove(taskId).catch(() => undefined);
    if (record?.scriptPath) await fs.promises.rm(record.scriptPath, { force: true }).catch(() => undefined);
  }

  /** Reattaches this session's durable tasks after a restart; memoized. */
  resume(): Promise<void> {
    const ownerId = this.options.ownerId;
    if (!ownerId || process.platform === "win32") return Promise.resolve();
    // One missing-file check when durable tasks were never used; no module loads.
    // Mirrors DURABLE_TASKS_DIRECTORY without loading the registry module. Only
    // tasks from earlier processes need reattaching, so one check per session decides.
    if (!this.#resumed && !fs.existsSync(path.join(this.#stateDirectory, "durable-tasks"))) {
      this.#resumed = Promise.resolve();
      return this.#resumed;
    }
    this.#resumed ??= (async () => {
      const { registry } = await this.#load();
      const records = (await registry.owned(ownerId)).filter(record => record.home === this.home);
      for (const record of records) {
        if (this.jobs.get(record.taskId)) continue;
        await this.#attach(record).catch(() => undefined);
      }
    })().catch(() => undefined);
    return this.#resumed;
  }

  /** jev-fabric jobs in this store that no task of this session tracks. */
  async external(): Promise<{ home: string; jobs: JevFabricListedJob[]; truncated: boolean }> {
    DurableShellBridge.assertSupported();
    const cli = await this.#cli();
    const tracked = new Set(this.jobs.list().flatMap(job => job.durable?.jobId ? [job.durable.jobId] : []));
    const listed = await cli.list();
    return { home: this.home, jobs: listed.jobs.filter(job => !tracked.has(job.id)), truncated: listed.truncated };
  }

  /** Attaches an existing jev-fabric job (for example, one another harness started) to this session. */
  async adopt(jobId: string, description?: string): Promise<FabricShellJobInfo> {
    DurableShellBridge.assertSupported();
    const [{ registry }, cli] = await Promise.all([this.#load(), this.#cli()]);
    const existing = this.jobs.list().find(job => job.durable?.jobId === jobId);
    if (existing) return existing;
    const status = await cli.status(jobId);
    const filtered = await registry.outputPolicy(this.home, jobId);
    if (filtered === undefined) throw new Error("Durable task output policy is absent or initializing; refusing adoption");
    const { randomUUID } = await import("node:crypto");
    const record: DurableTaskRecord = {
      taskId: randomUUID(), jobId, home: this.home, ownerId: this.options.ownerId ?? "",
      command: status.label ?? `jev-fabric job ${jobId}`,
      ...(description ?? status.label ? { description: description ?? status.label! } : {}),
      cwd: this.options.cwd, startedAt: Date.now(), filtered,
    };
    if (this.options.ownerId) await registry.add(record).catch(() => undefined);
    return this.#attach(record);
  }

  async #attach(record: DurableTaskRecord): Promise<FabricShellJobInfo> {
    const [{ operations, registry }, cli] = await Promise.all([this.#load(), this.#cli()]);
    const filtered = await registry.outputPolicy(record.home, record.jobId);
    if (filtered === undefined) throw new Error("Durable task output policy is absent or initializing; refusing attachment");
    const job = this.jobs.begin("bash", record.command, {
      id: record.taskId, startedAt: record.startedAt, cwd: record.cwd,
      ...(record.ownerId ? { ownerId: record.ownerId } : {}),
      ...(record.description ? { description: record.description } : {}),
      durable: { home: record.home, jobId: record.jobId, adopted: true },
    });
    job.spill();
    await job.persistLog().catch(() => undefined);
    const attach = operations.attachJevFabricBashOperations(cli, record.jobId, job.detached.signal);
    const middleware = this.options.middleware();
    let exec: BashOperations;
    if (middleware) {
      const wrapped = middleware.wrapOperations(attach);
      if (!wrapped || typeof wrapped.exec !== "function") throw new Error("Invalid Fabric bash middleware operations; refusing to bypass shell protection");
      exec = wrapped;
    } else exec = attach;
    // Only the launcher's existing live wrapper can preserve its output policy.
    // Adoption/resume has no proven policy identity, even with middleware active.
    const withheld = filtered;
    if (withheld) job.append(Buffer.from("[Output withheld: launched under an output filter.]\n"));
    void exec.exec(record.command, record.cwd, {
      onData: data => { if (!withheld) job.append(data); },
      signal: job.abort.signal,
    }).then(
      async ({ exitCode }) => {
        await job.finish(exitCode, `\n\n[Process exited with code ${exitCode ?? "unknown"}]\n`);
        await this.#forget(record.taskId);
      },
      async (error: unknown) => {
        if (job.finished || job.detached.signal.aborted) return;
        const message = error instanceof Error ? error.message : String(error);
        const stopped = job.abort.signal.aborted;
        await job.finish(null, `\n\n[${stopped ? "Stopped" : message}]\n`);
        if (stopped) await this.#forget(record.taskId);
      },
    );
    return job.info();
  }
}
