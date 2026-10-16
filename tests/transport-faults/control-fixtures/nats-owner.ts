import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { launchLog, same, type LaunchLog, type Owned } from "../../helpers/owned-processes.js";
import { bounded, type CrashBoundary, type OwnerReceipt, OWNER_HOST_ID, SENDER_HOST_ID } from "./paths.js";
import type { ParticipantView } from "../../../src/topology/participant-view.js";
import type { NatsConnectionOptions } from "../../../src/transport/nats/connection.js";

/** MANUAL routing/lease fixture, not KvParticipantDirectory or production composition-root proof.
 * Only these two known identities are live. No lease acquisition/takeover claim is made. */
export function knownParticipants(ownerIncarnation: string): ParticipantView {
  return {
    resolve: id => id === OWNER_HOST_ID || id === SENDER_HOST_ID ? {
      machine: id === OWNER_HOST_ID ? "machine-b" : "machine-a", ownerHostId: id, ownerIdentityId: id,
      ownerIncarnation: id === OWNER_HOST_ID ? ownerIncarnation : "nats-owner-sender",
      capabilities: ["followUp", "steer", "stop"], live: true,
    } : undefined,
    leaseLive: () => true,
    whenChanged: async () => { throw new Error("Static known-route fixture has no directory change events"); },
  };
}

type Close = { code: number | null; signal: NodeJS.Signals | null };
type Waiter = { accept(receipt: OwnerReceipt): boolean; reject(error: Error): void };

/** Same launcher/preload/birth-proof/close-join pattern as OwnedOwner, with a NATS entry/inputs.
 * No process-tree cleanup, polling, shell, graceful crash substitute or second dispatcher. */
export class OwnedNatsOwner {
  readonly child: ChildProcess;
  readonly receipts: OwnerReceipt[] = [];
  readonly closed: Promise<Close>;
  readonly launches: LaunchLog;
  private readonly waiters = new Set<Waiter>();
  private terminal: Close | undefined;
  private failure: Error | undefined;
  private stderr = "";
  private readonly script: string;

  constructor(repository: string, root: string, generation: number, runtimeOptions: NatsConnectionOptions, boundary?: CrashBoundary) {
    if (process.release.name !== "node" || "bun" in process.versions || !/^node(?:\.exe)?$/i.test(path.basename(process.execPath))) {
      throw new Error("NATS owner-crash proof requires a Node test runner");
    }
    this.script = path.join(repository, "tests/transport-faults/control-fixtures/nats-owner.mjs");
    const temporary = path.join(root, `node-${generation}`);
    fs.mkdirSync(temporary, { recursive: true });
    this.launches = launchLog(temporary);
    const env: NodeJS.ProcessEnv = { ...process.env, ...this.launches.env, TMPDIR: temporary, TMP: temporary, TEMP: temporary,
      VITE_CACHE_DIR: path.join(repository, ".local/cache/vite"), XDG_CACHE_HOME: path.join(repository, ".local/cache/xdg"),
      npm_config_cache: path.join(repository, ".local/cache/npm"), JITI_FS_CACHE: "false",
      PI_FABRIC_MESH_STATE_BACKEND: "file", FABRIC_OWNER_OPTIONS: JSON.stringify(runtimeOptions) };
    if (process.env.NODE_OPTIONS && !env.NODE_OPTIONS?.startsWith(process.env.NODE_OPTIONS)) throw new Error("Lost inherited NODE_OPTIONS guard");
    this.child = spawn(process.execPath, [this.script, repository, root, boundary ?? "none"], {
      cwd: repository, env, stdio: ["ignore", "ignore", "pipe", "ipc"],
    });
    this.closed = new Promise(resolve => this.child.once("close", (code, signal) => {
      this.terminal = { code, signal };
      this.fail(new Error(`NATS owner closed (${code}/${signal})\n${this.stderr}`));
      resolve(this.terminal);
    }));
    this.child.once("error", error => this.fail(error));
    this.child.stderr?.on("data", chunk => { this.stderr = (this.stderr + String(chunk)).slice(-64_000); });
    this.child.on("message", raw => {
      if (typeof raw !== "object" || raw === null) { this.fail(new Error("Malformed owner IPC")); return; }
      const message = raw as OwnerReceipt;
      if (message.pid !== this.child.pid || typeof message.type !== "string") { this.fail(new Error("IPC does not name exact launched PID")); return; }
      this.receipts.push(message);
      if (message.type === "fatal") { this.fail(new Error(String(message.error))); return; }
      for (const waiter of [...this.waiters]) if (waiter.accept(message)) this.waiters.delete(waiter);
    });
  }
  private fail(error: Error): void {
    this.failure ??= error;
    for (const waiter of this.waiters) waiter.reject(this.failure);
    this.waiters.clear();
  }
  async receipt(predicate: (receipt: OwnerReceipt) => boolean, after = 0): Promise<OwnerReceipt> {
    if (this.failure) throw this.failure;
    const previous = this.receipts.slice(after).find(predicate);
    if (previous) return previous;
    let waiter: Waiter | undefined;
    try {
      return await bounded(new Promise<OwnerReceipt>((resolve, reject) => {
        waiter = { accept: receipt => { if (!predicate(receipt)) return false; resolve(receipt); return true; }, reject };
        this.waiters.add(waiter);
      }), `NATS PID ${this.child.pid} IPC`, 55_000);
    } finally { if (waiter) this.waiters.delete(waiter); }
  }
  ownership(): Owned {
    const entry = this.launches.owned().find(record => record.pid === this.child.pid && record.argv[0] === this.script);
    if (!entry || !same(entry) || entry.pid === process.pid || entry.pid === process.ppid) {
      throw new Error(`Cannot prove exact birth/current ownership; retained ${this.launches.file}`);
    }
    return entry;
  }
  async killAndJoin(): Promise<Close> {
    if (this.terminal) return this.terminal;
    let entry: Owned;
    try { entry = this.ownership(); }
    catch (error) {
      try { return await bounded(this.closed, "close without current birth proof", 10_000); }
      catch { throw error; }
    }
    if (!same(entry)) return bounded(this.closed, "close after PID identity changed", 10_000);
    try { process.kill(entry.pid, "SIGKILL"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    return bounded(this.closed, `actual close after SIGKILL ${entry.pid}`, 10_000);
  }
  diagnostics(): string { return this.stderr; }
}
