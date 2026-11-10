import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { launchLog, same, type LaunchLog, type Owned } from "../../helpers/owned-processes.js";
import { bounded, type CrashBoundary, type OwnerReceipt } from "./paths.js";

type Close = { code: number | null; signal: NodeJS.Signals | null };
type Waiter = { accept(receipt: OwnerReceipt): boolean; reject(error: Error): void };

/** Exact owned child only. No process-tree/native cleanup, shell, polling or graceful crash substitute. */
export class OwnedOwner {
  readonly child: ChildProcess;
  readonly receipts: OwnerReceipt[] = [];
  readonly closed: Promise<Close>;
  readonly launches: LaunchLog;
  private readonly waiters = new Set<Waiter>();
  private terminal: Close | undefined;
  private failure: Error | undefined;
  private stderr = "";
  private stdout = "";
  private readonly script: string;

  constructor(repository: string, root: string, generation: number, boundary?: CrashBoundary) {
    // Under a Bun-driven runner, fail visibly rather than accidentally using a Bun child.
    if (!/^node(?:\.exe)?$/i.test(path.basename(process.execPath))) throw new Error("Owner-crash proof requires a Node test runner");
    this.script = path.join(repository, "tests/transport-faults/control-fixtures/owner.mjs");
    const temporary = path.join(root, `node-${generation}`);
    fs.mkdirSync(temporary, { recursive: true });
    this.launches = launchLog(temporary);
    const env: NodeJS.ProcessEnv = { ...process.env, ...this.launches.env, TMPDIR: temporary, TMP: temporary, TEMP: temporary,
      PI_FABRIC_MESH_STATE_BACKEND: "file" };
    // launchLog appends its --import preload, never discards an inherited NODE_OPTIONS guard.
    if (process.env.NODE_OPTIONS && !env.NODE_OPTIONS?.startsWith(process.env.NODE_OPTIONS)) {
      throw new Error("Lost inherited NODE_OPTIONS guard");
    }
    this.child = spawn(process.execPath, [this.script, repository, root, boundary ?? "none"], {
      cwd: repository, env, stdio: ["ignore", "pipe", "pipe", "ipc"],
    });
    this.closed = new Promise(resolve => {
      this.child.once("close", (code, signal) => {
        this.terminal = { code, signal };
        this.fail(new Error(`Owner closed (${code}/${signal})\n${this.stderr}\n${this.stdout}`));
        resolve(this.terminal);
      });
    });
    this.child.once("error", error => this.fail(error));
    this.child.stdout?.on("data", chunk => { this.stdout = (this.stdout + String(chunk)).slice(-64_000); });
    this.child.stderr?.on("data", chunk => { this.stderr = (this.stderr + String(chunk)).slice(-64_000); });
    this.child.on("message", raw => {
      if (typeof raw !== "object" || raw === null) { this.fail(new Error("Malformed owner IPC receipt")); return; }
      const message = raw as OwnerReceipt;
      if (message.pid !== this.child.pid || typeof message.type !== "string") {
        this.fail(new Error("Owner receipt does not name the exact launched PID")); return;
      }
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

  /** Saved receipts eliminate listener-registration races; after isolates a fresh retry's ACK. */
  async receipt(predicate: (receipt: OwnerReceipt) => boolean, after = 0): Promise<OwnerReceipt> {
    if (this.failure) throw this.failure;
    const previous = this.receipts.slice(after).find(predicate);
    if (previous) return previous;
    let waiter: Waiter | undefined;
    try {
      return await bounded(new Promise<OwnerReceipt>((resolve, reject) => {
        waiter = { accept: receipt => { if (!predicate(receipt)) return false; resolve(receipt); return true; }, reject };
        this.waiters.add(waiter);
      }), `PID ${this.child.pid} IPC receipt`);
    } finally { if (waiter) this.waiters.delete(waiter); }
  }

  /** Birth proof from the inherited preload, not argv, ancestry or a post-hoc process census. */
  ownership(): Owned {
    const entry = this.launches.owned().find(record => record.pid === this.child.pid && record.argv[0] === this.script);
    if (!entry || !same(entry) || entry.pid === process.pid || entry.pid === process.ppid) {
      throw new Error(`Cannot prove birth/current ownership of PID ${this.child.pid}; retained ${this.launches.file}`);
    }
    return entry;
  }

  async killAndJoin(): Promise<Close> {
    if (this.terminal) return this.terminal;
    // Revalidate the launch-log birth immediately before EACH signal. Never use child.kill
    // on a guessed/reused PID, stopOwned's polling loop, or a process-wide cleanup routine.
    let entry: Owned;
    try { entry = this.ownership(); }
    catch (error) {
      // Startup can fail before ready, or exit can precede delivery of close. Join only;
      // absence of birth proof is NEVER permission to signal. A live unproved child
      // times out here and retains its evidence root for diagnosis.
      try { return await bounded(this.closed, "close without current birth proof", 10_000); }
      catch { throw error; }
    }
    if (!same(entry)) return bounded(this.closed, `close after PID ${entry.pid} changed`, 10_000);
    try { process.kill(entry.pid, "SIGKILL"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    return bounded(this.closed, `actual close after SIGKILL PID ${entry.pid}`, 10_000);
  }

  async checkpoint(): Promise<OwnerReceipt> {
    const token = `checkpoint:${this.receipts.length}`;
    const waiting = this.receipt(message => (message.type === "checkpoint" || message.type === "checkpoint-refused") && message.token === token);
    this.child.send({ type: "checkpoint", token }, error => { if (error) this.fail(error); });
    return waiting;
  }

  diagnostics(): string { return this.stderr; }
}
