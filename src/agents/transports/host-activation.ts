import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { AgentTransportLaunch, HostActivationQueue } from "../types.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";
import { findExecutable, processIsAlive } from "./process-utils.js";

export interface HostActivationPolicy {
  limit: number;
  scope?: "actors" | "all";
  /** Trusted embedding/test override; never a workspace or worker argument. */
  directory?: string;
}

interface Ticket {
  id: string;
  activationId: string;
  sequence: number;
  pid: number;
  start: string;
  boot: string;
  waitingSince: number;
}

const startTime = (pid: number): string | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    // An unreaped zombie cannot make progress, even though kill(pid, 0) succeeds.
    return fields[0] === "Z" ? "exited" : fields[19];
  } catch { return undefined; }
};

/** flock(1) locks the inherited open file description. Its exit closes only
 * its reference: our FD retains the kernel lock, and is handed to the worker.
 * No helper/lease timeout can release an activation that is still running. */
const tryLock = async (executable: string, file: string, retainedFd?: number): Promise<number | undefined> => {
  const fd = retainedFd ?? fs.openSync(file, "a+", 0o600);
  try {
    const code = await new Promise<number | null>((resolve, reject) => {
      const child = spawn(executable, ["--exclusive", "--nonblock", "--conflict-exit-code", "75", "3"], {
        stdio: ["ignore", "ignore", "ignore", fd],
      });
      child.once("error", reject);
      child.once("close", resolve);
    });
    if (code === 0) return fd;
    if (code !== 75) throw new Error(`Host activation flock failed (exit ${code})`);
    if (retainedFd === undefined) fs.closeSync(fd);
    return undefined;
  } catch (error) { if (retainedFd === undefined) fs.closeSync(fd); throw error; }
};

const pause = (signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
  const abort = (): void => { clearTimeout(timer); signal?.removeEventListener("abort", abort); reject(new Error("Host activation wait aborted")); };
  const timer = setTimeout(() => { signal?.removeEventListener("abort", abort); resolve(); }, 35 + Math.floor(Math.random() * 40));
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
});

/** FIFO is ticket/admission order, not OS scheduling order after spawn. Queue
 * edits are atomic under a short kernel lock; only its live head tries tokens.
 * Cancellation removes its ticket before returning. Crash/reboot/PID reuse
 * are reaped by the next queue transaction without stealing a live claim. */
export const acquireHostActivation = async (
  policy: HostActivationPolicy,
  request: Pick<AgentTransportLaunch, "id" | "signal" | "authorize" | "onHostQueue">,
  // Trusted native custody: reacquire the same inherited open description.
  retainedFd?: number,
): Promise<{ fd: number; ticket: number }> => {
  if (!Number.isSafeInteger(policy.limit) || policy.limit < 1) throw new Error("agents.hostActivationLimit must be a positive safe integer");
  if (process.platform !== "linux") throw new Error("agents.hostActivationLimit requires Linux flock; refusing an uncapped launch");
  const executable = findExecutable("flock");
  if (!executable) throw new Error("agents.hostActivationLimit requires flock; refusing an uncapped launch");
  const directory = policy.directory ?? path.join(os.homedir(), ".local/share/smarty-dev/fabric-host-tokens");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const boot = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
  const start = startTime(process.pid);
  if (!start || start === "exited") throw new Error("Cannot identify host activation waiter process");
  const queuePath = path.join(directory, "queue.json");
  const sequencePath = path.join(directory, "sequence");
  let own: Ticket | undefined;
  const transaction = async <T>(operation: (queue: Ticket[]) => Promise<T>): Promise<T> => {
    let guard: number | undefined;
    while ((guard = await tryLock(executable, path.join(directory, "queue.lock"))) === undefined) await pause();
    try {
      let queue: Ticket[] = [];
      try { queue = JSON.parse(fs.readFileSync(queuePath, "utf8")) as Ticket[]; }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      if (!Array.isArray(queue) || queue.some(ticket => !Number.isSafeInteger(ticket.sequence) || !Number.isSafeInteger(ticket.pid) || typeof ticket.id !== "string" || typeof ticket.start !== "string" || typeof ticket.boot !== "string")) {
        throw new Error("Invalid host activation queue; refusing an uncapped launch");
      }
      queue = queue.filter(ticket => ticket.boot === boot && processIsAlive(ticket.pid) && (startTime(ticket.pid) ?? ticket.start) === ticket.start);
      const result = await operation(queue);
      // Fixed temporary name is safe under queue.lock. A crash leaves at most
      // a discarded temporary snapshot, never a torn queue or a leaked token.
      fs.writeFileSync(`${queuePath}.tmp`, JSON.stringify(queue), { mode: 0o600 });
      fs.renameSync(`${queuePath}.tmp`, queuePath);
      return result;
    } finally { fs.closeSync(guard); }
  };
  let previousPosition: number | undefined;
  const notify = (queue: HostActivationQueue | undefined): void => {
    if (queue?.position === previousPosition) return;
    previousPosition = queue?.position;
    try { request.onHostQueue?.(queue); } catch { /* Status observers cannot veto token custody. */ }
  };
  let acquired: number | undefined;
  try {
    assertTransportLaunchAllowed(request);
    await transaction(async queue => {
      assertTransportLaunchAllowed(request);
      // Append-only monotonic counter: reserve before publishing the ticket.
      // A crash may leave a gap, but can never reuse an issued sequence.
      const counter = fs.openSync(sequencePath, "a+", 0o600);
      let sequence: number;
      try {
        const size = fs.fstatSync(counter).size;
        const tail = Buffer.alloc(Math.min(size, 64));
        fs.readSync(counter, tail, 0, tail.length, size - tail.length);
        const previous = tail.toString("utf8").trim().split("\n").at(-1);
        sequence = previous ? Number(previous) + 1 : 1;
        if (!Number.isSafeInteger(sequence) || sequence < 1) throw new Error("Invalid host activation sequence");
        fs.writeSync(counter, `${sequence}\n`);
        fs.fsyncSync(counter);
      } finally { fs.closeSync(counter); }
      own = { id: randomUUID(), activationId: request.id, sequence, pid: process.pid, start, boot, waitingSince: Date.now() };
      queue.push(own);
    });
    for (;;) {
      assertTransportLaunchAllowed(request);
      let status: HostActivationQueue | undefined;
      await transaction(async queue => {
        assertTransportLaunchAllowed(request);
        const index = queue.findIndex(ticket => ticket.id === own!.id);
        if (index < 0) throw new Error("Host activation ticket disappeared");
        if (index === 0) {
          for (let token = 0; token < (retainedFd === undefined ? policy.limit : 1); token++) {
            acquired = await tryLock(executable, path.join(directory, `token-${token}.lock`), retainedFd);
            if (acquired !== undefined) { queue.splice(0, 1); break; }
          }
        }
        if (acquired === undefined) status = { position: index + 1, waitingSince: own!.waitingSince, limit: policy.limit };
      });
      assertTransportLaunchAllowed(request);
      notify(status);
      if (acquired !== undefined) {
        assertTransportLaunchAllowed(request);
        const fd = acquired;
        const ticket = own!.sequence;
        acquired = undefined;
        own = undefined; // Ticket already removed atomically with acquisition.
        return { fd, ticket };
      }
      await pause(request.signal);
    }
  } catch (error) {
    if (acquired !== undefined && retainedFd === undefined) fs.closeSync(acquired);
    throw error;
  } finally {
    try {
      if (own) await transaction(async queue => {
        const index = queue.findIndex(ticket => ticket.id === own!.id);
        if (index >= 0) queue.splice(index, 1);
      });
    } finally { notify(undefined); }
  }
};
