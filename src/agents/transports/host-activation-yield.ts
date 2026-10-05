import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { acquireHostActivation } from "./host-activation.js";
import { findExecutable } from "./process-utils.js";

interface Cycle {
  callers: Set<string>;
  released: Promise<void>;
  resumed: Promise<void>;
  resolve(): void;
  reject(error: unknown): void;
  reacquiring: boolean;
}
let cycle: Cycle | undefined;
const programs = new Set<string>();

/** Only a native outer execution fence may own suspension. Detached host
 * engines have no such owner and must reject dependencies before admission. */
export const beginHostActivationProgram = (caller: string): void => { programs.add(caller); };

const policy = () => {
  const limit = Number(process.env.PI_FABRIC_HOST_ACTIVATION_LIMIT);
  const token = fs.readlinkSync("/proc/self/fd/3");
  if (!Number.isSafeInteger(limit) || limit < 1 || !/^token-\d+\.lock$/.test(path.basename(token))) {
    throw new Error("Invalid inherited host activation custody");
  }
  return { limit, directory: path.dirname(token) };
};
const unlock = async (): Promise<void> => {
  policy(); // Fail closed before changing an unrelated inherited descriptor.
  const executable = findExecutable("flock");
  if (!executable) throw new Error("Host activation yield requires flock");
  const code = await new Promise<number | null>((resolve, reject) => {
    const child = spawn(executable, ["--unlock", "3"], { stdio: ["ignore", "ignore", "ignore", 3] });
    child.once("error", reject); child.once("close", resolve);
  });
  if (code !== 0) throw new Error(`Host activation unlock failed (exit ${code})`);
};

/** A blocked Fabric program does not occupy execution capacity. Keep the
 * worker's original open description: kernel crash custody remains intact.
 * Concurrent programs share one yield and all fence their return on resumption.
 * Calls in a single program can spawn, message and then join without prematurely
 * reclaiming the capacity their dependent child needs. */
export const yieldHostActivation = async (caller: string): Promise<void> => {
  if (!programs.has(caller)) throw new Error("Capped agent dependencies require an enclosing fabric_exec program");
  while (cycle?.reacquiring) await cycle.resumed;
  if (!cycle) {
    let resolve!: () => void; let reject!: (error: unknown) => void;
    const resumed = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    void resumed.catch(() => undefined);
    cycle = { callers: new Set(), released: unlock(), resumed, resolve, reject, reacquiring: false };
  }
  cycle.callers.add(caller);
  await cycle.released;
};

/** An admission fence, deliberately outside bounded best-effort finalizers. */
export const resumeHostActivation = async (caller: string): Promise<void> => {
  programs.delete(caller);
  const current = cycle;
  if (!current || !current.callers.delete(caller)) return;
  if (!current.callers.size) {
    current.reacquiring = true;
    try {
      await current.released;
      await acquireHostActivation(policy(), { id: process.env.PI_FABRIC_PARENT_RUN ?? caller }, 3);
      cycle = undefined;
      current.resolve();
    } catch (error) {
      current.reject(error);
      // A tool error alone would let Pi make another model call without a
      // slot. Terminate this native execution host; the worker owns teardown
      // of its execution tree. Never silently degrade admission after a yield.
      console.error("[pi-fabric] Cannot restore host activation custody:", error);
      process.exit(1);
    }
  }
  await current.resumed;
};
