import type { ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

/** Fixture-only OS teardown barrier, independent of runtime result/liveness settlement. */
export class ChildProcessCloseBarrier {
  readonly children: { child: ChildProcess; barrier: "exit" | "close"; released: boolean; done: Promise<void> }[] = [];

  observe(child: ChildProcess): ChildProcess {
    let resolve!: () => void;
    // IPC-only Node guests have no stdio handles to drain. Their runtime removes
    // even Node's internal disconnect listener, so ChildProcess.close may never
    // emit. Actual exit plus disconnected IPC is their barrier; require close
    // for children with real stdio (including CPython) and detached workers.
    const barrier: "exit" | "close" = child.channel && child.stdio.every(stream => stream === null) ? "exit" : "close";
    const entry = { child, barrier, released: false,
      done: new Promise<void>(done => { resolve = done; }) };
    this.children.push(entry);
    // Observe emission, not a removable listener: NodeProcessRuntime intentionally
    // removes all listeners when its result settles. No behavior or deadlines change.
    const emit = child.emit;
    let exited = false;
    child.emit = function (event: string | symbol, ...args: unknown[]) {
      try { return emit.call(this, event, ...args); }
      finally {
        if (event === "exit") exited = true;
        const released = entry.barrier === "close" ? event === "close"
          : exited && !child.connected && (event === "exit" || event === "disconnect");
        if (released) {
          entry.released = true;
          child.emit = emit;
          resolve();
        }
      }
    };
    return child;
  }

  snapshot() {
    return this.children.map(({ child, barrier, released }) => ({ pid: child.pid, barrier, released,
      connected: child.connected, exitCode: child.exitCode, signalCode: child.signalCode }));
  }

  async wait(): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      // This timer only diagnoses a missing exit; it never authorizes root removal.
      await Promise.race([
        Promise.all(this.children.map(({ done }) => done)),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`Children did not exit/close: ${JSON.stringify(this.snapshot())}`)), 10_000);
        }),
      ]);
    } finally { if (timer) clearTimeout(timer); }
  }
}

/** Opt-in Linux evidence at the actual rm boundary, including cwd and all readable FDs. */
export const assertLinuxRootReleased = (root: string, children: ReturnType<ChildProcessCloseBarrier["snapshot"]>) => {
  if (process.platform !== "linux") return undefined;
  const alive: number[] = [];
  for (const { pid } of children) {
    if (pid === undefined) continue;
    try { process.kill(pid, 0); alive.push(pid); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
  }
  const references: { pid: number; handle: string; target: string }[] = [];
  let inaccessible = 0;
  const inspect = (pid: number, handle: string) => {
    try {
      const target = fs.readlinkSync(`/proc/${pid}/${handle}`);
      const clean = target.replace(/ \(deleted\)$/, "");
      if (clean === root || clean.startsWith(root + path.sep)) references.push({ pid, handle, target });
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === "EACCES" || code === "EPERM") inaccessible++;
      else if (code !== "ENOENT" && code !== "ESRCH") throw error;
    }
  };
  for (const name of fs.readdirSync("/proc")) {
    if (!/^\d+$/.test(name)) continue;
    const pid = Number(name);
    inspect(pid, "cwd");
    let fds: string[];
    try { fds = fs.readdirSync(`/proc/${pid}/fd`); }
    catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === "EACCES" || code === "EPERM") { inaccessible++; continue; }
      if (code === "ENOENT" || code === "ESRCH") continue;
      throw error;
    }
    for (const fd of fds) inspect(pid, `fd/${fd}`);
  }
  const evidence = { alive, references, inaccessible };
  if (alive.length || references.length) throw new Error(`Root is still held: ${JSON.stringify(evidence)}`);
  return evidence;
};
