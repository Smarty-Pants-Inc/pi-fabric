import fs from "node:fs";
import type { ChildProcess } from "node:child_process";

/** Observe native/tree exit with notifications and ONE absolute deadline.
 * Without an execution-specific cgroup, descendant exit has no portable Node
 * notification. Check it at native events and the deadline, never by polling.
 * Native close alone is not evidence that an owned process group emptied. */
export const waitForExecutionExit = (
  child: ChildProcess,
  exited: () => boolean,
  deadline: number,
  cgroupEventsFile?: string,
  subscribeCleanup?: (notify: () => void) => () => void,
): Promise<boolean> => new Promise((resolve, reject) => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let watcher: fs.FSWatcher | undefined;
  let done = false;
  let unsubscribeCleanup: (() => void) | undefined;
  const finish = (result: boolean, error?: unknown): void => {
    if (done) return;
    done = true;
    clearTimeout(timer);
    watcher?.close();
    unsubscribeCleanup?.();
    child.off("exit", changed);
    child.off("close", changed);
    if (error !== undefined) reject(error);
    else resolve(result);
  };
  const changed = (): void => {
    try { if (exited()) finish(true); }
    catch (error) { finish(false, error); }
  };
  child.on("exit", changed);
  child.on("close", changed);
  unsubscribeCleanup = subscribeCleanup?.(changed);
  if (done) { unsubscribeCleanup?.(); return; }
  if (cgroupEventsFile) {
    try {
      watcher = fs.watch(cgroupEventsFile, () => {
        try {
          if (/^populated\s+0$/m.test(fs.readFileSync(cgroupEventsFile, "utf8"))) changed();
        } catch { /* unreadable scope is not an exit receipt; deadline still fires */ }
      });
      watcher.on("error", () => { watcher?.close(); watcher = undefined; });
    } catch { /* unavailable notifications fall back to the bounded final check */ }
  }
  // Subscribe first, then inspect, so fast native/scope exit cannot be missed.
  changed();
  if (!done) timer = setTimeout(() => {
    try { finish(exited()); } catch (error) { finish(false, error); }
  }, Math.max(0, deadline - Date.now()));
});
