import type { ChildProcess } from "node:child_process";

export interface ResidentChildExit {
  code: number | null;
  signal: NodeJS.Signals | null;
}

/** A receipt for the directly spawned child only, never whole-attempt membership. */
export interface ResidentChildLifetime {
  readonly exit: Promise<ResidentChildExit>;
  readonly exited: boolean;
  stop(): Promise<void>;
}

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/** Native exit is independent of Linux birth observation and inherited stdio.
 * A descendant can keep a pipe open after the child exits, delaying `close`.
 * Neither this receipt nor successful signaling authorizes resident fallback.
 */
export function watchResidentChild(child: ChildProcess): ResidentChildLifetime {
  let exited = false;
  let stopping: Promise<void> | undefined;
  const exit = new Promise<ResidentChildExit>(resolve => {
    const record = (code: number | null, signal: NodeJS.Signals | null): void => {
      if (exited) return;
      exited = true;
      resolve({ code, signal });
    };
    child.once("exit", record);
    // Failed spawn has no `exit` event. `close` also follows the spawn error,
    // and cannot turn an error from a failed kill into a fabricated receipt.
    child.once("close", record);
  });
  const stop = async (): Promise<void> => {
    for (const signal of ["SIGTERM", "SIGKILL"] as const) {
      if (exited) return;
      child.kill(signal);
      const deadline = Date.now() + 5_000;
      while (!exited && Date.now() < deadline) await delay(50);
      if (exited) return;
    }
    throw new Error("Resident child did not provide a native exit receipt");
  };
  return {
    exit,
    get exited() { return exited; },
    stop() { return stopping ??= stop(); },
  };
}
