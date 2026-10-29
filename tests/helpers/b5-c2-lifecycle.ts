// B5 C2 only: a timed-out scenario still owns its build and native children.
// Keep: data-safety failure interleavings cannot be reached cheaply by a user flow.
import type { ChildProcess, ChildProcessWithoutNullStreams } from "node:child_process";

type BuildContext = { rebuild(): Promise<unknown>; cancel(): Promise<void>; dispose(): Promise<void> };
export type C2Child = Pick<ChildProcess, "pid" | "once" | "kill">
  & Pick<ChildProcessWithoutNullStreams, "stdout" | "stderr">;
type NativeExit = { code: number | null; signal: NodeJS.Signals | null };
export type C2Terminal = {
  pid: number | null; nativeExit: NativeExit | null; stdout: string; stderr: string; error: string | null;
};

export function c2Lifecycle(registerCleanup: (close: () => Promise<void>) => void) {
  let cancelled = false, closing: Promise<void> | undefined;
  const builds = new Set<{ release(): Promise<void>; done: Promise<void> }>();
  const children = new Set<{ child: C2Child; exit: NativeExit | null; closed: Promise<C2Terminal> }>();
  const assertActive = () => { if (cancelled) throw new Error("C2 process scenario ended; no further receiver may start"); };
  const custody = (terminal: C2Terminal) => {
    // A failed spawn has no native child. An error from an existing child is NOT exit evidence.
    if (terminal.pid !== null && terminal.nativeExit === null) throw new Error("C2 stdio closed without actual native exit");
  };
  const close = () => {
    cancelled = true; // Synchronous, before waiting for even a pending context factory.
    if (closing) return closing;
    const pending: Promise<unknown>[] = [];
    for (const build of builds) pending.push(build.release(), build.done);
    for (const entry of children) {
      if (entry.exit === null && entry.child.pid !== undefined) entry.child.kill("SIGKILL");
      pending.push(entry.closed.then(custody)); // Both actual death and drained stdio, never a timeout proxy.
    }
    closing = (async () => {
      const results = await Promise.allSettled(pending);
      const errors = results.flatMap(result => result.status === "rejected" ? [result.reason] : []);
      if (errors.length) throw new AggregateError(errors, "C2 cleanup failed");
    })();
    return closing;
  };
  // This must happen before the caller can start/await context creation or bundling.
  registerCleanup(close);

  return {
    close,
    async bundle(createContext: () => Promise<BuildContext>) {
      assertActive();
      const creating = createContext();
      let releasing: Promise<void> | undefined;
      const release = () => releasing ??= (async () => {
        const context = await creating;
        try { if (cancelled) await context.cancel(); }
        finally { await context.dispose(); }
      })();
      const work = (async () => {
        try {
          const context = await creating;
          assertActive(); // Late context resolution after cancellation must not even rebuild.
          await context.rebuild();
          assertActive(); // Nor may a late rebuild completion authorize a receiver.
        } finally { await release(); }
      })();
      const owned = { release, done: work.then(() => {}, () => {}) };
      builds.add(owned);
      void owned.done.then(() => builds.delete(owned));
      return work;
    },
    async run(start: () => C2Child, onTerminal?: (terminal: C2Terminal) => void) {
      assertActive(); // Check BEFORE spawn, including every successor receiver.
      const child = start();
      let stdout = "", stderr = "", error: Error | undefined;
      const entry = { child, exit: null as NativeExit | null, closed: undefined as unknown as Promise<C2Terminal> };
      entry.closed = new Promise<C2Terminal>(resolve => {
        child.stdout.setEncoding("utf8"); child.stderr.setEncoding("utf8");
        child.stdout.on("data", chunk => { stdout += String(chunk); });
        child.stderr.on("data", chunk => { stderr += String(chunk); });
        child.once("error", failure => { error = failure; });
        child.once("exit", (code, signal) => { entry.exit = { code, signal }; });
        child.once("close", () => resolve({ pid: child.pid ?? null, nativeExit: entry.exit, stdout, stderr, error: error?.message ?? null }));
      });
      children.add(entry);
      const terminal = await entry.closed; // `exit` alone can precede the last data event.
      onTerminal?.(terminal);
      custody(terminal);
      children.delete(entry);
      if (error) throw error;
      if (terminal.nativeExit === null) throw new Error("C2 child never emitted actual native exit");
      assertActive();
      return { ...terminal.nativeExit, stdout: terminal.stdout, stderr: terminal.stderr };
    },
  };
}
