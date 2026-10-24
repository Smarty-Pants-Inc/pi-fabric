import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { launchLog, stopAllOwned, type LaunchLog } from "./owned-processes.js";

/**
 * Multi-process test harness (smarty-dev#8082): real processes (a root Main, the `fabric-actors` remover, the root's
 * ResidentHost) run the
 * real src code in tests/fixtures/process-harness-child.mjs, pause at a chosen point, and get real signals. Every
 * process it starts records itself in a launch log (owned-processes.ts), and `close()` stops only those.
 */
export type HarnessEvent = { pid: number; event: string; [key: string]: unknown };

export interface HarnessChild {
  readonly process: ChildProcess;
  readonly pid: number;
  /** The next event with this name (earlier unread events of other names are kept). */
  next(event: string, timeoutMs?: number): Promise<HarnessEvent>;
  send(line: string): void;
  /** Send `signal` and resolve the exit as [code, signal]. */
  kill(signal: NodeJS.Signals): Promise<[number | null, NodeJS.Signals | null]>;
  readonly stderr: () => string;
}

export type ResidentPause = "beforeRemove" | "beforeCheck" | "beforeDelete" | "fence-temp" | "fence-link" | "none";
export type ResidentCall =
  | { kind: "hook"; name: "beforeCheck" | "beforeDelete"; fenced: boolean }
  | { kind: "rm"; path: string; fenced: boolean }
  /** smarty-dev#16444: a whole actor tree's one archive rename (performed: it deletes nothing). */
  | { kind: "archive"; path: string; fenced: boolean }
  | { kind: "remove"; id: string; options: { wait?: boolean; operator?: true } };

const CHILD = path.resolve("tests/fixtures/process-harness-child.mjs");

export const processHarness = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-harness-"));
  let log: LaunchLog;
  try { log = launchLog(root); }
  catch (error) { fs.rmSync(root, { recursive: true, force: true }); throw error; }
  const children: ChildProcess[] = [];
  const start = (args: string[]): HarnessChild => {
    const child = spawn(process.execPath, [CHILD, ...args], { cwd: process.cwd(), env: { ...process.env, ...log.env },
      stdio: ["pipe", "pipe", "pipe"] });
    children.push(child);
    const events: HarnessEvent[] = [];
    const waiters: Array<() => void> = [];
    let buffered = "", stderr = "";
    child.stdout!.on("data", chunk => {
      buffered += String(chunk);
      for (let end = buffered.indexOf("\n"); end >= 0; end = buffered.indexOf("\n")) {
        const line = buffered.slice(0, end); buffered = buffered.slice(end + 1);
        try { events.push(JSON.parse(line) as HarnessEvent); } catch { stderr += `[stdout] ${line}\n`; }
      }
      for (const wake of waiters.splice(0)) wake();
    });
    child.stderr!.on("data", chunk => { stderr += String(chunk); });
    const exited = once(child, "exit") as Promise<[number | null, NodeJS.Signals | null]>;
    exited.then(() => { for (const wake of waiters.splice(0)) wake(); }, () => undefined);
    return {
      process: child,
      pid: child.pid!,
      stderr: () => stderr,
      next: async (event, timeoutMs = 30_000) => {
        const deadline = Date.now() + timeoutMs;
        for (;;) {
          const index = events.findIndex(entry => entry.event === event);
          if (index >= 0) return events.splice(index, 1)[0]!;
          if (child.exitCode !== null || child.signalCode !== null) {
            throw new Error(`Harness child ${child.pid} exited (${child.exitCode ?? child.signalCode}) before "${event}": ${stderr}`);
          }
          const left = deadline - Date.now();
          if (left <= 0) throw new Error(`Harness child ${child.pid}: no "${event}" within ${timeoutMs} ms: ${stderr}`);
          await new Promise<void>(resolve => {
            const timer = setTimeout(resolve, left);
            waiters.push(() => { clearTimeout(timer); resolve(); });
          });
        }
      },
      send: line => { child.stdin!.write(line + "\n"); },
      kill: async signal => {
        if (child.exitCode === null && child.signalCode === null) child.kill(signal);
        return exited;
      },
    };
  };
  return {
    root,
    /** A separate root Main process; resolves once its ParticipantDirectory is constructed. */
    main: async (meshRoot: string, rootId: string) => {
      const child = start(["main", meshRoot, rootId]);
      await child.next("ready");
      return Object.assign(child, {
        publish: async () => { child.send("publish"); return child.next("publish"); },
        /** The ordinary same-root `removeActor` request through the resident channel (not the operator route). */
        removeActor: async (id: string) => { child.send(`removeActor ${id}`); return child.next("removeActor", 60_000); },
        /** A clean Main stop: its ParticipantDirectory closes (and gives up its host lease), then it exits. */
        stop: async () => { child.send("close"); await child.next("closed"); return child.kill("SIGTERM"); },
      });
    },
    /** The real `fabric-actors` CLI in its own process, paused at `pauseAt` (see the child fixture). */
    remover: (pauseAt: "fence-temp" | "fence-link" | "beforeTreeMove" | "none", cli: string[]) => start(["remover", pauseAt, ...cli]),
    /** The root's real ResidentHost in its own process (from `<residencyRoot>/config.json`), with one armed pause.
     * `recordTreeDeletes` (argv, that child only): whole actor-tree removals are recorded, not performed (#4664). */
    resident: async (residencyRoot: string, pauseAt: ResidentPause, options: { recordTreeDeletes?: boolean } = {}) => {
      const child = start(["resident", residencyRoot, pauseAt, ...(options.recordTreeDeletes ? ["record-tree-deletes"] : [])]);
      await child.next("ready", 60_000);
      return Object.assign(child, {
        arm: async () => { child.send("arm"); await child.next("armed"); },
        report: async () => { child.send("report"); return (await child.next("report")).calls as ResidentCall[]; },
        stop: async () => { child.send("close"); await child.next("closed", 60_000); return child.kill("SIGTERM"); },
      });
    },
    /** A host writing its generation exit record, paused before or after the publishing link (smarty-dev#7950). */
    generationExit: (residencyRoot: string, generation: string, pauseAt: "before-link" | "after-link") =>
      start(["generation-exit", residencyRoot, generation, pauseAt]),
    /** The real launcher and child, paused just before its generation-private startup-error link. */
    launcherError: (configPath: string, generation: string) => start(["launcher-error", configPath, generation]),
    close: async () => {
      try {
        for (const child of children) child.stdin?.destroy();
        await stopAllOwned(log.owned(), 2_000, 5_000);
      } finally { fs.rmSync(root, { recursive: true, force: true }); }
    },
  };
};
export type ProcessHarness = ReturnType<typeof processHarness>;
