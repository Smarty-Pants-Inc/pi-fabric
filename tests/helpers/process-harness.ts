import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { launchLog, stopAllOwned, type LaunchLog } from "./owned-processes.js";

/**
 * Multi-process test harness (smarty-dev#8082): real processes (a root Main, the `fabric-actors` remover) run the
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
        /** A clean Main stop: its ParticipantDirectory closes (and gives up its host lease), then it exits. */
        stop: async () => { child.send("close"); await child.next("closed"); return child.kill("SIGTERM"); },
      });
    },
    /** The real `fabric-actors` CLI in its own process, paused at `pauseAt` (see the child fixture). */
    remover: (pauseAt: "fence-temp" | "fence-link" | "beforeTreeMove" | "none", cli: string[]) => start(["remover", pauseAt, ...cli]),
    close: async () => {
      try {
        for (const child of children) child.stdin?.destroy();
        await stopAllOwned(log.owned(), 2_000, 5_000);
      } finally { fs.rmSync(root, { recursive: true, force: true }); }
    },
  };
};
export type ProcessHarness = ReturnType<typeof processHarness>;
