import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

export type RetainedProcessState = "running" | "terminal-live" | "unknown-identity" | "missing-identity";
export const retainedProcessStates: RetainedProcessState[] = ["running", "terminal-live", "unknown-identity", "missing-identity"];

/** A detached worker with its own persistent nested record, but no managing handle/marker. */
export const retainedProcessWorker = async (runDirectory: string, cwd: string, state: RetainedProcessState) => {
  fs.mkdirSync(runDirectory, { recursive: true });
  const taskFile = path.join(runDirectory, "task.txt");
  const statusFile = path.join(runDirectory, "status.json");
  fs.writeFileSync(taskFile, "retained nested process input");
  const worker = spawn(process.execPath, ["-e", `
    const fs = require("node:fs");
    const [taskFile, statusFile, state] = process.argv.slice(1);
    fs.readFileSync(taskFile, "utf8");
    fs.writeFileSync(statusFile, JSON.stringify({
      status: state === "running" ? "running" : "completed", transport: "process",
      ...(state === "missing-identity" ? {} : { sessionId: state === "unknown-identity" ? "not-a-pid" : String(process.pid) }),
      cwd: process.cwd(), startedAt: 1, updatedAt: 2,
    }));
    setInterval(() => fs.readFileSync(taskFile, "utf8"), 1000);
    process.send("ready");
  `, taskFile, statusFile, state], { cwd, detached: true, stdio: ["ignore", "ignore", "ignore", "ipc"] });
  const exited = new Promise<void>(resolve => worker.once("close", () => resolve()));
  const stop = async () => {
    if (worker.exitCode === null && worker.signalCode === null) worker.kill("SIGTERM");
    await exited; // Reap/release the native process handle before Windows cwd removal.
  };
  try {
    await new Promise<void>((resolve, reject) => {
      worker.once("error", reject);
      worker.once("message", () => resolve());
      worker.once("close", () => reject(new Error("Retained process exited before readiness")));
    });
    return { worker, taskFile, statusFile, stop,
      confirmExit: () => fs.writeFileSync(statusFile, JSON.stringify({
        status: "completed", transport: "process", sessionId: String(worker.pid), finishedAt: Date.now(),
      })),
    };
  } catch (error) {
    await stop();
    throw error;
  }
};
