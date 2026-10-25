import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { consumeCompletion, pendingCompletions } from "../src/agents/completion-journal.js";
import { launchLog, stopAllOwned, type LaunchLog } from "./helpers/owned-processes.js";

// Recorded catch: smarty-dev#16691, a Main exited at its shutdown deadline while
// its process task remained alive. Use the real manager/transport/worker boundary.
const workerPath = path.resolve(process.env.FABRIC_PARENT_EXIT_WORKER ?? "dist/worker.js");
const fixture = path.resolve("tests/fixtures/orphan-spawner.mjs");
const launches: LaunchLog[] = [];
const running = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return false;
    throw error; // unknown identity/visibility is not proof of exit
  }
};
afterEach(async () => {
  for (const log of launches.splice(0)) {
    // Only recorded, birth-checked processes; zombies cannot execute and init
    // may not reap them promptly on a containerized test host.
    await stopAllOwned(log.owned().filter(entry => running(entry.pid)), 100, 2000);
    // The runner owns cleanup of these synthetic run/log fixtures.
  }
});
const eventReader = (child: ChildProcess) => {
  const events: Array<Record<string, unknown>> = [];
  const wakes = new Set<() => void>();
  let buffer = "", stderr = "";
  child.stdout!.on("data", data => {
    buffer += String(data);
    for (let end = buffer.indexOf("\n"); end >= 0; end = buffer.indexOf("\n")) {
      const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
      try { events.push(JSON.parse(line)); } catch { /* diagnostics are not fixture events */ }
    }
    for (const wake of wakes) wake();
  });
  child.stderr!.on("data", data => { stderr += String(data); });
  return (event: string) => new Promise<Record<string, unknown>>((resolve, reject) => {
    const timer = setTimeout(() => { wakes.delete(check); reject(new Error(`No ${event}: ${stderr}; stdout: ${buffer}`)); }, 60_000);
    const check = () => {
      const index = events.findIndex(entry => entry.event === event);
      if (index < 0) return;
      clearTimeout(timer); wakes.delete(check); resolve(events.splice(index, 1)[0]!);
    };
    wakes.add(check); check();
  });
};
const terminalRecord = (file: string) => new Promise<Record<string, unknown>>((resolve, reject) => {
  const finish = (error?: Error, record?: Record<string, unknown>) => {
    clearTimeout(timer); watcher.close();
    if (error) reject(error); else resolve(record!);
  };
  const check = () => {
    try {
      const record = JSON.parse(fs.readFileSync(file, "utf8"));
      if (["completed", "failed", "stopped", "timed_out"].includes(record.status)) finish(undefined, record);
    } catch { /* atomic status publication is the wake source */ }
  };
  const watcher = fs.watch(path.dirname(file), check);
  const timer = setTimeout(() => finish(new Error("Orphan worker did not settle within 12s")), 12_000);
  check();
});

describe.skipIf(process.platform !== "linux" || !fs.existsSync(workerPath))("process worker spawner death", () => {
  it.each(["SIGTERM", "SIGKILL", "shutdown-deadline", "before-ack"])("drains and retains its run after %s without manager close", async mode => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-parent-exit-")));
    const log = launchLog(root);
    launches.push(log);
    fs.mkdirSync(path.join(root, "profile"));
    fs.writeFileSync(path.join(root, "profile/settings.json"), "{}");
    const parent = spawn(process.execPath, [fixture, root, workerPath, mode], {
      cwd: process.cwd(), env: { ...process.env, ...log.env,
        ...(mode === "before-ack" ? { NODE_OPTIONS: `${log.env.NODE_OPTIONS} --import=${pathToFileURL(path.resolve("tests/fixtures/withhold-custody-ack.mjs")).href}` } : {}),
        PI_CODING_AGENT_DIR: path.join(root, "profile"), XDG_RUNTIME_DIR: "", DBUS_SESSION_BUS_ADDRESS: "",
        TMPDIR: root, TMP: root, TEMP: root }, stdio: ["pipe", "pipe", "pipe"],
    });
    const exited = once(parent, "exit");
    const next = eventReader(parent);
    const ready = await next("ready");
    const native = mode === "before-ack" ? undefined : JSON.parse(fs.readFileSync(path.join(root, "native.json"), "utf8"));
    const worker = Number(ready.worker);
    expect(running(worker)).toBe(true);
    if (native) {
      expect(running(native.pid)).toBe(true);
      expect(running(native.descendant)).toBe(true);
    }
    // Detached work survives the calling turn/observation, not the owning host.
    parent.stdin!.write("status\n");
    expect(await next("status")).toMatchObject({ status: "running" });
    const run = path.join(root, "runs", String(ready.id));
    const terminal = terminalRecord(path.join(run, "status.json"));
    const started = Date.now();
    if (mode !== "shutdown-deadline") parent.kill(mode === "SIGKILL" ? "SIGKILL" : "SIGTERM");
    else {
      parent.stdin!.write("deadline\n");
      expect(await next("shutdown-deadline")).toMatchObject({ managerCloseReached: false });
    }
    const exit = await exited;
    expect(exit).toEqual(mode === "shutdown-deadline" ? [1, null] : [null, mode === "SIGKILL" ? "SIGKILL" : "SIGTERM"]);
    const record = await terminal;
    const text = native ? "partial work retained" : "";
    expect(record).toMatchObject({ status: "stopped", error: native ? "Agent stopped: spawner disconnected"
      : "Agent stopped before execution launch: spawner disconnected", text });
    expect(Date.now() - started).toBeLessThan(12_000);
    if (native) {
      expect(running(native.pid)).toBe(false);
      expect(running(native.descendant)).toBe(false);
    } else expect(fs.existsSync(path.join(root, "native.json"))).toBe(false);
    // status.json is the normal wake. With no native parent handle left to join,
    // allow one bounded final event-loop drain before checking the worker's exit.
    if (running(worker)) await new Promise(resolve => setTimeout(resolve, 1000));
    expect(running(worker)).toBe(false);
    expect(Date.now() - started).toBeLessThan(13_000);
    if (native) {
      expect(fs.readFileSync(path.join(root, "native-trace.jsonl"), "utf8")).toBe('{"synthetic":"raw trace retained"}\n');
      expect(fs.readFileSync(path.join(run, "events.jsonl"), "utf8")).toContain(text);
    }
    expect(fs.readFileSync(path.join(run, "task.txt"), "utf8")).toContain("keep partial work");
    // The dead owner's immutable completion manifest still recovers and archives
    // the stopped outcome. Neither recovery nor receipt deletes the raw run.
    const mesh = path.join(root, "mesh");
    expect(pendingCompletions(mesh, root)).toEqual([expect.objectContaining({ result: expect.objectContaining({
      id: ready.id, status: "stopped", text,
    }) })]);
    consumeCompletion(mesh, String(ready.id), "orphan-owner");
    expect(fs.readdirSync(path.join(mesh, "agent-completions", "archive"))).toHaveLength(1);
    expect(fs.existsSync(path.join(run, "events.jsonl"))).toBe(true);
    expect(fs.existsSync(path.join(run, "status.json"))).toBe(true);
  }, 80_000);
});
