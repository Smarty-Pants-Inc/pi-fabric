import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

interface Message { phase: string; code?: string }
const workerFile = fileURLToPath(new URL("./fixtures/root-lock-race-worker.ts", import.meta.url));
const worker = (root: string, role: string, branch: string) => {
  const child = spawn("bun", [workerFile, root, role, branch], {
    env: { ...process.env, PI_FABRIC_MESH_ROOT: root },
    stdio: ["pipe", "pipe", "pipe"], timeout: 8_000,
  });
  const messages: Message[] = [];
  let pending: { resolve: (message: Message) => void; reject: (error: Error) => void } | undefined;
  let text = "";
  let stderr = "";
  child.stderr.on("data", value => { stderr += String(value); });
  child.stdout.on("data", value => {
    text += String(value);
    while (text.includes("\n")) {
      const newline = text.indexOf("\n");
      const message = JSON.parse(text.slice(0, newline)) as Message;
      text = text.slice(newline + 1);
      if (pending) { const current = pending; pending = undefined; current.resolve(message); }
      else messages.push(message);
    }
  });
  child.on("error", error => { pending?.reject(error); pending = undefined; });
  child.on("exit", code => { pending?.reject(new Error(`race worker ${role} exited ${code}: ${stderr}`)); pending = undefined; });
  const next = (): Promise<Message> => {
    const queued = messages.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending = undefined;
        reject(new Error(`race worker ${role} barrier timed out: ${stderr}`));
      }, 3_000);
      pending = {
        resolve: value => { clearTimeout(timer); resolve(value); },
        reject: error => { clearTimeout(timer); reject(error); },
      };
    });
  };
  return { child, next, resume: () => child.stdin.write("1") };
};
const stop = async (child: ChildProcessWithoutNullStreams): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const ended = new Promise<void>(resolve => child.once("exit", () => resolve()));
  child.stdin.end();
  const timer = setTimeout(() => child.kill("SIGKILL"), 1_000);
  try { await ended; } finally { clearTimeout(timer); }
};

describe("root admission during stale-lock recovery", () => {
  it.each(["owner", "ownerless"])("keeps continuous exclusion with two %s reapers and a replacement paused before commit", async branch => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-private-three-root-race-"));
    const children: ChildProcessWithoutNullStreams[] = [];
    const start = (role: string) => {
      const run = worker(root, role, branch);
      children.push(run.child);
      return run;
    };
    try {
      const lock = path.join(root, ".lock");
      const ownerPath = path.join(lock, "owner");
      fs.mkdirSync(lock);
      if (branch === "owner") fs.writeFileSync(ownerPath, `stale\n999999999\n${Date.now() - 60_000}\n`);
      else {
        const past = new Date(Date.now() - 60_000);
        fs.utimesSync(lock, past, past);
      }
      // A has read the stale instance. B reaps it and scans registrations,
      // holding its real fresh lock while paused BEFORE its ownership commit.
      const a = start("A");
      expect(await a.next()).toEqual({ phase: "checked-stale" });
      const b = start("B");
      expect(await b.next()).toEqual({ phase: "scanned-before-commit" });
      const freshToken = fs.readFileSync(ownerPath, "utf8");
      a.resume();
      let aResult = await a.next();
      const c = start("C");
      // The old rename-and-restore implementation exposes a missing canonical
      // lock here. C must not enter even while A is paused after the real move.
      const cResult = await c.next();
      if (aResult.phase === "moved-lock") { a.resume(); aResult = await a.next(); }
      expect.soft(aResult).toEqual({ phase: "result", code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect.soft(cResult).toEqual({ phase: "result", code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect.soft(fs.readFileSync(ownerPath, "utf8")).toBe(freshToken);
      b.resume();
      expect(await b.next()).toEqual({ phase: "result", code: "admitted" });
      const directory = path.join(root, "root-registrations");
      const registrations = fs.readdirSync(directory).filter(name => name.endsWith(".json"))
        .map(name => JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")));
      expect.soft(registrations).toHaveLength(1);
      expect(registrations).toContainEqual(expect.objectContaining({ sessionId: "B", name: "shared-name" }));
      // Real processes, not synthetic liveness: all contenders stay alive until
      // after this assertion, so another admitted claim could not be swept as dead.
      for (const child of children) expect(process.kill(child.pid!, 0)).toBe(true);
    } finally {
      await Promise.all(children.map(stop));
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
