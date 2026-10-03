import fs from "node:fs";
import net from "node:net";
import { spawn } from "node:child_process";
import { once } from "node:events";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { RemoteRecords } from "../src/records/client.js";

const roots: string[] = [], actors: ActorManager[] = [], agents: AgentManager[] = [];
const identity = { id: "session:review-r1", name: "main", kind: "main" as const };
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-review-r1-")); roots.push(dir); return dir; };
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const until = async (predicate: () => boolean) => { for (let n = 0; !predicate(); n++) { if (n > 500) throw new Error("review-r1 probe timed out"); await delay(20); } };
const host = (dir: string, mesh = new MeshStore(path.join(dir, "mesh"), 65536, 100)) => {
  const engine = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs") }); agents.push(engine);
  const manager = new ActorManager("review-r1", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, engine, () => {}, {
    actorRoot: path.join(dir, "actors"), persistent: true, closeGraceMs: 50, preparationRetryMs: 50,
  }); actors.push(manager);
  return { manager, engine, mesh };
};
const descriptors = () => {
  const files = new Map<number, string>(), open = fs.openSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; });
  return files;
};
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(actors.splice(0).map(manager => manager.close()));
  await Promise.all(agents.splice(0).map(engine => engine.close()));
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe("PR A round 1 receipts", () => {
  it.skipIf(process.platform === "win32")("F2/S1 recovers acknowledged B after ordinary completion and actual SIGKILL replacement", async () => {
    const dir = root();
    const child = spawn("bun", [path.resolve("tests/fixtures/atomic-completion-crash.ts"), dir], { stdio: ["ignore", "pipe", "pipe"] });
    const exited = once(child, "exit");
    let output = "", errors = "";
    child.stdout.on("data", chunk => { output += chunk.toString(); }); child.stderr.on("data", chunk => { errors += chunk.toString(); });
    try { await until(() => output.includes("\n") || child.exitCode !== null); }
    finally { child.kill("SIGKILL"); await exited; }
    expect(output, errors).toContain("\n");
    const produced = JSON.parse(output.trim()) as { actorId: string; cursor: number; sequence: number; events: string[] };
    expect(produced.cursor).toBeGreaterThanOrEqual(produced.sequence);
    expect(produced.events).toEqual(["file", "rename", "namespace-failed"]);
    const actorDir = path.join(dir, "actors", produced.actorId), files = descriptors(), sync = fs.fsyncSync.bind(fs);
    let unavailable = true, failures = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (unavailable && files.get(fd) === actorDir) { failures++; throw new Error("replacement queue namespace still unavailable"); }
      sync(fd);
    });
    const replacement = host(dir), launched = vi.spyOn(replacement.engine, "run");
    await until(() => failures > 0); await delay(120);
    expect(launched).not.toHaveBeenCalled();
    unavailable = false;
    await until(() => launched.mock.calls.some(([request]) => request.task.includes("accepted crash B")) && replacement.manager.inFlightCount() === 0);
    expect(launched).toHaveBeenCalledTimes(1);
  }, 30000);

  it.each(process.platform === "win32" ? ["healthy", "file"] : ["healthy", "file", "namespace"])("F2/S1 ordinary completion preserves accepted backlog at the %s barrier", async fault => {
    const dir = root(), first = host(dir);
    const actor = await first.manager.create({ name: "ordinary", instructions: "Work", responseMode: "text", coalesce: false });
    let releaseA!: () => void, releaseFinalizer!: () => void;
    const holdA = new Promise<void>(resolve => { releaseA = resolve; });
    const holdFinalizer = new Promise<void>(resolve => { releaseFinalizer = resolve; });
    const run = first.engine.run.bind(first.engine);
    let aFinished = false;
    const launched = vi.spyOn(first.engine, "run").mockImplementation(async (...args) => {
      const result = await run(...args);
      aFinished = true; await holdA; return result;
    });
    first.manager.tell(actor.id, "ordinary A");
    await until(() => aFinished);
    first.manager.tell(actor.id, "accepted B");
    const actorDir = path.join(dir, "actors", actor.id);
    const queue = path.join(actorDir, fs.readdirSync(actorDir).find(file => /^queue-.+\.json$/.test(file))!);
    expect(JSON.parse(fs.readFileSync(queue, "utf8")).items.map((item: any) => item.payload.message)).toContain("accepted B");
    const files = descriptors(), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
    const events: string[] = [];
    let completing = false, unavailable = fault !== "healthy", finalizing = false, failures = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { rename(from, to); if (completing && String(to) === queue) events.push("rename"); });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = files.get(fd) ?? "";
      if (completing && file.startsWith(queue + ".") && file.endsWith(".tmp")) {
        events.push("file"); if (unavailable && fault === "file") { failures++; throw new Error("completion queue file barrier unavailable"); }
      }
      if (completing && file === actorDir) {
        events.push("namespace"); if (unavailable && fault === "namespace") { failures++; throw new Error("completion queue namespace unavailable"); }
      }
      sync(fd);
    });
    const put = first.mesh.put.bind(first.mesh);
    vi.spyOn(first.mesh, "put").mockImplementation(async request => {
      if (completing && request.key === `actors/review-r1/${actor.id}`) { finalizing = true; await holdFinalizer; }
      return put(request);
    });
    try {
      completing = true; releaseA(); await until(() => finalizing);
      expect(launched).toHaveBeenCalledTimes(1);
      if (fault === "healthy") {
        expect(events.indexOf("file")).toBeLessThan(events.indexOf("rename"));
        expect(events).toContain("file"); expect(events).toContain("rename");
        if (process.platform !== "win32") expect(events.slice(events.indexOf("rename") + 1)).toContain("namespace");
      } else {
        expect(failures).toBeGreaterThan(0);
        // A live retry must not launch B while the completion replacement is unconfirmed.
        releaseFinalizer(); await delay(180);
        expect(launched).toHaveBeenCalledTimes(1);
        first.manager.pauseForRelease();
      }
      // Replace the manager directly: no close/save, queue edits, or new ingress repairs it.
      const replacement = host(dir, new MeshStore(path.join(dir, "mesh"), 65536, 100));
      const recoveredRuns = vi.spyOn(replacement.engine, "run");
      if (fault !== "healthy") { await delay(120); expect(recoveredRuns).not.toHaveBeenCalled(); }
      unavailable = false;
      await until(() => recoveredRuns.mock.calls.some(([request]) => request.task.includes("accepted B")) && replacement.manager.inFlightCount() === 0);
      expect(recoveredRuns.mock.calls.filter(([request]) => request.task.includes("accepted B"))).toHaveLength(1);
      expect(replacement.manager.messages(actor.id).some(message => message.direction === "out" && !message.error)).toBe(true);
    } finally { unavailable = false; first.manager.pauseForRelease(); releaseA(); releaseFinalizer(); }
  }, 20000);

  it("S2 known-token opens repay failed publication without registering again", async () => {
    const dir = root(), credentialDir = path.join(dir, "credentials"); fs.mkdirSync(credentialDir);
    const files = descriptors(), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
    let tokenRenamed = false, unavailable = true, registered = 0, whoami = 0, confirmations = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to).startsWith(credentialDir) && JSON.parse(fs.readFileSync(to, "utf8")).token) tokenRenamed = true;
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = files.get(fd) ?? "";
      if (tokenRenamed && (process.platform === "win32" ? file.startsWith(credentialDir + path.sep) && file.endsWith(".tmp") : file === credentialDir)) {
        if (unavailable) throw new Error("token confirmation unavailable"); confirmations++;
      }
      sync(fd);
    });
    const server = net.createServer(socket => {
      let input = "";
      socket.on("data", chunk => {
        input += chunk.toString(); let boundary: number;
        while ((boundary = input.indexOf("\n")) >= 0) {
          const request = JSON.parse(input.slice(0, boundary)); input = input.slice(boundary + 1);
          if (request.method === "register") registered++;
          if (request.method === "whoami") whoami++;
          socket.write(`${JSON.stringify({ id: request.id, ok: true, result: request.method === "hello" ? { org: "fake", origin: "fake" } : { id: identity.id, token: "synthetic-review-token" } })}\n`);
        }
      });
    });
    const address = process.platform === "win32" ? `\\\\.\\pipe\\atomic-review-${process.pid}-${Date.now()}` : path.join(dir, "records.sock");
    await new Promise<void>(resolve => server.listen(address, resolve));
    const client = new RemoteRecords({ socket: address, credentialDir, identity });
    try {
      if (process.platform !== "win32") await expect(client.open()).rejects.toThrow("token confirmation unavailable");
      else { unavailable = false; await client.open(); unavailable = true; }
      const file = path.join(credentialDir, fs.readdirSync(credentialDir)[0]!);
      const saved = JSON.parse(fs.readFileSync(file, "utf8"));
      await expect(client.open()).rejects.toThrow("token confirmation unavailable");
      await expect(client.open()).rejects.toThrow("token confirmation unavailable");
      expect(registered).toBe(1); expect(whoami).toBe(2);
      unavailable = false; await client.open();
      expect(confirmations).toBeGreaterThan(0);
      expect(registered).toBe(1);
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(saved);
    } finally { await client.close(); await new Promise<void>(resolve => server.close(() => resolve())); }
  });

  it("S3 directive stop retains registry publication debt for explicit stop retry", async () => {
    const dir = root(), first = host(dir);
    const actor = await first.manager.create({ name: "directive-stop", instructions: "Stop", responseMode: "directive", events: ["agent_settled"] });
    const registry = path.join(dir, "actors", "actors.json"), files = descriptors(), sync = fs.fsyncSync.bind(fs);
    let fail = true, failures = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = files.get(fd) ?? "";
      if (fail && first.manager.status(actor.id).status === "stopped" && file.startsWith(registry + ".") && file.endsWith(".tmp")) {
        failures++; throw new Error("directive stop registry unavailable");
      }
      sync(fd);
    });
    await expect(first.manager.ask(actor.id, "STOP_DIRECTIVE")).resolves.toMatchObject({ action: "stop" });
    await until(() => failures > 0 && first.manager.inFlightCount() === 0);
    const savedStatus = () => JSON.parse(fs.readFileSync(registry, "utf8")).actors.find((row: any) => row.id === actor.id).status;
    expect(savedStatus()).not.toBe("stopped");
    await expect(first.manager.stop(actor.id)).rejects.toThrow("directive stop registry unavailable");
    fail = false;
    await expect(first.manager.stop(actor.id)).resolves.toMatchObject({ status: "stopped" });
    expect(savedStatus()).toBe("stopped");
    const replacement = host(dir);
    expect(replacement.manager.status(actor.id).status).toBe("stopped");
    expect(() => replacement.manager.tell(actor.id, "later")).toThrow("is stopped");
    expect(replacement.manager.dispatchHostEvent("agent_settled", {})).toBe(0);
  });
});
