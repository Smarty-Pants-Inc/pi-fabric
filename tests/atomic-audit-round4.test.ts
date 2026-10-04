import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { RemoteRecords } from "../src/records/client.js";

const roots: string[] = [], managers: ActorManager[] = [], engines: AgentManager[] = [];
const identity = { id: "session:round4", name: "main", kind: "main" as const };
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-round4-")); roots.push(dir); return dir; };
const wait = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const until = async (predicate: () => boolean) => { for (let n = 0; !predicate(); n++) { if (n > 500) throw new Error("round4 probe timed out"); await wait(20); } };
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(engines.splice(0).map(engine => engine.close()));
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const actorHost = (directory: string) => {
  const mesh = new MeshStore(path.join(directory, "mesh"), 65536, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directory, "runs"),
  }); engines.push(agents);
  const actors = new ActorManager("round4", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(directory, "actors"), persistent: true,
    meshCursorPath: path.join(directory, "actors", "mesh-cursor.json"), closeGraceMs: 50,
  }); managers.push(actors);
  return { mesh, agents, actors };
};
const descriptors = () => {
  const files = new Map<number, string>(), open = fs.openSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; });
  return files;
};

describe("#2479 R4 real-path durability regressions", () => {
  it.skipIf(process.platform === "win32")("F2 confirms a visible post-rename queue after SIGKILL before cursor acknowledgement or launch", async () => {
    const directory = root();
    const child = spawn("bun", [path.resolve("tests/fixtures/atomic-queue-crash.ts"), directory], { stdio: ["ignore", "pipe", "pipe"] });
    const exited = once(child, "exit");
    let output = "", errors = "";
    child.stdout.on("data", chunk => { output += chunk.toString(); });
    child.stderr.on("data", chunk => { errors += chunk.toString(); });
    try { await until(() => output.includes("\n") || child.exitCode !== null); }
    finally { child.kill("SIGKILL"); await exited; }
    expect(output, errors).toContain("\n");
    const produced = JSON.parse(output.trim()) as { actorId: string; prefix: number; failed: number; failures: number; renamed: boolean };
    expect(produced.renamed).toBe(true); expect(produced.failures).toBeGreaterThan(0);
    const actorDir = path.join(directory, "actors", produced.actorId);
    const queue = fs.readdirSync(actorDir).find(file => /^queue-.+\.json$/.test(file))!;
    expect(JSON.parse(fs.readFileSync(path.join(actorDir, queue), "utf8")).items[0].payload.sequence).toBe(produced.failed);
    const files = descriptors(), sync = fs.fsyncSync.bind(fs);
    let failures = 0, cursorSyncs = 0;
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = files.get(fd)!;
      if (file === actorDir) { failures++; throw new Error("queue confirmation still unavailable"); }
      if (file.includes("mesh-cursor.json.") && file.endsWith(".tmp")) cursorSyncs++;
      sync(fd);
    });
    const restarted = actorHost(directory), launched = vi.spyOn(restarted.agents, "run");
    await until(() => failures > 0);
    await wait(300);
    await restarted.actors.close();
    const cursor = JSON.parse(fs.readFileSync(path.join(directory, "actors", "mesh-cursor.json"), "utf8"));
    expect(cursorSyncs, JSON.stringify(cursor)).toBeGreaterThan(0);
    expect(cursor.last.sequence).toBe(produced.prefix);
    expect(cursor.last.sequence).toBeLessThan(produced.failed);
    expect(launched).not.toHaveBeenCalled();
    synced.mockRestore(); vi.restoreAllMocks();
    const recovered = actorHost(directory), runs = vi.spyOn(recovered.agents, "run");
    await until(() => recovered.actors.messages(produced.actorId).some(message => message.direction === "out" && !message.error));
    await wait(100);
    expect(runs).toHaveBeenCalledTimes(1);
  }, 30000);

  it.skipIf(process.platform === "win32")("F9 reconfirms a retained nonce on every register retry after a post-rename failure", async () => {
    const directory = root(), credentialDir = path.join(directory, "credentials"); fs.mkdirSync(credentialDir);
    const files = descriptors(), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
    let renamed = false, fail = true, confirmed = false, registered = 0;
    const nonces: string[] = [], confirmations: boolean[] = [];
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { rename(from, to); if (String(to).startsWith(credentialDir)) renamed = true; });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (renamed && files.get(fd) === credentialDir) {
        if (fail) throw new Error("nonce namespace unavailable");
        confirmed = true;
      }
      sync(fd);
    });
    const server = net.createServer(socket => {
      let input = "";
      socket.on("data", chunk => {
        input += chunk.toString(); let boundary: number;
        while ((boundary = input.indexOf("\n")) >= 0) {
          const request = JSON.parse(input.slice(0, boundary)); input = input.slice(boundary + 1);
          if (request.method === "register") { registered++; nonces.push(request.args.nonce); confirmations.push(confirmed); }
          socket.write(`${JSON.stringify({ id: request.id, ok: true, result: request.method === "hello" ? { org: "fake", origin: "fake" } : { id: identity.id, token: "synthetic-test-only" } })}\n`);
        }
      });
    });
    const address = path.join(directory, "records.sock");
    await new Promise<void>(resolve => server.listen(address, resolve));
    const client = new RemoteRecords({ socket: address, credentialDir, identity });
    try {
      await expect(client.open()).rejects.toThrow("nonce namespace unavailable");
      const file = path.join(credentialDir, fs.readdirSync(credentialDir)[0]!);
      const nonce = JSON.parse(fs.readFileSync(file, "utf8")).nonce;
      await expect(client.open()).rejects.toThrow("nonce namespace unavailable");
      expect(registered).toBe(0);
      fail = false; confirmed = false;
      await client.open();
      expect(nonces).toEqual([nonce]);
      expect(confirmations).toEqual([true]);
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ nonce, token: "synthetic-test-only" });
    } finally { await client.close(); await new Promise<void>(resolve => server.close(() => resolve())); }
  });

  it.each([false, true])("F10 cleans up a prelaunch registry barrier failure and recovers in process (ask: %s)", async ask => {
    const directory = root(), { actors, agents } = actorHost(directory);
    const actor = await actors.create({ name: "prelaunch", instructions: "Work", responseMode: "text", coalesce: false });
    const files = descriptors(), sync = fs.fsyncSync.bind(fs);
    let fail = true, failures = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fail && files.get(fd)?.startsWith(path.join(directory, "actors", "actors.json."))) {
        failures++; throw new Error("registry-only barrier unavailable");
      }
      sync(fd);
    });
    const launched = vi.spyOn(agents, "run");
    const request = ask ? actors.ask(actor.id, "accepted first").then(() => "resolved", error => error.message) : undefined;
    if (!ask) actors.tell(actor.id, "accepted first");
    const actorDir = path.join(directory, "actors", actor.id);
    if (!ask) expect(fs.readdirSync(actorDir).some(file => /^queue-.+\.json$/.test(file))).toBe(true);
    await until(() => failures > 0);
    // A status string changes before the drain settles; assert actual admission cleanup.
    await until(() => actors.inFlightCount() === 0);
    expect(launched).not.toHaveBeenCalled();
    if (request) expect(await request).toContain("registry-only barrier unavailable");
    if (!ask) {
      const queue = fs.readdirSync(actorDir).find(file => /^queue-.+\.json$/.test(file))!;
      expect(JSON.parse(fs.readFileSync(path.join(actorDir, queue), "utf8")).items).toEqual([
        expect.objectContaining({ payload: { message: "accepted first" } }),
      ]);
    }
    fail = false;
    if (!ask) await until(() => actors.messages(actor.id).some(message => message.direction === "out" && !message.error));
    await actors.ask(actor.id, "subsequent activation");
    if (!ask) await until(() => actors.messages(actor.id).filter(message => message.direction === "out" && !message.error).length === 2);
    expect(launched).toHaveBeenCalledTimes(ask ? 1 : 2);
    await until(() => actors.inFlightCount() === 0);
  }, 20000);

});
