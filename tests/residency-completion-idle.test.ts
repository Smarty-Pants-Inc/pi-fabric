import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CompletionJournal, consumeCompletion } from "../src/agents/completion-journal.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentRunResult } from "../src/agents/types.js";
import type { MeshStateEntry, MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentDeliveryPrefix, residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
const roots: string[] = [], clients: ResidencyClient[] = [];
afterEach(async () => { await Promise.all(clients.splice(0).map(client => client.close())); vi.restoreAllMocks(); vi.useRealTimers(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
function setup() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-completion-idle-")); roots.push(root);
  const meshRoot = path.join(root, "mesh"), rootId = "session:observer", foreignId = "session:producer";
  const config: ResidentHostConfig = { format: 1, rootId, sessionId: "observer", cwd: root, projectRoot: root, mainName: "observer", mainStartedAt: 1, meshRoot,
    actorRoot: path.join(meshRoot, "actors"), residencyRoot: residentRoot(meshRoot, rootId), fullCodeMode: true,
    agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused" };
  const foreign = { ...config, rootId: foreignId, sessionId: "producer", mainName: "producer", residencyRoot: residentRoot(meshRoot, foreignId) };
  const result: AgentRunResult = { id: "a".repeat(32), name: "task", task: "work", status: "completed", runner: "pi", transport: "process", cwd: root, text: "x".repeat(4000), startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  const write = (file: string, value: unknown) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, typeof value === "string" ? value : JSON.stringify(value)); };
  const configFile = path.join(foreign.residencyRoot, "config.json"), resultFile = path.join(foreign.residencyRoot, "results", result.id + ".json");
  write(configFile, foreign); write(resultFile, result);
  const entry: MeshStateEntry = { key: residentDeliveryPrefix(foreignId) + "fixture", version: 1, updatedAt: 1, updatedBy: { id: residentHostId(foreignId), name: "resident", kind: "main" },
    value: { format: 1, rootId: foreignId, from: { id: result.id, name: result.name, kind: "agent" }, agentCompletionId: result.id } };
  const entries = new Map([[entry.key, entry]]);
  const deletion = vi.fn(async (input: { key: string; ifVersion: number }) => { if (entries.get(input.key)?.version === input.ifVersion) entries.delete(input.key); });
  const mesh = { listAll: (prefix: string) => [...entries.values()].filter(e => e.key.startsWith(prefix)), delete: deletion } as unknown as MeshStore;
  const mainAgent = { local: true, id: rootId, deliverAgent: vi.fn() } as unknown as FabricMainAgentTarget;
  const participants = { list: () => [], lastKnown: () => undefined } as unknown as FabricParticipantSource;
  let passEnd: (() => void) | undefined;
  vi.spyOn(CompletionJournal.prototype, "drain").mockImplementation(async () => { const resolve = passEnd; passEnd = undefined; resolve?.(); });
  const nextPass = () => new Promise<void>(resolve => { passEnd = resolve; });
  const client = new ResidencyClient({ config, mesh, mainAgent, participants }); clients.push(client);
  const start = async () => { const done = nextPass(); client.start(); await done; await Promise.resolve(); };
  const tick = async (ms: number) => { const done = nextPass(); await vi.advanceTimersByTimeAsync(ms); await done; await Promise.resolve(); };
  const hash = createHash("sha256").update(result.id).digest("hex");
  const envelope = path.join(meshRoot, "agent-completions", hash + ".json");
  const legacy = path.join(foreign.residencyRoot, "agents", result.id + ".json");
  return { root, meshRoot, foreign, result, entry, write, configFile, resultFile, envelope, legacy, start, tick, deletion, entries };
}
describe("foreign legacy completion idle import (#4233)", () => {
  it("does not reparse producer metadata or perform sync barriers on unchanged idle passes", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    const f = setup(), sync = vi.spyOn(fs, "fsyncSync"); await f.start();
    expect(fs.existsSync(f.envelope)).toBe(true); expect(sync).not.toHaveBeenCalled();
    const read = vi.spyOn(fs.promises, "readFile"), stat = vi.spyOn(fs.promises, "stat");
    await f.tick(1000); await f.tick(1000);
    expect(read).not.toHaveBeenCalled(); expect(stat).not.toHaveBeenCalled(); expect(f.deletion).not.toHaveBeenCalled();
    for (let n = 0; n < 3; n++) await f.tick(1000);
    expect(read.mock.calls.some(([file]) => [f.configFile, f.resultFile, f.envelope].includes(String(file)))).toBe(false);
    expect(stat.mock.calls.some(([file]) => String(file) === f.envelope)).toBe(true); // bounded 5-second revalidation
    expect(sync).not.toHaveBeenCalled();
  });
  it("rechecks fresh global receipts before retiring a stable source after the bounded maintenance window", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    const f = setup(); await f.start(); consumeCompletion(f.meshRoot, f.result.id, f.foreign.sessionId);
    await f.tick(1000); expect(f.deletion).not.toHaveBeenCalled();
    for (let n = 0; n < 4; n++) await f.tick(1000);
    expect(f.deletion).toHaveBeenCalledWith({ key: f.entry.key, ifVersion: 1 });
  });
  it("new mesh versions bypass the no-action window and do not hide unknown replay fences", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    const f = setup(); await f.start(); f.write(f.legacy, "{"); f.entry.version++;
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await f.tick(1000); expect(warn).toHaveBeenCalledOnce(); expect(String(warn.mock.calls[0]![0])).toMatch(/legacy.*replay fence/i);
    expect(f.deletion).not.toHaveBeenCalled(); expect(fs.existsSync(f.envelope)).toBe(true);
  });
  it("does not import config bytes from a different opened inode during replace/restore", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    const f = setup(), open = fs.promises.open.bind(fs.promises); let raced = false;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      if (String(args[0]) !== f.configFile || raced) return open(...args);
      raced = true; fs.renameSync(f.configFile, f.configFile + ".saved"); f.write(f.configFile, { ...f.foreign, mainName: "wrong-inode" });
      const handle = await open(...args); fs.rmSync(f.configFile); fs.renameSync(f.configFile + ".saved", f.configFile); return handle;
    });
    await f.start(); expect(fs.existsSync(f.envelope)).toBe(false);
    await f.tick(1000); expect(JSON.parse(fs.readFileSync(f.envelope, "utf8")).recipient.name).toBe("producer");
  });
  it("invalidates config identity when a replaced envelope requires a new import", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    const f = setup(); await f.start();
    fs.rmSync(f.envelope); f.write(f.configFile, { ...f.foreign, rootId: "session:invalid" }); f.entry.version++;
    await f.tick(1000); expect(fs.existsSync(f.envelope)).toBe(false); expect(f.deletion).not.toHaveBeenCalled();
    f.write(f.configFile, f.foreign); await f.tick(1000); expect(fs.existsSync(f.envelope)).toBe(true);
  });
});
