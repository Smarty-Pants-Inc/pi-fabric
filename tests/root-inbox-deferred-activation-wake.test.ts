import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { randomUUID } from "node:crypto";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, type AgentSession } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ROOT_INBOX_WAKE_TOPIC, RootInbox } from "../src/topology/root-inbox.js";
import { readHostLeases } from "../src/topology/host-leases.js";

// pi-fabric#792 x main's #676 custody-init retry: an idle Main wakes only on events.jsonl
// changes or a known deadline, so a deferred root-inbox start-up must request one wake itself.

// A genuinely foreign, live owner of the real mesh .lock; no shortened timeout or mocked error.
const holderSource = `
const fs = require("node:fs"), path = require("node:path"), { randomUUID } = require("node:crypto");
const lock = path.join(process.argv[1], process.argv[2]), token = randomUUID();
fs.mkdirSync(lock, { recursive: true, mode: 0o700 });
fs.writeFileSync(path.join(lock, "owner"), token + "\\n" + process.pid + "\\n" + Date.now() + "\\n", { mode: 0o600 });
process.stdin.once("data", () => {
  const released = lock + ".released." + token;
  fs.renameSync(lock, released);
  fs.rmSync(released, { recursive: true });
  process.stdin.destroy();
});
process.stdout.write("ready\\n");
`;

const capabilities = Symbol.for("pi-fabric.test.hostCapabilities");
const peer: MeshIdentity = { id: "session:peer", name: "Peer", kind: "main", sessionId: "peer" };
const roots: string[] = [];
const sessions: AgentSession[] = [];
afterEach(async () => {
  for (const session of sessions.splice(0)) {
    await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
    session.dispose();
  }
  delete (globalThis as Record<symbol, unknown>)[capabilities];
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true });
});

it("drains work queued during a lock-deferred root-inbox start-up once the heartbeat retry opens it, with no polling timer left", async () => {
  // #792 timer census: root-inbox-owned timers only (known deadlines, watcher retries, degraded drain).
  const nativeTimeout = globalThis.setTimeout, nativeClear = globalThis.clearTimeout;
  const nativeInterval = globalThis.setInterval, nativeClearInterval = globalThis.clearInterval;
  const rootOwned = () => /topology[\\/]root-inbox\.[jt]s/.test(new Error().stack ?? "");
  const rootTimers = new Set<unknown>();
  vi.spyOn(globalThis, "setTimeout").mockImplementation(((handler: (...args: unknown[]) => void, ms?: number, ...args: unknown[]) => {
    const owned = rootOwned();
    const timer = nativeTimeout(() => { rootTimers.delete(timer); handler(...args); }, ms);
    if (owned) rootTimers.add(timer);
    return timer;
  }) as typeof setTimeout);
  vi.spyOn(globalThis, "clearTimeout").mockImplementation(timer => { rootTimers.delete(timer); nativeClear(timer); });
  vi.spyOn(globalThis, "setInterval").mockImplementation(((handler: (...args: unknown[]) => void, ms?: number, ...args: unknown[]) => {
    const owned = rootOwned();
    const timer = nativeInterval(handler, ms, ...args);
    if (owned) rootTimers.add(timer);
    return timer;
  }) as typeof setInterval);
  vi.spyOn(globalThis, "clearInterval").mockImplementation(timer => { rootTimers.delete(timer); nativeClearInterval(timer); });
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "root-deferred-wake-"));
  roots.push(cwd);
  const agentDir = path.join(cwd, "agent"), meshRoot = path.join(cwd, "mesh");
  fs.mkdirSync(agentDir);
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  vi.stubEnv("PI_FABRIC_INBOX_WAKE_COOLDOWN_MS", "0");
  (globalThis as Record<symbol, unknown>)[capabilities] = { triggeredMessageQueuesBehindPreflight: true, promptPendingVisible: true };
  const faux = fauxProvider({ tokensPerSecond: 1_000 });
  const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(cwd, "auth.json") });
  modelRuntime.registerNativeProvider(faux.provider);
  const resourceLoader = new DefaultResourceLoader({ cwd, agentDir, noSkills: true, noPromptTemplates: true,
    noThemes: true, noContextFiles: true, additionalExtensionPaths: [path.resolve("src/index.ts")] });
  await resourceLoader.reload();
  const { session } = await createAgentSession({ cwd, agentDir, modelRuntime, model: faux.getModel(), resourceLoader,
    sessionManager: SessionManager.inMemory(cwd) });
  sessions.push(session);
  await session.bindExtensions({});
  const sessionId = session.sessionManager.getSessionId();
  const identity: MeshIdentity = { id: `session:${sessionId}`, name: "Main", kind: "main", sessionId };
  const inbox = () => session.messages.filter(message => message.role === "custom" &&
    (message as { customType?: string }).customType === "pi-fabric-inbox");
  const log = path.join(meshRoot, "events.jsonl");
  const logEvents = () => (fs.existsSync(log) ? fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean) : [])
    .map(line => JSON.parse(line) as { sequence: number; topic: string; from: { id: string } });

  // A resumed Main's saved cursor: work queued while start-up is deferred is its inbox, not history.
  const mesh = new MeshStore(meshRoot, 64 * 1024, 500);
  const cursorInbox = new RootInbox(mesh, identity, () => [identity.id]);
  await mesh.put({ key: cursorInbox.key, identity, value: { after: mesh.latestSequence() } });
  await cursorInbox.close();

  const holder = spawn(process.execPath, ["-e", holderSource, meshRoot, ".lock"], { stdio: ["pipe", "pipe", "pipe"] });
  const exited = once(holder, "exit");
  void exited.catch(() => undefined);
  let released = false;
  try {
    await once(holder.stdout, "data");
    // First start-up attempt: the mesh lock times out, so activation is deferred to the heartbeat.
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("ready")]);
    await session.prompt("activate");
    expect(warn.mock.calls.some(([line]) => String(line).includes("Initial mesh publish failed"))).toBe(true);
    expect(readHostLeases(meshRoot).has(identity.id)).toBe(false);
    expect(session.isStreaming).toBe(false);

    let turns = 0;
    session.subscribe(event => { if (event.type === "agent_start") turns++; });
    faux.setResponses([fauxAssistantMessage("deferred inbox drained")]);
    // Queue the work for the idle Main. Its append is the last event: the watch wake it
    // causes reads nothing, because inbox reads stay fenced until custody admission.
    const sequence = (logEvents().at(-1)?.sequence ?? 0) + 1;
    fs.appendFileSync(log, JSON.stringify({ id: randomUUID(), sequence, topic: "fleet.work.test.1", kind: "ack", from: peer,
      to: identity.id, text: "queued while root-inbox start-up was deferred", createdAt: Date.now() - 120_000 }) + "\n");
    fs.writeFileSync(path.join(meshRoot, "sequence"), String(sequence));
    await new Promise(resolve => nativeTimeout(resolve, 500));
    expect(inbox()).toHaveLength(0);
    expect(turns).toBe(0);

    holder.stdin.end("release\n");
    await exited;
    released = true;
    // No refresh, publish, prompt or tick from the test: the heartbeat retry finishes start-up...
    await vi.waitFor(() => expect(readHostLeases(meshRoot).has(identity.id)).toBe(true), { timeout: 20_000, interval: 50 });
    // ...and its one wake drains the queued work.
    await vi.waitFor(() => { expect(inbox()).toHaveLength(1); expect(session.isStreaming).toBe(false); }, { timeout: 10_000, interval: 50 });
    expect(JSON.stringify(inbox()[0])).toContain("queued while root-inbox start-up was deferred");
    // Nothing but the Main's own wake notice followed the queued event: no other event woke it.
    expect(logEvents().filter(event => event.sequence > sequence)
      .every(event => event.topic === ROOT_INBOX_WAKE_TOPIC && event.from.id === identity.id)).toBe(true);

    // #792 negative check: the deferred start-up armed no polling timer, and idle stays quiet.
    expect(rootTimers.size).toBe(0);
    await new Promise(resolve => nativeTimeout(resolve, 1_500));
    expect(inbox()).toHaveLength(1);
    expect(turns).toBe(1);
    expect(rootTimers.size).toBe(0);
    console.info(`[P0 evidence] deferred root-inbox start-up: inbox deliveries=${inbox().length}; triggered turns=${turns}; root-inbox timers=${rootTimers.size}`);
  } finally {
    if (!released) {
      holder.stdin.end("release\n");
      const kill = nativeTimeout(() => holder.kill(), 2_000);
      try { await exited; } finally { nativeClear(kill); }
    }
  }
}, 120_000);
