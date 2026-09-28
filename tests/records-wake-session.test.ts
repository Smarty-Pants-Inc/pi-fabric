import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, type AgentSession } from "@earendil-works/pi-coding-agent";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { RemoteRecords } from "../src/records/client.js";
import { migrate } from "../src/records/schema.js";
import { issuePrincipal, normalizeServiceConfig, RecordsServer } from "../src/records/server.js";
import type { ClientPool } from "../src/records/store.js";
import { postgresBin, startPostgres, type TestPostgres } from "./helpers/postgres.js";

/**
 * F21 in a real Pi session with the built Fabric: the records watchdog never starts a turn a user
 * cancelled; the record waits and arrives once with the next turn. With the host's preflight
 * capability, an idle, completed Main is still woken through the same gate as the mesh inbox (#107).
 */
const fabricEntry = path.resolve("dist/index.js");
const HOST_CAPABILITIES_KEY = Symbol.for("pi-fabric.test.hostCapabilities");
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR", "PI_FABRIC_INBOX_WAKE_MS", "PI_FABRIC_INBOX_WAKE_COOLDOWN_MS"] as const;
const REF = "Smarty-Pants-Inc/smarty-dev#754";
const canRun = fs.existsSync(fabricEntry) && Boolean(postgresBin) && process.platform !== "win32";

describe.skipIf(!canRun)("records wake a Main only through the idle gate (F21)", () => {
  let server: TestPostgres;
  let service: RecordsServer;
  let socket: string;
  let relayCredential: string;
  const sessions: AgentSession[] = [];
  const roots: string[] = [];
  const saved = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));

  beforeAll(async () => {
    server = await startPostgres();
    socket = path.join(server.dir, "records.sock");
    const pool = new pg.Pool({ ...server.connection, max: 6 });
    pool.on("error", () => undefined);
    const client = await pool.connect();
    try { await migrate(client); } finally { client.release(); }
    const config = normalizeServiceConfig({ org: "o", origin: "n", socket, database: server.connection, roles: { relay: ["relay:fabric"] } });
    service = await RecordsServer.open(config, { pool: pool as unknown as ClientPool });
    await service.listen();
    relayCredential = path.join(server.dir, "relay.json");
    fs.writeFileSync(relayCredential, JSON.stringify(await issuePrincipal(config, "relay:fabric", "relay", "relay", pool as unknown as ClientPool)), { mode: 0o600 });
  }, 60_000);
  afterAll(async () => { await service?.close(); await server?.stop(); }, 30_000);
  afterEach(async () => {
    for (const session of sessions.splice(0)) session.dispose();
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
    delete (globalThis as Record<symbol, unknown>)[HOST_CAPABILITIES_KEY];
  });

  const start = async (tokensPerSecond: number, capability: boolean) => {
    process.env.PI_FABRIC_INBOX_WAKE_MS = "100";
    process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS = "0";
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-records-wake-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    const capabilities = globalThis as Record<symbol, unknown>;
    if (capability) capabilities[HOST_CAPABILITIES_KEY] = { triggeredMessageQueuesBehindPreflight: true, promptPendingVisible: true };
    else delete capabilities[HOST_CAPABILITIES_KEY];
    // A fast watchdog (1 s) with a 1 s lag bound: it would wake a lagging root within seconds.
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      records: { enabled: true, socket, relayCredentialFile: relayCredential, watchdogMs: 1_000, consumerLagSeconds: 1 },
    }));
    process.env.PI_FABRIC_MESH_ROOT = path.join(root, "mesh");
    process.env.PI_CODING_AGENT_DIR = agentDir;
    const faux = fauxProvider({ tokensPerSecond });
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const loader = new DefaultResourceLoader({ cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true, additionalExtensionPaths: [fabricEntry] });
    await loader.reload();
    const { session } = await createAgentSession({ cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader, sessionManager: SessionManager.inMemory(root) });
    sessions.push(session);
    await session.bindExtensions({});
    const recordMessages = () => session.messages.filter((message) => message.role === "custom" && (message as { customType?: string }).customType === "pi-fabric-records");
    // The first turn activates Fabric; this root becomes a records consumer at the present.
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("ready")]);
    await session.prompt("start");
    const me = `session:${session.sessionManager.getSessionId()}`;
    // A peer, as another participant, asks this Main something.
    const peer = new RemoteRecords({ socket, identity: { id: `session:${randomUUID()}` }, credentialDir: path.join(root, "peer-cred") });
    await peer.open();
    const ask = async (key: string, text: string) => { await peer.append({ id: "x" }, { ref: REF, kind: "ask", key, text, data: { to: me } }); };
    return { session, faux, recordMessages, ask, peer };
  };

  it("starts no turn after a cancel through watchdog ticks, and delivers the record once with the next turn", async () => {
    const { session, faux, recordMessages, ask, peer } = await start(10, true);
    faux.setResponses([async () => { await ask("while-stopped", "Which host?"); return fauxAssistantMessage("a long answer ".repeat(300)); }]);
    const prompted = session.prompt("work");
    const deadline = Date.now() + 10_000;
    while (!session.isStreaming && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 20));
    await new Promise((resolve) => setTimeout(resolve, 200));
    await session.abort();
    await prompted.catch(() => undefined);
    // Several watchdog ticks (1 s each) past the lag bound: no turn starts, nothing is delivered.
    await new Promise((resolve) => setTimeout(resolve, 4_000));
    expect(session.isStreaming).toBe(false);
    expect(recordMessages()).toEqual([]);
    faux.setResponses([fauxAssistantMessage("resumed")]);
    await session.prompt("resume");
    expect(recordMessages()).toHaveLength(1);
    expect(JSON.stringify(recordMessages()[0])).toContain("Which host?");
    faux.setResponses([fauxAssistantMessage("again")]);
    await session.prompt("again");
    expect(recordMessages()).toHaveLength(1);
    peer.close();
  }, 60_000);

  it("without the host's preflight capability, never starts a turn for records", async () => {
    const { session, faux, recordMessages, ask, peer } = await start(1_000, false);
    await ask("idle", "Are you there?");
    await new Promise((resolve) => setTimeout(resolve, 4_000));
    expect(session.isStreaming).toBe(false);
    expect(recordMessages()).toEqual([]);
    faux.setResponses([fauxAssistantMessage("here")]);
    await session.prompt("next");
    expect(recordMessages()).toHaveLength(1);
    peer.close();
  }, 60_000);

  it("counterexample: an idle Main whose run completed is woken for a record, once", async () => {
    const { session, faux, recordMessages, ask, peer } = await start(1_000, true);
    faux.setResponses([fauxAssistantMessage("got it")]);
    await ask("idle", "Please review.");
    const deadline = Date.now() + 15_000;
    while (recordMessages().length === 0 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 100));
    expect(recordMessages()).toHaveLength(1);
    while (session.isStreaming && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 50));
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    expect(recordMessages()).toHaveLength(1);
    peer.close();
  }, 60_000);
});
