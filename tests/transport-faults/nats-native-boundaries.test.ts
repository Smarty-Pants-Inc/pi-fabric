// expected-skip: not explicitly authorized hosted execution: actual roots may physically KV.delete during terminal withdrawal/release.
// HOSTED AUTHOR-ONLY: GITHUB_ACTIONS=true AND FABRIC_NATIVE_PRODUCT_DELETE_HOSTED=1 are both required.
// Missing nats-server is optional only there; NATS_AUTH_REQUIRED=1 hard-fails. No local product-delete path is forwarded.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, type TestContext } from "vitest";
import { createPathBHarness, openJetStream, resolveNatsBinary, type PathBHarness } from "../helpers/nats-cluster.js";
import { launchLog, same, stopAllOwned, type LaunchLog, type Owned } from "../helpers/owned-processes.js";
import { ensureTransportStream } from "../../src/transport/nats/provision.js";
import { openNatsKv, type NatsKvStore } from "../../src/transport/nats/kv.js";
import { transportDurableName, transportInboxFilter, transportInboxStream } from "../../src/transport/nats/subjects.js";
import { MeshStore } from "../../src/mesh/store.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { runTreeResourceVeto } from "../../src/storage/retention.js";
import type { CompletionEnvelope } from "../../src/agents/completion-journal.js";
import { residentHostId } from "../../src/residency/protocol.js";

/** E4 B5 composition check; keep: native custody trust boundary + data safety.
 * Main uses the actual source FabricRuntimeState.initialize with a real native Pi
 * context and host bootstrap. Durable spawn uses its production registry/provider,
 * client, compiled launcher, ResidentHost.start, manager and worker. Only inference
 * is fake (held loopback SSE). This is not a two-host install/canary or a power-cut
 * test. E4 completions stay in one FILE mesh root; NATS is only the control carrier.
 * Existing native-boundaries.test.ts FILE B4/B5 assertions are not replaced here.
 */
const REPO = fileURLToPath(new URL("../../", import.meta.url));
const FIXTURE = fileURLToPath(new URL("./native-fixtures/nats-completion-parent.ts", import.meta.url));
const DEADLINE = 45_000;
const HOSTED_PRODUCT_DELETE = process.env.GITHUB_ACTIONS === "true" && process.env.FABRIC_NATIVE_PRODUCT_DELETE_HOSTED === "1";
const digest = (id: string) => createHash("sha256").update(id).digest("hex");
const token = (id: string) => digest(id).slice(0, 32);
const json = <T = Record<string, any>>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const rows = (file: string): Array<Record<string, any>> => {
  try { return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
const bounded = async <T>(promise: Promise<T>, label: string, ms = DEADLINE): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`NATS native boundary deadline: ${label}`)), ms);
    })]);
  } finally { clearTimeout(timer); }
};
// fs.watch is the normal path; the single deadline bounds missing receipts.
const observe = <T>(directory: string, read: () => T | undefined, label: string): Promise<T> => new Promise((resolve, reject) => {
  let watcher: fs.FSWatcher | undefined; let done = false;
  const finish = (error?: unknown, value?: T) => {
    if (done) return; done = true; clearTimeout(timer); watcher?.close();
    if (error) reject(error); else resolve(value!);
  };
  const timer = setTimeout(() => {
    // One final safety-net read also catches /proc exit without another file
    // event; no standing process/PID polling loop is introduced.
    try { const value = read(); if (value !== undefined) { finish(undefined, value); return; } } catch { /* deadline below */ }
    finish(new Error(`NATS native boundary deadline: ${label}`));
  }, DEADLINE);
  const check = () => {
    try { const value = read(); if (value !== undefined) finish(undefined, value); }
    catch (error) {
      if (!(error instanceof SyntaxError) && (error as NodeJS.ErrnoException).code !== "ENOENT") finish(error);
    }
  };
  try { watcher = fs.watch(directory, check); watcher.on("error", finish); check(); } catch (error) { finish(error); }
});
const observeJson = (file: string, accepts: (value: Record<string, any>) => boolean = () => true) =>
  observe(path.dirname(file), () => { const value = json(file); return accepts(value) ? value : undefined; }, file);
const roots: string[] = []; const logs: LaunchLog[] = []; const natives: NativeRpc[] = [];
const brokers: PathBHarness[] = []; const servers: http.Server[] = []; const meshes: MeshStore[] = [];

class NativeRpc {
  readonly child: ChildProcess;
  readonly exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  readonly events: Array<Record<string, any>> = [];
  readonly pending = new Map<string, { resolve(value: any): void; reject(error: Error): void }>();
  readonly listeners = new Set<() => void>();
  stderr = ""; closed = false;
  constructor(root: string, env: NodeJS.ProcessEnv, cli: string, sessionFile?: string) {
    this.child = spawn(process.execPath, [cli, "--mode", "rpc", "--offline", "--no-extensions", "--no-skills", "--no-context-files",
      "--no-prompt-templates", "--no-themes", "--approve", "-e", FIXTURE, "--provider", "boundary-offline", "--model", "offline",
      "--thinking", "off", "--tools", "", "--session-dir", path.join(root, "sessions"), ...(sessionFile ? ["--session", sessionFile] : [])],
    { cwd: root, env, stdio: ["pipe", "pipe", "pipe"], timeout: 180_000, killSignal: "SIGKILL" });
    this.exit = new Promise(resolve => this.child.once("close", (code, signal) => {
      this.closed = true;
      for (const pending of this.pending.values()) pending.reject(new Error(`Native Pi closed ${code}/${signal}: ${this.stderr}`));
      this.pending.clear(); for (const notify of this.listeners) notify(); resolve({ code, signal });
    }));
    this.child.on("error", error => {
      this.stderr += `\n${error.message}`;
      for (const pending of this.pending.values()) pending.reject(error);
    });
    let tail = "";
    this.child.stdout!.on("data", chunk => {
      tail += chunk.toString();
      for (let end = tail.indexOf("\n"); end !== -1; end = tail.indexOf("\n")) {
        const line = tail.slice(0, end); tail = tail.slice(end + 1); if (!line.trim()) continue;
        let event: Record<string, any>;
        try { event = JSON.parse(line); } catch { this.stderr += `\nNon-RPC stdout: ${line}`; continue; }
        this.events.push(event);
        if (event.type === "response") {
          const pending = this.pending.get(event.id); this.pending.delete(event.id);
          if (pending) event.success ? pending.resolve(event.data) : pending.reject(new Error(event.error));
        }
        for (const notify of this.listeners) notify();
      }
    });
    this.child.stderr!.on("data", chunk => { this.stderr += chunk.toString(); });
    natives.push(this);
  }
  async request(value: Record<string, unknown>): Promise<any> {
    const id = randomUUID();
    try {
      return await bounded(new Promise<any>((resolve, reject) => {
        if (this.closed) { reject(new Error(`Native Pi already closed: ${this.stderr}`)); return; }
        this.pending.set(id, { resolve, reject });
        this.child.stdin!.write(JSON.stringify({ ...value, id }) + "\n");
      }), `RPC ${value.type}`);
    } finally { this.pending.delete(id); }
  }
  async settled(): Promise<void> {
    let check!: () => void;
    try {
      await bounded(new Promise<void>((resolve, reject) => {
        check = () => {
          if (this.events.some(event => event.type === "agent_settled")) resolve();
          else if (this.closed) reject(new Error(`Native Pi closed before settlement: ${this.stderr}`));
        };
        this.listeners.add(check); check();
      }), "native baseline settlement");
    } finally { this.listeners.delete(check); }
  }
  async close(): Promise<void> {
    if (!HOSTED_PRODUCT_DELETE) throw new Error("Physical native graceful release is HOSTED AUTHOR-ONLY");
    if (this.closed) return;
    this.child.stdin!.end();
    const result = await bounded(this.exit, "native cooperative close/join", 15_000);
    if (result.code !== 0 || result.signal) throw new Error(`Native graceful close failed ${JSON.stringify(result)}: ${this.stderr}`);
  }
  async crash(log: LaunchLog): Promise<void> {
    if (this.closed) return;
    killRecorded(log, this.child.pid!);
    expect(await bounded(this.exit, "owned native SIGKILL close/join")).toEqual({ code: null, signal: "SIGKILL" });
  }
}
const identity = (log: LaunchLog, pid: number): Owned => {
  const recorded = log.owned().find(entry => entry.pid === pid);
  expect(recorded, `independent launch record for local PID ${pid}`).toBeDefined();
  expect(same(recorded!), "exact PID birth still live").toBe(true);
  return recorded!;
};
const killRecorded = (log: LaunchLog, pid: number) => {
  const recorded = identity(log, pid); process.kill(recorded.pid, "SIGKILL"); return recorded;
};
const assertLocal = (run: string, log: LaunchLog) => {
  const status = json(path.join(run, "status.json"));
  expect(status.transport).toBe("process"); expect(status.sessionId).toMatch(/^\d+$/);
  const pid = Number(status.sessionId); expect(Number.isSafeInteger(pid) && pid > 1).toBe(true);
  expect(fs.existsSync(path.join(run, "placement.json"))).toBe(false);
  return identity(log, pid);
};

const loopback = async (root: string) => {
  const requests: Array<Record<string, any>> = []; const held = new Set<number>();
  const releases: Array<() => void> = []; const arrivals = new Map<number, (value: Record<string, any>) => void>();
  const server = http.createServer((request, response) => {
    // Production agents.spawn verifies static OpenAI providers via GET /models.
    // Do not mistake catalog discovery for a held inference or parse an empty body.
    if (request.method === "GET" && request.url === "/v1/models") {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ data: [{ id: "offline", object: "model" }] })); return;
    }
    let text = ""; request.on("data", chunk => { text += chunk; }); request.on("end", () => {
      const payload = JSON.parse(text); const index = requests.length; requests.push(payload);
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      let released = false;
      const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
        id: "nats-e4-offline", object: "chat.completion.chunk", created: 1, model: "offline", choices: [{ index: 0, delta, finish_reason }],
      })}\n\n`);
      releases.push(() => {
        if (released || response.destroyed) return; released = true;
        chunk({ role: "assistant", content: "native NATS E4 final answer" }); chunk({}, "stop"); response.end("data: [DONE]\n\n");
      });
      arrivals.get(index)?.(payload); arrivals.delete(index); if (!held.has(index)) releases[index]!();
    });
  });
  servers.push(server); await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const agent = path.join(root, "agent"); fs.mkdirSync(agent, { mode: 0o700 });
  fs.writeFileSync(path.join(agent, "models.json"), JSON.stringify({ providers: { "boundary-offline": {
    baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only-not-a-credential", api: "openai-completions",
    models: [{ id: "offline", name: "Offline boundary", reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  } } }));
  fs.writeFileSync(path.join(agent, "settings.json"), JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false },
    retry: { enabled: false }, packages: [], extensions: [] }));
  return { agent, requests, holdNext: () => held.add(requests.length), release: (index: number) => {
    if (!releases[index]) throw new Error(`No held model request ${index}`); releases[index]!();
  }, requestAt: (index: number) => requests[index] ? Promise.resolve(requests[index]!)
    : bounded(new Promise<Record<string, any>>(resolve => arrivals.set(index, resolve)), "held native child model request") };
};

const broker = async (ctx: TestContext) => {
  const binary = resolveNatsBinary(); // Explicit bad NATS_BIN throws; never convert it to SKIP.
  if (!binary) {
    if (process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: nats-server executable missing");
    return ctx.skip("nats-server executable missing; native NATS E4 coverage not earned");
  }
  if (process.platform !== "linux" || !/^node(?:\.exe)?$/.test(path.basename(process.execPath))) {
    throw new Error("Native NATS E4 requires Linux Node for independent /proc PID birth and SIGKILL proof");
  }
  for (const file of ["dist/worker.js", "dist/index.js", "dist/native/manifest.json", "dist/residency/launcher.js", "dist/residency/pi-entry.js"]) {
    if (!fs.existsSync(path.join(REPO, file))) throw new Error(`Required native artifact missing: ${file}; parent must build`);
  }
  const h = await createPathBHarness({ binary }); brokers.push(h);
  // Separate bootstrap credential is used ONLY for provisioning, then closed.
  // Neither native runtime receives it. No operator body read or relay grant.
  const bootstrap = await h.connect(h.users.bootstrap);
  try {
    const { manager, js } = await openJetStream(bootstrap);
    const policy = (name: string, subjects: string[], maxAgeMs: number, maxMessageBytes = 1_048_576) => ({
      name, subjects, maxBytes: 1_073_741_824, maxAgeMs, maxMessageBytes, duplicateWindowMs: 600_000, replicas: 1,
    });
    await ensureTransportStream(manager, policy("FABRIC_ADV", ["$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"], 30 * 24 * 60 * 60_000, 65_536));
    await openNatsKv(js, { bucket: "fabric_leases", ttlMs: 15_000, markerTtlMs: 300_000 });
    await openNatsKv(js, { bucket: "fabric_presence", ttlMs: 600_000, markerTtlMs: 300_000 });
    for (const machine of ["machine-a", "machine-b"]) {
      await ensureTransportStream(manager, policy(transportInboxStream(machine), [`fabric.in.${machine}.>`], 0));
    }
  } finally { await bootstrap.close(); }
  const a = await h.connect(h.users.machineA); const b = await h.connect(h.users.machineB);
  const ajs = await openJetStream(a); const bjs = await openJetStream(b);
  const leases = await openNatsKv(ajs.js, { bucket: "fabric_leases" }, false);
  const presence = await openNatsKv(ajs.js, { bucket: "fabric_presence" }, false);
  // Limits retention keeps acknowledged messages: last_seq==0 proves no envelope
  // EVER crossed either inbox during this isolated composition case. This stronger
  // no-traffic observation avoids granting any operator/runtime inbox body reads.
  const noCompletionCarrier = async () => {
    for (const [machine, manager] of [["machine-a", ajs.manager], ["machine-b", bjs.manager]] as const) {
      const info = await manager.streams.info(transportInboxStream(machine));
      expect(info.config.retention).toBe("limits"); expect(info.config.max_age).toBe(0);
      expect(info.state.messages).toBe(0); expect(info.state.last_seq).toBe(0);
    }
  };
  return { h, leases, presence, manager: ajs.manager, noCompletionCarrier };
};
const waitLeaseGone = async (leases: NatsKvStore, machine: string, hostId: string) => {
  const key = `${machine}.h.${token(hostId)}`; const abort = new AbortController();
  // Broker TTL marker/watch is the normal event source; no TTL sleeps/polling.
  const timer = setTimeout(() => abort.abort(new Error("native predecessor lease did not expire")), DEADLINE);
  try {
    for await (const event of leases.watch(key, abort.signal)) {
      if (event.type === "gone" && event.key === key) return;
      if (event.type === "ready" && !await leases.get(key)) return;
    }
    throw new Error("lease watch ended before predecessor absence");
  } finally { clearTimeout(timer); abort.abort(); }
};
const seenFiles = (root: string): string[] => {
  if (!fs.existsSync(root)) return [];
  return fs.readdirSync(root, { withFileTypes: true }).flatMap(entry => entry.isDirectory()
    ? seenFiles(path.join(root, entry.name)) : entry.isFile() && entry.name.endsWith(".json") ? [path.join(root, entry.name)] : []);
};

afterEach(async () => {
  let failure: unknown;
  const processes = natives.splice(0);
  for (const rpc of processes) {
    if (rpc.closed) continue;
    try {
      const log = logs.find(candidate => candidate.owned().some(entry => entry.pid === rpc.child.pid));
      if (log) await rpc.crash(log);
      else if (rpc.child.pid !== undefined) throw new Error("Cannot signal unrecorded native parent");
    } catch (error) { failure ??= error; }
  }
  for (const log of logs.splice(0)) {
    try {
      // Kill launchers before hosts so no watchdog can restart our owned child.
      // Never SIGTERM a running runtime: that forwards HostLease.release/KV.delete.
      // Two bounded snapshots catch descendants whose launch raced the first kill.
      for (let pass = 0; pass < 2; pass++) {
        const owned = log.owned().filter(same).sort((a, b) => Number(b.argv.some(arg => arg.endsWith("launcher.js"))) - Number(a.argv.some(arg => arg.endsWith("launcher.js"))));
        for (const entry of owned) {
          if (!same(entry)) continue;
          try { process.kill(entry.pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
        }
        // Reuse the ownership helper only AFTER SIGKILL, for bounded death join.
        await stopAllOwned(owned, 0, 8_000);
      }
    } catch (error) { failure ??= error; }
  }
  for (const rpc of processes) {
    try { await bounded(rpc.exit, "captured native close before scratch cleanup", 8_000); } catch (error) { failure ??= error; }
  }
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    try { await bounded(new Promise<void>(resolve => server.close(() => resolve())), "loopback close", 5_000); } catch (error) { failure ??= error; }
  }
  // Always release the OWNED broker, including native/setup/assertion failures.
  for (const h of brokers.splice(0)) { try { await bounded(h.stop(), "owned broker stop", 10_000); } catch (error) { failure ??= error; } }
  for (const mesh of meshes.splice(0)) mesh.closeState();
  if (failure) { roots.splice(0); throw failure; } // Retain evidence if any child failed to join.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 90_000);

describe.skipIf(!HOSTED_PRODUCT_DELETE)("HOSTED AUTHOR-ONLY E4 native NATS B5 — actual roots, unchanged same-machine FILE journal", () => {
  for (const variant of ["durable-close", "durable-main-kill"] as const) {
    it(`${variant}: exact native-session restart consumes same-root durable child once, receipt durable BEFORE archive`, async ctx => {
      const transport = await broker(ctx); if (!transport) throw new Error("missing broker fixture");
      const { h, leases, presence, manager, noCompletionCarrier } = transport;
      const cli = path.join(REPO, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
      if (!fs.existsSync(cli)) throw new Error("Pinned native Pi CLI is missing; not an optional broker skip");
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nats-native-e4-")); roots.push(root);
      const model = await loopback(root); const log = launchLog(root); logs.push(log);
      const nats = { replicas: 1, servers: [h.serverUrl], machine: "machine-a", credentials: h.users.machineA.credsPath,
        operatorMachine: "fabric-ops", inboxLanes: 8 };
      fs.writeFileSync(path.join(root, "nats-settings.json"), JSON.stringify(nats), { mode: 0o600 });
      // Host stamp AND actual machine creds: neither setting only an env carrier
      // nor changing mesh.stateBackend constitutes NATS composition evidence.
      const env: NodeJS.ProcessEnv = { ...process.env, ...log.env, PI_CODING_AGENT_DIR: model.agent,
        PI_FABRIC_NODE_BINARY: process.execPath, PI_FABRIC_PI_BINARY: cli, PI_FABRIC_MESH_ROOT: path.join(root, "mesh"),
        PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_MESH_TRANSPORT: "nats", PI_FABRIC_MESH_HOST_TRANSPORT: "nats",
        PI_FABRIC_MESH_STATE_BACKEND: "file", PI_FABRIC_MESH_NATS: JSON.stringify(nats),
        PI_FABRIC_MACHINE: "machine-a", PI_FABRIC_NATS_SERVERS: h.serverUrl, PI_FABRIC_NATS_CREDENTIALS: h.users.machineA.credsPath,
        VITE_CACHE_DIR: path.join(REPO, ".local/cache/vite"), XDG_CACHE_HOME: path.join(REPO, ".local/cache/xdg"),
        npm_config_cache: path.join(REPO, ".local/cache/npm"), PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1", FABRIC_NATIVE_BOUNDARY_ROOT: root };
      const first = new NativeRpc(root, env, cli);
      const ready = await observeJson(path.join(root, "nats-completion-ready.json"), row => row.pid === first.child.pid);
      expect(ready).toMatchObject({ carrier: "nats", hostTransport: "nats", stateBackend: "file", observationHealth: [] });
      await first.request({ type: "prompt", message: "Persist this exact E4 native parent session." }); await first.settled();
      const state = await first.request({ type: "get_state" });
      expect(state.sessionId).toBe(ready.sessionId); expect(typeof state.sessionFile).toBe("string");
      const hostId = ready.main.id as string;
      const parentBirth = identity(log, first.child.pid!);
      const lease = await leases.get<Record<string, any>>(`machine-a.h.${token(hostId)}`);
      expect(lease?.value).toMatchObject({ id: hostId, pid: parentBirth.pid, pidStartTime: parentBirth.started });
      expect(await presence.get(`machine-a.p.${token(hostId)}`)).toBeDefined();
      const custodyDir = path.join(model.agent, "fabric");
      const parentCustodyFile = fs.readdirSync(custodyDir).filter(file => file.startsWith("transport-host-lease-") && file.endsWith(".json"))
        .map(file => path.join(custodyDir, file)).find(file => json(file).ownerHostId === hostId);
      expect(parentCustodyFile, "E8 successor proof is local to agentDir/fabric, not the shared mesh").toBeDefined();
      expect(json(parentCustodyFile!)).toMatchObject({ ownerHostId: hostId, pid: parentBirth.pid,
        processStartTime: parentBirth.started, releasedCleanly: false });
      const consumer = await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(hostId));
      expect(consumer.config.filter_subject).toBe(transportInboxFilter("machine-a", hostId));
      await noCompletionCarrier();
      model.holdNext();
      await first.request({ type: "prompt", message: "/native-nats-completion-spawn" });
      const admission = await observeJson(path.join(root, "nats-completion-spawned.json"));
      const id = admission.handle.id as string; const run = admission.run as string;
      await model.requestAt(1);
      const workerBirth = assertLocal(run, log);
      const manifest = json(path.join(run, "completion-recipient.json"));
      expect(manifest).toMatchObject({ meshRoot: path.join(root, "mesh"), recipient: { rootId: hostId, sessionId: state.sessionId } });
      const config = json(path.join(admission.residencyRoot, "config.json"));
      expect(config.mesh).toMatchObject({ transport: "nats", hostTransport: "nats", stateBackend: "file", nats });
      const owner = json(path.join(admission.residencyRoot, "owner.json")); const ownerBirth = identity(log, owner.pid);
      const residentId = residentHostId(hostId);
      expect((await leases.get<Record<string, any>>(`machine-a.h.${token(residentId)}`))?.value.pid).toBe(ownerBirth.pid);
      expect(manifest.supervisor.pid).toBe(ownerBirth.pid);
      const residentConsumer = await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(residentId));
      expect(residentConsumer.config.filter_subject).toBe(transportInboxFilter("machine-a", residentId));
      // Record custody BEFORE any kill, using each child's independent launch log.
      fs.writeFileSync(path.join(root, "independent-births.json"), JSON.stringify({ parentBirth, workerBirth, ownerBirth }));
      const directory = path.join(manifest.meshRoot, "agent-completions");
      fs.mkdirSync(directory, { recursive: true });
      const name = `${digest(id)}.json`; const envelopeFile = path.join(directory, name);
      const receiptFile = path.join(directory, "receipts", name); const archiveFile = path.join(directory, "archive", name);
      const claimKey = `residency/completion-claims/${digest(id)}`;
      expect(fs.existsSync(envelopeFile)).toBe(false); expect(fs.existsSync(receiptFile)).toBe(false);
      if (variant === "durable-close") await first.close();
      else {
        killRecorded(log, parentBirth.pid);
        expect(await bounded(first.exit, "exact native Main SIGKILL close/join")).toEqual({ code: null, signal: "SIGKILL" });
      }
      expect(same(parentBirth)).toBe(false);
      expect(json(parentCustodyFile!)).toMatchObject({ ownerHostId: hostId, pid: parentBirth.pid,
        processStartTime: parentBirth.started, releasedCleanly: variant === "durable-close" });
      expect(same(workerBirth)).toBe(true); expect(same(ownerBirth)).toBe(true);
      model.release(1);
      const envelope = await observeJson(envelopeFile) as unknown as CompletionEnvelope;
      expect(fs.readdirSync(directory).filter(file => /^[a-f0-9]{64}\.json$/.test(file))).toEqual([name]);
      expect(envelope.recipient).toEqual(manifest.recipient); expect(envelope.result.id).toBe(id);
      const settled = await observeJson(path.join(run, "manager-result.json")); const answer = json(path.join(run, "final-answer.json"));
      expect(envelope.result).toMatchObject({ status: settled.status, text: answer.text, finalAnswerReceipt: { id: answer.id } });
      expect(envelope.result.status).toBe("completed");
      expect(fs.existsSync(receiptFile)).toBe(false);
      expect(rows(path.join(root, "nats-completion-enqueued.jsonl"))).toEqual([]);
      await noCompletionCarrier();

      // Foreign native Main uses the same host credential and shared root, but
      // no PID/TTL/placement metadata transfers exact recipient ownership.
      const foreign = new NativeRpc(root, env, cli);
      const other = await observeJson(path.join(root, "nats-completion-ready.json"), row => row.pid === foreign.child.pid);
      expect(other.main.id).not.toBe(hostId); expect(other.sessionId).not.toBe(state.sessionId);
      await foreign.request({ type: "prompt", message: "/native-nats-completion-checkpoint" });
      expect(rows(path.join(root, "nats-completion-enqueued.jsonl"))).toEqual([]);
      expect(fs.existsSync(receiptFile)).toBe(false); expect(json(envelopeFile)).toEqual(envelope);
      await foreign.crash(log);
      // SIGKILL leaves E8's unclean receipt intact. Wait on the REAL KV expiry
      // event; never delete a lease or manufacture clean predecessor evidence.
      await waitLeaseGone(leases, "machine-a", hostId);
      const resumed = new NativeRpc(root, env, cli, state.sessionFile);
      const resumedReady = await observeJson(path.join(root, "nats-completion-ready.json"), row => row.pid === resumed.child.pid);
      expect(resumedReady.main.id).toBe(hostId); expect(resumedReady.sessionId).toBe(state.sessionId);
      const receipt = await observe<Record<string, any>>(directory, () => fs.existsSync(receiptFile) ? json(receiptFile) : undefined, "native inbox consumed receipt");
      expect(receipt).toMatchObject({ id, sessionId: state.sessionId }); expect(receipt.consumedAt).toBeGreaterThan(0);
      await observe(directory, () => fs.existsSync(archiveFile) && !fs.existsSync(envelopeFile) ? true : undefined, "archive after receipt");
      await resumed.request({ type: "prompt", message: "/native-nats-completion-checkpoint" });
      const mesh = new MeshStore(manifest.meshRoot, DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents); meshes.push(mesh);
      await observe(manifest.meshRoot, () => mesh.get(claimKey, { fresh: true }) === undefined ? true : undefined, "exact claim retirement");
      expect(fs.existsSync(path.join(directory, "attempts", name))).toBe(false);
      const enqueues = () => rows(path.join(root, "nats-completion-enqueued.jsonl")).filter(row => row.result.id === id);
      const deliveries = () => rows(path.join(root, "nats-completion-delivered.jsonl")).filter(row => row.id === id);
      expect(enqueues()).toHaveLength(1); expect(enqueues()[0]).toMatchObject({ pid: resumed.child.pid, sessionId: state.sessionId,
        result: { id, status: settled.status, finalAnswerReceipt: { id: answer.id } } });
      expect(deliveries()).toHaveLength(1);
      const archived = json<CompletionEnvelope>(archiveFile); expect(archived).toEqual(envelope);
      const order = rows(path.join(root, "nats-completion-order.jsonl")).filter(row => row.receipt.id === id);
      expect(order.length).toBeGreaterThan(0);
      expect(order[0]).toMatchObject({ pid: resumed.child.pid, event: "before-archive", receipt, source: envelope,
        claim: { key: claimKey, updatedBy: { id: hostId }, value: { rootId: hostId, sessionId: state.sessionId } } });
      expect(order[0]!.bodySyncSequence).toBeGreaterThan(0);
      expect(order[0]!.bodySyncSequence).toBeLessThan(order[0]!.receiptRenameSequence);
      expect(order[0]!.receiptRenameSequence).toBeLessThan(order[0]!.directorySyncSequence);
      expect(order[0]!.directorySyncSequence).toBeLessThan(order[0]!.archiveAttemptSequence);
      const carriers = () => rows(state.sessionFile).filter(row => row.type === "custom_message" && row.customType === "pi-fabric-agent-complete" && row.details?.ids?.includes(id));
      expect(carriers()).toHaveLength(1);
      // Read the existing read-only exit predicate; never invoke a product delete.
      await observe(run, () => runTreeResourceVeto(run, 0, undefined, true, true) === undefined ? true : undefined, "native durable worker persisted exit");
      await noCompletionCarrier();
      const localSeen = [path.join(model.agent, "fabric/control-seen"), path.join(manifest.meshRoot, "control-seen")];
      for (const file of localSeen.flatMap(seenFiles)) {
        const record = json(file);
        expect(JSON.stringify(record)).not.toMatch(/"operation"\s*:\s*"completion"|completion:/);
      }
      await resumed.crash(log); await waitLeaseGone(leases, "machine-a", hostId);
      const again = new NativeRpc(root, env, cli, state.sessionFile);
      const againReady = await observeJson(path.join(root, "nats-completion-ready.json"), row => row.pid === again.child.pid);
      expect(againReady.main.id).toBe(hostId); expect((await again.request({ type: "get_state" })).sessionId).toBe(state.sessionId);
      await again.request({ type: "prompt", message: "/native-nats-completion-checkpoint" });
      expect(enqueues()).toHaveLength(1); expect(deliveries()).toHaveLength(1); expect(carriers()).toHaveLength(1);
      expect(mesh.get(claimKey, { fresh: true })).toBeUndefined(); expect(json(receiptFile)).toEqual(receipt);
      await noCompletionCarrier(); await again.crash(log);
    }, 180_000);
  }
});
