import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { createHash, randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi, type TestContext } from "vitest";
import { AgentManager } from "../../src/agents/manager.js";
import { CompletionJournal, type CompletionEnvelope, type CompletionRecipient } from "../../src/agents/completion-journal.js";
import { ProcessTransport } from "../../src/agents/transports/process-transport.js";
import { runTreeResourceVeto, canRemoveTerminalRun } from "../../src/storage/retention.js";
import { followUpFile, followUpState } from "../../src/agents/follow-up-delivery.js";
import type { AgentRunResult } from "../../src/agents/types.js";
import { findExecutable } from "../../src/agents/transports/process-utils.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { fabricTurnProvenance } from "../../src/fabric-provenance.js";
import { MeshStore } from "../../src/mesh/store.js";
import type { FabricParticipantSource } from "../../src/topology/types.js";
import principalDelivery from "../../src/worker/principal-delivery.js";
import { readFinalAnswerReceipt } from "../../src/worker/terminal-answer.js";
import { readTerminalControl, terminalControlMessage } from "../../src/worker/terminal-controls.js";
import { launchLog, same, stopAllOwned, type LaunchLog } from "../helpers/owned-processes.js";

/** Contract v2 §3.1/§8.2. Keep reasons: data safety and a native custody trust
 * boundary. All custody/consumption logic below is production code.
 *
 * Scope of evidence:
 * - Main: REAL Node/Pi RPC process, actual SIGKILL after journal admission and
 *   before injection, exact-session restart, model context + durable consumed ID.
 *   The admission uses Main's production reload fence, NOT a fabricated broker
 *   ACK. This proves native B2 -> B3, not the A1 transport/control-seen boundary.
 * - Task: REAL compiled worker/native Pi and the original native terminal refusal.
 *   This is a queued-at-final boundary proof, NOT a task crash/restart proof.
 * - principal-delivery component check: recorded host callbacks, explicitly NOT
 *   native Pi crash/E2E proof. It checks the delivered half of the receipt gate.
 * - E4 B4: actual worker kill points and the existing read-only persisted-exit
 *   veto, independently of result publication; real transport stop-failure seam.
 * - E4 B5: same-root producer/admission manifest/journal/native Pi inbox.
 *   File-mode source classes are composed explicitly by the fixture. This is
 *   NOT the full fabric-runtime-state composition, a two-host canary, or NATS
 *   evidence. Actual-root NATS durable-close/Main-kill B5 cases now live in
 *   nats-native-boundaries.test.ts; remaining NATS variants stay explicit gaps.
 *   No B5 completion ENVELOPE crosses a machine. Remote placement can return
 *   result data to a local manager; these cases pin needs:["local"] instead.
 *   No completion operation, remote ledger, validator or receipt protocol.
 *
 * No commands are run to build/install anything. Parent builds before execution.
 */
const REPO = fileURLToPath(new URL("../../", import.meta.url));
const FIXTURES = fileURLToPath(new URL("./native-fixtures/", import.meta.url));
const REQUIRED = process.env.FABRIC_TRANSPORT_REQUIRED === "1";
const DEADLINE_MS = 30_000;
const MAIN_MARKER = "NATIVE_MAIN_JOURNAL_REPLAY_ONCE";
const TASK_MARKER = "NATIVE_TASK_B3_UNCONSUMED_FOLLOW_UP";
const NATS_DEPENDENCY = "Remaining E4 NATS variant is not authored here. Actual A2 runtime/resident roots exist; " +
  "nats-native-boundaries.test.ts composes durable-close and durable-main-kill B5 through them with a real JWT broker. " +
  "NATS B4 kill/stop-failure, durable-host-kill and session-main-kill still need their own actual-root cases. " +
  "Setting a carrier env on this FILE fixture is not coverage; no passing substitute.";
const digest = (id: string) => createHash("sha256").update(id).digest("hex");
const completionFiles = (meshRoot: string, id: string) => {
  const directory = path.join(meshRoot, "agent-completions"); const name = `${digest(id)}.json`;
  return { directory, envelope: path.join(directory, name), attempt: path.join(directory, "attempts", name),
    receipt: path.join(directory, "receipts", name), archive: path.join(directory, "archive", name),
    claim: `residency/completion-claims/${digest(id)}` };
};

const roots: string[] = [];
const managers: AgentManager[] = [];
const logs: LaunchLog[] = [];
const rpcs: NativeRpc[] = [];
const servers: http.Server[] = [];
const meshes: MeshStore[] = [];
const temporary = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-boundaries-"));
  roots.push(root);
  return root;
};
const json = <T = Record<string, any>>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const rows = (file: string): Array<Record<string, any>> => {
  try { return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
const bounded = async <T>(promise: Promise<T>, label: string, ms = DEADLINE_MS): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Native boundary deadline: ${label}`)), ms);
    })]);
  } finally { clearTimeout(timer); }
};

/** Normal source is fs.watch, not a polling timer. Initial read closes the
 * watch/creation race; one finite deadline rejects missing receipts. */
const observe = <T>(directory: string, read: () => T | undefined, label: string): Promise<T> => new Promise((resolve, reject) => {
  let watcher: fs.FSWatcher | undefined;
  const finish = (error?: unknown, value?: T) => {
    clearTimeout(timer); watcher?.close();
    if (error) reject(error); else resolve(value!);
  };
  const timer = setTimeout(() => finish(new Error(`Native boundary deadline: ${label}`)), DEADLINE_MS);
  const check = () => {
    try { const value = read(); if (value !== undefined) finish(undefined, value); }
    catch (error) {
      // A non-atomic diagnostic/JSONL append can be visible while its writer is
      // still appending. Retry only absence/torn JSON on the next filesystem event.
      if (!(error instanceof SyntaxError) && (error as NodeJS.ErrnoException).code !== "ENOENT") finish(error);
    }
  };
  try { watcher = fs.watch(directory, check); watcher.on("error", finish); check(); }
  catch (error) { finish(error); }
});
const observeJson = <T extends Record<string, any>>(file: string, accepts: (value: T) => boolean = () => true) =>
  observe(path.dirname(file), () => {
    const value = json<T>(file);
    return accepts(value) ? value : undefined;
  }, file);
const unavailable = (ctx: TestContext, reason: string): never => {
  if (REQUIRED) throw new Error(`FABRIC_TRANSPORT_REQUIRED=1: ${reason}`);
  return ctx.skip(reason);
};
const nativeRequirements = (ctx: TestContext, worker = false) => {
  if (process.platform !== "linux") unavailable(ctx, "native SIGKILL/PID birth proof currently requires Linux");
  if (!/^node(?:\.exe)?$/.test(path.basename(process.execPath))) unavailable(ctx, "run this suite under Node: production runtime selection must never choose Bun for crash fixtures");
  const node = findExecutable("node");
  const cli = path.join(REPO, "node_modules", "@earendil-works", "pi-coding-agent", "dist", "cli.js");
  if (!node || !fs.existsSync(cli)) unavailable(ctx, "real Node and the pinned native Pi CLI are required");
  const workerPath = path.join(REPO, "dist", "worker.js");
  if (worker && !fs.existsSync(workerPath)) unavailable(ctx, "compiled dist/worker.js is absent; parent must run bun run build");
  return { node: node!, cli, workerPath };
};
const killRecorded = (log: LaunchLog, pid: number, signal: NodeJS.Signals) => {
  const identity = log.owned().find(entry => entry.pid === pid);
  expect(identity, `PID ${pid} must have recorded its own launch before signalling`).toBeDefined();
  expect(same(identity!), "PID birth must still match before signalling").toBe(true);
  process.kill(identity!.pid, signal);
  return identity!;
};

/** Actual Pi RPC. No model text is used as a shell/program. Only inference is
 * synthetic (loopback SSE); native queues, sessions and extension hooks are real. */
class NativeRpc {
  readonly child: ChildProcess;
  readonly events: Array<Record<string, any>> = [];
  readonly exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  readonly pending = new Map<string, { resolve(value: any): void; reject(error: Error): void }>();
  readonly listeners = new Set<() => void>();
  stderr = "";
  closed = false;
  constructor(node: string, cli: string, root: string, env: NodeJS.ProcessEnv, sessionFile?: string, extension = "main-journal.ts") {
    this.child = spawn(node, [cli, "--mode", "rpc", "--offline", "--no-extensions", "--no-skills", "--no-context-files",
      "--no-prompt-templates", "--no-themes", "--approve", "-e", path.join(FIXTURES, extension),
      "--provider", "boundary-offline", "--model", "offline", "--thinking", "off", "--tools", "",
      "--session-dir", path.join(root, "sessions"), ...(sessionFile ? ["--session", sessionFile] : [])], {
      cwd: root, env, stdio: ["pipe", "pipe", "pipe"], timeout: 90_000, killSignal: "SIGKILL",
    });
    this.exit = new Promise(resolve => this.child.once("close", (code, signal) => {
      this.closed = true;
      for (const request of this.pending.values()) request.reject(new Error(`Native Pi closed (${code}/${signal}): ${this.stderr}`));
      this.pending.clear();
      for (const notify of this.listeners) notify();
      resolve({ code, signal });
    }));
    this.child.on("error", error => {
      for (const request of this.pending.values()) request.reject(error);
      this.pending.clear();
    });
    let tail = "";
    this.child.stdout!.on("data", chunk => {
      tail += chunk.toString();
      for (let end = tail.indexOf("\n"); end !== -1; end = tail.indexOf("\n")) {
        const line = tail.slice(0, end); tail = tail.slice(end + 1);
        if (!line.trim()) continue;
        let event: Record<string, any>;
        try { event = JSON.parse(line); } catch { this.stderr += `\nNon-RPC stdout: ${line}`; continue; }
        this.events.push(event);
        if (event.type === "response") {
          const request = this.pending.get(event.id); this.pending.delete(event.id);
          if (request) event.success ? request.resolve(event.data) : request.reject(new Error(event.error));
        }
        for (const notify of this.listeners) notify();
      }
    });
    this.child.stderr!.on("data", chunk => { this.stderr += chunk.toString(); });
    rpcs.push(this);
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
  async event(predicate: (event: Record<string, any>) => boolean): Promise<Record<string, any>> {
    let check!: () => void;
    try {
      return await bounded(new Promise<Record<string, any>>((resolve, reject) => {
        check = () => {
          const value = this.events.find(predicate);
          if (value) resolve(value);
          else if (this.closed) reject(new Error(`Native Pi closed before event: ${this.stderr}`));
        };
        this.listeners.add(check); check();
      }), "native lifecycle event");
    } finally { this.listeners.delete(check); }
  }
  async close(): Promise<void> {
    if (this.closed) return;
    this.child.stdin!.end();
    await bounded(this.exit, "native Pi cooperative cleanup", 10_000);
  }
}

const loopback = async (root: string, holdFirst = false) => {
  const requests: Array<Record<string, any>> = [];
  const arrivals = new Map<number, (value: Record<string, any>) => void>();
  const releases: Array<() => void> = [];
  const held = new Set<number>(holdFirst ? [0] : []);
  const server = http.createServer((request, response) => {
    let text = "";
    request.on("data", chunk => { text += chunk; });
    request.on("end", () => {
      const payload = JSON.parse(text) as Record<string, any>;
      requests.push(payload);
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
        id: "offline-boundary", object: "chat.completion.chunk", created: 1, model: "offline",
        choices: [{ index: 0, delta, finish_reason }],
      })}\n\n`);
      let released = false;
      const release = () => {
        if (released || response.destroyed) return;
        released = true;
        chunk({ role: "assistant", content: "native boundary final answer" });
        chunk({}, "stop"); response.end("data: [DONE]\n\n");
      };
      releases.push(release);
      arrivals.get(requests.length - 1)?.(payload); arrivals.delete(requests.length - 1);
      if (!held.has(requests.length - 1)) release();
    });
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const agent = path.join(root, "agent"); fs.mkdirSync(agent, { mode: 0o700 });
  fs.writeFileSync(path.join(agent, "models.json"), JSON.stringify({ providers: { "boundary-offline": {
    baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only-not-a-credential", api: "openai-completions",
    models: [{ id: "offline", name: "Offline boundary", reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  } } }));
  fs.writeFileSync(path.join(agent, "settings.json"), JSON.stringify({
    enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false }, packages: [], extensions: [],
  }));
  const log = launchLog(root); logs.push(log);
  const tmp = path.join(root, "tmp"); fs.mkdirSync(tmp, { mode: 0o700 });
  const env: NodeJS.ProcessEnv = { ...process.env, ...log.env, TMPDIR: tmp,
    VITE_CACHE_DIR: path.join(tmp, "vite"), XDG_CACHE_HOME: path.join(tmp, "xdg"), npm_config_cache: path.join(tmp, "npm"),
    PI_CODING_AGENT_DIR: agent, PI_OFFLINE: "1",
    PI_SKIP_VERSION_CHECK: "1", FABRIC_NATIVE_BOUNDARY_ROOT: root };
  return { agent, log, env, requests,
    holdNext: () => { held.add(requests.length); },
    requestAt: (index: number) => requests[index] ? Promise.resolve(requests[index]!) : bounded(new Promise<Record<string, any>>(resolve => arrivals.set(index, resolve)), "loopback model request"),
    release: (index: number) => { const release = releases[index]; if (!release) throw new Error(`No request ${index}`); release(); },
  };
};

// B4's source-real producer with intake held. Case (iii) also attaches a REAL
// native Main/inbox to this address, to consume while the worker remains alive.
const offlineParent = (root: string, nativeRecipient?: CompletionRecipient) => {
  const session = nativeRecipient ? undefined : SessionManager.create(root, path.join(root, "parent-sessions"));
  const recipient: CompletionRecipient = nativeRecipient ?? { rootId: `session:${session!.getSessionId()}`, sessionId: session!.getSessionId(),
    projectRoot: root, cwd: root, name: "spawn-bound parent", startedAt: Date.now() };
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents);
  const producer = new CompletionJournal(meshRoot, recipient, { list: () => [] } as unknown as FabricParticipantSource, mesh,
    () => { throw new Error("B4 must not drain the offline parent's journal"); });
  meshes.push(mesh);
  for (const name of ["", "attempts", "receipts", "archive"]) fs.mkdirSync(path.join(meshRoot, "agent-completions", name), { recursive: true, mode: 0o700 });
  return { recipient, meshRoot, producer,
    // Save through the REAL producer, which reads completion-recipient.json.
    onSettled: (result: AgentRunResult) => producer.save(result),
  };
};

// Cleanup retains scratch if any joined manager/native process still owns it.
// Node children inherit NODE_OPTIONS from launchLog, INCLUDING the deletion
// guard installed by fleet-isolation-setup. Never launch Bun crash fixtures.
afterEach(async () => {
  let failure: unknown;
  for (const manager of managers.splice(0)) {
    try { await bounded(manager.close(), "manager cleanup", 15_000); } catch (error) { failure ??= error; }
  }
  const native = rpcs.splice(0);
  for (const rpc of native) {
    try { await rpc.close(); } catch { /* bounded identity-safe stop below owns escalation */ }
  }
  for (const log of logs.splice(0)) {
    try { await stopAllOwned(log.owned(), 1_000, 5_000); } catch (error) { failure ??= error; }
  }
  for (const rpc of native) {
    try { await bounded(rpc.exit, "captured native close before scratch deletion", 5_000); }
    catch (error) { failure ??= error; }
  }
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    await bounded(new Promise<void>(resolve => server.close(() => resolve())), "loopback server cleanup", 5_000);
  }
  vi.restoreAllMocks();
  for (const mesh of meshes.splice(0)) mesh.closeState();
  vi.unstubAllEnvs();
  if (failure) { roots.splice(0); throw failure; }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 60_000);

describe("T-B3 native custody -> consumption (not carrier ACK conformance)", () => {
  it("Main survives SIGKILL after journal admission/before injection and consumes once in the exact native Pi session", async ctx => {
    const { node, cli } = nativeRequirements(ctx);
    const root = temporary(); const h = await loopback(root);
    const first = new NativeRpc(node, cli, root, h.env);
    await observeJson(path.join(root, "main-ready.json"), value => value.pid === first.child.pid);
    await first.request({ type: "prompt", message: "Persist the baseline native session." });
    await first.event(event => event.type === "agent_settled");
    const state = await first.request({ type: "get_state" });
    expect(typeof state.sessionId).toBe("string");
    expect(typeof state.sessionFile).toBe("string");
    const journal = path.join(root, "main-followups", `${state.sessionId}.json`);
    await first.request({ type: "prompt", message: "/native-boundary-admit" });
    const admission = await observeJson(path.join(root, "main-admitted.json"));
    expect(admission.result).toMatchObject({ queued: true, routed: "main", triggered: false });
    const custody = json(journal);
    expect(custody.items).toHaveLength(1);
    expect(custody.items[0]).toMatchObject({ id: admission.result.messageId, message: MAIN_MARKER,
      deliveryId: "native-boundary-main-b3", deliverAs: "followUp", triggerTurn: true });
    // The baseline user input may already have created an EMPTY owner index.
    expect(json(`${journal}.delivered`).ids).not.toContain("native-boundary-main-b3");
    expect(JSON.stringify(await first.request({ type: "get_messages" }))).not.toContain(MAIN_MARKER);
    expect(JSON.stringify(h.requests)).not.toContain(MAIN_MARKER);

    killRecorded(h.log, first.child.pid!, "SIGKILL");
    expect(await bounded(first.exit, "injected Main SIGKILL close")).toEqual({ code: null, signal: "SIGKILL" });
    // This is an actual killed process, not closeFollowUpDrain/reload pretending
    // to be a crash. Its already-acknowledgeable native custody still exists.
    expect(json(journal).items[0].id).toBe(admission.result.messageId);

    const second = new NativeRpc(node, cli, root, h.env, state.sessionFile);
    await observeJson(path.join(root, "main-ready.json"), value => value.pid === second.child.pid);
    await second.event(event => event.type === "agent_settled");
    const resumed = await second.request({ type: "get_state" });
    expect(resumed.sessionId).toBe(state.sessionId);
    const index = await observeJson(`${journal}.delivered`, value => value.ids?.includes("native-boundary-main-b3"));
    expect(index.ids.filter((id: string) => id === "native-boundary-main-b3")).toHaveLength(1);
    expect(fs.existsSync(journal)).toBe(false);
    expect(h.requests.filter(request => JSON.stringify(request.messages).includes(MAIN_MARKER))).toHaveLength(1);
    const carriers = () => rows(state.sessionFile).filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message" && JSON.stringify(entry).includes(MAIN_MARKER));
    expect(carriers()).toHaveLength(1);
    expect(JSON.stringify(carriers()[0])).toContain(admission.result.messageId);
    await second.request({ type: "prompt", message: "/native-boundary-retry" });
    expect(await observeJson(path.join(root, "main-retry.json"))).toMatchObject({ queued: true, duplicate: true, triggered: false });
    expect(carriers()).toHaveLength(1);
    await second.close(); // Explicitly graceful cleanup, never the crash claim.

    const third = new NativeRpc(node, cli, root, h.env, state.sessionFile);
    await observeJson(path.join(root, "main-ready.json"), value => value.pid === third.child.pid);
    expect((await third.request({ type: "get_state" })).sessionId).toBe(state.sessionId);
    expect(carriers()).toHaveLength(1);
    expect(h.requests.filter(request => JSON.stringify(request.messages).includes(MAIN_MARKER))).toHaveLength(1);
    expect(fs.existsSync(journal)).toBe(false);
  }, 90_000);

  it("real principal-delivery gate receipts delivered only at context consumption (recorded host callbacks; NOT native crash/E2E)", async () => {
    const root = temporary(); const deliveries = path.join(root, "deliveries"); fs.mkdirSync(deliveries);
    vi.stubEnv("PI_FABRIC_DELIVERY_DIR", deliveries);
    vi.stubEnv("PI_FABRIC_TERMINAL_TASK", "0");
    const id = randomUUID(); const file = followUpFile(root, id);
    fs.mkdirSync(path.dirname(file));
    fs.writeFileSync(file, JSON.stringify({ messageId: id, deadlineAt: Date.now() + 60_000 }));
    fs.writeFileSync(path.join(deliveries, `${id}.json`), JSON.stringify({ followUpId: id, delivery: "followUp", message: "receipt only at native context",
      provenance: fabricTurnProvenance({ id: "session:parent", name: "parent", kind: "main" }, "followUp", "mesh") }));
    const hooks = new Map<string, (...args: any[]) => any>();
    let command!: { handler: (id: string, ctx: ExtensionContext) => Promise<void> };
    const sent: Array<{ content: any; options: any }> = [];
    principalDelivery({
      on: (name: string, hook: (...args: any[]) => any) => { hooks.set(name, hook); },
      registerCommand: (_name: string, value: typeof command) => { command = value; },
      sendUserMessage: (content: any, options: any) => { sent.push({ content, options }); },
    } as unknown as ExtensionAPI);
    const context = { isIdle: () => true } as unknown as ExtensionContext;
    await command.handler(id, context);
    expect(sent).toHaveLength(1);
    expect(sent[0]!.options.deliverAs).toBe("followUp");
    expect(followUpState(file)).toBe("queued");
    expect(fs.existsSync(path.join(deliveries, `${id}.json`))).toBe(true);
    const message = { role: "user", content: [{ type: "text", text: sent[0]!.content }] };
    const consumed = hooks.get("context")!({ messages: [message, message] });
    expect(consumed.messages).toHaveLength(1);
    expect(consumed.messages[0].content[0].text).toBe("receipt only at native context");
    expect(followUpState(file)).toBe("delivered");
    expect(fs.readFileSync(`${file}.settled/state`, "utf8")).toBe("delivered");
    expect(fs.existsSync(path.join(deliveries, `${id}.json`))).toBe(false);
    await command.handler(id, context);
    expect(sent).toHaveLength(1); // Repeated transport submission does not inject again.
  });

  it("a real native task explicitly refuses an admitted, still-unconsumed followUp with its final-answer receipt", async ctx => {
    const { node, cli, workerPath } = nativeRequirements(ctx, true);
    const root = temporary(); const h = await loopback(root, true);
    useNativeEnvironment(h.env, node); // Production worker must not choose unguarded Bun.
    const notices: unknown[] = [];
    let noticeArrived!: () => void;
    const noticeReceipt = new Promise<void>(resolve => { noticeArrived = resolve; });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 30_000,
      retainRuns: true, sessionExport: false, nice: 19 }, {
      workerPath, piBinary: cli, runRoot: path.join(root, "runs"), fullCodeMode: false,
      onTerminalNotice: notice => { notices.push(notice); noticeArrived(); },
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "Return the bounded native task answer.", model: "boundary-offline/offline", thinking: "off",
      needs: ["local"], transport: "process", extensions: false, tools: [] });
    await h.requestAt(0);
    const run = path.join(root, "runs", handle.id);
    const provenance = fabricTurnProvenance({ id: "session:parent", name: "parent", kind: "main" }, "followUp", "mesh");
    assertLocalWorker(run, h.log);
    // No deadline means this is the ordinary control path: no tracked follow-up
    // admission, no private follow-up receipt, and no fabricated native state.
    const admitted = manager.followUp(handle.id, TASK_MARKER, undefined, provenance);
    expect(admitted).toMatchObject({ queued: true });
    expect(admitted.undelivered).toBeUndefined();
    expect(admitted.deadlineAt).toBeUndefined();
    expect(rows(path.join(run, "steer.jsonl")).find(row => row.id === admitted.messageId)).toMatchObject({
      type: "follow_up", message: TASK_MARKER, provenance,
    });
    const controls = path.join(run, "terminal-controls");
    await observe(run, () => fs.existsSync(controls) ? true : undefined, "worker terminal-control directory");
    await observe(controls, () => readTerminalControl(run, admitted.messageId)?.state === "queued" ? true : undefined, "worker followUp custody");
    // The worker's real bootstrap reports both the correlated prompt response
    // and Pi's native followUp queue admission. A command response alone only
    // proves its handler ran: sendUserMessage has asynchronous input preflight.
    // queue_update reports _queueFollowUp; its same synchronous stack then calls
    // agent.followUp before this parent can release the held first SSE.
    await observe(run, () => rows(path.join(run, "events.jsonl")).find(row => row.type === "response" &&
      row.id === admitted.messageId && row.command === "prompt" && row.success === true), "native followUp command response");
    const queuedMessage = terminalControlMessage(admitted.messageId, TASK_MARKER);
    const queue = await observe(run, () => rows(path.join(run, "events.jsonl")).find(row => row.type === "queue_update" &&
      Array.isArray(row.followUp) && row.followUp.includes(queuedMessage)), "native followUp queue admission");
    expect(queue).toMatchObject({ steering: [], followUp: [queuedMessage] });
    expect(readTerminalControl(run, admitted.messageId)).toMatchObject({ id: admitted.messageId,
      delivery: "followUp", provenance, state: "queued", forwarded: true });
    expect(readFinalAnswerReceipt(run, handle.id)).toBeUndefined();
    expect(rows(path.join(run, "events.jsonl")).filter(row => row.type === "message_end" &&
      row.message?.role === "user" && JSON.stringify(row.message).includes(TASK_MARKER))).toEqual([]);
    expect(h.requests).toHaveLength(1);
    expect(JSON.stringify(h.requests)).not.toContain(TASK_MARKER);
    h.release(0);
    const result = await manager.wait(handle.id, { timeoutMs: DEADLINE_MS });
    expect(result.status, result.error).toBe("completed");
    const receipt = readFinalAnswerReceipt(run, handle.id)!;
    expect(receipt).toBeDefined();
    expect(result.finalAnswerReceipt?.id).toBe(receipt.id);
    expect(Reflect.get(manager.status(handle.id), "lostContact"), "clean native exit has no exit debt").toBeUndefined();
    expect(readTerminalControl(run, admitted.messageId)).toMatchObject({ state: "refused", forwarded: true });
    const refusal = await observe(run, () => rows(path.join(run, "lifecycle.jsonl")).find(row => row.event === "message.refused" && row.data?.messageId === admitted.messageId), "native followUp terminal refusal");
    expect(refusal.data).toMatchObject({ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: receipt.id, messageId: admitted.messageId, delivery: "followUp" });
    expect(refusal.data.sender).toEqual(provenance.sender);
    // onSettled/wait can precede the monitor's lifecycle/outbox drain. Await
    // the real original-sender callback instead of treating wait as that receipt.
    await bounded(noticeReceipt, "original sender terminal refusal");
    const expectedNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: receipt.id, messageId: admitted.messageId, delivery: "followUp", sender: provenance.sender };
    expect(notices).toEqual([expectedNotice]);
    // The original-sender callback's durable route ACK is separate from native
    // discard. Await it before checking ordinary status/close cannot redeliver.
    expect(await observeJson(path.join(run, "terminal-notices", `${admitted.messageId}.json.delivered`))).toEqual(expectedNotice);
    expect(h.requests).toHaveLength(1);
    expect(h.requests.every(request => !JSON.stringify(request).includes(TASK_MARKER))).toBe(true);
    for (const deliver of [() => manager.followUp(handle.id, "LATE"), () => manager.steer(handle.id, "LATE")]) {
      let error: unknown;
      try { deliver(); } catch (caught) { error = caught; }
      expect(error).toMatchObject({ name: "FabricTargetTerminalError", code: "FABRIC_TARGET_TERMINAL",
        reason: "run-ended", deliveryStatus: "undelivered", targetId: handle.id, finalAnswerReceiptId: receipt.id });
    }
    manager.status(handle.id);
    await bounded(manager.close(), "native refusal manager drain", 15_000);
    expect(notices).toEqual([expectedNotice]);
    expect(rows(path.join(run, "lifecycle.jsonl")).filter(row => row.event === "message.refused" &&
      row.data?.messageId === admitted.messageId)).toHaveLength(1);
    expect(h.requests).toHaveLength(1);
    expect(readTerminalControl(run, admitted.messageId)).toMatchObject({ state: "refused", forwarded: true });
    // Native B3 refusal, not a task crash/restart or B4 exit assertion.
  }, 60_000);

  it("a tracked deadline followUp may be consumed before the final answer", async ctx => {
    const { node, cli, workerPath } = nativeRequirements(ctx, true);
    const root = temporary(); const h = await loopback(root, true);
    useNativeEnvironment(h.env, node);
    const notices: unknown[] = [];
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 30_000,
      retainRuns: true, sessionExport: false, nice: 19 }, {
      workerPath, piBinary: cli, runRoot: path.join(root, "runs"), fullCodeMode: false,
      onTerminalNotice: notice => { notices.push(notice); },
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "Return the bounded native task answer.", model: "boundary-offline/offline", thinking: "off",
      needs: ["local"], transport: "process", extensions: false, tools: [] });
    await h.requestAt(0);
    const run = path.join(root, "runs", handle.id);
    const provenance = fabricTurnProvenance({ id: "session:parent", name: "parent", kind: "main" }, "followUp", "mesh");
    assertLocalWorker(run, h.log);
    const admitted = manager.followUp(handle.id, TASK_MARKER, undefined, provenance, { deadlineMs: 60_000 });
    const file = followUpFile(run, admitted.messageId);
    expect(json(file)).toMatchObject({ messageId: admitted.messageId, deadlineAt: admitted.deadlineAt });
    expect(followUpState(file)).toBe("queued");
    expect(readFinalAnswerReceipt(run, handle.id)).toBeUndefined();
    const controls = path.join(run, "terminal-controls");
    await observe(run, () => fs.existsSync(controls) ? true : undefined, "worker terminal-control directory");
    await observe(controls, () => readTerminalControl(run, admitted.messageId)?.state === "queued" ? true : undefined, "worker tracked followUp custody");
    const deliveries = path.join(run, "deliveries");
    await observe(run, () => fs.existsSync(deliveries) ? true : undefined, "worker private-delivery directory");
    const payload = path.join(deliveries, `${admitted.messageId}.json`);
    await observeJson(payload, value => value.followUpId === admitted.messageId);
    expect(json(payload).message).toContain(TASK_MARKER);
    await observe(run, () => rows(path.join(run, "events.jsonl")).find(row => row.type === "response" &&
      row.id === admitted.messageId && row.command === "prompt" && row.success === true), "native private-delivery command response");
    expect(readTerminalControl(run, admitted.messageId)).toMatchObject({ state: "queued", forwarded: true });
    expect(followUpState(file)).toBe("queued");
    expect(h.requests).toHaveLength(1);
    expect(JSON.stringify(h.requests)).not.toContain(TASK_MARKER);
    expect(rows(path.join(run, "events.jsonl")).filter(row => row.type === "queue_update" &&
      JSON.stringify(row.followUp ?? []).includes(TASK_MARKER))).toEqual([]);
    // A tracked admission is held outside the native queue while busy. The
    // first assistant stop deliberately defers finality so this input can drain.
    // Hold the SECOND SSE too: prove actual consumption before any final receipt,
    // not just a post-settlement delivered snapshot or two allowed outcomes.
    h.holdNext();
    h.release(0);
    const continuation = await h.requestAt(1);
    expect(JSON.stringify(continuation)).toContain(TASK_MARKER);
    expect(JSON.stringify(h.requests[0])).not.toContain(TASK_MARKER);
    expect(followUpState(file)).toBe("delivered");
    expect(readTerminalControl(run, admitted.messageId)).toMatchObject({ state: "delivered", forwarded: true });
    expect(readFinalAnswerReceipt(run, handle.id)).toBeUndefined();
    expect(notices).toEqual([]);
    h.release(1);
    const result = await manager.wait(handle.id, { timeoutMs: DEADLINE_MS });
    expect(result.status, result.error).toBe("completed");
    const receipt = readFinalAnswerReceipt(run, handle.id)!;
    expect(receipt).toBeDefined();
    expect(result.finalAnswerReceipt?.id).toBe(receipt.id);
    expect(followUpState(file)).toBe("delivered");
    expect(readTerminalControl(run, admitted.messageId)).toMatchObject({ state: "delivered", forwarded: true });
    expect(h.requests).toHaveLength(2);
    expect(h.requests.filter(request => JSON.stringify(request).includes(TASK_MARKER))).toHaveLength(1);
    expect(JSON.stringify(h.requests[1])).toContain(TASK_MARKER);
    manager.status(handle.id);
    await bounded(manager.close(), "tracked followUp manager drain", 15_000);
    expect(rows(path.join(run, "lifecycle.jsonl")).filter(row => row.event === "message.refused" && row.data?.messageId === admitted.messageId)).toEqual([]);
    expect(notices).toEqual([]);
    expect(followUpState(file)).toBe("delivered");
    expect(readTerminalControl(run, admitted.messageId)).toMatchObject({ state: "delivered", forwarded: true });
    expect(h.requests).toHaveLength(2);
  }, 60_000);
});

const useNativeEnvironment = (env: NodeJS.ProcessEnv, node: string) => {
  for (const [key, value] of Object.entries(env)) if (value !== undefined &&
    (["NODE_OPTIONS", "TMPDIR", "VITE_CACHE_DIR", "XDG_CACHE_HOME", "npm_config_cache"].includes(key) || key.startsWith("PI_") || key.startsWith("FABRIC_NATIVE_"))) vi.stubEnv(key, value);
  vi.stubEnv("PI_FABRIC_NODE_BINARY", node);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
};
const nativeTask = { task: "Return the bounded native E4 result.", model: "boundary-offline/offline", thinking: "off" as const,
  needs: ["local"], transport: "process" as const, extensions: false, tools: [] };
const STOP_FAIL_KEEP_WORKER_ALIVE = "E4_STOP_FAIL_KEEP_WORKER_ALIVE";
const assertLocalWorker = (run: string, log: LaunchLog) => {
  const record = json(path.join(run, "status.json"));
  expect(record.transport).toBe("process");
  expect(record.sessionId).toMatch(/^\d+$/);
  const pid = Number(record.sessionId);
  expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
  expect(fs.existsSync(path.join(run, "placement.json"))).toBe(false);
  const identity = log.owned().find(entry => entry.pid === pid);
  expect(identity, "worker must be a locally recorded Node process").toBeDefined();
  expect(same(identity!), "local worker birth must still match").toBe(true);
  process.kill(pid, 0);
  return pid;
};

/** Records the actual production transport, not a replacement worker/engine.
 * The only injection for (iii) is failure of stop on that same native handle.
 * All launch, stdin, native Pi, final-answer, custody and admission code is real. */
const nativeExitHarness = async (ctx: TestContext, gated = false, withNativeParent = false) => {
  const { node, cli, workerPath } = nativeRequirements(ctx, true);
  const root = temporary(); const h = await loopback(root, true);
  const gate = path.join(root, "worker-exit-gate.json");
  const env = { ...h.env, ...(gated ? { FABRIC_NATIVE_WORKER_EXIT_GATE: gate,
    NODE_OPTIONS: `${h.env.NODE_OPTIONS ?? ""} --import=${pathToFileURL(path.join(FIXTURES, "worker-exit-gate.mjs")).href}` } : {}) };
  useNativeEnvironment(env, node);
  let deliveryParent: NativeRpc | undefined; let nativeRecipient: CompletionRecipient | undefined;
  if (withNativeParent) {
    deliveryParent = new NativeRpc(node, cli, root, { ...env, PI_FABRIC_NODE_BINARY: node,
      FABRIC_NATIVE_PI_CLI: cli, FABRIC_NATIVE_COMPLETION_INTAKE: "held" }, undefined, "completion-parent.ts");
    const ready = await observeJson(path.join(root, "completion-main-ready.json"), value => value.pid === deliveryParent!.child.pid);
    nativeRecipient = ready.recipient as CompletionRecipient;
  }
  const parent = offlineParent(root, nativeRecipient);
  const transports = new Map<string, Awaited<ReturnType<ProcessTransport["launch"]>>>();
  const original = ProcessTransport.prototype.launch;
  let failStop = false; let stopFailed = false;
  const stopFailures: Array<{ seam: string; pid: number; statusSessionId: string; alive: boolean; gateHeld: boolean;
    placementAbsent: boolean; unresolved: boolean; veto: string | undefined }> = [];
  let launchCheck: ((id: string) => void) | undefined;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    launchCheck?.(request.id);
    const native = await original.call(this, request);
    const handle = { ...native, stop: async (...args: Parameters<typeof native.stop>) => {
      if (failStop) {
        // Named seam: do NOT delegate stop or signal the worker. The preload
        // holds this actual worker inside its completed-status rename. Observe
        // (d) BEFORE throwing/markLost, otherwise the unresolved marker masks
        // the live numeric PID branch we are required to exercise.
        const run = path.join(root, "runs", request.id); const pid = Number(native.sessionId);
        const identity = h.log.owned().find(entry => entry.pid === pid);
        stopFailures.push({ seam: STOP_FAIL_KEEP_WORKER_ALIVE, pid,
          statusSessionId: String(json(path.join(run, "status.json")).sessionId),
          alive: identity !== undefined && same(identity),
          gateHeld: fs.existsSync(gate) && json(gate).pid === pid,
          placementAbsent: !fs.existsSync(path.join(run, "placement.json")),
          unresolved: fs.existsSync(path.join(run, "unresolved-worker.json")),
          veto: runTreeResourceVeto(run, 0, undefined, true, true) });
        stopFailed = true;
        throw new Error(`${STOP_FAIL_KEEP_WORKER_ALIVE}: native exit unconfirmed`);
      }
      return native.stop(...args);
    }, lostContact: () => stopFailed && failStop ? "E4 injected stop failure: native exit unconfirmed" : native.lostContact?.() };
    transports.set(request.id, handle);
    return handle;
  });
  const settlements: Array<{ result: AgentRunResult; veto: string | undefined; unresolved: boolean }> = [];
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, timeoutMs: 30_000,
    retainRuns: true, sessionExport: false, nice: 19 }, {
    workerPath, piBinary: cli, runRoot: path.join(root, "runs"), fullCodeMode: false,
    meshRoot: parent.meshRoot, projectRoot: root, completionRecipient: parent.recipient,
    onSettled: result => {
      const run = path.join(root, "runs", result.id);
      settlements.push({ result, veto: runTreeResourceVeto(run, 0, undefined, true, true),
        unresolved: fs.existsSync(path.join(run, "unresolved-worker.json")) });
      parent.onSettled(result);
    },
  });
  managers.push(manager);
  return { root, h, gate, parent, deliveryParent, manager, transports, settlements, stopFailures,
    injectStopFailure: () => { failStop = true; }, restoreStop: () => { failStop = false; },
    setLaunchCheck: (check: (id: string) => void) => { launchCheck = check; },
  };
};
const onlyEnvelope = (meshRoot: string, id: string) => {
  const files = completionFiles(meshRoot, id);
  expect(fs.readdirSync(files.directory).filter(file => /^[a-f0-9]{64}\.json$/.test(file))).toEqual([`${digest(id)}.json`]);
  return json<CompletionEnvelope>(files.envelope);
};

describe("E4 T-B4 — file carrier's unchanged native result/exit custody", () => {
  it("(i) SIGKILL before final answer, followed by explicit stop: one non-success envelope, persisted exit at onSettled or lostContact", async ctx => {
    const { root, h, parent, manager, transports, settlements } = await nativeExitHarness(ctx);
    const handle = await manager.spawn(nativeTask);
    await h.requestAt(0);
    const run = path.join(root, "runs", handle.id);
    assertLocalWorker(run, h.log);
    expect(readFinalAnswerReceipt(run, handle.id)).toBeUndefined();
    const transport = transports.get(handle.id)!;
    killRecorded(h.log, Number(transport.sessionId), "SIGKILL");
    // This case intentionally exercises kill+operator-stop, not startup retry.
    // Explicit stop forbids another attempt/model call after the killed worker.
    await bounded(manager.stop(handle.id), "kill-before-answer native stop/join");
    const result = await manager.wait(handle.id, { timeoutMs: DEADLINE_MS });
    expect(["failed", "timed_out", "stopped"]).toContain(result.status);
    expect(result.finalAnswerReceipt).toBeUndefined();
    await bounded(transport.closed!, "killed worker's captured native close");
    const envelope = onlyEnvelope(parent.meshRoot, handle.id);
    expect(envelope.recipient).toEqual(json(path.join(run, "completion-recipient.json")).recipient);
    expect(envelope.result.status).toBe(result.status);
    expect(envelope.result.finalAnswerReceipt).toBeUndefined();
    expect(settlements).toHaveLength(1);
    expect(settlements[0]!.veto === undefined || settlements[0]!.unresolved).toBe(true);
    if (!settlements[0]!.unresolved) expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeUndefined();
    expect(h.requests).toHaveLength(1);
  }, 60_000);

  it("(ii) SIGKILL after final-answer/status but before exit: completed receipt survives, concurrency-1 permit stays fenced until exit", async ctx => {
    const { root, h, gate, parent, manager, transports, setLaunchCheck } = await nativeExitHarness(ctx, true);
    const first = await manager.spawn(nativeTask);
    await h.requestAt(0);
    assertLocalWorker(path.join(root, "runs", first.id), h.log);
    const second = await manager.spawn({ ...nativeTask, task: "Queued E4 successor." });
    expect(second.status).toBe("queued");
    h.release(0);
    const held = await observeJson(gate, value => value.runId === first.id);
    const run = path.join(root, "runs", first.id);
    const result = await manager.wait(first.id, { timeoutMs: DEADLINE_MS });
    const answer = readFinalAnswerReceipt(run, first.id)!;
    expect(result.status).toBe("completed");
    expect(result.finalAnswerReceipt?.id).toBe(answer.id);
    const envelope = onlyEnvelope(parent.meshRoot, first.id);
    expect(envelope.result).toMatchObject({ status: "completed", text: answer.text, finalAnswerReceipt: { id: answer.id } });
    expect(held.receiptId).toBe(answer.id);
    expect(runTreeResourceVeto(run, 0, undefined, true, true)).toMatch(/root worker may still be running/);
    expect(manager.status(second.id).status).toBe("queued");
    expect(transports.has(second.id)).toBe(false);
    const transport = transports.get(first.id)!;
    // Check (d), the existing persisted-exit predicate, at the exact production
    // successor launch seam. Keep its strict live-PID veto: a live reused PID
    // is NOT safe here. A model request alone is too late to prove ordering.
    setLaunchCheck(id => {
      if (id === second.id) expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeUndefined();
    });
    killRecorded(h.log, held.pid, "SIGKILL");
    await bounded(transport.closed!, "post-answer SIGKILL native/tree close");
    await h.requestAt(1);
    expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeUndefined();
    expect((await manager.wait(second.id, { timeoutMs: DEADLINE_MS })).status).toBe("completed");
    expect((await manager.wait(first.id)).finalAnswerReceipt?.id).toBe(answer.id);
  }, 60_000);

  it("(iii) native stop failure: real completion is published but exit/collection/permit remain fenced and status must report unconfirmed exit", async ctx => {
    const { root, h, gate, parent, deliveryParent, manager, transports, stopFailures, injectStopFailure, restoreStop } = await nativeExitHarness(ctx, true, true);
    const handle = await manager.spawn(nativeTask);
    await h.requestAt(0);
    const run = path.join(root, "runs", handle.id);
    const localPid = assertLocalWorker(run, h.log);
    injectStopFailure();
    try {
      h.release(0);
      await observeJson(gate, value => value.runId === handle.id && value.pid === localPid);
      const result = await manager.wait(handle.id, { timeoutMs: DEADLINE_MS });
      expect(result.status).toBe("completed");
      expect(onlyEnvelope(parent.meshRoot, handle.id).result.finalAnswerReceipt?.id).toBe(result.finalAnswerReceipt?.id);
      const marker = await observeJson(path.join(run, "unresolved-worker.json"));
      expect(marker.reason).toContain("unconfirmed");
      expect(stopFailures.length, "named stop-failure seam must actually run").toBeGreaterThan(0);
      expect(stopFailures[0]).toMatchObject({ seam: STOP_FAIL_KEEP_WORKER_ALIVE, pid: localPid,
        statusSessionId: String(localPid), alive: true, gateHeld: true, placementAbsent: true, unresolved: false });
      expect(stopFailures[0]!.veto).toMatch(/root worker may still be running/);
      expect(assertLocalWorker(run, h.log)).toBe(localPid); // Still alive AFTER stop failed.
      expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeDefined();
      expect(canRemoveTerminalRun(run)).toBe(false);
      const queued = await manager.spawn({ ...nativeTask, task: "Must not launch while exit is unknown." });
      expect(queued.status).toBe("queued");
      expect(transports.has(queued.id)).toBe(false);
      expect(h.requests).toHaveLength(1);
      expect(fs.existsSync(run)).toBe(true);
      // Publication alone is not delivery. This actual native Main consumes
      // through its source-real journal/inbox while stop debt and the live PID
      // still fence execution resources; no recorded inbox substitute.
      const files = completionFiles(parent.meshRoot, handle.id);
      await deliveryParent!.request({ type: "prompt", message: "/native-completion-drain" });
      expect(await observeJson(files.receipt)).toMatchObject({ id: handle.id, sessionId: parent.recipient.sessionId });
      await observe(files.directory, () => fs.existsSync(files.archive) ? true : undefined, "B4(iii) native completion archived");
      expect(json<CompletionEnvelope>(files.archive).result).toMatchObject({ status: result.status,
        finalAnswerReceipt: { id: result.finalAnswerReceipt!.id } });
      const delivered = await observe(root, () => {
        const entries = rows(path.join(root, "completion-delivered.jsonl")).filter(row => row.id === handle.id);
        return entries.length ? entries : undefined;
      }, "B4(iii) native inbox delivered callback");
      expect(delivered).toHaveLength(1);
      expect(assertLocalWorker(run, h.log)).toBe(localPid);
      expect(manager.status(queued.id).status).toBe("queued");
      // E4 requires the public API, not only an internal marker, to report the
      // debt. Keep this assertion: at this pin #withTransportMetadata appears
      // to omit managed.lostContact, a potential REAL production bug for A2.
      // E18: terminal status stays unchanged; native-exit debt is an explicit field.
      expect(Reflect.get(manager.status(handle.id), "lostContact")).toEqual(expect.stringMatching(/\S/));
      expect(runTreeResourceVeto(run, 0, undefined, true, true)).toBeDefined();
    } finally {
      restoreStop(); // Cleanup regains the SAME real handle; no fake exit proof.
      const worker = transports.get(handle.id)!;
      if (h.log.owned().some(entry => entry.pid === Number(worker.sessionId) && same(entry))) killRecorded(h.log, Number(worker.sessionId), "SIGKILL");
      await bounded(worker.stop(), "stop-failure injection cleanup");
      await bounded(worker.closed!, "stop-failure captured close");
    }
  }, 60_000);
});

/** Same assertions for durable parent-close, durable Main SIGKILL, resident
 * SIGKILL after save, and session supervisor SIGKILL/attempt promotion. */
const parentOfflineCase = async (ctx: TestContext, variant: "durable-close" | "durable-main-kill" | "durable-host-kill" | "session-main-kill") => {
  const { node, cli } = nativeRequirements(ctx, true);
  for (const entry of ["index.js", "residency/launcher.js", "residency/pi-entry.js"]) {
    if (!fs.existsSync(path.join(REPO, "dist", entry))) unavailable(ctx, `native durable composition needs dist/${entry}; parent must build`);
  }
  const root = temporary(); const h = await loopback(root);
  // Baseline Main inference is immediate; only child inference is held.
  const env = { ...h.env, PI_FABRIC_NODE_BINARY: node, FABRIC_NATIVE_PI_CLI: cli, FABRIC_NATIVE_COMPLETION_INTAKE: "held" };
  const first = new NativeRpc(node, cli, root, env, undefined, "completion-parent.ts");
  const ready = await observeJson(path.join(root, "completion-main-ready.json"), value => value.pid === first.child.pid);
  await first.request({ type: "prompt", message: "Persist this exact E4 parent session before spawning." });
  await first.event(event => event.type === "agent_settled");
  const state = await first.request({ type: "get_state" });
  expect(state.sessionId).toBe(ready.recipient.sessionId);
  // Hold the child at a real model request without racing a file-written hook.
  // The loopback's explicit hold applies to the next request, not the baseline.
  h.holdNext();
  await first.request({ type: "prompt", message: `/native-completion-spawn ${variant.startsWith("durable") ? "durable" : "session"}` });
  const admitted = await observeJson(path.join(root, "completion-spawned.json"));
  const id = admitted.handle.id as string; const run = admitted.run as string;
  await h.requestAt(1);
  assertLocalWorker(run, h.log);
  const manifest = json(path.join(run, "completion-recipient.json"));
  expect(manifest.meshRoot).toBe(path.join(root, "mesh"));
  expect(manifest.recipient).toEqual(ready.recipient);
  const files = completionFiles(manifest.meshRoot, id);
  expect(fs.existsSync(files.envelope)).toBe(false);
  expect(fs.existsSync(files.receipt)).toBe(false);
  if (variant === "durable-close") await first.close();
  else {
    killRecorded(h.log, first.child.pid!, "SIGKILL");
    expect(await bounded(first.exit, "offline Main SIGKILL")).toEqual({ code: null, signal: "SIGKILL" });
  }
  h.release(1);
  if (variant === "session-main-kill") {
    const candidate = await observeJson(files.attempt);
    expect(candidate.supervisor.pid).toBe(first.child.pid);
    expect(candidate.recipient).toEqual(manifest.recipient);
    expect(candidate.result.status).toBe("completed");
    expect(fs.existsSync(files.envelope)).toBe(false); // Not settled by its dead manager.
  } else {
    const envelope = await observeJson(files.envelope);
    expect(envelope.recipient).toEqual(manifest.recipient);
    expect(onlyEnvelope(manifest.meshRoot, id).result.id).toBe(id);
    const settled = await observeJson(path.join(run, "manager-result.json"));
    expect(envelope.result.status).toBe(settled.status);
    expect(envelope.result.finalAnswerReceipt?.id).toBe(json(path.join(run, "final-answer.json")).id);
  }
  expect(fs.existsSync(files.receipt)).toBe(false);
  expect(rows(path.join(root, "completion-enqueued.jsonl"))).toEqual([]);
  if (variant === "durable-host-kill") {
    const owner = json(path.join(ready.config.residencyRoot, "owner.json"));
    // The real host has already durably saved, and Main has never drained.
    killRecorded(h.log, owner.pid, "SIGKILL");
    expect(fs.existsSync(files.envelope)).toBe(true);
  }

  const liveEnv = { ...env, FABRIC_NATIVE_COMPLETION_INTAKE: "live" };
  const other = new NativeRpc(node, cli, root, liveEnv, undefined, "completion-parent.ts");
  const foreign = await observeJson(path.join(root, "completion-main-ready.json"), value => value.pid === other.child.pid);
  expect(foreign.recipient.rootId).not.toBe(ready.recipient.rootId);
  expect(foreign.recipient.sessionId).not.toBe(ready.recipient.sessionId);
  await other.request({ type: "prompt", message: "/native-completion-drain" });
  expect(rows(path.join(root, "completion-enqueued.jsonl"))).toEqual([]);
  expect(fs.existsSync(files.receipt)).toBe(false);
  expect(fs.existsSync(variant === "session-main-kill" ? files.attempt : files.envelope)).toBe(true);
  await other.close();

  const resumed = new NativeRpc(node, cli, root, liveEnv, state.sessionFile, "completion-parent.ts");
  await observeJson(path.join(root, "completion-main-ready.json"), value => value.pid === resumed.child.pid);
  expect((await resumed.request({ type: "get_state" })).sessionId).toBe(state.sessionId);
  const receipt = await observeJson(files.receipt);
  expect(receipt).toMatchObject({ id, sessionId: state.sessionId });
  expect(receipt.consumedAt).toBeGreaterThan(0);
  await observe(files.directory, () => fs.existsSync(files.archive) && !fs.existsSync(files.envelope) ? true : undefined, "completion archive after durable receipt");
  // Await an explicit production pass after delivered()'s async retirement.
  await resumed.request({ type: "prompt", message: "/native-completion-drain" });
  const mesh = new MeshStore(manifest.meshRoot, DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents);
  meshes.push(mesh);
  expect(mesh.get(files.claim, { fresh: true })).toBeUndefined();
  expect(fs.existsSync(files.attempt)).toBe(false);
  const enqueued = rows(path.join(root, "completion-enqueued.jsonl")).filter(row => row.result.id === id);
  expect(enqueued).toHaveLength(1);
  expect(enqueued[0]!.sessionId).toBe(state.sessionId);
  const archived = json<CompletionEnvelope>(files.archive);
  expect(enqueued[0]!.result.status).toBe(archived.result.status);
  expect(["completed", "failed", "stopped", "timed_out"]).toContain(archived.result.status);
  const finalAnswer = json(path.join(run, "final-answer.json"));
  expect(enqueued[0]!.result.finalAnswerReceipt.id).toBe(finalAnswer.id);
  expect(archived.result.text).toBe(finalAnswer.text);
  if (variant !== "session-main-kill") expect(enqueued[0]!.result.status).toBe(json(path.join(run, "manager-result.json")).status);
  else expect(enqueued[0]!.result.status).toBe(json(path.join(run, "status.json")).status); // Orphan worker, no surviving manager publication.
  const order = rows(path.join(root, "completion-order.jsonl")).filter(row => row.receipt.id === id);
  expect(order.length).toBeGreaterThan(0);
  expect(order[0]).toMatchObject({ pid: resumed.child.pid, event: "before-archive", receipt,
    bodySynced: true, directorySynced: true, sourcePresentBeforeArchive: true, claimPresentBeforeArchive: true });
  // Hook observes the actual inode's fsync, its receipt rename, the subsequent
  // directory fsync and the still-unexecuted archive rename in this ONE process.
  // This is an ORDER witness, not an inference from final archive/receipt files.
  expect(order[0]!.bodySyncSequence).toBeGreaterThan(0);
  expect(order[0]!.bodySyncSequence).toBeLessThan(order[0]!.receiptRenameSequence);
  expect(order[0]!.receiptRenameSequence).toBeLessThan(order[0]!.directorySyncSequence);
  expect(order[0]!.directorySyncSequence).toBeLessThan(order[0]!.archiveAttemptSequence);
  const carriers = () => rows(state.sessionFile).filter(row => row.type === "custom_message" &&
    row.customType === "pi-fabric-agent-complete" && row.details?.ids?.includes(id));
  expect(carriers()).toHaveLength(1);
  expect(rows(path.join(root, "completion-delivered.jsonl")).filter(row => row.id === id)).toHaveLength(1);
  await resumed.close();
  const secondRestart = new NativeRpc(node, cli, root, liveEnv, state.sessionFile, "completion-parent.ts");
  await observeJson(path.join(root, "completion-main-ready.json"), value => value.pid === secondRestart.child.pid);
  await secondRestart.request({ type: "prompt", message: "/native-completion-drain" });
  expect(carriers()).toHaveLength(1);
  expect(rows(path.join(root, "completion-enqueued.jsonl")).filter(row => row.result.id === id)).toHaveLength(1);
  expect(rows(path.join(root, "completion-delivered.jsonl")).filter(row => row.id === id)).toHaveLength(1);
  expect(mesh.get(files.claim, { fresh: true })).toBeUndefined();
};

describe("E4 T-B5 — file carrier's unchanged same-root completion custody, native exact-session parent", () => {
  for (const variant of ["durable-close", "durable-main-kill", "durable-host-kill", "session-main-kill"] as const) {
    it(`${variant}: offline producer custody, manifest recipient, foreign Main refusal-by-nonintake, once-only durable native inbox`,
      ctx => parentOfflineCase(ctx, variant), 120_000);
  }
});

describe("E4 remaining NATS variants — explicit coverage gaps, NOT a passing substitute", () => {
  // Durable-close/Main-kill B5 routing: nats-native-boundaries.test.ts.
  for (const testCase of ["B4(i)", "B4(ii)", "B4(iii)", "B5 durable-host-kill", "B5 session-main-kill"]) {
    it(`${testCase} still needs an authored actual-root NATS case`, ctx => unavailable(ctx, NATS_DEPENDENCY));
  }
});
