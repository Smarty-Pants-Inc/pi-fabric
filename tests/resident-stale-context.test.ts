import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { fauxProvider } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Resident reload probe timed out");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

it("reloads a real resident Pi host while a process-backed browser call is in flight without touching its retired ctx (#5962)", async () => {
  installInProcessResidentFence();
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "resident-stale-ctx-")));
  const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
  const manager = SessionManager.inMemory(root);
  const residentRoot = path.join(root, "resident");
  fs.mkdirSync(residentRoot, { recursive: true });
  const configPath = path.join(residentRoot, "config.json");
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: `session:${manager.getSessionId()}`, sessionId: manager.getSessionId(),
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: residentRoot,
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("src/index.ts"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.writeFileSync(configPath, JSON.stringify(config));
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
    mesh: { enabled: true }, memory: { enabled: false }, jev: { enabled: false }, prewalk: { alwaysRearm: false },
    fullCodeMode: true, approvals: { execute: "allow" },
  }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", config.meshRoot);
  vi.stubEnv("PI_FABRIC_RESIDENT_CONFIG", configPath);
  vi.resetModules();
  const { default: residentEntry } = await import("../src/residency/pi-entry.js");
  const states: FabricState[] = [];
  const ensure = FabricState.prototype.ensure;
  vi.spyOn(FabricState.prototype, "ensure").mockImplementation(async function (this: FabricState, ctx) {
    if (!states.includes(this)) states.push(this);
    return ensure.call(this, ctx);
  });
  const children: Array<{ process: ChildProcess; closed: Promise<unknown> }> = [];
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  let calls = 0;
  let completed = 0;
  const faux = fauxProvider();
  const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "unused-auth.json") });
  modelRuntime.registerNativeProvider(faux.provider);
  const loader = new DefaultResourceLoader({
    cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    extensionFactories: [
      { name: "resident-host", factory: residentEntry },
      { name: "fabric", factory: piFabric },
      { name: "browser-process-bridge", factory: pi => pi.registerTool({
        name: "browser_child", label: "Browser child", description: "Offline process bridge", parameters: Type.Object({}),
        async execute(_id, _args, _signal, _update, ctx) {
          // The fixture deliberately ignores abort, like a late external browser reply.
          // It uses the host context only at dispatch, never after an await.
          expect(ctx.sessionManager.getSessionId()).toBe(manager.getSessionId());
          calls++;
          if (calls > 1) return { content: [{ type: "text", text: "fresh child" }], details: {} };
          const child = spawn(process.execPath, ["-e", "process.send('ready'); process.once('message', () => { process.send('result'); process.disconnect(); });"],
            { stdio: ["ignore", "ignore", "pipe", "ipc"] });
          const closed = once(child, "close"); children.push({ process: child, closed });
          await once(child, "message");
          const reply = once(child, "message"); entered();
          await reply; await closed; completed++;
          return { content: [{ type: "text", text: "late child" }], details: {} };
        },
      }) },
    ],
  });
  let session: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
  let pending: Promise<unknown> | undefined;
  const shutdown = vi.fn();
  const errors: unknown[] = [];
  const signalListeners = new Map(["SIGTERM", "SIGINT"].map(name => [name, new Set(process.listeners(name))]));
  try {
    await loader.reload();
    ({ session } = await createAgentSession({ cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader, sessionManager: manager }));
    await session.bindExtensions({ shutdownHandler: shutdown, onError: error => errors.push(error) });
    const ownerFile = path.join(residentRoot, "owner.json");
    await waitFor(() => {
      const error = path.join(residentRoot, "error.json");
      if (fs.existsSync(error)) throw new Error(fs.readFileSync(error, "utf8"));
      if (shutdown.mock.calls.length) throw new Error(`Resident stopped before ready: ${errors.map(String)}`);
      return fs.existsSync(ownerFile);
    });
    const oldOwner = JSON.parse(fs.readFileSync(ownerFile, "utf8")).token;
    await session.extensionRunner!.getToolDefinition("fabric_exec")!.execute("activate", { code: "return 1" }, undefined, undefined, session.extensionRunner!.createContext());
    const before = states[0]!;
    const retired = session.extensionRunner!;
    const getActiveTools = retired.getActiveTools.bind(retired);
    let staleReads = 0;
    vi.spyOn(retired, "getActiveTools").mockImplementation(() => {
      try { return getActiveTools(); } catch (error) { staleReads++; throw error; }
    });
    const invocation = { cwd: root, signal: undefined, parentToolCallId: "child", nestedToolCallId: "child", extensionContext: retired.createContext(),
      update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000 };
    pending = before.registry.invoke("extensions.browser_child", {}, invocation).then(value => ({ value }), error => ({ error }));
    await started;
    await session.reload();
    await waitFor(() => fs.existsSync(ownerFile) && JSON.parse(fs.readFileSync(ownerFile, "utf8")).token !== oldOwner);
    expect(shutdown).not.toHaveBeenCalled(); // Reload itself owns teardown, not the retired resident callback.
    expect(await pending).toMatchObject({ error: { message: expect.stringMatching(/closed|abort/i) } });
    children[0]!.process.send("release");
    await children[0]!.closed;
    await waitFor(() => completed === 1);
    await new Promise(resolve => setImmediate(resolve));
    expect(staleReads).toBe(0);
    await session.extensionRunner!.getToolDefinition("fabric_exec")!.execute("fresh", { code: "return 1" }, undefined, undefined, session.extensionRunner!.createContext());
    const fresh = states.at(-1)!;
    await expect(fresh.registry.invoke("extensions.browser_child", {}, { ...invocation, extensionContext: session.extensionRunner!.createContext() }))
      .resolves.toMatchObject({ text: "fresh child", isError: false });
    expect(calls).toBe(2); // No replay of the already-dispatched child call.
    expect(errors).toEqual([]);
  } finally {
    for (const child of children) { if (child.process.connected) child.process.send("release"); }
    await Promise.all(children.map(child => child.closed));
    await pending;
    if (session) { await session.extensionRunner!.emit({ type: "session_shutdown", reason: "quit" }); session.dispose(); }
    for (const [name, original] of signalListeners) for (const listener of process.listeners(name)) {
      if (!original.has(listener)) process.removeListener(name, listener);
    }
    vi.restoreAllMocks(); vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
}, 60_000);
