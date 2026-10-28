import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import * as connection from "../src/transport/nats/connection.js";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function fixture() {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "firstconnect-runtime-fixture-"));
  const agentDir = path.join(cwd, "agent");
  fs.mkdirSync(agentDir);
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_MESH_HOST_TRANSPORT", "nats");
  const config = normalizeFabricConfig({
    fullCodeMode: false, components: [], prewalk: { enabled: false, alwaysRearm: false },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false },
    memory: { enabled: false }, jev: { enabled: false },
    mesh: { enabled: true, announce: true, transport: "nats", hostTransport: "nats", root: path.join(cwd, "mesh"),
      nats: { servers: "tls://broker.example.invalid:4222", machine: "machine-a", operatorMachine: "fabric-ops",
        credentials: path.join(cwd, "agent.creds"), custodyStream: "CUSTODY", livenessStream: "LIVENESS",
        pathBMachines: ["machine-a"], tls: { caFile: path.join(cwd, "ca.pem") } } },
  });
  fs.mkdirSync(path.join(cwd, ".pi"));
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify(config));
  type Handler = (event: unknown, context: ExtensionContext) => unknown;
  const handlers = new Map<string, Handler[]>();
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    on(name: string, handler: Handler) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); },
    getActiveTools: () => [], getAllTools: () => [], getThinkingLevel: () => "off",
    getSessionName: () => "firstconnect-runtime", appendEntry: vi.fn(),
    registerCommand: vi.fn(), registerShortcut: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
    setActiveTools: vi.fn(), sendMessage: vi.fn(),
  } as unknown as ExtensionAPI;
  let disposed = false;
  const context = new Proxy({
    cwd, hasUI: false, mode: "rpc", isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => "firstconnect-runtime", getSessionFile: () => undefined,
      getBranch: () => [], getEntries: () => [], getLeafId: () => null, getLeafEntry: () => undefined, getEntry: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  }, { get(object, key, receiver) {
    if (disposed) throw new Error("This extension ctx is stale after session replacement or reload.");
    return Reflect.get(object, key, receiver);
  } }) as unknown as ExtensionContext;
  const emit = async (name: string, owner = context) => {
    for (const handler of handlers.get(name) ?? []) await handler({}, owner);
  };
  return { cwd, agentDir, config, context, pi, emit,
    dispose() { disposed = true; } };
}

// Failure-path boundary test: only the native connection wait is held; runtime, transport
// normalization, and extension lifecycle are real. Native delayed-broker tests cover the socket.
function pendingConnection() {
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  let fail!: (error: Error) => void;
  const open = vi.spyOn(connection, "openNatsConnection").mockImplementation(options => new Promise((_, reject) => {
    const onAbort = () => reject(options.signal?.reason);
    fail = error => { options.signal?.removeEventListener("abort", onAbort); reject(error); };
    options.signal?.addEventListener("abort", onAbort, { once: true });
    if (options.signal?.aborted) onAbort();
    entered();
  }));
  return { open, started, fail: (error: Error) => fail(error) };
}

describe("NATS first-connect runtime lifecycle", () => {
  it("propagates pending initialize cancellation and the owning PID readiness path through the real transport", async () => {
    const f = fixture();
    const pending = pendingConnection();
    const runtime = new FabricRuntimeState(f.pi, new CapturedToolCatalog());
    cleanup.push(() => runtime.shutdown());
    const abort = new AbortController();
    const activation = runtime.initialize(f.context, f.config, { signal: abort.signal }).catch(error => error);
    try {
      await pending.started;
      const options = pending.open.mock.calls[0]![0];
      expect(options.readyFile).toBe(path.join(f.agentDir, "fabric", `nats-ready-${process.pid}`));
      expect(fs.existsSync(options.readyFile!)).toBe(false);
      abort.abort(new Error("session replaced during first connect"));
      expect(options.signal?.aborted).toBe(true);
      expect(await activation).toMatchObject({ message: "session replaced during first connect" });
      expect(runtime.initialized).toBe(false);
    } finally {
      pending.fail(new Error("fixture cleanup"));
      await activation;
    }
  });

  it("returns ordinary session_start while native connect is pending, visibly not connected, and reports failure", async () => {
    const f = fixture();
    const pending = pendingConnection();
    const { createFabricExtension } = await import("../src/extension.js");
    await createFabricExtension(import.meta.url)(f.pi);
    cleanup.push(() => f.emit("session_shutdown"));
    let settled = false;
    const startup = f.emit("session_start").then(() => { settled = true; });
    void startup.catch(() => undefined);
    try {
      await pending.started;
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(settled).toBe(true);
      expect(f.context.ui.setStatus).toHaveBeenCalledWith("fabric-nats", expect.stringContaining("NOT CONNECTED"));
      pending.fail(new Error("broker authentication failed"));
      await startup;
      await vi.waitFor(() => expect(f.context.ui.setStatus).toHaveBeenCalledWith("fabric-nats", expect.stringContaining("broker authentication failed")));
    } finally {
      pending.fail(new Error("fixture cleanup"));
      await startup.catch(() => undefined);
    }
  });

  it("cancels ordinary background activation through session_shutdown without a stale failure status", async () => {
    const f = fixture();
    const pending = pendingConnection();
    const { createFabricExtension } = await import("../src/extension.js");
    await createFabricExtension(import.meta.url)(f.pi);
    cleanup.push(() => f.emit("session_shutdown"));
    const startup = f.emit("session_start");
    void startup.catch(() => undefined);
    try {
      await pending.started;
      await startup;
      const options = pending.open.mock.calls[0]![0];
      expect(options.signal?.aborted).toBe(false);
      await f.emit("session_shutdown");
      expect(options.signal?.aborted).toBe(true);
      const statuses = vi.mocked(f.context.ui.setStatus).mock.calls.filter(([key]) => key === "fabric-nats");
      expect(statuses.at(-1)?.[1]).not.toContain("superseded");
      expect(fs.existsSync(options.readyFile!)).toBe(false);
    } finally {
      pending.fail(new Error("fixture cleanup"));
      await startup.catch(() => undefined);
    }
  });

  it("retires a disposed pending owner without leaking its cancellation status into a live successor", async () => {
    const f = fixture();
    const pending = pendingConnection();
    const predecessorStatus = vi.mocked(f.context.ui.setStatus);
    const successorStatus = vi.fn();
    const successor = { ...f.context,
      sessionManager: { ...f.context.sessionManager, getSessionId: () => "firstconnect-successor" },
      ui: { ...f.context.ui, setStatus: successorStatus },
    } as unknown as ExtensionContext;
    const { createFabricExtension } = await import("../src/extension.js");
    await createFabricExtension(import.meta.url)(f.pi);
    cleanup.push(() => f.emit("session_shutdown", successor));
    try {
      await f.emit("session_start");
      await pending.started;
      const owner = pending.open.mock.calls[0]![0].signal!;
      const statusesBeforeDisposal = [...predecessorStatus.mock.calls];
      expect(owner.aborted).toBe(false);
      f.dispose(); // No shutdown, replacement, or other event supplies cancellation.
      await vi.waitFor(() => expect(owner.aborted).toBe(true), { timeout: 1_500 });
      expect(predecessorStatus.mock.calls).toEqual(statusesBeforeDisposal);

      await f.emit("session_start", successor);
      await vi.waitFor(() => expect(pending.open).toHaveBeenCalledTimes(2));
      const successorOwner = pending.open.mock.calls[1]![0].signal!;
      expect(successorOwner).not.toBe(owner);
      expect(successorOwner.aborted).toBe(false);
      await new Promise(resolve => setTimeout(resolve, 600));
      expect(successorOwner.aborted).toBe(false);
      expect(successorStatus).toHaveBeenCalledWith("fabric-nats", expect.stringContaining("NOT CONNECTED"));
      expect(successorStatus.mock.calls.some(([, value]) => /disposed|superseded/.test(String(value)))).toBe(false);
      expect(predecessorStatus.mock.calls).toEqual(statusesBeforeDisposal);
      await f.emit("session_shutdown", successor);
      expect(successorOwner.aborted).toBe(true);
    } finally {
      pending.fail(new Error("fixture cleanup"));
    }
  });

  it("keeps resident startup probes synchronous and never acknowledges a failed activation", async () => {
    const f = fixture();
    vi.stubEnv("PI_FABRIC_RESIDENT_PROBE_NONCE", "firstconnect-test");
    vi.stubEnv("PI_FABRIC_RESIDENT_PROBE_WORKER_PID", String(process.ppid));
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "firstconnect-test-run");
    const pending = pendingConnection();
    const { createFabricExtension } = await import("../src/extension.js");
    await createFabricExtension(import.meta.url)(f.pi);
    cleanup.push(() => f.emit("session_shutdown"));
    let settled = false;
    const startup = f.emit("session_start").catch(error => { settled = true; return error; });
    try {
      await pending.started;
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(settled).toBe(false);
      pending.fail(new Error("probe activation failed"));
      expect(await startup).toMatchObject({ message: "probe activation failed" });
    } finally {
      pending.fail(new Error("fixture cleanup"));
      await startup;
    }
  });
});
