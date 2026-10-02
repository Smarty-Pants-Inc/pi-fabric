import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FabricState } from "../src/fabric-state.js";
import piFabric from "../src/index.js";
import type { JevRunInfo } from "../src/jev/types.js";
import { SELF_RELOAD_COMMAND } from "../src/lifecycle/self-reload.js";

// review/astra on pi-fabric#160 finding 2: with the mesh off, an idle Escape halts only the Jev
// observers. The self-reload gate must see that halt after the cancelled observer stops counting
// busy, and hold the reload until the user's next input.

type Handler = (event: unknown, context: ExtensionContext) => unknown;
const anyFn = <T extends object>(base: T): T =>
  new Proxy(base, { get: (target, key) => key in target ? target[key as keyof T] : vi.fn() });

describe("self-reload after an idle Escape with the mesh off (pi-fabric#160)", () => {
  const roots: string[] = [];
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it.each([false, true])("mesh=%s: an idle Escape holds the reload through extension input; the user's input lifts it", async (mesh) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-reload-escape-"));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(path.join(root, ".pi"), { recursive: true });
    fs.mkdirSync(agentDir, { recursive: true });
    // Never touch the host's mesh or residency: a Fabric-launched shell exports its own.
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      fullCodeMode: true, selfReloadConcurrency: 0, mcp: { enabled: false, cache: { enabled: false } }, mesh: { enabled: mesh },
      agents: { enabled: false }, memory: { enabled: false }, residency: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
      approvals: { agent: "allow", execute: "allow", read: "allow", network: "deny" },
    }));
    // This checkout is the loaded release; the profile then activates a newer one.
    const settings = path.join(agentDir, "settings.json");
    const activate = (source: string, bump: number): void => {
      fs.writeFileSync(settings, JSON.stringify({ packages: [source], extensions: [codeOld] }));
      const at = new Date(Date.now() + bump);
      fs.utimesSync(settings, at, at);
    };
    const codeOld = path.join(root, "code-old.js");
    const codeNext = path.join(root, "code-next.js");
    fs.writeFileSync(codeOld, "export default () => {};\n");
    fs.writeFileSync(codeNext, "export default () => {};\n");
    activate(process.cwd(), 0);
    const next = path.join(root, "releases", "next");
    fs.mkdirSync(next, { recursive: true });
    fs.writeFileSync(path.join(next, "package.json"), JSON.stringify({ name: "pi-fabric" }));

    let state: FabricState | undefined;
    const bootstrap = FabricState.prototype.bootstrap;
    vi.spyOn(FabricState.prototype, "bootstrap").mockImplementation(function (this: FabricState, context) {
      state = this;
      return bootstrap.call(this, context);
    });

    const handlers = new Map<string, Handler[]>();
    const commands = new Map<string, { handler: (args: string, context: ExtensionContext) => Promise<void> }>();
    const sent: string[] = [];
    const terminalInputs = new Set<(data: string) => unknown>();
    const terminalInput = (data: string): void => { for (const handler of terminalInputs) handler(data); };
    const reload = vi.fn(async () => {});
    const context = anyFn({
      cwd: root, mode: "tui", hasUI: true, signal: undefined,
      isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      isPromptPending: () => false, isSettling: () => false, reload,
      modelRegistry: anyFn({ getAvailable: () => [], getAll: () => [] }),
      sessionManager: anyFn({
        getSessionId: () => "escape-reload", getSessionFile: () => undefined, getBranch: () => [],
        getEntries: () => [], getLeafId: () => undefined, getCwd: () => root,
      }),
      ui: anyFn({
        holdState: undefined, // Explicitly model the old host; the proxy otherwise supplies a function.
        onTerminalInput: (handler: (data: string) => unknown) => { terminalInputs.add(handler); return () => { terminalInputs.delete(handler); }; },
      }),
    }) as unknown as ExtensionContext;
    const bus = new Map<string, Set<(data: unknown) => void>>();
    const pi = anyFn({
      events: {
        emit: (topic: string, data: unknown) => { for (const handler of bus.get(topic) ?? []) handler(data); },
        on: (topic: string, handler: (data: unknown) => void) => {
          const listeners = bus.get(topic) ?? new Set(); listeners.add(handler); bus.set(topic, listeners);
          return () => { listeners.delete(handler); };
        },
      },
      getActiveTools: () => [], getAllTools: () => [], getThinkingLevel: () => "off",
      on: (name: string, handler: Handler) => { handlers.set(name, [...handlers.get(name) ?? [], handler]); },
      registerCommand: (name: string, command: { handler: (args: string, context: ExtensionContext) => Promise<void> }) => {
        commands.set(name, command);
      },
      sendUserMessage: (text: string) => {
        sent.push(text);
        const [name, ...args] = text.slice(1).split(" ");
        void commands.get(name!)?.handler(args.join(" "), context);
      },
    }) as unknown as ExtensionAPI;
    const emit = async (name: string, event: unknown): Promise<void> => {
      for (const handler of handlers.get(name) ?? []) await handler(event, context);
    };

    await piFabric(pi);
    try {
      await emit("session_start", { type: "session_start", reason: "startup" });
      // Exercise the production wiring through the public bus. Even with input checks present,
      // the pinned public UI API cannot establish a global dialog/editor-clear proof.
      const results: unknown[] = [];
      pi.events.on("pi-fabric:reload-target:v1:result", data => { results.push(data); });
      expect(bus.get("pi-fabric:reload-target:v1")?.size).toBeGreaterThan(0);
      pi.events.emit("pi-fabric:reload-target:v1", { requestId: "code-bind", owner: "smarty-code", resource: codeOld,
        loaded: codeOld, configured: codeOld, reason: "startup binding" });
      await vi.waitFor(() => expect(results.at(-1)).toMatchObject({ requestId: "code-bind", accepted: true, reason: "bound-unchanged" }));
      fs.writeFileSync(settings, JSON.stringify({ packages: [process.cwd()], extensions: [codeNext] }));
      pi.events.emit("pi-fabric:reload-target:v1", { requestId: "code-update", owner: "smarty-code", resource: codeOld,
        loaded: codeOld, configured: codeNext, reason: "Code-only install" });
      await vi.waitFor(() => expect(results.at(-1)).toMatchObject({ requestId: "code-update", accepted: false,
        reason: "unsupported-host:global-dialog/editor-hold-query", target: codeNext }));
      expect(sent).toEqual([]); expect(reload).not.toHaveBeenCalled();
      // Restore Code's pointer; the existing Fabric-only Escape test continues unchanged.
      activate(process.cwd(), 1);
      await state!.ensure(context); // first fabric_exec: activation installs the Escape handler
      expect(state!.config.mesh.enabled).toBe(mesh);
      expect(terminalInputs.size).toBeGreaterThan(0);

      const invocation = {
        cwd: root, signal: undefined, parentToolCallId: "observer", nestedToolCallId: "observer",
        extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32_768,
      };
      const observer = await state!.registry.invoke("jev.spawn", {
        input: null, observe: { events: ["turn_end"], delivery: "steer" },
        program: { name: "advisor", code: "while(true) await program.nextEvent();", requires: ["jev.advise"], inputSchema: {}, outputSchema: {} },
      }, invocation) as JevRunInfo;
      const observerDone = state!.registry.invoke("jev.wait", { id: observer.id }, invocation);
      expect(state!.backgroundWorkCount()).toBe(1);

      vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
      activate(next, 60_000);
      // Main's run completes; the observer holds the newer release, so the 5 s retry arms.
      await emit("agent_settled", { type: "agent_settled", outcome: "completed" });
      expect(sent).toEqual([]);

      // Escape while Main is idle, through the same terminal-input handler the TUI calls.
      terminalInput("\x1b");
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(await observerDone).toMatchObject({ state: "cancelled" });
      expect(state!.backgroundWorkCount()).toBe(0);
      expect(state!.actors.halted).toBe(mesh); // mesh off: only the Jev observers were halted

      await vi.advanceTimersByTimeAsync(15_000);
      expect(sent).toEqual([]);
      expect(reload).not.toHaveBeenCalled();

      // An extension's own prompt does not lift the halt; the user's input does, and that run's settle reloads.
      await emit("input", { type: "input", source: "extension", text: "inbox" });
      await emit("agent_settled", { type: "agent_settled", outcome: "completed" });
      expect(sent).toEqual([]);
      expect(state!.actors.halted).toBe(mesh); // an extension prompt does not resume the actors
      expect(state!.escapeHalted).toBe(true);
      await emit("input", { type: "input", source: "interactive", text: "go on" });
      expect(state!.actors.halted).toBe(false);
      expect(state!.escapeHalted).toBe(false);
      await emit("agent_settled", { type: "agent_settled", outcome: "completed" });
      expect(sent).toEqual([`/${SELF_RELOAD_COMMAND} auto`]);
      await vi.waitFor(() => expect(reload).toHaveBeenCalledTimes(1));
    } finally {
      for (const handler of handlers.get("session_shutdown") ?? []) await handler({ type: "session_shutdown" }, context);
    }
  }, 60_000);
});
