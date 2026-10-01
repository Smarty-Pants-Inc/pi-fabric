import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  type AgentSession,
  type ExtensionContext,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";

// P1 2026-09-27 (smarty-dev#1195): a /reload that landed mid-run crashed dev-lead's Pi. A
// fabric_exec call during Fabric's session_shutdown built a runtime that nothing shut down.
// After the reload, that runtime's component watcher read the stale ctx and threw uncaught.
const fabricEntry = path.resolve("dist/index.js");
const built = fs.existsSync(fabricEntry);
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR"] as const;

describe.skipIf(!built)("Fabric across /reload in a real Pi session", () => {
  const roots: string[] = [];
  const sessions: AgentSession[] = [];
  const saved = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
  afterEach(async () => {
    for (const session of sessions.splice(0)) session.dispose();
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it("refuses a tool call during reload's shutdown and leaves no runtime on the stale ctx", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-reload-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    const config = path.join(agentDir, "fabric.json");
    fs.writeFileSync(config, JSON.stringify({}));
    process.env.PI_FABRIC_MESH_ROOT = path.join(root, "mesh");
    process.env.PI_CODING_AGENT_DIR = agentDir;

    const faults: unknown[] = [];
    const onFault = (error: unknown) => { faults.push(error); };
    process.on("uncaughtException", onFault);
    process.on("unhandledRejection", onFault);
    try {
      // Loaded after Fabric, so its session_shutdown handler runs after Fabric's and before Pi
      // invalidates the ctx: the window in which a mid-run tool call reached the old Fabric.
      let oldExec: ToolDefinition | undefined;
      let lateCall: Promise<string> | undefined;
      const faux = fauxProvider({ tokensPerSecond: 1_000 });
      const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
      modelRuntime.registerNativeProvider(faux.provider);
      const loader = new DefaultResourceLoader({
        cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
        additionalExtensionPaths: [fabricEntry],
        extensionFactories: [{
          name: "tool-call-during-shutdown",
          factory: (pi) => {
            pi.on("session_shutdown", async (_event, ctx: ExtensionContext) => {
              const exec = oldExec;
              oldExec = undefined;
              if (!exec) return;
              lateCall = Promise.resolve()
                .then(() => exec.execute("late", { code: "return 1" } as never, undefined, undefined, ctx as Parameters<typeof exec.execute>[4]))
                .then(() => "completed", (error: unknown) => String(error));
              await lateCall;
            });
          },
        }],
      });
      await loader.reload();
      const { session } = await createAgentSession({
        cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader,
        sessionManager: SessionManager.inMemory(root),
      });
      sessions.push(session);
      await session.bindExtensions({});
      faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("done")]);
      await session.prompt("activate");

      oldExec = session.extensionRunner!.getToolDefinition("fabric_exec");
      expect(oldExec).toBeDefined();
      await session.reload();
      expect(await lateCall).toMatch(/shut down/);

      // The component watcher polls every 250 ms; a runtime left on the old ctx would see this.
      fs.writeFileSync(config, JSON.stringify({ components: [{ id: "missing-component", enabled: true }] }));
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      expect(faults.map(String)).toEqual([]);
    } finally {
      process.off("uncaughtException", onFault);
      process.off("unhandledRejection", onFault);
    }
  }, 60_000);
});
