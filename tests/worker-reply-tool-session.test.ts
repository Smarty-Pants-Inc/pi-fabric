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
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { directiveSchema } from "../src/actors/manager.js";

// review/astra F1 on #85: in full-code and Schema enforce modes Fabric keeps only fabric_exec
// visible, so the run-local fabric_reply tool must survive Fabric's tool ownership and its
// top-level gate. A real Pi session loads the built Fabric extension and the reply hook, and a
// model's fabric_reply call delivers the directive.
const fabricEntry = path.resolve("dist/index.js");
const replyHook = path.resolve("dist/worker/reply-tool.js");
const built = fs.existsSync(fabricEntry) && fs.existsSync(replyHook);

const ENV_KEYS = [
  "PI_FABRIC_REPLY_SCHEMA_FILE", "PI_FABRIC_REPLY_FILE", "PI_FABRIC_REPLY_HOOK", "PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR",
] as const;

describe.skipIf(!built)("fabric_reply in a real Pi session with Fabric", () => {
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

  it.each(["full-code", "schema enforce"] as const)("keeps fabric_reply callable in %s mode and delivers the reply", async (mode) => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-reply-session-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    if (mode === "schema enforce") {
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ schema: { mode: "enforce" } }));
    }
    const schemaFile = path.join(root, "schema.json");
    const replyFile = path.join(root, "reply.json");
    fs.writeFileSync(schemaFile, JSON.stringify(directiveSchema));
    process.env.PI_FABRIC_REPLY_SCHEMA_FILE = schemaFile;
    process.env.PI_FABRIC_REPLY_FILE = replyFile;
    process.env.PI_FABRIC_REPLY_HOOK = fs.realpathSync(replyHook);
    process.env.PI_FABRIC_MESH_ROOT = path.join(root, "mesh");
    process.env.PI_CODING_AGENT_DIR = agentDir;                    // Fabric reads fabric.json from here

    const faux = fauxProvider({ tokensPerSecond: 1_000 });
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      additionalExtensionPaths: [fabricEntry, replyHook],
    });
    await loader.reload();
    const { session } = await createAgentSession({
      cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.inMemory(root),
    });
    sessions.push(session);
    await session.bindExtensions({});                              // session_start: Fabric takes the tools
    const active = session.getActiveToolNames();
    expect(active).toContain("fabric_exec");
    expect(active).toContain("fabric_reply");
    expect(active).not.toContain("bash");                         // Fabric does own the model's tools

    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_reply", { action: "message", message: "Look at #85." }))]);
    await session.prompt("an event");
    expect(JSON.parse(fs.readFileSync(replyFile, "utf8"))).toEqual({ action: "message", message: "Look at #85." });
    const results = session.messages.filter((message) => message.role === "toolResult");
    expect(results.map((message) => (message as { isError?: boolean }).isError)).toEqual([false]);
  }, 60_000);
});
