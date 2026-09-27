import { randomUUID } from "node:crypto";
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

// smarty-dev#754 §3.2 step 3 in a real Pi session with the built Fabric: a work event addressed to
// this Main that no steer delivered reaches it with its next turn, once.
const fabricEntry = path.resolve("dist/index.js");
const built = fs.existsSync(fabricEntry);
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR"] as const;

describe.skipIf(!built)("the root inbox in a real Pi session", () => {
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

  const start = async (tokensPerSecond = 1_000) => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-inbox-session-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({}));
    const meshRoot = path.join(root, "mesh");
    process.env.PI_FABRIC_MESH_ROOT = meshRoot;
    process.env.PI_CODING_AGENT_DIR = agentDir;
    const faux = fauxProvider({ tokensPerSecond });
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      additionalExtensionPaths: [fabricEntry],
    });
    await loader.reload();
    const { session } = await createAgentSession({
      cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.inMemory(root),
    });
    sessions.push(session);
    await session.bindExtensions({});
    const inboxMessages = () => session.messages.filter((message) =>
      message.role === "custom" && (message as { customType?: string }).customType === "pi-fabric-inbox");
    // A peer's shadow record from two minutes ago, whose steer never arrived.
    const missedWork = (text: string) => {
      const log = path.join(meshRoot, "events.jsonl");
      const lines = fs.existsSync(log) ? fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean) : [];
      const sequence = (lines.length ? (JSON.parse(lines.at(-1)!) as { sequence: number }).sequence : 0) + 1;
      fs.appendFileSync(log, `${JSON.stringify({
        id: randomUUID(), sequence, topic: "fleet.work.pi-fabric.1", kind: "handoff",
        from: { id: "session:peer", name: "main", kind: "main", sessionId: "peer" },
        to: `session:${session.sessionManager.getSessionId()}`,
        text, data: { ref: "Smarty-Pants-Inc/pi-fabric#1", key: text }, createdAt: Date.now() - 120_000,
      })}\n`);
      fs.writeFileSync(path.join(meshRoot, "sequence"), String(sequence));
    };
    // The first turn activates Fabric; its settle starts the inbox at the present.
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("ready")]);
    await session.prompt("start");
    expect(inboxMessages()).toEqual([]);
    return { session, faux, inboxMessages, missedWork };
  };

  it("brings a missed work event to the next turn, and only once", async () => {
    const { session, faux, inboxMessages, missedWork } = await start();
    missedWork("Review pi-fabric#1 when you can.");
    faux.setResponses([fauxAssistantMessage("noted")]);
    await session.prompt("next");
    expect(inboxMessages()).toHaveLength(1);
    expect(JSON.stringify(inboxMessages()[0])).toContain("Review pi-fabric#1 when you can.");
    faux.setResponses([fauxAssistantMessage("again")]);
    await session.prompt("and again");
    expect(inboxMessages()).toHaveLength(1);
  }, 60_000);

  // review F3: stopping the Main must not start it again.
  it("starts no turn when an aborted run settles, and brings the event with the next turn", async () => {
    const slow = await start(10);
    const { session, faux, inboxMessages, missedWork } = slow;
    faux.setResponses([
      () => { missedWork("Waiting while you were stopped."); return fauxAssistantMessage("a long answer ".repeat(300)); },
    ]);
    const prompted = session.prompt("work");
    const deadline = Date.now() + 10_000;
    while (!session.isStreaming && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 20));
    await new Promise((resolve) => setTimeout(resolve, 200));
    await session.abort();
    await prompted.catch(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    expect(session.isStreaming).toBe(false);
    expect(inboxMessages()).toEqual([]);
    faux.setResponses([fauxAssistantMessage("resumed")]);
    await session.prompt("resume");
    expect(inboxMessages()).toHaveLength(1);
    expect(JSON.stringify(inboxMessages()[0])).toContain("Waiting while you were stopped.");
  }, 60_000);

  it("starts a turn for a work event that arrived during a run, when the run settles", async () => {
    const { session, faux, inboxMessages, missedWork } = await start();
    faux.setResponses([
      () => { missedWork("Arrived while you worked."); return fauxAssistantMessage("working"); },
      fauxAssistantMessage("got it"),
    ]);
    await session.prompt("work");
    const deadline = Date.now() + 10_000;
    while ((inboxMessages().length === 0 || session.isStreaming) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    expect(inboxMessages()).toHaveLength(1);
    expect(JSON.stringify(inboxMessages()[0])).toContain("Arrived while you worked.");
  }, 60_000);
});
