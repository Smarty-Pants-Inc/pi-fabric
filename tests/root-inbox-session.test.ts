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
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR", "PI_FABRIC_INBOX_WAKE_MS", "PI_FABRIC_INBOX_WAKE_COOLDOWN_MS"] as const;

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

  // `wake`: the idle wake ticks every 100 ms with no cooldown (smarty-dev#1595); otherwise it
  // keeps its 15 s default and stays out of these short tests.
  const start = async (tokensPerSecond = 1_000, wake = false) => {
    if (wake) {
      process.env.PI_FABRIC_INBOX_WAKE_MS = "100";
      process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS = "0";
    } else {
      delete process.env.PI_FABRIC_INBOX_WAKE_MS;
      delete process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS;
    }
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
    const missedWork = (text: string, to: string | null = `session:${session.sessionManager.getSessionId()}`) => {
      const log = path.join(meshRoot, "events.jsonl");
      const lines = fs.existsSync(log) ? fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean) : [];
      const sequence = (lines.length ? (JSON.parse(lines.at(-1)!) as { sequence: number }).sequence : 0) + 1;
      fs.appendFileSync(log, `${JSON.stringify({
        id: randomUUID(), sequence, topic: "fleet.work.pi-fabric.1", kind: "handoff",
        from: { id: "session:peer", name: "main", kind: "main", sessionId: "peer" },
        ...(to ? { to } : {}),
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

  it("skips a shadow record whose steer the session recorded, and brings one whose steer it did not", async () => {
    const { session, faux, inboxMessages, missedWork } = await start();
    // The steer that carried work key "steered" reached the session, as Main's deliverAgent writes it.
    await session.sendCustomMessage({
      customType: "pi-fabric-agent-message", content: "Steered text.", display: true,
      details: { id: "m1", from: { id: "session:peer", name: "main", kind: "main" }, delivery: "followUp", triggerTurn: false, data: { key: "Steered text." } },
    }, { triggerTurn: false });
    missedWork("Steered text.");
    missedWork("Only published.");
    faux.setResponses([fauxAssistantMessage("noted")]);
    await session.prompt("next");
    expect(inboxMessages()).toHaveLength(1);
    const content = JSON.stringify(inboxMessages()[0]);
    expect(content).toContain("Only published.");
    expect(content).not.toContain("Steered text.");
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

  // smarty-dev#1595: an idle Main reads its inbox on a timer and wakes for it, with the settle's call.
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
  const until = async (done: () => boolean, ms = 10_000) => {
    const deadline = Date.now() + ms;
    while (!done() && Date.now() < deadline) await sleep(50);
  };

  it("wakes an idle Main for an addressed work event with one new turn, and only once", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(1_000, true);
    faux.setResponses([fauxAssistantMessage("woken"), fauxAssistantMessage("extra")]);
    missedWork("Published from a shell while you were idle.");
    await until(() => inboxMessages().length > 0 && !session.isStreaming);
    expect(inboxMessages()).toHaveLength(1);
    expect(JSON.stringify(inboxMessages()[0])).toContain("Published from a shell while you were idle.");
    const last = session.messages.at(-1) as { role?: string; content?: unknown };
    expect(last.role).toBe("assistant");
    expect(JSON.stringify(last.content)).toContain("woken");
    // More ticks: the batch is held, so it never comes again.
    await sleep(1_000);
    expect(inboxMessages()).toHaveLength(1);
  }, 60_000);

  it("never wakes for a broadcast without `to`, or for a shadow copy whose steer the session holds", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(1_000, true);
    faux.setResponses([fauxAssistantMessage("should not run")]);
    await session.sendCustomMessage({
      customType: "pi-fabric-agent-message", content: "Steered text.", display: true,
      details: { id: "m1", from: { id: "session:peer", name: "main", kind: "main" }, delivery: "followUp", triggerTurn: false, data: { key: "Steered text." } },
    }, { triggerTurn: false });
    missedWork("Steered text.");
    missedWork("For everyone.", null);
    await sleep(1_500);
    expect(inboxMessages()).toEqual([]);
    expect(session.isStreaming).toBe(false);
  }, 60_000);

  it("does not wake a busy Main: the event comes after the run settles, in a new run", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(40, true);
    const order: string[] = [];
    session.subscribe((event) => {
      if (event.type === "agent_settled") order.push("settled");
      if (event.type === "message_end" && (event.message as { customType?: string }).customType === "pi-fabric-inbox") order.push("inbox");
    });
    faux.setResponses([
      () => { missedWork("Arrived while you were busy."); return fauxAssistantMessage("a long answer ".repeat(60)); },
      fauxAssistantMessage("got it"),
    ]);
    await session.prompt("work");
    await until(() => inboxMessages().length > 0 && !session.isStreaming);
    expect(inboxMessages()).toHaveLength(1);
    expect(order.slice(0, 2)).toEqual(["settled", "inbox"]);
  }, 60_000);

  it("stays off after a cancelled run until the next turn starts", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(10, true);
    faux.setResponses([
      () => { missedWork("Waiting while you were stopped."); return fauxAssistantMessage("a long answer ".repeat(300)); },
    ]);
    const prompted = session.prompt("work");
    await until(() => session.isStreaming);
    await sleep(200);
    await session.abort();
    await prompted.catch(() => undefined);
    // Many idle ticks after the cancel: none wakes the Main.
    await sleep(1_500);
    expect(session.isStreaming).toBe(false);
    expect(inboxMessages()).toEqual([]);
    faux.setResponses([fauxAssistantMessage("resumed")]);
    await session.prompt("resume");
    expect(inboxMessages()).toHaveLength(1);
    // The wake is on again after that turn: a new event wakes the idle Main.
    faux.setResponses([fauxAssistantMessage("woken again")]);
    missedWork("After the resume.");
    await until(() => inboxMessages().length > 1 && !session.isStreaming);
    expect(inboxMessages()).toHaveLength(2);
  }, 60_000);
});
