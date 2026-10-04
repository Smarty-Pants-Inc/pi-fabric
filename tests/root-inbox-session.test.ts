import { createHash, randomUUID } from "node:crypto";
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
const HOST_CAPABILITIES_KEY = Symbol.for("pi-fabric.test.hostCapabilities");
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR", "PI_FABRIC_INBOX_WAKE_MS", "PI_FABRIC_INBOX_WAKE_COOLDOWN_MS"] as const;

describe.skipIf(!built)("the root inbox in a real Pi session", () => {
  const roots: string[] = [];
  const sessions: AgentSession[] = [];
  const saved = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
  afterEach(async () => {
    for (const session of sessions.splice(0)) {
      // dispose() invalidates host contexts; it does not emit the extension shutdown
      // hook. Close Fabric's observers and delayed compiles before invalidating them.
      await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    }
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
    delete (globalThis as Record<symbol, unknown>)[HOST_CAPABILITIES_KEY];
  });

  // `wake`: the idle wake ticks every 100 ms with no cooldown (smarty-dev#1595); otherwise it
  // keeps its 15 s default and stays out of these short tests.
  const start = async (tokensPerSecond = 1_000, wake = false, extra: { persisted?: boolean; extensions?: (root: string) => string[]; warm?: boolean; wakeMs?: string; config?: unknown; optIn?: boolean } = {}) => {
    if (wake) {
      process.env.PI_FABRIC_INBOX_WAKE_MS = extra.wakeMs ?? "100";
      process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS = "0";
    } else {
      delete process.env.PI_FABRIC_INBOX_WAKE_MS;
      delete process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS;
    }
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-inbox-session-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    // The idle wake needs a Pi that queues a triggered message behind a live preflight. This Pi
    // predates that capability, so a test injects it; `optIn: false` leaves it absent.
    const capabilities = globalThis as Record<symbol, unknown>;
    if (wake && extra.optIn !== false) capabilities[HOST_CAPABILITIES_KEY] = { triggeredMessageQueuesBehindPreflight: true, promptPendingVisible: true };
    else delete capabilities[HOST_CAPABILITIES_KEY];
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify(extra.config ?? {}));
    const meshRoot = path.join(root, "mesh");
    process.env.PI_FABRIC_MESH_ROOT = meshRoot;
    process.env.PI_CODING_AGENT_DIR = agentDir;
    const faux = fauxProvider({ tokensPerSecond });
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      additionalExtensionPaths: [fabricEntry, ...(extra.extensions?.(root) ?? [])],
    });
    await loader.reload();
    const { session } = await createAgentSession({
      cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: extra.persisted ? SessionManager.create(root, path.join(root, "sessions")) : SessionManager.inMemory(root),
    });
    sessions.push(session);
    await session.bindExtensions({});
    const inboxMessages = () => session.messages.filter((message) =>
      message.role === "custom" && (message as { customType?: string }).customType === "pi-fabric-inbox");
    // A peer's shadow record from two minutes ago, whose steer never arrived.
    const missedWork = (text: string, to: string | null = `session:${session.sessionManager.getSessionId()}`, ageMs = 120_000) => {
      const id = randomUUID();
      const log = path.join(meshRoot, "events.jsonl");
      const lines = fs.existsSync(log) ? fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean) : [];
      const sequence = (lines.length ? (JSON.parse(lines.at(-1)!) as { sequence: number }).sequence : 0) + 1;
      fs.appendFileSync(log, `${JSON.stringify({
        id, sequence, topic: "fleet.work.pi-fabric.1", kind: "handoff",
        from: { id: "session:peer", name: "main", kind: "main", sessionId: "peer" },
        ...(to ? { to } : {}),
        text, data: { ref: "Smarty-Pants-Inc/pi-fabric#1", key: text }, createdAt: Date.now() - ageMs,
      })}\n`);
      fs.writeFileSync(path.join(meshRoot, "sequence"), String(sequence));
      return id;
    };
    // The first turn activates Fabric; its settle starts the inbox at the present.
    if (extra.warm === false) return { session, faux, inboxMessages, missedWork, root, meshRoot, loader, modelRuntime };
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("ready")]);
    await session.prompt("start");
    expect(inboxMessages()).toEqual([]);
    return { session, faux, inboxMessages, missedWork, root, meshRoot, loader, modelRuntime };
  };

  it("does not cache a failed native session append before its shadow, then delivers once after reload (F5)", async () => {
    const h = await start(1_000, false, { persisted: true });
    const { session, faux, root, meshRoot, loader, modelRuntime } = h;
    const file = session.sessionManager.getSessionFile()!;
    const backup = `${file}.backup`;
    const { MeshStore } = await import("../src/mesh/store.js");
    const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const key = "topology/inbox/" + createHash("sha256").update(`session:${session.sessionManager.getSessionId()}`).digest("hex").slice(0, 32);
    const receipts = () => (mesh.get(key, { fresh: true })?.value as { delivered?: unknown[] } | undefined)?.delivered ?? [];
    const before = receipts();
    fs.renameSync(file, backup);
    fs.mkdirSync(file); // Real Pi indexes the entry before appendFileSync fails with EISDIR.
    try {
      await expect(session.sendCustomMessage({
        customType: "pi-fabric-agent-message", content: "native write failed", display: true,
        details: { id: "failed-native", from: { id: "session:peer" }, data: { key: "retry unseen work" } },
      }, { triggerTurn: false })).rejects.toThrow();
      expect(session.sessionManager.getEntries().some(entry => entry.type === "custom_message" &&
        entry.customType === "pi-fabric-agent-message")).toBe(true);
    } finally {
      fs.rmSync(file, { recursive: true });
      fs.renameSync(backup, file);
    }
    // Drain at turn start before the shadow exists. In-memory failed entries are not receipts.
    faux.setResponses([fauxAssistantMessage("drained before shadow")]);
    await session.prompt("drain before shadow");
    expect(receipts()).toEqual(before);
    expect(fs.readFileSync(file, "utf8")).not.toContain("failed-native");
    h.missedWork("retry unseen work");
    await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
    session.dispose();
    sessions.splice(sessions.indexOf(session), 1);
    await loader.reload();
    const { session: reloaded } = await createAgentSession({
      cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.open(file),
    });
    sessions.push(reloaded);
    await reloaded.bindExtensions({});
    // Reopened Fabric is lazy until its first real tool call, as at initial startup.
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })),
      fauxAssistantMessage("recovered work"), fauxAssistantMessage("noted recovery")]);
    await reloaded.prompt("retry");
    const inbox = () => reloaded.sessionManager.getEntries().filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-inbox");
    expect(inbox()).toHaveLength(1);
    expect(JSON.stringify(inbox()[0])).toContain("retry unseen work");
    expect(receipts().length).toBeGreaterThan(before.length);
    h.missedWork("retry unseen work"); // A confirmed inbox receipt suppresses the new shadow id.
    faux.setResponses([fauxAssistantMessage("no duplicate")]);
    await reloaded.prompt("redelivery");
    expect(inbox()).toHaveLength(1);
  }, 60_000);

  it("summarises a 20-hour backlog once at turn start without injecting work or chaining turns (#3036)", async () => {
    const { session, faux, inboxMessages, missedWork } = await start();
    for (let index = 0; index < 45; index++) missedWork(`stale shadow ${index}`, undefined, 20 * 60 * 60_000);
    let inferences = 0;
    faux.setResponses([() => { inferences++; return fauxAssistantMessage("next"); },
      () => { inferences++; return fauxAssistantMessage("unwanted backlog wake"); }]);
    await session.prompt("next");
    expect(inboxMessages()).toEqual([]);
    expect(inferences).toBe(1);
    const summaries = () => session.messages.filter(message => message.role === "custom" &&
      (message as { customType?: string }).customType === "pi-fabric-inbox-summary");
    expect(summaries()).toHaveLength(1);
    expect((summaries()[0] as { content: string }).content).toBe("Fabric inbox: skipped 45 addressed shadows older than 7200000 ms; no stale work injected.");
    faux.setResponses([fauxAssistantMessage("again")]);
    await session.prompt("again");
    expect(summaries()).toHaveLength(1);
    expect(inboxMessages()).toEqual([]);
  }, 60_000);

  it("summarises stale-only idle work without starting a model turn (#3036)", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(1_000, true);
    let inferences = 0;
    faux.setResponses([() => { inferences++; return fauxAssistantMessage("must not wake"); }]);
    for (let index = 0; index < 45; index++) missedWork(`idle stale shadow ${index}`, undefined, 20 * 60 * 60_000);
    const summaries = () => session.sessionManager.getEntries().filter(entry => entry.type === "custom_message" &&
      entry.customType === "pi-fabric-inbox-summary");
    const deadline = Date.now() + 10_000;
    while (summaries().length === 0 && inferences === 0 && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 50));
    expect(inferences).toBe(0);
    expect(summaries()).toHaveLength(1);
    expect(inboxMessages()).toEqual([]);
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(summaries()).toHaveLength(1);
    expect(inferences).toBe(0);
    expect(session.isStreaming).toBe(false);
  }, 60_000);

  it("brings a missed work event to the next turn, and only once", async () => {
    const { session, faux, inboxMessages, missedWork } = await start();
    missedWork("Review pi-fabric#1 when you can.");
    let inferences = 0;
    faux.setResponses([() => {
      inferences++;
      // Hook-enqueued nextTurn delivery must join this first inference, not a later wake.
      expect(inboxMessages()).toHaveLength(1);
      expect(JSON.stringify(inboxMessages()[0])).toContain("Review pi-fabric#1 when you can.");
      return fauxAssistantMessage("noted");
    }, () => { inferences++; return fauxAssistantMessage("unexpected extra wake"); }]);
    await session.prompt("next");
    expect(inferences).toBe(1);
    expect(inboxMessages()).toHaveLength(1);
    expect(JSON.stringify(inboxMessages()[0])).toContain("Review pi-fabric#1 when you can.");
    faux.setResponses([fauxAssistantMessage("again")]);
    await session.prompt("and again");
    expect(inboxMessages()).toHaveLength(1);
  }, 60_000);

  it("turn start never re-inserts an aggregate after split inbox receipts", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(1_000, false, { warm: false });
    const identityId = `session:${session.sessionManager.getSessionId()}`;
    const { createHash } = await import("node:crypto");
    const { MeshStore } = await import("../src/mesh/store.js");
    const mesh = new MeshStore(process.env.PI_FABRIC_MESH_ROOT!, 64 * 1024, 100);
    const ids = [missedWork("sender one"), missedWork("sender two")];
    const key = "topology/inbox/" + createHash("sha256").update(identityId).digest("hex").slice(0, 32);
    const previous = mesh.get(key);
    const after = (previous?.value as { after?: number } | undefined)?.after ?? 0;
    await mesh.put({ key, identity: { id: identityId, name: "main", kind: "main" }, value: {
      after, pending: { through: mesh.latestSequence(), ids },
    } });
    for (const id of ids) await session.sendCustomMessage({
      customType: "pi-fabric-inbox", content: `recorded ${id}`, display: true, details: { ids: [id] },
    }, { triggerTurn: false });
    expect(inboxMessages()).toHaveLength(2);
    // Load the persisted pending batch into Fabric's real inbox. An unsuccessful warm
    // turn must not reconcile at settle; the next before_agent_start owns the receipt.
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })),
      { ...fauxAssistantMessage("warm failed"), stopReason: "error", errorMessage: "test warm failure" }]);
    await session.prompt("warm");
    expect((mesh.get(key, { fresh: true })!.value as { pending?: unknown }).pending).toBeDefined();
    expect(inboxMessages()).toHaveLength(2);
    faux.setResponses([fauxAssistantMessage("noted")]);
    await session.prompt("next");
    expect(inboxMessages()).toHaveLength(2);
    expect((mesh.get(key, { fresh: true })!.value as { pending?: unknown }).pending).toBeUndefined();
    faux.setResponses([fauxAssistantMessage("still noted")]);
    await session.prompt("again");
    expect(inboxMessages()).toHaveLength(2);
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

  it.each(["during-error", "after-error"] as const)("wakes once for mailbox work queued %s, not for the stream error alone (#4012)", async (timing) => {
    const { session, faux, inboxMessages, missedWork } = await start(1_000, true);
    session.setAutoRetryEnabled(false);
    const calls = faux.state.callCount;
    faux.setResponses([
      () => {
        if (timing === "during-error") missedWork("Follow up after stream disconnect.");
        return fauxAssistantMessage("partial answer", { stopReason: "error",
          errorMessage: "stream disconnected before completion: stream closed before response.completed" });
      },
      fauxAssistantMessage("mailbox followUp processed"),
    ]);
    await session.prompt("fail the stream");
    expect(session.isStreaming).toBe(false);
    if (timing === "after-error") {
      await sleep(300); // An error with no new work must not retry itself.
      expect(faux.state.callCount).toBe(calls + 1);
      missedWork("Follow up after stream disconnect.");
    }
    await until(() => inboxMessages().length > 0 && !session.isStreaming, 2_000);
    expect(inboxMessages()).toHaveLength(1);
    expect(faux.state.callCount).toBe(calls + 2);
    expect(JSON.stringify(session.messages.at(-1))).toContain("mailbox followUp processed");
    await sleep(300);
    expect(inboxMessages()).toHaveLength(1);
    expect(faux.state.callCount).toBe(calls + 2);
  }, 60_000);

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

  // A prompt's preflight (#107 review F2, #111 review) is the host's to report: the timer waits
  // while ctx.isPromptPending() is true. That needs a Pi with pi#74, so its regression runs
  // through the real CLIs (the PR's held-wake driver), not this older Pi.

  // Review F3: a fresh Main that announces itself at startup and has not had a turn yet: its inbox
  // starts when it becomes available, so an event sent before the first tick still wakes it.
  it("wakes a fresh Main, with no turn yet, for an event sent before its first tick", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(1_000, true, { warm: false, wakeMs: "1500", config: { mesh: { announce: true } } });
    faux.setResponses([fauxAssistantMessage("woken fresh")]);
    missedWork("Sent right after you started.");
    await until(() => inboxMessages().length > 0 && !session.isStreaming, 15_000);
    expect(inboxMessages()).toHaveLength(1);
    expect(JSON.stringify(inboxMessages()[0])).toContain("Sent right after you started.");
  }, 60_000);

  it("is inert on a Pi without the preflight capability: an idle Main takes the event at its next turn", async () => {
    const { session, faux, inboxMessages, missedWork } = await start(1_000, true, { optIn: false });
    faux.setResponses([fauxAssistantMessage("should not run"), fauxAssistantMessage("next turn")]);
    missedWork("Waiting for your next turn.");
    await sleep(3_000);
    expect(inboxMessages()).toEqual([]);
    expect(session.isStreaming).toBe(false);
    faux.setResponses([fauxAssistantMessage("next turn")]);
    await session.prompt("next");
    expect(inboxMessages()).toHaveLength(1);
  }, 60_000);
});

