import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  type AgentSession,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { MainAgentController } from "../src/main-agent.js";

// dev-lead's pass 4 on pi-fabric#102 (aa83731): delivered ids followed the active branch, so an
// entry /tree abandoned, or one on an unflushed or in-memory branch, was missed. These are its
// probes (.probe/branch-growth*, set-edge-probes) with the intended contract as the assertion.
const roots: string[] = [];
const sessions: AgentSession[] = [];
afterEach(() => {
  for (const session of sessions.splice(0)) session.dispose();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-branches-"));
  roots.push(root);
  return root;
};
const from = { id: "peer", name: "peer", kind: "main" as const };
const copies = (manager: SessionManager, id: string) => manager.getEntries().filter((entry) => {
  const details = (entry as { details?: { id?: string; items?: Array<{ id?: string }> } }).details;
  return entry.type === "custom_message" && (details?.id === id || details?.items?.some((item) => item.id === id));
}).length;
const retained = (journal: string) => fs.existsSync(journal) ? (JSON.parse(fs.readFileSync(journal, "utf8")) as { items: unknown[] }).items.length : 0;
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

describe("Main followUp delivered ids on every branch, in real Pi sessions", () => {
  for (const storage of ["file", "memory"] as const) {
    it(`a followUp appended after a cancelled settle, then abandoned by /tree, leaves 0 retained (${storage})`, async () => {
      const root = tempRoot();
      const faux = fauxProvider();
      const runtime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
      runtime.registerNativeProvider(faux.provider);
      const manager = storage === "file" ? SessionManager.create(root, path.join(root, "sessions")) : SessionManager.inMemory(root);
      const journal = path.join(root, "followups.json");
      let main: MainAgentController | undefined;
      let gate: (() => void) | undefined;
      let block = false;
      let pi: ExtensionAPI | undefined;
      let ctx: ExtensionContext | undefined;
      const loader = new DefaultResourceLoader({
        cwd: root, agentDir: path.join(root, "agent"), noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
        extensionFactories: [{
          name: "drain",
          factory: (api: ExtensionAPI) => {
            pi = api;
            api.on("session_start", (_event, context) => {
              ctx = context;
              main = new MainAgentController(api, "session:root", true, root, manager.getSessionId());
              main.attachFollowUpDrain(context, 120_000, journal);
              api.on("agent_before_settle", async () => { if (block) await new Promise<void>((resolve) => { gate = resolve; }); });
            });
          },
        }],
      });
      await loader.reload();
      const { session } = await createAgentSession({
        cwd: root, agentDir: path.join(root, "agent"), modelRuntime: runtime, model: faux.getModel(), resourceLoader: loader,
        sessionManager: manager, tools: [],
      });
      sessions.push(session);
      await session.bindExtensions({});
      await waitFor(() => main !== undefined);
      const cancelledWith = async (message: string) => {
        block = true;
        gate = undefined;
        faux.setResponses([fauxAssistantMessage("turn")]);
        const running = session.prompt("work");
        await waitFor(() => gate !== undefined);
        // Arrives after Fabric's before-settle handler: the abort appends it without a run.
        const receipt = main!.deliverAgent({ from, message, delivery: "followUp" });
        const aborting = session.abort();
        gate!();
        await aborting;
        await running.catch(() => undefined);
        block = false;
        return receipt.messageId;
      };
      await cancelledWith("seed");
      faux.setResponses([fauxAssistantMessage("index seed")]);
      await session.prompt("confirm seed");
      expect(retained(journal)).toBe(0);
      const fork = manager.getLeafId()!;
      const ids: string[] = [];
      for (let cycle = 0; cycle < 3; cycle++) {
        ids.push(await cancelledWith(`receipt ${cycle}`));
        expect(copies(manager, ids.at(-1)!)).toBe(1);
        await session.navigateTree(fork, { summarize: false });   // before the next boundary scans it
        faux.setResponses([fauxAssistantMessage("other branch")]);
        await session.prompt("continue other branch");
        expect(retained(journal)).toBe(0);
      }
      // A reload of the same session: nothing is replayed, nothing duplicated.
      main!.closeFollowUpDrain();
      const reloaded = new MainAgentController(pi!, "session:root", true, root, manager.getSessionId());
      reloaded.attachFollowUpDrain(ctx!, 120_000, journal);
      faux.setResponses([fauxAssistantMessage("after reload")]);
      await session.prompt("after reload");
      expect(ids.map((id) => copies(manager, id))).toEqual([1, 1, 1]);
      if (storage === "file") expect(ids.map((id) => copies(SessionManager.open(manager.getSessionFile()!), id))).toEqual([1, 1, 1]);
      reloaded.closeFollowUpDrain();
    });
  }
});

describe("Main followUp delivered ids on every branch, at the controller", () => {
  type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
  const setup = (manager: SessionManager, journal: string, persist = false) => {
    const handlers = new Map<string, Handler[]>();
    const sent: Array<{ customType: string; content: string; details: unknown }> = [];
    const ctx = { isIdle: () => false, hasPendingMessages: () => false, sessionManager: manager } as unknown as ExtensionContext;
    const pi = {
      on: (name: string, fn: Handler) => {
        handlers.set(name, [...(handlers.get(name) ?? []), fn]);
        return () => handlers.set(name, (handlers.get(name) ?? []).filter((handler) => handler !== fn));
      },
      sendMessage: (message: { customType: string; content: string; details: unknown }) => {
        sent.push(message);
        if (persist) manager.appendCustomMessageEntry(message.customType, message.content, true, message.details);
      },
    } as unknown as ExtensionAPI;
    const main = new MainAgentController(pi, "session:root", true, "/", manager.getSessionId());
    main.attachFollowUpDrain(ctx, 120_000, journal);
    const emit = (name: string, event: Record<string, unknown> = {}) => {
      for (const fn of handlers.get(name) ?? []) fn({ context: { pendingMessages: [] }, ...event }, ctx);
    };
    return { main, sent, emit };
  };

  for (const storage of ["file", "unflushed", "memory"] as const) {
    it(`an abandoned branch counts on a cold start: one durable copy, no resend (${storage})`, () => {
      const root = tempRoot();
      const manager = storage === "memory" ? SessionManager.inMemory(root) : SessionManager.create(root, path.join(root, storage));
      const fork = manager.appendMessage({ role: "user", content: "fork", timestamp: Date.now() });
      if (storage === "file") manager.appendMessage(fauxAssistantMessage("seed file"));
      const journal = path.join(root, `${storage}.json`);
      const first = setup(manager, journal);
      const receipt = first.main.deliverAgent({ from, message: "one copy on every branch", delivery: "followUp" });
      first.emit("agent_before_settle", { outcome: "completed" });
      const message = first.sent[0]!;
      manager.appendCustomMessageEntry(message.customType, message.content, true, message.details);
      manager.branch(fork);
      manager.appendMessage({ role: "user", content: "other branch", timestamp: Date.now() });
      first.main.closeFollowUpDrain();
      const second = setup(manager, journal, true);
      second.emit("agent_before_settle", { outcome: "completed" });
      second.emit("turn_end");
      if (storage === "unflushed") {
        manager.appendMessage(fauxAssistantMessage("first assistant flushes all branches"));
        // The receipt is the session file (review round 3 on pi-fabric#160): the next boundary sees the flush.
        second.emit("turn_end");
      }
      expect(second.sent).toHaveLength(0);
      expect(copies(manager, receipt.messageId)).toBe(1);
      if (storage !== "memory") expect(copies(SessionManager.open(manager.getSessionFile()!), receipt.messageId)).toBe(1);
      expect(fs.existsSync(journal)).toBe(false);
      second.main.closeFollowUpDrain();
    });
  }

  it("an entry another writer appends after the index is built, then abandoned, is not resent", () => {
    const root = tempRoot();
    const manager = SessionManager.create(root, path.join(root, "incremental"));
    manager.appendMessage({ role: "user", content: "start", timestamp: Date.now() });
    const fork = manager.appendMessage(fauxAssistantMessage("seed"));
    const journal = path.join(root, "incremental.json");
    const first = setup(manager, journal);
    const receipt = first.main.deliverAgent({ from, message: "external persistence", delivery: "followUp" });
    first.emit("agent_before_settle", { outcome: "completed" });
    first.main.closeFollowUpDrain();
    const second = setup(manager, journal, true);             // replay builds the index here
    const message = first.sent[0]!;
    manager.appendCustomMessageEntry(message.customType, message.content, true, message.details);
    manager.branch(fork);
    manager.appendMessage(fauxAssistantMessage("other branch"));
    second.emit("agent_before_settle", { outcome: "completed" });
    second.emit("turn_end");
    expect(second.sent).toHaveLength(0);
    expect(copies(manager, receipt.messageId)).toBe(1);
    expect(fs.existsSync(journal)).toBe(false);
    second.main.closeFollowUpDrain();
  });
});
