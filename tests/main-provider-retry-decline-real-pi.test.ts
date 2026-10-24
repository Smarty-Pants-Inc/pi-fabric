import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as realSetTimeout, clearTimeout as realClearTimeout } from "node:timers";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import {
  createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager,
  type AgentSession, type ExtensionAPI,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";

const gate = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const bounded = async <T>(promise: Promise<T>, label: string): Promise<T> => {
  let timer: ReturnType<typeof realSetTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = realSetTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), 2_000);
    })]);
  } finally { if (timer) realClearTimeout(timer); }
};
type Details = { id: string; items?: Array<{ id: string }> };
const ids = (details: Details) => (details.items ?? [details]).map(item => item.id);

describe.each([0, 120_000])("Main retry after a real Pi extension decline (flushMs=%s)", flushMs => {
  it("keeps the deadline and delivers each original once in FIFO order without a rescue input", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-retry-decline-"));
    const journal = path.join(root, "followups.json");
    const retryStarted = gate(), retryResponse = gate(), settled = gate();
    let main: MainAgentController | undefined;
    let session: AgentSession | undefined;
    let operation: AbortSignal | undefined;
    let starts = 0;
    const failures: Array<{ aborted: boolean }> = [];
    const handoffs: Array<{ ids: string[]; idle: boolean; triggerTurn: boolean | undefined }> = [];
    try {
      const manager = SessionManager.create(root, path.join(root, "sessions"));
      const faux = fauxProvider();
      const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
      modelRuntime.registerNativeProvider(faux.provider);
      const loader = new DefaultResourceLoader({
        cwd: root, agentDir: path.join(root, "agent"), noExtensions: true,
        noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
        extensionFactories: [{ name: "decline-retry", factory: (pi: ExtensionAPI) => {
          pi.on("agent_start", () => { starts++; });
          pi.on("agent_settled", () => { if (starts > 3) settled.resolve(); });
          pi.on("session_compact_failed", event => { failures.push(event); });
          pi.on("session_start", (_event, ctx) => {
            const send = pi.sendMessage.bind(pi);
            vi.spyOn(pi, "sendMessage").mockImplementation((message, options) => {
              if (message.customType === "pi-fabric-agent-message") handoffs.push({
                ids: ids(message.details as Details), idle: ctx.isIdle(), triggerTurn: options?.triggerTurn,
              });
              return send(message, options);
            });
            main = new MainAgentController(pi, "session:root", true, root, "root");
            main.attachFollowUpDrain(ctx, flushMs, journal);
            pi.on("session_before_compact", event => {
              operation = event.signal;
              return { cancel: true }; // A decline does not abort Pi's operation signal.
            });
          });
        } }],
      });
      await loader.reload();
      ({ session } = await createAgentSession({
        cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
        sessionManager: manager, noTools: "all",
        settingsManager: SettingsManager.inMemory({ retry: { enabled: false }, compaction: { enabled: false, keepRecentTokens: 1 } }),
      }));
      await session.bindExtensions({});
      faux.setResponses([fauxAssistantMessage("first answer"), fauxAssistantMessage("second answer"),
        fauxAssistantMessage("", { stopReason: "error", errorMessage: "503 service unavailable" })]);
      await session.prompt("first question");
      await session.prompt("second question");
      await session.prompt("provider failure");
      expect(starts).toBe(3);
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      const from = { id: "session:peer", name: "peer", kind: "main" as const };
      const deliver = (label: string) => main!.deliverAgent({ from, message: `${label}: ${"x".repeat(40_000)}`,
        delivery: "followUp", deliveryId: label });
      const oldest = deliver("oldest"), later = deliver("later");
      const deadline = Date.parse(oldest.reason!.replace("provider-backoff until ", ""));
      vi.advanceTimersByTime(5_000);
      await expect(session.compact()).rejects.toThrow("Compaction cancelled");
      expect(failures).toMatchObject([{ aborted: true }]);
      expect(operation?.aborted).toBe(false);
      expect(handoffs).toEqual([]);
      faux.appendResponses([async () => {
        retryStarted.resolve();
        await retryResponse.promise;
        return fauxAssistantMessage("retry recovered");
      }, fauxAssistantMessage("later handled")]);
      vi.advanceTimersByTime(deadline - Date.now() + 1);
      await bounded(retryStarted.promise, "retry after the extension decline");
      expect(handoffs).toEqual([{ ids: [oldest.messageId], idle: true, triggerTurn: true }]);
      expect(main!.queueDepth().pendingFollowUps).toBe(1);
      retryResponse.resolve();
      await bounded(settled.promise, "retry settlement");
      await bounded(session.waitForIdle(), "native FIFO drain");
      const persisted = fs.readFileSync(manager.getSessionFile()!, "utf8").trim().split("\n")
        .map(line => JSON.parse(line) as { type: string; customType?: string; details?: Details })
        .filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message")
        .flatMap(entry => ids(entry.details!));
      expect(persisted).toEqual([oldest.messageId, later.messageId]);
      expect(handoffs.flatMap(item => item.ids)).toEqual(persisted);
      main!.confirmInbox();
      expect(fs.existsSync(journal)).toBe(false);
      expect(main!.queueDepth().pendingFollowUps).toBe(0);
      expect(faux.state.callCount).toBe(5);
    } finally {
      retryResponse.resolve();
      main?.closeFollowUpDrain();
      await bounded(session?.abort() ?? Promise.resolve(), "session cleanup");
      vi.useRealTimers();
      session?.dispose();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// A compaction can end before Main's own session_before_compact handler runs: an extension
// loaded ahead of Main declines it, or Pi finds nothing to compact before the hook. When a
// cancelled tree summary came first, its aborted signal must not make that ending a cancel.
type UnseenEnding = { name: string; earlierDecline: boolean; error: string; failure: { aborted: boolean; errorMessage?: string } };
const unseenEndings: UnseenEnding[] = [
  { name: "an extension loaded ahead of Main declines it", earlierDecline: true, error: "Compaction cancelled", failure: { aborted: true } },
  { name: "Pi finds nothing to compact", earlierDecline: false, error: "Nothing to compact (session too small)",
    failure: { aborted: false, errorMessage: "Compaction failed: Nothing to compact (session too small)" } },
];

const unseenCompaction = async (flushMs: number, ending: UnseenEnding) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-retry-unseen-"));
  const journal = path.join(root, "followups.json");
  const treeEntered = gate(), treeRelease = gate(), retryStarted = gate(), retryResponse = gate(), settled = gate();
  let main: MainAgentController | undefined;
  let starts = 0;
  let trees = 0;
  let compactionsSeenAfterMain = 0;
  let treeSignal: AbortSignal | undefined;
  const failures: Array<{ aborted: boolean; errorMessage?: string }> = [];
  const handoffs: Array<{ ids: string[]; idle: boolean; triggerTurn: boolean | undefined }> = [];
  const inputs: string[] = [];
  const manager = SessionManager.create(root, path.join(root, "sessions"));
  const faux = fauxProvider();
  const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
  modelRuntime.registerNativeProvider(faux.provider);
  const loader = new DefaultResourceLoader({
    cwd: root, agentDir: path.join(root, "agent"), noExtensions: true,
    noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    extensionFactories: [{ name: "unseen-compaction", factory: (pi: ExtensionAPI) => {
      // Registered at load, ahead of Main's handlers (attached at session_start): an extension
      // loaded before Fabric. Pi stops session_before_compact dispatch at this veto.
      if (ending.earlierDecline) pi.on("session_before_compact", () => ({ cancel: true }));
      pi.on("input", event => { inputs.push(event.source); });
      pi.on("agent_start", () => { starts++; });
      pi.on("agent_settled", () => { if (starts > 3) settled.resolve(); });
      pi.on("session_tree", () => { trees++; });
      pi.on("session_compact_failed", event => { failures.push(event); });
      pi.on("session_start", (_event, ctx) => {
        const send = pi.sendMessage.bind(pi);
        vi.spyOn(pi, "sendMessage").mockImplementation((message, options) => {
          if (message.customType === "pi-fabric-agent-message") handoffs.push({
            ids: ids(message.details as Details), idle: ctx.isIdle(), triggerTurn: options?.triggerTurn,
          });
          return send(message, options);
        });
        main = new MainAgentController(pi, "session:root", true, root, "root");
        main.attachFollowUpDrain(ctx, flushMs, journal);
        // Registered AFTER Main: holds the branch summary open until Escape, and shows
        // whether any compaction hook after Main's own was reached.
        pi.on("session_before_tree", async event => {
          treeSignal = event.signal;
          treeEntered.resolve();
          await treeRelease.promise;
          return undefined;
        });
        pi.on("session_before_compact", () => { compactionsSeenAfterMain++; });
      });
    } }],
  });
  await loader.reload();
  const { session } = await createAgentSession({
    cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
    sessionManager: manager, noTools: "all",
    settingsManager: SettingsManager.inMemory({ retry: { enabled: false },
      compaction: { enabled: false, keepRecentTokens: ending.earlierDecline ? 1 : 10_000_000 } }),
  });
  await session.bindExtensions({});
  const from = { id: "session:peer", name: "peer", kind: "main" as const };
  const deliver = (label: string) => main!.deliverAgent({ from, message: `${label}: ${"x".repeat(40_000)}`,
    delivery: "followUp", deliveryId: label });
  const persisted = () => fs.readFileSync(manager.getSessionFile()!, "utf8").trim().split("\n")
    .map(line => JSON.parse(line) as { type: string; customType?: string; details?: Details })
    .filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message")
    .flatMap(entry => ids(entry.details!));
  return {
    session, faux, manager, journal, handoffs, failures, inputs, retryStarted, retryResponse, settled, persisted,
    main: () => main!,
    async begin() {
      faux.setResponses([fauxAssistantMessage("first answer"), fauxAssistantMessage("second answer"),
        fauxAssistantMessage("", { stopReason: "error", errorMessage: "503 service unavailable" })]);
      await session.prompt("first question");
      await session.prompt("second question");
      await session.prompt("provider failure");
      expect(starts).toBe(3);
      // Only the retry clock is faked; Pi's hooks, native queue and the drain's idle waiters run for real.
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      const oldest = deliver("oldest"), later = deliver("later");
      expect(oldest).toMatchObject({ triggered: false, reason: expect.stringContaining("provider-backoff until ") });
      expect(later).toMatchObject({ triggered: false, pendingFollowUps: 2 });
      return { oldest, later, deadline: Date.parse(oldest.reason!.replace("provider-backoff until ", "")) };
    },
    // Escape during an actual /tree branch summary: Pi returns cancelled and emits no session_tree.
    async cancelTreeSummary() {
      const leaf = manager.getLeafId();
      const target = manager.getEntries().find(entry => entry.type === "message" &&
        (entry as { message?: { role?: string } }).message?.role === "user")!;
      const navigation = session.navigateTree(target.id, { summarize: true })
        .then(result => result, (error: Error) => ({ error: error.message }));
      await bounded(treeEntered.promise, "branch summary hook");
      expect(session.isIdle).toBe(false);
      session.abortBranchSummary();
      treeRelease.resolve();
      expect(JSON.stringify(await bounded(navigation, "cancelled tree navigation"))).toMatch(/"cancelled":true|aborted/);
      expect(treeSignal?.aborted).toBe(true);
      expect(trees, "a cancelled summary emits no session_tree").toBe(0);
      expect(manager.getLeafId(), "the cancelled navigation leaves the branch in place").toBe(leaf);
      expect(session.isIdle).toBe(true);
    },
    compactionsSeenAfterMain: () => compactionsSeenAfterMain,
    async close() {
      main?.closeFollowUpDrain();
      treeRelease.resolve(); retryResponse.resolve();
      session.abortBranchSummary();
      session.abortCompaction();
      try {
        await bounded(session.abort(), "session cleanup");
      } finally {
        vi.useRealTimers();
        session.dispose();
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  };
};

describe.each([0, 120_000])("Main retry after a compaction Main never saw (flushMs=%s)", flushMs => {
  it.each(unseenEndings.flatMap(ending => [true, false].map(cancelledTree => ({ ...ending, cancelledTree }))))(
    "keeps the deadline when $name (after a cancelled tree summary: $cancelledTree)", async ({ cancelledTree, ...ending }) => {
      const f = await unseenCompaction(flushMs, ending);
      try {
        const { oldest, later, deadline } = await f.begin();
        vi.advanceTimersByTime(5_000);
        if (cancelledTree) await f.cancelTreeSummary();
        await expect(f.session.compact()).rejects.toThrow(ending.error);
        expect(f.failures).toMatchObject([ending.failure]);
        expect(f.compactionsSeenAfterMain(), "Main's compaction hook was never reached").toBe(0);
        expect(f.handoffs).toEqual([]);
        f.faux.appendResponses([async () => {
          f.retryStarted.resolve();
          await f.retryResponse.promise;
          return fauxAssistantMessage("retry recovered");
        }, fauxAssistantMessage("later handled")]);
        vi.advanceTimersByTime(deadline - Date.now() - 1);
        expect(f.handoffs, "no release before the deadline").toEqual([]);
        vi.advanceTimersByTime(2);
        // No owner input, settle or later delivery may rescue held work.
        await bounded(f.retryStarted.promise, "retry at the deadline");
        expect(f.handoffs).toEqual([{ ids: [oldest.messageId], idle: true, triggerTurn: true }]);
        expect(f.main().queueDepth().pendingFollowUps).toBe(1);
        f.retryResponse.resolve();
        await bounded(f.settled.promise, "retry settlement");
        await bounded(f.session.waitForIdle(), "native FIFO drain");
        const persisted = f.persisted();
        expect(persisted, "each original once, FIFO, natively persisted").toEqual([oldest.messageId, later.messageId]);
        expect(f.handoffs.flatMap(item => item.ids)).toEqual(persisted);
        f.main().confirmInbox();
        expect(fs.existsSync(f.journal)).toBe(false);
        expect(f.main().queueDepth().pendingFollowUps).toBe(0);
        expect(f.inputs, "no rescue input").toEqual(["interactive", "interactive", "interactive"]);
      } finally { await f.close(); }
    },
  );
});
