import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as realSetTimeout, clearTimeout as realClearTimeout } from "node:timers";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { FOLLOW_UP_LIMITS, MainAgentController } from "../src/main-agent.js";

const from = { id: "session:peer", name: "peer", kind: "main" as const };
const gate = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
// Lifecycle hooks drive the fixture. This real-time deadline only bounds a missing event;
// advancing the provider deadline must not advance Pi's compaction or provider work.
const bounded = async <T>(promise: Promise<T>, label: string): Promise<T> => {
  let timer: ReturnType<typeof realSetTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = realSetTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), 2_000);
    })]);
  } finally {
    if (timer) realClearTimeout(timer);
  }
};
type Details = { id: string; items?: Array<{ id: string }> };
const ids = (details: Details): string[] => (details.items ?? [details]).map(item => item.id);
type JournalItem = { id: string; handed?: boolean };

const setup = async (flushMs: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-backoff-real-pi-"));
  const journal = path.join(root, "followups.json");
  const manager = SessionManager.create(root, path.join(root, "sessions"));
  const faux = fauxProvider();
  const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
  modelRuntime.registerNativeProvider(faux.provider);
  const preparing = gate(), preparation = gate(), completing = gate(), completion = gate();
  const retryStarted = gate(), retryResponse = gate(), settled = gate();
  const handoffs: Array<{ ids: string[]; idle: boolean; triggerTurn: boolean | undefined }> = [];
  const releases: Array<{ until: string; messageIds: string[] }> = [];
  const inputs: string[] = [];
  const turns: string[] = [];
  const failures: Array<{ aborted: boolean }> = [];
  let main!: MainAgentController;
  let ctx!: ExtensionContext;
  let operation: AbortSignal | undefined;
  let starts = 0;
  let observeRetry = false;
  const loader = new DefaultResourceLoader({
    cwd: root, agentDir: path.join(root, "agent"), noExtensions: true,
    noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
    extensionFactories: [{ name: "backoff-drain", factory: (pi: ExtensionAPI) => {
      pi.on("input", event => { inputs.push(event.source); });
      pi.on("agent_start", () => { starts++; });
      pi.on("turn_end", event => { if (event.message.role === "assistant") turns.push(event.message.stopReason); });
      pi.on("agent_settled", () => { if (observeRetry) settled.resolve(); });
      pi.on("session_compact_failed", event => { failures.push(event); });
      pi.on("session_start", (_event, context) => {
        ctx = context;
        // Observe, but always call through to Pi's actual native delivery implementation.
        const send = pi.sendMessage.bind(pi);
        vi.spyOn(pi, "sendMessage").mockImplementation((message, options) => {
          if (message.customType === "pi-fabric-agent-message") handoffs.push({
            ids: ids(message.details as Details), idle: ctx.isIdle(), triggerTurn: options?.triggerTurn,
          });
          return send(message, options);
        });
        main = new MainAgentController(pi, "session:root", true, root, "root", true, event => { releases.push(event); });
        main.attachFollowUpDrain(ctx, flushMs, journal);
        // Register AFTER Main so the real drain sees the operation signal first.
        pi.on("session_before_compact", async event => {
          operation = event.signal;
          preparing.resolve();
          await preparation.promise;
          return { compaction: { summary: "Seeded conversation summary", firstKeptEntryId: event.preparation.firstKeptEntryId,
            tokensBefore: event.preparation.tokensBefore } };
        });
        // Pi remains busy until all completion handlers return, not just Main's.
        pi.on("session_compact", async () => { completing.resolve(); await completion.promise; });
      });
    } }],
  });
  await loader.reload();
  const { session } = await createAgentSession({
    cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
    sessionManager: manager, noTools: "all",
    settingsManager: SettingsManager.inMemory({ retry: { enabled: false }, compaction: { enabled: false, keepRecentTokens: 1 } }),
  });
  await session.bindExtensions({});
  let compacting: Promise<"completed" | "cancelled"> | undefined;
  const journalItems = (): JournalItem[] => fs.existsSync(journal) ? JSON.parse(fs.readFileSync(journal, "utf8")).items : [];
  const persisted = () => fs.readFileSync(manager.getSessionFile()!, "utf8").trim().split("\n")
    .map(line => JSON.parse(line) as { type: string; customType?: string; details?: Details })
    .filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message")
    .flatMap(entry => ids(entry.details!));
  const deliver = (label: string) => main.deliverAgent({ from, message: `${label}: ${"x".repeat(40_000)}`,
    delivery: "followUp", deliveryId: label });
  return {
    main, session, faux, journal, manager, preparing, preparation, completing, completion, retryStarted, retryResponse,
    handoffs, releases, inputs, turns, failures, journalItems, persisted, deliver,
    starts: () => starts,
    operation: () => operation,
    async begin() {
      faux.setResponses([fauxAssistantMessage("first answer"), fauxAssistantMessage("second answer"),
        fauxAssistantMessage("", { stopReason: "error", errorMessage: "503 service unavailable" })]);
      await session.prompt("first question");
      await session.prompt("second question");
      await session.prompt("provider failure");
      expect(turns).toEqual(["stop", "stop", "error"]);
      expect(session.isIdle).toBe(true);
      expect(starts).toBe(3);
      // Only the retry clock is accelerated. All Pi hooks, native drains, provider
      // callbacks and the drain's idle interval continue to run on real async work.
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      const oldest = deliver("oldest"), later = deliver("later");
      expect(oldest).toMatchObject({ triggered: false, reason: expect.stringContaining("provider-backoff until ") });
      expect(later).toMatchObject({ triggered: false, pendingFollowUps: 2 });
      expect(2 * 40_000).toBeGreaterThan(FOLLOW_UP_LIMITS.batchBytes);
      expect(journalItems().map(item => item.id)).toEqual([oldest.messageId, later.messageId]);
      expect(handoffs).toEqual([]);
      observeRetry = true;
      faux.appendResponses([async () => {
        retryStarted.resolve();
        await retryResponse.promise;
        return fauxAssistantMessage("retry recovered");
      }, fauxAssistantMessage("later handled"), fauxAssistantMessage("newest handled")]);
      compacting = session.compact().then(() => "completed" as const, error => {
        if (!(error instanceof Error) || error.message !== "Compaction cancelled") throw error;
        return "cancelled" as const;
      });
      await bounded(preparing.promise, "manual compaction preparation");
      expect(session.isIdle).toBe(false);
      // Parse the actual controller deadline rather than assuming setup took zero time.
      const deadline = Date.parse(oldest.reason!.replace("provider-backoff until ", ""));
      return { originalIds: [oldest.messageId, later.messageId], deadline };
    },
    async finishCompaction() {
      preparation.resolve();
      await bounded(completing.promise, "manual compaction completion handler");
      expect(session.isIdle).toBe(false);
      return () => { completion.resolve(); return bounded(compacting!, "manual compaction idle boundary"); };
    },
    async waitForRetrySettlement() { await bounded(settled.promise, "retry settlement"); await session.waitForIdle(); },
    async close() {
      main.closeFollowUpDrain();
      session.abortCompaction();
      preparation.resolve(); completion.resolve(); retryResponse.resolve();
      try {
        if (compacting) await bounded(compacting, "compaction teardown");
        await bounded(session.abort(), "session teardown");
      } finally {
        vi.useRealTimers();
        session.dispose();
        // Only the private directory made by this fixture is removed.
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  };
};

describe.each([0, 120_000])("Main provider backoff in real Pi (flushMs=%s)", flushMs => {
  it("waits past the retry deadline for compaction idle, then runs every byte-split original exactly once in FIFO order", async () => {
    const f = await setup(flushMs);
    try {
      const { originalIds, deadline } = await f.begin();
      vi.advanceTimersByTime(deadline - Date.now() - 1);
      expect(f.handoffs).toEqual([]);
      vi.advanceTimersByTime(2);
      expect(f.session.isIdle).toBe(false);
      expect.soft(f.handoffs, "the deadline must not hand work to busy Pi").toEqual([]);
      expect.soft(f.releases, "no provider wake is released before idle").toEqual([]);
      expect.soft(f.main.queueDepth().pendingFollowUps).toBe(2);
      expect.soft(f.journalItems().every(item => !item.handed), "busy work remains held, not handed").toBe(true);
      // A fresh peer is also a public observation of the otherwise-private retry latch.
      const newest = f.deliver("newest");
      originalIds.push(newest.messageId);
      expect.soft(newest.reason, "no retry is in flight during manual compaction").not.toBe("provider-retry in flight");
      expect.soft(f.main.queueDepth().pendingFollowUps).toBe(3);
      const finish = await f.finishCompaction();
      expect.soft(f.handoffs, "later completion handlers still own the busy boundary").toEqual([]);
      expect(await finish()).toBe("completed");
      // No human prompt or stand-in settlement may rescue the queue.
      await bounded(f.retryStarted.promise, "oldest held item to start the actual retry");
      expect.soft(f.handoffs).toEqual([{ ids: [originalIds[0]], idle: true, triggerTurn: true }]);
      expect.soft(f.main.queueDepth().pendingFollowUps, "later FIFO batches wait for the real retry outcome").toBe(2);
      expect.soft(f.releases).toHaveLength(1);
      expect.soft(f.releases[0]?.messageIds).toEqual([originalIds[0]]);
      f.retryResponse.resolve();
      await f.waitForRetrySettlement();
      expect.soft(f.handoffs.flatMap(handoff => handoff.ids)).toEqual(originalIds);
      expect.soft(f.persisted(), "all originals must have native session-file receipts, once and in order").toEqual(originalIds);
      f.main.confirmInbox();
      expect.soft(f.journalItems(), "handoff alone cannot confirm the journal; native persistence can").toEqual([]);
      expect.soft(fs.existsSync(f.journal)).toBe(false);
      expect.soft(f.main.queueDepth().pendingFollowUps).toBe(0);
      expect.soft(f.faux.state.callCount).toBe(6);
      expect(f.inputs).toEqual(["interactive", "interactive", "interactive"]);
    } finally { await f.close(); }
  });

  it.each(["halt", "cancel"] as const)("retains the %s veto while manual compaction crosses the retry deadline", async veto => {
    const f = await setup(flushMs);
    try {
      const { originalIds, deadline } = await f.begin();
      if (veto === "halt") f.main.halt();
      else f.session.abortCompaction();
      if (veto === "cancel") expect(f.operation()?.aborted).toBe(true);
      vi.advanceTimersByTime(deadline - Date.now() + 1);
      expect.soft(f.handoffs, "a halted or cancelled operation cannot release a triggering retry").toEqual([]);
      expect.soft(f.releases).toEqual([]);
      f.preparation.resolve(); f.completion.resolve();
      // Completion or cancellation cannot be allowed to create a hidden provider run.
      f.retryResponse.resolve();
      await bounded(f.session.waitForIdle(), "vetoed compaction to become idle");
      await new Promise<void>(resolve => realSetTimeout(resolve, 75));
      expect.soft(f.starts()).toBe(3);
      expect.soft(f.faux.state.callCount).toBe(3);
      expect.soft(f.handoffs.every(handoff => handoff.triggerTurn === false)).toBe(true);
      expect.soft(f.releases).toEqual([]);
      const appended = f.persisted();
      const held = f.journalItems().filter(item => !item.handed).map(item => item.id);
      expect.soft([...appended, ...held].sort(), "veto retains every original as passive context or held work").toEqual([...originalIds].sort());
      if (veto === "halt") expect(JSON.parse(fs.readFileSync(`${f.journal}.delivered`, "utf8")).halted).toBe(true);
      else expect(f.failures).toMatchObject([{ aborted: true }]);
    } finally { await f.close(); }
  });
});
