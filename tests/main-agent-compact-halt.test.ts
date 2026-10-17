import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import {
  createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager,
  type AgentSession, type ExtensionAPI, type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { describe, expect, it } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { installFabricEscapeHalt } from "../src/ui/escape-halt.js";

// Regression: generic compact() aborts the old run BEFORE session_before_compact.
// That lifecycle abort is not an explicit operator stop. Exercise the real SDK and
// built-in summarizer; fauxProvider replaces only model I/O, not compaction/hooks.
const gate = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const bounded = async <T>(promise: Promise<T>): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error("native lifecycle gate timed out")), 5_000);
    })]);
  } finally { clearTimeout(timer); }
};

describe("Main generic compaction does not manufacture an operator halt", () => {
  it.each([0, 60_000].flatMap(flushMs => (["active-tool", "late-settle", "idle", "raw-abort", "explicit-stop", "configured-escape", "disabled-escape"] as const)
    .map(phase => ({ flushMs, phase }))))(
    "native compact/reload preserves only explicit owner authority ($phase, flushMs=$flushMs)",
    async ({ flushMs, phase }) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-compact-halt-"));
      const journal = path.join(root, "journal.json");
      const events: Array<Record<string, unknown>> = [];
      const reached = gate();
      const release = gate();
      const escapeObserved = gate();
      const stopped = phase === "explicit-stop" || phase === "configured-escape";
      const toolPhase = phase !== "idle" && phase !== "late-settle";
      let terminalInput: ((data: string) => unknown) | undefined;
      let removeEscape: (() => void) | undefined;
      let generations = 0;
      let armed = false;
      let starts = 0;
      let main: MainAgentController | undefined;
      let session: AgentSession | undefined;
      let running: Promise<void> | undefined;
      let compacting: Promise<unknown> | undefined;
      const faux = fauxProvider();
      const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false,
        authPath: path.join(root, "auth.json") });
      modelRuntime.registerNativeProvider(faux.provider);
      const index = () => JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8"));
      const loader = new DefaultResourceLoader({
        cwd: root, agentDir: path.join(root, "agent"), noSkills: true, noPromptTemplates: true,
        noThemes: true, noContextFiles: true,
        extensionFactories: [{ name: "compact-halt-regression", factory: (pi: ExtensionAPI) => {
          pi.registerTool({ name: "work", label: "work", description: "waits for native run cancellation",
            parameters: Type.Object({}), execute: async (_id, _params, signal) => {
              events.push({ type: "tool-active" });
              reached.resolve();
              await Promise.race([release.promise, new Promise<void>(done => {
                if (signal?.aborted) done();
                else signal?.addEventListener("abort", () => {
                  events.push({ type: "tool-signal-abort" }); done();
                }, { once: true });
              })]);
              return { content: [{ type: "text", text: "work ended" }], details: {} };
            } });
          pi.on("agent_start", () => { starts++; events.push({ type: "agent_start" }); });
          pi.on("turn_end", (event, ctx) => {
            events.push({ type: "turn_end", stopReason: event.message.role === "assistant" ? event.message.stopReason : undefined,
              errorMessage: event.message.role === "assistant" ? event.message.errorMessage : undefined,
              signalAborted: ctx.signal?.aborted });
          });
          pi.on("agent_settled", (event, ctx) => {
            const settled = event as { outcome?: string; aborted?: boolean };
            events.push({ type: "agent_settled", outcome: settled.outcome, aborted: settled.aborted,
              signalAborted: ctx.signal?.aborted });
          });
          pi.on("session_before_compact", (event, ctx) => {
            events.push({ type: "session_before_compact", reason: event.reason,
              operationAborted: event.signal.aborted, runSignalAborted: ctx.signal?.aborted, halted: index().halted ?? false });
            // Do not provide an extension summary: use Pi's actual default compactor.
          });
          pi.on("session_compact", event => {
            events.push({ type: "session_compact", reason: event.reason, fromExtension: event.fromExtension,
              halted: index().halted ?? false });
          });
          pi.on("session_start", (_event, ctx) => {
            generations++;
            main = new MainAgentController(pi, "session:root", true, root, "root");
            main.attachFollowUpDrain(ctx, flushMs, journal);
            if (phase === "configured-escape" || phase === "disabled-escape") {
              removeEscape?.();
              // Exercise the real TUI observer with extension.ts's configured
              // halted callback; native SDK cancellation remains a separate path.
              removeEscape = installFabricEscapeHalt({ mode: "tui", hasUI: true, ui: {
                onTerminalInput: (handler: (data: string) => unknown) => { terminalInput = handler; return () => { terminalInput = undefined; }; },
                notify: () => undefined,
              } } as unknown as ExtensionContext, {
                enabled: () => phase === "configured-escape", ownsInput: () => false,
                halted: () => { main!.halt(); escapeObserved.resolve(); return false; }, halt: () => 0,
              });
            }
            // Deliberately after Main's handler: cancel after a normal last turn,
            // including after Main already saw the actionable settle boundary.
            pi.on("agent_before_settle", async (event, settleCtx) => {
              events.push({ type: "agent_before_settle", outcome: (event as { outcome?: string }).outcome, signalAborted: settleCtx.signal?.aborted });
              if (armed && phase === "late-settle") {
                armed = false;
                reached.resolve();
                await release.promise;
              }
            });
          });
          pi.on("session_shutdown", () => { removeEscape?.(); main?.closeFollowUpDrain(); });
        } }],
      });
      try {
        await loader.reload();
        ({ session } = await createAgentSession({ cwd: root, agentDir: path.join(root, "agent"),
          modelRuntime, model: faux.getModel(), resourceLoader: loader,
          sessionManager: SessionManager.inMemory(root), tools: ["work"],
          settingsManager: SettingsManager.inMemory({ retry: { enabled: false },
            compaction: { enabled: false, keepRecentTokens: 1 } }),
        }));
        await session.bindExtensions({ shutdownHandler: () => undefined });
        faux.setResponses([fauxAssistantMessage("first answer"), fauxAssistantMessage("second answer"),
          ...(phase === "idle" ? [] : [toolPhase ? fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" })
            : fauxAssistantMessage("normal final answer")]),
          ...Array.from({ length: 3 }, () => fauxAssistantMessage("native compaction summary")),
        ]);
        await session.prompt("first question");
        await session.prompt("second question");
        if (phase !== "idle") {
          armed = true;
          running = session.prompt("work to interrupt internally for compaction");
          await bounded(reached.promise);
          if (phase === "explicit-stop") main!.stop();
          if (phase === "configured-escape" || phase === "disabled-escape") {
            expect(terminalInput?.("\x1b")).toBeUndefined(); // Observer never consumes native Escape.
          }
          if (["raw-abort", "explicit-stop", "configured-escape", "disabled-escape"].includes(phase)) {
            await bounded(session.abort());
            if (phase === "configured-escape") await bounded(escapeObserved.promise);
          }
        }
        if (phase === "raw-abort") {
          const beforePeer = { starts, calls: faux.state.callCount };
          const rawReceipt = main!.deliverAgent({ from: { id: "session:peer", name: "peer", kind: "main" },
            message: "fresh followUp immediately after raw SDK abort", delivery: "followUp", deliveryId: "raw-abort-fresh" });
          await bounded(session.waitForIdle());
          expect(rawReceipt).toMatchObject({ triggered: true });
          expect(index().halted).toBeUndefined();
          expect(starts - beforePeer.starts).toBe(1);
          expect(faux.state.callCount - beforePeer.calls).toBe(1);
        }
        events.push({ type: "calling-session.compact", halted: index().halted ?? false });
        compacting = session.compact(); // Real entry: await abort() before the hook.
        release.resolve();
        const result = await bounded(compacting);
        if (running) await bounded(running);
        expect(result).toMatchObject({ summary: expect.stringContaining("native compaction summary") });
        expect(events.filter(event => event.type === "session_compact")).toMatchObject([{ fromExtension: false }]);
        const baseline = { starts, calls: faux.state.callCount };
        faux.appendResponses([fauxAssistantMessage("fresh peer followUp handled")]);
        const receipt = main!.deliverAgent({ from: { id: "session:peer", name: "peer", kind: "main" },
          message: "fresh followUp after successful native compaction", delivery: "followUp", deliveryId: "fresh-after-compact" });
        await bounded(session.waitForIdle());
        events.push({ type: "fresh-followUp-result", receipt, persistedHalted: index().halted ?? false,
          newRuns: starts - baseline.starts, newRequests: faux.state.callCount - baseline.calls });
        expect.soft(receipt).toMatchObject(stopped ? { triggered: false, halted: true, reason: "operator-halt" } : { triggered: true });
        expect.soft(index().halted).toBe(stopped ? true : undefined);
        expect.soft(starts - baseline.starts).toBe(stopped ? 0 : 1);
        expect.soft(faux.state.callCount - baseline.calls).toBe(stopped ? 0 : 1);
        await session.reload();
        expect(generations).toBe(2);
        faux.appendResponses([fauxAssistantMessage("fresh peer after reload")]);
        const beforeReloadPeer = { starts, calls: faux.state.callCount };
        const afterReload = main!.deliverAgent({ from: { id: "session:peer", name: "peer", kind: "main" },
          message: "fresh followUp after reload", delivery: "followUp", deliveryId: "fresh-after-reload" });
        await bounded(session.waitForIdle());
        expect(afterReload).toMatchObject(stopped ? { triggered: false, halted: true, reason: "operator-halt" } : { triggered: true });
        expect(index().halted).toBe(stopped ? true : undefined);
        expect(starts - beforeReloadPeer.starts).toBe(stopped ? 0 : 1);
        expect(faux.state.callCount - beforeReloadPeer.calls).toBe(stopped ? 0 : 1);
      } finally {
        console.info("COMPACT_HALT_EVIDENCE", JSON.stringify({ phase, flushMs, events }));
        release.resolve();
        session?.abortCompaction();
        await compacting?.catch(() => undefined);
        await session?.abort();
        await running?.catch(() => undefined);
        removeEscape?.();
        main?.closeFollowUpDrain();
        session?.dispose();
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  );
});
