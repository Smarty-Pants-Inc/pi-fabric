import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { AgentCompletionInbox, AGENT_COMPLETION_MESSAGE_TYPE } from "../src/agents/completion-inbox.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { ShellEventInbox, SHELL_MESSAGE_TYPE } from "../src/core/shell-inbox.js";

const usage = { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };

describe.each(["shell", "agent"] as const)("%s completion with the real Pi SDK", kind => {
  it.each(["recovered-error", "manual-compaction", "cancelled-compaction"] as const)("wakes Main after %s without more input", async scenario => {
    const cwd = await mkdtemp(path.join(os.tmpdir(), "fabric-completion-sdk-"));
    const jobs = new FabricShellJobStore(cwd);
    let shellInbox: ShellEventInbox | undefined;
    let agentInbox: AgentCompletionInbox | undefined;
    let job: ReturnType<typeof jobs.begin> | undefined;
    const messageType = kind === "shell" ? SHELL_MESSAGE_TYPE : AGENT_COMPLETION_MESSAGE_TYPE;
    let sends = 0, inputs = 0;
    let compacting = false;
    const requests: unknown[][] = [], errors: unknown[] = [], turns: string[] = [];
    const finish = async () => {
      if (kind === "shell") await job!.finish(0);
      else agentInbox!.enqueue({ id: "background-result", name: "Worker", status: "completed", text: "Done", startedAt: 1, finishedAt: 2 });
    };
    const settingsManager = SettingsManager.inMemory({
      defaultProjectTrust: "never", compaction: { enabled: false, keepRecentTokens: 1 },
      retry: { enabled: true, maxRetries: 1, baseDelayMs: 1 },
    });
    const loader = new DefaultResourceLoader({
      cwd, agentDir: cwd, settingsManager, noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true,
      agentsFilesOverride: () => ({ agentsFiles: [] }),
      extensionFactories: [pi => {
        pi.on("session_start", (_event, ctx) => {
          const observed = { ...pi, sendMessage: ((message, options) => {
            if (message.customType === messageType) {
              sends++;
              expect(compacting).toBe(false);
            }
            pi.sendMessage(message, options);
          }) satisfies typeof pi.sendMessage };
          if (kind === "shell") shellInbox = new ShellEventInbox(observed, ctx, jobs);
          else agentInbox = new AgentCompletionInbox(observed, ctx);
        });
        pi.on("input", () => { inputs++; });
        pi.on("turn_end", event => { if (event.message.role === "assistant") turns.push(event.message.stopReason); });
        pi.on("session_before_compact", async event => {
          compacting = true;
          await finish();
          // Let the initial 40ms delivery check observe the busy host.
          await delay(100);
          expect(sends).toBe(0);
          if (scenario === "cancelled-compaction") return { cancel: true };
          return { compaction: { summary: "Offline summary", firstKeptEntryId: event.preparation.firstKeptEntryId, tokensBefore: event.preparation.tokensBefore } };
        });
        pi.on("session_compact", async () => {
          // This event precedes idleness. Even a recheck armed here alone can
          // fire too early when a later compaction handler is asynchronous.
          await delay(350);
          expect(sends).toBe(0);
        });
        pi.registerProvider("offline-completion", {
          api: "offline-completion", apiKey: "offline", baseUrl: "http://invalid.local",
          models: [{ id: "fixture", name: "Fixture", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 4096 }],
          streamSimple(model, context) {
            requests.push(context.messages);
            const failed = scenario === "recovered-error" && requests.length === 1;
            const message: AssistantMessage = {
              role: "assistant", content: [{ type: "text", text: failed ? "" : "Waiting for the background result." }],
              api: model.api, provider: model.provider, model: model.id, usage, timestamp: Date.now(),
              stopReason: failed ? "error" : "stop", ...(failed ? { errorMessage: "503 Service Unavailable" } : {}),
            };
            const stream = createAssistantMessageEventStream();
            stream.push({ type: "start", partial: message });
            if (failed) stream.push({ type: "error", reason: "error", error: message });
            else stream.push({ type: "done", reason: "stop", message });
            stream.end(); return stream;
          },
        });
      }],
    });
    try {
      await loader.reload();
      expect(loader.getExtensions().errors).toEqual([]);
      const modelRuntime = await ModelRuntime.create({ authPath: path.join(cwd, "auth.json"), modelsPath: null, modelsStorePath: path.join(cwd, "models.json"), refreshOnCreate: false });
      const { session } = await createAgentSession({ cwd, agentDir: cwd, settingsManager, modelRuntime, resourceLoader: loader, sessionManager: SessionManager.inMemory(cwd), tools: [] });
      try {
        await session.bindExtensions({ onError: error => errors.push(error) });
        await session.setModel(modelRuntime.getModel("offline-completion", "fixture")!);
        if (kind === "shell") { job = jobs.begin("bash", "controlled background task"); job.spill(); }
        await session.prompt("Wait for the detached result.");
        expect(inputs).toBe(1);
        expect(turns).toEqual(scenario === "recovered-error" ? ["error", "stop"] : ["stop"]);
        expect(session.isIdle).toBe(true);
        const before = requests.length;
        if (scenario === "recovered-error") await finish();
        else {
          if (scenario === "cancelled-compaction") await expect(session.compact()).rejects.toThrow("Compaction cancelled");
          else await session.compact();
          compacting = false;
        }
        await vi.waitFor(() => expect(requests.length).toBe(before + 1), { timeout: 3000 });
        await session.waitForIdle();
        expect(sends).toBe(1);
        expect(inputs).toBe(1);
        expect(session.messages.filter(message => message.role === "custom" && message.customType === messageType)).toHaveLength(1);
        expect(JSON.stringify(requests.at(-1))).toContain(kind === "shell" ? "controlled background task" : "background-result");
        expect(errors).toEqual([]);
      } finally {
        shellInbox?.close(); agentInbox?.close();
        await jobs.close(); session.dispose();
      }
    } finally {
      shellInbox?.close(); agentInbox?.close();
      await jobs.close();
      await rm(cwd, { recursive: true, force: true });
    }
  }, 15000);
});
