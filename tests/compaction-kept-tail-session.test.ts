import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import {
  createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager,
  type SessionBeforeCompactEvent,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { expect, it } from "vitest";
import { registerCompactionHook } from "../src/compaction/hook.js";
import { registerLazyCompactionHook } from "../src/compaction/lazy-hook.js";

// Keep reason: recorded provider overflow recovery and append-only data safety.
// This crosses the real session/tool/provider/native compaction/retry boundary.
const usage = { input: 10, output: 5, cacheRead: 0, cacheWrite: 0, totalTokens: 15, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
const ordinarySummary = "## Goal\nFinish the ordinary task.\n## Progress\nThe requested tool ran.\n## Next Steps\nAnswer the user.";

it.each([
  { size: 2_700_000, register: registerLazyCompactionHook, label: "2.7 MB lazy" },
  { size: 3 * 1024 * 1024, register: registerCompactionHook, label: "3 MiB synchronous registration" },
])("recovers a kept-tail tool result through native summary and one retry ($label)", async ({ size, register }) => {
  const cwd = await mkdtemp(path.join(os.tmpdir(), "fabric-kept-tail-session-"));
  const manager = SessionManager.create(cwd, path.join(cwd, "sessions"));
  const settingsManager = SettingsManager.inMemory({
    defaultProjectTrust: "never", compaction: { enabled: true, keepRecentTokens: 1000, reserveTokens: 1024 }, retry: { enabled: false },
  });
  const huge = "X".repeat(size);
  const projectedContent = [{ type: "text" as const, text: huge }];
  const serializedBytes = Buffer.byteLength(JSON.stringify(projectedContent), "utf8");
  const requestKinds: string[] = [];
  const summaryInputs: string[] = [];
  const retryInputs: string[] = [];
  const reasons: string[] = [];
  const cuts: string[] = [];
  let prefixBeforeCompaction: Buffer | undefined;
  let emittedTool = false;
  let giantId: string | undefined;
  const loader = new DefaultResourceLoader({
    cwd, agentDir: cwd, settingsManager, noExtensions: true, noSkills: true, noThemes: true, noPromptTemplates: true,
    agentsFilesOverride: () => ({ agentsFiles: [] }),
    extensionFactories: [pi => {
      pi.registerTool({
        name: "giant_fixture", label: "Giant fixture", description: "Return a large fixture.", parameters: Type.Object({}),
        async execute() { return { content: projectedContent, details: { originalBytes: size } }; },
      });
      // Capture the native prepared cut before the guard claims the result.
      pi.on("session_before_compact", async event => {
        reasons.push(event.reason); cuts.push(event.preparation.firstKeptEntryId);
        const raw = manager.getBranch().find(entry => entry.type === "message" && entry.message.role === "toolResult");
        giantId = raw?.id;
        expect(raw?.type === "message" && raw.message.role === "toolResult" && raw.message.content).toEqual(projectedContent);
        const rawCut = manager.getBranch().findIndex(entry => entry.id === event.preparation.firstKeptEntryId);
        expect(manager.getBranch().slice(rawCut).some(entry => entry.id === giantId)).toBe(true);
        prefixBeforeCompaction = await readFile(manager.getSessionFile()!);
      });
      register(pi, { getEngine: () => "pi" });
      pi.registerProvider("offline-kept-tail-session", {
        api: "offline-kept-tail-session", apiKey: "offline-session-key", baseUrl: "http://invalid.local",
        models: [{ id: "fixture", name: "Fixture", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8_000_000, maxTokens: 2048 }],
        streamSimple(model, context) {
          const request = JSON.stringify(context.messages);
          const isSummary = request.includes("context summarization assistant");
          const hasGiant = request.includes("X".repeat(100_000));
          const kind = isSummary ? "native-summary" : hasGiant ? "provider-overflow" : emittedTool ? "retry-answer" : "tool-call";
          requestKinds.push(kind);
          const message: AssistantMessage = {
            role: "assistant", content: [{ type: "text", text: "Recovered answer." }], api: model.api, provider: model.provider, model: model.id,
            stopReason: "stop", timestamp: Date.now(), usage,
          };
          if (isSummary) {
            summaryInputs.push(request);
            // Deliberately do not echo a guard notice. The real native summary
            // is ordinary task content; production code must retain the notice.
            message.content = [{ type: "text", text: ordinarySummary }];
          } else if (hasGiant) {
            message.content = [{ type: "text", text: "ABANDONED_OVERFLOW_ATTEMPT" }];
            message.stopReason = "error";
            message.errorMessage = "maximum context length is 16384 tokens; requested 700000 tokens";
          } else if (!emittedTool) {
            emittedTool = true;
            message.stopReason = "toolUse";
            message.content = [{ type: "toolCall", id: "giant-call", name: "giant_fixture", arguments: {} }];
          } else retryInputs.push(request);
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "start", partial: message });
          if (message.stopReason === "error") stream.push({ type: "error", reason: "error", error: message });
          else stream.push({ type: "done", reason: message.stopReason === "toolUse" ? "toolUse" : "stop", message });
          stream.end(); return stream;
        },
      });
    }],
  });
  let session: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
  try {
    await loader.reload();
    expect(loader.getExtensions().errors).toEqual([]);
    const modelRuntime = await ModelRuntime.create({ authPath: path.join(cwd, "auth.json"), modelsPath: null, modelsStorePath: path.join(cwd, "models.json"), refreshOnCreate: false });
    ({ session } = await createAgentSession({ cwd, agentDir: cwd, settingsManager, modelRuntime, resourceLoader: loader, sessionManager: manager }));
    await session.bindExtensions({});
    await session.setModel(modelRuntime.getModel("offline-kept-tail-session", "fixture")!);
    await session.prompt("Run the fixture tool, then answer the ordinary task after recovery.");
    expect(requestKinds).toEqual(["tool-call", "provider-overflow", "native-summary", "retry-answer"]);
    expect(reasons).toEqual(["overflow"]);
    expect(session.getLastAssistantText()).toBe("Recovered answer.");
    const compact = [...manager.getBranch()].reverse().find(entry => entry.type === "compaction");
    expect(compact).toBeDefined();
    if (compact?.type !== "compaction") throw new Error("No saved native-summary checkpoint");
    const rawBranch = manager.getBranch();
    expect(rawBranch.findIndex(entry => entry.id === compact.firstKeptEntryId)).toBeGreaterThan(rawBranch.findIndex(entry => entry.id === giantId));
    expect(compact.firstKeptEntryId).not.toBe(cuts[0]);
    const omission = `[tool reply of ${serializedBytes} bytes from giant_fixture omitted; raw entry ${giantId} kept in the session file]`;
    expect(compact.summary).toContain(ordinarySummary);
    expect(compact.summary).toContain(omission);
    expect(compact.usage).toEqual(usage);
    expect(summaryInputs).toHaveLength(1);
    expect(summaryInputs[0]).toContain(omission);
    expect(summaryInputs[0]).toContain("answer the ordinary task");
    expect(Buffer.byteLength(summaryInputs[0]!, "utf8")).toBeLessThan(16_000);
    expect(retryInputs).toHaveLength(1);
    expect(retryInputs[0]).toContain(JSON.stringify(ordinarySummary).slice(1, -1));
    expect(retryInputs[0]).toContain(omission);
    expect(retryInputs[0]).not.toContain("X".repeat(1000));
    expect(retryInputs[0]).not.toContain("ABANDONED_OVERFLOW_ATTEMPT");
    const finalFile = await readFile(manager.getSessionFile()!);
    expect(prefixBeforeCompaction).toBeDefined();
    expect(finalFile.subarray(0, prefixBeforeCompaction!.length).equals(prefixBeforeCompaction!)).toBe(true);
    expect(manager.getEntry(giantId!)).toMatchObject({ message: { content: projectedContent } });
    const restored = SessionManager.open(manager.getSessionFile()!);
    expect(restored.buildSessionContext()).toEqual(manager.buildSessionContext());
    expect(JSON.stringify(restored.buildSessionContext())).not.toContain("X".repeat(1000));
  } finally {
    session?.dispose();
    await rm(cwd, { recursive: true, force: true });
  }
}, 30_000);

it.each(["auth", "cancel", "summary-error"] as const)("native session does not continue after kept-tail summary %s", async failure => {
  const cwd = await mkdtemp(path.join(os.tmpdir(), "fabric-kept-tail-failure-"));
  const manager = SessionManager.create(cwd, path.join(cwd, "sessions"));
  const settingsManager = SettingsManager.inMemory({ defaultProjectTrust: "never", compaction: { enabled: true, keepRecentTokens: 1000, reserveTokens: 1024 }, retry: { enabled: false } });
  let requests = 0;
  let summaryRequests = 0;
  const loader = new DefaultResourceLoader({
    cwd, agentDir: cwd, settingsManager, noExtensions: true, noSkills: true, noThemes: true, noPromptTemplates: true, agentsFilesOverride: () => ({ agentsFiles: [] }),
    extensionFactories: [pi => {
      pi.registerTool({
        name: "giant_fixture", label: "Giant fixture", description: "Return a large fixture.", parameters: Type.Object({}),
        async execute() { return { content: [{ type: "text", text: "X".repeat(100_000) }], details: undefined }; },
      });
      pi.on("session_before_compact", (event: SessionBeforeCompactEvent, context) => {
        const kept = event.branchEntries.findIndex(entry => entry.id === event.preparation.firstKeptEntryId);
        expect(event.branchEntries.slice(kept).some(entry => entry.type === "message" && entry.message.role === "toolResult")).toBe(true);
        if (failure === "auth") context.modelRegistry.getApiKeyAndHeaders = async () => ({ ok: false, error: "401 no summary auth" });
      });
      registerLazyCompactionHook(pi, { getEngine: () => "pi" });
      pi.registerProvider("offline-kept-tail-failure", {
        api: "offline-kept-tail-failure", apiKey: "offline", baseUrl: "http://invalid.local",
        models: [{ id: "fixture", name: "Fixture", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8_000_000, maxTokens: 2048 }],
        streamSimple(model, context, options) {
          const isSummary = JSON.stringify(context.messages).includes("context summarization assistant");
          if (isSummary) summaryRequests++; else requests++;
          const response: AssistantMessage = {
            role: "assistant", content: [], api: model.api, provider: model.provider, model: model.id, stopReason: "error", timestamp: Date.now(), usage,
            errorMessage: isSummary ? "401 summary denied" : "maximum context length is 16384 tokens; requested 700000 tokens",
          };
          if (!isSummary && requests === 1) {
            response.stopReason = "toolUse";
            response.content = [{ type: "toolCall", id: "giant-call", name: "giant_fixture", arguments: {} }];
          }
          if (isSummary && failure === "cancel") {
            // Native abort propagates the operation signal supplied to compact().
            session!.abortCompaction();
            expect(options?.signal?.aborted).toBe(true);
            response.stopReason = "aborted";
          }
          const stream = createAssistantMessageEventStream();
          if (response.stopReason === "toolUse") stream.push({ type: "done", reason: "toolUse", message: response });
          else stream.push({ type: "error", reason: response.stopReason === "aborted" ? "aborted" : "error", error: response });
          stream.end(); return stream;
        },
      });
    }],
  });
  let session: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
  try {
    await loader.reload();
    const modelRuntime = await ModelRuntime.create({ authPath: path.join(cwd, "auth.json"), modelsPath: null, modelsStorePath: path.join(cwd, "models.json"), refreshOnCreate: false });
    ({ session } = await createAgentSession({ cwd, agentDir: cwd, settingsManager, modelRuntime, resourceLoader: loader, sessionManager: manager }));
    await session.bindExtensions({}); await session.setModel(modelRuntime.getModel("offline-kept-tail-failure", "fixture")!);
    await session.prompt("Continue the ordinary task.");
    expect(requests).toBe(2);
    expect(summaryRequests).toBe(failure === "auth" ? 0 : 1);
    expect(manager.getBranch().some(entry => entry.type === "compaction")).toBe(false);
    expect(manager.getBranch().some(entry => entry.type === "custom" && entry.customType === "fabric.kept-tail-boundary")).toBe(false);
  } finally { session?.dispose(); await rm(cwd, { recursive: true, force: true }); }
}, 30_000);
