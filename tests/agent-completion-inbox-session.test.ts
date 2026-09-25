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
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { AGENT_COMPLETION_MESSAGE_TYPE, AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import type { AgentRunResult } from "../src/agents/types.js";

const roots: string[] = [];
const sessions: AgentSession[] = [];
afterEach(() => {
  for (const session of sessions.splice(0)) session.dispose();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const result = (id: string): AgentRunResult => ({
  id, name: `worker ${id}`, status: "completed", text: `result ${id}`, startedAt: 1, finishedAt: 2,
  task: "work", runner: "pi", transport: "process", cwd: ".", updatedAt: 2, turns: 1, toolCalls: 0,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});
const waitFor = async (predicate: () => boolean, timeoutMs = 5_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

// smarty-dev#733 with the real Pi session: a run started by a triggered custom message (how peer
// delivery and voice start Main) goes sendCustomMessage -> _runAgentPrompt, which emits neither
// input nor before_agent_start. Results parked after an abort must still reach that run.
describe("completion inbox in a real Pi session", () => {
  it("parks results after an abort, starts no run for them, and delivers them into a custom-message run", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-inbox-session-"));
    roots.push(root);
    const faux = fauxProvider({ tokensPerSecond: 40 });
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    let inbox: AgentCompletionInbox | undefined;
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir: path.join(root, "agent"), noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [{
        name: "completion-inbox",
        factory: (pi: ExtensionAPI) => {
          pi.on("session_start", (_event, ctx) => { inbox = new AgentCompletionInbox(pi, ctx); });
        },
      }],
    });
    await loader.reload();
    const { session } = await createAgentSession({
      cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.inMemory(root), noTools: "all",
    });
    sessions.push(session);
    await session.bindExtensions({});                              // emits session_start, as the CLI does
    await waitFor(() => inbox !== undefined);
    const completions = () => session.messages.filter((message) =>
      message.role === "custom" && (message as { customType?: string }).customType === AGENT_COMPLETION_MESSAGE_TYPE);

    // 1. A prompt whose turn is aborted (Esc): the inbox suspends.
    faux.setResponses([fauxAssistantMessage("a long answer ".repeat(200))]);
    const prompted = session.prompt("start");
    await waitFor(() => session.isStreaming);
    await session.abort();
    await prompted.catch(() => undefined);
    await waitFor(() => !session.isStreaming);
    // 2. A detached result arrives while Main is idle: no run is started for it.
    inbox!.enqueue(result("a"));
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(session.isStreaming).toBe(false);
    expect(completions()).toEqual([]);
    // 3. A peer message starts a run the way peer delivery and voice do.
    faux.setResponses([fauxAssistantMessage("peer handled"), fauxAssistantMessage("results noted")]);
    await session.sendCustomMessage({ customType: "pi-fabric-agent-message", content: "from a peer", display: true }, { triggerTurn: true });
    await waitFor(() => completions().length === 1 && !session.isStreaming);
    expect(JSON.stringify(completions()[0])).toContain("worker a (a) completed");
  }, 30_000);
});
