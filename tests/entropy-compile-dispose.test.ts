import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

// smarty-dev#8881: AgentSession.dispose() invalidates extension contexts but emits no session_shutdown
// (pi-coding-agent docs/sdk.md). A fabric_exec turn schedules Fabric's entropy compile 250 ms after
// turn_end; disposing the session inside that delay must stop it, so a stale context never runs in the
// next session (here: the next test case's PI_CODING_AGENT_DIR, as in worker-reply-tool-session).
const fabricEntry = path.resolve("dist/index.js");
const built = fs.existsSync(fabricEntry);
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR"] as const;

describe.skipIf(!built)("entropy compile after session dispose (#8881)", () => {
  const roots: string[] = [];
  const saved = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  const start = async (fakeTimers = false) => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-entropy-dispose-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    const nextAgentDir = path.join(root, "next-agent");
    fs.mkdirSync(agentDir, { recursive: true });
    fs.mkdirSync(nextAgentDir, { recursive: true });
    process.env.PI_FABRIC_MESH_ROOT = path.join(root, "mesh");
    process.env.PI_CODING_AGENT_DIR = agentDir;

    const faux = fauxProvider({ tokensPerSecond: 1_000 });
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
    await session.bindExtensions({});
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })),  // entropy evidence: compile scheduled
      fauxAssistantMessage("done"),
    ]);
    const warn = vi.spyOn(console, "warn");
    if (!fakeTimers) {
      await session.prompt("work");
      return { session, agentDir, nextAgentDir, warn, elapsedMs: 0 };
    }
    // Fake setTimeout only (file I/O and setImmediate stay real): the run advances in 1 ms steps, so the delayed
    // compile's clock is exactly what the run used, however slow the runner (#910 review).
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let done = false, elapsedMs = 0;
    const running = session.prompt("work").finally(() => { done = true; });
    while (!done && elapsedMs < 60_000) { await vi.advanceTimersByTimeAsync(1); elapsedMs++; }
    await running;
    return { session, agentDir, nextAgentDir, warn, elapsedMs };
  };
  const compileWarnings = (warn: { mock: { calls: unknown[][] } }): string[] =>
    warn.mock.calls.map((call: unknown[]) => String(call[0])).filter((line: string) => line.includes("entropy compile"));
  const files = (dir: string): string[] => fs.readdirSync(dir, { recursive: true }).map(String).sort();

  it("a live session still compiles into its own agent dir", async () => {
    const { session, agentDir, warn } = await start();
    const before = files(agentDir);
    try {
      await vi.waitFor(() => expect(files(agentDir).length).toBeGreaterThan(before.length), { timeout: 20_000, interval: 100 });
      expect(compileWarnings(warn)).toEqual([]);
    } finally { session.dispose(); }
  }, 60_000);

  it("a session disposed inside the compile delay runs no compile and touches no later agent dir", async () => {
    const { session, nextAgentDir, warn, elapsedMs } = await start(true);
    expect(elapsedMs).toBeLessThan(250);                  // the compile's 250 ms delay has not run out yet
    expect(compileWarnings(warn)).toEqual([]);
    session.dispose();                                    // no session_shutdown, inside the 250 ms delay
    process.env.PI_CODING_AGENT_DIR = nextAgentDir;      // the next session's agent dir
    await vi.advanceTimersByTimeAsync(5_000);             // well past the delay: a surviving compile runs now
    await new Promise((resolve) => setImmediate(resolve));
    expect(compileWarnings(warn)).toEqual([]);
    expect(fs.readdirSync(nextAgentDir)).toEqual([]);   // nothing read-then-written for the next session
  }, 60_000);
});
