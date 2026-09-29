import { execFileSync } from "node:child_process";
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

// smarty-dev#1668: a model put `git worktree add W` (bash) and a durable spawn with cwd W
// (fabric_exec) in one message. Pi runs them in parallel, so the spawn's cwd check ran before
// W existed and failed with ENOENT. The calls stay parallel; the durable spawn waits until the
// sibling's whole `git worktree add` (its checkout included) has finished.
const fabricEntry = path.resolve("dist/index.js");
const built = fs.existsSync(fabricEntry);
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR"] as const;

describe.skipIf(!built || process.platform === "win32")("durable spawn beside the tool call that creates its cwd", () => {
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

  it("spawns only after a parallel sibling git worktree add finishes its checkout", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-seq-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: false }));
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
    sessions.push(session);
    await session.bindExtensions({});
    expect(session.getActiveToolNames()).toEqual(expect.arrayContaining(["bash", "fabric_exec"]));

    // A slow smudge filter keeps the checkout running for 2 s after git creates the directory.
    const repo = path.join(root, "repo");
    const git = (...args: string[]) => execFileSync("git", ["-c", "user.email=t@example.invalid", "-c", "user.name=t", ...args], { cwd: repo });
    fs.mkdirSync(repo);
    git("init", "-q");
    fs.writeFileSync(path.join(repo, ".gitattributes"), "slow.txt filter=slow\n");
    fs.writeFileSync(path.join(repo, "slow.txt"), "complete\n");
    git("add", ".");
    git("commit", "-qm", "init");
    git("config", "filter.slow.smudge", "sleep 2; cat");
    const worktree = path.join(root, "worktree");
    // The real durable path: the provider and ResidencyClient validate cwd, then a resident host
    // starts and publishes the agent. Before the fix this returned the #1668 ENOENT.
    const code = `try { await agents.spawn({ task: "noop", residency: "durable", cwd: ${JSON.stringify(worktree)}, transport: "process" }); return "spawned " + Date.now(); } catch (e: any) { return String(e?.message ?? e); }`;
    faux.setResponses([
      fauxAssistantMessage([
        fauxToolCall("bash", { command: `cd ${JSON.stringify(repo)} && git worktree add -q -b wt ${JSON.stringify(worktree)} && date +%s%3N` }),
        fauxToolCall("fabric_exec", { code }),
      ]),
      fauxAssistantMessage("done"),
    ]);
    await session.prompt("create the worktree and spawn in it");

    const results = session.messages
      .filter((message) => message.role === "toolResult")
      .map((message) => {
        const result = message as { toolName?: string; content?: Array<{ type: string; text?: string }> };
        return [result.toolName, (result.content ?? []).map((part) => part.text ?? "").join("")] as const;
      });
    expect(results.map(([name]) => name)).toEqual(["bash", "fabric_exec"]);
    const addedAt = Number(results[0]![1].trim());
    expect(results[1]![1]).toMatch(/^spawned \d+$/);
    expect(Number(results[1]![1].split(" ")[1])).toBeGreaterThanOrEqual(addedAt);
    expect(fs.readFileSync(path.join(worktree, "slow.txt"), "utf8")).toBe("complete\n");
  }, 120_000);
});
