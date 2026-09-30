import { execFileSync, spawn, type ChildProcess } from "node:child_process";
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
import { launchLog, same, startTime, stopAllOwned, stopOwned, type LaunchLog } from "./helpers/owned-processes.js";

// smarty-dev#1668: a model put `git worktree add W` (bash) and a durable spawn with cwd W
// (fabric_exec) in one message. Pi runs them in parallel, so the spawn's cwd check ran before
// W existed and failed with ENOENT. The calls stay parallel; the durable spawn waits until the
// sibling's whole `git worktree add` (its checkout included) has finished.
const fabricEntry = path.resolve("dist/index.js");
const built = fs.existsSync(fabricEntry);
const ENV_KEYS = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR", "NODE_OPTIONS", "PI_FABRIC_TEST_LAUNCH_LOG"] as const;

// smarty-dev#883: the durable spawn starts detached processes that write under
// root until they exit (ENOTEMPTY on CI). Cleanup stops only processes that
// recorded themselves at launch in this fixture's launch log (the launcher, the
// resident host and each durable worker inherit its preload), never by command
// line, current parentage or a pid a record names later (pi-fabric#146, #160 S4).
const launchLogs = new Map<string, LaunchLog>();
const cleanupRoot = async (root: string): Promise<void> => {
  const log = launchLogs.get(root);
  launchLogs.delete(root);
  if (log) await stopAllOwned(log.owned());
};
const idle = ["-e", "setInterval(() => {}, 1000)"];
const exited = (child: ChildProcess) => new Promise<void>((resolve) => {
  if (child.exitCode !== null || child.signalCode !== null) resolve(); else child.once("exit", () => resolve());
});
const alive = (child: ChildProcess): boolean => child.exitCode === null && child.signalCode === null;
const tempRoot = (roots: string[]): string => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-seq-")));
  roots.push(root);
  return root;
};

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
    // Stop and wait for them before removing root; the retry only covers the
    // filesystem settling after their exits.
    await Promise.all(roots.map(cleanupRoot));
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }, 60_000);

  it("cleanup stops only processes that recorded their own launch: no argv match, no parent", async () => {
    const root = tempRoot(roots);
    const log = launchLog(root);
    launchLogs.set(root, log);
    const host = spawn(process.execPath, idle, { stdio: "ignore", env: { ...process.env, ...log.env } });
    // Its argv names root, but it never recorded a launch.
    const bystander = spawn(process.execPath, [...idle, root], { stdio: "ignore" });
    // An unowned process that starts an owned child: the child's current parent (as for a host
    // adopted by a subreaper) is never taken as the launcher.
    const adopter = spawn(process.execPath, ["-e", `
      const env = { ...process.env, ...JSON.parse(process.argv[1]) };
      require("node:child_process").spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore", env });
      setInterval(() => {}, 1000);`, JSON.stringify(log.env)], { stdio: "ignore" });
    try {
      fs.mkdirSync(path.join(root, "residency"));
      fs.writeFileSync(path.join(root, "residency", "owner.json"), JSON.stringify({ pid: bystander.pid }));
      const deadline = Date.now() + 10_000;
      while (log.owned().length < 2 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 20));
      const owned = log.owned();
      expect(owned).toHaveLength(2);
      expect(owned.map((entry) => entry.pid)).toContain(host.pid);
      expect(owned.map((entry) => entry.pid)).not.toContain(bystander.pid);
      expect(owned.map((entry) => entry.pid)).not.toContain(adopter.pid);
      await cleanupRoot(root);
      await exited(host);
      expect(owned.every((entry) => !same(entry))).toBe(true);
      expect(alive(bystander)).toBe(true);
      expect(alive(adopter)).toBe(true);
    } finally {
      for (const child of [host, bystander, adopter]) child.kill("SIGKILL");
      await Promise.all([exited(host), exited(bystander), exited(adopter)]);
    }
  });

  it("a stale record that names a bystander at the first snapshot is not owned (pi-fabric#146)", async () => {
    const root = tempRoot(roots);
    const bystander = spawn(process.execPath, idle, { stdio: "ignore" });
    try {
      const pid = bystander.pid!;
      const started = startTime(pid);
      expect(started).not.toBe("");
      const record = (at: number, value = started) => `${JSON.stringify({ pid, started: value, at, argv: [] })}\n`;
      // Recorded 10 s before the process now holding that pid started: the pid was reused.
      const late = launchLog(root, Date.now() - 20_000);
      fs.writeFileSync(late.file, record(Date.now() - 10_000));
      expect(late.owned()).toEqual([]);
      // The process predates the fixture, though the record's time is plausible.
      const early = launchLog(root, Date.now() + 10_000);
      fs.writeFileSync(early.file, record(Date.now() + 10_000));
      expect(early.owned()).toEqual([]);
      launchLogs.set(root, early);
      await cleanupRoot(root);
      // A launch record whose start time the pid no longer has is not signalled either.
      expect(await stopOwned({ pid, started: `${started}0`, at: Date.now(), argv: [] })).toBeUndefined();
      expect(alive(bystander)).toBe(true);
      expect(startTime(pid)).toBe(started);
    } finally {
      bystander.kill("SIGKILL");
      await exited(bystander);
    }
  });

  it("spawns only after a parallel sibling git worktree add finishes its checkout", async () => {
    const root = tempRoot(roots);
    // Before anything starts: the launcher, resident host and worker inherit this and record
    // themselves at launch; cleanup stops only those.
    const log = launchLog(root);
    launchLogs.set(root, log);
    Object.assign(process.env, log.env);
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
    // The durable spawn started a launcher, a resident host and a worker; each recorded its own
    // launch, so afterEach stops all three. Their records here only cross-check that.
    const files = (dir: string): string[] => fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
      entry.isDirectory() ? files(path.join(dir, entry.name)) : [path.join(dir, entry.name)]);
    const all = files(path.join(root, "mesh"));
    const json = (file: string): Record<string, unknown> => { try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return {}; } };
    const launcherPids = all.filter((file) => path.basename(file) === "launcher.log")
      .flatMap((file) => fs.readFileSync(file, "utf8").split("\n").flatMap((line) => {
        try { const event = JSON.parse(line); return event.event === "launcher-started" ? [Number(event.pid)] : []; } catch { return []; }
      }));
    const hostPids = all.filter((file) => path.basename(file) === "owner.json").map((file) => Number(json(file).pid));
    // The resident host's durable-agent metadata keeps each process worker's launch handle.
    const workerPids = all.filter((file) => path.basename(path.dirname(file)) === "agents").map((file) => json(file).handle)
      .flatMap((handle) => {
        const { transport, sessionId } = (handle ?? {}) as { transport?: unknown; sessionId?: unknown };
        return transport === "process" && typeof sessionId === "string" ? [Number(sessionId)] : [];
      });
    // A worker records itself once its runtime loads, which can be after the spawn returned.
    const expected = [...launcherPids, ...hostPids, ...workerPids];
    const deadline = Date.now() + 10_000;
    let owned = log.owned().map((entry) => entry.pid);
    while (!expected.every((pid) => owned.includes(pid)) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
      owned = log.owned().map((entry) => entry.pid);
    }
    for (const pids of [launcherPids, hostPids, workerPids]) {
      expect(pids.length).toBeGreaterThan(0);
      expect(owned).toEqual(expect.arrayContaining(pids));
    }
  }, 120_000);
});
