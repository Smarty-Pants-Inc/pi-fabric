import { execFileSync, spawn } from "node:child_process";
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

// smarty-dev#883: the durable spawn starts detached processes that write under
// root until they exit (ENOTEMPTY on CI). Cleanup stops only processes this
// fixture's own records name, never by matching command lines (#146 security
// review): the resident host (owner.json), its launcher (the host's parent,
// which appends a child-exit trace after the host exits, #145), and each
// durable process worker (its run record's sessionId, which outlives the host).
type Owned = { pid: number; started: string };
const ps = (field: string, pid: number): string => {
  try { return execFileSync("ps", ["-o", `${field}=`, "-p", String(pid)], { encoding: "utf8" }).trim(); } catch { return ""; }
};
// A pid plus its start time names one process; a recycled pid does not match.
const own = (pid: number): Owned | undefined => {
  if (!Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid) return undefined;
  const started = ps("lstart", pid);
  return started ? { pid, started } : undefined;
};
const same = (owned: Owned): boolean => ps("lstart", owned.pid) === owned.started;
// The processes remove run directories while this walks, so a vanished
// directory or file is skipped rather than failing the walk.
const records = (dir: string): Array<{ file: string; value: Record<string, unknown> }> => {
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return []; }
  return entries.flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return records(full);
    if (!entry.name.endsWith(".json")) return [];
    try { return [{ file: entry.name, value: JSON.parse(fs.readFileSync(full, "utf8")) }]; } catch { return []; }
  });
};
const fixtureProcesses = (root: string): { hosts: Owned[]; others: Owned[] } => {
  const found = records(root);
  const hosts = found.filter(({ file }) => file === "owner.json")
    .flatMap(({ value }) => own(Number(value.pid)) ?? []);
  const launchers = hosts.flatMap((host) => own(Number(ps("ppid", host.pid))) ?? []);
  const workers = found.filter(({ value }) => value.transport === "process" && typeof value.sessionId === "string")
    .flatMap(({ value }) => own(Number(value.sessionId)) ?? []);
  const seen = new Set(hosts.map((host) => host.pid));
  const others = [...launchers, ...workers].filter((owned) => !seen.has(owned.pid) && seen.add(owned.pid));
  return { hosts, others };
};
// SIGTERM lets each close and release its files. Wait for the exit; SIGKILL
// only if it hangs, and only while the pid still names the same process.
const stopOwned = async (owned: Owned): Promise<void> => {
  if (!same(owned)) return;
  try { process.kill(owned.pid, "SIGTERM"); } catch { return; }
  const killAt = Date.now() + 30_000;
  while (same(owned)) {
    if (Date.now() > killAt) { try { process.kill(owned.pid, "SIGKILL"); } catch { /* It exited. */ } }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
};
const stopFixture = async ({ hosts, others }: ReturnType<typeof fixtureProcesses>): Promise<void> => {
  await Promise.all([...hosts, ...others].map(stopOwned));
};

describe.skipIf(!built || process.platform === "win32")("durable spawn beside the tool call that creates its cwd", () => {
  const roots: string[] = [];
  const sessions: AgentSession[] = [];
  const saved = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
  afterEach(async () => {
    // Snapshot first, while owner.json still names the host.
    const started = roots.map(fixtureProcesses);
    for (const session of sessions.splice(0)) session.dispose();
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    // Stop and wait for them before removing root; the retry only covers the
    // filesystem settling after their exits.
    await Promise.all(started.map(stopFixture));
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }, 60_000);

  it("cleanup stops only processes its records name, not one whose argv names root", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-seq-")));
    roots.push(root);
    const idle = ["-e", "setInterval(() => {}, 1000)"];
    const host = spawn(process.execPath, idle, { stdio: "ignore" });
    const bystander = spawn(process.execPath, [...idle, root], { stdio: "ignore" });
    const exited = (child: typeof host) => new Promise<void>((resolve) => {
      if (child.exitCode !== null || child.signalCode !== null) resolve(); else child.once("exit", () => resolve());
    });
    try {
      fs.mkdirSync(path.join(root, "residency"));
      fs.writeFileSync(path.join(root, "residency", "owner.json"), JSON.stringify({ pid: host.pid }));
      const owned = fixtureProcesses(root);
      expect(owned.hosts.map((entry) => entry.pid)).toEqual([host.pid]);
      // The fake host's parent is this test process, which is never a target.
      expect(owned.others).toEqual([]);
      await stopFixture(owned);
      await exited(host);
      expect(bystander.exitCode).toBeNull();
      expect(bystander.signalCode).toBeNull();
    } finally {
      host.kill("SIGKILL");
      bystander.kill("SIGKILL");
      await Promise.all([exited(host), exited(bystander)]);
    }
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
    // The durable spawn started a resident host under its launcher; afterEach
    // stops them and the worker by these records.
    const owned = fixtureProcesses(root);
    expect(owned.hosts.length).toBeGreaterThan(0);
    expect(owned.others.length).toBeGreaterThan(0);
  }, 120_000);
});
