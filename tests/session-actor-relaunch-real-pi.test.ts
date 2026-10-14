import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { SESSION_ACTOR_ORPHAN_GRACE_MS } from "../src/actors/session-orphans.js";

// smarty-dev#9241 REAL path: installed Pi 0.87 RPC hosts running the compiled dist/ bundle.
// Main A creates a session actor and quits; while A is down a peer Main B, on the same mesh,
// retires A's session actors as orphans (the real presence-pass/actors reconciler). A then
// relaunches with the SAME session id. Run: bun run build && FABRIC_9241_REAL_PI=1 bunx vitest run
// tests/session-actor-relaunch-real-pi.test.ts (FABRIC_9241_PI_CLI selects another Pi cli.js).
type Event = Parameters<Parameters<RpcClient["onEvent"]>[0]>[0];
const repo = path.resolve(".");
const cli = process.env.FABRIC_9241_PI_CLI ?? path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const evidence = process.env.FABRIC_9241_EVIDENCE_DIR;

it.runIf(process.env.FABRIC_9241_REAL_PI === "1")("a same-session relaunch restarts host-exit-stopped session actors; an explicit stop stays stopped", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-real-9241-"));
  const mesh = path.join(root, "mesh");
  const sessions = { A: randomUUID(), B: randomUUID() };
  const clients: Record<string, RpcClient | undefined> = {};
  const stderr: string[] = [];
  const events: Event[] = [];
  const wait = async (check: () => boolean | Promise<boolean>, timeout = 30_000, interval = 100) =>
    vi.waitFor(async () => expect(await check()).toBe(true), { timeout, interval });
  const dir = (name: "A" | "B") => path.join(root, name);
  const ready = (name: "A" | "B") => JSON.parse(fs.readFileSync(path.join(dir(name), "ready"), "utf8"));
  const start = async (name: "A" | "B") => {
    const cwd = dir(name), agentDir = path.join(cwd, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      executor: { kernel: "typescript" }, fullCodeMode: false, mesh: { enabled: true, announce: true },
      mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
      compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
    }));
    const client = new RpcClient({ cliPath: cli, cwd, provider: "relaunch-9241", model: "faux-1",
      env: { PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(cwd, "exports"), PI_FABRIC_MESH_ROOT: mesh,
        PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(cwd, "runs"), FABRIC_9241_GATE_DIR: cwd,
        PI_FABRIC_PI_BINARY: path.join(repo, "tests/fixtures/actor-child-pi-9241.mjs"),
        HERDR_PANE_ID: "", HERDR_SOCKET_PATH: "", PI_OFFLINE: "1" },
      args: ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve", "--no-builtin-tools",
        "--thinking", "off", "--session-dir", path.join(cwd, "sessions"), "--session-id", sessions[name],
        "-e", path.join(repo, "dist/index.js"), "-e", path.join(repo, "tests/fixtures/session-actor-relaunch-pi.ts")] });
    client.onEvent(event => events.push(event));
    clients[name] = client;
    await client.start();
    const pid = (client as unknown as { process: { pid: number } }).process.pid;
    await wait(() => fs.existsSync(path.join(cwd, "ready")) && ready(name).pid === pid);
    return client;
  };
  // Pi's own clean exit (stdin EOF runs session_shutdown), not a SIGKILL.
  const quit = async (name: "A" | "B") => {
    const client = clients[name]!;
    const child = (client as unknown as { process: import("node:child_process").ChildProcess }).process;
    const exited = new Promise<number | null>(resolve => child.once("exit", code => resolve(code)));
    child.stdin!.end();
    expect(await Promise.race([exited, new Promise(resolve => setTimeout(() => resolve("timeout"), 60_000))])).toBe(0);
    stderr.push(`--- ${name}\n${client.getStderr()}`);
    await client.stop();
    clients[name] = undefined;
  };
  const exec = async <T>(name: "A" | "B", code: string): Promise<T> => {
    const result = await clients[name]!.promptAndWait(`EXEC ${code}`, undefined, 60_000);
    const ended = result.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec") as
      { isError: boolean; result: { content: Array<{ text: string }> } } | undefined;
    expect(ended, JSON.stringify(result.slice(-3))).toMatchObject({ isError: false });
    return JSON.parse(ended!.result.content.map(part => part.text).join(""));
  };
  type Actor = { id: string; name: string; status: string; topics: string[]; activationFilter?: unknown; instructionsDigest: string;
    stopReason?: string; sessionOrphan?: unknown; lastError?: string };
  const actors = (name: "A" | "B") => exec<Actor[]>(name, "return await agents.actors();");
  const replies = (id: string) => exec<Array<{ direction?: string; text?: string }>>("A", `return await agents.messages({ id: ${JSON.stringify(id)}, limit: 50 });`);
  const registry = () => JSON.parse(fs.readFileSync(path.join(mesh, "actors", sessions.A, "actors.json"), "utf8")).actors as Array<Record<string, unknown>>;
  const handled = async (id: string, marker: string) => wait(async () =>
    (await replies(id)).some(message => JSON.stringify(message).includes(`handled ${marker}`)), 60_000, 1_000);
  const filter = [{ id: "skip-noise", topic: ["fleet.noise9241"] }];
  let outcome: Record<string, unknown> = { passed: false };
  try {
    await Promise.all([start("A"), start("B")]);
    const supervisor = await exec<Actor>("A", `return await agents.createActor({ name: "relaunch-sup", scope: "session", residency: "session",
      instructions: "Supervise this Main", topics: ["fleet.relaunch9241", "fleet.noise9241"], activationFilter: ${JSON.stringify(filter)} });`);
    const stopped = await exec<Actor>("A", 'return await agents.createActor({ name: "relaunch-stopped", scope: "session", residency: "session", instructions: "Stay stopped" });');
    expect(supervisor).toMatchObject({ status: "idle", scope: "session", residency: "session" });
    await exec("A", `return await agents.tell({ id: ${JSON.stringify(supervisor.id)}, message: "TELL-9241-BEFORE" });`);
    await handled(supervisor.id, "TELL-9241-BEFORE");
    await exec("A", `return await agents.stop({ id: ${JSON.stringify(stopped.id)} });`);
    const before = Object.fromEntries((await actors("A")).map(actor => [actor.id, actor]));
    expect(before[stopped.id]).toMatchObject({ status: "stopped" });
    const cursorFile = path.join(mesh, "actors", sessions.A, "mesh-cursor.json.session");
    await quit("A");
    const quitAt = Date.now();
    const cursorSequence = (): number => fs.existsSync(cursorFile) ? JSON.parse(fs.readFileSync(cursorFile, "utf8")).last?.sequence ?? 0 : 0;
    const cursorAtQuit = cursorSequence();
    // The real reconciler retires A's session actors only after its restart/reload grace.
    await new Promise(resolve => setTimeout(resolve, Math.max(0, quitAt + SESSION_ACTOR_ORPHAN_GRACE_MS + 2_000 - Date.now())));
    await wait(async () => {
      await actors("B");
      return registry().find(row => row.id === supervisor.id)?.status === "stopped";
    }, 120_000, 3_000);
    const orphaned = registry().find(row => row.id === supervisor.id)!;
    expect(orphaned).toMatchObject({ status: "stopped", sessionOrphan: { oldRoot: `session:${sessions.A}` } });
    expect(registry().find(row => row.id === stopped.id)).toMatchObject({ status: "stopped" });
    expect(registry().find(row => row.id === stopped.id)?.sessionOrphan).toBeUndefined();

    await start("A");
    expect(ready("A")).toMatchObject({ id: `session:${sessions.A}`, reason: "startup" });
    const after = Object.fromEntries((await actors("A")).map(actor => [actor.id, actor]));
    expect(after[supervisor.id]).toMatchObject({ id: supervisor.id, name: "relaunch-sup", status: "idle",
      topics: supervisor.topics, activationFilter: filter, instructionsDigest: supervisor.instructionsDigest });
    expect(after[supervisor.id]?.sessionOrphan).toBeUndefined();
    expect(after[stopped.id]).toMatchObject({ id: stopped.id, status: "stopped", stopReason: "explicit" });
    // The restart resumes the session mesh cursor; it does not rewind it.
    expect(cursorSequence()).toBeGreaterThanOrEqual(cursorAtQuit);
    const told = await exec<{ queued: boolean }>("A", `return await agents.tell({ id: ${JSON.stringify(supervisor.id)}, message: "TELL-9241-AFTER" });`);
    expect(told).toMatchObject({ queued: true });
    await handled(supervisor.id, "TELL-9241-AFTER");
    const persisted = registry().find(row => row.id === supervisor.id)!;
    expect(persisted.status).not.toBe("stopped");
    expect(persisted.sessionOrphan).toBeUndefined();
    // An explicitly stopped actor stays stopped; its owning Main removes it.
    await expect(exec("A", `return await agents.tell({ id: ${JSON.stringify(stopped.id)}, message: "TELL-9241-STOPPED" });`)).rejects.toThrow();
    if (process.platform === "linux") {
      expect(await exec("A", `return await agents.remove({ id: ${JSON.stringify(stopped.id)} });`)).toMatchObject({ removed: true });
      expect((await actors("A")).map(actor => actor.id)).not.toContain(stopped.id);
    } else {
      // smarty-dev#11335: without a Main publication fence, removal is accepted but keeps the actor tree pending.
      const stoppedDir = path.join(mesh, "actors", sessions.A, stopped.id);
      fs.mkdirSync(stoppedDir, { recursive: true });
      const sentinel = path.join(stoppedDir, "kept-11335.txt");
      fs.writeFileSync(sentinel, "stopped history\n");
      const pending = `removal cleanup failed: Root actor tree deletion needs the Main publication fence, which this platform (${process.platform}) lacks (smarty-dev#8090); follow-up smarty-dev#11335; the root session:${sessions.A} actor tree is kept (a later remove or host start finishes it)`;
      expect(await exec("A", `return await agents.remove({ id: ${JSON.stringify(stopped.id)} });`)).toMatchObject({ removed: true, cleaned: false, pending });
      expect(await actors("A")).toContainEqual(expect.objectContaining({ id: stopped.id, status: "stopped", removal: expect.objectContaining({ state: pending }) }));
      expect(fs.readFileSync(sentinel, "utf8")).toBe("stopped history\n");
    }
    outcome = { passed: true, session: sessions.A, supervisor: supervisor.id, orphaned: orphaned.sessionOrphan,
      relaunched: after[supervisor.id]?.status, explicit: after[stopped.id]?.stopReason, cli: fs.realpathSync(cli) };
  } finally {
    for (const name of ["A", "B"] as const) {
      const client = clients[name];
      if (!client) continue;
      stderr.push(`--- ${name}\n${client.getStderr()}`);
      await client.stop().catch(() => undefined);
    }
    if (evidence) {
      fs.mkdirSync(evidence, { recursive: true });
      fs.writeFileSync(path.join(evidence, "outcome.json"), JSON.stringify(outcome, null, 2));
      fs.writeFileSync(path.join(evidence, "stderr.log"), stderr.join("\n"));
      const file = path.join(mesh, "actors", sessions.A, "actors.json");
      if (fs.existsSync(file)) fs.copyFileSync(file, path.join(evidence, "A-actors.json"));
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 420_000);
