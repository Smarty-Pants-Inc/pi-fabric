import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { acquireHostActivation } from "../src/agents/transports/host-activation.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { resolveScriptRuntime } from "../src/agents/transports/process-utils.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentTransportHandle, AgentTransportLaunch, HostActivationQueue } from "../src/agents/types.js";
import { runHostActivationProof } from "./helpers/host-activation-proof.js";

const roots: string[] = [];
const handles: AgentTransportHandle[] = [];
const fds: number[] = [];
const managers: AgentManager[] = [];
const actors: ActorManager[] = [];
const children: { child: ChildProcess; closed: Promise<void> }[] = [];
const wait = async (predicate: () => boolean, timeout = 10_000) => {
  const until = Date.now() + timeout;
  while (!predicate()) { if (Date.now() > until) throw new Error("Host cap test timed out"); await new Promise(resolve => setTimeout(resolve, 15)); }
};
const closeFd = (fd: number) => { fs.closeSync(fd); fds.splice(fds.indexOf(fd), 1); };
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-host-cap-")); roots.push(dir); return dir; };
const fixture = () => {
  const cwd = root(); const directory = path.join(cwd, "tokens"); const workerPath = path.join(cwd, "worker.mjs");
  fs.writeFileSync(workerPath, 'import fs from "node:fs"; fs.appendFileSync("starts", JSON.stringify({pid:process.pid,args:process.argv,fd:fs.readlinkSync("/proc/self/fd/3")})+"\\n"); setInterval(()=>{},1000);');
  const request: AgentTransportLaunch = { id: "probe", name: "probe", cwd, workerPath, workerArguments: ["--actor-id", "actor"] };
  const transport = new ProcessTransport(undefined, { limit: 1, directory });
  const launch = async (req: AgentTransportLaunch = request) => { const handle = await transport.launch(req); handles.push(handle); return handle; };
  const queue = (): { id: string; activationId: string; sequence: number }[] => JSON.parse(fs.readFileSync(path.join(directory, "queue.json"), "utf8"));
  return { cwd, directory, workerPath, request, transport, launch, queue };
};

afterEach(async () => {
  await Promise.all(actors.splice(0).map(manager => manager.close()));
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(handles.splice(0).map(async handle => { await handle.stop(); await handle.waitForClose?.(); }));
  for (const entry of children.splice(0)) { if (entry.child.exitCode === null && entry.child.signalCode === null) entry.child.kill("SIGKILL"); await entry.closed; }
  for (const fd of fds.splice(0)) fs.closeSync(fd);
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe.skipIf(process.platform !== "linux")("host-wide activation admission (#4444)", () => {
  it.each([1, 2])("caps 12 activations across 3 Main processes at %s, with cross-process FIFO at one", async limit => {
    const proof = await runHostActivationProof(root(), limit);
    expect(proof).toMatchObject({ processes: 3, activations: 12, completed: 12, maxConcurrent: limit, queuedBeforeRelease: 12, hostQueueStatuses: 12 });
    if (limit === 1) expect(proof.fifo).toBe(true);
  }, 60_000);

  it("rejects host-engine dependencies without an enclosing execution fence before releasing custody", async () => {
    const { yieldHostActivation } = await import("../src/agents/transports/host-activation-yield.js");
    // There is deliberately no FD 3 or host token here: rejection must precede
    // descriptor access, ticket creation, or dependency launch.
    await expect(yieldHostActivation("detached-host-engine")).rejects.toThrow("enclosing fabric_exec");
  });

  it("cold import and idle registration do not create host queue or token files", async () => {
    const cwd = root(); const directory = path.join(cwd, "idle-tokens");
    const runtime = await resolveScriptRuntime({ requireBun: true });
    const source = path.join(cwd, "idle.ts");
    fs.writeFileSync(source, `import fs from "node:fs"; import { ProcessTransport } from ${JSON.stringify(path.resolve("src/agents/transports/process-transport.ts"))}; const transport = new ProcessTransport(undefined,{limit:4,directory:${JSON.stringify(directory)}}); await transport.available(); await new Promise(resolve=>setTimeout(resolve,100)); if(fs.existsSync(${JSON.stringify(directory)})) throw new Error("Idle host cap performed IO");`);
    const child = spawn(runtime, [source], { stdio: "ignore" });
    const closed = new Promise<void>(resolve => child.once("close", () => resolve())); children.push({ child, closed });
    await closed; expect(child.exitCode).toBe(0); expect(fs.existsSync(directory)).toBe(false);
  });

  it("worker crash releases a token, and waiting is async", async () => {
    const f = fixture(); const first = await f.launch();
    await wait(() => fs.existsSync(path.join(f.cwd, "starts")));
    let queued: HostActivationQueue | undefined;
    const next = f.launch({ ...f.request, id: "next", onHostQueue: value => { queued = value; } });
    await wait(() => queued?.position === 1);
    expect(queued).toMatchObject({ position: 1, limit: 1 });
    let eventLoopTicked = false; await new Promise<void>(resolve => setTimeout(() => { eventLoopTicked = true; resolve(); }, 10));
    expect(eventLoopTicked).toBe(true);
    expect(f.queue().map(ticket => ticket.activationId)).toEqual(["next"]);
    process.kill(Number(first.sessionId), "SIGKILL"); await first.waitForClose?.();
    const second = await next; expect(Number(second.sessionId)).not.toBe(Number(first.sessionId)); expect(queued).toBeUndefined();
  });

  it("abort removes only its ticket and advances the next FIFO position", async () => {
    const f = fixture(); const first = await f.launch(); const abort = new AbortController();
    let secondQueue: HostActivationQueue | undefined; let thirdQueue: HostActivationQueue | undefined;
    const second = f.launch({ ...f.request, id: "second", signal: abort.signal, onHostQueue: value => { secondQueue = value; } }).then(() => undefined, error => error as Error);
    await wait(() => secondQueue?.position === 1);
    const third = f.launch({ ...f.request, id: "third", onHostQueue: value => { thirdQueue = value; } });
    await wait(() => thirdQueue?.position === 2);
    abort.abort(); expect(await second).toBeInstanceOf(Error);
    expect(secondQueue).toBeUndefined(); await wait(() => thirdQueue?.position === 1);
    expect(f.queue().map(ticket => ticket.activationId)).toEqual(["third"]);
    await first.stop(); await third;
  });

  it("rechecks ownership while waiting and cleans a revoked ticket", async () => {
    const f = fixture(); await f.launch(); let allowed = true;
    const pending = f.launch({ ...f.request, id: "revoked", authorize: () => allowed }).then(() => undefined, error => error as Error);
    await wait(() => f.queue().length === 1); allowed = false;
    expect((await pending)?.message).toContain("authorized"); expect(f.queue()).toEqual([]);
  });

  it("prunes dead PID, reused PID start and old boot tickets, but not a live waiter", async () => {
    const cwd = root(); const directory = path.join(cwd, "tokens"); fs.mkdirSync(directory);
    const boot = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    const base = { id: "stale", activationId: "stale", waitingSince: Date.now(), pid: process.pid, sequence: 1, start: "wrong-start", boot };
    fs.writeFileSync(path.join(directory, "queue.json"), JSON.stringify([base, { ...base, id: "dead", sequence: 2, pid: 2147483647 }, { ...base, id: "reboot", sequence: 3, boot: "old-boot" }]));
    fs.writeFileSync(path.join(directory, "sequence"), "1\n2\n3\n");
    const lease = await acquireHostActivation({ limit: 1, directory }, { id: "fresh" }); fds.push(lease.fd);
    expect(lease.ticket).toBe(4); expect(JSON.parse(fs.readFileSync(path.join(directory, "queue.json"), "utf8"))).toEqual([]);
  });

  it("a killed waiter is removed without starving a surviving process", async () => {
    const f = fixture(); const blocker = await acquireHostActivation({ limit: 1, directory: f.directory }, { id: "block" }); fds.push(blocker.fd);
    const runtime = await resolveScriptRuntime({ requireBun: true });
    const source = path.join(f.cwd, "waiter.ts");
    fs.writeFileSync(source, `import { acquireHostActivation } from ${JSON.stringify(path.resolve("src/agents/transports/host-activation.ts"))}; const token = await acquireHostActivation({limit:1,directory:${JSON.stringify(f.directory)}},{id:"dead-waiter"});`);
    const child = spawn(runtime, [source], { stdio: "ignore" }); const closed = new Promise<void>(resolve => child.once("close", () => resolve())); children.push({ child, closed });
    await wait(() => f.queue().length === 1);
    const surviving = acquireHostActivation({ limit: 1, directory: f.directory }, { id: "survivor" });
    await wait(() => f.queue().length === 2); child.kill("SIGKILL"); await closed;
    await wait(() => f.queue().length === 1); expect(f.queue()[0]?.activationId).toBe("survivor");
    closeFd(blocker.fd); const token = await surviving; fds.push(token.fd); expect(f.queue()).toEqual([]);
  });

  it("launcher crash does not release the still-running worker's inherited token", async () => {
    const f = fixture(); const runtime = await resolveScriptRuntime({ requireBun: true });
    const source = path.join(f.cwd, "launcher.ts"); const ready = path.join(f.cwd, "launched");
    fs.writeFileSync(source, `import fs from "node:fs"; import { ProcessTransport } from ${JSON.stringify(path.resolve("src/agents/transports/process-transport.ts"))}; const h = await new ProcessTransport(undefined,{limit:1,directory:${JSON.stringify(f.directory)}}).launch(${JSON.stringify(f.request)});fs.writeFileSync(${JSON.stringify(ready)},h.sessionId!);setInterval(()=>{},1000);`);
    const child = spawn(runtime, [source], { stdio: "ignore" }); const closed = new Promise<void>(resolve => child.once("close", () => resolve())); children.push({ child, closed });
    await wait(() => fs.existsSync(ready)); const workerPid = Number(fs.readFileSync(ready, "utf8"));
    try {
      child.kill("SIGKILL"); await closed; let queue: HostActivationQueue | undefined;
      const second = f.launch({ ...f.request, id: "after-parent-crash", onHostQueue: value => { queue = value; } });
      await wait(() => queue?.position === 1); expect(f.queue()).toHaveLength(1);
      process.kill(workerPid, "SIGKILL"); await second;
    } finally { try { process.kill(workerPid, "SIGKILL"); } catch { /* already gone */ } }
  });

  it("leaves unset policy and task agents unchanged, but caps tasks when scope is all", async () => {
    const f = fixture(); fs.writeFileSync(f.workerPath, 'setInterval(()=>{},1000);');
    const blocked = await f.launch();
    const taskRequest = { ...f.request, workerArguments: ["--task", "--actor-id"] }; // flag-shaped value is NOT an actor
    const task = await f.transport.launch(taskRequest); handles.push(task);
    expect(await task.isAlive()).toBe(true); expect(f.queue()).toEqual([]);
    const unconfigured = await new ProcessTransport().launch(f.request); handles.push(unconfigured); expect(await unconfigured.isAlive()).toBe(true);
    const abort = new AbortController(); let position = 0;
    const all = new ProcessTransport(undefined, { limit: 1, scope: "all", directory: f.directory }).launch({ ...taskRequest, signal: abort.signal, onHostQueue: value => { position = value?.position ?? 0; } }).then(() => undefined, error => error as Error);
    await wait(() => position === 1); abort.abort(); expect(await all).toBeInstanceOf(Error); expect(await blocked.isAlive()).toBe(true);
  });

  it.each([false, true])("retains tokens across processSlice admission/fallback (fallback=%s)", async fallback => {
    const f = fixture(); const scoped = path.join(f.cwd, "bin"); fs.mkdirSync(scoped);
    fs.writeFileSync(path.join(scoped, "systemd-run"), `#!/bin/sh\n${fallback ? "exit 1" : 'while [ "$1" != "--" ]; do shift; done; shift; exec "$@"'}\n`, { mode: 0o700 });
    vi.stubEnv("PATH", `${scoped}${path.delimiter}${process.env.PATH}`); vi.spyOn(console, "warn").mockImplementation(() => {});
    const handle = await new ProcessTransport("batch.slice", { limit: 1, directory: f.directory }).launch(f.request); handles.push(handle);
    await wait(() => fs.existsSync(path.join(f.cwd, "starts")));
    expect(JSON.parse(fs.readFileSync(path.join(f.cwd, "starts"), "utf8")).fd).toContain(f.directory);
    const abort = new AbortController(); let queued = false;
    const waiter = f.launch({ ...f.request, signal: abort.signal, onHostQueue: value => { queued = !!value; } }).then(() => undefined, error => error as Error);
    await wait(() => queued); abort.abort(); expect(await waiter).toBeInstanceOf(Error);
  });

  it("fails closed if flock is unavailable, and rejects corrupt queue state", async () => {
    const f = fixture(); vi.stubEnv("PATH", f.cwd);
    await expect(f.launch()).rejects.toThrow("requires flock"); expect(fs.existsSync(f.directory)).toBe(false);
    vi.unstubAllEnvs(); fs.mkdirSync(f.directory); fs.writeFileSync(path.join(f.directory, "queue.json"), "broken");
    await expect(f.launch()).rejects.toThrow(); expect(fs.existsSync(path.join(f.cwd, "starts"))).toBe(false);
  });

  it.each(["stop", "close", "abandon"] as const)("revokes an all-scope task relaunch ticket on %s without launching again", async action => {
    const cwd = root(); const directory = path.join(cwd, "tokens");
    const agent = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, hostActivationLimit: 1, hostActivationLimitScope: "all",
      maxConcurrent: 4, budgetUsd: 0, sessionExport: false, timeoutMs: 30_000 }, {
      hostActivationDirectory: directory, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(cwd, "runs"),
    }); managers.push(agent);
    const handle = await agent.spawn({ task: "HANG", transport: "process" });
    const blocking = acquireHostActivation({ limit: 1, directory }, { id: "block-retry" });
    process.kill(Number(handle.sessionId), "SIGKILL");
    const blocker = await blocking; fds.push(blocker.fd);
    const queue = () => JSON.parse(fs.readFileSync(path.join(directory, "queue.json"), "utf8")) as { activationId: string }[];
    await wait(() => queue().some(ticket => ticket.activationId === handle.id));
    if (action === "stop") expect((await agent.stop(handle.id)).status).toBe("stopped");
    else if (action === "close") await agent.close();
    else agent.abandon(handle.id);
    await wait(() => queue().length === 0);
    expect((await agent.wait(handle.id)).status).toBe(action === "abandon" ? "failed" : "stopped");
    // Token is still occupied: cleanup cannot depend on spare capacity.
    expect(fs.fstatSync(blocker.fd).isFile()).toBe(true);
  }, 20_000);

  it.each(["startup-retry", "resume"] as const)("gives a %s worker runtime after host admission exceeds the remaining deadline", async recovery => {
    const cwd = root(); const directory = path.join(cwd, "tokens");
    const target = path.resolve(recovery === "startup-retry" ? "tests/fixtures/fake-worker-startup-retry.mjs" : "tests/fixtures/fake-worker.mjs");
    const marker = recovery === "startup-retry" ? "startup-attempts" : "resume-attempts";
    const workerPath = path.join(cwd, "recovery.mjs");
    fs.writeFileSync(workerPath, `import fs from "node:fs"; import path from "node:path";
const args = new Map(); for(let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
if(fs.existsSync(path.join(path.dirname(args.get("--status-file")),${JSON.stringify(marker)}))) await new Promise(resolve=>setTimeout(resolve,500));
await import(${JSON.stringify(target)});`);
    const agent = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, hostActivationLimit: 1, hostActivationLimitScope: "all",
      maxConcurrent: 4, budgetUsd: 0, sessionExport: false, timeoutMs: 1_000 }, {
      hostActivationDirectory: directory, workerPath, runRoot: path.join(cwd, "runs"),
    }); managers.push(agent);
    const handle = await agent.spawn({ task: recovery === "startup-retry" ? "Recover startup" : "RESUME_AFTER_STOP", transport: "process" });
    const blocker = await acquireHostActivation({ limit: 1, directory }, { id: "other-root" }); fds.push(blocker.fd);
    const queue = () => JSON.parse(fs.readFileSync(path.join(directory, "queue.json"), "utf8")) as { activationId: string }[];
    await wait(() => queue().some(ticket => ticket.activationId === handle.id));
    // Exceeds even the 1s runtime + 1s native-exit grace; attempt two must still
    // receive the unspent runtime, not be started and immediately terminated.
    await new Promise(resolve => setTimeout(resolve, 2_500));
    expect(fs.readFileSync(path.join(agent.runDirectory(handle.id)!, marker), "utf8")).toBe("1");
    closeFd(blocker.fd);
    const result = await agent.wait(handle.id);
    expect(result, JSON.stringify(result)).toMatchObject({ status: "completed", text: recovery === "startup-retry" ? "startup retry recovered" : "resumed attempt 2" });
    expect(fs.readFileSync(path.join(agent.runDirectory(handle.id)!, marker), "utf8")).toBe("2");
  }, 20_000);

  it("rejects a replacement whose non-queue preparation spends the remaining runtime before launch", async () => {
    const cwd = root(); const directory = path.join(cwd, "tokens");
    const agent = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, hostActivationLimit: 1, hostActivationLimitScope: "all",
      maxConcurrent: 4, budgetUsd: 0, sessionExport: false, timeoutMs: 1_000 }, {
      hostActivationDirectory: directory, workerPath: path.resolve("tests/fixtures/fake-worker-startup-retry.mjs"), runRoot: path.join(cwd, "runs"),
    }); managers.push(agent);
    const handle = await agent.spawn({ task: "Recover startup", transport: "process" });
    const prepare = agent.prepareModelForAdmission.bind(agent);
    vi.spyOn(agent, "prepareModelForAdmission").mockImplementation(async (...args) => {
      await new Promise(resolve => setTimeout(resolve, 2_300));
      return prepare(...args);
    });
    const result = await agent.wait(handle.id);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("relaunch failed");
    expect(fs.readFileSync(path.join(agent.runDirectory(handle.id)!, "startup-attempts"), "utf8")).toBe("1");
  }, 10_000);

  it("actorStatus exposes hostQueue; stop preserves ordering and removes a claimed activation ticket", async () => {
    const cwd = root(); const directory = path.join(cwd, "tokens"); const blocker = await acquireHostActivation({ limit: 1, directory }, { id: "block" }); fds.push(blocker.fd);
    const agent = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, hostActivationLimit: 1, maxConcurrent: 4, budgetUsd: 0, sessionExport: false, timeoutMs: 100 }, {
      hostActivationDirectory: directory, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(cwd, "runs"),
    }); managers.push(agent);
    const mesh = new MeshStore(path.join(cwd, "mesh"), 64 * 1024, 100);
    const actor = new ActorManager("host-status", { id: "session:host-status", name: "main", kind: "main", sessionId: "host-status" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agent, () => {}, { actorRoot: path.join(cwd, "actors"), persistent: false, preparationTimeoutMs: 100 }); actors.push(actor);
    const first = await actor.create({ name: "first", instructions: "Test.", responseMode: "text" });
    const second = await actor.create({ name: "second", instructions: "Test.", responseMode: "text" });
    const nativeRun = agent.run.bind(agent);
    vi.spyOn(agent, "run").mockImplementation((...args) => {
      const spawned = args[2];
      args[2] = handle => {
        spawned?.(handle);
        // Relaunch waits reuse this observer after the actor already owns a run.
        const preparation = args[6];
        preparation?.onHostQueue?.({ position: 1, waitingSince: Date.now(), limit: 1 });
        expect(actor.status(second.id)).toMatchObject({ status: "waiting", hostQueue: { position: 1 } });
        preparation?.onHostQueue?.(undefined);
        expect(actor.status(second.id)).toMatchObject({ status: "running" });
        expect(actor.status(second.id).preparing).toBeUndefined();
      };
      return nativeRun(...args);
    });
    const firstAsk = actor.ask(first.id, "first claim").then(value => value, error => error as Error);
    await wait(() => actor.status(first.id).hostQueue?.position === 1);
    const queuedAsk = actor.ask(first.id, "next mailbox item").then(value => value, error => error as Error);
    const secondAsk = actor.ask(second.id, "surviving actor");
    await wait(() => actor.status(second.id).hostQueue?.position === 2);
    expect(actor.status(first.id)).toMatchObject({ status: "waiting", queued: 1, hostQueue: { position: 1, limit: 1 }, preparing: { phase: "host-queue" } });
    await new Promise(resolve => setTimeout(resolve, 1_500)); // Longer than preparation and worker timeouts.
    expect(actor.status(first.id).hostQueue?.position).toBe(1);
    expect(actor.status(second.id).hostQueue?.position).toBe(2);
    await actor.stop(first.id, undefined, true);
    expect(await firstAsk).toBeInstanceOf(Error); expect(await queuedAsk).toBeInstanceOf(Error);
    expect(actor.status(first.id)).toMatchObject({ status: "stopped" }); expect(actor.status(first.id).hostQueue).toBeUndefined();
    await wait(() => actor.status(second.id).hostQueue?.position === 1);
    closeFd(blocker.fd); expect((await secondAsk).error).toBeUndefined();
  });
});
