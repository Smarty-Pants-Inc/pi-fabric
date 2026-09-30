import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { DurableShellBridge, resolveJevFabricHome } from "../src/jev-fabric/bridge.js";
import { JevFabricCli } from "../src/jev-fabric/client.js";
import { DurableTaskRegistry } from "../src/jev-fabric/registry.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { TasksProvider } from "../src/providers/tasks-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const fake = fileURLToPath(new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url));
const stores: FabricShellJobStore[] = [];
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(stores.splice(0).map(store => store.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const environment = () => {
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-durable-")));
  roots.push(root);
  // PI_FABRIC_JEV_FABRIC_BIN runs the same contract against a real jev-fabric build.
  const binary = process.env.PI_FABRIC_JEV_FABRIC_BIN || path.join(root, "jev-fabric");
  if (!process.env.PI_FABRIC_JEV_FABRIC_BIN) fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
  const home = path.join(root, "home");
  const agentDir = path.join(root, "agent");
  return { root, binary, home, agentDir };
};

const session = (env: ReturnType<typeof environment>, ownerId = "durable-session") => {
  const store = new FabricShellJobStore();
  stores.push(store);
  store.durable = new DurableShellBridge(store, {
    cwd: env.root, agentDir: env.agentDir, ownerId,
    settings: () => ({ binary: env.binary, home: env.home, timeoutMs: 60_000 }),
    middleware: () => undefined,
  });
  const provider = new PiToolsProvider(env.root, undefined, undefined, { powerShellToolDefinitionFactory: undefined, shellJobs: store });
  const registry = new ActionRegistry();
  registry.register(provider);
  const context = {
    cwd: env.root, signal: new AbortController().signal, parentToolCallId: "parent", nestedToolCallId: "fabric_durable",
    extensionContext: { cwd: env.root, sessionManager: { getSessionId: () => ownerId, getSessionFile: () => undefined } } as unknown as ExtensionContext,
    update: () => undefined, approve: async () => {}, audits: [], maxResultChars: 100_000,
  };
  const bash = async (args: Record<string, unknown>) => await registry.invoke("pi.bash", args, context) as { ok: boolean; output: string; details: { taskId?: string } | null };
  return { store, tasks: new TasksProvider(store), bash, close: () => registry.close() };
};
const invocation = {} as FabricInvocationContext;

describe.skipIf(process.platform === "win32")("durable shell tasks through jev-fabric", () => {
  it("runs a durable command in the jev-fabric store and reports its real exit", async () => {
    const env = environment();
    const { store, tasks, bash } = session(env);
    const result = await bash({ command: "printf 'hello\\n'; exit 3", durable: true, description: "Durable probe" });
    expect(result.output).toContain("Durable: owned by the jev-fabric store");
    const id = result.details!.taskId!;
    const waited = await tasks.invoke("wait", { id, timeoutMs: 10_000 }, invocation) as { task: any; output: string };
    expect(waited.task).toMatchObject({ status: "failed", exitCode: 3, durable: { home: env.home, jobId: expect.any(String) } });
    expect(waited.output).toContain("hello");
    // The job lives in the jev-fabric store, not in this process.
    const jobId = store.get(id)!.durable!.jobId!;
    expect(await new JevFabricCli(env.binary, env.home).status(jobId)).toMatchObject({ state: "failed", exitCode: 3, label: "Durable probe" });
    // A terminal job no longer needs its session binding or launch script.
    await vi.waitFor(async () => expect(await new DurableTaskRegistry(path.join(env.agentDir, "fabric")).all()).toEqual([]));
    expect(fs.readdirSync(path.join(env.agentDir, "fabric", "durable-shell"))).toEqual([]);
  });

  it("detaches on session close and reattaches the same task on resume", async () => {
    const env = environment();
    const first = session(env);
    const result = await first.bash({ command: "sleep 3; printf 'done\\n'", durable: true, description: "Survivor" });
    const id = result.details!.taskId!;
    await vi.waitFor(() => expect(first.store.get(id)?.durable?.jobId).toBeDefined());
    const jobId = first.store.get(id)!.durable!.jobId!;
    await first.store.close();
    // Closing the session never stopped the process.
    expect(await new JevFabricCli(env.binary, env.home).status(jobId)).toMatchObject({ state: "running" });

    const second = session(env);
    const finished = vi.fn();
    second.store.subscribe(event => { if (event.type === "finished") finished(event.job); });
    const listed = await second.tasks.invoke("list", {}, invocation) as Array<{ id: string; durable?: { adopted?: boolean } }>;
    expect(listed).toEqual([expect.objectContaining({ id, description: "Survivor", durable: expect.objectContaining({ jobId, adopted: true }) })]);
    const waited = await second.tasks.invoke("wait", { id, timeoutMs: 10_000 }, invocation) as { task: any; output: string };
    expect(waited.task).toMatchObject({ status: "exited", exitCode: 0 });
    expect(waited.output).toContain("done");
    expect(finished).toHaveBeenCalledWith(expect.objectContaining({ id, spilledAt: expect.any(Number) }));
  });

  it("does not reattach another session's durable tasks", async () => {
    const env = environment();
    const first = session(env, "owner-a");
    await first.bash({ command: "sleep 2", durable: true });
    await first.store.close();
    const other = session(env, "owner-b");
    expect(await other.tasks.invoke("list", {}, invocation)).toEqual([]);
  });

  it("stops a durable task through jev-fabric, never by PID", async () => {
    const env = environment();
    const { store, tasks, bash } = session(env);
    const id = (await bash({ command: "sleep 30", durable: true })).details!.taskId!;
    await vi.waitFor(() => expect(store.get(id)?.durable?.jobId).toBeDefined());
    const jobId = store.get(id)!.durable!.jobId!;
    await tasks.invoke("stop", { id }, invocation);
    await vi.waitFor(() => expect(store.get(id)?.info().finishedAt).toBeDefined(), { timeout: 10_000 });
    expect(store.get(id)!.info().status).toBe("killed");
    expect(await new JevFabricCli(env.binary, env.home).status(jobId)).toMatchObject({ state: "cancelled" });
  });

  it("lists and adopts jobs another harness started in the shared store", async () => {
    const env = environment();
    const cli = new JevFabricCli(env.binary, env.home);
    const jobId = await cli.start(["/bin/sh", "-c", "sleep 3; echo external ready"], { cwd: env.root, timeoutMs: 60_000, label: "Dev server" });
    const { tasks } = session(env);
    const external = await tasks.invoke("external", {}, invocation) as { jobs: Array<{ id: string; label?: string }> };
    expect(external.jobs).toEqual([expect.objectContaining({ id: jobId, label: "Dev server", state: "running" })]);
    const adopted = await tasks.invoke("adopt", { jobId }, invocation) as { task: { id: string; description?: string } };
    expect(adopted.task.description).toBe("Dev server");
    expect(await tasks.invoke("external", {}, invocation)).toMatchObject({ jobs: [] });
    const waited = await tasks.invoke("wait", { id: adopted.task.id, timeoutMs: 10_000 }, invocation) as { task: any; output: string };
    expect(waited.task).toMatchObject({ status: "exited" });
    expect(waited.output).toContain("external ready");
  });

  it("canonicalizes the store path because jev-fabric rejects symlink components", () => {
    const env = environment();
    const link = path.join(env.root, "linked");
    fs.mkdirSync(path.join(env.root, "real"));
    fs.symlinkSync(path.join(env.root, "real"), link);
    const settings = { binary: env.binary, home: "", timeoutMs: 60_000 };
    expect(resolveJevFabricHome(settings, link)).toBe(path.join(env.root, "real", ".jev-fabric-native"));
    expect(resolveJevFabricHome({ ...settings, home: path.join(link, "missing", "store") }, env.root)).toBe(path.join(env.root, "real", "missing", "store"));
  });

  it("rejects durable use without a backend, on PowerShell, or with background:false", async () => {
    const env = environment();
    const { store, bash } = session(env);
    await expect(bash({ command: "true", durable: true, background: false })).rejects.toThrow("omit background:false");
    store.durable = undefined;
    await expect(bash({ command: "true", durable: true })).rejects.toThrow("need jev-fabric");
    expect(store.list()).toEqual([]);
  });

  it("explains a missing binary instead of falling back to a local process", async () => {
    const env = environment();
    const { store, bash } = session(env);
    store.durable = new DurableShellBridge(store, {
      cwd: env.root, agentDir: env.agentDir, ownerId: "missing",
      settings: () => ({ binary: path.join(env.root, "absent-jev-fabric"), home: env.home, timeoutMs: 60_000 }),
      middleware: () => undefined,
    });
    const result = await bash({ command: "echo never", durable: true }).catch((error: Error) => error);
    const message = result instanceof Error ? result.message : result.output;
    expect(message).toContain("unsuitable for durable: not found");
  });
});
