import fs from "node:fs";
import os from "node:os";
import { createRequire } from "node:module";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createBashToolDefinition, type ExtensionContext, type ExtensionRunner, type RegisteredTool } from "@earendil-works/pi-coding-agent";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { readFabricBashMiddleware } from "../src/core/shell-middleware.js";
import { FABRIC_BASH_MIDDLEWARE, type FabricBashMiddlewareV1 } from "../src/protocol.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { DurableShellBridge } from "../src/jev-fabric/bridge.js";
import { JevFabricCli } from "../src/jev-fabric/client.js";
import { JevFabricServe } from "../src/jev-fabric/serve.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { DurableTaskRegistry } from "../src/jev-fabric/registry.js";
import { SessionsProvider } from "../src/providers/sessions-provider.js";
import { TasksProvider } from "../src/providers/tasks-provider.js";

const SECRET = "fabric-test-secret-not-a-credential";
const registries: ActionRegistry[] = [];
const directories: string[] = [];
const removeDirectory = async (directory: string): Promise<void> => {
  for (let attempt = 0; ; attempt += 1) {
    try {
      fs.rmSync(directory, { recursive: true, force: true });
      return;
    } catch (error) {
      const code = error && typeof error === "object" && "code" in error ? String((error as { code?: unknown }).code) : "";
      if (attempt >= 10 || (code !== "EBUSY" && code !== "EPERM" && code !== "ENOTEMPTY")) throw error;
      await new Promise((resolve) => setTimeout(resolve, 50 * (attempt + 1)));
    }
  }
};

afterEach(async () => {
  await Promise.all(registries.splice(0).map(registry => registry.close()));
  for (const directory of directories.splice(0)) await removeDirectory(directory);
  vi.unstubAllEnvs();
});

const middleware = (): FabricBashMiddlewareV1 => ({
  version: 1,
  options: {
    commandPrefix: "export FABRIC_PREFIX=kept",
    spawnHook: ({ env, ...rest }) => {
      const childEnv = { ...env };
      delete childEnv.FABRIC_TEST_SECRET;
      return { ...rest, env: childEnv };
    },
  },
  wrapOperations: inner => ({
    exec: async (command, cwd, options) => {
      // Line buffering in this fixture tests that Fabric records only filtered bytes.
      let pending = "";
      const emit = (text: string) => options.onData(Buffer.from(text.split(SECRET).join("[filtered]")));
      try {
        return await inner.exec(command, cwd, { ...options, onData: data => {
          pending += data.toString();
          const last = pending.lastIndexOf("\n");
          if (last >= 0) { emit(pending.slice(0, last + 1)); pending = pending.slice(last + 1); }
        } });
      } finally { emit(pending); }
    },
  }),
});

const harness = (options: { middleware?: unknown; optIn?: boolean; hangMs?: number; managed?: boolean; blocked?: boolean } = {}) => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-middleware-test-"));
  directories.push(cwd);
  const extensionContext = {
    cwd,
    sessionManager: { getSessionId: () => "middleware-test", getSessionFile: () => undefined },
  } as unknown as ExtensionContext;
  const runner = {
    createContext: () => extensionContext,
    getActiveTools: () => ["bash"],
    emit: vi.fn(async () => {}),
    emitToolCall: vi.fn(async () => options.blocked ? { block: true, reason: "blocked by policy" } : undefined),
    emitToolResult: vi.fn(async () => undefined),
  } as unknown as ExtensionRunner;
  const fallback = vi.fn(async () => ({ content: [{ type: "text" as const, text: "standalone override" }], details: undefined }));
  const definition = { ...createBashToolDefinition(cwd), execute: fallback };
  if (options.optIn !== false) Object.assign(definition, { [FABRIC_BASH_MIDDLEWARE]: options.middleware ?? middleware() });
  const catalog = new CapturedToolCatalog();
  catalog.replace([{ definition, sourceInfo: { path: "/extensions/filtered-bash.ts", source: "test", scope: "user", origin: "package" } } as RegisteredTool], runner, DEFAULT_FABRIC_CONFIG.capture, "/fabric/index.ts");
  const captured = new CapturedToolsProvider(catalog);
  const provider = new PiToolsProvider(cwd, catalog, captured, {
    requireCapturedOverrides: options.managed === true,
    powerShellToolDefinitionFactory: undefined,
    getShellHangMs: () => options.hangMs ?? 120_000,
  });
  const registry = new ActionRegistry();
  registry.register(provider);
  registries.push(registry);
  const previews: unknown[] = [];
  const context = {
    cwd, extensionContext, signal: new AbortController().signal,
    parentToolCallId: "parent", nestedToolCallId: "fabric_middleware",
    update: () => {}, approve: async () => {}, audits: [], maxResultChars: 100_000,
    attachPreview: (preview: unknown) => { previews.push(preview); },
  };
  const invoke = (args: Record<string, unknown>, signal = context.signal) => registry.invoke("pi.bash", args, { ...context, signal }) as Promise<{
    ok: boolean; output: string; details: { running?: boolean; pid?: number; logPath?: string } | null;
  }>;
  return { provider, catalog, registry, runner, fallback, context, cwd, previews, invoke };
};

const reattachmentMiddleware = (policy: string): FabricBashMiddlewareV1 | undefined => {
  if (policy === "none") return undefined;
  return {
    version: 1,
    options: { spawnHook: options => ({ ...options, env: { ...options.env, SEC12_ENV_ONLY: "kept" } }) },
    wrapOperations: inner => policy === "environment-only" ? inner : ({
      exec: (command, cwd, options) => inner.exec(command, cwd, {
        ...options,
        // A real but different policy: it does not redact the launcher's canary.
        onData: data => options.onData(Buffer.from(data.toString().replaceAll("another-policy-secret", "[other-filter]"))),
      }),
    }),
  };
};

const checkTaskOutput = async (tasks: TasksProvider, id: string, context: ReturnType<typeof harness>["context"], expected: string[], protectedOutput = true) => {
  const get = await tasks.invoke("get", { id }, context) as any;
  const wait = await tasks.invoke("wait", { id, timeoutMs: 1 }, context) as any;
  const read = await tasks.invoke("read", { id }, context) as any;
  const encoded = await tasks.invoke("read", { id, encoding: "base64" }, context) as any;
  const decoded = Buffer.from(encoded.data, "base64").toString();
  const watch = await tasks.invoke("watch", { id, match: expected[0]!.split(":")[0], timeoutMs: 1 }, context) as any;
  const log = fs.readFileSync(get.task.logPath, "utf8");
  for (const output of [get.output, wait.output, read.text, decoded, watch.lines.join("\n"), log]) {
    for (const text of expected) expect(output).toContain(text);
    if (protectedOutput) expect(output).not.toContain(SECRET);
  }
  if (protectedOutput) {
    expect(JSON.stringify({ get, wait, read, watch })).not.toContain(SECRET);
    const canaryWatch = await tasks.invoke("watch", { id, match: SECRET, timeoutMs: 1 }, context) as any;
    expect(canaryWatch.lines).toEqual([]);
    expect(JSON.stringify(canaryWatch)).not.toContain(SECRET);
  }
  return get;
};

// Pin the native backend, not the protocol fixture or an environment override.
const sec12Job = async (filtered = true) => {
  const backend = createRequire(import.meta.url)("jev-fabric") as { version: string; binaryPath(): string | undefined };
  expect(backend.version).toBe("0.5.0");
  const binary = backend.binaryPath();
  expect(binary).toBeTruthy();
  const h = harness();
  if (!filtered) h.catalog.clear();
  const home = path.join(h.cwd, "home");
  const agentDir = path.join(h.cwd, "agent");
  const settings = () => ({ binary: binary!, home, timeoutMs: 60_000 });
  const jobs = h.provider.shellJobs;
  jobs.durable = new DurableShellBridge(jobs, { cwd: h.cwd, agentDir, ownerId: "middleware-test", settings,
    middleware: () => readFabricBashMiddleware(h.catalog.get("bash")?.definition) });
  const tasks = new TasksProvider(jobs);
  const cli = new JevFabricCli(binary!, home);
  const release = path.join(h.cwd, "release-sec12");
  fs.writeFileSync(path.join(h.cwd, "stdout-canary"), `${SECRET}:stdout\n`);
  fs.writeFileSync(path.join(h.cwd, "stderr-canary"), `${SECRET}:stderr\n`);
  const result = await h.invoke({ command: `cat stdout-canary; cat stderr-canary >&2; while [ ! -f '${release}' ]; do sleep 0.05; done; exit 7`, durable: true, description: "SEC-12 canary job" });
  const id = (result.details as any).taskId as string;
  return { h, jobs, tasks, cli, release, settings, agentDir, id };
};

describe("cooperative bash middleware", () => {
  it.skipIf(process.platform === "win32").each(
    ["running", "terminal"].flatMap(state => ["none", "environment-only", "different-redaction"].map(policy => ({ state, policy }))),
  )("SEC-12 real pinned backend withholds filtered adoption ($state, $policy)", async ({ state, policy }) => {
    const { h, jobs, tasks, cli, release, settings, id } = await sec12Job();
    const other = new FabricShellJobStore();
    other.durable = new DurableShellBridge(other, { cwd: h.cwd, agentDir: path.join(h.cwd, "foreign-agent"), ownerId: "foreign-session", settings,
      middleware: () => reattachmentMiddleware(policy) });
    const foreignTasks = new TasksProvider(other);
    try {
      await vi.waitFor(async () => {
        const page = await tasks.invoke("get", { id }, h.context) as any;
        expect(page.output).toContain("[filtered]:stderr");
      }, { timeout: 10_000 });
      const jobId = jobs.get(id)!.durable!.jobId!;
      // The launcher's own live wrapper remains authoritative, even on adopt.
      expect(await tasks.invoke("adopt", { jobId }, h.context)).toMatchObject({ task: { id } });
      await checkTaskOutput(tasks, id, h.context, ["[filtered]:stdout", "[filtered]:stderr"]);
      if (state === "terminal") {
        fs.writeFileSync(release, "done");
        await tasks.invoke("wait", { id, timeoutMs: 10_000 }, h.context);
      }
      const adopted = await foreignTasks.invoke("adopt", { jobId }, h.context) as any;
      await vi.waitFor(async () => {
        const page = await foreignTasks.invoke("get", { id: adopted.task.id }, h.context) as any;
        // On the vulnerable head, wait for actual canary evidence before failing.
        expect(page.output).toMatch(/Output withheld|fabric-test-secret-not-a-credential:stderr/);
      }, { timeout: 10_000 });
      const live = await checkTaskOutput(foreignTasks, adopted.task.id, h.context, ["Output withheld: launched under an output filter"]);
      expect(live.task).toMatchObject({ description: "SEC-12 canary job", durable: { jobId, adopted: true } });
      fs.writeFileSync(release, "done");
      const completed = await foreignTasks.invoke("wait", { id: adopted.task.id, timeoutMs: 10_000 }, h.context) as any;
      expect(completed).toMatchObject({ timedOut: false, task: { status: "failed", exitCode: 7 } });
      await checkTaskOutput(foreignTasks, adopted.task.id, h.context, ["Output withheld: launched under an output filter"]);
      await tasks.invoke("wait", { id, timeoutMs: 10_000 }, h.context);
      await checkTaskOutput(tasks, id, h.context, ["[filtered]:stdout", "[filtered]:stderr"]);
      // Prove both raw streams exist in the native retained receipt.
      expect(await cli.status(jobId)).toMatchObject({ state: "failed", exitCode: 7,
        stdout: expect.stringContaining(`${SECRET}:stdout`), stderr: expect.stringContaining(`${SECRET}:stderr`) });
    } finally {
      fs.writeFileSync(release, "done");
      await tasks.invoke("wait", { id, timeoutMs: 10_000 }, h.context);
      await other.close();
    }
  });

  it.skipIf(process.platform === "win32").each(["none", "environment-only", "different-redaction"])(
    "SEC-12 real pinned backend withholds resume after launching filter is gone (%s)", async policy => {
      const { h, jobs, tasks, cli, release, settings, agentDir, id } = await sec12Job();
      const resumed = new FabricShellJobStore();
      resumed.durable = new DurableShellBridge(resumed, { cwd: h.cwd, agentDir, ownerId: "middleware-test", settings,
        middleware: () => reattachmentMiddleware(policy) });
      const resumedTasks = new TasksProvider(resumed);
      let jobId: string | undefined;
      try {
        await vi.waitFor(async () => {
          expect((await tasks.invoke("get", { id }, h.context) as any).output).toContain("[filtered]:stderr");
        }, { timeout: 10_000 });
        jobId = jobs.get(id)!.durable!.jobId!;
        await checkTaskOutput(tasks, id, h.context, ["[filtered]:stdout", "[filtered]:stderr"]);
        await jobs.close();
        h.catalog.clear();
        expect(readFabricBashMiddleware(h.catalog.get("bash")?.definition)).toBeUndefined();
        expect(await resumedTasks.invoke("list", {}, h.context)).toEqual([expect.objectContaining({ id, durable: expect.objectContaining({ jobId, adopted: true }) })]);
        fs.writeFileSync(release, "done");
        const completed = await resumedTasks.invoke("wait", { id, timeoutMs: 10_000 }, h.context) as any;
        expect(completed).toMatchObject({ timedOut: false, task: { status: "failed", exitCode: 7 } });
        await checkTaskOutput(resumedTasks, id, h.context, ["Output withheld: launched under an output filter"]);
        expect(await cli.status(jobId)).toMatchObject({ stdout: expect.stringContaining(`${SECRET}:stdout`), stderr: expect.stringContaining(`${SECRET}:stderr`) });
      } finally {
        fs.writeFileSync(release, "done");
        if (jobId) await vi.waitFor(async () => expect(await cli.status(jobId!)).toMatchObject({ state: "failed" }), { timeout: 10_000 });
        else await tasks.invoke("wait", { id, timeoutMs: 10_000 }, h.context);
        await resumed.close();
      }
    },
  );

  it.skipIf(process.platform === "win32")("SEC-12 real pinned backend still streams unfiltered adoption", async () => {
    const { h, jobs, tasks, release, settings, id } = await sec12Job(false);
    const other = new FabricShellJobStore();
    other.durable = new DurableShellBridge(other, { cwd: h.cwd, agentDir: path.join(h.cwd, "foreign-agent"), ownerId: "foreign-session", settings, middleware: () => undefined });
    const foreignTasks = new TasksProvider(other);
    try {
      await vi.waitFor(() => expect(jobs.get(id)?.durable?.jobId).toBeDefined(), { timeout: 10_000 });
      const adopted = await foreignTasks.invoke("adopt", { jobId: jobs.get(id)!.durable!.jobId! }, h.context) as any;
      await vi.waitFor(async () => expect((await foreignTasks.invoke("get", { id: adopted.task.id }, h.context) as any).output).toContain(`${SECRET}:stderr`), { timeout: 10_000 });
      await checkTaskOutput(foreignTasks, adopted.task.id, h.context, [SECRET], false);
      fs.writeFileSync(release, "done");
      expect(await foreignTasks.invoke("wait", { id: adopted.task.id, timeoutMs: 10_000 }, h.context)).toMatchObject({ timedOut: false, task: { exitCode: 7 } });
      await checkTaskOutput(foreignTasks, adopted.task.id, h.context, [`${SECRET}:stdout`, `${SECRET}:stderr`], false);
    } finally {
      fs.writeFileSync(release, "done");
      await tasks.invoke("wait", { id, timeoutMs: 10_000 }, h.context);
      await other.close();
    }
  });
  it.skipIf(process.platform === "win32").each(
    ["status", "wait", "stop"].flatMap(verb => [false, true].flatMap(shellOverride => ["launch-session", "foreign-session"].map(reader => ({ verb, shellOverride, reader })))),
  )("SEC-3 real pinned backend refuses foreign receipt tails ($verb, override=$shellOverride, $reader)", async ({ verb, shellOverride, reader }) => {
    // No env override or fake fallback: these receipts must come from the
    // actual optional dependency pinned in package.json and bun.lock.
    const backend = createRequire(import.meta.url)("jev-fabric") as { version: string; binaryPath(): string | undefined };
    expect(backend.version).toBe("0.5.0");
    const binary = backend.binaryPath();
    expect(binary).toBeTruthy();
    const h = harness();
    const home = path.join(h.cwd, "home");
    const settings = () => ({ binary: binary!, home, timeoutMs: 60_000 });
    const jobs = h.provider.shellJobs;
    jobs.durable = new DurableShellBridge(jobs, { cwd: h.cwd, agentDir: path.join(h.cwd, "agent"), ownerId: "filtered-owner",
      settings, middleware: () => readFabricBashMiddleware(h.catalog.get("bash")?.definition) });
    const other = new FabricShellJobStore();
    other.durable = new DurableShellBridge(other, { cwd: h.cwd, agentDir: path.join(h.cwd, "foreign-agent"), ownerId: "foreign-reader",
      settings, middleware: () => undefined });
    const localSessions = new SessionsProvider(jobs.durable, { cwd: h.cwd, shellOverride: () => shellOverride });
    const foreignSessions = new SessionsProvider(other.durable, { cwd: h.cwd, shellOverride: () => shellOverride });
    const tasks = new TasksProvider(jobs);
    const foreignTasks = new TasksProvider(other);
    const release = path.join(h.cwd, "release-receipt-job");
    let taskId: string | undefined;
    let raw: JevFabricServe | undefined;
    try {
      fs.writeFileSync(path.join(h.cwd, "stdout-canary"), `${SECRET}:stdout\n`);
      fs.writeFileSync(path.join(h.cwd, "stderr-canary"), `${SECRET}:stderr\n`);
      const result = await h.invoke({ command: `cat stdout-canary; cat stderr-canary >&2; while [ ! -f '${release}' ]; do sleep 0.05; done`, durable: true });
      taskId = (result.details as any).taskId;
      await vi.waitFor(() => expect(jobs.get(taskId!)?.durable?.jobId).toBeDefined());
      const external = await foreignTasks.invoke("external", {}, h.context) as { jobs: Array<{ id: string }> };
      expect(external.jobs).toHaveLength(1);
      const jobId = external.jobs[0]!.id;
      expect(jobId).toBe(jobs.get(taskId!)!.durable!.jobId);
      fs.writeFileSync(release, "done");
      const filtered = await tasks.invoke("wait", { id: taskId, timeoutMs: 10_000 }, h.context) as any;
      expect(filtered.timedOut).toBe(false);
      expect(filtered.output).toContain("[filtered]:stdout");
      expect(filtered.output).toContain("[filtered]:stderr");
      expect(JSON.stringify(filtered)).not.toContain(SECRET);
      const adopted = await foreignTasks.invoke("adopt", { jobId }, h.context) as any;
      const withheld = await foreignTasks.invoke("wait", { id: adopted.task.id, timeoutMs: 10_000 }, h.context) as any;
      expect(withheld.timedOut).toBe(false);
      expect(withheld.output).toContain("Output withheld");
      expect(JSON.stringify(withheld)).not.toContain(SECRET);
      raw = await JevFabricServe.open(binary!, { home, cwd: h.cwd, timeoutMs: 60_000 });
      expect(raw.banner.version).toBe("0.5.0-native");
      const receipt = await raw.request<Record<string, any>>(verb, { job: jobId, ...(verb === "wait" ? { timeoutMs: 5000 } : {}) });
      // Establish that this verb genuinely returns both raw tails; a fixture
      // omitting receipt output would let the old implementation pass.
      expect(receipt.state).toBe("exited");
      expect(receipt.stdout).toContain(`${SECRET}:stdout`);
      expect(receipt.stderr).toContain(`${SECRET}:stderr`);
      const sessions = reader === "launch-session" ? localSessions : foreignSessions;
      if (shellOverride) await expect(sessions.invoke("open", { argv: ["true"] }, h.context)).rejects.toThrow("bypass its shell protection");
      await expect(sessions.invoke(verb, { id: jobId }, h.context)).rejects.toThrow("only for children opened here");
    } finally {
      // Even the vulnerable head must leave no durable worker or serve alive.
      fs.writeFileSync(release, "done");
      if (taskId) await tasks.invoke("wait", { id: taskId, timeoutMs: 10_000 }, h.context);
      await raw?.close();
      await Promise.all([localSessions.close(), foreignSessions.close(), other.close()]);
    }
  });
  it.skipIf(process.platform === "win32")("SEC-3 refuses cross-session adoption while discovered backend policy persistence is delayed", async () => {
    const h = harness();
    const fake = new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url).pathname;
    const binary = process.env.PI_FABRIC_JEV_FABRIC_BIN || path.join(h.cwd, "jev-fabric");
    if (!process.env.PI_FABRIC_JEV_FABRIC_BIN) fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
    const home = path.join(h.cwd, "home");
    const settings = () => ({ binary, home, timeoutMs: 60_000 });
    const jobs = h.provider.shellJobs;
    jobs.durable = new DurableShellBridge(jobs, { cwd: h.cwd, agentDir: path.join(h.cwd, "launch-agent"), ownerId: "filtered-launch", settings, middleware: () => middleware() });
    let releasePolicy!: () => void;
    const gate = new Promise<void>(resolve => { releasePolicy = resolve; });
    let entered = false;
    const protect = DurableTaskRegistry.prototype.protect;
    const spy = vi.spyOn(DurableTaskRegistry.prototype, "protect").mockImplementation(async function (this: DurableTaskRegistry, home, jobId) {
      entered = true;
      await gate;
      return protect.call(this, home, jobId);
    });
    const other = new FabricShellJobStore();
    other.durable = new DurableShellBridge(other, { cwd: h.cwd, agentDir: path.join(h.cwd, "other-agent"), ownerId: "unfiltered-reader", settings, middleware: () => undefined });
    const tasks = new TasksProvider(other);
    const releaseChild = path.join(h.cwd, "release-racing-child");
    let taskId: string | undefined;
    try {
      const result = await h.invoke({ command: `printf '${SECRET}\n'; while [ ! -f '${releaseChild}' ]; do sleep 0.05; done`, durable: true });
      taskId = (result.details as any).taskId;
      await vi.waitFor(() => expect(entered).toBe(true));
      const external = await tasks.invoke("external", {}, h.context) as { jobs: Array<{ id: string }> };
      expect(external.jobs).toHaveLength(1);
      // Discovery is independent of the original task publication fence.
      expect(jobs.get(taskId!)!.durable!.jobId).toBeUndefined();
      const jobId = external.jobs[0]!.id;
      await expect(tasks.invoke("adopt", { jobId }, h.context)).rejects.toThrow("output policy is absent or initializing");
      expect(other.list()).toEqual([]);
      releasePolicy();
      await vi.waitFor(() => expect(jobs.get(taskId!)!.durable!.jobId).toBe(jobId));
      const adopted = await tasks.invoke("adopt", { jobId }, h.context) as any;
      expect(await tasks.invoke("get", { id: adopted.task.id }, h.context)).toMatchObject({ output: expect.stringContaining("Output withheld") });
      expect(JSON.stringify(await tasks.invoke("read", { id: adopted.task.id }, h.context))).not.toContain(SECRET);
    } finally {
      releasePolicy(); spy.mockRestore();
      fs.writeFileSync(releaseChild, "done");
      if (taskId) await vi.waitFor(() => expect(jobs.get(taskId!)?.finished).toBe(true), { timeout: 10_000 });
      await other.close();
    }
  });

  it.skipIf(process.platform === "win32").each(["stdout", "stderr", "base64", "events", "adopt-running", "adopt-terminal"])("SEC-3 protects filtered durable output through %s", async (reader) => {
    vi.stubEnv("FAKE_JEV_FABRIC_FEATURES", "follow,list,label,sessions,serve-concurrent,read,cwd,serve-24h,durable-input");
    const h = harness();
    const fake = new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url).pathname;
    const binary = process.env.PI_FABRIC_JEV_FABRIC_BIN || path.join(h.cwd, "jev-fabric");
    if (!process.env.PI_FABRIC_JEV_FABRIC_BIN) fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
    const home = path.join(h.cwd, "home");
    const settings = () => ({ binary, home, timeoutMs: 60_000 });
    const jobs = h.provider.shellJobs;
    jobs.durable = new DurableShellBridge(jobs, { cwd: h.cwd, agentDir: path.join(h.cwd, "agent"), ownerId: "filtered-owner",
      settings, middleware: () => readFabricBashMiddleware(h.catalog.get("bash")?.definition) });
    const release = path.join(h.cwd, "release-filtered-job");
    const result = await h.invoke({ command: `printf '${SECRET}\\n'; printf '${SECRET}\\n' >&2; while [ ! -f '${release}' ]; do sleep 0.05; done`, durable: true });
    const taskId = (result.details as any).taskId as string;
    await vi.waitFor(() => expect(jobs.get(taskId)?.durable?.jobId).toBeDefined());
    const jobId = jobs.get(taskId)!.durable!.jobId!;
    const sessions = new SessionsProvider(jobs.durable, { cwd: h.cwd, shellOverride: () => false });
    const adoptedStores: FabricShellJobStore[] = [];
    try {
      await vi.waitFor(async () => {
        const page = await new TasksProvider(jobs).invoke("wait", { id: taskId, timeoutMs: 1 }, h.context) as any;
        expect(page.output).toContain("[filtered]");
        expect(page.output).not.toContain(SECRET);
      });
      if (["stdout", "stderr", "base64"].includes(reader)) {
        await expect(sessions.invoke("read", { id: jobId, ...(reader === "base64" ? { encoding: "base64" } : { stream: reader }) }, h.context)).rejects.toThrow("only for children opened here");
        return;
      }
      if (reader === "events") {
        await expect(sessions.invoke("events", { id: jobId }, h.context)).rejects.toThrow("only for children opened here");
        return;
      }
      const adoptWithoutFilter = async (ownerId: string) => {
        const store = new FabricShellJobStore(); adoptedStores.push(store);
        // A different agent directory must not erase a store/job's policy.
        store.durable = new DurableShellBridge(store, { cwd: h.cwd, agentDir: path.join(h.cwd, ownerId), ownerId, settings, middleware: () => undefined });
        const tasks = new TasksProvider(store);
        const adopted = await tasks.invoke("adopt", { jobId }, h.context) as any;
        let page: any;
        await vi.waitFor(async () => {
          page = await tasks.invoke("wait", { id: adopted.task.id, timeoutMs: 10 }, h.context);
          expect(page.output).toMatch(/Output withheld|fabric-test-secret-not-a-credential/);
        });
        expect(page.output).toContain("Output withheld");
        expect(page.output).not.toContain(SECRET);
        return { tasks, id: adopted.task.id };
      };
      if (reader === "adopt-running") {
        const other = await adoptWithoutFilter("unfiltered-owner");
        fs.writeFileSync(release, "done");
        await other.tasks.invoke("wait", { id: other.id, timeoutMs: 10_000 }, h.context);
      } else {
        fs.writeFileSync(release, "done");
        await vi.waitFor(() => expect(jobs.get(taskId)?.finished).toBe(true));
        await vi.waitFor(async () => expect(await new DurableTaskRegistry(path.join(h.cwd, "agent", "fabric")).all()).toEqual([]));
        // Terminal cleanup removed the binding, but must not remove provenance.
        await adoptWithoutFilter("terminal-reader");
      }
    } finally {
      fs.writeFileSync(release, "done");
      await sessions.close();
      await Promise.all(adoptedStores.map(store => store.close()));
      await vi.waitFor(() => expect(jobs.get(taskId)?.finished).toBe(true), { timeout: 10_000 });
    }
  });
  it.skipIf(process.platform === "win32")("wraps durable jev-fabric operations with the same filters, prefix and spawn hook", async () => {
    const h = harness();
    const fake = new URL("./fixtures/fake-jev-fabric.mjs", import.meta.url).pathname;
    const binary = process.env.PI_FABRIC_JEV_FABRIC_BIN || path.join(h.cwd, "jev-fabric");
    if (!process.env.PI_FABRIC_JEV_FABRIC_BIN) fs.writeFileSync(binary, `#!/bin/sh\nexec "${process.execPath}" "${fake}" "$@"\n`, { mode: 0o755 });
    const jobs = h.provider.shellJobs;
    jobs.durable = new DurableShellBridge(jobs, {
      cwd: h.cwd, agentDir: path.join(h.cwd, "agent"), ownerId: "middleware-test",
      settings: () => ({ binary, home: path.join(h.cwd, "home"), timeoutMs: 60_000 }),
      middleware: () => readFabricBashMiddleware(h.catalog.get("bash")?.definition),
    });
    vi.stubEnv("FABRIC_TEST_SECRET", SECRET);
    fs.writeFileSync(path.join(h.cwd, "fixture"), `${SECRET}\n`);
    const result = await h.invoke({ command: 'cat fixture; printf "prefix=%s secret=%s\\n" "$FABRIC_PREFIX" "${FABRIC_TEST_SECRET:-unset}"', durable: true });
    expect(result.details?.running).toBe(true);
    await vi.waitFor(() => expect(jobs.list()[0]?.finishedAt).toBeDefined(), { timeout: 10_000 });
    const output = await jobs.get(jobs.list()[0]!.id)!.outputText();
    expect(output).toContain("[filtered]");
    expect(output).toContain("prefix=kept secret=unset");
    expect(output).not.toContain(SECRET);
    expect(h.fallback).not.toHaveBeenCalled();
  });
  it("delivers monitor events only after cooperative output filtering and hooks", async () => {
    const h = harness();
    fs.writeFileSync(path.join(h.cwd, "fixture"), `${SECRET}\n`);
    const events: unknown[] = [];
    h.provider.shellJobs.subscribe(event => { if (event.type === "monitor") events.push(event); });
    await h.invoke({ command: "cat fixture; sleep 1.3", monitor: { delivery: "wake", intervalMs: 1000 } });
    await vi.waitFor(() => expect(h.provider.shellJobs.list()[0]?.finishedAt).toBeDefined(), { timeout: 5000 });
    expect(JSON.stringify(events)).toContain("[filtered]");
    expect(JSON.stringify(events)).not.toContain(SECRET);
    expect(h.runner.emitToolCall).toHaveBeenCalledOnce();
    expect(h.fallback).not.toHaveBeenCalled();
  });

  it("does not bypass an opaque shell override for monitors", async () => {
    const h = harness({ optIn: false });
    await expect(h.invoke({ command: "echo forbidden", monitor: { delivery: "wake" } })).rejects.toThrow();
    expect(h.provider.shellJobs.list()).toEqual([]);
    expect(h.fallback).not.toHaveBeenCalled();
  });
  it("advertises Fabric arguments without serializing host callbacks", async () => {
    const h = harness();
    const descriptor = await h.provider.describe("bash", h.context);
    expect(descriptor).toMatchObject({ namespace: "extension-middleware", inputSchema: { properties: {
      background: { type: "boolean" }, cwd: { type: "string" }, timeout: { type: "number" },
    } } });
    expect(JSON.stringify(descriptor)).not.toContain("spawnHook");
  });

  it("scrubs child env, keeps host credentials, prefix, cwd, and lifecycle", async () => {
    vi.stubEnv("FABRIC_TEST_SECRET", SECRET);
    const h = harness();
    const nested = path.join(h.cwd, "nested");
    fs.mkdirSync(nested);
    fs.writeFileSync(path.join(nested, "marker"), "nested-ok");
    const result = await h.invoke({ cmd: 'printf "%s|%s|%s|%s\\n" "${FABRIC_TEST_SECRET-unset}" "$FABRIC_PREFIX" "$PI_SESSION_ID" "$(cat marker)"', cwd: "nested" });
    expect(result.output).toBe("unset|kept|middleware-test|nested-ok\n");
    expect(process.env.FABRIC_TEST_SECRET).toBe(SECRET);
    expect(h.fallback).not.toHaveBeenCalled();
    expect(h.runner.emitToolCall).toHaveBeenCalledOnce();
    expect(h.runner.emitToolResult).toHaveBeenCalledOnce();
  });

  it.each([true, false])("filters output before and after handoff (immediate=%s)", async immediate => {
    const h = harness({ hangMs: immediate ? 120_000 : 30 });
    fs.writeFileSync(path.join(h.cwd, "fixture"), `${SECRET}\n`);
    const result = await h.invoke({ command: "cat fixture; sleep 0.2; cat fixture", ...(immediate ? { run_in_background: true } : {}) });
    expect(result).toMatchObject({ ok: true, details: { running: true, logPath: expect.any(String) } });
    // A handoff, after 30 ms or at once for a background run, can come before a slow Git Bash
    // (Windows CI) writes its pid file; readPid() then reports none (smarty-dev#883: the
    // immediate case failed there too). shell-hang.test.ts covers the pid itself.
    if (result.details?.pid !== undefined) expect(result.details.pid).toEqual(expect.any(Number));
    expect(result.output).not.toContain(SECRET);
    // A slow Git Bash start can take the run past vi.waitFor's default 1 s (smarty-dev#883).
    await vi.waitFor(() => expect(h.provider.shellJobs.list()[0]?.finishedAt).toEqual(expect.any(Number)), { timeout: 10_000 });
    const log = fs.readFileSync(result.details!.logPath!, "utf8");
    expect(log).toContain("[filtered]");
    expect(log).not.toContain(SECRET);
    expect(log).toContain("Process exited with code 0");
    expect(JSON.stringify(h.previews)).not.toContain(SECRET);
    expect(h.fallback).not.toHaveBeenCalled();
  });

  it("keeps explicit timeout a hard cap before handoff", async () => {
    const h = harness();
    await expect(h.invoke({ command: "sleep 8", timeout: 0.05 })).rejects.toThrow("timed out");
    expect(h.provider.shellJobs.list().every(job => job.status !== "running")).toBe(true);
  });

  it("keeps the explicit hard cap after handoff", async () => {
    const h = harness();
    // Exercise the cap after handoff, not Git Bash startup under Windows CI.
    const timeout = process.platform === "win32" ? 2 : 0.1;
    const result = await h.invoke({ command: "sleep 8", timeout, background: true });
    expect(result.details?.running).toBe(true);
    await vi.waitFor(() => expect(h.provider.shellJobs.list()[0]?.finishedAt).toEqual(expect.any(Number)), { timeout: 4_000 });
    expect(fs.readFileSync(result.details!.logPath!, "utf8")).toContain("timed out");
  });

  it("keeps nonzero status and redacted failure output", async () => {
    const h = harness();
    fs.writeFileSync(path.join(h.cwd, "fixture"), `${SECRET}\n`);
    await expect(h.invoke({ command: "cat fixture; exit 7" })).rejects.toThrow("[filtered]\n\n\nCommand exited with code 7");
  });

  it("allows policy hooks to block before middleware or spawn", async () => {
    const wrapOperations = vi.fn((inner) => inner);
    const h = harness({ middleware: { version: 1, wrapOperations }, blocked: true });
    await expect(h.invoke({ command: "echo forbidden" })).rejects.toThrow("blocked by policy");
    expect(wrapOperations).not.toHaveBeenCalled();
    expect(h.provider.shellJobs.list()).toEqual([]);
  });

  it("cancels a waiting child and cleans up detached children on close", async () => {
    const h = harness();
    const controller = new AbortController();
    const waiting = h.invoke({ command: "sleep 8" }, controller.signal);
    const rejected = expect(waiting).rejects.toThrow();
    // Git Bash on Windows CI can take over a second to start (smarty-dev#883).
    await vi.waitFor(() => expect(h.provider.shellJobs.list()[0]?.pid).toEqual(expect.any(Number)), { timeout: 10_000 });
    controller.abort(new Error("cancel probe"));
    await rejected;
    const result = await h.invoke({ command: "sleep 8", background: true });
    expect(result.details?.running).toBe(true);
    await h.registry.close();
    expect(h.provider.shellJobs.list().every(job => job.status !== "running" && job.status !== "spilled")).toBe(true);
  });

  it.each([{ optIn: false }, { managed: true }])("does not replace unrelated or managed overrides: %j", async options => {
    const h = harness(options);
    expect(await h.invoke({ command: "echo unused" })).toMatchObject({ output: "standalone override" });
    expect(h.fallback).toHaveBeenCalledOnce();
    expect(h.provider.shellJobs.list()).toEqual([]);
  });

  it("retains the selected protection when a lifecycle hook refreshes the catalog", async () => {
    vi.stubEnv("FABRIC_TEST_SECRET", SECRET);
    const h = harness();
    vi.mocked(h.runner.emitToolCall).mockImplementationOnce(async () => {
      h.catalog.clear();
      return undefined;
    });
    const result = await h.invoke({ command: 'printf "%s" "${FABRIC_TEST_SECRET-unset}"' });
    expect(result.output).toBe("unset");
  });

  it("observes catalog refresh rather than caching an obsolete middleware", async () => {
    const h = harness();
    expect((await h.provider.describe("bash", h.context))?.namespace).toBe("extension-middleware");
    h.catalog.replace([], h.runner, DEFAULT_FABRIC_CONFIG.capture, "/fabric/index.ts");
    expect((await h.provider.describe("bash", h.context))?.namespace).toBe("builtin");
  });

  it.each([{ version: 2, wrapOperations: () => ({}) }, { version: 1, wrapOperations: true }, { version: 1, wrapOperations: () => ({}), options: { operations: {} } }])("fails closed for invalid opt-in metadata", async value => {
    const h = harness({ middleware: value });
    await expect(h.invoke({ command: "echo forbidden" })).rejects.toThrow("Invalid Fabric bash middleware");
    expect(h.fallback).not.toHaveBeenCalled();
  });

  it("fails closed and closes the job when a middleware factory fails", async () => {
    const h = harness({ middleware: { version: 1, wrapOperations: () => { throw new Error("filter unavailable"); } } });
    await expect(h.invoke({ command: "echo forbidden" })).rejects.toThrow("filter unavailable");
    expect(h.provider.shellJobs.list().every(job => job.status !== "running")).toBe(true);
    expect(h.fallback).not.toHaveBeenCalled();
  });

  it("validates the public symbol by registry identity", () => {
    expect(FABRIC_BASH_MIDDLEWARE).toBe(Symbol.for("pi-fabric:bash-middleware:v1"));
    expect(readFabricBashMiddleware({})).toBeUndefined();
  });
});
