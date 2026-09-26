import fs from "node:fs";
import os from "node:os";
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

describe("cooperative bash middleware", () => {
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
    await vi.waitFor(() => expect(h.provider.shellJobs.list()[0]?.finishedAt).toEqual(expect.any(Number)));
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
