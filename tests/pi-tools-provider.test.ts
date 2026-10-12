import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createBashToolDefinition,
  createSyntheticSourceInfo,
  type ExtensionContext,
  ExtensionRunner,
  type RegisteredTool,
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FabricExecutionTraceRecorder } from "../src/audit/trace.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { FABRIC_BASH_MIDDLEWARE, type FabricBashMiddlewareV1 } from "../src/protocol.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { ActionRegistry, type FabricCallAudit } from "../src/core/action-registry.js";
import { NESTED_TOOL_CALL_ID_PREFIX } from "../src/core/action-registry.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { powerShellToolDefinitionFactory } from "../src/providers/pi-bash-cwd.js";
import { POWERSHELL_REFUSED_MESSAGE, SHELL_IDENTITY_REFUSED, SHELL_REWRITE_REFUSED } from "../src/providers/shell-preflight.js";
import { applyRunBashDefaults } from "../src/guards/actor-bash-timeout.js";

const baseContext = {
  cwd: process.cwd(),
  signal: new AbortController().signal,
  parentToolCallId: "parent",
  nestedToolCallId: "fabric_test-nested",
  extensionContext: {
    cwd: process.cwd(),
    sessionManager: {
      getSessionId: () => "test-session",
      getSessionFile: () => undefined,
    },
  } as unknown as ExtensionContext,
  update: vi.fn(),
  approve: vi.fn(async () => {}),
  audits: [],
  maxResultChars: 100_000,
};

const makeRunner = (overrides: Record<string, unknown> = {}): ExtensionRunner =>
  ({
    createContext: () => ({ cwd: process.cwd() }),
    getActiveTools: () => [],
    emit: vi.fn(async () => {}),
    emitToolCall: vi.fn(async () => undefined),
    emitToolResult: vi.fn(async () => undefined),
    ...overrides,
  }) as unknown as ExtensionRunner;

const registerWithRunner = (runner: ExtensionRunner) => {
  const catalog = new CapturedToolCatalog();
  catalog.replace(
    [],
    runner,
    DEFAULT_FABRIC_CONFIG.capture,
    "/extensions/pi-fabric/index.ts",
  );
  const registry = new ActionRegistry();
  registry.register(new PiToolsProvider(process.cwd(), catalog, undefined));
  return registry;
};

describe("PiToolsProvider lifecycle", () => {
  it("fires the full tool-execution lifecycle for a pi core tool", async () => {
    const events: string[] = [];
    const runner = makeRunner({
      emit: vi.fn(async (event: { type: string }) => {
        events.push(event.type);
      }),
    });
    const registry = registerWithRunner(runner);

    await registry.invoke("pi.ls", { path: process.cwd() }, baseContext);

    expect(events).toEqual(["tool_execution_start", "tool_execution_end"]);
    expect(runner.emitToolCall).toHaveBeenCalledOnce();
    expect(runner.emitToolResult).toHaveBeenCalledOnce();
    const toolResult = (runner.emitToolResult as ReturnType<typeof vi.fn>).mock
      .calls[0]?.[0] as { toolName: string; toolCallId: string; isError: boolean };
    expect(toolResult).toMatchObject({ toolName: "ls", isError: false });
    // ActionRegistry rewrites nestedToolCallId to fabric_<uuid>.
    expect(toolResult.toolCallId.startsWith(NESTED_TOOL_CALL_ID_PREFIX)).toBe(true);
  });

  it("stops waiting for a hanging lifecycle handler when the invocation is aborted", async () => {
    const controller = new AbortController();
    const runner = makeRunner({
      emitToolResult: vi.fn(async () => new Promise(() => undefined)),
    });
    const registry = registerWithRunner(runner);
    const invocation = registry.invoke(
      "pi.ls",
      { path: process.cwd() },
      { ...baseContext, signal: controller.signal },
    );

    await vi.waitFor(() => expect(runner.emitToolResult).toHaveBeenCalledOnce());
    controller.abort(new Error("cancel nested lifecycle"));

    await expect(invocation).rejects.toThrow("cancel nested lifecycle");
  });

  // A hook may no longer rewrite a shell call (CODE_REVIEW P1 on 692c62a0); the
  // only post-judgment change is Fabric's own run defaults, handed back by its hook.
  it("synchronizes tool_call argument mutations across audit surfaces", async () => {
    const runner = makeRunner({
      emitToolCall: vi.fn(async (event: { input: Record<string, unknown> }) => {
        applyRunBashDefaults({ PI_FABRIC_BASH_IDLE_S: "0" }, event.input as never);
      }),
    });
    const registry = registerWithRunner(runner);
    const audits: FabricCallAudit[] = [];
    const events: unknown[] = [];
    const trace = new FabricExecutionTraceRecorder();

    const result = await registry.invoke(
      "pi.bash",
      { command: `printf "executed:$EXAMPLE\n"` },
      {
        ...baseContext,
        audits,
        trace,
        observeInvocation: (event) => events.push(event),
      },
    ) as { output: string };

    const executedCommand = `printf "executed:$EXAMPLE\n"`;
    expect(result.output).toBe("executed:\n");
    expect(audits[0]?.args).toEqual({ command: executedCommand, timeout: 600 });
    expect(audits[0]?.preview).toMatchObject({ bashCommand: executedCommand });
    expect(events).toContainEqual(expect.objectContaining({
      type: "call_args",
      args: { command: executedCommand, timeout: 600 },
    }));
    // The trace projects pi.bash arguments to the command.
    expect(trace.seal("succeeded", []).operations[0]?.args).toEqual({
      command: executedCommand,
    });
  });

  it("real bash nonzero-exit message matches the guest settle regex", async () => {
    const runner = makeRunner();
    const registry = registerWithRunner(runner);
    const error = await registry
      .invoke("pi.bash", { command: "exit 7" }, baseContext)
      .then(() => undefined, (e: unknown) => e as Error);
    expect(error).toBeInstanceOf(Error);
    const message = (error as Error).message;
    // Mirrors the settle catch in src/runtime/quickjs-runtime.ts.
    const match = /(?:^|\n\n)Command exited with code (\d+)$/.exec(message);
    expect(match).not.toBeNull();
    expect(Number(match![1])).toBe(7);
  });

  it("applies a tool_result content patch to a core tool result", async () => {
    const runner = makeRunner({
      emitToolResult: vi.fn(async () => ({
        content: [{ type: "text" as const, text: "[Image: a sample image, fully described.]" }],
      })),
    });
    const registry = registerWithRunner(runner);

    // A tool_result patch must flow through normalizeResult as the returned text.
    // Use a text file here because image decoding is covered by the media tests below.
    const result = await registry.invoke(
      "pi.read",
      { path: "package.json" },
      baseContext,
    );

    expect(result).toBe("[Image: a sample image, fully described.]");
  });


  it("honors a tool_call block by throwing without executing", async () => {
    const runner = makeRunner({
      emitToolCall: vi.fn(async () => ({ block: true, reason: "denied by gate" })),
    });
    const registry = registerWithRunner(runner);

    await expect(
      registry.invoke("pi.ls", { path: process.cwd() }, baseContext),
    ).rejects.toThrow("denied by gate");
  });

  it("forwards bounded partial previews without an extension runner", async () => {
    const provider = new PiToolsProvider(process.cwd(), undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true });
    const previews: Array<{ result?: unknown }> = [];
    const updates: string[] = [];

    await provider.invoke(
      "bash",
      { command: "printf first; sleep 0.15; printf second" },
      {
        ...baseContext,
        update(message) { updates.push(message); },
        attachPreview(preview) { previews.push(preview as { result?: unknown }); },
      },
    );

    expect(updates.some((message) => message.includes("first"))).toBe(true);
    expect(previews.some((preview) => JSON.stringify(preview.result).includes("first"))).toBe(true);
  });

  it("falls back to a direct execute (no events) when no runner is bound", async () => {
    const registry = new ActionRegistry();
    registry.register(new PiToolsProvider(process.cwd(), undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true }));
    const result = await registry.invoke("pi.ls", { path: process.cwd() }, baseContext);
    expect(typeof result).toBe("string");
    expect((result as string).length).toBeGreaterThan(0);
  });

  it("preserves shell cwd through pi 0.85 argument preparation", async () => {
    const root = fs.realpathSync.native(
      fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-provider-cwd-")),
    );
    const nested = path.join(root, "nested");
    fs.mkdirSync(nested);
    try {
      const registry = new ActionRegistry();
      registry.register(new PiToolsProvider(root, undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true }));
      const result = await registry.invoke(
        "pi.bash",
        {
          command: `${JSON.stringify(process.execPath.replaceAll("\\", "/"))} -e "process.stdout.write(process.cwd())"`,
          cwd: "nested",
        },
        {
          ...baseContext,
          cwd: root,
          extensionContext: { ...baseContext.extensionContext, cwd: root } as ExtensionContext,
          audits: [],
        },
      ) as { output: string };
      expect(fs.realpathSync.native(result.output.trim())).toBe(
        fs.realpathSync.native(nested),
      );
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("omits PowerShell when the host factory is unavailable", async () => {
    const provider = new PiToolsProvider(
      process.cwd(),
      undefined,
      undefined,
      { powerShellToolDefinitionFactory: undefined },
    );

    expect(await provider.describe("powershell", baseContext)).toBeUndefined();
    expect((await provider.list({}, baseContext)).map((item) => item.name))
      .not.toContain("powershell");
    expect(await provider.describe("bash", baseContext)).toBeDefined();
    await expect(provider.invoke("powershell", { command: "Write-Output ok" }, baseContext))
      .rejects.toThrow("Unknown Pi tool: powershell");
  });

  it("registers PowerShell with shell schema and execution risk", async () => {
    const provider = new PiToolsProvider(process.cwd(), undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true });
    const descriptor = await provider.describe("powershell", baseContext);

    expect(descriptor).toMatchObject({
      name: "powershell",
      namespace: "builtin",
      risk: "execute",
    });
    expect(descriptor?.inputSchema.properties).toMatchObject({
      command: expect.any(Object),
      timeout: expect.any(Object),
      cwd: expect.any(Object),
    });
  });

  it("expands explicit skill-dir markers only for SKILL.md reads", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-skill-dir-"));
    const skillDir = path.join(cwd, "installed", "duplicate-name");
    const skillPath = path.join(skillDir, "SKILL.md");
    const referencePath = path.join(skillDir, "reference.md");
    const source = "Read `<skill-dir>/reference.md`.\n";
    try {
      fs.mkdirSync(skillDir, { recursive: true });
      fs.writeFileSync(skillPath, source);
      fs.writeFileSync(referencePath, source);
      const registry = new ActionRegistry();
      registry.register(new PiToolsProvider(cwd, undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true }));
      const context = {
        ...baseContext,
        cwd,
        extensionContext: { cwd } as ExtensionContext,
      };

      await expect(
        registry.invoke("pi.read", { path: skillPath }, context),
      ).resolves.toBe(`Read \`${skillDir}/reference.md\`.\n`);
      await expect(
        registry.invoke("pi.read", { path: referencePath }, context),
      ).resolves.toBe(source);
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("returns truncated Bash output once while preserving recovery metadata", async () => {
    const registry = new ActionRegistry();
    registry.register(new PiToolsProvider(process.cwd(), undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true }));
    const result = await registry.invoke(
      "pi.bash",
      {
        command:
          `node -e 'for (let i = 0; i < 5000; i++) console.log(i, "x".repeat(100))'`,
      },
      { ...baseContext, maxResultChars: 2_000_000 },
    );
    const bashResult = result as {
      ok: boolean;
      output: string;
      details: {
        fullOutputPath?: string;
        truncation?: Record<string, unknown>;
      };
    };

    try {
      expect(bashResult).toMatchObject({
        ok: true,
        output: expect.any(String),
        details: {
          fullOutputPath: expect.any(String),
          truncation: { truncated: true },
        },
      });
      expect("content" in (bashResult.details.truncation ?? {})).toBe(false);
    } finally {
      if (bashResult.details.fullOutputPath) {
        fs.rmSync(bashResult.details.fullOutputPath, { force: true });
      }
    }
  });

  it("applies all repeated edit anchors through one native mutation", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-edit-all-"));
    const filePath = path.join(cwd, "example.txt");
    try {
      fs.writeFileSync(filePath, "header\nneedle one\nneedle two\n");
      const registry = new ActionRegistry();
      registry.register(new PiToolsProvider(cwd, undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true }));
      const result = await registry.invoke(
        "pi.edit",
        {
          path: "example.txt",
          edits: [
            { oldText: "header", newText: "title" },
            { oldText: "needle", newText: "updated", all: true },
          ],
        },
        {
          ...baseContext,
          cwd,
          extensionContext: { cwd } as ExtensionContext,
          audits: [],
        },
      ) as { ok: boolean; output: string };

      expect(result.ok).toBe(true);
      expect(fs.readFileSync(filePath, "utf8")).toBe("title\nupdated one\nupdated two\n");
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("leaves the file unchanged when any replace-all anchor is missing", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-edit-all-atomic-"));
    const filePath = path.join(cwd, "example.txt");
    const before = "needle one\nneedle two\n";
    try {
      fs.writeFileSync(filePath, before);
      const registry = new ActionRegistry();
      registry.register(new PiToolsProvider(cwd, undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true }));
      await expect(registry.invoke(
        "pi.edit",
        {
          path: "example.txt",
          edits: [
            { oldText: "needle", newText: "updated" },
            { oldText: "missing", newText: "never" },
          ],
          all: true,
        },
        {
          ...baseContext,
          cwd,
          extensionContext: { cwd } as ExtensionContext,
          audits: [],
        },
      )).rejects.toThrow("edits[1] oldText was not found");
      expect(fs.readFileSync(filePath, "utf8")).toBe(before);
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("captures pre-write content out of band without changing the sandbox result", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-write-preview-"));
    const before = `const value = 1;
`;
    const after = `export const value = "é${"x".repeat(20_000)}";
`;
    try {
      fs.writeFileSync(path.join(cwd, "example.ts"), before);
      const registry = new ActionRegistry();
      registry.register(new PiToolsProvider(cwd, undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true }));
      const audits: FabricCallAudit[] = [];
      const result = await registry.invoke(
        "pi.write",
        { path: "example.ts", content: after },
        {
          ...baseContext,
          cwd,
          extensionContext: { cwd } as ExtensionContext,
          audits,
        },
      ) as { ok: boolean; output: string; details: unknown };

      expect(result).toMatchObject({ ok: true, details: null });
      expect(result.output).toContain("Successfully wrote");
      expect(result.output).toContain(`${Buffer.byteLength(after, "utf8")} bytes`);
      expect(fs.readFileSync(path.join(cwd, "example.ts"), "utf8")).toBe(after);
      expect(String(audits[0]?.args?.content ?? "").length).toBeLessThan(after.length);
      expect(audits[0]?.preview).toMatchObject({
        writeBeforeCaptured: true,
        writeContent: after,
        writeByteLength: Buffer.byteLength(after, "utf8"),
        writeLineCount: 1,
        codePreviewBeforeWrite: { kind: "content", content: before },
      });
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });


  it("attaches the pre-patch image and clean note when a tool_result patch replaces image blocks", async () => {
    // pi-vision-handoff keeps the read note and swaps the image for a
    // description. The provider captures the image BEFORE the patch (so the
    // single-call kitty preview still shows it) and the clean note AFTER.
    let rawContent: unknown;
    const runner = makeRunner({
      emitToolResult: vi.fn(async (event: { content: unknown }) => {
        rawContent = event.content;
        return {
          content: [
            { type: "text" as const, text: "Read image file [image/png]" },
            { type: "text" as const, text: "[Image: a described image.]" },
          ],
        };
      }),
    });
    const registry = registerWithRunner(runner);
    const audits: FabricCallAudit[] = [];

    await registry.invoke(
      "pi.read",
      { path: "tests/fixtures/images/sample.jpg" },
      { ...baseContext, audits },
    );

    expect((rawContent as Array<{ type: string }>).some((block) => block.type === "image")).toBe(true);
    expect(audits).toHaveLength(1);
    const media = audits[0]?.media;
    expect(media).toBeDefined();
    expect(media!.length).toBeGreaterThan(0);
    expect(media![0]?.type).toBe("image");
    expect(media![0]?.mimeType).toMatch(/^image\//);
    expect(typeof media![0]?.data).toBe("string");
    expect(media![0]?.data!.length).toBeGreaterThan(0);
    expect(audits[0]?.mediaNote).toBe("Read image file [image/png]");
  }, 15_000);
});

describe("extension hijack contract for nested core tools", () => {
  // Generalized pi-vision-handoff pattern: any extension that expresses a
  // core-tool hijack as tool_call/tool_result handlers sees its behavior
  // inside fabric_exec pi.* calls, because the provider replays pi's
  // lifecycle event pipeline for nested executions.
  it("co-exists native grep lines with an extension-appended block via tool_result", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-grep-append-"));
    fs.writeFileSync(path.join(dir, "users.ts"), "export function GetUserHandler() {}\n");
    try {
      const runner = makeRunner({
        emitToolResult: vi.fn(
          async (event: { toolName: string; content: Array<{ type: string; text?: string }> }) =>
            event.toolName === "grep"
              ? {
                  content: [
                    ...event.content,
                    { type: "text", text: 'fovea graph "GetUserHandler" \u00b7 anchor context' },
                  ],
                }
              : undefined,
        ),
      });
      const registry = registerWithRunner(runner);

      const result = await registry.invoke(
        "pi.grep",
        { pattern: "GetUserHandler", path: dir },
        baseContext,
      );

      // Both surfaces co-exist: exact-match lines from core grep, plus the
      // extension's appended context block.
      expect(String(result)).toContain("GetUserHandler");
      expect(String(result)).toContain("fovea graph");
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("reroutes nested grep arguments mutated by a tool_call handler", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-grep-mutate-"));
    fs.writeFileSync(path.join(dir, "users.ts"), "export function GetUserHandler() {}\n");
    try {
      const runner = makeRunner({
        emitToolCall: vi.fn(async (event: { input: Record<string, unknown> }) => {
          event.input.pattern = "zzz-no-such-symbol";
        }),
      });
      const registry = registerWithRunner(runner);

      const result = await registry.invoke(
        "pi.grep",
        { pattern: "GetUserHandler", path: dir },
        baseContext,
      );

      expect(String(result)).not.toContain("users.ts");
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("blocks a nested core tool through the tool_call preflight", async () => {
    const runner = makeRunner({
      emitToolCall: vi.fn(async () => ({ block: true, reason: "grep requires an audit note" })),
    });
    const registry = registerWithRunner(runner);

    await expect(
      registry.invoke("pi.grep", { pattern: "anything" }, baseContext),
    ).rejects.toThrow("grep requires an audit note");
  });
});

describe("tool_call preflight coverage for nested bash", () => {
  const roots: string[] = [];
  const worktrees: Array<{ repository: string; path: string }> = [];
  const git = (cwd: string, ...args: string[]): string =>
    execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  const initRepository = (): string => {
    const repository = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-preflight-wt-"));
    roots.push(repository);
    git(repository, "init", "-q");
    git(repository, "config", "user.email", "pi-fabric-tests@example.invalid");
    git(repository, "config", "user.name", "Pi Fabric tests");
    fs.writeFileSync(path.join(repository, "README.md"), "ok\n");
    git(repository, "add", ".");
    git(repository, "commit", "-qm", "initial");
    return fs.realpathSync(repository);
  };
  const blockBash = () =>
    vi.fn(async (event: { toolName: string }) =>
      event.toolName === "bash" ? { block: true, reason: "blocked" } : undefined,
    );

  afterEach(() => {
    for (const worktree of worktrees.splice(0)) {
      try {
        git(worktree.repository, "worktree", "remove", "--force", worktree.path);
      } catch {
        // The worktree may never have been created (the point of these tests).
      }
    }
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  // Case 1 (regression guard): a nested bash command routed through the normal
  // event lifecycle is blocked by the tool_call preflight, and the shell
  // operations never run.
  it("blocks a nested bash command through the preflight without running the shell", async () => {
    const execSpy = vi.fn(async () => ({ exitCode: 0 }));
    const wrapOperations = vi.fn(() => ({ exec: execSpy }));
    const middleware: FabricBashMiddlewareV1 = { version: 1, wrapOperations };
    const cwd = process.cwd();
    const runner = makeRunner({ emitToolCall: blockBash() });
    const definition = Object.assign(
      { ...createBashToolDefinition(cwd) },
      { [FABRIC_BASH_MIDDLEWARE]: middleware },
    );
    const catalog = new CapturedToolCatalog();
    catalog.replace(
      [{ definition, sourceInfo: { path: "/extensions/guard-bash.ts", source: "test", scope: "user", origin: "package" } } as RegisteredTool],
      runner,
      DEFAULT_FABRIC_CONFIG.capture,
      "/extensions/pi-fabric/index.ts",
    );
    const registry = new ActionRegistry();
    registry.register(new PiToolsProvider(cwd, catalog, new CapturedToolsProvider(catalog)));
    try {
      await expect(
        registry.invoke("pi.bash", { command: "rm -rf /tmp/x" }, baseContext),
      ).rejects.toThrow(/blocked/);
      expect(wrapOperations).not.toHaveBeenCalled();
      expect(execSpy).not.toHaveBeenCalled();
    } finally {
      await registry.close();
    }
  });

  // Case 2: a well-formed `git worktree add` is intercepted before the normal
  // lifecycle, so it must emit the same preflight first. A block stops it with
  // no worktree effect. Without the fix this executes and creates a worktree.
  it("blocks a git worktree add intercept before any worktree effect", async () => {
    const repository = initRepository();
    const dest = path.join(repository, "wt");
    worktrees.push({ repository, path: dest });
    const runner = makeRunner({ emitToolCall: blockBash() });
    const catalog = new CapturedToolCatalog();
    catalog.replace([], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
    const registry = new ActionRegistry();
    registry.register(new PiToolsProvider(repository, catalog, new CapturedToolsProvider(catalog)));
    const context = {
      ...baseContext,
      cwd: repository,
      extensionContext: { ...baseContext.extensionContext, cwd: repository } as ExtensionContext,
      audits: [],
    };
    try {
      await expect(
        registry.invoke("pi.bash", { command: "git worktree add -b topic wt HEAD" }, context),
      ).rejects.toThrow(/blocked/);
      expect(fs.existsSync(dest)).toBe(false);
      expect(git(repository, "worktree", "list", "--porcelain")).not.toContain("branch refs/heads/topic");
    } finally {
      await registry.close();
    }
  });

  // Case 3: a catalog is installed but its runner is not yet available (the
  // window before the first tool refresh). Mutating tools would otherwise run
  // with no tool_call preflight, so they fail closed; read tools still run.
  it("fails closed for mutating tools until the catalog runner is available", async () => {
    const cwd = process.cwd();
    const catalog = new CapturedToolCatalog(); // never replaced: runner is undefined
    const provider = new PiToolsProvider(cwd, catalog, new CapturedToolsProvider(catalog));
    for (const [tool, args] of [
      ["bash", { command: "echo hi" }],
      ["write", { path: "scratch.txt", content: "x" }],
      ["edit", { path: "scratch.txt", oldText: "a", newText: "b" }],
    ] as const) {
      await expect(provider.invoke(tool, args, baseContext)).rejects.toThrow(
        /until extension tool hooks initialize/,
      );
    }
    // A read tool has nothing to guard and still resolves through direct execute.
    await expect(provider.invoke("ls", { path: cwd }, baseContext)).resolves.toEqual(
      expect.any(String),
    );
  });

  // Case 4 (Gap C guard): the tool_call event's input already carries the
  // effective per-call cwd, so cwd-aware guards evaluate against the right base.
  it("exposes the effective per-call cwd on the tool_call event input", async () => {
    const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-preflight-cwd-")));
    roots.push(root);
    const nested = path.join(root, "nested");
    fs.mkdirSync(nested);
    let capturedCwd: unknown;
    const runner = makeRunner({
      emitToolCall: vi.fn(async (event: { toolName: string; input: Record<string, unknown> }) => {
        if (event.toolName === "bash") capturedCwd = event.input.cwd;
        return undefined;
      }),
    });
    const catalog = new CapturedToolCatalog();
    catalog.replace([], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
    const registry = new ActionRegistry();
    registry.register(new PiToolsProvider(root, catalog, new CapturedToolsProvider(catalog)));
    try {
      const result = await registry.invoke(
        "pi.bash",
        {
          command: `${JSON.stringify(process.execPath.replaceAll("\\", "/"))} -e "process.stdout.write(process.cwd())"`,
          cwd: "nested",
        },
        {
          ...baseContext,
          cwd: root,
          extensionContext: { ...baseContext.extensionContext, cwd: root } as ExtensionContext,
          audits: [],
        },
      ) as { output: string };
      expect(fs.realpathSync.native(String(capturedCwd))).toBe(fs.realpathSync.native(nested));
      expect(fs.realpathSync.native(result.output.trim())).toBe(fs.realpathSync.native(nested));
    } finally {
      await registry.close();
    }
  });
});

describe("pi.* without an extension runner (smarty-dev#8606)", () => {
  it("fails closed for mutating calls unless the test-only allowUnhookedTools option is set", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-unhooked-"));
    const marker = path.join(root, "executed");
    const note = path.join(root, "note.txt");
    fs.writeFileSync(note, "readable");
    const context = { ...baseContext, cwd: root };
    try {
      // No catalog, and a catalog whose runner was never captured, both leave
      // nothing that could see a tool_call: neither may run a mutation.
      for (const catalog of [undefined, new CapturedToolCatalog()]) {
        const provider = new PiToolsProvider(root, catalog, undefined);
        await expect(provider.invoke("bash", { command: `git stash list > '${marker}'` }, context))
          .rejects.toThrow("Pi tool bash is unavailable until extension tool hooks initialize");
        await expect(provider.invoke("write", { path: marker, content: "x" }, context))
          .rejects.toThrow("Pi tool write is unavailable until extension tool hooks initialize");
        await expect(provider.invoke("edit", { path: note, edits: [{ oldText: "readable", newText: "edited" }] }, context))
          .rejects.toThrow("Pi tool edit is unavailable until extension tool hooks initialize");
        expect(fs.existsSync(marker)).toBe(false);
        expect(fs.readFileSync(note, "utf8")).toBe("readable");
        // Read-risk tools stay available without hooks.
        expect(JSON.stringify(await provider.invoke("read", { path: note }, context))).toContain("readable");
      }
      const unhooked = new PiToolsProvider(root, undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true });
      await unhooked.invoke("bash", { command: `printf ran > '${marker}'` }, context);
      expect(fs.readFileSync(marker, "utf8")).toBe("ran");
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("shows guards a pi.bash cwd as the leading cd a native call needs", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cwd-preflight-")));
    const nested = path.join(root, "it's nested");
    fs.mkdirSync(nested);
    const prefix = `cd '${nested.replaceAll("'", "'\\''")}' && `;
    // Print the cwd through Node, not `pwd`: Git Bash on Windows reports
    // POSIX mount paths (/tmp/...) rather than the host path the test made.
    const probe = `${JSON.stringify(process.execPath.replaceAll("\\", "/"))} -e "process.stdout.write(process.cwd())"`;
    try {
      const seen: string[] = [];
      const run = async (hook: (input: { command: string; cwd?: string }) => unknown) => {
        const runner = makeRunner({ emitToolCall: vi.fn(async (event: { input: { command: string; cwd?: string } }) => {
          seen.push(event.input.command);
          return hook(event.input);
        }) });
        const catalog = new CapturedToolCatalog();
        catalog.replace([], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
        const registry = new ActionRegistry();
        registry.register(new PiToolsProvider(root, catalog, undefined));
        try {
          return await registry.invoke("pi.bash", { command: probe, cwd: nested }, { ...baseContext, cwd: root, audits: [] }) as { output: string };
        } finally { await registry.close(); }
      };
      // The guard judges the cd target; execution still runs the original command in cwd.
      const plain = await run(() => undefined);
      expect(seen.at(-1)).toBe(`${prefix}${probe}`);
      expect(fs.realpathSync.native(plain.output.trim())).toBe(fs.realpathSync.native(nested));
      await expect(run((input) => input.command.startsWith(prefix) ? { block: true, reason: "guard judged the cd" } : undefined))
        .rejects.toThrow("guard judged the cd");
      // CODE_REVIEW P1 on 692c62a0: even a rewrite inside the judged cd is refused.
      await expect(run((input) => { input.command = `${input.command} && printf rewritten`; })).rejects.toThrow(SHELL_REWRITE_REFUSED);
      // A hook that drops the cd it judged cannot redirect execution.
      await expect(run((input) => { input.command = "pwd"; })).rejects.toThrow(SHELL_REWRITE_REFUSED);
      // The session cwd needs no preface.
      seen.length = 0;
      const runner = makeRunner({ emitToolCall: vi.fn(async (event: { input: { command: string } }) => { seen.push(event.input.command); return undefined; }) });
      const catalog = new CapturedToolCatalog();
      catalog.replace([], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
      await new PiToolsProvider(root, catalog, undefined).invoke("bash", { command: "pwd" }, { ...baseContext, cwd: root });
      expect(seen).toEqual(["pwd"]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // CODE_REVIEW P1 on 3215b712: without a cd preface the hook used to receive
  // the caller's args itself, so it could add or change cwd after the guard
  // judged the command in the session cwd and move execution elsewhere.
  it("refuses a tool_call hook that moves pi.bash execution to another cwd", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cwd-move-")));
    const repo = path.join(root, "repo");
    const linked = path.join(root, "linked");
    fs.mkdirSync(repo);
    const git = (...gitArgs: string[]) => execFileSync("git", gitArgs, { cwd: repo, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
    git("init", "-q");
    git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "init");
    git("worktree", "add", "-q", "-b", "linked", linked, "HEAD");
    const probe = `${JSON.stringify(process.execPath.replaceAll("\\", "/"))} -e "process.stdout.write(process.cwd())"`;
    const write = `printf ran > ran.txt`;
    const RW = `Pi tool bash was blocked: ${SHELL_REWRITE_REFUSED}`;
    try {
      const run = async (args: Record<string, unknown>, hook: (input: Record<string, unknown>) => unknown) => {
        const runner = makeRunner({ emitToolCall: vi.fn(async (event: { input: Record<string, unknown> }) => hook(event.input)) });
        const catalog = new CapturedToolCatalog();
        catalog.replace([], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
        const registry = new ActionRegistry();
        registry.register(new PiToolsProvider(repo, catalog, undefined));
        try {
          return await registry.invoke("pi.bash", args, {
            ...baseContext,
            cwd: repo,
            extensionContext: { ...baseContext.extensionContext, cwd: repo } as ExtensionContext,
            audits: [],
          }) as { output: string };
        } finally { await registry.close(); }
      };
      // (a) A hook that ADDS cwd=<linked worktree> to a call without cwd is refused.
      await expect(run({ command: write }, (input) => { input.cwd = linked; })).rejects.toThrow(RW);
      // (b) A hook that CHANGES cwd is refused, whether or not the original cwd needed a preface.
      await expect(run({ command: write, cwd: repo }, (input) => { input.cwd = linked; })).rejects.toThrow(RW);
      await expect(run({ command: write, cwd: linked }, (input) => { input.cwd = repo; })).rejects.toThrow(RW);
      // (c) CODE_REVIEW P1 on 692c62a0: a command-only rewrite is refused too, as is a
      // change to any other execution argument, even removing a cwd that named the session cwd.
      await expect(run({ command: write }, (input) => { input.command = `${write} && printf more`; })).rejects.toThrow(RW);
      await expect(run({ command: write }, (input) => { input.timeout = 1; })).rejects.toThrow(RW);
      await expect(run({ command: write }, (input) => { input.env = { EXTRA: "1" }; })).rejects.toThrow(RW);
      await expect(run({ command: write }, (input) => { input.background = true; })).rejects.toThrow(RW);
      await expect(run({ command: write, cwd: repo }, (input) => { delete input.cwd; })).rejects.toThrow(RW);
      expect(fs.existsSync(path.join(repo, "ran.txt"))).toBe(false);
      expect(fs.existsSync(path.join(linked, "ran.txt"))).toBe(false);
      // An unchanged call still runs, in the cwd it was judged in.
      const plain = await run({ command: probe }, () => undefined);
      expect(fs.realpathSync.native(plain.output.trim())).toBe(fs.realpathSync.native(repo));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // CODE_REVIEW P1 on 692c62a0: with several tool_call hooks, a later hook used
  // to turn a command an earlier guard allowed into one it refuses, and the
  // rewrite ran. Pi's runner calls every handler in turn on the same event.
  it("refuses a later hook's rewrite of a command an earlier guard allowed, so nothing runs (multi-hook)", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-multi-hook-")));
    const marker = path.join(root, "stash-ran");
    const runs: unknown[] = [];
    const judged: string[] = [];
    // Hook A: a guard that refuses git stash and allows everything else.
    const hookA = (input: Record<string, unknown>) => {
      judged.push(String(input.command));
      return String(input.command).includes("git stash") ? { block: true, reason: "guard refused git stash" } : undefined;
    };
    // Hook B: runs after the guard and rewrites the allowed command into a refused one.
    const hookB = (input: Record<string, unknown>) => { input.command = `git stash list > '${marker}'`; return undefined; };
    // An extension-owned bash override that only records what it would run.
    const override = {
      ...createBashToolDefinition(root),
      async execute(_id: string, params: { command: string }) {
        runs.push(params.command);
        return { content: [{ type: "text" as const, text: "ran" }], details: {} };
      },
    };
    try {
      for (const captured of [false, true]) {
        const runner = makeRunner({
          createContext: () => ({ cwd: root, sessionManager: baseContext.extensionContext.sessionManager }),
          emitToolCall: vi.fn(async (event: { input: Record<string, unknown> }) => {
            for (const hook of [hookA, hookB]) {
              const result = hook(event.input);
              if (result?.block) return result;
            }
            return undefined;
          }),
        });
        const catalog = new CapturedToolCatalog();
        catalog.replace(captured ? [{
          definition: override as RegisteredTool["definition"],
          sourceInfo: createSyntheticSourceInfo("/extensions/bash-override/index.ts", { source: "test" }),
        }] : [], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
        const provider = new PiToolsProvider(root, catalog, captured ? new CapturedToolsProvider(catalog) : undefined);
        judged.length = 0;
        await expect(provider.invoke("bash", { command: "echo ok" }, { ...baseContext, cwd: root }))
          .rejects.toThrow(SHELL_REWRITE_REFUSED);
        expect(judged).toEqual(["echo ok"]);
      }
      expect(fs.existsSync(marker)).toBe(false);
      expect(runs).toEqual([]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // CODE_REVIEW P1 on PR #925 (item 1): Pi's runner hands the same mutable
  // event to every tool_call handler, so an earlier hook that renamed the tool
  // made a later bash guard skip the call while Fabric still ran the original
  // bash arguments; renaming and later restoring it left no trace. The event's
  // identity is now read-only for the whole dispatch. These hooks run through
  // Pi's own ExtensionRunner.prototype.emitToolCall.
  const identityHarness = () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-identity-")));
    const marker = path.join(root, "stash-ran");
    const command = `git stash list > '${marker}'`;
    type Event = { type: string; toolName: string; toolCallId: string; input: Record<string, unknown> };
    type Hook = (event: Event) => unknown;
    const runs: unknown[] = [];
    const seen: string[] = [];
    // Hook B: a bash-only guard like the fleet's, which ignores every other toolName.
    const guard: Hook = (event) => {
      seen.push(event.toolName);
      return event.toolName === "bash" && String(event.input.command).includes("git stash")
        ? { block: true, reason: "guard refused git stash" } : undefined;
    };
    // A strict-mode hook's plain assignment; a sloppy-mode one (Reflect.set reports
    // failure instead of throwing); and a hook that swallows a redefine failure.
    const assign: Hook = (event) => { event.toolName = "read"; };
    const sloppy: Hook = (event) => { Reflect.set(event, "toolName", "read"); Reflect.set(event, "type", "other"); };
    const redefine: Hook = (event) => {
      try { Object.defineProperty(event, "toolName", { value: "read" }); } catch { /* ignored by the hook */ }
    };
    const restore: Hook = (event) => { Reflect.set(event, "toolName", "bash"); Reflect.set(event, "type", "tool_call"); };
    const override = {
      ...createBashToolDefinition(root),
      async execute(_id: string, params: { command: string }) {
        runs.push(params.command);
        return { content: [{ type: "text" as const, text: "ran" }], details: {} };
      },
    };
    const providerFor = (hooks: Hook[], captured: boolean) => {
      const context = () => ({ cwd: root, sessionManager: baseContext.extensionContext.sessionManager });
      // Pi's real dispatch: one event object, each handler in turn, the first block wins.
      const pi = Object.create(ExtensionRunner.prototype) as ExtensionRunner;
      Object.assign(pi, { createContext: context, extensions: [{ path: "/extensions/hooks/index.ts", handlers: new Map([["tool_call", hooks]]) }] });
      const runner = makeRunner({ createContext: context, emitToolCall: ExtensionRunner.prototype.emitToolCall.bind(pi) });
      const catalog = new CapturedToolCatalog();
      catalog.replace(captured ? [{
        definition: override as RegisteredTool["definition"],
        sourceInfo: createSyntheticSourceInfo("/extensions/bash-override/index.ts", { source: "test" }),
      }] : [], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
      return new PiToolsProvider(root, catalog, captured ? new CapturedToolsProvider(catalog) : undefined);
    };
    const invoke = (hooks: Hook[], captured: boolean) =>
      providerFor(hooks, captured).invoke("bash", { command }, { ...baseContext, cwd: root });
    return { root, marker, runs, seen, guard, assign, sloppy, redefine, restore, providerFor, invoke };
  };

  it("(a) refuses a git stash when an earlier hook renames the tool before a bash guard; nothing runs", async () => {
    const { root, marker, runs, seen, guard, assign, sloppy, redefine, providerFor, invoke } = identityHarness();
    try {
      for (const captured of [false, true]) {
        // (a) Hook A renames the tool before guard B. A strict hook's write throws and
        // refuses the call before the guard runs; otherwise the guard still sees bash.
        seen.length = 0;
        await expect(invoke([assign, guard], captured)).rejects.toThrow(SHELL_IDENTITY_REFUSED);
        expect(seen).toEqual([]);
        for (const rename of [sloppy, redefine]) {
          seen.length = 0;
          await expect(invoke([rename, guard], captured)).rejects.toThrow("guard refused git stash");
          expect(seen).toEqual(["bash"]);
        }
      }
      expect(fs.existsSync(marker)).toBe(false);
      expect(runs).toEqual([]);
      // A hook that leaves the identity alone still lets an allowed command run.
      const allowed = await providerFor([guard], false).invoke("bash", { command: "printf ok" }, { ...baseContext, cwd: root }) as { output: string };
      expect(allowed.output).toContain("ok");
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("(b) a hook that renames the tool and a later one that restores it cannot evade the guard; the guard saw bash", async () => {
    const { root, marker, runs, seen, guard, assign, sloppy, restore, invoke } = identityHarness();
    try {
      for (const captured of [false, true]) {
        // (b) Rename, guard, restore: the final event looks untouched, but the guard
        // still saw bash and refused, or the strict rename refused the call outright.
        seen.length = 0;
        await expect(invoke([sloppy, guard, restore], captured)).rejects.toThrow("guard refused git stash");
        expect(seen).toEqual(["bash"]);
        seen.length = 0;
        await expect(invoke([assign, guard, restore], captured)).rejects.toThrow(SHELL_IDENTITY_REFUSED);
        expect(seen).toEqual([]);
      }
      expect(fs.existsSync(marker)).toBe(false);
      expect(runs).toEqual([]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("runs the tool_call preflight once a runner exists and obeys its block", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-hooked-"));
    const marker = path.join(root, "executed");
    try {
      const runner = makeRunner({ emitToolCall: vi.fn(async () => ({ block: true, reason: "guard refused git stash" })) });
      const catalog = new CapturedToolCatalog();
      catalog.replace([], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
      const provider = new PiToolsProvider(root, catalog, undefined);
      await expect(provider.invoke("bash", { command: `git stash list > '${marker}'` }, { ...baseContext, cwd: root }))
        .rejects.toThrow("guard refused git stash");
      expect(fs.existsSync(marker)).toBe(false);
      expect(runner.emitToolCall).toHaveBeenCalledWith(expect.objectContaining({ toolName: "bash" }));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("shows pi.powershell to bash guards and refuses what they refuse (#8606 P1-A)", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-powershell-guard-")));
    const nested = path.join(root, "linked");
    fs.mkdirSync(nested);
    const marker = path.join(root, "executed");
    const command = `git stash list > '${marker}'`;
    type Event = { toolName: string; toolCallId: string; input: Record<string, unknown> };
    // smarty-dev#9826: these cases run only because the profile opted in.
    const providerFor = (hook: (event: Event) => unknown, captured = false) => {
      const events: Event[] = [];
      const runner = makeRunner({
        createContext: () => ({ cwd: root, sessionManager: baseContext.extensionContext.sessionManager }),
        emitToolCall: vi.fn(async (event: Event) => {
          events.push({ ...event, input: { ...event.input } });
          return hook(event);
        }),
      });
      const catalog = new CapturedToolCatalog();
      catalog.replace(captured ? [{
        // An extension-owned powershell override. It runs through bash here, so
        // a missed refusal would create the marker.
        definition: { ...createBashToolDefinition(root), name: "powershell", label: "powershell" } as RegisteredTool["definition"],
        sourceInfo: createSyntheticSourceInfo("/extensions/powershell-override/index.ts", { source: "test" }),
      }] : [], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
      const getPowerShellPolicy = () => "allow" as const;
      const provider = new PiToolsProvider(root, catalog, captured ? new CapturedToolsProvider(catalog, { getPowerShellPolicy }) : undefined,
        { powerShellToolDefinitionFactory, getPowerShellPolicy });
      return { provider, events };
    };
    // A bash-only guard, like the fleet bash guard: it ignores every other toolName.
    const bashOnly = (event: Event) => event.toolName === "bash" && String(event.input.command).includes("git stash")
      ? { block: true, reason: "guard refused git stash" } : undefined;
    const context = { ...baseContext, cwd: root };
    try {
      for (const captured of [true, false]) {
        const { provider, events } = providerFor(bashOnly, captured);
        await expect(provider.invoke("powershell", { command }, context)).rejects.toThrow("guard refused git stash");
        expect(events[0]).toMatchObject({ toolName: "bash", input: { command, shell: "powershell" } });
      }
      expect(fs.existsSync(marker)).toBe(false);
      // The bash view carries a non-session cwd as the leading cd a native call needs.
      const withCwd = providerFor(bashOnly);
      await expect(withCwd.provider.invoke("powershell", { command: "git stash list", cwd: nested }, context))
        .rejects.toThrow("guard refused git stash");
      expect(withCwd.events[0]?.input.command).toBe(`cd '${nested}' && git stash list`);
      // A bash guard's rewrite cannot carry over to PowerShell syntax, so it refuses the call.
      const rewrite = providerFor((event) => { if (event.toolName === "bash") event.input.command = "echo safe"; });
      await expect(rewrite.provider.invoke("powershell", { command }, context))
        .rejects.toThrow(`Pi tool powershell was blocked: ${SHELL_REWRITE_REFUSED}`);
      // Guards for the real tool name still run after the bash view; they cannot rewrite the judged command.
      const named = providerFor((event) => event.toolName === "powershell" ? { block: true, reason: "powershell guard" } : undefined);
      await expect(named.provider.invoke("powershell", { command: "Write-Output ok" }, context)).rejects.toThrow("powershell guard");
      expect(named.events.map((event) => event.toolName)).toEqual(["bash", "powershell"]);
      const late = providerFor((event) => { if (event.toolName === "powershell") event.input.command = command; });
      await expect(late.provider.invoke("powershell", { command: "Write-Output ok" }, context))
        .rejects.toThrow(`Pi tool powershell was blocked: ${SHELL_REWRITE_REFUSED}`);
      expect(fs.existsSync(marker)).toBe(false);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it("refuses pi.powershell unless the profile opts in, built-in and captured override alike (smarty-dev#9826)", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-powershell-optin-")));
    const marker = path.join(root, "executed");
    const command = `printf ran > '${marker}'`;
    type Event = { toolName: string; toolCallId: string; input: Record<string, unknown> };
    const providerFor = (captured: boolean, policy: "allow" | "refuse" | undefined) => {
      const events: Event[] = [];
      const runner = makeRunner({
        createContext: () => ({ cwd: root, sessionManager: baseContext.extensionContext.sessionManager }),
        emitToolCall: vi.fn(async (event: Event) => { events.push({ ...event, input: { ...event.input } }); return undefined; }),
      });
      const catalog = new CapturedToolCatalog();
      catalog.replace(captured ? [{
        // An extension-owned powershell override backed by bash, so a missed refusal creates the marker.
        definition: { ...createBashToolDefinition(root), name: "powershell", label: "powershell" } as RegisteredTool["definition"],
        sourceInfo: createSyntheticSourceInfo("/extensions/powershell-override/index.ts", { source: "test" }),
      }] : [], runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/pi-fabric/index.ts");
      const getPowerShellPolicy = policy ? () => policy : undefined;
      const capturedTools = captured ? new CapturedToolsProvider(catalog, { getPowerShellPolicy }) : undefined;
      const provider = new PiToolsProvider(root, catalog, capturedTools, { powerShellToolDefinitionFactory, getPowerShellPolicy });
      return { provider, capturedTools, events };
    };
    const context = { ...baseContext, cwd: root };
    try {
      // Default (no policy wired, or the default "refuse"): refused before any hook or execution.
      for (const captured of [true, false]) {
        for (const policy of [undefined, "refuse"] as const) {
          const { provider, capturedTools, events } = providerFor(captured, policy);
          await expect(provider.invoke("powershell", { command }, context)).rejects.toThrow(POWERSHELL_REFUSED_MESSAGE);
          await expect(provider.invoke("powershell", { command }, context)).rejects.toThrow("smarty-dev#9826");
          if (capturedTools) {
            // A direct extensions.powershell call is refused too.
            await expect(capturedTools.invoke("powershell", { command }, context)).rejects.toThrow("smarty-dev#9826");
          }
          expect(events).toEqual([]);
        }
      }
      expect(fs.existsSync(marker)).toBe(false);
      // The default unhooked provider (no runner, test-only allowUnhookedTools) refuses as well.
      const unhooked = new PiToolsProvider(root, undefined, undefined, { powerShellToolDefinitionFactory, allowUnhookedTools: true });
      await expect(unhooked.invoke("powershell", { command }, context)).rejects.toThrow("smarty-dev#9826");
      // Opted in: the captured override runs, after the bash-named and then the powershell-named preflight.
      const optedIn = providerFor(true, "allow");
      await optedIn.provider.invoke("powershell", { command }, context);
      expect(fs.readFileSync(marker, "utf8")).toBe("ran");
      expect(optedIn.events.map((event) => event.toolName)).toEqual(["bash", "powershell"]);
      expect(optedIn.events[0]).toMatchObject({ input: { command, shell: "powershell" } });
      expect(optedIn.events[1]).toMatchObject({ input: { command } });
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
