import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { createBashToolDefinition, createExtensionRuntime, ExtensionRunner, SessionManager, type Extension, type ModelRegistry, type RegisteredTool } from "@earendil-works/pi-coding-agent";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, liveLandlockSettings, loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { LandlockBashConfinement } from "../src/core/landlock.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import type { LandlockSettings } from "../src/core/landlock.js";
import { FABRIC_BASH_MIDDLEWARE, type FabricBashMiddlewareV1 } from "../src/protocol.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";

const roots: string[] = [];
const registries: ActionRegistry[] = [];
const quote = (text: string) => `'${text.replaceAll("'", "'\\''")}'`;
const helper = path.resolve("dist/native/fabric-landlock");

const harness = (settings: LandlockSettings = { mode: "enforce", disabled: false },
  opt: { opaque?: boolean; managed?: boolean; blocked?: boolean; escapeEnv?: string; gate?: { entered: () => void; open: Promise<void> } } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-test-"));
  roots.push(root);
  const cwd = path.join(root, "lane");
  const sibling = path.join(root, "sibling");
  const tmpdir = path.join(root, "private-tmp");
  for (const directory of [cwd, sibling, tmpdir]) fs.mkdirSync(directory, { mode: 0o700 });
  fs.writeFileSync(path.join(sibling, "victim"), "keep-me");
  vi.stubEnv("TMPDIR", tmpdir);
  vi.stubEnv("SMARTY_ROLE", "task-agent@reviewed-policy");
  const middleware: FabricBashMiddlewareV1 = {
    version: 1, options: {
      commandPrefix: "export LANDLOCK_PREFIX=preserved",
      spawnHook: ({ env, ...rest }) => {
        const filtered: NodeJS.ProcessEnv = { ...env, LANDLOCK_SPAWN: "preserved" };
        delete filtered.FABRIC_TEST_SECRET;
        // Even a cooperative env rewrite must not widen kernel policy.
        filtered.PI_FABRIC_LANDLOCK_WRITES = "/";
        if (opt.escapeEnv !== undefined) filtered.PI_FABRIC_LANDLOCK_ESCAPE = opt.escapeEnv;
        return { ...rest, env: filtered };
      },
    },
    wrapOperations: inner => ({ exec: async (command, directory, options) => {
      // Optional delayed preparation before delegating to the supplied operations.
      if (opt.gate) { opt.gate.entered(); await opt.gate.open; }
      return inner.exec(command, directory, {
        ...options, onData: data => options.onData(Buffer.from(data.toString().replaceAll("filter-me", "[filtered]"))),
      });
    } }),
  };
  const fallback = vi.fn(async () => ({ content: [{ type: "text" as const, text: "opaque" }], details: undefined }));
  const definition = { ...createBashToolDefinition(cwd), execute: fallback };
  if (!opt.opaque) Object.assign(definition, { [FABRIC_BASH_MIDDLEWARE]: middleware });
  const sourceInfo = { path: "/test/local-middleware.ts", source: "test", scope: "user" as const, origin: "package" as const };
  const extension: Extension = {
    path: sourceInfo.path, resolvedPath: sourceInfo.path, sourceInfo,
    tools: new Map([["bash", { definition, sourceInfo } as RegisteredTool]]),
    handlers: new Map([["tool_call", [async () => opt.blocked ? { block: true, reason: "early warning" } : undefined]]]),
    flags: new Map(), commands: new Map(), shortcuts: new Map(), messageRenderers: new Map(),
  };
  const runtime = createExtensionRuntime();
  runtime.getThinkingLevel = () => "off";
  runtime.getActiveTools = () => ["bash"];
  // Genuine Pi runner and in-memory session; no agent/model/auth storage is created.
  const runner = new ExtensionRunner([extension], runtime, cwd, SessionManager.inMemory(cwd), {} as ModelRegistry);
  vi.spyOn(runner, "emitToolCall"); vi.spyOn(runner, "emitToolResult");
  const extensionContext = runner.createContext();
  const catalog = new CapturedToolCatalog();
  catalog.replace(runner.getAllRegisteredTools(), runner, DEFAULT_FABRIC_CONFIG.capture, "/fabric/index.ts");
  const provider = new PiToolsProvider(cwd, catalog, new CapturedToolsProvider(catalog), {
    powerShellToolDefinitionFactory: undefined, getShellHangMs: () => 0,
    getLandlockSettings: () => settings, requireCapturedOverrides: !!opt.managed,
  });
  const registry = new ActionRegistry();
  registry.register(provider);
  registries.push(registry);
  const context = { cwd, extensionContext, signal: new AbortController().signal,
    parentToolCallId: "landlock-parent", nestedToolCallId: "landlock-bash",
    update: () => {}, approve: async () => {}, audits: [], maxResultChars: 100_000,
  };
  const invoke = async (args: Record<string, unknown>, signal = context.signal): Promise<{
    ok: boolean; output: string; details: { running?: boolean; logPath?: string } | null;
  }> => {
    const { settle, ...input } = args;
    try { return await registry.invoke("pi.bash", input, { ...context, signal }) as {
      ok: boolean; output: string; details: { running?: boolean; logPath?: string } | null;
    }; } catch (error) {
      // Match pi.bash's bridge settle option while retaining the real provider path.
      if (settle !== true) throw error;
      return { ok: false, output: error instanceof Error ? error.message : String(error), details: null };
    }
  };
  const audit = () => fs.readFileSync(path.join(cwd, ".pi/landlock-audit.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
  return { root, cwd, sibling, tmpdir, provider, registry, runner, fallback, invoke, settings, audit };
};

afterEach(async () => {
  await Promise.all(registries.splice(0).map(registry => registry.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("Landlock settings", () => {
  it("defaults off, accepts enforce and rejects fake warn data", () => {
    expect(DEFAULT_FABRIC_CONFIG.executor.landlock).toEqual({ mode: "off", disabled: false });
    expect(normalizeFabricConfig({ executor: { landlock: { mode: "enforce" } } }).executor.landlock.mode).toBe("enforce");
    expect(() => normalizeFabricConfig({ executor: { landlock: { mode: "warn" } } })).toThrow("no honest warn/audit mode");
  });
  it("keeps the fleet kill switch host-only", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-config-"));
    roots.push(root);
    const agentDir = path.join(root, "profile");
    const cwd = path.join(root, "lane");
    fs.mkdirSync(agentDir); fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ executor: { landlock: { disabled: true } } }));
    fs.writeFileSync(path.join(cwd, ".pi/fabric.json"), JSON.stringify({ executor: { landlock: { mode: "enforce", disabled: false } } }));
    expect(loadFabricConfig({ cwd, agentDir, projectTrusted: true }).executor.landlock).toEqual({ mode: "enforce", disabled: false });
  });
});

describe.skipIf(process.platform !== "linux")("real kernel through Pi tool_call -> cooperative Fabric bash -> Landlock", () => {
  beforeAll(() => {
    const probe = spawnSync(helper, ["--abi"], { encoding: "utf8" });
    expect(probe.status, probe.error?.message || probe.stderr).toBe(0);
    expect(Number(probe.stdout)).toBeGreaterThanOrEqual(4);
  });

  it.each([
    ["rm", (dir: string) => `rm -rf -- ${quote(dir)}`],
    ["absolute rm", (dir: string) => `/bin/rm -rf -- ${quote(dir)}`],
    ["variable/encoded rm", (dir: string) => `c=$(printf '\\162\\155'); "$c" -rf -- ${quote(dir)}`],
    ["find -delete", (dir: string) => `find ${quote(dir)} -depth -delete`],
    ["python shutil", (dir: string) => `python3 -c ${quote(`import shutil; shutil.rmtree(${JSON.stringify(dir)})`)}`],
    ["perl unlink", (dir: string) => `perl -e ${quote('unlink($ARGV[0]) or die "errno=".(0+$!)." $!\\n";')} -- ${quote(path.join(dir, "victim"))}`],
    ["tee redirection", (dir: string) => `printf bad | tee > ${quote(path.join(dir, "victim"))}`],
  ])("denies outside writes/deletion with EACCES: %s", async (_name, command) => {
    const h = harness();
    const result = await h.invoke({ command: (command as (dir: string) => string)(h.sibling), settle: true });
    expect(result.ok).toBe(false);
    expect(result.output).toMatch(/Permission denied|Errno 13|errno=13/);
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
    expect(h.runner.emitToolCall).toHaveBeenCalledOnce();
    expect(h.runner.emitToolResult).toHaveBeenCalledOnce();
    expect(h.fallback).not.toHaveBeenCalled();
    expect(h.audit()[0].event).toBe("enforce");
  });

  it("allows lane/private TMPDIR writes, reads anywhere, filters/env/prefix and job pid", async () => {
    vi.stubEnv("FABRIC_TEST_SECRET", "filter-me");
    const h = harness();
    const result = await h.invoke({ command: `printf lane > lane-file; printf temp > "$TMPDIR/temp-file"; cat ${quote(path.join(h.sibling, "victim"))}; printf '|%s|%s|%s|filter-me' "$LANDLOCK_PREFIX" "$LANDLOCK_SPAWN" "\${FABRIC_TEST_SECRET-unset}"` });
    expect(result.output).toBe("keep-me|preserved|preserved|unset|[filtered]");
    expect(fs.readFileSync(path.join(h.cwd, "lane-file"), "utf8")).toBe("lane");
    expect(fs.readFileSync(path.join(h.tmpdir, "temp-file"), "utf8")).toBe("temp");
    const writes = h.audit()[0].writes as string[];
    expect(writes).toContain(h.cwd); expect(writes).toContain(h.tmpdir);
    expect(writes).not.toContain(h.root); expect(writes).not.toContain("/tmp");
    expect(h.provider.shellJobs.list()[0]?.status).toBe("exited");
    expect(process.env.FABRIC_TEST_SECRET).toBe("filter-me");
  });

  it("does not widen to an out-of-lane per-call cwd or symlink target", async () => {
    const h = harness();
    fs.symlinkSync(h.sibling, path.join(h.cwd, "outside"));
    for (const args of [{ command: "printf bad > victim", cwd: h.sibling }, { command: "printf bad > outside/victim" }]) {
      const result = await h.invoke({ ...args, settle: true });
      expect(result.ok).toBe(false); expect(result.output).toContain("Permission denied");
    }
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
  });

  it("denies truncate, create and rename with EACCES; hard-link access gain with EXDEV", async () => {
    const h = harness();
    const script = `import os, errno
p=${JSON.stringify(h.sibling)}
for action, expected in [(lambda: os.truncate(p+'/victim', 0), errno.EACCES), (lambda: os.mkdir(p+'/new'), errno.EACCES), (lambda: os.rename(p+'/victim', 'moved'), errno.EACCES), (lambda: os.link(p+'/victim', 'linked'), errno.EXDEV)]:
 try: action()
 except OSError as e:
  assert e.errno == expected, e
  print(e.errno)
 else: raise Exception('unexpected success')`;
    expect((await h.invoke({ command: `python3 -c ${quote(script)}` })).output).toBe("13\n13\n13\n18\n");
    expect((await h.invoke({ command: 'mkdir a b; echo yes > a/item; mv a/item b/item; rm -rf a; cat b/item' })).output).toBe("yes\n");
  });

  it("confines BASH_ENV startup before any shell parsing", async () => {
    const h = harness();
    const startup = path.join(h.cwd, "startup.sh");
    fs.writeFileSync(startup, `printf bypass > ${quote(path.join(h.sibling, "victim"))}\n`);
    vi.stubEnv("BASH_ENV", startup);
    const result = await h.invoke({ command: "printf normal" });
    expect(result.output).toContain("Permission denied"); expect(result.output).toContain("normal");
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
  });

  it.each([{ mode: "off" as const, disabled: false }, { mode: "enforce" as const, disabled: true }])("leaves disabled behavior unchanged: %j", async settings => {
    const h = harness(settings);
    expect((await h.invoke({ command: `rm -rf -- ${quote(h.sibling)}` })).ok).toBe(true);
    expect(fs.existsSync(h.sibling)).toBe(false);
    expect(fs.existsSync(path.join(h.cwd, ".pi/landlock-audit.jsonl"))).toBe(false);
  });

  it("keeps an ungranted leading escape confined and warns once", async () => {
    const h = harness();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const command = `PI_FABRIC_LANDLOCK_ESCAPE=1 printf changed > ${quote(path.join(h.sibling, "victim"))}`;
    for (let i = 0; i < 2; i++) {
      const result = await h.invoke({ command, settle: true });
      expect(result.ok).toBe(false);
      expect(result.output).toContain("Permission denied");
    }
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
    expect(h.audit().map(row => row.event)).toEqual(["enforce", "enforce"]);
    expect(warn.mock.calls.filter(call => String(call[0]).includes("allowEscape"))).toHaveLength(1);
    // The reserved assignment is removed even when its authority is denied.
    expect((await h.invoke({ command: "PI_FABRIC_LANDLOCK_ESCAPE=1 printf %s \"${PI_FABRIC_LANDLOCK_ESCAPE-unset}\"" })).output).toBe("unset");
  });

  it("logs every root-granted per-command escape, then confines the next command; live kill switch is honored", async () => {
    const h = harness({ mode: "enforce", disabled: false, allowEscape: true });
    const command = `PI_FABRIC_LANDLOCK_ESCAPE=1 printf changed > ${quote(path.join(h.sibling, "victim"))}`;
    for (let i = 0; i < 2; i++) expect((await h.invoke({ command })).output).toContain("Landlock escape: unconfined");
    expect(h.audit().filter(row => row.event === "escape")).toHaveLength(2);
    expect(JSON.stringify(h.audit())).not.toContain(command);
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("changed");
    expect((await h.invoke({ command: `printf denied > ${quote(path.join(h.sibling, "victim"))}`, settle: true })).ok).toBe(false);
    h.settings.disabled = true;
    expect((await h.invoke({ command: `printf disabled > ${quote(path.join(h.sibling, "victim"))}` })).ok).toBe(true);
  });

  it.each([false, true])("consumes spawnHook/inherited env after middleware; grant=%s", async allowEscape => {
    for (const request of ["spawnHook", "inherited"] as const) {
      vi.stubEnv("PI_FABRIC_LANDLOCK_ESCAPE", request === "inherited" ? "1" : undefined);
      const h = harness({ mode: "enforce", disabled: false, allowEscape },
        request === "spawnHook" ? { escapeEnv: "1" } : {});
      const command = `printf '%s:%s:%s\\n' "$LANDLOCK_PREFIX" "$LANDLOCK_SPAWN" "\${PI_FABRIC_LANDLOCK_ESCAPE-unset}"; printf changed > ${quote(path.join(h.sibling, "victim"))}`;
      const result = await h.invoke({ command, settle: true });
      expect(result.ok).toBe(allowEscape);
      expect(result.output).toContain("preserved:preserved:unset");
      expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe(allowEscape ? "changed" : "keep-me");
      expect(h.audit().at(-1).event).toBe(allowEscape ? "escape" : "enforce");
    }
  });

  it("scrubs non-request escape env values even with a grant", async () => {
    const h = harness({ mode: "enforce", disabled: false, allowEscape: true }, { escapeEnv: "0" });
    expect((await h.invoke({ command: "printf %s \"${PI_FABRIC_LANDLOCK_ESCAPE-unset}\"" })).output).toBe("unset");
    expect(h.audit()[0].event).toBe("enforce");
  });

  it("revokes a grant while middleware prepares before entering the common launch boundary", async () => {
    let entered!: () => void; let release!: () => void;
    const reached = new Promise<void>(done => { entered = done; });
    const open = new Promise<void>(done => { release = done; });
    const h = harness({ mode: "enforce", disabled: false, allowEscape: true }, { gate: { entered, open } });
    const pending = h.invoke({ command: `PI_FABRIC_LANDLOCK_ESCAPE=1 printf changed > ${quote(path.join(h.sibling, "victim"))}`, settle: true });
    await reached;
    h.settings.allowEscape = false;
    release();
    expect((await pending).ok).toBe(false);
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
    expect(h.audit()[0].event).toBe("enforce");
  });

  it("does not treat a shell-body environment assignment as a kernel escape", async () => {
    const h = harness();
    expect((await h.invoke({ command: `export PI_FABRIC_LANDLOCK_ESCAPE=1; printf bad > ${quote(path.join(h.sibling, "victim"))}`, settle: true })).ok).toBe(false);
  });

  it.each([{ opaque: true }, { managed: true }])("fails closed for a backend it cannot confine: %j", async opt => {
    const h = harness(undefined, opt);
    await expect(h.invoke({ command: "printf unreachable" })).rejects.toThrow("unconfined opaque/managed override");
    expect(h.fallback).not.toHaveBeenCalled();
  });

  it("preserves early policy refusals before starting confinement", async () => {
    const h = harness(undefined, { blocked: true });
    await expect(h.invoke({ command: "printf blocked" })).rejects.toThrow("early warning");
    expect(fs.existsSync(path.join(h.cwd, ".pi/landlock-audit.jsonl"))).toBe(false);
  });

  it("never escapes if mandatory logging fails", async () => {
    const h = harness({ mode: "enforce", disabled: false, allowEscape: true });
    fs.mkdirSync(path.join(h.cwd, ".pi"));
    fs.symlinkSync(path.join(h.sibling, "victim"), path.join(h.cwd, ".pi/landlock-audit.jsonl"));
    const result = await h.invoke({ command: `PI_FABRIC_LANDLOCK_ESCAPE=1 printf bad > ${quote(path.join(h.sibling, "victim"))}`, settle: true });
    expect(result.ok).toBe(false);
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
  });

  it("grants only its own Fabric worker run, not the fleet run parent", async () => {
    const h = harness();
    const own = path.join(h.root, "worker-runs/own");
    const other = path.join(h.root, "worker-runs/other");
    fs.mkdirSync(own, { recursive: true }); fs.mkdirSync(other);
    vi.stubEnv("PI_FABRIC_AGENT_RUN_DIR", own);
    expect((await h.invoke({ command: `printf ok > ${quote(path.join(own, "output"))}` })).ok).toBe(true);
    expect((await h.invoke({ command: `printf no > ${quote(path.join(other, "output"))}`, settle: true })).ok).toBe(false);
    expect(h.audit()[0].writes).toContain(own);
    expect(h.audit()[0].writes).not.toContain(path.dirname(own));
  });

  it("grants the lane's git common directory for linked-worktree administration", async () => {
    const h = harness();
    const common = path.join(h.root, "git-admin");
    const worktree = path.join(common, "worktrees/lane");
    fs.mkdirSync(worktree, { recursive: true });
    fs.writeFileSync(path.join(h.cwd, ".git"), `gitdir: ${worktree}\n`);
    fs.writeFileSync(path.join(worktree, "commondir"), "../..\n");
    expect((await h.invoke({ command: `printf obj > ${quote(path.join(common, "object"))}` })).ok).toBe(true);
    expect(h.audit()[0].writes).toContain(common);
    expect(h.audit()[0].writes).not.toContain(h.root);
  });

  it("uses a generated private TMPDIR instead of shared /tmp", async () => {
    const h = harness();
    vi.stubEnv("TMPDIR", "/tmp");
    vi.spyOn(os, "tmpdir").mockReturnValue(h.root);
    const result = await h.invoke({ command: 'printf %s "$TMPDIR"; printf ok > "$TMPDIR/new"' });
    expect(result.output.startsWith(path.join(h.root, "pi-fabric-landlock-"))).toBe(true);
    expect(h.audit()[0].writes).not.toContain("/tmp");
    // Registry close removes only this generated private temp directory.
    await h.registry.close();
    expect(fs.existsSync(result.output)).toBe(false);
  });

  it("preserves hard timeouts and cancellation and confines background children", async () => {
    const h = harness();
    await expect(h.invoke({ command: "sleep 8", timeout: 0.05 })).rejects.toThrow("timed out");
    const controller = new AbortController();
    const run = h.invoke({ command: "sleep 8" }, controller.signal);
    const rejected = expect(run).rejects.toThrow();
    await vi.waitFor(() => expect(h.provider.shellJobs.live().length).toBe(1));
    controller.abort(); await rejected;
    const result = await h.invoke({ command: `sleep 0.1; printf bad > ${quote(path.join(h.sibling, "victim"))}`, background: true });
    expect(result.details?.running).toBe(true);
    await vi.waitFor(() => expect(h.provider.shellJobs.live()).toHaveLength(0), { timeout: 3000 });
    expect(fs.readFileSync(result.details!.logPath!, "utf8")).toContain("Permission denied");
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
  });

  it("S1: a replaced in-lane .git cannot redirect the next call's write grant", async () => {
    const h = harness();
    fs.mkdirSync(path.join(h.cwd, ".git"));
    const first = await h.invoke({ command: `mv .git .git-old && ln -s ${quote(h.sibling)} .git && printf staged` });
    expect(first.output).toBe("staged");
    const second = await h.invoke({ command: "printf redirected > .git/victim", settle: true });
    expect(second.ok).toBe(false);
    expect(second.output).toMatch(/changed identity|Permission denied/);
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
    // A real directory substituted at the same name is refused too.
    fs.rmSync(path.join(h.cwd, ".git")); fs.mkdirSync(path.join(h.cwd, ".git"));
    expect((await h.invoke({ command: "printf x", settle: true })).output).toContain("changed identity");
  });

  it("S1: a replaced in-lane private TMPDIR cannot redirect the next call's write grant", async () => {
    const h = harness();
    const tmp = path.join(h.cwd, "tmp");
    fs.mkdirSync(tmp, { mode: 0o700 });
    vi.stubEnv("TMPDIR", tmp);
    expect((await h.invoke({ command: `mv "$TMPDIR" "$TMPDIR.old" && ln -s ${quote(h.sibling)} "$TMPDIR" && printf staged` })).output).toBe("staged");
    const second = await h.invoke({ command: 'printf redirected > "$TMPDIR/victim"', settle: true });
    expect(second.ok).toBe(false);
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
  });

  it("S1: the native helper binds each rule to the approved inode, never a re-resolved name", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-ident-"));
    roots.push(root);
    const good = path.join(root, "good"); const other = path.join(root, "other");
    fs.mkdirSync(good); fs.mkdirSync(other);
    const id = (p: string) => { const s = fs.statSync(p, { bigint: true }); return `${s.dev}:${s.ino}`; };
    const run = (writes: string) => spawnSync(helper, ["-c", `printf ok > ${quote(path.join(good, "f"))}`], {
      encoding: "utf8", env: { PATH: process.env.PATH, PI_FABRIC_LANDLOCK_SHELL: "/bin/sh", PI_FABRIC_LANDLOCK_WRITES: writes } });
    expect(run(`${id(good)}:${good}`).status).toBe(0);
    const swapped = run(`${id(other)}:${good}`);
    expect(swapped.status).toBe(125); expect(swapped.stderr).toContain("grant identity changed");
    const link = path.join(root, "link"); fs.symlinkSync(good, link);
    expect(run(`${id(good)}:${link}`).status).toBe(125);
    expect(run(good).status).toBe(125); // legacy name-only grants are malformed
  });

  it.each(["resolve", "reject"] as const)("S2: generated TMPDIR is retained without confirmed process-group custody (%s)", async outcome => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-s2-"));
    roots.push(root);
    vi.stubEnv("TMPDIR", "/tmp");
    vi.spyOn(os, "tmpdir").mockReturnValue(root);
    const confinement = new LandlockBashConfinement(root);
    let settle!: (value: { exitCode: number | null }) => void; let fail!: (error: Error) => void;
    const pending = new Promise<{ exitCode: number | null }>((done, reject) => { settle = done; fail = reject; });
    const ops = { exec: () => pending };
    const runDir = fs.mkdtempSync(path.join(root, "run-"));
    const execution = confinement.operations(ops, ops, "/bin/sh", runDir, () => ({ mode: "enforce", disabled: false }), "delayed launch")
      .exec("delayed launch", root, { onData: () => {} });
    confinement.close(); // shell store closed / job aborted; launch still unresolved
    expect(confinement.pendingOperations).toBe(1);
    expect(fs.existsSync(confinement.tmpdir)).toBe(true);
    // Foreign operations never report a process group: a resolved exit is not quiescence.
    if (outcome === "resolve") { settle({ exitCode: 0 }); await execution; }
    else { fail(new Error("abort acknowledged, exit unknown")); await expect(execution).rejects.toThrow(); }
    expect(confinement.pendingOperations).toBe(0);
    expect(fs.existsSync(confinement.tmpdir)).toBe(true);
    expect(JSON.parse(fs.readFileSync(`${confinement.tmpdir}.custody`, "utf8")).unconfirmed).toBe(true);
  });

  it("S2 r4: the next session's bounded sweep removes only confirmed-quiescent retained temps", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-sweep-"));
    roots.push(root);
    vi.stubEnv("TMPDIR", "/tmp");
    vi.spyOn(os, "tmpdir").mockReturnValue(root);
    const retained = (name: string, record: Record<string, unknown>) => {
      const dir = path.join(root, `pi-fabric-landlock-${name}`);
      fs.mkdirSync(dir, { mode: 0o700 }); fs.writeFileSync(path.join(dir, "data"), "x");
      const stat = fs.statSync(dir, { bigint: true });
      // An ended owner: no live process has this start time.
      fs.writeFileSync(`${dir}.custody`, JSON.stringify({ host: process.pid, hostStart: -1, since: 0,
        dev: String(stat.dev), ino: String(stat.ino), groups: [], unconfirmed: false, ...record }));
      return dir;
    };
    const live = spawnSync("sh", ["-c", "sleep 30 >/dev/null 2>&1 & echo $!"], { encoding: "utf8" });
    const livePid = Number(live.stdout.trim());
    // This synthetic old-ledger test owns just the fixture process inventory.
    // Unrelated same-uid no_new_privs jobs on a busy shared host intentionally
    // veto real cleanup; they must not turn this deterministic sweep probe flaky.
    const readdir = fs.readdirSync;
    vi.spyOn(fs, "readdirSync").mockImplementation(((directory: fs.PathLike, ...args: unknown[]) =>
      String(directory) === "/proc" ? [String(process.pid), String(livePid)]
        : (readdir as (...input: unknown[]) => unknown)(directory, ...args)) as typeof fs.readdirSync);
    try {
      const done = retained("done00", {});
      const unknown = retained("unk000", { unconfirmed: true });
      const owned = retained("own000", { hostStart: undefined }); // owner identity unknown
      const busy = retained("busy00", { groups: [Number(fs.readFileSync(`/proc/${livePid}/stat`, "utf8").split(") ")[1]!.split(" ")[2])] });
      new LandlockBashConfinement(root).close();
      expect(fs.existsSync(done)).toBe(false); expect(fs.existsSync(`${done}.custody`)).toBe(false);
      for (const kept of [unknown, owned, busy]) expect(fs.readFileSync(path.join(kept, "data"), "utf8")).toBe("x");
    } finally { process.kill(livePid, "SIGKILL"); }
  });

  it("S1 r2: a grant absent at first enforced use is never admitted later (dangling cache alias)", async () => {
    const h = harness();
    const home = path.join(h.root, "home");
    const cache = path.join(home, ".cache");
    fs.mkdirSync(cache, { recursive: true, mode: 0o700 });
    // Host-configured ~/.npm alias to a not-yet-created directory beneath the allowed cache.
    fs.symlinkSync(path.join(cache, "npm"), path.join(home, ".npm"));
    vi.stubEnv("HOME", home);
    const first = await h.invoke({ command: `ln -s ${quote(h.sibling)} ${quote(path.join(cache, "npm"))} && printf staged` });
    expect(first.output).toBe("staged");
    const second = await h.invoke({ command: `printf redirected > ${quote(path.join(home, ".npm", "victim"))}`, settle: true });
    expect(second.ok).toBe(false);
    expect(fs.readFileSync(path.join(h.sibling, "victim"), "utf8")).toBe("keep-me");
    const writes = h.audit().filter(entry => entry.event === "enforce").map(entry => entry.writes as string[]);
    expect(writes).toHaveLength(2);
    for (const grant of writes.flat()) expect(grant.startsWith(fs.realpathSync(h.sibling))).toBe(false);
  });

  it("S2 r2: temp custody covers a background launch whose middleware is still preparing at close", async () => {
    let entered!: () => void; let open!: () => void;
    const reached = new Promise<void>(resolve => { entered = resolve; });
    const gate = { entered: () => entered(), open: new Promise<void>(resolve => { open = resolve; }) };
    const h = harness(undefined, { gate });
    vi.stubEnv("TMPDIR", "/tmp"); // not private: Fabric generates its own temp
    vi.spyOn(os, "tmpdir").mockReturnValue(h.root);
    const launched = await h.invoke({ command: "printf late", background: true });
    expect(launched.details?.running).toBe(true);
    await reached; // middleware is awaiting preparation; inner confined exec not entered
    const [generated] = fs.readdirSync(h.root).filter(name => name.startsWith("pi-fabric-landlock-"));
    expect(generated).toBeDefined();
    const temp = path.join(h.root, generated!);
    fs.writeFileSync(path.join(temp, "sentinel"), "keep");
    await h.registry.close(); // session/provider close inside the delayed-launch window
    expect(fs.readFileSync(path.join(temp, "sentinel"), "utf8")).toBe("keep");
    open(); // the late delegation is fenced after close; settlement releases custody
    await vi.waitFor(() => expect(fs.existsSync(temp)).toBe(false), { timeout: 3000 });
  });

  it("S2 r3: a background descendant that outlives its shell keeps the generated TMPDIR until it exits", async () => {
    const h = harness();
    vi.stubEnv("TMPDIR", "/tmp"); // not private: Fabric generates its own temp
    vi.spyOn(os, "tmpdir").mockReturnValue(h.root);
    // Real local operations: the shell resolves while its setsid-less delayed child lives on.
    const started = performance.now();
    const result = await h.invoke({ command: 'printf input > "$TMPDIR/input"; (sleep 1.5; cat "$TMPDIR/input" > "$TMPDIR/result"; sleep 1) </dev/null >/dev/null 2>&1 & printf "%s" "$TMPDIR"' });
    expect(performance.now() - started).toBeLessThan(1400);
    const temp = result.output.trim();
    expect(path.dirname(temp)).toBe(h.root);
    await h.registry.close(); // provider/session close while the descendant is alive
    expect(fs.readFileSync(path.join(temp, "input"), "utf8")).toBe("input");
    await vi.waitFor(() => expect(fs.readFileSync(path.join(temp, "result"), "utf8")).toBe("input"), { timeout: 5000 });
    expect(fs.existsSync(temp)).toBe(true); // descendant still sleeping
    await vi.waitFor(() => expect(fs.existsSync(temp)).toBe(false), { timeout: 8000 });
  }, 15_000);

  it.each(["in-group", "setsid"] as const)("S2 r4: a sanitized-env worker holding only an absolute temp pathname keeps the generated TMPDIR (%s)", async variant => {
    const h = harness();
    vi.stubEnv("TMPDIR", "/tmp"); // not private: Fabric generates its own temp
    vi.spyOn(os, "tmpdir").mockReturnValue(h.root);
    // Review trigger: no TMPDIR in env, cwd outside, no fd under the temp; the input is only
    // named in argv and opened after the shell returned and the provider closed.
    const launch = variant === "setsid" ? "setsid -f env -i" : "env -i";
    const result = await h.invoke({ command: `d="$TMPDIR"; printf input > "$d/input"; cd /; ${launch} /bin/sh -c 'sleep 1.5; cat "$1" > "$2"; sleep 1' worker "$d/input" "$d/result" </dev/null >/dev/null 2>&1 & printf "%s" "$d"` });
    const temp = result.output.trim();
    expect(path.dirname(temp)).toBe(h.root);
    await h.registry.close();
    expect(fs.readFileSync(path.join(temp, "input"), "utf8")).toBe("input");
    await vi.waitFor(() => expect(fs.readFileSync(path.join(temp, "result"), "utf8")).toBe("input"), { timeout: 5000 });
    expect(fs.existsSync(temp)).toBe(true); // worker still sleeping
    await vi.waitFor(() => expect(fs.existsSync(temp)).toBe(false), { timeout: 8000 });
  }, 15_000);

  it("S2 r4: unreadable process state is unresolved, never absence", async () => {
    const h = harness();
    vi.stubEnv("TMPDIR", "/tmp");
    vi.spyOn(os, "tmpdir").mockReturnValue(h.root);
    const temp = (await h.invoke({ command: 'printf "%s" "$TMPDIR"' })).output.trim();
    const read = fs.readFileSync;
    const spy = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (typeof file === "string" && /^\/proc\/\d+\/status$/.test(file)) {
        throw Object.assign(new Error("EACCES"), { code: "EACCES" });
      }
      return (read as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.readFileSync);
    await h.registry.close();
    expect(fs.existsSync(temp)).toBe(true); // incomplete visibility: retained
    spy.mockRestore();
    await vi.waitFor(() => expect(fs.existsSync(temp)).toBe(false), { timeout: 5000 });
  });

  it.each(["user cancel", "store close"] as const)("S3/F3: cancellation during awaited cwd preparation never starts the command (%s)", async variant => {
    const h = harness();
    const seeded = path.join(h.cwd, "seeded");
    const access = fs.promises.access;
    let entered!: () => void; let open!: () => void;
    const reached = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { open = resolve; });
    vi.spyOn(fs.promises, "access").mockImplementation(async (file, mode) => {
      if (file === h.cwd) { entered(); await gate; }
      return access(file, mode);
    });
    const controller = new AbortController();
    const run = h.invoke({ command: `printf ran > ${quote(seeded)}; echo $$ > ${quote(seeded)}.pid; sleep 5`,
      ...(variant === "store close" ? { background: true } : {}) }, controller.signal);
    const settled = run.then(() => undefined, () => undefined);
    await reached; // cwd preparation is pending
    if (variant === "user cancel") controller.abort();
    else await h.provider.shellJobs.close();
    open();
    await settled;
    await new Promise(resolve => setTimeout(resolve, 400));
    expect(fs.existsSync(seeded)).toBe(false);
    expect(fs.existsSync(`${seeded}.pid`)).toBe(false);
  });

  it.each(["deferred", "direct"] as const)("S4/F4: failed temp cleanup is contained and retained; Pi and a new session stay alive (%s)", async variant => {
    const h = harness();
    vi.stubEnv("TMPDIR", "/tmp"); // not private: Fabric generates its own temp
    vi.spyOn(os, "tmpdir").mockReturnValue(h.root);
    const crashes: unknown[] = [];
    const crash = (error: unknown): void => { crashes.push(error); };
    process.on("uncaughtException", crash);
    let temp = "";
    try {
      const child = variant === "deferred" ? '(sleep 1) </dev/null >/dev/null 2>&1 & ' : "";
      temp = (await h.invoke({ command: `mkdir "$TMPDIR/locked"; printf x > "$TMPDIR/locked/input"; chmod 500 "$TMPDIR/locked"; ${child}printf "%s" "$TMPDIR"` })).output.trim();
      expect(path.dirname(temp)).toBe(h.root);
      await expect(h.registry.close()).resolves.toBeUndefined(); // old session/provider close
      const next = harness(); // the replacement session
      if (variant === "deferred") await new Promise(resolve => setTimeout(resolve, 3000)); // child exits; recheck runs
      expect((await next.invoke({ command: "printf alive" })).output).toContain("alive");
      expect(crashes).toEqual([]);
      expect(fs.existsSync(path.join(temp, "locked/input"))).toBe(true); // safely retained
      expect(fs.existsSync(`${temp}.custody`)).toBe(true); // retention evidence kept
    } finally {
      process.off("uncaughtException", crash);
      if (temp) try { fs.chmodSync(path.join(temp, "locked"), 0o700); } catch { /* gone */ }
    }
  }, 10_000);

  it("S5: a custody-ledger failure after spawn kills and reaps the started command", async () => {
    const h = harness();
    vi.stubEnv("TMPDIR", "/tmp"); // generated temp: the host owns a custody ledger
    vi.spyOn(os, "tmpdir").mockReturnValue(h.root);
    await h.invoke({ command: "true" }); // confinement constructed, ledger created
    const seeded = path.join(h.cwd, "seeded");
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (typeof file === "string" && file.endsWith(".custody")) throw Object.assign(new Error("ENOSPC: no space left on device"), { code: "ENOSPC" });
      return (open as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    await expect(h.invoke({ command: `echo $$ > ${quote(seeded)}.pid; sleep 0.5; printf late > ${quote(seeded)}` })).rejects.toThrow("ENOSPC");
    await new Promise(resolve => setTimeout(resolve, 900));
    expect(fs.existsSync(seeded)).toBe(false); // no delayed write
    if (fs.existsSync(`${seeded}.pid`)) {
      const pid = Number(fs.readFileSync(`${seeded}.pid`, "utf8"));
      expect(() => process.kill(pid, 0)).toThrow(); // no surviving process
    }
    await expect(h.registry.close()).resolves.toBeUndefined();
  });

  it("F2: a missing root policy keeps already-active lanes confined; agent edits cannot override", async () => {
    const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-host-"));
    roots.push(agentDir);
    const hostFile = path.join(agentDir, "fabric.json");
    fs.writeFileSync(hostFile, JSON.stringify({ executor: { landlock: { mode: "enforce" } } }));
    const lanes = [0, 1].map(() => {
      // Each provider reads through the production composition on every call.
      const state: { loaded: LandlockSettings } = { loaded: { mode: "off", disabled: false } };
      const live = { get mode() { return liveLandlockSettings(state.loaded, agentDir).mode; },
        get disabled() { return liveLandlockSettings(state.loaded, agentDir).disabled; } } as LandlockSettings;
      return { ...harness(live), state };
    });
    for (const lane of lanes) {
      fs.mkdirSync(path.join(lane.cwd, ".pi"), { recursive: true });
      fs.writeFileSync(path.join(lane.cwd, ".pi/fabric.json"), JSON.stringify({ executor: { landlock: { mode: "enforce", disabled: false } } }));
      // Loaded once at session start, as an already-active runtime would hold it.
      const loaded = loadFabricConfig({ cwd: lane.cwd, agentDir, projectTrusted: true }).executor.landlock;
      expect(loaded).toEqual({ mode: "enforce", disabled: false });
      lane.state.loaded = loaded;
      const denied = await lane.invoke({ command: `printf confined > ${quote(path.join(lane.sibling, "victim"))}`, settle: true });
      expect(denied.ok).toBe(false);
    }
    fs.writeFileSync(hostFile, JSON.stringify({ executor: { landlock: { mode: "enforce", disabled: true } } }));
    for (const lane of lanes) {
      expect((await lane.invoke({ command: `printf still-confined > ${quote(path.join(lane.sibling, "victim"))}`, settle: true })).ok).toBe(false);
      expect(fs.readFileSync(path.join(lane.sibling, "victim"), "utf8")).toBe("keep-me");
    }
  });

  it.runIf(process.env.LANDLOCK_BENCH === "1")("measures interleaved real per-call overhead", async () => {
    const h = harness();
    const samples: Record<string, number[]> = { off: [], enforce: [] };
    for (let round = 0; round < 85; round++) {
      for (const mode of round % 2 ? ["enforce", "off"] as const : ["off", "enforce"] as const) {
        h.settings.mode = mode;
        const start = performance.now(); await h.invoke({ command: ":" });
        if (round >= 5) samples[mode]!.push(performance.now() - start);
      }
    }
    const stats = Object.fromEntries(Object.entries(samples).map(([mode, values]) => {
      const sorted = [...values].sort((a,b) => a-b);
      return [mode, { n: values.length, meanMs: values.reduce((a,b) => a+b,0)/values.length,
        medianMs: sorted[Math.floor(sorted.length/2)], p95Ms: sorted[Math.floor(sorted.length*0.95)], minMs: sorted[0], maxMs: sorted.at(-1) }];
    }));
    console.log("LANDLOCK_REAL_PATH_OVERHEAD", JSON.stringify(stats));
    if (process.env.TASK_OUT) fs.writeFileSync(path.join(process.env.TASK_OUT, "overhead-real-path.json"), JSON.stringify({ stats, samples }, null, 2)+"\n");
  }, 120_000);
});

describe.skipIf(process.platform === "linux")("non-Linux", () => {
  it("leaves bash unchanged even when Linux enforcement is selected", async () => {
    const h = harness();
    expect((await h.invoke({ command: "printf portable" })).output).toBe("portable");
    expect(fs.existsSync(path.join(h.cwd, ".pi/landlock-audit.jsonl"))).toBe(false);
  });
});
