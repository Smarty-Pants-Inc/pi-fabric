import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { HOST_POLICY_PATH } from "../src/host-policy.js";

type Metadata = { uid?: number; mode?: number; dev?: number; ino?: number };
const policyDirectory = path.dirname(HOST_POLICY_PATH);
let agentDir: string;
let cwd: string;
let config: typeof import("../src/config.js");
let policy: typeof import("../src/host-policy.js");
let warn: MockInstance<typeof console.warn>;
const metadata = new Map<string, Metadata>();
let openedMetadata: Metadata;
let openedDescriptor: number | undefined;

const changeStat = (stat: fs.Stats, changes: Metadata): fs.Stats =>
  Object.assign(Object.create(Object.getPrototypeOf(stat)) as fs.Stats, stat, changes);

// Unprivileged tests use REAL temp files, opens, reads and dev/ino identities;
// only ownership/ancestor modes are simulated. No chown or /etc writes.
const simulateRootOwnership = (): void => {
  const ancestors = new Set<string>();
  for (let directory = policyDirectory;;) {
    ancestors.add(directory);
    const parent = path.dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }
  const lstat = fs.lstatSync;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    const stat = (lstat as (...args: unknown[]) => fs.Stats)(file, ...args);
    const name = String(file);
    if (name === HOST_POLICY_PATH || ancestors.has(name)) {
      return changeStat(stat, { uid: 0, ...(ancestors.has(name) ? { mode: stat.mode & ~0o022 } : {}), ...metadata.get(name) });
    }
    return stat;
  }) as typeof fs.lstatSync);
  const open = fs.openSync;
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    const fd = (open as (...args: unknown[]) => number)(file, ...args);
    if (String(file) === HOST_POLICY_PATH) openedDescriptor = fd;
    return fd;
  }) as typeof fs.openSync);
  const fstat = fs.fstatSync;
  vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...args: unknown[]) => {
    const stat = (fstat as (...args: unknown[]) => fs.Stats)(fd, ...args);
    return fd === openedDescriptor ? changeStat(stat, { uid: 0, ...openedMetadata }) : stat;
  }) as typeof fs.fstatSync);
};
const writePolicy = (document: unknown): void => {
  fs.writeFileSync(HOST_POLICY_PATH, JSON.stringify(document), { mode: 0o600 });
};
const writeAgent = (document: unknown): void => {
  fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ configVersion: 4, ...document as object }));
};
const load = () => config.loadFabricConfig({ cwd, agentDir, projectTrusted: true });

// POSIX uid/mode/O_NOFOLLOW policy; Windows skips root metadata checks.
describe.runIf(process.platform !== "win32")("root-owned host policy (#7591)", () => {
  beforeEach(async () => {
    // Hard guard: test build constant must point into its private temp directory.
    if (!HOST_POLICY_PATH.startsWith(`${os.tmpdir()}${path.sep}`)) throw new Error("test policy path is not private");
    fs.mkdirSync(policyDirectory, { recursive: true, mode: 0o700 });
    agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "policy-agent-"));
    cwd = fs.mkdtempSync(path.join(os.tmpdir(), "policy-project-"));
    fs.mkdirSync(path.join(cwd, ".pi"));
    metadata.clear(); openedMetadata = {}; openedDescriptor = undefined;
    vi.resetModules();
    warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    policy = await import("../src/host-policy.js");
    config = await import("../src/config.js");
  });
  afterEach(() => {
    vi.restoreAllMocks(); vi.unstubAllEnvs();
    for (const directory of [policyDirectory, agentDir, cwd]) fs.rmSync(directory, { recursive: true, force: true });
  });

  it("keeps the default baseline when missing and ignores agent relaxations", () => {
    writeAgent({ executor: { landlock: { mode: "off", disabled: true } }, agents: {
      processSlice: "legacy.slice", deniedModels: [], modelPolicy: { requireReason: [] },
    } });
    for (let i = 0; i < 3; i++) {
      const loaded = load();
      expect(loaded.executor.landlock).toEqual({ mode: "off", disabled: false });
      expect(loaded.agents.processSlice).toBeUndefined();
      expect(loaded.agents.deniedModels).toEqual([]);
      expect(loaded.agents.modelPolicy.requireReason).toEqual(["gpt-6-astra"]);
      expect(config.readHostLandlockDisabled(agentDir)).toBe(false);
      expect(config.liveLandlockSettings({ mode: "off", disabled: true }, agentDir)).toEqual({ mode: "off", disabled: false });
    }
    expect(warn.mock.calls.filter(call => String(call[0]).includes("is missing"))).toHaveLength(1);
    expect(String(warn.mock.calls.find(call => String(call[0]).includes("is missing"))?.[0])).toContain(HOST_POLICY_PATH);
  });

  describe.each(["agent", "project"] as const)("%s Landlock tightening", scope => {
    it.each(([undefined, "off", "enforce"] as const).flatMap(rootMode =>
      ["enforce", "off", "permissive", "warn", "audit", "unknown", null].map(mode => ({ rootMode, mode })),
    ))("root=$rootMode, writable mode=$mode", ({ rootMode, mode }) => {
      if (rootMode !== undefined) {
        simulateRootOwnership(); writePolicy({ executor: { landlock: { mode: rootMode } } });
      }
      const document = { executor: { landlock: { mode, disabled: true } } };
      if (scope === "agent") writeAgent(document);
      else fs.writeFileSync(path.join(cwd, ".pi/fabric.json"), JSON.stringify(document));
      const expected = { mode: mode === "enforce" ? "enforce" : rootMode ?? "off", disabled: false };
      for (const loaded of [load(), config.loadFabricConfigForScope({ cwd, agentDir, projectTrusted: true }, "project")]) {
        expect(loaded.executor.landlock).toEqual(expected);
        expect(config.liveLandlockSettings(loaded.executor.landlock, agentDir)).toEqual(expected);
      }
    });
  });

  it("preserves the built-in baseline with no files and with an empty valid root policy", () => {
    expect(fs.existsSync(HOST_POLICY_PATH)).toBe(false);
    expect(load().executor.landlock).toEqual(config.DEFAULT_FABRIC_CONFIG.executor.landlock);
    expect(config.liveLandlockSettings(load().executor.landlock, agentDir)).toEqual({ mode: "off", disabled: false });
    simulateRootOwnership(); writePolicy({});
    expect(load().executor.landlock).toEqual({ mode: "off", disabled: false });
    expect(config.liveLandlockSettings(load().executor.landlock, agentDir)).toEqual({ mode: "off", disabled: false });
  });

  it.each([true, false])("ignores agent disabled=%s without a root policy or mode setting", disabled => {
    writeAgent({ executor: { landlock: { disabled } } });
    expect(load().executor.landlock).toEqual({ mode: "off", disabled: false });
    expect(config.readHostLandlockDisabled(agentDir)).toBe(false);
    expect(config.liveLandlockSettings({ mode: "off", disabled }, agentDir)).toEqual({ mode: "off", disabled: false });
  });

  it("does not let a project off value undo agent enforcement", () => {
    writeAgent({ executor: { landlock: { mode: "enforce" } } });
    fs.writeFileSync(path.join(cwd, ".pi/fabric.json"), JSON.stringify({ executor: { landlock: { mode: "off" } } }));
    expect(load().executor.landlock).toEqual({ mode: "enforce", disabled: false });
  });

  it("keeps default mode off with invalid policy, but preserves agent enforcement", () => {
    simulateRootOwnership(); fs.writeFileSync(HOST_POLICY_PATH, "{broken");
    expect(load().executor.landlock).toEqual(config.DEFAULT_FABRIC_CONFIG.executor.landlock);
    expect(config.liveLandlockSettings({ mode: "off", disabled: true }, agentDir)).toEqual({ mode: "off", disabled: false });
    writeAgent({ executor: { landlock: { mode: "enforce", disabled: true } } });
    expect(load().executor.landlock).toEqual({ mode: "enforce", disabled: false });
    expect(config.liveLandlockSettings(load().executor.landlock, agentDir)).toEqual({ mode: "enforce", disabled: false });
  });

  it("ignores invalid root Landlock fields rather than changing product defaults", () => {
    simulateRootOwnership(); writePolicy({ executor: { landlock: { mode: "permissive", disabled: "true" } } });
    expect(load().executor.landlock).toEqual({ mode: "off", disabled: false });
    expect(config.liveLandlockSettings(load().executor.landlock, agentDir)).toEqual({ mode: "off", disabled: false });
  });

  it("keeps tightening agent settings without a root policy", () => {
    writeAgent({ executor: { landlock: { mode: "enforce" } }, agents: {
      deniedModels: ["agent-deny"], modelPolicy: { requireReason: ["agent-reason"] },
    } });
    const loaded = load();
    expect(loaded.executor.landlock).toEqual({ mode: "enforce", disabled: false });
    expect(loaded.agents.deniedModels).toEqual(["agent-deny"]);
    expect(loaded.agents.modelPolicy.requireReason).toEqual(["gpt-6-astra", "agent-reason"]);
  });

  it("applies root relaxations and unions only agent restrictions across all loaders", () => {
    simulateRootOwnership();
    writePolicy({ executor: { landlock: { disabled: true, mode: "enforce" } }, fullCodeMode: false,
      agents: { processSlice: "root.slice", deniedModels: [" ROOT-DENY "], modelPolicy: { requireReason: [] } } });
    writeAgent({ executor: { landlock: { disabled: false } }, agents: {
      processSlice: "agent.slice", deniedModels: ["agent-deny", "root-deny"], modelPolicy: { requireReason: [" AGENT-REASON "] },
    } });
    fs.writeFileSync(path.join(cwd, ".pi/fabric.json"), JSON.stringify({ agents: {
      processSlice: "project.slice", deniedModels: [], modelPolicy: { requireReason: [] },
    }, executor: { landlock: { disabled: false } } }));
    for (const loaded of [load(), config.loadGlobalFabricConfig(agentDir),
      config.loadFabricConfigForScope({ cwd, agentDir, projectTrusted: true }, "global")]) {
      expect(loaded.executor.landlock.disabled).toBe(true);
      expect(loaded.executor.landlock.mode).toBe("enforce"); // root policy controls Landlock mode
      expect(loaded.fullCodeMode).toBe(config.DEFAULT_FABRIC_CONFIG.fullCodeMode);
      expect(loaded.agents.processSlice).toBe("root.slice");
      expect(loaded.agents.deniedModels).toEqual(["root-deny", "agent-deny"]);
      expect(loaded.agents.modelPolicy.requireReason).toEqual(["agent-reason"]);
    }
    writeAgent({ agents: { deniedModels: [], modelPolicy: { requireReason: [] } } });
    writePolicy({ agents: { deniedModels: ["root-deny"], modelPolicy: { requireReason: ["root-reason"] } } });
    expect(load().agents.deniedModels).toEqual(["root-deny"]);
    expect(load().agents.modelPolicy.requireReason).toEqual(["root-reason"]);
    writePolicy({ agents: { deniedModels: [], modelPolicy: { requireReason: [] } } });
    expect(load().agents.deniedModels).toEqual([]); // only root can remove its deny
    expect(load().agents.modelPolicy.requireReason).toEqual([]);
  });

  it("ignores agent kill switch once provisioned and preserves root Landlock mode", () => {
    simulateRootOwnership(); writePolicy({});
    writeAgent({ executor: { landlock: { mode: "enforce", disabled: true } }, agents: {
      deniedModels: ["agent-deny"], modelPolicy: { requireReason: ["extra-reason"] },
    } });
    expect(load().executor.landlock).toEqual({ mode: "enforce", disabled: false });
    expect(load().agents.modelPolicy.requireReason).toEqual(["gpt-6-astra", "extra-reason"]);
    writeAgent({ agents: { deniedModels: ["agent-deny"], modelPolicy: { requireReason: [] } } });
    expect(load().agents.deniedModels).toEqual(["agent-deny"]);
    expect(load().agents.modelPolicy.requireReason).toEqual(["gpt-6-astra"]);
  });

  it.each(["non-root", "group-writable", "world-writable", "directory", "malformed", "array", "symlink"])("rejects %s policy without fallback", kind => {
    simulateRootOwnership();
    writeAgent({ executor: { landlock: { mode: "enforce", disabled: true } }, agents: {
      processSlice: "unsafe.slice", deniedModels: ["agent-deny"], modelPolicy: { requireReason: [] },
    } });
    writePolicy({ executor: { landlock: { disabled: true } } });
    if (kind === "non-root") metadata.set(HOST_POLICY_PATH, { uid: 1000 });
    if (kind === "group-writable") fs.chmodSync(HOST_POLICY_PATH, 0o660);
    if (kind === "world-writable") fs.chmodSync(HOST_POLICY_PATH, 0o606);
    if (kind === "malformed") fs.writeFileSync(HOST_POLICY_PATH, "{broken");
    if (kind === "array") writePolicy([]);
    if (kind === "directory") { fs.unlinkSync(HOST_POLICY_PATH); fs.mkdirSync(HOST_POLICY_PATH); }
    if (kind === "symlink") {
      fs.renameSync(HOST_POLICY_PATH, `${HOST_POLICY_PATH}.target`);
      fs.symlinkSync(`${HOST_POLICY_PATH}.target`, HOST_POLICY_PATH);
    }
    for (let i = 0; i < 2; i++) {
      const loaded = load();
      expect(loaded.executor.landlock.disabled).toBe(false);
      expect(loaded.agents.processSlice).toBeUndefined();
      expect(loaded.agents.deniedModels).toEqual(["agent-deny"]);
      expect(loaded.agents.modelPolicy.requireReason).toEqual(["gpt-6-astra"]);
      expect(config.liveLandlockSettings({ mode: "enforce", disabled: true }, agentDir).disabled).toBe(false);
    }
    fs.unlinkSync(path.join(agentDir, "fabric.json"));
    expect(load().executor.landlock).toEqual(config.DEFAULT_FABRIC_CONFIG.executor.landlock);
    expect(config.liveLandlockSettings({ mode: "off", disabled: true }, agentDir)).toEqual({ mode: "off", disabled: false });
    expect(warn.mock.calls.filter(call => String(call[0]).includes("ignoring untrusted"))).toHaveLength(1);
    expect(warn.mock.calls.some(call => String(call[0]).includes("is missing"))).toBe(false);
  });

  it("rejects a real non-root-owned file without simulated metadata", () => {
    if (process.getuid?.() === 0) return; // covered above via explicit non-root uid on root runners
    writePolicy({ executor: { landlock: { disabled: true } } });
    expect(policy.readHostPolicy().status).toBe("invalid");
  });

  it("applies a root-owned mode-off relaxation while ignoring the agent copy", () => {
    simulateRootOwnership(); writePolicy({ executor: { landlock: { mode: "off" } } });
    writeAgent({ executor: { landlock: { mode: "off", disabled: false } } });
    const loaded = load().executor.landlock;
    expect(loaded).toEqual({ mode: "off", disabled: false });
    expect(config.liveLandlockSettings(loaded, agentDir)).toEqual(loaded);
    writePolicy({});
    expect(config.liveLandlockSettings(loaded, agentDir)).toEqual({ mode: "off", disabled: false });
    writePolicy({ executor: { landlock: { mode: "enforce" } } });
    expect(config.liveLandlockSettings(loaded, agentDir)).toEqual({ mode: "enforce", disabled: false });
    writePolicy({ executor: { landlock: { mode: "off" } } });
    writeAgent({ executor: { landlock: { mode: "enforce" } } });
    expect(load().executor.landlock).toEqual({ mode: "enforce", disabled: false });
  });

  it.each(["non-root", "writable", "symlink"])("rejects %s ancestry", kind => {
    simulateRootOwnership(); writePolicy({});
    if (kind === "non-root") metadata.set(policyDirectory, { uid: 1000 });
    if (kind === "writable") metadata.set(policyDirectory, { mode: 0o777 });
    if (kind === "symlink") {
      const target = path.join(agentDir, "policy-target");
      fs.renameSync(policyDirectory, target);
      fs.symlinkSync(target, policyDirectory);
    }
    expect(policy.readHostPolicy().status).toBe("invalid");
  });

  it.each(["inode", "device", "owner", "mode"])("rejects opened-fd %s changes and closes the fd", change => {
    simulateRootOwnership(); writePolicy({});
    const stat = fs.statSync(HOST_POLICY_PATH);
    openedMetadata = change === "inode" ? { ino: stat.ino + 1 } : change === "device" ? { dev: stat.dev + 1 }
      : change === "owner" ? { uid: 1000 } : { mode: 0o100666 };
    expect(policy.readHostPolicy().status).toBe("invalid");
    expect(fs.fstatSync).toHaveBeenCalledTimes(1);
    expect(() => fs.readFileSync(openedDescriptor!)).toThrow();
  });

  it("reads through one verified fd opened O_NOFOLLOW and closes it", () => {
    simulateRootOwnership(); writePolicy({ agents: { processSlice: "root.slice" } });
    vi.mocked(fs.openSync).mockClear();
    expect(policy.readHostPolicy()).toEqual({ status: "valid", document: { agents: { processSlice: "root.slice" } } });
    expect(fs.fstatSync).toHaveBeenCalledTimes(1);
    const call = vi.mocked(fs.openSync).mock.calls.find(args => String(args[0]) === HOST_POLICY_PATH)!;
    expect(Number(call[1]) & fs.constants.O_NOFOLLOW).toBe(fs.constants.O_NOFOLLOW);
    expect(() => fs.readFileSync(openedDescriptor!)).toThrow();
  });

  it("does not fall back if an existing file vanishes before open", () => {
    simulateRootOwnership(); writePolicy({});
    vi.mocked(fs.openSync).mockImplementationOnce(() => { throw Object.assign(new Error("removed"), { code: "ENOENT" }); });
    expect(policy.readHostPolicy().status).toBe("invalid");
  });

  it("rereads root kill-switch flips and revokes a loaded grant when policy becomes unsafe", () => {
    simulateRootOwnership(); writePolicy({ executor: { landlock: { disabled: true } } });
    writeAgent({ executor: { landlock: { mode: "enforce", disabled: true } } });
    const loaded = load().executor.landlock;
    expect(config.liveLandlockSettings(loaded, agentDir).disabled).toBe(true);
    writePolicy({ executor: { landlock: { disabled: false } } });
    expect(config.liveLandlockSettings(loaded, agentDir).disabled).toBe(false);
    writePolicy({ executor: { landlock: { disabled: true } } });
    fs.chmodSync(HOST_POLICY_PATH, 0o660);
    expect(config.liveLandlockSettings(loaded, agentDir).disabled).toBe(false);
  });

  it.skipIf(process.platform !== "linux").each([
    { landlock: { mode: "enforce", disabled: true }, rootEnforce: false },
    { landlock: { mode: "off" }, rootEnforce: true },
    { landlock: { mode: "permissive" }, rootEnforce: true },
  ])("enforces opted-in real Bash (%j); only a verified root flip disables it", async ({ landlock, rootEnforce }) => {
    simulateRootOwnership();
    if (rootEnforce) writePolicy({ executor: { landlock: { mode: "enforce" } } });
    writeAgent({ executor: { landlock } });
    const loaded = load().executor.landlock;
    const tmpdir = path.join(agentDir, "private-tmp"); fs.mkdirSync(tmpdir, { mode: 0o700 });
    vi.stubEnv("TMPDIR", tmpdir); vi.stubEnv("SMARTY_ROLE", "task-agent@reviewed-policy");
    const { PiToolsProvider } = await import("../src/providers/pi-tools-provider.js");
    const { ActionRegistry } = await import("../src/core/action-registry.js");
    const { SessionManager, createExtensionRuntime, ExtensionRunner } = await import("@earendil-works/pi-coding-agent");
    const runtime = createExtensionRuntime(); runtime.getThinkingLevel = () => "off";
    const runner = new ExtensionRunner([], runtime, cwd, SessionManager.inMemory(cwd), {} as never);
    const provider = new PiToolsProvider(cwd, undefined, undefined, {
      powerShellToolDefinitionFactory: undefined, getShellHangMs: () => 0,
      getLandlockSettings: () => config.liveLandlockSettings(loaded, agentDir),
    });
    const registry = new ActionRegistry(); registry.register(provider);
    const context = { cwd, extensionContext: runner.createContext(), signal: new AbortController().signal,
      parentToolCallId: "root-policy-parent", nestedToolCallId: "root-policy-bash", update: () => {},
      approve: async () => {}, audits: [], maxResultChars: 100_000,
    };
    const victim = path.join(agentDir, "victim");
    const quote = (text: string) => `'${text.replaceAll("'", "'\\''")}'`;
    const writeVictim = async (): Promise<boolean> => {
      try {
        const result = await registry.invoke("pi.bash", { command: `printf released > ${quote(victim)}` }, context) as { ok: boolean };
        return result.ok;
      } catch { return false; }
    };
    try {
      expect(await writeVictim()).toBe(false);
      expect(fs.existsSync(victim)).toBe(false);
      writeAgent({ executor: { landlock: { mode: "enforce", disabled: true } } });
      expect(await writeVictim()).toBe(false); // an edit after session load cannot grant an escape
      writePolicy({ executor: { landlock: { disabled: true } } });
      expect(await writeVictim()).toBe(true);
      expect(fs.readFileSync(victim, "utf8")).toBe("released");
      fs.chmodSync(HOST_POLICY_PATH, 0o660); fs.writeFileSync(victim, "protected");
      expect(await writeVictim()).toBe(false);
      expect(fs.readFileSync(victim, "utf8")).toBe("protected");
    } finally { await registry.close(); }
  });

  it("ignores environment/config attempts to select a different authority path", () => {
    const other = path.join(agentDir, "other-policy.json");
    fs.writeFileSync(other, JSON.stringify({ executor: { landlock: { disabled: true } } }));
    vi.stubEnv("PI_FABRIC_HOST_POLICY_PATH", other); vi.stubEnv("FABRIC_HOST_POLICY_PATH", other);
    writeAgent({ hostPolicyPath: other, agents: { modelPolicy: { requireReason: [] } } });
    expect(policy.readHostPolicy().status).toBe("missing");
    expect(load().agents.modelPolicy.requireReason).toEqual(["gpt-6-astra"]);
  });
});
