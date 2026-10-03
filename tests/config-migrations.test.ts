import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  CURRENT_FABRIC_CONFIG_VERSION,
  migrateFabricConfigDocument,
} from "../src/config-migrations.js";
import { loadFabricConfig, saveFabricConfig, writeJsonAtomic } from "../src/config.js";

const roots: string[] = [];

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-migration-"));
  roots.push(root);
  const cwd = path.join(root, "project");
  const agentDir = path.join(root, "agent");
  fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
  fs.mkdirSync(agentDir, { recursive: true });
  return {
    cwd,
    agentDir,
    globalPath: path.join(agentDir, "fabric.json"),
    projectPath: path.join(cwd, ".pi", "fabric.json"),
  };
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("Fabric configuration migrations", () => {
  it.skipIf(process.platform === "win32")("#2479 syncs new config ancestors after publishing the file", () => {
    const paths = fixture(), target = path.join(paths.agentDir, "nested", "deeper", "fabric.json");
    const events: string[] = [], descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { events.push(descriptors.get(fd)!); sync(fd); });
    writeJsonAtomic(target, { agents: { maxConcurrent: 7 } }, null);
    expect(events).toContain(path.dirname(target));
    expect(events).toContain(path.dirname(path.dirname(target)));
    expect(events).toContain(paths.agentDir);
    expect(events).toContain(path.dirname(paths.agentDir));
  });
  it("migrates the legacy agent section without mutating its input", () => {
    const input = { subagents: { runner: "claude", defaultTools: ["read"] }, ui: { enabled: false } };
    const result = migrateFabricConfigDocument(input);

    expect(result).toMatchObject({
      fromVersion: 0,
      toVersion: CURRENT_FABRIC_CONFIG_VERSION,
      appliedVersions: [1, 2, 3, 4],
      changed: true,
    });
    expect(result.document).toEqual({
      configVersion: 4,
      agents: { runner: "claude", defaultTools: ["read"] },
      ui: { enabled: false },
    });
    expect(input).toHaveProperty("subagents");
  });

  it("merges both section names with the canonical section taking precedence", () => {
    const result = migrateFabricConfigDocument({
      subagents: { runner: "pi", claude: { binary: "old", model: "claude/old" }, defaultTools: ["bash"] },
      agents: { runner: "claude", claude: { binary: "new" }, defaultTools: ["read"] },
    });

    expect(result.document.agents).toEqual({
      runner: "claude",
      claude: { binary: "new", model: "claude/old" },
      defaultTools: ["read"],
    });
    expect(result.document).not.toHaveProperty("subagents");
  });

  it("rejects an ambiguous malformed canonical section instead of discarding legacy values", () => {
    expect(() =>
      migrateFabricConfigDocument({ subagents: { maxConcurrent: 6 }, agents: false }),
    ).toThrow(/malformed agents section/);
  });

  it("rejects invalid and legacy keys in current documents", () => {
    expect(() => migrateFabricConfigDocument({ configVersion: -1 })).toThrow(/non-negative integer/);
    expect(() =>
      migrateFabricConfigDocument({ configVersion: 1, subagents: {} }),
    ).toThrow(/removed key/);
  });

  it("accepts newer configuration versions as forward-compatible documents", () => {
    // A config written by a newer build (schema only adds semantics) must not
    // brick this extension: accept as-is, apply no migrations, never rewrite.
    const input = { configVersion: 5, futureSection: { enabled: true } };
    const result = migrateFabricConfigDocument(input);
    expect(result.document).toEqual(input);
    expect(result.fromVersion).toBe(5);
    expect(result.toVersion).toBe(5);
    expect(result.appliedVersions).toEqual([]);
    expect(result.changed).toBe(false);
    expect(result.forwardCompatible).toBe(true);
  });

  it("migrates each config layer before applying project precedence", () => {
    const paths = fixture();
    fs.writeFileSync(paths.globalPath, JSON.stringify({ agents: { runner: "claude", maxConcurrent: 2 } }));
    fs.writeFileSync(paths.projectPath, JSON.stringify({ subagents: { runner: "pi", transport: "tmux" } }));

    const config = loadFabricConfig({ cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: true });
    expect(config.agents).toMatchObject({ runner: "pi", transport: "tmux", maxConcurrent: 2 });
    expect(JSON.parse(fs.readFileSync(paths.globalPath, "utf8"))).toMatchObject({ configVersion: 4, agents: { runner: "claude" } });
    expect(JSON.parse(fs.readFileSync(paths.projectPath, "utf8"))).toEqual({
      configVersion: 4,
      agents: { runner: "pi", transport: "tmux" },
    });
  });

  it("does not inspect or migrate an untrusted project config", () => {
    const paths = fixture();
    const legacy = JSON.stringify({ subagents: { maxConcurrent: 9 } });
    fs.writeFileSync(paths.projectPath, legacy);

    const config = loadFabricConfig({ cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: false });
    expect(config.agents.maxConcurrent).not.toBe(9);
    expect(fs.readFileSync(paths.projectPath, "utf8")).toBe(legacy);
  });

  it("does not rewrite an already-current config during load", () => {
    const paths = fixture();
    const current = JSON.stringify({ configVersion: 4, agents: { maxConcurrent: 3 } }, null, 2) + "\n";
    fs.writeFileSync(paths.globalPath, current);
    const before = fs.statSync(paths.globalPath).mtimeMs;

    loadFabricConfig({ cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: false });

    expect(fs.readFileSync(paths.globalPath, "utf8")).toBe(current);
    expect(fs.statSync(paths.globalPath).mtimeMs).toBe(before);
  });

  it("migrates a legacy target while saving a canonical partial", () => {
    const paths = fixture();
    fs.writeFileSync(paths.projectPath, JSON.stringify({ subagents: { transport: "screen" } }));

    saveFabricConfig(
      { cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: true },
      { agents: { maxConcurrent: 7 } },
    );

    expect(JSON.parse(fs.readFileSync(paths.projectPath, "utf8"))).toEqual({
      configVersion: 4,
      agents: { transport: "screen", maxConcurrent: 7 },
    });
  });

  it.skipIf(process.platform === "win32")(
    "migrates a symlink target without replacing the configuration symlink",
    () => {
      const paths = fixture();
      const target = path.join(path.dirname(paths.globalPath), "shared.json");
      fs.writeFileSync(target, JSON.stringify({ subagents: { maxConcurrent: 5 } }));
      fs.symlinkSync(target, paths.globalPath);

      const config = loadFabricConfig({
        cwd: paths.cwd,
        agentDir: paths.agentDir,
        projectTrusted: false,
      });

      expect(config.agents.maxConcurrent).toBe(5);
      expect(fs.lstatSync(paths.globalPath).isSymbolicLink()).toBe(true);
      expect(JSON.parse(fs.readFileSync(target, "utf8"))).toEqual({
        configVersion: 4,
        agents: { maxConcurrent: 5 },
      });
    },
  );

  it.skipIf(process.platform === "win32")("#2479 R3 F27 rejects a configuration symlink retargeted after publication", () => {
    const paths = fixture(), root = path.dirname(paths.agentDir);
    const oldDir = path.join(root, "old-target"), newDir = path.join(root, "new-target");
    fs.mkdirSync(oldDir); fs.mkdirSync(newDir);
    const oldTarget = path.join(oldDir, "fabric.json"), newTarget = path.join(newDir, "fabric.json");
    const source = JSON.stringify({ subagents: { maxConcurrent: 5 } });
    fs.writeFileSync(oldTarget, source); fs.writeFileSync(newTarget, source); fs.symlinkSync(oldTarget, paths.globalPath);
    const rename = fs.renameSync.bind(fs), open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
    const descriptors = new Map<number, string>(), barriers: string[] = [];
    let published = false;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { if (published) barriers.push(descriptors.get(fd)!); sync(fd); });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (to === oldTarget) { published = true; fs.unlinkSync(paths.globalPath); fs.symlinkSync(newTarget, paths.globalPath); }
    });
    expect(() => loadFabricConfig({ cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: false })).toThrow(/inode changed|configuration changed/);
    expect(published).toBe(true); expect(barriers).toContain(oldDir);
    expect(JSON.parse(fs.readFileSync(oldTarget, "utf8"))).toMatchObject({ configVersion: 4, agents: { maxConcurrent: 5 } });
    expect(fs.readFileSync(newTarget, "utf8")).toBe(source); expect(fs.realpathSync(paths.globalPath)).toBe(newTarget);
  });

  it("does not replace a config concurrently created during its first save", () => {
    const paths = fixture();
    const fsyncSync = fs.fsyncSync.bind(fs);
    const concurrent = { ui: { widget: "always" } };
    let created = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation((descriptor) => {
      fsyncSync(descriptor);
      if (!created && fs.fstatSync(descriptor).isFile()) {
        created = true;
        fs.writeFileSync(paths.projectPath, JSON.stringify(concurrent));
      }
    });

    expect(() =>
      saveFabricConfig(
        { cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: true },
        { prewalk: { enabled: false } },
      )
    ).toThrow(/changed while updating/);
    expect(JSON.parse(fs.readFileSync(paths.projectPath, "utf8"))).toEqual(concurrent);
  });

  it("preserves existing file permissions during migration", () => {
    const paths = fixture();
    fs.writeFileSync(paths.globalPath, JSON.stringify({ agents: { maxConcurrent: 5 } }), { mode: 0o640 });
    const mode = fs.statSync(paths.globalPath).mode & 0o777;

    loadFabricConfig({ cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: false });

    expect(fs.statSync(paths.globalPath).mode & 0o777).toBe(mode);
  });

  it("#2479 skips unsupported Windows directory barriers but fails closed on POSIX", () => {
    const paths = fixture();
    const fsyncSync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation((descriptor) => {
      if (fs.fstatSync(descriptor).isDirectory()) {
        throw Object.assign(new Error("operation not permitted"), { code: "EPERM" });
      }
      fsyncSync(descriptor);
    });

    const save = () => saveFabricConfig(
      { cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: true },
      { agents: { maxConcurrent: 7 } },
    );
    if (process.platform === "win32") expect(save).not.toThrow();
    else expect(save).toThrow("operation not permitted");
  });

  it("rejects obsolete or caller-controlled migration metadata on save", () => {
    const paths = fixture();
    const options = { cwd: paths.cwd, agentDir: paths.agentDir, projectTrusted: true };
    expect(() => saveFabricConfig(options, { subagents: {} })).toThrow(/current schema/);
    expect(() => saveFabricConfig(options, { configVersion: 1 })).toThrow(/current schema/);
  });

  it("renames ui.showNestedToolCalls to ui.showAgentToolPreview without mutating its input", () => {
    const input = {
      configVersion: 1,
      ui: { showNestedToolCalls: false, maxRows: 8 },
    };
    const result = migrateFabricConfigDocument(input);

    expect(result).toMatchObject({
      fromVersion: 1,
      toVersion: CURRENT_FABRIC_CONFIG_VERSION,
      appliedVersions: [2, 3, 4],
      changed: true,
    });
    expect(result.document.ui).toEqual({ showAgentToolPreview: false, maxRows: 8 });
    expect(input.ui).toHaveProperty("showNestedToolCalls");
  });

  it("keeps an explicit ui.showAgentToolPreview value over the legacy key", () => {
    const result = migrateFabricConfigDocument({
      configVersion: 1,
      ui: { showNestedToolCalls: false, showAgentToolPreview: true },
    });

    expect(result.document.ui).toEqual({ showAgentToolPreview: true });
  });

  it("leaves version 1 documents without the legacy key unchanged in content", () => {
    const result = migrateFabricConfigDocument({
      configVersion: 1,
      ui: { maxRows: 4 },
    });

    expect(result.changed).toBe(true); // only the version stamp advances
    expect(result.document).toEqual({ configVersion: 4, ui: { maxRows: 4 } });
  });

  it("renames ui.nestedToolDebounceMs to ui.updateDebounceMs", () => {
    const result = migrateFabricConfigDocument({
      configVersion: 2,
      ui: { nestedToolDebounceMs: 250 },
    });

    expect(result).toMatchObject({
      fromVersion: 2,
      toVersion: CURRENT_FABRIC_CONFIG_VERSION,
      appliedVersions: [3, 4],
      changed: true,
    });
    expect(result.document.ui).toEqual({ updateDebounceMs: 250 });
  });

  it("repairs string booleans emitted for the prewalk master switch", () => {
    const disabledInput = {
      configVersion: 3,
      prewalk: { enabled: "false", mode: "trajectory" },
    };
    const disabled = migrateFabricConfigDocument(disabledInput);
    const enabled = migrateFabricConfigDocument({
      configVersion: 3,
      prewalk: { enabled: "true" },
    });

    expect(disabled.document).toEqual({
      configVersion: 4,
      prewalk: { enabled: false, mode: "trajectory" },
    });
    expect(enabled.document).toEqual({
      configVersion: 4,
      prewalk: { enabled: true },
    });
    expect(disabledInput.prewalk.enabled).toBe("false");
  });

  it("persists a repaired project disable while loading configuration", () => {
    const paths = fixture();
    fs.writeFileSync(
      paths.projectPath,
      JSON.stringify({ configVersion: 3, prewalk: { enabled: "false" } }),
    );

    const config = loadFabricConfig({
      cwd: paths.cwd,
      agentDir: paths.agentDir,
      projectTrusted: true,
    });

    expect(config.prewalk.enabled).toBe(false);
    expect(JSON.parse(fs.readFileSync(paths.projectPath, "utf8"))).toEqual({
      configVersion: 4,
      prewalk: { enabled: false },
    });
  });

  it("persists the rename when loading a legacy ui key", () => {
    const paths = fixture();
    fs.writeFileSync(
      paths.globalPath,
      JSON.stringify({ configVersion: 1, ui: { showNestedToolCalls: false } }),
    );

    const config = loadFabricConfig({
      cwd: paths.cwd,
      agentDir: paths.agentDir,
      projectTrusted: false,
    });

    expect(config.ui.showAgentToolPreview).toBe(false);
    expect(JSON.parse(fs.readFileSync(paths.globalPath, "utf8"))).toEqual({
      configVersion: 4,
      ui: { showAgentToolPreview: false },
    });
  });
});
