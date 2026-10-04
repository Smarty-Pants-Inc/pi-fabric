import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { JevClient, JevCredentials } from "../src/jev/client.js";
import type { DurableShellBridge } from "../src/jev-fabric/bridge.js";
import { resolveJevFabric, stageBundledJevFabric } from "../src/jev-fabric/resolve.js";
import { JevProvider } from "../src/providers/jev-provider.js";
import { jevContext } from "./jev-test-helpers.js";

const mocks = vi.hoisted(() => ({ execFile: vi.fn(), open: vi.fn(), request: vi.fn() }));
vi.mock("node:child_process", async (original) => ({
  ...await original<typeof import("node:child_process")>(), execFile: mocks.execFile,
}));
vi.mock("../src/jev-fabric/serve.js", () => ({ JevFabricServe: { open: mocks.open } }));
const roots: string[] = [];
afterEach(() => {
  vi.resetAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const root = () => {
  const dir = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-provenance-")));
  roots.push(dir);
  return dir;
};
const binary = (directory: string) => {
  fs.mkdirSync(directory, { recursive: true, mode: 0o755 });
  const file = path.join(directory, "jev-fabric");
  fs.writeFileSync(file, "fixture, never executed", { mode: 0o755 });
  return file;
};
const answer = () => mocks.execFile.mockImplementation((_file, _args, _options, callback) => {
  callback(null, JSON.stringify({ version: "0.5.0", protocol: 2, features: ["follow", "list", "label", "serve-concurrent", "serve-24h", "jev-request-credential"] }));
});
const options = (dir: string, cwd: string, entry: string) => ({
  configured: "auto", cwd, agentDir: path.join(dir, "agent"), home: path.join(dir, "home"),
  requirement: "jev" as const, env: { PATH: entry }, bundled: () => undefined,
});

describe.skipIf(process.platform === "win32")("SR-11 installed executable provenance", () => {
  it.each(["repository", "worktree", "project"])("nested Pi cwd refuses ancestor/sibling candidates in a %s with zero probes or credential forwarding", async (kind) => {
    const dir = root();
    const repo = path.join(dir, "repo");
    const cwd = path.join(repo, "src", "nested");
    fs.mkdirSync(cwd, { recursive: true });
    if (kind === "repository") fs.mkdirSync(path.join(repo, ".git"));
    else fs.writeFileSync(path.join(repo, kind === "worktree" ? ".git" : "package.json"), kind === "worktree" ? "gitdir: /not-followed" : "{}");
    const ancestor = binary(path.join(repo, "bin"));
    const sibling = binary(path.join(repo, "tools", "bin"));
    const resolveOptions = options(dir, cwd, [path.dirname(ancestor), path.dirname(sibling)].join(path.delimiter));
    answer(); // Even a perfect capabilities claim grants no executable authority.
    await expect(resolveJevFabric(resolveOptions)).rejects.toThrow("inside the workspace");
    expect(mocks.execFile).not.toHaveBeenCalled();

    const credential = vi.fn(async () => "test-only-never-a-real-key");
    const fetcher = vi.fn();
    const config = normalizeFabricConfig({ jev: { transport: "jev-fabric" } });
    const registry = new ActionRegistry();
    const client = new JevClient(config.jev, fetcher, new JevCredentials([], {}, { configured: () => true, resolve: credential }));
    const bridge = { home: dir, options: { cwd }, resolve: () => resolveJevFabric(resolveOptions) } as unknown as DurableShellBridge;
    const provider = new JevProvider({ registry, config, jevFabric: bridge }, client);
    mocks.open.mockResolvedValue({ request: mocks.request, close: vi.fn() });
    try {
      await expect(provider.invoke("evaluate", { state: "fixture", questions: { safe: { type: "noul", instructions: "fixture" } } },
        { ...jevContext(), cwd, parentToolCallId: "jev:provenance" })).rejects.toThrow("No suitable jev-fabric");
      expect(mocks.execFile).not.toHaveBeenCalled();
      expect(mocks.open).not.toHaveBeenCalled();
      expect(mocks.request).not.toHaveBeenCalled();
      expect(credential).not.toHaveBeenCalled();
      expect(fetcher).not.toHaveBeenCalled();
    } finally { await provider.close(); }
  });

  it.each(["direct", "release symlink"])("pins a legitimate external %s installation to its canonical path", async (layout) => {
    const dir = root();
    const home = path.join(dir, "home");
    const install = layout === "direct" ? path.join(home, ".local", "bin") : path.join(home, ".local", "share", "jev-fabric", "releases", "v0.5.0", "bin");
    const candidate = binary(install);
    const entry = path.join(home, ".local", "bin");
    if (layout !== "direct") {
      fs.mkdirSync(entry, { recursive: true });
      fs.symlinkSync(candidate, path.join(entry, "jev-fabric"));
    }
    answer();
    await expect(resolveJevFabric(options(dir, path.join(dir, "repo", "nested"), entry))).resolves.toMatchObject({ path: candidate, source: "user" });
    expect(mocks.execFile).toHaveBeenCalledExactlyOnceWith(candidate, ["--", "capabilities"], expect.any(Object), expect.any(Function));
  });

  it.each(["alias", "redirected root", "other worktree", "world writable", "uninstalled sibling"])("refuses %s before probing", async (layout) => {
    const dir = root();
    const repo = path.join(dir, "repo");
    const cwd = path.join(repo, "nested");
    fs.mkdirSync(cwd, { recursive: true }); fs.mkdirSync(path.join(repo, ".git"));
    const home = path.join(dir, "home");
    let entry = path.join(home, ".local", "bin");
    if (layout === "alias") {
      const target = binary(path.join(repo, "tools"));
      fs.mkdirSync(entry, { recursive: true }); fs.symlinkSync(target, path.join(entry, "jev-fabric"));
    } else if (layout === "redirected root") {
      const target = binary(path.join(dir, "uninstalled"));
      fs.mkdirSync(path.dirname(entry), { recursive: true }); fs.symlinkSync(path.dirname(target), entry, "dir");
    } else if (layout === "other worktree") {
      binary(entry); fs.writeFileSync(path.join(home, ".git"), "gitdir: /not-followed");
    } else if (layout === "world writable") fs.chmodSync(binary(entry), 0o777);
    else { entry = path.join(dir, "uninstalled"); binary(entry); }
    answer();
    await expect(resolveJevFabric(options(dir, cwd, entry))).rejects.toThrow("No suitable jev-fabric");
    expect(mocks.execFile).not.toHaveBeenCalled();
  });

  it("retains the explicit trusted custom-prefix override", async () => {
    const dir = root(); const candidate = binary(path.join(dir, "custom")); answer();
    await expect(resolveJevFabric({ ...options(dir, dir, ""), configured: candidate })).resolves.toMatchObject({ path: candidate, source: "config" });
  });

  it.each(["repository", "worktree", "repository alias", "staging worktree", "relative source", "relative agent directory"])("bundled staging refuses %s before creating an installed release", (layout) => {
    const dir = root();
    const repo = path.join(dir, "checkout");
    fs.mkdirSync(repo, { recursive: true });
    fs.writeFileSync(path.join(repo, ".git"), "gitdir: /fixture-not-followed");
    let agentDir = path.join(dir, "agent");
    let source = binary(path.join(dir, "installed", "node_modules", "jev-fabric-linux-x64", "bin"));
    if (layout === "repository" || layout === "worktree" || layout === "repository alias") {
      if (layout === "repository") { fs.unlinkSync(path.join(repo, ".git")); fs.mkdirSync(path.join(repo, ".git")); }
      source = binary(path.join(repo, "bin"));
      if (layout === "repository alias") {
        const alias = path.join(dir, "installed-source-alias");
        fs.symlinkSync(source, alias); source = alias;
      }
    } else if (layout === "staging worktree") agentDir = repo;
    else if (layout === "relative source") source = "relative-source/jev-fabric";
    else agentDir = "relative-agent";
    expect(stageBundledJevFabric(agentDir, () => ({ version: "0.5.0", binaryPath: () => source }))).toBeUndefined();
    expect(fs.existsSync(path.join(agentDir, "fabric"))).toBe(false);
    expect(mocks.execFile).not.toHaveBeenCalled();
  });

  it("refuses an installation leaf owned by another principal", async () => {
    const dir = root(); const candidate = binary(path.join(dir, "home", ".local", "bin"));
    const lstat = fs.lstatSync.bind(fs);
    const spy = vi.spyOn(fs, "lstatSync").mockImplementation(((target: any, ...args: any[]) => {
      const stat = (lstat as any)(target, ...args);
      return target === candidate ? Object.assign(Object.create(stat), { uid: process.getuid!() + 1 }) : stat;
    }) as typeof fs.lstatSync);
    answer();
    try {
      await expect(resolveJevFabric(options(dir, path.join(dir, "repo", "nested"), path.dirname(candidate)))).rejects.toThrow("not host-owned");
      expect(mocks.execFile).not.toHaveBeenCalled();
    } finally { spy.mockRestore(); }
  });

  it("refuses a repository-provided bundled candidate too", async () => {
    const dir = root(); const repo = path.join(dir, "repo"); const candidate = binary(path.join(repo, "bin"));
    fs.mkdirSync(path.join(repo, ".git")); answer();
    await expect(resolveJevFabric({ ...options(dir, path.join(repo, "subdir"), ""), agentDir: repo, bundled: () => candidate })).rejects.toThrow("inside the workspace");
    expect(mocks.execFile).not.toHaveBeenCalled();
  });
});
