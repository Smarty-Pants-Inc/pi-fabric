import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resolveJevFabric } from "../src/jev-fabric/resolve.js";

const mocks = vi.hoisted(() => ({ execFile: vi.fn() }));
vi.mock("node:child_process", async (importOriginal) => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  execFile: mocks.execFile,
}));

const roots: string[] = [];
afterEach(() => {
  mocks.execFile.mockReset();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const root = () => {
  const dir = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resolve-security-")));
  roots.push(dir);
  return dir;
};
const binary = (directory: string) => {
  fs.mkdirSync(directory, { recursive: true });
  const file = path.join(directory, "jev-fabric");
  // No real candidate is executed: all capability probes go through the mock.
  fs.writeFileSync(file, "fixture", { mode: 0o755 });
  return file;
};
const answer = () => mocks.execFile.mockImplementation((_file, _args, _options, callback) => {
  callback(null, JSON.stringify({ version: "0.5.0", protocol: 1, features: ["follow", "list", "label"] }));
});
const options = (dir: string, entry: string) => ({
  configured: "auto", cwd: path.join(dir, "workspace"), agentDir: dir, home: dir,
  requirement: "durable" as const, env: { PATH: entry }, bundled: () => undefined,
});

// Jev's binary transport is unsupported on Windows, as in the main resolve suite.
describe.skipIf(process.platform === "win32")("SEC-11 public resolveJevFabric trust boundary", () => {
  it.each(["..tools", "..tools/nested"])("never probes a non-Git workspace descendant %s", async (descendant) => {
    const dir = root();
    const candidate = binary(path.join(dir, "workspace", descendant));
    answer();
    await expect(resolveJevFabric(options(dir, path.dirname(candidate)))).rejects.toThrow("inside the workspace");
    expect(mocks.execFile).not.toHaveBeenCalled();
  });

  it.each(["directory into workspace", "binary into workspace", "directory outside workspace", "binary outside workspace"])(
    "never probes a symlinked PATH candidate (%s)", async (layout) => {
      const dir = root();
      const target = binary(layout.includes("into workspace")
        ? path.join(dir, "workspace", "..tools") : path.join(dir, "installed"));
      const entry = path.join(dir, "host-looking-tools");
      if (layout.startsWith("directory")) fs.symlinkSync(path.dirname(target), entry, "dir");
      else {
        fs.mkdirSync(entry);
        fs.symlinkSync(target, path.join(entry, "jev-fabric"));
      }
      answer();
      await expect(resolveJevFabric(options(dir, entry))).rejects.toThrow("No suitable jev-fabric");
      expect(mocks.execFile).not.toHaveBeenCalled();
    },
  );

  it.each(["workspace-tools", "..tools"])("still resolves a legitimate sibling install %s", async (directory) => {
    const dir = root();
    const candidate = binary(path.join(dir, directory));
    answer();
    await expect(resolveJevFabric(options(dir, path.dirname(candidate)))).resolves.toMatchObject({ path: candidate, source: "user" });
    expect(mocks.execFile).toHaveBeenCalledExactlyOnceWith(candidate, ["--", "capabilities"], expect.any(Object), expect.any(Function));
  });

  it("falls back to a legitimate install without probing rejected PATH aliases", async () => {
    const dir = root();
    const rejected = binary(path.join(dir, "workspace", "..tools"));
    const trusted = binary(path.join(dir, "installed"));
    const alias = path.join(dir, "alias");
    fs.symlinkSync(path.dirname(trusted), alias, "dir");
    answer();
    const entry = [path.dirname(rejected), alias, path.dirname(trusted)].join(path.delimiter);
    await expect(resolveJevFabric(options(dir, entry))).resolves.toMatchObject({ path: trusted, source: "user" });
    expect(mocks.execFile).toHaveBeenCalledExactlyOnceWith(trusted, ["--", "capabilities"], expect.any(Object), expect.any(Function));
  });
});
