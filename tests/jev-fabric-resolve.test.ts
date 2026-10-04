import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { probeJevFabric, resolveJevFabric, stageBundledJevFabric, userCandidates } from "../src/jev-fabric/resolve.js";
import { jevFabricStatus } from "../src/jev-fabric/status.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const root = () => { const dir = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resolve-"))); roots.push(dir); return dir; };

// A stand-in binary answering capabilities (or only --version, like 0.4.0).
const fake = (dir: string, answer: { capabilities?: object; version: string }) => {
  fs.mkdirSync(dir, { recursive: true, mode: 0o755 });
  const file = path.join(dir, "jev-fabric");
  const capabilities = answer.capabilities ? `[ "$2" = capabilities ] && { echo '${JSON.stringify(answer.capabilities)}'; exit 0; }\n` : `[ "$2" = capabilities ] && { echo 'unknown command' >&2; exit 2; }\n`;
  fs.writeFileSync(file, `#!/bin/sh\n${capabilities}[ "$2" = --version ] && { echo '${answer.version}'; exit 0; }\nexit 2\n`, { mode: 0o755 });
  return file;
};
const current = { capabilities: { version: "0.5.0-native", protocol: 2, store: 1, features: ["follow", "list", "label", "sessions", "serve-concurrent", "read", "cwd", "serve-24h", "durable-input"] }, version: "0.5.0-native" };
const newer = { capabilities: { version: "0.7.0-native", protocol: 2, store: 1, features: ["follow", "list", "label", "sessions", "serve-concurrent", "read", "cwd", "serve-24h", "durable-input", "future"] }, version: "0.7.0-native" };

describe.skipIf(process.platform === "win32")("jev-fabric binary resolution", () => {
  it("maps a 0.4 binary without capabilities to protocol 1 with its shipped features", async () => {
    const dir = root();
    expect(await probeJevFabric(fake(dir, { version: "0.4.0-native (Bend 2.0.34)" }))).toEqual({ version: "0.4.0-native (Bend 2.0.34)", protocol: 1, features: ["follow", "list", "label", "start-24h"] });
    expect(await probeJevFabric(fake(path.join(dir, "old"), { version: "0.3.1-native" }))).toMatchObject({ protocol: 0, features: [] });
  });

  it("skips binaries inside the workspace or on relative PATH entries", () => {
    const dir = root();
    const workspace = path.join(dir, "repo");
    fake(path.join(workspace, "bin"), current);
    const user = fake(path.join(dir, ".local", "bin"), current);
    const found = userCandidates(workspace, { PATH: [path.join(workspace, "bin"), "relative/bin", path.join(dir, ".local", "bin")].join(path.delimiter) }, dir);
    expect(found.found[0]).toBe(user);
    expect(found.skipped).toEqual([{ path: path.join(workspace, "bin", "jev-fabric"), reason: "inside the workspace" }]);
  });

  it("prefers a compatible user install, including a newer one", async () => {
    const dir = root();
    const user = fake(path.join(dir, ".local", "bin"), newer);
    const bundled = fake(path.join(dir, "bundled"), current);
    const resolution = await resolveJevFabric({ configured: "", cwd: path.join(dir, "repo"), agentDir: dir, home: dir, requirement: "sessions", env: { PATH: path.join(dir, ".local", "bin") }, bundled: () => bundled });
    expect(resolution).toMatchObject({ path: user, source: "user", capabilities: { version: "0.7.0-native" } });
    expect(jevFabricStatus()).toContain("(yours)");
  });

  it("falls back to the bundled binary when the user's is too old, and says why", async () => {
    const dir = root();
    const user = fake(path.join(dir, ".local", "bin"), { version: "0.4.0-native" });
    const bundled = fake(path.join(dir, "bundled"), current);
    const resolution = await resolveJevFabric({ configured: "auto", cwd: path.join(dir, "repo"), agentDir: dir, home: dir, requirement: "sessions", env: { PATH: path.join(dir, ".local", "bin") }, bundled: () => bundled });
    expect(resolution).toMatchObject({ path: bundled, source: "bundled", skipped: [{ path: user, source: "user", reason: "protocol 1 < 2 (0.4.0-native)" }] });
    // The same old binary still serves durable tasks, which protocol 1 covers.
    expect(await resolveJevFabric({ configured: "", cwd: path.join(dir, "repo"), agentDir: dir, home: dir, requirement: "durable", env: { PATH: path.join(dir, ".local", "bin") }, bundled: () => bundled })).toMatchObject({ path: user, source: "user" });
  });

  it("stages the bundled binary at a versioned path that survives package upgrades", () => {
    const dir = root();
    const source = fake(path.join(dir, "node_modules", "jev-fabric-darwin", "bin"), current);
    const staged = stageBundledJevFabric(dir, () => ({ version: "0.5.0", binaryPath: () => source }));
    expect(staged).toBe(path.join(dir, "fabric", "jev-fabric", "0.5.0", "jev-fabric"));
    expect(fs.statSync(staged!).mode & 0o777).toBe(0o755);
    const before = fs.statSync(staged!).mtimeMs;
    expect(stageBundledJevFabric(dir, () => ({ version: "0.5.0", binaryPath: () => source }))).toBe(staged);
    expect(fs.statSync(staged!).mtimeMs).toBe(before);
    expect(stageBundledJevFabric(dir, () => { throw new Error("not installed"); })).toBeUndefined();
    expect(stageBundledJevFabric(dir, () => ({ version: "../escape", binaryPath: () => source }))).toBeUndefined();
  });

  it("never falls back from an explicit binary, and explains a total miss", async () => {
    const dir = root();
    const old = fake(path.join(dir, "old"), { version: "0.4.0-native" });
    const bundled = fake(path.join(dir, "bundled"), current);
    await expect(resolveJevFabric({ configured: old, cwd: dir, agentDir: dir, home: dir, requirement: "sessions", env: { PATH: "" }, bundled: () => bundled }))
      .rejects.toThrow("does not fall back from an explicit binary");
    await expect(resolveJevFabric({ configured: "", cwd: dir, agentDir: dir, home: dir, requirement: "durable", env: { PATH: "" }, bundled: () => undefined }))
      .rejects.toThrow("No suitable jev-fabric for durable");
  });
});
