import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

const gitConfig = vi.hoisted(() => vi.fn(() => "git@github.com:Smarty-Pants-Inc/pi-fabric.git\n"));
vi.mock("node:child_process", () => ({ execFileSync: gitConfig }));
const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("repository identity startup", () => {
  it("does no git config work during cold import or idle, then memoizes the first repository use", async () => {
    vi.resetModules();
    gitConfig.mockClear();
    const identity = await import("../src/topology/project-identity.js");
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(gitConfig).not.toHaveBeenCalled();
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-origin-first-use-"));
    roots.push(cwd);
    fs.mkdirSync(path.join(cwd, ".git"));
    expect(identity.repositoryOf(cwd)).toBe("github.com/smarty-pants-inc/pi-fabric");
    expect(identity.repositoryOf(cwd)).toBe("github.com/smarty-pants-inc/pi-fabric");
    expect(gitConfig).toHaveBeenCalledExactlyOnceWith("git", ["-C", identity.projectOf(cwd), "config", "--get", "remote.origin.url"],
      expect.objectContaining({ timeout: 2_000, stdio: ["ignore", "pipe", "ignore"] }));
  });
});
