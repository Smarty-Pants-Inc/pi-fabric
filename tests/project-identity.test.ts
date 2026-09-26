import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { participantRole, projectOf, resolveProjectAgent } from "../src/topology/project-identity.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const git = (cwd: string, ...args: string[]) =>
  execFileSync("git", args, { cwd, stdio: "ignore", env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1" } });

// smarty-dev#784: a worktree agent could not find its project agent.
describe("project identity", () => {
  it("maps a linked worktree and a subdirectory to the checkout that owns the common git dir", () => {
    // The native realpath, as projectOf uses: Windows runners report temp paths in 8.3 short form.
    const base = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-project-")));
    roots.push(base);
    const main = path.join(base, "main");
    fs.mkdirSync(path.join(main, "sub"), { recursive: true });
    git(main, "init", "-q");
    git(main, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init");
    git(main, "worktree", "add", "-q", path.join(base, "wt"));
    const plain = path.join(base, "plain");
    fs.mkdirSync(plain);
    expect(projectOf(path.join(base, "wt"))).toBe(main);
    expect(projectOf(path.join(main, "sub"))).toBe(main);
    expect(projectOf(main)).toBe(main);
    expect(projectOf(plain)).toBe(plain);                           // outside git: the directory
  });

  // review/astra F1 on #73: a checkout reached through another spelling (a symlink here; an 8.3
  // short name on Windows) is the same project as its linked worktree.
  it.skipIf(process.platform === "win32")("gives the main checkout and its worktree one identity through an alias", () => {
    const base = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-project-")));
    roots.push(base);
    const main = path.join(base, "main");
    fs.mkdirSync(main);
    git(main, "init", "-q");
    git(main, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init");
    git(main, "worktree", "add", "-q", path.join(base, "wt"));
    const alias = path.join(base, "alias");
    fs.symlinkSync(base, alias);
    expect(projectOf(path.join(alias, "main"))).toBe(projectOf(path.join(base, "wt")));
    expect(projectOf(path.join(alias, "wt"))).toBe(main);
  });

  it("reads the role from PI_FABRIC_ROLE, else SMARTY_ROLE without its stamp", () => {
    expect(participantRole({ SMARTY_ROLE: "project-agent@5358e96a418f" })).toBe("project-agent");
    expect(participantRole({ PI_FABRIC_ROLE: "worktree-agent", SMARTY_ROLE: "project-agent@x" })).toBe("worktree-agent");
    expect(participantRole({})).toBeUndefined();
  });

  // Native absolute paths: on Windows, path.resolve("/p/x") gains a drive letter.
  const P = (posix: string): string => path.resolve(posix);
  const root = (id: string, fields: { role?: string; project?: string; cwd?: string; startedAt?: number }) =>
    ({ id, startedAt: 1, ...fields });

  it("resolves the project agent of the caller's project, the newest when several are live", () => {
    const live = [
      root("session:org", { role: "org-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev/smarty-chief") }),
      root("session:dev-lead", { role: "project-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev"), startedAt: 5 }),
      root("session:old-dev-lead", { role: "project-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev"), startedAt: 2 }),
      root("session:knowledge", { role: "project-agent", project: P("/p/knowledge"), cwd: P("/p/knowledge") }),
      root("session:worktree", { role: "worktree-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev/worktrees/x") }),
    ];
    expect(resolveProjectAgent(live, P("/p/smarty-dev")).id).toBe("session:dev-lead");
    expect(resolveProjectAgent(live, P("/p/knowledge")).id).toBe("session:knowledge");
  });

  it("falls back to a root without a role whose cwd is the project checkout, and explains a miss", () => {
    const older = [root("session:dev-lead", { cwd: P("/p/smarty-dev") }), root("session:worktree", { role: "worktree-agent", project: P("/p/smarty-dev") })];
    expect(resolveProjectAgent(older, P("/p/smarty-dev")).id).toBe("session:dev-lead");
    expect(() => resolveProjectAgent([root("session:worktree", { role: "worktree-agent", project: P("/p/smarty-dev") })], P("/p/smarty-dev")))
      .toThrow(`No live project agent for ${P("/p/smarty-dev")}. Live roots in this project: session:worktree (worktree-agent).`);
  });
});
