import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { deliveryRoot, normalizeOrigin, participantProject, participantRole, projectOf, recordedProjectLead, repositoryOf, resolveProjectAgent } from "../src/topology/project-identity.js";

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

  // smarty-dev#977: a lead whose cwd is a worktree of another repository names its own project.
  it("takes a root's project from PI_FABRIC_PROJECT when set, else from its cwd", () => {
    const base = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-project-")));
    roots.push(base);
    const repo = (name: string) => {
      const dir = path.join(base, name);
      fs.mkdirSync(path.join(dir, "sub"), { recursive: true });
      git(dir, "init", "-q");
      git(dir, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init");
      return dir;
    };
    const host = repo("smarty-dev");                                 // the repository the lead's cwd is in
    const own = repo("pi-fabric");                                   // the project it leads
    git(host, "worktree", "add", "-q", path.join(base, "lead-home"));
    const cwd = path.join(base, "lead-home");
    expect(participantProject(cwd, {})).toBe(host);                  // without it: the cwd's repository
    expect(participantProject(cwd, { PI_FABRIC_PROJECT: own })).toBe(own);
    expect(participantProject(cwd, { PI_FABRIC_PROJECT: path.join(own, "sub") })).toBe(own);   // any path in it
    expect(participantProject(cwd, { PI_FABRIC_PROJECT: "  " })).toBe(host);                 // blank: unset
  });

  it("reads the role from PI_FABRIC_ROLE, else SMARTY_ROLE without its stamp", () => {
    expect(participantRole({ SMARTY_ROLE: "project-agent@5358e96a418f" })).toBe("project-agent");
    expect(participantRole({ PI_FABRIC_ROLE: "worktree-agent", SMARTY_ROLE: "project-agent@x" })).toBe("worktree-agent");
    expect(participantRole({})).toBeUndefined();
  });

  it("normalizes HTTPS, SSH and scp origins without user credentials or .git suffixes", () => {
    const expected = "github.com/smarty-pants-inc/pi-fabric";
    for (const origin of ["https://GitHub.com/Smarty-Pants-Inc/pi-fabric.git", "git@github.com:Smarty-Pants-Inc/pi-fabric.git",
      "ssh://git@github.com/Smarty-Pants-Inc/pi-fabric.git", `git+https://${expected}`, expected]) {
      expect(normalizeOrigin(origin)).toBe(expected);
    }
    expect(normalizeOrigin("https://example.org/Team/Repo.git")).toBe("example.org/team/repo");
    expect(normalizeOrigin("ssh://git@example.org:2222/Team/Repo.git")).toBe("example.org:2222/team/repo");
    expect(normalizeOrigin("https://example.org:22/Team/Repo.git")).toBe("example.org:22/team/repo");
    expect(normalizeOrigin("")).toBeUndefined();
  });

  it("gives independent host checkouts and a worktree the same origin identity and reads the lane launch marker", () => {
    const base = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-origin-")));
    roots.push(base);
    const lead = path.join(base, "lead");
    const lane = path.join(base, "moved-lane");
    for (const dir of [lead, lane]) { fs.mkdirSync(dir); git(dir, "init", "-q"); }
    git(lead, "remote", "add", "origin", "git@github.com:Smarty-Pants-Inc/pi-fabric.git");
    git(lane, "remote", "add", "origin", "https://github.com/Smarty-Pants-Inc/pi-fabric.git");
    git(lead, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init");
    git(lead, "worktree", "add", "-q", path.join(base, "wt"));
    expect(repositoryOf(lane)).toBe(repositoryOf(lead));
    expect(repositoryOf(path.join(base, "wt"))).toBe(repositoryOf(lead));
    fs.mkdirSync(path.join(lane, ".local"));
    fs.mkdirSync(path.join(lane, "sub"));
    fs.writeFileSync(path.join(lane, ".local", "lead"), "session:11111111-1111-4111-8111-111111111111\n");
    expect(recordedProjectLead(path.join(lane, "sub"), {})).toBe("session:11111111-1111-4111-8111-111111111111");
    expect(recordedProjectLead(lane, { SMARTY_LEAD_SESSION: "session:22222222-2222-4222-8222-222222222222" }))
      .toBe("session:22222222-2222-4222-8222-222222222222");
  });

  const markerLane = () => {
    const lane = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-marker-"));
    roots.push(lane);
    fs.mkdirSync(path.join(lane, ".local"));
    return lane;
  };

  it.each(["ghp_sensitive_marker_secret", "session:not-a-uuid"])("#201 rejects invalid sensitive launch-marker contents without echoing them: %s", (contents) => {
    const lane = markerLane();
    fs.writeFileSync(path.join(lane, ".local", "lead"), contents);
    expect(() => recordedProjectLead(lane, {})).toThrow(expect.objectContaining({
      name: "FabricProjectLeadInvalidError", code: "FABRIC_PROJECT_LEAD_INVALID",
      message: "Invalid project lead launch metadata: expected a regular, bounded marker containing session:<UUID>.",
    }));
  });

  it("#201 bounds marker reads even when a large file begins with a valid id", () => {
    const lane = markerLane();
    fs.writeFileSync(path.join(lane, ".local", "lead"), `session:11111111-1111-4111-8111-111111111111${" ".repeat(4096)}`);
    const read = vi.spyOn(fs, "readSync");
    try {
      expect(() => recordedProjectLead(lane, {})).toThrow(expect.objectContaining({ code: "FABRIC_PROJECT_LEAD_INVALID" }));
      expect(read).not.toHaveBeenCalled(); // reject by fstat before reading an oversized file
    } finally {
      read.mockRestore();
    }
  });

  it.each(["ghp_sensitive_env_secret", "session:fake"])("#201 validates explicit launch metadata without echoing it: %s", (value) => {
    expect(() => recordedProjectLead(os.tmpdir(), { SMARTY_LEAD_SESSION: value }))
      .toThrow(expect.objectContaining({ code: "FABRIC_PROJECT_LEAD_INVALID" }));
  });

  it.skipIf(process.platform === "win32")("#201 refuses marker and .local symlinks without opening their targets", () => {
    const lane = markerLane();
    const secret = path.join(lane, "host-secret");
    fs.writeFileSync(secret, "ghp_sensitive_symlink_secret");
    fs.symlinkSync(secret, path.join(lane, ".local", "lead"));
    const open = vi.spyOn(fs, "openSync");
    try {
      expect(() => recordedProjectLead(lane, {})).toThrow(expect.objectContaining({ code: "FABRIC_PROJECT_LEAD_INVALID" }));
      expect(open).not.toHaveBeenCalled();
      fs.rmSync(path.join(lane, ".local"), { recursive: true });
      fs.mkdirSync(path.join(lane, "outside"));
      fs.writeFileSync(path.join(lane, "outside", "lead"), "session:11111111-1111-4111-8111-111111111111");
      fs.symlinkSync(path.join(lane, "outside"), path.join(lane, ".local"));
      expect(() => recordedProjectLead(lane, {})).toThrow(expect.objectContaining({ code: "FABRIC_PROJECT_LEAD_INVALID" }));
      expect(open).not.toHaveBeenCalled();
    } finally {
      open.mockRestore();
    }
  });

  it.each([
    ["ssh://git@code.example.net:2222/team/app.git", "code.example.net:2222/team/app"],
    ["ssh://git@forge/team/app.git", "forge/team/app"],
    ["ssh://git@forge:2222/team/app.git", "forge:2222/team/app"],
    ["https://CODE.example.net/Team/App.git", "code.example.net/team/app"],
    ["https://code.example.net/team/app.git.git", "code.example.net/team/app.git"],
  ])("#201 origin normalization is idempotent for %s", (origin, expected) => {
    const identity = normalizeOrigin(origin);
    expect(identity).toBe(expected);
    expect(normalizeOrigin(identity!)).toBe(identity);
  });

  it.each([
    ["ssh://git@code.example.net:2222/team/app.git", "https://code.example.net/2222/team/app.git"],
    ["ssh://git@code.example.net:2222/team/app.git", "git@code.example.net:2222/team/app.git"],
    ["ssh://git@code.example.net:22/team/app.git", "https://code.example.net/team/app.git"],
    ["https://code.example.net:443/team/app.git", "https://code.example.net/team/app.git"],
    ["https://code.example.net/team/app.git", "https://other.example.net/team/app.git"],
    ["https://code.example.net/team/app.git", "https://code.example.net/other/app.git"],
    ["https://code.example.net/team/app.git", "https://code.example.net/team/other.git"],
    ["https://code.example.net/team/app.git", "https://code.example.net/team/app.git.git"],
  ])("#201 rejects cross-repository recorded leads: %s versus %s", (origin, foreignOrigin) => {
    const repository = normalizeOrigin(origin)!;
    const foreignRepository = normalizeOrigin(foreignOrigin)!;
    expect(repository).not.toBe(foreignRepository);
    const lead = { id: "session:lead", startedAt: 1, repository: foreignRepository, remoteHost: "forge" };
    expect(() => resolveProjectAgent([lead], path.resolve("/lane"), { repository, leadId: lead.id }))
      .toThrow(expect.objectContaining({ code: "FABRIC_PROJECT_AGENT_UNRESOLVED" }));
  });

  it.each([
    "https://code.example.net/team/../app.git", "https://code.example.net/team/./app.git",
    "https://code.example.net//team/app.git", "https://code.example.net/team/app.git/",
    "https://code.example.net/team/app.git?other", "https://code.example.net/team/app.git#other",
    "https://code.example.net/team\\app.git",
  ])("#201 rejects syntax whose lossy URL normalization could alias a repository: %s", (origin) => {
    expect(normalizeOrigin(origin)).toBeUndefined();
    const project = path.resolve("/lane");
    const lead = { id: "session:lead", startedAt: 1, project, role: "project-agent" };
    expect(() => resolveProjectAgent([lead], project, { repository: origin, leadId: lead.id }))
      .toThrow(expect.objectContaining({ code: "FABRIC_PROJECT_AGENT_UNRESOLVED" }));
  });

  it("#201 resolves a dotless-host mirror using the already published repository identity", () => {
    const repository = normalizeOrigin("ssh://git@forge/team/app.git")!;
    const lead = { id: "session:lead", startedAt: 1, repository, remoteHost: "forge" };
    expect(resolveProjectAgent([lead], path.resolve("/lane"), { repository, leadId: lead.id })).toBe(lead);
  });

  it("never elects a non-interactive auditor or a recorded lead from a different repository", () => {
    const project = path.resolve("/p/repo");
    const auditor = { id: "session:audit", startedAt: 2, role: "project-agent", project, interactive: false };
    expect(() => resolveProjectAgent([auditor], project, { leadId: auditor.id }))
      .toThrow(expect.objectContaining({ name: "FabricProjectAgentUnresolvedError" }));
    const foreign = { ...auditor, interactive: true, repository: "github.com/other/repo" };
    expect(() => resolveProjectAgent([foreign], project, { repository: "github.com/our/repo", leadId: foreign.id }))
      .toThrow(expect.objectContaining({ name: "FabricProjectAgentUnresolvedError" }));
  });

  // Native absolute paths: on Windows, path.resolve("/p/x") gains a drive letter.
  const P = (posix: string): string => path.resolve(posix);
  const root = (id: string, fields: { role?: string; project?: string; cwd?: string; startedAt?: number }) =>
    ({ id, startedAt: 1, ...fields });

  it("resolves the caller's project agent with the recorded launch id when several are live", () => {
    const live = [
      root("session:org", { role: "org-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev/smarty-chief") }),
      root("session:dev-lead", { role: "project-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev"), startedAt: 5 }),
      root("session:old-dev-lead", { role: "project-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev"), startedAt: 2 }),
      root("session:knowledge", { role: "project-agent", project: P("/p/knowledge"), cwd: P("/p/knowledge") }),
      root("session:worktree", { role: "worktree-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev/worktrees/x") }),
    ];
    expect(resolveProjectAgent(live, P("/p/smarty-dev"), { leadId: "session:dev-lead" }).id).toBe("session:dev-lead");
    expect(resolveProjectAgent(live, P("/p/knowledge")).id).toBe("session:knowledge");
  });

  it("resolves a moved lane by repository identity, not the lead's local path", () => {
    const repository = "github.com/smarty-pants-inc/pi-fabric";
    const lead = { ...root("session:lead", { role: "project-agent", project: P("/lead/checkout") }), repository };
    expect(resolveProjectAgent([lead], P("/moved/lane"), { repository }).id).toBe(lead.id);
  });

  it("uses the recorded launch lead id to resolve ambiguous same-origin roots, including its bridge mirror", () => {
    const repository = "github.com/smarty-pants-inc/pi-fabric";
    const lead = { ...root("session:lead", { role: "project-agent", project: P("/lead/checkout") }), repository, remoteHost: "forge" };
    const other = { ...root("session:other", { role: "project-agent", project: P("/other/checkout"), startedAt: 99 }), repository };
    expect(resolveProjectAgent([other, lead], P("/moved/lane"), { repository, leadId: lead.id }).id).toBe(lead.id);
  });

  it("fails by name on ambiguous project agents instead of selecting the newest", () => {
    const all = [root("session:a", { role: "project-agent", project: P("/p/repo") }),
      root("session:b", { role: "project-agent", project: P("/p/repo"), startedAt: 2 })];
    expect(() => resolveProjectAgent(all, P("/p/repo"))).toThrow(expect.objectContaining({ name: "FabricProjectAgentAmbiguousError" }));
  });

  // smarty-dev#878: a durable actor's messages follow its project's agent once its root is gone.
  it("delivers to the root while it is live, else to the project's live project agent, else still to the root", () => {
    const next = root("session:new-dev-lead", { role: "project-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev"), startedAt: 9 });
    const worktree = root("session:worktree", { role: "worktree-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev/worktrees/x") });
    const other = root("session:knowledge", { role: "project-agent", project: P("/p/knowledge"), cwd: P("/p/knowledge") });
    const old = root("session:dev-lead", { role: "project-agent", project: P("/p/smarty-dev"), cwd: P("/p/smarty-dev"), startedAt: 1 });
    // While the root lives, even a newer project agent does not take its messages.
    expect(deliveryRoot("session:dev-lead", [old, next, worktree], P("/p/smarty-dev"))).toBe("session:dev-lead");
    expect(deliveryRoot("session:dev-lead", [next, worktree, other], P("/p/smarty-dev"))).toBe("session:new-dev-lead");
    // Counterexamples: never a worktree agent of the project, nor another project's agent.
    expect(deliveryRoot("session:dev-lead", [worktree, other], P("/p/smarty-dev"))).toBe("session:dev-lead");
  });

  it("falls back to a root without a role whose cwd is the project checkout, and explains a miss", () => {
    const older = [root("session:dev-lead", { cwd: P("/p/smarty-dev") }), root("session:worktree", { role: "worktree-agent", project: P("/p/smarty-dev") })];
    expect(resolveProjectAgent(older, P("/p/smarty-dev")).id).toBe("session:dev-lead");
    expect(() => resolveProjectAgent([root("session:worktree", { role: "worktree-agent", project: P("/p/smarty-dev") })], P("/p/smarty-dev")))
      .toThrow(`No live project agent for ${P("/p/smarty-dev")}. Live roots in this project: session:worktree (worktree-agent).`);
  });
});
