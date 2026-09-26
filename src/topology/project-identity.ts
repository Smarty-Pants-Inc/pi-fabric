import fs from "node:fs";
import path from "node:path";

// smarty-dev#784: a worktree agent could not find its project agent. Every root was named
// "main", and nothing said which project it served or in what role. A root now publishes both.

/** This process's fleet role: PI_FABRIC_ROLE, else SMARTY_ROLE without its "@stamp" suffix. */
export const participantRole = (env: NodeJS.ProcessEnv = process.env): string | undefined => {
  const value = (env.PI_FABRIC_ROLE ?? env.SMARTY_ROLE ?? "").split("@")[0]!.trim();
  return value || undefined;
};

const projects = new Map<string, string>();

// One spelling per directory: Windows reports a temp or home path in 8.3 short form (RUNNER~1)
// from the cwd, but git records the long form in a worktree's gitdir.
const canonical = (dir: string): string => {
  try {
    return fs.realpathSync.native(dir);
  } catch {
    return path.resolve(dir);
  }
};

/**
 * The project a directory belongs to: the checkout that owns its git common directory, so every
 * linked worktree of one repository maps to the same main checkout. Outside git, the directory.
 */
export const projectOf = (cwd: string): string => {
  const known = projects.get(cwd);
  if (known) return known;
  let project = path.resolve(cwd);
  for (let dir = path.resolve(cwd); ; dir = path.dirname(dir)) {
    const dotGit = path.join(dir, ".git");
    let stat: fs.Stats | undefined;
    try {
      stat = fs.statSync(dotGit);
    } catch {
      stat = undefined;
    }
    if (stat?.isDirectory()) {
      project = dir;
      break;
    }
    if (stat?.isFile()) {
      // A linked worktree: ".git" names its git dir, whose commondir leads to the main .git.
      const gitdir = /^gitdir:\s*(.+)$/m.exec(fs.readFileSync(dotGit, "utf8"))?.[1]?.trim();
      if (gitdir) {
        const resolved = path.resolve(dir, gitdir);
        let common = resolved;
        try {
          common = path.resolve(resolved, fs.readFileSync(path.join(resolved, "commondir"), "utf8").trim());
        } catch {
          // A submodule or a bare layout without commondir: its own git dir.
        }
        project = path.basename(common) === ".git" ? path.dirname(common) : dir;
      } else {
        project = dir;
      }
      break;
    }
    if (path.dirname(dir) === dir) break;
  }
  project = canonical(project);
  projects.set(cwd, project);
  return project;
};

/**
 * This root's project: the project of PI_FABRIC_PROJECT when that is set, else of its cwd. A lead
 * whose cwd is a worktree of another repository names its own project, or it would count as a
 * project agent of that repository (smarty-dev#977).
 */
export const participantProject = (cwd: string, env: NodeJS.ProcessEnv = process.env): string => {
  const explicit = env.PI_FABRIC_PROJECT?.trim();
  return projectOf(explicit ? path.resolve(explicit) : cwd);
};

interface ProjectRoot {
  id: string;
  role?: string;
  project?: string;
  cwd?: string;
  startedAt: number;
}

/**
 * Where a resident host delivers its actors' messages (smarty-dev#878): its root while that root
 * is live, else the project's live project agent, else still its root, where the record waits.
 */
export const deliveryRoot = (rootId: string, liveRoots: readonly ProjectRoot[], project: string): string => {
  if (liveRoots.some((root) => root.id === rootId)) return rootId;
  try {
    return resolveProjectAgent(liveRoots, project).id;
  } catch {
    return rootId;
  }
};

/**
 * The live project agent for a project: the root with role "project-agent" and that project, the
 * most recently started when several match. A root from a runtime that publishes neither field
 * counts when its cwd is the project checkout (smarty-dev#784).
 */
export const resolveProjectAgent = <T extends ProjectRoot>(roots: readonly T[], project: string): T => {
  const tagged = roots.filter((root) => root.role === "project-agent" && root.project === project);
  const untagged = roots.filter((root) =>
    root.role === undefined && root.project === undefined && root.cwd !== undefined && canonical(root.cwd) === project);
  const candidates = tagged.length > 0 ? tagged : untagged;
  if (candidates.length === 0) {
    const inProject = roots
      .filter((root) => (root.project ?? (root.cwd ? canonical(root.cwd) : undefined)) === project)
      .map((root) => `${root.id} (${root.role ?? "no role"})`);
    throw new Error(
      `No live project agent for ${project}. ` +
        (inProject.length > 0 ? `Live roots in this project: ${inProject.join(", ")}.` : "No live root is in this project."),
    );
  }
  return [...candidates].sort((a, b) => b.startedAt - a.startedAt)[0]!;
};
