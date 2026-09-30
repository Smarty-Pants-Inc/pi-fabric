import { execFileSync } from "node:child_process";
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

/** One repository identity across HTTPS, ssh:// and Git's user@host:path spelling. */
export const normalizeOrigin = (origin: string): string | undefined => {
  let value = origin.trim().replace(/^git\+/, "");
  if (!value) return undefined;
  if (/^[^/\s:]+\.[^/\s:]+\/.+$/.test(value)) value = `https://${value}`;
  if (!value.includes("://")) {
    const scp = /^(?:[^/@:]+@)?([^/:]+):(.+)$/.exec(value);
    if (!scp) return undefined;
    value = `ssh://${scp[1]}/${scp[2]}`;
  }
  try {
    const url = new URL(value);
    if (!["https:", "http:", "ssh:", "git:"].includes(url.protocol) || !url.hostname) return undefined;
    const host = url.hostname.toLowerCase();
    const defaultPort = url.protocol === "ssh:" ? "22" : url.protocol === "git:" ? "9418" : "";
    const port = url.port && url.port !== defaultPort ? `:${url.port}` : "";
    let repo = url.pathname.replace(/^\/+|\/+$/g, "").replace(/\.git$/i, "");
    if (!repo) return undefined;
    if (host === "github.com") repo = repo.toLowerCase();
    return `${host}${port}/${repo}`;
  } catch {
    return undefined;
  }
};

const repositories = new Map<string, string | undefined>();
/** Memoized first-use git config lookup; never runs git during import or registration. */
export const repositoryOf = (cwd: string): string | undefined => {
  const project = projectOf(cwd);
  if (repositories.has(project)) return repositories.get(project);
  let repository: string | undefined;
  try {
    repository = normalizeOrigin(execFileSync("git", ["-C", project, "config", "--get", "remote.origin.url"],
      { encoding: "utf8", timeout: 2_000, stdio: ["ignore", "pipe", "ignore"] }));
  } catch {
    // Non-git directories and repositories without an origin retain their native path identity.
  }
  repositories.set(project, repository);
  return repository;
};

/** Launch metadata written by smarty-lane-move, or supplied explicitly by its launcher. */
export const recordedProjectLead = (cwd: string, env: NodeJS.ProcessEnv = process.env): string | undefined => {
  const explicit = env.SMARTY_LEAD_SESSION?.trim();
  if (explicit) return explicit;
  for (let dir = path.resolve(cwd); ; dir = path.dirname(dir)) {
    try {
      const id = fs.readFileSync(path.join(dir, ".local", "lead"), "utf8").trim();
      if (id) return id;
    } catch {
      // Most sessions have no launch lead marker.
    }
    if (fs.existsSync(path.join(dir, ".git")) || path.dirname(dir) === dir) return undefined;
  }
};

interface ProjectRoot {
  id: string;
  role?: string;
  project?: string;
  repository?: string;
  interactive?: boolean;
  capabilities?: readonly string[];
  cwd?: string;
  startedAt: number;
  /** Set on a root mirrored from another host's mesh (smarty-dev#2045). */
  remoteHost?: string;
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

export class FabricProjectAgentUnresolvedError extends Error {
  override readonly name = "FabricProjectAgentUnresolvedError";
  readonly code = "FABRIC_PROJECT_AGENT_UNRESOLVED";
}

export class FabricProjectAgentAmbiguousError extends Error {
  override readonly name = "FabricProjectAgentAmbiguousError";
  readonly code = "FABRIC_PROJECT_AGENT_AMBIGUOUS";
}

/**
 * Repository identity survives a lane move. The launch-recorded id is authoritative only
 * within that repository; it also permits that exact mirror, not arbitrary remote lead claims
 * (smarty-dev#2045). Legacy native records retain path matching. Never choose by recency.
 */
export const resolveProjectAgent = <T extends ProjectRoot>(
  allRoots: readonly T[],
  project: string,
  options: { repository?: string; leadId?: string } = {},
): T => {
  const repository = options.repository ? normalizeOrigin(options.repository) : undefined;
  const eligible = (root: T): boolean => root.interactive !== false &&
    (!root.capabilities || (root.capabilities.includes("steer") && root.capabilities.includes("followUp")));
  const sameProject = (root: T): boolean => {
    if (repository && root.repository) return normalizeOrigin(root.repository) === repository;
    // Remote paths are neither locally meaningful nor authority to name this repository.
    return root.remoteHost === undefined &&
      (root.project ?? (root.cwd ? canonical(root.cwd) : undefined)) === project;
  };
  const roots = allRoots.filter((root) => eligible(root) && sameProject(root));
  if (options.leadId) {
    const recorded = roots.find((root) => root.id === options.leadId);
    if (recorded) return recorded;
    throw new FabricProjectAgentUnresolvedError(
      `No live project agent for ${project}: recorded launch lead ${options.leadId} is unavailable, non-interactive, or belongs to another repository.`,
    );
  }
  const native = roots.filter((root) => root.remoteHost === undefined);
  const tagged = native.filter((root) => root.role === "project-agent");
  const untagged = native.filter((root) =>
    root.role === undefined && root.project === undefined && root.cwd !== undefined && canonical(root.cwd) === project);
  const candidates = tagged.length > 0 ? tagged : untagged;
  if (candidates.length === 0) {
    const inProject = native.map((root) => `${root.id} (${root.role ?? "no role"})`);
    throw new FabricProjectAgentUnresolvedError(
      `No live project agent for ${project}. ` +
        (inProject.length > 0 ? `Live roots in this project: ${inProject.join(", ")}.` : "No live root is in this project."),
    );
  }
  if (candidates.length > 1) {
    throw new FabricProjectAgentAmbiguousError(
      `Ambiguous Fabric project agent for ${project}: ${candidates.map((root) => root.id).sort().join(", ")}. Record the launch lead in SMARTY_LEAD_SESSION or .local/lead.`,
    );
  }
  return candidates[0]!;
};
