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

/**
 * One host[:port]/owner/name identity across HTTPS, ssh:// and user@host:path.
 * Canonical identities (including dotless hosts) are already suffix-normalized: never strip
 * another .git from their repository name. Explicit ports and every path component survive,
 * except the known GitHub SSH-over-443 alias. Only GitHub and its aliases fold path case.
 * Parse without URL, which silently removes dot segments, default ports and backslashes.
 * A numeric host:port/path spelling is canonical; scp paths with numeric owners use user@host:.
 */
export const normalizeOrigin = (origin: string): string | undefined => {
  const value = origin.trim().replace(/^git\+(?=[a-z]+:\/\/)/i, "");
  if (!value || /[\s\\?#]/.test(value)) return undefined;
  const hostPattern = /^(\[[0-9a-f:.]+\]|[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?)(?::([0-9]+))?$/i;
  let authority: string;
  let repo: string;
  let canonicalIdentity = false;
  const url = /^([a-z]+):\/\/([^/]+)\/(.+)$/i.exec(value);
  if (url) {
    if (!["https", "http", "ssh", "git"].includes(url[1]!.toLowerCase())) return undefined;
    // Authentication is not part of repository identity. Do not remove anything from the path.
    authority = url[2]!;
    if (authority.includes("@")) {
      if (authority.indexOf("@") !== authority.lastIndexOf("@")) return undefined;
      authority = authority.slice(authority.indexOf("@") + 1);
    }
    repo = url[3]!;
  } else {
    const slash = value.indexOf("/");
    const prefix = value.slice(0, slash);
    if (slash > 0 && hostPattern.test(prefix)) {
      authority = prefix;
      repo = value.slice(slash + 1);
      canonicalIdentity = true;
    } else {
      const scp = /^(?:[^/@:\s]+@)?(\[[0-9a-f:.]+\]|[^/:@\s]+):(.+)$/i.exec(value);
      if (!scp) return undefined;
      authority = scp[1]!;
      repo = scp[2]!;
    }
  }
  const host = hostPattern.exec(authority);
  if (!host || (host[2] !== undefined && (Number(host[2]) < 1 || Number(host[2]) > 65535))) return undefined;
  const segments = repo.split("/");
  if (segments.some((segment) => !segment || segment === "." || segment === "..")) return undefined;
  if (!canonicalIdentity) repo = repo.replace(/\.git$/i, "");
  if (repo.split("/").some((segment) => !segment || segment === "." || segment === "..")) return undefined;
  const hostname = host[1]!.toLowerCase();
  const github = hostname === "github.com" || hostname === "www.github.com" || hostname === "ssh.github.com";
  // These are known equivalent endpoints; never discard a generic host's explicit port.
  const identityHost = hostname === "www.github.com" ? `github.com${host[2] ? `:${host[2]}` : ""}`
    : hostname === "ssh.github.com" && host[2] === "443" ? "github.com" : authority.toLowerCase();
  return `${identityHost}/${github ? repo.toLowerCase() : repo}`;
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

export class FabricProjectLeadInvalidError extends Error {
  override readonly name = "FabricProjectLeadInvalidError";
  readonly code = "FABRIC_PROJECT_LEAD_INVALID";
  constructor() {
    // Never include file contents, an environment value, or an underlying filesystem error.
    super("Invalid project lead launch metadata: expected a regular, bounded marker containing session:<UUID>.");
  }
}

const MAX_LEAD_MARKER_BYTES = 128;
const LEAD_SESSION_ID = /^session:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const validatedLead = (value: string): string => {
  const id = value.trim();
  if (value.length > MAX_LEAD_MARKER_BYTES || !LEAD_SESSION_ID.test(id)) throw new FabricProjectLeadInvalidError();
  return id;
};

/** Launch metadata written by smarty-lane-move, or supplied explicitly by its launcher. */
export const recordedProjectLead = (cwd: string, env: NodeJS.ProcessEnv = process.env): string | undefined => {
  const explicit = env.SMARTY_LEAD_SESSION?.trim();
  if (explicit) return validatedLead(explicit);
  for (let dir = path.resolve(cwd); ; dir = path.dirname(dir)) {
    const local = path.join(dir, ".local");
    const marker = path.join(local, "lead");
    let fd: number | undefined;
    try {
      // Refuse both leaf and .local symlinks. O_NOFOLLOW also closes the leaf check/open race
      // on platforms that support it; fstat verifies the opened file, not just the earlier path.
      const directory = fs.lstatSync(local);
      if (!directory.isDirectory() || directory.isSymbolicLink()) throw new FabricProjectLeadInvalidError();
      const before = fs.lstatSync(marker);
      if (!before.isFile() || before.isSymbolicLink()) throw new FabricProjectLeadInvalidError();
      fd = fs.openSync(marker, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.size > MAX_LEAD_MARKER_BYTES || opened.dev !== before.dev || opened.ino !== before.ino) {
        throw new FabricProjectLeadInvalidError();
      }
      const afterDirectory = fs.lstatSync(local);
      if (!afterDirectory.isDirectory() || afterDirectory.dev !== directory.dev || afterDirectory.ino !== directory.ino) {
        throw new FabricProjectLeadInvalidError();
      }
      // One extra byte detects growth after fstat; never read an unbounded file or special device.
      const bytes = Buffer.alloc(MAX_LEAD_MARKER_BYTES + 1);
      const count = fs.readSync(fd, bytes, 0, bytes.length, 0);
      if (count > MAX_LEAD_MARKER_BYTES) throw new FabricProjectLeadInvalidError();
      return validatedLead(bytes.subarray(0, count).toString("utf8"));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw new FabricProjectLeadInvalidError();
      // Most sessions have no launch lead marker; an invalid one must not fall back to election.
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
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
 * A resident actor's messages stay at its root until its lineage is provably dead. A lease
 * lapse is not death (smarty-dev#3662). Only an exact launch-bound integrator may inherit;
 * without an available binding the record still waits in the root's mailbox. Never elect.
 */
export const deliveryRoot = (
  rootId: string,
  liveRoots: readonly ProjectRoot[],
  project: string,
  options: {
    /** The same lineage test used by orphan adoption, not a lease-filtered root listing. */
    lineageAlive?: (rootId: string) => boolean;
    /** Read launch metadata only after confirmed death, never during idle registration. */
    boundIntegrator?: () => { repository?: string; leadId?: string };
  } = {},
): string => {
  if (liveRoots.some((root) => root.id === rootId)) return rootId;
  try {
    if (options.lineageAlive?.(rootId) !== false) return rootId;
    const binding = options.boundIntegrator?.();
    if (!binding?.leadId) return rootId;
    return resolveProjectAgent(liveRoots, project, binding).id;
  } catch {
    // Unknown liveness or invalid/unavailable metadata must never elect another project agent.
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
  if (options.repository !== undefined && !repository) {
    throw new FabricProjectAgentUnresolvedError(`No live project agent for ${project}: invalid repository identity.`);
  }
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
