import { execFile } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { setJevFabricStatus } from "./status.js";

/** What a candidate binary reports; 0.4 binaries predate `capabilities` and are mapped to protocol 1. */
export interface JevFabricCapabilities {
  version: string;
  protocol: number;
  store?: number;
  platform?: string;
  features: readonly string[];
}

export type JevFabricSource = "config" | "user" | "bundled";

export interface JevFabricResolution {
  path: string;
  source: JevFabricSource;
  capabilities: JevFabricCapabilities;
  /** Candidates tried first and why they were passed over. */
  skipped: Array<{ path: string; source: JevFabricSource; reason: string }>;
}

/** What each Fabric feature needs from a binary. */
export const JEV_FABRIC_REQUIREMENTS = {
  durable: { protocol: 1, features: ["follow", "list", "label"] },
  jev: { protocol: 2, features: ["serve-concurrent", "serve-24h", "jev-request-credential"] },
  sessions: { protocol: 2, features: ["sessions", "serve-concurrent", "read", "cwd", "serve-24h", "durable-input"] },
} as const;
export type JevFabricRequirement = keyof typeof JEV_FABRIC_REQUIREMENTS;

const PROBE_TIMEOUT_MS = 10_000;
// Features 0.4.0 shipped before the capabilities verb existed.
const LEGACY_FEATURES = ["follow", "list", "label", "start-24h"];

const run = (file: string, args: string[]): Promise<string> =>
  new Promise((resolve, reject) => {
    execFile(file, ["--", ...args], { timeout: PROBE_TIMEOUT_MS, maxBuffer: 64 * 1024, windowsHide: true }, (error, stdout) => {
      if (error) reject(error); else resolve(stdout);
    });
  });

const versionOf = (text: string): number[] | undefined => {
  const match = /(\d+)\.(\d+)\.(\d+)/.exec(text);
  return match ? match.slice(1).map(Number) : undefined;
};

/** Asks a binary what it supports, without touching any store. */
export async function probeJevFabric(file: string): Promise<JevFabricCapabilities> {
  try {
    const value = JSON.parse(await run(file, ["capabilities"])) as Partial<JevFabricCapabilities>;
    if (typeof value.protocol !== "number" || !Array.isArray(value.features) || typeof value.version !== "string") throw new Error("unrecognized capabilities");
    return { version: value.version, protocol: value.protocol, features: value.features.filter(f => typeof f === "string"),
      ...(typeof value.store === "number" ? { store: value.store } : {}), ...(typeof value.platform === "string" ? { platform: value.platform } : {}) };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") throw error;
    const text = (await run(file, ["--version"])).trim();
    const version = versionOf(text);
    const legacy = version !== undefined && (version[0]! > 0 || version[1]! >= 4);
    return { version: text, protocol: legacy ? 1 : 0, features: legacy ? LEGACY_FEATURES : [] };
  }
}

const unmet = (capabilities: JevFabricCapabilities, requirement: JevFabricRequirement): string | undefined => {
  const need = JEV_FABRIC_REQUIREMENTS[requirement];
  if (capabilities.protocol < need.protocol) return `protocol ${capabilities.protocol} < ${need.protocol} (${capabilities.version})`;
  const missing = need.features.filter(feature => !capabilities.features.includes(feature));
  return missing.length ? `missing ${missing.join(", ")} (${capabilities.version})` : undefined;
};

const inside = (child: string, parent: string): boolean => {
  const relative = path.relative(parent, child);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
};
const real = (file: string): string => { try { return fs.realpathSync.native(file); } catch { return file; } };
const executable = (file: string): boolean => {
  try { fs.accessSync(file, fs.constants.X_OK); return fs.statSync(file).isFile(); } catch { return false; }
};

// A linked worktree has a .git file, not a directory. Do not invoke repository
// code (or git) to discover this boundary. Include outer roots of nested projects.
const enclosingRoot = (directory: string, project = false): string | undefined => {
  const markers = project ? [".git", "package.json", "pyproject.toml", "Cargo.toml", "go.mod"] : [".git"];
  let root: string | undefined;
  for (let current = path.resolve(directory); ; current = path.dirname(current)) {
    if (markers.some(marker => {
      try { fs.lstatSync(path.join(current, marker)); return true; } catch { return false; }
    })) root = current;
    if (path.dirname(current) === current) return root;
  }
};

const workspaceRoots = (cwd: string): string[] =>
  [path.resolve(cwd), real(path.resolve(cwd))].map(directory => enclosingRoot(directory, true) ?? directory);

/** Canonical installation identity, established BEFORE capabilities or credentials. */
const installedBinary = (file: string, roots: string[], workspaces: string[]): { path?: string; reason?: string } => {
  try {
    if (!path.isAbsolute(file)) return { reason: "relative PATH entry" };
    const resolved = fs.realpathSync.native(file);
    if (workspaces.some(workspace => inside(file, workspace) || inside(resolved, workspace))) return { reason: "inside the workspace" };
    if (enclosingRoot(path.dirname(file)) || enclosingRoot(path.dirname(resolved))) return { reason: "inside a repository or worktree" };
    // Roots come only from host installation locations, never cwd, PATH or a
    // repository manifest. A redirected release root does not become an allowlist.
    const root = roots.find(root => inside(resolved, root) && fs.realpathSync.native(root) === root);
    if (!root) return { reason: "outside trusted installation roots; use an explicit trusted binary setting for custom installs" };
    const uid = process.getuid?.();
    for (let current = resolved; ; current = path.dirname(current)) {
      const stat = fs.lstatSync(current);
      if (stat.isSymbolicLink() || (current === resolved ? !stat.isFile() : !stat.isDirectory()) ||
          (stat.mode & 0o022) !== 0 || (uid !== undefined && stat.uid !== uid && stat.uid !== 0)) {
        return { reason: "installation is not host-owned or is group/world-writable" };
      }
      if (current === root) break;
    }
    return { path: resolved };
  } catch {
    return { reason: "cannot establish installed executable provenance" };
  }
};

/**
 * PATH is only a locator, not provenance. Automatic candidates must resolve to
 * host-owned installer/release locations outside EVERY repository/worktree.
 * Custom installation prefixes require the existing trusted explicit setting.
 */
export function userCandidates(cwd: string, env: NodeJS.ProcessEnv = process.env, home = os.homedir()): { found: string[]; skipped: Array<{ path: string; reason: string }> } {
  const workspaces = workspaceRoots(cwd);
  const hostHome = real(path.resolve(home));
  const roots = [path.join(hostHome, ".local", "share", "jev-fabric"), path.join(hostHome, ".local", "bin"), "/usr/local/bin", "/usr/bin"];
  const directories = (env.PATH ?? "").split(path.delimiter).filter(Boolean);
  directories.push(path.join(hostHome, ".local", "bin"));
  const found: string[] = [];
  const skipped: Array<{ path: string; reason: string }> = [];
  const seen = new Set<string>();
  for (const directory of directories) {
    const file = path.join(directory, "jev-fabric");
    if (!executable(file)) continue;
    const identity = installedBinary(file, roots, workspaces);
    if (!identity.path) { skipped.push({ path: file, reason: identity.reason! }); continue; }
    if (seen.has(identity.path)) continue;
    seen.add(identity.path);
    found.push(identity.path);
  }
  return { found, skipped };
}

/**
 * The optional `jev-fabric` npm package, copied once to a versioned path under
 * the agent directory: running workers re-execute their own path, so it must
 * not change when an upgrade replaces node_modules.
 */
type BundledPackage = { version: string; binaryPath(): string | undefined };
const loadBundled = (): BundledPackage => createRequire(import.meta.url)("jev-fabric") as BundledPackage;

export function stageBundledJevFabric(agentDir: string, load: () => BundledPackage = loadBundled): string | undefined {
  let located: BundledPackage;
  try {
    located = load();
  } catch {
    return undefined;
  }
  const source = located.binaryPath();
  if (!source || !/^[0-9A-Za-z.+-]+$/.test(located.version)) return undefined;
  const target = path.join(agentDir, "fabric", "jev-fabric", located.version, "jev-fabric");
  try {
    if (fs.statSync(target).size === fs.statSync(source).size) return target;
  } catch { /* Not staged yet. */ }
  fs.mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
  const temporary = `${target}.${process.pid}.tmp`;
  fs.copyFileSync(source, temporary);
  fs.chmodSync(temporary, 0o755);
  fs.renameSync(temporary, target);
  return target;
}

const describe = (resolution: JevFabricResolution): string =>
  `${resolution.capabilities.version.replace(/-native.*/, "")} · ${resolution.path} (${resolution.source === "user" ? "yours" : resolution.source})` +
  (resolution.skipped.length ? ` · skipped ${resolution.skipped.map(s => `${s.path}: ${s.reason}`).join("; ")}` : "");

/**
 * Picks the binary for one requirement: explicit trusted config wins outright
 * (no fallback when it is unsuitable), then the user's compatible install,
 * then the bundled package. Newer binaries that meet the protocol and
 * features are accepted; only an unsuitable one is passed over, and why is reported.
 */
export async function resolveJevFabric(options: {
  configured: string;
  cwd: string;
  agentDir: string;
  requirement: JevFabricRequirement;
  env?: NodeJS.ProcessEnv;
  home?: string;
  bundled?: () => string | undefined;
}): Promise<JevFabricResolution> {
  const skipped: JevFabricResolution["skipped"] = [];
  const tryCandidate = async (file: string, source: JevFabricSource): Promise<JevFabricResolution | undefined> => {
    let capabilities: JevFabricCapabilities;
    try {
      capabilities = await probeJevFabric(file);
    } catch (error) {
      skipped.push({ path: file, source, reason: (error as NodeJS.ErrnoException).code === "ENOENT" ? "not found" : "did not answer capabilities or --version" });
      return undefined;
    }
    const reason = unmet(capabilities, options.requirement);
    if (reason) { skipped.push({ path: file, source, reason }); return undefined; }
    return { path: file, source, capabilities, skipped };
  };
  const finish = (resolution: JevFabricResolution): JevFabricResolution => {
    setJevFabricStatus(describe(resolution));
    return resolution;
  };

  const configured = options.configured.trim();
  if (configured && configured !== "auto") {
    const resolution = await tryCandidate(configured, "config");
    if (resolution) return finish(resolution);
    const why = skipped.at(-1)!.reason;
    setJevFabricStatus(`executor.jevFabric.binary ${configured}: ${why}`);
    throw new Error(`executor.jevFabric.binary (${configured}) is unsuitable for ${options.requirement}: ${why}. Fix or clear the setting; Fabric does not fall back from an explicit binary.`);
  }
  const user = userCandidates(options.cwd, options.env, options.home);
  skipped.push(...user.skipped.map(entry => ({ ...entry, source: "user" as const })));
  for (const file of user.found) {
    const resolution = await tryCandidate(file, "user");
    if (resolution) return finish(resolution);
  }
  const bundled = (options.bundled ?? (() => stageBundledJevFabric(options.agentDir)))();
  if (bundled) {
    const root = real(path.resolve(options.agentDir));
    const identity = installedBinary(bundled, [root], workspaceRoots(options.cwd));
    if (!identity.path) skipped.push({ path: bundled, source: "bundled", reason: identity.reason! });
    else {
      const resolution = await tryCandidate(identity.path, "bundled");
      if (resolution) return finish(resolution);
    }
  }
  const tried = skipped.length ? ` Tried: ${skipped.map(s => `${s.path} (${s.reason})`).join("; ")}.` : "";
  setJevFabricStatus(`unavailable for ${options.requirement}${tried}`);
  throw new Error(`No suitable jev-fabric for ${options.requirement}.${tried} Ask the user before installing or updating it (curl -fsSL https://raw.githubusercontent.com/fabric-runtime/jev-fabric/main/install.sh | sh, or jev-fabric -- update), or set executor.jevFabric.binary.`);
}
