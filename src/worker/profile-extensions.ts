import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import ignore from "ignore";
import { minimatch } from "minimatch";
import { resolveAgentDir } from "../core/agent-dir.js";
import { fabricResourceRoot } from "../core/fabric-resource.js";

export interface ProfileExtensions {
  paths: string[];
  names: string[];
  warnings: string[];
}

const record = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const override = (value: string): boolean => /^[!+-]/.test(value);
const glob = (value: string): boolean => /[*?]/.test(value);
const real = (value: string): string => { try { return fs.realpathSync(value); } catch { return path.resolve(value); } };
const resolvePath = (value: string, base: string): string => {
  const trimmed = value.trim();
  const expanded = trimmed === "~" ? os.homedir() : /^~[/\\]/.test(trimmed) ? path.join(os.homedir(), trimmed.slice(2))
    : trimmed.startsWith("file://") ? fileURLToPath(trimmed) : trimmed;
  return path.resolve(base, expanded);
};
const matches = (file: string, pattern: string, base: string, exact = false): boolean => {
  const normalized = pattern.replace(/^\.\//, "").split(path.sep).join("/");
  const candidates = [path.relative(base, file), file, ...(exact ? [] : [path.basename(file)])].map(value => value.split(path.sep).join("/"));
  return candidates.some(value => exact ? value === normalized : minimatch(value, normalized));
};
const filter = (files: string[], patterns: string[], base: string): string[] => {
  const includes = patterns.filter(value => !override(value));
  return files.filter(file => {
    let enabled = !includes.length || includes.some(pattern => matches(file, pattern, base));
    if (patterns.some(value => value.startsWith("!") && matches(file, value.slice(1), base))) enabled = false;
    if (patterns.some(value => value.startsWith("+") && matches(file, value.slice(1), base, true))) enabled = true;
    if (patterns.some(value => value.startsWith("-") && matches(file, value.slice(1), base, true))) enabled = false;
    return enabled;
  });
};

/** Opaque launchers cannot expose Pi's pre-load discovery API. Replay only the
 * selected personal profile's explicit extensions/packages, never auto-discovery
 * or project resources (whose trust boundary belongs to Pi). Filesystem discovery
 * mirrors Pi's path bases, manifests, conventional directories and filters; it
 * neither imports extension code/a peer SDK nor installs missing packages.
 */
export const resolveProfileExtensions = (pinnedFabricPath: string, environment: NodeJS.ProcessEnv = process.env): ProfileExtensions => {
  const paths: string[] = [];
  const warnings = new Map<string, string>();
  const warn = (entry: string, reason: string): void => { if (!warnings.has(entry)) warnings.set(entry, `Profile extension ${entry} skipped: ${reason}`); };
  let profile: string;
  let settings: Record<string, unknown>;
  try {
    profile = path.resolve(resolveAgentDir(environment.PI_CODING_AGENT_DIR ?? ""));
    const parsed: unknown = JSON.parse(fs.readFileSync(path.join(profile, "settings.json"), "utf8").replace(/^\uFEFF/, ""));
    if (!record(parsed)) throw new Error("invalid settings");
    settings = parsed;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") warn("settings.json", "unreadable or invalid settings");
    return { paths, names: [], warnings: [...warnings.values()] };
  }
  const pinned = real(pinnedFabricPath);
  const isFabric = (file: string): boolean => {
    if (real(file) === pinned || fabricResourceRoot(file, "extensions")) return true;
    try { return JSON.parse(fs.readFileSync(path.join(file, "package.json"), "utf8")).name === "pi-fabric"; } catch { return false; }
  };
  const strings = (value: unknown, entry: string): string[] => {
    if (value === undefined) return [];
    if (!Array.isArray(value)) { warn(entry, "expected a list"); return []; }
    // Keep positions stable so a bad item and a later missing path cannot
    // collide in the one-warning-per-entry ledger. Empty sentinels never load.
    return value.map((item, index) => {
      if (typeof item === "string" && item.trim() && !item.includes("\0")) return item;
      warn(`${entry}[${index}]`, "expected a nonempty path or pattern"); return "";
    });
  };
  const manifest = (directory: string): Record<string, unknown> | undefined => {
    try {
      const parsed: unknown = JSON.parse(fs.readFileSync(path.join(directory, "package.json"), "utf8"));
      if (!record(parsed)) throw new Error("invalid manifest");
      return parsed;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    }
  };
  const visiting = new Set<string>();
  const collect = (file: string, entry: string): string[] => {
    const canonical = real(file);
    if (visiting.has(canonical)) { warn(entry, "cyclic extension directory"); return []; }
    visiting.add(canonical);
    try {
      if (isFabric(file)) return [];
      const stat = fs.statSync(file);
      if (stat.isFile()) {
        if (!/\.(?:[cm]?[jt]s)$/.test(file)) { warn(entry, "not an extension file"); return []; }
        return [real(file)];
      }
      if (!stat.isDirectory()) { warn(entry, "not a file or directory"); return []; }
      const pkg = manifest(file);
      const declared = record(pkg?.pi) ? strings(pkg.pi.extensions, `${entry}.pi.extensions`) : [];
      if (declared.length) {
        const found = fromManifest(declared, file, entry);
        if (found.length) return found;
      }
      for (const index of ["index.ts", "index.js"]) {
        const target = path.join(file, index);
        if (fs.existsSync(target)) return collect(target, entry);
      }
      // Pi discovers direct .ts/.js children and subdirectory entrypoints, not
      // arbitrary recursive trees. Exact manifest paths may target dot paths.
      const ignored = ignore();
      for (const name of [".gitignore", ".ignore", ".fdignore"]) {
        try { ignored.add(fs.readFileSync(path.join(file, name), "utf8")); } catch { /* Pi ignores unreadable ignore files. */ }
      }
      return fs.readdirSync(file, { withFileTypes: true }).flatMap(child => {
        if (child.name.startsWith(".") || child.name === "node_modules") return [];
        try {
          const target = path.join(file, child.name);
          const childStat = fs.statSync(target);
          const ignorePath = childStat.isDirectory() ? `${child.name}/` : child.name;
          if (ignored.ignores(ignorePath)) return [];
          if (childStat.isFile()) return /\.(?:ts|js)$/.test(child.name) ? collect(target, `${entry}/${child.name}`) : [];
          if (!childStat.isDirectory()) return [];
          const childPkg = manifest(target);
          if (record(childPkg?.pi) && Array.isArray(childPkg.pi.extensions) && childPkg.pi.extensions.length) return collect(target, `${entry}/${child.name}`);
          const index = ["index.ts", "index.js"].find(name => fs.existsSync(path.join(target, name)));
          return index ? collect(path.join(target, index), `${entry}/${child.name}`) : [];
        } catch { warn(`${entry}/${child.name}`, "missing, unreadable or invalid entry"); return []; }
      });
    } catch { warn(entry, "missing, unreadable or invalid entry"); return []; }
    finally { visiting.delete(canonical); }
  };
  const fromManifest = (entries: string[], base: string, entry: string): string[] => {
    const found = entries.flatMap((value, index) => {
      if (!value || override(value)) return [];
      const key = `${entry}.pi.extensions[${index}]`;
      try {
        const targets = glob(value) ? fs.globSync(value, { cwd: base }).filter(match => path.relative(base, path.resolve(base, match)).split(path.sep).every(part => part === ".." || !part.startsWith("."))).sort().map(match => path.resolve(base, match)) : [path.resolve(base, value)];
        if (!targets.length) warn(key, "pattern matched no extensions");
        return targets.flatMap(target => collect(target, key));
      } catch { warn(key, "invalid extension path or pattern"); return []; }
    });
    return filter(found, entries.filter(override), base);
  };
  const extensions = strings(settings.extensions, "extensions");
  const explicit = extensions.flatMap((value, index) => {
    // Top-level globs are selectors, not discovery roots, just like Pi.
    if (!value || override(value) || glob(value)) return [];
    try { return collect(resolvePath(value, profile), `extensions[${index}]`); }
    catch { warn(`extensions[${index}]`, "invalid extension path"); return []; }
  });
  paths.push(...filter(explicit, extensions.filter(value => override(value) || glob(value)), profile));
  const installedPath = (source: string): string => {
    if (source.startsWith("npm:")) {
      const spec = source.slice(4).trim();
      const split = spec.indexOf("@", spec.startsWith("@") ? 1 : 0);
      const name = split < 0 ? spec : spec.slice(0, split);
      if (!/^(?:@[a-zA-Z0-9._-]+\/)?[a-zA-Z0-9._-]+$/.test(name) || name.split("/").some(part => part === "." || part === "..")) throw new Error("invalid npm package");
      const managed = path.join(profile, "npm", "node_modules", name);
      if (name === "pi-fabric" || fs.existsSync(managed)) return managed;
      // Match Pi's legacy global fallback without installing or updating. Only
      // a bounded local package-root query is needed; no network/auth lookup.
      const command = settings.npmCommand === undefined ? ["npm"] : strings(settings.npmCommand, "npmCommand");
      if (!command.length || command.some(value => !value)) return managed;
      const bun = path.basename(command[0]!).replace(/\.exe$/, "") === "bun";
      const result = spawnSync(command[0]!, [...command.slice(1), ...(bun ? ["pm", "bin", "-g"] : ["root", "-g"])], { encoding: "utf8", timeout: 2000, windowsHide: true });
      if (result.status !== 0 || !result.stdout.trim()) return managed;
      const root = bun ? path.join(path.dirname(result.stdout.trim()), "install", "global", "node_modules") : result.stdout.trim();
      const legacy = path.join(root, name);
      return fs.existsSync(legacy) ? legacy : managed;
    }
    if (/^(?:git:|github:|https?:\/\/|ssh:\/\/)/.test(source)) {
      let url = (source.startsWith("git://") ? source : source.replace(/^git:/, "")).trim().replace(/^github:/, "github.com/");
      if (/^git@[^:]+:/.test(url)) url = url.replace(/^git@([^:]+):/, "https://$1/");
      if (!url.includes("://")) url = `https://${url}`;
      const parsed = new URL(url);
      const repo = parsed.pathname.replace(/^\/+/, "").split("@")[0]!.replace(/\.git$/, "");
      const decoded = decodeURIComponent(repo);
      if (!parsed.hostname || decoded.split("/").length < 2 || decoded.includes("\\") || decoded.split("/").includes("..")) throw new Error("invalid git package");
      return path.join(profile, "git", parsed.hostname, repo);
    }
    return resolvePath(source, profile);
  };
  const seenPackages = new Set<string>();
  const packages = settings.packages;
  if (packages !== undefined && !Array.isArray(packages)) warn("packages", "expected a list");
  for (const [index, item] of (Array.isArray(packages) ? packages : []).entries()) {
    const entry = `packages[${index}]`;
    const source = typeof item === "string" ? item : record(item) ? item.source : undefined;
    if (typeof source !== "string" || !source.trim() || source.includes("\0")) { warn(entry, "expected a package source"); continue; }
    if (/^npm:pi-fabric(?:@|$)/.test(source.trim())) continue;
    try {
      const root = installedPath(source.trim());
      if (isFabric(root)) continue;
      const identity = real(root);
      if (seenPackages.has(identity)) continue; // Personal profile: first package declaration wins, like Pi.
      seenPackages.add(identity);
      const rootStat = fs.statSync(root);
      const pkg = rootStat.isDirectory() ? manifest(root) : undefined;
      if (record(item) && item.extensions !== undefined && (!Array.isArray(item.extensions) || item.extensions.some(value => typeof value !== "string" || !value))) {
        warn(entry, "invalid extension filter"); continue;
      }
      if (record(item) && item.autoload !== undefined && typeof item.autoload !== "boolean") { warn(entry, "invalid autoload filter"); continue; }
      const pi = record(pkg?.pi) ? pkg.pi : undefined;
      let found: string[];
      const packageFilter = record(item) && (item.extensions !== undefined || item.autoload === false);
      if (rootStat.isFile()) found = collect(root, entry); // Pi loads a direct package file regardless of resource filters.
      else if (pi && (!packageFilter || (Array.isArray(pi.extensions) && pi.extensions.length))) found = fromManifest(strings(pi.extensions, `${entry}.pi.extensions`), root, entry);
      else if (fs.existsSync(path.join(root, "extensions"))) found = collect(path.join(root, "extensions"), entry);
      else if (record(item) || ["skills", "prompts", "themes"].some(type => fs.existsSync(path.join(root, type)))) found = [];
      else found = collect(root, entry);
      if (rootStat.isDirectory() && record(item) && item.autoload === false) {
        // Pi applies a delta in order: an unmentioned resource is not enabled,
        // and the last matching plain/glob or +/-/! pattern wins.
        const patterns = Array.isArray(item.extensions) ? item.extensions as string[] : [];
        found = found.filter(file => {
          let enabled = false;
          for (const value of patterns) {
            const target = override(value) ? value.slice(1) : value;
            if (matches(file, target, root, value.startsWith("+") || value.startsWith("-"))) enabled = !value.startsWith("-") && !value.startsWith("!");
          }
          return enabled;
        });
      } else if (rootStat.isDirectory() && record(item) && Array.isArray(item.extensions)) {
        const patterns = item.extensions as string[];
        found = patterns.length ? filter(found, patterns, root) : [];
      }
      paths.push(...found);
    } catch { warn(entry, "missing, unreadable or invalid package"); }
  }
  const unique = [...new Set(paths)].filter(file => !isFabric(file));
  return { paths: unique, names: unique.map(file => path.basename(file)), warnings: [...warnings.values()] };
};
