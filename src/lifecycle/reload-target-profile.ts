import fs from "node:fs";
import path from "node:path";

const packageName = (root: string): string | undefined => {
  try {
    const parsed = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")) as { name?: unknown };
    return typeof parsed.name === "string" ? parsed.name : undefined;
  } catch {
    return undefined;
  }
};

const real = (value: string): string => {
  try { return fs.realpathSync(value); } catch { return path.resolve(value); }
};

type ProfileSection = "extensions" | "packages";
interface ProfileSlot {
  section: ProfileSection;
  index: number;
  selector: string;
  target: string;
  packageName?: string;
}
export interface ResourceBinding {
  loaded: string;
  slot: ProfileSlot;
  entries: string[];
}
interface ResourceProfile { extensions: unknown[]; packages: unknown[] }
export const readResourceProfile = (settingsPath: string): ResourceProfile | undefined => {
  try {
    const value = JSON.parse(fs.readFileSync(settingsPath, "utf8")) as ResourceProfile;
    if (!value || typeof value !== "object") return undefined;
    if (value.extensions !== undefined && !Array.isArray(value.extensions)) return undefined;
    if (value.packages !== undefined && !Array.isArray(value.packages)) return undefined;
    return { extensions: value.extensions ?? [], packages: value.packages ?? [] };
  } catch { return undefined; }
};
const localResourcePath = (source: unknown, base: string): string | undefined => {
  if (typeof source !== "string" || !source || /^(npm|git|https?):|^[!+-]|[*?{}[\]]/.test(source)) return undefined;
  return path.resolve(base, source.startsWith("~/") ? path.join(process.env.HOME ?? "", source.slice(2)) : source);
};
export const canonicalEntrypoint = (value: string): boolean => {
  try { return path.isAbsolute(value) && fs.realpathSync(value) === value && fs.statSync(value).isFile() && /\.(ts|js)$/.test(value); }
  catch { return false; }
};
/** Conservative subset of Pi discovery: exact local files, index directories and explicit manifests. */
const entrypoints = (source: string, packageEntry = false): Array<{ selector: string; target: string }> => {
  try {
    if (fs.statSync(source).isFile()) return /\.(ts|js)$/.test(source) ? [{ selector: "", target: fs.realpathSync(source) }] : [];
    let manifest: { pi?: { extensions?: unknown } } | undefined;
    try { manifest = JSON.parse(fs.readFileSync(path.join(source, "package.json"), "utf8")); } catch { /* index-only extension */ }
    const declared = manifest?.pi?.extensions;
    // A package with a Pi manifest loads only declared resources, including an empty list.
    // Packages with conventional resource directories require unsupported directory discovery.
    if (packageEntry && (manifest?.pi && (!Array.isArray(declared) || !declared.length)
      || !manifest?.pi && ["extensions", "skills", "prompts", "themes"].some(name => fs.existsSync(path.join(source, name))))) return [];
    if (Array.isArray(declared) && declared.length) {
      // Glob/filter/directory discovery is deliberately unsupported: do not guess which file Pi loaded.
      if (!declared.every(item => typeof item === "string" && localResourcePath(item, source))) return [];
      return declared.flatMap(item => {
        const file = localResourcePath(item, source)!;
        try { return fs.statSync(file).isFile() && /\.(ts|js)$/.test(file)
          ? [{ selector: path.relative(source, file), target: fs.realpathSync(file) }] : []; } catch { return []; }
      });
    }
    for (const name of ["index.ts", "index.js"]) {
      const file = path.join(source, name);
      if (fs.existsSync(file)) return [{ selector: name, target: fs.realpathSync(file) }];
    }
  } catch { /* Missing paths are not targets. */ }
  return [];
};
export const profileSlots = (profile: ResourceProfile, base: string): ProfileSlot[] => {
  const slots: ProfileSlot[] = [];
  // Top-level resource overrides can exclude package resources too. Their selection semantics
  // need full Pi discovery; never silently ignore an exclusion or glob when proving a binding.
  if (profile.extensions.some(entry => !localResourcePath(entry, base))) return slots;
  for (const section of ["extensions", "packages"] as const) {
    profile[section].forEach((entry, index) => {
      const object = typeof entry === "object" && entry !== null ? entry as { source?: unknown; extensions?: unknown; autoload?: unknown } : undefined;
      // Package filters and autoload deltas need full Pi discovery semantics; refuse this subset.
      if (object && (object.extensions !== undefined || object.autoload !== undefined)) return;
      const source = localResourcePath(section === "packages" ? (typeof entry === "string" ? entry : object?.source) : entry, base);
      if (!source) return;
      const name = section === "packages" ? packageName(source) : undefined;
      for (const file of entrypoints(source, section === "packages")) slots.push({ section, index, ...file, ...(name ? { packageName: name } : {}) });
    });
  }
  return slots;
};
export const serializedEntries = (profile: ResourceProfile, section: ProfileSection): string[] =>
  profile[section].map(entry => JSON.stringify(entry));
export const explicitResource = (sources: string[], loaded: string): boolean => sources.some(source => {
  const resolved = localResourcePath(source, process.cwd());
  // An opaque explicit source cannot be proved independent of this loaded extension.
  if (!resolved) return true;
  return real(resolved) === loaded || entrypoints(resolved).some(file => file.target === loaded);
});

export const targetFor = (binding: ResourceBinding, settingsPath: string): { target?: string; reason?: string } => {
  // Never use the caller's configured path as authority, including at command execution.
  const profile = readResourceProfile(settingsPath);
  if (!profile) return { reason: "profile-unreadable" };
  const { section, index, selector, packageName: name } = binding.slot;
  const entries = serializedEntries(profile, section);
  if (entries.length < binding.entries.length || entries[index] === undefined) return { reason: "removed-profile-entry" };
  const slots = profileSlots(profile, path.dirname(settingsPath));
  const entry = profile[section][index];
  if (entry && typeof entry === "object" && ((entry as { extensions?: unknown }).extensions !== undefined
    || (entry as { autoload?: unknown }).autoload !== undefined)) return { reason: "removed-profile-entry" };
  const selected = slots.filter(slot => slot.section === section && slot.index === index && slot.selector === selector);
  if (selected.length !== 1) return { reason: "missing-or-removed-profile-target" };
  const active = selected[0]!;
  if (slots.filter(slot => slot.target === active.target).length !== 1) return { reason: "ambiguous-profile-entry" };
  if (entries.length !== binding.entries.length || entries.some((entry, at) => at !== index && entry !== binding.entries[at])) {
    return { reason: "ambiguous-profile-change" };
  }
  if (active.packageName !== name) return { reason: "removed-profile-entry" };
  return { target: active.target };
};
