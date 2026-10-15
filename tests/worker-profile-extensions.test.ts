import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DefaultPackageManager, SettingsManager } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProfileSettingsError, resolveProfileExtensions } from "../src/worker/profile-extensions.js";
import { resolveRetrySdk } from "../src/worker/retry-profile.js";
import { recoveryTimeScale } from "../src/worker/recovery-watchdog.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const servers: http.Server[] = [];
const locked: string[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const file of locked.splice(0)) fs.chmodSync(file, 0o600);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const SECRET = "SECRET-SENTINEL-8606";
/** Writes a settings.json the resolver must refuse; returns the expected reason. */
const brokenSettings = (profile: string, mode: string): string => {
  const file = path.join(profile, "settings.json");
  if (mode === "malformed") { fs.writeFileSync(file, `{"apiKey": "${SECRET}"`); return "is not valid JSON"; }
  if (mode === "wrong-shape") { fs.writeFileSync(file, JSON.stringify([SECRET])); return "is not a JSON object"; }
  if (mode === "directory") { fs.mkdirSync(file); return "cannot be read (EISDIR)"; }
  // unreadable: a real file whose read permission is removed.
  fs.writeFileSync(file, JSON.stringify({ apiKey: SECRET, extensions: [] })); fs.chmodSync(file, 0o000); locked.push(file);
  return "cannot be read (EACCES)";
};
/** Windows ignores POSIX read bits and root bypasses them: there is no permission failure to observe. */
const chmodIneffective = (file: string): string | undefined => {
  try { fs.readFileSync(file); } catch { return undefined; }
  return `chmod 000 left settings.json readable on ${process.platform}${process.getuid?.() === 0 ? " as root" : ""}; permission refusal cannot be observed`;
};
const fixture = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-profile-extensions-"))); roots.push(root);
  const profile = path.join(root, "profile"); fs.mkdirSync(profile);
  const file = (name: string, text = "export default function() {}") => {
    const target = path.join(root, name); fs.mkdirSync(path.dirname(target), { recursive: true }); fs.writeFileSync(target, text); return target;
  };
  const settings = (value: unknown) => fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify(value));
  const pinned = file("parent/dist/index.js");
  file("parent/package.json", JSON.stringify({ name: "pi-fabric", type: "module", pi: { extensions: ["dist/index.js"] } }));
  return { root, profile, file, settings, pinned };
};
const profileResult = (f: ReturnType<typeof fixture>) => resolveProfileExtensions(f.pinned, { PI_CODING_AGENT_DIR: f.profile });
const installWorker = (f: ReturnType<typeof fixture>) => {
  const parent = path.dirname(path.dirname(f.pinned));
  fs.cpSync(path.resolve("dist"), path.join(parent, "dist"), { recursive: true });
  fs.symlinkSync(path.resolve("node_modules"), path.join(parent, "node_modules"), "junction");
  return parent;
};
const managerFor = (f: ReturnType<typeof fixture>, binary: string) => {
  vi.stubEnv("PI_CODING_AGENT_DIR", f.profile); vi.stubEnv("PI_OFFLINE", "1");
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-profile-run-root-")); roots.push(runRoot);
  const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, nice: 19, timeoutMs: 30_000, retainRuns: true, budgetUsd: 0, deniedModels: [] }, {
    workerPath: path.join(path.dirname(f.pinned), "worker.js"), piBinary: binary, fabricExtensionPath: f.pinned, fullCodeMode: false, runRoot,
  }); managers.push(manager); return manager;
};
const shellQuote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const shim = (f: ReturnType<typeof fixture>, target: string) => {
  const file = f.file("pi-shell-shim", `#!/bin/sh\nexec ${shellQuote(process.execPath)} ${shellQuote(target)} "$@"\n`);
  fs.chmodSync(file, 0o700); return file;
};
const events = (file: string) => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
const nativeEnabled = async (f: ReturnType<typeof fixture>) => {
  const settings = SettingsManager.create(f.root, f.profile, { projectTrusted: false });
  const native = await new DefaultPackageManager({ cwd: f.root, agentDir: f.profile, settingsManager: settings }).resolve(() => Promise.resolve("skip"));
  return new Set(native.extensions.filter(resource => resource.enabled).map(resource => fs.realpathSync(resource.path)));
};
/** real-ext/{a,b}.ts reached through a directory symlink at `at` (root-relative). */
const linkedDirectory = (f: ReturnType<typeof fixture>, at: string) => {
  const a = f.file("real-ext/a.ts"); const b = f.file("real-ext/b.ts"); const link = path.join(f.root, at);
  fs.mkdirSync(path.dirname(link), { recursive: true }); fs.symlinkSync(path.dirname(a), link, "junction");
  return { a, b, link };
};
const sorted = (values: readonly string[]) => [...values].sort();

describe("opaque profile extension resolution", () => {
  it("matches native Pi explicit path bases, directory/manifest discovery and exclusions, without auto-discovery", async () => {
    const f = fixture();
    const one = f.file("hooks/one.ts"); const two = f.file("hooks/two.ts"); const three = f.file("package/src/three.ts");
    f.file("package/src/excluded.ts");
    f.file("package/package.json", JSON.stringify({ name: "safety-package", pi: { extensions: ["src/*.ts", "!src/excluded.ts"] } }));
    const nested = f.file("directory/nested/index.js");
    const hidden = f.file("profile/extensions/auto.ts");
    f.settings({ extensions: ["../hooks", "../directory", "!two.ts"], packages: [{ source: "../package", extensions: ["src/*.ts"] }, path.dirname(path.dirname(f.pinned))] });
    const actual = profileResult(f);
    expect(actual.paths).toEqual([one, nested, three]); expect(actual.names).toEqual(["one.ts", "index.js", "three.ts"]); expect(actual.warnings).toEqual([]);
    const settings = SettingsManager.create(f.root, f.profile, { projectTrusted: false });
    const native = await new DefaultPackageManager({ cwd: f.root, agentDir: f.profile, settingsManager: settings }).resolve(() => Promise.resolve("skip"));
    const expected = native.extensions.filter(resource => resource.enabled && resource.path !== f.pinned && resource.path !== hidden).map(resource => fs.realpathSync(resource.path));
    expect(new Set(actual.paths)).toEqual(new Set(expected)); expect(actual.paths).not.toContain(two);
  });
  it("honors Pi directory ignore files and minimatch filters without disabling explicit file paths", async () => {
    const f = fixture(); const one = f.file("directory/one.ts"); const ignored = f.file("directory/ignored.ts");
    f.file("directory/two.ts"); f.file("directory/disabled/index.ts");
    f.file("directory/.gitignore", "ignored.ts\n"); f.file("directory/.ignore", "two.ts\n"); f.file("directory/.fdignore", "disabled/\n");
    f.settings({ extensions: ["../directory", "../directory/ignored.ts", "!@(two|ignored).ts", "+../directory/ignored.ts"] });
    const actual = profileResult(f);
    const settings = SettingsManager.create(f.root, f.profile, { projectTrusted: false });
    const native = await new DefaultPackageManager({ cwd: f.root, agentDir: f.profile, settingsManager: settings }).resolve(() => Promise.resolve("skip"));
    expect(actual.paths).toEqual([one, ignored]);
    expect(new Set(actual.paths)).toEqual(new Set(native.extensions.filter(resource => resource.enabled).map(resource => fs.realpathSync(resource.path))));
    expect(actual.warnings).toEqual([]);
  });
  it("uses tilde, file URLs, relative PI_CODING_AGENT_DIR and the default ~/.pi/agent profile", () => {
    const f = fixture(); const hook = f.file("hook.ts");
    vi.stubEnv("HOME", f.root); vi.stubEnv("USERPROFILE", f.root);
    f.settings({ extensions: ["~/hook.ts", pathToFileURL(hook).href] });
    expect(resolveProfileExtensions(f.pinned, { PI_CODING_AGENT_DIR: "~/profile" }).paths).toEqual([hook]);
    const defaultProfile = path.join(os.homedir(), ".pi/agent"); fs.mkdirSync(defaultProfile, { recursive: true });
    fs.copyFileSync(path.join(f.profile, "settings.json"), path.join(defaultProfile, "settings.json"));
    expect(resolveProfileExtensions(f.pinned, {}).paths).toEqual([hook]);
    const relative = path.relative(process.cwd(), f.profile);
    expect(resolveProfileExtensions(f.pinned, { PI_CODING_AGENT_DIR: relative }).paths).toEqual([hook]);
  });
  it("resolves already-installed npm/git packages, respects package filters and never installs missing sources", () => {
    const f = fixture();
    const npm = f.file("profile/npm/node_modules/@test/safety/extensions/npm.ts");
    f.file("profile/npm/node_modules/@test/safety/package.json", JSON.stringify({ name: "@test/safety", version: "1.0.0" }));
    const git = f.file("profile/git/github.com/test/safety/extensions/git.ts");
    f.file("profile/git/github.com/test/safety/package.json", JSON.stringify({ name: "git-safety" }));
    const disabled = f.file("disabled/disabled.ts"); f.file("disabled/package.json", JSON.stringify({ name: "disabled", pi: { extensions: ["disabled.ts"] } }));
    f.settings({ packages: ["npm:@test/safety@1.0.0", "git:github.com/test/safety@main", { source: "../disabled", extensions: [] }, "npm:pi-fabric@99.0.0"] });
    expect(profileResult(f)).toEqual({ paths: [npm, git], names: ["npm.ts", "git.ts"], warnings: [] }); expect(profileResult(f).paths).not.toContain(disabled);
  });
  it("matches native local package files and filtered conventional directories", async () => {
    const f = fixture(); const file = f.file("single.ts"); const conventional = f.file("package/extensions/guard.ts");
    f.file("package/package.json", JSON.stringify({ name: "guards", pi: { extensions: [] } }));
    f.file("root-index/index.ts");
    f.settings({ packages: [{ source: "../single.ts", extensions: [] }, { source: "../package", extensions: ["extensions/*.ts"] }, { source: "../root-index" }] });
    const actual = profileResult(f);
    const settings = SettingsManager.create(f.root, f.profile, { projectTrusted: false });
    const native = await new DefaultPackageManager({ cwd: f.root, agentDir: f.profile, settingsManager: settings }).resolve(() => Promise.resolve("skip"));
    expect(actual.paths).toEqual([file, conventional]);
    expect(new Set(actual.paths)).toEqual(new Set(native.extensions.filter(resource => resource.enabled).map(resource => fs.realpathSync(resource.path))));
    expect(actual.warnings).toEqual([]);
  });
  it("matches native ordered autoload deltas and keeps the first duplicate package declaration", async () => {
    const f = fixture(); const one = f.file("package/extensions/one.ts"); const two = f.file("package/extensions/two.ts");
    f.file("package/package.json", JSON.stringify({ name: "guards", pi: { extensions: ["extensions/*.ts"] } }));
    for (const item of [
      { source: "../package", autoload: false },
      { source: "../package", autoload: false, extensions: ["extensions/*.ts", "!two.ts"] },
      { source: "../package", autoload: false, extensions: ["extensions/*.ts", "!one.ts", "+extensions/one.ts", "-extensions/two.ts", "+extensions/two.ts"] },
    ]) {
      f.settings({ packages: [item, "../package"] });
      const actual = profileResult(f);
      const settings = SettingsManager.create(f.root, f.profile, { projectTrusted: false });
      const native = await new DefaultPackageManager({ cwd: f.root, agentDir: f.profile, settingsManager: settings }).resolve(() => Promise.resolve("skip"));
      expect(new Set(actual.paths)).toEqual(new Set(native.extensions.filter(resource => resource.enabled).map(resource => fs.realpathSync(resource.path))));
      expect(actual.warnings).toEqual([]);
    }
    expect(profileResult(f).paths).toEqual([one, two]);
  });
  it("excludes a file by its alias path through a symlinked directory, like Pi", async () => {
    const f = fixture(); const { a, link } = linkedDirectory(f, "profile/linked");
    for (const exclusion of ["!linked/b.ts", "!linked/b.*", `!${path.join(link, "b.ts")}`, "-linked/b.ts", `-${path.join(link, "b.ts")}`]) {
      f.settings({ extensions: ["linked", exclusion] });
      const actual = profileResult(f);
      expect(actual, exclusion).toEqual({ paths: [a], names: ["a.ts"], warnings: [] });
      expect(new Set(actual.paths), exclusion).toEqual(await nativeEnabled(f));
    }
  });
  it("excludes a file by its alias path in a package whose extensions directory is a symlink, like Pi", async () => {
    const f = fixture(); const { a } = linkedDirectory(f, "package/extensions");
    f.file("package/package.json", JSON.stringify({ name: "guards" }));
    for (const item of [
      { source: "../package", extensions: ["extensions/*.ts", "!extensions/b.ts"] },
      { source: "../package", extensions: ["!extensions/b.ts"] },
      { source: "../package", extensions: ["-extensions/b.ts"] },
      { source: "../package", autoload: false, extensions: ["extensions/*.ts", "!extensions/b.ts"] },
      { source: "../package", autoload: false, extensions: ["extensions/*.ts", "-extensions/b.ts"] },
    ]) {
      f.settings({ packages: [item] });
      const actual = profileResult(f);
      expect(actual.paths, JSON.stringify(item)).toEqual([a]);
      expect(new Set(actual.paths), JSON.stringify(item)).toEqual(await nativeEnabled(f));
    }
  });
  it("also excludes a symlinked file by its canonical path: stricter than Pi, never looser", async () => {
    const f = fixture(); const { a, b } = linkedDirectory(f, "profile/linked");
    for (const exclusion of [`!${b}`, "!../real-ext/b.ts", "!../real-ext/b.*", `-${b}`, "-../real-ext/b.ts"]) {
      f.settings({ extensions: ["linked", exclusion] });
      const actual = profileResult(f);
      expect(actual.paths, exclusion).toEqual([a]);
      // Pi matches filters only against the listed path, so it would still load b.
      const native = await nativeEnabled(f);
      expect(native.has(b), exclusion).toBe(true); expect(actual.paths.every(file => native.has(file)), exclusion).toBe(true);
    }
    f.file("package/package.json", JSON.stringify({ name: "guards" }));
    fs.symlinkSync(path.dirname(a), path.join(f.root, "package/extensions"), "junction");
    for (const item of [{ source: "../package", extensions: [`!${b}`] }, { source: "../package", autoload: false, extensions: ["extensions/*.ts", `-${b}`] }]) {
      f.settings({ packages: [item] });
      expect(profileResult(f).paths, JSON.stringify(item)).toEqual([a]);
    }
  });
  it("keeps inclusions through a symlinked directory exactly Pi's", async () => {
    const f = fixture(); const { a, b } = linkedDirectory(f, "profile/linked");
    const cases: Array<[string[], string[]]> = [
      [["linked"], [a, b]],
      [["linked", "linked/a.*"], [a]],
      [["linked", "!*.ts", "+linked/b.ts"], [b]],
      // Pi never matches inclusions against the symlink target, so neither do we.
      [["linked", "../real-ext/a.*"], []],
      [["linked", "!*.ts", `+${b}`], []],
    ];
    for (const [extensions, expected] of cases) {
      f.settings({ extensions });
      const actual = profileResult(f);
      expect(sorted(actual.paths), extensions.join(" ")).toEqual(sorted(expected));
      expect(new Set(actual.paths), extensions.join(" ")).toEqual(await nativeEnabled(f));
    }
  });
  it("dedupes by canonical path after filtering: the first-ranked entry decides, like Pi", async () => {
    const f = fixture(); const { a, b } = linkedDirectory(f, "profile/linked");
    // The excluded alias is listed first, so it suppresses the target path listed later.
    f.settings({ extensions: ["linked", "../real-ext", "!linked/b.ts"] });
    let actual = profileResult(f);
    expect(sorted(actual.paths)).toEqual([a]); expect(new Set(actual.paths)).toEqual(await nativeEnabled(f));
    // Top-level settings outrank packages: an alias exclusion there wins over a package that enables the file.
    f.file("package/package.json", JSON.stringify({ name: "guards", pi: { extensions: ["../real-ext/b.ts"] } }));
    f.settings({ extensions: ["linked", "!linked/b.ts"], packages: ["../package"] });
    actual = profileResult(f);
    expect(sorted(actual.paths)).toEqual([a]); expect(new Set(actual.paths)).toEqual(await nativeEnabled(f));
    // An unexcluded path listed first decides instead, exactly as in Pi.
    f.settings({ extensions: ["../real-ext", "linked", "!linked/b.ts"] });
    actual = profileResult(f);
    expect(sorted(actual.paths)).toEqual(sorted([a, b])); expect(new Set(actual.paths)).toEqual(await nativeEnabled(f));
  });
  it("lets a top-level exclusion beat a package entry for the identical path: stricter than Pi, never looser", async () => {
    const f = fixture(); const a = f.file("package/extensions/a.ts"); const b = f.file("package/extensions/b.ts");
    f.file("package/package.json", JSON.stringify({ name: "guards" }));
    for (const extensions of [
      ["../package/extensions/b.ts", "!b.ts"],
      ["../package/extensions", "!../package/extensions/b.ts"],
      ["../package/extensions", `-${b}`],
      ["../package/extensions/b.ts", "!*.ts", "+../package/extensions/a.ts"],
    ]) {
      f.settings({ extensions, packages: ["../package"] });
      const actual = profileResult(f);
      expect(actual, extensions.join(" ")).toEqual({ paths: [a], names: ["a.ts"], warnings: [] });
      // Pi keeps the package entry for the identical listed path, so it would still load b.
      const native = await nativeEnabled(f);
      expect(native.has(b), extensions.join(" ")).toBe(true); expect(actual.paths.every(file => native.has(file)), extensions.join(" ")).toBe(true);
    }
  });
  it("lets a top-level inclusion beat a package exclusion of the same canonical file, like Pi", async () => {
    const f = fixture(); const { a, b } = linkedDirectory(f, "package/extensions");
    f.file("package/package.json", JSON.stringify({ name: "guards" }));
    for (const item of [{ source: "../package", extensions: ["!extensions/b.ts"] }, { source: "../package", extensions: [] }, { source: "../package", autoload: false, extensions: ["extensions/*.ts", "-extensions/b.ts"] }]) {
      f.settings({ extensions: ["../real-ext/b.ts"], packages: [item] });
      const actual = profileResult(f);
      expect(actual.paths, JSON.stringify(item)).toContain(b); expect(actual.warnings).toEqual([]);
      expect(new Set(actual.paths), JSON.stringify(item)).toEqual(await nativeEnabled(f));
    }
    expect(profileResult(f).paths).toEqual([b, a]);
    // For the identical listed path Pi keeps the excluding package entry; so do we, never loading what Pi leaves disabled.
    f.settings({ extensions: ["../package/extensions/b.ts"], packages: [{ source: "../package", extensions: ["!extensions/b.ts"] }] });
    const identical = profileResult(f);
    expect(identical.paths).toEqual([a]); expect(new Set(identical.paths)).toEqual(await nativeEnabled(f));
  });
  it("skips an installed npm package whose version does not satisfy the configured spec, like Pi", async () => {
    const f = fixture(); const npm = f.file("profile/npm/node_modules/@test/pinned/extensions/pinned.ts");
    const manifestFile = path.join(f.profile, "npm/node_modules/@test/pinned/package.json");
    const cases: Array<[string, string | undefined, boolean]> = [
      ["npm:@test/pinned@1.0.0", "1.0.0", true], ["npm:@test/pinned@^1.0.0", "1.4.2", true], ["npm:@test/pinned", "0.1.0", true], ["npm:@test/pinned@latest", "9.9.9", true],
      ["npm:@test/pinned@1.0.0", "2.0.0", false], ["npm:@test/pinned@^2.0.0", "1.4.2", false], ["npm:@test/pinned@1.0.0", undefined, false],
    ];
    for (const [source, version, loaded] of cases) {
      fs.writeFileSync(manifestFile, JSON.stringify({ name: "@test/pinned", ...(version ? { version } : {}) }));
      f.settings({ packages: [source] });
      const actual = profileResult(f); const label = `${source} installed ${version}`;
      expect(actual.paths, label).toEqual(loaded ? [npm] : []);
      expect(actual.warnings, label).toEqual(loaded ? [] : [`Profile extension packages[0] skipped: ${version ? "installed version does not match the configured spec" : "installed npm package has no version"}`]);
      expect(new Set(actual.paths), label).toEqual(await nativeEnabled(f));
    }
  });
  it("resolves Pi's git:-prefixed SCP and ssh:// sources to the clone but leaves a bare git@host:owner/repo a local path, like Pi", async () => {
    const f = fixture(); const clone = f.file("profile/git/github.com/test/scp/extensions/scp.ts");
    f.file("profile/git/github.com/test/scp/package.json", JSON.stringify({ name: "scp" }));
    for (const source of ["git:git@github.com:test/scp", "git:git@github.com:test/scp.git@v1", "ssh://git@github.com/test/scp"]) {
      f.settings({ packages: [source] });
      expect(profileResult(f), source).toEqual({ paths: [clone], names: ["scp.ts"], warnings: [] });
      expect(new Set(profileResult(f).paths), source).toEqual(await nativeEnabled(f));
    }
    // Pi's parseSource treats a source without git: or a URL scheme as a local path.
    f.settings({ packages: ["git@github.com:test/scp"] });
    const bare = profileResult(f);
    expect(bare.paths).toEqual([]); expect(bare.warnings).toEqual(["Profile extension packages[0] skipped: missing, unreadable or invalid package"]);
    expect(await nativeEnabled(f)).toEqual(new Set());
  });
  it("excludes Fabric packages, direct files and symlink aliases but retains unrelated checkout hooks", () => {
    const f = fixture(); const other = f.file("foreign/dist/index.js");
    f.file("foreign/package.json", JSON.stringify({ name: "pi-fabric", pi: { extensions: ["dist/index.js"] } }));
    const caller = f.file("foreign/caller.ts");
    const alias = path.join(f.root, "fabric-alias.js"); fs.symlinkSync(other, alias);
    f.settings({ extensions: [f.pinned, other, alias, caller], packages: ["../foreign", "../parent"] });
    expect(profileResult(f)).toEqual({ paths: [caller], names: ["caller.ts"], warnings: [] });
  });
  it("counts one warning per invalid/missing entry and keeps valid siblings", () => {
    const f = fixture(); const valid = f.file("valid.ts");
    f.settings({ extensions: [valid, "../missing.ts", null, 7, "", "../not-extension.txt"], packages: [null, { source: 1 }, "../missing-package", { source: "../parent", extensions: null }] });
    f.file("not-extension.txt", "not code");
    const result = profileResult(f); expect(result.paths).toEqual([valid]);
    // Fabric's own package is excluded regardless of its filter.
    expect(result.warnings).toHaveLength(8); expect(new Set(result.warnings).size).toBe(8);
  });
  it("skips broken directory siblings and cyclic manifests independently", () => {
    const f = fixture(); const valid = f.file("directory/valid.ts");
    fs.symlinkSync(path.join(f.root, "missing.ts"), path.join(f.root, "directory/broken.ts"));
    f.file("directory/invalid/package.json", "{");
    f.file("cycle/package.json", JSON.stringify({ pi: { extensions: ["."] } }));
    f.settings({ extensions: ["../directory", "../cycle"] });
    const result = profileResult(f);
    expect(result.paths).toEqual([valid]); expect(result.warnings).toHaveLength(3);
    expect(result.warnings.some(warning => warning.includes("cyclic"))).toBe(true);
  });
  it("treats missing profile settings as nothing to replay", () => {
    expect(profileResult(fixture())).toEqual({ paths: [], names: [], warnings: [] });
  });
  it.for(["malformed", "wrong-shape", "directory", "unreadable"])("fails closed for %s profile settings, naming the file but not its contents", (mode, ctx) => {
    const f = fixture(); const file = path.join(f.profile, "settings.json");
    const reason = brokenSettings(f.profile, mode);
    if (mode === "unreadable") { const why = chmodIneffective(file); if (why) ctx.skip(why); }
    let thrown: unknown;
    try { profileResult(f); } catch (error) { thrown = error; }
    expect(thrown).toBeInstanceOf(ProfileSettingsError);
    expect(thrown).toMatchObject({ settingsFile: file, reason, message: new ProfileSettingsError(file, reason).message });
    expect((thrown as Error).message).toContain(file); expect((thrown as Error).message).not.toContain(SECRET);
  });
});

describe.skipIf(process.platform === "win32")("real worker profile guards through a shell shim", () => {
  it("keeps --no-extensions and explicitly passes every non-Fabric profile extension, with basename receipt and counted skips", async () => {
    const f = fixture(); const parent = installWorker(f); fs.writeFileSync(f.pinned, "export default function() {}\n");
    const guard = f.file("guards/bash-guard.ts"); const projectGuard = f.file("guards/project-tool-guard.ts"); const packageGuard = f.file("safety/extension.ts");
    f.file("safety/package.json", JSON.stringify({ name: "safety", pi: { extensions: ["extension.ts"] } }));
    const foreign = f.file("foreign/dist/index.js"); f.file("foreign/package.json", JSON.stringify({ name: "pi-fabric", pi: { extensions: ["dist/index.js"] } }));
    const alias = path.join(f.root, "fabric-alias.js"); fs.symlinkSync(foreign, alias);
    f.settings({ extensions: [guard, "../guards/project-tool-guard.ts", foreign, alias, f.pinned, "../missing.ts", 3], packages: [parent, { source: "../safety" }] });
    const argvFile = path.join(f.root, "argv.json");
    const recorder = f.file("argv-recorder.mjs", `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(argvFile)}, JSON.stringify(process.argv.slice(2))); await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-rpc.mjs")).href)});`);
    const result = await managerFor(f, shim(f, recorder)).run({ task: "profile argv proof", transport: "process", extensions: true, tools: [] });
    expect(result.status, result.error).toBe("completed");
    const argv = JSON.parse(fs.readFileSync(argvFile, "utf8"));
    expect(argv).toEqual(["--mode", "rpc", "--session", path.join(path.dirname(result.logFile!), "session.jsonl"), "--no-extensions",
      "-e", guard, "-e", projectGuard, "-e", packageGuard, "-e", f.pinned, "-e", path.join(parent, "dist/worker/principal-delivery.js"), "-e", path.join(parent, "dist/worker/session-id.js"), "-e", path.join(parent, "dist/guards/sender-headers.js"), "-e", path.join(parent, "dist/agents/write-guard.js"), "--no-tools", "--thinking", "medium"]);
    expect(argv).not.toContain(foreign); expect(argv).not.toContain(alias);
    const log = events(result.logFile!);
    expect(log.filter(event => event.type === "fabric_profile_extensions")).toEqual([{ type: "fabric_profile_extensions", names: ["bash-guard.ts", "project-tool-guard.ts", "extension.ts"], loadedCount: 3, skippedCount: 2 }]);
    expect(log.filter(event => event.source === "profile_extensions").map(event => event.count)).toEqual([1, 2]);
    expect(result.warnings).toHaveLength(2);
  });
  it("does not replay profile guards when extensions are explicitly disabled", async () => {
    const f = fixture(); const parent = installWorker(f); fs.writeFileSync(f.pinned, "export default function() {}\n");
    const guard = f.file("guard.ts"); f.settings({ extensions: [guard], packages: [parent] });
    const argvFile = path.join(f.root, "argv.json");
    const recorder = f.file("argv-recorder.mjs", `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(argvFile)}, JSON.stringify(process.argv.slice(2))); await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-rpc.mjs")).href)});`);
    const result = await managerFor(f, shim(f, recorder)).run({ task: "disabled extensions", transport: "process", extensions: false, tools: [] });
    expect(result.status, result.error).toBe("completed"); const argv = JSON.parse(fs.readFileSync(argvFile, "utf8"));
    expect(argv).toContain("--no-extensions"); expect(argv).not.toContain(guard); expect(events(result.logFile!).some(event => event.type === "fabric_profile_extensions")).toBe(false);
  });
  it.for(["malformed", "unreadable"])("refuses the launch for %s profile settings before the launcher runs", async (mode, ctx) => {
    const f = fixture(); installWorker(f); fs.writeFileSync(f.pinned, "export default function() {}\n");
    const file = path.join(f.profile, "settings.json");
    const reason = brokenSettings(f.profile, mode);
    if (mode === "unreadable") { const why = chmodIneffective(file); if (why) ctx.skip(why); }
    const argvFile = path.join(f.root, "argv.json");
    const recorder = f.file("argv-recorder.mjs", `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(argvFile)}, JSON.stringify(process.argv.slice(2))); await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-rpc.mjs")).href)});`);
    const result = await managerFor(f, shim(f, recorder)).run({ task: `${mode} settings`, transport: "process", extensions: true, tools: [] });
    expect(result.status).toBe("failed");
    expect(result.error).toBe(new ProfileSettingsError(file, reason).message); expect(result.error).not.toContain(SECRET);
    expect(fs.existsSync(argvFile)).toBe(false);
    const log = events(result.logFile!);
    expect(log.filter(event => event.type === "fabric_profile_extensions_refused")).toEqual([{ type: "fabric_profile_extensions_refused", settingsFile: file, reason }]);
    expect(log.some(event => event.type === "fabric_profile_extensions")).toBe(false);
    expect(JSON.stringify(log)).not.toContain(SECRET);
  });
  it("launches with missing profile settings: nothing to replay", async () => {
    const f = fixture(); const parent = installWorker(f); fs.writeFileSync(f.pinned, "export default function() {}\n");
    const argvFile = path.join(f.root, "argv.json");
    const recorder = f.file("argv-recorder.mjs", `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(argvFile)}, JSON.stringify(process.argv.slice(2))); await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi-rpc.mjs")).href)});`);
    const result = await managerFor(f, shim(f, recorder)).run({ task: "missing settings", transport: "process", extensions: true, tools: [] });
    expect(result.status, result.error).toBe("completed"); expect(result.warnings ?? []).toEqual([]);
    expect(JSON.parse(fs.readFileSync(argvFile, "utf8"))).toEqual(["--mode", "rpc", "--session", path.join(path.dirname(result.logFile!), "session.jsonl"), "--no-extensions",
      "-e", f.pinned, "-e", path.join(parent, "dist/worker/principal-delivery.js"), "-e", path.join(parent, "dist/worker/session-id.js"), "-e", path.join(parent, "dist/guards/sender-headers.js"), "-e", path.join(parent, "dist/agents/write-guard.js"), "--no-tools", "--thinking", "medium"]);
    expect(events(result.logFile!).filter(event => event.type === "fabric_profile_extensions")).toEqual([{ type: "fabric_profile_extensions", names: [], loadedCount: 0, skippedCount: 0 }]);
  });
  it.each(["shim", "native"])("%s real Pi process preserves the profile guard (native argv remains unchanged)", async launch => {
    const f = fixture(); const parent = installWorker(f);
    const loaded = path.join(f.root, "loaded.json"); const refused = path.join(f.root, "refused.json"); const sideEffect = path.join(f.root, "must-not-exist");
    fs.writeFileSync(f.pinned, "export default function() {}\n");
    const guard = f.file("guard/marker-guard.ts", `import fs from 'node:fs'; export default function(pi) { fs.writeFileSync(${JSON.stringify(loaded)}, JSON.stringify(process.argv.slice(2))); pi.on('tool_call', event => { if (event.toolName === 'bash' && event.input.command.includes('PROFILE_GUARD_MARKER')) { fs.writeFileSync(${JSON.stringify(refused)}, JSON.stringify(event.input)); return { block: true, reason: 'profile marker guard refused PROFILE_GUARD_MARKER' }; } }); }`);
    let toolResult: unknown;
    const server = http.createServer((request, response) => {
      const chunks: Buffer[] = []; request.on("data", chunk => chunks.push(chunk)); request.on("end", () => {
        const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        toolResult = body.messages.find((message: { role: string }) => message.role === "tool") ?? toolResult;
        const delta = toolResult ? { role: "assistant", content: "guard refusal verified" } : { role: "assistant", tool_calls: [{ index: 0, id: "guard-call", type: "function", function: { name: "bash", arguments: JSON.stringify({ command: `printf PROFILE_GUARD_MARKER > ${shellQuote(sideEffect)}` }) } }] };
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        for (const [part, finish] of [[delta, null], [{}, toolResult ? "stop" : "tool_calls"]]) response.write(`data: ${JSON.stringify({ id: "offline", object: "chat.completion.chunk", choices: [{ index: 0, delta: part, finish_reason: finish }] })}\n\n`);
        response.end("data: [DONE]\n\n");
      });
    }); servers.push(server); await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    f.settings({ extensions: ["../guard/marker-guard.ts"], packages: [parent], defaultProjectTrust: "never", enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } });
    fs.writeFileSync(path.join(f.profile, "models.json"), JSON.stringify({ providers: { proof: { baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-only", api: "openai-completions", models: [{ id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 32000, maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }] } } }));
    const cli = fs.realpathSync(path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
    const result = await managerFor(f, launch === "shim" ? shim(f, cli) : cli).run({ task: "invoke the marker command", transport: "process", extensions: true, tools: ["bash"], model: "proof/offline" });
    expect(result.status, result.error).toBe("completed"); expect(result.text).toBe("guard refusal verified");
    expect(fs.existsSync(sideEffect)).toBe(false); expect(fs.existsSync(refused)).toBe(true);
    expect(toolResult).toMatchObject({ role: "tool", content: expect.stringContaining("profile marker guard refused PROFILE_GUARD_MARKER") });
    const argv = JSON.parse(fs.readFileSync(loaded, "utf8"));
    const session = path.join(path.dirname(result.logFile!), "session.jsonl");
    // A native launcher runs through the SDK task entry (final-answer fence),
    // which receives its SDK directory and recovery scale ahead of Pi's argv.
    expect(argv).toEqual([...(launch === "native" ? [resolveRetrySdk(cli), String(recoveryTimeScale())] : []), "--mode", "rpc", "--session", session, ...(launch === "shim" ? ["--no-extensions", "-e", guard] : []), "-e", f.pinned,
      "-e", path.join(parent, "dist/worker/principal-delivery.js"), "-e", path.join(parent, "dist/worker/session-id.js"), "-e", path.join(parent, "dist/guards/actor-bash-hook.js"), "-e", path.join(parent, "dist/guards/sender-headers.js"), "-e", path.join(parent, "dist/agents/write-guard.js"), "--tools", "bash", "--model", "proof/offline", "--thinking", "medium"]);
    expect(events(result.logFile!).filter(event => event.type === "fabric_profile_extensions")).toEqual(launch === "shim" ? [{ type: "fabric_profile_extensions", names: ["marker-guard.ts"], loadedCount: 1, skippedCount: 0 }] : []);
    const sessionEvents = events(session);
    expect(sessionEvents.some(event => event.type === "message" && event.message?.role === "toolResult" && event.message.isError === true)).toBe(true);
    if (process.env.TASK_OUT) fs.writeFileSync(path.join(process.env.TASK_OUT, `${launch}-guard-evidence.json`), JSON.stringify({ status: result.status, text: result.text, argv, toolResult, sideEffectExists: fs.existsSync(sideEffect), profileReceipt: events(result.logFile!).filter(event => event.type === "fabric_profile_extensions") }, null, 2) + "\n");
  }, 45_000);
});
