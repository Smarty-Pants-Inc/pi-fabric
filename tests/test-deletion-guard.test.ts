import { spawnSync } from "node:child_process";
import fs, {
  chmodSync, existsSync, lstatSync, mkdirSync, mkdtempSync, realpathSync, rmSync as namedRmSync, symlinkSync, writeFileSync,
} from "node:fs";
import type { PathLike } from "node:fs";
import { rm as promisesRm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, sep } from "node:path";
import { pathToFileURL } from "node:url";
import vm from "node:vm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

// smarty-dev#11145: tests/test-deletion-guard.mjs (imported by tests/fleet-isolation-setup.ts, inherited by
// children through NODE_OPTIONS --import) refuses deletes outside the run's private TMPDIR locally and only
// reports them under GITHUB_ACTIONS=true. External fixtures here are NEVER deleted by this file: a sibling
// mkdtemp under the real /tmp (POSIX) or dirname(TMPDIR) (Windows), left for the owner to clean.

const guardFile = "test-deletion-guard.mjs";
const guardUrl = new URL(`./${guardFile}`, import.meta.url).href;
const refusal = /test deletion outside private temp \(smarty-dev#11145\)/;
const privateTemp = tmpdir();
const posix = process.platform !== "win32";
const hosted = process.env.GITHUB_ACTIONS === "true";

// RED first: every outside attempt is gated on the installed guard, so a missing guard fails without deleting.
const requireGuard = () =>
  expect(process.env.NODE_OPTIONS ?? "", "deletion guard must be installed before any outside deletion attempt")
    .toContain(guardFile);

const inside = (root: string, target: string) => {
  const rel = relative(root, target);
  return rel === "" || (!isAbsolute(rel) && rel !== ".." && !rel.startsWith(`..${sep}`));
};

type Outcome = { kind: "sync" | "async"; error?: unknown };
async function settle(call: () => unknown): Promise<Outcome> {
  let value: unknown;
  try { value = call(); } catch (error) { return { kind: "sync", error }; }
  if (value === undefined) return { kind: "sync" };
  try { await value; return { kind: "async" }; } catch (error) { return { kind: "async", error }; }
}

type Done = (error: NodeJS.ErrnoException | null) => void;
// A synchronous throw escapes to settle() as "sync": callback APIs must report through the callback.
const callback = (invoke: (path: PathLike, done: Done) => void) => (path: PathLike) => {
  let done!: Done;
  const result = new Promise<void>((resolve, reject) => { done = (error) => (error ? reject(error) : resolve()); });
  invoke(path, done);
  return result;
};

type Op = { name: string; target: "file" | "dir"; mode: Outcome["kind"]; call: (path: PathLike) => unknown };
const ops: Op[] = [
  { name: "fs.rmSync", target: "file", mode: "sync", call: (path) => fs.rmSync(path) },
  { name: "named rmSync", target: "file", mode: "sync", call: (path) => namedRmSync(path) },
  { name: "fs.unlinkSync", target: "file", mode: "sync", call: (path) => fs.unlinkSync(path) },
  { name: "fs.rmdirSync", target: "dir", mode: "sync", call: (path) => fs.rmdirSync(path) },
  { name: "fs.rm callback", target: "file", mode: "async", call: callback((path, done) => fs.rm(path, done)) },
  { name: "fs.unlink callback", target: "file", mode: "async", call: callback((path, done) => fs.unlink(path, done)) },
  { name: "fs.rmdir callback", target: "dir", mode: "async", call: callback((path, done) => fs.rmdir(path, done)) },
  { name: "fs.promises.rm", target: "file", mode: "async", call: (path) => fs.promises.rm(path) },
  { name: "fs.promises.unlink", target: "file", mode: "async", call: (path) => fs.promises.unlink(path) },
  { name: "fs.promises.rmdir", target: "dir", mode: "async", call: (path) => fs.promises.rmdir(path) },
  { name: "named fs/promises rm", target: "file", mode: "async", call: (path) => promisesRm(path) },
];
const forms: Array<[string, (path: string) => PathLike]> = [
  ["string", (path) => path], ["Buffer", (path) => Buffer.from(path)], ["URL", (path) => pathToFileURL(path)],
];
const cases = ops.flatMap((op) => forms.map(([form, toPath]) => ({ label: `${op.name} (${form})`, op, toPath })));
const fileOps = ops.filter((op) => op.target === "file");
// "dotdot" is <inside>/nested/../keep.txt with nested -> <external>/keep-dir: the kernel walks the real
// symlink to <external>/keep.txt, while a lexical path.resolve would wrongly land on an inside decoy.
// Built by string concatenation: join() and pathToFileURL() would normalize the ".." away.
const escapeCases = fileOps.flatMap((op) => [
  { label: `${op.name} through an escaping symlinked parent (string)`, op, kind: "escape" as const, toPath: (p: string): PathLike => p },
  { label: `${op.name} through symlink/.. (string)`, op, kind: "dotdot" as const, toPath: (p: string): PathLike => p },
  { label: `${op.name} through symlink/.. (Buffer)`, op, kind: "dotdot" as const, toPath: (p: string): PathLike => Buffer.from(p) },
]);

let external = "";
let externalFile = "";
let externalDir = "";
let fixtures = "";

beforeAll(() => {
  requireGuard();
  external = mkdtempSync(join(posix ? "/tmp" : dirname(privateTemp), "delguard-external-"));
  externalFile = join(external, "keep.txt");
  externalDir = join(external, "keep-dir");
  writeFileSync(externalFile, "keep");
  mkdirSync(externalDir);
  expect(inside(privateTemp, external), `${external} must be outside ${privateTemp}`).toBe(false);
  // Defence in depth only: refusals are asserted by message, so EACCES can never pass for the guard.
  if (posix) chmodSync(external, 0o555);
  fixtures = mkdtempSync(join(privateTemp, "delguard-inside-"));
});

afterAll(() => {
  if (!external) return;
  if (posix && existsSync(external)) chmodSync(external, 0o755);
  // Never deleted here (smarty-dev#11145 scope): the owner removes it.
  console.warn(`delguard external fixture left for owner cleanup: ${external}`);
});

const escapeLinks = () => {
  const root = mkdtempSync(join(fixtures, "escape-"));
  const parent = join(root, "parent");
  const nested = join(root, "nested");
  const decoy = join(root, basename(externalFile));
  symlinkSync(external, parent, "dir");
  symlinkSync(externalDir, nested, "dir");
  writeFileSync(decoy, "decoy");
  return { escape: join(parent, basename(externalFile)), dotdot: `${nested}${sep}..${sep}${basename(externalFile)}`, decoy };
};

const expectExternalIntact = () => {
  expect(fs.readFileSync(externalFile, "utf8")).toBe("keep");
  expect(lstatSync(externalDir).isDirectory()).toBe(true);
};

const expectRefused = (outcome: Outcome, mode: Outcome["kind"]) => {
  expect(outcome.kind).toBe(mode);
  expect(String((outcome.error as Error | undefined)?.message)).toMatch(refusal);
};

const runChild = (script: string, source: string, args: string[], env: NodeJS.ProcessEnv = process.env) => {
  const path = join(fixtures, script);
  writeFileSync(path, source);
  const result = spawnSync(process.execPath, [path, ...args], { env, encoding: "utf8", timeout: 15_000 });
  expect(result.error).toBeUndefined();
  expect(result.status, result.stderr).toBe(0);
  return result;
};

describe.skipIf(hosted)("deletion guard refuses outside the private temp (local)", () => {
  it.each(cases)("refuses $label outside and leaves the target", async ({ op, toPath }) => {
    requireGuard();
    expectRefused(await settle(() => op.call(toPath(op.target === "file" ? externalFile : externalDir))), op.mode);
    expectExternalIntact();
  });

  it.skipIf(!posix).each(escapeCases)("refuses $label", async ({ op, kind, toPath }) => {
    requireGuard();
    const links = escapeLinks();
    expectRefused(await settle(() => op.call(toPath(links[kind]))), op.mode);
    expectExternalIntact();
    expect(fs.readFileSync(links.decoy, "utf8")).toBe("decoy");
  });

  it("cannot be widened by changing TMPDIR after installation", async () => {
    requireGuard();
    const saved = { TMPDIR: process.env.TMPDIR, TMP: process.env.TMP, TEMP: process.env.TEMP };
    try {
      Object.assign(process.env, { TMPDIR: external, TMP: external, TEMP: external });
      expectRefused(await settle(() => fs.rmSync(externalFile)), "sync");
      expectRefused(await settle(() => fs.promises.unlink(externalFile)), "async");
    } finally {
      for (const [key, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
    expect(tmpdir()).toBe(privateTemp);
    expectExternalIntact();
  });
});

describe("deletion guard permits the private temp", () => {
  it.each(cases)("deletes inside with $label", async ({ op, toPath }) => {
    const root = mkdtempSync(join(fixtures, "allowed-"));
    const target = join(root, op.target);
    if (op.target === "file") writeFileSync(target, "x");
    else mkdirSync(target);
    expect(await settle(() => op.call(toPath(target)))).toEqual({ kind: op.mode });
    expect(existsSync(target)).toBe(false);
  });

  it.skipIf(!posix).each(fileOps)("removes a final symlink to an outside file with $name", async (op) => {
    requireGuard();
    const link = join(mkdtempSync(join(fixtures, "final-link-")), "link");
    symlinkSync(externalFile, link, "file");
    expect(await settle(() => op.call(link))).toEqual({ kind: op.mode });
    expect(() => lstatSync(link)).toThrow(/ENOENT/);
    expectExternalIntact();
  });
});

// Same operation names as ops; the parent asserts they match. Runs with GITHUB_ACTIONS=false so it
// enforces even under a hosted (report-mode) parent.
const refusalChild = `
import fs, { rmSync } from "node:fs";
import { rm } from "node:fs/promises";
import { pathToFileURL } from "node:url";
const cb = (invoke) => (p) => { let done; const r = new Promise((ok, no) => { done = (e) => (e ? no(e) : ok()); }); invoke(p, done); return r; };
const ops = {
  "fs.rmSync": (p) => fs.rmSync(p), "named rmSync": (p) => rmSync(p), "fs.unlinkSync": (p) => fs.unlinkSync(p),
  "fs.rmdirSync": (p) => fs.rmdirSync(p), "fs.rm callback": cb((p, d) => fs.rm(p, d)),
  "fs.unlink callback": cb((p, d) => fs.unlink(p, d)), "fs.rmdir callback": cb((p, d) => fs.rmdir(p, d)),
  "fs.promises.rm": (p) => fs.promises.rm(p), "fs.promises.unlink": (p) => fs.promises.unlink(p),
  "fs.promises.rmdir": (p) => fs.promises.rmdir(p), "named fs/promises rm": (p) => rm(p),
};
const forms = { string: (p) => p, Buffer: (p) => Buffer.from(p), URL: (p) => pathToFileURL(p) };
const results = [];
for (const { label, op, form, path } of JSON.parse(process.argv[2])) {
  let kind = "sync", message = null;
  try { const value = ops[op](forms[form](path)); kind = "async"; await value; }
  catch (error) { message = error.message; }
  results.push({ label, kind, message });
}
console.log(JSON.stringify({ ops: Object.keys(ops), results, nodeOptions: process.env.NODE_OPTIONS ?? "" }));
`;

const runRootChild = `
import fs from "node:fs";
const [root, sibling] = process.argv.slice(2);
let siblingError = null;
try { fs.rmSync(sibling); } catch (error) { siblingError = error.message; }
fs.rmSync(root, { recursive: true, force: true });
console.log(JSON.stringify({ siblingError, rootExists: fs.existsSync(root) }));
`;

// The recorder replaces every deleter BEFORE the explicit guard import, so report mode wraps the
// recorder: even a report-mode guard that forwards the call cannot delete the external target.
const reportChild = `
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
const calls = [];
for (const name of ["rm", "rmSync", "rmdir", "rmdirSync", "unlink", "unlinkSync"]) {
  fs[name] = (...args) => { calls.push({ name, path: String(args[0]) }); if (typeof args.at(-1) === "function") args.at(-1)(null); };
}
for (const name of ["rm", "rmdir", "unlink"]) {
  fs.promises[name] = async (...args) => { calls.push({ name: "promises." + name, path: String(args[0]) }); };
}
syncBuiltinESMExports();
const [guard, target] = process.argv.slice(2);
await import(guard);
function reportCallerMarker() { fs.rmSync(target); }
reportCallerMarker();
console.log("RECORDER:" + JSON.stringify({ calls, exists: fs.existsSync(target) }));
`;

// F-01: on POSIX "\" is a filename character, not a separator. Recorder installed BEFORE the explicit
// guard import, so even a buggy delegation reaches only the recorder; no native deleter is ever called.
const backslashChild = String.raw`
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
const root = fs.mkdtempSync(join(tmpdir(), "backslash-root-"));
Object.assign(process.env, { TMPDIR: root, TMP: root, TEMP: root });
const calls = [];
fs.rmSync = (path) => { calls.push(String(path)); };
syncBuiltinESMExports();
await import(process.argv[2]);
const bs = String.fromCharCode(92);
process.chdir(dirname(root));
const paths = { backslash: root + bs, relativeBackslash: basename(root) + bs, sibling: root + "-sibling",
  child: root + "/x", trailingSlash: root + "/" };
const results = {};
for (const [label, path] of Object.entries(paths)) {
  try { fs.rmSync(path); results[label] = null; } catch (error) { results[label] = error.message; }
}
console.log(JSON.stringify({ root, results, calls }));
`;

const withoutGuard = (options = "") => {
  const tokens = options.split(/\s+/).filter(Boolean);
  const kept: string[] = [];
  for (let index = 0; index < tokens.length; index++) {
    if (tokens[index] === "--import" && tokens[index + 1]?.includes(guardFile)) { index++; continue; }
    if (!tokens[index]!.includes(guardFile)) kept.push(tokens[index]!);
  }
  return kept.join(" ");
};

it("keeps native Windows deletion hosted-only without invoking its test callbacks", () => {
  const source = fs.readFileSync(new URL("./verified-remove-windows.test.ts", import.meta.url), "utf8");
  const line = source.split("\n").find(line => line.startsWith("describe."))!;
  expect(line).toMatch(/^describe\.runIf\(/);
  const condition = line.slice("describe.runIf(".length, line.indexOf(')("Windows verified removal'));
  for (const [platform, hosted, expected] of [["win32", undefined, false], ["win32", "false", false],
    ["win32", "true", true], ["linux", "true", false]] as const) {
    let enabled: unknown;
    const describe = { runIf: (value: unknown) => (_name: string, _callback: unknown) => { enabled = value; } };
    vm.runInNewContext(`describe.runIf(${condition})("Windows verified removal", () => {})`,
      { describe, process: { platform, env: { GITHUB_ACTIONS: hosted } } });
    expect(enabled).toBe(expected);
  }
});

// smarty-dev#11145 / #4664: product suites that delete real paths run only hosted. The config derives them by
// scanning test sources for its exported markers; this file keeps the original six plus every actor-remove suite
// as an independent floor. No marker text appears in this source, so the scanner cannot select this file.
const originalSuites = ["tests/residency-operator-actors.test.ts", "tests/mesh-publish-batch-hold.test.ts",
  "tests/verified-remove-windows.test.ts", "tests/verified-remove.test.ts", "tests/verified-remove-secondary.test.ts",
  "tests/scratch-platform-retention.test.ts"];
const testsDir = new URL("./", import.meta.url);
const actorRemoveSuites = (fs.readdirSync(testsDir, { recursive: true }) as string[])
  .map((name) => String(name).split(sep).join("/"))
  .filter((name) => /(^|\/)actor-remove-[^/]*\.test\.ts$/.test(name))
  .map((name) => `tests/${name}`).sort();
const ownSuite = "tests/test-deletion-guard.test.ts";
// Imports only the real vitest.config.ts (type-only Vitest edge erased); no test file or callback runs. The child
// also writes a pseudo tests root under the private TMPDIR whose nested suite carries the config's first marker
// (taken from the config at runtime) and asks the scanner.
const configChild = `
import fs from "node:fs";
import { registerHooks } from "node:module";
registerHooks({ resolve(specifier, context, nextResolve) {
  if (specifier.endsWith(".js") && specifier.startsWith(".") && context.parentURL?.startsWith("file:")) {
    const source = new URL(specifier.slice(0, -3) + ".ts", context.parentURL);
    if (fs.existsSync(source)) return nextResolve(source.href, context);
  }
  return nextResolve(specifier, context);
}});
const [configUrl, fixtureRoot] = process.argv.slice(2);
const mod = await import(configUrl);
const config = mod.default;
const project = config.test.projects.find((entry) => entry.test.name === "default");
const out = { include: project.test.include, exclude: project.test.exclude ?? [], rootExclude: config.test.exclude ?? [],
  exported: typeof mod.productDeletionSuites === "function" && Array.isArray(mod.productDeletionMarkers)
    && mod.productDeletionMarkers.length > 0 };
if (out.exported) {
  const text = mod.productDeletionMarkers[0];
  out.markerFound = typeof text === "string" && text.length > 0;
  if (out.markerFound) {
    fs.mkdirSync(fixtureRoot + "/nested", { recursive: true });
    fs.writeFileSync(fixtureRoot + "/nested/marker.test.ts", "/* fixture */\\n" + text + "\\n");
    fs.writeFileSync(fixtureRoot + "/nested/helper.ts", "/* fixture */\\n" + text + "\\n");
    fs.writeFileSync(fixtureRoot + "/neutral.test.ts", "export const neutral = 1;\\n");
    out.fixture = mod.productDeletionSuites(fixtureRoot);
  }
  out.suites = mod.productDeletionSuites();
}
console.log("CONFIG:" + JSON.stringify(out));
`;

it.each([[undefined, false], ["false", false], ["true", true]] as const)(
  "vitest.config.ts selects real-delete suites only when GITHUB_ACTIONS=%s is hosted", (value, selected) => {
    const env: NodeJS.ProcessEnv = { ...process.env, GITHUB_ACTIONS: value,
      NODE_OPTIONS: `${process.env.NODE_OPTIONS ?? ""} --experimental-transform-types` };
    if (value === undefined) delete env.GITHUB_ACTIONS;
    const fixtureRoot = mkdtempSync(join(fixtures, "pseudo-tests-"));
    expect(inside(privateTemp, fixtureRoot)).toBe(true);
    const result = runChild(`config-child-${value}.mjs`, configChild,
      [new URL("../vitest.config.ts", import.meta.url).href, fixtureRoot], env);
    const line = result.stdout.split(/\r?\n/).find((entry) => entry.startsWith("CONFIG:"));
    expect(line, result.stdout + result.stderr).toBeDefined();
    const output = JSON.parse(line!.slice("CONFIG:".length)) as { include: string[]; exclude: string[];
      rootExclude: string[]; exported: boolean; markerFound?: boolean; fixture?: string[]; suites?: string[] };
    expect(output.include).toEqual(["tests/**/*.test.ts"]);
    expect(output.exported, "vitest.config.ts must export productDeletionSuites() and non-empty productDeletionMarkers")
      .toBe(true);
    expect(output.markerFound, "first deletion marker must be usable as fixture source").toBe(true);
    expect(actorRemoveSuites).toContain("tests/actor-remove-dead-root.test.ts");
    const required = [...originalSuites, ...actorRemoveSuites];
    if (selected) {
      expect(output.suites).toEqual([]);
      expect(output.fixture).toEqual([]);
      expect(required.filter((suite) => output.exclude.includes(suite) || output.rootExclude.includes(suite))).toEqual([]);
    } else {
      const suites = output.suites!;
      expect(suites).toEqual(expect.arrayContaining(required));
      expect(suites).not.toContain(ownSuite);
      for (const suite of suites) expect(suite).toMatch(/^tests\/(?:[^/\\]+\/)*[^/\\]+\.test\.ts$/);
      expect(suites.filter((suite) => !output.exclude.includes(suite))).toEqual([]);
      expect(output.fixture).toEqual(["tests/nested/marker.test.ts"]);
    }
  });

describe("deletion guard in child processes", () => {
  it("refuses every outside API, path form and symlink escape in a child that inherits NODE_OPTIONS", () => {
    requireGuard();
    const childCases = cases.map(({ label, op }) => ({
      label, op: op.name, form: label.slice(label.lastIndexOf("(") + 1, -1),
      path: op.target === "file" ? externalFile : externalDir,
    }));
    const links = posix ? escapeLinks() : undefined;
    if (links) {
      for (const { label, op, kind, toPath } of escapeCases) {
        childCases.push({ label, op: op.name, form: typeof toPath("") === "string" ? "string" : "Buffer", path: links[kind] });
      }
    }
    // Inherited NODE_OPTIONS; only the hosted report switch is forced off so the child enforces.
    const result = runChild("refusal-child.mjs", refusalChild, [JSON.stringify(childCases)],
      { ...process.env, GITHUB_ACTIONS: "false" });
    const output = JSON.parse(result.stdout.trim()) as
      { ops: string[]; results: Array<{ label: string; kind: string; message: string | null }>; nodeOptions: string };
    expect(output.nodeOptions).toContain(guardFile);
    expect(output.ops).toEqual(ops.map((op) => op.name));
    const modes = new Map(ops.map((op) => [op.name, op.mode]));
    expect(output.results).toEqual(childCases.map(({ label, op }) =>
      ({ label, kind: modes.get(op), message: expect.stringMatching(refusal) })));
    expectExternalIntact();
    if (links) expect(fs.readFileSync(links.decoy, "utf8")).toBe("decoy");
  });

  it("cannot widen the run's private root by changing a spawned child's TMPDIR", () => {
    requireGuard();
    const input = [{ label: "changed TMPDIR", op: "fs.rmSync", form: "string", path: externalFile }];
    const result = runChild("wider-root-child.mjs", refusalChild, [JSON.stringify(input)],
      { ...process.env, GITHUB_ACTIONS: "false", TMPDIR: external, TMP: external, TEMP: external });
    const output = JSON.parse(result.stdout.trim()) as { results: Array<{ message: string | null }> };
    expect(output.results[0]?.message).toMatch(refusal);
    expectExternalIntact();
  });

  it("captures the child's narrower TMPDIR at installation and permits exit cleanup of that root", () => {
    requireGuard();
    const root = mkdtempSync(join(fixtures, "child-root-"));
    const sibling = join(fixtures, "child-sibling.txt");
    writeFileSync(join(root, "cache.json"), "x");
    writeFileSync(sibling, "sibling");
    const result = runChild("run-root-child.mjs", runRootChild, [root, sibling],
      { ...process.env, GITHUB_ACTIONS: "false", TMPDIR: root, TMP: root, TEMP: root });
    const output = JSON.parse(result.stdout.trim()) as { siblingError: string | null; rootExists: boolean };
    expect(output.siblingError).toMatch(refusal);
    expect(output.rootExists).toBe(false);
    expect(fs.readFileSync(sibling, "utf8")).toBe("sibling");
  });

  it("reports once without throwing under GITHUB_ACTIONS=true and never reaches the real deleter", () => {
    requireGuard();
    const nodeOptions = withoutGuard(process.env.NODE_OPTIONS);
    expect(nodeOptions).not.toContain(guardFile);
    const result = runChild("report-caller-marker.mjs", reportChild, [guardUrl, externalFile],
      { ...process.env, GITHUB_ACTIONS: "true", NODE_OPTIONS: nodeOptions });
    const lines = `${result.stdout}\n${result.stderr}`.split(/\r?\n/);
    const marker = "TEST_DELETION_REPORT:";
    const reports = lines.filter((line) => line.includes(marker));
    expect(reports, result.stdout + result.stderr).toHaveLength(1);
    const report = JSON.parse(reports[0]!.slice(reports[0]!.indexOf(marker) + marker.length).trim()) as
      { path: string; operation: string; caller: string };
    expect([externalFile, join(realpathSync(external), basename(externalFile))]).toContain(report.path);
    expect(report.operation).toBe("rmSync");
    expect(report.caller).toMatch(/report-caller-marker/);
    const recorded = lines.find((line) => line.startsWith("RECORDER:"));
    expect(recorded, result.stdout).toBeDefined();
    const recorder = JSON.parse(recorded!.slice("RECORDER:".length)) as
      { calls: Array<{ name: string; path: string }>; exists: boolean };
    expect(recorder.calls).toEqual([{ name: "rmSync", path: externalFile }]);
    expect(recorder.exists).toBe(true);
    expectExternalIntact();
  });

  it.skipIf(!posix)("refuses a POSIX literal trailing backslash and sibling but delegates root/ and root/x to a recorder", () => {
    const nodeOptions = withoutGuard(process.env.NODE_OPTIONS);
    expect(nodeOptions).not.toContain(guardFile);
    const result = runChild("backslash-recorder-child.mjs", backslashChild, [guardUrl],
      { ...process.env, GITHUB_ACTIONS: "false", NODE_OPTIONS: nodeOptions });
    const output = JSON.parse(result.stdout.trim()) as
      { root: string; results: Record<string, string | null>; calls: string[] };
    expect(inside(privateTemp, output.root)).toBe(true);
    expect(output.results).toEqual({
      backslash: expect.stringMatching(refusal), relativeBackslash: expect.stringMatching(refusal),
      sibling: expect.stringMatching(refusal), child: null, trailingSlash: null,
    });
    expect(output.calls).toEqual([`${output.root}/x`, `${output.root}/`]);
  });
});
