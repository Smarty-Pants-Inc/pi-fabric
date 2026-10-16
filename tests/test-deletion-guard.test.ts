import { spawnSync } from "node:child_process";
import { isHostedOnlyAllowed } from "../scripts/hosted-only.mjs";
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
// reports them only on a verified GitHub-hosted runner. External fixtures here are NEVER deleted by this file: a sibling
// mkdtemp under dirname(TMPDIR), inside test-env's private parent but outside Vitest's guard root,
// left for the owner to clean.

const guardFile = "test-deletion-guard.mjs";
const guardUrl = new URL(`./${guardFile}`, import.meta.url).href;
const refusal = /test deletion outside private temp \(smarty-dev#11145\)/;
const privateTemp = tmpdir();
const posix = process.platform !== "win32";
const hosted = isHostedOnlyAllowed(process.env);

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
  external = mkdtempSync(join(dirname(privateTemp), "delguard-external-"));
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
const [root, sibling, outside] = process.argv.slice(2);
const statusTemp = sibling + ".tmp";
let siblingError = null, outsideError = null;
fs.writeFileSync(statusTemp, "status");
try { fs.renameSync(statusTemp, sibling); } catch (error) { siblingError = error.message; }
try { fs.rmSync(outside); } catch (error) { outsideError = error.message; }
fs.rmSync(root, { recursive: true, force: true });
console.log(JSON.stringify({ siblingError, outsideError, status: fs.existsSync(sibling) ? fs.readFileSync(sibling, "utf8") : null,
  rootExists: fs.existsSync(root) }));
`;

// Synthetic runner evidence is confined to config-only and recorded-delegate probes below.
// It never admits/imports a hosted test module or performs a native deletion.
const hostedRunnerProbe = `
if (process.env.GITHUB_ACTIONS === "true") {
  delete process.env.SMARTY_ROLE;
  delete process.env.PI_TEST_FLEET_HOST;
  process.env.RUNNER_ENVIRONMENT = "github-hosted";
  const runner = process.platform === "win32" ? process.env.TMPDIR : "/home/runner/work/_temp";
  process.env.RUNNER_TEMP = runner;
  const stat = fs.statSync, realpath = fs.realpathSync;
  fs.statSync = (p, ...args) => String(p) === runner ? { isDirectory: () => true } : stat(p, ...args);
  fs.realpathSync = Object.assign((p, ...args) => String(p) === runner ? runner : realpath(p, ...args), realpath);
}
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
${hostedRunnerProbe}
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

// Every mutating delegate is a recorder installed before the guard. Only the explicitly known
// inside -> NEW destination rename may reach native fs; even RED cannot overwrite an outside victim.
const overwriteChild = String.raw`
import fs from "node:fs";
import * as fsModule from "node:fs";
import * as promiseModule from "node:fs/promises";
import { renameSync, rename, copyFileSync, copyFile, cpSync, cp, linkSync, link, symlinkSync, symlink } from "node:fs";
import { rename as promiseRename, copyFile as promiseCopyFile, cp as promiseCp,
  link as promiseLink, symlink as promiseSymlink } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
const [guard, parent, input] = process.argv.slice(2);
const spec = JSON.parse(input);
const caseDir = fs.mkdtempSync(join(parent, "overwrite-case-"));
const root = join(caseDir, "guard-root");
const outside = join(caseDir, "outside");
fs.mkdirSync(root);
fs.mkdirSync(outside);
const victim = join(outside, "keep.txt");
const source = join(root, "source.txt");
const fresh = join(root, "new-target.txt");
fs.writeFileSync(victim, "outside victim\n");
fs.writeFileSync(source, "inside source\n");
let src = spec.direction === "outside-source" ? victim : source;
let dest = ["outside-dest", "thenable"].includes(spec.direction) ? victim : fresh;
let nestedSrc, nestedDest;
if (spec.direction === "final-symlink") {
  dest = join(root, "final-link");
  fs.symlinkSync(victim, dest, "file");
}
// Dangling links point at absent outside names; record-only delegates must never create them.
const absentOutside = join(outside, "new-file");
const absentOutsideDir = join(outside, "missing-dir");
if (spec.direction === "dangling-final") {
  dest = join(root, "dangling-link");
  fs.symlinkSync(absentOutside, dest, "file");
}
if (spec.direction === "dangling-ancestor") {
  fs.symlinkSync(absentOutsideDir, join(root, "dangling-dir"), "dir");
  dest = join(root, "dangling-dir", "leaf.txt");
}
if (spec.direction === "missing-plain") dest = join(root, "plain-missing", "leaf.txt");
if (spec.direction === "nested" || spec.direction === "filtered") {
  src = join(root, "tree");
  dest = join(root, "copied");
  fs.mkdirSync(join(src, "nested"), { recursive: true });
  fs.mkdirSync(dest);
  nestedSrc = join(src, "nested", "keep.txt");
  nestedDest = join(dest, "nested", "keep.txt");
  fs.writeFileSync(nestedSrc, "nested source\n");
  fs.symlinkSync(outside, join(dest, "nested"), "dir");
}
Object.assign(process.env, { TMPDIR: root, TMP: root, TEMP: root });
const text = (p) => p instanceof URL ? fileURLToPath(p) : String(p);
const calls = [], filterVisits = [];
const native = { sync: fs.renameSync, callback: fs.rename, promise: fs.promises.rename };
const safeRename = (name, args) => name === "rename" && spec.direction === "inside"
  && text(args[0]) === source && text(args[1]) === fresh && !fs.existsSync(fresh);
const record = (name, args) => calls.push({ name, source: text(args[0]), dest: text(args[1]) });
// Match Node's options-or-callback overload and filter every kept root/recursive pair.
const cpOptions = (args) => typeof args[2] === "function" ? {} : args[2] ?? {};
const cpPairs = (args) => [[args[0], args[1]], ...(nestedSrc ? [[nestedSrc, nestedDest]] : [])];
const filterSync = (name, args) => {
  const options = cpOptions(args);
  if (name !== "cp" || typeof options.filter !== "function") return true;
  for (const pair of cpPairs(args)) if (!options.filter(...pair)) return false;
  return true;
};
const filterAsync = async (name, args) => {
  const options = cpOptions(args);
  if (name !== "cp" || typeof options.filter !== "function") return true;
  for (const pair of cpPairs(args)) if (!await options.filter(...pair)) return false;
  return true;
};
for (const name of ["rename", "copyFile", "cp", "link", "symlink"]) {
  fs[name + "Sync"] = (...args) => {
    if (!filterSync(name, args)) return;
    record(name, args);
    if (safeRename(name, args)) return native.sync(...args);
  };
  fs[name] = (...args) => {
    const done = args.at(-1);
    void (async () => {
      if (!await filterAsync(name, args)) { done(null); return; }
      record(name, args);
      if (safeRename(name, args)) { native.callback(...args); return; }
      done(null);
    })().catch(done);
  };
  fs.promises[name] = async (...args) => {
    if (!await filterAsync(name, args)) return;
    record(name, args);
    if (safeRename(name, args)) return native.promise(...args);
  };
}
syncBuiltinESMExports();
${hostedRunnerProbe}
await import(guard);
// Read the static named bindings after synchronization, alongside the live module namespaces.
const namedSync = { rename: renameSync, copyFile: copyFileSync, cp: cpSync, link: linkSync, symlink: symlinkSync };
const namedCallback = { rename, copyFile, cp, link, symlink };
const namedPromises = { rename: promiseRename, copyFile: promiseCopyFile, cp: promiseCp,
  link: promiseLink, symlink: promiseSymlink };
const form = (p) => spec.form === "Buffer" ? Buffer.from(p) : spec.form === "URL" ? pathToFileURL(p) : p;
const args = [form(src), form(dest)];
if (spec.op === "cp" && !spec.bare) {
  const options = { recursive: true };
  // cpSync treats a non-Promise thenable as a truthy filter result; it must not bypass destination checks.
  if (spec.direction === "thenable") options.filter = () => ({ then() { return true; } });
  if (spec.direction === "filtered") options.filter = (s, d) => {
    filterVisits.push([text(s), text(d)]);
    return text(d) !== nestedDest;
  };
  args.push(options);
}
const api = spec.surface.startsWith("module") ? fsModule : fs;
let kind = "sync", errorCode = null, errorMessage = null;
try {
  let value;
  if (spec.surface.endsWith("sync")) {
    const invoke = spec.surface === "named-sync" ? namedSync[spec.op] : api[spec.op + "Sync"];
    value = invoke(...args);
  } else if (spec.surface.endsWith("callback")) {
    let done;
    value = new Promise((ok, no) => { done = (error) => error ? no(error) : ok(); });
    const invoke = spec.surface === "named-callback" ? namedCallback[spec.op] : api[spec.op];
    invoke(...args, done);
  } else {
    const promises = spec.surface === "named-promise" ? namedPromises
      : spec.surface === "module-promise" ? promiseModule : fs.promises;
    value = promises[spec.op](...args);
  }
  if (value !== undefined) { kind = "async"; await value; }
} catch (error) { errorCode = error.code ?? null; errorMessage = error.message; }
console.log("OVERWRITE:" + JSON.stringify({ caseDir, root, victim: fs.readFileSync(victim, "utf8"),
  source: fs.existsSync(source) ? fs.readFileSync(source, "utf8") : null,
  renamed: fs.existsSync(fresh) ? fs.readFileSync(fresh, "utf8") : null,
  kind, errorCode, errorMessage, calls, filterVisits,
  outsideCreated: fs.existsSync(absentOutside) || fs.existsSync(absentOutsideDir) }));
`;

const overwriteSurfaces = ["fs-sync", "named-sync", "module-sync", "fs-callback", "named-callback",
  "module-callback", "fs-promise", "named-promise", "module-promise"] as const;
type OverwriteCase = { op: string; surface: typeof overwriteSurfaces[number]; direction: string;
  form?: string; report?: boolean; bare?: boolean };
const overwriteCases: OverwriteCase[] = [
  ...overwriteSurfaces.flatMap((surface) => ["outside-dest", "outside-source", "inside"]
    .map((direction) => ({ op: "rename", surface, direction }))),
  ...["copyFile", "cp", "link", "symlink"].flatMap((op) => overwriteSurfaces.flatMap((surface) =>
    ["outside-dest", "outside-source"].map((direction) => ({ op, surface, direction })))),
  ...["copyFile", "cp"].flatMap((op) => overwriteSurfaces.map((surface) =>
    ({ op, surface, direction: "final-symlink" }))),
  ...["copyFile", "cp"].flatMap((op) => overwriteSurfaces.flatMap((surface) =>
    ["dangling-final", "dangling-ancestor", "missing-plain"].map((direction) => ({ op, surface, direction })))),
  ...overwriteSurfaces.flatMap((surface) => ["nested", "filtered"].map((direction) =>
    ({ op: "cp", surface, direction }))),
  ...["Buffer", "URL"].flatMap((form) => ["outside-dest", "outside-source", "inside"].map((direction) =>
    ({ op: "rename", surface: "fs-sync" as const, direction, form }))),
  ...overwriteSurfaces.filter((surface) => surface.endsWith("callback")).flatMap((surface) =>
    ["outside-dest", "outside-source"].map((direction) => ({ op: "cp", surface, direction, bare: true }))),
  ...overwriteSurfaces.filter((surface) => surface.endsWith("sync")).map((surface) =>
    ({ op: "cp", surface, direction: "thenable" })),
  { op: "rename", surface: "fs-sync", direction: "outside-dest", report: true },
];
const recordedCases = overwriteCases.map((spec) => ({ ...spec,
  label: `${spec.op} ${spec.surface} ${spec.direction} ${spec.form ?? "string"}${spec.bare ? " bare-options" : ""}${spec.report ? " hosted-report" : ""}` }));

it.each(recordedCases)("overwrite guard recorded $label", (spec) => {
  const nodeOptions = withoutGuard(process.env.NODE_OPTIONS);
  expect(nodeOptions).not.toContain(guardFile);
  const result = runChild("overwrite-recorder-child.mjs", overwriteChild,
    [guardUrl, privateTemp, JSON.stringify(spec)],
    { ...process.env, NODE_OPTIONS: nodeOptions, GITHUB_ACTIONS: spec.report ? "true" : "false" });
  const line = result.stdout.split(/\r?\n/).find((entry) => entry.startsWith("OVERWRITE:"));
  expect(line, result.stdout + result.stderr).toBeDefined();
  const output = JSON.parse(line!.slice("OVERWRITE:".length)) as { caseDir: string; root: string;
    victim: string; source: string | null; renamed: string | null; kind: Outcome["kind"];
    errorCode: string | null; errorMessage: string | null; calls: unknown[]; filterVisits: unknown[];
    outsideCreated: boolean };
  console.info("OVERWRITE_EVIDENCE:" + JSON.stringify({ label: spec.label, code: output.errorCode,
    delegates: output.calls.length, kind: output.kind, intact: output.victim === "outside victim\n",
    renamed: output.renamed, filterVisits: output.filterVisits.length,
    reports: result.stderr.split(/\r?\n/).filter((entry) => entry.startsWith("TEST_DELETION_REPORT:")).length }));
  expect(inside(privateTemp, output.caseDir)).toBe(true);
  expect(inside(output.caseDir, output.root)).toBe(true);
  expect(output.victim).toBe("outside victim\n");
  expect(output.outsideCreated).toBe(false);
  if (!(spec.op === "rename" && spec.direction === "inside")) expect(output.source).toBe("inside source\n");
  expect(output.kind).toBe(spec.surface.endsWith("sync") ? "sync" : "async");
  if (spec.report) {
    expect(output.errorCode).toBeNull();
    expect(output.calls).toHaveLength(1);
    const reports = result.stderr.split(/\r?\n/).filter((entry) => entry.startsWith("TEST_DELETION_REPORT:"));
    expect(reports).toHaveLength(1);
    expect(JSON.parse(reports[0]!.slice("TEST_DELETION_REPORT:".length))).toMatchObject({
      operation: "renameSync", caller: expect.stringContaining("overwrite-recorder-child") });
  } else if (["outside-dest", "outside-source", "final-symlink", "nested", "thenable", "dangling-final",
    "dangling-ancestor"].includes(spec.direction)
    && !(spec.direction === "outside-source" && spec.op !== "rename")) {
    expect(output.errorCode, output.errorMessage ?? JSON.stringify(output.calls)).toBe("ERR_TEST_DELETION_GUARD");
    expect(output.errorMessage).toMatch(refusal);
    expect(output.calls).toEqual([]);
  } else {
    expect(output.errorCode, output.errorMessage ?? "unexpected refusal").toBeNull();
    expect(output.calls).toHaveLength(spec.direction === "filtered" ? 0 : 1);
    if (spec.direction === "filtered") expect(output.filterVisits).toHaveLength(2);
    if (spec.op === "rename") {
      expect(output.source).toBeNull();
      expect(output.renamed).toBe("inside source\n");
    }
  }
});

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
      { describe, process: { platform, env: { GITHUB_ACTIONS: hosted } },
        hostedOnly: () => isHostedOnlyAllowed({ GITHUB_ACTIONS: hosted, RUNNER_ENVIRONMENT: "github-hosted",
          RUNNER_TEMP: "/home/runner/work/_temp" }, () => "/home/runner/work/_temp") });
    expect(enabled).toBe(expected);
  }
});

// smarty-dev#11145 / #4664: native/tool deletion, Bun children that ignore the guard import (#13001),
// and the outside-TMPDIR fixture caught by the revert rule stay hosted-only. Exercise the real config
// in a child (no test callbacks execute), including the newly guardable local path.
const hostedOnlySuites = ["tests/actor-child-completion-idle-preflight.test.ts", "tests/actor-manager.test.ts",
  "tests/actor-remove-dead-root.test.ts", "tests/agent-manager.test.ts", "tests/agent-release-observation.test.ts",
  "tests/agents-provider.test.ts", "tests/residency-commit-fence.test.ts", "tests/residency-stopped-writer-retention.test.ts",
  "tests/residency.test.ts", "tests/routed-session-boundaries.test.ts", "tests/stall-alarms.test.ts",
  "tests/task-write-policy.test.ts", "tests/temp-run-sweep.test.ts", "tests/verified-remove-windows.test.ts", "tests/windows-temp-root-native.test.ts"];
const guardableSuites = ["tests/actor-directory.test.ts",
  "tests/mesh-backend-migration-nats.test.ts",
  "tests/state-backend-nats.test.ts", "tests/residency-operator-actors.test.ts", "tests/mesh-publish-batch-hold.test.ts",
  "tests/verified-remove.test.ts", "tests/verified-remove-secondary.test.ts", "tests/scratch-platform-retention.test.ts",
  "tests/test-deletion-guard.test.ts", "tests/test-fleet-isolation.test.ts"];
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
const [configUrl] = process.argv.slice(2);
${hostedRunnerProbe}
const mod = await import(configUrl);
const config = mod.default;
const project = config.test.projects.find((entry) => entry.test.name === "default");
const out = { include: project.test.include, exclude: project.test.exclude ?? [], rootExclude: config.test.exclude ?? [],
  suites: mod.productDeletionSuites(),
  projects: config.test.projects.map(entry => ({ name: entry.test.name, exclude: entry.test.exclude ?? [] })) };
console.log("CONFIG:" + JSON.stringify(out));
`;

it.each([[undefined, false], ["false", false], ["true", true]] as const)(
  "vitest.config.cts keeps native/tool, Bun and reverted deletion hosted-only (GITHUB_ACTIONS=%s)", (value, hosted) => {
    const env: NodeJS.ProcessEnv = { ...process.env, GITHUB_ACTIONS: value,
      NODE_OPTIONS: `${process.env.NODE_OPTIONS ?? ""} --experimental-transform-types` };
    if (value === undefined) delete env.GITHUB_ACTIONS;
    const result = runChild(`config-child-${value}.mjs`, configChild,
      [new URL("../vitest.config.cts", import.meta.url).href], env);
    const line = result.stdout.split(/\r?\n/).find((entry) => entry.startsWith("CONFIG:"));
    expect(line, result.stdout + result.stderr).toBeDefined();
    const output = JSON.parse(line!.slice("CONFIG:".length)) as { include: string[]; exclude: string[];
      rootExclude: string[]; suites: string[]; projects: Array<{ name: string; exclude: string[] }> };
    expect(output.include).toEqual(["tests/**/*.test.ts"]);
    const required = hosted ? [] : hostedOnlySuites;
    expect(output.suites).toEqual(required);
    expect(output.rootExclude).toEqual(required);
    for (const project of output.projects) {
      for (const suite of hostedOnlySuites) expect(project.exclude.includes(suite), `${project.name}: ${suite}`).toBe(!hosted);
      for (const suite of guardableSuites) expect(project.exclude, `${project.name}: guarded Node fs stays local`).not.toContain(suite);
    }
    // Native/tool and Bun counterexamples stay excluded; a guarded actor suite and the migration suites
    // no longer disappear just because they import deleting product code (pi-fabric#1151).
    expect(output.exclude.includes("tests/verified-remove-windows.test.ts")).toBe(!hosted);
    expect(output.exclude.includes("tests/temp-run-sweep.test.ts")).toBe(!hosted);
    expect(output.exclude).not.toContain("tests/actor-directory.test.ts");
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

  it("keeps the inherited private ceiling when a child narrows TMPDIR for atomic status rename", () => {
    requireGuard();
    const run = mkdtempSync(join(fixtures, "child-run-"));
    const root = join(run, "tmp");
    const sibling = join(run, "status.json");
    mkdirSync(root);
    writeFileSync(sibling, "old status");
    writeFileSync(join(root, "cache.json"), "x");
    const result = runChild("run-root-child.mjs", runRootChild, [root, sibling, externalFile],
      { ...process.env, GITHUB_ACTIONS: "false", TMPDIR: root, TMP: root, TEMP: root });
    const output = JSON.parse(result.stdout.trim()) as
      { siblingError: string | null; outsideError: string | null; status: string | null; rootExists: boolean };
    expect(output.siblingError).toBeNull();
    expect(output.outsideError).toMatch(refusal);
    expect(output.status).toBe("status");
    expect(output.rootExists).toBe(false);
    expect(fs.readFileSync(sibling, "utf8")).toBe("status");
    expectExternalIntact();
  });

  it("reports once on a simulated GitHub-hosted runner and never reaches the real deleter", () => {
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

// Config imports test-temp before the deletion guard. Record every outside request before import:
// even RED never creates/deletes a shared /tmp root. Exit hooks are invoked manually, not registered.
const tempOwnershipChild = String.raw`
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
const [helperUrl, parent] = process.argv.slice(2);
const native = { mkdtemp: fs.mkdtempSync, mkdir: fs.mkdirSync, write: fs.writeFileSync, rename: fs.renameSync };
const once = process.once;
let caller, hooks = [], early = [], guarded = [], outside = [], serial = 0;
const inside = (p) => { const r = relative(caller, resolve(String(p))); return r === "" || (!isAbsolute(r) && r !== ".." && !r.startsWith(".." + sep)); };
const record = (op, path) => { if (!inside(path)) outside.push({ op, path: String(path) }); };
fs.mkdtempSync = (prefix) => { record("mkdtempSync", prefix); return inside(prefix) ? native.mkdtemp(prefix) : join(parent, "VIRTUAL-NOT-CREATED-" + ++serial); };
for (const [op, invoke] of [["mkdirSync", native.mkdir], ["writeFileSync", native.write]]) {
  fs[op] = (p, ...args) => { record(op, p); if (inside(p)) return invoke(p, ...args); };
}
for (const op of ["rmSync", "rmdirSync", "unlinkSync", "rm", "rmdir", "unlink"]) {
  fs[op] = (p, ...args) => { record(op, p); early.push(String(p)); if (typeof args.at(-1) === "function") args.at(-1)(null); };
}
for (const op of ["rm", "rmdir", "unlink"]) fs.promises[op] = async p => { record(op, p); early.push(String(p)); };
process.once = function(event, hook) { if (event === "exit") { hooks.push(hook); return this; } return once.call(this, event, hook); };
syncBuiltinESMExports();
const helper = await import(helperUrl);
const results = [];
for (const mode of ["long", "short"]) {
  caller = native.mkdtemp(join(parent, mode === "long" ? "long-private-caller-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-" : "s-"));
  process.chdir(caller);
  // The short counterexample uses a one-character caller TMPDIR, still inside our real private root.
  const envTemp = mode === "short" ? "." : caller;
  Object.assign(process.env, { TMPDIR: envTemp, TMP: envTemp, TEMP: envTemp });
  delete process.env.RUNNER_TEMP;
  delete process[Symbol.for("pi-fabric.test-owned-fleet-roots")];
  delete process[Symbol.for("pi-fabric.test-owned-temp-cleanups")];
  hooks = []; early = []; guarded = []; outside = [];
  const temp = helper.isolatedTestTemp("owned-");
  // Sibling roots let us replace each identity independently without moving the other fixture.
  Object.assign(process.env, { TMPDIR: caller, TMP: caller, TEMP: caller });
  const fleet = helper.isolateTestFleetEnvironment();
  const roots = [resolve(temp.TMPDIR), resolve(fleet.PI_FABRIC_MESH_ROOT, "..")];
  // No syncBuiltinESMExports here: the later guard must be looked up dynamically.
  fs.rmSync = p => { record("current rmSync", p); guarded.push(resolve(String(p))); };
  for (const hook of hooks) hook(0);
  const cleanup = [...guarded];
  let sentinels = false;
  if (roots.every(p => inside(p) && fs.existsSync(p))) {
    for (const [i, root] of roots.entries()) {
      const unowned = join(caller, "unowned-" + i);
      native.mkdir(unowned);
      native.write(join(unowned, "sentinel"), "pre-existing");
      native.rename(root, join(caller, "held-owned-" + i));
      native.rename(unowned, root);
    }
    guarded = [];
    for (const hook of hooks) hook(0);
    sentinels = roots.every(p => fs.readFileSync(join(p, "sentinel"), "utf8") === "pre-existing");
  }
  results.push({ mode, caller, callerTempLength: envTemp.length, roots, outside, early, cleanup,
    replacementCleanup: guarded, sentinels, hookCount: hooks.length });
}
console.log("TEMP_OWNERSHIP:" + JSON.stringify(results));
`;

it("test-temp keeps long and short callers private, uses the current exit guard and pins created roots", () => {
  const nodeOptions = withoutGuard(process.env.NODE_OPTIONS);
  expect(nodeOptions).not.toContain(guardFile);
  const result = runChild("temp-ownership-child.mjs", tempOwnershipChild,
    [new URL("../scripts/test-temp.ts", import.meta.url).href, fixtures],
    { ...process.env, NODE_OPTIONS: `${nodeOptions} --experimental-transform-types` });
  const line = result.stdout.split(/\r?\n/).find(entry => entry.startsWith("TEMP_OWNERSHIP:"));
  expect(line, result.stdout + result.stderr).toBeDefined();
  const output = JSON.parse(line!.slice("TEMP_OWNERSHIP:".length)) as Array<{ mode: string; caller: string;
    callerTempLength: number; roots: string[]; outside: unknown[]; early: string[]; cleanup: string[];
    replacementCleanup: string[]; sentinels: boolean; hookCount: number }>;
  expect(output.map(entry => entry.mode)).toEqual(["long", "short"]);
  for (const entry of output) {
    console.info("TEMP_OWNERSHIP_EVIDENCE:" + JSON.stringify(entry));
    expect(inside(privateTemp, entry.caller)).toBe(true);
    expect(entry.callerTempLength > 40).toBe(entry.mode === "long");
    expect(entry.roots.every(root => inside(entry.caller, root))).toBe(true);
    expect(entry.outside).toEqual([]);
    expect(entry.early).toEqual([]);
    expect(entry.cleanup).toEqual(entry.roots);
    expect(entry.replacementCleanup).toEqual([]);
    expect(entry.sentinels).toBe(true);
    expect(entry.hookCount).toBe(2);
  }
});
