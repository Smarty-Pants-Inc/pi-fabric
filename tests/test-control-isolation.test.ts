import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

// Evaluate real selectors/registration only. Imports, live describe callbacks and ALL
// test/hook callbacks are excluded: no provider, localterm, profile, model or native Pi.
const probe = String.raw`
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";
import { registerHooks } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
registerHooks({ resolve(specifier, context, nextResolve) {
  if (specifier.endsWith(".js") && specifier.startsWith(".") && context.parentURL?.startsWith("file:")) {
    const source = new URL(specifier.slice(0, -3) + ".ts", context.parentURL);
    if (fs.existsSync(fileURLToPath(source))) return nextResolve(source.href, context);
  }
  return nextResolve(specifier, context);
}});
const input = JSON.parse(process.env.CONTROL_INPUT);
const entry = await import(pathToFileURL(path.resolve(process.env.CONTROL_ENTRY)).href);
const check = boundary => {
for (const [key, value] of Object.entries(input)) assert.equal(process.env[key], value, key + " lost at " + boundary);
const source = file => ts.createSourceFile(file, fs.readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
const js = text => ts.transpileModule(text, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.None } }).outputText;
const fail = () => { throw new Error("a test/hook callback or dependency was invoked"); };
const registered = [];
const it = (name, callback) => registered.push({ name, skip: false });
it.skipIf = skip => {
  const register = (name, callback) => registered.push({ name, skip: Boolean(skip) });
  register.each = values => (name, callback) => values.forEach(value => register(name.replace("%s", value), callback));
  return register;
};
it.each = values => (name, callback) => values.forEach(value => it(name.replace("%s", value), callback));
const liveRegistrations = [];
const describe = (name, callback) => callback(); // activation definitions only; never their tests
// Jev's describe body constructs credentials/providers, even when skipped: DON'T run it.
describe.skipIf = skip => (name, callback) => liveRegistrations.push({ name, skip });
const context = vm.createContext({ process, fs, os, path, describe, it, afterEach: () => {}, afterAll: () => {}, expect: fail, vi: {}, spawn: fail, spawnSync: fail });
const evaluateModule = (file, expose) => {
  const tree = source(file);
  const body = tree.statements.filter(node => !ts.isImportDeclaration(node)).map(node => node.getText(tree)).join("\n");
  vm.runInContext(js(body + "\nglobalThis.selection = " + expose), context);
  return tree;
};
evaluateModule("tests/jev-live.test.ts", "({enabled, command});");
assert.equal(context.selection.enabled, input.PI_FABRIC_JEV_LIVE === "1");
assert.equal(liveRegistrations.length, 1);
assert.equal(liveRegistrations[0].skip, input.PI_FABRIC_JEV_LIVE !== "1", "actual live suite registration changed");
assert.equal(JSON.stringify(context.selection.command), JSON.stringify(input.PI_FABRIC_JEV_LOCALTERM === "1" ? ["localterm", "secret", "get", "typesafe_api_key"] : []));
const activation = evaluateModule("tests/worker-activation-window.test.ts", "({selectedNativeBinary, nativeBinary});");
assert.equal(context.selection.selectedNativeBinary, input.PI_FABRIC_ACTIVATION_TEST_PI_BINARY);
assert.equal(context.selection.nativeBinary, input.PI_FABRIC_ACTIVATION_TEST_PI_BINARY ?? path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
const nativeCases = registered.filter(item => /retains LARGE old|blocks native manual|two real activations/.test(item.name));
assert.equal(nativeCases.length, 4);
for (const item of nativeCases) assert.equal(item.skip, !input.PI_FABRIC_ACTIVATION_TEST_PI_BINARY, "qualification registration: " + item.name);
assert.equal(registered.find(item => item.name.startsWith("rejects an old native CLI")).skip, Boolean(input.PI_FABRIC_ACTIVATION_TEST_PI_BINARY));
// Worker selection is inside native setup, the retry fixture and ResidentHost,
// so evaluate ONLY their actual AST initializers (never a test callback).
const workers = [];
const visit = node => {
  if (ts.isPropertyAssignment(node) && node.name.getText(activation) === "workerPath" && node.initializer.getText(activation).includes("PI_FABRIC_ACTIVATION_TEST_WORKER")) workers.push(node.initializer.getText(activation));
  ts.forEachChild(node, visit);
};
visit(activation);
assert.equal(workers.length, 3);
for (const worker of workers) {
  vm.runInContext(js("globalThis.worker = " + worker), context);
  assert.equal(context.worker, path.resolve(input.PI_FABRIC_ACTIVATION_TEST_WORKER ?? "src/worker.ts"));
}
// Audit the other two test-only inputs without executing PostgreSQL or a shell.
const initializer = (file, name) => {
  const tree = source(file);
  let value;
  const walk = node => {
    if (ts.isVariableDeclaration(node) && node.name.getText(tree) === name) value = node.initializer.getText(tree);
    ts.forEachChild(node, walk);
  };
  walk(tree);
  assert(value, name + " initializer missing");
  vm.runInContext(js("globalThis.audit = (" + value + ");"), context);
  return context.audit;
};
// Evaluate the manual native case's actual sibling-hook selector, not realpath
// existence: qualification owns artifact validity, not the isolation boundary.
context.fs = { realpathSync: value => value };
const hook = initializer("tests/worker-activation-window.test.ts", "hook");
assert.equal(hook, input.PI_FABRIC_ACTIVATION_TEST_WORKER
  ? path.join(path.dirname(path.resolve(input.PI_FABRIC_ACTIVATION_TEST_WORKER)), "worker/activation-window.js")
  : path.resolve("src/worker/activation-window.ts"));
context.fs = fs;
const candidates = initializer("tests/helpers/postgres.ts", "CANDIDATES");
if (input.PI_FABRIC_TEST_PG_BIN) assert.equal(candidates[0], input.PI_FABRIC_TEST_PG_BIN);
const delay = initializer("src/core/shell-jobs.ts", "testPidDelay");
assert.equal(delay("bash"), input.PI_FABRIC_TEST_PID_DELAY_MS === "600" ? "sleep 0.6\n" : "");
console.log(JSON.stringify({ boundary, live: liveRegistrations, nativeCases, worker: context.worker }));
};
check(process.env.CONTROL_ENTRY);
if (entry.default) {
  // Check the config-before-fork snapshot, then its actual registered setup.
  await import(pathToFileURL(path.resolve(entry.default.test.setupFiles[0])).href);
  check("worker setup after config");
}
`;

const controls = ["PI_FABRIC_JEV_LIVE", "PI_FABRIC_JEV_LOCALTERM", "PI_FABRIC_ACTIVATION_TEST_PI_BINARY",
  "PI_FABRIC_ACTIVATION_TEST_WORKER", "PI_FABRIC_TEST_PG_BIN", "PI_FABRIC_TEST_PID_DELAY_MS"];
const optedIn = {
  PI_FABRIC_JEV_LIVE: "1", PI_FABRIC_JEV_LOCALTERM: "1",
  PI_FABRIC_ACTIVATION_TEST_PI_BINARY: "C:\\exact artifact\\[native].exe",
  PI_FABRIC_ACTIVATION_TEST_WORKER: "./exact artifact/[worker].js",
  PI_FABRIC_TEST_PG_BIN: "./exact artifact/postgres bin", PI_FABRIC_TEST_PID_DELAY_MS: "600",
};
for (const entry of ["vitest.config.ts", "tests/fleet-isolation-setup.ts"]) {
  test.each([
    ["live opt-in", { PI_FABRIC_JEV_LIVE: "1", PI_FABRIC_JEV_LOCALTERM: "1" }],
    ["exact artifacts", { PI_FABRIC_ACTIVATION_TEST_PI_BINARY: optedIn.PI_FABRIC_ACTIVATION_TEST_PI_BINARY, PI_FABRIC_ACTIVATION_TEST_WORKER: optedIn.PI_FABRIC_ACTIVATION_TEST_WORKER }],
    ["opt-in and exact artifacts", optedIn],
    ["off", { PI_FABRIC_JEV_LIVE: "0", PI_FABRIC_JEV_LOCALTERM: "0", PI_FABRIC_TEST_PID_DELAY_MS: "0" }],
    ["strict non-boolean strings", { PI_FABRIC_JEV_LIVE: "true", PI_FABRIC_JEV_LOCALTERM: "01", PI_FABRIC_TEST_PID_DELAY_MS: "-1" }],
    ["missing", {}],
  ])(`${entry}: %s reaches real selectors without invoking tests`, (_name, input) => {
    const env = { ...process.env };
    for (const key of controls) delete env[key];
    Object.assign(env, input, { CONTROL_INPUT: JSON.stringify(input), CONTROL_ENTRY: entry });
    const child = spawnSync(process.execPath, ["--experimental-transform-types", "--input-type=module", "-e", probe], {
      cwd: fileURLToPath(new URL("../", import.meta.url)), env, encoding: "utf8", timeout: 10_000,
    });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
  });
}
