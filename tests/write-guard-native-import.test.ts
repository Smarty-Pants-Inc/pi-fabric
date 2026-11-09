import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, expect, it } from "vitest";

const cleanups: string[] = [];
afterEach(() => {
  for (const directory of cleanups.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

// Trust-boundary regression: real raw-source entry in a fresh Node process, not Vitest's
// .js-to-.ts resolver or stale dist. This proves guard decisions; alpha-task-writes covers
// genuine native write/edit mutations separately. Never inspect the caller's real home.
it("imports the standalone TS guard in Node and rechecks strict/default/file-override mode through its hook", () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "wg-native-import-")));
  cleanups.push(root);
  const home = path.join(root, "home");
  const cwd = path.join(root, "work");
  fs.mkdirSync(home);
  fs.mkdirSync(cwd);
  const guardUrl = pathToFileURL(path.resolve("src/agents/write-guard.ts")).href;
  const probe = `
import assert from "node:assert/strict";
import fs from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
assert.equal(homedir(), ${JSON.stringify(home)});
assert.equal(process.env.SMARTY_ALPHA, "0");
const { default: register, readWritePolicy } = await import(${JSON.stringify(guardUrl)});
const cwd = process.cwd();
const outside = path.join(${JSON.stringify(root)}, "outside.txt");
const inside = path.join(cwd, "inside.txt");
const disabled = path.join(homedir(), ".config/smarty/bash-guard.disabled");
const rooted = { readOnly: false, writableRoots: [cwd], shell: "deny" };
const readOnly = { readOnly: true, writableRoots: [], shell: "deny" };
const fixtures = [
  { raw: JSON.stringify(rooted), expected: rooted },
  { raw: JSON.stringify(readOnly), expected: readOnly },
  { raw: "{malformed", expected: readOnly },
].map(({ raw, expected }) => {
  process.env.PI_FABRIC_WRITE_POLICY = raw;
  assert.deepEqual(readWritePolicy(), expected);
  let hook;
  register({ on(name, handler) {
    assert.equal(name, "tool_call");
    assert.equal(hook, undefined);
    hook = handler;
  } });
  assert.equal(typeof hook, "function");
  return { policy: expected, call: (toolName, input) => hook({ toolName, input }, { cwd }) };
});
const warnings = [];
console.warn = (...args) => warnings.push(args.join(' '));
const stages = [];
const check = (stage, alpha) => {
  for (const { policy, call } of fixtures) {
    for (const tool of ["write", "edit"]) {
      const scope = "refused path: " + JSON.stringify(outside) + "; allowed roots: " + JSON.stringify(policy.writableRoots);
      const reason = policy.readOnly
        ? "Fabric write policy refuses " + tool + ": this agent is read-only; " + scope
        : "Fabric write policy refuses " + tool + " outside writable roots; " + scope;
      assert.deepEqual(call(tool, { path: outside }), alpha ? undefined : { block: true, reason });
      if (!policy.readOnly) assert.equal(call(tool, { path: inside }), undefined);
    }
    for (const tool of ["bash", "powershell"]) {
      assert.deepEqual(call(tool, { command: "true" }), alpha ? undefined : { block: true,
        reason: "Fabric write policy refuses " + tool + ': shell commands cannot be confined; the parent must request shell: "unconfined"' });
      if (alpha) assert.equal(warnings.length, 1);
    }
    assert.equal(call("read", { path: outside }), undefined);
  }
  stages.push(stage);
};
check("strict-at-import", false);
delete process.env.SMARTY_ALPHA;
check("default-alpha", true);
process.env.SMARTY_ALPHA = "0";
check("strict-again", false);
fs.mkdirSync(path.dirname(disabled), { recursive: true });
fs.writeFileSync(disabled, "");
check("live-file-override", true);
fs.unlinkSync(disabled);
check("strict-after-file-removal", false);
assert.equal(warnings.length, 1);
assert.match(warnings[0], /FABRIC_ALPHA_SHELL_CONFINEMENT_BYPASS/);
console.log(JSON.stringify({ policies: fixtures.length, stages, shellWarnings: warnings.length }));
`;
  const child = spawnSync(process.execPath, ["--input-type=module", "--eval", probe], {
    cwd,
    encoding: "utf8",
    timeout: 10_000,
    // Retain NODE_OPTIONS, including the test runner's inherited deletion guard.
    env: { ...process.env, HOME: home, USERPROFILE: home, SMARTY_ALPHA: "0",
      PI_CODING_AGENT_DIR: path.join(home, ".pi", "agent"),
      PI_FABRIC_ACTOR_ID: "", PI_FABRIC_ACTOR_NAME: "" },
  });
  expect(child.error, child.stderr).toBeUndefined();
  expect(child.status, child.stderr).toBe(0);
  expect(JSON.parse(child.stdout.trim())).toEqual({ policies: 3, shellWarnings: 1, stages: [
    "strict-at-import", "default-alpha", "strict-again", "live-file-override", "strict-after-file-removal",
  ] });
});
