import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, vi } from "vitest";

// Native Node 24 transforms types; the hook only mirrors this repo's .js -> .ts source
// resolution. No CLI, model, host session, or production mesh is started.
const childProbe = String.raw`
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { registerHooks } from "node:module";
registerHooks({ resolve(specifier, context, nextResolve) {
  if (specifier.endsWith(".js") && specifier.startsWith(".") && context.parentURL?.startsWith("file:")) {
    const source = new URL(specifier.slice(0, -3) + ".ts", context.parentURL);
    if (fs.existsSync(fileURLToPath(source))) return nextResolve(source.href, context);
  }
  return nextResolve(specifier, context);
}});
const repo = process.cwd();
const sentinel = process.env.FLEET_ISOLATION_SENTINEL;
const entry = process.env.FLEET_ISOLATION_ENTRY;
const baseline = fs.readFileSync(path.join(sentinel, "agent", "fabric.json"), "utf8");
const module = await import(pathToFileURL(entry).href);
const keys = ["PI_FABRIC_MESH_ROOT", "PI_FABRIC_PROJECT_ROOT", "PI_FABRIC_PROJECT",
  "PI_FABRIC_RUN_ROOT", "PI_FABRIC_AGENT_DIR", "PI_CODING_AGENT_DIR", "MCPORTER_CONFIG"];
const contained = (root, target) => {
  const relative = path.relative(root, target);
  return relative !== "" && relative !== ".." && !relative.startsWith(".." + path.sep) && !path.isAbsolute(relative);
};
const assertIsolated = () => {
  const root = path.dirname(process.env.PI_CODING_AGENT_DIR);
  assert(root !== sentinel && !contained(sentinel, root), "test root must not be inside the inherited fleet sentinel");
  for (const key of keys) {
    assert(process.env[key], key + " must be explicitly isolated, not defaulted");
    assert(contained(root, process.env[key]), key + " inherited a fleet path");
  }
  const safe = new Set(keys.filter(key => key.startsWith("PI_FABRIC_")));
  assert.deepEqual(Object.keys(process.env).filter(key => key.startsWith("PI_FABRIC_") && !safe.has(key)), []);
  for (const key of ["SMARTY_ROLE", "HERDR_ENV", "HERDR_SOCKET_PATH", "HERDR_WORKSPACE_ID"])
    assert.equal(process.env[key], undefined, key + " inherited fleet authority");
  return root;
};
const assertChildSnapshot = () => {
  const child = spawnSync(process.execPath, ["-e", 'console.log(JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("PI_FABRIC_") || ["PI_CODING_AGENT_DIR", "SMARTY_ROLE", "HERDR_ENV", "HERDR_SOCKET_PATH", "HERDR_WORKSPACE_ID", "MCPORTER_CONFIG"].includes(key)))))'], { encoding: "utf8", timeout: 3_000 });
  assert.equal(child.status, 0, child.stderr);
  const inherited = JSON.parse(child.stdout);
  for (const key of keys) assert.equal(inherited[key], process.env[key]);
  assert.deepEqual(Object.keys(inherited).sort(), keys.slice().sort());
};
const configRoot = assertIsolated(); // MUST fail on HEAD before any real runtime writes.
assertChildSnapshot();
if (module.default) {
  const config = module.default;
  for (const key of keys) assert.equal(config.test.env[key], process.env[key]);
  assert.equal(config.test.env.TMPDIR, process.env.TMPDIR);
  assert(config.test.setupFiles.includes("./tests/fleet-isolation-setup.ts"));
  // Exercise the actual registered worker setup, not a copy of its implementation.
  await import(pathToFileURL(path.resolve(repo, config.test.setupFiles[0])).href);
  assert.notEqual(assertIsolated(), configRoot, "each test file needs a private fleet root");
}
const root = assertIsolated();
assertChildSnapshot();
const { resolveAgentDir } = await import(pathToFileURL(path.join(repo, "src/core/agent-dir.ts")).href);
const { resolveFabricIdentity } = await import(pathToFileURL(path.join(repo, "src/main-agent.ts")).href);
const { participantRole, participantProject } = await import(pathToFileURL(path.join(repo, "src/topology/project-identity.ts")).href);
const { ResidentActorClient } = await import(pathToFileURL(path.join(repo, "src/residency/actor-client.ts")).href);
const { loadFabricConfig, saveFabricConfig } = await import(pathToFileURL(path.join(repo, "src/config.ts")).href);
const { resolveSessionExportDir } = await import(pathToFileURL(path.join(repo, "src/agents/session-export.ts")).href);
assert.equal(resolveAgentDir(), process.env.PI_CODING_AGENT_DIR);
assert.equal(resolveFabricIdentity("isolated-test").mainAgentId, "session:isolated-test");
assert.equal(resolveFabricIdentity("isolated-test").identity.kind, "main");
assert.equal(participantRole(), undefined);
assert.equal(participantProject(repo), process.env.PI_FABRIC_PROJECT_ROOT);
assert.equal(ResidentActorClient.fromEnv(), undefined);
assert.deepEqual(JSON.parse(fs.readFileSync(process.env.MCPORTER_CONFIG, "utf8")), { mcpServers: {}, imports: [] });
process.env.PI_FABRIC_MAIN_AGENT_ID = "explicit-test-main";
assert.equal(resolveFabricIdentity("isolated-test").mainAgentId, "explicit-test-main");
delete process.env.PI_FABRIC_MAIN_AGENT_ID;
const location = { cwd: process.env.PI_FABRIC_PROJECT_ROOT, agentDir: resolveAgentDir(), projectTrusted: true };
// Real migration and save paths: all containment assertions precede these writes.
fs.writeFileSync(path.join(location.agentDir, "fabric.json"), "{}");
loadFabricConfig(location);
for (const scope of ["global", "project"]) {
  const saved = saveFabricConfig({ ...location, scope }, { mesh: { enabled: false } });
  assert(contained(root, saved.path), "configuration escaped the owned root");
  assert.equal(JSON.parse(fs.readFileSync(saved.path, "utf8")).mesh.enabled, false);
}
assert.equal(resolveSessionExportDir({ sessionExport: true, sessionExportDir: sentinel }), process.env.PI_FABRIC_AGENT_DIR);
assert.equal(fs.readFileSync(path.join(sentinel, "agent", "fabric.json"), "utf8"), baseline);
console.log(JSON.stringify({ root, configRoot, paths: Object.fromEntries(keys.map(key => [key, process.env[key]])) }));
`;

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

test.each(["vitest.config.ts", "tests/fleet-isolation-setup.ts"])(
  "%s fences inherited fleet paths before real source initialization and config writes",
  (entry) => {
    fs.mkdirSync(path.join(repo, ".local"), { recursive: true });
    const sentinel = fs.mkdtempSync(path.join(repo, ".local", "fleet-isolation-sentinel-"));
    try {
      fs.mkdirSync(path.join(sentinel, "agent"));
      fs.writeFileSync(path.join(sentinel, "agent", "fabric.json"), '{"sentinel":"unchanged"}\n');
      const env: NodeJS.ProcessEnv = { ...process.env, FLEET_ISOLATION_SENTINEL: sentinel, FLEET_ISOLATION_ENTRY: path.join(repo, entry) };
      for (const key of ["PI_FABRIC_MESH_ROOT", "PI_FABRIC_PROJECT_ROOT", "PI_FABRIC_PROJECT",
        "PI_FABRIC_RUN_ROOT", "PI_FABRIC_AGENT_DIR", "PI_FABRIC_RESIDENT_CONFIG", "PI_FABRIC_BUDGET_FILE",
        "PI_FABRIC_REPLY_FILE", "PI_FABRIC_REPLY_SCHEMA_FILE", "PI_CODING_AGENT_DIR", "MCPORTER_CONFIG", "HERDR_SOCKET_PATH"])
        env[key] = path.join(sentinel, "agent");
      for (const key of ["PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID",
        "PI_FABRIC_SESSION_ID", "PI_FABRIC_HOST_ID", "PI_FABRIC_IDENTITY_ID", "PI_FABRIC_OWNER_HOST_ID",
        "PI_FABRIC_OWNER_IDENTITY_ID", "PI_FABRIC_ROLE", "PI_FABRIC_CAPABILITY_REQUIREMENTS",
        "PI_FABRIC_CAPABILITY_DIGEST", "PI_FABRIC_GRANTED_RISKS", "PI_FABRIC_TOOL_ALLOWLIST",
        "PI_FABRIC_FUTURE_SELECTOR", "SMARTY_ROLE", "HERDR_WORKSPACE_ID"])
        env[key] = "production-main-sentinel";
      env.HERDR_ENV = "1";
      const child = spawnSync(process.execPath, ["--experimental-transform-types", "--input-type=module", "-e", childProbe], {
        cwd: repo, env, encoding: "utf8", timeout: 10_000,
      });
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr).toBe(0);
      const result = JSON.parse(child.stdout.trim());
      expect(fs.existsSync(result.root)).toBe(false);
      expect(fs.existsSync(result.configRoot)).toBe(false);
      expect(fs.readdirSync(sentinel)).toEqual(["agent"]);
      expect(fs.readdirSync(path.join(sentinel, "agent"))).toEqual(["fabric.json"]);
      expect(fs.readFileSync(path.join(sentinel, "agent", "fabric.json"), "utf8")).toBe('{"sentinel":"unchanged"}\n');
    } finally {
      fs.rmSync(sentinel, { recursive: true, force: true });
    }
  },
);

test("explicit behavior stubs still override the safe defaults", async () => {
  vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", "explicit-test-main");
  try {
    const { resolveFabricIdentity } = await import("../src/main-agent.js");
    expect(resolveFabricIdentity("test").mainAgentId).toBe("explicit-test-main");
  } finally {
    vi.unstubAllEnvs();
  }
});
