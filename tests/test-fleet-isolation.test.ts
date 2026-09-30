import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
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
const testControls = JSON.parse(process.env.FLEET_ISOLATION_TEST_CONTROLS);
const baseline = fs.readFileSync(path.join(sentinel, "agent", "fabric.json"), "utf8");
const module = await import(pathToFileURL(entry).href);
// These selectors grant WRITE locations; semantic project attribution is deliberately absent.
const keys = ["PI_FABRIC_MESH_ROOT", "PI_FABRIC_PROJECT_ROOT",
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
  assert.equal(process.env.PI_FABRIC_PROJECT, undefined, "inherited semantic project must be scrubbed, not replaced");
  const safe = new Set(keys.filter(key => key.startsWith("PI_FABRIC_")));
  // Test controls are separate from the private writable roots, not fleet authority.
  for (const [key, value] of Object.entries(testControls)) assert.equal(process.env[key], value);
  assert.deepEqual(Object.keys(process.env).filter(key => key.toUpperCase().startsWith("PI_FABRIC_") && !safe.has(key) && !Object.hasOwn(testControls, key)), []);
  for (const key of ["SMARTY_ROLE", "HERDR_ENV", "HERDR_SOCKET_PATH", "HERDR_WORKSPACE_ID"])
    assert.equal(process.env[key], undefined, key + " inherited fleet authority");
  return root;
};
const assertChildSnapshot = () => {
  const child = spawnSync(process.execPath, ["-e", 'console.log(JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([key]) => key.toUpperCase().startsWith("PI_FABRIC_") || ["PI_CODING_AGENT_DIR", "SMARTY_ROLE", "HERDR_ENV", "HERDR_SOCKET_PATH", "HERDR_WORKSPACE_ID", "MCPORTER_CONFIG"].includes(key)))))'], { encoding: "utf8", timeout: 3_000 });
  assert.equal(child.status, 0, child.stderr);
  const inherited = JSON.parse(child.stdout);
  for (const key of keys) assert.equal(inherited[key], process.env[key]);
  for (const [key, value] of Object.entries(testControls)) assert.equal(inherited[key], value);
  assert.deepEqual(Object.keys(inherited).sort(), [...keys, ...Object.keys(testControls)].sort());
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
const { participantRole, participantProject, projectOf, resolveProjectAgent } = await import(pathToFileURL(path.join(repo, "src/topology/project-identity.ts")).href);
const { ResidentActorClient } = await import(pathToFileURL(path.join(repo, "src/residency/actor-client.ts")).href);
const { loadFabricConfig, saveFabricConfig } = await import(pathToFileURL(path.join(repo, "src/config.ts")).href);
const { resolveSessionExportDir } = await import(pathToFileURL(path.join(repo, "src/agents/session-export.ts")).href);
assert.equal(resolveAgentDir(), process.env.PI_CODING_AGENT_DIR);
assert.equal(resolveFabricIdentity("isolated-test").mainAgentId, "session:isolated-test");
assert.equal(resolveFabricIdentity("isolated-test").identity.kind, "main");
assert.equal(participantRole(), undefined);
assert.equal(participantProject(repo), projectOf(repo));
const { MeshStore } = await import(pathToFileURL(path.join(repo, "src/mesh/store.ts")).href);
const { ParticipantDirectory } = await import(pathToFileURL(path.join(repo, "src/topology/participant-directory.ts")).href);
const identity = { id: "session:isolated-test", name: "main", kind: "main", sessionId: "isolated-test" };
const directory = new ParticipantDirectory(new MeshStore(process.env.PI_FABRIC_MESH_ROOT, 65536, 1000), {
  enabled: false, hostId: identity.id, rootId: identity.id, identity,
});
const main = { id: identity.id, cwd: repo, status: "idle", startedAt: 1, updatedAt: 2, pendingMessages: false };
assert.equal(directory.root(main).role, undefined);
assert.equal(directory.root(main).project, projectOf(repo));
// Native role and semantic project overrides set AFTER the boundary must still work.
process.env.SMARTY_ROLE = "project-agent@test";
assert.equal(participantRole(), "project-agent");
const native = directory.root(main);
assert.equal(native.role, "project-agent");
assert.equal(native.project, projectOf(repo));
const mirror = { ...native, id: "session:mirror", startedAt: 99, remoteHost: "forge" };
assert.equal(resolveProjectAgent([native, mirror], participantProject(repo)).id, identity.id);
assert.throws(() => resolveProjectAgent([mirror], participantProject(repo)), /No live project agent/);
process.env.PI_FABRIC_PROJECT = process.env.PI_FABRIC_PROJECT_ROOT;
assert.equal(participantProject(repo), projectOf(process.env.PI_FABRIC_PROJECT_ROOT));
assert.equal(directory.root(main).project, projectOf(process.env.PI_FABRIC_PROJECT_ROOT));
process.env.PI_FABRIC_ROLE = "worktree-agent";
assert.equal(participantRole(), "worktree-agent");
delete process.env.PI_FABRIC_ROLE;
delete process.env.PI_FABRIC_PROJECT;
delete process.env.SMARTY_ROLE;
assert.equal(participantRole(), undefined);
assert.equal(participantProject(repo), projectOf(repo));
assertIsolated();
assertChildSnapshot();
assert.equal(ResidentActorClient.fromEnv(), undefined);
assert.deepEqual(JSON.parse(fs.readFileSync(process.env.MCPORTER_CONFIG, "utf8")), { mcpServers: {}, imports: [] });
process.env.PI_FABRIC_MAIN_AGENT_ID = "explicit-test-main";
assert.equal(resolveFabricIdentity("isolated-test").mainAgentId, "explicit-test-main");
delete process.env.PI_FABRIC_MAIN_AGENT_ID;
const location = { cwd: process.env.PI_FABRIC_PROJECT_ROOT, agentDir: resolveAgentDir(), projectTrusted: true };
// Config APIs write to their supplied cwd, NOT semantic projectOf(repo): keep it private.
// Real migration and save paths: all containment assertions precede these writes.
fs.mkdirSync(path.join(location.cwd, ".pi"), { recursive: true });
const configPaths = [path.join(location.agentDir, "fabric.json"), path.join(location.cwd, ".pi", "fabric.json")];
for (const configPath of configPaths) {
  assert(contained(root, configPath), "migration target escaped the owned root");
  fs.writeFileSync(configPath, "{}");
}
loadFabricConfig(location);
for (const configPath of configPaths) {
  assert(JSON.parse(fs.readFileSync(configPath, "utf8")).configVersion > 0, "real legacy configuration was not migrated");
}
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
    const sentinel = fs.mkdtempSync(path.join(tmpdir(), "fleet-isolation-sentinel-"));
    try {
      fs.mkdirSync(path.join(sentinel, "agent"));
      fs.writeFileSync(path.join(sentinel, "agent", "fabric.json"), '{"sentinel":"unchanged"}\n');
      const testControls = {
        PI_FABRIC_JEV_LIVE: "1", PI_FABRIC_JEV_LOCALTERM: "1",
        PI_FABRIC_ACTIVATION_TEST_PI_BINARY: "./exact artifact/native.exe",
        PI_FABRIC_ACTIVATION_TEST_WORKER: "./exact artifact/worker.js",
        PI_FABRIC_TEST_PG_BIN: "./exact artifact/pg bin", PI_FABRIC_TEST_PID_DELAY_MS: "600",
      };
      const env: NodeJS.ProcessEnv = { ...process.env, ...testControls, FLEET_ISOLATION_SENTINEL: sentinel,
        FLEET_ISOLATION_ENTRY: path.join(repo, entry), FLEET_ISOLATION_TEST_CONTROLS: JSON.stringify(testControls) };
      for (const key of ["PI_FABRIC_MESH_ROOT", "PI_FABRIC_PROJECT_ROOT", "PI_FABRIC_PROJECT",
        "PI_FABRIC_RUN_ROOT", "PI_FABRIC_AGENT_DIR", "PI_FABRIC_RESIDENT_CONFIG", "PI_FABRIC_BUDGET_FILE",
        "PI_FABRIC_REPLY_FILE", "PI_FABRIC_REPLY_SCHEMA_FILE", "PI_CODING_AGENT_DIR", "MCPORTER_CONFIG", "HERDR_SOCKET_PATH"])
        env[key] = path.join(sentinel, "agent");
      for (const key of ["PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID",
        "PI_FABRIC_SESSION_ID", "PI_FABRIC_HOST_ID", "PI_FABRIC_IDENTITY_ID", "PI_FABRIC_OWNER_HOST_ID",
        "PI_FABRIC_OWNER_IDENTITY_ID", "PI_FABRIC_ROLE", "PI_FABRIC_CAPABILITY_REQUIREMENTS",
        "PI_FABRIC_CAPABILITY_DIGEST", "PI_FABRIC_GRANTED_RISKS", "PI_FABRIC_TOOL_ALLOWLIST",
        "PI_FABRIC_FUTURE_SELECTOR", "PI_FABRIC_PI_BINARY", "PI_FABRIC_NODE_BINARY",
        "PI_FABRIC_PROFILE", "PI_FABRIC_JEV_LIVE_EXTRA", "PI_FABRIC_ACTIVATION_TEST_WORKER_EXTRA",
        "PI_FABRIC_TEST_FUTURE", "PI_FABRIC_TEST_PG_BIN_EXTRA", "pi_fabric_future_case_selector",
        "SMARTY_ROLE", "HERDR_WORKSPACE_ID"])
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
    const { participantRole, participantProject, projectOf } = await import("../src/topology/project-identity.js");
    vi.stubEnv("SMARTY_ROLE", "project-agent@test");
    expect(participantRole()).toBe("project-agent");
    vi.stubEnv("PI_FABRIC_PROJECT", process.env.PI_FABRIC_PROJECT_ROOT!);
    expect(participantProject(repo)).toBe(projectOf(process.env.PI_FABRIC_PROJECT_ROOT!));
    vi.stubEnv("PI_FABRIC_ROLE", "worktree-agent");
    expect(participantRole()).toBe("worktree-agent");
  } finally {
    vi.unstubAllEnvs();
  }
});
