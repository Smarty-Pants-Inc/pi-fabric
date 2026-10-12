import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Git Bash (MSYS2) keeps its mount table, with /tmp resolved from the first shell's TEMP, in
// per-user shared memory for as long as any MSYS process lives. A shell a test could not reap
// (a `sleep` orphaned by taskkill /T) pins it, so after one vitest run deletes its temp, every
// Git Bash in the next run starts with "bash.exe: warning: could not find /tmp, please create!"
// on stderr. On a Windows CI runner, reuse one path per job and leave it for the runner
// to clean: no test process owns this shared parent or the lifetime of a pinned /tmp.
// ponytail: a runner runs one job at a time and RUNNER_TEMP is per runner, so the path is private.
const stableRoot = (prefix: string): string | undefined => {
  const runnerTemp = process.env.RUNNER_TEMP;
  if (process.platform !== "win32" || !runnerTemp) return;
  const directory = join(runnerTemp, `${prefix}job`);
  mkdirSync(directory, { recursive: true });
  return directory;
};

/** Tests must never prune a real session's caches or leave their own behind. */
export function isolatedTestTemp(prefix: string): Record<"TMPDIR" | "TMP" | "TEMP", string> {
  const sharedDirectory = stableRoot(prefix);
  if (sharedDirectory) {
    // Config probes and workers share this path; their exit must not delete live siblings.
    return { TMPDIR: sharedDirectory, TMP: sharedDirectory, TEMP: sharedDirectory };
  }
  // ponytail: smarty-dev#7554. Unix socket paths have a 108-byte limit; an agent host's long TMPDIR
  // (/srv/scratch/<user>/smarty-pi-tmp/...) plus nested fixture dirs exceeded it (listen EINVAL, tmux
  // "File name too long"). POSIX-only: Windows keeps its own TEMP and pipes, not path-bound sockets.
  const base = tmpdir();
  const directory = process.platform !== "win32" && base.length > 40
    ? mkdtempSync(join("/tmp", prefix))
    : mkdtempSync(join(base, prefix));
  process.once("exit", () => {
    try {
      rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    } catch {
      // A killed child may still hold a Windows handle; never mask test results.
    }
  });
  return { TMPDIR: directory, TMP: directory, TEMP: directory };
}

// Vitest may reload this module for each isolated file; one process-owned exit hook
// avoids accumulating hundreds of listeners while retaining cleanup for every root.
const fleetRootsKey = Symbol.for("pi-fabric.test-owned-fleet-roots");

// An exact test-only channel, NOT a prefix exemption or production authority.
// Jev owns its strict === "1" opt-ins; activation owns exact native/worker paths.
// PostgreSQL reads its binary directory at import; the shell seam reads a delay
// at first use and accepts only positive integers. Preserve those semantics,
// including missing/off values, without validating existence or rewriting paths.
const testControls = new Set([
  "PI_FABRIC_JEV_LIVE", "PI_FABRIC_JEV_LOCALTERM",
  "PI_FABRIC_ACTIVATION_TEST_PI_BINARY", "PI_FABRIC_ACTIVATION_TEST_WORKER",
  "PI_FABRIC_TEST_PG_BIN", "PI_FABRIC_TEST_PID_DELAY_MS",
]);

/** Run at config evaluation AND before each test file's source imports. */
export function isolateTestFleetEnvironment(): Record<string, string> {
  // Scrub production/future selectors, not the exact test-only controls above.
  // Keep values verbatim: consumers own strict booleans and artifact selection.
  // Unsetting paths alone would fall back to the checkout or the user's Pi profile.
  // smarty-dev#7554: an agent pane's own Herdr pane, Pi session and lane lead must not
  // reach transports, spawner attribution or project-agent resolution under test.
  for (const key of Object.keys(process.env)) {
    const upper = key.toUpperCase();
    if (upper.startsWith("PI_FABRIC_") && !testControls.has(key)) delete process.env[key];
    else if (upper.startsWith("HERDR_")) delete process.env[key];
  }
  for (const key of [
    "PI_CODING_AGENT_DIR", "PI_CODING_AGENT", "PI_SESSION_ID", "PI_SESSION_FILE",
    "SMARTY_ROLE", "SMARTY_LEAD_SESSION", "MCPORTER_CONFIG",
  ]) delete process.env[key];

  // Keep Windows' stable TMP policy, but never share fleet state between workers/files.
  const root = mkdtempSync(join(tmpdir(), "pi-fabric-test-fleet-"));
  const owner = process as typeof process & { [fleetRootsKey]?: Set<string> };
  if (!owner[fleetRootsKey]) {
    const roots = owner[fleetRootsKey] = new Set<string>();
    process.once("exit", () => {
      for (const directory of roots) {
        try {
          rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
        } catch {
          // A killed child may still hold a Windows handle; never mask test results.
        }
      }
    });
  }
  owner[fleetRootsKey]!.add(root);
  const environment = {
    PI_FABRIC_MESH_ROOT: join(root, "mesh"),
    // Writable project state is private; PI_FABRIC_PROJECT is attribution only.
    // Leave it scrubbed so participantProject(cwd) keeps the fixture's real project.
    PI_FABRIC_PROJECT_ROOT: join(root, "project"),
    PI_FABRIC_RUN_ROOT: join(root, "runs"),
    PI_FABRIC_AGENT_DIR: join(root, "exports"),
    PI_CODING_AGENT_DIR: join(root, "agent"),
    // An explicit private MCP layer prevents fallback to the real home config.
    MCPORTER_CONFIG: join(root, "mcporter.json"),
    // Mesh-lock diagnostics write <mesh>/lock-stats each minute and at exit (smarty-dev#6477 L8);
    // suites that list a mesh root must not race that timer. Lock-stats suites opt in explicitly.
    PI_FABRIC_LOCK_STATS: "0",
  };
  for (const directory of ["mesh", "project", "runs", "exports", "agent"]) {
    mkdirSync(join(root, directory), { recursive: true });
  }
  writeFileSync(environment.MCPORTER_CONFIG, '{"mcpServers":{},"imports":[]}\n');
  Object.assign(process.env, environment);
  return environment;
}
