import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Git Bash (MSYS2) keeps its mount table, with /tmp resolved from the first shell's TEMP, in
// per-user shared memory for as long as any MSYS process lives. A shell a test could not reap
// (a `sleep` orphaned by taskkill /T) pins it, so after one vitest run deletes its temp, every
// Git Bash in the next run starts with "bash.exe: warning: could not find /tmp, please create!"
// on stderr. On a Windows CI runner, reuse one path per job so a pinned /tmp always exists again.
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
  const directory = stableRoot(prefix) ?? mkdtempSync(join(tmpdir(), prefix));
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

/** Run at config evaluation AND before each test file's source imports. */
export function isolateTestFleetEnvironment(): Record<string, string> {
  // Clear the entire namespace, including future path/identity/capability selectors.
  // Unsetting paths alone would fall back to the checkout or the user's Pi profile.
  for (const key of Object.keys(process.env)) {
    if (key.toUpperCase().startsWith("PI_FABRIC_")) delete process.env[key];
  }
  for (const key of [
    "PI_CODING_AGENT_DIR", "SMARTY_ROLE", "HERDR_ENV", "HERDR_SOCKET_PATH",
    "HERDR_WORKSPACE_ID", "MCPORTER_CONFIG",
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
    PI_FABRIC_PROJECT_ROOT: join(root, "project"),
    PI_FABRIC_PROJECT: join(root, "project"),
    PI_FABRIC_RUN_ROOT: join(root, "runs"),
    PI_FABRIC_AGENT_DIR: join(root, "exports"),
    PI_CODING_AGENT_DIR: join(root, "agent"),
    // An explicit private MCP layer prevents fallback to the real home config.
    MCPORTER_CONFIG: join(root, "mcporter.json"),
  };
  for (const directory of ["mesh", "project", "runs", "exports", "agent"]) {
    mkdirSync(join(root, directory), { recursive: true });
  }
  writeFileSync(environment.MCPORTER_CONFIG, '{"mcpServers":{},"imports":[]}\n');
  Object.assign(process.env, environment);
  return environment;
}
