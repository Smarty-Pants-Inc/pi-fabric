import fs, { lstatSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";

// Only call immediately after mkdtemp: path ownership is not ownership of a replacement.
const ownedRootCleanup = (directory: string): (() => void) => {
  const pin = lstatSync(directory, { bigint: true });
  const parent = dirname(directory);
  const parentPin = lstatSync(parent, { bigint: true });
  return () => {
    try {
      const current = lstatSync(directory, { bigint: true });
      const currentParent = lstatSync(parent, { bigint: true });
      if (!current.isDirectory() || current.isSymbolicLink()
        || current.dev !== pin.dev || current.ino !== pin.ino
        || currentParent.dev !== parentPin.dev || currentParent.ino !== parentPin.ino) return;
      // Config imports precede the deletion guard: look up the CURRENT wrapped remover.
      fs.rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    } catch {
      // Missing/replaced roots or a child's Windows handle must not mask test results.
    }
  };
};

// Git Bash (MSYS2) keeps its mount table, with /tmp resolved from the first shell's TEMP, in
// per-user shared memory for as long as any MSYS process lives. A shell a test could not reap
// (a `sleep` orphaned by taskkill /T) pins it, so after one vitest run deletes its temp, every
// Git Bash in the next run starts with "bash.exe: warning: could not find /tmp, please create!"
// on stderr. On a Windows CI runner, reuse one path per job and leave it for the runner
// to clean: no test process owns this shared parent or the lifetime of a pinned /tmp.
// RUNNER_TEMP also keeps Windows paths on the runner's checkout volume, so drive-relative
// paths resolve on that drive and temp I/O uses runner storage rather than caller TMPDIR.
// Not PI_FABRIC_-prefixed: isolateTestFleetEnvironment keeps it, so workers and children inherit it.
const jobTempMarker = "PI_TEST_TEMP_JOB_DIR";
const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** True only for a run directory this helper's mkdtemp could have made: a direct, pattern-named child of a
 * `<prefix>job` root, a real directory (not a link), and on POSIX owned by this uid with mode 0700. Windows has no
 * meaningful uid or mode bits; there the name and type checks stand alone. There is no vitest globalSetup creation
 * record, so no job-start time check is made (smarty-dev#11145 r5). */
const ownRunDirectory = (candidate: string, prefix: string): boolean => {
  const jobRoot = dirname(candidate);
  if (basename(jobRoot) !== `${prefix}job`) return false;
  if (!new RegExp(`^${escapeRegExp(prefix)}run-[A-Za-z0-9]{6}$`).test(basename(candidate))) return false;
  try {
    const root = lstatSync(jobRoot);
    const run = lstatSync(candidate);
    if (!root.isDirectory() || root.isSymbolicLink() || !run.isDirectory() || run.isSymbolicLink()) return false;
    if (typeof process.getuid === "function" && (run.uid !== process.getuid() || (run.mode & 0o777) !== 0o700)) return false;
    return true;
  } catch {
    return false;
  }
};

const stableRoot = (prefix: string): string | undefined => {
  const runnerTemp = process.env.RUNNER_TEMP;
  if (process.platform !== "win32" || !runnerTemp) return;
  // Never adopt a directory this run did not create (smarty-dev#11145 r5). The first call makes a fresh private
  // mkdtemp child of `<RUNNER_TEMP>/<prefix>job` and exports it as the marker; vitest.config makes it TMPDIR, so
  // a later call in that process or an inheriting child (a config probe, a worker) stands in it and reuses it
  // without nesting. A marker that is not such a child, or not the current TMPDIR, is ignored: a new child is made.
  // The shared `<prefix>job` root itself is never a run directory, so it never becomes the deletion guard's root.
  const currentTemp = resolve(tmpdir());
  const inherited = process.env[jobTempMarker];
  if (inherited !== undefined && resolve(inherited) === currentTemp && ownRunDirectory(currentTemp, prefix)) return currentTemp;
  const jobRoot = join(resolve(runnerTemp), `${prefix}job`);
  mkdirSync(jobRoot, { recursive: true });
  if (!lstatSync(jobRoot).isDirectory()) {
    throw new Error(`Windows job temp must be a real directory inside RUNNER_TEMP on the runner's checkout volume: ${jobRoot}`);
  }
  const directory = mkdtempSync(join(jobRoot, `${prefix}run-`));
  process.env[jobTempMarker] = directory;
  return directory;
};

/** Tests must never prune a real session's caches or leave their own behind. */
export function isolatedTestTemp(prefix: string): Record<"TMPDIR" | "TMP" | "TEMP", string> {
  if (!prefix || prefix === "." || prefix === ".." || /[/\\]/.test(prefix)) {
    throw new Error("Test temp prefix must be a single name inside the test temp base");
  }
  const sharedDirectory = stableRoot(prefix);
  if (sharedDirectory) {
    // Config probes and workers share this path; their exit must not delete live siblings.
    return { TMPDIR: sharedDirectory, TMP: sharedDirectory, TEMP: sharedDirectory };
  }
  // Outside Windows runner jobs, long paths stay private. Socket fixtures that need shorter
  // paths must use a shorter caller TMPDIR or refuse explicitly; never use a shared /tmp fallback.
  const directory = mkdtempSync(join(resolve(tmpdir()), prefix));
  process.once("exit", ownedRootCleanup(directory));
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
    "SMARTY_ROLE", "SMARTY_AGENT", "SMARTY_LEAD_SESSION", "MCPORTER_CONFIG",
  ]) delete process.env[key];

  // Keep Windows' stable TMP policy, but never share fleet state between workers/files.
  const root = mkdtempSync(join(resolve(tmpdir()), "pi-fabric-test-fleet-"));
  const owner = process as typeof process & { [fleetRootsKey]?: Set<() => void> };
  if (!owner[fleetRootsKey]) {
    const roots = owner[fleetRootsKey] = new Set<() => void>();
    process.once("exit", () => {
      for (const cleanup of roots) cleanup();
    });
  }
  owner[fleetRootsKey]!.add(ownedRootCleanup(root));
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
