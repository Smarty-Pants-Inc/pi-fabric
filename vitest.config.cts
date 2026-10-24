type UserConfig = import("vitest/config").ViteUserConfig;

// Vite loads a CommonJS config bundle in memory; ESM configs emit into node_modules/.vite-temp
// before cacheDir can take effect. Keep this entry .cts so plain `vitest run` is read-only-install safe.
const { join } = require("node:path") as typeof import("node:path");
const { hostedOnly } = require("./scripts/hosted-only.mjs") as typeof import("./scripts/hosted-only.mjs");
const { isolatedTestTemp, isolateTestFleetEnvironment } = require("./scripts/test-temp.ts") as typeof import("./scripts/test-temp.js");

// Assign the caller-owned run root before Vitest snapshots its environment, forks workers,
// or allocates fleet state. Every later fixture temp directory stays beneath this root.
const temp = isolatedTestTemp("pi-fabric-vitest-");
Object.assign(process.env, temp);
const fleet = isolateTestFleetEnvironment();
// Vitest appends vitest/<project hash>/results.json. The pinned run root already falls back to
// os.tmpdir() when TMPDIR is absent (including Windows); teardown owns the cache's lifetime too.
const cacheDir = join(temp.TMPDIR, "vite");
// smarty-dev#7554: fixtures assert 0644/0755 modes; an agent shell's umask 0077 must not decide them.
// Forked workers inherit it. POSIX-only: Windows has no meaningful umask.
if (process.platform !== "win32") process.umask(0o022);

// GitHub Actions sets CI=true; a local CI=0 or CI=false must not enable retries (review c6081609482).
const ci = process.env.CI === "true";

// smarty-dev#4664/#11145: fleet-isolation-setup installs the Node fs deletion guard before suite imports;
// Node children inherit its pinned TMPDIR ceiling via NODE_OPTIONS --import. Native/tool deletion that
// bypasses that seam stays hosted-only, as do suites restored by the revert rule below. New native
// deletions or deleting children that drop/ignore NODE_OPTIONS (including Bun, smarty-dev#13001)
// must be added here. flock/setpriv only lock: their callers still delete through guarded Node fs.
// Revert rule: any deletion outside TMPDIR by a moved suite restores that suite's exclusion.
// ponytail: reviewed explicit denylist, not an import-name detector that hides guardable suites.
const hostedOnlyDeletionSuites = [
  "tests/actor-child-completion-idle-preflight.test.ts", // Bun enqueue releases a claim lock through unguarded fs.
  "tests/actor-manager.test.ts", // real src/worker.ts launch selects Bun; its cleanup is not guarded.
  "tests/actor-remove-dead-root.test.ts", // revert rule: real /dev/shm cross-device fixture lies outside TMPDIR.
  "tests/agent-manager.test.ts", // worktree cleanup delegates deletion to git worktree remove --force.
  "tests/agent-release-observation.test.ts", // successful cleanup removes a real worktree through git.
  "tests/agents-provider.test.ts", // public cleanup's worktree=true cases invoke git worktree remove.
  "tests/residency-commit-fence.test.ts", // direct Bun owner fixture deletes participant/lock/census state.
  "tests/residency-stopped-writer-retention.test.ts", // durable cleanup removes a real worktree through git.
  "tests/residency.test.ts", // restored/tampered worktree cleanup and fixture teardown invoke git removal.
  "tests/routed-session-boundaries.test.ts", // rollback and successful cleanup invoke git worktree remove.
  "tests/stall-alarms.test.ts", // nice/bun crash fixture reaches MeshStore/lock cleanup before its crash.
  "tests/task-write-policy.test.ts", // sentinel run's native proof timed out; unproven slice stays hosted-only (#13018).
  "tests/temp-run-sweep.test.ts", // scriptSpawnArgs selects Bun for the real .ts sweep entry.
  "tests/mesh-state-windows-acl.test.ts", // native ACL fixtures are created/removed below USERPROFILE, outside TMPDIR.
  "tests/verified-remove-windows.test.ts", // fabric-win-remove.exe deletes by handle, outside Node fs.
  "tests/windows-temp-root-native.test.ts", // diskpart formats a native NTFS VHD mounted outside TMPDIR.
];

/** Unguarded or reverted deletion suites require a GitHub-hosted runner with no fleet veto. */
function productDeletionSuites(): string[] {
  return hostedOnly("native/tool or reverted deletion suites") ? [] : [...hostedOnlyDeletionSuites];
}

const localRealDeleteSuites = productDeletionSuites();

// Generic CI checks do not own a broker. Exclude whole broker-capable suites before
// affected-test discovery (including their fake cases); dedicated nats-* jobs run them.
// Fake-only NATS suites stay in the generic cross-platform selection.
const realBrokerNatsSuites = [
  "tests/mesh-state-backend.test.ts",
  "tests/mesh-state-backend-multiprocess.test.ts",
  "tests/mesh-backend-migration-nats.test.ts",
  "tests/state-backend-nats-election.test.ts",
  "tests/state-backend-nats-publish-dedupe.test.ts",
  "tests/state-backend-nats-pull-recovery.test.ts",
  "tests/nats-auth-server.test.ts",
];
const excludedSuites = [...localRealDeleteSuites,
  ...(process.env.FABRIC_TEST_SKIP_NATS === "1" ? realBrokerNatsSuites : [])];

// Separate policy projects avoid writing a fixture read by unrelated config tests.
const policyConstants = (name: string) => ({
  __FABRIC_HOST_POLICY_PATH__: JSON.stringify(join(temp.TMPDIR, `host-policy-${name}`, "fabric-policy.json")),
});

const sharedTests = {
  environment: "node",
  env: { ...temp, ...fleet },
  setupFiles: ["./tests/fleet-isolation-setup.ts"],
  include: ["tests/**/*.test.ts"],
  ...(excludedSuites.length > 0 ? { exclude: excludedSuites } : {}),
  // Real worker processes and cold TypeScript compilers share this suite.
  // Behavioral timeouts and performance budgets remain asserted by tests.
  testTimeout: 15_000,
  restoreMocks: true,
  // ponytail: CI-only retries for load-timing flakes on 2-core hosted runners (three runs of pi-fabric#694 each failed
  // one different timing test). Every retried test is listed by scripts/ci-retry-reporter.ts; smarty-dev#7651 fixes
  // them and removes this retry. Local runs never retry.
  retry: ci ? 2 : 0,
} satisfies NonNullable<UserConfig["test"]>;

module.exports = {
  cacheDir,
  test: {
    ...sharedTests,
    reporters: ci ? ["default", "./scripts/ci-retry-reporter.ts"] : ["default"],
    // Root config only: teardown removes this process's created root on pass and failure.
    // The exit fallback keeps the same inode pins; Windows job temp stays runner-owned.
    globalSetup: ["./scripts/test-temp-global-setup.ts"],
    maxWorkers: 2,
    // Expose GC only for retention tests: application worker_threads reject this flag.
    // Explicit shared options avoid extends merging the broad include into the GC project.
    projects: [
      {
        cacheDir,
        define: policyConstants("missing"),
        test: { ...sharedTests, name: "default", exclude: [...excludedSuites,
          "tests/session-entry-retention.test.ts", "tests/host-policy.test.ts",
          "tests/process-slice-scope-e2e.test.ts"] },
      },
      {
        cacheDir,
        define: policyConstants("host-policy"),
        test: { ...sharedTests, name: "host-policy", include: ["tests/host-policy.test.ts"] },
      },
      {
        cacheDir,
        // smarty-dev#8953: its own policy file, so the staged processSlice never reaches another file's config.
        define: policyConstants("scope-e2e"),
        test: { ...sharedTests, name: "scope-e2e", include: ["tests/process-slice-scope-e2e.test.ts"] },
      },
      {
        cacheDir,
        define: policyConstants("missing"),
        test: {
          ...sharedTests,
          name: "history-gc", include: ["tests/session-entry-retention.test.ts"],
          pool: "forks", execArgv: ["--expose-gc"],
        },
      },
    ],
  },
} satisfies UserConfig;

module.exports.productDeletionSuites = productDeletionSuites;
