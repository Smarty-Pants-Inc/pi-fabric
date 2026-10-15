import { join } from "node:path";
import type { ViteUserConfig as UserConfig } from "vitest/config";
import { isolatedTestTemp, isolateTestFleetEnvironment } from "./scripts/test-temp.js";

// Assign the caller-owned run root before Vitest snapshots its environment, forks workers,
// or allocates fleet state. Every later fixture temp directory stays beneath this root.
const temp = isolatedTestTemp("pi-fabric-vitest-");
Object.assign(process.env, temp);
const fleet = isolateTestFleetEnvironment();
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
  "tests/verified-remove-windows.test.ts", // fabric-win-remove.exe deletes by handle, outside Node fs.
  "tests/windows-temp-root-native.test.ts", // diskpart formats a native NTFS VHD mounted outside TMPDIR.
  // smarty-dev#3252/#4664: actual product compaction/pruning and terminal-writer retirement run hosted only.
  // Recorded-call/dry-run companions stay local; GITHUB_ACTIONS still removes every exclusion below.
  "tests/actor-log-store.test.ts", // positive archive pruning executes verifiedRemove.
  "tests/conversation-native-reader.test.ts", // direct successful terminal log replacement.
  "tests/conversation-native-reader-suspension.test.ts",
  "tests/conversation-native-reader-generation.test.ts", // suspended reader checkpoint disposal removes product files.
  "tests/log-cursor-generation.test.ts", // cursor rollover uses the actual terminal compactor.
  "tests/mesh-retention-sweep.test.ts", // positive removeUnreferencedRun helper bypasses the public dry-run.
  "tests/production-worker-retention.test.ts", // real worker finalization and positive run/archive collection.
  "tests/residency-retention.test.ts", // positive resident run/request collection.
  "tests/retention.test.ts", // positive terminal compaction and run/archive deletion.
  "tests/run-retention-compaction.test.ts", // positive compaction, archive prune and run-root sweep.
  "tests/run-directory-mode.test.ts", // real archive replacement and failure-temp removal.
  "tests/worker-run-log.test.ts", // actual compactor and real worker finalization.
  "tests/worker-run-log-legacy-reuse.test.ts", // direct successful terminal log replacement.
  "tests/durable-worker-e2e.test.ts", // real worker completion can retire its owned plain log.
  "tests/judge-worker.test.ts", // judgment manager close retires its owned plain log.
  "tests/model-route-worker.test.ts", // real worker finalization and worktree cleanup.
  "tests/worker-native-session-id.test.ts", // real worker finalization and actor source cleanup.
  "tests/worker-activation-window.test.ts", // real terminal worker and actor log retirement.
  "tests/worker-overload.test.ts", // real terminal manager log retirement.
  "tests/worker-retry-profile.test.ts", // real terminal manager log retirement.
  "tests/worker-pi-bash-guard.test.ts", // real terminal manager log retirement.
  "tests/actor-child-routing-native.test.ts", // real terminal child/actor retirement and cleanup.
  "tests/sender-headers.test.ts", // real terminal manager log retirement.
  "tests/runner-adapters.test.ts", // real process/hosted manager log retirement.
  "tests/durable-fabric-exec.test.ts", // real terminal durable worker log retirement.
  "tests/worker-resident-startup.test.ts", // real terminal startup-probe log retirement.
  "tests/durable-runner.test.ts", // real hosted manager log retirement.
  "tests/worker-model-admission.test.ts", // real terminal manager log retirement.
  "tests/handoff-continuation-worker.test.ts", // real terminal continuation-worker log retirement.
  "tests/durable-worker-routing.test.ts", // real terminal manager log retirement.
  "tests/process-placement.test.ts", // real terminal local-fallback log retirement.
  "tests/worker-profile-extensions.test.ts", // real terminal manager log retirement.
  "tests/residency-windows-main-parity.test.ts", // positive legacy terminal compaction replaces plain logs.
  "tests/worker-child-questions.test.ts", // real worker terminal/cancelled completion.
  "tests/worker-e2e.test.ts", // real worker finalization; also explicitly selected by hosted check.
  "tests/worker-exit-grace.test.ts", // real worker completed/failed terminal paths.
  "tests/worker-interrupted-report.test.ts", // includes normal real-worker terminal paths.
  "tests/worker-steer-ended.test.ts", // completed real-worker run through AgentManager.
  "tests/worker-terminal-task.test.ts", // successful real-worker terminal completion.
  "tests/worker-tool-call-native.test.ts", // real worker terminal success/failure.
  "tests/worker-tool-call-stream.test.ts", // real worker terminal success/failure.
];

/** Unguarded or reverted deletion suites stay hosted-only; GitHub Actions always collects every suite. */
export function productDeletionSuites(): string[] {
  return process.env.GITHUB_ACTIONS === "true" ? [] : [...hostedOnlyDeletionSuites];
}

const localRealDeleteSuites = productDeletionSuites();
if (localRealDeleteSuites.length > 0) {
  console.warn(`smarty-dev#4664: local run skips ${localRealDeleteSuites.length} native/tool or reverted deletion suites`);
}

// Separate policy projects avoid writing a fixture read by unrelated config tests.
const policyConstants = (name: string) => ({
  __FABRIC_HOST_POLICY_PATH__: JSON.stringify(join(temp.TMPDIR, `host-policy-${name}`, "fabric-policy.json")),
});

const sharedTests = {
  environment: "node",
  env: { ...temp, ...fleet },
  setupFiles: ["./tests/fleet-isolation-setup.ts"],
  include: ["tests/**/*.test.ts"],
  ...(localRealDeleteSuites.length > 0 ? { exclude: localRealDeleteSuites } : {}),
  // Real worker processes and cold TypeScript compilers share this suite.
  // Behavioral timeouts and performance budgets remain asserted by tests.
  testTimeout: 15_000,
  restoreMocks: true,
  // ponytail: CI-only retries for load-timing flakes on 2-core hosted runners (three runs of pi-fabric#694 each failed
  // one different timing test). Every retried test is listed by scripts/ci-retry-reporter.ts; smarty-dev#7651 fixes
  // them and removes this retry. Local runs never retry.
  retry: ci ? 2 : 0,
} satisfies NonNullable<UserConfig["test"]>;

export default {
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
        define: policyConstants("missing"),
        test: { ...sharedTests, name: "default", exclude: [...localRealDeleteSuites,
          "tests/session-entry-retention.test.ts", "tests/host-policy.test.ts",
          "tests/process-slice-scope-e2e.test.ts"] },
      },
      {
        define: policyConstants("host-policy"),
        test: { ...sharedTests, name: "host-policy", include: ["tests/host-policy.test.ts"] },
      },
      {
        // smarty-dev#8953: its own policy file, so the staged processSlice never reaches another file's config.
        define: policyConstants("scope-e2e"),
        test: { ...sharedTests, name: "scope-e2e", include: ["tests/process-slice-scope-e2e.test.ts"] },
      },
      {
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
