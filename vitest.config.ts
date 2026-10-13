import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";
import type { ViteUserConfig as UserConfig } from "vitest/config";
import { isolatedTestTemp, isolateTestFleetEnvironment } from "./scripts/test-temp.js";

// Mutate the parent before Vitest snapshots its environment or forks any workers.
const temp = isolatedTestTemp("pi-fabric-vitest-");
Object.assign(process.env, temp);
const fleet = isolateTestFleetEnvironment();
// smarty-dev#7554: fixtures assert 0644/0755 modes; an agent shell's umask 0077 must not decide them.
// Forked workers inherit it. POSIX-only: Windows has no meaningful umask.
if (process.platform !== "win32") process.umask(0o022);

// GitHub Actions sets CI=true; a local CI=0 or CI=false must not enable retries (review c6081609482).
const ci = process.env.CI === "true";

// smarty-dev#4664/#11145: suites whose source references a product-deletion entrypoint are excluded before collection
// outside hosted CI. A broad, conservative text heuristic (comments and quoted strings count), not a proof.
// Substrings: actor removal (actors-cli, ActorManager.remove, remove-offline), storage removers (verifiedRemove,
// retention/retention-cli --apply, scratch), mesh store cleanup (MeshStore, StoreBridgeSide), native Windows helper.
export const productDeletionMarkers: readonly string[] = ["actors-cli", "fabric-actors", "actors/manager", "ActorManager",
  "actors/remove-offline", "removeActorOffline", ".actors.remove(", "manager.remove(", "verifiedRemove",
  "storage/verified-remove", "storage/retention", "storage/scratch", "removeEmptyRunRoot", "sweepTempRunRoots",
  "removeUnreferencedRun", "compactTerminalRunEvents", "pruneActorRunArchives", "pruneActorSessionBackups",
  "sweepScratch", "closeScratch", "deleteRootActorTree", "MeshStore", "StoreBridgeSide", "pruneDeadCensusRecords",
  "mesh/store", "mesh/bridge", "mesh/event-log", "fabric-win-remove"];

/** Sorted `tests/<relative>` paths of `.test.ts` files under testRoot that mention a marker; [] on GitHub Actions. */
export function productDeletionSuites(testRoot = fileURLToPath(new URL("./tests", import.meta.url))): string[] {
  if (process.env.GITHUB_ACTIONS === "true") return [];
  const found: string[] = [];
  const walk = (dir: string): void => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) { // Dirent types never follow symlinks
      const path = join(dir, entry.name);
      if (entry.isSymbolicLink() && statSync(path).isDirectory()) {
        throw new Error(`smarty-dev#4664: cannot scan symlinked test directory: ${path}`);
      }
      if (entry.isDirectory()) walk(path);
      else if (entry.name.endsWith(".test.ts") && (entry.isFile() || entry.isSymbolicLink())) {
        const source = readFileSync(path, "utf8"); // Unreadable sources stop config evaluation, not bypass the scan.
        if (productDeletionMarkers.some((marker) => source.includes(marker))
          || (source.includes("retention") && source.includes("--apply"))) {
          found.push(`tests/${relative(testRoot, path).split(sep).join("/")}`);
        }
      }
    }
  };
  walk(testRoot);
  return found.sort();
}

const localRealDeleteSuites = productDeletionSuites();
if (localRealDeleteSuites.length > 0) {
  console.warn(`smarty-dev#4664: local run skips ${localRealDeleteSuites.length} product-deletion suites`);
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
