import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
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
// outside hosted CI. Syntactic (TypeScript AST; comments and plain string mentions do not count), not a proof.
// A suite qualifies by importing/requiring a deletion entry module, calling a deletion function (including the resident
// removeActor request, which deletes the actor directory), calling .remove with
// an actor-manager/directory value import (or on a .actors/manager receiver), calling .publishBatch/.prune/.compact with a mesh
// store/bridge import, or spawning/executing fabric-win-remove or a retention CLI with --apply.
export const productDeletionMarkers: readonly string[] = ["actors-cli", "actors/remove-offline",
  "storage/verified-remove", "storage/retention", "storage/scratch", "mesh/event-log"];
const deletionCalls = new Set(["verifiedRemove", "removeActorOffline", "removeActor", "removeEmptyRunRoot", "sweepTempRunRoots",
  "removeUnreferencedRun", "compactTerminalRunEvents", "pruneActorRunArchives", "pruneActorSessionBackups",
  "sweepScratch", "closeScratch", "deleteRootActorTree", "pruneDeadCensusRecords"]);
const meshCalls = new Set(["publishBatch", "prune", "compact"]);
const execCalls = new Set(["spawn", "spawnSync", "exec", "execSync", "execFile", "execFileSync", "fork", "execa"]);

function literals(node: ts.Node, out: string[] = []): string[] {
  if (ts.isStringLiteralLike(node)) out.push(node.text);
  ts.forEachChild(node, (child) => { literals(child, out); });
  return out;
}

/** True when the parsed suite syntactically reaches a product-deletion entrypoint. */
function reachesProductDeletion(path: string, source: string): boolean {
  const file = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const modules: string[] = [];
  const calls: { name: string; receiver: string | undefined; args: string[] }[] = [];
  const visit = (node: ts.Node): void => {
    if (ts.isImportDeclaration(node)) {
      const clause = node.importClause, bindings = clause?.namedBindings;
      const typeOnly = clause?.isTypeOnly || (!clause?.name && bindings && ts.isNamedImports(bindings)
        && bindings.elements.length > 0 && bindings.elements.every((entry) => entry.isTypeOnly));
      if (!typeOnly && ts.isStringLiteralLike(node.moduleSpecifier)) modules.push(node.moduleSpecifier.text);
    } else if (ts.isExportDeclaration(node) && !node.isTypeOnly && node.moduleSpecifier
      && ts.isStringLiteralLike(node.moduleSpecifier)) modules.push(node.moduleSpecifier.text);
    if (ts.isCallExpression(node)) {
      const callee = node.expression;
      const first = node.arguments[0];
      if ((callee.kind === ts.SyntaxKind.ImportKeyword || (ts.isIdentifier(callee) && callee.text === "require"))
        && first && ts.isStringLiteralLike(first)) modules.push(first.text);
      const name = ts.isIdentifier(callee) ? callee.text : ts.isPropertyAccessExpression(callee) ? callee.name.text : "";
      const target = ts.isPropertyAccessExpression(callee) ? callee.expression : undefined;
      const receiver = target && ts.isPropertyAccessExpression(target) ? target.name.text
        : target && ts.isIdentifier(target) ? target.text : undefined;
      if (name) calls.push({ name, receiver, args: node.arguments.flatMap((arg) => literals(arg)) });
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  const imports = (...needles: string[]) => modules.some((m) => {
    const module = m.replace(/\.[cm]?[jt]s$/, "");
    return needles.some((needle) => module === needle || module.endsWith("/" + needle));
  });
  if (imports(...productDeletionMarkers)) return true;
  const managerImport = imports("actors/manager", "actors/directory", "fabric-actors");
  const meshImport = imports("mesh/store", "mesh/bridge");
  return calls.some(({ name, receiver, args }) => deletionCalls.has(name)
    || (name === "remove" && (managerImport || receiver === "actors" || receiver === "manager"))
    || (meshImport && meshCalls.has(name))
    || (execCalls.has(name) && (args.some((arg) => arg.includes("fabric-win-remove"))
      || (args.includes("--apply") && args.some((arg) => arg.includes("retention"))))));
}

/** Sorted `tests/<relative>` paths of `.test.ts` files under testRoot reaching product deletion; [] on GitHub Actions. */
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
        if (reachesProductDeletion(path, source)) {
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
