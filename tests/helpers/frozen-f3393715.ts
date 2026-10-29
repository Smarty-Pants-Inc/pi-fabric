// Real pre-#342 client for mixed-version integration tests. No hand-written wire/provider fixture.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { CapturedToolCatalog } from "../../src/capture/catalog.js";
import type { normalizeFabricConfig } from "../../src/config.js";
import type { FabricRuntimeState } from "../../src/fabric-runtime-state.js";

export const FROZEN_F3393715_COMMIT = "f33937156f587f2479c651ae301de44ec0f0592a";
export const FROZEN_F3393715_SHA256 = Object.freeze({
  "src/providers/agents-message-router.ts": "544971ab607c1ad6d5f0ced43bdc540bea124e3e97c6eaa8e291d4eee1bdd7c8",
  "src/topology/control-plane.ts": "4101689148db8ba6df87e7a719574d9c6f8397c377b3e8f21a144f4ba711c8fe",
  "src/providers/agents-provider.ts": "7bf93ff6f412a94c87159ef58e837a94836d2299a8e78dd545b8166a3fabbed3",
  "src/fabric-runtime-state.ts": "6682fce72c33f4bd3e8caafe732ffae3cff8cd50b0094e94e42b906feea649d8",
  "bun.lock": "7b871eab624822df21b6d4628f420ffd09e5d7ac4b62eca5436a129fb4b63ebb",
});

export interface FrozenF3393715 {
  readonly sourceRoot: string;
  readonly FabricRuntimeState: typeof FabricRuntimeState;
  readonly CapturedToolCatalog: typeof CapturedToolCatalog;
  readonly normalizeFabricConfig: typeof normalizeFabricConfig;
  readonly provenance: {
    readonly commit: typeof FROZEN_F3393715_COMMIT;
    readonly sourceTree: string;
    readonly archiveSha256: string;
    readonly sha256: typeof FROZEN_F3393715_SHA256;
    readonly sharedNodeModules: string;
  };
}

const sha256 = (bytes: Buffer): string => createHash("sha256").update(bytes).digest("hex");

/**
 * Materialize and import the ACTUAL old runtime, catalog and config through Vitest's source loader.
 * `directory` must be an existing, empty, test-created temporary directory outside the checkout.
 * The caller owns shutdown and cleanup; this helper never removes anything, fetches, or installs.
 * Missing local history, changed dependencies or changed frozen bytes fail closed (never skip).
 *
 * All package-local runtime imports resolve inside the archived OLD src tree, including lazy imports
 * and generated verified kernels. Only third-party node_modules are shared, after a byte-for-byte
 * old/current bun.lock comparison. The current-source imports above are erased type-only edges.
 * This intentionally does not import the current runtime or patch classes into it.
 */
export async function loadFrozenF3393715(
  directory: string,
  repoRoot = path.resolve(import.meta.dirname, "../.."),
): Promise<FrozenF3393715> {
  const repo = fs.realpathSync(repoRoot);
  const git = (...args: string[]): Buffer => execFileSync("git", ["-C", repo, ...args], {
    maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"],
  });
  if (fs.realpathSync(git("rev-parse", "--show-toplevel").toString().trim()) !== repo) {
    throw new Error(`Frozen client repoRoot is not a repository toplevel: ${repo}`);
  }
  // No fallback to HEAD/current source and no network recovery for a shallow checkout.
  if (git("rev-parse", "--verify", `${FROZEN_F3393715_COMMIT}^{commit}`).toString().trim() !== FROZEN_F3393715_COMMIT) {
    throw new Error(`Frozen client commit is unavailable: ${FROZEN_F3393715_COMMIT}`);
  }
  if (!fs.lstatSync(directory).isDirectory()) throw new Error("Frozen client destination must be a real directory");
  const sourceRoot = fs.realpathSync(directory);
  const relative = path.relative(repo, sourceRoot);
  if (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`)) {
    throw new Error("Frozen client destination must be outside the checkout");
  }
  if (fs.readdirSync(sourceRoot).length !== 0) throw new Error("Frozen client destination must be empty");

  const oldLock = git("show", `${FROZEN_F3393715_COMMIT}:bun.lock`);
  if (sha256(oldLock) !== FROZEN_F3393715_SHA256["bun.lock"] || !oldLock.equals(fs.readFileSync(path.join(repo, "bun.lock")))) {
    throw new Error("Frozen client requires the pinned old bun.lock and a byte-identical current bun.lock before sharing node_modules");
  }
  const sharedNodeModules = fs.realpathSync(path.join(repo, "node_modules"));
  if (!fs.statSync(sharedNodeModules).isDirectory()) throw new Error("Frozen client requires already-installed node_modules");

  // Keep file contents unchanged. In particular, don't rewrite .js specifiers to current .ts files.
  // hosted-only.mjs is the old source's sole static import outside src. Other assets support its
  // package-root/landlock/native/proof lookups without reaching into the current checkout.
  const archive = git("archive", "--format=tar", FROZEN_F3393715_COMMIT, "--",
    "src", "package.json", "bun.lock", "tsconfig.json", "config", "native", "proofs",
    "LAWS.bend", "PROOF.bend", "scripts/hosted-only.mjs", "scripts/verified-kernels.mjs");
  execFileSync("tar", ["-xf", "-", "-C", sourceRoot], { input: archive, stdio: ["pipe", "pipe", "pipe"] });
  for (const [file, expected] of Object.entries(FROZEN_F3393715_SHA256)) {
    const actual = sha256(fs.readFileSync(path.join(sourceRoot, file)));
    if (actual !== expected) throw new Error(`Frozen client SHA-256 mismatch: ${file}: ${actual} != ${expected}`);
  }
  // Recheck after extraction and before publishing the only edge to the current installation.
  if (!fs.readFileSync(path.join(sourceRoot, "bun.lock")).equals(fs.readFileSync(path.join(repo, "bun.lock")))) {
    throw new Error("Frozen client bun.lock changed while materializing source");
  }
  fs.symlinkSync(sharedNodeModules, path.join(sourceRoot, "node_modules"), process.platform === "win32" ? "junction" : "dir");

  const [runtime, catalog, config] = await Promise.all([
    import(/* @vite-ignore */ pathToFileURL(path.join(sourceRoot, "src/fabric-runtime-state.ts")).href),
    import(/* @vite-ignore */ pathToFileURL(path.join(sourceRoot, "src/capture/catalog.ts")).href),
    import(/* @vite-ignore */ pathToFileURL(path.join(sourceRoot, "src/config.ts")).href),
  ]);
  return {
    sourceRoot,
    FabricRuntimeState: runtime.FabricRuntimeState,
    CapturedToolCatalog: catalog.CapturedToolCatalog,
    normalizeFabricConfig: config.normalizeFabricConfig,
    provenance: {
      commit: FROZEN_F3393715_COMMIT,
      sourceTree: git("rev-parse", `${FROZEN_F3393715_COMMIT}:src`).toString().trim(),
      archiveSha256: sha256(archive), sha256: FROZEN_F3393715_SHA256, sharedNodeModules,
    },
  };
}
