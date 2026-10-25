// N12 exact-ca4 SOURCE-reader leg. This is NOT the installed ca4 compiled-artifact gate.
// Same unchanged-source archive/source-loader technique as frozen-f3393715.ts; no edits to it.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type * as LedgerModule from "../../src/mesh/bridge-nats-ledger.js";

export const FROZEN_CA4_LEDGER = Object.freeze({
  commit: "ca4b1482119748394df5125c2a8d2853676f7e04",
  tree: "fd090a6771e33b56614eb8a3fd3ec245868360fb",
  sourceTree: "8a5ed7c4453401b26fc8339ced304eb23da44dc0",
  // Pins EVERY archived source/asset byte, not only the entrypoint. The real custody/fence/
  // mesh-lock/atomic-write/verified-remove graph stays old, including its lazy local imports.
  archiveSha256: "8b7407aafd0d9b3ffb0afe42b81d3ad7703382797a21568090140762e491709f",
  lockSha256: "7b871eab624822df21b6d4628f420ffd09e5d7ac4b62eca5436a129fb4b63ebb",
  ledgerSha256: "02ed87aa5434d591850035a0eb91149151c62fd8711f30bb3d38760e099b60b8",
  custodySha256: "51cbedc15cbebdc7f8095af5d3271d10d0d7a26d59f051b3668729f77e23a197",
});
const ARCHIVE_PATHS = ["src", "package.json", "bun.lock", "tsconfig.json", "config", "native", "proofs",
  "LAWS.bend", "PROOF.bend", "scripts/hosted-only.mjs", "scripts/verified-kernels.mjs"];
const sha256 = (bytes: Buffer): string => createHash("sha256").update(bytes).digest("hex");

/** Caller supplies an empty, test-created temp directory OUTSIDE the checkout. No fetch/install,
 * source rewrite, candidate parser, or current internal-dependency fallback. Only type imports above
 * refer to the candidate. No node_modules are shared: this old reader's exercised lock/ledger graph
 * is source + Node builtins. An unexpected dependency/import failure is a BLOCKER, never a skip.
 * The full source tree is retained rather than guessing a reduced closure of the custody graph.
 */
export async function loadFrozenCa4Ledger(directory: string, repoRoot = path.resolve(import.meta.dirname, "../..")) {
  const repo = fs.realpathSync(repoRoot);
  const git = (...args: string[]): Buffer => execFileSync("git", ["-C", repo, ...args], {
    maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"],
  });
  const pin = (expression: string, expected: string): void => {
    if (git("rev-parse", "--verify", expression).toString().trim() !== expected) {
      throw new Error(`N12 exact-ca4 source prerequisite mismatch: ${expression}`);
    }
  };
  if (fs.realpathSync(git("rev-parse", "--show-toplevel").toString().trim()) !== repo) {
    throw new Error("N12 source repoRoot must be the repository toplevel");
  }
  pin(`${FROZEN_CA4_LEDGER.commit}^{commit}`, FROZEN_CA4_LEDGER.commit);
  pin(`${FROZEN_CA4_LEDGER.commit}^{tree}`, FROZEN_CA4_LEDGER.tree);
  pin(`${FROZEN_CA4_LEDGER.commit}:src`, FROZEN_CA4_LEDGER.sourceTree);
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("N12 source destination must be a real directory");
  const sourceRoot = fs.realpathSync(directory), relative = path.relative(repo, sourceRoot);
  if (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`)) {
    throw new Error("N12 old source must be materialized outside the checkout");
  }
  if (fs.readdirSync(sourceRoot).length) throw new Error("N12 old source destination must be empty");

  // Match the existing frozen-client prerequisite even though this smaller reader shares NO packages.
  const oldLock = git("show", `${FROZEN_CA4_LEDGER.commit}:bun.lock`);
  if (sha256(oldLock) !== FROZEN_CA4_LEDGER.lockSha256 || !oldLock.equals(fs.readFileSync(path.join(repo, "bun.lock")))) {
    throw new Error("N12 exact-ca4 SOURCE-reader requires pinned old/current bun.lock parity; no dependency fallback");
  }
  const archive = git("archive", "--format=tar", FROZEN_CA4_LEDGER.commit, "--", ...ARCHIVE_PATHS);
  if (sha256(archive) !== FROZEN_CA4_LEDGER.archiveSha256) throw new Error("N12 ca4 source archive SHA-256 mismatch");
  execFileSync("tar", ["-xf", "-", "-C", sourceRoot], { input: archive, stdio: ["pipe", "pipe", "pipe"] });
  for (const [file, expected] of [
    ["src/mesh/bridge-nats-ledger.ts", FROZEN_CA4_LEDGER.ledgerSha256],
    ["src/mesh/custody-lock.ts", FROZEN_CA4_LEDGER.custodySha256],
    ["bun.lock", FROZEN_CA4_LEDGER.lockSha256],
  ] as const) {
    if (sha256(fs.readFileSync(path.join(sourceRoot, file))) !== expected) throw new Error(`N12 extracted ca4 hash mismatch: ${file}`);
  }
  if (!oldLock.equals(fs.readFileSync(path.join(repo, "bun.lock")))) throw new Error("N12 bun.lock changed during materialization");
  // No source file is patched (including .js specifiers). Vitest resolves them within OLD src,
  // just as the existing frozen-f3393715 helper does. Missing old files must fail the import.
  const loaded = await import(/* @vite-ignore */ pathToFileURL(path.join(sourceRoot, "src/mesh/bridge-nats-ledger.ts")).href) as typeof LedgerModule;
  if (typeof loaded.openBridgeNatsLedger !== "function" || typeof loaded.bridgeNatsClaimAction !== "function") {
    throw new Error("N12 actual ca4 ledger exports are unavailable");
  }
  return {
    openBridgeNatsLedger: loaded.openBridgeNatsLedger,
    bridgeNatsClaimAction: loaded.bridgeNatsClaimAction,
    provenance: Object.freeze({ leg: "exact-ca4 SOURCE-reader" as const, ...FROZEN_CA4_LEDGER, sourceRoot,
      sharedNodeModules: null, installedCompiledArtifactVerified: false as const }),
  };
}
export type FrozenCa4Ledger = Awaited<ReturnType<typeof loadFrozenCa4Ledger>>;
