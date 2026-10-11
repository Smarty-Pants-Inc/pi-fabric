// smarty-dev#8121: the pinned compiler for the static native helpers (native/toolchain.json).
// `node scripts/native-toolchain.mjs fetch` downloads the pinned zig tarball for this architecture,
// checks its sha256 and unpacks it into a per-user cache (no root, no service). The build uses it only
// when that verified copy is present; otherwise it falls back to the host's gcc/clang, marked unpinned.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

export const TOOLCHAIN = JSON.parse(readFileSync(new URL("../native/toolchain.json", import.meta.url), "utf8"));
const ARCHES = { x64: "x86_64", arm64: "aarch64" };
export const nativeArch = () => ARCHES[process.arch];

const cacheRoot = () => process.env.FABRIC_TOOLCHAIN_CACHE || join(process.env.XDG_CACHE_HOME || join(homedir(), ".cache"), "pi-fabric", "toolchains");
const pin = () => TOOLCHAIN.zig[nativeArch()];
const installDir = () => join(cacheRoot(), `zig-${TOOLCHAIN.zig.version}-${nativeArch()}-${pin().sha256.slice(0, 16)}`);

/** The verified pinned zig binary, or undefined when it was never fetched (or this arch has no pin). */
export const pinnedZig = () => {
  if (process.platform !== "linux" || !pin()) return undefined;
  const dir = installDir();
  // The receipt is written only after the tarball's sha256 matched and it unpacked completely.
  try { if (readFileSync(join(dir, ".verified-sha256"), "utf8").trim() !== pin().sha256) return undefined; }
  catch { return undefined; }
  const zig = join(dir, "zig");
  return existsSync(zig) ? zig : undefined;
};

export async function fetchPinnedZig() {
  if (process.platform !== "linux" || !pin()) throw new Error(`no pinned zig for ${process.platform}/${process.arch}`);
  const ready = pinnedZig();
  if (ready) return ready;
  const response = await fetch(pin().url);
  if (!response.ok) throw new Error(`zig download failed: ${response.status} ${pin().url}`);
  const tarball = Buffer.from(await response.arrayBuffer());
  const actual = createHash("sha256").update(tarball).digest("hex");
  if (actual !== pin().sha256) throw new Error(`zig tarball sha256 ${actual} is not the pinned ${pin().sha256}`);
  mkdirSync(cacheRoot(), { recursive: true });
  const staging = mkdtempSync(join(cacheRoot(), ".zig-"));
  try {
    writeFileSync(join(staging, "zig.tar.xz"), tarball);
    mkdirSync(join(staging, "out"));
    const unpack = spawnSync("tar", ["-xJf", "zig.tar.xz", "--strip-components=1", "-C", "out"], { cwd: staging, encoding: "utf8" });
    if (unpack.status !== 0) throw new Error(`zig unpack failed: ${unpack.stderr || unpack.error?.message}`);
    writeFileSync(join(staging, "out", ".verified-sha256"), `${pin().sha256}\n`);
    rmSync(installDir(), { recursive: true, force: true });
    renameSync(join(staging, "out"), installDir());
  } finally { rmSync(staging, { recursive: true, force: true }); }
  return pinnedZig();
}

if (process.argv[1]?.endsWith("native-toolchain.mjs") && process.argv[2] === "fetch") {
  console.log(await fetchPinnedZig());
}
