// smarty-dev#8121: the pinned compiler for the static native helpers (native/toolchain.json).
// `node scripts/native-toolchain.mjs fetch` downloads the pinned zig tarball for this architecture, checks its
// sha256 and keeps it in a private per-user cache (no root, no service). Each build re-reads that tarball into
// memory, checks the sha256 again and unpacks it into a fresh private temp dir: nothing unpacked or written
// into the cache is trusted (pi-fabric#879 review). Without a usable tarball the build falls back to the
// host's gcc/clang, marked unpinned.
import { createHash } from "node:crypto";
import fs, { chmodSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

export const TOOLCHAIN = JSON.parse(readFileSync(new URL("../native/toolchain.json", import.meta.url), "utf8"));
const ARCHES = { x64: "x86_64", arm64: "aarch64" };
export const nativeArch = () => ARCHES[process.arch];

export const cacheRoot = () => resolve(process.env.FABRIC_TOOLCHAIN_CACHE ||
  join(process.env.XDG_CACHE_HOME || join(homedir(), ".cache"), "pi-fabric", "toolchains"));
const pin = () => TOOLCHAIN.zig[nativeArch()];
const tarballPath = () => join(cacheRoot(), `zig-${TOOLCHAIN.zig.version}-${nativeArch()}-${pin().sha256}.tar.xz`);
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");

/**
 * The cache root and every ancestor up to the first root-owned one must be real directories (no symlink),
 * owned by this uid and not group- or other-writable: no other user can plant or swap the tarball.
 * Returns why not, or undefined when the chain holds.
 */
export const untrustedCache = (root = cacheRoot(), stat = lstatSync) => {
  const uid = process.getuid?.();
  for (let dir = root; ; dir = dirname(dir)) {
    let info;
    try { info = stat(dir); } catch (error) { return `${dir}: ${error.code ?? error.message}`; }
    if (info.isSymbolicLink() || !info.isDirectory()) return `${dir} is not a real directory`;
    const writable = (info.mode & 0o022) !== 0;
    // ponytail: the one exception is a root-owned sticky directory (/tmp): others may add entries there but
    // cannot rename or remove ours. Any other group/other-writable ancestor could swap a directory below it.
    if (writable && !(info.uid === 0 && dir !== root && (info.mode & 0o1000) !== 0)) {
      return `${dir} is group- or other-writable (mode ${(info.mode & 0o7777).toString(8)})`;
    }
    if (info.uid === 0 && dir !== root) return undefined; // a system ancestor: the chain below it is ours
    if (info.uid !== uid) return `${dir} is owned by uid ${info.uid}, not ${uid}`;
    if (dirname(dir) === dir) return undefined;
  }
};

/** The cached tarball path when its directory chain is private, or the reason it is not usable. */
export const pinnedTarball = () => {
  if (process.platform !== "linux" || !pin()) return { reason: `no pinned zig for ${process.platform}/${process.arch}` };
  const reason = untrustedCache();
  if (reason) return { reason: `untrusted toolchain cache: ${reason}` };
  try { lstatSync(tarballPath()); } catch { return { reason: `pinned zig ${TOOLCHAIN.zig.version} not fetched (run node scripts/native-toolchain.mjs fetch)` }; }
  return { path: tarballPath() };
};

/**
 * Runs `use(zig, env)` with the pinned zig unpacked from the sha256-verified tarball into a fresh private
 * temp dir, with zig's own caches inside it, then removes it. Throws when the bytes are not the pin.
 */
export const withPinnedZig = (tarball, use) => {
  const bytes = readFileSync(tarball); // hash and unpack the SAME bytes: no swap between check and use
  const actual = sha256(bytes);
  if (actual !== pin().sha256) throw new Error(`cached zig tarball sha256 ${actual} is not the pinned ${pin().sha256}`);
  const work = mkdtempSync(join(tmpdir(), "fabric-zig-"));
  try {
    chmodSync(work, 0o700);
    writeFileSync(join(work, "zig.tar.xz"), bytes, { mode: 0o600 });
    mkdirSync(join(work, "zig"), { mode: 0o700 });
    const unpack = spawnSync("tar", ["-xJf", "zig.tar.xz", "--strip-components=1", "--no-same-owner", "-C", "zig"], { cwd: work, encoding: "utf8" });
    if (unpack.status !== 0) throw new Error(`zig unpack failed: ${unpack.stderr || unpack.error?.message}`);
    // zig's global and local caches hold compiled runtime pieces: keep them private to this build too.
    const env = { ZIG_GLOBAL_CACHE_DIR: join(work, "cache"), ZIG_LOCAL_CACHE_DIR: join(work, "cache") };
    return use(join(work, "zig", "zig"), env);
  } finally { fs.rmSync(work, { recursive: true, force: true }); }
};

export async function fetchPinnedZig() {
  if (process.platform !== "linux" || !pin()) throw new Error(`no pinned zig for ${process.platform}/${process.arch}`);
  // Create the cache levels private (umask-proof), then require the whole chain to be private.
  mkdirSync(cacheRoot(), { recursive: true, mode: 0o700 });
  chmodSync(cacheRoot(), 0o700);
  if (!process.env.FABRIC_TOOLCHAIN_CACHE) chmodSync(dirname(cacheRoot()), 0o700); // our own pi-fabric level
  const reason = untrustedCache();
  if (reason) throw new Error(`untrusted toolchain cache: ${reason}`);
  try { if (sha256(readFileSync(tarballPath())) === pin().sha256) return tarballPath(); } catch { /* fetch below */ }
  const response = await fetch(pin().url);
  if (!response.ok) throw new Error(`zig download failed: ${response.status} ${pin().url}`);
  const tarball = Buffer.from(await response.arrayBuffer());
  const actual = sha256(tarball);
  if (actual !== pin().sha256) throw new Error(`zig tarball sha256 ${actual} is not the pinned ${pin().sha256}`);
  const staging = `${tarballPath()}.${process.pid}.tmp`;
  writeFileSync(staging, tarball, { mode: 0o600 });
  renameSync(staging, tarballPath());
  return tarballPath();
}

if (process.argv[1]?.endsWith("native-toolchain.mjs") && process.argv[2] === "fetch") {
  console.log(await fetchPinnedZig());
}
