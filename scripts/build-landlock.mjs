// Linux-only native artifacts: the Landlock adapter and the mesh lease probe (smarty-dev#7936).
// Other platforms keep bash unchanged and the readiness gate fails closed.
// Each helper is built TWICE into separate temporary directories and must be byte-identical; the
// compiler identity and the sha256 values go to dist/native/manifest.json, which
// assert-build-artifacts.mjs checks. smarty-dev#8121: with the pinned zig from native/toolchain.json
// (scripts/native-toolchain.mjs fetch) every host and CI build the same bytes, which must equal the
// expected sha256 for that toolchain. Without it the host's gcc or clang builds them, marked unpinned.
// Either way the flags are fixed (CFLAGS, LDFLAGS and compiler search variables are ignored).
import { createHash } from "node:crypto";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { nativeArch, pinnedTarball, TOOLCHAIN, withPinnedZig } from "./native-toolchain.mjs";

export const NATIVE_HELPERS = ["fabric-landlock", "fabric-mesh-lease"];
// smarty-dev#7858: the Windows verified-removal helper, cross-built by the pinned zig (on Linux, shipped in the
// package; on the Windows CI job, with the pinned Windows zig in FABRIC_WIN_ZIG). One expected sha256 for both.
export const WINDOWS_HELPER = "fabric-win-remove.exe";
const winCompile = (root) => ["cc", "-target", "x86_64-windows-gnu", "-std=c11", "-O2", "-g0", "-Wall", "-Wextra", "-Werror",
  "-municode", "-s", `-ffile-prefix-map=${root}=.`];
const winBuild = (zig, env, root) => (source, output) =>
  spawnSync(zig, [...winCompile(root), source, "-ladvapi32", "-o", output], { encoding: "utf8", env });

const flags = (root) => [
  "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-static",
  "-nostdlib", "-ffreestanding", "-fno-builtin", "-fno-stack-protector",
  "-fno-pie", "-no-pie", `-ffile-prefix-map=${root}=.`, "-Wl,--build-id=none",
];
// zig cc drops its bundled Linux headers under -nostdlib, so it compiles and links in two steps:
// the same freestanding object, then a static link without libc or start files.
const zigCompile = (root, arch) => ["cc", "-target", `${arch}-linux-musl`, "-std=c11", "-O2", "-g0", "-Wall", "-Wextra", "-Werror",
  "-ffreestanding", "-fno-builtin", "-fno-stack-protector", "-fno-pie", `-ffile-prefix-map=${root}=.`, "-c"];
const zigLink = (arch) => ["cc", "-target", `${arch}-linux-musl`, "-static", "-nostdlib", "-no-pie", "-Wl,--build-id=none"];

const sha256 = (file) => createHash("sha256").update(readFileSync(file)).digest("hex");

/** Returns the sha256 of each helper ({} off Linux): build.mjs embeds them into the bundle. */
export function buildLandlock() {
  if (process.platform === "win32" && process.env.FABRIC_WIN_ZIG) {
    const zig = process.env.FABRIC_WIN_ZIG;
    const zigVersion = spawnSync(zig, ["version"], { encoding: "utf8" }).stdout?.trim();
    if (zigVersion !== TOOLCHAIN.zig.version) throw new Error(`FABRIC_WIN_ZIG ${zig} is not zig ${TOOLCHAIN.zig.version} (${zigVersion})`);
    const env = { ...process.env, SOURCE_DATE_EPOCH: "0" };
    return buildHelpers(`zig ${zigVersion} (windows host)`, true, winCompile("<root>"), undefined, "x86_64",
      { name: WINDOWS_HELPER, build: winBuild(zig, env, resolve(".")) }, []);
  }
  if (process.platform !== "linux") return {};
  const root = resolve(".");
  const env = { ...process.env, SOURCE_DATE_EPOCH: "0", LC_ALL: "C" };
  for (const name of ["CFLAGS", "CPPFLAGS", "LDFLAGS", "CPATH", "C_INCLUDE_PATH", "LIBRARY_PATH", "GCC_EXEC_PREFIX", "COMPILER_PATH"]) delete env[name];
  const arch = nativeArch();
  const tarball = pinnedTarball();
  if (tarball.path) {
    return withPinnedZig(tarball.path, (zig, zigEnv) => {
      const pinnedEnv = { ...env, ...zigEnv };
      const clang = spawnSync(zig, ["cc", "--version"], { encoding: "utf8", env: pinnedEnv });
      const zigVersion = spawnSync(zig, ["version"], { encoding: "utf8", env: pinnedEnv }).stdout?.trim();
      if (clang.status !== 0 || zigVersion !== TOOLCHAIN.zig.version) throw new Error(`pinned zig ${zig} is not usable (version ${zigVersion})`);
      const compiler = `zig ${zigVersion} (${clang.stdout.split("\n")[0].trim()}; tarball sha256 ${TOOLCHAIN.zig[arch].sha256})`;
      return buildHelpers(compiler, true, [...zigCompile("<root>", arch), "&&", ...zigLink(arch)], (source, output) => {
        const compiled = spawnSync(zig, [...zigCompile(root, arch), source, "-o", `${output}.o`], { encoding: "utf8", env: pinnedEnv });
        return compiled.status !== 0 || compiled.error ? compiled
          : spawnSync(zig, [...zigLink(arch), `${output}.o`, "-o", output], { encoding: "utf8", env: pinnedEnv });
      }, arch, { name: WINDOWS_HELPER, build: winBuild(zig, pinnedEnv, root) });
    });
  }
  if (process.env.FABRIC_NATIVE_CC_PINNED === "1") throw new Error(`FABRIC_NATIVE_CC_PINNED=1 but ${tarball.reason}`);
  console.warn(`[pi-fabric] native helpers built UNPINNED with the host compiler: ${tarball.reason} (smarty-dev#8121)`);
  const cc = process.env.CC || "cc";
  // `cc -v` names the driver for real ("gcc version ..." or "... clang version ..."); --version only echoes argv[0].
  const version = spawnSync(cc, ["-v"], { encoding: "utf8", env });
  const compiler = `${version.stdout ?? ""}\n${version.stderr ?? ""}`.split("\n").map(line => line.trim())
    .find(line => /^(gcc version |(\S+ )?clang version )/.test(line)) ?? "";
  if (version.error || version.status !== 0 || compiler === "") {
    throw new Error(`native helper build refuses compiler ${cc} (${compiler || version.error?.message || `exit ${version.status}`}): gcc or clang only`);
  }
  return buildHelpers(compiler, false, flags("<root>"), (source, output) => spawnSync(cc, [...flags(root), source, "-o", output], { encoding: "utf8", env }), arch);
}

function buildHelpers(compiler, pinned, recordedFlags, build, arch, windows, linuxHelpers = NATIVE_HELPERS) {
  mkdirSync("dist/native", { recursive: true });
  const helpers = {};
  for (const helper of [...linuxHelpers, ...windows ? [windows.name] : []]) {
    const windowsHelper = helper === windows?.name;
    const hashes = [];
    const outputs = [];
    try {
      for (const attempt of ["a", "b"]) {
        const dir = mkdtempSync(join(tmpdir(), `fabric-native-${helper}-${attempt}-`));
        outputs.push(dir);
        const output = join(dir, helper);
        const result = (windowsHelper ? windows.build : build)(`native/${windowsHelper ? helper.replace(/\.exe$/, "") : helper}.c`, output);
        if (result.error || result.status !== 0) {
          throw new Error(`${helper} build failed: install a Linux C compiler and Linux syscall headers (x86_64/aarch64). ${result.error?.message || result.stderr}`);
        }
        hashes.push(sha256(output));
      }
      if (hashes[0] !== hashes[1]) throw new Error(`${helper} is not reproducible: two builds differ (${hashes[0]} vs ${hashes[1]}) with ${compiler}`);
      // The pinned toolchain has one known output per architecture: any other bytes fail the build.
      const expected = pinned ? (windowsHelper ? TOOLCHAIN.expected.windows?.[helper] : TOOLCHAIN.expected[arch]?.[helper]) : undefined;
      if (pinned && expected !== hashes[0]) {
        throw new Error(`${helper} sha256 ${hashes[0]} is not the expected ${expected} for ${compiler} (native/toolchain.json)`);
      }
      copyFileSync(join(outputs[0], helper), `dist/native/${helper}`);
    } finally { for (const dir of outputs) rmSync(dir, { recursive: true, force: true }); }
    helpers[helper] = hashes[0];
  }
  // For assert-build-artifacts.mjs only; the runtime trusts the digests embedded into the bundle.
  writeFileSync("dist/native/manifest.json", `${JSON.stringify({ compiler, toolchain: pinned ? "pinned" : "unpinned",
    flags: recordedFlags, helpers }, null, 2)}\n`);
  return helpers;
}

if (process.argv[1]?.endsWith("build-landlock.mjs")) buildLandlock();
