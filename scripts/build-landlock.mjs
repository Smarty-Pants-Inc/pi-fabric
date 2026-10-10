// Linux-only native artifacts: the Landlock adapter and the mesh lease probe (smarty-dev#7936).
// Other platforms keep bash unchanged and the readiness gate fails closed.
// Each helper is built TWICE into separate temporary directories and must be byte-identical; the
// compiler identity and the sha256 values go to dist/native/manifest.json, which
// assert-build-artifacts.mjs checks. ponytail: an exact compiler pin across hosts is a follow-up; until
// then the build pins its flags (CFLAGS, LDFLAGS and compiler search variables from the environment are
// ignored) and accepts only gcc or clang.
import { createHash } from "node:crypto";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

export const NATIVE_HELPERS = ["fabric-landlock", "fabric-mesh-lease"];

const flags = (root) => [
  "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-static",
  "-nostdlib", "-ffreestanding", "-fno-builtin", "-fno-stack-protector",
  "-fno-pie", "-no-pie", `-ffile-prefix-map=${root}=.`, "-Wl,--build-id=none",
];

const sha256 = (file) => createHash("sha256").update(readFileSync(file)).digest("hex");

/** Returns the sha256 of each helper ({} off Linux): build.mjs embeds them into the bundle. */
export function buildLandlock() {
  if (process.platform !== "linux") return {};
  const root = resolve(".");
  const cc = process.env.CC || "cc";
  const env = { ...process.env, SOURCE_DATE_EPOCH: "0", LC_ALL: "C" };
  for (const name of ["CFLAGS", "CPPFLAGS", "LDFLAGS", "CPATH", "C_INCLUDE_PATH", "LIBRARY_PATH", "GCC_EXEC_PREFIX", "COMPILER_PATH"]) delete env[name];
  // `cc -v` names the driver for real ("gcc version ..." or "... clang version ..."); --version only echoes argv[0].
  const version = spawnSync(cc, ["-v"], { encoding: "utf8", env });
  const compiler = `${version.stdout ?? ""}\n${version.stderr ?? ""}`.split("\n").map(line => line.trim())
    .find(line => /^(gcc version |(\S+ )?clang version )/.test(line)) ?? "";
  if (version.error || version.status !== 0 || compiler === "") {
    throw new Error(`native helper build refuses compiler ${cc} (${compiler || version.error?.message || `exit ${version.status}`}): gcc or clang only`);
  }
  mkdirSync("dist/native", { recursive: true });
  const helpers = {};
  for (const helper of NATIVE_HELPERS) {
    const hashes = [];
    const outputs = [];
    try {
      for (const attempt of ["a", "b"]) {
        const dir = mkdtempSync(join(tmpdir(), `fabric-native-${helper}-${attempt}-`));
        outputs.push(dir);
        const output = join(dir, helper);
        const result = spawnSync(cc, [...flags(root), `native/${helper}.c`, "-o", output], { encoding: "utf8", env });
        if (result.error || result.status !== 0) {
          throw new Error(`${helper} build failed: install a Linux C compiler and Linux syscall headers (x86_64/aarch64). ${result.error?.message || result.stderr}`);
        }
        hashes.push(sha256(output));
      }
      if (hashes[0] !== hashes[1]) throw new Error(`${helper} is not reproducible: two builds differ (${hashes[0]} vs ${hashes[1]}) with ${compiler}`);
      copyFileSync(join(outputs[0], helper), `dist/native/${helper}`);
    } finally { for (const dir of outputs) rmSync(dir, { recursive: true, force: true }); }
    helpers[helper] = hashes[0];
  }
  // For assert-build-artifacts.mjs only; the runtime trusts the digests embedded into the bundle.
  writeFileSync("dist/native/manifest.json", `${JSON.stringify({ compiler, flags: flags("<root>"), helpers }, null, 2)}\n`);
  return helpers;
}

if (process.argv[1]?.endsWith("build-landlock.mjs")) buildLandlock();
