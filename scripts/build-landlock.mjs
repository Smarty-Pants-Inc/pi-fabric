// Linux-only native artifacts: the Landlock adapter and the mesh lease probe (smarty-dev#7936).
// Other platforms keep bash unchanged and the readiness gate fails closed.
import { mkdirSync } from "node:fs";
import { spawnSync } from "node:child_process";

export function buildLandlock() {
  if (process.platform !== "linux") return;
  mkdirSync("dist/native", { recursive: true });
  for (const helper of ["fabric-landlock", "fabric-mesh-lease"]) {
    const result = spawnSync(process.env.CC || "cc", [
      "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-static",
      "-nostdlib", "-ffreestanding", "-fno-builtin", "-fno-stack-protector",
      "-fno-pie", "-no-pie",
      `native/${helper}.c`, "-o", `dist/native/${helper}`,
    ], { encoding: "utf8" });
    if (result.error || result.status !== 0) {
      throw new Error(`${helper} build failed: install a Linux C compiler and Linux syscall headers (x86_64/aarch64). ${result.error?.message || result.stderr}`);
    }
  }
}

if (process.argv[1]?.endsWith("build-landlock.mjs")) buildLandlock();
