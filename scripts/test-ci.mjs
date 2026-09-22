import { execFileSync } from "node:child_process";
import { testTempEnvironment } from "./test-temp.mjs";

// Keep the MSYS usertemp backing directory through the entire ordered CI test batch.
// Child configs borrow it; only this process registers its removal on exit.
Object.assign(process.env, testTempEnvironment());
try {
  if (process.platform === "win32")
    execFileSync("bunx", ["vitest", "run", "tests/type-checker.test.ts"], {stdio:"inherit"});
  execFileSync("bun", ["run", "test:affected"], {stdio:"inherit"});
  execFileSync("bun", ["run", "test:smoke"], {stdio:"inherit"});
} catch {
  process.exitCode = 1;
}
