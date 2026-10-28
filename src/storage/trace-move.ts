import fs from "node:fs";
import os from "node:os";
import { createHash } from "node:crypto";
import childProcess from "node:child_process";
import { fileURLToPath } from "node:url";

// The same build-time digest map and adjacent native/ layout as the existing native helpers.
declare const __FABRIC_NATIVE_DIGESTS__: string | undefined;
export type TraceMoveResult = { status: "moved" } | { status: "error"; code: number } | { status: "unknown" };
/** The actual host, not process.platform (which platform-branch tests may override). */
export const nativeTraceMoveHost = (): boolean => os.type() === "Linux" || os.type() === "Windows_NT";

/** Source/jiti loads use the actual adjacent package manifest; bundles trust only their embedded map. */
export const traceMoveHelper = (): string | undefined => {
  const name = os.type() === "Linux" ? "fabric-trace-move" : os.type() === "Windows_NT" ? "fabric-win-trace-move.exe" : undefined;
  if (!name) return undefined;
  let embedded: Record<string, string> | undefined;
  if (typeof __FABRIC_NATIVE_DIGESTS__ !== "undefined") {
    try { embedded = JSON.parse(__FABRIC_NATIVE_DIGESTS__ as string); } catch { return undefined; }
  }
  for (const relative of ["../../dist/native/", "../dist/native/"]) {
    try {
      const base = new URL(relative, import.meta.url);
      const digests = embedded ?? JSON.parse(fs.readFileSync(new URL("manifest.json", base), "utf8")).helpers;
      const expected = digests?.[name];
      if (typeof expected !== "string" || !/^[0-9a-f]{64}$/.test(expected)) continue;
      const file = fileURLToPath(new URL(name, base));
      if (fs.lstatSync(file).isFile() && createHash("sha256").update(fs.readFileSync(file)).digest("hex") === expected) return file;
    } catch { /* next package-relative location */ }
  }
  return undefined;
};

/** One subprocess, exactly SOURCE DEST, one atomic no-replace move. Never retry or emulate an unknown verdict.
 * Hash-then-exec retains the existing helpers' same-user executable replacement residual. */
export const moveTraceNoReplace = (source: string, destination: string): TraceMoveResult => {
  const helper = traceMoveHelper();
  if (!helper) return { status: "unknown" };
  try {
    const result = childProcess.spawnSync(helper, [source, destination], { encoding: "utf8", timeout: 10_000, windowsHide: true, maxBuffer: 1024 });
    if (result.error || result.signal) return { status: "unknown" };
    if (result.status === 0 && result.stdout === "moved\n") return { status: "moved" };
    const match = /^error ([1-9][0-9]*)\n$/.exec(result.stdout ?? "");
    const code = match ? Number(match[1]) : NaN;
    return result.status === 2 && Number.isSafeInteger(code) ? { status: "error", code } : { status: "unknown" };
  } catch { return { status: "unknown" }; }
};
