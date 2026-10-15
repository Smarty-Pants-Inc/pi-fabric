import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { build } from "esbuild";
import { afterEach, describe, expect, it } from "vitest";
import { windowsHelperDigest } from "../src/storage/verified-remove.js";

const nativeGlobal = globalThis as typeof globalThis & { __FABRIC_NATIVE_DIGESTS__?: string };
const previous = nativeGlobal.__FABRIC_NATIVE_DIGESTS__;

afterEach(() => {
  if (previous === undefined) delete nativeGlobal.__FABRIC_NATIVE_DIGESTS__;
  else nativeGlobal.__FABRIC_NATIVE_DIGESTS__ = previous;
});

describe("source-run native helper trust", () => {
  it("uses the pinned Windows helper digest when the build define is absent", () => {
    const toolchain = JSON.parse(readFileSync(new URL("../native/toolchain.json", import.meta.url), "utf8")) as {
      expected?: { windows?: Record<string, string> };
    };
    delete nativeGlobal.__FABRIC_NATIVE_DIGESTS__;
    expect(windowsHelperDigest()).toBe(toolchain.expected?.windows?.["fabric-win-remove.exe"]);
  });

  it("does not let a relocated bundle adopt checkout metadata as helper authority", async () => {
    const root = mkdtempSync(path.join(os.tmpdir(), "fabric-source-helper-"));
    try {
      const entry = path.join(root, "probe.mjs");
      await build({
        stdin: { contents: `import { windowsHelperCandidates, windowsHelperDigest } from ${JSON.stringify(path.resolve("src/storage/verified-remove.ts"))}; console.log(JSON.stringify({ digest: windowsHelperDigest(), candidates: windowsHelperCandidates() }));`, resolveDir: process.cwd(), sourcefile: "probe.ts" },
        outfile: entry, bundle: true, format: "esm", platform: "node", packages: "external", logLevel: "silent",
      });
      const result = spawnSync(process.execPath, [entry], { encoding: "utf8", cwd: process.cwd(), timeout: 15_000 });
      expect(result.error).toBeUndefined();
      expect(result.status, result.stderr).toBe(0);
      const output = JSON.parse(result.stdout.trim()) as { digest?: string; candidates: string[] };
      expect(output.digest).toBeUndefined();
      expect(output.candidates).not.toContain(path.resolve("dist/native/fabric-win-remove.exe"));
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
