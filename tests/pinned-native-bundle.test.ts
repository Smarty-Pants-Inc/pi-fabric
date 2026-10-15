import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { pinnedNativeBundle } from "./helpers/pinned-native-bundle.js";

const repo = fileURLToPath(new URL("../", import.meta.url));
const helperName = "fabric-win-remove.exe";
const missingCrossBuild = process.platform !== "win32" && !fs.existsSync(path.join(repo, "dist/native", helperName));
const roots: string[] = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pinned-native-bundle-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Keep: #1191's hosted catch and native-helper trust boundary. Exercise the built
// lookup on every OS; never call the executable or a product deletion child.
describe("pinned native test bundle", () => {
  it.skipIf(missingCrossBuild)("finds only the pinned helper beside the actual bundle and refuses changed bytes (requires a Windows artifact)", async () => {
    const root = scratch();
    const expected = JSON.parse(fs.readFileSync(path.join(repo, "native/toolchain.json"), "utf8"))
      .expected.windows[helperName] as string;
    // A missing build define must not be rescued by Vitest's source-test global.
    vi.stubGlobal("__FABRIC_NATIVE_DIGESTS__", JSON.stringify({ [helperName]: "0".repeat(64) }));
    const options = pinnedNativeBundle(root, "lookup.mjs");
    buildSync({ ...options, stdin: {
      contents: `export { windowsHelperBootstrap, windowsHelperCandidates, windowsHelperDigest } from ${JSON.stringify(path.join(repo, "src/storage/verified-remove.ts"))};`,
      resolveDir: repo,
    }, bundle: true, platform: "node", format: "esm" });
    const lookup = await import(/* @vite-ignore */ pathToFileURL(options.outfile).href) as
      Pick<typeof import("../src/storage/verified-remove.js"), "windowsHelperBootstrap" | "windowsHelperCandidates" | "windowsHelperDigest">;
    const staged = path.join(root, "dist/native", helperName);
    const bytes = fs.readFileSync(path.join(repo, "dist/native", helperName));
    expect(lookup.windowsHelperDigest()).toBe(expected);
    expect(lookup.windowsHelperCandidates()).toContain(staged);
    expect(lookup.windowsHelperBootstrap()).toEqual({ file: staged, bytes, digest: expected });
    expect(fs.readdirSync(path.dirname(staged))).toEqual([helperName]);
    fs.writeFileSync(staged, "tampered helper");
    expect(lookup.windowsHelperDigest()).toBe(expected);
    expect(lookup.windowsHelperBootstrap()).toBeUndefined();
  });

  it.skipIf(missingCrossBuild)("refuses to stage repository bytes that do not match the checked-in pin (requires a Windows artifact)", () => {
    const root = scratch();
    const source = path.join(repo, "dist/native", helperName);
    const read = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: Parameters<typeof fs.readFileSync>[1]) =>
      String(file) === source ? Buffer.from("untrusted build") : read(file, options)
    ) as typeof fs.readFileSync);
    expect(() => pinnedNativeBundle(root, "refused.mjs")).toThrow("digest mismatch");
    expect(fs.existsSync(path.join(root, "dist/native", helperName))).toBe(false);
  });

  it.skipIf(process.platform === "win32")("keeps native-only non-Windows fixtures independent of a Windows cross-build", () => {
    const root = scratch();
    const exists = fs.existsSync.bind(fs);
    const source = path.join(repo, "dist/native", helperName);
    vi.spyOn(fs, "existsSync").mockImplementation(file => String(file) === source ? false : exists(file));
    expect(pinnedNativeBundle(root, "linux.mjs")).toEqual({ outfile: path.join(root, "dist/fixture/linux.mjs"), define: {} });
    expect(fs.existsSync(path.join(root, "dist/native"))).toBe(false);
  });
});
