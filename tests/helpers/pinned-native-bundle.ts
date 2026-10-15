import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repo = fileURLToPath(new URL("../../", import.meta.url));
const helperName = "fabric-win-remove.exe";

/** Preserve production's bundle-relative helper layout without trusting build output as a pin. */
export function pinnedNativeBundle(root: string, entryName: string): { outfile: string; define: Record<string, string> } {
  const outfile = path.join(root, "dist/fixture", entryName);
  const source = path.join(repo, "dist/native", helperName);
  // A native-only Linux/macOS build need not cross-build Windows. Those fixtures never execute its helper.
  if (process.platform !== "win32" && !fs.existsSync(source)) return { outfile, define: {} };
  const toolchain = JSON.parse(fs.readFileSync(path.join(repo, "native/toolchain.json"), "utf8")) as {
    expected?: { windows?: Record<string, string> };
  };
  const expected = toolchain.expected?.windows?.[helperName];
  if (typeof expected !== "string" || !/^[0-9a-f]{64}$/.test(expected)) throw new Error("Missing pinned Windows helper digest");
  const bytes = fs.readFileSync(source);
  if (createHash("sha256").update(bytes).digest("hex") !== expected) throw new Error(`Native fixture helper digest mismatch: ${source}`);
  fs.mkdirSync(path.dirname(outfile), { recursive: true, mode: 0o700 });
  const native = path.join(root, "dist/native");
  fs.mkdirSync(native, { recursive: true, mode: 0o700 });
  // Stage exactly the bytes we verified, not another read or the whole native directory.
  fs.writeFileSync(path.join(native, helperName), bytes);
  return { outfile, define: { __FABRIC_NATIVE_DIGESTS__: JSON.stringify(JSON.stringify({ [helperName]: expected })) } };
}
