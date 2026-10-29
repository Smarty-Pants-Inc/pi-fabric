import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// The public export resolves to dist/, not the tested source tree. Build first.
// Keep the test-owned raw evidence; never truncate or remove these records.
describe("published recovery entry", () => {
  it.skipIf(process.platform === "win32")("cold-imports without store I/O and durably recovers on actual first use", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-package-"));
    const directory = path.join(root, "journal");
    const child = spawnSync(process.execPath, [fileURLToPath(new URL("./helpers/recovery-package-import.mjs", import.meta.url)), directory], {
      encoding: "utf8", timeout: 30_000,
    });
    expect(child.error, child.stderr).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout)).toMatchObject({
      first: { seq: 1, existing: false }, same: { seq: 1, existing: true }, bytes: "retained public-entry evidence",
    });
    expect(fs.readdirSync(directory).filter(name => name.startsWith("raw-"))).toHaveLength(1);
    expect(fs.readdirSync(directory).filter(name => name.startsWith("commit-"))).toHaveLength(1);
  });
});
