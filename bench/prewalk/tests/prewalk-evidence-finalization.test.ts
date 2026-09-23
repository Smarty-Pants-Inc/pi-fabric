import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const cli = path.join(projectRoot, "bench", "prewalk", "finalize-prewalk-evidence.mjs");
const roots: string[] = [];
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "prewalk-finalize-"));
  roots.push(root);
  return root;
};
afterAll(() => {
  for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
});

interface ArchiveOptions {
  preservation?: Record<string, unknown> | null;
  report?: string | null;
  extra?: string[];
  fifo?: boolean;
  symlinks?: Array<{ name: string; target: string }>;
}

const makeArchive = (options: ArchiveOptions = {}) => {
  const archive = tempRoot();
  fs.mkdirSync(path.join(archive, "nested"), { recursive: true });
  if (options.preservation !== null) {
    fs.writeFileSync(
      path.join(archive, "preservation.json"),
      JSON.stringify(options.preservation ?? { ok: true, unauthorized: [] }, null, 2) + "\n",
    );
  }
  if (options.report !== null) {
    fs.writeFileSync(path.join(archive, "report.md"), options.report ?? "# Evidence\n\nsummary\n");
  }
  for (const rel of options.extra ?? ["nested/evidence.json"]) {
    fs.writeFileSync(path.join(archive, rel), "data\n");
  }
  if (options.fifo && process.platform !== "win32") {
    execFileSync("mkfifo", [path.join(archive, "probe.fifo")]);
  }
  for (const link of options.symlinks ?? []) fs.symlinkSync(link.target, path.join(archive, link.name));
  return archive;
};

const runCli = (args: string[]) =>
  spawnSync(process.execPath, [cli, ...args], { cwd: projectRoot, encoding: "utf8", timeout: 30_000 });

describe("finalize-prewalk-evidence", () => {
  it("finalizes after preservation and report, hashing files and accounting special entries", () => {
    const archive = makeArchive({ fifo: true });
    const before = fs.readFileSync(path.join(archive, "report.md"));
    const result = runCli(["--archive", archive]);
    expect(result.status).toBe(0);
    const manifest = JSON.parse(fs.readFileSync(path.join(archive, "manifest.json"), "utf8")) as {
      fileCount: number;
      files: Record<string, string>;
      skipped: Array<{ path: string; type: string }>;
      preservation: { file: string; ok: boolean };
      report: string;
    };
    expect(Object.keys(manifest.files).sort()).toEqual(["nested/evidence.json", "preservation.json", "report.md"]);
    expect(manifest.fileCount).toBe(3);
    expect(manifest.preservation).toEqual({ file: "preservation.json", ok: true });
    expect(manifest.report).toBe("report.md");
    expect(manifest.files["manifest.json"]).toBeUndefined();
    if (process.platform !== "win32") {
      expect(manifest.skipped).toEqual([{ path: "probe.fifo", type: "fifo" }]);
    }
    // The archive is untouched apart from the new manifest.
    expect(fs.readFileSync(path.join(archive, "report.md")).equals(before)).toBe(true);
  });

  it("verifies an existing manifest and detects tampering", () => {
    const archive = makeArchive();
    expect(runCli(["--archive", archive]).status).toBe(0);
    expect(runCli(["--archive", archive, "--verify"]).status).toBe(0);
    fs.writeFileSync(path.join(archive, "nested/evidence.json"), "tampered\n");
    const result = runCli(["--archive", archive, "--verify"]);
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("digest mismatch");
  });

  it("refuses to overwrite an existing manifest", () => {
    const archive = makeArchive();
    expect(runCli(["--archive", archive]).status).toBe(0);
    const result = runCli(["--archive", archive]);
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("refusing to overwrite");
  });

  it("requires a successful preservation report", () => {
    const missing = makeArchive({ preservation: null });
    const r1 = runCli(["--archive", missing]);
    expect(r1.status).toBe(1);
    expect(r1.stderr).toContain("preservation report missing");
    const failed = makeArchive({ preservation: { ok: false, unauthorized: ["src/x.ts"] } });
    const r2 = runCli(["--archive", failed]);
    expect(r2.status).toBe(1);
    expect(r2.stderr).toContain("not ok");
  });

  it("requires a non-empty report", () => {
    const empty = makeArchive({ report: "" });
    const result = runCli(["--archive", empty]);
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("empty");
  });

  describe.skipIf(process.platform === "win32")("special-entry inventory", () => {
    type SkippedEntry = { path: string; type: string; target?: string };
    const skippedOf = (archive: string) =>
      (
        JSON.parse(fs.readFileSync(path.join(archive, "manifest.json"), "utf8")) as {
          skipped: SkippedEntry[];
        }
      ).skipped;
    // Directory order is not part of the contract, so compare by path.
    const byPath = (entries: SkippedEntry[]) => [...entries].sort((left, right) => left.path.localeCompare(right.path));

    it("records a symlink target and verifies an unchanged special inventory", () => {
      const archive = makeArchive({ fifo: true, symlinks: [{ name: "probe.link", target: "nested/evidence.json" }] });
      expect(runCli(["--archive", archive]).status).toBe(0);
      expect(byPath(skippedOf(archive))).toEqual(
        byPath([
          { path: "probe.fifo", type: "fifo" },
          { path: "probe.link", type: "symlink", target: "nested/evidence.json" },
        ]),
      );
      // The symlinked file is not followed: it stays a special entry, not a second digest.
      expect(runCli(["--archive", archive, "--verify"]).status).toBe(0);
    });

    it("detects an added or removed special entry", () => {
      const added = makeArchive({ symlinks: [{ name: "probe.link", target: "nested/evidence.json" }] });
      expect(runCli(["--archive", added]).status).toBe(0);
      fs.symlinkSync("nested/evidence.json", path.join(added, "extra.link"));
      const addedResult = runCli(["--archive", added, "--verify"]);
      expect(addedResult.status).toBe(1);
      expect(addedResult.stderr).toContain("extra.link");

      const removed = makeArchive({ symlinks: [{ name: "probe.link", target: "nested/evidence.json" }] });
      expect(runCli(["--archive", removed]).status).toBe(0);
      fs.rmSync(path.join(removed, "probe.link"));
      const removedResult = runCli(["--archive", removed, "--verify"]);
      expect(removedResult.status).toBe(1);
      expect(removedResult.stderr).toContain("probe.link");
    });

    it("detects a retargeted symlink even when every regular digest is unchanged", () => {
      const archive = makeArchive({ symlinks: [{ name: "probe.link", target: "nested/evidence.json" }] });
      expect(runCli(["--archive", archive]).status).toBe(0);
      const link = path.join(archive, "probe.link");
      fs.rmSync(link);
      fs.symlinkSync("report.md", link);
      const result = runCli(["--archive", archive, "--verify"]);
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("probe.link");
    });

    it("detects a special entry that changed type", () => {
      const archive = makeArchive({ fifo: true });
      expect(runCli(["--archive", archive]).status).toBe(0);
      const fifo = path.join(archive, "probe.fifo");
      fs.rmSync(fifo);
      fs.symlinkSync("report.md", fifo);
      const result = runCli(["--archive", archive, "--verify"]);
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("probe.fifo");
    });
  });
});
