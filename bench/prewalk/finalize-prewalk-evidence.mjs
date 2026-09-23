#!/usr/bin/env node
// Finalize an immutable evidence archive in a fixed order: preservation first,
// then the human report, then this manifest. Requires a successful preservation
// report and a non-empty report file before any manifest is written; hashes
// regular files only, accounting explicitly for special entries (dead tmux
// sockets, FIFOs, symlinks) instead of trying to open them; refuses to
// overwrite an existing manifest and never modifies anything else in the
// archive.
//
// usage: node bench/prewalk/finalize-prewalk-evidence.mjs --archive <dir> \
//        [--preservation preservation.json] [--report report.md] \
//        [--manifest manifest.json] [--verify]
//
// --verify re-checks an existing manifest against the archive and exits
// nonzero on any digest or coverage mismatch without writing anything.

import fs from "node:fs";
import path from "node:path";
import { sha256, writeExclusive } from "./lib/prewalk-bench-lib.mjs";

const argv = process.argv.slice(2);
const value = (flag) => {
  const index = argv.indexOf(flag);
  return index >= 0 ? argv[index + 1] : undefined;
};
const fail = (error) => {
  console.error(JSON.stringify({ ok: false, error }));
  process.exit(1);
};

const archiveArg = value("--archive");
if (!archiveArg) {
  fail("usage: node bench/prewalk/finalize-prewalk-evidence.mjs --archive <dir> [--preservation <file>] [--report <file>] [--manifest <file>] [--verify]");
}
const archive = path.resolve(archiveArg);
if (!fs.existsSync(archive) || !fs.lstatSync(archive).isDirectory()) {
  fail(`--archive is not a directory: ${archive}`);
}
const preservationRel = value("--preservation") ?? "preservation.json";
const reportRel = value("--report") ?? "report.md";
const manifestRel = value("--manifest") ?? "manifest.json";
const verifyOnly = argv.includes("--verify");

const entryType = (entry) =>
  entry.isSymbolicLink() ? "symlink"
    : entry.isFIFO() ? "fifo"
    : entry.isSocket() ? "socket"
    : entry.isBlockDevice() ? "block-device"
    : entry.isCharacterDevice() ? "character-device"
    : "other";

// Hash regular files; account for special entries explicitly. The manifest
// output itself is excluded so the listing covers evidence, not its index.
const walk = () => {
  const files = {};
  const skipped = [];
  const visit = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      const rel = path.relative(archive, full).split(path.sep).join("/");
      if (entry.isDirectory()) visit(full);
      else if (entry.isFile() && rel !== manifestRel) files[rel] = sha256(fs.readFileSync(full));
      else if (!entry.isFile()) {
        const type = entryType(entry);
        // A symlink's target belongs to the inventory: record it without
        // following it, so a retarget cannot hide behind unchanged digests.
        skipped.push(type === "symlink" ? { path: rel, type, target: fs.readlinkSync(full) } : { path: rel, type });
      }
    }
  };
  visit(archive);
  return { files, skipped };
};

if (verifyOnly) {
  const manifestPath = path.join(archive, manifestRel);
  if (!fs.existsSync(manifestPath)) fail(`no manifest to verify at ${manifestPath}`);
  let manifest;
  try {
    manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  } catch (error) {
    fail(`manifest is not valid JSON: ${String(error?.message ?? error)}`);
  }
  const { files, skipped } = walk();
  const problems = [];
  for (const [rel, digest] of Object.entries(manifest.files ?? {})) {
    if (typeof digest !== "string" || !/^[0-9a-f]{64}$/.test(digest)) {
      problems.push(`${rel}: malformed digest`);
      continue;
    }
    if (!(rel in files)) {
      problems.push(`${rel}: listed but missing or not a regular file`);
      continue;
    }
    if (files[rel] !== digest) problems.push(`${rel}: digest mismatch`);
    delete files[rel];
  }
  for (const rel of Object.keys(files)) problems.push(`${rel}: regular file missing from the manifest`);
  // Special entries are part of the inventory as well: an added, removed,
  // retyped or retargeted symlink/FIFO/socket changes the archive without
  // changing a single digest.
  const skippedKey = (entry) =>
    `${typeof entry?.path === "string" ? entry.path : ""}\u0000${typeof entry?.type === "string" ? entry.type : ""}\u0000${typeof entry?.target === "string" ? entry.target : ""}`;
  if (!Array.isArray(manifest.skipped)) {
    if (skipped.length > 0) {
      problems.push(`manifest has no skipped inventory but the archive holds ${skipped.length} special entr${skipped.length === 1 ? "y" : "ies"}`);
    }
  } else {
    const walkedSkipped = new Map(skipped.map((entry) => [skippedKey(entry), entry]));
    for (const entry of manifest.skipped) {
      const rel = typeof entry?.path === "string" ? entry.path : "?";
      if (!walkedSkipped.has(skippedKey(entry))) {
        problems.push(`${rel}: special entry missing from the archive inventory (${entry?.type ?? "unknown"})`);
      } else {
        walkedSkipped.delete(skippedKey(entry));
      }
    }
    for (const entry of walkedSkipped.values()) problems.push(`${entry.path}: special entry missing from the manifest (${entry.type})`);
  }
  if (problems.length > 0) fail(JSON.stringify(problems.slice(0, 20)));
  console.log(JSON.stringify({ ok: true, verified: true, archive, manifest: manifestRel, fileCount: Object.keys(manifest.files ?? {}).length, skippedCount: (manifest.skipped ?? []).length }));
  process.exit(0);
}

// 1. A successful preservation report must exist before a manifest is written.
const preservationPath = path.join(archive, preservationRel);
if (!fs.existsSync(preservationPath)) fail(`preservation report missing: ${preservationRel}`);
let preservation;
try {
  preservation = JSON.parse(fs.readFileSync(preservationPath, "utf8"));
} catch (error) {
  fail(`preservation report is not valid JSON: ${String(error?.message ?? error)}`);
}
if (preservation === null || typeof preservation !== "object" || Array.isArray(preservation)) {
  fail("preservation report is not an object");
}
if (preservation.ok !== true) {
  fail(`preservation report is not ok: ${JSON.stringify({ ok: preservation.ok ?? null, unauthorized: preservation.unauthorized ?? null })}`);
}

// 2. The human report must exist and be non-empty.
const reportPath = path.join(archive, reportRel);
if (!fs.existsSync(reportPath)) fail(`report file missing: ${reportRel}`);
if (!fs.statSync(reportPath).isFile() || fs.statSync(reportPath).size === 0) {
  fail(`report file is empty or not a regular file: ${reportRel}`);
}

// 3. The manifest must not exist yet: finalization is once-only.
const manifestPath = path.join(archive, manifestRel);
if (fs.existsSync(manifestPath)) fail(`refusing to overwrite an existing manifest: ${manifestRel}`);

const { files, skipped } = walk();
if (!(preservationRel in files)) fail(`preservation report not covered by the manifest walk: ${preservationRel}`);
if (!(reportRel in files)) fail(`report file not covered by the manifest walk: ${reportRel}`);

// 4. Self-verify before committing: re-hash every listed file so a torn write
// or concurrent change cannot ship an inconsistent manifest.
for (const [rel, digest] of Object.entries(files)) {
  const actual = sha256(fs.readFileSync(path.join(archive, rel)));
  if (actual !== digest) fail(`self-verification failed, ${rel} changed during finalization`);
}

const manifest = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  archive,
  preservation: { file: preservationRel, ok: true },
  report: reportRel,
  fileCount: Object.keys(files).length,
  files,
  skipped,
};

try {
  writeExclusive(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
} catch (error) {
  fail(`could not write the manifest exclusively: ${String(error?.message ?? error)}`);
}

// 5. Read-back: the committed manifest must parse and carry the same count.
let written;
try {
  written = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
} catch (error) {
  fail(`written manifest failed to read back: ${String(error?.message ?? error)}`);
}
if (written.fileCount !== manifest.fileCount || Object.keys(written.files ?? {}).length !== manifest.fileCount) {
  fail("written manifest does not match the finalized inventory");
}
console.log(JSON.stringify({ ok: true, manifest: manifestRel, fileCount: manifest.fileCount, skippedCount: skipped.length }));
process.exit(0);
