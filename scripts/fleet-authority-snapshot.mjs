#!/usr/bin/env node
// SQLite uses native readOnly + query_only. Lead explicitly accepts SHM read-index maintenance.
import fs from "node:fs";
import path from "node:path";
import { collectAuthoritySnapshot, SnapshotError } from "./lib/fleet-authority-snapshot.mjs";

function parseArgs(argv) {
  const options = { cursors: [] }, seen = new Set();
  for (let at = 0; at < argv.length; at += 2) {
    const flag = argv[at], value = argv[at + 1];
    if (!["--mesh", "--site", "--name", "--peer", "--cursor", "--output"].includes(flag) || !value || value.startsWith("--") || flag !== "--cursor" && seen.has(flag)) throw new SnapshotError("INVALID_OPTIONS");
    seen.add(flag);
    if (flag === "--cursor") options.cursors.push(value);
    else options[flag.slice(2)] = value;
  }
  for (const key of ["mesh", "site", "name", "peer", "output"]) if (!options[key]) throw new SnapshotError("INVALID_OPTIONS");
  return options;
}
function outside(parent, file) { const relative = path.relative(parent, file); return relative !== "" && (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)); }
try {
  const options = parseArgs(process.argv.slice(2));
  const root = fs.realpathSync(options.mesh), parent = fs.realpathSync(path.dirname(path.resolve(options.output)));
  const destination = path.join(parent, path.basename(options.output));
  if (!outside(root, destination) || options.cursors.some(file => fs.realpathSync(file) === destination) || fs.existsSync(destination)) throw new SnapshotError("UNSAFE_OUTPUT");
  const snapshot = await collectAuthoritySnapshot(options), text = `${JSON.stringify(snapshot)}\n`;
  const fd = fs.openSync(destination, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
  try { fs.writeFileSync(fd, text); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  process.stdout.write(text);
} catch (error) {
  const code = error instanceof SnapshotError ? error.code : "COLLECTION_FAILED";
  const diagnostic = { error: code };
  if (error instanceof SnapshotError && code === "UNSUPPORTED_ID" && error.field !== undefined) {
    diagnostic.field = error.field; diagnostic.recordKind = error.recordKind; diagnostic.shape = error.shape;
  }
  process.stderr.write(`${JSON.stringify(diagnostic)}\n`); process.exitCode = 1;
}
