// Lock-safe patch capture for benchmark checkouts.
//
// capturePatch never touches the repository's real index, its index.lock, HEAD,
// refs or configuration: staging happens in a private temporary index, new
// objects are written into the capture output, and existing repository objects
// are read through Git's alternate-object mechanism. A real index.lock is left
// untouched (it may belong to a live process); concurrent HEAD or index changes
// during capture fail the capture instead of silently producing a stale patch.

import { execFile } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { sha256 } from "./prewalk-bench-lib.mjs";

const execFileAsync = promisify(execFile);
const equalBytes = (a, b) =>
  (a === null && b === null) || (a !== null && b !== null && Buffer.compare(a, b) === 0);

const git = (repo, args, env) =>
  execFileAsync("git", args, { cwd: repo, env, maxBuffer: 256 * 1024 * 1024 }).then(({ stdout }) => stdout);

export async function capturePatch({ repo, baseline, out }) {
  if (!repo || !baseline || !out) throw new Error("capturePatch requires repo, baseline and out");
  const patchPath = path.join(out, "patch.diff");
  if (fs.existsSync(patchPath)) throw new Error(`refusing to overwrite existing capture output: ${patchPath}`);
  const gitDir = (await git(repo, ["rev-parse", "--absolute-git-dir"], process.env)).trim();
  const realIndex = path.join(gitDir, "index");
  const lockPath = path.join(gitDir, "index.lock");
  const readMaybe = (file) => (fs.existsSync(file) ? fs.readFileSync(file) : null);
  const indexBefore = readMaybe(realIndex);
  const lockBefore = readMaybe(lockPath);
  const headBefore = (await git(repo, ["rev-parse", "HEAD"], process.env)).trim();
  if (headBefore !== baseline) {
    throw new Error(`HEAD ${headBefore} does not match the recorded baseline ${baseline}`);
  }
  fs.mkdirSync(out, { recursive: true });
  const tmpIndex = path.join(out, "index");
  const objectsDir = path.join(out, "git-objects");
  if (fs.existsSync(tmpIndex) || fs.existsSync(objectsDir)) {
    throw new Error(`refusing to reuse non-empty capture output: ${out}`);
  }
  fs.mkdirSync(objectsDir, { recursive: true });
  const childEnv = {
    ...process.env,
    GIT_INDEX_FILE: tmpIndex,
    GIT_OBJECT_DIRECTORY: objectsDir,
    GIT_ALTERNATE_OBJECT_DIRECTORIES: path.join(gitDir, "objects"),
  };
  await git(repo, ["read-tree", baseline], childEnv);
  await git(repo, ["add", "-A", "."], childEnv);
  const diff = await git(
    repo,
    ["diff", "--binary", "--no-ext-diff", "--no-textconv", "--cached", baseline],
    childEnv,
  );
  const headAfter = (await git(repo, ["rev-parse", "HEAD"], process.env)).trim();
  if (headAfter !== baseline) throw new Error("HEAD changed during capture; refusing unstable patch");
  if (!equalBytes(readMaybe(realIndex), indexBefore)) {
    throw new Error("original index changed during capture");
  }
  if (!equalBytes(readMaybe(lockPath), lockBefore)) {
    throw new Error("index.lock changed during capture");
  }
  fs.writeFileSync(patchPath, diff, { flag: "wx", mode: 0o600 });
  return { patchPath, sha256: sha256(Buffer.from(diff, "utf8")), bytes: Buffer.byteLength(diff) };
}
