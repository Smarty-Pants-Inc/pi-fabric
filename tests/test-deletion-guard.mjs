// smarty-dev#11145: test-only deletion guard. Importing this module installs it (side effect).
// Locally (GITHUB_ACTIONS !== "true" at install) fs/fs.promises rm, rmdir and unlink (and the Sync forms)
// refuse any target outside the physical TMPDIR this process had at install. Under hosted CI they log one
// TEST_DELETION_REPORT: line per outside target, then delegate. Children inherit it via NODE_OPTIONS --import.
// ponytail: a JS seam, not native race protection; helpers deleting through their own syscalls are out of reach.
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, parse, sep } from "node:path";
import { fileURLToPath } from "node:url";

const REFUSAL = "test deletion outside private temp (smarty-dev#11145)";
const REPORT = "TEST_DELETION_REPORT:";
const stateKey = Symbol.for("pi-fabric.test-deletion-guard");
const windows = process.platform === "win32";

const selfUrl = new URL(import.meta.url);
const inheritedRoot = selfUrl.searchParams.get("root");
selfUrl.search = "";
selfUrl.hash = "";
const guardHref = selfUrl.href;
const guardPath = fileURLToPath(guardHref);

const realpath = fs.realpathSync.native;

/** Physical path of p, climbing to the nearest existing ancestor; missing components are joined lexically. */
function physical(p) {
  const tail = [];
  for (let current = p; ;) {
    try {
      return tail.length ? join(realpath(current), ...tail) : realpath(current);
    } catch (error) {
      if (error?.code !== "ENOENT" && error?.code !== "ENOTDIR") throw error;
      const parent = dirname(current);
      if (parent === current) throw error;
      tail.unshift(basename(current));
      current = parent;
    }
  }
}

const isShared = (root) => {
  const shared = new Set([parse(root).root, "/tmp", "/var/tmp", "/private/tmp", "/dev/shm"].map((p) => {
    try { return realpath(p); } catch { return p; }
  }));
  if (shared.has(root)) return true;
  // The root must not contain the home directory (or be it).
  try { return within(root, realpath(homedir())); } catch { return false; }
};

function pinRoot() {
  try {
    const root = realpath(tmpdir());
    // A child may narrow TMPDIR, but changing its environment must not widen the run's root.
    if (inheritedRoot !== null) {
      const cap = realpath(inheritedRoot);
      return isShared(cap) ? undefined : within(cap, root) ? root : cap;
    }
    return isShared(root) ? undefined : root;
  } catch { return undefined; }
}

const segments = windows ? /[\\/]+/ : /\/+/;
const within = (root, target) => {
  // Realpath canonicalizes ancestors; compare whole segments, without Unicode case folding.
  const r = root.split(segments);
  const t = target.split(segments);
  return r.length <= t.length && r.every((part, index) => part === t[index]);
};

/** The raw path as a string, or undefined when Node itself must reject the argument. */
function rawPath(input) {
  if (typeof input === "string") return input.includes("\0") ? undefined : input;
  if (Buffer.isBuffer(input)) {
    const text = input.toString("utf8");
    if (!Buffer.from(text, "utf8").equals(input)) throw new Error("undecodable Buffer path");
    return text.includes("\0") ? undefined : text;
  }
  if (input && typeof input === "object" && typeof input.href === "string" && typeof input.protocol === "string") {
    if (input.protocol !== "file:") return undefined;
    return fileURLToPath(input.href);
  }
  return undefined;
}

/** Kernel-faithful target: realpath of the raw parent (no lexical "..", so link/.. follows the link). */
function resolveTarget(raw) {
  // Do not collapse relative link/.. on Windows either. Ambiguous drive-relative paths fail closed.
  if (windows && /^[a-z]:(?![\\/])/i.test(raw)) throw new Error("drive-relative path");
  const absolute = isAbsolute(raw) ? raw : `${process.cwd()}${sep}${raw}`;
  let trimmed = absolute;
  const trailingSeparator = windows ? /[\\/]$/ : /\/$/;
  while (trimmed.length > parse(trimmed).root.length && trailingSeparator.test(trimmed)) trimmed = trimmed.slice(0, -1);
  const name = basename(trimmed);
  // A trailing separator, "." or ".." makes the kernel follow the final component: resolve it whole.
  if (trimmed !== absolute || name === "" || name === "." || name === "..") return physical(trimmed);
  return join(physical(trimmed.slice(0, trimmed.length - name.length) || sep), name);
}

function callerFrame() {
  const limit = Error.stackTraceLimit;
  Error.stackTraceLimit = 40;
  const stack = new Error().stack ?? "";
  Error.stackTraceLimit = limit;
  const frames = stack.split("\n").slice(1).map((line) => line.trim());
  return frames.find((line) => !line.includes(guardPath) && !line.includes(guardHref)
    && !line.includes("node:internal") && !line.includes("(node:fs") && !/^at (node:fs|fs\.)/.test(line)) ?? "unknown";
}

function install() {
  const report = process.env.GITHUB_ACTIONS === "true";
  const state = { report, root: pinRoot(), count: 0 };

  /** An Error to refuse with, or undefined to delegate. */
  const check = (operation, input) => {
    let raw;
    let outside;
    try {
      raw = rawPath(input);
      if (raw === undefined) return undefined; // Node rejects it before any syscall.
      outside = state.root === undefined || !within(state.root, resolveTarget(raw));
    } catch {
      outside = true; // Unresolvable: fail closed.
    }
    if (!outside) return undefined;
    state.count++;
    if (state.report) {
      const line = JSON.stringify({ path: raw ?? String(input), operation, caller: callerFrame() });
      try { process.stderr.write(`${REPORT} ${line}\n`); } catch { /* reporting never breaks a test */ }
      return undefined;
    }
    const error = new Error(REFUSAL);
    error.code = "ERR_TEST_DELETION_GUARD";
    error.path = raw ?? String(input);
    error.syscall = operation;
    return error;
  };

  const sync = (operation, original) => function guardedSync(path, ...rest) {
    const error = check(operation, path);
    if (error) throw error;
    return Reflect.apply(original, this, [path, ...rest]);
  };
  const callback = (operation, original) => function guardedCallback(path, ...rest) {
    const error = check(operation, path);
    if (error) {
      const done = rest.at(-1);
      if (typeof done !== "function") throw error;
      process.nextTick(done, error);
      return;
    }
    return Reflect.apply(original, this, [path, ...rest]);
  };
  const promise = (operation, original) => function guardedPromise(path, ...rest) {
    const error = check(operation, path);
    if (error) return Promise.reject(error);
    return Reflect.apply(original, this, [path, ...rest]);
  };

  // Wrap whatever is installed now (a test recorder included), once per process.
  for (const name of ["rm", "rmdir", "unlink"]) {
    fs[name] = callback(name, fs[name]);
    fs[`${name}Sync`] = sync(`${name}Sync`, fs[`${name}Sync`]);
    fs.promises[name] = promise(`promises.${name}`, fs.promises[name]);
  }
  syncBuiltinESMExports();

  process.once("exit", () => {
    if (!state.count) return;
    const key = state.report ? "reported" : "refused";
    try { process.stderr.write(`TEST_DELETION_REPORT_TOTAL: ${JSON.stringify({ [key]: state.count })}\n`); } catch { /* exit */ }
  });
  return state;
}

const owner = /** @type {Record<symbol, unknown>} */ (process);
owner[stateKey] ??= install();

// Children inherit the guard and its pinned ceiling, not a wider test-mutated TMPDIR.
{
  const existing = process.env.NODE_OPTIONS ?? "";
  if (!existing.includes(guardHref)) {
    const preload = new URL(guardHref);
    preload.searchParams.set("root", owner[stateKey].root ?? sep);
    const href = preload.href;
    const token = /[\s"\\]/.test(href) ? `"--import=${href.replace(/[\\"]/g, "\\$&")}"` : `--import=${href}`;
    process.env.NODE_OPTIONS = existing.trim() ? `${existing.trim()} ${token}` : token;
  }
}
