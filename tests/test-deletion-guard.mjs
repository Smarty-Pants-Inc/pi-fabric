// smarty-dev#11145: test-only deletion guard. Importing this module installs it (side effect).
// Outside a verified GitHub-hosted job, deletion and rename operands, and copy/link destinations,
// refuse paths outside the physical TMPDIR pinned at install. cp checks every kept recursive destination.
// Hosted CI logs TEST_DELETION_REPORT: per outside operand, then delegates. Children inherit NODE_OPTIONS.
// ponytail: a JS seam, not native race protection; helpers deleting through their own syscalls are out of reach.
import fs from "node:fs";
import childProcess from "node:child_process";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir, userInfo } from "node:os";
import { basename, dirname, isAbsolute, join, parse, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { isPromise } from "node:util/types";

const REFUSAL = "test deletion outside private temp (smarty-dev#11145)";
const REPORT = "TEST_DELETION_REPORT:";
const stateKey = Symbol.for("pi-fabric.test-deletion-guard");
const windows = process.platform === "win32";

const selfUrl = new URL(import.meta.url);
const inheritedRoot = selfUrl.searchParams.get("root")
  ?? (process.versions.bun ? process.env.TEST_DELETION_GUARD_ROOT ?? null : null);
selfUrl.search = "";
selfUrl.hash = "";
const guardHref = selfUrl.href;
const guardPath = fileURLToPath(guardHref);

const realpath = fs.realpathSync.native;
const lstat = fs.lstatSync;

/** Physical path of p, climbing to the nearest existing ancestor; missing non-symlink components are joined lexically. */
function physical(p) {
  const tail = [];
  for (let current = p; ;) {
    try {
      return tail.length ? join(realpath(current), ...tail) : realpath(current);
    } catch (error) {
      if (error?.code !== "ENOENT" && error?.code !== "ENOTDIR") throw error;
      // A dangling symlink (leaf or ancestor) has an unresolved target: fail closed, never lexical.
      let link = false;
      try { link = lstat(current).isSymbolicLink(); } catch (statError) {
        if (statError?.code !== "ENOENT" && statError?.code !== "ENOTDIR") throw statError;
      }
      if (link) throw error;
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
  // Fleet isolation redirects HOME/USERPROFILE into private TMPDIR before installation.
  // Protect the native account home, not that mutable environment; unprovable means shared.
  try {
    const home = userInfo().homedir;
    return !isAbsolute(home) || within(root, realpath(home));
  } catch { return true; }
};

function pinRoot() {
  try {
    const root = realpath(tmpdir());
    // A child may narrow TMPDIR, but changing its environment must not widen the run's root.
    if (inheritedRoot !== null) {
      const cap = realpath(inheritedRoot);
      // Descendants may narrow TMPDIR for their own scratch files, but the inherited
      // ceiling is the already-validated private test root and must not narrow with it.
      return isShared(cap) ? undefined : cap;
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
function resolveTarget(raw, followFinal = false) {
  // Do not collapse relative link/.. on Windows either. Ambiguous drive-relative paths fail closed.
  if (windows && /^[a-z]:(?![\\/])/i.test(raw)) throw new Error("drive-relative path");
  const absolute = isAbsolute(raw) ? raw : `${process.cwd()}${sep}${raw}`;
  let trimmed = absolute;
  const trailingSeparator = windows ? /[\\/]$/ : /\/$/;
  while (trimmed.length > parse(trimmed).root.length && trailingSeparator.test(trimmed)) trimmed = trimmed.slice(0, -1);
  const name = basename(trimmed);
  // Copies follow the destination itself; deletions/renames/link creation retain its final symlink.
  if (followFinal || trimmed !== absolute || name === "" || name === "." || name === "..") return physical(trimmed);
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
  const report = hostedOnly("outside-temp deletion reporting");
  const state = { report, root: pinRoot(), count: 0 };

  /** An Error to refuse with, or undefined to delegate. */
  const check = (operation, input, followFinal = false) => {
    let raw;
    let outside;
    try {
      raw = rawPath(input);
      if (raw === undefined) return undefined; // Node rejects it before any syscall.
      outside = state.root === undefined || !within(state.root, resolveTarget(raw, followFinal));
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

  const checkArgs = (operation, args, indexes, followFinal = false) => {
    let first;
    for (const index of indexes) {
      const error = check(operation, args[index], followFinal);
      if (first === undefined && error) first = error;
    }
    return first;
  };

  // Node's filter covers root and recursive pairs, including its sync JS copy path.
  // ponytail: conservative final-target checks also cover dereference:true and nested destination links.
  const guardedCpOptions = (operation, options) => {
    if (options === undefined) options = {};
    if (options === null || typeof options !== "object") return options;
    const filter = options.filter;
    if (filter !== undefined && typeof filter !== "function") return options;
    return {
      ...options,
      filter(source, destination) {
        const selected = filter === undefined ? true : Reflect.apply(filter, this, [source, destination]);
        // cpSync rejects real Promises but accepts truthy non-Promise objects without awaiting them.
        if (operation === "cpSync" && isPromise(selected)) return selected;
        if (operation !== "cpSync" && selected && typeof selected.then === "function") {
          return Promise.resolve(selected).then((keep) => {
            if (!keep) return keep;
            const error = check(operation, destination, true);
            if (error) throw error;
            return keep;
          });
        }
        if (!selected) return selected;
        const error = check(operation, destination, true);
        if (error) throw error;
        return selected;
      },
    };
  };

  const sync = (operation, original, indexes = [0], followFinal = false) => function guardedSync(...args) {
    const error = checkArgs(operation, args, indexes, followFinal);
    if (error) throw error;
    return Reflect.apply(original, this, args);
  };
  const callback = (operation, original, indexes = [0], followFinal = false) => function guardedCallback(...args) {
    const error = checkArgs(operation, args, indexes, followFinal);
    if (error) {
      const done = args.at(-1);
      if (typeof done !== "function") throw error;
      process.nextTick(done, error);
      return;
    }
    return Reflect.apply(original, this, args);
  };
  const promise = (operation, original, indexes = [0], followFinal = false) => function guardedPromise(...args) {
    const error = checkArgs(operation, args, indexes, followFinal);
    if (error) return Promise.reject(error);
    return Reflect.apply(original, this, args);
  };
  const cpCallback = (operation, original) => function guardedCp(...args) {
    if (typeof args[2] === "function") {
      return Reflect.apply(original, this, [args[0], args[1], guardedCpOptions(operation, undefined), args[2]]);
    }
    return Reflect.apply(original, this, [args[0], args[1], guardedCpOptions(operation, args[2]), args[3]]);
  };
  const cpPromise = (operation, original) => function guardedCpPromise(src, dest, options) {
    return Reflect.apply(original, this, [src, dest, guardedCpOptions(operation, options)]);
  };
  const cpSync = (operation, original) => function guardedCpSync(src, dest, options) {
    return Reflect.apply(original, this, [src, dest, guardedCpOptions(operation, options)]);
  };

  // Wrap whatever is installed now (a test recorder included), once per process.
  for (const name of ["rm", "rmdir", "unlink"]) {
    fs[name] = callback(name, fs[name]);
    fs[`${name}Sync`] = sync(`${name}Sync`, fs[`${name}Sync`]);
    fs.promises[name] = promise(`promises.${name}`, fs.promises[name]);
  }
  fs.rename = callback("rename", fs.rename, [0, 1]);
  fs.renameSync = sync("renameSync", fs.renameSync, [0, 1]);
  fs.promises.rename = promise("promises.rename", fs.promises.rename, [0, 1]);
  fs.copyFile = callback("copyFile", fs.copyFile, [1], true);
  fs.copyFileSync = sync("copyFileSync", fs.copyFileSync, [1], true);
  fs.promises.copyFile = promise("promises.copyFile", fs.promises.copyFile, [1], true);
  fs.link = callback("link", fs.link, [1]);
  fs.linkSync = sync("linkSync", fs.linkSync, [1]);
  fs.promises.link = promise("promises.link", fs.promises.link, [1]);
  fs.symlink = callback("symlink", fs.symlink, [1]);
  fs.symlinkSync = sync("symlinkSync", fs.symlinkSync, [1]);
  fs.promises.symlink = promise("promises.symlink", fs.promises.symlink, [1]);
  fs.cp = cpCallback("cp", fs.cp);
  fs.cpSync = cpSync("cpSync", fs.cpSync);
  fs.promises.cp = cpPromise("promises.cp", fs.promises.cp);
  const childOptionsIndex = (name, args) => name === "exec" || name === "execSync"
    ? 1 : Array.isArray(args[1]) || args[1] == null ? 2 : 1;
  const pinnedBunEnvironment = (env, root) => {
    let cap = root ?? sep;
    try {
      const requested = env.TEST_DELETION_GUARD_ROOT && realpath(env.TEST_DELETION_GUARD_ROOT);
      if (requested && root && within(root, requested)) cap = requested;
    } catch { /* retain the pinned ceiling */ }
    const preload = windows ? guardPath.replaceAll("\\", "/") : guardPath;
    const token = `--preload=${/[\s"\\]/.test(preload) ? JSON.stringify(preload) : preload}`;
    const options = env.BUN_OPTIONS ?? "";
    return { ...env, BUN_OPTIONS: options.split(/\s+/).includes(token) ? options : [options.trim(), token].filter(Boolean).join(" "),
      TEST_DELETION_GUARD_ROOT: cap };
  };
  for (const name of ["spawn", "spawnSync", "exec", "execSync", "execFile", "execFileSync"]) {
    const original = childProcess[name];
    childProcess[name] = function (...args) {
      const index = childOptionsIndex(name, args), options = args[index];
      if (options?.env) args[index] = { ...options, env: pinnedBunEnvironment(options.env, state.root) };
      return Reflect.apply(original, this, args);
    };
  }
  if (process.versions.bun) for (const name of ["spawn", "spawnSync"]) {
    const original = Bun[name];
    Bun[name] = function (...args) {
      const index = Array.isArray(args[0]) ? 1 : 0, options = args[index];
      if (options?.env) args[index] = { ...options, env: pinnedBunEnvironment(options.env, state.root) };
      return Reflect.apply(original, this, args);
    };
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
  const root = owner[stateKey].root;
  const preload = windows ? guardPath.replaceAll("\\", "/") : guardPath;
  const token = `--preload=${/[\s"\\]/.test(preload) ? JSON.stringify(preload) : preload}`;
  const options = process.env.BUN_OPTIONS ?? "";
  process.env.BUN_OPTIONS = options.split(/\s+/).includes(token) ? options : [options.trim(), token].filter(Boolean).join(" ");
  process.env.TEST_DELETION_GUARD_ROOT = root ?? sep;
  const existing = process.env.NODE_OPTIONS ?? "";
  if (!existing.includes(guardHref)) {
    const preload = new URL(guardHref);
    preload.searchParams.set("root", owner[stateKey].root ?? sep);
    const href = preload.href;
    const token = /[\s"\\]/.test(href) ? `"--import=${href.replace(/[\\"]/g, "\\$&")}"` : `--import=${href}`;
    process.env.NODE_OPTIONS = existing.trim() ? `${existing.trim()} ${token}` : token;
  }
}
