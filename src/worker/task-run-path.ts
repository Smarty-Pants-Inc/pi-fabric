import fs from "node:fs";
import path from "node:path";

/**
 * Pure: the components of an absolute run path BELOW its parsed root, using Win32 or
 * POSIX semantics. The root (`/`, `C:\`, `\\server\share\`) is never repeated as a
 * component, so a walk that starts at the root never joins `C:\` + `C:`.
 */
export const runPathComponents = (runDirectory: string, platform: "win32" | "posix" = process.platform === "win32" ? "win32" : "posix"): { root: string; components: string[] } => {
  const api = platform === "win32" ? path.win32 : path.posix;
  const lexical = api.resolve(runDirectory);
  const root = api.parse(lexical).root;
  const components = lexical.slice(root.length).split(api.sep).filter((part) => part.length > 0);
  return { root, components };
};

/**
 * Owner OPTION1 (#12214 recursion cut): on Windows an inherited PI_FABRIC_RUN_ROOT fails
 * closed before any write; a trusted host root (explicit option or host-created temp root)
 * is unaffected. Pure: no filesystem access.
 */
export const assertTaskRunRootAdmissible = (inheritedRoot: boolean, makeError: (message: string) => Error = (message) => new Error(message), platform: NodeJS.Platform = process.platform): void => {
  if (inheritedRoot && platform === "win32") throw makeError("Inherited PI_FABRIC_RUN_ROOT is refused on Windows; nested runs need a trusted host run root");
};

/**
 * Refuse a run path whose EXISTING prefix traverses any link alias, before any write.
 * Walks from the parsed root (drive/UNC aware) with lstat: Node/libuv reports both
 * Win32 symlinks and junctions (IO_REPARSE_TAG_SYMLINK / MOUNT_POINT) as symbolic
 * links. Other reparse kinds are caught fail-closed by comparing the deepest existing
 * prefix with its native realpath. Missing trailing components (a future run path) are
 * accepted only after the whole existing prefix validated. Host path semantics only;
 * `makeError` affects error construction, never validation.
 */
export const assertTaskRunPathUnaliased = (runDirectory: string, makeError: (message: string) => Error = (message) => new Error(message)): void => {
  const lexical = path.resolve(runDirectory);
  const win32 = path.sep === "\\";
  const { root, components } = runPathComponents(lexical, win32 ? "win32" : "posix");
  let current = root;
  let deepest = root;
  for (const part of components) {
    const next = path.join(current, part);
    let stat: fs.Stats;
    try { stat = fs.lstatSync(next); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") break; throw error; }
    if (stat.isSymbolicLink()) throw makeError(`Task run path ${lexical} must not traverse a link alias (symbolic link or not a directory) at ${next}`);
    if (!stat.isDirectory() && next !== lexical) throw makeError(`Task run path ${lexical} traverses a non-directory at ${next}`);
    current = next;
    deepest = next;
  }
  // Fail closed on any remaining reparse/alias kind lstat does not classify as a link.
  const physical = fs.realpathSync.native(deepest);
  const same = win32 ? physical.toLowerCase() === deepest.toLowerCase() : physical === deepest;
  if (!same) throw makeError(`Task run path ${lexical} resolves through an alias: ${deepest} is ${physical}`);
};
