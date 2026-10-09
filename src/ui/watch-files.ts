import fs from "node:fs";
import path from "node:path";

/** Nonpersistent dirty hints, not a new store reader. Atomic writers replace watched inodes. */
export const watchUiFiles = (root: string, names: readonly string[], changed: () => void): (() => void) => {
  let closed = false;
  const files = new Map<string, fs.FSWatcher>();
  const versions = new Map<string, string | undefined>();
  const version = (name: string): string | undefined => {
    try {
      const stat = fs.statSync(path.join(root, name), { bigint: true });
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;
    } catch { return undefined; }
  };
  const notify = (name: string): void => {
    const current = version(name);
    if (versions.get(name) === current) return;
    versions.set(name, current);
    changed(); // A file + namespace notification for the same version is just one dirty hint.
  };
  const bind = (name: string): void => {
    files.get(name)?.close();
    files.delete(name);
    if (closed) return;
    try {
      const watcher = fs.watch(path.join(root, name), { persistent: false }, (event) => {
        if (closed || files.get(name) !== watcher) return;
        if (event === "rename") bind(name);
        notify(name);
      });
      files.set(name, watcher);
      watcher.on("error", () => {
        if (files.get(name) !== watcher) return;
        watcher.close();
        files.delete(name); // The directory watcher can recover a later creation/replacement.
      });
    } catch { /* Missing signal / unsupported watch: the controller's slow fallback owns it. */ }
  };
  let directory: fs.FSWatcher | undefined;
  try {
    // Also watch the namespace: a missing file has no inode to watch, and an atomic replacement
    // can invalidate the file watcher before its rename notification reaches us.
    directory = fs.watch(root, { persistent: false }, (event, filename) => {
      if (closed || filename === null) return;
      const name = String(filename);
      if (!names.includes(name)) return;
      if (event === "rename" || !files.has(name)) bind(name);
      notify(name);
    });
    directory.on("error", () => { directory?.close(); directory = undefined; });
  } catch { /* File watchers, or the slow fallback, still work. */ }
  for (const name of names) { versions.set(name, version(name)); bind(name); }
  return () => {
    closed = true;
    directory?.close();
    for (const watcher of files.values()) watcher.close();
    files.clear();
  };
};
