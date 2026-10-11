import fs from "node:fs";
import path from "node:path";

export type BridgeChange = "events" | "presence" | "lease";

/** Directory watches survive atomic file replacement. The root watch also discovers/rebinds
 * lease and participant directories: watching their old inode alone loses a mkdir/rename.
 * Notifications are hints, not authority; the bridge still reads/fences canonical snapshots.
 * There is deliberately no polling fallback on filesystems without working watches. */
export const watchBridgeStore = (
  root: string,
  changed: (kind: BridgeChange) => void,
  failed: (error: Error) => void,
): (() => void) => {
  let closed = false;
  const children = new Map<string, { identity: string; watcher: fs.FSWatcher }>();
  let rootWatcher: fs.FSWatcher | undefined;
  const identity = (directory: string): string | undefined => {
    try {
      const stat = fs.statSync(directory, { bigint: true });
      if (!stat.isDirectory()) throw new Error(`Bridge watch target is not a directory: ${directory}`);
      return `${stat.dev}:${stat.ino}`;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      throw error;
    }
  };
  const close = (): void => {
    closed = true;
    rootWatcher?.close();
    for (const { watcher } of children.values()) watcher.close();
    children.clear();
  };
  const fail = (error: unknown): void => {
    if (closed) return;
    close();
    failed(new Error(`Bridge filesystem notifications failed for ${root}: ${error instanceof Error ? error.message : String(error)}`));
  };
  const open = (directory: string, callback: (name: string | null) => void): fs.FSWatcher => {
    const watcher = fs.watch(directory, (_event, name) => {
      if (closed) return;
      try { callback(name === null ? null : String(name)); } catch (error) { fail(error); }
    });
    watcher.on("error", fail);
    return watcher;
  };
  const bind = (name: string): void => {
    const directory = path.join(root, name);
    const current = identity(directory);
    const old = children.get(name);
    if (current === old?.identity) return;
    old?.watcher.close();
    children.delete(name);
    if (current === undefined) return; // root watch catches its creation; snapshot catches its contents
    try {
      const watcher = open(directory, (file) => {
        bind(name);
        if (file === null || file === name || file.endsWith(".json")) changed("presence");
      });
      children.set(name, { identity: current, watcher });
    } catch (error) {
      // Directory removed between stat and watch: the root's rename notification covers it.
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  };
  try {
    const rootIdentity = identity(root);
    if (rootIdentity === undefined) throw new Error("mesh root does not exist");
    rootWatcher = open(root, (name) => {
      if (identity(root) !== rootIdentity) throw new Error("mesh root was removed or replaced; reconnect the bridge");
      if (name === null || name === "participants" || name === "host-leases") {
        bind("participants");
        bind("host-leases");
        changed("presence");
      }
      if (name === null || name === "events.jsonl" || name === "generation") changed("events");
      if (name === null || name === "state.json" || name === "state.db" || name === "state.db-wal" || name === "state.db-journal") changed("presence");
      // Ignore cursor, temporary, lock, journal/signal and outbox sidecar writes.
    });
    bind("participants");
    bind("host-leases");
    return close;
  } catch (error) {
    close();
    throw new Error(`Bridge requires filesystem notifications for ${root}: ${error instanceof Error ? error.message : String(error)}`);
  }
};
