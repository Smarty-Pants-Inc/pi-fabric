import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export interface MainMarker {
  pid: number;
  startTime: string;
  rootId: string;
  sessionId: string;
  createdAt: number;
  /** Published by the root-scoped Main fence, including both bindings on rebind. */
  fenced: true;
  transition?: true;
  rootIds?: string[];
}
export const mainMarkerPath = (meshRoot: string, pid: number, startTime: string): string =>
  path.join(meshRoot, "main-markers", `${pid}-${startTime}.json`);

/** Required startup evidence, not a runtime activation. The host supplies the
 * native session only in its first context; empty bindings mean still starting
 * and MUST NOT authorize ignoring this Main. No session files are inspected. */
const activeMarkers = new Set<MainProcessMarker>();
const closeMainMarkers = (): void => { for (const marker of activeMarkers) marker.close(); };

export class MainProcessMarker {
  #pending: Promise<void> = Promise.resolve();
  #closed = false;
  #current: { meshRoot: string; marker: MainMarker } | undefined;
  readonly #startTime = (() => {
    if (process.platform !== "linux") return undefined;
    try {
      const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
      const value = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
      return value && /^\d+$/.test(value) ? value : undefined;
    } catch { return undefined; }
  })();
  readonly #createdAt = Date.now();
  constructor() {
    if (activeMarkers.size === 0) process.once("exit", closeMainMarkers);
    activeMarkers.add(this);
  }

  publish(meshRoot: string, rootId: string, sessionId: string): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("Fabric Main marker is closed"));
    const publication = this.#pending.catch(() => undefined).then(() => this.#publish(meshRoot, rootId, sessionId));
    this.#pending = publication;
    return publication;
  }

  /** Await before changing any session state. Unknown next binding keeps the old
   * marker transitioning; config/activation failures deliberately leave it so. */
  beginTransition(meshRoot?: string, rootId?: string, sessionId?: string): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("Fabric Main marker is closed"));
    const publication = this.#pending.catch(() => undefined).then(() => {
      const current = this.#current;
      if (!meshRoot && !current) throw new Error("Fabric Main marker has no starting binding");
      return this.#publish(meshRoot ?? current!.meshRoot, rootId ?? current?.marker.rootId ?? "",
        sessionId ?? current?.marker.sessionId ?? "", true);
    });
    this.#pending = publication;
    return publication;
  }

  async #publish(meshRoot: string, rootId: string, sessionId: string, transition = false): Promise<void> {
    if (process.platform !== "linux") return;
    const { withMainPublicationFence } = await import("./main-publication-fence.js");
    if (!this.#startTime) throw new Error("Cannot publish Fabric Main marker without Linux process birth evidence");
    const previous = this.#current;
    const marker: MainMarker = { pid: process.pid, startTime: this.#startTime, rootId, sessionId, createdAt: this.#createdAt, fenced: true,
      ...(transition ? { transition: true, rootIds: [...new Set([previous?.marker.rootId, rootId].filter((id): id is string => !!id))] } : {}) };
    const write = () => {
      if (this.#closed) return;
      const file = mainMarkerPath(meshRoot, marker.pid, marker.startTime);
      fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
      const temp = `${file}.${randomUUID()}.tmp`;
      try {
        fs.writeFileSync(temp, JSON.stringify(marker), { mode: 0o600, flag: "wx" });
        fs.renameSync(temp, file);
      } finally { fs.rmSync(temp, { force: true }); }
      this.#current = { meshRoot, marker };
      if (previous && previous.meshRoot !== meshRoot) {
        fs.rmSync(mainMarkerPath(previous.meshRoot, marker.pid, marker.startTime), { force: true });
      }
    };
    // Acquire both roots in a stable order when changing bindings, so two
    // concurrent session switches cannot deadlock by taking opposite roots.
    const fences = [{ meshRoot, rootId }];
    if (previous && (previous.meshRoot !== meshRoot || previous.marker.rootId !== rootId)) {
      fences.push({ meshRoot: previous.meshRoot, rootId: previous.marker.rootId });
    }
    fences.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    const fence = (index: number): Promise<void> => index === fences.length ? Promise.resolve(write())
      : withMainPublicationFence(fences[index]!.meshRoot, fences[index]!.rootId, () => fence(index + 1));
    await fence(0);
  }

  close(): void {
    this.#closed = true;
    activeMarkers.delete(this);
    if (activeMarkers.size === 0) process.removeListener("exit", closeMainMarkers);
    if (!this.#current) return;
    const { meshRoot, marker } = this.#current;
    try { fs.rmSync(mainMarkerPath(meshRoot, marker.pid, marker.startTime), { force: true }); } catch { /* Stale births are harmless. */ }
    this.#current = undefined;
  }
}
