import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { writeFileAtomic } from "../core/atomic-write.js";
import type { MeshEvent } from "./store.js";

/**
 * The durable fleet log (smarty-dev#754, Paul's decision 2): every event of a mesh root as plain
 * append-only files, one per topic and UTC day, in the node's file tree (smarty-dev#1120):
 *
 *   <dir>/MESH.json                      the mesh root this archive belongs to
 *   <dir>/HEAD.json                      the last archived event, written around each append
 *   <dir>/<yyyy>/<mm>/<dd>/<topic>.jsonl  one line per event, the same bytes as the live log
 *   <dir>/<yyyy>/<mm>/<dd>/SEAL.json     per file line count, sequence range and sha256
 *
 * The mesh root enables it with `event-archive.json` ({ "version": 1, "dir": "/abs" }), so every
 * store of that root archives, whatever its process's configuration. The store calls this under
 * its publish lock; nothing here locks.
 */
export const MESH_ARCHIVE_CONFIG = "event-archive.json";

export interface MeshArchiveEntry {
  event: MeshEvent;
  line: string;
}

interface ArchiveHead {
  sequence: number;
  id: string;
  file: string;
}

interface SealFile {
  lines: number;
  firstSequence: number;
  lastSequence: number;
  sha256: string;
}

const errorCode = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "code" in error ? String((error as { code: unknown }).code) : undefined;

const pad = (value: number): string => String(value).padStart(2, "0");

// The topic grammar is file-safe except "/" and ":". "%" is outside it, so this reverses exactly.
export const archiveFileName = (topic: string): string =>
  `${topic.replaceAll("/", "%2F").replaceAll(":", "%3A")}.jsonl`;

const dayOf = (createdAt: number): string => {
  const date = new Date(createdAt);
  return `${date.getUTCFullYear()}/${pad(date.getUTCMonth() + 1)}/${pad(date.getUTCDate())}`;
};

const relativeFile = (event: MeshEvent): string => `${dayOf(event.createdAt)}/${archiveFileName(event.topic)}`;

const parseEvent = (line: string): MeshEvent | undefined => {
  try {
    const parsed = JSON.parse(line) as MeshEvent;
    return typeof parsed.sequence === "number" && typeof parsed.id === "string" ? parsed : undefined;
  } catch {
    return undefined;
  }
};

export class MeshArchive {
  constructor(readonly dir: string, readonly meshRoot: string) {}

  /** The archive a mesh root names, or undefined when it names none. A bad entry throws (fail closed). */
  static fromRoot(root: string): MeshArchive | undefined {
    let text: string;
    try {
      text = fs.readFileSync(path.join(root, MESH_ARCHIVE_CONFIG), "utf8");
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
    const parsed = JSON.parse(text) as { version?: unknown; dir?: unknown };
    if (parsed.version !== 1 || typeof parsed.dir !== "string" || !path.isAbsolute(parsed.dir)) {
      throw new Error(`Invalid mesh event archive configuration in ${path.join(root, MESH_ARCHIVE_CONFIG)}`);
    }
    return new MeshArchive(parsed.dir, root);
  }

  head(): ArchiveHead | undefined {
    let text: string;
    try {
      text = fs.readFileSync(path.join(this.dir, "HEAD.json"), "utf8");
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
    const head = JSON.parse(text) as ArchiveHead;
    if (typeof head.sequence !== "number" || typeof head.id !== "string" || typeof head.file !== "string") {
      throw new Error(`Invalid mesh event archive head in ${this.dir}`);
    }
    return head;
  }

  /** The line of the head event, when its append reached the file (crash recovery). */
  headLine(head: ArchiveHead): string | undefined {
    const file = path.join(this.dir, head.file);
    let text: string;
    try {
      text = this.#tail(file);
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
    const line = text.split("\n").filter(Boolean).at(-1);
    const event = line === undefined ? undefined : parseEvent(line);
    return event?.id === head.id && event.sequence === head.sequence ? line : undefined;
  }

  /**
   * Appends events in order and syncs every file it wrote before it returns. An event already
   * at the end of its file is skipped, so a repeated catch-up adds nothing. `intent` writes the
   * head before the append (a publish, whose event is not live yet); otherwise it follows it.
   */
  append(entries: MeshArchiveEntry[], options: { intent?: boolean } = {}): void {
    if (entries.length === 0) return;
    const last = entries.at(-1)!.event;
    const head = { sequence: last.sequence, id: last.id, file: relativeFile(last) };
    const previousDay = this.head()?.file.split("/").slice(0, 3).join("/");
    this.#describeMesh(entries[0]!.event.sequence);
    if (options.intent) this.#writeHead(head);
    const open = new Map<string, { descriptor: number; last: { sequence: number; id: string } | undefined }>();
    const days: string[] = previousDay ? [previousDay] : [];
    try {
      for (const { event, line } of entries) {
        const relative = relativeFile(event);
        const day = relative.split("/").slice(0, 3).join("/");
        if (days.at(-1) !== day) days.push(day);
        let file = open.get(relative);
        if (!file) {
          const absolute = path.join(this.dir, relative);
          fs.mkdirSync(path.dirname(absolute), { recursive: true, mode: 0o700 });
          const descriptor = fs.openSync(absolute, "a+", 0o600);
          file = { descriptor, last: this.#repairAndReadLast(descriptor) };
          open.set(relative, file);
        }
        const previous = file.last;
        if (previous && (previous.sequence > event.sequence || (previous.sequence === event.sequence && previous.id === event.id))) {
          continue;
        }
        fs.writeSync(file.descriptor, `${line}\n`);
        file.last = { sequence: event.sequence, id: event.id };
      }
      for (const { descriptor } of open.values()) fs.fdatasyncSync(descriptor);
    } finally {
      for (const { descriptor } of open.values()) fs.closeSync(descriptor);
    }
    if (!options.intent) this.#writeHead(head);
    // A day is closed once a later day has an event; seal each closed day once.
    for (const day of days.slice(0, -1)) this.#seal(day);
  }

  /** Events after a sequence, in sequence order, for reads older than the live log. */
  readAfter(after: number, matches: (event: MeshEvent) => boolean, limit: number, topic?: string): MeshEvent[] {
    const found: MeshEvent[] = [];
    for (const day of this.#days()) {
      const directory = path.join(this.dir, day);
      const seal = this.#readSeal(directory);
      if (seal && Math.max(...Object.values(seal.files).map((file) => file.lastSequence)) <= after) continue;
      const names = topic !== undefined
        ? [archiveFileName(topic)]
        : fs.readdirSync(directory).filter((name) => name.endsWith(".jsonl"));
      const dayEvents: MeshEvent[] = [];
      for (const name of names) {
        if (seal?.files[name] && seal.files[name].lastSequence <= after) continue;
        let text: string;
        try {
          text = fs.readFileSync(path.join(directory, name), "utf8");
        } catch (error) {
          if (errorCode(error) === "ENOENT") continue;
          throw error;
        }
        for (const line of text.split("\n")) {
          const event = line ? parseEvent(line) : undefined;
          if (event && event.sequence > after && matches(event)) dayEvents.push(event);
        }
      }
      found.push(...dayEvents.sort((left, right) => left.sequence - right.sequence));
      if (found.length >= limit) break;
    }
    return found.slice(0, limit);
  }

  #days(): string[] {
    const numeric = (directory: string): string[] => {
      try {
        return fs.readdirSync(directory).filter((name) => /^\d+$/.test(name)).sort();
      } catch (error) {
        if (errorCode(error) === "ENOENT") return [];
        throw error;
      }
    };
    return numeric(this.dir).flatMap((year) =>
      numeric(path.join(this.dir, year)).flatMap((month) =>
        numeric(path.join(this.dir, year, month)).map((day) => `${year}/${month}/${day}`)));
  }

  #writeHead(head: ArchiveHead): void {
    writeFileAtomic(path.join(this.dir, "HEAD.json"), `${JSON.stringify(head)}\n`);
  }

  #describeMesh(firstSequence: number): void {
    const file = path.join(this.dir, "MESH.json");
    if (fs.existsSync(file)) return;
    writeFileAtomic(file, `${JSON.stringify({
      version: 1, meshRoot: this.meshRoot, host: os.hostname(), firstSequence, createdAt: new Date().toISOString(),
    })}\n`);
  }

  // A torn last line belongs to an append that never returned: cut it, as the live log does.
  #repairAndReadLast(descriptor: number): { sequence: number; id: string } | undefined {
    let size = fs.fstatSync(descriptor).size;
    let tail = this.#lastLines(descriptor, size);
    if (tail.length && tail[tail.length - 1] !== 0x0a) {
      const cut = tail.lastIndexOf(0x0a) + 1;
      fs.ftruncateSync(descriptor, size - tail.length + cut);
      size = size - tail.length + cut;
      tail = this.#lastLines(descriptor, size);
    }
    const line = tail.toString("utf8").split("\n").filter(Boolean).at(-1);
    const event = line === undefined ? undefined : parseEvent(line);
    return event ? { sequence: event.sequence, id: event.id } : undefined;
  }

  // The end of a file, back to the newline before its last line: 64 KiB covers almost every
  // event, so a publish reads one chunk, not the file.
  #lastLines(descriptor: number, size: number): Buffer {
    for (let readBytes = Math.min(size, 64 * 1024); ; readBytes = Math.min(size, readBytes * 4)) {
      const tail = Buffer.allocUnsafe(readBytes);
      fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
      if (readBytes === size || tail.lastIndexOf(0x0a, tail.length - 2) >= 0) return tail;
    }
  }

  #tail(file: string): string {
    const descriptor = fs.openSync(file, "r");
    try {
      return this.#lastLines(descriptor, fs.fstatSync(descriptor).size).toString("utf8");
    } finally {
      fs.closeSync(descriptor);
    }
  }

  #readSeal(directory: string): { files: Record<string, SealFile> } | undefined {
    try {
      return JSON.parse(fs.readFileSync(path.join(directory, "SEAL.json"), "utf8")) as { files: Record<string, SealFile> };
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
  }

  #seal(day: string): void {
    const directory = path.join(this.dir, day);
    const sealPath = path.join(directory, "SEAL.json");
    if (fs.existsSync(sealPath)) return;
    const files: Record<string, SealFile> = {};
    let names: string[];
    try {
      names = fs.readdirSync(directory).filter((name) => name.endsWith(".jsonl")).sort();
    } catch (error) {
      if (errorCode(error) === "ENOENT") return;
      throw error;
    }
    for (const name of names) {
      const bytes = fs.readFileSync(path.join(directory, name));
      let lines = 0;
      let firstSequence = Number.POSITIVE_INFINITY;
      let lastSequence = 0;
      for (const line of bytes.toString("utf8").split("\n")) {
        const event = line ? parseEvent(line) : undefined;
        if (!event) continue;
        lines++;
        firstSequence = Math.min(firstSequence, event.sequence);
        lastSequence = Math.max(lastSequence, event.sequence);
      }
      if (lines === 0) continue;
      files[name] = { lines, firstSequence, lastSequence, sha256: createHash("sha256").update(bytes).digest("hex") };
    }
    writeFileAtomic(sealPath, `${JSON.stringify({ version: 1, day, files })}\n`);
  }
}
