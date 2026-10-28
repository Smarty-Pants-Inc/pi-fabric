/** Process-local, disposable read hints. Never persisted or used as publication evidence. */
export type ArchiveRangeBlock = [start: number, end: number, min: number | null, max: number | null];
export interface ArchiveRangeIndex {
  identity: string;
  min: number | null;
  max: number | null;
  blocks: ArchiveRangeBlock[];
}

export const ARCHIVE_RANGE_CACHE_ENTRIES = 128;
export const ARCHIVE_RANGE_CACHE_BYTES = 32 * 1024 * 1024;

interface CachedDay {
  generation: string;
  files: Map<string, ArchiveRangeIndex>;
  bytes: number;
}

class ArchiveRangeCache {
  readonly #days = new Map<string, CachedDay>();
  #bytes = 0;

  get stats(): { entries: number; bytes: number } {
    return { entries: this.#days.size, bytes: this.#bytes };
  }

  #forget(day: string): void {
    const cached = this.#days.get(day);
    if (cached) { this.#bytes -= cached.bytes; this.#days.delete(day); }
  }

  get(day: string, generation: string): Map<string, ArchiveRangeIndex> {
    const cached = this.#days.get(day);
    if (cached?.generation !== generation) { this.#forget(day); return new Map(); }
    this.#days.delete(day); this.#days.set(day, cached); // LRU touch.
    // Readers can overlap at async yields; never mutate another snapshot's map.
    return new Map(cached.files);
  }

  set(day: string, generation: string, files: Map<string, ArchiveRangeIndex>): void {
    this.#forget(day);
    // Charge UTF-16 strings, map/object overhead and 128 bytes per four-number block,
    // rather than just JSON bytes. This bounds retained hints, not transient scan data.
    let bytes = 512 + 2 * (day.length + generation.length);
    for (const [name, index] of files) {
      bytes += 512 + 2 * (name.length + index.identity.length) + 128 * index.blocks.length;
      if (bytes > ARCHIVE_RANGE_CACHE_BYTES) return; // Oversized days remain readable, uncached.
    }
    while (this.#days.size >= ARCHIVE_RANGE_CACHE_ENTRIES || this.#bytes + bytes > ARCHIVE_RANGE_CACHE_BYTES) {
      this.#forget(this.#days.keys().next().value!);
    }
    this.#days.set(day, { generation, files: new Map(files), bytes });
    this.#bytes += bytes;
  }
}

// Shared across fresh MeshArchive/fromRoot objects; the caps apply to the whole process.
export const archiveRangeCache = new ArchiveRangeCache();
