/** Process-local, disposable read hints. Never persisted or used as publication evidence. */
export type ArchiveRangeBlock = [start: number, end: number, min: number | null, max: number | null];
export interface ArchiveRangeBlocks extends Iterable<ArchiveRangeBlock> { readonly length: number }

// Four IEEE-754 doubles per block, not a V8 array/object per tuple. NaN encodes
// unknown extrema; offsets and every finite sequence retain their exact bits.
class PackedRangeBlocks implements ArchiveRangeBlocks {
  readonly #values: Float64Array;
  readonly length: number;
  constructor(blocks: ArchiveRangeBlocks) {
    this.length = blocks.length; this.#values = new Float64Array(this.length * 4);
    let offset = 0;
    for (const block of blocks) {
      this.#values[offset++] = block[0]; this.#values[offset++] = block[1];
      this.#values[offset++] = block[2] ?? NaN; this.#values[offset++] = block[3] ?? NaN;
    }
  }
  *[Symbol.iterator](): IterableIterator<ArchiveRangeBlock> {
    for (let offset = 0; offset < this.#values.length; offset += 4) {
      const min = this.#values[offset + 2]!, max = this.#values[offset + 3]!;
      yield [this.#values[offset]!, this.#values[offset + 1]!, Number.isNaN(min) ? null : min, Number.isNaN(max) ? null : max];
    }
  }
}

export interface ArchiveRangeIndex {
  identity: string;
  min: number | null;
  max: number | null;
  blocks: ArchiveRangeBlocks;
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
    // Charge UTF-16 strings, map/object/typed-array overhead and the exact packed
    // buffer bytes. This bounds retained hints, not transient scan data or total RSS.
    let bytes = 512 + 2 * (day.length + generation.length);
    for (const [name, index] of files) {
      bytes += 768 + 2 * (name.length + index.identity.length) + 32 * index.blocks.length;
      if (bytes > ARCHIVE_RANGE_CACHE_BYTES) return; // Oversized days remain readable, uncached.
    }
    while (this.#days.size >= ARCHIVE_RANGE_CACHE_ENTRIES || this.#bytes + bytes > ARCHIVE_RANGE_CACHE_BYTES) {
      this.#forget(this.#days.keys().next().value!);
    }
    const packed = new Map<string, ArchiveRangeIndex>();
    for (const [name, index] of files) packed.set(name, {
      ...index, blocks: index.blocks instanceof PackedRangeBlocks ? index.blocks : new PackedRangeBlocks(index.blocks),
    });
    this.#days.set(day, { generation, files: packed, bytes });
    this.#bytes += bytes;
  }
}

// Shared across fresh MeshArchive/fromRoot objects; the caps apply to the whole process.
export const archiveRangeCache = new ArchiveRangeCache();
