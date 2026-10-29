/** Cheap, independent summaries survive eviction of fine-grained range hints.
 * Fingerprints cover every segment name and exact stat identity; SEAL counters are
 * not trusted as range evidence. Nothing here is persisted or publication evidence.
 */
export interface ArchiveDayBounds {
  generation: string;
  fingerprint: string;
  min: number | null;
  max: number | null;
}
export const ARCHIVE_DAY_BOUNDS_BYTES = 4 * 1024 * 1024;
export const ARCHIVE_DAY_BOUNDS_ENTRIES = 4096;
class ArchiveDayBoundsCache {
  readonly #days = new Map<string, { bounds: ArchiveDayBounds; bytes: number }>();
  #bytes = 0;
  get stats(): { entries: number; bytes: number } { return { entries: this.#days.size, bytes: this.#bytes }; }
  #forget(day: string): void {
    const value = this.#days.get(day);
    if (value) { this.#bytes -= value.bytes; this.#days.delete(day); }
  }
  get(day: string, generation: string): ArchiveDayBounds | undefined {
    const value = this.#days.get(day);
    if (value?.bounds.generation !== generation) { this.#forget(day); return undefined; }
    this.#days.delete(day); this.#days.set(day, value);
    return value.bounds;
  }
  set(day: string, bounds: ArchiveDayBounds): void {
    this.#forget(day);
    const bytes = 512 + 2 * (day.length + bounds.generation.length + bounds.fingerprint.length);
    if (bytes > ARCHIVE_DAY_BOUNDS_BYTES) return;
    while (this.#days.size >= ARCHIVE_DAY_BOUNDS_ENTRIES || this.#bytes + bytes > ARCHIVE_DAY_BOUNDS_BYTES) {
      this.#forget(this.#days.keys().next().value!);
    }
    this.#days.set(day, { bounds, bytes }); this.#bytes += bytes;
  }
}
export const archiveDayBounds = new ArchiveDayBoundsCache();
