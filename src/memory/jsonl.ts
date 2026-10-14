import fs from "node:fs";
import { StringDecoder } from "node:string_decoder";

const READ_CHUNK_BYTES = 64 * 1024;

/**
 * Yield JSONL payloads without materialising the source file or an intermediate
 * `split("\\n")` array. The iterator owns the descriptor and closes it even
 * when a caller stops before EOF.
 */
export function* readJsonlLines(file: string): Generator<string> {
  const fd = fs.openSync(file, "r");
  const buffer = Buffer.allocUnsafe(READ_CHUNK_BYTES);
  const decoder = new StringDecoder("utf8");
  let pending = "";
  try {
    for (;;) {
      const bytesRead = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (bytesRead === 0) break;
      pending += decoder.write(buffer.subarray(0, bytesRead));
      let newline = pending.indexOf("\n");
      while (newline !== -1) {
        yield pending.slice(0, newline);
        pending = pending.slice(newline + 1);
        newline = pending.indexOf("\n");
      }
    }
    pending += decoder.end();
    if (pending.length > 0) yield pending;
  } finally {
    fs.closeSync(fd);
  }
}

/** Parse tolerant JSONL one record at a time, skipping blank, invalid, and
 * non-object lines in the same way as the historical whole-file parser. */
export function* readJsonlRecords(file: string): Generator<unknown> {
  for (const line of readJsonlLines(file)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      yield JSON.parse(trimmed) as unknown;
    } catch {
      // Session logs are append-only and can contain a torn final line.
    }
  }
}
